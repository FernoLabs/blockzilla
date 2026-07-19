import { createContext } from 'svelte';
import {
  asRecord,
  integerValue,
  parsePipelineSnapshot,
  parsePipelineSnapshotPatch,
  type PipelineSnapshot,
  type PipelineSnapshotPatch
} from '$lib/pipeline-snapshot';
import {
  applySnapshotPatch,
  snapshotPatchSequenceAction
} from '$lib/snapshot-patch';
import {
  parseBlockTimeGapBackfill,
  type BlockTimeGapBackfill
} from '$lib/block-time-gap-backfill';
import {
  parseRuntimeOperations,
  type RuntimeOperations
} from '$lib/runtime-operations';
import {
  parseIngestPipelineStatus,
  type IngestPipelineStatus
} from '$lib/ingest-pipeline-status';

export type ConnectionState = 'connecting' | 'live' | 'retrying' | 'offline';
export type IngestFeedState = 'loading' | 'live' | 'retrying' | 'unavailable';

const STATUS_REQUEST_TIMEOUT_MS = 5_000;
const STATUS_RETRY_INTERVAL_MS = 10_000;
const INGEST_REQUEST_TIMEOUT_MS = 4_000;

export class WatcherClient {
  snapshot = $state.raw<PipelineSnapshot | null>(null);
  blockTimeGapBackfill = $state.raw<BlockTimeGapBackfill | null>(null);
  runtimeOperations = $state.raw<RuntimeOperations | null>(null);
  ingestPipeline = $state.raw<IngestPipelineStatus | null>(null);
  ingestFeedState = $state<IngestFeedState>('loading');
  ingestFeedMessage = $state('Loading ingest status');
  connectionState = $state<ConnectionState>('connecting');
  connectionMessage = $state('Connecting');

  #disconnect: (() => void) | null = null;

  connect = () => {
    if (this.#disconnect) return this.#disconnect;

    let disposed = false;
    let lastSequence = -1;
    let statusFetchInFlight = false;
    let backfillFetchInFlight = false;
    let runtimeFetchInFlight = false;
    let ingestFetchInFlight = false;
    let ingestRequestController: AbortController | null = null;

    const acceptPayload = (value: unknown, sequence?: number) => {
      const normalized = parsePipelineSnapshot(value);
      if (!normalized) return false;
      const incomingSequence = sequence ?? normalized.sequence;
      if (sequence !== undefined && sequence !== normalized.sequence) return false;
      if (incomingSequence <= lastSequence) {
        const currentSnapshotTime = this.snapshot?.now_unix_secs ?? 0;
        // A restarted service resets its process-local sequence. A newer
        // snapshot timestamp is the evidence that this is a restart rather
        // than an out-of-order event from the current process.
        if (normalized.now_unix_secs <= currentSnapshotTime) return true;
      }
      lastSequence = incomingSequence;
      this.snapshot = normalized;
      return true;
    };

    const requestSnapshotResync = (message: string) => {
      this.connectionState = 'retrying';
      this.connectionMessage = message;
      void resyncSnapshot();
    };

    const acceptSnapshotPatch = (
      patch: PipelineSnapshotPatch,
      envelopeSequence: number
    ) => {
      const currentSnapshot = this.snapshot;
      if (!currentSnapshot) {
        requestSnapshotResync('Incremental event arrived before its base snapshot; resyncing.');
        return;
      }
      const sequenceAction = snapshotPatchSequenceAction(lastSequence, envelopeSequence);
      if (sequenceAction === 'ignore') return;
      if (patch.sequence !== envelopeSequence) {
        requestSnapshotResync('Incremental event sequence did not match its envelope; resyncing.');
        return;
      }
      if (patch.schema_version !== currentSnapshot.schema_version) {
        requestSnapshotResync('Incremental event schema changed; resyncing.');
        return;
      }
      if (sequenceAction === 'resync') {
        requestSnapshotResync(`Incremental event gap after sequence ${lastSequence}; resyncing.`);
        return;
      }

      this.snapshot = applySnapshotPatch(currentSnapshot, patch);
      lastSequence = envelopeSequence;
      this.connectionState = 'live';
      this.connectionMessage = 'Live event stream';
    };

    const resyncSnapshot = async () => {
      if (statusFetchInFlight) return;
      statusFetchInFlight = true;
      const controller = new AbortController();
      const requestTimeout = window.setTimeout(
        () => controller.abort(),
        STATUS_REQUEST_TIMEOUT_MS
      );
      try {
        const response = await fetch('/api/v1/status', {
          headers: { accept: 'application/json' },
          signal: controller.signal
        });
        if (!response.ok) throw new Error(`status ${response.status}`);
        if (!acceptPayload(await response.json())) throw new Error('invalid status snapshot');
        if (events.readyState === EventSource.OPEN) {
          this.connectionState = 'live';
          this.connectionMessage = 'Live event stream';
        } else {
          this.connectionState = 'retrying';
          this.connectionMessage = 'Snapshot current; event stream reconnecting';
        }
      } catch (error) {
        if (!disposed) {
          this.connectionState = events.readyState === EventSource.CLOSED ? 'offline' : 'retrying';
          this.connectionMessage = `Event stream resync failed: ${errorMessage(error)}`;
        }
      } finally {
        window.clearTimeout(requestTimeout);
        statusFetchInFlight = false;
      }
    };

    const refreshBlockTimeGapBackfill = async () => {
      if (backfillFetchInFlight) return;
      backfillFetchInFlight = true;
      try {
        const response = await fetch('/api/v1/sidecars/block-time-gaps/status.json', {
          cache: 'no-store',
          headers: { accept: 'application/json' }
        });
        if (response.status === 404) {
          this.blockTimeGapBackfill = null;
          return;
        }
        if (!response.ok) throw new Error(`backfill status ${response.status}`);
        const status = parseBlockTimeGapBackfill(await response.json());
        if (!status) throw new Error('invalid block-time-gap backfill status');
        this.blockTimeGapBackfill = status;
      } catch {
        // This feed is optional. Preserve the last valid sample so the page can
        // mark it stale instead of flashing the row during a transient failure.
      } finally {
        backfillFetchInFlight = false;
      }
    };

    const refreshRuntimeOperations = async () => {
      if (runtimeFetchInFlight) return;
      runtimeFetchInFlight = true;
      try {
        const response = await fetch('/api/v1/sidecars/runtime-operations/status.json', {
          cache: 'no-store',
          headers: { accept: 'application/json' }
        });
        if (response.status === 404) {
          this.runtimeOperations = null;
          return;
        }
        if (!response.ok) throw new Error(`runtime operations status ${response.status}`);
        const status = parseRuntimeOperations(await response.json());
        if (!status) throw new Error('invalid runtime operations status');
        this.runtimeOperations = status;
      } catch {
        // Preserve the last valid sample so the UI can mark it stale instead
        // of hiding an operation during a transient sidecar refresh failure.
      } finally {
        runtimeFetchInFlight = false;
      }
    };

    const refreshIngestPipeline = async () => {
      if (ingestFetchInFlight) return;
      ingestFetchInFlight = true;
      const controller = new AbortController();
      ingestRequestController = controller;
      const requestTimeout = window.setTimeout(
        () => controller.abort(),
        INGEST_REQUEST_TIMEOUT_MS
      );
      try {
        const response = await fetch('/api/v1/sidecars/ingest-pipeline/status.json', {
          cache: 'no-store',
          headers: { accept: 'application/json' },
          signal: controller.signal
        });
        if (disposed) return;
        if (response.status === 404) {
          this.ingestPipeline = null;
          this.ingestFeedState = 'unavailable';
          this.ingestFeedMessage = 'Ingest status publisher is not installed';
          return;
        }
        if (!response.ok) throw new Error(`ingest status ${response.status}`);
        const status = parseIngestPipelineStatus(await response.json());
        if (!status) throw new Error('invalid ingest status');
        if (disposed) return;
        this.ingestPipeline = status;
        this.ingestFeedState = 'live';
        this.ingestFeedMessage = 'Ingest status is updating';
      } catch (error) {
        if (!disposed) {
          this.ingestFeedState = this.ingestPipeline ? 'retrying' : 'unavailable';
          this.ingestFeedMessage = controller.signal.aborted
            ? 'Ingest status timed out; retrying'
            : `Ingest status unavailable: ${errorMessage(error)}`;
        }
      } finally {
        window.clearTimeout(requestTimeout);
        if (ingestRequestController === controller) ingestRequestController = null;
        ingestFetchInFlight = false;
      }
    };

    const events = new EventSource('/api/v1/events');
    void resyncSnapshot();
    const statusPoll = window.setInterval(() => {
      if (events.readyState !== EventSource.OPEN) void resyncSnapshot();
    }, STATUS_RETRY_INTERVAL_MS);
    void refreshBlockTimeGapBackfill();
    const backfillPoll = window.setInterval(refreshBlockTimeGapBackfill, 15_000);
    void refreshRuntimeOperations();
    const runtimePoll = window.setInterval(refreshRuntimeOperations, 5_000);
    void refreshIngestPipeline();
    const ingestPoll = window.setInterval(refreshIngestPipeline, 5_000);
    events.onopen = () => {
      if (disposed) return;
      // The service sequence is process-local and restarts from zero. Reset
      // the client-side guard whenever EventSource establishes a new stream
      // so a service restart cannot leave the dashboard frozen on old state.
      lastSequence = -1;
      this.connectionState = this.snapshot ? 'retrying' : 'connecting';
      this.connectionMessage = 'Event stream connected; waiting for snapshot';
    };
    events.addEventListener('snapshot', (event) => {
      try {
        const envelope = asRecord(JSON.parse(event.data) as unknown);
        const sequence = integerValue(envelope?.sequence);
        if (envelope?.type !== 'snapshot' || sequence === null) {
          requestSnapshotResync('Ignored an invalid full snapshot event; resyncing.');
          return;
        }
        if (!acceptPayload(envelope.data, sequence)) {
          requestSnapshotResync('Rejected an invalid full snapshot event; resyncing.');
          return;
        }
        this.connectionState = 'live';
        this.connectionMessage = 'Live event stream';
      } catch (error) {
        requestSnapshotResync(`Ignored an invalid full snapshot event: ${errorMessage(error)}`);
      }
    });
    events.addEventListener('snapshot_patch', (event) => {
      try {
        const envelope = asRecord(JSON.parse(event.data) as unknown);
        const patch = parsePipelineSnapshotPatch(envelope?.data);
        const sequence = integerValue(envelope?.sequence);
        if (envelope?.type !== 'snapshot_patch' || !patch || sequence === null) {
          requestSnapshotResync('Ignored an invalid incremental event; resyncing.');
          return;
        }
        acceptSnapshotPatch(patch, sequence);
      } catch (error) {
        requestSnapshotResync(`Ignored an invalid incremental event: ${errorMessage(error)}`);
      }
    });
    events.addEventListener('resync', (event) => {
      try {
        const envelope = asRecord(JSON.parse(event.data) as unknown);
        if (envelope?.type !== 'resync') {
          requestSnapshotResync('Ignored an invalid resync event; resyncing.');
          return;
        }
        requestSnapshotResync('Event stream requested a full resync.');
      } catch (error) {
        requestSnapshotResync(`Ignored an invalid resync event: ${errorMessage(error)}`);
      }
    });
    events.onerror = () => {
      if (disposed) return;
      this.connectionState = events.readyState === EventSource.CLOSED ? 'offline' : 'retrying';
      this.connectionMessage = events.readyState === EventSource.CLOSED
        ? 'Event stream closed'
        : 'Event stream reconnecting';
      void resyncSnapshot();
    };

    const disconnect = () => {
      if (disposed) return;
      disposed = true;
      events.close();
      window.clearInterval(statusPoll);
      window.clearInterval(backfillPoll);
      window.clearInterval(runtimePoll);
      window.clearInterval(ingestPoll);
      ingestRequestController?.abort();
      ingestRequestController = null;
      this.#disconnect = null;
    };
    this.#disconnect = disconnect;
    return disconnect;
  };
}

const watcherContext = createContext<WatcherClient>();

export const useWatcherClient = watcherContext[0];
export const provideWatcherClient = watcherContext[1];

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
