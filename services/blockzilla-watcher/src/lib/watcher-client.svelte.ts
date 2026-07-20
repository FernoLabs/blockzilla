import { createContext } from 'svelte';
import * as publicEnv from '$env/static/public';
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
  parseShredIngestStatus,
  type ShredIngestStatus
} from '$lib/shred-ingest-status';

export type ConnectionState = 'connecting' | 'live' | 'retrying' | 'offline';
export type ShredIngestFeedState = 'loading' | 'live' | 'retrying' | 'unavailable';

const STATUS_REQUEST_TIMEOUT_MS = 5_000;
const STATUS_RETRY_INTERVAL_MS = 10_000;
const SHRED_INGEST_REQUEST_TIMEOUT_MS = 4_000;
const SHRED_INGEST_POLL_INTERVAL_MS = 5_000;
const SHRED_INGEST_STATUS_FALLBACK_URL =
  'https://hivezilla-shred-status-8dafe7-188-245-147-127.sslip.io/api/v1/sidecars/shred-ingest/status.json';
const SHRED_INGEST_STATUS_URL = configuredShredIngestStatusUrl();

export class WatcherClient {
  snapshot = $state.raw<PipelineSnapshot | null>(null);
  blockTimeGapBackfill = $state.raw<BlockTimeGapBackfill | null>(null);
  runtimeOperations = $state.raw<RuntimeOperations | null>(null);
  shredIngestStatus = $state.raw<ShredIngestStatus | null>(null);
  shredIngestFeedState = $state<ShredIngestFeedState>('loading');
  shredIngestFeedMessage = $state('Loading shred ingest status');
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
    let shredIngestFetchInFlight = false;
    let shredIngestRequestController: AbortController | null = null;

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

    const refreshShredIngestStatus = async () => {
      if (shredIngestFetchInFlight) return;
      shredIngestFetchInFlight = true;
      const controller = new AbortController();
      shredIngestRequestController = controller;
      const requestTimeout = window.setTimeout(
        () => controller.abort(),
        SHRED_INGEST_REQUEST_TIMEOUT_MS
      );
      try {
        const response = await fetch(SHRED_INGEST_STATUS_URL, {
          cache: 'no-store',
          headers: { accept: 'application/json' },
          signal: controller.signal
        });
        if (!response.ok) throw new Error(`shred ingest status ${response.status}`);
        const status = parseShredIngestStatus(await response.json());
        if (!status) throw new Error('invalid shred ingest status');
        if (disposed) return;
        this.shredIngestStatus = status;
        this.shredIngestFeedState = 'live';
        this.shredIngestFeedMessage = 'Shred ingest status is updating';
      } catch (error) {
        if (!disposed) {
          this.shredIngestFeedState = this.shredIngestStatus ? 'retrying' : 'unavailable';
          this.shredIngestFeedMessage = controller.signal.aborted
            ? 'Shred ingest status timed out; retrying'
            : `Shred ingest status unavailable: ${errorMessage(error)}`;
        }
      } finally {
        window.clearTimeout(requestTimeout);
        if (shredIngestRequestController === controller) shredIngestRequestController = null;
        shredIngestFetchInFlight = false;
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
    void refreshShredIngestStatus();
    const shredIngestPoll = window.setInterval(
      refreshShredIngestStatus,
      SHRED_INGEST_POLL_INTERVAL_MS
    );
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
      window.clearInterval(shredIngestPoll);
      shredIngestRequestController?.abort();
      shredIngestRequestController = null;
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

function configuredShredIngestStatusUrl() {
  const configured = Object.entries(publicEnv)
    .find(([name]) => name === 'PUBLIC_HIVEZILLA_SHRED_STATUS_URL')?.[1];
  return typeof configured === 'string' && configured.trim().length > 0
    ? configured.trim()
    : SHRED_INGEST_STATUS_FALLBACK_URL;
}
