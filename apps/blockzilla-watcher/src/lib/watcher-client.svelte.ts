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
  parseBlockTimeGapIndex,
  type BlockTimeGapIndex
} from '$lib/block-time-gap-index';
import {
  parseRuntimeOperations,
  type RuntimeOperations
} from '$lib/runtime-operations';
import {
  parseIngestPipelineStatus,
  type IngestPipelineStatus
} from '$lib/ingest-pipeline-status';
import {
  parseRawShredStatus,
  type RawShredStatus
} from '$lib/raw-shred-status';

export type ConnectionState = 'connecting' | 'live' | 'retrying' | 'offline';
export type IngestFeedState = 'loading' | 'live' | 'retrying' | 'unavailable';
export type WatcherTrendSample = {
  at: number;
  archiveBlocksDone: number;
  load1m: number;
  memoryAvailableBytes: number;
  diskReadMibPerSec: number;
  diskWriteMibPerSec: number;
};
export type RawShredTrendSample = {
  at: number;
  latestSlot: number;
  acceptedTotal: number;
  spoolBytes: number;
};

const STATUS_REQUEST_TIMEOUT_MS = 5_000;
const STATUS_RETRY_INTERVAL_MS = 10_000;
const INGEST_REQUEST_TIMEOUT_MS = 4_000;
const RAW_SHRED_STATUS_PATH = '/api/v1/sidecars/shred-ingest/status.json';
const RAW_SHRED_STATUS_FALLBACK =
  'https://hivezilla-shred-status-8dafe7-188-245-147-127.sslip.io/api/v1/sidecars/shred-ingest/status.json';

export class WatcherClient {
  snapshot = $state.raw<PipelineSnapshot | null>(null);
  blockTimeGapIndex = $state.raw<BlockTimeGapIndex | null>(null);
  runtimeOperations = $state.raw<RuntimeOperations | null>(null);
  ingestPipeline = $state.raw<IngestPipelineStatus | null>(null);
  rawShred = $state.raw<RawShredStatus | null>(null);
  watcherTrend = $state.raw<WatcherTrendSample[]>([]);
  rawShredTrend = $state.raw<RawShredTrendSample[]>([]);
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
    let gapIndexFetchInFlight = false;
    let runtimeFetchInFlight = false;
    let ingestFetchInFlight = false;
    let rawShredFetchInFlight = false;
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
      this.watcherTrend = appendTrendSample(this.watcherTrend, {
        at: normalized.now_unix_secs,
        archiveBlocksDone: normalized.summary.blocks_done,
        load1m: normalized.machine.load_1m,
        memoryAvailableBytes: normalized.machine.memory_available_bytes,
        diskReadMibPerSec: normalized.machine.archive_device_read_mib_per_sec ?? 0,
        diskWriteMibPerSec: normalized.machine.archive_device_write_mib_per_sec ?? 0
      });
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
      const updated = this.snapshot;
      this.watcherTrend = appendTrendSample(this.watcherTrend, {
        at: updated.now_unix_secs,
        archiveBlocksDone: updated.summary.blocks_done,
        load1m: updated.machine.load_1m,
        memoryAvailableBytes: updated.machine.memory_available_bytes,
        diskReadMibPerSec: updated.machine.archive_device_read_mib_per_sec ?? 0,
        diskWriteMibPerSec: updated.machine.archive_device_write_mib_per_sec ?? 0
      });
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

    const refreshBlockTimeGapIndex = async () => {
      if (gapIndexFetchInFlight) return;
      gapIndexFetchInFlight = true;
      try {
        const response = await fetch('/api/v1/sidecars/block-time-gaps/index.json', {
          cache: 'no-store',
          headers: { accept: 'application/json' }
        });
        if (response.status === 404) {
          this.blockTimeGapIndex = null;
          return;
        }
        if (!response.ok) throw new Error(`block-time gap index ${response.status}`);
        const index = parseBlockTimeGapIndex(await response.json());
        if (!index) throw new Error('invalid block-time gap index');
        this.blockTimeGapIndex = index;
      } catch {
        // This immutable aggregate is optional. Preserve the last valid copy
        // when a transient static-file request fails.
      } finally {
        gapIndexFetchInFlight = false;
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

    const refreshRawShred = async () => {
      if (rawShredFetchInFlight) return;
      rawShredFetchInFlight = true;
      try {
        let response = await fetch(RAW_SHRED_STATUS_PATH, {
          cache: 'no-store',
          headers: { accept: 'application/json' }
        });
        if (response.status === 404) {
          response = await fetch(RAW_SHRED_STATUS_FALLBACK, {
            cache: 'no-store',
            headers: { accept: 'application/json' }
          });
        }
        if (!response.ok) throw new Error(`raw shred status ${response.status}`);
        const status = parseRawShredStatus(await response.json());
        if (status && !disposed) {
          this.rawShred = status;
          if (
            status.hivezilla.latest_slot !== null &&
            status.hivezilla.accepted_total !== null &&
            status.hivezilla.spool_bytes !== null
          ) {
            this.rawShredTrend = appendTrendSample(this.rawShredTrend, {
              at: status.updated_unix_secs,
              latestSlot: status.hivezilla.latest_slot,
              acceptedTotal: status.hivezilla.accepted_total,
              spoolBytes: status.hivezilla.spool_bytes
            });
          }
        }
      } catch {
        // Keep the last valid raw-shred sample. The UI marks it stale rather
        // than presenting a transient network error as a stopped recorder.
      } finally {
        rawShredFetchInFlight = false;
      }
    };

    const events = new EventSource('/api/v1/events');
    void resyncSnapshot();
    const statusPoll = window.setInterval(() => {
      if (events.readyState !== EventSource.OPEN) void resyncSnapshot();
    }, STATUS_RETRY_INTERVAL_MS);
    void refreshBlockTimeGapIndex();
    void refreshRuntimeOperations();
    const runtimePoll = window.setInterval(refreshRuntimeOperations, 5_000);
    void refreshIngestPipeline();
    const ingestPoll = window.setInterval(refreshIngestPipeline, 5_000);
    void refreshRawShred();
    const rawShredPoll = window.setInterval(refreshRawShred, 5_000);
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
      window.clearInterval(runtimePoll);
      window.clearInterval(ingestPoll);
      window.clearInterval(rawShredPoll);
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

function appendTrendSample<T extends { at: number }>(samples: T[], sample: T) {
  const previous = samples.at(-1);
  if (previous?.at === sample.at) return samples;
  if (previous && previous.at > sample.at) return samples;
  const cutoff = sample.at - 10 * 60;
  return [...samples.filter((entry) => entry.at >= cutoff), sample].slice(-121);
}
