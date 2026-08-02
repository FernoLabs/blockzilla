//! Ordered blocking-I/O boundary for the repair provenance WAL.
//!
//! The raw TVU receiver and repair transport share a Tokio runtime. Opening, recovering, appending,
//! or syncing the repair WAL on a Tokio worker could therefore stall raw capture on a slow or full
//! filesystem. This worker owns `RepairWal` on one dedicated OS thread and exposes a capacity-one
//! command queue. Callers receive an acknowledgement only after the requested operation completes;
//! with `EveryRecord`, an append acknowledgement is consequently also the durable-acceptance gate.

use std::{io, path::PathBuf, thread, time::Instant};

use tokio::sync::{mpsc, oneshot};

use crate::repair_wal::{
    RepairProvenance, RepairWal, RepairWalAppend, RepairWalConfig, RepairWalInspection,
};

const COMMAND_CAPACITY: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairWalSnapshot {
    pub next_sequence: u64,
    pub retained_bytes: u64,
    pub max_file_bytes: u64,
    pub max_retained_bytes: u64,
    pub filesystem_reserve_bytes: u64,
    pub filesystem_available_bytes: u64,
    pub v3_sealed: bool,
    pub active_segment_bytes: u64,
    pub segment_count: u64,
    pub active_segment_id: u64,
    pub rollovers: u64,
    pub durable_through_sequence: Option<u64>,
    pub syncs: u64,
}

enum Command {
    Append {
        provenance: RepairProvenance,
        shred_payload: Vec<u8>,
        now: Instant,
        reply: oneshot::Sender<io::Result<(RepairWalAppend, RepairWalSnapshot)>>,
    },
    SyncIfDue {
        now: Instant,
        reply: oneshot::Sender<io::Result<RepairWalSnapshot>>,
    },
    Flush {
        now: Instant,
        reply: oneshot::Sender<io::Result<RepairWalSnapshot>>,
    },
}

/// A single-owner, bounded, ordered async facade over blocking repair-WAL operations.
pub struct RepairWalWorker {
    commands: mpsc::Sender<Command>,
    snapshot: RepairWalSnapshot,
}

impl RepairWalWorker {
    /// Reads capacity and validation state without running filesystem work on a Tokio worker.
    pub async fn inspect(path: PathBuf) -> io::Result<RepairWalInspection> {
        let (reply, response) = oneshot::channel();
        thread::Builder::new()
            .name("repair-wal-inspect".to_owned())
            .spawn(move || {
                let _ = reply.send(RepairWal::inspect(&path));
            })
            .map_err(|error| io::Error::other(format!("spawn repair WAL inspection: {error}")))?;
        response.await.map_err(|_| {
            io::Error::other("repair WAL inspection thread stopped without a result")
        })?
    }

    /// Opens and fully recovers the WAL on its dedicated thread.
    pub async fn open(config: RepairWalConfig, now: Instant) -> io::Result<Self> {
        Self::start(move || RepairWal::open(config, now)).await
    }

    /// Moves an already-open WAL onto its dedicated thread. Kept for focused runtime tests and
    /// callers that completed recovery before constructing the UDP runtime.
    pub async fn from_wal(wal: RepairWal) -> io::Result<Self> {
        Self::start(move || Ok(wal)).await
    }

    async fn start(
        open: impl FnOnce() -> io::Result<RepairWal> + Send + 'static,
    ) -> io::Result<Self> {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (ready_tx, ready_rx) = oneshot::channel();
        thread::Builder::new()
            .name("repair-wal".to_owned())
            .spawn(move || match open() {
                Ok(wal) => {
                    let snapshot = snapshot(&wal);
                    if ready_tx.send(Ok(snapshot)).is_ok() {
                        run_worker(wal, receiver);
                    }
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            })
            .map_err(|error| io::Error::other(format!("spawn repair WAL worker: {error}")))?;
        let snapshot = ready_rx
            .await
            .map_err(|_| io::Error::other("repair WAL worker exited during startup"))??;
        Ok(Self { commands, snapshot })
    }

    pub fn snapshot(&self) -> RepairWalSnapshot {
        self.snapshot
    }

    pub async fn append(
        &mut self,
        provenance: RepairProvenance,
        shred_payload: Vec<u8>,
        now: Instant,
    ) -> io::Result<RepairWalAppend> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Append {
            provenance,
            shred_payload,
            now,
            reply,
        })
        .await?;
        let (append, snapshot) = receive(response)
            .await
            .map_err(|error| operation_error("append", error))?;
        self.snapshot = snapshot;
        Ok(append)
    }

    pub async fn sync_if_due(&mut self, now: Instant) -> io::Result<()> {
        if self.snapshot.next_sequence.checked_sub(1) == self.snapshot.durable_through_sequence {
            return Ok(());
        }
        let (reply, response) = oneshot::channel();
        self.send(Command::SyncIfDue { now, reply }).await?;
        self.snapshot = receive(response)
            .await
            .map_err(|error| operation_error("sync", error))?;
        Ok(())
    }

    pub async fn flush(&mut self, now: Instant) -> io::Result<()> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Flush { now, reply }).await?;
        self.snapshot = receive(response)
            .await
            .map_err(|error| operation_error("flush", error))?;
        Ok(())
    }

    async fn send(&self, command: Command) -> io::Result<()> {
        self.commands
            .send(command)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "repair WAL worker stopped"))
    }
}

async fn receive<T>(response: oneshot::Receiver<io::Result<T>>) -> io::Result<T> {
    response
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "repair WAL worker dropped ACK"))?
}

fn operation_error(operation: &str, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("repair WAL {operation} failed: {error}"),
    )
}

fn run_worker(mut wal: RepairWal, mut commands: mpsc::Receiver<Command>) {
    while let Some(command) = commands.blocking_recv() {
        match command {
            Command::Append {
                provenance,
                shred_payload,
                now,
                reply,
            } => {
                let result = (|| {
                    let mut append = wal.append(&provenance, &shred_payload, now)?;
                    // RepairRuntime may expose the shred as accepted as soon as this ACK arrives.
                    // Preserve that invariant even if a non-production caller supplied a batch
                    // policy: the worker boundary always promotes the append to durability.
                    if !append.synced {
                        wal.flush_and_sync(now)?;
                        append.synced = true;
                    }
                    Ok((append, snapshot(&wal)))
                })();
                let _ = reply.send(result);
            }
            Command::SyncIfDue { now, reply } => {
                let result = wal.sync_if_due(now).map(|_| snapshot(&wal));
                let _ = reply.send(result);
            }
            Command::Flush { now, reply } => {
                let result = wal.flush_and_sync(now).map(|_| snapshot(&wal));
                let _ = reply.send(result);
            }
        }
    }
    // Any best-effort sync performed by RepairWal::drop also remains confined to this thread.
}

fn snapshot(wal: &RepairWal) -> RepairWalSnapshot {
    RepairWalSnapshot {
        next_sequence: wal.next_sequence(),
        retained_bytes: wal.retained_bytes(),
        max_file_bytes: wal.max_file_bytes(),
        max_retained_bytes: wal.max_retained_bytes(),
        filesystem_reserve_bytes: wal.filesystem_reserve_bytes(),
        filesystem_available_bytes: wal.filesystem_available_bytes(),
        v3_sealed: wal.v3_sealed(),
        active_segment_bytes: wal.file_len(),
        segment_count: wal.segment_count(),
        active_segment_id: wal.active_segment_id(),
        rollovers: wal.rollovers(),
        durable_through_sequence: wal.durable_through_sequence(),
        syncs: wal.syncs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{num::NonZeroU64, time::Duration};
    use tempfile::tempdir;

    use crate::{repair_wal::RepairWalFsyncPolicy, repair_wire::ShredRepairRequest};
    use solana_hash::Hash;
    use solana_keypair::Signature;
    use solana_pubkey::Pubkey;

    fn provenance() -> RepairProvenance {
        RepairProvenance {
            received_at_unix_ms: 1,
            nonce: 2,
            request: ShredRepairRequest::Shred {
                slot: 3,
                shred_index: 4,
            },
            peer_addr: "127.0.0.1:5".to_owned(),
            peer_pubkey: Pubkey::new_from_array([1; 32]),
            shred_slot: 3,
            shred_index: 4,
            fec_set_index: 0,
            shred_version: 6,
            expected_slot_leader: Pubkey::new_from_array([2; 32]),
            fec_merkle_root: Hash::new_from_array([3; 32]),
            trust_anchor_fec_set_index: 32,
            learned_chained_merkle_root: false,
            chained_merkle_root: Some(Hash::new_from_array([4; 32])),
            leader_signature: Signature::from([5; 64]),
        }
    }

    #[tokio::test]
    async fn dedicated_inspection_reports_an_absent_wal() {
        let directory = tempdir().unwrap();
        let inspection = RepairWalWorker::inspect(directory.path().join("absent.repair.wal"))
            .await
            .unwrap();

        assert_eq!(inspection.retained_bytes, 0);
        assert_eq!(inspection.segment_count, 0);
        assert!(inspection.validation_error.is_none());
    }

    #[tokio::test]
    async fn append_ack_reports_only_the_durable_ordered_prefix() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("worker.repair.wal");
        let now = Instant::now();
        let mut worker = RepairWalWorker::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 8 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .await
        .unwrap();

        let first = worker
            .append(provenance(), vec![7; 128], now)
            .await
            .unwrap();
        let second = worker
            .append(provenance(), vec![8; 128], now + Duration::from_millis(1))
            .await
            .unwrap();

        assert_eq!((first.sequence, second.sequence), (0, 1));
        assert_eq!(worker.snapshot().durable_through_sequence, Some(1));
        assert_eq!(worker.snapshot().next_sequence, 2);
        drop(worker);
        assert_eq!(RepairWal::read_all(&path).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn batch_policy_is_promoted_to_a_durable_append_ack() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("worker.repair.wal");
        let now = Instant::now();
        let mut worker = RepairWalWorker::open(
            RepairWalConfig {
                path,
                fsync: RepairWalFsyncPolicy::Batch {
                    max_unsynced_records: NonZeroU64::new(2).unwrap(),
                    max_unsynced_age: Duration::from_secs(60),
                },
                max_file_bytes: 1024 * 1024,
                max_retained_bytes: 8 * 1024 * 1024,
                filesystem_reserve_bytes: 1,
            },
            now,
        )
        .await
        .unwrap();

        let append = worker
            .append(provenance(), vec![9; 128], now)
            .await
            .unwrap();
        assert!(append.synced);
        assert_eq!(worker.snapshot().durable_through_sequence, Some(0));
    }
}
