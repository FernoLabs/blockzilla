//! Identical application output for the independent CAR and Jetstreamer probes.
//! See README.md for the binary layout and limits of the comparison.
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const SCHEMA: &str = "car-common-export-v1";
const MAGIC: &[u8; 16] = b"CAR-TX-EXPORT-01";
const MAX_BLOCK: usize = 64 << 20;

pub struct Transaction<'a> {
    pub slot: u64,
    pub index: u64,
    pub signature: &'a [u8],
    pub message_hash: &'a [u8],
    pub vote: bool,
    pub failed: bool,
    pub fee: u64,
    pub pre_balances: &'a [u64],
    pub post_balances: &'a [u64],
}

pub fn encode(out: &mut Vec<u8>, tx: Transaction<'_>) -> io::Result<()> {
    if tx.signature.len() != 64 || tx.message_hash.len() != 32 {
        return Err(io::Error::other("invalid signature or message hash length"));
    }
    let extra = tx
        .pre_balances
        .len()
        .checked_add(tx.post_balances.len())
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(138))
        .ok_or_else(|| io::Error::other("export length overflow"))?;
    if out.len().checked_add(extra).is_none_or(|n| n > MAX_BLOCK) {
        return Err(io::Error::other("export block exceeds 64 MiB"));
    }
    out.extend_from_slice(&tx.slot.to_le_bytes());
    out.extend_from_slice(&tx.index.to_le_bytes());
    out.extend_from_slice(tx.signature);
    out.extend_from_slice(tx.message_hash);
    out.extend_from_slice(&[u8::from(tx.vote), u8::from(tx.failed)]);
    out.extend_from_slice(&tx.fee.to_le_bytes());
    for balances in [tx.pre_balances, tx.post_balances] {
        out.extend_from_slice(&(balances.len() as u64).to_le_bytes());
        for balance in balances {
            out.extend_from_slice(&balance.to_le_bytes());
        }
    }
    Ok(())
}

struct Block {
    count: u64,
    offset: u64,
    len: u64,
    // None also marks a memory block whose payload copy is in progress.
    memory: Option<Vec<u8>>,
}

enum Storage {
    Disk(BufWriter<File>),
    Memory {
        limit: usize,
        retained: usize,
        peak: usize,
        finish_started: bool,
    },
}

struct Spool {
    storage: Storage,
    offset: u64,
    blocks: BTreeMap<u64, Block>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportStats {
    pub mode: &'static str,
    /// Payload bytes only; excludes callers' buffers, map and allocator overhead.
    pub memory_limit_bytes: usize,
    /// Includes payload bytes reserved for copies still in progress.
    pub peak_memory_bytes: usize,
    pub spool_bytes: u64,
}

pub struct Export {
    spool: Mutex<Spool>,
    scratch: Option<PathBuf>,
    output: Mutex<File>,
}

impl Export {
    pub fn create(path: &Path) -> io::Result<Self> {
        let output = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut scratch = path.as_os_str().to_owned();
        scratch.push(".spool");
        let scratch = PathBuf::from(scratch);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&scratch)?;
        Ok(Self {
            spool: Mutex::new(Spool {
                storage: Storage::Disk(BufWriter::with_capacity(1 << 20, file)),
                offset: 0,
                blocks: BTreeMap::new(),
            }),
            scratch: Some(scratch),
            output: Mutex::new(output),
        })
    }

    /// Keep completed block payloads in memory, with an explicit byte limit.
    /// Concurrent copies reserve their entire payload before allocating it.
    /// The final output is still written in canonical order and synced to disk.
    pub fn create_memory(path: &Path, max_bytes: usize) -> io::Result<Self> {
        if max_bytes == 0 {
            return Err(io::Error::other("memory export limit must be positive"));
        }
        let output = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self {
            spool: Mutex::new(Spool {
                storage: Storage::Memory {
                    limit: max_bytes,
                    retained: 0,
                    peak: 0,
                    finish_started: false,
                },
                offset: 0,
                blocks: BTreeMap::new(),
            }),
            scratch: None,
            output: Mutex::new(output),
        })
    }

    pub fn stats(&self) -> io::Result<ExportStats> {
        let spool = self
            .spool
            .lock()
            .map_err(|_| io::Error::other("export lock poisoned"))?;
        Ok(match &spool.storage {
            Storage::Disk(_) => ExportStats {
                mode: "disk",
                memory_limit_bytes: 0,
                peak_memory_bytes: 0,
                spool_bytes: spool.offset,
            },
            Storage::Memory { limit, peak, .. } => ExportStats {
                mode: "memory",
                memory_limit_bytes: *limit,
                peak_memory_bytes: *peak,
                spool_bytes: 0,
            },
        })
    }

    pub fn block(&self, slot: u64, count: u64, bytes: &[u8]) -> io::Result<()> {
        let mut spool = self
            .spool
            .lock()
            .map_err(|_| io::Error::other("export lock poisoned"))?;
        if spool.blocks.contains_key(&slot)
            || bytes.len() > MAX_BLOCK
            || (count == 0) != bytes.is_empty()
        {
            return Err(io::Error::other("invalid or duplicate export block"));
        }
        let offset = spool.offset;
        let next_offset = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("export length overflow"))?;
        match &mut spool.storage {
            Storage::Disk(file) => {
                file.write_all(bytes)?;
                spool.blocks.insert(
                    slot,
                    Block {
                        count,
                        offset,
                        len: bytes.len() as u64,
                        memory: None,
                    },
                );
                spool.offset = next_offset;
                return Ok(());
            }
            Storage::Memory {
                limit,
                retained,
                peak,
                finish_started,
            } => {
                if *finish_started {
                    return Err(io::Error::other(
                        "memory export finalization already started",
                    ));
                }
                let next = retained
                    .checked_add(bytes.len())
                    .filter(|next| *next <= *limit)
                    .ok_or_else(|| io::Error::other("memory export byte limit exceeded"))?;
                *retained = next;
                *peak = (*peak).max(next);
            }
        }
        // Reserve the slot and bytes under the lock, then copy without the lock.
        // A concurrent duplicate or allocation cannot exceed the payload limit.
        spool.blocks.insert(
            slot,
            Block {
                count,
                offset,
                len: bytes.len() as u64,
                memory: None,
            },
        );
        spool.offset = next_offset;
        drop(spool);
        let mut owned = Vec::new();
        if let Err(error) = owned.try_reserve_exact(bytes.len()) {
            let mut spool = self
                .spool
                .lock()
                .map_err(|_| io::Error::other("export lock poisoned"))?;
            spool.blocks.remove(&slot);
            if let Storage::Memory { retained, .. } = &mut spool.storage {
                *retained -= bytes.len();
            }
            return Err(io::Error::other(format!(
                "memory export allocation failed: {error}"
            )));
        }
        owned.extend_from_slice(bytes);
        let mut spool = self
            .spool
            .lock()
            .map_err(|_| io::Error::other("export lock poisoned"))?;
        spool
            .blocks
            .get_mut(&slot)
            .expect("reserved export block")
            .memory = Some(owned);
        Ok(())
    }

    /// Validate coverage, write canonical slot order, and sync the final file.
    /// Both probes include this work in end-to-end elapsed time.
    pub fn finish(&self, expected: &[[u64; 2]]) -> io::Result<u64> {
        let mut spool = self
            .spool
            .lock()
            .map_err(|_| io::Error::other("export lock poisoned"))?;
        if expected.is_empty() || !expected.windows(2).all(|r| r[0][0] < r[1][0]) {
            return Err(io::Error::other("invalid export plan"));
        }
        for (&slot, block) in &spool.blocks {
            if expected
                .binary_search_by_key(&slot, |r| r[0])
                .ok()
                .is_none_or(|i| expected[i][1] != block.count)
            {
                return Err(io::Error::other(
                    "export contains unexpected block or count",
                ));
            }
        }
        for &[slot, count] in expected {
            if count > 0 && !spool.blocks.contains_key(&slot) {
                return Err(io::Error::other("export is missing a nonempty block"));
            }
        }
        let memory = matches!(spool.storage, Storage::Memory { .. });
        if memory && spool.blocks.values().any(|block| block.memory.is_none()) {
            return Err(io::Error::other(
                "memory export payload is unavailable or in progress",
            ));
        }
        match &mut spool.storage {
            Storage::Disk(file) => file.flush()?,
            Storage::Memory { finish_started, .. } => {
                if *finish_started {
                    return Err(io::Error::other(
                        "memory export finalization already started",
                    ));
                }
                *finish_started = true;
            }
        }
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("output lock poisoned"))?;
        let mut writer = BufWriter::with_capacity(1 << 20, &mut *output);
        writer.write_all(MAGIC)?;
        for &[slot, count] in expected {
            let (offset, len) = spool
                .blocks
                .get(&slot)
                .map(|block| (block.offset, block.len))
                .unwrap_or((0, 0));
            for n in [slot, count, len] {
                writer.write_all(&n.to_le_bytes())?;
            }
            if memory {
                if let Some(block) = spool.blocks.get_mut(&slot) {
                    let bytes = block.memory.take().expect("validated memory payload");
                    let written = writer.write_all(&bytes);
                    drop(bytes);
                    if let Storage::Memory { retained, .. } = &mut spool.storage {
                        *retained -= len as usize;
                    }
                    written?;
                }
            } else if let Storage::Disk(file) = &mut spool.storage {
                file.get_mut().seek(SeekFrom::Start(offset))?;
                if io::copy(&mut file.get_mut().take(len), &mut writer)? != len {
                    return Err(io::Error::other("truncated export spool"));
                }
            }
        }
        writer.flush()?;
        drop(writer);
        output.sync_all()?;
        let bytes = output.metadata()?.len();
        if let Some(scratch) = &self.scratch {
            std::fs::remove_file(scratch)?;
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    struct TemporaryOutput(PathBuf);
    impl TemporaryOutput {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            Self(std::env::temp_dir().join(format!(
                "car-export-memory-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }
    impl Drop for TemporaryOutput {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let mut scratch = self.0.as_os_str().to_owned();
            scratch.push(".spool");
            let _ = std::fs::remove_file(PathBuf::from(scratch));
        }
    }

    fn transaction(slot: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        encode(
            &mut bytes,
            Transaction {
                slot,
                index: 0,
                signature: &[1; 64],
                message_hash: &[2; 32],
                vote: true,
                failed: false,
                fee: 5000,
                pre_balances: &[10, 20],
                post_balances: &[5, 25],
            },
        )
        .unwrap();
        bytes
    }

    #[test]
    fn canonical_order_and_missing_block_validation() {
        let path = std::env::temp_dir().join(format!("car-export-test-{}", std::process::id()));
        let export = Export::create(&path).unwrap();
        let mut bytes = Vec::new();
        encode(
            &mut bytes,
            Transaction {
                slot: 3,
                index: 0,
                signature: &[1; 64],
                message_hash: &[2; 32],
                vote: true,
                failed: false,
                fee: 5000,
                pre_balances: &[10, 20],
                post_balances: &[5, 25],
            },
        )
        .unwrap();
        assert_eq!(bytes.len(), 170);
        export.block(3, 1, &bytes).unwrap();
        assert!(export.block(3, 1, &bytes).is_err());
        assert!(export.finish(&[[2, 1], [3, 1]]).is_err());
        assert_eq!(export.finish(&[[2, 0], [3, 1]]).unwrap(), 234);
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[..16], MAGIC);
        assert_eq!(u64::from_le_bytes(data[16..24].try_into().unwrap()), 2);
        assert_eq!(&data[64..], &bytes);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn memory_reverse_order_matches_disk_and_writes_no_spool() {
        let disk_path = TemporaryOutput::new();
        let memory_path = TemporaryOutput::new();
        let disk = Export::create(&disk_path.0).unwrap();
        let memory = Export::create_memory(&memory_path.0, 340).unwrap();
        let plan = [[2, 0], [3, 1], [4, 0], [5, 1]];
        for slot in [3, 5] {
            disk.block(slot, 1, &transaction(slot)).unwrap();
        }
        for slot in [5, 3] {
            memory.block(slot, 1, &transaction(slot)).unwrap();
        }
        memory.block(4, 0, &[]).unwrap();
        assert_eq!(disk.finish(&plan).unwrap(), memory.finish(&plan).unwrap());
        assert_eq!(
            std::fs::read(&disk_path.0).unwrap(),
            std::fs::read(&memory_path.0).unwrap()
        );
        assert_eq!(
            disk.stats().unwrap(),
            ExportStats {
                mode: "disk",
                memory_limit_bytes: 0,
                peak_memory_bytes: 0,
                spool_bytes: 340,
            }
        );
        assert_eq!(
            memory.stats().unwrap(),
            ExportStats {
                mode: "memory",
                memory_limit_bytes: 340,
                peak_memory_bytes: 340,
                spool_bytes: 0,
            }
        );
        let mut scratch = memory_path.0.as_os_str().to_owned();
        scratch.push(".spool");
        assert!(!PathBuf::from(scratch).exists());
        let spool = memory.spool.lock().unwrap();
        assert!(matches!(spool.storage, Storage::Memory { retained: 0, .. }));
        assert!(spool.blocks.values().all(|block| block.memory.is_none()));
        drop(spool);
        assert!(memory.block(6, 1, &transaction(6)).is_err());
        assert!(memory.finish(&plan).is_err());
    }

    #[test]
    fn concurrent_memory_blocks_match_disk_bytes() {
        let disk_path = TemporaryOutput::new();
        let memory_path = TemporaryOutput::new();
        let disk = Export::create(&disk_path.0).unwrap();
        let memory = Export::create_memory(&memory_path.0, 32 * 170).unwrap();
        let plan: Vec<_> = (0..32).map(|slot| [slot, 1]).collect();
        for slot in 0..32 {
            disk.block(slot, 1, &transaction(slot)).unwrap();
        }
        std::thread::scope(|scope| {
            for worker in 0usize..8 {
                let memory = &memory;
                scope.spawn(move || {
                    for slot in (worker..32).step_by(8).rev() {
                        memory
                            .block(slot as u64, 1, &transaction(slot as u64))
                            .unwrap();
                    }
                });
            }
        });
        disk.finish(&plan).unwrap();
        memory.finish(&plan).unwrap();
        assert_eq!(
            std::fs::read(&disk_path.0).unwrap(),
            std::fs::read(&memory_path.0).unwrap()
        );
        assert_eq!(memory.stats().unwrap().peak_memory_bytes, 32 * 170);
    }

    #[test]
    fn concurrent_memory_reservations_cannot_exceed_limit() {
        let path = TemporaryOutput::new();
        let export = Export::create_memory(&path.0, 4 * 170).unwrap();
        let barrier = Barrier::new(8);
        let accepted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|slot| {
                    let barrier = &barrier;
                    let export = &export;
                    scope.spawn(move || {
                        let bytes = transaction(slot);
                        barrier.wait();
                        export.block(slot, 1, &bytes).map(|_| slot)
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().unwrap().ok())
                .collect::<Vec<_>>()
        });
        assert_eq!(accepted.len(), 4);
        let mut plan: Vec<_> = accepted.into_iter().map(|slot| [slot, 1]).collect();
        plan.sort_unstable();
        assert_eq!(export.stats().unwrap().peak_memory_bytes, 4 * 170);
        export.finish(&plan).unwrap();
    }

    #[test]
    fn memory_rejects_duplicates_bad_counts_and_missing_blocks() {
        let path = TemporaryOutput::new();
        assert!(Export::create_memory(&path.0, 0).is_err());
        assert!(!path.0.exists());
        let export = Export::create_memory(&path.0, 340).unwrap();
        export.block(3, 1, &transaction(3)).unwrap();
        assert!(export.block(3, 1, &transaction(3)).is_err());
        assert!(export.block(4, 0, &[1]).is_err());
        assert!(export.block(4, 1, &[]).is_err());
        assert!(export.finish(&[]).is_err());
        assert!(export.finish(&[[3, 1], [2, 0]]).is_err());
        assert!(export.finish(&[[3, 1], [3, 1]]).is_err());
        assert!(export.finish(&[[3, 2]]).is_err());
        assert!(export.finish(&[[2, 1], [3, 1]]).is_err());
        assert!(export.finish(&[[4, 0]]).is_err());
        assert_eq!(std::fs::metadata(&path.0).unwrap().len(), 0);
        export.finish(&[[2, 0], [3, 1]]).unwrap();
    }
}
