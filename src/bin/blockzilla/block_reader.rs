use anyhow::Result;
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
    transaction_parser::VersionedTransaction,
};
use indicatif::ProgressBar;
use std::{
    io::Read,
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, mpsc};
use wincode::Deserialize;

pub const LOG_INTERVAL_SECS: u64 = 2;

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockStats {
    pub blocks: u64,
    pub txs: u64,
    pub bytes: u64,
    pub entries: u64,
}

impl std::ops::AddAssign for BlockStats {
    fn add_assign(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.txs += other.txs;
        self.bytes += other.bytes;
        self.entries += other.entries;
    }
}

pub async fn read_block(epoch: u64, cache_dir: &str, mode: FetchMode) -> Result<BlockStats> {
    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();

    let mut reusable_tx = MaybeUninit::uninit();
    let mut out = Vec::new();

    let mut blocks_count = 0u64;
    let mut tx_count = 0u64;
    let mut bytes_count = 0u64;
    let mut entry_count = 0u64;

    while let Some(block) = car.next_block().await? {
        entry_count += block.entries.len() as u64;

        for entry_cid in block.block()?.entries.iter() {
            let entry_cid = entry_cid?;
            let Node::Entry(entry) = block.decode(entry_cid.hash_bytes())? else {
                continue;
            };

            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                    continue;
                };

                let tx_bytes = match tx.data.next {
                    None => tx.data.data,
                    Some(df_cid) => {
                        let df_cid = df_cid.to_cid()?;
                        let mut reader = block.dataframe_reader(&df_cid);
                        out.clear();
                        reader.read_to_end(&mut out)?;
                        &out
                    }
                };

                VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)?;
                let tx = unsafe { reusable_tx.assume_init_ref() };
                unsafe {
                    std::ptr::drop_in_place(tx as *const _ as *mut VersionedTransaction);
                }
                tx_count += 1;
            }
        }

        blocks_count += 1;
        bytes_count += block
            .entries
            .iter()
            .map(|entry| entry.len() as u64)
            .sum::<u64>();
        drop(block);

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            let elapsed_secs = elapsed.as_secs_f64();
            let blocks_per_sec = if elapsed_secs > 0.0 {
                blocks_count as f64 / elapsed_secs
            } else {
                0.0
            };
            let mb_per_sec = if elapsed_secs > 0.0 {
                bytes_count as f64 / (1024.0 * 1024.0) / elapsed_secs
            } else {
                0.0
            };
            let avg_block_size = if blocks_count > 0 {
                bytes_count / blocks_count
            } else {
                0
            };
            let avg_entry = if blocks_count > 0 {
                entry_count / blocks_count
            } else {
                0
            };
            let tps = match elapsed.as_secs() {
                0 => 0,
                secs => tx_count / secs,
            };
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {avg_block_size} avg blk size | {tps} TPS | {avg_entry} avg entry",
                blocks_count,
                blocks_per_sec,
                mb_per_sec,
            ));
        }
    }

    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let blocks_per_sec = if elapsed_secs > 0.0 {
        blocks_count as f64 / elapsed_secs
    } else {
        0.0
    };
    let mb_per_sec = if elapsed_secs > 0.0 {
        bytes_count as f64 / (1024.0 * 1024.0) / elapsed_secs
    } else {
        0.0
    };
    let avg_block_size = if blocks_count > 0 {
        bytes_count / blocks_count
    } else {
        0
    };
    let tps = match elapsed.as_secs() {
        0 => 0,
        secs => tx_count / secs,
    };
    tracing::info!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {avg_block_size} avg blk size | {tps} TPS",
        blocks_count,
        blocks_per_sec,
        mb_per_sec,
    );

    Ok(BlockStats {
        blocks: blocks_count,
        txs: tx_count,
        bytes: bytes_count,
        entries: entry_count,
    })
}

pub async fn read_block_par(
    epoch: u64,
    cache_dir: &str,
    mode: FetchMode,
    jobs: usize,
) -> Result<BlockStats> {
    use indicatif::ProgressBar;
    use std::time::{Duration, Instant};

    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let blocks_count = Arc::new(AtomicU64::new(0));
    let tx_count = Arc::new(AtomicU64::new(0));
    let bytes_count = Arc::new(AtomicU64::new(0));
    let entry_count = Arc::new(AtomicU64::new(0));

    let (tx, rx) = mpsc::channel::<blockzilla::car_block_reader::CarBlock>(jobs * 2);
    let rx = Arc::new(Mutex::new(rx));

    for _ in 0..jobs {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let tx_count = Arc::clone(&tx_count);
        let bytes_count = Arc::clone(&bytes_count);
        let entry_count = Arc::clone(&entry_count);

        tokio::spawn(async move {
            let mut reusable_tx = MaybeUninit::uninit();
            let mut tmp_buf = Vec::new();

            loop {
                let Some(block) = ({
                    let mut guard = rx.lock().await;
                    guard.recv().await
                }) else {
                    break;
                };

                entry_count.fetch_add(block.entries.len() as u64, Ordering::Relaxed);

                let block_node = match block.block() {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                for entry_cid in block_node.entries.iter() {
                    let entry_cid = match entry_cid {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    let Node::Entry(entry) = (match block.decode(entry_cid.hash_bytes()) {
                        Ok(n) => n,
                        Err(_) => continue,
                    }) else {
                        continue;
                    };

                    for tx_cid in entry.transactions.iter() {
                        let tx_cid = match tx_cid {
                            Ok(c) => c,
                            Err(_) => continue,
                        };
                        let Node::Transaction(tx) = (match block.decode(tx_cid.hash_bytes()) {
                            Ok(n) => n,
                            Err(_) => continue,
                        }) else {
                            continue;
                        };

                        let tx_bytes = if let Some(df_cid) = tx.data.next {
                            let Ok(df_cid) = df_cid.to_cid() else {
                                continue;
                            };
                            let mut reader = block.dataframe_reader(&df_cid);
                            tmp_buf.clear();
                            if reader.read_to_end(&mut tmp_buf).is_err() {
                                continue;
                            }
                            &tmp_buf
                        } else {
                            tx.data.data
                        };

                        if VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)
                            .is_err()
                        {
                            continue;
                        }
                        let tx = unsafe { reusable_tx.assume_init_ref() };
                        unsafe {
                            std::ptr::drop_in_place(tx as *const _ as *mut VersionedTransaction);
                        }
                        tx_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                bytes_count.fetch_add(
                    block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64,
                    Ordering::Relaxed,
                );
                blocks_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;

    while let Some(block) = car.next_block().await? {
        if tx.send(block).await.is_err() {
            break;
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);

            let b = blocks_count.load(Ordering::Relaxed);
            let t = tx_count.load(Ordering::Relaxed);
            let by = bytes_count.load(Ordering::Relaxed);
            let e = entry_count.load(Ordering::Relaxed);

            let elapsed_secs = elapsed.as_secs_f64();
            let blocks_per_sec = if elapsed_secs > 0.0 {
                b as f64 / elapsed_secs
            } else {
                0.0
            };
            let mb_per_sec = if elapsed_secs > 0.0 {
                by as f64 / (1024.0 * 1024.0) / elapsed_secs
            } else {
                0.0
            };

            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS | {} avg entry",
                b,
                blocks_per_sec,
                mb_per_sec,
                if b > 0 { by / b } else { 0 },
                if elapsed.as_secs() > 0 { t / elapsed.as_secs() } else { 0 },
                if b > 0 { e / b } else { 0 },
            ));
        }
    }

    drop(tx);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let elapsed = start.elapsed();
    let b = blocks_count.load(Ordering::Relaxed);
    let t = tx_count.load(Ordering::Relaxed);
    let by = bytes_count.load(Ordering::Relaxed);
    let elapsed_secs = elapsed.as_secs_f64();
    let blocks_per_sec = if elapsed_secs > 0.0 {
        b as f64 / elapsed_secs
    } else {
        0.0
    };
    let mb_per_sec = if elapsed_secs > 0.0 {
        by as f64 / (1024.0 * 1024.0) / elapsed_secs
    } else {
        0.0
    };
    tracing::info!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS",
        b,
        blocks_per_sec,
        mb_per_sec,
        if b > 0 { by / b } else { 0 },
        if elapsed.as_secs() > 0 {
            t / elapsed.as_secs()
        } else {
            0
        },
    );

    Ok(BlockStats {
        blocks: b,
        txs: t,
        bytes: by,
        entries: entry_count.load(Ordering::Relaxed),
    })
}
