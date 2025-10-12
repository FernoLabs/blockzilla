use ahash::AHashSet;
use anyhow::{Context, Result};
use blockzilla::{
    block_stream::{CarBlock, SolanaBlockStream},
    node::Node,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::{prelude::*, ThreadPoolBuilder};
use solana_sdk::pubkey::Pubkey;
use std::{
    fs,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::time::Instant as TokioInstant;
use tracing::{error, warn};

use crate::transaction_parser;

const LOG_INTERVAL_SECS: u64 = 10;

/// Global stats shared across threads
struct GlobalStats {
    planned_bytes: u64,
    done_bytes: u64,
    planned_epochs: u64,
    done_epochs: u64,
}

/// Run parallel extraction with per-CPU lines and global ETA.
pub fn extract_all_pubkeys(base: &PathBuf, out_dir: &PathBuf, max_epoch: u64, max_threads: usize) {
    fs::create_dir_all(out_dir).expect("failed to create output dir");

    let mp = Arc::new(MultiProgress::new());
    let header = mp.add(ProgressBar::new_spinner());
    header.set_style(
        ProgressStyle::with_template("🌍 {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    header.enable_steady_tick(Duration::from_millis(250));

    let start_global = Instant::now();
    let stats = Arc::new(Mutex::new(GlobalStats {
        planned_bytes: 0,
        done_bytes: 0,
        planned_epochs: 0,
        done_epochs: 0,
    }));

    // --- Precompute which epochs exist ---
    let mut epoch_sizes: Vec<u64> = Vec::with_capacity(max_epoch as usize + 1);
    let mut planned_bytes = 0u64;
    let mut planned_epochs = 0u64;
    for epoch in 0..=max_epoch {
        let car = base.join(format!("epoch-{epoch}.car"));
        let bin = out_dir.join(format!("pubkeys-{epoch:04}.bin"));
        let size = fs::metadata(&car).map(|m| m.len()).unwrap_or(0);
        epoch_sizes.push(size);
        if car.exists() && !bin.exists() {
            planned_bytes += size;
            planned_epochs += 1;
        }
    }
    {
        let mut s = stats.lock().unwrap();
        s.planned_bytes = planned_bytes;
        s.planned_epochs = planned_epochs;
    }

    // --- Background thread for global ETA ---
    {
        let stats = Arc::clone(&stats);
        let header = header.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(2));
            let s = stats.lock().unwrap();

            // Graceful warm-up
            if s.done_epochs == 0 && start_global.elapsed().as_secs() < 10 {
                header.set_message("🌍 warming up... estimating throughput...");
                continue;
            }

            if s.planned_bytes == 0 {
                header.set_message("Nothing to process — all epochs skipped.");
                break;
            }

            let elapsed = start_global.elapsed().as_secs_f64().max(0.001);
            let mb_done = s.done_bytes as f64 / 1_000_000.0;
            let mb_total = s.planned_bytes as f64 / 1_000_000.0;
            let speed = mb_done / elapsed;
            let remaining_mb = (mb_total - mb_done).max(0.0);
            let eta_min = if speed > 0.0 {
                (remaining_mb / speed) / 60.0
            } else {
                0.0
            };
            let speed_disp = if speed > 1000.0 {
                format!("{:.2} GB/s", speed / 1000.0)
            } else {
                format!("{:.1} MB/s", speed)
            };

            header.set_message(format!(
                "Epochs {}/{} | {:.1}/{:.1} GB read | {} | ETA {:.1} min",
                s.done_epochs, s.planned_epochs,
                mb_done / 1000.0, mb_total / 1000.0,
                speed_disp, eta_min
            ));

            if s.done_epochs >= s.planned_epochs {
                break;
            }
        });
    }

    // --- One progress line per CPU thread ---
    let bars: Vec<_> = (0..max_threads)
        .map(|i| {
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::with_template("🧮 [CPU {prefix}] {spinner:.cyan} {wide_msg}")
                    .unwrap()
                    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
            );
            pb.set_prefix(format!("{i:02}"));
            pb.enable_steady_tick(Duration::from_millis(250));
            pb
        })
        .collect();

    // --- Thread pool for parallel extraction ---
    let pool = ThreadPoolBuilder::new()
        .num_threads(max_threads)
        .build()
        .expect("failed to build rayon pool");

    pool.install(|| {
        (0..=max_epoch).into_par_iter().for_each(|epoch| {
            let thread_id = rayon::current_thread_index().unwrap_or(0);
            let pb = &bars[thread_id];

            let file_path = base.join(format!("epoch-{epoch}.car"));
            let bin_path = out_dir.join(format!("pubkeys-{epoch:04}.bin"));
            let file_size = epoch_sizes[epoch as usize];

            // --- Skip logic ---
            if !file_path.exists() {
                pb.set_message(format!("⚠️  epoch {epoch:04} missing"));
                return;
            }
            if bin_path.exists() {
                pb.set_message(format!("⏭️  epoch {epoch:04} (already done)"));
                return;
            }

            pb.set_message(format!(
                "epoch {epoch:04} — reading {:.1} MB...",
                file_size as f64 / 1_000_000.0
            ));
            let start = Instant::now();

            let res = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(extract_unique_pubkeys_with_pb(
                    &file_path, &bin_path, epoch, pb.clone(),
                ));

            let mut s = stats.lock().unwrap();
            match res {
                Ok((blocks, count)) => {
                    s.done_epochs += 1;
                    s.done_bytes += file_size;
                    pb.set_message(format!(
                        "✅ epoch {epoch:04} — {:.1} MB, {blocks} blk, {count} keys ({:.1?})",
                        file_size as f64 / 1_000_000.0,
                        start.elapsed()
                    ));
                }
                Err(e) => {
                    s.done_epochs += 1;
                    pb.set_message(format!("❌ epoch {epoch:04} — {e:?}"));
                    error!(epoch, error=?e, "Epoch failed");
                }
            }
        });
    });

    let _ = mp.clear();

    // --- Final summary ---
    let s = stats.lock().unwrap();
    let gb = s.done_bytes as f64 / 1_000_000_000.0;
    println!(
        "\n📊 Summary: {} epochs processed ({:.2} GB read) in {:.1?}",
        s.done_epochs,
        gb,
        start_global.elapsed()
    );
    println!("✅ Extraction complete.");
}

/// Extract unique pubkeys from one epoch and return (blocks, unique_count)
pub async fn extract_unique_pubkeys_with_pb(
    path: &Path,
    bin_path: &Path,
    epoch: u64,
    pb: ProgressBar,
) -> Result<(u64, usize)> {
    let mut set: AHashSet<Pubkey> = AHashSet::with_capacity(30_000_000);

    let file = tokio::fs::File::open(path).await?;
    let mut stream = SolanaBlockStream::new(file).await?;

    let start = TokioInstant::now();
    let mut last_log = start;
    let mut blocks = 0u64;

    while let Some(cb) = stream.next_solana_block().await? {
        extract_transactions(&cb, &mut set)?;
        blocks += 1;

        let now = TokioInstant::now();
        if now.duration_since(last_log).as_secs() >= LOG_INTERVAL_SECS {
            let elapsed = now.duration_since(start).as_secs_f64();
            let blk_per_s = (blocks as f64) / elapsed;
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>9} unique",
                blocks,
                blk_per_s,
                set.len()
            ));
            last_log = now;
        }
    }

    dump_pubkeys_to_bin(bin_path, &set)?;
    Ok((blocks, set.len()))
}

/// Extract pubkeys from transactions only
fn extract_transactions(cb: &CarBlock, out: &mut AHashSet<Pubkey>) -> Result<()> {
    for entry in cb.entries.get_block_entries() {
        for tx_cid in &entry.transactions {
            let Some(Node::Transaction(tx)) = cb.entries.get(&tx_cid.0) else {
                continue;
            };
            let tx_bytes = if tx.data.next.is_none() {
                tx.data.data
            } else {
                &cb.merge_dataframe(tx.data)?
            };
            transaction_parser::parse_account_keys_only(tx_bytes, out)?;
        }
    }
    Ok(())
}

/// Write all pubkeys into a compact binary file
fn dump_pubkeys_to_bin(path: &Path, set: &AHashSet<Pubkey>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    let count = set.len() as u64;
    writer.write_all(&count.to_le_bytes())?;
    for pk in set {
        writer.write_all(&pk.to_bytes())?;
    }
    writer.flush()?;
    Ok(())
}
