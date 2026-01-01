use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{self, BufReader, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::info;

use blockzilla_format::{PostcardFramedReader, compact::CompactBlockRecord};

fn fmt_dur(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

pub fn dump_log_strings(
    path: &PathBuf,
    out_path: Option<&Path>,
    limit_blocks: Option<u64>,
    progress_every: u64,
    max_lines: u64,
) -> Result<()> {
    info!("dump-log-strings input={} (framed)", path.display());

    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = PostcardFramedReader::new(BufReader::with_capacity(64 << 20, f));

    let mut out: Box<dyn Write> = match out_path {
        Some(p) => Box::new(io::BufWriter::with_capacity(
            64 << 20,
            File::create(p).with_context(|| format!("create {}", p.display()))?,
        )),
        None => Box::new(io::BufWriter::with_capacity(64 << 20, io::stdout())),
    };

    let start = Instant::now();
    let mut blocks = 0u64;
    let mut lines_out = 0u64;

    while let Some(rec) = reader
        .read::<CompactBlockRecord>()
        .context("postcard decode CompactBlockRecord")?
    {
        if let Some(max) = limit_blocks {
            if blocks >= max {
                break;
            }
        }
        if max_lines != 0 && lines_out >= max_lines {
            break;
        }

        for tx in rec.txs.iter() {
            if max_lines != 0 && lines_out >= max_lines {
                break;
            }

            let Some(meta) = tx.metadata.as_ref() else {
                continue;
            };
            let Some(logs) = meta.logs.as_ref() else {
                continue;
            };

            for s in logs.strings.strings.iter() {
                if max_lines != 0 && lines_out >= max_lines {
                    break;
                }
                writeln!(out, "{s}")?;
                lines_out += 1;
            }
        }

        blocks += 1;

        if progress_every > 0 && blocks % progress_every == 0 {
            let elapsed = start.elapsed().as_secs().max(1);
            let blk_s = blocks as f64 / elapsed as f64;
            let line_s = lines_out as f64 / elapsed as f64;

            let eta = limit_blocks.map(|max| {
                let remain = max.saturating_sub(blocks);
                if blk_s > 0.0 {
                    fmt_dur((remain as f64 / blk_s) as u64)
                } else {
                    "--:--".to_string()
                }
            });

            info!(
                blocks = blocks,
                lines_out = lines_out,
                blk_per_s = blk_s,
                lines_per_s = line_s,
                elapsed = fmt_dur(elapsed),
                eta = eta.as_deref().unwrap_or(""),
                "dump-log-strings progress"
            );
        }
    }

    out.flush().context("flush output")?;
    info!("done blocks={} lines_out={}", blocks, lines_out);
    Ok(())
}
