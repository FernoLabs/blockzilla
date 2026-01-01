use anyhow::{Context, Result};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::BufReader,
    mem::Discriminant,
    path::PathBuf,
    time::Instant,
};
use tracing::info;

use blockzilla_format::{
    CompactBlockRecord, CompactTxWithMeta, PostcardFramedReader,
    compact::{CompactMessage, CompactTransaction},
};

// Adjust these imports if your log types live elsewhere.
use blockzilla_format::log::LogEvent;

#[derive(Default, Debug, Clone)]
pub struct LogEventStat {
    pub count: u64,
    pub bytes: u64, // sum(postcard serialized_size(event))
}

#[derive(Default, Debug, Clone)]
pub struct EpochReport {
    // counts
    pub blocks: u64,
    pub txs: u64,
    pub metas_some: u64,

    // original size accounting
    pub bytes_header: u64,
    pub bytes_tx: u64,
    pub bytes_meta: u64,
    pub bytes_frame_prefix: u64, // 4 * blocks

    // compactness / composition (tx only)
    pub instr_data_raw_bytes: u64, // sum(ix.data.len())
    pub tx_serialized_bytes: u64,  // sum(postcard size of tx)

    // tx breakdown buckets (serialized sizes)
    pub sigs_bytes: u64,

    pub msg_header_bytes: u64,
    pub msg_recent_blockhash_bytes: u64,
    pub msg_account_keys_bytes: u64,

    pub ix_container_bytes: u64, // serialized size of Vec<CompactInstruction>
    pub ix_accounts_bytes: u64,  // serialized size of all ix.accounts Vecs
    pub ix_data_bytes: u64,      // serialized size of all ix.data Vecs

    pub atl_container_bytes: u64, // serialized size of address_table_lookups Vec
    pub atl_payload_bytes: u64,   // serialized size of ATL fields

    // meta breakdown (extra details)
    pub bytes_meta_logs: u64,            // serialized size of meta.logs
    pub bytes_meta_logs_strings: u64,    // serialized size of logs.strings
    pub bytes_meta_logs_events: u64,     // serialized size of logs.events Vec container
    pub bytes_meta_logs_events_sum: u64, // sum(serialized size of each event)

    pub meta_logs_some: u64,
    pub meta_log_lines: u64,
    pub meta_log_events: u64,

    // name -> stats
    pub meta_log_event_stats: BTreeMap<String, LogEventStat>,
}

impl EpochReport {
    pub fn bytes_total_payload(&self) -> u64 {
        self.bytes_header + self.bytes_tx + self.bytes_meta
    }

    pub fn bytes_total_including_prefix(&self) -> u64 {
        self.bytes_total_payload() + self.bytes_frame_prefix
    }

    /// "Good compact tx" score: raw instruction bytes / serialized tx bytes
    pub fn compactness(&self) -> f64 {
        if self.tx_serialized_bytes == 0 {
            0.0
        } else {
            self.instr_data_raw_bytes as f64 / self.tx_serialized_bytes as f64
        }
    }

    pub fn ix_overhead_bytes_approx(&self) -> u64 {
        self.ix_container_bytes
            .saturating_sub(self.ix_accounts_bytes)
            .saturating_sub(self.ix_data_bytes)
    }
}

#[inline]
fn sz<T: serde::Serialize>(v: &T) -> Result<u64> {
    Ok(postcard::experimental::serialized_size(v).context("postcard serialized_size")? as u64)
}

#[inline]
fn instr_data_raw_len(tx: &CompactTransaction) -> u64 {
    let ixs = match &tx.message {
        CompactMessage::Legacy(m) => &m.instructions,
        CompactMessage::V0(m) => &m.instructions,
    };
    ixs.iter().map(|ix| ix.data.len() as u64).sum()
}

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

#[inline]
fn log_event_kind_name(e: &LogEvent) -> String {
    // Parse "Variant(...)" -> "Variant"
    let s = format!("{:?}", e);
    s.split(['(', '{']).next().unwrap_or(&s).to_string()
}

#[derive(Default)]
struct DiscAgg {
    name: String,
    count: u64,
    bytes: u64,
}

pub fn analyze_epoch_file(
    path: &PathBuf,
    progress_every: u64,       // blocks, 0 disables
    limit_blocks: Option<u64>, // optional early stop + ETA
) -> Result<EpochReport> {
    info!("analyze-epoch input={} (framed)", path.display());

    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = PostcardFramedReader::new(BufReader::with_capacity(64 << 20, f));

    let start = Instant::now();
    let mut rep = EpochReport::default();
    let mut next_progress = progress_every.max(1);

    // Fast aggregation keyed by discriminant (avoid allocating String per event)
    let mut disc_map: HashMap<Discriminant<LogEvent>, DiscAgg> = HashMap::new();

    while let Some(block) = reader
        .read::<CompactBlockRecord>()
        .context("decode CompactBlockRecord")?
    {
        if let Some(max) = limit_blocks {
            if rep.blocks >= max {
                break;
            }
        }

        rep.blocks += 1;
        rep.bytes_frame_prefix += 4;

        rep.bytes_header += sz(&block.header)?;
        rep.txs += block.txs.len() as u64;

        for CompactTxWithMeta { tx, metadata } in &block.txs {
            // tx sizing
            let tx_sz = sz(&tx)?;
            rep.bytes_tx += tx_sz;

            rep.tx_serialized_bytes += tx_sz;
            rep.instr_data_raw_bytes += instr_data_raw_len(tx);

            rep.sigs_bytes += sz(&tx.signatures)?;

            match &tx.message {
                CompactMessage::Legacy(m) => {
                    rep.msg_header_bytes += sz(&m.header)?;
                    rep.msg_recent_blockhash_bytes += sz(&m.recent_blockhash)?;
                    rep.msg_account_keys_bytes += sz(&m.account_keys)?;

                    rep.ix_container_bytes += sz(&m.instructions)?;
                    for ix in &m.instructions {
                        rep.ix_accounts_bytes += sz(&ix.accounts)?;
                        rep.ix_data_bytes += sz(&ix.data)?;
                    }
                }
                CompactMessage::V0(m) => {
                    rep.msg_header_bytes += sz(&m.header)?;
                    rep.msg_recent_blockhash_bytes += sz(&m.recent_blockhash)?;
                    rep.msg_account_keys_bytes += sz(&m.account_keys)?;

                    rep.ix_container_bytes += sz(&m.instructions)?;
                    for ix in &m.instructions {
                        rep.ix_accounts_bytes += sz(&ix.accounts)?;
                        rep.ix_data_bytes += sz(&ix.data)?;
                    }

                    rep.atl_container_bytes += sz(&m.address_table_lookups)?;
                    for l in &m.address_table_lookups {
                        rep.atl_payload_bytes += sz(&l.account_key)?;
                        rep.atl_payload_bytes += sz(&l.writable_indexes)?;
                        rep.atl_payload_bytes += sz(&l.readonly_indexes)?;
                    }
                }
            }

            // meta sizing (details)
            let Some(meta) = metadata.as_ref() else {
                continue;
            };
            rep.metas_some += 1;
            rep.bytes_meta += sz(meta)?;

            let Some(logs) = meta.logs.as_ref() else {
                continue;
            };

            rep.meta_logs_some += 1;
            rep.bytes_meta_logs += sz(logs)?;

            // Assumes CompactLogStream has these fields:
            // logs.strings.strings: Vec<String>
            // logs.events: Vec<LogEvent>
            rep.bytes_meta_logs_strings += sz(&logs.strings)?;
            rep.meta_log_lines += logs.strings.strings.len() as u64;

            rep.bytes_meta_logs_events += sz(&logs.events)?;
            rep.meta_log_events += logs.events.len() as u64;

            for ev in logs.events.iter() {
                let ev_sz = sz(ev)?;
                rep.bytes_meta_logs_events_sum += ev_sz;

                let d = std::mem::discriminant(ev);
                let entry = disc_map.entry(d).or_insert_with(|| DiscAgg {
                    name: log_event_kind_name(ev),
                    ..DiscAgg::default()
                });
                entry.count += 1;
                entry.bytes += ev_sz;
            }
        }

        if rep.blocks >= next_progress {
            let elapsed = start.elapsed().as_secs().max(1);
            let blk_s = rep.blocks as f64 / elapsed as f64;
            let tx_s = rep.txs as f64 / elapsed as f64;

            let eta = limit_blocks.map(|max| {
                let remain = max.saturating_sub(rep.blocks);
                if blk_s > 0.0 {
                    fmt_dur((remain as f64 / blk_s) as u64)
                } else {
                    "--:--".to_string()
                }
            });
            info!(
                blocks = rep.blocks,
                blk_per_s = blk_s,
                tx_per_s = tx_s,
                elapsed = fmt_dur(elapsed),
                eta = eta.as_deref().unwrap_or(""),
                "analyze-epoch progress"
            );
            next_progress += progress_every;
        }
    }

    // finalize per-kind stats with stable ordering
    for (_disc, agg) in disc_map {
        rep.meta_log_event_stats
            .entry(agg.name)
            .and_modify(|s| {
                s.count += agg.count;
                s.bytes += agg.bytes;
            })
            .or_insert(LogEventStat {
                count: agg.count,
                bytes: agg.bytes,
            });
    }

    info!(
        "analyze-epoch done blocks={} txs={} metas_some={} compactness={:.4}",
        rep.blocks,
        rep.txs,
        rep.metas_some,
        rep.compactness()
    );
    Ok(rep)
}

pub fn print_epoch_report(rep: &EpochReport) {
    // original size report
    let payload_total = rep.bytes_total_payload() as f64;
    let pct_payload = |x: u64| {
        if payload_total > 0.0 {
            (x as f64) * 100.0 / payload_total
        } else {
            0.0
        }
    };

    println!("blocks={}", rep.blocks);
    println!("txs={}", rep.txs);
    println!("metas_some={}", rep.metas_some);
    println!("payload_bytes_total={}", rep.bytes_total_payload());
    println!(
        "file_bytes_total_including_u32_prefix={}",
        rep.bytes_total_including_prefix()
    );
    println!();

    println!(
        "{:>14} {:>8.2}%  {}",
        rep.bytes_header,
        pct_payload(rep.bytes_header),
        "header"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.bytes_tx,
        pct_payload(rep.bytes_tx),
        "tx"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.bytes_meta,
        pct_payload(rep.bytes_meta),
        "meta"
    );
    println!(
        "{:>14} {:>8}    {}",
        rep.bytes_frame_prefix, "", "frame_prefix_u32"
    );

    // tx composition / compactness
    println!();
    println!("tx_serialized_bytes={}", rep.tx_serialized_bytes);
    println!("instr_data_raw_bytes={}", rep.instr_data_raw_bytes);
    println!(
        "compactness(instr_data_raw / tx_serialized)={:.4}",
        rep.compactness()
    );
    println!();

    let total_tx = rep.tx_serialized_bytes as f64;
    let pct_tx = |x: u64| {
        if total_tx > 0.0 {
            (x as f64) * 100.0 / total_tx
        } else {
            0.0
        }
    };

    println!(
        "{:>14} {:>8.2}%  {}",
        rep.sigs_bytes,
        pct_tx(rep.sigs_bytes),
        "signatures"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.msg_header_bytes,
        pct_tx(rep.msg_header_bytes),
        "msg.header"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.msg_recent_blockhash_bytes,
        pct_tx(rep.msg_recent_blockhash_bytes),
        "msg.recent_blockhash"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.msg_account_keys_bytes,
        pct_tx(rep.msg_account_keys_bytes),
        "msg.account_keys"
    );

    println!(
        "{:>14} {:>8.2}%  {}",
        rep.ix_container_bytes,
        pct_tx(rep.ix_container_bytes),
        "ix.container(total)"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.ix_accounts_bytes,
        pct_tx(rep.ix_accounts_bytes),
        "ix.accounts"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.ix_data_bytes,
        pct_tx(rep.ix_data_bytes),
        "ix.data(serialized)"
    );
    println!(
        "{:>14} {:>8.2}%  {}",
        rep.ix_overhead_bytes_approx(),
        pct_tx(rep.ix_overhead_bytes_approx()),
        "ix.overhead(approx)"
    );

    if rep.atl_container_bytes > 0 || rep.atl_payload_bytes > 0 {
        println!(
            "{:>14} {:>8.2}%  {}",
            rep.atl_container_bytes,
            pct_tx(rep.atl_container_bytes),
            "atl.container"
        );
        println!(
            "{:>14} {:>8.2}%  {}",
            rep.atl_payload_bytes,
            pct_tx(rep.atl_payload_bytes),
            "atl.payload"
        );
    }

    // meta details
    println!();
    println!("meta_bytes_total={}", rep.bytes_meta);
    if rep.metas_some > 0 {
        println!("meta_logs_some={}", rep.meta_logs_some);
        println!("meta_log_lines={}", rep.meta_log_lines);
        println!("meta_log_events={}", rep.meta_log_events);
        println!(
            "meta_logs_bytes_total={} (strings={} events_container={} events_sum={})",
            rep.bytes_meta_logs,
            rep.bytes_meta_logs_strings,
            rep.bytes_meta_logs_events,
            rep.bytes_meta_logs_events_sum
        );

        if !rep.meta_log_event_stats.is_empty() {
            let mut v: Vec<_> = rep.meta_log_event_stats.iter().collect();
            v.sort_by_key(|(_k, s)| std::cmp::Reverse(s.bytes));

            println!();
            println!("top LogEvent kinds by bytes:");
            for (k, s) in v.into_iter().take(40) {
                println!("  {:>10} {:>14}  {}", s.count, s.bytes, k);
            }
        }
    }
}
