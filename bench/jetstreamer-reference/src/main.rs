#[path = "../../car-export/shared.rs"]
mod common_export;
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use futures_util::future::BoxFuture;
use jetstreamer_firehose::{
    firehose::{
        firehose, BlockData, FirehoseErrorContext, OnEntryFn, OnRewardFn, Stats, StatsTracking,
        TransactionData,
    },
    SharedError,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::broadcast;

type Callback = BoxFuture<'static, Result<(), SharedError>>;
const ABSENT: u64 = u64::MAX;

#[repr(align(64))]
struct Worker {
    pending: AtomicU64,
    slot: AtomicU64,
    blocks: AtomicU64,
    transactions: AtomicU64,
    votes: AtomicU64,
    failed: AtomicU64,
    export_buffer: Mutex<Vec<u8>>,
}

impl Worker {
    fn new() -> Self {
        Self {
            pending: AtomicU64::new(0),
            slot: AtomicU64::new(ABSENT),
            blocks: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            votes: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            export_buffer: Mutex::new(Vec::new()),
        }
    }
}

struct State {
    workers: Vec<Worker>,
    slots: Vec<AtomicU64>,
    start: u64,
    end: u64,
    decoded: bool,
    export: common_export::Export,
    stats_transactions: AtomicU64,
    errors: Mutex<Vec<String>>,
    stop: broadcast::Sender<()>,
}

impl State {
    fn fail(&self, error: String) {
        let mut errors = self.errors.lock().unwrap();
        if errors.len() < 32 {
            errors.push(error);
        }
        let _ = self.stop.send(());
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = BTreeMap::new();
    let mut input = std::env::args().skip(1);
    while let Some(key) = input.next() {
        if key == "--help" {
            println!("jetstreamer-count-reference --epoch N --http-base URL --output NEW_JSON --plan PLAN_JSON --export NEW_BIN [--index-base URL] [--workers 12] [--start-slot N] [--end-slot-exclusive N] [--mode decoded]\nUses upstream Jetstreamer 0.7.0, parallel raw HTTP CAR only. No local-file or outer-zstd adapter. Only decoded mode is supported: complete transaction callbacks are checked against Entry counts and an external canonical block plan. Upstream statistics are sampled telemetry, not final counts. No signature verification feature is enabled.");
            return Ok(());
        }
        if ![
            "--epoch",
            "--http-base",
            "--output",
            "--index-base",
            "--workers",
            "--start-slot",
            "--end-slot-exclusive",
            "--mode",
            "--plan",
            "--export",
        ]
        .contains(&key.as_str())
        {
            return Err(format!("unknown option {key}").into());
        }
        let value = input
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if args.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate {key}").into());
        }
    }
    let required = |name: &str| args.get(name).ok_or_else(|| format!("required: {name}"));
    let epoch: u64 = required("--epoch")?.parse()?;
    let epoch_start = epoch.checked_mul(432_000).ok_or("epoch overflow")?;
    let epoch_end = epoch_start.checked_add(432_000).ok_or("epoch overflow")?;
    let start: u64 = args
        .get("--start-slot")
        .map(|x| x.parse())
        .transpose()?
        .unwrap_or(epoch_start);
    let end: u64 = args
        .get("--end-slot-exclusive")
        .map(|x| x.parse())
        .transpose()?
        .unwrap_or(epoch_end);
    let workers: usize = args
        .get("--workers")
        .map(|x| x.parse())
        .transpose()?
        .unwrap_or(12);
    let mode = args.get("--mode").map(String::as_str).unwrap_or("decoded");
    if !(1..=256).contains(&workers) || start < epoch_start || end > epoch_end || end <= start {
        return Err("invalid worker count or epoch-contained half-open slot range".into());
    }
    if mode != "decoded" {
        return Err("mode must be decoded; nodes mode is disabled because upstream statistics do not guarantee a final transaction-node count".into());
    }
    let plan: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(required("--plan")?)?)?;
    let expected: Vec<[u64; 2]> = serde_json::from_value(plan["block_transaction_rows"].clone())?;
    if plan["epoch"].as_u64() != Some(epoch)
        || plan["start_slot"].as_u64() != Some(start)
        || plan["end_slot_exclusive"].as_u64() != Some(end)
        || expected.is_empty()
        || !expected.windows(2).all(|r| r[0][0] < r[1][0])
    {
        return Err("plan does not match requested range".into());
    }
    let base = required("--http-base")?;
    if !(base.starts_with("https://") || base.starts_with("http://")) || !base.ends_with('/') {
        return Err("http-base must be an HTTP(S) URL with a final slash".into());
    }
    let index = args.get("--index-base").unwrap_or(base);
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(required("--output")?)?;
    // No runtime or background threads exist while the process environment is configured.
    std::env::set_var("JETSTREAMER_HTTP_BASE_URL", base);
    std::env::set_var("JETSTREAMER_COMPACT_INDEX_BASE_URL", index);
    let total_started = Instant::now();
    let export = common_export::Export::create(std::path::Path::new(required("--export")?))?;
    let (stop, receiver) = broadcast::channel(16);
    let state = Arc::new(State {
        workers: (0..workers).map(|_| Worker::new()).collect(),
        slots: (start..end).map(|_| AtomicU64::new(ABSENT)).collect(),
        start,
        end,
        decoded: mode == "decoded",
        export,
        stats_transactions: AtomicU64::new(0),
        errors: Mutex::new(Vec::new()),
        stop,
    });

    let tx_state = Arc::clone(&state);
    let on_tx = move |thread: usize, tx: TransactionData| -> Callback {
        let s = &tx_state;
        let w = &s.workers[thread];
        let index = w.pending.fetch_add(1, Relaxed);
        let old_slot = w.slot.swap(tx.slot, Relaxed);
        if tx.slot < s.start
            || tx.slot >= s.end
            || tx.transaction_slot_index as u64 != index
            || (old_slot != ABSENT && old_slot != tx.slot)
        {
            s.fail(format!("transaction order/range violation: worker={thread} slot={} index={} expected={index}", tx.slot, tx.transaction_slot_index));
        }
        let encoded = common_export::encode(
            &mut w.export_buffer.lock().unwrap(),
            common_export::Transaction {
                slot: tx.slot,
                index: tx.transaction_slot_index as u64,
                signature: tx.signature.as_ref(),
                message_hash: tx.message_hash.as_ref(),
                vote: tx.is_vote,
                failed: tx.transaction_status_meta.status.is_err(),
                fee: tx.transaction_status_meta.fee,
                pre_balances: &tx.transaction_status_meta.pre_balances,
                post_balances: &tx.transaction_status_meta.post_balances,
            },
        );
        if let Err(error) = encoded {
            s.fail(format!("export encode: {error}"));
        }
        w.transactions.fetch_add(1, Relaxed);
        w.votes.fetch_add(u64::from(tx.is_vote), Relaxed);
        w.failed.fetch_add(
            u64::from(tx.transaction_status_meta.status.is_err()),
            Relaxed,
        );
        Box::pin(async { Ok(()) })
    };
    let block_state = Arc::clone(&state);
    let on_block = move |thread: usize, block: BlockData| -> Callback {
        if let BlockData::Block {
            slot,
            executed_transaction_count,
            ..
        } = block
        {
            let s = &block_state;
            if slot < s.start || slot >= s.end {
                s.fail(format!("block outside requested range: {slot}"));
            } else {
                let previous = s.slots[(slot - s.start) as usize].compare_exchange(
                    ABSENT,
                    executed_transaction_count,
                    Relaxed,
                    Relaxed,
                );
                if previous.is_err() {
                    s.fail(format!("duplicate real block: {slot}"));
                }
                let w = &s.workers[thread];
                let mut bytes = w.export_buffer.lock().unwrap();
                if let Err(error) = s.export.block(slot, executed_transaction_count, &bytes) {
                    s.fail(format!("export block: {error}"));
                }
                bytes.clear();
                w.blocks.fetch_add(1, Relaxed);
                let callback_count = w.pending.swap(0, Relaxed);
                let callback_slot = w.slot.swap(ABSENT, Relaxed);
                if s.decoded
                    && (callback_count != executed_transaction_count
                        || (callback_count > 0 && callback_slot != slot))
                {
                    s.fail(format!("block/callback count mismatch at {slot}: entries={executed_transaction_count} callbacks={callback_count}"));
                }
            }
        }
        // PossibleLeaderSkipped is an inference, never a real block in the output plan.
        Box::pin(async { Ok(()) })
    };
    let stats_state = Arc::clone(&state);
    let on_stats = move |_thread: usize, stats: Stats| -> Callback {
        stats_state
            .stats_transactions
            .fetch_max(stats.transactions_processed, Relaxed);
        Box::pin(async { Ok(()) })
    };
    let error_state = Arc::clone(&state);
    let on_error = move |_thread: usize, error: FirehoseErrorContext| -> Callback {
        error_state.fail(format!(
            "upstream retry invalidates sample: slot={} {}",
            error.slot, error.error_message
        ));
        Box::pin(async { Ok(()) })
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    let started = Instant::now();
    let result = runtime.block_on(firehose(
        workers as u64,
        false,
        false,
        None,
        start..end,
        Some(on_block),
        if state.decoded { Some(on_tx) } else { None },
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        Some(on_error),
        Some(StatsTracking {
            on_stats,
            tracking_interval_slots: 10_000,
        }),
        Some(receiver),
    ));
    let elapsed = started.elapsed().as_secs_f64();
    if let Err(error) = result {
        state.fail(format!("firehose failed: {error:?}"));
    }
    let rows: Vec<_> = state
        .slots
        .iter()
        .enumerate()
        .filter_map(|(i, count)| {
            let count = count.load(Relaxed);
            (count != ABSENT).then_some([start + i as u64, count])
        })
        .collect();
    let transactions: u64 = rows.iter().map(|row| row[1]).sum();
    let stats_transactions = state.stats_transactions.load(Relaxed);
    // Upstream may complete a range before its final Stats callback. These
    // snapshots are telemetry only; completed callbacks and the external exact
    // block plan establish the count contract for decoded mode.
    let transaction_callbacks: u64 = state
        .workers
        .iter()
        .map(|worker| worker.transactions.load(Relaxed))
        .sum();
    if transaction_callbacks != transactions {
        state.fail(format!(
            "transaction-callback/entry mismatch: callbacks={transaction_callbacks} entries={transactions}"
        ));
    }
    let worker_rows: Vec<_> = state.workers.iter().enumerate().map(|(i, w)| {
        if w.pending.load(Relaxed) != 0 { state.fail(format!("unfinished block in worker {i}")); }
            json!({"worker": i, "blocks": w.blocks.load(Relaxed), "transaction_callbacks": w.transactions.load(Relaxed),
            "simple_vote_callbacks": w.votes.load(Relaxed), "failed_status_callbacks": w.failed.load(Relaxed)})
    }).collect();
    if rows != expected {
        state.fail("block rows differ from canonical plan".into());
    }
    let mut export_bytes = None;
    if state.errors.lock().unwrap().is_empty() {
        match state.export.finish(&expected) {
            Ok(bytes) => export_bytes = Some(bytes),
            Err(error) => state.fail(format!("export finish: {error}")),
        }
    }
    drop(runtime);
    let total_seconds = total_started.elapsed().as_secs_f64();
    let errors = state.errors.lock().unwrap();
    serde_json::to_writer_pretty(
        &mut output,
        &json!({
            "schema": "jetstreamer-common-export-reference-v1",
            "export_schema":common_export::SCHEMA, "export_bytes":export_bytes, "total_seconds":total_seconds, "allocator":"mimalloc", "upstream_version": "0.7.0",
            "upstream_commit": "cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24", "mode": mode,
            "epoch": epoch, "start_slot": start, "end_slot_exclusive": end,
            "http_base": base, "index_base": index, "processing_workers": workers,
        "sequential": false, "scan_seconds": elapsed,
        "workers_with_real_blocks": state.workers.iter().filter(|w| w.blocks.load(Relaxed) > 0).count(),
        "peak_active_workers": null,
            "blocks": rows.len(), "transactions": transactions,
            "transaction_callbacks": transaction_callbacks,
            "sampled_transaction_nodes_from_upstream_stats": stats_transactions,
            "sampled_upstream_stats_are_final": false,
            "stats_tracking_interval_slots": 10_000,
            "retry_observation": "reported firehose errors invalidate the sample; deliberate connection recycling and lower-level transport retries are not counted here",
            "worker_counters": worker_rows, "block_transaction_rows": rows,
            "errors": *errors, "valid": errors.is_empty(),
            "signature_verification": false, "outer_car_zstd": false,
            "metadata_absence_coverage": "unavailable: upstream maps empty metadata to default status",
            "network_bytes": null, "decoded_bytes": null,
            "plan_parity": "external comparison required"
        }),
    )?;
    writeln!(output)?;
    if !errors.is_empty() {
        return Err("invalid benchmark; inspect report errors".into());
    }
    Ok(())
}
