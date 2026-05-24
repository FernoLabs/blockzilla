use anyhow::{Context, Result};
use axum::{Router, routing::post};
use clap::{Parser, Subcommand, ValueEnum};
use of_car_reader::slot_ranges::write_slot_ranges_v2_raw;
use of_get_block_worker::archive::{Archive, FetchedBlock};
use of_get_block_worker::car_to_json_stream::RenderProfile;
use of_get_block_worker::get_block::{
    GetBlockConfig, GetBlockEncoding, TransactionDetails, render_get_block_json_bytes,
    render_get_block_json_bytes_profiled, render_get_block_time,
};
use of_get_block_worker::index::build_slot_ranges_v2;
use of_get_block_worker::rpc::{self, RpcState};
use of_get_block_worker::source::SourceArgs;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap, fs::File, io::Write, net::SocketAddr, path::Path, path::PathBuf,
    sync::Arc, time::Instant,
};

#[derive(Parser)]
#[command(name = "of-get-block-worker")]
#[command(about = "Serve Solana JSON-RPC responses from Old Faithful CAR files")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve a Solana JSON-RPC HTTP endpoint from CAR files and slot indexes.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8899")]
        bind: SocketAddr,
        /// Directory containing slot-index/epoch-N-slot-ranges-v2.raw files.
        #[arg(long)]
        index_dir: PathBuf,
        /// Optional upstream Solana JSON-RPC endpoint for methods not served locally.
        #[arg(long)]
        proxy_url: Option<String>,
        #[command(flatten)]
        source: SourceArgs,
    },

    /// Build a v2 slot range index with previousBlockhash side data.
    BuildSlotIndexV2 {
        #[arg(long)]
        epoch: u64,
        /// Output file, usually slot-index/epoch-N-slot-ranges-v2.raw.
        #[arg(long)]
        out: PathBuf,
        /// Base58 blockhash immediately before the first produced block in this epoch.
        #[arg(long)]
        seed_previous_blockhash: Option<String>,
        #[arg(long, default_value_t = of_slot_ranges::DEFAULT_MAX_BUCKET_PAYLOAD_BYTES)]
        max_bucket_payload_bytes: usize,
        #[arg(long)]
        allow_node_read_fallback: bool,
        #[command(flatten)]
        source: SourceArgs,
    },

    /// Benchmark the native archive/render path without a Worker or HTTP server.
    Bench {
        /// Directory containing slot-index/epoch-N-slot-ranges-v2.raw files.
        #[arg(long)]
        index_dir: PathBuf,
        /// Slot to benchmark. May be repeated.
        #[arg(long = "slot")]
        slots: Vec<u64>,
        /// Optional file containing slots separated by whitespace, commas, or newlines.
        #[arg(long)]
        slots_file: Option<PathBuf>,
        /// Response modes to benchmark. Defaults to full, accounts, signatures, none, block-time.
        #[arg(long = "mode", value_enum)]
        modes: Vec<BenchMode>,
        /// Measured iterations over all selected slots and modes.
        #[arg(long, default_value_t = 1)]
        iterations: usize,
        /// Unmeasured warmup iterations over all selected slots and modes.
        #[arg(long, default_value_t = 0)]
        warmup: usize,
        /// Maximum number of concurrent native requests.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        /// Preload each slot payload once, then measure decode/render only.
        #[arg(long)]
        render_only: bool,
        /// Write per-stage renderer timings to profile.tsv. Only applies to --render-only.
        #[arg(long)]
        profile_render: bool,
        /// Optional directory for requests.tsv and summary.json.
        #[arg(long)]
        out_dir: Option<PathBuf>,
        #[command(flatten)]
        source: SourceArgs,
    },

    /// Generate a local JSON reference corpus by fetching each slot once natively.
    ReferenceJsonCorpus {
        /// Directory containing slot-index/epoch-N-slot-ranges-v2.raw files.
        #[arg(long)]
        index_dir: PathBuf,
        /// Plan JSON containing a `slots` array with epoch/slot/kind rows.
        #[arg(long)]
        plan_file: PathBuf,
        /// Output directory for response bodies, requests, metadata, and summary.
        #[arg(long)]
        out_dir: PathBuf,
        /// Maximum number of concurrent slot fetch/render jobs.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        #[command(flatten)]
        source: SourceArgs,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum BenchMode {
    Full,
    Accounts,
    Signatures,
    None,
    Rewards,
    BlockTime,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    match Cli::parse().command {
        Command::Serve {
            bind,
            index_dir,
            proxy_url,
            source,
        } => {
            let source = Arc::new(source.into_source()?);
            let archive = Arc::new(Archive::new(source, index_dir));
            let state = RpcState::new(archive, proxy_url)?;
            let app = Router::new()
                .route("/", post(rpc::handle_rpc))
                .with_state(state);
            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("bind {bind}"))?;
            tracing::info!("of-get-block-worker listening on http://{bind}");
            axum::serve(listener, app).await.context("serve JSON-RPC")?;
        }
        Command::BuildSlotIndexV2 {
            epoch,
            out,
            seed_previous_blockhash,
            max_bucket_payload_bytes,
            allow_node_read_fallback,
            source,
        } => {
            let source = Arc::new(source.into_source()?);
            let seed_previous_blockhash = seed_previous_blockhash
                .as_deref()
                .map(decode_blockhash)
                .transpose()?;
            let output = build_slot_ranges_v2(
                source,
                epoch,
                seed_previous_blockhash,
                max_bucket_payload_bytes,
                allow_node_read_fallback,
            )
            .await?;

            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            let mut file =
                std::fs::File::create(&out).with_context(|| format!("create {}", out.display()))?;
            write_slot_ranges_v2_raw(&mut file, &output.ranges)
                .with_context(|| format!("write {}", out.display()))?;

            println!(
                "epoch {}: wrote {} v2 slot range rows to {} (present slots {}, first {:?}, last {:?}, last blockhash {})",
                epoch,
                output.ranges.len(),
                out.display(),
                output.present_slots,
                output.first_present_slot,
                output.last_present_slot,
                bs58::encode(output.last_blockhash).into_string()
            );
        }
        Command::Bench {
            index_dir,
            slots,
            slots_file,
            modes,
            iterations,
            warmup,
            concurrency,
            render_only,
            profile_render,
            out_dir,
            source,
        } => {
            let slots = load_bench_slots(slots, slots_file)?;
            if slots.is_empty() {
                anyhow::bail!("bench requires at least one --slot or --slots-file entry");
            }
            let modes = if modes.is_empty() {
                vec![
                    BenchMode::Full,
                    BenchMode::Accounts,
                    BenchMode::Signatures,
                    BenchMode::None,
                    BenchMode::BlockTime,
                ]
            } else {
                modes
            };

            let source = Arc::new(source.into_source()?);
            let archive = Arc::new(Archive::new(source, index_dir));
            let jobs = bench_jobs(&slots, &modes, iterations.max(1));
            let warmup_jobs = bench_jobs(&slots, &modes, warmup);

            eprintln!(
                "native bench: slots={} modes={} warmup_jobs={} measured_jobs={} concurrency={} render_only={} profile_render={}",
                slots.len(),
                modes.len(),
                warmup_jobs.len(),
                jobs.len(),
                concurrency.max(1),
                render_only,
                profile_render
            );

            let preloaded = if render_only {
                Some(Arc::new(
                    preload_bench_blocks(Arc::clone(&archive), &slots, &modes, concurrency.max(1))
                        .await,
                ))
            } else {
                None
            };

            if !warmup_jobs.is_empty() {
                let _ = run_bench_jobs(
                    Arc::clone(&archive),
                    preloaded.clone(),
                    warmup_jobs,
                    concurrency.max(1),
                    false,
                )
                .await;
            }

            let wall_started = Instant::now();
            let rows = run_bench_jobs(
                archive,
                preloaded,
                jobs,
                concurrency.max(1),
                render_only && profile_render,
            )
            .await;
            let wall_s = wall_started.elapsed().as_secs_f64();
            print_bench_summary(&rows, wall_s);

            if let Some(out_dir) = out_dir {
                write_bench_report(&out_dir, &rows, wall_s)
                    .with_context(|| format!("write bench report to {}", out_dir.display()))?;
            }
        }
        Command::ReferenceJsonCorpus {
            index_dir,
            plan_file,
            out_dir,
            concurrency,
            source,
        } => {
            let source = Arc::new(source.into_source()?);
            let archive = Arc::new(Archive::new(source, index_dir));
            let plan = load_reference_plan(&plan_file)?;
            let wall_started = Instant::now();
            let rows =
                run_reference_json_corpus(archive, plan, &out_dir, concurrency.max(1)).await?;
            let wall_s = wall_started.elapsed().as_secs_f64();
            write_reference_summary(&out_dir, &rows, wall_s)?;
            println!(
                "reference-json-corpus wrote {} rows to {} in {:.3}s",
                rows.len(),
                out_dir.display(),
                wall_s
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct ReferencePlanFile {
    slots: Vec<ReferenceSlot>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReferenceSlot {
    epoch: u64,
    slot: u64,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    sample_index: usize,
}

#[derive(Debug, Clone)]
struct ReferenceRow {
    epoch: u64,
    slot: u64,
    kind: String,
    sample_index: usize,
    call: String,
    status: &'static str,
    fetch_ms: f64,
    render_ms: f64,
    bytes: usize,
    error: String,
    body_path: String,
    request_path: String,
    meta_path: String,
}

#[derive(Clone, Copy)]
struct BenchJob {
    slot: u64,
    mode: BenchMode,
    iteration: usize,
}

struct BenchRow {
    slot: u64,
    mode: BenchMode,
    iteration: usize,
    status: &'static str,
    elapsed_ms: f64,
    bytes: usize,
    error: String,
    profile: Option<RenderProfile>,
}

type PreloadedBlocks = BTreeMap<(u64, bool), Result<Option<FetchedBlock>, String>>;

struct BenchOutcome {
    bytes: usize,
    profile: Option<RenderProfile>,
}

fn load_bench_slots(slots: Vec<u64>, slots_file: Option<PathBuf>) -> Result<Vec<u64>> {
    let mut out = slots;
    if let Some(path) = slots_file {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("read slots from {}", path.display()))?;
        for line in text.lines() {
            let line = line.split_once('#').map_or(line, |(left, _)| left);
            for part in line
                .split(|ch: char| ch == ',' || ch == ';' || ch.is_whitespace())
                .filter(|part| !part.is_empty())
            {
                out.push(
                    part.parse::<u64>()
                        .with_context(|| format!("parse slot {part:?} in {}", path.display()))?,
                );
            }
        }
    }
    Ok(out)
}

fn load_reference_plan(path: &Path) -> Result<Vec<ReferenceSlot>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    if let Ok(file) = serde_json::from_str::<ReferencePlanFile>(&text) {
        return Ok(file.slots);
    }
    serde_json::from_str::<Vec<ReferenceSlot>>(&text)
        .with_context(|| format!("parse reference plan {}", path.display()))
}

fn reference_calls() -> Vec<ReferenceCall> {
    let mut calls = vec![ReferenceCall::BlockTime];
    for details in [
        TransactionDetails::Full,
        TransactionDetails::Accounts,
        TransactionDetails::Signatures,
        TransactionDetails::None,
    ] {
        for rewards in [false, true] {
            calls.push(ReferenceCall::GetBlock { details, rewards });
        }
    }
    calls
}

#[derive(Debug, Clone, Copy)]
enum ReferenceCall {
    BlockTime,
    GetBlock {
        details: TransactionDetails,
        rewards: bool,
    },
}

impl ReferenceCall {
    fn label(self) -> String {
        match self {
            Self::BlockTime => "getBlockTime".to_string(),
            Self::GetBlock { details, rewards } => format!(
                "getBlock:encoding=json:details={}:rewards={}",
                transaction_details_label_local(details),
                rewards
            ),
        }
    }

    fn file_label(self) -> String {
        self.label()
            .replace(':', "_")
            .replace('=', "-")
            .replace('/', "_")
    }
}

async fn run_reference_json_corpus(
    archive: Arc<Archive>,
    plan: Vec<ReferenceSlot>,
    out_dir: &Path,
    concurrency: usize,
) -> Result<Vec<ReferenceRow>> {
    let responses_dir = out_dir.join("responses").join("local");
    std::fs::create_dir_all(&responses_dir)
        .with_context(|| format!("create {}", responses_dir.display()))?;
    std::fs::write(
        out_dir.join("endpoints.json"),
        serde_json::to_vec_pretty(&json!({"labels": ["local"], "primary": "local"}))?,
    )?;

    let mut rows = Vec::with_capacity(plan.len() * reference_calls().len());
    let mut set = tokio::task::JoinSet::new();
    let mut plan = plan.into_iter();

    loop {
        while set.len() < concurrency {
            let Some(slot) = plan.next() else {
                break;
            };
            let archive = Arc::clone(&archive);
            let responses_dir = responses_dir.clone();
            set.spawn(async move { render_reference_slot(archive, slot, responses_dir).await });
        }

        if set.is_empty() {
            break;
        }

        if let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(mut slot_rows)) => {
                    rows.append(&mut slot_rows);
                    if rows.len() % 450 == 0 {
                        eprintln!("reference-json-corpus rows={}", rows.len());
                    }
                }
                Ok(Err(err)) => return Err(err),
                Err(err) => anyhow::bail!("reference slot task failed: {err}"),
            }
        }
    }

    write_reference_rows(out_dir, &rows)?;
    Ok(rows)
}

async fn render_reference_slot(
    archive: Arc<Archive>,
    slot: ReferenceSlot,
    responses_dir: PathBuf,
) -> Result<Vec<ReferenceRow>> {
    let fetch_started = Instant::now();
    let fetched = archive.fetch_block_payload(slot.slot, true).await;
    let fetch_ms = fetch_started.elapsed().as_secs_f64() * 1000.0;
    let calls = reference_calls();
    let mut rows = Vec::with_capacity(calls.len());

    for call in calls {
        let call_started = Instant::now();
        let (body, status, error) = match &fetched {
            Ok(Some(block)) => render_reference_call(&slot, block, call),
            Ok(None) => (
                rpc_error_bytes(
                    -32009,
                    &format!(
                        "Slot {} was skipped, or missing in long-term storage",
                        slot.slot
                    ),
                )?,
                "error",
                String::new(),
            ),
            Err(err) => (
                rpc_error_bytes(-32000, &err.to_string())?,
                "error",
                err.to_string(),
            ),
        };
        let render_ms = call_started.elapsed().as_secs_f64() * 1000.0;
        let request = reference_request_json(&slot, call);
        let stem = format!(
            "e{}-s{}-{}-{}",
            slot.epoch,
            slot.slot,
            call.file_label(),
            slot.sample_index
        );
        let body_path = responses_dir.join(format!("{stem}.body"));
        let request_path = responses_dir.join(format!("{stem}.request.json"));
        let meta_path = responses_dir.join(format!("{stem}.meta.json"));
        std::fs::write(&body_path, &body)
            .with_context(|| format!("write {}", body_path.display()))?;
        std::fs::write(&request_path, serde_json::to_vec_pretty(&request)?)
            .with_context(|| format!("write {}", request_path.display()))?;

        let row = ReferenceRow {
            epoch: slot.epoch,
            slot: slot.slot,
            kind: slot.kind.clone(),
            sample_index: slot.sample_index,
            call: call.label(),
            status,
            fetch_ms,
            render_ms,
            bytes: body.len(),
            error,
            body_path: body_path.display().to_string(),
            request_path: request_path.display().to_string(),
            meta_path: meta_path.display().to_string(),
        };
        std::fs::write(
            &meta_path,
            serde_json::to_vec_pretty(&reference_row_json(&row))?,
        )
        .with_context(|| format!("write {}", meta_path.display()))?;
        rows.push(row);
    }

    Ok(rows)
}

fn render_reference_call(
    slot: &ReferenceSlot,
    block: &FetchedBlock,
    call: ReferenceCall,
) -> (Vec<u8>, &'static str, String) {
    let result = match call {
        ReferenceCall::BlockTime => {
            render_get_block_time(block.bytes.clone()).map(|value| match value {
                Some(block_time) => block_time.to_string().into_bytes(),
                None => b"null".to_vec(),
            })
        }
        ReferenceCall::GetBlock { details, rewards } => render_get_block_json_bytes(
            block.bytes.clone(),
            block.previous_blockhash,
            GetBlockConfig {
                encoding: GetBlockEncoding::Json,
                transaction_details: details,
                rewards,
            },
        ),
    };

    match result {
        Ok(result) => (
            rpc_success_bytes(&result).unwrap_or_else(|err| {
                rpc_error_bytes(-32000, &err.to_string()).unwrap_or_else(|_| b"{}".to_vec())
            }),
            "ok",
            String::new(),
        ),
        Err(err) => (
            rpc_error_bytes(
                -32000,
                &format!("failed to decode slot {}: {err}", slot.slot),
            )
            .unwrap_or_else(|_| b"{}".to_vec()),
            "error",
            err,
        ),
    }
}

fn reference_request_json(slot: &ReferenceSlot, call: ReferenceCall) -> Value {
    match call {
        ReferenceCall::BlockTime => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlockTime",
            "params": [slot.slot],
        }),
        ReferenceCall::GetBlock { details, rewards } => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [
                slot.slot,
                {
                    "commitment": "finalized",
                    "encoding": "json",
                    "transactionDetails": transaction_details_label_local(details),
                    "maxSupportedTransactionVersion": 0,
                    "rewards": rewards,
                }
            ],
        }),
    }
}

fn rpc_success_bytes(result: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(result.len() + 32);
    out.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"result\":");
    out.extend_from_slice(result);
    out.extend_from_slice(b",\"id\":1}");
    Ok(out)
}

fn rpc_error_bytes(code: i64, message: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    serde_json::to_writer(
        &mut out,
        &json!({
            "jsonrpc": "2.0",
            "error": {
                "code": code,
                "message": message,
            },
            "id": 1,
        }),
    )?;
    Ok(out)
}

fn write_reference_rows(out_dir: &Path, rows: &[ReferenceRow]) -> Result<()> {
    let mut file = File::create(out_dir.join("rows.jsonl"))?;
    for row in rows {
        serde_json::to_writer(&mut file, &reference_row_json(row))?;
        writeln!(file)?;
    }
    Ok(())
}

fn write_reference_summary(out_dir: &Path, rows: &[ReferenceRow], wall_s: f64) -> Result<()> {
    write_reference_rows(out_dir, rows)?;
    let mut by_call = serde_json::Map::new();
    for call in reference_calls() {
        let label = call.label();
        let call_rows = rows
            .iter()
            .filter(|row| row.call == label)
            .collect::<Vec<_>>();
        by_call.insert(label, summarize_reference_rows(&call_rows));
    }
    let summary = json!({
        "wall_s": wall_s,
        "endpoint": "local",
        "global": summarize_reference_rows(&rows.iter().collect::<Vec<_>>()),
        "by_call": by_call,
        "artifacts": {
            "rows": out_dir.join("rows.jsonl").display().to_string(),
            "responses_dir": out_dir.join("responses").join("local").display().to_string(),
        }
    });
    std::fs::write(
        out_dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

fn summarize_reference_rows(rows: &[&ReferenceRow]) -> Value {
    let mut elapsed = rows
        .iter()
        .map(|row| row.fetch_ms + row.render_ms)
        .collect::<Vec<_>>();
    elapsed.sort_by(|left, right| left.total_cmp(right));
    let mut fetch = rows.iter().map(|row| row.fetch_ms).collect::<Vec<_>>();
    fetch.sort_by(|left, right| left.total_cmp(right));
    let mut render = rows.iter().map(|row| row.render_ms).collect::<Vec<_>>();
    render.sort_by(|left, right| left.total_cmp(right));
    let count = rows.len();
    json!({
        "count": count,
        "ok": rows.iter().filter(|row| row.status == "ok").count(),
        "error": rows.iter().filter(|row| row.status == "error").count(),
        "avg_ms": avg(&elapsed),
        "p50_ms": percentile(&elapsed, 0.50),
        "p90_ms": percentile(&elapsed, 0.90),
        "p95_ms": percentile(&elapsed, 0.95),
        "p99_ms": percentile(&elapsed, 0.99),
        "max_ms": max_f64(&elapsed),
        "fetch": phase_summary(&fetch),
        "render": phase_summary(&render),
        "bytes": rows.iter().map(|row| row.bytes).sum::<usize>(),
    })
}

fn phase_summary(values: &[f64]) -> Value {
    json!({
        "avg_ms": avg(values),
        "p50_ms": percentile(values, 0.50),
        "p90_ms": percentile(values, 0.90),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": max_f64(values),
    })
}

fn avg(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn max_f64(values: &[f64]) -> f64 {
    values.iter().copied().reduce(f64::max).unwrap_or(0.0)
}

fn reference_row_json(row: &ReferenceRow) -> Value {
    json!({
        "endpoint": "local",
        "epoch": row.epoch,
        "slot": row.slot,
        "kind": row.kind,
        "sample_index": row.sample_index,
        "call": row.call,
        "http": "local",
        "rpc": row.status,
        "fetch_ms": row.fetch_ms,
        "render_ms": row.render_ms,
        "elapsed_ms": row.fetch_ms + row.render_ms,
        "body_bytes": row.bytes,
        "body_path": row.body_path,
        "request_path": row.request_path,
        "meta_path": row.meta_path,
        "error": row.error,
    })
}

fn transaction_details_label_local(details: TransactionDetails) -> &'static str {
    match details {
        TransactionDetails::Full => "full",
        TransactionDetails::Accounts => "accounts",
        TransactionDetails::Signatures => "signatures",
        TransactionDetails::None => "none",
    }
}

fn bench_jobs(slots: &[u64], modes: &[BenchMode], iterations: usize) -> Vec<BenchJob> {
    let mut jobs = Vec::with_capacity(slots.len() * modes.len() * iterations);
    for iteration in 0..iterations {
        for &slot in slots {
            for &mode in modes {
                jobs.push(BenchJob {
                    slot,
                    mode,
                    iteration,
                });
            }
        }
    }
    jobs
}

async fn run_bench_jobs(
    archive: Arc<Archive>,
    preloaded: Option<Arc<PreloadedBlocks>>,
    jobs: Vec<BenchJob>,
    concurrency: usize,
    profile_render: bool,
) -> Vec<BenchRow> {
    let mut rows = Vec::with_capacity(jobs.len());
    let mut set = tokio::task::JoinSet::new();
    let mut jobs = jobs.into_iter();

    loop {
        while set.len() < concurrency {
            let Some(job) = jobs.next() else {
                break;
            };
            let archive = Arc::clone(&archive);
            let preloaded = preloaded.clone();
            set.spawn(async move { run_bench_job(archive, preloaded, job, profile_render).await });
        }

        if set.is_empty() {
            break;
        }

        if let Some(result) = set.join_next().await {
            match result {
                Ok(row) => rows.push(row),
                Err(err) => rows.push(BenchRow {
                    slot: 0,
                    mode: BenchMode::Full,
                    iteration: 0,
                    status: "join_error",
                    elapsed_ms: 0.0,
                    bytes: 0,
                    error: err.to_string(),
                    profile: None,
                }),
            }
        }
    }

    rows
}

async fn run_bench_job(
    archive: Arc<Archive>,
    preloaded: Option<Arc<PreloadedBlocks>>,
    job: BenchJob,
    profile_render: bool,
) -> BenchRow {
    let started = Instant::now();
    let result = if let Some(preloaded) = preloaded {
        run_preloaded_bench_job(&preloaded, job, profile_render)
    } else {
        run_archive_bench_job(archive, job).await
    };
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

    match result {
        Ok(Some(outcome)) => BenchRow {
            slot: job.slot,
            mode: job.mode,
            iteration: job.iteration,
            status: "ok",
            elapsed_ms,
            bytes: outcome.bytes,
            error: String::new(),
            profile: outcome.profile,
        },
        Ok(None) => BenchRow {
            slot: job.slot,
            mode: job.mode,
            iteration: job.iteration,
            status: "null",
            elapsed_ms,
            bytes: 0,
            error: String::new(),
            profile: None,
        },
        Err(err) => BenchRow {
            slot: job.slot,
            mode: job.mode,
            iteration: job.iteration,
            status: "error",
            elapsed_ms,
            bytes: 0,
            error: err.to_string(),
            profile: None,
        },
    }
}

async fn run_archive_bench_job(
    archive: Arc<Archive>,
    job: BenchJob,
) -> Result<Option<BenchOutcome>> {
    match job.mode {
        BenchMode::BlockTime => {
            Ok(archive
                .get_block_time_json_bytes(job.slot)
                .await?
                .map(|bytes| BenchOutcome {
                    bytes: bytes.len(),
                    profile: None,
                }))
        }
        mode => {
            let config = bench_config(mode);
            Ok(archive
                .get_block_json_bytes(
                    job.slot,
                    of_get_block_worker::archive::GetBlockOptions { config },
                )
                .await?
                .map(|bytes| BenchOutcome {
                    bytes: bytes.len(),
                    profile: None,
                }))
        }
    }
}

fn run_preloaded_bench_job(
    preloaded: &PreloadedBlocks,
    job: BenchJob,
    profile_render: bool,
) -> Result<Option<BenchOutcome>> {
    let include_rewards = mode_include_rewards(job.mode);
    let Some(result) = preloaded.get(&(job.slot, include_rewards)) else {
        anyhow::bail!(
            "preloaded payload missing for slot {} rewards={}",
            job.slot,
            include_rewards
        );
    };
    let Some(block) = result
        .as_ref()
        .map_err(|err| anyhow::anyhow!(err.clone()))?
    else {
        return Ok(None);
    };
    render_bench_payload(block, job.mode, profile_render).map(Some)
}

fn render_bench_payload(
    block: &FetchedBlock,
    mode: BenchMode,
    profile_render: bool,
) -> Result<BenchOutcome> {
    let config = if mode == BenchMode::BlockTime {
        bench_config(BenchMode::None)
    } else {
        bench_config(mode)
    };
    if mode == BenchMode::BlockTime {
        return Ok(BenchOutcome {
            bytes: match render_get_block_time(block.bytes.clone())
                .map_err(|err| anyhow::anyhow!(err))?
            {
                Some(block_time) => block_time.to_string().len(),
                None => 4,
            },
            profile: None,
        });
    }

    if profile_render {
        let (bytes, profile) = render_get_block_json_bytes_profiled(
            block.bytes.clone(),
            block.previous_blockhash,
            config,
        )
        .map_err(|err| anyhow::anyhow!(err))?;
        return Ok(BenchOutcome {
            bytes: bytes.len(),
            profile: Some(profile),
        });
    }

    render_get_block_json_bytes(block.bytes.clone(), block.previous_blockhash, config)
        .map(|bytes| BenchOutcome {
            bytes: bytes.len(),
            profile: None,
        })
        .map_err(|err| anyhow::anyhow!(err))
}

async fn preload_bench_blocks(
    archive: Arc<Archive>,
    slots: &[u64],
    modes: &[BenchMode],
    concurrency: usize,
) -> PreloadedBlocks {
    let mut keys = slots
        .iter()
        .flat_map(|slot| {
            modes
                .iter()
                .map(move |mode| (*slot, mode_include_rewards(*mode)))
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();

    eprintln!("preloading {} slot payload(s)", keys.len());

    let mut out = PreloadedBlocks::new();
    let mut set = tokio::task::JoinSet::new();
    let mut keys = keys.into_iter();

    loop {
        while set.len() < concurrency {
            let Some((slot, include_rewards)) = keys.next() else {
                break;
            };
            let archive = Arc::clone(&archive);
            set.spawn(async move {
                let result = archive
                    .fetch_block_payload(slot, include_rewards)
                    .await
                    .map_err(|err| err.to_string());
                ((slot, include_rewards), result)
            });
        }

        if set.is_empty() {
            break;
        }
        if let Some(result) = set.join_next().await {
            match result {
                Ok((key, value)) => {
                    out.insert(key, value);
                }
                Err(err) => {
                    out.insert((0, false), Err(err.to_string()));
                }
            }
        }
    }

    out
}

fn bench_config(mode: BenchMode) -> GetBlockConfig {
    GetBlockConfig {
        rewards: mode == BenchMode::Rewards,
        transaction_details: match mode {
            BenchMode::Full => TransactionDetails::Full,
            BenchMode::Accounts => TransactionDetails::Accounts,
            BenchMode::Signatures => TransactionDetails::Signatures,
            BenchMode::None | BenchMode::Rewards => TransactionDetails::None,
            BenchMode::BlockTime => TransactionDetails::None,
        },
        ..Default::default()
    }
}

fn mode_include_rewards(mode: BenchMode) -> bool {
    mode == BenchMode::Rewards
}

fn print_bench_summary(rows: &[BenchRow], wall_s: f64) {
    let summary = summarize_bench(rows);
    println!(
        "global\tcount={}\tok={}\tnull={}\terror={}\tavg_ms={:.3}\tp50_ms={:.3}\tp95_ms={:.3}\tmax_ms={:.3}\tavg_bytes={:.0}\trps={:.2}\twall_s={:.3}",
        summary.count,
        summary.ok,
        summary.null,
        summary.error,
        summary.avg_ms,
        summary.p50_ms,
        summary.p95_ms,
        summary.max_ms,
        summary.avg_bytes,
        if wall_s > 0.0 {
            summary.count as f64 / wall_s
        } else {
            0.0
        },
        wall_s
    );

    let mut by_mode = BTreeMap::<BenchMode, Vec<&BenchRow>>::new();
    for row in rows {
        by_mode.entry(row.mode).or_default().push(row);
    }

    println!("mode\tcount\tok\tnull\terror\tavg_ms\tp50_ms\tp95_ms\tmax_ms\tavg_bytes");
    for (mode, rows) in by_mode {
        let summary = summarize_bench_refs(&rows);
        println!(
            "{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.0}",
            mode.label(),
            summary.count,
            summary.ok,
            summary.null,
            summary.error,
            summary.avg_ms,
            summary.p50_ms,
            summary.p95_ms,
            summary.max_ms,
            summary.avg_bytes
        );
    }
}

fn write_bench_report(out_dir: &PathBuf, rows: &[BenchRow], wall_s: f64) -> Result<()> {
    std::fs::create_dir_all(out_dir)?;

    let mut requests = File::create(out_dir.join("requests.tsv"))?;
    writeln!(
        requests,
        "iteration\tslot\tmode\tstatus\telapsed_ms\tbytes\terror"
    )?;
    for row in rows {
        writeln!(
            requests,
            "{}\t{}\t{}\t{}\t{:.6}\t{}\t{}",
            row.iteration,
            row.slot,
            row.mode.label(),
            row.status,
            row.elapsed_ms,
            row.bytes,
            row.error.replace(['\t', '\n', '\r'], " ")
        )?;
    }

    if rows.iter().any(|row| row.profile.is_some()) {
        let mut profiles = File::create(out_dir.join("profile.tsv"))?;
        writeln!(
            profiles,
            "iteration\tslot\tmode\tprofile_mode\tinclude_rewards\telapsed_ms\ttotal_us\tread_car_us\tdecode_rewards_us\twrite_header_us\twrite_rewards_us\tdecode_transactions_us\twrite_transactions_us\twrite_transaction_json_us\twrite_meta_json_us\ttransactions\tinput_bytes\toutput_bytes"
        )?;
        for row in rows {
            let Some(profile) = &row.profile else {
                continue;
            };
            writeln!(
                profiles,
                "{}\t{}\t{}\t{}\t{}\t{:.6}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                row.iteration,
                row.slot,
                row.mode.label(),
                profile.mode,
                profile.include_rewards,
                row.elapsed_ms,
                profile.total_us,
                profile.read_car_us,
                profile.decode_rewards_us,
                profile.write_header_us,
                profile.write_rewards_us,
                profile.decode_transactions_us,
                profile.write_transactions_us,
                profile.write_transaction_json_us,
                profile.write_meta_json_us,
                profile.transactions,
                profile.input_bytes,
                profile.output_bytes,
            )?;
        }
    }

    let mut by_mode = serde_json::Map::new();
    let mut grouped = BTreeMap::<BenchMode, Vec<&BenchRow>>::new();
    for row in rows {
        grouped.entry(row.mode).or_default().push(row);
    }
    for (mode, rows) in grouped {
        by_mode.insert(mode.label().to_string(), summarize_bench_refs(&rows).json());
    }

    let report = json!({
        "wall_s": wall_s,
        "global": summarize_bench(rows).json(),
        "by_mode": by_mode,
    });
    std::fs::write(
        out_dir.join("summary.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(())
}

struct BenchSummary {
    count: usize,
    ok: usize,
    null: usize,
    error: usize,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
    avg_bytes: f64,
}

impl BenchSummary {
    fn json(&self) -> Value {
        json!({
            "count": self.count,
            "ok": self.ok,
            "null": self.null,
            "error": self.error,
            "avg_ms": self.avg_ms,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "max_ms": self.max_ms,
            "avg_bytes": self.avg_bytes,
        })
    }
}

fn summarize_bench(rows: &[BenchRow]) -> BenchSummary {
    let refs = rows.iter().collect::<Vec<_>>();
    summarize_bench_refs(&refs)
}

fn summarize_bench_refs(rows: &[&BenchRow]) -> BenchSummary {
    let mut elapsed = rows.iter().map(|row| row.elapsed_ms).collect::<Vec<_>>();
    elapsed.sort_by(|left, right| left.total_cmp(right));
    let count = rows.len();
    let avg_ms = if count == 0 {
        0.0
    } else {
        elapsed.iter().sum::<f64>() / count as f64
    };
    let avg_bytes = if count == 0 {
        0.0
    } else {
        rows.iter().map(|row| row.bytes as f64).sum::<f64>() / count as f64
    };

    BenchSummary {
        count,
        ok: rows.iter().filter(|row| row.status == "ok").count(),
        null: rows.iter().filter(|row| row.status == "null").count(),
        error: rows.iter().filter(|row| row.status == "error").count(),
        avg_ms,
        p50_ms: percentile(&elapsed, 0.50),
        p95_ms: percentile(&elapsed, 0.95),
        max_ms: elapsed.last().copied().unwrap_or(0.0),
        avg_bytes,
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * p).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

impl BenchMode {
    fn label(self) -> &'static str {
        match self {
            BenchMode::Full => "full",
            BenchMode::Accounts => "accounts",
            BenchMode::Signatures => "signatures",
            BenchMode::None => "none",
            BenchMode::Rewards => "rewards",
            BenchMode::BlockTime => "block-time",
        }
    }
}

fn decode_blockhash(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode base58 blockhash {value}"))?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        anyhow::anyhow!("blockhash must be 32 bytes, got {}", bytes.len())
    })?;
    Ok(bytes)
}
