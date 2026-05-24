use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinSet, time::sleep};

const SLOTS_PER_EPOCH: u64 = 432_000;
const V2_STRIDE: usize = 44;

#[derive(Parser, Debug)]
#[command(name = "rpc-epoch-bench")]
#[command(about = "Rust live Solana getBlock epoch benchmark runner")]
struct Cli {
    /// Endpoint as label=url or label=@ENV_VAR. URL is never written to artifacts.
    #[arg(long = "endpoint", required = true)]
    endpoints: Vec<String>,

    /// Directory containing epoch-N-slot-ranges-v2.raw files.
    #[arg(long, default_value = "/srv/blockzilla/blockzilla/slot-index")]
    slot_index_dir: PathBuf,

    /// Epoch specs: 10, 10-20, 10,50,100, or available.
    #[arg(default_value = "available")]
    epochs: Vec<String>,

    /// Explicit slots to benchmark. When present, slot-index epoch sampling is skipped.
    #[arg(long = "slot")]
    slots: Vec<u64>,

    /// Random present slots per epoch, excluding first/last boundary slots.
    #[arg(long, default_value_t = 100)]
    samples_per_epoch: usize,

    /// Deterministic sample seed.
    #[arg(long, default_value_t = 4242)]
    seed: u64,

    /// Concurrent HTTP requests across all endpoints.
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// Request timeout seconds.
    #[arg(long, default_value_t = 180.0)]
    timeout: f64,

    /// JSON-RPC method to benchmark.
    #[arg(long, default_value = "getBlock")]
    method: String,

    /// Query string to append to blockBin requests, for example access=0.
    #[arg(long)]
    block_bin_query: Option<String>,

    /// Full-plan warmup passes to run before measured requests.
    #[arg(long, default_value_t = 0)]
    warmup_passes: usize,

    #[arg(long, default_value = "finalized")]
    commitment: String,

    #[arg(long, default_value = "json")]
    encoding: String,

    #[arg(long, default_value = "full")]
    transaction_details: String,

    #[arg(long, default_value_t = 0)]
    max_supported_transaction_version: u64,

    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1
    )]
    rewards: bool,

    /// Retry HTTP 429 or JSON-RPC rate-limit responses.
    #[arg(long, default_value_t = 4)]
    rate_limit_retries: usize,

    #[arg(long, default_value_t = 2.0)]
    rate_limit_sleep: f64,

    #[arg(long, default_value_t = 2.0)]
    rate_limit_backoff: f64,

    /// Retry HTTP 502/503/504 and transport failures.
    #[arg(long, default_value_t = 2)]
    transient_retries: usize,

    #[arg(long, default_value_t = 1.0)]
    transient_sleep: f64,

    #[arg(long, default_value_t = 2.0)]
    transient_backoff: f64,

    #[arg(long)]
    output_dir: Option<PathBuf>,

    #[arg(long, default_value = "rpc-epoch-bench")]
    prefix: String,

    #[arg(long)]
    dry_run: bool,

    #[arg(long, default_value_t = 50)]
    progress_every: usize,
}

#[derive(Debug, Clone)]
struct Endpoint {
    label: String,
    url: String,
}

#[derive(Debug, Clone, Serialize)]
struct PlanRow {
    epoch: u64,
    slot: u64,
    kind: String,
    sample_index: usize,
    present_slots: usize,
    index_path: String,
}

#[derive(Debug, Clone)]
struct RunConfig {
    method: RpcMethod,
    block_bin_query: Option<String>,
    commitment: String,
    encoding: String,
    transaction_details: String,
    max_supported_transaction_version: u64,
    rewards: bool,
    timeout: Duration,
    rate_limit_retries: usize,
    rate_limit_sleep: f64,
    rate_limit_backoff: f64,
    transient_retries: usize,
    transient_sleep: f64,
    transient_backoff: f64,
}

#[derive(Debug, Clone, Serialize)]
struct RequestRow {
    endpoint: String,
    epoch: u64,
    slot: u64,
    kind: String,
    sample_index: usize,
    http: String,
    rpc: String,
    bytes: usize,
    elapsed_s: f64,
    final_attempt_elapsed_s: f64,
    attempts: usize,
    rate_limited: bool,
    rate_limit_events: usize,
    rate_limit_retries: usize,
    rate_limit_sleep_s: f64,
    transient_retried: bool,
    transient_events: usize,
    transient_retries: usize,
    transient_sleep_s: f64,
    error: String,
}

#[derive(Debug, Serialize)]
struct Summary {
    attempted: usize,
    ok: usize,
    errors: usize,
    bytes: usize,
    attempts: usize,
    request_elapsed_sum_s: f64,
    min_s: f64,
    p50_s: f64,
    p90_s: f64,
    p95_s: f64,
    p99_s: f64,
    max_s: f64,
    avg_s: f64,
    rate_limit_events: usize,
    rate_limit_retries: usize,
    rate_limit_sleep_s: f64,
    transient_events: usize,
    transient_retries: usize,
    transient_sleep_s: f64,
    http: String,
    rpc: String,
}

#[derive(Debug, Serialize)]
struct GlobalReport<'a> {
    endpoints: Vec<&'a str>,
    epochs: Vec<u64>,
    planned_slots: usize,
    planned_requests: usize,
    method: &'a str,
    encoding: &'a str,
    transaction_details: &'a str,
    rewards: bool,
    warmup_passes: usize,
    warmups: Vec<Summary>,
    concurrency: usize,
    timeout_s: f64,
    wall_s: f64,
    global: Summary,
    by_endpoint: BTreeMap<String, Summary>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.concurrency == 0 {
        bail!("--concurrency must be >= 1");
    }
    let method = RpcMethod::parse(&cli.method)?;
    if method == RpcMethod::GetBlock {
        if !matches!(
            cli.transaction_details.as_str(),
            "full" | "accounts" | "signatures" | "none"
        ) {
            bail!("--transaction-details must be full, accounts, signatures, or none");
        }
    }

    let endpoints = cli
        .endpoints
        .iter()
        .map(|value| parse_endpoint(value))
        .collect::<Result<Vec<_>>>()?;
    let epochs = if cli.slots.is_empty() {
        expand_epoch_specs(&cli.epochs, &cli.slot_index_dir)?
    } else {
        cli.slots
            .iter()
            .map(|slot| slot / SLOTS_PER_EPOCH)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let plan = if cli.slots.is_empty() {
        build_plan(
            &cli.slot_index_dir,
            &epochs,
            cli.samples_per_epoch,
            cli.seed,
        )?
    } else {
        build_explicit_slot_plan(&cli.slots)
    };
    let output_dir = cli.output_dir.clone().unwrap_or_else(default_output_dir);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    write_plan(&output_dir.join(format!("{}-plan.tsv", cli.prefix)), &plan)?;

    eprintln!(
        "plan={} endpoints={} epochs={} slots={} requests={}",
        output_dir
            .join(format!("{}-plan.tsv", cli.prefix))
            .display(),
        endpoints.len(),
        epochs.len(),
        plan.len(),
        endpoints.len() * plan.len()
    );
    if cli.dry_run {
        return Ok(());
    }

    let config = Arc::new(RunConfig {
        method,
        block_bin_query: cli.block_bin_query.clone(),
        commitment: cli.commitment.clone(),
        encoding: cli.encoding.clone(),
        transaction_details: cli.transaction_details.clone(),
        max_supported_transaction_version: cli.max_supported_transaction_version,
        rewards: cli.rewards,
        timeout: Duration::from_secs_f64(cli.timeout),
        rate_limit_retries: cli.rate_limit_retries,
        rate_limit_sleep: cli.rate_limit_sleep,
        rate_limit_backoff: cli.rate_limit_backoff,
        transient_retries: cli.transient_retries,
        transient_sleep: cli.transient_sleep,
        transient_backoff: cli.transient_backoff,
    });
    let client = Client::builder()
        .timeout(config.timeout)
        .build()
        .context("build reqwest client")?;
    let jobs = make_jobs(&endpoints, &plan);
    let mut warmup_summaries = Vec::with_capacity(cli.warmup_passes);
    for pass in 0..cli.warmup_passes {
        eprintln!("warmup_pass={}/{}", pass + 1, cli.warmup_passes);
        let rows = run_jobs(
            client.clone(),
            jobs.clone(),
            Arc::clone(&config),
            cli.concurrency,
            cli.progress_every,
        )
        .await?;
        let summary = summarize(rows.iter());
        eprintln!(
            "warmup attempted={} ok={} errors={} avg_s={:.3} p50_s={:.3} p95_s={:.3} max_s={:.3}",
            summary.attempted,
            summary.ok,
            summary.errors,
            summary.avg_s,
            summary.p50_s,
            summary.p95_s,
            summary.max_s
        );
        warmup_summaries.push(summary);
    }
    let started = Instant::now();
    let rows = run_jobs(client, jobs, config, cli.concurrency, cli.progress_every).await?;
    let wall_s = started.elapsed().as_secs_f64();

    write_requests(
        &output_dir.join(format!("{}-requests.tsv", cli.prefix)),
        &rows,
    )?;
    write_per_epoch(
        &output_dir.join(format!("{}-per-epoch.tsv", cli.prefix)),
        &rows,
    )?;
    let epoch_values = plan
        .iter()
        .map(|row| row.epoch)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    write_global(
        &output_dir.join(format!("{}-global.json", cli.prefix)),
        &rows,
        &endpoints,
        &epoch_values,
        &cli,
        plan.len(),
        wall_s,
        warmup_summaries,
    )?;

    let summary = summarize(rows.iter());
    eprintln!(
        "global attempted={} ok={} errors={} avg_s={:.3} p50_s={:.3} p95_s={:.3} max_s={:.3} wall_s={:.3}",
        summary.attempted,
        summary.ok,
        summary.errors,
        summary.avg_s,
        summary.p50_s,
        summary.p95_s,
        summary.max_s,
        wall_s
    );
    Ok(())
}

fn parse_endpoint(value: &str) -> Result<Endpoint> {
    let (label, target) = value
        .split_once('=')
        .with_context(|| "--endpoint must be label=url or label=@ENV_VAR")?;
    if label.is_empty() {
        bail!("endpoint label must not be empty");
    }
    let url = if let Some(env_name) = target.strip_prefix('@') {
        env::var(env_name).with_context(|| format!("read endpoint env var {env_name}"))?
    } else {
        target.to_string()
    };
    if !url.starts_with("http://") && !url.starts_with("https://") {
        bail!("endpoint {label} must be an HTTP URL");
    }
    Ok(Endpoint {
        label: label.to_string(),
        url,
    })
}

fn expand_epoch_specs(specs: &[String], slot_index_dir: &Path) -> Result<Vec<u64>> {
    let specs = if specs.is_empty() {
        vec!["available".to_string()]
    } else {
        specs.to_vec()
    };
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for spec in specs {
        for part in spec.replace(',', " ").split_whitespace() {
            let values = if part == "available" {
                available_epochs(slot_index_dir)?
            } else if let Some((start, end)) = part.split_once('-') {
                let start = start.parse::<u64>()?;
                let end = end.parse::<u64>()?;
                if start <= end {
                    (start..=end).collect()
                } else {
                    (end..=start).rev().collect()
                }
            } else {
                vec![part.parse::<u64>()?]
            };
            for epoch in values {
                if seen.insert(epoch) {
                    out.push(epoch);
                }
            }
        }
    }
    Ok(out)
}

fn available_epochs(slot_index_dir: &Path) -> Result<Vec<u64>> {
    let mut epochs = Vec::new();
    for entry in fs::read_dir(slot_index_dir)
        .with_context(|| format!("read slot index dir {}", slot_index_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(value) = name
            .strip_prefix("epoch-")
            .and_then(|rest| rest.strip_suffix("-slot-ranges-v2.raw"))
        else {
            continue;
        };
        epochs.push(value.parse::<u64>()?);
    }
    epochs.sort_unstable();
    Ok(epochs)
}

fn build_plan(
    slot_index_dir: &Path,
    epochs: &[u64],
    samples_per_epoch: usize,
    seed: u64,
) -> Result<Vec<PlanRow>> {
    let mut rows = Vec::new();
    let mut rng = SplitMix64::new(seed);
    for epoch in epochs {
        let path = slot_index_dir.join(format!("epoch-{epoch}-slot-ranges-v2.raw"));
        let slots = present_slots(*epoch, &path)?;
        if slots.is_empty() {
            eprintln!("epoch={epoch}: no present slots in {}", path.display());
            continue;
        }
        let first = slots[0];
        let last = *slots.last().expect("checked nonempty slots");
        rows.push(PlanRow {
            epoch: *epoch,
            slot: first,
            kind: "bound_first".to_string(),
            sample_index: 0,
            present_slots: slots.len(),
            index_path: path.display().to_string(),
        });

        let interior = if slots.len() > 2 {
            &slots[1..slots.len() - 1]
        } else {
            &[]
        };
        let count = samples_per_epoch.min(interior.len());
        let mut picks = BTreeSet::new();
        while picks.len() < count {
            picks.insert(rng.gen_range(interior.len()));
        }
        for (sample_index, index) in picks.into_iter().enumerate() {
            rows.push(PlanRow {
                epoch: *epoch,
                slot: interior[index],
                kind: "random".to_string(),
                sample_index: sample_index + 1,
                present_slots: slots.len(),
                index_path: path.display().to_string(),
            });
        }

        if last != first {
            rows.push(PlanRow {
                epoch: *epoch,
                slot: last,
                kind: "bound_last".to_string(),
                sample_index: 0,
                present_slots: slots.len(),
                index_path: path.display().to_string(),
            });
        }
        eprintln!(
            "epoch={epoch} present={} samples={count} first={first} last={last} index={}",
            slots.len(),
            path.display()
        );
    }
    Ok(rows)
}

fn build_explicit_slot_plan(slots: &[u64]) -> Vec<PlanRow> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for slot in slots {
        if !seen.insert(*slot) {
            continue;
        }
        let epoch = slot / SLOTS_PER_EPOCH;
        rows.push(PlanRow {
            epoch,
            slot: *slot,
            kind: "slot".to_string(),
            sample_index: rows.len(),
            present_slots: 0,
            index_path: "explicit".to_string(),
        });
    }
    rows
}

fn present_slots(epoch: u64, path: &Path) -> Result<Vec<u64>> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let expected = SLOTS_PER_EPOCH as usize * V2_STRIDE;
    if data.len() != expected {
        bail!(
            "{} has {} bytes, expected {expected}",
            path.display(),
            data.len()
        );
    }
    let mut slots = Vec::new();
    for slot_in_epoch in 0..SLOTS_PER_EPOCH as usize {
        let offset = slot_in_epoch * V2_STRIDE + 8;
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if len != 0 {
            slots.push(epoch * SLOTS_PER_EPOCH + slot_in_epoch as u64);
        }
    }
    Ok(slots)
}

fn make_jobs(endpoints: &[Endpoint], plan: &[PlanRow]) -> Vec<(Endpoint, PlanRow)> {
    let mut jobs = Vec::with_capacity(endpoints.len() * plan.len());
    for endpoint in endpoints {
        for row in plan {
            jobs.push((endpoint.clone(), row.clone()));
        }
    }
    jobs
}

async fn run_jobs(
    client: Client,
    jobs: Vec<(Endpoint, PlanRow)>,
    config: Arc<RunConfig>,
    concurrency: usize,
    progress_every: usize,
) -> Result<Vec<RequestRow>> {
    let mut rows = Vec::with_capacity(jobs.len());
    let total = jobs.len();
    let mut jobs = jobs.into_iter();
    let mut set = JoinSet::new();
    let mut completed = 0usize;

    loop {
        while set.len() < concurrency {
            let Some((endpoint, row)) = jobs.next() else {
                break;
            };
            let client = client.clone();
            let config = Arc::clone(&config);
            set.spawn(async move { request_with_retries(&client, &endpoint, &row, &config).await });
        }
        if set.is_empty() {
            break;
        }
        let result = set
            .join_next()
            .await
            .expect("join set nonempty")
            .context("request task panicked")?;
        rows.push(result);
        completed += 1;
        if progress_every > 0 && completed % progress_every == 0 {
            eprintln!("completed={completed}/{total}");
        }
    }
    rows.sort_by(|a, b| {
        (
            a.endpoint.as_str(),
            a.epoch,
            a.slot,
            a.kind.as_str(),
            a.sample_index,
        )
            .cmp(&(
                b.endpoint.as_str(),
                b.epoch,
                b.slot,
                b.kind.as_str(),
                b.sample_index,
            ))
    });
    Ok(rows)
}

async fn request_with_retries(
    client: &Client,
    endpoint: &Endpoint,
    row: &PlanRow,
    config: &RunConfig,
) -> RequestRow {
    let started = Instant::now();
    let mut attempts = 0usize;
    let mut rate_limit_events = 0usize;
    let mut rate_limit_retries = 0usize;
    let mut rate_limit_sleep_s = 0.0;
    let mut transient_events = 0usize;
    let mut transient_retries = 0usize;
    let mut transient_sleep_s = 0.0;
    let mut rate_sleep = config.rate_limit_sleep;
    let mut transient_sleep = config.transient_sleep;

    loop {
        attempts += 1;
        let mut result = request_once(client, endpoint, row, config).await;
        if is_rate_limited(&result) {
            rate_limit_events += 1;
            if rate_limit_retries < config.rate_limit_retries {
                rate_limit_retries += 1;
                rate_limit_sleep_s += rate_sleep;
                sleep(Duration::from_secs_f64(rate_sleep)).await;
                rate_sleep *= config.rate_limit_backoff;
                continue;
            }
        } else if is_transient(&result) {
            transient_events += 1;
            if transient_retries < config.transient_retries {
                transient_retries += 1;
                transient_sleep_s += transient_sleep;
                sleep(Duration::from_secs_f64(transient_sleep)).await;
                transient_sleep *= config.transient_backoff;
                continue;
            }
        }
        result.elapsed_s = started.elapsed().as_secs_f64();
        result.attempts = attempts;
        result.rate_limited = rate_limit_events > 0;
        result.rate_limit_events = rate_limit_events;
        result.rate_limit_retries = rate_limit_retries;
        result.rate_limit_sleep_s = rate_limit_sleep_s;
        result.transient_retried = transient_events > 0;
        result.transient_events = transient_events;
        result.transient_retries = transient_retries;
        result.transient_sleep_s = transient_sleep_s;
        return result;
    }
}

async fn request_once(
    client: &Client,
    endpoint: &Endpoint,
    row: &PlanRow,
    config: &RunConfig,
) -> RequestRow {
    let request_started = Instant::now();
    let mut http = "000".to_string();
    let mut rpc = "unknown".to_string();
    let mut body = Vec::new();
    let mut error = String::new();

    let response = match config.method {
        RpcMethod::BlockBin => {
            client
                .get(block_bin_url(
                    &endpoint.url,
                    row.slot,
                    config.block_bin_query.as_deref(),
                ))
                .send()
                .await
        }
        RpcMethod::GetBlock | RpcMethod::GetBlockTime | RpcMethod::GetVersion => {
            let payload = build_payload(row.slot, config);
            client
                .post(&endpoint.url)
                .header("content-type", "application/json")
                .body(payload.to_string())
                .send()
                .await
        }
    };
    match response {
        Ok(response) => {
            http = response.status().as_u16().to_string();
            match response.bytes().await {
                Ok(bytes) => {
                    body = bytes.to_vec();
                    match config.method {
                        RpcMethod::BlockBin => {
                            if http == "200" {
                                rpc = "ok".to_string();
                            } else {
                                rpc = "http_error".to_string();
                                error = String::from_utf8_lossy(&body).into_owned();
                            }
                        }
                        RpcMethod::GetBlock | RpcMethod::GetBlockTime | RpcMethod::GetVersion => {
                            match serde_json::from_slice::<Value>(&body) {
                                Ok(value) if value.get("error").is_some() => {
                                    rpc = "error".to_string();
                                    error =
                                        compact_json(value.get("error").unwrap_or(&Value::Null));
                                }
                                Ok(_) => rpc = "ok".to_string(),
                                Err(err) => {
                                    rpc = "invalid_json".to_string();
                                    error = err.to_string();
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    error = format!("read response body: {err}");
                }
            }
        }
        Err(err) => {
            error = err.to_string();
        }
    }
    RequestRow {
        endpoint: endpoint.label.clone(),
        epoch: row.epoch,
        slot: row.slot,
        kind: row.kind.clone(),
        sample_index: row.sample_index,
        http,
        rpc,
        bytes: body.len(),
        elapsed_s: request_started.elapsed().as_secs_f64(),
        final_attempt_elapsed_s: request_started.elapsed().as_secs_f64(),
        attempts: 1,
        rate_limited: false,
        rate_limit_events: 0,
        rate_limit_retries: 0,
        rate_limit_sleep_s: 0.0,
        transient_retried: false,
        transient_events: 0,
        transient_retries: 0,
        transient_sleep_s: 0.0,
        error: one_line(&error),
    }
}

fn block_bin_url(endpoint: &str, slot: u64, query: Option<&str>) -> String {
    let mut url = format!("{}/block/{slot}.bin", endpoint.trim_end_matches('/'));
    if let Some(query) = query.filter(|value| !value.is_empty()) {
        url.push('?');
        url.push_str(query.trim_start_matches('?'));
    }
    url
}

fn is_rate_limited(row: &RequestRow) -> bool {
    row.http == "429" || {
        let error = row.error.to_ascii_lowercase();
        error.contains("rate limit")
            || error.contains("too many requests")
            || error.contains("\"code\":429")
    }
}

fn is_transient(row: &RequestRow) -> bool {
    matches!(row.http.as_str(), "000" | "502" | "503" | "504") || {
        let error = row.error.to_ascii_lowercase();
        error.contains("timeout")
            || error.contains("service unavailable")
            || error.contains("bad gateway")
    }
}

fn write_plan(path: &Path, rows: &[PlanRow]) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(
        out,
        "epoch\tslot\tkind\tsample_index\tpresent_slots\tindex_path"
    )?;
    for row in rows {
        writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}",
            row.epoch, row.slot, row.kind, row.sample_index, row.present_slots, row.index_path
        )?;
    }
    Ok(())
}

fn write_requests(path: &Path, rows: &[RequestRow]) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(
        out,
        "endpoint\tepoch\tslot\tkind\tsample_index\thttp\trpc\tbytes\telapsed_s\tfinal_attempt_elapsed_s\tattempts\trate_limited\trate_limit_events\trate_limit_retries\trate_limit_sleep_s\ttransient_retried\ttransient_events\ttransient_retries\ttransient_sleep_s\terror"
    )?;
    for row in rows {
        writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}\t{:.6}\t{}\t{}\t{}\t{:.6}\t{}",
            row.endpoint,
            row.epoch,
            row.slot,
            row.kind,
            row.sample_index,
            row.http,
            row.rpc,
            row.bytes,
            row.elapsed_s,
            row.final_attempt_elapsed_s,
            row.attempts,
            row.rate_limited,
            row.rate_limit_events,
            row.rate_limit_retries,
            row.rate_limit_sleep_s,
            row.transient_retried,
            row.transient_events,
            row.transient_retries,
            row.transient_sleep_s,
            row.error,
        )?;
    }
    Ok(())
}

fn write_per_epoch(path: &Path, rows: &[RequestRow]) -> Result<()> {
    let mut grouped = BTreeMap::<(String, u64), Vec<&RequestRow>>::new();
    for row in rows {
        grouped
            .entry((row.endpoint.clone(), row.epoch))
            .or_default()
            .push(row);
    }
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(
        out,
        "endpoint\tepoch\tattempted\tok\terrors\tbytes\tattempts\tavg_s\tp50_s\tp90_s\tp95_s\tp99_s\tmax_s\trate_limit_events\trate_limit_retries\trate_limit_sleep_s\ttransient_events\ttransient_retries\ttransient_sleep_s\thttp\trpc"
    )?;
    for ((endpoint, epoch), group) in grouped {
        let summary = summarize(group.into_iter());
        writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{}\t{}\t{:.6}\t{}\t{}\t{:.6}\t{}\t{}",
            endpoint,
            epoch,
            summary.attempted,
            summary.ok,
            summary.errors,
            summary.bytes,
            summary.attempts,
            summary.avg_s,
            summary.p50_s,
            summary.p90_s,
            summary.p95_s,
            summary.p99_s,
            summary.max_s,
            summary.rate_limit_events,
            summary.rate_limit_retries,
            summary.rate_limit_sleep_s,
            summary.transient_events,
            summary.transient_retries,
            summary.transient_sleep_s,
            summary.http,
            summary.rpc,
        )?;
    }
    Ok(())
}

fn write_global(
    path: &Path,
    rows: &[RequestRow],
    endpoints: &[Endpoint],
    epochs: &[u64],
    cli: &Cli,
    planned_slots: usize,
    wall_s: f64,
    warmups: Vec<Summary>,
) -> Result<()> {
    let mut by_endpoint = BTreeMap::new();
    for endpoint in endpoints {
        by_endpoint.insert(
            endpoint.label.clone(),
            summarize(rows.iter().filter(|row| row.endpoint == endpoint.label)),
        );
    }
    let endpoint_labels = endpoints
        .iter()
        .map(|endpoint| endpoint.label.as_str())
        .collect::<Vec<_>>();
    let report = GlobalReport {
        endpoints: endpoint_labels,
        epochs: epochs.to_vec(),
        planned_slots,
        planned_requests: planned_slots * endpoints.len(),
        method: &cli.method,
        encoding: &cli.encoding,
        transaction_details: &cli.transaction_details,
        rewards: cli.rewards,
        warmup_passes: cli.warmup_passes,
        warmups,
        concurrency: cli.concurrency,
        timeout_s: cli.timeout,
        wall_s,
        global: summarize(rows.iter()),
        by_endpoint,
    };
    serde_json::to_writer_pretty(File::create(path)?, &report)?;
    Ok(())
}

fn summarize<'a>(rows: impl Iterator<Item = &'a RequestRow>) -> Summary {
    let rows = rows.collect::<Vec<_>>();
    if rows.is_empty() {
        return Summary {
            attempted: 0,
            ok: 0,
            errors: 0,
            bytes: 0,
            attempts: 0,
            request_elapsed_sum_s: 0.0,
            min_s: 0.0,
            p50_s: 0.0,
            p90_s: 0.0,
            p95_s: 0.0,
            p99_s: 0.0,
            max_s: 0.0,
            avg_s: 0.0,
            rate_limit_events: 0,
            rate_limit_retries: 0,
            rate_limit_sleep_s: 0.0,
            transient_events: 0,
            transient_retries: 0,
            transient_sleep_s: 0.0,
            http: String::new(),
            rpc: String::new(),
        };
    }
    let mut times = rows.iter().map(|row| row.elapsed_s).collect::<Vec<_>>();
    times.sort_by(f64::total_cmp);
    let http = counts(rows.iter().map(|row| row.http.as_str()));
    let rpc = counts(rows.iter().map(|row| row.rpc.as_str()));
    let ok = rows
        .iter()
        .filter(|row| row.http == "200" && row.rpc == "ok")
        .count();
    Summary {
        attempted: rows.len(),
        ok,
        errors: rows.len() - ok,
        bytes: rows.iter().map(|row| row.bytes).sum(),
        attempts: rows.iter().map(|row| row.attempts).sum(),
        request_elapsed_sum_s: rows.iter().map(|row| row.elapsed_s).sum(),
        min_s: times[0],
        p50_s: percentile(&times, 0.50),
        p90_s: percentile(&times, 0.90),
        p95_s: percentile(&times, 0.95),
        p99_s: percentile(&times, 0.99),
        max_s: *times.last().unwrap(),
        avg_s: times.iter().sum::<f64>() / times.len() as f64,
        rate_limit_events: rows.iter().map(|row| row.rate_limit_events).sum(),
        rate_limit_retries: rows.iter().map(|row| row.rate_limit_retries).sum(),
        rate_limit_sleep_s: rows.iter().map(|row| row.rate_limit_sleep_s).sum(),
        transient_events: rows.iter().map(|row| row.transient_events).sum(),
        transient_retries: rows.iter().map(|row| row.transient_retries).sum(),
        transient_sleep_s: rows.iter().map(|row| row.transient_sleep_s).sum(),
        http,
        rpc,
    }
}

fn counts<'a>(values: impl Iterator<Item = &'a str>) -> String {
    let mut counts = BTreeMap::<&str, usize>::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() as f64 * pct).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|err| format!("encode JSON error: {err}"))
}

fn build_payload(slot: u64, config: &RunConfig) -> Value {
    match config.method {
        RpcMethod::GetBlock => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [
                slot,
                {
                    "commitment": config.commitment,
                    "encoding": config.encoding,
                    "transactionDetails": config.transaction_details,
                    "maxSupportedTransactionVersion": config.max_supported_transaction_version,
                    "rewards": config.rewards,
                }
            ]
        }),
        RpcMethod::GetBlockTime => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlockTime",
            "params": [slot]
        }),
        RpcMethod::GetVersion => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getVersion",
            "params": []
        }),
        RpcMethod::BlockBin => Value::Null,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcMethod {
    GetBlock,
    GetBlockTime,
    GetVersion,
    BlockBin,
}

impl RpcMethod {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "getBlock" => Ok(Self::GetBlock),
            "getBlockTime" => Ok(Self::GetBlockTime),
            "getVersion" => Ok(Self::GetVersion),
            "blockBin" | "blockWincode" => Ok(Self::BlockBin),
            _ => bail!("--method must be getBlock, getBlockTime, getVersion, or blockBin"),
        }
    }
}

fn one_line(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}

fn default_output_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!("/tmp/rpc-epoch-bench-{stamp}"))
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn gen_range(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            return 0;
        }
        (self.next_u64() % upper as u64) as usize
    }
}
