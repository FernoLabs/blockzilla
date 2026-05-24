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
const VALUE_SUMMARY_LIMIT: usize = 700;

#[derive(Parser, Debug)]
#[command(name = "rpc-correctness-check")]
#[command(about = "Compare live getBlock JSON-RPC responses across providers")]
struct Cli {
    /// Endpoint as label=url or label=@ENV_VAR. URL is never written to artifacts.
    #[arg(long = "endpoint", required = true)]
    endpoints: Vec<String>,

    /// Endpoint label used as the correctness reference.
    #[arg(long, default_value = "helius")]
    primary: String,

    /// Directory containing epoch-N-slot-ranges-v2.raw files.
    #[arg(long, default_value = "/volume1/blockzilla/slot-index")]
    slot_index_dir: PathBuf,

    /// Epoch specs: 10, 10-20, 10,50,100, or available.
    #[arg(default_value = "available")]
    epochs: Vec<String>,

    /// Explicit slot to check. May be repeated; bypasses slot-index sampling.
    #[arg(long = "slot")]
    slots: Vec<u64>,

    /// Random present slots per epoch, excluding first/last boundary slots.
    #[arg(long, default_value_t = 100)]
    samples_per_epoch: usize,

    /// Deterministic sample seed.
    #[arg(long, default_value_t = 4242)]
    seed: u64,

    /// Concurrent slots. Each slot concurrently requests all endpoints.
    #[arg(long, default_value_t = 2)]
    slot_concurrency: usize,

    /// Request timeout seconds.
    #[arg(long, default_value_t = 180.0)]
    timeout: f64,

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

    /// Maximum JSON diffs to emit per slot/endpoint comparison. Use 0 for unlimited.
    #[arg(long, default_value_t = 256)]
    max_diffs_per_call: usize,

    #[arg(long)]
    output_dir: Option<PathBuf>,

    #[arg(long, default_value = "rpc-correctness")]
    prefix: String,

    #[arg(long)]
    dry_run: bool,

    #[arg(long, default_value_t = 25)]
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
    max_diffs_per_call: usize,
}

#[derive(Debug, Clone, Serialize)]
struct CallRow {
    endpoint: String,
    epoch: u64,
    slot: u64,
    kind: String,
    sample_index: usize,
    is_primary: bool,
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
    compared_to_primary: bool,
    matches_primary: String,
    diff_count: usize,
    diff_path: String,
    error: String,
}

#[derive(Debug)]
struct RpcResult {
    row: CallRow,
    body: Option<Value>,
}

#[derive(Debug, Serialize)]
struct MismatchRow {
    endpoint: String,
    primary: String,
    epoch: u64,
    slot: u64,
    kind: String,
    sample_index: usize,
    diff_index: usize,
    transaction_index: String,
    signature: String,
    instruction_index: String,
    program_id_index: String,
    program_id: String,
    category: String,
    diff_path: String,
    endpoint_http: String,
    endpoint_rpc: String,
    endpoint_bytes: usize,
    primary_http: String,
    primary_rpc: String,
    primary_bytes: usize,
    endpoint_value: String,
    primary_value: String,
    endpoint_error: String,
    primary_error: String,
}

#[derive(Debug)]
struct SlotCheck {
    calls: Vec<CallRow>,
    mismatches: Vec<MismatchRow>,
}

#[derive(Debug, Serialize)]
struct Summary {
    calls: usize,
    ok: usize,
    errors: usize,
    compared: usize,
    matches: usize,
    mismatches: usize,
    not_compared: usize,
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
    mismatch_categories: String,
    mismatch_paths: String,
}

#[derive(Debug, Serialize)]
struct GlobalReport<'a> {
    endpoints: Vec<&'a str>,
    primary: &'a str,
    epochs: Vec<u64>,
    planned_slots: usize,
    planned_requests: usize,
    transaction_details: &'a str,
    rewards: bool,
    slot_concurrency: usize,
    timeout_s: f64,
    wall_s: f64,
    global: Summary,
    by_endpoint: BTreeMap<String, Summary>,
    mismatch_paths: BTreeMap<String, usize>,
}

#[derive(Debug)]
struct DiffSummary {
    path: String,
    endpoint_value: String,
    primary_value: String,
}

#[derive(Debug)]
struct DiffReport {
    diffs: Vec<DiffSummary>,
    truncated: bool,
}

#[derive(Debug, Default)]
struct MismatchContext {
    transaction_index: String,
    signature: String,
    instruction_index: String,
    program_id_index: String,
    program_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.slot_concurrency == 0 {
        bail!("--slot-concurrency must be >= 1");
    }
    if !matches!(
        cli.transaction_details.as_str(),
        "full" | "accounts" | "signatures" | "none"
    ) {
        bail!("--transaction-details must be full, accounts, signatures, or none");
    }

    let endpoints = cli
        .endpoints
        .iter()
        .map(|value| parse_endpoint(value))
        .collect::<Result<Vec<_>>>()?;
    if !endpoints
        .iter()
        .any(|endpoint| endpoint.label == cli.primary)
    {
        bail!("--primary must match one --endpoint label");
    }

    let (epochs, plan) = if cli.slots.is_empty() {
        let epochs = expand_epoch_specs(&cli.epochs, &cli.slot_index_dir)?;
        let plan = build_plan(
            &cli.slot_index_dir,
            &epochs,
            cli.samples_per_epoch,
            cli.seed,
        )?;
        (epochs, plan)
    } else {
        build_explicit_slot_plan(&cli.slots)
    };
    let output_dir = cli.output_dir.clone().unwrap_or_else(default_output_dir);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    write_plan(&output_dir.join(format!("{}-plan.tsv", cli.prefix)), &plan)?;

    eprintln!(
        "plan={} primary={} endpoints={} epochs={} slots={} requests={}",
        output_dir
            .join(format!("{}-plan.tsv", cli.prefix))
            .display(),
        cli.primary,
        endpoints.len(),
        epochs.len(),
        plan.len(),
        endpoints.len() * plan.len()
    );
    if cli.dry_run {
        return Ok(());
    }

    let config = Arc::new(RunConfig {
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
        max_diffs_per_call: cli.max_diffs_per_call,
    });
    let client = Client::builder()
        .timeout(config.timeout)
        .build()
        .context("build reqwest client")?;

    let started = Instant::now();
    let checks = run_checks(
        client,
        endpoints.clone(),
        plan.clone(),
        cli.primary.clone(),
        config,
        cli.slot_concurrency,
        cli.progress_every,
    )
    .await?;
    let wall_s = started.elapsed().as_secs_f64();

    let mut calls = Vec::new();
    let mut mismatches = Vec::new();
    for check in checks {
        calls.extend(check.calls);
        mismatches.extend(check.mismatches);
    }
    sort_calls(&mut calls);
    sort_mismatches(&mut mismatches);

    write_calls(
        &output_dir.join(format!("{}-calls.tsv", cli.prefix)),
        &calls,
    )?;
    write_mismatches(
        &output_dir.join(format!("{}-mismatches.jsonl", cli.prefix)),
        &mismatches,
    )?;
    let epoch_values = plan
        .iter()
        .map(|row| row.epoch)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    write_summary(
        &output_dir.join(format!("{}-summary.json", cli.prefix)),
        &calls,
        &mismatches,
        &endpoints,
        &epoch_values,
        &cli,
        plan.len(),
        wall_s,
    )?;

    let summary = summarize(calls.iter(), mismatches.iter());
    eprintln!(
        "global calls={} ok={} errors={} compared={} matches={} mismatches={} not_compared={} avg_s={:.3} p50_s={:.3} p95_s={:.3} wall_s={:.3}",
        summary.calls,
        summary.ok,
        summary.errors,
        summary.compared,
        summary.matches,
        summary.mismatches,
        summary.not_compared,
        summary.avg_s,
        summary.p50_s,
        summary.p95_s,
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

fn build_explicit_slot_plan(slots: &[u64]) -> (Vec<u64>, Vec<PlanRow>) {
    let mut epochs = BTreeSet::new();
    let mut rows = Vec::with_capacity(slots.len());
    for (index, slot) in slots.iter().copied().enumerate() {
        let epoch = slot / SLOTS_PER_EPOCH;
        epochs.insert(epoch);
        rows.push(PlanRow {
            epoch,
            slot,
            kind: "explicit".to_string(),
            sample_index: index + 1,
            present_slots: 0,
            index_path: String::new(),
        });
    }
    (epochs.into_iter().collect(), rows)
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

async fn run_checks(
    client: Client,
    endpoints: Vec<Endpoint>,
    plan: Vec<PlanRow>,
    primary: String,
    config: Arc<RunConfig>,
    slot_concurrency: usize,
    progress_every: usize,
) -> Result<Vec<SlotCheck>> {
    let total = plan.len();
    let mut rows = plan.into_iter();
    let mut set = JoinSet::new();
    let mut checks = Vec::with_capacity(total);
    let mut completed = 0usize;

    loop {
        while set.len() < slot_concurrency {
            let Some(row) = rows.next() else {
                break;
            };
            let client = client.clone();
            let endpoints = endpoints.clone();
            let primary = primary.clone();
            let config = Arc::clone(&config);
            set.spawn(async move { check_slot(client, endpoints, primary, row, config).await });
        }
        if set.is_empty() {
            break;
        }
        let result = set
            .join_next()
            .await
            .expect("join set nonempty")
            .context("slot task panicked")?;
        checks.push(result?);
        completed += 1;
        if progress_every > 0 && completed % progress_every == 0 {
            eprintln!("checked_slots={completed}/{total}");
        }
    }
    Ok(checks)
}

async fn check_slot(
    client: Client,
    endpoints: Vec<Endpoint>,
    primary: String,
    plan_row: PlanRow,
    config: Arc<RunConfig>,
) -> Result<SlotCheck> {
    let mut set = JoinSet::new();
    for endpoint in endpoints {
        let client = client.clone();
        let row = plan_row.clone();
        let config = Arc::clone(&config);
        set.spawn(async move { request_with_retries(&client, &endpoint, &row, &config).await });
    }

    let mut results = Vec::new();
    while let Some(result) = set.join_next().await {
        results.push(result.context("endpoint task panicked")?);
    }
    results.sort_by(|a, b| a.row.endpoint.cmp(&b.row.endpoint));

    let primary_index = results
        .iter()
        .position(|result| result.row.endpoint == primary)
        .with_context(|| format!("primary endpoint {primary} missing from slot result"))?;
    results[primary_index].row.is_primary = true;
    results[primary_index].row.matches_primary = "primary".to_string();

    let mut mismatches = Vec::new();
    let primary_result = result_snapshot(&results[primary_index]);
    for result in &mut results {
        if result.row.endpoint == primary {
            continue;
        }
        match compare_to_primary(result, &primary_result, config.max_diffs_per_call) {
            CompareOutcome::Match => {
                result.row.compared_to_primary = true;
                result.row.matches_primary = "true".to_string();
            }
            CompareOutcome::Mismatch(mismatch_rows) => {
                result.row.diff_count = mismatch_rows.len();
                let first_mismatch = mismatch_rows
                    .first()
                    .expect("mismatch outcome must contain at least one row");
                result.row.compared_to_primary = first_mismatch.category != "primary_unavailable";
                result.row.matches_primary = if result.row.compared_to_primary {
                    "false".to_string()
                } else {
                    "not_compared".to_string()
                };
                result.row.diff_path = first_mismatch.diff_path.clone();
                mismatches.extend(mismatch_rows);
            }
        }
    }

    Ok(SlotCheck {
        calls: results.into_iter().map(|result| result.row).collect(),
        mismatches,
    })
}

#[derive(Clone)]
struct ResultSnapshot {
    row: CallRow,
    body: Option<Value>,
}

fn result_snapshot(result: &RpcResult) -> ResultSnapshot {
    ResultSnapshot {
        row: result.row.clone(),
        body: result.body.clone(),
    }
}

enum CompareOutcome {
    Match,
    Mismatch(Vec<MismatchRow>),
}

fn compare_to_primary(
    result: &RpcResult,
    primary: &ResultSnapshot,
    max_diffs_per_call: usize,
) -> CompareOutcome {
    if primary.row.http != "200" || primary.row.rpc != "ok" {
        return CompareOutcome::Mismatch(vec![make_mismatch(
            result,
            primary,
            "primary_unavailable",
            "$",
            1,
            summarize_json(result.body.as_ref()),
            summarize_json(primary.body.as_ref()),
        )]);
    }
    if result.row.http != "200" || result.row.rpc != "ok" {
        return CompareOutcome::Mismatch(vec![make_mismatch(
            result,
            primary,
            "endpoint_error",
            "$.error",
            1,
            summarize_json(result.body.as_ref()),
            summarize_json(primary.body.as_ref()),
        )]);
    }

    match (result.body.as_ref(), primary.body.as_ref()) {
        (Some(endpoint), Some(reference)) if endpoint == reference => CompareOutcome::Match,
        (Some(endpoint), Some(reference)) => {
            let report = diff_report(endpoint, reference, "$", max_diffs_per_call);
            let mut rows = report
                .diffs
                .into_iter()
                .enumerate()
                .map(|(index, diff)| {
                    let category = classify_diff_path(&diff.path);
                    make_mismatch(
                        result,
                        primary,
                        category,
                        &diff.path,
                        index + 1,
                        diff.endpoint_value,
                        diff.primary_value,
                    )
                })
                .collect::<Vec<_>>();
            if rows.is_empty() {
                rows.push(make_mismatch(
                    result,
                    primary,
                    "json_diff",
                    "$",
                    1,
                    summarize_json(Some(endpoint)),
                    summarize_json(Some(reference)),
                ));
            }
            if report.truncated {
                rows.push(make_mismatch(
                    result,
                    primary,
                    "json_diff_truncated",
                    "$",
                    rows.len() + 1,
                    format!("diff limit reached at {max_diffs_per_call}"),
                    "rerun with --max-diffs-per-call 0 for unlimited diff output".to_string(),
                ));
            }
            CompareOutcome::Mismatch(rows)
        }
        _ => CompareOutcome::Mismatch(vec![make_mismatch(
            result,
            primary,
            "missing_body",
            "$",
            1,
            summarize_json(result.body.as_ref()),
            summarize_json(primary.body.as_ref()),
        )]),
    }
}

fn make_mismatch(
    result: &RpcResult,
    primary: &ResultSnapshot,
    category: &str,
    diff_path: &str,
    diff_index: usize,
    endpoint_value: String,
    primary_value: String,
) -> MismatchRow {
    let context = mismatch_context(result.body.as_ref(), primary.body.as_ref(), diff_path);
    MismatchRow {
        endpoint: result.row.endpoint.clone(),
        primary: primary.row.endpoint.clone(),
        epoch: result.row.epoch,
        slot: result.row.slot,
        kind: result.row.kind.clone(),
        sample_index: result.row.sample_index,
        diff_index,
        transaction_index: context.transaction_index,
        signature: context.signature,
        instruction_index: context.instruction_index,
        program_id_index: context.program_id_index,
        program_id: context.program_id,
        category: category.to_string(),
        diff_path: diff_path.to_string(),
        endpoint_http: result.row.http.clone(),
        endpoint_rpc: result.row.rpc.clone(),
        endpoint_bytes: result.row.bytes,
        primary_http: primary.row.http.clone(),
        primary_rpc: primary.row.rpc.clone(),
        primary_bytes: primary.row.bytes,
        endpoint_value,
        primary_value,
        endpoint_error: result.row.error.clone(),
        primary_error: primary.row.error.clone(),
    }
}

fn mismatch_context(
    endpoint: Option<&Value>,
    primary: Option<&Value>,
    diff_path: &str,
) -> MismatchContext {
    let Some(index) = transaction_index_from_path(diff_path) else {
        return MismatchContext::default();
    };
    let signature = primary
        .and_then(|value| transaction_signature(value, index))
        .or_else(|| endpoint.and_then(|value| transaction_signature(value, index)))
        .unwrap_or_default();
    let Some(instruction_index) = instruction_index_from_path(diff_path) else {
        return MismatchContext {
            transaction_index: index.to_string(),
            signature,
            ..MismatchContext::default()
        };
    };
    let (instruction_label, instruction) =
        if let Some((inner_group_index, inner_instruction_index)) =
            inner_instruction_index_from_path(diff_path)
        {
            (
                format!("inner:{inner_group_index}:{inner_instruction_index}"),
                primary
                    .and_then(|value| {
                        transaction_inner_instruction(
                            value,
                            index,
                            inner_group_index,
                            inner_instruction_index,
                        )
                    })
                    .or_else(|| {
                        endpoint.and_then(|value| {
                            transaction_inner_instruction(
                                value,
                                index,
                                inner_group_index,
                                inner_instruction_index,
                            )
                        })
                    }),
            )
        } else {
            (
                instruction_index.to_string(),
                primary
                    .and_then(|value| transaction_instruction(value, index, instruction_index))
                    .or_else(|| {
                        endpoint.and_then(|value| {
                            transaction_instruction(value, index, instruction_index)
                        })
                    }),
            )
        };
    let program_id_index = instruction
        .and_then(|value| value.get("programIdIndex"))
        .and_then(Value::as_u64)
        .map(|value| value.to_string())
        .unwrap_or_default();
    let program_id = program_id_index
        .parse::<usize>()
        .ok()
        .and_then(|program_index| {
            primary
                .and_then(|value| transaction_account_key(value, index, program_index))
                .or_else(|| {
                    endpoint.and_then(|value| transaction_account_key(value, index, program_index))
                })
        })
        .unwrap_or_default();
    MismatchContext {
        transaction_index: index.to_string(),
        signature,
        instruction_index: instruction_label,
        program_id_index,
        program_id,
    }
}

fn transaction_index_from_path(path: &str) -> Option<usize> {
    let marker = ".transactions[";
    let start = path.find(marker)? + marker.len();
    let rest = path.get(start..)?;
    let end = rest.find(']')?;
    rest.get(..end)?.parse().ok()
}

fn transaction_signature(value: &Value, index: usize) -> Option<String> {
    value
        .get("result")?
        .get("transactions")?
        .get(index)?
        .get("transaction")?
        .get("signatures")?
        .get(0)?
        .as_str()
        .map(ToString::to_string)
}

fn instruction_index_from_path(path: &str) -> Option<usize> {
    let marker = ".instructions[";
    let start = path.find(marker)? + marker.len();
    let rest = path.get(start..)?;
    let end = rest.find(']')?;
    rest.get(..end)?.parse().ok()
}

fn inner_instruction_index_from_path(path: &str) -> Option<(usize, usize)> {
    let marker = ".meta.innerInstructions[";
    let start = path.find(marker)? + marker.len();
    let rest = path.get(start..)?;
    let group_end = rest.find(']')?;
    let group_index = rest.get(..group_end)?.parse().ok()?;
    let rest = rest.get(group_end..)?;
    let instruction_marker = ".instructions[";
    let instruction_start = rest.find(instruction_marker)? + instruction_marker.len();
    let rest = rest.get(instruction_start..)?;
    let instruction_end = rest.find(']')?;
    let instruction_index = rest.get(..instruction_end)?.parse().ok()?;
    Some((group_index, instruction_index))
}

fn transaction_instruction(
    value: &Value,
    transaction_index: usize,
    instruction_index: usize,
) -> Option<&Value> {
    value
        .get("result")?
        .get("transactions")?
        .get(transaction_index)?
        .get("transaction")?
        .get("message")?
        .get("instructions")?
        .get(instruction_index)
}

fn transaction_inner_instruction(
    value: &Value,
    transaction_index: usize,
    inner_group_index: usize,
    instruction_index: usize,
) -> Option<&Value> {
    value
        .get("result")?
        .get("transactions")?
        .get(transaction_index)?
        .get("meta")?
        .get("innerInstructions")?
        .get(inner_group_index)?
        .get("instructions")?
        .get(instruction_index)
}

fn transaction_account_key(
    value: &Value,
    transaction_index: usize,
    account_index: usize,
) -> Option<String> {
    let key = value
        .get("result")?
        .get("transactions")?
        .get(transaction_index)?
        .get("transaction")?
        .get("message")?
        .get("accountKeys")?
        .get(account_index)?;
    match key {
        Value::String(value) => Some(value.clone()),
        Value::Object(map) => map
            .get("pubkey")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        _ => None,
    }
}

async fn request_with_retries(
    client: &Client,
    endpoint: &Endpoint,
    row: &PlanRow,
    config: &RunConfig,
) -> RpcResult {
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
        if is_rate_limited(&result.row) {
            rate_limit_events += 1;
            if rate_limit_retries < config.rate_limit_retries {
                rate_limit_retries += 1;
                rate_limit_sleep_s += rate_sleep;
                sleep(Duration::from_secs_f64(rate_sleep)).await;
                rate_sleep *= config.rate_limit_backoff;
                continue;
            }
        } else if is_transient(&result.row) {
            transient_events += 1;
            if transient_retries < config.transient_retries {
                transient_retries += 1;
                transient_sleep_s += transient_sleep;
                sleep(Duration::from_secs_f64(transient_sleep)).await;
                transient_sleep *= config.transient_backoff;
                continue;
            }
        }
        result.row.elapsed_s = started.elapsed().as_secs_f64();
        result.row.attempts = attempts;
        result.row.rate_limited = rate_limit_events > 0;
        result.row.rate_limit_events = rate_limit_events;
        result.row.rate_limit_retries = rate_limit_retries;
        result.row.rate_limit_sleep_s = rate_limit_sleep_s;
        result.row.transient_retried = transient_events > 0;
        result.row.transient_events = transient_events;
        result.row.transient_retries = transient_retries;
        result.row.transient_sleep_s = transient_sleep_s;
        return result;
    }
}

async fn request_once(
    client: &Client,
    endpoint: &Endpoint,
    row: &PlanRow,
    config: &RunConfig,
) -> RpcResult {
    let request_started = Instant::now();
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            row.slot,
            {
                "commitment": config.commitment,
                "encoding": config.encoding,
                "transactionDetails": config.transaction_details,
                "maxSupportedTransactionVersion": config.max_supported_transaction_version,
                "rewards": config.rewards,
            }
        ]
    });
    let mut http = "000".to_string();
    let mut rpc = "unknown".to_string();
    let mut bytes_len = 0usize;
    let mut error = String::new();
    let mut body_value = None;

    let response = client
        .post(&endpoint.url)
        .header("content-type", "application/json")
        .body(payload.to_string())
        .send()
        .await;
    match response {
        Ok(response) => {
            http = response.status().as_u16().to_string();
            match response.bytes().await {
                Ok(bytes) => {
                    bytes_len = bytes.len();
                    match serde_json::from_slice::<Value>(&bytes) {
                        Ok(value) if value.get("error").is_some() => {
                            rpc = "error".to_string();
                            error = redact_sensitive(
                                &compact_json(value.get("error").unwrap_or(&Value::Null)),
                                endpoint,
                            );
                            body_value = Some(value);
                        }
                        Ok(value) => {
                            rpc = "ok".to_string();
                            body_value = Some(value);
                        }
                        Err(err) => {
                            rpc = "invalid_json".to_string();
                            error = format!("json_parse: {err}");
                        }
                    }
                }
                Err(err) => {
                    error = classify_reqwest_error("read_response_body", &err);
                }
            }
        }
        Err(err) => {
            error = classify_reqwest_error("send_request", &err);
        }
    }
    RpcResult {
        row: CallRow {
            endpoint: endpoint.label.clone(),
            epoch: row.epoch,
            slot: row.slot,
            kind: row.kind.clone(),
            sample_index: row.sample_index,
            is_primary: false,
            http,
            rpc,
            bytes: bytes_len,
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
            compared_to_primary: false,
            matches_primary: String::new(),
            diff_count: 0,
            diff_path: String::new(),
            error: one_line(&redact_sensitive(&error, endpoint)),
        },
        body: body_value,
    }
}

fn is_rate_limited(row: &CallRow) -> bool {
    row.http == "429" || {
        let error = row.error.to_ascii_lowercase();
        error.contains("rate limit")
            || error.contains("too many requests")
            || error.contains("\"code\":429")
    }
}

fn is_transient(row: &CallRow) -> bool {
    matches!(row.http.as_str(), "000" | "502" | "503" | "504") || {
        let error = row.error.to_ascii_lowercase();
        error.contains("timeout")
            || error.contains("service unavailable")
            || error.contains("bad gateway")
    }
}

fn classify_reqwest_error(context: &str, err: &reqwest::Error) -> String {
    let mut parts = vec![context.to_string()];
    if err.is_timeout() {
        parts.push("timeout".to_string());
    }
    if err.is_connect() {
        parts.push("connect".to_string());
    }
    if err.is_request() {
        parts.push("request".to_string());
    }
    if err.is_body() {
        parts.push("body".to_string());
    }
    if err.is_decode() {
        parts.push("decode".to_string());
    }
    if let Some(status) = err.status() {
        parts.push(format!("status={}", status.as_u16()));
    }
    if parts.len() == 1 {
        parts.push("failed".to_string());
    }
    parts.join(":")
}

fn diff_report(endpoint: &Value, primary: &Value, path: &str, max: usize) -> DiffReport {
    let mut diffs = Vec::new();
    let mut truncated = false;
    collect_diffs(endpoint, primary, path, max, &mut diffs, &mut truncated);
    DiffReport { diffs, truncated }
}

fn collect_diffs(
    endpoint: &Value,
    primary: &Value,
    path: &str,
    max: usize,
    diffs: &mut Vec<DiffSummary>,
    truncated: &mut bool,
) {
    if *truncated {
        return;
    }
    match (endpoint, primary) {
        (Value::Object(endpoint_map), Value::Object(primary_map)) => {
            let keys = endpoint_map
                .keys()
                .chain(primary_map.keys())
                .collect::<BTreeSet<_>>();
            for key in keys {
                let next_path = json_path_field(path, key);
                match (endpoint_map.get(key), primary_map.get(key)) {
                    (Some(endpoint_value), Some(primary_value)) => {
                        collect_diffs(
                            endpoint_value,
                            primary_value,
                            &next_path,
                            max,
                            diffs,
                            truncated,
                        );
                    }
                    (endpoint_value, primary_value) => {
                        push_diff(
                            next_path,
                            endpoint_value,
                            primary_value,
                            max,
                            diffs,
                            truncated,
                        );
                    }
                }
                if *truncated {
                    return;
                }
            }
        }
        (Value::Array(endpoint_values), Value::Array(primary_values)) => {
            for index in 0..endpoint_values.len().min(primary_values.len()) {
                let next_path = format!("{path}[{index}]");
                collect_diffs(
                    &endpoint_values[index],
                    &primary_values[index],
                    &next_path,
                    max,
                    diffs,
                    truncated,
                );
                if *truncated {
                    return;
                }
            }
            if endpoint_values.len() != primary_values.len() {
                let index = endpoint_values.len().min(primary_values.len());
                push_diff(
                    format!("{path}[{index}]"),
                    endpoint_values.get(index),
                    primary_values.get(index),
                    max,
                    diffs,
                    truncated,
                );
            }
        }
        _ if endpoint != primary && !ignored_diff(path, endpoint, primary) => {
            push_diff(
                path.to_string(),
                Some(endpoint),
                Some(primary),
                max,
                diffs,
                truncated,
            );
        }
        _ => {}
    }
}

fn ignored_diff(path: &str, endpoint: &Value, primary: &Value) -> bool {
    is_ui_token_amount_rounding_diff(path, endpoint, primary)
}

fn is_ui_token_amount_rounding_diff(path: &str, endpoint: &Value, primary: &Value) -> bool {
    if !path.ends_with(".uiTokenAmount.uiAmount") {
        return false;
    }
    let (Some(endpoint), Some(primary)) = (endpoint.as_f64(), primary.as_f64()) else {
        return false;
    };
    let delta = (endpoint - primary).abs();
    let scale = endpoint.abs().max(primary.abs()).max(1.0);
    delta <= 1e-9_f64.max(scale * 1e-13)
}

fn push_diff(
    path: String,
    endpoint: Option<&Value>,
    primary: Option<&Value>,
    max: usize,
    diffs: &mut Vec<DiffSummary>,
    truncated: &mut bool,
) {
    if max != 0 && diffs.len() >= max {
        *truncated = true;
        return;
    }
    diffs.push(DiffSummary {
        path,
        endpoint_value: summarize_json(endpoint),
        primary_value: summarize_json(primary),
    });
}

fn classify_diff_path(path: &str) -> &'static str {
    if path.contains(".instructions[") && path.ends_with(".data") {
        "instruction_data"
    } else if path.contains(".innerInstructions") && path.contains(".instructions[") {
        "inner_instruction"
    } else if path.contains(".meta.innerInstructions") {
        "inner_instructions"
    } else if path.contains(".meta.err") || path.contains(".meta.status") {
        "meta_error"
    } else if path.contains(".meta.preBalances") || path.contains(".meta.postBalances") {
        "balances"
    } else if path.contains(".meta.preTokenBalances") || path.contains(".meta.postTokenBalances") {
        "token_balances"
    } else if path.contains(".meta.rewards") || path.ends_with(".rewards") {
        "rewards"
    } else if path.contains(".transaction.signatures") {
        "signature"
    } else if path.contains(".transaction.message.accountKeys") {
        "account_keys"
    } else if path.contains(".blockhash")
        || path.contains(".previousBlockhash")
        || path.contains(".parentSlot")
    {
        "block_header"
    } else {
        "json_diff"
    }
}

fn json_path_field(path: &str, key: &str) -> String {
    if key
        .chars()
        .all(|ch| ch == '_' || ch == '-' || ch.is_ascii_alphanumeric())
    {
        format!("{path}.{key}")
    } else {
        let escaped = key.replace('\\', "\\\\").replace('\'', "\\'");
        format!("{path}['{escaped}']")
    }
}

fn summarize_json(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return "[missing]".to_string();
    };
    let mut out = compact_json(value);
    if out.len() > VALUE_SUMMARY_LIMIT {
        out.truncate(VALUE_SUMMARY_LIMIT);
        out.push_str("...[truncated]");
    }
    redact_url_like(&out)
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

fn write_calls(path: &Path, rows: &[CallRow]) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(
        out,
        "endpoint\tepoch\tslot\tkind\tsample_index\tis_primary\thttp\trpc\tbytes\telapsed_s\tfinal_attempt_elapsed_s\tattempts\trate_limited\trate_limit_events\trate_limit_retries\trate_limit_sleep_s\ttransient_retried\ttransient_events\ttransient_retries\ttransient_sleep_s\tcompared_to_primary\tmatches_primary\tdiff_count\tdiff_path\terror"
    )?;
    for row in rows {
        writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}\t{:.6}\t{}\t{}\t{}\t{:.6}\t{}\t{}\t{}\t{}\t{}",
            row.endpoint,
            row.epoch,
            row.slot,
            row.kind,
            row.sample_index,
            row.is_primary,
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
            row.compared_to_primary,
            row.matches_primary,
            row.diff_count,
            row.diff_path,
            row.error,
        )?;
    }
    Ok(())
}

fn write_mismatches(path: &Path, rows: &[MismatchRow]) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    for row in rows {
        serde_json::to_writer(&mut out, row)?;
        writeln!(out)?;
    }
    Ok(())
}

fn write_summary(
    path: &Path,
    calls: &[CallRow],
    mismatches: &[MismatchRow],
    endpoints: &[Endpoint],
    epochs: &[u64],
    cli: &Cli,
    planned_slots: usize,
    wall_s: f64,
) -> Result<()> {
    let mut by_endpoint = BTreeMap::new();
    for endpoint in endpoints {
        by_endpoint.insert(
            endpoint.label.clone(),
            summarize(
                calls.iter().filter(|row| row.endpoint == endpoint.label),
                mismatches
                    .iter()
                    .filter(|row| row.endpoint == endpoint.label),
            ),
        );
    }
    let endpoint_labels = endpoints
        .iter()
        .map(|endpoint| endpoint.label.as_str())
        .collect::<Vec<_>>();
    let report = GlobalReport {
        endpoints: endpoint_labels,
        primary: &cli.primary,
        epochs: epochs.to_vec(),
        planned_slots,
        planned_requests: planned_slots * endpoints.len(),
        transaction_details: &cli.transaction_details,
        rewards: cli.rewards,
        slot_concurrency: cli.slot_concurrency,
        timeout_s: cli.timeout,
        wall_s,
        global: summarize(calls.iter(), mismatches.iter()),
        by_endpoint,
        mismatch_paths: counts_map(mismatches.iter().map(|row| row.diff_path.as_str())),
    };
    serde_json::to_writer_pretty(File::create(path)?, &report)?;
    Ok(())
}

fn summarize<'a>(
    rows: impl Iterator<Item = &'a CallRow>,
    mismatches: impl Iterator<Item = &'a MismatchRow>,
) -> Summary {
    let rows = rows.collect::<Vec<_>>();
    let mismatches = mismatches.collect::<Vec<_>>();
    if rows.is_empty() {
        return Summary {
            calls: 0,
            ok: 0,
            errors: 0,
            compared: 0,
            matches: 0,
            mismatches: 0,
            not_compared: 0,
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
            mismatch_categories: String::new(),
            mismatch_paths: String::new(),
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
    let compared = rows.iter().filter(|row| row.compared_to_primary).count();
    let matches = rows
        .iter()
        .filter(|row| row.matches_primary == "true")
        .count();
    let not_compared = rows
        .iter()
        .filter(|row| row.matches_primary == "not_compared")
        .count();
    Summary {
        calls: rows.len(),
        ok,
        errors: rows.len() - ok,
        compared,
        matches,
        mismatches: mismatches.len(),
        not_compared,
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
        mismatch_categories: counts(mismatches.iter().map(|row| row.category.as_str())),
        mismatch_paths: counts(mismatches.iter().map(|row| row.diff_path.as_str())),
    }
}

fn counts<'a>(values: impl Iterator<Item = &'a str>) -> String {
    counts_map(values)
        .into_iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn counts_map<'a>(values: impl Iterator<Item = &'a str>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for value in values {
        *counts.entry(value.to_string()).or_default() += 1;
    }
    counts
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

fn sort_calls(rows: &mut [CallRow]) {
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
}

fn sort_mismatches(rows: &mut [MismatchRow]) {
    rows.sort_by(|a, b| {
        (
            a.endpoint.as_str(),
            a.epoch,
            a.slot,
            a.kind.as_str(),
            a.sample_index,
            a.diff_path.as_str(),
            a.diff_index,
        )
            .cmp(&(
                b.endpoint.as_str(),
                b.epoch,
                b.slot,
                b.kind.as_str(),
                b.sample_index,
                b.diff_path.as_str(),
                b.diff_index,
            ))
    });
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|err| format!("encode JSON error: {err}"))
}

fn one_line(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}

fn redact_sensitive(value: &str, endpoint: &Endpoint) -> String {
    redact_url_like(&value.replace(&endpoint.url, "[redacted-endpoint-url]"))
}

fn redact_url_like(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    loop {
        let http = rest.find("http://");
        let https = rest.find("https://");
        let Some(index) = (match (http, https) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }) else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..index]);
        let tail = &rest[index..];
        let end = tail
            .find(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | ')' | ']' | '}'))
            .unwrap_or(tail.len());
        out.push_str("[redacted-url]");
        rest = &tail[end..];
    }
    out
}

fn default_output_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!("/tmp/rpc-correctness-check-{stamp}"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_report_collects_multiple_instruction_data_diffs() {
        let endpoint = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "transactions": [{
                    "transaction": {
                        "signatures": ["sig"],
                        "message": {
                            "accountKeys": ["payer", "Vote111111111111111111111111111111111111111"],
                            "instructions": [
                                {"programIdIndex": 1, "accounts": [0], "data": ""},
                                {"programIdIndex": 1, "accounts": [0], "data": ""}
                            ]
                        }
                    }
                }]
            }
        });
        let primary = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "transactions": [{
                    "transaction": {
                        "signatures": ["sig"],
                        "message": {
                            "accountKeys": ["payer", "Vote111111111111111111111111111111111111111"],
                            "instructions": [
                                {"programIdIndex": 1, "accounts": [0], "data": "vote-data-0"},
                                {"programIdIndex": 1, "accounts": [0], "data": "vote-data-1"}
                            ]
                        }
                    }
                }]
            }
        });

        let report = diff_report(&endpoint, &primary, "$", 16);
        let paths = report
            .diffs
            .iter()
            .map(|diff| diff.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "$.result.transactions[0].transaction.message.instructions[0].data",
                "$.result.transactions[0].transaction.message.instructions[1].data",
            ]
        );
        assert!(!report.truncated);
        assert_eq!(classify_diff_path(paths[0]), "instruction_data");
    }

    #[test]
    fn diff_report_marks_truncation_at_limit() {
        let endpoint = json!([0, 0, 0]);
        let primary = json!([1, 1, 1]);

        let report = diff_report(&endpoint, &primary, "$", 2);

        assert_eq!(report.diffs.len(), 2);
        assert!(report.truncated);
    }

    #[test]
    fn diff_report_ignores_ui_token_amount_rounding_noise() {
        let endpoint = json!({
            "result": {
                "transactions": [{
                    "meta": {
                        "postTokenBalances": [{
                            "uiTokenAmount": {
                                "amount": "9968552975221405",
                                "decimals": 2,
                                "uiAmount": 99685529752.21405,
                                "uiAmountString": "99685529752214.05"
                            }
                        }]
                    }
                }]
            }
        });
        let primary = json!({
            "result": {
                "transactions": [{
                    "meta": {
                        "postTokenBalances": [{
                            "uiTokenAmount": {
                                "amount": "9968552975221405",
                                "decimals": 2,
                                "uiAmount": 99685529752.21404,
                                "uiAmountString": "99685529752214.05"
                            }
                        }]
                    }
                }]
            }
        });

        let report = diff_report(&endpoint, &primary, "$", 16);

        assert!(report.diffs.is_empty());
        assert!(!report.truncated);
    }

    #[test]
    fn mismatch_context_extracts_transaction_signature_and_program() {
        let body = json!({
            "result": {
                "transactions": [{
                    "transaction": {
                        "signatures": ["vote-signature"],
                        "message": {
                            "accountKeys": ["payer", "Vote111111111111111111111111111111111111111"],
                            "instructions": [
                                {"programIdIndex": 1, "accounts": [0], "data": ""}
                            ]
                        }
                    }
                }]
            }
        });
        let context = mismatch_context(
            None,
            Some(&body),
            "$.result.transactions[0].transaction.message.instructions[0].data",
        );

        assert_eq!(context.transaction_index, "0");
        assert_eq!(context.signature, "vote-signature");
        assert_eq!(context.instruction_index, "0");
        assert_eq!(context.program_id_index, "1");
        assert_eq!(
            context.program_id,
            "Vote111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn mismatch_context_extracts_inner_instruction_program() {
        let body = json!({
            "result": {
                "transactions": [{
                    "transaction": {
                        "signatures": ["inner-signature"],
                        "message": {
                            "accountKeys": ["payer", "11111111111111111111111111111111", "Vote111111111111111111111111111111111111111"],
                            "instructions": []
                        }
                    },
                    "meta": {
                        "innerInstructions": [{
                            "index": 0,
                            "instructions": [
                                {"programIdIndex": 2, "accounts": [0], "data": ""}
                            ]
                        }]
                    }
                }]
            }
        });
        let context = mismatch_context(
            None,
            Some(&body),
            "$.result.transactions[0].meta.innerInstructions[0].instructions[0].data",
        );

        assert_eq!(context.transaction_index, "0");
        assert_eq!(context.signature, "inner-signature");
        assert_eq!(context.instruction_index, "inner:0:0");
        assert_eq!(context.program_id_index, "2");
        assert_eq!(
            context.program_id,
            "Vote111111111111111111111111111111111111111"
        );
    }
}
