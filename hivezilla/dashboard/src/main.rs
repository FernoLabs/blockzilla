use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use futures_util::StreamExt;
use topcoat::{
    Result,
    router::{Router, RouterBuilderDiscoverExt, header, page, route},
    view::{View, view},
};

const DEFAULT_REPORTS_DIR: &str = "/home/ubuntu/live/hivezilla-validator-20260930/reports";
const DEFAULT_METRICS_URL: &str = "http://127.0.0.1:8999/metrics";
const MAX_METRICS_BYTES: usize = 8 * 1024 * 1024;

#[tokio::main]
async fn main() {
    ensure_loopback_bind();
    let router = Router::builder().discover().build();
    topcoat::start(router)
        .await
        .expect("dashboard server failed");
}

fn ensure_loopback_bind() {
    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    assert!(
        is_loopback_host(&host),
        "HOST must be a loopback IP address"
    );
}

fn is_loopback_host(host: &str) -> bool {
    host.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

#[page("/")]
async fn overview() -> Result<impl View> {
    let snapshot = DashboardSnapshot::collect().await;
    let status_class = match snapshot.canary_state.as_str() {
        "READY" | "COMPLETE" => "state state-good",
        "WAITING" => "state state-waiting",
        _ => "state state-offline",
    };
    let metrics_label = if snapshot.direct.is_some() {
        "Direct health check"
    } else if snapshot.metrics_available {
        "Connected"
    } else {
        "Waiting for validator metrics"
    };

    Ok(view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <meta name="robots" content="noindex, nofollow" />
                <meta http-equiv="refresh" content="10" />
                <title>"Hivezilla validator"</title>
                <link rel="stylesheet" href="/app.css" />
            </head>
            <body>
                <main class="page">
                    <header class="topbar">
                        <h1>"Hivezilla validator"</h1>
                        <span class=(status_class)>
                            (snapshot.canary_state.as_str())
                        </span>
                    </header>

                    <section class="summary" aria-label="Validator status">
                        <div class="summary-item">
                            <span class="label">"Validator"</span>
                            <strong>(snapshot.validator_identity.as_str())</strong>
                        </div>
                        <div class="summary-item">
                            <span class="label">"Finalized slot"</span>
                            <strong>(snapshot.finalized_slot.as_deref().unwrap_or("Waiting"))</strong>
                        </div>
                        <div class="summary-item">
                            <span class="label">"Metrics"</span>
                            <strong>(metrics_label)</strong>
                        </div>
                        <div class="summary-item">
                            <span class="label">"Direct capture"</span>
                            <strong>(snapshot.direct.as_ref().map(|e| e.state.as_str()).unwrap_or("Not configured"))</strong>
                        </div>
                    </section>

                    <section class="section" aria-label="Current epoch and next epoch start">
                        <div class="section-heading"><h2>"Epoch timing"</h2></div>
                        if let Some(clock) = &snapshot.clock {
                            <div class="table-wrap"><table><tbody>
                                <tr><td>"Current epoch"</td><td class="value">(clock.epoch)</td></tr>
                                <tr><td>"Network epoch progress"</td><td class="value">(format!("{:.2}% · {} / {} slots", clock.slot_index as f64 * 100.0 / clock.total_slots as f64, clock.slot_index, clock.total_slots))</td></tr>
                                <tr><td>(format!("Epoch {} starts in", clock.next_epoch))</td><td class="value"><strong id="epoch-countdown" data-start-unix=(clock.estimated_start_unix.map(|n| n.to_string()).unwrap_or_default())>(clock.countdown_text())</strong></td></tr>
                                <tr><td>"Estimated start · Paris time"</td><td class="value"><span id="epoch-start-time">(if clock.estimated_start_unix.is_some() { "Loading local time" } else { "Unavailable" })</span></td></tr>
                                <tr><td>"Next epoch starts at slot"</td><td class="value">(clock.next_start_slot)</td></tr>
                            </tbody></table></div>
                            <p class="notice">"Start time uses the recent network slot rate. The estimate can change. Capture progress is shown below."</p>
                        } else {
                            <p class="notice">"Epoch timing is unavailable. The local node did not return a current epoch."</p>
                        }
                    </section>

                    if let Some(direct) = &snapshot.direct {
                        <section class="section" aria-label="Direct Agave capture and compaction">
                            <div class="section-heading">
                                <h2>"Direct Agave capture"</h2>
                                <span>(format!("Updated {} seconds ago", direct.age_seconds))</span>
                            </div>
                            <div class="table-wrap">
                                <table>
                                    <thead><tr><th>"Progress"</th><th>"Value"</th></tr></thead>
                                    <tbody>
                                        for (label, value) in &direct.rows {
                                            <tr><td>(label.as_str())</td><td class="value">(value.as_str())</td></tr>
                                        }
                                    </tbody>
                                </table>
                            </div>
                            for message in &direct.messages {
                                <p class="notice">(message.as_str())</p>
                            }
                        </section>
                    }

                    <section class="section" aria-label="Epoch collection and compaction">
                        if let Some(epoch) = &snapshot.epoch {
                            <div class="section-heading">
                                <h2>(format!("Earlier gRPC capture — epoch {}", epoch.epoch))</h2>
                                <span>(format!("Updated {} seconds ago", epoch.age_seconds))</span>
                            </div>
                            <p class="notice">(epoch.note.as_str())</p>
                            <div class="table-wrap">
                                <table>
                                    <thead><tr><th>"Progress"</th><th>"Value"</th></tr></thead>
                                    <tbody>
                                        <tr><td>"Blocks collected"</td><td class="value">(epoch.collected)</td></tr>
                                        <tr><td>"Blocks compacted"</td><td class="value">(epoch.compacted)</td></tr>
                                        <tr><td>"Waiting for compaction"</td><td class="value">(epoch.collected - epoch.compacted)</td></tr>
                                        <tr><td>"Epoch slots covered"</td><td class="value">(format!("{} / {} ({:.2}%)", epoch.covered, epoch.total_slots, epoch.covered as f64 * 100.0 / epoch.total_slots as f64))</td></tr>
                                        <tr><td>"Skipped slots"</td><td class="value">(epoch.skipped)</td></tr>
                                        <tr><td>"Missing blocks"</td><td class="value">(if epoch.collected == 0 && epoch.missing == 0 { "Not checked".to_owned() } else { epoch.missing.to_string() })</td></tr>
                                        <tr><td>"Last block collected"</td><td class="value">(epoch.last_collected.as_str())</td></tr>
                                        <tr><td>"Last block compacted"</td><td class="value">(epoch.last_compacted.as_str())</td></tr>
                                        <tr><td>"Raw data saved"</td><td class="value">(epoch.raw_size.as_str())</td></tr>
                                        <tr><td>"Compact data saved"</td><td class="value">(epoch.compact_size.as_str())</td></tr>
                                        <tr><td>"Capture"</td><td class="value">(epoch.state.as_str())</td></tr>
                                        <tr><td>"Compaction"</td><td class="value">(epoch.compaction_state.as_str())</td></tr>
                                        <tr><td>"Archive verification"</td><td class="value">"Skipped"</td></tr>
                                        <tr><td>"Upload"</td><td class="value">"Disabled"</td></tr>
                                    </tbody>
                                </table>
                            </div>
                            <p class="notice">(format!("Slot range {} to {}. Counts include saved blocks from this epoch only.", epoch.start, epoch.end - 1))</p>
                            if let Some(error) = &epoch.error {
                                <details class="epoch-error"><summary>"Capture or compaction error"</summary><pre>(error.as_str())</pre></details>
                            }
                        } else {
                            <div class="section-heading"><h2>"Epoch run"</h2></div>
                            <p class="notice">"No current epoch progress report is available."</p>
                        }
                    </section>

                    <section class="section">
                        <div class="section-heading">
                            <h2>"Node"</h2>
                            <span>(format!("Updated {}", snapshot.report_age))</span>
                        </div>
                        <dl class="detail-grid">
                            <div><dt>"Validator service"</dt><dd>(snapshot.service_state.as_str())</dd></div>
                            <div><dt>"Process ID"</dt><dd>(snapshot.validator_pid.as_deref().unwrap_or("—"))</dd></div>
                            <div><dt>"Genesis"</dt><dd class="mono">(snapshot.genesis_hash.as_str())</dd></div>
                            <div><dt>"Readiness report"</dt><dd>(snapshot.report_age.as_str())</dd></div>
                        </dl>
                        <p class="notice">(snapshot.node_note.as_str())</p>
                    </section>

                    if snapshot.direct.is_none() {
                    <section class="section">
                        <div class="section-heading">
                            <h2>"Validator metrics"</h2>
                            <span>(format!("{} metric names · {} samples", snapshot.metric_families, snapshot.metric_samples))</span>
                        </div>
                        if snapshot.metrics_available {
                            <div class="table-wrap">
                                <table>
                                    <thead><tr><th>"Metric"</th><th>"Value"</th></tr></thead>
                                    <tbody>
                                        for metric in &snapshot.metrics {
                                            <tr><td class="mono">(metric.name.as_str())</td><td class="mono value">(metric.value.as_str())</td></tr>
                                        }
                                    </tbody>
                                </table>
                            </div>
                            if snapshot.metrics.is_empty() {
                                <p class="notice">"The endpoint is live, but no supported validator metrics were found."</p>
                            }
                        } else {
                            <p class="notice">"The metrics endpoint is on loopback. It will appear here when the validator starts it."</p>
                        }
                    </section>

                    <section class="section">
                        <div class="section-heading">
                            <h2>"Execution canary"</h2>
                            <span>(format!("{} readiness checks", snapshot.readiness_samples))</span>
                        </div>
                        <dl class="detail-grid">
                            <div><dt>"Capture"</dt><dd>(snapshot.capture_status.as_str())</dd></div>
                            <div><dt>"Controller launches"</dt><dd>(snapshot.controller_launches.as_str())</dd></div>
                            <div><dt>"Finalized slot advancing"</dt><dd>(snapshot.finalized_slot.as_deref().unwrap_or("Not yet"))</dd></div>
                            <div><dt>"Upload"</dt><dd>"Disabled for this canary"</dd></div>
                        </dl>
                    </section>
                    }
                    <footer>"Refreshes every 10 seconds · bound to localhost by default"</footer>
                </main>
                <script src="/epoch-clock.js"></script>
            </body>
        </html>
    })
}

#[route(GET "/app.css")]
async fn app_css() -> Result<(
    [(topcoat::router::HeaderName, &'static str); 2],
    &'static str,
)> {
    Ok((
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("../assets/app.css"),
    ))
}

#[derive(Debug, Default)]
struct DashboardSnapshot {
    clock: Option<EpochClock>,
    epoch: Option<EpochSnapshot>,
    direct: Option<DirectSnapshot>,
    canary_state: String,
    service_state: String,
    validator_identity: String,
    genesis_hash: String,
    validator_pid: Option<String>,
    finalized_slot: Option<String>,
    report_age: String,
    capture_status: String,
    controller_launches: String,
    readiness_samples: usize,
    node_note: String,
    metrics_available: bool,
    metric_samples: usize,
    metric_families: usize,
    metrics: Vec<MetricRow>,
}

#[route(GET "/epoch-clock.js")]
async fn epoch_clock_js() -> Result<(
    [(topcoat::router::HeaderName, &'static str); 2],
    &'static str,
)> {
    Ok((
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("../assets/epoch-clock.js"),
    ))
}

#[derive(Debug)]
struct EpochClock {
    epoch: u64,
    next_epoch: u64,
    slot_index: u64,
    total_slots: u64,
    next_start_slot: u64,
    estimated_seconds: Option<u64>,
    estimated_start_unix: Option<u64>,
}

impl EpochClock {
    fn countdown_text(&self) -> String {
        self.estimated_seconds
            .map(|n| format!("{}h {}m", n / 3600, (n % 3600) / 60))
            .unwrap_or_else(|| "Estimate unavailable".into())
    }
}

fn epoch_clock_from_rpc(
    epoch: &serde_json::Value,
    samples: &serde_json::Value,
    now: u64,
) -> anyhow::Result<EpochClock> {
    let n = |key: &str| {
        epoch[key]
            .as_u64()
            .with_context(|| format!("missing epoch timing field {key}"))
    };
    let epoch_number = n("epoch")?;
    let slot_index = n("slotIndex")?;
    let total_slots = n("slotsInEpoch")?;
    let absolute_slot = n("absoluteSlot")?;
    anyhow::ensure!(
        total_slots > 0 && slot_index < total_slots,
        "invalid epoch slot counters"
    );
    let next_start_slot = absolute_slot
        .checked_sub(slot_index)
        .and_then(|n| n.checked_add(total_slots))
        .context("epoch slot range overflow")?;
    let rate = samples
        .as_array()
        .filter(|s| !s.is_empty())
        .and_then(|items| {
            let newest = items.first()?["slot"].as_u64()?;
            if absolute_slot.saturating_sub(newest) > 1000 {
                return None;
            }
            let mut slots = 0u64;
            let mut seconds = 0u64;
            for item in items {
                slots = slots.checked_add(item["numSlots"].as_u64()?)?;
                seconds = seconds.checked_add(item["samplePeriodSecs"].as_u64()?)?;
            }
            let rate = slots as f64 / seconds as f64;
            (rate.is_finite() && (0.1..=20.0).contains(&rate)).then_some(rate)
        });
    let estimated_seconds = rate.map(|r| ((total_slots - slot_index) as f64 / r).ceil() as u64);
    let estimated_start_unix = estimated_seconds.and_then(|n| now.checked_add(n));
    Ok(EpochClock {
        epoch: epoch_number,
        next_epoch: epoch_number.checked_add(1).context("epoch overflow")?,
        slot_index,
        total_slots,
        next_start_slot,
        estimated_seconds,
        estimated_start_unix,
    })
}

async fn live_epoch_clock() -> anyhow::Result<EpochClock> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let response = client.post("http://127.0.0.1:8899/")
        .header("Content-Type", "application/json")
        .body(r#"[{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[{"commitment":"finalized"}]},{"jsonrpc":"2.0","id":2,"method":"getRecentPerformanceSamples","params":[10]}]"#)
        .send().await?.error_for_status()?;
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        anyhow::ensure!(
            bytes.len() + chunk.len() <= 16 * 1024,
            "epoch RPC response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let items = value.as_array().context("epoch RPC batch missing")?;
    let epoch = items
        .iter()
        .find(|v| v["id"] == 1)
        .context("epoch RPC result missing")?;
    let missing_samples = serde_json::Value::Null;
    let samples = items
        .iter()
        .find(|v| v["id"] == 2)
        .map(|v| &v["result"])
        .unwrap_or(&missing_samples);
    epoch_clock_from_rpc(
        &epoch["result"],
        samples,
        unix_nanoseconds() / 1_000_000_000,
    )
}

#[derive(Debug, PartialEq, Eq)]
struct MetricRow {
    name: String,
    value: String,
}

impl DashboardSnapshot {
    async fn collect() -> Self {
        let mut snapshot = Self {
            canary_state: "NO REPORT".to_owned(),
            service_state: "Unknown".to_owned(),
            validator_identity: "—".to_owned(),
            genesis_hash: "—".to_owned(),
            report_age: "Unavailable".to_owned(),
            capture_status: "Not started".to_owned(),
            controller_launches: "0".to_owned(),
            node_note: "No readiness report is available yet.".to_owned(),
            ..Self::default()
        };

        if let Some(path) = latest_report_path() {
            match read_report(&path) {
                Ok((report, age)) => {
                    snapshot.canary_state =
                        string_at(&report, &["state"]).unwrap_or_else(|| "UNKNOWN".to_owned());
                    snapshot.validator_pid = string_at(&report, &["validator", "pid"]);
                    snapshot.validator_identity = string_after(&report, "--expected-node-identity")
                        .unwrap_or_else(|| "—".to_owned());
                    snapshot.genesis_hash = string_after(&report, "--expected-genesis-hash")
                        .unwrap_or_else(|| "—".to_owned());
                    snapshot.controller_launches = string_at(&report, &["controller_launches"])
                        .unwrap_or_else(|| "0".to_owned());
                    snapshot.readiness_samples = report
                        .get("samples")
                        .and_then(serde_json::Value::as_array)
                        .map_or(0, Vec::len);
                    snapshot.capture_status = if report
                        .get("complete")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                    {
                        "Complete".to_owned()
                    } else if snapshot.readiness_samples > 0 && snapshot.canary_state == "WAITING" {
                        "Waiting for node".to_owned()
                    } else if snapshot.canary_state == "ERROR" {
                        "Failed".to_owned()
                    } else {
                        "Not started".to_owned()
                    };
                    snapshot.report_age = format!("{age} seconds ago");
                    snapshot.service_state = match report
                        .get("validator_service_changed")
                        .and_then(serde_json::Value::as_bool)
                    {
                        Some(false) => "Unchanged since launch".to_owned(),
                        Some(true) => "Changed".to_owned(),
                        None => "Unknown".to_owned(),
                    };
                    let sample = report
                        .get("samples")
                        .and_then(serde_json::Value::as_array)
                        .and_then(|samples| samples.last());
                    snapshot.finalized_slot = sample
                        .and_then(|sample| sample.get("finalized_slot"))
                        .and_then(serde_json::Value::as_u64)
                        .map(|slot| slot.to_string());
                    snapshot.node_note = if snapshot.finalized_slot.is_some() {
                        "The validator reports a finalized slot.".to_owned()
                    } else {
                        "RPC is not ready. The capture waits for a healthy node and an advancing finalized slot.".to_owned()
                    };
                }
                Err(_) => {
                    snapshot.node_note = "The readiness report could not be read.".to_owned();
                }
            }
        }

        snapshot.epoch = read_epoch_progress().ok();
        snapshot.direct = read_direct_progress().ok();
        snapshot.apply_direct_status();
        snapshot.clock = live_epoch_clock().await.ok();
        if let Ok(slot) = live_finalized_slot().await {
            snapshot.finalized_slot = Some(slot.to_string());
            snapshot.node_note = "The finalized slot is read from the running node.".into();
        }
        if let Ok(metrics) = scrape_metrics().await {
            snapshot.metrics_available = true;
            snapshot.metric_samples = metrics.samples;
            snapshot.metric_families = metrics.families;
            snapshot.metrics = metrics.rows;
        }
        snapshot
    }

    fn apply_direct_status(&mut self) {
        let Some(direct) = &self.direct else { return };
        self.finalized_slot = None;
        self.validator_identity = direct.node_identity.clone();
        self.genesis_hash = direct.genesis_hash.clone();
        self.validator_pid = direct.validator_pid.clone();
        self.report_age = format!("{} seconds ago", direct.age_seconds);
        if direct.age_seconds > 45 {
            self.canary_state = "STATUS OLD".into();
            self.service_state = "Health not confirmed".into();
        } else if direct.validator_healthy {
            self.canary_state = "READY".into();
            self.service_state = "Running".into();
        } else {
            self.canary_state = "WAITING".into();
            self.service_state = "Health not confirmed".into();
        }
        self.node_note = "Current node health comes from the direct capture health check.".into();
    }
}

#[derive(Debug)]
struct DirectSnapshot {
    state: String,
    age_seconds: u64,
    validator_healthy: bool,
    node_identity: String,
    genesis_hash: String,
    validator_pid: Option<String>,
    rows: Vec<(String, String)>,
    messages: Vec<String>,
}

fn read_direct_progress() -> anyhow::Result<DirectSnapshot> {
    let path = std::env::var_os("HIVEZILLA_DIRECT_PROGRESS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_REPORTS_DIR).join("direct-agave-health.json"));
    anyhow::ensure!(
        fs::metadata(&path)?.len() <= 64 * 1024,
        "direct report is too large"
    );
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    direct_from_value(&value)
}

fn direct_from_value(value: &serde_json::Value) -> anyhow::Result<DirectSnapshot> {
    anyhow::ensure!(
        value["schema"] == "hivezilla-direct-pipeline-health-v1",
        "unknown direct progress schema"
    );
    let updated = value["updated_unix_ms"]
        .as_u64()
        .context("missing direct report time")?;
    let age_seconds = (unix_nanoseconds() / 1_000_000).saturating_sub(updated) / 1000;
    let state = if age_seconds > 45 {
        "STATUS OLD".to_owned()
    } else {
        value["state"]
            .as_str()
            .context("missing direct state")?
            .to_owned()
    };
    let text = |v: &serde_json::Value| {
        v.as_str()
            .map(str::to_owned)
            .or_else(|| v.as_u64().map(|n| n.to_string()))
            .unwrap_or_else(|| "Waiting".to_owned())
    };
    let size = |v: &serde_json::Value| {
        v.as_u64()
            .map(|n| format!("{:.2} GiB", n as f64 / 1_073_741_824.0))
            .unwrap_or_else(|| "Waiting".to_owned())
    };
    let worker = &value["worker"];
    let capture = &value["capture"];
    let rows = vec![
        ("Capture".into(), text(&capture["state"])),
        ("Compaction".into(), text(&worker["state"])),
        (
            "Latest rooted slot saved".into(),
            text(&capture["latest_rooted_slot"]),
        ),
        (
            "Last block compacted".into(),
            text(&worker["last_compact_slot"]),
        ),
        (
            "Compaction delay in slots".into(),
            text(&value["compact_lag_slots"]),
        ),
        ("Blocks compacted".into(), text(&worker["compacted_blocks"])),
        (
            "Transactions compacted".into(),
            text(&worker["compacted_transactions"]),
        ),
        ("Source data saved".into(), size(&capture["journal_bytes"])),
        (
            "Source storage limit".into(),
            size(&capture["journal_limit_bytes"]),
        ),
        ("Free disk space".into(), size(&value["free_disk_bytes"])),
        ("Raw shreds".into(), text(&value["shreds"]["state"])),
        ("Upload".into(), "Not configured".into()),
    ];
    let mut messages = vec![
        "Counts apply to the current capture session. A full epoch has not yet been captured."
            .into(),
    ];
    for key in ["problems", "warnings"] {
        if let Some(items) = value[key].as_array() {
            messages.extend(items.iter().filter_map(|v| v.as_str().map(str::to_owned)));
        }
    }
    if age_seconds > 45 {
        messages.push("The direct capture status is old. Check the health service.".into());
    }
    Ok(DirectSnapshot {
        state,
        age_seconds,
        validator_healthy: value["validator"]["health"] == "ok",
        node_identity: text(
            value["validator"]
                .get("identity")
                .unwrap_or(&capture["node_identity"]),
        ),
        genesis_hash: text(
            value["validator"]
                .get("genesis_hash")
                .unwrap_or(&capture["genesis_hash"]),
        ),
        validator_pid: value["capture"]["validator_pid"]
            .as_u64()
            .map(|pid| pid.to_string()),
        rows,
        messages,
    })
}

#[derive(Debug)]
struct EpochSnapshot {
    epoch: u64,
    state: String,
    compaction_state: String,
    age_seconds: u64,
    collected: u64,
    compacted: u64,
    covered: u64,
    total_slots: u64,
    skipped: u64,
    missing: u64,
    last_collected: String,
    last_compacted: String,
    raw_size: String,
    compact_size: String,
    start: u64,
    end: u64,
    note: String,
    error: Option<String>,
}
fn read_epoch_progress() -> anyhow::Result<EpochSnapshot> {
    let path = std::env::var_os("HIVEZILLA_EPOCH_PROGRESS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_REPORTS_DIR).join("epoch-progress.json"));
    anyhow::ensure!(
        fs::metadata(&path)?.len() <= 64 * 1024,
        "epoch progress report is too large"
    );
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    epoch_from_value(&value)
}
fn epoch_from_value(value: &serde_json::Value) -> anyhow::Result<EpochSnapshot> {
    anyhow::ensure!(
        value["schema"] == "hivezilla-epoch-progress-v1",
        "unknown epoch progress schema"
    );
    let n = |name: &str| {
        value[name]
            .as_u64()
            .with_context(|| format!("missing epoch field {name}"))
    };
    let collected = n("collected_blocks")?;
    let compacted = n("compacted_blocks")?;
    let covered = n("covered_slots")?;
    let total_slots = n("slots_in_epoch")?;
    let start = n("start_slot")?;
    let end = n("end_slot_exclusive")?;
    let skipped = n("skipped_slots")?;
    anyhow::ensure!(
        total_slots > 0
            && end.checked_sub(start) == Some(total_slots)
            && compacted <= collected
            && collected <= covered
            && covered <= total_slots
            && collected.checked_add(skipped) == Some(covered),
        "invalid epoch progress counters"
    );
    let age_seconds = unix_nanoseconds()
        .saturating_div(1_000_000)
        .saturating_sub(n("updated_unix_ms")?)
        .saturating_div(1_000);
    let state = value["state"]
        .as_str()
        .context("missing epoch state")?
        .to_owned();
    anyhow::ensure!(
        state != "COMPLETE" || (covered == total_slots && compacted == collected),
        "complete epoch report has unfinished work"
    );
    let note = if age_seconds > 180 {
        "The progress report is old. Check the epoch capture service."
    } else if state == "ARMED" {
        "The capture is ready and waits for this epoch to start."
    } else if state == "COMPLETE" {
        "All epoch slots are covered and all saved blocks are compacted."
    } else if state == "GAP" {
        "A produced block is missing. Capture has not advanced past the gap."
    } else {
        "Saved blocks are compacted in batches. Skipped slots come from the local finalized ledger."
    }
    .to_owned();
    Ok(EpochSnapshot {
        epoch: n("epoch")?,
        state,
        age_seconds,
        collected,
        compacted,
        covered,
        total_slots,
        skipped,
        missing: n("missing_blocks")?,
        start,
        end,
        note,
        compaction_state: value["compaction_state"]
            .as_str()
            .unwrap_or("Unknown")
            .to_owned(),
        last_collected: value["last_collected_slot"]
            .as_u64()
            .map(|n| n.to_string())
            .unwrap_or("Waiting".into()),
        last_compacted: value["last_compacted_slot"]
            .as_u64()
            .map(|n| n.to_string())
            .unwrap_or("Waiting".into()),
        raw_size: format!("{:.2} GiB", n("raw_bytes")? as f64 / 1_073_741_824.0),
        compact_size: format!("{:.2} GiB", n("compact_bytes")? as f64 / 1_073_741_824.0),
        error: value["capture_error"]
            .as_str()
            .or_else(|| value["compaction_error"].as_str())
            .map(str::to_owned),
    })
}
async fn live_finalized_slot() -> anyhow::Result<u64> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let response = client
        .post("http://127.0.0.1:8899/")
        .header("Content-Type", "application/json")
        .body(
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}"#,
        )
        .send()
        .await?
        .error_for_status()?;
    let bytes = response.bytes().await?;
    anyhow::ensure!(bytes.len() <= 4096, "RPC response too large");
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    value["result"].as_u64().context("finalized slot missing")
}

#[derive(Debug, Default)]
struct MetricsSnapshot {
    samples: usize,
    families: usize,
    rows: Vec<MetricRow>,
}

async fn scrape_metrics() -> anyhow::Result<MetricsSnapshot> {
    let url = std::env::var("HIVEZILLA_VALIDATOR_METRICS_URL")
        .unwrap_or_else(|_| DEFAULT_METRICS_URL.to_owned());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let response = client.get(url).send().await?.error_for_status()?;
    if let Some(length) = response.content_length() {
        anyhow::ensure!(
            length as usize <= MAX_METRICS_BYTES,
            "metrics payload is too large"
        );
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        anyhow::ensure!(
            bytes.len() + chunk.len() <= MAX_METRICS_BYTES,
            "metrics payload is too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    let body = std::str::from_utf8(&bytes).context("metrics payload is not UTF-8")?;
    Ok(parse_metrics(body))
}

fn parse_metrics(body: &str) -> MetricsSnapshot {
    let mut parsed = MetricsSnapshot::default();
    let mut families = BTreeSet::new();
    let mut series = BTreeMap::<String, (usize, String)>::new();
    for line in body
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let Some((sample, value)) = line.rsplit_once(char::is_whitespace) else {
            continue;
        };
        let name_end = sample
            .find(|character| character == '{' || character == '[')
            .unwrap_or(sample.len());
        let name = &sample[..name_end];
        if name.is_empty() || !value.parse::<f64>().is_ok_and(f64::is_finite) {
            continue;
        }
        parsed.samples += 1;
        families.insert(name.to_owned());
        if is_interesting_metric(name) && !is_histogram_component(name) {
            let entry = series
                .entry(name.to_owned())
                .or_insert_with(|| (0, value.to_owned()));
            entry.0 += 1;
        }
    }
    parsed.families = families.len();
    parsed.rows = series
        .into_iter()
        // Skip metric families with many labelled series. Showing one would hide
        // the other workers and suggest that it is the total.
        .filter(|(_, (count, _))| *count == 1)
        .take(16)
        .map(|(name, (_, value))| MetricRow { name, value })
        .collect();
    parsed
}

fn is_histogram_component(name: &str) -> bool {
    ["_bucket", "_sum", "_count"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

fn is_interesting_metric(name: &str) -> bool {
    [
        "slot",
        "block",
        "shred",
        "bank",
        "rpc",
        "ledger",
        "process_cpu",
        "process_resident",
    ]
    .iter()
    .any(|needle| name.contains(needle))
}

fn latest_report_path() -> Option<PathBuf> {
    let dir = std::env::var_os("HIVEZILLA_CANARY_REPORTS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_REPORTS_DIR));
    fs::read_dir(dir)
        .ok()?
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.starts_with("canary-readiness-") && name.ends_with(".json")
            })
        })
        .filter_map(|entry| {
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((modified, entry.path()))
        })
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path)
}

fn read_report(path: &Path) -> anyhow::Result<(serde_json::Value, u64)> {
    anyhow::ensure!(
        fs::metadata(path)?.len() <= 1024 * 1024,
        "readiness report is too large"
    );
    let text = fs::read_to_string(path).context("readiness report is not readable")?;
    let report: serde_json::Value = serde_json::from_str(&text)?;
    let age = report
        .get("updated_unix_ns")
        .and_then(serde_json::Value::as_u64)
        .map(|updated_ns| unix_nanoseconds().saturating_sub(updated_ns) / 1_000_000_000)
        .unwrap_or_default();
    Ok((report, age))
}

fn string_at(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    let value = path
        .iter()
        .try_fold(value, |value, part| value.get(*part))?;
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|number| number.to_string()))
}

fn string_after(report: &serde_json::Value, expected_flag: &str) -> Option<String> {
    let args = report.get("controller_argv")?.as_array()?;
    let index = args
        .iter()
        .position(|argument| argument.as_str() == Some(expected_flag))?;
    args.get(index + 1)?.as_str().map(str::to_owned)
}

fn unix_nanoseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    #[test]
    fn epoch_countdown_uses_network_rate_and_handles_rollover() {
        let samples = serde_json::json!([{"slot":990,"numSlots":240,"samplePeriodSecs":60}]);
        let mut epoch =
            serde_json::json!({"epoch":9,"slotIndex":90,"slotsInEpoch":100,"absoluteSlot":990});
        let before = super::epoch_clock_from_rpc(&epoch, &samples, 1000).unwrap();
        assert_eq!(before.next_epoch, 10);
        assert_eq!(before.next_start_slot, 1000);
        assert_eq!(before.estimated_seconds, Some(3));
        epoch["epoch"] = serde_json::json!(10);
        epoch["slotIndex"] = serde_json::json!(0);
        epoch["absoluteSlot"] = serde_json::json!(1000);
        let after = super::epoch_clock_from_rpc(&epoch, &samples, 1000).unwrap();
        assert_eq!(after.next_epoch, 11);
        assert_eq!(after.next_start_slot, 1100);
        assert_eq!(after.estimated_start_unix, Some(1025));
    }

    #[test]
    fn epoch_countdown_does_not_invent_a_rate() {
        let epoch =
            serde_json::json!({"epoch":9,"slotIndex":90,"slotsInEpoch":100,"absoluteSlot":990});
        for samples in [
            serde_json::json!([]),
            serde_json::json!([{"slot":990,"numSlots":0,"samplePeriodSecs":60}]),
        ] {
            let clock = super::epoch_clock_from_rpc(&epoch, &samples, 1000).unwrap();
            assert_eq!(clock.estimated_start_unix, None);
            assert_eq!(clock.countdown_text(), "Estimate unavailable");
        }
        let mut invalid = epoch;
        invalid["slotIndex"] = serde_json::json!(100);
        assert!(super::epoch_clock_from_rpc(&invalid, &serde_json::json!([]), 1000).is_err());
    }

    use super::*;

    #[test]
    fn direct_health_replaces_old_ready_node_and_slot() {
        for (time, health, expected) in [
            (unix_nanoseconds() / 1_000_000, "unavailable", "WAITING"),
            (1, "ok", "STATUS OLD"),
        ] {
            let value = serde_json::json!({
                "schema":"hivezilla-direct-pipeline-health-v1", "updated_unix_ms":time,
                "state":"FAILED", "validator":{"health":health},
                "capture":{"validator_pid":123,"node_identity":"current-node","genesis_hash":"mainnet"}
            });
            let mut snapshot = DashboardSnapshot {
                canary_state: "READY".into(),
                finalized_slot: Some("old-slot".into()),
                direct: Some(direct_from_value(&value).unwrap()),
                ..DashboardSnapshot::default()
            };
            snapshot.apply_direct_status();
            assert_eq!(snapshot.canary_state, expected);
            assert_eq!(snapshot.finalized_slot, None);
            assert_eq!(snapshot.validator_pid.as_deref(), Some("123"));
            assert_eq!(snapshot.validator_identity, "current-node");
        }
    }

    #[test]
    fn direct_capture_shows_failure_when_source_process_is_live() {
        let value = serde_json::json!({
            "schema":"hivezilla-direct-pipeline-health-v1",
            "updated_unix_ms":unix_nanoseconds() / 1_000_000,
            "state":"FAILED", "capture":{"state":"FAILED","writer_live":true},
            "worker":{"state":"FAILED"}, "problems":["Capture queue limit reached"],
            "warnings":[]
        });
        let report = direct_from_value(&value).unwrap();
        assert_eq!(report.state, "FAILED");
        assert!(
            report
                .messages
                .iter()
                .any(|m| m == "Capture queue limit reached")
        );
        assert_eq!(report.rows[0].1, "FAILED");
    }

    #[test]
    fn stale_direct_health_cannot_show_healthy_state() {
        let value = serde_json::json!({
            "schema":"hivezilla-direct-pipeline-health-v1",
            "updated_unix_ms":1, "state":"HEALTHY",
            "capture":{"state":"CAPTURING"}, "worker":{}, "problems":[], "warnings":[]
        });
        assert_eq!(direct_from_value(&value).unwrap().state, "STATUS OLD");
    }

    fn epoch_report() -> serde_json::Value {
        serde_json::json!({
            "schema":"hivezilla-epoch-progress-v1", "epoch":1047,
            "state":"CAPTURING", "compaction_state":"COMPACTING",
            "updated_unix_ms":unix_nanoseconds() / 1_000_000,
            "start_slot":452304000, "end_slot_exclusive":452736000,
            "slots_in_epoch":432000, "covered_slots":130,
            "collected_blocks":128, "compacted_blocks":64,
            "skipped_slots":2, "missing_blocks":0,
            "last_collected_slot":452304129, "last_compacted_slot":452304063,
            "raw_bytes":1073741824, "compact_bytes":536870912
        })
    }

    #[test]
    fn epoch_progress_keeps_capture_and_compaction_separate() {
        let report = epoch_from_value(&epoch_report()).unwrap();
        assert_eq!(
            (report.collected, report.compacted, report.skipped),
            (128, 64, 2)
        );
        assert_eq!((report.covered, report.total_slots), (130, 432000));
        assert_eq!(report.raw_size, "1.00 GiB");
        assert_eq!(report.compact_size, "0.50 GiB");
    }

    #[test]
    fn epoch_progress_rejects_false_counts_and_completion() {
        let mut value = epoch_report();
        value["compacted_blocks"] = 129.into();
        assert!(epoch_from_value(&value).is_err());
        value = epoch_report();
        value["skipped_slots"] = 3.into();
        assert!(epoch_from_value(&value).is_err());
        value = epoch_report();
        value["state"] = "COMPLETE".into();
        assert!(epoch_from_value(&value).is_err());
        value["covered_slots"] = 432000.into();
        value["skipped_slots"] = (432000 - 128).into();
        value["compacted_blocks"] = 128.into();
        assert!(epoch_from_value(&value).is_ok());
    }

    #[test]
    fn bind_host_must_be_loopback() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("213.239.141.61"));
    }

    #[tokio::test]
    async fn serves_dashboard_and_stylesheet() {
        use topcoat::router::{Body, StatusCode, request::Request, to_bytes};

        let router = Router::builder().discover().build();
        let response = router
            .handle(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let html = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let html = std::str::from_utf8(&html).unwrap();
        assert!(html.contains("Hivezilla validator"));
        assert!(html.contains("noindex, nofollow"));
        if html.contains("Direct Agave capture") {
            assert!(html.contains("Direct health check"));
            assert!(!html.contains("Execution canary"));
        } else {
            assert!(html.contains("Validator metrics"));
        }

        let response = router
            .handle(
                Request::builder()
                    .uri("/app.css")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/css; charset=utf-8"
        );
    }

    #[test]
    fn parses_prometheus_metrics_without_exposing_labels() {
        let parsed = parse_metrics(
            "# HELP solana_slot Current slot\nsolana_slot{worker=\"8\"} 42\nsolana_rpc_requests_total 7\ninvalid NaN\n",
        );
        assert_eq!(parsed.samples, 2);
        assert_eq!(parsed.families, 2);
        assert_eq!(
            parsed.rows,
            vec![
                MetricRow {
                    name: "solana_rpc_requests_total".to_owned(),
                    value: "7".to_owned(),
                },
                MetricRow {
                    name: "solana_slot".to_owned(),
                    value: "42".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn skips_histogram_noise_and_ambiguous_series() {
        let parsed = parse_metrics(
            "solana_rpc_latency_bucket{le=\"1\"} 0\nsolana_rpc_latency_bucket{le=\"2\"} 0\nsolana_rpc_latency_sum 0\nsolana_slot{worker=\"0\"} 12\nsolana_slot{worker=\"1\"} 13\nprocess_cpu_seconds_total 4\n",
        );
        assert_eq!(parsed.samples, 6);
        assert_eq!(parsed.rows.len(), 1);
        assert_eq!(parsed.rows[0].name, "process_cpu_seconds_total");
    }

    #[test]
    fn report_cli_fields_are_read_without_guessing() {
        let report = serde_json::json!({
            "controller_argv": ["validator-canary", "--expected-node-identity", "public", "--expected-genesis-hash", "genesis"]
        });
        assert_eq!(
            string_after(&report, "--expected-node-identity").as_deref(),
            Some("public")
        );
        assert_eq!(
            string_after(&report, "--expected-genesis-hash").as_deref(),
            Some("genesis")
        );
        assert_eq!(string_after(&report, "--missing"), None);
    }
}
