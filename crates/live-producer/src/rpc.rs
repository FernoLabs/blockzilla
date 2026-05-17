use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant as StdInstant},
};

use anyhow::{Context, Result, anyhow};
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, RETRY_AFTER},
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::{Instant as TokioInstant, sleep};

use crate::{
    epoch::{EpochSlot, OLD_FAITHFUL_SLOTS_PER_EPOCH, old_faithful_epoch_slot},
    layout::ProducerLayout,
};

const RPC_X_TOKEN_HEADER: &str = "x-token";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcBackfillConfig {
    pub rpc_url: String,
    pub archive_dir: PathBuf,
    pub start_slot: u64,
    pub end_slot: u64,
    pub commitment: String,
    pub timeout_secs: u64,
    pub slots_per_epoch: u64,
    pub skip_existing: bool,
    pub rate_limit: RpcRateLimitConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEpochSyncConfig {
    pub rpc_url: String,
    pub commitment: String,
    pub timeout_secs: u64,
    pub rate_limit: RpcRateLimitConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRateLimitConfig {
    /// Local client-side pacing. `0.0` disables local pacing.
    pub requests_per_second: f64,
    /// Honor server rate-limit responses such as HTTP 429 + Retry-After.
    pub follow_server_rate_limits: bool,
    /// Number of retries after rate-limit responses.
    pub max_retries: u32,
    /// Initial retry delay when the server does not provide Retry-After.
    pub base_retry_delay_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RpcRateLimitStats {
    pub paced_requests: u64,
    pub local_rate_limit_sleep_ms: u128,
    pub server_rate_limit_retries: u64,
    pub server_rate_limit_sleep_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEpochSyncReport {
    pub rpc_url: String,
    pub commitment: String,
    pub absolute_slot: u64,
    pub block_height: Option<u64>,
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub transaction_count: Option<u64>,
    pub epoch_start_slot: u64,
    pub epoch_end_slot: u64,
    pub next_boundary_slot: u64,
    pub slots_remaining_in_epoch: u64,
    pub old_faithful_epoch: u64,
    pub old_faithful_slot_index: u64,
    pub old_faithful_epoch_start_slot: u64,
    pub old_faithful_epoch_end_slot: u64,
    pub old_faithful_compatible: bool,
    pub rate_limit: RpcRateLimitConfig,
    pub rate_limit_stats: RpcRateLimitStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcBackfillReport {
    pub rpc_url: String,
    pub archive_dir: PathBuf,
    pub start_slot: u64,
    pub end_slot: u64,
    pub slots_requested: u64,
    pub blocks_written: u64,
    pub skipped_existing: u64,
    pub null_blocks: u64,
    pub rpc_errors: u64,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub elapsed_ms: u128,
    pub repair_dir: PathBuf,
    pub journal_path: PathBuf,
    pub rate_limit: RpcRateLimitConfig,
    pub rate_limit_stats: RpcRateLimitStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RpcBackfillJournalRecord {
    slot: u64,
    epoch: u64,
    status: RpcBackfillStatus,
    path: Option<PathBuf>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RpcBackfillStatus {
    Written,
    SkippedExisting,
    NullBlock,
    RpcError,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[allow(dead_code)]
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcEpochInfoResult {
    absolute_slot: u64,
    block_height: Option<u64>,
    epoch: u64,
    slot_index: u64,
    slots_in_epoch: u64,
    transaction_count: Option<u64>,
}

struct RpcJsonRpcClient {
    client: Client,
    rate_limit: RpcRateLimitConfig,
    stats: RpcRateLimitStats,
    next_request_at: Option<TokioInstant>,
}

impl Default for RpcBackfillConfig {
    fn default() -> Self {
        Self {
            rpc_url: String::new(),
            archive_dir: PathBuf::from("blockzilla-live"),
            start_slot: 0,
            end_slot: 0,
            commitment: "finalized".to_string(),
            timeout_secs: 30,
            slots_per_epoch: OLD_FAITHFUL_SLOTS_PER_EPOCH,
            skip_existing: true,
            rate_limit: RpcRateLimitConfig::default(),
        }
    }
}

impl Default for RpcEpochSyncConfig {
    fn default() -> Self {
        Self {
            rpc_url: String::new(),
            commitment: "finalized".to_string(),
            timeout_secs: 10,
            rate_limit: RpcRateLimitConfig::default(),
        }
    }
}

impl Default for RpcRateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 0.0,
            follow_server_rate_limits: true,
            max_retries: 8,
            base_retry_delay_ms: 1_000,
        }
    }
}

impl RpcJsonRpcClient {
    fn new(
        timeout_secs: u64,
        user_agent: &'static str,
        rate_limit: RpcRateLimitConfig,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs.max(1)))
            .user_agent(user_agent)
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            client,
            rate_limit,
            stats: RpcRateLimitStats::default(),
            next_request_at: None,
        })
    }

    async fn json_rpc<T: DeserializeOwned>(
        &mut self,
        rpc_url: &str,
        request: &Value,
        operation: &str,
    ) -> Result<Option<T>> {
        let mut attempt = 0u32;
        loop {
            self.wait_for_local_rate_limit().await;
            let response = self
                .client
                .post(rpc_url)
                .maybe_x_token_header()
                .json(request)
                .send()
                .await
                .with_context(|| format!("POST {operation}"))?;

            if response.status() == StatusCode::TOO_MANY_REQUESTS
                && self.can_retry_rate_limit(attempt)
            {
                let wait = retry_after_delay(response.headers())
                    .unwrap_or_else(|| self.rate_limit_backoff(attempt));
                self.wait_for_server_rate_limit(wait).await;
                attempt += 1;
                continue;
            }

            if !response.status().is_success() {
                let status = response.status();
                return Err(anyhow!("HTTP {} for {operation}", status.as_u16()));
            }

            let decoded = response
                .json::<JsonRpcResponse<T>>()
                .await
                .with_context(|| format!("decode JSON-RPC {operation}"))?;

            if let Some(error) = decoded.error {
                if is_json_rpc_rate_limit_error(&error) && self.can_retry_rate_limit(attempt) {
                    let wait = self.rate_limit_backoff(attempt);
                    self.wait_for_server_rate_limit(wait).await;
                    attempt += 1;
                    continue;
                }
                return Err(anyhow!("JSON-RPC {}: {}", error.code, error.message));
            }

            return Ok(decoded.result);
        }
    }

    async fn wait_for_local_rate_limit(&mut self) {
        let Some(interval) = self.local_rate_limit_interval() else {
            return;
        };
        let now = TokioInstant::now();
        let scheduled = self.next_request_at.unwrap_or(now);
        let send_at = scheduled.max(now);
        if send_at > now {
            let wait = send_at - now;
            sleep(wait).await;
            self.stats.local_rate_limit_sleep_ms += wait.as_millis();
        }
        self.next_request_at = Some(send_at + interval);
        self.stats.paced_requests = self.stats.paced_requests.saturating_add(1);
    }

    async fn wait_for_server_rate_limit(&mut self, wait: Duration) {
        self.stats.server_rate_limit_retries =
            self.stats.server_rate_limit_retries.saturating_add(1);
        self.stats.server_rate_limit_sleep_ms += wait.as_millis();
        sleep(wait).await;
    }

    fn local_rate_limit_interval(&self) -> Option<Duration> {
        (self.rate_limit.requests_per_second > 0.0)
            .then(|| Duration::from_secs_f64(1.0 / self.rate_limit.requests_per_second))
    }

    fn can_retry_rate_limit(&self, attempt: u32) -> bool {
        self.rate_limit.follow_server_rate_limits && attempt < self.rate_limit.max_retries
    }

    fn rate_limit_backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.min(5);
        let base_ms = self.rate_limit.base_retry_delay_ms.max(1) as u128;
        let delay_ms = base_ms.saturating_mul(1u128 << shift).min(30_000);
        Duration::from_millis(delay_ms as u64)
    }
}

pub async fn sync_epoch_info(config: RpcEpochSyncConfig) -> Result<RpcEpochSyncReport> {
    let rate_limit = config.rate_limit.clone();
    let mut rpc = RpcJsonRpcClient::new(
        config.timeout_secs,
        "blockzilla-live-producer/rpc-epoch-sync",
        rate_limit.clone(),
    )?;
    let epoch_info = fetch_epoch_info(&mut rpc, &config.rpc_url, &config.commitment).await?;
    let epoch_start_slot = epoch_info
        .absolute_slot
        .checked_sub(epoch_info.slot_index)
        .context("RPC epoch info slotIndex exceeds absoluteSlot")?;
    let epoch_end_slot = epoch_start_slot
        .checked_add(epoch_info.slots_in_epoch.saturating_sub(1))
        .context("RPC epoch end slot overflow")?;
    let next_boundary_slot = epoch_end_slot
        .checked_add(1)
        .context("RPC next boundary slot overflow")?;
    let slots_remaining_in_epoch = epoch_info
        .slots_in_epoch
        .saturating_sub(epoch_info.slot_index)
        .saturating_sub(1);
    let old_faithful = old_faithful_epoch_slot(epoch_info.absolute_slot);
    let old_faithful_compatible = epoch_info.epoch == old_faithful.epoch
        && epoch_info.slot_index == old_faithful.slot_index
        && epoch_info.slots_in_epoch == OLD_FAITHFUL_SLOTS_PER_EPOCH
        && epoch_start_slot == old_faithful.epoch_start_slot
        && epoch_end_slot == old_faithful.epoch_end_slot;

    Ok(RpcEpochSyncReport {
        rpc_url: config.rpc_url,
        commitment: config.commitment,
        absolute_slot: epoch_info.absolute_slot,
        block_height: epoch_info.block_height,
        epoch: epoch_info.epoch,
        slot_index: epoch_info.slot_index,
        slots_in_epoch: epoch_info.slots_in_epoch,
        transaction_count: epoch_info.transaction_count,
        epoch_start_slot,
        epoch_end_slot,
        next_boundary_slot,
        slots_remaining_in_epoch,
        old_faithful_epoch: old_faithful.epoch,
        old_faithful_slot_index: old_faithful.slot_index,
        old_faithful_epoch_start_slot: old_faithful.epoch_start_slot,
        old_faithful_epoch_end_slot: old_faithful.epoch_end_slot,
        old_faithful_compatible,
        rate_limit,
        rate_limit_stats: rpc.stats,
    })
}

pub async fn backfill_get_blocks(config: RpcBackfillConfig) -> Result<RpcBackfillReport> {
    anyhow::ensure!(
        config.start_slot <= config.end_slot,
        "start-slot must be <= end-slot"
    );
    let layout = ProducerLayout::create(&config.archive_dir)?;
    let repair_dir = layout.repair_dir.join("rpc-get-block");
    fs::create_dir_all(&repair_dir).with_context(|| format!("create {}", repair_dir.display()))?;
    let journal_path = repair_dir.join("backfill-get-block.jsonl");
    let journal_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .with_context(|| format!("open {}", journal_path.display()))?;
    let mut journal = BufWriter::new(journal_file);

    let rate_limit = config.rate_limit.clone();
    let mut rpc = RpcJsonRpcClient::new(
        config.timeout_secs,
        "blockzilla-live-producer/rpc-backfill",
        rate_limit.clone(),
    )?;

    let started_at = StdInstant::now();
    let slots_per_epoch = config.slots_per_epoch.max(1);
    let first_epoch = EpochSlot::from_slot(config.start_slot, slots_per_epoch).epoch;
    let last_epoch = EpochSlot::from_slot(config.end_slot, slots_per_epoch).epoch;
    let mut report = RpcBackfillReport {
        rpc_url: config.rpc_url.clone(),
        archive_dir: layout.archive_dir,
        start_slot: config.start_slot,
        end_slot: config.end_slot,
        slots_requested: config.end_slot - config.start_slot + 1,
        blocks_written: 0,
        skipped_existing: 0,
        null_blocks: 0,
        rpc_errors: 0,
        first_epoch,
        last_epoch,
        elapsed_ms: 0,
        repair_dir,
        journal_path,
        rate_limit,
        rate_limit_stats: RpcRateLimitStats::default(),
    };

    for slot in config.start_slot..=config.end_slot {
        let epoch_slot = EpochSlot::from_slot(slot, slots_per_epoch);
        let epoch_dir = report
            .repair_dir
            .join(format!("epoch-{}", epoch_slot.epoch));
        fs::create_dir_all(&epoch_dir)
            .with_context(|| format!("create {}", epoch_dir.display()))?;
        let path = epoch_dir.join(format!("slot-{slot}.getBlock.json"));

        if config.skip_existing && path.exists() {
            report.skipped_existing += 1;
            write_journal(
                &mut journal,
                RpcBackfillJournalRecord {
                    slot,
                    epoch: epoch_slot.epoch,
                    status: RpcBackfillStatus::SkippedExisting,
                    path: Some(path),
                    error: None,
                },
            )?;
            continue;
        }

        match fetch_get_block(&mut rpc, &config.rpc_url, slot, &config.commitment).await {
            Ok(Some(block)) => {
                let tmp_path = path.with_extension("json.tmp");
                let file = File::create(&tmp_path)
                    .with_context(|| format!("create {}", tmp_path.display()))?;
                let mut writer = BufWriter::new(file);
                serde_json::to_writer(&mut writer, &block)
                    .with_context(|| format!("write {}", tmp_path.display()))?;
                writer
                    .flush()
                    .with_context(|| format!("flush {}", tmp_path.display()))?;
                fs::rename(&tmp_path, &path).with_context(|| {
                    format!("rename {} to {}", tmp_path.display(), path.display())
                })?;
                report.blocks_written += 1;
                write_journal(
                    &mut journal,
                    RpcBackfillJournalRecord {
                        slot,
                        epoch: epoch_slot.epoch,
                        status: RpcBackfillStatus::Written,
                        path: Some(path),
                        error: None,
                    },
                )?;
            }
            Ok(None) => {
                report.null_blocks += 1;
                write_journal(
                    &mut journal,
                    RpcBackfillJournalRecord {
                        slot,
                        epoch: epoch_slot.epoch,
                        status: RpcBackfillStatus::NullBlock,
                        path: None,
                        error: None,
                    },
                )?;
            }
            Err(err) => {
                report.rpc_errors += 1;
                write_journal(
                    &mut journal,
                    RpcBackfillJournalRecord {
                        slot,
                        epoch: epoch_slot.epoch,
                        status: RpcBackfillStatus::RpcError,
                        path: None,
                        error: Some(err.to_string()),
                    },
                )?;
            }
        }
    }

    journal.flush()?;
    report.elapsed_ms = started_at.elapsed().as_millis();
    report.rate_limit_stats = rpc.stats;
    Ok(report)
}

async fn fetch_get_block(
    rpc: &mut RpcJsonRpcClient,
    rpc_url: &str,
    slot: u64,
    commitment: &str,
) -> Result<Option<Value>> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": slot,
        "method": "getBlock",
        "params": [
            slot,
            {
                "encoding": "base64",
                "transactionDetails": "full",
                "rewards": true,
                "commitment": commitment,
                "maxSupportedTransactionVersion": 0
            }
        ]
    });

    rpc.json_rpc(rpc_url, &request, &format!("getBlock slot {slot}"))
        .await
}

async fn fetch_epoch_info(
    rpc: &mut RpcJsonRpcClient,
    rpc_url: &str,
    commitment: &str,
) -> Result<RpcEpochInfoResult> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": "blockzilla-epoch-sync",
        "method": "getEpochInfo",
        "params": [
            {
                "commitment": commitment
            }
        ]
    });

    rpc.json_rpc(rpc_url, &request, "getEpochInfo")
        .await?
        .ok_or_else(|| anyhow!("getEpochInfo returned null result"))
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    value
        .parse::<f64>()
        .ok()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(Duration::from_secs_f64)
}

fn is_json_rpc_rate_limit_error(error: &JsonRpcError) -> bool {
    error.code == 429
        || error.message.to_ascii_lowercase().contains("rate limit")
        || error
            .message
            .to_ascii_lowercase()
            .contains("too many requests")
}

trait MaybeXTokenHeader {
    fn maybe_x_token_header(self) -> Self;
}

impl MaybeXTokenHeader for reqwest::RequestBuilder {
    fn maybe_x_token_header(self) -> Self {
        match rpc_x_token_from_env() {
            Some(token) => self.header(RPC_X_TOKEN_HEADER, token),
            None => self,
        }
    }
}

fn rpc_x_token_from_env() -> Option<String> {
    env::var("BLOCKZILLA_RPC_X_TOKEN")
        .or_else(|_| env::var("BLOCKZILLA_GRPC_X_TOKEN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn write_journal(writer: &mut impl Write, record: RpcBackfillJournalRecord) -> Result<()> {
    serde_json::to_writer(&mut *writer, &record)?;
    writer.write_all(b"\n")?;
    Ok(())
}
