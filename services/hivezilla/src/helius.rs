use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use futures::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, Url, header::RETRY_AFTER};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const CURSOR_SCHEMA_VERSION: u32 = 1;
const BLOCK_SCHEMA_VERSION: u32 = 1;
const WEBSOCKET_CREDIT_BYTES: u64 = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeliusBlockRecordConfig {
    pub api_key_file: PathBuf,
    pub output_dir: PathBuf,
    pub from_slot: Option<u64>,
    pub max_blocks: usize,
    pub timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub rpc_timeout_secs: u64,
    pub batch_slots: u64,
    pub max_rpc_retries: u32,
    pub compression_level: i32,
    pub slots_per_epoch: u64,
    pub max_response_bytes: usize,
}

impl Default for HeliusBlockRecordConfig {
    fn default() -> Self {
        Self {
            api_key_file: PathBuf::new(),
            output_dir: PathBuf::from("blockzilla-helius-raw"),
            from_slot: None,
            max_blocks: 1,
            timeout_secs: 300,
            idle_timeout_secs: 45,
            rpc_timeout_secs: 30,
            batch_slots: 64,
            max_rpc_retries: 8,
            compression_level: 1,
            slots_per_epoch: 432_000,
            max_response_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeliusRecordOutcome {
    MaxBlocks,
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeliusBlockRecordReport {
    pub provider: &'static str,
    pub mode: &'static str,
    pub output_dir: PathBuf,
    pub outcome: HeliusRecordOutcome,
    pub start_slot: Option<u64>,
    pub next_slot: Option<u64>,
    pub last_finalized_root: Option<u64>,
    pub blocks_written: u64,
    pub blocks_already_present: u64,
    pub skipped_finalized_slots: u64,
    pub websocket_connections: u64,
    pub websocket_reconnects: u64,
    pub websocket_messages: u64,
    pub websocket_bytes: u64,
    pub rpc_requests: u64,
    pub rpc_response_bytes: u64,
    pub rpc_rate_limit_responses: u64,
    pub rpc_transport_retries: u64,
    pub rpc_server_error_retries: u64,
    pub uncompressed_block_bytes: u64,
    pub stored_block_bytes: u64,
    pub estimated_rpc_credits: u64,
    pub estimated_websocket_connection_credits: u64,
    pub estimated_websocket_stream_credits: u64,
    pub estimated_total_credits: u64,
    pub direct_block_subscribe_supported: bool,
    pub poh_entries_available: bool,
    pub elapsed_ms: u128,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeliusCursor {
    schema_version: u32,
    next_slot: u64,
    last_finalized_root: u64,
    updated_unix_secs: u64,
}

#[derive(Debug, Serialize)]
struct StoredRpcBlock<'a> {
    schema_version: u32,
    slot: u64,
    source: &'static str,
    commitment: &'static str,
    poh_entries_available: bool,
    block: &'a Value,
}

#[derive(Debug, Serialize)]
struct CoverageRecord {
    schema_version: u32,
    start_slot: u64,
    end_slot: u64,
    finalized_root: u64,
    produced_slots: Vec<u64>,
    skipped_slots: Vec<u64>,
    recorded_unix_secs: u64,
}

#[derive(Debug, Default)]
struct RuntimeStats {
    blocks_written: u64,
    blocks_already_present: u64,
    skipped_finalized_slots: u64,
    websocket_connections: u64,
    websocket_messages: u64,
    websocket_bytes: u64,
    rpc_requests: u64,
    rpc_response_bytes: u64,
    rpc_rate_limit_responses: u64,
    rpc_transport_retries: u64,
    rpc_server_error_retries: u64,
    uncompressed_block_bytes: u64,
    stored_block_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct JsonRpcEnvelope<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

struct OutputLock(File);

impl Drop for OutputLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

struct HeliusRpcClient {
    client: Client,
    url: Url,
    max_response_bytes: usize,
    max_retries: u32,
}

impl HeliusRpcClient {
    fn new(api_key: &str, config: &HeliusBlockRecordConfig) -> Result<Self> {
        let url = helius_url("https", api_key)?;
        let client = Client::builder()
            .timeout(Duration::from_secs(config.rpc_timeout_secs.max(1)))
            .user_agent("hivezilla/helius-root-get-block")
            .build()
            .context("build Helius RPC client")?;
        Ok(Self {
            client,
            url,
            max_response_bytes: config.max_response_bytes.max(1),
            max_retries: config.max_rpc_retries,
        })
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
        stats: &mut RuntimeStats,
    ) -> Result<Option<T>> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": stats.rpc_requests.saturating_add(1),
            "method": method,
            "params": params,
        });
        let mut attempt = 0u32;
        loop {
            stats.rpc_requests = stats.rpc_requests.saturating_add(1);
            let response = match self
                .client
                .post(self.url.clone())
                .json(&request)
                .send()
                .await
            {
                Ok(response) => response,
                Err(_) if attempt < self.max_retries => {
                    stats.rpc_transport_retries = stats.rpc_transport_retries.saturating_add(1);
                    sleep(retry_delay(attempt)).await;
                    attempt += 1;
                    continue;
                }
                Err(_) => return Err(anyhow!("Helius RPC {method} transport failed")),
            };

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                stats.rpc_rate_limit_responses = stats.rpc_rate_limit_responses.saturating_add(1);
            } else if response.status().is_server_error() && attempt < self.max_retries {
                stats.rpc_server_error_retries = stats.rpc_server_error_retries.saturating_add(1);
            }
            if (response.status() == StatusCode::TOO_MANY_REQUESTS
                || response.status().is_server_error())
                && attempt < self.max_retries
            {
                let delay = retry_after(response.headers()).unwrap_or_else(|| retry_delay(attempt));
                sleep(delay).await;
                attempt += 1;
                continue;
            }
            if !response.status().is_success() {
                return Err(anyhow!(
                    "Helius RPC {method} returned HTTP {}",
                    response.status().as_u16()
                ));
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.max_response_bytes as u64)
            {
                return Err(anyhow!(
                    "Helius RPC {method} response exceeds configured limit"
                ));
            }
            let bytes = response
                .bytes()
                .await
                .map_err(|_| anyhow!("read Helius RPC {method} response failed"))?;
            if bytes.len() > self.max_response_bytes {
                return Err(anyhow!(
                    "Helius RPC {method} response exceeds configured limit"
                ));
            }
            stats.rpc_response_bytes = stats
                .rpc_response_bytes
                .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            let decoded: JsonRpcEnvelope<T> = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode Helius RPC {method} response"))?;
            if let Some(error) = decoded.error {
                let rate_limited =
                    error.code == 429 || error.message.to_ascii_lowercase().contains("rate limit");
                if rate_limited {
                    stats.rpc_rate_limit_responses =
                        stats.rpc_rate_limit_responses.saturating_add(1);
                    if attempt < self.max_retries {
                        sleep(retry_delay(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                }
                return Err(anyhow!(
                    "Helius RPC {method} error {}: {}",
                    error.code,
                    error.message
                ));
            }
            return Ok(decoded.result);
        }
    }

    async fn finalized_slot(&self, stats: &mut RuntimeStats) -> Result<u64> {
        self.call("getSlot", json!([{"commitment": "finalized"}]), stats)
            .await?
            .context("Helius getSlot returned null")
    }

    async fn produced_slots(
        &self,
        start: u64,
        end: u64,
        stats: &mut RuntimeStats,
    ) -> Result<Vec<u64>> {
        self.call(
            "getBlocks",
            json!([start, end, {"commitment": "finalized"}]),
            stats,
        )
        .await?
        .context("Helius getBlocks returned null")
    }

    async fn block(&self, slot: u64, stats: &mut RuntimeStats) -> Result<Option<Value>> {
        self.call(
            "getBlock",
            json!([
                slot,
                {
                    "encoding": "base64",
                    "transactionDetails": "full",
                    "rewards": true,
                    "commitment": "finalized",
                    "maxSupportedTransactionVersion": 0
                }
            ]),
            stats,
        )
        .await
    }
}

pub async fn record_helius_blocks(
    config: HeliusBlockRecordConfig,
) -> Result<HeliusBlockRecordReport> {
    validate_config(&config)?;
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create {}", config.output_dir.display()))?;
    let _lock = acquire_output_lock(&config.output_dir)?;
    let api_key = read_api_key(&config.api_key_file)?;
    let websocket_url = helius_url("wss", &api_key)?;
    let rpc = HeliusRpcClient::new(&api_key, &config)?;
    drop(api_key);

    let cursor_path = config.output_dir.join("cursor.json");
    let coverage_path = config.output_dir.join("coverage.jsonl");
    let mut cursor = read_cursor(&cursor_path)?;
    let mut start_slot = cursor
        .as_ref()
        .map(|value| value.next_slot)
        .or(config.from_slot);
    let started = Instant::now();
    let deadline = started + Duration::from_secs(config.timeout_secs.max(1));
    let mut stats = RuntimeStats::default();
    let mut last_error = None;
    let mut reconnect_delay = Duration::from_secs(1);
    let mut websocket_reconnects = 0u64;
    let outcome;

    loop {
        if Instant::now() >= deadline {
            outcome = HeliusRecordOutcome::TimedOut;
            break;
        }
        let session = run_websocket_session(
            &config,
            &websocket_url,
            &rpc,
            &cursor_path,
            &coverage_path,
            &mut cursor,
            &mut start_slot,
            &mut stats,
            deadline,
        )
        .await;
        match session {
            Ok(SessionOutcome::MaxBlocks) => {
                last_error = None;
                outcome = HeliusRecordOutcome::MaxBlocks;
                break;
            }
            Ok(SessionOutcome::Reconnect(reason)) => last_error = Some(reason),
            Err(error) => last_error = Some(error.to_string()),
        }
        websocket_reconnects = websocket_reconnects.saturating_add(1);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            outcome = HeliusRecordOutcome::TimedOut;
            break;
        }
        sleep(reconnect_delay.min(remaining)).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
    }

    let websocket_stream_credits = websocket_stream_credits(stats.websocket_bytes);
    let estimated_total_credits = stats
        .rpc_requests
        .saturating_add(stats.websocket_connections)
        .saturating_add(websocket_stream_credits);
    Ok(HeliusBlockRecordReport {
        provider: "helius",
        mode: "rootSubscribe+getBlocks+getBlock",
        output_dir: config.output_dir,
        outcome,
        start_slot,
        next_slot: cursor.as_ref().map(|value| value.next_slot),
        last_finalized_root: cursor.as_ref().map(|value| value.last_finalized_root),
        blocks_written: stats.blocks_written,
        blocks_already_present: stats.blocks_already_present,
        skipped_finalized_slots: stats.skipped_finalized_slots,
        websocket_connections: stats.websocket_connections,
        websocket_reconnects,
        websocket_messages: stats.websocket_messages,
        websocket_bytes: stats.websocket_bytes,
        rpc_requests: stats.rpc_requests,
        rpc_response_bytes: stats.rpc_response_bytes,
        rpc_rate_limit_responses: stats.rpc_rate_limit_responses,
        rpc_transport_retries: stats.rpc_transport_retries,
        rpc_server_error_retries: stats.rpc_server_error_retries,
        uncompressed_block_bytes: stats.uncompressed_block_bytes,
        stored_block_bytes: stats.stored_block_bytes,
        estimated_rpc_credits: stats.rpc_requests,
        estimated_websocket_connection_credits: stats.websocket_connections,
        estimated_websocket_stream_credits: websocket_stream_credits,
        estimated_total_credits,
        direct_block_subscribe_supported: false,
        poh_entries_available: false,
        elapsed_ms: started.elapsed().as_millis(),
        last_error,
    })
}

enum SessionOutcome {
    MaxBlocks,
    Reconnect(String),
}

#[allow(clippy::too_many_arguments)]
async fn run_websocket_session(
    config: &HeliusBlockRecordConfig,
    websocket_url: &Url,
    rpc: &HeliusRpcClient,
    cursor_path: &Path,
    coverage_path: &Path,
    cursor: &mut Option<HeliusCursor>,
    start_slot: &mut Option<u64>,
    stats: &mut RuntimeStats,
    deadline: Instant,
) -> Result<SessionOutcome> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let (mut socket, _) = timeout(
        remaining.min(Duration::from_secs(30)),
        connect_async(websocket_url.as_str()),
    )
    .await
    .map_err(|_| anyhow!("Helius WebSocket connection timed out"))?
    .map_err(|_| anyhow!("Helius WebSocket connection failed"))?;
    stats.websocket_connections = stats.websocket_connections.saturating_add(1);
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"rootSubscribe"})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|_| anyhow!("send Helius rootSubscribe failed"))?;

    wait_for_subscription(config, &mut socket, stats).await?;
    let current_root = rpc.finalized_slot(stats).await?;
    if cursor.is_none() {
        *cursor = Some(HeliusCursor {
            schema_version: CURSOR_SCHEMA_VERSION,
            next_slot: config.from_slot.unwrap_or(current_root),
            last_finalized_root: current_root,
            updated_unix_secs: unix_secs(),
        });
    }
    start_slot.get_or_insert_with(|| cursor.as_ref().expect("cursor initialized").next_slot);
    if process_root(
        config,
        rpc,
        cursor_path,
        coverage_path,
        cursor.as_mut().expect("cursor initialized"),
        current_root,
        stats,
    )
    .await?
    {
        return Ok(SessionOutcome::MaxBlocks);
    }

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(SessionOutcome::Reconnect(
                "capture timeout reached".to_string(),
            ));
        }
        let wait = remaining.min(Duration::from_secs(config.idle_timeout_secs.max(1)));
        let message = match timeout(wait, socket.next()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(_))) => {
                return Ok(SessionOutcome::Reconnect(
                    "Helius WebSocket receive failed".to_string(),
                ));
            }
            Ok(None) => {
                return Ok(SessionOutcome::Reconnect(
                    "Helius WebSocket closed".to_string(),
                ));
            }
            Err(_) => {
                return Ok(SessionOutcome::Reconnect(
                    "Helius WebSocket idle timeout".to_string(),
                ));
            }
        };
        count_websocket_message(&message, stats);
        match message {
            Message::Text(text) => {
                if let Some(root) = root_notification(text.as_bytes())? {
                    if process_root(
                        config,
                        rpc,
                        cursor_path,
                        coverage_path,
                        cursor.as_mut().expect("cursor initialized"),
                        root,
                        stats,
                    )
                    .await?
                    {
                        return Ok(SessionOutcome::MaxBlocks);
                    }
                }
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| anyhow!("send Helius WebSocket pong failed"))?;
            }
            Message::Close(_) => {
                return Ok(SessionOutcome::Reconnect(
                    "Helius WebSocket closed".to_string(),
                ));
            }
            _ => {}
        }
    }
}

async fn wait_for_subscription<S>(
    config: &HeliusBlockRecordConfig,
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    stats: &mut RuntimeStats,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let message = timeout(
            Duration::from_secs(config.idle_timeout_secs.max(1)),
            socket.next(),
        )
        .await
        .map_err(|_| anyhow!("Helius rootSubscribe response timed out"))?
        .ok_or_else(|| anyhow!("Helius WebSocket closed before subscription"))?
        .map_err(|_| anyhow!("read Helius rootSubscribe response failed"))?;
        count_websocket_message(&message, stats);
        match message {
            Message::Text(text) => {
                let value: Value = serde_json::from_slice(text.as_bytes())
                    .context("decode Helius rootSubscribe response")?;
                if let Some(error) = value.get("error") {
                    return Err(anyhow!("Helius rootSubscribe rejected: {error}"));
                }
                if value.get("id").and_then(Value::as_u64) == Some(1)
                    && value.get("result").and_then(Value::as_u64).is_some()
                {
                    return Ok(());
                }
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| anyhow!("send Helius WebSocket pong failed"))?;
            }
            Message::Close(_) => {
                return Err(anyhow!("Helius WebSocket closed before subscription"));
            }
            _ => {}
        }
    }
}

async fn process_root(
    config: &HeliusBlockRecordConfig,
    rpc: &HeliusRpcClient,
    cursor_path: &Path,
    coverage_path: &Path,
    cursor: &mut HeliusCursor,
    finalized_root: u64,
    stats: &mut RuntimeStats,
) -> Result<bool> {
    cursor.last_finalized_root = cursor.last_finalized_root.max(finalized_root);
    while cursor.next_slot <= finalized_root {
        let start = cursor.next_slot;
        let end = start
            .saturating_add(config.batch_slots.max(1).saturating_sub(1))
            .min(finalized_root);
        let mut prefetched_block = None;
        let produced = if start == end {
            match rpc.block(start, stats).await? {
                Some(block) => {
                    prefetched_block = Some(block);
                    vec![start]
                }
                None => rpc.produced_slots(start, end, stats).await?,
            }
        } else {
            rpc.produced_slots(start, end, stats).await?
        };
        validate_produced_slots(start, end, &produced)?;
        let produced_set = produced.iter().copied().collect::<BTreeSet<_>>();
        let mut completed_produced = Vec::new();
        let mut skipped = Vec::new();
        let mut completed_end = None;

        for slot in start..=end {
            if produced_set.contains(&slot) {
                let block = match prefetched_block.take().filter(|_| slot == start) {
                    Some(block) => block,
                    None => rpc.block(slot, stats).await?.ok_or_else(|| {
                        anyhow!("Helius getBlock returned null for produced finalized slot {slot}")
                    })?,
                };
                let write = write_block(config, slot, &block)?;
                if write.created {
                    stats.blocks_written = stats.blocks_written.saturating_add(1);
                    stats.uncompressed_block_bytes = stats
                        .uncompressed_block_bytes
                        .saturating_add(write.uncompressed_bytes);
                    stats.stored_block_bytes =
                        stats.stored_block_bytes.saturating_add(write.stored_bytes);
                } else {
                    stats.blocks_already_present = stats.blocks_already_present.saturating_add(1);
                }
                completed_produced.push(slot);
            } else {
                stats.skipped_finalized_slots = stats.skipped_finalized_slots.saturating_add(1);
                skipped.push(slot);
            }
            completed_end = Some(slot);
            if config.max_blocks > 0 && stats.blocks_written >= config.max_blocks as u64 {
                break;
            }
        }

        let completed_end = completed_end.context("finalized slot batch was empty")?;
        append_coverage(
            coverage_path,
            &CoverageRecord {
                schema_version: CURSOR_SCHEMA_VERSION,
                start_slot: start,
                end_slot: completed_end,
                finalized_root,
                produced_slots: completed_produced,
                skipped_slots: skipped,
                recorded_unix_secs: unix_secs(),
            },
        )?;
        cursor.next_slot = completed_end
            .checked_add(1)
            .context("Helius cursor slot overflow")?;
        cursor.last_finalized_root = cursor.last_finalized_root.max(finalized_root);
        cursor.updated_unix_secs = unix_secs();
        write_json_atomic(cursor_path, cursor)?;
        if config.max_blocks > 0 && stats.blocks_written >= config.max_blocks as u64 {
            return Ok(true);
        }
    }
    Ok(false)
}

struct BlockWriteOutcome {
    created: bool,
    uncompressed_bytes: u64,
    stored_bytes: u64,
}

fn write_block(
    config: &HeliusBlockRecordConfig,
    slot: u64,
    block: &Value,
) -> Result<BlockWriteOutcome> {
    let epoch = slot / config.slots_per_epoch.max(1);
    let shard = slot / 1_000;
    let dir = config
        .output_dir
        .join("blocks")
        .join(format!("epoch-{epoch}"))
        .join(format!("shard-{shard}"));
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(format!("slot-{slot}.getBlock.json.zst"));
    if path.exists() {
        let stored_bytes = fs::metadata(&path)
            .with_context(|| format!("inspect {}", path.display()))?
            .len();
        return Ok(BlockWriteOutcome {
            created: false,
            uncompressed_bytes: 0,
            stored_bytes,
        });
    }

    let value = StoredRpcBlock {
        schema_version: BLOCK_SCHEMA_VERSION,
        slot,
        source: "helius_getBlock",
        commitment: "finalized",
        poh_entries_available: false,
        block,
    };
    let encoded = serde_json::to_vec(&value).context("encode Helius block")?;
    let compressed = zstd::stream::encode_all(encoded.as_slice(), config.compression_level)
        .context("compress Helius block")?;
    let temp = path.with_extension(format!("zst.tmp.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .with_context(|| format!("create {}", temp.display()))?;
    file.write_all(&compressed)
        .with_context(|| format!("write {}", temp.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", temp.display()))?;
    fs::rename(&temp, &path).with_context(|| format!("publish {}", path.display()))?;
    sync_directory(&dir)?;
    Ok(BlockWriteOutcome {
        created: true,
        uncompressed_bytes: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        stored_bytes: u64::try_from(compressed.len()).unwrap_or(u64::MAX),
    })
}

fn append_coverage(path: &Path, record: &CoverageRecord) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, record)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_data()?;
    Ok(())
}

fn acquire_output_lock(output_dir: &Path) -> Result<OutputLock> {
    let path = output_dir.join("recorder.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(anyhow!(
            "another Helius recorder owns {}",
            output_dir.display()
        ));
    }
    Ok(OutputLock(file))
}

fn read_api_key(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect Helius API key file {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "Helius API key path must be a regular, non-symlink file"
    );
    let mut file =
        File::open(path).with_context(|| format!("open Helius API key file {}", path.display()))?;
    anyhow::ensure!(
        metadata.len() <= 16 * 1024,
        "Helius API key file is too large"
    );
    let mut value = String::new();
    file.read_to_string(&mut value)
        .context("read Helius API key file")?;
    let key = value.trim().to_string();
    anyhow::ensure!(!key.is_empty(), "Helius API key file is empty");
    anyhow::ensure!(
        !key.chars().any(char::is_whitespace),
        "Helius API key contains whitespace"
    );
    Ok(key)
}

fn helius_url(scheme: &str, api_key: &str) -> Result<Url> {
    let mut url = Url::parse(&format!("{scheme}://mainnet.helius-rpc.com/"))
        .context("construct Helius endpoint")?;
    url.query_pairs_mut().append_pair("api-key", api_key);
    Ok(url)
}

fn read_cursor(path: &Path) -> Result<Option<HeliusCursor>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    };
    anyhow::ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "Helius cursor is not a regular file"
    );
    anyhow::ensure!(metadata.len() <= 64 * 1024, "Helius cursor is too large");
    let cursor: HeliusCursor = serde_json::from_reader(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("decode {}", path.display()))?;
    anyhow::ensure!(
        cursor.schema_version == CURSOR_SCHEMA_VERSION,
        "unsupported Helius cursor schema {}",
        cursor.schema_version
    );
    Ok(Some(cursor))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("atomic JSON path has no parent")?;
    let temp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .with_context(|| format!("create {}", temp.display()))?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temp, path).with_context(|| format!("publish {}", path.display()))?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn validate_config(config: &HeliusBlockRecordConfig) -> Result<()> {
    anyhow::ensure!(
        !config.api_key_file.as_os_str().is_empty(),
        "api-key-file is required"
    );
    anyhow::ensure!(config.batch_slots > 0, "batch-slots must be positive");
    anyhow::ensure!(
        config.batch_slots <= 500_000,
        "batch-slots exceeds Solana getBlocks range limit"
    );
    anyhow::ensure!(
        config.slots_per_epoch > 0,
        "slots-per-epoch must be positive"
    );
    anyhow::ensure!(
        config.max_response_bytes >= 1024,
        "max-response-bytes is too small"
    );
    Ok(())
}

fn validate_produced_slots(start: u64, end: u64, slots: &[u64]) -> Result<()> {
    let mut previous = None;
    for &slot in slots {
        anyhow::ensure!(
            (start..=end).contains(&slot),
            "Helius getBlocks returned slot {slot} outside {start}-{end}"
        );
        if let Some(previous) = previous {
            anyhow::ensure!(slot > previous, "Helius getBlocks returned unsorted slots");
        }
        previous = Some(slot);
    }
    Ok(())
}

fn root_notification(bytes: &[u8]) -> Result<Option<u64>> {
    let value: Value = serde_json::from_slice(bytes).context("decode Helius WebSocket message")?;
    if value.get("method").and_then(Value::as_str) != Some("rootNotification") {
        return Ok(None);
    }
    value
        .pointer("/params/result")
        .and_then(Value::as_u64)
        .map(Some)
        .context("Helius rootNotification has no numeric result")
}

fn count_websocket_message(message: &Message, stats: &mut RuntimeStats) {
    stats.websocket_messages = stats.websocket_messages.saturating_add(1);
    let bytes = match message {
        Message::Text(value) => value.len(),
        Message::Binary(value) | Message::Ping(value) | Message::Pong(value) => value.len(),
        Message::Close(_) | Message::Frame(_) => 0,
    };
    stats.websocket_bytes = stats
        .websocket_bytes
        .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(Duration::from_secs_f64)
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(500u64.saturating_mul(1u64 << attempt.min(6)))
}

fn websocket_stream_credits(bytes: u64) -> u64 {
    if bytes == 0 {
        0
    } else {
        bytes.div_ceil(WEBSOCKET_CREDIT_BYTES).saturating_mul(2)
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_root_notifications() {
        let root = root_notification(
            br#"{"jsonrpc":"2.0","method":"rootNotification","params":{"result":42,"subscription":7}}"#,
        )
        .unwrap();
        assert_eq!(root, Some(42));
    }

    #[test]
    fn ignores_other_websocket_messages() {
        assert_eq!(
            root_notification(br#"{"jsonrpc":"2.0","result":7,"id":1}"#).unwrap(),
            None
        );
    }

    #[test]
    fn validates_sorted_bounded_produced_slots() {
        validate_produced_slots(10, 15, &[10, 12, 15]).unwrap();
        assert!(validate_produced_slots(10, 15, &[12, 11]).is_err());
        assert!(validate_produced_slots(10, 15, &[16]).is_err());
    }

    #[test]
    fn rounds_websocket_stream_cost_by_one_tenth_megabyte() {
        assert_eq!(websocket_stream_credits(0), 0);
        assert_eq!(websocket_stream_credits(1), 2);
        assert_eq!(websocket_stream_credits(100_000), 2);
        assert_eq!(websocket_stream_credits(100_001), 4);
    }
}
