//! Bounded, cacheable produced-slot coverage audits for Old Faithful epochs.
//!
//! One successful refresh performs exactly one `getBlocks` JSON-RPC call. The endpoint and
//! optional token are runtime-only inputs and are deliberately absent from every persisted type.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use reqwest::{Client, header::HeaderValue, redirect::Policy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const OLD_FAITHFUL_SLOTS_PER_EPOCH: u64 = 432_000;
pub const EPOCH_BITMAP_BYTES: usize = (OLD_FAITHFUL_SLOTS_PER_EPOCH as usize) / 8;
pub const DEFAULT_MAX_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const SNAPSHOT_SCHEMA_VERSION: u16 = 1;
const AUDIT_SCHEMA_VERSION: u16 = 1;
const ELIGIBILITY_SCHEMA_VERSION: u16 = 1;
const MAX_LABEL_BYTES: usize = 128;
const MAX_REPAIR_MANIFEST_BYTES: u64 = 32 * 1024 * 1024;
const MAX_REPAIR_PLAN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_REPAIR_PLAN_LINE_BYTES: usize = 64 * 1024;
const MAX_SUMMARY_RANGES: usize = 64;
const HOT_INDEX_MAGIC: &[u8; 8] = b"BZV2HIX1";
const HOT_INDEX_VERSION: u16 = 1;
const HOT_INDEX_HEADER_LEN: usize = 36;
const HOT_INDEX_ROW_LEN: usize = 52;
const HOT_INDEX_FLAG_DICTIONARY: u32 = 1 << 0;
const HOT_INDEX_FLAG_RAW_BLOCKS: u32 = 1 << 1;

#[derive(Debug, Clone)]
pub enum LocalEpochSource {
    ArchiveDir(PathBuf),
    RepairBundle(PathBuf),
}

#[derive(Clone)]
pub struct EpochSlotAuditConfig {
    pub epoch: u64,
    pub rpc_url: String,
    pub rpc_x_token: Option<String>,
    pub provider_label: String,
    pub cluster_label: String,
    pub provider_archival_guarantee: bool,
    pub eligibility_receipt: PathBuf,
    pub state_dir: PathBuf,
    pub local_source: Option<LocalEpochSource>,
    pub refresh_rpc_snapshot: bool,
    pub timeout: Duration,
    pub max_rpc_response_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizedEligibilityReceipt {
    pub schema_version: u16,
    pub cluster_label: String,
    pub finalized_through_slot: u64,
    pub observed_unix_secs: u64,
    pub source_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeSummary {
    pub count: u64,
    pub ranges: Vec<[u64; 2]>,
    pub ranges_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalCoverageSummary {
    pub kind: String,
    pub source_path: PathBuf,
    pub source_fingerprint_sha256: String,
    pub listed_slots: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochSlotAuditReport {
    pub schema_version: u16,
    pub state: String,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub commitment: String,
    pub provider_label: String,
    pub cluster_label: String,
    pub provider_archival_guarantee: bool,
    pub rpc_snapshot_reused: bool,
    pub rpc_snapshot_path: PathBuf,
    pub receipt_path: PathBuf,
    pub rpc_listed_slots: u64,
    pub rpc_unlisted: RangeSummary,
    pub local: Option<LocalCoverageSummary>,
    pub missing_locally: RangeSummary,
    pub extra_locally: RangeSummary,
    pub skipped_slot_interpretation: String,
    pub verification_scope: String,
    pub observed_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RpcSnapshot {
    schema_version: u16,
    epoch: u64,
    start_slot: u64,
    end_slot: u64,
    commitment: String,
    provider_label: String,
    cluster_label: String,
    provider_archival_guarantee: bool,
    eligibility_receipt_sha256: String,
    eligibility_finalized_through_slot: u64,
    response_bytes: u64,
    listed_slots: u64,
    unlisted_slots: u64,
    bitmap_encoding: String,
    bitmap_decoded_bytes: u64,
    bitmap_base64: String,
    bitmap_sha256: String,
    listed_slot_digest_sha256: String,
    observed_unix_secs: u64,
}

#[derive(Debug, Clone)]
struct EpochBitmap {
    bytes: Vec<u8>,
}

impl EpochBitmap {
    fn empty() -> Self {
        Self {
            bytes: vec![0; EPOCH_BITMAP_BYTES],
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        anyhow::ensure!(
            bytes.len() == EPOCH_BITMAP_BYTES,
            "epoch bitmap has {} decoded bytes, expected {EPOCH_BITMAP_BYTES}",
            bytes.len()
        );
        Ok(Self { bytes })
    }

    fn set_offset(&mut self, offset: u64) -> Result<()> {
        anyhow::ensure!(
            offset < OLD_FAITHFUL_SLOTS_PER_EPOCH,
            "slot offset {offset} is outside an Old Faithful epoch"
        );
        let byte = usize::try_from(offset / 8).context("bitmap offset exceeds usize")?;
        let bit = (offset % 8) as u8;
        let mask = 1u8 << bit;
        anyhow::ensure!(
            self.bytes[byte] & mask == 0,
            "duplicate slot offset {offset}"
        );
        self.bytes[byte] |= mask;
        Ok(())
    }

    fn get_offset(&self, offset: u64) -> bool {
        let byte = (offset / 8) as usize;
        let bit = (offset % 8) as u8;
        self.bytes[byte] & (1u8 << bit) != 0
    }

    fn count(&self) -> u64 {
        self.bytes
            .iter()
            .map(|value| value.count_ones() as u64)
            .sum()
    }

    fn sha256(&self) -> String {
        hex_digest(Sha256::digest(&self.bytes))
    }
}

#[derive(Debug)]
struct LoadedLocalCoverage {
    bitmap: EpochBitmap,
    summary: LocalCoverageSummary,
}

struct AuditLock {
    file: File,
}

impl Drop for AuditLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            let _ = libc::flock(std::os::fd::AsRawFd::as_raw_fd(&self.file), libc::LOCK_UN);
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonRpcGetBlocksResponse {
    jsonrpc: Option<String>,
    id: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_json_member")]
    result: JsonMember<Vec<u64>>,
    #[serde(default, deserialize_with = "deserialize_json_member")]
    error: JsonMember<JsonRpcError>,
}

#[derive(Debug, Default)]
enum JsonMember<T> {
    #[default]
    Absent,
    Null,
    Value(T),
}

fn deserialize_json_member<'de, D, T>(
    deserializer: D,
) -> std::result::Result<JsonMember<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(match Option::<T>::deserialize(deserializer)? {
        Some(value) => JsonMember::Value(value),
        None => JsonMember::Null,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

pub async fn run_epoch_slot_audit(config: EpochSlotAuditConfig) -> Result<EpochSlotAuditReport> {
    validate_config(&config)?;
    let (start_slot, end_slot) = epoch_bounds(config.epoch)?;
    let eligibility_bytes = read_bounded_file(&config.eligibility_receipt, 1024 * 1024)?;
    let eligibility: FinalizedEligibilityReceipt = serde_json::from_slice(&eligibility_bytes)
        .with_context(|| {
            format!(
                "decode eligibility receipt {}",
                config.eligibility_receipt.display()
            )
        })?;
    validate_eligibility(&config, &eligibility, end_slot)?;
    let eligibility_sha256 = hex_digest(Sha256::digest(&eligibility_bytes));

    let epoch_dir = config.state_dir.join(format!("epoch-{}", config.epoch));
    fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("create audit state directory {}", epoch_dir.display()))?;
    let _lock = acquire_lock(&epoch_dir.join("audit.lock"))?;
    let snapshot_path = epoch_dir.join("rpc-produced-slots.json");
    let receipt_path = epoch_dir.join("coverage-audit.json");

    let (snapshot, rpc_bitmap, rpc_snapshot_reused) = if snapshot_path
        .try_exists()
        .with_context(|| format!("check cached snapshot {}", snapshot_path.display()))?
        && !config.refresh_rpc_snapshot
    {
        let (snapshot, bitmap) = load_snapshot(&snapshot_path, &config, start_slot, end_slot)?;
        (snapshot, bitmap, true)
    } else {
        let (bitmap, response_bytes, slot_digest) =
            fetch_get_blocks_once(&config, start_slot, end_slot).await?;
        let snapshot = RpcSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            epoch: config.epoch,
            start_slot,
            end_slot,
            commitment: "finalized".to_string(),
            provider_label: config.provider_label.clone(),
            cluster_label: config.cluster_label.clone(),
            provider_archival_guarantee: config.provider_archival_guarantee,
            eligibility_receipt_sha256: eligibility_sha256.clone(),
            eligibility_finalized_through_slot: eligibility.finalized_through_slot,
            response_bytes: response_bytes as u64,
            listed_slots: bitmap.count(),
            unlisted_slots: OLD_FAITHFUL_SLOTS_PER_EPOCH - bitmap.count(),
            bitmap_encoding: "base64-lsb0-slot-offset".to_string(),
            bitmap_decoded_bytes: EPOCH_BITMAP_BYTES as u64,
            bitmap_base64: BASE64_STANDARD.encode(&bitmap.bytes),
            bitmap_sha256: bitmap.sha256(),
            listed_slot_digest_sha256: slot_digest,
            observed_unix_secs: unix_now()?,
        };
        write_json_atomic(&snapshot_path, &snapshot)?;
        (snapshot, bitmap, false)
    };

    // Validate local material only after the possibly slow network request. This keeps the
    // source-stability checks adjacent to receipt publication and ensures a successful RPC result
    // remains cached even if local validation fails and must be retried.
    let local = config
        .local_source
        .as_ref()
        .map(|source| load_local_coverage(source, config.epoch, start_slot, end_slot))
        .transpose()?;

    let rpc_unlisted = summarize_offsets(start_slot, |offset| !rpc_bitmap.get_offset(offset));
    let (missing_locally, extra_locally, state) = if let Some(local) = &local {
        let missing = summarize_offsets(start_slot, |offset| {
            rpc_bitmap.get_offset(offset) && !local.bitmap.get_offset(offset)
        });
        let extra = summarize_offsets(start_slot, |offset| {
            !rpc_bitmap.get_offset(offset) && local.bitmap.get_offset(offset)
        });
        let state = if missing.count == 0 && extra.count == 0 {
            if config.provider_archival_guarantee {
                "slot_coverage_verified"
            } else {
                "agrees_unproven"
            }
        } else {
            "slot_coverage_mismatch"
        };
        (missing, extra, state)
    } else {
        (
            RangeSummary {
                count: 0,
                ranges: Vec::new(),
                ranges_truncated: false,
            },
            RangeSummary {
                count: 0,
                ranges: Vec::new(),
                ranges_truncated: false,
            },
            "rpc_snapshot_only",
        )
    };

    let report = EpochSlotAuditReport {
        schema_version: AUDIT_SCHEMA_VERSION,
        state: state.to_string(),
        epoch: config.epoch,
        start_slot,
        end_slot,
        commitment: "finalized".to_string(),
        provider_label: config.provider_label.clone(),
        cluster_label: config.cluster_label.clone(),
        provider_archival_guarantee: config.provider_archival_guarantee,
        rpc_snapshot_reused,
        rpc_snapshot_path: snapshot_path.clone(),
        receipt_path: receipt_path.clone(),
        rpc_listed_slots: snapshot.listed_slots,
        rpc_unlisted,
        local: local.map(|coverage| coverage.summary),
        missing_locally,
        extra_locally,
        skipped_slot_interpretation: if config.provider_archival_guarantee {
            "provider-guaranteed archival omissions may be treated as skipped slots".to_string()
        } else {
            "RPC-unlisted slots are not proven skipped; provider history may be incomplete"
                .to_string()
        },
        verification_scope:
            "produced-slot membership only; this audit does not prove block payload integrity"
                .to_string(),
        observed_unix_secs: unix_now()?,
    };
    write_json_atomic(&receipt_path, &report)?;
    Ok(report)
}

fn validate_config(config: &EpochSlotAuditConfig) -> Result<()> {
    validate_label("provider", &config.provider_label)?;
    validate_label("cluster", &config.cluster_label)?;
    anyhow::ensure!(
        !config.rpc_url.trim().is_empty(),
        "RPC URL environment value is empty"
    );
    anyhow::ensure!(config.timeout > Duration::ZERO, "timeout must be positive");
    anyhow::ensure!(
        config.max_rpc_response_bytes > 0
            && config.max_rpc_response_bytes <= DEFAULT_MAX_RPC_RESPONSE_BYTES,
        "max RPC response bytes must be in 1..={DEFAULT_MAX_RPC_RESPONSE_BYTES}"
    );
    Ok(())
}

fn validate_label(kind: &str, label: &str) -> Result<()> {
    anyhow::ensure!(
        !label.is_empty()
            && label.len() <= MAX_LABEL_BYTES
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:/".contains(&byte)),
        "{kind} label must be 1..={MAX_LABEL_BYTES} safe ASCII characters"
    );
    Ok(())
}

fn validate_eligibility(
    config: &EpochSlotAuditConfig,
    eligibility: &FinalizedEligibilityReceipt,
    end_slot: u64,
) -> Result<()> {
    anyhow::ensure!(
        eligibility.schema_version == ELIGIBILITY_SCHEMA_VERSION,
        "unsupported eligibility receipt schema {}",
        eligibility.schema_version
    );
    anyhow::ensure!(
        eligibility.cluster_label == config.cluster_label,
        "eligibility receipt cluster does not match configured cluster"
    );
    validate_label("eligibility source", &eligibility.source_label)?;
    anyhow::ensure!(
        eligibility.finalized_through_slot >= end_slot,
        "epoch end {end_slot} is not closed by finalized slot {}",
        eligibility.finalized_through_slot
    );
    anyhow::ensure!(
        eligibility.observed_unix_secs > 0,
        "eligibility receipt has no timestamp"
    );
    Ok(())
}

fn epoch_bounds(epoch: u64) -> Result<(u64, u64)> {
    let start = epoch
        .checked_mul(OLD_FAITHFUL_SLOTS_PER_EPOCH)
        .context("epoch start slot overflow")?;
    let end = start
        .checked_add(OLD_FAITHFUL_SLOTS_PER_EPOCH - 1)
        .context("epoch end slot overflow")?;
    Ok((start, end))
}

async fn fetch_get_blocks_once(
    config: &EpochSlotAuditConfig,
    start_slot: u64,
    end_slot: u64,
) -> Result<(EpochBitmap, usize, String)> {
    let client = Client::builder()
        .timeout(config.timeout)
        .redirect(Policy::none())
        // Reqwest otherwise retries protocol-level NACKs. This worker promises one wire request
        // per explicit snapshot refresh, so every automatic retry must remain disabled.
        .retry(reqwest::retry::never())
        .user_agent("hivezilla-epoch-slot-audit/1")
        .build()
        .context("build bounded RPC client")?;
    let request_id = config.epoch;
    let mut request = client.post(&config.rpc_url).json(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "getBlocks",
        "params": [start_slot, end_slot, {"commitment": "finalized"}]
    }));
    if let Some(token) = config.rpc_x_token.as_deref() {
        let mut value =
            HeaderValue::from_str(token).context("RPC x-token is not a valid header")?;
        value.set_sensitive(true);
        request = request.header("x-token", value);
    }
    let response = request
        .send()
        .await
        .map_err(|error| anyhow!("getBlocks RPC request failed: {}", error.without_url()))?;
    anyhow::ensure!(
        response.status().is_success(),
        "getBlocks RPC returned HTTP status {}",
        response.status().as_u16()
    );
    if let Some(length) = response.content_length() {
        anyhow::ensure!(
            length <= config.max_rpc_response_bytes as u64,
            "getBlocks response Content-Length {length} exceeds configured limit"
        );
    }
    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or(0)
            .min(config.max_rpc_response_bytes as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| anyhow!("read getBlocks response body: {}", error.without_url()))?;
        anyhow::ensure!(
            body.len().saturating_add(chunk.len()) <= config.max_rpc_response_bytes,
            "getBlocks response body exceeds configured limit"
        );
        body.extend_from_slice(&chunk);
    }
    let (bitmap, digest) = parse_get_blocks_body(&body, request_id, start_slot, end_slot)?;
    Ok((bitmap, body.len(), digest))
}

fn parse_get_blocks_body(
    body: &[u8],
    request_id: u64,
    start_slot: u64,
    end_slot: u64,
) -> Result<(EpochBitmap, String)> {
    let parsed: JsonRpcGetBlocksResponse =
        serde_json::from_slice(body).context("decode typed getBlocks response")?;
    anyhow::ensure!(
        parsed.jsonrpc.as_deref() == Some("2.0"),
        "invalid JSON-RPC version"
    );
    anyhow::ensure!(
        parsed.id == Some(request_id),
        "getBlocks JSON-RPC id mismatch"
    );
    let slots = match (parsed.result, parsed.error) {
        (JsonMember::Value(slots), JsonMember::Absent) => slots,
        (JsonMember::Absent, JsonMember::Value(error)) => {
            let _ = (&error.message, &error.data);
            bail!("getBlocks JSON-RPC error code {}", error.code);
        }
        _ => bail!("getBlocks response must contain exactly one non-null result or error member"),
    };
    anyhow::ensure!(
        slots.len() <= OLD_FAITHFUL_SLOTS_PER_EPOCH as usize,
        "getBlocks returned too many slots"
    );
    let mut bitmap = EpochBitmap::empty();
    let mut digest = Sha256::new();
    let mut previous = None;
    for slot in slots {
        anyhow::ensure!(
            (start_slot..=end_slot).contains(&slot),
            "getBlocks returned out-of-range slot {slot}"
        );
        if let Some(previous) = previous {
            anyhow::ensure!(slot > previous, "getBlocks slots are duplicate or unsorted");
        }
        bitmap.set_offset(slot - start_slot)?;
        digest.update(slot.to_le_bytes());
        previous = Some(slot);
    }
    Ok((bitmap, hex_digest(digest.finalize())))
}

fn load_snapshot(
    path: &Path,
    config: &EpochSlotAuditConfig,
    start_slot: u64,
    end_slot: u64,
) -> Result<(RpcSnapshot, EpochBitmap)> {
    let bytes = read_bounded_file(path, 1024 * 1024)?;
    let snapshot: RpcSnapshot = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode cached RPC snapshot {}", path.display()))?;
    anyhow::ensure!(
        snapshot.schema_version == SNAPSHOT_SCHEMA_VERSION,
        "bad snapshot schema"
    );
    anyhow::ensure!(
        snapshot.epoch == config.epoch
            && snapshot.start_slot == start_slot
            && snapshot.end_slot == end_slot,
        "cached RPC snapshot epoch/range mismatch"
    );
    anyhow::ensure!(
        snapshot.commitment == "finalized",
        "cached snapshot is not finalized"
    );
    anyhow::ensure!(
        snapshot.provider_label == config.provider_label
            && snapshot.cluster_label == config.cluster_label
            && snapshot.provider_archival_guarantee == config.provider_archival_guarantee,
        "cached RPC snapshot provider policy mismatch"
    );
    anyhow::ensure!(
        snapshot.eligibility_finalized_through_slot >= end_slot,
        "cached RPC snapshot was not created from a closed epoch"
    );
    anyhow::ensure!(
        snapshot.bitmap_encoding == "base64-lsb0-slot-offset"
            && snapshot.bitmap_decoded_bytes == EPOCH_BITMAP_BYTES as u64,
        "cached RPC snapshot bitmap metadata is invalid"
    );
    let bitmap = EpochBitmap::from_bytes(
        BASE64_STANDARD
            .decode(&snapshot.bitmap_base64)
            .context("decode cached RPC bitmap")?,
    )?;
    anyhow::ensure!(
        bitmap.sha256() == snapshot.bitmap_sha256,
        "cached bitmap digest mismatch"
    );
    anyhow::ensure!(
        bitmap.count() == snapshot.listed_slots
            && snapshot.unlisted_slots == OLD_FAITHFUL_SLOTS_PER_EPOCH - snapshot.listed_slots,
        "cached bitmap counts do not match snapshot"
    );
    let digest = listed_slot_digest(&bitmap, start_slot);
    anyhow::ensure!(
        digest == snapshot.listed_slot_digest_sha256,
        "cached listed-slot digest mismatch"
    );
    Ok((snapshot, bitmap))
}

fn listed_slot_digest(bitmap: &EpochBitmap, start_slot: u64) -> String {
    let mut digest = Sha256::new();
    for offset in 0..OLD_FAITHFUL_SLOTS_PER_EPOCH {
        if bitmap.get_offset(offset) {
            digest.update((start_slot + offset).to_le_bytes());
        }
    }
    hex_digest(digest.finalize())
}

fn summarize_offsets(start_slot: u64, mut predicate: impl FnMut(u64) -> bool) -> RangeSummary {
    let mut count = 0u64;
    let mut ranges = Vec::new();
    let mut run_start = None;
    for offset in 0..OLD_FAITHFUL_SLOTS_PER_EPOCH {
        if predicate(offset) {
            count += 1;
            run_start.get_or_insert(offset);
        } else if let Some(start) = run_start.take() {
            if ranges.len() < MAX_SUMMARY_RANGES {
                ranges.push([start_slot + start, start_slot + offset - 1]);
            }
        }
    }
    if let Some(start) = run_start {
        if ranges.len() < MAX_SUMMARY_RANGES {
            ranges.push([
                start_slot + start,
                start_slot + OLD_FAITHFUL_SLOTS_PER_EPOCH - 1,
            ]);
        }
    }
    let represented: u64 = ranges.iter().map(|range| range[1] - range[0] + 1).sum();
    RangeSummary {
        count,
        ranges,
        ranges_truncated: represented < count,
    }
}

fn load_local_coverage(
    source: &LocalEpochSource,
    epoch: u64,
    start_slot: u64,
    end_slot: u64,
) -> Result<LoadedLocalCoverage> {
    match source {
        LocalEpochSource::ArchiveDir(path) => {
            load_hot_archive_coverage(path, epoch, start_slot, end_slot)
        }
        LocalEpochSource::RepairBundle(path) => {
            load_repair_bundle_coverage(path, epoch, start_slot, end_slot)
        }
    }
}

fn load_hot_archive_coverage(
    archive_dir: &Path,
    _epoch: u64,
    start_slot: u64,
    end_slot: u64,
) -> Result<LoadedLocalCoverage> {
    let index_path = archive_dir.join("archive-v2-blocks.index");
    let blob_path = archive_dir.join("archive-v2-blocks.zstd");
    let index_meta_before = regular_file_metadata(&index_path)?;
    anyhow::ensure!(
        index_meta_before.len() >= HOT_INDEX_HEADER_LEN as u64,
        "Archive V2 hot index is shorter than its header"
    );
    let file = File::open(&index_path)
        .with_context(|| format!("open Archive V2 index {}", index_path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Sha256::new();
    let mut header = [0u8; HOT_INDEX_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read Archive V2 index header {}", index_path.display()))?;
    hasher.update(header);
    anyhow::ensure!(
        &header[..8] == HOT_INDEX_MAGIC,
        "unsupported archive block index magic"
    );
    anyhow::ensure!(
        le_u16(&header[8..10]) == HOT_INDEX_VERSION,
        "unsupported hot index version"
    );
    anyhow::ensure!(
        le_u16(&header[10..12]) == 0,
        "hot index reserved field is non-zero"
    );
    let row_count = le_u64(&header[12..20]);
    let blob_file_bytes = le_u64(&header[20..28]);
    let flags = le_u32(&header[32..36]);
    anyhow::ensure!(
        row_count <= OLD_FAITHFUL_SLOTS_PER_EPOCH,
        "hot index row count {row_count} exceeds one epoch"
    );
    anyhow::ensure!(
        flags & !(HOT_INDEX_FLAG_DICTIONARY | HOT_INDEX_FLAG_RAW_BLOCKS) == 0,
        "hot index contains unknown flags 0x{flags:08x}"
    );
    anyhow::ensure!(
        flags & HOT_INDEX_FLAG_RAW_BLOCKS == 0,
        "raw-stream hot indexes are not accepted by this coverage worker"
    );
    let expected_index_bytes = (HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            row_count
                .checked_mul(HOT_INDEX_ROW_LEN as u64)
                .context("hot index size overflow")?,
        )
        .context("hot index size overflow")?;
    anyhow::ensure!(
        index_meta_before.len() == expected_index_bytes,
        "hot index byte length does not match its row count"
    );
    let blob_meta_before = regular_file_metadata(&blob_path)?;
    anyhow::ensure!(
        blob_meta_before.len() == blob_file_bytes,
        "hot index blob byte count does not match archive-v2-blocks.zstd"
    );

    let mut bitmap = EpochBitmap::empty();
    let mut row = [0u8; HOT_INDEX_ROW_LEN];
    let mut expected_blob_offset = 0u64;
    let mut expected_tx_ordinal = 0u64;
    let mut expected_signature_ordinal = 0u64;
    let mut previous_slot = None;
    for ordinal in 0..row_count {
        reader
            .read_exact(&mut row)
            .with_context(|| format!("read row {ordinal} from {}", index_path.display()))?;
        hasher.update(row);
        let block_id = le_u32(&row[0..4]);
        let slot = le_u64(&row[4..12]);
        let compressed_offset = le_u64(&row[12..20]);
        let compressed_len = le_u32(&row[20..24]);
        let uncompressed_len = le_u32(&row[24..28]);
        let tx_count = le_u32(&row[28..32]);
        let first_tx_ordinal = le_u64(&row[32..40]);
        let first_signature_ordinal = le_u64(&row[40..48]);
        let signature_count = le_u32(&row[48..52]);
        anyhow::ensure!(
            block_id as u64 == ordinal,
            "hot index block IDs are not sequential"
        );
        anyhow::ensure!(
            (start_slot..=end_slot).contains(&slot),
            "hot index slot {slot} is outside the requested epoch"
        );
        if let Some(previous) = previous_slot {
            anyhow::ensure!(slot > previous, "hot index slots are duplicate or unsorted");
        }
        anyhow::ensure!(
            compressed_offset == expected_blob_offset,
            "hot index compressed frames are not contiguous at block {block_id}"
        );
        anyhow::ensure!(
            compressed_len > 0 && uncompressed_len > 0,
            "hot index has an empty frame"
        );
        expected_blob_offset = compressed_offset
            .checked_add(compressed_len as u64)
            .context("hot index compressed offset overflow")?;
        anyhow::ensure!(
            expected_blob_offset <= blob_file_bytes,
            "hot index frame exceeds its backing blob"
        );
        anyhow::ensure!(
            first_tx_ordinal == expected_tx_ordinal,
            "hot index transaction ordinals are discontinuous"
        );
        anyhow::ensure!(
            first_signature_ordinal == expected_signature_ordinal,
            "hot index signature ordinals are discontinuous"
        );
        expected_tx_ordinal = expected_tx_ordinal
            .checked_add(tx_count as u64)
            .context("transaction ordinal overflow")?;
        expected_signature_ordinal = expected_signature_ordinal
            .checked_add(signature_count as u64)
            .context("signature ordinal overflow")?;
        bitmap.set_offset(slot - start_slot)?;
        previous_slot = Some(slot);
    }
    anyhow::ensure!(
        expected_blob_offset == blob_file_bytes,
        "hot index rows do not cover the complete backing blob"
    );
    anyhow::ensure!(
        same_file_metadata(&index_meta_before, &fs::metadata(&index_path)?),
        "hot index changed while it was read"
    );
    anyhow::ensure!(
        same_file_metadata(&blob_meta_before, &fs::metadata(&blob_path)?),
        "hot block blob changed while its index was read"
    );
    let index_sha = hasher.finalize();
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"hix1\0");
    fingerprint.update(index_sha);
    update_metadata_digest(&mut fingerprint, b"index", &index_meta_before)?;
    update_metadata_digest(&mut fingerprint, b"blob", &blob_meta_before)?;
    Ok(LoadedLocalCoverage {
        summary: LocalCoverageSummary {
            kind: "archive_v2_hix1_structural_index".to_string(),
            source_path: archive_dir.to_path_buf(),
            source_fingerprint_sha256: hex_digest(fingerprint.finalize()),
            listed_slots: bitmap.count(),
        },
        bitmap,
    })
}

#[derive(Debug, Deserialize)]
struct RepairManifestSubset {
    version: u16,
    state: String,
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
    blockhash_records: u64,
    duplicate_live_blocks: u64,
    first_produced_slot: Option<u64>,
    last_produced_slot: Option<u64>,
    poh: RepairPohSubset,
    normalized_frames: RepairNormalizedFramesSubset,
    rpc_only_slots: Vec<RepairRpcSlotSubset>,
    publication_ready: bool,
}

#[derive(Debug, Deserialize)]
struct RepairPohSubset {
    path: String,
    records: u64,
    rpc_only_records_omitted: u64,
    produced_id_space: u64,
    record_ids_have_explicit_rpc_gaps: bool,
    missing_record_ids: Vec<u32>,
}

#[derive(Debug, Deserialize)]
struct RepairNormalizedFramesSubset {
    current_live_finalizer_compatible: bool,
}

#[derive(Debug, Deserialize)]
struct RepairRpcSlotSubset {
    slot: u64,
    parent_slot: u64,
    source_path: String,
    source_bytes: u64,
    source_sha256: String,
    source_modified_nanos: u128,
    source_device: Option<u64>,
    source_inode: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RepairPlanSourceSubset {
    source_id: u32,
    block_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RepairPlanRecord {
    Header {
        version: u16,
        epoch: u64,
        expected_live_blocks: u64,
        expected_rpc_blocks: u64,
        expected_produced_blocks: u64,
        block_id_space: String,
        live_rows_have_explicit_rpc_gaps: bool,
        sources: Vec<RepairPlanSourceSubset>,
    },
    Block {
        block_id: u32,
        slot: u64,
        parent_slot: u64,
        source_id: u32,
        source_block_id: u32,
        source_offset: u64,
        block_len: u32,
    },
}

#[derive(Debug)]
struct RepairPlanBlock {
    block_id: u32,
    slot: u64,
    parent_slot: u64,
    source_id: u32,
    source_block_id: u32,
    source_offset: u64,
    block_len: u32,
}

fn load_repair_bundle_coverage(
    root: &Path,
    epoch: u64,
    start_slot: u64,
    end_slot: u64,
) -> Result<LoadedLocalCoverage> {
    let marker_path = root.join("REPAIR-REQUIRED.json");
    let manifest_path = root.join("repair/epoch-repair-manifest.json");
    let plan_path = root.join("repair/live-merge-plan.jsonl");
    anyhow::ensure!(
        !root.join("READY").exists(),
        "repair bundle unexpectedly contains READY"
    );
    let marker = read_bounded_file(&marker_path, MAX_REPAIR_MANIFEST_BYTES)?;
    anyhow::ensure!(
        file_equals_bytes(&manifest_path, &marker, MAX_REPAIR_MANIFEST_BYTES)?,
        "repair root and internal manifests differ"
    );
    let manifest: RepairManifestSubset =
        serde_json::from_slice(&marker).context("decode repair coverage manifest")?;
    anyhow::ensure!(manifest.version == 1, "unsupported repair manifest version");
    anyhow::ensure!(
        manifest.state == "rpc_fallback_missing_poh_and_shredding",
        "repair manifest has an unsupported state"
    );
    anyhow::ensure!(
        manifest.epoch == epoch
            && manifest.epoch_start_slot == start_slot
            && manifest.epoch_end_slot == end_slot,
        "repair manifest epoch/range mismatch"
    );
    anyhow::ensure!(
        !manifest.publication_ready,
        "RPC repair bundle must not claim READY"
    );
    anyhow::ensure!(
        !manifest.normalized_frames.current_live_finalizer_compatible,
        "repair bundle unexpectedly claims current-finalizer compatibility"
    );
    anyhow::ensure!(
        manifest.duplicate_live_blocks == 0,
        "repair plan has duplicate live blocks"
    );
    anyhow::ensure!(
        manifest.live_blocks.checked_add(manifest.rpc_only_blocks)
            == Some(manifest.produced_blocks)
            && manifest.blockhash_records == manifest.produced_blocks,
        "repair manifest produced-block accounting mismatch"
    );
    anyhow::ensure!(
        manifest.produced_blocks <= OLD_FAITHFUL_SLOTS_PER_EPOCH,
        "repair manifest contains too many blocks"
    );
    anyhow::ensure!(
        manifest.rpc_only_slots.len() as u64 == manifest.rpc_only_blocks
            && manifest.poh.missing_record_ids.len() as u64 == manifest.rpc_only_blocks,
        "repair RPC/Poh gap counts do not match"
    );
    anyhow::ensure!(
        manifest.poh.records == manifest.live_blocks
            && manifest.poh.rpc_only_records_omitted == manifest.rpc_only_blocks
            && manifest.poh.produced_id_space == manifest.produced_blocks
            && manifest.poh.record_ids_have_explicit_rpc_gaps,
        "repair PoH coverage accounting mismatch"
    );
    anyhow::ensure!(
        manifest.poh.path == "repair/available-poh.wincode",
        "repair PoH path does not match the repair schema"
    );
    let poh_path = confined_regular_file(root, Path::new(&manifest.poh.path))?;
    let poh_meta = regular_file_metadata(&poh_path)?;
    anyhow::ensure!(poh_meta.len() > 0, "repair PoH sidecar is empty");
    let blockhash_path = confined_regular_file(root, Path::new("repair/produced-blockhashes.bin"))?;
    let blockhash_meta = regular_file_metadata(&blockhash_path)?;
    let expected_blockhash_bytes = manifest
        .produced_blocks
        .checked_mul(32)
        .context("repair produced-blockhash length overflow")?;
    anyhow::ensure!(
        blockhash_meta.len() == expected_blockhash_bytes,
        "repair produced-blockhash file length mismatch"
    );
    let mut material_hasher = Sha256::new();
    update_metadata_digest(&mut material_hasher, b"poh", &poh_meta)?;
    update_metadata_digest(&mut material_hasher, b"blockhash", &blockhash_meta)?;
    let mut tracked_material = vec![(poh_path, poh_meta), (blockhash_path, blockhash_meta)];
    for pair in manifest.rpc_only_slots.windows(2) {
        anyhow::ensure!(
            pair[0].slot < pair[1].slot,
            "repair RPC slots are duplicate or unsorted"
        );
    }
    for pair in manifest.poh.missing_record_ids.windows(2) {
        anyhow::ensure!(
            pair[0] < pair[1],
            "repair PoH gap IDs are duplicate or unsorted"
        );
    }
    for rpc in &manifest.rpc_only_slots {
        anyhow::ensure!(
            rpc.source_sha256.len() == 64
                && rpc
                    .source_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit()),
            "repair RPC source digest is malformed"
        );
        let expected_source_path = format!(
            "repair/rpc-get-block/epoch-{epoch}/slot-{}.getBlock.json",
            rpc.slot
        );
        anyhow::ensure!(
            rpc.source_path == expected_source_path,
            "repair RPC source path does not match its epoch/slot"
        );
        let relative = Path::new(&rpc.source_path);
        let path = confined_regular_file(root, relative)?;
        let metadata = regular_file_metadata(&path)?;
        anyhow::ensure!(
            metadata.len() == rpc.source_bytes,
            "repair RPC source length mismatch"
        );
        ensure_recorded_file_identity(
            &metadata,
            rpc.source_modified_nanos,
            rpc.source_device,
            rpc.source_inode,
            "repair RPC source",
        )?;
        material_hasher.update(relative.as_os_str().as_encoded_bytes());
        material_hasher.update(rpc.source_sha256.as_bytes());
        update_metadata_digest(&mut material_hasher, b"rpc", &metadata)?;
        tracked_material.push((path, metadata));
    }

    let plan_meta = regular_file_metadata(&plan_path)?;
    anyhow::ensure!(
        plan_meta.len() <= MAX_REPAIR_PLAN_BYTES,
        "repair merge plan exceeds the configured bound"
    );
    let mut reader = BufReader::with_capacity(1024 * 1024, File::open(&plan_path)?);
    let mut line = Vec::new();
    let mut plan_hasher = Sha256::new();
    anyhow::ensure!(
        read_bounded_line(&mut reader, &mut line)?,
        "repair merge plan is empty"
    );
    plan_hasher.update(&line);
    let plan_sources = match serde_json::from_slice::<RepairPlanRecord>(trim_line(&line))
        .context("decode repair merge-plan header")?
    {
        RepairPlanRecord::Header {
            version,
            epoch: header_epoch,
            expected_live_blocks,
            expected_rpc_blocks,
            expected_produced_blocks,
            block_id_space,
            live_rows_have_explicit_rpc_gaps,
            sources,
        } => {
            anyhow::ensure!(
                version == 1
                    && header_epoch == epoch
                    && expected_live_blocks == manifest.live_blocks
                    && expected_rpc_blocks == manifest.rpc_only_blocks
                    && expected_produced_blocks == manifest.produced_blocks
                    && block_id_space == "produced_ordinal"
                    && live_rows_have_explicit_rpc_gaps,
                "repair merge-plan header does not match its manifest"
            );
            sources
        }
        RepairPlanRecord::Block { .. } => bail!("repair merge plan begins with a block"),
    };
    anyhow::ensure!(
        !plan_sources.is_empty(),
        "repair merge plan has no normalized sources"
    );
    let mut source_sizes = Vec::with_capacity(plan_sources.len());
    let mut source_paths = HashSet::with_capacity(plan_sources.len());
    for (ordinal, source) in plan_sources.iter().enumerate() {
        anyhow::ensure!(
            source.source_id as usize == ordinal,
            "repair merge-plan source IDs are not dense"
        );
        anyhow::ensure!(
            source_paths.insert(source.block_path.clone()),
            "repair merge-plan source paths are not unique"
        );
        let relative = Path::new(&source.block_path);
        let path = confined_regular_file(root, relative)?;
        let metadata = regular_file_metadata(&path)?;
        anyhow::ensure!(metadata.len() > 0, "repair normalized source is empty");
        source_sizes.push(metadata.len());
        material_hasher.update(relative.as_os_str().as_encoded_bytes());
        update_metadata_digest(&mut material_hasher, b"source", &metadata)?;
        tracked_material.push((path, metadata));
    }

    let mut next_live = read_next_plan_block(&mut reader, &mut line, &mut plan_hasher)?;
    let mut rpc_index = 0usize;
    let mut live_count = 0u64;
    let mut bitmap = EpochBitmap::empty();
    let mut previous_slot = None;
    let mut source_tails = vec![None::<(u32, u64)>; source_sizes.len()];
    for ordinal in 0..manifest.produced_blocks {
        let rpc = manifest.rpc_only_slots.get(rpc_index);
        let take_live = next_live
            .as_ref()
            .is_some_and(|live| rpc.is_none_or(|rpc| live.slot < rpc.slot));
        let (slot, parent_slot) = if take_live {
            let live = next_live.take().expect("checked live row");
            anyhow::ensure!(
                live.block_id as u64 == ordinal,
                "repair live produced ID mismatch"
            );
            let source_bytes = *source_sizes
                .get(live.source_id as usize)
                .context("repair live row references an unknown source")?;
            anyhow::ensure!(
                live.block_len > 0,
                "repair live row has an empty block frame"
            );
            let source_frame_len = u64::from(live.block_len)
                .checked_add(u64::from(u32_varint_len(live.block_len)))
                .context("repair live frame length overflow")?;
            let source_end = live
                .source_offset
                .checked_add(source_frame_len)
                .context("repair live source range overflow")?;
            anyhow::ensure!(
                source_end <= source_bytes,
                "repair live row exceeds retained normalized source"
            );
            let source_tail = source_tails
                .get_mut(live.source_id as usize)
                .expect("source size lookup already succeeded");
            if let Some((previous_source_block_id, previous_source_end)) = source_tail {
                anyhow::ensure!(
                    live.source_block_id > *previous_source_block_id,
                    "repair live source block IDs are duplicate or unsorted"
                );
                anyhow::ensure!(
                    live.source_offset >= *previous_source_end,
                    "repair live source frames overlap or move backwards"
                );
            }
            *source_tail = Some((live.source_block_id, source_end));
            live_count += 1;
            let values = (live.slot, live.parent_slot);
            next_live = read_next_plan_block(&mut reader, &mut line, &mut plan_hasher)?;
            values
        } else {
            let rpc = rpc.context("repair produced set ended before manifest count")?;
            anyhow::ensure!(
                next_live.as_ref().is_none_or(|live| live.slot != rpc.slot),
                "repair live/RPC slot overlap"
            );
            anyhow::ensure!(
                manifest.poh.missing_record_ids[rpc_index] as u64 == ordinal,
                "repair RPC produced ID does not match PoH gap ID"
            );
            rpc_index += 1;
            (rpc.slot, rpc.parent_slot)
        };
        anyhow::ensure!(
            (start_slot..=end_slot).contains(&slot),
            "repair produced slot is outside its epoch"
        );
        if let Some(previous) = previous_slot {
            anyhow::ensure!(
                slot > previous,
                "repair produced slots are duplicate or unsorted"
            );
            anyhow::ensure!(
                parent_slot == previous,
                "repair produced parent chain is discontinuous"
            );
        }
        bitmap.set_offset(slot - start_slot)?;
        previous_slot = Some(slot);
    }
    anyhow::ensure!(
        next_live.is_none()
            && rpc_index == manifest.rpc_only_slots.len()
            && live_count == manifest.live_blocks,
        "repair plan contains trailing or missing coverage rows"
    );
    anyhow::ensure!(
        bitmap.count() == manifest.produced_blocks
            && previous_slot == manifest.last_produced_slot
            && first_set_slot(&bitmap, start_slot) == manifest.first_produced_slot,
        "repair produced bounds/counts do not match manifest"
    );
    anyhow::ensure!(
        same_file_metadata(&plan_meta, &fs::metadata(&plan_path)?),
        "repair merge plan changed while it was read"
    );
    for (path, before) in &tracked_material {
        anyhow::ensure!(
            same_file_metadata(before, &regular_file_metadata(path)?),
            "repair material {} changed while coverage was validated",
            path.display()
        );
    }
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"repair-v1\0");
    fingerprint.update(Sha256::digest(&marker));
    fingerprint.update(plan_hasher.finalize());
    fingerprint.update(material_hasher.finalize());
    fingerprint.update(manifest.produced_blocks.to_le_bytes());
    Ok(LoadedLocalCoverage {
        summary: LocalCoverageSummary {
            kind: "epoch_repair_union_v1".to_string(),
            source_path: root.to_path_buf(),
            source_fingerprint_sha256: hex_digest(fingerprint.finalize()),
            listed_slots: bitmap.count(),
        },
        bitmap,
    })
}

fn read_next_plan_block(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    hasher: &mut Sha256,
) -> Result<Option<RepairPlanBlock>> {
    if !read_bounded_line(reader, line)? {
        return Ok(None);
    }
    hasher.update(&*line);
    match serde_json::from_slice::<RepairPlanRecord>(trim_line(line))
        .context("decode repair merge-plan row")?
    {
        RepairPlanRecord::Block {
            block_id,
            slot,
            parent_slot,
            source_id,
            source_block_id,
            source_offset,
            block_len,
        } => Ok(Some(RepairPlanBlock {
            block_id,
            slot,
            parent_slot,
            source_id,
            source_block_id,
            source_offset,
            block_len,
        })),
        RepairPlanRecord::Header { .. } => bail!("repair merge plan contains a second header"),
    }
}

fn read_bounded_line(reader: &mut impl BufRead, line: &mut Vec<u8>) -> Result<bool> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(!line.is_empty());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        anyhow::ensure!(
            line.len().saturating_add(take) <= MAX_REPAIR_PLAN_LINE_BYTES,
            "repair merge-plan line exceeds {MAX_REPAIR_PLAN_LINE_BYTES} bytes"
        );
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(true);
        }
    }
}

fn trim_line(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn first_set_slot(bitmap: &EpochBitmap, start_slot: u64) -> Option<u64> {
    (0..OLD_FAITHFUL_SLOTS_PER_EPOCH)
        .find(|offset| bitmap.get_offset(*offset))
        .map(|offset| start_slot + offset)
}

fn acquire_lock(path: &Path) -> Result<AuditLock> {
    #[cfg(unix)]
    use std::{os::fd::AsRawFd, os::unix::fs::OpenOptionsExt};

    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options
        .open(path)
        .with_context(|| format!("open audit lock {}", path.display()))?;
    anyhow::ensure!(
        file.metadata()?.file_type().is_file(),
        "audit lock is not a regular file"
    );
    #[cfg(unix)]
    {
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            bail!("epoch audit lock is held: {error}");
        }
    }
    file.set_len(0)?;
    writeln!(
        file,
        "pid={} acquired_unix_secs={}",
        std::process::id(),
        unix_now()?
    )?;
    file.sync_all()?;
    Ok(AuditLock { file })
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    let parent = path.parent().context("atomic JSON path has no parent")?;
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("atomic JSON path has no UTF-8 file name")?;
    let tmp = parent.join(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        unix_now_nanos()?
    ));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&tmp)
            .with_context(|| format!("create atomic audit receipt {}", tmp.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(&tmp, path).with_context(|| {
            format!(
                "publish audit receipt {} -> {}",
                tmp.display(),
                path.display()
            )
        })?;
        sync_dir(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn read_bounded_file(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let metadata = regular_file_metadata(path)?;
    anyhow::ensure!(
        metadata.len() <= limit,
        "{} is {} bytes, exceeding limit {limit}",
        path.display(),
        metadata.len()
    );
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = file.take(limit + 1);
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    reader.read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= limit,
        "{} grew beyond its limit",
        path.display()
    );
    anyhow::ensure!(
        same_file_metadata(&metadata, &fs::metadata(path)?),
        "{} changed while it was read",
        path.display()
    );
    Ok(bytes)
}

fn file_equals_bytes(path: &Path, expected: &[u8], limit: u64) -> Result<bool> {
    let before = regular_file_metadata(path)?;
    anyhow::ensure!(
        before.len() <= limit,
        "{} exceeds bounded comparison limit",
        path.display()
    );
    if before.len() != expected.len() as u64 {
        return Ok(false);
    }
    let mut reader = BufReader::with_capacity(1024 * 1024, File::open(path)?);
    let mut offset = 0usize;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if expected.get(offset..offset + read) != Some(&buffer[..read]) {
            return Ok(false);
        }
        offset += read;
    }
    anyhow::ensure!(
        same_file_metadata(&before, &fs::metadata(path)?),
        "{} changed during bounded comparison",
        path.display()
    );
    Ok(offset == expected.len())
}

fn confined_regular_file(root: &Path, relative: &Path) -> Result<PathBuf> {
    anyhow::ensure!(
        !relative.as_os_str().is_empty()
            && !relative.is_absolute()
            && relative
                .components()
                .all(|component| { matches!(component, std::path::Component::Normal(_)) }),
        "repair manifest contains an unsafe relative path"
    );
    let canonical_root = fs::canonicalize(root)
        .with_context(|| format!("canonicalize repair root {}", root.display()))?;
    let joined = root.join(relative);
    let canonical = fs::canonicalize(&joined)
        .with_context(|| format!("canonicalize repair artifact {}", joined.display()))?;
    anyhow::ensure!(
        canonical.starts_with(&canonical_root),
        "repair artifact escapes its bundle root"
    );
    regular_file_metadata(&canonical)?;
    Ok(canonical)
}

fn regular_file_metadata(path: &Path) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat required regular file {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "{} is not a regular file",
        path.display()
    );
    Ok(metadata)
}

fn same_file_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    if left.len() != right.len() || modified_nanos(left).ok() != modified_nanos(right).ok() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        left.dev() == right.dev() && left.ino() == right.ino()
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn modified_nanos(metadata: &fs::Metadata) -> Result<u128> {
    Ok(metadata
        .modified()
        .context("read file modification time")?
        .duration_since(UNIX_EPOCH)
        .context("file modification time predates Unix epoch")?
        .as_nanos())
}

fn ensure_recorded_file_identity(
    metadata: &fs::Metadata,
    expected_modified_nanos: u128,
    expected_device: Option<u64>,
    expected_inode: Option<u64>,
    label: &str,
) -> Result<()> {
    anyhow::ensure!(
        modified_nanos(metadata)? == expected_modified_nanos,
        "{label} modification time no longer matches its manifest"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(
            expected_device == Some(metadata.dev()) && expected_inode == Some(metadata.ino()),
            "{label} filesystem identity no longer matches its manifest"
        );
    }
    #[cfg(not(unix))]
    {
        let _ = (expected_device, expected_inode);
    }
    Ok(())
}

fn update_metadata_digest(
    digest: &mut Sha256,
    label: &[u8],
    metadata: &fs::Metadata,
) -> Result<()> {
    digest.update(label);
    digest.update([0]);
    digest.update(metadata.len().to_le_bytes());
    digest.update(modified_nanos(metadata)?.to_le_bytes());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        digest.update(metadata.dev().to_le_bytes());
        digest.update(metadata.ino().to_le_bytes());
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn le_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("exact u16 slice"))
}

fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("exact u32 slice"))
}

fn le_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("exact u64 slice"))
}

fn u32_varint_len(mut value: u32) -> u8 {
    let mut len = 1u8;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("write to String");
    }
    output
}

fn unix_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_secs())
}

fn unix_now_nanos() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_nanos())
}
