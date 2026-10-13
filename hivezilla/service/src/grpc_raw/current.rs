//! Finite current-schema custody capture. This is separate from legacy format 2.
//!
//! A successful seal proves a stopped, checked byte capture. It does not prove canonical slot
//! coverage, independent consensus, complete footer delivery, or a publishable archive. Each
//! attempt uses a new directory. Failed attempts remain unsealed and are never resumed in place.

use anyhow::{Context, Result, bail, ensure};
use futures::{SinkExt, StreamExt, channel::mpsc};
use prost::Message;
use proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlockFooter,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
    SubscribeRequestPing, SubscribeUpdate, subscribe_update::UpdateOneof,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::time::{Instant as TokioInstant, timeout_at};
use yellowstone_grpc_client_current::{ClientTlsConfig, GeyserGrpcClient};
pub use yellowstone_grpc_proto_current as proto;

pub const SEAL_FILE: &str = "CURRENT-GRPC-SEALED.v1.json";
pub const INVENTORY_FILE: &str = "CURRENT-GRPC-INVENTORY.v1.json";
pub const EVENTS_FILE: &str = "events.wal";
pub const RPC_BEFORE_FILE: &str = "validator-rpc-before.json";
pub const RPC_AFTER_FILE: &str = "validator-rpc-after.json";
pub const PAYLOAD_FORMAT: u16 = 3;
pub const PROTOBUF_SCHEMA: &str = "yellowstone-grpc-proto@14.0.0";
const MAGIC: &[u8; 8] = b"HZGRPC03";
const COMMIT: &[u8; 4] = b"CMIT";
const FRAME_HEADER_BYTES: u64 = 88;
const MAX_JSON_BYTES: u64 = 1024 * 1024;
const WAL_SYNC_BYTES: u64 = 1024 * 1024;
const WAL_SYNC_INTERVAL: Duration = Duration::from_millis(100);
const GEYSER_PROTO_SHA256: &str =
    "56244cb007a872f28e294eedba0db4ae4bcbfb401035e4fbb8ec9c347d8ee9b3";
const STORAGE_PROTO_SHA256: &str =
    "e5d16ba9a9e83f6ed8c92a96b6b75dfd64c43f897b976011eb3ec5abe2f3c818";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CaptureCommitment {
    Confirmed,
    #[default]
    Finalized,
}
impl CaptureCommitment {
    fn proto(self) -> CommitmentLevel {
        match self {
            Self::Confirmed => CommitmentLevel::Confirmed,
            Self::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CurrentGrpcConfig {
    pub endpoint: String,
    pub validator_rpc_endpoint: Option<String>,
    pub output_dir: PathBuf,
    pub max_blocks: u64,
    pub timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub from_slot: Option<u64>,
    pub commitment: CaptureCommitment,
    pub compression_level: i32,
    pub max_record_bytes: u64,
    pub max_capture_bytes: u64,
    pub min_free_bytes: u64,
    pub cluster_id: String,
    pub origin_node_id: String,
    pub source_id: String,
    pub x_token_file: Option<PathBuf>,
}
impl Default for CurrentGrpcConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:10000".into(),
            validator_rpc_endpoint: Some("http://127.0.0.1:8899".into()),
            output_dir: "hivezilla-current-capture".into(),
            max_blocks: 128,
            timeout_secs: 300,
            idle_timeout_secs: 60,
            from_slot: None,
            commitment: CaptureCommitment::Finalized,
            compression_level: 1,
            max_record_bytes: 128 * 1024 * 1024,
            max_capture_bytes: 4 * 1024 * 1024 * 1024,
            min_free_bytes: 16 * 1024 * 1024 * 1024,
            cluster_id: "solana-mainnet".into(),
            origin_node_id: "local-validator".into(),
            source_id: "yellowstone-current".into(),
            x_token_file: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentCaptureCounts {
    pub events: u64,
    pub blocks: u64,
    pub transactions: u64,
    pub block_entries: u64,
    pub slots: u64,
    pub entries: u64,
    pub footers: u64,
    pub parent_updates: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentFileEvidence {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentValidatorIdentity {
    pub rpc_endpoint: String,
    pub genesis_hash: String,
    pub node_identity: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidatorRpcEvidence {
    schema: String,
    rpc_endpoint: String,
    genesis_response: serde_json::Value,
    identity_response: serde_json::Value,
    genesis_response_body: String,
    identity_response_body: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentGrpcSeal {
    pub schema: String,
    pub state: String,
    pub generation_id: String,
    pub source_profile: String,
    pub payload_format: u16,
    pub protobuf_schema: String,
    pub geyser_proto_sha256: String,
    pub storage_proto_sha256: String,
    pub requested_commitment: CaptureCommitment,
    pub source_endpoint: String,
    pub validator_identity: Option<CurrentValidatorIdentity>,
    pub cluster_id: String,
    pub origin_node_id: String,
    pub source_id: String,
    pub from_slot: Option<u64>,
    pub subscription_sha256: String,
    pub max_record_bytes: u64,
    pub counts: CurrentCaptureCounts,
    pub stop_reason: String,
    pub elapsed_seconds: f64,
    pub canonical_coverage_verified: bool,
    pub independent_consensus_verified: bool,
    pub footer_completeness_verified: bool,
    pub archive_publication_allowed: bool,
    pub files: Vec<CurrentFileEvidence>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentGrpcInventory {
    pub schema: String,
    pub source_profile: String,
    pub payload_format: u16,
    pub protobuf_schema: String,
    pub requested_commitment: CaptureCommitment,
    pub generation_id: String,
    pub source_receipt: CurrentFileEvidence,
    pub files: Vec<CurrentFileEvidence>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentEventRef {
    pub sequence: u64,
    pub offset: u64,
    pub frame_bytes: u64,
    pub raw_sha256: String,
    pub kind: String,
    pub slot: u64,
    pub bank_id: Option<u64>,
}

/// Entries must be requested to receive parent invalidation events. Retain both the standalone
/// entries and those nested in full blocks. Slot events are evidence, not an assertion that every
/// event reached the requested block commitment. Footer certificates remain opaque source bytes.
pub fn current_subscription(config: &CurrentGrpcConfig) -> SubscribeRequest {
    SubscribeRequest {
        blocks: HashMap::from([(
            "current-blocks".into(),
            SubscribeRequestFilterBlocks {
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        slots: HashMap::from([(
            "current-banks".into(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )]),
        entry: HashMap::from([(
            "current-entries-and-parents".into(),
            SubscribeRequestFilterEntry {
                include_update_parent: Some(true),
            },
        )]),
        block_footer: HashMap::from([(
            "current-footers".into(),
            SubscribeRequestFilterBlockFooter {
                include_certificates: Some(true),
            },
        )]),
        commitment: Some(config.commitment.proto() as i32),
        from_slot: config.from_slot,
        ..Default::default()
    }
}

fn event_key(update: &SubscribeUpdate) -> Result<(&'static str, u64, Option<u64>)> {
    Ok(
        match update
            .update_oneof
            .as_ref()
            .context("current update has no known event")?
        {
            UpdateOneof::Block(b) => ("block", b.slot, Some(b.bank_id)),
            UpdateOneof::Slot(s) => ("slot", s.slot, s.bank_id),
            UpdateOneof::Entry(e) => ("entry", e.slot, Some(e.bank_id)),
            UpdateOneof::BlockFooter(f) => ("block_footer", f.slot, Some(f.bank_id)),
            UpdateOneof::EntryUpdateParent(p) => {
                ("entry_update_parent", p.slot, Some(p.cleared_bank_id))
            }
            _ => bail!("unexpected event in current custody subscription"),
        },
    )
}
fn add_count(target: &mut u64, value: u64) -> Result<()> {
    *target = target
        .checked_add(value)
        .context("current capture count overflow")?;
    Ok(())
}
impl CurrentCaptureCounts {
    fn observe(&mut self, update: &SubscribeUpdate) -> Result<()> {
        let (_, slot, _) = event_key(update)?;
        add_count(&mut self.events, 1)?;
        self.first_slot = Some(self.first_slot.map_or(slot, |v| v.min(slot)));
        self.last_slot = Some(self.last_slot.map_or(slot, |v| v.max(slot)));
        match update.update_oneof.as_ref().unwrap() {
            UpdateOneof::Block(b) => {
                add_count(&mut self.blocks, 1)?;
                add_count(&mut self.transactions, b.transactions.len() as u64)?;
                add_count(&mut self.block_entries, b.entries.len() as u64)?;
            }
            UpdateOneof::Slot(_) => add_count(&mut self.slots, 1)?,
            UpdateOneof::Entry(_) => add_count(&mut self.entries, 1)?,
            UpdateOneof::BlockFooter(_) => add_count(&mut self.footers, 1)?,
            UpdateOneof::EntryUpdateParent(_) => add_count(&mut self.parent_updates, 1)?,
            _ => unreachable!(),
        }
        Ok(())
    }
}

fn validate_config(config: &CurrentGrpcConfig) -> Result<()> {
    ensure!(
        config.max_blocks > 0 && config.timeout_secs > 0 && config.idle_timeout_secs > 0,
        "current capture block and timeout limits must be positive"
    );
    ensure!(
        config.max_record_bytes > 0 && config.max_record_bytes <= 1024 * 1024 * 1024,
        "current capture record limit must be within 1..=1GiB"
    );
    ensure!(
        config.max_capture_bytes >= MAGIC.len() as u64 + FRAME_HEADER_BYTES + 4,
        "current capture byte limit is too small"
    );
    let url = reqwest::Url::parse(&config.endpoint).context("parse current capture endpoint")?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "capture endpoint must be HTTP(S) without embedded credentials or query"
    );
    for value in [
        &config.cluster_id,
        &config.origin_node_id,
        &config.source_id,
    ] {
        ensure!(
            !value.is_empty() && value.len() <= 256,
            "invalid current source identity label"
        );
    }
    if let Some(rpc) = &config.validator_rpc_endpoint {
        validate_loopback_endpoint(&config.endpoint)?;
        validate_loopback_endpoint(rpc)?;
    }
    Ok(())
}

fn validate_loopback_endpoint(endpoint: &str) -> Result<()> {
    let url = reqwest::Url::parse(endpoint)?;
    let authority = endpoint
        .split_once("://")
        .map(|(_, rest)| rest.split('/').next().unwrap_or(""))
        .unwrap_or("");
    let literal = authority == "127.0.0.1"
        || authority.starts_with("127.0.0.1:")
        || authority == "[::1]"
        || authority.starts_with("[::1]:");
    ensure!(
        literal
            && matches!(url.host_str(), Some("127.0.0.1" | "[::1]"))
            && matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "trusted validator capture requires literal loopback HTTP(S) endpoints"
    );
    Ok(())
}
fn source_profile(commitment: CaptureCommitment, bound: bool) -> &'static str {
    match (commitment, bound) {
        (CaptureCommitment::Finalized, true) => "validator-finalized-execution-v1",
        (CaptureCommitment::Confirmed, true) => "validator-confirmed-execution-v1",
        (CaptureCommitment::Finalized, false) => "unbound-finalized-observation-v1",
        (CaptureCommitment::Confirmed, false) => "unbound-confirmed-observation-v1",
    }
}
fn rpc_identity(evidence: &ValidatorRpcEvidence) -> Result<CurrentValidatorIdentity> {
    ensure!(
        evidence.schema == "hivezilla-validator-rpc-identity-v1",
        "unsupported RPC evidence schema"
    );
    validate_loopback_endpoint(&evidence.rpc_endpoint)?;
    ensure!(
        serde_json::from_str::<serde_json::Value>(&evidence.genesis_response_body)?
            == evidence.genesis_response
            && serde_json::from_str::<serde_json::Value>(&evidence.identity_response_body)?
                == evidence.identity_response,
        "validator RPC parsed response differs from retained response body"
    );
    for response in [&evidence.genesis_response, &evidence.identity_response] {
        ensure!(
            response.get("jsonrpc").and_then(|v| v.as_str()) == Some("2.0")
                && response.get("id").and_then(|v| v.as_u64()) == Some(1)
                && response.get("error").is_none(),
            "validator RPC returned an invalid response or error"
        );
    }
    let genesis_hash = evidence
        .genesis_response
        .get("result")
        .and_then(|v| v.as_str())
        .context("validator getGenesisHash result is missing")?
        .to_owned();
    let node_identity = evidence
        .identity_response
        .pointer("/result/identity")
        .and_then(|v| v.as_str())
        .context("validator getIdentity result is missing")?
        .to_owned();
    super::decode_blockhash(&genesis_hash).context("invalid genesis hash")?;
    super::decode_blockhash(&node_identity).context("invalid validator identity")?;
    Ok(CurrentValidatorIdentity {
        rpc_endpoint: evidence.rpc_endpoint.clone(),
        genesis_hash,
        node_identity,
    })
}
async fn fetch_rpc_evidence(endpoint: &str) -> Result<ValidatorRpcEvidence> {
    validate_loopback_endpoint(endpoint)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(15))
        .build()?;
    async fn call(
        client: &reqwest::Client,
        endpoint: &str,
        method: &str,
    ) -> Result<(serde_json::Value, String)> {
        let mut response = client
            .post(endpoint)
            .json(&serde_json::json!({
                "jsonrpc":"2.0", "id":1, "method":method
            }))
            .send()
            .await?
            .error_for_status()?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= 64 * 1024,
                "validator RPC response exceeds limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice(&bytes).context("decode validator RPC response")?;
        Ok((
            value,
            String::from_utf8(bytes).context("validator RPC body is not UTF-8")?,
        ))
    }
    let (genesis_response, genesis_response_body) =
        call(&client, endpoint, "getGenesisHash").await?;
    let (identity_response, identity_response_body) =
        call(&client, endpoint, "getIdentity").await?;
    let evidence = ValidatorRpcEvidence {
        schema: "hivezilla-validator-rpc-identity-v1".into(),
        rpc_endpoint: endpoint.into(),
        genesis_response,
        identity_response,
        genesis_response_body,
        identity_response_body,
    };
    rpc_identity(&evidence)?;
    Ok(evidence)
}

fn open_regular(path: &Path, write_new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    if write_new {
        options.write(true).create_new(true);
    }
    #[cfg(unix)]
    {
        options
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .mode(0o600);
    }
    let file = options
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "capture object is not a regular file"
    );
    Ok(file)
}
fn lock(file: &File, exclusive: bool) -> Result<()> {
    #[cfg(unix)]
    {
        let kind = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        };
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), kind | libc::LOCK_NB) } == 0,
            "capture file is still in use: {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (file, exclusive);
        bail!("current capture requires Unix file locks")
    }
}
fn digest(bytes: &[u8]) -> String {
    super::sha256_hex(bytes)
}
fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn hash_file(file: &mut File) -> Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(hex_digest(&hash.finalize()))
}
fn file_evidence(root: &Path, name: &str) -> Result<CurrentFileEvidence> {
    let mut file = open_regular(&root.join(name), false)?;
    Ok(CurrentFileEvidence {
        path: name.into(),
        bytes: file.metadata()?.len(),
        sha256: hash_file(&mut file)?,
    })
}
fn write_json_new(root: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let temp = root.join(format!(".{name}.tmp"));
    let mut file = open_regular(&temp, true)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    ensure!(
        !root.join(name).try_exists()?,
        "capture receipt already exists"
    );
    fs::rename(temp, root.join(name))?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let file = open_regular(path, false)?;
    ensure!(
        file.metadata()?.len() <= MAX_JSON_BYTES,
        "capture JSON exceeds limit"
    );
    Ok(serde_json::from_reader(file.take(MAX_JSON_BYTES + 1))?)
}

struct CaptureWriter {
    root: PathBuf,
    file: File,
    compressor: zstd::bulk::Compressor<'static>,
    counts: CurrentCaptureCounts,
    bytes: u64,
    max_record: u64,
    max_capture: u64,
    pending_sync_bytes: u64,
    last_sync: Instant,
    validator_identity: Option<CurrentValidatorIdentity>,
}
impl CaptureWriter {
    fn create(config: &CurrentGrpcConfig) -> Result<Self> {
        validate_config(config)?;
        fs::create_dir(&config.output_dir)
            .context("current capture requires a new output directory")?;
        #[cfg(unix)]
        fs::set_permissions(&config.output_dir, fs::Permissions::from_mode(0o700))?;
        let mut file = open_regular(&config.output_dir.join(EVENTS_FILE), true)?;
        lock(&file, true)?;
        file.write_all(MAGIC)?;
        file.sync_all()?;
        File::open(&config.output_dir)?.sync_all()?;
        Ok(Self {
            root: config.output_dir.clone(),
            file,
            compressor: zstd::bulk::Compressor::new(config.compression_level)?,
            counts: CurrentCaptureCounts::default(),
            bytes: MAGIC.len() as u64,
            max_record: config.max_record_bytes,
            max_capture: config.max_capture_bytes,
            pending_sync_bytes: 0,
            last_sync: Instant::now(),
            validator_identity: None,
        })
    }
    fn append(&mut self, update: &SubscribeUpdate) -> Result<()> {
        event_key(update)?;
        ensure!(
            update.encoded_len() as u64 <= self.max_record,
            "current protobuf exceeds record limit"
        );
        let raw = update.encode_to_vec();
        let compressed = self.compressor.compress(&raw)?;
        ensure!(
            compressed.len() as u64 <= self.max_record,
            "current compressed frame exceeds record limit"
        );
        let frame_bytes = FRAME_HEADER_BYTES
            .checked_add(compressed.len() as u64)
            .and_then(|n| n.checked_add(4))
            .context("capture frame size overflow")?;
        let after = self
            .bytes
            .checked_add(frame_bytes)
            .context("capture byte count overflow")?;
        ensure!(
            after <= self.max_capture,
            "current capture byte limit reached before requested blocks"
        );
        // Preserve the exact frame format, but avoid seven writes and a disk
        // barrier for every tiny entry. Only the stopped, fully synced seal
        // grants custody; unsealed attempts are never resumed or acknowledged.
        let mut frame = Vec::with_capacity(usize::try_from(frame_bytes)?);
        frame.extend_from_slice(&self.counts.events.to_le_bytes());
        frame.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        frame.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        frame.extend_from_slice(&Sha256::digest(&compressed));
        frame.extend_from_slice(&Sha256::digest(&raw));
        frame.extend_from_slice(&compressed);
        frame.extend_from_slice(COMMIT);
        self.file.write_all(&frame)?;
        self.pending_sync_bytes += frame_bytes;
        if matches!(update.update_oneof, Some(UpdateOneof::Block(_)))
            || self.pending_sync_bytes >= WAL_SYNC_BYTES
            || self.last_sync.elapsed() >= WAL_SYNC_INTERVAL
        {
            self.file.sync_data()?;
            self.pending_sync_bytes = 0;
            self.last_sync = Instant::now();
        }
        self.counts.observe(update)?;
        self.bytes = after;
        Ok(())
    }
    fn seal(mut self, config: &CurrentGrpcConfig, elapsed_seconds: f64) -> Result<CurrentGrpcSeal> {
        ensure!(
            self.counts.blocks == config.max_blocks,
            "cannot seal an incomplete finite capture"
        );
        ensure!(
            config.validator_rpc_endpoint.is_some() == self.validator_identity.is_some(),
            "capture validator identity was not checked"
        );
        self.file.sync_all()?;
        self.file.seek(SeekFrom::Start(0))?;
        let checked = read_frames(&mut self.file, self.max_record, |_, _| Ok(()))?;
        ensure!(checked == self.counts, "capture read-back counts differ");
        let sha = hash_file(&mut self.file)?;
        let evidence = CurrentFileEvidence {
            path: EVENTS_FILE.into(),
            bytes: self.bytes,
            sha256: sha,
        };
        let mut entropy = [0; 16];
        File::open("/dev/urandom")?.read_exact(&mut entropy)?;
        let profile = source_profile(config.commitment, self.validator_identity.is_some());
        let mut files = vec![evidence];
        if let Some(identity) = &self.validator_identity {
            for name in [RPC_BEFORE_FILE, RPC_AFTER_FILE] {
                let rpc: ValidatorRpcEvidence = read_json(&self.root.join(name))?;
                ensure!(
                    rpc_identity(&rpc)? == *identity,
                    "validator identity or genesis changed during capture"
                );
                files.push(file_evidence(&self.root, name)?);
            }
        }
        let seal = CurrentGrpcSeal {
            schema: "hivezilla-current-grpc-seal-v1".into(),
            state: "SEALED".into(),
            generation_id: hex_digest(&entropy),
            source_profile: profile.into(),
            payload_format: PAYLOAD_FORMAT,
            protobuf_schema: PROTOBUF_SCHEMA.into(),
            geyser_proto_sha256: GEYSER_PROTO_SHA256.into(),
            storage_proto_sha256: STORAGE_PROTO_SHA256.into(),
            requested_commitment: config.commitment,
            source_endpoint: config.endpoint.clone(),
            validator_identity: self.validator_identity.clone(),
            cluster_id: config.cluster_id.clone(),
            origin_node_id: config.origin_node_id.clone(),
            source_id: config.source_id.clone(),
            from_slot: config.from_slot,
            subscription_sha256: digest(&current_subscription(config).encode_to_vec()),
            max_record_bytes: self.max_record,
            counts: self.counts.clone(),
            stop_reason: "max_blocks".into(),
            elapsed_seconds,
            canonical_coverage_verified: false,
            independent_consensus_verified: false,
            footer_completeness_verified: false,
            archive_publication_allowed: false,
            files: files.clone(),
        };
        write_json_new(&self.root, SEAL_FILE, &seal)?;
        let inventory = CurrentGrpcInventory {
            schema: "hivezilla-live-execution-source-inventory-v1".into(),
            source_profile: profile.into(),
            payload_format: PAYLOAD_FORMAT,
            protobuf_schema: PROTOBUF_SCHEMA.into(),
            requested_commitment: config.commitment,
            generation_id: seal.generation_id.clone(),
            source_receipt: file_evidence(&self.root, SEAL_FILE)?,
            files,
        };
        write_json_new(&self.root, INVENTORY_FILE, &inventory)?;
        #[cfg(unix)]
        for name in seal
            .files
            .iter()
            .map(|f| f.path.as_str())
            .chain([SEAL_FILE, INVENTORY_FILE])
        {
            fs::set_permissions(self.root.join(name), fs::Permissions::from_mode(0o400))?;
        }
        File::open(&self.root)?.sync_all()?;
        Ok(seal)
    }
}

fn read_frames(
    file: &mut File,
    maximum: u64,
    mut callback: impl FnMut(CurrentEventRef, SubscribeUpdate) -> Result<()>,
) -> Result<CurrentCaptureCounts> {
    ensure!(
        maximum > 0 && maximum <= 1024 * 1024 * 1024,
        "invalid current reader record limit"
    );
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    ensure!(&magic == MAGIC, "not a current format-3 capture");
    let mut counts = CurrentCaptureCounts::default();
    loop {
        let offset = file.stream_position()?;
        let mut header = [0; FRAME_HEADER_BYTES as usize];
        let n = file.read(&mut header[..1])?;
        if n == 0 {
            break;
        }
        file.read_exact(&mut header[1..])
            .context("incomplete current frame header")?;
        let u64_at = |start| u64::from_le_bytes(header[start..start + 8].try_into().unwrap());
        let sequence = u64_at(0);
        let compressed_len = u64_at(8);
        let raw_len = u64_at(16);
        ensure!(
            sequence == counts.events,
            "current event sequence gap or duplicate"
        );
        ensure!(
            compressed_len > 0 && compressed_len <= maximum && raw_len > 0 && raw_len <= maximum,
            "current event length exceeds bounds"
        );
        let mut compressed = vec![0; usize::try_from(compressed_len)?];
        file.read_exact(&mut compressed)
            .context("incomplete current event payload")?;
        let mut commit = [0; 4];
        file.read_exact(&mut commit)?;
        ensure!(&commit == COMMIT, "missing current frame commit marker");
        ensure!(
            Sha256::digest(&compressed).as_slice() == &header[24..56],
            "current compressed digest mismatch"
        );
        let decoder = zstd::stream::read::Decoder::new(compressed.as_slice())?;
        let mut raw = Vec::with_capacity(usize::try_from(raw_len)?);
        decoder.take(raw_len + 1).read_to_end(&mut raw)?;
        ensure!(
            raw.len() as u64 == raw_len,
            "current event uncompressed length mismatch"
        );
        ensure!(
            Sha256::digest(&raw).as_slice() == &header[56..88],
            "current protobuf digest mismatch"
        );
        let update = SubscribeUpdate::decode(raw.as_slice()).context("decode current event")?;
        ensure!(
            update.encode_to_vec() == raw,
            "current event differs from pinned known-schema encoding"
        );
        let (kind, slot, bank_id) = event_key(&update)?;
        let reference = CurrentEventRef {
            sequence,
            offset,
            frame_bytes: FRAME_HEADER_BYTES + compressed_len + 4,
            raw_sha256: hex_digest(&header[56..88]),
            kind: kind.into(),
            slot,
            bank_id,
        };
        counts.observe(&update)?;
        callback(reference, update)?;
    }
    Ok(counts)
}

/// Hold the WAL read lock, validate the whole source digest before callbacks, and reject mutation
/// before returning. Callers must publish their own output only after this function succeeds.
pub fn read_sealed_current_events(
    root: &Path,
    max_record_bytes: u64,
    callback: impl FnMut(CurrentEventRef, SubscribeUpdate) -> Result<()>,
) -> Result<CurrentGrpcSeal> {
    ensure!(
        !fs::symlink_metadata(root)?.file_type().is_symlink(),
        "capture root cannot be a symlink"
    );
    let inventory: CurrentGrpcInventory = read_json(&root.join(INVENTORY_FILE))?;
    ensure!(
        inventory.schema == "hivezilla-live-execution-source-inventory-v1"
            && inventory.payload_format == PAYLOAD_FORMAT
            && inventory.protobuf_schema == PROTOBUF_SCHEMA,
        "unsupported current capture inventory"
    );
    ensure!(
        inventory.source_receipt.path == SEAL_FILE
            && matches!(inventory.files.len(), 1 | 3)
            && inventory.files[0].path == EVENTS_FILE,
        "unexpected current capture inventory paths"
    );
    ensure!(
        file_evidence(root, SEAL_FILE)? == inventory.source_receipt,
        "current seal digest mismatch"
    );
    let seal: CurrentGrpcSeal = read_json(&root.join(SEAL_FILE))?;
    ensure!(
        seal.schema == "hivezilla-current-grpc-seal-v1"
            && seal.state == "SEALED"
            && seal.payload_format == PAYLOAD_FORMAT
            && seal.protobuf_schema == PROTOBUF_SCHEMA
            && seal.geyser_proto_sha256 == GEYSER_PROTO_SHA256
            && seal.storage_proto_sha256 == STORAGE_PROTO_SHA256,
        "unsupported current capture seal"
    );
    ensure!(
        seal.files == inventory.files
            && seal.generation_id == inventory.generation_id
            && seal.requested_commitment == inventory.requested_commitment
            && seal.source_profile == inventory.source_profile,
        "current capture inventory and seal differ"
    );
    ensure!(
        !seal.archive_publication_allowed
            && !seal.canonical_coverage_verified
            && !seal.independent_consensus_verified
            && !seal.footer_completeness_verified,
        "raw capture seal cannot grant archive publication"
    );
    ensure!(
        seal.counts.blocks > 0
            && seal.stop_reason == "max_blocks"
            && seal.source_profile
                == source_profile(seal.requested_commitment, seal.validator_identity.is_some()),
        "invalid finite capture source profile"
    );
    ensure!(
        seal.generation_id.len() == 32
            && seal
                .generation_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid current generation identity"
    );
    let subscription = current_subscription(&CurrentGrpcConfig {
        commitment: seal.requested_commitment,
        from_slot: seal.from_slot,
        ..Default::default()
    });
    ensure!(
        seal.subscription_sha256 == digest(&subscription.encode_to_vec()),
        "current capture subscription identity differs from sealed settings"
    );
    if let Some(identity) = &seal.validator_identity {
        validate_loopback_endpoint(&seal.source_endpoint)?;
        ensure!(
            seal.files.len() == 3
                && seal.files[1].path == RPC_BEFORE_FILE
                && seal.files[2].path == RPC_AFTER_FILE,
            "missing validator identity evidence"
        );
        for expected in &seal.files[1..] {
            ensure!(
                file_evidence(root, &expected.path)? == *expected,
                "validator RPC evidence digest mismatch"
            );
            let evidence: ValidatorRpcEvidence = read_json(&root.join(&expected.path))?;
            ensure!(
                rpc_identity(&evidence)? == *identity,
                "validator identity evidence differs from seal"
            );
        }
    } else {
        ensure!(
            seal.files.len() == 1,
            "unbound capture has unexpected source identity objects"
        );
    }
    let mut file = open_regular(&root.join(EVENTS_FILE), false)?;
    lock(&file, false)?;
    let before = file.metadata()?;
    ensure!(
        before.len() == seal.files[0].bytes && hash_file(&mut file)? == seal.files[0].sha256,
        "current WAL length or digest mismatch"
    );
    let counts = read_frames(
        &mut file,
        max_record_bytes.min(seal.max_record_bytes),
        callback,
    )?;
    ensure!(
        counts == seal.counts,
        "current capture counts differ from seal"
    );
    let after = file.metadata()?;
    #[cfg(unix)]
    ensure!(
        before.dev() == after.dev()
            && before.ino() == after.ino()
            && before.len() == after.len()
            && before.mtime() == after.mtime()
            && before.mtime_nsec() == after.mtime_nsec()
            && before.ctime() == after.ctime()
            && before.ctime_nsec() == after.ctime_nsec(),
        "current WAL changed during read"
    );
    #[cfg(not(unix))]
    ensure!(
        before.len() == after.len() && before.modified()? == after.modified()?,
        "current WAL changed during read"
    );
    Ok(seal)
}

/// Open one subscription without automatic reconnect or lossy bank deduplication. A transport
/// failure leaves the attempt unsealed. A new finite attempt needs a new output directory.
pub async fn record_current_grpc(config: CurrentGrpcConfig) -> Result<CurrentGrpcSeal> {
    validate_config(&config)?;
    let mut writer = CaptureWriter::create(&config)?;
    write_json_new(
        &config.output_dir,
        "CAPTURE-STARTED.v1.json",
        &serde_json::json!({
            "schema":"hivezilla-current-grpc-started-v1", "payload_format":PAYLOAD_FORMAT,
            "protobuf_schema":PROTOBUF_SCHEMA, "requested_commitment":config.commitment,
            "source_endpoint":config.endpoint, "archive_publication_allowed":false
        }),
    )?;
    let started = Instant::now();
    let deadline = TokioInstant::now()
        .checked_add(Duration::from_secs(config.timeout_secs))
        .context("current capture deadline overflow")?;
    let result: Result<CurrentGrpcSeal> = async {
        ensure!(
            super::low_disk_bytes(&config.output_dir, config.min_free_bytes)?.is_none(),
            "current capture free disk floor reached"
        );
        if let Some(endpoint) = &config.validator_rpc_endpoint {
            let evidence = timeout_at(deadline, fetch_rpc_evidence(endpoint))
                .await
                .context("current validator RPC preflight timeout")??;
            writer.validator_identity = Some(rpc_identity(&evidence)?);
            write_json_new(&config.output_dir, RPC_BEFORE_FILE, &evidence)?;
        }
        let token = config
            .x_token_file
            .as_ref()
            .map(|path| -> Result<String> {
                let file = open_regular(path, false)?;
                ensure!(file.metadata()?.len() <= 4096, "x-token exceeds limit");
                let mut token = String::new();
                file.take(4097).read_to_string(&mut token)?;
                let token = token.trim_end_matches(['\r', '\n']).to_owned();
                ensure!(
                    !token.is_empty() && !token.contains(['\r', '\n']),
                    "invalid x-token"
                );
                Ok(token)
            })
            .transpose()?;
        let mut builder = GeyserGrpcClient::build_from_shared(config.endpoint.clone())?
            .x_token(token)?
            .max_decoding_message_size(usize::try_from(config.max_record_bytes)?)
            .initial_connection_window_size(32 * 1024 * 1024)
            .initial_stream_window_size(32 * 1024 * 1024)
            .http2_adaptive_window(true)
            .connect_timeout(Duration::from_secs(30));
        if config.endpoint.starts_with("https://") {
            builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let mut client = timeout_at(deadline, builder.connect())
            .await
            .context("current connect timeout")??;
        let (mut sink, stream) = mpsc::channel(8);
        sink.send(current_subscription(&config)).await?;
        let request = crate::grpc::grpc_subscription_transport_request(stream, config.from_slot)?;
        let response = timeout_at(deadline, client.geyser.subscribe(request))
            .await
            .context("current subscribe timeout")??;
        let mut updates = response.into_inner();
        let mut idle_deadline = TokioInstant::now()
            .checked_add(Duration::from_secs(config.idle_timeout_secs))
            .context("current idle deadline overflow")?;
        while writer.counts.blocks < config.max_blocks {
            ensure!(
                super::low_disk_bytes(&config.output_dir, config.min_free_bytes)?.is_none(),
                "current capture free disk floor reached"
            );
            let update = timeout_at(deadline.min(idle_deadline), updates.next())
                .await
                .context("current capture total or data idle timeout")?
                .context("current capture stream ended before requested blocks")??;
            match update.update_oneof.as_ref() {
                Some(UpdateOneof::Ping(_)) => {
                    timeout_at(
                        deadline.min(idle_deadline),
                        sink.send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        }),
                    )
                    .await
                    .context("current ping response timeout")??;
                    continue;
                }
                Some(UpdateOneof::Pong(_)) => continue,
                _ => {}
            }
            writer.append(&update)?;
            idle_deadline = TokioInstant::now()
                .checked_add(Duration::from_secs(config.idle_timeout_secs))
                .context("current idle deadline overflow")?;
        }
        drop(updates);
        drop(sink);
        drop(client);
        if let Some(endpoint) = &config.validator_rpc_endpoint {
            let evidence = timeout_at(deadline, fetch_rpc_evidence(endpoint))
                .await
                .context("current validator RPC postflight timeout")??;
            ensure!(
                writer.validator_identity.as_ref() == Some(&rpc_identity(&evidence)?),
                "validator identity or genesis changed during capture"
            );
            write_json_new(&config.output_dir, RPC_AFTER_FILE, &evidence)?;
        }
        writer.seal(&config, started.elapsed().as_secs_f64())
    }
    .await;
    if let Err(error) = &result {
        let _ = write_json_new(
            &config.output_dir,
            "CAPTURE-FAILED.v1.json",
            &serde_json::json!({
                "schema":"hivezilla-current-grpc-failed-v1", "state":"UNSEALED",
                "error":format!("{error:#}"), "archive_publication_allowed":false
            }),
        );
    }
    result
}

#[cfg(test)]
mod tests;
