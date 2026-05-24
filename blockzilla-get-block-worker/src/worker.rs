use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash, ArchiveV2BlockAccessPubkey,
    ArchiveV2GetBlockIndexRow, ArchiveV2HotBlockBlob, ArchiveV2HotBlockIndexRow,
    ArchiveV2HotInstructionData, ArchiveV2HotMessagePayload, ArchiveV2HotTxRow,
    ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync,
    BlockzillaGetBlockBlobEncoding, BlockzillaGetBlockBundleV1, CompactInnerInstructions,
    CompactInstructionError as InstructionError, CompactLogStream, CompactMetaV1, CompactPubkey,
    CompactReturnData, CompactReward, CompactTokenBalance,
    CompactTransactionError as StoredTransactionError, KeyStore, LogEvent,
    OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
    WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION, WINCODE_BLOCKZILLA_GET_BLOCK_BUNDLE_VERSION,
    WincodeLeb128Config, deserialize_archive_v2_hot_block_blob,
    program_logs::{self, ProgramLog},
    wincode_leb128_config,
};
use futures_channel::mpsc;
use futures_util::{SinkExt, future};
use ruzstd::decoding::StreamingDecoder;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    io::{Read, Write},
    rc::Rc,
};
use wasm_bindgen::{JsCast, JsValue};
use wincode::{SchemaRead, io::Reader};
use worker::{
    Bucket, Cache, Fetch, Headers, Method, Range as R2Range, Request, RequestInit, Response,
    Result, RouteContext, Router, UploadedPart, console_error, event,
};

const SIGNATURE_BYTES: u64 = 64;
const SLOTS_PER_EPOCH: u64 = 432_000;

const BLOCK_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const INFO_CACHE_CONTROL: &str = "public, max-age=30";
const ERROR_CACHE_CONTROL: &str = "no-store";
const LOG_MESSAGES_BYTES_LIMIT: usize = 10 * 1000;

fn base58_32(bytes: &[u8; 32]) -> String {
    let mut buf = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
    let len = five8::encode_32(bytes, &mut buf) as usize;
    // five8 writes only Bitcoin base58 alphabet bytes, which are valid UTF-8.
    unsafe { String::from_utf8_unchecked(buf[..len].to_vec()) }
}

fn base58_64(bytes: &[u8; 64]) -> String {
    let mut buf = [0u8; five8::BASE58_ENCODED_64_MAX_LEN];
    let len = five8::encode_64(bytes, &mut buf) as usize;
    // five8 writes only Bitcoin base58 alphabet bytes, which are valid UTF-8.
    unsafe { String::from_utf8_unchecked(buf[..len].to_vec()) }
}

fn base58_bytes(bytes: &[u8]) -> String {
    match bytes.len() {
        32 => base58_32(bytes.try_into().expect("checked 32-byte base58 input")),
        64 => base58_64(bytes.try_into().expect("checked 64-byte base58 input")),
        _ => bs58::encode(bytes).into_string(),
    }
}

const JSON_STREAM_CHUNK_BYTES: usize = 64 * 1024;

struct JsonByteWriter {
    output: JsonByteOutput,
}

enum JsonByteOutput {
    Buffered(Vec<u8>),
    Channel {
        sender: mpsc::Sender<std::result::Result<Vec<u8>, worker::Error>>,
        current: Vec<u8>,
        threshold: usize,
        len: usize,
        sent_bytes: usize,
        max_capacity: usize,
        failed: bool,
    },
}

impl JsonByteWriter {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            output: JsonByteOutput::Buffered(Vec::with_capacity(capacity)),
        }
    }

    fn channel(
        threshold: usize,
        sender: mpsc::Sender<std::result::Result<Vec<u8>, worker::Error>>,
    ) -> Self {
        let threshold = threshold.max(1024);
        Self {
            output: JsonByteOutput::Channel {
                sender,
                current: Vec::with_capacity(threshold),
                threshold,
                len: 0,
                sent_bytes: 0,
                max_capacity: threshold,
                failed: false,
            },
        }
    }

    fn into_inner(self) -> Vec<u8> {
        match self.output {
            JsonByteOutput::Buffered(out) => out,
            JsonByteOutput::Channel { .. } => Vec::new(),
        }
    }

    fn len(&self) -> usize {
        match &self.output {
            JsonByteOutput::Buffered(out) => out.len(),
            JsonByteOutput::Channel { len, .. } => *len,
        }
    }

    fn sent_bytes(&self) -> usize {
        match &self.output {
            JsonByteOutput::Buffered(_) => 0,
            JsonByteOutput::Channel { sent_bytes, .. } => *sent_bytes,
        }
    }

    fn capacity(&self) -> usize {
        match &self.output {
            JsonByteOutput::Buffered(out) => out.capacity(),
            JsonByteOutput::Channel { max_capacity, .. } => *max_capacity,
        }
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.write_raw(bytes);
    }

    fn string(&mut self, value: &str) -> std::result::Result<(), String> {
        serde_json::to_writer(&mut *self, value).map_err(|err| err.to_string())
    }

    fn json<T: Serialize + ?Sized>(&mut self, value: &T) -> std::result::Result<(), String> {
        serde_json::to_writer(&mut *self, value).map_err(|err| err.to_string())
    }

    fn u64(&mut self, value: u64) {
        let mut buffer = itoa::Buffer::new();
        self.raw(buffer.format(value).as_bytes());
    }

    fn i64(&mut self, value: i64) {
        let mut buffer = itoa::Buffer::new();
        self.raw(buffer.format(value).as_bytes());
    }

    fn json_bool(&mut self, value: bool) {
        self.raw(if value { b"true" } else { b"false" });
    }

    fn option_i64(&mut self, value: Option<i64>) {
        if let Some(value) = value {
            self.i64(value);
        } else {
            self.raw(b"null");
        }
    }

    fn option_u64(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.u64(value);
        } else {
            self.raw(b"null");
        }
    }

    fn option_u8(&mut self, value: Option<u8>) {
        if let Some(value) = value {
            self.u64(u64::from(value));
        } else {
            self.raw(b"null");
        }
    }

    fn base58_32(&mut self, bytes: &[u8; 32]) {
        let mut buf = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
        let len = five8::encode_32(bytes, &mut buf) as usize;
        self.raw(b"\"");
        self.raw(&buf[..len]);
        self.raw(b"\"");
    }

    fn base58_64(&mut self, bytes: &[u8; 64]) {
        let mut buf = [0u8; five8::BASE58_ENCODED_64_MAX_LEN];
        let len = five8::encode_64(bytes, &mut buf) as usize;
        self.raw(b"\"");
        self.raw(&buf[..len]);
        self.raw(b"\"");
    }

    fn u8_array(&mut self, values: &[u8]) {
        self.raw(b"[");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.raw(b",");
            }
            self.u64(u64::from(*value));
        }
        self.raw(b"]");
    }

    fn u64_array(&mut self, values: &[u64]) {
        self.raw(b"[");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.raw(b",");
            }
            self.u64(*value);
        }
        self.raw(b"]");
    }

    fn write_raw(&mut self, mut bytes: &[u8]) {
        match &mut self.output {
            JsonByteOutput::Buffered(out) => out.extend_from_slice(bytes),
            JsonByteOutput::Channel {
                current,
                threshold,
                len,
                max_capacity,
                ..
            } => {
                *len = len.saturating_add(bytes.len());
                while !bytes.is_empty() {
                    let available = threshold.saturating_sub(current.len()).max(1);
                    let take = available.min(bytes.len());
                    current.extend_from_slice(&bytes[..take]);
                    *max_capacity = (*max_capacity).max(current.capacity());
                    bytes = &bytes[take..];
                }
            }
        }
    }

    async fn flush_chunk_async(&mut self) -> std::result::Result<(), String> {
        if let JsonByteOutput::Channel {
            sender,
            current,
            threshold,
            sent_bytes,
            max_capacity,
            failed,
            ..
        } = &mut self.output
        {
            if *failed || current.is_empty() {
                return Ok(());
            }
            let chunk = std::mem::take(current);
            *current = Vec::with_capacity(*threshold);
            *max_capacity = (*max_capacity).max(current.capacity());
            let chunk_len = chunk.len();
            if sender.send(Ok(chunk)).await.is_err() {
                *failed = true;
                return Err("response stream closed before chunk could be sent".to_string());
            }
            *sent_bytes = sent_bytes.saturating_add(chunk_len);
        }
        Ok(())
    }
}

impl Write for JsonByteWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_raw(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn stream_optional_transaction_error(
    w: &mut JsonByteWriter,
    err: Option<&StoredTransactionError>,
) -> std::result::Result<(), String> {
    if let Some(err) = err {
        stream_stored_transaction_error(w, err)
    } else {
        w.raw(b"null");
        Ok(())
    }
}

fn stream_transaction_status(
    w: &mut JsonByteWriter,
    err: Option<&StoredTransactionError>,
) -> std::result::Result<(), String> {
    if let Some(err) = err {
        w.raw(b"{\"Err\":");
        stream_stored_transaction_error(w, err)?;
        w.raw(b"}");
    } else {
        w.raw(br#"{"Ok":null}"#);
    }
    Ok(())
}

fn stream_stored_transaction_error(
    w: &mut JsonByteWriter,
    err: &StoredTransactionError,
) -> std::result::Result<(), String> {
    match err {
        StoredTransactionError::AccountInUse => w.string("AccountInUse"),
        StoredTransactionError::AccountLoadedTwice => w.string("AccountLoadedTwice"),
        StoredTransactionError::AccountNotFound => w.string("AccountNotFound"),
        StoredTransactionError::ProgramAccountNotFound => w.string("ProgramAccountNotFound"),
        StoredTransactionError::InsufficientFundsForFee => w.string("InsufficientFundsForFee"),
        StoredTransactionError::InvalidAccountForFee => w.string("InvalidAccountForFee"),
        StoredTransactionError::AlreadyProcessed => w.string("AlreadyProcessed"),
        StoredTransactionError::BlockhashNotFound => w.string("BlockhashNotFound"),
        StoredTransactionError::InstructionError(index, err) => {
            w.raw(b"{\"InstructionError\":[");
            w.u64(u64::from(*index));
            w.raw(b",");
            stream_instruction_error(w, err)?;
            w.raw(b"]}");
            Ok(())
        }
        StoredTransactionError::CallChainTooDeep => w.string("CallChainTooDeep"),
        StoredTransactionError::MissingSignatureForFee => w.string("MissingSignatureForFee"),
        StoredTransactionError::InvalidAccountIndex => w.string("InvalidAccountIndex"),
        StoredTransactionError::SignatureFailure => w.string("SignatureFailure"),
        StoredTransactionError::InvalidProgramForExecution => {
            w.string("InvalidProgramForExecution")
        }
        StoredTransactionError::SanitizeFailure => w.string("SanitizeFailure"),
        StoredTransactionError::ClusterMaintenance => w.string("ClusterMaintenance"),
        StoredTransactionError::AccountBorrowOutstanding => w.string("AccountBorrowOutstanding"),
        StoredTransactionError::WouldExceedMaxBlockCostLimit => {
            w.string("WouldExceedMaxBlockCostLimit")
        }
        StoredTransactionError::UnsupportedVersion => w.string("UnsupportedVersion"),
        StoredTransactionError::InvalidWritableAccount => w.string("InvalidWritableAccount"),
        StoredTransactionError::WouldExceedMaxAccountCostLimit => {
            w.string("WouldExceedMaxAccountCostLimit")
        }
        StoredTransactionError::WouldExceedAccountDataBlockLimit => {
            w.string("WouldExceedAccountDataBlockLimit")
        }
        StoredTransactionError::TooManyAccountLocks => w.string("TooManyAccountLocks"),
        StoredTransactionError::AddressLookupTableNotFound => {
            w.string("AddressLookupTableNotFound")
        }
        StoredTransactionError::InvalidAddressLookupTableOwner => {
            w.string("InvalidAddressLookupTableOwner")
        }
        StoredTransactionError::InvalidAddressLookupTableData => {
            w.string("InvalidAddressLookupTableData")
        }
        StoredTransactionError::InvalidAddressLookupTableIndex => {
            w.string("InvalidAddressLookupTableIndex")
        }
        StoredTransactionError::InvalidRentPayingAccount => w.string("InvalidRentPayingAccount"),
        StoredTransactionError::WouldExceedMaxVoteCostLimit => {
            w.string("WouldExceedMaxVoteCostLimit")
        }
        StoredTransactionError::WouldExceedAccountDataTotalLimit => {
            w.string("WouldExceedAccountDataTotalLimit")
        }
        StoredTransactionError::DuplicateInstruction(index) => {
            w.raw(b"{\"DuplicateInstruction\":");
            w.u64(u64::from(*index));
            w.raw(b"}");
            Ok(())
        }
        StoredTransactionError::InsufficientFundsForRent { account_index } => {
            w.raw(b"{\"InsufficientFundsForRent\":{\"account_index\":");
            w.u64(u64::from(*account_index));
            w.raw(b"}}");
            Ok(())
        }
        StoredTransactionError::MaxLoadedAccountsDataSizeExceeded => {
            w.string("MaxLoadedAccountsDataSizeExceeded")
        }
        StoredTransactionError::InvalidLoadedAccountsDataSizeLimit => {
            w.string("InvalidLoadedAccountsDataSizeLimit")
        }
        StoredTransactionError::ResanitizationNeeded => w.string("ResanitizationNeeded"),
        StoredTransactionError::ProgramExecutionTemporarilyRestricted { account_index } => {
            w.raw(b"{\"ProgramExecutionTemporarilyRestricted\":{\"account_index\":");
            w.u64(u64::from(*account_index));
            w.raw(b"}}");
            Ok(())
        }
        StoredTransactionError::UnbalancedTransaction => w.string("UnbalancedTransaction"),
        StoredTransactionError::ProgramCacheHitMaxLimit => w.string("ProgramCacheHitMaxLimit"),
        StoredTransactionError::CommitCancelled => w.string("CommitCancelled"),
    }
}

fn stream_instruction_error(
    w: &mut JsonByteWriter,
    err: &InstructionError,
) -> std::result::Result<(), String> {
    match err {
        InstructionError::GenericError => w.string("GenericError"),
        InstructionError::InvalidArgument => w.string("InvalidArgument"),
        InstructionError::InvalidInstructionData => w.string("InvalidInstructionData"),
        InstructionError::InvalidAccountData => w.string("InvalidAccountData"),
        InstructionError::AccountDataTooSmall => w.string("AccountDataTooSmall"),
        InstructionError::InsufficientFunds => w.string("InsufficientFunds"),
        InstructionError::IncorrectProgramId => w.string("IncorrectProgramId"),
        InstructionError::MissingRequiredSignature => w.string("MissingRequiredSignature"),
        InstructionError::AccountAlreadyInitialized => w.string("AccountAlreadyInitialized"),
        InstructionError::UninitializedAccount => w.string("UninitializedAccount"),
        InstructionError::UnbalancedInstruction => w.string("UnbalancedInstruction"),
        InstructionError::ModifiedProgramId => w.string("ModifiedProgramId"),
        InstructionError::ExternalAccountLamportSpend => w.string("ExternalAccountLamportSpend"),
        InstructionError::ExternalAccountDataModified => w.string("ExternalAccountDataModified"),
        InstructionError::ReadonlyLamportChange => w.string("ReadonlyLamportChange"),
        InstructionError::ReadonlyDataModified => w.string("ReadonlyDataModified"),
        InstructionError::DuplicateAccountIndex => w.string("DuplicateAccountIndex"),
        InstructionError::ExecutableModified => w.string("ExecutableModified"),
        InstructionError::RentEpochModified => w.string("RentEpochModified"),
        InstructionError::NotEnoughAccountKeys => w.string("NotEnoughAccountKeys"),
        InstructionError::AccountDataSizeChanged => w.string("AccountDataSizeChanged"),
        InstructionError::AccountNotExecutable => w.string("AccountNotExecutable"),
        InstructionError::AccountBorrowFailed => w.string("AccountBorrowFailed"),
        InstructionError::AccountBorrowOutstanding => w.string("AccountBorrowOutstanding"),
        InstructionError::DuplicateAccountOutOfSync => w.string("DuplicateAccountOutOfSync"),
        InstructionError::Custom(code) => {
            w.raw(b"{\"Custom\":");
            w.u64(u64::from(*code));
            w.raw(b"}");
            Ok(())
        }
        InstructionError::InvalidError => w.string("InvalidError"),
        InstructionError::ExecutableDataModified => w.string("ExecutableDataModified"),
        InstructionError::ExecutableLamportChange => w.string("ExecutableLamportChange"),
        InstructionError::ExecutableAccountNotRentExempt => {
            w.string("ExecutableAccountNotRentExempt")
        }
        InstructionError::UnsupportedProgramId => w.string("UnsupportedProgramId"),
        InstructionError::CallDepth => w.string("CallDepth"),
        InstructionError::MissingAccount => w.string("MissingAccount"),
        InstructionError::ReentrancyNotAllowed => w.string("ReentrancyNotAllowed"),
        InstructionError::MaxSeedLengthExceeded => w.string("MaxSeedLengthExceeded"),
        InstructionError::InvalidSeeds => w.string("InvalidSeeds"),
        InstructionError::InvalidRealloc => w.string("InvalidRealloc"),
        InstructionError::ComputationalBudgetExceeded => w.string("ComputationalBudgetExceeded"),
        InstructionError::PrivilegeEscalation => w.string("PrivilegeEscalation"),
        InstructionError::ProgramEnvironmentSetupFailure => {
            w.string("ProgramEnvironmentSetupFailure")
        }
        InstructionError::ProgramFailedToComplete => w.string("ProgramFailedToComplete"),
        InstructionError::ProgramFailedToCompile => w.string("ProgramFailedToCompile"),
        InstructionError::Immutable => w.string("Immutable"),
        InstructionError::IncorrectAuthority => w.string("IncorrectAuthority"),
        InstructionError::BorshIoError(_) => w.string("BorshIoError"),
        InstructionError::AccountNotRentExempt => w.string("AccountNotRentExempt"),
        InstructionError::InvalidAccountOwner => w.string("InvalidAccountOwner"),
        InstructionError::ArithmeticOverflow => w.string("ArithmeticOverflow"),
        InstructionError::UnsupportedSysvar => w.string("UnsupportedSysvar"),
        InstructionError::IllegalOwner => w.string("IllegalOwner"),
        InstructionError::MaxAccountsDataAllocationsExceeded => {
            w.string("MaxAccountsDataAllocationsExceeded")
        }
        InstructionError::MaxAccountsExceeded => w.string("MaxAccountsExceeded"),
        InstructionError::MaxInstructionTraceLengthExceeded => {
            w.string("MaxInstructionTraceLengthExceeded")
        }
        InstructionError::BuiltinProgramsMustConsumeComputeUnits => {
            w.string("BuiltinProgramsMustConsumeComputeUnits")
        }
    }
}

fn transaction_error_value(
    err: Option<&StoredTransactionError>,
) -> std::result::Result<Value, String> {
    let Some(err) = err else {
        return Ok(Value::Null);
    };
    let mut w = JsonByteWriter::with_capacity(64);
    stream_stored_transaction_error(&mut w, err)?;
    serde_json::from_slice(&w.into_inner())
        .map_err(|err| format!("parse transaction error JSON: {err}"))
}

#[derive(Clone)]
struct ProfileHandle(Rc<RefCell<RequestProfile>>);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestProfile {
    started_ms: f64,
    events: Vec<ProfileEvent>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfileEvent {
    name: String,
    ms: f64,
    bytes: u64,
    detail: String,
    status: Option<u16>,
}

#[derive(Default)]
struct ProfileSummary {
    count: u32,
    total_ms: f64,
    bytes: u64,
}

impl ProfileHandle {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(RequestProfile {
            started_ms: now_ms(),
            events: Vec::new(),
        })))
    }

    fn record_elapsed(
        &self,
        name: &str,
        started_ms: f64,
        bytes: u64,
        detail: impl Into<String>,
        status: Option<u16>,
    ) {
        self.0.borrow_mut().events.push(ProfileEvent {
            name: name.to_string(),
            ms: (now_ms() - started_ms).max(0.0),
            bytes,
            detail: detail.into(),
            status,
        });
    }

    fn profile_json(&self) -> Value {
        let profile = self.0.borrow();
        let mut summary = BTreeMap::<String, ProfileSummary>::new();
        for event in &profile.events {
            let entry = summary.entry(event.name.clone()).or_default();
            entry.count += 1;
            entry.total_ms += event.ms;
            entry.bytes += event.bytes;
        }
        let summary = summary
            .into_iter()
            .map(|(name, stats)| {
                json!({
                    "name": name,
                    "count": stats.count,
                    "totalMs": round_ms(stats.total_ms),
                    "bytes": stats.bytes,
                })
            })
            .collect::<Vec<_>>();
        json!({
            "totalMs": round_ms((now_ms() - profile.started_ms).max(0.0)),
            "events": profile.events.clone(),
            "summary": summary,
        })
    }

    fn server_timing(&self) -> String {
        let profile = self.0.borrow();
        let mut summary = BTreeMap::<String, ProfileSummary>::new();
        for event in &profile.events {
            let entry = summary.entry(event.name.clone()).or_default();
            entry.count += 1;
            entry.total_ms += event.ms;
            entry.bytes += event.bytes;
        }
        summary
            .into_iter()
            .map(|(name, stats)| {
                format!(
                    "{};dur={:.2};desc=\"{}x {}B\"",
                    name, stats.total_ms, stats.count, stats.bytes
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn request_profile(req: &Request) -> Option<ProfileHandle> {
    let url = req.url().ok()?;
    let enabled = url.query_pairs().any(|(key, value)| {
        matches!(key.as_ref(), "debug" | "profile")
            && matches!(value.as_ref(), "1" | "true" | "yes" | "on")
    });
    enabled.then(ProfileHandle::new)
}

fn now_ms() -> f64 {
    let global = js_sys::global();
    if let Ok(performance) = js_sys::Reflect::get(&global, &JsValue::from_str("performance"))
        && !performance.is_null()
        && !performance.is_undefined()
        && let Ok(now) = js_sys::Reflect::get(&performance, &JsValue::from_str("now"))
        && let Some(now) = now.dyn_ref::<js_sys::Function>()
        && let Ok(value) = now.call0(&performance)
        && let Some(ms) = value.as_f64()
    {
        return ms;
    }
    js_sys::Date::now()
}

fn round_ms(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[event(fetch)]
async fn main(req: Request, env: worker::Env, ctx: worker::Context) -> Result<Response> {
    if req.method() == Method::Get && block_bin_slot_from_path(&req.path()).is_some() {
        return handle_block_bin(req, env, ctx).await;
    }

    Router::new()
        .get_async("/", handle_info)
        .post_async("/", handle_rpc)
        .get_async("/info", handle_info)
        .get_async("/block/:slot", handle_block)
        .get_async("/block-lite/:slot", handle_block_lite)
        .get_async("/probe", handle_probe_first)
        .get_async("/probe/:slot", handle_probe_slot)
        .post_async("/admin/upload/start", handle_admin_upload_start)
        .put_async("/admin/upload/part", handle_admin_upload_part)
        .post_async("/admin/upload/complete", handle_admin_upload_complete)
        .post_async("/admin/upload/abort", handle_admin_upload_abort)
        .run(req, env)
        .await
}

async fn handle_block_bin(
    req: Request,
    env: worker::Env,
    ctx: worker::Context,
) -> Result<Response> {
    let profile = request_profile(&req);
    let access_mode = block_bin_access_mode(&req);
    let cache_key = profile
        .is_none()
        .then(|| block_bin_cache_key(&req, access_mode))
        .transpose()?;

    if let Some(cache_key) = cache_key.as_ref() {
        if let Some(response) = Cache::default().get(cache_key.clone(), false).await? {
            return Ok(response);
        }
    }

    let path = req.path();
    let Some(slot_text) = block_bin_slot_from_path(&path) else {
        return json_error_profiled(400, "invalid_slot", "missing slot", profile.as_ref());
    };
    let slot = match slot_text.parse::<u64>() {
        Ok(slot) => slot,
        Err(_) => {
            return json_error_profiled(
                400,
                "invalid_slot",
                "slot must be an unsigned integer",
                profile.as_ref(),
            );
        }
    };
    let source = match WorkerSource::from_env(&env, profile.clone()) {
        Ok(source) => source,
        Err(err) => return json_error_profiled(500, "config_error", &err, profile.as_ref()),
    };

    let mut response = match fetch_block_bundle_bytes(&source, slot, access_mode).await {
        Ok(Some(bytes)) => block_bundle_response_profiled(bytes, profile.as_ref())?,
        Ok(None) => cached_json_response_profiled(
            &json!({ "ok": true, "empty": true, "slot": slot }),
            BLOCK_CACHE_CONTROL,
            profile.as_ref(),
        )?,
        Err(err) => {
            console_error!("block bin route failed for slot {}: {}", slot, err);
            json_error_profiled(
                502,
                "blockzilla_decode_error",
                &format!("failed to fetch slot {slot}: {err}"),
                profile.as_ref(),
            )?
        }
    };

    if let Some(cache_key) = cache_key {
        if response.status_code() == 200 {
            let cache_response = response.cloned()?;
            ctx.wait_until(async move {
                if let Err(err) = Cache::default().put(cache_key, cache_response).await {
                    console_error!("block bin cache put failed: {}", err);
                }
            });
        }
    }

    Ok(response)
}

async fn handle_info(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    cached_json_response(
        &json!({
            "ok": true,
            "name": "blockzilla-get-block-worker",
            "json_rpc_methods": ["getBlock", "getBlockTime", "getVersion"],
            "block_routes": ["/block/:slot", "/block-lite/:slot"],
            "binary_formats": {
                ".bin": "Blockzilla getBlock bundle: zstd hot-block blob plus optional block-access blob",
                ".proto.bin": "not implemented"
            },
            "probe_routes": ["GET /probe?epoch=N", "GET /probe/:slot"],
            "archive_source": "blockzilla-v2",
            "archive_backends": ["r2", "s3"],
            "r2_binding": "BZ_ARCHIVE_BUCKET",
        }),
        INFO_CACHE_CONTROL,
    )
}

async fn handle_admin_upload_start(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    require_admin(&req, &ctx.env)?;
    let bucket = admin_bucket(&ctx.env)?;
    let key = admin_upload_key(&req, &ctx.env)?;
    let upload = bucket
        .create_multipart_upload(key.clone())
        .execute()
        .await
        .map_err(|err| worker::Error::RustError(format!("create R2 multipart upload: {err}")))?;
    let upload_id = upload.upload_id().await;
    cached_json_response(
        &json!({
            "ok": true,
            "key": key,
            "uploadId": upload_id,
        }),
        ERROR_CACHE_CONTROL,
    )
}

async fn handle_admin_upload_part(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    require_admin(&req, &ctx.env)?;
    let bucket = admin_bucket(&ctx.env)?;
    let key = admin_upload_key(&req, &ctx.env)?;
    let upload_id = required_query_param(&req, "uploadId")?;
    let part_number = required_query_param(&req, "partNumber")?
        .parse::<u16>()
        .map_err(|_| worker::Error::RustError("partNumber must be a u16".to_string()))?;
    if part_number == 0 {
        return json_error(400, "invalid_part_number", "partNumber must be >= 1");
    }
    let bytes = req.bytes().await?;
    let upload = bucket
        .resume_multipart_upload(key.clone(), upload_id.clone())
        .map_err(|err| worker::Error::RustError(format!("resume R2 multipart upload: {err}")))?;
    let part = upload
        .upload_part(part_number, bytes)
        .await
        .map_err(|err| worker::Error::RustError(format!("upload R2 multipart part: {err}")))?;
    cached_json_response(
        &json!({
            "ok": true,
            "key": key,
            "uploadId": upload_id,
            "partNumber": part.part_number(),
            "etag": part.etag(),
        }),
        ERROR_CACHE_CONTROL,
    )
}

async fn handle_admin_upload_complete(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    require_admin(&req, &ctx.env)?;
    let bucket = admin_bucket(&ctx.env)?;
    let key = admin_upload_key(&req, &ctx.env)?;
    let upload_id = required_query_param(&req, "uploadId")?;
    let body = req.bytes().await?;
    let parts: Vec<AdminUploadedPart> = serde_json::from_slice(&body)
        .map_err(|err| worker::Error::RustError(format!("parse uploaded parts: {err}")))?;
    if parts.is_empty() {
        return json_error(
            400,
            "missing_parts",
            "complete requires at least one uploaded part",
        );
    }
    let upload = bucket
        .resume_multipart_upload(key.clone(), upload_id.clone())
        .map_err(|err| worker::Error::RustError(format!("resume R2 multipart upload: {err}")))?;
    let object = upload
        .complete(
            parts
                .into_iter()
                .map(|part| UploadedPart::new(part.part_number, part.etag)),
        )
        .await
        .map_err(|err| worker::Error::RustError(format!("complete R2 multipart upload: {err}")))?;
    cached_json_response(
        &json!({
            "ok": true,
            "key": object.key(),
            "size": object.size(),
            "etag": object.etag(),
        }),
        ERROR_CACHE_CONTROL,
    )
}

async fn handle_admin_upload_abort(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    require_admin(&req, &ctx.env)?;
    let bucket = admin_bucket(&ctx.env)?;
    let key = admin_upload_key(&req, &ctx.env)?;
    let upload_id = required_query_param(&req, "uploadId")?;
    let upload = bucket
        .resume_multipart_upload(key.clone(), upload_id.clone())
        .map_err(|err| worker::Error::RustError(format!("resume R2 multipart upload: {err}")))?;
    upload
        .abort()
        .await
        .map_err(|err| worker::Error::RustError(format!("abort R2 multipart upload: {err}")))?;
    cached_json_response(
        &json!({
            "ok": true,
            "key": key,
            "uploadId": upload_id,
            "aborted": true,
        }),
        ERROR_CACHE_CONTROL,
    )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdminUploadedPart {
    part_number: u16,
    etag: String,
}

async fn handle_block(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    handle_block_response(req, ctx, RouteBlockMode::Full).await
}

async fn handle_block_lite(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    handle_block_response(req, ctx, RouteBlockMode::Lite).await
}

async fn handle_block_response(
    req: Request,
    ctx: RouteContext<()>,
    mode: RouteBlockMode,
) -> Result<Response> {
    let profile = request_profile(&req);
    let route_started = now_ms();
    let request = match parse_block_request(&req, &ctx) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };
    let source = match WorkerSource::from_env(&ctx.env, profile.clone()) {
        Ok(source) => source,
        Err(err) => return json_error_profiled(500, "config_error", &err, profile.as_ref()),
    };

    let response = match request.format {
        BlockResponseFormat::Json => match render_route_block_bytes(&source, request, mode).await {
            Ok(Some(bytes)) => {
                source.record_elapsed(
                    "route_total",
                    route_started,
                    bytes.len() as u64,
                    "json_streamed",
                    None,
                );
                json_bytes_response_profiled(bytes, BLOCK_CACHE_CONTROL, profile.as_ref())
            }
            Ok(None) => {
                source.record_elapsed("route_total", route_started, 0, "json_empty", None);
                cached_json_response_profiled(
                    &json!({ "ok": true, "empty": true, "slot": request.slot }),
                    BLOCK_CACHE_CONTROL,
                    profile.as_ref(),
                )
            }
            Err(err) => {
                console_error!("block route failed for slot {}: {}", request.slot, err);
                source.record_elapsed("route_total", route_started, 0, "json_error", None);
                json_error_profiled(
                    502,
                    "blockzilla_decode_error",
                    &format!("failed to fetch slot {}: {err}", request.slot),
                    profile.as_ref(),
                )
            }
        },
        BlockResponseFormat::Wincode => {
            let access_mode = block_bin_access_mode(&req);
            match fetch_block_bundle_bytes(&source, request.slot, access_mode).await {
                Ok(Some(bytes)) => {
                    source.record_elapsed(
                        "route_total",
                        route_started,
                        bytes.len() as u64,
                        "wincode_bundle",
                        None,
                    );
                    block_bundle_response_profiled(bytes, profile.as_ref())
                }
                Ok(None) => {
                    source.record_elapsed("route_total", route_started, 0, "wincode_empty", None);
                    cached_json_response_profiled(
                        &json!({ "ok": true, "empty": true, "slot": request.slot }),
                        BLOCK_CACHE_CONTROL,
                        profile.as_ref(),
                    )
                }
                Err(err) => {
                    console_error!("wincode route failed for slot {}: {}", request.slot, err);
                    source.record_elapsed("route_total", route_started, 0, "wincode_error", None);
                    json_error_profiled(
                        502,
                        "blockzilla_decode_error",
                        &format!("failed to fetch slot {}: {err}", request.slot),
                        profile.as_ref(),
                    )
                }
            }
        }
        BlockResponseFormat::Proto => json_error_profiled(
            400,
            "unsupported_format",
            "protobuf block responses are not implemented for Blockzilla V2 hot blocks yet; use .bin for wincode or .json",
            profile.as_ref(),
        ),
    };
    response
}

#[derive(Clone, Copy)]
struct BlockRequest {
    slot: u64,
    format: BlockResponseFormat,
    include_rewards: bool,
}

#[derive(Clone, Copy)]
enum BlockResponseFormat {
    Json,
    Wincode,
    Proto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockBinAccessMode {
    Include,
    Skip,
}

impl BlockBinAccessMode {
    fn cache_key_suffix(self) -> &'static str {
        match self {
            Self::Include => "access",
            Self::Skip => "no-access",
        }
    }
}

#[derive(Clone, Copy)]
enum RouteBlockMode {
    Full,
    Lite,
}

fn parse_block_request(
    req: &Request,
    ctx: &RouteContext<()>,
) -> std::result::Result<BlockRequest, Response> {
    let raw_slot = ctx.param("slot").ok_or_else(invalid_slot_response)?;
    let (slot_text, format) = parse_slot_and_format(raw_slot)?;
    let slot = slot_text.parse().map_err(|_| invalid_slot_response())?;
    let include_rewards = parse_include_rewards(req)?;
    Ok(BlockRequest {
        slot,
        format,
        include_rewards,
    })
}

fn parse_slot_and_format(
    raw_slot: &str,
) -> std::result::Result<(&str, BlockResponseFormat), Response> {
    if let Some(slot) = raw_slot.strip_suffix(".proto.bin") {
        return Ok((slot, BlockResponseFormat::Proto));
    }
    if raw_slot.ends_with(".zstd") {
        return Err(json_error(
            400,
            "unsupported_format",
            "zstd block responses are not supported; use .json or .bin",
        )
        .unwrap_or_else(|_| Response::error("Unsupported format", 400).unwrap()));
    }
    if let Some(slot) = raw_slot.strip_suffix(".json") {
        return Ok((slot, BlockResponseFormat::Json));
    }
    if let Some(slot) = raw_slot.strip_suffix(".bin") {
        return Ok((slot, BlockResponseFormat::Wincode));
    }
    if raw_slot.contains('.') {
        return Err(json_error(
            400,
            "unsupported_format",
            "unsupported block response extension; use .json, .bin, or .proto.bin",
        )
        .unwrap_or_else(|_| Response::error("Unsupported format", 400).unwrap()));
    }

    Ok((raw_slot, BlockResponseFormat::Json))
}

fn block_bin_slot_from_path(path: &str) -> Option<&str> {
    let slot = path.strip_prefix("/block/")?.strip_suffix(".bin")?;
    (!slot.is_empty() && !slot.contains('/')).then_some(slot)
}

fn block_bin_access_mode(req: &Request) -> BlockBinAccessMode {
    let skip_access = req.url().ok().is_some_and(|url| {
        url.query_pairs().any(|(key, value)| {
            let key = key.as_ref();
            let value = value.as_ref();
            (matches!(key, "access" | "includeAccess")
                && matches!(value, "0" | "false" | "no" | "none" | "skip"))
                || (matches!(key, "noAccess" | "skipAccess")
                    && matches!(value, "" | "1" | "true" | "yes"))
        })
    });
    if skip_access {
        BlockBinAccessMode::Skip
    } else {
        BlockBinAccessMode::Include
    }
}

fn block_bin_cache_key(req: &Request, access_mode: BlockBinAccessMode) -> Result<String> {
    let mut url = req.url()?;
    url.query_pairs_mut()
        .append_pair("__bz_access", access_mode.cache_key_suffix());
    Ok(url.to_string())
}

fn parse_include_rewards(req: &Request) -> std::result::Result<bool, Response> {
    let url = req
        .url()
        .map_err(|_| Response::error("Invalid request URL", 400).unwrap())?;
    let mut include_rewards = true;
    for (key, value) in url.query_pairs() {
        if key == "rewards" || key == "includeRewards" || key == "include_rewards" {
            include_rewards = parse_bool_query_value(&value).ok_or_else(|| {
                json_error(
                    400,
                    "invalid_rewards",
                    "rewards must be true/false, 1/0, yes/no, or on/off",
                )
                .unwrap_or_else(|_| Response::error("Invalid rewards flag", 400).unwrap())
            })?;
        }
    }
    Ok(include_rewards)
}

fn parse_bool_query_value(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn invalid_slot_response() -> Response {
    json_error(400, "invalid_slot", "slot must be an unsigned integer")
        .unwrap_or_else(|_| Response::error("Invalid slot", 400).unwrap())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("Method not found: {method}"),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32000,
            message: message.into(),
        }
    }

    fn skipped_slot(slot: u64) -> Self {
        Self {
            code: -32009,
            message: format!("Slot {slot} was skipped, or missing in long-term storage"),
        }
    }
}

async fn handle_rpc(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let profile = request_profile(&req);
    let rpc_started = now_ms();
    let source = match WorkerSource::from_env(&ctx.env, profile.clone()) {
        Ok(source) => source,
        Err(err) => {
            if let Some(profile) = profile.as_ref() {
                profile.record_elapsed("rpc_total", rpc_started, 0, "config_error", None);
            }
            return json_rpc_response_profiled(
                error_response(Value::Null, -32000, err),
                profile.as_ref(),
            );
        }
    };
    let body_started = now_ms();
    let body = req.bytes().await?;
    source.record_elapsed(
        "request_body",
        body_started,
        body.len() as u64,
        "json_rpc",
        None,
    );
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            source.record_elapsed("rpc_total", rpc_started, 0, "parse_error", None);
            return json_rpc_response_profiled(
                error_response(Value::Null, -32700, err.to_string()),
                profile.as_ref(),
            );
        }
    };

    if value.is_array() {
        source.record_elapsed("rpc_total", rpc_started, 0, "batch_rejected", None);
        return json_rpc_response_profiled(
            error_response(
                Value::Null,
                -32600,
                "JSON-RPC batch requests are not supported; submit one request per HTTP call for predictable pricing",
            ),
            profile.as_ref(),
        );
    }

    match handle_one_streamed(&source, &value).await {
        Ok(Some(StreamedRpcBody::Buffered(bytes))) => {
            source.record_elapsed(
                "rpc_total",
                rpc_started,
                bytes.len() as u64,
                "single_streamed",
                None,
            );
            return json_bytes_response_profiled(bytes, ERROR_CACHE_CONTROL, profile.as_ref());
        }
        Ok(Some(StreamedRpcBody::Stream(body))) => {
            source.record_elapsed("rpc_total", rpc_started, 0, "single_stream_body", None);
            return json_stream_response_profiled(body, ERROR_CACHE_CONTROL, profile.as_ref());
        }
        Ok(None) => {}
        Err(response) => {
            source.record_elapsed("rpc_total", rpc_started, 0, "single_streamed_error", None);
            return json_rpc_response_profiled(response, profile.as_ref());
        }
    }

    let response = handle_one(&source, value).await;
    source.record_elapsed("rpc_total", rpc_started, 0, "single", None);
    json_rpc_response_profiled(response, profile.as_ref())
}

enum StreamedRpcBody {
    Buffered(Vec<u8>),
    Stream(mpsc::Receiver<std::result::Result<Vec<u8>, worker::Error>>),
}

async fn handle_one_streamed(
    source: &WorkerSource,
    value: &Value,
) -> std::result::Result<Option<StreamedRpcBody>, Value> {
    let Ok(request) = serde_json::from_value::<RpcRequest>(value.clone()) else {
        return Ok(None);
    };
    let id = request.id.clone().unwrap_or(Value::Null);
    if request.jsonrpc.as_deref() != Some("2.0") {
        return Ok(None);
    }
    if request.params.as_ref().is_some_and(contains_json_parsed) {
        return Ok(None);
    }
    let Some(method) = request.method.as_deref() else {
        return Ok(None);
    };

    match method {
        "getBlock" => {
            let Ok((slot, config)) = get_block_params(&request) else {
                return Ok(None);
            };
            if !should_stream_get_block(config) {
                return Ok(None);
            }

            let block = match fetch_block(source, slot, access_fetch_mode(config)).await {
                Ok(Some(block)) => block,
                Ok(None) => {
                    let err = RpcError::skipped_slot(slot);
                    return Err(error_response(id, err.code, err.message));
                }
                Err(err) => {
                    let err = RpcError::internal(format!("failed to decode slot {slot}: {err}"));
                    return Err(error_response(id, err.code, err.message));
                }
            };
            let body = stream_json_rpc_get_block_success_body(source.clone(), id, block, config);
            Ok(Some(StreamedRpcBody::Stream(body)))
        }
        "getBlockTime" => {
            let params = match params_array(&request) {
                Ok(params) => params,
                Err(_) => return Ok(None),
            };
            let Some(slot) = params.first().and_then(Value::as_u64) else {
                return Ok(None);
            };
            let block = match fetch_block(source, slot, AccessFetchMode::None).await {
                Ok(Some(block)) => block,
                Ok(None) => {
                    let err = RpcError::skipped_slot(slot);
                    return Err(error_response(id, err.code, err.message));
                }
                Err(err) => {
                    let err = RpcError::internal(format!("failed to decode slot {slot}: {err}"));
                    return Err(error_response(id, err.code, err.message));
                }
            };
            let result = stream_block_time_value(source, block.block.header.block_time);
            stream_json_rpc_success(&id, result)
                .map(|bytes| Some(StreamedRpcBody::Buffered(bytes)))
                .map_err(|err| {
                    let err = RpcError::internal(format!(
                        "failed to encode response for slot {slot}: {err}"
                    ));
                    error_response(id, err.code, err.message)
                })
        }
        "getVersion" => {
            let started = now_ms();
            let mut result = Vec::with_capacity(64);
            result.extend_from_slice(br#"{"solana-core":"blockzilla-get-block-worker/"#);
            result.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
            result.extend_from_slice(br#""}"#);
            source.record_elapsed(
                "render_streamed_version",
                started,
                result.len() as u64,
                "version",
                None,
            );
            stream_json_rpc_success(&id, result)
                .map(|bytes| Some(StreamedRpcBody::Buffered(bytes)))
                .map_err(|err| {
                    let err = RpcError::internal(format!("failed to encode response: {err}"));
                    error_response(id, err.code, err.message)
                })
        }
        _ => Ok(None),
    }
}

async fn handle_one(source: &WorkerSource, value: Value) -> Value {
    let request: RpcRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(err) => return error_response(Value::Null, -32600, err.to_string()),
    };
    let id = request.id.clone().unwrap_or(Value::Null);

    if request.jsonrpc.as_deref() != Some("2.0") {
        return error_response(id, -32600, "Invalid Request: jsonrpc must be \"2.0\"");
    }
    let Some(method) = request.method.as_deref() else {
        return error_response(id, -32600, "Invalid Request: missing method");
    };

    if request.params.as_ref().is_some_and(contains_json_parsed) {
        return error_response(
            id,
            -32602,
            "The jsonParsed encoding is not supported by this archive RPC",
        );
    }

    let result = match method {
        "getBlock" => handle_get_block(source, &request).await,
        "getBlockTime" => handle_get_block_time(source, &request).await,
        "getVersion" => Ok(json!({
            "solana-core": concat!("blockzilla-get-block-worker/", env!("CARGO_PKG_VERSION"))
        })),
        _ => Err(RpcError::method_not_found(method)),
    };

    match result {
        Ok(result) => success_response(id, result),
        Err(err) => error_response(id, err.code, err.message),
    }
}

fn should_stream_get_block(config: GetBlockConfig) -> bool {
    match config.transaction_details {
        TransactionDetails::Full
        | TransactionDetails::None
        | TransactionDetails::Signatures
        | TransactionDetails::Accounts => true,
    }
}

async fn handle_get_block(
    source: &WorkerSource,
    request: &RpcRequest,
) -> std::result::Result<Value, RpcError> {
    let (slot, config) = get_block_params(request)?;
    let Some(block) = fetch_block(source, slot, access_fetch_mode(config))
        .await
        .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))?
    else {
        return Err(RpcError::skipped_slot(slot));
    };
    render_block_value(source, &block, config, None)
        .await
        .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))
}

async fn handle_get_block_time(
    source: &WorkerSource,
    request: &RpcRequest,
) -> std::result::Result<Value, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlockTime requires slot as first parameter"))?;
    let Some(block) = fetch_block(source, slot, AccessFetchMode::None)
        .await
        .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))?
    else {
        return Err(RpcError::skipped_slot(slot));
    };
    Ok(rpc_block_time(block.block.header.block_time)
        .map(Value::from)
        .unwrap_or(Value::Null))
}

fn get_block_params(request: &RpcRequest) -> std::result::Result<(u64, GetBlockConfig), RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlock requires slot as first parameter"))?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;
    Ok((slot, config))
}

fn params_array(request: &RpcRequest) -> std::result::Result<Vec<Value>, RpcError> {
    match request.params.as_ref() {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(_) => Err(RpcError::invalid_params("params must be an array")),
    }
}

fn success_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id,
    })
}

fn error_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message.into(),
        },
        "id": id,
    })
}

fn json_rpc_response_profiled(value: Value, profile: Option<&ProfileHandle>) -> Result<Response> {
    cached_json_response_profiled(&value, ERROR_CACHE_CONTROL, profile)
}

fn json_bytes_response_profiled(
    bytes: Vec<u8>,
    cache_control: &str,
    profile: Option<&ProfileHandle>,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Type", "application/json")?;
    add_profile_headers(&headers, profile)?;
    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}

fn json_stream_response_profiled(
    body: mpsc::Receiver<std::result::Result<Vec<u8>, worker::Error>>,
    cache_control: &str,
    profile: Option<&ProfileHandle>,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Type", "application/json")?;
    add_profile_headers(&headers, profile)?;
    Ok(Response::from_stream(body)?.with_headers(headers))
}

#[derive(Debug, Clone, Copy)]
enum TransactionDetails {
    Full,
    Signatures,
    None,
    Accounts,
}

#[derive(Debug, Clone, Copy)]
struct GetBlockConfig {
    transaction_details: TransactionDetails,
    rewards: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccessFetchMode {
    None,
    BlockhashOnly,
    Signatures,
    Full,
}

fn access_fetch_mode(config: GetBlockConfig) -> AccessFetchMode {
    if config.rewards {
        return AccessFetchMode::Full;
    }
    match config.transaction_details {
        TransactionDetails::None => AccessFetchMode::BlockhashOnly,
        TransactionDetails::Signatures => AccessFetchMode::Signatures,
        TransactionDetails::Full | TransactionDetails::Accounts => AccessFetchMode::Full,
    }
}

fn parse_get_block_config(value: Option<&Value>) -> std::result::Result<GetBlockConfig, String> {
    let mut config = GetBlockConfig {
        transaction_details: TransactionDetails::Full,
        rewards: true,
    };

    match value {
        None => {}
        Some(Value::String(encoding)) if encoding == "json" => {}
        Some(Value::String(encoding)) if encoding == "jsonParsed" => {
            return Err("The jsonParsed encoding is not supported by this archive RPC".to_string());
        }
        Some(Value::String(encoding)) => {
            return Err(format!("unsupported transaction encoding {encoding}"));
        }
        Some(Value::Object(map)) => {
            if let Some(encoding) = map.get("encoding").and_then(Value::as_str) {
                match encoding {
                    "json" => {}
                    "jsonParsed" => {
                        return Err(
                            "The jsonParsed encoding is not supported by this archive RPC"
                                .to_string(),
                        );
                    }
                    _ => return Err(format!("unsupported transaction encoding {encoding}")),
                }
            }
            if let Some(details) = map.get("transactionDetails").and_then(Value::as_str) {
                config.transaction_details = match details {
                    "full" => TransactionDetails::Full,
                    "signatures" => TransactionDetails::Signatures,
                    "none" => TransactionDetails::None,
                    "accounts" => TransactionDetails::Accounts,
                    _ => return Err(format!("unsupported transactionDetails {details}")),
                };
            }
            if let Some(rewards) = map.get("rewards") {
                config.rewards = rewards
                    .as_bool()
                    .ok_or_else(|| "rewards must be a boolean".to_string())?;
            }
        }
        Some(_) => return Err("getBlock config must be a string or object".to_string()),
    }

    Ok(config)
}

fn contains_json_parsed(value: &Value) -> bool {
    match value {
        Value::String(value) => value == "jsonParsed",
        Value::Array(items) => items.iter().any(contains_json_parsed),
        Value::Object(map) => map.values().any(contains_json_parsed),
        _ => false,
    }
}

async fn handle_probe_first(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    handle_probe(req, ctx, None).await
}

async fn handle_probe_slot(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let Some(slot) = ctx.param("slot") else {
        return json_error(400, "invalid_slot", "missing slot");
    };
    let slot = match slot.parse::<u64>() {
        Ok(slot) => slot,
        Err(_) => return json_error(400, "invalid_slot", "slot must be an unsigned integer"),
    };
    handle_probe(req, ctx, Some(slot)).await
}

async fn handle_probe(req: Request, ctx: RouteContext<()>, slot: Option<u64>) -> Result<Response> {
    let profile = request_profile(&req);
    let probe_started = now_ms();
    let source = match WorkerSource::from_env(&ctx.env, profile.clone()) {
        Ok(source) => source,
        Err(err) => return json_error_profiled(500, "config_error", &err, profile.as_ref()),
    };
    let epoch = query_epoch(&req).unwrap_or(10);
    let selected_slot = slot.unwrap_or(epoch * SLOTS_PER_EPOCH);
    match fetch_block(&source, selected_slot, AccessFetchMode::None).await {
        Ok(Some(block)) => {
            source.record_elapsed("probe_total", probe_started, 0, "ok", None);
            cached_json_response_profiled(
                &json!({
                    "source": {
                        "epoch": block.epoch,
                        "prefix": source.archive_prefix,
                    },
                    "indexRow": {
                        "slotOffset": block.block.header.slot % SLOTS_PER_EPOCH,
                        "blockOffset": block.index_row.block_offset,
                        "blockLen": block.index_row.block_len,
                        "accessOffset": block.index_row.access_offset,
                        "accessLen": block.index_row.access_len,
                    },
                    "block": {
                        "slot": block.block.header.slot,
                        "parentSlot": block.block.header.parent_slot,
                        "blockTime": block.block.header.block_time,
                        "blockHeight": block.block.header.block_height,
                        "txCount": block.block.tx_count,
                        "decodedTxRows": block.block.tx_rows.len(),
                        "messageBytes": block.block.message_bytes.len(),
                        "metadataBytes": block.block.metadata_bytes.len(),
                    }
                }),
                ERROR_CACHE_CONTROL,
                profile.as_ref(),
            )
        }
        Ok(None) => {
            source.record_elapsed("probe_total", probe_started, 0, "missing", None);
            json_error_profiled(
                404,
                "missing_slot",
                "slot not found in Blockzilla archive",
                profile.as_ref(),
            )
        }
        Err(err) => {
            console_error!("probe failed: {}", err);
            source.record_elapsed("probe_total", probe_started, 0, "error", None);
            json_error_profiled(502, "probe_error", &err, profile.as_ref())
        }
    }
}

fn query_epoch(req: &Request) -> Option<u64> {
    let url = req.url().ok()?;
    url.query_pairs()
        .find(|(key, _)| key == "epoch")
        .and_then(|(_, value)| value.parse().ok())
}

async fn render_route_block_bytes(
    source: &WorkerSource,
    request: BlockRequest,
    mode: RouteBlockMode,
) -> std::result::Result<Option<Vec<u8>>, String> {
    let Some(block) = fetch_block(source, request.slot, AccessFetchMode::Full).await? else {
        return Ok(None);
    };
    let config = GetBlockConfig {
        transaction_details: TransactionDetails::Full,
        rewards: request.include_rewards,
    };
    match mode {
        RouteBlockMode::Full => stream_get_block_full_value(source, &block, config)
            .await
            .map(Some),
        RouteBlockMode::Lite => stream_get_block_lite_value(source, &block, config)
            .await
            .map(Some),
    }
}

struct FetchedHotBlock {
    epoch: u64,
    index_row: ArchiveV2GetBlockIndexRow,
    row: ArchiveV2HotBlockIndexRow,
    block_bytes: Vec<u8>,
    block: ArchiveV2HotBlockBlob,
    access: Option<FetchedAccess>,
}

struct FetchedAccessLite {
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyBlockAccessBlobV1 {
    version: u16,
    flags: u32,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

impl From<LegacyBlockAccessBlobV1> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV1) -> Self {
        Self {
            version: value.version,
            flags: value.flags,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyCompactMetaV1 {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    logs: Option<CompactLogStream>,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<CompactReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

impl TryFrom<LegacyCompactMetaV1> for CompactMetaV1 {
    type Error = String;

    fn try_from(value: LegacyCompactMetaV1) -> std::result::Result<Self, Self::Error> {
        let err = value
            .err
            .as_deref()
            .map(StoredTransactionError::from_stored_wincode_bytes)
            .transpose()
            .map_err(|err| err.to_string())?;
        Ok(Self {
            err,
            fee: value.fee,
            pre_balances: value.pre_balances,
            post_balances: value.post_balances,
            inner_instructions: value.inner_instructions,
            logs: value.logs,
            pre_token_balances: value.pre_token_balances,
            post_token_balances: value.post_token_balances,
            rewards: value.rewards,
            loaded_writable_addresses: value.loaded_writable_addresses,
            loaded_readonly_addresses: value.loaded_readonly_addresses,
            return_data: value.return_data,
            compute_units_consumed: value.compute_units_consumed,
            cost_units: value.cost_units,
        })
    }
}

enum FetchedAccess {
    Full(ArchiveV2BlockAccessBlob),
    Lite(FetchedAccessLite),
}

impl FetchedAccess {
    fn full(&self) -> Option<&ArchiveV2BlockAccessBlob> {
        match self {
            Self::Full(access) => Some(access),
            Self::Lite(_) => None,
        }
    }

    fn blockhash(&self) -> &[u8; 32] {
        match self {
            Self::Full(access) => &access.blockhash,
            Self::Lite(access) => &access.blockhash,
        }
    }

    fn previous_blockhash(&self) -> &[u8; 32] {
        match self {
            Self::Full(access) => &access.previous_blockhash,
            Self::Lite(access) => &access.previous_blockhash,
        }
    }

    fn signatures(&self) -> &[u8] {
        match self {
            Self::Full(access) => &access.signatures,
            Self::Lite(access) => &access.signatures,
        }
    }
}

async fn fetch_block(
    source: &WorkerSource,
    slot: u64,
    access_mode: AccessFetchMode,
) -> std::result::Result<Option<FetchedHotBlock>, String> {
    let total_started = now_ms();
    let epoch = epoch_for_slot(slot);
    let Some(index_row) = read_get_block_index_row(source, epoch, slot).await? else {
        source.record_elapsed("fetch_block_total", total_started, 0, "missing", None);
        return Ok(None);
    };
    fetch_block_from_get_block_index(source, total_started, epoch, slot, index_row, access_mode)
        .await
}

async fn fetch_block_bundle_bytes(
    source: &WorkerSource,
    slot: u64,
    access_mode: BlockBinAccessMode,
) -> std::result::Result<Option<Vec<u8>>, String> {
    let total_started = now_ms();
    let epoch = epoch_for_slot(slot);
    let Some(index_row) = read_get_block_index_row_with_access(
        source,
        epoch,
        slot,
        access_mode == BlockBinAccessMode::Include,
    )
    .await?
    else {
        source.record_elapsed("fetch_block_bin_total", total_started, 0, "missing", None);
        return Ok(None);
    };
    let blocks_path = epoch_file(&source.archive_prefix, epoch, ARCHIVE_V2_BLOCKS_FILE);
    let hot_block = fetch_compressed_block(source, &blocks_path, &index_row).await?;
    let block_access = if access_mode == BlockBinAccessMode::Include {
        let access_path = epoch_file(&source.archive_prefix, epoch, ARCHIVE_V2_BLOCK_ACCESS_FILE);
        let started = now_ms();
        let bytes = source
            .get_range(
                &access_path,
                index_row.access_offset,
                index_row.access_len as usize,
            )
            .await?;
        source.record_elapsed(
            "access_fetch",
            started,
            bytes.len() as u64,
            "block_access_bundle",
            None,
        );
        Some(bytes)
    } else {
        None
    };
    let started = now_ms();
    let bundle = BlockzillaGetBlockBundleV1 {
        version: WINCODE_BLOCKZILLA_GET_BLOCK_BUNDLE_VERSION,
        slot,
        hot_block_encoding: BlockzillaGetBlockBlobEncoding::ZstdWincode,
        hot_block,
        block_access_encoding: BlockzillaGetBlockBlobEncoding::Wincode,
        block_access,
    };
    let bytes = wincode::config::serialize(&bundle, wincode_leb128_config())
        .map_err(|err| format!("encode blockzilla getBlock bundle for slot {slot}: {err}"))?;
    source.record_elapsed(
        "bundle_encode",
        started,
        bytes.len() as u64,
        access_mode.cache_key_suffix(),
        None,
    );
    source.record_elapsed(
        "fetch_block_bin_total",
        total_started,
        bytes.len() as u64,
        access_mode.cache_key_suffix(),
        None,
    );
    Ok(Some(bytes))
}

async fn read_get_block_index_row(
    source: &WorkerSource,
    epoch: u64,
    slot: u64,
) -> std::result::Result<Option<ArchiveV2GetBlockIndexRow>, String> {
    read_get_block_index_row_with_access(source, epoch, slot, true).await
}

async fn read_get_block_index_row_with_access(
    source: &WorkerSource,
    epoch: u64,
    slot: u64,
    require_access: bool,
) -> std::result::Result<Option<ArchiveV2GetBlockIndexRow>, String> {
    let index_path = epoch_file(
        &source.archive_prefix,
        epoch,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    );
    let slot_index = slot % SLOTS_PER_EPOCH;
    let offset = slot_index
        .checked_mul(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN as u64)
        .ok_or_else(|| "get-block index row offset overflow".to_string())?;
    let started = now_ms();
    let bytes = source
        .get_range(&index_path, offset, ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN)
        .await?;
    let row = ArchiveV2GetBlockIndexRow {
        block_offset: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        block_len: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        access_offset: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        access_len: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
    };
    if row.block_len == 0 || (require_access && row.access_len == 0) {
        source.record_elapsed("get_block_index", started, 0, "missing_slot", None);
        Ok(None)
    } else {
        source.record_elapsed("get_block_index", started, 0, "found", None);
        Ok(Some(row))
    }
}

async fn fetch_block_from_get_block_index(
    source: &WorkerSource,
    total_started: f64,
    epoch: u64,
    slot: u64,
    index_row: ArchiveV2GetBlockIndexRow,
    access_mode: AccessFetchMode,
) -> std::result::Result<Option<FetchedHotBlock>, String> {
    let blocks_path = epoch_file(&source.archive_prefix, epoch, ARCHIVE_V2_BLOCKS_FILE);
    let access_path = (access_mode != AccessFetchMode::None)
        .then(|| epoch_file(&source.archive_prefix, epoch, ARCHIVE_V2_BLOCK_ACCESS_FILE));
    let block_fetch = async { fetch_compressed_block(source, &blocks_path, &index_row).await };
    let access_fetch = async {
        let Some(access_path) = access_path.as_ref() else {
            return Ok::<Option<Vec<u8>>, String>(None);
        };
        let started = now_ms();
        let bytes = source
            .get_range(
                access_path,
                index_row.access_offset,
                index_row.access_len as usize,
            )
            .await?;
        source.record_elapsed(
            "access_fetch",
            started,
            bytes.len() as u64,
            "block_access",
            None,
        );
        Ok(Some(bytes))
    };
    let (compressed, access_bytes) = future::try_join(block_fetch, access_fetch).await?;

    let block_bytes = decode_zstd_block_bytes(source, slot, &compressed)?;
    let started = now_ms();
    let block: ArchiveV2HotBlockBlob = deserialize_archive_v2_hot_block_blob(&block_bytes)
        .map_err(|err| format!("decode Blockzilla hot block for slot {slot}: {err}"))?;
    source.record_elapsed(
        "wincode_decode",
        started,
        block_bytes.len() as u64,
        "hot_block",
        None,
    );
    if block.header.slot != slot {
        return Err(format!(
            "decoded slot {} but request was {}",
            block.header.slot, slot
        ));
    }
    let tx_count = u32::try_from(block.tx_rows.len())
        .map_err(|_| format!("slot {slot} transaction count exceeds u32::MAX"))?;
    let signature_count = block
        .tx_rows
        .iter()
        .try_fold(0u32, |acc, row| {
            acc.checked_add(u32::from(row.signature_count))
        })
        .ok_or_else(|| format!("slot {slot} signature count overflow"))?;
    let uncompressed_len = u32::try_from(block_bytes.len())
        .map_err(|_| format!("slot {slot} decoded block exceeds u32::MAX"))?;
    let row = ArchiveV2HotBlockIndexRow {
        block_id: u32::MAX,
        slot,
        compressed_offset: index_row.block_offset,
        compressed_len: index_row.block_len,
        uncompressed_len,
        tx_count,
        first_tx_ordinal: 0,
        first_signature_ordinal: 0,
        signature_count,
    };
    let access = if let Some(bytes) = access_bytes {
        decode_block_access_bytes(source, &row, &bytes, access_mode)?
    } else {
        None
    };
    source.record_elapsed(
        "fetch_block_total",
        total_started,
        block_bytes.len() as u64,
        "ok_direct",
        None,
    );
    Ok(Some(FetchedHotBlock {
        epoch,
        index_row,
        row,
        block_bytes,
        block,
        access,
    }))
}

async fn fetch_compressed_block(
    source: &WorkerSource,
    blocks_path: &str,
    index_row: &ArchiveV2GetBlockIndexRow,
) -> std::result::Result<Vec<u8>, String> {
    let started = now_ms();
    let compressed = source
        .get_range(
            blocks_path,
            index_row.block_offset,
            index_row.block_len as usize,
        )
        .await?;
    source.record_elapsed(
        "block_fetch",
        started,
        compressed.len() as u64,
        "zstd_block",
        None,
    );
    Ok(compressed)
}

fn decode_zstd_block_bytes(
    source: &WorkerSource,
    slot: u64,
    compressed: &[u8],
) -> std::result::Result<Vec<u8>, String> {
    let started = now_ms();
    let mut decoder = StreamingDecoder::new(compressed)
        .map_err(|err| format!("zstd-decode init slot {slot}: {err}"))?;
    let content_size = usize::try_from(decoder.decoder.content_size()).unwrap_or(0);
    let mut block_bytes = Vec::with_capacity(content_size);
    decoder
        .read_to_end(&mut block_bytes)
        .map_err(|err| format!("zstd-decode read slot {slot}: {err}"))?;
    source.record_elapsed(
        "zstd_decode",
        started,
        block_bytes.len() as u64,
        "hot_block",
        None,
    );
    Ok(block_bytes)
}

fn decode_block_access_bytes(
    source: &WorkerSource,
    hot_row: &ArchiveV2HotBlockIndexRow,
    bytes: &[u8],
    mode: AccessFetchMode,
) -> std::result::Result<Option<FetchedAccess>, String> {
    let started = now_ms();
    let access = match mode {
        AccessFetchMode::Full => {
            let access = decode_full_access_blob(bytes, hot_row.slot)?;
            validate_access_blob(&access, hot_row)?;
            FetchedAccess::Full(access)
        }
        AccessFetchMode::BlockhashOnly | AccessFetchMode::Signatures => {
            let access = decode_lite_access_blob(
                bytes,
                matches!(mode, AccessFetchMode::Signatures),
                hot_row.slot,
            )?;
            if mode == AccessFetchMode::Signatures {
                validate_lite_access_blob(&access, hot_row)?;
            }
            FetchedAccess::Lite(access)
        }
        AccessFetchMode::None => return Ok(None),
    };
    source.record_elapsed(
        "access_decode",
        started,
        bytes.len() as u64,
        "block_access",
        None,
    );
    Ok(Some(access))
}

fn decode_full_access_blob(
    bytes: &[u8],
    slot: u64,
) -> std::result::Result<ArchiveV2BlockAccessBlob, String> {
    match wincode::config::deserialize::<ArchiveV2BlockAccessBlob, _>(
        bytes,
        wincode_leb128_config(),
    ) {
        Ok(access) => Ok(access),
        Err(primary_err) => {
            let legacy: LegacyBlockAccessBlobV1 =
                wincode::config::deserialize(bytes, wincode_leb128_config()).map_err(
                    |legacy_err| {
                        format!(
                            "decode Blockzilla block-access sidecar for slot {slot}: {primary_err}; legacy v1 decode: {legacy_err}"
                        )
                    },
                )?;
            if legacy.version != 1 {
                return Err(format!(
                    "slot {slot} block-access version {} is unsupported",
                    legacy.version
                ));
            }
            Ok(legacy.into())
        }
    }
}

fn decode_lite_access_blob(
    bytes: &[u8],
    include_signatures: bool,
    slot: u64,
) -> std::result::Result<FetchedAccessLite, String> {
    let mut reader = bytes;
    let version: u16 = read_wincode_value(&mut reader)
        .map_err(|err| format!("slot {slot} read block-access version: {err}"))?;
    if version != WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && version != 1 {
        return Err(format!(
            "slot {slot} block-access version {version} is unsupported"
        ));
    }
    let _: u32 = read_wincode_value(&mut reader)
        .map_err(|err| format!("slot {slot} read block-access flags: {err}"))?;
    let blockhash = reader
        .take_array::<32>()
        .map_err(|err| format!("slot {slot} read block-access blockhash: {err}"))?;
    let previous_blockhash = reader
        .take_array::<32>()
        .map_err(|err| format!("slot {slot} read block-access previous blockhash: {err}"))?;
    if !include_signatures {
        return Ok(FetchedAccessLite {
            blockhash,
            previous_blockhash,
            signature_counts: Vec::new(),
            signatures: Vec::new(),
        });
    }
    let signature_counts: Vec<u8> = read_wincode_value(&mut reader)
        .map_err(|err| format!("slot {slot} read block-access signature counts: {err}"))?;
    let signatures: Vec<u8> = read_wincode_value(&mut reader)
        .map_err(|err| format!("slot {slot} read block-access signatures: {err}"))?;
    Ok(FetchedAccessLite {
        blockhash,
        previous_blockhash,
        signature_counts,
        signatures,
    })
}

fn validate_lite_access_blob(
    access: &FetchedAccessLite,
    hot_row: &ArchiveV2HotBlockIndexRow,
) -> std::result::Result<(), String> {
    if access.signature_counts.len() != hot_row.tx_count as usize {
        return Err(format!(
            "slot {} block-access has {} signature counts, expected {}",
            hot_row.slot,
            access.signature_counts.len(),
            hot_row.tx_count
        ));
    }
    let signature_count: usize = access
        .signature_counts
        .iter()
        .map(|count| usize::from(*count))
        .sum();
    if signature_count != hot_row.signature_count as usize {
        return Err(format!(
            "slot {} block-access has {} signatures by tx counts, expected {}",
            hot_row.slot, signature_count, hot_row.signature_count
        ));
    }
    let expected_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES as usize)
        .ok_or_else(|| format!("slot {} signature byte count overflow", hot_row.slot))?;
    if access.signatures.len() != expected_bytes {
        return Err(format!(
            "slot {} block-access signature bytes {}, expected {}",
            hot_row.slot,
            access.signatures.len(),
            expected_bytes
        ));
    }
    Ok(())
}

fn validate_access_blob(
    access: &ArchiveV2BlockAccessBlob,
    hot_row: &ArchiveV2HotBlockIndexRow,
) -> std::result::Result<(), String> {
    if access.version != WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && access.version != 1 {
        return Err(format!(
            "slot {} block-access version {} is unsupported",
            hot_row.slot, access.version
        ));
    }
    if access.signature_counts.len() != hot_row.tx_count as usize {
        return Err(format!(
            "slot {} block-access has {} signature counts, expected {}",
            hot_row.slot,
            access.signature_counts.len(),
            hot_row.tx_count
        ));
    }
    let signature_count: usize = access
        .signature_counts
        .iter()
        .map(|count| usize::from(*count))
        .sum();
    if signature_count != hot_row.signature_count as usize {
        return Err(format!(
            "slot {} block-access has {} signatures by tx counts, expected {}",
            hot_row.slot, signature_count, hot_row.signature_count
        ));
    }
    let expected_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES as usize)
        .ok_or_else(|| "block-access signature length overflow".to_string())?;
    if access.signatures.len() != expected_bytes {
        return Err(format!(
            "slot {} block-access signature bytes {}, expected {}",
            hot_row.slot,
            access.signatures.len(),
            expected_bytes
        ));
    }
    Ok(())
}

async fn render_block_value(
    source: &WorkerSource,
    fetched: &FetchedHotBlock,
    config: GetBlockConfig,
    route_mode: Option<RouteBlockMode>,
) -> std::result::Result<Value, String> {
    let render_started = now_ms();
    let mut resolver = Resolver::new(
        source,
        fetched.access.as_ref().and_then(FetchedAccess::full),
    );
    let blockhash = match fetched.access.as_ref() {
        Some(access) => base58_32(access.blockhash()),
        None => {
            resolver
                .resolve_blockhash_id(fetched.block.header.blockhash_id as i32)
                .await?
        }
    };
    let previous_blockhash = match fetched.access.as_ref() {
        Some(access) => base58_32(access.previous_blockhash()),
        None => {
            resolver
                .resolve_blockhash_id(fetched.block.header.previous_blockhash_id as i32)
                .await?
        }
    };

    let mut out = Map::new();
    out.insert("blockhash".to_string(), Value::String(blockhash));
    out.insert(
        "previousBlockhash".to_string(),
        Value::String(previous_blockhash),
    );
    out.insert(
        "parentSlot".to_string(),
        Value::from(fetched.block.header.parent_slot),
    );
    out.insert(
        "blockTime".to_string(),
        rpc_block_time(fetched.block.header.block_time)
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    out.insert(
        "blockHeight".to_string(),
        fetched
            .block
            .header
            .block_height
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    if config.rewards {
        out.insert(
            "rewards".to_string(),
            rewards_value(&mut resolver, fetched.block.header.rewards.as_ref()).await?,
        );
    }

    match route_mode {
        Some(RouteBlockMode::Lite) => {
            out.insert(
                "transactions".to_string(),
                transactions_value(&mut resolver, fetched, true, false).await?,
            );
        }
        None | Some(RouteBlockMode::Full) => match config.transaction_details {
            TransactionDetails::Full => {
                out.insert(
                    "transactions".to_string(),
                    transactions_value(&mut resolver, fetched, false, true).await?,
                );
            }
            TransactionDetails::Accounts => {
                out.insert(
                    "transactions".to_string(),
                    account_transactions_value(&mut resolver, fetched, config.rewards).await?,
                );
            }
            TransactionDetails::Signatures => {
                out.insert(
                    "signatures".to_string(),
                    signatures_value(&resolver, fetched).await?,
                );
            }
            TransactionDetails::None => {}
        },
    }
    source.record_elapsed("render_total", render_started, 0, "json_value", None);
    Ok(Value::Object(out))
}

fn stream_json_rpc_success(id: &Value, result: Vec<u8>) -> std::result::Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(result.len().saturating_add(96));
    out.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"result\":");
    out.extend_from_slice(&result);
    out.extend_from_slice(b",\"id\":");
    serde_json::to_writer(&mut out, id).map_err(|err| format!("encode JSON-RPC id: {err}"))?;
    out.push(b'}');
    Ok(out)
}

fn stream_json_rpc_success_prefix(w: &mut JsonByteWriter) {
    w.raw(b"{\"jsonrpc\":\"2.0\",\"result\":");
}

fn stream_json_rpc_success_suffix(
    w: &mut JsonByteWriter,
    id: &Value,
) -> std::result::Result<(), String> {
    w.raw(b",\"id\":");
    w.json(id)
        .map_err(|err| format!("encode JSON-RPC id: {err}"))?;
    w.raw(b"}");
    Ok(())
}

fn stream_json_rpc_get_block_success_body(
    source: WorkerSource,
    id: Value,
    fetched: FetchedHotBlock,
    config: GetBlockConfig,
) -> mpsc::Receiver<std::result::Result<Vec<u8>, worker::Error>> {
    let (sender, receiver) = mpsc::channel(4);
    let mut error_sender = sender.clone();
    wasm_bindgen_futures::spawn_local(async move {
        let started = now_ms();
        let mut w = JsonByteWriter::channel(JSON_STREAM_CHUNK_BYTES, sender);
        let result = async {
            stream_json_rpc_success_prefix(&mut w);
            stream_get_block_value_into(&source, &fetched, config, &mut w).await?;
            stream_json_rpc_success_suffix(&mut w, &id)?;
            Ok::<(), String>(())
        }
        .await;
        let bytes = w.len() as u64;
        match result {
            Ok(()) => {
                if let Err(err) = w.flush_chunk_async().await {
                    console_error!("failed to finish getBlock response stream: {err}");
                }
                source.record_elapsed(
                    "render_streamed_get_block",
                    started,
                    bytes,
                    "stream_body",
                    None,
                );
            }
            Err(err) => {
                let message = format!("failed to stream getBlock response: {err}");
                console_error!("{message}");
                if w.sent_bytes() == 0 {
                    let fallback = serde_json::to_vec(&error_response(id, -32000, message.clone()))
                        .map_err(|err| worker::Error::RustError(err.to_string()));
                    match fallback {
                        Ok(bytes) => {
                            let _ = error_sender.send(Ok(bytes)).await;
                        }
                        Err(err) => {
                            let _ = error_sender.send(Err(err)).await;
                        }
                    }
                } else {
                    let _ = error_sender
                        .send(Err(worker::Error::RustError(message)))
                        .await;
                }
                source.record_elapsed(
                    "render_streamed_get_block",
                    started,
                    bytes,
                    "stream_body_error",
                    None,
                );
            }
        }
    });
    receiver
}

async fn stream_resolved_pubkey_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    key: CompactPubkey,
) -> std::result::Result<(), String> {
    let bytes = resolver.resolve_pubkey_bytes(key).await?;
    w.base58_32(&bytes);
    Ok(())
}

async fn stream_resolved_recent_blockhash_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    value: &OwnedCompactRecentBlockhash,
) -> std::result::Result<(), String> {
    let bytes = resolver.resolve_recent_blockhash_bytes(value).await?;
    w.base58_32(&bytes);
    Ok(())
}

fn stream_block_time_value(source: &WorkerSource, block_time: Option<i64>) -> Vec<u8> {
    let started = now_ms();
    let mut w = JsonByteWriter::with_capacity(20);
    w.option_i64(rpc_block_time(block_time));
    let out = w.into_inner();
    source.record_elapsed(
        "render_streamed_block_time",
        started,
        out.len() as u64,
        "block_time",
        None,
    );
    out
}

async fn stream_get_block_value_into(
    source: &WorkerSource,
    fetched: &FetchedHotBlock,
    config: GetBlockConfig,
    w: &mut JsonByteWriter,
) -> std::result::Result<(), String> {
    match config.transaction_details {
        TransactionDetails::Full => {
            return stream_get_block_full_value_into(source, fetched, config, w).await;
        }
        TransactionDetails::Accounts => {
            return stream_get_block_accounts_value_into(source, fetched, config, w).await;
        }
        TransactionDetails::None | TransactionDetails::Signatures => {}
    }

    let started = now_ms();
    let start_len = w.len();
    let access = fetched
        .access
        .as_ref()
        .ok_or_else(|| "streamed getBlock path requires block access".to_string())?;
    w.raw(b"{\"blockhash\":");
    w.base58_32(access.blockhash());
    w.raw(b",\"previousBlockhash\":");
    w.base58_32(access.previous_blockhash());
    w.raw(b",\"parentSlot\":");
    w.u64(fetched.block.header.parent_slot);
    w.raw(b",\"blockTime\":");
    w.option_i64(rpc_block_time(fetched.block.header.block_time));
    w.raw(b",\"blockHeight\":");
    w.option_u64(fetched.block.header.block_height);
    if config.rewards {
        let mut resolver = Resolver::new(
            source,
            fetched.access.as_ref().and_then(FetchedAccess::full),
        );
        w.raw(b",\"rewards\":");
        stream_rewards_value(w, &mut resolver, fetched.block.header.rewards.as_ref()).await?;
    }

    if matches!(config.transaction_details, TransactionDetails::Signatures) {
        w.raw(b",\"signatures\":[");
        let mut signature_index = 0usize;
        let mut first = true;
        for row in &fetched.block.tx_rows {
            if row.signature_count > 0 {
                let start = signature_index
                    .checked_mul(SIGNATURE_BYTES as usize)
                    .ok_or_else(|| "signature byte offset overflow".to_string())?;
                let end = start
                    .checked_add(SIGNATURE_BYTES as usize)
                    .ok_or_else(|| "signature byte end overflow".to_string())?;
                let signature = access.signatures().get(start..end).ok_or_else(|| {
                    "signature sidecar ended before first transaction signature".to_string()
                })?;
                if !first {
                    w.raw(b",");
                }
                first = false;
                w.base58_64(signature.try_into().expect("checked signature len"));
                if signature_index & 0x3ff == 0x3ff {
                    w.flush_chunk_async().await?;
                }
            }
            signature_index = signature_index
                .checked_add(usize::from(row.signature_count))
                .ok_or_else(|| "signature index overflow".to_string())?;
        }
        w.raw(b"]");
    }

    w.raw(b"}");
    let detail = match config.transaction_details {
        TransactionDetails::Signatures => "signatures",
        TransactionDetails::None => "none",
        TransactionDetails::Full | TransactionDetails::Accounts => unreachable!("handled above"),
    };
    source.record_elapsed(
        "render_streamed_get_block",
        started,
        w.len().saturating_sub(start_len) as u64,
        detail,
        None,
    );
    Ok(())
}

async fn stream_get_block_full_value(
    source: &WorkerSource,
    fetched: &FetchedHotBlock,
    config: GetBlockConfig,
) -> std::result::Result<Vec<u8>, String> {
    let full_access = fetched
        .access
        .as_ref()
        .and_then(FetchedAccess::full)
        .ok_or_else(|| "streamed full getBlock path requires full block access".to_string())?;
    let estimated = full_json_capacity_hint(fetched, full_access);
    let mut w = JsonByteWriter::with_capacity(estimated);
    stream_get_block_full_value_into(source, fetched, config, &mut w).await?;
    Ok(w.into_inner())
}

async fn stream_get_block_full_value_into(
    source: &WorkerSource,
    fetched: &FetchedHotBlock,
    config: GetBlockConfig,
    w: &mut JsonByteWriter,
) -> std::result::Result<(), String> {
    let started = now_ms();
    let start_len = w.len();
    let full_access = fetched
        .access
        .as_ref()
        .and_then(FetchedAccess::full)
        .ok_or_else(|| "streamed full getBlock path requires full block access".to_string())?;
    let access = fetched
        .access
        .as_ref()
        .ok_or_else(|| "streamed full getBlock path requires block access".to_string())?;
    let mut resolver = Resolver::new(source, Some(full_access));

    write_block_header_fields(w, fetched, access);
    if config.rewards {
        w.raw(b",\"rewards\":");
        stream_rewards_value(w, &mut resolver, fetched.block.header.rewards.as_ref()).await?;
    }
    w.raw(b",\"transactions\":[");

    let mut signature_index = 0usize;
    for (index, row) in fetched.block.tx_rows.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_full_transaction_entry(
            w,
            &mut resolver,
            fetched,
            row,
            &mut signature_index,
            config.rewards,
        )
        .await?;
        if index & 0x3f == 0x3f {
            w.flush_chunk_async().await?;
        }
    }

    w.raw(b"]}");
    let detail = format!(
        "full capacity={} slack={}",
        w.capacity(),
        w.capacity()
            .saturating_sub(w.len().saturating_sub(start_len))
    );
    source.record_elapsed(
        "render_streamed_get_block",
        started,
        w.len().saturating_sub(start_len) as u64,
        detail,
        None,
    );
    Ok(())
}

async fn stream_get_block_accounts_value_into(
    source: &WorkerSource,
    fetched: &FetchedHotBlock,
    config: GetBlockConfig,
    w: &mut JsonByteWriter,
) -> std::result::Result<(), String> {
    let started = now_ms();
    let start_len = w.len();
    let full_access = fetched
        .access
        .as_ref()
        .and_then(FetchedAccess::full)
        .ok_or_else(|| "streamed accounts getBlock path requires full block access".to_string())?;
    let access = fetched
        .access
        .as_ref()
        .ok_or_else(|| "streamed accounts getBlock path requires block access".to_string())?;
    let mut resolver = Resolver::new(source, Some(full_access));

    write_block_header_fields(w, fetched, access);
    if config.rewards {
        w.raw(b",\"rewards\":");
        stream_rewards_value(w, &mut resolver, fetched.block.header.rewards.as_ref()).await?;
    }
    w.raw(b",\"transactions\":[");

    let mut signature_index = 0usize;
    for (index, row) in fetched.block.tx_rows.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_accounts_transaction_entry(
            w,
            &mut resolver,
            fetched,
            row,
            &mut signature_index,
            config.rewards,
        )
        .await?;
        if index & 0x3f == 0x3f {
            w.flush_chunk_async().await?;
        }
    }

    w.raw(b"]}");
    let detail = format!(
        "accounts capacity={} slack={}",
        w.capacity(),
        w.capacity()
            .saturating_sub(w.len().saturating_sub(start_len))
    );
    source.record_elapsed(
        "render_streamed_get_block",
        started,
        w.len().saturating_sub(start_len) as u64,
        detail,
        None,
    );
    Ok(())
}

fn full_json_capacity_hint(
    fetched: &FetchedHotBlock,
    full_access: &ArchiveV2BlockAccessBlob,
) -> usize {
    fetched
        .block_bytes
        .len()
        .saturating_mul(7)
        .saturating_add(full_access.signatures.len().saturating_mul(2))
        .saturating_add(fetched.block.tx_rows.len().saturating_mul(256))
        .saturating_add(4096)
}

async fn stream_get_block_lite_value(
    source: &WorkerSource,
    fetched: &FetchedHotBlock,
    config: GetBlockConfig,
) -> std::result::Result<Vec<u8>, String> {
    let started = now_ms();
    let full_access = fetched
        .access
        .as_ref()
        .and_then(FetchedAccess::full)
        .ok_or_else(|| "streamed lite block route requires full block access".to_string())?;
    let access = fetched
        .access
        .as_ref()
        .ok_or_else(|| "streamed lite block route requires block access".to_string())?;
    let estimated = fetched
        .block_bytes
        .len()
        .saturating_add(full_access.signatures.len().saturating_mul(2))
        .saturating_add(4096);
    let mut w = JsonByteWriter::with_capacity(estimated);
    let mut resolver = Resolver::new(source, Some(full_access));

    write_block_header_fields(&mut w, fetched, access);
    if config.rewards {
        w.raw(b",\"rewards\":");
        stream_rewards_value(&mut w, &mut resolver, fetched.block.header.rewards.as_ref()).await?;
    }
    w.raw(b",\"transactions\":[");

    let mut signature_index = 0usize;
    for (index, row) in fetched.block.tx_rows.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_lite_transaction_entry(&mut w, &mut resolver, fetched, row, &mut signature_index)
            .await?;
    }

    w.raw(b"]}");
    let out = w.into_inner();
    source.record_elapsed(
        "render_streamed_route_block",
        started,
        out.len() as u64,
        "lite",
        None,
    );
    Ok(out)
}

fn write_block_header_fields(
    w: &mut JsonByteWriter,
    fetched: &FetchedHotBlock,
    access: &FetchedAccess,
) {
    w.raw(b"{\"blockhash\":");
    w.base58_32(access.blockhash());
    w.raw(b",\"previousBlockhash\":");
    w.base58_32(access.previous_blockhash());
    w.raw(b",\"parentSlot\":");
    w.u64(fetched.block.header.parent_slot);
    w.raw(b",\"blockTime\":");
    w.option_i64(rpc_block_time(fetched.block.header.block_time));
    w.raw(b",\"blockHeight\":");
    w.option_u64(fetched.block.header.block_height);
}

async fn stream_full_transaction_entry(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    fetched: &FetchedHotBlock,
    row: &ArchiveV2HotTxRow,
    signature_index: &mut usize,
    include_rewards: bool,
) -> std::result::Result<(), String> {
    let message = decode_message(&fetched.block, row)?;
    let meta = decode_metadata(&fetched.block, row)?;

    w.raw(b"{\"transaction\":{\"signatures\":");
    stream_transaction_signatures(w, fetched, row, signature_index)?;
    w.raw(b",\"message\":");
    stream_message_value(w, resolver, &message, false).await?;
    w.raw(b"},\"meta\":");
    stream_metadata_value(w, resolver, meta.as_ref(), include_rewards).await?;
    w.raw(b",\"version\":");
    stream_message_version_value(w, &message);
    w.raw(b"}");
    Ok(())
}

async fn stream_lite_transaction_entry(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    fetched: &FetchedHotBlock,
    row: &ArchiveV2HotTxRow,
    signature_index: &mut usize,
) -> std::result::Result<(), String> {
    let message = decode_message(&fetched.block, row)?;

    w.raw(b"{\"transaction\":{\"signatures\":");
    stream_transaction_signatures(w, fetched, row, signature_index)?;
    w.raw(b",\"message\":");
    stream_message_value(w, resolver, &message, true).await?;
    w.raw(b"}}");
    Ok(())
}

async fn stream_accounts_transaction_entry(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    fetched: &FetchedHotBlock,
    row: &ArchiveV2HotTxRow,
    signature_index: &mut usize,
    include_rewards: bool,
) -> std::result::Result<(), String> {
    let message = decode_account_message_lite(&fetched.block, row)?;
    let meta = decode_metadata(&fetched.block, row)?;
    let meta_lite = meta.as_ref().map(account_meta_lite_from_compact);

    w.raw(b"{\"transaction\":");
    stream_account_transaction_lite_value(
        w,
        resolver,
        &message,
        meta_lite.as_ref(),
        fetched,
        row,
        signature_index,
    )
    .await?;
    w.raw(b",\"meta\":");
    stream_account_metadata_value(w, resolver, meta.as_ref(), include_rewards).await?;
    w.raw(b",\"version\":");
    stream_account_message_version_value(w, &message.version);
    w.raw(b"}");
    Ok(())
}

async fn stream_account_transaction_lite_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    message: &AccountMessageLite,
    meta: Option<&AccountMetaLite>,
    fetched: &FetchedHotBlock,
    row: &ArchiveV2HotTxRow,
    signature_index: &mut usize,
) -> std::result::Result<(), String> {
    let (loaded_writable, loaded_readonly) = meta
        .map(|meta| {
            (
                meta.loaded_writable_addresses.as_slice(),
                meta.loaded_readonly_addresses.as_slice(),
            )
        })
        .unwrap_or((&[], &[]));
    w.raw(b"{\"accountKeys\":");
    stream_account_key_objects_value(
        resolver,
        w,
        message.header,
        &message.account_keys,
        &message.program_id_indices,
        loaded_writable,
        loaded_readonly,
    )
    .await?;
    w.raw(b",\"signatures\":");
    stream_transaction_signatures(w, fetched, row, signature_index)?;
    w.raw(b"}");
    Ok(())
}

fn stream_transaction_signatures(
    w: &mut JsonByteWriter,
    fetched: &FetchedHotBlock,
    row: &ArchiveV2HotTxRow,
    signature_index: &mut usize,
) -> std::result::Result<(), String> {
    let access = fetched
        .access
        .as_ref()
        .ok_or_else(|| "streamed transaction signatures require block access".to_string())?;
    let start = signature_index
        .checked_mul(SIGNATURE_BYTES as usize)
        .ok_or_else(|| "signature byte offset overflow".to_string())?;
    let count = usize::from(row.signature_count);
    let byte_len = count
        .checked_mul(SIGNATURE_BYTES as usize)
        .ok_or_else(|| "signature byte count overflow".to_string())?;
    let end = start
        .checked_add(byte_len)
        .ok_or_else(|| "signature byte end overflow".to_string())?;
    let signatures = access
        .signatures()
        .get(start..end)
        .ok_or_else(|| "signature sidecar ended before transaction signatures".to_string())?;

    w.raw(b"[");
    for (index, signature) in signatures
        .chunks_exact(SIGNATURE_BYTES as usize)
        .enumerate()
    {
        if index > 0 {
            w.raw(b",");
        }
        w.base58_64(signature.try_into().expect("checked signature len"));
    }
    w.raw(b"]");
    *signature_index = signature_index
        .checked_add(count)
        .ok_or_else(|| "signature index overflow".to_string())?;
    Ok(())
}

async fn stream_account_key_objects_value(
    resolver: &mut Resolver<'_>,
    w: &mut JsonByteWriter,
    header: blockzilla_format::CompactMessageHeader,
    keys: &[CompactPubkey],
    program_id_indices: &[u8],
    loaded_writable_addresses: &[CompactPubkey],
    loaded_readonly_addresses: &[CompactPubkey],
) -> std::result::Result<(), String> {
    let required_signatures = header.num_required_signatures as usize;
    let readonly_signed = header.num_readonly_signed_accounts as usize;
    let readonly_unsigned = header.num_readonly_unsigned_accounts as usize;
    let signed_writable = required_signatures.saturating_sub(readonly_signed);
    let unsigned_writable_end = keys.len().saturating_sub(readonly_unsigned);

    w.raw(b"[");
    let mut first = true;
    for (index, key) in keys.iter().enumerate() {
        let signer = index < required_signatures;
        let header_writable = if signer {
            index < signed_writable
        } else {
            index < unsigned_writable_end
        };
        let writable = header_writable
            && u8::try_from(index)
                .ok()
                .is_none_or(|index| !program_id_indices.contains(&index));
        stream_account_key_object(
            w,
            resolver,
            *key,
            signer,
            "transaction",
            writable,
            &mut first,
        )
        .await?;
    }
    for key in loaded_writable_addresses {
        stream_account_key_object(w, resolver, *key, false, "lookupTable", true, &mut first)
            .await?;
    }
    for key in loaded_readonly_addresses {
        stream_account_key_object(w, resolver, *key, false, "lookupTable", false, &mut first)
            .await?;
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_account_key_object(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    key: CompactPubkey,
    signer: bool,
    source: &str,
    writable: bool,
    first: &mut bool,
) -> std::result::Result<(), String> {
    if !*first {
        w.raw(b",");
    }
    *first = false;
    let pubkey = resolver.resolve_pubkey(key).await?;
    let writable = writable && !is_reserved_readonly_account(&pubkey);
    w.raw(b"{\"pubkey\":");
    w.string(&pubkey)?;
    w.raw(b",\"signer\":");
    w.json_bool(signer);
    w.raw(b",\"source\":");
    w.string(source)?;
    w.raw(b",\"writable\":");
    w.json_bool(writable);
    w.raw(b"}");
    Ok(())
}

async fn stream_message_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    message: &ArchiveV2HotMessagePayload,
    account_objects: bool,
) -> std::result::Result<(), String> {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            stream_message_fields_value(
                w,
                resolver,
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                None,
                account_objects,
            )
            .await
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            stream_message_fields_value(
                w,
                resolver,
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                Some(&message.address_table_lookups),
                account_objects,
            )
            .await
        }
    }
}

async fn stream_message_fields_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    header: blockzilla_format::CompactMessageHeader,
    account_keys: &[CompactPubkey],
    recent_blockhash: &OwnedCompactRecentBlockhash,
    instructions: &[blockzilla_format::ArchiveV2HotInstruction],
    address_table_lookups: Option<&[OwnedCompactAddressTableLookup]>,
    account_objects: bool,
) -> std::result::Result<(), String> {
    w.raw(b"{\"accountKeys\":");
    if account_objects {
        stream_account_key_objects_value(resolver, w, header, account_keys, &[], &[], &[]).await?;
    } else {
        stream_compact_pubkey_array_value(w, resolver, account_keys).await?;
    }
    w.raw(b",\"instructions\":");
    stream_instruction_array_value(w, resolver, instructions).await?;
    w.raw(b",\"recentBlockhash\":");
    stream_resolved_recent_blockhash_value(w, resolver, recent_blockhash).await?;
    w.raw(b",\"header\":{\"numRequiredSignatures\":");
    w.u64(u64::from(header.num_required_signatures));
    w.raw(b",\"numReadonlySignedAccounts\":");
    w.u64(u64::from(header.num_readonly_signed_accounts));
    w.raw(b",\"numReadonlyUnsignedAccounts\":");
    w.u64(u64::from(header.num_readonly_unsigned_accounts));
    w.raw(b"}");
    if let Some(lookups) = address_table_lookups {
        w.raw(b",\"addressTableLookups\":");
        stream_address_table_lookups_value(w, resolver, lookups).await?;
    }
    w.raw(b"}");
    Ok(())
}

async fn stream_compact_pubkey_array_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    keys: &[CompactPubkey],
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_resolved_pubkey_value(w, resolver, *key).await?;
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_address_table_lookups_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    lookups: &[OwnedCompactAddressTableLookup],
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, lookup) in lookups.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"accountKey\":");
        stream_resolved_pubkey_value(w, resolver, lookup.account_key).await?;
        w.raw(b",\"writableIndexes\":");
        w.u8_array(&lookup.writable_indexes);
        w.raw(b",\"readonlyIndexes\":");
        w.u8_array(&lookup.readonly_indexes);
        w.raw(b"}");
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_instruction_array_value(
    w: &mut JsonByteWriter,
    resolver: &Resolver<'_>,
    instructions: &[blockzilla_format::ArchiveV2HotInstruction],
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, instruction) in instructions.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"programIdIndex\":");
        w.u64(u64::from(instruction.program_id_index));
        w.raw(b",\"accounts\":");
        w.u8_array(&instruction.accounts);
        w.raw(b",\"data\":");
        let data = instruction_data_base58(&instruction.data, Some(resolver))?;
        // Instruction data is base58, so the string is already JSON-safe ASCII.
        w.raw(b"\"");
        w.raw(data.as_bytes());
        w.raw(b"\"");
        w.raw(b",\"stackHeight\":1}");
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_metadata_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    meta: Option<&CompactMetaV1>,
    include_rewards: bool,
) -> std::result::Result<(), String> {
    let Some(meta) = meta else {
        w.raw(b"null");
        return Ok(());
    };

    let decoded_err = meta.err.as_ref();

    w.raw(b"{\"err\":");
    stream_optional_transaction_error(w, decoded_err)?;
    w.raw(b",\"status\":");
    stream_transaction_status(w, decoded_err)?;
    w.raw(b",\"fee\":");
    w.u64(meta.fee);
    w.raw(b",\"preBalances\":");
    w.u64_array(&meta.pre_balances);
    w.raw(b",\"postBalances\":");
    w.u64_array(&meta.post_balances);
    w.raw(b",\"preTokenBalances\":");
    stream_token_balances_value(w, resolver, &meta.pre_token_balances, false).await?;
    w.raw(b",\"postTokenBalances\":");
    stream_token_balances_value(w, resolver, &meta.post_token_balances, false).await?;
    w.raw(b",\"logMessages\":");
    stream_log_messages_value(w, resolver, meta.logs.as_ref()).await?;
    w.raw(b",\"innerInstructions\":");
    stream_inner_instructions_value(w, meta.inner_instructions.as_deref());
    w.raw(b",\"loadedAddresses\":{\"writable\":");
    stream_compact_pubkey_array_value(w, resolver, &meta.loaded_writable_addresses).await?;
    w.raw(b",\"readonly\":");
    stream_compact_pubkey_array_value(w, resolver, &meta.loaded_readonly_addresses).await?;
    w.raw(b"}");
    if let Some(return_data) = &meta.return_data {
        w.raw(b",\"returnData\":");
        stream_return_data_value(w, resolver, return_data).await?;
    }
    w.raw(b",\"rewards\":");
    if include_rewards {
        stream_compact_rewards_value(w, resolver, &meta.rewards).await?;
    } else {
        w.raw(b"null");
    }
    if let Some(value) = meta.compute_units_consumed {
        w.raw(b",\"computeUnitsConsumed\":");
        w.u64(value);
    }
    if let Some(value) = meta.cost_units {
        w.raw(b",\"costUnits\":");
        w.u64(value);
    }
    w.raw(b"}");
    Ok(())
}

async fn stream_return_data_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    return_data: &CompactReturnData,
) -> std::result::Result<(), String> {
    w.raw(b"{\"programId\":");
    stream_resolved_pubkey_value(w, resolver, return_data.program_id).await?;
    w.raw(b",\"data\":[");
    w.string(&BASE64.encode(&return_data.data))?;
    w.raw(br#","base64"]}"#);
    Ok(())
}

async fn stream_account_metadata_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    meta: Option<&CompactMetaV1>,
    include_rewards: bool,
) -> std::result::Result<(), String> {
    let Some(meta) = meta else {
        w.raw(b"null");
        return Ok(());
    };

    let decoded_err = meta.err.as_ref();

    w.raw(b"{\"err\":");
    stream_optional_transaction_error(w, decoded_err)?;
    w.raw(b",\"status\":");
    stream_transaction_status(w, decoded_err)?;
    w.raw(b",\"fee\":");
    w.u64(meta.fee);
    w.raw(b",\"preBalances\":");
    w.u64_array(&meta.pre_balances);
    w.raw(b",\"postBalances\":");
    w.u64_array(&meta.post_balances);
    w.raw(b",\"preTokenBalances\":");
    stream_token_balances_value(w, resolver, &meta.pre_token_balances, false).await?;
    w.raw(b",\"postTokenBalances\":");
    stream_token_balances_value(w, resolver, &meta.post_token_balances, false).await?;
    if include_rewards {
        w.raw(b",\"rewards\":");
        stream_compact_rewards_value(w, resolver, &meta.rewards).await?;
    }
    w.raw(b"}");
    Ok(())
}

async fn stream_token_balances_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    balances: &[blockzilla_format::CompactTokenBalance],
    ui_amount_null: bool,
) -> std::result::Result<(), String> {
    w.raw(b"[");
    for (index, balance) in balances.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_token_balance_value(w, resolver, balance, ui_amount_null).await?;
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_token_balance_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    balance: &blockzilla_format::CompactTokenBalance,
    ui_amount_null: bool,
) -> std::result::Result<(), String> {
    w.raw(b"{\"accountIndex\":");
    w.u64(u64::from(balance.account_index));
    if let Some(mint) = balance.mint {
        w.raw(b",\"mint\":");
        stream_resolved_pubkey_value(w, resolver, mint).await?;
    }
    if let Some(owner) = balance.owner {
        w.raw(b",\"owner\":");
        stream_resolved_pubkey_value(w, resolver, owner).await?;
    }
    if let Some(program_id) = balance.program_id {
        w.raw(b",\"programId\":");
        stream_resolved_pubkey_value(w, resolver, program_id).await?;
    }
    w.raw(b",\"uiTokenAmount\":");
    stream_ui_token_amount_value(w, balance.amount, balance.decimals, ui_amount_null)?;
    w.raw(b"}");
    Ok(())
}

fn stream_ui_token_amount_value(
    w: &mut JsonByteWriter,
    amount: u64,
    decimals: u8,
    _ui_amount_null: bool,
) -> std::result::Result<(), String> {
    w.raw(b"{\"amount\":");
    w.string(&amount.to_string())?;
    w.raw(b",\"decimals\":");
    w.u64(u64::from(decimals));
    w.raw(b",\"uiAmount\":");
    if amount == 0 {
        w.raw(b"null");
    } else {
        w.json(&ui_amount_number(amount, decimals))?;
    }
    w.raw(b",\"uiAmountString\":");
    w.string(&ui_amount_string(amount, decimals))?;
    w.raw(b"}");
    Ok(())
}

async fn stream_log_messages_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    logs: Option<&CompactLogStream>,
) -> std::result::Result<(), String> {
    let Some(logs) = logs else {
        w.raw(b"null");
        return Ok(());
    };
    if let Some(rendered) = render_logs_with_resolver(resolver, logs).await {
        w.json(&rendered)?;
    } else {
        w.raw(b"[]");
    }
    Ok(())
}

async fn render_logs_with_resolver(
    resolver: &mut Resolver<'_>,
    logs: &CompactLogStream,
) -> Option<Vec<String>> {
    let mut out = Vec::with_capacity(logs.events.len());
    let st = &logs.strings;
    let dt = &logs.data;
    let empty_store = KeyStore { keys: Vec::new() };

    for event in &logs.events {
        match event {
            LogEvent::Invoke { program, depth } => out.push(format!(
                "Program {} invoke [{}]",
                resolver.resolve_pubkey(*program).await.ok()?,
                depth
            )),
            LogEvent::BpfInvoke { program } => out.push(format!(
                "Call BPF program {}",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::Consumed {
                program,
                used,
                limit,
            } => out.push(format!(
                "Program {} consumed {} of {} compute units",
                resolver.resolve_pubkey(*program).await.ok()?,
                used,
                limit
            )),
            LogEvent::BpfConsumed { used, limit } => {
                out.push(format!("BPF program consumed {used} of {limit} units"))
            }
            LogEvent::Success { program } => out.push(format!(
                "Program {} success",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::BpfSuccess { program } => out.push(format!(
                "BPF program {} success",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::Failure { program, reason } => out.push(format!(
                "Program {} failed: {}",
                resolver.resolve_pubkey(*program).await.ok()?,
                st.resolve(*reason)
            )),
            LogEvent::BpfFailure { program, reason } => out.push(format!(
                "BPF program {} failed {}",
                resolver.resolve_pubkey(*program).await.ok()?,
                st.resolve(*reason)
            )),
            LogEvent::FailureCustomProgramError { program, code } => out.push(format!(
                "Program {} failed: custom program error: 0x{:x}",
                resolver.resolve_pubkey(*program).await.ok()?,
                code
            )),
            LogEvent::BpfFailureCustomProgramError { program, code } => out.push(format!(
                "BPF program {} failed custom program error: 0x{:x}",
                resolver.resolve_pubkey(*program).await.ok()?,
                code
            )),
            LogEvent::FailureInvalidAccountData { program } => out.push(format!(
                "Program {} failed: invalid account data for instruction",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::BpfFailureInvalidAccountData { program } => out.push(format!(
                "BPF program {} failed invalid account data for instruction",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::FailureInvalidProgramArgument { program } => out.push(format!(
                "Program {} failed: invalid program argument",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::BpfFailureInvalidProgramArgument { program } => out.push(format!(
                "BPF program {} failed invalid program argument",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::FailedToComplete { reason } => {
                out.push(format!("Program failed to complete: {}", st.resolve(*reason)))
            }
            LogEvent::LogTruncated => out.push("Log truncated".to_string()),
            LogEvent::StakeMergingAccounts => out.push("Merging stake accounts".to_string()),
            LogEvent::LoaderUpgradedProgram { program } => out.push(format!(
                "Upgraded program {}",
                resolver.resolve_pubkey(*program).await.ok()?
            )),
            LogEvent::LoaderFinalizedAccount { account } => out.push(format!(
                "Finalized account {}",
                resolver.resolve_pubkey(*account).await.ok()?
            )),
            LogEvent::ProgramLog(log) => {
                let payload = render_program_log_without_pubkey_store(log, &empty_store, st)?;
                if payload.is_empty() {
                    out.push("Program log:".to_string());
                } else {
                    out.push(format!("Program log: {payload}"));
                }
            }
            LogEvent::ProgramLogError { msg } => {
                out.push(format!("Program log: Error: {}", st.resolve(*msg)))
            }
            LogEvent::ProgramIdLog { program, log } => {
                let program = resolver.resolve_pubkey(*program).await.ok()?;
                let payload = render_program_log_without_pubkey_store(log, &empty_store, st)?;
                if payload.is_empty() {
                    out.push(format!("Program {program} log:"));
                } else {
                    out.push(format!("Program {program} log: {payload}"));
                }
            }
            LogEvent::ProgramPlainLog(log) => {
                out.push(render_program_log_without_pubkey_store(log, &empty_store, st)?)
            }
            LogEvent::ProgramAccountNotWritable => {
                out.push("Program account not writeable".to_string())
            }
            LogEvent::ProgramNotUpgradeable => out.push("Program not upgradeable".to_string()),
            LogEvent::ProgramAndProgramDataAccountMismatch => {
                out.push("Program and ProgramData account mismatch".to_string())
            }
            LogEvent::ProgramWasExtendedInThisBlockAlready => {
                out.push("Program was extended in this block already".to_string())
            }
            LogEvent::CustomProgramError { code } => {
                out.push(format!("custom program error: 0x{:x}", code))
            }
            LogEvent::Return { program, data } => out.push(format!(
                "Program return: {} {}",
                resolver.resolve_pubkey(*program).await.ok()?,
                dt.render(*data),
            )),
            LogEvent::Data { data } => out.push(format!("Program data: {}", dt.render(*data))),
            LogEvent::Consumption { units } => {
                out.push(format!("Program consumption: {} units remaining", units))
            }
            LogEvent::CbRequestUnits { units } => out.push(format!(
                "Program ComputeBudget111111111111111111111111111111 request units {}",
                units
            )),
            LogEvent::ProgramNotDeployed { program } => {
                if let Some(program) = program {
                    out.push(format!(
                        "Program {} is not deployed",
                        resolver.resolve_pubkey(*program).await.ok()?
                    ));
                } else {
                    out.push("Program is not deployed".to_string());
                }
            }
            LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    out.push(format!(
                        "Program {} is not cached",
                        resolver.resolve_pubkey(*program).await.ok()?
                    ));
                } else {
                    out.push("Program is not cached".to_string());
                }
            }
            LogEvent::UnknownProgram { program } => {
                out.push(format!("Unknown program {}", st.resolve(*program)))
            }
            LogEvent::UnknownAccount { account } => out.push(format!(
                "Instruction references an unknown account {}",
                st.resolve(*account)
            )),
            LogEvent::VerifyEd25519 => out.push("VerifyEd25519".to_string()),
            LogEvent::VerifySecp256k1 => out.push("VerifySecp256k1".to_string()),
            LogEvent::RuntimeWritablePrivilegeEscalated { account } => out.push(format!(
                "{}'s writable privilege escalated",
                resolver.resolve_pubkey(*account).await.ok()?
            )),
            LogEvent::RuntimeSignerPrivilegeEscalated { account } => out.push(format!(
                "{}'s signer privilege escalated",
                resolver.resolve_pubkey(*account).await.ok()?
            )),
            LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => out.push(format!(
                "failed to verify account {} instruction spent from the balance of an account it does not own",
                resolver.resolve_pubkey(*account).await.ok()?
            )),
            LogEvent::CloseContextState => out.push("CloseContextState".to_string()),
            LogEvent::Plain { text } | LogEvent::Unparsed { text } => {
                out.push(st.resolve(*text).to_string())
            }
            LogEvent::ProgramIdMismatch => out.push("Program id mismatch".to_string()),
            LogEvent::System(system_log) => {
                out.push(render_system_log_without_pubkey_store(system_log, st)?)
            }
        }
    }

    Some(apply_log_messages_byte_limit(out))
}

fn apply_log_messages_byte_limit(messages: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(messages.len());
    let mut bytes_written = 0usize;
    let mut limit_warning = false;

    for message in messages {
        let next_bytes_written = bytes_written.saturating_add(message.len());
        if next_bytes_written >= LOG_MESSAGES_BYTES_LIMIT {
            if !limit_warning {
                limit_warning = true;
                out.push("Log truncated".to_string());
            }
        } else {
            bytes_written = next_bytes_written;
            out.push(message);
        }
    }

    out
}

fn render_program_log_without_pubkey_store(
    log: &ProgramLog,
    empty_store: &KeyStore,
    strings: &blockzilla_format::StringTable,
) -> Option<String> {
    if let ProgramLog::Memo(blockzilla_format::program_logs::memo::MemoLog::MemoLenAndDebug {
        len,
        memo,
    }) = log
    {
        let memo = strings.resolve(*memo);
        let memo = if memo.starts_with('"') && memo.ends_with('"') {
            memo.to_string()
        } else {
            format!("{memo:?}")
        };
        return Some(format!("Memo (len {}): {}", strings.resolve(*len), memo));
    }
    Some(program_logs::render_program_log(log, empty_store, strings))
}

fn render_system_log_without_pubkey_store(
    log: &blockzilla_format::program_logs::system_program::SystemProgramLog,
    strings: &blockzilla_format::StringTable,
) -> Option<String> {
    use blockzilla_format::program_logs::system_program::{SystemInstructionLog, SystemProgramLog};

    Some(match log {
        SystemProgramLog::Instruction(instruction) => match instruction {
            SystemInstructionLog::RevokePendingActivation => {
                "Instruction: RevokePendingActivation".to_string()
            }
        },
        SystemProgramLog::AllocateRequestedTooLarge {
            requested,
            max_allowed,
        } => format!(
            "Allocate: requested {}, max allowed {}",
            requested, max_allowed
        ),
        SystemProgramLog::TransferFromMustNotCarryData => {
            "Transfer: `from` must not carry data".to_string()
        }
        SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { limit } => format!(
            "SystemProgram::CreateAccount data size limited to {} in inner instructions",
            limit
        ),
        SystemProgramLog::TransferInsufficient { have, need } => {
            format!("Transfer: insufficient lamports {}, need {}", have, need)
        }
        SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty => {
            "Advance nonce account: recent blockhash list is empty".to_string()
        }
        SystemProgramLog::InitializeNonceRecentBlockhashesEmpty => {
            "Initialize nonce account: recent blockhash list is empty".to_string()
        }
        SystemProgramLog::AuthorizeNonceAccount { msg } => {
            format!("Authorize nonce account: {}", strings.resolve(*msg))
        }
        SystemProgramLog::NonceInsufficientLamports { action, have, need } => format!(
            "{} nonce account: insufficient lamports {}, need {}",
            nonce_action_name(*action),
            have,
            need
        ),
        SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { action } => format!(
            "{} nonce account: nonce can only advance once per slot",
            nonce_action_name(*action)
        ),
        _ => return None,
    })
}

fn nonce_action_name(
    action: blockzilla_format::program_logs::system_program::NonceAction,
) -> &'static str {
    use blockzilla_format::program_logs::system_program::NonceAction;

    match action {
        NonceAction::Advance => "Advance",
        NonceAction::Withdraw => "Withdraw",
        NonceAction::Initialize => "Initialize",
        NonceAction::Authorize => "Authorize",
    }
}

fn stream_inner_instructions_value(
    w: &mut JsonByteWriter,
    inner_instructions: Option<&[CompactInnerInstructions]>,
) {
    let Some(inner_instructions) = inner_instructions else {
        w.raw(b"null");
        return;
    };
    w.raw(b"[");
    for (index, group) in inner_instructions.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"index\":");
        w.u64(u64::from(group.index));
        w.raw(b",\"instructions\":[");
        for (instruction_index, instruction) in group.instructions.iter().enumerate() {
            if instruction_index > 0 {
                w.raw(b",");
            }
            w.raw(b"{\"programIdIndex\":");
            w.u64(u64::from(instruction.program_id_index));
            w.raw(b",\"accounts\":");
            w.u8_array(&instruction.accounts);
            w.raw(b",\"data\":");
            let data = base58_bytes(&instruction.data);
            w.raw(b"\"");
            w.raw(data.as_bytes());
            w.raw(b"\"");
            w.raw(b",\"stackHeight\":");
            w.option_u64(instruction.stack_height.map(u64::from));
            w.raw(b"}");
        }
        w.raw(b"]}");
    }
    w.raw(b"]");
}

async fn stream_compact_rewards_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    rewards: &[CompactReward],
) -> std::result::Result<(), String> {
    if rewards.is_empty() {
        w.raw(b"[]");
        return Ok(());
    }
    w.raw(b"[");
    for (index, reward) in rewards.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        stream_compact_reward_value(w, resolver, reward).await?;
    }
    w.raw(b"]");
    Ok(())
}

async fn stream_compact_reward_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    reward: &CompactReward,
) -> std::result::Result<(), String> {
    w.raw(b"{\"pubkey\":");
    stream_resolved_pubkey_value(w, resolver, reward.pubkey).await?;
    w.raw(b",\"lamports\":");
    w.i64(reward.lamports);
    w.raw(b",\"postBalance\":");
    w.u64(reward.post_balance);
    w.raw(b",\"rewardType\":");
    match reward.reward_type {
        1 => w.raw(br#""Fee""#),
        2 => w.raw(br#""Rent""#),
        3 => w.raw(br#""Staking""#),
        4 => w.raw(br#""Voting""#),
        _ => w.raw(b"null"),
    }
    w.raw(b",\"commission\":");
    w.option_u8(reward.commission);
    w.raw(b"}");
    Ok(())
}

fn stream_account_metadata_lite_value(
    w: &mut JsonByteWriter,
    meta: Option<&AccountMetaLite>,
    include_rewards: bool,
) -> std::result::Result<(), String> {
    let Some(meta) = meta else {
        w.raw(b"null");
        return Ok(());
    };

    let decoded_err = meta.err.as_ref();

    w.raw(b"{\"err\":");
    stream_optional_transaction_error(w, decoded_err)?;
    w.raw(b",\"status\":");
    stream_transaction_status(w, decoded_err)?;
    w.raw(b",\"fee\":");
    w.u64(meta.fee);
    w.raw(b",\"preBalances\":");
    w.u64_array(&meta.pre_balances);
    w.raw(b",\"postBalances\":");
    w.u64_array(&meta.post_balances);
    w.raw(b",\"preTokenBalances\":");
    stream_token_balances_lite_value(w, meta.pre_token_balance_count);
    w.raw(b",\"postTokenBalances\":");
    stream_token_balances_lite_value(w, meta.post_token_balance_count);
    if include_rewards {
        w.raw(b",\"rewards\":null");
    }
    w.raw(b"}");
    Ok(())
}

fn stream_token_balances_lite_value(w: &mut JsonByteWriter, count: usize) {
    if count == 0 {
        w.raw(b"null");
    } else {
        w.raw(b"[]");
    }
}

async fn stream_rewards_value(
    w: &mut JsonByteWriter,
    resolver: &mut Resolver<'_>,
    rewards: Option<&blockzilla_format::ArchiveV2HotRewards>,
) -> std::result::Result<(), String> {
    let Some(rewards) = rewards else {
        w.raw(b"null");
        return Ok(());
    };

    w.raw(b"[");
    for (index, reward) in rewards.decoded.iter().enumerate() {
        if index > 0 {
            w.raw(b",");
        }
        w.raw(b"{\"pubkey\":");
        stream_resolved_pubkey_value(w, resolver, reward.pubkey).await?;
        w.raw(b",\"lamports\":");
        w.i64(reward.lamports);
        w.raw(b",\"postBalance\":");
        w.u64(reward.post_balance);
        w.raw(b",\"rewardType\":");
        match reward.reward_type {
            1 => w.raw(br#""Fee""#),
            2 => w.raw(br#""Rent""#),
            3 => w.raw(br#""Staking""#),
            4 => w.raw(br#""Voting""#),
            _ => w.raw(b"null"),
        }
        w.raw(b",\"commission\":");
        w.option_u8(reward.commission);
        w.raw(b"}");
        if index & 0x3ff == 0x3ff {
            w.flush_chunk_async().await?;
        }
    }
    w.raw(b"]");
    Ok(())
}

fn stream_message_version_value(w: &mut JsonByteWriter, message: &ArchiveV2HotMessagePayload) {
    match message {
        ArchiveV2HotMessagePayload::Legacy(_) => w.raw(br#""legacy""#),
        ArchiveV2HotMessagePayload::V0(_) => w.raw(b"0"),
    }
}

fn stream_account_message_version_value(w: &mut JsonByteWriter, version: &AccountMessageVersion) {
    match version {
        AccountMessageVersion::Legacy => w.raw(br#""legacy""#),
        AccountMessageVersion::V0 => w.raw(b"0"),
    }
}

async fn account_transactions_value(
    resolver: &mut Resolver<'_>,
    fetched: &FetchedHotBlock,
    include_rewards: bool,
) -> std::result::Result<Value, String> {
    let started = now_ms();
    let signatures = resolver.read_block_signatures(&fetched.row).await?;
    let mut signature_index = 0usize;
    let mut out = Vec::with_capacity(fetched.block.tx_rows.len());
    for row in &fetched.block.tx_rows {
        let tx_signatures =
            take_signatures(&signatures, &mut signature_index, row.signature_count)?;
        let message = decode_account_message_lite(&fetched.block, row)?;
        let meta = decode_metadata(&fetched.block, row)?;
        let meta_lite = meta.as_ref().map(account_meta_lite_from_compact);
        out.push(json!({
            "transaction": account_transaction_lite_value(resolver, &message, meta_lite.as_ref(), tx_signatures).await?,
            "meta": account_metadata_value(resolver, meta.as_ref(), include_rewards).await?,
            "version": message.version.value(),
        }));
    }
    resolver.source.record_elapsed(
        "render_transactions",
        started,
        0,
        "account_transactions",
        None,
    );
    Ok(Value::Array(out))
}

struct AccountMessageLite {
    header: blockzilla_format::CompactMessageHeader,
    account_keys: Vec<CompactPubkey>,
    program_id_indices: Vec<u8>,
    version: AccountMessageVersion,
}

enum AccountMessageVersion {
    Legacy,
    V0,
}

impl AccountMessageVersion {
    fn value(&self) -> Value {
        match self {
            Self::Legacy => Value::String("legacy".to_string()),
            Self::V0 => Value::from(0),
        }
    }
}

struct AccountMetaLite {
    err: Option<StoredTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    pre_token_balance_count: usize,
    post_token_balance_count: usize,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
}

fn account_meta_lite_from_compact(meta: &CompactMetaV1) -> AccountMetaLite {
    AccountMetaLite {
        err: meta.err.clone(),
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        pre_token_balance_count: meta.pre_token_balances.len(),
        post_token_balance_count: meta.post_token_balances.len(),
        loaded_writable_addresses: meta.loaded_writable_addresses.clone(),
        loaded_readonly_addresses: meta.loaded_readonly_addresses.clone(),
    }
}

async fn transactions_value(
    resolver: &mut Resolver<'_>,
    fetched: &FetchedHotBlock,
    lite: bool,
    include_meta: bool,
) -> std::result::Result<Value, String> {
    let started = now_ms();
    let signatures = resolver.read_block_signatures(&fetched.row).await?;
    let mut signature_index = 0usize;
    let mut out = Vec::with_capacity(fetched.block.tx_rows.len());
    for row in &fetched.block.tx_rows {
        let tx_signatures =
            take_signatures(&signatures, &mut signature_index, row.signature_count)?;
        let message = decode_message(&fetched.block, row)?;
        let transaction =
            transaction_value(resolver, &message, tx_signatures, !include_meta).await?;
        if lite {
            out.push(json!({ "transaction": transaction }));
            continue;
        }
        let meta = if include_meta {
            decode_metadata(&fetched.block, row)?
        } else {
            None
        };
        out.push(json!({
            "transaction": transaction,
            "meta": metadata_value(resolver, meta.as_ref()).await?,
            "version": message_version_value(&message),
        }));
    }
    resolver
        .source
        .record_elapsed("render_transactions", started, 0, "transactions", None);
    Ok(Value::Array(out))
}

async fn account_transaction_value(
    resolver: &mut Resolver<'_>,
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
    signatures: Vec<String>,
) -> std::result::Result<Value, String> {
    let account_keys = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            let program_id_indices = message_program_id_indices(&message.instructions);
            let (loaded_writable, loaded_readonly) = meta
                .map(|meta| {
                    (
                        meta.loaded_writable_addresses.as_slice(),
                        meta.loaded_readonly_addresses.as_slice(),
                    )
                })
                .unwrap_or((&[], &[]));
            account_key_objects_value(
                resolver,
                message.header,
                &message.account_keys,
                &program_id_indices,
                loaded_writable,
                loaded_readonly,
            )
            .await?
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            let program_id_indices = message_program_id_indices(&message.instructions);
            let (loaded_writable, loaded_readonly) = meta
                .map(|meta| {
                    (
                        meta.loaded_writable_addresses.as_slice(),
                        meta.loaded_readonly_addresses.as_slice(),
                    )
                })
                .unwrap_or((&[], &[]));
            account_key_objects_value(
                resolver,
                message.header,
                &message.account_keys,
                &program_id_indices,
                loaded_writable,
                loaded_readonly,
            )
            .await?
        }
    };
    Ok(json!({
        "accountKeys": account_keys,
        "signatures": signatures,
    }))
}

async fn account_transaction_lite_value(
    resolver: &mut Resolver<'_>,
    message: &AccountMessageLite,
    meta: Option<&AccountMetaLite>,
    signatures: Vec<String>,
) -> std::result::Result<Value, String> {
    let (loaded_writable, loaded_readonly) = meta
        .map(|meta| {
            (
                meta.loaded_writable_addresses.as_slice(),
                meta.loaded_readonly_addresses.as_slice(),
            )
        })
        .unwrap_or((&[], &[]));
    Ok(json!({
        "accountKeys": account_key_objects_value(
            resolver,
            message.header,
            &message.account_keys,
            &message.program_id_indices,
            loaded_writable,
            loaded_readonly,
        ).await?,
        "signatures": signatures,
    }))
}

fn take_signatures(
    signatures: &[String],
    index: &mut usize,
    count: u8,
) -> std::result::Result<Vec<String>, String> {
    let start = *index;
    let end = start
        .checked_add(usize::from(count))
        .ok_or_else(|| "signature index overflow".to_string())?;
    let values = signatures
        .get(start..end)
        .ok_or_else(|| "signature sidecar ended before block row signatures".to_string())?
        .to_vec();
    *index = end;
    Ok(values)
}

async fn signatures_value(
    resolver: &Resolver<'_>,
    fetched: &FetchedHotBlock,
) -> std::result::Result<Value, String> {
    let started = now_ms();
    if let Some(access) = fetched.access.as_ref() {
        let mut signature_index = 0usize;
        let mut out = Vec::with_capacity(fetched.block.tx_rows.len());
        for row in &fetched.block.tx_rows {
            if row.signature_count > 0 {
                let start = signature_index
                    .checked_mul(SIGNATURE_BYTES as usize)
                    .ok_or_else(|| "signature byte offset overflow".to_string())?;
                let end = start
                    .checked_add(SIGNATURE_BYTES as usize)
                    .ok_or_else(|| "signature byte end overflow".to_string())?;
                let signature = access.signatures().get(start..end).ok_or_else(|| {
                    "signature sidecar ended before first transaction signature".to_string()
                })?;
                out.push(Value::String(base58_64(
                    signature.try_into().expect("checked signature len"),
                )));
            }
            signature_index = signature_index
                .checked_add(usize::from(row.signature_count))
                .ok_or_else(|| "signature index overflow".to_string())?;
        }
        resolver
            .source
            .record_elapsed("render_signatures", started, 0, "signatures", None);
        return Ok(Value::Array(out));
    }

    let signatures = resolver.read_block_signatures(&fetched.row).await?;
    let mut index = 0usize;
    let mut out = Vec::with_capacity(fetched.block.tx_rows.len());
    for row in &fetched.block.tx_rows {
        if row.signature_count == 0 {
            continue;
        }
        let signature = signatures
            .get(index)
            .ok_or_else(|| {
                "signature sidecar ended before first transaction signature".to_string()
            })?
            .clone();
        out.push(Value::String(signature));
        index += usize::from(row.signature_count);
    }
    resolver
        .source
        .record_elapsed("render_signatures", started, 0, "signatures", None);
    Ok(Value::Array(out))
}

fn decode_message(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> std::result::Result<ArchiveV2HotMessagePayload, String> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Err(format!(
            "tx_index {} uses raw transaction fallback, which this renderer cannot serve yet",
            row.tx_index
        ));
    }
    let bytes = region_slice(
        &block.message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        row.tx_index,
    )?;
    wincode::config::deserialize(bytes, wincode_leb128_config())
        .map_err(|err| format!("decode message tx_index {}: {err}", row.tx_index))
}

fn decode_metadata(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> std::result::Result<Option<CompactMetaV1>, String> {
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 || row.metadata_len == 0 {
        return Ok(None);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Ok(None);
    }
    let bytes = region_slice(
        &block.metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        row.tx_index,
    )?;
    match wincode::config::deserialize(bytes, wincode_leb128_config()) {
        Ok(meta) => Ok(Some(meta)),
        Err(primary_err) => wincode::config::deserialize::<LegacyCompactMetaV1, _>(
            bytes,
            wincode_leb128_config(),
        )
        .map_err(|legacy_err| {
            format!(
                "decode metadata tx_index {}: {primary_err}; legacy err fallback: {legacy_err}",
                row.tx_index
            )
        })
        .and_then(|meta| {
            CompactMetaV1::try_from(meta).map_err(|err| {
                format!(
                    "decode legacy metadata error tx_index {}: {err}",
                    row.tx_index
                )
            })
        })
        .map(Some),
    }
}

fn decode_account_message_lite(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> std::result::Result<AccountMessageLite, String> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Err(format!(
            "tx_index {} uses raw transaction fallback, which this renderer cannot serve yet",
            row.tx_index
        ));
    }
    let bytes = region_slice(
        &block.message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        row.tx_index,
    )?;
    let mut reader = bytes;
    let variant: u32 = read_wincode_value(&mut reader).map_err(|err| {
        format!(
            "decode accounts message variant tx_index {}: {err}",
            row.tx_index
        )
    })?;
    let header = read_wincode_value(&mut reader).map_err(|err| {
        format!(
            "decode accounts message header tx_index {}: {err}",
            row.tx_index
        )
    })?;
    let account_keys = read_wincode_value(&mut reader).map_err(|err| {
        format!(
            "decode accounts message keys tx_index {}: {err}",
            row.tx_index
        )
    })?;
    let _recent_blockhash: OwnedCompactRecentBlockhash =
        read_wincode_value(&mut reader).map_err(|err| {
            format!(
                "decode accounts message recent blockhash tx_index {}: {err}",
                row.tx_index
            )
        })?;
    let instructions: Vec<blockzilla_format::ArchiveV2HotInstruction> =
        read_wincode_value(&mut reader).map_err(|err| {
            format!(
                "decode accounts message instructions tx_index {}: {err}",
                row.tx_index
            )
        })?;
    let mut program_id_indices = instructions
        .iter()
        .map(|instruction| instruction.program_id_index)
        .collect::<Vec<_>>();
    program_id_indices.sort_unstable();
    program_id_indices.dedup();
    let version = match variant {
        0 => AccountMessageVersion::Legacy,
        1 => AccountMessageVersion::V0,
        _ => {
            return Err(format!(
                "decode accounts message tx_index {}: unsupported message variant {}",
                row.tx_index, variant
            ));
        }
    };
    Ok(AccountMessageLite {
        header,
        account_keys,
        program_id_indices,
        version,
    })
}

fn decode_account_metadata_lite(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> std::result::Result<Option<AccountMetaLite>, String> {
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 || row.metadata_len == 0 {
        return Ok(None);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Ok(None);
    }
    let bytes = region_slice(
        &block.metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        row.tx_index,
    )?;
    decode_account_metadata_lite_bytes(bytes)
        .map(Some)
        .map_err(|err| format!("decode accounts metadata tx_index {}: {err}", row.tx_index))
}

fn decode_account_metadata_lite_bytes(
    bytes: &[u8],
) -> std::result::Result<AccountMetaLite, String> {
    let mut reader = bytes;
    let err = read_wincode_value(&mut reader)?;
    let fee = read_wincode_value(&mut reader)?;
    let pre_balances = read_wincode_value(&mut reader)?;
    let post_balances = read_wincode_value(&mut reader)?;
    skip_option_inner_instructions(&mut reader)?;
    skip_option_log_stream(&mut reader)?;
    let pre_token_balance_count = skip_token_balances(&mut reader)?;
    let post_token_balance_count = skip_token_balances(&mut reader)?;
    skip_rewards(&mut reader)?;
    let loaded_writable_addresses = read_wincode_value(&mut reader)?;
    let loaded_readonly_addresses = read_wincode_value(&mut reader)?;
    skip_option_return_data(&mut reader)?;
    skip_option_u64(&mut reader)?;
    skip_option_u64(&mut reader)?;
    Ok(AccountMetaLite {
        err,
        fee,
        pre_balances,
        post_balances,
        pre_token_balance_count,
        post_token_balance_count,
        loaded_writable_addresses,
        loaded_readonly_addresses,
    })
}

fn read_wincode_value<'de, T>(reader: &mut impl Reader<'de>) -> std::result::Result<T, String>
where
    T: SchemaRead<'de, WincodeLeb128Config, Dst = T>,
{
    T::get(reader.by_ref()).map_err(|err| err.to_string())
}

fn read_wincode_len<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<usize, String> {
    let len: u64 = read_wincode_value(reader)?;
    usize::try_from(len).map_err(|_| "wincode length does not fit usize".to_string())
}

fn skip_raw_bytes<'de>(
    reader: &mut impl Reader<'de>,
    len: usize,
) -> std::result::Result<(), String> {
    reader
        .take_scoped(len)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

fn read_option_tag<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<u8, String> {
    let tag: u8 = read_wincode_value(reader)?;
    match tag {
        0 | 1 => Ok(tag),
        _ => Err(format!("invalid option tag {tag}")),
    }
}

fn skip_option_u8<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        let _: u8 = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_option_u32<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        let _: u32 = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_option_u64<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        let _: u64 = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_byte_vec<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let len = read_wincode_len(reader)?;
    skip_raw_bytes(reader, len)
}

fn skip_u32_vec<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let len = read_wincode_len(reader)?;
    for _ in 0..len {
        let _: u32 = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_option_inner_instructions<'de>(
    reader: &mut impl Reader<'de>,
) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        let len = read_wincode_len(reader)?;
        for _ in 0..len {
            skip_inner_instructions(reader)?;
        }
    }
    Ok(())
}

fn skip_inner_instructions<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let _: u32 = read_wincode_value(reader)?;
    let len = read_wincode_len(reader)?;
    for _ in 0..len {
        let _: u32 = read_wincode_value(reader)?;
        skip_byte_vec(reader)?;
        skip_byte_vec(reader)?;
        skip_option_u32(reader)?;
    }
    Ok(())
}

fn skip_option_log_stream<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        skip_log_events(reader)?;
        skip_string_table(reader)?;
        skip_data_table(reader)?;
    }
    Ok(())
}

fn skip_log_events<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let len = read_wincode_len(reader)?;
    for _ in 0..len {
        let _: blockzilla_format::LogEvent = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_string_table<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    skip_u32_vec(reader)?;
    skip_byte_vec(reader)
}

fn skip_data_table<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let arrays_len = read_wincode_len(reader)?;
    for _ in 0..arrays_len {
        let _: u32 = read_wincode_value(reader)?;
    }
    skip_u32_vec(reader)?;
    skip_byte_vec(reader)
}

fn skip_token_balances<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<usize, String> {
    let len = read_wincode_len(reader)?;
    for _ in 0..len {
        let _: u32 = read_wincode_value(reader)?;
        skip_option_compact_pubkey(reader)?;
        skip_option_compact_pubkey(reader)?;
        skip_option_compact_pubkey(reader)?;
        let _: u64 = read_wincode_value(reader)?;
        let _: u8 = read_wincode_value(reader)?;
    }
    Ok(len)
}

fn skip_rewards<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let len = read_wincode_len(reader)?;
    for _ in 0..len {
        skip_compact_pubkey(reader)?;
        let _: i64 = read_wincode_value(reader)?;
        let _: u64 = read_wincode_value(reader)?;
        let _: i32 = read_wincode_value(reader)?;
        skip_option_u8(reader)?;
    }
    Ok(())
}

fn skip_option_return_data<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        skip_compact_pubkey(reader)?;
        skip_byte_vec(reader)?;
    }
    Ok(())
}

fn skip_option_compact_pubkey<'de>(
    reader: &mut impl Reader<'de>,
) -> std::result::Result<(), String> {
    if read_option_tag(reader)? == 1 {
        skip_compact_pubkey(reader)?;
    }
    Ok(())
}

fn skip_compact_pubkey<'de>(reader: &mut impl Reader<'de>) -> std::result::Result<(), String> {
    let id: u32 = read_wincode_value(reader)?;
    if id == CompactPubkey::RAW_SENTINEL {
        skip_raw_bytes(reader, 32)?;
    }
    Ok(())
}

async fn transaction_value(
    resolver: &mut Resolver<'_>,
    message: &ArchiveV2HotMessagePayload,
    signatures: Vec<String>,
    account_objects: bool,
) -> std::result::Result<Value, String> {
    Ok(json!({
        "signatures": signatures,
        "message": message_value(resolver, message, account_objects).await?,
    }))
}

async fn message_value(
    resolver: &mut Resolver<'_>,
    message: &ArchiveV2HotMessagePayload,
    account_objects: bool,
) -> std::result::Result<Value, String> {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            message_fields_value(
                resolver,
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                None,
                account_objects,
            )
            .await
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            message_fields_value(
                resolver,
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                Some(&message.address_table_lookups),
                account_objects,
            )
            .await
        }
    }
}

async fn message_fields_value(
    resolver: &mut Resolver<'_>,
    header: blockzilla_format::CompactMessageHeader,
    account_keys: &[CompactPubkey],
    recent_blockhash: &OwnedCompactRecentBlockhash,
    instructions: &[blockzilla_format::ArchiveV2HotInstruction],
    address_table_lookups: Option<&[OwnedCompactAddressTableLookup]>,
    account_objects: bool,
) -> std::result::Result<Value, String> {
    let keys_value = if account_objects {
        let program_id_indices = message_program_id_indices(instructions);
        account_key_objects_value(
            resolver,
            header,
            account_keys,
            &program_id_indices,
            &[],
            &[],
        )
        .await?
    } else {
        compact_pubkey_array_value(resolver, account_keys).await?
    };
    let mut out = Map::new();
    out.insert("accountKeys".to_string(), keys_value);
    out.insert(
        "instructions".to_string(),
        instruction_array_value(instructions)?,
    );
    out.insert(
        "recentBlockhash".to_string(),
        Value::String(resolver.resolve_recent_blockhash(recent_blockhash).await?),
    );
    out.insert(
        "header".to_string(),
        json!({
            "numRequiredSignatures": header.num_required_signatures,
            "numReadonlySignedAccounts": header.num_readonly_signed_accounts,
            "numReadonlyUnsignedAccounts": header.num_readonly_unsigned_accounts,
        }),
    );
    if let Some(lookups) = address_table_lookups {
        let mut items = Vec::with_capacity(lookups.len());
        for lookup in lookups {
            items.push(json!({
                "accountKey": resolver.resolve_pubkey(lookup.account_key).await?,
                "writableIndexes": lookup.writable_indexes,
                "readonlyIndexes": lookup.readonly_indexes,
            }));
        }
        out.insert("addressTableLookups".to_string(), Value::Array(items));
    }
    Ok(Value::Object(out))
}

async fn compact_pubkey_array_value(
    resolver: &mut Resolver<'_>,
    keys: &[CompactPubkey],
) -> std::result::Result<Value, String> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        out.push(Value::String(resolver.resolve_pubkey(*key).await?));
    }
    Ok(Value::Array(out))
}

async fn account_key_objects_value(
    resolver: &mut Resolver<'_>,
    header: blockzilla_format::CompactMessageHeader,
    keys: &[CompactPubkey],
    program_id_indices: &[u8],
    loaded_writable_addresses: &[CompactPubkey],
    loaded_readonly_addresses: &[CompactPubkey],
) -> std::result::Result<Value, String> {
    let required_signatures = header.num_required_signatures as usize;
    let readonly_signed = header.num_readonly_signed_accounts as usize;
    let readonly_unsigned = header.num_readonly_unsigned_accounts as usize;
    let signed_writable = required_signatures.saturating_sub(readonly_signed);
    let unsigned_writable_end = keys.len().saturating_sub(readonly_unsigned);
    let mut out = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let signer = index < required_signatures;
        let header_writable = if signer {
            index < signed_writable
        } else {
            index < unsigned_writable_end
        };
        let writable = header_writable
            && u8::try_from(index)
                .ok()
                .is_none_or(|index| !program_id_indices.contains(&index));
        let pubkey = resolver.resolve_pubkey(*key).await?;
        let writable = writable && !is_reserved_readonly_account(&pubkey);
        out.push(json!({
            "pubkey": pubkey,
            "signer": signer,
            "source": "transaction",
            "writable": writable,
        }));
    }
    for key in loaded_writable_addresses {
        out.push(json!({
            "pubkey": resolver.resolve_pubkey(*key).await?,
            "signer": false,
            "source": "lookupTable",
            "writable": true,
        }));
    }
    for key in loaded_readonly_addresses {
        out.push(json!({
            "pubkey": resolver.resolve_pubkey(*key).await?,
            "signer": false,
            "source": "lookupTable",
            "writable": false,
        }));
    }
    Ok(Value::Array(out))
}

fn message_program_id_indices(
    instructions: &[blockzilla_format::ArchiveV2HotInstruction],
) -> Vec<u8> {
    let mut indices = instructions
        .iter()
        .map(|instruction| instruction.program_id_index)
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn is_reserved_readonly_account(pubkey: &str) -> bool {
    matches!(
        pubkey,
        "11111111111111111111111111111111"
            | "SysvarC1ock11111111111111111111111111111111"
            | "SysvarEpochSchedu1e111111111111111111111111"
            | "SysvarFees111111111111111111111111111111111"
            | "SysvarRecentB1ockHashes11111111111111111111"
            | "SysvarRent111111111111111111111111111111111"
            | "SysvarRewards111111111111111111111111111111"
            | "SysvarS1otHashes111111111111111111111111111"
            | "SysvarS1otHistory11111111111111111111111111"
            | "SysvarStakeHistory1111111111111111111111111"
            | "Sysvar1nstructions1111111111111111111111111"
            | "SysvarLastRestartS1ot1111111111111111111111"
    )
}

fn instruction_array_value(
    instructions: &[blockzilla_format::ArchiveV2HotInstruction],
) -> std::result::Result<Value, String> {
    Ok(Value::Array(
        instructions
            .iter()
            .map(|instruction| {
                Ok(json!({
                    "programIdIndex": instruction.program_id_index,
                    "accounts": instruction.accounts,
                    "data": instruction_data_base58(&instruction.data, None)?,
                    "stackHeight": 1,
                }))
            })
            .collect::<std::result::Result<Vec<_>, String>>()?,
    ))
}

fn instruction_data_base58(
    data: &ArchiveV2HotInstructionData,
    resolver: Option<&Resolver<'_>>,
) -> std::result::Result<String, String> {
    match data {
        ArchiveV2HotInstructionData::Raw(bytes) => Ok(base58_bytes(bytes)),
        ArchiveV2HotInstructionData::ComputeBudget(value) => {
            let mut out = Vec::new();
            match value {
                blockzilla_format::ArchiveV2ComputeBudgetInstructionData::Unused => out.push(0),
                blockzilla_format::ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(bytes) => {
                    out.push(1);
                    out.extend_from_slice(&bytes.to_le_bytes());
                }
                blockzilla_format::ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(units) => {
                    out.push(2);
                    out.extend_from_slice(&units.to_le_bytes());
                }
                blockzilla_format::ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(price) => {
                    out.push(3);
                    out.extend_from_slice(&price.to_le_bytes());
                }
                blockzilla_format::ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(bytes) => {
                    out.push(4);
                    out.extend_from_slice(&bytes.to_le_bytes());
                }
            }
            Ok(base58_bytes(&out))
        }
        ArchiveV2HotInstructionData::System(value) => {
            Ok(base58_bytes(&system_instruction_bytes(value)))
        }
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
            let resolver = resolver.ok_or_else(|| {
                "compact vote instruction data requires block access vote hashes".to_string()
            })?;
            Ok(base58_bytes(&vote_update_instruction_bytes(
                12, update, resolver,
            )?))
        }
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
            update,
            switch_proof_hash,
        } => {
            let resolver = resolver.ok_or_else(|| {
                "compact vote instruction data requires block access vote hashes".to_string()
            })?;
            let mut out = vote_update_instruction_bytes(13, update, resolver)?;
            out.extend_from_slice(
                &resolver.resolve_vote_hash_ref(*switch_proof_hash, VoteHashKind::Aux)?,
            );
            Ok(base58_bytes(&out))
        }
        ArchiveV2HotInstructionData::VoteTowerSync(tower) => {
            let resolver = resolver.ok_or_else(|| {
                "compact vote instruction data requires block access vote hashes".to_string()
            })?;
            Ok(base58_bytes(&vote_tower_sync_instruction_bytes(
                14, tower, resolver,
            )?))
        }
        ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
            tower,
            switch_proof_hash,
        } => {
            let resolver = resolver.ok_or_else(|| {
                "compact vote instruction data requires block access vote hashes".to_string()
            })?;
            let mut out = vote_tower_sync_instruction_bytes(15, tower, resolver)?;
            out.extend_from_slice(
                &resolver.resolve_vote_hash_ref(*switch_proof_hash, VoteHashKind::Aux)?,
            );
            Ok(base58_bytes(&out))
        }
    }
}

#[derive(Clone, Copy)]
enum VoteHashKind {
    Bank,
    BlockId,
    Aux,
}

fn vote_update_instruction_bytes(
    variant: u32,
    update: &ArchiveV2VoteStateUpdate,
    resolver: &Resolver<'_>,
) -> std::result::Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(128);
    push_u32_le(&mut out, variant);
    push_u64_le(&mut out, update.root.unwrap_or(u64::MAX));
    push_short_vec_len(&mut out, update.lockout_offsets.len())?;
    for lockout in &update.lockout_offsets {
        push_var_u64(&mut out, lockout.offset);
        out.push(lockout.confirmation_count);
    }
    out.extend_from_slice(&resolver.resolve_vote_hash_ref(update.hash, VoteHashKind::Bank)?);
    push_option_i64(&mut out, update.timestamp);
    Ok(out)
}

fn vote_tower_sync_instruction_bytes(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    resolver: &Resolver<'_>,
) -> std::result::Result<Vec<u8>, String> {
    let mut out = vote_update_instruction_bytes(variant, &tower.update, resolver)?;
    out.extend_from_slice(
        &resolver.resolve_vote_hash_ref(tower.block_id_hash, VoteHashKind::BlockId)?,
    );
    Ok(out)
}

fn push_short_vec_len(out: &mut Vec<u8>, mut len: usize) -> std::result::Result<(), String> {
    if len > u16::MAX as usize {
        return Err(format!("vote short_vec length {len} exceeds u16::MAX"));
    }
    loop {
        let mut byte = (len & 0x7f) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            return Ok(());
        }
    }
}

fn push_var_u64(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn push_option_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn system_instruction_bytes(data: &blockzilla_format::ArchiveV2SystemInstructionData) -> Vec<u8> {
    use blockzilla_format::ArchiveV2SystemInstructionData as SystemIx;

    let mut out = Vec::with_capacity(96);
    match data {
        SystemIx::CreateAccount {
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 0);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::Assign { owner } => {
            push_u32_le(&mut out, 1);
            out.extend_from_slice(owner);
        }
        SystemIx::Transfer { lamports } => {
            push_u32_le(&mut out, 2);
            push_u64_le(&mut out, *lamports);
        }
        SystemIx::CreateAccountWithSeed {
            base,
            seed,
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 3);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::AdvanceNonceAccount => {
            push_u32_le(&mut out, 4);
        }
        SystemIx::WithdrawNonceAccount { lamports } => {
            push_u32_le(&mut out, 5);
            push_u64_le(&mut out, *lamports);
        }
        SystemIx::InitializeNonceAccount { authority } => {
            push_u32_le(&mut out, 6);
            out.extend_from_slice(authority);
        }
        SystemIx::AuthorizeNonceAccount { authority } => {
            push_u32_le(&mut out, 7);
            out.extend_from_slice(authority);
        }
        SystemIx::Allocate { space } => {
            push_u32_le(&mut out, 8);
            push_u64_le(&mut out, *space);
        }
        SystemIx::AllocateWithSeed {
            base,
            seed,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 9);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::AssignWithSeed { base, seed, owner } => {
            push_u32_le(&mut out, 10);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed);
            out.extend_from_slice(owner);
        }
        SystemIx::TransferWithSeed {
            lamports,
            from_seed,
            from_owner,
        } => {
            push_u32_le(&mut out, 11);
            push_u64_le(&mut out, *lamports);
            push_system_seed(&mut out, from_seed);
            out.extend_from_slice(from_owner);
        }
        SystemIx::UpgradeNonceAccount => {
            push_u32_le(&mut out, 12);
        }
        SystemIx::CreateAccountAllowPrefund {
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 13);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
    }
    out
}

fn push_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64_le(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_system_seed(out: &mut Vec<u8>, seed: &str) {
    push_u64_le(out, seed.len() as u64);
    out.extend_from_slice(seed.as_bytes());
}

async fn metadata_value(
    resolver: &mut Resolver<'_>,
    meta: Option<&CompactMetaV1>,
) -> std::result::Result<Value, String> {
    let Some(meta) = meta else {
        return Ok(Value::Null);
    };
    let err = transaction_error_value(meta.err.as_ref())?;
    let status = if err.is_null() {
        json!({ "Ok": null })
    } else {
        json!({ "Err": err.clone() })
    };
    let mut out = Map::new();
    out.insert("err".to_string(), err);
    out.insert("status".to_string(), status);
    out.insert("fee".to_string(), Value::from(meta.fee));
    out.insert("preBalances".to_string(), json!(meta.pre_balances));
    out.insert("postBalances".to_string(), json!(meta.post_balances));
    out.insert(
        "preTokenBalances".to_string(),
        token_balances_value(resolver, &meta.pre_token_balances, false).await?,
    );
    out.insert(
        "postTokenBalances".to_string(),
        token_balances_value(resolver, &meta.post_token_balances, false).await?,
    );
    out.insert(
        "logMessages".to_string(),
        log_messages_value(resolver, meta.logs.as_ref()).await?,
    );
    out.insert(
        "innerInstructions".to_string(),
        inner_instructions_value(meta.inner_instructions.as_deref()),
    );
    out.insert(
        "loadedAddresses".to_string(),
        json!({
            "writable": compact_pubkey_array_value(resolver, &meta.loaded_writable_addresses).await?,
            "readonly": compact_pubkey_array_value(resolver, &meta.loaded_readonly_addresses).await?,
        }),
    );
    if let Some(return_data) = &meta.return_data {
        out.insert(
            "returnData".to_string(),
            return_data_value(resolver, return_data).await?,
        );
    }
    out.insert(
        "rewards".to_string(),
        compact_rewards_value(resolver, &meta.rewards).await?,
    );
    if let Some(value) = meta.compute_units_consumed {
        out.insert("computeUnitsConsumed".to_string(), Value::from(value));
    }
    if let Some(value) = meta.cost_units {
        out.insert("costUnits".to_string(), Value::from(value));
    }
    Ok(Value::Object(out))
}

async fn return_data_value(
    resolver: &mut Resolver<'_>,
    return_data: &CompactReturnData,
) -> std::result::Result<Value, String> {
    Ok(json!({
        "programId": resolver.resolve_pubkey(return_data.program_id).await?,
        "data": [BASE64.encode(&return_data.data), "base64".to_string()],
    }))
}

async fn account_metadata_value(
    resolver: &mut Resolver<'_>,
    meta: Option<&CompactMetaV1>,
    include_rewards: bool,
) -> std::result::Result<Value, String> {
    let Some(meta) = meta else {
        return Ok(Value::Null);
    };
    let err = transaction_error_value(meta.err.as_ref())?;
    let status = if err.is_null() {
        json!({ "Ok": null })
    } else {
        json!({ "Err": err.clone() })
    };
    let mut out = Map::new();
    out.insert("err".to_string(), err);
    out.insert("status".to_string(), status);
    out.insert("fee".to_string(), Value::from(meta.fee));
    out.insert("preBalances".to_string(), json!(meta.pre_balances));
    out.insert("postBalances".to_string(), json!(meta.post_balances));
    out.insert(
        "preTokenBalances".to_string(),
        token_balances_value(resolver, &meta.pre_token_balances, false).await?,
    );
    out.insert(
        "postTokenBalances".to_string(),
        token_balances_value(resolver, &meta.post_token_balances, false).await?,
    );
    if include_rewards {
        out.insert(
            "rewards".to_string(),
            compact_rewards_value(resolver, &meta.rewards).await?,
        );
    }
    Ok(Value::Object(out))
}

fn account_metadata_lite_value(
    meta: Option<&AccountMetaLite>,
    include_rewards: bool,
) -> std::result::Result<Value, String> {
    let Some(meta) = meta else {
        return Ok(Value::Null);
    };
    let err = transaction_error_value(meta.err.as_ref())?;
    let status = if err.is_null() {
        json!({ "Ok": null })
    } else {
        json!({ "Err": err.clone() })
    };
    let mut out = json!({
        "err": err,
        "status": status,
        "fee": meta.fee,
        "preBalances": meta.pre_balances,
        "postBalances": meta.post_balances,
        "preTokenBalances": token_balances_lite_value(meta.pre_token_balance_count),
        "postTokenBalances": token_balances_lite_value(meta.post_token_balance_count),
    });
    if include_rewards && let Value::Object(map) = &mut out {
        map.insert("rewards".to_string(), Value::Null);
    }
    Ok(out)
}

fn token_balances_lite_value(_count: usize) -> Value {
    json!([])
}

async fn token_balances_value(
    resolver: &mut Resolver<'_>,
    balances: &[blockzilla_format::CompactTokenBalance],
    ui_amount_null: bool,
) -> std::result::Result<Value, String> {
    let mut out = Vec::with_capacity(balances.len());
    for balance in balances {
        out.push(token_balance_value(resolver, balance, ui_amount_null).await?);
    }
    Ok(Value::Array(out))
}

async fn token_balance_value(
    resolver: &mut Resolver<'_>,
    balance: &blockzilla_format::CompactTokenBalance,
    ui_amount_null: bool,
) -> std::result::Result<Value, String> {
    let mut out = Map::new();
    out.insert(
        "accountIndex".to_string(),
        Value::from(u64::from(balance.account_index)),
    );
    if let Some(mint) = balance.mint {
        out.insert(
            "mint".to_string(),
            Value::String(resolver.resolve_pubkey(mint).await?),
        );
    }
    if let Some(owner) = balance.owner {
        out.insert(
            "owner".to_string(),
            Value::String(resolver.resolve_pubkey(owner).await?),
        );
    }
    if let Some(program_id) = balance.program_id {
        out.insert(
            "programId".to_string(),
            Value::String(resolver.resolve_pubkey(program_id).await?),
        );
    }
    out.insert(
        "uiTokenAmount".to_string(),
        ui_token_amount_value(balance.amount, balance.decimals, ui_amount_null),
    );
    Ok(Value::Object(out))
}

fn ui_token_amount_value(amount: u64, decimals: u8, _ui_amount_null: bool) -> Value {
    json!({
        "amount": amount.to_string(),
        "decimals": decimals,
        "uiAmount": if amount == 0 { Value::Null } else { json!(ui_amount_number(amount, decimals)) },
        "uiAmountString": ui_amount_string(amount, decimals),
    })
}

fn ui_amount_number(amount: u64, decimals: u8) -> f64 {
    (amount as f64) / 10f64.powi(i32::from(decimals))
}

fn ui_amount_string(amount: u64, decimals: u8) -> String {
    if decimals == 0 {
        return amount.to_string();
    }
    let decimals = decimals as usize;
    let mut digits = amount.to_string();
    if digits.len() <= decimals {
        let zeros = decimals + 1 - digits.len();
        let mut padded = String::with_capacity(decimals + 1);
        padded.extend(std::iter::repeat_n('0', zeros));
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - decimals;
    let (whole, fraction) = digits.split_at(split);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

async fn log_messages_value(
    resolver: &mut Resolver<'_>,
    logs: Option<&CompactLogStream>,
) -> std::result::Result<Value, String> {
    let Some(logs) = logs else {
        return Ok(Value::Null);
    };
    Ok(render_logs_with_resolver(resolver, logs)
        .await
        .map(Value::from)
        .unwrap_or_else(|| Value::Array(Vec::new())))
}

fn inner_instructions_value(inner_instructions: Option<&[CompactInnerInstructions]>) -> Value {
    let Some(inner_instructions) = inner_instructions else {
        return Value::Null;
    };
    Value::Array(
        inner_instructions
            .iter()
            .map(|group| {
                json!({
                    "index": group.index,
                    "instructions": group.instructions.iter().map(inner_instruction_value).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn inner_instruction_value(instruction: &blockzilla_format::CompactInnerInstruction) -> Value {
    json!({
        "programIdIndex": instruction.program_id_index,
        "accounts": instruction.accounts,
        "data": base58_bytes(&instruction.data),
        "stackHeight": instruction.stack_height,
    })
}

async fn compact_rewards_value(
    resolver: &mut Resolver<'_>,
    rewards: &[CompactReward],
) -> std::result::Result<Value, String> {
    if rewards.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    let mut out = Vec::with_capacity(rewards.len());
    for reward in rewards {
        out.push(compact_reward_value(resolver, reward).await?);
    }
    Ok(Value::Array(out))
}

async fn compact_reward_value(
    resolver: &mut Resolver<'_>,
    reward: &CompactReward,
) -> std::result::Result<Value, String> {
    Ok(json!({
        "pubkey": resolver.resolve_pubkey(reward.pubkey).await?,
        "lamports": reward.lamports,
        "postBalance": reward.post_balance,
        "rewardType": match reward.reward_type {
            1 => Value::String("Fee".to_string()),
            2 => Value::String("Rent".to_string()),
            3 => Value::String("Staking".to_string()),
            4 => Value::String("Voting".to_string()),
            _ => Value::Null,
        },
        "commission": reward.commission,
    }))
}

async fn rewards_value(
    resolver: &mut Resolver<'_>,
    rewards: Option<&blockzilla_format::ArchiveV2HotRewards>,
) -> std::result::Result<Value, String> {
    let Some(rewards) = rewards else {
        return Ok(Value::Null);
    };
    let mut out = Vec::with_capacity(rewards.decoded.len());
    for reward in &rewards.decoded {
        out.push(json!({
            "pubkey": resolver.resolve_pubkey(reward.pubkey).await?,
            "lamports": reward.lamports,
            "postBalance": reward.post_balance,
            "rewardType": match reward.reward_type {
                1 => Value::String("Fee".to_string()),
                2 => Value::String("Rent".to_string()),
                3 => Value::String("Staking".to_string()),
                4 => Value::String("Voting".to_string()),
                _ => Value::Null,
            },
            "commission": reward.commission,
        }));
    }
    Ok(Value::Array(out))
}

fn message_version_value(message: &ArchiveV2HotMessagePayload) -> Value {
    match message {
        ArchiveV2HotMessagePayload::Legacy(_) => Value::String("legacy".to_string()),
        ArchiveV2HotMessagePayload::V0(_) => Value::from(0),
    }
}

struct Resolver<'a> {
    source: &'a WorkerSource,
    access: Option<&'a ArchiveV2BlockAccessBlob>,
}

impl<'a> Resolver<'a> {
    fn new(source: &'a WorkerSource, access: Option<&'a ArchiveV2BlockAccessBlob>) -> Self {
        Self { source, access }
    }

    async fn resolve_pubkey(&mut self, key: CompactPubkey) -> std::result::Result<String, String> {
        let bytes = self.resolve_pubkey_bytes(key).await?;
        Ok(base58_32(&bytes))
    }

    async fn resolve_pubkey_bytes(
        &mut self,
        key: CompactPubkey,
    ) -> std::result::Result<[u8; 32], String> {
        let bytes = match key {
            CompactPubkey::Raw(bytes) => bytes,
            CompactPubkey::Id(id) => {
                if let Some(bytes) = self.access_pubkey(id) {
                    return Ok(bytes);
                }
                return Err(format!(
                    "block access is missing pubkey id {id}; registry fallback is disabled"
                ));
            }
        };
        Ok(bytes)
    }

    fn access_pubkey(&self, id: u32) -> Option<[u8; 32]> {
        let pubkeys = &self.access?.pubkeys;
        if let Some(index) = pubkeys.binary_search_by_key(&id, |entry| entry.id).ok() {
            return Some(pubkeys[index].pubkey);
        }
        pubkeys
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.pubkey)
    }

    async fn resolve_recent_blockhash(
        &mut self,
        value: &OwnedCompactRecentBlockhash,
    ) -> std::result::Result<String, String> {
        let bytes = self.resolve_recent_blockhash_bytes(value).await?;
        Ok(base58_32(&bytes))
    }

    async fn resolve_recent_blockhash_bytes(
        &mut self,
        value: &OwnedCompactRecentBlockhash,
    ) -> std::result::Result<[u8; 32], String> {
        match value {
            OwnedCompactRecentBlockhash::Id(id) => self.resolve_blockhash_id_bytes(*id).await,
            OwnedCompactRecentBlockhash::Nonce(bytes) => Ok(*bytes),
        }
    }

    async fn resolve_blockhash_id(&mut self, id: i32) -> std::result::Result<String, String> {
        let bytes = self.resolve_blockhash_id_bytes(id).await?;
        Ok(base58_32(&bytes))
    }

    async fn resolve_blockhash_id_bytes(
        &mut self,
        id: i32,
    ) -> std::result::Result<[u8; 32], String> {
        if let Some(bytes) = self.access_blockhash(id) {
            return Ok(bytes);
        }
        Err(format!(
            "block access is missing blockhash id {id}; registry fallback is disabled"
        ))
    }

    fn access_blockhash(&self, id: i32) -> Option<[u8; 32]> {
        let blockhashes = &self.access?.blockhashes;
        blockhashes
            .binary_search_by_key(&id, |entry| entry.id)
            .ok()
            .map(|index| blockhashes[index].blockhash)
    }

    fn resolve_vote_hash_ref(
        &self,
        value: ArchiveV2VoteHashRef,
        kind: VoteHashKind,
    ) -> std::result::Result<[u8; 32], String> {
        match value {
            ArchiveV2VoteHashRef::Zero => Ok([0u8; 32]),
            ArchiveV2VoteHashRef::Raw(hash) => Ok(hash),
            ArchiveV2VoteHashRef::Block(block_id) => {
                let Some(access) = self.access else {
                    return Err(format!(
                        "block access is missing vote hash block id {block_id}; fallback is disabled"
                    ));
                };
                let Some(entry) = access
                    .vote_hashes
                    .iter()
                    .find(|entry| entry.block_id == block_id)
                else {
                    return Err(format!(
                        "block access is missing vote hash block id {block_id}; fallback is disabled"
                    ));
                };
                match kind {
                    VoteHashKind::Bank => entry.bank_hash.ok_or_else(|| {
                        format!("block access vote hash block id {block_id} is missing bank hash")
                    }),
                    VoteHashKind::BlockId => entry.block_id_hash.ok_or_else(|| {
                        format!(
                            "block access vote hash block id {block_id} is missing block-id hash"
                        )
                    }),
                    VoteHashKind::Aux => Err(format!(
                        "aux vote hash unexpectedly references block id {block_id}"
                    )),
                }
            }
        }
    }

    async fn read_block_signatures(
        &self,
        row: &ArchiveV2HotBlockIndexRow,
    ) -> std::result::Result<Vec<String>, String> {
        if let Some(access) = self.access {
            return Ok(access
                .signatures
                .chunks_exact(SIGNATURE_BYTES as usize)
                .map(|signature| base58_bytes(signature))
                .collect());
        }
        let _ = row;
        Err("block access is missing signatures; signatures file fallback is disabled".to_string())
    }
}

fn region_slice<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    label: &str,
    tx_index: u32,
) -> std::result::Result<&'a [u8], String> {
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .ok_or_else(|| format!("{label} region overflow for tx_index {tx_index}"))?;
    bytes.get(start..end).ok_or_else(|| {
        format!(
            "{label} region out of bounds for tx_index {tx_index}: offset={offset} len={len} bytes={}",
            bytes.len()
        )
    })
}

fn epoch_for_slot(slot: u64) -> u64 {
    slot / SLOTS_PER_EPOCH
}

fn epoch_file(prefix: &str, epoch: u64, file: &str) -> String {
    if prefix.is_empty() {
        format!("epoch-{epoch}/{file}")
    } else {
        format!("{prefix}/epoch-{epoch}/{file}")
    }
}

fn rpc_block_time(value: Option<i64>) -> Option<i64> {
    value.filter(|value| *value != 0)
}

#[derive(Clone)]
struct WorkerSource {
    archive_prefix: String,
    backend: ArchiveBackend,
    profile: Option<ProfileHandle>,
}

#[derive(Clone)]
enum ArchiveBackend {
    R2 { bucket: Bucket, binding: String },
    S3(S3Source),
}

#[derive(Clone)]
struct S3Source {
    endpoint: String,
    host: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
}

#[derive(Clone, Copy)]
enum StorageBackendKind {
    R2,
    S3,
}

impl WorkerSource {
    fn from_env(
        env: &worker::Env,
        profile: Option<ProfileHandle>,
    ) -> std::result::Result<Self, String> {
        let source = optional_var(env, "BZ_ARCHIVE_SOURCE").unwrap_or_else(|| "auto".to_string());
        let archive_prefix = optional_var(env, "BZ_ARCHIVE_PREFIX")
            .unwrap_or_else(|| "blockzilla-v2".to_string())
            .trim_matches('/')
            .to_string();
        let backend = match source.as_str() {
            "auto" => {
                let binding = r2_binding_name(env);
                match env.bucket(&binding) {
                    Ok(bucket) => ArchiveBackend::R2 { bucket, binding },
                    Err(_) => ArchiveBackend::S3(S3Source::from_env(env)?),
                }
            }
            "r2" | "cloudflare" | "cloudflare-r2" => {
                let binding = r2_binding_name(env);
                let bucket = env
                    .bucket(&binding)
                    .map_err(|err| format!("missing R2 bucket binding {binding}: {err}"))?;
                ArchiveBackend::R2 { bucket, binding }
            }
            "s3" | "backblaze" | "b2" => ArchiveBackend::S3(S3Source::from_env(env)?),
            _ => {
                return Err(
                    "BZ_ARCHIVE_SOURCE must be auto, r2, cloudflare-r2, s3, backblaze, or b2"
                        .to_string(),
                );
            }
        };
        Ok(Self {
            archive_prefix,
            backend,
            profile,
        })
    }

    fn record_elapsed(
        &self,
        name: &str,
        started_ms: f64,
        bytes: u64,
        detail: impl Into<String>,
        status: Option<u16>,
    ) {
        if let Some(profile) = &self.profile {
            profile.record_elapsed(name, started_ms, bytes, detail, status);
        }
    }

    async fn get_range(
        &self,
        path: &str,
        offset: u64,
        len: usize,
    ) -> std::result::Result<Vec<u8>, String> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let bytes = self.get_range_inner(path, offset, len).await?;
        if bytes.len() != len {
            return Err(format!(
                "archive GET {path} returned {} bytes for {}, expected {len}",
                bytes.len(),
                range_fetch_detail(path, offset, len)
            ));
        }
        Ok(bytes)
    }

    async fn get_range_inner(
        &self,
        path: &str,
        offset: u64,
        len: usize,
    ) -> std::result::Result<Vec<u8>, String> {
        match &self.backend {
            ArchiveBackend::R2 { bucket, binding } => {
                self.get_r2_range(bucket, binding, path, offset, len).await
            }
            ArchiveBackend::S3(source) => self.get_s3_range(source, path, offset, len).await,
        }
    }

    async fn get_r2_range(
        &self,
        bucket: &Bucket,
        binding: &str,
        path: &str,
        offset: u64,
        len: usize,
    ) -> std::result::Result<Vec<u8>, String> {
        ensure_js_safe_range(offset, len)?;
        let started = now_ms();
        let key = path.trim_start_matches('/').to_string();
        let detail = range_fetch_detail(path, offset, len);
        let profile_name = range_fetch_profile_name(StorageBackendKind::R2, path);
        let object = bucket
            .get(key.clone())
            .range(R2Range::OffsetWithLength {
                offset,
                length: len as u64,
            })
            .execute()
            .await
            .map_err(|err| format!("R2 GET {path} via {binding} {}: {err}", detail))?;
        let Some(object) = object else {
            self.record_elapsed(&profile_name, started, 0, detail, Some(404));
            return Err(format!(
                "R2 GET {path} via {binding} returned missing object"
            ));
        };
        let Some(body) = object.body() else {
            self.record_elapsed(&profile_name, started, 0, detail, None);
            return Err(format!(
                "R2 GET {path} via {binding} returned metadata without a body"
            ));
        };
        let bytes = body
            .bytes()
            .await
            .map_err(|err| format!("read R2 GET {path} via {binding}: {err}"))?;
        self.record_elapsed(
            &profile_name,
            started,
            bytes.len() as u64,
            detail,
            Some(206),
        );
        Ok(bytes)
    }

    async fn get_s3_range(
        &self,
        source: &S3Source,
        path: &str,
        offset: u64,
        len: usize,
    ) -> std::result::Result<Vec<u8>, String> {
        let started = now_ms();
        let range = s3_range_header(offset, len)?;
        let detail = range_fetch_detail(path, offset, len);
        let profile_name = range_fetch_profile_name(StorageBackendKind::S3, path);
        let (url, headers) = source.signed_get(path, Some(&range))?;
        let mut response = fetch_with_headers(&url, headers)
            .await
            .map_err(|err| format!("S3 GET {path} {range}: {err}"))?;
        let status = response.status_code();
        if status != 206 && status != 200 {
            let body = response
                .text()
                .await
                .unwrap_or_else(|err| format!("<failed to read error body: {err}>"));
            self.record_elapsed(
                &profile_name,
                started,
                body.len() as u64,
                detail,
                Some(status),
            );
            return Err(format!(
                "S3 GET {path} returned HTTP {status} for {range}: {}",
                short_error_body(&body)
            ));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|err| format!("read S3 GET {path} {range}: {err}"))?;
        self.record_elapsed(
            &profile_name,
            started,
            bytes.len() as u64,
            detail,
            Some(status),
        );
        Ok(bytes)
    }
}

impl S3Source {
    fn from_env(env: &worker::Env) -> std::result::Result<Self, String> {
        let endpoint = required_var(env, "BZ_S3_ENDPOINT")?
            .trim_end_matches('/')
            .to_string();
        let host = endpoint
            .strip_prefix("https://")
            .or_else(|| endpoint.strip_prefix("http://"))
            .ok_or_else(|| "BZ_S3_ENDPOINT must start with http:// or https://".to_string())?
            .split('/')
            .next()
            .unwrap_or_default()
            .to_string();
        if host.is_empty() {
            return Err("BZ_S3_ENDPOINT must include a host".to_string());
        }
        Ok(Self {
            endpoint,
            host,
            bucket: required_var(env, "BZ_S3_BUCKET")?,
            region: required_var(env, "BZ_S3_REGION")?,
            access_key: required_var(env, "BZ_S3_ACCESS_KEY_ID")?,
            secret_key: required_var(env, "BZ_S3_SECRET_ACCESS_KEY")?,
        })
    }

    fn signed_get(
        &self,
        path: &str,
        range: Option<&str>,
    ) -> std::result::Result<(String, Headers), String> {
        let key = path.trim_start_matches('/');
        let canonical_uri = format!(
            "/{}/{}",
            uri_encode_path_segment(&self.bucket),
            uri_encode_path(key)
        );
        let url = format!("{}{}", self.endpoint, canonical_uri);
        let (date, amz_date) = amz_dates();
        let mut signed = vec![
            ("host".to_string(), self.host.clone()),
            (
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            ),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        signed.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_headers = signed
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_headers = signed
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "GET\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
        );
        let credential_scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex_lower(&Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = signing_key(&self.secret_key, &date, &self.region);
        let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );
        let headers = Headers::new();
        headers
            .set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .map_err(|err| err.to_string())?;
        headers
            .set("x-amz-date", &amz_date)
            .map_err(|err| err.to_string())?;
        headers
            .set("authorization", &authorization)
            .map_err(|err| err.to_string())?;
        if let Some(range) = range {
            headers.set("range", range).map_err(|err| err.to_string())?;
        }
        Ok((url, headers))
    }
}

async fn fetch_with_headers(url: &str, headers: Headers) -> Result<worker::Response> {
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    init.with_headers(headers);
    let request = Request::new_with_init(url, &init)?;
    Fetch::Request(request).send().await
}

fn required_var(env: &worker::Env, name: &str) -> std::result::Result<String, String> {
    env.var(name)
        .map(|value| value.to_string())
        .map_err(|_| format!("missing required binding {name}"))
}

fn optional_var(env: &worker::Env, name: &str) -> Option<String> {
    env.var(name)
        .ok()
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty())
}

fn optional_secret_or_var(env: &worker::Env, name: &str) -> Option<String> {
    env.secret(name)
        .ok()
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| optional_var(env, name))
}

fn require_admin(req: &Request, env: &worker::Env) -> Result<()> {
    let Some(expected) = optional_secret_or_var(env, "BZ_ADMIN_TOKEN") else {
        return Err(worker::Error::RustError(
            "admin upload endpoint is disabled".to_string(),
        ));
    };
    let authorization = req
        .headers()
        .get("authorization")?
        .and_then(|value| value.strip_prefix("Bearer ").map(str::to_string));
    let header_token = req.headers().get("x-bz-admin-token")?;
    let supplied = authorization.or(header_token);
    if supplied.is_some_and(|value| token_matches(&value, &expected)) {
        Ok(())
    } else {
        Err(worker::Error::RustError(
            "admin upload endpoint requires a valid token".to_string(),
        ))
    }
}

fn token_matches(supplied: &str, expected: &str) -> bool {
    if supplied.is_empty() || expected.is_empty() {
        return false;
    }
    Sha256::digest(supplied.as_bytes()) == Sha256::digest(expected.as_bytes())
}

fn admin_bucket(env: &worker::Env) -> Result<Bucket> {
    let binding = r2_binding_name(env);
    env.bucket(&binding).map_err(|err| {
        worker::Error::RustError(format!("missing R2 bucket binding {binding}: {err}"))
    })
}

fn admin_upload_key(req: &Request, env: &worker::Env) -> Result<String> {
    let key = required_query_param(req, "key")?
        .trim_start_matches('/')
        .to_string();
    let archive_prefix = optional_var(env, "BZ_ARCHIVE_PREFIX")
        .unwrap_or_else(|| "blockzilla-v2".to_string())
        .trim_matches('/')
        .to_string();
    let required_prefix = format!("{archive_prefix}/");
    if key.is_empty() || !key.starts_with(&required_prefix) {
        return Err(worker::Error::RustError(format!(
            "upload key must be under {required_prefix}"
        )));
    }
    Ok(key)
}

fn required_query_param(req: &Request, name: &str) -> Result<String> {
    req.url()
        .map_err(|err| worker::Error::RustError(format!("invalid request URL: {err}")))?
        .query_pairs()
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| worker::Error::RustError(format!("missing required query parameter {name}")))
}

fn r2_binding_name(env: &worker::Env) -> String {
    optional_var(env, "BZ_R2_BUCKET_BINDING").unwrap_or_else(|| "BZ_ARCHIVE_BUCKET".to_string())
}

fn s3_range_header(offset: u64, len: usize) -> std::result::Result<String, String> {
    let end = offset
        .checked_add(len as u64)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| "range end overflow".to_string())?;
    Ok(format!("bytes={offset}-{end}"))
}

fn ensure_js_safe_range(offset: u64, len: usize) -> std::result::Result<(), String> {
    let len = len as u64;
    let end = offset
        .checked_add(len)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| "R2 range end overflow".to_string())?;
    let max = js_sys::Number::MAX_SAFE_INTEGER as u64;
    if offset > max || len > max || end > max {
        return Err(format!(
            "R2 range offset={offset} len={len} exceeds JavaScript safe integer limit {max}"
        ));
    }
    Ok(())
}

fn amz_dates() -> (String, String) {
    let now = js_sys::Date::new_0();
    let year = now.get_utc_full_year();
    let month = now.get_utc_month() + 1;
    let day = now.get_utc_date();
    let hour = now.get_utc_hours();
    let minute = now.get_utc_minutes();
    let second = now.get_utc_seconds();
    (
        format!("{year:04}{month:02}{day:02}"),
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
    )
}

fn signing_key(secret_key: &str, date: &str, region: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key_block = [0u8; 64];
    if key.len() > 64 {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for i in 0..64 {
        inner_pad[i] ^= key_block[i];
        outer_pad[i] ^= key_block[i];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    let out = outer.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn uri_encode_path(path: &str) -> String {
    path.split('/')
        .map(uri_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn uri_encode_path_segment(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn short_error_body(body: &str) -> String {
    let body = body.trim().replace(['\n', '\r', '\t'], " ");
    const MAX_LEN: usize = 300;
    if body.len() <= MAX_LEN {
        body
    } else {
        format!("{}...", &body[..MAX_LEN])
    }
}

fn range_fetch_profile_name(kind: StorageBackendKind, path: &str) -> &'static str {
    match kind {
        StorageBackendKind::R2 => {
            if path.ends_with(ARCHIVE_V2_GET_BLOCK_INDEX_FILE) {
                "r2_get_block_index"
            } else if path.ends_with(ARCHIVE_V2_BLOCKS_FILE) {
                "r2_blocks"
            } else if path.ends_with(ARCHIVE_V2_BLOCK_ACCESS_FILE) {
                "r2_meta_bin"
            } else {
                "r2_other"
            }
        }
        StorageBackendKind::S3 => {
            if path.ends_with(ARCHIVE_V2_GET_BLOCK_INDEX_FILE) {
                "s3_get_block_index"
            } else if path.ends_with(ARCHIVE_V2_BLOCKS_FILE) {
                "s3_blocks"
            } else if path.ends_with(ARCHIVE_V2_BLOCK_ACCESS_FILE) {
                "s3_meta_bin"
            } else {
                "s3_other"
            }
        }
    }
}

fn range_fetch_detail(path: &str, offset: u64, len: usize) -> String {
    let file = path.rsplit('/').next().unwrap_or(path);
    let end = offset.saturating_add(len as u64).saturating_sub(1);
    format!("{file} bytes={offset}-{end}")
}

fn cached_json_response<T: Serialize>(value: &T, cache_control: &str) -> Result<Response> {
    cached_json_response_profiled(value, cache_control, None)
}

fn cached_json_response_profiled<T: Serialize>(
    value: &T,
    cache_control: &str,
    profile: Option<&ProfileHandle>,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Type", "application/json")?;
    add_profile_headers(&headers, profile)?;
    Ok(Response::from_json(value)?.with_headers(headers))
}

fn block_bundle_response_profiled(
    bytes: Vec<u8>,
    profile: Option<&ProfileHandle>,
) -> Result<Response> {
    let headers = Headers::new();
    let cache_control = if profile.is_some() {
        ERROR_CACHE_CONTROL
    } else {
        BLOCK_CACHE_CONTROL
    };
    headers.set("Cache-Control", cache_control)?;
    headers.set(
        "Content-Type",
        "application/vnd.blockzilla.get-block-bundle.wincode",
    )?;
    headers.set("Content-Length", &bytes.len().to_string())?;
    add_profile_headers(&headers, profile)?;
    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}

fn json_error(status: u16, code: &str, message: &str) -> Result<Response> {
    json_error_profiled(status, code, message, None)
}

fn json_error_profiled(
    status: u16,
    code: &str,
    message: &str,
    profile: Option<&ProfileHandle>,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", ERROR_CACHE_CONTROL)?;
    headers.set("Content-Type", "application/json")?;
    add_profile_headers(&headers, profile)?;
    Ok(Response::from_json(&json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
        }
    }))?
    .with_status(status)
    .with_headers(headers))
}

fn add_profile_headers(headers: &Headers, profile: Option<&ProfileHandle>) -> Result<()> {
    let Some(profile) = profile else {
        return Ok(());
    };
    let server_timing = profile.server_timing();
    if !server_timing.is_empty() {
        headers.set("Server-Timing", &server_timing)?;
    }
    headers.set("X-BZ-Profile", &profile.profile_json().to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_error_render_instruction_custom() {
        let decoded = StoredTransactionError::InstructionError(0, InstructionError::Custom(0));
        let mut err = JsonByteWriter::with_capacity(64);
        stream_optional_transaction_error(&mut err, Some(&decoded)).expect("write err");
        assert_eq!(
            String::from_utf8(err.into_inner()).expect("utf8 err"),
            r#"{"InstructionError":[0,{"Custom":0}]}"#
        );

        let mut status = JsonByteWriter::with_capacity(80);
        stream_transaction_status(&mut status, Some(&decoded)).expect("write status");
        assert_eq!(
            String::from_utf8(status.into_inner()).expect("utf8 status"),
            r#"{"Err":{"InstructionError":[0,{"Custom":0}]}}"#
        );
    }

    #[test]
    fn transaction_error_render_borsh_io_error_as_string() {
        let decoded = StoredTransactionError::InstructionError(
            238,
            InstructionError::BorshIoError("Unknown".to_string()),
        );
        let mut err = JsonByteWriter::with_capacity(64);
        stream_optional_transaction_error(&mut err, Some(&decoded)).expect("write err");
        assert_eq!(
            String::from_utf8(err.into_inner()).expect("utf8 err"),
            r#"{"InstructionError":[238,"BorshIoError"]}"#
        );
    }

    #[test]
    fn parses_block_bin_route_path() {
        assert_eq!(block_bin_slot_from_path("/block/123.bin"), Some("123"));
        assert_eq!(block_bin_slot_from_path("/block/123.json"), None);
        assert_eq!(block_bin_slot_from_path("/block-lite/123.bin"), None);
        assert_eq!(block_bin_slot_from_path("/block/nested/123.bin"), None);
    }

    #[test]
    fn applies_rpc_log_messages_byte_limit() {
        let messages = (0..LOG_MESSAGES_BYTES_LIMIT + 1)
            .map(|_| "x".to_string())
            .collect();
        let limited = apply_log_messages_byte_limit(messages);
        assert_eq!(limited.len(), LOG_MESSAGES_BYTES_LIMIT);
        assert_eq!(limited.last().map(String::as_str), Some("Log truncated"));
    }
}
