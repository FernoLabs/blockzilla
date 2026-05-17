mod car_to_json;
mod rpc;
mod slot_index_builder;
mod source;

use self::car_to_json::car_bytes_to_protobuf;
use self::source::{ObjectSource, SourceError, Storage};
use crate::car_to_json_stream::{car_bytes_to_json_bytes, car_bytes_to_json_light_bytes};
use anyhow::{Result as AnyhowResult, anyhow};
use of_car_reader::compact_index::decode_offset_and_size;
use of_car_reader::node::{Node, decode_node, peek_node_type};
use of_car_reader::slot_ranges::{
    SLOT_RANGE_ENTRY_SIZE_U64, SLOT_RANGE_V2_ENTRY_SIZE_U64, SlotRange, decode_slot_range_entry,
    decode_slot_range_v2_entry, epoch_for_slot, slot_in_epoch as slot_in_epoch_for_slot,
    slot_range_entry_offset, slot_range_v2_entry_offset,
};
use of_car_reader::{CarBlockReader, car_block_group::CarBlockGroup};
use of_slot_ranges::{AsyncCompactIndex, RangeReader};
use serde::Serialize;
use std::{future::Future, io::Cursor, pin::Pin};
use worker::*;

const BLOCK_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const INFO_CACHE_CONTROL: &str = "public, max-age=30";
const ERROR_CACHE_CONTROL: &str = "no-store";
const MAX_CAR_BLOCK_BYTES: u32 = 64 * 1024 * 1024;
const MAX_REWARDS_ENTRY_BYTES: u32 = 8 * 1024 * 1024;
const CAR_CID_LEN: usize = 36;

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/", handle_info)
        .post_async("/", rpc::handle_rpc)
        .get_async("/info", handle_info)
        .get_async("/block/:slot", handle_block)
        .get_async("/block-lite/:slot", handle_block_lite)
        .run(req, env)
        .await
}

async fn handle_block(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    handle_block_response(req, ctx, BlockResponseMode::Full).await
}

async fn handle_block_lite(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    handle_block_response(req, ctx, BlockResponseMode::Lite).await
}

async fn handle_block_response(
    req: Request,
    ctx: RouteContext<()>,
    mode: BlockResponseMode,
) -> Result<Response> {
    let block_request = match parse_block_request(&req, &ctx) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };

    let storage = Storage::from_env(&ctx.env)?;

    let block = match get_block(&storage, block_request.slot, block_request.include_rewards).await {
        Ok(block) => block,
        Err(err) => {
            console_error!("Error fetching slot {}: {:?}", block_request.slot, err);
            return json_error(
                err.status_code(),
                err.code(),
                &format!(
                    "failed to fetch slot {}: {}",
                    block_request.slot,
                    err.client_message()
                ),
            );
        }
    };

    if block.bytes.is_empty() {
        return cached_json_response(
            &serde_json::json!({
                "ok": true,
                "empty": true,
                "slot": block_request.slot
            }),
            BLOCK_CACHE_CONTROL,
        );
    }

    match (mode, block_request.format) {
        (BlockResponseMode::Full, BlockResponseFormat::Json) => match car_bytes_to_json_bytes(
            block.bytes,
            block.previous_blockhash,
            block_request.include_rewards,
        ) {
            Ok(bytes) => cached_binary_response(bytes, "application/json", BLOCK_CACHE_CONTROL),
            Err(err) => {
                console_error!("Error decoding slot {}: {:?}", block_request.slot, err);
                json_error(
                    502,
                    "decode_error",
                    &format!("failed to decode slot {}: {err}", block_request.slot),
                )
            }
        },
        (BlockResponseMode::Lite, BlockResponseFormat::Json) => {
            match car_bytes_to_json_light_bytes(
                block.bytes,
                block.previous_blockhash,
                block_request.include_rewards,
            ) {
                Ok(bytes) => cached_binary_response(bytes, "application/json", BLOCK_CACHE_CONTROL),
                Err(err) => {
                    console_error!(
                        "Error decoding slot {} (lite): {:?}",
                        block_request.slot,
                        err
                    );
                    json_error(
                        502,
                        "decode_error",
                        &format!("failed to decode slot {} (lite): {err}", block_request.slot),
                    )
                }
            }
        }
        (mode, BlockResponseFormat::Protobuf) => match car_bytes_to_protobuf(
            block.bytes,
            block.previous_blockhash,
            block_request.include_rewards,
            mode.include_transaction_meta(),
        ) {
            Ok(bytes) => {
                cached_binary_response(bytes, "application/x-protobuf", BLOCK_CACHE_CONTROL)
            }
            Err(err) => {
                console_error!(
                    "Error protobuf-encoding slot {}: {:?}",
                    block_request.slot,
                    err
                );
                json_error(
                    502,
                    "decode_error",
                    &format!(
                        "failed to protobuf-encode slot {}: {err}",
                        block_request.slot
                    ),
                )
            }
        },
    }
}

#[derive(Clone, Copy)]
struct BlockRequest {
    slot: u64,
    format: BlockResponseFormat,
    include_rewards: bool,
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

#[derive(Clone, Copy)]
enum BlockResponseFormat {
    Json,
    Protobuf,
}

fn parse_slot_and_format(
    raw_slot: &str,
) -> std::result::Result<(&str, BlockResponseFormat), Response> {
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
        return Ok((slot, BlockResponseFormat::Protobuf));
    }
    if raw_slot.contains('.') {
        return Err(json_error(
            400,
            "unsupported_format",
            "unsupported block response extension; use .json or .bin",
        )
        .unwrap_or_else(|_| Response::error("Unsupported format", 400).unwrap()));
    }

    Ok((raw_slot, BlockResponseFormat::Json))
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

pub(crate) struct FetchedBlock {
    pub(crate) bytes: Vec<u8>,
    pub(crate) previous_blockhash: Option<[u8; 32]>,
}

#[derive(Clone, Copy)]
struct SlotRangeLookup {
    range: SlotRange,
    previous_blockhash: Option<[u8; 32]>,
}

pub(crate) async fn get_block(
    storage: &Storage,
    slot: u64,
    include_rewards: bool,
) -> FetchResult<FetchedBlock> {
    let epoch = epoch_for_slot(slot);
    let slot_in_epoch = slot_in_epoch_for_slot(slot);

    let slot_range = get_range_from_source(&storage.slot_index, epoch, slot_in_epoch).await?;

    if slot_range.range.len == 0 {
        return Ok(FetchedBlock {
            bytes: Vec::new(),
            previous_blockhash: slot_range.previous_blockhash,
        });
    }
    if slot_range.range.len > MAX_CAR_BLOCK_BYTES {
        return Err(FetchError::RangeTooLarge {
            slot,
            len: slot_range.range.len,
        });
    }

    let car_path = format!("{}/epoch-{}.car", epoch, epoch);
    let mut block_bytes = storage
        .archive
        .get_range(
            &car_path,
            slot_range.range.offset,
            slot_range.range.len as usize,
        )
        .await?;
    let previous_blockhash = match slot_range.previous_blockhash {
        Some(previous_blockhash) => Some(previous_blockhash),
        None => resolve_previous_blockhash(storage, slot, &block_bytes).await?,
    };

    if include_rewards && let Some(reward) = missing_block_rewards_entry(&block_bytes)? {
        let (rewards_start, rewards_len) =
            lookup_car_entry_range_for_cid(&storage.archive, epoch, &reward.cid)
                .await
                .map_err(|err| FetchError::RewardLookup {
                    reason: err.to_string(),
                })?;
        if rewards_len > MAX_REWARDS_ENTRY_BYTES {
            return Err(FetchError::RewardLookup {
                reason: format!(
                    "rewards entry is {rewards_len} bytes, above the {MAX_REWARDS_ENTRY_BYTES} byte limit"
                ),
            });
        }

        let rewards_entry = storage
            .archive
            .get_range(&car_path, rewards_start, rewards_len as usize)
            .await?;
        block_bytes.splice(reward.insert_at..reward.insert_at, rewards_entry);
    }

    Ok(FetchedBlock {
        bytes: block_bytes,
        previous_blockhash,
    })
}

async fn get_range_from_source(
    source: &ObjectSource,
    epoch: u64,
    slot_in_epoch: u64,
) -> FetchResult<SlotRangeLookup> {
    if let Some(v2) = get_range_v2_from_source(source, epoch, slot_in_epoch).await? {
        return Ok(v2);
    }

    get_legacy_range_from_source(source, epoch, slot_in_epoch).await
}

async fn get_range_v2_from_source(
    source: &ObjectSource,
    epoch: u64,
    slot_in_epoch: u64,
) -> FetchResult<Option<SlotRangeLookup>> {
    let offset = slot_range_v2_entry_offset(slot_in_epoch).map_err(|err| {
        FetchError::MalformedSlotIndexEntry {
            key: format!("epoch {epoch}"),
            reason: err.to_string(),
        }
    })?;
    let key = format!("slot-index/epoch-{}-slot-ranges-v2.raw", epoch);

    let bytes = match source
        .get_range(&key, offset, SLOT_RANGE_V2_ENTRY_SIZE_U64 as usize)
        .await
    {
        Ok(bytes) => bytes,
        Err(SourceError::SourceMissing { .. }) => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    let expected_len = SLOT_RANGE_V2_ENTRY_SIZE_U64 as usize;
    if bytes.len() != expected_len {
        return Err(FetchError::MalformedSlotIndexEntry {
            key,
            reason: format!("expected {expected_len} bytes, got {}", bytes.len()),
        });
    }

    let range =
        decode_slot_range_v2_entry(&bytes).map_err(|err| FetchError::MalformedSlotIndexEntry {
            key,
            reason: err.to_string(),
        })?;
    Ok(Some(SlotRangeLookup {
        range: range.range,
        previous_blockhash: Some(range.previous_blockhash),
    }))
}

async fn get_legacy_range_from_source(
    source: &ObjectSource,
    epoch: u64,
    slot_in_epoch: u64,
) -> FetchResult<SlotRangeLookup> {
    let offset = slot_range_entry_offset(slot_in_epoch).map_err(|err| {
        FetchError::MalformedSlotIndexEntry {
            key: format!("epoch {epoch}"),
            reason: err.to_string(),
        }
    })?;
    let key = format!("slot-index/epoch-{}-slot-ranges.raw", epoch);

    let bytes = source
        .get_range(&key, offset, SLOT_RANGE_ENTRY_SIZE_U64 as usize)
        .await
        .map_err(|err| match err {
            SourceError::SourceMissing { .. } => FetchError::MissingSlotIndex { key: key.clone() },
            err => err.into(),
        })?;

    let expected_len = SLOT_RANGE_ENTRY_SIZE_U64 as usize;
    if bytes.len() != expected_len {
        return Err(FetchError::MalformedSlotIndexEntry {
            key,
            reason: format!("expected {expected_len} bytes, got {}", bytes.len()),
        });
    }

    let range =
        decode_slot_range_entry(&bytes).map_err(|err| FetchError::MalformedSlotIndexEntry {
            key,
            reason: err.to_string(),
        })?;
    Ok(SlotRangeLookup {
        range,
        previous_blockhash: None,
    })
}

async fn lookup_car_entry_range_for_cid(
    source: &ObjectSource,
    epoch: u64,
    cid: &[u8],
) -> AnyhowResult<(u64, u32)> {
    let epoch_cid_path = format!("{epoch}/epoch-{epoch}.cid");
    let epoch_cid = source
        .get_text(&epoch_cid_path)
        .await
        .map_err(|err| anyhow!("{epoch_cid_path}: {}", err.client_message()))?;
    let epoch_cid = epoch_cid.trim();
    if epoch_cid.is_empty() {
        return Err(anyhow!("empty epoch cid from {epoch_cid_path}"));
    }

    let cid_index_path =
        format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");
    let cid_reader = SourceRangeReader::new(source.clone(), cid_index_path.clone());
    let mut cid_index = AsyncCompactIndex::open(cid_reader, cid_index_path).await?;

    let mut offset_and_size = vec![0u8; cid_index.value_size()];
    let found = cid_index
        .lookup_into_node_reads(cid, &mut offset_and_size)
        .await?;
    if !found {
        return Err(anyhow!("cid not found in epoch {epoch} cid index"));
    }

    Ok(decode_offset_and_size(&offset_and_size)?)
}

async fn resolve_previous_blockhash(
    storage: &Storage,
    slot: u64,
    block_bytes: &[u8],
) -> FetchResult<Option<[u8; 32]>> {
    let Some(parent_slot) = decode_block_parent_slot(block_bytes.to_vec())? else {
        return Ok(None);
    };
    if parent_slot == slot {
        return Ok(None);
    }

    let parent_range = get_range_from_source(
        &storage.slot_index,
        epoch_for_slot(parent_slot),
        slot_in_epoch_for_slot(parent_slot),
    )
    .await?;
    if parent_range.range.is_empty() {
        return Ok(None);
    }
    if parent_range.range.len > MAX_CAR_BLOCK_BYTES {
        return Err(FetchError::RangeTooLarge {
            slot: parent_slot,
            len: parent_range.range.len,
        });
    }

    let parent_epoch = epoch_for_slot(parent_slot);
    let parent_car_path = format!("{}/epoch-{}.car", parent_epoch, parent_epoch);
    let parent_bytes = storage
        .archive
        .get_range(
            &parent_car_path,
            parent_range.range.offset,
            parent_range.range.len as usize,
        )
        .await?;
    decode_blockhash(parent_bytes).map(Some)
}

fn decode_block_parent_slot(bytes: Vec<u8>) -> FetchResult<Option<u64>> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = CarBlockGroup::without_rewards();
    if !reader
        .read_until_block_into(&mut block)
        .map_err(|err| FetchError::MalformedCarSlice {
            reason: format!("decode block node: {err}"),
        })?
    {
        return Err(FetchError::MalformedCarSlice {
            reason: "CAR slice did not contain a block node".to_string(),
        });
    }
    Ok(block.parent_slot)
}

fn decode_blockhash(bytes: Vec<u8>) -> FetchResult<[u8; 32]> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = CarBlockGroup::without_rewards();
    if !reader
        .read_until_block_into(&mut block)
        .map_err(|err| FetchError::MalformedCarSlice {
            reason: format!("decode block node: {err}"),
        })?
    {
        return Err(FetchError::MalformedCarSlice {
            reason: "CAR slice did not contain a block node".to_string(),
        });
    }
    if !block.has_blockhash {
        return Err(FetchError::MalformedCarSlice {
            reason: "block did not contain an entry hash".to_string(),
        });
    }
    Ok(block.blockhash)
}

struct SourceRangeReader {
    source: ObjectSource,
    path: String,
}

impl SourceRangeReader {
    fn new(source: ObjectSource, path: String) -> Self {
        Self { source, path }
    }
}

impl RangeReader for SourceRangeReader {
    type ReadFuture<'a>
        = Pin<Box<dyn Future<Output = AnyhowResult<()>> + 'a>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        let source = self.source.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let bytes = source
                .get_range(&path, offset, out.len())
                .await
                .map_err(|err| anyhow!("{}: {}", path, err.client_message()))?;
            out.copy_from_slice(&bytes);
            Ok(())
        })
    }
}

fn cached_json_response<T: Serialize>(value: &T, cache_control: &str) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Type", "application/json")?;

    Ok(Response::from_json(value)?.with_headers(headers))
}

fn cached_binary_response(
    bytes: Vec<u8>,
    content_type: &str,
    cache_control: &str,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Type", content_type)?;

    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}

fn json_error(status: u16, code: &str, message: &str) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", ERROR_CACHE_CONTROL)?;
    headers.set("Content-Type", "application/json")?;

    Ok(Response::from_json(&serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message
        }
    }))?
    .with_status(status)
    .with_headers(headers))
}

struct MissingRewardEntry {
    cid: Vec<u8>,
    insert_at: usize,
}

fn missing_block_rewards_entry(car_fragment: &[u8]) -> FetchResult<Option<MissingRewardEntry>> {
    let mut pos = 0usize;
    let mut entry_cids = Vec::new();
    let mut missing_reward = None;

    while pos < car_fragment.len() {
        let (entry_len, varint_len) = read_uvarint64(&car_fragment[pos..])?;
        let entry_len = usize::try_from(entry_len).map_err(|_| FetchError::MalformedCarSlice {
            reason: "CAR entry length exceeds usize".to_string(),
        })?;
        if entry_len < CAR_CID_LEN {
            return Err(FetchError::MalformedCarSlice {
                reason: "CAR entry length is smaller than CID length".to_string(),
            });
        }

        let cid_start =
            pos.checked_add(varint_len)
                .ok_or_else(|| FetchError::MalformedCarSlice {
                    reason: "CAR entry offset overflow".to_string(),
                })?;
        let payload_start =
            cid_start
                .checked_add(CAR_CID_LEN)
                .ok_or_else(|| FetchError::MalformedCarSlice {
                    reason: "CAR payload offset overflow".to_string(),
                })?;
        let next_pos = pos
            .checked_add(varint_len)
            .and_then(|value| value.checked_add(entry_len))
            .ok_or_else(|| FetchError::MalformedCarSlice {
                reason: "CAR entry end overflow".to_string(),
            })?;
        if next_pos > car_fragment.len() {
            return Err(FetchError::MalformedCarSlice {
                reason: "CAR entry extends past fetched slot range".to_string(),
            });
        }

        entry_cids.push(car_fragment[cid_start..payload_start].to_vec());
        let payload = &car_fragment[payload_start..next_pos];
        if let Some(cid) = block_rewards_cid(payload)? {
            missing_reward = Some(MissingRewardEntry {
                cid,
                insert_at: pos,
            });
        }

        pos = next_pos;
    }

    let Some(missing_reward) = missing_reward else {
        return Ok(None);
    };
    if entry_cids
        .iter()
        .any(|cid| cid.as_slice() == missing_reward.cid.as_slice())
    {
        Ok(None)
    } else {
        Ok(Some(missing_reward))
    }
}

fn block_rewards_cid(payload: &[u8]) -> FetchResult<Option<Vec<u8>>> {
    let kind = peek_node_type(payload).map_err(|err| FetchError::MalformedCarSlice {
        reason: format!("decode CAR node kind: {err}"),
    })?;
    if kind != 2 {
        return Ok(None);
    }

    let node = decode_node(payload).map_err(|err| FetchError::MalformedCarSlice {
        reason: format!("decode block node: {err}"),
    })?;
    let Node::Block(block) = node else {
        return Ok(None);
    };
    let Some(rewards) = block.rewards else {
        return Ok(None);
    };
    if rewards.inline_raw_bytes().is_some() {
        return Ok(None);
    }
    rewards
        .car_cid_bytes()
        .map(|cid| cid.to_vec())
        .ok_or_else(|| FetchError::MalformedCarSlice {
            reason: format!(
                "CID ref has {} bytes and is not an inline identity CID",
                rewards.normalized_bytes().len()
            ),
        })
        .map(Some)
}

fn read_uvarint64(bytes: &[u8]) -> FetchResult<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;

    for (index, byte) in bytes.iter().take(10).copied().enumerate() {
        if byte < 0x80 {
            if index == 9 && byte > 1 {
                return Err(FetchError::MalformedCarSlice {
                    reason: "CAR entry length varint overflows u64".to_string(),
                });
            }
            value |= (byte as u64) << shift;
            return Ok((value, index + 1));
        }

        value |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }

    Err(FetchError::MalformedCarSlice {
        reason: "unterminated CAR entry length varint".to_string(),
    })
}

pub(crate) type FetchResult<T> = std::result::Result<T, FetchError>;

#[derive(Debug)]
pub(crate) enum FetchError {
    MissingSlotIndex { key: String },
    MalformedSlotIndexEntry { key: String, reason: String },
    RangeTooLarge { slot: u64, len: u32 },
    MalformedCarSlice { reason: String },
    RewardLookup { reason: String },
    Source(SourceError),
    Worker(Error),
}

impl FetchError {
    pub(crate) fn status_code(&self) -> u16 {
        match self {
            Self::MissingSlotIndex { .. } => 404,
            Self::MalformedSlotIndexEntry { .. }
            | Self::RangeTooLarge { .. }
            | Self::MalformedCarSlice { .. }
            | Self::RewardLookup { .. }
            | Self::Worker(_) => 500,
            Self::Source(err) => err.status_code(),
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::MissingSlotIndex { .. } => "slot_index_missing",
            Self::MalformedSlotIndexEntry { .. } => "slot_index_malformed",
            Self::RangeTooLarge { .. } => "range_too_large",
            Self::MalformedCarSlice { .. } => "car_slice_malformed",
            Self::RewardLookup { .. } => "reward_lookup_failed",
            Self::Source(err) => err.code(),
            Self::Worker(_) => "worker_error",
        }
    }

    pub(crate) fn client_message(&self) -> String {
        match self {
            Self::MissingSlotIndex { key } => {
                format!("{key} is not available in the slot index source")
            }
            Self::MalformedSlotIndexEntry { key, reason } => {
                format!("{key} has an invalid slot-range entry: {reason}")
            }
            Self::RangeTooLarge { slot, len } => {
                format!(
                    "slot {slot} CAR range is {len} bytes, above the {MAX_CAR_BLOCK_BYTES} byte limit"
                )
            }
            Self::MalformedCarSlice { reason } => {
                format!("slot CAR slice is malformed: {reason}")
            }
            Self::RewardLookup { reason } => {
                format!("failed to fetch block rewards by CID: {reason}")
            }
            Self::Source(err) => err.client_message(),
            Self::Worker(err) => err.to_string(),
        }
    }
}

impl From<Error> for FetchError {
    fn from(err: Error) -> Self {
        Self::Worker(err)
    }
}

impl From<SourceError> for FetchError {
    fn from(err: SourceError) -> Self {
        Self::Source(err)
    }
}

#[derive(Clone, Copy)]
enum BlockResponseMode {
    Full,
    Lite,
}

impl BlockResponseMode {
    fn include_transaction_meta(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// GET /info
/// Returns static Worker capability information.
async fn handle_info(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    cached_json_response(
        &serde_json::json!({
            "ok": true,
            "json_rpc_methods": ["getBlock", "getBlockTime", "getVersion"],
            "block_routes": ["/block/:slot", "/block-lite/:slot"],
            "slot_index_source": ctx.var("OF_SLOT_INDEX_SOURCE").ok().map(|value| value.to_string()).unwrap_or_else(|| "r2".to_string()),
            "archive_source": ctx.var("OF_ARCHIVE_SOURCE").ok().map(|value| value.to_string()).unwrap_or_else(|| "http".to_string()),
        }),
        INFO_CACHE_CONTROL,
    )
}
