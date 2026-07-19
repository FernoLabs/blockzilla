use super::http;
use crate::error::{FetchError, FetchResult, MAX_CAR_BLOCK_BYTES};
use anyhow::{Result as AnyhowResult, anyhow};
use of_car_reader::compact_index::decode_offset_and_size;
use of_car_reader::node::{CarCid, Node, car_cid_from_bytes, decode_node, peek_node_type};
use of_car_reader::slot_ranges::{
    SLOT_RANGE_ENTRY_SIZE, SLOT_RANGE_V2_ENTRY_SIZE, SlotRange, decode_slot_range_entry,
    decode_slot_range_v2_entry, epoch_for_slot, slot_in_epoch, slot_range_entry_offset,
    slot_range_v2_entry_offset,
};
use of_slot_ranges::{AsyncCompactIndex, RangeReader};
use std::{collections::HashSet, future::Future, pin::Pin};
use worker::{Bucket, Env, Range as R2Range};

const MAX_REWARDS_ENTRY_BYTES: u32 = 8 * 1024 * 1024;
const CAR_CID_LEN: usize = 36;
const CAR_HEADER_PREFIX_READ_LEN: usize = 16;
const INDEX_BUCKET_BINDING: &str = "OF_INDEXES";

pub(crate) struct FetchedBlock {
    pub(crate) bytes: Vec<u8>,
    pub(crate) previous_blockhash: Option<[u8; 32]>,
}

struct SlotLookup {
    range: SlotRange,
    previous_blockhash: Option<[u8; 32]>,
}

pub(crate) async fn get_block(
    env: &Env,
    slot: u64,
    include_rewards: bool,
    require_previous_blockhash: bool,
) -> FetchResult<FetchedBlock> {
    let index_bucket = env.bucket(INDEX_BUCKET_BINDING)?;
    let epoch = epoch_for_slot(slot);
    let slot_lookup = get_slot_lookup(&index_bucket, epoch, slot)
        .await?
        .unwrap_or(SlotLookup {
            range: SlotRange::EMPTY,
            previous_blockhash: None,
        });
    let slot_range = slot_lookup.range;

    if slot_range.len == 0 {
        return Ok(FetchedBlock {
            bytes: Vec::new(),
            previous_blockhash: None,
        });
    }
    let previous_blockhash = slot_lookup.previous_blockhash;
    if require_previous_blockhash && previous_blockhash.is_none() {
        return Err(FetchError::PreviousBlockhashUnavailable { slot });
    }
    if slot_range.len > MAX_CAR_BLOCK_BYTES {
        return Err(FetchError::RangeTooLarge {
            slot,
            len: slot_range.len,
        });
    }

    let car_path = format!("{}/epoch-{}.car", epoch, epoch);
    let mut block_bytes =
        http::get_range(&car_path, slot_range.offset, slot_range.len as usize).await?;
    let missing_reward = if include_rewards {
        missing_block_rewards_entry(&block_bytes)?
    } else {
        None
    };
    if let Some(reward) = missing_reward {
        let rewards_entries =
            fetch_rewards_entry_chain(&index_bucket, epoch, &car_path, &reward.cid)
                .await
                .map_err(|err| FetchError::RewardLookup {
                    reason: err.to_string(),
                })?;
        block_bytes.splice(reward.insert_at..reward.insert_at, rewards_entries);
    }

    Ok(FetchedBlock {
        bytes: block_bytes,
        previous_blockhash,
    })
}

async fn get_slot_lookup(
    index_bucket: &Bucket,
    epoch: u64,
    slot: u64,
) -> FetchResult<Option<SlotLookup>> {
    if let Some(lookup) = get_slot_lookup_from_raw_index(index_bucket, epoch, slot).await? {
        return Ok(Some(lookup));
    }

    get_slot_lookup_from_compact_indexes(index_bucket, epoch, slot).await
}

async fn get_slot_lookup_from_raw_index(
    index_bucket: &Bucket,
    epoch: u64,
    slot: u64,
) -> FetchResult<Option<SlotLookup>> {
    let slot_in_epoch = slot_in_epoch(slot);
    let v2_key = format!("slot-index/epoch-{epoch}-slot-ranges-v2.raw");
    if let Some(bytes) = get_r2_range(
        index_bucket,
        &v2_key,
        slot_range_v2_entry_offset(slot_in_epoch).map_err(|err| {
            FetchError::MalformedSlotIndexEntry {
                key: v2_key.clone(),
                reason: err.to_string(),
            }
        })?,
        SLOT_RANGE_V2_ENTRY_SIZE,
    )
    .await?
    {
        let entry = decode_slot_range_v2_entry(&bytes).map_err(|err| {
            FetchError::MalformedSlotIndexEntry {
                key: v2_key,
                reason: err.to_string(),
            }
        })?;
        return Ok(Some(SlotLookup {
            range: entry.range,
            previous_blockhash: indexed_previous_blockhash(slot, entry.previous_blockhash),
        }));
    }

    let raw_key = format!("slot-index/epoch-{epoch}-slot-ranges.raw");
    if let Some(bytes) = get_r2_range(
        index_bucket,
        &raw_key,
        slot_range_entry_offset(slot_in_epoch).map_err(|err| {
            FetchError::MalformedSlotIndexEntry {
                key: raw_key.clone(),
                reason: err.to_string(),
            }
        })?,
        SLOT_RANGE_ENTRY_SIZE,
    )
    .await?
    {
        let range =
            decode_slot_range_entry(&bytes).map_err(|err| FetchError::MalformedSlotIndexEntry {
                key: raw_key,
                reason: err.to_string(),
            })?;
        return Ok(Some(SlotLookup {
            range,
            previous_blockhash: None,
        }));
    }

    Ok(None)
}

async fn get_slot_lookup_from_compact_indexes(
    index_bucket: &Bucket,
    epoch: u64,
    slot: u64,
) -> FetchResult<Option<SlotLookup>> {
    let epoch_cid_path = format!("{epoch}/epoch-{epoch}.cid");
    let epoch_cid = epoch_cid(index_bucket, epoch).await?;
    let epoch_cid = epoch_cid.trim();
    if epoch_cid.is_empty() {
        return Err(FetchError::MalformedSlotIndexEntry {
            key: epoch_cid_path,
            reason: "empty epoch CID".to_string(),
        });
    }

    let slot_index_path = format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-slot-to-cid.index");
    let cid_index_path =
        format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");

    let missing_indexes =
        missing_index_keys(index_bucket, [&slot_index_path, &cid_index_path]).await?;
    if !missing_indexes.is_empty() {
        return Err(FetchError::IndexUnavailable {
            keys: missing_indexes,
        });
    }

    let car_path = format!("{epoch}/epoch-{epoch}.car");
    let car_prefix = http::get_range(&car_path, 0, CAR_HEADER_PREFIX_READ_LEN).await?;
    let car_header_size = of_slot_ranges::decode_car_header_total_size(&car_prefix, &car_path)
        .map_err(|err| FetchError::MalformedSlotIndexEntry {
            key: car_path.clone(),
            reason: err.to_string(),
        })?;

    let slot_reader = R2RangeReader::new(index_bucket.clone(), slot_index_path.clone());
    let cid_reader = R2RangeReader::new(index_bucket.clone(), cid_index_path.clone());
    let mut slot_index = AsyncCompactIndex::open(slot_reader, slot_index_path.clone())
        .await
        .map_err(|err| FetchError::MalformedSlotIndexEntry {
            key: slot_index_path,
            reason: err.to_string(),
        })?;
    let mut cid_index = AsyncCompactIndex::open(cid_reader, cid_index_path.clone())
        .await
        .map_err(|err| FetchError::MalformedSlotIndexEntry {
            key: cid_index_path,
            reason: err.to_string(),
        })?;

    let Some(end) = lookup_slot_end(&mut slot_index, &mut cid_index, slot).await? else {
        return Ok(None);
    };
    let start = find_previous_slot_end_or_header(
        &mut slot_index,
        &mut cid_index,
        epoch,
        slot,
        car_header_size,
    )
    .await?;

    if end <= start {
        return Ok(None);
    }
    let len = end - start;
    if len > u32::MAX as u64 {
        return Err(FetchError::RangeTooLarge {
            slot,
            len: u32::MAX,
        });
    }

    Ok(Some(SlotLookup {
        range: SlotRange {
            offset: start,
            len: len as u32,
        },
        previous_blockhash: None,
    }))
}

async fn find_previous_slot_end_or_header<S, C>(
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    epoch: u64,
    slot: u64,
    car_header_size: u64,
) -> FetchResult<u64>
where
    S: RangeReader,
    C: RangeReader,
{
    let epoch_start = epoch
        .checked_mul(of_car_reader::slot_ranges::SLOTS_PER_EPOCH)
        .ok_or_else(|| FetchError::MalformedSlotIndexEntry {
            key: format!("epoch {epoch}"),
            reason: "epoch start overflow".to_string(),
        })?;
    let mut previous_slot = slot;
    while previous_slot > epoch_start {
        previous_slot -= 1;
        if let Some(end) = lookup_slot_end(slot_index, cid_index, previous_slot).await? {
            return Ok(end);
        }
    }
    Ok(car_header_size)
}

async fn lookup_slot_end<S, C>(
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    slot: u64,
) -> FetchResult<Option<u64>>
where
    S: RangeReader,
    C: RangeReader,
{
    let mut cid = vec![0u8; slot_index.value_size()];
    let found = slot_index
        .lookup_into_node_reads(&slot.to_le_bytes(), &mut cid)
        .await
        .map_err(|err| FetchError::MalformedSlotIndexEntry {
            key: format!("slot-to-cid for slot {slot}"),
            reason: err.to_string(),
        })?;
    if !found {
        return Ok(None);
    }

    let mut offset_and_size = vec![0u8; cid_index.value_size()];
    let found = cid_index
        .lookup_into_node_reads(&cid, &mut offset_and_size)
        .await
        .map_err(|err| FetchError::MalformedSlotIndexEntry {
            key: format!("cid-to-offset-and-size for slot {slot}"),
            reason: err.to_string(),
        })?;
    if !found {
        return Ok(None);
    }

    let (offset, size) = decode_offset_and_size(&offset_and_size).map_err(|err| {
        FetchError::MalformedSlotIndexEntry {
            key: format!("cid-to-offset-and-size for slot {slot}"),
            reason: err.to_string(),
        }
    })?;
    offset
        .checked_add(size as u64)
        .map(Some)
        .ok_or_else(|| FetchError::MalformedSlotIndexEntry {
            key: format!("cid-to-offset-and-size for slot {slot}"),
            reason: "slot end offset overflow".to_string(),
        })
}

async fn missing_index_keys<'a>(
    index_bucket: &Bucket,
    paths: impl IntoIterator<Item = &'a String>,
) -> FetchResult<Vec<String>> {
    let mut missing = Vec::new();
    for path in paths {
        if index_bucket.head(path).await?.is_none() {
            missing.push(path.clone());
        }
    }
    Ok(missing)
}

async fn epoch_cid(index_bucket: &Bucket, epoch: u64) -> FetchResult<String> {
    let path = format!("{epoch}/epoch-{epoch}.cid");
    if let Some(bytes) = get_r2_bytes(index_bucket, &path).await? {
        return String::from_utf8(bytes).map_err(|err| FetchError::MalformedSlotIndexEntry {
            key: path,
            reason: err.to_string(),
        });
    }

    let cid = http::get_text(&path).await?;
    Ok(cid)
}

async fn get_r2_bytes(index_bucket: &Bucket, path: &str) -> FetchResult<Option<Vec<u8>>> {
    let Some(object) = index_bucket.get(path).execute().await? else {
        return Ok(None);
    };
    let Some(body) = object.body() else {
        return Ok(None);
    };
    Ok(Some(body.bytes().await?))
}

async fn get_r2_range(
    index_bucket: &Bucket,
    path: &str,
    offset: u64,
    len: usize,
) -> FetchResult<Option<Vec<u8>>> {
    let Some(object) = index_bucket
        .get(path)
        .range(R2Range::OffsetWithLength {
            offset,
            length: len as u64,
        })
        .execute()
        .await?
    else {
        return Ok(None);
    };
    let Some(body) = object.body() else {
        return Ok(None);
    };
    let bytes = body.bytes().await?;
    if bytes.len() != len {
        return Err(FetchError::MalformedSlotIndexEntry {
            key: path.to_string(),
            reason: format!("range returned {} bytes, expected {len}", bytes.len()),
        });
    }
    Ok(Some(bytes))
}

fn indexed_previous_blockhash(slot: u64, blockhash: [u8; 32]) -> Option<[u8; 32]> {
    (slot == 0 || blockhash.iter().any(|byte| *byte != 0)).then_some(blockhash)
}

async fn lookup_car_entry_range_for_cid(
    index_bucket: &Bucket,
    epoch: u64,
    cid: &[u8],
) -> AnyhowResult<(u64, u32)> {
    let epoch_cid_path = format!("{epoch}/epoch-{epoch}.cid");
    let epoch_cid = epoch_cid(index_bucket, epoch)
        .await
        .map_err(|err| anyhow!("{epoch_cid_path}: {}", err.client_message()))?;
    let epoch_cid = epoch_cid.trim();
    if epoch_cid.is_empty() {
        return Err(anyhow!("empty epoch cid from {epoch_cid_path}"));
    }

    let cid_index_path =
        format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");
    let cid_reader = R2RangeReader::new(index_bucket.clone(), cid_index_path.clone());
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

async fn fetch_rewards_entry_chain(
    index_bucket: &Bucket,
    epoch: u64,
    car_path: &str,
    root_cid: &CarCid,
) -> AnyhowResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut queue = vec![*root_cid];
    let mut seen = HashSet::<CarCid>::new();

    while let Some(cid) = queue.pop() {
        if !seen.insert(cid) {
            continue;
        }
        let (offset, len) = lookup_car_entry_range_for_cid(index_bucket, epoch, &cid).await?;
        if len > MAX_REWARDS_ENTRY_BYTES {
            return Err(anyhow!(
                "rewards continuation entry is {len} bytes, above {MAX_REWARDS_ENTRY_BYTES}"
            ));
        }
        let entry = http::get_range(car_path, offset, len as usize)
            .await
            .map_err(|err| anyhow!("{car_path}: {}", err.client_message()))?;
        for next in rewards_continuation_cids(&entry)? {
            if !seen.contains(&next) {
                queue.push(next);
            }
        }
        out.extend_from_slice(&entry);
    }

    Ok(out)
}

struct R2RangeReader {
    bucket: Bucket,
    path: String,
}

impl R2RangeReader {
    fn new(bucket: Bucket, path: String) -> Self {
        Self { bucket, path }
    }
}

impl RangeReader for R2RangeReader {
    type ReadFuture<'a>
        = Pin<Box<dyn Future<Output = AnyhowResult<()>> + 'a>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        let path = self.path.as_str();
        let bucket = &self.bucket;
        Box::pin(async move {
            let Some(object) = bucket
                .get(path)
                .range(R2Range::OffsetWithLength {
                    offset,
                    length: out.len() as u64,
                })
                .execute()
                .await
                .map_err(|err| anyhow!("{path}: {err}"))?
            else {
                return Err(anyhow!("{path}: object missing from R2"));
            };
            let Some(body) = object.body() else {
                return Err(anyhow!("{path}: R2 object body missing"));
            };
            let bytes = body.bytes().await?;
            if bytes.len() != out.len() {
                return Err(anyhow!(
                    "{path}: range returned {} bytes, expected {}",
                    bytes.len(),
                    out.len()
                ));
            }
            out.copy_from_slice(&bytes);
            Ok(())
        })
    }
}

struct MissingRewardEntry {
    cid: CarCid,
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

        entry_cids.push(
            car_cid_from_bytes(&car_fragment[cid_start..payload_start]).ok_or_else(|| {
                FetchError::MalformedCarSlice {
                    reason: format!(
                        "CAR CID has {} bytes, expected {CAR_CID_LEN}",
                        payload_start - cid_start
                    ),
                }
            })?,
        );
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
    if entry_cids.contains(&missing_reward.cid) {
        Ok(None)
    } else {
        Ok(Some(missing_reward))
    }
}

fn block_rewards_cid(payload: &[u8]) -> FetchResult<Option<CarCid>> {
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
    let cid = rewards
        .car_cid()
        .ok_or_else(|| FetchError::MalformedCarSlice {
            reason: format!(
                "CID ref has {} bytes and is not an inline identity CID",
                rewards.normalized_bytes().len()
            ),
        })?;
    Ok(Some(cid))
}

fn rewards_continuation_cids(car_entries: &[u8]) -> AnyhowResult<Vec<CarCid>> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    while pos < car_entries.len() {
        let (entry_len, varint_len) =
            read_uvarint64(&car_entries[pos..]).map_err(|err| anyhow!(err.client_message()))?;
        let entry_len =
            usize::try_from(entry_len).map_err(|_| anyhow!("CAR entry length exceeds usize"))?;
        if entry_len < CAR_CID_LEN {
            return Err(anyhow!("CAR entry length is smaller than CID length"));
        }
        let payload_start = pos
            .checked_add(varint_len)
            .and_then(|value| value.checked_add(CAR_CID_LEN))
            .ok_or_else(|| anyhow!("CAR payload offset overflow"))?;
        let next_pos = pos
            .checked_add(varint_len)
            .and_then(|value| value.checked_add(entry_len))
            .ok_or_else(|| anyhow!("CAR entry end overflow"))?;
        if next_pos > car_entries.len() {
            return Err(anyhow!("CAR entry extends past fetched reward range"));
        }
        let payload = &car_entries[payload_start..next_pos];
        out.extend(rewards_payload_next_cids(payload)?);
        pos = next_pos;
    }
    Ok(out)
}

fn rewards_payload_next_cids(payload: &[u8]) -> AnyhowResult<Vec<CarCid>> {
    let node =
        decode_node(payload).map_err(|err| anyhow!("decode rewards continuation node: {err}"))?;
    let next = match node {
        Node::Rewards(rewards) => rewards.data.next,
        Node::DataFrame(frame) => frame.next,
        _ => None,
    };
    let Some(next) = next else {
        return Ok(Vec::new());
    };
    next.iter()
        .map(|cid| {
            let cid = cid.map_err(|err| anyhow!(err.to_string()))?;
            if cid.inline_raw_bytes().is_some() {
                return Ok(None);
            }
            let Some(cid) = cid.car_cid() else {
                return Err(anyhow!(
                    "CID ref has {} bytes and is not an inline identity CID",
                    cid.normalized_bytes().len()
                ));
            };
            Ok(Some(cid))
        })
        .filter_map(|result| result.transpose())
        .collect()
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

#[cfg(test)]
mod tests {
    use super::indexed_previous_blockhash;

    #[test]
    fn zero_previous_blockhash_is_valid_only_for_genesis() {
        assert_eq!(indexed_previous_blockhash(0, [0; 32]), Some([0; 32]));
        assert_eq!(indexed_previous_blockhash(1, [0; 32]), None);
        assert_eq!(indexed_previous_blockhash(1, [7; 32]), Some([7; 32]));
    }
}
