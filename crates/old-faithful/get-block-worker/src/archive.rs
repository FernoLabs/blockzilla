use crate::{
    get_block::{GetBlockConfig, render_get_block_json_bytes, render_get_block_time},
    source::Source,
};
use anyhow::{Context, Result, anyhow, bail};
use of_car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    compact_index::decode_offset_and_size,
    node::{Node, decode_node, peek_node_type},
    slot_ranges::{
        SLOT_RANGE_ENTRY_SIZE, SLOT_RANGE_V2_ENTRY_SIZE, SlotRange, decode_slot_range_entry,
        decode_slot_range_v2_entry, epoch_for_slot, slot_in_epoch, slot_range_entry_offset,
        slot_range_v2_entry_offset,
    },
};
use of_slot_ranges::{AsyncCompactIndex, RangeReader};
use serde_json::Value;
use std::{
    collections::HashSet,
    fs::File,
    future::Future,
    io::{Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
};

const MAX_CAR_BLOCK_BYTES: u32 = 64 * 1024 * 1024;
const MAX_REWARDS_ENTRY_BYTES: u32 = 8 * 1024 * 1024;
const CAR_CID_LEN: usize = 36;

#[derive(Clone)]
pub struct Archive {
    source: Arc<Source>,
    index_dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct GetBlockOptions {
    pub config: GetBlockConfig,
}

#[derive(Clone)]
pub struct FetchedBlock {
    pub bytes: Vec<u8>,
    pub previous_blockhash: Option<[u8; 32]>,
}

struct SlotRangeLookup {
    range: SlotRange,
    previous_blockhash: Option<[u8; 32]>,
}

impl Archive {
    pub fn new(source: Arc<Source>, index_dir: PathBuf) -> Self {
        Self { source, index_dir }
    }

    pub async fn get_block_json(
        &self,
        slot: u64,
        options: GetBlockOptions,
    ) -> Result<Option<Value>> {
        let Some(bytes) = self.get_block_json_bytes(slot, options).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| anyhow!("parse generated getBlock JSON for slot {slot}: {err}"))
    }

    pub async fn get_block_json_bytes(
        &self,
        slot: u64,
        options: GetBlockOptions,
    ) -> Result<Option<Vec<u8>>> {
        let Some(block) = self.fetch_block(slot, options.config.rewards).await? else {
            return Ok(None);
        };

        render_get_block_json_bytes(block.bytes, block.previous_blockhash, options.config)
            .map(Some)
            .map_err(|err| anyhow!("decode slot {slot} into getBlock JSON: {err}"))
    }

    pub async fn get_block_time(&self, slot: u64) -> Result<Option<Value>> {
        let Some(bytes) = self.get_block_time_json_bytes(slot).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| anyhow!("parse generated getBlockTime JSON for slot {slot}: {err}"))
    }

    pub async fn get_block_time_json_bytes(&self, slot: u64) -> Result<Option<Vec<u8>>> {
        let Some(block) = self.fetch_block(slot, false).await? else {
            return Ok(None);
        };
        let block_time = render_get_block_time(block.bytes)
            .map_err(|err| anyhow!("decode slot {slot} blockTime: {err}"))?;
        Ok(Some(match block_time {
            Some(block_time) => block_time.to_string().into_bytes(),
            None => b"null".to_vec(),
        }))
    }

    pub async fn fetch_block_payload(
        &self,
        slot: u64,
        include_rewards: bool,
    ) -> Result<Option<FetchedBlock>> {
        self.fetch_block(slot, include_rewards).await
    }

    async fn fetch_block(&self, slot: u64, include_rewards: bool) -> Result<Option<FetchedBlock>> {
        let epoch = epoch_for_slot(slot);
        let Some(entry) = self.read_slot_index_optional(epoch, slot_in_epoch(slot))? else {
            return Ok(None);
        };
        if entry.range.is_empty() {
            return Ok(None);
        }
        if entry.range.len > MAX_CAR_BLOCK_BYTES {
            bail!(
                "slot {slot} CAR range is {} bytes, above the {MAX_CAR_BLOCK_BYTES} byte limit",
                entry.range.len
            );
        }

        let car_path = car_path(epoch);
        let mut bytes = self
            .source
            .get_range(&car_path, entry.range.offset, entry.range.len as usize)
            .await
            .with_context(|| {
                format!(
                    "read CAR range for slot {slot} at {}+{}",
                    entry.range.offset, entry.range.len
                )
            })?;
        let previous_blockhash = match entry.previous_blockhash {
            Some(previous_blockhash) => Some(previous_blockhash),
            None => self.resolve_previous_blockhash(slot, &bytes).await?,
        };

        if include_rewards && let Some(reward) = missing_block_rewards_entry(&bytes)? {
            let rewards_entries = self
                .fetch_rewards_entry_chain(epoch, &car_path, &reward.cid)
                .await?;
            bytes.splice(reward.insert_at..reward.insert_at, rewards_entries);
        }

        Ok(Some(FetchedBlock {
            bytes,
            previous_blockhash,
        }))
    }

    fn read_slot_index_optional(
        &self,
        epoch: u64,
        slot_in_epoch: u64,
    ) -> Result<Option<SlotRangeLookup>> {
        if let Some(path) = self.find_slot_index_v2_path(epoch) {
            let mut file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
            let range = read_v2_entry_from_file(&mut file, slot_in_epoch)?;
            return Ok(Some(SlotRangeLookup {
                range: range.range,
                previous_blockhash: Some(range.previous_blockhash),
            }));
        }

        let Some(path) = self.find_slot_index_legacy_path(epoch) else {
            return Ok(None);
        };
        let mut file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        Ok(Some(SlotRangeLookup {
            range: read_legacy_entry_from_file(&mut file, slot_in_epoch)?,
            previous_blockhash: None,
        }))
    }

    fn find_slot_index_v2_path(&self, epoch: u64) -> Option<PathBuf> {
        let name = format!("epoch-{epoch}-slot-ranges-v2.raw");
        [
            self.index_dir.join("slot-index").join(&name),
            self.index_dir.join(&name),
        ]
        .into_iter()
        .find(|path| path.is_file())
    }

    fn find_slot_index_legacy_path(&self, epoch: u64) -> Option<PathBuf> {
        let name = format!("epoch-{epoch}-slot-ranges.raw");
        [
            self.index_dir.join("slot-index").join(&name),
            self.index_dir.join(&name),
        ]
        .into_iter()
        .find(|path| path.is_file())
    }

    async fn lookup_car_entry_range_for_cid(&self, epoch: u64, cid: &[u8]) -> Result<(u64, u32)> {
        let epoch_cid_path = format!("{epoch}/epoch-{epoch}.cid");
        let epoch_cid = self.source.get_text(&epoch_cid_path).await?;
        let epoch_cid = epoch_cid.trim();
        if epoch_cid.is_empty() {
            bail!("{epoch_cid_path} was empty");
        }
        let cid_index_path =
            format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");
        let reader = SourceRangeReader::new(Arc::clone(&self.source), cid_index_path.clone());
        let mut index = AsyncCompactIndex::open(reader, cid_index_path).await?;
        let mut value = vec![0u8; index.value_size()];
        if !index.lookup_into_node_reads(cid, &mut value).await? {
            bail!("CID not found in epoch {epoch} cid-to-offset-and-size index");
        }
        Ok(decode_offset_and_size(&value)?)
    }

    async fn fetch_rewards_entry_chain(
        &self,
        epoch: u64,
        car_path: &str,
        root_cid: &[u8],
    ) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut queue = vec![root_cid.to_vec()];
        let mut seen = HashSet::<Vec<u8>>::new();

        while let Some(cid) = queue.pop() {
            if !seen.insert(cid.clone()) {
                continue;
            }
            let (offset, len) = self.lookup_car_entry_range_for_cid(epoch, &cid).await?;
            if len > MAX_REWARDS_ENTRY_BYTES {
                bail!("rewards continuation entry is {len} bytes, above {MAX_REWARDS_ENTRY_BYTES}");
            }
            let entry = self
                .source
                .get_range(car_path, offset, len as usize)
                .await
                .context("read rewards CAR entry")?;
            for next in rewards_continuation_cids(&entry)? {
                if !seen.contains(&next) {
                    queue.push(next);
                }
            }
            out.extend_from_slice(&entry);
        }

        Ok(out)
    }

    async fn resolve_previous_blockhash(
        &self,
        slot: u64,
        block_bytes: &[u8],
    ) -> Result<Option<[u8; 32]>> {
        let Some(parent_slot) = decode_block_parent_slot(block_bytes.to_vec())
            .with_context(|| format!("decode parent slot for slot {slot}"))?
        else {
            return Ok(None);
        };
        if parent_slot == slot {
            return Ok(None);
        }

        let Some(parent_entry) =
            self.read_slot_index_optional(epoch_for_slot(parent_slot), slot_in_epoch(parent_slot))?
        else {
            return Ok(None);
        };
        if parent_entry.range.is_empty() {
            return Ok(None);
        }
        if parent_entry.range.len > MAX_CAR_BLOCK_BYTES {
            bail!(
                "parent slot {parent_slot} CAR range is {} bytes, above the {MAX_CAR_BLOCK_BYTES} byte limit",
                parent_entry.range.len
            );
        }

        let parent_car_path = car_path(epoch_for_slot(parent_slot));
        let parent_bytes = self
            .source
            .get_range(
                &parent_car_path,
                parent_entry.range.offset,
                parent_entry.range.len as usize,
            )
            .await
            .with_context(|| format!("read parent CAR range for slot {parent_slot}"))?;
        decode_blockhash(parent_bytes)
            .map(Some)
            .with_context(|| format!("decode blockhash for parent slot {parent_slot}"))
    }
}

fn car_path(epoch: u64) -> String {
    format!("{epoch}/epoch-{epoch}.car")
}

fn read_v2_entry_from_file(
    file: &mut File,
    slot_in_epoch: u64,
) -> Result<of_car_reader::slot_ranges::SlotRangeWithPreviousBlockhash> {
    let offset = slot_range_v2_entry_offset(slot_in_epoch)?;
    let mut bytes = [0u8; SLOT_RANGE_V2_ENTRY_SIZE];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut bytes)?;
    Ok(decode_slot_range_v2_entry(&bytes)?)
}

fn read_legacy_entry_from_file(file: &mut File, slot_in_epoch: u64) -> Result<SlotRange> {
    let offset = slot_range_entry_offset(slot_in_epoch)?;
    let mut bytes = [0u8; SLOT_RANGE_ENTRY_SIZE];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut bytes)?;
    Ok(decode_slot_range_entry(&bytes)?)
}

fn decode_block_parent_slot(bytes: Vec<u8>) -> Result<Option<u64>> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = CarBlockGroup::without_rewards_and_transaction_payloads();
    if !reader.read_until_block_into(&mut block)? {
        bail!("CAR slice did not contain a block node");
    }
    Ok(block.parent_slot)
}

fn decode_blockhash(bytes: Vec<u8>) -> Result<[u8; 32]> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = CarBlockGroup::without_rewards_and_transaction_payloads();
    if !reader.read_until_block_into(&mut block)? {
        bail!("CAR slice did not contain a block node");
    }
    if !block.has_blockhash {
        bail!("block did not contain an entry hash");
    }
    Ok(block.blockhash)
}

struct MissingRewardEntry {
    cid: Vec<u8>,
    insert_at: usize,
}

fn missing_block_rewards_entry(car_fragment: &[u8]) -> Result<Option<MissingRewardEntry>> {
    let mut pos = 0usize;
    let mut entry_cids = Vec::new();
    let mut missing_reward = None;

    while pos < car_fragment.len() {
        let (entry_len, varint_len) = read_uvarint64(&car_fragment[pos..])?;
        let entry_len = usize::try_from(entry_len).context("CAR entry length exceeds usize")?;
        if entry_len < CAR_CID_LEN {
            bail!("CAR entry length is smaller than CID length");
        }

        let cid_start = pos
            .checked_add(varint_len)
            .ok_or_else(|| anyhow!("CAR entry offset overflow"))?;
        let payload_start = cid_start
            .checked_add(CAR_CID_LEN)
            .ok_or_else(|| anyhow!("CAR payload offset overflow"))?;
        let next_pos = pos
            .checked_add(varint_len)
            .and_then(|value| value.checked_add(entry_len))
            .ok_or_else(|| anyhow!("CAR entry end overflow"))?;
        if next_pos > car_fragment.len() {
            bail!("CAR entry extends past fetched slot range");
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

fn block_rewards_cid(payload: &[u8]) -> Result<Option<Vec<u8>>> {
    if peek_node_type(payload).context("decode CAR node kind")? != 2 {
        return Ok(None);
    }

    let Node::Block(block) = decode_node(payload).context("decode block node")? else {
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
        .map(|cid| Some(cid.to_vec()))
        .ok_or_else(|| {
            anyhow!(
                "CID ref has {} bytes and is not an inline identity CID",
                rewards.normalized_bytes().len()
            )
        })
}

fn rewards_continuation_cids(car_entries: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    while pos < car_entries.len() {
        let (entry_len, varint_len) = read_uvarint64(&car_entries[pos..])?;
        let entry_len = usize::try_from(entry_len).context("CAR entry length exceeds usize")?;
        if entry_len < CAR_CID_LEN {
            bail!("CAR entry length is smaller than CID length");
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
            bail!("CAR entry extends past fetched reward range");
        }
        let payload = &car_entries[payload_start..next_pos];
        out.extend(rewards_payload_next_cids(payload)?);
        pos = next_pos;
    }
    Ok(out)
}

fn rewards_payload_next_cids(payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    let node = decode_node(payload).context("decode rewards continuation node")?;
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
            cid.car_cid_bytes()
                .map(|bytes| Some(bytes.to_vec()))
                .ok_or_else(|| {
                    anyhow!(
                        "CID ref has {} bytes and is not an inline identity CID",
                        cid.normalized_bytes().len()
                    )
                })
        })
        .filter_map(|result| result.transpose())
        .collect()
}

fn read_uvarint64(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;

    for (index, byte) in bytes.iter().take(10).copied().enumerate() {
        if byte < 0x80 {
            if index == 9 && byte > 1 {
                bail!("CAR entry length varint overflows u64");
            }
            value |= (byte as u64) << shift;
            return Ok((value, index + 1));
        }

        value |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }

    bail!("unterminated CAR entry length varint")
}

struct SourceRangeReader {
    source: Arc<Source>,
    path: String,
}

impl SourceRangeReader {
    fn new(source: Arc<Source>, path: String) -> Self {
        Self { source, path }
    }
}

impl RangeReader for SourceRangeReader {
    type ReadFuture<'a>
        = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        let source = Arc::clone(&self.source);
        let path = self.path.clone();
        Box::pin(async move {
            let bytes = source.get_range(&path, offset, out.len()).await?;
            out.copy_from_slice(&bytes);
            Ok(())
        })
    }
}
