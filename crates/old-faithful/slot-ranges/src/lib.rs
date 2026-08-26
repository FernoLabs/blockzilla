use anyhow::{Result, anyhow};
use of_car_reader::compact_index::{
    BUCKET_HEADER_SIZE, CompactIndexHeader, CompactIndexMeta, bst_lookup, bucket_hash,
    decode_offset_and_size, truncate_entry_hash,
};
use of_car_reader::slot_ranges::{SLOTS_PER_EPOCH, SlotRange};
use of_car_reader::{
    CarBlockReader,
    node::{Node, decode_node},
    reconstruct::Cid36,
};
use std::collections::{HashMap, HashSet};
use std::fmt;
#[cfg(any(not(target_arch = "wasm32"), test))]
use std::future::{Ready, ready};

pub const DEFAULT_MAX_BUCKET_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct BuildSlotRangesConfig {
    pub max_bucket_payload_bytes: usize,
    pub allow_node_read_fallback: bool,
}

impl Default for BuildSlotRangesConfig {
    fn default() -> Self {
        Self {
            max_bucket_payload_bytes: DEFAULT_MAX_BUCKET_PAYLOAD_BYTES,
            allow_node_read_fallback: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BuildSlotRangesStats {
    pub present_slots: u32,
    pub slot_bucket_payload_bytes_read: u64,
    pub cid_bucket_payload_bytes_read: u64,
    pub max_slot_bucket_payload_bytes: usize,
    pub max_cid_bucket_payload_bytes: usize,
    pub slot_node_read_fallbacks: u64,
    pub cid_node_read_fallbacks: u64,
}

#[derive(Clone, Debug)]
pub struct BuildSlotRangesOutput {
    pub ranges: Vec<SlotRange>,
    /// Slots present in the Old Faithful slot-to-CID index, in epoch order.
    ///
    /// This can be larger than the number of non-empty raw ranges because some
    /// blocks may occupy no byte range after range reconstruction. Keep it for
    /// consumers that need blockhash ordering.
    pub block_slots: Vec<u64>,
    pub stats: BuildSlotRangesStats,
}

#[derive(Clone, Debug)]
pub struct BuildBlockSlotsStats {
    pub present_slots: u32,
    pub unique_block_slots: u32,
    pub slot_bucket_payload_bytes_read: u64,
    pub max_slot_bucket_payload_bytes: usize,
    pub slot_node_read_fallbacks: u64,
}

#[derive(Clone, Debug)]
pub struct BuildBlockSlotsOutput {
    pub block_slots: Vec<u64>,
    pub stats: BuildBlockSlotsStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockSlotCandidate {
    pub slot: u64,
    pub cid: [u8; 36],
}

#[derive(Clone, Debug)]
pub struct BuildBlockSlotCandidatesOutput {
    pub candidates: Vec<BlockSlotCandidate>,
    pub stats: BuildBlockSlotsStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AmbiguousBlockCidError {
    pub cid: [u8; 36],
    pub candidate_slots: Vec<u64>,
}

impl fmt::Display for AmbiguousBlockCidError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "slot-to-CID index maps one CID to multiple candidate slots {}; the compact hash match is ambiguous",
            self.candidate_slots
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl std::error::Error for AmbiguousBlockCidError {}

pub trait RangeReader {
    type ReadFuture<'a>: Future<Output = Result<()>> + 'a
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a>;
}

pub async fn build_block_slot_candidates_from_slot_index<S>(
    epoch: u64,
    slot_index: &mut AsyncCompactIndex<S>,
    config: BuildSlotRangesConfig,
) -> Result<BuildBlockSlotCandidatesOutput>
where
    S: RangeReader,
{
    if slot_index.version() != 1 {
        return Err(anyhow!(
            "unsupported compact index version slot={}",
            slot_index.version()
        ));
    }
    if slot_index.value_size() != 36 {
        return Err(anyhow!(
            "unsupported slot index value_size={} (expected 36-byte CID)",
            slot_index.value_size()
        ));
    }

    let epoch_start_slot = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow"))?;
    let cid_value_size = slot_index.value_size();
    let bitset_len = (SLOTS_PER_EPOCH as usize).div_ceil(8);
    let mut stats = BuildBlockSlotsStats {
        present_slots: 0,
        unique_block_slots: 0,
        slot_bucket_payload_bytes_read: 0,
        max_slot_bucket_payload_bytes: 0,
        slot_node_read_fallbacks: 0,
    };

    let mut slot_groups = Vec::with_capacity(SLOTS_PER_EPOCH as usize);
    for i in 0..SLOTS_PER_EPOCH {
        let slot = epoch_start_slot + i;
        let bucket = bucket_hash(&slot.to_le_bytes(), slot_index.num_buckets());
        slot_groups.push((bucket, i as u32));
    }
    slot_groups.sort_unstable_by_key(|(bucket, slot)| (*bucket, *slot));

    let mut slot_has_cid = vec![0u8; bitset_len];
    let mut slot_cids = vec![0u8; (SLOTS_PER_EPOCH as usize) * cid_value_size];
    let mut bucket_buf = Vec::new();

    let mut group_start = 0usize;
    while group_start < slot_groups.len() {
        let bucket = slot_groups[group_start].0;
        let mut group_end = group_start + 1;
        while group_end < slot_groups.len() && slot_groups[group_end].0 == bucket {
            group_end += 1;
        }

        let header = slot_index.meta().bucket_header(bucket)?;
        let hash_len = header.hash_len as usize;
        let payload_len =
            bucket_payload_len(hash_len, slot_index.value_size(), header.num_entries)?;
        stats.max_slot_bucket_payload_bytes = stats.max_slot_bucket_payload_bytes.max(payload_len);

        if payload_len > config.max_bucket_payload_bytes {
            if !config.allow_node_read_fallback {
                return Err(anyhow!(
                    "{} bucket {bucket} payload is {} bytes, over configured cap {}",
                    slot_index.source(),
                    payload_len,
                    config.max_bucket_payload_bytes
                ));
            }

            for &(_, i) in &slot_groups[group_start..group_end] {
                let slot = epoch_start_slot + i as u64;
                let key = slot.to_le_bytes();
                let out = &mut slot_cids
                    [(i as usize) * cid_value_size..(i as usize + 1) * cid_value_size];
                if slot_index.lookup_into_node_reads(&key, out).await? {
                    set_bit(&mut slot_has_cid, i as usize);
                }
                stats.slot_node_read_fallbacks += 1;
            }
        } else {
            bucket_buf.clear();
            bucket_buf.resize(payload_len, 0);
            slot_index
                .read_bucket_payload_into(bucket, &mut bucket_buf)
                .await?;
            stats.slot_bucket_payload_bytes_read += payload_len as u64;

            for &(_, i) in &slot_groups[group_start..group_end] {
                let slot = epoch_start_slot + i as u64;
                let key = slot.to_le_bytes();
                let target = truncate_entry_hash(header.hash_domain, &key, hash_len);
                if let Some(value) = bst_lookup(
                    &bucket_buf,
                    header.num_entries,
                    hash_len,
                    cid_value_size,
                    target,
                ) {
                    let out = &mut slot_cids
                        [(i as usize) * cid_value_size..(i as usize + 1) * cid_value_size];
                    out.copy_from_slice(value);
                    set_bit(&mut slot_has_cid, i as usize);
                }
            }
        }

        group_start = group_end;
    }

    let mut candidates = Vec::new();
    let mut unique_block_cids = HashSet::new();
    for i in 0..SLOTS_PER_EPOCH as usize {
        if !get_bit(&slot_has_cid, i) {
            continue;
        }
        stats.present_slots += 1;
        let cid: [u8; 36] = slot_cids[i * cid_value_size..(i + 1) * cid_value_size]
            .try_into()
            .expect("slot index value size was checked");
        let slot = epoch_start_slot + i as u64;
        unique_block_cids.insert(cid);
        candidates.push(BlockSlotCandidate { slot, cid });
    }
    stats.unique_block_slots = u32::try_from(unique_block_cids.len())
        .map_err(|_| anyhow!("unique block slot count exceeds u32"))?;

    Ok(BuildBlockSlotCandidatesOutput { candidates, stats })
}

pub async fn build_block_slots_from_slot_index<S>(
    epoch: u64,
    slot_index: &mut AsyncCompactIndex<S>,
    config: BuildSlotRangesConfig,
) -> Result<BuildBlockSlotsOutput>
where
    S: RangeReader,
{
    let output = build_block_slot_candidates_from_slot_index(epoch, slot_index, config).await?;
    let selected = select_block_slot_candidates(epoch, &output.candidates, None)?;
    Ok(BuildBlockSlotsOutput {
        block_slots: selected
            .into_iter()
            .map(|candidate| candidate.slot)
            .collect(),
        stats: output.stats,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub struct LocalFileRangeReader {
    file: std::fs::File,
}

#[cfg(not(target_arch = "wasm32"))]
impl LocalFileRangeReader {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        use anyhow::Context;

        let path = path.as_ref();
        let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        Ok(Self { file })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl RangeReader for LocalFileRangeReader {
    type ReadFuture<'a>
        = Ready<Result<()>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        use anyhow::Context;
        use std::os::unix::fs::FileExt;

        ready(
            self.file
                .read_exact_at(out, offset)
                .with_context(|| format!("read {} bytes at offset {offset}", out.len())),
        )
    }
}

pub struct AsyncCompactIndex<R> {
    source: String,
    meta: CompactIndexMeta,
    reader: R,
}

impl<R: RangeReader> AsyncCompactIndex<R> {
    pub async fn open(mut reader: R, source: impl Into<String>) -> Result<Self> {
        let source = source.into();
        let mut fixed = vec![0u8; of_car_reader::compact_index::COMPACT_INDEX_FIXED_HEADER_SIZE];
        reader.read_exact_at(0, &mut fixed).await?;

        let header = CompactIndexMeta::parse_header_prefix(&fixed, &source)?;
        let bucket_headers_len = bucket_headers_len(header)?;
        let mut bucket_headers = vec![0u8; bucket_headers_len];
        reader
            .read_exact_at(header.total_header_size, &mut bucket_headers)
            .await?;
        let meta = CompactIndexMeta::parse_bucket_headers(header, &bucket_headers, &source)?;

        Ok(Self {
            source,
            meta,
            reader,
        })
    }

    #[inline]
    pub fn value_size(&self) -> usize {
        self.meta.value_size()
    }

    #[inline]
    pub fn num_buckets(&self) -> u32 {
        self.meta.num_buckets()
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.meta.version()
    }

    #[inline]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[inline]
    pub fn meta(&self) -> &CompactIndexMeta {
        &self.meta
    }

    pub async fn read_bucket_payload_into(&mut self, bucket: u32, buf: &mut [u8]) -> Result<()> {
        let header = self.meta.bucket_header(bucket)?;
        self.reader.read_exact_at(header.data_offset, buf).await
    }

    pub async fn lookup_into_node_reads(&mut self, key: &[u8], out: &mut [u8]) -> Result<bool> {
        if out.len() != self.value_size() {
            return Err(anyhow!(
                "lookup output length {} does not match index value size {}",
                out.len(),
                self.value_size()
            ));
        }

        let bucket = bucket_hash(key, self.num_buckets());
        let header = self.meta.bucket_header(bucket)?;
        let hash_len = header.hash_len as usize;
        let stride = hash_len
            .checked_add(self.value_size())
            .ok_or_else(|| anyhow!("compact index stride overflow"))?;
        let entries = header.num_entries as usize;

        if entries == 0 {
            return Ok(false);
        }

        let target = truncate_entry_hash(header.hash_domain, key, hash_len);
        let mut index = 0usize;
        let mut node = vec![0u8; stride];

        while index < entries {
            let offset = header
                .data_offset
                .checked_add((index * stride) as u64)
                .ok_or_else(|| anyhow!("compact index node offset overflow"))?;
            self.reader.read_exact_at(offset, &mut node).await?;

            let hash = read_hash(&node[..hash_len]);
            if hash == target {
                out.copy_from_slice(&node[hash_len..hash_len + self.value_size()]);
                return Ok(true);
            }

            index = (index << 1) | 1;
            if hash < target {
                index += 1;
            }
        }

        Ok(false)
    }
}

pub async fn build_slot_ranges_from_indexes<S, C>(
    epoch: u64,
    car_header_size: u64,
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    config: BuildSlotRangesConfig,
) -> Result<BuildSlotRangesOutput>
where
    S: RangeReader,
    C: RangeReader,
{
    build_slot_ranges_from_indexes_inner(
        epoch,
        car_header_size,
        slot_index,
        cid_index,
        config,
        None,
    )
    .await
}

pub async fn build_slot_ranges_from_indexes_with_block_slots<S, C>(
    epoch: u64,
    car_header_size: u64,
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    config: BuildSlotRangesConfig,
    resolved_block_slots: &[u64],
) -> Result<BuildSlotRangesOutput>
where
    S: RangeReader,
    C: RangeReader,
{
    build_slot_ranges_from_indexes_inner(
        epoch,
        car_header_size,
        slot_index,
        cid_index,
        config,
        Some(resolved_block_slots),
    )
    .await
}

async fn build_slot_ranges_from_indexes_inner<S, C>(
    epoch: u64,
    car_header_size: u64,
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    config: BuildSlotRangesConfig,
    resolved_block_slots: Option<&[u64]>,
) -> Result<BuildSlotRangesOutput>
where
    S: RangeReader,
    C: RangeReader,
{
    if slot_index.version() != 1 || cid_index.version() != 1 {
        return Err(anyhow!(
            "unsupported compact index version slot={} cid={}",
            slot_index.version(),
            cid_index.version()
        ));
    }
    if cid_index.value_size() != 9 {
        return Err(anyhow!(
            "unsupported cid index value_size={} (expected 9)",
            cid_index.value_size()
        ));
    }

    let epoch_start_slot = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow"))?;
    let bitset_len = (SLOTS_PER_EPOCH as usize).div_ceil(8);
    let candidate_output =
        build_block_slot_candidates_from_slot_index(epoch, slot_index, config).await?;
    let selected_candidates =
        select_block_slot_candidates(epoch, &candidate_output.candidates, resolved_block_slots)?;
    let mut stats = BuildSlotRangesStats {
        present_slots: u32::try_from(selected_candidates.len())
            .map_err(|_| anyhow!("resolved block slot count exceeds u32"))?,
        slot_bucket_payload_bytes_read: candidate_output.stats.slot_bucket_payload_bytes_read,
        max_slot_bucket_payload_bytes: candidate_output.stats.max_slot_bucket_payload_bytes,
        slot_node_read_fallbacks: candidate_output.stats.slot_node_read_fallbacks,
        ..BuildSlotRangesStats::default()
    };
    let mut bucket_buf = Vec::new();

    let mut cid_groups = Vec::new();
    let block_slots = selected_candidates
        .iter()
        .map(|candidate| candidate.slot)
        .collect::<Vec<_>>();
    for (candidate_index, candidate) in selected_candidates.iter().enumerate() {
        let bucket = bucket_hash(&candidate.cid, cid_index.num_buckets());
        cid_groups.push((bucket, candidate_index));
    }
    cid_groups.sort_unstable_by_key(|(bucket, candidate)| {
        (*bucket, selected_candidates[*candidate].slot)
    });

    let mut slot_has_end = vec![0u8; bitset_len];
    let mut slot_end_excl_abs = vec![0u64; SLOTS_PER_EPOCH as usize];

    let mut group_start = 0usize;
    while group_start < cid_groups.len() {
        let bucket = cid_groups[group_start].0;
        let mut group_end = group_start + 1;
        while group_end < cid_groups.len() && cid_groups[group_end].0 == bucket {
            group_end += 1;
        }

        let header = cid_index.meta().bucket_header(bucket)?;
        let hash_len = header.hash_len as usize;
        let payload_len = bucket_payload_len(hash_len, cid_index.value_size(), header.num_entries)?;
        stats.max_cid_bucket_payload_bytes = stats.max_cid_bucket_payload_bytes.max(payload_len);

        if payload_len > config.max_bucket_payload_bytes {
            if !config.allow_node_read_fallback {
                return Err(anyhow!(
                    "{} bucket {bucket} payload is {} bytes, over configured cap {}",
                    cid_index.source(),
                    payload_len,
                    config.max_bucket_payload_bytes
                ));
            }

            let mut out = vec![0u8; cid_index.value_size()];
            for &(_, candidate_index) in &cid_groups[group_start..group_end] {
                let candidate = selected_candidates[candidate_index];
                let slot_in_epoch = usize::try_from(candidate.slot - epoch_start_slot)
                    .map_err(|_| anyhow!("slot-in-epoch exceeds usize"))?;
                if cid_index
                    .lookup_into_node_reads(&candidate.cid, &mut out)
                    .await?
                    && out.len() == 9
                {
                    let (offset, size) = decode_offset_and_size(&out)?;
                    slot_end_excl_abs[slot_in_epoch] = offset
                        .checked_add(size as u64)
                        .ok_or_else(|| anyhow!("overflow end_excl_abs"))?;
                    set_bit(&mut slot_has_end, slot_in_epoch);
                }
                stats.cid_node_read_fallbacks += 1;
            }
        } else {
            bucket_buf.clear();
            bucket_buf.resize(payload_len, 0);
            cid_index
                .read_bucket_payload_into(bucket, &mut bucket_buf)
                .await?;
            stats.cid_bucket_payload_bytes_read += payload_len as u64;

            for &(_, candidate_index) in &cid_groups[group_start..group_end] {
                let candidate = selected_candidates[candidate_index];
                let slot_in_epoch = usize::try_from(candidate.slot - epoch_start_slot)
                    .map_err(|_| anyhow!("slot-in-epoch exceeds usize"))?;
                let target = truncate_entry_hash(header.hash_domain, &candidate.cid, hash_len);

                if let Some(value) = bst_lookup(
                    &bucket_buf,
                    header.num_entries,
                    hash_len,
                    cid_index.value_size(),
                    target,
                ) && value.len() == 9
                {
                    let (offset, size) = decode_offset_and_size(value)?;
                    slot_end_excl_abs[slot_in_epoch] = offset
                        .checked_add(size as u64)
                        .ok_or_else(|| anyhow!("overflow end_excl_abs"))?;
                    set_bit(&mut slot_has_end, slot_in_epoch);
                }
            }
        }

        group_start = group_end;
    }

    let mut ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
    for candidate in &selected_candidates {
        let slot_in_epoch = usize::try_from(candidate.slot - epoch_start_slot)
            .map_err(|_| anyhow!("slot-in-epoch exceeds usize"))?;
        if !get_bit(&slot_has_end, slot_in_epoch) {
            return Err(anyhow!(
                "CID-to-offset index has no entry for selected block slot {}",
                candidate.slot
            ));
        }
    }

    let mut prev_end_excl_abs: Option<u64> = None;
    for i in 0..SLOTS_PER_EPOCH as usize {
        if !get_bit(&slot_has_end, i) {
            continue;
        }

        let cur_end_excl_abs = slot_end_excl_abs[i];
        ranges[i] = canonical_range_for_block_end(
            car_header_size,
            &mut prev_end_excl_abs,
            cur_end_excl_abs,
            epoch_start_slot + i as u64,
        )?;
    }

    Ok(BuildSlotRangesOutput {
        ranges,
        block_slots,
        stats,
    })
}

fn canonical_range_for_block_end(
    car_header_size: u64,
    previous_end: &mut Option<u64>,
    current_end: u64,
    slot: u64,
) -> Result<SlotRange> {
    let start = previous_end.unwrap_or(car_header_size);
    if current_end <= start {
        // The block node is already inside an earlier canonical byte range.
        // It stays in the blockhash chain, but its range row is all zero.
        return Ok(SlotRange::EMPTY);
    }
    let len = current_end - start;
    if len > u32::MAX as u64 {
        return Err(anyhow!(
            "CAR range length {len} for slot {slot} exceeds u32"
        ));
    }
    *previous_end = Some(current_end);
    Ok(SlotRange {
        offset: start,
        len: len as u32,
    })
}

fn select_block_slot_candidates<'a>(
    epoch: u64,
    candidates: &'a [BlockSlotCandidate],
    resolved_block_slots: Option<&[u64]>,
) -> Result<Vec<&'a BlockSlotCandidate>> {
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow"))?;
    let resolved = resolved_block_slots
        .map(|slots| {
            let mut previous = None;
            let mut set = HashSet::with_capacity(slots.len());
            for (position, slot) in slots.iter().copied().enumerate() {
                if !(epoch_start..epoch_end).contains(&slot) {
                    return Err(anyhow!(
                        "resolved block slot {slot} at position {position} is outside epoch {epoch}"
                    ));
                }
                if previous.is_some_and(|prior| slot <= prior) {
                    return Err(anyhow!(
                        "resolved block slots are not strictly increasing at position {position}: {slot} follows {}",
                        previous.unwrap()
                    ));
                }
                set.insert(slot);
                previous = Some(slot);
            }
            Ok(set)
        })
        .transpose()?;

    let mut groups: HashMap<[u8; 36], Vec<&BlockSlotCandidate>> = HashMap::new();
    for candidate in candidates {
        groups.entry(candidate.cid).or_default().push(candidate);
    }

    let mut selected = Vec::with_capacity(groups.len());
    for group in groups.values() {
        let chosen = match &resolved {
            None if group.len() == 1 => group[0],
            None => {
                return Err(AmbiguousBlockCidError {
                    cid: group[0].cid,
                    candidate_slots: group.iter().map(|candidate| candidate.slot).collect(),
                }
                .into());
            }
            Some(resolved) => {
                let mut chosen = group
                    .iter()
                    .copied()
                    .filter(|candidate| resolved.contains(&candidate.slot));
                let first = chosen.next().ok_or_else(|| {
                    anyhow!(
                        "resolved block slots select no candidate from CID group {}",
                        display_candidate_slots(group)
                    )
                })?;
                if chosen.next().is_some() {
                    return Err(anyhow!(
                        "resolved block slots select multiple candidates from CID group {}",
                        display_candidate_slots(group)
                    ));
                }
                first
            }
        };
        selected.push(chosen);
    }
    selected.sort_unstable_by_key(|candidate| candidate.slot);

    if let Some(resolved_slots) = resolved_block_slots {
        let selected_slots = selected
            .iter()
            .map(|candidate| candidate.slot)
            .collect::<Vec<_>>();
        if selected_slots != resolved_slots {
            return Err(anyhow!(
                "resolved block slot list does not match one candidate from every CID group"
            ));
        }
    }
    Ok(selected)
}

fn display_candidate_slots(candidates: &[&BlockSlotCandidate]) -> String {
    candidates
        .iter()
        .map(|candidate| candidate.slot.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn decode_block_slot_from_car_frame(expected_cid: &[u8], frame: &[u8]) -> Result<u64> {
    let mut reader = CarBlockReader::with_capacity(std::io::Cursor::new(frame), frame.len().max(1));
    let mut scratch = Vec::new();
    let entry = reader
        .read_entry_payload_with_scratch(&mut scratch)?
        .ok_or_else(|| anyhow!("CAR frame is empty"))?;
    if entry.total_len != frame.len() {
        return Err(anyhow!(
            "CID index frame has {} bytes but the CAR entry uses {}",
            frame.len(),
            entry.total_len
        ));
    }
    verify_block_entry_slot(expected_cid, entry.cid, entry.payload)
}

fn verify_block_entry_slot(expected_cid: &[u8], frame_cid: Cid36, payload: &[u8]) -> Result<u64> {
    if expected_cid.len() != 36 {
        return Err(anyhow!(
            "slot-to-CID value has {} bytes, expected 36",
            expected_cid.len()
        ));
    }
    if frame_cid.car_bytes().as_slice() != expected_cid {
        return Err(anyhow!("CAR frame CID differs from the slot-to-CID value"));
    }
    let recomputed = Cid36::compute(payload);
    if recomputed.car_bytes().as_slice() != expected_cid {
        return Err(anyhow!(
            "CAR frame payload does not recompute to the expected CID"
        ));
    }
    match decode_node(payload)? {
        Node::Block(block) => Ok(block.slot),
        _ => Err(anyhow!("CID index frame does not decode as a block node")),
    }
}

pub fn decode_car_header_total_size(prefix: &[u8], source: &str) -> Result<u64> {
    if prefix.len() < 10 {
        return Err(anyhow!(
            "car header prefix from {source} is too short ({} bytes), need at least 10",
            prefix.len()
        ));
    }

    let (header_len, varint_len) = decode_uvarint64(&prefix[..10])
        .ok_or_else(|| anyhow!("could not decode car header length from {source}"))?;

    (varint_len as u64)
        .checked_add(header_len)
        .ok_or_else(|| anyhow!("car header size overflow for {source}"))
}

#[inline]
fn bucket_payload_len(hash_len: usize, value_size: usize, entries: u32) -> Result<usize> {
    hash_len
        .checked_add(value_size)
        .and_then(|stride| stride.checked_mul(entries as usize))
        .ok_or_else(|| anyhow!("bucket payload size overflow"))
}

fn bucket_headers_len(header: CompactIndexHeader) -> Result<usize> {
    (header.num_buckets as usize)
        .checked_mul(BUCKET_HEADER_SIZE)
        .ok_or_else(|| anyhow!("bucket headers size overflow"))
}

#[inline(always)]
fn set_bit(bitset: &mut [u8], index: usize) {
    bitset[index / 8] |= 1 << (index % 8);
}

#[inline(always)]
fn get_bit(bitset: &[u8], index: usize) -> bool {
    (bitset[index / 8] & (1 << (index % 8))) != 0
}

#[inline]
fn read_hash(bytes: &[u8]) -> u64 {
    let mut value = 0u64;
    for (index, byte) in bytes.iter().enumerate() {
        value |= (*byte as u64) << (index * 8);
    }
    value
}

#[inline]
fn decode_uvarint64(buf: &[u8]) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;

    for (i, &b) in buf.iter().take(10).enumerate() {
        if b < 0x80 {
            if i == 9 && b > 1 {
                return None;
            }
            x |= (b as u64) << s;
            return Some((x, i + 1));
        } else {
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct MemoryRangeReader {
        bytes: Vec<u8>,
        max_read: Rc<RefCell<usize>>,
    }

    impl RangeReader for MemoryRangeReader {
        type ReadFuture<'a>
            = Ready<Result<()>>
        where
            Self: 'a;

        fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
            let previous = *self.max_read.borrow();
            *self.max_read.borrow_mut() = previous.max(out.len());
            let start = offset as usize;
            let end = start + out.len();
            if end > self.bytes.len() {
                return ready(Err(anyhow!("test range out of bounds")));
            }
            out.copy_from_slice(&self.bytes[start..end]);
            ready(Ok(()))
        }
    }

    #[test]
    fn decodes_car_header_size() {
        assert_eq!(
            decode_car_header_total_size(&[4, 0, 0, 0, 0, 0, 0, 0, 0, 0], "x").unwrap(),
            5
        );
    }

    #[test]
    fn canonical_range_is_empty_when_block_end_does_not_advance() {
        let mut previous_end = Some(200);
        assert_eq!(
            canonical_range_for_block_end(59, &mut previous_end, 150, 7).unwrap(),
            SlotRange::EMPTY
        );
        assert_eq!(previous_end, Some(200));
        assert_eq!(
            canonical_range_for_block_end(59, &mut previous_end, 250, 8).unwrap(),
            SlotRange {
                offset: 200,
                len: 50
            }
        );
        assert_eq!(previous_end, Some(250));
    }

    #[test]
    fn async_compact_index_reads_bounded_bucket() {
        let max_read = Rc::new(RefCell::new(0usize));
        let bytes = tiny_compact_index(b"slot-key", &[1, 2, 3]);
        let reader = MemoryRangeReader {
            bytes,
            max_read: Rc::clone(&max_read),
        };
        let mut index =
            futures::executor::block_on(AsyncCompactIndex::open(reader, "mem")).unwrap();
        let mut out = [0u8; 3];
        assert!(
            futures::executor::block_on(index.lookup_into_node_reads(b"slot-key", &mut out))
                .unwrap()
        );
        assert_eq!(out, [1, 2, 3]);
        assert_eq!(
            *max_read.borrow(),
            of_car_reader::compact_index::COMPACT_INDEX_FIXED_HEADER_SIZE
        );
    }

    #[test]
    fn block_slot_builder_rejects_one_cid_for_multiple_candidate_slots() {
        let slot_reader = MemoryRangeReader {
            bytes: ambiguous_slot_compact_index(&[1; 36]),
            max_read: Rc::new(RefCell::new(0)),
        };
        let mut slot_index =
            futures::executor::block_on(AsyncCompactIndex::open(slot_reader, "ambiguous-slot"))
                .expect("open slot index");
        let error = futures::executor::block_on(build_block_slots_from_slot_index(
            0,
            &mut slot_index,
            BuildSlotRangesConfig::default(),
        ))
        .expect_err("duplicate CID candidate must fail");
        assert!(error.to_string().contains("multiple candidate slots 0, 1"));
    }

    #[test]
    fn range_builder_rejects_one_cid_for_multiple_candidate_slots() {
        let slot_reader = MemoryRangeReader {
            bytes: ambiguous_slot_compact_index(&[1; 36]),
            max_read: Rc::new(RefCell::new(0)),
        };
        let cid_reader = MemoryRangeReader {
            bytes: tiny_compact_index(&[1; 36], &[0; 9]),
            max_read: Rc::new(RefCell::new(0)),
        };
        let mut slot_index =
            futures::executor::block_on(AsyncCompactIndex::open(slot_reader, "ambiguous-slot"))
                .expect("open slot index");
        let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(cid_reader, "cid"))
            .expect("open CID index");
        let error = futures::executor::block_on(build_slot_ranges_from_indexes(
            0,
            59,
            &mut slot_index,
            &mut cid_index,
            BuildSlotRangesConfig::default(),
        ))
        .expect_err("duplicate CID candidate must fail");
        assert!(error.to_string().contains("multiple candidate slots 0, 1"));
    }

    #[test]
    fn block_slot_builder_requires_full_cid_values() {
        let slot_reader = MemoryRangeReader {
            bytes: tiny_compact_index(&0u64.to_le_bytes(), &[1, 2, 3]),
            max_read: Rc::new(RefCell::new(0)),
        };
        let mut slot_index =
            futures::executor::block_on(AsyncCompactIndex::open(slot_reader, "short-cid"))
                .expect("open slot index");
        let error = futures::executor::block_on(build_block_slots_from_slot_index(
            0,
            &mut slot_index,
            BuildSlotRangesConfig::default(),
        ))
        .expect_err("short CID values must fail");
        assert!(error.to_string().contains("expected 36-byte CID"));
    }

    #[test]
    fn exact_car_frame_proves_block_slot_and_cid() {
        let payload = block_payload(7);
        let cid = *Cid36::compute(&payload).car_bytes();
        let frame = car_frame(cid, &payload);
        assert_eq!(decode_block_slot_from_car_frame(&cid, &frame).unwrap(), 7);

        let mut wrong_frame_cid = frame.clone();
        wrong_frame_cid[36] ^= 1;
        let error = decode_block_slot_from_car_frame(&cid, &wrong_frame_cid)
            .expect_err("wrong frame CID must fail");
        assert!(error.to_string().contains("CID differs"));

        let mut changed_payload = frame.clone();
        *changed_payload.last_mut().unwrap() ^= 1;
        let error = decode_block_slot_from_car_frame(&cid, &changed_payload)
            .expect_err("changed payload must fail its CID");
        assert!(error.to_string().contains("does not recompute"));

        let non_block_payload = [0x84, 0x03, 0x00, 0x00, 0x80];
        let non_block_cid = *Cid36::compute(&non_block_payload).car_bytes();
        let non_block_frame = car_frame(non_block_cid, &non_block_payload);
        let error = decode_block_slot_from_car_frame(&non_block_cid, &non_block_frame)
            .expect_err("non-block node must fail");
        assert!(error.to_string().contains("does not decode as a block"));
    }

    fn block_payload(slot: u8) -> Vec<u8> {
        assert!(slot < 24);
        vec![0x86, 0x02, slot, 0x80, 0x80, 0x83, 0xf6, 0xf6, 0xf6, 0xf6]
    }

    fn car_frame(cid: [u8; 36], payload: &[u8]) -> Vec<u8> {
        let entry_len = cid.len() + payload.len();
        assert!(entry_len < 128);
        let mut frame = Vec::with_capacity(entry_len + 1);
        frame.push(entry_len as u8);
        frame.extend_from_slice(&cid);
        frame.extend_from_slice(payload);
        frame
    }

    fn ambiguous_slot_compact_index(value: &[u8]) -> Vec<u8> {
        let hash_domain = 11u32;
        let header_len = 13u32;
        let bucket_count = 1u32;
        let fixed_len = of_car_reader::compact_index::COMPACT_INDEX_FIXED_HEADER_SIZE;
        let data_offset = fixed_len + BUCKET_HEADER_SIZE;

        let mut out = Vec::new();
        out.extend_from_slice(of_car_reader::compact_index::COMPACT_INDEX_MAGIC);
        out.extend_from_slice(&header_len.to_le_bytes());
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(&bucket_count.to_le_bytes());
        out.push(1);

        let mut bucket_header = [0u8; BUCKET_HEADER_SIZE];
        bucket_header[0..4].copy_from_slice(&hash_domain.to_le_bytes());
        bucket_header[4..8].copy_from_slice(&1u32.to_le_bytes());
        bucket_header[8] = 0;
        bucket_header[10..16].copy_from_slice(&(data_offset as u64).to_le_bytes()[..6]);
        out.extend_from_slice(&bucket_header);
        out.extend_from_slice(value);
        out
    }

    fn tiny_compact_index(key: &[u8], value: &[u8]) -> Vec<u8> {
        let hash_domain = 11u32;
        let hash_len = 8u8;
        let header_len = 13u32;
        let bucket_count = 1u32;
        let fixed_len = of_car_reader::compact_index::COMPACT_INDEX_FIXED_HEADER_SIZE;
        let data_offset = fixed_len + BUCKET_HEADER_SIZE;
        let target = truncate_entry_hash(hash_domain, key, hash_len as usize);

        let mut out = Vec::new();
        out.extend_from_slice(of_car_reader::compact_index::COMPACT_INDEX_MAGIC);
        out.extend_from_slice(&header_len.to_le_bytes());
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(&bucket_count.to_le_bytes());
        out.push(1);

        let mut bucket_header = [0u8; BUCKET_HEADER_SIZE];
        bucket_header[0..4].copy_from_slice(&hash_domain.to_le_bytes());
        bucket_header[4..8].copy_from_slice(&1u32.to_le_bytes());
        bucket_header[8] = hash_len;
        bucket_header[10..16].copy_from_slice(&(data_offset as u64).to_le_bytes()[..6]);
        out.extend_from_slice(&bucket_header);
        out.extend_from_slice(&target.to_le_bytes());
        out.extend_from_slice(value);
        out
    }
}
