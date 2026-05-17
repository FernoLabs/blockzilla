use crate::source::Source;
use anyhow::{Context, Result, anyhow};
use of_car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    slot_ranges::{SLOTS_PER_EPOCH, SlotRangeWithPreviousBlockhash},
};
use of_slot_ranges::{
    AsyncCompactIndex, BuildSlotRangesConfig, RangeReader, build_slot_ranges_from_indexes,
    decode_car_header_total_size,
};
use std::{future::Future, io::Cursor, pin::Pin, sync::Arc};

const CAR_HEADER_PREFIX_READ_LEN: usize = 16;

pub struct BuildSlotIndexV2Output {
    pub ranges: Vec<SlotRangeWithPreviousBlockhash>,
    pub present_slots: u32,
    pub first_present_slot: Option<u64>,
    pub last_present_slot: Option<u64>,
    pub last_blockhash: [u8; 32],
}

pub async fn build_slot_ranges_v2(
    source: Arc<Source>,
    epoch: u64,
    seed_previous_blockhash: Option<[u8; 32]>,
    max_bucket_payload_bytes: usize,
    allow_node_read_fallback: bool,
) -> Result<BuildSlotIndexV2Output> {
    let epoch_cid_path = format!("{epoch}/epoch-{epoch}.cid");
    let epoch_cid = source
        .get_text(&epoch_cid_path)
        .await
        .with_context(|| format!("fetch {epoch_cid_path}"))?;
    let epoch_cid = epoch_cid.trim();
    if epoch_cid.is_empty() {
        return Err(anyhow!("{epoch_cid_path} was empty"));
    }

    let slot_index_path = format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-slot-to-cid.index");
    let cid_index_path =
        format!("{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");
    let car_path = format!("{epoch}/epoch-{epoch}.car");

    let car_prefix = source
        .get_range(&car_path, 0, CAR_HEADER_PREFIX_READ_LEN)
        .await
        .with_context(|| format!("read CAR header prefix from {car_path}"))?;
    let car_header_size = decode_car_header_total_size(&car_prefix, &car_path)?;

    let mut slot_index = AsyncCompactIndex::open(
        SourceRangeReader::new(Arc::clone(&source), slot_index_path.clone()),
        slot_index_path,
    )
    .await?;
    let mut cid_index = AsyncCompactIndex::open(
        SourceRangeReader::new(Arc::clone(&source), cid_index_path.clone()),
        cid_index_path,
    )
    .await?;

    let output = build_slot_ranges_from_indexes(
        epoch,
        car_header_size,
        &mut slot_index,
        &mut cid_index,
        BuildSlotRangesConfig {
            max_bucket_payload_bytes,
            allow_node_read_fallback,
        },
    )
    .await?;

    let epoch_start_slot = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow"))?;
    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut previous_blockhash = seed_previous_blockhash.unwrap_or([0; 32]);
    let mut first_present_slot = None;
    let mut last_present_slot = None;

    for (slot_in_epoch, range) in output.ranges.iter().copied().enumerate() {
        ranges[slot_in_epoch] = SlotRangeWithPreviousBlockhash {
            range,
            previous_blockhash,
        };

        if range.is_empty() {
            continue;
        }

        let slot = epoch_start_slot + slot_in_epoch as u64;
        let bytes = source
            .get_range(&car_path, range.offset, range.len as usize)
            .await
            .with_context(|| {
                format!(
                    "read CAR block range for slot {slot} at {}+{}",
                    range.offset, range.len
                )
            })?;
        previous_blockhash = decode_blockhash_from_car_slice(bytes)
            .with_context(|| format!("decode blockhash for slot {slot}"))?;
        first_present_slot.get_or_insert(slot);
        last_present_slot = Some(slot);
    }

    Ok(BuildSlotIndexV2Output {
        ranges,
        present_slots: output.stats.present_slots,
        first_present_slot,
        last_present_slot,
        last_blockhash: previous_blockhash,
    })
}

fn decode_blockhash_from_car_slice(bytes: Vec<u8>) -> Result<[u8; 32]> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = CarBlockGroup::without_rewards();
    if !reader.read_until_block_into(&mut block)? {
        return Err(anyhow!("CAR slice did not contain a block node"));
    }
    if !block.has_blockhash {
        return Err(anyhow!("block did not contain an entry hash"));
    }
    Ok(block.blockhash)
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
