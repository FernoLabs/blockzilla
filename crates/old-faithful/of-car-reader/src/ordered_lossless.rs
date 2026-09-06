use crate::{
    confirmed_block::TransactionStatusMeta,
    data_buffer_pool::{LosslessDataBufferPool, LosslessDataBufferPoolStats},
    error::{CarReadError, CarReadResult},
    metadata_decoder::{
        ZstdReusableDecoder, decode_rewards_from_frame, decode_transaction_status_meta_from_frame,
    },
    node::{CborCidRef, Shredding, SlotMeta, decode_entry_summary, peek_node_type},
    reconstruct::{
        Cid36, LosslessPendingNodeCounts, NodeLocation, RawNode, RawRewardsNode,
        RawTransactionNode, ReconstructError, StandaloneDataFrame,
    },
    versioned_transaction::VersionedTransaction,
};

const ORDERED_BLOCK_SCRATCH_MAX_RETAINED_BYTES: usize = 32 << 20;
const ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS: usize = 1 << 16;

/// PoH data and transaction grouping for one entry in physical CAR order.
///
/// The ordered reader trusts the audited physical order. It keeps the number
/// of transaction references but does not materialize their CIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedEntrySummary {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub transaction_count: u32,
}

/// The form of the optional rewards reference on a terminal block node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderedRewardsRef {
    /// A normal CAR CID. The fixed-size value does not allocate.
    External(Cid36),
    /// Rewards bytes embedded in an identity CID.
    Inline(Vec<u8>),
}

/// Block fields needed by an ordered archive consumer.
///
/// Entry CIDs and the original CBOR sections are intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedBlockSummary {
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    pub entry_count: u32,
    pub meta: SlotMeta,
    pub rewards: Option<OrderedRewardsRef>,
}

#[derive(Debug)]
pub(crate) enum OrderedRawNode {
    Transaction(RawTransactionNode),
    Entry(OrderedEntrySummary),
    Block(OrderedBlockSummary),
    Rewards(RawRewardsNode),
    DataFrame(StandaloneDataFrame),
    Subset,
    Epoch,
}

impl OrderedRawNode {
    pub(crate) fn kind(&self) -> u64 {
        match self {
            Self::Transaction(_) => 0,
            Self::Entry(_) => 1,
            Self::Block(_) => 2,
            Self::Subset => 3,
            Self::Epoch => 4,
            Self::Rewards(_) => 5,
            Self::DataFrame(_) => 6,
        }
    }
}

/// One lossless block read in canonical Old Faithful physical order.
///
/// This buffer appends transaction and entry nodes directly to vectors and
/// keeps one rewards node. It does not build CID tables for those node kinds.
/// Use LosslessCarBlock when input can place or reference these nodes in a
/// different order. Rare rewards continuations are collected sequentially and
/// joined when their rewards node arrives; this path does not build CID tables.
#[derive(Debug, Default)]
pub struct OrderedLosslessCarBlock {
    pub block: Option<OrderedBlockSummary>,
    pub entries: Vec<OrderedEntrySummary>,
    pub transactions: Vec<RawTransactionNode>,
    pub rewards: Option<RawRewardsNode>,

    referenced_transactions: usize,
    pending_reward_frames: Vec<StandaloneDataFrame>,
    pub(crate) recycled_shredding: Vec<Shredding>,
    pub(crate) scratch: Vec<u8>,
    pub(crate) data_buffer_pool: LosslessDataBufferPool,
}

impl OrderedLosslessCarBlock {
    /// Clear the block and retain bounded dataframe allocations for reuse.
    pub fn clear(&mut self) {
        self.recycle_all_data_buffers();
        if let Some(mut block) = self.block.take() {
            block.shredding.clear();
            if block.shredding.capacity() <= ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS {
                self.recycled_shredding = block.shredding;
            }
        }
        clear_vec_with_capacity_limit(
            &mut self.recycled_shredding,
            ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS,
        );
        clear_vec_with_capacity_limit(
            &mut self.entries,
            ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS,
        );
        clear_vec_with_capacity_limit(
            &mut self.transactions,
            ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS,
        );
        clear_vec_with_capacity_limit(
            &mut self.pending_reward_frames,
            ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS,
        );
        clear_vec_with_capacity_limit(&mut self.scratch, ORDERED_BLOCK_SCRATCH_MAX_RETAINED_BYTES);
        self.referenced_transactions = 0;
    }

    /// Return cumulative statistics for reusable dataframe buffers.
    pub fn data_buffer_pool_stats(&self) -> LosslessDataBufferPoolStats {
        self.data_buffer_pool.stats()
    }

    /// Release buffers retained after earlier blocks.
    ///
    /// Call clear first when the current block is no longer needed.
    pub fn release_reusable_data_buffers(&mut self) {
        self.data_buffer_pool.release();
    }

    /// Return physical nodes collected before a terminal block node.
    pub fn pending_node_counts(&self) -> LosslessPendingNodeCounts {
        if self.block.is_some() {
            return LosslessPendingNodeCounts::default();
        }
        LosslessPendingNodeCounts {
            transactions: self.transactions.len(),
            entries: self.entries.len(),
            rewards: usize::from(self.rewards.is_some()),
            dataframes: self.pending_reward_frames.len(),
        }
    }

    pub fn has_pending_nodes(&self) -> bool {
        !self.pending_node_counts().is_empty()
    }

    pub(crate) fn unterminated_block_group_error(&self) -> Option<ReconstructError> {
        let pending = self.pending_node_counts();
        (!pending.is_empty()).then_some(ReconstructError::UnterminatedBlockGroup {
            transactions: pending.transactions,
            entries: pending.entries,
            rewards: pending.rewards,
            dataframes: pending.dataframes,
        })
    }

    pub(crate) fn push_ordered_node(&mut self, node: OrderedRawNode) -> CarReadResult<bool> {
        match node {
            OrderedRawNode::Transaction(transaction) => {
                if !transaction.data.next.is_empty() {
                    let location = transaction.location;
                    recycle_transaction_data(&mut self.data_buffer_pool, transaction);
                    return Err(CarReadError::InvalidData(format!(
                        "ordered lossless reader rejects transaction data continuation at CAR entry {} offset {}",
                        location.entry_index, location.car_offset
                    )));
                }
                if !transaction.metadata.next.is_empty() {
                    let location = transaction.location;
                    recycle_transaction_data(&mut self.data_buffer_pool, transaction);
                    return Err(CarReadError::InvalidData(format!(
                        "ordered lossless reader rejects transaction metadata continuation at CAR entry {} offset {}",
                        location.entry_index, location.car_offset
                    )));
                }
                self.transactions.push(transaction);
                Ok(false)
            }
            OrderedRawNode::Entry(entry) => {
                self.referenced_transactions = self
                    .referenced_transactions
                    .checked_add(entry.transaction_count as usize)
                    .ok_or_else(|| {
                        CarReadError::InvalidData(
                            "ordered block transaction reference count overflow".to_string(),
                        )
                    })?;
                self.entries.push(entry);
                Ok(false)
            }
            OrderedRawNode::Rewards(mut rewards) => {
                if self.rewards.is_some() {
                    recycle_rewards_data(&mut self.data_buffer_pool, rewards);
                    return Err(CarReadError::InvalidData(
                        "ordered lossless block contains more than one rewards node".to_string(),
                    ));
                }
                if let Err(error) = join_ordered_reward_frames(
                    &mut rewards,
                    &mut self.pending_reward_frames,
                    &mut self.data_buffer_pool,
                ) {
                    recycle_rewards_data(&mut self.data_buffer_pool, rewards);
                    return Err(error);
                }
                self.rewards = Some(rewards);
                Ok(false)
            }
            OrderedRawNode::DataFrame(frame) => {
                self.pending_reward_frames.push(frame);
                Ok(false)
            }
            OrderedRawNode::Block(block) => {
                self.finish_block(block)?;
                Ok(true)
            }
            OrderedRawNode::Subset | OrderedRawNode::Epoch => Ok(false),
        }
    }

    fn finish_block(&mut self, block: OrderedBlockSummary) -> CarReadResult<()> {
        if !self.pending_reward_frames.is_empty() {
            return Err(CarReadError::InvalidData(format!(
                "ordered block has {} standalone dataframe nodes without a rewards continuation",
                self.pending_reward_frames.len()
            )));
        }
        if block.entry_count as usize != self.entries.len() {
            return Err(CarReadError::InvalidData(format!(
                "ordered block references {} entries but reader collected {}",
                block.entry_count,
                self.entries.len()
            )));
        }
        if self.referenced_transactions != self.transactions.len() {
            return Err(CarReadError::InvalidData(format!(
                "ordered entries reference {} transactions but reader collected {}",
                self.referenced_transactions,
                self.transactions.len()
            )));
        }

        let expects_rewards_node = match block.rewards.as_ref() {
            None => false,
            Some(OrderedRewardsRef::External(_)) => true,
            Some(OrderedRewardsRef::Inline(_)) => false,
        };
        if expects_rewards_node != self.rewards.is_some() {
            return Err(CarReadError::InvalidData(
                "ordered block rewards linkage does not match the collected rewards node"
                    .to_string(),
            ));
        }
        if let (Some(OrderedRewardsRef::External(expected)), Some(rewards)) =
            (block.rewards.as_ref(), self.rewards.as_ref())
            && rewards.cid != *expected
        {
            return Err(CarReadError::InvalidData(
                "ordered block rewards CID does not match the collected rewards node".to_string(),
            ));
        }
        if let Some(rewards) = self.rewards.as_ref()
            && rewards.slot != block.slot
        {
            return Err(CarReadError::InvalidData(format!(
                "ordered rewards node slot {} does not match block slot {}",
                rewards.slot, block.slot
            )));
        }

        self.block = Some(block);
        Ok(())
    }

    fn recycle_all_data_buffers(&mut self) {
        let pool = &mut self.data_buffer_pool;
        for transaction in self.transactions.drain(..) {
            recycle_transaction_data(pool, transaction);
        }
        if let Some(rewards) = self.rewards.take() {
            recycle_rewards_data(pool, rewards);
        }
        for frame in self.pending_reward_frames.drain(..) {
            pool.recycle(frame.frame.data);
        }
    }

    pub fn validate_decoding(&self) -> Result<(), ReconstructError> {
        let mut zstd = ZstdReusableDecoder::new();

        for transaction in &self.transactions {
            if !transaction.data.next.is_empty() || !transaction.metadata.next.is_empty() {
                return Err(ReconstructError::NodeDecode(
                    "ordered transaction contains a dataframe continuation".to_string(),
                ));
            }
            let _ = wincode::deserialize::<VersionedTransaction<'_>>(&transaction.data.data)
                .map_err(|error| ReconstructError::TransactionDecode(error.to_string()))?;

            let mut output = TransactionStatusMeta::default();
            decode_transaction_status_meta_from_frame(
                transaction.slot,
                &transaction.metadata.data,
                &mut output,
                &mut zstd,
            )?;
        }

        if let Some(rewards) = &self.rewards {
            if !rewards.data.next.is_empty() {
                return Err(ReconstructError::NodeDecode(
                    "ordered rewards continuation was not joined".to_string(),
                ));
            }
            let mut output = crate::confirmed_block::Rewards::default();
            decode_rewards_from_frame(&rewards.data.data, &mut output, &mut zstd)
                .map_err(ReconstructError::RewardsDecode)?;
        }

        Ok(())
    }

    /// Return a terminal block's shredding allocation to this reusable reader.
    ///
    /// Consumers that take `self.block` can call this after they finish the
    /// block. The next ordered decode then fills the same allocation again.
    pub fn recycle_block_shredding(&mut self, mut shredding: Vec<Shredding>) {
        shredding.clear();
        if shredding.capacity() <= ORDERED_BLOCK_CONTAINER_MAX_RETAINED_ITEMS {
            self.recycled_shredding = shredding;
        }
    }
}

/// Join rewards frames written by the canonical Old Faithful generator.
///
/// The generator writes continuation frames immediately before their parent,
/// in descending frame-index order. Link targets introduce the same frames in
/// ascending index order. This permits one sequential validation pass and one
/// copy pass without a CID lookup table.
fn join_ordered_reward_frames(
    rewards: &mut RawRewardsNode,
    frames: &mut Vec<StandaloneDataFrame>,
    pool: &mut LosslessDataBufferPool,
) -> CarReadResult<()> {
    if rewards.data.next.is_empty() {
        if frames.is_empty() {
            return Ok(());
        }
        return Err(CarReadError::InvalidData(format!(
            "ordered rewards node has no continuation but {} standalone dataframe nodes precede it",
            frames.len()
        )));
    }

    let total = rewards.data.total.ok_or_else(|| {
        CarReadError::InvalidData(
            "ordered rewards continuation is missing its total frame count".to_string(),
        )
    })?;
    if rewards.data.index != Some(0) {
        return Err(CarReadError::InvalidData(format!(
            "ordered rewards first frame has index {:?}, expected 0",
            rewards.data.index
        )));
    }
    let total = usize::try_from(total).map_err(|_| {
        CarReadError::InvalidData(
            "ordered rewards total frame count exceeds usize::MAX".to_string(),
        )
    })?;
    let expected_total = frames.len().checked_add(1).ok_or_else(|| {
        CarReadError::InvalidData("ordered rewards frame count overflow".to_string())
    })?;
    if total != expected_total {
        return Err(CarReadError::InvalidData(format!(
            "ordered rewards declare {total} frames but reader collected {expected_total}"
        )));
    }

    let expected_hash = (rewards.data.hash, rewards.data.hash_was_negative);
    for (offset, frame) in frames.iter().rev().enumerate() {
        let expected_index = offset + 1;
        if frame.frame.index != Some(expected_index as u64) {
            return Err(CarReadError::InvalidData(format!(
                "ordered rewards continuation at CAR entry {} has frame index {:?}, expected {expected_index}",
                frame.location.entry_index, frame.frame.index
            )));
        }
        if frame.frame.total != Some(total as u64) {
            return Err(CarReadError::InvalidData(format!(
                "ordered rewards continuation frame {expected_index} declares total {:?}, expected {total}",
                frame.frame.total
            )));
        }
        if (frame.frame.hash, frame.frame.hash_was_negative) != expected_hash {
            return Err(CarReadError::InvalidData(format!(
                "ordered rewards continuation frame {expected_index} has a different payload hash"
            )));
        }
    }

    let mut next_index = 1usize;
    validate_ordered_next_refs(&rewards.data.next, frames, &mut next_index)?;
    for logical_index in 1..total {
        let frame = &frames[frames.len() - logical_index];
        validate_ordered_next_refs(&frame.frame.next, frames, &mut next_index)?;
    }
    if next_index != total {
        return Err(CarReadError::InvalidData(format!(
            "ordered rewards links introduce {} continuation frames, expected {}",
            next_index.saturating_sub(1),
            total.saturating_sub(1)
        )));
    }

    let joined_len = frames
        .iter()
        .try_fold(rewards.data.data.len(), |len, frame| {
            len.checked_add(frame.frame.data.len()).ok_or_else(|| {
                CarReadError::InvalidData("ordered rewards byte length overflow".to_string())
            })
        })?;
    let mut joined = pool.take(joined_len);
    joined.extend_from_slice(&rewards.data.data);
    for frame in frames.iter().rev() {
        joined.extend_from_slice(&frame.frame.data);
    }

    let first_frame = std::mem::replace(&mut rewards.data.data, joined);
    pool.recycle(first_frame);
    rewards.data.next.clear();
    for frame in frames.drain(..) {
        pool.recycle(frame.frame.data);
    }
    Ok(())
}

fn validate_ordered_next_refs(
    refs: &[crate::reconstruct::RawCidRef],
    frames: &[StandaloneDataFrame],
    next_index: &mut usize,
) -> CarReadResult<()> {
    for reference in refs {
        let logical_index = *next_index;
        let physical_index = frames.len().checked_sub(logical_index).ok_or_else(|| {
            CarReadError::InvalidData(
                "ordered rewards continuation links exceed collected frames".to_string(),
            )
        })?;
        let expected = &frames[physical_index];
        let actual = reference.cid.ok_or_else(|| {
            CarReadError::InvalidData(
                "ordered rewards continuation uses an unsupported inline CID".to_string(),
            )
        })?;
        if actual != expected.cid {
            return Err(CarReadError::InvalidData(format!(
                "ordered rewards continuation link {logical_index} does not match the next physical frame"
            )));
        }
        *next_index = (*next_index).checked_add(1).ok_or_else(|| {
            CarReadError::InvalidData("ordered rewards continuation index overflow".to_string())
        })?;
    }
    Ok(())
}

pub(crate) fn decode_ordered_raw_node_with_data_buffers<F>(
    location: NodeLocation,
    cid: Cid36,
    payload: &[u8],
    take_data_buffer: &mut F,
    recycled_shredding: &mut Vec<Shredding>,
) -> Result<OrderedRawNode, ReconstructError>
where
    F: FnMut(usize) -> Vec<u8>,
{
    let kind =
        peek_node_type(payload).map_err(|error| ReconstructError::NodeDecode(error.to_string()))?;
    match kind {
        1 => decode_ordered_entry_summary(payload).map(OrderedRawNode::Entry),
        2 => decode_ordered_block_summary(payload, recycled_shredding).map(OrderedRawNode::Block),
        3 => Ok(OrderedRawNode::Subset),
        4 => Ok(OrderedRawNode::Epoch),
        0 | 5 | 6 => {
            let node = crate::reconstruct::decode_raw_node_with_known_kind_and_data_buffers(
                location,
                cid,
                payload,
                kind,
                take_data_buffer,
            )?;
            match node {
                RawNode::Transaction(transaction) => Ok(OrderedRawNode::Transaction(transaction)),
                RawNode::Rewards(rewards) => Ok(OrderedRawNode::Rewards(rewards)),
                RawNode::DataFrame(frame) => Ok(OrderedRawNode::DataFrame(frame)),
                _ => Err(ReconstructError::NodeDecode(format!(
                    "ordered decoder got unexpected node kind {kind}"
                ))),
            }
        }
        _ => Err(ReconstructError::NodeDecode(format!(
            "unknown kind id {kind}"
        ))),
    }
}

fn decode_ordered_entry_summary(payload: &[u8]) -> Result<OrderedEntrySummary, ReconstructError> {
    let (num_hashes, hash, transaction_count) = decode_entry_summary(payload)
        .map_err(|error| ReconstructError::NodeDecode(error.to_string()))?;
    if hash.len() != 32 {
        return Err(ReconstructError::InvalidEntryHashLen(hash.len()));
    }
    let transaction_count = u32::try_from(transaction_count).map_err(|_| {
        ReconstructError::NodeDecode("entry transaction count exceeds u32::MAX".to_string())
    })?;
    let mut owned_hash = [0u8; 32];
    owned_hash.copy_from_slice(hash);
    Ok(OrderedEntrySummary {
        num_hashes,
        hash: owned_hash,
        transaction_count,
    })
}

fn decode_ordered_block_summary(
    payload: &[u8],
    shredding: &mut Vec<Shredding>,
) -> Result<OrderedBlockSummary, ReconstructError> {
    use minicbor::{Decoder, data::Type, decode::Error as CborError};

    let mut decoder = Decoder::new(payload);
    let array_len = decoder
        .array()?
        .ok_or_else(|| CborError::message("indefinite block array not supported"))?;
    if array_len < 5 {
        return Err(CborError::message("block array too short").into());
    }
    let kind = decoder.u64()?;
    if kind != 2 {
        return Err(ReconstructError::NodeDecode(format!(
            "expected block kind 2, got {kind}"
        )));
    }
    let slot = decoder.u64()?;

    shredding.clear();
    let shredding_count = decoder
        .array()?
        .ok_or_else(|| CborError::message("indefinite shredding array not supported"))?;
    let shredding_count = usize::try_from(shredding_count)
        .map_err(|_| CborError::message("shredding count exceeds usize"))?;
    shredding.reserve(shredding_count);
    for _ in 0..shredding_count {
        shredding.push(decoder.decode()?);
    }

    let entry_count = decoder
        .array()?
        .ok_or_else(|| CborError::message("indefinite block entries array not supported"))?;
    let entry_count = u32::try_from(entry_count)
        .map_err(|_| CborError::message("block entry count exceeds u32::MAX"))?;
    for _ in 0..entry_count {
        decoder.skip()?;
    }

    let meta = decoder.decode::<SlotMeta>()?;
    let rewards = if array_len > 5 {
        if decoder.datatype()? == Type::Null {
            decoder.null()?;
            None
        } else {
            let reference = decoder.decode::<CborCidRef<'_>>()?;
            if let Some(inline) = reference.inline_raw_bytes() {
                Some(OrderedRewardsRef::Inline(inline.to_vec()))
            } else if let Some(cid) = reference.car_cid() {
                Some(OrderedRewardsRef::External(Cid36::from_car_bytes(cid)))
            } else {
                return Err(ReconstructError::NodeDecode(
                    "ordered block has an unsupported rewards reference".to_string(),
                ));
            }
        }
    } else {
        None
    };
    for _ in 6..array_len {
        decoder.skip()?;
    }

    Ok(OrderedBlockSummary {
        slot,
        shredding: std::mem::take(shredding),
        entry_count,
        meta,
        rewards,
    })
}

fn recycle_transaction_data(pool: &mut LosslessDataBufferPool, transaction: RawTransactionNode) {
    pool.recycle(transaction.data.data);
    pool.recycle(transaction.metadata.data);
}

fn recycle_rewards_data(pool: &mut LosslessDataBufferPool, rewards: RawRewardsNode) {
    pool.recycle(rewards.data.data);
}

fn clear_vec_with_capacity_limit<T>(values: &mut Vec<T>, max_capacity: usize) {
    values.clear();
    if values.capacity() > max_capacity {
        *values = Vec::new();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use minicbor::Encoder;

    use super::{OrderedLosslessCarBlock, OrderedRewardsRef};
    use crate::{CarBlockReader, reconstruct::Cid36};

    #[test]
    fn collects_canonical_nodes_in_physical_order() {
        let fixture = one_block_fixture(42, true);
        let mut reader =
            CarBlockReader::with_capacity(Cursor::new(&fixture.car), fixture.car.len());
        reader.skip_header().expect("skip header");
        let mut output = OrderedLosslessCarBlock::default();

        let read = reader
            .read_until_block_ordered_lossless_with_stats(&mut output)
            .expect("read ordered block");

        assert!(read.has_block);
        assert_eq!(output.transactions.len(), 2);
        assert_eq!(output.transactions[0].cid, fixture.transaction_cids[0]);
        assert_eq!(output.transactions[1].cid, fixture.transaction_cids[1]);
        assert_eq!(output.entries.len(), 2);
        assert_eq!(output.entries[0].num_hashes, 1);
        assert_eq!(output.entries[0].hash, [1; 32]);
        assert_eq!(output.entries[0].transaction_count, 1);
        assert_eq!(output.entries[1].num_hashes, 2);
        assert_eq!(output.entries[1].hash, [2; 32]);
        assert_eq!(output.entries[1].transaction_count, 1);
        assert_eq!(
            output
                .block
                .as_ref()
                .and_then(|block| block.rewards.clone()),
            fixture.rewards_cid.map(OrderedRewardsRef::External)
        );
        let block = output.block.as_ref().expect("block summary");
        assert_eq!(block.slot, 42);
        assert_eq!(block.entry_count, 2);
        assert_eq!(block.meta.parent_slot, Some(41));
        assert_eq!(block.shredding.len(), 1);
        assert_eq!(block.shredding[0].entry_end_idx, 7);
        assert_eq!(block.shredding[0].shred_end_idx, 11);
        assert_eq!(read.stats.transactions, 2);
        assert_eq!(read.stats.entries, 2);
        assert_eq!(read.stats.rewards, 1);
        assert_eq!(read.stats.blocks, 1);
        assert_eq!(output.pending_node_counts(), Default::default());
    }

    #[test]
    fn rejects_transaction_data_and_metadata_continuations() {
        for continuation in [
            TransactionContinuation::Data,
            TransactionContinuation::Metadata,
        ] {
            let continuation_cid = Cid36::compute(b"transaction continuation");
            let payload = transaction_payload(42, 0x11, continuation, continuation_cid);
            let cid = Cid36::compute(&payload);
            let mut car = vec![0];
            push_car_entry(&mut car, cid, &payload);
            let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1);
            reader.skip_header().expect("skip header");
            let mut output = OrderedLosslessCarBlock::default();

            let error = reader
                .read_until_block_ordered_lossless_with_stats(&mut output)
                .expect_err("transaction continuation must fail");

            let expected = match continuation {
                TransactionContinuation::Data => "transaction data continuation",
                TransactionContinuation::Metadata => "transaction metadata continuation",
                TransactionContinuation::None => unreachable!(),
            };
            assert!(error.to_string().contains(expected));
            let stats = output.data_buffer_pool_stats();
            assert_eq!(stats.current_buffers, 0);
            assert_eq!(stats.retained_buffers, 2);
        }
    }

    #[test]
    fn rejects_a_second_rewards_node_and_recycles_its_buffer() {
        let first_payload = rewards_payload(42, &[1, 2, 3], None);
        let second_payload = rewards_payload(42, &[4, 5, 6], None);
        let mut car = vec![0];
        push_car_entry(&mut car, Cid36::compute(&first_payload), &first_payload);
        push_car_entry(&mut car, Cid36::compute(&second_payload), &second_payload);
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1);
        reader.skip_header().expect("skip header");
        let mut output = OrderedLosslessCarBlock::default();

        let error = reader
            .read_until_block_ordered_lossless_with_stats(&mut output)
            .expect_err("duplicate rewards must fail");

        assert!(error.to_string().contains("more than one rewards node"));
        let failed = output.data_buffer_pool_stats();
        assert_eq!(failed.current_buffers, 1);
        assert_eq!(failed.retained_buffers, 1);
        output.clear();
        let cleared = output.data_buffer_pool_stats();
        assert_eq!(cleared.current_buffers, 0);
        assert_eq!(cleared.retained_buffers, 2);
    }

    #[test]
    fn direct_and_scratch_reads_have_output_and_counter_parity() {
        let fixture = one_block_fixture(42, true);
        let mut direct_reader =
            CarBlockReader::with_capacity(Cursor::new(&fixture.car), fixture.car.len());
        let mut scratch_reader = CarBlockReader::with_capacity(Cursor::new(&fixture.car), 1);
        direct_reader.skip_header().expect("skip direct header");
        scratch_reader.skip_header().expect("skip scratch header");
        let mut direct = OrderedLosslessCarBlock::default();
        let mut scratch = OrderedLosslessCarBlock::default();

        let direct_read = direct_reader
            .read_until_block_ordered_lossless_with_stats(&mut direct)
            .expect("direct read");
        let scratch_read = scratch_reader
            .read_until_block_ordered_lossless_with_stats(&mut scratch)
            .expect("scratch read");

        assert_eq!(direct.block, scratch.block);
        assert_eq!(direct.transactions, scratch.transactions);
        assert_eq!(direct.entries, scratch.entries);
        assert_eq!(direct.rewards, scratch.rewards);
        assert_eq!(direct.pending_reward_frames, scratch.pending_reward_frames);
        assert_eq!(
            direct_read.stats.car_entries,
            scratch_read.stats.car_entries
        );
        assert_eq!(
            direct_read.stats.payload_bytes,
            scratch_read.stats.payload_bytes
        );
        assert_eq!(direct_read.stats.wire_bytes, scratch_read.stats.wire_bytes);
        assert_eq!(
            direct_read.stats.transactions,
            scratch_read.stats.transactions
        );
        assert_eq!(direct_read.stats.entries, scratch_read.stats.entries);
        assert_eq!(direct_read.stats.rewards, scratch_read.stats.rewards);
        assert_eq!(direct_read.stats.blocks, scratch_read.stats.blocks);
        assert_eq!(
            direct_read.stats.direct_buffer_entries,
            direct_read.stats.car_entries
        );
        assert_eq!(direct_read.stats.scratch_entries, 0);
        assert_eq!(scratch_read.stats.direct_buffer_entries, 0);
        assert_eq!(
            scratch_read.stats.scratch_entries,
            scratch_read.stats.car_entries
        );
    }

    #[test]
    fn reuses_transaction_dataframe_buffers_between_blocks() {
        let first = one_block_fixture(42, false);
        let second = one_block_fixture(43, false);
        let mut car = first.car;
        car.extend_from_slice(&second.car[1..]);
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1);
        reader.skip_header().expect("skip header");
        let mut output = OrderedLosslessCarBlock::default();

        assert!(
            reader
                .read_until_block_ordered_lossless(&mut output)
                .expect("read first block")
        );
        let mut first_data = output
            .transactions
            .iter()
            .map(|transaction| transaction.data.data.as_ptr() as usize)
            .collect::<Vec<_>>();
        let mut first_metadata = output
            .transactions
            .iter()
            .map(|transaction| transaction.metadata.data.as_ptr() as usize)
            .collect::<Vec<_>>();
        first_data.sort_unstable();
        first_metadata.sort_unstable();
        let first_stats = output.data_buffer_pool_stats();
        assert_eq!(first_stats.fresh_buffers, 4);
        assert_eq!(first_stats.reused_buffers, 0);

        assert!(
            reader
                .read_until_block_ordered_lossless(&mut output)
                .expect("read second block")
        );
        let mut second_data = output
            .transactions
            .iter()
            .map(|transaction| transaction.data.data.as_ptr() as usize)
            .collect::<Vec<_>>();
        let mut second_metadata = output
            .transactions
            .iter()
            .map(|transaction| transaction.metadata.data.as_ptr() as usize)
            .collect::<Vec<_>>();
        second_data.sort_unstable();
        second_metadata.sort_unstable();
        assert_eq!(second_data, first_data);
        assert_eq!(second_metadata, first_metadata);
        let second_stats = output.data_buffer_pool_stats();
        assert_eq!(second_stats.fresh_buffers, 4);
        assert_eq!(second_stats.reused_buffers, 4);

        output.clear();
        let cleared = output.data_buffer_pool_stats();
        assert_eq!(cleared.current_buffers, 0);
        assert_eq!(cleared.retained_buffers, 4);
    }

    #[test]
    fn reuses_block_shredding_storage_between_blocks() {
        let first = one_block_fixture(42, false);
        let second = one_block_fixture(43, false);
        let mut car = first.car;
        car.extend_from_slice(&second.car[1..]);
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1);
        reader.skip_header().expect("skip header");
        let mut output = OrderedLosslessCarBlock::default();

        assert!(
            reader
                .read_until_block_ordered_lossless(&mut output)
                .expect("read first block")
        );
        let first_block = output.block.take().expect("first block");
        let first_pointer = first_block.shredding.as_ptr();
        output.recycle_block_shredding(first_block.shredding);

        assert!(
            reader
                .read_until_block_ordered_lossless(&mut output)
                .expect("read second block")
        );
        let second_pointer = output
            .block
            .as_ref()
            .expect("second block")
            .shredding
            .as_ptr();
        assert_eq!(second_pointer, first_pointer);
    }

    #[test]
    fn joins_ordered_rewards_continuations_without_a_cid_table() {
        let hash = 77;
        let second_payload = indexed_dataframe_payload(&[5, 6], hash, 2, 3, &[]);
        let second_cid = Cid36::compute(&second_payload);
        let first_payload = indexed_dataframe_payload(&[3, 4], hash, 1, 3, &[]);
        let first_cid = Cid36::compute(&first_payload);
        let rewards_payload =
            indexed_rewards_payload(42, &[1, 2], hash, 0, 3, &[first_cid, second_cid]);
        let rewards_cid = Cid36::compute(&rewards_payload);
        let block_payload = block_payload(42, &[], Some(rewards_cid));
        let block_cid = Cid36::compute(&block_payload);
        let mut car = vec![0];
        // The canonical writer stores continuation frames in descending index
        // order, followed by the parent rewards node.
        push_car_entry(&mut car, second_cid, &second_payload);
        push_car_entry(&mut car, first_cid, &first_payload);
        push_car_entry(&mut car, rewards_cid, &rewards_payload);
        push_car_entry(&mut car, block_cid, &block_payload);
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1);
        reader.skip_header().expect("skip header");
        let mut output = OrderedLosslessCarBlock::default();

        assert!(
            reader
                .read_until_block_ordered_lossless(&mut output)
                .expect("read continued rewards")
        );

        let rewards = output.rewards.as_ref().expect("rewards");
        assert_eq!(rewards.data.data, [1, 2, 3, 4, 5, 6]);
        assert!(rewards.data.next.is_empty());
        assert!(output.pending_reward_frames.is_empty());
    }

    #[test]
    fn joins_ordered_rewards_continuation_fanout_without_a_cid_table() {
        const TOTAL: usize = 12;
        let hash = 88;
        let mut payloads = vec![Vec::new(); TOTAL];
        let mut cids = vec![None; TOTAL];

        // This is the canonical writer's two-level, five-link fanout. Frames
        // 6 and 1 introduce the next groups; all other continuation frames
        // are leaves.
        for logical_index in [2usize, 3, 4, 5, 7, 8, 9, 10, 11] {
            payloads[logical_index] = indexed_dataframe_payload(
                &[logical_index as u8],
                hash,
                logical_index as u64,
                TOTAL as u64,
                &[],
            );
            cids[logical_index] = Some(Cid36::compute(&payloads[logical_index]));
        }
        let links_7_to_11 = (7..TOTAL)
            .map(|index| cids[index].expect("leaf CID"))
            .collect::<Vec<_>>();
        payloads[6] = indexed_dataframe_payload(&[6], hash, 6, TOTAL as u64, &links_7_to_11);
        cids[6] = Some(Cid36::compute(&payloads[6]));
        let links_2_to_6 = (2..=6)
            .map(|index| cids[index].expect("middle CID"))
            .collect::<Vec<_>>();
        payloads[1] = indexed_dataframe_payload(&[1], hash, 1, TOTAL as u64, &links_2_to_6);
        cids[1] = Some(Cid36::compute(&payloads[1]));

        payloads[0] = indexed_rewards_payload(
            42,
            &[0],
            hash,
            0,
            TOTAL as u64,
            &[cids[1].expect("first continuation CID")],
        );
        cids[0] = Some(Cid36::compute(&payloads[0]));
        let block_payload = block_payload(42, &[], cids[0]);
        let block_cid = Cid36::compute(&block_payload);
        let mut car = vec![0];
        for logical_index in (1..TOTAL).rev() {
            push_car_entry(
                &mut car,
                cids[logical_index].expect("continuation CID"),
                &payloads[logical_index],
            );
        }
        push_car_entry(&mut car, cids[0].expect("rewards CID"), &payloads[0]);
        push_car_entry(&mut car, block_cid, &block_payload);
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1);
        reader.skip_header().expect("skip header");
        let mut output = OrderedLosslessCarBlock::default();

        assert!(
            reader
                .read_until_block_ordered_lossless(&mut output)
                .expect("read fanout rewards")
        );

        assert_eq!(
            output.rewards.as_ref().expect("rewards").data.data,
            (0..TOTAL as u8).collect::<Vec<_>>()
        );
        assert!(output.pending_reward_frames.is_empty());
    }

    #[derive(Clone, Copy)]
    enum TransactionContinuation {
        None,
        Data,
        Metadata,
    }

    struct BlockFixture {
        car: Vec<u8>,
        transaction_cids: [Cid36; 2],
        rewards_cid: Option<Cid36>,
    }

    fn one_block_fixture(slot: u64, with_rewards: bool) -> BlockFixture {
        let first_transaction = transaction_payload(
            slot,
            0x11,
            TransactionContinuation::None,
            Cid36::compute(b"none"),
        );
        let second_transaction = transaction_payload(
            slot,
            0x22,
            TransactionContinuation::None,
            Cid36::compute(b"none"),
        );
        let transaction_cids = [
            Cid36::compute(&first_transaction),
            Cid36::compute(&second_transaction),
        ];
        let first_entry = entry_payload(1, &[transaction_cids[0]]);
        let second_entry = entry_payload(2, &[transaction_cids[1]]);
        let entry_cids = [Cid36::compute(&first_entry), Cid36::compute(&second_entry)];
        let rewards_payload = with_rewards.then(|| rewards_payload(slot, &[7, 8], None));
        let rewards_cid = rewards_payload
            .as_ref()
            .map(|payload| Cid36::compute(payload));
        let block_payload = block_payload(slot, &entry_cids, rewards_cid);
        let block_cid = Cid36::compute(&block_payload);
        let mut car = vec![0];
        push_car_entry(&mut car, transaction_cids[0], &first_transaction);
        push_car_entry(&mut car, transaction_cids[1], &second_transaction);
        push_car_entry(&mut car, entry_cids[0], &first_entry);
        push_car_entry(&mut car, entry_cids[1], &second_entry);
        if let (Some(cid), Some(payload)) = (rewards_cid, rewards_payload.as_ref()) {
            push_car_entry(&mut car, cid, payload);
        }
        push_car_entry(&mut car, block_cid, &block_payload);

        BlockFixture {
            car,
            transaction_cids,
            rewards_cid,
        }
    }

    fn transaction_payload(
        slot: u64,
        marker: u8,
        continuation: TransactionContinuation,
        continuation_cid: Cid36,
    ) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(5).expect("transaction");
        encoder.u64(0).expect("transaction kind");
        encode_dataframe(
            &mut encoder,
            &[marker, marker.wrapping_add(1)],
            matches!(continuation, TransactionContinuation::Data).then_some(continuation_cid),
        );
        encode_dataframe(
            &mut encoder,
            &[marker.wrapping_add(2)],
            matches!(continuation, TransactionContinuation::Metadata).then_some(continuation_cid),
        );
        encoder.u64(slot).expect("transaction slot");
        encoder.null().expect("transaction index");
        encoder.into_writer()
    }

    fn entry_payload(num_hashes: u64, transaction_cids: &[Cid36]) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(4).expect("entry");
        encoder.u64(1).expect("entry kind");
        encoder.u64(num_hashes).expect("num hashes");
        encoder.bytes(&[num_hashes as u8; 32]).expect("entry hash");
        encode_cid_refs(&mut encoder, transaction_cids);
        encoder.into_writer()
    }

    fn rewards_payload(slot: u64, data: &[u8], next: Option<Cid36>) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(3).expect("rewards");
        encoder.u64(5).expect("rewards kind");
        encoder.u64(slot).expect("rewards slot");
        encode_dataframe(&mut encoder, data, next);
        encoder.into_writer()
    }

    fn indexed_dataframe_payload(
        data: &[u8],
        hash: u64,
        index: u64,
        total: u64,
        next: &[Cid36],
    ) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encode_indexed_dataframe(&mut encoder, data, hash, index, total, next);
        encoder.into_writer()
    }

    fn indexed_rewards_payload(
        slot: u64,
        data: &[u8],
        hash: u64,
        index: u64,
        total: u64,
        next: &[Cid36],
    ) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(3).expect("rewards");
        encoder.u64(5).expect("rewards kind");
        encoder.u64(slot).expect("rewards slot");
        encode_indexed_dataframe(&mut encoder, data, hash, index, total, next);
        encoder.into_writer()
    }

    fn encode_dataframe(encoder: &mut Encoder<Vec<u8>>, data: &[u8], next: Option<Cid36>) {
        encoder
            .array(if next.is_some() { 6 } else { 5 })
            .expect("dataframe");
        encoder.u64(6).expect("dataframe kind");
        encoder.null().expect("dataframe hash");
        encoder.null().expect("dataframe index");
        encoder.null().expect("dataframe total");
        encoder.bytes(data).expect("dataframe data");
        if let Some(cid) = next {
            encode_cid_refs(encoder, &[cid]);
        }
    }

    fn encode_indexed_dataframe(
        encoder: &mut Encoder<Vec<u8>>,
        data: &[u8],
        hash: u64,
        index: u64,
        total: u64,
        next: &[Cid36],
    ) {
        encoder
            .array(if next.is_empty() { 5 } else { 6 })
            .expect("dataframe");
        encoder.u64(6).expect("dataframe kind");
        encoder.u64(hash).expect("dataframe hash");
        encoder.u64(index).expect("dataframe index");
        encoder.u64(total).expect("dataframe total");
        encoder.bytes(data).expect("dataframe data");
        if !next.is_empty() {
            encode_cid_refs(encoder, next);
        }
    }

    fn block_payload(slot: u64, entry_cids: &[Cid36], rewards_cid: Option<Cid36>) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder
            .array(if rewards_cid.is_some() { 6 } else { 5 })
            .expect("block");
        encoder.u64(2).expect("block kind");
        encoder.u64(slot).expect("block slot");
        encoder.array(1).expect("shredding");
        encoder.array(2).expect("shredding row");
        encoder.i64(7).expect("entry end index");
        encoder.i64(11).expect("shred end index");
        encode_cid_refs(&mut encoder, entry_cids);
        encoder.array(1).expect("slot metadata");
        encoder.u64(slot.saturating_sub(1)).expect("parent slot");
        if let Some(cid) = rewards_cid {
            encode_cid_ref(&mut encoder, cid);
        }
        encoder.into_writer()
    }

    fn encode_cid_refs(encoder: &mut Encoder<Vec<u8>>, cids: &[Cid36]) {
        encoder.array(cids.len() as u64).expect("CID refs");
        for &cid in cids {
            encode_cid_ref(encoder, cid);
        }
    }

    fn encode_cid_ref(encoder: &mut Encoder<Vec<u8>>, cid: Cid36) {
        encoder.tag(minicbor::data::Tag::new(42)).expect("CID tag");
        encoder.bytes(&cid.cbor_bytes()).expect("CID bytes");
    }

    fn push_car_entry(output: &mut Vec<u8>, cid: Cid36, payload: &[u8]) {
        push_uvarint(output, (cid.car_bytes().len() + payload.len()) as u64);
        output.extend_from_slice(cid.car_bytes());
        output.extend_from_slice(payload);
    }

    fn push_uvarint(output: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                return;
            }
        }
    }
}
