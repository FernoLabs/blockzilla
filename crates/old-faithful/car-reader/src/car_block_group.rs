use solana_short_vec::decode_shortu16_len;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::mem::MaybeUninit;
use wincode::Deserialize;

use crate::confirmed_block::TransactionStatusMeta;
use crate::error::{CarReadError, CarReadResult, GroupError};
use crate::metadata_decoder::{
    BorrowedTransactionStatusMetaView, TransactionStatusMetaVisitor, ZstdReusableDecoder,
    decode_protobuf_transaction_status_meta_borrowed, decode_transaction_status_meta_from_frame,
    slot_uses_protobuf_metadata, visit_protobuf_transaction_status_meta,
};
use crate::node::{
    CarCid, CborCidRef, Node, NodeDecodeError, OwnedDataFrame, Shredding, SlotMeta,
    decode_entry_summary, decode_node, peek_node_type,
};
use crate::versioned_transaction::VersionedTransaction;

const TX_BUF_INITIAL_CAPACITY: usize = 12 << 20;
const TX_RANGES_INITIAL_CAPACITY: usize = 12_000;
const SCRATCH_WITH_TX_INITIAL_CAPACITY: usize = 3 << 20;
const SCRATCH_NO_TX_INITIAL_CAPACITY: usize = 64 << 10;
const TX_SIGNATURE_NODE_PREFIX_BYTES: usize = 256;

#[derive(Debug, Clone)]
struct PendingDataFrame {
    frame: OwnedDataFrame,
    next: Vec<PendingDataFrameRef>,
}

#[derive(Debug, Clone)]
enum PendingDataFrameRef {
    Cid(CarCid),
    Inline(Vec<u8>),
}

impl PendingDataFrame {
    fn from_borrowed(frame: &crate::node::DataFrame<'_>) -> CarReadResult<Self> {
        let next = frame
            .next
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .map(|cid| {
                        let cid = cid.map_err(|err| CarReadError::InvalidData(err.to_string()))?;
                        PendingDataFrameRef::from_cbor(cid)
                    })
                    .collect::<CarReadResult<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            frame: OwnedDataFrame::from(frame),
            next,
        })
    }
}

impl PendingDataFrameRef {
    fn from_cbor(value: CborCidRef<'_>) -> CarReadResult<Self> {
        if let Some(bytes) = value.inline_raw_bytes() {
            return Ok(Self::Inline(bytes.to_vec()));
        }
        if let Some(cid) = value.car_cid() {
            return Ok(Self::Cid(cid));
        }
        Err(CarReadError::InvalidData(
            "unsupported dataframe continuation CID".to_string(),
        ))
    }
}

/// Simple, fast CarBlockGroup that stores:
/// - Transaction payloads in file order (no CID resolution)
/// - Entry membership by transaction count (still no CID table)
/// - Block metadata fields directly in struct
/// - Inline rewards dataframe bytes when present
///
/// This is the single-threaded version backported from the parallel implementation.
pub struct CarBlockGroup {
    // tx storage (in file order)
    tx_buf: Vec<u8>,
    tx_ranges: Vec<(u32, u32)>, // (start, end) in tx_buf

    // block metadata (extracted directly from Block node)
    pub slot: Option<u64>,
    pub parent_slot: Option<u64>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,

    // blockhash (from Entry.hash - last seen)
    pub blockhash: [u8; 32],
    pub has_blockhash: bool,

    // PoH per-entry info, kept in file order.
    pub poh_num_hashes: Vec<u64>,
    pub poh_hashes: Vec<[u8; 32]>,
    pub entry_tx_counts: Vec<u32>,

    // Block-level archive fields skipped by the previous fast path.
    pub shredding: Vec<Shredding>,
    pub rewards: Option<OwnedDataFrame>,
    pub rewards_slot: Option<u64>,
    pending_rewards: Option<PendingDataFrame>,
    dataframes: HashMap<[u8; 36], PendingDataFrame>,

    // scratch buffer for reading
    scratch: Vec<u8>,
    txs_assigned_to_entries: u32,
    tx_count_seen: u32,
    read_rewards: bool,
    read_transactions: bool,
    transaction_prefix_bytes: Option<usize>,
}

impl Default for CarBlockGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroup {
    pub fn new() -> Self {
        Self::with_read_options(true, true, None)
    }

    fn with_read_options(
        read_rewards: bool,
        read_transactions: bool,
        transaction_prefix_bytes: Option<usize>,
    ) -> Self {
        let tx_buf_capacity = if read_transactions {
            transaction_prefix_bytes
                .map(|prefix| {
                    prefix
                        .saturating_mul(TX_RANGES_INITIAL_CAPACITY)
                        .min(TX_BUF_INITIAL_CAPACITY)
                })
                .unwrap_or(TX_BUF_INITIAL_CAPACITY)
        } else {
            0
        };
        let tx_ranges_capacity = if read_transactions {
            TX_RANGES_INITIAL_CAPACITY
        } else {
            0
        };
        let scratch_capacity = if read_transactions && transaction_prefix_bytes.is_none() {
            SCRATCH_WITH_TX_INITIAL_CAPACITY
        } else {
            SCRATCH_NO_TX_INITIAL_CAPACITY
        };

        Self {
            tx_buf: Vec::with_capacity(tx_buf_capacity),
            tx_ranges: Vec::with_capacity(tx_ranges_capacity),
            scratch: Vec::with_capacity(scratch_capacity),

            slot: None,
            parent_slot: None,
            block_time: None,
            block_height: None,

            blockhash: [0; 32],
            has_blockhash: false,

            poh_num_hashes: Vec::with_capacity(4096),
            poh_hashes: Vec::with_capacity(4096),
            entry_tx_counts: Vec::with_capacity(4096),

            shredding: Vec::with_capacity(128),
            rewards: None,
            rewards_slot: None,
            pending_rewards: None,
            dataframes: HashMap::new(),

            txs_assigned_to_entries: 0,
            tx_count_seen: 0,
            read_rewards,
            read_transactions,
            transaction_prefix_bytes,
        }
    }

    pub fn without_rewards() -> Self {
        Self::with_read_options(false, true, None)
    }

    pub fn without_transaction_payloads() -> Self {
        Self::with_read_options(true, false, None)
    }

    pub fn without_rewards_and_transaction_payloads() -> Self {
        Self::with_read_options(false, false, None)
    }

    pub fn with_transaction_signature_prefixes() -> Self {
        Self::with_read_options(true, true, Some(TX_SIGNATURE_NODE_PREFIX_BYTES))
    }

    pub fn without_rewards_and_transaction_signature_prefixes() -> Self {
        Self::with_read_options(false, true, Some(TX_SIGNATURE_NODE_PREFIX_BYTES))
    }

    #[inline]
    pub fn clear(&mut self) {
        self.tx_buf.clear();
        self.tx_ranges.clear();

        self.slot = None;
        self.parent_slot = None;
        self.block_time = None;
        self.block_height = None;

        self.blockhash = [0; 32];
        self.has_blockhash = false;

        self.poh_num_hashes.clear();
        self.poh_hashes.clear();
        self.entry_tx_counts.clear();
        self.shredding.clear();
        self.rewards = None;
        self.rewards_slot = None;
        self.pending_rewards = None;
        self.dataframes.clear();

        self.scratch.clear();
        self.txs_assigned_to_entries = 0;
        self.tx_count_seen = 0;
    }

    /// Returns (tx_count, tx_buf_bytes)
    pub fn get_len(&self) -> (usize, usize) {
        let tx_count = if self.read_transactions {
            self.tx_ranges.len()
        } else {
            self.tx_count_seen as usize
        };
        (tx_count, self.tx_buf.len())
    }

    #[inline]
    pub fn entry_count(&self) -> usize {
        self.entry_tx_counts.len()
    }

    #[inline]
    pub fn reads_transaction_payloads(&self) -> bool {
        self.read_transactions && self.transaction_prefix_bytes.is_none()
    }

    #[inline]
    pub fn transaction_prefix_bytes(&self) -> Option<usize> {
        self.transaction_prefix_bytes
    }

    /// Read one CAR entry payload into the group.
    /// Returns Ok(true) when Block node is reached (group complete).
    #[inline]
    pub fn read_entry_payload_into<R: Read>(
        &mut self,
        reader: &mut R,
        payload_len: usize,
    ) -> CarReadResult<bool> {
        self.read_entry_payload_with_cid_into(None, reader, payload_len)
    }

    #[inline]
    pub fn read_entry_payload_with_cid_into<R: Read>(
        &mut self,
        cid: Option<&[u8; 36]>,
        reader: &mut R,
        payload_len: usize,
    ) -> CarReadResult<bool> {
        if payload_len == 0 {
            return Err(CarReadError::UnexpectedEof(format!(
                "Empty payload len ({payload_len})"
            )));
        }

        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        scratch.resize(payload_len, 0u8);

        if let Err(e) = reader.read_exact(&mut scratch) {
            scratch.clear();
            self.scratch = scratch;
            return Err(CarReadError::Io(e.to_string()));
        }

        let result = self.read_entry_payload_slice_with_cid_into(cid, &scratch, payload_len);
        self.scratch = scratch;
        result
    }

    #[inline]
    pub fn read_entry_maybe_payload_into(
        &mut self,
        prefix: &[u8],
        payload: Option<&[u8]>,
        payload_len: usize,
    ) -> CarReadResult<bool> {
        self.read_entry_maybe_payload_with_cid_into(None, prefix, payload, payload_len)
    }

    #[inline]
    pub fn read_entry_maybe_payload_with_cid_into(
        &mut self,
        cid: Option<&[u8; 36]>,
        prefix: &[u8],
        payload: Option<&[u8]>,
        payload_len: usize,
    ) -> CarReadResult<bool> {
        if payload_len == 0 {
            return Err(CarReadError::UnexpectedEof(format!(
                "Empty payload len ({payload_len})"
            )));
        }

        let node_type = peek_node_type(prefix)
            .map_err(|err| CarReadError::InvalidData(format!("Can't read node type ({err})")))?;
        if node_type == 0 {
            if !self.read_transactions {
                self.tx_count_seen = self
                    .tx_count_seen
                    .checked_add(1)
                    .ok_or_else(|| CarReadError::InvalidData("tx count overflow".to_string()))?;
                return Ok(false);
            }

            if self.transaction_prefix_bytes.is_some() {
                return self.read_entry_payload_slice_impl(prefix, payload_len);
            }
        }

        let payload = payload.ok_or_else(|| {
            CarReadError::InvalidData("non-transaction CAR payload was skipped".to_string())
        })?;
        self.read_entry_payload_slice_with_cid_into(cid, payload, payload_len)
    }

    #[inline]
    pub fn read_entry_payload_slice_into(
        &mut self,
        payload: &[u8],
        payload_len: usize,
    ) -> CarReadResult<bool> {
        self.read_entry_payload_slice_impl(payload, payload_len)
    }

    #[inline]
    pub fn read_entry_payload_slice_with_cid_into(
        &mut self,
        cid: Option<&[u8; 36]>,
        payload: &[u8],
        payload_len: usize,
    ) -> CarReadResult<bool> {
        self.read_entry_payload_slice_with_cid_impl(cid, payload, payload_len)
    }

    fn read_entry_payload_slice_impl(
        &mut self,
        payload: &[u8],
        payload_len: usize,
    ) -> CarReadResult<bool> {
        self.read_entry_payload_slice_with_cid_impl(None, payload, payload_len)
    }

    fn read_entry_payload_slice_with_cid_impl(
        &mut self,
        cid: Option<&[u8; 36]>,
        payload: &[u8],
        payload_len: usize,
    ) -> CarReadResult<bool> {
        if payload_len == 0 {
            return Err(CarReadError::UnexpectedEof(format!(
                "Empty payload len ({payload_len})"
            )));
        }
        if payload.len() > payload_len {
            return Err(CarReadError::InvalidData(format!(
                "payload length exceeds frame length: got {}, expected at most {payload_len}",
                payload.len()
            )));
        }

        let node_type = peek_node_type(payload)
            .map_err(|err| CarReadError::InvalidData(format!("Can't read node type ({err})")))?;

        //println!("{node_type} {payload_len}");

        // Transaction: store in file order
        if node_type == 0 {
            if !self.read_transactions {
                self.tx_count_seen = self
                    .tx_count_seen
                    .checked_add(1)
                    .ok_or_else(|| CarReadError::InvalidData("tx count overflow".to_string()))?;
                return Ok(false);
            }
            if self.transaction_prefix_bytes.is_none() && payload.len() != payload_len {
                return Err(CarReadError::InvalidData(format!(
                    "payload length mismatch: got {}, expected {payload_len}",
                    payload.len()
                )));
            }

            let start = self.tx_buf.len();
            let end = start + payload.len();

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData(
                    "tx buffer exceeds u32::MAX".to_string(),
                ));
            }

            self.tx_buf.extend_from_slice(payload);
            self.tx_ranges.push((start as u32, end as u32));
            return Ok(false);
        }

        if payload.len() != payload_len {
            return Err(CarReadError::InvalidData(format!(
                "payload length mismatch: got {}, expected {payload_len}",
                payload.len()
            )));
        }

        // Entry: extract PoH info and transaction grouping in file order
        if node_type == 1 {
            let (num_hashes, hash_bytes, tx_count) = decode_entry_summary(payload)
                .map_err(|e| CarReadError::InvalidData(e.to_string()))?;

            if hash_bytes.len() != 32 {
                return Err(CarReadError::InvalidData(
                    "entry hash len != 32".to_string(),
                ));
            }

            let mut h = [0u8; 32];
            h.copy_from_slice(hash_bytes);

            self.poh_num_hashes.push(num_hashes);
            self.poh_hashes.push(h);

            let tx_count = u32::try_from(tx_count).map_err(|_| {
                CarReadError::InvalidData("entry tx count exceeds u32::MAX".to_string())
            })?;
            let seen_txs = self.seen_transaction_count()?;
            let assigned_after = self
                .txs_assigned_to_entries
                .checked_add(tx_count)
                .ok_or_else(|| CarReadError::InvalidData("entry tx count overflow".to_string()))?;
            if assigned_after > seen_txs {
                return Err(CarReadError::InvalidData(format!(
                    "entry references {assigned_after} txs but only {seen_txs} were seen before it"
                )));
            }
            self.entry_tx_counts.push(tx_count);
            self.txs_assigned_to_entries = assigned_after;

            self.blockhash.copy_from_slice(&h);
            self.has_blockhash = true;
            return Ok(false);
        }

        if node_type == 5 {
            if !self.read_rewards {
                return Ok(false);
            }

            let node =
                decode_node(payload).map_err(|e| CarReadError::InvalidData(e.to_string()))?;
            let Node::Rewards(rewards) = node else {
                return Err(CarReadError::InvalidData(
                    "expected rewards node".to_string(),
                ));
            };

            self.rewards_slot = Some(rewards.slot);
            let rewards = PendingDataFrame::from_borrowed(&rewards.data)?;
            if rewards.next.is_empty() {
                self.rewards = Some(rewards.frame);
                self.pending_rewards = None;
            } else {
                self.rewards = None;
                self.pending_rewards = Some(rewards);
            }
            return Ok(false);
        }

        if node_type == 6 {
            if !self.read_rewards {
                return Ok(false);
            }
            let Some(cid) = cid else {
                return Err(CarReadError::InvalidData(
                    "dataframe node missing CAR CID".to_string(),
                ));
            };

            let node =
                decode_node(payload).map_err(|e| CarReadError::InvalidData(e.to_string()))?;
            let Node::DataFrame(frame) = node else {
                return Err(CarReadError::InvalidData(
                    "expected dataframe node".to_string(),
                ));
            };
            self.dataframes
                .insert(*cid, PendingDataFrame::from_borrowed(&frame)?);
            return Ok(false);
        }

        // Block: extract metadata, shredding, rewards linkage and signal completion
        if node_type == 2 {
            let block = decode_block_summary_into(payload, &mut self.shredding)
                .map_err(|e| CarReadError::InvalidData(e.to_string()))?;

            if block.entry_count != self.entry_count() {
                return Err(CarReadError::InvalidData(format!(
                    "block references {} entries but reader collected {}",
                    block.entry_count,
                    self.entry_count()
                )));
            }

            let seen_txs = self.seen_transaction_count()?;
            if self.txs_assigned_to_entries != seen_txs {
                return Err(CarReadError::InvalidData(format!(
                    "entry grouping covers {} txs but block contains {seen_txs}",
                    self.txs_assigned_to_entries
                )));
            }

            if self.read_rewards {
                if self.rewards.is_none()
                    && let Some(pending_rewards) = self.pending_rewards.take()
                {
                    self.rewards = Some(self.resolve_pending_dataframe(pending_rewards)?);
                }

                if self.rewards.is_none()
                    && let Some(inline_rewards) = block.inline_rewards
                {
                    self.rewards_slot = Some(block.slot);
                    self.rewards = Some(inline_rewards);
                }

                if block.has_rewards_ref != self.rewards.is_some() {
                    return Err(CarReadError::InvalidData(
                        "block rewards linkage did not match the streamed rewards node".to_string(),
                    ));
                }
                if let Some(rewards_slot) = self.rewards_slot
                    && rewards_slot != block.slot
                {
                    return Err(CarReadError::InvalidData(format!(
                        "rewards node slot {rewards_slot} does not match block slot {}",
                        block.slot
                    )));
                }
            }

            self.slot = Some(block.slot);
            self.parent_slot = block.meta.parent_slot;
            self.block_time = block.meta.blocktime;
            self.block_height = block.meta.block_height;

            return Ok(true);
        }

        Ok(false)
    }

    fn resolve_pending_dataframe(
        &self,
        pending: PendingDataFrame,
    ) -> CarReadResult<OwnedDataFrame> {
        let mut frame = pending.frame;
        let mut seen = HashSet::new();
        self.append_pending_refs(&mut frame.data, &pending.next, &mut seen)?;
        Ok(frame)
    }

    fn append_pending_refs(
        &self,
        out: &mut Vec<u8>,
        refs: &[PendingDataFrameRef],
        seen: &mut HashSet<[u8; 36]>,
    ) -> CarReadResult<()> {
        for next in refs {
            match next {
                PendingDataFrameRef::Inline(bytes) => out.extend_from_slice(bytes),
                PendingDataFrameRef::Cid(cid) => {
                    if !seen.insert(*cid) {
                        return Err(CarReadError::InvalidData(
                            "cycle in rewards dataframe continuation chain".to_string(),
                        ));
                    }
                    let Some(frame) = self.dataframes.get(cid) else {
                        return Err(CarReadError::InvalidData(
                            "missing rewards dataframe continuation".to_string(),
                        ));
                    };
                    out.extend_from_slice(&frame.frame.data);
                    self.append_pending_refs(out, &frame.next, seen)?;
                }
            }
        }
        Ok(())
    }

    fn seen_transaction_count(&self) -> CarReadResult<u32> {
        if self.read_transactions {
            u32::try_from(self.tx_ranges.len())
                .map_err(|_| CarReadError::InvalidData("tx count exceeds u32::MAX".to_string()))
        } else {
            Ok(self.tx_count_seen)
        }
    }

    /// Iterate transactions in file order
    pub fn transactions<'a>(&'a mut self) -> TxIter<'a> {
        TxIter {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,

            reusable_tx: MaybeUninit::uninit(),
            reusable_meta: TransactionStatusMeta::default(),
            zstd: ZstdReusableDecoder::new(),
            has_tx: false,
            has_meta: false,
        }
    }

    /// Iterate transactions in file order without decoding status metadata.
    pub fn transactions_no_meta<'a>(&'a mut self) -> TxNoMetaIter<'a> {
        TxNoMetaIter {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,

            reusable_tx: MaybeUninit::uninit(),
            has_tx: false,
        }
    }

    /// Iterate first transaction signatures in file order without decoding transactions.
    pub fn first_signatures<'a>(&'a mut self) -> FirstSignatureIter<'a> {
        FirstSignatureIter {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,
        }
    }

    /// Iterate raw transaction and metadata frames in file order without decoding either payload.
    pub fn transaction_frames<'a>(&'a self) -> TransactionFrameIter<'a> {
        TransactionFrameIter {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,
        }
    }

    /// Iterate transactions in file order with protobuf metadata decoded as borrowed views.
    ///
    /// This opt-in iterator avoids allocating metadata strings and byte fields. It only supports
    /// protobuf-era metadata; bincode-era transaction metadata will return `TxMetaDecode`.
    pub fn transactions_borrowed_metadata<'a>(&'a mut self) -> BorrowedTxIter<'a> {
        BorrowedTxIter {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,

            reusable_tx: MaybeUninit::uninit(),
            zstd: ZstdReusableDecoder::new(),
            has_tx: false,
        }
    }

    /// Iterate transaction metadata in file order without decoding transaction payloads.
    pub fn transaction_metadata<'a>(&'a mut self) -> TxMetadataIter<'a> {
        TxMetadataIter {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,

            reusable_meta: TransactionStatusMeta::default(),
            zstd: ZstdReusableDecoder::new(),
        }
    }
}

struct BlockSummary {
    slot: u64,
    meta: SlotMeta,
    entry_count: usize,
    has_rewards_ref: bool,
    inline_rewards: Option<OwnedDataFrame>,
}

fn decode_block_summary_into(
    data: &[u8],
    shredding: &mut Vec<Shredding>,
) -> crate::node::Result<BlockSummary> {
    use minicbor::{Decoder, data::Type, decode::Error as CborError};

    let mut d = Decoder::new(data);
    let array_len = d.array()?.ok_or_else(|| {
        NodeDecodeError::Cbor(CborError::message("indefinite block array not supported"))
    })?;
    if array_len < 5 {
        return Err(NodeDecodeError::Cbor(CborError::message(
            "block array too short",
        )));
    }

    let kind = d.u64()?;
    if kind != 2 {
        return Err(NodeDecodeError::UnknownKind(kind));
    }

    let slot = d.u64()?;

    shredding.clear();
    let shredding_len = d.array()?.ok_or_else(|| {
        NodeDecodeError::Cbor(CborError::message(
            "indefinite shredding array not supported",
        ))
    })?;
    for _ in 0..shredding_len {
        shredding.push(d.decode()?);
    }

    let entry_count = d.array()?.ok_or_else(|| {
        NodeDecodeError::Cbor(CborError::message(
            "indefinite block entries array not supported",
        ))
    })? as usize;
    for _ in 0..entry_count {
        d.skip()?;
    }

    let meta: SlotMeta = d.decode()?;
    let mut inline_rewards = None;
    let has_rewards_ref = if array_len > 5 {
        if d.datatype()? == Type::Null {
            d.skip()?;
            false
        } else {
            let rewards: CborCidRef<'_> = d.decode()?;
            if let Some(data) = rewards.inline_raw_bytes() {
                inline_rewards = Some(OwnedDataFrame {
                    hash: None,
                    index: None,
                    total: None,
                    data: data.to_vec(),
                });
            }
            true
        }
    } else {
        false
    };

    Ok(BlockSummary {
        slot,
        meta,
        entry_count,
        has_rewards_ref,
        inline_rewards,
    })
}

#[cfg(test)]
mod tests {
    use super::{CarBlockGroup, TxMetadataVisit};
    use crate::confirmed_block::TransactionStatusMeta;
    use crate::metadata_decoder::TransactionStatusMetaVisitor;
    use crate::node::Shredding;
    use crate::reader::CarBlockReader;
    use minicbor::{Encoder, encode::Write};
    use prost::Message;
    use std::io::Cursor;

    #[test]
    fn reads_entries_shredding_and_rewards_without_cid_table() {
        let car = build_car();
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        let done = reader
            .read_until_block_into(&mut group)
            .expect("read first block");

        assert!(done);
        assert_eq!(group.slot, Some(42));
        assert_eq!(group.parent_slot, Some(41));
        assert_eq!(group.block_time, Some(1_700_000_000));
        assert_eq!(group.block_height, Some(99));

        assert_eq!(group.get_len().0, 2);
        assert_eq!(group.entry_count(), 2);
        assert_eq!(group.entry_tx_counts, vec![1, 1]);
        assert_eq!(group.poh_num_hashes, vec![3, 4]);
        assert_eq!(group.poh_hashes[0], [0x11; 32]);
        assert_eq!(group.poh_hashes[1], [0x22; 32]);
        assert_eq!(group.blockhash, [0x22; 32]);

        assert_eq!(
            group.shredding,
            vec![
                Shredding {
                    entry_end_idx: 10,
                    shred_end_idx: 11,
                },
                Shredding {
                    entry_end_idx: 20,
                    shred_end_idx: 21,
                },
            ]
        );

        let rewards = group.rewards.as_ref().expect("block rewards");
        assert_eq!(group.rewards_slot, Some(42));
        assert_eq!(rewards.hash, Some(7));
        assert_eq!(rewards.index, Some(0));
        assert_eq!(rewards.total, Some(1));
        assert_eq!(rewards.data, vec![0xaa, 0xbb, 0xcc]);
    }

    #[test]
    fn reads_inline_identity_rewards_from_block_ref() {
        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(&mut car, &[0x01; 36], &transaction_node(42, &[0x10]));
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &block_node_with_inline_rewards(42, 41, 1_700_000_000, 99, &[0xaa, 0xbb]),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        let done = reader
            .read_until_block_into(&mut group)
            .expect("read first block");

        assert!(done);
        let rewards = group.rewards.as_ref().expect("inline block rewards");
        assert_eq!(group.rewards_slot, Some(42));
        assert_eq!(rewards.hash, None);
        assert_eq!(rewards.index, None);
        assert_eq!(rewards.total, None);
        assert_eq!(rewards.data, vec![0xaa, 0xbb]);
    }

    #[test]
    fn reads_rewards_dataframe_continuations_by_cid() {
        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        let continuation_cid = [0x07; 36];

        push_car_entry(&mut car, &[0x01; 36], &transaction_node(42, &[0x10]));
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x05; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &continuation_cid,
            &dataframe_node(Some(7), Some(1), Some(2), &[0xcc, 0xdd]),
        );
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &rewards_node_with_next_cids(
                42,
                Some(7),
                Some(0),
                Some(2),
                &[0xaa, 0xbb],
                &[continuation_cid],
            ),
        );
        push_car_entry(
            &mut car,
            &[0x04; 36],
            &block_node(42, 41, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let rewards = group.rewards.as_ref().expect("continued rewards");
        assert_eq!(group.rewards_slot, Some(42));
        assert_eq!(rewards.hash, Some(7));
        assert_eq!(rewards.index, Some(0));
        assert_eq!(rewards.total, Some(2));
        assert_eq!(rewards.data, vec![0xaa, 0xbb, 0xcc, 0xdd]);
    }

    #[test]
    fn reads_rewards_dataframe_continuations_without_transaction_payloads() {
        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        let continuation_cid = [0x08; 36];

        push_car_entry(&mut car, &[0x01; 36], &transaction_node(42, &[0x10]));
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x05; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &continuation_cid,
            &dataframe_node(Some(7), Some(1), Some(2), &[0xcc]),
        );
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &rewards_node_with_next_cids(
                42,
                Some(7),
                Some(0),
                Some(2),
                &[0xaa, 0xbb],
                &[continuation_cid],
            ),
        );
        push_car_entry(
            &mut car,
            &[0x04; 36],
            &block_node(42, 41, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::without_transaction_payloads();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let rewards = group.rewards.as_ref().expect("continued rewards");
        assert_eq!(group.get_len().0, 1);
        assert_eq!(rewards.data, vec![0xaa, 0xbb, 0xcc]);
    }

    #[test]
    fn borrowed_metadata_iterator_decodes_protobuf_views() {
        let slot = 80_000_000;
        let metadata = TransactionStatusMeta {
            log_messages: vec!["Program log: borrowed iterator".to_string()],
            loaded_writable_addresses: vec![vec![0x77; 32]],
            compute_units_consumed: Some(0),
            cost_units: Some(91),
            ..Default::default()
        }
        .encode_to_vec();

        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(
            &mut car,
            &[0x01; 36],
            &transaction_node_with_metadata(slot, &minimal_legacy_transaction(), &metadata),
        );
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x05; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &rewards_node(slot, Some(7), Some(0), Some(1), &[0xaa]),
        );
        push_car_entry(
            &mut car,
            &[0x04; 36],
            &block_node(slot, slot - 1, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let mut iter = group.transactions_borrowed_metadata();
        {
            let tx = iter.next_tx().expect("borrowed iterator").expect("tx");
            assert_eq!(tx.transaction.signatures.len(), 0);

            let meta = tx.metadata.expect("metadata");
            assert_eq!(
                meta.log_messages[0].as_ref(),
                "Program log: borrowed iterator"
            );
            assert_eq!(meta.loaded_writable_addresses[0].as_ref(), &[0x77; 32]);
            assert_eq!(meta.compute_units_consumed, Some(0));
            assert_eq!(meta.cost_units, Some(91));
        }
        assert!(iter.next_tx().expect("borrowed iterator").is_none());
    }

    #[test]
    fn transaction_frames_expose_raw_transaction_and_metadata_without_decoding() {
        let slot = 80_000_000;
        let tx_bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let metadata = vec![0xaa, 0xbb, 0xcc];

        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(
            &mut car,
            &[0x01; 36],
            &transaction_node_with_metadata(slot, &tx_bytes, &metadata),
        );
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x04; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &[0x05; 36],
            &rewards_node(slot, Some(7), Some(0), Some(1), &[0xaa]),
        );
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &block_node(slot, slot - 1, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let mut frames = group.transaction_frames();
        let frame = frames.next_frame().expect("frame iterator").expect("frame");
        assert_eq!(frame.slot, slot);
        assert_eq!(frame.index, None);
        assert_eq!(frame.transaction_data, tx_bytes.as_slice());
        assert_eq!(frame.metadata_data, Some(metadata.as_slice()));
        assert!(frames.next_frame().expect("frame iterator").is_none());
    }

    #[test]
    fn transaction_frames_treat_empty_metadata_as_missing() {
        let slot = 80_000_000;
        let tx_bytes = vec![0xde, 0xad, 0xbe, 0xef];

        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(&mut car, &[0x01; 36], &transaction_node(slot, &tx_bytes));
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x04; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &[0x05; 36],
            &rewards_node(slot, Some(7), Some(0), Some(1), &[0xaa]),
        );
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &block_node(slot, slot - 1, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let mut frames = group.transaction_frames();
        let frame = frames.next_frame().expect("frame iterator").expect("frame");
        assert_eq!(frame.transaction_data, tx_bytes.as_slice());
        assert_eq!(frame.metadata_data, None);
        assert!(frames.next_frame().expect("frame iterator").is_none());
    }

    #[test]
    fn metadata_iterator_decodes_metadata_without_transaction_payload() {
        let slot = 80_000_000;
        let metadata = TransactionStatusMeta {
            log_messages: vec!["Program log: metadata only".to_string()],
            ..Default::default()
        }
        .encode_to_vec();

        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(
            &mut car,
            &[0x01; 36],
            &transaction_node_with_metadata(slot, &[0xff, 0x00], &metadata),
        );
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x05; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &rewards_node(slot, Some(7), Some(0), Some(1), &[0xaa]),
        );
        push_car_entry(
            &mut car,
            &[0x04; 36],
            &block_node(slot, slot - 1, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let mut iter = group.transaction_metadata();
        let tx = iter
            .next_metadata()
            .expect("metadata iterator")
            .expect("tx metadata");
        assert_eq!(tx.slot, slot);
        assert_eq!(
            tx.metadata.expect("metadata").log_messages[0],
            "Program log: metadata only"
        );
        assert!(iter.next_metadata().expect("metadata iterator").is_none());
    }

    #[test]
    fn metadata_visitor_reads_logs_without_transaction_payload() {
        let slot = 80_000_000;
        let metadata = TransactionStatusMeta {
            log_messages: vec!["Program log: visitor metadata only".to_string()],
            ..Default::default()
        }
        .encode_to_vec();

        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(
            &mut car,
            &[0x01; 36],
            &transaction_node_with_metadata(slot, &[0xff, 0x00], &metadata),
        );
        push_car_entry(&mut car, &[0x02; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut car, &[0x05; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &[0x03; 36],
            &rewards_node(slot, Some(7), Some(0), Some(1), &[0xaa]),
        );
        push_car_entry(
            &mut car,
            &[0x04; 36],
            &block_node(slot, slot - 1, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::new();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let mut iter = group.transaction_metadata();
        let mut visitor = CollectingLogVisitor::default();
        let tx = iter
            .next_metadata_visit(&mut visitor)
            .expect("metadata visitor")
            .expect("tx metadata");

        assert!(matches!(tx, TxMetadataVisit::ProtobufVisited { slot: s, .. } if s == slot));
        assert_eq!(visitor.logs, vec!["Program log: visitor metadata only"]);
    }

    #[test]
    fn first_signature_iterator_reads_signature_prefix_only() {
        let slot = 80_000_000;
        let signature = [0x42; 64];
        let mut large_signed_tx = minimal_legacy_transaction_with_signature(signature);
        large_signed_tx.extend_from_slice(&[0xaa; 512]);
        let large_signed_node = transaction_node(slot, &large_signed_tx);

        let mut car = Vec::new();
        push_uvarint(&mut car, 1);
        car.push(0);

        push_car_entry(
            &mut car,
            &[0x01; 36],
            &transaction_node(slot, &minimal_legacy_transaction()),
        );
        push_car_entry(&mut car, &[0x02; 36], &large_signed_node);
        push_car_entry(&mut car, &[0x03; 36], &entry_node(3, [0x11; 32], 2));
        push_car_entry(&mut car, &[0x04; 36], &entry_node(4, [0x22; 32], 0));
        push_car_entry(
            &mut car,
            &[0x05; 36],
            &rewards_node(slot, Some(7), Some(0), Some(1), &[0xaa]),
        );
        push_car_entry(
            &mut car,
            &[0x06; 36],
            &block_node(slot, slot - 1, 1_700_000_000, 99),
        );

        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut group = CarBlockGroup::with_transaction_signature_prefixes();
        assert!(
            reader
                .read_until_block_into(&mut group)
                .expect("read first block")
        );

        let (tx_count, tx_bytes) = group.get_len();
        assert_eq!(tx_count, 2);
        assert!(tx_bytes < large_signed_node.len());

        let mut iter = group.first_signatures();
        assert_eq!(
            iter.next_signature().expect("first signature").copied(),
            Some(signature)
        );
        assert!(iter.next_signature().expect("end").is_none());
    }

    #[derive(Default)]
    struct CollectingLogVisitor {
        logs: Vec<String>,
        none: bool,
    }

    impl<'a> TransactionStatusMetaVisitor<'a> for CollectingLogVisitor {
        fn wants_log_messages(&self) -> bool {
            true
        }

        fn log_message(&mut self, message: &'a str) {
            self.logs.push(message.to_owned());
        }

        fn log_messages_none(&mut self, none: bool) {
            self.none = none;
        }
    }

    fn build_car() -> Vec<u8> {
        let mut out = Vec::new();
        push_uvarint(&mut out, 1);
        out.push(0);

        push_car_entry(&mut out, &[0x01; 36], &transaction_node(42, &[0x10]));
        push_car_entry(&mut out, &[0x02; 36], &transaction_node(42, &[0x20]));
        push_car_entry(&mut out, &[0x03; 36], &entry_node(3, [0x11; 32], 1));
        push_car_entry(&mut out, &[0x04; 36], &entry_node(4, [0x22; 32], 1));
        push_car_entry(
            &mut out,
            &[0x05; 36],
            &rewards_node(42, Some(7), Some(0), Some(1), &[0xaa, 0xbb, 0xcc]),
        );
        push_car_entry(
            &mut out,
            &[0x06; 36],
            &block_node(42, 41, 1_700_000_000, 99),
        );

        out
    }

    fn push_car_entry(out: &mut Vec<u8>, cid: &[u8; 36], payload: &[u8]) {
        push_uvarint(out, (cid.len() + payload.len()) as u64);
        out.extend_from_slice(cid);
        out.extend_from_slice(payload);
    }

    fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
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

    fn transaction_node(slot: u64, tx_bytes: &[u8]) -> Vec<u8> {
        transaction_node_with_metadata(slot, tx_bytes, &[])
    }

    fn transaction_node_with_metadata(slot: u64, tx_bytes: &[u8], metadata: &[u8]) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        e.array(5).expect("tx array").u64(0).expect("kind");
        encode_dataframe(&mut e, None, None, None, tx_bytes);
        encode_dataframe(&mut e, None, None, None, metadata);
        e.u64(slot).expect("slot");
        e.null().expect("index");
        e.into_writer()
    }

    fn minimal_legacy_transaction() -> Vec<u8> {
        let mut out = Vec::with_capacity(38);
        out.push(0); // signatures len
        out.extend_from_slice(&[0, 0, 0]); // message header
        out.push(0); // account keys len
        out.extend_from_slice(&[0; 32]); // recent blockhash
        out.push(0); // instructions len
        out
    }

    fn minimal_legacy_transaction_with_signature(signature: [u8; 64]) -> Vec<u8> {
        let mut out = Vec::with_capacity(102);
        out.push(1); // signatures len
        out.extend_from_slice(&signature);
        out.extend_from_slice(&[1, 0, 0]); // message header
        out.push(1); // account keys len
        out.extend_from_slice(&[0x55; 32]); // payer
        out.extend_from_slice(&[0; 32]); // recent blockhash
        out.push(0); // instructions len
        out
    }

    fn entry_node(num_hashes: u64, hash: [u8; 32], tx_count: usize) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        e.array(4).expect("entry array").u64(1).expect("kind");
        e.u64(num_hashes).expect("num_hashes");
        e.bytes(&hash).expect("hash");
        e.array(tx_count as u64).expect("tx refs");
        for i in 0..tx_count {
            encode_cid_ref(&mut e, (0x30 + i) as u8);
        }
        e.into_writer()
    }

    fn rewards_node(
        slot: u64,
        hash: Option<u64>,
        index: Option<u64>,
        total: Option<u64>,
        data: &[u8],
    ) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        e.array(3).expect("rewards array").u64(5).expect("kind");
        e.u64(slot).expect("slot");
        encode_dataframe(&mut e, hash, index, total, data);
        e.into_writer()
    }

    fn rewards_node_with_next_cids(
        slot: u64,
        hash: Option<u64>,
        index: Option<u64>,
        total: Option<u64>,
        data: &[u8],
        next: &[[u8; 36]],
    ) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        e.array(3).expect("rewards array").u64(5).expect("kind");
        e.u64(slot).expect("slot");
        encode_dataframe_with_next_cids(&mut e, hash, index, total, data, next);
        e.into_writer()
    }

    fn dataframe_node(
        hash: Option<u64>,
        index: Option<u64>,
        total: Option<u64>,
        data: &[u8],
    ) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        encode_dataframe(&mut e, hash, index, total, data);
        e.into_writer()
    }

    fn block_node(slot: u64, parent_slot: u64, block_time: i64, block_height: u64) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        e.array(6).expect("block array").u64(2).expect("kind");
        e.u64(slot).expect("slot");

        e.array(2).expect("shredding");
        e.array(2).expect("shred 0");
        e.i64(10).expect("entry_end_idx 0");
        e.i64(11).expect("shred_end_idx 0");
        e.array(2).expect("shred 1");
        e.i64(20).expect("entry_end_idx 1");
        e.i64(21).expect("shred_end_idx 1");

        e.array(2).expect("entries");
        encode_cid_ref(&mut e, 0x40);
        encode_cid_ref(&mut e, 0x41);

        e.array(3).expect("meta");
        e.u64(parent_slot).expect("parent_slot");
        e.i64(block_time).expect("block_time");
        e.u64(block_height).expect("block_height");

        encode_cid_ref(&mut e, 0x50);
        e.into_writer()
    }

    fn block_node_with_inline_rewards(
        slot: u64,
        parent_slot: u64,
        block_time: i64,
        block_height: u64,
        rewards: &[u8],
    ) -> Vec<u8> {
        let mut e = Encoder::new(Vec::new());
        e.array(6).expect("block array").u64(2).expect("kind");
        e.u64(slot).expect("slot");

        e.array(1).expect("shredding");
        e.array(2).expect("shred 0");
        e.i64(10).expect("entry_end_idx 0");
        e.i64(11).expect("shred_end_idx 0");

        e.array(1).expect("entries");
        encode_cid_ref(&mut e, 0x40);

        e.array(3).expect("meta");
        e.u64(parent_slot).expect("parent_slot");
        e.i64(block_time).expect("block_time");
        e.u64(block_height).expect("block_height");

        encode_inline_identity_cid_ref(&mut e, rewards);
        e.into_writer()
    }

    fn encode_dataframe<W: Write>(
        e: &mut Encoder<W>,
        hash: Option<u64>,
        index: Option<u64>,
        total: Option<u64>,
        data: &[u8],
    ) where
        W::Error: std::fmt::Debug,
    {
        e.array(6).expect("dataframe array").u64(6).expect("kind");
        encode_optional_u64(e, hash);
        encode_optional_u64(e, index);
        encode_optional_u64(e, total);
        e.bytes(data).expect("data");
        e.null().expect("next");
    }

    fn encode_dataframe_with_next_cids<W: Write>(
        e: &mut Encoder<W>,
        hash: Option<u64>,
        index: Option<u64>,
        total: Option<u64>,
        data: &[u8],
        next: &[[u8; 36]],
    ) where
        W::Error: std::fmt::Debug,
    {
        e.array(6).expect("dataframe array").u64(6).expect("kind");
        encode_optional_u64(e, hash);
        encode_optional_u64(e, index);
        encode_optional_u64(e, total);
        e.bytes(data).expect("data");
        e.array(next.len() as u64).expect("next");
        for cid in next {
            encode_car_cid_ref(e, cid);
        }
    }

    fn encode_optional_u64<W: Write>(e: &mut Encoder<W>, value: Option<u64>)
    where
        W::Error: std::fmt::Debug,
    {
        if let Some(value) = value {
            e.u64(value).expect("u64");
        } else {
            e.null().expect("null");
        }
    }

    fn encode_cid_ref<W: Write>(e: &mut Encoder<W>, fill: u8)
    where
        W::Error: std::fmt::Debug,
    {
        e.tag(minicbor::data::Tag::new(42)).expect("cid tag");
        let bytes = [fill; 37];
        e.bytes(&bytes).expect("cid bytes");
    }

    fn encode_car_cid_ref<W: Write>(e: &mut Encoder<W>, cid: &[u8; 36])
    where
        W::Error: std::fmt::Debug,
    {
        let mut bytes = [0u8; 37];
        bytes[1..].copy_from_slice(cid);
        e.tag(minicbor::data::Tag::new(42)).expect("cid tag");
        e.bytes(&bytes).expect("cid bytes");
    }

    fn encode_inline_identity_cid_ref<W: Write>(e: &mut Encoder<W>, data: &[u8])
    where
        W::Error: std::fmt::Debug,
    {
        let mut bytes = vec![0, 1, 0x55, 0];
        push_uvarint(&mut bytes, data.len() as u64);
        bytes.extend_from_slice(data);
        e.tag(minicbor::data::Tag::new(42)).expect("cid tag");
        e.bytes(&bytes).expect("inline identity cid bytes");
    }
}

pub struct TxIter<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_ranges: &'a [(u32, u32)],
    pos: usize,

    reusable_tx: MaybeUninit<VersionedTransaction<'a>>,
    reusable_meta: TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
    has_tx: bool,
    has_meta: bool,
}

impl<'a> Drop for TxIter<'a> {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

impl<'a> TxIter<'a> {
    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    fn decode_next_in_place(&mut self) -> Result<bool, GroupError> {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);

            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.data.next.is_some() || tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            if self.has_tx {
                unsafe { self.reusable_tx.assume_init_drop() };
                self.has_tx = false;
            }

            VersionedTransaction::deserialize_into(tx.data.data, &mut self.reusable_tx)
                .map_err(|_| GroupError::TxDecode)?;

            let has_metadata = !tx.metadata.data.is_empty();
            if has_metadata {
                decode_transaction_status_meta_from_frame(
                    tx.slot,
                    tx.metadata.data,
                    &mut self.reusable_meta,
                    &mut self.zstd,
                )
                .map_err(|_| GroupError::TxMetaDecode)?;
            }

            self.has_tx = true;
            self.has_meta = has_metadata;
            return Ok(true);
        }

        Ok(false)
    }

    #[inline]
    pub fn next_tx(
        &mut self,
    ) -> Result<Option<(&VersionedTransaction<'a>, Option<&TransactionStatusMeta>)>, GroupError>
    {
        if !self.decode_next_in_place()? {
            return Ok(None);
        }
        let tx = unsafe { self.reusable_tx.assume_init_ref() };
        Ok(Some((tx, self.has_meta.then_some(&self.reusable_meta))))
    }
}

pub struct TxNoMetaIter<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_ranges: &'a [(u32, u32)],
    pos: usize,

    reusable_tx: MaybeUninit<VersionedTransaction<'a>>,
    has_tx: bool,
}

impl<'a> Drop for TxNoMetaIter<'a> {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

impl<'a> TxNoMetaIter<'a> {
    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    fn decode_next_in_place(&mut self) -> Result<bool, GroupError> {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);

            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.data.next.is_some() || tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            if self.has_tx {
                unsafe { self.reusable_tx.assume_init_drop() };
                self.has_tx = false;
            }

            VersionedTransaction::deserialize_into(tx.data.data, &mut self.reusable_tx)
                .map_err(|_| GroupError::TxDecode)?;

            self.has_tx = true;
            return Ok(true);
        }

        Ok(false)
    }

    #[inline]
    pub fn next_tx(&mut self) -> Result<Option<&VersionedTransaction<'a>>, GroupError> {
        if !self.decode_next_in_place()? {
            return Ok(None);
        }
        Ok(Some(unsafe { self.reusable_tx.assume_init_ref() }))
    }
}

pub struct FirstSignatureIter<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_ranges: &'a [(u32, u32)],
    pos: usize,
}

impl<'a> FirstSignatureIter<'a> {
    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    pub fn next_signature(&mut self) -> Result<Option<&'a [u8; 64]>, GroupError> {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);
            let Some(signature) = first_signature_from_transaction_node_prefix(payload)? else {
                continue;
            };
            return Ok(Some(signature));
        }

        Ok(None)
    }
}

pub struct TransactionFrame<'a> {
    pub slot: u64,
    pub index: Option<u64>,
    pub transaction_data: &'a [u8],
    pub metadata_data: Option<&'a [u8]>,
}

pub struct TransactionFrameIter<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_ranges: &'a [(u32, u32)],
    pos: usize,
}

impl<'a> TransactionFrameIter<'a> {
    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    pub fn next_frame(&mut self) -> Result<Option<TransactionFrame<'a>>, GroupError> {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);
            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.data.next.is_some() || tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            let has_metadata = !tx.metadata.data.is_empty();
            return Ok(Some(TransactionFrame {
                slot: tx.slot,
                index: tx.index,
                transaction_data: tx.data.data,
                metadata_data: has_metadata.then_some(tx.metadata.data),
            }));
        }

        Ok(None)
    }
}

#[inline]
fn first_signature_from_transaction_node_prefix(
    payload: &[u8],
) -> Result<Option<&[u8; 64]>, GroupError> {
    let mut d = minicbor::Decoder::new(payload);
    let array_len = d
        .array()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?
        .ok_or_else(|| GroupError::Other("indefinite transaction node array".to_string()))?;
    if array_len < 2 {
        return Err(GroupError::TxDecode);
    }

    let kind = d
        .u64()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?;
    if kind != 0 {
        return Ok(None);
    }

    let frame_len = d
        .array()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?
        .ok_or_else(|| GroupError::Other("indefinite transaction data frame".to_string()))?;
    if frame_len < 5 {
        return Err(GroupError::TxDecode);
    }
    d.skip()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?;
    d.skip()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?;
    d.skip()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?;
    d.skip()
        .map_err(|err| GroupError::Node(NodeDecodeError::Cbor(err)))?;

    let tx_data = cbor_byte_string_value_prefix(&payload[d.position()..])?;
    first_signature_from_transaction(tx_data)
}

#[inline]
fn cbor_byte_string_value_prefix(input: &[u8]) -> Result<&[u8], GroupError> {
    let first = *input.first().ok_or(GroupError::TxDecode)?;
    if first >> 5 != 2 {
        return Err(GroupError::TxDecode);
    }

    let info = first & 0x1f;
    let (len, header_len) = match info {
        0..=23 => (info as usize, 1usize),
        24 => (*input.get(1).ok_or(GroupError::TxDecode)? as usize, 2usize),
        25 => {
            let bytes: [u8; 2] = input
                .get(1..3)
                .ok_or(GroupError::TxDecode)?
                .try_into()
                .map_err(|_| GroupError::TxDecode)?;
            (u16::from_be_bytes(bytes) as usize, 3usize)
        }
        26 => {
            let bytes: [u8; 4] = input
                .get(1..5)
                .ok_or(GroupError::TxDecode)?
                .try_into()
                .map_err(|_| GroupError::TxDecode)?;
            (
                usize::try_from(u32::from_be_bytes(bytes)).map_err(|_| GroupError::TxDecode)?,
                5usize,
            )
        }
        27 => {
            let bytes: [u8; 8] = input
                .get(1..9)
                .ok_or(GroupError::TxDecode)?
                .try_into()
                .map_err(|_| GroupError::TxDecode)?;
            (
                usize::try_from(u64::from_be_bytes(bytes)).map_err(|_| GroupError::TxDecode)?,
                9usize,
            )
        }
        _ => return Err(GroupError::TxDecode),
    };

    let value = input.get(header_len..).ok_or(GroupError::TxDecode)?;
    let available_len = value.len().min(len);
    Ok(&value[..available_len])
}

#[inline]
fn first_signature_from_transaction(tx_data: &[u8]) -> Result<Option<&[u8; 64]>, GroupError> {
    let (signature_count, prefix_len) =
        decode_shortu16_len(tx_data).map_err(|_| GroupError::TxDecode)?;
    if signature_count == 0 {
        return Ok(None);
    }

    let first_signature_end = prefix_len.checked_add(64).ok_or(GroupError::TxDecode)?;
    let signature = tx_data
        .get(prefix_len..first_signature_end)
        .ok_or(GroupError::TxDecode)?;
    signature
        .try_into()
        .map(Some)
        .map_err(|_| GroupError::TxDecode)
}

pub struct TxMetadata<'iter> {
    pub slot: u64,
    pub index: Option<u64>,
    pub metadata: Option<&'iter TransactionStatusMeta>,
}

pub enum TxMetadataVisit<'iter> {
    /// The transaction node did not contain a metadata frame.
    Missing { slot: u64, index: Option<u64> },
    /// Protobuf metadata was decoded through `TransactionStatusMetaVisitor`.
    ProtobufVisited { slot: u64, index: Option<u64> },
    /// Older bincode metadata was materialized because the visitor decoder is protobuf-only.
    LegacyDecoded {
        slot: u64,
        index: Option<u64>,
        metadata: &'iter TransactionStatusMeta,
    },
}

pub struct TxMetadataIter<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_ranges: &'a [(u32, u32)],
    pos: usize,

    reusable_meta: TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
}

impl<'a> TxMetadataIter<'a> {
    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    pub fn next_metadata<'iter>(&'iter mut self) -> Result<Option<TxMetadata<'iter>>, GroupError>
    where
        'a: 'iter,
    {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);
            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            let has_metadata = !tx.metadata.data.is_empty();
            if has_metadata {
                decode_transaction_status_meta_from_frame(
                    tx.slot,
                    tx.metadata.data,
                    &mut self.reusable_meta,
                    &mut self.zstd,
                )
                .map_err(|_| GroupError::TxMetaDecode)?;
            }

            return Ok(Some(TxMetadata {
                slot: tx.slot,
                index: tx.index,
                metadata: has_metadata.then_some(&self.reusable_meta),
            }));
        }

        Ok(None)
    }

    /// Visit protobuf-era metadata without materializing it.
    ///
    /// Bincode-era metadata is decoded into this iterator's reusable `TransactionStatusMeta` and
    /// returned as `LegacyDecoded` so callers can keep a single scan path across historical slots.
    #[inline]
    pub fn next_metadata_visit<'iter, V>(
        &'iter mut self,
        visitor: &mut V,
    ) -> Result<Option<TxMetadataVisit<'iter>>, GroupError>
    where
        'a: 'iter,
        V: TransactionStatusMetaVisitor<'iter> + ?Sized,
    {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);
            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            if tx.metadata.data.is_empty() {
                return Ok(Some(TxMetadataVisit::Missing {
                    slot: tx.slot,
                    index: tx.index,
                }));
            }

            if slot_uses_protobuf_metadata(tx.slot) {
                let metadata_bytes: &'iter [u8] = if self
                    .zstd
                    .decompress_if_zstd(tx.metadata.data)
                    .map_err(|_| GroupError::TxMetaDecode)?
                {
                    self.zstd.output()
                } else {
                    tx.metadata.data
                };

                visit_protobuf_transaction_status_meta(metadata_bytes, visitor)
                    .map_err(|_| GroupError::TxMetaDecode)?;

                return Ok(Some(TxMetadataVisit::ProtobufVisited {
                    slot: tx.slot,
                    index: tx.index,
                }));
            }

            decode_transaction_status_meta_from_frame(
                tx.slot,
                tx.metadata.data,
                &mut self.reusable_meta,
                &mut self.zstd,
            )
            .map_err(|_| GroupError::TxMetaDecode)?;

            return Ok(Some(TxMetadataVisit::LegacyDecoded {
                slot: tx.slot,
                index: tx.index,
                metadata: &self.reusable_meta,
            }));
        }

        Ok(None)
    }
}

pub struct BorrowedTx<'group, 'iter> {
    pub transaction: &'iter VersionedTransaction<'group>,
    pub metadata: Option<BorrowedTransactionStatusMetaView<'iter>>,
}

pub struct BorrowedTxIter<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_ranges: &'a [(u32, u32)],
    pos: usize,

    reusable_tx: MaybeUninit<VersionedTransaction<'a>>,
    zstd: ZstdReusableDecoder,
    has_tx: bool,
}

impl<'a> Drop for BorrowedTxIter<'a> {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

impl<'a> BorrowedTxIter<'a> {
    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    pub fn next_tx<'iter>(&'iter mut self) -> Result<Option<BorrowedTx<'a, 'iter>>, GroupError>
    where
        'a: 'iter,
    {
        while self.pos < self.tx_ranges.len() {
            let (s, e) = self.tx_ranges[self.pos];
            self.pos += 1;

            let payload = self.tx_payload(s, e);

            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.data.next.is_some() || tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            if self.has_tx {
                unsafe { self.reusable_tx.assume_init_drop() };
                self.has_tx = false;
            }

            VersionedTransaction::deserialize_into(tx.data.data, &mut self.reusable_tx)
                .map_err(|_| GroupError::TxDecode)?;
            self.has_tx = true;

            let metadata = if tx.metadata.data.is_empty() {
                None
            } else {
                if !slot_uses_protobuf_metadata(tx.slot) {
                    return Err(GroupError::TxMetaDecode);
                }

                let metadata_bytes: &'iter [u8] = if self
                    .zstd
                    .decompress_if_zstd(tx.metadata.data)
                    .map_err(|_| GroupError::TxMetaDecode)?
                {
                    self.zstd.output()
                } else {
                    tx.metadata.data
                };

                Some(
                    decode_protobuf_transaction_status_meta_borrowed(metadata_bytes)
                        .map_err(|_| GroupError::TxMetaDecode)?,
                )
            };

            let transaction = unsafe { self.reusable_tx.assume_init_ref() };
            return Ok(Some(BorrowedTx {
                transaction,
                metadata,
            }));
        }

        Ok(None)
    }
}
