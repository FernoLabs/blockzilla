use std::io::Read;
use std::mem::MaybeUninit;
use wincode::Deserialize;

use crate::confirmed_block::TransactionStatusMeta;
use crate::error::{CarReadError, CarReadResult, GroupError};
use crate::metadata_decoder::{decode_transaction_status_meta_from_frame, ZstdReusableDecoder};
use crate::node::{
    decode_node, is_block_node, is_entry_node, is_transaction_node, CborArrayIter, CborCidRef, Node,
    NodeDecodeError,
};
use crate::versioned_transaction::VersionedTransaction;

// Keep existing crate call sites compiling.
pub type CarBlockGroup = CarBlockGroupSafe;
pub type TxIter<'a> = TxIterSafe<'a>;


#[inline(always)]
fn cid_id(cid_bytes: &[u8; 36]) -> u64 {
    gxhash::gxhash64(cid_bytes, 0)
}

/// SAFE CarBlockGroup:
/// - stores Transaction payloads (indexed by CID id)
/// - stores Entry payloads (indexed by CID id)
/// - on iteration, follows links: Block -> Entry -> Transaction
/// - during ingest, decodes Entry only to extract entry.hash for blockhash tracking
pub struct CarBlockGroupSafe {
    // tx storage
    tx_buf: Vec<u8>,
    tx_map: Vec<(u64, u32, u32)>, // id -> (s,e)

    // entry storage
    entry_buf: Vec<u8>,
    entry_map: Vec<(u64, u32, u32)>, // id -> (s,e)

    // block storage
    block_buf: Vec<u8>,
    block_range: (u32, u32),

    // scratch
    scratch: Vec<u8>,

    // blockhash tracking via last Entry.hash
    last_entry_hash: [u8; 32],
    has_last_entry_hash: bool,
    last_entry_cid_id: u64,
}

impl Default for CarBlockGroupSafe {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroupSafe {
    pub fn new() -> Self {
        Self {
            tx_buf: Vec::with_capacity(10 * 1024 * 1024),
            tx_map: Vec::with_capacity(8192),

            entry_buf: Vec::with_capacity(2 * 1024 * 1024),
            entry_map: Vec::with_capacity(4096),

            block_buf: Vec::with_capacity(2 * 1024 * 1024),
            block_range: (0, 0),

            scratch: Vec::with_capacity(1 * 1024 * 1024),

            last_entry_hash: [0u8; 32],
            has_last_entry_hash: false,
            last_entry_cid_id: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.tx_buf.clear();
        self.tx_map.clear();

        self.entry_buf.clear();
        self.entry_map.clear();

        self.block_buf.clear();
        self.block_range = (0, 0);

        self.scratch.clear();

        self.last_entry_hash = [0u8; 32];
        self.has_last_entry_hash = false;
        self.last_entry_cid_id = 0;
    }

    /// Returns (entries_count, tx_buf_bytes).
    pub fn get_len(&self) -> (usize, usize) {
        (self.entry_map.len(), self.tx_buf.len())
    }

    #[inline(always)]
    pub fn block_payload(&self) -> &[u8] {
        let (s, e) = self.block_range;
        let s = s as usize;
        let e = e as usize;

        debug_assert!(s <= e);
        debug_assert!(e <= self.block_buf.len());

        unsafe { std::slice::from_raw_parts(self.block_buf.as_ptr().add(s), e - s) }
    }

    /// Blockhash from the "last Entry seen" assumption.
    pub fn blockhash_checked(&self) -> Result<[u8; 32], GroupError> {
        if !self.has_last_entry_hash {
            return Err(GroupError::Other("missing last entry hash".to_string()));
        }

        #[cfg(debug_assertions)]
        {
            let block = match decode_node(self.block_payload()).map_err(GroupError::Node)? {
                Node::Block(bk) => bk,
                _ => return Err(GroupError::WrongRootKind),
            };

            let n = block.entries.len();
            if n == 0 {
                return Err(GroupError::Other("entries array is empty".to_string()));
            }

            let last_entry_cid = block
                .entries
                .decode_at(n - 1)
                .map_err(|e| GroupError::Other(format!("decode last entry cid: {e}")))?;

            let expected = gxhash::gxhash64(last_entry_cid.hash_bytes(), 0);
            debug_assert!(
                expected == self.last_entry_cid_id,
                "entry order assumption violated: last entry in block != last entry seen"
            );
        }

        Ok(self.last_entry_hash)
    }

    /// Fill group by reading one section payload.
    ///
    /// Returns Ok(true) when the block node is reached (group complete).
    #[inline]
    pub fn read_entry_payload_into<R: Read>(
        &mut self,
        reader: &mut R,
        cid_bytes: &[u8; 36],
        entry_len: usize, // cid + payload (no uvarint prefix)
    ) -> CarReadResult<bool> {
        let payload_len = entry_len
            .checked_sub(cid_bytes.len())
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;

        self.scratch.clear();
        self.scratch.resize(payload_len, 0u8);

        if let Err(e) = reader.read_exact(&mut self.scratch) {
            self.scratch.clear();
            return Err(CarReadError::Io(e.to_string()));
        }

        // Block
        if is_block_node(&self.scratch) {
            let start = self.block_buf.len();
            let end = start + payload_len;

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData("block buffer exceeds u32::MAX".to_string()));
            }

            self.block_buf.extend_from_slice(&self.scratch);
            self.block_range = (start as u32, end as u32);
            return Ok(true);
        }

        // Transaction
        if is_transaction_node(&self.scratch) {
            let id = cid_id(cid_bytes);

            let start = self.tx_buf.len();
            let end = start + payload_len;

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData("tx buffer exceeds u32::MAX".to_string()));
            }

            self.tx_buf.extend_from_slice(&self.scratch);
            self.tx_map.push((id, start as u32, end as u32));
            return Ok(false);
        }

        // Entry
        if is_entry_node(&self.scratch) {
            let id = cid_id(cid_bytes);

            // Decode only to extract entry.hash for blockhash tracking.
            let node =
                decode_node(&self.scratch).map_err(|e| CarReadError::InvalidData(e.to_string()))?;
            let Node::Entry(entry) = node else {
                return Ok(false);
            };

            if entry.hash.len() != 32 {
                return Err(CarReadError::InvalidData("entry.hash len != 32".to_string()));
            }
            self.last_entry_hash.copy_from_slice(entry.hash);
            self.has_last_entry_hash = true;
            self.last_entry_cid_id = id;

            // Store payload for safe resolution.
            let start = self.entry_buf.len();
            let end = start + payload_len;

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData(
                    "entry buffer exceeds u32::MAX".to_string(),
                ));
            }

            self.entry_buf.extend_from_slice(&self.scratch);
            self.entry_map.push((id, start as u32, end as u32));
            return Ok(false);
        }

        Ok(false)
    }

    pub fn transactions<'a>(&'a mut self) -> Result<TxIterSafe<'a>, GroupError> {
        // Important: avoid borrowing self immutably across the returned iterator.
        let (bs, be) = self.block_range;
        let block_ptr = self.block_buf.as_ptr();
        let block_len = self.block_buf.len();

        let bs = bs as usize;
        let be = be as usize;

        debug_assert!(bs <= be);
        debug_assert!(be <= block_len);

        let block_payload: &[u8] =
            unsafe { std::slice::from_raw_parts(block_ptr.add(bs), be - bs) };

        let block = match decode_node(block_payload).map_err(GroupError::Node)? {
            Node::Block(bk) => bk,
            _ => return Err(GroupError::WrongRootKind),
        };

        let entry_iter = block
            .entries
            .iter_stateful()
            .map_err(|e| GroupError::Node(NodeDecodeError::from(e)))?;

        Ok(TxIterSafe {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_map: &mut self.tx_map,

            entry_buf_ptr: self.entry_buf.as_ptr(),
            entry_buf_len: self.entry_buf.len(),
            entry_map: &mut self.entry_map,

            entry_iter,
            cur_entry_tx_ids: Vec::new(),
            cur_pos: 0,

            reusable_tx: MaybeUninit::uninit(),
            reusable_meta: TransactionStatusMeta::default(),
            zstd: ZstdReusableDecoder::new(),
            has_tx: false,
            has_meta: false,
        })
    }
}

pub struct TxIterSafe<'a> {
    tx_buf_ptr: *const u8,
    tx_buf_len: usize,
    tx_map: &'a mut Vec<(u64, u32, u32)>,

    entry_buf_ptr: *const u8,
    entry_buf_len: usize,
    entry_map: &'a mut Vec<(u64, u32, u32)>,

    entry_iter: CborArrayIter<'a, CborCidRef<'a>>,

    cur_entry_tx_ids: Vec<u64>,
    cur_pos: usize,

    reusable_tx: MaybeUninit<VersionedTransaction<'a>>,
    reusable_meta: TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
    has_tx: bool,
    has_meta: bool,
}

impl<'a> Drop for TxIterSafe<'a> {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

impl<'a> TxIterSafe<'a> {
    #[inline(always)]
    fn take_entry_range(&mut self, entry_id: u64) -> Option<(u32, u32)> {
        for i in (0..self.entry_map.len()).rev() {
            if unsafe { self.entry_map.get_unchecked(i).0 } == entry_id {
                let (_, s, e) = self.entry_map.swap_remove(i);
                return Some((s, e));
            }
        }
        None
    }

    #[inline(always)]
    fn take_tx_range(&mut self, tx_id: u64) -> Option<(u32, u32)> {
        for i in (0..self.tx_map.len()).rev() {
            if unsafe { self.tx_map.get_unchecked(i).0 } == tx_id {
                let (_, s, e) = self.tx_map.swap_remove(i);
                return Some((s, e));
            }
        }
        None
    }

    #[inline(always)]
    fn entry_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.entry_buf_len);
        unsafe { std::slice::from_raw_parts(self.entry_buf_ptr.add(s), e - s) }
    }

    #[inline(always)]
    fn tx_payload(&self, s: u32, e: u32) -> &'a [u8] {
        let s = s as usize;
        let e = e as usize;
        debug_assert!(s <= e);
        debug_assert!(e <= self.tx_buf_len);
        unsafe { std::slice::from_raw_parts(self.tx_buf_ptr.add(s), e - s) }
    }

    #[inline]
    fn decode_error(e: impl Into<NodeDecodeError>) -> GroupError {
        GroupError::Node(e.into())
    }

    #[inline]
    fn load_next_entry(&mut self) -> Result<bool, GroupError> {
        while let Some(entry_cid) = self.entry_iter.next_item() {
            let entry_cid = entry_cid.map_err(Self::decode_error)?;
            let entry_id = gxhash::gxhash64(entry_cid.hash_bytes(), 0);

            let (s, e) = self.take_entry_range(entry_id).ok_or(GroupError::MissingCid)?;
            let payload = self.entry_payload(s, e);

            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Entry(entry) = node else {
                return Err(GroupError::Other("expected Entry node".to_string()));
            };

            self.cur_entry_tx_ids.clear();

            let mut it = entry
                .transactions
                .iter_stateful()
                .map_err(|e| GroupError::Node(NodeDecodeError::from(e)))?;

            while let Some(tx_cid) = it.next_item() {
                let tx_cid = tx_cid.map_err(Self::decode_error)?;
                let tx_id = gxhash::gxhash64(tx_cid.hash_bytes(), 0);
                self.cur_entry_tx_ids.push(tx_id);
            }

            if self.cur_entry_tx_ids.is_empty() {
                continue;
            }

            self.cur_pos = 0;
            return Ok(true);
        }
        Ok(false)
    }

    #[inline]
    fn decode_next_in_place(&mut self) -> Result<bool, GroupError> {
        loop {
            if self.cur_pos >= self.cur_entry_tx_ids.len() {
                self.cur_entry_tx_ids.clear();
                self.cur_pos = 0;

                if !self.load_next_entry()? {
                    return Ok(false);
                }
            }

            let tx_id = self.cur_entry_tx_ids[self.cur_pos];
            self.cur_pos += 1;

            let (s, e) = self.take_tx_range(tx_id).ok_or(GroupError::MissingCid)?;
            let payload = self.tx_payload(s, e);

            let node = decode_node(payload).map_err(GroupError::Node)?;
            let Node::Transaction(tx) = node else {
                continue;
            };

            if tx.data.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }
            if tx.metadata.next.is_some() {
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

/// UNCHECKED CarBlockGroup:
/// - stores Transaction payloads, and records them in file order
/// - does NOT store Entry payloads
/// - still decodes Entry.hash to compute blockhash (last entry seen)
/// - iteration decodes transactions in file order, ignoring block->entry->tx links
///
/// Only correct if TX stream order equals the canonical order implied by entries.
pub struct CarBlockGroupUnchecked {
    // tx storage
    tx_buf: Vec<u8>,
    tx_ranges: Vec<(u32, u32)>, // in file order

    // block storage
    block_buf: Vec<u8>,
    block_range: (u32, u32),

    // scratch
    scratch: Vec<u8>,

    // blockhash tracking via last Entry.hash
    last_entry_hash: [u8; 32],
    has_last_entry_hash: bool,
    last_entry_cid_id: u64,
}

impl Default for CarBlockGroupUnchecked {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroupUnchecked {
    pub fn new() -> Self {
        Self {
            tx_buf: Vec::with_capacity(10 * 1024 * 1024),
            tx_ranges: Vec::with_capacity(8192),

            block_buf: Vec::with_capacity(2 * 1024 * 1024),
            block_range: (0, 0),

            scratch: Vec::with_capacity(1 * 1024 * 1024),

            last_entry_hash: [0u8; 32],
            has_last_entry_hash: false,
            last_entry_cid_id: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.tx_buf.clear();
        self.tx_ranges.clear();

        self.block_buf.clear();
        self.block_range = (0, 0);

        self.scratch.clear();

        self.last_entry_hash = [0u8; 32];
        self.has_last_entry_hash = false;
        self.last_entry_cid_id = 0;
    }

    /// Returns (0, tx_buf_bytes). Kept for compatibility with existing Stats.
    pub fn get_len(&self) -> (usize, usize) {
        (0, self.tx_buf.len())
    }

    #[inline(always)]
    pub fn block_payload(&self) -> &[u8] {
        let (s, e) = self.block_range;
        let s = s as usize;
        let e = e as usize;

        debug_assert!(s <= e);
        debug_assert!(e <= self.block_buf.len());

        unsafe { std::slice::from_raw_parts(self.block_buf.as_ptr().add(s), e - s) }
    }

    pub fn blockhash_checked(&self) -> Result<[u8; 32], GroupError> {
        if !self.has_last_entry_hash {
            return Err(GroupError::Other("missing last entry hash".to_string()));
        }

        #[cfg(debug_assertions)]
        {
            let block = match decode_node(self.block_payload()).map_err(GroupError::Node)? {
                Node::Block(bk) => bk,
                _ => return Err(GroupError::WrongRootKind),
            };

            let n = block.entries.len();
            if n == 0 {
                return Err(GroupError::Other("entries array is empty".to_string()));
            }

            let last_entry_cid = block
                .entries
                .decode_at(n - 1)
                .map_err(|e| GroupError::Other(format!("decode last entry cid: {e}")))?;

            let expected = gxhash::gxhash64(last_entry_cid.hash_bytes(), 0);
            debug_assert!(
                expected == self.last_entry_cid_id,
                "entry order assumption violated: last entry in block != last entry seen"
            );
        }

        Ok(self.last_entry_hash)
    }

    #[inline]
    pub fn read_entry_payload_into<R: Read>(
        &mut self,
        reader: &mut R,
        cid_bytes: &[u8; 36],
        entry_len: usize,
    ) -> CarReadResult<bool> {
        let payload_len = entry_len
            .checked_sub(cid_bytes.len())
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;

        self.scratch.clear();
        self.scratch.resize(payload_len, 0u8);

        if let Err(e) = reader.read_exact(&mut self.scratch) {
            self.scratch.clear();
            return Err(CarReadError::Io(e.to_string()));
        }

        // Block
        if is_block_node(&self.scratch) {
            let start = self.block_buf.len();
            let end = start + payload_len;

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData("block buffer exceeds u32::MAX".to_string()));
            }

            self.block_buf.extend_from_slice(&self.scratch);
            self.block_range = (start as u32, end as u32);
            return Ok(true);
        }

        // Transaction: store payload and record range in file order
        if is_transaction_node(&self.scratch) {
            let start = self.tx_buf.len();
            let end = start + payload_len;

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData("tx buffer exceeds u32::MAX".to_string()));
            }

            self.tx_buf.extend_from_slice(&self.scratch);
            self.tx_ranges.push((start as u32, end as u32));
            return Ok(false);
        }

        // Entry: decode only to extract entry.hash for blockhash tracking
        if is_entry_node(&self.scratch) {
            let id = cid_id(cid_bytes);

            let node =
                decode_node(&self.scratch).map_err(|e| CarReadError::InvalidData(e.to_string()))?;
            let Node::Entry(entry) = node else {
                return Ok(false);
            };

            if entry.hash.len() != 32 {
                return Err(CarReadError::InvalidData("entry.hash len != 32".to_string()));
            }

            self.last_entry_hash.copy_from_slice(entry.hash);
            self.has_last_entry_hash = true;
            self.last_entry_cid_id = id;

            return Ok(false);
        }

        Ok(false)
    }

    pub fn transactions<'a>(&'a mut self) -> Result<TxIterUnchecked<'a>, GroupError> {
        Ok(TxIterUnchecked {
            tx_buf_ptr: self.tx_buf.as_ptr(),
            tx_buf_len: self.tx_buf.len(),
            tx_ranges: &self.tx_ranges,
            pos: 0,

            reusable_tx: MaybeUninit::uninit(),
            reusable_meta: TransactionStatusMeta::default(),
            zstd: ZstdReusableDecoder::new(),
            has_tx: false,
            has_meta: false,
        })
    }
}

pub struct TxIterUnchecked<'a> {
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

impl<'a> Drop for TxIterUnchecked<'a> {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

impl<'a> TxIterUnchecked<'a> {
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

            if tx.data.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }
            if tx.metadata.next.is_some() {
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

