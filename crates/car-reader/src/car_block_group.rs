use std::io::Read;
use std::mem::MaybeUninit;
use wincode::Deserialize;

use crate::confirmed_block::TransactionStatusMeta;
use crate::error::{CarReadError, CarReadResult, GroupError};
use crate::metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame};
use crate::node::{Node, decode_block_metadata, decode_entry_hash, decode_node, peek_node_type};
use crate::versioned_transaction::VersionedTransaction;

/// Simple, fast CarBlockGroup that stores:
/// - Transaction payloads in file order (no CID resolution)
/// - Block metadata fields directly in struct
/// - No Entry tracking
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

    // scratch buffer for reading
    scratch: Vec<u8>,
}

impl Default for CarBlockGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroup {
    pub fn new() -> Self {
        Self {
            tx_buf: Vec::with_capacity(12 << 20), // 12 MB
            tx_ranges: Vec::with_capacity(12_000),
            scratch: Vec::with_capacity(3 << 20), // 3 MB

            slot: None,
            parent_slot: None,
            block_time: None,
            block_height: None,

            blockhash: [0; 32],
            has_blockhash: false,
        }
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

        self.scratch.clear();
    }

    /// Returns (tx_count, tx_buf_bytes)
    pub fn get_len(&self) -> (usize, usize) {
        (self.tx_ranges.len(), self.tx_buf.len())
    }

    /// Read one CAR entry payload into the group.
    /// Returns Ok(true) when Block node is reached (group complete).
    #[inline]
    pub fn read_entry_payload_into<R: Read>(
        &mut self,
        reader: &mut R,
        payload_len: usize,
    ) -> CarReadResult<bool> {
        if payload_len == 0 {
            return Err(CarReadError::UnexpectedEof(format!(
                "Empty payload len ({payload_len})"
            )));
        }

        self.scratch.clear();
        self.scratch.resize(payload_len, 0u8);

        if let Err(e) = reader.read_exact(&mut self.scratch) {
            self.scratch.clear();
            return Err(CarReadError::Io(e.to_string()));
        }

        let node_type = peek_node_type(&self.scratch)
            .map_err(|err| CarReadError::InvalidData(format!("Can't read node type ({err})")))?;

        //println!("{node_type} {payload_len}");

        // Transaction: store in file order
        if node_type == 0 {
            let start = self.tx_buf.len();
            let end = start + payload_len;

            if end > u32::MAX as usize {
                return Err(CarReadError::InvalidData(
                    "tx buffer exceeds u32::MAX".to_string(),
                ));
            }

            self.tx_buf.extend_from_slice(&self.scratch);
            self.tx_ranges.push((start as u32, end as u32));
            return Ok(false);
        }

        // Entry: extract hash for blockhash
        if node_type == 1 {
            let hash = decode_entry_hash(&self.scratch)
                .map_err(|e| CarReadError::InvalidData(e.to_string()))?;

            self.blockhash.copy_from_slice(hash);
            self.has_blockhash = true;
            return Ok(false);
        }

        // Block: extract metadata and signal completion
        if node_type == 2 {
            let (slot, meta) = decode_block_metadata(&self.scratch)
                .map_err(|e| CarReadError::InvalidData(e.to_string()))?;

            self.slot = Some(slot);
            self.parent_slot = meta.parent_slot;
            self.block_time = meta.blocktime;
            self.block_height = meta.block_height;

            return Ok(true);
        }

        Ok(false)
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
