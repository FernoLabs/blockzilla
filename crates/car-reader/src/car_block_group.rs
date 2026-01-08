use gxhash::GxHasher;
use gxhash::HashMap;
use gxhash::HashMapExt;
use std::hash::Hasher;
use std::io::Read;
use std::mem::MaybeUninit;

use crate::confirmed_block::TransactionStatusMeta;
use crate::error::{CarReadError, CarReadResult, GroupError};
use crate::metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame};
use crate::node::{CborArrayIter, CborCidRef, Node, NodeDecodeError, decode_node, is_block_node};
use crate::versioned_transaction::VersionedTransaction;

use wincode::Deserialize;

pub struct CarBlockGroup {
    /// Concatenated payload bytes for the current group.
    pub buffer: Vec<u8>,

    /// FxHash(CID bytes) -> (payload_start, payload_end) offsets in `buffer`.
    cid_map: HashMap<u64, (u32, u32)>,

    /// (payload_start, payload_end) for the block node payload, inside `buffer`.
    block_range: (u32, u32),
}

impl Default for CarBlockGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroup {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(5 * 1024 * 1024),
            cid_map: HashMap::with_capacity(8096),
            block_range: (0, 0),
        }
    }

    #[inline]
    pub fn get_len(&self) -> (usize, usize) {
        (self.cid_map.len(), self.buffer.len())
    }

    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.cid_map.clear();
        self.block_range = (0, 0);
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn hash_cid(cid_bytes: &[u8]) -> u64 {
        let mut h = GxHasher::default();
        h.write(cid_bytes);
        h.finish()
    }

    /// Lookup payload by CID bytes (hash computed internally).
    #[inline(always)]
    pub fn get_entry(&self, cid_bytes: &[u8]) -> Option<&[u8]> {
        let key = Self::hash_cid(cid_bytes);
        let (s, e) = *self.cid_map.get(&key)?;

        let s = s as usize;
        let e = e as usize;

        // These are invariants enforced by read_entry_payload_into.
        debug_assert!(s <= e);
        debug_assert!(e <= self.buffer.len());

        // SAFETY: this is sound as long as self.buffer is in sync with cid_map
        unsafe {
            // Equivalent to &self.buffer[s..e] but without bounds checks.
            Some(std::slice::from_raw_parts(
                self.buffer.as_ptr().add(s),
                e - s,
            ))
        }
    }

    /// Returns the current block payload slice.
    #[inline(always)]
    pub fn block_payload(&self) -> &[u8] {
        let (s, e) = self.block_range;
        let s = s as usize;
        let e = e as usize;

        debug_assert!(s <= e);
        debug_assert!(e <= self.buffer.len());

        // SAFETY: this is sound as long as self.buffer is in sync with cid_map
        unsafe {
            // Equivalent to &self.buffer[s..e] but without bounds checks.
            std::slice::from_raw_parts(self.buffer.as_ptr().add(s), e - s)
        }
    }

    #[inline]
    pub fn decode_by_hash<'a>(&'a self, cid_hash_bytes: &[u8]) -> Result<Node<'a>, GroupError> {
        let payload = self
            .get_entry(cid_hash_bytes)
            .ok_or(GroupError::MissingCid)?;
        decode_node(payload).map_err(GroupError::Node)
    }

    pub fn transactions<'a>(&'a self) -> Result<TxIter<'a>, GroupError> {
        let block = match decode_node(self.block_payload()).map_err(GroupError::Node)? {
            Node::Block(b) => b,
            _ => return Err(GroupError::WrongRootKind),
        };

        let entry_iter = block
            .entries
            .iter_stateful()
            .map_err(|e| GroupError::Node(NodeDecodeError::from(e)))?;

        Ok(TxIter {
            group: self,
            entry_iter,
            tx_iter: None,
            reusable_tx: MaybeUninit::uninit(),
            reusable_meta: TransactionStatusMeta::default(),
            zstd: ZstdReusableDecoder::new(),
            has_tx: false,
            has_meta: false,
        })
    }

    /// Reads the payload bytes of one CAR entry into `buffer`, hashes CID bytes,
    /// inserts cid_map entry, and if payload is a block node sets `block_range`.
    ///
    /// `entry_len` is the total section size (CID bytes + payload bytes).
    ///
    /// Returns:
    /// - Ok(true)  => this entry was the block node (group complete)
    /// - Ok(false) => continue reading
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

        let start = self.buffer.len();
        let end = start + payload_len;

        if end > u32::MAX as usize {
            return Err(CarReadError::InvalidData(
                "group payload buffer exceeds u32::MAX".to_string(),
            ));
        }

        // Grow and read payload bytes.
        self.buffer.resize(end, 0);
        reader
            .read_exact(&mut self.buffer[start..end])
            .map_err(|e| CarReadError::Io(e.to_string()))?;

        // Hash CID and store mapping.
        let key = Self::hash_cid(cid_bytes);
        self.cid_map.insert(key, (start as u32, end as u32));

        // If this payload is the block node, record it.
        if is_block_node(&self.buffer[start..end]) {
            self.block_range = (start as u32, end as u32);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub struct TxIter<'a> {
    group: &'a CarBlockGroup,

    entry_iter: CborArrayIter<'a, CborCidRef<'a>>,
    tx_iter: Option<CborArrayIter<'a, CborCidRef<'a>>>,

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
    #[inline]
    fn decode_error(e: impl Into<NodeDecodeError>) -> GroupError {
        GroupError::Node(e.into())
    }

    #[inline]
    fn load_next_entry(&mut self) -> Result<bool, GroupError> {
        while let Some(entry_cid) = self.entry_iter.next_item() {
            let entry_cid = entry_cid.map_err(Self::decode_error)?;

            if let Node::Entry(entry) = self.group.decode_by_hash(entry_cid.hash_bytes())? {
                self.tx_iter = Some(
                    entry
                        .transactions
                        .iter_stateful()
                        .map_err(Self::decode_error)?,
                );
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[inline]
    fn decode_next_tx_in_place(&mut self) -> Result<bool, GroupError> {
        loop {
            // Get or load tx_iter
            let tx_iter = match &mut self.tx_iter {
                Some(iter) => iter,
                None => {
                    if !self.load_next_entry()? {
                        return Ok(false);
                    }
                    self.tx_iter.as_mut().unwrap()
                }
            };

            let tx_cid = match tx_iter.next_item() {
                None => {
                    self.tx_iter = None;
                    continue;
                }
                Some(r) => r.map_err(Self::decode_error)?,
            };

            let Node::Transaction(tx) = self.group.decode_by_hash(tx_cid.hash_bytes())? else {
                continue;
            };

            if tx.data.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            if tx.metadata.next.is_some() {
                return Err(GroupError::DataFrameHasNext);
            }

            // Drop previous transaction if exists
            if self.has_tx {
                unsafe { self.reusable_tx.assume_init_drop() };
                self.has_tx = false;
            }

            VersionedTransaction::deserialize_into(tx.data.data, &mut self.reusable_tx)
                .map_err(|_| GroupError::TxDecode)?;

            // Decode metadata if present
            let has_metadata = !tx.metadata.data.is_empty();
            if has_metadata {
                decode_transaction_status_meta_from_frame(
                    tx.slot,
                    tx.metadata.data,
                    &mut self.reusable_meta,
                    &mut self.zstd,
                )
                .inspect_err(|err| {
                    println!("tx {:?} failed : {err}", unsafe {
                        &*self.reusable_tx.as_ptr()
                    })
                })
                .map_err(|_| GroupError::TxMetaDecode)?;
            }

            self.has_tx = true;
            self.has_meta = has_metadata;

            return Ok(true);
        }
    }

    /// Returns a reference to the metadata of the current transaction.
    /// Valid until next_tx() is called again.
    #[inline]
    pub fn current_metadata(&self) -> &TransactionStatusMeta {
        &self.reusable_meta
    }

    #[inline]
    pub fn next_tx(
        &mut self,
    ) -> Result<Option<(&VersionedTransaction<'a>, Option<&TransactionStatusMeta>)>, GroupError>
    {
        if !self.decode_next_tx_in_place()? {
            return Ok(None);
        }

        let tx = unsafe { self.reusable_tx.assume_init_ref() };
        Ok(Some((tx, self.has_meta.then_some(&self.reusable_meta))))
    }
}
