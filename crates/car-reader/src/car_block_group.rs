use std::mem::MaybeUninit;

use bytes::Bytes;
use rustc_hash::{FxBuildHasher, FxHashMap};
use wincode::Deserialize;

use crate::{
    confirmed_block::TransactionStatusMeta,
    error::GroupError,
    metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame},
    node::{CborArrayIter, CborCidRef, Node, NodeDecodeError, decode_node}, versioned_transaction::VersionedTransaction,
};

const DEFAULT_CAPACITY: usize = 8192;

pub struct CarBlockGroup {
    pub block_payload: Bytes,
    pub payloads: Vec<Bytes>,
    pub cid_map: FxHashMap<Bytes, usize>,
}

impl Default for CarBlockGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroup {
    pub fn new() -> Self {
        let cid_map = FxHashMap::with_capacity_and_hasher(DEFAULT_CAPACITY, FxBuildHasher);
        Self {
            block_payload: Bytes::new(),
            payloads: Vec::with_capacity(DEFAULT_CAPACITY),
            cid_map,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.block_payload = Bytes::new();
        self.payloads.clear();
        self.cid_map.clear();
    }

    #[inline]
    pub fn get(&self, cid_key: &[u8]) -> Option<&Bytes> {
        let idx = *self.cid_map.get(cid_key)?;
        self.payloads.get(idx)
    }

    #[inline]
    pub fn decode_by_hash<'a>(&'a self, cid_hash_bytes: &[u8]) -> Result<Node<'a>, GroupError> {
        let payload = self.get(cid_hash_bytes).ok_or(GroupError::MissingCid)?;
        decode_node(payload.as_ref()).map_err(GroupError::Node)
    }

    pub fn transactions<'a>(&'a self) -> Result<TxIter<'a>, GroupError> {
        let block = match decode_node(self.block_payload.as_ref()).map_err(GroupError::Node)? {
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
            reusable_meta: None,
            zstd: ZstdReusableDecoder::new(4096),
            has_tx: false,
        })
    }
}

pub struct TxIter<'a> {
    group: &'a CarBlockGroup,

    entry_iter: CborArrayIter<'a, CborCidRef<'a>>,
    tx_iter: Option<CborArrayIter<'a, CborCidRef<'a>>>,

    reusable_tx: MaybeUninit<VersionedTransaction>,
    reusable_meta: Option<TransactionStatusMeta>,
    zstd: ZstdReusableDecoder,
    has_tx: bool,
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
                panic!(
                    "unexpected tx dataframe continuation (tx.data.next != None) at slot={} index={:?}",
                    tx.slot, tx.index
                );
            }

            if tx.metadata.next.is_some() {
                panic!(
                    "unexpected tx dataframe continuation (tx.metadata.next != None) at slot={} index={:?}",
                    tx.slot, tx.index
                );
            }

            // Drop previous transaction if exists
            if self.has_tx {
                unsafe { self.reusable_tx.assume_init_drop() };
                self.has_tx = false;
            }

            // Decode metadata if present

            let has_metadata = !tx.metadata.data.is_empty();
            if has_metadata {
                let meta = self
                    .reusable_meta
                    .get_or_insert_with(TransactionStatusMeta::default);

                decode_transaction_status_meta_from_frame(
                    tx.slot,
                    tx.metadata.data,
                    meta,
                    &mut self.zstd,
                )
                .map_err(|_| GroupError::TxMetaDecode)?;
            } else {
                self.reusable_meta = None;
            }

            VersionedTransaction::deserialize_into(tx.data.data, &mut self.reusable_tx)
                .map_err(|_| GroupError::TxDecode)?;
            self.has_tx = true;

            return Ok(true);
        }
    }

    /// Returns a reference to the metadata of the current transaction.
    /// Returns None if no transaction is loaded or if the transaction has no metadata.
    /// This reference is valid until next() is called again.
    #[inline]
    pub fn current_metadata(&self) -> Option<&TransactionStatusMeta> {
        if self.has_tx {
            self.reusable_meta.as_ref()
        } else {
            None
        }
    }
}

impl<'a> Iterator for TxIter<'a> {
    // Reference is valid until next() is called again (reused buffer).
    type Item = Result<(&'a VersionedTransaction, Option<&'a TransactionStatusMeta>), GroupError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.decode_next_tx_in_place() {
            Ok(false) => None,
            Ok(true) => {
                let tx = unsafe { self.reusable_tx.assume_init_ref() };
                let tx_ptr: *const VersionedTransaction = tx;
                let meta_ptr: Option<*const TransactionStatusMeta> =
                    self.reusable_meta.as_ref().map(|m| m as *const _);
                Some(Ok(unsafe { (&*tx_ptr, meta_ptr.map(|p| &*p)) }))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
