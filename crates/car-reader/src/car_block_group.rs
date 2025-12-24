use std::mem::MaybeUninit;

use ahash::AHashMap;
use bytes::Bytes;
use solana_transaction::versioned::VersionedTransaction;
use wincode::Deserialize;

use crate::{
    confirmed_block::TransactionStatusMeta,
    error::GroupError,
    metadata_decoder::{decode_transaction_status_meta_from_frame, ZstdReusableDecoder},
    node::{decode_node, CborArrayIter, CborCidRef, Node, NodeDecodeError},
    versioned_transaction::VersionedTransactionSchema,
};

pub struct CarBlockGroup {
    pub block_payload: Bytes,
    pub payloads: Vec<Bytes>,
    pub cid_map: AHashMap<Bytes, usize>,
}

impl Default for CarBlockGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl CarBlockGroup {
    pub fn new() -> Self {
        Self {
            block_payload: Bytes::new(),
            payloads: Vec::with_capacity(8192),
            cid_map: AHashMap::with_capacity(8192),
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
    fn decode_by_hash<'a>(&'a self, cid_hash_bytes: &[u8]) -> Result<Node<'a>, GroupError> {
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
            reusable_meta: TransactionStatusMeta::default(),
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
    reusable_meta: TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
    has_tx: bool,
}

impl<'a> Drop for TxIter<'a> {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { core::ptr::drop_in_place(self.reusable_tx.as_mut_ptr()) };
            self.has_tx = false;
        }
    }
}

impl<'a> TxIter<'a> {
    #[inline]
    fn load_next_entry(&mut self) -> Result<bool, GroupError> {
        loop {
            let entry_cid = match self.entry_iter.next_item() {
                None => return Ok(false),
                Some(r) => r.map_err(|e| GroupError::Node(NodeDecodeError::from(e)))?,
            };

            let node = self.group.decode_by_hash(entry_cid.hash_bytes())?;
            let Node::Entry(entry) = node else {
                continue;
            };

            self.tx_iter = Some(
                entry
                    .transactions
                    .iter_stateful()
                    .map_err(|e| GroupError::Node(NodeDecodeError::from(e)))?,
            );

            return Ok(true);
        }
    }

    #[inline]
    fn decode_next_tx_in_place(&mut self) -> Result<bool, GroupError> {
        loop {
            if self.tx_iter.is_none() && !self.load_next_entry()? {
                return Ok(false);
            }

            let tx_cid = match self.tx_iter.as_mut().unwrap().next_item() {
                None => {
                    self.tx_iter = None;
                    continue;
                }
                Some(r) => r.map_err(|e| GroupError::Node(NodeDecodeError::from(e)))?,
            };

            let node = self.group.decode_by_hash(tx_cid.hash_bytes())?;
            let Node::Transaction(tx) = node else {
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

            if self.has_tx {
                unsafe { core::ptr::drop_in_place(self.reusable_tx.as_mut_ptr()) };
                self.has_tx = false;
            }

            decode_transaction_status_meta_from_frame(
                tx.slot,
                tx.metadata.data,
                &mut self.reusable_meta,
                &mut self.zstd,
            )
            .map_err(|_e| GroupError::TxMetaDecode)?;
            VersionedTransactionSchema::deserialize_into(tx.data.data, &mut self.reusable_tx)
                .map_err(|_e| GroupError::TxDecode)?;

            self.has_tx = true;
            return Ok(true);
        }
    }
}

impl<'a> Iterator for TxIter<'a> {
    // Reference is valid until next() is called again (reused buffer).
    type Item = Result<&'a VersionedTransaction, GroupError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.decode_next_tx_in_place() {
            Ok(false) => None,
            Ok(true) => {
                let tx = unsafe { self.reusable_tx.assume_init_ref() };
                let ptr: *const VersionedTransaction = tx;
                Some(Ok(unsafe { &*ptr }))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
