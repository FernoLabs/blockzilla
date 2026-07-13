use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::Read,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use sha2::{Digest, Sha256};

use crate::{
    CarBlockReader,
    confirmed_block::TransactionStatusMeta,
    error::{CarReadError, CarReadResult},
    metadata_decoder::{
        MetadataDecodeError, RewardsDecodeError, ZstdReusableDecoder, decode_rewards_from_frame,
        decode_transaction_status_meta_from_frame,
    },
    node::{
        BlockNode, CborCidRef, DataFrame, EntryNode, EpochNode, Node, RewardsNode, Shredding,
        SlotMeta, SubsetNode, TransactionNode, decode_node,
    },
    versioned_transaction::VersionedTransaction,
};

const CAR_CID_PREFIX: [u8; 4] = [0x01, 0x71, 0x12, 0x20];
const MAX_UVARINT_LEN_64: usize = 10;
type BlockCborSections = (Vec<u8>, Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cid36([u8; 36]);

impl Cid36 {
    #[inline]
    pub fn from_car_bytes(bytes: [u8; 36]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub fn from_ref_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes = normalize_ref_bytes(bytes);
        let bytes = match bytes {
            bytes if bytes.len() == 36 => bytes,
            _ => return None,
        };

        let mut out = [0u8; 36];
        out.copy_from_slice(bytes);
        Some(Self(out))
    }

    #[inline]
    pub fn compute(payload: &[u8]) -> Self {
        let digest = Sha256::digest(payload);
        let mut out = [0u8; 36];
        out[..4].copy_from_slice(&CAR_CID_PREFIX);
        out[4..].copy_from_slice(&digest);
        Self(out)
    }

    #[inline]
    pub fn car_bytes(&self) -> &[u8; 36] {
        &self.0
    }

    #[inline]
    pub fn cbor_bytes(&self) -> [u8; 37] {
        let mut out = [0u8; 37];
        out[1..].copy_from_slice(&self.0);
        out
    }
}

impl fmt::Display for Cid36 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for Cid36 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Cid36 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::<u8>::deserialize(deserializer)?;
        if bytes.len() != 36 {
            return Err(serde::de::Error::invalid_length(bytes.len(), &"36 bytes"));
        }
        let mut out = [0u8; 36];
        out.copy_from_slice(&bytes);
        Ok(Self(out))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeLocation {
    pub entry_index: u64,
    pub car_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCidRef {
    pub cid: Option<Cid36>,
    bytes: RawCidRefBytes,
    pub tagged: bool,
}

/// Exact encoded bytes for a CID reference.
///
/// The overwhelmingly common CAR CID is kept inline. Unusual identity CIDs and malformed
/// references retain their exact bytes in one allocation so lossless reconstruction remains
/// possible.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RawCidRefBytes {
    Standard {
        cbor_bytes: [u8; 37],
        len: u8,
    },
    Owned {
        /// `split == false`: CBOR is the full slice and normalized bytes start at `boundary`.
        /// `split == true`: normalized bytes precede CBOR bytes at `boundary`.
        bytes: Box<[u8]>,
        boundary: usize,
        split: bool,
    },
}

impl RawCidRef {
    fn from_borrowed(value: CborCidRef<'_>) -> Result<Self, ReconstructError> {
        let normalized_bytes = normalize_ref_bytes(value.bytes);
        let cid = Cid36::from_ref_bytes(normalized_bytes);
        let bytes = if cid.is_some() {
            debug_assert!(matches!(value.bytes.len(), 36 | 37));
            let mut cbor_bytes = [0u8; 37];
            cbor_bytes[..value.bytes.len()].copy_from_slice(value.bytes);
            RawCidRefBytes::Standard {
                cbor_bytes,
                len: value.bytes.len() as u8,
            }
        } else {
            RawCidRefBytes::Owned {
                bytes: value.bytes.to_vec().into_boxed_slice(),
                boundary: usize::from(value.bytes.first() == Some(&0)),
                split: false,
            }
        };

        Ok(Self {
            cid,
            bytes,
            tagged: value.tagged,
        })
    }

    /// Construct the canonical tagged CBOR form of a normal CAR CID reference.
    pub fn from_car_cid(cid: Cid36) -> Self {
        let cbor_bytes = cid.cbor_bytes();
        Self {
            cid: Some(cid),
            bytes: RawCidRefBytes::Standard {
                cbor_bytes,
                len: cbor_bytes.len() as u8,
            },
            tagged: true,
        }
    }

    fn from_serialized_parts(
        cid: Option<Cid36>,
        normalized_bytes: Vec<u8>,
        cbor_bytes: Vec<u8>,
        tagged: bool,
    ) -> Self {
        let normalized_matches_cbor = normalize_ref_bytes(&cbor_bytes) == normalized_bytes;
        let is_inline_standard = cid.is_some()
            && normalized_matches_cbor
            && cid
                .as_ref()
                .is_some_and(|cid| cid.car_bytes().as_slice() == normalized_bytes)
            && matches!(cbor_bytes.len(), 36 | 37);

        let bytes = if is_inline_standard {
            let mut inline = [0u8; 37];
            inline[..cbor_bytes.len()].copy_from_slice(&cbor_bytes);
            RawCidRefBytes::Standard {
                cbor_bytes: inline,
                len: cbor_bytes.len() as u8,
            }
        } else if normalized_matches_cbor {
            let boundary = usize::from(cbor_bytes.first() == Some(&0));
            RawCidRefBytes::Owned {
                bytes: cbor_bytes.into_boxed_slice(),
                boundary,
                split: false,
            }
        } else {
            let boundary = normalized_bytes.len();
            let mut bytes = Vec::with_capacity(boundary.saturating_add(cbor_bytes.len()));
            bytes.extend_from_slice(&normalized_bytes);
            bytes.extend_from_slice(&cbor_bytes);
            RawCidRefBytes::Owned {
                bytes: bytes.into_boxed_slice(),
                boundary,
                split: true,
            }
        };

        Self { cid, bytes, tagged }
    }

    #[inline]
    pub fn normalized_bytes(&self) -> &[u8] {
        match &self.bytes {
            RawCidRefBytes::Standard { cbor_bytes, len } => {
                normalize_ref_bytes(&cbor_bytes[..usize::from(*len)])
            }
            RawCidRefBytes::Owned {
                bytes,
                boundary,
                split: false,
            } => &bytes[*boundary..],
            RawCidRefBytes::Owned {
                bytes,
                boundary,
                split: true,
            } => &bytes[..*boundary],
        }
    }

    #[inline]
    pub fn cbor_bytes(&self) -> &[u8] {
        match &self.bytes {
            RawCidRefBytes::Standard { cbor_bytes, len } => &cbor_bytes[..usize::from(*len)],
            RawCidRefBytes::Owned {
                bytes,
                split: false,
                ..
            } => bytes,
            RawCidRefBytes::Owned {
                bytes,
                boundary,
                split: true,
            } => &bytes[*boundary..],
        }
    }

    fn encode_into(&self, e: &mut minicbor::Encoder<Vec<u8>>) {
        if self.tagged {
            e.tag(minicbor::data::Tag::new(42))
                .expect("vec encoder is infallible");
        }
        e.bytes(self.cbor_bytes())
            .expect("vec encoder is infallible");
    }

    fn require_car_cid(&self) -> Result<Cid36, ReconstructError> {
        self.cid
            .ok_or_else(|| ReconstructError::UnsupportedCidRef(self.normalized_bytes().to_vec()))
    }

    pub fn inline_raw_bytes(&self) -> Option<&[u8]> {
        parse_inline_raw_identity_cid(self.normalized_bytes())
    }
}

impl Serialize for RawCidRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Preserve the field order and byte-sequence representation emitted by the former
        // derived implementation so existing postcard lossless archives stay readable.
        let mut state = serializer.serialize_struct("RawCidRef", 4)?;
        state.serialize_field("cid", &self.cid)?;
        state.serialize_field("normalized_bytes", self.normalized_bytes())?;
        state.serialize_field("cbor_bytes", self.cbor_bytes())?;
        state.serialize_field("tagged", &self.tagged)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RawCidRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SerializedRawCidRef {
            cid: Option<Cid36>,
            normalized_bytes: Vec<u8>,
            cbor_bytes: Vec<u8>,
            tagged: bool,
        }

        let value = SerializedRawCidRef::deserialize(deserializer)?;
        Ok(Self::from_serialized_parts(
            value.cid,
            value.normalized_bytes,
            value.cbor_bytes,
            value.tagged,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawDataFrame {
    pub hash: Option<u64>,
    #[serde(default)]
    pub hash_was_negative: bool,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
    pub next: Vec<RawCidRef>,
}

impl RawDataFrame {
    fn from_borrowed_with_data_buffer<F>(
        value: &DataFrame<'_>,
        take_data_buffer: &mut F,
    ) -> Result<Self, ReconstructError>
    where
        F: FnMut(usize) -> Vec<u8>,
    {
        let next = match value.next.as_ref() {
            Some(next) => next
                .iter()
                .map(|item| {
                    item.map_err(ReconstructError::from)
                        .and_then(RawCidRef::from_borrowed)
                })
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };

        let mut data = take_data_buffer(value.data.len());
        data.clear();
        data.reserve(value.data.len());
        data.extend_from_slice(value.data);

        Ok(Self {
            hash: value.hash,
            hash_was_negative: value.hash_was_negative,
            index: value.index,
            total: value.total,
            data,
            next,
        })
    }

    fn encode_into(&self, e: &mut minicbor::Encoder<Vec<u8>>) {
        let len = if self.next.is_empty() { 5 } else { 6 };
        e.array(len).expect("vec encoder is infallible");
        e.u64(6).expect("vec encoder is infallible");
        encode_option_hash_u64(e, self.hash, self.hash_was_negative);
        encode_option_u64(e, self.index);
        encode_option_u64(e, self.total);
        e.bytes(&self.data).expect("vec encoder is infallible");
        if !self.next.is_empty() {
            e.array(self.next.len() as u64)
                .expect("vec encoder is infallible");
            for next in &self.next {
                next.encode_into(e);
            }
        }
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        self.encode_into(&mut e);
        e.into_writer()
    }

    pub fn reassemble_bytes(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    ) -> Result<Vec<u8>, ReconstructError> {
        let mut out = Vec::with_capacity(self.data.len());
        let mut visited = HashSet::new();
        self.append_reassembled(&mut out, dataframes, &mut visited)?;
        Ok(out)
    }

    pub fn reassemble_bytes_into(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
        out: &mut Vec<u8>,
        visited: &mut HashSet<Cid36>,
    ) -> Result<(), ReconstructError> {
        out.clear();
        visited.clear();
        out.reserve(self.data.len());
        self.append_reassembled(out, dataframes, visited)
    }

    fn append_reassembled(
        &self,
        out: &mut Vec<u8>,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
        visited: &mut HashSet<Cid36>,
    ) -> Result<(), ReconstructError> {
        out.extend_from_slice(&self.data);

        for next in &self.next {
            if let Some(inline) = next.inline_raw_bytes() {
                out.extend_from_slice(inline);
                continue;
            }

            let cid = next.require_car_cid()?;
            if !visited.insert(cid) {
                return Err(ReconstructError::DataFrameCycle(cid));
            }
            let frame = dataframes
                .get(&cid)
                .ok_or(ReconstructError::MissingDataFrame(cid))?;
            frame.frame.append_reassembled(out, dataframes, visited)?;
            visited.remove(&cid);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandaloneDataFrame {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub frame: RawDataFrame,
}

impl StandaloneDataFrame {
    fn from_borrowed_with_data_buffer<F>(
        location: NodeLocation,
        cid: Cid36,
        frame: &DataFrame<'_>,
        take_data_buffer: &mut F,
    ) -> Result<Self, ReconstructError>
    where
        F: FnMut(usize) -> Vec<u8>,
    {
        Ok(Self {
            location,
            cid,
            frame: RawDataFrame::from_borrowed_with_data_buffer(frame, take_data_buffer)?,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        self.frame.encode_payload()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTransactionNode {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub slot: u64,
    pub index: Option<u64>,
    pub data: RawDataFrame,
    pub metadata: RawDataFrame,
}

impl RawTransactionNode {
    fn from_borrowed_with_data_buffers<F>(
        location: NodeLocation,
        cid: Cid36,
        tx: &TransactionNode<'_>,
        take_data_buffer: &mut F,
    ) -> Result<Self, ReconstructError>
    where
        F: FnMut(usize) -> Vec<u8>,
    {
        Ok(Self {
            location,
            cid,
            slot: tx.slot,
            index: tx.index,
            data: RawDataFrame::from_borrowed_with_data_buffer(&tx.data, take_data_buffer)?,
            metadata: RawDataFrame::from_borrowed_with_data_buffer(&tx.metadata, take_data_buffer)?,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(if self.index.is_some() { 5 } else { 4 })
            .expect("vec encoder is infallible");
        e.u64(0).expect("vec encoder is infallible");
        self.data.encode_into(&mut e);
        self.metadata.encode_into(&mut e);
        e.u64(self.slot).expect("vec encoder is infallible");
        if let Some(index) = self.index {
            e.u64(index).expect("vec encoder is infallible");
        }
        e.into_writer()
    }

    pub fn transaction_bytes(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    ) -> Result<Vec<u8>, ReconstructError> {
        self.data.reassemble_bytes(dataframes)
    }

    pub fn metadata_bytes(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    ) -> Result<Vec<u8>, ReconstructError> {
        self.metadata.reassemble_bytes(dataframes)
    }

    pub fn transaction_bytes_into(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
        out: &mut Vec<u8>,
        visited: &mut HashSet<Cid36>,
    ) -> Result<(), ReconstructError> {
        self.data.reassemble_bytes_into(dataframes, out, visited)
    }

    pub fn metadata_bytes_into(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
        out: &mut Vec<u8>,
        visited: &mut HashSet<Cid36>,
    ) -> Result<(), ReconstructError> {
        self.metadata
            .reassemble_bytes_into(dataframes, out, visited)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawEntryNode {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub transactions: Vec<RawCidRef>,
}

impl RawEntryNode {
    fn from_borrowed(
        location: NodeLocation,
        cid: Cid36,
        entry: &EntryNode<'_>,
    ) -> Result<Self, ReconstructError> {
        if entry.hash.len() != 32 {
            return Err(ReconstructError::InvalidEntryHashLen(entry.hash.len()));
        }

        let mut hash = [0u8; 32];
        hash.copy_from_slice(entry.hash);

        let transactions = entry
            .transactions
            .iter()
            .map(|item| {
                item.map_err(ReconstructError::from)
                    .and_then(RawCidRef::from_borrowed)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            location,
            cid,
            num_hashes: entry.num_hashes,
            hash,
            transactions,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(1).expect("vec encoder is infallible");
        e.u64(self.num_hashes).expect("vec encoder is infallible");
        e.bytes(&self.hash).expect("vec encoder is infallible");
        e.array(self.transactions.len() as u64)
            .expect("vec encoder is infallible");
        for tx in &self.transactions {
            tx.encode_into(&mut e);
        }
        e.into_writer()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawRewardsNode {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub slot: u64,
    pub data: RawDataFrame,
}

impl RawRewardsNode {
    fn from_borrowed_with_data_buffer<F>(
        location: NodeLocation,
        cid: Cid36,
        rewards: &RewardsNode<'_>,
        take_data_buffer: &mut F,
    ) -> Result<Self, ReconstructError>
    where
        F: FnMut(usize) -> Vec<u8>,
    {
        Ok(Self {
            location,
            cid,
            slot: rewards.slot,
            data: RawDataFrame::from_borrowed_with_data_buffer(&rewards.data, take_data_buffer)?,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(5).expect("vec encoder is infallible");
        e.u64(self.slot).expect("vec encoder is infallible");
        self.data.encode_into(&mut e);
        e.into_writer()
    }

    pub fn rewards_bytes(
        &self,
        dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    ) -> Result<Vec<u8>, ReconstructError> {
        self.data.reassemble_bytes(dataframes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBlockNode {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    pub shredding_cbor: Vec<u8>,
    pub entries: Vec<RawCidRef>,
    pub entries_cbor: Vec<u8>,
    pub meta: SlotMeta,
    pub meta_cbor: Vec<u8>,
    pub rewards: Option<RawCidRef>,
}

impl RawBlockNode {
    fn from_borrowed(
        location: NodeLocation,
        cid: Cid36,
        payload: &[u8],
        block: &BlockNode<'_>,
    ) -> Result<Self, ReconstructError> {
        let (shredding_cbor, entries_cbor, meta_cbor) = extract_block_cbor_sections(payload)?;
        let shredding = block
            .shredding
            .iter()
            .map(|item| item.map_err(ReconstructError::from))
            .collect::<Result<Vec<_>, _>>()?;
        let entries = block
            .entries
            .iter()
            .map(|item| {
                item.map_err(ReconstructError::from)
                    .and_then(RawCidRef::from_borrowed)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rewards = block.rewards.map(RawCidRef::from_borrowed).transpose()?;

        Ok(Self {
            location,
            cid,
            slot: block.slot,
            shredding,
            shredding_cbor,
            entries,
            entries_cbor,
            meta: block.meta.clone(),
            meta_cbor,
            rewards,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(if self.rewards.is_some() { 6 } else { 5 })
            .expect("vec encoder is infallible");
        e.u64(2).expect("vec encoder is infallible");
        e.u64(self.slot).expect("vec encoder is infallible");

        e.writer_mut().extend_from_slice(&self.shredding_cbor);
        e.writer_mut().extend_from_slice(&self.entries_cbor);

        e.writer_mut().extend_from_slice(&self.meta_cbor);

        if let Some(rewards) = &self.rewards {
            rewards.encode_into(&mut e);
        }

        e.into_writer()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSubsetNode {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub first: u64,
    pub last: u64,
    pub blocks: Vec<RawCidRef>,
}

impl RawSubsetNode {
    fn from_borrowed(
        location: NodeLocation,
        cid: Cid36,
        subset: &SubsetNode<'_>,
    ) -> Result<Self, ReconstructError> {
        let blocks = subset
            .blocks
            .iter()
            .map(|item| {
                item.map_err(ReconstructError::from)
                    .and_then(RawCidRef::from_borrowed)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            location,
            cid,
            first: subset.first,
            last: subset.last,
            blocks,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(3).expect("vec encoder is infallible");
        e.u64(self.first).expect("vec encoder is infallible");
        e.u64(self.last).expect("vec encoder is infallible");
        e.array(self.blocks.len() as u64)
            .expect("vec encoder is infallible");
        for block in &self.blocks {
            block.encode_into(&mut e);
        }
        e.into_writer()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawEpochNode {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub epoch: u64,
    pub subsets: Vec<RawCidRef>,
}

impl RawEpochNode {
    fn from_borrowed(
        location: NodeLocation,
        cid: Cid36,
        epoch: &EpochNode<'_>,
    ) -> Result<Self, ReconstructError> {
        let subsets = epoch
            .subsets
            .iter()
            .map(|item| {
                item.map_err(ReconstructError::from)
                    .and_then(RawCidRef::from_borrowed)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            location,
            cid,
            epoch: epoch.epoch,
            subsets,
        })
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(4).expect("vec encoder is infallible");
        e.u64(self.epoch).expect("vec encoder is infallible");
        e.array(self.subsets.len() as u64)
            .expect("vec encoder is infallible");
        for subset in &self.subsets {
            subset.encode_into(&mut e);
        }
        e.into_writer()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RawNode {
    Transaction(RawTransactionNode),
    Entry(RawEntryNode),
    Block(RawBlockNode),
    Subset(RawSubsetNode),
    Epoch(RawEpochNode),
    Rewards(RawRewardsNode),
    DataFrame(StandaloneDataFrame),
}

impl RawNode {
    pub fn location(&self) -> NodeLocation {
        match self {
            RawNode::Transaction(node) => node.location,
            RawNode::Entry(node) => node.location,
            RawNode::Block(node) => node.location,
            RawNode::Subset(node) => node.location,
            RawNode::Epoch(node) => node.location,
            RawNode::Rewards(node) => node.location,
            RawNode::DataFrame(node) => node.location,
        }
    }

    pub fn cid(&self) -> Cid36 {
        match self {
            RawNode::Transaction(node) => node.cid,
            RawNode::Entry(node) => node.cid,
            RawNode::Block(node) => node.cid,
            RawNode::Subset(node) => node.cid,
            RawNode::Epoch(node) => node.cid,
            RawNode::Rewards(node) => node.cid,
            RawNode::DataFrame(node) => node.cid,
        }
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        match self {
            RawNode::Transaction(node) => node.encode_payload(),
            RawNode::Entry(node) => node.encode_payload(),
            RawNode::Block(node) => node.encode_payload(),
            RawNode::Subset(node) => node.encode_payload(),
            RawNode::Epoch(node) => node.encode_payload(),
            RawNode::Rewards(node) => node.encode_payload(),
            RawNode::DataFrame(node) => node.encode_payload(),
        }
    }

    pub fn validate_cid(&self) -> Result<(), ReconstructError> {
        let kind = match self {
            RawNode::Transaction(_) => "transaction",
            RawNode::Entry(_) => "entry",
            RawNode::Block(_) => "block",
            RawNode::Subset(_) => "subset",
            RawNode::Epoch(_) => "epoch",
            RawNode::Rewards(_) => "rewards",
            RawNode::DataFrame(_) => "dataframe",
        };

        validate_payload_cid(kind, self.cid(), &self.encode_payload())
    }
}

#[derive(Debug, Default)]
pub struct LosslessCarBlock {
    pub block: Option<RawBlockNode>,
    pub entries: Vec<RawEntryNode>,
    pub transactions: Vec<RawTransactionNode>,
    pub rewards: Option<RawRewardsNode>,
    pub dataframes: HashMap<Cid36, StandaloneDataFrame>,

    tx_by_cid: HashMap<Cid36, RawTransactionNode>,
    entry_by_cid: HashMap<Cid36, RawEntryNode>,
    rewards_by_cid: HashMap<Cid36, RawRewardsNode>,
    scratch: Vec<u8>,
}

impl LosslessCarBlock {
    pub fn clear(&mut self) {
        self.block = None;
        self.entries.clear();
        self.transactions.clear();
        self.rewards = None;
        self.dataframes.clear();
        self.tx_by_cid.clear();
        self.entry_by_cid.clear();
        self.rewards_by_cid.clear();
        self.scratch.clear();
    }

    pub fn read_entry_payload_into<R: Read>(
        &mut self,
        reader: &mut R,
        payload_len: usize,
        location: NodeLocation,
        cid_bytes: [u8; 36],
    ) -> CarReadResult<bool> {
        if payload_len == 0 {
            return Err(CarReadError::UnexpectedEof(format!(
                "Empty payload len ({payload_len})"
            )));
        }

        self.scratch.clear();
        self.scratch.resize(payload_len, 0u8);
        reader.read_exact(&mut self.scratch)?;

        let cid = Cid36::from_car_bytes(cid_bytes);
        let node = decode_raw_node(location, cid, &self.scratch).map_err(|err| {
            CarReadError::InvalidData(format!(
                "entry {} at offset {}: {}",
                location.entry_index, location.car_offset, err
            ))
        })?;

        match node {
            RawNode::Transaction(tx) => {
                insert_unique(&mut self.tx_by_cid, cid, tx)
                    .map_err(|err| CarReadError::InvalidData(err.to_string()))?;
                Ok(false)
            }
            RawNode::Entry(entry) => {
                insert_unique(&mut self.entry_by_cid, cid, entry)
                    .map_err(|err| CarReadError::InvalidData(err.to_string()))?;
                Ok(false)
            }
            RawNode::Rewards(rewards) => {
                insert_unique(&mut self.rewards_by_cid, cid, rewards)
                    .map_err(|err| CarReadError::InvalidData(err.to_string()))?;
                Ok(false)
            }
            RawNode::DataFrame(frame) => {
                insert_unique(&mut self.dataframes, cid, frame)
                    .map_err(|err| CarReadError::InvalidData(err.to_string()))?;
                Ok(false)
            }
            RawNode::Block(block) => {
                self.finalize(block)
                    .map_err(|err| CarReadError::InvalidData(err.to_string()))?;
                Ok(true)
            }
            RawNode::Subset(_) | RawNode::Epoch(_) => Ok(false),
        }
    }

    fn finalize(&mut self, block: RawBlockNode) -> Result<(), ReconstructError> {
        self.transactions.clear();
        self.entries.clear();
        self.rewards = None;

        for entry_ref in &block.entries {
            let entry_cid = entry_ref.require_car_cid()?;
            let entry = self
                .entry_by_cid
                .get(&entry_cid)
                .cloned()
                .ok_or(ReconstructError::MissingEntry(entry_cid))?;

            for tx_ref in &entry.transactions {
                let tx_cid = tx_ref.require_car_cid()?;
                let tx = self
                    .tx_by_cid
                    .get(&tx_cid)
                    .cloned()
                    .ok_or(ReconstructError::MissingTransaction(tx_cid))?;
                self.transactions.push(tx);
            }

            self.entries.push(entry);
        }

        if let Some(rewards_ref) = &block.rewards {
            if let Some(rewards_cid) = rewards_ref.cid {
                self.rewards = Some(
                    self.rewards_by_cid
                        .get(&rewards_cid)
                        .cloned()
                        .ok_or(ReconstructError::MissingRewards(rewards_cid))?,
                );
            } else if rewards_ref.inline_raw_bytes().is_none() {
                return Err(ReconstructError::UnsupportedCidRef(
                    rewards_ref.normalized_bytes().to_vec(),
                ));
            }
        }

        self.block = Some(block);
        Ok(())
    }

    pub fn validate_cids(&self) -> Result<(), ReconstructError> {
        let Some(block) = self.block.as_ref() else {
            return Ok(());
        };

        for tx in &self.transactions {
            validate_payload_cid("transaction", tx.cid, &tx.encode_payload())?;
        }
        for entry in &self.entries {
            validate_payload_cid("entry", entry.cid, &entry.encode_payload())?;
        }
        if let Some(rewards) = &self.rewards {
            validate_payload_cid("rewards", rewards.cid, &rewards.encode_payload())?;
        }
        for dataframe in self.dataframes.values() {
            validate_payload_cid("dataframe", dataframe.cid, &dataframe.encode_payload())?;
        }
        validate_payload_cid("block", block.cid, &block.encode_payload())?;

        Ok(())
    }

    pub fn validate_decoding(&self) -> Result<(), ReconstructError> {
        let mut zstd = ZstdReusableDecoder::new();

        for tx in &self.transactions {
            let tx_bytes = tx.transaction_bytes(&self.dataframes)?;
            let _ = wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes)
                .map_err(|err| ReconstructError::TransactionDecode(err.to_string()))?;

            let metadata = tx.metadata_bytes(&self.dataframes)?;
            let mut out = TransactionStatusMeta::default();
            decode_transaction_status_meta_from_frame(tx.slot, &metadata, &mut out, &mut zstd)?;
        }

        if let Some(rewards) = &self.rewards {
            let bytes = rewards.rewards_bytes(&self.dataframes)?;
            let mut out = crate::confirmed_block::Rewards::default();
            decode_rewards_from_frame(bytes.as_slice(), &mut out, &mut zstd)
                .map_err(ReconstructError::RewardsDecode)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationStats {
    pub car_entries: u64,
    pub bytes_read: u64,
    pub blocks: u64,
    pub entries: u64,
    pub transactions: u64,
    pub rewards: u64,
    pub dataframes: u64,
    pub subsets: u64,
    pub epochs: u64,
    pub tx_data_continuation_refs: u64,
    pub tx_metadata_continuation_refs: u64,
    pub rewards_continuation_refs: u64,
    pub dataframe_continuation_refs: u64,
}

#[derive(Debug)]
pub enum ValidationError {
    Read(CarReadError),
    Reconstruct(ReconstructError),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::Read(err) => write!(f, "{err}"),
            ValidationError::Reconstruct(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<CarReadError> for ValidationError {
    fn from(value: CarReadError) -> Self {
        Self::Read(value)
    }
}

impl From<ReconstructError> for ValidationError {
    fn from(value: ReconstructError) -> Self {
        Self::Reconstruct(value)
    }
}

pub fn decode_raw_node(
    location: NodeLocation,
    cid: Cid36,
    payload: &[u8],
) -> Result<RawNode, ReconstructError> {
    decode_raw_node_with_data_buffers(location, cid, payload, &mut |len| Vec::with_capacity(len))
}

/// Decodes a lossless raw node while obtaining each [`RawDataFrame`] payload
/// buffer from the caller.
///
/// The callback is invoked once for rewards and standalone dataframe nodes,
/// twice for transaction nodes (transaction data followed by metadata), and
/// not at all for other node kinds. Returned buffer contents are discarded;
/// their allocations are retained when they are large enough. The existing
/// [`decode_raw_node`] API remains the allocation-owning convenience path.
pub fn decode_raw_node_with_data_buffers<F>(
    location: NodeLocation,
    cid: Cid36,
    payload: &[u8],
    take_data_buffer: &mut F,
) -> Result<RawNode, ReconstructError>
where
    F: FnMut(usize) -> Vec<u8>,
{
    let node = decode_node(payload).map_err(|err| ReconstructError::NodeDecode(err.to_string()))?;

    Ok(match node {
        Node::Transaction(tx) => {
            RawNode::Transaction(RawTransactionNode::from_borrowed_with_data_buffers(
                location,
                cid,
                &tx,
                take_data_buffer,
            )?)
        }
        Node::Entry(entry) => RawNode::Entry(RawEntryNode::from_borrowed(location, cid, &entry)?),
        Node::Block(block) => {
            RawNode::Block(RawBlockNode::from_borrowed(location, cid, payload, &block)?)
        }
        Node::Subset(subset) => {
            RawNode::Subset(RawSubsetNode::from_borrowed(location, cid, &subset)?)
        }
        Node::Epoch(epoch) => RawNode::Epoch(RawEpochNode::from_borrowed(location, cid, &epoch)?),
        Node::Rewards(rewards) => RawNode::Rewards(RawRewardsNode::from_borrowed_with_data_buffer(
            location,
            cid,
            &rewards,
            take_data_buffer,
        )?),
        Node::DataFrame(frame) => {
            RawNode::DataFrame(StandaloneDataFrame::from_borrowed_with_data_buffer(
                location,
                cid,
                &frame,
                take_data_buffer,
            )?)
        }
    })
}

pub fn validate_car_stream<R: Read>(
    inner: R,
    io_buf_bytes: usize,
) -> Result<ValidationStats, ValidationError> {
    let mut reader = CarBlockReader::with_capacity(inner, io_buf_bytes);
    reader.skip_header()?;
    validate_reader_after_header(&mut reader)
}

pub fn validate_reader_after_header<R: Read>(
    reader: &mut CarBlockReader<R>,
) -> Result<ValidationStats, ValidationError> {
    let mut stats = ValidationStats::default();
    let mut pending = LosslessCarBlock::default();
    let mut scratch = Vec::new();
    let mut blocks_by_cid = HashMap::new();
    let mut subsets_by_cid = HashMap::new();
    let mut epochs_by_cid = HashMap::new();

    while let Some(node) = reader.read_lossless_node_with_scratch(&mut scratch)? {
        stats.car_entries += 1;

        match node {
            RawNode::Transaction(tx) => {
                stats.transactions += 1;
                stats.tx_data_continuation_refs += tx.data.next.len() as u64;
                stats.tx_metadata_continuation_refs += tx.metadata.next.len() as u64;
                insert_unique(&mut pending.tx_by_cid, tx.cid, tx)?;
            }
            RawNode::Entry(entry) => {
                stats.entries += 1;
                insert_unique(&mut pending.entry_by_cid, entry.cid, entry)?;
            }
            RawNode::Rewards(rewards) => {
                stats.rewards += 1;
                stats.rewards_continuation_refs += rewards.data.next.len() as u64;
                insert_unique(&mut pending.rewards_by_cid, rewards.cid, rewards)?;
            }
            RawNode::DataFrame(frame) => {
                stats.dataframes += 1;
                stats.dataframe_continuation_refs += frame.frame.next.len() as u64;
                insert_unique(&mut pending.dataframes, frame.cid, frame)?;
            }
            RawNode::Block(block) => {
                stats.blocks += 1;
                let block_cid = block.cid;
                pending.finalize(block)?;
                pending.validate_cids()?;
                pending.validate_decoding()?;
                let block = pending
                    .block
                    .as_ref()
                    .cloned()
                    .ok_or(ReconstructError::MissingBlock(block_cid))?;
                insert_unique(&mut blocks_by_cid, block.cid, block)?;
                pending.clear();
            }
            RawNode::Subset(subset) => {
                stats.subsets += 1;
                validate_payload_cid("subset", subset.cid, &subset.encode_payload())?;
                insert_unique(&mut subsets_by_cid, subset.cid, subset)?;
            }
            RawNode::Epoch(epoch) => {
                stats.epochs += 1;
                validate_payload_cid("epoch", epoch.cid, &epoch.encode_payload())?;
                insert_unique(&mut epochs_by_cid, epoch.cid, epoch)?;
            }
        }
    }

    if !pending.tx_by_cid.is_empty()
        || !pending.entry_by_cid.is_empty()
        || !pending.rewards_by_cid.is_empty()
        || !pending.dataframes.is_empty()
    {
        return Err(ReconstructError::UnterminatedBlockGroup {
            transactions: pending.tx_by_cid.len(),
            entries: pending.entry_by_cid.len(),
            rewards: pending.rewards_by_cid.len(),
            dataframes: pending.dataframes.len(),
        }
        .into());
    }

    for subset in subsets_by_cid.values() {
        for block in &subset.blocks {
            let block_cid = block.require_car_cid()?;
            if !blocks_by_cid.contains_key(&block_cid) {
                return Err(ReconstructError::MissingBlock(block_cid).into());
            }
        }
    }

    for epoch in epochs_by_cid.values() {
        for subset in &epoch.subsets {
            let subset_cid = subset.require_car_cid()?;
            if !subsets_by_cid.contains_key(&subset_cid) {
                return Err(ReconstructError::MissingSubset(subset_cid).into());
            }
        }
    }

    stats.bytes_read = reader.offset;
    Ok(stats)
}

#[derive(Debug)]
pub enum ReconstructError {
    NodeDecode(String),
    Cbor(minicbor::decode::Error),
    Metadata(MetadataDecodeError),
    RewardsDecode(RewardsDecodeError),
    TransactionDecode(String),
    DuplicateCid(Cid36),
    InvalidCidRef(Vec<u8>),
    UnsupportedCidRef(Vec<u8>),
    InvalidEntryHashLen(usize),
    MissingDataFrame(Cid36),
    MissingBlock(Cid36),
    MissingEntry(Cid36),
    MissingTransaction(Cid36),
    MissingRewards(Cid36),
    MissingSubset(Cid36),
    UnterminatedBlockGroup {
        transactions: usize,
        entries: usize,
        rewards: usize,
        dataframes: usize,
    },
    DataFrameCycle(Cid36),
    CidMismatch {
        kind: &'static str,
        expected: Cid36,
        actual: Cid36,
    },
}

impl fmt::Display for ReconstructError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReconstructError::NodeDecode(err) => write!(f, "{err}"),
            ReconstructError::Cbor(err) => write!(f, "{err}"),
            ReconstructError::Metadata(err) => write!(f, "{err}"),
            ReconstructError::RewardsDecode(err) => write!(f, "{err}"),
            ReconstructError::TransactionDecode(err) => write!(f, "{err}"),
            ReconstructError::DuplicateCid(cid) => write!(f, "duplicate cid {cid}"),
            ReconstructError::InvalidCidRef(bytes) => {
                write!(
                    f,
                    "invalid cid ref len {} ({})",
                    bytes.len(),
                    hex_bytes(bytes)
                )
            }
            ReconstructError::UnsupportedCidRef(bytes) => {
                write!(f, "unsupported cid ref {}", hex_bytes(bytes))
            }
            ReconstructError::InvalidEntryHashLen(len) => {
                write!(f, "invalid entry hash len {len}")
            }
            ReconstructError::MissingDataFrame(cid) => write!(f, "missing dataframe {cid}"),
            ReconstructError::MissingBlock(cid) => write!(f, "missing block {cid}"),
            ReconstructError::MissingEntry(cid) => write!(f, "missing entry {cid}"),
            ReconstructError::MissingTransaction(cid) => write!(f, "missing transaction {cid}"),
            ReconstructError::MissingRewards(cid) => write!(f, "missing rewards {cid}"),
            ReconstructError::MissingSubset(cid) => write!(f, "missing subset {cid}"),
            ReconstructError::UnterminatedBlockGroup {
                transactions,
                entries,
                rewards,
                dataframes,
            } => write!(
                f,
                "unterminated block group: txs={transactions} entries={entries} rewards={rewards} dataframes={dataframes}"
            ),
            ReconstructError::DataFrameCycle(cid) => write!(f, "dataframe cycle at {cid}"),
            ReconstructError::CidMismatch {
                kind,
                expected,
                actual,
            } => write!(
                f,
                "{kind} cid mismatch: expected {expected}, recomputed {actual}"
            ),
        }
    }
}

impl std::error::Error for ReconstructError {}

impl From<minicbor::decode::Error> for ReconstructError {
    fn from(value: minicbor::decode::Error) -> Self {
        Self::Cbor(value)
    }
}

impl From<MetadataDecodeError> for ReconstructError {
    fn from(value: MetadataDecodeError) -> Self {
        Self::Metadata(value)
    }
}

fn insert_unique<T>(
    map: &mut HashMap<Cid36, T>,
    cid: Cid36,
    value: T,
) -> Result<(), ReconstructError> {
    if map.insert(cid, value).is_some() {
        return Err(ReconstructError::DuplicateCid(cid));
    }
    Ok(())
}

fn validate_payload_cid(
    kind: &'static str,
    expected: Cid36,
    payload: &[u8],
) -> Result<(), ReconstructError> {
    let actual = Cid36::compute(payload);
    if actual != expected {
        return Err(ReconstructError::CidMismatch {
            kind,
            expected,
            actual,
        });
    }
    Ok(())
}

fn encode_option_u64(e: &mut minicbor::Encoder<Vec<u8>>, value: Option<u64>) {
    if let Some(value) = value {
        e.u64(value).expect("vec encoder is infallible");
    } else {
        e.null().expect("vec encoder is infallible");
    }
}

fn encode_option_hash_u64(
    e: &mut minicbor::Encoder<Vec<u8>>,
    value: Option<u64>,
    was_negative: bool,
) {
    if let Some(value) = value {
        if was_negative {
            e.i64(value as i64).expect("vec encoder is infallible");
        } else {
            e.u64(value).expect("vec encoder is infallible");
        }
    } else {
        e.null().expect("vec encoder is infallible");
    }
}

fn normalize_ref_bytes(bytes: &[u8]) -> &[u8] {
    match bytes {
        [0, rest @ ..] => rest,
        bytes => bytes,
    }
}

fn extract_block_cbor_sections(payload: &[u8]) -> Result<BlockCborSections, ReconstructError> {
    let mut d = minicbor::Decoder::new(payload);
    let _ = d.array()?;
    let _ = d.u64()?;
    let _ = d.u64()?;

    let shredding_start = d.position();
    d.skip()?;
    let shredding_end = d.position();

    let entries_start = d.position();
    d.skip()?;
    let entries_end = d.position();

    let meta_start = d.position();
    d.skip()?;
    let meta_end = d.position();

    Ok((
        payload[shredding_start..shredding_end].to_vec(),
        payload[entries_start..entries_end].to_vec(),
        payload[meta_start..meta_end].to_vec(),
    ))
}

fn parse_inline_raw_identity_cid(bytes: &[u8]) -> Option<&[u8]> {
    let bytes = normalize_ref_bytes(bytes);

    let (version, version_len) = parse_uvarint(bytes)?;
    if version != 1 {
        return None;
    }
    let (codec, codec_len) = parse_uvarint(&bytes[version_len..])?;
    if codec != 0x55 {
        return None;
    }
    let (multihash_code, mh_code_len) = parse_uvarint(&bytes[version_len + codec_len..])?;
    if multihash_code != 0 {
        return None;
    }
    let digest_start = version_len + codec_len + mh_code_len;
    let (digest_len, digest_len_len) = parse_uvarint(&bytes[digest_start..])?;
    let digest_start = digest_start + digest_len_len;
    let digest_end = digest_start.checked_add(digest_len as usize)?;
    let digest = bytes.get(digest_start..digest_end)?;
    if digest.len() != digest_len as usize {
        return None;
    }
    Some(digest)
}

fn parse_uvarint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    let mut shift = 0u32;

    for (i, byte) in bytes.iter().copied().enumerate().take(MAX_UVARINT_LEN_64) {
        if byte < 0x80 {
            if i == MAX_UVARINT_LEN_64 - 1 && byte > 1 {
                return None;
            }
            x |= (byte as u64) << shift;
            return Some((x, i + 1));
        }

        x |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
        if shift > 63 {
            return None;
        }
    }

    None
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        CAR_CID_PREFIX, Cid36, LosslessCarBlock, NodeLocation, RawCidRef, RawCidRefBytes,
        RawDataFrame, RawNode, ReconstructError, StandaloneDataFrame, decode_raw_node,
        decode_raw_node_with_data_buffers, validate_car_stream,
    };
    use crate::{CarBlockReader, confirmed_block, node::CborCidRef};
    use prost::Message;
    use serde::Serialize;
    use std::{
        collections::{HashMap, VecDeque},
        io::Cursor,
    };

    #[test]
    fn cid_prefix_matches_old_faithful() {
        assert_eq!(CAR_CID_PREFIX, [0x01, 0x71, 0x12, 0x20]);
    }

    #[test]
    fn normal_tagged_leading_zero_cid_ref_is_inline_and_lossless() {
        let cid = Cid36::compute(b"normal tagged CID");
        let cbor_bytes = cid.cbor_bytes();
        let raw = RawCidRef::from_borrowed(CborCidRef {
            bytes: &cbor_bytes,
            tagged: true,
        })
        .expect("decode normal CID ref");

        assert!(matches!(raw.bytes, RawCidRefBytes::Standard { .. }));
        assert_eq!(raw.cid, Some(cid));
        assert_eq!(raw.normalized_bytes(), cid.car_bytes());
        assert_eq!(raw.cbor_bytes(), cbor_bytes);
        assert!(raw.tagged);
        assert_eq!(
            encode_raw_cid_ref(&raw),
            encode_cbor_cid_ref(&cbor_bytes, true)
        );
    }

    #[test]
    fn normal_untagged_cid_ref_without_leading_zero_is_inline_and_lossless() {
        let cid = Cid36::compute(b"normal untagged CID");
        let car_bytes = *cid.car_bytes();
        let raw = RawCidRef::from_borrowed(CborCidRef {
            bytes: &car_bytes,
            tagged: false,
        })
        .expect("decode untagged CID ref");

        assert!(matches!(raw.bytes, RawCidRefBytes::Standard { .. }));
        assert_eq!(raw.cid, Some(cid));
        assert_eq!(raw.normalized_bytes(), car_bytes);
        assert_eq!(raw.cbor_bytes(), car_bytes);
        assert!(!raw.tagged);
        assert_eq!(
            encode_raw_cid_ref(&raw),
            encode_cbor_cid_ref(&car_bytes, false)
        );
    }

    #[test]
    fn inline_identity_cid_uses_owned_fallback_and_preserves_bytes() {
        let digest = [9u8, 8, 7, 6];
        // CIDv1 + raw codec + identity multihash + digest length + digest. The initial zero is
        // the conventional CBOR CID byte-string prefix and is not part of the normalized CID.
        let cbor_bytes = [0, 1, 0x55, 0, digest.len() as u8, 9, 8, 7, 6];
        let raw = RawCidRef::from_borrowed(CborCidRef {
            bytes: &cbor_bytes,
            tagged: true,
        })
        .expect("decode identity CID ref");

        assert!(matches!(raw.bytes, RawCidRefBytes::Owned { .. }));
        assert_eq!(raw.cid, None);
        assert_eq!(raw.normalized_bytes(), &cbor_bytes[1..]);
        assert_eq!(raw.cbor_bytes(), cbor_bytes);
        assert_eq!(raw.inline_raw_bytes(), Some(digest.as_slice()));
        assert_eq!(
            encode_raw_cid_ref(&raw),
            encode_cbor_cid_ref(&cbor_bytes, true)
        );
    }

    #[test]
    fn malformed_cid_ref_uses_owned_fallback_without_changing_semantics() {
        let cbor_bytes = [0, 0xff, 0x01, 0x02];
        let raw = RawCidRef::from_borrowed(CborCidRef {
            bytes: &cbor_bytes,
            tagged: false,
        })
        .expect("retain malformed CID ref");

        assert!(matches!(raw.bytes, RawCidRefBytes::Owned { .. }));
        assert_eq!(raw.cid, None);
        assert_eq!(raw.normalized_bytes(), &cbor_bytes[1..]);
        assert_eq!(raw.cbor_bytes(), cbor_bytes);
        assert_eq!(raw.inline_raw_bytes(), None);
        assert!(matches!(
            raw.require_car_cid(),
            Err(ReconstructError::UnsupportedCidRef(bytes)) if bytes == cbor_bytes[1..]
        ));
        assert_eq!(
            encode_raw_cid_ref(&raw),
            encode_cbor_cid_ref(&cbor_bytes, false)
        );
    }

    #[test]
    fn cid_ref_postcard_encoding_remains_compatible_with_legacy_vec_fields() {
        #[derive(Serialize)]
        struct LegacyRawCidRef {
            cid: Option<Cid36>,
            normalized_bytes: Vec<u8>,
            cbor_bytes: Vec<u8>,
            tagged: bool,
        }

        let cid = Cid36::compute(b"postcard compatibility");
        let raw = RawCidRef::from_car_cid(cid);
        let legacy = LegacyRawCidRef {
            cid: raw.cid,
            normalized_bytes: raw.normalized_bytes().to_vec(),
            cbor_bytes: raw.cbor_bytes().to_vec(),
            tagged: raw.tagged,
        };

        let current_bytes = postcard::to_allocvec(&raw).expect("serialize current CID ref");
        let legacy_bytes = postcard::to_allocvec(&legacy).expect("serialize legacy CID ref");
        assert_eq!(current_bytes, legacy_bytes);

        let decoded: RawCidRef =
            postcard::from_bytes(&legacy_bytes).expect("decode legacy CID ref");
        assert_eq!(decoded, raw);
        assert!(matches!(decoded.bytes, RawCidRefBytes::Standard { .. }));
    }

    fn encode_raw_cid_ref(value: &RawCidRef) -> Vec<u8> {
        let mut encoder = minicbor::Encoder::new(Vec::new());
        value.encode_into(&mut encoder);
        encoder.into_writer()
    }

    fn encode_cbor_cid_ref(bytes: &[u8], tagged: bool) -> Vec<u8> {
        let mut encoder = minicbor::Encoder::new(Vec::new());
        if tagged {
            encoder
                .tag(minicbor::data::Tag::new(42))
                .expect("vec encoder is infallible");
        }
        encoder.bytes(bytes).expect("vec encoder is infallible");
        encoder.into_writer()
    }

    #[test]
    fn reads_lossless_block_and_recomputes_cids() {
        let car = build_synthetic_car();
        let mut reader = CarBlockReader::with_capacity(Cursor::new(car), 1024);
        reader.skip_header().expect("skip header");

        let mut block = LosslessCarBlock::default();
        let done = reader
            .read_until_block_lossless(&mut block)
            .expect("read lossless block");

        assert!(done);
        assert_eq!(block.entries.len(), 1);
        assert_eq!(block.transactions.len(), 1);
        assert!(block.rewards.is_some());
        block.validate_cids().expect("validate cids");
        block.validate_decoding().expect("validate decoding");
    }

    #[test]
    fn validates_full_synthetic_car_stream() {
        let car = build_synthetic_car();
        let stats = validate_car_stream(Cursor::new(car), 1024).expect("validate full stream");

        assert_eq!(stats.car_entries, 6);
        assert_eq!(stats.blocks, 1);
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.transactions, 1);
        assert_eq!(stats.rewards, 1);
        assert_eq!(stats.subsets, 1);
        assert_eq!(stats.epochs, 1);
    }

    #[test]
    fn raw_dataframe_preserves_signed_hash_encoding() {
        let signed_hash = -5_787_489_622_768_765_176i64;
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("array");
        e.u64(6).expect("kind");
        e.i64(signed_hash).expect("signed hash");
        e.null().expect("index");
        e.null().expect("total");
        e.bytes(&[1, 2, 3]).expect("data");
        let payload = e.into_writer();
        let cid = Cid36::compute(&payload);

        let RawNode::DataFrame(frame) = decode_raw_node(
            NodeLocation {
                entry_index: 0,
                car_offset: 0,
            },
            cid,
            &payload,
        )
        .expect("decode raw dataframe") else {
            panic!("expected dataframe");
        };

        assert_eq!(frame.frame.hash, Some(signed_hash as u64));
        assert!(frame.frame.hash_was_negative);
        let encoded = frame.encode_payload();
        assert_eq!(encoded, payload);
        assert_eq!(Cid36::compute(&encoded), cid);
    }

    #[test]
    fn pooled_raw_transaction_decode_reuses_both_payload_buffers() {
        let tx_data = [1u8, 2, 3, 4, 5, 6];
        let metadata = [7u8, 8, 9, 10];
        let data_frame = encode_dataframe(&tx_data);
        let metadata_frame = encode_dataframe(&metadata);
        let mut encoder = minicbor::Encoder::new(Vec::new());
        encoder.array(5).unwrap();
        encoder.u64(0).unwrap();
        encoder.writer_mut().extend_from_slice(&data_frame);
        encoder.writer_mut().extend_from_slice(&metadata_frame);
        encoder.u64(42).unwrap();
        encoder.u64(3).unwrap();
        let payload = encoder.into_writer();
        let cid = Cid36::compute(&payload);

        let mut tx_buffer = Vec::with_capacity(128);
        tx_buffer.extend_from_slice(b"discard tx contents");
        let tx_ptr = tx_buffer.as_ptr();
        let mut metadata_buffer = Vec::with_capacity(64);
        metadata_buffer.extend_from_slice(b"discard metadata contents");
        let metadata_ptr = metadata_buffer.as_ptr();
        let mut supplied = VecDeque::from([tx_buffer, metadata_buffer]);
        let mut requested = Vec::new();
        let pooled = decode_raw_node_with_data_buffers(
            NodeLocation {
                entry_index: 1,
                car_offset: 2,
            },
            cid,
            &payload,
            &mut |required| {
                requested.push(required);
                supplied.pop_front().expect("supplied dataframe buffer")
            },
        )
        .unwrap();
        let ordinary = decode_raw_node(
            NodeLocation {
                entry_index: 1,
                car_offset: 2,
            },
            cid,
            &payload,
        )
        .unwrap();

        assert_eq!(pooled, ordinary);
        assert_eq!(requested, vec![tx_data.len(), metadata.len()]);
        assert!(supplied.is_empty());
        let RawNode::Transaction(transaction) = pooled else {
            panic!("expected transaction")
        };
        assert_eq!(transaction.data.data, tx_data);
        assert_eq!(transaction.metadata.data, metadata);
        assert_eq!(transaction.data.data.as_ptr(), tx_ptr);
        assert_eq!(transaction.metadata.data.as_ptr(), metadata_ptr);
        assert_eq!(transaction.encode_payload(), payload);
    }

    #[test]
    fn pooled_rewards_decode_reuses_payload_buffer() {
        let rewards_data = [41u8, 42, 43, 44, 45];
        let payload = encode_rewards_node(99, &rewards_data);
        let cid = Cid36::compute(&payload);
        let mut supplied = Vec::with_capacity(128);
        supplied.extend_from_slice(b"discard rewards contents");
        let supplied_ptr = supplied.as_ptr();
        let pooled = decode_raw_node_with_data_buffers(
            NodeLocation {
                entry_index: 8,
                car_offset: 9,
            },
            cid,
            &payload,
            &mut |required| {
                assert_eq!(required, rewards_data.len());
                std::mem::take(&mut supplied)
            },
        )
        .unwrap();
        let ordinary = decode_raw_node(
            NodeLocation {
                entry_index: 8,
                car_offset: 9,
            },
            cid,
            &payload,
        )
        .unwrap();
        assert_eq!(pooled, ordinary);
        let RawNode::Rewards(rewards) = pooled else {
            panic!("expected rewards")
        };
        assert_eq!(rewards.data.data, rewards_data);
        assert_eq!(rewards.data.data.as_ptr(), supplied_ptr);
        assert_eq!(rewards.encode_payload(), payload);
    }

    #[test]
    fn pooled_dataframe_preserves_negative_hash_and_odd_continuations() {
        let continuation_cid = Cid36::compute(b"pooled continuation");
        let normal_ref = RawCidRef::from_car_cid(continuation_cid);
        let identity_digest = [9u8, 8, 7, 6];
        let identity_bytes = [0, 1, 0x55, 0, identity_digest.len() as u8, 9, 8, 7, 6];
        let identity_ref = RawCidRef::from_borrowed(CborCidRef {
            bytes: &identity_bytes,
            tagged: true,
        })
        .unwrap();
        let malformed_bytes = [0, 0xff, 0x01, 0x02];
        let malformed_ref = RawCidRef::from_borrowed(CborCidRef {
            bytes: &malformed_bytes,
            tagged: false,
        })
        .unwrap();
        let signed_hash = -5_787_489_622_768_765_176i64;
        let mut encoder = minicbor::Encoder::new(Vec::new());
        encoder.array(6).unwrap();
        encoder.u64(6).unwrap();
        encoder.i64(signed_hash).unwrap();
        encoder.null().unwrap();
        encoder.null().unwrap();
        encoder.bytes(&[1, 2]).unwrap();
        encoder.array(3).unwrap();
        normal_ref.encode_into(&mut encoder);
        identity_ref.encode_into(&mut encoder);
        malformed_ref.encode_into(&mut encoder);
        let payload = encoder.into_writer();
        let cid = Cid36::compute(&payload);

        let mut supplied = Vec::with_capacity(256);
        supplied.extend_from_slice(b"contents must be cleared");
        let supplied_ptr = supplied.as_ptr();
        let pooled = decode_raw_node_with_data_buffers(
            NodeLocation {
                entry_index: 4,
                car_offset: 5,
            },
            cid,
            &payload,
            &mut |required| {
                assert_eq!(required, 2);
                std::mem::take(&mut supplied)
            },
        )
        .unwrap();
        let ordinary = decode_raw_node(
            NodeLocation {
                entry_index: 4,
                car_offset: 5,
            },
            cid,
            &payload,
        )
        .unwrap();
        assert_eq!(pooled, ordinary);

        let RawNode::DataFrame(frame) = pooled else {
            panic!("expected dataframe")
        };
        assert_eq!(frame.frame.data.as_ptr(), supplied_ptr);
        assert_eq!(frame.frame.hash, Some(signed_hash as u64));
        assert!(frame.frame.hash_was_negative);
        assert_eq!(frame.encode_payload(), payload);
        assert_eq!(Cid36::compute(&frame.encode_payload()), cid);

        let continuation = StandaloneDataFrame {
            location: NodeLocation {
                entry_index: 6,
                car_offset: 7,
            },
            cid: continuation_cid,
            frame: RawDataFrame {
                hash: None,
                hash_was_negative: false,
                index: None,
                total: None,
                data: vec![3, 4],
                next: Vec::new(),
            },
        };
        let dataframes = HashMap::from([(continuation_cid, continuation)]);
        let mut supported = frame.frame.clone();
        supported.next.pop();
        assert_eq!(
            supported.reassemble_bytes(&dataframes).unwrap(),
            [1, 2, 3, 4, 9, 8, 7, 6]
        );
        assert!(matches!(
            frame.frame.reassemble_bytes(&dataframes),
            Err(ReconstructError::UnsupportedCidRef(bytes))
                if bytes == malformed_bytes[1..]
        ));

        let mut cyclic_continuation = dataframes[&continuation_cid].clone();
        cyclic_continuation
            .frame
            .next
            .push(RawCidRef::from_car_cid(cid));
        let cyclic = HashMap::from([
            (continuation_cid, cyclic_continuation),
            (cid, frame.clone()),
        ]);
        assert!(matches!(
            frame.frame.reassemble_bytes(&cyclic),
            Err(ReconstructError::DataFrameCycle(cycle_cid))
                if cycle_cid == continuation_cid
        ));
    }

    fn build_synthetic_car() -> Vec<u8> {
        let tx_payload = encode_transaction_node(42, Some(0), &minimal_legacy_transaction());
        let tx_cid = Cid36::compute(&tx_payload);

        let rewards_bytes = confirmed_block::Rewards {
            rewards: Vec::new(),
            num_partitions: None,
        }
        .encode_to_vec();
        let rewards_payload = encode_rewards_node(42, &rewards_bytes);
        let rewards_cid = Cid36::compute(&rewards_payload);

        let entry_ref = cid_ref(tx_cid);
        let entry_payload = encode_entry_node(1, [0x44; 32], &[entry_ref]);
        let entry_cid = Cid36::compute(&entry_payload);

        let block_payload = encode_block_node(42, entry_cid, rewards_cid);
        let block_cid = Cid36::compute(&block_payload);

        let subset_payload = encode_subset_node(42, 42, &[cid_ref(block_cid)]);
        let subset_cid = Cid36::compute(&subset_payload);

        let epoch_payload = encode_epoch_node(0, &[cid_ref(subset_cid)]);

        let mut out = Vec::new();
        out.push(0);
        push_car_entry(&mut out, tx_cid, &tx_payload);
        push_car_entry(&mut out, entry_cid, &entry_payload);
        push_car_entry(&mut out, rewards_cid, &rewards_payload);
        push_car_entry(&mut out, block_cid, &block_payload);
        push_car_entry(&mut out, subset_cid, &subset_payload);
        push_car_entry(&mut out, Cid36::compute(&epoch_payload), &epoch_payload);
        out
    }

    fn push_car_entry(out: &mut Vec<u8>, cid: Cid36, payload: &[u8]) {
        push_uvarint(out, (cid.car_bytes().len() + payload.len()) as u64);
        out.extend_from_slice(cid.car_bytes());
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

    fn cid_ref(cid: Cid36) -> RawCidRef {
        RawCidRef::from_car_cid(cid)
    }

    fn encode_dataframe(data: &[u8]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("vec encoder is infallible");
        e.u64(6).expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.bytes(data).expect("vec encoder is infallible");
        e.into_writer()
    }

    fn encode_transaction_node(slot: u64, index: Option<u64>, tx_bytes: &[u8]) -> Vec<u8> {
        let data_frame = encode_dataframe(tx_bytes);
        let meta_frame = encode_dataframe(&[]);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("vec encoder is infallible");
        e.u64(0).expect("vec encoder is infallible");
        e.writer_mut().extend_from_slice(&data_frame);
        e.writer_mut().extend_from_slice(&meta_frame);
        e.u64(slot).expect("vec encoder is infallible");
        if let Some(index) = index {
            e.u64(index).expect("vec encoder is infallible");
        } else {
            e.null().expect("vec encoder is infallible");
        }
        e.into_writer()
    }

    fn encode_rewards_node(slot: u64, rewards_bytes: &[u8]) -> Vec<u8> {
        let frame = encode_dataframe(rewards_bytes);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(5).expect("vec encoder is infallible");
        e.u64(slot).expect("vec encoder is infallible");
        e.writer_mut().extend_from_slice(&frame);
        e.into_writer()
    }

    fn encode_entry_node(num_hashes: u64, hash: [u8; 32], txs: &[RawCidRef]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(1).expect("vec encoder is infallible");
        e.u64(num_hashes).expect("vec encoder is infallible");
        e.bytes(&hash).expect("vec encoder is infallible");
        e.array(txs.len() as u64)
            .expect("vec encoder is infallible");
        for tx in txs {
            tx.encode_into(&mut e);
        }
        e.into_writer()
    }

    fn encode_block_node(slot: u64, entry_cid: Cid36, rewards_cid: Cid36) -> Vec<u8> {
        let entry_ref = cid_ref(entry_cid);
        let rewards_ref = cid_ref(rewards_cid);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(6).expect("vec encoder is infallible");
        e.u64(2).expect("vec encoder is infallible");
        e.u64(slot).expect("vec encoder is infallible");
        e.array(1).expect("vec encoder is infallible");
        e.array(2).expect("vec encoder is infallible");
        e.i64(1).expect("vec encoder is infallible");
        e.i64(2).expect("vec encoder is infallible");
        e.array(1).expect("vec encoder is infallible");
        entry_ref.encode_into(&mut e);
        e.array(1).expect("vec encoder is infallible");
        e.u64(slot - 1).expect("vec encoder is infallible");
        rewards_ref.encode_into(&mut e);
        e.into_writer()
    }

    fn encode_subset_node(first: u64, last: u64, blocks: &[RawCidRef]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(3).expect("vec encoder is infallible");
        e.u64(first).expect("vec encoder is infallible");
        e.u64(last).expect("vec encoder is infallible");
        e.array(blocks.len() as u64)
            .expect("vec encoder is infallible");
        for block in blocks {
            block.encode_into(&mut e);
        }
        e.into_writer()
    }

    fn encode_epoch_node(epoch: u64, subsets: &[RawCidRef]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(4).expect("vec encoder is infallible");
        e.u64(epoch).expect("vec encoder is infallible");
        e.array(subsets.len() as u64)
            .expect("vec encoder is infallible");
        for subset in subsets {
            subset.encode_into(&mut e);
        }
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
}
