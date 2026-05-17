use core::marker::PhantomData;
use minicbor::data::Type;
use minicbor::decode::Error as CborError;
use minicbor::{Decode, Decoder, Encode};
use serde::{Deserialize, Serialize};

pub type Result<T> = core::result::Result<T, NodeDecodeError>;

#[derive(Debug)]
pub enum NodeDecodeError {
    Cbor(CborError),
    UnknownKind(u64),
}

impl core::fmt::Display for NodeDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            NodeDecodeError::Cbor(e) => write!(f, "cbor decode error: {e}"),
            NodeDecodeError::UnknownKind(k) => write!(f, "unknown kind id {k}"),
        }
    }
}

impl std::error::Error for NodeDecodeError {}

impl From<CborError> for NodeDecodeError {
    #[inline]
    fn from(e: CborError) -> Self {
        NodeDecodeError::Cbor(e)
    }
}

/// Borrowed view over an encoded CBOR array, allowing cheap `len()` + iterator decoding.
#[derive(Debug, Clone)]
pub struct CborArrayView<'b, T> {
    pub slice: &'b [u8],
    pub(crate) _t: PhantomData<T>,
}

impl<'b, C, T> Decode<'b, C> for CborArrayView<'b, T> {
    #[inline]
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> core::result::Result<Self, CborError> {
        let start = d.position();
        d.skip()?;
        let end = d.position();
        let input = d.input();

        Ok(Self {
            slice: &input[start..end],
            _t: PhantomData,
        })
    }
}

impl<'b, T> CborArrayView<'b, T>
where
    T: Decode<'b, ()>,
{
    #[inline]
    pub fn len(&self) -> usize {
        let mut d = Decoder::new(self.slice);
        // `array()` returns Option<u64> for indefinite arrays.
        d.array().ok().flatten().unwrap_or(0) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = core::result::Result<T, CborError>> + 'b {
        let mut d = Decoder::new(self.slice);
        let n = d.array().ok().flatten().unwrap_or(0);
        (0..n).map(move |_| d.decode_with(&mut ()))
    }

    #[inline]
    pub fn decode_at(&self, idx: usize) -> core::result::Result<T, minicbor::decode::Error> {
        let mut d = minicbor::Decoder::new(self.slice);
        let n = d.array().ok().flatten().unwrap_or(0) as usize;
        if idx >= n {
            return Err(minicbor::decode::Error::message("index out of bounds"));
        }
        for _ in 0..idx {
            d.skip()?;
        }
        d.decode_with(&mut ())
    }
}

#[derive(Debug)]
pub struct DataFrame<'a> {
    pub kind: u64,
    pub hash: Option<u64>,
    pub hash_was_negative: bool,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: &'a [u8],
    pub next: Option<CborArrayView<'a, CborCidRef<'a>>>,
}

impl<'b, C> Decode<'b, C> for DataFrame<'b> {
    #[inline]
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> core::result::Result<Self, CborError> {
        let len = d
            .array()?
            .ok_or_else(|| CborError::message("indefinite dataframe array not supported"))?;
        if len < 5 {
            return Err(CborError::message("dataframe array too short"));
        }

        let kind = d.u64()?;
        let (hash, hash_was_negative) = decode_optional_hash_u64(d)?;
        let index = decode_optional_u64(d)?;
        let total = decode_optional_u64(d)?;
        let data = d.bytes()?;
        let next = if len > 5 { d.decode()? } else { None };
        for _ in 6..len {
            d.skip()?;
        }

        Ok(Self {
            kind,
            hash,
            hash_was_negative,
            index,
            total,
            data,
            next,
        })
    }
}

#[inline]
fn decode_optional_u64(d: &mut Decoder<'_>) -> core::result::Result<Option<u64>, CborError> {
    if d.datatype()? == Type::Null {
        d.null()?;
        return Ok(None);
    }
    Ok(Some(d.u64()?))
}

#[inline]
fn decode_optional_hash_u64(
    d: &mut Decoder<'_>,
) -> core::result::Result<(Option<u64>, bool), CborError> {
    match d.datatype()? {
        Type::Null => {
            d.null()?;
            Ok((None, false))
        }
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => Ok((Some(d.u64()?), false)),
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => Ok((Some(d.i64()? as u64), true)),
        other => Err(CborError::type_mismatch(other)),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedDataFrame {
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
}

impl<'a> From<&DataFrame<'a>> for OwnedDataFrame {
    #[inline]
    fn from(value: &DataFrame<'a>) -> Self {
        Self {
            hash: value.hash,
            index: value.index,
            total: value.total,
            data: value.data.to_vec(),
        }
    }
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cbor(array)]
pub struct SlotMeta {
    #[n(0)]
    pub parent_slot: Option<u64>,
    #[n(1)]
    pub blocktime: Option<i64>,
    #[n(2)]
    pub block_height: Option<u64>,
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cbor(array)]
pub struct Shredding {
    #[n(0)]
    pub entry_end_idx: i64,
    #[n(1)]
    pub shred_end_idx: i64,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct TransactionNode<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    #[cbor(borrow = "'a + 'bytes")]
    pub data: DataFrame<'a>,
    #[n(2)]
    pub metadata: DataFrame<'a>,
    #[n(3)]
    pub slot: u64,
    #[n(4)]
    pub index: Option<u64>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct EntryNode<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub num_hashes: u64,
    #[n(2)]
    #[cbor(decode_with = "minicbor::bytes::decode")]
    pub hash: &'a [u8],
    #[n(3)]
    #[cbor(borrow = "'a + 'bytes")]
    pub transactions: CborArrayView<'a, CborCidRef<'a>>,
}

#[derive(Debug, Decode, Clone)]
#[cbor(array)]
pub struct BlockNode<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub slot: u64,
    #[n(2)]
    #[cbor(borrow = "'a + 'bytes")]
    pub shredding: CborArrayView<'a, Shredding>,
    #[n(3)]
    #[cbor(borrow = "'a + 'bytes")]
    pub entries: CborArrayView<'a, CborCidRef<'a>>,
    #[n(4)]
    pub meta: SlotMeta,
    #[n(5)]
    pub rewards: Option<CborCidRef<'a>>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct SubsetNode<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub first: u64,
    #[n(2)]
    pub last: u64,
    #[n(3)]
    #[cbor(borrow = "'a + 'bytes")]
    pub blocks: CborArrayView<'a, CborCidRef<'a>>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct EpochNode<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub epoch: u64,
    #[n(2)]
    #[cbor(borrow = "'a + 'bytes")]
    pub subsets: CborArrayView<'a, CborCidRef<'a>>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct RewardsNode<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub slot: u64,
    #[n(2)]
    #[cbor(borrow = "'a + 'bytes")]
    pub data: DataFrame<'a>,
}

#[derive(Debug)]
pub enum Node<'a> {
    Transaction(TransactionNode<'a>),
    Entry(EntryNode<'a>),
    Block(BlockNode<'a>),
    Subset(SubsetNode<'a>),
    Epoch(EpochNode<'a>),
    Rewards(RewardsNode<'a>),
    DataFrame(DataFrame<'a>),
}

#[inline]
pub fn decode_node(data: &[u8]) -> Result<Node<'_>> {
    let kind = peek_node_type(data)?;
    let mut d = Decoder::new(data);

    Ok(match kind {
        0 => Node::Transaction(d.decode()?),
        1 => Node::Entry(d.decode()?),
        2 => Node::Block(d.decode()?),
        3 => Node::Subset(d.decode()?),
        4 => Node::Epoch(d.decode()?),
        5 => Node::Rewards(d.decode()?),
        6 => Node::DataFrame(d.decode()?),
        _ => return Err(NodeDecodeError::UnknownKind(kind)),
    })
}

#[inline]
pub fn peek_node_type(data: &[u8]) -> Result<u64> {
    let mut peek = Decoder::new(data);
    let _ = peek.array()?;
    Ok(peek.u64()?)
}

#[derive(Debug, Clone, Copy)]
pub struct CborCidRef<'a> {
    pub bytes: &'a [u8],
    pub tagged: bool,
}

impl<'a> CborCidRef<'a> {
    #[inline]
    pub fn hash_bytes(&self) -> &'a [u8] {
        &self.bytes[1..]
    }

    #[inline]
    pub fn normalized_bytes(&self) -> &'a [u8] {
        normalize_cid_ref_bytes(self.bytes)
    }

    #[inline]
    pub fn car_cid_bytes(&self) -> Option<&'a [u8]> {
        let bytes = self.normalized_bytes();
        (bytes.len() == 36).then_some(bytes)
    }

    #[inline]
    pub fn inline_raw_bytes(&self) -> Option<&'a [u8]> {
        parse_inline_raw_identity_cid(self.normalized_bytes())
    }
}

impl<'b, C> Decode<'b, C> for CborCidRef<'b> {
    #[inline]
    fn decode(d: &mut Decoder<'b>, _: &mut C) -> core::result::Result<Self, CborError> {
        let tagged = d.datatype()? == Type::Tag;
        if tagged {
            let _ = d.tag()?;
        }
        let bytes = d.bytes()?;
        if bytes.len() <= 1 {
            return Err(CborError::message("invalid CID bytes"));
        }
        Ok(Self { bytes, tagged })
    }
}

#[inline]
fn normalize_cid_ref_bytes(bytes: &[u8]) -> &[u8] {
    match bytes {
        [0, rest @ ..] => rest,
        bytes => bytes,
    }
}

fn parse_inline_raw_identity_cid(bytes: &[u8]) -> Option<&[u8]> {
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
    let digest_len_start = version_len + codec_len + mh_code_len;
    let (digest_len, digest_len_len) = parse_uvarint(&bytes[digest_len_start..])?;
    let digest_start = digest_len_start + digest_len_len;
    let digest_end = digest_start.checked_add(digest_len as usize)?;
    let digest = bytes.get(digest_start..digest_end)?;
    (digest.len() == digest_len as usize).then_some(digest)
}

fn parse_uvarint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;

    for (index, byte) in bytes.iter().take(10).copied().enumerate() {
        if byte < 0x80 {
            if index == 9 && byte > 1 {
                return None;
            }
            value |= (byte as u64) << shift;
            return Some((value, index + 1));
        }

        value |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }

    None
}

pub struct CborArrayIter<'b, T> {
    d: Decoder<'b>,
    rem: u64,
    _t: PhantomData<T>,
}

impl<'b, T> CborArrayIter<'b, T>
where
    T: Decode<'b, ()>,
{
    #[inline]
    pub fn new(slice: &'b [u8]) -> core::result::Result<Self, CborError> {
        let mut d = Decoder::new(slice);

        let n = d.array().ok().flatten().unwrap_or(0);

        Ok(Self {
            d,
            rem: n,
            _t: PhantomData,
        })
    }

    #[inline]
    pub fn next_item(&mut self) -> Option<core::result::Result<T, CborError>> {
        if self.rem == 0 {
            return None;
        }
        self.rem -= 1;
        Some(self.d.decode_with(&mut ()))
    }
}

impl<'b, T> CborArrayView<'b, T>
where
    T: Decode<'b, ()>,
{
    #[inline]
    pub fn iter_stateful(&self) -> core::result::Result<CborArrayIter<'b, T>, CborError> {
        CborArrayIter::new(self.slice)
    }
}

/// Returns true if `payload` looks like a CBOR array whose 1st element (kind) is small uint 2.
#[inline]
pub fn is_block_node(payload: &[u8]) -> bool {
    payload.len() >= 2
        && payload[0] >= 0x80
        && payload[0] < 0xA0 // CBOR array (major type 4)
        && payload[1] == 0x02
}

#[inline]
pub fn is_entry_node(payload: &[u8]) -> bool {
    payload.len() >= 2 && payload[0] >= 0x84 && payload[0] < 0xA0 && payload[1] == 0x01
}

#[inline]
pub fn is_transaction_node(payload: &[u8]) -> bool {
    payload.len() >= 2 && payload[0] >= 0x80 && payload[0] < 0xA0 && payload[1] == 0x00
}

/// Fast Block metadata decoder that skips all unnecessary fields
#[inline]
pub fn decode_block_metadata(data: &[u8]) -> Result<(u64, SlotMeta)> {
    let mut d = Decoder::new(data);

    // Verify it's an array
    let array_len = d.array()?.ok_or_else(|| {
        NodeDecodeError::Cbor(CborError::message("indefinite array not supported"))
    })?;

    if array_len < 5 {
        return Err(NodeDecodeError::Cbor(CborError::message(
            "block array too short",
        )));
    }

    // [0] kind - verify it's 2 (Block)
    let kind = d.u64()?;
    if kind != 2 {
        return Err(NodeDecodeError::UnknownKind(kind));
    }

    // [1] slot - decode this
    let slot = d.u64()?;

    // [2] shredding - skip
    d.skip()?;

    // [3] entries - skip
    d.skip()?;

    // [4] meta - decode this
    let meta: SlotMeta = d.decode()?;

    // Don't need to read [5] rewards

    Ok((slot, meta))
}

/// Fast Entry hash decoder that skips all unnecessary fields
#[inline]
pub fn decode_entry_hash(data: &[u8]) -> Result<&[u8]> {
    let mut d = Decoder::new(data);

    // Verify it's an array
    let array_len = d.array()?.ok_or_else(|| {
        NodeDecodeError::Cbor(CborError::message("indefinite array not supported"))
    })?;

    if array_len < 3 {
        return Err(NodeDecodeError::Cbor(CborError::message(
            "entry array too short",
        )));
    }

    // [0] kind - verify it's 1 (Entry)
    let kind = d.u64()?;
    if kind != 1 {
        return Err(NodeDecodeError::UnknownKind(kind));
    }

    // [1] num_hashes - skip
    d.skip()?;

    // [2] hash - decode this as bytes
    let hash = d.bytes()?;

    // Don't need to read [3] transactions

    Ok(hash)
}

#[inline]
pub fn decode_entry_poh(data: &[u8]) -> crate::node::Result<(u64, &[u8])> {
    use minicbor::{Decoder, decode::Error as CborError};

    let mut d = Decoder::new(data);

    let array_len = d.array()?.ok_or_else(|| {
        crate::node::NodeDecodeError::Cbor(CborError::message("indefinite array not supported"))
    })?;

    if array_len < 3 {
        return Err(crate::node::NodeDecodeError::Cbor(CborError::message(
            "entry array too short",
        )));
    }

    let kind = d.u64()?;
    if kind != 1 {
        return Err(crate::node::NodeDecodeError::UnknownKind(kind));
    }

    // [1] num_hashes
    let num_hashes = d.u64()?;

    // [2] hash bytes
    let hash = d.bytes()?;

    Ok((num_hashes, hash))
}

#[inline]
pub fn decode_entry_summary(data: &[u8]) -> crate::node::Result<(u64, &[u8], usize)> {
    use minicbor::{Decoder, decode::Error as CborError};

    let mut d = Decoder::new(data);

    let array_len = d.array()?.ok_or_else(|| {
        crate::node::NodeDecodeError::Cbor(CborError::message("indefinite array not supported"))
    })?;

    if array_len < 4 {
        return Err(crate::node::NodeDecodeError::Cbor(CborError::message(
            "entry array too short",
        )));
    }

    let kind = d.u64()?;
    if kind != 1 {
        return Err(crate::node::NodeDecodeError::UnknownKind(kind));
    }

    let num_hashes = d.u64()?;
    let hash = d.bytes()?;

    let tx_count = d.array()?.ok_or_else(|| {
        crate::node::NodeDecodeError::Cbor(CborError::message("indefinite array not supported"))
    })? as usize;

    Ok((num_hashes, hash, tx_count))
}

#[cfg(test)]
mod tests {
    use super::{DataFrame, Node, decode_node};
    use minicbor::{Decode, Decoder, Encoder};

    #[test]
    fn dataframe_hash_accepts_signed_cbor_i64() {
        let signed_hash = -5_787_489_622_768_765_176i64;
        let mut e = Encoder::new(Vec::new());
        e.array(5).expect("array");
        e.u64(6).expect("kind");
        e.i64(signed_hash).expect("signed hash");
        e.null().expect("index");
        e.null().expect("total");
        e.bytes(&[1, 2, 3]).expect("data");
        let payload = e.into_writer();

        let Node::DataFrame(frame) = decode_node(&payload).expect("decode dataframe") else {
            panic!("expected dataframe");
        };

        assert_eq!(frame.hash, Some(signed_hash as u64));
        assert!(frame.hash_was_negative);
        assert_eq!(frame.index, None);
        assert_eq!(frame.total, None);
        assert_eq!(frame.data, &[1, 2, 3]);
    }

    #[test]
    fn dataframe_hash_keeps_unsigned_cbor_marker() {
        let mut e = Encoder::new(Vec::new());
        e.array(5).expect("array");
        e.u64(6).expect("kind");
        e.u64(42).expect("unsigned hash");
        e.null().expect("index");
        e.null().expect("total");
        e.bytes(&[]).expect("data");
        let payload = e.into_writer();

        let mut d = Decoder::new(&payload);
        let frame = DataFrame::decode(&mut d, &mut ()).expect("decode dataframe");

        assert_eq!(frame.hash, Some(42));
        assert!(!frame.hash_was_negative);
    }
}
