use cid::Cid;
use core::marker::PhantomData;
use minicbor::data::Type;
use minicbor::decode::Error as CborError;
use minicbor::{Decode, Decoder, Encode};

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

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct DataFrame<'a> {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub hash: Option<u64>,
    #[n(2)]
    pub index: Option<u64>,
    #[n(3)]
    pub total: Option<u64>,
    #[n(4)]
    #[cbor(decode_with = "minicbor::bytes::decode")]
    pub data: &'a [u8],
    #[n(5)]
    pub next: Option<CborCidRef<'a>>,
}

#[derive(Debug, Decode, Clone)]
#[cbor(array)]
pub struct SlotMeta {
    #[n(0)]
    pub parent_slot: Option<u64>,
    #[n(1)]
    pub blocktime: Option<i64>,
    #[n(2)]
    pub block_height: Option<u64>,
}

#[derive(Debug, Decode, Encode, Clone)]
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
    pub shredding: Vec<Shredding>,
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
    pub blocks: Vec<CborCidRef<'a>>,
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
    pub subsets: Vec<CborCidRef<'a>>,
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
}

impl<'a> CborCidRef<'a> {
    #[inline]
    pub fn to_cid(&self) -> core::result::Result<Cid, cid::Error> {
        Cid::read_bytes(&self.bytes[1..])
    }

    #[inline]
    pub fn hash_bytes(&self) -> &'a [u8] {
        &self.bytes[1..]
    }
}

impl<'b, C> Decode<'b, C> for CborCidRef<'b> {
    #[inline]
    fn decode(d: &mut Decoder<'b>, _: &mut C) -> core::result::Result<Self, CborError> {
        if d.datatype()? == Type::Tag {
            let _ = d.tag()?;
        }
        let bytes = d.bytes()?;
        if bytes.len() <= 1 {
            return Err(CborError::message("invalid CID bytes"));
        }
        Ok(Self { bytes })
    }
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

        // If you ever hit indefinite arrays, this will treat them as length 0 (same as your len/iter).
        // If you want to support indefinite arrays, we can extend this.
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
