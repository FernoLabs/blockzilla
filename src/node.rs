use cid::Cid;
use core::marker::PhantomData;
use minicbor::decode::Error as CborError;
use minicbor::{Decode, Decoder, Encode, data::Type};

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

pub fn decode_node(data: &[u8]) -> anyhow::Result<Node<'_>> {
    let kind = peek_node_type(data)?;
    let mut d = minicbor::Decoder::new(data);

    Ok(match kind {
        0 => Node::Transaction(d.decode()?),
        1 => Node::Entry(d.decode()?),
        2 => Node::Block(d.decode()?),
        3 => Node::Subset(d.decode()?),
        4 => Node::Epoch(d.decode()?),
        5 => Node::Rewards(d.decode()?),
        6 => Node::DataFrame(d.decode()?),
        _ => anyhow::bail!("unknown kind id {kind}"),
    })
}

pub fn peek_node_type(data: &[u8]) -> anyhow::Result<u64> {
    let mut peek = minicbor::Decoder::new(data);
    let _ = peek.array()?;
    let kind = peek.u64()?;
    Ok(kind)
}
#[derive(Debug, Clone, Copy)]
pub struct CborCidRef<'a> {
    pub bytes: &'a [u8],
}

impl<'a> CborCidRef<'a> {
    #[inline]
    pub fn to_cid(&self) -> Result<Cid, cid::Error> {
        Cid::read_bytes(&self.bytes[1..])
    }

    #[inline]
    pub fn hash_bytes(&self) -> &[u8] {
        &self.bytes[1..]
    }
}

impl<'b, C> Decode<'b, C> for CborCidRef<'b> {
    fn decode(
        d: &mut Decoder<'b>,
        _: &mut C,
    ) -> std::result::Result<Self, minicbor::decode::Error> {
        if d.datatype()? == Type::Tag {
            let _ = d.tag()?;
        }
        let bytes = d.bytes()?;
        if bytes.len() <= 1 {
            return Err(minicbor::decode::Error::message("invalid CID bytes"));
        }
        Ok(Self { bytes })
    }
}

#[derive(Debug, Clone)]
pub struct CborArrayView<'b, T> {
    slice: &'b [u8],
    _t: PhantomData<T>,
}

impl<'b, C, T> Decode<'b, C> for CborArrayView<'b, T> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, CborError> {
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
    pub fn len(&self) -> usize {
        let mut d = minicbor::Decoder::new(self.slice);
        d.array().unwrap_or(Some(0)).unwrap_or(0) as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<T, minicbor::decode::Error>> + 'b {
        let mut d = minicbor::Decoder::new(self.slice);
        let n = d.array().unwrap_or(Some(0)).unwrap_or(0);
        (0..n).map(move |_| d.decode_with(&mut ()))
    }
}
