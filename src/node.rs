use cid::Cid;
use minicbor::{Decode, Decoder, Encode, data::Type};

#[derive(Debug, Clone)]
pub struct CborCid(pub Cid);

impl<'b, C> Decode<'b, C> for CborCid {
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
        let cid = Cid::try_from(&bytes[1..])
            .map_err(|e| minicbor::decode::Error::message(format!("invalid CID: {e}")))?;
        Ok(Self(cid))
    }
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct DataFrame {
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
    pub data: Vec<u8>,
    #[n(5)]
    pub next: Option<CborCid>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct SlotMeta {
    #[n(0)]
    pub parent_slot: Option<u64>,
    #[n(1)]
    pub blocktime: Option<i64>,
    #[n(2)]
    pub block_height: Option<u64>,
}

#[derive(Debug, Decode, Encode)]
#[cbor(array)]
pub struct Shredding {
    #[n(0)]
    pub entry_end_idx: i64,
    #[n(1)]
    pub shred_end_idx: i64,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct TransactionNode {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub data: DataFrame,
    #[n(2)]
    pub metadata: DataFrame,
    #[n(3)]
    pub slot: u64,
    #[n(4)]
    pub index: Option<u64>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct EntryNode {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub num_hashes: u64,
    #[n(2)]
    #[cbor(decode_with = "minicbor::bytes::decode")]
    pub hash: Vec<u8>,
    #[n(3)]
    pub transactions: Vec<CborCid>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct BlockNode {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub slot: u64,
    #[n(2)]
    pub shredding: Vec<Shredding>,
    #[n(3)]
    pub entries: Vec<CborCid>,
    #[n(4)]
    pub meta: SlotMeta,
    #[n(5)]
    pub rewards: Option<CborCid>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct SubsetNode {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub first: u64,
    #[n(2)]
    pub last: u64,
    #[n(3)]
    pub blocks: Vec<CborCid>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct EpochNode {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub epoch: u64,
    #[n(2)]
    pub subsets: Vec<CborCid>,
}

#[derive(Debug, Decode)]
#[cbor(array)]
pub struct RewardsNode {
    #[n(0)]
    pub kind: u64,
    #[n(1)]
    pub slot: u64,
    #[n(2)]
    pub data: DataFrame,
}

#[derive(Debug)]
pub enum Node {
    Transaction(TransactionNode),
    Entry(EntryNode),
    Block(BlockNode),
    Subset(SubsetNode),
    Epoch(EpochNode),
    Rewards(RewardsNode),
    DataFrame(DataFrame),
}

pub fn decode_node(data: &[u8]) -> anyhow::Result<Node> {
    // 1) Peek `kind` without consuming the real decode stream.
    let mut peek = minicbor::Decoder::new(data);
    let _ = peek.array()?; // enter the top-level array
    let kind = peek.u64()?; // first element is the kind

    // 2) Decode the full value with a fresh decoder so the derive sees the array header.
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
