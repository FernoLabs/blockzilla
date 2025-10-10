use anyhow::{Result, anyhow, bail};
use cid::Cid;
use serde_cbor::Value;

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

#[derive(Debug)]
pub struct TransactionNode {
    pub kind: u64,
    pub data: DataFrame,
    pub metadata: DataFrame,
    pub slot: u64,
    pub index: Option<u64>,
}

#[derive(Debug)]
pub struct EntryNode {
    pub kind: u64,
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub transactions: Vec<Cid>,
}

#[derive(Debug)]
pub struct Shredding {
    pub entry_end_idx: u64,
    pub shred_end_idx: u64,
}

#[derive(Debug)]
pub struct BlockNode {
    pub kind: u64,
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    pub entries: Vec<Cid>,
    pub meta: SlotMeta,
    pub rewards: Option<Cid>,
}

#[derive(Debug)]
pub struct SubsetNode {
    pub kind: u64,
    pub first: u64,
    pub last: u64,
    pub blocks: Vec<Cid>,
}

#[derive(Debug)]
pub struct EpochNode {
    pub kind: u64,
    pub epoch: u64,
    pub subsets: Vec<Cid>,
}

#[derive(Debug)]
pub struct RewardsNode {
    pub kind: u64,
    pub slot: u64,
    pub data: DataFrame,
}

#[derive(Debug, Default)]
pub struct DataFrame {
    pub kind: u64,
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
    pub next: Option<Vec<Cid>>,
}

#[derive(Debug, Default)]
pub struct SlotMeta {
    pub parent_slot: Option<u64>,
    pub blocktime: Option<i64>,
    pub block_height: Option<u64>,
}

#[inline]
fn extract_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Integer(i) if *i >= 0 => Some(*i as u64),
        _ => None,
    }
}

#[inline]
fn extract_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(i) if *i < i64::MAX as i128 => Some(*i as i64),
        _ => None,
    }
}

#[inline]
fn strict_cid(v: &Value, ctx: &str) -> Result<Cid> {
    match v {
        Value::Bytes(bytes) if bytes.len() > 1 => {
            Cid::try_from(&bytes[1..]).map_err(|e| anyhow!("{ctx}: {e}"))
        }
        Value::Bytes(_) => bail!("{ctx}: CID bytes too short"),
        _ => bail!("{ctx}: CID not bytes"),
    }
}

// Pre-allocate CID vector with capacity hint
#[inline]
fn extract_cid_vec(v: &Value, ctx: &str) -> Result<Vec<Cid>> {
    match v {
        Value::Array(list) => {
            let mut cids = Vec::with_capacity(list.len());
            for item in list {
                cids.push(strict_cid(item, ctx)?);
            }
            Ok(cids)
        }
        _ => Ok(Vec::new()),
    }
}

pub fn decode_node(data: &[u8]) -> Result<Node> {
    let value: Value = serde_cbor::from_slice(data)?;
    let Value::Array(arr) = value else {
        bail!("node not array");
    };
    
    let kind = arr.first()
        .and_then(extract_u64)
        .ok_or_else(|| anyhow!("missing kind id"))?;

    Ok(match kind {
        0 => Node::Transaction(decode_transaction(arr)?),
        1 => Node::Entry(decode_entry(arr)?),
        2 => Node::Block(decode_block(arr)?),
        3 => Node::Subset(decode_subset(arr)?),
        4 => Node::Epoch(decode_epoch(arr)?),
        5 => Node::Rewards(decode_rewards(arr)?),
        6 => Node::DataFrame(decode_dataframe(Value::Array(arr))?),
        _ => bail!("unknown kind id {kind}"),
    })
}

fn decode_transaction(arr: Vec<Value>) -> Result<TransactionNode> {
    if arr.len() != 5 {
        bail!("Transaction: expected 5 elements, got {}", arr.len());
    }
    
    let mut iter = arr.into_iter();
    let kind = extract_u64(&iter.next().unwrap()).unwrap_or_default();
    let data = decode_dataframe(iter.next().unwrap())?;
    let metadata = decode_dataframe(iter.next().unwrap())?;
    let slot = extract_u64(&iter.next().unwrap()).unwrap_or_default();
    let index = extract_u64(&iter.next().unwrap());

    Ok(TransactionNode { kind, data, metadata, slot, index })
}

fn decode_entry(arr: Vec<Value>) -> Result<EntryNode> {
    if arr.len() != 4 {
        bail!("Entry: expected 4 elements, got {}", arr.len());
    }

    let mut iter = arr.into_iter();
    let kind = extract_u64(&iter.next().unwrap()).unwrap_or_default();
    let num_hashes = extract_u64(&iter.next().unwrap()).unwrap_or_default();
    
    let hash = match iter.next().unwrap() {
        Value::Bytes(b) => b,
        _ => Vec::new(),
    };
    
    let transactions = extract_cid_vec(&iter.next().unwrap(), "entry tx")?;

    Ok(EntryNode { kind, num_hashes, hash, transactions })
}

fn decode_block(mut arr: Vec<Value>) -> Result<BlockNode> {
    if arr.len() < 5 {
        bail!("Block: expected ≥5 elements, got {}", arr.len());
    }

    let kind = extract_u64(&arr[0]).unwrap_or_default();
    let slot = extract_u64(&arr[1]).unwrap_or_default();
    
    let shredding = match &arr[2] {
        Value::Array(list) => {
            let mut result = Vec::with_capacity(list.len());
            for v in list {
                if let Value::Array(a) = v {
                    if a.len() == 2 {
                        result.push(Shredding {
                            entry_end_idx: extract_u64(&a[0]).unwrap_or_default(),
                            shred_end_idx: extract_u64(&a[1]).unwrap_or_default(),
                        });
                    }
                }
            }
            result
        }
        _ => Vec::new(),
    };

    let entries = extract_cid_vec(&arr[3], "block entry")?;
    let meta = decode_slot_meta(&arr[4])?;
    
    let rewards = if arr.len() > 5 {
        match &arr[5] {
            Value::Bytes(_) => Some(strict_cid(&arr[5], "block rewards")?),
            _ => None,
        }
    } else {
        None
    };

    Ok(BlockNode { kind, slot, shredding, entries, meta, rewards })
}

fn decode_subset(arr: Vec<Value>) -> Result<SubsetNode> {
    if arr.len() != 4 {
        bail!("Subset: expected 4 elements, got {}", arr.len());
    }

    let kind = extract_u64(&arr[0]).unwrap_or_default();
    let first = extract_u64(&arr[1]).unwrap_or_default();
    let last = extract_u64(&arr[2]).unwrap_or_default();
    let blocks = extract_cid_vec(&arr[3], "subset block")?;

    Ok(SubsetNode { kind, first, last, blocks })
}

fn decode_epoch(arr: Vec<Value>) -> Result<EpochNode> {
    if arr.len() != 3 {
        bail!("Epoch: expected 3 elements, got {}", arr.len());
    }

    let kind = extract_u64(&arr[0]).unwrap_or_default();
    let epoch = extract_u64(&arr[1]).unwrap_or_default();
    let subsets = extract_cid_vec(&arr[2], "epoch subset")?;

    Ok(EpochNode { kind, epoch, subsets })
}

fn decode_rewards(arr: Vec<Value>) -> Result<RewardsNode> {
    if arr.len() != 3 {
        bail!("Rewards: expected 3 elements, got {}", arr.len());
    }

    let kind = extract_u64(&arr[0]).unwrap_or_default();
    let slot = extract_u64(&arr[1]).unwrap_or_default();
    let data = decode_dataframe(arr[2].clone())?;

    Ok(RewardsNode { kind, slot, data })
}

fn decode_dataframe(v: Value) -> Result<DataFrame> {
    let Value::Array(arr) = v else {
        bail!("DataFrame not array");
    };
    
    if arr.len() < 5 {
        bail!("DataFrame: too short ({})", arr.len());
    }

    if extract_u64(&arr[0]) != Some(6) {
        bail!("invalid DataFrame header");
    }

    let hash = extract_u64(&arr[1]);
    let index = extract_u64(&arr[2]);
    let total = extract_u64(&arr[3]);
    
    let data = match &arr[4] {
        Value::Bytes(b) => b.clone(),
        _ => Vec::new(),
    };

    let next = if arr.len() > 5 {
        match &arr[5] {
            Value::Array(list) => {
                let mut cids = Vec::with_capacity(list.len());
                for x in list {
                    if let Ok(cid) = strict_cid(x, "dataframe next") {
                        cids.push(cid);
                    }
                }
                Some(cids)
            }
            _ => None,
        }
    } else {
        None
    };

    Ok(DataFrame { kind: 6, hash, index, total, data, next })
}

fn decode_slot_meta(v: &Value) -> Result<SlotMeta> {
    match v {
        Value::Array(a) => Ok(SlotMeta {
            parent_slot: a.first().and_then(extract_u64),
            blocktime: a.get(1).and_then(extract_i64),
            block_height: a.get(2).and_then(extract_u64),
        }),
        _ => Ok(SlotMeta::default()),
    }
}
