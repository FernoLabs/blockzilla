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

fn extract_u64(v: &Value) -> Option<u64> {
    if let Value::Integer(i) = v {
        (*i >= 0).then_some(*i as u64)
    } else {
        None
    }
}

fn extract_i64(v: &Value) -> Option<i64> {
    if let Value::Integer(i) = v {
        (*i < i64::MAX as i128).then_some(*i as i64)
    } else {
        None
    }
}

fn strict_cid(v: &Value, ctx: &str) -> Result<Cid> {
    if let Value::Bytes(bytes) = v {
        Cid::try_from(&bytes[1..]).map_err(|e| anyhow!("{ctx}: {e}"))
    } else {
        bail!("{ctx}: CID not bytes");
    }
}

pub fn decode_node(data: &[u8]) -> Result<Node> {
    let value: Value = serde_cbor::from_slice(data)?;
    let Value::Array(arr) = value else {
        bail!("node not array");
    };
    if arr.is_empty() {
        bail!("empty node array");
    }

    let kind = extract_u64(&arr[0]).ok_or_else(|| anyhow!("missing kind id"))?;

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
    let [kind, data, metadata, slot, index] = arr.as_slice() else {
        bail!("Transaction: expected 5 elements, got {}", arr.len());
    };

    Ok(TransactionNode {
        kind: extract_u64(kind).unwrap_or_default(),
        data: decode_dataframe(data.clone())?,
        metadata: decode_dataframe(metadata.clone())?,
        slot: extract_u64(slot).unwrap_or_default(),
        index: extract_u64(index),
    })
}

fn decode_entry(arr: Vec<Value>) -> Result<EntryNode> {
    let [kind, num_hashes, hash, txs] = arr.as_slice() else {
        bail!("Entry: expected 4 elements, got {}", arr.len());
    };

    let transactions = match txs {
        Value::Array(list) => list
            .iter()
            .map(|v| strict_cid(v, "entry tx"))
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };

    let hash = match hash {
        Value::Bytes(b) => b.clone(),
        _ => Vec::new(),
    };

    Ok(EntryNode {
        kind: extract_u64(kind).unwrap_or_default(),
        num_hashes: extract_u64(num_hashes).unwrap_or_default(),
        hash,
        transactions,
    })
}

fn decode_block(arr: Vec<Value>) -> Result<BlockNode> {
    let [kind, slot, shredding, entries, meta, rest @ ..] = arr.as_slice() else {
        bail!("Block: expected ≥5 elements, got {}", arr.len());
    };

    let shredding = match shredding {
        Value::Array(list) => list
            .iter()
            .filter_map(|v| match v {
                Value::Array(a) if a.len() == 2 => Some(Shredding {
                    entry_end_idx: extract_u64(&a[0]).unwrap_or_default(),
                    shred_end_idx: extract_u64(&a[1]).unwrap_or_default(),
                }),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    let entries = match entries {
        Value::Array(list) => list
            .iter()
            .map(|v| strict_cid(v, "block entry"))
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };

    let meta = decode_slot_meta(meta)?;

    let rewards = rest
        .first()
        .and_then(|v| {
            if matches!(v, Value::Bytes(_)) {
                Some(strict_cid(v, "block rewards"))
            } else {
                None
            }
        })
        .transpose()?;

    Ok(BlockNode {
        kind: extract_u64(kind).unwrap_or_default(),
        slot: extract_u64(slot).unwrap_or_default(),
        shredding,
        entries,
        meta,
        rewards,
    })
}

fn decode_subset(arr: Vec<Value>) -> Result<SubsetNode> {
    let [kind, first, last, blocks] = arr.as_slice() else {
        bail!("Subset: expected 4 elements, got {}", arr.len());
    };

    let blocks = match blocks {
        Value::Array(list) => list
            .iter()
            .map(|v| strict_cid(v, "subset block"))
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };

    Ok(SubsetNode {
        kind: extract_u64(kind).unwrap_or_default(),
        first: extract_u64(first).unwrap_or_default(),
        last: extract_u64(last).unwrap_or_default(),
        blocks,
    })
}

fn decode_epoch(arr: Vec<Value>) -> Result<EpochNode> {
    let [kind, epoch, subsets] = arr.as_slice() else {
        bail!("Epoch: expected 3 elements, got {}", arr.len());
    };

    let subsets = match subsets {
        Value::Array(list) => list
            .iter()
            .map(|v| strict_cid(v, "epoch subset"))
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };

    Ok(EpochNode {
        kind: extract_u64(kind).unwrap_or_default(),
        epoch: extract_u64(epoch).unwrap_or_default(),
        subsets,
    })
}

fn decode_rewards(arr: Vec<Value>) -> Result<RewardsNode> {
    let [kind, slot, data] = arr.as_slice() else {
        bail!("Rewards: expected 3 elements, got {}", arr.len());
    };

    Ok(RewardsNode {
        kind: extract_u64(kind).unwrap_or_default(),
        slot: extract_u64(slot).unwrap_or_default(),
        data: decode_dataframe(data.clone())?,
    })
}

fn decode_dataframe(v: Value) -> Result<DataFrame> {
    let Value::Array(arr) = v else {
        bail!("DataFrame not array");
    };
    let [kind, hash, index, total, data, next @ ..] = arr.as_slice() else {
        bail!("DataFrame: too short ({})", arr.len());
    };

    if extract_u64(kind) != Some(6) {
        bail!("invalid DataFrame header");
    }

    Ok(DataFrame {
        kind: 6,
        hash: extract_u64(hash),
        index: extract_u64(index),
        total: extract_u64(total),
        data: match data {
            Value::Bytes(b) => b.clone(),
            _ => Vec::new(),
        },
        next: next.first().and_then(|v| match v {
            Value::Array(list) => Some(
                list.iter()
                    .filter_map(|x| strict_cid(x, "dataframe next").ok())
                    .collect(),
            ),
            _ => None,
        }),
    })
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
