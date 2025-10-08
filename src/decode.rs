use anyhow::{Result, bail};
use cid::Cid;
use serde_cbor::Value;

use crate::node::*;

pub fn decode_node(value: &Value) -> Result<Node> {
    let arr = expect_array(value, "node")?;
    if arr.is_empty() {
        bail!("empty node array");
    }

    let kind_id = expect_u64(arr.first(), "kind")?;
    match kind_id {
        0 => decode_transaction(arr),
        1 => decode_entry(arr),
        2 => decode_block(arr),
        3 => decode_subset(arr),
        4 => decode_epoch(arr),
        5 => decode_rewards(arr),
        6 => decode_dataframe_node(arr),
        _ => bail!("unknown kind id {kind_id}"),
    }
}

fn decode_transaction(f: &[Value]) -> Result<Node> {
    if f.len() < 3 {
        bail!("Transaction node too short");
    }

    let data = decode_dataframe(&f[1])?;
    let metadata = decode_dataframe(&f[2])?;
    let slot = f.get(3).and_then(extract_u64).unwrap_or_default();
    let index = f.get(4).and_then(extract_u64);

    Ok(Node::Transaction(Transaction {
        kind: 0,
        data,
        metadata,
        slot,
        index,
    }))
}

fn decode_entry(f: &[Value]) -> Result<Node> {
    if f.len() < 4 {
        bail!("Entry node too short");
    }

    let num_hashes = extract_u64(&f[1]).unwrap_or_default();
    let hash = extract_bytes(&f[2]).unwrap_or_default();
    let transactions = extract_array(&f[3])
        .unwrap_or(&vec![])
        .iter()
        .map(|v| strict_cid(v, "entry transaction"))
        .collect::<Result<Vec<_>>>()?;

    Ok(Node::Entry(Entry {
        kind: 1,
        num_hashes,
        hash,
        transactions,
    }))
}

fn decode_block(f: &[Value]) -> Result<Node> {
    if f.len() < 4 {
        bail!("Block node too short");
    }

    let slot = extract_u64(&f[1]).unwrap_or_default();

    let shredding = extract_array(&f[2])
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| {
            extract_array(v).and_then(|a| {
                if a.len() == 2 {
                    Some(Shredding {
                        entry_end_idx: extract_u64(&a[0]).unwrap_or_default(),
                        shred_end_idx: extract_u64(&a[1]).unwrap_or_default(),
                    })
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>();

    let entries = extract_array(&f[3])
        .unwrap_or(&vec![])
        .iter()
        .map(|v| strict_cid(v, "block entry"))
        .collect::<Result<Vec<_>>>()?;

    let meta = if let Some(v) = f.get(4) {
        decode_slot_meta(v)?
    } else {
        SlotMeta::default()
    };

    let rewards = match f.get(5) {
        Some(v) if matches!(v, Value::Bytes(_)) => Some(strict_cid(v, "block rewards")?),
        _ => None,
    };

    Ok(Node::Block(Block {
        kind: 2,
        slot,
        shredding,
        entries,
        meta,
        rewards,
    }))
}

fn decode_subset(f: &[Value]) -> Result<Node> {
    if f.len() < 4 {
        bail!("Subset node too short");
    }

    let first = extract_u64(&f[1]).unwrap_or_default();
    let last = extract_u64(&f[2]).unwrap_or_default();
    let blocks = extract_array(&f[3])
        .unwrap_or(&vec![])
        .iter()
        .map(|v| strict_cid(v, "subset block"))
        .collect::<Result<Vec<_>>>()?;

    Ok(Node::Subset(Subset {
        kind: 3,
        first,
        last,
        blocks,
    }))
}

fn decode_epoch(f: &[Value]) -> Result<Node> {
    if f.len() < 3 {
        bail!("Epoch node too short");
    }

    let epoch = extract_u64(&f[1]).unwrap_or_default();
    let subsets = extract_array(&f[2])
        .unwrap_or(&vec![])
        .iter()
        .map(|v| strict_cid(v, "epoch subset"))
        .collect::<Result<Vec<_>>>()?;

    Ok(Node::Epoch(Epoch {
        kind: 4,
        epoch,
        subsets,
    }))
}

fn decode_rewards(f: &[Value]) -> Result<Node> {
    if f.len() < 3 {
        bail!("Rewards node too short");
    }

    let slot = extract_u64(&f[1]).unwrap_or_default();
    let data = decode_dataframe(&f[2])?;

    Ok(Node::Rewards(Rewards {
        kind: 5,
        slot,
        data,
    }))
}

fn decode_dataframe_node(f: &[Value]) -> Result<Node> {
    Ok(Node::DataFrame(decode_dataframe(&Value::Array(
        f.to_vec(),
    ))?))
}

fn decode_dataframe(v: &Value) -> Result<DataFrame> {
    let arr = expect_array(v, "DataFrame")?;
    if arr.is_empty() || !matches!(arr.first(), Some(Value::Integer(6))) {
        bail!("invalid DataFrame header");
    }

    Ok(DataFrame {
        kind: 6,
        hash: arr.get(1).and_then(extract_u64),
        index: arr.get(2).and_then(extract_u64),
        total: arr.get(3).and_then(extract_u64),
        data: arr.get(4).and_then(extract_bytes).unwrap_or_default(),
        next: arr.get(5).and_then(|v| {
            extract_array(v).map(|links| {
                links
                    .iter()
                    .filter_map(|x| strict_cid(x, "dataframe next").ok())
                    .collect()
            })
        }),
    })
}

fn decode_slot_meta(v: &Value) -> Result<SlotMeta> {
    if let Value::Array(a) = v {
        Ok(SlotMeta {
            parent_slot: a.first().and_then(extract_u64),
            blocktime: a.get(1).and_then(extract_u64),
            block_height: a.get(2).and_then(extract_u64),
        })
    } else {
        Ok(SlotMeta::default())
    }
}

fn extract_u64(v: &Value) -> Option<u64> {
    if let Value::Integer(i) = v {
        if *i >= 0 { Some(*i as u64) } else { None }
    } else {
        None
    }
}

fn extract_bytes(v: &Value) -> Option<Vec<u8>> {
    if let Value::Bytes(b) = v {
        Some(b.clone())
    } else {
        None
    }
}

fn extract_array(v: &Value) -> Option<&Vec<Value>> {
    if let Value::Array(a) = v {
        Some(a)
    } else {
        None
    }
}

fn expect_array<'a>(v: &'a Value, ctx: &str) -> Result<&'a Vec<Value>> {
    extract_array(v).ok_or_else(|| anyhow::anyhow!("{ctx} is not an array"))
}

fn expect_u64(v: Option<&Value>, name: &str) -> Result<u64> {
    extract_u64(v.ok_or_else(|| anyhow::anyhow!("missing {name}"))?)
        .ok_or_else(|| anyhow::anyhow!("invalid {name}"))
}

fn strict_cid(v: &Value, ctx: &str) -> Result<Cid> {
    let bytes = extract_bytes(v).ok_or_else(|| anyhow::anyhow!("{ctx}: CID not bytes"))?;
    // not sure why we need to skip first byte here
    Cid::try_from(&bytes[1..]).map_err(|e| anyhow::anyhow!("{ctx}: {e}"))
}
