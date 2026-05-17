// src/bin/car_dot_all.rs
//
// DOT graph + summary for CAR CBOR nodes.
//
// Fixes included:
// - CID normalization: CBOR CID refs are often 37 bytes, CAR keys are 36 bytes.
// - DataFrame.next is an array of CID refs (definite or indefinite).
// - Transaction/Rewards embed DataFrame inline; we emit edges from the parent node
//   directly to each CID in the embedded DataFrame.next array.
//
// Usage:
//   cargo run --release --bin car_dot_all -- <file.car> > graph.dot 2> summary.txt
//   sfdp -x -Goverlap=scale -Tsvg graph.dot -o graph.svg

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Read};

use minicbor::Decoder;
use minicbor::data::Type;

const MAX_UVARINT_LEN_64: usize = 10;
const CID_LEN: usize = 36;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy)]
struct Edge {
    src: u64,
    dst: u64,
    label: &'static str,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!(
            "Usage: {} <file.car>",
            args.first().map(String::as_str).unwrap_or("car_dot_all")
        );
        std::process::exit(1);
    }
    let path = &args[1];

    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(256 << 20, file);

    // CAR header: uvarint(header_len) then skip bytes
    let header_len = read_uvarint64(&mut reader)? as usize;
    if header_len > 0 {
        skip_bytes(&mut reader, header_len)?;
    }

    let mut nodes: HashMap<u64, (u64, usize)> = HashMap::new(); // id64 -> (kind, payload_len)
    let mut edges: Vec<Edge> = Vec::new();

    let mut count_by_kind: [u64; 8] = [0; 8]; // 0..6 + 7=unknown
    let mut bytes_by_kind: [u64; 8] = [0; 8];

    // DataFrame.next field type histogram
    let mut df_next_null = 0u64;
    let mut df_next_array = 0u64;
    let mut df_next_other = 0u64;
    let mut df_next_err = 0u64;

    // Tx embedded DF.next edges count
    let mut tx_data_next_edges = 0u64;
    let mut tx_meta_next_edges = 0u64;
    let mut rewards_data_next_edges = 0u64;

    let mut cid = [0u8; CID_LEN];
    let mut payload: Vec<u8> = Vec::new();

    loop {
        let len = match read_uvarint64(&mut reader) {
            Ok(v) => v as usize,
            Err(_) => break,
        };

        reader.read_exact(&mut cid)?;
        let payload_len = len.checked_sub(CID_LEN).ok_or("len < CID_LEN")?;

        payload.clear();
        payload.resize(payload_len, 0);
        reader.read_exact(&mut payload)?;

        let id = id_from_car_cid36(&cid);

        let kind = peek_kind(&payload).unwrap_or(999);

        nodes.insert(id, (kind, payload_len));
        let b = kind_bucket(kind);
        count_by_kind[b] += 1;
        bytes_by_kind[b] += payload_len as u64;

        match kind {
            4 => {
                if let Ok(list) = epoch_subsets(&payload) {
                    for dst in list {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "epoch.subset",
                        });
                    }
                }
            }
            3 => {
                if let Ok(list) = subset_blocks(&payload) {
                    for dst in list {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "subset.block",
                        });
                    }
                }
            }
            2 => {
                if let Ok(list) = block_entries(&payload) {
                    for dst in list {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "block.entry",
                        });
                    }
                }
                if let Ok(Some(dst)) = block_rewards(&payload) {
                    edges.push(Edge {
                        src: id,
                        dst,
                        label: "block.rewards",
                    });
                }
            }
            1 => {
                if let Ok(list) = entry_txs(&payload) {
                    for dst in list {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "entry.tx",
                        });
                    }
                }
            }
            6 => match dataframe_next_array_ids(&payload) {
                Ok((ids, NextFieldClass::Array)) => {
                    df_next_array += 1;
                    for dst in ids {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "df.next",
                        });
                    }
                }
                Ok((_, NextFieldClass::Null)) => df_next_null += 1,
                Ok((_, NextFieldClass::Other)) => df_next_other += 1,
                Err(_) => df_next_err += 1,
            },
            0 => {
                // Transaction embeds two DataFrames inline: fields [1] and [2].
                // Emit edges from TX -> each CID in embedded df.next array.
                if let Ok((data_children, meta_children)) = tx_embedded_df_next_children(&payload) {
                    for dst in data_children {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "tx.data.next",
                        });
                        tx_data_next_edges += 1;
                    }
                    for dst in meta_children {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "tx.meta.next",
                        });
                        tx_meta_next_edges += 1;
                    }
                }
            }
            5 => {
                // Rewards embeds one DataFrame inline at field [2]
                if let Ok(children) = rewards_embedded_df_next_children(&payload) {
                    for dst in children {
                        edges.push(Edge {
                            src: id,
                            dst,
                            label: "rewards.data.next",
                        });
                        rewards_data_next_edges += 1;
                    }
                }
            }
            _ => {}
        }
    }

    // DOT
    println!("digraph car {{");
    println!("  rankdir=LR;");
    println!("  node [shape=box, fontsize=10];");

    for (id, (kind, size)) in nodes.iter() {
        println!(
            "  n{:016x} [label=\"{}\\n{} B\\nkind={}\"];",
            id,
            kind_name(*kind),
            size,
            kind
        );
    }
    for e in edges.iter() {
        println!(
            "  n{:016x} -> n{:016x} [label=\"{}\"];",
            e.src, e.dst, e.label
        );
    }
    println!("}}");

    // children-bytes summary
    let mut children_bytes_by_kind: [u64; 8] = [0; 8];
    let mut children_edges_by_kind: [u64; 8] = [0; 8];

    for e in edges.iter() {
        let (src_kind, _) = nodes.get(&e.src).copied().unwrap_or((999, 0));
        let (_, dst_size) = nodes.get(&e.dst).copied().unwrap_or((999, 0));
        let b = kind_bucket(src_kind);
        children_edges_by_kind[b] += 1;
        children_bytes_by_kind[b] += dst_size as u64;
    }

    // summaries to stderr
    eprintln!("summary (direct):");
    eprintln!("kind\tname\tcount\tbytes_total\tavg_bytes");
    for k in 0..=6 {
        let b = kind_bucket(k);
        let c = count_by_kind[b];
        let t = bytes_by_kind[b];
        let avg = if c > 0 { t as f64 / c as f64 } else { 0.0 };
        eprintln!("{k}\t{}\t{}\t{}\t{:.0}", kind_name(k), c, t, avg);
    }
    {
        let b = 7;
        let c = count_by_kind[b];
        let t = bytes_by_kind[b];
        let avg = if c > 0 { t as f64 / c as f64 } else { 0.0 };
        eprintln!("999\tunknown\t{}\t{}\t{:.0}", c, t, avg);
    }

    eprintln!();
    eprintln!("summary (children via edges):");
    eprintln!("kind\tname\tedges_out\tchildren_bytes_total\tavg_child_bytes_per_edge");
    for k in 0..=6 {
        let b = kind_bucket(k);
        let ecount = children_edges_by_kind[b];
        let t = children_bytes_by_kind[b];
        let avg = if ecount > 0 {
            t as f64 / ecount as f64
        } else {
            0.0
        };
        eprintln!("{k}\t{}\t{}\t{}\t{:.0}", kind_name(k), ecount, t, avg);
    }
    {
        let b = 7;
        let ecount = children_edges_by_kind[b];
        let t = children_bytes_by_kind[b];
        let avg = if ecount > 0 {
            t as f64 / ecount as f64
        } else {
            0.0
        };
        eprintln!("999\tunknown\t{}\t{}\t{:.0}", ecount, t, avg);
    }

    eprintln!();
    eprintln!(
        "dataframe.next field types (kind=6): null={df_next_null} array={df_next_array} other={df_next_other} err={df_next_err}"
    );
    eprintln!(
        "tx embedded edges: tx.data.next={tx_data_next_edges} tx.meta.next={tx_meta_next_edges} rewards.data.next={rewards_data_next_edges}"
    );

    Ok(())
}

#[inline]
fn kind_bucket(kind: u64) -> usize {
    if kind <= 6 { kind as usize } else { 7 }
}

fn kind_name(kind: u64) -> &'static str {
    match kind {
        0 => "Transaction",
        1 => "Entry",
        2 => "Block",
        3 => "Subset",
        4 => "Epoch",
        5 => "Rewards",
        6 => "DataFrame",
        _ => "Unknown",
    }
}

#[inline]
fn id_from_car_cid36(cid36: &[u8; CID_LEN]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    cid36.hash(&mut h);
    h.finish()
}

#[inline]
fn normalize_ref_cid(b: &[u8]) -> &[u8] {
    if b.len() == CID_LEN + 1 { &b[1..] } else { b }
}

#[inline]
fn id_from_ref_bytes(b: &[u8]) -> u64 {
    let b = normalize_ref_cid(b);
    let mut h = std::collections::hash_map::DefaultHasher::new();
    b.hash(&mut h);
    h.finish()
}

#[inline]
fn skip_tag_if_any(d: &mut Decoder<'_>) -> core::result::Result<(), minicbor::decode::Error> {
    if d.datatype()? == Type::Tag {
        let _ = d.tag()?;
    }
    Ok(())
}

#[inline]
fn peek_kind(payload: &[u8]) -> Option<u64> {
    let mut d = Decoder::new(payload);
    if skip_tag_if_any(&mut d).is_err() {
        return None;
    }
    if d.array().is_err() {
        return None;
    }
    d.u64().ok()
}

#[inline]
fn decode_cid_ref_bytes<'a>(
    d: &mut Decoder<'a>,
) -> core::result::Result<&'a [u8], minicbor::decode::Error> {
    if d.datatype()? == Type::Tag {
        let _ = d.tag()?;
    }
    let b = d.bytes()?;
    if b.len() <= 1 {
        return Err(minicbor::decode::Error::message("invalid CID bytes"));
    }
    Ok(b)
}

#[derive(Clone, Copy)]
enum NextFieldClass {
    Null,
    Array,
    Other,
}

/// Read a CBOR array of CID refs (definite or indefinite) and return ids.
/// Decoder must be positioned *at the array header* (datatype must be Array).
fn read_cid_array_ids<'a>(
    d: &mut Decoder<'a>,
) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    let n_opt = d.array()?; // consumes array header
    let mut out: Vec<u64> = Vec::new();

    match n_opt {
        Some(n) => {
            out.reserve(n as usize);
            for _ in 0..n {
                let cid = decode_cid_ref_bytes(d)?;
                out.push(id_from_ref_bytes(cid));
            }
        }
        None => {
            // Indefinite array: items until break (0xff).
            // minicbor exposes this as Type::Break in recent versions.
            loop {
                let dt = d.datatype()?;
                if dt == Type::Break {
                    d.skip()?; // consume break
                    break;
                }
                let cid = decode_cid_ref_bytes(d)?;
                out.push(id_from_ref_bytes(cid));
            }
        }
    }

    Ok(out)
}

/// Standalone DataFrame node: parse next array at field [5]
fn dataframe_next_array_ids(
    payload: &[u8],
) -> core::result::Result<(Vec<u64>, NextFieldClass), minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 6 {
        return Ok((Vec::new(), NextFieldClass::Other));
    }

    d.skip()?; // hash
    d.skip()?; // index
    d.skip()?; // total
    d.skip()?; // data

    match d.datatype()? {
        Type::Null => {
            d.skip()?;
            Ok((Vec::new(), NextFieldClass::Null))
        }
        Type::Array => {
            let ids = read_cid_array_ids(&mut d)?;
            Ok((ids, NextFieldClass::Array))
        }
        _ => {
            d.skip()?;
            Ok((Vec::new(), NextFieldClass::Other))
        }
    }
}

/// Decode an *embedded* DataFrame (inline) and return its next children IDs.
/// Decoder must be positioned at the start of the embedded DataFrame array value.
fn embedded_dataframe_next_children<'a>(
    d: &mut Decoder<'a>,
) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    // DataFrame: [kind, hash?, index?, total?, data, next?]
    d.array()?;
    let k = d.u64()?;
    if k != 6 {
        // Not a DataFrame, skip it so caller can keep going
        d.skip()?;
        return Ok(Vec::new());
    }

    d.skip()?; // hash
    d.skip()?; // index
    d.skip()?; // total
    d.skip()?; // data

    match d.datatype()? {
        Type::Null => {
            d.skip()?;
            Ok(Vec::new())
        }
        Type::Array => read_cid_array_ids(d),
        _ => {
            // unexpected type, just skip and return no children
            d.skip()?;
            Ok(Vec::new())
        }
    }
}

/// Transaction: [kind, data: DataFrame, metadata: DataFrame, slot, index?]
fn tx_embedded_df_next_children(
    payload: &[u8],
) -> core::result::Result<(Vec<u64>, Vec<u64>), minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 0 {
        return Ok((Vec::new(), Vec::new()));
    }

    // field [1] data: embedded DataFrame
    let data_children = embedded_dataframe_next_children(&mut d)?;

    // field [2] metadata: embedded DataFrame
    let meta_children = embedded_dataframe_next_children(&mut d)?;

    Ok((data_children, meta_children))
}

/// Rewards: [kind, slot, data: DataFrame]
fn rewards_embedded_df_next_children(
    payload: &[u8],
) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 5 {
        return Ok(Vec::new());
    }

    d.skip()?; // slot
    embedded_dataframe_next_children(&mut d)
}

// EpochNode: [kind, epoch, subsets]
fn epoch_subsets(payload: &[u8]) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 4 {
        return Ok(Vec::new());
    }
    d.skip()?; // epoch
    let n = match d.array()? {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let cid = decode_cid_ref_bytes(&mut d)?;
        out.push(id_from_ref_bytes(cid));
    }
    Ok(out)
}

// SubsetNode: [kind, first, last, blocks]
fn subset_blocks(payload: &[u8]) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 3 {
        return Ok(Vec::new());
    }
    d.skip()?; // first
    d.skip()?; // last
    let n = match d.array()? {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let cid = decode_cid_ref_bytes(&mut d)?;
        out.push(id_from_ref_bytes(cid));
    }
    Ok(out)
}

// BlockNode: [kind, slot, shredding, entries, meta, rewards?]
fn block_entries(payload: &[u8]) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 2 {
        return Ok(Vec::new());
    }
    d.skip()?; // slot
    d.skip()?; // shredding
    let n = match d.array()? {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let cid = decode_cid_ref_bytes(&mut d)?;
        out.push(id_from_ref_bytes(cid));
    }
    Ok(out)
}

fn block_rewards(payload: &[u8]) -> core::result::Result<Option<u64>, minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 2 {
        return Ok(None);
    }

    d.skip()?; // slot
    d.skip()?; // shredding
    d.skip()?; // entries
    d.skip()?; // meta

    match d.datatype()? {
        Type::Null => {
            d.skip()?;
            Ok(None)
        }
        _ => {
            let cid = decode_cid_ref_bytes(&mut d)?;
            Ok(Some(id_from_ref_bytes(cid)))
        }
    }
}

// EntryNode: [kind, num_hashes, hash, transactions]
fn entry_txs(payload: &[u8]) -> core::result::Result<Vec<u64>, minicbor::decode::Error> {
    let mut d = Decoder::new(payload);
    skip_tag_if_any(&mut d)?;
    d.array()?;
    let k = d.u64()?;
    if k != 1 {
        return Ok(Vec::new());
    }
    d.skip()?; // num_hashes
    d.skip()?; // hash
    let n = match d.array()? {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let cid = decode_cid_ref_bytes(&mut d)?;
        out.push(id_from_ref_bytes(cid));
    }
    Ok(out)
}

fn skip_bytes<R: Read>(r: &mut R, mut n: usize) -> Result<()> {
    let mut buf = [0u8; 1024];
    while n > 0 {
        let k = n.min(buf.len());
        r.read_exact(&mut buf[..k])?;
        n -= k;
    }
    Ok(())
}

fn read_uvarint64<R: Read>(r: &mut R) -> Result<u64> {
    let mut x = 0u64;
    let mut shift = 0;
    let mut b = [0u8; 1];

    for _ in 0..MAX_UVARINT_LEN_64 {
        r.read_exact(&mut b)?;
        let byte = b[0];
        if byte < 0x80 {
            return Ok(x | ((byte as u64) << shift));
        }
        x |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }
    Err("uvarint overflow".into())
}
