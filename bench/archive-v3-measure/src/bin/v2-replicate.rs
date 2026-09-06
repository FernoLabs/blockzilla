//! Grow an Archive V2 generation by repeating its blocks, for benchmarking.
//!
//! The CAR fixtures in this repo hold a single block each, which is enough to
//! check correctness and useless for measuring how conversion scales across
//! cores: one block cannot saturate one thread, let alone eight.
//!
//! This repeats a real generation's block frames N times, renumbering slots and
//! ordinals so the result is a structurally valid generation the converter
//! accepts unmodified. The block *contents* repeat, so treat absolute
//! throughput as optimistic — the decoded working set stays cache-resident in a
//! way a real epoch's would not. It is the **scaling curve** this is for, and
//! that is unaffected: every worker does the same real decode, page build and
//! compress that a real block costs.
use std::{
    env, fs,
    io::{BufWriter, Write},
    path::PathBuf,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_POH_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ArchiveV2HotBlockIndexRow, WincodeArchiveV2PohRecord, WincodeArchiveV2ShreddingRecord,
    deserialize_archive_v2_hot_block_blob, deserialize_archive_v2_poh_record,
    read_archive_v2_hot_block_index, write_archive_v2_hot_block_index,
};
use blockzilla_archive_v3::sidecars::framing;
use blockzilla_primitives::wincode_leb128_config;

const SIGNATURE_LEN: u64 = 64;
const BLOCKHASH_RECORD_LEN: usize = 32;

/// Split a retained sidecar file into its canonical LEB128-prefixed frames.
fn split_frames(mut bytes: &[u8]) -> Result<Vec<&[u8]>> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        // The prefix is a canonical u32 LEB128; walk it to find where the
        // payload starts, then hand the whole frame to the shared decoder so
        // this agrees with the reader by construction.
        let mut prefix_len = 0;
        loop {
            let byte = *bytes
                .get(prefix_len)
                .context("sidecar frame ends inside its length prefix")?;
            prefix_len += 1;
            if byte & 0x80 == 0 {
                break;
            }
            ensure!(
                prefix_len < 5,
                "sidecar frame length prefix is not canonical"
            );
        }
        let payload = framing::decode_frame(bytes).map_err(|error| anyhow::anyhow!(error))?;
        let frame_len = prefix_len + payload.len();
        frames.push(&bytes[..frame_len]);
        bytes = &bytes[frame_len..];
    }
    Ok(frames)
}

/// Make one block's final PoH hash unique by stamping its ordinal into it.
///
/// The converter requires a distinct final hash per block and uses it as the
/// block's blockhash registry record. It does **not** recompute the PoH chain,
/// so a stamped hash is structurally valid input. The result is therefore a
/// throughput fixture only: `verify-archive-v2-poh` would reject it, and it
/// must never be mistaken for chain data.
fn stamped_hash(mut hash: [u8; 32], block_index: u32) -> [u8; 32] {
    hash[..4].copy_from_slice(&block_index.to_le_bytes());
    hash
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let source = PathBuf::from(
        args.next()
            .context("usage: v2-replicate <src> <dst> <copies>")?,
    );
    let dest = PathBuf::from(
        args.next()
            .context("usage: v2-replicate <src> <dst> <copies>")?,
    );
    let copies: u32 = args
        .next()
        .context("usage: v2-replicate <src> <dst> <copies>")?
        .parse()
        .context("copies must be a positive integer")?;
    if copies == 0 {
        bail!("copies must be at least 1");
    }
    fs::create_dir_all(&dest).context("create destination")?;

    let index = read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    let blob = fs::read(source.join(ARCHIVE_V2_BLOCKS_FILE)).context("read blocks blob")?;
    if index.rows.is_empty() {
        bail!("source generation has no blocks");
    }

    // Replicas are restamped into one strictly ascending slot sequence below.
    let first_slot = index.rows.first().expect("non-empty").slot;
    let txs_per_copy: u64 = index.rows.iter().map(|row| u64::from(row.tx_count)).sum();
    let sigs_per_copy: u64 = index
        .rows
        .iter()
        .map(|row| u64::from(row.signature_count))
        .sum();

    // The retained sidecars are per-block too, and the converter checks that
    // each has exactly one frame per catalog row with matching block_id and
    // slot. Copying them verbatim would leave one frame against N blocks, so
    // they are rebuilt from the source frames alongside the block blob.
    let poh_source = fs::read(source.join(ARCHIVE_V2_POH_FILE)).context("read source PoH")?;
    let poh_templates = split_frames(&poh_source)?
        .into_iter()
        .map(|frame| {
            let payload = framing::decode_frame(frame).map_err(|e| anyhow::anyhow!(e))?;
            deserialize_archive_v2_poh_record(payload).context("decode source PoH frame")
        })
        .collect::<Result<Vec<_>>>()?;
    let shredding_source =
        fs::read(source.join(ARCHIVE_V2_SHREDDING_FILE)).context("read source shredding")?;
    let shredding_templates = split_frames(&shredding_source)?
        .into_iter()
        .map(|frame| {
            let payload = framing::decode_frame(frame).map_err(|e| anyhow::anyhow!(e))?;
            wincode::config::deserialize::<WincodeArchiveV2ShreddingRecord, _>(
                payload,
                wincode_leb128_config(),
            )
            .context("decode source shredding frame")
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        poh_templates.len() == index.rows.len() && shredding_templates.len() == index.rows.len(),
        "source has {} blocks but {} PoH and {} shredding frames",
        index.rows.len(),
        poh_templates.len(),
        shredding_templates.len()
    );

    let slots_per_epoch = 432_000_u64;
    let epoch_start = first_slot / slots_per_epoch * slots_per_epoch;
    ensure!(
        epoch_start >= slots_per_epoch,
        "fixture slot {first_slot} is in epoch 0, which has no previous tail"
    );

    let blob_out = fs::File::create(dest.join(ARCHIVE_V2_BLOCKS_FILE)).context("create blob")?;
    let mut blob_out = BufWriter::with_capacity(8 << 20, blob_out);
    let mut rows: Vec<ArchiveV2HotBlockIndexRow> =
        Vec::with_capacity(index.rows.len() * copies as usize);
    let mut offset = 0u64;
    let mut poh_out = Vec::new();
    let mut shredding_out = Vec::new();
    let mut registry_out = Vec::with_capacity(index.rows.len() * copies as usize * 32);

    for copy in 0..u64::from(copies) {
        for (position, row) in index.rows.iter().enumerate() {
            let start = row.compressed_offset as usize;
            let end = start + row.compressed_len as usize;
            let frame = blob
                .get(start..end)
                .context("index row points outside the blob")?;
            // The catalog takes its slot from block.header.slot inside the blob,
            // not from the index row, so a repeated frame would produce repeated
            // slots and fail the ascending check. Decode, restamp, re-encode.
            let raw = zstd::decode_all(frame).context("decode frame")?;
            let mut block =
                deserialize_archive_v2_hot_block_blob(&raw).context("deserialize block")?;
            // Slots are restamped contiguously from the epoch boundary rather
            // than offset from the source slot. The generation must start at
            // its epoch's first slot so that block 0's parent lands in the
            // previous epoch, which is where the predecessor tail has to sit.
            // Contiguous slots also satisfy the catalog's parent-link check
            // without carrying the source's skipped slots.
            let block_index = copy * index.rows.len() as u64 + position as u64;
            block.header.slot = epoch_start + block_index;
            block.header.parent_slot = block.header.slot - 1;
            let block_id = u32::try_from(block_index).context("block ordinal exceeds u32")?;
            // One registry record per block, so the block's own id is its
            // record index and its predecessor is the row before it.
            block.header.blockhash_id = block_id;
            block.header.previous_blockhash_id = block_id.saturating_sub(1);
            let restamped: Vec<u8> = wincode::config::serialize(&block, wincode_leb128_config())
                .context("re-serialize block")?;

            let template = &poh_templates[position];
            let final_hash = template
                .entries
                .last()
                .map(|entry| stamped_hash(entry.hash, block_id))
                .context("source PoH frame has no entries")?;
            let mut entries = template.entries.clone();
            entries.last_mut().expect("checked non-empty above").hash = final_hash;
            registry_out.extend_from_slice(&final_hash);
            poh_out.extend_from_slice(
                &framing::encode_frame(&wincode::config::serialize(
                    &WincodeArchiveV2PohRecord {
                        block_id,
                        slot: block.header.slot,
                        entries,
                    },
                    wincode_leb128_config(),
                )?)
                .map_err(|e| anyhow::anyhow!(e))?,
            );
            shredding_out.extend_from_slice(
                &framing::encode_frame(&wincode::config::serialize(
                    &WincodeArchiveV2ShreddingRecord {
                        block_id,
                        slot: block.header.slot,
                        shredding: shredding_templates[position].shredding.clone(),
                    },
                    wincode_leb128_config(),
                )?)
                .map_err(|e| anyhow::anyhow!(e))?,
            );
            let recompressed =
                zstd::encode_all(&restamped[..], index.level).context("recompress frame")?;
            let compressed_len = u32::try_from(recompressed.len()).context("frame exceeds u32")?;
            blob_out.write_all(&recompressed).context("write frame")?;
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id,
                slot: block.header.slot,
                compressed_offset: offset,
                compressed_len,
                uncompressed_len: u32::try_from(restamped.len()).context("blob exceeds u32")?,
                tx_count: row.tx_count,
                first_tx_ordinal: row.first_tx_ordinal + copy * txs_per_copy,
                first_signature_ordinal: row.first_signature_ordinal + copy * sigs_per_copy,
                signature_count: row.signature_count,
            });
            offset += u64::from(compressed_len);
        }
    }
    blob_out.flush().context("flush blob")?;
    drop(blob_out);
    write_archive_v2_hot_block_index(
        &dest.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        offset,
        index.level,
        index.flags,
        &rows,
    )?;

    // A generation needs a previous-epoch tail. The fixture has none, so one
    // record is synthesised at the last slot of the preceding epoch, which is
    // the range the converter requires. Like the stamped PoH hashes this is
    // structurally valid and chain-meaningless.
    let mut tail = Vec::with_capacity(40);
    tail.extend_from_slice(&stamped_hash([0x5a; 32], u32::MAX));
    tail.extend_from_slice(&(epoch_start - 1).to_le_bytes());
    fs::write(
        dest.join(blockzilla_archive_v2::ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
        &tail,
    )
    .context("write previous blockhash tail")?;

    fs::write(dest.join(ARCHIVE_V2_POH_FILE), &poh_out).context("write PoH")?;
    fs::write(dest.join(ARCHIVE_V2_SHREDDING_FILE), &shredding_out).context("write shredding")?;
    fs::write(dest.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), &registry_out)
        .context("write blockhash registry")?;
    ensure!(
        registry_out.len() == rows.len() * BLOCKHASH_RECORD_LEN,
        "blockhash registry has {} records for {} blocks",
        registry_out.len() / BLOCKHASH_RECORD_LEN,
        rows.len()
    );

    // Every other file is carried across as-is, except signatures: ordinals
    // above grew by a copy's worth per copy, so the file has to grow with them
    // or a catalog first_signature would point past its end.
    for entry in fs::read_dir(&source).context("read source dir")? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(
            &*name,
            ARCHIVE_V2_BLOCKS_FILE
                | ARCHIVE_V2_BLOCK_INDEX_FILE
                | ARCHIVE_V2_POH_FILE
                | ARCHIVE_V2_SHREDDING_FILE
                | ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE
        ) {
            continue;
        }
        if !entry.file_type()?.is_file() {
            continue;
        }
        let bytes = fs::read(entry.path()).with_context(|| format!("read {name}"))?;
        if name == ARCHIVE_V2_SIGNATURES_FILE {
            let mut out = fs::File::create(dest.join(&*name)).context("create signatures")?;
            for _ in 0..copies {
                out.write_all(&bytes).context("write signatures")?;
            }
            continue;
        }
        fs::write(dest.join(&*name), &bytes).with_context(|| format!("write {name}"))?;
    }

    let total_txs = txs_per_copy * u64::from(copies);
    println!(
        "{} blocks -> {} blocks ({} copies), {} transactions, {:.1} MiB of frames",
        index.rows.len(),
        rows.len(),
        copies,
        total_txs,
        offset as f64 / (1024.0 * 1024.0),
    );
    println!(
        "signatures: {} bytes ({} ordinals)",
        sigs_per_copy * u64::from(copies) * SIGNATURE_LEN,
        sigs_per_copy * u64::from(copies)
    );
    Ok(())
}
