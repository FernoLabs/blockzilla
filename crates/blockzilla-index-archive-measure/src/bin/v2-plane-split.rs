//! Measure the message / metadata split inside real Archive V2 hot blocks.
use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE, deserialize_archive_v2_hot_block_blob, read_archive_v2_hot_block_index};

fn main() -> Result<()> {
    let dir = PathBuf::from(
        env::args()
            .nth(1)
            .context("usage: v2split <generation-dir>")?,
    );
    let index = read_archive_v2_hot_block_index(&dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    let blob = fs::read(dir.join(ARCHIVE_V2_BLOCKS_FILE))?;

    let (mut message, mut metadata, mut frame_uncompressed, mut frame_compressed) =
        (0u64, 0u64, 0u64, 0u64);
    let (mut blocks, mut txs) = (0u64, 0u64);
    for row in &index.rows {
        let start = row.compressed_offset as usize;
        let end = start + row.compressed_len as usize;
        let raw = zstd::decode_all(&blob[start..end]).context("zstd decode block frame")?;
        let block = deserialize_archive_v2_hot_block_blob(&raw).context("decode hot block")?;
        message += block.message_bytes.len() as u64;
        metadata += block.metadata_bytes.len() as u64;
        frame_uncompressed += raw.len() as u64;
        frame_compressed += row.compressed_len as u64;
        blocks += 1;
        txs += u64::from(block.tx_count);
    }
    let payload = message + metadata;
    println!("blocks                        {blocks}");
    println!("transactions                  {txs}");
    println!("message_bytes                 {message:>10}");
    println!("metadata_bytes                {metadata:>10}");
    println!(
        "metadata share of payload     {:>9.2}%",
        metadata as f64 / payload as f64 * 100.0
    );
    println!("frame uncompressed            {frame_uncompressed:>10}");
    println!("frame compressed (zstd)       {frame_compressed:>10}");
    // Metadata is the only region replay can drop: it still needs the header,
    // the transaction row directory, and the message bytes.
    let replay_needed = frame_uncompressed - metadata;
    println!(
        "replay needs                  {replay_needed:>10}  ({:.2}% of the frame)",
        replay_needed as f64 / frame_uncompressed as f64 * 100.0
    );
    println!(
        "replay decompression waste    {:>9.2}x",
        frame_uncompressed as f64 / replay_needed as f64
    );
    Ok(())
}
