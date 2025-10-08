use anyhow::{Context, Result};
use blockzilla::block_stream::SolanaBlockStream;
use futures::{StreamExt, TryStreamExt};
use reqwest::Client;
use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
};
use tokio_util::{compat::TokioAsyncReadCompatExt, io::StreamReader};

use crate::optimizer::{to_compact_block, KeyRegistry};
use solana_transaction_status_client_types::EncodedConfirmedBlock;

/// Stream a remote `.car` file directly over HTTP and optimize it on the fly
pub async fn run_network_mode(source: &str, output_dir: Option<String>) -> Result<()> {
    println!("🌐 Streaming CAR file from {source}");

    // HTTP stream
    let client = Client::new();
    let resp = client.get(source).send().await?.error_for_status()?;
    let byte_stream = resp
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let reader = StreamReader::new(byte_stream).compat();

    // Solana CAR stream
    let mut stream = SolanaBlockStream::new(reader).await?;

    // Setup output directories
    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    fs::create_dir_all(&out_dir)?;

    // Extract epoch number from filename if present (optional)
    let file_stem = PathBuf::from(source)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "network".into());
    let epoch_name = file_stem.split('-').nth(1).unwrap_or("0");
    let epoch = epoch_name.parse::<u64>().unwrap_or(0);

    let bin_path = out_dir.join(format!("epoch-{epoch}.bin"));
    let idx_path = out_dir.join(format!("epoch-{epoch}.idx"));
    let reg_path = out_dir.join("registry.sqlite");

    // Init registry
    let mut reg = KeyRegistry::new();

    // Open output files
    let bin_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&bin_path)
        .context("Failed to open .bin file")?;
    let mut bin = BufWriter::with_capacity(8 * 1024 * 1024, bin_file);

    let idx_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&idx_path)
        .context("Failed to open .idx file")?;
    let mut idx = BufWriter::with_capacity(1024 * 1024, idx_file);

    // Counters
    let mut count: u64 = 0;
    let mut current_offset: u64 = 0;

    // Stream + compress + index
    while let Some(block) = stream.next_solana_block().await? {
        let rpc_block: EncodedConfirmedBlock = block.try_into()?;
        let compact = to_compact_block(&rpc_block, &mut reg)?;

        // Serialize + compress (zstd level 5 = balanced)
        let raw = postcard::to_allocvec(&compact)?;
        let compressed = zstd::bulk::compress(&raw, 5)?;
        let len = compressed.len() as u32;

        // Write block to .bin
        bin.write_all(&len.to_le_bytes())?;
        bin.write_all(&compressed)?;

        // Write slot + offset to .idx
        idx.write_all(&compact.slot.to_le_bytes())?;
        idx.write_all(&current_offset.to_le_bytes())?;

        current_offset += 4 + compressed.len() as u64;
        count += 1;

        if count.is_multiple_of(100) {
            bin.flush()?;
            idx.flush()?;
            reg.save_to_sqlite(reg_path.to_str().unwrap())?;
            println!(
                "Processed {} blocks, {} unique keys, file ≈ {:.2} MB",
                count,
                reg.len(),
                current_offset as f64 / 1_000_000.0
            );
        }
    }

    bin.flush()?;
    idx.flush()?;
    reg.save_to_sqlite(reg_path.to_str().unwrap())?;

    println!(
        "✅ Done streaming: {} blocks → {}, {} unique keys",
        count,
        bin_path.display(),
        reg.len()
    );

    Ok(())
}
