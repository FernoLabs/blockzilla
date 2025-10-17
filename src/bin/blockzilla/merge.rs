use ahash::AHashMap;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::{
    fs::{File},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};
use indicatif::{ProgressBar, ProgressStyle};

use crate::types::KeyStats;

/// Dump one chunk to disk (unsorted)
pub fn dump_chunk_to_bin(path: &Path, map: &AHashMap<Pubkey, KeyStats>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writer.write_all(&(map.len() as u64).to_le_bytes())?;
    for (pk, meta) in map {
        writer.write_all(&pk.to_bytes())?;
        writer.write_all(&meta.id.to_le_bytes())?;
        writer.write_all(&meta.count.to_le_bytes())?;
        writer.write_all(&meta.first_fee_payer.to_le_bytes())?;
        writer.write_all(&meta.first_epoch.to_le_bytes())?;
        writer.write_all(&meta.last_epoch.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

/// Merge all chunk files and final map, write to final .bin
pub fn merge_chunks_and_dump(
    bin_path: &Path,
    chunk_dir: &Path,
    final_map: AHashMap<Pubkey, KeyStats>,
    chunk_count: u32,
) -> Result<usize> {
    let mut merged: AHashMap<Pubkey, KeyStats> = final_map;
    let mut next_id = merged.values().map(|v| v.id).max().unwrap_or(0) + 1;

    for i in 0..chunk_count {
        let chunk_path = chunk_dir.join(format!("chunk-{i:06}.bin"));
        let file = File::open(&chunk_path)?;
        let mut reader = BufReader::new(file);

        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        for _ in 0..len {
            let mut pk_bytes = [0u8; 32];
            reader.read_exact(&mut pk_bytes)?;
            let pk = Pubkey::new_from_array(pk_bytes);

            let mut id_bytes = [0u8; 4];
            reader.read_exact(&mut id_bytes)?;
            // id will be replaced when merging
            let _id = u32::from_le_bytes(id_bytes);

            let mut count_bytes = [0u8; 4];
            reader.read_exact(&mut count_bytes)?;
            let count = u32::from_le_bytes(count_bytes);

            let mut fee_payer_bytes = [0u8; 4];
            reader.read_exact(&mut fee_payer_bytes)?;
            let fee_payer = u32::from_le_bytes(fee_payer_bytes);

            let mut first_epoch_bytes = [0u8; 2];
            reader.read_exact(&mut first_epoch_bytes)?;
            let first_epoch = u16::from_le_bytes(first_epoch_bytes);

            let mut last_epoch_bytes = [0u8; 2];
            reader.read_exact(&mut last_epoch_bytes)?;
            let last_epoch = u16::from_le_bytes(last_epoch_bytes);

            merged
                .entry(pk)
                .and_modify(|e| {
                    e.count += count;
                    e.last_epoch = e.last_epoch.max(last_epoch);
                })
                .or_insert(KeyStats {
                    id: next_id,
                    count,
                    first_fee_payer: fee_payer,
                    first_epoch,
                    last_epoch,
                });
            next_id += 1;
        }
    }

    dump_pubkeys_to_bin(bin_path, &merged)?;
    Ok(merged.len())
}

/// Write final merged map to binary file
pub fn dump_pubkeys_to_bin(path: &Path, map: &AHashMap<Pubkey, KeyStats>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writer.write_all(&(map.len() as u64).to_le_bytes())?;
    for (pk, meta) in map {
        writer.write_all(&pk.to_bytes())?;
        writer.write_all(&meta.id.to_le_bytes())?;
        writer.write_all(&meta.count.to_le_bytes())?;
        writer.write_all(&meta.first_fee_payer.to_le_bytes())?;
        writer.write_all(&meta.first_epoch.to_le_bytes())?;
        writer.write_all(&meta.last_epoch.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}





/// Merge all chunk files for one epoch into final map and write .bin output.
/// Safe for large epochs that fit entirely in RAM.
pub fn merge_chunks_and_dump_singlethread(
    bin_path: &Path,
    chunk_dir: &Path,
    final_map: AHashMap<Pubkey, KeyStats>,
    chunk_count: u32,
    epoch: u64,
) -> Result<usize> {
    let pb = ProgressBar::new(chunk_count as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "🧩 [epoch {prefix}] merging {pos}/{len} | {wide_msg}"
        )
        .unwrap()
        .progress_chars("━╾─"),
    );
    pb.set_prefix(format!("{epoch:04}"));
    pb.set_message("starting merge...");
    pb.enable_steady_tick(std::time::Duration::from_millis(250));

    let mut merged: AHashMap<Pubkey, KeyStats> = final_map;
    let mut next_id = merged.values().map(|v| v.id).max().unwrap_or(0) + 1;

    for i in 0..chunk_count {
        let chunk_path = chunk_dir.join(format!("chunk-{i:06}.bin"));
        pb.set_message(format!("reading {chunk_path:?}"));
        let file = File::open(&chunk_path)?;
        let mut reader = BufReader::new(file);

        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        for _ in 0..len {
            let mut pk_bytes = [0u8; 32];
            reader.read_exact(&mut pk_bytes)?;
            let pk = Pubkey::new_from_array(pk_bytes);

            let mut id_bytes = [0u8; 4];
            reader.read_exact(&mut id_bytes)?;
            let _id = u32::from_le_bytes(id_bytes);

            let mut count_bytes = [0u8; 4];
            reader.read_exact(&mut count_bytes)?;
            let count = u32::from_le_bytes(count_bytes);

            let mut fee_payer_bytes = [0u8; 4];
            reader.read_exact(&mut fee_payer_bytes)?;
            let fee_payer = u32::from_le_bytes(fee_payer_bytes);

            let mut first_epoch_bytes = [0u8; 2];
            reader.read_exact(&mut first_epoch_bytes)?;
            let first_epoch = u16::from_le_bytes(first_epoch_bytes);

            let mut last_epoch_bytes = [0u8; 2];
            reader.read_exact(&mut last_epoch_bytes)?;
            let last_epoch = u16::from_le_bytes(last_epoch_bytes);

            merged
                .entry(pk)
                .and_modify(|e| {
                    e.count += count;
                    e.last_epoch = e.last_epoch.max(last_epoch);
                })
                .or_insert(KeyStats {
                    id: next_id,
                    count,
                    first_fee_payer: fee_payer,
                    first_epoch,
                    last_epoch,
                });
            next_id += 1;
        }

        pb.inc(1);
    }

    pb.set_message(format!("writing final bin ({} keys)...", merged.len()));
    crate::merge::dump_pubkeys_to_bin(bin_path, &merged)?;
    pb.finish_with_message(format!("✅ merged {} keys total", merged.len()));
    Ok(merged.len())
}
