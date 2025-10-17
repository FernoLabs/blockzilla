use ahash::AHashMap;
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::types::KeyStats;

/// Compact merged registry entry written at the end
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct MergedEntry {
    pubkey: [u8; 32],
    total_count: u64,
    first_epoch: u16,
    last_epoch: u16,
    first_fee_payer: u32,
}

/// Merge multiple `.bin` files from the key extractor
/// into a single deduplicated binary registry file.
pub fn merge_bin_to_binary(src_dir: &PathBuf, dest_path: &PathBuf) -> Result<()> {
    println!(
        "\n🧱  Merging pubkeys from {} → {}\n{}",
        src_dir.display(),
        dest_path.display(),
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    );

    // 1️⃣ Collect .bin files
    let mut files: Vec<_> = fs::read_dir(src_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "bin").unwrap_or(false))
        .collect();
    files.sort();

    if files.is_empty() {
        println!("⚠️  No .bin files found in {}", src_dir.display());
        return Ok(());
    }

    // 2️⃣ Setup progress UI
    let mp = MultiProgress::new();
    let header = mp.add(ProgressBar::new_spinner());
    header.set_style(
        ProgressStyle::with_template("🌍 {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    header.enable_steady_tick(Duration::from_millis(250));

    // 3️⃣ Aggregation state
    let start_global = Instant::now();
    let mut map: AHashMap<[u8; 32], (u64, u16, u16, u32)> = AHashMap::with_capacity(50_000_000);

    // 4️⃣ Merge loop
    for (i, path) in files.iter().enumerate() {
        let Ok(mut reader) = File::open(path).map(BufReader::new) else {
            eprintln!("⚠️ Could not open {}", path.display());
            continue;
        };

        // --- Read header count (8 bytes)
        let mut count_buf = [0u8; 8];
        if reader.read_exact(&mut count_buf).is_err() {
            eprintln!("⚠️ Invalid header in {}", path.display());
            continue;
        }
        let count = u64::from_le_bytes(count_buf);

        // --- Buffers for pubkey + stats
        let mut pubkey_buf = [0u8; 32];
        let mut stats_buf = [0u8; mem::size_of::<KeyStats>()];

        let mut processed = 0u64;
        let mut last_update = Instant::now();

        while reader.read_exact(&mut pubkey_buf).is_ok()
            && reader.read_exact(&mut stats_buf).is_ok()
        {
            let stats = decode_keystats(&stats_buf);

            map.entry(pubkey_buf)
                .and_modify(|(total, first, last, fee)| {
                    *total += stats.count as u64;
                    *first = (*first).min(stats.first_epoch);
                    *last = (*last).max(stats.last_epoch);
                    if *fee == 0 {
                        *fee = stats.first_fee_payer;
                    }
                })
                .or_insert((
                    stats.count as u64,
                    stats.first_epoch,
                    stats.last_epoch,
                    stats.first_fee_payer,
                ));

            processed += 1;
            if processed % 200_000 == 0 || last_update.elapsed() > Duration::from_millis(300) {
                last_update = Instant::now();
                let pct = processed as f64 / count as f64 * 100.0;
                header.set_message(format!(
                    "📦 File {}/{} | {:>7}/{:<7} rows | {:>9} unique | {:>5.1}%",
                    i + 1,
                    files.len(),
                    processed,
                    count,
                    map.len(),
                    pct
                ));
            }
        }
    }

    mp.clear()?;
    println!(
        "\n✅ Merge complete in {:.1?} → {} unique pubkeys",
        start_global.elapsed(),
        map.len()
    );

    // 5️⃣ Write merged result
    println!("💾 Writing merged registry → {}", dest_path.display());
    let mut writer = BufWriter::new(File::create(dest_path)?);

    writer.write_all(b"BLZ3")?;
    writer.write_all(&(map.len() as u64).to_le_bytes())?;

    let entry_size = mem::size_of::<MergedEntry>();
    let pb_write = ProgressBar::new(map.len() as u64);
    pb_write.set_style(
        ProgressStyle::with_template("💾 [{bar:60.green/white}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb_write.enable_steady_tick(Duration::from_millis(150));

    for (i, (pubkey, (count, first, last, fee))) in map.iter().enumerate() {
        write_merged_entry(&mut writer, pubkey, *count, *first, *last, *fee)?;
        if i % 100_000 == 0 {
            pb_write.set_position(i as u64);
        }
    }

    writer.flush()?;
    pb_write.finish_with_message("✅ Done writing merged registry");

    println!(
        "\n✅ Wrote {} entries ({:.2} MB, {} bytes/entry)",
        map.len(),
        (map.len() as f64 * entry_size as f64) / 1_000_000.0,
        entry_size
    );

    Ok(())
}

/// Decode raw bytes into a KeyStats struct
fn decode_keystats(buf: &[u8]) -> KeyStats {
    let id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let count = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let first_fee_payer = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let first_epoch = u16::from_le_bytes(buf[12..14].try_into().unwrap());
    let last_epoch = u16::from_le_bytes(buf[14..16].try_into().unwrap());

    KeyStats {
        id,
        count,
        first_fee_payer,
        first_epoch,
        last_epoch,
    }
}

/// Write one merged entry in the BLZ3 format
fn write_merged_entry<W: Write>(
    w: &mut W,
    pubkey: &[u8; 32],
    total_count: u64,
    first_epoch: u16,
    last_epoch: u16,
    first_fee_payer: u32,
) -> Result<()> {
    w.write_all(pubkey)?;
    w.write_all(&total_count.to_le_bytes())?;
    w.write_all(&first_epoch.to_le_bytes())?;
    w.write_all(&last_epoch.to_le_bytes())?;
    w.write_all(&first_fee_payer.to_le_bytes())?;
    Ok(())
}
