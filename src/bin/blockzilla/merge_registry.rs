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

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Entry {
    pubkey: [u8; 32],
    first_epoch: u16,
    last_epoch: u16,
}

/// Merge all `pubkeys-XXXX.bin` files into one deduplicated registry
/// Each entry: pubkey (32B) + first_epoch (u16) + last_epoch (u16)
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

    // Extract first and last epochs for header display
    let extract_epoch = |name: &str| -> Option<u16> {
        Some(
            name.chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .ok()?,
        )
    };
    let first_epoch = extract_epoch(&files.first().unwrap().file_name().unwrap().to_string_lossy())
        .unwrap_or(0);
    let last_epoch = extract_epoch(&files.last().unwrap().file_name().unwrap().to_string_lossy())
        .unwrap_or(0);

    // 2️⃣ Setup MultiProgress
    let mp = MultiProgress::new();

    let global_line = mp.add(ProgressBar::new_spinner());
    global_line.set_style(
        ProgressStyle::with_template("{msg}")
            .unwrap()
            .tick_strings(&["", ""]),
    );

    let file_info = mp.add(ProgressBar::new_spinner());
    file_info.set_style(
        ProgressStyle::with_template("{msg}")
            .unwrap()
            .tick_strings(&["", ""]),
    );

    let file_bar = mp.add(ProgressBar::new(1));
    file_bar.set_style(
        ProgressStyle::with_template("[{bar:60.cyan/blue}]")
            .unwrap()
            .progress_chars("=>-"),
    );

    // 3️⃣ Global state
    let total_files = files.len();
    let mut map: AHashMap<[u8; 32], (u16, u16)> = AHashMap::with_capacity(50_000_000);
    let start_global = Instant::now();

    // 4️⃣ Merge loop
    for (idx, path) in files.iter().enumerate() {
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let epoch: u16 = extract_epoch(&file_name).unwrap_or(0);

        let Ok(mut reader) = File::open(path).map(BufReader::new) else {
            eprintln!("⚠️ Could not open {}", path.display());
            continue;
        };

        // Read header (8 bytes = u64 count)
        let mut count_buf = [0u8; 8];
        if reader.read_exact(&mut count_buf).is_err() {
            eprintln!("⚠️ Invalid header in {}", path.display());
            continue;
        }
        let count = u64::from_le_bytes(count_buf);

        file_bar.set_length(count);
        file_bar.set_position(0);

        let start_file = Instant::now();
        let mut processed = 0u64;
        let mut buf = [0u8; 32];
        let mut last_update = Instant::now();

        while reader.read_exact(&mut buf).is_ok() {
            map.entry(buf)
                .and_modify(|e| e.1 = epoch)
                .or_insert((epoch, epoch));
            processed += 1;

            if processed % 100_000 == 0 || last_update.elapsed() > Duration::from_millis(200) {
                last_update = Instant::now();

                let global_pct =
                    (idx as f64 + processed as f64 / count as f64) / total_files as f64 * 100.0;
                let elapsed_global = start_global.elapsed();
                let elapsed_file = start_file.elapsed();
                let speed = processed as f64 / elapsed_file.as_secs_f64() / 1000.0;
                let eta = if speed > 0.0 {
                    Duration::from_secs_f64((count - processed) as f64 / (speed * 1000.0))
                } else {
                    Duration::ZERO
                };

                global_line.set_message(format!(
                    "🧱  {first_epoch:04}→{last_epoch:04} | {}/{} epochs | unique {:>12} | elapsed {:>6} | {:>5.1}% total",
                    idx + 1,
                    total_files,
                    map.len(),
                    humantime::format_duration(elapsed_global),
                    global_pct
                ));

                file_info.set_message(format!(
                    "📦  {:>10}/{:<10} rows | {:>8} | {:>5.1}k/s",
                    processed,
                    count,
                    humantime::format_duration(eta),
                    speed
                ));

                file_bar.set_position(processed);
            }
        }

        file_bar.finish_with_message("");
    }

    mp.clear()?;
    println!(
        "\n✅ Merge complete in {:.1?} → {} unique pubkeys collected",
        start_global.elapsed(),
        map.len()
    );

    // 5️⃣ Writing merged file
    println!("💾 Writing merged file → {}\n", dest_path.display());
    let mut writer = BufWriter::new(File::create(dest_path)?);

    // Header: magic + count
    writer.write_all(b"BLZ2")?; // version marker for first/last epochs
    writer.write_all(&(map.len() as u64).to_le_bytes())?;

    let entry_size = mem::size_of::<Entry>();
    let total_entries = map.len() as u64;

    let write_pb = mp.add(ProgressBar::new(total_entries));
    write_pb.set_style(
        ProgressStyle::with_template("💾 [{bar:60.green/white}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("=>-"),
    );
    write_pb.enable_steady_tick(Duration::from_millis(120));

    let mut buf = [0u8; mem::size_of::<Entry>()];
    for (i, (pubkey, (first, last))) in map.iter().enumerate() {
        let entry = Entry {
            pubkey: *pubkey,
            first_epoch: *first,
            last_epoch: *last,
        };
        let bytes: &[u8; 36] = unsafe { std::mem::transmute(&entry) };
        writer.write_all(bytes)?;

        if i % 100_000 == 0 {
            write_pb.set_position(i as u64);
        }
    }

    writer.flush()?;
    write_pb.finish_with_message("✅ Done writing");

    println!(
        "\n✅ Wrote {} entries ({:.2} MB, {} bytes each including first/last epochs)",
        map.len(),
        (map.len() as f64 * entry_size as f64) / 1_000_000.0,
        entry_size
    );

    Ok(())
}
