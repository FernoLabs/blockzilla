use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[repr(C)]
pub struct KeyStats {
    pub id: u32,
    pub count: u64,
    pub first_fee_payer: u32,
    pub first_slot: u64,
    pub epoch: u64,
}

#[derive(Parser)]
#[command(name = "registry-splitter")]
#[command(about = "Split a large registry file into per-epoch files and a pubkey-only set")]
struct Cli {
    /// Input registry file (e.g., registry-0500.bin)
    #[arg(short, long)]
    input: PathBuf,

    /// Output directory for split files
    #[arg(short, long, default_value = "./split")]
    output: PathBuf,

    /// Create pubkey-only hashset file
    #[arg(long, default_value_t = true)]
    create_pubkey_set: bool,

    /// Use streaming mode (lower memory, slower)
    #[arg(long, default_value_t = true)]
    streaming: bool,

    /// Buffer size for streaming (bytes)
    #[arg(long, default_value_t = 50_000_000)]
    buffer_size: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("📂 Reading registry from: {}", cli.input.display());
    std::fs::create_dir_all(&cli.output)?;

    if cli.streaming {
        process_streaming(&cli)?;
    } else {
        process_in_memory(&cli)?;
    }

    Ok(())
}

/// Streaming mode: Multiple passes, low memory usage
fn process_streaming(cli: &Cli) -> Result<()> {
    println!("🌊 Using STREAMING mode (low memory)");
    
    // Pass 1: Collect unique epochs and count entries
    println!("\n📊 Pass 1/3: Scanning for epochs...");
    let (epochs, total_entries) = scan_epochs(&cli.input)?;
    println!("   Found {} epochs with {} total entries", epochs.len(), total_entries);
    
    // Pass 2: Write per-epoch files
    println!("\n💾 Pass 2/3: Writing per-epoch files...");
    write_epoch_files_streaming(&cli.input, &cli.output, &epochs, total_entries)?;
    
    // Pass 3: Extract pubkeys only
    if cli.create_pubkey_set {
        println!("\n🔑 Pass 3/3: Extracting pubkeys...");
        write_pubkey_set_streaming(&cli.input, &cli.output.join("all-pubkeys.bin"))?;
    }
    
    // Write summary
    let summary_file = cli.output.join("summary.txt");
    write_summary_streaming(&cli.output, &epochs)?;
    
    println!("\n🎉 Split complete!");
    println!("   Output directory: {}", cli.output.display());
    println!("   Epoch files: {}", epochs.len());
    
    Ok(())
}

/// In-memory mode: Single pass, faster but high memory
fn process_in_memory(cli: &Cli) -> Result<()> {
    println!("⚡ Using IN-MEMORY mode (fast, high memory)");
    
    let file = File::open(&cli.input)?;
    let mut reader = BufReader::new(file);
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    
    println!("📊 Deserializing registry...");
    let registry: AHashMap<Pubkey, KeyStats> = postcard::from_bytes(&data)
        .context("Failed to deserialize registry")?;
    
    println!("✅ Loaded {} entries", registry.len());

    println!("🔍 Grouping entries by epoch...");
    let mut epoch_groups: AHashMap<u64, Vec<(Pubkey, KeyStats)>> = AHashMap::new();
    let mut all_pubkeys: AHashSet<Pubkey> = AHashSet::with_capacity(registry.len());

    let pb = ProgressBar::new(registry.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
    );

    for (pubkey, stats) in registry.iter() {
        epoch_groups
            .entry(stats.epoch)
            .or_insert_with(Vec::new)
            .push((*pubkey, *stats));
        all_pubkeys.insert(*pubkey);
        pb.inc(1);
    }
    pb.finish_with_message("Grouped!");

    let mut epochs: Vec<u64> = epoch_groups.keys().copied().collect();
    epochs.sort_unstable();

    println!("\n📋 Found {} unique epochs", epochs.len());
    println!("   Range: {} → {}", epochs.first().unwrap(), epochs.last().unwrap());

    println!("\n💾 Writing per-epoch files...");
    let pb = ProgressBar::new(epochs.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} epoch {msg}")
            .unwrap()
    );

    for epoch in &epochs {
        let epoch_file = cli.output.join(format!("epoch-{:04}.bin", epoch));
        let entries = epoch_groups.get(epoch).unwrap();
        
        let epoch_map: AHashMap<Pubkey, KeyStats> = entries
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();

        write_registry(&epoch_file, &epoch_map)?;
        
        pb.set_message(format!("{:04} ({} entries)", epoch, entries.len()));
        pb.inc(1);
    }
    pb.finish_with_message("Done!");

    if cli.create_pubkey_set {
        println!("\n🔑 Writing pubkey-only hashset...");
        let pubkey_file = cli.output.join("all-pubkeys.bin");
        write_pubkey_set(&pubkey_file, &all_pubkeys)?;
        println!("   ✅ Wrote {} unique pubkeys", all_pubkeys.len());
    }

    let summary_file = cli.output.join("summary.txt");
    write_summary(&summary_file, &epochs, &epoch_groups, all_pubkeys.len())?;

    println!("\n🎉 Split complete!");
    println!("   Output directory: {}", cli.output.display());
    println!("   Epoch files: {}", epochs.len());
    println!("   Total unique keys: {}", all_pubkeys.len());
    
    Ok(())
}

/// Scan file to discover all unique epochs
fn scan_epochs(path: &Path) -> Result<(Vec<u64>, usize)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    
    let registry: AHashMap<Pubkey, KeyStats> = postcard::from_bytes(&data)?;
    let mut epochs: AHashSet<u64> = AHashSet::new();
    
    for stats in registry.values() {
        epochs.insert(stats.epoch);
    }
    
    let mut sorted_epochs: Vec<u64> = epochs.into_iter().collect();
    sorted_epochs.sort_unstable();
    
    Ok((sorted_epochs, registry.len()))
}

/// Write per-epoch files with streaming (one epoch at a time)
fn write_epoch_files_streaming(
    input: &Path,
    output: &Path,
    epochs: &[u64],
    total_entries: usize,
) -> Result<()> {
    let pb = ProgressBar::new(epochs.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} epoch {msg}")
            .unwrap()
    );
    
    for epoch in epochs {
        // Read full registry and filter for this epoch
        let file = File::open(input)?;
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        
        let registry: AHashMap<Pubkey, KeyStats> = postcard::from_bytes(&data)?;
        
        // Filter for this epoch
        let epoch_map: AHashMap<Pubkey, KeyStats> = registry
            .into_iter()
            .filter(|(_, stats)| stats.epoch == *epoch)
            .collect();
        
        let epoch_file = output.join(format!("epoch-{:04}.bin", epoch));
        write_registry(&epoch_file, &epoch_map)?;
        
        pb.set_message(format!("{:04} ({} entries)", epoch, epoch_map.len()));
        pb.inc(1);
        
        // Drop epoch_map to free memory
        drop(epoch_map);
    }
    
    pb.finish_with_message("Done!");
    Ok(())
}

/// Extract all pubkeys in streaming fashion
fn write_pubkey_set_streaming(input: &Path, output: &Path) -> Result<()> {
    let file = File::open(input)?;
    let mut reader = BufReader::new(file);
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    
    let registry: AHashMap<Pubkey, KeyStats> = postcard::from_bytes(&data)?;
    let pubkeys: AHashSet<Pubkey> = registry.keys().copied().collect();
    
    println!("   ✅ Extracted {} unique pubkeys", pubkeys.len());
    write_pubkey_set(output, &pubkeys)?;
    
    Ok(())
}

/// Write summary for streaming mode
fn write_summary_streaming(output: &Path, epochs: &[u64]) -> Result<()> {
    let summary_file = output.join("summary.txt");
    let mut file = BufWriter::new(File::create(&summary_file)?);
    
    writeln!(file, "Registry Split Summary")?;
    writeln!(file, "=====================")?;
    writeln!(file)?;
    writeln!(file, "Number of epochs: {}", epochs.len())?;
    writeln!(file, "Epoch range: {} → {}", epochs.first().unwrap(), epochs.last().unwrap())?;
    writeln!(file)?;
    writeln!(file, "Per-Epoch Files:")?;
    writeln!(file, "-------------------")?;
    
    for epoch in epochs {
        let epoch_file = output.join(format!("epoch-{:04}.bin", epoch));
        if let Ok(metadata) = std::fs::metadata(&epoch_file) {
            writeln!(file, "epoch-{:04}.bin: {} bytes", epoch, metadata.len())?;
        }
    }
    
    file.flush()?;
    Ok(())
}

fn write_registry(path: &Path, registry: &AHashMap<Pubkey, KeyStats>) -> Result<()> {
    let data = postcard::to_allocvec(registry)?;
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&data)?;
    writer.flush()?;
    Ok(())
}

fn write_pubkey_set(path: &Path, pubkeys: &AHashSet<Pubkey>) -> Result<()> {
    let data = postcard::to_allocvec(pubkeys)?;
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&data)?;
    writer.flush()?;
    Ok(())
}

fn write_summary(
    path: &Path,
    epochs: &[u64],
    epoch_groups: &AHashMap<u64, Vec<(Pubkey, KeyStats)>>,
    total_keys: usize,
) -> Result<()> {
    let mut file = BufWriter::new(File::create(path)?);
    
    writeln!(file, "Registry Split Summary")?;
    writeln!(file, "=====================")?;
    writeln!(file)?;
    writeln!(file, "Total unique keys: {}", total_keys)?;
    writeln!(file, "Number of epochs: {}", epochs.len())?;
    writeln!(file, "Epoch range: {} → {}", epochs.first().unwrap(), epochs.last().unwrap())?;
    writeln!(file)?;
    writeln!(file, "Per-Epoch Breakdown:")?;
    writeln!(file, "-------------------")?;
    
    for epoch in epochs {
        let count = epoch_groups.get(epoch).map(|v| v.len()).unwrap_or(0);
        writeln!(file, "Epoch {:04}: {:>10} keys", epoch, count)?;
    }
    
    file.flush()?;
    Ok(())
}