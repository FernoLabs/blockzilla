use anyhow::Result;
use clap::Parser;
use compact_archive::archive::CompactArchive;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about = "Inspect compacted archives")]
struct Cli {
    /// Path to the compact archive file (postcard encoded)
    #[arg(value_name = "ARCHIVE_FILE")]
    archive: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let archive = CompactArchive::read_from_path(&cli.archive)?;

    let total_txs: usize = archive.blocks.iter().map(|b| b.txs.len()).sum();
    println!(
        "Archive summary:\n  pubkeys: {}\n  blocks: {}\n  transactions: {}",
        archive.pubkeys.len(),
        archive.blocks.len(),
        total_txs
    );

    Ok(())
}
