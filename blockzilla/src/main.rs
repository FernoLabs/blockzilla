use anyhow::Result;
use clap::{Parser, ValueEnum};
use compact_archive::{
    carblock_to_compact::CompactBlock,
    io::{ArchiveFormat, read_block},
};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ArchiveFormatArg {
    Wincode,
    Postcard,
    Cbor,
}

impl ArchiveFormatArg {
    fn to_format(self) -> ArchiveFormat {
        match self {
            ArchiveFormatArg::Wincode => ArchiveFormat::Wincode,
            ArchiveFormatArg::Postcard => ArchiveFormat::Postcard,
            ArchiveFormatArg::Cbor => ArchiveFormat::Cbor,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "blockzilla", about = "Inspect compact Solana block archives")]
struct Args {
    /// Path to the compact archive file to read.
    #[arg(long)]
    input: PathBuf,

    /// Override the format instead of inferring it from the file extension.
    #[arg(long, value_enum)]
    format: Option<ArchiveFormatArg>,

    /// Print a line per block instead of a summary.
    #[arg(long)]
    verbose: bool,
}

fn detect_format(path: &PathBuf, override_fmt: Option<ArchiveFormatArg>) -> ArchiveFormat {
    if let Some(fmt) = override_fmt {
        return fmt.to_format();
    }

    compact_archive::io::ArchiveFormat::from_extension(path).unwrap_or(ArchiveFormat::Wincode)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let format = detect_format(&args.input, args.format);

    let file = File::open(&args.input)?;
    let mut reader = BufReader::new(file);

    let mut count = 0usize;
    let mut first_slot: Option<u64> = None;
    let mut last_slot: Option<u64> = None;

    while let Some(block) = read_block(&mut reader, format)? {
        handle_block(&block, args.verbose);
        count += 1;
        first_slot.get_or_insert(block.slot);
        last_slot = Some(block.slot);
    }

    println!("Blocks: {count}");
    if let Some(start) = first_slot {
        if let Some(end) = last_slot {
            println!("Slot range: {start} - {end}");
        }
    }

    Ok(())
}

fn handle_block(block: &CompactBlock, verbose: bool) {
    if verbose {
        println!(
            "slot={} txs={} rewards={}",
            block.slot,
            block.txs.len(),
            block.rewards.len()
        );
    }
}
