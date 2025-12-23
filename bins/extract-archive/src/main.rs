use std::{
    fs,
    io::{self, BufReader, Read},
    path::PathBuf,
};

use anyhow::Result;
use clap::Parser;
use optimized_archive::optimized_cbor::decode_owned_compact_block;

#[derive(Parser, Debug)]
#[command(about = "Read optimized CAR archives and optionally extract them to JSON")]
struct Args {
    /// Path to the optimized archive produced by optimize-car
    #[arg(value_name = "BLOCKS_CBOR")]
    input: PathBuf,

    /// Directory to write decoded JSON blocks into.
    #[arg(long)]
    output_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let file = fs::File::open(&args.input)?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::with_capacity(512 << 10);
    let mut blocks = 0usize;

    while let Some(len) = read_varint(&mut reader)? {
        buf.resize(len, 0);
        reader.read_exact(&mut buf)?;
        let block = decode_owned_compact_block(&buf)?;
        blocks += 1;

        if let Some(dir) = &args.output_dir {
            fs::create_dir_all(dir)?;
            let path = dir.join(format!("slot-{}.json", block.slot));
            let json = serde_json::to_vec_pretty(&block)?;
            fs::write(path, json)?;
        } else {
            println!("slot {} ({} transactions)", block.slot, block.txs.len());
        }
    }

    eprintln!("Decoded {blocks} blocks from {}", args.input.display());
    Ok(())
}

fn read_varint<R: Read>(rdr: &mut R) -> io::Result<Option<usize>> {
    let mut value: usize = 0;
    let mut shift = 0usize;
    let mut seen = 0usize;
    loop {
        let mut byte = [0u8; 1];
        match rdr.read_exact(&mut byte) {
            Ok(()) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                if seen == 0 {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
        seen += 1;
        let b = byte[0];
        value |= ((b & 0x7F) as usize) << shift;
        if (b & 0x80) == 0 {
            return Ok(Some(value));
        }
        shift += 7;
        if shift >= usize::BITS as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint overflow"));
        }
    }
}
