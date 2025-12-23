use std::path::PathBuf;

use ahash::AHashMap;
use anyhow::Result;
use car_reader::car_block_reader::CarBlockReader;
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use optimized_archive::{
    carblock_to_compact::{
        CompactBlock, MetadataMode, PubkeyIdProvider, carblock_to_compactblock_inplace,
    },
    optimized_cbor,
};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

#[derive(Parser, Debug)]
#[command(about = "Convert CAR streams into the optimized archive format")]
struct Args {
    /// Path to the source CAR file.
    #[arg(long, value_name = "CAR_PATH")]
    input: PathBuf,

    /// Output file for the optimized archive.
    #[arg(long, default_value = "blocks.cbor")]
    output: PathBuf,

    /// Metadata encoding mode to use when compacting transactions.
    #[arg(long, value_enum, default_value_t = MetadataArg::Compact)]
    metadata: MetadataArg,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MetadataArg {
    Compact,
    Raw,
    None,
}

impl From<MetadataArg> for MetadataMode {
    fn from(value: MetadataArg) -> Self {
        match value {
            MetadataArg::Compact => MetadataMode::Compact,
            MetadataArg::Raw => MetadataMode::Raw,
            MetadataArg::None => MetadataMode::None,
        }
    }
}

#[derive(Default)]
struct SimplePubkeyIds {
    map: AHashMap<[u8; 32], u32>,
}

impl PubkeyIdProvider for SimplePubkeyIds {
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32> {
        if let Some(id) = self.map.get(key) {
            return Some(*id);
        }
        let next = self.map.len() as u32;
        self.map.insert(*key, next);
        Some(next)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
        Box::new(File::open(&args.input).await?);

    let mut car_reader = CarBlockReader::new(reader);
    car_reader.read_header().await?;

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] blocks: {pos}")
            .unwrap()
            .tick_strings(&["-", "\\", "|", "/"]),
    );

    let mut output = BufWriter::new(File::create(&args.output).await?);
    let mut scratch = Vec::with_capacity(512 << 10);
    let mut encoded = Vec::with_capacity(512 << 10);
    let mut provider = SimplePubkeyIds::default();
    let metadata_mode: MetadataMode = args.metadata.into();

    while let Some(block) = car_reader.next_block().await? {
        let compact: CompactBlock =
            carblock_to_compactblock_inplace(&block, &mut scratch, &mut provider, metadata_mode)?;

        optimized_cbor::encode_compact_block_to_vec(&compact, &mut encoded)?;
        write_varint(encoded.len(), &mut output).await?;
        output.write_all(&encoded).await?;
        progress.inc(1);
    }

    output.flush().await?;
    progress.finish_with_message("optimization complete");
    Ok(())
}

async fn write_varint(value: usize, writer: &mut BufWriter<File>) -> Result<()> {
    let mut v = value;
    let mut tmp = [0u8; 10];
    let mut i = 0;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            tmp[i] = byte;
            i += 1;
            break;
        } else {
            tmp[i] = byte | 0x80;
            i += 1;
        }
    }
    writer.write_all(&tmp[..i]).await?;
    Ok(())
}
