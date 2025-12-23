use ahash::AHashMap;
use anyhow::Result;
use car_reader::{car_block_reader::CarBlockReader, open_epoch::open_local_epoch};
use clap::{Parser, ValueEnum};
use compact_archive::{
    carblock_to_compact::{MetadataMode, PubkeyIdProvider, carblock_to_compactblock_inplace},
    carblock_to_compact::CompactBlock,
    io::{ArchiveFormat, write_block},
};
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ArchiveFormatArg {
    Wincode,
    Postcard,
    Cbor,
}

impl From<ArchiveFormatArg> for ArchiveFormat {
    fn from(value: ArchiveFormatArg) -> Self {
        match value {
            ArchiveFormatArg::Wincode => ArchiveFormat::Wincode,
            ArchiveFormatArg::Postcard => ArchiveFormat::Postcard,
            ArchiveFormatArg::Cbor => ArchiveFormat::Cbor,
        }
    }
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

#[derive(Parser, Debug)]
#[command(name = "optimize-car-archive", about = "Convert CAR data into a compact archive")]
struct Args {
    /// Epoch number whose CAR file should be read.
    #[arg(long)]
    epoch: u64,

    /// Directory containing pre-downloaded CAR files.
    #[arg(long, default_value = "cache")]
    cache_dir: PathBuf,

    /// Path to the compact archive output file.
    #[arg(long)]
    output: PathBuf,

    /// Output format for the compact archive payloads.
    #[arg(long, value_enum, default_value_t = ArchiveFormatArg::Wincode)]
    format: ArchiveFormatArg,

    /// How transaction metadata should be stored in the compact archive.
    #[arg(long, value_enum, default_value_t = MetadataArg::Compact)]
    metadata: MetadataArg,
}

#[derive(Default)]
struct DynamicPubkeyResolver {
    map: AHashMap<u128, u32>,
}

impl DynamicPubkeyResolver {
    fn id_for(&mut self, key: &[u8; 32]) -> u32 {
        let fp = fp128_from_bytes(key);
        if let Some(id) = self.map.get(&fp) {
            *id
        } else {
            let id = self.map.len() as u32;
            self.map.insert(fp, id);
            id
        }
    }
}

impl PubkeyIdProvider for DynamicPubkeyResolver {
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32> {
        Some(self.id_for(key))
    }
}

fn fp128_from_bytes(b: &[u8; 32]) -> u128 {
    let a = u128::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ]);
    let c = u128::from_le_bytes([
        b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23], b[24], b[25], b[26], b[27], b[28],
        b[29], b[30], b[31],
    ]);
    a ^ c.rotate_left(64)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let source = open_local_epoch(args.epoch, &args.cache_dir).await?;
    let mut reader = CarBlockReader::new(source);
    reader.read_header().await?;

    let mut resolver = DynamicPubkeyResolver::default();
    let mut tx_buf = Vec::new();
    let mut meta_buf = Vec::new();
    let mut rewards_buf = Vec::new();
    let mut compact_block = CompactBlock {
        slot: 0,
        txs: Vec::new(),
        rewards: Vec::new(),
    };

    let output_file = File::create(&args.output)?;
    let mut writer = BufWriter::new(output_file);
    let format = args.format.into();
    let metadata_mode = args.metadata.into();

    while let Some(block) = reader.next_block().await? {
        carblock_to_compactblock_inplace(
            &block,
            &mut resolver,
            metadata_mode,
            &mut tx_buf,
            &mut meta_buf,
            &mut rewards_buf,
            &mut compact_block,
        )?;

        write_block(&mut writer, &compact_block, format)?;
    }

    writer.flush()?;
    Ok(())
}
