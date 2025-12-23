use anyhow::{Context, Result};
use async_compression::tokio::bufread::ZstdDecoder;
use car_reader::car_block_reader::CarBlockReader;
use clap::{Parser, ValueEnum};
use compact_archive::{archive::CompactArchive, format::CompactBlock};
use convert::{MetadataMode, PubkeyIdProvider, carblock_to_compactblock_inplace};
use std::path::PathBuf;

mod compact_log;
mod convert;

#[derive(Parser, Debug)]
#[command(author, version, about = "Convert CAR files into compact archives")]
struct Cli {
    /// Path to the zstd-compressed CAR file
    #[arg(short, long)]
    input: PathBuf,

    /// Path to write the compact archive (postcard encoded)
    #[arg(short, long)]
    output: PathBuf,

    /// How to handle transaction metadata in the compact archive
    #[arg(long, value_enum, default_value_t = MetadataFlag::Compact)]
    metadata: MetadataFlag,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MetadataFlag {
    Compact,
    Raw,
    None,
}

impl From<MetadataFlag> for MetadataMode {
    fn from(value: MetadataFlag) -> Self {
        match value {
            MetadataFlag::Compact => MetadataMode::Compact,
            MetadataFlag::Raw => MetadataMode::Raw,
            MetadataFlag::None => MetadataMode::None,
        }
    }
}

#[derive(Default)]
struct DynamicPubkeyIdProvider {
    fp_to_id: ahash::AHashMap<u128, u32>,
    ids: Vec<[u8; 32]>,
}

impl DynamicPubkeyIdProvider {
    fn new() -> Self {
        Self {
            fp_to_id: ahash::AHashMap::with_capacity(1 << 17),
            ids: Vec::with_capacity(1 << 17),
        }
    }

    fn into_keys(self) -> Vec<[u8; 32]> {
        self.ids
    }
}

impl PubkeyIdProvider for DynamicPubkeyIdProvider {
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32> {
        let fp = fp128_from_bytes(key);
        if let Some(id) = self.fp_to_id.get(&fp) {
            return Some(*id);
        }

        if self.ids.len() >= u32::MAX as usize {
            return None;
        }

        let next_id = self.ids.len() as u32;
        self.fp_to_id.insert(fp, next_id);
        self.ids.push(*key);
        Some(next_id)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let file = tokio::fs::File::open(&cli.input)
        .await
        .with_context(|| format!("open CAR file at {}", cli.input.display()))?;
    let buf = tokio::io::BufReader::new(file);
    let decoder = ZstdDecoder::new(buf);

    let mut reader = CarBlockReader::new(decoder);
    reader.read_header().await.context("read CAR header")?;

    let mut buf_tx = Vec::new();
    let mut buf_meta = Vec::new();
    let mut buf_rewards = Vec::new();
    let mut provider = DynamicPubkeyIdProvider::new();
    let mut out_block = CompactBlock {
        slot: 0,
        txs: Vec::new(),
        rewards: Vec::new(),
    };
    let mut blocks = Vec::new();

    while let Some(car_block) = reader.next_block().await.context("read CAR block")? {
        carblock_to_compactblock_inplace(
            &car_block,
            &mut provider,
            cli.metadata.into(),
            &mut buf_tx,
            &mut buf_meta,
            &mut buf_rewards,
            &mut out_block,
        )
        .with_context(|| format!("convert slot {}", out_block.slot))?;

        blocks.push(out_block.clone());
    }

    let archive = CompactArchive::new(provider.into_keys(), blocks);
    archive
        .write_to_path(&cli.output)
        .with_context(|| format!("write archive to {}", cli.output.display()))?;

    println!(
        "wrote compact archive with {} blocks to {}",
        archive.blocks.len(),
        cli.output.display()
    );

    Ok(())
}

#[inline(always)]
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
