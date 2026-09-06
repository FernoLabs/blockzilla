use anyhow::{Context, Result, anyhow};
use clap::Parser;
use of_car_reader::{
    compact_index::decode_offset_and_size,
    slot_ranges::{SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, decode_slot_range_entry},
};
use of_slot_ranges::{AsyncCompactIndex, LocalFileRangeReader};
use std::{fs, path::PathBuf};

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long)]
    epoch: u64,
    #[arg(long)]
    indexes_dir: PathBuf,
    #[arg(long)]
    slot_index_dir: PathBuf,
    #[arg(long, required = true)]
    slot: Vec<u64>,
    #[arg(long, default_value_t = 3)]
    radius: u64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let epoch_start = cli
        .epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start overflow"))?;
    let epoch_end = epoch_start + SLOTS_PER_EPOCH - 1;

    let epoch_dir = cli.indexes_dir.join(cli.epoch.to_string());
    let epoch_cid_path = epoch_dir.join(format!("epoch-{}.cid", cli.epoch));
    let epoch_cid = fs::read_to_string(&epoch_cid_path)
        .with_context(|| format!("read {}", epoch_cid_path.display()))?;
    let epoch_cid = epoch_cid.trim();

    let slot_index_path = epoch_dir.join(format!(
        "epoch-{}-{epoch_cid}-mainnet-slot-to-cid.index",
        cli.epoch
    ));
    let cid_index_path = epoch_dir.join(format!(
        "epoch-{}-{epoch_cid}-mainnet-cid-to-offset-and-size.index",
        cli.epoch
    ));
    let raw_path = cli
        .slot_index_dir
        .join(format!("epoch-{}-slot-ranges.raw", cli.epoch));
    let raw = fs::read(&raw_path).with_context(|| format!("read {}", raw_path.display()))?;

    let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
        LocalFileRangeReader::open(&slot_index_path)?,
        slot_index_path.display().to_string(),
    ))?;
    let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(
        LocalFileRangeReader::open(&cid_index_path)?,
        cid_index_path.display().to_string(),
    ))?;

    println!(
        "target_slot\tslot\tslot_in_epoch\thas_slot_cid\tcid_hex\tcid_offset\tcid_size\tcid_end\traw_offset\traw_len\traw_end"
    );
    for target in cli.slot {
        let start = target.saturating_sub(cli.radius).max(epoch_start);
        let end = target.saturating_add(cli.radius).min(epoch_end);
        for slot in start..=end {
            let slot_in_epoch = usize::try_from(slot - epoch_start).context("slot-in-epoch")?;
            let raw_off = slot_in_epoch
                .checked_mul(SLOT_RANGE_ENTRY_SIZE)
                .ok_or_else(|| anyhow!("raw offset overflow"))?;
            let range = decode_slot_range_entry(&raw[raw_off..raw_off + SLOT_RANGE_ENTRY_SIZE])?;
            let raw_end = range.end_exclusive().unwrap_or(0);

            let mut cid = vec![0u8; slot_index.value_size()];
            let key = slot.to_le_bytes();
            let has_cid =
                futures::executor::block_on(slot_index.lookup_into_node_reads(&key, &mut cid))?;
            let (cid_hex, cid_offset, cid_size, cid_end) = if has_cid {
                let mut off_size = vec![0u8; cid_index.value_size()];
                if futures::executor::block_on(
                    cid_index.lookup_into_node_reads(&cid, &mut off_size),
                )? {
                    let (offset, size) = decode_offset_and_size(&off_size)?;
                    let end = offset + size as u64;
                    (
                        hex_encode(&cid),
                        offset.to_string(),
                        size.to_string(),
                        end.to_string(),
                    )
                } else {
                    (
                        hex_encode(&cid),
                        "missing".to_string(),
                        "missing".to_string(),
                        "missing".to_string(),
                    )
                }
            } else {
                (
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            };
            println!(
                "{target}\t{slot}\t{slot_in_epoch}\t{has_cid}\t{cid_hex}\t{cid_offset}\t{cid_size}\t{cid_end}\t{}\t{}\t{raw_end}",
                range.offset, range.len
            );
        }
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
