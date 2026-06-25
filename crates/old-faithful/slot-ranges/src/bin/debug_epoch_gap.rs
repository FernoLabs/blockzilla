use anyhow::{Context, Result, anyhow};
use clap::Parser;
use of_car_reader::{
    compact_index::decode_offset_and_size,
    slot_ranges::{SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, decode_slot_range_entry},
};
use of_slot_ranges::{
    AsyncCompactIndex, BuildSlotRangesConfig, LocalFileRangeReader,
    build_block_slots_from_slot_index,
};
use std::{fs, path::PathBuf};

const MAX_BUCKET_SIZE: usize = 64 * 1024 * 1024;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long)]
    epoch: u64,
    #[arg(long)]
    indexes_dir: PathBuf,
    #[arg(long)]
    slot_index_dir: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
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
    let expected_raw_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;
    if raw.len() != expected_raw_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_raw_len}",
            raw_path.display(),
            raw.len()
        ));
    }
    let mut raw_nonempty = vec![false; SLOTS_PER_EPOCH as usize];
    for (i, chunk) in raw.chunks_exact(SLOT_RANGE_ENTRY_SIZE).enumerate() {
        raw_nonempty[i] = !decode_slot_range_entry(chunk)?.is_empty();
    }

    let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
        LocalFileRangeReader::open(&slot_index_path)?,
        slot_index_path.display().to_string(),
    ))?;
    let block_slots = futures::executor::block_on(build_block_slots_from_slot_index(
        cli.epoch,
        &mut slot_index,
        BuildSlotRangesConfig {
            max_bucket_payload_bytes: MAX_BUCKET_SIZE,
            allow_node_read_fallback: true,
        },
    ))?;

    let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
        LocalFileRangeReader::open(&slot_index_path)?,
        slot_index_path.display().to_string(),
    ))?;
    let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(
        LocalFileRangeReader::open(&cid_index_path)?,
        cid_index_path.display().to_string(),
    ))?;

    let epoch_start = cli
        .epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start overflow"))?;
    let mut suspect_count = 0usize;
    println!("epoch\tblock_index\tslot\tslot_in_epoch\tcid_hex\toffset\tsize\tend_exclusive");
    for (block_index, slot) in block_slots.block_slots.iter().copied().enumerate() {
        let slot_in_epoch = usize::try_from(slot - epoch_start).context("slot-in-epoch")?;
        if raw_nonempty[slot_in_epoch] {
            continue;
        }

        let mut cid = vec![0u8; slot_index.value_size()];
        let key = slot.to_le_bytes();
        if !futures::executor::block_on(slot_index.lookup_into_node_reads(&key, &mut cid))? {
            return Err(anyhow!("slot {slot} disappeared from slot-to-cid"));
        }
        let mut off_size = vec![0u8; cid_index.value_size()];
        let (offset, size, end) = if futures::executor::block_on(
            cid_index.lookup_into_node_reads(&cid, &mut off_size),
        )? {
            let (offset, size) = decode_offset_and_size(&off_size)?;
            let end = offset
                .checked_add(size as u64)
                .ok_or_else(|| anyhow!("offset+size overflow for slot {slot}"))?;
            (offset.to_string(), size.to_string(), end.to_string())
        } else {
            (
                "missing".to_string(),
                "missing".to_string(),
                "missing".to_string(),
            )
        };
        println!(
            "{}\t{block_index}\t{slot}\t{slot_in_epoch}\t{}\t{offset}\t{size}\t{end}",
            cli.epoch,
            hex_encode(&cid)
        );
        suspect_count += 1;
    }

    eprintln!(
        "summary epoch={} slot_to_cid_unique_blocks={} raw_nonempty={} suspect_empty_raw_blocks={}",
        cli.epoch,
        block_slots.block_slots.len(),
        raw_nonempty.iter().filter(|value| **value).count(),
        suspect_count
    );
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
