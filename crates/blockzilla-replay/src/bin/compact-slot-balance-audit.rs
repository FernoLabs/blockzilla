//! Compare every Compact V2 message in one slot with its archived balance metadata.

use std::{env, path::PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use blockzilla_format::{
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, CompactMetaV1,
    wincode_leb128_config,
};
use blockzilla_read_sdk::{ArchiveReader, HashVerification, LocalRangeSource, OpenOptions};
use blockzilla_replay::{CompactProbeConfig, probe_compact_generation};

fn main() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow!("usage: compact-slot-balance-audit COMPACT_GENERATION SLOT"))?,
    );
    let slot = args
        .next()
        .context("missing SLOT")?
        .to_string_lossy()
        .parse::<u64>()
        .context("parse SLOT")?;
    if args.next().is_some() {
        bail!("usage: compact-slot-balance-audit COMPACT_GENERATION SLOT");
    }

    let archive = ArchiveReader::open_with_options(
        LocalRangeSource::new(&root),
        OpenOptions {
            hash_verification: HashVerification::ControlFiles,
            ..OpenOptions::default()
        },
    )?;
    let row_number = archive.index().rows.partition_point(|row| row.slot < slot);
    let index_row = archive
        .index()
        .rows
        .get(row_number)
        .filter(|row| row.slot == slot)
        .ok_or_else(|| anyhow!("slot {slot} is not present"))?;
    let decoded = archive.read_block(row_number)?;
    let probe = probe_compact_generation(
        &root,
        CompactProbeConfig {
            start_slot: Some(slot),
            end_slot_exclusive: slot.checked_add(1),
            max_slots: 1,
            max_transactions: usize::try_from(index_row.tx_count).unwrap_or(usize::MAX),
        },
    )?;
    let transactions = &probe
        .slots
        .first()
        .context("probe omitted slot")?
        .transactions;

    println!("slot={slot} transactions={}", decoded.block.tx_rows.len());
    for tx_row in &decoded.block.tx_rows {
        let transaction = transactions
            .iter()
            .find(|transaction| transaction.tx_index == tx_row.tx_index)
            .with_context(|| format!("probe omitted transaction {}", tx_row.tx_index))?;
        println!(
            "tx={} flags=0x{:x} signatures={} accounts={} instructions={}",
            tx_row.tx_index,
            tx_row.flags,
            tx_row.signature_count,
            transaction.account_keys.len(),
            transaction.instructions.len(),
        );
        for instruction in &transaction.instructions {
            println!(
                "  instruction={} program={} account_indexes={:?} data={:?}",
                instruction.instruction_index,
                bs58::encode(instruction.program_id).into_string(),
                instruction.account_indexes,
                instruction.data,
            );
        }
        if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 || tx_row.metadata_len == 0 {
            println!("  metadata=absent");
            continue;
        }
        if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            println!("  metadata=raw-fallback");
            continue;
        }
        let start = usize::try_from(tx_row.metadata_offset)?;
        let end = start
            .checked_add(usize::try_from(tx_row.metadata_len)?)
            .context("metadata end overflow")?;
        let bytes = decoded
            .block
            .metadata_bytes
            .get(start..end)
            .context("metadata range")?;
        let metadata: CompactMetaV1 = wincode::config::deserialize(bytes, wincode_leb128_config())?;
        println!("  fee={} err={:?}", metadata.fee, metadata.err);
        for index in 0..metadata
            .pre_balances
            .len()
            .max(metadata.post_balances.len())
        {
            let pre = metadata.pre_balances.get(index).copied();
            let post = metadata.post_balances.get(index).copied();
            let delta = pre
                .zip(post)
                .map(|(pre, post)| i128::from(post) - i128::from(pre));
            if index == 0 || delta != Some(0) {
                let pubkey = transaction
                    .account_keys
                    .get(index)
                    .map(|key| bs58::encode(key).into_string())
                    .unwrap_or_else(|| "<loaded-or-missing>".to_owned());
                println!(
                    "  balance index={index} pubkey={pubkey} pre={pre:?} post={post:?} delta={delta:?}"
                );
            }
        }
    }
    Ok(())
}
