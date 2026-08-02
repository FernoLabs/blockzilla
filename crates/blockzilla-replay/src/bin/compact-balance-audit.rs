//! Inspect the balance oracle retained by one Compact V2 transaction.

use std::{env, path::PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use blockzilla_format::{
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, CompactMetaV1, KeyStore, wincode_leb128_config,
};
use blockzilla_read_sdk::{ArchiveReader, HashVerification, LocalRangeSource, OpenOptions};
use blockzilla_replay::{CompactProbeConfig, probe_compact_generation};

fn main() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let root =
        PathBuf::from(args.next().ok_or_else(|| {
            anyhow!("usage: compact-balance-audit COMPACT_GENERATION SLOT TX_INDEX")
        })?);
    let slot = args
        .next()
        .context("missing SLOT")?
        .to_string_lossy()
        .parse::<u64>()
        .context("parse SLOT")?;
    let tx_index = args
        .next()
        .context("missing TX_INDEX")?
        .to_string_lossy()
        .parse::<u32>()
        .context("parse TX_INDEX")?;
    if args.next().is_some() {
        bail!("usage: compact-balance-audit COMPACT_GENERATION SLOT TX_INDEX");
    }

    let options = OpenOptions {
        hash_verification: HashVerification::ControlFiles,
        ..OpenOptions::default()
    };
    let archive = ArchiveReader::open_with_options(LocalRangeSource::new(&root), options)
        .with_context(|| format!("open {}", root.display()))?;
    let row_number = archive.index().rows.partition_point(|row| row.slot < slot);
    let index_row = archive
        .index()
        .rows
        .get(row_number)
        .filter(|row| row.slot == slot)
        .ok_or_else(|| anyhow!("slot {slot} is not present"))?;
    let decoded = archive.read_block(row_number)?;
    let tx_row = decoded
        .block
        .tx_rows
        .iter()
        .find(|row| row.tx_index == tx_index)
        .ok_or_else(|| anyhow!("slot {slot} has no transaction {tx_index}"))?;
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 || tx_row.metadata_len == 0 {
        bail!("slot {slot} transaction {tx_index} has no metadata");
    }
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        bail!("slot {slot} transaction {tx_index} metadata is a raw fallback");
    }
    let start = usize::try_from(tx_row.metadata_offset).context("metadata offset")?;
    let end = start
        .checked_add(usize::try_from(tx_row.metadata_len).context("metadata length")?)
        .context("metadata end overflow")?;
    let bytes = decoded
        .block
        .metadata_bytes
        .get(start..end)
        .context("metadata range")?;
    let metadata: CompactMetaV1 = wincode::config::deserialize(bytes, wincode_leb128_config())
        .context("decode transaction metadata")?;

    let probe = probe_compact_generation(
        &root,
        CompactProbeConfig {
            start_slot: Some(slot),
            end_slot_exclusive: slot.checked_add(1),
            max_slots: 1,
            max_transactions: usize::try_from(index_row.tx_count).unwrap_or(usize::MAX),
        },
    )?;
    let transaction = probe
        .slots
        .first()
        .and_then(|block| block.transactions.iter().find(|tx| tx.tx_index == tx_index))
        .ok_or_else(|| anyhow!("replay probe did not retain transaction {tx_index}"))?;

    println!(
        "slot={slot} tx={tx_index} flags=0x{:x} fee={} err={:?} signatures={}",
        tx_row.flags, metadata.fee, metadata.err, tx_row.signature_count,
    );
    let account_count = transaction.account_keys.len();
    println!(
        "static_accounts={account_count} pre_balances={} post_balances={}",
        metadata.pre_balances.len(),
        metadata.post_balances.len(),
    );
    for instruction in &transaction.instructions {
        println!(
            "instruction={} program={} account_indexes={:?} data={:?}",
            instruction.instruction_index,
            bs58::encode(instruction.program_id).into_string(),
            instruction.account_indexes,
            instruction.data,
        );
    }
    for index in 0..metadata
        .pre_balances
        .len()
        .max(metadata.post_balances.len())
    {
        let pubkey = transaction
            .account_keys
            .get(index)
            .map(|key| bs58::encode(key).into_string())
            .unwrap_or_else(|| "<loaded-or-missing>".to_owned());
        let pre = metadata.pre_balances.get(index).copied();
        let post = metadata.post_balances.get(index).copied();
        let delta = pre
            .zip(post)
            .map(|(pre, post)| i128::from(post) - i128::from(pre));
        println!("account_index={index} pubkey={pubkey} pre={pre:?} post={post:?} delta={delta:?}");
    }

    let keys = KeyStore::load(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let rewards = decoded.block.header.rewards.as_ref();
    println!(
        "block_rewards_present={} block_rewards={}",
        rewards.is_some(),
        rewards.map_or(0, |rewards| rewards.decoded.len()),
    );
    if let Some(rewards) = rewards {
        for reward in &rewards.decoded {
            let pubkey = reward
                .pubkey
                .resolve(&keys)
                .map(|key| bs58::encode(key).into_string())
                .unwrap_or_else(|| "<invalid-registry-id>".to_owned());
            println!(
                "block_reward pubkey={pubkey} lamports={} post_balance={} reward_type={} commission={:?}",
                reward.lamports, reward.post_balance, reward.reward_type, reward.commission,
            );
        }
    }
    Ok(())
}
