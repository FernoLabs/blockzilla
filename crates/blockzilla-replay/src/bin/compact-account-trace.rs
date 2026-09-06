//! Stream Blockzilla Compact V2 account references and block rewards.
//!
//! This is a read-only diagnostic.  The normal replay visitor deliberately
//! discards block rewards, so reward inspection opens the same Compact V2
//! generation with the full borrowed-block decoder.

use std::{
    collections::{BTreeSet, VecDeque},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ArchiveV2SystemInstructionData};
use blockzilla_registry::KeyStore;
use blockzilla_read_sdk::{ArchiveReader, HashVerification, LocalRangeSource, OpenOptions};
use blockzilla_replay::{
    CompactArchivedTransactionOutcome, CompactInstructionData, CompactMessageVersion,
    CompactTransactionProbe, CompactVisitConfig, CompactVisitControl, CompactVisitEvent,
    visit_compact_generation,
};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "compact-account-trace",
    about = "Trace one account through Blockzilla Compact V2 transactions and block rewards"
)]
struct Cli {
    /// Base58 account pubkey to trace.
    #[arg(long)]
    account: String,
    /// Do not inspect slots at or after this bound.
    #[arg(long)]
    end_slot_exclusive: Option<u64>,
    /// Retain this many final matching transaction references.
    #[arg(long, default_value_t = 24)]
    tail_transactions: usize,
    /// Inspect rewards only, without decoding transaction messages.
    #[arg(long, conflicts_with = "transactions_only")]
    rewards_only: bool,
    /// Inspect transaction references only, without decoding block rewards.
    #[arg(long, conflicts_with = "rewards_only")]
    transactions_only: bool,
    /// Compact V2 generations in ledger order.
    #[arg(required = true)]
    generations: Vec<PathBuf>,
}

#[derive(Debug, Default)]
struct RewardSummary {
    slots: u64,
    rewards: u64,
    target_rewards: u64,
    target_lamports: i128,
    last_post_balance: Option<u64>,
}

#[derive(Debug, Default)]
struct TransactionSummary {
    transactions: u64,
    target_transactions: u64,
    target_fee_payer_transactions: u64,
    target_signer_transactions: u64,
    target_writable_transactions: u64,
    target_instruction_references: u64,
    target_system_lamport_events: u64,
    known_succeeded_system_delta: i128,
    known_failed_requested_system_delta: i128,
    unknown_requested_system_delta: i128,
    tail: VecDeque<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let target = parse_pubkey(&cli.account).context("parse --account")?;
    println!("input_format=blockzilla-compact-archive-v2");
    println!("trace_account={}", bs58::encode(target).into_string());

    if !cli.transactions_only {
        let mut rewards = RewardSummary::default();
        for generation in &cli.generations {
            scan_rewards(generation, target, cli.end_slot_exclusive, &mut rewards)?;
        }
        println!(
            "reward_summary slots={} rewards={} target_rewards={} target_lamports={} last_post_balance={}",
            rewards.slots,
            rewards.rewards,
            rewards.target_rewards,
            rewards.target_lamports,
            rewards
                .last_post_balance
                .map_or_else(|| "none".to_owned(), |balance| balance.to_string()),
        );
    }

    if cli.rewards_only {
        return Ok(());
    }

    let mut transactions = TransactionSummary::default();
    for generation in &cli.generations {
        scan_transactions(
            generation,
            target,
            cli.end_slot_exclusive,
            cli.tail_transactions,
            &mut transactions,
        )?;
    }
    println!(
        "transaction_summary transactions={} target_transactions={} fee_payer={} signer={} writable={} instruction_references={} system_lamport_events={} known_succeeded_system_delta={} known_failed_requested_system_delta={} unknown_requested_system_delta={}",
        transactions.transactions,
        transactions.target_transactions,
        transactions.target_fee_payer_transactions,
        transactions.target_signer_transactions,
        transactions.target_writable_transactions,
        transactions.target_instruction_references,
        transactions.target_system_lamport_events,
        transactions.known_succeeded_system_delta,
        transactions.known_failed_requested_system_delta,
        transactions.unknown_requested_system_delta,
    );
    for event in transactions.tail {
        println!("tail_transaction {event}");
    }
    Ok(())
}

fn scan_rewards(
    root: &Path,
    target: [u8; 32],
    end_slot_exclusive: Option<u64>,
    summary: &mut RewardSummary,
) -> Result<()> {
    let options = OpenOptions {
        hash_verification: HashVerification::ControlFiles,
        ..OpenOptions::default()
    };
    let archive = ArchiveReader::open_with_options(LocalRangeSource::new(root), options)
        .with_context(|| format!("open Compact V2 generation {}", root.display()))?;
    let keys_path = root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let keys = KeyStore::load(&keys_path)
        .with_context(|| format!("load pubkey registry {}", keys_path.display()))?;
    let rows = &archive.index().rows;
    let end = end_slot_exclusive.map_or(rows.len(), |slot| {
        rows.partition_point(|row| row.slot < slot)
    });
    let mut blocks = archive
        .borrowed_blocks_range(0..end)
        .with_context(|| format!("open reward-bearing blocks in {}", root.display()))?;
    let mut generation_slots = 0_u64;
    let mut generation_rewards = 0_u64;
    let mut generation_target_rewards = 0_u64;
    while let Some(block) = blocks.next_block() {
        let block = block.with_context(|| format!("decode rewards in {}", root.display()))?;
        let slot = block.header().slot;
        generation_slots = generation_slots
            .checked_add(1)
            .context("generation reward slot counter overflow")?;
        let Some(rewards) = &block.header().rewards else {
            continue;
        };
        generation_rewards = generation_rewards
            .checked_add(u64::try_from(rewards.decoded.len()).context("reward count exceeds u64")?)
            .context("generation reward counter overflow")?;
        for reward in &rewards.decoded {
            let pubkey = reward
                .pubkey
                .resolve(&keys)
                .with_context(|| format!("resolve reward pubkey at slot {slot}"))?;
            if pubkey != target {
                continue;
            }
            generation_target_rewards = generation_target_rewards
                .checked_add(1)
                .context("target reward counter overflow")?;
            summary.target_lamports = summary
                .target_lamports
                .checked_add(i128::from(reward.lamports))
                .context("target reward lamport sum overflow")?;
            summary.last_post_balance = Some(reward.post_balance);
            println!(
                "reward generation={} slot={} lamports={} post_balance={} reward_type={} reward_kind={} commission={}",
                root.display(),
                slot,
                reward.lamports,
                reward.post_balance,
                reward.reward_type,
                reward_kind(reward.reward_type),
                reward
                    .commission
                    .map_or_else(|| "none".to_owned(), |commission| commission.to_string()),
            );
        }
    }
    summary.slots = summary
        .slots
        .checked_add(generation_slots)
        .context("reward slot counter overflow")?;
    summary.rewards = summary
        .rewards
        .checked_add(generation_rewards)
        .context("reward counter overflow")?;
    summary.target_rewards = summary
        .target_rewards
        .checked_add(generation_target_rewards)
        .context("target reward counter overflow")?;
    println!(
        "reward_generation generation={} slots={} rewards={} target_rewards={}",
        root.display(),
        generation_slots,
        generation_rewards,
        generation_target_rewards,
    );
    Ok(())
}

fn scan_transactions(
    root: &Path,
    target: [u8; 32],
    end_slot_exclusive: Option<u64>,
    tail_limit: usize,
    summary: &mut TransactionSummary,
) -> Result<()> {
    let visit = visit_compact_generation(
        root,
        CompactVisitConfig {
            start_slot: None,
            end_slot_exclusive,
            max_slots: None,
        },
        |event| {
            let CompactVisitEvent::Slot { slot, .. } = event else {
                return Ok(CompactVisitControl::Continue);
            };
            for transaction in &slot.transactions {
                summary.transactions = summary.transactions.checked_add(1).ok_or_else(|| {
                    blockzilla_replay::CompactProbeError::Visitor(
                        "transaction counter overflow".to_owned(),
                    )
                })?;
                let Some(target_index) = transaction
                    .account_keys
                    .iter()
                    .position(|pubkey| *pubkey == target)
                else {
                    continue;
                };
                trace_transaction(
                    slot.slot,
                    transaction,
                    target,
                    target_index,
                    summary,
                    tail_limit,
                )
                .map_err(|error| {
                    blockzilla_replay::CompactProbeError::Visitor(error.to_string())
                })?;
            }
            Ok(CompactVisitControl::Continue)
        },
    )
    .with_context(|| format!("trace Compact transactions in {}", root.display()))?;
    println!(
        "transaction_generation generation={} slots={} transactions={} instructions={}",
        root.display(),
        visit.slots_visited,
        visit.transactions_visited,
        visit.instructions_visited,
    );
    Ok(())
}

fn trace_transaction(
    slot: u64,
    transaction: &CompactTransactionProbe,
    target: [u8; 32],
    target_index: usize,
    summary: &mut TransactionSummary,
    tail_limit: usize,
) -> Result<()> {
    summary.target_transactions = summary
        .target_transactions
        .checked_add(1)
        .context("target transaction counter overflow")?;
    let flags = account_flags(transaction)?;
    let (is_signer, is_writable) = flags[target_index];
    if target_index == 0 {
        summary.target_fee_payer_transactions = summary
            .target_fee_payer_transactions
            .checked_add(1)
            .context("fee-payer counter overflow")?;
    }
    if is_signer {
        summary.target_signer_transactions = summary
            .target_signer_transactions
            .checked_add(1)
            .context("signer counter overflow")?;
    }
    if is_writable {
        summary.target_writable_transactions = summary
            .target_writable_transactions
            .checked_add(1)
            .context("writable counter overflow")?;
    }

    let mut instruction_references = 0_u64;
    let mut programs = BTreeSet::new();
    for instruction in &transaction.instructions {
        let positions = instruction
            .account_indexes
            .iter()
            .enumerate()
            .filter_map(|(position, account_index)| {
                (usize::from(*account_index) == target_index).then_some(position)
            })
            .collect::<Vec<_>>();
        if positions.is_empty() {
            continue;
        }
        instruction_references = instruction_references
            .checked_add(1)
            .context("instruction reference counter overflow")?;
        programs.insert(bs58::encode(instruction.program_id).into_string());
        if let CompactInstructionData::System(system) = &instruction.data
            && let Some((delta, role, counterpart)) = system_lamport_delta(
                transaction,
                instruction.account_indexes.as_slice(),
                system,
                target,
            )?
        {
            summary.target_system_lamport_events = summary
                .target_system_lamport_events
                .checked_add(1)
                .context("system lamport event counter overflow")?;
            match transaction.archived_outcome {
                CompactArchivedTransactionOutcome::Succeeded => {
                    summary.known_succeeded_system_delta = summary
                        .known_succeeded_system_delta
                        .checked_add(delta)
                        .context("known-success delta overflow")?;
                }
                CompactArchivedTransactionOutcome::Failed => {
                    summary.known_failed_requested_system_delta = summary
                        .known_failed_requested_system_delta
                        .checked_add(delta)
                        .context("known-failed delta overflow")?;
                }
                CompactArchivedTransactionOutcome::Unknown => {
                    summary.unknown_requested_system_delta = summary
                        .unknown_requested_system_delta
                        .checked_add(delta)
                        .context("unknown delta overflow")?;
                }
            }
            println!(
                "system_lamport_event slot={} transaction={} instruction={} outcome={} role={} delta={} counterpart={} data={:?}",
                slot,
                transaction.tx_index,
                instruction.instruction_index,
                archived_outcome(transaction.archived_outcome),
                role,
                delta,
                counterpart
                    .map_or_else(|| "none".to_owned(), |key| bs58::encode(key).into_string()),
                system,
            );
        }
    }
    summary.target_instruction_references = summary
        .target_instruction_references
        .checked_add(instruction_references)
        .context("target instruction reference total overflow")?;

    if tail_limit != 0 {
        if summary.tail.len() == tail_limit {
            summary.tail.pop_front();
        }
        summary.tail.push_back(format!(
            "slot={slot} transaction={} outcome={} row_flags=0x{:x} version={} account_index={target_index} fee_payer={} signer={is_signer} writable={is_writable} instruction_references={instruction_references} programs={}",
            transaction.tx_index,
            archived_outcome(transaction.archived_outcome),
            transaction.row_flags,
            message_version(transaction.version),
            target_index == 0,
            programs.into_iter().collect::<Vec<_>>().join(","),
        ));
    }
    Ok(())
}

fn system_lamport_delta(
    transaction: &CompactTransactionProbe,
    indexes: &[u8],
    instruction: &ArchiveV2SystemInstructionData,
    target: [u8; 32],
) -> Result<Option<(i128, &'static str, Option<[u8; 32]>)>> {
    let key = |position: usize| -> Result<[u8; 32]> {
        let index = *indexes
            .get(position)
            .with_context(|| format!("system instruction omits account position {position}"))?;
        transaction
            .account_keys
            .get(usize::from(index))
            .copied()
            .with_context(|| format!("system account index {index} is unresolved"))
    };
    let transfer = |amount: u64, from_position: usize, to_position: usize| -> Result<_> {
        let from = key(from_position)?;
        let to = key(to_position)?;
        if target == from {
            Ok(Some((-i128::from(amount), "source", Some(to))))
        } else if target == to {
            Ok(Some((i128::from(amount), "destination", Some(from))))
        } else {
            Ok(None)
        }
    };
    match instruction {
        ArchiveV2SystemInstructionData::CreateAccount { lamports, .. }
        | ArchiveV2SystemInstructionData::CreateAccountWithSeed { lamports, .. }
        | ArchiveV2SystemInstructionData::CreateAccountAllowPrefund { lamports, .. } => {
            transfer(*lamports, 0, 1)
        }
        ArchiveV2SystemInstructionData::Transfer { lamports } => transfer(*lamports, 0, 1),
        ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports } => {
            transfer(*lamports, 0, 1)
        }
        ArchiveV2SystemInstructionData::TransferWithSeed { lamports, .. } => {
            transfer(*lamports, 0, 2)
        }
        ArchiveV2SystemInstructionData::Assign { .. }
        | ArchiveV2SystemInstructionData::AdvanceNonceAccount
        | ArchiveV2SystemInstructionData::InitializeNonceAccount { .. }
        | ArchiveV2SystemInstructionData::AuthorizeNonceAccount { .. }
        | ArchiveV2SystemInstructionData::Allocate { .. }
        | ArchiveV2SystemInstructionData::AllocateWithSeed { .. }
        | ArchiveV2SystemInstructionData::AssignWithSeed { .. }
        | ArchiveV2SystemInstructionData::UpgradeNonceAccount => Ok(None),
    }
}

fn account_flags(transaction: &CompactTransactionProbe) -> Result<Vec<(bool, bool)>> {
    ensure!(
        transaction.version == CompactMessageVersion::Legacy,
        "account trace currently expects a legacy message at tx {}",
        transaction.tx_index,
    );
    let key_count = transaction.account_keys.len();
    let required = usize::from(transaction.header.num_required_signatures);
    let readonly_signed = usize::from(transaction.header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(transaction.header.num_readonly_unsigned_accounts);
    ensure!(
        required <= key_count,
        "required signer count exceeds account keys"
    );
    ensure!(
        readonly_signed <= required,
        "readonly signed count exceeds required signer count"
    );
    ensure!(
        readonly_unsigned <= key_count - required,
        "readonly unsigned count exceeds unsigned account count"
    );
    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = key_count - readonly_unsigned;
    Ok((0..key_count)
        .map(|index| {
            let is_signer = index < required;
            let is_writable = if is_signer {
                index < writable_signed_end
            } else {
                index < writable_unsigned_end
            };
            (is_signer, is_writable)
        })
        .collect())
}

fn parse_pubkey(value: &str) -> Result<[u8; 32]> {
    let decoded = bs58::decode(value).into_vec().context("invalid base58")?;
    if decoded.len() != 32 {
        bail!("pubkey decodes to {} bytes instead of 32", decoded.len());
    }
    decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("pubkey length changed after validation"))
}

fn archived_outcome(value: CompactArchivedTransactionOutcome) -> &'static str {
    match value {
        CompactArchivedTransactionOutcome::Unknown => "unknown",
        CompactArchivedTransactionOutcome::Succeeded => "succeeded",
        CompactArchivedTransactionOutcome::Failed => "failed",
    }
}

fn message_version(value: CompactMessageVersion) -> &'static str {
    match value {
        CompactMessageVersion::Legacy => "legacy",
        CompactMessageVersion::V0 => "v0",
        CompactMessageVersion::V1 => "v1",
    }
}

fn reward_kind(value: i32) -> &'static str {
    match value {
        1 => "fee",
        2 => "rent",
        3 => "staking",
        4 => "voting",
        _ => "unspecified",
    }
}
