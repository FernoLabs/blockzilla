//! Stream Compact V2 generations and summarize native instruction families.
//!
//! This diagnostic intentionally reads no signatures or transaction metadata.

use std::{collections::BTreeMap, env, path::PathBuf};

use anyhow::{Context, Result, bail};
use blockzilla_format::{ArchiveV2ComputeBudgetInstructionData, ArchiveV2SystemInstructionData};
use blockzilla_replay::{
    CONFIG_PROGRAM_ID, CompactArchivedTransactionOutcome, CompactInstructionData,
    CompactVisitConfig, CompactVisitControl, CompactVisitEvent, STAKE_PROGRAM_ID,
    SYSTEM_PROGRAM_ID, VOTE_PROGRAM_ID, visit_compact_generation,
};

#[derive(Debug, Clone, Copy)]
struct FirstSeen {
    slot: u64,
    transaction: u32,
    instruction: u32,
    prefix: [u8; 16],
    prefix_len: u8,
}

#[derive(Debug, Default)]
struct Entry {
    count: u64,
    first: Option<FirstSeen>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirstArchivedOutcome {
    slot: u64,
    transaction: u32,
    outcome: CompactArchivedTransactionOutcome,
    row_flags: u32,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ArchivedOutcomeSummary {
    unknown: u64,
    succeeded: u64,
    failed: u64,
    first_known: Option<FirstArchivedOutcome>,
}

impl ArchivedOutcomeSummary {
    fn observe(
        &mut self,
        slot: u64,
        transaction: u32,
        outcome: CompactArchivedTransactionOutcome,
        row_flags: u32,
    ) {
        match outcome {
            CompactArchivedTransactionOutcome::Unknown => self.unknown += 1,
            CompactArchivedTransactionOutcome::Succeeded => self.succeeded += 1,
            CompactArchivedTransactionOutcome::Failed => self.failed += 1,
        }
        if outcome != CompactArchivedTransactionOutcome::Unknown
            && self
                .first_known
                .is_none_or(|first| (slot, transaction) < (first.slot, first.transaction))
        {
            self.first_known = Some(FirstArchivedOutcome {
                slot,
                transaction,
                outcome,
                row_flags,
            });
        }
    }

    fn merge(&mut self, other: &Self) {
        self.unknown += other.unknown;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        if let Some(first) = other.first_known
            && self.first_known.is_none_or(|current| {
                (first.slot, first.transaction) < (current.slot, current.transaction)
            })
        {
            self.first_known = Some(first);
        }
    }
}

fn main() -> Result<()> {
    let mut outcomes_only = false;
    let mut roots = Vec::new();
    for argument in env::args_os().skip(1) {
        if argument == "--outcomes-only" {
            outcomes_only = true;
        } else {
            roots.push(PathBuf::from(argument));
        }
    }
    if roots.is_empty() {
        bail!(
            "usage: compact-native-audit [--outcomes-only] COMPACT_GENERATION [COMPACT_GENERATION ...]"
        );
    }

    let mut counts = BTreeMap::<String, Entry>::new();
    let mut all_outcomes = ArchivedOutcomeSummary::default();
    for root in roots {
        let mut generation_outcomes = ArchivedOutcomeSummary::default();
        let summary = visit_compact_generation(
            &root,
            CompactVisitConfig {
                start_slot: None,
                end_slot_exclusive: None,
                max_slots: None,
            },
            |event| {
                let CompactVisitEvent::Slot { slot, .. } = event else {
                    return Ok(CompactVisitControl::Continue);
                };
                for transaction in &slot.transactions {
                    generation_outcomes.observe(
                        slot.slot,
                        transaction.tx_index,
                        transaction.archived_outcome,
                        transaction.row_flags,
                    );
                    if outcomes_only {
                        continue;
                    }
                    for instruction in &transaction.instructions {
                        let (family, raw) =
                            classify_instruction(instruction.program_id, &instruction.data);
                        let entry = counts.entry(family).or_default();
                        entry.count = entry.count.saturating_add(1);
                        if entry.first.is_none() {
                            let mut prefix = [0_u8; 16];
                            let prefix_len = raw.map_or(0, |bytes| bytes.len().min(prefix.len()));
                            if let Some(bytes) = raw {
                                prefix[..prefix_len].copy_from_slice(&bytes[..prefix_len]);
                            }
                            entry.first = Some(FirstSeen {
                                slot: slot.slot,
                                transaction: transaction.tx_index,
                                instruction: instruction.instruction_index,
                                prefix,
                                prefix_len: prefix_len as u8,
                            });
                        }
                    }
                }
                Ok(CompactVisitControl::Continue)
            },
        )
        .with_context(|| format!("audit Compact generation {}", root.display()))?;
        print_outcome_summary(
            &format!("generation:{}", root.display()),
            &generation_outcomes,
        );
        all_outcomes.merge(&generation_outcomes);
        println!(
            "generation={} slots={} transactions={} instructions={}",
            root.display(),
            summary.slots_visited,
            summary.transactions_visited,
            summary.instructions_visited,
        );
    }

    print_outcome_summary("all", &all_outcomes);

    if !outcomes_only {
        for (family, entry) in counts {
            let first = entry.first.expect("counted entry has a first location");
            println!(
                "family={family} count={} first_slot={} first_tx={} first_ix={} prefix={}",
                entry.count,
                first.slot,
                first.transaction,
                first.instruction,
                hex(&first.prefix[..usize::from(first.prefix_len)]),
            );
        }
    }
    Ok(())
}

fn print_outcome_summary(scope: &str, summary: &ArchivedOutcomeSummary) {
    if let Some(first) = summary.first_known {
        println!(
            "outcomes scope={scope} unknown={} succeeded={} failed={} first_known_slot={} first_known_tx={} first_known_outcome={} first_known_row_flags=0x{:x}",
            summary.unknown,
            summary.succeeded,
            summary.failed,
            first.slot,
            first.transaction,
            archived_outcome_name(first.outcome),
            first.row_flags,
        );
    } else {
        println!(
            "outcomes scope={scope} unknown={} succeeded={} failed={} first_known=none",
            summary.unknown, summary.succeeded, summary.failed,
        );
    }
}

fn archived_outcome_name(outcome: CompactArchivedTransactionOutcome) -> &'static str {
    match outcome {
        CompactArchivedTransactionOutcome::Unknown => "unknown",
        CompactArchivedTransactionOutcome::Succeeded => "succeeded",
        CompactArchivedTransactionOutcome::Failed => "failed",
    }
}

fn classify_instruction(
    program_id: [u8; 32],
    data: &CompactInstructionData,
) -> (String, Option<&[u8]>) {
    let program = if program_id == VOTE_PROGRAM_ID {
        "vote"
    } else if program_id == SYSTEM_PROGRAM_ID {
        "system"
    } else if program_id == STAKE_PROGRAM_ID {
        "stake"
    } else if program_id == CONFIG_PROGRAM_ID {
        "config"
    } else {
        "other"
    };

    let (kind, raw) = match data {
        CompactInstructionData::Raw(bytes) => (raw_kind(bytes), Some(bytes.as_slice())),
        CompactInstructionData::UnknownSystem(bytes) => (
            format!("unknown-system/{}", raw_kind(bytes)),
            Some(bytes.as_slice()),
        ),
        CompactInstructionData::UnknownVote(bytes) => (
            format!("unknown-vote/{}", raw_kind(bytes)),
            Some(bytes.as_slice()),
        ),
        CompactInstructionData::ComputeBudget(value) => (
            match value {
                ArchiveV2ComputeBudgetInstructionData::Unused => "compute/unused",
                ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(_) => {
                    "compute/request-heap-frame"
                }
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(_) => {
                    "compute/set-unit-limit"
                }
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(_) => {
                    "compute/set-unit-price"
                }
                ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(_) => {
                    "compute/set-loaded-account-data-limit"
                }
            }
            .to_owned(),
            None,
        ),
        CompactInstructionData::System(value) => (
            match value {
                ArchiveV2SystemInstructionData::CreateAccount { .. } => "system/create-account",
                ArchiveV2SystemInstructionData::Assign { .. } => "system/assign",
                ArchiveV2SystemInstructionData::Transfer { .. } => "system/transfer",
                ArchiveV2SystemInstructionData::CreateAccountWithSeed { .. } => {
                    "system/create-account-with-seed"
                }
                ArchiveV2SystemInstructionData::AdvanceNonceAccount => {
                    "system/advance-nonce-account"
                }
                ArchiveV2SystemInstructionData::WithdrawNonceAccount { .. } => {
                    "system/withdraw-nonce-account"
                }
                ArchiveV2SystemInstructionData::InitializeNonceAccount { .. } => {
                    "system/initialize-nonce-account"
                }
                ArchiveV2SystemInstructionData::AuthorizeNonceAccount { .. } => {
                    "system/authorize-nonce-account"
                }
                ArchiveV2SystemInstructionData::Allocate { .. } => "system/allocate",
                ArchiveV2SystemInstructionData::AllocateWithSeed { .. } => {
                    "system/allocate-with-seed"
                }
                ArchiveV2SystemInstructionData::AssignWithSeed { .. } => "system/assign-with-seed",
                ArchiveV2SystemInstructionData::TransferWithSeed { .. } => {
                    "system/transfer-with-seed"
                }
                ArchiveV2SystemInstructionData::UpgradeNonceAccount => {
                    "system/upgrade-nonce-account"
                }
                ArchiveV2SystemInstructionData::CreateAccountAllowPrefund { .. } => {
                    "system/create-account-allow-prefund"
                }
            }
            .to_owned(),
            None,
        ),
        CompactInstructionData::VoteCompactUpdateVoteState(_) => {
            ("vote/compact-update-vote-state".to_owned(), None)
        }
        CompactInstructionData::VoteCompactUpdateVoteStateSwitch { .. } => {
            ("vote/compact-update-vote-state-switch".to_owned(), None)
        }
        CompactInstructionData::VoteTowerSync(_) => ("vote/tower-sync".to_owned(), None),
        CompactInstructionData::VoteTowerSyncSwitch { .. } => {
            ("vote/tower-sync-switch".to_owned(), None)
        }
    };
    (format!("{program}/{kind}"), raw)
}

fn raw_kind(bytes: &[u8]) -> String {
    bytes
        .get(..4)
        .map(|prefix| {
            let value = u32::from_le_bytes(prefix.try_into().expect("four-byte prefix"));
            format!("raw-u32-{value}")
        })
        .unwrap_or_else(|| format!("raw-short-{}", bytes.len()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[usize::from(byte >> 4)] as char);
        output.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archived_outcomes_count_and_retain_the_earliest_known_coordinate() {
        let mut summary = ArchivedOutcomeSummary::default();
        summary.observe(100, 3, CompactArchivedTransactionOutcome::Unknown, 0);
        summary.observe(101, 8, CompactArchivedTransactionOutcome::Failed, 3);
        summary.observe(101, 2, CompactArchivedTransactionOutcome::Succeeded, 1);
        summary.observe(99, 9, CompactArchivedTransactionOutcome::Succeeded, 9);

        assert_eq!(summary.unknown, 1);
        assert_eq!(summary.succeeded, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(
            summary.first_known,
            Some(FirstArchivedOutcome {
                slot: 99,
                transaction: 9,
                outcome: CompactArchivedTransactionOutcome::Succeeded,
                row_flags: 9,
            })
        );
    }

    #[test]
    fn archived_outcome_merge_preserves_all_unknown_and_global_boundary() {
        let mut earlier = ArchivedOutcomeSummary::default();
        earlier.observe(41, 7, CompactArchivedTransactionOutcome::Unknown, 0);

        let mut later = ArchivedOutcomeSummary::default();
        later.observe(50, 4, CompactArchivedTransactionOutcome::Failed, 3);
        later.observe(51, 0, CompactArchivedTransactionOutcome::Succeeded, 1);

        later.merge(&earlier);
        assert_eq!(later.unknown, 1);
        assert_eq!(later.succeeded, 1);
        assert_eq!(later.failed, 1);
        assert_eq!(
            later
                .first_known
                .map(|first| (first.slot, first.transaction)),
            Some((50, 4))
        );
    }
}
