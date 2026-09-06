//! Full-generation proof for the selected Archive V2 message grammar.

use blockzilla_archive_v2::{
    ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
};
use blockzilla_primitives::CompactPubkey;

use crate::{
    ArchiveReader, ArchiveV2InstructionProgramSemantics, ArchiveV2MetadataProjectionLimits, Error,
    RangeSource, Result, WireProfileAuditOutcome,
};

/// Classification of every typed message in one generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FullGenerationWireProfileAudit {
    pub blocks: u64,
    pub typed_messages: u64,
    pub raw_transaction_fallbacks: u64,
    pub raw_metadata_fallbacks: u64,
    pub selected_only: u64,
    pub both_semantically_equivalent: u64,
    pub both_semantically_divergent: u64,
}

const METADATA_DERIVED_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_ERROR
    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
    | ARCHIVE_V2_TX_FLAG_HAS_LOGS
    | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
    | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;

fn validate_registry_pubkey(value: CompactPubkey, registry_entries: u32) -> Result<()> {
    if let CompactPubkey::Id(id) = value
        && (id == 0 || id > registry_entries)
    {
        return Err(Error::WireProfileAudit(format!(
            "pubkey registry ID {id} exceeds the admitted registry entry count {registry_entries}",
        )));
    }
    Ok(())
}

fn selected_semantic_rejection<S: RangeSource>(
    reader: &ArchiveReader<S>,
    slot: u64,
    tx_index: u32,
    message: impl Into<String>,
) -> Error {
    Error::SelectedWireProfileSemanticRejected {
        profile: reader.wire_profile(),
        slot,
        tx_index,
        message: message.into(),
    }
}

/// Proof available without trusting an external producer statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnprovenWireProfileDecision {
    /// The selected grammar decoded every typed message and the other grammar
    /// failed on at least one message.
    UniqueFullGenerationDecode,
    /// Both grammars gave the same normalized meaning for every typed message.
    AllSemanticallyEquivalent,
}

impl FullGenerationWireProfileAudit {
    /// Require the message bytes themselves to prove that the selected profile
    /// is safe. A dual-valid generation with different meanings needs separate
    /// immutable producer evidence and is rejected here.
    pub fn require_unproven_authority(self) -> Result<UnprovenWireProfileDecision> {
        let classified = self
            .selected_only
            .checked_add(self.both_semantically_equivalent)
            .and_then(|count| count.checked_add(self.both_semantically_divergent))
            .ok_or(Error::Overflow("wire-profile classified message count"))?;
        if classified != self.typed_messages {
            return Err(Error::WireProfileAudit(
                "not every typed message was classified".into(),
            ));
        }
        if self.selected_only != 0 {
            return Ok(UnprovenWireProfileDecision::UniqueFullGenerationDecode);
        }
        if self.both_semantically_divergent != 0 {
            return Err(Error::WireProfileAudit(format!(
                "both message grammars decode the full generation but differ semantically on {} messages",
                self.both_semantically_divergent
            )));
        }
        Ok(UnprovenWireProfileDecision::AllSemanticallyEquivalent)
    }
}

/// Decode and classify every typed hot message with bounded borrowed readers.
pub fn audit_full_generation_wire_profile<S: RangeSource>(
    reader: &ArchiveReader<S>,
    max_message_bytes: usize,
) -> Result<FullGenerationWireProfileAudit> {
    if max_message_bytes == 0 {
        return Err(Error::WireProfileAudit(
            "maximum message size must be positive".into(),
        ));
    }
    if !reader.metadata_footer().decode_errors.is_empty() {
        return Err(Error::WireProfileAudit(
            "generation footer reports decode errors".into(),
        ));
    }

    let projector = reader.message_projector();
    let system_program = solana_pubkey::pubkey!("11111111111111111111111111111111").to_bytes();
    let compute_budget_program =
        solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes();
    let vote_program =
        solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111").to_bytes();
    let known_programs =
        reader.compile_pubkey_filter([system_program, compute_budget_program, vote_program])?;
    let mut audit = FullGenerationWireProfileAudit::default();
    if let Some(genesis) = reader.genesis() {
        for account in genesis.accounts.iter().chain(&genesis.reward_pools) {
            validate_registry_pubkey(account.pubkey, reader.registry_entries())?;
            validate_registry_pubkey(account.owner, reader.registry_entries())?;
        }
        for builtin in &genesis.builtins {
            validate_registry_pubkey(builtin.pubkey, reader.registry_entries())?;
        }
    }
    let mut blocks = reader.borrowed_blocks();
    while let Some(block) = blocks.next_block() {
        let block = block?;
        audit.blocks = checked_add(audit.blocks, 1, "block count")?;
        let slot = block.header().slot;
        if let Some(rewards) = &block.header().rewards {
            for reward in &rewards.decoded {
                validate_registry_pubkey(reward.pubkey, reader.registry_entries()).map_err(
                    |source| {
                        Error::WireProfileAudit(format!(
                            "slot {slot} has an invalid block-reward pubkey: {source}",
                        ))
                    },
                )?;
            }
        }
        for row in block.tx_rows() {
            let has_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
            let raw_metadata = row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
            if raw_metadata {
                audit.raw_metadata_fallbacks = checked_add(
                    audit.raw_metadata_fallbacks,
                    1,
                    "raw metadata fallback count",
                )?;
            }
            if raw_metadata && !has_metadata {
                return Err(Error::WireProfileAudit(format!(
                    "slot {slot} transaction {} declares raw metadata without metadata",
                    row.tx_index
                )));
            }
            if (!has_metadata || raw_metadata) && row.flags & METADATA_DERIVED_FLAGS != 0 {
                return Err(Error::WireProfileAudit(format!(
                    "slot {slot} transaction {} declares typed metadata facts without typed metadata",
                    row.tx_index
                )));
            }
            if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
                if has_metadata && !raw_metadata {
                    return Err(Error::WireProfileAudit(format!(
                        "slot {slot} transaction {} combines an opaque transaction with typed metadata that cannot be validated against its message",
                        row.tx_index
                    )));
                }
                audit.raw_transaction_fallbacks = checked_add(
                    audit.raw_transaction_fallbacks,
                    1,
                    "raw transaction fallback count",
                )?;
                continue;
            }
            let length = row.message_len as usize;
            if length > max_message_bytes {
                return Err(Error::WireProfileAudit(format!(
                    "slot {slot} transaction {} message has {length} bytes, above the {max_message_bytes} byte limit",
                    row.tx_index
                )));
            }
            let start = row.message_offset as usize;
            let end = start
                .checked_add(length)
                .ok_or(Error::Overflow("message range"))?;
            let bytes = block.message_bytes().get(start..end).ok_or_else(|| {
                Error::WireProfileAudit(format!(
                    "slot {slot} transaction {} message range is outside its block",
                    row.tx_index
                ))
            })?;
            let outcome = projector
                .audit_alternate_profile_with_program_oracle(bytes, |program, semantics| {
                    match semantics {
                        ArchiveV2InstructionProgramSemantics::Raw => true,
                        ArchiveV2InstructionProgramSemantics::ComputeBudget => {
                            known_programs.matches_reference(program, &compute_budget_program)
                        }
                        ArchiveV2InstructionProgramSemantics::System => {
                            known_programs.matches_reference(program, &system_program)
                        }
                        ArchiveV2InstructionProgramSemantics::Vote => {
                            known_programs.matches_reference(program, &vote_program)
                        }
                    }
                })
                .map_err(|source| Error::SelectedWireProfileDecodeRejected {
                    profile: reader.wire_profile(),
                    slot,
                    tx_index: row.tx_index,
                    source,
                })?;
            let projected = projector.project(bytes, |_| {}).map_err(|source| {
                Error::SelectedWireProfileDecodeRejected {
                    profile: reader.wire_profile(),
                    slot,
                    tx_index: row.tx_index,
                    source,
                }
            })?;
            for pubkey in projected
                .account_keys
                .iter()
                .chain(&projected.address_table_keys)
            {
                validate_registry_pubkey(*pubkey, reader.registry_entries()).map_err(|source| {
                    selected_semantic_rejection(
                        reader,
                        slot,
                        row.tx_index,
                        format!("message pubkey reference is invalid: {source}"),
                    )
                })?;
            }
            let row_is_v0 = row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0;
            if projected.is_v0 != row_is_v0 {
                return Err(Error::WireProfileAudit(format!(
                    "slot {slot} transaction {} message version disagrees with its transaction-row flags",
                    row.tx_index
                )));
            }
            if projected.num_required_signatures != row.signature_count {
                return Err(Error::WireProfileAudit(format!(
                    "slot {slot} transaction {} message requires {} signatures but its transaction row declares {}",
                    row.tx_index, projected.num_required_signatures, row.signature_count
                )));
            }
            if projected.has_compact_vote_instruction
                != (row.flags & ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX != 0)
            {
                return Err(selected_semantic_rejection(
                    reader,
                    slot,
                    row.tx_index,
                    "compact-vote presence disagrees with its transaction-row flag",
                ));
            }
            if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0
                && row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0
            {
                let metadata_start = row.metadata_offset as usize;
                let metadata_end = metadata_start
                    .checked_add(row.metadata_len as usize)
                    .ok_or(Error::Overflow("metadata range"))?;
                let metadata_bytes = block
                    .metadata_bytes()
                    .get(metadata_start..metadata_end)
                    .ok_or_else(|| {
                        Error::WireProfileAudit(format!(
                            "slot {slot} transaction {} metadata range is outside its block",
                            row.tx_index
                        ))
                    })?;
                let total_message_accounts = projected
                    .account_keys
                    .len()
                    .checked_add(projected.expected_loaded_writable)
                    .and_then(|count| count.checked_add(projected.expected_loaded_readonly))
                    .ok_or(Error::Overflow("resolved message account count"))?;
                let metadata = reader
                    .validate_metadata_exact(
                        metadata_bytes,
                        ArchiveV2MetadataProjectionLimits {
                            total_message_accounts,
                            top_level_instruction_count: projected.instruction_count,
                        },
                    )
                    .map_err(|source| {
                        selected_semantic_rejection(
                            reader,
                            slot,
                            row.tx_index,
                            format!(
                                "typed metadata is invalid for the projected message: {source}"
                            ),
                        )
                    })?;
                if metadata.has_error != (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                    || metadata.inner_instructions_present
                        != (row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0)
                    || metadata.logs_present != Some(row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS != 0)
                    || metadata.token_balances_present
                        != Some(row.flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES != 0)
                    || metadata.return_data_present
                        != Some(row.flags & ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA != 0)
                {
                    return Err(selected_semantic_rejection(
                        reader,
                        slot,
                        row.tx_index,
                        "typed metadata disagrees with its transaction-row flags",
                    ));
                }
                if metadata.pre_balance_count != metadata.post_balance_count
                    || (metadata.pre_balance_count != 0
                        && metadata.pre_balance_count < projected.minimum_balance_accounts)
                {
                    return Err(selected_semantic_rejection(
                        reader,
                        slot,
                        row.tx_index,
                        "metadata balance vectors cannot cover the writable message-account prefix",
                    ));
                }
                let (loaded_writable, loaded_readonly) = metadata
                    .loaded_addresses
                    .expect("exact metadata validation always reads loaded addresses");
                let loaded_counts_match = loaded_writable.len()
                    == projected.expected_loaded_writable
                    && loaded_readonly.len() == projected.expected_loaded_readonly;
                let loaded_are_absent = loaded_writable.is_empty() && loaded_readonly.is_empty();
                let row_reports_loaded = row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0;
                if !loaded_counts_match || row_reports_loaded == loaded_are_absent {
                    return Err(selected_semantic_rejection(
                        reader,
                        slot,
                        row.tx_index,
                        "loaded-address values disagree with the transaction-row coverage flag or V0 lookup descriptors",
                    ));
                }
            }
            audit.typed_messages = checked_add(audit.typed_messages, 1, "typed message count")?;
            match outcome {
                WireProfileAuditOutcome::SelectedOnly => {
                    audit.selected_only =
                        checked_add(audit.selected_only, 1, "selected-only message count")?;
                }
                WireProfileAuditOutcome::BothSemanticallyEquivalent => {
                    audit.both_semantically_equivalent = checked_add(
                        audit.both_semantically_equivalent,
                        1,
                        "equivalent message count",
                    )?;
                }
                WireProfileAuditOutcome::BothSemanticallyDivergent => {
                    audit.both_semantically_divergent = checked_add(
                        audit.both_semantically_divergent,
                        1,
                        "divergent message count",
                    )?;
                }
            }
        }
    }
    if audit.blocks != reader.index().rows.len() as u64 {
        return Err(Error::WireProfileAudit(
            "audit did not visit every validated block".into(),
        ));
    }
    if audit.raw_transaction_fallbacks != reader.metadata_footer().tx_raw_fallbacks {
        return Err(Error::WireProfileAudit(
            "audited raw transaction fallback count differs from the generation footer".into(),
        ));
    }
    if audit.raw_metadata_fallbacks != reader.metadata_footer().metadata_raw_fallbacks {
        return Err(Error::WireProfileAudit(
            "audited raw metadata fallback count differs from the generation footer".into(),
        ));
    }
    Ok(audit)
}

fn checked_add(value: u64, amount: u64, label: &'static str) -> Result<u64> {
    value.checked_add(amount).ok_or(Error::Overflow(label))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unproven_selection_is_fail_closed() {
        assert_eq!(
            FullGenerationWireProfileAudit {
                typed_messages: 2,
                selected_only: 1,
                both_semantically_divergent: 1,
                ..FullGenerationWireProfileAudit::default()
            }
            .require_unproven_authority()
            .unwrap(),
            UnprovenWireProfileDecision::UniqueFullGenerationDecode
        );
        assert!(
            FullGenerationWireProfileAudit {
                typed_messages: 1,
                both_semantically_divergent: 1,
                ..FullGenerationWireProfileAudit::default()
            }
            .require_unproven_authority()
            .is_err()
        );
        assert_eq!(
            FullGenerationWireProfileAudit::default()
                .require_unproven_authority()
                .unwrap(),
            UnprovenWireProfileDecision::AllSemanticallyEquivalent
        );
    }
}
