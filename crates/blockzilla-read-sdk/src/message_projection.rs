//! Bounded, allocation-light projection of Archive V2 hot messages.
//!
//! This module is the shared wire decoder for readers and indexers that need
//! message structure but do not need to own instruction payloads. Raw byte
//! payloads and instruction account lists are skipped or borrowed directly
//! from the input. The only variable allocation in a full projection is the
//! decoded static-account-key vector; signer-only projection uses inline
//! storage for the common case.
//!
//! A projector always carries one [`ArchiveV2WireProfile`]. Callers obtain it
//! from an admitted [`crate::ArchiveReader`] and use it for the whole
//! generation. There is no message-level fallback or format probing.

use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
    ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate,
    ArchiveV2VoteTowerSync, ArchiveV2WireRewriteLimits, ArchiveV2WireRewriteResult,
    ArchiveV2WireRewriteStats, ArchiveV2WireRewriteVisitor, CompactMessageHeader, CompactPubkey,
    OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash, WincodeLeb128Config,
    rewrite_archive_v2_hot_message_wire, rewrite_archive_v2_hot_message_wire_pre_unknown_fallbacks,
    wincode_leb128_config,
};
use smallvec::SmallVec;
use thiserror::Error;
use wincode::{ReadResult, SchemaRead, error::invalid_tag_encoding, io::Reader};

use crate::ArchiveV2WireProfile;

type Cfg = WincodeLeb128Config;

/// Every message account is addressed by a one-byte instruction index. This
/// includes static and address-table-loaded accounts together.
pub const MAX_MESSAGE_ACCOUNTS: usize = u8::MAX as usize + 1;

#[derive(Debug, Error)]
pub enum MessageProjectionError {
    #[error("message wire decode failed: {0}")]
    Decode(#[from] wincode::error::ReadError),
    #[error("message projection left {0} trailing bytes")]
    TrailingBytes(usize),
}

pub type MessageProjectionResult<T> = std::result::Result<T, MessageProjectionError>;

/// A generation-bound, allocation-light Archive V2 message projector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveV2MessageProjector {
    profile: ArchiveV2WireProfile,
}

impl ArchiveV2MessageProjector {
    pub const fn new(profile: ArchiveV2WireProfile) -> Self {
        Self { profile }
    }

    pub const fn wire_profile(self) -> ArchiveV2WireProfile {
        self.profile
    }

    /// Decode and validate one complete message while streaming top-level
    /// instructions to `on_instruction`.
    ///
    /// The callback can run before a later field proves malformed. A caller
    /// must not publish callback side effects unless this method returns `Ok`.
    pub fn project<'de>(
        self,
        bytes: &'de [u8],
        mut on_instruction: impl FnMut(BorrowedArchiveV2Instruction<'de>),
    ) -> MessageProjectionResult<ProjectedArchiveV2Message> {
        let mut cursor = bytes;
        // Select the generation grammar once per message. The hot instruction
        // loop is then monomorphized for that grammar and has no profile
        // branch for each instruction.
        let projected = match self.profile {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
                decode_message::<false>(&mut cursor, &mut on_instruction)?
            }
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
                decode_message::<true>(&mut cursor, &mut on_instruction)?
            }
        };
        if !cursor.is_empty() {
            return Err(MessageProjectionError::TrailingBytes(cursor.len()));
        }
        Ok(projected)
    }

    /// Decode only the version, header, and required signer-key prefix.
    ///
    /// This fast pass intentionally stops inside `account_keys`; it does not
    /// claim that the remaining message is valid. The prefix grammar is the
    /// same in both supported profiles, but requiring a projector keeps the
    /// call bound to one explicit generation profile.
    pub fn project_signers(self, bytes: &[u8]) -> MessageProjectionResult<SignerKeys> {
        let _profile_binding = self.profile;
        let mut cursor = bytes;
        decode_signers_prefix(&mut cursor).map_err(Into::into)
    }

    /// Decode one complete message into the canonical current owned type.
    ///
    /// Historical semantic variants map one-to-one into the current type.
    /// Pre-fallback bytes cannot produce `UnknownSystem` or `UnknownVote`.
    /// Exact input consumption is enforced for both profiles.
    pub fn decode_owned_message(
        self,
        bytes: &[u8],
    ) -> MessageProjectionResult<ArchiveV2HotMessagePayload> {
        match self.profile {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
                wincode::config::deserialize_exact(bytes, wincode_leb128_config())
                    .map_err(Into::into)
            }
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
                let historical: HistoricalMessagePayload =
                    wincode::config::deserialize_exact(bytes, wincode_leb128_config())?;
                Ok(historical.into())
            }
        }
    }

    /// Rewrite one complete message with this generation's selected wire grammar.
    ///
    /// The selected profile is authoritative. This method never retries with the other grammar,
    /// and both grammar implementations stream fields without building an owned message graph.
    pub fn rewrite_message_wire<V: ArchiveV2WireRewriteVisitor>(
        self,
        bytes: &[u8],
        output: &mut Vec<u8>,
        visitor: &mut V,
        limits: ArchiveV2WireRewriteLimits,
    ) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
        match self.profile {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
                rewrite_archive_v2_hot_message_wire(bytes, output, visitor, limits)
            }
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
                rewrite_archive_v2_hot_message_wire_pre_unknown_fallbacks(
                    bytes, output, visitor, limits,
                )
            }
        }
    }

    /// Validate the selected generation profile and compare it with the other
    /// supported profile without creating an owned message.
    ///
    /// The selected profile remains authoritative. An invalid selected decode
    /// is an error. An invalid alternate decode returns
    /// [`WireProfileAuditOutcome::SelectedOnly`]. If both profiles are valid,
    /// the frozen tag tables report whether their normalized instruction
    /// semantics are equivalent or divergent. Tag zero is Raw in both
    /// profiles. Every other tag that both profiles accept has a different
    /// meaning, so this path does not need to hash message bytes. The caller
    /// must apply the selection policy only after it audits the full
    /// generation. One message with divergent dual-valid semantics does not
    /// make the generation ambiguous when another message rejects the
    /// alternate grammar. This path does not allocate for a valid message and
    /// never serializes a decoded message.
    pub fn audit_alternate_profile(
        self,
        bytes: &[u8],
    ) -> MessageProjectionResult<WireProfileAuditOutcome> {
        self.audit_alternate_profile_with_program_oracle(bytes, |_, _| true)
    }

    /// Apply the same dual-profile audit and reject a structured instruction
    /// when its static program key does not match its semantic family.
    /// The predicate must give a stable answer for the same key and family;
    /// it can be called for both the selected and alternate interpretations.
    pub fn audit_alternate_profile_with_program_oracle(
        self,
        bytes: &[u8],
        mut program_is_valid: impl FnMut(CompactPubkey, ArchiveV2InstructionProgramSemantics) -> bool,
    ) -> MessageProjectionResult<WireProfileAuditOutcome> {
        let selected_scan = scan_profile(self.profile, bytes, &mut program_is_valid)?;
        if selected_scan.alternate_is_impossible {
            return Ok(WireProfileAuditOutcome::SelectedOnly);
        }
        if selected_scan.profile_neutral {
            return Ok(WireProfileAuditOutcome::BothSemanticallyEquivalent);
        }
        // Only an instruction whose tag remains valid and non-equivalent in
        // both profiles reaches this slow path. The selected message was
        // already validated above. Validate the alternate once; if it also
        // succeeds, at least one non-Raw tag has a different frozen meaning.
        let alternate_profile = match self.profile {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
            }
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
            }
        };
        if scan_profile(alternate_profile, bytes, &mut program_is_valid).is_err() {
            return Ok(WireProfileAuditOutcome::SelectedOnly);
        }
        Ok(WireProfileAuditOutcome::BothSemanticallyDivergent)
    }
}

/// Result of a generation-authoritative, borrowed dual-profile audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireProfileAuditOutcome {
    /// The selected profile decoded exactly and the alternate profile failed.
    SelectedOnly,
    /// Both profiles decoded exactly and normalized to the same semantics.
    BothSemanticallyEquivalent,
    /// Both profiles decoded exactly but normalized to different semantics.
    /// A full-generation audit can accept the selected profile only if the
    /// alternate profile fails at least one other message, or if independent
    /// producer provenance selects the profile.
    BothSemanticallyDivergent,
}

#[derive(wincode::SchemaRead)]
enum HistoricalMessagePayload {
    #[wincode(tag = 0)]
    Legacy(HistoricalLegacyMessage),
    #[wincode(tag = 1)]
    V0(HistoricalV0Message),
}

#[derive(wincode::SchemaRead)]
struct HistoricalLegacyMessage {
    header: CompactMessageHeader,
    account_keys: Vec<CompactPubkey>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: Vec<HistoricalInstruction>,
}

#[derive(wincode::SchemaRead)]
struct HistoricalV0Message {
    header: CompactMessageHeader,
    account_keys: Vec<CompactPubkey>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: Vec<HistoricalInstruction>,
    address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
}

#[derive(wincode::SchemaRead)]
struct HistoricalInstruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: HistoricalInstructionData,
}

/// Exact declaration order used by pre-fallback Archive V2 writers. Never
/// add or reorder variants in this compatibility type.
#[derive(wincode::SchemaRead)]
enum HistoricalInstructionData {
    #[wincode(tag = 0)]
    Raw(Vec<u8>),
    #[wincode(tag = 1)]
    ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    #[wincode(tag = 2)]
    System(ArchiveV2SystemInstructionData),
    #[wincode(tag = 3)]
    VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
    #[wincode(tag = 4)]
    VoteCompactUpdateVoteStateSwitch {
        update: ArchiveV2VoteStateUpdate,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
    #[wincode(tag = 5)]
    VoteTowerSync(ArchiveV2VoteTowerSync),
    #[wincode(tag = 6)]
    VoteTowerSyncSwitch {
        tower: ArchiveV2VoteTowerSync,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
}

impl From<HistoricalMessagePayload> for ArchiveV2HotMessagePayload {
    fn from(value: HistoricalMessagePayload) -> Self {
        match value {
            HistoricalMessagePayload::Legacy(message) => Self::Legacy(ArchiveV2HotLegacyMessage {
                header: message.header,
                account_keys: message.account_keys,
                recent_blockhash: message.recent_blockhash,
                instructions: message.instructions.into_iter().map(Into::into).collect(),
            }),
            HistoricalMessagePayload::V0(message) => Self::V0(ArchiveV2HotV0Message {
                header: message.header,
                account_keys: message.account_keys,
                recent_blockhash: message.recent_blockhash,
                instructions: message.instructions.into_iter().map(Into::into).collect(),
                address_table_lookups: message.address_table_lookups,
            }),
        }
    }
}

impl From<HistoricalInstruction> for ArchiveV2HotInstruction {
    fn from(value: HistoricalInstruction) -> Self {
        Self {
            program_id_index: value.program_id_index,
            accounts: value.accounts,
            data: value.data.into(),
        }
    }
}

impl From<HistoricalInstructionData> for ArchiveV2HotInstructionData {
    fn from(value: HistoricalInstructionData) -> Self {
        match value {
            HistoricalInstructionData::Raw(bytes) => Self::Raw(bytes),
            HistoricalInstructionData::ComputeBudget(data) => Self::ComputeBudget(data),
            HistoricalInstructionData::System(data) => Self::System(data),
            HistoricalInstructionData::VoteCompactUpdateVoteState(update) => {
                Self::VoteCompactUpdateVoteState(update)
            }
            HistoricalInstructionData::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            } => Self::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            },
            HistoricalInstructionData::VoteTowerSync(tower) => Self::VoteTowerSync(tower),
            HistoricalInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            } => Self::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            },
        }
    }
}

/// Most transactions have exactly one signer. Inline storage covers the
/// common case with no allocation.
pub type SignerKeys = SmallVec<[CompactPubkey; 2]>;

/// One top-level instruction. Its account-index slice borrows the message;
/// its data payload was validated and skipped without allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedArchiveV2Instruction<'de> {
    pub program_id_index: u8,
    pub accounts: &'de [u8],
    pub is_compact_vote: bool,
    pub program_semantics: ArchiveV2InstructionProgramSemantics,
}

/// Program family required by a structured Archive V2 instruction payload.
/// Raw payloads are profile-neutral and can belong to any program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2InstructionProgramSemantics {
    Raw,
    ComputeBudget,
    System,
    Vote,
}

/// Vote-registry column required by one compact vote-hash reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2VoteHashKind {
    Bank,
    BlockId,
}

/// One epoch-local vote-hash reference found while projecting a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveV2VoteHashReference {
    pub block_id: u32,
    pub kind: ArchiveV2VoteHashKind,
}

/// The structural fields needed by selective readers and indexers.
#[derive(Debug, Clone)]
pub struct ProjectedArchiveV2Message {
    pub account_keys: Vec<CompactPubkey>,
    pub address_table_keys: Vec<CompactPubkey>,
    pub recent_blockhash: OwnedCompactRecentBlockhash,
    pub vote_hash_references: Vec<ArchiveV2VoteHashReference>,
    pub is_v0: bool,
    pub num_required_signatures: u8,
    pub instruction_count: usize,
    pub has_compact_vote_instruction: bool,
    pub minimum_balance_accounts: usize,
    pub expected_loaded_writable: usize,
    pub expected_loaded_readonly: usize,
}

#[inline]
fn get<'de, T: SchemaRead<'de, Cfg>>(cursor: &mut &'de [u8]) -> ReadResult<T::Dst> {
    T::get(&mut *cursor)
}

#[inline]
fn read_len(cursor: &mut &[u8]) -> ReadResult<usize> {
    let len = get::<u64>(cursor)?;
    usize::try_from(len).map_err(|_| wincode::error::pointer_sized_decode_error())
}

#[inline]
fn read_bounded_len(cursor: &mut &[u8], maximum: usize, error: &'static str) -> ReadResult<usize> {
    let len = read_len(cursor)?;
    if len > maximum {
        return Err(wincode::error::invalid_value(error));
    }
    Ok(len)
}

#[inline]
fn read_len_bounded_by_remaining(cursor: &mut &[u8], error: &'static str) -> ReadResult<usize> {
    read_bounded_len(cursor, cursor.len(), error)
}

#[inline]
fn skip_bytes(cursor: &mut &[u8]) -> ReadResult<()> {
    let len = read_len_bounded_by_remaining(cursor, "byte string length exceeds remaining input")?;
    cursor.take_borrowed(len)?;
    Ok(())
}

#[inline]
fn skip_string(cursor: &mut &[u8]) -> ReadResult<()> {
    let len = read_len_bounded_by_remaining(cursor, "string length exceeds remaining input")?;
    let bytes = cursor.take_borrowed(len)?;
    std::str::from_utf8(bytes)
        .map_err(|_| wincode::error::invalid_value("string is not valid UTF-8"))?;
    Ok(())
}

fn skip_system_instruction_data(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 | 13 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        1 | 6 | 7 => {
            get::<[u8; 32]>(cursor)?;
        }
        2 | 5 | 8 => {
            get::<u64>(cursor)?;
        }
        3 => {
            get::<[u8; 32]>(cursor)?;
            skip_string(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        4 | 12 => {}
        9 => {
            get::<[u8; 32]>(cursor)?;
            skip_string(cursor)?;
            get::<u64>(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        10 => {
            get::<[u8; 32]>(cursor)?;
            skip_string(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        11 => {
            get::<u64>(cursor)?;
            skip_string(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn read_vote_hash_reference(
    cursor: &mut &[u8],
    kind: Option<ArchiveV2VoteHashKind>,
    on_reference: &mut impl FnMut(ArchiveV2VoteHashReference),
) -> ReadResult<()> {
    let value = get::<ArchiveV2VoteHashRef>(cursor)?;
    if let ArchiveV2VoteHashRef::Block(block_id) = value {
        let Some(kind) = kind else {
            return Err(wincode::error::invalid_value(
                "switch-proof vote hash cannot use an epoch-local block reference",
            ));
        };
        on_reference(ArchiveV2VoteHashReference { block_id, kind });
    }
    Ok(())
}

fn skip_vote_state_update(
    cursor: &mut &[u8],
    on_reference: &mut impl FnMut(ArchiveV2VoteHashReference),
) -> ReadResult<()> {
    get::<Option<u64>>(cursor)?;
    let maximum = cursor.len() / 2;
    let lockout_count = read_bounded_len(
        cursor,
        maximum,
        "vote lockout count exceeds remaining input",
    )?;
    for _ in 0..lockout_count {
        get::<u64>(cursor)?;
        get::<u8>(cursor)?;
    }
    read_vote_hash_reference(cursor, Some(ArchiveV2VoteHashKind::Bank), on_reference)?;
    get::<Option<i64>>(cursor)?;
    Ok(())
}

fn skip_vote_tower_sync(
    cursor: &mut &[u8],
    on_reference: &mut impl FnMut(ArchiveV2VoteHashReference),
) -> ReadResult<()> {
    skip_vote_state_update(cursor, on_reference)?;
    read_vote_hash_reference(cursor, Some(ArchiveV2VoteHashKind::BlockId), on_reference)?;
    Ok(())
}

fn skip_instruction_data<const PRE_UNKNOWN_FALLBACKS: bool>(
    cursor: &mut &[u8],
    on_reference: &mut impl FnMut(ArchiveV2VoteHashReference),
) -> ReadResult<(bool, ArchiveV2InstructionProgramSemantics)> {
    let tag = get::<u32>(cursor)?;
    let is_compact_vote;
    let program_semantics;
    if PRE_UNKNOWN_FALLBACKS {
        is_compact_vote = matches!(tag, 3..=6);
        program_semantics = match tag {
            0 => ArchiveV2InstructionProgramSemantics::Raw,
            1 => ArchiveV2InstructionProgramSemantics::ComputeBudget,
            2 => ArchiveV2InstructionProgramSemantics::System,
            3..=6 => ArchiveV2InstructionProgramSemantics::Vote,
            _ => ArchiveV2InstructionProgramSemantics::Raw,
        };
        match tag {
            0 => skip_bytes(cursor)?,
            1 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
            }
            2 => skip_system_instruction_data(cursor)?,
            3 => skip_vote_state_update(cursor, on_reference)?,
            4 => {
                skip_vote_state_update(cursor, on_reference)?;
                read_vote_hash_reference(cursor, None, on_reference)?;
            }
            5 => skip_vote_tower_sync(cursor, on_reference)?,
            6 => {
                skip_vote_tower_sync(cursor, on_reference)?;
                read_vote_hash_reference(cursor, None, on_reference)?;
            }
            other => return Err(invalid_tag_encoding(other as usize)),
        }
    } else {
        is_compact_vote = matches!(tag, 5..=8);
        program_semantics = match tag {
            0 => ArchiveV2InstructionProgramSemantics::Raw,
            1 | 4 => ArchiveV2InstructionProgramSemantics::System,
            2 | 5..=8 => ArchiveV2InstructionProgramSemantics::Vote,
            3 => ArchiveV2InstructionProgramSemantics::ComputeBudget,
            _ => ArchiveV2InstructionProgramSemantics::Raw,
        };
        match tag {
            0..=2 => skip_bytes(cursor)?,
            3 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
            }
            4 => skip_system_instruction_data(cursor)?,
            5 => skip_vote_state_update(cursor, on_reference)?,
            6 => {
                skip_vote_state_update(cursor, on_reference)?;
                read_vote_hash_reference(cursor, None, on_reference)?;
            }
            7 => skip_vote_tower_sync(cursor, on_reference)?,
            8 => {
                skip_vote_tower_sync(cursor, on_reference)?;
                read_vote_hash_reference(cursor, None, on_reference)?;
            }
            other => return Err(invalid_tag_encoding(other as usize)),
        }
    }
    Ok((is_compact_vote, program_semantics))
}

fn read_instruction<'de, const PRE_UNKNOWN_FALLBACKS: bool>(
    cursor: &mut &'de [u8],
    on_reference: &mut impl FnMut(ArchiveV2VoteHashReference),
) -> ReadResult<BorrowedArchiveV2Instruction<'de>> {
    let program_id_index = get::<u8>(cursor)?;
    let accounts_len = read_len_bounded_by_remaining(
        cursor,
        "instruction account-index count exceeds remaining input",
    )?;
    let accounts = cursor.take_borrowed(accounts_len)?;
    let (is_compact_vote, program_semantics) =
        skip_instruction_data::<PRE_UNKNOWN_FALLBACKS>(cursor, on_reference)?;
    Ok(BorrowedArchiveV2Instruction {
        program_id_index,
        accounts,
        is_compact_vote,
        program_semantics,
    })
}

fn decode_signers_prefix(cursor: &mut &[u8]) -> ReadResult<SignerKeys> {
    match get::<u32>(cursor)? {
        0 | 1 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    let header = get::<CompactMessageHeader>(cursor)?;
    let account_keys_len = read_len(cursor)?;
    if account_keys_len > MAX_MESSAGE_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "static account key count exceeds message account cap",
        ));
    }
    if usize::from(header.num_required_signatures) > account_keys_len {
        return Err(wincode::error::invalid_value(
            "required signature count exceeds account key count",
        ));
    }
    let mut signers = SignerKeys::new();
    for _ in 0..header.num_required_signatures {
        signers.push(get::<CompactPubkey>(cursor)?);
    }
    Ok(signers)
}

fn decode_message<'de, const PRE_UNKNOWN_FALLBACKS: bool>(
    cursor: &mut &'de [u8],
    on_instruction: &mut impl FnMut(BorrowedArchiveV2Instruction<'de>),
) -> ReadResult<ProjectedArchiveV2Message> {
    let is_v0 = match get::<u32>(cursor)? {
        0 => false,
        1 => true,
        other => return Err(invalid_tag_encoding(other as usize)),
    };
    let header = get::<CompactMessageHeader>(cursor)?;
    let account_key_count = read_bounded_len(
        cursor,
        MAX_MESSAGE_ACCOUNTS,
        "static account key count exceeds message account cap",
    )?;
    let mut account_keys = Vec::with_capacity(account_key_count);
    for _ in 0..account_key_count {
        account_keys.push(get::<CompactPubkey>(cursor)?);
    }
    let required = usize::from(header.num_required_signatures);
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    if required == 0
        || required > account_key_count
        || readonly_signed >= required
        || readonly_unsigned > account_key_count.saturating_sub(required)
    {
        return Err(wincode::error::invalid_value(
            "message header does not describe a writable fee payer and valid account partitions",
        ));
    }
    let recent_blockhash = get::<OwnedCompactRecentBlockhash>(cursor)?;

    let instruction_count = read_len_bounded_by_remaining(
        cursor,
        "top-level instruction count exceeds remaining input",
    )?;
    let mut maximum_instruction_account = None::<usize>;
    let mut has_compact_vote_instruction = false;
    let mut vote_hash_references = Vec::new();
    for _ in 0..instruction_count {
        let instruction = read_instruction::<PRE_UNKNOWN_FALLBACKS>(cursor, &mut |reference| {
            vote_hash_references.push(reference);
        })?;
        let program_index = usize::from(instruction.program_id_index);
        if program_index == 0 || program_index >= account_key_count {
            return Err(wincode::error::invalid_value(
                "instruction program ID index is not a non-payer static account",
            ));
        }
        for account in instruction.accounts {
            maximum_instruction_account = Some(
                maximum_instruction_account
                    .unwrap_or_default()
                    .max(usize::from(*account)),
            );
        }
        has_compact_vote_instruction |= instruction.is_compact_vote;
        on_instruction(instruction);
    }

    let mut expected_loaded_writable = 0usize;
    let mut expected_loaded_readonly = 0usize;
    let mut address_table_keys = Vec::new();
    if is_v0 {
        let lookup_count = read_bounded_len(
            cursor,
            MAX_MESSAGE_ACCOUNTS,
            "address-table lookup count exceeds message account cap",
        )?;
        address_table_keys.reserve(lookup_count);
        for _ in 0..lookup_count {
            let account_key = get::<CompactPubkey>(cursor)?;
            let writable = read_bounded_len(
                cursor,
                MAX_MESSAGE_ACCOUNTS,
                "writable address-table index count exceeds message account cap",
            )?;
            cursor.take_borrowed(writable)?;
            let readonly = read_bounded_len(
                cursor,
                MAX_MESSAGE_ACCOUNTS,
                "readonly address-table index count exceeds message account cap",
            )?;
            cursor.take_borrowed(readonly)?;

            if writable == 0 && readonly == 0 {
                return Err(wincode::error::invalid_value(
                    "address-table lookup has no writable or readonly indexes",
                ));
            }
            address_table_keys.push(account_key);

            expected_loaded_writable = expected_loaded_writable
                .checked_add(writable)
                .ok_or_else(|| wincode::error::invalid_value("loaded writable count overflow"))?;
            expected_loaded_readonly = expected_loaded_readonly
                .checked_add(readonly)
                .ok_or_else(|| wincode::error::invalid_value("loaded readonly count overflow"))?;
            let total_accounts = account_keys
                .len()
                .checked_add(expected_loaded_writable)
                .and_then(|count| count.checked_add(expected_loaded_readonly))
                .ok_or_else(|| wincode::error::invalid_value("message account count overflow"))?;
            if total_accounts > MAX_MESSAGE_ACCOUNTS {
                return Err(wincode::error::invalid_value(
                    "static and loaded account count exceeds message account cap",
                ));
            }
        }
    }

    let total_accounts = account_keys
        .len()
        .checked_add(expected_loaded_writable)
        .and_then(|count| count.checked_add(expected_loaded_readonly))
        .ok_or_else(|| wincode::error::invalid_value("message account count overflow"))?;
    if maximum_instruction_account.is_some_and(|index| index >= total_accounts) {
        return Err(wincode::error::invalid_value(
            "instruction account index is outside resolved message accounts",
        ));
    }
    let writable_signed = required - readonly_signed;
    let writable_unsigned_end = account_key_count - readonly_unsigned;
    let minimum_balance_accounts = if is_v0 {
        total_accounts
    } else if writable_unsigned_end > required {
        writable_unsigned_end
    } else {
        writable_signed
    };

    Ok(ProjectedArchiveV2Message {
        account_keys,
        address_table_keys,
        recent_blockhash,
        vote_hash_references,
        is_v0,
        num_required_signatures: header.num_required_signatures,
        instruction_count,
        has_compact_vote_instruction,
        minimum_balance_accounts,
        expected_loaded_writable,
        expected_loaded_readonly,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfileScan {
    /// At least one selected instruction cannot be valid in the alternate
    /// profile. This is proved either by an unsupported alternate tag or by
    /// the caller's program-ID predicate.
    alternate_is_impossible: bool,
    /// The message contains only Raw instruction-data tags. Raw has the same
    /// tag and payload in both profiles, so a second decode cannot add facts.
    profile_neutral: bool,
}

fn scan_profile(
    profile: ArchiveV2WireProfile,
    bytes: &[u8],
    program_is_valid: &mut impl FnMut(CompactPubkey, ArchiveV2InstructionProgramSemantics) -> bool,
) -> MessageProjectionResult<ProfileScan> {
    let mut cursor = bytes;
    let fingerprint = match profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            scan_message_profile::<false>(&mut cursor, program_is_valid)?
        }
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            scan_message_profile::<true>(&mut cursor, program_is_valid)?
        }
    };
    if !cursor.is_empty() {
        return Err(MessageProjectionError::TrailingBytes(cursor.len()));
    }
    Ok(fingerprint)
}

/// Parse one complete message without materializing its vectors. Only the
/// instruction-data tag table differs between supported profiles, but this
/// function validates the full common envelope too. It records only whether
/// the selected message is profile-neutral and whether the alternate profile
/// is impossible under the program-family oracle.
fn scan_message_profile<const PRE_UNKNOWN_FALLBACKS: bool>(
    cursor: &mut &[u8],
    program_is_valid: &mut impl FnMut(CompactPubkey, ArchiveV2InstructionProgramSemantics) -> bool,
) -> ReadResult<ProfileScan> {
    let is_v0 = match get::<u32>(cursor)? {
        0 => false,
        1 => true,
        other => return Err(invalid_tag_encoding(other as usize)),
    };
    let header = get::<CompactMessageHeader>(cursor)?;
    let account_key_count = read_bounded_len(
        cursor,
        MAX_MESSAGE_ACCOUNTS,
        "static account key count exceeds message account cap",
    )?;
    let required = usize::from(header.num_required_signatures);
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    if required == 0
        || required > account_key_count
        || readonly_signed >= required
        || readonly_unsigned > account_key_count.saturating_sub(required)
    {
        return Err(wincode::error::invalid_value(
            "message header does not describe a writable fee payer and valid account partitions",
        ));
    }
    // Only the initialized prefix can be addressed by a validated program
    // index. SmallVec leaves its inline storage uninitialized, so this avoids
    // clearing about 9 KiB for every message. The 256-key format cap prevents
    // a spill to the heap.
    let mut account_keys = SmallVec::<[CompactPubkey; MAX_MESSAGE_ACCOUNTS]>::new();
    for _ in 0..account_key_count {
        account_keys.push(get::<CompactPubkey>(cursor)?);
    }
    debug_assert!(!account_keys.spilled());
    get::<OwnedCompactRecentBlockhash>(cursor)?;

    let instruction_count = read_len_bounded_by_remaining(
        cursor,
        "top-level instruction count exceeds remaining input",
    )?;
    let mut maximum_instruction_account = None::<usize>;
    let mut alternate_is_impossible = false;
    // Selected and alternate instruction boundaries are identical until the
    // first non-Raw tag. After an unresolved profile-specific payload, a
    // later selected-profile byte is not known to be an alternate tag.
    let mut alternate_boundary_is_shared = true;
    let mut profile_neutral = true;
    for _ in 0..instruction_count {
        let program_id_index = get::<u8>(cursor)?;
        let program_index = usize::from(program_id_index);
        if program_index == 0 || program_index >= account_key_count {
            return Err(wincode::error::invalid_value(
                "instruction program ID index is not a non-payer static account",
            ));
        }
        let accounts_len = read_len_bounded_by_remaining(
            cursor,
            "instruction account-index count exceeds remaining input",
        )?;
        let accounts = cursor.take_borrowed(accounts_len)?;
        for account in accounts {
            maximum_instruction_account = Some(
                maximum_instruction_account
                    .unwrap_or_default()
                    .max(usize::from(*account)),
            );
        }
        let (semantic_tag, program_semantics, wire_tag) =
            scan_instruction_data::<PRE_UNKNOWN_FALLBACKS>(cursor)?;
        let program_key = account_keys[program_index];
        if !program_is_valid(program_key, program_semantics) {
            return Err(wincode::error::invalid_value(
                "structured instruction payload does not match its static program ID",
            ));
        }
        profile_neutral &= semantic_tag == 0;
        if alternate_boundary_is_shared && !alternate_is_impossible {
            match alternate_program_semantics::<PRE_UNKNOWN_FALLBACKS>(wire_tag) {
                None => alternate_is_impossible = true,
                Some(alternate_semantics)
                    if alternate_semantics != program_semantics
                        && !program_is_valid(program_key, alternate_semantics) =>
                {
                    alternate_is_impossible = true;
                }
                Some(_) => {}
            }
            if wire_tag != 0 && !alternate_is_impossible {
                alternate_boundary_is_shared = false;
            }
        }
    }

    let mut expected_loaded_writable = 0usize;
    let mut expected_loaded_readonly = 0usize;
    if is_v0 {
        let lookup_count = read_bounded_len(
            cursor,
            MAX_MESSAGE_ACCOUNTS,
            "address-table lookup count exceeds message account cap",
        )?;
        for _ in 0..lookup_count {
            get::<CompactPubkey>(cursor)?;
            let writable = read_bounded_len(
                cursor,
                MAX_MESSAGE_ACCOUNTS,
                "writable address-table index count exceeds message account cap",
            )?;
            cursor.take_borrowed(writable)?;
            let readonly = read_bounded_len(
                cursor,
                MAX_MESSAGE_ACCOUNTS,
                "readonly address-table index count exceeds message account cap",
            )?;
            cursor.take_borrowed(readonly)?;
            if writable == 0 && readonly == 0 {
                return Err(wincode::error::invalid_value(
                    "address-table lookup has no writable or readonly indexes",
                ));
            }
            expected_loaded_writable = expected_loaded_writable
                .checked_add(writable)
                .ok_or_else(|| wincode::error::invalid_value("loaded writable count overflow"))?;
            expected_loaded_readonly = expected_loaded_readonly
                .checked_add(readonly)
                .ok_or_else(|| wincode::error::invalid_value("loaded readonly count overflow"))?;
            let total_accounts = account_key_count
                .checked_add(expected_loaded_writable)
                .and_then(|count| count.checked_add(expected_loaded_readonly))
                .ok_or_else(|| wincode::error::invalid_value("message account count overflow"))?;
            if total_accounts > MAX_MESSAGE_ACCOUNTS {
                return Err(wincode::error::invalid_value(
                    "static and loaded account count exceeds message account cap",
                ));
            }
        }
    }
    let total_accounts = account_key_count
        .checked_add(expected_loaded_writable)
        .and_then(|count| count.checked_add(expected_loaded_readonly))
        .ok_or_else(|| wincode::error::invalid_value("message account count overflow"))?;
    if maximum_instruction_account.is_some_and(|index| index >= total_accounts) {
        return Err(wincode::error::invalid_value(
            "instruction account index is outside resolved message accounts",
        ));
    }
    Ok(ProfileScan {
        alternate_is_impossible,
        profile_neutral,
    })
}

fn alternate_program_semantics<const PRE_UNKNOWN_FALLBACKS: bool>(
    wire_tag: u32,
) -> Option<ArchiveV2InstructionProgramSemantics> {
    use ArchiveV2InstructionProgramSemantics::{ComputeBudget, Raw, System, Vote};

    if PRE_UNKNOWN_FALLBACKS {
        match wire_tag {
            0 => Some(Raw),
            1 | 4 => Some(System),
            2 | 5 | 6 => Some(Vote),
            3 => Some(ComputeBudget),
            _ => None,
        }
    } else {
        match wire_tag {
            0 => Some(Raw),
            1 => Some(ComputeBudget),
            2 => Some(System),
            3..=6 => Some(Vote),
            _ => None,
        }
    }
}

fn scan_instruction_data<const PRE_UNKNOWN_FALLBACKS: bool>(
    cursor: &mut &[u8],
) -> ReadResult<(u8, ArchiveV2InstructionProgramSemantics, u32)> {
    let wire_tag = get::<u32>(cursor)?;
    let Some(semantic_tag) = normalized_instruction_semantic_tag::<PRE_UNKNOWN_FALLBACKS>(wire_tag)
    else {
        return Err(invalid_tag_encoding(wire_tag as usize));
    };
    let mut ignore_reference = |_: ArchiveV2VoteHashReference| {};
    let program_semantics = if PRE_UNKNOWN_FALLBACKS {
        match wire_tag {
            0 => {
                skip_bytes(cursor)?;
                ArchiveV2InstructionProgramSemantics::Raw
            }
            1 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
                ArchiveV2InstructionProgramSemantics::ComputeBudget
            }
            2 => {
                skip_system_instruction_data(cursor)?;
                ArchiveV2InstructionProgramSemantics::System
            }
            3 => {
                skip_vote_state_update(cursor, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            4 => {
                skip_vote_state_update(cursor, &mut ignore_reference)?;
                read_vote_hash_reference(cursor, None, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            5 => {
                skip_vote_tower_sync(cursor, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            6 => {
                skip_vote_tower_sync(cursor, &mut ignore_reference)?;
                read_vote_hash_reference(cursor, None, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            _ => unreachable!("the normalized semantic-tag table rejected invalid tags"),
        }
    } else {
        match wire_tag {
            0 => {
                skip_bytes(cursor)?;
                ArchiveV2InstructionProgramSemantics::Raw
            }
            1 => {
                skip_bytes(cursor)?;
                ArchiveV2InstructionProgramSemantics::System
            }
            2 => {
                skip_bytes(cursor)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            3 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
                ArchiveV2InstructionProgramSemantics::ComputeBudget
            }
            4 => {
                skip_system_instruction_data(cursor)?;
                ArchiveV2InstructionProgramSemantics::System
            }
            5 => {
                skip_vote_state_update(cursor, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            6 => {
                skip_vote_state_update(cursor, &mut ignore_reference)?;
                read_vote_hash_reference(cursor, None, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            7 => {
                skip_vote_tower_sync(cursor, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            8 => {
                skip_vote_tower_sync(cursor, &mut ignore_reference)?;
                read_vote_hash_reference(cursor, None, &mut ignore_reference)?;
                ArchiveV2InstructionProgramSemantics::Vote
            }
            _ => unreachable!("the normalized semantic-tag table rejected invalid tags"),
        }
    };
    Ok((semantic_tag, program_semantics, wire_tag))
}

const fn normalized_instruction_semantic_tag<const PRE_UNKNOWN_FALLBACKS: bool>(
    wire_tag: u32,
) -> Option<u8> {
    if PRE_UNKNOWN_FALLBACKS {
        match wire_tag {
            0 => Some(0),
            1 => Some(3),
            2 => Some(4),
            3 => Some(5),
            4 => Some(6),
            5 => Some(7),
            6 => Some(8),
            _ => None,
        }
    } else if wire_tag <= 8 {
        Some(wire_tag as u8)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use blockzilla_format::{
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMessagePayload, ArchiveV2HotV0Message, ArchiveV2SystemInstructionData,
        ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, OwnedCompactAddressTableLookup,
        wincode_leb128_config,
    };
    use wincode::SchemaWrite;

    use super::*;

    fn serialize<T: SchemaWrite<Cfg, Src = T>>(value: &T) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn post_projector() -> ArchiveV2MessageProjector {
        ArchiveV2MessageProjector::new(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
    }

    fn pre_projector() -> ArchiveV2MessageProjector {
        ArchiveV2MessageProjector::new(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1)
    }

    #[test]
    fn current_legacy_and_v0_messages_match_the_canonical_writer() {
        let legacy = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                CompactPubkey::Id(10),
                CompactPubkey::Id(20),
                CompactPubkey::Id(30),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: vec![0],
                    data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3]),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: vec![1, 0],
                    data: ArchiveV2HotInstructionData::System(
                        ArchiveV2SystemInstructionData::Transfer { lamports: 9 },
                    ),
                },
            ],
        });
        let bytes = serialize(&legacy);
        let mut instructions = Vec::new();
        let projected = post_projector()
            .project(&bytes, |instruction| {
                instructions.push((instruction.program_id_index, instruction.accounts.to_vec()));
            })
            .unwrap();
        assert!(!projected.is_v0);
        assert_eq!(projected.num_required_signatures, 2);
        assert_eq!(projected.instruction_count, 2);
        assert_eq!(
            projected.account_keys,
            vec![
                CompactPubkey::Id(10),
                CompactPubkey::Id(20),
                CompactPubkey::Id(30),
            ]
        );
        assert_eq!(instructions, vec![(2, vec![0]), (2, vec![1, 0])]);
        assert_eq!(
            post_projector().project_signers(&bytes).unwrap().as_slice(),
            &[CompactPubkey::Id(10), CompactPubkey::Id(20)]
        );

        let v0 = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::UnknownVote(vec![9, 9]),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(99),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let projected = post_projector().project(&serialize(&v0), |_| {}).unwrap();
        assert!(projected.is_v0);
        assert_eq!(projected.expected_loaded_writable, 1);
        assert_eq!(projected.expected_loaded_readonly, 1);
    }

    #[derive(SchemaWrite)]
    enum HistoricalMessagePayload {
        Legacy(HistoricalLegacyMessage),
        V0(HistoricalV0Message),
    }

    #[derive(SchemaWrite)]
    struct HistoricalLegacyMessage {
        header: CompactMessageHeader,
        account_keys: Vec<CompactPubkey>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: Vec<HistoricalInstruction>,
    }

    #[derive(SchemaWrite)]
    struct HistoricalV0Message {
        header: CompactMessageHeader,
        account_keys: Vec<CompactPubkey>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: Vec<HistoricalInstruction>,
        address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
    }

    #[derive(SchemaWrite)]
    struct HistoricalInstruction {
        program_id_index: u8,
        accounts: Vec<u8>,
        data: HistoricalInstructionData,
    }

    /// Frozen declaration order used by pre-fallback Archive V2 writers.
    #[derive(SchemaWrite)]
    enum HistoricalInstructionData {
        Raw(Vec<u8>),
        ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
        System(ArchiveV2SystemInstructionData),
        VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
        VoteCompactUpdateVoteStateSwitch {
            update: ArchiveV2VoteStateUpdate,
            switch_proof_hash: ArchiveV2VoteHashRef,
        },
        VoteTowerSync(ArchiveV2VoteTowerSync),
        VoteTowerSyncSwitch {
            tower: ArchiveV2VoteTowerSync,
            switch_proof_hash: ArchiveV2VoteHashRef,
        },
    }

    fn historical_instruction(data: HistoricalInstructionData) -> HistoricalInstruction {
        HistoricalInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data,
        }
    }

    fn update() -> ArchiveV2VoteStateUpdate {
        ArchiveV2VoteStateUpdate {
            root: Some(1),
            lockout_offsets: vec![],
            hash: ArchiveV2VoteHashRef::Block(2),
            timestamp: Some(3),
        }
    }

    #[test]
    fn historical_profile_decodes_every_frozen_instruction_tag() {
        let tower = ArchiveV2VoteTowerSync {
            update: update(),
            block_id_hash: ArchiveV2VoteHashRef::Raw([7; 32]),
        };
        let message = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![
                historical_instruction(HistoricalInstructionData::Raw(vec![1, 2, 3])),
                historical_instruction(HistoricalInstructionData::ComputeBudget(
                    ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(1_000),
                )),
                historical_instruction(HistoricalInstructionData::System(
                    ArchiveV2SystemInstructionData::Transfer { lamports: 4 },
                )),
                historical_instruction(HistoricalInstructionData::VoteCompactUpdateVoteState(
                    update(),
                )),
                historical_instruction(
                    HistoricalInstructionData::VoteCompactUpdateVoteStateSwitch {
                        update: update(),
                        switch_proof_hash: ArchiveV2VoteHashRef::Zero,
                    },
                ),
                historical_instruction(HistoricalInstructionData::VoteTowerSync(tower.clone())),
                historical_instruction(HistoricalInstructionData::VoteTowerSyncSwitch {
                    tower,
                    switch_proof_hash: ArchiveV2VoteHashRef::Zero,
                }),
            ],
        });
        let bytes = serialize(&message);
        let mut instruction_count = 0;
        let projected = pre_projector()
            .project(&bytes, |_| instruction_count += 1)
            .unwrap();
        assert_eq!(instruction_count, 7);
        assert_eq!(projected.instruction_count, 7);
        assert_eq!(
            pre_projector().project_signers(&bytes).unwrap().as_slice(),
            &[CompactPubkey::Id(1)]
        );
        let owned = pre_projector().decode_owned_message(&bytes).unwrap();
        let ArchiveV2HotMessagePayload::Legacy(owned) = owned else {
            panic!("expected historical legacy message");
        };
        assert_eq!(owned.instructions.len(), 7);
        assert!(matches!(
            owned.instructions[2].data,
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports: 4
            })
        ));

        let wrong = post_projector().project(&bytes, |_| {}).unwrap_err();
        assert!(matches!(
            wrong,
            MessageProjectionError::Decode(_) | MessageProjectionError::TrailingBytes(_)
        ));
    }

    #[test]
    fn borrowed_dual_profile_audit_accepts_equivalent_raw_messages() {
        let message = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![historical_instruction(HistoricalInstructionData::Raw(
                vec![1, 2, 3],
            ))],
        });
        let bytes = serialize(&message);
        // Prime any test-harness thread-local state before counting only this
        // thread. The audit API promises no heap allocation for valid input.
        pre_projector().audit_alternate_profile(&bytes).unwrap();
        assert_eq!(
            post_projector().audit_alternate_profile(&bytes).unwrap(),
            WireProfileAuditOutcome::BothSemanticallyEquivalent
        );
        let (outcome, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut outcome = WireProfileAuditOutcome::SelectedOnly;
                for _ in 0..10_000 {
                    outcome = pre_projector().audit_alternate_profile(&bytes).unwrap();
                }
                outcome
            });
        assert_eq!(outcome, WireProfileAuditOutcome::BothSemanticallyEquivalent);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn frozen_non_raw_tags_never_have_the_same_profile_semantics() {
        assert_eq!(
            normalized_instruction_semantic_tag::<true>(0),
            normalized_instruction_semantic_tag::<false>(0)
        );
        for wire_tag in 1..=6 {
            assert_ne!(
                normalized_instruction_semantic_tag::<true>(wire_tag),
                normalized_instruction_semantic_tag::<false>(wire_tag)
            );
        }
        assert_eq!(normalized_instruction_semantic_tag::<true>(7), None);
        assert_eq!(normalized_instruction_semantic_tag::<true>(8), None);
        assert_eq!(normalized_instruction_semantic_tag::<false>(7), Some(7));
        assert_eq!(normalized_instruction_semantic_tag::<false>(8), Some(8));
    }

    #[test]
    fn audit_keeps_the_maximum_account_key_set_inline() {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: (1..=MAX_MESSAGE_ACCOUNTS)
                .map(|id| CompactPubkey::Id(id as u32))
                .collect(),
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: u8::MAX,
                accounts: vec![u8::MAX],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        let bytes = serialize(&message);
        post_projector().audit_alternate_profile(&bytes).unwrap();
        let (outcome, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                post_projector().audit_alternate_profile(&bytes).unwrap()
            });
        assert_eq!(outcome, WireProfileAuditOutcome::BothSemanticallyEquivalent);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn borrowed_dual_profile_audit_reports_dual_valid_different_semantics() {
        // Historical tag 1 plus payload tag 0 is ComputeBudget::Unused. The
        // same two bytes are current UnknownSystem with an empty byte vector.
        // Both exact parsers therefore succeed, but their meanings differ.
        let message = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![historical_instruction(
                HistoricalInstructionData::ComputeBudget(
                    ArchiveV2ComputeBudgetInstructionData::Unused,
                ),
            )],
        });
        let bytes = serialize(&message);
        assert!(pre_projector().project(&bytes, |_| {}).is_ok());
        assert!(post_projector().project(&bytes, |_| {}).is_ok());
        pre_projector().audit_alternate_profile(&bytes).unwrap();
        assert_eq!(
            pre_projector().audit_alternate_profile(&bytes).unwrap(),
            WireProfileAuditOutcome::BothSemanticallyDivergent
        );
        assert_eq!(
            post_projector().audit_alternate_profile(&bytes).unwrap(),
            WireProfileAuditOutcome::BothSemanticallyDivergent
        );
        assert_eq!(
            pre_projector()
                .audit_alternate_profile_with_program_oracle(&bytes, |program, semantics| {
                    program == CompactPubkey::Id(2)
                        && matches!(
                            semantics,
                            ArchiveV2InstructionProgramSemantics::Raw
                                | ArchiveV2InstructionProgramSemantics::ComputeBudget
                        )
                })
                .unwrap(),
            WireProfileAuditOutcome::SelectedOnly
        );
        assert_eq!(
            post_projector()
                .audit_alternate_profile_with_program_oracle(&bytes, |program, semantics| {
                    program == CompactPubkey::Id(2)
                        && matches!(
                            semantics,
                            ArchiveV2InstructionProgramSemantics::Raw
                                | ArchiveV2InstructionProgramSemantics::System
                        )
                })
                .unwrap(),
            WireProfileAuditOutcome::SelectedOnly
        );
        assert!(
            pre_projector()
                .audit_alternate_profile_with_program_oracle(&bytes, |_, semantics| {
                    semantics == ArchiveV2InstructionProgramSemantics::Raw
                })
                .is_err()
        );
        let (outcome, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut outcome = WireProfileAuditOutcome::SelectedOnly;
                for _ in 0..10_000 {
                    outcome = pre_projector().audit_alternate_profile(&bytes).unwrap();
                }
                outcome
            });
        assert_eq!(outcome, WireProfileAuditOutcome::BothSemanticallyDivergent);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn alternate_elimination_stops_after_profile_boundaries_diverge() {
        // Post sees [Vote tag 5, Vote tag 7, Raw]. Pre sees [Vote tag 5,
        // Raw, Vote tag 5]. The first tag 5 payload has a different length in
        // each grammar, so later Post tag boundaries cannot reject Pre.
        let bytes = decode_hex(
            "000100000201020000030100050000000001010107010001000100000001010100050000000000",
        );
        assert!(post_projector().project(&bytes, |_| {}).is_ok());
        assert!(pre_projector().project(&bytes, |_| {}).is_ok());
        let outcome = post_projector()
            .audit_alternate_profile_with_program_oracle(&bytes, |_, semantics| {
                matches!(
                    semantics,
                    ArchiveV2InstructionProgramSemantics::Raw
                        | ArchiveV2InstructionProgramSemantics::Vote
                )
            })
            .unwrap();
        assert_eq!(outcome, WireProfileAuditOutcome::BothSemanticallyDivergent);
    }

    #[test]
    fn borrowed_dual_profile_audit_requires_the_selected_profile() {
        let message = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![historical_instruction(HistoricalInstructionData::System(
                ArchiveV2SystemInstructionData::Transfer { lamports: 4 },
            ))],
        });
        let bytes = serialize(&message);
        assert_eq!(
            pre_projector().audit_alternate_profile(&bytes).unwrap(),
            WireProfileAuditOutcome::SelectedOnly
        );
        let (outcome, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                let mut outcome = WireProfileAuditOutcome::BothSemanticallyEquivalent;
                for _ in 0..10_000 {
                    outcome = pre_projector().audit_alternate_profile(&bytes).unwrap();
                }
                outcome
            });
        assert_eq!(outcome, WireProfileAuditOutcome::SelectedOnly);
        assert_eq!(allocations, 0);
        assert!(post_projector().audit_alternate_profile(&bytes).is_err());
    }

    #[test]
    fn historical_v0_shape_and_exact_real_message_are_supported() {
        let v0 = HistoricalMessagePayload::V0(HistoricalV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![historical_instruction(HistoricalInstructionData::Raw(
                vec![],
            ))],
            address_table_lookups: vec![],
        });
        assert!(
            pre_projector()
                .project(&serialize(&v0), |_| {})
                .unwrap()
                .is_v0
        );

        // Exact epoch-0 slot 105368 tx 2 bytes from the pinned historical
        // generation. Its first instruction uses old tag 2 (`System`).
        let bytes = decode_hex(
            "0002010206121813150e0d00c0e60c02040202000209ccf1736d29ad6e301871d2d5a34e01709272ebdc60b9b855a31b7c3036fae9360131c80106a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc0000000000503030201000c030000000080c6a47e8d0300",
        );
        let projected = pre_projector().project(&bytes, |_| {}).unwrap();
        assert!(!projected.is_v0);
        assert_eq!(projected.instruction_count, 2);
        let owned = pre_projector().decode_owned_message(&bytes).unwrap();
        let ArchiveV2HotMessagePayload::Legacy(owned) = owned else {
            panic!("expected historical legacy message");
        };
        assert!(matches!(
            owned.instructions[0].data,
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AllocateWithSeed {
                space: 200,
                ..
            })
        ));
        assert!(post_projector().project(&bytes, |_| {}).is_err());
    }

    #[test]
    fn exact_projection_rejects_trailing_bytes_and_hostile_lengths() {
        let payload = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
        });
        let mut trailing = serialize(&payload);
        trailing.push(0);
        assert!(matches!(
            post_projector().project(&trailing, |_| {}),
            Err(MessageProjectionError::TrailingBytes(1))
        ));

        let mut huge_accounts = Vec::new();
        huge_accounts.extend(serialize(&0u32));
        huge_accounts.extend(serialize(&CompactMessageHeader {
            num_required_signatures: 0,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        }));
        huge_accounts.extend(serialize(&u64::MAX));
        assert!(post_projector().project(&huge_accounts, |_| {}).is_err());

        let invalid_program = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        assert!(
            post_projector()
                .project(&serialize(&invalid_program), |_| {})
                .is_err()
        );

        let invalid_account = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![2],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        assert!(
            post_projector()
                .project(&serialize(&invalid_account), |_| {})
                .is_err()
        );

        let empty_lookup = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(3),
                writable_indexes: vec![],
                readonly_indexes: vec![],
            }],
        });
        assert!(
            post_projector()
                .project(&serialize(&empty_lookup), |_| {})
                .is_err()
        );
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid hex fixture"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }
}
