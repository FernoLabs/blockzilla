//! Bounded semantic projection of Compact V2 hot messages.
//!
//! This module decodes only the message facts needed by query adapters and
//! exact signed-message reconstruction. It does not deserialize the complete
//! Wincode message object graph. The selected generation schema is fixed for
//! the full call. The decoder does not probe or retry another schema.

use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstructionData,
    ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef, ArchiveV2VoteLockoutOffset,
    ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, CompactMessageHeader, CompactPubkey,
    CompactTransactionConfig, OwnedCompactRecentBlockhash, WincodeLeb128Config,
};
use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use wincode::{ReadResult, SchemaRead, error::invalid_tag_encoding, io::Reader};

use crate::{
    CompactV2MessageSchema, InstructionDataCandidate, InstructionDataEncoding,
    MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS, SignedMessageError, SignedTransactionConfig,
    VoteHashResolver, reconstruct_instruction_data_candidates,
};

type Cfg = WincodeLeb128Config;

/// All message account indexes are one byte wide.
pub const MAX_COMPACT_V2_MESSAGE_ACCOUNTS: usize = u8::MAX as usize + 1;

const MAX_SOLANA_SHORT_VEC_ITEMS: usize = u16::MAX as usize;
const MAX_V1_STATIC_ACCOUNTS: usize = 64;
const MAX_V1_INSTRUCTIONS: usize = 64;
const MAX_SELECTED_INSTRUCTION_DATA_PROGRAMS: usize = MAX_COMPACT_V2_MESSAGE_ACCOUNTS;

// These bounds include the fixed canonical SystemInstruction bytes around the
// string. They guarantee that ownership and exact-candidate construction happen
// only after the complete instruction is known to fit the signed u16 data bound.
const MAX_CREATE_ACCOUNT_WITH_SEED_BYTES: usize = MAX_SOLANA_SHORT_VEC_ITEMS - 92;
const MAX_ALLOCATE_WITH_SEED_BYTES: usize = MAX_SOLANA_SHORT_VEC_ITEMS - 84;
const MAX_ASSIGN_WITH_SEED_BYTES: usize = MAX_SOLANA_SHORT_VEC_ITEMS - 76;
const MAX_TRANSFER_WITH_SEED_BYTES: usize = MAX_SOLANA_SHORT_VEC_ITEMS - 52;

#[derive(Debug, Error)]
pub enum CompactV2MessageProjectionError {
    #[error("Compact V2 message wire decode failed: {0}")]
    Decode(#[from] wincode::error::ReadError),

    #[error("Compact V2 message has {0} trailing bytes")]
    TrailingBytes(usize),

    #[error("cannot reconstruct exact Compact V2 instruction data: {0}")]
    ExactInstructionData(#[from] SignedMessageError),

    #[error(
        "Compact V2 message instruction candidates exceed the {MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS}-combination limit"
    )]
    CandidateCombinationLimit,

    #[error(
        "Compact V2 message data selection has {actual} program references, above the {MAX_SELECTED_INSTRUCTION_DATA_PROGRAMS}-reference limit"
    )]
    SelectedProgramLimit { actual: usize },

    #[error(
        "Compact V2 message data selection uses registry ID {id}, outside 1..={registry_entries}"
    )]
    InvalidSelectedProgramRegistryId { id: u32, registry_entries: u32 },
}

pub type CompactV2MessageProjectionResult<T> =
    std::result::Result<T, CompactV2MessageProjectionError>;

/// A generation-bound Compact V2 message projector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2MessageProjector {
    schema: CompactV2MessageSchema,
    registry_entries: u32,
}

impl CompactV2MessageProjector {
    /// Validate the message using borrowed bytes and retain only count geometry.
    pub fn count_message(
        self,
        bytes: &[u8],
    ) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'_>> {
        self.project_with_policy(bytes, None, InstructionDataPolicy::Counts)
    }
    /// Bind one projector to the schema and public-key registry admitted by an
    /// [`crate::ArchiveReader`].
    pub const fn new(schema: CompactV2MessageSchema, registry_entries: u32) -> Self {
        Self {
            schema,
            registry_entries,
        }
    }

    pub const fn schema(self) -> CompactV2MessageSchema {
        self.schema
    }

    pub const fn registry_entries(self) -> u32 {
        self.registry_entries
    }

    /// Decode and validate one complete message.
    ///
    /// Raw instruction account lists and V0 lookup index lists borrow from
    /// `bytes`. Static keys, projected rows, requested data candidates, and
    /// temporary structured-data values use explicit protocol bounds.
    ///
    /// A vote-hash resolver is required only when a compact vote instruction
    /// refers to `vote_hash_registry.bin`. TowerSync can return two candidates.
    /// A caller must use transaction signatures to select one candidate. It
    /// must not select a candidate by preference.
    pub fn project<'de>(
        self,
        bytes: &'de [u8],
        vote_hashes: Option<&dyn VoteHashResolver>,
    ) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'de>> {
        self.project_with_policy(
            bytes,
            vote_hashes,
            InstructionDataPolicy::All { relaxed: false },
        )
    }

    /// Decode a complete message and retain clean missing vote proof as an
    /// empty selected candidate set.
    ///
    /// Malformed wire data and malformed supplied sidecars still fail. This
    /// method is for an adapter that publishes explicit non-exact instruction
    /// data coverage when its request permits that result.
    pub fn project_relaxed<'de>(
        self,
        bytes: &'de [u8],
        vote_hashes: Option<&dyn VoteHashResolver>,
    ) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'de>> {
        self.project_with_policy(
            bytes,
            vote_hashes,
            InstructionDataPolicy::All { relaxed: true },
        )
    }

    /// Decode one complete message, but reconstruct instruction data only for
    /// the selected compact program references.
    ///
    /// Non-selected instructions still receive full bounds-checked wire
    /// traversal. They do not read vote-hash sidecars, produce signed-message
    /// candidates, or count towards the ambiguity limit. Their
    /// [`ProjectedCompactV2Instruction::data_candidates`] result is `None`.
    ///
    /// Compact references are generation-local. To select one logical program
    /// that can occur both inline and through `registry.bin`, the caller must
    /// include both its `Raw` reference and its admitted `Id` reference.
    pub fn project_with_instruction_data_for_programs<'de>(
        self,
        bytes: &'de [u8],
        programs: &[CompactPubkey],
        vote_hashes: Option<&dyn VoteHashResolver>,
    ) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'de>> {
        if programs.len() > MAX_SELECTED_INSTRUCTION_DATA_PROGRAMS {
            return Err(CompactV2MessageProjectionError::SelectedProgramLimit {
                actual: programs.len(),
            });
        }
        for program in programs {
            if let CompactPubkey::Id(id) = program
                && (*id == 0 || *id > self.registry_entries)
            {
                return Err(
                    CompactV2MessageProjectionError::InvalidSelectedProgramRegistryId {
                        id: *id,
                        registry_entries: self.registry_entries,
                    },
                );
            }
        }
        self.project_with_policy(
            bytes,
            vote_hashes,
            InstructionDataPolicy::Programs {
                programs,
                relaxed: false,
            },
        )
    }

    /// Apply selected-program projection and retain clean missing vote proof
    /// as an empty selected candidate set.
    pub fn project_with_instruction_data_for_programs_relaxed<'de>(
        self,
        bytes: &'de [u8],
        programs: &[CompactPubkey],
        vote_hashes: Option<&dyn VoteHashResolver>,
    ) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'de>> {
        if programs.len() > MAX_SELECTED_INSTRUCTION_DATA_PROGRAMS {
            return Err(CompactV2MessageProjectionError::SelectedProgramLimit {
                actual: programs.len(),
            });
        }
        for program in programs {
            if let CompactPubkey::Id(id) = program
                && (*id == 0 || *id > self.registry_entries)
            {
                return Err(
                    CompactV2MessageProjectionError::InvalidSelectedProgramRegistryId {
                        id: *id,
                        registry_entries: self.registry_entries,
                    },
                );
            }
        }
        self.project_with_policy(
            bytes,
            vote_hashes,
            InstructionDataPolicy::Programs {
                programs,
                relaxed: true,
            },
        )
    }

    fn project_with_policy<'de>(
        self,
        bytes: &'de [u8],
        vote_hashes: Option<&dyn VoteHashResolver>,
        data_policy: InstructionDataPolicy<'_>,
    ) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'de>> {
        let mut cursor = bytes;
        let projected = match self.schema {
            CompactV2MessageSchema::Current => decode_message::<false>(
                &mut cursor,
                self.registry_entries,
                vote_hashes,
                data_policy,
            )?,
            CompactV2MessageSchema::May24PreUnknownFallbacks => decode_message::<true>(
                &mut cursor,
                self.registry_entries,
                vote_hashes,
                data_policy,
            )?,
        };
        if !cursor.is_empty() {
            return Err(CompactV2MessageProjectionError::TrailingBytes(cursor.len()));
        }
        Ok(projected)
    }
}

/// The message version and its version-specific signed fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectedCompactV2MessageVersion<'de> {
    Legacy,
    V0 {
        address_table_lookups: Vec<ProjectedCompactV2AddressTableLookup<'de>>,
    },
    V1 {
        config: SignedTransactionConfig,
    },
}

/// One V0 lookup descriptor. The table key is a registry reference. The two
/// index lists borrow their exact Compact V2 payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCompactV2AddressTableLookup<'de> {
    account_key: CompactPubkey,
    writable_indexes: &'de [u8],
    readonly_indexes: &'de [u8],
}

impl<'de> ProjectedCompactV2AddressTableLookup<'de> {
    pub const fn account_key(&self) -> CompactPubkey {
        self.account_key
    }

    pub const fn writable_indexes(&self) -> &'de [u8] {
        self.writable_indexes
    }

    pub const fn readonly_indexes(&self) -> &'de [u8] {
        self.readonly_indexes
    }
}

/// One top-level instruction with all exact on-chain data candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCompactV2Instruction<'de> {
    program_id_index: u8,
    accounts: &'de [u8],
    data_candidates: Option<SmallVec<[InstructionDataCandidate<'de>; 1]>>,
}

impl<'de> ProjectedCompactV2Instruction<'de> {
    pub const fn program_id_index(&self) -> u8 {
        self.program_id_index
    }

    pub const fn accounts(&self) -> &'de [u8] {
        self.accounts
    }

    /// Return all possible exact on-chain data forms when this instruction's
    /// program was selected. `None` means that data was not requested. An
    /// empty slice means that relaxed projection could not obtain a required
    /// vote-hash proof.
    pub fn data_candidates(&self) -> Option<&[InstructionDataCandidate<'de>]> {
        self.data_candidates.as_deref()
    }
}

/// The minimum semantic message graph needed for exact query projection and
/// later signature-based candidate selection.
#[derive(Debug, Clone)]
pub struct ProjectedCompactV2Message<'de> {
    static_account_count: usize,
    instruction_count: usize,
    version: ProjectedCompactV2MessageVersion<'de>,
    header: CompactMessageHeader,
    static_account_keys: smallvec::SmallVec<[CompactPubkey; 8]>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: smallvec::SmallVec<[ProjectedCompactV2Instruction<'de>; 4]>,
    expected_loaded_writable: usize,
    expected_loaded_readonly: usize,
}

impl<'de> ProjectedCompactV2Message<'de> {
    pub fn count_limits(&self) -> crate::CompactV2MetadataProjectionLimits {
        crate::CompactV2MetadataProjectionLimits {
            total_message_accounts: self.static_account_count + self.expected_loaded_addresses(),
            top_level_instruction_count: self.instruction_count,
            expected_loaded_writable: self.expected_loaded_writable,
            expected_loaded_readonly: self.expected_loaded_readonly,
        }
    }
    pub const fn version(&self) -> &ProjectedCompactV2MessageVersion<'_> {
        &self.version
    }

    pub const fn header(&self) -> CompactMessageHeader {
        self.header
    }

    pub fn static_account_keys(&self) -> &[CompactPubkey] {
        &self.static_account_keys
    }

    pub const fn recent_blockhash(&self) -> &OwnedCompactRecentBlockhash {
        &self.recent_blockhash
    }

    pub fn instructions(&self) -> &[ProjectedCompactV2Instruction<'de>] {
        &self.instructions
    }

    /// Required signers are the exact static-account prefix selected by the
    /// message header.
    pub fn required_signers(&self) -> &[CompactPubkey] {
        let required = usize::from(self.header.num_required_signatures);
        &self.static_account_keys[..required]
    }

    pub const fn expected_loaded_writable(&self) -> usize {
        self.expected_loaded_writable
    }

    pub const fn expected_loaded_readonly(&self) -> usize {
        self.expected_loaded_readonly
    }

    pub const fn expected_loaded_addresses(&self) -> usize {
        self.expected_loaded_writable + self.expected_loaded_readonly
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageKind {
    Legacy,
    V0,
    V1,
}

#[derive(Debug, Clone, Copy)]
enum InstructionDataPolicy<'a> {
    Counts,
    All {
        relaxed: bool,
    },
    Programs {
        programs: &'a [CompactPubkey],
        relaxed: bool,
    },
}

impl InstructionDataPolicy<'_> {
    fn includes(self, program: CompactPubkey) -> bool {
        match self {
            Self::Counts => false,
            Self::All { .. } => true,
            Self::Programs { programs, .. } => programs.contains(&program),
        }
    }

    fn relaxed(self) -> bool {
        match self {
            Self::Counts => false,
            Self::All { relaxed } | Self::Programs { relaxed, .. } => relaxed,
        }
    }
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
fn read_bytes_bounded<'de>(
    cursor: &mut &'de [u8],
    maximum: usize,
    error: &'static str,
) -> ReadResult<&'de [u8]> {
    let len = read_bounded_len(cursor, maximum.min(cursor.len()), error)?;
    Ok(cursor.take_borrowed(len)?)
}

#[inline]
fn read_string_bounded<'de>(cursor: &mut &'de [u8], maximum: usize) -> ReadResult<&'de str> {
    let bytes = read_bytes_bounded(
        cursor,
        maximum,
        "system instruction seed length exceeds its signed-data bound or remaining input",
    )?;
    std::str::from_utf8(bytes)
        .map_err(|_| wincode::error::invalid_value("system instruction seed is not valid UTF-8"))
}

#[inline]
fn validate_pubkey(value: CompactPubkey, registry_entries: u32) -> ReadResult<CompactPubkey> {
    if let CompactPubkey::Id(id) = value
        && (id == 0 || id > registry_entries)
    {
        return Err(wincode::error::invalid_value(
            "pubkey registry ID is outside the admitted registry",
        ));
    }
    Ok(value)
}

fn validate_header(
    kind: MessageKind,
    header: CompactMessageHeader,
    static_account_count: usize,
) -> ReadResult<()> {
    let required = usize::from(header.num_required_signatures);
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    if required == 0
        || required > static_account_count
        || readonly_signed >= required
        || readonly_unsigned > static_account_count.saturating_sub(required)
    {
        return Err(wincode::error::invalid_value(
            "message header does not describe a writable fee payer and valid account partitions",
        ));
    }
    if kind == MessageKind::Legacy && header.num_required_signatures & 0x80 != 0 {
        return Err(wincode::error::invalid_value(
            "legacy required-signature count sets the version prefix bit",
        ));
    }
    Ok(())
}

fn decode_message<'de, const MAY24: bool>(
    cursor: &mut &'de [u8],
    registry_entries: u32,
    vote_hashes: Option<&dyn VoteHashResolver>,
    data_policy: InstructionDataPolicy<'_>,
) -> CompactV2MessageProjectionResult<ProjectedCompactV2Message<'de>> {
    let count_only = matches!(data_policy, InstructionDataPolicy::Counts);
    let kind = match get::<u32>(cursor)? {
        0 => MessageKind::Legacy,
        1 => MessageKind::V0,
        2 if !MAY24 => MessageKind::V1,
        other => return Err(invalid_tag_encoding(other as usize).into()),
    };
    let header = get::<CompactMessageHeader>(cursor)?;
    let v1_config = if kind == MessageKind::V1 {
        let config = get::<CompactTransactionConfig>(cursor)?;
        Some(SignedTransactionConfig {
            priority_fee: config.priority_fee,
            compute_unit_limit: config.compute_unit_limit,
            loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit,
            heap_size: config.heap_size,
        })
    } else {
        None
    };

    let maximum_static_accounts = if kind == MessageKind::V1 {
        MAX_V1_STATIC_ACCOUNTS
    } else {
        MAX_COMPACT_V2_MESSAGE_ACCOUNTS
    };
    let static_account_count = read_bounded_len(
        cursor,
        maximum_static_accounts.min(cursor.len()),
        "static account count exceeds its message bound or remaining input",
    )?;
    let mut static_account_keys =
        smallvec::SmallVec::<[CompactPubkey; 8]>::with_capacity(if count_only {
            0
        } else {
            static_account_count
        });
    for _ in 0..static_account_count {
        let key = validate_pubkey(get::<CompactPubkey>(cursor)?, registry_entries)?;
        if !count_only {
            static_account_keys.push(key);
        }
    }
    validate_header(kind, header, static_account_count)?;
    let recent_blockhash = get::<OwnedCompactRecentBlockhash>(cursor)?;

    let maximum_instructions = if kind == MessageKind::V1 {
        MAX_V1_INSTRUCTIONS
    } else {
        MAX_SOLANA_SHORT_VEC_ITEMS
    };
    let instruction_count = read_bounded_len(
        cursor,
        maximum_instructions.min(cursor.len()),
        "top-level instruction count exceeds its signed-message bound or remaining input",
    )?;
    let mut instructions =
        smallvec::SmallVec::<[ProjectedCompactV2Instruction<'_>; 4]>::with_capacity(
            if count_only { 0 } else { instruction_count },
        );
    let mut maximum_account_index = None::<u8>;
    let mut candidate_combinations = 1_usize;
    for _ in 0..instruction_count {
        let instruction = read_instruction::<MAY24>(
            cursor,
            kind,
            &static_account_keys,
            static_account_count,
            vote_hashes,
            data_policy,
        )?;
        if let Some(candidates) = &instruction.data_candidates
            && candidates.len() > 1
        {
            let next = candidate_combinations
                .checked_mul(candidates.len())
                .filter(|count| *count <= MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS);
            if let Some(next) = next {
                candidate_combinations = next;
            } else if data_policy.relaxed() {
                candidate_combinations = MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS + 1;
            } else {
                return Err(CompactV2MessageProjectionError::CandidateCombinationLimit);
            }
        }
        if count_only {
            for &index in instruction.accounts {
                maximum_account_index =
                    Some(maximum_account_index.map_or(index, |value| value.max(index)));
            }
        } else {
            instructions.push(instruction);
        }
    }

    let mut expected_loaded_writable = 0_usize;
    let mut expected_loaded_readonly = 0_usize;
    let version = match kind {
        MessageKind::Legacy => ProjectedCompactV2MessageVersion::Legacy,
        MessageKind::V1 => ProjectedCompactV2MessageVersion::V1 {
            config: v1_config.expect("V1 config was decoded"),
        },
        MessageKind::V0 => {
            let lookup_count = read_bounded_len(
                cursor,
                MAX_COMPACT_V2_MESSAGE_ACCOUNTS.min(cursor.len()),
                "address-table lookup count exceeds its message bound or remaining input",
            )?;
            let mut address_table_lookups =
                Vec::with_capacity(if count_only { 0 } else { lookup_count });
            for _ in 0..lookup_count {
                let account_key = validate_pubkey(get::<CompactPubkey>(cursor)?, registry_entries)?;
                let writable_indexes = read_bytes_bounded(
                    cursor,
                    MAX_COMPACT_V2_MESSAGE_ACCOUNTS,
                    "writable lookup index count exceeds the message account bound or remaining input",
                )?;
                let readonly_indexes = read_bytes_bounded(
                    cursor,
                    MAX_COMPACT_V2_MESSAGE_ACCOUNTS,
                    "readonly lookup index count exceeds the message account bound or remaining input",
                )?;
                if writable_indexes.is_empty() && readonly_indexes.is_empty() {
                    return Err(wincode::error::invalid_value(
                        "address-table lookup has no writable or readonly indexes",
                    )
                    .into());
                }
                expected_loaded_writable = expected_loaded_writable
                    .checked_add(writable_indexes.len())
                    .ok_or_else(|| {
                        wincode::error::invalid_value("loaded writable account count overflow")
                    })?;
                expected_loaded_readonly = expected_loaded_readonly
                    .checked_add(readonly_indexes.len())
                    .ok_or_else(|| {
                        wincode::error::invalid_value("loaded readonly account count overflow")
                    })?;
                let total_accounts = static_account_count
                    .checked_add(expected_loaded_writable)
                    .and_then(|count| count.checked_add(expected_loaded_readonly))
                    .ok_or_else(|| {
                        wincode::error::invalid_value("message account count overflow")
                    })?;
                if total_accounts > MAX_COMPACT_V2_MESSAGE_ACCOUNTS {
                    return Err(wincode::error::invalid_value(
                        "static and loaded account count exceeds the message account bound",
                    )
                    .into());
                }
                if !count_only {
                    address_table_lookups.push(ProjectedCompactV2AddressTableLookup {
                        account_key,
                        writable_indexes,
                        readonly_indexes,
                    });
                }
            }
            ProjectedCompactV2MessageVersion::V0 {
                address_table_lookups,
            }
        }
    };

    let total_accounts = static_account_count
        .checked_add(expected_loaded_writable)
        .and_then(|count| count.checked_add(expected_loaded_readonly))
        .ok_or_else(|| wincode::error::invalid_value("message account count overflow"))?;
    for instruction in &instructions {
        if instruction
            .accounts
            .iter()
            .any(|index| usize::from(*index) >= total_accounts)
        {
            return Err(wincode::error::invalid_value(
                "instruction account index is outside the resolved message accounts",
            )
            .into());
        }
    }

    if maximum_account_index.is_some_and(|index| usize::from(index) >= total_accounts) {
        return Err(wincode::error::invalid_value(
            "instruction account index is outside the resolved message accounts",
        )
        .into());
    }

    Ok(ProjectedCompactV2Message {
        static_account_count,
        instruction_count,
        version,
        header,
        static_account_keys,
        recent_blockhash,
        instructions,
        expected_loaded_writable,
        expected_loaded_readonly,
    })
}

fn read_instruction<'de, const MAY24: bool>(
    cursor: &mut &'de [u8],
    kind: MessageKind,
    static_account_keys: &[CompactPubkey],
    static_account_count: usize,
    vote_hashes: Option<&dyn VoteHashResolver>,
    data_policy: InstructionDataPolicy<'_>,
) -> CompactV2MessageProjectionResult<ProjectedCompactV2Instruction<'de>> {
    let program_id_index = get::<u8>(cursor)?;
    if program_id_index == 0 || usize::from(program_id_index) >= static_account_count {
        return Err(wincode::error::invalid_value(
            "instruction program ID is not a non-payer static account",
        )
        .into());
    }
    let account_limit = if kind == MessageKind::V1 {
        u8::MAX as usize
    } else {
        MAX_SOLANA_SHORT_VEC_ITEMS
    };
    let accounts = read_bytes_bounded(
        cursor,
        account_limit,
        "instruction account count exceeds its signed-message bound or remaining input",
    )?;
    let data_candidates = if !matches!(data_policy, InstructionDataPolicy::Counts)
        && data_policy.includes(static_account_keys[usize::from(program_id_index)])
    {
        let candidates = match read_instruction_data::<MAY24>(cursor, vote_hashes) {
            Ok(candidates) => candidates,
            Err(error) if data_policy.relaxed() && is_missing_vote_proof(&error) => SmallVec::new(),
            Err(error) => return Err(error),
        };
        let data_limit = u16::MAX as usize;
        if candidates
            .iter()
            .any(|candidate| candidate.bytes.len() > data_limit)
        {
            return Err(wincode::error::invalid_value(
                "instruction data exceeds its signed-message bound",
            )
            .into());
        }
        Some(candidates)
    } else {
        skip_instruction_data::<MAY24>(cursor)?;
        None
    };
    Ok(ProjectedCompactV2Instruction {
        program_id_index,
        accounts,
        data_candidates,
    })
}

fn is_missing_vote_proof(error: &CompactV2MessageProjectionError) -> bool {
    matches!(
        error,
        CompactV2MessageProjectionError::ExactInstructionData(
            SignedMessageError::MissingVoteHashResolver { .. }
        )
    )
}

fn read_instruction_data<'de, const MAY24: bool>(
    cursor: &mut &'de [u8],
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> CompactV2MessageProjectionResult<SmallVec<[InstructionDataCandidate<'de>; 1]>> {
    let tag = get::<u32>(cursor)?;
    if MAY24 {
        match tag {
            0 => raw_candidate(cursor),
            1 => reconstruct(
                ArchiveV2HotInstructionData::ComputeBudget(get::<
                    ArchiveV2ComputeBudgetInstructionData,
                >(cursor)?),
                vote_hashes,
            ),
            2 => reconstruct(
                ArchiveV2HotInstructionData::System(read_system_instruction(cursor)?),
                vote_hashes,
            ),
            3 => reconstruct(
                ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(read_vote_state_update(
                    cursor,
                )?),
                vote_hashes,
            ),
            4 => reconstruct(
                ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                    update: read_vote_state_update(cursor)?,
                    switch_proof_hash: get::<ArchiveV2VoteHashRef>(cursor)?,
                },
                vote_hashes,
            ),
            5 => reconstruct(
                ArchiveV2HotInstructionData::VoteTowerSync(read_vote_tower_sync(cursor)?),
                vote_hashes,
            ),
            6 => reconstruct(
                ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                    tower: read_vote_tower_sync(cursor)?,
                    switch_proof_hash: get::<ArchiveV2VoteHashRef>(cursor)?,
                },
                vote_hashes,
            ),
            other => Err(invalid_tag_encoding(other as usize).into()),
        }
    } else {
        match tag {
            0..=2 => raw_candidate(cursor),
            3 => reconstruct(
                ArchiveV2HotInstructionData::ComputeBudget(get::<
                    ArchiveV2ComputeBudgetInstructionData,
                >(cursor)?),
                vote_hashes,
            ),
            4 => reconstruct(
                ArchiveV2HotInstructionData::System(read_system_instruction(cursor)?),
                vote_hashes,
            ),
            5 => reconstruct(
                ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(read_vote_state_update(
                    cursor,
                )?),
                vote_hashes,
            ),
            6 => reconstruct(
                ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                    update: read_vote_state_update(cursor)?,
                    switch_proof_hash: get::<ArchiveV2VoteHashRef>(cursor)?,
                },
                vote_hashes,
            ),
            7 => reconstruct(
                ArchiveV2HotInstructionData::VoteTowerSync(read_vote_tower_sync(cursor)?),
                vote_hashes,
            ),
            8 => reconstruct(
                ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                    tower: read_vote_tower_sync(cursor)?,
                    switch_proof_hash: get::<ArchiveV2VoteHashRef>(cursor)?,
                },
                vote_hashes,
            ),
            other => Err(invalid_tag_encoding(other as usize).into()),
        }
    }
}

fn raw_candidate<'de>(
    cursor: &mut &'de [u8],
) -> CompactV2MessageProjectionResult<SmallVec<[InstructionDataCandidate<'de>; 1]>> {
    let bytes = read_bytes_bounded(
        cursor,
        MAX_SOLANA_SHORT_VEC_ITEMS,
        "instruction data length exceeds its signed-message bound or remaining input",
    )?;
    Ok(smallvec![InstructionDataCandidate {
        encoding: InstructionDataEncoding::Raw,
        bytes: std::borrow::Cow::Borrowed(bytes),
    }])
}

#[test]
fn raw_candidate_borrows_payload_and_keeps_one_candidate_inline() {
    let wire = [3, 7, 8, 9];
    let mut cursor = wire.as_slice();
    let candidates = raw_candidate(&mut cursor).unwrap();
    assert!(!candidates.spilled());
    assert!(matches!(candidates[0].bytes, std::borrow::Cow::Borrowed(_)));
    assert_eq!(candidates[0].bytes.as_ptr(), wire[1..].as_ptr());
    assert!(cursor.is_empty());
}

fn reconstruct<'de>(
    data: ArchiveV2HotInstructionData,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> CompactV2MessageProjectionResult<SmallVec<[InstructionDataCandidate<'de>; 1]>> {
    reconstruct_instruction_data_candidates(&data, vote_hashes)
        .map(|values| {
            values
                .into_iter()
                .map(|value| InstructionDataCandidate {
                    encoding: value.encoding,
                    bytes: value.bytes,
                })
                .collect()
        })
        .map_err(Into::into)
}

fn read_system_instruction(cursor: &mut &[u8]) -> ReadResult<ArchiveV2SystemInstructionData> {
    let value = match get::<u32>(cursor)? {
        0 => ArchiveV2SystemInstructionData::CreateAccount {
            lamports: get::<u64>(cursor)?,
            space: get::<u64>(cursor)?,
            owner: get::<[u8; 32]>(cursor)?,
        },
        1 => ArchiveV2SystemInstructionData::Assign {
            owner: get::<[u8; 32]>(cursor)?,
        },
        2 => ArchiveV2SystemInstructionData::Transfer {
            lamports: get::<u64>(cursor)?,
        },
        3 => ArchiveV2SystemInstructionData::CreateAccountWithSeed {
            base: get::<[u8; 32]>(cursor)?,
            seed: read_string_bounded(cursor, MAX_CREATE_ACCOUNT_WITH_SEED_BYTES)?.to_owned(),
            lamports: get::<u64>(cursor)?,
            space: get::<u64>(cursor)?,
            owner: get::<[u8; 32]>(cursor)?,
        },
        4 => ArchiveV2SystemInstructionData::AdvanceNonceAccount,
        5 => ArchiveV2SystemInstructionData::WithdrawNonceAccount {
            lamports: get::<u64>(cursor)?,
        },
        6 => ArchiveV2SystemInstructionData::InitializeNonceAccount {
            authority: get::<[u8; 32]>(cursor)?,
        },
        7 => ArchiveV2SystemInstructionData::AuthorizeNonceAccount {
            authority: get::<[u8; 32]>(cursor)?,
        },
        8 => ArchiveV2SystemInstructionData::Allocate {
            space: get::<u64>(cursor)?,
        },
        9 => ArchiveV2SystemInstructionData::AllocateWithSeed {
            base: get::<[u8; 32]>(cursor)?,
            seed: read_string_bounded(cursor, MAX_ALLOCATE_WITH_SEED_BYTES)?.to_owned(),
            space: get::<u64>(cursor)?,
            owner: get::<[u8; 32]>(cursor)?,
        },
        10 => ArchiveV2SystemInstructionData::AssignWithSeed {
            base: get::<[u8; 32]>(cursor)?,
            seed: read_string_bounded(cursor, MAX_ASSIGN_WITH_SEED_BYTES)?.to_owned(),
            owner: get::<[u8; 32]>(cursor)?,
        },
        11 => ArchiveV2SystemInstructionData::TransferWithSeed {
            lamports: get::<u64>(cursor)?,
            from_seed: read_string_bounded(cursor, MAX_TRANSFER_WITH_SEED_BYTES)?.to_owned(),
            from_owner: get::<[u8; 32]>(cursor)?,
        },
        12 => ArchiveV2SystemInstructionData::UpgradeNonceAccount,
        13 => ArchiveV2SystemInstructionData::CreateAccountAllowPrefund {
            lamports: get::<u64>(cursor)?,
            space: get::<u64>(cursor)?,
            owner: get::<[u8; 32]>(cursor)?,
        },
        other => return Err(invalid_tag_encoding(other as usize)),
    };
    Ok(value)
}

fn skip_instruction_data<const MAY24: bool>(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    if MAY24 {
        match tag {
            0 => skip_raw_instruction_data(cursor)?,
            1 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
            }
            2 => skip_system_instruction(cursor)?,
            3 => skip_vote_state_update(cursor)?,
            4 => {
                skip_vote_state_update(cursor)?;
                validate_auxiliary_vote_hash(get::<ArchiveV2VoteHashRef>(cursor)?)?;
            }
            5 => skip_vote_tower_sync(cursor)?,
            6 => {
                skip_vote_tower_sync(cursor)?;
                validate_auxiliary_vote_hash(get::<ArchiveV2VoteHashRef>(cursor)?)?;
            }
            other => return Err(invalid_tag_encoding(other as usize)),
        }
    } else {
        match tag {
            0..=2 => skip_raw_instruction_data(cursor)?,
            3 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
            }
            4 => skip_system_instruction(cursor)?,
            5 => skip_vote_state_update(cursor)?,
            6 => {
                skip_vote_state_update(cursor)?;
                validate_auxiliary_vote_hash(get::<ArchiveV2VoteHashRef>(cursor)?)?;
            }
            7 => skip_vote_tower_sync(cursor)?,
            8 => {
                skip_vote_tower_sync(cursor)?;
                validate_auxiliary_vote_hash(get::<ArchiveV2VoteHashRef>(cursor)?)?;
            }
            other => return Err(invalid_tag_encoding(other as usize)),
        }
    }
    Ok(())
}

fn skip_raw_instruction_data(cursor: &mut &[u8]) -> ReadResult<()> {
    read_bytes_bounded(
        cursor,
        MAX_SOLANA_SHORT_VEC_ITEMS,
        "instruction data length exceeds its signed-message bound or remaining input",
    )?;
    Ok(())
}

fn skip_system_instruction(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u32>(cursor)? {
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
            read_string_bounded(cursor, MAX_CREATE_ACCOUNT_WITH_SEED_BYTES)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        4 | 12 => {}
        9 => {
            get::<[u8; 32]>(cursor)?;
            read_string_bounded(cursor, MAX_ALLOCATE_WITH_SEED_BYTES)?;
            get::<u64>(cursor)?;
            get::<[u8; 32]>(cursor)?;
        }
        10 => {
            get::<[u8; 32]>(cursor)?;
            read_string_bounded(cursor, MAX_ASSIGN_WITH_SEED_BYTES)?;
            get::<[u8; 32]>(cursor)?;
        }
        11 => {
            get::<u64>(cursor)?;
            read_string_bounded(cursor, MAX_TRANSFER_WITH_SEED_BYTES)?;
            get::<[u8; 32]>(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_vote_state_update(cursor: &mut &[u8]) -> ReadResult<()> {
    get::<Option<u64>>(cursor)?;
    let lockout_count = read_bounded_len(
        cursor,
        MAX_SOLANA_SHORT_VEC_ITEMS.min(cursor.len() / 2),
        "vote lockout count exceeds its canonical or remaining-input bound",
    )?;
    for _ in 0..lockout_count {
        get::<u64>(cursor)?;
        get::<u8>(cursor)?;
    }
    get::<ArchiveV2VoteHashRef>(cursor)?;
    get::<Option<i64>>(cursor)?;
    Ok(())
}

fn skip_vote_tower_sync(cursor: &mut &[u8]) -> ReadResult<()> {
    skip_vote_state_update(cursor)?;
    get::<ArchiveV2VoteHashRef>(cursor)?;
    Ok(())
}

fn validate_auxiliary_vote_hash(value: ArchiveV2VoteHashRef) -> ReadResult<()> {
    if let ArchiveV2VoteHashRef::Block(_) = value {
        return Err(wincode::error::invalid_value(
            "switch-proof hash cannot refer to the vote-hash registry",
        ));
    }
    Ok(())
}

fn read_vote_state_update(cursor: &mut &[u8]) -> ReadResult<ArchiveV2VoteStateUpdate> {
    let root = get::<Option<u64>>(cursor)?;
    let lockout_count = read_bounded_len(
        cursor,
        MAX_SOLANA_SHORT_VEC_ITEMS.min(cursor.len() / 2),
        "vote lockout count exceeds its canonical or remaining-input bound",
    )?;
    let mut lockout_offsets = Vec::with_capacity(lockout_count);
    for _ in 0..lockout_count {
        lockout_offsets.push(ArchiveV2VoteLockoutOffset {
            offset: get::<u64>(cursor)?,
            confirmation_count: get::<u8>(cursor)?,
        });
    }
    Ok(ArchiveV2VoteStateUpdate {
        root,
        lockout_offsets,
        hash: get::<ArchiveV2VoteHashRef>(cursor)?,
        timestamp: get::<Option<i64>>(cursor)?,
    })
}

fn read_vote_tower_sync(cursor: &mut &[u8]) -> ReadResult<ArchiveV2VoteTowerSync> {
    Ok(ArchiveV2VoteTowerSync {
        update: read_vote_state_update(cursor)?,
        block_id_hash: get::<ArchiveV2VoteHashRef>(cursor)?,
    })
}

#[cfg(test)]
mod tests {
    use blockzilla_format::{
        ArchiveV2HotInstruction, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotV0Message, ArchiveV2HotV1Message, OwnedCompactAddressTableLookup,
        wincode_leb128_config,
    };
    use smallvec::SmallVec;
    use wincode::SchemaWrite;

    use super::*;

    fn header() -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        }
    }

    fn key(byte: u8) -> CompactPubkey {
        CompactPubkey::Raw([byte; 32])
    }

    fn current_instruction(data: ArchiveV2HotInstructionData) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data,
        }
    }

    fn project_current(message: &ArchiveV2HotMessagePayload) -> ProjectedCompactV2Message<'static> {
        let bytes = wincode::config::serialize(message, wincode_leb128_config()).unwrap();
        // Tests need to inspect borrowed fields after this helper returns.
        let bytes = Box::leak(bytes.into_boxed_slice());
        CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0)
            .project(bytes, None)
            .unwrap()
    }

    #[derive(Debug)]
    struct TestVoteHashResolver;

    impl VoteHashResolver for TestVoteHashResolver {
        fn resolve_vote_hash(
            &self,
            block_id: u32,
            kind: crate::VoteHashKind,
        ) -> Result<[u8; 32], SignedMessageError> {
            let byte: u8 = match kind {
                crate::VoteHashKind::Bank => 0x44,
                crate::VoteHashKind::BlockId => 0x55,
            };
            Ok([byte.wrapping_add(block_id as u8); 32])
        }
    }

    #[derive(Debug)]
    struct MissingTestVoteHashResolver;

    impl VoteHashResolver for MissingTestVoteHashResolver {
        fn resolve_vote_hash(
            &self,
            block_id: u32,
            kind: crate::VoteHashKind,
        ) -> Result<[u8; 32], SignedMessageError> {
            Err(SignedMessageError::MissingVoteHash { block_id, kind })
        }
    }

    #[test]
    fn current_legacy_projects_exact_instruction_candidates() {
        let data = vec![
            ArchiveV2HotInstructionData::Raw(vec![1, 2, 3]),
            ArchiveV2HotInstructionData::UnknownSystem(vec![4, 5]),
            ArchiveV2HotInstructionData::UnknownVote(vec![6, 7]),
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(250_000),
            ),
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports: 42,
            }),
        ];
        let expected = data
            .iter()
            .map(|value| reconstruct_instruction_data_candidates(value, None).unwrap())
            .collect::<Vec<_>>();
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: header(),
            account_keys: vec![key(1), key(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: data.into_iter().map(current_instruction).collect(),
        });

        let projected = project_current(&message);
        assert!(matches!(
            projected.version(),
            ProjectedCompactV2MessageVersion::Legacy
        ));
        assert_eq!(projected.required_signers(), &[key(1)]);
        assert_eq!(projected.instructions().len(), expected.len());
        for (actual, expected) in projected.instructions().iter().zip(expected) {
            assert_eq!(actual.program_id_index(), 1);
            assert_eq!(actual.accounts(), [0]);
            assert_eq!(actual.data_candidates(), Some(expected.as_slice()));
        }
    }

    #[test]
    fn current_v0_projects_lookup_descriptors_and_counts() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: header(),
            account_keys: vec![key(1), key(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-1),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0, 2, 4],
                data: ArchiveV2HotInstructionData::Raw(vec![9]),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: key(3),
                writable_indexes: vec![4, 5],
                readonly_indexes: vec![6],
            }],
        });

        let projected = project_current(&message);
        let ProjectedCompactV2MessageVersion::V0 {
            address_table_lookups,
        } = projected.version()
        else {
            panic!("expected V0 projection");
        };
        assert_eq!(projected.expected_loaded_writable(), 2);
        assert_eq!(projected.expected_loaded_readonly(), 1);
        assert_eq!(projected.expected_loaded_addresses(), 3);
        assert_eq!(address_table_lookups.len(), 1);
        assert_eq!(address_table_lookups[0].account_key(), key(3));
        assert_eq!(address_table_lookups[0].writable_indexes(), [4, 5]);
        assert_eq!(address_table_lookups[0].readonly_indexes(), [6]);
    }

    #[test]
    fn current_v1_projects_config_signers_and_exact_data() {
        let source_data = ArchiveV2HotInstructionData::System(
            ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                base: [4; 32],
                seed: "seed".to_owned(),
                lamports: 7,
                space: 8,
                owner: [5; 32],
            },
        );
        let expected = reconstruct_instruction_data_candidates(&source_data, None).unwrap();
        let message = ArchiveV2HotMessagePayload::V1(ArchiveV2HotV1Message {
            header: header(),
            config: CompactTransactionConfig {
                priority_fee: Some(11),
                compute_unit_limit: Some(12),
                loaded_accounts_data_size_limit: None,
                heap_size: Some(13),
            },
            account_keys: vec![key(1), key(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![current_instruction(source_data)],
        });

        let projected = project_current(&message);
        let ProjectedCompactV2MessageVersion::V1 { config } = projected.version() else {
            panic!("expected V1 projection");
        };
        assert_eq!(config.priority_fee, Some(11));
        assert_eq!(config.compute_unit_limit, Some(12));
        assert_eq!(config.loaded_accounts_data_size_limit, None);
        assert_eq!(config.heap_size, Some(13));
        assert_eq!(projected.required_signers(), &[key(1)]);
        assert_eq!(
            projected.instructions()[0].data_candidates(),
            Some(expected.as_slice())
        );
    }

    #[test]
    fn selected_program_skips_unrelated_vote_resolution_and_ambiguity() {
        let token_program = key(2);
        let vote_program = key(3);
        let tower = ArchiveV2VoteTowerSync {
            update: ArchiveV2VoteStateUpdate {
                root: Some(10),
                lockout_offsets: vec![ArchiveV2VoteLockoutOffset {
                    offset: 1,
                    confirmation_count: 2,
                }],
                hash: ArchiveV2VoteHashRef::Block(0),
                timestamp: None,
            },
            block_id_hash: ArchiveV2VoteHashRef::Block(0),
        };
        let mut instructions = (0..14)
            .map(|_| ArchiveV2HotInstruction {
                program_id_index: 2,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::VoteTowerSync(tower.clone()),
            })
            .collect::<Vec<_>>();
        instructions.push(ArchiveV2HotInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: ArchiveV2HotInstructionData::Raw(vec![7, 8, 9]),
        });
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![key(1), token_program, vote_program],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([4; 32]),
            instructions,
        });
        let bytes = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let projector = CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0);

        let projected = projector
            .project_with_instruction_data_for_programs(&bytes, &[token_program], None)
            .unwrap();
        assert_eq!(projected.instructions().len(), 15);
        assert!(
            projected.instructions()[..14]
                .iter()
                .all(|instruction| instruction.data_candidates().is_none())
        );
        assert_eq!(
            projected.instructions()[14].data_candidates(),
            Some(
                [InstructionDataCandidate {
                    encoding: InstructionDataEncoding::Raw,
                    bytes: vec![7, 8, 9].into(),
                }]
                .as_slice()
            )
        );

        assert!(matches!(
            projector.project(&bytes, None),
            Err(CompactV2MessageProjectionError::ExactInstructionData(
                SignedMessageError::MissingVoteHashResolver { .. }
            ))
        ));
        assert!(matches!(
            projector.project(&bytes, Some(&TestVoteHashResolver)),
            Err(CompactV2MessageProjectionError::CandidateCombinationLimit)
        ));

        let relaxed_missing = projector.project_relaxed(&bytes, None).unwrap();
        assert!(
            relaxed_missing.instructions()[..14]
                .iter()
                .all(|instruction| instruction.data_candidates() == Some(&[]))
        );
        assert_eq!(
            relaxed_missing.instructions()[14].data_candidates(),
            Some(
                [InstructionDataCandidate {
                    encoding: InstructionDataEncoding::Raw,
                    bytes: vec![7, 8, 9].into(),
                }]
                .as_slice()
            )
        );

        assert!(matches!(
            projector.project_relaxed(&bytes, Some(&MissingTestVoteHashResolver)),
            Err(CompactV2MessageProjectionError::ExactInstructionData(
                SignedMessageError::MissingVoteHash { .. }
            ))
        ));

        let relaxed_ambiguous = projector
            .project_relaxed(&bytes, Some(&TestVoteHashResolver))
            .unwrap();
        assert!(
            relaxed_ambiguous.instructions()[..14]
                .iter()
                .all(|instruction| instruction
                    .data_candidates()
                    .is_some_and(|candidates| candidates.len() == 2))
        );
    }

    #[test]
    fn selected_program_policy_is_bounded_and_registry_checked() {
        let projector = CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 1);
        let programs = vec![key(1); MAX_SELECTED_INSTRUCTION_DATA_PROGRAMS + 1];
        assert!(matches!(
            projector.project_with_instruction_data_for_programs(&[], &programs, None),
            Err(CompactV2MessageProjectionError::SelectedProgramLimit { actual })
                if actual == MAX_SELECTED_INSTRUCTION_DATA_PROGRAMS + 1
        ));
        assert!(matches!(
            projector.project_with_instruction_data_for_programs(
                &[],
                &[CompactPubkey::Id(2)],
                None,
            ),
            Err(
                CompactV2MessageProjectionError::InvalidSelectedProgramRegistryId {
                    id: 2,
                    registry_entries: 1,
                }
            )
        ));
    }

    #[test]
    fn oversized_system_seed_is_rejected_for_selected_and_skipped_data() {
        let token_program = key(2);
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![key(1), key(3), token_program],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([4; 32]),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::System(
                    ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                        base: [5; 32],
                        seed: "a".repeat(MAX_CREATE_ACCOUNT_WITH_SEED_BYTES + 1),
                        lamports: 1,
                        space: 2,
                        owner: [6; 32],
                    },
                ),
            }],
        });
        let bytes = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let projector = CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0);

        assert!(matches!(
            projector.project(&bytes, None),
            Err(CompactV2MessageProjectionError::Decode(_))
        ));
        assert!(matches!(
            projector.project_with_instruction_data_for_programs(&bytes, &[token_program], None),
            Err(CompactV2MessageProjectionError::Decode(_))
        ));
    }

    #[derive(SchemaWrite)]
    enum May24Message {
        Legacy(May24LegacyMessage),
        V0(May24V0Message),
    }

    #[derive(SchemaWrite)]
    struct May24LegacyMessage {
        header: CompactMessageHeader,
        account_keys: SmallVec<[CompactPubkey; 8]>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: SmallVec<[May24Instruction; 2]>,
    }

    #[derive(SchemaWrite)]
    struct May24V0Message {
        header: CompactMessageHeader,
        account_keys: SmallVec<[CompactPubkey; 8]>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: SmallVec<[May24Instruction; 2]>,
        address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
    }

    #[derive(SchemaWrite)]
    struct May24Instruction {
        program_id_index: u8,
        accounts: SmallVec<[u8; 8]>,
        data: May24InstructionData,
    }

    #[allow(dead_code)]
    #[derive(SchemaWrite)]
    enum May24InstructionData {
        Raw(SmallVec<[u8; 64]>),
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

    fn may24_instruction(data: May24InstructionData) -> May24Instruction {
        May24Instruction {
            program_id_index: 1,
            accounts: SmallVec::from_slice(&[0]),
            data,
        }
    }

    #[test]
    fn may24_legacy_and_v0_use_the_historical_tag_table() {
        let legacy = May24Message::Legacy(May24LegacyMessage {
            header: header(),
            account_keys: SmallVec::from_slice(&[key(1), key(2)]),
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: SmallVec::from_vec(vec![may24_instruction(
                May24InstructionData::ComputeBudget(
                    ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(99),
                ),
            )]),
        });
        let legacy_bytes = wincode::config::serialize(&legacy, wincode_leb128_config()).unwrap();
        let legacy_projected =
            CompactV2MessageProjector::new(CompactV2MessageSchema::May24PreUnknownFallbacks, 0)
                .project(&legacy_bytes, None)
                .unwrap();
        assert!(matches!(
            legacy_projected.version(),
            ProjectedCompactV2MessageVersion::Legacy
        ));
        assert_eq!(
            legacy_projected.instructions()[0].data_candidates(),
            Some(
                reconstruct_instruction_data_candidates(
                    &ArchiveV2HotInstructionData::ComputeBudget(
                        ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(99),
                    ),
                    None,
                )
                .unwrap()
                .as_slice()
            )
        );

        let v0 = May24Message::V0(May24V0Message {
            header: header(),
            account_keys: SmallVec::from_slice(&[key(1), key(2)]),
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: SmallVec::from_vec(vec![may24_instruction(
                May24InstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                    lamports: 77,
                }),
            )]),
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: key(9),
                writable_indexes: vec![1],
                readonly_indexes: vec![],
            }],
        });
        let v0_bytes = wincode::config::serialize(&v0, wincode_leb128_config()).unwrap();
        let v0_projected =
            CompactV2MessageProjector::new(CompactV2MessageSchema::May24PreUnknownFallbacks, 0)
                .project(&v0_bytes, None)
                .unwrap();
        assert!(matches!(
            v0_projected.version(),
            ProjectedCompactV2MessageVersion::V0 { .. }
        ));
        assert_eq!(v0_projected.expected_loaded_writable(), 1);
    }

    fn append_wire<T>(bytes: &mut Vec<u8>, value: &T)
    where
        T: SchemaWrite<Cfg, Src = T>,
    {
        bytes.extend(wincode::config::serialize(value, wincode_leb128_config()).unwrap());
    }

    fn valid_legacy_prefix(bytes: &mut Vec<u8>) {
        append_wire(bytes, &0_u32);
        append_wire(bytes, &header());
        append_wire(bytes, &2_u64);
        append_wire(bytes, &key(1));
        append_wire(bytes, &key(2));
        append_wire(bytes, &OwnedCompactRecentBlockhash::Nonce([3; 32]));
    }

    #[test]
    fn corrupt_huge_counts_fail_before_input_driven_allocation() {
        let mut huge_static_count = Vec::new();
        append_wire(&mut huge_static_count, &0_u32);
        append_wire(&mut huge_static_count, &header());
        append_wire(&mut huge_static_count, &u64::MAX);

        for schema in [
            CompactV2MessageSchema::Current,
            CompactV2MessageSchema::May24PreUnknownFallbacks,
        ] {
            assert!(
                CompactV2MessageProjector::new(schema, 0)
                    .project(&huge_static_count, None)
                    .is_err()
            );
        }

        let mut huge_instruction_count = Vec::new();
        valid_legacy_prefix(&mut huge_instruction_count);
        append_wire(&mut huge_instruction_count, &u64::MAX);
        assert!(
            CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0)
                .project(&huge_instruction_count, None)
                .is_err()
        );

        let mut huge_raw_data_count = Vec::new();
        valid_legacy_prefix(&mut huge_raw_data_count);
        append_wire(&mut huge_raw_data_count, &1_u64);
        append_wire(&mut huge_raw_data_count, &1_u8);
        append_wire(&mut huge_raw_data_count, &0_u64);
        append_wire(&mut huge_raw_data_count, &0_u32);
        append_wire(&mut huge_raw_data_count, &u64::MAX);
        assert!(
            CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0)
                .project(&huge_raw_data_count, None)
                .is_err()
        );

        let mut huge_vote_lockout_count = Vec::new();
        valid_legacy_prefix(&mut huge_vote_lockout_count);
        append_wire(&mut huge_vote_lockout_count, &1_u64);
        append_wire(&mut huge_vote_lockout_count, &1_u8);
        append_wire(&mut huge_vote_lockout_count, &0_u64);
        append_wire(&mut huge_vote_lockout_count, &5_u32);
        append_wire(&mut huge_vote_lockout_count, &Option::<u64>::None);
        append_wire(&mut huge_vote_lockout_count, &u64::MAX);
        assert!(
            CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0)
                .project(&huge_vote_lockout_count, None)
                .is_err()
        );

        let mut huge_v1_static_count = Vec::new();
        append_wire(&mut huge_v1_static_count, &2_u32);
        append_wire(&mut huge_v1_static_count, &header());
        append_wire(
            &mut huge_v1_static_count,
            &CompactTransactionConfig::default(),
        );
        append_wire(&mut huge_v1_static_count, &u64::MAX);
        assert!(
            CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0)
                .project(&huge_v1_static_count, None)
                .is_err()
        );
    }
}
