//! Exact Solana signed-message serialization and Ed25519 verification.
//!
//! Archive V2 can retain more than one possible byte form for an instruction.
//! This module owns the bounded whole-message search used to recover the one
//! form proved by transaction signatures. It also exposes the serializer and
//! signature verifier so archive readers and conversion tools use the same
//! wire rules.

use std::{borrow::Cow, fmt};

use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstructionData,
    ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate,
    ArchiveV2VoteTowerSync, CompactMessageHeader,
};
use solana_signature::Signature;
use thiserror::Error;

const VERSIONED_MESSAGE_PREFIX: u8 = 0x80;
const V0_MESSAGE_VERSION: u8 = 0;
const V1_MESSAGE_VERSION: u8 = 1;

/// SIMD-0385 caps a v1 message's own counts.
const V1_MAX_ADDRESSES: u8 = 64;
const V1_MAX_INSTRUCTIONS: u8 = 64;

/// Config mask bits, in the order their values are written. `PRIORITY_FEE`
/// takes two bits because the value array is counted in four-byte slots.
const V1_CONFIG_PRIORITY_FEE: u32 = 0b11;
const V1_CONFIG_COMPUTE_UNIT_LIMIT: u32 = 0b100;
const V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE: u32 = 0b1000;
const V1_CONFIG_HEAP_SIZE: u32 = 0b1_0000;

/// Absolute bound on whole-message candidates tested for one transaction.
///
/// This covers 13 independent two-form instructions (`2^13` candidates), the
/// largest ambiguous combination that still fits a Solana transaction packet.
pub const MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS: usize = 8_192;

/// Width of one Compact V2 vote-hash dictionary record.
pub const VOTE_HASH_RECORD_LEN: usize = 65;
/// Independent practical cap for one vote-hash registry.
pub const MAX_VOTE_HASH_REGISTRY_BYTES: usize = 64 << 20;

/// A possible exact wire form for a typed Compact V2 instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionDataEncoding {
    Raw,
    ComputeBudget,
    System,
    VoteCompact,
    VoteTowerCanonical,
    VoteTowerHistorical,
}

/// One exact instruction-data candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionDataCandidate<'a> {
    pub encoding: InstructionDataEncoding,
    pub bytes: Cow<'a, [u8]>,
}

/// Which column of the Compact V2 vote-hash dictionary a reference uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteHashKind {
    Bank,
    BlockId,
}

impl fmt::Display for VoteHashKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bank => f.write_str("bank"),
            Self::BlockId => f.write_str("block-id"),
        }
    }
}

/// Resolve block-local vote hash references used by typed vote instructions.
pub trait VoteHashResolver {
    fn resolve_vote_hash(
        &self,
        block_id: u32,
        kind: VoteHashKind,
    ) -> Result<[u8; 32], SignedMessageError>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct VoteHashRow {
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>,
}

/// Checked view of `vote_hash_registry.bin`.
#[derive(Debug, Clone)]
pub struct VoteHashRegistry {
    rows: Vec<VoteHashRow>,
}

/// One resolved V0 lookup descriptor as it appears in signed message bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedAddressTableLookup<'a> {
    pub account_key: [u8; 32],
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
}

/// The compute budget a v1 message carries in its header (SIMD-0385).
///
/// Presence is load-bearing: which fields are set is exactly what the config
/// mask encodes, and the values follow in bit order. `Some(0)` and `None` are
/// different messages and hash differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SignedTransactionConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

impl SignedTransactionConfig {
    fn mask(&self) -> u32 {
        let mut mask = 0;
        if self.priority_fee.is_some() {
            mask |= V1_CONFIG_PRIORITY_FEE;
        }
        if self.compute_unit_limit.is_some() {
            mask |= V1_CONFIG_COMPUTE_UNIT_LIMIT;
        }
        if self.loaded_accounts_data_size_limit.is_some() {
            mask |= V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE;
        }
        if self.heap_size.is_some() {
            mask |= V1_CONFIG_HEAP_SIZE;
        }
        mask
    }
}

/// The signed-message envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedMessageVersion<'a> {
    Legacy,
    V0 {
        address_table_lookups: &'a [ResolvedAddressTableLookup<'a>],
    },
    /// v1 carries no lookup tables; its compute budget rides in the header.
    V1 {
        config: SignedTransactionConfig,
    },
}

/// One instruction whose exact data bytes are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedInstruction<'a> {
    pub program_id_index: u8,
    pub accounts: &'a [u8],
    pub data: &'a [u8],
}

/// A resolved Legacy, V0, or V1 message ready for canonical serialization.
#[derive(Debug, Clone, Copy)]
pub struct SignedMessage<'a> {
    pub version: SignedMessageVersion<'a>,
    pub header: CompactMessageHeader,
    pub static_account_keys: &'a [[u8; 32]],
    pub recent_blockhash: [u8; 32],
    pub instructions: &'a [SignedInstruction<'a>],
}

/// One instruction with all byte forms that Compact V2 could have erased.
#[derive(Debug, Clone, Copy)]
pub struct SignedInstructionCandidates<'a> {
    pub program_id_index: u8,
    pub accounts: &'a [u8],
    pub data_candidates: &'a [InstructionDataCandidate<'a>],
}

/// A resolved message whose instruction bytes still need signature proof.
#[derive(Debug, Clone, Copy)]
pub struct SignedMessageCandidates<'a> {
    pub version: SignedMessageVersion<'a>,
    pub header: CompactMessageHeader,
    pub static_account_keys: &'a [[u8; 32]],
    pub recent_blockhash: [u8; 32],
    pub instructions: &'a [SignedInstructionCandidates<'a>],
}

/// The unique instruction bytes and whole message accepted by a verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedSignedMessage {
    pub instruction_data: Vec<Vec<u8>>,
    pub signed_message: Vec<u8>,
}

/// An invalid signed message, candidate set, or Ed25519 proof.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SignedMessageError {
    #[error("vote-hash registry has {actual} bytes, above the {maximum}-byte practical limit")]
    VoteHashRegistryByteLimit { actual: usize, maximum: usize },
    #[error("vote-hash registry length {actual} is not a multiple of 65")]
    InvalidVoteHashRegistryLength { actual: usize },
    #[error("vote-hash registry has {records} records, which exceeds the u32 block-id range")]
    VoteHashRegistryTooLarge { records: usize },
    #[error("cannot reserve {records} vote-hash registry records")]
    VoteHashRegistryAllocation { records: usize },
    #[error("vote-hash registry block {block_id} has unknown flags {flags:#04x}")]
    InvalidVoteHashRegistryFlags { block_id: u32, flags: u8 },
    #[error(
        "instruction needs {kind} vote hash for block {block_id}, but no registry was supplied"
    )]
    MissingVoteHashResolver { block_id: u32, kind: VoteHashKind },
    #[error("vote-hash registry block {block_id} has no {kind} hash")]
    MissingVoteHash { block_id: u32, kind: VoteHashKind },
    #[error("switch-proof hash unexpectedly refers to vote-hash registry block {block_id}")]
    AuxiliaryVoteHashBlockReference { block_id: u32 },
    #[error(
        "Compact V2 does not identify which of {candidates} valid instruction encodings was signed"
    )]
    AmbiguousInstructionEncoding { candidates: usize },
    #[error("legacy required-signature count {required} sets the version prefix bit")]
    LegacyRequiredSignaturesSetVersionBit { required: u8 },
    #[error("required-signature count {required} exceeds {static_keys} static account keys")]
    RequiredSignaturesExceedStaticKeys { required: u8, static_keys: usize },
    #[error("readonly signed count {readonly} exceeds required-signature count {required}")]
    ReadonlySignedExceedRequired { readonly: u8, required: u8 },
    #[error(
        "readonly signed count {readonly} leaves no writable fee payer among {required} required signatures"
    )]
    NoWritableFeePayer { readonly: u8, required: u8 },
    #[error(
        "readonly unsigned count {readonly} exceeds {unsigned_static} unsigned static account keys"
    )]
    ReadonlyUnsignedExceedUnsignedStatic {
        readonly: u8,
        unsigned_static: usize,
    },
    #[error("static and loaded message account counts overflow usize")]
    MessageAccountCountOverflow,
    #[error(
        "message has {actual} static and loaded accounts, which exceeds the 256 accounts addressable by u8 indexes"
    )]
    MessageAccountCountExceedsIndexRange { actual: usize },
    #[error("V0 address-table lookup {lookup} does not load a writable or readonly account")]
    EmptyAddressTableLookup { lookup: usize },
    #[error(
        "instruction {instruction} {field} index {index} is outside {account_count} message accounts"
    )]
    MessageAccountIndexOutOfBounds {
        instruction: usize,
        field: &'static str,
        index: u8,
        account_count: usize,
    },
    #[error("instruction {instruction} uses the fee payer as its program")]
    ProgramIdIsFeePayer { instruction: usize },
    #[error(
        "V0 instruction {instruction} program index {index} refers to a loaded account; only {static_account_count} static accounts can be programs"
    )]
    V0ProgramIdIsLoaded {
        instruction: usize,
        index: u8,
        static_account_count: usize,
    },
    #[error("{field} length {actual} exceeds u16")]
    ShortVecLengthTooLarge { field: &'static str, actual: usize },
    #[error("instruction {instruction} has no data candidate")]
    EmptyInstructionCandidates { instruction: usize },
    #[error("candidate combination limit {requested} is outside 1..={hard_maximum}")]
    InvalidCandidateCombinationLimit {
        requested: usize,
        hard_maximum: usize,
    },
    #[error("instruction candidates require more than {maximum} message combinations")]
    CandidateCombinationLimitExceeded { maximum: usize },
    #[error("no reconstructed signed-message candidate verified")]
    NoVerifiedMessageCandidate,
    #[error("more than one reconstructed signed-message candidate verified")]
    MultipleVerifiedMessageCandidates,
    #[error("Ed25519 verification needs at least one signature")]
    NoSignatures,
    #[error("Ed25519 signer count {signer_pubkeys} does not match signature count {signatures}")]
    SignerSignatureCountMismatch {
        signer_pubkeys: usize,
        signatures: usize,
    },
    #[error(
        "message requires {required} signatures, but the archive supplied {signatures} signatures"
    )]
    RequiredSignatureCountMismatch { required: usize, signatures: usize },
    #[error("Ed25519 signature {signer_index} did not verify")]
    SignatureVerificationFailed { signer_index: usize },
    #[error("vote lockout count {actual} exceeds the canonical short-vector u16 limit")]
    VoteLockoutCountTooLarge { actual: usize },
    #[error("vote lockout slot {previous}+{offset} overflows u64")]
    VoteLockoutSlotOverflow { previous: u64, offset: u64 },
    #[error("system instruction seed length {actual} exceeds u64")]
    SystemSeedTooLong { actual: usize },
}

/// Serialize the canonical Solana bytes covered by transaction signatures.
///
/// All public keys and the recent blockhash must already be resolved to their
/// exact 32-byte values. This function does not read a dictionary and does not
/// use runtime-loaded addresses. V0 lookup descriptors are signed, while the
/// addresses that they load are not part of the signed message.
pub fn serialize_signed_message(
    message: &SignedMessage<'_>,
) -> Result<Vec<u8>, SignedMessageError> {
    validate_message_header(message)?;
    let account_count = message_account_count(message)?;

    if let SignedMessageVersion::V1 { config } = message.version {
        return serialize_signed_v1_message(message, &config, account_count);
    }

    let mut out = Vec::new();
    if matches!(message.version, SignedMessageVersion::V0 { .. }) {
        out.push(VERSIONED_MESSAGE_PREFIX | V0_MESSAGE_VERSION);
    }
    out.extend_from_slice(&[
        message.header.num_required_signatures,
        message.header.num_readonly_signed_accounts,
        message.header.num_readonly_unsigned_accounts,
    ]);
    push_message_short_vec_len(
        &mut out,
        message.static_account_keys.len(),
        "static account keys",
    )?;
    for key in message.static_account_keys {
        out.extend_from_slice(key);
    }
    out.extend_from_slice(&message.recent_blockhash);

    push_message_short_vec_len(&mut out, message.instructions.len(), "instructions")?;
    let is_v0 = matches!(message.version, SignedMessageVersion::V0 { .. });
    for (instruction_index, instruction) in message.instructions.iter().enumerate() {
        validate_instruction_indices(
            instruction_index,
            instruction,
            account_count,
            message.static_account_keys.len(),
            is_v0,
        )?;
        out.push(instruction.program_id_index);
        push_message_short_vec_len(&mut out, instruction.accounts.len(), "instruction accounts")?;
        out.extend_from_slice(instruction.accounts);
        push_message_short_vec_len(&mut out, instruction.data.len(), "instruction data")?;
        out.extend_from_slice(instruction.data);
    }

    if let SignedMessageVersion::V0 {
        address_table_lookups,
    } = message.version
    {
        push_message_short_vec_len(
            &mut out,
            address_table_lookups.len(),
            "address-table lookups",
        )?;
        for lookup in address_table_lookups {
            out.extend_from_slice(&lookup.account_key);
            push_message_short_vec_len(
                &mut out,
                lookup.writable_indexes.len(),
                "writable lookup indexes",
            )?;
            out.extend_from_slice(lookup.writable_indexes);
            push_message_short_vec_len(
                &mut out,
                lookup.readonly_indexes.len(),
                "readonly lookup indexes",
            )?;
            out.extend_from_slice(lookup.readonly_indexes);
        }
    }

    Ok(out)
}

/// Verify every Ed25519 signature over the same serialized message.
///
/// Signer public key `i` must match signature `i`. The function rejects empty
/// and unequal lists, and it reports the first signature that does not verify.
pub fn verify_ed25519_signatures(
    signed_message: &[u8],
    signer_pubkeys: &[[u8; 32]],
    signatures: &[[u8; 64]],
) -> Result<(), SignedMessageError> {
    if signatures.is_empty() {
        return Err(SignedMessageError::NoSignatures);
    }
    if signer_pubkeys.len() != signatures.len() {
        return Err(SignedMessageError::SignerSignatureCountMismatch {
            signer_pubkeys: signer_pubkeys.len(),
            signatures: signatures.len(),
        });
    }
    for (signer_index, (signer, signature)) in signer_pubkeys.iter().zip(signatures).enumerate() {
        if !Signature::from(*signature).verify(signer, signed_message) {
            return Err(SignedMessageError::SignatureVerificationFailed { signer_index });
        }
    }
    Ok(())
}

/// Select with the first signer, then verify every required signature.
///
/// The first signature is the candidate oracle because one valid signer is
/// enough to prove the exact signed bytes. After it selects one unique byte
/// form, every required signer is checked against those same bytes. Missing,
/// extra, invalid, absent, and non-unique proofs are errors.
pub fn select_signed_message_candidate_ed25519(
    message: &SignedMessageCandidates<'_>,
    max_combinations: usize,
    signatures: &[[u8; 64]],
) -> Result<SelectedSignedMessage, SignedMessageError> {
    let required = usize::from(message.header.num_required_signatures);
    if signatures.len() != required {
        return Err(SignedMessageError::RequiredSignatureCountMismatch {
            required,
            signatures: signatures.len(),
        });
    }
    let first_signature = signatures.first().ok_or(SignedMessageError::NoSignatures)?;
    let signer_pubkeys = message.static_account_keys.get(..required).ok_or(
        SignedMessageError::RequiredSignaturesExceedStaticKeys {
            required: message.header.num_required_signatures,
            static_keys: message.static_account_keys.len(),
        },
    )?;
    let first_signer = signer_pubkeys
        .first()
        .ok_or(SignedMessageError::NoSignatures)?;
    let first_signature = Signature::from(*first_signature);
    let selected = select_signed_message_candidate(message, max_combinations, |signed_message| {
        first_signature.verify(first_signer, signed_message)
    })?;
    verify_ed25519_signatures(&selected.signed_message, signer_pubkeys, signatures)?;
    Ok(selected)
}

/// Select the only whole-message candidate accepted by `verify`.
///
/// Candidate combinations are enumerated in instruction order and candidate
/// order. The function calculates the full combination count before it calls
/// `verify`. It does no work if that count exceeds `max_combinations`, and the
/// caller cannot raise the limit above
/// [`MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS`].
pub fn select_signed_message_candidate<F>(
    message: &SignedMessageCandidates<'_>,
    max_combinations: usize,
    mut verify: F,
) -> Result<SelectedSignedMessage, SignedMessageError>
where
    F: FnMut(&[u8]) -> bool,
{
    if max_combinations == 0 || max_combinations > MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS {
        return Err(SignedMessageError::InvalidCandidateCombinationLimit {
            requested: max_combinations,
            hard_maximum: MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
        });
    }

    let mut combination_count = 1_usize;
    for (instruction, candidates) in message.instructions.iter().enumerate() {
        if candidates.data_candidates.is_empty() {
            return Err(SignedMessageError::EmptyInstructionCandidates { instruction });
        }
        combination_count = combination_count
            .checked_mul(candidates.data_candidates.len())
            .filter(|count| *count <= max_combinations)
            .ok_or(SignedMessageError::CandidateCombinationLimitExceeded {
                maximum: max_combinations,
            })?;
    }

    let mut choice_indices = vec![0_usize; message.instructions.len()];
    let mut selected: Option<SelectedSignedMessage> = None;
    for ordinal in 0..combination_count {
        let instructions = message
            .instructions
            .iter()
            .zip(&choice_indices)
            .map(|(instruction, choice)| SignedInstruction {
                program_id_index: instruction.program_id_index,
                accounts: instruction.accounts,
                data: &instruction.data_candidates[*choice].bytes,
            })
            .collect::<Vec<_>>();
        let signed_message = serialize_signed_message(&SignedMessage {
            version: message.version,
            header: message.header,
            static_account_keys: message.static_account_keys,
            recent_blockhash: message.recent_blockhash,
            instructions: &instructions,
        })?;

        if verify(&signed_message) {
            let instruction_data = instructions
                .iter()
                .map(|instruction| instruction.data.to_vec())
                .collect::<Vec<_>>();
            let candidate = SelectedSignedMessage {
                instruction_data,
                signed_message,
            };
            if let Some(previous) = &selected {
                if previous != &candidate {
                    return Err(SignedMessageError::MultipleVerifiedMessageCandidates);
                }
            } else {
                selected = Some(candidate);
            }
        }

        if ordinal + 1 < combination_count {
            increment_candidate_choices(&mut choice_indices, message.instructions);
        }
    }

    selected.ok_or(SignedMessageError::NoVerifiedMessageCandidate)
}

fn serialize_signed_v1_message(
    message: &SignedMessage<'_>,
    config: &SignedTransactionConfig,
    account_count: usize,
) -> Result<Vec<u8>, SignedMessageError> {
    let instruction_count = u8::try_from(message.instructions.len()).map_err(|_| {
        SignedMessageError::ShortVecLengthTooLarge {
            field: "v1 instructions",
            actual: message.instructions.len(),
        }
    })?;
    let address_count = u8::try_from(message.static_account_keys.len()).map_err(|_| {
        SignedMessageError::ShortVecLengthTooLarge {
            field: "v1 addresses",
            actual: message.static_account_keys.len(),
        }
    })?;
    if instruction_count > V1_MAX_INSTRUCTIONS {
        return Err(SignedMessageError::ShortVecLengthTooLarge {
            field: "v1 instructions",
            actual: message.instructions.len(),
        });
    }
    if address_count > V1_MAX_ADDRESSES {
        return Err(SignedMessageError::ShortVecLengthTooLarge {
            field: "v1 addresses",
            actual: message.static_account_keys.len(),
        });
    }

    let mut out = Vec::new();
    out.push(VERSIONED_MESSAGE_PREFIX | V1_MESSAGE_VERSION);
    out.extend_from_slice(&[
        message.header.num_required_signatures,
        message.header.num_readonly_signed_accounts,
        message.header.num_readonly_unsigned_accounts,
    ]);
    out.extend_from_slice(&config.mask().to_le_bytes());
    out.extend_from_slice(&message.recent_blockhash);
    out.push(instruction_count);
    out.push(address_count);

    for key in message.static_account_keys {
        out.extend_from_slice(key);
    }

    if let Some(priority_fee) = config.priority_fee {
        out.extend_from_slice(&priority_fee.to_le_bytes());
    }
    if let Some(compute_unit_limit) = config.compute_unit_limit {
        out.extend_from_slice(&compute_unit_limit.to_le_bytes());
    }
    if let Some(limit) = config.loaded_accounts_data_size_limit {
        out.extend_from_slice(&limit.to_le_bytes());
    }
    if let Some(heap_size) = config.heap_size {
        out.extend_from_slice(&heap_size.to_le_bytes());
    }

    for (instruction_index, instruction) in message.instructions.iter().enumerate() {
        validate_instruction_indices(
            instruction_index,
            instruction,
            account_count,
            message.static_account_keys.len(),
            false,
        )?;
        let num_accounts = u8::try_from(instruction.accounts.len()).map_err(|_| {
            SignedMessageError::ShortVecLengthTooLarge {
                field: "v1 instruction accounts",
                actual: instruction.accounts.len(),
            }
        })?;
        let data_len = u16::try_from(instruction.data.len()).map_err(|_| {
            SignedMessageError::ShortVecLengthTooLarge {
                field: "v1 instruction data",
                actual: instruction.data.len(),
            }
        })?;
        out.push(instruction.program_id_index);
        out.push(num_accounts);
        out.extend_from_slice(&data_len.to_le_bytes());
    }

    for instruction in message.instructions {
        out.extend_from_slice(instruction.accounts);
        out.extend_from_slice(instruction.data);
    }

    Ok(out)
}

fn validate_message_header(message: &SignedMessage<'_>) -> Result<(), SignedMessageError> {
    let required = message.header.num_required_signatures;
    if matches!(message.version, SignedMessageVersion::Legacy)
        && required & VERSIONED_MESSAGE_PREFIX != 0
    {
        return Err(SignedMessageError::LegacyRequiredSignaturesSetVersionBit { required });
    }
    if usize::from(required) > message.static_account_keys.len() {
        return Err(SignedMessageError::RequiredSignaturesExceedStaticKeys {
            required,
            static_keys: message.static_account_keys.len(),
        });
    }
    let readonly_signed = message.header.num_readonly_signed_accounts;
    if readonly_signed > required {
        return Err(SignedMessageError::ReadonlySignedExceedRequired {
            readonly: readonly_signed,
            required,
        });
    }
    if readonly_signed == required {
        return Err(SignedMessageError::NoWritableFeePayer {
            readonly: readonly_signed,
            required,
        });
    }
    let unsigned_static = message
        .static_account_keys
        .len()
        .saturating_sub(usize::from(required));
    let readonly_unsigned = message.header.num_readonly_unsigned_accounts;
    if usize::from(readonly_unsigned) > unsigned_static {
        return Err(SignedMessageError::ReadonlyUnsignedExceedUnsignedStatic {
            readonly: readonly_unsigned,
            unsigned_static,
        });
    }
    Ok(())
}

fn message_account_count(message: &SignedMessage<'_>) -> Result<usize, SignedMessageError> {
    let loaded = match message.version {
        SignedMessageVersion::Legacy | SignedMessageVersion::V1 { .. } => 0,
        SignedMessageVersion::V0 {
            address_table_lookups,
        } => address_table_lookups.iter().enumerate().try_fold(
            0_usize,
            |count, (lookup_index, lookup)| {
                if lookup.writable_indexes.is_empty() && lookup.readonly_indexes.is_empty() {
                    return Err(SignedMessageError::EmptyAddressTableLookup {
                        lookup: lookup_index,
                    });
                }
                count
                    .checked_add(lookup.writable_indexes.len())
                    .and_then(|count| count.checked_add(lookup.readonly_indexes.len()))
                    .ok_or(SignedMessageError::MessageAccountCountOverflow)
            },
        )?,
    };
    let account_count = message
        .static_account_keys
        .len()
        .checked_add(loaded)
        .ok_or(SignedMessageError::MessageAccountCountOverflow)?;
    if account_count > usize::from(u8::MAX) + 1 {
        return Err(SignedMessageError::MessageAccountCountExceedsIndexRange {
            actual: account_count,
        });
    }
    Ok(account_count)
}

fn validate_instruction_indices(
    instruction_index: usize,
    instruction: &SignedInstruction<'_>,
    account_count: usize,
    static_account_count: usize,
    is_v0: bool,
) -> Result<(), SignedMessageError> {
    if usize::from(instruction.program_id_index) >= account_count {
        return Err(SignedMessageError::MessageAccountIndexOutOfBounds {
            instruction: instruction_index,
            field: "program",
            index: instruction.program_id_index,
            account_count,
        });
    }
    if instruction.program_id_index == 0 {
        return Err(SignedMessageError::ProgramIdIsFeePayer {
            instruction: instruction_index,
        });
    }
    if is_v0 && usize::from(instruction.program_id_index) >= static_account_count {
        return Err(SignedMessageError::V0ProgramIdIsLoaded {
            instruction: instruction_index,
            index: instruction.program_id_index,
            static_account_count,
        });
    }
    for &index in instruction.accounts {
        if usize::from(index) >= account_count {
            return Err(SignedMessageError::MessageAccountIndexOutOfBounds {
                instruction: instruction_index,
                field: "account",
                index,
                account_count,
            });
        }
    }
    Ok(())
}

fn push_message_short_vec_len(
    out: &mut Vec<u8>,
    len: usize,
    field: &'static str,
) -> Result<(), SignedMessageError> {
    let mut value = u16::try_from(len)
        .map_err(|_| SignedMessageError::ShortVecLengthTooLarge { field, actual: len })?;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

fn increment_candidate_choices(
    choices: &mut [usize],
    instructions: &[SignedInstructionCandidates<'_>],
) {
    for (choice, instruction) in choices.iter_mut().zip(instructions).rev() {
        *choice += 1;
        if *choice < instruction.data_candidates.len() {
            return;
        }
        *choice = 0;
    }
    unreachable!("the caller increments only before the final combination");
}

impl VoteHashRegistry {
    /// Decode and validate a complete `vote_hash_registry.bin` image.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SignedMessageError> {
        if bytes.len() > MAX_VOTE_HASH_REGISTRY_BYTES {
            return Err(SignedMessageError::VoteHashRegistryByteLimit {
                actual: bytes.len(),
                maximum: MAX_VOTE_HASH_REGISTRY_BYTES,
            });
        }
        if !bytes.len().is_multiple_of(VOTE_HASH_RECORD_LEN) {
            return Err(SignedMessageError::InvalidVoteHashRegistryLength {
                actual: bytes.len(),
            });
        }
        let records = bytes.len() / VOTE_HASH_RECORD_LEN;
        let mut rows = Vec::new();
        rows.try_reserve_exact(records)
            .map_err(|_| SignedMessageError::VoteHashRegistryAllocation { records })?;
        for (block_id, chunk) in bytes.chunks_exact(VOTE_HASH_RECORD_LEN).enumerate() {
            let block_id = u32::try_from(block_id).map_err(|_| {
                SignedMessageError::VoteHashRegistryTooLarge {
                    records: bytes.len() / VOTE_HASH_RECORD_LEN,
                }
            })?;
            let flags = chunk[0];
            if flags & !0b11 != 0 {
                return Err(SignedMessageError::InvalidVoteHashRegistryFlags { block_id, flags });
            }
            let bank_hash = (flags & 1 != 0).then(|| {
                chunk[1..33]
                    .try_into()
                    .expect("checked vote bank-hash record")
            });
            let block_id_hash = (flags & 2 != 0).then(|| {
                chunk[33..65]
                    .try_into()
                    .expect("checked vote block-id-hash record")
            });
            rows.push(VoteHashRow {
                bank_hash,
                block_id_hash,
            });
        }
        Ok(Self { rows })
    }
}

impl VoteHashResolver for VoteHashRegistry {
    fn resolve_vote_hash(
        &self,
        block_id: u32,
        kind: VoteHashKind,
    ) -> Result<[u8; 32], SignedMessageError> {
        let row = usize::try_from(block_id)
            .ok()
            .and_then(|index| self.rows.get(index))
            .ok_or(SignedMessageError::MissingVoteHash { block_id, kind })?;
        let value = match kind {
            VoteHashKind::Bank => row.bank_hash,
            VoteHashKind::BlockId => row.block_id_hash,
        };
        value.ok_or(SignedMessageError::MissingVoteHash { block_id, kind })
    }
}

/// Reconstruct the exact on-chain bytes of an unambiguous Compact V2
/// top-level instruction.
///
/// Raw variants pass through unchanged. Compute-budget and System variants
/// use their canonical wire forms. Typed vote variants resolve hashes that
/// Compact V2 moved to `vote_hash_registry.bin`.
///
/// Compact V2 maps both the canonical and historical TowerSync wire forms to
/// the same value. This function rejects those ambiguous values. Use
/// [`reconstruct_instruction_data_candidates`] and a transaction signature to
/// select the correct candidate.
pub fn reconstruct_instruction_data(
    data: &ArchiveV2HotInstructionData,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SignedMessageError> {
    let mut candidates = reconstruct_instruction_data_candidates(data, vote_hashes)?;
    if candidates.len() != 1 {
        return Err(SignedMessageError::AmbiguousInstructionEncoding {
            candidates: candidates.len(),
        });
    }
    Ok(candidates
        .pop()
        .expect("one checked candidate")
        .bytes
        .into_owned())
}

/// Return all possible exact on-chain byte forms retained by Compact V2.
///
/// TowerSync values have two candidates because the source parser accepted a
/// historical bincode form and then erased the form marker. A caller must
/// rebuild the signed message with each candidate and use the first required
/// signature as the oracle. It must not select a form by preference.
pub fn reconstruct_instruction_data_candidates(
    data: &ArchiveV2HotInstructionData,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<InstructionDataCandidate<'static>>, SignedMessageError> {
    match data {
        ArchiveV2HotInstructionData::Raw(bytes)
        | ArchiveV2HotInstructionData::UnknownSystem(bytes)
        | ArchiveV2HotInstructionData::UnknownVote(bytes) => Ok(vec![InstructionDataCandidate {
            encoding: InstructionDataEncoding::Raw,
            bytes: bytes.clone().into(),
        }]),
        ArchiveV2HotInstructionData::ComputeBudget(value) => {
            let mut out = Vec::with_capacity(9);
            match value {
                ArchiveV2ComputeBudgetInstructionData::Unused => out.push(0),
                ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(bytes) => {
                    out.push(1);
                    out.extend_from_slice(&bytes.to_le_bytes());
                }
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(units) => {
                    out.push(2);
                    out.extend_from_slice(&units.to_le_bytes());
                }
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(price) => {
                    out.push(3);
                    out.extend_from_slice(&price.to_le_bytes());
                }
                ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(bytes) => {
                    out.push(4);
                    out.extend_from_slice(&bytes.to_le_bytes());
                }
            }
            Ok(vec![InstructionDataCandidate {
                encoding: InstructionDataEncoding::ComputeBudget,
                bytes: out.into(),
            }])
        }
        ArchiveV2HotInstructionData::System(value) => Ok(vec![InstructionDataCandidate {
            encoding: InstructionDataEncoding::System,
            bytes: system_instruction_bytes(value)?.into(),
        }]),
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
            Ok(vec![InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteCompact,
                bytes: vote_update_instruction_bytes(12, update, vote_hashes)?.into(),
            }])
        }
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
            update,
            switch_proof_hash,
        } => {
            let mut out = vote_update_instruction_bytes(13, update, vote_hashes)?;
            out.extend_from_slice(&resolve_aux_hash(*switch_proof_hash)?);
            Ok(vec![InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteCompact,
                bytes: out.into(),
            }])
        }
        ArchiveV2HotInstructionData::VoteTowerSync(tower) => {
            tower_candidates(14, tower, None, vote_hashes)
        }
        ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
            tower,
            switch_proof_hash,
        } => tower_candidates(15, tower, Some(*switch_proof_hash), vote_hashes),
    }
}

fn tower_candidates(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    switch_proof_hash: Option<ArchiveV2VoteHashRef>,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<InstructionDataCandidate<'static>>, SignedMessageError> {
    let switch_proof_hash = switch_proof_hash.map(resolve_aux_hash).transpose()?;
    let mut canonical = vote_tower_sync_instruction_bytes(variant, tower, vote_hashes)?;
    if let Some(hash) = switch_proof_hash {
        canonical.extend_from_slice(&hash);
    }
    let mut historical = historical_tower_sync_instruction_bytes(variant, tower, vote_hashes)?;
    if let Some(hash) = switch_proof_hash {
        historical.extend_from_slice(&hash);
    }
    let mut candidates = vec![InstructionDataCandidate {
        encoding: InstructionDataEncoding::VoteTowerCanonical,
        bytes: canonical.into(),
    }];
    if candidates[0].bytes != historical {
        candidates.push(InstructionDataCandidate {
            encoding: InstructionDataEncoding::VoteTowerHistorical,
            bytes: historical.into(),
        });
    }
    Ok(candidates)
}

fn resolve_vote_hash_ref(
    value: ArchiveV2VoteHashRef,
    kind: VoteHashKind,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<[u8; 32], SignedMessageError> {
    match value {
        ArchiveV2VoteHashRef::Zero => Ok([0; 32]),
        ArchiveV2VoteHashRef::Raw(hash) => Ok(hash),
        ArchiveV2VoteHashRef::Block(block_id) => vote_hashes
            .ok_or(SignedMessageError::MissingVoteHashResolver { block_id, kind })?
            .resolve_vote_hash(block_id, kind),
    }
}

fn resolve_aux_hash(value: ArchiveV2VoteHashRef) -> Result<[u8; 32], SignedMessageError> {
    match value {
        ArchiveV2VoteHashRef::Zero => Ok([0; 32]),
        ArchiveV2VoteHashRef::Raw(hash) => Ok(hash),
        ArchiveV2VoteHashRef::Block(block_id) => {
            Err(SignedMessageError::AuxiliaryVoteHashBlockReference { block_id })
        }
    }
}

fn vote_update_instruction_bytes(
    variant: u32,
    update: &ArchiveV2VoteStateUpdate,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SignedMessageError> {
    let mut out = Vec::with_capacity(128);
    push_u32_le(&mut out, variant);
    push_u64_le(&mut out, update.root.unwrap_or(u64::MAX));
    push_short_vec_len(&mut out, update.lockout_offsets.len())?;
    for lockout in &update.lockout_offsets {
        push_var_u64(&mut out, lockout.offset);
        out.push(lockout.confirmation_count);
    }
    out.extend_from_slice(&resolve_vote_hash_ref(
        update.hash,
        VoteHashKind::Bank,
        vote_hashes,
    )?);
    push_option_i64(&mut out, update.timestamp);
    Ok(out)
}

fn vote_tower_sync_instruction_bytes(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SignedMessageError> {
    let mut out = vote_update_instruction_bytes(variant, &tower.update, vote_hashes)?;
    out.extend_from_slice(&resolve_vote_hash_ref(
        tower.block_id_hash,
        VoteHashKind::BlockId,
        vote_hashes,
    )?);
    Ok(out)
}

fn historical_tower_sync_instruction_bytes(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SignedMessageError> {
    let mut out = Vec::with_capacity(160);
    push_u32_le(&mut out, variant);
    let lockout_count = u64::try_from(tower.update.lockout_offsets.len()).map_err(|_| {
        SignedMessageError::VoteLockoutCountTooLarge {
            actual: tower.update.lockout_offsets.len(),
        }
    })?;
    push_u64_le(&mut out, lockout_count);
    let mut slot = tower.update.root.unwrap_or_default();
    for lockout in &tower.update.lockout_offsets {
        slot = slot.checked_add(lockout.offset).ok_or(
            SignedMessageError::VoteLockoutSlotOverflow {
                previous: slot,
                offset: lockout.offset,
            },
        )?;
        push_u64_le(&mut out, slot);
        push_u32_le(&mut out, u32::from(lockout.confirmation_count));
    }
    push_option_u64(&mut out, tower.update.root);
    out.extend_from_slice(&resolve_vote_hash_ref(
        tower.update.hash,
        VoteHashKind::Bank,
        vote_hashes,
    )?);
    push_option_i64(&mut out, tower.update.timestamp);
    out.extend_from_slice(&resolve_vote_hash_ref(
        tower.block_id_hash,
        VoteHashKind::BlockId,
        vote_hashes,
    )?);
    Ok(out)
}

fn push_short_vec_len(out: &mut Vec<u8>, mut len: usize) -> Result<(), SignedMessageError> {
    if len > usize::from(u16::MAX) {
        return Err(SignedMessageError::VoteLockoutCountTooLarge { actual: len });
    }
    loop {
        let mut byte = (len & 0x7f) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            return Ok(());
        }
    }
}

fn push_var_u64(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn push_option_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn push_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn system_instruction_bytes(
    data: &ArchiveV2SystemInstructionData,
) -> Result<Vec<u8>, SignedMessageError> {
    use ArchiveV2SystemInstructionData as SystemIx;

    let mut out = Vec::with_capacity(96);
    match data {
        SystemIx::CreateAccount {
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 0);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::Assign { owner } => {
            push_u32_le(&mut out, 1);
            out.extend_from_slice(owner);
        }
        SystemIx::Transfer { lamports } => {
            push_u32_le(&mut out, 2);
            push_u64_le(&mut out, *lamports);
        }
        SystemIx::CreateAccountWithSeed {
            base,
            seed,
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 3);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed)?;
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::AdvanceNonceAccount => push_u32_le(&mut out, 4),
        SystemIx::WithdrawNonceAccount { lamports } => {
            push_u32_le(&mut out, 5);
            push_u64_le(&mut out, *lamports);
        }
        SystemIx::InitializeNonceAccount { authority } => {
            push_u32_le(&mut out, 6);
            out.extend_from_slice(authority);
        }
        SystemIx::AuthorizeNonceAccount { authority } => {
            push_u32_le(&mut out, 7);
            out.extend_from_slice(authority);
        }
        SystemIx::Allocate { space } => {
            push_u32_le(&mut out, 8);
            push_u64_le(&mut out, *space);
        }
        SystemIx::AllocateWithSeed {
            base,
            seed,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 9);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed)?;
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::AssignWithSeed { base, seed, owner } => {
            push_u32_le(&mut out, 10);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed)?;
            out.extend_from_slice(owner);
        }
        SystemIx::TransferWithSeed {
            lamports,
            from_seed,
            from_owner,
        } => {
            push_u32_le(&mut out, 11);
            push_u64_le(&mut out, *lamports);
            push_system_seed(&mut out, from_seed)?;
            out.extend_from_slice(from_owner);
        }
        SystemIx::UpgradeNonceAccount => push_u32_le(&mut out, 12),
        SystemIx::CreateAccountAllowPrefund {
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 13);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
    }
    Ok(out)
}

#[inline]
fn push_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn push_u64_le(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_system_seed(out: &mut Vec<u8>, seed: &str) -> Result<(), SignedMessageError> {
    let len = u64::try_from(seed.len())
        .map_err(|_| SignedMessageError::SystemSeedTooLong { actual: seed.len() })?;
    push_u64_le(out, len);
    out.extend_from_slice(seed.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    #[test]
    fn short_vector_lengths_match_canonical_boundaries() {
        for (length, expected) in [
            (0, vec![0]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (16_383, vec![0xff, 0x7f]),
            (16_384, vec![0x80, 0x80, 0x01]),
            (65_535, vec![0xff, 0xff, 0x03]),
        ] {
            let mut bytes = Vec::new();
            push_message_short_vec_len(&mut bytes, length, "fixture").unwrap();
            assert_eq!(bytes, expected, "length {length}");
        }

        assert_eq!(
            push_message_short_vec_len(&mut Vec::new(), 65_536, "fixture"),
            Err(SignedMessageError::ShortVecLengthTooLarge {
                field: "fixture",
                actual: 65_536,
            })
        );
    }

    #[test]
    fn candidate_selection_uses_one_exact_whole_message() {
        let keys = [[1; 32], [9; 32]];
        let choices = [
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerCanonical,
                bytes: vec![0xaa].into(),
            },
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerHistorical,
                bytes: vec![0xbb].into(),
            },
        ];
        let candidate_instructions = [SignedInstructionCandidates {
            program_id_index: 1,
            accounts: &[0],
            data_candidates: &choices,
        }];
        let message = SignedMessageCandidates {
            version: SignedMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [2; 32],
            instructions: &candidate_instructions,
        };
        let selected_instruction = [SignedInstruction {
            program_id_index: 1,
            accounts: &[0],
            data: &[0xbb],
        }];
        let expected_message = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header: message.header,
            static_account_keys: &keys,
            recent_blockhash: [2; 32],
            instructions: &selected_instruction,
        })
        .unwrap();

        let selected = select_signed_message_candidate(&message, 2, |bytes| {
            bytes == expected_message.as_slice()
        })
        .unwrap();
        assert_eq!(selected.instruction_data, vec![vec![0xbb]]);
        assert_eq!(selected.signed_message, expected_message);
    }

    #[test]
    fn typed_instruction_candidates_use_exact_wire_bytes() {
        let raw = ArchiveV2HotInstructionData::UnknownSystem(vec![9, 8, 7]);
        assert_eq!(reconstruct_instruction_data(&raw, None).unwrap(), [9, 8, 7]);

        let price = ArchiveV2HotInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(10_000),
        );
        assert_eq!(
            reconstruct_instruction_data(&price, None).unwrap(),
            [3, 0x10, 0x27, 0, 0, 0, 0, 0, 0]
        );

        let transfer =
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports: 0x0102,
            });
        let mut expected = 2u32.to_le_bytes().to_vec();
        expected.extend_from_slice(&0x0102u64.to_le_bytes());
        assert_eq!(
            reconstruct_instruction_data(&transfer, None).unwrap(),
            expected
        );

        assert!(matches!(
            VoteHashRegistry::from_bytes(&[4; VOTE_HASH_RECORD_LEN]),
            Err(SignedMessageError::InvalidVoteHashRegistryFlags {
                block_id: 0,
                flags: 4
            })
        ));
    }

    #[test]
    fn ed25519_verifier_checks_every_signer() {
        let public_key: [u8; 32] =
            hex_bytes("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
                .try_into()
                .unwrap();
        let signature: [u8; 64] = hex_bytes(
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155\
             5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
        )
        .try_into()
        .unwrap();

        assert_eq!(
            verify_ed25519_signatures(b"", &[public_key, public_key], &[signature, signature]),
            Ok(())
        );

        let mut invalid_second = signature;
        invalid_second[0] ^= 1;
        assert_eq!(
            verify_ed25519_signatures(b"", &[public_key, public_key], &[signature, invalid_second],),
            Err(SignedMessageError::SignatureVerificationFailed { signer_index: 1 })
        );
    }

    #[test]
    fn ed25519_candidate_selection_uses_first_then_checks_all() {
        let first = SigningKey::from_bytes(&[1; 32]);
        let second = SigningKey::from_bytes(&[2; 32]);
        let keys = [
            first.verifying_key().to_bytes(),
            second.verifying_key().to_bytes(),
            [9; 32],
        ];
        let choices = [
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::Raw,
                bytes: vec![0xaa].into(),
            },
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::Raw,
                bytes: vec![0xbb].into(),
            },
        ];
        let candidate_instructions = [SignedInstructionCandidates {
            program_id_index: 2,
            accounts: &[0],
            data_candidates: &choices,
        }];
        let message = SignedMessageCandidates {
            version: SignedMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            static_account_keys: &keys,
            recent_blockhash: [3; 32],
            instructions: &candidate_instructions,
        };
        let selected_instruction = [SignedInstruction {
            program_id_index: 2,
            accounts: &[0],
            data: &[0xbb],
        }];
        let expected = serialize_signed_message(&SignedMessage {
            version: message.version,
            header: message.header,
            static_account_keys: &keys,
            recent_blockhash: message.recent_blockhash,
            instructions: &selected_instruction,
        })
        .unwrap();
        let first_signature = first.sign(&expected).to_bytes();
        let second_signature = second.sign(&expected).to_bytes();

        let selected = select_signed_message_candidate_ed25519(
            &message,
            2,
            &[first_signature, second_signature],
        )
        .unwrap();
        assert_eq!(selected.signed_message, expected);
        assert_eq!(selected.instruction_data, vec![vec![0xbb]]);

        let mut invalid_second = second_signature;
        invalid_second[0] ^= 1;
        assert_eq!(
            select_signed_message_candidate_ed25519(
                &message,
                2,
                &[first_signature, invalid_second],
            ),
            Err(SignedMessageError::SignatureVerificationFailed { signer_index: 1 })
        );
    }

    #[test]
    fn ed25519_verifier_rejects_empty_and_unpaired_proofs() {
        assert_eq!(
            verify_ed25519_signatures(b"message", &[], &[]),
            Err(SignedMessageError::NoSignatures)
        );
        assert_eq!(
            verify_ed25519_signatures(b"message", &[[0; 32]], &[]),
            Err(SignedMessageError::NoSignatures)
        );
        assert_eq!(
            verify_ed25519_signatures(b"message", &[[0; 32]], &[[0; 64], [0; 64]]),
            Err(SignedMessageError::SignerSignatureCountMismatch {
                signer_pubkeys: 1,
                signatures: 2,
            })
        );
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        value
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }
}
