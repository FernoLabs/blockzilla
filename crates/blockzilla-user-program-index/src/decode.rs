//! Hand-written, zero-copy decoders for the specific fields this user-program index
//! actually reads out of an Archive V2 Compact message and transaction
//! metadata.
//!
//! `blockzilla_format`'s derived `SchemaRead` impls for
//! `ArchiveV2HotInstructionData` and `CompactMetaV1` are correct but decode
//! (and heap-allocate) *every* field, including ones this indexer never
//! reads: raw instruction bytes for every non-system/vote/compute-budget
//! instruction (i.e. every token/DeFi call), full transaction logs, token
//! balances, rewards. A CPU profile of the original fully-owned decode path
//! confirmed this is real cost, not a theoretical one: allocator overhead
//! alone was ~21% of total samples, with instruction/vote-variant decoding
//! contributing a further ~25-30%.
//!
//! This module decodes the *identical* wire format field-by-field —
//! reusing `blockzilla_format`'s own types' `SchemaRead` impls for every
//! field this indexer actually needs — but **skips** (bounds-checked,
//! zero-copy, via `wincode::io::Reader::take_borrowed`) every raw-byte-blob
//! field it doesn't: instruction `data` payloads, inner-instruction `data`
//! payloads, and (for legacy messages, which have no address-table lookups
//! to resolve) the entire metadata tail past `inner_instructions`.
//!
//! ## Wire format, verified against source
//!
//! Nothing here is guessed. Every mechanic below was confirmed by reading
//! wincode 0.5.5's own source (not just its docs), specifically:
//! - Enum discriminants are a `u32` (the default `TagEncoding`, since
//!   `ArchiveV2HotInstructionData` has no `#[wincode(tag = ...)]` override),
//!   0-based by variant *declaration order*, decoded via the configured
//!   `IntEncoding` like any other integer (`wincode-derive`'s `impl_enum` in
//!   `schema_read.rs`; `Configuration`'s `TagEncoding = u32` default in
//!   `config/mod.rs`).
//! - `Vec<T>`'s length prefix is a `u64` decoded via the configured
//!   `IntEncoding` (`BincodeLen = UseIntLen<u64, _>`, whose `read` is
//!   exactly `u64::get` then `usize::try_from` — `len.rs`).
//! - `Option<T>` is a `u8` tag (0 = None, 1 = Some) followed by `T` if Some
//!   (`schema/impls.rs`).
//! - `&'de [u8]` implements `Reader<'de>` directly, and
//!   `take_borrowed(len)` both advances the cursor and returns a `&'de [u8]`
//!   slice with **zero allocation** (`io/slice.rs`).
//!
//! Any future change to `ArchiveV2HotInstructionData` or `CompactMetaV1`'s
//! variant order/field order breaks this module silently (wrong bytes
//! skipped, not a panic). Verified two ways: the `tests` module below
//! round-trips synthetic messages/metadata through `blockzilla_format`'s
//! own `SchemaWrite` and asserts this module decodes them correctly
//! (catches wire-format drift for the shapes it covers); separately, a full
//! `build` run against real mainnet data produced byte-identical output
//! files to the original fully-owned-decode implementation across every
//! transaction in the sample (catches anything the synthetic cases miss,
//! but isn't a repeatable regression test — it depended on a local fixture
//! outside this repo).
//!
//! One real bug was caught this way during development: `decode_signers`
//! initially forgot `account_keys`'s `Vec<CompactPubkey>` length prefix
//! before reading its elements, silently desyncing every read after the
//! message header. It surfaced as an implausible result (fewer distinct
//! signers than a real build's signer user count, which should be impossible),
//! not a crash — exactly the failure mode this module's docs warn about,
//! and why the tests below check exact decoded values, not just "did it
//! return without erroring."

use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2VoteHashRef, CompactInnerInstruction,
    CompactMessageHeader, CompactPubkey, CompactReward, CompactTokenBalance,
    CompactTransactionConfig, DataArray, OwnedCompactRecentBlockhash, WincodeLeb128Config,
};
use blockzilla_compact_v2_reader::{CompactV2MessageSchema, CompactV2MetadataSchema};
use wincode::{ReadResult, SchemaRead, error::invalid_tag_encoding, io::Reader};

pub type Cfg = WincodeLeb128Config;

/// Every message account is addressed by a one-byte instruction index.
/// This includes static and address-table-loaded accounts together.
pub const MAX_MESSAGE_ACCOUNTS: usize = u8::MAX as usize + 1;

#[inline]
fn get<'de, T: SchemaRead<'de, Cfg>>(cursor: &mut &'de [u8]) -> ReadResult<T::Dst> {
    T::get(&mut *cursor)
}

/// `Vec<_>`'s `u64` LEB128 length prefix (element/byte count, shared by
/// every sequence type in this wire format).
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

/// Bound a sequence count by the bytes still present. Every sequence item
/// decoded by callers consumes at least one byte, so a larger count can
/// never be valid and must not drive an attacker-controlled loop.
#[inline]
fn read_len_bounded_by_remaining(cursor: &mut &[u8], error: &'static str) -> ReadResult<usize> {
    let remaining = cursor.len();
    read_bounded_len(cursor, remaining, error)
}

/// Skip a `Vec<u8>` (length prefix + raw bytes) without allocating.
#[inline]
fn skip_bytes(cursor: &mut &[u8]) -> ReadResult<()> {
    let len = read_len_bounded_by_remaining(cursor, "byte string length exceeds remaining input")?;
    cursor.take_borrowed(len)?;
    Ok(())
}

/// Skip a wincode `String` without allocating, while preserving the
/// canonical decoder's UTF-8 validation.
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

fn skip_vote_state_update(cursor: &mut &[u8]) -> ReadResult<()> {
    get::<Option<u64>>(cursor)?; // root
    // Every lockout consumes at least one byte for its offset and one for its
    // confirmation count, so even the loop count is bounded by real input.
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
    get::<ArchiveV2VoteHashRef>(cursor)?;
    get::<Option<i64>>(cursor)?;
    Ok(())
}

fn skip_vote_tower_sync(cursor: &mut &[u8]) -> ReadResult<()> {
    skip_vote_state_update(cursor)?;
    get::<ArchiveV2VoteHashRef>(cursor)?;
    Ok(())
}

/// Skip one `ArchiveV2HotInstructionData` value (tag + payload), never
/// allocating for the `Raw`/`UnknownSystem`/`UnknownVote` catch-all variants
/// — which is what every instruction for a program `blockzilla_format`
/// doesn't specifically decode (i.e. every token/DeFi program) uses.
fn skip_instruction_data(
    cursor: &mut &[u8],
    message_schema: CompactV2MessageSchema,
) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    if message_schema == CompactV2MessageSchema::May24PreUnknownFallbacks {
        match tag {
            0 => skip_bytes(cursor)?,
            1 => {
                get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
            }
            2 => skip_system_instruction_data(cursor)?,
            3 => skip_vote_state_update(cursor)?,
            4 => {
                skip_vote_state_update(cursor)?;
                get::<ArchiveV2VoteHashRef>(cursor)?;
            }
            5 => skip_vote_tower_sync(cursor)?,
            6 => {
                skip_vote_tower_sync(cursor)?;
                get::<ArchiveV2VoteHashRef>(cursor)?;
            }
            other => return Err(invalid_tag_encoding(other as usize)),
        }
        return Ok(());
    }
    match tag {
        0..=2 => skip_bytes(cursor)?, // Raw | UnknownSystem | UnknownVote
        3 => {
            get::<ArchiveV2ComputeBudgetInstructionData>(cursor)?;
        }
        4 => {
            skip_system_instruction_data(cursor)?;
        }
        5 => {
            skip_vote_state_update(cursor)?;
        }
        6 => {
            skip_vote_state_update(cursor)?;
            get::<ArchiveV2VoteHashRef>(cursor)?;
        }
        7 => {
            skip_vote_tower_sync(cursor)?;
        }
        8 => {
            skip_vote_tower_sync(cursor)?;
            get::<ArchiveV2VoteHashRef>(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

/// One decoded top-level instruction: `program_id_index` plus a borrowed
/// (zero-copy) slice of account indices. `data` has already been skipped.
pub struct BorrowedInstruction<'de> {
    pub program_id_index: u8,
    pub accounts: &'de [u8],
}

fn read_instruction<'de>(
    cursor: &mut &'de [u8],
    message_schema: CompactV2MessageSchema,
) -> ReadResult<BorrowedInstruction<'de>> {
    let program_id_index = get::<u8>(cursor)?;
    // Repeated account indices are legal, so this slice's *length* is not
    // bounded by the number of distinct message accounts. It is still
    // strictly bounded by the bytes available and remains borrowed.
    let accounts_len = read_len_bounded_by_remaining(
        cursor,
        "instruction account-index count exceeds remaining input",
    )?;
    let accounts = cursor.take_borrowed(accounts_len)?;
    skip_instruction_data(cursor, message_schema)?;
    Ok(BorrowedInstruction {
        program_id_index,
        accounts,
    })
}

/// Decode *only* enough of a message to learn its signers: the version tag,
/// the header (for `num_required_signatures`), and that many `CompactPubkey`
/// entries off the front of `account_keys`. Doesn't touch the rest of
/// `account_keys`, `recent_blockhash`, or any instruction — the cursor is
/// left mid-message on return, which is fine here (the caller isn't
/// decoding anything else from these particular bytes). Used to cheaply
/// discover the real signer population of an epoch (see
/// `build::discover_signers`) without paying for a full instruction decode.
pub fn decode_signers(cursor: &mut &[u8]) -> ReadResult<SignerKeys> {
    decode_signers_with_schema(cursor, CompactV2MessageSchema::Current)
}

/// Decode required signers under one explicitly selected message grammar.
pub fn decode_signers_with_schema(
    cursor: &mut &[u8],
    message_schema: CompactV2MessageSchema,
) -> ReadResult<SignerKeys> {
    let is_v1 = match (message_schema, get::<u32>(cursor)?) {
        (_, 0 | 1) => false,
        (CompactV2MessageSchema::Current, 2) => true,
        (_, other) => return Err(invalid_tag_encoding(other as usize)),
    };
    let header = get::<CompactMessageHeader>(cursor)?;
    if is_v1 {
        get::<CompactTransactionConfig>(cursor)?;
    }
    // `account_keys: Vec<CompactPubkey>`'s length prefix must still be
    // consumed before its elements, even though we only want the first
    // `num_required_signatures` of them — Vec's wire format is always
    // [u64 LEB128 length][element]*length, elements aren't independently
    // addressable without it.
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

/// Most transactions have exactly one signer (the fee payer); a handful
/// need more. Inline storage covers the common case with no allocation.
pub type SignerKeys = smallvec::SmallVec<[CompactPubkey; 2]>;

/// A decoded message's fields, up through `instructions` — mirrors
/// `ArchiveV2HotLegacyMessage`/`ArchiveV2HotV0Message` exactly, in field
/// order, calling `on_instruction` for each top-level instruction as it's
/// decoded (no `Vec<Instruction>` is ever materialized).
///
/// Returns the static account keys, message shape, and exact writable/
/// readonly address counts declared by V0 address-table lookups.
/// `num_required_signatures` is the count of *signer* positions at the
/// front of `account_keys` (Solana's standard message layout: signed
/// accounts first, then unsigned; V0's appended loaded addresses are never
/// signers) — callers use it to tell a real signer user from a merely-
/// referenced account. For a V0 message, `address_table_lookups` is streamed
/// far enough to validate and count its writable/readonly indices (its
/// resolved form lives in transaction metadata), and the cursor is positioned
/// exactly at the end of the message, same as the legacy case — callers
/// that want to assert full consumption can rely on that.
pub struct DecodedMessage {
    pub account_keys: Vec<CompactPubkey>,
    pub is_v0: bool,
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub instruction_count: usize,
    pub expected_loaded_writable: usize,
    pub expected_loaded_readonly: usize,
}

/// One account-bearing event from the borrowed message traversal.
pub enum MessageAccountEvent<'de> {
    StaticAccountCount(usize),
    StaticAccount {
        source_position: usize,
        key: CompactPubkey,
    },
    Instruction(BorrowedInstruction<'de>),
}

/// Message shape retained after the streaming account traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamedMessageShape {
    pub static_account_count: usize,
    pub is_v0: bool,
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub instruction_count: usize,
    pub expected_loaded_writable: usize,
    pub expected_loaded_readonly: usize,
}

pub fn decode_message<'de>(
    cursor: &mut &'de [u8],
    on_instruction: impl FnMut(BorrowedInstruction<'de>),
) -> ReadResult<DecodedMessage> {
    decode_message_with_schema(cursor, CompactV2MessageSchema::Current, on_instruction)
}

/// Decode the account-bearing message fields under one explicitly selected
/// Compact V2 message grammar.
pub fn decode_message_with_schema<'de>(
    cursor: &mut &'de [u8],
    message_schema: CompactV2MessageSchema,
    mut on_instruction: impl FnMut(BorrowedInstruction<'de>),
) -> ReadResult<DecodedMessage> {
    let mut account_keys = Vec::new();
    let shape = stream_message_accounts_with_schema(cursor, message_schema, |event| {
        match event {
            MessageAccountEvent::StaticAccountCount(count) => {
                account_keys.reserve_exact(count);
            }
            MessageAccountEvent::StaticAccount { key, .. } => account_keys.push(key),
            MessageAccountEvent::Instruction(instruction) => on_instruction(instruction),
        }
        Ok::<(), wincode::error::ReadError>(())
    })?;
    Ok(DecodedMessage {
        account_keys,
        is_v0: shape.is_v0,
        num_required_signatures: shape.num_required_signatures,
        num_readonly_signed_accounts: shape.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: shape.num_readonly_unsigned_accounts,
        instruction_count: shape.instruction_count,
        expected_loaded_writable: shape.expected_loaded_writable,
        expected_loaded_readonly: shape.expected_loaded_readonly,
    })
}

/// Traverse message accounts and borrowed instruction indexes without
/// allocating an owned message-account lane.
pub fn stream_message_accounts_with_schema<'de, E>(
    cursor: &mut &'de [u8],
    message_schema: CompactV2MessageSchema,
    mut on_event: impl FnMut(MessageAccountEvent<'de>) -> Result<(), E>,
) -> Result<StreamedMessageShape, E>
where
    E: From<wincode::error::ReadError> + From<wincode::io::ReadError>,
{
    let message_tag = get::<u32>(cursor)?;
    let (is_v0, is_v1) = match (message_schema, message_tag) {
        (_, 0) => (false, false),
        (_, 1) => (true, false),
        (CompactV2MessageSchema::Current, 2) => (false, true),
        (_, other) => return Err(invalid_tag_encoding(other as usize).into()),
    };
    let header = get::<CompactMessageHeader>(cursor)?;
    if is_v1 {
        get::<CompactTransactionConfig>(cursor)?;
    }
    let account_key_count = read_bounded_len(
        cursor,
        MAX_MESSAGE_ACCOUNTS,
        "static account key count exceeds message account cap",
    )?;
    on_event(MessageAccountEvent::StaticAccountCount(account_key_count))?;
    for source_position in 0..account_key_count {
        on_event(MessageAccountEvent::StaticAccount {
            source_position,
            key: get::<CompactPubkey>(cursor)?,
        })?;
    }
    get::<OwnedCompactRecentBlockhash>(cursor)?;

    let instruction_count = read_len_bounded_by_remaining(
        cursor,
        "top-level instruction count exceeds remaining input",
    )?;
    for _ in 0..instruction_count {
        on_event(MessageAccountEvent::Instruction(read_instruction(
            cursor,
            message_schema,
        )?))?;
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
            get::<CompactPubkey>(cursor)?; // lookup table account key
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
                )
                .into());
            }
        }
    }

    Ok(StreamedMessageShape {
        static_account_count: account_key_count,
        is_v0,
        num_required_signatures: header.num_required_signatures,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
        instruction_count,
        expected_loaded_writable,
        expected_loaded_readonly,
    })
}

/// One decoded inner instruction: `program_id_index` plus a borrowed slice
/// of account indices. `data` and `stack_height` have already been skipped
/// / discarded.
pub struct BorrowedInnerInstruction<'de> {
    pub program_id_index: u32,
    pub accounts: &'de [u8],
}

/// One account-bearing event from the borrowed metadata traversal.
pub enum MetadataAccountEvent<'de> {
    InnerInstruction(BorrowedInnerInstruction<'de>),
    LoadedWritableCount(usize),
    LoadedWritable(CompactPubkey),
    LoadedReadonlyCount(usize),
    LoadedReadonly(CompactPubkey),
}

/// Metadata shape retained after the streaming account traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamedMetadataShape {
    pub has_error: bool,
    pub inner_instructions_present: bool,
    pub loaded_writable_count: usize,
    pub loaded_readonly_count: usize,
}

/// Exact borrowed Compact V2 metadata field ranges for the source-split
/// canary. Every range includes its source Wincode tag or length prefix.
/// The two outcome ranges are non-contiguous in `CompactMetaV1` and must be
/// length-framed before they are joined in a derived output record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedMetadataEffectFields<'de> {
    pub outcome_head: &'de [u8],
    pub pre_balances: &'de [u8],
    pub post_balances: &'de [u8],
    pub inner_instructions: &'de [u8],
    pub logs: &'de [u8],
    pub pre_token_balances: &'de [u8],
    pub post_token_balances: &'de [u8],
    pub transaction_rewards: &'de [u8],
    pub loaded_writable: &'de [u8],
    pub loaded_readonly: &'de [u8],
    pub outcome_tail: &'de [u8],
}

/// Shape and exact field ranges from one complete decoded metadata row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamedMetadataEffects<'de> {
    pub shape: StreamedMetadataShape,
    pub fields: BorrowedMetadataEffectFields<'de>,
    pub inner_group_count: usize,
    pub logs_present: bool,
    pub pre_token_balance_count: usize,
    pub post_token_balance_count: usize,
    pub transaction_reward_count: usize,
}

pub struct DecodedMetadataPrefix {
    pub has_error: bool,
    pub inner_instructions_present: bool,
    pub loaded_addresses: Option<(Vec<CompactPubkey>, Vec<CompactPubkey>)>,
}

/// Decode just the archived transaction outcome. This is sufficient when the
/// row flags prove there are neither inner instructions nor loaded addresses.
pub fn decode_metadata_error(cursor: &mut &[u8]) -> ReadResult<bool> {
    decode_metadata_error_with_schema(cursor, CompactV2MetadataSchema::CurrentTypedError)
}

/// Decode only the outcome under one explicitly selected metadata grammar.
pub fn decode_metadata_error_with_schema(
    cursor: &mut &[u8],
    metadata_schema: CompactV2MetadataSchema,
) -> ReadResult<bool> {
    match get::<u8>(cursor)? {
        0 => Ok(false),
        1 => {
            match metadata_schema {
                CompactV2MetadataSchema::CurrentTypedError => skip_transaction_error(cursor)?,
                CompactV2MetadataSchema::LegacyRawError => skip_bytes(cursor)?,
            }
            Ok(true)
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_transaction_error(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u8>(cursor)?;
    match tag {
        8 => {
            get::<u8>(cursor)?; // instruction index
            skip_instruction_error(cursor)?;
        }
        30 | 31 | 35 => {
            get::<u8>(cursor)?;
        }
        0..=38 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_instruction_error(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u8>(cursor)?;
    match tag {
        25 => {
            get::<u32>(cursor)?;
        }
        44 => skip_string(cursor)?,
        0..=53 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn read_inner_instruction<'de>(
    cursor: &mut &'de [u8],
) -> ReadResult<BorrowedInnerInstruction<'de>> {
    let program_id_index = get::<u32>(cursor)?;
    let accounts_len = read_len_bounded_by_remaining(
        cursor,
        "inner-instruction account-index count exceeds remaining input",
    )?;
    let accounts = cursor.take_borrowed(accounts_len)?;
    skip_bytes(cursor)?; // data: Vec<u8> — never read, never allocated.
    get::<Option<u32>>(cursor)?; // stack_height — discarded.
    Ok(BorrowedInnerInstruction {
        program_id_index,
        accounts,
    })
}

#[derive(Clone, Copy)]
pub struct MetadataDecodeLimits {
    pub total_message_accounts: usize,
    pub top_level_instruction_count: usize,
}

fn skip_balances(cursor: &mut &[u8], maximum: usize) -> ReadResult<()> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        get::<u64>(cursor)?;
    }
    Ok(())
}

fn skip_program_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => {}
        1 => {
            get::<blockzilla_format::program_logs::token::TokenLog>(cursor)?;
        }
        2 => {
            get::<blockzilla_format::program_logs::token_2022::Token2022Log>(cursor)?;
        }
        3 => {
            get::<blockzilla_format::program_logs::associated_token_account::TokenLog>(cursor)?;
        }
        4 => {
            get::<blockzilla_format::program_logs::address_lookup_table::AddressLookupTableLog>(
                cursor,
            )?;
        }
        5 => {
            get::<blockzilla_format::program_logs::loader_v3::LoaderV3Log>(cursor)?;
        }
        6 => {
            get::<blockzilla_format::program_logs::loader_v4::LoaderV4Log>(cursor)?;
        }
        7 => {
            get::<blockzilla_format::program_logs::memo::MemoLog>(cursor)?;
        }
        8 => {
            get::<blockzilla_format::program_logs::record::RecordLog>(cursor)?;
        }
        9 => {
            get::<blockzilla_format::program_logs::transfer_hook::TransferHookLog>(cursor)?;
        }
        10 => {
            get::<blockzilla_format::program_logs::account_compression::AccountCompressionLog>(
                cursor,
            )?;
        }
        11 => {
            get::<blockzilla_format::program_logs::stake::StakeProgramLog>(cursor)?;
        }
        12 => {
            get::<blockzilla_format::program_logs::zk_elgamal_proof::ZkElgamalProofLog>(cursor)?;
        }
        13 | 16 => {
            get::<u32>(cursor)?;
        }
        14 => {
            get::<u32>(cursor)?;
            get::<u32>(cursor)?;
            get::<u32>(cursor)?;
        }
        15 => {
            for _ in 0..5 {
                get::<u32>(cursor)?;
            }
        }
        17 => skip_known_program_log(cursor)?,
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_known_program_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => skip_drift_log(cursor)?,
        1 => skip_okx_router_log(cursor)?,
        2 => skip_phoenix_perps_log(cursor)?,
        3 => skip_phoenix_v1_log(cursor)?,
        4 => {
            get::<blockzilla_format::program_logs::known_programs::raydium_amm::RaydiumAmmLog>(
                cursor,
            )?;
        }
        5 => {
            get::<
                blockzilla_format::program_logs::known_programs::static_programs::StaticProgramLog,
            >(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_drift_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => skip_bytes(cursor)?,
        1 => {
            get::<u64>(cursor)?;
        }
        2 | 5 | 6 | 12 | 16 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        3 | 4 | 7..=11 | 13..=15 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_okx_router_log(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => {
            skip_string(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<blockzilla_format::program_logs::known_programs::okx_router::AmountInSpelling>(
                cursor,
            )?;
        }
        1 => skip_string(cursor)?,
        2..=4 => {
            get::<u64>(cursor)?;
        }
        5 | 11 => {}
        6 | 7 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        8 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            skip_string(cursor)?;
        }
        9 => {
            get::<u8>(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        10 => {
            get::<blockzilla_format::program_logs::known_programs::okx_router::OkxRouteLabel>(
                cursor,
            )?;
        }
        12 => {
            get::<blockzilla_format::program_logs::known_programs::okx_router::OkxMarker>(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_phoenix_perps_log(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u32>(cursor)? {
        0 => skip_bytes(cursor),
        1 => {
            get::<blockzilla_format::program_logs::known_programs::phoenix_perps::PhoenixPerpsStaticLog>(cursor)?;
            Ok(())
        }
        2 => {
            get::<u64>(cursor)?;
            Ok(())
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_phoenix_v1_log(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u32>(cursor)? {
        0 => {
            get::<
                blockzilla_format::program_logs::known_programs::phoenix_v1::PhoenixInstructionLog,
            >(cursor)?;
        }
        1 => {
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
            get::<u64>(cursor)?;
        }
        2 => {
            skip_string(cursor)?;
            get::<u64>(cursor)?;
        }
        3 => {
            get::<blockzilla_format::program_logs::known_programs::phoenix_v1::PhoenixStaticLog>(
                cursor,
            )?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

fn skip_log_event(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
    match tag {
        0 => {
            get::<blockzilla_format::program_logs::system_program::SystemProgramLog>(cursor)?;
        }
        1 | 2 | 9..=13 | 38 | 39 | 43 => {}
        3 | 4 | 15 | 18 | 19 | 24..=27 | 40..=42 => {
            get::<CompactPubkey>(cursor)?;
        }
        5 | 8 => skip_program_log(cursor)?,
        6 | 28 | 29 | 31..=33 | 36 | 37 | 44 | 45 => {
            get::<u32>(cursor)?;
        }
        7 => {
            get::<CompactPubkey>(cursor)?;
            skip_program_log(cursor)?;
        }
        14 => {
            get::<CompactPubkey>(cursor)?;
            get::<u8>(cursor)?;
        }
        16 => {
            get::<CompactPubkey>(cursor)?;
            get::<u32>(cursor)?;
            get::<u32>(cursor)?;
        }
        17 => {
            get::<u32>(cursor)?;
            get::<u32>(cursor)?;
        }
        20..=23 | 30 => {
            get::<CompactPubkey>(cursor)?;
            get::<u32>(cursor)?;
        }
        34 | 35 => {
            get::<Option<CompactPubkey>>(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    Ok(())
}

/// Stream past a `CompactLogStream` without materializing any outer or
/// nested vectors/strings. Every allocation-bearing known-program payload is
/// skipped from its bounded wire representation as well.
fn skip_logs_present(cursor: &mut &[u8]) -> ReadResult<bool> {
    match get::<u8>(cursor)? {
        0 => Ok(false),
        1 => {
            let event_count =
                read_len_bounded_by_remaining(cursor, "log event count exceeds remaining input")?;
            for _ in 0..event_count {
                skip_log_event(cursor)?;
            }

            let string_length_count = read_len_bounded_by_remaining(
                cursor,
                "log string-length count exceeds remaining input",
            )?;
            for _ in 0..string_length_count {
                get::<u32>(cursor)?;
            }
            skip_bytes(cursor)?; // StringTable::bytes

            let data_array_count = read_len_bounded_by_remaining(
                cursor,
                "log data-array count exceeds remaining input",
            )?;
            for _ in 0..data_array_count {
                get::<DataArray>(cursor)?;
            }
            let chunk_length_count = read_len_bounded_by_remaining(
                cursor,
                "log chunk-length count exceeds remaining input",
            )?;
            for _ in 0..chunk_length_count {
                get::<u32>(cursor)?;
            }
            skip_bytes(cursor)?; // DataTable::bytes
            Ok(true)
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_logs(cursor: &mut &[u8]) -> ReadResult<()> {
    skip_logs_present(cursor).map(|_| ())
}

fn skip_token_balances_count(cursor: &mut &[u8], maximum: usize) -> ReadResult<usize> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "token-balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        get::<CompactTokenBalance>(cursor)?;
    }
    Ok(count)
}

fn skip_token_balances(cursor: &mut &[u8], maximum: usize) -> ReadResult<()> {
    skip_token_balances_count(cursor, maximum).map(|_| ())
}

fn skip_rewards_count(cursor: &mut &[u8]) -> ReadResult<usize> {
    let count = read_len_bounded_by_remaining(cursor, "reward count exceeds remaining input")?;
    for _ in 0..count {
        get::<CompactReward>(cursor)?;
    }
    Ok(count)
}

fn skip_rewards(cursor: &mut &[u8]) -> ReadResult<()> {
    skip_rewards_count(cursor).map(|_| ())
}

/// Decode `CompactMetaV1`'s `err`/`fee`/`pre_balances`/`post_balances`
/// prefix (discarded — this indexer doesn't use them) followed by
/// `inner_instructions`, calling `on_inner_instruction` for each one as
/// it's decoded (no intermediate `Vec` is materialized, and no inner
/// instruction's `data` is ever allocated).
///
/// If `need_loaded_addresses` is false (legacy messages, which have no
/// address-table lookups to resolve), returns `None` and stops immediately
/// after `inner_instructions` — the entire metadata tail (logs, token
/// balances, rewards, return data, compute units) is never touched. If
/// true (V0 messages), streams past `logs`, `pre_token_balances`,
/// `post_token_balances`, and `rewards` without allocating their outer
/// vectors or byte tables, to reach and return
/// `Some((loaded_writable_addresses, loaded_readonly_addresses))`.
pub fn decode_metadata_prefix<'de>(
    cursor: &mut &'de [u8],
    need_loaded_addresses: bool,
    limits: MetadataDecodeLimits,
    on_inner_instruction: impl FnMut(BorrowedInnerInstruction<'de>),
) -> ReadResult<DecodedMetadataPrefix> {
    decode_metadata_prefix_with_schema(
        cursor,
        CompactV2MetadataSchema::CurrentTypedError,
        need_loaded_addresses,
        limits,
        on_inner_instruction,
    )
}

/// Decode the account-bearing metadata prefix under one explicitly selected
/// Compact V2 metadata grammar.
pub fn decode_metadata_prefix_with_schema<'de>(
    cursor: &mut &'de [u8],
    metadata_schema: CompactV2MetadataSchema,
    need_loaded_addresses: bool,
    limits: MetadataDecodeLimits,
    mut on_inner_instruction: impl FnMut(BorrowedInnerInstruction<'de>),
) -> ReadResult<DecodedMetadataPrefix> {
    let mut loaded_writable_addresses = Vec::new();
    let mut loaded_readonly_addresses = Vec::new();
    let shape = stream_metadata_accounts_with_schema(
        cursor,
        metadata_schema,
        need_loaded_addresses,
        limits,
        |event| {
            match event {
                MetadataAccountEvent::InnerInstruction(instruction) => {
                    on_inner_instruction(instruction);
                }
                MetadataAccountEvent::LoadedWritableCount(count) => {
                    loaded_writable_addresses.reserve_exact(count);
                }
                MetadataAccountEvent::LoadedWritable(key) => {
                    loaded_writable_addresses.push(key);
                }
                MetadataAccountEvent::LoadedReadonlyCount(count) => {
                    loaded_readonly_addresses.reserve_exact(count);
                }
                MetadataAccountEvent::LoadedReadonly(key) => {
                    loaded_readonly_addresses.push(key);
                }
            }
            Ok::<(), wincode::error::ReadError>(())
        },
    )?;
    Ok(DecodedMetadataPrefix {
        has_error: shape.has_error,
        inner_instructions_present: shape.inner_instructions_present,
        loaded_addresses: need_loaded_addresses
            .then_some((loaded_writable_addresses, loaded_readonly_addresses)),
    })
}

/// Traverse inner-instruction indexes and loaded account references without
/// allocating owned metadata account lanes.
pub fn stream_metadata_accounts_with_schema<'de, E>(
    cursor: &mut &'de [u8],
    metadata_schema: CompactV2MetadataSchema,
    need_loaded_addresses: bool,
    limits: MetadataDecodeLimits,
    mut on_event: impl FnMut(MetadataAccountEvent<'de>) -> Result<(), E>,
) -> Result<StreamedMetadataShape, E>
where
    E: From<wincode::error::ReadError> + From<wincode::io::ReadError>,
{
    if limits.total_message_accounts > MAX_MESSAGE_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "total message account count exceeds message account cap",
        )
        .into());
    }
    let has_error = decode_metadata_error_with_schema(cursor, metadata_schema)?;
    get::<u64>(cursor)?; // fee
    skip_balances(cursor, limits.total_message_accounts)?;
    skip_balances(cursor, limits.total_message_accounts)?;

    let inner_instructions_present = match get::<u8>(cursor)? {
        0 => false,
        1 => {
            let group_count = read_bounded_len(
                cursor,
                limits.top_level_instruction_count.min(cursor.len()),
                "inner-instruction group count exceeds top-level instruction count",
            )?;
            for _ in 0..group_count {
                let group_index = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if group_index >= limits.top_level_instruction_count {
                    return Err(wincode::error::invalid_value(
                        "inner-instruction group index is outside top-level instructions",
                    )
                    .into());
                }
                let inner_count = read_len_bounded_by_remaining(
                    cursor,
                    "inner-instruction count exceeds remaining input",
                )?;
                for _ in 0..inner_count {
                    let instruction = read_inner_instruction(cursor)?;
                    let program_index = usize::try_from(instruction.program_id_index)
                        .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                    if program_index >= limits.total_message_accounts {
                        return Err(wincode::error::invalid_value(
                            "inner-instruction program index is outside message accounts",
                        )
                        .into());
                    }
                    on_event(MetadataAccountEvent::InnerInstruction(instruction))?;
                }
            }
            true
        }
        other => return Err(invalid_tag_encoding(other as usize).into()),
    };

    if !need_loaded_addresses {
        return Ok(StreamedMetadataShape {
            has_error,
            inner_instructions_present,
            loaded_writable_count: 0,
            loaded_readonly_count: 0,
        });
    }

    skip_logs(cursor)?;
    skip_token_balances(cursor, limits.total_message_accounts)?;
    skip_token_balances(cursor, limits.total_message_accounts)?;
    skip_rewards(cursor)?;
    let loaded_writable_count = read_bounded_len(
        cursor,
        limits.total_message_accounts,
        "loaded address count exceeds total message account count",
    )?;
    on_event(MetadataAccountEvent::LoadedWritableCount(
        loaded_writable_count,
    ))?;
    for _ in 0..loaded_writable_count {
        on_event(MetadataAccountEvent::LoadedWritable(get::<CompactPubkey>(
            cursor,
        )?))?;
    }
    let loaded_readonly_count = read_bounded_len(
        cursor,
        limits.total_message_accounts,
        "loaded address count exceeds total message account count",
    )?;
    on_event(MetadataAccountEvent::LoadedReadonlyCount(
        loaded_readonly_count,
    ))?;
    for _ in 0..loaded_readonly_count {
        on_event(MetadataAccountEvent::LoadedReadonly(get::<CompactPubkey>(
            cursor,
        )?))?;
    }
    if loaded_writable_count + loaded_readonly_count > limits.total_message_accounts {
        return Err(wincode::error::invalid_value(
            "loaded address count exceeds total message account count",
        )
        .into());
    }
    Ok(StreamedMetadataShape {
        has_error,
        inner_instructions_present,
        loaded_writable_count,
        loaded_readonly_count,
    })
}

#[inline]
fn consumed_prefix<'de>(start: &'de [u8], remaining: &[u8]) -> &'de [u8] {
    let consumed = start
        .len()
        .checked_sub(remaining.len())
        .expect("metadata cursor remains a suffix of its source");
    &start[..consumed]
}

/// Traverse one complete decoded `CompactMetaV1` record once, lending exact
/// source field ranges while emitting the same account events as
/// [`stream_metadata_accounts_with_schema`].
///
/// This function is for the source-split canary. It always consumes loaded
/// address lanes and the complete metadata tail, and it rejects trailing
/// bytes. It does not allocate or normalize any effect value.
pub fn stream_metadata_effects_with_schema<'de, E>(
    cursor: &mut &'de [u8],
    metadata_schema: CompactV2MetadataSchema,
    limits: MetadataDecodeLimits,
    on_event: impl FnMut(MetadataAccountEvent<'de>) -> Result<(), E>,
) -> Result<StreamedMetadataEffects<'de>, E>
where
    E: From<wincode::error::ReadError> + From<wincode::io::ReadError>,
{
    stream_metadata_effects_impl(cursor, metadata_schema, Some(limits), on_event)
}

/// Traverse complete metadata for a raw transaction without inventing
/// relational message-account or top-level-instruction counts.
///
/// Wire collection sizes remain bounded by the protocol account cap and by
/// the input length. Index values are decoded but are not compared with an
/// unavailable message shape.
pub fn stream_metadata_effects_structural_with_schema<'de, E>(
    cursor: &mut &'de [u8],
    metadata_schema: CompactV2MetadataSchema,
    on_event: impl FnMut(MetadataAccountEvent<'de>) -> Result<(), E>,
) -> Result<StreamedMetadataEffects<'de>, E>
where
    E: From<wincode::error::ReadError> + From<wincode::io::ReadError>,
{
    stream_metadata_effects_impl(cursor, metadata_schema, None, on_event)
}

fn stream_metadata_effects_impl<'de, E>(
    cursor: &mut &'de [u8],
    metadata_schema: CompactV2MetadataSchema,
    limits: Option<MetadataDecodeLimits>,
    mut on_event: impl FnMut(MetadataAccountEvent<'de>) -> Result<(), E>,
) -> Result<StreamedMetadataEffects<'de>, E>
where
    E: From<wincode::error::ReadError> + From<wincode::io::ReadError>,
{
    let total_message_accounts = limits
        .map(|limits| limits.total_message_accounts)
        .unwrap_or(MAX_MESSAGE_ACCOUNTS);
    if total_message_accounts > MAX_MESSAGE_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "total message account count exceeds message account cap",
        )
        .into());
    }

    let outcome_head_start = *cursor;
    let has_error = decode_metadata_error_with_schema(cursor, metadata_schema)?;
    get::<u64>(cursor)?; // fee
    let outcome_head = consumed_prefix(outcome_head_start, cursor);

    let pre_balances_start = *cursor;
    skip_balances(cursor, total_message_accounts)?;
    let pre_balances = consumed_prefix(pre_balances_start, cursor);
    let post_balances_start = *cursor;
    skip_balances(cursor, total_message_accounts)?;
    let post_balances = consumed_prefix(post_balances_start, cursor);

    let inner_start = *cursor;
    let (inner_instructions_present, inner_group_count) = match get::<u8>(cursor)? {
        0 => (false, 0),
        1 => {
            let group_count = if let Some(limits) = limits {
                read_bounded_len(
                    cursor,
                    limits.top_level_instruction_count.min(cursor.len()),
                    "inner-instruction group count exceeds top-level instruction count",
                )?
            } else {
                read_len_bounded_by_remaining(
                    cursor,
                    "inner-instruction group count exceeds remaining input",
                )?
            };
            for _ in 0..group_count {
                let group_index = usize::try_from(get::<u32>(cursor)?)
                    .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                if limits.is_some_and(|limits| group_index >= limits.top_level_instruction_count) {
                    return Err(wincode::error::invalid_value(
                        "inner-instruction group index is outside top-level instructions",
                    )
                    .into());
                }
                let inner_count = read_len_bounded_by_remaining(
                    cursor,
                    "inner-instruction count exceeds remaining input",
                )?;
                for _ in 0..inner_count {
                    let instruction = read_inner_instruction(cursor)?;
                    let program_index = usize::try_from(instruction.program_id_index)
                        .map_err(|_| wincode::error::pointer_sized_decode_error())?;
                    if limits.is_some_and(|limits| program_index >= limits.total_message_accounts) {
                        return Err(wincode::error::invalid_value(
                            "inner-instruction program index is outside message accounts",
                        )
                        .into());
                    }
                    on_event(MetadataAccountEvent::InnerInstruction(instruction))?;
                }
            }
            (true, group_count)
        }
        other => return Err(invalid_tag_encoding(other as usize).into()),
    };
    let inner_instructions = consumed_prefix(inner_start, cursor);

    let logs_start = *cursor;
    let logs_present = skip_logs_present(cursor)?;
    let logs = consumed_prefix(logs_start, cursor);

    let pre_token_start = *cursor;
    let pre_token_balance_count = skip_token_balances_count(cursor, total_message_accounts)?;
    let pre_token_balances = consumed_prefix(pre_token_start, cursor);
    let post_token_start = *cursor;
    let post_token_balance_count = skip_token_balances_count(cursor, total_message_accounts)?;
    let post_token_balances = consumed_prefix(post_token_start, cursor);

    let rewards_start = *cursor;
    let transaction_reward_count = skip_rewards_count(cursor)?;
    let transaction_rewards = consumed_prefix(rewards_start, cursor);

    let loaded_writable_start = *cursor;
    let loaded_writable_count = read_bounded_len(
        cursor,
        total_message_accounts,
        "loaded address count exceeds total message account count",
    )?;
    on_event(MetadataAccountEvent::LoadedWritableCount(
        loaded_writable_count,
    ))?;
    for _ in 0..loaded_writable_count {
        on_event(MetadataAccountEvent::LoadedWritable(get::<CompactPubkey>(
            cursor,
        )?))?;
    }
    let loaded_writable = consumed_prefix(loaded_writable_start, cursor);

    let loaded_readonly_start = *cursor;
    let loaded_readonly_count = read_bounded_len(
        cursor,
        total_message_accounts,
        "loaded address count exceeds total message account count",
    )?;
    on_event(MetadataAccountEvent::LoadedReadonlyCount(
        loaded_readonly_count,
    ))?;
    for _ in 0..loaded_readonly_count {
        on_event(MetadataAccountEvent::LoadedReadonly(get::<CompactPubkey>(
            cursor,
        )?))?;
    }
    let loaded_readonly = consumed_prefix(loaded_readonly_start, cursor);
    if loaded_writable_count
        .checked_add(loaded_readonly_count)
        .is_none_or(|count| count > total_message_accounts)
    {
        return Err(wincode::error::invalid_value(
            "loaded address count exceeds total message account count",
        )
        .into());
    }

    let outcome_tail_start = *cursor;
    match get::<u8>(cursor)? {
        0 => {}
        1 => {
            get::<CompactPubkey>(cursor)?;
            skip_bytes(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize).into()),
    }
    get::<Option<u64>>(cursor)?;
    get::<Option<u64>>(cursor)?;
    let outcome_tail = consumed_prefix(outcome_tail_start, cursor);
    if !cursor.is_empty() {
        return Err(wincode::error::invalid_value("metadata has trailing bytes").into());
    }

    Ok(StreamedMetadataEffects {
        shape: StreamedMetadataShape {
            has_error,
            inner_instructions_present,
            loaded_writable_count,
            loaded_readonly_count,
        },
        fields: BorrowedMetadataEffectFields {
            outcome_head,
            pre_balances,
            post_balances,
            inner_instructions,
            logs,
            pre_token_balances,
            post_token_balances,
            transaction_rewards,
            loaded_writable,
            loaded_readonly,
            outcome_tail,
        },
        inner_group_count,
        logs_present,
        pre_token_balance_count,
        post_token_balance_count,
        transaction_reward_count,
    })
}

/// Finish and validate the metadata grammar after
/// [`stream_metadata_accounts_with_schema`].
///
/// The account projection intentionally stops as soon as it has all account
/// fields. A verifier must call this function and then require an empty cursor
/// so malformed or trailing non-account metadata cannot be accepted. When
/// `loaded_addresses_were_streamed` is false, this consumes the whole tail
/// starting at `logs`; otherwise it starts at `return_data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedMetadataTail {
    pub unstreamed_loaded_writable_count: usize,
    pub unstreamed_loaded_readonly_count: usize,
}

pub fn finish_metadata_tail_exact(
    cursor: &mut &[u8],
    loaded_addresses_were_streamed: bool,
    limits: MetadataDecodeLimits,
) -> ReadResult<FinishedMetadataTail> {
    let mut unstreamed_loaded_writable_count = 0usize;
    let mut unstreamed_loaded_readonly_count = 0usize;
    if !loaded_addresses_were_streamed {
        skip_logs(cursor)?;
        skip_token_balances(cursor, limits.total_message_accounts)?;
        skip_token_balances(cursor, limits.total_message_accounts)?;
        skip_rewards(cursor)?;
        let loaded_writable = read_bounded_len(
            cursor,
            limits.total_message_accounts,
            "loaded address count exceeds total message account count",
        )?;
        for _ in 0..loaded_writable {
            get::<CompactPubkey>(cursor)?;
        }
        let loaded_readonly = read_bounded_len(
            cursor,
            limits.total_message_accounts,
            "loaded address count exceeds total message account count",
        )?;
        for _ in 0..loaded_readonly {
            get::<CompactPubkey>(cursor)?;
        }
        if loaded_writable
            .checked_add(loaded_readonly)
            .is_none_or(|count| count > limits.total_message_accounts)
        {
            return Err(wincode::error::invalid_value(
                "loaded address count exceeds total message account count",
            ));
        }
        unstreamed_loaded_writable_count = loaded_writable;
        unstreamed_loaded_readonly_count = loaded_readonly;
    }

    match get::<u8>(cursor)? {
        0 => {}
        1 => {
            get::<CompactPubkey>(cursor)?;
            skip_bytes(cursor)?;
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    get::<Option<u64>>(cursor)?;
    get::<Option<u64>>(cursor)?;
    Ok(FinishedMetadataTail {
        unstreamed_loaded_writable_count,
        unstreamed_loaded_readonly_count,
    })
}

/// `CompactInnerInstruction` is unused directly (its fields are read
/// piecemeal by `read_inner_instruction`) but referenced here so `cargo`
/// flags it if the upstream type's shape ever changes in a way `rustc`
/// can statically catch.
#[allow(dead_code)]
fn _assert_inner_instruction_shape(value: CompactInnerInstruction) {
    let CompactInnerInstruction {
        program_id_index: _,
        accounts: _,
        data: _,
        stack_height: _,
    } = value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::program_logs::{
        ProgramLog,
        known_programs::{
            KnownProgramLog,
            drift::DriftLog,
            okx_router::{AmountInSpelling, OkxRouterLog},
            phoenix_perps::PhoenixPerpsLog,
            phoenix_v1::PhoenixLog,
        },
    };
    use blockzilla_format::{
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMessagePayload, ArchiveV2HotV0Message, ArchiveV2HotV1Message,
        ArchiveV2SystemInstructionData, ArchiveV2VoteLockoutOffset, ArchiveV2VoteStateUpdate,
        CompactInnerInstructions, CompactInstructionError, CompactLogStream, CompactMessageHeader,
        CompactMetaV1, CompactReturnData, CompactTransactionError, DataTable, LogEvent,
        OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash, StringTable,
        wincode_leb128_config,
    };

    fn serialize<T: wincode::SchemaWrite<Cfg, Src = T>>(value: &T) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    #[test]
    fn exact_metadata_finisher_consumes_tail_and_exposes_trailing_bytes() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![9],
            post_balances: vec![8],
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![CompactPubkey::Id(1)],
            loaded_readonly_addresses: vec![CompactPubkey::Raw([2; 32])],
            return_data: Some(CompactReturnData {
                program_id: CompactPubkey::Id(3),
                data: vec![4, 5, 6],
            }),
            compute_units_consumed: Some(7),
            cost_units: Some(8),
        };
        let encoded = serialize(&metadata);
        for loaded_were_streamed in [false, true] {
            let mut cursor = encoded.as_slice();
            stream_metadata_accounts_with_schema(
                &mut cursor,
                CompactV2MetadataSchema::CurrentTypedError,
                loaded_were_streamed,
                MetadataDecodeLimits {
                    total_message_accounts: 3,
                    top_level_instruction_count: 0,
                },
                |_| Ok::<_, wincode::error::ReadError>(()),
            )
            .unwrap();
            finish_metadata_tail_exact(
                &mut cursor,
                loaded_were_streamed,
                MetadataDecodeLimits {
                    total_message_accounts: 3,
                    top_level_instruction_count: 0,
                },
            )
            .unwrap();
            assert!(cursor.is_empty());
        }

        let mut with_trailing = encoded.clone();
        with_trailing.push(0xaa);
        let mut cursor = with_trailing.as_slice();
        stream_metadata_accounts_with_schema(
            &mut cursor,
            CompactV2MetadataSchema::CurrentTypedError,
            true,
            MetadataDecodeLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 0,
            },
            |_| Ok::<_, wincode::error::ReadError>(()),
        )
        .unwrap();
        finish_metadata_tail_exact(
            &mut cursor,
            true,
            MetadataDecodeLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 0,
            },
        )
        .unwrap();
        assert_eq!(cursor, &[0xaa]);
    }

    #[test]
    fn exact_metadata_finisher_rejects_truncated_tail() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 1,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: Some(CompactReturnData {
                program_id: CompactPubkey::Raw([7; 32]),
                data: vec![1, 2, 3, 4],
            }),
            compute_units_consumed: Some(9),
            cost_units: Some(10),
        };
        let encoded = serialize(&metadata);
        let mut full_cursor = encoded.as_slice();
        stream_metadata_accounts_with_schema(
            &mut full_cursor,
            CompactV2MetadataSchema::CurrentTypedError,
            true,
            MetadataDecodeLimits {
                total_message_accounts: 1,
                top_level_instruction_count: 0,
            },
            |_| Ok::<_, wincode::error::ReadError>(()),
        )
        .unwrap();
        let tail = full_cursor;
        assert!(tail.len() > 1);
        for length in 0..tail.len() {
            let mut truncated = &tail[..length];
            assert!(
                finish_metadata_tail_exact(
                    &mut truncated,
                    true,
                    MetadataDecodeLimits {
                        total_message_accounts: 1,
                        top_level_instruction_count: 0,
                    },
                )
                .is_err(),
                "truncated metadata tail length {length} was accepted"
            );
        }
    }

    #[test]
    fn source_split_metadata_ranges_reconstruct_current_record_exactly() {
        let metadata = CompactMetaV1 {
            err: Some(CompactTransactionError::AccountInUse),
            fee: 9,
            pre_balances: vec![10, 11],
            post_balances: vec![12, 13],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 1],
                    data: vec![4, 5],
                    stack_height: Some(3),
                }],
            }]),
            logs: Some(CompactLogStream {
                events: Vec::new(),
                strings: StringTable::default(),
                data: DataTable::default(),
            }),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(1)),
                owner: Some(CompactPubkey::Raw([2; 32])),
                program_id: None,
                amount: 14,
                decimals: 6,
            }],
            post_token_balances: vec![CompactTokenBalance {
                account_index: 1,
                mint: None,
                owner: Some(CompactPubkey::Id(2)),
                program_id: Some(CompactPubkey::Raw([3; 32])),
                amount: 15,
                decimals: 7,
            }],
            rewards: vec![CompactReward {
                pubkey: CompactPubkey::Raw([4; 32]),
                lamports: -5,
                post_balance: 16,
                reward_type: 2,
                commission: Some(8),
            }],
            loaded_writable_addresses: vec![CompactPubkey::Id(2)],
            loaded_readonly_addresses: Vec::new(),
            return_data: Some(CompactReturnData {
                program_id: CompactPubkey::Raw([6; 32]),
                data: vec![7, 8],
            }),
            compute_units_consumed: Some(17),
            cost_units: Some(18),
        };
        let encoded = serialize(&metadata);
        let mut cursor = encoded.as_slice();
        let effects = stream_metadata_effects_with_schema(
            &mut cursor,
            CompactV2MetadataSchema::CurrentTypedError,
            MetadataDecodeLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 1,
            },
            |_| Ok::<(), wincode::error::ReadError>(()),
        )
        .unwrap();
        assert!(cursor.is_empty());
        assert_eq!(effects.inner_group_count, 1);
        assert!(effects.logs_present);
        assert_eq!(effects.pre_token_balance_count, 1);
        assert_eq!(effects.post_token_balance_count, 1);
        assert_eq!(effects.transaction_reward_count, 1);

        let mut expected_head = serialize(&metadata.err);
        expected_head.extend(serialize(&metadata.fee));
        let mut expected_tail = serialize(&metadata.return_data);
        expected_tail.extend(serialize(&metadata.compute_units_consumed));
        expected_tail.extend(serialize(&metadata.cost_units));
        assert_eq!(effects.fields.outcome_head, expected_head);
        assert_eq!(
            effects.fields.pre_balances,
            serialize(&metadata.pre_balances)
        );
        assert_eq!(
            effects.fields.post_balances,
            serialize(&metadata.post_balances)
        );
        assert_eq!(
            effects.fields.inner_instructions,
            serialize(&metadata.inner_instructions)
        );
        assert_eq!(effects.fields.logs, serialize(&metadata.logs));
        assert_eq!(
            effects.fields.pre_token_balances,
            serialize(&metadata.pre_token_balances)
        );
        assert_eq!(
            effects.fields.post_token_balances,
            serialize(&metadata.post_token_balances)
        );
        assert_eq!(
            effects.fields.transaction_rewards,
            serialize(&metadata.rewards)
        );
        assert_eq!(
            effects.fields.loaded_writable,
            serialize(&metadata.loaded_writable_addresses)
        );
        assert_eq!(
            effects.fields.loaded_readonly,
            serialize(&metadata.loaded_readonly_addresses)
        );
        assert_eq!(effects.fields.outcome_tail, expected_tail);

        let mut reconstructed = Vec::new();
        reconstructed.extend_from_slice(effects.fields.outcome_head);
        reconstructed.extend_from_slice(effects.fields.pre_balances);
        reconstructed.extend_from_slice(effects.fields.post_balances);
        reconstructed.extend_from_slice(effects.fields.inner_instructions);
        reconstructed.extend_from_slice(effects.fields.logs);
        reconstructed.extend_from_slice(effects.fields.pre_token_balances);
        reconstructed.extend_from_slice(effects.fields.post_token_balances);
        reconstructed.extend_from_slice(effects.fields.transaction_rewards);
        reconstructed.extend_from_slice(effects.fields.loaded_writable);
        reconstructed.extend_from_slice(effects.fields.loaded_readonly);
        reconstructed.extend_from_slice(effects.fields.outcome_tail);
        assert_eq!(reconstructed, encoded);
    }

    #[test]
    fn source_split_legacy_ranges_are_exact_and_structural_mode_has_no_fake_relations() {
        let legacy = legacy_raw_metadata_bytes(
            Some(vec![0xaa, 0xbb]),
            Some(vec![CompactInnerInstructions {
                index: 300,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 500,
                    accounts: vec![255],
                    data: vec![1],
                    stack_height: None,
                }],
            }]),
            Vec::new(),
            Vec::new(),
        );
        let mut relational = legacy.as_slice();
        assert!(
            stream_metadata_effects_with_schema(
                &mut relational,
                CompactV2MetadataSchema::LegacyRawError,
                MetadataDecodeLimits {
                    total_message_accounts: MAX_MESSAGE_ACCOUNTS,
                    top_level_instruction_count: MAX_MESSAGE_ACCOUNTS,
                },
                |_| Ok::<(), wincode::error::ReadError>(()),
            )
            .is_err()
        );
        let mut structural = legacy.as_slice();
        let effects = stream_metadata_effects_structural_with_schema(
            &mut structural,
            CompactV2MetadataSchema::LegacyRawError,
            |_| Ok::<(), wincode::error::ReadError>(()),
        )
        .unwrap();
        assert!(structural.is_empty());
        assert!(effects.shape.has_error);
        assert_eq!(effects.inner_group_count, 1);
        let mut reconstructed = Vec::new();
        reconstructed.extend_from_slice(effects.fields.outcome_head);
        reconstructed.extend_from_slice(effects.fields.pre_balances);
        reconstructed.extend_from_slice(effects.fields.post_balances);
        reconstructed.extend_from_slice(effects.fields.inner_instructions);
        reconstructed.extend_from_slice(effects.fields.logs);
        reconstructed.extend_from_slice(effects.fields.pre_token_balances);
        reconstructed.extend_from_slice(effects.fields.post_token_balances);
        reconstructed.extend_from_slice(effects.fields.transaction_rewards);
        reconstructed.extend_from_slice(effects.fields.loaded_writable);
        reconstructed.extend_from_slice(effects.fields.loaded_readonly);
        reconstructed.extend_from_slice(effects.fields.outcome_tail);
        assert_eq!(reconstructed, legacy);

        let mut trailing = legacy;
        trailing.push(0xff);
        assert!(
            stream_metadata_effects_structural_with_schema(
                &mut trailing.as_slice(),
                CompactV2MetadataSchema::LegacyRawError,
                |_| Ok::<(), wincode::error::ReadError>(()),
            )
            .is_err()
        );
    }

    fn append<T: wincode::SchemaWrite<Cfg, Src = T>>(bytes: &mut Vec<u8>, value: &T) {
        bytes.extend(serialize(value));
    }

    #[derive(Debug, PartialEq, Eq)]
    enum OwnedMessageAccountEvent {
        StaticAccountCount(usize),
        StaticAccount {
            source_position: usize,
            key: CompactPubkey,
        },
        Instruction {
            program_id_index: u8,
            accounts: Vec<u8>,
        },
    }

    fn assert_message_stream_matches_owned(
        bytes: &[u8],
        schema: CompactV2MessageSchema,
    ) -> (DecodedMessage, Vec<OwnedMessageAccountEvent>) {
        let mut owned_cursor = bytes;
        let mut owned_instructions = Vec::new();
        let owned = decode_message_with_schema(&mut owned_cursor, schema, |instruction| {
            owned_instructions.push((instruction.program_id_index, instruction.accounts.to_vec()));
        })
        .unwrap();

        let mut streamed_cursor = bytes;
        let mut streamed_events = Vec::new();
        let streamed = stream_message_accounts_with_schema(&mut streamed_cursor, schema, |event| {
            streamed_events.push(match event {
                MessageAccountEvent::StaticAccountCount(count) => {
                    OwnedMessageAccountEvent::StaticAccountCount(count)
                }
                MessageAccountEvent::StaticAccount {
                    source_position,
                    key,
                } => OwnedMessageAccountEvent::StaticAccount {
                    source_position,
                    key,
                },
                MessageAccountEvent::Instruction(instruction) => {
                    OwnedMessageAccountEvent::Instruction {
                        program_id_index: instruction.program_id_index,
                        accounts: instruction.accounts.to_vec(),
                    }
                }
            });
            Ok::<(), wincode::error::ReadError>(())
        })
        .unwrap();

        let mut expected_events = Vec::new();
        expected_events.push(OwnedMessageAccountEvent::StaticAccountCount(
            owned.account_keys.len(),
        ));
        expected_events.extend(owned.account_keys.iter().copied().enumerate().map(
            |(source_position, key)| OwnedMessageAccountEvent::StaticAccount {
                source_position,
                key,
            },
        ));
        expected_events.extend(
            owned_instructions
                .iter()
                .map(
                    |(program_id_index, accounts)| OwnedMessageAccountEvent::Instruction {
                        program_id_index: *program_id_index,
                        accounts: accounts.clone(),
                    },
                ),
        );

        assert_eq!(streamed_events, expected_events);
        assert_eq!(streamed_cursor, owned_cursor);
        assert_eq!(
            streamed,
            StreamedMessageShape {
                static_account_count: owned.account_keys.len(),
                is_v0: owned.is_v0,
                num_required_signatures: owned.num_required_signatures,
                num_readonly_signed_accounts: owned.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: owned.num_readonly_unsigned_accounts,
                instruction_count: owned.instruction_count,
                expected_loaded_writable: owned.expected_loaded_writable,
                expected_loaded_readonly: owned.expected_loaded_readonly,
            }
        );
        (owned, streamed_events)
    }

    #[derive(Debug, PartialEq, Eq)]
    enum OwnedMetadataAccountEvent {
        InnerInstruction {
            program_id_index: u32,
            accounts: Vec<u8>,
        },
        LoadedWritableCount(usize),
        LoadedWritable(CompactPubkey),
        LoadedReadonlyCount(usize),
        LoadedReadonly(CompactPubkey),
    }

    fn assert_metadata_stream_matches_owned(
        bytes: &[u8],
        schema: CompactV2MetadataSchema,
        limits: MetadataDecodeLimits,
    ) -> (DecodedMetadataPrefix, Vec<OwnedMetadataAccountEvent>) {
        let mut owned_cursor = bytes;
        let mut owned_inner = Vec::new();
        let owned = decode_metadata_prefix_with_schema(
            &mut owned_cursor,
            schema,
            true,
            limits,
            |instruction| {
                owned_inner.push((instruction.program_id_index, instruction.accounts.to_vec()));
            },
        )
        .unwrap();

        let mut streamed_cursor = bytes;
        let mut streamed_events = Vec::new();
        let streamed = stream_metadata_accounts_with_schema(
            &mut streamed_cursor,
            schema,
            true,
            limits,
            |event| {
                streamed_events.push(match event {
                    MetadataAccountEvent::InnerInstruction(instruction) => {
                        OwnedMetadataAccountEvent::InnerInstruction {
                            program_id_index: instruction.program_id_index,
                            accounts: instruction.accounts.to_vec(),
                        }
                    }
                    MetadataAccountEvent::LoadedWritableCount(count) => {
                        OwnedMetadataAccountEvent::LoadedWritableCount(count)
                    }
                    MetadataAccountEvent::LoadedWritable(key) => {
                        OwnedMetadataAccountEvent::LoadedWritable(key)
                    }
                    MetadataAccountEvent::LoadedReadonlyCount(count) => {
                        OwnedMetadataAccountEvent::LoadedReadonlyCount(count)
                    }
                    MetadataAccountEvent::LoadedReadonly(key) => {
                        OwnedMetadataAccountEvent::LoadedReadonly(key)
                    }
                });
                Ok::<(), wincode::error::ReadError>(())
            },
        )
        .unwrap();

        let (loaded_writable, loaded_readonly) = owned.loaded_addresses.as_ref().unwrap();
        let mut expected_events = owned_inner
            .iter()
            .map(
                |(program_id_index, accounts)| OwnedMetadataAccountEvent::InnerInstruction {
                    program_id_index: *program_id_index,
                    accounts: accounts.clone(),
                },
            )
            .collect::<Vec<_>>();
        expected_events.push(OwnedMetadataAccountEvent::LoadedWritableCount(
            loaded_writable.len(),
        ));
        expected_events.extend(
            loaded_writable
                .iter()
                .copied()
                .map(OwnedMetadataAccountEvent::LoadedWritable),
        );
        expected_events.push(OwnedMetadataAccountEvent::LoadedReadonlyCount(
            loaded_readonly.len(),
        ));
        expected_events.extend(
            loaded_readonly
                .iter()
                .copied()
                .map(OwnedMetadataAccountEvent::LoadedReadonly),
        );

        assert_eq!(streamed_events, expected_events);
        assert_eq!(streamed_cursor, owned_cursor);
        assert_eq!(streamed.has_error, owned.has_error);
        assert_eq!(
            streamed.inner_instructions_present,
            owned.inner_instructions_present
        );
        assert_eq!(streamed.loaded_writable_count, loaded_writable.len());
        assert_eq!(streamed.loaded_readonly_count, loaded_readonly.len());
        (owned, streamed_events)
    }

    fn assert_message_stream_error_matches_owned(bytes: &[u8], schema: CompactV2MessageSchema) {
        let mut owned_cursor = bytes;
        let owned_error = decode_message_with_schema(&mut owned_cursor, schema, |_| {})
            .err()
            .expect("owned message decoder must reject the fixture")
            .to_string();
        let mut streamed_cursor = bytes;
        let streamed_error =
            stream_message_accounts_with_schema(&mut streamed_cursor, schema, |_| {
                Ok::<(), wincode::error::ReadError>(())
            })
            .unwrap_err()
            .to_string();
        assert_eq!(streamed_error, owned_error);
        assert_eq!(streamed_cursor, owned_cursor);
    }

    fn assert_metadata_stream_error_matches_owned(
        bytes: &[u8],
        schema: CompactV2MetadataSchema,
        limits: MetadataDecodeLimits,
    ) {
        let mut owned_cursor = bytes;
        let owned_error =
            decode_metadata_prefix_with_schema(&mut owned_cursor, schema, true, limits, |_| {})
                .err()
                .expect("owned metadata decoder must reject the fixture")
                .to_string();
        let mut streamed_cursor = bytes;
        let streamed_error = stream_metadata_accounts_with_schema(
            &mut streamed_cursor,
            schema,
            true,
            limits,
            |_| Ok::<(), wincode::error::ReadError>(()),
        )
        .unwrap_err()
        .to_string();
        assert_eq!(streamed_error, owned_error);
        assert_eq!(streamed_cursor, owned_cursor);
    }

    fn legacy_raw_metadata_bytes(
        raw_error: Option<Vec<u8>>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
    ) -> Vec<u8> {
        let mut bytes = serialize(&raw_error);
        append(&mut bytes, &5_000_u64);
        append(&mut bytes, &Vec::<u64>::new());
        append(&mut bytes, &Vec::<u64>::new());
        append(&mut bytes, &inner_instructions);
        append(&mut bytes, &Option::<CompactLogStream>::None);
        append(&mut bytes, &Vec::<CompactTokenBalance>::new());
        append(&mut bytes, &Vec::<CompactTokenBalance>::new());
        append(&mut bytes, &Vec::<CompactReward>::new());
        append(&mut bytes, &loaded_writable_addresses);
        append(&mut bytes, &loaded_readonly_addresses);
        append(
            &mut bytes,
            &Option::<blockzilla_format::CompactReturnData>::None,
        );
        append(&mut bytes, &Option::<u64>::None);
        append(&mut bytes, &Option::<u64>::None);
        bytes
    }

    #[test]
    fn streaming_message_events_match_owned_current_and_may24_profiles() {
        let legacy = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Raw([2; 32]),
                CompactPubkey::Id(3),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 2,
                accounts: vec![1, 0, 1],
                data: ArchiveV2HotInstructionData::Raw(vec![7, 8]),
            }],
        });
        let legacy_bytes = serialize(&legacy);
        for schema in [
            CompactV2MessageSchema::Current,
            CompactV2MessageSchema::May24PreUnknownFallbacks,
        ] {
            let (decoded, events) = assert_message_stream_matches_owned(&legacy_bytes, schema);
            assert!(!decoded.is_v0);
            assert_eq!(decoded.account_keys.len(), 3);
            assert_eq!(events.len(), 5);
        }

        let v0 = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(4), CompactPubkey::Raw([5; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([6; 32]),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0, 2, 3],
                data: ArchiveV2HotInstructionData::Raw(Vec::new()),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(7),
                writable_indexes: vec![0],
                readonly_indexes: vec![1, 2],
            }],
        });
        let (decoded, events) =
            assert_message_stream_matches_owned(&serialize(&v0), CompactV2MessageSchema::Current);
        assert!(decoded.is_v0);
        assert_eq!(decoded.expected_loaded_writable, 1);
        assert_eq!(decoded.expected_loaded_readonly, 2);
        assert_eq!(events.len(), 4);

        let v1 = ArchiveV2HotMessagePayload::V1(ArchiveV2HotV1Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            config: CompactTransactionConfig {
                priority_fee: Some(9),
                compute_unit_limit: Some(10),
                loaded_accounts_data_size_limit: Some(11),
                heap_size: Some(12),
            },
            account_keys: vec![CompactPubkey::Raw([8; 32]), CompactPubkey::Id(9)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::Raw(vec![13]),
            }],
        });
        let (decoded, events) =
            assert_message_stream_matches_owned(&serialize(&v1), CompactV2MessageSchema::Current);
        assert!(!decoded.is_v0);
        assert_eq!(decoded.expected_loaded_writable, 0);
        assert_eq!(decoded.expected_loaded_readonly, 0);
        assert_eq!(events.len(), 4);
    }

    #[test]
    fn signer_prefix_uses_the_selected_message_schema() {
        let message = ArchiveV2HotMessagePayload::V1(ArchiveV2HotV1Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            config: CompactTransactionConfig {
                priority_fee: None,
                compute_unit_limit: None,
                loaded_accounts_data_size_limit: None,
                heap_size: None,
            },
            account_keys: vec![CompactPubkey::Id(9)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
            instructions: vec![],
        });
        let bytes = serialize(&message);
        assert_eq!(
            decode_signers_with_schema(&mut bytes.as_slice(), CompactV2MessageSchema::Current)
                .unwrap()
                .as_slice(),
            &[CompactPubkey::Id(9)]
        );
        assert!(
            decode_signers_with_schema(
                &mut bytes.as_slice(),
                CompactV2MessageSchema::May24PreUnknownFallbacks,
            )
            .is_err()
        );
    }

    #[test]
    fn streaming_metadata_events_match_owned_current_and_legacy_profiles() {
        let inner_instructions = Some(vec![CompactInnerInstructions {
            index: 1,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 2,
                accounts: vec![0, 3, 0],
                data: vec![9, 8, 7],
                stack_height: Some(2),
            }],
        }]);
        let loaded_writable = vec![CompactPubkey::Id(3)];
        let loaded_readonly = vec![CompactPubkey::Raw([4; 32])];
        let current = CompactMetaV1 {
            err: Some(CompactTransactionError::InvalidProgramForExecution),
            fee: 5_000,
            pre_balances: vec![10, 20],
            post_balances: vec![9, 21],
            inner_instructions: inner_instructions.clone(),
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: loaded_writable.clone(),
            loaded_readonly_addresses: loaded_readonly.clone(),
            return_data: None,
            compute_units_consumed: Some(42),
            cost_units: Some(84),
        };
        let limits = MetadataDecodeLimits {
            total_message_accounts: 4,
            top_level_instruction_count: 2,
        };
        let (decoded, events) = assert_metadata_stream_matches_owned(
            &serialize(&current),
            CompactV2MetadataSchema::CurrentTypedError,
            limits,
        );
        assert!(decoded.has_error);
        assert!(decoded.inner_instructions_present);
        assert_eq!(events.len(), 5);

        let legacy = legacy_raw_metadata_bytes(
            Some(vec![8, 0, 0, 0]),
            inner_instructions,
            loaded_writable,
            loaded_readonly,
        );
        let (decoded, events) = assert_metadata_stream_matches_owned(
            &legacy,
            CompactV2MetadataSchema::LegacyRawError,
            limits,
        );
        assert!(decoded.has_error);
        assert!(decoded.inner_instructions_present);
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn streaming_decoders_preserve_owned_errors_and_callback_failures() {
        let valid = serialize(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
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
                    data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3]),
                }],
            },
        ));
        assert_message_stream_error_matches_owned(
            &valid[..valid.len() - 1],
            CompactV2MessageSchema::Current,
        );
        assert_message_stream_error_matches_owned(
            &serialize(&3_u32),
            CompactV2MessageSchema::Current,
        );
        assert_message_stream_error_matches_owned(
            &serialize(&ArchiveV2HotMessagePayload::Legacy(
                ArchiveV2HotLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![CompactPubkey::Id(1); MAX_MESSAGE_ACCOUNTS + 1],
                    recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                    instructions: Vec::new(),
                },
            )),
            CompactV2MessageSchema::Current,
        );
        let mut may24_rejects_v1 = serialize(&2_u32);
        append(
            &mut may24_rejects_v1,
            &CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
        );
        assert_message_stream_error_matches_owned(
            &may24_rejects_v1,
            CompactV2MessageSchema::May24PreUnknownFallbacks,
        );

        let callback_error = stream_message_accounts_with_schema(
            &mut valid.as_slice(),
            CompactV2MessageSchema::Current,
            |event| match event {
                MessageAccountEvent::StaticAccount { .. } => {
                    Err(anyhow::anyhow!("message callback stopped"))
                }
                _ => Ok(()),
            },
        )
        .unwrap_err();
        assert_eq!(callback_error.to_string(), "message callback stopped");

        let limits = MetadataDecodeLimits {
            total_message_accounts: 1,
            top_level_instruction_count: 1,
        };
        let mut invalid_inner_tag = serialize(&Option::<CompactTransactionError>::None);
        append(&mut invalid_inner_tag, &0_u64);
        append(&mut invalid_inner_tag, &Vec::<u64>::new());
        append(&mut invalid_inner_tag, &Vec::<u64>::new());
        append(&mut invalid_inner_tag, &2_u8);
        assert_metadata_stream_error_matches_owned(
            &invalid_inner_tag,
            CompactV2MetadataSchema::CurrentTypedError,
            limits,
        );

        let loaded_over_bound = CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        assert_metadata_stream_error_matches_owned(
            &serialize(&loaded_over_bound),
            CompactV2MetadataSchema::CurrentTypedError,
            limits,
        );

        let valid_metadata = CompactMetaV1 {
            loaded_writable_addresses: vec![CompactPubkey::Id(1)],
            ..loaded_over_bound
        };
        let valid_metadata = serialize(&valid_metadata);
        let mut consumed_cursor = valid_metadata.as_slice();
        stream_metadata_accounts_with_schema(
            &mut consumed_cursor,
            CompactV2MetadataSchema::CurrentTypedError,
            true,
            limits,
            |_| Ok::<(), wincode::error::ReadError>(()),
        )
        .unwrap();
        let consumed = valid_metadata.len() - consumed_cursor.len();
        assert_metadata_stream_error_matches_owned(
            &valid_metadata[..consumed - 1],
            CompactV2MetadataSchema::CurrentTypedError,
            limits,
        );

        let callback_error = stream_metadata_accounts_with_schema(
            &mut valid_metadata.as_slice(),
            CompactV2MetadataSchema::CurrentTypedError,
            true,
            limits,
            |event| match event {
                MetadataAccountEvent::LoadedWritable(_) => {
                    Err(anyhow::anyhow!("metadata callback stopped"))
                }
                _ => Ok(()),
            },
        )
        .unwrap_err();
        assert_eq!(callback_error.to_string(), "metadata callback stopped");
    }

    #[test]
    fn allocation_bearing_instruction_and_error_variants_stream_without_owned_values() {
        let system = ArchiveV2HotInstructionData::System(
            ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                base: [1; 32],
                seed: "seed".into(),
                lamports: 2,
                space: 3,
                owner: [4; 32],
            },
        );
        let system_bytes = serialize(&system);
        let mut cursor = system_bytes.as_slice();
        skip_instruction_data(&mut cursor, CompactV2MessageSchema::Current).unwrap();
        assert!(cursor.is_empty());

        let vote =
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate {
                root: Some(1),
                lockout_offsets: vec![ArchiveV2VoteLockoutOffset {
                    offset: 2,
                    confirmation_count: 3,
                }],
                hash: ArchiveV2VoteHashRef::Raw([5; 32]),
                timestamp: Some(4),
            });
        let vote_bytes = serialize(&vote);
        let mut cursor = vote_bytes.as_slice();
        skip_instruction_data(&mut cursor, CompactV2MessageSchema::Current).unwrap();
        assert!(cursor.is_empty());

        let outcome = Some(CompactTransactionError::InstructionError(
            7,
            CompactInstructionError::BorshIoError("borsh".into()),
        ));
        let outcome_bytes = serialize(&outcome);
        let mut cursor = outcome_bytes.as_slice();
        assert!(decode_metadata_error(&mut cursor).unwrap());
        assert!(cursor.is_empty());
    }

    #[test]
    fn allocation_bearing_known_program_logs_stream_without_owned_values() {
        let events = [
            LogEvent::ProgramLog(ProgramLog::Known(KnownProgramLog::Drift(DriftLog::Event(
                vec![1, 2, 3],
            )))),
            LogEvent::ProgramLog(ProgramLog::Known(KnownProgramLog::OkxRouter(
                OkxRouterLog::DexAmountIn {
                    dex: "dex".into(),
                    amount_in: 1,
                    offset: 2,
                    spelling: AmountInSpelling::Underscore,
                },
            ))),
            LogEvent::ProgramLog(ProgramLog::Known(KnownProgramLog::PhoenixPerps(
                PhoenixPerpsLog::Event(vec![4, 5, 6]),
            ))),
            LogEvent::ProgramLog(ProgramLog::Known(KnownProgramLog::PhoenixV1(
                PhoenixLog::Discriminant {
                    type_name: "type".into(),
                    value: 9,
                },
            ))),
        ];
        for event in events {
            let bytes = serialize(&event);
            let mut cursor = bytes.as_slice();
            skip_log_event(&mut cursor).unwrap();
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn hostile_huge_nested_lengths_are_rejected_from_tiny_inputs() {
        let huge_length = serialize(&u64::MAX);

        let mut system = Vec::new();
        append(&mut system, &4u32); // ArchiveV2HotInstructionData::System
        append(&mut system, &3u32); // CreateAccountWithSeed
        append(&mut system, &[0u8; 32]);
        system.extend_from_slice(&huge_length);
        assert!(
            skip_instruction_data(&mut system.as_slice(), CompactV2MessageSchema::Current).is_err()
        );

        let mut vote = Vec::new();
        append(&mut vote, &5u32); // VoteCompactUpdateVoteState
        append(&mut vote, &Option::<u64>::None);
        vote.extend_from_slice(&huge_length);
        assert!(
            skip_instruction_data(&mut vote.as_slice(), CompactV2MessageSchema::Current).is_err()
        );

        let mut outcome = Vec::new();
        append(&mut outcome, &1u8); // Some(transaction error)
        append(&mut outcome, &8u8); // InstructionError
        append(&mut outcome, &0u8); // instruction index
        append(&mut outcome, &44u8); // BorshIoError
        outcome.extend_from_slice(&huge_length);
        assert!(decode_metadata_error(&mut outcome.as_slice()).is_err());

        let mut log = Vec::new();
        append(&mut log, &5u32); // LogEvent::ProgramLog
        append(&mut log, &17u32); // ProgramLog::Known
        append(&mut log, &0u32); // KnownProgramLog::Drift
        append(&mut log, &0u32); // DriftLog::Event
        log.extend_from_slice(&huge_length);
        assert!(skip_log_event(&mut log.as_slice()).is_err());
    }

    #[test]
    fn decode_message_matches_legacy_message_written_by_the_real_writer() {
        let payload = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
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
                    data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3, 4, 5]),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: vec![1, 0],
                    data: ArchiveV2HotInstructionData::ComputeBudget(
                        blockzilla_format::ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(1_000),
                    ),
                },
            ],
        });
        let bytes = serialize(&payload);

        let mut cursor = bytes.as_slice();
        let mut instructions = Vec::new();
        let decoded = decode_message(&mut cursor, |instruction| {
            instructions.push((instruction.program_id_index, instruction.accounts.to_vec()));
        })
        .unwrap();

        assert!(
            cursor.is_empty(),
            "decode_message must consume the whole message"
        );
        assert!(!decoded.is_v0);
        assert_eq!(decoded.num_required_signatures, 2);
        assert_eq!(decoded.instruction_count, 2);
        assert_eq!(decoded.expected_loaded_writable, 0);
        assert_eq!(decoded.expected_loaded_readonly, 0);
        assert_eq!(
            decoded.account_keys,
            vec![
                CompactPubkey::Id(10),
                CompactPubkey::Id(20),
                CompactPubkey::Id(30)
            ]
        );
        assert_eq!(instructions, vec![(2, vec![0]), (2, vec![1, 0])]);

        // Same bytes, independently decoded by decode_signers: only the
        // first `num_required_signatures` account_keys entries.
        let mut signers_cursor = bytes.as_slice();
        let signers = decode_signers(&mut signers_cursor).unwrap();
        assert_eq!(
            signers.as_slice(),
            &[CompactPubkey::Id(10), CompactPubkey::Id(20)]
        );
    }

    #[test]
    fn decode_message_matches_v0_message_with_address_table_lookups() {
        let payload = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
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
                data: ArchiveV2HotInstructionData::Raw(vec![9, 9]),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(99),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let bytes = serialize(&payload);

        let mut cursor = bytes.as_slice();
        let mut instructions = Vec::new();
        let decoded = decode_message(&mut cursor, |instruction| {
            instructions.push((instruction.program_id_index, instruction.accounts.to_vec()));
        })
        .unwrap();

        assert!(
            cursor.is_empty(),
            "decode_message must consume address_table_lookups too, for a V0 message"
        );
        assert!(decoded.is_v0);
        assert_eq!(decoded.num_required_signatures, 1);
        assert_eq!(decoded.instruction_count, 1);
        assert_eq!(decoded.expected_loaded_writable, 1);
        assert_eq!(decoded.expected_loaded_readonly, 1);
        assert_eq!(
            decoded.account_keys,
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)]
        );
        assert_eq!(instructions, vec![(1, vec![0])]);

        let mut signers_cursor = bytes.as_slice();
        let signers = decode_signers(&mut signers_cursor).unwrap();
        assert_eq!(signers.as_slice(), &[CompactPubkey::Id(1)]);
    }

    #[test]
    fn decode_message_rejects_more_than_256_static_accounts() {
        let payload = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1); MAX_MESSAGE_ACCOUNTS + 1],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
        });
        let bytes = serialize(&payload);
        assert!(decode_message(&mut bytes.as_slice(), |_| {}).is_err());
        assert!(decode_signers(&mut bytes.as_slice()).is_err());
    }

    #[test]
    fn decode_message_rejects_oversized_lookup_index_slice() {
        let payload = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(2),
                writable_indexes: vec![0; MAX_MESSAGE_ACCOUNTS + 1],
                readonly_indexes: vec![],
            }],
        });
        let bytes = serialize(&payload);
        assert!(decode_message(&mut bytes.as_slice(), |_| {}).is_err());
    }

    #[test]
    fn decode_metadata_prefix_legacy_stops_after_inner_instructions() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![100, 200],
            post_balances: vec![90, 210],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 1],
                    data: vec![7, 7, 7],
                    stack_height: Some(2),
                }],
            }]),
            logs: Some(CompactLogStream {
                events: vec![LogEvent::LogTruncated],
                strings: StringTable {
                    lengths: vec![3],
                    bytes: b"log".to_vec(),
                },
                data: DataTable {
                    arrays: vec![DataArray { chunk_count: 1 }],
                    chunk_lengths: vec![2],
                    bytes: vec![1, 2],
                },
            }),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(70)),
                owner: None,
                program_id: Some(CompactPubkey::Id(71)),
                amount: 10,
                decimals: 6,
            }],
            post_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(70)),
                owner: None,
                program_id: Some(CompactPubkey::Id(71)),
                amount: 9,
                decimals: 6,
            }],
            rewards: vec![CompactReward {
                pubkey: CompactPubkey::Id(72),
                lamports: 1,
                post_balance: 1,
                reward_type: 0,
                commission: None,
            }],
            loaded_writable_addresses: vec![CompactPubkey::Id(50)],
            loaded_readonly_addresses: vec![CompactPubkey::Id(60)],
            return_data: None,
            compute_units_consumed: Some(100),
            cost_units: Some(100),
        };
        let bytes = serialize(&metadata);

        let mut cursor = bytes.as_slice();
        let mut inner = Vec::new();
        let decoded = decode_metadata_prefix(
            &mut cursor,
            false,
            MetadataDecodeLimits {
                total_message_accounts: 3,
                top_level_instruction_count: 1,
            },
            |instruction| {
                inner.push((instruction.program_id_index, instruction.accounts.to_vec()));
            },
        )
        .unwrap();

        assert!(!decoded.has_error);
        assert!(decoded.inner_instructions_present);
        assert!(
            decoded.loaded_addresses.is_none(),
            "legacy (need_loaded_addresses=false) must not decode the tail"
        );
        assert_eq!(inner, vec![(2, vec![0, 1])]);
        assert!(
            !cursor.is_empty(),
            "legacy stops right after inner_instructions — logs/balances/rewards/addresses/\
             return_data/compute_units are never decoded, so bytes remain"
        );
    }

    #[test]
    fn decode_metadata_prefix_v0_decodes_through_loaded_addresses() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![100],
            post_balances: vec![90],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 3,
                    accounts: vec![1],
                    data: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    stack_height: None,
                }],
            }]),
            logs: Some(CompactLogStream {
                events: vec![LogEvent::LogTruncated],
                strings: StringTable {
                    lengths: vec![3],
                    bytes: b"log".to_vec(),
                },
                data: DataTable {
                    arrays: vec![DataArray { chunk_count: 1 }],
                    chunk_lengths: vec![2],
                    bytes: vec![1, 2],
                },
            }),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(70)),
                owner: None,
                program_id: Some(CompactPubkey::Id(71)),
                amount: 10,
                decimals: 6,
            }],
            post_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(70)),
                owner: None,
                program_id: Some(CompactPubkey::Id(71)),
                amount: 9,
                decimals: 6,
            }],
            rewards: vec![CompactReward {
                pubkey: CompactPubkey::Id(72),
                lamports: 1,
                post_balance: 1,
                reward_type: 0,
                commission: None,
            }],
            loaded_writable_addresses: vec![CompactPubkey::Id(50), CompactPubkey::Id(51)],
            loaded_readonly_addresses: vec![CompactPubkey::Id(60)],
            return_data: None,
            compute_units_consumed: Some(100),
            cost_units: None,
        };
        let bytes = serialize(&metadata);

        let mut cursor = bytes.as_slice();
        let mut inner = Vec::new();
        let decoded = decode_metadata_prefix(
            &mut cursor,
            true,
            MetadataDecodeLimits {
                total_message_accounts: 4,
                top_level_instruction_count: 1,
            },
            |instruction| {
                inner.push((instruction.program_id_index, instruction.accounts.to_vec()));
            },
        )
        .unwrap();

        assert_eq!(inner, vec![(3, vec![1])]);
        assert!(!decoded.has_error);
        assert!(decoded.inner_instructions_present);
        let (writable, readonly) = decoded
            .loaded_addresses
            .expect("V0 (need_loaded_addresses=true) must decode the addresses");
        assert_eq!(writable, vec![CompactPubkey::Id(50), CompactPubkey::Id(51)]);
        assert_eq!(readonly, vec![CompactPubkey::Id(60)]);
        // Still stops before return_data/compute_units/cost_units — this
        // indexer never needs them, so bytes may remain; only asserting we
        // got the addresses correctly is the point of this test.
    }

    #[test]
    fn decode_metadata_prefix_rejects_invalid_inner_option_tag() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![],
            loaded_readonly_addresses: vec![],
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let mut bytes = serialize(&metadata);
        // None(err), fee=0, two empty balance lengths, then the inner Option tag.
        assert_eq!(bytes[4], 0);
        bytes[4] = 2;
        let mut cursor = bytes.as_slice();
        assert!(
            decode_metadata_prefix(
                &mut cursor,
                false,
                MetadataDecodeLimits {
                    total_message_accounts: 0,
                    top_level_instruction_count: 0,
                },
                |_| {},
            )
            .is_err()
        );
    }

    #[test]
    fn decode_metadata_prefix_bounds_balance_arrays_by_message_accounts() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![1, 2],
            post_balances: vec![],
            inner_instructions: None,
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![],
            loaded_readonly_addresses: vec![],
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let bytes = serialize(&metadata);
        assert!(
            decode_metadata_prefix(
                &mut bytes.as_slice(),
                false,
                MetadataDecodeLimits {
                    total_message_accounts: 1,
                    top_level_instruction_count: 0,
                },
                |_| {},
            )
            .is_err()
        );
    }

    #[test]
    fn decode_metadata_prefix_rejects_out_of_range_inner_group_index() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 1,
                instructions: vec![],
            }]),
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![],
            loaded_readonly_addresses: vec![],
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let bytes = serialize(&metadata);
        assert!(
            decode_metadata_prefix(
                &mut bytes.as_slice(),
                false,
                MetadataDecodeLimits {
                    total_message_accounts: 1,
                    top_level_instruction_count: 1,
                },
                |_| {},
            )
            .is_err()
        );
    }

    #[test]
    fn decode_metadata_prefix_rejects_inner_count_larger_than_remaining_input() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![],
            }]),
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![],
            loaded_readonly_addresses: vec![],
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let mut bytes = serialize(&metadata);
        // None(err), fee=0, two empty balance vectors, Some(inner), one
        // group, group index 0, then its inner-instruction vector length.
        assert_eq!(&bytes[..8], &[0, 0, 0, 0, 1, 1, 0, 0]);
        bytes[7] = 0x7f;

        assert!(
            decode_metadata_prefix(
                &mut bytes.as_slice(),
                false,
                MetadataDecodeLimits {
                    total_message_accounts: 1,
                    top_level_instruction_count: 1,
                },
                |_| {},
            )
            .is_err()
        );
    }
}
