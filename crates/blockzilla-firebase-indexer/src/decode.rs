//! Hand-written, zero-copy decoders for the specific fields this indexer
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
//! signers than a real build's wallet count, which should be impossible),
//! not a crash — exactly the failure mode this module's docs warn about,
//! and why the tests below check exact decoded values, not just "did it
//! return without erroring."

use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2VoteHashRef, CompactInnerInstruction,
    CompactMessageHeader, CompactPubkey, CompactReward, CompactTokenBalance, DataArray,
    OwnedCompactRecentBlockhash, WincodeLeb128Config,
};
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
fn skip_instruction_data(cursor: &mut &[u8]) -> ReadResult<()> {
    let tag = get::<u32>(cursor)?;
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

fn read_instruction<'de>(cursor: &mut &'de [u8]) -> ReadResult<BorrowedInstruction<'de>> {
    let program_id_index = get::<u8>(cursor)?;
    // Repeated account indices are legal, so this slice's *length* is not
    // bounded by the number of distinct message accounts. It is still
    // strictly bounded by the bytes available and remains borrowed.
    let accounts_len = read_len_bounded_by_remaining(
        cursor,
        "instruction account-index count exceeds remaining input",
    )?;
    let accounts = cursor.take_borrowed(accounts_len)?;
    skip_instruction_data(cursor)?;
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
    match get::<u32>(cursor)? {
        0 | 1 => {}
        other => return Err(invalid_tag_encoding(other as usize)),
    }
    let header = get::<CompactMessageHeader>(cursor)?;
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
/// signers) — callers use it to tell a real signing wallet from a merely-
/// referenced account. For a V0 message, `address_table_lookups` is streamed
/// far enough to validate and count its writable/readonly indices (its
/// resolved form lives in transaction metadata), and the cursor is positioned
/// exactly at the end of the message, same as the legacy case — callers
/// that want to assert full consumption can rely on that.
pub struct DecodedMessage {
    pub account_keys: Vec<CompactPubkey>,
    pub is_v0: bool,
    pub num_required_signatures: u8,
    pub instruction_count: usize,
    pub expected_loaded_writable: usize,
    pub expected_loaded_readonly: usize,
}

pub fn decode_message<'de>(
    cursor: &mut &'de [u8],
    mut on_instruction: impl FnMut(BorrowedInstruction<'de>),
) -> ReadResult<DecodedMessage> {
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
    get::<OwnedCompactRecentBlockhash>(cursor)?;

    let instruction_count = read_len_bounded_by_remaining(
        cursor,
        "top-level instruction count exceeds remaining input",
    )?;
    for _ in 0..instruction_count {
        on_instruction(read_instruction(cursor)?);
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

    Ok(DecodedMessage {
        account_keys,
        is_v0,
        num_required_signatures: header.num_required_signatures,
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

pub struct DecodedMetadataPrefix {
    pub has_error: bool,
    pub inner_instructions_present: bool,
    pub loaded_addresses: Option<(Vec<CompactPubkey>, Vec<CompactPubkey>)>,
}

/// Decode just the archived transaction outcome. This is sufficient when the
/// row flags prove there are neither inner instructions nor loaded addresses.
pub fn decode_metadata_error(cursor: &mut &[u8]) -> ReadResult<bool> {
    match get::<u8>(cursor)? {
        0 => Ok(false),
        1 => {
            skip_transaction_error(cursor)?;
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
fn skip_logs(cursor: &mut &[u8]) -> ReadResult<()> {
    match get::<u8>(cursor)? {
        0 => Ok(()),
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
            Ok(())
        }
        other => Err(invalid_tag_encoding(other as usize)),
    }
}

fn skip_token_balances(cursor: &mut &[u8], maximum: usize) -> ReadResult<()> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "token-balance count exceeds total message account count",
    )?;
    for _ in 0..count {
        get::<CompactTokenBalance>(cursor)?;
    }
    Ok(())
}

fn skip_rewards(cursor: &mut &[u8]) -> ReadResult<()> {
    let count = read_len_bounded_by_remaining(cursor, "reward count exceeds remaining input")?;
    for _ in 0..count {
        get::<CompactReward>(cursor)?;
    }
    Ok(())
}

fn read_loaded_addresses(cursor: &mut &[u8], maximum: usize) -> ReadResult<Vec<CompactPubkey>> {
    let count = read_bounded_len(
        cursor,
        maximum,
        "loaded address count exceeds total message account count",
    )?;
    let mut addresses = Vec::with_capacity(count);
    for _ in 0..count {
        addresses.push(get::<CompactPubkey>(cursor)?);
    }
    Ok(addresses)
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
    mut on_inner_instruction: impl FnMut(BorrowedInnerInstruction<'de>),
) -> ReadResult<DecodedMetadataPrefix> {
    if limits.total_message_accounts > MAX_MESSAGE_ACCOUNTS {
        return Err(wincode::error::invalid_value(
            "total message account count exceeds message account cap",
        ));
    }
    let has_error = decode_metadata_error(cursor)?;
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
                    ));
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
                        ));
                    }
                    on_inner_instruction(instruction);
                }
            }
            true
        }
        other => return Err(invalid_tag_encoding(other as usize)),
    };

    if !need_loaded_addresses {
        return Ok(DecodedMetadataPrefix {
            has_error,
            inner_instructions_present,
            loaded_addresses: None,
        });
    }

    skip_logs(cursor)?;
    skip_token_balances(cursor, limits.total_message_accounts)?;
    skip_token_balances(cursor, limits.total_message_accounts)?;
    skip_rewards(cursor)?;
    let loaded_writable_addresses = read_loaded_addresses(cursor, limits.total_message_accounts)?;
    let loaded_readonly_addresses = read_loaded_addresses(cursor, limits.total_message_accounts)?;
    if loaded_writable_addresses.len() + loaded_readonly_addresses.len()
        > limits.total_message_accounts
    {
        return Err(wincode::error::invalid_value(
            "loaded address count exceeds total message account count",
        ));
    }
    Ok(DecodedMetadataPrefix {
        has_error,
        inner_instructions_present,
        loaded_addresses: Some((loaded_writable_addresses, loaded_readonly_addresses)),
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
        ArchiveV2HotMessagePayload, ArchiveV2HotV0Message, ArchiveV2SystemInstructionData,
        ArchiveV2VoteLockoutOffset, ArchiveV2VoteStateUpdate, CompactInnerInstructions,
        CompactInstructionError, CompactLogStream, CompactMessageHeader, CompactMetaV1,
        CompactTransactionError, DataTable, LogEvent, OwnedCompactAddressTableLookup,
        OwnedCompactRecentBlockhash, StringTable, wincode_leb128_config,
    };

    fn serialize<T: wincode::SchemaWrite<Cfg, Src = T>>(value: &T) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn append<T: wincode::SchemaWrite<Cfg, Src = T>>(bytes: &mut Vec<u8>, value: &T) {
        bytes.extend(serialize(value));
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
        skip_instruction_data(&mut cursor).unwrap();
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
        skip_instruction_data(&mut cursor).unwrap();
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
        assert!(skip_instruction_data(&mut system.as_slice()).is_err());

        let mut vote = Vec::new();
        append(&mut vote, &5u32); // VoteCompactUpdateVoteState
        append(&mut vote, &Option::<u64>::None);
        vote.extend_from_slice(&huge_length);
        assert!(skip_instruction_data(&mut vote.as_slice()).is_err());

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
