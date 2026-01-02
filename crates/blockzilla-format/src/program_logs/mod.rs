use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{Registry, StrId, StringTable};

pub mod account_compression;
pub mod address_lookup_table;
pub mod associated_token_account;
pub mod loader_v3;
pub mod loader_v4;
pub mod memo;
pub mod record;
pub mod system_program;
pub mod token;
pub mod token_2022;
pub mod transfer_hook;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ProgramLog {
    /// SPL Token program logs
    Token(token::TokenLog),

    /// SPL Token-2022 program logs
    Token2022(token_2022::Token2022Log),

    Ata(associated_token_account::TokenErrorLog),

    /// Address Lookup Table
    AddressLookupTable(address_lookup_table::AddressLookupTableLog),

    /// BPF Upgradeable Loader (loader-v3)
    LoaderV3(loader_v3::LoaderV3Log),

    /// Loader v4
    LoaderV4(loader_v4::LoaderV4Log),

    /// Memo program
    Memo(memo::MemoLog),

    /// Record program
    Record(record::RecordLog),

    /// Transfer-hook program/interface errors
    TransferHook(transfer_hook::TransferHookLog),

    /// Account-compression
    AccountCompression(account_compression::AccountCompressionLog),

    /// Anchor programs commonly log: `Instruction: <Name>`
    /// Store only the instruction name in the string table.
    AnchorInstruction {
        name: StrId,
    },

    AnchorErrorThrown {
        file: StrId,
        line: u32,
        code: StrId,
        number: u32,
        msg: StrId,
    },

    /// Fallback for unsupported programs or unparsed payloads
    /// Stored verbatim via string table
    Unknown(StrId),
}

/// `Program log: <msg>`
///
/// No program id available -> best-effort parse across known log formats.
/// This can misclassify if two programs share identical text patterns,
/// but it's useful for logs that omit program id.
#[inline]
pub fn parse_program_log_no_id(
    payload: &str,
    registry: &Registry,
    st: &mut StringTable,
) -> ProgramLog {
    // -----------------------------
    // 1) zero-alloc / no StringTable
    // -----------------------------
    if let Some(t) = token::TokenLog::parse(payload) {
        return ProgramLog::Token(t);
    }

    if let Some(t) = associated_token_account::TokenErrorLog::parse(payload) {
        return ProgramLog::Ata(t);
    }

    // Program-agnostic Anchor patterns (cheap)
    if let Some(ev) = parse_anchor_instruction(payload, st) {
        return ev;
    }

    // -----------------------------
    // 2) parsers that may push to StringTable (and/or need registry)
    // -----------------------------
    if let Some(t) = token_2022::Token2022Log::parse(payload, registry, st) {
        return ProgramLog::Token2022(t);
    }

    if let Some(x) = address_lookup_table::AddressLookupTableLog::parse(payload, st) {
        return ProgramLog::AddressLookupTable(x);
    }

    if let Some(x) = loader_v3::LoaderV3Log::parse(payload, st) {
        return ProgramLog::LoaderV3(x);
    }

    if let Some(x) = loader_v4::LoaderV4Log::parse(payload, st) {
        return ProgramLog::LoaderV4(x);
    }

    if let Some(x) = memo::MemoLog::parse(payload, st) {
        return ProgramLog::Memo(x);
    }

    if let Some(x) = record::RecordLog::parse(payload, st) {
        return ProgramLog::Record(x);
    }

    if let Some(x) = transfer_hook::TransferHookLog::parse(payload, st) {
        return ProgramLog::TransferHook(x);
    }

    if let Some(x) = account_compression::AccountCompressionLog::parse(payload, st) {
        return ProgramLog::AccountCompression(x);
    }

    // Program-agnostic Anchor error (may push multiple strings)
    if let Some(ev) = parse_anchor_error(payload, st) {
        return ev;
    }

    ProgramLog::Unknown(st.push(payload))
}

/// `Program <id> log: <msg>`
///
/// Dispatch by program id, fall back to raw.
#[inline]
pub fn parse_program_log_for_program(
    program: &str,
    payload: &str,
    registry: &Registry,
    st: &mut StringTable,
) -> ProgramLog {
    if let Some(log) = try_parse_program_log_with_table(program, payload, registry, st) {
        return log;
    }

    // anchor instruction is program-agnostic
    if let Some(ev) = parse_anchor_instruction(payload, st) {
        return ev;
    }

    // anchor error is program-agnostic
    if let Some(ev) = parse_anchor_error(payload, st) {
        return ev;
    }

    ProgramLog::Unknown(st.push(payload))
}

/// Program-aware parser (slow path).
/// May push args into the string table.
#[inline]
pub fn try_parse_program_log_with_table(
    program: &str,
    payload: &str,
    registry: &Registry,
    st: &mut StringTable,
) -> Option<ProgramLog> {
    if token::STR_ID == program
        && let Some(t) = token::TokenLog::parse(payload)
    {
        return Some(ProgramLog::Token(t));
    }

    if token_2022::STR_ID == program
        && let Some(t) = token_2022::Token2022Log::parse(payload, registry, st)
    {
        return Some(ProgramLog::Token2022(t));
    }

    if associated_token_account::STR_ID == program
        && let Some(t) = associated_token_account::TokenErrorLog::parse(payload)
    {
        return Some(ProgramLog::Ata(t));
    }

    if address_lookup_table::STR_ID == program
        && let Some(x) = address_lookup_table::AddressLookupTableLog::parse(payload, st)
    {
        return Some(ProgramLog::AddressLookupTable(x));
    }

    if loader_v3::STR_ID == program
        && let Some(x) = loader_v3::LoaderV3Log::parse(payload, st)
    {
        return Some(ProgramLog::LoaderV3(x));
    }

    if loader_v4::STR_ID == program
        && let Some(x) = loader_v4::LoaderV4Log::parse(payload, st)
    {
        return Some(ProgramLog::LoaderV4(x));
    }

    if memo::STR_ID == program
        && let Some(x) = memo::MemoLog::parse(payload, st)
    {
        return Some(ProgramLog::Memo(x));
    }

    if record::STR_ID == program
        && let Some(x) = record::RecordLog::parse(payload, st)
    {
        return Some(ProgramLog::Record(x));
    }

    if transfer_hook::STR_ID == program
        && let Some(x) = transfer_hook::TransferHookLog::parse(payload, st)
    {
        return Some(ProgramLog::TransferHook(x));
    }

    if account_compression::STR_ID == program
        && let Some(x) = account_compression::AccountCompressionLog::parse(payload, st)
    {
        return Some(ProgramLog::AccountCompression(x));
    }

    None
}

/// Render a program log payload back to its exact textual form
#[inline]
pub fn render_program_log(log: &ProgramLog, registry: &Registry, st: &StringTable) -> String {
    match log {
        ProgramLog::Token(t) => t.as_str().to_string(),
        ProgramLog::Token2022(t) => t.as_str(st, registry),
        ProgramLog::Ata(t) => t.as_str().to_string(),
        ProgramLog::AddressLookupTable(x) => x.as_str(st),
        ProgramLog::LoaderV3(x) => x.as_str(st),
        ProgramLog::LoaderV4(x) => x.as_str(st),
        ProgramLog::Memo(x) => x.as_str(st),
        ProgramLog::Record(x) => x.as_str(st),
        ProgramLog::TransferHook(x) => x.as_str(st),
        ProgramLog::AccountCompression(x) => x.as_str(st),

        ProgramLog::AnchorInstruction { name } => {
            format!("Instruction: {}", st.resolve(*name))
        }

        ProgramLog::AnchorErrorThrown {
            file,
            line,
            code,
            number,
            msg,
        } => format!(
            "AnchorError thrown in {}:{}. Error Code: {}. Error Number: {}. Error Message: {}.",
            st.resolve(*file),
            line,
            st.resolve(*code),
            number,
            st.resolve(*msg),
        ),

        // ProgramLog::System(s) => s.as_str().to_string(),
        ProgramLog::Unknown(id) => st.resolve(*id).to_string(),
    }
}

/// Parses common Anchor log:
/// `Instruction: <Name>`
#[inline]
fn parse_anchor_instruction(text: &str, st: &mut StringTable) -> Option<ProgramLog> {
    let name = text.strip_prefix("Instruction: ")?;
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    Some(ProgramLog::AnchorInstruction {
        name: st.push(name),
    })
}

/// Parses:
/// `AnchorError thrown in programs/.../buy.rs:63. Error Code: X. Error Number: N. Error Message: Y.`
fn parse_anchor_error(text: &str, st: &mut StringTable) -> Option<ProgramLog> {
    let rest = text.strip_prefix("AnchorError thrown in ")?;
    let (loc, tail1) = rest.split_once(". Error Code: ")?;
    let loc = loc.trim();

    let colon = loc.rfind(':')?;
    let file = loc[..colon].trim();
    let line = loc[colon + 1..].trim().parse::<u32>().ok()?;

    let (code_str, tail2) = tail1.split_once(". Error Number: ")?;
    let code = code_str.trim();

    let (num_str, tail3) = tail2.split_once(". Error Message: ")?;
    let number = num_str.trim().parse::<u32>().ok()?;

    let msg = tail3.strip_suffix('.').unwrap_or(tail3).trim();

    Some(ProgramLog::AnchorErrorThrown {
        file: st.push(file),
        line,
        code: st.push(code),
        number,
        msg: st.push(msg),
    })
}
