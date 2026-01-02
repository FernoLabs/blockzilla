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
    Token(token::TokenLog),
    Token2022(token_2022::Token2022Log),
    Ata(associated_token_account::TokenErrorLog),
    AddressLookupTable(address_lookup_table::AddressLookupTableLog),
    LoaderV3(loader_v3::LoaderV3Log),
    LoaderV4(loader_v4::LoaderV4Log),
    Memo(memo::MemoLog),
    Record(record::RecordLog),
    TransferHook(transfer_hook::TransferHookLog),
    AccountCompression(account_compression::AccountCompressionLog),
    AnchorInstruction {
        name: StrId,
    },
    AnchorErrorOccurred {
        code: StrId,
        number: u32,
        msg: StrId,
    },
    AnchorErrorThrown {
        file: StrId,
        line: u32,
        code: StrId,
        number: u32,
        msg: StrId,
    },
    Unknown(StrId),
}

#[inline]
pub fn parse_program_log_no_id(
    payload: &str,
    registry: &Registry,
    st: &mut StringTable,
) -> ProgramLog {
    // Fast path: zero-alloc parsers
    if let Some(t) = token::TokenLog::parse(payload) {
        return ProgramLog::Token(t);
    }
    if let Some(t) = associated_token_account::TokenErrorLog::parse(payload) {
        return ProgramLog::Ata(t);
    }
    if let Some(ev) = parse_anchor_instruction(payload, st) {
        return ev;
    }

    // Slow path: parsers using StringTable
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
    if let Some(ev) = parse_anchor_error(payload, st) {
        return ev;
    }

    ProgramLog::Unknown(st.push(payload))
}

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
    if let Some(ev) = parse_anchor_instruction(payload, st) {
        return ev;
    }
    if let Some(ev) = parse_anchor_error(payload, st) {
        return ev;
    }
    ProgramLog::Unknown(st.push(payload))
}

macro_rules! try_parse {
    ($program:expr, $id:expr, $parser:expr) => {
        if $program == $id {
            if let Some(log) = $parser {
                return Some(log);
            }
        }
    };
}

#[inline]
pub fn try_parse_program_log_with_table(
    program: &str,
    payload: &str,
    registry: &Registry,
    st: &mut StringTable,
) -> Option<ProgramLog> {
    try_parse!(
        program,
        token::STR_ID,
        token::TokenLog::parse(payload).map(ProgramLog::Token)
    );

    try_parse!(
        program,
        token_2022::STR_ID,
        token_2022::Token2022Log::parse(payload, registry, st).map(ProgramLog::Token2022)
    );

    try_parse!(
        program,
        associated_token_account::STR_ID,
        associated_token_account::TokenErrorLog::parse(payload).map(ProgramLog::Ata)
    );

    try_parse!(
        program,
        address_lookup_table::STR_ID,
        address_lookup_table::AddressLookupTableLog::parse(payload, st)
            .map(ProgramLog::AddressLookupTable)
    );

    try_parse!(
        program,
        loader_v3::STR_ID,
        loader_v3::LoaderV3Log::parse(payload, st).map(ProgramLog::LoaderV3)
    );

    try_parse!(
        program,
        loader_v4::STR_ID,
        loader_v4::LoaderV4Log::parse(payload, st).map(ProgramLog::LoaderV4)
    );

    try_parse!(
        program,
        memo::STR_ID,
        memo::MemoLog::parse(payload, st).map(ProgramLog::Memo)
    );

    try_parse!(
        program,
        record::STR_ID,
        record::RecordLog::parse(payload, st).map(ProgramLog::Record)
    );

    try_parse!(
        program,
        transfer_hook::STR_ID,
        transfer_hook::TransferHookLog::parse(payload, st).map(ProgramLog::TransferHook)
    );

    try_parse!(
        program,
        account_compression::STR_ID,
        account_compression::AccountCompressionLog::parse(payload, st)
            .map(ProgramLog::AccountCompression)
    );

    None
}

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
        ProgramLog::AnchorErrorOccurred { code, number, msg } => format!(
            "AnchorError occurred. Error Code: {}. Error Number: {}. Error Message: {}.",
            st.resolve(*code),
            number,
            st.resolve(*msg)
        ),
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
            st.resolve(*msg)
        ),
        ProgramLog::Unknown(id) => st.resolve(*id).to_string(),
    }
}

#[inline]
fn parse_anchor_instruction(text: &str, st: &mut StringTable) -> Option<ProgramLog> {
    let name = text.strip_prefix("Instruction: ")?.trim();
    if name.is_empty() {
        return None;
    }
    Some(ProgramLog::AnchorInstruction {
        name: st.push(name),
    })
}

fn parse_anchor_error(text: &str, st: &mut StringTable) -> Option<ProgramLog> {
    // Try "thrown" variant with file location
    if let Some(rest) = text.strip_prefix("AnchorError thrown in ") {
        let (loc, tail) = rest.split_once(". Error Code: ")?;
        let colon = loc.rfind(':')?;
        let file = st.push(loc[..colon].trim());
        let line = loc[colon + 1..].trim().parse().ok()?;

        return parse_error_fields(tail, st).map(|(code, number, msg)| {
            ProgramLog::AnchorErrorThrown {
                file,
                line,
                code,
                number,
                msg,
            }
        });
    }

    // Try "occurred" variant without file location
    if let Some(rest) = text.strip_prefix("AnchorError occurred. Error Code: ") {
        return parse_error_fields(rest, st)
            .map(|(code, number, msg)| ProgramLog::AnchorErrorOccurred { code, number, msg });
    }

    None
}

fn parse_error_fields(text: &str, st: &mut StringTable) -> Option<(StrId, u32, StrId)> {
    let (code_str, tail) = text.split_once(". Error Number: ")?;
    let (num_str, msg_str) = tail.split_once(". Error Message: ")?;

    let code = st.push(code_str.trim());
    let number = num_str.trim().parse().ok()?;
    let msg = st.push(msg_str.strip_suffix('.').unwrap_or(msg_str).trim());

    Some((code, number, msg))
}
