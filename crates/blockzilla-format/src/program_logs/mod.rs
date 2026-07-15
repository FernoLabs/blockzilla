use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{KeyStore, PubkeyCompactor, StrId, StringTable};

pub const KNOWN_PROGRAM_LOGS_ENABLED: bool = cfg!(feature = "known-program-logs");

pub mod account_compression;
pub mod address_lookup_table;
pub mod associated_token_account;
#[cfg(feature = "known-program-logs")]
pub mod known_programs;
pub mod loader_v3;
pub mod loader_v4;
pub mod memo;
pub mod record;
pub mod stake;
pub mod system_program;
pub mod token;
pub mod token_2022;
pub mod transfer_hook;
pub mod zk_elgamal_proof;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ProgramLog {
    Empty,
    Token(token::TokenLog),
    Token2022(token_2022::Token2022Log),
    Ata(associated_token_account::TokenLog),
    AddressLookupTable(address_lookup_table::AddressLookupTableLog),
    LoaderV3(loader_v3::LoaderV3Log),
    LoaderV4(loader_v4::LoaderV4Log),
    Memo(memo::MemoLog),
    Record(record::RecordLog),
    TransferHook(transfer_hook::TransferHookLog),
    AccountCompression(account_compression::AccountCompressionLog),
    Stake(stake::StakeProgramLog),
    ZkElgamalProof(zk_elgamal_proof::ZkElgamalProofLog),
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
    #[cfg(feature = "known-program-logs")]
    Known(known_programs::KnownProgramLog),
}

#[inline]
pub fn parse_program_log_no_id<C: PubkeyCompactor>(
    payload: &str,
    index: &C,
    st: &mut StringTable,
) -> ProgramLog {
    if payload.is_empty() {
        return ProgramLog::Empty;
    }

    // Fast path: zero-alloc parsers
    if let Some(t) = token::TokenLog::parse(payload) {
        return ProgramLog::Token(t);
    }
    if let Some(t) = associated_token_account::TokenLog::parse(payload) {
        return ProgramLog::Ata(t);
    }
    if let Some(ev) = parse_anchor_instruction(payload, st) {
        return ev;
    }

    // Slow path: parsers using StringTable
    if let Some(t) = token_2022::Token2022Log::parse(payload, index, st) {
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
pub fn parse_program_log_for_program<C: PubkeyCompactor>(
    program: &str,
    payload: &str,
    index: &C,
    st: &mut StringTable,
) -> ProgramLog {
    if payload.is_empty() {
        return ProgramLog::Empty;
    }

    if let Some(log) = try_parse_program_log_with_table(program, payload, index, st) {
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

/// Returns true when this payload already has a compact `ProgramLog` representation.
///
/// This registry-free check is used by audit tooling to avoid dumping payloads that the binary
/// encoder already handles. Logs whose parser needs a registry lookup are reported as unknown here
/// unless another registry-free parser recognizes the same payload.
#[inline]
pub fn program_log_has_known_binary_form(program: Option<&str>, payload: &str) -> bool {
    if payload.is_empty() {
        return true;
    }

    if let Some(program) = program {
        if try_parse_registry_free_for_program(program, payload) {
            return true;
        }
        #[cfg(feature = "known-program-logs")]
        if known_programs::has_known_binary_form(program, payload) {
            return true;
        }
    } else if try_parse_registry_free_no_id(payload) {
        return true;
    }

    is_anchor_instruction(payload) || is_anchor_error(payload)
}

#[inline]
pub fn official_program_log_has_known_binary_form(program: &str, payload: &str) -> bool {
    try_parse_registry_free_for_program(program, payload)
}

#[inline]
fn try_parse_registry_free_no_id(payload: &str) -> bool {
    if token::TokenLog::parse(payload).is_some()
        || associated_token_account::TokenLog::parse(payload).is_some()
        || token_2022::Token2022Log::parse_without_registry(payload).is_some()
    {
        return true;
    }

    let mut st = StringTable::default();
    address_lookup_table::AddressLookupTableLog::parse(payload, &mut st).is_some()
        || loader_v3::LoaderV3Log::parse(payload, &mut st).is_some()
        || loader_v4::LoaderV4Log::parse(payload, &mut st).is_some()
        || memo::MemoLog::parse(payload, &mut st).is_some()
        || record::RecordLog::parse(payload, &mut st).is_some()
        || transfer_hook::TransferHookLog::parse(payload, &mut st).is_some()
        || account_compression::AccountCompressionLog::parse(payload, &mut st).is_some()
}

#[inline]
fn try_parse_registry_free_for_program(program: &str, payload: &str) -> bool {
    if program == token::STR_ID {
        return token::TokenLog::parse(payload).is_some();
    }
    if program == associated_token_account::STR_ID {
        return associated_token_account::TokenLog::parse_for_program(payload).is_some();
    }
    if program == token_2022::STR_ID {
        return token_2022::Token2022Log::parse_without_registry(payload).is_some();
    }

    let mut st = StringTable::default();
    if loader_v3::is_bpf_loader_id(program) {
        return loader_v3::LoaderV3Log::parse(payload, &mut st).is_some();
    }

    match program {
        address_lookup_table::STR_ID => {
            address_lookup_table::AddressLookupTableLog::parse(payload, &mut st).is_some()
        }
        loader_v4::STR_ID => loader_v4::LoaderV4Log::parse(payload, &mut st).is_some(),
        memo::STR_ID => memo::MemoLog::parse(payload, &mut st).is_some(),
        record::STR_ID => record::RecordLog::parse(payload, &mut st).is_some(),
        stake::STR_ID => stake::StakeProgramLog::parse(payload, &mut st).is_some(),
        transfer_hook::STR_ID => transfer_hook::TransferHookLog::parse(payload, &mut st).is_some(),
        account_compression::STR_ID => {
            account_compression::AccountCompressionLog::parse(payload, &mut st).is_some()
        }
        zk_elgamal_proof::STR_ID => {
            zk_elgamal_proof::ZkElgamalProofLog::parse(payload, &mut st).is_some()
        }
        _ => false,
    }
}

#[inline]
fn is_anchor_instruction(payload: &str) -> bool {
    payload
        .strip_prefix("Instruction: ")
        .is_some_and(|name| !name.trim().is_empty())
}

#[inline]
fn is_anchor_error(payload: &str) -> bool {
    let mut st = StringTable::default();
    parse_anchor_error(payload, &mut st).is_some()
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
pub fn try_parse_program_log_with_table<C: PubkeyCompactor>(
    program: &str,
    payload: &str,
    index: &C,
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
        token_2022::Token2022Log::parse(payload, index, st).map(ProgramLog::Token2022)
    );

    try_parse!(
        program,
        associated_token_account::STR_ID,
        associated_token_account::TokenLog::parse_for_program(payload).map(ProgramLog::Ata)
    );

    try_parse!(
        program,
        address_lookup_table::STR_ID,
        address_lookup_table::AddressLookupTableLog::parse(payload, st)
            .map(ProgramLog::AddressLookupTable)
    );

    if loader_v3::is_bpf_loader_id(program)
        && let Some(log) = loader_v3::LoaderV3Log::parse(payload, st)
    {
        return Some(ProgramLog::LoaderV3(log));
    }

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
        stake::STR_ID,
        stake::StakeProgramLog::parse(payload, st).map(ProgramLog::Stake)
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

    try_parse!(
        program,
        zk_elgamal_proof::STR_ID,
        zk_elgamal_proof::ZkElgamalProofLog::parse(payload, st).map(ProgramLog::ZkElgamalProof)
    );

    #[cfg(feature = "known-program-logs")]
    if let Some(log) = known_programs::parse_for_program(program, payload) {
        return Some(ProgramLog::Known(log));
    }

    None
}

#[inline]
pub fn render_program_log(log: &ProgramLog, store: &KeyStore, st: &StringTable) -> String {
    match log {
        ProgramLog::Empty => String::new(),
        ProgramLog::Token(t) => t.as_str().to_string(),
        ProgramLog::Token2022(t) => t.as_str(st, store),
        ProgramLog::Ata(t) => t.as_str().to_string(),
        ProgramLog::AddressLookupTable(x) => x.as_str(st),
        ProgramLog::LoaderV3(x) => x.as_str(st),
        ProgramLog::LoaderV4(x) => x.as_str(st),
        ProgramLog::Memo(x) => x.as_str(st),
        ProgramLog::Record(x) => x.as_str(st),
        ProgramLog::TransferHook(x) => x.as_str(st),
        ProgramLog::AccountCompression(x) => x.as_str(st),
        ProgramLog::Stake(x) => x.as_str(st),
        ProgramLog::ZkElgamalProof(x) => x.as_str(st),
        #[cfg(feature = "known-program-logs")]
        ProgramLog::Known(x) => known_programs::render(x),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_program_payload_filter_excludes_structured_payloads() {
        assert!(program_log_has_known_binary_form(None, ""));
        assert!(program_log_has_known_binary_form(
            None,
            "Instruction: InitializeAccount3"
        ));
        assert!(program_log_has_known_binary_form(
            None,
            "Instruction: TransferChecked"
        ));
        assert!(program_log_has_known_binary_form(
            None,
            "Instruction: CloseAccount"
        ));
        assert!(program_log_has_known_binary_form(
            None,
            "Instruction: GetAccountDataSize"
        ));
        assert!(program_log_has_known_binary_form(
            None,
            "Initialize the associated token account"
        ));
        assert!(program_log_has_known_binary_form(None, "Instruction: Buy"));
        assert!(!program_log_has_known_binary_form(None, "Create"));
    }

    #[test]
    fn known_program_payload_filter_uses_program_id_when_present() {
        assert!(program_log_has_known_binary_form(
            Some(token::STR_ID),
            "Instruction: TransferChecked"
        ));
        assert!(!program_log_has_known_binary_form(
            Some(token::STR_ID),
            "Create"
        ));
        assert!(program_log_has_known_binary_form(
            Some(associated_token_account::STR_ID),
            "Create"
        ));
        assert!(program_log_has_known_binary_form(
            Some(associated_token_account::STR_ID),
            "CreateIdempotent"
        ));
        assert!(program_log_has_known_binary_form(
            Some(token_2022::STR_ID),
            "Instruction: Reallocate"
        ));
        assert!(program_log_has_known_binary_form(
            Some(stake::STR_ID),
            "Instruction: DelegateStake"
        ));
        assert!(program_log_has_known_binary_form(
            Some(loader_v3::V1_STR_ID),
            "Deprecated loader is no longer supported"
        ));
        assert!(program_log_has_known_binary_form(
            Some(loader_v3::V2_STR_ID),
            "BPF loader management instructions are no longer supported"
        ));
        assert!(program_log_has_known_binary_form(
            Some(loader_v4::STR_ID),
            "Program is finalized"
        ));
        assert!(program_log_has_known_binary_form(
            Some(zk_elgamal_proof::STR_ID),
            "VerifyBatchedRangeProofU64"
        ));
    }

    #[cfg(feature = "known-program-logs")]
    #[test]
    fn known_program_payload_filter_handles_raydium_ray_log() {
        let payload = known_programs::raydium_amm::RaydiumAmmLog::SwapBaseIn(
            known_programs::raydium_amm::SwapBaseInLog {
                amount_in: 1,
                minimum_out: 2,
                direction: 3,
                user_source: 4,
                pool_coin: 5,
                pool_pc: 6,
                out_amount: 7,
            },
        )
        .as_str();

        assert!(program_log_has_known_binary_form(
            Some(known_programs::raydium_amm::STR_ID),
            &payload
        ));
        assert!(!official_program_log_has_known_binary_form(
            known_programs::raydium_amm::STR_ID,
            &payload
        ));
    }
}
