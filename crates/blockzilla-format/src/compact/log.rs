use std::str::FromStr;

use data_encoding::BASE64;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use wincode::{SchemaRead, SchemaWrite};

use crate::program_logs::{self, ProgramLog, system_program};
use crate::{KeyIndex, KeyStore};

pub type StrId = u32;
pub type ProgramId = u32;
pub type DataId = u32;

const CB_PK: &str = "ComputeBudget111111111111111111111111111111";

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactLogStream {
    pub events: Vec<LogEvent>,
    pub strings: StringTable,
    pub data: DataTable,
}

#[derive(Debug, Default, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StringTable {
    pub strings: Vec<String>,
}

impl StringTable {
    #[inline]
    pub fn push(&mut self, s: &str) -> StrId {
        let id = self.strings.len() as StrId;
        self.strings.push(s.to_owned());
        id
    }

    #[inline]
    pub fn resolve(&self, id: StrId) -> &str {
        &self.strings[id as usize]
    }
}

#[derive(Debug, Default, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct DataTable {
    pub arrays: Vec<Vec<Vec<u8>>>,
}

impl DataTable {
    #[inline]
    pub fn push(&mut self, data: Vec<Vec<u8>>) -> DataId {
        let id = self.arrays.len() as DataId;
        self.arrays.push(data);
        id
    }

    #[inline]
    pub fn resolve(&self, id: DataId) -> &[Vec<u8>] {
        &self.arrays[id as usize]
    }

    #[inline]
    pub fn render_array(data: &[Vec<u8>]) -> String {
        data.iter()
            .map(|chunk| BASE64.encode(chunk))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LogEvent {
    /// System program structured logs (system_program.rs)
    System(system_program::SystemProgramLog),

    /// Program logs (structured by per-program modules inside program_logs)
    /// `Program log: <msg>`
    ProgramLog(ProgramLog),

    /// `Program log: Error: <msg>`
    ProgramLogError {
        msg: StrId,
    },

    /// `Program <id> log: <msg>`
    ProgramIdLog {
        program: ProgramId,
        log: ProgramLog,
    },

    Invoke {
        program: ProgramId,
        depth: u8,
    },
    Consumed {
        program: ProgramId,
        used: u32,
        limit: u32,
    },
    Success {
        program: ProgramId,
    },

    /// `Program <pk> failed: <reason>`
    Failure {
        program: ProgramId,
        reason: StrId,
    },

    /// `Program <pk> failed: custom program error: 0xNN`
    FailureCustomProgramError {
        program: ProgramId,
        code: u32,
    },

    /// `Program <pk> failed: invalid account data for instruction`
    FailureInvalidAccountData {
        program: ProgramId,
    },

    /// `Program <pk> failed: invalid program argument`
    FailureInvalidProgramArgument {
        program: ProgramId,
    },

    FailedToComplete {
        reason: StrId,
    },

    /// Standalone: `custom program error: 0xNN`
    CustomProgramError {
        code: u32,
    },

    /// `Program return: <pk> <b64>`
    /// We keep the base64 payload decoded into byte arrays.
    Return {
        program: ProgramId,
        data: DataId,
    },

    /// `Program data: <b64>`
    /// We keep the base64 payload decoded into byte arrays.
    Data {
        data: DataId,
    },

    Consumption {
        units: u32,
    },
    CbRequestUnits {
        units: u32,
    },

    ProgramNotDeployed {
        program: Option<ProgramId>,
    },

    /// Runtime says this program is unknown. Keep as string (may not exist in registry).
    UnknownProgram {
        program: StrId,
    },

    /// Runtime says this account is unknown. Keep as string (will often not exist in registry).
    UnknownAccount {
        account: StrId,
    },

    /// Hardcoded runtime verifiers (remove stringly Syscall)
    VerifyEd25519,
    VerifySecp256k1,

    /// Runtime context teardown
    CloseContextState,

    Plain {
        text: StrId,
    },
    Unparsed {
        text: StrId,
    },
}

#[inline]
fn parse_u32_commas(s: &str) -> Option<u32> {
    s.trim().replace(',', "").parse().ok()
}

#[inline]
fn parse_consumed(after_pk: &str) -> Option<(u32, u32)> {
    let rem = after_pk.strip_prefix("consumed ")?;
    let of_pos = rem.find(" of ")?;
    let end_pos = rem.find(" compute units")?;
    Some((
        parse_u32_commas(&rem[..of_pos])?,
        parse_u32_commas(&rem[of_pos + 4..end_pos])?,
    ))
}

#[inline]
pub fn strip_trailing_dot(s: &str) -> &str {
    s.strip_suffix('.').unwrap_or(s).trim()
}

#[inline]
fn parse_custom_program_error_reason(s: &str) -> Option<u32> {
    let hex = s.trim().strip_prefix("custom program error: 0x")?;
    u32::from_str_radix(hex.trim(), 16).ok()
}

#[inline]
fn parse_program_log_error_payload(s: &str) -> Option<&str> {
    // "Error: <msg>"
    let msg = s.trim().strip_prefix("Error: ")?;
    Some(msg.trim())
}

enum FailedReasonClass<'a> {
    Custom(u32),
    InvalidAccountData,
    InvalidProgramArgument,
    Other(&'a str),
}

#[inline]
fn classify_failed_reason(reason: &str) -> FailedReasonClass<'_> {
    let r = reason.trim();

    if let Some(code) = parse_custom_program_error_reason(r) {
        return FailedReasonClass::Custom(code);
    }
    if r == "invalid account data for instruction" {
        return FailedReasonClass::InvalidAccountData;
    }
    if r == "invalid program argument" {
        return FailedReasonClass::InvalidProgramArgument;
    }

    FailedReasonClass::Other(r)
}

#[inline]
fn decode_base64_array(text: &str, dt: &mut DataTable, scratch: &mut Vec<u8>) -> Option<DataId> {
    let mut decoded = Vec::new();
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Some(dt.push(decoded));
    }

    for token in trimmed.split_whitespace() {
        scratch.clear();
        let capacity = BASE64.decode_len(token.len()).ok()?;
        scratch.resize(capacity, 0);
        let used = BASE64.decode_mut(token.as_bytes(), scratch).ok()?;
        scratch.truncate(used);
        decoded.push(scratch.to_vec());
    }

    Some(dt.push(decoded))
}

#[inline]
fn pid_to_pubkey(store: &KeyStore, pid: ProgramId) -> Pubkey {
    assert!(pid != 0, "log.rs: ProgramId=0 is reserved/invalid");
    let bytes = store.get(pid).unwrap_or_else(|| {
        panic!(
            "log.rs: ProgramId out of bounds: pid={} len={}",
            pid,
            store.len()
        )
    });
    Pubkey::new_from_array(*bytes)
}

pub fn parse_logs(lines: &[String], index: &KeyIndex) -> CompactLogStream {
    let mut st = StringTable::default();
    let mut dt = DataTable::default();
    let mut events = Vec::with_capacity(lines.len());
    let mut decode_buf = Vec::new();

    // CB id must exist in registry (else bug)
    let cb_pid = index
        .lookup_str(CB_PK)
        .expect("CB id not found in registry");

    for line in lines {
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }

        // 1) First, let the SystemProgramLog try to parse any "system program-ish" lines.
        if let Some(sys) = system_program::SystemProgramLog::parse(line, index, &mut st) {
            events.push(LogEvent::System(sys));
            continue;
        }

        // standalone: custom program error: 0x....
        if let Some(hex) = line.strip_prefix("custom program error: 0x")
            && let Ok(code) = u32::from_str_radix(hex.trim(), 16)
        {
            events.push(LogEvent::CustomProgramError { code });
            continue;
        }

        // Program failed to complete: ...
        if let Some(msg) = line.strip_prefix("Program failed to complete: ") {
            events.push(LogEvent::FailedToComplete {
                reason: st.push(msg),
            });
            continue;
        }

        // Unknown program <pubkey>
        if let Some(pk_txt) = line.strip_prefix("Unknown program ") {
            let pk_txt = pk_txt.trim();
            if Pubkey::from_str(pk_txt).is_ok() {
                events.push(LogEvent::UnknownProgram {
                    program: st.push(pk_txt),
                });
            } else {
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
            }
            continue;
        }

        // Instruction references an unknown account <pubkey>
        if let Some(pk_txt) = line.strip_prefix("Instruction references an unknown account ") {
            let pk_txt = pk_txt.trim();
            if Pubkey::from_str(pk_txt).is_ok() {
                events.push(LogEvent::UnknownAccount {
                    account: st.push(pk_txt),
                });
            } else {
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
            }
            continue;
        }

        // Hardcoded runtime verifiers
        if line == "VerifyEd25519" {
            events.push(LogEvent::VerifyEd25519);
            continue;
        }
        if line == "VerifySecp256k1" {
            events.push(LogEvent::VerifySecp256k1);
            continue;
        }

        // CloseContextState
        if line == "CloseContextState" {
            events.push(LogEvent::CloseContextState);
            continue;
        }

        // Program log: <msg>
        if let Some(text) = line.strip_prefix("Program log: ") {
            let text = text.trim();

            // If a program logged the runtime custom error string, capture it structurally.
            if let Some(code) = parse_custom_program_error_reason(text) {
                events.push(LogEvent::CustomProgramError { code });
                continue;
            }

            // Program log: Error: <msg>
            if let Some(msg) = parse_program_log_error_payload(text) {
                events.push(LogEvent::ProgramLogError { msg: st.push(msg) });
                continue;
            }

            let log = program_logs::parse_program_log_no_id(text, index, &mut st);
            events.push(LogEvent::ProgramLog(log));
            continue;
        }

        // Program <id> log: <msg>
        if let Some(rest) = line.strip_prefix("Program ")
            && let Some(pos) = rest.find(" log: ")
        {
            let pk_txt = rest[..pos].trim();
            let text = rest[pos + " log: ".len()..].trim();

            let program = index.lookup_str(pk_txt).expect("malformed Program");

            // If a program emitted the runtime custom error string in its own log channel,
            // record it as a program-attributed custom error.
            if let Some(code) = parse_custom_program_error_reason(text) {
                events.push(LogEvent::FailureCustomProgramError { program, code });
                continue;
            }

            // Optional: Program <pk> log: Error: <msg>
            if let Some(msg) = parse_program_log_error_payload(text) {
                events.push(LogEvent::ProgramLogError { msg: st.push(msg) });
                continue;
            }

            let log = program_logs::parse_program_log_for_program(pk_txt, text, index, &mut st);
            events.push(LogEvent::ProgramIdLog { program, log });
            continue;
        }

        // Program ...
        if let Some(rest) = line.strip_prefix("Program ") {
            // Program data: <b64>
            if let Some(b64) = rest.strip_prefix("data: ") {
                if let Some(data) = decode_base64_array(b64, &mut dt, &mut decode_buf) {
                    events.push(LogEvent::Data { data });
                    continue;
                }
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
                continue;
            }

            // Program return: <pk> <b64>
            if let Some(tail) = rest.strip_prefix("return: ") {
                if let Some((pk_txt, b64_txt)) = tail.trim().split_once(' ') {
                    let program = index.lookup_str(pk_txt.trim()).expect("Invaild Pubkey");
                    if let Some(data) = decode_base64_array(b64_txt, &mut dt, &mut decode_buf) {
                        events.push(LogEvent::Return { program, data });
                        continue;
                    }
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                    continue;
                }
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
                continue;
            }

            // Program consumption: N units remaining
            if let Some(rem) = rest.strip_prefix("consumption: ") {
                if let Some(pos) = rem.find(" units remaining")
                    && let Some(units) = parse_u32_commas(&rem[..pos])
                {
                    events.push(LogEvent::Consumption { units });
                    continue;
                }
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
                continue;
            }

            // Program is not deployed
            if rest == "is not deployed" {
                events.push(LogEvent::ProgramNotDeployed { program: None });
                continue;
            }

            // Program <pk> is not deployed
            if let Some(pk_txt) = rest.strip_suffix(" is not deployed") {
                let program = index.lookup_str(pk_txt.trim()).expect("Invaild Pubkey");
                events.push(LogEvent::ProgramNotDeployed {
                    program: Some(program),
                });
                continue;
            }

            // Program <pk> ...
            if let Some(space_pos) = rest.find(' ') {
                let pk_txt = rest[..space_pos].trim();
                let after_pk = rest[space_pos + 1..].trim();

                let program = index.lookup_str(pk_txt).expect("Invaild Pubkey");
                let is_cb = program == cb_pid;

                // invoke [N]
                if let Some(depth_str) = after_pk.strip_prefix("invoke [")
                    && let Some(d) = depth_str.strip_suffix(']')
                    && let Ok(depth_u32) = d.trim().parse::<u32>()
                {
                    let depth = depth_u32.min(255) as u8;
                    events.push(LogEvent::Invoke { program, depth });
                    continue;
                }

                // success
                if after_pk == "success" {
                    events.push(LogEvent::Success { program });
                    continue;
                }

                // failed: <reason>
                if let Some(reason) = after_pk.strip_prefix("failed: ") {
                    match classify_failed_reason(reason) {
                        FailedReasonClass::Custom(code) => {
                            events.push(LogEvent::FailureCustomProgramError { program, code });
                            continue;
                        }
                        FailedReasonClass::InvalidAccountData => {
                            events.push(LogEvent::FailureInvalidAccountData { program });
                            continue;
                        }
                        FailedReasonClass::InvalidProgramArgument => {
                            events.push(LogEvent::FailureInvalidProgramArgument { program });
                            continue;
                        }
                        FailedReasonClass::Other(r) => {
                            events.push(LogEvent::Failure {
                                program,
                                reason: st.push(r),
                            });
                            continue;
                        }
                    }
                }

                // consumed X of Y compute units
                if let Some((used, limit)) = parse_consumed(after_pk) {
                    events.push(LogEvent::Consumed {
                        program,
                        used,
                        limit,
                    });
                    continue;
                }

                // ComputeBudget special: request units
                if is_cb {
                    let norm = after_pk.replace(':', "").to_lowercase();
                    if let Some(tail) = norm.strip_prefix("request units ")
                        && let Some(units) = parse_u32_commas(tail)
                    {
                        events.push(LogEvent::CbRequestUnits { units });
                        continue;
                    }
                }

                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
                continue;
            }
        }

        // Default
        events.push(LogEvent::Plain {
            text: st.push(line),
        });
    }

    CompactLogStream {
        events,
        strings: st,
        data: dt,
    }
}

pub fn render_logs(cls: &CompactLogStream, store: &KeyStore) -> Vec<String> {
    let mut out = Vec::with_capacity(cls.events.len());
    let st = &cls.strings;
    let dt = &cls.data;

    for ev in cls.events.iter() {
        match ev {
            LogEvent::Invoke { program, depth, .. } => out.push(format!(
                "Program {} invoke [{}]",
                pid_to_pubkey(store, *program),
                depth
            )),
            LogEvent::Consumed {
                program,
                used,
                limit,
            } => out.push(format!(
                "Program {} consumed {} of {} compute units",
                pid_to_pubkey(store, *program),
                used,
                limit
            )),
            LogEvent::Success { program } => out.push(format!(
                "Program {} success",
                pid_to_pubkey(store, *program)
            )),

            LogEvent::Failure { program, reason } => out.push(format!(
                "Program {} failed: {}",
                pid_to_pubkey(store, *program),
                st.resolve(*reason)
            )),
            LogEvent::FailureCustomProgramError { program, code } => out.push(format!(
                "Program {} failed: custom program error: 0x{:x}",
                pid_to_pubkey(store, *program),
                code
            )),
            LogEvent::FailureInvalidAccountData { program } => out.push(format!(
                "Program {} failed: invalid account data for instruction",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::FailureInvalidProgramArgument { program } => out.push(format!(
                "Program {} failed: invalid program argument",
                pid_to_pubkey(store, *program)
            )),

            LogEvent::FailedToComplete { reason } => out.push(format!(
                "Program failed to complete: {}",
                st.resolve(*reason)
            )),

            LogEvent::System(sys) => out.push(sys.render(st, store)),

            LogEvent::ProgramLog(log) => {
                let payload = program_logs::render_program_log(log, store, st);
                out.push(format!("Program log: {}", payload));
            }
            LogEvent::ProgramLogError { msg } => {
                out.push(format!("Program log: Error: {}", st.resolve(*msg)));
            }
            LogEvent::ProgramIdLog { program, log } => {
                let payload = program_logs::render_program_log(log, store, st);
                out.push(format!(
                    "Program {} log: {}",
                    pid_to_pubkey(store, *program),
                    payload
                ));
            }

            LogEvent::CustomProgramError { code } => {
                out.push(format!("custom program error: 0x{:x}", code))
            }

            LogEvent::Return { program, data } => out.push(format!(
                "Program return: {} {}",
                pid_to_pubkey(store, *program),
                DataTable::render_array(dt.resolve(*data)),
            )),

            LogEvent::Data { data } => out.push(format!(
                "Program data: {}",
                DataTable::render_array(dt.resolve(*data))
            )),

            LogEvent::Consumption { units } => {
                out.push(format!("Program consumption: {} units remaining", units))
            }
            LogEvent::CbRequestUnits { units } => {
                out.push(format!("Program {} request units {}", CB_PK, units))
            }
            LogEvent::ProgramNotDeployed { program } => {
                if let Some(pid) = program {
                    out.push(format!(
                        "Program {} is not deployed",
                        pid_to_pubkey(store, *pid)
                    ));
                } else {
                    out.push("Program is not deployed".to_string());
                }
            }

            LogEvent::UnknownProgram { program } => {
                out.push(format!("Unknown program {}", st.resolve(*program)))
            }
            LogEvent::UnknownAccount { account } => out.push(format!(
                "Instruction references an unknown account {}",
                st.resolve(*account)
            )),

            LogEvent::VerifyEd25519 => out.push("VerifyEd25519".to_string()),
            LogEvent::VerifySecp256k1 => out.push("VerifySecp256k1".to_string()),

            LogEvent::CloseContextState => out.push("CloseContextState".to_string()),

            LogEvent::Plain { text } | LogEvent::Unparsed { text } => {
                out.push(st.resolve(*text).to_string())
            }
        }
    }

    out
}
