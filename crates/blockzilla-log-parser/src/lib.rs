//! Borrowed parser for Solana runtime log lines.
//!
//! This crate only classifies the top-level log shape. It does not validate
//! pubkeys, decode base64 payloads, or interpret program-specific payload text.
//!
//! ```
//! use blockzilla_log_parser::{ParsedLogLine, parse_line};
//!
//! let line = "Program ComputeBudget111111111111111111111111111111 consumed 1,234 of 2,000 compute units";
//! assert_eq!(
//!     parse_line(line),
//!     ParsedLogLine::Consumed {
//!         program: "ComputeBudget111111111111111111111111111111",
//!         used: 1_234,
//!         limit: 2_000,
//!     }
//! );
//! ```

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedReason<'a> {
    Custom(u32),
    InvalidAccountData,
    InvalidProgramArgument,
    Other(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemAddress<'a> {
    pub address: &'a str,
    pub base: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedLogLine<'a> {
    CustomProgramError {
        code: u32,
    },
    FailedToComplete {
        reason: &'a str,
    },
    UnknownProgram {
        program: &'a str,
    },
    UnknownAccount {
        account: &'a str,
    },
    LogTruncated,
    VerifyEd25519,
    VerifySecp256k1,
    CloseContextState,
    ProgramAccountNotWritable,
    ProgramIdMismatch,
    ProgramNotUpgradeable,
    ProgramAndProgramDataAccountMismatch,
    ProgramWasExtendedInThisBlockAlready,
    StakeMergingAccounts,
    LoaderUpgradedProgram {
        program: &'a str,
    },
    LoaderFinalizedAccount {
        account: &'a str,
    },
    BpfInvoke {
        program: &'a str,
    },
    BpfSuccess {
        program: &'a str,
    },
    BpfFailure {
        program: &'a str,
        reason: &'a str,
    },
    BpfConsumed {
        used: u32,
        limit: u32,
    },
    RuntimeWritablePrivilegeEscalated {
        account: &'a str,
    },
    RuntimeSignerPrivilegeEscalated {
        account: &'a str,
    },
    RuntimeAccountOwnerBalanceVerificationFailed {
        account: &'a str,
    },
    SystemTransferInsufficient {
        have: u64,
        need: u64,
    },
    SystemTransferFromMustNotCarryData,
    SystemAllocateAccountAlreadyInUse {
        account: SystemAddress<'a>,
    },
    SystemCreateAccountAlreadyInUse {
        account: SystemAddress<'a>,
    },
    SystemCreateAccountDataSizeLimited {
        limit: u64,
    },
    ProgramLog {
        text: &'a str,
    },
    ProgramIdLog {
        program: &'a str,
        text: &'a str,
    },
    ProgramData {
        data: &'a str,
    },
    ProgramReturn {
        program: &'a str,
        data: &'a str,
    },
    ProgramConsumption {
        units: u32,
    },
    ProgramNotCached {
        program: Option<&'a str>,
    },
    ProgramNotDeployed {
        program: Option<&'a str>,
    },
    Invoke {
        program: &'a str,
        depth: u8,
    },
    Success {
        program: &'a str,
    },
    Failure {
        program: &'a str,
        reason: &'a str,
    },
    Consumed {
        program: &'a str,
        used: u32,
        limit: u32,
    },
    CbRequestUnits {
        program: &'a str,
        units: u32,
    },
    UnparsedProgram,
    Plain {
        text: &'a str,
    },
}

#[inline]
pub fn parse_u32_commas(s: &str) -> Option<u32> {
    let mut out = 0u32;
    let mut saw_digit = false;

    for b in s.trim().bytes() {
        match b {
            b'0'..=b'9' => {
                saw_digit = true;
                out = out.checked_mul(10)?.checked_add(u32::from(b - b'0'))?;
            }
            b',' => {}
            _ => return None,
        }
    }

    saw_digit.then_some(out)
}

#[inline]
pub fn parse_u64_commas(s: &str) -> Option<u64> {
    let mut out = 0u64;
    let mut saw_digit = false;

    for b in s.trim().bytes() {
        match b {
            b'0'..=b'9' => {
                saw_digit = true;
                out = out.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
            }
            b',' => {}
            _ => return None,
        }
    }

    saw_digit.then_some(out)
}

#[inline]
pub fn parse_custom_program_error_reason(s: &str) -> Option<u32> {
    let hex = s.trim().strip_prefix("custom program error: 0x")?;
    if hex.is_empty() {
        return None;
    }
    u32::from_str_radix(hex, 16).ok()
}

#[inline]
pub fn parse_program_log_error_payload(s: &str) -> Option<&str> {
    let msg = s.trim().strip_prefix("Error: ")?;
    Some(msg.trim())
}

#[inline]
pub fn classify_failed_reason(reason: &str) -> FailedReason<'_> {
    let r = reason.trim();

    if let Some(code) = parse_custom_program_error_reason(r) {
        return FailedReason::Custom(code);
    }
    if r == "invalid account data for instruction" {
        return FailedReason::InvalidAccountData;
    }
    if r == "invalid program argument" {
        return FailedReason::InvalidProgramArgument;
    }

    FailedReason::Other(r)
}

#[inline]
pub fn parse_line(line: &str) -> ParsedLogLine<'_> {
    match line {
        "VerifyEd25519" => return ParsedLogLine::VerifyEd25519,
        "VerifySecp256k1" => return ParsedLogLine::VerifySecp256k1,
        "CloseContextState" => return ParsedLogLine::CloseContextState,
        "Program account not writeable" => return ParsedLogLine::ProgramAccountNotWritable,
        "Program not upgradeable" => return ParsedLogLine::ProgramNotUpgradeable,
        "Program id mismatch" => return ParsedLogLine::ProgramIdMismatch,
        "Program and ProgramData account mismatch" => {
            return ParsedLogLine::ProgramAndProgramDataAccountMismatch;
        }
        "Program was extended in this block already" => {
            return ParsedLogLine::ProgramWasExtendedInThisBlockAlready;
        }
        "Merging stake accounts" => return ParsedLogLine::StakeMergingAccounts,
        "Log truncated" => return ParsedLogLine::LogTruncated,
        _ => {}
    }

    if let Some(program_rest) = line.strip_prefix("Program ") {
        return parse_program_line(line, program_rest);
    }

    if let Some(program) = parse_prefixed_rest(line, "Upgraded program ") {
        return ParsedLogLine::LoaderUpgradedProgram { program };
    }

    if let Some(account) = parse_prefixed_rest(line, "Finalized account ") {
        return ParsedLogLine::LoaderFinalizedAccount { account };
    }

    if let Some(program) = parse_prefixed_rest(line, "Call BPF program ") {
        return ParsedLogLine::BpfInvoke { program };
    }

    if let Some(rest) = line.strip_prefix("BPF program ") {
        if let Some((used, limit)) = parse_old_bpf_consumed(rest) {
            return ParsedLogLine::BpfConsumed { used, limit };
        }

        if let Some(program) = rest.strip_suffix(" success") {
            return ParsedLogLine::BpfSuccess {
                program: program.trim(),
            };
        }
        if let Some((program, reason)) = rest.split_once(" failed ") {
            return ParsedLogLine::BpfFailure {
                program: program.trim(),
                reason: reason.trim(),
            };
        }
    }

    if let Some(account) =
        parse_runtime_privilege_escalated(line, "'s writable privilege escalated")
    {
        return ParsedLogLine::RuntimeWritablePrivilegeEscalated { account };
    }

    if let Some(account) = parse_runtime_privilege_escalated(line, "'s signer privilege escalated")
    {
        return ParsedLogLine::RuntimeSignerPrivilegeEscalated { account };
    }

    if let Some(account) = parse_between(
        line,
        "failed to verify account ",
        " instruction spent from the balance of an account it does not own",
    ) {
        return ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed {
            account: account.trim(),
        };
    }

    if line == "Transfer: `from` must not carry data" {
        return ParsedLogLine::SystemTransferFromMustNotCarryData;
    }

    if let Some(limit) = parse_between(
        line,
        "SystemProgram::CreateAccount data size limited to ",
        " in inner instructions",
    )
    .and_then(parse_u64_commas)
    {
        return ParsedLogLine::SystemCreateAccountDataSizeLimited { limit };
    }

    if let Some((have, need)) = parse_system_transfer_insufficient(line) {
        return ParsedLogLine::SystemTransferInsufficient { have, need };
    }

    if let Some(account) =
        parse_system_account_already_in_use(line, "Allocate: account ", " already in use")
    {
        return ParsedLogLine::SystemAllocateAccountAlreadyInUse { account };
    }

    if let Some(account) =
        parse_system_account_already_in_use(line, "Create Account: account ", " already in use")
    {
        return ParsedLogLine::SystemCreateAccountAlreadyInUse { account };
    }

    if let Some(code) = parse_custom_program_error_reason(line) {
        return ParsedLogLine::CustomProgramError { code };
    }

    if let Some(program) = parse_prefixed_rest(line, "Unknown program ") {
        return ParsedLogLine::UnknownProgram { program };
    }

    if let Some(account) = parse_prefixed_rest(line, "Instruction references an unknown account ") {
        return ParsedLogLine::UnknownAccount { account };
    }

    ParsedLogLine::Plain { text: line }
}

#[inline]
fn parse_program_line<'a>(line: &'a str, program_rest: &'a str) -> ParsedLogLine<'a> {
    if let Some(text) = program_rest.strip_prefix("log:") {
        return ParsedLogLine::ProgramLog { text: text.trim() };
    }

    if let Some(reason) = parse_prefixed_rest(program_rest, "failed to complete: ") {
        return ParsedLogLine::FailedToComplete { reason };
    }

    if let Some(data) = parse_program_data(program_rest) {
        return ParsedLogLine::ProgramData { data: data.trim() };
    }

    if let Some((program, data)) = parse_program_return(program_rest) {
        return ParsedLogLine::ProgramReturn { program, data };
    }

    if let Some(units) = parse_program_consumption(program_rest) {
        return ParsedLogLine::ProgramConsumption { units };
    }

    if program_rest == "is not cached" {
        return ParsedLogLine::ProgramNotCached { program: None };
    }

    if let Some(program) = program_rest.strip_suffix(" is not cached") {
        return ParsedLogLine::ProgramNotCached {
            program: Some(program.trim()),
        };
    }

    if program_rest == "is not deployed" {
        return ParsedLogLine::ProgramNotDeployed { program: None };
    }

    if let Some(program) = program_rest.strip_suffix(" is not deployed") {
        return ParsedLogLine::ProgramNotDeployed {
            program: Some(program.trim()),
        };
    }

    if is_raw_program_space_log(program_rest) {
        return ParsedLogLine::Plain { text: line };
    }

    if let Some((program, tail)) = parse_program_keyed(program_rest) {
        let parsed = parse_program_tail(program, tail);
        if !matches!(parsed, ParsedLogLine::UnparsedProgram) {
            return parsed;
        }
        if let Some((program, text)) = parse_program_id_log(program_rest) {
            return ParsedLogLine::ProgramIdLog { program, text };
        }
        return ParsedLogLine::UnparsedProgram;
    }

    ParsedLogLine::Plain { text: line }
}

#[inline]
fn parse_consumed(input: &str) -> Option<(u32, u32)> {
    let rest = input.strip_prefix("consumed ")?;
    let rest = rest.strip_suffix(" compute units")?;
    let (used, limit) = rest.split_once(" of ")?;
    Some((parse_u32_commas(used)?, parse_u32_commas(limit)?))
}

#[inline]
fn parse_old_bpf_consumed(input: &str) -> Option<(u32, u32)> {
    let rest = input.strip_prefix("consumed ")?;
    let rest = rest.strip_suffix(" units")?;
    let (used, limit) = rest.split_once(" of ")?;
    Some((parse_u32_commas(used)?, parse_u32_commas(limit)?))
}

#[inline]
fn parse_prefixed_rest<'a>(line: &'a str, prefix: &'static str) -> Option<&'a str> {
    line.strip_prefix(prefix).map(str::trim)
}

#[inline]
fn parse_between<'a>(line: &'a str, prefix: &'static str, suffix: &'static str) -> Option<&'a str> {
    let rest = line.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(suffix)?;
    Some(inner.trim())
}

#[inline]
fn parse_runtime_privilege_escalated<'a>(line: &'a str, suffix: &'static str) -> Option<&'a str> {
    let account = line.strip_suffix(suffix)?.trim();
    (!account.is_empty()).then_some(account)
}

#[inline]
fn parse_system_transfer_insufficient(line: &str) -> Option<(u64, u64)> {
    let rest = line.strip_prefix("Transfer: insufficient lamports ")?;
    let (have, need) = rest.split_once(", need ")?;
    Some((parse_u64_commas(have)?, parse_u64_commas(need)?))
}

#[inline]
fn is_raw_program_space_log(program_rest: &str) -> bool {
    matches!(
        program_rest.split_whitespace().next(),
        Some("account" | "is" | "must" | "not" | "was")
    )
}

#[inline]
fn parse_system_account_already_in_use<'a>(
    line: &'a str,
    prefix: &'static str,
    suffix: &'static str,
) -> Option<SystemAddress<'a>> {
    let rest = line.strip_prefix(prefix)?;
    let account = rest.strip_suffix(suffix)?;
    parse_system_address(account)
}

#[inline]
fn parse_system_address(s: &str) -> Option<SystemAddress<'_>> {
    let s = s.trim();
    let Some(inner) = s.strip_prefix("Address {") else {
        return Some(SystemAddress {
            address: s,
            base: None,
        });
    };

    let inner = inner.trim().strip_suffix('}')?.trim();
    let rest = inner.strip_prefix("address:")?.trim();
    let (address, base) = if let Some((address, base)) = rest.split_once(", base:") {
        (address, parse_system_address_base(base.trim())?)
    } else {
        (rest, None)
    };
    Some(SystemAddress {
        address: address.trim(),
        base,
    })
}

#[inline]
fn parse_system_address_base(base: &str) -> Option<Option<&str>> {
    if base == "None" {
        return Some(None);
    }

    let base = base.strip_prefix("Some(")?.strip_suffix(')')?.trim();
    Some(Some(base))
}

fn parse_program_data(input: &str) -> Option<&str> {
    input.strip_prefix("data: ")
}

#[inline]
fn parse_program_return(input: &str) -> Option<(&str, &str)> {
    let rest = input.strip_prefix("return: ")?;
    let (program, data) = rest.split_once(' ')?;
    if program.is_empty() {
        return None;
    }
    Some((program.trim(), data.trim()))
}

#[inline]
fn parse_program_consumption(input: &str) -> Option<u32> {
    let rest = input.strip_prefix("consumption: ")?;
    let units = rest.strip_suffix(" units remaining")?;
    parse_u32_commas(units)
}

#[inline]
fn parse_program_id_log(input: &str) -> Option<(&str, &str)> {
    let (program, text) = input.split_once(" log:")?;
    if program.trim().is_empty() {
        return None;
    }
    Some((program.trim(), text.trim()))
}

#[inline]
fn parse_program_keyed(input: &str) -> Option<(&str, &str)> {
    let (program, tail) = input.split_once(' ')?;
    if program.is_empty() {
        return None;
    }
    Some((program.trim(), tail.trim()))
}

#[inline]
fn parse_invoke(input: &str) -> Option<u8> {
    let depth = input.strip_prefix("invoke [")?.strip_suffix(']')?;
    let depth = parse_u32_decimal(depth)?;
    Some(depth.min(u32::from(u8::MAX)) as u8)
}

#[inline]
fn parse_u32_decimal(input: &str) -> Option<u32> {
    let mut out = 0u32;
    let mut saw_digit = false;

    for b in input.bytes() {
        let digit = b.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        saw_digit = true;
        out = out.checked_mul(10)?.checked_add(u32::from(digit))?;
    }

    saw_digit.then_some(out)
}

#[inline]
fn parse_request_units(input: &str) -> Option<u32> {
    let rest = strip_ascii_case_prefix(input, "request units")?;
    let trimmed = rest.trim_start();
    let rest = if let Some(rest) = trimmed.strip_prefix(':') {
        rest
    } else if rest.len() != trimmed.len() {
        trimmed
    } else {
        return None;
    };
    parse_u32_commas(rest)
}

#[inline]
fn strip_ascii_case_prefix<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    if input.len() < prefix.len() {
        return None;
    }
    let (head, rest) = input.split_at(prefix.len());
    head.eq_ignore_ascii_case(prefix).then_some(rest)
}

#[inline]
fn parse_program_tail<'a>(program: &'a str, tail: &'a str) -> ParsedLogLine<'a> {
    match tail.as_bytes().first().copied() {
        Some(b'i') => {
            if let Some(depth) = parse_invoke(tail) {
                return ParsedLogLine::Invoke { program, depth };
            }
        }
        Some(b's') if tail == "success" => {
            return ParsedLogLine::Success { program };
        }
        Some(b'f') => {
            if let Some(reason) = tail.strip_prefix("failed: ") {
                return ParsedLogLine::Failure {
                    program,
                    reason: reason.trim(),
                };
            }
        }
        Some(b'c') => {
            if let Some((used, limit)) = parse_consumed(tail) {
                return ParsedLogLine::Consumed {
                    program,
                    used,
                    limit,
                };
            }
        }
        Some(b'r' | b'R') => {
            if let Some(units) = parse_request_units(tail) {
                return ParsedLogLine::CbRequestUnits { program, units };
            }
        }
        Some(b'l') => {
            if let Some(text) = tail.strip_prefix("log:") {
                return ParsedLogLine::ProgramIdLog {
                    program,
                    text: text.trim(),
                };
            }
        }
        _ => {}
    }

    ParsedLogLine::UnparsedProgram
}

#[cfg(test)]
mod tests {
    use super::*;

    const CB_PK: &str = "ComputeBudget111111111111111111111111111111";

    #[test]
    fn parses_comma_numbers_without_allocating() {
        assert_eq!(parse_u32_commas("1,234,567"), Some(1_234_567));
        assert_eq!(parse_u64_commas("8,526,566,072"), Some(8_526_566_072));
        assert_eq!(parse_u32_commas(""), None);
        assert_eq!(parse_u32_commas("12x"), None);
    }

    #[test]
    fn parses_runtime_program_shapes() {
        let program = CB_PK;

        assert_eq!(
            parse_line(&format!("Program {program} invoke [300]")),
            ParsedLogLine::Invoke {
                program,
                depth: u8::MAX
            }
        );
        assert_eq!(
            parse_line(&format!(
                "Program {program} consumed 1,234 of 2,000 compute units"
            )),
            ParsedLogLine::Consumed {
                program,
                used: 1_234,
                limit: 2_000
            }
        );
        assert_eq!(
            parse_line(&format!("Program {program} request units: 42")),
            ParsedLogLine::CbRequestUnits { program, units: 42 }
        );
        assert_eq!(
            parse_line(&format!("Program {program} request units : 43")),
            ParsedLogLine::CbRequestUnits { program, units: 43 }
        );
        assert_eq!(
            parse_line("Program is not cached"),
            ParsedLogLine::ProgramNotCached { program: None }
        );
        assert_eq!(
            parse_line(&format!("Program {program} is not cached")),
            ParsedLogLine::ProgramNotCached {
                program: Some(program)
            }
        );
        assert_eq!(
            parse_line("Program is finalized"),
            ParsedLogLine::Plain {
                text: "Program is finalized"
            }
        );
        assert_eq!(
            parse_line("Program failed to complete: custom program error: 0x1"),
            ParsedLogLine::FailedToComplete {
                reason: "custom program error: 0x1"
            }
        );
        assert_eq!(
            parse_line("Program account too small"),
            ParsedLogLine::Plain {
                text: "Program account too small"
            }
        );
    }

    #[test]
    fn parses_empty_program_log_payloads() {
        let program = CB_PK;

        assert_eq!(
            parse_line("Program log:"),
            ParsedLogLine::ProgramLog { text: "" }
        );
        assert_eq!(
            parse_line("Program log: "),
            ParsedLogLine::ProgramLog { text: "" }
        );
        assert_eq!(
            parse_line(&format!("Program {program} log:")),
            ParsedLogLine::ProgramIdLog { program, text: "" }
        );
        assert_eq!(
            parse_line(&format!("Program {program} log: ")),
            ParsedLogLine::ProgramIdLog { program, text: "" }
        );
    }

    #[test]
    fn classifies_failed_reasons() {
        assert_eq!(
            classify_failed_reason("custom program error: 0x2a"),
            FailedReason::Custom(42)
        );
        assert_eq!(
            classify_failed_reason("invalid account data for instruction"),
            FailedReason::InvalidAccountData
        );
        assert_eq!(
            classify_failed_reason(" something else "),
            FailedReason::Other("something else")
        );
    }

    #[test]
    fn parses_system_and_loader_plain_shapes() {
        assert_eq!(
            parse_line("Transfer: insufficient lamports 6,990,000, need 8,526,566,072"),
            ParsedLogLine::SystemTransferInsufficient {
                have: 6_990_000,
                need: 8_526_566_072,
            }
        );
        assert_eq!(
            parse_line("Transfer: `from` must not carry data"),
            ParsedLogLine::SystemTransferFromMustNotCarryData
        );
        assert_eq!(
            parse_line(
                "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe's writable privilege escalated"
            ),
            ParsedLogLine::RuntimeWritablePrivilegeEscalated {
                account: "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe"
            }
        );
        assert_eq!(
            parse_line("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe's signer privilege escalated"),
            ParsedLogLine::RuntimeSignerPrivilegeEscalated {
                account: "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe"
            }
        );
        assert_eq!(parse_line("Log truncated"), ParsedLogLine::LogTruncated);
        assert_eq!(
            parse_line(
                "failed to verify account SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe instruction spent from the balance of an account it does not own"
            ),
            ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed {
                account: "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe"
            }
        );
        assert_eq!(
            parse_line(
                "SystemProgram::CreateAccount data size limited to 10240 in inner instructions"
            ),
            ParsedLogLine::SystemCreateAccountDataSizeLimited { limit: 10_240 }
        );

        assert_eq!(
            parse_line(
                "Allocate: account Address { address: HPaSvdb4hyEP5SEJZW1toTAAmtrM3cGdntw2xQaFb2Uo, base: None } already in use"
            ),
            ParsedLogLine::SystemAllocateAccountAlreadyInUse {
                account: SystemAddress {
                    address: "HPaSvdb4hyEP5SEJZW1toTAAmtrM3cGdntw2xQaFb2Uo",
                    base: None,
                },
            }
        );

        assert_eq!(
            parse_line(
                "Create Account: account Address { address: FVkeQ7KtaT4SGcAc4DjJR263ucmGfoHJ1gC1JWQYmwMu, base: Some(13H2M1C3w2pwr6oYf6ZKcPGNut6mQedRAFzCpMF2iGJK) } already in use"
            ),
            ParsedLogLine::SystemCreateAccountAlreadyInUse {
                account: SystemAddress {
                    address: "FVkeQ7KtaT4SGcAc4DjJR263ucmGfoHJ1gC1JWQYmwMu",
                    base: Some("13H2M1C3w2pwr6oYf6ZKcPGNut6mQedRAFzCpMF2iGJK"),
                },
            }
        );

        assert_eq!(
            parse_line("Merging stake accounts"),
            ParsedLogLine::StakeMergingAccounts
        );
        assert_eq!(
            parse_line("Upgraded program 3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"),
            ParsedLogLine::LoaderUpgradedProgram {
                program: "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"
            }
        );
        assert_eq!(
            parse_line("Finalized account 3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"),
            ParsedLogLine::LoaderFinalizedAccount {
                account: "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"
            }
        );
        assert_eq!(
            parse_line("Call BPF program 3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"),
            ParsedLogLine::BpfInvoke {
                program: "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"
            }
        );
        assert_eq!(
            parse_line("BPF program 3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328 success"),
            ParsedLogLine::BpfSuccess {
                program: "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328"
            }
        );
        assert_eq!(
            parse_line(
                "BPF program 3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328 failed custom program error: 0x1"
            ),
            ParsedLogLine::BpfFailure {
                program: "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328",
                reason: "custom program error: 0x1",
            }
        );
        assert_eq!(
            parse_line("BPF program consumed 1,234 of 2,000 units"),
            ParsedLogLine::BpfConsumed {
                used: 1_234,
                limit: 2_000,
            }
        );
        assert_eq!(
            parse_line(
                "BPF program 3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328 failed insufficient account keys for instruction"
            ),
            ParsedLogLine::BpfFailure {
                program: "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328",
                reason: "insufficient account keys for instruction",
            }
        );
    }
}
