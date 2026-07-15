use std::{collections::BTreeMap, str::FromStr};

use blockzilla_log_parser::{
    FailedReason, ParsedLogLine, classify_failed_reason, parse_custom_program_error_reason,
    parse_line, parse_program_log_error_payload,
};
use data_encoding::BASE64;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use wincode::{SchemaRead, SchemaWrite};

use crate::program_logs::{self, ProgramLog, system_program};
use crate::{CompactPubkey, KeyIndex, KeyStore, PubkeyCompactor};

pub type StrId = u32;
pub type ProgramId = CompactPubkey;
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
    pub lengths: Vec<u32>,
    pub bytes: Vec<u8>,
}

impl StringTable {
    #[inline]
    pub fn push(&mut self, s: &str) -> StrId {
        let id = self.lengths.len() as StrId;
        let len = u32::try_from(s.len()).expect("log string too large");
        self.lengths.push(len);
        self.bytes.extend_from_slice(s.as_bytes());
        id
    }

    #[inline]
    pub fn resolve(&self, id: StrId) -> &str {
        let id = id as usize;
        let start = self
            .lengths
            .iter()
            .take(id)
            .fold(0usize, |offset, len| offset + *len as usize);
        let end = start + self.lengths[id] as usize;
        std::str::from_utf8(&self.bytes[start..end]).expect("StringTable stores valid utf-8")
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.lengths.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.lengths.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> StringTableIter<'_> {
        StringTableIter {
            table: self,
            next: 0,
            offset: 0,
        }
    }
}

pub struct StringTableIter<'a> {
    table: &'a StringTable,
    next: usize,
    offset: usize,
}

impl<'a> Iterator for StringTableIter<'a> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let len = *self.table.lengths.get(self.next)? as usize;
        let start = self.offset;
        let end = start + len;
        self.next += 1;
        self.offset = end;
        Some(
            std::str::from_utf8(&self.table.bytes[start..end])
                .expect("StringTable stores valid utf-8"),
        )
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct DataArray {
    pub chunk_count: u32,
}

#[derive(Debug, Default, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct DataTable {
    pub arrays: Vec<DataArray>,
    pub chunk_lengths: Vec<u32>,
    pub bytes: Vec<u8>,
}

impl DataTable {
    #[inline]
    pub fn push_base64_chunks(&mut self, text: &str, scratch: &mut Vec<u8>) -> Option<DataId> {
        let id = self.arrays.len() as DataId;
        let arrays_len = self.arrays.len();
        let chunks_len = self.chunk_lengths.len();
        let bytes_len = self.bytes.len();
        let mut chunk_count = 0u32;

        let trimmed = text.trim();
        if !trimmed.is_empty() {
            for token in trimmed.split_whitespace() {
                scratch.clear();
                let capacity = BASE64.decode_len(token.len()).ok()?;
                scratch.resize(capacity, 0);
                let used = match BASE64.decode_mut(token.as_bytes(), scratch) {
                    Ok(used) => used,
                    Err(_) => {
                        self.arrays.truncate(arrays_len);
                        self.chunk_lengths.truncate(chunks_len);
                        self.bytes.truncate(bytes_len);
                        return None;
                    }
                };
                scratch.truncate(used);
                self.chunk_lengths.push(used as u32);
                self.bytes.extend_from_slice(scratch);
                let Some(next_chunk_count) = chunk_count.checked_add(1) else {
                    self.arrays.truncate(arrays_len);
                    self.chunk_lengths.truncate(chunks_len);
                    self.bytes.truncate(bytes_len);
                    return None;
                };
                chunk_count = next_chunk_count;
            }
        }

        self.arrays.push(DataArray { chunk_count });
        Some(id)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.arrays.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    #[inline]
    pub fn chunks(&self, id: DataId) -> DataChunks<'_> {
        let id = id as usize;
        let first_chunk = self
            .arrays
            .iter()
            .take(id)
            .fold(0usize, |offset, array| offset + array.chunk_count as usize);
        let chunk_count = self.arrays[id].chunk_count as usize;
        let byte_offset = self
            .chunk_lengths
            .iter()
            .take(first_chunk)
            .fold(0usize, |offset, len| offset + *len as usize);

        DataChunks {
            lengths: &self.chunk_lengths[first_chunk..first_chunk + chunk_count],
            bytes: &self.bytes[byte_offset..],
            offset: 0,
            next: 0,
        }
    }

    #[inline]
    pub fn render(&self, id: DataId) -> String {
        self.chunks(id)
            .map(|chunk| BASE64.encode(chunk))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub struct DataChunks<'a> {
    lengths: &'a [u32],
    bytes: &'a [u8],
    offset: usize,
    next: usize,
}

impl<'a> Iterator for DataChunks<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let len = *self.lengths.get(self.next)? as usize;
        let start = self.offset;
        let end = start + len;
        self.next += 1;
        self.offset = end;
        Some(&self.bytes[start..end])
    }
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LogEvent {
    /// System program structured logs (system_program.rs)
    System(system_program::SystemProgramLog),

    /// Runtime log collector limit marker: `Log truncated`
    LogTruncated,

    /// Stake program/runtime: `Merging stake accounts`
    StakeMergingAccounts,

    /// BPF upgradeable loader: `Upgraded program <pk>`
    LoaderUpgradedProgram {
        program: ProgramId,
    },

    /// Historical loader log: `Finalized account <pk>`
    LoaderFinalizedAccount {
        account: ProgramId,
    },

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

    /// Official program/runtime payload emitted as a raw line without `Program log: `.
    ProgramPlainLog(ProgramLog),

    ProgramAccountNotWritable,
    ProgramIdMismatch,
    ProgramNotUpgradeable,
    ProgramAndProgramDataAccountMismatch,
    ProgramWasExtendedInThisBlockAlready,

    Invoke {
        program: ProgramId,
        depth: u8,
    },

    /// Historical loader/runtime log: `Call BPF program <pk>`
    BpfInvoke {
        program: ProgramId,
    },
    Consumed {
        program: ProgramId,
        used: u32,
        limit: u32,
    },

    /// Historical loader/runtime log: `BPF program consumed <used> of <limit> units`
    BpfConsumed {
        used: u32,
        limit: u32,
    },
    Success {
        program: ProgramId,
    },

    /// Historical loader/runtime log: `BPF program <pk> success`
    BpfSuccess {
        program: ProgramId,
    },

    /// `Program <pk> failed: <reason>`
    Failure {
        program: ProgramId,
        reason: StrId,
    },

    /// Historical loader/runtime log: `BPF program <pk> failed <reason>`
    BpfFailure {
        program: ProgramId,
        reason: StrId,
    },

    /// `Program <pk> failed: custom program error: 0xNN`
    FailureCustomProgramError {
        program: ProgramId,
        code: u32,
    },

    /// Historical loader/runtime log: `BPF program <pk> failed custom program error: 0xNN`
    BpfFailureCustomProgramError {
        program: ProgramId,
        code: u32,
    },

    /// `Program <pk> failed: invalid account data for instruction`
    FailureInvalidAccountData {
        program: ProgramId,
    },

    /// Historical loader/runtime log: `BPF program <pk> failed invalid account data for instruction`
    BpfFailureInvalidAccountData {
        program: ProgramId,
    },

    /// `Program <pk> failed: invalid program argument`
    FailureInvalidProgramArgument {
        program: ProgramId,
    },

    /// Historical loader/runtime log: `BPF program <pk> failed invalid program argument`
    BpfFailureInvalidProgramArgument {
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

    ProgramNotCached {
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

    RuntimeWritablePrivilegeEscalated {
        account: ProgramId,
    },
    RuntimeSignerPrivilegeEscalated {
        account: ProgramId,
    },

    RuntimeAccountOwnerBalanceVerificationFailed {
        account: ProgramId,
    },

    /// Runtime context teardown
    CloseContextState,

    Plain {
        text: StrId,
    },
    Unparsed {
        text: StrId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactLogParseCount {
    pub label: String,
    pub count: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactLogParseStatsReport {
    pub lines: u64,
    pub empty_lines: u64,
    pub system_candidate_lines: u64,
    pub system_parsed_lines: u64,
    pub program_log_known: u64,
    pub program_log_unknown: u64,
    pub program_plain_log_known: u64,
    pub program_plain_log_unknown: u64,
    pub pubkey_cache_hits: u64,
    pub pubkey_cache_misses: u64,
    pub pubkey_cache_max_entries: usize,
    pub shape_counts: Vec<CompactLogParseCount>,
    pub program_counts: Vec<CompactLogParseCount>,
    pub program_log_program_counts: Vec<CompactLogParseCount>,
    pub program_log_variant_counts: Vec<CompactLogParseCount>,
}

#[derive(Debug, Clone, Default)]
pub struct CompactLogParseStats {
    pub lines: u64,
    pub empty_lines: u64,
    pub system_candidate_lines: u64,
    pub system_parsed_lines: u64,
    pub program_log_known: u64,
    pub program_log_unknown: u64,
    pub program_plain_log_known: u64,
    pub program_plain_log_unknown: u64,
    pub pubkey_cache_hits: u64,
    pub pubkey_cache_misses: u64,
    pub pubkey_cache_max_entries: usize,
    shape_counts: BTreeMap<&'static str, u64>,
    program_counts: BTreeMap<String, u64>,
    program_log_program_counts: BTreeMap<String, u64>,
    program_log_variant_counts: BTreeMap<&'static str, u64>,
}

impl CompactLogParseStats {
    #[inline]
    fn increment_shape(&mut self, label: &'static str) {
        increment_count(&mut self.shape_counts, label);
    }

    #[inline]
    fn increment_program(&mut self, program: &str) {
        increment_count(&mut self.program_counts, program.to_owned());
    }

    #[inline]
    fn increment_program_log_program(&mut self, program: &str) {
        increment_count(&mut self.program_log_program_counts, program.to_owned());
    }

    #[inline]
    fn increment_program_log_variant(&mut self, label: &'static str) {
        increment_count(&mut self.program_log_variant_counts, label);
    }

    #[inline]
    fn record_program_log(&mut self, program: Option<&str>, log: &ProgramLog) {
        if let Some(program) = program {
            self.increment_program_log_program(program);
        }
        self.increment_program_log_variant(program_log_variant_label(log));
        if matches!(log, ProgramLog::Unknown(_)) {
            self.program_log_unknown = self.program_log_unknown.saturating_add(1);
        } else {
            self.program_log_known = self.program_log_known.saturating_add(1);
        }
    }

    #[inline]
    fn record_program_plain_log(&mut self, log: &ProgramLog) {
        self.increment_program_log_variant(program_log_variant_label(log));
        if matches!(log, ProgramLog::Unknown(_)) {
            self.program_plain_log_unknown = self.program_plain_log_unknown.saturating_add(1);
        } else {
            self.program_plain_log_known = self.program_plain_log_known.saturating_add(1);
        }
    }

    #[inline]
    fn record_pubkey_cache(&mut self, cache: &LogPubkeyCache<'_>) {
        self.pubkey_cache_hits = self.pubkey_cache_hits.saturating_add(cache.hits);
        self.pubkey_cache_misses = self.pubkey_cache_misses.saturating_add(cache.misses);
        self.pubkey_cache_max_entries = self.pubkey_cache_max_entries.max(cache.max_entries);
    }

    pub fn report(&self, limit: usize) -> CompactLogParseStatsReport {
        CompactLogParseStatsReport {
            lines: self.lines,
            empty_lines: self.empty_lines,
            system_candidate_lines: self.system_candidate_lines,
            system_parsed_lines: self.system_parsed_lines,
            program_log_known: self.program_log_known,
            program_log_unknown: self.program_log_unknown,
            program_plain_log_known: self.program_plain_log_known,
            program_plain_log_unknown: self.program_plain_log_unknown,
            pubkey_cache_hits: self.pubkey_cache_hits,
            pubkey_cache_misses: self.pubkey_cache_misses,
            pubkey_cache_max_entries: self.pubkey_cache_max_entries,
            shape_counts: top_counts(&self.shape_counts, limit),
            program_counts: top_counts(&self.program_counts, limit),
            program_log_program_counts: top_counts(&self.program_log_program_counts, limit),
            program_log_variant_counts: top_counts(&self.program_log_variant_counts, limit),
        }
    }
}

#[inline]
fn increment_count<K>(counts: &mut BTreeMap<K, u64>, key: K)
where
    K: Ord,
{
    let entry = counts.entry(key).or_insert(0);
    *entry = entry.saturating_add(1);
}

fn top_counts<K>(counts: &BTreeMap<K, u64>, limit: usize) -> Vec<CompactLogParseCount>
where
    K: Ord + ToString,
{
    let mut values = counts
        .iter()
        .map(|(label, count)| CompactLogParseCount {
            label: label.to_string(),
            count: *count,
        })
        .collect::<Vec<_>>();
    values.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.label.cmp(&b.label)));
    values.truncate(limit);
    values
}

const LOG_PUBKEY_CACHE_MAX: usize = 64;

#[derive(Debug, Default)]
struct LogPubkeyCache<'a> {
    entries: Vec<(&'a str, Option<CompactPubkey>)>,
    hits: u64,
    misses: u64,
    max_entries: usize,
}

impl<'a> LogPubkeyCache<'a> {
    #[inline]
    fn compact<C: PubkeyCompactor>(&mut self, index: &C, key: &'a str) -> Option<CompactPubkey> {
        if let Some((_, value)) = self.entries.iter().find(|(cached, _)| *cached == key) {
            self.hits = self.hits.saturating_add(1);
            return *value;
        }

        let value = index.compact_str(key);
        self.misses = self.misses.saturating_add(1);
        if self.entries.len() < LOG_PUBKEY_CACHE_MAX {
            self.entries.push((key, value));
            self.max_entries = self.max_entries.max(self.entries.len());
        }
        value
    }
}

#[inline]
pub fn strip_trailing_dot(s: &str) -> &str {
    s.strip_suffix('.').unwrap_or(s).trim()
}

#[inline]
fn could_be_system_program_log(line: &str) -> bool {
    let line = match line.as_bytes().first().copied() {
        Some(b' ' | b'\t' | b'\n' | b'\r') => line.trim_start(),
        _ => line,
    };
    match line.as_bytes().first().copied() {
        Some(b'A') => {
            line.starts_with("Advance nonce account: ")
                || line.starts_with("Allocate: ")
                || line.starts_with("Assign: ")
                || line.starts_with("Authorize nonce account: ")
        }
        Some(b'C') => line.starts_with("Create: ") || line.starts_with("Create Account: "),
        Some(b'I') => {
            line.starts_with("Initialize nonce account: ") || line.starts_with("Instruction: ")
        }
        Some(b'S') => line.starts_with("SystemProgram::"),
        Some(b'T') => line.starts_with("Transfer: "),
        Some(b'W') => line.starts_with("Withdraw nonce account: "),
        _ => false,
    }
}

#[inline]
fn pid_to_pubkey(store: &KeyStore, pid: ProgramId) -> Pubkey {
    pid.to_pubkey(store).unwrap_or_else(|| {
        panic!(
            "log.rs: ProgramId out of bounds: pid={:?} len={}",
            pid,
            store.len()
        )
    })
}

pub fn parse_logs(lines: &[String], index: &KeyIndex) -> CompactLogStream {
    parse_logs_with_compactor(lines, index)
}

pub fn parse_logs_with_compactor<C: PubkeyCompactor>(
    lines: &[String],
    index: &C,
) -> CompactLogStream {
    parse_log_iter_with_compactor(lines.iter().map(String::as_str), lines.len(), index, None)
}

pub fn parse_logs_with_compactor_and_stats<C: PubkeyCompactor>(
    lines: &[String],
    index: &C,
    stats: &mut CompactLogParseStats,
) -> CompactLogStream {
    parse_log_iter_with_compactor(
        lines.iter().map(String::as_str),
        lines.len(),
        index,
        Some(stats),
    )
}

pub fn parse_log_strs_with_compactor<C: PubkeyCompactor>(
    lines: &[&str],
    index: &C,
) -> CompactLogStream {
    parse_log_iter_with_compactor(lines.iter().copied(), lines.len(), index, None)
}

pub fn parse_log_strs_with_compactor_and_stats<C: PubkeyCompactor>(
    lines: &[&str],
    index: &C,
    stats: &mut CompactLogParseStats,
) -> CompactLogStream {
    parse_log_iter_with_compactor(lines.iter().copied(), lines.len(), index, Some(stats))
}

fn parse_log_iter_with_compactor<'a, I, C>(
    lines: I,
    estimated_len: usize,
    index: &C,
    mut stats: Option<&mut CompactLogParseStats>,
) -> CompactLogStream
where
    I: IntoIterator<Item = &'a str>,
    C: PubkeyCompactor,
{
    let mut st = StringTable::default();
    let mut dt = DataTable::default();
    let mut events = Vec::with_capacity(estimated_len);
    let mut decode_buf = Vec::new();
    let mut program_stack = Vec::<&'a str>::new();
    let mut pubkey_cache = LogPubkeyCache::default();

    let cb_pid = index.compact_str(CB_PK);

    for line in lines {
        if let Some(stats) = stats.as_deref_mut() {
            stats.lines = stats.lines.saturating_add(1);
        }

        let line = line.trim_end();
        if line.is_empty() {
            if let Some(stats) = stats.as_deref_mut() {
                stats.empty_lines = stats.empty_lines.saturating_add(1);
            }
            continue;
        }

        // 1) First, let the SystemProgramLog try to parse any "system program-ish" lines.
        if could_be_system_program_log(line) {
            if let Some(stats) = stats.as_deref_mut() {
                stats.system_candidate_lines = stats.system_candidate_lines.saturating_add(1);
            }
            if let Some(sys) = system_program::SystemProgramLog::parse(line, index, &mut st) {
                if let Some(stats) = stats.as_deref_mut() {
                    stats.system_parsed_lines = stats.system_parsed_lines.saturating_add(1);
                    stats.increment_shape("System");
                }
                events.push(LogEvent::System(sys));
                continue;
            }
        }

        let parsed = parse_line(line);
        if let Some(stats) = stats.as_deref_mut() {
            stats.increment_shape(parsed_log_line_label(parsed));
            if let Some(program) = parsed_log_primary_program(parsed, program_stack.last().copied())
            {
                stats.increment_program(program);
            }
        }

        match parsed {
            ParsedLogLine::CustomProgramError { code } => {
                events.push(LogEvent::CustomProgramError { code });
            }
            ParsedLogLine::FailedToComplete { reason } => {
                events.push(LogEvent::FailedToComplete {
                    reason: st.push(reason),
                });
            }
            ParsedLogLine::UnknownProgram { program } => {
                if Pubkey::from_str(program).is_ok() {
                    events.push(LogEvent::UnknownProgram {
                        program: st.push(program),
                    });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::UnknownAccount { account } => {
                if Pubkey::from_str(account).is_ok() {
                    events.push(LogEvent::UnknownAccount {
                        account: st.push(account),
                    });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::LogTruncated => events.push(LogEvent::LogTruncated),
            ParsedLogLine::VerifyEd25519 => events.push(LogEvent::VerifyEd25519),
            ParsedLogLine::VerifySecp256k1 => events.push(LogEvent::VerifySecp256k1),
            ParsedLogLine::CloseContextState => events.push(LogEvent::CloseContextState),
            ParsedLogLine::ProgramAccountNotWritable => {
                events.push(LogEvent::ProgramAccountNotWritable);
            }
            ParsedLogLine::ProgramIdMismatch => events.push(LogEvent::ProgramIdMismatch),
            ParsedLogLine::ProgramNotUpgradeable => events.push(LogEvent::ProgramNotUpgradeable),
            ParsedLogLine::ProgramAndProgramDataAccountMismatch => {
                events.push(LogEvent::ProgramAndProgramDataAccountMismatch);
            }
            ParsedLogLine::ProgramWasExtendedInThisBlockAlready => {
                events.push(LogEvent::ProgramWasExtendedInThisBlockAlready);
            }
            ParsedLogLine::StakeMergingAccounts => events.push(LogEvent::StakeMergingAccounts),
            ParsedLogLine::LoaderUpgradedProgram { program: pk_txt } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::LoaderUpgradedProgram { program });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::LoaderFinalizedAccount { account: pk_txt } => {
                if let Some(account) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::LoaderFinalizedAccount { account });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::RuntimeWritablePrivilegeEscalated { account: pk_txt } => {
                if let Some(account) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::RuntimeWritablePrivilegeEscalated { account });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::RuntimeSignerPrivilegeEscalated { account: pk_txt } => {
                if let Some(account) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::RuntimeSignerPrivilegeEscalated { account });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { account: pk_txt } => {
                if let Some(account) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::SystemTransferFromMustNotCarryData => {
                events.push(LogEvent::System(
                    system_program::SystemProgramLog::TransferFromMustNotCarryData,
                ));
            }
            ParsedLogLine::SystemCreateAccountDataSizeLimited { limit } => {
                events.push(LogEvent::System(
                    system_program::SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions {
                        limit,
                    },
                ));
            }
            ParsedLogLine::SystemTransferInsufficient { .. }
            | ParsedLogLine::SystemAllocateAccountAlreadyInUse { .. }
            | ParsedLogLine::SystemCreateAccountAlreadyInUse { .. } => {
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
            }
            ParsedLogLine::ProgramLog { text } => {
                if let Some(code) = parse_custom_program_error_reason(text) {
                    events.push(LogEvent::CustomProgramError { code });
                } else if let Some(msg) = parse_program_log_error_payload(text) {
                    events.push(LogEvent::ProgramLogError { msg: st.push(msg) });
                } else {
                    let log = if let Some(program) = program_stack.last() {
                        program_logs::parse_program_log_for_program(program, text, index, &mut st)
                    } else {
                        program_logs::parse_program_log_no_id(text, index, &mut st)
                    };
                    if let Some(stats) = stats.as_deref_mut() {
                        stats.record_program_log(program_stack.last().copied(), &log);
                    }
                    events.push(LogEvent::ProgramLog(log));
                }
            }
            ParsedLogLine::ProgramIdLog {
                program: pk_txt,
                text,
            } => {
                let Some(program) = pubkey_cache.compact(index, pk_txt) else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                    continue;
                };

                if let Some(code) = parse_custom_program_error_reason(text) {
                    events.push(LogEvent::FailureCustomProgramError { program, code });
                } else if let Some(msg) = parse_program_log_error_payload(text) {
                    events.push(LogEvent::ProgramLogError { msg: st.push(msg) });
                } else {
                    let log =
                        program_logs::parse_program_log_for_program(pk_txt, text, index, &mut st);
                    if let Some(stats) = stats.as_deref_mut() {
                        stats.record_program_log(Some(pk_txt), &log);
                    }
                    events.push(LogEvent::ProgramIdLog { program, log });
                }
            }
            ParsedLogLine::ProgramData { data } => {
                if let Some(data) = dt.push_base64_chunks(data, &mut decode_buf) {
                    events.push(LogEvent::Data { data });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::ProgramReturn {
                program: pk_txt,
                data,
            } => {
                let Some(program) = pubkey_cache.compact(index, pk_txt) else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                    continue;
                };
                if let Some(data) = dt.push_base64_chunks(data, &mut decode_buf) {
                    events.push(LogEvent::Return { program, data });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::ProgramConsumption { units } => {
                events.push(LogEvent::Consumption { units });
            }
            ParsedLogLine::ProgramNotCached { program: None } => {
                events.push(LogEvent::ProgramNotCached { program: None });
            }
            ParsedLogLine::ProgramNotCached {
                program: Some(pk_txt),
            } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::ProgramNotCached {
                        program: Some(program),
                    });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::ProgramNotDeployed { program: None } => {
                events.push(LogEvent::ProgramNotDeployed { program: None });
            }
            ParsedLogLine::ProgramNotDeployed {
                program: Some(pk_txt),
            } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::ProgramNotDeployed {
                        program: Some(program),
                    });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::Invoke {
                program: pk_txt,
                depth,
            } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::Invoke { program, depth });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::BpfInvoke { program: pk_txt } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::BpfInvoke { program });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::Success { program: pk_txt } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::Success { program });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::BpfSuccess { program: pk_txt } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::BpfSuccess { program });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::Failure {
                program: pk_txt,
                reason,
            } => {
                let Some(program) = pubkey_cache.compact(index, pk_txt) else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                    continue;
                };

                match classify_failed_reason(reason) {
                    FailedReason::Custom(code) => {
                        events.push(LogEvent::FailureCustomProgramError { program, code });
                    }
                    FailedReason::InvalidAccountData => {
                        events.push(LogEvent::FailureInvalidAccountData { program });
                    }
                    FailedReason::InvalidProgramArgument => {
                        events.push(LogEvent::FailureInvalidProgramArgument { program });
                    }
                    FailedReason::Other(r) => {
                        events.push(LogEvent::Failure {
                            program,
                            reason: st.push(r),
                        });
                    }
                }
            }
            ParsedLogLine::BpfFailure {
                program: pk_txt,
                reason,
            } => {
                let Some(program) = pubkey_cache.compact(index, pk_txt) else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                    continue;
                };

                match classify_failed_reason(reason) {
                    FailedReason::Custom(code) => {
                        events.push(LogEvent::BpfFailureCustomProgramError { program, code });
                    }
                    FailedReason::InvalidAccountData => {
                        events.push(LogEvent::BpfFailureInvalidAccountData { program });
                    }
                    FailedReason::InvalidProgramArgument => {
                        events.push(LogEvent::BpfFailureInvalidProgramArgument { program });
                    }
                    FailedReason::Other(r) => {
                        events.push(LogEvent::BpfFailure {
                            program,
                            reason: st.push(r),
                        });
                    }
                }
            }
            ParsedLogLine::Consumed {
                program: pk_txt,
                used,
                limit,
            } => {
                if let Some(program) = pubkey_cache.compact(index, pk_txt) {
                    events.push(LogEvent::Consumed {
                        program,
                        used,
                        limit,
                    });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::BpfConsumed { used, limit } => {
                events.push(LogEvent::BpfConsumed { used, limit });
            }
            ParsedLogLine::CbRequestUnits {
                program: pk_txt,
                units,
            } => {
                let program = pubkey_cache.compact(index, pk_txt);
                if program.is_some() && program == cb_pid {
                    events.push(LogEvent::CbRequestUnits { units });
                } else {
                    events.push(LogEvent::Unparsed {
                        text: st.push(line),
                    });
                }
            }
            ParsedLogLine::UnparsedProgram => {
                events.push(LogEvent::Unparsed {
                    text: st.push(line),
                });
            }
            ParsedLogLine::Plain { text } => {
                let log = program_stack
                    .last()
                    .and_then(|program| {
                        program_logs::try_parse_program_log_with_table(
                            program, text, index, &mut st,
                        )
                    })
                    .or_else(|| {
                        if program_logs::program_log_has_known_binary_form(None, text) {
                            Some(program_logs::parse_program_log_no_id(text, index, &mut st))
                        } else {
                            None
                        }
                    });
                if let Some(log) = log {
                    if let Some(stats) = stats.as_deref_mut() {
                        stats.record_program_plain_log(&log);
                    }
                    events.push(LogEvent::ProgramPlainLog(log));
                } else {
                    events.push(LogEvent::Plain {
                        text: st.push(text),
                    });
                }
            }
        }

        update_program_stack(parsed, &mut program_stack);
    }

    if let Some(stats) = stats.as_deref_mut() {
        stats.record_pubkey_cache(&pubkey_cache);
    }

    CompactLogStream {
        events,
        strings: st,
        data: dt,
    }
}

#[inline]
fn parsed_log_line_label(parsed: ParsedLogLine<'_>) -> &'static str {
    match parsed {
        ParsedLogLine::CustomProgramError { .. } => "CustomProgramError",
        ParsedLogLine::FailedToComplete { .. } => "FailedToComplete",
        ParsedLogLine::UnknownProgram { .. } => "UnknownProgram",
        ParsedLogLine::UnknownAccount { .. } => "UnknownAccount",
        ParsedLogLine::LogTruncated => "LogTruncated",
        ParsedLogLine::VerifyEd25519 => "VerifyEd25519",
        ParsedLogLine::VerifySecp256k1 => "VerifySecp256k1",
        ParsedLogLine::CloseContextState => "CloseContextState",
        ParsedLogLine::ProgramAccountNotWritable => "ProgramAccountNotWritable",
        ParsedLogLine::ProgramIdMismatch => "ProgramIdMismatch",
        ParsedLogLine::ProgramNotUpgradeable => "ProgramNotUpgradeable",
        ParsedLogLine::ProgramAndProgramDataAccountMismatch => {
            "ProgramAndProgramDataAccountMismatch"
        }
        ParsedLogLine::ProgramWasExtendedInThisBlockAlready => {
            "ProgramWasExtendedInThisBlockAlready"
        }
        ParsedLogLine::StakeMergingAccounts => "StakeMergingAccounts",
        ParsedLogLine::LoaderUpgradedProgram { .. } => "LoaderUpgradedProgram",
        ParsedLogLine::LoaderFinalizedAccount { .. } => "LoaderFinalizedAccount",
        ParsedLogLine::BpfInvoke { .. } => "BpfInvoke",
        ParsedLogLine::BpfSuccess { .. } => "BpfSuccess",
        ParsedLogLine::BpfFailure { .. } => "BpfFailure",
        ParsedLogLine::BpfConsumed { .. } => "BpfConsumed",
        ParsedLogLine::RuntimeWritablePrivilegeEscalated { .. } => {
            "RuntimeWritablePrivilegeEscalated"
        }
        ParsedLogLine::RuntimeSignerPrivilegeEscalated { .. } => "RuntimeSignerPrivilegeEscalated",
        ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { .. } => {
            "RuntimeAccountOwnerBalanceVerificationFailed"
        }
        ParsedLogLine::SystemTransferInsufficient { .. } => "SystemTransferInsufficient",
        ParsedLogLine::SystemTransferFromMustNotCarryData => "SystemTransferFromMustNotCarryData",
        ParsedLogLine::SystemAllocateAccountAlreadyInUse { .. } => {
            "SystemAllocateAccountAlreadyInUse"
        }
        ParsedLogLine::SystemCreateAccountAlreadyInUse { .. } => "SystemCreateAccountAlreadyInUse",
        ParsedLogLine::SystemCreateAccountDataSizeLimited { .. } => {
            "SystemCreateAccountDataSizeLimited"
        }
        ParsedLogLine::ProgramLog { .. } => "ProgramLog",
        ParsedLogLine::ProgramIdLog { .. } => "ProgramIdLog",
        ParsedLogLine::ProgramData { .. } => "ProgramData",
        ParsedLogLine::ProgramReturn { .. } => "ProgramReturn",
        ParsedLogLine::ProgramConsumption { .. } => "ProgramConsumption",
        ParsedLogLine::ProgramNotCached { .. } => "ProgramNotCached",
        ParsedLogLine::ProgramNotDeployed { .. } => "ProgramNotDeployed",
        ParsedLogLine::Invoke { .. } => "Invoke",
        ParsedLogLine::Success { .. } => "Success",
        ParsedLogLine::Failure { .. } => "Failure",
        ParsedLogLine::Consumed { .. } => "Consumed",
        ParsedLogLine::CbRequestUnits { .. } => "CbRequestUnits",
        ParsedLogLine::UnparsedProgram => "UnparsedProgram",
        ParsedLogLine::Plain { .. } => "Plain",
    }
}

#[inline]
fn parsed_log_primary_program<'a>(
    parsed: ParsedLogLine<'a>,
    active_program: Option<&'a str>,
) -> Option<&'a str> {
    match parsed {
        ParsedLogLine::ProgramLog { .. } | ParsedLogLine::ProgramConsumption { .. } => {
            active_program
        }
        ParsedLogLine::ProgramIdLog { program, .. }
        | ParsedLogLine::ProgramReturn { program, .. }
        | ParsedLogLine::BpfInvoke { program }
        | ParsedLogLine::BpfSuccess { program }
        | ParsedLogLine::BpfFailure { program, .. }
        | ParsedLogLine::Invoke { program, .. }
        | ParsedLogLine::Success { program }
        | ParsedLogLine::Failure { program, .. }
        | ParsedLogLine::Consumed { program, .. }
        | ParsedLogLine::CbRequestUnits { program, .. } => Some(program),
        ParsedLogLine::ProgramNotCached { program }
        | ParsedLogLine::ProgramNotDeployed { program } => program,
        _ => None,
    }
}

#[inline]
fn program_log_variant_label(log: &ProgramLog) -> &'static str {
    match log {
        ProgramLog::Empty => "Empty",
        ProgramLog::Token(_) => "Token",
        ProgramLog::Token2022(_) => "Token2022",
        ProgramLog::Ata(_) => "Ata",
        ProgramLog::AddressLookupTable(_) => "AddressLookupTable",
        ProgramLog::LoaderV3(_) => "LoaderV3",
        ProgramLog::LoaderV4(_) => "LoaderV4",
        ProgramLog::Memo(_) => "Memo",
        ProgramLog::Record(_) => "Record",
        ProgramLog::TransferHook(_) => "TransferHook",
        ProgramLog::AccountCompression(_) => "AccountCompression",
        ProgramLog::Stake(_) => "Stake",
        ProgramLog::ZkElgamalProof(_) => "ZkElgamalProof",
        ProgramLog::AnchorInstruction { .. } => "AnchorInstruction",
        ProgramLog::AnchorErrorOccurred { .. } => "AnchorErrorOccurred",
        ProgramLog::AnchorErrorThrown { .. } => "AnchorErrorThrown",
        ProgramLog::Unknown(_) => "Unknown",
        #[cfg(feature = "known-program-logs")]
        ProgramLog::Known(_) => "Known",
    }
}

fn update_program_stack<'a>(parsed: ParsedLogLine<'a>, stack: &mut Vec<&'a str>) {
    match parsed {
        ParsedLogLine::Invoke { program, depth } => {
            let depth = depth as usize;
            if depth == 0 {
                stack.clear();
                stack.push(program);
                return;
            }
            stack.truncate(depth.saturating_sub(1));
            stack.push(program);
        }
        ParsedLogLine::BpfInvoke { program } => {
            stack.push(program);
        }
        ParsedLogLine::Success { program }
        | ParsedLogLine::Failure { program, .. }
        | ParsedLogLine::BpfSuccess { program }
        | ParsedLogLine::BpfFailure { program, .. } => {
            if stack.last().is_some_and(|active| *active == program) {
                stack.pop();
            } else if let Some(position) = stack.iter().rposition(|active| *active == program) {
                stack.truncate(position);
            } else {
                stack.clear();
            }
        }
        _ => {}
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
            LogEvent::BpfInvoke { program } => out.push(format!(
                "Call BPF program {}",
                pid_to_pubkey(store, *program)
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
            LogEvent::BpfConsumed { used, limit } => {
                out.push(format!("BPF program consumed {used} of {limit} units"))
            }
            LogEvent::Success { program } => out.push(format!(
                "Program {} success",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::BpfSuccess { program } => out.push(format!(
                "BPF program {} success",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::Failure { program, reason } => out.push(format!(
                "Program {} failed: {}",
                pid_to_pubkey(store, *program),
                st.resolve(*reason)
            )),
            LogEvent::BpfFailure { program, reason } => out.push(format!(
                "BPF program {} failed {}",
                pid_to_pubkey(store, *program),
                st.resolve(*reason)
            )),
            LogEvent::FailureCustomProgramError { program, code } => out.push(format!(
                "Program {} failed: custom program error: 0x{:x}",
                pid_to_pubkey(store, *program),
                code
            )),
            LogEvent::BpfFailureCustomProgramError { program, code } => out.push(format!(
                "BPF program {} failed custom program error: 0x{:x}",
                pid_to_pubkey(store, *program),
                code
            )),
            LogEvent::FailureInvalidAccountData { program } => out.push(format!(
                "Program {} failed: invalid account data for instruction",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::BpfFailureInvalidAccountData { program } => out.push(format!(
                "BPF program {} failed invalid account data for instruction",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::FailureInvalidProgramArgument { program } => out.push(format!(
                "Program {} failed: invalid program argument",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::BpfFailureInvalidProgramArgument { program } => out.push(format!(
                "BPF program {} failed invalid program argument",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::FailedToComplete { reason } => out.push(format!(
                "Program failed to complete: {}",
                st.resolve(*reason)
            )),
            LogEvent::System(sys) => out.push(sys.render(st, store)),
            LogEvent::LogTruncated => out.push("Log truncated".to_string()),
            LogEvent::StakeMergingAccounts => out.push("Merging stake accounts".to_string()),
            LogEvent::LoaderUpgradedProgram { program } => out.push(format!(
                "Upgraded program {}",
                pid_to_pubkey(store, *program)
            )),
            LogEvent::LoaderFinalizedAccount { account } => out.push(format!(
                "Finalized account {}",
                pid_to_pubkey(store, *account)
            )),
            LogEvent::ProgramLog(log) => {
                let payload = program_logs::render_program_log(log, store, st);
                if payload.is_empty() {
                    out.push("Program log:".to_string());
                } else {
                    out.push(format!("Program log: {}", payload));
                }
            }
            LogEvent::ProgramLogError { msg } => {
                out.push(format!("Program log: Error: {}", st.resolve(*msg)));
            }
            LogEvent::ProgramIdLog { program, log } => {
                let payload = program_logs::render_program_log(log, store, st);
                let program = pid_to_pubkey(store, *program);
                if payload.is_empty() {
                    out.push(format!("Program {program} log:"));
                } else {
                    out.push(format!("Program {program} log: {payload}"));
                }
            }
            LogEvent::ProgramPlainLog(log) => {
                out.push(program_logs::render_program_log(log, store, st));
            }
            LogEvent::ProgramAccountNotWritable => {
                out.push("Program account not writeable".to_string())
            }
            LogEvent::ProgramNotUpgradeable => out.push("Program not upgradeable".to_string()),
            LogEvent::ProgramAndProgramDataAccountMismatch => {
                out.push("Program and ProgramData account mismatch".to_string())
            }
            LogEvent::ProgramWasExtendedInThisBlockAlready => {
                out.push("Program was extended in this block already".to_string())
            }
            LogEvent::CustomProgramError { code } => {
                out.push(format!("custom program error: 0x{:x}", code))
            }
            LogEvent::Return { program, data } => out.push(format!(
                "Program return: {} {}",
                pid_to_pubkey(store, *program),
                dt.render(*data),
            )),
            LogEvent::Data { data } => out.push(format!("Program data: {}", dt.render(*data))),
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
            LogEvent::ProgramNotCached { program } => {
                if let Some(pid) = program {
                    out.push(format!(
                        "Program {} is not cached",
                        pid_to_pubkey(store, *pid)
                    ));
                } else {
                    out.push("Program is not cached".to_string());
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
            LogEvent::RuntimeWritablePrivilegeEscalated { account } => out.push(format!(
                "{}'s writable privilege escalated",
                pid_to_pubkey(store, *account)
            )),
            LogEvent::RuntimeSignerPrivilegeEscalated { account } => out.push(format!(
                "{}'s signer privilege escalated",
                pid_to_pubkey(store, *account)
            )),
            LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => out.push(format!(
                "failed to verify account {} instruction spent from the balance of an account it does not own",
                pid_to_pubkey(store, *account)
            )),
            LogEvent::CloseContextState => out.push("CloseContextState".to_string()),
            LogEvent::Plain { text } | LogEvent::Unparsed { text } => {
                out.push(st.resolve(*text).to_string())
            }
            LogEvent::ProgramIdMismatch => out.push("Program id mismatch".to_string()),
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_index(keys: &[&str]) -> KeyIndex {
        KeyIndex::build(
            keys.iter()
                .map(|key| *Pubkey::from_str(key).unwrap().as_array())
                .collect(),
        )
    }

    fn key_store(keys: &[&str]) -> KeyStore {
        KeyStore {
            keys: keys
                .iter()
                .map(|key| *Pubkey::from_str(key).unwrap().as_array())
                .collect(),
        }
    }

    #[test]
    fn string_table_is_flat_but_resolves_by_id() {
        let mut table = StringTable::default();
        let first = table.push("alpha");
        let second = table.push("beta");

        assert_eq!(table.resolve(first), "alpha");
        assert_eq!(table.resolve(second), "beta");
        assert_eq!(table.iter().collect::<Vec<_>>(), vec!["alpha", "beta"]);
    }

    #[test]
    fn data_table_decodes_base64_chunks_into_flat_storage() {
        let mut table = DataTable::default();
        let mut scratch = Vec::new();
        let id = table.push_base64_chunks("AQID BAU=", &mut scratch).unwrap();

        assert_eq!(
            table.chunks(id).collect::<Vec<_>>(),
            vec![&[1, 2, 3][..], &[4, 5][..]]
        );
        assert_eq!(table.render(id), "AQID BAU=");
    }

    #[test]
    fn malformed_program_lines_are_unparsed_instead_of_panicking() {
        let index = key_index(&[CB_PK]);
        let lines = vec!["Program NotAPubkey invoke [1]".to_string()];

        let logs = parse_logs(&lines, &index);

        match logs.events.as_slice() {
            [LogEvent::Unparsed { text }] => {
                assert_eq!(logs.strings.resolve(*text), "Program NotAPubkey invoke [1]");
            }
            other => panic!("unexpected events: {other:?}"),
        }
    }

    #[test]
    fn system_debug_address_logs_are_structured() {
        let addr_pk = Pubkey::new_from_array([7; 32]);
        let addr = addr_pk.to_string();
        let index = key_index(&[addr.as_str(), CB_PK]);
        let store = key_store(&[addr.as_str(), CB_PK]);
        let line =
            format!("Allocate: account Address {{ address: {addr}, base: None }} already in use");

        let logs = parse_logs(std::slice::from_ref(&line), &index);

        assert!(matches!(logs.events.as_slice(), [LogEvent::System(_)]));
        assert_eq!(render_logs(&logs, &store), vec![line]);
    }

    #[test]
    fn empty_program_logs_are_structured_and_round_trip() {
        let program = CB_PK;
        let index = key_index(&[program]);
        let store = key_store(&[program]);
        let lines = vec![
            "Program log:".to_string(),
            format!("Program {program} log:"),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::ProgramLog(ProgramLog::Empty),
                LogEvent::ProgramIdLog {
                    log: ProgramLog::Empty,
                    ..
                }
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn top_level_stake_and_loader_lines_are_structured() {
        let program_pk = Pubkey::new_from_array([9; 32]);
        let program = program_pk.to_string();
        let index = key_index(&[program.as_str(), CB_PK]);
        let store = key_store(&[program.as_str(), CB_PK]);
        let lines = vec![
            "Merging stake accounts".to_string(),
            format!("Upgraded program {program}"),
            format!("Finalized account {program}"),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::StakeMergingAccounts,
                LogEvent::LoaderUpgradedProgram { .. },
                LogEvent::LoaderFinalizedAccount { .. }
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn historical_bpf_loader_lines_are_structured_and_round_trip() {
        let program_pk = Pubkey::new_from_array([10; 32]);
        let program = program_pk.to_string();
        let index = key_index(&[program.as_str(), CB_PK]);
        let store = key_store(&[program.as_str(), CB_PK]);
        let lines = vec![
            format!("Call BPF program {program}"),
            "BPF program consumed 1234 of 2000 units".to_string(),
            format!("BPF program {program} success"),
            format!("Call BPF program {program}"),
            format!("BPF program {program} failed custom program error: 0x1"),
            format!("Call BPF program {program}"),
            format!("BPF program {program} failed invalid account data for instruction"),
            format!("Call BPF program {program}"),
            format!("BPF program {program} failed insufficient account keys for instruction"),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::BpfInvoke { .. },
                LogEvent::BpfConsumed { .. },
                LogEvent::BpfSuccess { .. },
                LogEvent::BpfInvoke { .. },
                LogEvent::BpfFailureCustomProgramError { .. },
                LogEvent::BpfInvoke { .. },
                LogEvent::BpfFailureInvalidAccountData { .. },
                LogEvent::BpfInvoke { .. },
                LogEvent::BpfFailure { .. },
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn program_log_payloads_use_active_program_context() {
        let ata = program_logs::associated_token_account::STR_ID;
        let index = key_index(&[ata, CB_PK]);
        let store = key_store(&[ata, CB_PK]);
        let lines = vec![
            format!("Program {ata} invoke [1]"),
            "Program log: Create".to_string(),
            format!("Program {ata} success"),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::Invoke { .. },
                LogEvent::ProgramLog(ProgramLog::Ata(
                    program_logs::associated_token_account::TokenLog::BareCreate
                )),
                LogEvent::Success { .. }
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn raw_official_program_lines_use_active_program_context() {
        let stake = program_logs::stake::STR_ID;
        let loader = program_logs::loader_v3::STR_ID;
        let deployed_pk = Pubkey::new_from_array([8; 32]).to_string();
        let index = key_index(&[stake, loader, deployed_pk.as_str(), CB_PK]);
        let store = key_store(&[stake, loader, deployed_pk.as_str(), CB_PK]);
        let lines = vec![
            format!("Program {stake} invoke [1]"),
            "Checking if source stake is mergeable".to_string(),
            format!("Program {stake} success"),
            format!("Program {loader} invoke [1]"),
            format!("Deployed program {deployed_pk}"),
            "Extended ProgramData account by 4432 bytes".to_string(),
            format!("Program {loader} success"),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::Invoke { .. },
                LogEvent::ProgramPlainLog(ProgramLog::Stake(_)),
                LogEvent::Success { .. },
                LogEvent::Invoke { .. },
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::Success { .. },
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn bpf_loader_versions_and_loader_v4_raw_lines_are_structured() {
        let loader_v1 = program_logs::loader_v3::V1_STR_ID;
        let loader_v2 = program_logs::loader_v3::V2_STR_ID;
        let loader_v4 = program_logs::loader_v4::STR_ID;
        let index = key_index(&[loader_v1, loader_v2, loader_v4, CB_PK]);
        let store = key_store(&[loader_v1, loader_v2, loader_v4, CB_PK]);
        let lines = vec![
            format!("Program {loader_v1} invoke [1]"),
            "Deprecated loader is no longer supported".to_string(),
            format!("Program {loader_v1} success"),
            format!("Program {loader_v2} invoke [1]"),
            "BPF loader management instructions are no longer supported".to_string(),
            format!("Program {loader_v2} success"),
            format!("Program {loader_v4} invoke [1]"),
            "Program is finalized".to_string(),
            "Insufficient lamports, 42 are required".to_string(),
            format!("Program {loader_v4} success"),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::Invoke { .. },
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::Success { .. },
                LogEvent::Invoke { .. },
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::Success { .. },
                LogEvent::Invoke { .. },
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV4(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV4(_)),
                LogEvent::Success { .. },
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn nonce_account_plain_lines_are_system_logs() {
        let nonce_account = "SysvarRecentB1ockHashes11111111111111111111";
        let index = key_index(&[nonce_account, CB_PK]);
        let store = key_store(&[nonce_account, CB_PK]);
        let lines = vec![
            format!("Authorize nonce account: Account {nonce_account} must be writeable"),
            format!("Advance nonce account: Account {nonce_account} must be a signer"),
            "Withdraw nonce account: insufficient lamports 7, need 9".to_string(),
            "Advance nonce account: nonce can only advance once per slot".to_string(),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::System(_),
                LogEvent::System(_),
                LogEvent::System(_),
                LogEvent::System(_),
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn runtime_privilege_escalation_is_structured() {
        let account = Pubkey::new_from_array([3; 32]).to_string();
        let index = key_index(&[account.as_str(), CB_PK]);
        let store = key_store(&[account.as_str(), CB_PK]);
        let lines = vec![
            format!("{account}'s writable privilege escalated"),
            format!("{account}'s signer privilege escalated"),
            format!(
                "failed to verify account {account} instruction spent from the balance of an account it does not own"
            ),
            "Log truncated".to_string(),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::RuntimeWritablePrivilegeEscalated { .. },
                LogEvent::RuntimeSignerPrivilegeEscalated { .. },
                LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { .. },
                LogEvent::LogTruncated,
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }

    #[test]
    fn no_active_program_official_plain_lines_still_get_binary_forms() {
        let deployed_pk = Pubkey::new_from_array([11; 32]).to_string();
        let index = key_index(&[deployed_pk.as_str(), CB_PK]);
        let store = key_store(&[deployed_pk.as_str(), CB_PK]);
        let lines = vec![
            "Incorrect buffer authority provided".to_string(),
            format!("Deployed program {deployed_pk}"),
            format!("Closed Buffer {deployed_pk}"),
            format!("Closed Program {deployed_pk}"),
            "ProgramData account not large enough".to_string(),
            format!("New authority Some({deployed_pk})"),
            "New authority None".to_string(),
            "SystemProgram::CreateAccount data size limited to 10240 in inner instructions"
                .to_string(),
        ];

        let logs = parse_logs(&lines, &index);

        assert!(matches!(
            logs.events.as_slice(),
            [
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::ProgramPlainLog(ProgramLog::LoaderV3(_)),
                LogEvent::System(_),
            ]
        ));
        assert_eq!(render_logs(&logs, &store), lines);
    }
}
