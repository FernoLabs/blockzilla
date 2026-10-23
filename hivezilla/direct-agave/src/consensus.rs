//! Offline export of already captured native footers. Export receipts do not
//! authorize source retirement, cloud custody, or a finality transition.
use anyhow::{Context, Result, ensure};
use blockzilla_bank_stream_v2::{self as v2, BankIdentity, SourceIdentity};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use v2::consensus::{self as codec, ConsensusFact, FactBinding};

pub const RECEIPT: &str = "CONSENSUS-EXPORT.v1.json";
pub const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_FACTS: usize = 1024;
pub const MAX_TOTAL_FOOTER_BYTES: usize = 1024 * 1024;
const MAX_RECORDS: usize = 1_000_000;
const MAX_RECEIPT_BYTES: usize = 4 * 1024 * 1024;
const SCHEMA: &str = "hivezilla-native-consensus-export-v1";
const DOMAIN: &str = "exact execution frame bytes";
const LOCK: &str = "EXPORT.lock";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBinding {
    pub source: SourceIdentity,
    pub bank: BankIdentity,
    pub execution_payload_sha256: String,
}
impl ExecutionBinding {
    fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.bank.validate()?;
        ensure!(
            v2::is_digest(&self.execution_payload_sha256),
            "invalid_execution_digest"
        );
        Ok(())
    }
    fn matches(&self, fact: &FactBinding) -> bool {
        self.source == fact.source
            && self.bank == fact.bank
            && self.execution_payload_sha256 == fact.execution_payload_sha256
    }
}
impl From<&FactBinding> for ExecutionBinding {
    fn from(binding: &FactBinding) -> Self {
        Self {
            source: binding.source.clone(),
            bank: binding.bank.clone(),
            execution_payload_sha256: binding.execution_payload_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputIdentity {
    pub kind: String,
    /// Digest and size of the supplied file, including compression if present.
    pub bytes: usize,
    pub sha256: String,
    pub decoded_bytes: usize,
    pub decoded_sha256: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactFile {
    pub path: String,
    pub bytes: usize,
    pub sha256: String,
    pub binding: FactBinding,
    pub footer_bytes: usize,
    pub footer_sha256: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportReceipt {
    pub schema: String,
    pub state: String,
    pub profile: String,
    pub execution: ExecutionBinding,
    pub execution_digest_domain: String,
    pub execution_frame_bytes: usize,
    pub input: InputIdentity,
    pub facts: Vec<FactFile>,
    pub verification: String,
    pub no_source_retirement_authority: bool,
    pub continuous_late_evidence_coverage: bool,
}
#[derive(Debug, Serialize)]
pub struct ExportInspection {
    pub receipt: ExportReceipt,
    /// The receipt and its files agree. An external expected binding is still
    /// needed to establish provenance; inspection does not authenticate input.
    pub local_hashes_consistent: bool,
    pub provenance_authenticated: bool,
    pub footers: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    sequence: u64,
    sha256: String,
    event: Box<serde_json::value::RawValue>,
}
#[derive(Deserialize)]
struct EventKey {
    kind: String,
    slot: u64,
    bank_id: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FooterEvent {
    kind: String,
    slot: u64,
    bank_id: u64,
    wire_footer: Vec<u8>,
}

fn open_regular(path: &Path) -> Result<File> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "input_not_regular_file"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    ensure!(file.metadata()?.is_file(), "input_not_regular_file");
    Ok(file)
}
fn bounded_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = open_regular(path)?;
    ensure!(
        file.metadata()?.len() <= limit as u64,
        "input_file_too_large"
    );
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "input_file_too_large");
    Ok(bytes)
}
fn verify_execution(binding: &ExecutionBinding, payload_path: &Path) -> Result<usize> {
    binding.validate()?;
    let bytes = bounded_file(payload_path, v2::MAX_FRAME)?;
    ensure!(
        v2::digest(&bytes) == binding.execution_payload_sha256,
        "execution_frame_digest_mismatch"
    );
    let frame = v2::decode(bytes.into())?;
    ensure!(
        frame.envelope.event.kind == "block"
            && frame.source == binding.source
            && frame.envelope.event.bank == binding.bank,
        "execution_frame_binding_mismatch"
    );
    Ok(frame.wire.len())
}
fn footer(binding: &FactBinding, raw: &[u8]) -> Result<ConsensusFact> {
    binding.validate()?;
    ensure!(
        raw.len() <= MAX_EVENT_BYTES && v2::digest(raw) == binding.source_event_sha256,
        "source_event_digest_mismatch"
    );
    let event: FooterEvent = serde_json::from_slice(raw)?;
    ensure!(
        event.kind == "Footer"
            && event.slot == binding.bank.slot
            && event.bank_id == binding.bank.bank_id,
        "footer_event_binding_mismatch"
    );
    ConsensusFact::new(binding.clone(), event.wire_footer)
}
fn add_footer_bytes(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total.checked_add(bytes).context("footer_total_overflow")?;
    ensure!(*total <= MAX_TOTAL_FOOTER_BYTES, "footer_total_too_large");
    Ok(())
}

fn input_identity(kind: &str, stored: &[u8], decoded: &[u8]) -> InputIdentity {
    InputIdentity {
        kind: kind.into(),
        bytes: stored.len(),
        sha256: v2::digest(stored),
        decoded_bytes: decoded.len(),
        decoded_sha256: v2::digest(decoded),
    }
}

/// Export exactly one Footer from an exact raw Event file or one RawEvidence
/// record. A supplied marker digest cannot substitute for the actual v2 frame.
pub fn export_event(
    binding: &FactBinding,
    event_path: &Path,
    payload_path: &Path,
    output: &Path,
) -> Result<ExportReceipt> {
    binding.validate()?;
    let execution = ExecutionBinding::from(binding);
    let frame_bytes = verify_execution(&execution, payload_path)?;
    let input = bounded_file(event_path, MAX_EVENT_BYTES)?;
    let shape: serde_json::Value = serde_json::from_slice(&input)?;
    let (kind, fact) = if shape.get("kind").is_some() {
        ("raw-footer-event", footer(binding, &input)?)
    } else {
        let record: Record = serde_json::from_slice(&input)?;
        ensure!(
            record.sequence == binding.source_event_sequence
                && record.sha256 == binding.source_event_sha256,
            "source_record_binding_mismatch"
        );
        (
            "raw-evidence-record",
            footer(binding, record.event.get().as_bytes())?,
        )
    };
    publish(
        output,
        execution,
        frame_bytes,
        input_identity(kind, &input, &input),
        vec![fact],
    )
}

/// Export each captured Footer from one bank's retained source-events array.
/// Source identity comes from the caller and must also match the actual frame.
/// All records must have the same slot and exact bank (or a slot-only status).
pub fn export_source_events(
    binding: &ExecutionBinding,
    source_events_path: &Path,
    payload_path: &Path,
    output: &Path,
) -> Result<ExportReceipt> {
    let frame_bytes = verify_execution(binding, payload_path)?;
    let stored = bounded_file(source_events_path, MAX_SOURCE_BYTES)?;
    let compressed = stored.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]);
    let decoded = if compressed {
        let mut bytes = Vec::new();
        zstd::stream::read::Decoder::new(stored.as_slice())?
            .take(MAX_SOURCE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= MAX_SOURCE_BYTES,
            "source_events_decoded_too_large"
        );
        bytes
    } else {
        stored.clone()
    };
    let records: Vec<Record> = serde_json::from_slice(&decoded)?;
    ensure!(
        !records.is_empty() && records.len() <= MAX_RECORDS,
        "source_record_count_invalid"
    );
    let mut after = 0;
    let mut facts = Vec::new();
    let mut total_footer_bytes = 0;
    for record in records {
        ensure!(
            record.sequence > after && record.sequence <= v2::SAFE && v2::is_digest(&record.sha256),
            "source_record_order_invalid"
        );
        after = record.sequence;
        let raw = record.event.get().as_bytes();
        ensure!(
            raw.len() <= MAX_EVENT_BYTES && v2::digest(raw) == record.sha256,
            "source_event_digest_mismatch"
        );
        let key: EventKey = serde_json::from_slice(raw)?;
        ensure!(
            key.slot == binding.bank.slot
                && (key.bank_id == Some(binding.bank.bank_id)
                    || (key.bank_id.is_none()
                        && matches!(key.kind.as_str(), "Status" | "BankStatusV2"))),
            "source_events_mixed_bank"
        );
        ensure!(
            matches!(
                key.kind.as_str(),
                "BankStateV2"
                    | "BankStatusV2"
                    | "Transaction"
                    | "Entry"
                    | "Block"
                    | "Status"
                    | "ClearBank"
                    | "Footer"
            ),
            "unknown_source_event"
        );
        if key.kind == "Footer" {
            ensure!(facts.len() < MAX_FACTS, "too_many_footer_facts");
            let fact_binding = FactBinding {
                source: binding.source.clone(),
                bank: binding.bank.clone(),
                execution_payload_sha256: binding.execution_payload_sha256.clone(),
                source_event_sequence: record.sequence,
                source_event_sha256: record.sha256,
            };
            let fact = footer(&fact_binding, raw)?;
            add_footer_bytes(&mut total_footer_bytes, fact.footer_wire().len())?;
            facts.push(fact);
        }
    }
    ensure!(!facts.is_empty(), "source_events_have_no_footer");
    let kind = if compressed {
        "source-events-json-zstd"
    } else {
        "source-events-json"
    };
    publish(
        output,
        binding.clone(),
        frame_bytes,
        input_identity(kind, &stored, &decoded),
        facts,
    )
}

fn fact_name(binding: &FactBinding) -> String {
    format!(
        "{:020}-{}.bzcnf",
        binding.source_event_sequence, binding.source_event_sha256
    )
}
fn directory_files(output: &Path, expected: &BTreeSet<String>) -> Result<()> {
    ensure!(
        fs::symlink_metadata(output)?.file_type().is_dir(),
        "output_not_directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            fs::metadata(output)?.permissions().mode() & 0o022 == 0,
            "output_directory_writable_by_others"
        );
    }
    let mut count = 0;
    for row in fs::read_dir(output)? {
        count += 1;
        ensure!(count <= MAX_FACTS + 1024, "output_directory_too_large");
        let row = row?;
        let name = row
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid_output_name"))?;
        ensure!(
            name == LOCK
                || name == RECEIPT
                || expected.contains(&name)
                || name.starts_with(".consensus-tmp-"),
            "output_contains_unrelated_files"
        );
        ensure!(
            row.file_type()?.is_file(),
            "output_contains_nonregular_file"
        );
    }
    Ok(())
}
fn output_lock(output: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::{fs::OpenOptionsExt, io::AsRawFd};
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let file = options.open(output.join(LOCK))?;
        ensure!(file.metadata()?.is_file(), "export_lock_not_regular");
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "consensus_export_already_active"
        );
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("consensus_export_requires_file_lock")
    }
}
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn create_immutable(output: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let destination = output.join(name);
    if fs::symlink_metadata(&destination).is_ok() {
        ensure!(
            bounded_file(&destination, bytes.len())? == bytes,
            "existing_export_file_conflict"
        );
        return Ok(());
    }
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let scratch = Scratch(output.join(format!(".consensus-tmp-{}-{nonce}", std::process::id())));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&scratch.0)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o444))?;
        file.sync_all()?;
    }
    // link creates the destination atomically and never replaces it. A crash
    // can leave only an uncommitted scratch name, not a partial fact/receipt.
    match fs::hard_link(&scratch.0, &destination) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                bounded_file(&destination, bytes.len())? == bytes,
                "existing_export_file_conflict"
            );
        }
        Err(error) => return Err(error.into()),
    }
    fs::remove_file(&scratch.0)?;
    File::open(output)?.sync_all()?;
    Ok(())
}
fn publish(
    output: &Path,
    execution: ExecutionBinding,
    frame_bytes: usize,
    input: InputIdentity,
    facts: Vec<ConsensusFact>,
) -> Result<ExportReceipt> {
    ensure!(
        !facts.is_empty() && facts.len() <= MAX_FACTS,
        "invalid_footer_fact_count"
    );
    let mut encoded = Vec::new();
    let mut files = Vec::new();
    let mut previous = 0;
    let mut bank_footer_sha: Option<String> = None;
    let mut total_footer_bytes = 0;
    for fact in facts {
        ensure!(
            execution.matches(&fact.binding) && fact.binding.source_event_sequence > previous,
            "export_fact_binding_mismatch"
        );
        previous = fact.binding.source_event_sequence;
        add_footer_bytes(&mut total_footer_bytes, fact.footer_wire().len())?;
        let footer_sha = v2::digest(fact.footer_wire());
        ensure!(
            bank_footer_sha
                .as_ref()
                .is_none_or(|old| old == &footer_sha),
            "conflicting_bank_footer"
        );
        bank_footer_sha = Some(footer_sha.clone());
        let bytes = codec::encode(&fact)?;
        files.push(FactFile {
            path: fact_name(&fact.binding),
            bytes: bytes.len(),
            sha256: v2::digest(&bytes),
            binding: fact.binding.clone(),
            footer_bytes: fact.footer_wire().len(),
            footer_sha256: footer_sha,
        });
        encoded.push(bytes);
    }
    let receipt = ExportReceipt {
        schema: SCHEMA.into(),
        state: "COMPLETE".into(),
        profile: codec::PROFILE.into(),
        execution,
        execution_digest_domain: DOMAIN.into(),
        execution_frame_bytes: frame_bytes,
        input,
        facts: files,
        verification: codec::VERIFICATION.into(),
        no_source_retirement_authority: true,
        continuous_late_evidence_coverage: false,
    };
    let mut receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
    receipt_bytes.push(b'\n');
    ensure!(
        receipt_bytes.len() <= MAX_RECEIPT_BYTES,
        "export_receipt_too_large"
    );
    let expected: BTreeSet<_> = receipt.facts.iter().map(|f| f.path.clone()).collect();
    if output.exists() {
        directory_files(output, &expected)?;
    } else {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(output)?;
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    let _lock = output_lock(output)?;
    directory_files(output, &expected)?;
    if output.join(RECEIPT).exists() {
        ensure!(
            bounded_file(&output.join(RECEIPT), MAX_RECEIPT_BYTES)? == receipt_bytes,
            "existing_export_receipt_conflict"
        );
        // A committed receipt cannot be repaired with different or missing facts.
        inspect_export(output)?;
        return Ok(receipt);
    }
    for (row, bytes) in receipt.facts.iter().zip(encoded) {
        create_immutable(output, &row.path, &bytes)?;
    }
    create_immutable(output, RECEIPT, &receipt_bytes)?; // Commit last.
    Ok(receipt)
}

pub fn inspect_export(output: &Path) -> Result<ExportInspection> {
    let receipt: ExportReceipt =
        serde_json::from_slice(&bounded_file(&output.join(RECEIPT), MAX_RECEIPT_BYTES)?)?;
    receipt.execution.validate()?;
    ensure!(
        receipt.schema == SCHEMA
            && receipt.state == "COMPLETE"
            && receipt.profile == codec::PROFILE
            && receipt.execution_digest_domain == DOMAIN
            && receipt.verification == codec::VERIFICATION
            && receipt.no_source_retirement_authority
            && !receipt.continuous_late_evidence_coverage
            && (16..=v2::MAX_FRAME).contains(&receipt.execution_frame_bytes)
            && !receipt.facts.is_empty()
            && receipt.facts.len() <= MAX_FACTS
            && (1..=MAX_SOURCE_BYTES).contains(&receipt.input.bytes)
            && (1..=MAX_SOURCE_BYTES).contains(&receipt.input.decoded_bytes)
            && v2::is_digest(&receipt.input.sha256)
            && v2::is_digest(&receipt.input.decoded_sha256)
            && matches!(
                receipt.input.kind.as_str(),
                "raw-footer-event"
                    | "raw-evidence-record"
                    | "source-events-json"
                    | "source-events-json-zstd"
            ),
        "invalid_export_receipt"
    );
    // Validate every digest and the derived flat filename before joining a
    // receipt-controlled path. decode_bound happens after the file is opened.
    let mut total_footer_bytes = 0;
    for row in &receipt.facts {
        row.binding.validate()?;
        ensure!(
            (1..=codec::MAX_FOOTER_BYTES).contains(&row.footer_bytes),
            "invalid_footer_size"
        );
        add_footer_bytes(&mut total_footer_bytes, row.footer_bytes)?;
        ensure!(
            row.path == fact_name(&row.binding),
            "invalid_export_fact_path"
        );
    }
    let expected: BTreeSet<_> = receipt.facts.iter().map(|f| f.path.clone()).collect();
    ensure!(
        expected.len() == receipt.facts.len(),
        "duplicate_export_fact"
    );
    directory_files(output, &expected)?;
    let mut footers = Vec::new();
    let mut previous = 0;
    let mut bank_footer_sha: Option<String> = None;
    for row in &receipt.facts {
        ensure!(
            receipt.execution.matches(&row.binding)
                && row.path == fact_name(&row.binding)
                && row.binding.source_event_sequence > previous,
            "export_fact_binding_mismatch"
        );
        previous = row.binding.source_event_sequence;
        let bytes = bounded_file(&output.join(&row.path), codec::MAX_FACT_BYTES)?;
        ensure!(
            bytes.len() == row.bytes && v2::digest(&bytes) == row.sha256,
            "export_fact_digest_mismatch"
        );
        let fact = codec::decode_bound(&bytes, &row.binding)?;
        ensure!(
            fact.footer_wire().len() == row.footer_bytes
                && v2::digest(fact.footer_wire()) == row.footer_sha256,
            "export_footer_digest_mismatch"
        );
        ensure!(
            bank_footer_sha
                .as_ref()
                .is_none_or(|old| old == &row.footer_sha256),
            "conflicting_bank_footer"
        );
        bank_footer_sha = Some(row.footer_sha256.clone());
        footers.push(serde_json::to_value(fact.footer()?)?);
    }
    Ok(ExportInspection {
        receipt,
        local_hashes_consistent: true,
        provenance_authenticated: false,
        footers,
    })
}

/// Inspect one bounded fact. Callers that require trusted provenance must use
/// decode_bound with their independent expected binding instead of this view.
pub fn inspect_fact(path: &Path) -> Result<serde_json::Value> {
    let bytes = bounded_file(path, codec::MAX_FACT_BYTES)?;
    let fact = codec::decode(&bytes)?;
    Ok(
        serde_json::json!({"profile":codec::PROFILE,"sha256":v2::digest(&bytes),
        "bytes":bytes.len(),"binding":fact.binding,"footer":fact.footer()?,
        "provenance_authenticated":false,"no_source_retirement_authority":true}),
    )
}

#[cfg(test)]
#[path = "consensus_tests.rs"]
mod tests;
