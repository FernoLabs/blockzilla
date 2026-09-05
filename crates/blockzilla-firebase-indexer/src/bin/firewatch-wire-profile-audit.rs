//! Full-generation, fail-closed Archive V2 wire-profile attestation.
//!
//! This tool is deliberately separate from the live controller. It reads one immutable trusted
//! local generation, validates the complete archive structure, runs the SDK's borrowed dual-profile
//! validator over every typed message, and publishes one exact no-clobber profile attestation.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions as FsOpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
#[cfg(test)]
use blockzilla_firebase_indexer::firewatch_wire_profile_attestation::validate_receipt_source_recovery_evidence;
use blockzilla_firebase_indexer::{
    firewatch_wire_profile_attestation::{
        FullGenerationAuditDecisionV3 as GenerationProfileDecision, FullGenerationAuditEvidenceV3,
        WIRE_PROFILE_ATTESTATION_KIND, WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
        WIRE_PROFILE_AUDIT_ALGORITHM, WIRE_PROFILE_AUDITED_PROFILES, WireProfileAttestation,
        encode_full_generation_audit_evidence_v3, encode_receipt_source_recovery_evidence_v3,
        validate_wire_profile_attestation_structure,
    },
    format::RegistryFileIdentity,
};
#[cfg(test)]
use blockzilla_format::ARCHIVE_V2_TX_FLAG_MESSAGE_V0;
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
};
#[cfg(test)]
use blockzilla_read_sdk::WireProfileAuditOutcome;
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveV2MetadataWireProfile, ArchiveV2WireProfile, Error as ArchiveReaderError,
    HashVerification, OpenOptions, PinnedLocalRangeSource, audit_full_generation_wire_profile,
    manifest::TrustedGenerationIdentity, validate_pinned_local_registry_index_mapping,
    wire_profile_marker, wire_profile_marker_bytes,
};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

const SELECTED_PROFILE_DECODE_REJECTED_EXIT_CODE: i32 = 20;
const TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE: i32 = 21;
const DIRECT_GENERATION_DOMAIN: &[u8] = b"blockzilla.firewatch.direct-generation.v1\0";
const MAX_ATTESTATION_BYTES: usize = 64 * 1024;
const MAX_RECEIPT_BYTES: usize = 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const DIRECT_SEMANTIC_FILES: [&str; 6] = [
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
];

#[derive(Debug, Parser)]
#[command(
    name = "firewatch-wire-profile-audit",
    about = "Audit one complete Archive V2 generation and attest its exact wire profile"
)]
struct Args {
    #[arg(long)]
    archive: PathBuf,
    #[arg(long)]
    epoch: u64,
    #[arg(long, value_parser = ["first_seen", "usage_sorted"])]
    registry_order: String,
    /// Identity authority used by this generation. This must match the controller candidate mode.
    #[arg(long, value_enum)]
    generation_kind: GenerationKind,
    /// Exact no-follow registry-reprocess receipt for either receipt generation kind.
    #[arg(long)]
    registry_receipt: Option<PathBuf>,
    /// Exact content identity calculated by the Firewatch controller.
    #[arg(long)]
    content_generation_sha256: String,
    /// Candidate generation profile. The selected grammar must decode every
    /// message, and the full-generation audit must make the choice unambiguous.
    #[arg(long)]
    wire_profile: ArchiveV2WireProfile,
    /// Existing controller-owned wire-profile-attestations directory.
    #[arg(long)]
    attestation_root: PathBuf,
    #[arg(long, default_value_t = 432_000)]
    slots_per_epoch: u64,
    #[arg(long, default_value_t = MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum GenerationKind {
    DirectFileIdentityV1,
    RegistryReceiptSourceFilesV1,
    RegistryReceiptTargetFilesV1,
}

impl GenerationKind {
    fn stable_id(self) -> &'static str {
        match self {
            Self::DirectFileIdentityV1 => "direct-file-identity-v1",
            Self::RegistryReceiptSourceFilesV1 => "registry-receipt-source-files-v1",
            Self::RegistryReceiptTargetFilesV1 => "registry-receipt-target-files-v1",
        }
    }

    fn is_receipt(self) -> bool {
        !matches!(self, Self::DirectFileIdentityV1)
    }
}

type FileIdentity = RegistryFileIdentity;

#[derive(Debug, Clone, PartialEq, Eq)]
struct GenerationCapture {
    content_generation_sha256: String,
    selected_archive_files: BTreeMap<String, FileIdentity>,
    all_bound_files: BTreeMap<String, FileIdentity>,
    profile_provenance: Option<ProfileProvenance>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RegistryReceipt {
    version: u32,
    algorithm: String,
    epoch: u64,
    source_dir: String,
    target_dir: String,
    source_generation_sha256: String,
    target_generation_sha256: String,
    source_files: BTreeMap<String, RegistryFileBinding>,
    target_files: BTreeMap<String, RegistryFileBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    wire_profile: Option<ArchiveV2WireProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct RegistryFileBinding {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Default)]
struct AuditCounts {
    blocks: u64,
    messages: u64,
    raw_transaction_fallbacks: u64,
    raw_metadata_fallbacks: u64,
    alternate_failures: u64,
    both_equivalent: u64,
    both_divergent: u64,
}

impl AuditCounts {
    #[cfg(test)]
    fn record_profile_outcome(&mut self, outcome: WireProfileAuditOutcome) -> Result<()> {
        self.messages = checked_add(self.messages, 1, "message count")?;
        match outcome {
            WireProfileAuditOutcome::SelectedOnly => {
                self.alternate_failures =
                    checked_add(self.alternate_failures, 1, "alternate failures")?;
            }
            WireProfileAuditOutcome::BothSemanticallyEquivalent => {
                self.both_equivalent =
                    checked_add(self.both_equivalent, 1, "equivalent-message count")?;
            }
            WireProfileAuditOutcome::BothSemanticallyDivergent => {
                self.both_divergent =
                    checked_add(self.both_divergent, 1, "divergent-message count")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProfileProvenance {
    ProfileBoundReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireProfileMarkerCapture {
    profile: ArchiveV2WireProfile,
    name: String,
    identity: FileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiptFileVerification {
    HashContents,
    IdentityOnly,
}

#[derive(Debug, thiserror::Error)]
#[error("wire-profile audit cannot safely select this profile: {0}")]
struct TerminalProfileAuditRejected(String);

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(error_exit_code(&error));
    }
}

fn error_exit_code(error: &anyhow::Error) -> i32 {
    if matches!(
        error.downcast_ref::<ArchiveReaderError>(),
        Some(
            ArchiveReaderError::SelectedWireProfileDecodeRejected { .. }
                | ArchiveReaderError::SelectedWireProfileSemanticRejected { .. }
        )
    ) {
        SELECTED_PROFILE_DECODE_REJECTED_EXIT_CODE
    } else if error
        .downcast_ref::<TerminalProfileAuditRejected>()
        .is_some()
    {
        TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE
    } else {
        1
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    let archive = fs::canonicalize(&args.archive)
        .with_context(|| format!("canonicalize archive {}", args.archive.display()))?;
    ensure!(
        archive == args.archive,
        "--archive must already be canonical"
    );
    let attestation_root = fs::canonicalize(&args.attestation_root).with_context(|| {
        format!(
            "canonicalize attestation root {}",
            args.attestation_root.display()
        )
    })?;
    ensure!(
        attestation_root == args.attestation_root,
        "--attestation-root must already be canonical"
    );
    ensure_real_directory(&archive, "archive")?;
    ensure_real_directory(&attestation_root, "attestation root")?;

    let before =
        capture_generation_for_audit(&args, &archive, ReceiptFileVerification::HashContents)?;
    let before_generation = before.content_generation_sha256.clone();
    ensure!(
        before_generation == args.content_generation_sha256,
        "archive content generation is {before_generation}, not the required {}",
        args.content_generation_sha256
    );

    let source = PinnedLocalRangeSource::new(&archive);
    bind_selected_capture_to_pinned_source(&source, &before)
        .context("bind attestation evidence to the exact pinned audit descriptors")?;
    let reader = ArchiveReader::open_trusted_with_metadata_profile(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".into(),
            epoch: args.epoch,
            generation_id: before_generation.clone(),
            slots_per_epoch: args.slots_per_epoch,
            wire_profile: args.wire_profile,
        },
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .context("validate the complete generation structure")?;
    ensure!(
        reader.index().rows.len() as u64 == reader.metadata_footer().blocks,
        "validated block count differs from the metadata footer"
    );
    let registry_validation = validate_canonical_registry_evidence(&source, &reader, &before)?;
    let footer = reader.metadata_footer();
    ensure!(
        footer.decode_errors.is_empty(),
        "generation footer reports decode errors; its typed message grammar cannot be attested"
    );
    if args.generation_kind == GenerationKind::RegistryReceiptSourceFilesV1 {
        if footer.tx_raw_fallbacks != 0 || footer.metadata_raw_fallbacks != 0 {
            return Err(TerminalProfileAuditRejected(
                "receipt-source recovery contains raw transaction or metadata fallbacks".into(),
            )
            .into());
        }
    }

    let counts = audit_all_messages(&reader, args.wire_profile, args.max_message_bytes)?;
    ensure!(
        counts.raw_transaction_fallbacks == footer.tx_raw_fallbacks,
        "audited raw transaction fallback count differs from the generation footer"
    );
    ensure!(
        counts.raw_metadata_fallbacks == footer.metadata_raw_fallbacks,
        "audited raw metadata fallback count differs from the generation footer"
    );
    if args.generation_kind == GenerationKind::RegistryReceiptSourceFilesV1
        && (counts.raw_transaction_fallbacks != 0 || counts.raw_metadata_fallbacks != 0)
    {
        return Err(TerminalProfileAuditRejected(
            "receipt-source recovery contains audited raw transaction or metadata fallbacks".into(),
        )
        .into());
    }
    source
        .verify_unchanged()
        .context("verify every pinned audit input remained unchanged")?;
    bind_selected_capture_to_pinned_source(&source, &before)
        .context("recheck attestation evidence against the pinned audit descriptors")?;
    // Full receipt hashes were checked before the scan. Exact identity checks
    // detect later replacement or mutation without a second full-file read.
    let after =
        capture_generation_for_audit(&args, &archive, ReceiptFileVerification::IdentityOnly)?;
    ensure!(
        before == after,
        "archive input identities changed during audit"
    );
    let after_generation = after.content_generation_sha256.clone();
    ensure!(
        before_generation == after_generation,
        "archive content generation changed during audit"
    );

    let (decision, evidence) =
        produce_audit_evidence(args.generation_kind, &counts, before.profile_provenance)?;
    if decision == GenerationProfileDecision::AllSemanticallyEquivalent
        && args.wire_profile != ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
    {
        return Err(TerminalProfileAuditRejected(
            "an all-equivalent generation must use the canonical Post profile".into(),
        )
        .into());
    }
    let attestation = WireProfileAttestation {
        schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
        kind: WIRE_PROFILE_ATTESTATION_KIND.into(),
        audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
        audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
        cluster_id: "mainnet-beta".into(),
        epoch: args.epoch,
        archive,
        registry_order: args.registry_order.clone(),
        generation_kind: args.generation_kind.stable_id().into(),
        content_generation_sha256: before_generation.clone(),
        archive_files: before.selected_archive_files,
        wire_profile: args.wire_profile,
        evidence,
        attested_unix_secs: unix_now(),
    };
    validate_wire_profile_attestation_structure(&attestation)?;
    let path = attestation_root.join(format!("epoch-{}-{}.json", args.epoch, before_generation));
    publish_json_no_replace(&path, &attestation)?;
    println!("attestation={}", path.display());
    println!("wire_profile={}", args.wire_profile);
    println!("blocks={}", counts.blocks);
    println!("messages={}", counts.messages);
    println!(
        "raw_transaction_fallbacks={}",
        counts.raw_transaction_fallbacks
    );
    println!("alternate_profile_failures={}", counts.alternate_failures);
    println!("both_semantically_equivalent={}", counts.both_equivalent);
    println!("both_semantically_divergent={}", counts.both_divergent);
    println!("decision_basis={}", decision.stable_id());
    println!("registry_entries={}", registry_validation.entries);
    println!(
        "registry_index_bytes={}",
        registry_validation.registry_index_bytes
    );
    Ok(())
}

fn bind_selected_capture_to_pinned_source(
    source: &PinnedLocalRangeSource,
    capture: &GenerationCapture,
) -> Result<()> {
    ensure!(
        !capture.selected_archive_files.is_empty(),
        "selected attestation file binding is empty"
    );
    for (name, expected) in &capture.selected_archive_files {
        let file = source
            .open_file(name)
            .with_context(|| format!("pin selected attestation input {name}"))?;
        let actual = file_identity(
            &file
                .metadata()
                .with_context(|| format!("stat pinned selected attestation input {name}"))?,
        );
        ensure!(
            &actual == expected,
            "selected attestation input {name} differs from the descriptor used by the audit"
        );
    }
    Ok(())
}

fn validate_canonical_registry_evidence(
    source: &PinnedLocalRangeSource,
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    capture: &GenerationCapture,
) -> Result<blockzilla_read_sdk::LocalRegistryIndexValidation> {
    let registry = capture
        .selected_archive_files
        .get(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
        .context("wire-profile evidence does not bind registry.bin")?;
    let registry_index = capture
        .selected_archive_files
        .get(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)
        .context("wire-profile evidence does not bind registry.mphf")?;
    let validated = validate_pinned_local_registry_index_mapping(source, reader.registry_entries())
        .context("prove canonical registry.bin/registry.mphf mapping")?;
    ensure!(
        registry.size == validated.registry_bytes,
        "wire-profile evidence binds registry.bin size {}, canonical registry scan used {}",
        registry.size,
        validated.registry_bytes
    );
    ensure!(
        registry_index.size == validated.registry_index_bytes,
        "wire-profile evidence binds registry.mphf size {}, canonical registry scan used {}",
        registry_index.size,
        validated.registry_index_bytes
    );
    Ok(validated)
}

fn validate_args(args: &Args) -> Result<()> {
    ensure!(args.archive.is_absolute(), "--archive must be absolute");
    ensure!(
        args.attestation_root.is_absolute(),
        "--attestation-root must be absolute"
    );
    ensure!(
        is_sha256(&args.content_generation_sha256),
        "--content-generation-sha256 must be a lowercase SHA-256"
    );
    ensure!(
        args.slots_per_epoch > 0,
        "--slots-per-epoch must be positive"
    );
    ensure!(
        (1..=MAX_MESSAGE_BYTES).contains(&args.max_message_bytes),
        "--max-message-bytes must be between 1 and {MAX_MESSAGE_BYTES}"
    );
    match args.generation_kind {
        GenerationKind::DirectFileIdentityV1 => ensure!(
            args.registry_receipt.is_none(),
            "--registry-receipt is not valid for a direct-file identity audit"
        ),
        GenerationKind::RegistryReceiptSourceFilesV1 => {
            ensure!(
                args.registry_order == "first_seen",
                "a receipt-source audit requires --registry-order first_seen"
            );
            validate_registry_receipt_arg(args)?;
        }
        GenerationKind::RegistryReceiptTargetFilesV1 => {
            ensure!(
                args.registry_order == "usage_sorted",
                "a receipt-target audit requires --registry-order usage_sorted"
            );
            validate_registry_receipt_arg(args)?;
        }
    }
    Ok(())
}

fn validate_registry_receipt_arg(args: &Args) -> Result<()> {
    let path = args
        .registry_receipt
        .as_ref()
        .context("a receipt generation audit requires --registry-receipt")?;
    ensure!(path.is_absolute(), "--registry-receipt must be absolute");
    Ok(())
}

fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} is not a real directory"
    );
    Ok(())
}

fn audit_all_messages(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    _selected_profile: ArchiveV2WireProfile,
    max_message_bytes: usize,
) -> Result<AuditCounts> {
    let audited = audit_full_generation_wire_profile(reader, max_message_bytes)
        .map_err(classify_full_generation_audit_error)?;
    Ok(AuditCounts {
        blocks: audited.blocks,
        messages: audited.typed_messages,
        raw_transaction_fallbacks: audited.raw_transaction_fallbacks,
        raw_metadata_fallbacks: audited.raw_metadata_fallbacks,
        alternate_failures: audited.selected_only,
        both_equivalent: audited.both_semantically_equivalent,
        both_divergent: audited.both_semantically_divergent,
    })
}

fn classify_full_generation_audit_error(error: ArchiveReaderError) -> anyhow::Error {
    match error {
        error @ (ArchiveReaderError::SelectedWireProfileDecodeRejected { .. }
        | ArchiveReaderError::SelectedWireProfileSemanticRejected { .. }) => {
            anyhow::Error::new(error)
        }
        error => anyhow::Error::new(TerminalProfileAuditRejected(error.to_string())),
    }
}

#[cfg(test)]
fn validate_selected_message_envelope(
    slot: u64,
    tx_index: u32,
    row_flags: u32,
    row_signature_count: u8,
    projected_is_v0: bool,
    projected_required_signatures: u8,
) -> Result<()> {
    if projected_is_v0 != (row_flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0) {
        return Err(TerminalProfileAuditRejected(format!(
            "slot {slot} transaction {tx_index} message version disagrees with its transaction-row flags"
        ))
        .into());
    }
    if projected_required_signatures != row_signature_count {
        return Err(TerminalProfileAuditRejected(format!(
            "slot {slot} transaction {tx_index} message requires {projected_required_signatures} signatures but its transaction row declares {row_signature_count}"
        ))
        .into());
    }
    Ok(())
}

fn select_generation_profile(
    counts: &AuditCounts,
    provenance: Option<ProfileProvenance>,
) -> Result<GenerationProfileDecision> {
    ensure!(counts.messages > 0, "generation contains no typed messages");
    let classified = checked_add(
        checked_add(
            counts.alternate_failures,
            counts.both_equivalent,
            "classified message count",
        )?,
        counts.both_divergent,
        "classified message count",
    )?;
    ensure!(
        classified == counts.messages,
        "wire-profile audit did not classify every selected-profile message"
    );
    if let Some(provenance) = provenance {
        return Ok(match provenance {
            ProfileProvenance::ProfileBoundReceipt => {
                GenerationProfileDecision::ProfileBoundReceipt
            }
        });
    }
    if counts.alternate_failures > 0 {
        // The selected grammar decoded every message, while the alternate did
        // not decode the generation. Dual-valid divergent messages do not
        // make this generation ambiguous.
        return Ok(GenerationProfileDecision::UniqueFullGenerationDecode);
    }
    if counts.both_divergent != 0 {
        return Err(TerminalProfileAuditRejected(format!(
            "both wire profiles decode the full generation but differ semantically on {} messages; independent producer provenance is required",
            counts.both_divergent
        ))
        .into());
    }
    Ok(GenerationProfileDecision::AllSemanticallyEquivalent)
}

fn produce_audit_evidence(
    generation_kind: GenerationKind,
    counts: &AuditCounts,
    provenance: Option<ProfileProvenance>,
) -> Result<(GenerationProfileDecision, String)> {
    let receipt_source = generation_kind == GenerationKind::RegistryReceiptSourceFilesV1;
    let decision = if receipt_source {
        ensure!(
            provenance.is_none(),
            "receipt-source recovery cannot use producer provenance"
        );
        select_generation_profile(counts, None)?
    } else {
        select_generation_profile(counts, provenance)?
    };
    let evidence = if receipt_source {
        encode_receipt_source_recovery_evidence_v3(
            counts.blocks,
            counts.messages,
            counts.raw_transaction_fallbacks,
            counts.alternate_failures,
            counts.both_equivalent,
            counts.both_divergent,
        )?
    } else {
        encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: generation_kind.stable_id().into(),
            blocks: counts.blocks,
            messages: counts.messages,
            raw_transaction_fallbacks: counts.raw_transaction_fallbacks,
            alternate_profile_failures: counts.alternate_failures,
            both_semantically_equivalent: counts.both_equivalent,
            both_semantically_divergent: counts.both_divergent,
            decision,
        })?
    };
    Ok((decision, evidence))
}

fn checked_add(value: u64, increment: u64, label: &'static str) -> Result<u64> {
    value
        .checked_add(increment)
        .with_context(|| format!("{label} overflow"))
}

fn capture_generation_for_audit(
    args: &Args,
    archive: &Path,
    verification: ReceiptFileVerification,
) -> Result<GenerationCapture> {
    match args.generation_kind {
        GenerationKind::DirectFileIdentityV1 => {
            let (files, profile_provenance) =
                capture_generation(archive, args.epoch, &args.registry_order, args.wire_profile)?;
            Ok(GenerationCapture {
                content_generation_sha256: direct_generation_digest(
                    args.epoch,
                    &args.registry_order,
                    &files,
                ),
                selected_archive_files: files.clone(),
                all_bound_files: files,
                profile_provenance,
            })
        }
        GenerationKind::RegistryReceiptSourceFilesV1
        | GenerationKind::RegistryReceiptTargetFilesV1 => capture_registry_receipt_generation(
            archive,
            args.epoch,
            &args.registry_order,
            args.generation_kind,
            args.registry_receipt
                .as_deref()
                .context("receipt audit has no receipt path")?,
            args.wire_profile,
            verification,
        ),
    }
}

fn capture_registry_receipt_generation(
    archive: &Path,
    epoch: u64,
    registry_order: &str,
    generation_kind: GenerationKind,
    receipt_path: &Path,
    requested_profile: ArchiveV2WireProfile,
    verification: ReceiptFileVerification,
) -> Result<GenerationCapture> {
    ensure!(
        generation_kind.is_receipt(),
        "registry receipt capture received a direct generation kind"
    );
    ensure!(
        fs::canonicalize(receipt_path)? == receipt_path,
        "registry receipt path is not canonical"
    );
    let (receipt, receipt_identity): (RegistryReceipt, FileIdentity) =
        read_bounded_pinned_json(receipt_path, MAX_RECEIPT_BYTES)?;
    ensure!(
        receipt.epoch == epoch
            && matches!(
                (receipt.version, receipt.algorithm.as_str()),
                (
                    1,
                    "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1"
                ) | (
                    2,
                    "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v2"
                ) | (
                    3,
                    "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3"
                )
            ),
        "registry receipt provenance is invalid"
    );
    let receipt_profile_bound = validate_receipt_wire_profile(&receipt, requested_profile)?;
    let source = PathBuf::from(&receipt.source_dir);
    let target = PathBuf::from(&receipt.target_dir);
    ensure!(
        source.is_absolute()
            && target.is_absolute()
            && fs::canonicalize(&source)? == source
            && fs::canonicalize(&target)? == target,
        "registry receipt archive paths are not canonical"
    );
    ensure_real_directory(&source, "registry receipt source")?;
    ensure_real_directory(&target, "registry receipt target")?;
    ensure!(
        receipt_path == target.join("archive-v2-registry-reprocess.receipt.json"),
        "registry receipt is not at the exact target generation path"
    );
    ensure!(
        registry_generation_digest(&receipt.source_files) == receipt.source_generation_sha256
            && registry_generation_digest(&receipt.target_files)
                == receipt.target_generation_sha256,
        "registry receipt generation digest is invalid"
    );

    let source_marker = capture_wire_profile_marker(
        &source,
        requested_profile,
        receipt.wire_profile,
        "registry receipt source",
    )?;
    let target_marker = capture_wire_profile_marker(
        &target,
        requested_profile,
        receipt.wire_profile,
        "registry receipt target",
    )?;
    if let Some(marker) = &source_marker {
        ensure!(
            receipt.source_files.contains_key(&marker.name),
            "registry receipt source contains an unbound wire-profile marker {}",
            marker.name
        );
    }
    if let Some(marker) = &target_marker {
        ensure!(
            receipt.target_files.contains_key(&marker.name),
            "registry receipt target contains an unbound wire-profile marker {}",
            marker.name
        );
    }

    let source_verification = if verification == ReceiptFileVerification::HashContents
        && generation_kind == GenerationKind::RegistryReceiptSourceFilesV1
    {
        ReceiptFileVerification::HashContents
    } else {
        ReceiptFileVerification::IdentityOnly
    };
    let target_verification = if verification == ReceiptFileVerification::HashContents
        && generation_kind == GenerationKind::RegistryReceiptTargetFilesV1
    {
        ReceiptFileVerification::HashContents
    } else {
        ReceiptFileVerification::IdentityOnly
    };
    let mut source_files =
        capture_receipt_files(&source, &receipt.source_files, source_verification)?;
    let mut target_files =
        capture_receipt_files(&target, &receipt.target_files, target_verification)?;
    bind_marker_identity(&mut source_files, source_marker)?;
    bind_marker_identity(&mut target_files, target_marker)?;
    let (expected_archive, expected_order, generation, selected_archive_files) =
        match generation_kind {
            GenerationKind::RegistryReceiptSourceFilesV1 => (
                &source,
                "first_seen",
                receipt.source_generation_sha256,
                source_files.clone(),
            ),
            GenerationKind::RegistryReceiptTargetFilesV1 => (
                &target,
                "usage_sorted",
                receipt.target_generation_sha256,
                target_files.clone(),
            ),
            GenerationKind::DirectFileIdentityV1 => unreachable!(),
        };
    ensure!(
        archive == expected_archive && registry_order == expected_order,
        "registry receipt side differs from the requested archive or registry order"
    );
    let mut all_bound_files = BTreeMap::from([("receipt".into(), receipt_identity)]);
    for (name, identity) in source_files {
        all_bound_files.insert(format!("source/{name}"), identity);
    }
    for (name, identity) in target_files {
        all_bound_files.insert(format!("target/{name}"), identity);
    }
    Ok(GenerationCapture {
        content_generation_sha256: generation,
        selected_archive_files,
        all_bound_files,
        // Receipt generations can use a marker only when the receipt itself
        // also binds the profile. A marker in a profile-neutral receipt is an
        // exact input, but it is not independent producer provenance.
        // A receipt-source recovery always needs an independent neutral
        // dual-profile decision. The profile-bound v3 receipt is still fully
        // validated above, but it cannot replace that recovery proof.
        profile_provenance: if receipt_profile_bound
            && generation_kind != GenerationKind::RegistryReceiptSourceFilesV1
        {
            Some(ProfileProvenance::ProfileBoundReceipt)
        } else {
            None
        },
    })
}

fn validate_receipt_wire_profile(
    receipt: &RegistryReceipt,
    requested_profile: ArchiveV2WireProfile,
) -> Result<bool> {
    validate_receipt_marker_bindings(&receipt.source_files, "source")?;
    validate_receipt_marker_bindings(&receipt.target_files, "target")?;
    match receipt.version {
        1 | 2 => {
            ensure!(
                receipt.wire_profile.is_none(),
                "profile-neutral v{} registry receipt must omit wire_profile",
                receipt.version
            );
            Ok(false)
        }
        3 => {
            let Some(receipt_profile) = receipt.wire_profile else {
                // The deployed staged-access v3 cohort predates wire-profile
                // provenance. It remains valid as an exact file-identity
                // authority, but it cannot authorize either grammar. Require
                // the old marker-free shape so a damaged profile-bound receipt
                // cannot silently fall back to this migration path.
                for (files, side) in [
                    (&receipt.source_files, "source"),
                    (&receipt.target_files, "target"),
                ] {
                    ensure!(
                        wire_profiles()
                            .into_iter()
                            .all(|profile| !files.contains_key(&wire_profile_marker(profile).name)),
                        "profile-neutral legacy v3 registry receipt {side} binds a wire-profile marker"
                    );
                }
                return Ok(false);
            };
            ensure!(
                receipt_profile == requested_profile,
                "registry receipt wire profile {receipt_profile} differs from requested profile {requested_profile}"
            );
            validate_v3_receipt_profile_marker(
                &receipt.source_files,
                requested_profile,
                false,
                "source",
            )?;
            validate_v3_receipt_profile_marker(
                &receipt.target_files,
                requested_profile,
                true,
                "target",
            )?;
            Ok(true)
        }
        version => anyhow::bail!("unsupported registry receipt version {version}"),
    }
}

fn validate_v3_receipt_profile_marker(
    files: &BTreeMap<String, RegistryFileBinding>,
    selected_profile: ArchiveV2WireProfile,
    required: bool,
    side: &str,
) -> Result<()> {
    let selected = wire_profile_marker(selected_profile);
    let selected_bytes = wire_profile_marker_bytes(selected_profile);
    ensure!(
        selected.size == selected_bytes.len() as u64
            && selected.sha256 == hex_digest(Sha256::digest(selected_bytes)),
        "SDK Archive V2 wire-profile marker definition is inconsistent"
    );
    let opposite_profile = match selected_profile {
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        }
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        }
    };
    let opposite = wire_profile_marker(opposite_profile);
    ensure!(
        !files.contains_key(&opposite.name),
        "profile-bound v3 registry receipt {side} binds the opposite wire-profile marker {}",
        opposite.name
    );
    let Some(binding) = files.get(&selected.name) else {
        ensure!(
            !required,
            "profile-bound v3 registry receipt {side} omits selected wire-profile marker {}",
            selected.name
        );
        return Ok(());
    };
    ensure!(
        binding.bytes == selected.size && binding.sha256 == selected.sha256,
        "profile-bound v3 registry receipt {side} has a malformed selected wire-profile marker binding {}",
        selected.name
    );
    Ok(())
}

fn validate_receipt_marker_bindings(
    files: &BTreeMap<String, RegistryFileBinding>,
    side: &str,
) -> Result<()> {
    let mut bound_profile = None;
    for profile in wire_profiles() {
        let marker = wire_profile_marker(profile);
        let Some(binding) = files.get(&marker.name) else {
            continue;
        };
        ensure!(
            bound_profile.is_none(),
            "registry receipt {side} binds conflicting Archive V2 wire-profile markers"
        );
        ensure!(
            binding.bytes == marker.size && binding.sha256 == marker.sha256,
            "registry receipt {side} has a malformed Archive V2 wire-profile marker binding {}",
            marker.name
        );
        bound_profile = Some(profile);
    }
    Ok(())
}

fn capture_wire_profile_marker(
    directory: &Path,
    requested_profile: ArchiveV2WireProfile,
    receipt_profile: Option<ArchiveV2WireProfile>,
    label: &str,
) -> Result<Option<WireProfileMarkerCapture>> {
    let mut selected = None;
    for profile in wire_profiles() {
        let marker = wire_profile_marker(profile);
        let path = directory.join(&marker.name);
        match fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect {label} marker {}", path.display()));
            }
        }
        ensure!(
            selected.is_none(),
            "{label} contains conflicting Archive V2 wire-profile markers"
        );
        let expected = wire_profile_marker_bytes(profile);
        ensure!(
            marker.size == expected.len() as u64
                && marker.sha256 == hex_digest(Sha256::digest(expected)),
            "SDK Archive V2 wire-profile marker definition is inconsistent"
        );
        let (bytes, identity) = read_bounded_pinned_bytes(&path, expected.len())
            .with_context(|| format!("read {label} marker {}", marker.name))?;
        ensure!(
            bytes == expected,
            "{label} has malformed Archive V2 wire-profile marker bytes {}",
            marker.name
        );
        ensure!(
            identity.size == marker.size,
            "{label} has a malformed Archive V2 wire-profile marker size {}",
            marker.name
        );
        selected = Some(WireProfileMarkerCapture {
            profile,
            name: marker.name,
            identity,
        });
    }
    if let Some(marker) = &selected {
        ensure!(
            marker.profile == requested_profile,
            "{label} marker profile {} differs from requested profile {requested_profile}",
            marker.profile
        );
        if let Some(receipt_profile) = receipt_profile {
            ensure!(
                marker.profile == receipt_profile,
                "{label} marker profile {} differs from registry receipt profile {receipt_profile}",
                marker.profile
            );
        }
    }
    Ok(selected)
}

fn bind_marker_identity(
    files: &mut BTreeMap<String, FileIdentity>,
    marker: Option<WireProfileMarkerCapture>,
) -> Result<()> {
    let Some(marker) = marker else {
        return Ok(());
    };
    if let Some(existing) = files.insert(marker.name.clone(), marker.identity.clone()) {
        ensure!(
            existing == marker.identity,
            "wire-profile marker identity differs from its receipt-bound identity: {}",
            marker.name
        );
    }
    Ok(())
}

fn wire_profiles() -> [ArchiveV2WireProfile; 2] {
    WIRE_PROFILE_AUDITED_PROFILES
}

fn capture_receipt_files(
    directory: &Path,
    files: &BTreeMap<String, RegistryFileBinding>,
    verification: ReceiptFileVerification,
) -> Result<BTreeMap<String, FileIdentity>> {
    ensure!(!files.is_empty(), "registry receipt file map is empty");
    let mut identities = BTreeMap::new();
    for (name, binding) in files {
        ensure!(
            !name.is_empty()
                && Path::new(name).components().count() == 1
                && name != "."
                && name != ".."
                && is_sha256(&binding.sha256),
            "registry receipt has an invalid file binding"
        );
        let identity = match verification {
            ReceiptFileVerification::HashContents => {
                let (identity, sha256) = capture_file_identity_and_sha256(&directory.join(name))?;
                ensure!(
                    identity.size == binding.bytes && sha256 == binding.sha256,
                    "registry receipt hash or size differs for {name}"
                );
                identity
            }
            ReceiptFileVerification::IdentityOnly => {
                let identity = capture_file_identity(&directory.join(name))?;
                ensure!(
                    identity.size == binding.bytes,
                    "registry receipt size differs for {name}"
                );
                identity
            }
        };
        identities.insert(name.clone(), identity);
    }
    Ok(identities)
}

fn registry_generation_digest(files: &BTreeMap<String, RegistryFileBinding>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla.registry-reprocess.generation.v1");
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, binding) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    hex_digest(hasher.finalize())
}

fn read_bounded_pinned_json<T: DeserializeOwned>(
    path: &Path,
    max_bytes: usize,
) -> Result<(T, FileIdentity)> {
    let (bytes, identity) = read_bounded_pinned_bytes(path, max_bytes)?;
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode JSON {}", path.display()))?;
    Ok((value, identity))
}

fn read_bounded_pinned_bytes(path: &Path, max_bytes: usize) -> Result<(Vec<u8>, FileIdentity)> {
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.file_type().is_file()
            && !before.file_type().is_symlink()
            && before.len() <= max_bytes as u64,
        "bounded input is not a safe regular file"
    );
    let mut file = FsOpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&before, &opened) && same_version(&before, &opened),
        "bounded input changed before open"
    );
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= max_bytes, "bounded input is too large");
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        bytes.len() as u64 == after.len()
            && same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after),
        "bounded input changed while reading"
    );
    Ok((bytes, file_identity(&after)))
}

fn capture_file_identity_and_sha256(path: &Path) -> Result<(FileIdentity, String)> {
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "receipt-bound input is not a real regular file: {}",
        path.display()
    );
    let mut file = FsOpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&before, &opened) && same_version(&before, &opened),
        "receipt-bound input changed before hashing"
    );
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after),
        "receipt-bound input changed while hashing"
    );
    Ok((file_identity(&after), hex_digest(hasher.finalize())))
}

fn file_identity(metadata: &fs::Metadata) -> FileIdentity {
    FileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

fn capture_generation(
    archive: &Path,
    epoch: u64,
    registry_order: &str,
    requested_profile: ArchiveV2WireProfile,
) -> Result<(BTreeMap<String, FileIdentity>, Option<ProfileProvenance>)> {
    let mut files = BTreeMap::new();
    for name in DIRECT_SEMANTIC_FILES {
        let identity = capture_file_identity(&archive.join(name))?;
        if name != ARCHIVE_V2_SIGNATURES_FILE {
            ensure!(identity.size > 0, "archive input {name} is empty");
        }
        files.insert(name.to_owned(), identity);
    }
    let marker = capture_wire_profile_marker(
        archive,
        requested_profile,
        None,
        "direct archive generation",
    )?;
    bind_marker_identity(&mut files, marker)?;
    let generation = direct_generation_digest(epoch, registry_order, &files);
    ensure!(
        is_sha256(&generation),
        "captured generation digest is invalid"
    );
    // A standalone direct marker is an exact input and a consistency check,
    // but it is not independent producer provenance. Otherwise a retrofit
    // marker could authorize the profile that it asserts.
    Ok((files, None))
}

fn capture_file_identity(path: &Path) -> Result<FileIdentity> {
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect archive input {}", path.display()))?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "archive input is not a real regular file: {}",
        path.display()
    );
    let file = FsOpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open archive input {}", path.display()))?;
    let opened = file.metadata()?;
    let after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&before, &opened)
            && same_version(&before, &opened)
            && same_file(&opened, &after)
            && same_version(&opened, &after),
        "archive input changed while its identity was captured: {}",
        path.display()
    );
    Ok(FileIdentity {
        size: opened.len(),
        device: opened.dev(),
        inode: opened.ino(),
        modified_seconds: opened.mtime(),
        modified_nanoseconds: opened.mtime_nsec(),
        changed_seconds: opened.ctime(),
        changed_nanoseconds: opened.ctime_nsec(),
    })
}

fn direct_generation_digest(
    epoch: u64,
    registry_order: &str,
    files: &BTreeMap<String, FileIdentity>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DIRECT_GENERATION_DOMAIN);
    hasher.update(epoch.to_le_bytes());
    hasher.update((registry_order.len() as u64).to_le_bytes());
    hasher.update(registry_order.as_bytes());
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, identity) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(identity.size.to_le_bytes());
        hasher.update(identity.device.to_le_bytes());
        hasher.update(identity.inode.to_le_bytes());
        hasher.update(identity.modified_seconds.to_le_bytes());
        hasher.update(identity.modified_nanoseconds.to_le_bytes());
        hasher.update(identity.changed_seconds.to_le_bytes());
        hasher.update(identity.changed_nanoseconds.to_le_bytes());
    }
    hex_digest(hasher.finalize())
}

fn publish_json_no_replace(path: &Path, value: &impl Serialize) -> Result<()> {
    ensure!(
        !path.exists(),
        "attestation already exists: {}",
        path.display()
    );
    let parent = path.parent().context("attestation has no parent")?;
    ensure_real_directory(parent, "attestation parent")?;
    let bytes = serde_json::to_vec_pretty(value)?;
    ensure!(
        bytes.len() <= MAX_ATTESTATION_BYTES,
        "attestation is too large"
    );
    let name = path.file_name().context("attestation has no filename")?;
    let temp = parent.join(format!(
        ".{}.{}-{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let mut file = FsOpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::hard_link(&temp, path)?;
    File::open(parent)?.sync_all()?;
    fs::remove_file(&temp)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.file_type().is_file()
        && right.file_type().is_file()
}

fn same_version(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(bytes: &[u8]) -> RegistryFileBinding {
        RegistryFileBinding {
            bytes: bytes.len() as u64,
            sha256: hex_digest(Sha256::digest(bytes)),
        }
    }

    #[test]
    fn typed_audit_failures_keep_their_exit_contract_through_context() {
        let decode = classify_full_generation_audit_error(
            ArchiveReaderError::SelectedWireProfileDecodeRejected {
                profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                slot: 1,
                tx_index: 2,
                source: blockzilla_read_sdk::MessageProjectionError::TrailingBytes(1),
            },
        )
        .context("outer context");
        assert_eq!(error_exit_code(&decode), 20);
        let semantic = classify_full_generation_audit_error(
            ArchiveReaderError::SelectedWireProfileSemanticRejected {
                profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                slot: 1,
                tx_index: 2,
                message: "lookup shape differs".into(),
            },
        )
        .context("outer context");
        assert_eq!(error_exit_code(&semantic), 20);
        let terminal = classify_full_generation_audit_error(ArchiveReaderError::WireProfileAudit(
            "ambiguous".into(),
        ))
        .context("outer context");
        assert_eq!(error_exit_code(&terminal), 21);
        assert_eq!(error_exit_code(&anyhow::anyhow!("I/O failure")), 1);
    }

    #[test]
    fn selected_message_envelope_must_match_its_transaction_row() {
        validate_selected_message_envelope(1, 2, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, 1, true, 1)
            .unwrap();
        let version = validate_selected_message_envelope(1, 2, 0, 1, true, 1).unwrap_err();
        assert_eq!(
            error_exit_code(&version),
            TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE
        );
        let signatures = validate_selected_message_envelope(1, 2, 0, 2, false, 1).unwrap_err();
        assert_eq!(
            error_exit_code(&signatures),
            TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE
        );
    }

    fn profile_validation_receipt(profile: ArchiveV2WireProfile) -> RegistryReceipt {
        let marker = wire_profile_marker(profile);
        let marker_binding = binding(wire_profile_marker_bytes(profile));
        RegistryReceipt {
            version: 3,
            algorithm: "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3".into(),
            epoch: 700,
            source_dir: "/source".into(),
            target_dir: "/target".into(),
            source_generation_sha256: "a".repeat(64),
            target_generation_sha256: "b".repeat(64),
            source_files: BTreeMap::from([(marker.name.clone(), marker_binding.clone())]),
            target_files: BTreeMap::from([(marker.name, marker_binding)]),
            wire_profile: Some(profile),
        }
    }

    #[test]
    fn profile_neutral_v1_and_v2_receipts_reject_injected_profile() {
        let profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        for (version, algorithm) in [
            (
                1,
                "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1",
            ),
            (
                2,
                "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v2",
            ),
        ] {
            let mut receipt = profile_validation_receipt(profile);
            receipt.version = version;
            receipt.algorithm = algorithm.into();
            receipt.wire_profile = None;
            assert!(!validate_receipt_wire_profile(&receipt, profile).unwrap());

            receipt.wire_profile = Some(profile);
            assert!(validate_receipt_wire_profile(&receipt, profile).is_err());
        }
    }

    #[test]
    fn marker_free_legacy_v3_receipt_is_identity_only() {
        let profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let mut receipt = profile_validation_receipt(profile);
        receipt.wire_profile = None;
        receipt.source_files.clear();
        receipt.target_files.clear();

        assert!(!validate_receipt_wire_profile(&receipt, profile).unwrap());

        let marker = wire_profile_marker(profile);
        let marker_binding = binding(wire_profile_marker_bytes(profile));
        for source_side in [true, false] {
            let mut damaged_profile_bound_receipt = receipt.clone();
            if source_side {
                damaged_profile_bound_receipt
                    .source_files
                    .insert(marker.name.clone(), marker_binding.clone());
            } else {
                damaged_profile_bound_receipt
                    .target_files
                    .insert(marker.name.clone(), marker_binding.clone());
            }
            assert!(
                validate_receipt_wire_profile(&damaged_profile_bound_receipt, profile).is_err()
            );
        }
    }

    #[test]
    fn profile_bound_v3_receipt_requires_exact_target_marker() {
        let profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let receipt = profile_validation_receipt(profile);
        assert!(validate_receipt_wire_profile(&receipt, profile).unwrap());

        let selected = wire_profile_marker(profile);
        let mut missing = receipt.clone();
        missing.target_files.remove(&selected.name);
        assert!(validate_receipt_wire_profile(&missing, profile).is_err());

        let mut malformed = receipt;
        malformed
            .target_files
            .get_mut(&selected.name)
            .unwrap()
            .bytes += 1;
        assert!(validate_receipt_wire_profile(&malformed, profile).is_err());
    }

    #[test]
    fn profile_bound_v3_receipt_rejects_opposite_marker_on_either_side() {
        let profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let selected = wire_profile_marker(profile);
        let opposite_profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let opposite = wire_profile_marker(opposite_profile);
        let opposite_binding = binding(wire_profile_marker_bytes(opposite_profile));

        let mut target_opposite = profile_validation_receipt(profile);
        target_opposite.target_files.remove(&selected.name);
        target_opposite
            .target_files
            .insert(opposite.name.clone(), opposite_binding.clone());
        assert!(validate_receipt_wire_profile(&target_opposite, profile).is_err());

        let mut source_opposite = profile_validation_receipt(profile);
        source_opposite.source_files.remove(&selected.name);
        source_opposite
            .source_files
            .insert(opposite.name, opposite_binding);
        assert!(validate_receipt_wire_profile(&source_opposite, profile).is_err());
    }

    #[test]
    fn generation_level_unique_selection_allows_dual_divergence() {
        let mut counts = AuditCounts::default();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::BothSemanticallyDivergent)
            .unwrap();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::BothSemanticallyEquivalent)
            .unwrap();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::SelectedOnly)
            .unwrap();
        assert_eq!(
            select_generation_profile(&counts, None).unwrap(),
            GenerationProfileDecision::UniqueFullGenerationDecode
        );
    }

    #[test]
    fn generation_level_all_valid_divergent_requires_provenance() {
        let mut counts = AuditCounts::default();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::BothSemanticallyDivergent)
            .unwrap();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::BothSemanticallyEquivalent)
            .unwrap();
        assert!(select_generation_profile(&counts, None).is_err());
        assert_eq!(
            select_generation_profile(&counts, Some(ProfileProvenance::ProfileBoundReceipt))
                .unwrap(),
            GenerationProfileDecision::ProfileBoundReceipt
        );
    }

    #[test]
    fn generation_level_all_equivalent_is_safe_without_provenance() {
        let mut counts = AuditCounts::default();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::BothSemanticallyEquivalent)
            .unwrap();
        counts
            .record_profile_outcome(WireProfileAuditOutcome::BothSemanticallyEquivalent)
            .unwrap();
        assert_eq!(
            select_generation_profile(&counts, None).unwrap(),
            GenerationProfileDecision::AllSemanticallyEquivalent
        );
    }

    #[test]
    fn generation_level_selection_requires_a_complete_classification() {
        let counts = AuditCounts {
            messages: 2,
            both_equivalent: 1,
            ..AuditCounts::default()
        };
        assert!(select_generation_profile(&counts, None).is_err());
    }

    #[test]
    fn effective_direct_identity_changes_with_any_bound_field() {
        let identity = FileIdentity {
            size: 1,
            device: 2,
            inode: 3,
            modified_seconds: 4,
            modified_nanoseconds: 5,
            changed_seconds: 6,
            changed_nanoseconds: 7,
        };
        let files = BTreeMap::from([("archive-v2-blocks.zstd".into(), identity.clone())]);
        let base = direct_generation_digest(700, "usage_sorted", &files);
        assert_ne!(base, direct_generation_digest(701, "usage_sorted", &files));
        assert_ne!(base, direct_generation_digest(700, "first_seen", &files));
        let mut changed = files;
        changed.get_mut("archive-v2-blocks.zstd").unwrap().inode += 1;
        assert_ne!(
            base,
            direct_generation_digest(700, "usage_sorted", &changed)
        );
    }

    #[test]
    fn attestation_name_uses_exact_content_generation() {
        let generation = "a".repeat(64);
        assert_eq!(
            format!("epoch-{}-{}.json", 700, generation),
            format!("epoch-700-{}.json", "a".repeat(64))
        );
    }

    #[test]
    fn receipt_source_and_target_generation_authority_is_derived_and_hash_checked() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(source.join("archive-v2-blocks.zstd"), b"src").unwrap();
        fs::write(target.join("archive-v2-blocks.zstd"), b"dst").unwrap();
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        let marker_bytes = wire_profile_marker_bytes(profile);
        fs::write(source.join(&marker.name), marker_bytes).unwrap();
        fs::write(target.join(&marker.name), marker_bytes).unwrap();
        let source_files = BTreeMap::from([
            ("archive-v2-blocks.zstd".into(), binding(b"src")),
            (marker.name.clone(), binding(marker_bytes)),
        ]);
        let target_files = BTreeMap::from([
            ("archive-v2-blocks.zstd".into(), binding(b"dst")),
            (marker.name.clone(), binding(marker_bytes)),
        ]);
        let source_generation = registry_generation_digest(&source_files);
        let target_generation = registry_generation_digest(&target_files);
        let receipt = RegistryReceipt {
            version: 3,
            algorithm: "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3".into(),
            epoch: 700,
            source_dir: source.to_string_lossy().into_owned(),
            target_dir: target.to_string_lossy().into_owned(),
            source_generation_sha256: source_generation.clone(),
            target_generation_sha256: target_generation.clone(),
            source_files,
            target_files,
            wire_profile: Some(profile),
        };
        validate_receipt_wire_profile(&receipt, profile).unwrap();
        let mut missing_profile = receipt.clone();
        missing_profile.wire_profile = None;
        assert!(validate_receipt_wire_profile(&missing_profile, profile,).is_err());
        assert!(
            validate_receipt_wire_profile(
                &receipt,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            )
            .is_err()
        );
        let receipt_path = target.join("archive-v2-registry-reprocess.receipt.json");
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();

        let source_capture = capture_registry_receipt_generation(
            &source,
            700,
            "first_seen",
            GenerationKind::RegistryReceiptSourceFilesV1,
            &receipt_path,
            profile,
            ReceiptFileVerification::HashContents,
        )
        .unwrap();
        let target_capture = capture_registry_receipt_generation(
            &target,
            700,
            "usage_sorted",
            GenerationKind::RegistryReceiptTargetFilesV1,
            &receipt_path,
            profile,
            ReceiptFileVerification::HashContents,
        )
        .unwrap();
        assert_eq!(source_capture.content_generation_sha256, source_generation);
        assert_eq!(target_capture.content_generation_sha256, target_generation);
        assert_eq!(source_capture.profile_provenance, None);
        assert_eq!(
            target_capture.profile_provenance,
            Some(ProfileProvenance::ProfileBoundReceipt)
        );
        assert_eq!(source_capture.selected_archive_files.len(), 2);
        assert_eq!(target_capture.selected_archive_files.len(), 2);
        assert!(
            source_capture
                .selected_archive_files
                .contains_key(&marker.name)
        );
        assert!(
            target_capture
                .selected_archive_files
                .contains_key(&marker.name)
        );

        // This uses the real profile-bound receipt capture and the exact
        // producer formatter. A source recovery must still produce the
        // neutral dual-profile decision accepted by every consumer.
        let counts = AuditCounts {
            blocks: 1,
            messages: 2,
            both_equivalent: 2,
            ..AuditCounts::default()
        };
        let (decision, evidence) = produce_audit_evidence(
            GenerationKind::RegistryReceiptSourceFilesV1,
            &counts,
            source_capture.profile_provenance,
        )
        .unwrap();
        assert_eq!(
            decision,
            GenerationProfileDecision::AllSemanticallyEquivalent
        );
        validate_receipt_source_recovery_evidence(&evidence).unwrap();
        assert!(
            produce_audit_evidence(
                GenerationKind::RegistryReceiptSourceFilesV1,
                &counts,
                Some(ProfileProvenance::ProfileBoundReceipt),
            )
            .is_err()
        );

        // Same-size replacement bytes cannot reuse the receipt authority.
        fs::write(source.join("archive-v2-blocks.zstd"), b"bad").unwrap();
        assert!(
            capture_registry_receipt_generation(
                &source,
                700,
                "first_seen",
                GenerationKind::RegistryReceiptSourceFilesV1,
                &receipt_path,
                profile,
                ReceiptFileVerification::HashContents,
            )
            .is_err()
        );
    }

    #[test]
    fn legacy_v3_receipt_capture_binds_both_sides_without_claiming_profile_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(source.join("archive-v2-blocks.zstd"), b"src").unwrap();
        fs::write(target.join("archive-v2-blocks.zstd"), b"dst").unwrap();
        let source_files = BTreeMap::from([("archive-v2-blocks.zstd".into(), binding(b"src"))]);
        let target_files = BTreeMap::from([("archive-v2-blocks.zstd".into(), binding(b"dst"))]);
        let source_generation = registry_generation_digest(&source_files);
        let target_generation = registry_generation_digest(&target_files);
        let receipt = RegistryReceipt {
            version: 3,
            algorithm: "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3".into(),
            epoch: 700,
            source_dir: source.to_string_lossy().into_owned(),
            target_dir: target.to_string_lossy().into_owned(),
            source_generation_sha256: source_generation.clone(),
            target_generation_sha256: target_generation,
            source_files,
            target_files,
            wire_profile: None,
        };
        let receipt_path = target.join("archive-v2-registry-reprocess.receipt.json");
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();

        let capture = capture_registry_receipt_generation(
            &source,
            700,
            "first_seen",
            GenerationKind::RegistryReceiptSourceFilesV1,
            &receipt_path,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ReceiptFileVerification::HashContents,
        )
        .unwrap();
        assert_eq!(capture.content_generation_sha256, source_generation);
        assert_eq!(capture.selected_archive_files.len(), 1);
        assert_eq!(capture.all_bound_files.len(), 3);
        assert_eq!(capture.profile_provenance, None);

        let marker = wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        fs::write(
            source.join(&marker.name),
            wire_profile_marker_bytes(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1),
        )
        .unwrap();
        assert!(
            capture_registry_receipt_generation(
                &source,
                700,
                "first_seen",
                GenerationKind::RegistryReceiptSourceFilesV1,
                &receipt_path,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                ReceiptFileVerification::HashContents,
            )
            .is_err()
        );
    }

    #[test]
    fn exact_wire_profile_marker_is_bound_and_must_match_requested_profile() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let descriptor = wire_profile_marker(profile);
        fs::write(
            root.join(&descriptor.name),
            wire_profile_marker_bytes(profile),
        )
        .unwrap();

        let marker = capture_wire_profile_marker(&root, profile, None, "test generation")
            .unwrap()
            .unwrap();
        assert_eq!(marker.profile, profile);
        assert_eq!(marker.name, descriptor.name);
        assert_eq!(marker.identity.size, descriptor.size);

        assert!(
            capture_wire_profile_marker(
                &root,
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                None,
                "test generation",
            )
            .is_err()
        );
    }

    #[test]
    fn standalone_direct_marker_is_bound_but_is_not_profile_authority() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        for name in DIRECT_SEMANTIC_FILES {
            fs::write(root.join(name), b"semantic-input").unwrap();
        }
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        fs::write(root.join(&marker.name), wire_profile_marker_bytes(profile)).unwrap();

        let (files, provenance) = capture_generation(&root, 700, "usage_sorted", profile).unwrap();
        assert!(files.contains_key(&marker.name));
        assert_eq!(provenance, None);
    }

    #[test]
    fn audit_descriptors_must_match_the_selected_attestation_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let name = "receipt-selected-extra.bin";
        let path = root.join(name);
        fs::write(&path, b"original").unwrap();
        let expected = capture_file_identity(&path).unwrap();
        let capture = GenerationCapture {
            content_generation_sha256: "a".repeat(64),
            selected_archive_files: BTreeMap::from([(name.into(), expected)]),
            all_bound_files: BTreeMap::new(),
            profile_provenance: None,
        };

        let source = PinnedLocalRangeSource::new(&root);
        bind_selected_capture_to_pinned_source(&source, &capture).unwrap();

        let replaced_root = root.join("replacement-case");
        fs::create_dir(&replaced_root).unwrap();
        let replaced_path = replaced_root.join(name);
        fs::write(&replaced_path, b"original").unwrap();
        let replaced_expected = capture_file_identity(&replaced_path).unwrap();
        let old_path = replaced_root.join("old.bin");
        fs::rename(&replaced_path, &old_path).unwrap();
        fs::write(&replaced_path, b"restored").unwrap();
        let replaced_capture = GenerationCapture {
            content_generation_sha256: "b".repeat(64),
            selected_archive_files: BTreeMap::from([(name.into(), replaced_expected)]),
            all_bound_files: BTreeMap::new(),
            profile_provenance: None,
        };
        assert!(
            bind_selected_capture_to_pinned_source(
                &PinnedLocalRangeSource::new(&replaced_root),
                &replaced_capture,
            )
            .is_err()
        );
    }

    #[test]
    fn conflicting_or_malformed_wire_profile_markers_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let pre = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let post = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let pre_marker = wire_profile_marker(pre);
        let post_marker = wire_profile_marker(post);
        fs::write(root.join(&pre_marker.name), wire_profile_marker_bytes(pre)).unwrap();
        fs::write(
            root.join(&post_marker.name),
            wire_profile_marker_bytes(post),
        )
        .unwrap();
        assert!(capture_wire_profile_marker(&root, pre, None, "test generation").is_err());

        fs::remove_file(root.join(&post_marker.name)).unwrap();
        fs::write(
            root.join(&pre_marker.name),
            vec![0; pre_marker.size as usize],
        )
        .unwrap();
        assert!(capture_wire_profile_marker(&root, pre, None, "test generation").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn wire_profile_marker_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        let target = root.join("marker-target");
        fs::write(&target, wire_profile_marker_bytes(profile)).unwrap();
        symlink(&target, root.join(&marker.name)).unwrap();
        assert!(capture_wire_profile_marker(&root, profile, None, "test generation").is_err());
    }
}
