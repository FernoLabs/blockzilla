//! Controlled marker transition for marker-free direct Archive V2 generations.
//!
//! The transition does not infer a wire profile. Its only authority is an
//! exact, protected, marker-free direct attestation whose full-generation
//! dual-profile evidence made a neutral decision. The operation is resumable:
//! a protected intent is durable before the marker becomes visible.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
};
use blockzilla_read_sdk_legacy::{
    ArchiveReader, ArchiveV2MetadataWireProfile, ArchiveV2PublicationLock, ArchiveV2WireProfile,
    FullGenerationWireProfileAudit, HashVerification, OpenOptions as ReaderOpenOptions,
    PinnedLocalRangeSource, UnprovenWireProfileDecision, acquire_archive_v2_publication_lock,
    audit_full_generation_wire_profile,
    manifest::{GENERATION_MANIFEST_FILE, TrustedGenerationIdentity},
    wire_profile_marker, wire_profile_marker_bytes,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    firewatch_wire_profile_attestation::{
        DIRECT_ATTESTATION_GENERATION_KIND, FullGenerationAuditEvidenceV3,
        WIRE_PROFILE_ATTESTATION_KIND, WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
        WIRE_PROFILE_AUDIT_ALGORITHM, WIRE_PROFILE_AUDITED_PROFILES, WireProfileAttestation,
        validate_neutral_direct_generation_evidence, validate_wire_profile_attestation_structure,
    },
    format::RegistryFileIdentity,
};

pub const MARKER_TRANSITION_INTENT_SCHEMA_VERSION: u32 = 1;
pub const MARKER_TRANSITION_INTENT_KIND: &str = "archive_v2_wire_profile_marker_transition_intent";
pub const MARKER_TRANSITION_RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const MARKER_TRANSITION_RECEIPT_KIND: &str =
    "archive_v2_wire_profile_marker_transition_receipt";
pub const MARKER_TRANSITION_ALGORITHM: &str =
    "direct-neutral-full-generation-audit-marker-transition-v1";
pub const WIRE_PROFILE_ATTESTATIONS_DIR: &str = "wire-profile-attestations";
pub const WIRE_PROFILE_TRANSITIONS_DIR: &str = "wire-profile-marker-transitions";

const DIRECT_GENERATION_DOMAIN: &[u8] = b"blockzilla.firewatch.direct-generation.v1\0";
const MAX_CONTROL_JSON_BYTES: u64 = 64 * 1024;
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const DIRECT_SEMANTIC_FILES: [&str; 6] = [
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
];

/// Concurrency proof required by the mutating transition.
///
/// # Safety
///
/// An implementation must prove that, for the complete call, no archive
/// publisher can add or replace a generation object and no Firewatch
/// controller can admit or start work from either the old or new identity.
/// `recheck_exclusive` must fail if either exclusion is lost or its lock path
/// no longer names the locked file descriptor.
pub unsafe trait ExclusiveMarkerTransitionLock {
    fn recheck_exclusive(&self, archive: &Path, controller_state_root: &Path) -> Result<()>;
}

/// The two exclusions required for an in-place direct-generation transition.
///
/// Acquisition fails while the Firewatch controller is running. This is
/// intentional: a controller child must not keep using the old identity while
/// the marker creates the new identity.
#[derive(Debug)]
pub struct MarkerTransitionLocks {
    archive: ArchiveV2PublicationLock,
    controller_root: PathBuf,
    controller_path: PathBuf,
    controller_file: File,
    controller_device: u64,
    controller_inode: u64,
}

impl MarkerTransitionLocks {
    pub fn acquire(archive: &Path, controller_state_root: &Path) -> Result<Self> {
        ensure_protected_directory(controller_state_root, "controller state root")?;
        let controller_path = controller_state_root.join("controller.lock");
        let controller_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(&controller_path)
            .with_context(|| format!("open controller lock {}", controller_path.display()))?;
        let opened = controller_file.metadata()?;
        ensure!(
            opened.file_type().is_file(),
            "controller lock is not a regular file"
        );
        // SAFETY: `controller_file` remains owned by this guard.
        let result =
            unsafe { libc::flock(controller_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        ensure!(
            result == 0,
            "the Firewatch controller is running; stop it before a marker transition"
        );
        let current = fs::symlink_metadata(&controller_path)?;
        ensure!(
            same_file(&opened, &current),
            "controller lock path changed while it was acquired"
        );

        let archive =
            acquire_archive_v2_publication_lock(archive).map_err(|error| anyhow::anyhow!(error))?;
        let locks = Self {
            archive,
            controller_root: controller_state_root.to_path_buf(),
            controller_path,
            controller_file,
            controller_device: opened.dev(),
            controller_inode: opened.ino(),
        };
        locks.recheck_exclusive(locks.archive.root(), controller_state_root)?;
        Ok(locks)
    }
}

// SAFETY: this guard owns both shared lock descriptors for its full lifetime
// and rechecks that each pathname still names the admitted inode.
unsafe impl ExclusiveMarkerTransitionLock for MarkerTransitionLocks {
    fn recheck_exclusive(&self, archive: &Path, controller_state_root: &Path) -> Result<()> {
        ensure!(
            self.archive.root() == archive && self.controller_root == controller_state_root,
            "marker transition lock roots differ from the requested roots"
        );
        self.archive
            .recheck()
            .map_err(|error| anyhow::anyhow!(error))?;
        let opened = self.controller_file.metadata()?;
        let current = fs::symlink_metadata(&self.controller_path)?;
        ensure!(
            opened.file_type().is_file()
                && current.file_type().is_file()
                && !current.file_type().is_symlink()
                && opened.dev() == self.controller_device
                && opened.ino() == self.controller_inode
                && current.dev() == self.controller_device
                && current.ino() == self.controller_inode,
            "controller lock path changed while the lock was held"
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MarkerTransitionOptions {
    pub archive: PathBuf,
    pub controller_state_root: PathBuf,
    pub epoch: u64,
    pub registry_order: String,
    pub old_content_generation_sha256: String,
    pub slots_per_epoch: u64,
    pub max_message_bytes: usize,
}

impl MarkerTransitionOptions {
    pub fn new(
        archive: PathBuf,
        controller_state_root: PathBuf,
        epoch: u64,
        registry_order: String,
        old_content_generation_sha256: String,
    ) -> Self {
        Self {
            archive,
            controller_state_root,
            epoch,
            registry_order,
            old_content_generation_sha256,
            slots_per_epoch: 432_000,
            max_message_bytes: MAX_MESSAGE_BYTES,
        }
    }

    pub fn attestation_root(&self) -> PathBuf {
        self.controller_state_root
            .join(WIRE_PROFILE_ATTESTATIONS_DIR)
    }

    pub fn transition_root(&self) -> PathBuf {
        self.controller_state_root
            .join(WIRE_PROFILE_TRANSITIONS_DIR)
    }

    pub fn old_attestation_path(&self) -> PathBuf {
        self.attestation_root().join(format!(
            "epoch-{}-{}.json",
            self.epoch, self.old_content_generation_sha256
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImmutableFileEvidence {
    pub path: PathBuf,
    pub identity: RegistryFileIdentity,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImmutableContentBinding {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkerBinding {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
    pub identity: RegistryFileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkerTransitionIntent {
    pub schema_version: u32,
    pub kind: String,
    pub algorithm: String,
    pub cluster_id: String,
    pub epoch: u64,
    pub archive: PathBuf,
    pub registry_order: String,
    pub wire_profile: ArchiveV2WireProfile,
    pub old_content_generation_sha256: String,
    pub old_archive_files: BTreeMap<String, RegistryFileIdentity>,
    pub authority_attestation: ImmutableFileEvidence,
    pub neutral_audit_evidence: String,
    pub marker_name: String,
    pub marker_bytes: u64,
    pub marker_sha256: String,
    pub new_attestation_root: PathBuf,
    pub prepared_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkerTransitionReceipt {
    pub schema_version: u32,
    pub kind: String,
    pub algorithm: String,
    pub cluster_id: String,
    pub epoch: u64,
    pub archive: PathBuf,
    pub registry_order: String,
    pub wire_profile: ArchiveV2WireProfile,
    pub old_content_generation_sha256: String,
    pub new_content_generation_sha256: String,
    pub old_archive_files: BTreeMap<String, RegistryFileIdentity>,
    pub new_archive_files: BTreeMap<String, RegistryFileIdentity>,
    pub authority_attestation: ImmutableFileEvidence,
    pub transition_intent: ImmutableFileEvidence,
    pub marker: MarkerBinding,
    pub neutral_audit_evidence: String,
    pub new_attestation: ImmutableContentBinding,
    pub transitioned_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerTransitionOutcome {
    pub wire_profile: ArchiveV2WireProfile,
    pub old_content_generation_sha256: String,
    pub new_content_generation_sha256: String,
    pub marker_path: PathBuf,
    pub intent_path: PathBuf,
    pub receipt_path: PathBuf,
    pub new_attestation_path: PathBuf,
}

#[derive(Debug)]
struct PinnedFileEvidence {
    path: PathBuf,
    file: File,
    identity: RegistryFileIdentity,
    sha256: String,
    max_bytes: u64,
}

impl PinnedFileEvidence {
    fn public(&self) -> ImmutableFileEvidence {
        ImmutableFileEvidence {
            path: self.path.clone(),
            identity: self.identity.clone(),
            sha256: self.sha256.clone(),
        }
    }

    fn recheck(&self, protected: bool, label: &str) -> Result<()> {
        let descriptor_before = self.file.metadata()?;
        ensure!(
            file_identity(&descriptor_before) == self.identity,
            "{label} descriptor identity changed"
        );
        if protected {
            validate_protected_file(&descriptor_before, label)?;
        }
        let path_before = fs::symlink_metadata(&self.path)?;
        ensure!(
            path_before.file_type().is_file()
                && !path_before.file_type().is_symlink()
                && file_identity(&path_before) == self.identity,
            "{label} path no longer names its admitted descriptor"
        );
        if protected {
            validate_protected_file(&path_before, label)?;
        }
        let bytes = read_pinned_fd_bytes(&self.file, self.max_bytes, label)?;
        ensure!(
            bytes.len() as u64 == self.identity.size
                && hex_digest(Sha256::digest(&bytes)) == self.sha256,
            "{label} content changed"
        );
        let descriptor_after = self.file.metadata()?;
        let path_after = fs::symlink_metadata(&self.path)?;
        ensure!(
            file_identity(&descriptor_after) == self.identity
                && path_after.file_type().is_file()
                && !path_after.file_type().is_symlink()
                && file_identity(&path_after) == self.identity,
            "{label} changed during final recheck"
        );
        if protected {
            validate_protected_file(&descriptor_after, label)?;
            validate_protected_file(&path_after, label)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SemanticFilePins {
    files: BTreeMap<String, RegistryFileIdentity>,
    pins: Vec<(String, File, RegistryFileIdentity)>,
}

impl SemanticFilePins {
    fn recheck(&self, archive: &Path) -> Result<()> {
        for (name, file, identity) in &self.pins {
            let descriptor = file.metadata()?;
            let path = fs::symlink_metadata(archive.join(name))?;
            ensure!(
                file_identity(&descriptor) == *identity
                    && path.file_type().is_file()
                    && !path.file_type().is_symlink()
                    && file_identity(&path) == *identity,
                "direct archive semantic file changed: {name}"
            );
        }
        Ok(())
    }
}

/// Add an authenticated marker to one exact marker-free direct generation.
///
/// The function is idempotent only through its protected intent. A selected
/// marker that exists without the exact intent is rejected. The opposite
/// marker is always rejected.
pub fn transition_marker_free_direct_generation(
    options: &MarkerTransitionOptions,
    locks: &impl ExclusiveMarkerTransitionLock,
) -> Result<MarkerTransitionOutcome> {
    validate_options(options)?;
    let archive = &options.archive;
    let controller_root = &options.controller_state_root;
    let attestation_root = options.attestation_root();
    let transition_root = options.transition_root();
    locks.recheck_exclusive(archive, controller_root)?;
    ensure_canonical_real_directory(archive, "archive")?;
    ensure_no_published_manifest(archive)?;
    ensure_protected_directory(controller_root, "controller state root")?;
    ensure_protected_directory(&attestation_root, "wire-profile attestation root")?;
    ensure_or_create_protected_child(
        controller_root,
        WIRE_PROFILE_TRANSITIONS_DIR,
        "wire-profile transition root",
    )?;
    ensure_protected_directory(&transition_root, "wire-profile transition root")?;

    let old_attestation_path = options.old_attestation_path();
    let (old_attestation, old_attestation_pin) = read_pinned_json::<WireProfileAttestation>(
        &old_attestation_path,
        MAX_CONTROL_JSON_BYTES,
        "old wire-profile attestation",
    )?;
    old_attestation_pin.recheck(true, "old wire-profile attestation")?;
    validate_old_attestation(options, &old_attestation)?;
    let neutral_evidence = validate_neutral_direct_generation_evidence(
        &old_attestation.evidence,
        old_attestation.wire_profile,
    )?;

    let semantic_pins = pin_direct_semantic_files(archive)?;
    ensure!(
        semantic_pins.files == old_attestation.archive_files,
        "live semantic identities differ from the old attestation"
    );
    ensure!(
        direct_generation_digest(options.epoch, &options.registry_order, &semantic_pins.files,)
            == options.old_content_generation_sha256,
        "old direct-generation identity does not match its exact semantic files"
    );
    semantic_pins.recheck(archive)?;

    let marker = wire_profile_marker(old_attestation.wire_profile);
    let marker_bytes = wire_profile_marker_bytes(old_attestation.wire_profile);
    ensure!(
        marker.size == marker_bytes.len() as u64
            && marker.sha256 == hex_digest(Sha256::digest(marker_bytes)),
        "SDK wire-profile marker definition is inconsistent"
    );
    let opposite_profile = opposite_profile(old_attestation.wire_profile);
    let opposite = wire_profile_marker(opposite_profile);
    ensure_path_absent(
        &archive.join(&opposite.name),
        "opposite wire-profile marker",
    )?;
    let selected_marker_before = read_exact_marker_if_present(
        &archive.join(&marker.name),
        marker_bytes,
        marker.size,
        &marker.sha256,
    )?;

    let intent_path = transition_root.join(format!(
        "epoch-{}-{}.intent.json",
        options.epoch, options.old_content_generation_sha256
    ));
    let existing_intent = read_optional_pinned_json::<MarkerTransitionIntent>(
        &intent_path,
        MAX_CONTROL_JSON_BYTES,
        "marker transition intent",
    )?;
    ensure!(
        existing_intent.is_some() || selected_marker_before.is_none(),
        "selected wire-profile marker exists without the exact protected transition intent"
    );
    let prepared_unix_secs = existing_intent
        .as_ref()
        .map_or_else(unix_now, |(intent, _)| intent.prepared_unix_secs);
    let intent = MarkerTransitionIntent {
        schema_version: MARKER_TRANSITION_INTENT_SCHEMA_VERSION,
        kind: MARKER_TRANSITION_INTENT_KIND.into(),
        algorithm: MARKER_TRANSITION_ALGORITHM.into(),
        cluster_id: "mainnet-beta".into(),
        epoch: options.epoch,
        archive: archive.clone(),
        registry_order: options.registry_order.clone(),
        wire_profile: old_attestation.wire_profile,
        old_content_generation_sha256: options.old_content_generation_sha256.clone(),
        old_archive_files: semantic_pins.files.clone(),
        authority_attestation: old_attestation_pin.public(),
        neutral_audit_evidence: old_attestation.evidence.clone(),
        marker_name: marker.name.clone(),
        marker_bytes: marker.size,
        marker_sha256: marker.sha256.clone(),
        // The exact new generation is inode-bound and is known only after the
        // no-clobber marker publication.
        new_attestation_root: attestation_root.clone(),
        prepared_unix_secs,
    };
    validate_transition_intent(&intent)?;
    let intent_bytes = serde_json::to_vec_pretty(&intent)?;
    let intent_pin = match existing_intent {
        Some((existing, pin)) => {
            ensure!(
                existing == intent,
                "existing marker transition intent differs from this exact transition"
            );
            pin.recheck(true, "marker transition intent")?;
            pin
        }
        None => {
            publish_bytes_no_replace(&intent_path, &intent_bytes, 0o600)?;
            let (stored, pin) = read_pinned_json::<MarkerTransitionIntent>(
                &intent_path,
                MAX_CONTROL_JSON_BYTES,
                "marker transition intent",
            )?;
            ensure!(
                stored == intent,
                "published marker transition intent changed"
            );
            pin.recheck(true, "marker transition intent")?;
            pin
        }
    };
    ensure!(
        intent_pin.sha256 == hex_digest(Sha256::digest(&intent_bytes)),
        "marker transition intent hash differs from its canonical bytes"
    );

    locks.recheck_exclusive(archive, controller_root)?;
    ensure_no_published_manifest(archive)?;
    old_attestation_pin.recheck(true, "old wire-profile attestation")?;
    intent_pin.recheck(true, "marker transition intent")?;
    semantic_pins.recheck(archive)?;
    ensure_path_absent(
        &archive.join(&opposite.name),
        "opposite wire-profile marker",
    )?;

    let marker_path = archive.join(&marker.name);
    let marker_identity = match selected_marker_before {
        Some(identity) => identity,
        None => publish_marker_no_replace(&marker_path, marker_bytes)?,
    };
    locks.recheck_exclusive(archive, controller_root)?;
    ensure_no_published_manifest(archive)?;
    let rechecked_marker =
        read_exact_marker_if_present(&marker_path, marker_bytes, marker.size, &marker.sha256)?
            .context("selected wire-profile marker disappeared after publication")?;
    ensure!(
        rechecked_marker == marker_identity,
        "selected wire-profile marker identity changed after publication"
    );
    ensure_path_absent(
        &archive.join(&opposite.name),
        "opposite wire-profile marker",
    )?;
    semantic_pins.recheck(archive)?;

    let mut new_files = semantic_pins.files.clone();
    ensure!(
        new_files
            .insert(marker.name.clone(), marker_identity.clone())
            .is_none(),
        "selected marker collides with a semantic file"
    );
    let new_generation =
        direct_generation_digest(options.epoch, &options.registry_order, &new_files);
    ensure!(
        new_generation != options.old_content_generation_sha256,
        "marker publication did not change the direct-generation identity"
    );

    let rechecked_counts =
        audit_exact_generation(options, &new_generation, old_attestation.wire_profile)?;
    ensure!(
        audit_matches_evidence(&rechecked_counts, &neutral_evidence),
        "post-marker semantic recheck differs from the neutral authority audit"
    );
    locks.recheck_exclusive(archive, controller_root)?;
    ensure_no_published_manifest(archive)?;
    old_attestation_pin.recheck(true, "old wire-profile attestation")?;
    intent_pin.recheck(true, "marker transition intent")?;
    semantic_pins.recheck(archive)?;
    ensure!(
        read_exact_marker_if_present(&marker_path, marker_bytes, marker.size, &marker.sha256,)?
            == Some(marker_identity.clone()),
        "selected wire-profile marker changed during semantic recheck"
    );

    let new_attestation_path =
        attestation_root.join(format!("epoch-{}-{}.json", options.epoch, new_generation));
    let new_attestation = WireProfileAttestation {
        schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
        kind: WIRE_PROFILE_ATTESTATION_KIND.into(),
        audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
        audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
        cluster_id: "mainnet-beta".into(),
        epoch: options.epoch,
        archive: archive.clone(),
        registry_order: options.registry_order.clone(),
        generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
        content_generation_sha256: new_generation.clone(),
        archive_files: new_files.clone(),
        wire_profile: old_attestation.wire_profile,
        evidence: old_attestation.evidence.clone(),
        attested_unix_secs: intent.prepared_unix_secs,
    };
    validate_wire_profile_attestation_structure(&new_attestation)?;
    validate_neutral_direct_generation_evidence(
        &new_attestation.evidence,
        new_attestation.wire_profile,
    )?;
    let new_attestation_bytes = serde_json::to_vec_pretty(&new_attestation)?;
    ensure!(
        new_attestation_bytes.len() as u64 <= MAX_CONTROL_JSON_BYTES,
        "new marker-bound attestation is too large"
    );
    let new_attestation_sha256 = hex_digest(Sha256::digest(&new_attestation_bytes));
    let receipt_path = transition_root.join(format!(
        "epoch-{}-{}-to-{}.receipt.json",
        options.epoch, options.old_content_generation_sha256, new_generation
    ));
    let receipt = MarkerTransitionReceipt {
        schema_version: MARKER_TRANSITION_RECEIPT_SCHEMA_VERSION,
        kind: MARKER_TRANSITION_RECEIPT_KIND.into(),
        algorithm: MARKER_TRANSITION_ALGORITHM.into(),
        cluster_id: "mainnet-beta".into(),
        epoch: options.epoch,
        archive: archive.clone(),
        registry_order: options.registry_order.clone(),
        wire_profile: old_attestation.wire_profile,
        old_content_generation_sha256: options.old_content_generation_sha256.clone(),
        new_content_generation_sha256: new_generation.clone(),
        old_archive_files: semantic_pins.files.clone(),
        new_archive_files: new_files,
        authority_attestation: old_attestation_pin.public(),
        transition_intent: intent_pin.public(),
        marker: MarkerBinding {
            name: marker.name.clone(),
            bytes: marker.size,
            sha256: marker.sha256.clone(),
            identity: marker_identity,
        },
        neutral_audit_evidence: old_attestation.evidence,
        new_attestation: ImmutableContentBinding {
            path: new_attestation_path.clone(),
            bytes: new_attestation_bytes.len() as u64,
            sha256: new_attestation_sha256,
        },
        transitioned_unix_secs: intent.prepared_unix_secs,
    };
    validate_transition_receipt(&receipt)?;
    let receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
    ensure!(
        receipt_bytes.len() as u64 <= MAX_CONTROL_JSON_BYTES,
        "marker transition receipt is too large"
    );

    // Publish the durable receipt before the new attestation. If the process
    // stops between these operations, the protected intent and receipt make
    // the exact attestation bytes reconstructible and the controller remains
    // fail-closed because the new generation has no attestation yet.
    locks.recheck_exclusive(archive, controller_root)?;
    ensure_no_published_manifest(archive)?;
    publish_or_validate_exact_json(&receipt_path, &receipt_bytes, "marker transition receipt")?;
    publish_or_validate_exact_json(
        &new_attestation_path,
        &new_attestation_bytes,
        "new marker-bound attestation",
    )?;
    sync_directory(&transition_root)?;
    sync_directory(&attestation_root)?;
    locks.recheck_exclusive(archive, controller_root)?;
    ensure_no_published_manifest(archive)?;
    old_attestation_pin.recheck(true, "old wire-profile attestation")?;
    intent_pin.recheck(true, "marker transition intent")?;
    semantic_pins.recheck(archive)?;
    ensure_path_absent(
        &archive.join(&opposite.name),
        "opposite wire-profile marker",
    )?;
    ensure!(
        read_exact_marker_if_present(&marker_path, marker_bytes, marker.size, &marker.sha256,)?
            == Some(receipt.marker.identity.clone()),
        "selected wire-profile marker changed before transition completion"
    );
    validate_live_output(&receipt_path, &receipt_bytes, "marker transition receipt")?;
    validate_live_output(
        &new_attestation_path,
        &new_attestation_bytes,
        "new marker-bound attestation",
    )?;

    Ok(MarkerTransitionOutcome {
        wire_profile: new_attestation.wire_profile,
        old_content_generation_sha256: options.old_content_generation_sha256.clone(),
        new_content_generation_sha256: new_generation,
        marker_path,
        intent_path,
        receipt_path,
        new_attestation_path,
    })
}

fn validate_options(options: &MarkerTransitionOptions) -> Result<()> {
    ensure!(
        options.archive.is_absolute(),
        "archive path must be absolute"
    );
    ensure!(
        options.controller_state_root.is_absolute(),
        "controller state root must be absolute"
    );
    ensure!(
        matches!(
            options.registry_order.as_str(),
            "first_seen" | "usage_sorted"
        ),
        "registry order is invalid"
    );
    ensure!(
        is_sha256(&options.old_content_generation_sha256),
        "old generation is not a lowercase SHA-256"
    );
    ensure!(
        options.slots_per_epoch > 0,
        "slots per epoch must be positive"
    );
    ensure!(
        (1..=MAX_MESSAGE_BYTES).contains(&options.max_message_bytes),
        "maximum message bytes must be between 1 and {MAX_MESSAGE_BYTES}"
    );
    Ok(())
}

fn validate_old_attestation(
    options: &MarkerTransitionOptions,
    attestation: &WireProfileAttestation,
) -> Result<()> {
    validate_wire_profile_attestation_structure(attestation)?;
    ensure!(
        attestation.cluster_id == "mainnet-beta"
            && attestation.epoch == options.epoch
            && attestation.archive == options.archive
            && attestation.registry_order == options.registry_order
            && attestation.generation_kind == DIRECT_ATTESTATION_GENERATION_KIND
            && attestation.content_generation_sha256 == options.old_content_generation_sha256,
        "old attestation provenance differs from the requested transition"
    );
    ensure_exact_marker_free_semantic_file_set(&attestation.archive_files)?;
    validate_neutral_direct_generation_evidence(&attestation.evidence, attestation.wire_profile)?;
    Ok(())
}

pub fn validate_transition_intent(intent: &MarkerTransitionIntent) -> Result<()> {
    ensure!(
        intent.schema_version == MARKER_TRANSITION_INTENT_SCHEMA_VERSION
            && intent.kind == MARKER_TRANSITION_INTENT_KIND
            && intent.algorithm == MARKER_TRANSITION_ALGORITHM
            && intent.cluster_id == "mainnet-beta",
        "marker transition intent header is invalid"
    );
    ensure!(
        intent.archive.is_absolute()
            && intent.new_attestation_root.is_absolute()
            && matches!(
                intent.registry_order.as_str(),
                "first_seen" | "usage_sorted"
            )
            && is_sha256(&intent.old_content_generation_sha256)
            && intent.prepared_unix_secs > 0,
        "marker transition intent provenance is invalid"
    );
    ensure_exact_marker_free_semantic_file_set(&intent.old_archive_files)?;
    ensure!(
        direct_generation_digest(
            intent.epoch,
            &intent.registry_order,
            &intent.old_archive_files,
        ) == intent.old_content_generation_sha256,
        "marker transition intent old identity is invalid"
    );
    validate_immutable_file_evidence(
        &intent.authority_attestation,
        "intent authority attestation",
    )?;
    validate_neutral_direct_generation_evidence(
        &intent.neutral_audit_evidence,
        intent.wire_profile,
    )?;
    let marker = wire_profile_marker(intent.wire_profile);
    ensure!(
        intent.marker_name == marker.name
            && intent.marker_bytes == marker.size
            && intent.marker_sha256 == marker.sha256
            && marker.size == wire_profile_marker_bytes(intent.wire_profile).len() as u64
            && marker.sha256
                == hex_digest(Sha256::digest(wire_profile_marker_bytes(
                    intent.wire_profile,
                ))),
        "marker transition intent marker binding is invalid"
    );
    ensure!(
        intent.authority_attestation.path
            == intent.new_attestation_root.join(format!(
                "epoch-{}-{}.json",
                intent.epoch, intent.old_content_generation_sha256
            )),
        "marker transition intent authority path is not canonical"
    );
    Ok(())
}

pub fn validate_transition_receipt(receipt: &MarkerTransitionReceipt) -> Result<()> {
    ensure!(
        receipt.schema_version == MARKER_TRANSITION_RECEIPT_SCHEMA_VERSION
            && receipt.kind == MARKER_TRANSITION_RECEIPT_KIND
            && receipt.algorithm == MARKER_TRANSITION_ALGORITHM
            && receipt.cluster_id == "mainnet-beta",
        "marker transition receipt header is invalid"
    );
    ensure!(
        receipt.archive.is_absolute()
            && matches!(
                receipt.registry_order.as_str(),
                "first_seen" | "usage_sorted"
            )
            && is_sha256(&receipt.old_content_generation_sha256)
            && is_sha256(&receipt.new_content_generation_sha256)
            && receipt.old_content_generation_sha256 != receipt.new_content_generation_sha256
            && receipt.transitioned_unix_secs > 0,
        "marker transition receipt provenance is invalid"
    );
    ensure_exact_marker_free_semantic_file_set(&receipt.old_archive_files)?;
    ensure!(
        direct_generation_digest(
            receipt.epoch,
            &receipt.registry_order,
            &receipt.old_archive_files,
        ) == receipt.old_content_generation_sha256,
        "marker transition receipt old identity is invalid"
    );
    validate_immutable_file_evidence(
        &receipt.authority_attestation,
        "receipt authority attestation",
    )?;
    validate_immutable_file_evidence(&receipt.transition_intent, "receipt transition intent")?;
    validate_neutral_direct_generation_evidence(
        &receipt.neutral_audit_evidence,
        receipt.wire_profile,
    )?;
    let marker = wire_profile_marker(receipt.wire_profile);
    ensure!(
        receipt.marker.name == marker.name
            && receipt.marker.bytes == marker.size
            && receipt.marker.sha256 == marker.sha256
            && receipt.marker.identity.size == marker.size,
        "marker transition receipt marker binding is invalid"
    );
    validate_identity_timestamps(&receipt.marker.identity, "receipt marker")?;
    let mut expected_new = receipt.old_archive_files.clone();
    ensure!(
        expected_new
            .insert(marker.name, receipt.marker.identity.clone())
            .is_none()
            && expected_new == receipt.new_archive_files,
        "marker transition receipt changes data other than the selected marker"
    );
    ensure!(
        direct_generation_digest(
            receipt.epoch,
            &receipt.registry_order,
            &receipt.new_archive_files,
        ) == receipt.new_content_generation_sha256,
        "marker transition receipt new identity is invalid"
    );
    let expected_new_attestation_name = format!(
        "epoch-{}-{}.json",
        receipt.epoch, receipt.new_content_generation_sha256
    );
    let new_attestation_root = receipt
        .new_attestation
        .path
        .parent()
        .context("new attestation path has no parent")?;
    ensure!(
        receipt.new_attestation.path.is_absolute()
            && receipt.new_attestation.bytes > 0
            && receipt.new_attestation.bytes <= MAX_CONTROL_JSON_BYTES
            && is_sha256(&receipt.new_attestation.sha256)
            && receipt
                .new_attestation
                .path
                .file_name()
                .and_then(|name| name.to_str())
                == Some(expected_new_attestation_name.as_str())
            && receipt.authority_attestation.path
                == new_attestation_root.join(format!(
                    "epoch-{}-{}.json",
                    receipt.epoch, receipt.old_content_generation_sha256
                )),
        "marker transition receipt new attestation binding is invalid"
    );
    let expected_intent_name = format!(
        "epoch-{}-{}.intent.json",
        receipt.epoch, receipt.old_content_generation_sha256
    );
    ensure!(
        receipt
            .transition_intent
            .path
            .file_name()
            .and_then(|name| name.to_str())
            == Some(expected_intent_name.as_str()),
        "marker transition receipt intent path is invalid"
    );
    Ok(())
}

fn validate_immutable_file_evidence(evidence: &ImmutableFileEvidence, label: &str) -> Result<()> {
    ensure!(
        evidence.path.is_absolute()
            && evidence.identity.size > 0
            && evidence.identity.device > 0
            && evidence.identity.inode > 0
            && is_sha256(&evidence.sha256),
        "{label} is invalid"
    );
    validate_identity_timestamps(&evidence.identity, label)
}

fn ensure_exact_marker_free_semantic_file_set(
    files: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<()> {
    let expected = DIRECT_SEMANTIC_FILES
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    ensure!(
        files.keys().cloned().collect::<BTreeSet<_>>() == expected,
        "direct generation is not the exact marker-free semantic file set"
    );
    for (name, identity) in files {
        ensure!(
            identity.size > 0 || name == ARCHIVE_V2_SIGNATURES_FILE,
            "direct semantic file is empty: {name}"
        );
        ensure!(
            identity.device > 0 && identity.inode > 0,
            "direct semantic file identity is invalid: {name}"
        );
        validate_identity_timestamps(identity, name)?;
    }
    Ok(())
}

fn validate_identity_timestamps(identity: &RegistryFileIdentity, label: &str) -> Result<()> {
    ensure!(
        (0..1_000_000_000).contains(&identity.modified_nanoseconds)
            && (0..1_000_000_000).contains(&identity.changed_nanoseconds),
        "file identity timestamps are invalid: {label}"
    );
    Ok(())
}

fn pin_direct_semantic_files(archive: &Path) -> Result<SemanticFilePins> {
    let mut files = BTreeMap::new();
    let mut pins = Vec::with_capacity(DIRECT_SEMANTIC_FILES.len());
    for name in DIRECT_SEMANTIC_FILES {
        let path = archive.join(name);
        let before = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect direct semantic file {}", path.display()))?;
        ensure!(
            before.file_type().is_file() && !before.file_type().is_symlink(),
            "direct semantic input is not a real regular file: {name}"
        );
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(&path)
            .with_context(|| format!("open direct semantic file {}", path.display()))?;
        let opened = file.metadata()?;
        let after = fs::symlink_metadata(&path)?;
        ensure!(
            same_file(&before, &opened)
                && same_version(&before, &opened)
                && same_file(&opened, &after)
                && same_version(&opened, &after),
            "direct semantic file changed while it was pinned: {name}"
        );
        let identity = file_identity(&opened);
        ensure!(
            identity.size > 0 || name == ARCHIVE_V2_SIGNATURES_FILE,
            "direct semantic input is empty: {name}"
        );
        files.insert(name.to_owned(), identity.clone());
        pins.push((name.to_owned(), file, identity));
    }
    Ok(SemanticFilePins { files, pins })
}

fn audit_exact_generation(
    options: &MarkerTransitionOptions,
    generation: &str,
    profile: ArchiveV2WireProfile,
) -> Result<FullGenerationWireProfileAudit> {
    let source = PinnedLocalRangeSource::new(&options.archive);
    let reader = ArchiveReader::open_trusted_with_metadata_profile(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".into(),
            epoch: options.epoch,
            generation_id: generation.to_owned(),
            slots_per_epoch: options.slots_per_epoch,
            wire_profile: profile,
        },
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        ReaderOpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..ReaderOpenOptions::default()
        },
    )
    .context("validate marker-bound generation structure")?;
    ensure!(
        reader.index().rows.len() as u64 == reader.metadata_footer().blocks,
        "marker-bound block count differs from its metadata footer"
    );
    ensure!(
        reader.metadata_footer().tx_raw_fallbacks == 0
            && reader.metadata_footer().metadata_raw_fallbacks == 0
            && reader.metadata_footer().decode_errors.is_empty(),
        "marker-bound footer reports raw fallbacks or decode errors"
    );

    let audit = audit_full_generation_wire_profile(&reader, options.max_message_bytes)
        .map_err(|error| anyhow::anyhow!(error))?;
    ensure!(
        audit.raw_transaction_fallbacks == 0,
        "marker-bound generation has raw transaction fallbacks"
    );
    audit
        .require_unproven_authority()
        .map_err(|error| anyhow::anyhow!(error))?;
    source
        .verify_unchanged()
        .context("marker-bound generation changed during semantic recheck")?;
    Ok(audit)
}

fn audit_matches_evidence(
    audit: &FullGenerationWireProfileAudit,
    evidence: &FullGenerationAuditEvidenceV3,
) -> bool {
    use crate::firewatch_wire_profile_attestation::FullGenerationAuditDecisionV3;

    let decision_matches = match evidence.decision {
        FullGenerationAuditDecisionV3::UniqueFullGenerationDecode => {
            matches!(
                audit.require_unproven_authority(),
                Ok(UnprovenWireProfileDecision::UniqueFullGenerationDecode)
            )
        }
        FullGenerationAuditDecisionV3::AllSemanticallyEquivalent => {
            matches!(
                audit.require_unproven_authority(),
                Ok(UnprovenWireProfileDecision::AllSemanticallyEquivalent)
            )
        }
        FullGenerationAuditDecisionV3::ProducerMarker
        | FullGenerationAuditDecisionV3::ProfileBoundReceipt => false,
    };
    decision_matches
        && audit.blocks == evidence.blocks
        && audit.typed_messages == evidence.messages
        && audit.raw_transaction_fallbacks == evidence.raw_transaction_fallbacks
        && audit.selected_only == evidence.alternate_profile_failures
        && audit.both_semantically_equivalent == evidence.both_semantically_equivalent
        && audit.both_semantically_divergent == evidence.both_semantically_divergent
}

fn read_exact_marker_if_present(
    path: &Path,
    expected_bytes: &[u8],
    expected_size: u64,
    expected_sha256: &str,
) -> Result<Option<RegistryFileIdentity>> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
        Ok(metadata) => ensure!(
            metadata.file_type().is_file()
                && !metadata.file_type().is_symlink()
                && metadata.nlink() == 1,
            "wire-profile marker is not an nlink-1 real regular file"
        ),
    }
    let (bytes, pin) = read_pinned_bytes(path, expected_size, "selected wire-profile marker")?;
    ensure!(
        bytes == expected_bytes
            && pin.identity.size == expected_size
            && pin.sha256 == expected_sha256,
        "selected wire-profile marker content is invalid"
    );
    pin.recheck(false, "selected wire-profile marker")?;
    Ok(Some(pin.identity))
}

fn publish_marker_no_replace(path: &Path, bytes: &[u8]) -> Result<RegistryFileIdentity> {
    let parent = path.parent().context("wire-profile marker has no parent")?;
    let basename = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("wire-profile marker name is not UTF-8")?;
    let temp = unique_temp_path(parent, basename);
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o444)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temp)
            .with_context(|| format!("create staged marker {}", temp.display()))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::hard_link(&temp, path).with_context(|| {
            format!(
                "publish wire-profile marker without replacing {}",
                path.display()
            )
        })?;
        sync_directory(parent)?;
        fs::remove_file(&temp)?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
        let _ = sync_directory(parent);
    }
    result?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file()
            && !metadata.file_type().is_symlink()
            && metadata.nlink() == 1,
        "published wire-profile marker is not an nlink-1 regular file"
    );
    Ok(file_identity(&metadata))
}

fn publish_or_validate_exact_json(path: &Path, bytes: &[u8], label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_live_output(path, bytes, label),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            publish_bytes_no_replace(path, bytes, 0o600)?;
            validate_live_output(path, bytes, label)
        }
        Err(error) => Err(error).with_context(|| format!("inspect {label} {}", path.display())),
    }
}

fn publish_bytes_no_replace(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    ensure!(
        bytes.len() as u64 <= MAX_CONTROL_JSON_BYTES,
        "control object is too large"
    );
    let parent = path.parent().context("control object has no parent")?;
    ensure_protected_directory(parent, "control object parent")?;
    let basename = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("control object name is not UTF-8")?;
    let temp = unique_temp_path(parent, basename);
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temp)
            .with_context(|| format!("create staged control object {}", temp.display()))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::hard_link(&temp, path).with_context(|| {
            format!(
                "publish control object without replacing {}",
                path.display()
            )
        })?;
        sync_directory(parent)?;
        fs::remove_file(&temp)?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
        let _ = sync_directory(parent);
    }
    result
}

fn validate_live_output(path: &Path, expected: &[u8], label: &str) -> Result<()> {
    let (bytes, pin) = read_pinned_bytes(path, MAX_CONTROL_JSON_BYTES, label)?;
    ensure!(
        bytes == expected,
        "existing {label} differs from exact bytes"
    );
    pin.recheck(true, label)
}

fn read_optional_pinned_json<T>(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<Option<(T, PinnedFileEvidence)>>
where
    T: DeserializeOwned,
{
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("inspect {label} {}", path.display())),
        Ok(_) => read_pinned_json(path, max_bytes, label).map(Some),
    }
}

fn read_pinned_json<T>(path: &Path, max_bytes: u64, label: &str) -> Result<(T, PinnedFileEvidence)>
where
    T: DeserializeOwned,
{
    let (bytes, pin) = read_pinned_bytes(path, max_bytes, label)?;
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode pinned {label} JSON {}", path.display()))?;
    Ok((value, pin))
}

fn read_pinned_bytes(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<(Vec<u8>, PinnedFileEvidence)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open pinned {label} {}", path.display()))?;
    let opened = file.metadata()?;
    ensure!(
        opened.file_type().is_file() && opened.len() <= max_bytes,
        "pinned {label} is not a bounded regular file"
    );
    let identity = file_identity(&opened);
    let bytes = read_pinned_fd_bytes(&file, max_bytes, label)?;
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        file_identity(&after) == identity
            && path_after.file_type().is_file()
            && !path_after.file_type().is_symlink()
            && file_identity(&path_after) == identity
            && after.len() == bytes.len() as u64,
        "pinned {label} changed during its descriptor read"
    );
    let sha256 = hex_digest(Sha256::digest(&bytes));
    Ok((
        bytes,
        PinnedFileEvidence {
            path: path.to_path_buf(),
            file,
            identity,
            sha256,
            max_bytes,
        },
    ))
}

fn read_pinned_fd_bytes(file: &File, max_bytes: u64, label: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let capacity = max_bytes
        .checked_add(1)
        .context("pinned byte limit overflow")?;
    let mut offset = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    while (bytes.len() as u64) < capacity {
        let remaining = capacity - bytes.len() as u64;
        let limit = usize::try_from(remaining.min(buffer.len() as u64))?;
        let read = loop {
            match file.read_at(&mut buffer[..limit], offset) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => break result,
            }
        }
        .with_context(|| format!("read pinned {label}"))?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        offset = offset
            .checked_add(read as u64)
            .context("pinned file offset overflow")?;
    }
    ensure!(
        bytes.len() as u64 <= max_bytes,
        "pinned {label} is too large"
    );
    Ok(bytes)
}

fn ensure_canonical_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} is not a real directory"
    );
    ensure!(
        fs::canonicalize(path)? == path,
        "{label} path is not canonical"
    );
    Ok(())
}

fn ensure_protected_directory(path: &Path, label: &str) -> Result<()> {
    ensure_canonical_real_directory(path, label)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() } && metadata.permissions().mode() & 0o022 == 0,
        "{label} is not an euid-owned protected directory"
    );
    Ok(())
}

fn ensure_or_create_protected_child(parent: &Path, name: &str, label: &str) -> Result<()> {
    ensure_protected_directory(parent, "protected child parent")?;
    ensure!(
        !name.is_empty() && Path::new(name).components().count() == 1,
        "protected child name is invalid"
    );
    let child = parent.join(name);
    match fs::symlink_metadata(&child) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&child) {
                Ok(()) => sync_directory(parent)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("create {label} {}", child.display()));
                }
            }
        }
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {label} {}", child.display()));
        }
    }
    ensure_protected_directory(&child, label)
}

fn validate_protected_file(metadata: &fs::Metadata, label: &str) -> Result<()> {
    ensure!(
        metadata.file_type().is_file()
            && metadata.nlink() == 1
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o022 == 0,
        "{label} is not an euid-owned protected nlink-1 regular file"
    );
    Ok(())
}

fn ensure_path_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {label} {}", path.display())),
        Ok(_) => bail!("{label} exists: {}", path.display()),
    }
}

fn ensure_no_published_manifest(archive: &Path) -> Result<()> {
    ensure_path_absent(
        &archive.join(GENERATION_MANIFEST_FILE),
        "published generation manifest",
    )
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory for sync {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn unique_temp_path(parent: &Path, basename: &str) -> PathBuf {
    parent.join(format!(
        ".{basename}.tmp.{}.{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn opposite_profile(profile: ArchiveV2WireProfile) -> ArchiveV2WireProfile {
    match profile {
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        }
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        }
    }
}

pub fn direct_generation_digest(
    epoch: u64,
    registry_order: &str,
    files: &BTreeMap<String, RegistryFileIdentity>,
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

fn file_identity(metadata: &fs::Metadata) -> RegistryFileIdentity {
    RegistryFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
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
    use crate::firewatch_wire_profile_attestation::{
        FullGenerationAuditDecisionV3, encode_full_generation_audit_evidence_v3,
    };
    use blockzilla_archive_v2::{
        ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow,
        ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord,
        ArchiveV2HotTxRow, WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        WincodeArchiveV2Footer, WincodeArchiveV2Header, write_archive_v2_hot_block_index,
    };
    use blockzilla_compact::{CompactMessageHeader, OwnedCompactRecentBlockhash};
    use blockzilla_primitives::{CompactPubkey, wincode_leb128_config};
    use blockzilla_registry::KeyIndex;

    fn identity(size: u64, seed: u64) -> RegistryFileIdentity {
        RegistryFileIdentity {
            size,
            device: 1,
            inode: seed,
            modified_seconds: 10,
            modified_nanoseconds: 11,
            changed_seconds: 12,
            changed_nanoseconds: 13,
        }
    }

    fn semantic_files() -> BTreeMap<String, RegistryFileIdentity> {
        DIRECT_SEMANTIC_FILES
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name.to_owned(),
                    identity(
                        if name == ARCHIVE_V2_SIGNATURES_FILE {
                            0
                        } else {
                            index as u64 + 1
                        },
                        index as u64 + 10,
                    ),
                )
            })
            .collect()
    }

    fn neutral_evidence(profile: ArchiveV2WireProfile) -> String {
        let decision = if profile == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 {
            FullGenerationAuditDecisionV3::AllSemanticallyEquivalent
        } else {
            FullGenerationAuditDecisionV3::UniqueFullGenerationDecode
        };
        let (alternate, equivalent) = match decision {
            FullGenerationAuditDecisionV3::UniqueFullGenerationDecode => (1, 0),
            FullGenerationAuditDecisionV3::AllSemanticallyEquivalent => (0, 1),
            _ => unreachable!(),
        };
        encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 1,
            raw_transaction_fallbacks: 0,
            alternate_profile_failures: alternate,
            both_semantically_equivalent: equivalent,
            both_semantically_divergent: 0,
            decision,
        })
        .unwrap()
    }

    fn write_u32_varint(output: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    fn build_marker_free_archive(root: &Path, epoch: u64, slots_per_epoch: u64) {
        let registry_key = [1u8; 32];
        fs::write(root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), registry_key).unwrap();
        KeyIndex::build(vec![registry_key])
            .write(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();

        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
        });
        let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let slot = epoch * slots_per_epoch + 1;
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot,
                parent_slot: slot - 1,
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: Some(1_700_000_000),
                block_height: Some(1),
                rewards: None,
            },
            tx_count: 1,
            tx_rows: vec![ArchiveV2HotTxRow {
                tx_index: 0,
                flags: 0,
                message_offset: 0,
                message_len: message.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            }],
            message_bytes: message,
            metadata_bytes: Vec::new(),
        };
        let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
        fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed.len() as u64,
            1,
            0,
            &[ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            }],
        )
        .unwrap();
        fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), [7u8; 64]).unwrap();

        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: 1,
                transactions: 1,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut metadata = Vec::new();
        for record in records {
            let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_u32_varint(&mut metadata, bytes.len() as u32);
            metadata.extend_from_slice(&bytes);
        }
        fs::write(root.join(ARCHIVE_V2_META_FILE), metadata).unwrap();
    }

    fn valid_intent(profile: ArchiveV2WireProfile) -> MarkerTransitionIntent {
        let epoch = 700;
        let registry_order = "usage_sorted";
        let old_archive_files = semantic_files();
        let old_generation = direct_generation_digest(epoch, registry_order, &old_archive_files);
        let attestation_root = PathBuf::from("/state/wire-profile-attestations");
        let marker = wire_profile_marker(profile);
        MarkerTransitionIntent {
            schema_version: MARKER_TRANSITION_INTENT_SCHEMA_VERSION,
            kind: MARKER_TRANSITION_INTENT_KIND.into(),
            algorithm: MARKER_TRANSITION_ALGORITHM.into(),
            cluster_id: "mainnet-beta".into(),
            epoch,
            archive: PathBuf::from("/archive/epoch-700"),
            registry_order: registry_order.into(),
            wire_profile: profile,
            old_content_generation_sha256: old_generation.clone(),
            old_archive_files,
            authority_attestation: ImmutableFileEvidence {
                path: attestation_root.join(format!("epoch-{epoch}-{old_generation}.json")),
                identity: identity(100, 90),
                sha256: "a".repeat(64),
            },
            neutral_audit_evidence: neutral_evidence(profile),
            marker_name: marker.name,
            marker_bytes: marker.size,
            marker_sha256: marker.sha256,
            new_attestation_root: attestation_root,
            prepared_unix_secs: 1,
        }
    }

    fn valid_receipt(profile: ArchiveV2WireProfile) -> MarkerTransitionReceipt {
        let intent = valid_intent(profile);
        let old_generation = intent.old_content_generation_sha256.clone();
        let marker = wire_profile_marker(profile);
        let marker_identity = identity(marker.size, 100);
        let mut new_archive_files = intent.old_archive_files.clone();
        new_archive_files.insert(marker.name.clone(), marker_identity.clone());
        let new_generation =
            direct_generation_digest(intent.epoch, &intent.registry_order, &new_archive_files);
        MarkerTransitionReceipt {
            schema_version: MARKER_TRANSITION_RECEIPT_SCHEMA_VERSION,
            kind: MARKER_TRANSITION_RECEIPT_KIND.into(),
            algorithm: MARKER_TRANSITION_ALGORITHM.into(),
            cluster_id: "mainnet-beta".into(),
            epoch: intent.epoch,
            archive: intent.archive,
            registry_order: intent.registry_order,
            wire_profile: profile,
            old_content_generation_sha256: intent.old_content_generation_sha256,
            new_content_generation_sha256: new_generation.clone(),
            old_archive_files: intent.old_archive_files,
            new_archive_files,
            authority_attestation: intent.authority_attestation,
            transition_intent: ImmutableFileEvidence {
                path: PathBuf::from(format!(
                    "/state/wire-profile-marker-transitions/epoch-{}-{}.intent.json",
                    intent.epoch, old_generation
                )),
                identity: identity(200, 91),
                sha256: "b".repeat(64),
            },
            marker: MarkerBinding {
                name: marker.name,
                bytes: marker.size,
                sha256: marker.sha256,
                identity: marker_identity,
            },
            neutral_audit_evidence: intent.neutral_audit_evidence,
            new_attestation: ImmutableContentBinding {
                path: intent
                    .new_attestation_root
                    .join(format!("epoch-{}-{new_generation}.json", intent.epoch)),
                bytes: 100,
                sha256: "c".repeat(64),
            },
            transitioned_unix_secs: intent.prepared_unix_secs,
        }
    }

    #[test]
    fn neutral_intent_and_exact_receipt_are_valid() {
        for profile in [
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ] {
            validate_transition_intent(&valid_intent(profile)).unwrap();
            validate_transition_receipt(&valid_receipt(profile)).unwrap();
        }
    }

    #[test]
    fn intent_rejects_a_changed_identity_or_extra_file() {
        let mut changed = valid_intent(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        changed
            .old_archive_files
            .get_mut(ARCHIVE_V2_BLOCKS_FILE)
            .unwrap()
            .inode += 1;
        assert!(validate_transition_intent(&changed).is_err());

        let mut extra = valid_intent(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        extra.old_archive_files.insert(
            wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name,
            identity(1, 200),
        );
        assert!(validate_transition_intent(&extra).is_err());
    }

    #[test]
    fn receipt_rejects_any_change_other_than_the_marker() {
        let mut receipt = valid_receipt(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        receipt
            .new_archive_files
            .get_mut(ARCHIVE_V2_META_FILE)
            .unwrap()
            .inode += 1;
        receipt.new_content_generation_sha256 = direct_generation_digest(
            receipt.epoch,
            &receipt.registry_order,
            &receipt.new_archive_files,
        );
        assert!(validate_transition_receipt(&receipt).is_err());
    }

    #[test]
    fn receipt_rejects_a_wrong_marker_or_attestation_name() {
        let mut wrong_marker =
            valid_receipt(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        wrong_marker.marker.sha256 = "d".repeat(64);
        assert!(validate_transition_receipt(&wrong_marker).is_err());

        let mut wrong_attestation =
            valid_receipt(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        wrong_attestation.new_attestation.path = PathBuf::from("/state/wrong.json");
        assert!(validate_transition_receipt(&wrong_attestation).is_err());
    }

    #[test]
    fn marker_publication_is_exact_durable_and_no_clobber() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        let path = root.join(marker.name);
        let bytes = wire_profile_marker_bytes(profile);
        let identity = publish_marker_no_replace(&path, bytes).unwrap();
        assert_eq!(identity.size, bytes.len() as u64);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(publish_marker_no_replace(&path, bytes).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);

        let alias = root.join("marker-alias");
        fs::hard_link(&path, &alias).unwrap();
        assert!(read_exact_marker_if_present(&path, bytes, marker.size, &marker.sha256).is_err());
    }

    #[test]
    fn exact_json_publication_never_accepts_different_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let path = root.join("receipt.json");
        publish_or_validate_exact_json(&path, b"one", "test receipt").unwrap();
        publish_or_validate_exact_json(&path, b"one", "test receipt").unwrap();
        assert!(publish_or_validate_exact_json(&path, b"two", "test receipt").is_err());
        assert_eq!(fs::read(path).unwrap(), b"one");
    }

    #[test]
    fn published_manifest_makes_in_place_transition_ineligible() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        ensure_no_published_manifest(&root).unwrap();
        fs::write(root.join(GENERATION_MANIFEST_FILE), b"immutable manifest").unwrap();
        assert!(ensure_no_published_manifest(&root).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn marker_reader_rejects_a_symlink() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        let target = root.join("target");
        fs::write(&target, wire_profile_marker_bytes(profile)).unwrap();
        let path = root.join(marker.name);
        symlink(target, &path).unwrap();
        assert!(
            read_exact_marker_if_present(
                &path,
                wire_profile_marker_bytes(profile),
                marker.size,
                &marker.sha256,
            )
            .is_err()
        );
    }

    #[test]
    fn transition_lock_excludes_a_running_controller_peer() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let archive = root.join("archive");
        let state = root.join("state");
        fs::create_dir(&archive).unwrap();
        fs::create_dir(&state).unwrap();
        let archive = fs::canonicalize(archive).unwrap();
        let state = fs::canonicalize(state).unwrap();
        let first = MarkerTransitionLocks::acquire(&archive, &state).unwrap();
        assert!(MarkerTransitionLocks::acquire(&archive, &state).is_err());
        first.recheck_exclusive(&archive, &state).unwrap();
    }

    #[test]
    fn complete_transition_preserves_data_and_is_resumable() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let archive = root.join("archive");
        let state = root.join("state");
        let attestations = state.join(WIRE_PROFILE_ATTESTATIONS_DIR);
        fs::create_dir(&archive).unwrap();
        fs::create_dir(&state).unwrap();
        fs::create_dir(&attestations).unwrap();
        let archive = fs::canonicalize(archive).unwrap();
        let state = fs::canonicalize(state).unwrap();
        let epoch = 7;
        let slots_per_epoch = 100;
        build_marker_free_archive(&archive, epoch, slots_per_epoch);
        let semantic = pin_direct_semantic_files(&archive).unwrap().files;
        let old_generation = direct_generation_digest(epoch, "usage_sorted", &semantic);
        let evidence = neutral_evidence(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        let old_attestation = WireProfileAttestation {
            schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
            kind: WIRE_PROFILE_ATTESTATION_KIND.into(),
            audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
            audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
            cluster_id: "mainnet-beta".into(),
            epoch,
            archive: archive.clone(),
            registry_order: "usage_sorted".into(),
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            content_generation_sha256: old_generation.clone(),
            archive_files: semantic.clone(),
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            evidence,
            attested_unix_secs: 1,
        };
        validate_wire_profile_attestation_structure(&old_attestation).unwrap();
        fs::write(
            attestations.join(format!("epoch-{epoch}-{old_generation}.json")),
            serde_json::to_vec_pretty(&old_attestation).unwrap(),
        )
        .unwrap();

        let mut options = MarkerTransitionOptions::new(
            archive.clone(),
            state.clone(),
            epoch,
            "usage_sorted".into(),
            old_generation.clone(),
        );
        options.slots_per_epoch = slots_per_epoch;
        let locks = MarkerTransitionLocks::acquire(&archive, &state).unwrap();
        let outcome = transition_marker_free_direct_generation(&options, &locks).unwrap();
        assert_ne!(outcome.new_content_generation_sha256, old_generation);
        assert_eq!(
            fs::read(&outcome.marker_path).unwrap(),
            wire_profile_marker_bytes(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
        );
        let receipt: MarkerTransitionReceipt =
            serde_json::from_slice(&fs::read(&outcome.receipt_path).unwrap()).unwrap();
        validate_transition_receipt(&receipt).unwrap();
        let new_attestation: WireProfileAttestation =
            serde_json::from_slice(&fs::read(&outcome.new_attestation_path).unwrap()).unwrap();
        validate_wire_profile_attestation_structure(&new_attestation).unwrap();
        assert_eq!(
            new_attestation.content_generation_sha256,
            outcome.new_content_generation_sha256
        );
        for (name, identity) in semantic {
            assert_eq!(new_attestation.archive_files.get(&name), Some(&identity));
        }

        fs::write(
            archive.join(GENERATION_MANIFEST_FILE),
            b"pre-existing immutable manifest",
        )
        .unwrap();
        assert!(transition_marker_free_direct_generation(&options, &locks).is_err());
        fs::remove_file(archive.join(GENERATION_MANIFEST_FILE)).unwrap();

        let resumed = transition_marker_free_direct_generation(&options, &locks).unwrap();
        assert_eq!(resumed, outcome);
        ensure_no_published_manifest(&archive).unwrap();
    }
}
