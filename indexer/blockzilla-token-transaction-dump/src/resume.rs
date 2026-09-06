//! Durable stage checkpoints for the two-pass raw extractor.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;

use crate::format::{DumpSourceBinding, SourceTransactionCoordinate};

pub const RESUME_CHECKPOINT_FILE: &str = "resume-checkpoint.json";
pub const RESUME_CHECKPOINT_PENDING_FILE: &str = "resume-checkpoint.pending.json";
const RESUME_CHECKPOINT_STAGING_FILE: &str = ".resume-checkpoint.pending.json.partial";
pub const PARTIAL_SHARD_SUFFIX: &str = ".partial";

const CHECKPOINT_SCHEMA_VERSION: u16 = 3;
const CHECKPOINT_KIND: &str = "blockzilla-token-transaction-dump-two-pass-resume";
const CHECKPOINT_HASH_DOMAIN: &[u8] = b"blockzilla-token-transaction-dump/resume/v3\0";
const FILE_HASH_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeIdentity {
    pub dump_schema_version: u16,
    pub mint: String,
    pub mint_slot: u64,
    pub mint_signature: String,
    pub workers: usize,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub cluster_id: String,
    pub slots_per_epoch: u64,
    pub source_binding: DumpSourceBinding,
    /// The field is absent in schema-3 checkpoints made before the fused
    /// reader existed. Its default keeps those checkpoint hashes stable.
    #[serde(default, skip_serializing_if = "ResumeExtractionMode::is_two_pass")]
    pub extraction_mode: ResumeExtractionMode,
    /// A short-lived build wrote this performance option into the identity.
    /// New writers always keep it false. Readers retain it only to authenticate
    /// and resume checkpoints from that build.
    #[serde(default, skip_serializing_if = "is_false")]
    pub single_read_match_hints: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeExtractionMode {
    #[default]
    TwoPass,
    SingleReadBatches,
}

impl ResumeExtractionMode {
    fn is_two_pass(value: &Self) -> bool {
        *value == Self::TwoPass
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeStage {
    Discovery,
    Extraction,
    Complete,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeCounters {
    pub transactions: u64,
    pub anchor_transactions: u64,
    pub blocks_scanned: u64,
    pub transactions_scanned: u64,
    pub owned_block_fallbacks: u64,
}

impl ResumeCounters {
    pub fn checked_add(self, other: Self) -> Result<Self> {
        Ok(Self {
            transactions: checked_sum(self.transactions, other.transactions, "transaction")?,
            anchor_transactions: checked_sum(
                self.anchor_transactions,
                other.anchor_transactions,
                "anchor transaction",
            )?,
            blocks_scanned: checked_sum(
                self.blocks_scanned,
                other.blocks_scanned,
                "scanned block",
            )?,
            transactions_scanned: checked_sum(
                self.transactions_scanned,
                other.transactions_scanned,
                "scanned transaction",
            )?,
            owned_block_fallbacks: checked_sum(
                self.owned_block_fallbacks,
                other.owned_block_fallbacks,
                "owned block fallback",
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeDiscoveryBinding {
    pub epoch: u64,
    pub source_generation_digest: String,
    pub creation_log_sha256: String,
    pub creations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeFrozenAccountBinding {
    pub accounts_sha256: String,
    pub account_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeShardBinding {
    pub epoch: u64,
    pub source_generation_digest: String,
    pub transaction_stream_sha256: String,
    pub account_id_log_sha256: String,
    pub counters: ResumeCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeCheckpointPayload {
    pub schema_version: u16,
    pub kind: String,
    pub identity: ResumeIdentity,
    pub stage: ResumeStage,
    pub discovery_shards: Vec<ResumeDiscoveryBinding>,
    pub frozen_accounts: Option<ResumeFrozenAccountBinding>,
    pub raw_shards: Vec<ResumeShardBinding>,
    pub cumulative: ResumeCounters,
    /// The fused reader records the anchor before it commits the first epoch.
    /// Older two-pass checkpoints omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_position: Option<SourceTransactionCoordinate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeCheckpointEnvelope {
    payload: ResumeCheckpointPayload,
    payload_sha256: String,
}

#[derive(Debug, Clone)]
pub struct LoadedResumeCheckpoint {
    pub payload: ResumeCheckpointPayload,
    pub payload_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeShardLayout {
    pub complete: Vec<(u64, PathBuf)>,
    pub partial: Option<(u64, PathBuf)>,
}

impl ResumeCheckpointPayload {
    pub fn new(
        identity: ResumeIdentity,
        discovery_shards: Vec<ResumeDiscoveryBinding>,
        frozen_accounts: Option<ResumeFrozenAccountBinding>,
        raw_shards: Vec<ResumeShardBinding>,
    ) -> Result<Self> {
        ensure!(
            identity.extraction_mode == ResumeExtractionMode::TwoPass,
            "the two-pass checkpoint constructor received a single-read identity"
        );
        let epoch_count = epoch_count(&identity)?;
        let stage = if raw_shards.len() == epoch_count {
            ResumeStage::Complete
        } else if frozen_accounts.is_some() {
            ResumeStage::Extraction
        } else {
            ResumeStage::Discovery
        };
        let cumulative = raw_shards
            .iter()
            .try_fold(ResumeCounters::default(), |sum, shard| {
                sum.checked_add(shard.counters)
            })?;
        let payload = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            kind: CHECKPOINT_KIND.to_owned(),
            identity,
            stage,
            discovery_shards,
            frozen_accounts,
            raw_shards,
            cumulative,
            anchor_position: None,
        };
        payload.validate(None)?;
        Ok(payload)
    }

    pub fn new_single_read_batches(
        identity: ResumeIdentity,
        anchor_position: Option<SourceTransactionCoordinate>,
        discovery_shards: Vec<ResumeDiscoveryBinding>,
        frozen_accounts: Option<ResumeFrozenAccountBinding>,
        raw_shards: Vec<ResumeShardBinding>,
    ) -> Result<Self> {
        ensure!(
            identity.extraction_mode == ResumeExtractionMode::SingleReadBatches,
            "the single-read checkpoint constructor received a two-pass identity"
        );
        let count = epoch_count(&identity)?;
        let stage = if frozen_accounts.is_some()
            && discovery_shards.len() == count
            && raw_shards.len() == count
        {
            ResumeStage::Complete
        } else {
            ResumeStage::Extraction
        };
        let cumulative = raw_shards
            .iter()
            .try_fold(ResumeCounters::default(), |sum, shard| {
                sum.checked_add(shard.counters)
            })?;
        let payload = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            kind: CHECKPOINT_KIND.to_owned(),
            identity,
            stage,
            discovery_shards,
            frozen_accounts,
            raw_shards,
            cumulative,
            anchor_position,
        };
        payload.validate(None)?;
        Ok(payload)
    }

    pub fn validate(&self, expected_identity: Option<&ResumeIdentity>) -> Result<()> {
        ensure!(
            self.schema_version == CHECKPOINT_SCHEMA_VERSION,
            "resume checkpoint schema is {}, expected {CHECKPOINT_SCHEMA_VERSION}",
            self.schema_version
        );
        ensure!(
            self.kind == CHECKPOINT_KIND,
            "resume checkpoint kind differs"
        );
        validate_identity(&self.identity)?;
        if let Some(expected) = expected_identity {
            ensure!(
                resume_identities_are_compatible(&self.identity, expected),
                "resume checkpoint target or source identity differs from this run"
            );
        }
        let count = epoch_count(&self.identity)?;
        validate_epoch_prefix(
            &self.discovery_shards,
            self.identity.first_epoch,
            count,
            |binding| binding.epoch,
            "discovery",
        )?;
        validate_epoch_prefix(
            &self.raw_shards,
            self.identity.first_epoch,
            count,
            |binding| binding.epoch,
            "raw shard",
        )?;
        for binding in &self.discovery_shards {
            parse_hex_digest(
                &binding.source_generation_digest,
                "source generation digest",
            )?;
            parse_hex_digest(&binding.creation_log_sha256, "creation log digest")?;
        }
        for binding in &self.raw_shards {
            parse_hex_digest(
                &binding.source_generation_digest,
                "source generation digest",
            )?;
            parse_hex_digest(
                &binding.transaction_stream_sha256,
                "transaction stream digest",
            )?;
            parse_hex_digest(&binding.account_id_log_sha256, "account ID log digest")?;
        }
        if let Some(frozen) = &self.frozen_accounts {
            parse_hex_digest(&frozen.accounts_sha256, "frozen account list digest")?;
        }
        ensure!(
            self.cumulative
                == self
                    .raw_shards
                    .iter()
                    .try_fold(ResumeCounters::default(), |sum, shard| sum
                        .checked_add(shard.counters),)?,
            "resume checkpoint counters differ from raw shards"
        );
        if self.identity.extraction_mode == ResumeExtractionMode::SingleReadBatches {
            if self.raw_shards.is_empty() {
                ensure!(
                    self.anchor_position.is_none() && self.cumulative.anchor_transactions == 0,
                    "empty single-read checkpoint contains an anchor"
                );
            } else {
                let anchor = self
                    .anchor_position
                    .context("nonempty single-read checkpoint has no anchor")?;
                ensure!(
                    anchor.epoch == self.identity.first_epoch
                        && anchor.slot == self.identity.mint_slot
                        && anchor.signature_count != 0,
                    "single-read checkpoint anchor differs from the run floor"
                );
                ensure!(
                    self.cumulative.anchor_transactions == 1,
                    "nonempty single-read checkpoint does not bind one raw anchor transaction"
                );
            }
        }
        match self.identity.extraction_mode {
            ResumeExtractionMode::TwoPass => match self.stage {
                ResumeStage::Discovery => ensure!(
                    self.anchor_position.is_none()
                        && self.frozen_accounts.is_none()
                        && self.raw_shards.is_empty(),
                    "discovery-stage checkpoint has frozen, anchor, or raw artifacts"
                ),
                ResumeStage::Extraction => ensure!(
                    self.anchor_position.is_none()
                        && self.discovery_shards.len() == count
                        && self.frozen_accounts.is_some()
                        && self.raw_shards.len() < count,
                    "extraction-stage checkpoint is incomplete or inconsistent"
                ),
                ResumeStage::Complete => ensure!(
                    self.anchor_position.is_none()
                        && self.discovery_shards.len() == count
                        && self.frozen_accounts.is_some()
                        && self.raw_shards.len() == count,
                    "complete checkpoint does not bind every artifact"
                ),
            },
            ResumeExtractionMode::SingleReadBatches => match self.stage {
                ResumeStage::Discovery => {
                    anyhow::bail!("single-read checkpoints do not use the discovery-only stage")
                }
                ResumeStage::Extraction => ensure!(
                    self.frozen_accounts.is_none()
                        && self.discovery_shards.len() == self.raw_shards.len()
                        && self.raw_shards.len() <= count
                        && (self.raw_shards.is_empty() || self.anchor_position.is_some()),
                    "single-read extraction checkpoint is incomplete or inconsistent"
                ),
                ResumeStage::Complete => ensure!(
                    self.discovery_shards.len() == count
                        && self.raw_shards.len() == count
                        && self.frozen_accounts.is_some()
                        && self.anchor_position.is_some(),
                    "complete single-read checkpoint does not bind every artifact"
                ),
            },
        }
        Ok(())
    }

    pub fn validate_artifacts(
        &self,
        discovery: &[ResumeDiscoveryBinding],
        frozen: Option<&ResumeFrozenAccountBinding>,
        raw: &[ResumeShardBinding],
    ) -> Result<()> {
        ensure!(
            self.discovery_shards == discovery,
            "resume discovery bindings differ from validated artifacts"
        );
        ensure!(
            self.frozen_accounts.as_ref() == frozen,
            "resume frozen-account binding differs from validated artifact"
        );
        ensure!(
            self.raw_shards == raw,
            "resume raw-shard bindings differ from validated artifacts"
        );
        Ok(())
    }

    /// Check that a committed checkpoint is not ahead of the immutable artifacts that were
    /// validated during recovery. The validated artifacts can be newer than the checkpoint when
    /// a process stopped after an artifact rename and before its checkpoint rename.
    pub fn validate_artifact_prefixes(
        &self,
        discovery: &[ResumeDiscoveryBinding],
        frozen: Option<&ResumeFrozenAccountBinding>,
        raw: &[ResumeShardBinding],
    ) -> Result<()> {
        ensure!(
            discovery.starts_with(&self.discovery_shards),
            "resume checkpoint discovery bindings are ahead of or differ from validated artifacts"
        );
        if let Some(checkpoint_frozen) = self.frozen_accounts.as_ref() {
            ensure!(
                frozen == Some(checkpoint_frozen),
                "resume checkpoint frozen-account binding differs from validated artifact"
            );
        }
        ensure!(
            raw.starts_with(&self.raw_shards),
            "resume checkpoint raw-shard bindings are ahead of or differ from validated artifacts"
        );
        let discovery_delta = discovery.len() - self.discovery_shards.len();
        let raw_delta = raw.len() - self.raw_shards.len();
        let frozen_added = self.frozen_accounts.is_none() && frozen.is_some();
        let valid_delta = match self.identity.extraction_mode {
            ResumeExtractionMode::TwoPass => {
                (discovery_delta == 0 && raw_delta == 0 && !frozen_added)
                    || (discovery_delta == 1 && raw_delta == 0 && !frozen_added)
                    || (discovery_delta == 0 && raw_delta == 0 && frozen_added)
                    || (discovery_delta == 0 && raw_delta == 1 && !frozen_added)
            }
            ResumeExtractionMode::SingleReadBatches => {
                (discovery_delta == 0 && raw_delta == 0 && !frozen_added)
                    || (discovery_delta == 1 && raw_delta == 1 && !frozen_added)
                    || (discovery_delta == 0 && raw_delta == 0 && frozen_added)
            }
        };
        ensure!(
            valid_delta,
            "validated artifacts are more than one atomic step ahead of the committed checkpoint"
        );
        Ok(())
    }
}

fn resume_identities_are_compatible(
    checkpoint: &ResumeIdentity,
    expected: &ResumeIdentity,
) -> bool {
    let mut checkpoint = checkpoint.clone();
    let mut expected = expected.clone();
    checkpoint.single_read_match_hints = false;
    expected.single_read_match_hints = false;
    checkpoint == expected
}

pub fn checkpoint_path(root: &Path) -> PathBuf {
    root.join(RESUME_CHECKPOINT_FILE)
}

pub fn pending_checkpoint_path(root: &Path) -> PathBuf {
    root.join(RESUME_CHECKPOINT_PENDING_FILE)
}

pub fn pending_checkpoint_staging_path(root: &Path) -> PathBuf {
    root.join(RESUME_CHECKPOINT_STAGING_FILE)
}

pub fn partial_shard_path(shard_root: &Path, epoch: u64) -> PathBuf {
    shard_root.join(format!("epoch-{epoch}{PARTIAL_SHARD_SUFFIX}"))
}

pub fn partial_artifact_file_path(root: &Path, file_name: &str) -> Result<PathBuf> {
    validate_artifact_file_name(file_name)?;
    Ok(root.join(format!("{file_name}{PARTIAL_SHARD_SUFFIX}")))
}

pub fn load_resume_checkpoint(
    root: &Path,
    expected_identity: &ResumeIdentity,
) -> Result<Option<LoadedResumeCheckpoint>> {
    load_checkpoint_file(&checkpoint_path(root), expected_identity)
}

pub fn load_pending_resume_checkpoint(
    root: &Path,
    expected_identity: &ResumeIdentity,
) -> Result<Option<LoadedResumeCheckpoint>> {
    load_checkpoint_file(&pending_checkpoint_path(root), expected_identity)
}

pub fn write_resume_checkpoint_atomic(
    root: &Path,
    payload: &ResumeCheckpointPayload,
) -> Result<LoadedResumeCheckpoint> {
    stage_resume_checkpoint(root, payload)?;
    promote_pending_resume_checkpoint(root, &payload.identity)
}

pub fn stage_resume_checkpoint(root: &Path, payload: &ResumeCheckpointPayload) -> Result<()> {
    ensure_direct_directory(root, "extraction output")?;
    let path = pending_checkpoint_path(root);
    let bytes = checkpoint_bytes(payload)?;
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file(),
                "pending resume checkpoint is not a regular file"
            );
            ensure!(
                fs::read(&path)? == bytes,
                "a different pending resume checkpoint already exists"
            );
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let staging = pending_checkpoint_staging_path(root);
    match fs::symlink_metadata(&staging) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file(),
                "staged resume checkpoint is not a regular file"
            );
            if fs::read(&staging)? == bytes {
                fs::rename(&staging, &path)?;
                sync_directory(root)?;
                return Ok(());
            }
            quarantine_pending_checkpoint_staging(root)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .with_context(|| format!("create {}", staging.display()))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&staging, &path)?;
    sync_directory(root)?;
    Ok(())
}

pub fn promote_pending_resume_checkpoint(
    root: &Path,
    expected_identity: &ResumeIdentity,
) -> Result<LoadedResumeCheckpoint> {
    let pending = load_pending_resume_checkpoint(root, expected_identity)?
        .context("no pending resume checkpoint exists")?;
    let source = pending_checkpoint_path(root);
    let target = checkpoint_path(root);
    match fs::symlink_metadata(&target) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_file(),
            "committed resume checkpoint is not a regular file"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("inspect {}", target.display())),
    }
    fs::rename(&source, &target)?;
    sync_directory(root)?;
    Ok(pending)
}

pub fn discover_resume_shard_layout(
    shard_root: &Path,
    first_epoch: u64,
) -> Result<ResumeShardLayout> {
    ensure_direct_directory(shard_root, "artifact root")?;
    let mut complete = Vec::new();
    let mut partial = None;
    for entry in fs::read_dir(shard_root)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("artifact root contains a non-UTF-8 name"))?;
        let file_type = entry.file_type()?;
        if name.starts_with(".abandoned-") {
            ensure!(file_type.is_dir(), "abandoned artifact is not a directory");
            continue;
        }
        ensure!(file_type.is_dir(), "artifact is not a direct directory");
        let epoch_text = name
            .strip_prefix("epoch-")
            .with_context(|| format!("unexpected artifact directory {name}"))?;
        if let Some(epoch_text) = epoch_text.strip_suffix(PARTIAL_SHARD_SUFFIX) {
            let epoch = parse_canonical_epoch(epoch_text, &name)?;
            ensure!(partial.is_none(), "more than one partial artifact exists");
            partial = Some((epoch, entry.path()));
        } else {
            let epoch = parse_canonical_epoch(epoch_text, &name)?;
            complete.push((epoch, entry.path()));
        }
    }
    complete.sort_unstable_by_key(|(epoch, _)| *epoch);
    for (index, (epoch, _)) in complete.iter().enumerate() {
        let expected = first_epoch
            .checked_add(u64::try_from(index)?)
            .context("artifact epoch overflow")?;
        ensure!(*epoch == expected, "artifact prefix has an epoch gap");
    }
    if let Some((epoch, _)) = partial {
        let expected = first_epoch
            .checked_add(u64::try_from(complete.len())?)
            .context("partial artifact epoch overflow")?;
        ensure!(epoch == expected, "partial artifact is not the next epoch");
    }
    Ok(ResumeShardLayout { complete, partial })
}

pub fn create_partial_shard_directory(shard_root: &Path, epoch: u64) -> Result<PathBuf> {
    ensure_direct_directory(shard_root, "artifact root")?;
    ensure_absent_path(&shard_root.join(format!("epoch-{epoch}")))?;
    let partial = partial_shard_path(shard_root, epoch);
    fs::create_dir(&partial)?;
    sync_directory(shard_root)?;
    Ok(partial)
}

pub fn commit_partial_shard(shard_root: &Path, epoch: u64) -> Result<PathBuf> {
    let partial = partial_shard_path(shard_root, epoch);
    ensure_direct_directory(&partial, "partial artifact")?;
    let complete = shard_root.join(format!("epoch-{epoch}"));
    ensure_absent_path(&complete)?;
    fs::rename(&partial, &complete)?;
    sync_directory(shard_root)?;
    Ok(complete)
}

pub fn quarantine_partial_shard(shard_root: &Path, epoch: u64) -> Result<Option<PathBuf>> {
    let partial = partial_shard_path(shard_root, epoch);
    match fs::symlink_metadata(&partial) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => ensure!(metadata.file_type().is_dir(), "partial is not a directory"),
        Err(error) => return Err(error.into()),
    }
    let quarantine =
        unique_quarantine_path(shard_root, &format!(".abandoned-epoch-{epoch}-partial"), "")?;
    fs::rename(&partial, &quarantine)?;
    sync_directory(shard_root)?;
    Ok(Some(quarantine))
}

/// Preserve one complete shard that has no matching artifact in another fused
/// output lane. This is used only to roll back an interrupted two-directory
/// epoch commit. The bytes are renamed, never deleted.
pub fn quarantine_complete_shard(shard_root: &Path, epoch: u64) -> Result<Option<PathBuf>> {
    let complete = shard_root.join(format!("epoch-{epoch}"));
    match fs::symlink_metadata(&complete) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir(),
            "complete shard is not a directory"
        ),
        Err(error) => return Err(error.into()),
    }
    let quarantine = unique_quarantine_path(
        shard_root,
        &format!(".abandoned-epoch-{epoch}-complete"),
        "",
    )?;
    fs::rename(&complete, &quarantine)?;
    sync_directory(shard_root)?;
    Ok(Some(quarantine))
}

/// Write a root artifact to a new partial file. The caller must validate the bytes before it
/// promotes the file with [`commit_partial_artifact_file`].
pub fn create_partial_artifact_file(root: &Path, file_name: &str, bytes: &[u8]) -> Result<PathBuf> {
    ensure_direct_directory(root, "artifact root")?;
    validate_artifact_file_name(file_name)?;
    ensure_absent_path(&root.join(file_name))?;
    let partial = partial_artifact_file_path(root, file_name)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)
        .with_context(|| format!("create partial artifact {}", partial.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write partial artifact {}", partial.display()))?;
    file.sync_all()
        .with_context(|| format!("sync partial artifact {}", partial.display()))?;
    sync_directory(root)?;
    Ok(partial)
}

pub fn commit_partial_artifact_file(root: &Path, file_name: &str) -> Result<PathBuf> {
    ensure_direct_directory(root, "artifact root")?;
    validate_artifact_file_name(file_name)?;
    let partial = partial_artifact_file_path(root, file_name)?;
    ensure_regular_file(&partial, "partial artifact")?;
    let complete = root.join(file_name);
    ensure_absent_path(&complete)?;
    fs::rename(&partial, &complete)?;
    sync_directory(root)?;
    Ok(complete)
}

pub fn quarantine_partial_artifact_file(root: &Path, file_name: &str) -> Result<Option<PathBuf>> {
    ensure_direct_directory(root, "artifact root")?;
    validate_artifact_file_name(file_name)?;
    let partial = partial_artifact_file_path(root, file_name)?;
    match fs::symlink_metadata(&partial) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => ensure!(
            metadata.file_type().is_file(),
            "partial artifact is not a regular file"
        ),
        Err(error) => return Err(error.into()),
    }
    let quarantine = unique_quarantine_path(root, &format!(".abandoned-{file_name}-partial"), "")?;
    fs::rename(&partial, &quarantine)?;
    sync_directory(root)?;
    Ok(Some(quarantine))
}

pub fn quarantine_pending_resume_checkpoint(root: &Path) -> Result<Option<PathBuf>> {
    let path = pending_checkpoint_path(root);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => ensure!(
            metadata.file_type().is_file(),
            "pending is not a regular file"
        ),
        Err(error) => return Err(error.into()),
    }
    let quarantine = unique_quarantine_path(root, ".abandoned-resume-checkpoint", ".json")?;
    fs::rename(&path, &quarantine)?;
    sync_directory(root)?;
    Ok(Some(quarantine))
}

pub fn quarantine_pending_checkpoint_staging(root: &Path) -> Result<Option<PathBuf>> {
    let path = pending_checkpoint_staging_path(root);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => ensure!(
            metadata.file_type().is_file(),
            "staged pending checkpoint is not a regular file"
        ),
        Err(error) => return Err(error.into()),
    }
    let quarantine = unique_quarantine_path(root, ".abandoned-resume-checkpoint-staging", ".json")?;
    fs::rename(&path, &quarantine)?;
    sync_directory(root)?;
    Ok(Some(quarantine))
}

pub fn sha256_regular_file(path: &Path) -> Result<[u8; 32]> {
    ensure_regular_file(path, "artifact")?;
    let mut reader = BufReader::with_capacity(FILE_HASH_BUFFER_BYTES, File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; FILE_HASH_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

pub fn digest_hex(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn checkpoint_bytes(payload: &ResumeCheckpointPayload) -> Result<Vec<u8>> {
    payload.validate(None)?;
    let canonical = serde_json::to_vec(payload)?;
    let digest = domain_digest(&canonical);
    serde_json::to_vec_pretty(&ResumeCheckpointEnvelope {
        payload: payload.clone(),
        payload_sha256: digest_hex(digest),
    })
    .context("encode resume checkpoint")
}

fn load_checkpoint_file(
    path: &Path,
    expected_identity: &ResumeIdentity,
) -> Result<Option<LoadedResumeCheckpoint>> {
    let metadata = match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => metadata,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.file_type().is_file(),
        "checkpoint is not a regular file"
    );
    ensure!(
        metadata.len() <= MAX_CHECKPOINT_BYTES,
        "checkpoint is too large"
    );
    let envelope: ResumeCheckpointEnvelope = serde_json::from_slice(&fs::read(path)?)?;
    let canonical = serde_json::to_vec(&envelope.payload)?;
    let digest = domain_digest(&canonical);
    ensure!(
        parse_hex_digest(&envelope.payload_sha256, "checkpoint digest")? == digest,
        "resume checkpoint payload digest mismatch"
    );
    envelope.payload.validate(Some(expected_identity))?;
    Ok(Some(LoadedResumeCheckpoint {
        payload: envelope.payload,
        payload_sha256: digest,
    }))
}

fn validate_identity(identity: &ResumeIdentity) -> Result<()> {
    Pubkey::from_str(&identity.mint).context("parse checkpoint mint")?;
    let signature = bs58::decode(&identity.mint_signature).into_vec()?;
    ensure!(
        signature.len() == 64,
        "checkpoint mint signature is not 64 bytes"
    );
    ensure!((1..=64).contains(&identity.workers), "invalid worker count");
    ensure!(
        identity.first_epoch <= identity.last_epoch,
        "reversed epoch range"
    );
    ensure!(!identity.cluster_id.is_empty(), "empty cluster ID");
    ensure!(identity.slots_per_epoch != 0, "zero slots per epoch");
    Ok(())
}

fn epoch_count(identity: &ResumeIdentity) -> Result<usize> {
    let count = identity
        .last_epoch
        .checked_sub(identity.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("resume epoch count overflow")?;
    usize::try_from(count).context("resume epoch count exceeds usize")
}

fn validate_epoch_prefix<T>(
    rows: &[T],
    first_epoch: u64,
    maximum: usize,
    epoch: impl Fn(&T) -> u64,
    label: &str,
) -> Result<()> {
    ensure!(rows.len() <= maximum, "too many {label} artifacts");
    for (index, row) in rows.iter().enumerate() {
        let expected = first_epoch
            .checked_add(u64::try_from(index)?)
            .context("checkpoint epoch overflow")?;
        ensure!(epoch(row) == expected, "{label} artifact prefix has a gap");
    }
    Ok(())
}

fn parse_hex_digest(value: &str, label: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "{label} is not 64 hex digits");
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(output)
}

fn checked_sum(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .with_context(|| format!("resume {label} count overflow"))
}

fn domain_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CHECKPOINT_HASH_DOMAIN);
    hasher.update(bytes);
    hasher.finalize().into()
}

fn parse_canonical_epoch(value: &str, name: &str) -> Result<u64> {
    let epoch = value.parse::<u64>()?;
    let expected = if name.ends_with(PARTIAL_SHARD_SUFFIX) {
        format!("epoch-{epoch}{PARTIAL_SHARD_SUFFIX}")
    } else {
        format!("epoch-{epoch}")
    };
    ensure!(name == expected, "non-canonical epoch artifact name");
    Ok(epoch)
}

fn ensure_direct_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir(),
        "{label} is not a direct directory"
    );
    Ok(())
}

fn ensure_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "{label} is not a regular file"
    );
    Ok(())
}

fn ensure_absent_path(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => anyhow::bail!("path already exists at {}", path.display()),
        Err(error) => Err(error.into()),
    }
}

fn validate_artifact_file_name(file_name: &str) -> Result<()> {
    let path = Path::new(file_name);
    ensure!(
        !file_name.is_empty() && path.file_name().and_then(|name| name.to_str()) == Some(file_name),
        "artifact file name is not one direct UTF-8 name"
    );
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn unique_quarantine_path(root: &Path, stem: &str, suffix: &str) -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    for counter in 0..=u16::MAX {
        let path = root.join(format!("{stem}-{timestamp}-{counter}{suffix}"));
        if fs::symlink_metadata(&path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Ok(path);
        }
    }
    anyhow::bail!("cannot allocate quarantine path")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::DumpWireProfile;
    use tempfile::tempdir;

    fn identity() -> ResumeIdentity {
        ResumeIdentity {
            dump_schema_version: 3,
            mint: bs58::encode([9; 32]).into_string(),
            mint_slot: crate::format::SPYX_MINT_SLOT,
            mint_signature: crate::format::SPYX_MINT_SIGNATURE.to_owned(),
            workers: 12,
            first_epoch: 801,
            last_epoch: 802,
            cluster_id: "mainnet-beta".to_owned(),
            slots_per_epoch: 432_000,
            source_binding: DumpSourceBinding::TrustedLocalSizesOnly {
                cluster_id: "mainnet-beta".to_owned(),
                slots_per_epoch: 432_000,
                wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            },
            extraction_mode: ResumeExtractionMode::TwoPass,
            single_read_match_hints: false,
        }
    }

    fn discovery(epoch: u64) -> ResumeDiscoveryBinding {
        ResumeDiscoveryBinding {
            epoch,
            source_generation_digest: digest_hex([epoch as u8; 32]),
            creation_log_sha256: digest_hex([epoch as u8 + 1; 32]),
            creations: 2,
        }
    }

    fn shard(epoch: u64) -> ResumeShardBinding {
        ResumeShardBinding {
            epoch,
            source_generation_digest: digest_hex([epoch as u8; 32]),
            transaction_stream_sha256: digest_hex([epoch as u8 + 2; 32]),
            account_id_log_sha256: digest_hex([epoch as u8 + 3; 32]),
            counters: ResumeCounters {
                transactions: 5,
                anchor_transactions: u64::from(epoch == 801),
                blocks_scanned: 10,
                transactions_scanned: 100,
                owned_block_fallbacks: 0,
            },
        }
    }

    #[test]
    fn stage_invariants_are_enforced() {
        let discovery_checkpoint =
            ResumeCheckpointPayload::new(identity(), vec![discovery(801)], None, Vec::new())
                .unwrap();
        assert_eq!(discovery_checkpoint.stage, ResumeStage::Discovery);

        let discovery_complete_checkpoint = ResumeCheckpointPayload::new(
            identity(),
            vec![discovery(801), discovery(802)],
            None,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(discovery_complete_checkpoint.stage, ResumeStage::Discovery);

        let frozen = ResumeFrozenAccountBinding {
            accounts_sha256: digest_hex([3; 32]),
            account_count: 2,
        };
        let extraction = ResumeCheckpointPayload::new(
            identity(),
            vec![discovery(801), discovery(802)],
            Some(frozen.clone()),
            vec![shard(801)],
        )
        .unwrap();
        assert_eq!(extraction.stage, ResumeStage::Extraction);
        let complete = ResumeCheckpointPayload::new(
            identity(),
            vec![discovery(801), discovery(802)],
            Some(frozen),
            vec![shard(801), shard(802)],
        )
        .unwrap();
        assert_eq!(complete.stage, ResumeStage::Complete);
    }

    #[test]
    fn checkpoint_can_lag_validated_artifacts_but_cannot_lead_them() {
        let checkpoint =
            ResumeCheckpointPayload::new(identity(), vec![discovery(801)], None, Vec::new())
                .unwrap();
        checkpoint
            .validate_artifact_prefixes(&[discovery(801), discovery(802)], None, &[])
            .unwrap();

        assert!(
            ResumeCheckpointPayload::new(
                identity(),
                vec![discovery(801), discovery(802)],
                None,
                Vec::new(),
            )
            .unwrap()
            .validate_artifact_prefixes(&[discovery(801)], None, &[])
            .is_err()
        );

        let all_discovery = ResumeCheckpointPayload::new(
            identity(),
            vec![discovery(801), discovery(802)],
            None,
            Vec::new(),
        )
        .unwrap();
        let frozen = ResumeFrozenAccountBinding {
            accounts_sha256: digest_hex([3; 32]),
            account_count: 2,
        };
        all_discovery
            .validate_artifact_prefixes(&[discovery(801), discovery(802)], Some(&frozen), &[])
            .unwrap();
        assert!(
            all_discovery
                .validate_artifact_prefixes(
                    &[discovery(801), discovery(802)],
                    Some(&frozen),
                    &[shard(801)],
                )
                .is_err()
        );
    }

    #[test]
    fn checkpoint_round_trip_detects_tampering() {
        let directory = tempdir().unwrap();
        let payload =
            ResumeCheckpointPayload::new(identity(), vec![discovery(801)], None, Vec::new())
                .unwrap();
        write_resume_checkpoint_atomic(directory.path(), &payload).unwrap();
        let loaded = load_resume_checkpoint(directory.path(), &identity())
            .unwrap()
            .unwrap();
        assert_eq!(loaded.payload, payload);

        let path = checkpoint_path(directory.path());
        let mut bytes = fs::read(&path).unwrap();
        let index = bytes.iter().position(|byte| *byte == b'2').unwrap();
        bytes[index] = b'4';
        fs::write(&path, bytes).unwrap();
        assert!(load_resume_checkpoint(directory.path(), &identity()).is_err());
    }

    #[test]
    fn partial_artifacts_are_quarantined_not_deleted() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("epoch-801.partial")).unwrap();
        fs::write(directory.path().join("epoch-801.partial/kept"), b"bytes").unwrap();
        let quarantined = quarantine_partial_shard(directory.path(), 801)
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(quarantined.join("kept")).unwrap(), b"bytes");
    }

    #[test]
    fn partial_root_files_are_validated_promoted_or_quarantined() {
        let directory = tempdir().unwrap();
        let partial =
            create_partial_artifact_file(directory.path(), "accounts.wincode", b"account bytes")
                .unwrap();
        assert_eq!(fs::read(&partial).unwrap(), b"account bytes");
        let complete = commit_partial_artifact_file(directory.path(), "accounts.wincode").unwrap();
        assert_eq!(fs::read(complete).unwrap(), b"account bytes");

        create_partial_artifact_file(directory.path(), "manifest.json", b"partial manifest")
            .unwrap();
        let quarantined = quarantine_partial_artifact_file(directory.path(), "manifest.json")
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(quarantined).unwrap(), b"partial manifest");
        assert!(partial_artifact_file_path(directory.path(), "../escape").is_err());
    }

    #[test]
    fn source_binding_is_part_of_identity() {
        let directory = tempdir().unwrap();
        let payload =
            ResumeCheckpointPayload::new(identity(), vec![discovery(801)], None, Vec::new())
                .unwrap();
        write_resume_checkpoint_atomic(directory.path(), &payload).unwrap();
        let mut changed = identity();
        changed.source_binding = DumpSourceBinding::TrustedLocalSizesOnly {
            cluster_id: "testnet".to_owned(),
            slots_per_epoch: 432_000,
            wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
        };
        assert!(load_resume_checkpoint(directory.path(), &changed).is_err());
    }

    #[test]
    fn corrected_initialize_mint_anchor_rejects_the_old_resume_identity() {
        let directory = tempdir().unwrap();
        let payload =
            ResumeCheckpointPayload::new(identity(), vec![discovery(801)], None, Vec::new())
                .unwrap();
        write_resume_checkpoint_atomic(directory.path(), &payload).unwrap();

        let mut old_anchor = identity();
        old_anchor.mint_slot = 346_330_505;
        old_anchor.mint_signature =
            "2xCKWC2Q7My3bZRcqjXUiU5EbFud1RgAJJpdUJ9cA1FzEfwFgzNHuADZV3J1xCpQ5bENE2drpLQZbyRBKh1gmLvF"
                .to_owned();
        assert!(load_resume_checkpoint(directory.path(), &old_anchor).is_err());
    }

    #[test]
    fn resume_identity_keeps_performance_options_out_of_the_checkpoint() {
        let directory = tempdir().unwrap();
        let mut baseline = identity();
        baseline.extraction_mode = ResumeExtractionMode::SingleReadBatches;
        assert!(
            serde_json::to_value(&baseline)
                .unwrap()
                .get("single_read_match_hints")
                .is_none(),
            "the false default must stay absent for old checkpoint hash compatibility"
        );
        let payload = ResumeCheckpointPayload::new_single_read_batches(
            baseline.clone(),
            None,
            Vec::new(),
            None,
            Vec::new(),
        )
        .unwrap();
        write_resume_checkpoint_atomic(directory.path(), &payload).unwrap();
        let loaded = load_resume_checkpoint(directory.path(), &baseline)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.payload.identity, baseline);
        assert_eq!(
            loaded.payload_sha256,
            domain_digest(&serde_json::to_vec(&payload).unwrap())
        );

        let legacy_directory = tempdir().unwrap();
        let mut legacy_payload = payload.clone();
        legacy_payload.identity.single_read_match_hints = true;
        write_resume_checkpoint_atomic(legacy_directory.path(), &legacy_payload).unwrap();
        let legacy_json: serde_json::Value =
            serde_json::from_slice(&fs::read(checkpoint_path(legacy_directory.path())).unwrap())
                .unwrap();
        assert_eq!(
            legacy_json["payload"]["identity"]["single_read_match_hints"], true,
            "the compatibility fixture must use the temporary hinted identity"
        );
        let legacy_loaded = load_resume_checkpoint(legacy_directory.path(), &baseline)
            .unwrap()
            .unwrap();
        assert!(legacy_loaded.payload.identity.single_read_match_hints);

        let mut different_mint = baseline.clone();
        different_mint.mint = bs58::encode([7; 32]).into_string();
        assert!(load_resume_checkpoint(legacy_directory.path(), &different_mint).is_err());
        let mut different_range = baseline.clone();
        different_range.last_epoch += 1;
        assert!(load_resume_checkpoint(legacy_directory.path(), &different_range).is_err());
        let mut different_mode = baseline;
        different_mode.extraction_mode = ResumeExtractionMode::TwoPass;
        assert!(load_resume_checkpoint(legacy_directory.path(), &different_mode).is_err());
    }

    #[test]
    fn single_read_checkpoint_requires_paired_epochs_and_binds_the_anchor() {
        let mut single_identity = identity();
        single_identity.extraction_mode = ResumeExtractionMode::SingleReadBatches;
        let anchor = SourceTransactionCoordinate {
            epoch: 801,
            slot: 346_066_298,
            source_block_id: 1,
            tx_index: 2,
            source_first_signature_ordinal: 3,
            signature_count: 1,
        };
        let checkpoint = ResumeCheckpointPayload::new_single_read_batches(
            single_identity.clone(),
            Some(anchor),
            vec![discovery(801)],
            None,
            vec![shard(801)],
        )
        .unwrap();
        assert_eq!(checkpoint.stage, ResumeStage::Extraction);
        assert_eq!(checkpoint.anchor_position, Some(anchor));
        checkpoint
            .validate_artifact_prefixes(
                &[discovery(801), discovery(802)],
                None,
                &[shard(801), shard(802)],
            )
            .unwrap();

        assert!(
            ResumeCheckpointPayload::new_single_read_batches(
                single_identity.clone(),
                Some(anchor),
                vec![discovery(801)],
                None,
                Vec::new(),
            )
            .is_err()
        );
        assert!(
            ResumeCheckpointPayload::new_single_read_batches(
                single_identity.clone(),
                Some(SourceTransactionCoordinate {
                    slot: anchor.slot + 1,
                    ..anchor
                }),
                vec![discovery(801)],
                None,
                vec![shard(801)],
            )
            .is_err()
        );
        let mut no_raw_anchor = shard(801);
        no_raw_anchor.counters.anchor_transactions = 0;
        assert!(
            ResumeCheckpointPayload::new_single_read_batches(
                single_identity,
                Some(anchor),
                vec![discovery(801)],
                None,
                vec![no_raw_anchor],
            )
            .is_err()
        );
    }
}
