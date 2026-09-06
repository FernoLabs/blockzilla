use std::{fs, path::Path};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    index_format::{hex_digest, parse_hex_digest},
    owner_balance_history_format::{
        OWNER_BALANCE_EVENT_RECORD_BYTES, OWNER_BALANCE_HISTORY_HEADER_BYTES,
        OWNER_BALANCE_HISTORY_MANIFEST_FILE, OwnerBalanceEventRecord,
        OwnerBalanceHistoryFileHeader, OwnerBalanceHistoryFileKind, OwnerBalanceHistoryManifest,
        OwnerBalanceHistorySemanticHasher,
    },
    owner_postings_format::{
        OWNER_POSTINGS_HEADER_BYTES, OWNER_POSTINGS_MANIFEST_FILE, OwnerPostingsFileHeader,
        OwnerPostingsFileKind, OwnerPostingsManifest,
    },
    postings_format::{
        POSTINGS_BODY_RECORD_BYTES, POSTINGS_DIRECTORY_RECORD_BYTES, POSTINGS_HEADER_BYTES,
        POSTINGS_MANIFEST_FILE, PostingRecord, PostingsDirectoryKind, PostingsDirectoryRecord,
        PostingsFileHeader, PostingsFileKind, PostingsManifest, PostingsSemanticHasher,
        ProgramInstructionScope, ProgramPostingRecord, ProgramPostingsSemanticHasher,
        TARGET_ADDRESS_FLAG_MINT, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
    },
    source::{PinnedSourceFile, SourceDump, load_source_dump},
};

const MAX_POSTINGS_MANIFEST_BYTES: u64 = 16 << 20;
const VALIDATION_BUFFER_BYTES: usize = 8 << 20;
const REGISTRY_KEY_BYTES: u64 = 32;
pub const MAX_POSTINGS_PAGE_ROWS: usize = 200;
pub const MAX_OWNER_BALANCE_HISTORY_ROWS: usize = 4_096;

pub const FINAL_SPYX_TRANSACTION_SHA256: &str =
    "2849a8e8fbe7d8dbb553022355cfd33d0e50971166242534a398334e79d977de";
pub const FINAL_SPYX_TRANSACTIONS: u64 = 7_311_137;
pub const FINAL_SPYX_DISCOVERED_ACCOUNTS: u64 = 134_942;
pub const FINAL_SPYX_TARGET_KEYS: u64 = 134_943;
pub const FINAL_SPYX_TARGET_POSTINGS: u64 = 29_060_229;
pub const FINAL_SPYX_PROGRAM_KEYS: u64 = 1_070;
pub const FINAL_SPYX_PROGRAM_POSTINGS: u64 = 39_753_473;

#[derive(Debug, Clone, Copy, Default)]
pub struct PostingsOpenOptions {
    pub allow_incomplete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostingLookupKind {
    TargetAddress,
    TokenAccount,
    Program,
}

impl PostingLookupKind {
    fn directory_kind(self) -> PostingsDirectoryKind {
        match self {
            Self::TargetAddress | Self::TokenAccount => PostingsDirectoryKind::TargetAddress,
            Self::Program => PostingsDirectoryKind::Program,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostingsPage {
    pub registry_id: u32,
    pub flags: u32,
    pub total: u64,
    pub offset: u64,
    pub transaction_ordinals: Vec<u64>,
    pub next_offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OwnerBalanceHistoryPage {
    pub registry_id: u32,
    pub total: u64,
    pub offset: u64,
    pub events: Vec<OwnerBalanceEventRecord>,
    pub next_offset: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerBalanceHistoryRangeQuery {
    /// Inclusive zero-based source transaction ID lower bound.
    pub transaction_id_from: Option<u64>,
    /// Inclusive zero-based source transaction ID upper bound.
    pub transaction_id_to: Option<u64>,
    /// Maximum exact event points returned after deterministic sampling.
    pub max_points: usize,
}

impl Default for OwnerBalanceHistoryRangeQuery {
    fn default() -> Self {
        Self {
            transaction_id_from: None,
            transaction_id_to: None,
            max_points: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OwnerBalanceHistorySeries {
    pub registry_id: u32,
    pub matching_events: u64,
    pub sampled: bool,
    pub events: Vec<OwnerBalanceEventRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyPostingsSummary {
    pub complete: bool,
    pub transactions: u64,
    pub target_address_keys: u64,
    pub target_address_postings: u64,
    pub program_keys: u64,
    pub program_postings: u64,
    pub program_direct_postings: u64,
    pub program_inner_postings: u64,
    pub source_transaction_sha256: String,
    pub target_address_semantic_sha256: String,
    pub program_semantic_sha256: String,
    pub program_direct_semantic_sha256: String,
    pub program_inner_semantic_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyOwnerPostingsSummary {
    pub complete: bool,
    pub transactions: u64,
    pub owner_keys: u64,
    pub owner_postings: u64,
    pub balance_history_available: bool,
    pub balance_history_owner_keys: u64,
    pub balance_history_events: u64,
    pub source_transaction_sha256: String,
    pub replay_state_sha256: String,
    pub owner_semantic_sha256: String,
}

pub struct PostingsStore {
    manifest_handle: PinnedSourceFile,
    manifest_bytes_sha256: [u8; 32],
    manifest: PostingsManifest,
    target_directory: PinnedPostingsFile,
    target_postings: PinnedPostingsFile,
    program_directory: PinnedPostingsFile,
    program_postings: PinnedPostingsFile,
    program_direct_directory: PinnedPostingsFile,
    program_direct_postings: PinnedPostingsFile,
    program_inner_directory: PinnedPostingsFile,
    program_inner_postings: PinnedPostingsFile,
    source: SourceDump,
}

impl PostingsStore {
    pub fn open(dump: &Path, postings: &Path) -> Result<Self> {
        Self::open_with_options(dump, postings, PostingsOpenOptions::default())
    }

    pub fn open_with_options(
        dump: &Path,
        postings: &Path,
        options: PostingsOpenOptions,
    ) -> Result<Self> {
        let source = load_source_dump(dump)?;
        let root = fs::canonicalize(postings)
            .with_context(|| format!("resolve postings directory {}", postings.display()))?;
        ensure!(root.is_dir(), "postings path is not a directory");

        let manifest_path = root.join(POSTINGS_MANIFEST_FILE);
        let manifest_handle = PinnedSourceFile::open(&manifest_path, "postings manifest")?;
        let manifest_bytes = manifest_handle.read_bounded(MAX_POSTINGS_MANIFEST_BYTES)?;
        let manifest_bytes_sha256: [u8; 32] = Sha256::digest(&manifest_bytes).into();
        let manifest: PostingsManifest =
            serde_json::from_slice(&manifest_bytes).context("parse postings manifest")?;
        validate_manifest_source(&manifest, &source, options)?;
        if manifest.complete {
            validate_final_spyx_counts(&manifest)?;
        }

        let mut scratch = Vec::new();
        let mint_registry_id = validate_registry_source(&source, &mut scratch)?;

        let target_directory =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::TargetAddressDirectory)?;
        let target_postings =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::TargetAddressPostings)?;
        let program_directory =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::ProgramDirectory)?;
        let program_postings =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::ProgramPostings)?;
        let program_direct_directory =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::ProgramDirectDirectory)?;
        let program_direct_postings =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::ProgramDirectPostings)?;
        let program_inner_directory =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::ProgramInnerDirectory)?;
        let program_inner_postings =
            PinnedPostingsFile::open(&root, &manifest, PostingsFileKind::ProgramInnerPostings)?;

        let target_semantic = validate_posting_pair(
            PostingsDirectoryKind::TargetAddress,
            &target_directory,
            &target_postings,
            PostingPairValidation {
                transaction_count: manifest.transactions,
                registry_entries: source.pubkeys,
                target_mint_id: Some(mint_registry_id),
                final_spyx: manifest.complete,
            },
            &mut scratch,
        )?;
        ensure!(
            target_semantic
                == parse_hex_digest(
                    &manifest.target_address_semantic_sha256,
                    "target-address semantic digest",
                )?,
            "target-address semantic digest differs from its manifest"
        );
        let program_semantic = validate_program_posting_pair(
            ProgramInstructionScope::All,
            &program_directory,
            &program_postings,
            PostingPairValidation {
                transaction_count: manifest.transactions,
                registry_entries: source.pubkeys,
                target_mint_id: None,
                final_spyx: manifest.complete,
            },
            &mut scratch,
        )?;
        ensure!(
            program_semantic
                == parse_hex_digest(&manifest.program_semantic_sha256, "program semantic digest",)?,
            "program semantic digest differs from its manifest"
        );
        let program_direct_semantic = validate_program_posting_pair(
            ProgramInstructionScope::Direct,
            &program_direct_directory,
            &program_direct_postings,
            PostingPairValidation {
                transaction_count: manifest.transactions,
                registry_entries: source.pubkeys,
                target_mint_id: None,
                final_spyx: manifest.complete,
            },
            &mut scratch,
        )?;
        ensure!(
            program_direct_semantic
                == parse_hex_digest(
                    &manifest.program_direct_semantic_sha256,
                    "direct program semantic digest",
                )?,
            "direct program semantic digest differs from its manifest"
        );
        let program_inner_semantic = validate_program_posting_pair(
            ProgramInstructionScope::Inner,
            &program_inner_directory,
            &program_inner_postings,
            PostingPairValidation {
                transaction_count: manifest.transactions,
                registry_entries: source.pubkeys,
                target_mint_id: None,
                final_spyx: manifest.complete,
            },
            &mut scratch,
        )?;
        ensure!(
            program_inner_semantic
                == parse_hex_digest(
                    &manifest.program_inner_semantic_sha256,
                    "inner program semantic digest",
                )?,
            "inner program semantic digest differs from its manifest"
        );
        let (derived_direct_semantic, derived_inner_semantic) = validate_scoped_program_projection(
            &program_directory,
            &program_postings,
            &program_direct_directory,
            &program_direct_postings,
            &program_inner_directory,
            &program_inner_postings,
            &mut scratch,
        )?;
        ensure!(
            derived_direct_semantic == program_direct_semantic
                && derived_inner_semantic == program_inner_semantic,
            "scoped program postings are not exact filtered projections of all postings"
        );

        manifest_handle.verify_identity("postings manifest")?;
        source.verify_file_identities()?;
        Ok(Self {
            manifest_handle,
            manifest_bytes_sha256,
            manifest,
            target_directory,
            target_postings,
            program_directory,
            program_postings,
            program_direct_directory,
            program_direct_postings,
            program_inner_directory,
            program_inner_postings,
            source,
        })
    }

    pub const fn complete(&self) -> bool {
        self.manifest.complete
    }

    pub const fn transaction_count(&self) -> u64 {
        self.manifest.transactions
    }

    pub fn source_transaction_sha256(&self) -> &str {
        &self.manifest.source.transaction_sha256
    }

    pub fn manifest_sha256(&self) -> String {
        hex_digest(self.manifest_bytes_sha256)
    }

    pub const fn manifest_sha256_bytes(&self) -> [u8; 32] {
        self.manifest_bytes_sha256
    }

    pub fn target_address_semantic_sha256(&self) -> &str {
        &self.manifest.target_address_semantic_sha256
    }

    pub fn program_semantic_sha256(&self) -> &str {
        &self.manifest.program_semantic_sha256
    }

    pub fn program_direct_semantic_sha256(&self) -> &str {
        &self.manifest.program_direct_semantic_sha256
    }

    pub fn program_inner_semantic_sha256(&self) -> &str {
        &self.manifest.program_inner_semantic_sha256
    }

    pub const fn target_address_key_count(&self) -> u64 {
        self.manifest.target_address_directory.records
    }

    pub const fn target_address_posting_count(&self) -> u64 {
        self.manifest.target_address_postings.records
    }

    pub const fn program_key_count(&self) -> u64 {
        self.manifest.program_directory.records
    }

    pub const fn program_posting_count(&self) -> u64 {
        self.manifest.program_postings.records
    }

    pub const fn program_direct_posting_count(&self) -> u64 {
        self.manifest.program_direct_postings.records
    }

    pub const fn program_inner_posting_count(&self) -> u64 {
        self.manifest.program_inner_postings.records
    }

    pub fn lookup_base58(
        &self,
        kind: PostingLookupKind,
        key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Option<PostingsPage>> {
        let mut raw = [0u8; 32];
        let decoded = bs58::decode(key)
            .onto(&mut raw)
            .context("decode posting key as base58")?;
        ensure!(decoded == raw.len(), "posting key is not 32 bytes");
        self.lookup(kind, raw, offset, limit)
    }

    pub fn lookup(
        &self,
        kind: PostingLookupKind,
        key: [u8; 32],
        offset: u64,
        limit: usize,
    ) -> Result<Option<PostingsPage>> {
        self.lookup_with_program_scope(kind, key, ProgramInstructionScope::All, offset, limit)
    }

    pub fn lookup_program(
        &self,
        key: [u8; 32],
        instruction_scope: ProgramInstructionScope,
        offset: u64,
        limit: usize,
    ) -> Result<Option<PostingsPage>> {
        self.lookup_with_program_scope(
            PostingLookupKind::Program,
            key,
            instruction_scope,
            offset,
            limit,
        )
    }

    fn lookup_with_program_scope(
        &self,
        kind: PostingLookupKind,
        key: [u8; 32],
        instruction_scope: ProgramInstructionScope,
        offset: u64,
        limit: usize,
    ) -> Result<Option<PostingsPage>> {
        ensure!(
            (1..=MAX_POSTINGS_PAGE_ROWS).contains(&limit),
            "posting page limit must be from 1 through {MAX_POSTINGS_PAGE_ROWS}"
        );
        ensure!(
            kind == PostingLookupKind::Program || instruction_scope == ProgramInstructionScope::All,
            "instruction scope is only valid for program postings"
        );
        let Some(registry_id) = self.registry_id(key)? else {
            return Ok(None);
        };
        let (directory, body) = match kind {
            PostingLookupKind::TargetAddress | PostingLookupKind::TokenAccount => {
                (&self.target_directory, &self.target_postings)
            }
            PostingLookupKind::Program => match instruction_scope {
                ProgramInstructionScope::All => (&self.program_directory, &self.program_postings),
                ProgramInstructionScope::Direct => (
                    &self.program_direct_directory,
                    &self.program_direct_postings,
                ),
                ProgramInstructionScope::Inner => {
                    (&self.program_inner_directory, &self.program_inner_postings)
                }
            },
        };
        let Some(record) = directory.lookup_directory(kind.directory_kind(), registry_id)? else {
            return Ok(None);
        };
        if kind == PostingLookupKind::TokenAccount
            && record.flags != TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT
        {
            return Ok(None);
        }
        ensure!(
            offset <= record.posting_count,
            "posting page offset exceeds the key range"
        );
        let remaining = record.posting_count - offset;
        let row_count = usize::try_from(remaining.min(limit as u64))
            .context("posting page row count exceeds usize")?;
        let first = record
            .first_posting_row
            .checked_add(offset)
            .context("posting page start overflow")?;
        let mut encoded = [0u8; MAX_POSTINGS_PAGE_ROWS * POSTINGS_BODY_RECORD_BYTES];
        let byte_count = row_count
            .checked_mul(POSTINGS_BODY_RECORD_BYTES)
            .context("posting page byte count overflow")?;
        if byte_count != 0 {
            body.read_rows(first, &mut encoded[..byte_count])?;
        }
        let mut transaction_ordinals = Vec::new();
        transaction_ordinals
            .try_reserve_exact(row_count)
            .context("reserve posting page")?;
        for row in encoded[..byte_count].chunks_exact(POSTINGS_BODY_RECORD_BYTES) {
            let transaction_ordinal = if kind == PostingLookupKind::Program {
                let posting = ProgramPostingRecord::decode(row)?;
                ensure!(
                    instruction_scope.includes(posting.instruction_scope_mask),
                    "program posting row does not match its selected scope"
                );
                posting.transaction_ordinal
            } else {
                PostingRecord::decode(row)?.transaction_ordinal
            };
            transaction_ordinals.push(transaction_ordinal);
        }
        let consumed = u64::try_from(row_count).expect("bounded page row count fits u64");
        let end = offset
            .checked_add(consumed)
            .context("posting page end overflow")?;
        Ok(Some(PostingsPage {
            registry_id,
            flags: record.flags,
            total: record.posting_count,
            offset,
            transaction_ordinals,
            next_offset: (end < record.posting_count).then_some(end),
        }))
    }

    fn registry_id(&self, key: [u8; 32]) -> Result<Option<u32>> {
        registry_id_in_source(&self.source, key)
    }

    fn verify_identities(&self) -> Result<()> {
        self.manifest_handle.verify_identity("postings manifest")?;
        for (file, label) in [
            (&self.target_directory, "target-address directory"),
            (&self.target_postings, "target-address postings"),
            (&self.program_directory, "program directory"),
            (&self.program_postings, "program postings"),
            (&self.program_direct_directory, "direct program directory"),
            (&self.program_direct_postings, "direct program postings"),
            (&self.program_inner_directory, "inner program directory"),
            (&self.program_inner_postings, "inner program postings"),
        ] {
            file.file.verify_identity(label)?;
        }
        self.source.verify_file_identities()
    }

    fn summary(&self) -> VerifyPostingsSummary {
        VerifyPostingsSummary {
            complete: self.complete(),
            transactions: self.transaction_count(),
            target_address_keys: self.target_address_key_count(),
            target_address_postings: self.target_address_posting_count(),
            program_keys: self.program_key_count(),
            program_postings: self.program_posting_count(),
            program_direct_postings: self.program_direct_posting_count(),
            program_inner_postings: self.program_inner_posting_count(),
            source_transaction_sha256: self.source_transaction_sha256().to_owned(),
            target_address_semantic_sha256: self.target_address_semantic_sha256().to_owned(),
            program_semantic_sha256: self.program_semantic_sha256().to_owned(),
            program_direct_semantic_sha256: self.program_direct_semantic_sha256().to_owned(),
            program_inner_semantic_sha256: self.program_inner_semantic_sha256().to_owned(),
        }
    }
}

pub struct OwnerPostingsStore {
    manifest_handle: PinnedSourceFile,
    manifest_bytes_sha256: [u8; 32],
    manifest: OwnerPostingsManifest,
    owner_directory: PinnedPostingsFile,
    owner_postings: PinnedPostingsFile,
    balance_history: Option<OpenedOwnerBalanceHistory>,
    source: SourceDump,
}

struct OpenedOwnerBalanceHistory {
    manifest_handle: PinnedSourceFile,
    manifest: OwnerBalanceHistoryManifest,
    owner_directory: PinnedPostingsFile,
    events: PinnedPostingsFile,
}

impl OwnerPostingsStore {
    pub fn open(dump: &Path, postings: &Path) -> Result<Self> {
        Self::open_with_options(dump, postings, PostingsOpenOptions::default())
    }

    pub fn open_with_options(
        dump: &Path,
        postings: &Path,
        options: PostingsOpenOptions,
    ) -> Result<Self> {
        let source = load_source_dump(dump)?;
        let root = fs::canonicalize(postings)
            .with_context(|| format!("resolve owner postings directory {}", postings.display()))?;
        ensure!(root.is_dir(), "owner postings path is not a directory");
        let manifest_path = root.join(OWNER_POSTINGS_MANIFEST_FILE);
        let manifest_handle = PinnedSourceFile::open(&manifest_path, "owner postings manifest")?;
        let manifest_bytes = manifest_handle.read_bounded(MAX_POSTINGS_MANIFEST_BYTES)?;
        let manifest_bytes_sha256: [u8; 32] = Sha256::digest(&manifest_bytes).into();
        let manifest: OwnerPostingsManifest =
            serde_json::from_slice(&manifest_bytes).context("parse owner postings manifest")?;
        validate_owner_manifest_source(&manifest, &source, options)?;

        let mut scratch = Vec::new();
        validate_registry_source(&source, &mut scratch)?;
        let owner_directory =
            PinnedPostingsFile::open_owner(&root, &manifest, OwnerPostingsFileKind::Directory)?;
        let owner_postings =
            PinnedPostingsFile::open_owner(&root, &manifest, OwnerPostingsFileKind::Postings)?;
        let semantic = validate_posting_pair(
            PostingsDirectoryKind::Owner,
            &owner_directory,
            &owner_postings,
            PostingPairValidation {
                transaction_count: manifest.transactions,
                registry_entries: source.pubkeys,
                target_mint_id: None,
                final_spyx: manifest.complete,
            },
            &mut scratch,
        )?;
        ensure!(
            semantic == parse_hex_digest(&manifest.owner_semantic_sha256, "owner semantic digest")?,
            "owner semantic digest differs from its manifest"
        );
        let balance_history_path = root.join(OWNER_BALANCE_HISTORY_MANIFEST_FILE);
        let balance_history = if let Some(binding) = &manifest.balance_history_manifest {
            let history_manifest_handle =
                PinnedSourceFile::open(&balance_history_path, "owner balance-history manifest")?;
            let history_manifest_bytes =
                history_manifest_handle.read_bounded(MAX_POSTINGS_MANIFEST_BYTES)?;
            ensure!(
                history_manifest_handle.len() == binding.bytes
                    && hex_digest(Sha256::digest(&history_manifest_bytes).into()) == binding.sha256,
                "owner balance-history manifest differs from its owner-postings binding"
            );
            let history_manifest: OwnerBalanceHistoryManifest =
                serde_json::from_slice(&history_manifest_bytes)
                    .context("parse owner balance-history manifest")?;
            validate_owner_balance_history_manifest_source(
                &history_manifest,
                &manifest,
                &source,
                options,
            )?;
            let history_directory = PinnedPostingsFile::open_owner_balance_history(
                &root,
                &history_manifest,
                OwnerBalanceHistoryFileKind::Directory,
            )?;
            let history_events = PinnedPostingsFile::open_owner_balance_history(
                &root,
                &history_manifest,
                OwnerBalanceHistoryFileKind::Events,
            )?;
            let history_semantic = validate_owner_balance_history_pair(
                &history_directory,
                &history_events,
                history_manifest.transactions,
                source.pubkeys,
                &mut scratch,
            )?;
            ensure!(
                history_semantic
                    == parse_hex_digest(
                        &history_manifest.history_semantic_sha256,
                        "owner balance-history semantic digest",
                    )?,
                "owner balance-history semantic digest differs from its manifest"
            );
            history_manifest_handle.verify_identity("owner balance-history manifest")?;
            Some(OpenedOwnerBalanceHistory {
                manifest_handle: history_manifest_handle,
                manifest: history_manifest,
                owner_directory: history_directory,
                events: history_events,
            })
        } else {
            ensure!(
                !balance_history_path.try_exists().with_context(|| {
                    format!(
                        "inspect owner balance-history manifest {}",
                        balance_history_path.display()
                    )
                })?,
                "owner postings contain an unbound balance-history manifest"
            );
            None
        };
        manifest_handle.verify_identity("owner postings manifest")?;
        source.verify_file_identities()?;
        Ok(Self {
            manifest_handle,
            manifest_bytes_sha256,
            manifest,
            owner_directory,
            owner_postings,
            balance_history,
            source,
        })
    }

    pub const fn complete(&self) -> bool {
        self.manifest.complete
    }

    pub const fn transaction_count(&self) -> u64 {
        self.manifest.transactions
    }

    pub fn source_transaction_sha256(&self) -> &str {
        &self.manifest.source.transaction_sha256
    }

    pub const fn manifest_sha256_bytes(&self) -> [u8; 32] {
        self.manifest_bytes_sha256
    }

    pub const fn owner_key_count(&self) -> u64 {
        self.manifest.owner_directory.records
    }

    pub const fn owner_posting_count(&self) -> u64 {
        self.manifest.owner_postings.records
    }

    pub const fn has_balance_history(&self) -> bool {
        self.balance_history.is_some()
    }

    pub fn balance_history_owner_key_count(&self) -> u64 {
        self.balance_history
            .as_ref()
            .map_or(0, |history| history.manifest.owner_directory.records)
    }

    pub fn balance_history_event_count(&self) -> u64 {
        self.balance_history
            .as_ref()
            .map_or(0, |history| history.manifest.balance_events.records)
    }

    /// Read one exact page of sparse transaction-final owner balance changes.
    /// Legacy owner-postings v1 artifacts return `None` because they have no
    /// balance-history extension.
    pub fn lookup_balance_history(
        &self,
        key: [u8; 32],
        offset: u64,
        limit: usize,
    ) -> Result<Option<OwnerBalanceHistoryPage>> {
        ensure!(
            (1..=MAX_OWNER_BALANCE_HISTORY_ROWS).contains(&limit),
            "owner balance-history page limit must be from 1 through {MAX_OWNER_BALANCE_HISTORY_ROWS}"
        );
        let Some(history) = self.balance_history.as_ref() else {
            return Ok(None);
        };
        let Some(registry_id) = registry_id_in_source(&self.source, key)? else {
            return Ok(None);
        };
        let Some(directory) = history
            .owner_directory
            .lookup_directory(PostingsDirectoryKind::Owner, registry_id)?
        else {
            return Ok(None);
        };
        ensure!(
            offset <= directory.posting_count,
            "owner balance-history page offset exceeds the owner range"
        );
        let row_count =
            usize::try_from((directory.posting_count - offset).min(u64::try_from(limit)?))
                .context("owner balance-history page row count exceeds usize")?;
        let first = directory
            .first_posting_row
            .checked_add(offset)
            .context("owner balance-history page start overflow")?;
        let events = history.events.read_balance_events(first, row_count)?;
        let end = offset
            .checked_add(u64::try_from(row_count)?)
            .context("owner balance-history page end overflow")?;
        Ok(Some(OwnerBalanceHistoryPage {
            registry_id,
            total: directory.posting_count,
            offset,
            events,
            next_offset: (end < directory.posting_count).then_some(end),
        }))
    }

    pub fn lookup_balance_history_base58(
        &self,
        key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Option<OwnerBalanceHistoryPage>> {
        let mut raw = [0u8; 32];
        let decoded = bs58::decode(key)
            .onto(&mut raw)
            .context("decode owner balance-history key as base58")?;
        ensure!(
            decoded == raw.len(),
            "owner balance-history key is not 32 bytes"
        );
        self.lookup_balance_history(raw, offset, limit)
    }

    /// Read an inclusive transaction-ID range and return exact selected event
    /// rows. With at least two output points, the first and last matching
    /// events are preserved and intermediate rows use deterministic spacing.
    /// A one-point request returns the latest matching event.
    pub fn lookup_balance_history_range(
        &self,
        key: [u8; 32],
        query: OwnerBalanceHistoryRangeQuery,
    ) -> Result<Option<OwnerBalanceHistorySeries>> {
        ensure!(
            (1..=MAX_OWNER_BALANCE_HISTORY_ROWS).contains(&query.max_points),
            "owner balance-history max_points must be from 1 through {MAX_OWNER_BALANCE_HISTORY_ROWS}"
        );
        ensure!(
            !matches!(
                (query.transaction_id_from, query.transaction_id_to),
                (Some(from), Some(to)) if from > to
            ),
            "owner balance-history transaction range is reversed"
        );
        let Some(history) = self.balance_history.as_ref() else {
            return Ok(None);
        };
        let Some(registry_id) = registry_id_in_source(&self.source, key)? else {
            return Ok(None);
        };
        let Some(directory) = history
            .owner_directory
            .lookup_directory(PostingsDirectoryKind::Owner, registry_id)?
        else {
            return Ok(None);
        };
        let first_offset = history.events.lower_bound_transaction_id(
            directory.first_posting_row,
            directory.posting_count,
            query.transaction_id_from.unwrap_or(0),
        )?;
        let end_offset = if let Some(to) = query.transaction_id_to {
            history.events.upper_bound_transaction_id(
                directory.first_posting_row,
                directory.posting_count,
                to,
            )?
        } else {
            directory.posting_count
        };
        ensure!(
            first_offset <= end_offset && end_offset <= directory.posting_count,
            "owner balance-history search produced an invalid range"
        );
        let matching_events = end_offset - first_offset;
        let sample_count = matching_events.min(u64::try_from(query.max_points)?);
        let mut events = Vec::new();
        events
            .try_reserve_exact(usize::try_from(sample_count)?)
            .context("reserve owner balance-history sample")?;
        if matching_events <= u64::try_from(query.max_points)? {
            let first = directory
                .first_posting_row
                .checked_add(first_offset)
                .context("owner balance-history range start overflow")?;
            events = history
                .events
                .read_balance_events(first, usize::try_from(matching_events)?)?;
        } else if query.max_points == 1 {
            let row = directory
                .first_posting_row
                .checked_add(end_offset - 1)
                .context("owner balance-history last sample row overflow")?;
            events.push(history.events.read_balance_event(row)?);
        } else {
            let denominator = u64::try_from(query.max_points - 1)?;
            for point in 0..query.max_points {
                let relative = u64::try_from(point)?
                    .checked_mul(matching_events - 1)
                    .context("owner balance-history sample position overflow")?
                    / denominator;
                let row = directory
                    .first_posting_row
                    .checked_add(first_offset)
                    .and_then(|base| base.checked_add(relative))
                    .context("owner balance-history sample row overflow")?;
                events.push(history.events.read_balance_event(row)?);
            }
        }
        Ok(Some(OwnerBalanceHistorySeries {
            registry_id,
            matching_events,
            sampled: matching_events > u64::try_from(query.max_points)?,
            events,
        }))
    }

    pub fn lookup_balance_history_range_base58(
        &self,
        key: &str,
        query: OwnerBalanceHistoryRangeQuery,
    ) -> Result<Option<OwnerBalanceHistorySeries>> {
        let mut raw = [0u8; 32];
        let decoded = bs58::decode(key)
            .onto(&mut raw)
            .context("decode owner balance-history key as base58")?;
        ensure!(
            decoded == raw.len(),
            "owner balance-history key is not 32 bytes"
        );
        self.lookup_balance_history_range(raw, query)
    }

    pub fn lookup(&self, key: [u8; 32], offset: u64, limit: usize) -> Result<Option<PostingsPage>> {
        ensure!(
            (1..=MAX_POSTINGS_PAGE_ROWS).contains(&limit),
            "owner posting page limit must be from 1 through {MAX_POSTINGS_PAGE_ROWS}"
        );
        let Some(registry_id) = registry_id_in_source(&self.source, key)? else {
            return Ok(None);
        };
        let Some(record) = self
            .owner_directory
            .lookup_directory(PostingsDirectoryKind::Owner, registry_id)?
        else {
            return Ok(None);
        };
        ensure!(
            offset <= record.posting_count,
            "owner posting page offset exceeds the key range"
        );
        let remaining = record.posting_count - offset;
        let row_count = usize::try_from(remaining.min(limit as u64))
            .context("owner posting page row count exceeds usize")?;
        let first = record
            .first_posting_row
            .checked_add(offset)
            .context("owner posting page start overflow")?;
        let byte_count = row_count
            .checked_mul(POSTINGS_BODY_RECORD_BYTES)
            .context("owner posting page byte count overflow")?;
        let mut encoded = [0u8; MAX_POSTINGS_PAGE_ROWS * POSTINGS_BODY_RECORD_BYTES];
        if byte_count != 0 {
            self.owner_postings
                .read_rows(first, &mut encoded[..byte_count])?;
        }
        let mut transaction_ordinals = Vec::new();
        transaction_ordinals
            .try_reserve_exact(row_count)
            .context("reserve owner posting page")?;
        for row in encoded[..byte_count].chunks_exact(POSTINGS_BODY_RECORD_BYTES) {
            transaction_ordinals.push(PostingRecord::decode(row)?.transaction_ordinal);
        }
        let end = offset
            .checked_add(u64::try_from(row_count)?)
            .context("owner posting page end overflow")?;
        Ok(Some(PostingsPage {
            registry_id,
            flags: 0,
            total: record.posting_count,
            offset,
            transaction_ordinals,
            next_offset: (end < record.posting_count).then_some(end),
        }))
    }

    fn verify_identities(&self) -> Result<()> {
        self.manifest_handle
            .verify_identity("owner postings manifest")?;
        self.owner_directory
            .file
            .verify_identity("owner directory")?;
        self.owner_postings.file.verify_identity("owner postings")?;
        if let Some(history) = &self.balance_history {
            history
                .manifest_handle
                .verify_identity("owner balance-history manifest")?;
            history
                .owner_directory
                .file
                .verify_identity("owner balance-history directory")?;
            history
                .events
                .file
                .verify_identity("owner balance-history events")?;
        }
        self.source.verify_file_identities()
    }

    fn summary(&self) -> VerifyOwnerPostingsSummary {
        VerifyOwnerPostingsSummary {
            complete: self.complete(),
            transactions: self.transaction_count(),
            owner_keys: self.owner_key_count(),
            owner_postings: self.owner_posting_count(),
            balance_history_available: self.has_balance_history(),
            balance_history_owner_keys: self.balance_history_owner_key_count(),
            balance_history_events: self.balance_history_event_count(),
            source_transaction_sha256: self.source_transaction_sha256().to_owned(),
            replay_state_sha256: self.manifest.replay_state_sha256.clone(),
            owner_semantic_sha256: self.manifest.owner_semantic_sha256.clone(),
        }
    }
}

pub fn verify_owner_postings_artifact(
    dump: &Path,
    postings: &Path,
    allow_incomplete: bool,
) -> Result<VerifyOwnerPostingsSummary> {
    let store = OwnerPostingsStore::open_with_options(
        dump,
        postings,
        PostingsOpenOptions { allow_incomplete },
    )?;
    let mut scratch = Vec::new();
    for (file, expected, label) in [
        (
            &store.source.manifest_handle,
            store.source.manifest_sha256,
            "source manifest",
        ),
        (
            &store.source.transaction_handle,
            store.source.transaction_sha256,
            "source transactions",
        ),
        (
            &store.source.signature_handle,
            store.source.signature_sha256,
            "source signatures",
        ),
        (
            &store.source.registry_handle,
            store.source.registry_sha256,
            "source registry",
        ),
        (
            &store.source.accounts_handle,
            store.source.accounts_sha256,
            "source accounts",
        ),
    ] {
        ensure!(
            hash_pinned_file_reused(file, &mut scratch)? == expected,
            "{label} digest differs during full owner postings verification"
        );
    }
    store.verify_identities()?;
    Ok(store.summary())
}

pub fn verify_postings_artifact(
    dump: &Path,
    postings: &Path,
    allow_incomplete: bool,
) -> Result<VerifyPostingsSummary> {
    let store =
        PostingsStore::open_with_options(dump, postings, PostingsOpenOptions { allow_incomplete })?;
    let mut scratch = Vec::new();
    for (file, expected, label) in [
        (
            &store.source.manifest_handle,
            store.source.manifest_sha256,
            "source manifest",
        ),
        (
            &store.source.transaction_handle,
            store.source.transaction_sha256,
            "source transactions",
        ),
        (
            &store.source.signature_handle,
            store.source.signature_sha256,
            "source signatures",
        ),
        (
            &store.source.registry_handle,
            store.source.registry_sha256,
            "source registry",
        ),
        (
            &store.source.accounts_handle,
            store.source.accounts_sha256,
            "source accounts",
        ),
    ] {
        ensure!(
            hash_pinned_file_reused(file, &mut scratch)? == expected,
            "{label} digest differs during full postings verification"
        );
    }
    store.verify_identities()?;
    Ok(store.summary())
}

struct PinnedPostingsFile {
    file: PinnedSourceFile,
    record_count: u64,
    record_bytes: usize,
    expected_sha256: [u8; 32],
}

impl PinnedPostingsFile {
    fn open(root: &Path, manifest: &PostingsManifest, kind: PostingsFileKind) -> Result<Self> {
        let binding = manifest.binding(kind);
        let path = root.join(kind.file_name());
        let file = PinnedSourceFile::open(&path, "postings file")?;
        ensure!(
            file.len() == binding.bytes
                && file.len() == kind.encoded_file_bytes(binding.records)?,
            "postings file size differs from its manifest"
        );
        let mut header_bytes = [0u8; POSTINGS_HEADER_BYTES];
        positioned_read_exact(file.file(), &mut header_bytes, 0)?;
        manifest.validate_header(PostingsFileHeader::decode(&header_bytes, kind)?)?;
        Ok(Self {
            file,
            record_count: binding.records,
            record_bytes: usize::from(kind.record_bytes()),
            expected_sha256: parse_hex_digest(&binding.sha256, "postings file digest")?,
        })
    }

    fn open_owner(
        root: &Path,
        manifest: &OwnerPostingsManifest,
        kind: OwnerPostingsFileKind,
    ) -> Result<Self> {
        const { assert!(OWNER_POSTINGS_HEADER_BYTES == POSTINGS_HEADER_BYTES) };

        let binding = manifest.binding(kind);
        let path = root.join(kind.file_name());
        let file = PinnedSourceFile::open(&path, "owner postings file")?;
        ensure!(
            file.len() == binding.bytes
                && file.len() == kind.encoded_file_bytes(binding.records)?,
            "owner postings file size differs from its manifest"
        );
        let mut header_bytes = [0u8; OWNER_POSTINGS_HEADER_BYTES];
        positioned_read_exact(file.file(), &mut header_bytes, 0)?;
        manifest.validate_header(OwnerPostingsFileHeader::decode(&header_bytes, kind)?)?;
        Ok(Self {
            file,
            record_count: binding.records,
            record_bytes: usize::from(kind.record_bytes()),
            expected_sha256: parse_hex_digest(&binding.sha256, "owner postings file digest")?,
        })
    }

    fn open_owner_balance_history(
        root: &Path,
        manifest: &OwnerBalanceHistoryManifest,
        kind: OwnerBalanceHistoryFileKind,
    ) -> Result<Self> {
        const { assert!(OWNER_BALANCE_HISTORY_HEADER_BYTES == POSTINGS_HEADER_BYTES) };

        let binding = manifest.binding(kind);
        let path = root.join(kind.file_name());
        let file = PinnedSourceFile::open(&path, "owner balance-history file")?;
        ensure!(
            file.len() == binding.bytes
                && file.len() == kind.encoded_file_bytes(binding.records)?,
            "owner balance-history file size differs from its manifest"
        );
        let mut header_bytes = [0u8; OWNER_BALANCE_HISTORY_HEADER_BYTES];
        positioned_read_exact(file.file(), &mut header_bytes, 0)?;
        manifest.validate_header(OwnerBalanceHistoryFileHeader::decode(&header_bytes, kind)?)?;
        Ok(Self {
            file,
            record_count: binding.records,
            record_bytes: usize::from(kind.record_bytes()),
            expected_sha256: parse_hex_digest(
                &binding.sha256,
                "owner balance-history file digest",
            )?,
        })
    }

    fn read_rows(&self, first: u64, bytes: &mut [u8]) -> Result<()> {
        ensure!(
            bytes.len().is_multiple_of(self.record_bytes),
            "postings read byte count is not a whole number of rows"
        );
        let rows = u64::try_from(bytes.len() / self.record_bytes)?;
        ensure!(
            first
                .checked_add(rows)
                .is_some_and(|end| end <= self.record_count),
            "postings row range exceeds its file"
        );
        positioned_read_exact(self.file.file(), bytes, self.row_offset(first)?)
    }

    fn read_directory_row(
        &self,
        kind: PostingsDirectoryKind,
        ordinal: u64,
    ) -> Result<PostingsDirectoryRecord> {
        ensure!(
            self.record_bytes == POSTINGS_DIRECTORY_RECORD_BYTES,
            "postings file is not a directory"
        );
        let mut bytes = [0u8; POSTINGS_DIRECTORY_RECORD_BYTES];
        self.read_rows(ordinal, &mut bytes)?;
        PostingsDirectoryRecord::decode(&bytes, kind)
    }

    fn read_balance_event(&self, ordinal: u64) -> Result<OwnerBalanceEventRecord> {
        ensure!(
            self.record_bytes == OWNER_BALANCE_EVENT_RECORD_BYTES,
            "owner balance-history file is not an event file"
        );
        let mut bytes = [0u8; OWNER_BALANCE_EVENT_RECORD_BYTES];
        self.read_rows(ordinal, &mut bytes)?;
        OwnerBalanceEventRecord::decode(&bytes)
    }

    fn read_balance_events(&self, first: u64, rows: usize) -> Result<Vec<OwnerBalanceEventRecord>> {
        ensure!(
            rows <= MAX_OWNER_BALANCE_HISTORY_ROWS,
            "owner balance-history read exceeds its fixed row limit"
        );
        let bytes_len = rows
            .checked_mul(OWNER_BALANCE_EVENT_RECORD_BYTES)
            .context("owner balance-history read byte count overflow")?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(bytes_len)
            .context("reserve bounded owner balance-history read")?;
        encoded.resize(bytes_len, 0);
        self.read_rows(first, &mut encoded)?;
        let mut events = Vec::new();
        events
            .try_reserve_exact(rows)
            .context("reserve owner balance-history result")?;
        for row in encoded.chunks_exact(OWNER_BALANCE_EVENT_RECORD_BYTES) {
            events.push(OwnerBalanceEventRecord::decode(row)?);
        }
        Ok(events)
    }

    fn lower_bound_transaction_id(
        &self,
        first: u64,
        count: u64,
        transaction_id: u64,
    ) -> Result<u64> {
        let mut left = 0u64;
        let mut right = count;
        while left < right {
            let middle = left + (right - left) / 2;
            let row = first
                .checked_add(middle)
                .context("owner balance-history lower-bound row overflow")?;
            if self.read_balance_event(row)?.transaction_id < transaction_id {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        Ok(left)
    }

    fn upper_bound_transaction_id(
        &self,
        first: u64,
        count: u64,
        transaction_id: u64,
    ) -> Result<u64> {
        let mut left = 0u64;
        let mut right = count;
        while left < right {
            let middle = left + (right - left) / 2;
            let row = first
                .checked_add(middle)
                .context("owner balance-history upper-bound row overflow")?;
            if self.read_balance_event(row)?.transaction_id <= transaction_id {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        Ok(left)
    }

    fn lookup_directory(
        &self,
        kind: PostingsDirectoryKind,
        registry_id: u32,
    ) -> Result<Option<PostingsDirectoryRecord>> {
        let mut left = 0u64;
        let mut right = self.record_count;
        while left < right {
            let middle = left + (right - left) / 2;
            let record = self.read_directory_row(kind, middle)?;
            match record.registry_id.cmp(&registry_id) {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok(Some(record)),
            }
        }
        Ok(None)
    }

    fn row_offset(&self, ordinal: u64) -> Result<u64> {
        u64::try_from(POSTINGS_HEADER_BYTES)
            .expect("postings header size fits u64")
            .checked_add(
                ordinal
                    .checked_mul(u64::try_from(self.record_bytes)?)
                    .context("postings row byte offset overflow")?,
            )
            .context("postings row file offset overflow")
    }
}

fn validate_manifest_source(
    manifest: &PostingsManifest,
    source: &SourceDump,
    options: PostingsOpenOptions,
) -> Result<()> {
    manifest.validate()?;
    ensure!(
        manifest.complete || options.allow_incomplete,
        "postings are an incomplete canary; pass --allow-incomplete explicitly"
    );
    ensure!(
        manifest.source.manifest_bytes == source.manifest_handle.len()
            && manifest.source.manifest_sha256 == hex_digest(source.manifest_sha256)
            && manifest.source.transaction_bytes == source.transaction_bytes
            && manifest.source.transaction_sha256 == hex_digest(source.transaction_sha256)
            && manifest.source.registry_bytes == source.registry_bytes
            && manifest.source.registry_sha256 == hex_digest(source.registry_sha256)
            && manifest.source.accounts_bytes == source.accounts_bytes
            && manifest.source.accounts_sha256 == hex_digest(source.accounts_sha256)
            && manifest.source.transactions == source.manifest.transactions
            && manifest.source.pubkeys == source.pubkeys
            && manifest.source.accounts
                == source
                    .manifest
                    .discovered_account_count
                    .context("source manifest has no discovered-account count")?,
        "postings source binding differs from the consolidated dump"
    );
    Ok(())
}

fn validate_owner_manifest_source(
    manifest: &OwnerPostingsManifest,
    source: &SourceDump,
    options: PostingsOpenOptions,
) -> Result<()> {
    manifest.validate()?;
    ensure!(
        manifest.complete || options.allow_incomplete,
        "owner postings are an incomplete canary; pass --allow-incomplete explicitly"
    );
    ensure!(
        manifest.source.manifest_bytes == source.manifest_handle.len()
            && manifest.source.manifest_sha256 == hex_digest(source.manifest_sha256)
            && manifest.source.transaction_bytes == source.transaction_bytes
            && manifest.source.transaction_sha256 == hex_digest(source.transaction_sha256)
            && manifest.source.registry_bytes == source.registry_bytes
            && manifest.source.registry_sha256 == hex_digest(source.registry_sha256)
            && manifest.source.accounts_bytes == source.accounts_bytes
            && manifest.source.accounts_sha256 == hex_digest(source.accounts_sha256)
            && manifest.source.transactions == source.manifest.transactions
            && manifest.source.pubkeys == source.pubkeys
            && manifest.source.accounts
                == source
                    .manifest
                    .discovered_account_count
                    .context("source manifest has no discovered-account count")?,
        "owner postings source binding differs from the consolidated dump"
    );
    if manifest.complete {
        ensure!(
            manifest.source.transaction_sha256 == FINAL_SPYX_TRANSACTION_SHA256
                && manifest.source.transactions == FINAL_SPYX_TRANSACTIONS
                && manifest.transactions == FINAL_SPYX_TRANSACTIONS
                && manifest.source.accounts == FINAL_SPYX_DISCOVERED_ACCOUNTS,
            "complete owner postings do not satisfy the final SPYx source acceptance values"
        );
    }
    Ok(())
}

fn validate_owner_balance_history_manifest_source(
    history: &OwnerBalanceHistoryManifest,
    owner_postings: &OwnerPostingsManifest,
    source: &SourceDump,
    options: PostingsOpenOptions,
) -> Result<()> {
    history.validate()?;
    ensure!(
        history.complete || options.allow_incomplete,
        "owner balance history is an incomplete canary; pass --allow-incomplete explicitly"
    );
    ensure!(
        history.complete == owner_postings.complete
            && history.canary_max_transactions == owner_postings.canary_max_transactions
            && history.transactions == owner_postings.transactions
            && history.source.manifest_file == owner_postings.source.manifest_file
            && history.source.manifest_bytes == owner_postings.source.manifest_bytes
            && history.source.manifest_sha256 == owner_postings.source.manifest_sha256
            && history.source.transaction_file == owner_postings.source.transaction_file
            && history.source.transaction_bytes == owner_postings.source.transaction_bytes
            && history.source.transaction_sha256 == owner_postings.source.transaction_sha256
            && history.source.registry_file == owner_postings.source.registry_file
            && history.source.registry_bytes == owner_postings.source.registry_bytes
            && history.source.registry_sha256 == owner_postings.source.registry_sha256
            && history.source.accounts_file == owner_postings.source.accounts_file
            && history.source.accounts_bytes == owner_postings.source.accounts_bytes
            && history.source.accounts_sha256 == owner_postings.source.accounts_sha256
            && history.source.transactions == owner_postings.source.transactions
            && history.source.pubkeys == owner_postings.source.pubkeys
            && history.source.accounts == owner_postings.source.accounts
            && history.replay_semantic_version == owner_postings.replay_semantic_version
            && history.replay_state_sha256 == owner_postings.replay_state_sha256
            && history.owner_postings_semantic_sha256 == owner_postings.owner_semantic_sha256,
        "owner balance history differs from its owner-postings artifact"
    );
    ensure!(
        history.source.manifest_sha256 == hex_digest(source.manifest_sha256)
            && history.source.transaction_sha256 == hex_digest(source.transaction_sha256)
            && history.source.registry_sha256 == hex_digest(source.registry_sha256)
            && history.source.accounts_sha256 == hex_digest(source.accounts_sha256),
        "owner balance-history source binding differs from the consolidated dump"
    );
    Ok(())
}

fn registry_id_in_source(source: &SourceDump, key: [u8; 32]) -> Result<Option<u32>> {
    let mut left = 0u64;
    let mut right = source.pubkeys;
    let mut encoded = [0u8; REGISTRY_KEY_BYTES as usize];
    while left < right {
        let middle = left + (right - left) / 2;
        positioned_read_exact(
            source.registry_handle.file(),
            &mut encoded,
            middle
                .checked_mul(REGISTRY_KEY_BYTES)
                .context("registry lookup byte offset overflow")?,
        )?;
        match encoded.cmp(&key) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => {
                return Ok(Some(
                    u32::try_from(
                        middle
                            .checked_add(1)
                            .context("registry lookup ID overflow")?,
                    )
                    .context("registry lookup ID exceeds u32")?,
                ));
            }
        }
    }
    Ok(None)
}

fn validate_final_spyx_counts(manifest: &PostingsManifest) -> Result<()> {
    ensure!(
        manifest.source.transaction_sha256 == FINAL_SPYX_TRANSACTION_SHA256
            && manifest.source.transactions == FINAL_SPYX_TRANSACTIONS
            && manifest.transactions == FINAL_SPYX_TRANSACTIONS
            && manifest.source.accounts == FINAL_SPYX_DISCOVERED_ACCOUNTS
            && manifest.target_address_directory.records == FINAL_SPYX_TARGET_KEYS
            && manifest.target_address_postings.records == FINAL_SPYX_TARGET_POSTINGS
            && manifest.program_directory.records == FINAL_SPYX_PROGRAM_KEYS
            && manifest.program_postings.records == FINAL_SPYX_PROGRAM_POSTINGS
            && manifest.program_direct_directory.records == FINAL_SPYX_PROGRAM_KEYS
            && manifest.program_inner_directory.records == FINAL_SPYX_PROGRAM_KEYS,
        "complete postings do not satisfy the final SPYx acceptance counts"
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct PostingPairValidation {
    transaction_count: u64,
    registry_entries: u64,
    target_mint_id: Option<u32>,
    final_spyx: bool,
}

fn validate_posting_pair(
    kind: PostingsDirectoryKind,
    directory_file: &PinnedPostingsFile,
    body_file: &PinnedPostingsFile,
    validation: PostingPairValidation,
    scratch: &mut Vec<u8>,
) -> Result<[u8; 32]> {
    let PostingPairValidation {
        transaction_count,
        registry_entries,
        target_mint_id,
        final_spyx,
    } = validation;
    ensure!(
        directory_file.record_bytes == POSTINGS_DIRECTORY_RECORD_BYTES,
        "postings directory record size differs"
    );
    ensure!(
        body_file.record_bytes == POSTINGS_BODY_RECORD_BYTES,
        "postings body record size differs"
    );
    if kind == PostingsDirectoryKind::TargetAddress {
        ensure!(
            target_mint_id.is_some(),
            "target-address validation has no source mint ID"
        );
    } else {
        ensure!(
            target_mint_id.is_none(),
            "program validation unexpectedly has a target mint ID"
        );
    }

    let require_transaction_coverage = final_spyx && kind == PostingsDirectoryKind::TargetAddress;
    let coverage_bytes = if require_transaction_coverage {
        usize::try_from(transaction_count.div_ceil(u8::BITS.into()))
            .context("target transaction coverage bitset exceeds usize")?
    } else {
        0
    };
    ensure_validation_scratch(scratch)?;
    ensure!(
        coverage_bytes
            .checked_add(POSTINGS_DIRECTORY_RECORD_BYTES)
            .and_then(|bytes| bytes.checked_add(POSTINGS_BODY_RECORD_BYTES))
            .is_some_and(|minimum| minimum <= scratch.len()),
        "target transaction coverage does not fit the fixed validation scratch"
    );
    scratch[..coverage_bytes].fill(0);
    let (coverage, scan_scratch) = scratch.split_at_mut(coverage_bytes);
    let desired_directory_bytes =
        (1usize << 20).min(scan_scratch.len() - POSTINGS_BODY_RECORD_BYTES);
    let directory_buffer_bytes = (desired_directory_bytes / POSTINGS_DIRECTORY_RECORD_BYTES)
        * POSTINGS_DIRECTORY_RECORD_BYTES;
    ensure!(
        directory_buffer_bytes >= POSTINGS_DIRECTORY_RECORD_BYTES,
        "fixed validation scratch cannot hold one directory row"
    );
    let (directory_buffer, body_storage) = scan_scratch.split_at_mut(directory_buffer_bytes);
    let body_buffer_bytes =
        (body_storage.len() / POSTINGS_BODY_RECORD_BYTES) * POSTINGS_BODY_RECORD_BYTES;
    ensure!(
        body_buffer_bytes >= POSTINGS_BODY_RECORD_BYTES,
        "fixed validation scratch cannot hold one posting row"
    );
    let body_buffer = &mut body_storage[..body_buffer_bytes];

    let mut directory_header = [0u8; POSTINGS_HEADER_BYTES];
    positioned_read_exact(directory_file.file.file(), &mut directory_header, 0)?;
    let mut directory_hasher = Sha256::new();
    directory_hasher.update(directory_header);
    let mut body_header = [0u8; POSTINGS_HEADER_BYTES];
    positioned_read_exact(body_file.file.file(), &mut body_header, 0)?;
    let mut body_hasher = Sha256::new();
    body_hasher.update(body_header);
    let mut semantic = PostingsSemanticHasher::new(kind, body_file.record_count);

    let directory_rows_per_chunk = directory_buffer_bytes / POSTINGS_DIRECTORY_RECORD_BYTES;
    let body_rows_per_chunk = body_buffer_bytes / POSTINGS_BODY_RECORD_BYTES;
    let mut directory_ordinal = 0u64;
    let mut expected_first_posting = 0u64;
    let mut previous_registry_id = None;
    let mut observed_target_mint = false;
    let mut covered_transactions = 0u64;
    while directory_ordinal < directory_file.record_count {
        let rows = usize::try_from(
            (directory_file.record_count - directory_ordinal).min(directory_rows_per_chunk as u64),
        )
        .context("directory validation row count exceeds usize")?;
        let bytes = rows
            .checked_mul(POSTINGS_DIRECTORY_RECORD_BYTES)
            .context("directory validation byte count overflow")?;
        directory_file.read_rows(directory_ordinal, &mut directory_buffer[..bytes])?;
        directory_hasher.update(&directory_buffer[..bytes]);

        for encoded in directory_buffer[..bytes].chunks_exact(POSTINGS_DIRECTORY_RECORD_BYTES) {
            let record = PostingsDirectoryRecord::decode(encoded, kind)?;
            ensure!(
                previous_registry_id.is_none_or(|previous| previous < record.registry_id),
                "postings directory registry IDs are not strictly sorted"
            );
            ensure!(
                u64::from(record.registry_id) <= registry_entries,
                "postings directory row is outside the registry"
            );
            if final_spyx || kind == PostingsDirectoryKind::Owner {
                ensure!(
                    record.posting_count != 0,
                    "postings contain an empty key range where none is permitted"
                );
            }
            if let Some(expected_mint) = target_mint_id {
                let is_mint = record.flags == TARGET_ADDRESS_FLAG_MINT;
                ensure!(
                    is_mint == (record.registry_id == expected_mint),
                    "target-address mint role differs from the source mint"
                );
                observed_target_mint |= is_mint;
            }
            ensure!(
                record.first_posting_row == expected_first_posting,
                "postings directory ranges are not contiguous"
            );
            let end = record.end_posting_row()?;
            ensure!(
                end <= body_file.record_count,
                "postings directory range exceeds its body"
            );

            let mut posting_ordinal = record.first_posting_row;
            let mut previous_transaction = None;
            while posting_ordinal < end {
                let posting_rows =
                    usize::try_from((end - posting_ordinal).min(body_rows_per_chunk as u64))
                        .context("posting validation row count exceeds usize")?;
                let posting_bytes = posting_rows
                    .checked_mul(POSTINGS_BODY_RECORD_BYTES)
                    .context("posting validation byte count overflow")?;
                body_file.read_rows(posting_ordinal, &mut body_buffer[..posting_bytes])?;
                body_hasher.update(&body_buffer[..posting_bytes]);
                for posting_bytes in
                    body_buffer[..posting_bytes].chunks_exact(POSTINGS_BODY_RECORD_BYTES)
                {
                    let posting = PostingRecord::decode(posting_bytes)?;
                    ensure!(
                        posting.transaction_ordinal < transaction_count,
                        "posting transaction ordinal is outside the indexed source prefix"
                    );
                    ensure!(
                        previous_transaction
                            .is_none_or(|previous| { previous < posting.transaction_ordinal }),
                        "one postings range is not strictly sorted and unique"
                    );
                    semantic.update(
                        record.registry_id,
                        record.flags,
                        posting.transaction_ordinal,
                    )?;
                    if require_transaction_coverage {
                        let index = usize::try_from(posting.transaction_ordinal)
                            .context("target transaction ordinal exceeds usize")?;
                        let byte = &mut coverage[index / u8::BITS as usize];
                        let mask = 1u8 << (index % u8::BITS as usize);
                        if *byte & mask == 0 {
                            *byte |= mask;
                            covered_transactions = covered_transactions
                                .checked_add(1)
                                .context("covered transaction count overflow")?;
                        }
                    }
                    previous_transaction = Some(posting.transaction_ordinal);
                }
                posting_ordinal = posting_ordinal
                    .checked_add(u64::try_from(posting_rows)?)
                    .context("posting validation ordinal overflow")?;
            }
            expected_first_posting = end;
            previous_registry_id = Some(record.registry_id);
        }
        directory_ordinal = directory_ordinal
            .checked_add(u64::try_from(rows)?)
            .context("directory validation ordinal overflow")?;
    }

    ensure!(
        expected_first_posting == body_file.record_count,
        "postings directory does not cover its body exactly"
    );
    if final_spyx && kind == PostingsDirectoryKind::TargetAddress {
        ensure!(
            observed_target_mint,
            "complete target-address directory has no source mint row"
        );
        ensure!(
            covered_transactions == transaction_count,
            "complete target-address postings do not cover every source transaction"
        );
    }
    ensure!(
        <[u8; 32]>::from(directory_hasher.finalize()) == directory_file.expected_sha256,
        "postings directory digest differs from its manifest"
    );
    ensure!(
        <[u8; 32]>::from(body_hasher.finalize()) == body_file.expected_sha256,
        "postings body digest differs from its manifest"
    );
    let semantic = semantic.finish()?;
    directory_file.file.verify_identity("postings directory")?;
    body_file.file.verify_identity("postings body")?;
    Ok(semantic)
}

fn validate_owner_balance_history_pair(
    directory_file: &PinnedPostingsFile,
    event_file: &PinnedPostingsFile,
    transaction_count: u64,
    registry_entries: u64,
    scratch: &mut Vec<u8>,
) -> Result<[u8; 32]> {
    ensure!(
        directory_file.record_bytes == POSTINGS_DIRECTORY_RECORD_BYTES
            && event_file.record_bytes == OWNER_BALANCE_EVENT_RECORD_BYTES,
        "owner balance-history record sizes differ"
    );
    ensure_validation_scratch(scratch)?;
    let desired_directory_bytes =
        (1usize << 20).min(scratch.len() - OWNER_BALANCE_EVENT_RECORD_BYTES);
    let directory_buffer_bytes = (desired_directory_bytes / POSTINGS_DIRECTORY_RECORD_BYTES)
        * POSTINGS_DIRECTORY_RECORD_BYTES;
    let (directory_buffer, event_storage) = scratch.split_at_mut(directory_buffer_bytes);
    let event_buffer_bytes =
        (event_storage.len() / OWNER_BALANCE_EVENT_RECORD_BYTES) * OWNER_BALANCE_EVENT_RECORD_BYTES;
    ensure!(
        directory_buffer_bytes >= POSTINGS_DIRECTORY_RECORD_BYTES
            && event_buffer_bytes >= OWNER_BALANCE_EVENT_RECORD_BYTES,
        "fixed validation scratch cannot hold owner balance-history rows"
    );
    let event_buffer = &mut event_storage[..event_buffer_bytes];

    let mut directory_header = [0u8; OWNER_BALANCE_HISTORY_HEADER_BYTES];
    positioned_read_exact(directory_file.file.file(), &mut directory_header, 0)?;
    let mut directory_hasher = Sha256::new();
    directory_hasher.update(directory_header);
    let mut event_header = [0u8; OWNER_BALANCE_HISTORY_HEADER_BYTES];
    positioned_read_exact(event_file.file.file(), &mut event_header, 0)?;
    let mut event_hasher = Sha256::new();
    event_hasher.update(event_header);
    let mut semantic = OwnerBalanceHistorySemanticHasher::new(event_file.record_count);

    let directory_rows_per_chunk = directory_buffer_bytes / POSTINGS_DIRECTORY_RECORD_BYTES;
    let event_rows_per_chunk = event_buffer_bytes / OWNER_BALANCE_EVENT_RECORD_BYTES;
    let mut directory_ordinal = 0u64;
    let mut expected_first_event = 0u64;
    let mut previous_registry_id = None;
    while directory_ordinal < directory_file.record_count {
        let rows = usize::try_from(
            (directory_file.record_count - directory_ordinal).min(directory_rows_per_chunk as u64),
        )
        .context("owner balance-history directory chunk exceeds usize")?;
        let bytes = rows
            .checked_mul(POSTINGS_DIRECTORY_RECORD_BYTES)
            .context("owner balance-history directory chunk byte count overflow")?;
        directory_file.read_rows(directory_ordinal, &mut directory_buffer[..bytes])?;
        directory_hasher.update(&directory_buffer[..bytes]);

        for encoded in directory_buffer[..bytes].chunks_exact(POSTINGS_DIRECTORY_RECORD_BYTES) {
            let directory = PostingsDirectoryRecord::decode(encoded, PostingsDirectoryKind::Owner)?;
            ensure!(
                previous_registry_id.is_none_or(|previous| previous < directory.registry_id)
                    && u64::from(directory.registry_id) <= registry_entries
                    && directory.posting_count != 0
                    && directory.first_posting_row == expected_first_event,
                "owner balance-history directory is not canonical"
            );
            let end = directory.end_posting_row()?;
            ensure!(
                end <= event_file.record_count,
                "owner balance-history directory range exceeds its events"
            );
            let mut event_ordinal = directory.first_posting_row;
            let mut previous_transaction_id = None;
            let mut previous_post_balance = 0u128;
            while event_ordinal < end {
                let event_rows =
                    usize::try_from((end - event_ordinal).min(event_rows_per_chunk as u64))
                        .context("owner balance-history event chunk exceeds usize")?;
                let event_bytes = event_rows
                    .checked_mul(OWNER_BALANCE_EVENT_RECORD_BYTES)
                    .context("owner balance-history event chunk byte count overflow")?;
                event_file.read_rows(event_ordinal, &mut event_buffer[..event_bytes])?;
                event_hasher.update(&event_buffer[..event_bytes]);
                for event_bytes in
                    event_buffer[..event_bytes].chunks_exact(OWNER_BALANCE_EVENT_RECORD_BYTES)
                {
                    let event = OwnerBalanceEventRecord::decode(event_bytes)?;
                    ensure!(
                        event.transaction_id < transaction_count
                            && previous_transaction_id
                                .is_none_or(|previous| previous < event.transaction_id),
                        "owner balance-history owner range is not strictly ordered"
                    );
                    let expected_post_balance = if event.raw_delta > 0 {
                        previous_post_balance
                            .checked_add(event.raw_delta.unsigned_abs())
                            .context("owner balance-history validation overflow")?
                    } else {
                        previous_post_balance
                            .checked_sub(event.raw_delta.unsigned_abs())
                            .context("owner balance-history validation underflow")?
                    };
                    ensure!(
                        event.post_raw_balance == expected_post_balance,
                        "owner balance-history post balance differs from its exact delta chain"
                    );
                    semantic.update(directory.registry_id, event)?;
                    previous_transaction_id = Some(event.transaction_id);
                    previous_post_balance = event.post_raw_balance;
                }
                event_ordinal = event_ordinal
                    .checked_add(u64::try_from(event_rows)?)
                    .context("owner balance-history validation row overflow")?;
            }
            expected_first_event = end;
            previous_registry_id = Some(directory.registry_id);
        }
        directory_ordinal = directory_ordinal
            .checked_add(u64::try_from(rows)?)
            .context("owner balance-history directory ordinal overflow")?;
    }
    ensure!(
        expected_first_event == event_file.record_count,
        "owner balance-history directory does not cover all events"
    );
    ensure!(
        <[u8; 32]>::from(directory_hasher.finalize()) == directory_file.expected_sha256,
        "owner balance-history directory digest differs from its manifest"
    );
    ensure!(
        <[u8; 32]>::from(event_hasher.finalize()) == event_file.expected_sha256,
        "owner balance-history event digest differs from its manifest"
    );
    directory_file
        .file
        .verify_identity("owner balance-history directory")?;
    event_file
        .file
        .verify_identity("owner balance-history events")?;
    semantic.finish()
}

fn validate_program_posting_pair(
    scope: ProgramInstructionScope,
    directory_file: &PinnedPostingsFile,
    body_file: &PinnedPostingsFile,
    validation: PostingPairValidation,
    scratch: &mut Vec<u8>,
) -> Result<[u8; 32]> {
    let PostingPairValidation {
        transaction_count,
        registry_entries,
        target_mint_id,
        final_spyx,
    } = validation;
    ensure!(
        target_mint_id.is_none(),
        "program validation unexpectedly has a target mint ID"
    );
    ensure!(
        directory_file.record_bytes == POSTINGS_DIRECTORY_RECORD_BYTES
            && body_file.record_bytes == POSTINGS_BODY_RECORD_BYTES,
        "program postings record size differs"
    );
    ensure_validation_scratch(scratch)?;
    let desired_directory_bytes = (1usize << 20).min(scratch.len() - POSTINGS_BODY_RECORD_BYTES);
    let directory_buffer_bytes = (desired_directory_bytes / POSTINGS_DIRECTORY_RECORD_BYTES)
        * POSTINGS_DIRECTORY_RECORD_BYTES;
    let (directory_buffer, body_storage) = scratch.split_at_mut(directory_buffer_bytes);
    let body_buffer_bytes =
        (body_storage.len() / POSTINGS_BODY_RECORD_BYTES) * POSTINGS_BODY_RECORD_BYTES;
    ensure!(
        directory_buffer_bytes >= POSTINGS_DIRECTORY_RECORD_BYTES
            && body_buffer_bytes >= POSTINGS_BODY_RECORD_BYTES,
        "fixed validation scratch cannot hold scoped program rows"
    );
    let body_buffer = &mut body_storage[..body_buffer_bytes];

    let mut directory_header = [0u8; POSTINGS_HEADER_BYTES];
    positioned_read_exact(directory_file.file.file(), &mut directory_header, 0)?;
    let mut directory_hasher = Sha256::new();
    directory_hasher.update(directory_header);
    let mut body_header = [0u8; POSTINGS_HEADER_BYTES];
    positioned_read_exact(body_file.file.file(), &mut body_header, 0)?;
    let mut body_hasher = Sha256::new();
    body_hasher.update(body_header);
    let mut semantic = ProgramPostingsSemanticHasher::new(scope, body_file.record_count);

    let directory_rows_per_chunk = directory_buffer_bytes / POSTINGS_DIRECTORY_RECORD_BYTES;
    let body_rows_per_chunk = body_buffer_bytes / POSTINGS_BODY_RECORD_BYTES;
    let mut directory_ordinal = 0u64;
    let mut expected_first_posting = 0u64;
    let mut previous_registry_id = None;
    while directory_ordinal < directory_file.record_count {
        let rows = usize::try_from(
            (directory_file.record_count - directory_ordinal).min(directory_rows_per_chunk as u64),
        )
        .context("program directory validation row count exceeds usize")?;
        let bytes = rows
            .checked_mul(POSTINGS_DIRECTORY_RECORD_BYTES)
            .context("program directory validation byte count overflow")?;
        directory_file.read_rows(directory_ordinal, &mut directory_buffer[..bytes])?;
        directory_hasher.update(&directory_buffer[..bytes]);

        for encoded in directory_buffer[..bytes].chunks_exact(POSTINGS_DIRECTORY_RECORD_BYTES) {
            let record = PostingsDirectoryRecord::decode(encoded, PostingsDirectoryKind::Program)?;
            ensure!(
                previous_registry_id.is_none_or(|previous| previous < record.registry_id)
                    && u64::from(record.registry_id) <= registry_entries,
                "program postings directory is not canonical or is outside the registry"
            );
            if final_spyx && scope == ProgramInstructionScope::All {
                ensure!(
                    record.posting_count != 0,
                    "complete all-scope program postings contain an empty key range"
                );
            }
            ensure!(
                record.first_posting_row == expected_first_posting,
                "program postings directory ranges are not contiguous"
            );
            let end = record.end_posting_row()?;
            ensure!(
                end <= body_file.record_count,
                "program postings directory range exceeds its body"
            );

            let mut posting_ordinal = record.first_posting_row;
            let mut previous_transaction = None;
            while posting_ordinal < end {
                let posting_rows =
                    usize::try_from((end - posting_ordinal).min(body_rows_per_chunk as u64))
                        .context("program posting validation row count exceeds usize")?;
                let posting_bytes = posting_rows
                    .checked_mul(POSTINGS_BODY_RECORD_BYTES)
                    .context("program posting validation byte count overflow")?;
                body_file.read_rows(posting_ordinal, &mut body_buffer[..posting_bytes])?;
                body_hasher.update(&body_buffer[..posting_bytes]);
                for encoded in body_buffer[..posting_bytes].chunks_exact(POSTINGS_BODY_RECORD_BYTES)
                {
                    let posting = ProgramPostingRecord::decode(encoded)?;
                    ensure!(
                        posting.transaction_ordinal < transaction_count
                            && scope.includes(posting.instruction_scope_mask),
                        "program posting is outside its source range or instruction scope"
                    );
                    ensure!(
                        previous_transaction
                            .is_none_or(|previous| previous < posting.transaction_ordinal),
                        "one program postings range is not strictly sorted and unique"
                    );
                    semantic.update(
                        record.registry_id,
                        posting.instruction_scope_mask,
                        posting.transaction_ordinal,
                    )?;
                    previous_transaction = Some(posting.transaction_ordinal);
                }
                posting_ordinal = posting_ordinal
                    .checked_add(u64::try_from(posting_rows)?)
                    .context("program posting validation ordinal overflow")?;
            }
            expected_first_posting = end;
            previous_registry_id = Some(record.registry_id);
        }
        directory_ordinal = directory_ordinal
            .checked_add(u64::try_from(rows)?)
            .context("program directory validation ordinal overflow")?;
    }

    ensure!(
        expected_first_posting == body_file.record_count,
        "program postings directory does not cover its body exactly"
    );
    ensure!(
        <[u8; 32]>::from(directory_hasher.finalize()) == directory_file.expected_sha256,
        "program postings directory digest differs from its manifest"
    );
    ensure!(
        <[u8; 32]>::from(body_hasher.finalize()) == body_file.expected_sha256,
        "program postings body digest differs from its manifest"
    );
    let semantic = semantic.finish()?;
    directory_file
        .file
        .verify_identity("program postings directory")?;
    body_file.file.verify_identity("program postings body")?;
    Ok(semantic)
}

fn validate_scoped_program_projection(
    all_directory: &PinnedPostingsFile,
    all_postings: &PinnedPostingsFile,
    direct_directory: &PinnedPostingsFile,
    direct_postings: &PinnedPostingsFile,
    inner_directory: &PinnedPostingsFile,
    inner_postings: &PinnedPostingsFile,
    scratch: &mut Vec<u8>,
) -> Result<([u8; 32], [u8; 32])> {
    ensure!(
        all_directory.record_count == direct_directory.record_count
            && all_directory.record_count == inner_directory.record_count,
        "scoped program directories do not contain the all-scope keys"
    );
    ensure_validation_scratch(scratch)?;
    let buffer_bytes = (scratch.len() / POSTINGS_BODY_RECORD_BYTES) * POSTINGS_BODY_RECORD_BYTES;
    ensure!(
        buffer_bytes >= POSTINGS_BODY_RECORD_BYTES,
        "scoped program projection scratch cannot hold one posting"
    );
    let buffer = &mut scratch[..buffer_bytes];
    let rows_per_chunk = buffer_bytes / POSTINGS_BODY_RECORD_BYTES;
    let mut direct_semantic = ProgramPostingsSemanticHasher::new(
        ProgramInstructionScope::Direct,
        direct_postings.record_count,
    );
    let mut inner_semantic = ProgramPostingsSemanticHasher::new(
        ProgramInstructionScope::Inner,
        inner_postings.record_count,
    );
    let mut derived_direct_rows = 0u64;
    let mut derived_inner_rows = 0u64;

    for directory_ordinal in 0..all_directory.record_count {
        let all =
            all_directory.read_directory_row(PostingsDirectoryKind::Program, directory_ordinal)?;
        let direct = direct_directory
            .read_directory_row(PostingsDirectoryKind::Program, directory_ordinal)?;
        let inner = inner_directory
            .read_directory_row(PostingsDirectoryKind::Program, directory_ordinal)?;
        ensure!(
            all.registry_id == direct.registry_id
                && all.registry_id == inner.registry_id
                && direct.first_posting_row == derived_direct_rows
                && inner.first_posting_row == derived_inner_rows,
            "scoped program directory keys or range starts differ from all postings"
        );
        let direct_start = derived_direct_rows;
        let inner_start = derived_inner_rows;
        let end = all.end_posting_row()?;
        let mut posting_ordinal = all.first_posting_row;
        while posting_ordinal < end {
            let rows = usize::try_from((end - posting_ordinal).min(rows_per_chunk as u64))
                .context("scoped program projection row count exceeds usize")?;
            let bytes = rows
                .checked_mul(POSTINGS_BODY_RECORD_BYTES)
                .context("scoped program projection byte count overflow")?;
            all_postings.read_rows(posting_ordinal, &mut buffer[..bytes])?;
            for encoded in buffer[..bytes].chunks_exact(POSTINGS_BODY_RECORD_BYTES) {
                let posting = ProgramPostingRecord::decode(encoded)?;
                if ProgramInstructionScope::Direct.includes(posting.instruction_scope_mask) {
                    direct_semantic.update(
                        all.registry_id,
                        posting.instruction_scope_mask,
                        posting.transaction_ordinal,
                    )?;
                    derived_direct_rows = derived_direct_rows
                        .checked_add(1)
                        .context("derived direct program posting count overflow")?;
                }
                if ProgramInstructionScope::Inner.includes(posting.instruction_scope_mask) {
                    inner_semantic.update(
                        all.registry_id,
                        posting.instruction_scope_mask,
                        posting.transaction_ordinal,
                    )?;
                    derived_inner_rows = derived_inner_rows
                        .checked_add(1)
                        .context("derived inner program posting count overflow")?;
                }
            }
            posting_ordinal = posting_ordinal
                .checked_add(u64::try_from(rows)?)
                .context("scoped program projection ordinal overflow")?;
        }
        ensure!(
            direct.posting_count == derived_direct_rows - direct_start
                && inner.posting_count == derived_inner_rows - inner_start,
            "scoped program directory counts differ from all posting masks"
        );
    }
    ensure!(
        derived_direct_rows == direct_postings.record_count
            && derived_inner_rows == inner_postings.record_count,
        "scoped program posting totals differ from all posting masks"
    );
    Ok((direct_semantic.finish()?, inner_semantic.finish()?))
}

fn validate_registry_source(source: &SourceDump, scratch: &mut Vec<u8>) -> Result<u32> {
    ensure_validation_scratch(scratch)?;
    let rows_per_chunk = (scratch.len() / REGISTRY_KEY_BYTES as usize).max(1);
    let mut hasher = Sha256::new();
    let mut ordinal = 0u64;
    let mut previous = None::<[u8; REGISTRY_KEY_BYTES as usize]>;
    let mut mint_registry_id = None;
    while ordinal < source.pubkeys {
        let rows = usize::try_from((source.pubkeys - ordinal).min(rows_per_chunk as u64))
            .context("registry validation row count exceeds usize")?;
        let bytes = rows
            .checked_mul(REGISTRY_KEY_BYTES as usize)
            .context("registry validation byte count overflow")?;
        positioned_read_exact(
            source.registry_handle.file(),
            &mut scratch[..bytes],
            ordinal
                .checked_mul(REGISTRY_KEY_BYTES)
                .context("registry validation offset overflow")?,
        )?;
        hasher.update(&scratch[..bytes]);
        for encoded in scratch[..bytes].chunks_exact(REGISTRY_KEY_BYTES as usize) {
            let key: [u8; REGISTRY_KEY_BYTES as usize] =
                encoded.try_into().expect("fixed registry key row");
            ensure!(
                previous.is_none_or(|value| value < key),
                "source registry is not strictly sorted and unique"
            );
            if key == source.mint {
                ensure!(
                    mint_registry_id.is_none(),
                    "source registry repeats its mint"
                );
                mint_registry_id = Some(
                    u32::try_from(
                        ordinal
                            .checked_add(1)
                            .context("source mint registry ID overflow")?,
                    )
                    .context("source mint registry ID exceeds u32")?,
                );
            }
            previous = Some(key);
            ordinal = ordinal
                .checked_add(1)
                .context("registry validation ordinal overflow")?;
        }
    }
    ensure!(
        <[u8; 32]>::from(hasher.finalize()) == source.registry_sha256,
        "source registry digest differs during postings startup"
    );
    source
        .registry_handle
        .verify_identity("source public-key registry")?;
    mint_registry_id.context("source mint is absent from the registry")
}

fn hash_pinned_file_reused(file: &PinnedSourceFile, scratch: &mut Vec<u8>) -> Result<[u8; 32]> {
    ensure_validation_scratch(scratch)?;
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    while offset < file.len() {
        let bytes = usize::try_from((file.len() - offset).min(scratch.len() as u64))
            .context("source hash byte count exceeds usize")?;
        positioned_read_exact(file.file(), &mut scratch[..bytes], offset)?;
        hasher.update(&scratch[..bytes]);
        offset = offset
            .checked_add(u64::try_from(bytes)?)
            .context("source hash offset overflow")?;
    }
    file.verify_identity("hashed source file")?;
    Ok(hasher.finalize().into())
}

fn ensure_validation_scratch(scratch: &mut Vec<u8>) -> Result<()> {
    if scratch.len() < VALIDATION_BUFFER_BYTES {
        scratch
            .try_reserve_exact(VALIDATION_BUFFER_BYTES - scratch.len())
            .context("reserve fixed postings validation scratch")?;
        scratch.resize(VALIDATION_BUFFER_BYTES, 0);
    } else {
        scratch.truncate(VALIDATION_BUFFER_BYTES);
    }
    Ok(())
}

#[cfg(unix)]
fn positioned_read_exact(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(bytes, offset)?;
    Ok(())
}

#[cfg(windows)]
fn positioned_read_exact(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;

    let mut read = 0usize;
    while read < bytes.len() {
        let current = offset
            .checked_add(u64::try_from(read)?)
            .context("positioned postings read offset overflow")?;
        let count = file.seek_read(&mut bytes[read..], current)?;
        ensure!(count != 0, "positioned postings read reached end of file");
        read += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn positioned_read_exact(_file: &std::fs::File, _bytes: &mut [u8], _offset: u64) -> Result<()> {
    anyhow::bail!("positioned file reads are not supported on this platform")
}
