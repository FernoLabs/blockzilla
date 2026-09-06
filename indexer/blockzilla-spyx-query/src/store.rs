use std::{
    fs::{self, File},
    path::Path,
};

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use blockzilla_primitives::WINCODE_LEB128_MAX_FRAME_BYTES;
use blockzilla_token_transaction_dump::{
    DumpWireProfile,
    consolidated_posting_projection::{
        ConsolidatedPostingProjectionScratch, project_consolidated_transaction_postings,
    },
    consolidated_reader::{BorrowedDumpRecord, read_frame_at},
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    builder::KNOWN_TRANSACTION_FLAGS,
    index_format::{
        INDEX_FLAG_COMPLETE, INDEX_HEADER_BYTES, INDEX_MANIFEST_FILE, INDEX_SCHEMA_VERSION,
        IndexFileBinding, IndexHeader, IndexManifest, LOCATOR_MAGIC, LOCATOR_RECORD_BYTES,
        LOCATORS_FILE, LocatorRecord, SIGNATURE_LOOKUP_FILE, SIGNATURE_MAGIC,
        SIGNATURE_RECORD_BYTES, SignatureIndexRecord, TransactionCoordinate, encoded_file_bytes,
        hex_digest, parse_hex_digest,
    },
    source::{PinnedSourceFile, hash_pinned_file, load_source_dump},
};

const MAX_INDEX_MANIFEST_BYTES: u64 = 16 << 20;
const SIGNATURE_BYTES: u64 = 64;
const PUBLIC_KEY_BYTES: u64 = 32;
const INDEX_SCAN_BUFFER_BYTES: usize = 8 << 20;
const MAX_SIGNATURE_COUNT_TABLE_BYTES: u64 = 64 << 20;

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryOpenOptions {
    pub allow_incomplete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignatureLookup {
    pub occurrences: Vec<SignatureOccurrence>,
    pub transaction_ids: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SignatureOccurrence {
    pub transaction_id: u64,
    pub signature_position: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionDetail {
    pub id: u64,
    pub coordinate: TransactionCoordinate,
    pub block: TransactionBlockDetail,
    pub signatures: Vec<String>,
    pub accounts: Vec<TransactionAccountDetail>,
    pub flags: u32,
    pub source_wire_profile: DumpWireProfile,
    pub message_bytes_base64: String,
    pub metadata_bytes_base64: String,
}

/// One resolved message account in canonical static-plus-loaded order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionAccountDetail {
    pub account_index: u16,
    pub registry_id: u32,
    pub address: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionBlockDetail {
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transaction_count: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct PostingTransactionDetail {
    pub transaction_id: u64,
    pub coordinate: TransactionCoordinate,
    pub first_signature: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifySummary {
    pub complete: bool,
    pub transactions: u64,
    pub signature_occurrences: u64,
    pub source_transaction_sha256: String,
}

pub struct QueryStore {
    index_manifest: PinnedSourceFile,
    manifest: IndexManifest,
    locators: PinnedIndexFile,
    signatures: PinnedIndexFile,
    source: crate::source::SourceDump,
}

impl QueryStore {
    pub fn open(dump: &Path, index: &Path) -> Result<Self> {
        Self::open_with_options(dump, index, QueryOpenOptions::default())
    }

    pub fn open_with_options(dump: &Path, index: &Path, options: QueryOpenOptions) -> Result<Self> {
        let source = load_source_dump(dump)?;
        let index_root = fs::canonicalize(index)
            .with_context(|| format!("resolve query index {}", index.display()))?;
        ensure!(index_root.is_dir(), "query index is not a directory");
        let manifest_path = index_root.join(INDEX_MANIFEST_FILE);
        let index_manifest = PinnedSourceFile::open(&manifest_path, "index manifest")?;
        let manifest_bytes = index_manifest.read_bounded(MAX_INDEX_MANIFEST_BYTES)?;
        let manifest: IndexManifest =
            serde_json::from_slice(&manifest_bytes).context("parse query index manifest")?;
        validate_manifest(&manifest, &source, options)?;

        let source_manifest_sha256 = parse_hex_digest(
            &manifest.source.manifest_sha256,
            "index source manifest digest",
        )?;
        let source_transaction_sha256 = parse_hex_digest(
            &manifest.source.transaction_sha256,
            "index source transaction digest",
        )?;
        let locator_path = index_root.join(LOCATORS_FILE);
        let signature_path = index_root.join(SIGNATURE_LOOKUP_FILE);
        let locators = PinnedIndexFile::open(
            &locator_path,
            &manifest.locators,
            LOCATOR_MAGIC,
            LOCATOR_RECORD_BYTES as u16,
            source_manifest_sha256,
            source_transaction_sha256,
            manifest.complete,
        )?;
        let signatures = PinnedIndexFile::open(
            &signature_path,
            &manifest.signature_lookup,
            SIGNATURE_MAGIC,
            SIGNATURE_RECORD_BYTES as u16,
            source_manifest_sha256,
            source_transaction_sha256,
            manifest.complete,
        )?;

        let mut scan_scratch = Vec::new();
        let locator_validation =
            validate_locator_order(&locators, &manifest, &source, false, &mut scan_scratch)?;
        ensure!(
            locator_validation.sha256 == locators.expected_sha256,
            "locator index digest differs from its manifest"
        );
        let signature_sha256 = validate_signature_order(
            &signatures,
            &manifest,
            &locator_validation.signature_counts,
            None,
            &mut scan_scratch,
        )?;
        ensure!(
            signature_sha256 == signatures.expected_sha256,
            "signature index digest differs from its manifest"
        );
        index_manifest.verify_identity("index manifest")?;
        source.verify_file_identities()?;
        Ok(Self {
            index_manifest,
            manifest,
            locators,
            signatures,
            source,
        })
    }

    pub const fn complete(&self) -> bool {
        self.manifest.complete
    }

    pub const fn transaction_count(&self) -> u64 {
        self.manifest.transactions
    }

    pub const fn signature_occurrence_count(&self) -> u64 {
        self.manifest.signature_occurrences
    }

    pub fn source_transaction_sha256(&self) -> &str {
        &self.manifest.source.transaction_sha256
    }

    pub fn lookup_coordinate(&self, coordinate: TransactionCoordinate) -> Result<Option<u64>> {
        let mut left = 0u64;
        let mut right = self.transaction_count();
        while left < right {
            let middle = left + (right - left) / 2;
            let record = self.locator(middle)?;
            match record.coordinate.cmp(&coordinate) {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok(Some(middle)),
            }
        }
        Ok(None)
    }

    pub fn lookup_signature(&self, signature: [u8; 64]) -> Result<SignatureLookup> {
        let mut left = 0u64;
        let mut right = self.signature_occurrence_count();
        while left < right {
            let middle = left + (right - left) / 2;
            if self.signature(middle)?.signature < signature {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        let first = left;
        right = self.signature_occurrence_count();
        while left < right {
            let middle = left + (right - left) / 2;
            if self.signature(middle)?.signature <= signature {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        let match_count =
            usize::try_from(left - first).context("signature match count exceeds usize")?;
        let mut occurrences = Vec::with_capacity(match_count);
        let mut transaction_ids = Vec::new();
        for ordinal in first..left {
            let match_record = self.signature(ordinal)?;
            let transaction_id = match_record.transaction_id;
            occurrences.push(SignatureOccurrence {
                transaction_id,
                signature_position: match_record.signature_position,
            });
            if transaction_ids.last().copied() != Some(transaction_id) {
                transaction_ids.push(transaction_id);
            }
        }
        Ok(SignatureLookup {
            occurrences,
            transaction_ids,
        })
    }

    /// Read the fixed locator and first source signature needed by one posting row.
    ///
    /// This does not decode or copy the transaction frame.
    pub fn posting_transaction_detail(&self, id: u64) -> Result<PostingTransactionDetail> {
        let locator = self.locator(id)?;
        let signature_offset = locator
            .first_signature_ordinal
            .checked_mul(SIGNATURE_BYTES)
            .context("posting first-signature offset overflow")?;
        ensure!(
            signature_offset
                .checked_add(SIGNATURE_BYTES)
                .is_some_and(|end| end <= self.source.signature_bytes),
            "posting first signature is outside signatures.bin"
        );
        let mut signature = [0u8; SIGNATURE_BYTES as usize];
        positioned_read_exact(
            self.source.signature_handle.file(),
            &mut signature,
            signature_offset,
        )?;
        Ok(PostingTransactionDetail {
            transaction_id: id,
            coordinate: locator.coordinate,
            first_signature: bs58::encode(signature).into_string(),
        })
    }

    /// Read one original frame and its signature range with one reused scratch buffer.
    pub fn transaction_detail(&self, id: u64, scratch: &mut Vec<u8>) -> Result<TransactionDetail> {
        let locator = self.locator(id)?;
        let registry_entries = u32::try_from(self.source.pubkeys)
            .context("public-key registry entry count exceeds u32")?;
        let mut projection_scratch = ConsolidatedPostingProjectionScratch::new(registry_entries)?;
        let mut detail = {
            let decoded = read_frame_at(
                self.source.transaction_handle.file(),
                locator.frame,
                scratch,
            )?;
            let BorrowedDumpRecord::Transaction(record) = decoded else {
                bail!("located source frame is not a transaction")
            };
            let coordinate = TransactionCoordinate {
                epoch: record.source_epoch,
                slot: record.block.slot,
                source_block_id: record.source_block_id,
                tx_index: record.tx_index,
            };
            ensure!(
                coordinate == locator.coordinate
                    && record.dump_signature_ordinal == Some(locator.first_signature_ordinal)
                    && record.signature_count == locator.signature_count
                    && record.flags == locator.flags
                    && record.source_wire_profile == locator.source_wire_profile
                    && record.block.parent_slot == locator.parent_slot
                    && record.block.block_time == locator.block_time
                    && record.block.block_height == locator.block_height
                    && record.block.transaction_count == locator.transaction_count,
                "located source transaction differs from its immutable locator"
            );
            let projection = project_consolidated_transaction_postings(
                &record,
                registry_entries,
                &mut projection_scratch,
            )
            .context("project exact consolidated transaction accounts")?;
            let mut accounts = Vec::new();
            accounts
                .try_reserve_exact(projection.resolved_account_registry_ids.len())
                .context("reserve transaction account details")?;
            for (account_index, &registry_id) in
                projection.resolved_account_registry_ids.iter().enumerate()
            {
                let registry_ordinal = u64::from(
                    registry_id
                        .checked_sub(1)
                        .context("resolved transaction account registry ID is zero")?,
                );
                let registry_offset = registry_ordinal
                    .checked_mul(PUBLIC_KEY_BYTES)
                    .context("transaction account registry offset overflow")?;
                ensure!(
                    registry_offset
                        .checked_add(PUBLIC_KEY_BYTES)
                        .is_some_and(|end| end <= self.source.registry_bytes),
                    "resolved transaction account is outside the public-key registry"
                );
                let mut key = [0u8; PUBLIC_KEY_BYTES as usize];
                positioned_read_exact(
                    self.source.registry_handle.file(),
                    &mut key,
                    registry_offset,
                )
                .context("read resolved transaction account from public-key registry")?;
                accounts.push(TransactionAccountDetail {
                    account_index: u16::try_from(account_index)
                        .context("transaction account index exceeds u16")?,
                    registry_id,
                    address: bs58::encode(key).into_string(),
                });
            }
            TransactionDetail {
                id,
                coordinate,
                block: TransactionBlockDetail {
                    parent_slot: locator.parent_slot,
                    block_time: locator.block_time,
                    block_height: locator.block_height,
                    transaction_count: locator.transaction_count,
                },
                signatures: Vec::with_capacity(usize::from(locator.signature_count)),
                accounts,
                flags: locator.flags,
                source_wire_profile: locator.source_wire_profile,
                message_bytes_base64: BASE64_STANDARD.encode(record.message_bytes),
                metadata_bytes_base64: BASE64_STANDARD.encode(record.metadata_bytes),
            }
        };

        let signature_bytes = usize::from(locator.signature_count)
            .checked_mul(SIGNATURE_BYTES as usize)
            .context("transaction signature byte length overflow")?;
        let signature_offset = locator
            .first_signature_ordinal
            .checked_mul(SIGNATURE_BYTES)
            .context("transaction signature offset overflow")?;
        let signature_end = signature_offset
            .checked_add(u64::try_from(signature_bytes)?)
            .context("transaction signature end overflow")?;
        ensure!(
            signature_end <= self.source.signature_bytes,
            "transaction signature range exceeds signatures.bin"
        );
        scratch.resize(signature_bytes, 0);
        positioned_read_exact(
            self.source.signature_handle.file(),
            scratch,
            signature_offset,
        )?;
        for signature in scratch.chunks_exact(SIGNATURE_BYTES as usize) {
            detail
                .signatures
                .push(bs58::encode(signature).into_string());
        }
        Ok(detail)
    }

    fn locator(&self, id: u64) -> Result<LocatorRecord> {
        ensure!(
            id < self.transaction_count(),
            "transaction ID is outside the index"
        );
        let mut bytes = [0u8; LOCATOR_RECORD_BYTES];
        self.locators.read_row(id, &mut bytes)?;
        LocatorRecord::decode(&bytes)
    }

    fn signature(&self, ordinal: u64) -> Result<SignatureIndexRecord> {
        ensure!(
            ordinal < self.signature_occurrence_count(),
            "signature ordinal is outside the index"
        );
        let mut bytes = [0u8; SIGNATURE_RECORD_BYTES];
        self.signatures.read_row(ordinal, &mut bytes)?;
        SignatureIndexRecord::decode(&bytes)
    }
}

/// Fully verify the source and index bytes, including every signature mapping.
///
/// The explicit semantic check loads `signatures.bin` and one `u64` start
/// ordinal and one `u8` count per indexed transaction. For the current SPYx
/// corpus this is about 483 MB plus 59 MB plus 7 MB, in addition to one
/// reusable 8 MiB scan buffer.
pub fn verify_index(dump: &Path, index: &Path, allow_incomplete: bool) -> Result<VerifySummary> {
    let store = QueryStore::open_with_options(dump, index, QueryOpenOptions { allow_incomplete })?;
    for (file, expected, label) in [
        (
            &store.source.transaction_handle,
            store.source.transaction_sha256,
            "transaction stream",
        ),
        (
            &store.source.registry_handle,
            store.source.registry_sha256,
            "registry",
        ),
        (
            &store.source.accounts_handle,
            store.source.accounts_sha256,
            "accounts",
        ),
    ] {
        ensure!(
            hash_pinned_file(file)? == expected,
            "{label} digest differs during full verification"
        );
    }

    let source_signatures = store
        .source
        .signature_handle
        .read_bounded(store.source.signature_bytes)?;
    let observed_signature_sha256: [u8; 32] = Sha256::digest(&source_signatures).into();
    ensure!(
        observed_signature_sha256 == store.source.signature_sha256,
        "signature stream digest differs during full verification"
    );

    let mut scan_scratch = Vec::new();
    let locator_validation = validate_locator_order(
        &store.locators,
        &store.manifest,
        &store.source,
        true,
        &mut scan_scratch,
    )?;
    ensure!(
        locator_validation.sha256 == store.locators.expected_sha256,
        "locator index digest differs during full verification"
    );
    let first_signature_ordinals = locator_validation
        .first_signature_ordinals
        .as_deref()
        .context("full verification did not collect locator signature starts")?;
    let signature_sha256 = validate_signature_order(
        &store.signatures,
        &store.manifest,
        &locator_validation.signature_counts,
        Some(SignatureSourceBinding {
            bytes: &source_signatures,
            first_ordinals: first_signature_ordinals,
        }),
        &mut scan_scratch,
    )?;
    ensure!(
        signature_sha256 == store.signatures.expected_sha256,
        "signature index digest differs during full verification"
    );
    store.index_manifest.verify_identity("index manifest")?;
    store.source.verify_file_identities()?;
    Ok(VerifySummary {
        complete: store.complete(),
        transactions: store.transaction_count(),
        signature_occurrences: store.signature_occurrence_count(),
        source_transaction_sha256: store.source_transaction_sha256().to_owned(),
    })
}

struct PinnedIndexFile {
    file: PinnedSourceFile,
    record_bytes: usize,
    record_count: u64,
    expected_sha256: [u8; 32],
}

impl PinnedIndexFile {
    #[allow(clippy::too_many_arguments)]
    fn open(
        path: &Path,
        binding: &IndexFileBinding,
        magic: [u8; 8],
        record_bytes: u16,
        source_manifest_sha256: [u8; 32],
        source_transaction_sha256: [u8; 32],
        complete: bool,
    ) -> Result<Self> {
        ensure!(
            binding.file
                == path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                && binding.record_bytes == record_bytes,
            "index manifest file binding differs"
        );
        let expected_sha256 = parse_hex_digest(&binding.sha256, "index file digest")?;
        let file = PinnedSourceFile::open(path, "index file")?;
        let bytes = file.len();
        ensure!(
            bytes == binding.bytes
                && bytes == encoded_file_bytes(binding.records, usize::from(record_bytes))?,
            "index file size differs from its manifest"
        );
        let mut header_bytes = [0u8; INDEX_HEADER_BYTES];
        positioned_read_exact(file.file(), &mut header_bytes, 0)?;
        let header = IndexHeader::decode(&header_bytes, magic, record_bytes)?;
        ensure!(
            header.record_count == binding.records
                && header.source_manifest_sha256 == source_manifest_sha256
                && header.source_transaction_sha256 == source_transaction_sha256
                && header.complete() == complete,
            "index header differs from its manifest or source binding"
        );
        Ok(Self {
            file,
            record_bytes: usize::from(record_bytes),
            record_count: binding.records,
            expected_sha256,
        })
    }

    fn read_row(&self, ordinal: u64, bytes: &mut [u8]) -> Result<()> {
        ensure!(ordinal < self.record_count, "index row is outside the file");
        ensure!(
            bytes.len() == self.record_bytes,
            "index row destination size differs"
        );
        positioned_read_exact(self.file.file(), bytes, self.row_offset(ordinal)?)?;
        Ok(())
    }

    fn scan_rows(
        &self,
        scratch: &mut Vec<u8>,
        mut visit: impl FnMut(u64, &[u8]) -> Result<()>,
    ) -> Result<[u8; 32]> {
        let mut header = [0u8; INDEX_HEADER_BYTES];
        positioned_read_exact(self.file.file(), &mut header, 0)?;
        let mut hasher = Sha256::new();
        hasher.update(header);

        let rows_per_chunk = (INDEX_SCAN_BUFFER_BYTES / self.record_bytes).max(1);
        let chunk_capacity = rows_per_chunk
            .checked_mul(self.record_bytes)
            .context("index scan buffer size overflow")?;
        scratch.resize(chunk_capacity, 0);
        let mut ordinal = 0u64;
        while ordinal < self.record_count {
            let remaining = self.record_count - ordinal;
            let rows = usize::try_from(remaining.min(rows_per_chunk as u64))
                .context("index scan row count exceeds usize")?;
            let bytes_len = rows
                .checked_mul(self.record_bytes)
                .context("index scan byte count overflow")?;
            positioned_read_exact(
                self.file.file(),
                &mut scratch[..bytes_len],
                self.row_offset(ordinal)?,
            )?;
            hasher.update(&scratch[..bytes_len]);
            for row in scratch[..bytes_len].chunks_exact(self.record_bytes) {
                visit(ordinal, row)?;
                ordinal = ordinal
                    .checked_add(1)
                    .context("index scan ordinal overflow")?;
            }
        }
        self.file.verify_identity("index file")?;
        Ok(hasher.finalize().into())
    }

    fn row_offset(&self, ordinal: u64) -> Result<u64> {
        u64::try_from(INDEX_HEADER_BYTES)
            .expect("index header size fits u64")
            .checked_add(
                ordinal
                    .checked_mul(
                        u64::try_from(self.record_bytes).expect("index record size fits u64"),
                    )
                    .context("index row offset overflow")?,
            )
            .context("index row start overflow")
    }
}

fn validate_manifest(
    manifest: &IndexManifest,
    source: &crate::source::SourceDump,
    options: QueryOpenOptions,
) -> Result<()> {
    ensure!(
        manifest.schema_version == INDEX_SCHEMA_VERSION
            && manifest.artifact_kind == IndexManifest::ARTIFACT_KIND
            && manifest.transactions != 0
            && manifest.signature_occurrences != 0,
        "invalid query index manifest header"
    );
    ensure!(
        manifest.complete || options.allow_incomplete,
        "query index is an incomplete canary; pass --allow-incomplete explicitly"
    );
    ensure!(
        manifest.complete == manifest.canary_max_transactions.is_none()
            && manifest.complete == manifest.source.transaction_hash_verified_during_build
            && manifest.complete == manifest.source.signature_hash_verified_during_build,
        "query index completion markers are inconsistent"
    );
    ensure!(
        manifest.locators.file == LOCATORS_FILE
            && manifest.locators.records == manifest.transactions
            && manifest.locators.record_bytes == LOCATOR_RECORD_BYTES as u16
            && manifest.signature_lookup.file == SIGNATURE_LOOKUP_FILE
            && manifest.signature_lookup.records == manifest.signature_occurrences
            && manifest.signature_lookup.record_bytes == SIGNATURE_RECORD_BYTES as u16,
        "query index artifact bindings differ"
    );
    ensure!(
        manifest.source.manifest_sha256 == hex_digest(source.manifest_sha256)
            && manifest.source.transaction_file == source.manifest.transaction_stream
            && manifest.source.transaction_bytes == source.transaction_bytes
            && manifest.source.transaction_sha256 == hex_digest(source.transaction_sha256)
            && manifest.source.signature_file
                == source
                    .manifest
                    .signature_stream
                    .as_deref()
                    .expect("validated source signature binding")
            && manifest.source.signature_bytes == source.signature_bytes
            && manifest.source.signature_sha256 == hex_digest(source.signature_sha256)
            && manifest.source.registry_file
                == source
                    .manifest
                    .pubkey_registry
                    .as_deref()
                    .expect("validated source registry binding")
            && manifest.source.registry_bytes == source.registry_bytes
            && manifest.source.registry_sha256 == hex_digest(source.registry_sha256)
            && manifest.source.accounts_file
                == source
                    .manifest
                    .discovered_accounts
                    .as_deref()
                    .expect("validated source accounts binding")
            && manifest.source.accounts_bytes == source.accounts_bytes
            && manifest.source.accounts_sha256 == hex_digest(source.accounts_sha256)
            && manifest.source.manifest_transactions == source.manifest.transactions
            && manifest.source.manifest_signatures == source.signatures
            && manifest.source.manifest_pubkeys == source.pubkeys,
        "query index source binding differs from the consolidated dump"
    );
    if manifest.complete {
        ensure!(
            manifest.transactions == source.manifest.transactions
                && manifest.signature_occurrences == source.signatures,
            "complete query index counts differ from the source manifest"
        );
    } else {
        let maximum = manifest
            .canary_max_transactions
            .context("incomplete query index has no canary transaction limit")?;
        ensure!(
            maximum != 0
                && manifest.transactions == maximum.min(source.manifest.transactions)
                && manifest.signature_occurrences <= source.signatures,
            "canary query index counts differ from its declared prefix"
        );
        if manifest.transactions == source.manifest.transactions {
            ensure!(
                manifest.signature_occurrences == source.signatures,
                "full-length canary signature count differs from the source manifest"
            );
        }
    }
    Ok(())
}

struct LocatorValidation {
    sha256: [u8; 32],
    signature_counts: Vec<u8>,
    first_signature_ordinals: Option<Vec<u64>>,
}

fn validate_locator_order(
    locators: &PinnedIndexFile,
    manifest: &IndexManifest,
    source: &crate::source::SourceDump,
    collect_signature_starts: bool,
    scan_scratch: &mut Vec<u8>,
) -> Result<LocatorValidation> {
    ensure!(
        manifest.transactions <= MAX_SIGNATURE_COUNT_TABLE_BYTES,
        "query index has too many transactions for its bounded validation table"
    );
    let transaction_count =
        usize::try_from(manifest.transactions).context("transaction count exceeds usize")?;
    let mut signature_counts = Vec::new();
    signature_counts
        .try_reserve_exact(transaction_count)
        .context("reserve locator signature-count table")?;
    let mut first_signature_ordinals = if collect_signature_starts {
        let capacity = transaction_count
            .checked_add(1)
            .context("locator signature-start table length overflow")?;
        let mut starts = Vec::new();
        starts
            .try_reserve_exact(capacity)
            .context("reserve locator signature-start table")?;
        Some(starts)
    } else {
        None
    };
    let mut previous_coordinate = None;
    let mut previous_payload_end = 0u64;
    let mut expected_signature_ordinal = 0u64;
    let sha256 = locators.scan_rows(scan_scratch, |_ordinal, row| {
        let record = LocatorRecord::decode(row)?;
        ensure!(
            previous_coordinate.is_none_or(|previous| previous < record.coordinate),
            "locator coordinates are not strictly canonical"
        );
        source.validate_record_binding(
            record.coordinate.epoch,
            record.coordinate.slot,
            record.coordinate.source_block_id,
            record.source_wire_profile,
        )?;
        ensure!(
            record.parent_slot < record.coordinate.slot
                && record.transaction_count != 0
                && record.coordinate.tx_index < record.transaction_count
                && record.signature_count != 0
                && record.flags & !KNOWN_TRANSACTION_FLAGS == 0
                && record.frame.payload_len != 0
                && usize::try_from(record.frame.payload_len)? <= WINCODE_LEB128_MAX_FRAME_BYTES
                && record.frame.payload_offset > previous_payload_end,
            "locator row has invalid source fields"
        );
        let payload_end = record
            .frame
            .payload_offset
            .checked_add(u64::from(record.frame.payload_len))
            .context("locator payload end overflow")?;
        ensure!(
            payload_end <= source.transaction_bytes,
            "locator payload exceeds transactions.wincode"
        );
        ensure!(
            record.first_signature_ordinal == expected_signature_ordinal,
            "locator signature ordinals are not contiguous"
        );
        expected_signature_ordinal = expected_signature_ordinal
            .checked_add(u64::from(record.signature_count))
            .context("locator signature count overflow")?;
        ensure!(
            expected_signature_ordinal <= source.signatures,
            "locator signature range exceeds signatures.bin"
        );
        previous_coordinate = Some(record.coordinate);
        previous_payload_end = payload_end;
        signature_counts.push(record.signature_count);
        if let Some(starts) = &mut first_signature_ordinals {
            starts.push(record.first_signature_ordinal);
        }
        Ok(())
    })?;
    ensure!(
        expected_signature_ordinal == manifest.signature_occurrences,
        "locator and signature index counts differ"
    );
    ensure!(
        signature_counts.len() == transaction_count,
        "locator validation table count differs"
    );
    if let Some(starts) = &mut first_signature_ordinals {
        starts.push(expected_signature_ordinal);
    }
    Ok(LocatorValidation {
        sha256,
        signature_counts,
        first_signature_ordinals,
    })
}

#[derive(Clone, Copy)]
struct SignatureSourceBinding<'a> {
    bytes: &'a [u8],
    first_ordinals: &'a [u64],
}

fn validate_signature_order(
    signatures: &PinnedIndexFile,
    manifest: &IndexManifest,
    signature_counts: &[u8],
    source: Option<SignatureSourceBinding<'_>>,
    scan_scratch: &mut Vec<u8>,
) -> Result<[u8; 32]> {
    ensure!(
        signature_counts.len() == usize::try_from(manifest.transactions)?,
        "signature-count validation table differs from the manifest"
    );
    if let Some(source) = source {
        ensure!(
            source.first_ordinals.len()
                == signature_counts
                    .len()
                    .checked_add(1)
                    .context("signature-start table length overflow")?,
            "signature-start validation table differs from the locator index"
        );
    }
    let mut previous = None;
    signatures.scan_rows(scan_scratch, |_ordinal, row| {
        let record = SignatureIndexRecord::decode(row)?;
        ensure!(
            previous.is_none_or(|value| value < record),
            "signature index is not strictly sorted"
        );
        ensure!(
            record.transaction_id < manifest.transactions,
            "signature index transaction ID is outside the locator index"
        );
        let transaction_index = usize::try_from(record.transaction_id)
            .context("signature transaction ID exceeds usize")?;
        ensure!(
            record.signature_position < signature_counts[transaction_index],
            "signature position exceeds its transaction range"
        );
        if let Some(source) = source {
            let source_ordinal = source.first_ordinals[transaction_index]
                .checked_add(u64::from(record.signature_position))
                .context("source signature ordinal overflow")?;
            let source_start = usize::try_from(
                source_ordinal
                    .checked_mul(SIGNATURE_BYTES)
                    .context("source signature byte offset overflow")?,
            )
            .context("source signature byte offset exceeds usize")?;
            let source_end = source_start
                .checked_add(SIGNATURE_BYTES as usize)
                .context("source signature byte end overflow")?;
            let source_signature = source
                .bytes
                .get(source_start..source_end)
                .context("indexed signature maps outside signatures.bin")?;
            ensure!(
                source_signature == record.signature,
                "signature index occurrence differs from signatures.bin"
            );
        }
        previous = Some(record);
        Ok(())
    })
}

#[cfg(unix)]
fn positioned_read_exact(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(bytes, offset)
}

#[cfg(windows)]
fn positioned_read_exact(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;

    let mut read = 0usize;
    while read < bytes.len() {
        let read_offset = offset
            .checked_add(u64::try_from(read).map_err(Error::other)?)
            .ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "positioned read offset overflow")
            })?;
        let count = file.seek_read(&mut bytes[read..], read_offset)?;
        if count == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "positioned read reached end of file",
            ));
        }
        read += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn positioned_read_exact(_file: &File, _bytes: &mut [u8], _offset: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positioned file reads are not supported on this platform",
    ))
}

const _: () = assert!(INDEX_FLAG_COMPLETE == 1);
