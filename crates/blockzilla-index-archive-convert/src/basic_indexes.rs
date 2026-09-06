//! Build the fixed-width slot index from the canonical block catalog.
//!
//! Signature lookup is deliberately not generation-local. A future global
//! signature index can span every epoch without repeating one large derived
//! object in each generation.

use std::{
    fs::{self, File},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use blockzilla_index_archive_format::{
    ArchiveId, FILE_HEADER_LEN, FileHeader, catalog::blocks as catalog_blocks,
    indexes::slots as slot_index,
};

use crate::container::{HeaderedWriter, validate_open_file};

const WRITE_BUFFER_BYTES: usize = 1 << 20;

/// Reserved for source compatibility with callers that configure every index
/// builder through one options value. The slot builder has no sort buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BasicIndexBuildOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasicIndexBuildReport {
    pub archive_id: ArchiveId,
    pub blocks: u64,
    pub transactions: u64,
    pub slots_object_bytes: u64,
}

struct StagingDirectory {
    path: PathBuf,
}

impl StagingDirectory {
    fn create(root: &Path) -> Result<Self> {
        let path = root.join(format!(".basic-indexes.building-{}", std::process::id()));
        fs::create_dir(&path).with_context(|| {
            format!(
                "create slot-index staging directory {}; remove an abandoned directory first if needed",
                path.display()
            )
        })?;
        Ok(Self { path })
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn archive_id_from_catalog(file: &File) -> Result<ArchiveId> {
    let mut bytes = [0_u8; FILE_HEADER_LEN];
    file.read_exact_at(&mut bytes, 0)
        .context("read catalog common header")?;
    Ok(FileHeader::decode(&bytes)
        .context("decode catalog common header")?
        .archive_id)
}

fn read_catalog_row(file: &File, block_ordinal: u64) -> Result<catalog_blocks::BlockRow> {
    let offset = block_ordinal
        .checked_mul(catalog_blocks::ROW_LEN as u64)
        .and_then(|offset| offset.checked_add(FILE_HEADER_LEN as u64))
        .context("catalog row offset overflow")?;
    let mut bytes = [0_u8; catalog_blocks::ROW_LEN];
    file.read_exact_at(&mut bytes, offset)
        .with_context(|| format!("read catalog block ordinal {block_ordinal}"))?;
    catalog_blocks::BlockRow::decode(&bytes)
        .with_context(|| format!("decode catalog block ordinal {block_ordinal}"))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

/// Rebuild `indexes/slots.idx` from the canonical block catalog.
pub fn build_basic_indexes(
    root: &Path,
    _options: BasicIndexBuildOptions,
) -> Result<BasicIndexBuildReport> {
    ensure!(
        root.is_dir(),
        "{} is not an archive directory",
        root.display()
    );

    let catalog_path = root.join(catalog_blocks::PATH);
    let catalog_file =
        File::open(&catalog_path).with_context(|| format!("open {}", catalog_path.display()))?;
    let archive_id = archive_id_from_catalog(&catalog_file)?;
    let catalog_header = validate_open_file(&catalog_file, catalog_blocks::PATH, archive_id)?;
    ensure!(
        catalog_header.decoded_bytes == catalog_header.payload_bytes,
        "catalog must keep its fixed-width payload raw"
    );
    let expected_catalog_bytes = catalog_header
        .record_count
        .checked_mul(catalog_blocks::ROW_LEN as u64)
        .context("catalog payload length overflow")?;
    ensure!(
        catalog_header.payload_bytes == expected_catalog_bytes,
        "catalog has {} payload bytes, expected {expected_catalog_bytes} for {} rows",
        catalog_header.payload_bytes,
        catalog_header.record_count
    );

    let staging = StagingDirectory::create(root)?;
    let mut slot_writer =
        HeaderedWriter::create(&staging.path, slot_index::PATH, WRITE_BUFFER_BYTES)?;
    let mut expected_transaction = 0_u64;
    let mut previous_slot = None;
    for block_ordinal in 0..catalog_header.record_count {
        let block = read_catalog_row(&catalog_file, block_ordinal)?;
        if let Some(previous) = previous_slot {
            ensure!(
                block.slot > previous,
                "catalog slots do not strictly ascend at block ordinal {block_ordinal}"
            );
        }
        previous_slot = Some(block.slot);
        ensure!(
            block.first_transaction == expected_transaction,
            "block ordinal {block_ordinal} starts at transaction {}, expected {expected_transaction}",
            block.first_transaction
        );
        slot_writer.append(&block.slot.to_le_bytes(), slot_index::RECORD_LEN as u64)?;
        expected_transaction = block.transactions_end()?;
    }

    let slots_finished = slot_writer.finish(archive_id, catalog_header.record_count)?;
    let final_index_directory = root.join("indexes");
    fs::create_dir_all(&final_index_directory)
        .with_context(|| format!("create {}", final_index_directory.display()))?;
    fs::rename(
        staging.path.join(slot_index::PATH),
        root.join(slot_index::PATH),
    )
    .context("publish slot index")?;
    sync_directory(&final_index_directory)?;

    Ok(BasicIndexBuildReport {
        archive_id,
        blocks: catalog_header.record_count,
        transactions: expected_transaction,
        slots_object_bytes: slots_finished.file_bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use blockzilla_index_archive_format::{ObjectRole, catalog::blocks::BlockRow};
    use tempfile::tempdir;

    use crate::container::{payload_from_bytes, write_payload};

    use super::*;

    fn write_catalog(root: &Path) -> ArchiveId {
        let archive_id = ArchiveId::new([8; 16]);
        let transactions = catalog_blocks::PageSpan {
            offset: FILE_HEADER_LEN as u64,
            stored_len: 1,
            decoded_len: 1,
        };
        let catalog = catalog_blocks::encode_table(&[
            BlockRow {
                slot: 100,
                parent_slot: 99,
                first_transaction: 0,
                transaction_count: 2,
                transactions,
                ..BlockRow::default()
            },
            BlockRow {
                slot: 103,
                parent_slot: 100,
                first_transaction: 2,
                transaction_count: 1,
                transactions,
                ..BlockRow::default()
            },
        ])
        .unwrap();
        write_payload(root, catalog_blocks::PATH, archive_id, 2, &catalog).unwrap();
        archive_id
    }

    #[test]
    fn tiny_candidate_builds_only_the_slot_index() {
        let root = tempdir().unwrap();
        let archive_id = write_catalog(root.path());
        let report =
            build_basic_indexes(root.path(), BasicIndexBuildOptions).expect("build slot index");
        assert_eq!(report.blocks, 2);
        assert_eq!(report.transactions, 3);

        let slot_bytes = fs::read(root.path().join(slot_index::PATH)).unwrap();
        let (slot_header, slot_payload) =
            payload_from_bytes(&slot_bytes, slot_index::PATH, archive_id).unwrap();
        assert_eq!(slot_header.role, ObjectRole::IndexSlots);
        assert_eq!(slot_header.schema, slot_index::SCHEMA);
        let slots = slot_index::SlotIndex::parse(slot_payload, slot_header.record_count).unwrap();
        assert_eq!(slots.find(100), Some(0));
        assert_eq!(slots.find(103), Some(1));
        assert_eq!(slots.find(101), None);
        assert!(!root.path().join("indexes/signatures.idx").exists());
    }

    #[test]
    fn non_contiguous_transaction_ranges_are_rejected_before_publication() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let transactions = catalog_blocks::PageSpan {
            offset: FILE_HEADER_LEN as u64,
            stored_len: 1,
            decoded_len: 1,
        };
        let catalog = [
            BlockRow {
                slot: 100,
                parent_slot: 99,
                first_transaction: 0,
                transaction_count: 2,
                transactions,
                ..BlockRow::default()
            }
            .encode()
            .unwrap()
            .to_vec(),
            BlockRow {
                slot: 103,
                parent_slot: 100,
                first_transaction: 3,
                transaction_count: 1,
                transactions,
                ..BlockRow::default()
            }
            .encode()
            .unwrap()
            .to_vec(),
        ]
        .concat();
        write_payload(root.path(), catalog_blocks::PATH, archive_id, 2, &catalog).unwrap();

        let error = build_basic_indexes(root.path(), BasicIndexBuildOptions)
            .unwrap_err()
            .to_string();
        assert!(error.contains("starts at transaction 3, expected 2"));
        assert!(!root.path().join(slot_index::PATH).exists());
    }
}
