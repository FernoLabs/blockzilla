use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

pub const ARCHIVE_V2_BLOCKS_FILE: &str = "archive-v2-blocks.zstd";
pub const ARCHIVE_V2_RAW_BLOCKS_FILE: &str = "archive-v2-blocks.wincode";
pub const ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE: &str = "archive-v2-blocks.wincode.zst";
pub const ARCHIVE_V2_BLOCK_INDEX_FILE: &str = "archive-v2-blocks.index";
pub const ARCHIVE_V2_META_FILE: &str = "archive-v2-meta.wincode";
pub const ARCHIVE_V2_SIGNATURES_FILE: &str = "signatures.bin";
pub const ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE: &str = "vote_hash_registry.bin";
pub const ARCHIVE_V2_POH_FILE: &str = "poh.wincode";
pub const ARCHIVE_V2_SHREDDING_FILE: &str = "shredding.wincode";
pub const ARCHIVE_V2_PUBKEY_REGISTRY_FILE: &str = "registry.bin";
pub const ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE: &str = "registry_counts.bin";
pub const ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
pub const ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE: &str = "prev_blockhash_tail.bin";

pub const ARCHIVE_V2_HOT_INDEX_MAGIC: &[u8; 8] = b"BZV2HIX1";
pub const ARCHIVE_V2_HOT_INDEX_VERSION: u16 = 1;
pub const ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY: u32 = 1 << 0;
/// Index rows point at raw serialized block payloads, not independent zstd frames.
///
/// The same index is valid for `archive-v2-blocks.wincode` and for the
/// whole-file-zstd companion `archive-v2-blocks.wincode.zst`; in the latter
/// case offsets are in the decompressed stream.
pub const ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS: u32 = 1 << 1;
pub const ARCHIVE_V2_HOT_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8 + 4 + 4;
pub const ARCHIVE_V2_HOT_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4;

#[derive(Debug)]
pub struct ArchiveV2HotBlockIndex {
    pub blob_file_bytes: u64,
    pub level: i32,
    pub flags: u32,
    pub rows: Vec<ArchiveV2HotBlockIndexRow>,
}

#[derive(Debug, Clone, Copy)]
pub struct ArchiveV2HotBlockIndexRow {
    pub block_id: u32,
    pub slot: u64,
    pub compressed_offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub tx_count: u32,
    pub first_tx_ordinal: u64,
    pub first_signature_ordinal: u64,
    pub signature_count: u32,
}

pub fn archive_v2_hot_index_path(input: &Path) -> PathBuf {
    input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(ARCHIVE_V2_BLOCK_INDEX_FILE)
}

pub fn write_archive_v2_hot_block_index(
    path: &Path,
    blob_file_bytes: u64,
    level: i32,
    flags: u32,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(ARCHIVE_V2_BLOCK_INDEX_FILE)
    ));
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(8 << 20, file);
    writer.write_all(ARCHIVE_V2_HOT_INDEX_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_HOT_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    writer.write_all(&blob_file_bytes.to_le_bytes())?;
    writer.write_all(&level.to_le_bytes())?;
    writer.write_all(&flags.to_le_bytes())?;
    for row in rows {
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.compressed_offset.to_le_bytes())?;
        writer.write_all(&row.compressed_len.to_le_bytes())?;
        writer.write_all(&row.uncompressed_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
        writer.write_all(&row.first_tx_ordinal.to_le_bytes())?;
        writer.write_all(&row.first_signature_ordinal.to_le_bytes())?;
        writer.write_all(&row.signature_count.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

pub fn read_archive_v2_hot_block_index(path: &Path) -> Result<ArchiveV2HotBlockIndex> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(8 << 20, file);
    let mut header = [0u8; ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        &header[..8] == ARCHIVE_V2_HOT_INDEX_MAGIC,
        "{} is not an Archive V2 hot-block index",
        path.display()
    );
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    anyhow::ensure!(
        version == ARCHIVE_V2_HOT_INDEX_VERSION,
        "{} has unsupported Archive V2 hot-block index version {version}",
        path.display()
    );
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let blob_file_bytes = u64::from_le_bytes(header[20..28].try_into().unwrap());
    let level = i32::from_le_bytes(header[28..32].try_into().unwrap());
    let flags = u32::from_le_bytes(header[32..36].try_into().unwrap());
    let row_count_usize = usize::try_from(row_count).context("index row count exceeds usize")?;
    let expected_len =
        ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64 + row_count * ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64;
    let actual_len = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    anyhow::ensure!(
        actual_len == expected_len,
        "{} has size {}, expected {} for {} rows",
        path.display(),
        actual_len,
        expected_len,
        row_count
    );

    let mut rows = Vec::with_capacity(row_count_usize);
    let mut row_buf = [0u8; ARCHIVE_V2_HOT_INDEX_ROW_LEN];
    for _ in 0..row_count_usize {
        reader
            .read_exact(&mut row_buf)
            .with_context(|| format!("read row from {}", path.display()))?;
        rows.push(ArchiveV2HotBlockIndexRow {
            block_id: u32::from_le_bytes(row_buf[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row_buf[4..12].try_into().unwrap()),
            compressed_offset: u64::from_le_bytes(row_buf[12..20].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(row_buf[20..24].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(row_buf[24..28].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row_buf[28..32].try_into().unwrap()),
            first_tx_ordinal: u64::from_le_bytes(row_buf[32..40].try_into().unwrap()),
            first_signature_ordinal: u64::from_le_bytes(row_buf[40..48].try_into().unwrap()),
            signature_count: u32::from_le_bytes(row_buf[48..52].try_into().unwrap()),
        });
    }
    Ok(ArchiveV2HotBlockIndex {
        blob_file_bytes,
        level,
        flags,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_v2_hot_block_index_roundtrips() {
        let path = std::env::temp_dir().join(format!(
            "blockzilla-hot-index-roundtrip-{}-{}.index",
            std::process::id(),
            unique_suffix()
        ));
        let rows = [
            ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 42,
                compressed_offset: 0,
                compressed_len: 123,
                uncompressed_len: 456,
                tx_count: 7,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 9,
            },
            ArchiveV2HotBlockIndexRow {
                block_id: 1,
                slot: 43,
                compressed_offset: 123,
                compressed_len: 321,
                uncompressed_len: 654,
                tx_count: 8,
                first_tx_ordinal: 7,
                first_signature_ordinal: 9,
                signature_count: 10,
            },
        ];

        write_archive_v2_hot_block_index(
            &path,
            444,
            1,
            ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY,
            &rows,
        )
        .unwrap();
        let index = read_archive_v2_hot_block_index(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(index.blob_file_bytes, 444);
        assert_eq!(index.level, 1);
        assert_eq!(index.flags, ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY);
        assert_eq!(index.rows.len(), rows.len());
        for (actual, expected) in index.rows.iter().zip(rows) {
            assert_eq!(actual.block_id, expected.block_id);
            assert_eq!(actual.slot, expected.slot);
            assert_eq!(actual.compressed_offset, expected.compressed_offset);
            assert_eq!(actual.compressed_len, expected.compressed_len);
            assert_eq!(actual.uncompressed_len, expected.uncompressed_len);
            assert_eq!(actual.tx_count, expected.tx_count);
            assert_eq!(actual.first_tx_ordinal, expected.first_tx_ordinal);
            assert_eq!(
                actual.first_signature_ordinal,
                expected.first_signature_ordinal
            );
            assert_eq!(actual.signature_count, expected.signature_count);
        }
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
