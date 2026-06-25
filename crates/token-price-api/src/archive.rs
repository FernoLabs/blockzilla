use anyhow::{Context, Result};
use blockzilla_format::{
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ArchiveV2HotBlockBlob, ArchiveV2HotBlockIndex,
    ArchiveV2HotBlockIndexRow, archive_v2_hot_index_path, deserialize_archive_v2_hot_block_blob,
    read_archive_v2_hot_block_index,
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

pub struct ArchiveV2Reader {
    input: PathBuf,
    file: File,
    index: ArchiveV2HotBlockIndex,
    raw_blocks: bool,
    compressed: Vec<u8>,
    block_bytes: Vec<u8>,
    decompressor: zstd::bulk::Decompressor<'static>,
}

impl ArchiveV2Reader {
    pub fn open(input: &Path, index_path: Option<&Path>) -> Result<Self> {
        let index_path = index_path
            .map(Path::to_path_buf)
            .unwrap_or_else(|| archive_v2_hot_index_path(input));
        let index = read_archive_v2_hot_block_index(&index_path)
            .with_context(|| format!("read {}", index_path.display()))?;
        let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
        let raw_blocks = index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0
            || input
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".wincode"));

        Ok(Self {
            input: input.to_path_buf(),
            file,
            index,
            raw_blocks,
            compressed: Vec::with_capacity(2 << 20),
            block_bytes: Vec::with_capacity(2 << 20),
            decompressor: zstd::bulk::Decompressor::new()?,
        })
    }

    #[inline]
    pub fn rows(&self) -> &[ArchiveV2HotBlockIndexRow] {
        &self.index.rows
    }

    #[inline]
    pub fn raw_blocks(&self) -> bool {
        self.raw_blocks
    }

    #[inline]
    pub fn compressed_input(&self) -> bool {
        !self.raw_blocks
    }

    #[inline]
    pub fn input(&self) -> &Path {
        &self.input
    }

    pub fn read_block(&mut self, row: &ArchiveV2HotBlockIndexRow) -> Result<ArchiveV2HotBlockBlob> {
        self.read_block_bytes(row)?;
        let block = deserialize_archive_v2_hot_block_blob(&self.block_bytes)
            .with_context(|| format!("decode hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot,
            "hot block_id {} slot mismatch: decoded={}, index={}",
            row.block_id,
            block.header.slot,
            row.slot
        );
        anyhow::ensure!(
            block.tx_rows.len() == row.tx_count as usize,
            "hot block_id {} tx count mismatch: decoded={}, index={}",
            row.block_id,
            block.tx_rows.len(),
            row.tx_count
        );
        Ok(block)
    }

    fn read_block_bytes(&mut self, row: &ArchiveV2HotBlockIndexRow) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(row.compressed_offset))
            .with_context(|| {
                format!(
                    "seek {} to block_id {} offset {}",
                    self.input.display(),
                    row.block_id,
                    row.compressed_offset
                )
            })?;

        if self.raw_blocks {
            self.block_bytes.resize(row.uncompressed_len as usize, 0);
            self.file.read_exact(&mut self.block_bytes)?;
            return Ok(());
        }

        self.compressed.resize(row.compressed_len as usize, 0);
        self.file.read_exact(&mut self.compressed)?;
        let decoded = self
            .decompressor
            .decompress(&self.compressed, row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            decoded.len() == row.uncompressed_len as usize,
            "hot block_id {} decompressed length mismatch: decoded={}, index={}",
            row.block_id,
            decoded.len(),
            row.uncompressed_len
        );
        self.block_bytes.clear();
        self.block_bytes.extend_from_slice(&decoded);
        Ok(())
    }
}

pub fn default_registry_path(input: &Path) -> PathBuf {
    input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
}
