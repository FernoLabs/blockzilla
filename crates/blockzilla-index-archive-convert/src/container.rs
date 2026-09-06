//! Common-header I/O for Index Archive binary objects.
//!
//! Page extents are absolute file offsets. Therefore, payload byte zero starts
//! after the common 64-byte header and the first extent starts at byte 64.

use std::{
    fs::{self, File},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use blockzilla_index_archive_format::{
    ArchiveId, FILE_HEADER_LEN, FileHeader, ObjectSpec, object_by_path,
};

/// Physical and logical sizes written for one headered object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedObject {
    pub file_bytes: u64,
    pub payload_bytes: u64,
    pub decoded_bytes: u64,
    pub record_count: u64,
}

/// A sequential writer that reserves and finishes the common file header.
pub struct HeaderedWriter {
    path: PathBuf,
    spec: &'static ObjectSpec,
    writer: BufWriter<File>,
    file_bytes: u64,
    decoded_bytes: u64,
}

impl HeaderedWriter {
    pub fn create(root: &Path, relative_path: &'static str, capacity: usize) -> Result<Self> {
        let spec = object_by_path(relative_path)
            .with_context(|| format!("{relative_path} is not in the Index Archive layout"))?;
        let path = root.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(capacity.max(FILE_HEADER_LEN), file);
        writer
            .write_all(&[0; FILE_HEADER_LEN])
            .with_context(|| format!("reserve header for {}", path.display()))?;
        Ok(Self {
            path,
            spec,
            writer,
            file_bytes: FILE_HEADER_LEN as u64,
            decoded_bytes: 0,
        })
    }

    /// Append stored bytes and return their absolute file offset.
    pub fn append(&mut self, stored: &[u8], decoded_len: u64) -> Result<u64> {
        let offset = self.file_bytes;
        self.writer
            .write_all(stored)
            .with_context(|| format!("write {}", self.path.display()))?;
        self.file_bytes = self
            .file_bytes
            .checked_add(u64::try_from(stored.len()).context("stored length exceeds u64")?)
            .context("headered object size overflow")?;
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(decoded_len)
            .context("decoded object size overflow")?;
        Ok(offset)
    }

    pub fn finish(mut self, archive_id: ArchiveId, record_count: u64) -> Result<FinishedObject> {
        self.writer
            .flush()
            .with_context(|| format!("flush {}", self.path.display()))?;
        let payload_bytes = self
            .file_bytes
            .checked_sub(FILE_HEADER_LEN as u64)
            .expect("writer starts after its header");
        let header = FileHeader::new(
            self.spec,
            archive_id,
            record_count,
            self.decoded_bytes,
            payload_bytes,
        )
        .with_context(|| format!("build header for {}", self.path.display()))?;
        self.writer
            .seek(SeekFrom::Start(0))
            .with_context(|| format!("seek header for {}", self.path.display()))?;
        self.writer
            .write_all(&header.encode())
            .with_context(|| format!("write header for {}", self.path.display()))?;
        self.writer
            .flush()
            .with_context(|| format!("finish {}", self.path.display()))?;
        self.writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("sync {}", self.path.display()))?;
        ensure!(
            self.writer.get_ref().metadata()?.len() == self.file_bytes,
            "{} length changed while it was written",
            self.path.display()
        );
        Ok(FinishedObject {
            file_bytes: self.file_bytes,
            payload_bytes,
            decoded_bytes: self.decoded_bytes,
            record_count,
        })
    }
}

/// Write one raw payload behind the common file header.
pub fn write_payload(
    root: &Path,
    relative_path: &'static str,
    archive_id: ArchiveId,
    record_count: u64,
    payload: &[u8],
) -> Result<FinishedObject> {
    let mut writer = HeaderedWriter::create(root, relative_path, 1 << 20)?;
    writer.append(
        payload,
        u64::try_from(payload.len()).context("payload exceeds u64")?,
    )?;
    writer.finish(archive_id, record_count)
}

/// Copy a large raw source object behind a target common header without
/// loading the source object into memory.
pub fn copy_payload(
    source: &Path,
    root: &Path,
    relative_path: &'static str,
    archive_id: ArchiveId,
    record_count: u64,
) -> Result<FinishedObject> {
    let source_file = File::open(source).with_context(|| format!("open {}", source.display()))?;
    copy_file_payload(
        source_file,
        source,
        root,
        relative_path,
        archive_id,
        record_count,
    )
}

/// Copy one already-pinned source descriptor behind a target common header.
pub fn copy_file_payload(
    source_file: File,
    source_label: &Path,
    root: &Path,
    relative_path: &'static str,
    archive_id: ArchiveId,
    record_count: u64,
) -> Result<FinishedObject> {
    copy_file_payload_with_suffix(
        source_file,
        source_label,
        root,
        relative_path,
        archive_id,
        record_count,
        &[],
    )
}

/// Copy one pinned source payload and append a small target-only suffix.
///
/// The Compact V2 pubkey registry uses this when inline source keys must be
/// interned after the source registry without loading that registry into RAM.
pub fn copy_file_payload_with_suffix(
    source_file: File,
    source_label: &Path,
    root: &Path,
    relative_path: &'static str,
    archive_id: ArchiveId,
    record_count: u64,
    suffix: &[u8],
) -> Result<FinishedObject> {
    let expected = source_file
        .metadata()
        .with_context(|| format!("stat {}", source_label.display()))?
        .len();
    let mut target = HeaderedWriter::create(root, relative_path, 8 << 20)?;
    let mut buffer = vec![0_u8; 8 << 20];
    let mut copied = 0_u64;
    while copied < expected {
        let remaining = usize::try_from((expected - copied).min(buffer.len() as u64))
            .expect("copy chunk is bounded by the buffer length");
        let read = source_file
            .read_at(&mut buffer[..remaining], copied)
            .with_context(|| format!("read {}", source_label.display()))?;
        ensure!(
            read != 0,
            "{} changed length while it was copied",
            source_label.display()
        );
        target.append(&buffer[..read], read as u64)?;
        copied = copied
            .checked_add(read as u64)
            .context("copied source length overflow")?;
    }
    ensure!(
        source_file
            .metadata()
            .with_context(|| format!("restat {}", source_label.display()))?
            .len()
            == expected,
        "{} changed length while it was copied",
        source_label.display()
    );
    target.append(
        suffix,
        u64::try_from(suffix.len()).context("suffix length exceeds u64")?,
    )?;
    target.finish(archive_id, record_count)
}

/// Decode and validate a complete in-memory headered object.
pub fn payload_from_bytes<'a>(
    bytes: &'a [u8],
    relative_path: &str,
    expected_archive_id: ArchiveId,
) -> Result<(FileHeader, &'a [u8])> {
    let spec = object_by_path(relative_path)
        .with_context(|| format!("{relative_path} is not in the Index Archive layout"))?;
    let header = FileHeader::decode(bytes)
        .with_context(|| format!("decode common header for {relative_path}"))?;
    header
        .validate_for(
            spec,
            expected_archive_id,
            u64::try_from(bytes.len()).context("file length exceeds u64")?,
        )
        .with_context(|| format!("validate common header for {relative_path}"))?;
    Ok((header, &bytes[FILE_HEADER_LEN..]))
}

/// Validate an already-open headered object without reading its payload.
pub fn validate_open_file(
    file: &File,
    relative_path: &str,
    expected_archive_id: ArchiveId,
) -> Result<FileHeader> {
    let spec = object_by_path(relative_path)
        .with_context(|| format!("{relative_path} is not in the Index Archive layout"))?;
    let mut bytes = [0_u8; FILE_HEADER_LEN];
    file.read_exact_at(&mut bytes, 0)
        .with_context(|| format!("read common header for {relative_path}"))?;
    let header = FileHeader::decode(&bytes)
        .with_context(|| format!("decode common header for {relative_path}"))?;
    header
        .validate_for(spec, expected_archive_id, file.metadata()?.len())
        .with_context(|| format!("validate common header for {relative_path}"))?;
    Ok(header)
}

/// Decode one zstd page to exactly the catalog-declared length.
///
/// This does not use `decode_all`: a corrupt small frame must not allocate or
/// expand beyond the page bound before the reader rejects it.
pub fn decode_zstd_exact(stored: &[u8], decoded_len: usize, label: &str) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(stored)
        .with_context(|| format!("open zstd page for {label}"))?;
    let mut decoded = vec![0_u8; decoded_len];
    decoder
        .read_exact(&mut decoded)
        .with_context(|| format!("zstd page for {label} ended before {decoded_len} bytes"))?;
    let mut trailing = [0_u8; 1];
    ensure!(
        decoder
            .read(&mut trailing)
            .with_context(|| format!("finish zstd page for {label}"))?
            == 0,
        "zstd page for {label} expands beyond {decoded_len} bytes"
    );
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use blockzilla_index_archive_format::{FileHeader, ledger::transactions};

    use super::*;

    #[test]
    fn writer_uses_absolute_payload_offsets_and_finishes_header() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let mut writer = HeaderedWriter::create(root.path(), transactions::PATH, 128).unwrap();
        assert_eq!(
            writer.append(&[1, 2, 3], 9).unwrap(),
            FILE_HEADER_LEN as u64
        );
        assert_eq!(
            writer.append(&[4, 5], 11).unwrap(),
            FILE_HEADER_LEN as u64 + 3
        );
        let finished = writer.finish(archive_id, 2).unwrap();
        assert_eq!(finished.file_bytes, FILE_HEADER_LEN as u64 + 5);
        assert_eq!(finished.payload_bytes, 5);
        assert_eq!(finished.decoded_bytes, 20);

        let bytes = fs::read(root.path().join(transactions::PATH)).unwrap();
        let header = FileHeader::decode(&bytes).unwrap();
        assert_eq!(header.archive_id, archive_id);
        assert_eq!(header.record_count, 2);
        assert_eq!(header.decoded_bytes, 20);
        assert_eq!(&bytes[FILE_HEADER_LEN..], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn payload_reader_rejects_another_archive() {
        let root = tempdir().unwrap();
        write_payload(
            root.path(),
            transactions::PATH,
            ArchiveId::new([1; 16]),
            1,
            &[3],
        )
        .unwrap();
        let bytes = fs::read(root.path().join(transactions::PATH)).unwrap();
        assert!(payload_from_bytes(&bytes, transactions::PATH, ArchiveId::new([2; 16])).is_err());
    }

    #[test]
    fn pinned_payload_copy_does_not_use_the_shared_file_cursor() {
        let root = tempdir().unwrap();
        let source_path = root.path().join("source.bin");
        fs::write(&source_path, [1, 2, 3, 4]).unwrap();
        let mut source = File::open(&source_path).unwrap();
        source.seek(SeekFrom::Start(3)).unwrap();
        let mut shared_cursor_observer = source.try_clone().unwrap();

        copy_file_payload_with_suffix(
            source,
            &source_path,
            root.path(),
            transactions::PATH,
            ArchiveId::new([3; 16]),
            1,
            &[5, 6],
        )
        .unwrap();

        assert_eq!(shared_cursor_observer.stream_position().unwrap(), 3);
        let bytes = fs::read(root.path().join(transactions::PATH)).unwrap();
        assert_eq!(&bytes[FILE_HEADER_LEN..], &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn exact_zstd_decode_rejects_short_and_overlong_output() {
        let stored = zstd::encode_all(&b"abcd"[..], 1).unwrap();
        assert_eq!(decode_zstd_exact(&stored, 4, "test").unwrap(), b"abcd");
        assert!(decode_zstd_exact(&stored, 3, "test").is_err());
        assert!(decode_zstd_exact(&stored, 5, "test").is_err());
    }
}
