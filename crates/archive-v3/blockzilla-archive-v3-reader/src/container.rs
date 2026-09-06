//! Header validation and bounded decompression for canonical Archive V3 objects.

use std::{fs::File, io::Read, os::unix::fs::FileExt};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3::{ArchiveId, FILE_HEADER_LEN, FileHeader, object_by_path};

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
