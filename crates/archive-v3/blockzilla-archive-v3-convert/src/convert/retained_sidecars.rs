//! Bounded validation and exact retention of Compact V2 framed sidecars.

use std::{
    fs::File,
    io::{BufReader, Read},
    os::unix::fs::FileExt,
    path::Path,
};

use crate::{
    container::{FinishedObject, HeaderedWriter},
    source_v2_sidecars::{
        BlockSignatureCountCoverage, BlockhashRegistryLayout, PohBlockMapping, SourcePohSchema,
        detect_blockhash_registry_layout,
    },
};
use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::ArchiveV2HotBlockIndexRow;
use blockzilla_archive_v3::{
    ArchiveId, FILE_HEADER_LEN,
    catalog::blocks::PageSpan,
    sidecars::{poh, shredding},
};

const MAX_FRAME_BYTES: usize = 64 << 20;

#[derive(Debug)]
pub(crate) struct RetainedPoh {
    pub(crate) source_schema: SourcePohSchema,
    pub(crate) blockhash_registry_offset: u32,
    pub(crate) mappings: Vec<PohBlockMapping>,
    pub(crate) spans: Vec<PageSpan>,
    pub(crate) entry_count: u64,
    pub(crate) object: FinishedObject,
}

#[derive(Debug)]
pub(crate) struct RetainedShredding {
    pub(crate) spans: Vec<PageSpan>,
    pub(crate) boundary_count: u64,
    pub(crate) recorded_empty_blocks: u64,
    pub(crate) object: FinishedObject,
}

fn decode_poh_frame(
    exact_frame: &[u8],
    frame: usize,
    selected: Option<SourcePohSchema>,
) -> Result<(SourcePohSchema, poh::DecodedPohFrame)> {
    match selected {
        Some(SourcePohSchema::Current) => {
            let decoded =
                poh::decode_frame(poh::PohWireProfile::ArchiveV2CurrentWincode055, exact_frame)
                    .with_context(|| {
                        format!("PoH frame {frame} changed from the selected current schema")
                    })?;
            return Ok((SourcePohSchema::Current, decoded));
        }
        Some(SourcePohSchema::LegacyNoSignatureCount) => {
            let decoded = poh::decode_frame(
                poh::PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055,
                exact_frame,
            )
            .with_context(|| {
                format!("PoH frame {frame} changed from the selected legacy schema")
            })?;
            return Ok((SourcePohSchema::LegacyNoSignatureCount, decoded));
        }
        Some(SourcePohSchema::NoEntrySchemaEvidence) => {
            bail!("an unproved PoH schema cannot be selected")
        }
        None => {}
    }
    let current = poh::decode_frame(poh::PohWireProfile::ArchiveV2CurrentWincode055, exact_frame);
    let legacy = poh::decode_frame(
        poh::PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055,
        exact_frame,
    );
    match (current, legacy) {
        (Ok(decoded), Err(_)) => Ok((SourcePohSchema::Current, decoded)),
        (Err(_), Ok(decoded)) => Ok((SourcePohSchema::LegacyNoSignatureCount, decoded)),
        (Ok(_), Ok(_)) => bail!(
            "PoH frame {frame} is ambiguous between current and legacy schemas; empty frames cannot bind a profile"
        ),
        (Err(current), Err(legacy)) => bail!(
            "PoH frame {frame} does not match a strict target schema: current={current}; legacy={legacy}"
        ),
    }
}

fn validate_current(
    entries: &[poh::CurrentPohEntry],
    row: &ArchiveV2HotBlockIndexRow,
) -> Result<BlockSignatureCountCoverage> {
    let tx_count = entries.iter().try_fold(0_u32, |total, entry| {
        total
            .checked_add(entry.transaction_count)
            .context("PoH transaction count overflow")
    })?;
    ensure!(
        entries.is_empty() || tx_count == row.tx_count,
        "block {} PoH has {tx_count} transactions, expected {}",
        row.block_id,
        row.tx_count
    );
    if entries.is_empty() {
        return Ok(BlockSignatureCountCoverage::NoEntries);
    }
    let signatures = entries.iter().try_fold(0_u32, |total, entry| {
        total
            .checked_add(entry.signature_count)
            .context("PoH signature count overflow")
    })?;
    if signatures == row.signature_count {
        ensure!(
            entries.iter().all(|entry| {
                (entry.transaction_count == 0 && entry.signature_count == 0)
                    || (entry.transaction_count != 0
                        && entry.signature_count >= entry.transaction_count)
            }),
            "block {} has an invalid PoH transaction/signature partition",
            row.block_id
        );
        Ok(BlockSignatureCountCoverage::CurrentExact)
    } else {
        ensure!(
            entries.iter().all(|entry| entry.signature_count == 0),
            "block {} PoH signature total {signatures} does not match {}",
            row.block_id,
            row.signature_count
        );
        Ok(BlockSignatureCountCoverage::LegacyUnknown)
    }
}

fn validate_legacy(
    entries: &[poh::LegacyPohEntry],
    row: &ArchiveV2HotBlockIndexRow,
) -> Result<BlockSignatureCountCoverage> {
    let tx_count = entries.iter().try_fold(0_u32, |total, entry| {
        total
            .checked_add(entry.transaction_count)
            .context("PoH transaction count overflow")
    })?;
    ensure!(
        entries.is_empty() || tx_count == row.tx_count,
        "block {} legacy PoH has {tx_count} transactions, expected {}",
        row.block_id,
        row.tx_count
    );
    Ok(if entries.is_empty() {
        BlockSignatureCountCoverage::NoEntries
    } else {
        BlockSignatureCountCoverage::LegacyUnknown
    })
}

pub(crate) fn retain_poh(
    source: File,
    output: &Path,
    archive_id: ArchiveId,
    rows: &[ArchiveV2HotBlockIndexRow],
    blockhash_registry: &[u8],
    epoch: u64,
) -> Result<RetainedPoh> {
    retain_poh_selected(
        source,
        output,
        archive_id,
        rows,
        rows.len(),
        blockhash_registry,
        epoch,
        true,
    )
}

/// Retain a leading block range for a non-publishable converter benchmark.
///
/// The full source block count keeps the blockhash-registry admission exact.
/// Trailing PoH frames are expected because a prefix is not a generation.
pub(crate) fn retain_poh_prefix(
    source: File,
    output: &Path,
    archive_id: ArchiveId,
    rows: &[ArchiveV2HotBlockIndexRow],
    source_block_count: usize,
    blockhash_registry: &[u8],
    epoch: u64,
) -> Result<RetainedPoh> {
    retain_poh_selected(
        source,
        output,
        archive_id,
        rows,
        source_block_count,
        blockhash_registry,
        epoch,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn retain_poh_selected(
    source: File,
    output: &Path,
    archive_id: ArchiveId,
    rows: &[ArchiveV2HotBlockIndexRow],
    source_block_count: usize,
    blockhash_registry: &[u8],
    epoch: u64,
    require_source_eof: bool,
) -> Result<RetainedPoh> {
    ensure!(
        !rows.is_empty() && rows.len() <= source_block_count,
        "selected PoH prefix must contain 1..={source_block_count} rows"
    );
    ensure!(
        blockhash_registry.len().is_multiple_of(32),
        "blockhash registry is not 32-byte aligned"
    );
    let registry_records = blockhash_registry.len() / 32;
    let registry_offset = match detect_blockhash_registry_layout(
        registry_records,
        source_block_count,
    )
    .with_context(|| {
        format!(
            "validate blockhash registry layout for {source_block_count} blocks in epoch {epoch}"
        )
    })? {
        BlockhashRegistryLayout::LegacyCurrentOnly => 0,
        BlockhashRegistryLayout::BoundaryPrefixed => 1,
    };

    let mut reader = BufReader::with_capacity(8 << 20, source);
    let mut writer = HeaderedWriter::create(output, poh::PATH, 8 << 20)?;
    // The source profile is proved while the frames are validated. Reserve
    // its fixed slot now, then patch the selected profile before this staging
    // object can take part in archive-ID derivation.
    writer.append(&[0_u8; poh::PREAMBLE_LEN], poh::PREAMBLE_LEN as u64)?;
    let mut mappings = Vec::with_capacity(rows.len());
    let mut spans = Vec::with_capacity(rows.len());
    let mut next_entry = 0_u64;
    let mut source_schema = None;

    for (frame_index, row) in rows.iter().enumerate() {
        ensure!(
            usize::try_from(row.block_id).ok() == Some(frame_index),
            "hot index block ID {} is not position {frame_index}",
            row.block_id
        );
        let exact_frame = read_frame(&mut reader, frame_index)?
            .with_context(|| format!("PoH ended before block {}", row.block_id))?;
        let (found, frame) = decode_poh_frame(&exact_frame, frame_index, source_schema)?;
        if let Some(expected) = source_schema {
            ensure!(
                expected == found,
                "PoH changes schema at frame {frame_index}"
            );
        } else {
            source_schema = Some(found);
        }
        let (block_id, slot) = frame.identity();
        ensure!(
            block_id == row.block_id && slot == row.slot,
            "PoH frame {frame_index} is block {block_id} slot {slot}, expected block {} slot {}",
            row.block_id,
            row.slot
        );

        let (entry_count, final_hash, coverage) = match &frame {
            poh::DecodedPohFrame::Current(record) => (
                u32::try_from(record.entries.len()).context("PoH entry count exceeds u32")?,
                record.entries.last().map(|entry| entry.hash),
                validate_current(&record.entries, row)?,
            ),
            poh::DecodedPohFrame::LegacyNoSignatureCount(record) => (
                u32::try_from(record.entries.len()).context("PoH entry count exceeds u32")?,
                record.entries.last().map(|entry| entry.hash),
                validate_legacy(&record.entries, row)?,
            ),
        };
        ensure!(
            entry_count != 0,
            "block {} PoH record has no entries; empty source frames do not prove PoH coverage",
            row.block_id
        );
        let expected_hash: [u8; 32] = blockhash_registry
            [(registry_offset + frame_index) * 32..(registry_offset + frame_index + 1) * 32]
            .try_into()
            .expect("checked registry record range");
        if let Some(final_hash) = final_hash {
            ensure!(
                final_hash == expected_hash,
                "block {} final PoH hash does not match the blockhash registry",
                row.block_id
            );
        }

        let frame_len = u32::try_from(exact_frame.len()).context("PoH frame exceeds u32")?;
        let offset = writer.append(&exact_frame, u64::from(frame_len))?;
        spans.push(PageSpan {
            offset,
            stored_len: frame_len,
            decoded_len: frame_len,
        });

        let final_entry_ordinal = entry_count
            .checked_sub(1)
            .map(u64::from)
            .and_then(|delta| next_entry.checked_add(delta));
        ensure!(
            entry_count == 0 || final_entry_ordinal.is_some(),
            "PoH ordinal overflow"
        );
        mappings.push(PohBlockMapping {
            source_block_id: row.block_id,
            block_ordinal: frame_index as u64,
            first_entry_ordinal: next_entry,
            entry_count,
            final_entry_ordinal,
            signature_count_coverage: coverage,
            block_signature_count: row.signature_count,
        });
        next_entry = next_entry
            .checked_add(u64::from(entry_count))
            .context("PoH entry ordinal overflow")?;
    }
    if require_source_eof {
        ensure!(
            read_frame(&mut reader, rows.len())?.is_none(),
            "PoH has trailing frames"
        );
    }
    let source_schema = source_schema.context("PoH has no frame that proves its source schema")?;
    let profile = match source_schema {
        SourcePohSchema::Current => poh::PohWireProfile::ArchiveV2CurrentWincode055,
        SourcePohSchema::LegacyNoSignatureCount => {
            poh::PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055
        }
        SourcePohSchema::NoEntrySchemaEvidence => unreachable!("rejected above"),
    };
    let preamble = poh::PohPreamble { profile }.encode();
    poh::PohPreamble::decode(&preamble).context("validate PoH target preamble")?;
    let object = writer.finish(archive_id, rows.len() as u64)?;
    let target = output.join(poh::PATH);
    let target_file = File::options()
        .write(true)
        .open(&target)
        .with_context(|| format!("open {} to bind PoH profile", target.display()))?;
    target_file
        .write_all_at(&preamble, FILE_HEADER_LEN as u64)
        .with_context(|| format!("write {} PoH profile", target.display()))?;
    target_file
        .sync_all()
        .with_context(|| format!("sync {} PoH profile", target.display()))?;
    Ok(RetainedPoh {
        source_schema,
        blockhash_registry_offset: registry_offset as u32,
        mappings,
        spans,
        entry_count: next_entry,
        object,
    })
}

pub(crate) fn retain_shredding(
    source: File,
    output: &Path,
    archive_id: ArchiveId,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> Result<RetainedShredding> {
    retain_shredding_selected(source, output, archive_id, rows, true)
}

/// Retain a leading shredding range for a non-publishable benchmark.
pub(crate) fn retain_shredding_prefix(
    source: File,
    output: &Path,
    archive_id: ArchiveId,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> Result<RetainedShredding> {
    retain_shredding_selected(source, output, archive_id, rows, false)
}

fn retain_shredding_selected(
    source: File,
    output: &Path,
    archive_id: ArchiveId,
    rows: &[ArchiveV2HotBlockIndexRow],
    require_source_eof: bool,
) -> Result<RetainedShredding> {
    ensure!(!rows.is_empty(), "selected shredding prefix is empty");
    let mut reader = BufReader::with_capacity(8 << 20, source);
    let mut writer = HeaderedWriter::create(output, shredding::PATH, 8 << 20)?;
    let preamble = shredding::ShreddingPreamble {
        profile: shredding::ShreddingWireProfile::ArchiveV2Wincode055,
    }
    .encode();
    shredding::ShreddingPreamble::decode(&preamble)
        .context("validate shredding target preamble")?;
    writer.append(&preamble, preamble.len() as u64)?;
    let mut spans = Vec::with_capacity(rows.len());
    let mut boundary_count = 0_u64;
    let mut recorded_empty_blocks = 0_u64;
    for (frame_index, row) in rows.iter().enumerate() {
        let exact_frame = read_frame(&mut reader, frame_index)?
            .with_context(|| format!("shredding ended before block {}", row.block_id))?;
        let record = shredding::decode_frame(
            shredding::ShreddingWireProfile::ArchiveV2Wincode055,
            &exact_frame,
        )
        .with_context(|| {
            format!("shredding frame {frame_index} is not canonical target Wincode")
        })?;
        ensure!(
            record.block_id == row.block_id && record.slot == row.slot,
            "shredding frame {frame_index} is block {} slot {}, expected block {} slot {}",
            record.block_id,
            record.slot,
            row.block_id,
            row.slot
        );
        if record.boundaries.is_empty() {
            recorded_empty_blocks += 1;
        }
        boundary_count = boundary_count
            .checked_add(record.boundaries.len() as u64)
            .context("shredding boundary count overflow")?;
        let frame_len = u32::try_from(exact_frame.len()).context("shredding frame exceeds u32")?;
        let offset = writer.append(&exact_frame, u64::from(frame_len))?;
        spans.push(PageSpan {
            offset,
            stored_len: frame_len,
            decoded_len: frame_len,
        });
    }
    if require_source_eof {
        ensure!(
            read_frame(&mut reader, rows.len())?.is_none(),
            "shredding has trailing frames"
        );
    }
    let object = writer.finish(archive_id, rows.len() as u64)?;
    Ok(RetainedShredding {
        spans,
        boundary_count,
        recorded_empty_blocks,
        object,
    })
}

fn read_frame(reader: &mut impl Read, frame: usize) -> Result<Option<Vec<u8>>> {
    let mut prefix = Vec::with_capacity(5);
    let mut value = 0_u32;
    for shift in [0_u32, 7, 14, 21, 28] {
        let mut byte = [0_u8; 1];
        let read = reader
            .read(&mut byte)
            .with_context(|| format!("read frame {frame} length"))?;
        if read == 0 {
            ensure!(
                prefix.is_empty(),
                "frame {frame} has a truncated length prefix"
            );
            return Ok(None);
        }
        prefix.push(byte[0]);
        let payload = u32::from(byte[0] & 0x7f);
        ensure!(
            shift != 28 || payload <= 0x0f,
            "frame {frame} length overflows u32"
        );
        value |= payload << shift;
        if byte[0] & 0x80 == 0 {
            ensure!(
                prefix.len() == 1 || payload != 0,
                "frame {frame} has a non-canonical length prefix"
            );
            let len = usize::try_from(value).context("frame length exceeds usize")?;
            ensure!(
                len <= MAX_FRAME_BYTES,
                "frame {frame} declares {len} bytes, above the {MAX_FRAME_BYTES}-byte limit"
            );
            let prefix_len = prefix.len();
            prefix.resize(
                prefix_len
                    .checked_add(len)
                    .context("frame length exceeds usize")?,
                0,
            );
            reader
                .read_exact(&mut prefix[prefix_len..])
                .with_context(|| format!("read frame {frame} payload"))?;
            return Ok(Some(prefix));
        }
    }
    bail!("frame {frame} length prefix is too long")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use blockzilla_archive_v2::{
        WincodeArchiveV2PohRecord, WincodeArchiveV2PohRecordLegacyNoSignatureCount,
        WincodeArchiveV2ShreddingRecord,
    };
    use blockzilla_archive_v3::sidecars::{framing, poh::DecodedPohFrame};
    use blockzilla_compact::{
        CompactPohEntry, CompactPohEntryLegacyNoSignatureCount, CompactShredding,
    };
    use blockzilla_primitives::WincodeLeb128FramedWriter;
    use tempfile::tempdir;

    use super::*;

    fn row(tx_count: u32, signature_count: u32) -> ArchiveV2HotBlockIndexRow {
        ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 100,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count,
        }
    }

    fn source_frame<T>(record: &T) -> Vec<u8>
    where
        T: wincode::SchemaWrite<blockzilla_primitives::WincodeLeb128Config, Src = T>,
    {
        let mut writer = WincodeLeb128FramedWriter::new(Vec::new());
        writer.write(record).unwrap();
        writer.into_inner()
    }

    fn target_frame(bytes: &[u8], span: PageSpan) -> &[u8] {
        let start = usize::try_from(span.offset).unwrap();
        let end = start + usize::try_from(span.stored_len).unwrap();
        &bytes[start..end]
    }

    fn pad_first_payload_integer(frame: &[u8]) -> Vec<u8> {
        let payload = framing::decode_frame(frame).unwrap();
        assert_eq!(payload[0], 0, "test record block ID must be zero");
        let mut padded = Vec::with_capacity(payload.len() + 1);
        padded.extend_from_slice(&[0x80, 0]);
        padded.extend_from_slice(&payload[1..]);
        framing::encode_frame(&padded).unwrap()
    }

    #[test]
    fn benchmark_prefix_retains_selected_frames_and_allows_trailing_source_frames() {
        let first_poh = WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: 100,
            entries: vec![CompactPohEntry {
                num_hashes: 1,
                hash: [9; 32],
                tx_count: 1,
                signature_count: 1,
            }],
        };
        let second_poh = WincodeArchiveV2PohRecord {
            block_id: 1,
            slot: 101,
            entries: vec![CompactPohEntry {
                num_hashes: 1,
                hash: [8; 32],
                tx_count: 1,
                signature_count: 1,
            }],
        };
        let first_shredding = WincodeArchiveV2ShreddingRecord {
            block_id: 0,
            slot: 100,
            shredding: vec![CompactShredding {
                entry_end_idx: 1,
                shred_end_idx: 2,
            }],
        };
        let second_shredding = WincodeArchiveV2ShreddingRecord {
            block_id: 1,
            slot: 101,
            shredding: vec![CompactShredding {
                entry_end_idx: 3,
                shred_end_idx: 4,
            }],
        };
        let root = tempdir().unwrap();
        let poh_source = root.path().join("source-prefix.poh");
        let shredding_source = root.path().join("source-prefix.shredding");
        fs::write(
            &poh_source,
            [source_frame(&first_poh), source_frame(&second_poh)].concat(),
        )
        .unwrap();
        fs::write(
            &shredding_source,
            [
                source_frame(&first_shredding),
                source_frame(&second_shredding),
            ]
            .concat(),
        )
        .unwrap();

        let poh_output = root.path().join("poh-output");
        let retained_poh = retain_poh_prefix(
            File::open(&poh_source).unwrap(),
            &poh_output,
            ArchiveId::new([6; 16]),
            &[row(1, 1)],
            2,
            &[[9; 32], [8; 32]].concat(),
            1,
        )
        .unwrap();
        assert_eq!(retained_poh.spans.len(), 1);
        assert_eq!(retained_poh.mappings.len(), 1);

        let shredding_output = root.path().join("shredding-output");
        let retained_shredding = retain_shredding_prefix(
            File::open(&shredding_source).unwrap(),
            &shredding_output,
            ArchiveId::new([7; 16]),
            &[row(1, 1)],
        )
        .unwrap();
        assert_eq!(retained_shredding.spans.len(), 1);
        assert_eq!(retained_shredding.boundary_count, 1);
    }

    #[test]
    fn retained_current_poh_frame_decodes_with_bound_target_profile() {
        let source = WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: 100,
            entries: vec![
                CompactPohEntry {
                    num_hashes: 7,
                    hash: [8; 32],
                    tx_count: 1,
                    signature_count: 1,
                },
                CompactPohEntry {
                    num_hashes: 9,
                    hash: [9; 32],
                    tx_count: 1,
                    signature_count: 2,
                },
            ],
        };
        let source_frame = source_frame(&source);
        let root = tempdir().unwrap();
        let source_path = root.path().join("source-current.poh");
        fs::write(&source_path, &source_frame).unwrap();
        let retained = retain_poh(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([1; 16]),
            &[row(2, 3)],
            &[9; 32],
            1,
        )
        .unwrap();
        let target = fs::read(root.path().join(poh::PATH)).unwrap();
        let preamble =
            poh::PohPreamble::decode(&target[FILE_HEADER_LEN..FILE_HEADER_LEN + poh::PREAMBLE_LEN])
                .unwrap();
        assert_eq!(
            preamble.profile,
            poh::PohWireProfile::ArchiveV2CurrentWincode055
        );
        let exact_frame = target_frame(&target, retained.spans[0]);
        assert_eq!(exact_frame, source_frame);
        assert_eq!(
            poh::decode_frame(preamble.profile, exact_frame).unwrap(),
            DecodedPohFrame::Current(poh::CurrentPohRecord {
                block_id: 0,
                slot: 100,
                entries: vec![
                    poh::CurrentPohEntry {
                        num_hashes: 7,
                        hash: [8; 32],
                        transaction_count: 1,
                        signature_count: 1,
                    },
                    poh::CurrentPohEntry {
                        num_hashes: 9,
                        hash: [9; 32],
                        transaction_count: 1,
                        signature_count: 2,
                    },
                ],
            })
        );
        assert_eq!(retained.blockhash_registry_offset, 0);
    }

    #[test]
    fn nonzero_epoch_accepts_a_boundary_prefixed_blockhash_registry() {
        let source = WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: 100,
            entries: vec![CompactPohEntry {
                num_hashes: 7,
                hash: [9; 32],
                tx_count: 1,
                signature_count: 1,
            }],
        };
        let root = tempdir().unwrap();
        let source_path = root.path().join("source-boundary.poh");
        fs::write(&source_path, source_frame(&source)).unwrap();

        let retained = retain_poh(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([8; 16]),
            &[row(1, 1)],
            &[[8; 32], [9; 32]].concat(),
            2,
        )
        .unwrap();

        assert_eq!(retained.blockhash_registry_offset, 1);
    }

    #[test]
    fn retained_legacy_poh_frame_decodes_with_bound_target_profile() {
        let source = WincodeArchiveV2PohRecordLegacyNoSignatureCount {
            block_id: 0,
            slot: 100,
            entries: vec![
                CompactPohEntryLegacyNoSignatureCount {
                    num_hashes: 5,
                    hash: [6; 32],
                    tx_count: 1,
                },
                CompactPohEntryLegacyNoSignatureCount {
                    num_hashes: 8,
                    hash: [7; 32],
                    tx_count: 1,
                },
            ],
        };
        let source_frame = source_frame(&source);
        let root = tempdir().unwrap();
        let source_path = root.path().join("source-legacy.poh");
        fs::write(&source_path, &source_frame).unwrap();
        let retained = retain_poh(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([2; 16]),
            &[row(2, 3)],
            &[7; 32],
            1,
        )
        .unwrap();
        let target = fs::read(root.path().join(poh::PATH)).unwrap();
        let preamble =
            poh::PohPreamble::decode(&target[FILE_HEADER_LEN..FILE_HEADER_LEN + poh::PREAMBLE_LEN])
                .unwrap();
        assert_eq!(
            preamble.profile,
            poh::PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055
        );
        let exact_frame = target_frame(&target, retained.spans[0]);
        assert_eq!(exact_frame, source_frame);
        assert_eq!(
            poh::decode_frame(preamble.profile, exact_frame).unwrap(),
            DecodedPohFrame::LegacyNoSignatureCount(poh::LegacyPohRecord {
                block_id: 0,
                slot: 100,
                entries: vec![
                    poh::LegacyPohEntry {
                        num_hashes: 5,
                        hash: [6; 32],
                        transaction_count: 1,
                    },
                    poh::LegacyPohEntry {
                        num_hashes: 8,
                        hash: [7; 32],
                        transaction_count: 1,
                    },
                ],
            })
        );
    }

    #[test]
    fn empty_poh_frame_does_not_claim_complete_coverage() {
        let source = WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: 100,
            entries: Vec::new(),
        };
        let root = tempdir().unwrap();
        let source_path = root.path().join("source-empty.poh");
        fs::write(&source_path, source_frame(&source)).unwrap();
        let error = retain_poh(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([4; 16]),
            &[row(0, 0)],
            &[9; 32],
            1,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("ambiguous") && error.contains("empty frames"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn retain_poh_rejects_padded_payload_leb128() {
        let source = WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: 100,
            entries: vec![CompactPohEntry {
                num_hashes: 7,
                hash: [9; 32],
                tx_count: 1,
                signature_count: 1,
            }],
        };
        let padded_frame = pad_first_payload_integer(&source_frame(&source));
        let root = tempdir().unwrap();
        let source_path = root.path().join("source-padded.poh");
        fs::write(&source_path, padded_frame).unwrap();

        let error = retain_poh(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([5; 16]),
            &[row(1, 1)],
            &[9; 32],
            1,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("does not match a strict target schema"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn retained_shredding_frame_decodes_with_bound_target_profile() {
        let source = WincodeArchiveV2ShreddingRecord {
            block_id: 0,
            slot: 100,
            shredding: vec![
                CompactShredding {
                    entry_end_idx: 11,
                    shred_end_idx: 22,
                },
                CompactShredding {
                    entry_end_idx: 33,
                    shred_end_idx: 44,
                },
            ],
        };
        let source_frame = source_frame(&source);
        let root = tempdir().unwrap();
        let source_path = root.path().join("source.shredding");
        fs::write(&source_path, &source_frame).unwrap();
        let retained = retain_shredding(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([3; 16]),
            &[row(0, 0)],
        )
        .unwrap();
        let target = fs::read(root.path().join(shredding::PATH)).unwrap();
        let preamble = shredding::ShreddingPreamble::decode(
            &target[FILE_HEADER_LEN..FILE_HEADER_LEN + shredding::PREAMBLE_LEN],
        )
        .unwrap();
        assert_eq!(
            preamble.profile,
            shredding::ShreddingWireProfile::ArchiveV2Wincode055
        );
        let exact_frame = target_frame(&target, retained.spans[0]);
        assert_eq!(exact_frame, source_frame);
        assert_eq!(
            shredding::decode_frame(preamble.profile, exact_frame).unwrap(),
            shredding::ShreddingRecord {
                block_id: 0,
                slot: 100,
                boundaries: vec![
                    shredding::ShreddingBoundary {
                        entry_end_index: 11,
                        shred_end_index: 22,
                    },
                    shredding::ShreddingBoundary {
                        entry_end_index: 33,
                        shred_end_index: 44,
                    },
                ],
            }
        );
    }

    #[test]
    fn retain_shredding_rejects_padded_payload_leb128() {
        let source = WincodeArchiveV2ShreddingRecord {
            block_id: 0,
            slot: 100,
            shredding: vec![CompactShredding {
                entry_end_idx: 11,
                shred_end_idx: 22,
            }],
        };
        let padded_frame = pad_first_payload_integer(&source_frame(&source));
        let root = tempdir().unwrap();
        let source_path = root.path().join("source-padded.shredding");
        fs::write(&source_path, padded_frame).unwrap();

        let error = retain_shredding(
            File::open(source_path).unwrap(),
            root.path(),
            ArchiveId::new([6; 16]),
            &[row(0, 0)],
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("not canonical target Wincode"),
            "unexpected error: {error}"
        );
    }
}
