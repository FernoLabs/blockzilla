//! Print the consensus id and Alpenglow markers stored on hot-block Archive V2.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ArchiveV2HotBlockBlob, ArchiveV2HotBlockIndexRow,
    archive_v2_hot_index_path, deserialize_archive_v2_hot_block_blob,
    read_archive_v2_hot_block_index,
};
use clap::Args;
use of_car_reader::blockmarker::{self, Marker};

#[derive(Args, Debug)]
pub(crate) struct ShowArchiveV2BlockMarkersArgs {
    /// Directory that contains archive-v2-blocks.zstd and archive-v2-blocks.index.
    pub archive_dir: PathBuf,
    /// Print this slot only.
    #[arg(long)]
    pub slot: Option<u64>,
    /// Stop after N blocks, in index order.
    #[arg(long)]
    pub max_blocks: Option<u64>,
    /// Require each parent consensus id to equal the previous block's consensus id.
    ///
    /// The first shown block has no previous id. A previous block with no consensus id
    /// starts the chain again.
    #[arg(long)]
    pub check_parent_chain: bool,
}

pub(crate) fn run(args: ShowArchiveV2BlockMarkersArgs) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    write!(stdout, "{}", render(&args)?)?;
    Ok(())
}

fn render(args: &ShowArchiveV2BlockMarkersArgs) -> Result<String> {
    if let Some(0) = args.max_blocks {
        return Err(anyhow!("--max-blocks must be greater than zero"));
    }
    let blocks_path = args.archive_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index = read_archive_v2_hot_block_index(&archive_v2_hot_index_path(&blocks_path))
        .with_context(|| {
            format!(
                "read {}",
                args.archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE).display()
            )
        })?;
    let raw_blocks = index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
    let rows = selected_rows(&index.rows, args)?;
    if !raw_blocks && index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY != 0 && !rows.is_empty() {
        return Err(anyhow!(
            "archive index uses a zstd dictionary; this command reads plain zstd frames"
        ));
    }

    let mut file =
        File::open(&blocks_path).with_context(|| format!("open {}", blocks_path.display()))?;
    let mut out = format!("blocks {}\n", rows.len());
    let mut previous_consensus: Option<[u8; 32]> = None;
    for row in rows {
        let plain = read_block_bytes(&mut file, raw_blocks, row)?;
        let block = deserialize_archive_v2_hot_block_blob(&plain)
            .map_err(|err| anyhow!("slot {} hot block: {err}", row.slot))?;
        ensure!(
            block.header.slot == row.slot,
            "slot {} index row decodes as slot {}",
            row.slot,
            block.header.slot
        );
        ensure!(
            block.tx_count == row.tx_count,
            "slot {} index tx_count {} decodes as {}",
            row.slot,
            row.tx_count,
            block.tx_count
        );
        let parent = blockmarker::parent_consensus_block_id(&block.block_markers);
        if args.check_parent_chain {
            if let Some(previous) = previous_consensus {
                ensure!(
                    parent == Some(previous),
                    "slot {} parent {} does not match previous consensus {}",
                    block.header.slot,
                    hex(parent),
                    hex(Some(previous))
                );
            }
            previous_consensus = block.consensus_block_id;
        }
        append_block(&mut out, &block, plain.len(), parent)?;
    }
    Ok(out)
}

fn selected_rows<'a>(
    rows: &'a [ArchiveV2HotBlockIndexRow],
    args: &ShowArchiveV2BlockMarkersArgs,
) -> Result<Vec<&'a ArchiveV2HotBlockIndexRow>> {
    let mut selected: Vec<_> = rows
        .iter()
        .filter(|row| args.slot.is_none_or(|slot| row.slot == slot))
        .collect();
    if let Some(slot) = args.slot {
        ensure!(!selected.is_empty(), "slot {slot} not found");
    }
    if let Some(max_blocks) = args.max_blocks {
        selected.truncate(usize::try_from(max_blocks).unwrap_or(usize::MAX));
    }
    Ok(selected)
}

fn read_block_bytes(
    file: &mut File,
    raw_blocks: bool,
    row: &ArchiveV2HotBlockIndexRow,
) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(row.compressed_offset))
        .with_context(|| format!("seek slot {}", row.slot))?;
    if raw_blocks {
        let mut bytes = vec![0; row.uncompressed_len as usize];
        file.read_exact(&mut bytes)
            .with_context(|| format!("read raw slot {}", row.slot))?;
        return Ok(bytes);
    }
    let mut input = vec![0; row.compressed_len as usize];
    file.read_exact(&mut input)
        .with_context(|| format!("read zstd slot {}", row.slot))?;
    let plain = zstd::bulk::decompress(&input, row.uncompressed_len as usize)
        .with_context(|| format!("decompress slot {}", row.slot))?;
    ensure!(
        plain.len() == row.uncompressed_len as usize,
        "slot {} decompressed to {} bytes, index says {}",
        row.slot,
        plain.len(),
        row.uncompressed_len
    );
    Ok(plain)
}

fn append_block(
    out: &mut String,
    block: &ArchiveV2HotBlockBlob,
    uncompressed_len: usize,
    parent: Option<[u8; 32]>,
) -> Result<()> {
    out.push_str(&format!(
        "slot {} txs {} uncompressed {uncompressed_len} consensus {} parent {} markers {}\n",
        block.header.slot,
        block.tx_count,
        hex(block.consensus_block_id),
        hex(parent),
        block.block_markers.len()
    ));
    for (index, marker) in block.block_markers.iter().enumerate() {
        let parsed = blockmarker::parse(marker)
            .map_err(|err| anyhow!("slot {} marker {index}: {err}", block.header.slot))?;
        match parsed {
            Marker::Header(header) => {
                out.push_str(&format!(
                    "  marker {index} header parent_slot {} parent {}\n",
                    header.parent_slot,
                    hex(Some(header.parent_block_id))
                ));
            }
            Marker::UpdateParent(update) => {
                out.push_str(&format!(
                    "  marker {index} update-parent slot {} parent {}\n",
                    update.new_parent_slot,
                    hex(Some(update.new_parent_block_id))
                ));
            }
            Marker::Footer(footer) => {
                let certs = blockmarker::decode_footer_certificates(&footer.certificates).map_err(
                    |err| anyhow!("slot {} marker {index} footer: {err}", block.header.slot),
                )?;
                let kind = match certs.finalization.as_ref() {
                    Some(cert) if cert.notar_aggregate.is_some() => "slow",
                    Some(_) => "fast",
                    None => "no-finalization",
                };
                out.push_str(&format!(
                    "  marker {index} footer bytes {} bank {} agent {:?} finalization {kind} skip {} notar-reward {}\n",
                    marker.len(),
                    hex(Some(footer.bank_hash)),
                    String::from_utf8_lossy(&footer.block_user_agent),
                    certs.skip_reward.is_some(),
                    certs.notar_reward.is_some()
                ));
            }
            Marker::Genesis(cert) => {
                out.push_str(&format!(
                    "  marker {index} genesis slot {} block {}\n",
                    cert.slot,
                    hex(Some(cert.block_id))
                ));
            }
        }
    }
    Ok(())
}

fn hex(bytes: Option<[u8; 32]>) -> String {
    match bytes {
        None => "none".to_string(),
        Some(bytes) => bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_archive_v2::{ArchiveV2HotBlockHeader, write_archive_v2_hot_block_index};
    use blockzilla_primitives::historical_source_wincode_leb128_config;

    fn header_marker(variant: u8, parent_slot: u64, parent: [u8; 32]) -> Vec<u8> {
        let mut marker = vec![0u8; 46];
        marker[0] = 1;
        marker[2] = variant;
        marker[3] = 41;
        marker[5] = 1;
        marker[6..14].copy_from_slice(&parent_slot.to_le_bytes());
        marker[14..46].copy_from_slice(&parent);
        marker
    }

    fn footer_marker() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(1);
        payload.extend_from_slice(&[0xab; 32]);
        payload.extend_from_slice(&7u64.to_le_bytes());
        let agent = b"agave/4.3.2";
        payload.push(u8::try_from(agent.len()).unwrap());
        payload.extend_from_slice(agent);
        payload.extend_from_slice(&[0, 0, 0]);
        let mut marker = Vec::new();
        marker.extend_from_slice(&1u16.to_le_bytes());
        marker.push(0);
        marker.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
        marker.extend(payload);
        marker
    }

    fn hot_block(
        slot: u64,
        consensus: Option<[u8; 32]>,
        markers: Vec<Vec<u8>>,
    ) -> ArchiveV2HotBlockBlob {
        ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot,
                parent_slot: slot.saturating_sub(1),
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
            consensus_block_id: consensus,
            block_markers: markers,
        }
    }

    fn write_archive(dir: &Path, blocks: &[ArchiveV2HotBlockBlob], flags: u32) {
        let raw = flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
        let mut blob = Vec::new();
        let mut rows = Vec::new();
        for (index, block) in blocks.iter().enumerate() {
            let plain =
                wincode::config::serialize(block, historical_source_wincode_leb128_config())
                    .unwrap();
            let stored = if raw {
                plain.clone()
            } else {
                zstd::bulk::compress(&plain, 1).unwrap()
            };
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id: u32::try_from(index).unwrap(),
                slot: block.header.slot,
                compressed_offset: u64::try_from(blob.len()).unwrap(),
                compressed_len: u32::try_from(stored.len()).unwrap(),
                uncompressed_len: u32::try_from(plain.len()).unwrap(),
                tx_count: block.tx_count,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 0,
            });
            blob.extend(stored);
        }
        std::fs::write(dir.join(ARCHIVE_V2_BLOCKS_FILE), &blob).unwrap();
        write_archive_v2_hot_block_index(
            &dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            u64::try_from(blob.len()).unwrap(),
            1,
            flags,
            &rows,
        )
        .unwrap();
    }

    fn args(dir: &Path) -> ShowArchiveV2BlockMarkersArgs {
        ShowArchiveV2BlockMarkersArgs {
            archive_dir: dir.to_path_buf(),
            slot: None,
            max_blocks: None,
            check_parent_chain: false,
        }
    }

    #[test]
    fn prints_markers_and_accepts_a_parent_chain() {
        let dir = tempfile::tempdir().unwrap();
        let first = [0x11; 32];
        let second = [0x22; 32];
        write_archive(
            dir.path(),
            &[
                hot_block(
                    10,
                    Some(first),
                    vec![header_marker(1, 9, [0x10; 32]), footer_marker()],
                ),
                hot_block(
                    11,
                    Some(second),
                    vec![
                        header_marker(1, 10, [0x99; 32]),
                        header_marker(2, 10, first),
                    ],
                ),
            ],
            0,
        );
        let mut request = args(dir.path());
        request.check_parent_chain = true;
        let text = render(&request).unwrap();
        assert!(text.starts_with("blocks 2\n"));
        let slot10 = text
            .lines()
            .find(|line| line.starts_with("slot 10 "))
            .unwrap();
        assert!(slot10.contains("txs 0 "));
        assert!(slot10.contains(&format!(
            "consensus {} parent {} markers 2",
            hex(Some(first)),
            hex(Some([0x10; 32]))
        )));
        assert!(text.contains(&format!(
            "marker 0 header parent_slot 9 parent {}",
            hex(Some([0x10; 32]))
        )));
        assert!(text.contains("finalization no-finalization skip false notar-reward false"));
        assert!(text.contains("agent \"agave/4.3.2\""));
        let slot11 = text
            .lines()
            .find(|line| line.starts_with("slot 11 "))
            .unwrap();
        assert!(slot11.contains(&format!(
            "consensus {} parent {} markers 2",
            hex(Some(second)),
            hex(Some(first))
        )));
        assert!(text.contains(&format!(
            "marker 0 header parent_slot 10 parent {}",
            hex(Some([0x99; 32]))
        )));
        assert!(text.contains(&format!(
            "marker 1 update-parent slot 10 parent {}",
            hex(Some(first))
        )));
    }

    #[test]
    fn parent_chain_failure_names_the_slot() {
        let dir = tempfile::tempdir().unwrap();
        write_archive(
            dir.path(),
            &[
                hot_block(10, Some([0x11; 32]), vec![header_marker(1, 9, [0x10; 32])]),
                hot_block(11, Some([0x22; 32]), vec![header_marker(1, 10, [0x33; 32])]),
            ],
            0,
        );
        let mut request = args(dir.path());
        request.check_parent_chain = true;
        let error = render(&request).unwrap_err().to_string();
        assert!(error.contains("slot 11 parent"));
        assert!(error.contains("does not match previous consensus"));
    }

    #[test]
    fn slot_and_max_blocks_select_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_archive(
            dir.path(),
            &[
                hot_block(10, Some([0x11; 32]), Vec::new()),
                hot_block(11, Some([0x22; 32]), Vec::new()),
                hot_block(12, None, Vec::new()),
            ],
            0,
        );
        let mut request = args(dir.path());
        request.max_blocks = Some(1);
        let text = render(&request).unwrap();
        assert!(text.starts_with("blocks 1\n"));
        assert!(text.contains("slot 10 "));
        assert!(!text.contains("slot 11 "));

        request.max_blocks = None;
        request.slot = Some(12);
        let text = render(&request).unwrap();
        assert!(text.contains("slot 12 txs 0"));
        assert!(text.contains("consensus none parent none markers 0\n"));
        assert!(!text.contains("slot 10 "));

        request.slot = Some(99);
        let error = render(&request).unwrap_err().to_string();
        assert!(error.contains("slot 99 not found"));

        request.slot = None;
        request.max_blocks = Some(0);
        let error = render(&request).unwrap_err().to_string();
        assert!(error.contains("--max-blocks must be greater than zero"));
    }

    #[test]
    fn reads_raw_block_bytes() {
        let dir = tempfile::tempdir().unwrap();
        write_archive(
            dir.path(),
            &[hot_block(
                10,
                Some([0x44; 32]),
                vec![header_marker(1, 9, [0x10; 32])],
            )],
            ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
        );
        let text = render(&args(dir.path())).unwrap();
        assert!(text.contains(&format!("consensus {}", hex(Some([0x44; 32])))));
        assert!(text.contains("marker 0 header parent_slot 9"));
    }

    #[test]
    fn dictionary_frames_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        write_archive(
            dir.path(),
            &[hot_block(10, None, Vec::new())],
            ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY,
        );
        let error = render(&args(dir.path())).unwrap_err().to_string();
        assert!(error.contains("zstd dictionary"));
    }
}
