use super::{
    PROGRAM_USAGE_EXPORT_VERSION, PROGRESS_FILE_NAME, ProgramStatsProgress, ProgramUsageEpochPart,
    ProgramUsageEpochPartV1, ProgramUsageExport, ProgramUsagePartsPreference, ProgramUsageRecord,
    ProgramUsageStats, Result,
};
use ahash::{AHashMap, AHashSet};
use anyhow::anyhow;
use blockzilla::{
    car_block_reader::CarBlock, confirmed_block::TransactionStatusMeta, node::TransactionNode,
};
use cid::Cid;
use prost::Message;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use tokio::fs;

pub(super) fn decode_epoch_part(bytes: &[u8]) -> Result<ProgramUsageEpochPart> {
    if let Ok(part) = wincode::deserialize::<ProgramUsageEpochPart>(bytes) {
        return Ok(part);
    }

    let legacy = wincode::deserialize::<ProgramUsageEpochPartV1>(bytes)?;
    Ok(ProgramUsageEpochPart {
        epoch: legacy.epoch,
        last_slot: legacy.last_slot,
        last_blocktime: None,
        records: legacy.records,
    })
}

pub(super) async fn load_progress(parts_dir: &Path) -> Result<AHashSet<u64>> {
    let path = parts_dir.join(PROGRESS_FILE_NAME);
    let mut processed = AHashSet::with_capacity(256);

    match fs::read(&path).await {
        Ok(bytes) => match wincode::deserialize::<ProgramStatsProgress>(&bytes) {
            Ok(progress) => {
                for epoch in progress.processed_epochs {
                    processed.insert(epoch);
                }
            }
            Err(err) => {
                tracing::warn!("failed to decode progress file {}: {}", path.display(), err);
            }
        },
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }

    Ok(processed)
}

pub(super) async fn persist_progress(parts_dir: &Path, processed: &AHashSet<u64>) -> Result<()> {
    fs::create_dir_all(parts_dir).await?;

    let mut epochs: Vec<u64> = processed.iter().copied().collect();
    epochs.sort_unstable();
    let progress = ProgramStatsProgress {
        processed_epochs: epochs,
    };

    let encoded = wincode::serialize(&progress)?;
    let path = parts_dir.join(PROGRESS_FILE_NAME);
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, &encoded).await?;
    fs::rename(&tmp_path, &path).await?;
    Ok(())
}

pub(super) async fn load_program_usage_export(
    input_path: &Path,
    parts_preference: ProgramUsagePartsPreference,
) -> Result<ProgramUsageExport> {
    match fs::read(input_path).await {
        Ok(data) => {
            let export: ProgramUsageExport = wincode::deserialize(&data)?;
            if export.version != PROGRAM_USAGE_EXPORT_VERSION {
                return Err(anyhow!(
                    "unsupported program usage export version {}",
                    export.version
                ));
            }
            parts_preference.ensure_matches(export.top_level_only, input_path)?;
            Ok(export)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            load_program_usage_export_from_parts(input_path, parts_preference).await
        }
        Err(e) => Err(e.into()),
    }
}

pub(super) async fn load_program_usage_export_from_parts(
    input_path: &Path,
    parts_preference: ProgramUsagePartsPreference,
) -> Result<ProgramUsageExport> {
    async fn collect_part_entries(parts_dir: &Path) -> Result<Option<Vec<(u64, PathBuf)>>> {
        let mut rd = match fs::read_dir(parts_dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut entries = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }

            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(epoch_str) = name
                .strip_prefix("epoch-")
                .and_then(|s| s.strip_suffix(".bin"))
            else {
                continue;
            };
            let Ok(epoch) = epoch_str.parse::<u64>() else {
                continue;
            };

            entries.push((epoch, entry.path()));
        }

        if entries.is_empty() {
            return Ok(None);
        }

        entries.sort_unstable_by_key(|(epoch, _)| *epoch);
        Ok(Some(entries))
    }

    let parts_path = input_path.with_extension("parts");
    let parts_top_level_path = input_path.with_extension("parts.top_level");

    let (parts_dir, top_level_only, part_entries) = match parts_preference {
        ProgramUsagePartsPreference::Auto => match collect_part_entries(&parts_path).await? {
            Some(entries) => (parts_path, false, entries),
            None => match collect_part_entries(&parts_top_level_path).await? {
                Some(entries) => (parts_top_level_path, true, entries),
                None => {
                    return Err(anyhow!(
                        "no program usage export found at {} or {}",
                        parts_path.display(),
                        parts_top_level_path.display()
                    ));
                }
            },
        },
        ProgramUsagePartsPreference::IncludeInner => match collect_part_entries(&parts_path).await?
        {
            Some(entries) => (parts_path, false, entries),
            None => {
                if collect_part_entries(&parts_top_level_path).await?.is_some() {
                    return Err(anyhow!(
                        "full stats were requested but only top-level parts are available in {}",
                        parts_top_level_path.display()
                    ));
                }
                return Err(anyhow!(
                    "no program usage export parts found in {}",
                    parts_path.display()
                ));
            }
        },
        ProgramUsagePartsPreference::TopLevelOnly => {
            match collect_part_entries(&parts_top_level_path).await? {
                Some(entries) => (parts_top_level_path, true, entries),
                None => {
                    if collect_part_entries(&parts_path).await?.is_some() {
                        return Err(anyhow!(
                            "top-level stats were requested but only full parts are available in {}",
                            parts_path.display()
                        ));
                    }
                    return Err(anyhow!(
                        "no top-level program usage export parts found in {}",
                        parts_top_level_path.display()
                    ));
                }
            }
        }
    };

    parts_preference.ensure_matches(top_level_only, parts_dir.as_path())?;

    let mut aggregated: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(16_384);
    let mut epochs = Vec::with_capacity(part_entries.len());

    for (expected_epoch, path) in part_entries {
        let data = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(
                    "failed to read cached epoch stats {}: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        let part = match decode_epoch_part(&data) {
            Ok(part) => part,
            Err(e) => {
                tracing::warn!(
                    "failed to decode cached epoch stats {}: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        for record in part.records.iter() {
            aggregated
                .entry(record.program)
                .or_insert_with(ProgramUsageStats::default)
                .accumulate(&record.stats);
        }

        if part.epoch != expected_epoch {
            tracing::warn!(
                "cached epoch stats {} has mismatched epoch {} (expected {})",
                path.display(),
                part.epoch,
                expected_epoch
            );
        }

        epochs.push(part);
    }

    if epochs.is_empty() {
        return Err(anyhow!(
            "no program usage export parts found in {}",
            parts_dir.display()
        ));
    }

    epochs.sort_unstable_by_key(|part| part.epoch);

    let mut aggregated_records: Vec<ProgramUsageRecord> = aggregated
        .into_iter()
        .map(|(program, stats)| ProgramUsageRecord { program, stats })
        .collect();
    aggregated_records
        .sort_unstable_by(|a, b| b.stats.instruction_count.cmp(&a.stats.instruction_count));

    Ok(ProgramUsageExport {
        version: PROGRAM_USAGE_EXPORT_VERSION,
        start_epoch: epochs.first().unwrap().epoch,
        start_slot: epochs.first().unwrap().last_slot,
        top_level_only,
        processed_epochs: epochs.iter().map(|part| part.epoch).collect(),
        aggregated: aggregated_records,
        epochs,
    })
}

pub(super) async fn find_last_cached_epoch(cache_dir: &Path) -> Result<Option<u64>> {
    let mut rd = match fs::read_dir(cache_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut max_epoch = None;
    while let Some(entry) = rd.next_entry().await? {
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        let Some(epoch_str) = name.strip_prefix("epoch-").and_then(|s| {
            s.strip_suffix(".car")
                .or_else(|| s.strip_suffix(".car.zst"))
        }) else {
            continue;
        };

        let Ok(epoch) = epoch_str.parse::<u64>() else {
            continue;
        };

        max_epoch = Some(max_epoch.map_or(epoch, |current: u64| current.max(epoch)));
    }

    Ok(max_epoch)
}

#[inline]
pub(super) fn peek_block_slot(block: &CarBlock) -> Result<u64> {
    let mut decoder = minicbor::Decoder::new(block.block_bytes.as_ref());
    decoder
        .array()
        .map_err(|e| anyhow!("failed to decode block header array: {e}"))?;
    let kind = decoder
        .u64()
        .map_err(|e| anyhow!("failed to decode block kind: {e}"))?;
    if kind != 2 {
        return Err(anyhow!("unexpected block kind {kind}, expected Block"));
    }
    let slot = decoder
        .u64()
        .map_err(|e| anyhow!("failed to decode block slot: {e}"))?;
    Ok(slot)
}

#[inline]
pub(super) fn copy_dataframe_into<'a>(
    block: &CarBlock,
    cid: &Cid,
    dst: &'a mut Vec<u8>,
    inline_prefix: Option<&[u8]>,
) -> Result<&'a [u8]> {
    let prefix_len = inline_prefix.map_or(0, |p| p.len());
    dst.clear();
    dst.reserve(prefix_len + (64 << 10));

    if let Some(prefix) = inline_prefix {
        dst.extend_from_slice(prefix);
    }

    let mut rdr = block.dataframe_reader(cid);
    rdr.read_to_end(dst)
        .map_err(|e| anyhow!("read dataframe chain: {e}"))?;
    Ok(&*dst)
}

#[inline]
pub(super) fn transaction_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'_>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.data.next {
        None => {
            buf.clear();
            buf.extend_from_slice(tx_node.data.data);
            Ok(&*buf)
        }
        Some(df_cbor) => {
            let df_cid = df_cbor.to_cid()?;
            copy_dataframe_into(block, &df_cid, buf, None)
        }
    }
}

#[inline]
pub(super) fn metadata_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'a>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.metadata.next {
        None => Ok(tx_node.metadata.data),
        Some(df_cbor) => {
            let df_cid = df_cbor.to_cid()?;
            copy_dataframe_into(block, &df_cid, buf, None)
        }
    }
}

pub(super) fn extend_with_loaded_addresses(dest: &mut Vec<[u8; 32]>, addrs: &[Vec<u8>]) {
    for addr in addrs {
        if addr.len() == 32 {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(addr);
            dest.push(bytes);
        }
    }
}

pub(super) fn decode_transaction_meta(
    bytes: &[u8],
    scratch: &mut Vec<u8>,
) -> Option<TransactionStatusMeta> {
    if bytes.is_empty() {
        return None;
    }

    if is_zstd(bytes) {
        scratch.clear();
        let mut decoder = zstd::Decoder::new(bytes).ok()?;
        if decoder.read_to_end(scratch).is_err() {
            return None;
        }
        TransactionStatusMeta::decode(scratch.as_slice()).ok()
    } else {
        TransactionStatusMeta::decode(bytes).ok()
    }
}

#[inline]
pub(super) fn is_zstd(buf: &[u8]) -> bool {
    buf.get(0..4)
        .map(|m| m == [0x28, 0xB5, 0x2F, 0xFD])
        .unwrap_or(false)
}
