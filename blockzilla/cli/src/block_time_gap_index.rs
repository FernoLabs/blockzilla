use anyhow::{Context, Result, ensure};
use blockzilla_archive_v2::{
    BLOCK_TIME_GAP_FILE, BLOCK_TIME_GAP_MISSING_TIME, BlockTimeGapRow, read_block_time_gap_sidecar,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Cursor, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const BLOCK_TIME_GAP_INDEX_SCHEMA_VERSION: u32 = 1;
const BLOCK_TIME_GAP_INDEX_HASH_DOMAIN: &[u8] = b"blockzilla:block-time-gap-index:v1";
const SECONDS_PER_DAY: u64 = 86_400;

#[derive(Debug, Clone)]
pub(crate) struct BuildBlockTimeGapIndexConfig<'a> {
    pub archive_root: &'a Path,
    pub output: &'a Path,
    pub start_epoch: u64,
    pub end_epoch: u64,
    pub minimum_interruption_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildBlockTimeGapIndexSummary {
    pub output: PathBuf,
    pub indexed_epochs: u64,
    pub missing_epochs: u64,
    pub interruptions: u64,
    pub interruption_days: u64,
    pub source_sidecar_bytes: u64,
}

#[derive(Debug, Serialize)]
struct BlockTimeGapIndex {
    schema_version: u32,
    generated_unix_secs: u64,
    minimum_interruption_secs: u64,
    source_sha256: String,
    coverage: BlockTimeGapIndexCoverage,
    interruptions: Vec<BlockTimeInterruption>,
    days: Vec<BlockTimeInterruptionDay>,
}

#[derive(Debug, Serialize)]
struct BlockTimeGapIndexCoverage {
    start_epoch: u64,
    end_epoch: u64,
    expected_epoch_count: u64,
    indexed_epoch_count: u64,
    missing_epochs: Vec<u64>,
    indexed_boundary_count: u64,
    source_sidecar_bytes: u64,
    source_gap_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BlockTimeInterruptionKind {
    IntraEpoch,
    EpochBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BlockTimeInterruption {
    id: u64,
    kind: BlockTimeInterruptionKind,
    previous_epoch: u64,
    next_epoch: u64,
    previous_slot: u64,
    next_slot: u64,
    previous_block_time: i64,
    next_block_time: i64,
    elapsed_secs: u64,
    missing_slots: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BlockTimeInterruptionDigest {
    id: u64,
    kind: BlockTimeInterruptionKind,
    previous_slot: u64,
    next_slot: u64,
    previous_block_time: i64,
    next_block_time: i64,
    elapsed_secs: u64,
    missing_slots: u64,
}

impl From<&BlockTimeInterruption> for BlockTimeInterruptionDigest {
    fn from(value: &BlockTimeInterruption) -> Self {
        Self {
            id: value.id,
            kind: value.kind,
            previous_slot: value.previous_slot,
            next_slot: value.next_slot,
            previous_block_time: value.previous_block_time,
            next_block_time: value.next_block_time,
            elapsed_secs: value.elapsed_secs,
            missing_slots: value.missing_slots,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BlockTimeInterruptionDay {
    day_start_unix_secs: u64,
    interruption_count: u64,
    boundary_interruption_count: u64,
    interruption_seconds: u64,
    longest_interruption_secs: u64,
    largest_missing_slots: u64,
    longest_interruption: BlockTimeInterruptionDigest,
}

#[derive(Debug)]
struct BlockTimeInterruptionDayAccumulator {
    interruption_count: u64,
    boundary_interruption_count: u64,
    interruption_seconds: u64,
    largest_missing_slots: u64,
    longest_interruption: BlockTimeInterruptionDigest,
}

#[derive(Debug, Clone, Copy)]
struct EpochEndpoints {
    epoch: u64,
    first_slot: u64,
    first_block_time: i64,
    last_slot: u64,
    last_block_time: i64,
}

pub(crate) fn build_block_time_gap_index(
    config: BuildBlockTimeGapIndexConfig<'_>,
) -> Result<BuildBlockTimeGapIndexSummary> {
    ensure!(
        config.end_epoch >= config.start_epoch,
        "block-time gap index end epoch precedes start epoch"
    );
    ensure!(
        config.minimum_interruption_secs > 1,
        "minimum interruption must exceed the sidecar's one-second timestamp threshold"
    );

    let expected_epoch_count = config
        .end_epoch
        .checked_sub(config.start_epoch)
        .and_then(|span| span.checked_add(1))
        .context("block-time gap index epoch range overflows u64")?;
    let mut source_hasher = Sha256::new();
    source_hasher.update(BLOCK_TIME_GAP_INDEX_HASH_DOMAIN);
    source_hasher.update(config.start_epoch.to_le_bytes());
    source_hasher.update(config.end_epoch.to_le_bytes());
    source_hasher.update(config.minimum_interruption_secs.to_le_bytes());

    let mut endpoints = BTreeMap::new();
    let mut missing_epochs = Vec::new();
    let mut interruptions = Vec::new();
    let mut source_sidecar_bytes = 0u64;
    let mut source_gap_rows = 0u64;

    for epoch in config.start_epoch..=config.end_epoch {
        let path = config
            .archive_root
            .join(format!("epoch-{epoch}"))
            .join(BLOCK_TIME_GAP_FILE);
        if !path.is_file() {
            missing_epochs.push(epoch);
            continue;
        }

        let bytes = fs::read(&path)
            .with_context(|| format!("read block-time gap sidecar {}", path.display()))?;
        source_sidecar_bytes = source_sidecar_bytes
            .checked_add(bytes.len() as u64)
            .context("aggregate sidecar byte count overflows u64")?;
        source_hasher.update(epoch.to_le_bytes());
        source_hasher.update((bytes.len() as u64).to_le_bytes());
        source_hasher.update(&bytes);

        let sidecar = read_block_time_gap_sidecar(Cursor::new(&bytes))
            .with_context(|| format!("validate block-time gap sidecar {}", path.display()))?;
        ensure!(
            sidecar.header.epoch == epoch,
            "block-time gap sidecar {} declares epoch {}, expected {epoch}",
            path.display(),
            sidecar.header.epoch
        );
        source_gap_rows = source_gap_rows
            .checked_add(sidecar.header.gap_count)
            .context("aggregate source gap row count overflows u64")?;

        if sidecar.header.block_count > 0 {
            endpoints.insert(
                epoch,
                EpochEndpoints {
                    epoch,
                    first_slot: sidecar.header.first_slot,
                    first_block_time: sidecar.header.first_block_time,
                    last_slot: sidecar.header.last_slot,
                    last_block_time: sidecar.header.last_block_time,
                },
            );
        }
        for row in sidecar.rows {
            if let Some(interruption) = interruption_from_row(
                row,
                epoch,
                epoch,
                BlockTimeInterruptionKind::IntraEpoch,
                config.minimum_interruption_secs,
            )? {
                interruptions.push(interruption);
            }
        }
    }

    let mut indexed_boundary_count = 0u64;
    for epoch in config.start_epoch..config.end_epoch {
        let Some(previous) = endpoints.get(&epoch) else {
            continue;
        };
        let Some(next) = endpoints.get(&(epoch + 1)) else {
            continue;
        };
        indexed_boundary_count += 1;
        let row = BlockTimeGapRow {
            previous_slot: previous.last_slot,
            next_slot: next.first_slot,
            previous_block_time: previous.last_block_time,
            next_block_time: next.first_block_time,
            flags: boundary_flags(previous.last_block_time, next.first_block_time),
            reserved: 0,
        };
        if let Some(interruption) = interruption_from_row(
            row,
            previous.epoch,
            next.epoch,
            BlockTimeInterruptionKind::EpochBoundary,
            config.minimum_interruption_secs,
        )? {
            interruptions.push(interruption);
        }
    }

    interruptions.sort_by_key(|entry| {
        (
            entry.previous_block_time,
            entry.next_block_time,
            entry.previous_slot,
            entry.next_slot,
        )
    });
    for (index, interruption) in interruptions.iter_mut().enumerate() {
        interruption.id = index as u64;
    }
    let days = summarize_days(&interruptions)?;
    let indexed_epoch_count = expected_epoch_count
        .checked_sub(missing_epochs.len() as u64)
        .context("missing epoch count exceeds expected epoch count")?;
    let index = BlockTimeGapIndex {
        schema_version: BLOCK_TIME_GAP_INDEX_SCHEMA_VERSION,
        generated_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock precedes Unix epoch")?
            .as_secs(),
        minimum_interruption_secs: config.minimum_interruption_secs,
        source_sha256: hex_digest(source_hasher.finalize().as_slice()),
        coverage: BlockTimeGapIndexCoverage {
            start_epoch: config.start_epoch,
            end_epoch: config.end_epoch,
            expected_epoch_count,
            indexed_epoch_count,
            missing_epochs,
            indexed_boundary_count,
            source_sidecar_bytes,
            source_gap_rows,
        },
        interruptions,
        days,
    };
    write_json_atomic(config.output, &index)?;

    Ok(BuildBlockTimeGapIndexSummary {
        output: config.output.to_path_buf(),
        indexed_epochs: index.coverage.indexed_epoch_count,
        missing_epochs: index.coverage.missing_epochs.len() as u64,
        interruptions: index.interruptions.len() as u64,
        interruption_days: index.days.len() as u64,
        source_sidecar_bytes,
    })
}

fn interruption_from_row(
    row: BlockTimeGapRow,
    previous_epoch: u64,
    next_epoch: u64,
    kind: BlockTimeInterruptionKind,
    minimum_interruption_secs: u64,
) -> Result<Option<BlockTimeInterruption>> {
    if row.flags != 0
        || row.previous_block_time == BLOCK_TIME_GAP_MISSING_TIME
        || row.next_block_time == BLOCK_TIME_GAP_MISSING_TIME
        || row.previous_block_time < 0
    {
        return Ok(None);
    }
    let Some(elapsed_secs) = row.elapsed_seconds() else {
        return Ok(None);
    };
    if elapsed_secs < minimum_interruption_secs {
        return Ok(None);
    }
    let missing_slots = row
        .missing_slots()
        .context("block-time interruption does not advance by at least one slot")?;
    Ok(Some(BlockTimeInterruption {
        id: 0,
        kind,
        previous_epoch,
        next_epoch,
        previous_slot: row.previous_slot,
        next_slot: row.next_slot,
        previous_block_time: row.previous_block_time,
        next_block_time: row.next_block_time,
        elapsed_secs,
        missing_slots,
    }))
}

fn boundary_flags(previous_block_time: i64, next_block_time: i64) -> u32 {
    let mut flags = 0;
    if previous_block_time == BLOCK_TIME_GAP_MISSING_TIME {
        flags |= blockzilla_archive_v2::BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING;
    }
    if next_block_time == BLOCK_TIME_GAP_MISSING_TIME {
        flags |= blockzilla_archive_v2::BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING;
    }
    if flags == 0 && next_block_time < previous_block_time {
        flags |= blockzilla_archive_v2::BLOCK_TIME_GAP_FLAG_TIME_DECREASING;
    }
    flags
}

fn summarize_days(
    interruptions: &[BlockTimeInterruption],
) -> Result<Vec<BlockTimeInterruptionDay>> {
    let mut days: BTreeMap<u64, BlockTimeInterruptionDayAccumulator> = BTreeMap::new();
    for interruption in interruptions {
        let start = u64::try_from(interruption.previous_block_time)
            .context("block-time interruption starts before Unix epoch")?;
        let end = u64::try_from(interruption.next_block_time)
            .context("block-time interruption ends before Unix epoch")?;
        ensure!(
            end > start,
            "block-time interruption has no positive duration"
        );
        let mut day_start = start / SECONDS_PER_DAY * SECONDS_PER_DAY;
        while day_start < end {
            let day_end = day_start
                .checked_add(SECONDS_PER_DAY)
                .context("UTC day boundary overflows u64")?;
            let overlap_start = start.max(day_start);
            let overlap_end = end.min(day_end);
            if overlap_end > overlap_start {
                let digest = BlockTimeInterruptionDigest::from(interruption);
                let day =
                    days.entry(day_start)
                        .or_insert_with(|| BlockTimeInterruptionDayAccumulator {
                            interruption_count: 0,
                            boundary_interruption_count: 0,
                            interruption_seconds: 0,
                            largest_missing_slots: 0,
                            longest_interruption: digest.clone(),
                        });
                day.interruption_count = day
                    .interruption_count
                    .checked_add(1)
                    .expect("one UTC day cannot contain u64::MAX interruptions");
                if interruption.kind == BlockTimeInterruptionKind::EpochBoundary {
                    day.boundary_interruption_count = day
                        .boundary_interruption_count
                        .checked_add(1)
                        .expect("one UTC day cannot contain u64::MAX boundary interruptions");
                }
                day.interruption_seconds = day
                    .interruption_seconds
                    .checked_add(overlap_end - overlap_start)
                    .context("daily interruption duration overflows u64")?;
                day.largest_missing_slots =
                    day.largest_missing_slots.max(interruption.missing_slots);
                if interruption.elapsed_secs > day.longest_interruption.elapsed_secs {
                    day.longest_interruption = digest;
                }
            }
            day_start = day_end;
        }
    }

    Ok(days
        .into_iter()
        .map(|(day_start_unix_secs, day)| BlockTimeInterruptionDay {
            day_start_unix_secs,
            interruption_count: day.interruption_count,
            boundary_interruption_count: day.boundary_interruption_count,
            interruption_seconds: day.interruption_seconds,
            longest_interruption_secs: day.longest_interruption.elapsed_secs,
            largest_missing_slots: day.largest_missing_slots,
            longest_interruption: day.longest_interruption,
        })
        .collect())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create block-time index directory {}", parent.display()))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("block-time index output needs a UTF-8 file name")?;
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("create temporary block-time index {}", temporary.display()))?;
    let write_result = (|| -> Result<()> {
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value)
            .context("serialize block-time gap aggregate index")?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "publish block-time gap index {} -> {}",
                temporary.display(),
                path.display()
            )
        })?;
        File::open(parent)
            .with_context(|| format!("open index parent directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("sync index parent directory {}", parent.display()))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_archive_v2::{
        BLOCK_TIME_GAP_HEADER_LEN, BLOCK_TIME_GAP_MAGIC, BLOCK_TIME_GAP_ROW_LEN,
        BLOCK_TIME_GAP_TIME_THRESHOLD_SECS, BLOCK_TIME_GAP_VERSION, BlockTimeGapHeader,
        BlockTimeGapSidecar, BlockTimeGapSourceKind, write_block_time_gap_sidecar,
    };

    fn temporary_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-gap-index-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn write_sidecar(
        root: &Path,
        epoch: u64,
        first_time: i64,
        last_time: i64,
        rows: Vec<BlockTimeGapRow>,
    ) {
        let epoch_start = epoch * crate::SLOTS_PER_EPOCH;
        let first_slot = epoch_start + 10;
        let last_slot = epoch_start + 20;
        let missing_slot_count = last_slot - first_slot + 1 - 2;
        let sidecar = BlockTimeGapSidecar {
            header: BlockTimeGapHeader {
                magic: BLOCK_TIME_GAP_MAGIC,
                version: BLOCK_TIME_GAP_VERSION,
                header_len: BLOCK_TIME_GAP_HEADER_LEN as u16,
                row_len: BLOCK_TIME_GAP_ROW_LEN as u16,
                flags: 0,
                epoch,
                slots_per_epoch: crate::SLOTS_PER_EPOCH,
                source_kind: BlockTimeGapSourceKind::Car,
                time_gap_threshold_secs: BLOCK_TIME_GAP_TIME_THRESHOLD_SECS as u32,
                source_bytes: 100,
                source_sha256: [epoch as u8; 32],
                block_count: 2,
                gap_count: rows.len() as u64,
                missing_slot_count,
                first_slot,
                first_block_time: first_time,
                last_slot,
                last_block_time: last_time,
                timed_gap_count: rows.len() as u64,
                missing_time_gap_count: 0,
                decreasing_time_gap_count: 0,
            },
            rows,
        };
        let directory = root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&directory).unwrap();
        let mut bytes = Vec::new();
        write_block_time_gap_sidecar(&mut bytes, &sidecar).unwrap();
        fs::write(directory.join(BLOCK_TIME_GAP_FILE), bytes).unwrap();
    }

    #[test]
    fn aggregate_includes_intra_epoch_and_boundary_interruptions() {
        let root = temporary_root("aggregate");
        let output = root.join("public/index.json");
        let day = 1_700_006_400i64;
        write_sidecar(
            &root,
            10,
            day + 100,
            day + 1_000,
            vec![BlockTimeGapRow {
                previous_slot: 10 * crate::SLOTS_PER_EPOCH + 10,
                next_slot: 10 * crate::SLOTS_PER_EPOCH + 20,
                previous_block_time: day + 100,
                next_block_time: day + 1_000,
                flags: 0,
                reserved: 0,
            }],
        );
        write_sidecar(
            &root,
            11,
            day + 2_000,
            day + 2_100,
            vec![BlockTimeGapRow {
                previous_slot: 11 * crate::SLOTS_PER_EPOCH + 10,
                next_slot: 11 * crate::SLOTS_PER_EPOCH + 20,
                previous_block_time: day + 2_000,
                next_block_time: day + 2_100,
                flags: 0,
                reserved: 0,
            }],
        );

        let summary = build_block_time_gap_index(BuildBlockTimeGapIndexConfig {
            archive_root: &root,
            output: &output,
            start_epoch: 10,
            end_epoch: 12,
            minimum_interruption_secs: 300,
        })
        .unwrap();

        assert_eq!(summary.indexed_epochs, 2);
        assert_eq!(summary.missing_epochs, 1);
        assert_eq!(summary.interruptions, 2);
        let value: serde_json::Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(value["coverage"]["missing_epochs"], serde_json::json!([12]));
        assert_eq!(value["coverage"]["indexed_boundary_count"], 1);
        assert_eq!(value["interruptions"][0]["kind"], "intra_epoch");
        assert_eq!(value["interruptions"][1]["kind"], "epoch_boundary");
        assert_eq!(value["days"].as_array().unwrap().len(), 1);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn day_summary_splits_an_interruption_across_utc_midnight() {
        let interruption = BlockTimeInterruption {
            id: 7,
            kind: BlockTimeInterruptionKind::IntraEpoch,
            previous_epoch: 1,
            next_epoch: 1,
            previous_slot: 10,
            next_slot: 11,
            previous_block_time: (SECONDS_PER_DAY - 60) as i64,
            next_block_time: (SECONDS_PER_DAY + 120) as i64,
            elapsed_secs: 180,
            missing_slots: 0,
        };

        let days = summarize_days(&[interruption]).unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].day_start_unix_secs, 0);
        assert_eq!(days[0].interruption_seconds, 60);
        assert_eq!(days[1].day_start_unix_secs, SECONDS_PER_DAY);
        assert_eq!(days[1].interruption_seconds, 120);
        assert_eq!(days[1].longest_interruption.id, 7);
    }
}
