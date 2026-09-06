//! Bounded bridge from the durable raw-shred journal into a Blockzilla-owned spool.
//!
//! The source is read-only.  A cursor is written only after the target frame has been synced,
//! which makes replay safe across crashes and deliberately does not authorize source GC.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::{
    ContentDigest, SpoolJournalIdentity, SpoolLocation, SpoolOptions, SpoolWriter,
    read_spool_committed_snapshot_after,
};

const CURSOR_FILE: &str = "SHRED-BRIDGE-CURSOR.v1.json";

#[derive(Debug, Clone)]
pub struct ShredBridgeConfig {
    pub source_spool_root: PathBuf,
    pub identity: SpoolJournalIdentity,
    pub output_dir: PathBuf,
    pub durable_through_sequence: u64,
    pub max_record_bytes: u64,
    pub segment_target_bytes: u64,
    pub max_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShredBridgeCursor {
    schema_version: u32,
    source_location: SpoolLocation,
    source_sequence: u64,
    content_digest: ContentDigest,
    target_location: SpoolLocation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShredBridgeReport {
    pub output_dir: PathBuf,
    pub durable_through_sequence: u64,
    pub records_written: u64,
    pub target_records_after: u64,
    pub reached_durable_tail: bool,
    pub cursor_path: PathBuf,
}

pub fn bridge_shred_spool(config: ShredBridgeConfig) -> Result<ShredBridgeReport> {
    ensure!(
        config.max_record_bytes > 0,
        "max_record_bytes must be non-zero"
    );
    ensure!(config.max_records > 0, "max_records must be non-zero");
    ensure!(
        config.output_dir.is_absolute() && config.output_dir != Path::new("/"),
        "output_dir must be an absolute non-root directory"
    );
    ensure!(
        config.source_spool_root != config.output_dir,
        "source and target spool roots must differ"
    );
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create shred bridge output {}", config.output_dir.display()))?;

    let target_root = config.output_dir.join("spool");
    let mut target = SpoolWriter::open(
        &target_root,
        config.identity.clone(),
        SpoolOptions {
            segment_target_bytes: config.segment_target_bytes,
            max_record_bytes: config.max_record_bytes,
        },
    )?;
    let cursor_path = config.output_dir.join(CURSOR_FILE);
    let cursor = read_cursor(&cursor_path)?;
    if let Some(cursor) = &cursor {
        ensure!(
            cursor.source_sequence < config.durable_through_sequence.saturating_add(1),
            "bridge cursor is ahead of supplied durable sequence"
        );
        if let Some(last) = target.last_record() {
            ensure!(
                last.metadata().observation.sequence == cursor.source_sequence,
                "target spool tail does not match bridge cursor"
            );
        }
    }
    let after = cursor.as_ref().map(|c| c.source_location);
    let mut written = 0u64;
    let mut target_records = target
        .last_record()
        .map(|r| r.metadata().observation.sequence.saturating_add(1))
        .unwrap_or(0);
    let snapshot = read_spool_committed_snapshot_after(
        &config.source_spool_root,
        config.identity.clone(),
        config.max_record_bytes,
        after,
        config.durable_through_sequence,
        config.max_records,
        |record| {
            let projected = target.project_append(&record.metadata, &record.payload)?;
            let durable = target.append_and_sync(record.metadata.clone(), &record.payload)?;
            ensure!(
                durable.location() == projected.location,
                "target spool append differed from projection"
            );
            let next = ShredBridgeCursor {
                schema_version: 1,
                source_location: record.location,
                source_sequence: record.metadata.observation.sequence,
                content_digest: record.metadata.content_digest,
                target_location: durable.location(),
            };
            write_cursor(&cursor_path, &next)?;
            target_records = target_records.saturating_add(1);
            written = written.saturating_add(1);
            Ok(())
        },
    )?;
    Ok(ShredBridgeReport {
        output_dir: config.output_dir,
        durable_through_sequence: config.durable_through_sequence,
        records_written: written,
        target_records_after: target_records,
        reached_durable_tail: snapshot.reached_durable_tail,
        cursor_path,
    })
}

fn read_cursor(path: &Path) -> Result<Option<ShredBridgeCursor>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("read bridge cursor {}", path.display()))?;
    Ok(Some(
        serde_json::from_slice(&bytes).context("decode shred bridge cursor")?,
    ))
}

fn write_cursor(path: &Path, cursor: &ShredBridgeCursor) -> Result<()> {
    let temp = path.with_extension("tmp");
    let bytes = serde_json::to_vec(cursor).context("encode shred bridge cursor")?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
}
