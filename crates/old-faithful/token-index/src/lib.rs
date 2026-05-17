#![allow(clippy::too_many_arguments)]

mod format;
mod piggy;
mod piggy_analysis;
mod piggy_dump;
mod piggy_meta;
mod scan;

use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

pub use format::{
    INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED, IndexMeta, InstructionRecord, NO_STACK_HEIGHT,
    NO_TX_INDEX, OUTER_INSTRUCTION_SENTINEL, PROGRAM_MASK_TOKEN, PROGRAM_MASK_TOKEN_2022,
    ProgramKind, TX_FLAG_HAS_ERROR, TX_FLAG_VERSIONED, TX_RECORD_BYTES, TransactionRecord,
    token_2022_program_id, token_program_id,
};
pub use piggy::{PiggyScanConfig, PiggyScanSummary, scan_piggy};
pub use piggy_analysis::{
    DEFAULT_MAX_FLOWS_PER_BURN, PiggyAnalyzeConfig, PiggyAnalyzeSummary, analyze_piggy_flows,
};
pub use piggy_dump::{PiggyDumpConfig, PiggyDumpSummary, dump_piggy};

#[derive(Debug, Clone)]
pub struct BatchBuildResult {
    pub epoch: u64,
    pub car_path: PathBuf,
    pub index_dir: PathBuf,
    pub matched_txs: usize,
    pub matched_instructions: usize,
    pub reused_existing: bool,
}

pub fn build_index(car_path: &Path, out_dir: &Path) -> Result<IndexMeta> {
    fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;
    eprintln!(
        "of-token-index: building {} -> {}",
        car_path.display(),
        out_dir.display()
    );

    let meta = scan::build_index(car_path, out_dir, is_zstd_path(car_path))?;
    fs::write(
        out_dir.join("meta.json"),
        serde_json::to_vec_pretty(&meta).context("serialize meta.json")?,
    )
    .context("write meta.json")?;
    Ok(meta)
}

pub fn build_indexes_in_dir(
    car_dir: &Path,
    out_dir: &Path,
    resume: bool,
) -> Result<Vec<BatchBuildResult>> {
    fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let epoch_cars = discover_epoch_cars(car_dir)?;
    eprintln!(
        "of-token-index: discovered {} epoch car files in {}",
        epoch_cars.len(),
        car_dir.display()
    );

    let total = epoch_cars.len();
    let mut results = Vec::with_capacity(total);
    for (index, epoch_car) in epoch_cars.into_iter().enumerate() {
        let index_dir = out_dir.join(format!("epoch-{}", epoch_car.epoch));
        eprintln!(
            "of-token-index: [{}/{}] epoch {} from {}",
            index + 1,
            total,
            epoch_car.epoch,
            epoch_car.path.display()
        );

        if resume && let Some(meta) = reusable_index_meta(&index_dir, &epoch_car.path)? {
            eprintln!(
                "of-token-index: epoch {} already indexed at {}, skipping",
                epoch_car.epoch,
                index_dir.display()
            );
            results.push(BatchBuildResult {
                epoch: epoch_car.epoch,
                car_path: epoch_car.path,
                index_dir,
                matched_txs: meta.matched_txs,
                matched_instructions: meta.matched_instructions,
                reused_existing: true,
            });
            continue;
        }

        let meta = build_index(&epoch_car.path, &index_dir)?;
        eprintln!(
            "of-token-index: epoch {} complete (matched {} txs, {} instructions)",
            epoch_car.epoch, meta.matched_txs, meta.matched_instructions
        );
        results.push(BatchBuildResult {
            epoch: epoch_car.epoch,
            car_path: epoch_car.path,
            index_dir,
            matched_txs: meta.matched_txs,
            matched_instructions: meta.matched_instructions,
            reused_existing: false,
        });
    }

    Ok(results)
}

pub fn read_index_meta(index_dir: &Path) -> Result<IndexMeta> {
    let bytes = fs::read(index_dir.join("meta.json")).context("read meta.json")?;
    serde_json::from_slice(&bytes).context("parse meta.json")
}

fn reusable_index_meta(index_dir: &Path, car_path: &Path) -> Result<Option<IndexMeta>> {
    if !index_dir.exists() {
        return Ok(None);
    }

    let meta = match read_index_meta(index_dir) {
        Ok(meta) => meta,
        Err(err) => {
            eprintln!(
                "of-token-index: existing index at {} is incomplete or unreadable ({}), rebuilding",
                index_dir.display(),
                err
            );
            return Ok(None);
        }
    };

    if meta.car_path != car_path.display().to_string() {
        eprintln!(
            "of-token-index: existing index at {} was built from {}, expected {}, rebuilding",
            index_dir.display(),
            meta.car_path,
            car_path.display()
        );
        return Ok(None);
    }
    if meta.transaction_record_bytes != TX_RECORD_BYTES {
        eprintln!(
            "of-token-index: existing index at {} uses {}-byte tx records, expected {}, rebuilding",
            index_dir.display(),
            meta.transaction_record_bytes,
            TX_RECORD_BYTES
        );
        return Ok(None);
    }

    for (filename, expected_len) in [
        ("transactions.bin", meta.transactions_file_bytes),
        ("instructions.bin", meta.instructions_file_bytes),
    ] {
        let path = index_dir.join(filename);
        if !path.is_file() {
            eprintln!(
                "of-token-index: existing index at {} is missing {}, rebuilding",
                index_dir.display(),
                path.display()
            );
            return Ok(None);
        }

        let actual_len = fs::metadata(&path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        if actual_len != expected_len {
            eprintln!(
                "of-token-index: existing index at {} has {}-byte {}, expected {}, rebuilding",
                index_dir.display(),
                actual_len,
                filename,
                expected_len
            );
            return Ok(None);
        }
    }

    if meta.transactions_file_bytes % TX_RECORD_BYTES as u64 != 0 {
        eprintln!(
            "of-token-index: existing index at {} has misaligned transactions.bin, rebuilding",
            index_dir.display()
        );
        return Ok(None);
    }

    Ok(Some(meta))
}

#[derive(Debug, Clone)]
struct EpochCar {
    epoch: u64,
    path: PathBuf,
    compressed: bool,
}

fn discover_epoch_cars(car_dir: &Path) -> Result<Vec<EpochCar>> {
    let mut cars_by_epoch: BTreeMap<u64, EpochCar> = BTreeMap::new();

    for entry in fs::read_dir(car_dir).with_context(|| format!("read {}", car_dir.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", car_dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        let Some(epoch) = epoch_from_car_path(&path) else {
            continue;
        };

        let candidate = EpochCar {
            epoch,
            path,
            compressed: is_zstd_path(entry.path().as_path()),
        };

        match cars_by_epoch.get(&epoch) {
            Some(existing) if existing.compressed && !candidate.compressed => {}
            _ => {
                cars_by_epoch.insert(epoch, candidate);
            }
        }
    }

    if cars_by_epoch.is_empty() {
        bail!(
            "no epoch car files found in {} (expected names like epoch-0.car or epoch-0.car.zst)",
            car_dir.display()
        );
    }

    Ok(cars_by_epoch.into_values().collect())
}

fn epoch_from_car_path(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    let rest = filename.strip_prefix("epoch-")?;
    let epoch = rest
        .strip_suffix(".car")
        .or_else(|| rest.strip_suffix(".car.zst"))?;
    epoch.parse().ok()
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "zst" | "zstd"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        TX_RECORD_BYTES, build_index, build_indexes_in_dir, discover_epoch_cars,
        epoch_from_car_path, reusable_index_meta,
    };
    use std::{
        fs,
        path::{Path, PathBuf},
    };
    use tempfile::tempdir;

    #[test]
    fn parses_epoch_from_supported_car_names() {
        assert_eq!(epoch_from_car_path(Path::new("/tmp/epoch-0.car")), Some(0));
        assert_eq!(
            epoch_from_car_path(Path::new("/tmp/epoch-12.car.zst")),
            Some(12)
        );
        assert_eq!(
            epoch_from_car_path(Path::new("/tmp/epoch-157-biggest.car")),
            None
        );
    }

    #[test]
    fn discovers_epoch_cars_preferring_zstd() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("epoch-0.car"), []).expect("write epoch-0.car");
        fs::write(dir.path().join("epoch-0.car.zst"), []).expect("write epoch-0.car.zst");
        fs::write(dir.path().join("epoch-1.car"), []).expect("write epoch-1.car");

        let discovered = discover_epoch_cars(dir.path()).expect("discover epoch cars");
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].epoch, 0);
        assert!(discovered[0].compressed);
        assert_eq!(
            discovered[0]
                .path
                .file_name()
                .and_then(|name| name.to_str()),
            Some("epoch-0.car.zst")
        );
    }

    #[test]
    fn validates_complete_existing_index_for_resume() {
        let dir = tempdir().expect("tempdir");
        let car_path = fixture_car_path();
        let index_dir = dir.path().join("epoch-157");
        let meta = build_index(&car_path, &index_dir).expect("build fixture index");
        assert!(meta.matched_txs > 0);
        assert!(meta.matched_instructions > 0);
        assert_eq!(meta.transactions_file_bytes % TX_RECORD_BYTES as u64, 0);

        let reusable = reusable_index_meta(&index_dir, &car_path)
            .expect("validate reusable index")
            .expect("existing index should be reusable");
        assert_eq!(reusable.matched_txs, meta.matched_txs);
    }

    #[test]
    fn resume_skips_completed_epochs_and_builds_remaining_ones() {
        let temp = tempdir().expect("tempdir");
        let car_dir = temp.path().join("cars");
        let out_dir = temp.path().join("indexes");
        fs::create_dir_all(&car_dir).expect("create car dir");

        let fixture = fixture_car_path();
        fs::copy(&fixture, car_dir.join("epoch-0.car")).expect("copy epoch 0 fixture");
        fs::copy(&fixture, car_dir.join("epoch-1.car")).expect("copy epoch 1 fixture");

        build_index(&car_dir.join("epoch-0.car"), &out_dir.join("epoch-0"))
            .expect("prebuild epoch 0");

        let results = build_indexes_in_dir(&car_dir, &out_dir, true).expect("resume build-all");
        assert_eq!(results.len(), 2);
        assert!(results[0].reused_existing);
        assert!(!results[1].reused_existing);
        assert!(out_dir.join("epoch-1/meta.json").is_file());
    }

    fn fixture_car_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../car-reader/benches/fixtures/epoch-157-biggest.car")
    }
}
