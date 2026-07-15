use crate::{ProgressTracker, SLOTS_PER_EPOCH};
use anyhow::{Context, Result, anyhow};
use of_car_reader::{CarBlockGroup, CarBlockReader};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::info;

const RECEIPT_SCHEMA_VERSION: u32 = 1;
const VALIDATION_LEVEL: &str = "structural";
const ZSTD_LONG_WINDOW_LOG_MAX: u32 = 31;
const MAX_PREFLIGHT_RECEIPT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct CarPreflightConfig<'a> {
    pub input: &'a Path,
    pub epoch: u64,
    pub receipt: &'a Path,
    pub io_buffer_bytes: usize,
    pub progress_json: Option<&'a Path>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PohCoverage {
    pub records: u64,
    pub blocks_with_entries: u64,
    pub blocks_without_entries: u64,
    pub entries: u64,
    pub transaction_references: u64,
    /// A decimal string because a full epoch's PoH hash count can exceed the
    /// integer precision available to JSON consumers.
    pub num_hashes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ShreddingCoverage {
    pub records: u64,
    pub blocks_with_spans: u64,
    pub blocks_without_spans: u64,
    pub spans: u64,
}

/// Durable result of a bounded-memory CAR preflight.
///
/// `validation_level=structural` is deliberate. This pass consumes the full
/// CAR/zstd stream and validates block grouping without retaining transaction
/// payloads, but it does not recompute every CAR CID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CarPreflightReceipt {
    pub schema_version: u32,
    pub validation_level: String,
    pub structurally_valid: bool,
    pub clean_eof: bool,
    pub eligible_for_compaction: bool,
    pub epoch: u64,
    pub source_path: PathBuf,
    pub source_bytes: u64,
    pub source_modified_unix_secs: u64,
    pub source_modified_subsec_nanos: u32,
    pub compressed: bool,
    pub io_buffer_bytes: u64,
    pub decompressed_car_bytes: u64,
    pub blocks: u64,
    pub blocks_in_epoch: u64,
    pub present_slots: u64,
    pub duplicate_slots: u64,
    pub out_of_epoch_blocks: u64,
    pub non_monotonic_slots: u64,
    pub transactions: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub poh: PohCoverage,
    pub shredding: ShreddingCoverage,
    pub started_unix_secs: u64,
    pub completed_unix_secs: u64,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceFingerprint {
    bytes: u64,
    modified_unix_secs: u64,
    modified_subsec_nanos: u32,
}

#[derive(Debug, Default)]
struct ScanStats {
    decompressed_car_bytes: u64,
    blocks: u64,
    blocks_in_epoch: u64,
    present_slots: u64,
    duplicate_slots: u64,
    out_of_epoch_blocks: u64,
    non_monotonic_slots: u64,
    transactions: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    previous_slot: Option<u64>,
    poh_records: u64,
    poh_blocks_with_entries: u64,
    poh_blocks_without_entries: u64,
    poh_entries: u64,
    poh_transaction_references: u64,
    poh_num_hashes: u128,
    shredding_records: u64,
    shredding_blocks_with_spans: u64,
    shredding_blocks_without_spans: u64,
    shredding_spans: u64,
}

pub(crate) fn preflight_car(config: CarPreflightConfig<'_>) -> Result<CarPreflightReceipt> {
    anyhow::ensure!(
        config.io_buffer_bytes > 0,
        "CAR preflight I/O buffer must be positive"
    );
    anyhow::ensure!(
        config.input != config.receipt,
        "CAR preflight receipt must not replace its input"
    );

    // Keep this descriptor alive until the function returns. A stable sibling
    // lock serializes both receipt validation and the full CAR scan, so a
    // restarted/duplicate worker can reuse the first worker's durable result.
    let _receipt_lock = acquire_receipt_lock(config.receipt)?;
    let source_before = source_fingerprint(config.input)?;
    if let Some(receipt) = reusable_receipt(config.receipt, config.epoch, source_before) {
        info!(
            "Reusing CAR preflight receipt: epoch={} source={} receipt={}",
            receipt.epoch,
            config.input.display(),
            config.receipt.display()
        );
        return Ok(receipt);
    }
    let compressed = is_zstd_input(config.input)?;
    let started_unix_secs = unix_now();
    let started = Instant::now();
    let mut progress = ProgressTracker::new("CAR Preflight");
    if let Some(path) = config.progress_json {
        progress.progress_path = Some(path.to_path_buf());
    }

    let file = File::open(config.input)
        .with_context(|| format!("open CAR input {}", config.input.display()))?;
    let stats = if compressed {
        let decoder = zstd_decoder_with_long_window(file, config.io_buffer_bytes)
            .with_context(|| format!("open zstd CAR input {}", config.input.display()))?;
        scan_reader(decoder, config.epoch, config.io_buffer_bytes, &mut progress)?
    } else {
        scan_reader(file, config.epoch, config.io_buffer_bytes, &mut progress)?
    };
    let source_after = source_fingerprint(config.input)?;
    anyhow::ensure!(
        source_before == source_after,
        "CAR input {} changed during preflight: before={source_before:?} after={source_after:?}",
        config.input.display()
    );
    if !compressed {
        anyhow::ensure!(
            stats.decompressed_car_bytes == source_before.bytes,
            "raw CAR scan consumed {} bytes but source has {} bytes",
            stats.decompressed_car_bytes,
            source_before.bytes
        );
    }

    let receipt = CarPreflightReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        validation_level: VALIDATION_LEVEL.to_string(),
        structurally_valid: true,
        clean_eof: true,
        eligible_for_compaction: stats.blocks > 0
            && stats.blocks_in_epoch > 0
            && stats.duplicate_slots == 0
            && stats.out_of_epoch_blocks == 0
            && stats.non_monotonic_slots == 0,
        epoch: config.epoch,
        source_path: config.input.to_path_buf(),
        source_bytes: source_before.bytes,
        source_modified_unix_secs: source_before.modified_unix_secs,
        source_modified_subsec_nanos: source_before.modified_subsec_nanos,
        compressed,
        io_buffer_bytes: u64::try_from(config.io_buffer_bytes)
            .context("CAR preflight I/O buffer does not fit u64")?,
        decompressed_car_bytes: stats.decompressed_car_bytes,
        blocks: stats.blocks,
        blocks_in_epoch: stats.blocks_in_epoch,
        present_slots: stats.present_slots,
        duplicate_slots: stats.duplicate_slots,
        out_of_epoch_blocks: stats.out_of_epoch_blocks,
        non_monotonic_slots: stats.non_monotonic_slots,
        transactions: stats.transactions,
        first_slot: stats.first_slot,
        last_slot: stats.last_slot,
        poh: PohCoverage {
            records: stats.poh_records,
            blocks_with_entries: stats.poh_blocks_with_entries,
            blocks_without_entries: stats.poh_blocks_without_entries,
            entries: stats.poh_entries,
            transaction_references: stats.poh_transaction_references,
            num_hashes: stats.poh_num_hashes.to_string(),
        },
        shredding: ShreddingCoverage {
            records: stats.shredding_records,
            blocks_with_spans: stats.shredding_blocks_with_spans,
            blocks_without_spans: stats.shredding_blocks_without_spans,
            spans: stats.shredding_spans,
        },
        started_unix_secs,
        completed_unix_secs: unix_now(),
        elapsed_secs: started.elapsed().as_secs_f64(),
    };

    write_receipt_atomic(config.receipt, &receipt)?;
    progress.final_report();
    info!(
        "CAR preflight complete: epoch={} blocks={} present_slots={} duplicate_slots={} out_of_epoch={} poh_entries={} shredding_spans={} source={} receipt={}",
        receipt.epoch,
        receipt.blocks,
        receipt.present_slots,
        receipt.duplicate_slots,
        receipt.out_of_epoch_blocks,
        receipt.poh.entries,
        receipt.shredding.spans,
        receipt.source_path.display(),
        config.receipt.display()
    );
    Ok(receipt)
}

fn scan_reader<R: Read>(
    reader: R,
    epoch: u64,
    io_buffer_bytes: usize,
    progress: &mut ProgressTracker,
) -> Result<ScanStats> {
    let mut reader = CarBlockReader::with_capacity(reader, io_buffer_bytes);
    reader.skip_header().context("read CAR header")?;
    let mut group = CarBlockGroup::without_rewards_and_transaction_payloads();
    let mut seen_slots = vec![0u64; (SLOTS_PER_EPOCH as usize).div_ceil(u64::BITS as usize)];
    let mut stats = ScanStats::default();

    loop {
        let has_block = reader
            .read_until_block_into(&mut group)
            .with_context(|| format!("read CAR block group after {} blocks", stats.blocks))?;
        if !has_block {
            let (pending_transactions, _) = group.get_len();
            anyhow::ensure!(
                pending_transactions == 0 && group.entry_count() == 0,
                "CAR ended with an unterminated block group: transactions={} entries={}",
                pending_transactions,
                group.entry_count()
            );
            break;
        }

        let slot = group
            .slot
            .ok_or_else(|| anyhow!("CAR block group {} has no slot", stats.blocks))?;
        anyhow::ensure!(
            group.poh_num_hashes.len() == group.poh_hashes.len()
                && group.poh_hashes.len() == group.entry_tx_counts.len(),
            "slot {slot} has inconsistent PoH vectors: num_hashes={} hashes={} tx_counts={}",
            group.poh_num_hashes.len(),
            group.poh_hashes.len(),
            group.entry_tx_counts.len()
        );

        let (transaction_count, transaction_payload_bytes) = group.get_len();
        anyhow::ensure!(
            transaction_payload_bytes == 0,
            "CAR preflight unexpectedly retained {transaction_payload_bytes} transaction bytes"
        );
        add_u64(
            &mut stats.transactions,
            usize_to_u64(transaction_count, "transaction count")?,
            "transaction count",
        )?;
        add_u64(&mut stats.blocks, 1, "block count")?;
        if stats.first_slot.is_none() {
            stats.first_slot = Some(slot);
        }
        if stats.previous_slot.is_some_and(|previous| slot <= previous) {
            add_u64(
                &mut stats.non_monotonic_slots,
                1,
                "non-monotonic slot count",
            )?;
        }
        stats.previous_slot = Some(slot);
        stats.last_slot = Some(slot);

        if slot / SLOTS_PER_EPOCH == epoch {
            add_u64(&mut stats.blocks_in_epoch, 1, "in-epoch block count")?;
            let slot_index = usize::try_from(slot % SLOTS_PER_EPOCH)
                .context("slot-in-epoch does not fit usize")?;
            let word = slot_index / u64::BITS as usize;
            let mask = 1u64 << (slot_index % u64::BITS as usize);
            if seen_slots[word] & mask == 0 {
                seen_slots[word] |= mask;
                add_u64(&mut stats.present_slots, 1, "present slot count")?;
            } else {
                add_u64(&mut stats.duplicate_slots, 1, "duplicate slot count")?;
            }
        } else {
            add_u64(
                &mut stats.out_of_epoch_blocks,
                1,
                "out-of-epoch block count",
            )?;
        }

        add_u64(&mut stats.poh_records, 1, "PoH record count")?;
        let poh_entries = usize_to_u64(group.poh_num_hashes.len(), "PoH entry count")?;
        add_u64(&mut stats.poh_entries, poh_entries, "PoH entry count")?;
        if poh_entries == 0 {
            add_u64(
                &mut stats.poh_blocks_without_entries,
                1,
                "empty PoH block count",
            )?;
        } else {
            add_u64(
                &mut stats.poh_blocks_with_entries,
                1,
                "non-empty PoH block count",
            )?;
        }
        for num_hashes in &group.poh_num_hashes {
            stats.poh_num_hashes = stats
                .poh_num_hashes
                .checked_add(u128::from(*num_hashes))
                .context("PoH num_hashes total overflow")?;
        }
        for transaction_references in &group.entry_tx_counts {
            add_u64(
                &mut stats.poh_transaction_references,
                u64::from(*transaction_references),
                "PoH transaction reference count",
            )?;
        }

        add_u64(&mut stats.shredding_records, 1, "shredding record count")?;
        let shredding_spans = usize_to_u64(group.shredding.len(), "shredding span count")?;
        add_u64(
            &mut stats.shredding_spans,
            shredding_spans,
            "shredding span count",
        )?;
        if shredding_spans == 0 {
            add_u64(
                &mut stats.shredding_blocks_without_spans,
                1,
                "empty shredding block count",
            )?;
        } else {
            add_u64(
                &mut stats.shredding_blocks_with_spans,
                1,
                "non-empty shredding block count",
            )?;
        }

        progress.update_slot(slot);
        progress.update(1, transaction_count as u64);
    }

    stats.decompressed_car_bytes = reader.offset;
    Ok(stats)
}

fn zstd_decoder_with_long_window<R: Read>(
    reader: R,
    io_buffer_bytes: usize,
) -> Result<zstd::Decoder<'static, BufReader<R>>> {
    let context = Box::leak(Box::new(zstd::zstd_safe::DCtx::create()));
    context
        .set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(
            ZSTD_LONG_WINDOW_LOG_MAX,
        ))
        .map_err(|code| {
            anyhow!(
                "set zstd windowLogMax={ZSTD_LONG_WINDOW_LOG_MAX}: {}",
                zstd::zstd_safe::get_error_name(code)
            )
        })?;
    let reader = BufReader::with_capacity(io_buffer_bytes, reader);
    Ok(zstd::Decoder::with_context(reader, context))
}

fn source_fingerprint(path: &Path) -> Result<SourceFingerprint> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file(),
        "CAR input is not a file: {}",
        path.display()
    );
    let modified = metadata
        .modified()
        .with_context(|| format!("read mtime for {}", path.display()))?
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("mtime predates Unix epoch for {}", path.display()))?;
    Ok(SourceFingerprint {
        bytes: metadata.len(),
        modified_unix_secs: modified.as_secs(),
        modified_subsec_nanos: modified.subsec_nanos(),
    })
}

fn reusable_receipt(
    path: &Path,
    epoch: u64,
    source: SourceFingerprint,
) -> Option<CarPreflightReceipt> {
    let file = File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if metadata.len() == 0 || metadata.len() > MAX_PREFLIGHT_RECEIPT_BYTES {
        return None;
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).ok()?);
    file.take(MAX_PREFLIGHT_RECEIPT_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_PREFLIGHT_RECEIPT_BYTES {
        return None;
    }
    let receipt: CarPreflightReceipt = serde_json::from_slice(&bytes).ok()?;
    (receipt.schema_version == RECEIPT_SCHEMA_VERSION
        && receipt.validation_level == VALIDATION_LEVEL
        && receipt.structurally_valid
        && receipt.clean_eof
        && receipt.eligible_for_compaction
        && receipt.epoch == epoch
        && receipt.source_bytes == source.bytes
        && receipt.source_modified_unix_secs == source.modified_unix_secs
        && receipt.source_modified_subsec_nanos == source.modified_subsec_nanos)
        .then_some(receipt)
}

fn acquire_receipt_lock(receipt: &Path) -> Result<File> {
    let parent = receipt
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create receipt lock directory {}", parent.display()))?;
    let lock_path = receipt_lock_path(receipt);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open CAR preflight receipt lock {}", lock_path.display()))?;
    lock_file_blocking(&file)
        .with_context(|| format!("lock CAR preflight receipt {}", lock_path.display()))?;
    Ok(file)
}

fn receipt_lock_path(receipt: &Path) -> PathBuf {
    let mut name = receipt
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("car-preflight.json"))
        .to_os_string();
    name.push(".lock");
    receipt.with_file_name(name)
}

#[cfg(unix)]
fn lock_file_blocking(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    loop {
        // SAFETY: `file` owns a valid descriptor throughout this blocking call.
        // flock does not dereference process memory.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(unix))]
fn lock_file_blocking(_file: &File) -> std::io::Result<()> {
    Ok(())
}

fn write_receipt_atomic(path: &Path, receipt: &CarPreflightReceipt) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create receipt directory {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("car-preflight.json");
    let temp_path = parent.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos())
    ));

    let result = (|| -> Result<()> {
        let mut bytes = serde_json::to_vec_pretty(receipt).context("serialize CAR receipt")?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| format!("create temporary receipt {}", temp_path.display()))?;
        file.write_all(&bytes)
            .with_context(|| format!("write temporary receipt {}", temp_path.display()))?;
        file.flush()
            .with_context(|| format!("flush temporary receipt {}", temp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temporary receipt {}", temp_path.display()))?;
        fs::rename(&temp_path, path).with_context(|| {
            format!(
                "publish CAR preflight receipt {} -> {}",
                temp_path.display(),
                path.display()
            )
        })?;
        File::open(parent)
            .with_context(|| format!("open receipt directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("sync receipt directory {}", parent.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn is_zstd_input(path: &Path) -> Result<bool> {
    let lower_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    if lower_name.ends_with(".zst") || lower_name.ends_with(".zst.part") {
        return Ok(true);
    }

    let mut file = File::open(path)
        .with_context(|| format!("open {} to detect compression", path.display()))?;
    let mut magic = [0u8; 4];
    let read = file
        .read(&mut magic)
        .with_context(|| format!("read compression magic from {}", path.display()))?;
    Ok(read == magic.len() && magic == [0x28, 0xb5, 0x2f, 0xfd])
}

fn add_u64(target: &mut u64, value: u64, label: &str) -> Result<()> {
    *target = target
        .checked_add(value)
        .with_context(|| format!("{label} overflow"))?;
    Ok(())
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{label} does not fit u64"))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use minicbor::Encoder;
    use std::io::Cursor;

    #[test]
    fn raw_preflight_reports_epoch_poh_shredding_and_duplicates() {
        let epoch = 7;
        let in_epoch_slot = epoch * SLOTS_PER_EPOCH + 9;
        let out_of_epoch_slot = (epoch + 1) * SLOTS_PER_EPOCH;
        let car = test_car(&[
            TestBlock {
                slot: in_epoch_slot,
                num_hashes: 11,
                transactions: 2,
                shredding_spans: 2,
            },
            TestBlock {
                slot: in_epoch_slot,
                num_hashes: 13,
                transactions: 1,
                shredding_spans: 0,
            },
            TestBlock {
                slot: out_of_epoch_slot,
                num_hashes: 17,
                transactions: 0,
                shredding_spans: 1,
            },
        ]);
        let root = test_root("raw");
        let input = root.join(format!("epoch-{epoch}.car.part"));
        let receipt_path = root.join("receipt.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&input, &car).unwrap();

        let receipt = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 64 * 1024,
            progress_json: None,
        })
        .unwrap();

        assert_eq!(receipt.validation_level, "structural");
        assert_eq!(receipt.epoch, epoch);
        assert_eq!(receipt.source_bytes, car.len() as u64);
        assert_eq!(receipt.decompressed_car_bytes, car.len() as u64);
        assert_eq!(receipt.blocks, 3);
        assert_eq!(receipt.blocks_in_epoch, 2);
        assert_eq!(receipt.present_slots, 1);
        assert_eq!(receipt.duplicate_slots, 1);
        assert_eq!(receipt.out_of_epoch_blocks, 1);
        assert_eq!(receipt.non_monotonic_slots, 1);
        assert_eq!(receipt.transactions, 3);
        assert_eq!(receipt.poh.records, 3);
        assert_eq!(receipt.poh.entries, 3);
        assert_eq!(receipt.poh.transaction_references, 3);
        assert_eq!(receipt.poh.num_hashes, "41");
        assert_eq!(receipt.shredding.records, 3);
        assert_eq!(receipt.shredding.spans, 3);
        assert_eq!(receipt.shredding.blocks_without_spans, 1);
        assert!(!receipt.eligible_for_compaction);

        let persisted: CarPreflightReceipt =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(persisted, receipt);
        assert!(persisted.source_modified_unix_secs > 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn zstd_preflight_streams_to_clean_eof_with_bounded_reader() {
        let epoch = 9;
        let car = test_car(&[TestBlock {
            slot: epoch * SLOTS_PER_EPOCH + 1,
            num_hashes: 5,
            transactions: 1,
            shredding_spans: 1,
        }]);
        let compressed = zstd::stream::encode_all(Cursor::new(&car), 1).unwrap();
        let root = test_root("zstd");
        // A downloader may use a generic hidden `.part` name. Compression is
        // therefore detected from zstd magic as well as `.zst.part` suffixes.
        let input = root.join(format!("epoch-{epoch}.download.part"));
        let receipt_path = root.join("receipt.json");
        let progress_path = root.join("progress.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&input, &compressed).unwrap();

        let receipt = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 64 * 1024,
            progress_json: Some(&progress_path),
        })
        .unwrap();

        assert!(receipt.compressed);
        assert!(receipt.clean_eof);
        assert!(receipt.eligible_for_compaction);
        assert_eq!(receipt.source_bytes, compressed.len() as u64);
        assert_eq!(receipt.decompressed_car_bytes, car.len() as u64);
        assert_eq!(receipt.poh.blocks_with_entries, 1);
        assert_eq!(receipt.shredding.blocks_with_spans, 1);
        let progress = fs::read_to_string(progress_path).unwrap();
        assert!(progress.contains("\"state\":\"complete\""));
        assert!(progress.contains("\"blocks_done\":1"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn matching_typed_receipt_is_reused_without_rereading_car() {
        let epoch = 10;
        let car = test_car(&[TestBlock {
            slot: epoch * SLOTS_PER_EPOCH + 1,
            num_hashes: 7,
            transactions: 1,
            shredding_spans: 1,
        }]);
        let root = test_root("receipt-reuse");
        let input = root.join(format!("epoch-{epoch}.car"));
        let receipt_path = root.join("receipt.json");
        let unexpected_progress = root.join("reused-progress.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&input, car).unwrap();

        let original = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 64 * 1024,
            progress_json: None,
        })
        .unwrap();
        let reused = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 128 * 1024,
            progress_json: Some(&unexpected_progress),
        })
        .unwrap();

        assert_eq!(reused, original);
        assert_eq!(reused.io_buffer_bytes, 64 * 1024);
        assert!(!unexpected_progress.exists());
        assert!(receipt_lock_path(&receipt_path).is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_source_fingerprint_forces_a_full_rescan() {
        let epoch = 11;
        let root = test_root("receipt-stale-fingerprint");
        let input = root.join(format!("epoch-{epoch}.car"));
        let receipt_path = root.join("receipt.json");
        let progress_path = root.join("rescan-progress.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &input,
            test_car(&[TestBlock {
                slot: epoch * SLOTS_PER_EPOCH + 1,
                num_hashes: 3,
                transactions: 0,
                shredding_spans: 1,
            }]),
        )
        .unwrap();
        let original = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 64 * 1024,
            progress_json: None,
        })
        .unwrap();

        let replacement = test_car(&[
            TestBlock {
                slot: epoch * SLOTS_PER_EPOCH + 1,
                num_hashes: 3,
                transactions: 0,
                shredding_spans: 1,
            },
            TestBlock {
                slot: epoch * SLOTS_PER_EPOCH + 2,
                num_hashes: 5,
                transactions: 1,
                shredding_spans: 2,
            },
        ]);
        fs::write(&input, &replacement).unwrap();
        let rescanned = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 128 * 1024,
            progress_json: Some(&progress_path),
        })
        .unwrap();

        assert_ne!(rescanned.source_bytes, original.source_bytes);
        assert_eq!(rescanned.source_bytes, replacement.len() as u64);
        assert_eq!(rescanned.blocks, 2);
        assert_eq!(rescanned.io_buffer_bytes, 128 * 1024);
        assert!(progress_path.is_file());
        let persisted: CarPreflightReceipt =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(persisted, rescanned);
        assert!(receipt_lock_path(&receipt_path).is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn oversized_receipt_is_ignored_and_replaced_by_a_full_scan() {
        let epoch = 12;
        let root = test_root("receipt-oversized");
        let input = root.join(format!("epoch-{epoch}.car"));
        let receipt_path = root.join("receipt.json");
        let progress_path = root.join("rescan-progress.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &input,
            test_car(&[TestBlock {
                slot: epoch * SLOTS_PER_EPOCH + 1,
                num_hashes: 9,
                transactions: 1,
                shredding_spans: 1,
            }]),
        )
        .unwrap();
        fs::write(
            &receipt_path,
            vec![b' '; (MAX_PREFLIGHT_RECEIPT_BYTES + 1) as usize],
        )
        .unwrap();

        let receipt = preflight_car(CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt_path,
            io_buffer_bytes: 64 * 1024,
            progress_json: Some(&progress_path),
        })
        .unwrap();

        assert!(receipt.eligible_for_compaction);
        assert_eq!(receipt.blocks, 1);
        assert!(progress_path.is_file());
        assert!(fs::metadata(&receipt_path).unwrap().len() < MAX_PREFLIGHT_RECEIPT_BYTES);
        let persisted: CarPreflightReceipt =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(persisted, receipt);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compression_detection_accepts_zst_part_suffix_and_zstd_magic() {
        let root = test_root("compression-detection");
        fs::create_dir_all(&root).unwrap();
        let suffix = root.join("epoch-1.car.zst.part");
        let magic = root.join("epoch-1.download.part");
        let raw = root.join("epoch-1.car.part");
        fs::write(&suffix, b"suffix is sufficient for dispatch").unwrap();
        fs::write(&magic, [0x28, 0xb5, 0x2f, 0xfd, 0]).unwrap();
        fs::write(&raw, [1, 0x80]).unwrap();

        assert!(is_zstd_input(&suffix).unwrap());
        assert!(is_zstd_input(&magic).unwrap());
        assert!(!is_zstd_input(&raw).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_rejects_unterminated_transaction_entry_group() {
        let mut car = car_header();
        push_car_entry(&mut car, &transaction_payload());
        push_car_entry(&mut car, &entry_payload(1, 1));
        let mut progress = ProgressTracker::new("CAR Preflight Test");
        let error = scan_reader(Cursor::new(car), 1, 64 * 1024, &mut progress).unwrap_err();
        assert!(error.to_string().contains("unterminated block group"));
    }

    #[derive(Clone, Copy)]
    struct TestBlock {
        slot: u64,
        num_hashes: u64,
        transactions: usize,
        shredding_spans: usize,
    }

    fn test_car(blocks: &[TestBlock]) -> Vec<u8> {
        let mut car = car_header();
        for block in blocks {
            for _ in 0..block.transactions {
                push_car_entry(&mut car, &transaction_payload());
            }
            push_car_entry(
                &mut car,
                &entry_payload(block.num_hashes, block.transactions),
            );
            push_car_entry(
                &mut car,
                &block_payload(block.slot, 1, block.shredding_spans),
            );
        }
        car
    }

    fn car_header() -> Vec<u8> {
        vec![1, 0x80]
    }

    fn transaction_payload() -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(1).unwrap();
        encoder.u8(0).unwrap();
        encoder.into_writer()
    }

    fn entry_payload(num_hashes: u64, transactions: usize) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(4).unwrap();
        encoder.u8(1).unwrap();
        encoder.u64(num_hashes).unwrap();
        encoder.bytes(&[num_hashes as u8; 32]).unwrap();
        encoder.array(transactions as u64).unwrap();
        for _ in 0..transactions {
            encoder.null().unwrap();
        }
        encoder.into_writer()
    }

    fn block_payload(slot: u64, entries: usize, shredding_spans: usize) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(5).unwrap();
        encoder.u8(2).unwrap();
        encoder.u64(slot).unwrap();
        encoder.array(shredding_spans as u64).unwrap();
        for index in 0..shredding_spans {
            encoder.array(2).unwrap();
            encoder.i64(index as i64).unwrap();
            encoder.i64((index + 1) as i64).unwrap();
        }
        encoder.array(entries as u64).unwrap();
        for _ in 0..entries {
            encoder.null().unwrap();
        }
        encoder.array(3).unwrap();
        encoder.u64(slot.saturating_sub(1)).unwrap();
        encoder.i64(1_700_000_000).unwrap();
        encoder.u64(slot).unwrap();
        encoder.into_writer()
    }

    fn push_car_entry(car: &mut Vec<u8>, payload: &[u8]) {
        push_uvarint(car, (36 + payload.len()) as u64);
        car.extend_from_slice(&[0u8; 36]);
        car.extend_from_slice(payload);
    }

    fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn test_root(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-car-preflight-{label}-{}-{unique}",
            std::process::id()
        ))
    }
}
