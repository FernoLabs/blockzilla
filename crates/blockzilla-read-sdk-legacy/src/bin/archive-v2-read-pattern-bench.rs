//! Read-only benchmark for ordered and disjoint Archive V2 block reads.
//!
//! The benchmark admits the real block index, selects a frame-aligned byte
//! window, and reads the indexed compressed bytes without hashing or decoding
//! them. It never writes inside the archive generation.

use std::{
    env, fs,
    ops::Range,
    path::PathBuf,
    process,
    sync::{Arc, Barrier},
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use blockzilla_format::ArchiveV2HotBlockIndexRow;
use blockzilla_read_sdk_legacy::{
    ArchiveReader, ArchiveV2MetadataWireProfile, ArchiveV2WireProfile, HashVerification,
    OpenOptions, PinnedLocalRangeSource, RangeSource,
    manifest::{BLOCKS_FILE, TrustedGenerationIdentity},
};
use serde::Serialize;

const DEFAULT_SLOTS_PER_EPOCH: u64 = 432_000;
const DEFAULT_WINDOW_MIB: u64 = 8 * 1024;
const DEFAULT_BATCH_MIB: usize = 64;
const DEFAULT_DISJOINT_WORKERS: usize = 8;
const MAX_WORKERS: usize = 64;
const MAX_BATCH_MIB: usize = 64;
const MIB: u64 = 1024 * 1024;

#[derive(Debug)]
struct Args {
    archive: PathBuf,
    epoch: u64,
    slots_per_epoch: u64,
    mode: Mode,
    window_index: u64,
    window_bytes: u64,
    workers: usize,
    batch_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Mode {
    Ordered,
    Disjoint,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "ordered" => Ok(Self::Ordered),
            "disjoint" => Ok(Self::Disjoint),
            _ => Err(format!(
                "unsupported mode {value:?}; expected ordered or disjoint"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowPlan {
    rows: Range<usize>,
    logical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BatchPlan {
    rows: Range<usize>,
    logical_bytes: u64,
}

#[derive(Debug, Default)]
struct ReadStats {
    calls: u64,
    logical_bytes: u64,
}

impl ReadStats {
    fn merge(&mut self, other: Self) -> Result<(), String> {
        self.calls = self
            .calls
            .checked_add(other.calls)
            .ok_or_else(|| "read-call count overflow".to_owned())?;
        self.logical_bytes = self
            .logical_bytes
            .checked_add(other.logical_bytes)
            .ok_or_else(|| "logical-byte count overflow".to_owned())?;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    kind: &'static str,
    archive: String,
    epoch: u64,
    mode: Mode,
    window_index: u64,
    requested_window_bytes: u64,
    start_row: usize,
    end_row_exclusive: usize,
    start_slot: u64,
    end_slot_inclusive: u64,
    start_byte: u64,
    end_byte_exclusive: u64,
    logical_bytes: u64,
    workers: usize,
    batch_bytes: usize,
    read_calls: u64,
    elapsed_seconds: f64,
    logical_bytes_per_second: f64,
    hash_verification: &'static str,
    completed_unix_seconds: u64,
}

#[derive(Debug, Serialize)]
struct ErrorReport {
    schema_version: u32,
    kind: &'static str,
    archive: String,
    epoch: u64,
    mode: Mode,
    error: String,
    completed_unix_seconds: u64,
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}\n\n{}", usage());
            process::exit(2);
        }
    };

    let archive = match fs::canonicalize(&args.archive) {
        Ok(archive) => archive,
        Err(error) => {
            print_error_and_exit(
                &args,
                args.archive.display().to_string(),
                format!("cannot open archive directory: {error}"),
            );
        }
    };
    let archive_display = archive.display().to_string();
    match run(&args, archive) {
        Ok(report) => print_json(&report),
        Err(error) => print_error_and_exit(&args, archive_display, error),
    }
}

fn run(args: &Args, archive: PathBuf) -> Result<Report, String> {
    let source = PinnedLocalRangeSource::new(&archive);
    let reader = ArchiveReader::open_trusted_with_metadata_profile(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".into(),
            epoch: args.epoch,
            generation_id: "read-only-read-pattern-bench".into(),
            slots_per_epoch: args.slots_per_epoch,
            // The hot-block envelope is profile-neutral. This assertion only
            // supplies the trusted-local generation binding.
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        },
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .map_err(|error| format!("cannot validate Archive V2 container structure: {error}"))?;

    let rows = &reader.index().rows;
    let lengths: Vec<u32> = rows.iter().map(|row| row.compressed_len).collect();
    let window = plan_window(&lengths, args.window_index, args.window_bytes)?;
    let first = rows
        .get(window.rows.start)
        .ok_or_else(|| "selected window has no first row".to_owned())?;
    let last = rows
        .get(window.rows.end - 1)
        .ok_or_else(|| "selected window has no last row".to_owned())?;
    let end_byte = last
        .compressed_offset
        .checked_add(u64::from(last.compressed_len))
        .ok_or_else(|| "selected byte-range end overflow".to_owned())?;
    let selected_bytes = end_byte
        .checked_sub(first.compressed_offset)
        .ok_or_else(|| "selected byte range is reversed".to_owned())?;
    if selected_bytes != window.logical_bytes {
        return Err(format!(
            "selected index rows contain {selected_bytes} contiguous bytes, expected {}",
            window.logical_bytes
        ));
    }

    let ranges = match args.mode {
        Mode::Ordered => vec![window.rows.clone()],
        Mode::Disjoint => partition_weighted(&lengths, window.rows.clone(), args.workers)?,
    };
    let actual_workers = ranges.len();
    let (stats, elapsed) = read_ranges(&source, rows, &lengths, ranges, args.batch_bytes)?;
    if stats.logical_bytes != window.logical_bytes {
        return Err(format!(
            "workers read {} logical bytes, expected {}",
            stats.logical_bytes, window.logical_bytes
        ));
    }
    source
        .verify_unchanged()
        .map_err(|error| format!("archive files changed during the benchmark: {error}"))?;

    let elapsed_seconds = elapsed.as_secs_f64();
    let logical_bytes_per_second = if elapsed_seconds == 0.0 {
        0.0
    } else {
        stats.logical_bytes as f64 / elapsed_seconds
    };
    Ok(Report {
        schema_version: 1,
        kind: "archive-v2-read-pattern-bench",
        archive: archive.display().to_string(),
        epoch: args.epoch,
        mode: args.mode,
        window_index: args.window_index,
        requested_window_bytes: args.window_bytes,
        start_row: window.rows.start,
        end_row_exclusive: window.rows.end,
        start_slot: first.slot,
        end_slot_inclusive: last.slot,
        start_byte: first.compressed_offset,
        end_byte_exclusive: end_byte,
        logical_bytes: stats.logical_bytes,
        workers: actual_workers,
        batch_bytes: args.batch_bytes,
        read_calls: stats.calls,
        elapsed_seconds,
        logical_bytes_per_second,
        hash_verification: "sizes-only-no-block-hash",
        completed_unix_seconds: unix_seconds(),
    })
}

fn read_ranges(
    source: &PinnedLocalRangeSource,
    rows: &[ArchiveV2HotBlockIndexRow],
    lengths: &[u32],
    ranges: Vec<Range<usize>>,
    batch_bytes: usize,
) -> Result<(ReadStats, std::time::Duration), String> {
    let maximum_bytes =
        u64::try_from(batch_bytes).map_err(|_| "batch size does not fit u64".to_owned())?;
    let workers: Vec<Vec<BatchPlan>> = ranges
        .into_iter()
        .map(|range| plan_batches(lengths, range, maximum_bytes))
        .collect::<Result<_, _>>()?;
    let barrier = Arc::new(Barrier::new(workers.len() + 1));
    thread::scope(|scope| {
        let handles: Vec<_> = workers
            .into_iter()
            .map(|batches| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    // Two barrier rounds keep thread creation outside the
                    // measured interval and release all readers together.
                    barrier.wait();
                    barrier.wait();
                    read_batches(source, rows, batches)
                })
            })
            .collect();

        barrier.wait();
        let started = Instant::now();
        barrier.wait();
        let mut stats = ReadStats::default();
        for handle in handles {
            let worker = handle
                .join()
                .map_err(|_| "a benchmark worker panicked".to_owned())??;
            stats.merge(worker)?;
        }
        Ok((stats, started.elapsed()))
    })
}

fn read_batches(
    source: &PinnedLocalRangeSource,
    rows: &[ArchiveV2HotBlockIndexRow],
    batches: Vec<BatchPlan>,
) -> Result<ReadStats, String> {
    let mut bytes = Vec::new();
    let mut stats = ReadStats::default();
    for batch in batches {
        let first = rows
            .get(batch.rows.start)
            .ok_or_else(|| "batch has no first row".to_owned())?;
        let length = usize::try_from(batch.logical_bytes)
            .map_err(|_| "batch byte length does not fit usize".to_owned())?;
        source
            .read_range_into(BLOCKS_FILE, first.compressed_offset, length, &mut bytes)
            .map_err(|error| {
                format!(
                    "cannot read rows {}..{} at byte {}: {error}",
                    batch.rows.start, batch.rows.end, first.compressed_offset
                )
            })?;
        stats.calls = stats
            .calls
            .checked_add(1)
            .ok_or_else(|| "read-call count overflow".to_owned())?;
        stats.logical_bytes = stats
            .logical_bytes
            .checked_add(batch.logical_bytes)
            .ok_or_else(|| "logical-byte count overflow".to_owned())?;
    }
    Ok(stats)
}

fn plan_window(
    lengths: &[u32],
    window_index: u64,
    window_bytes: u64,
) -> Result<WindowPlan, String> {
    if lengths.is_empty() {
        return Err("block index has no rows".to_owned());
    }
    if window_bytes == 0 {
        return Err("window size must be positive".to_owned());
    }
    let requested_start = window_index
        .checked_mul(window_bytes)
        .ok_or_else(|| "requested window start overflows u64".to_owned())?;
    let requested_end = window_index
        .checked_add(1)
        .and_then(|index| index.checked_mul(window_bytes))
        .ok_or_else(|| "requested window end overflows u64".to_owned())?;
    let prefix = prefix_bytes(lengths)?;
    let total = *prefix.last().expect("prefix always contains zero");
    if requested_end > total {
        return Err(format!(
            "window {window_index} ends at requested byte {requested_end}, after the {total}-byte block file"
        ));
    }
    let start = boundary_at_or_after(&prefix, requested_start);
    let end = boundary_at_or_after(&prefix, requested_end);
    if start >= end {
        return Err(format!(
            "window {window_index} contains no complete frame-aligned row boundary"
        ));
    }
    Ok(WindowPlan {
        rows: start..end,
        logical_bytes: prefix[end] - prefix[start],
    })
}

fn partition_weighted(
    lengths: &[u32],
    range: Range<usize>,
    requested_workers: usize,
) -> Result<Vec<Range<usize>>, String> {
    if range.start > range.end || range.end > lengths.len() {
        return Err("selected row range is outside the block index".to_owned());
    }
    if range.is_empty() {
        return Err("selected row range is empty".to_owned());
    }
    let workers = requested_workers.max(1).min(range.len());
    let selected = &lengths[range.clone()];
    let prefix = prefix_bytes(selected)?;
    let total = *prefix.last().expect("prefix always contains zero");
    let mut ranges = Vec::with_capacity(workers);
    let mut relative_start = 0usize;
    for boundary in 1..workers {
        let target = u64::try_from(u128::from(total) * boundary as u128 / workers as u128)
            .expect("weighted target cannot exceed total");
        let min_end = relative_start + 1;
        let max_end = selected.len() - (workers - boundary);
        let relative_end = nearest_boundary(&prefix, target, min_end, max_end);
        ranges.push((range.start + relative_start)..(range.start + relative_end));
        relative_start = relative_end;
    }
    ranges.push((range.start + relative_start)..range.end);
    Ok(ranges)
}

fn plan_batches(
    lengths: &[u32],
    range: Range<usize>,
    maximum_bytes: u64,
) -> Result<Vec<BatchPlan>, String> {
    if maximum_bytes == 0 {
        return Err("batch size must be positive".to_owned());
    }
    if range.start > range.end || range.end > lengths.len() {
        return Err("batch row range is outside the block index".to_owned());
    }
    let mut batches = Vec::new();
    let mut start = range.start;
    while start < range.end {
        let mut end = start + 1;
        let mut bytes = u64::from(lengths[start]);
        while end < range.end {
            let next = u64::from(lengths[end]);
            let Some(combined) = bytes.checked_add(next) else {
                return Err("batch byte length overflow".to_owned());
            };
            if combined > maximum_bytes {
                break;
            }
            bytes = combined;
            end += 1;
        }
        batches.push(BatchPlan {
            rows: start..end,
            logical_bytes: bytes,
        });
        start = end;
    }
    Ok(batches)
}

fn prefix_bytes(lengths: &[u32]) -> Result<Vec<u64>, String> {
    let mut prefix = Vec::with_capacity(lengths.len() + 1);
    prefix.push(0u64);
    for &length in lengths {
        if length == 0 {
            return Err("block index contains a zero-length compressed frame".to_owned());
        }
        let next = prefix
            .last()
            .copied()
            .expect("prefix contains zero")
            .checked_add(u64::from(length))
            .ok_or_else(|| "compressed-byte count overflow".to_owned())?;
        prefix.push(next);
    }
    Ok(prefix)
}

fn boundary_at_or_after(prefix: &[u64], target: u64) -> usize {
    prefix.partition_point(|&offset| offset < target)
}

fn nearest_boundary(prefix: &[u64], target: u64, minimum: usize, maximum: usize) -> usize {
    debug_assert!(minimum <= maximum);
    let insertion = prefix.partition_point(|&offset| offset < target);
    let upper = insertion.clamp(minimum, maximum);
    let lower = upper.saturating_sub(1).clamp(minimum, maximum);
    if target.abs_diff(prefix[lower]) <= target.abs_diff(prefix[upper]) {
        lower
    } else {
        upper
    }
}

fn parse_args() -> Result<Args, String> {
    let mut archive = None;
    let mut epoch = None;
    let mut slots_per_epoch = DEFAULT_SLOTS_PER_EPOCH;
    let mut mode = None;
    let mut window_index = 0u64;
    let mut window_mib = DEFAULT_WINDOW_MIB;
    let mut workers = None;
    let mut batch_mib = DEFAULT_BATCH_MIB;
    let mut arguments = env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        let argument = argument
            .into_string()
            .map_err(|_| "arguments must be valid UTF-8".to_owned())?;
        if argument == "--help" || argument == "-h" {
            println!("{}", usage());
            process::exit(0);
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("{argument} requires a value"))?;
        match argument.as_str() {
            "--archive" => archive = Some(PathBuf::from(value)),
            "--epoch" => epoch = Some(parse_number(value, "epoch")?),
            "--slots-per-epoch" => {
                slots_per_epoch = parse_number(value, "slots per epoch")?;
            }
            "--mode" => {
                let value = value
                    .into_string()
                    .map_err(|_| "mode must be valid UTF-8".to_owned())?;
                mode = Some(Mode::parse(&value)?);
            }
            "--window-index" => window_index = parse_number(value, "window index")?,
            "--window-mib" => window_mib = parse_number(value, "window MiB")?,
            "--workers" => workers = Some(parse_number(value, "worker count")?),
            "--batch-mib" => batch_mib = parse_number(value, "batch MiB")?,
            _ => return Err(format!("unknown argument {argument:?}")),
        }
    }

    let archive = archive.ok_or_else(|| "--archive is required".to_owned())?;
    let epoch = epoch.ok_or_else(|| "--epoch is required".to_owned())?;
    let mode = mode.ok_or_else(|| "--mode is required".to_owned())?;
    if slots_per_epoch == 0 {
        return Err("--slots-per-epoch must be positive".to_owned());
    }
    if window_mib == 0 {
        return Err("--window-mib must be positive".to_owned());
    }
    if !(1..=MAX_BATCH_MIB).contains(&batch_mib) {
        return Err(format!("--batch-mib must be between 1 and {MAX_BATCH_MIB}"));
    }
    let workers = workers.unwrap_or(match mode {
        Mode::Ordered => 1,
        Mode::Disjoint => DEFAULT_DISJOINT_WORKERS,
    });
    if !(1..=MAX_WORKERS).contains(&workers) {
        return Err(format!("--workers must be between 1 and {MAX_WORKERS}"));
    }
    if mode == Mode::Ordered && workers != 1 {
        return Err("ordered mode requires --workers 1".to_owned());
    }
    let window_bytes = window_mib
        .checked_mul(MIB)
        .ok_or_else(|| "window byte size overflows u64".to_owned())?;
    let batch_bytes_u64 = u64::try_from(batch_mib)
        .expect("batch MiB fits u64")
        .checked_mul(MIB)
        .ok_or_else(|| "batch byte size overflows u64".to_owned())?;
    let batch_bytes = usize::try_from(batch_bytes_u64)
        .map_err(|_| "batch byte size does not fit usize".to_owned())?;
    Ok(Args {
        archive,
        epoch,
        slots_per_epoch,
        mode,
        window_index,
        window_bytes,
        workers,
        batch_bytes,
    })
}

fn parse_number<T: std::str::FromStr>(value: std::ffi::OsString, label: &str) -> Result<T, String> {
    value
        .into_string()
        .map_err(|_| format!("{label} must be valid UTF-8"))?
        .parse()
        .map_err(|_| format!("{label} is not a valid number"))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn print_json(report: &impl Serialize) {
    match serde_json::to_string(report) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("cannot serialize benchmark report: {error}");
            process::exit(1);
        }
    }
}

fn print_error_and_exit(args: &Args, archive: String, error: String) -> ! {
    print_json(&ErrorReport {
        schema_version: 1,
        kind: "archive-v2-read-pattern-bench-error",
        archive,
        epoch: args.epoch,
        mode: args.mode,
        error,
        completed_unix_seconds: unix_seconds(),
    });
    process::exit(1);
}

fn usage() -> &'static str {
    "Usage: archive-v2-read-pattern-bench --archive ABSOLUTE_EPOCH_DIR --epoch N \\
     --mode ordered|disjoint [--window-index 0] [--window-mib 8192] \\
     [--workers 1|8] [--batch-mib 64] [--slots-per-epoch 432000]\n\
\n\
Crossed cold example:\n\
  epoch A: ordered --window-index 0; disjoint --window-index 1\n\
  epoch B: disjoint --window-index 0; ordered --window-index 1"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacent_windows_share_one_frame_boundary() {
        let lengths = [4, 4, 4, 4, 4, 4];
        let first = plan_window(&lengths, 0, 7).unwrap();
        let second = plan_window(&lengths, 1, 7).unwrap();
        assert_eq!(first.rows, 0..2);
        assert_eq!(second.rows, 2..4);
        assert_eq!(first.logical_bytes, 8);
        assert_eq!(second.logical_bytes, 8);
        assert_eq!(first.rows.end, second.rows.start);
    }

    #[test]
    fn window_rejects_a_request_past_the_block_file() {
        let error = plan_window(&[4, 4, 4], 1, 8).unwrap_err();
        assert!(error.contains("after the 12-byte block file"));
    }

    #[test]
    fn weighted_ranges_are_complete_disjoint_and_nonempty() {
        let lengths = [8, 1, 1, 8, 1, 1];
        let ranges = partition_weighted(&lengths, 0..lengths.len(), 3).unwrap();
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, lengths.len());
        assert_eq!(ranges.len(), 3);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
        assert!(ranges.iter().all(|range| !range.is_empty()));
    }

    #[test]
    fn worker_count_is_bounded_by_selected_rows() {
        let ranges = partition_weighted(&[1, 1, 1], 0..3, 8).unwrap();
        assert_eq!(ranges, vec![0..1, 1..2, 2..3]);
    }

    #[test]
    fn batches_never_split_frames_and_allow_one_oversized_frame() {
        let batches = plan_batches(&[3, 4, 8, 2], 0..4, 7).unwrap();
        assert_eq!(
            batches,
            vec![
                BatchPlan {
                    rows: 0..2,
                    logical_bytes: 7,
                },
                BatchPlan {
                    rows: 2..3,
                    logical_bytes: 8,
                },
                BatchPlan {
                    rows: 3..4,
                    logical_bytes: 2,
                },
            ]
        );
    }
}
