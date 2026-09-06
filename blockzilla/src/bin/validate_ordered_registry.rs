//! Validate a direct-CAR Archive V2 usage-sorted registry in O(1) memory.
//!
//! The registry is a flat sequence of 32-byte keys paired with a varint count
//! stream. A valid usage-sorted registry is ordered by descending count, and
//! keys with an equal count are strictly increasing. One optional synthetic
//! builtin row (Compute Budget with a zero count) may lead the file.
//!
//! Skipped-slot map structure is validated by
//! [`blockzilla_archive_v2::read_skipped_slot_map`], so this binary only adds the
//! cross-file agreement between that map and `blockhash_registry.bin`.

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::read_skipped_slot_map;
use blockzilla_primitives::framed::read_u32_varint;
use clap::Parser;
use serde::Serialize;
use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

/// One Solana epoch. A blockhash registry may never exceed this many rows.
const SLOTS_PER_EPOCH: u64 = 432_000;

/// Compute Budget, the one key admitted as a leading zero-count synthetic row.
const COMPUTE_BUDGET_KEY: [u8; 32] = [
    0x03, 0x06, 0x46, 0x6f, 0xe5, 0x21, 0x17, 0x32, 0xff, 0xec, 0xad, 0xba, 0x72, 0xc3, 0x9b, 0xe7,
    0xbc, 0x8c, 0xe5, 0xbb, 0xc5, 0xf7, 0x12, 0x6b, 0x2c, 0x43, 0x9b, 0x3a, 0x40, 0x00, 0x00, 0x00,
];

const READ_BUFFER_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Parser)]
struct Args {
    registry: PathBuf,
    counts: PathBuf,
    blockhashes: PathBuf,
    skipped_slots: PathBuf,
}

#[derive(Debug, Serialize)]
struct Report {
    blockhash_registry_bytes: u64,
    blockhash_rows: u64,
    epoch: Option<u64>,
    present_slots: u32,
    registry_bytes: u64,
    registry_counts_bytes: u64,
    registry_rows: u64,
    skipped_slots: u32,
    skipped_slots_bytes: u64,
    slots_per_epoch: u32,
    synthetic_builtin_rows: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let registry_bytes = file_len(&args.registry)?;
    let counts_bytes = file_len(&args.counts)?;
    let blockhash_bytes = file_len(&args.blockhashes)?;

    ensure!(
        registry_bytes != 0 && registry_bytes % 32 == 0,
        "registry.bin must be nonempty and divisible by 32"
    );
    ensure!(counts_bytes != 0, "registry_counts.bin must be nonempty");
    ensure!(
        blockhash_bytes != 0 && blockhash_bytes % 32 == 0,
        "blockhash_registry.bin must be nonempty and divisible by 32"
    );

    let expected_rows = registry_bytes / 32;
    let (rows, synthetic_builtin_rows) = validate_registry_order(&args)?;
    ensure!(
        rows == expected_rows,
        "registry row mismatch: expected {expected_rows}, read {rows}"
    );

    let blockhash_rows = blockhash_bytes / 32;
    ensure!(
        blockhash_rows <= SLOTS_PER_EPOCH,
        "blockhash registry exceeds one Solana epoch: {blockhash_rows} rows"
    );

    let map = read_skipped_slot_map(&args.skipped_slots)
        .with_context(|| format!("read {}", args.skipped_slots.display()))?;
    let present_slots = u64::from(map.present_slots());
    ensure!(
        present_slots == blockhash_rows,
        "skipped_slots.bin present-slot count differs from blockhash_registry.bin: \
         {present_slots} != {blockhash_rows}"
    );

    let report = Report {
        blockhash_registry_bytes: blockhash_bytes,
        blockhash_rows,
        epoch: map.epoch(),
        present_slots: map.present_slots(),
        registry_bytes,
        registry_counts_bytes: counts_bytes,
        registry_rows: rows,
        skipped_slots: map.skipped_slots(),
        skipped_slots_bytes: file_len(&args.skipped_slots)?,
        slots_per_epoch: map.slots_per_epoch(),
        synthetic_builtin_rows,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

/// Stream both files once and confirm the usage-sorted ordering contract.
///
/// Returns the total row count and whether a synthetic builtin row led the file.
fn validate_registry_order(args: &Args) -> Result<(u64, u64)> {
    let mut registry = BufReader::with_capacity(
        READ_BUFFER_BYTES,
        File::open(&args.registry)
            .with_context(|| format!("open {}", args.registry.display()))?,
    );
    let mut counts = BufReader::with_capacity(
        READ_BUFFER_BYTES,
        File::open(&args.counts).with_context(|| format!("open {}", args.counts.display()))?,
    );

    let mut previous: Option<([u8; 32], u32)> = None;
    let mut rows = 0u64;
    let mut synthetic_builtin_rows = 0u64;

    loop {
        let mut key = [0u8; 32];
        match read_full(&mut registry, &mut key)? {
            0 => break,
            32 => {}
            _ => bail!("truncated registry key"),
        }
        let count = read_u32_varint(&mut counts)
            .context("read registry_counts.bin")?
            .context("registry_counts.bin has fewer rows than registry.bin")?;

        if rows == 0 && key == COMPUTE_BUDGET_KEY && count == 0 {
            // The synthetic builtin row is exempt from the ordering contract and
            // must not seed the comparison for the first real row.
            synthetic_builtin_rows = 1;
            previous = None;
        } else {
            ensure!(count != 0, "zero count at normal registry row {}", rows + 1);
            if let Some((previous_key, previous_count)) = previous {
                ensure!(
                    count <= previous_count,
                    "count order increases at registry row {}: {previous_count} -> {count}",
                    rows + 1
                );
                ensure!(
                    count != previous_count || key > previous_key,
                    "equal-count keys are not strictly increasing at row {}",
                    rows + 1
                );
            }
            previous = Some((key, count));
        }
        rows += 1;
    }

    ensure!(
        read_u32_varint(&mut counts)
            .context("read registry_counts.bin")?
            .is_none(),
        "registry_counts.bin has more rows than registry.bin"
    );
    ensure!(
        counts.read(&mut [0u8; 1])? == 0,
        "registry_counts.bin has trailing bytes"
    );

    Ok((rows, synthetic_builtin_rows))
}

/// Read until `buffer` is full or the source ends, returning the bytes read.
fn read_full<R: Read>(reader: &mut R, buffer: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn file_len(path: &PathBuf) -> Result<u64> {
    Ok(std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len())
}
