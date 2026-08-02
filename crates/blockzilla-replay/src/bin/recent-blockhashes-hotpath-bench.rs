//! Focused microbenchmark for the launch RecentBlockhashes sysvar wire update.
//!
//! This compares the former validate-then-shift implementation with the
//! canonical in-place rewrite used by replay. Both workloads advance the same
//! 301-entry Bank queue and assert byte-identical 150-entry account state.

use std::{collections::VecDeque, env, hint::black_box, process, time::Instant};

const DATA_LEN: usize = 6_008;
const HEADER_LEN: usize = 8;
const ENTRY_LEN: usize = 40;
const WIRE_ENTRIES: usize = 150;
const BANK_QUEUE_ENTRIES: usize = 301;
const DEFAULT_ITERATIONS: usize = 432_000;
const DEFAULT_ROUNDS: usize = 5;

#[derive(Clone, Copy)]
struct Entry {
    hash: [u8; 32],
    fee: u64,
}

#[derive(Clone, Copy)]
enum Strategy {
    ValidateThenShift,
    CanonicalRewrite,
}

fn main() {
    let (iterations, rounds) = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => return,
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(2);
        }
    };

    verify_equivalence(iterations.min(10_000));
    let _ = run_workload(Strategy::ValidateThenShift, iterations.min(10_000));
    let _ = run_workload(Strategy::CanonicalRewrite, iterations.min(10_000));

    let mut old = Vec::with_capacity(rounds);
    let mut rewrite = Vec::with_capacity(rounds);
    for round in 0..rounds {
        if round.is_multiple_of(2) {
            old.push(timed(Strategy::ValidateThenShift, iterations));
            rewrite.push(timed(Strategy::CanonicalRewrite, iterations));
        } else {
            rewrite.push(timed(Strategy::CanonicalRewrite, iterations));
            old.push(timed(Strategy::ValidateThenShift, iterations));
        }
    }
    old.sort_unstable();
    rewrite.sort_unstable();
    let old = old[old.len() / 2];
    let rewrite = rewrite[rewrite.len() / 2];

    println!("recent-blockhashes-hotpath-bench iterations={iterations} rounds={rounds}");
    println!(
        "validate_then_shift median_ms={:.3} ns/update={:.1}",
        old.as_secs_f64() * 1_000.0,
        old.as_nanos() as f64 / iterations as f64,
    );
    println!(
        "canonical_rewrite  median_ms={:.3} ns/update={:.1}",
        rewrite.as_secs_f64() * 1_000.0,
        rewrite.as_nanos() as f64 / iterations as f64,
    );
    println!(
        "speedup={:.3}x equivalence=PASS",
        old.as_secs_f64() / rewrite.as_secs_f64(),
    );
}

fn timed(strategy: Strategy, iterations: usize) -> std::time::Duration {
    let started = Instant::now();
    let (checksum, _) = run_workload(strategy, iterations);
    let elapsed = started.elapsed();
    black_box(checksum);
    elapsed
}

fn verify_equivalence(iterations: usize) {
    let old = run_workload(Strategy::ValidateThenShift, iterations);
    let rewrite = run_workload(Strategy::CanonicalRewrite, iterations);
    assert_eq!(
        old, rewrite,
        "both update strategies must be byte-identical"
    );
}

fn run_workload(strategy: Strategy, iterations: usize) -> (u64, Vec<u8>) {
    let mut entries = initial_entries();
    let mut data = vec![0_u8; DATA_LEN];
    canonical_rewrite(&mut data, &entries);

    for ordinal in BANK_QUEUE_ENTRIES as u64..BANK_QUEUE_ENTRIES as u64 + iterations as u64 {
        entries.push_front(entry(ordinal));
        if entries.len() > BANK_QUEUE_ENTRIES {
            entries.pop_back();
        }
        match strategy {
            Strategy::ValidateThenShift => validate_then_shift(&mut data, &entries),
            Strategy::CanonicalRewrite => canonical_rewrite(&mut data, &entries),
        }
    }

    let checksum = data.chunks_exact(8).fold(0_u64, |checksum, bytes| {
        checksum.rotate_left(7) ^ u64::from_le_bytes(bytes.try_into().unwrap())
    });
    (checksum, data)
}

fn initial_entries() -> VecDeque<Entry> {
    (0..BANK_QUEUE_ENTRIES as u64).rev().map(entry).collect()
}

fn entry(ordinal: u64) -> Entry {
    Entry {
        hash: deterministic_hash(ordinal),
        fee: 5_000 + ordinal % 23,
    }
}

fn deterministic_hash(ordinal: u64) -> [u8; 32] {
    let mut hash = [0_u8; 32];
    let mut state = ordinal ^ 0xa076_1d64_78bd_642f;
    for word in hash.chunks_exact_mut(8) {
        state = mix64(state);
        word.copy_from_slice(&state.to_le_bytes());
    }
    hash
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_then_shift(data: &mut [u8], entries: &VecDeque<Entry>) {
    let previous_count = entries.len().saturating_sub(1).min(WIRE_ENTRIES);
    let header = u64::from_le_bytes(data[..HEADER_LEN].try_into().unwrap());
    let mut offset = HEADER_LEN;
    let mut matches = header == previous_count as u64;
    for entry in entries.iter().skip(1).take(previous_count) {
        matches &= data[offset..offset + 32] == entry.hash;
        matches &= data[offset + 32..offset + ENTRY_LEN] == entry.fee.to_le_bytes();
        offset += ENTRY_LEN;
    }
    matches &= data[offset..].iter().all(|byte| *byte == 0);
    if !matches {
        canonical_rewrite(data, entries);
        return;
    }

    let retained_count = previous_count.min(WIRE_ENTRIES - 1);
    let retained_bytes = retained_count * ENTRY_LEN;
    data.copy_within(
        HEADER_LEN..HEADER_LEN + retained_bytes,
        HEADER_LEN + ENTRY_LEN,
    );
    write_entry(data, HEADER_LEN, entries.front().unwrap());
    data[..HEADER_LEN].copy_from_slice(&((retained_count + 1) as u64).to_le_bytes());
}

fn canonical_rewrite(data: &mut [u8], entries: &VecDeque<Entry>) {
    let count = entries.len().min(WIRE_ENTRIES);
    data[..HEADER_LEN].copy_from_slice(&(count as u64).to_le_bytes());
    let mut offset = HEADER_LEN;
    for entry in entries.iter().take(count) {
        write_entry(data, offset, entry);
        offset += ENTRY_LEN;
    }
    data[offset..].fill(0);
}

fn write_entry(data: &mut [u8], offset: usize, entry: &Entry) {
    data[offset..offset + 32].copy_from_slice(&entry.hash);
    data[offset + 32..offset + ENTRY_LEN].copy_from_slice(&entry.fee.to_le_bytes());
}

fn parse_args() -> Result<Option<(usize, usize)>, String> {
    let mut iterations = DEFAULT_ITERATIONS;
    let mut rounds = DEFAULT_ROUNDS;
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        if matches!(argument.as_str(), "-h" | "--help") {
            println!("Usage: recent-blockhashes-hotpath-bench [--iterations N] [--rounds N]");
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?
            .parse::<usize>()
            .map_err(|_| format!("invalid integer for {argument}"))?;
        match argument.as_str() {
            "--iterations" => iterations = value,
            "--rounds" => rounds = value,
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    if iterations == 0 || rounds == 0 {
        return Err("iterations and rounds must be greater than zero".to_owned());
    }
    Ok(Some((iterations, rounds)))
}
