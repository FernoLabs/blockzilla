//! End-to-end microbenchmark for the launch replay hot path.
//!
//! This deliberately uses a real Blockzilla Compact Archive V2 generation. It
//! neither accepts nor opens a CAR file. Two otherwise identical replays make
//! the cost of analytical instruction diffs visible:
//!
//! - `all`: construct and stream every instruction diff;
//! - `none`: execute the same state transitions with allocation-minimal `None`
//!   diff capture, including no diagnostic-only rollback diffs.
//!
//! Timing and allocation traffic are sampled in separate replay runs so the
//! allocator's atomic counters cannot bias wall-clock results. The benchmark
//! checks every measured run against the same state hash and replay counters.
//! Run it with the release profile and enough prefix rows to amortize archive
//! setup:
//!
//! ```text
//! cargo run --release -p blockzilla-replay --bin replay-hotpath-bench -- \
//!   /path/to/epoch-0 --max-slots 50000 --warmups 1 --rounds 5
//! ```
//!
//! During optimization cycles, `--mode none` benchmarks only the
//! allocation-minimal branch; `--mode all` selects only full diff capture.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    env,
    fmt::Write as _,
    hint::black_box,
    path::PathBuf,
    process,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use blockzilla_replay::launch_replay::{
    LaunchDerivedTransactionFailure, LaunchInstructionDiffCapture,
    visit_launch_prefix_diagnostic_with_diff_capture,
};
use blockzilla_replay::{CompactVisitConfig, LaunchDiagnosticReplayOutcome};
use sha2::{Digest, Sha256};

const DEFAULT_MAX_SLOTS: usize = 10_000;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_ROUNDS: usize = 5;

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);

struct CountingAllocator;

// Count successful allocation and reallocation requests only during dedicated
// allocation-sampling replays. Timing replays still pay for one relaxed atomic
// load per allocation, but perform no atomic read-modify-write operations. The
// counters are intentionally process-global because replay is single-threaded.
#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarding the exact layout supplied by the allocator caller.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarding the exact layout supplied by the allocator caller.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` and `layout` came from the corresponding allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `pointer` and `layout` came from the corresponding allocator,
        // and `new_size` is forwarded unchanged.
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            record_allocation(new_size);
        }
        new_pointer
    }
}

fn record_allocation(bytes: usize) {
    if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
struct AllocationSnapshot {
    calls: u64,
    bytes: u64,
}

impl AllocationSnapshot {
    fn now() -> Self {
        Self {
            calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
            bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }
}

struct AllocationCountingGuard;

impl AllocationCountingGuard {
    fn start() -> Self {
        assert!(
            !COUNT_ALLOCATIONS.load(Ordering::Relaxed),
            "allocation counting must not be nested"
        );
        ALLOCATION_CALLS.store(0, Ordering::Relaxed);
        ALLOCATED_BYTES.store(0, Ordering::Relaxed);
        COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
        Self
    }

    fn finish(self) -> AllocationSnapshot {
        COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
        let snapshot = AllocationSnapshot::now();
        std::mem::forget(self);
        snapshot
    }
}

impl Drop for AllocationCountingGuard {
    fn drop(&mut self) {
        COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
struct Config {
    generation: PathBuf,
    start_slot: Option<u64>,
    max_slots: usize,
    warmups: usize,
    rounds: usize,
    mode: ModeSelection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModeSelection {
    Both,
    All,
    None,
}

impl ModeSelection {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "both" => Ok(Self::Both),
            "all" => Ok(Self::All),
            "none" => Ok(Self::None),
            _ => Err(format!(
                "invalid value for --mode: {value} (expected both, all, or none)"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchMode {
    AllDiffs,
    NoDiffs,
}

impl BenchMode {
    fn label(self) -> &'static str {
        match self {
            Self::AllDiffs => "all",
            Self::NoDiffs => "none",
        }
    }

    fn capture(self) -> LaunchInstructionDiffCapture {
        match self {
            Self::AllDiffs => LaunchInstructionDiffCapture::All,
            Self::NoDiffs => LaunchInstructionDiffCapture::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayFingerprint {
    epoch: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    slots_processed: u64,
    compact_transactions: u64,
    compact_instructions: u64,
    transactions_processed: u64,
    failed_transactions: u64,
    first_failed_transaction: Option<LaunchDerivedTransactionFailure>,
    instructions_processed: u64,
    rolled_back_instructions: u64,
    vote_mutations: u64,
    config_mutations: u64,
    system_mutations: u64,
    stake_mutations: u64,
    bpf_loader_mutations: u64,
    bank_sysvar_writes: u64,
    bank_sysvar_accounts: usize,
    bank_sysvar_accounts_hash: [u8; 32],
    slot_hashes_unavailable: bool,
    changed_accounts: usize,
    changed_accounts_hash: [u8; 32],
    accounts: usize,
    state_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleKind {
    Timing,
    Allocations,
}

#[derive(Debug, Clone, Copy)]
enum Measurement {
    Timing(Duration),
    Allocations(AllocationSnapshot),
}

#[derive(Debug)]
struct Sample {
    mode: BenchMode,
    measurement: Measurement,
    emitted_diffs: u64,
    fingerprint: ReplayFingerprint,
}

impl Sample {
    fn elapsed(&self) -> Duration {
        match self.measurement {
            Measurement::Timing(elapsed) => elapsed,
            Measurement::Allocations(_) => panic!("expected a timing sample"),
        }
    }

    fn allocations(&self) -> AllocationSnapshot {
        match self.measurement {
            Measurement::Allocations(allocations) => allocations,
            Measurement::Timing(_) => panic!("expected an allocation sample"),
        }
    }
}

fn main() {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => return,
        Err(error) => {
            eprintln!("error: {error}\n");
            print_usage();
            process::exit(2);
        }
    };

    if let Err(error) = benchmark(&config) {
        eprintln!("error: {error}");
        process::exit(1);
    }
}

fn benchmark(config: &Config) -> Result<(), String> {
    println!(
        "replay-hotpath-bench: compact_v2={} start_slot={} max_slots={} warmups={} rounds={}",
        config.generation.display(),
        config
            .start_slot
            .map_or_else(|| "generation-start".to_owned(), |slot| slot.to_string()),
        config.max_slots,
        config.warmups,
        config.rounds,
    );
    if cfg!(debug_assertions) {
        println!("warning: debug build detected; rerun with --release for meaningful timings");
    }
    println!("timing samples: allocator counters disabled (no atomic counter increments)");
    println!(
        "allocation samples: separate replay runs count successful alloc/realloc calls and requested bytes"
    );

    match config.mode {
        ModeSelection::Both => benchmark_both_modes(config),
        ModeSelection::All => benchmark_single_mode(config, BenchMode::AllDiffs),
        ModeSelection::None => benchmark_single_mode(config, BenchMode::NoDiffs),
    }
}

fn benchmark_both_modes(config: &Config) -> Result<(), String> {
    for _ in 0..config.warmups {
        // Warm both the archive pages and each replay branch before sampling.
        let all = run_once(config, BenchMode::AllDiffs, SampleKind::Timing)?;
        let none = run_once(config, BenchMode::NoDiffs, SampleKind::Timing)?;
        ensure_equivalent(&all, &none)?;
        black_box((all.elapsed(), none.elapsed()));
    }

    let mut all_timing_samples = Vec::with_capacity(config.rounds);
    let mut none_timing_samples = Vec::with_capacity(config.rounds);
    let mut reference = None::<ReplayFingerprint>;

    for round in 0..config.rounds {
        let order = alternating_order(round);
        let first = run_once(config, order[0], SampleKind::Timing)?;
        let second = run_once(config, order[1], SampleKind::Timing)?;
        ensure_equivalent(&first, &second)?;

        for sample in [first, second] {
            ensure_matches_reference(&sample, &mut reference)?;
            match sample.mode {
                BenchMode::AllDiffs => all_timing_samples.push(sample),
                BenchMode::NoDiffs => none_timing_samples.push(sample),
            }
        }
    }

    let mut all_allocation_samples = Vec::with_capacity(config.rounds);
    let mut none_allocation_samples = Vec::with_capacity(config.rounds);
    for round in 0..config.rounds {
        let order = alternating_order(round);
        let first = run_once(config, order[0], SampleKind::Allocations)?;
        let second = run_once(config, order[1], SampleKind::Allocations)?;
        ensure_equivalent(&first, &second)?;

        for sample in [first, second] {
            ensure_matches_reference(&sample, &mut reference)?;
            match sample.mode {
                BenchMode::AllDiffs => all_allocation_samples.push(sample),
                BenchMode::NoDiffs => none_allocation_samples.push(sample),
            }
        }
    }

    let fingerprint = reference.expect("at least one measured replay");
    print_timing_summary(&all_timing_samples, &fingerprint);
    print_timing_summary(&none_timing_samples, &fingerprint);
    print_allocation_summary(&all_allocation_samples, &fingerprint);
    print_allocation_summary(&none_allocation_samples, &fingerprint);

    let all_median = median_duration(&all_timing_samples);
    let none_median = median_duration(&none_timing_samples);
    println!(
        "diff_capture_speedup={:.3}x (all median / none median)",
        all_median.as_secs_f64() / none_median.as_secs_f64(),
    );
    println!(
        "equivalence=PASS state_hash={} slots={} transactions={} compact_instructions={} committed_instructions={} failed_transactions={} first_failed_transaction={:?} changed_accounts={} changed_accounts_hash={} bank_sysvar_accounts={} bank_sysvar_accounts_hash={} accounts={}",
        hex(&fingerprint.state_hash),
        fingerprint.slots_processed,
        fingerprint.compact_transactions,
        fingerprint.compact_instructions,
        fingerprint.instructions_processed,
        fingerprint.failed_transactions,
        fingerprint.first_failed_transaction,
        fingerprint.changed_accounts,
        hex(&fingerprint.changed_accounts_hash),
        fingerprint.bank_sysvar_accounts,
        hex(&fingerprint.bank_sysvar_accounts_hash),
        fingerprint.accounts,
    );
    Ok(())
}

fn benchmark_single_mode(config: &Config, mode: BenchMode) -> Result<(), String> {
    for _ in 0..config.warmups {
        let sample = run_once(config, mode, SampleKind::Timing)?;
        black_box(sample.elapsed());
    }

    let mut timing_samples = Vec::with_capacity(config.rounds);
    let mut reference = None::<ReplayFingerprint>;
    for _ in 0..config.rounds {
        let sample = run_once(config, mode, SampleKind::Timing)?;
        ensure_matches_reference(&sample, &mut reference)?;
        timing_samples.push(sample);
    }

    let mut allocation_samples = Vec::with_capacity(config.rounds);
    for _ in 0..config.rounds {
        let sample = run_once(config, mode, SampleKind::Allocations)?;
        ensure_matches_reference(&sample, &mut reference)?;
        allocation_samples.push(sample);
    }

    let fingerprint = reference.expect("at least one measured replay");
    print_timing_summary(&timing_samples, &fingerprint);
    print_allocation_summary(&allocation_samples, &fingerprint);
    println!(
        "fingerprint=PASS mode={} state_hash={} slots={} transactions={} compact_instructions={} committed_instructions={} failed_transactions={} first_failed_transaction={:?} changed_accounts={} changed_accounts_hash={} bank_sysvar_accounts={} bank_sysvar_accounts_hash={} accounts={}",
        mode.label(),
        hex(&fingerprint.state_hash),
        fingerprint.slots_processed,
        fingerprint.compact_transactions,
        fingerprint.compact_instructions,
        fingerprint.instructions_processed,
        fingerprint.failed_transactions,
        fingerprint.first_failed_transaction,
        fingerprint.changed_accounts,
        hex(&fingerprint.changed_accounts_hash),
        fingerprint.bank_sysvar_accounts,
        hex(&fingerprint.bank_sysvar_accounts_hash),
        fingerprint.accounts,
    );
    Ok(())
}

fn alternating_order(round: usize) -> [BenchMode; 2] {
    if round.is_multiple_of(2) {
        [BenchMode::AllDiffs, BenchMode::NoDiffs]
    } else {
        [BenchMode::NoDiffs, BenchMode::AllDiffs]
    }
}

fn run_once(config: &Config, mode: BenchMode, kind: SampleKind) -> Result<Sample, String> {
    let visit_config = CompactVisitConfig {
        start_slot: config.start_slot,
        end_slot_exclusive: None,
        max_slots: Some(config.max_slots),
    };
    let mut emitted_diffs = 0_u64;
    let allocation_guard = (kind == SampleKind::Allocations).then(AllocationCountingGuard::start);
    let started = (kind == SampleKind::Timing).then(Instant::now);
    let outcome_result = visit_launch_prefix_diagnostic_with_diff_capture(
        &config.generation,
        visit_config,
        mode.capture(),
        |mutation| {
            emitted_diffs = emitted_diffs.wrapping_add(1);
            black_box(mutation);
        },
    );
    if let Ok(outcome) = &outcome_result {
        black_box(&outcome.replay.account_state);
    }
    let measurement = match (started, allocation_guard) {
        (Some(started), None) => Measurement::Timing(started.elapsed()),
        (None, Some(guard)) => Measurement::Allocations(guard.finish()),
        _ => unreachable!("sample kind selects exactly one measurement"),
    };
    let outcome = outcome_result
        .map_err(|error| format!("{} replay failed to start: {error}", mode.label()))?;

    if let Some(failure) = &outcome.failure {
        return Err(format!(
            "{} replay stopped at slot {} transaction {:?} instruction {:?}: {}",
            mode.label(),
            failure.location.slot,
            failure.location.transaction_index,
            failure.location.instruction_index,
            failure.error,
        ));
    }
    if outcome.compact_visit.instructions_visited == 0 {
        return Err("selected Compact prefix contains no instructions".to_owned());
    }
    if !outcome.replay.instruction_mutations.is_empty() {
        return Err("streaming replay unexpectedly retained instruction mutations".to_owned());
    }
    if mode == BenchMode::NoDiffs && emitted_diffs != 0 {
        return Err(format!(
            "None capture emitted {emitted_diffs} instruction diffs instead of zero"
        ));
    }

    // Hashing intentionally happens after both timed/allocation snapshots. It
    // verifies output equivalence without charging deterministic reporting work
    // to either replay mode.
    let fingerprint = fingerprint(&outcome);
    Ok(Sample {
        mode,
        measurement,
        emitted_diffs,
        fingerprint,
    })
}

fn fingerprint(outcome: &LaunchDiagnosticReplayOutcome) -> ReplayFingerprint {
    ReplayFingerprint {
        epoch: outcome.replay.epoch,
        first_slot: outcome.replay.first_slot,
        last_slot: outcome.replay.last_slot,
        slots_processed: outcome.replay.slots_processed,
        compact_transactions: outcome.compact_visit.transactions_visited,
        compact_instructions: outcome.compact_visit.instructions_visited,
        transactions_processed: outcome.replay.transactions_processed,
        failed_transactions: outcome.replay.failed_transactions,
        first_failed_transaction: outcome.replay.first_failed_transaction.clone(),
        instructions_processed: outcome.replay.instructions_processed,
        rolled_back_instructions: outcome.replay.rolled_back_instructions,
        vote_mutations: outcome.replay.vote_mutations,
        config_mutations: outcome.replay.config_mutations,
        system_mutations: outcome.replay.system_mutations,
        stake_mutations: outcome.replay.stake_mutations,
        bpf_loader_mutations: outcome.replay.bpf_loader_mutations,
        bank_sysvar_writes: outcome.replay.bank_sysvar_writes,
        bank_sysvar_accounts: outcome.replay.bank_sysvar_accounts_written.len(),
        bank_sysvar_accounts_hash: pubkey_set_hash(
            b"blockzilla-replay-bench-bank-sysvar-accounts-v1",
            &outcome.replay.bank_sysvar_accounts_written,
        ),
        slot_hashes_unavailable: outcome.replay.slot_hashes_unavailable,
        changed_accounts: outcome.replay.changed_accounts.len(),
        changed_accounts_hash: pubkey_set_hash(
            b"blockzilla-replay-bench-changed-accounts-v1",
            &outcome.replay.changed_accounts,
        ),
        accounts: outcome.replay.account_state.len(),
        state_hash: outcome.replay.account_state.canonical_hash(),
    }
}

fn pubkey_set_hash(domain: &[u8], pubkeys: &std::collections::BTreeSet<[u8; 32]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((pubkeys.len() as u64).to_le_bytes());
    for pubkey in pubkeys {
        hasher.update(pubkey);
    }
    hasher.finalize().into()
}

fn ensure_equivalent(left: &Sample, right: &Sample) -> Result<(), String> {
    if left.fingerprint == right.fingerprint {
        Ok(())
    } else {
        Err(format!(
            "{} and {} replay produced different state or counters: {:?} != {:?}",
            left.mode.label(),
            right.mode.label(),
            left.fingerprint,
            right.fingerprint,
        ))
    }
}

fn ensure_matches_reference(
    sample: &Sample,
    reference: &mut Option<ReplayFingerprint>,
) -> Result<(), String> {
    if let Some(expected) = reference {
        if sample.fingerprint != *expected {
            return Err(format!(
                "{} replay changed state or counters between samples: expected {expected:?}, found {:?}",
                sample.mode.label(),
                sample.fingerprint,
            ));
        }
    } else {
        *reference = Some(sample.fingerprint.clone());
    }
    Ok(())
}

fn print_timing_summary(samples: &[Sample], fingerprint: &ReplayFingerprint) {
    let operations = fingerprint.compact_instructions as f64;
    let elapsed = median_duration(samples);
    let emitted_diffs = deterministic_emitted_diffs(samples);
    let min = samples
        .iter()
        .map(Sample::elapsed)
        .min()
        .expect("at least one timing sample");
    let max = samples
        .iter()
        .map(Sample::elapsed)
        .max()
        .expect("at least one timing sample");

    println!(
        "timing mode={:<4} median={:>10.3} ms range={:>10.3}..{:>10.3} ms ns/instruction={:>10.1} diffs_emitted={}",
        samples[0].mode.label(),
        millis(elapsed),
        millis(min),
        millis(max),
        elapsed.as_nanos() as f64 / operations,
        emitted_diffs,
    );
}

fn print_allocation_summary(samples: &[Sample], fingerprint: &ReplayFingerprint) {
    let operations = fingerprint.compact_instructions as f64;
    let allocations = median_u64(samples.iter().map(|sample| sample.allocations().calls));
    let allocated_bytes = median_u64(samples.iter().map(|sample| sample.allocations().bytes));
    let emitted_diffs = deterministic_emitted_diffs(samples);

    println!(
        "allocations mode={:<4} calls={} calls/instruction={:.4} requested_bytes={} bytes/instruction={:.1} diffs_emitted={}",
        samples[0].mode.label(),
        allocations,
        allocations as f64 / operations,
        allocated_bytes,
        allocated_bytes as f64 / operations,
        emitted_diffs,
    );
}

fn deterministic_emitted_diffs(samples: &[Sample]) -> u64 {
    let emitted_diffs = samples[0].emitted_diffs;
    assert!(
        samples
            .iter()
            .all(|sample| sample.emitted_diffs == emitted_diffs),
        "diff emission count must be deterministic"
    );
    emitted_diffs
}

fn median_duration(samples: &[Sample]) -> Duration {
    let mut values = samples.iter().map(Sample::elapsed).collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_u64(values: impl Iterator<Item = u64>) -> u64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn parse_args() -> Result<Option<Config>, String> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from(args: impl IntoIterator<Item = String>) -> Result<Option<Config>, String> {
    let mut config = Config {
        generation: PathBuf::new(),
        start_slot: None,
        max_slots: DEFAULT_MAX_SLOTS,
        warmups: DEFAULT_WARMUPS,
        rounds: DEFAULT_ROUNDS,
        mode: ModeSelection::Both,
    };
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument == "--help" || argument == "-h" {
            print_usage();
            return Ok(None);
        }
        if !argument.starts_with('-') {
            if config.generation.as_os_str().is_empty() {
                config.generation = PathBuf::from(argument);
                continue;
            }
            return Err(format!("unexpected positional argument: {argument}"));
        }

        let (name, inline_value) = argument
            .split_once('=')
            .map_or((argument.as_str(), None), |(name, value)| {
                (name, Some(value))
            });
        let value = match inline_value {
            Some(value) => value.to_owned(),
            None => args
                .next()
                .ok_or_else(|| format!("missing value for {name}"))?,
        };
        match name {
            "--start-slot" => {
                config.start_slot = Some(parse_integer::<u64>(name, &value)?);
            }
            "--max-slots" => config.max_slots = parse_integer(name, &value)?,
            "--warmups" => config.warmups = parse_integer(name, &value)?,
            "--rounds" => config.rounds = parse_integer(name, &value)?,
            "--mode" => config.mode = ModeSelection::parse(&value)?,
            _ => return Err(format!("unknown argument: {name}")),
        }
    }

    if config.generation.as_os_str().is_empty() {
        return Err("missing Compact V2 generation directory".to_owned());
    }
    if config.max_slots == 0 {
        return Err("--max-slots must be greater than zero".to_owned());
    }
    if config.rounds == 0 {
        return Err("--rounds must be greater than zero".to_owned());
    }
    Ok(Some(config))
}

fn parse_integer<T>(name: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse::<T>()
        .map_err(|_| format!("invalid non-negative integer for {name}: {value}"))
}

fn print_usage() {
    println!(
        "Usage: replay-hotpath-bench <compact-v2-generation> [--start-slot SLOT] \
         [--max-slots N] [--warmups N] [--rounds N] [--mode both|all|none]\n\
         Defaults: --max-slots {DEFAULT_MAX_SLOTS} --warmups {DEFAULT_WARMUPS} \
         --rounds {DEFAULT_ROUNDS} --mode both\n\
         This benchmark accepts only a Blockzilla Compact V2 generation directory."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Result<Option<Config>, String> {
        parse_args_from(arguments.iter().map(|argument| (*argument).to_owned()))
    }

    #[test]
    fn mode_defaults_to_both() {
        let config = parse(&["compact-generation"]).unwrap().unwrap();
        assert_eq!(config.mode, ModeSelection::Both);
    }

    #[test]
    fn parses_each_mode_with_separate_or_inline_value() {
        for (arguments, expected) in [
            (
                &["compact-generation", "--mode", "both"][..],
                ModeSelection::Both,
            ),
            (
                &["compact-generation", "--mode=all"][..],
                ModeSelection::All,
            ),
            (
                &["--mode", "none", "compact-generation"][..],
                ModeSelection::None,
            ),
        ] {
            let config = parse(arguments).unwrap().unwrap();
            assert_eq!(config.mode, expected);
        }
    }

    #[test]
    fn rejects_unknown_mode() {
        let error = parse(&["compact-generation", "--mode", "fast"]).unwrap_err();
        assert_eq!(
            error,
            "invalid value for --mode: fast (expected both, all, or none)"
        );
    }
}
