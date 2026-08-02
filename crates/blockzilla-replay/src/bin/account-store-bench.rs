//! Cardinality sweep for the in-memory replay account store.
//!
//! The default sweep includes the 54,339-account epoch-77 live registry and
//! larger planning points through one million accounts. Setup, workload
//! generation, full-state hashing, and atomic-batch staging are outside timed
//! and allocation-counted regions. Every mutating workload is reversible, and
//! a canonical pre/post state hash proves that a cardinality starts and ends in
//! identical state.
//!
//! Run with optimizations enabled:
//!
//! ```text
//! cargo run --release -p blockzilla-replay --bin account-store-bench
//! ```
//!
//! Select one cardinality with the backwards-compatible `--accounts` option,
//! or supply a comma-separated sweep with `--cardinalities`.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    env,
    fmt::Write as _,
    hint::black_box,
    process,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use blockzilla_replay::{
    AccountBatchCommit, AccountSnapshot, AccountWriteBatch, MemoryAccountStore,
};

const DEFAULT_CARDINALITIES: &[usize] =
    &[1_000, 10_000, 54_339, 100_000, 250_000, 500_000, 1_000_000];
const DEFAULT_LOOKUPS: usize = 1_000_000;
const DEFAULT_UPDATES: usize = 250_000;
const DEFAULT_CHURN_PAIRS: usize = 100_000;
const DEFAULT_BATCH_WRITES: usize = 250_000;
const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_DATA_BYTES: usize = 128;
const DEFAULT_ROUNDS: usize = 5;

const LOOKUP_SEED: u64 = 0x7a31_3d89_2ce4_65bf;
const UPDATE_SEED: u64 = 0xc6bc_2796_92b5_cc83;
const UPDATE_DELTA_SEED: u64 = 0xd1b5_4a32_d192_ed03;
const TRANSIENT_KEY_BASE: u64 = 1_u64 << 63;
const BATCH_INSERT_KEY_BASE: u64 = 3_u64 << 62;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplied this exact allocation layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplied this exact allocation layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: pointer and layout came from this allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: pointer and layout came from this allocator.
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    cardinalities: Vec<usize>,
    lookups: usize,
    updates: usize,
    churn_pairs: usize,
    batch_writes: usize,
    batch_size: usize,
    data_bytes: usize,
    rounds: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AllocationStats {
    calls: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct RawSample {
    elapsed: Duration,
    allocations: AllocationStats,
    fingerprint: u64,
}

#[derive(Debug, Clone, Copy)]
struct Measurement {
    median: Duration,
    allocations: AllocationStats,
    fingerprint: u64,
}

#[derive(Debug, Clone, Copy)]
struct BatchShape {
    updates: usize,
    deletes: usize,
    inserts: usize,
}

impl BatchShape {
    fn new(batch_size: usize) -> Self {
        let updates = batch_size / 2;
        let deletes = (batch_size - updates) / 2;
        let inserts = batch_size - updates - deletes;
        Self {
            updates,
            deletes,
            inserts,
        }
    }

    fn existing_accounts(self) -> usize {
        self.updates + self.deletes
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

    println!(
        "account-store-bench cardinalities={} lookups={} updates={} churn_pairs={} batch_writes_target={} batch_size={} data_bytes={} rounds={}",
        join_cardinalities(&config.cardinalities),
        config.lookups,
        config.updates,
        config.churn_pairs,
        config.batch_writes,
        config.batch_size,
        config.data_bytes,
        config.rounds,
    );
    println!(
        "measurement_scope=store-operations-only batch_staging=excluded canonical_hashing=excluded insert_delete_unit=individual-map-mutation"
    );
    if cfg!(debug_assertions) {
        println!("warning=debug-build rerun_with=--release");
    }

    for &cardinality in &config.cardinalities {
        if let Err(error) = run_cardinality(&config, cardinality) {
            eprintln!("error: cardinality {cardinality}: {error}");
            process::exit(1);
        }
    }
}

fn run_cardinality(config: &Config, cardinality: usize) -> Result<(), String> {
    let batch_shape = BatchShape::new(config.batch_size);
    if batch_shape.existing_accounts() > cardinality {
        return Err(format!(
            "--batch-size {} needs {} existing accounts, but cardinality is {cardinality}",
            config.batch_size,
            batch_shape.existing_accounts(),
        ));
    }
    let reserved = cardinality
        .checked_add(batch_shape.inserts.max(1))
        .ok_or_else(|| "account-store capacity overflow".to_owned())?;
    let keys = generate_keys(cardinality);
    let build_started = Instant::now();
    let mut accounts = build_store(&keys, config.data_bytes, reserved);
    let build_elapsed = build_started.elapsed();
    if accounts.len() != cardinality {
        return Err("deterministic pubkeys were not unique".to_owned());
    }

    let initial_hash = accounts.canonical_hash();
    println!(
        "cardinality={cardinality} phase=setup build_ms={:.3} reserved={} state_sha256={}",
        millis(build_elapsed),
        reserved,
        hex(&initial_hash),
    );

    let lookup_indices = generate_indices(config.lookups, cardinality, LOOKUP_SEED);
    let update_indices = generate_indices(config.updates, cardinality, UPDATE_SEED);
    let transient_keys = (0..config.churn_pairs)
        .map(|index| deterministic_key(TRANSIENT_KEY_BASE + index as u64))
        .collect::<Vec<_>>();
    let (forward_batch, inverse_batch) =
        make_batch_templates(&accounts, &keys, batch_shape, config.data_bytes)?;
    let batch_count = batch_count(config.batch_writes, config.batch_size);
    let actual_batch_writes = batch_count
        .checked_mul(config.batch_size)
        .ok_or_else(|| "batch write count overflow".to_owned())?;

    let lookup_warmup = lookup_indices.len().min(10_000);
    black_box(lookup_accounts(
        &accounts,
        &keys,
        &lookup_indices[..lookup_warmup],
    ));
    let update_warmup = update_indices.len().min(1_000);
    black_box(toggle_updates(
        &mut accounts,
        &keys,
        &update_indices[..update_warmup],
    ));
    black_box(toggle_updates(
        &mut accounts,
        &keys,
        &update_indices[..update_warmup],
    ));
    let churn_warmup = transient_keys.len().min(1_000);
    let mut warmup_transient_account =
        Some(deterministic_account(TRANSIENT_KEY_BASE, config.data_bytes));
    black_box(insert_delete_pairs(
        &mut accounts,
        &transient_keys[..churn_warmup],
        &mut warmup_transient_account,
    ));
    black_box(apply_batch_sequence(
        &mut accounts,
        vec![forward_batch.clone(), inverse_batch.clone()],
    )?);

    let lookup = benchmark_operation(config.rounds, |count_allocations| {
        measure_once(count_allocations, || {
            lookup_accounts(&accounts, &keys, &lookup_indices)
        })
    })?;

    let update = benchmark_operation(config.rounds, |count_allocations| {
        let sample = measure_once(count_allocations, || {
            toggle_updates(&mut accounts, &keys, &update_indices)
        });
        black_box(toggle_updates(&mut accounts, &keys, &update_indices));
        sample
    })?;

    let churn = benchmark_operation(config.rounds, |count_allocations| {
        let mut transient_account =
            Some(deterministic_account(TRANSIENT_KEY_BASE, config.data_bytes));
        measure_once(count_allocations, || {
            insert_delete_pairs(&mut accounts, &transient_keys, &mut transient_account)
        })
    })?;

    let batch = benchmark_operation(config.rounds, |count_allocations| {
        let staged = stage_batch_sequence(&forward_batch, &inverse_batch, batch_count);
        measure_once(count_allocations, || {
            apply_batch_sequence(&mut accounts, staged)
                .expect("validated benchmark batches remain applicable")
        })
    })?;

    print_measurement(
        cardinality,
        "lookup-hit",
        config.lookups,
        "lookups",
        &lookup,
        "",
    );
    print_measurement(
        cardinality,
        "direct-update",
        config.updates,
        "updates",
        &update,
        "",
    );
    print_measurement(
        cardinality,
        "insert-delete",
        config.churn_pairs.saturating_mul(2),
        "map-mutations",
        &churn,
        &format!("pairs={}", config.churn_pairs),
    );
    print_measurement(
        cardinality,
        "batch-commit",
        actual_batch_writes,
        "writes",
        &batch,
        &format!("batches={batch_count} batch_size={}", config.batch_size),
    );

    let final_hash = accounts.canonical_hash();
    if final_hash != initial_hash {
        return Err(format!(
            "reversible workloads changed canonical state: before={} after={}",
            hex(&initial_hash),
            hex(&final_hash),
        ));
    }
    println!(
        "cardinality={cardinality} phase=complete final_state_sha256={} state_equivalence=PASS",
        hex(&final_hash),
    );
    Ok(())
}

fn benchmark_operation(
    rounds: usize,
    mut run: impl FnMut(bool) -> RawSample,
) -> Result<Measurement, String> {
    let mut samples = Vec::with_capacity(rounds);
    let mut fingerprint = None;
    for _ in 0..rounds {
        let sample = run(false);
        validate_fingerprint(&mut fingerprint, sample.fingerprint)?;
        samples.push(sample.elapsed);
    }
    let allocation_sample = run(true);
    validate_fingerprint(&mut fingerprint, allocation_sample.fingerprint)?;
    samples.sort_unstable();
    Ok(Measurement {
        median: samples[samples.len() / 2],
        allocations: allocation_sample.allocations,
        fingerprint: fingerprint.expect("positive benchmark round count"),
    })
}

fn validate_fingerprint(expected: &mut Option<u64>, found: u64) -> Result<(), String> {
    match expected {
        Some(expected) if *expected != found => Err(format!(
            "operation fingerprint changed between samples: expected {expected:016x}, found {found:016x}",
        )),
        Some(_) => Ok(()),
        None => {
            *expected = Some(found);
            Ok(())
        }
    }
}

fn measure_once(count_allocations: bool, operation: impl FnOnce() -> u64) -> RawSample {
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(count_allocations, Ordering::Relaxed);
    let started = Instant::now();
    let fingerprint = black_box(operation());
    let elapsed = started.elapsed();
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    RawSample {
        elapsed,
        allocations: AllocationStats {
            calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
            bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        },
        fingerprint,
    }
}

fn generate_keys(count: usize) -> Vec<[u8; 32]> {
    (0..count)
        .map(|index| deterministic_key(index as u64))
        .collect()
}

fn build_store(keys: &[[u8; 32]], data_bytes: usize, capacity: usize) -> MemoryAccountStore {
    let mut accounts = MemoryAccountStore::with_capacity(capacity);
    for (index, &pubkey) in keys.iter().enumerate() {
        let replaced = accounts.insert(pubkey, deterministic_account(index as u64, data_bytes));
        assert!(
            replaced.is_none(),
            "deterministic benchmark keys are unique"
        );
    }
    accounts
}

fn deterministic_account(index: u64, data_bytes: usize) -> AccountSnapshot {
    let owner = deterministic_key(index ^ 0xa076_1d64_78bd_642f);
    let data = (0..data_bytes)
        .map(|offset| mix64(index.wrapping_add((offset as u64).wrapping_mul(0x9e37))) as u8)
        .collect();
    AccountSnapshot {
        lamports: mix64(index) | 1,
        owner,
        executable: index.is_multiple_of(97),
        rent_epoch: index % 432,
        data,
    }
}

fn generate_indices(count: usize, account_count: usize, seed: u64) -> Vec<usize> {
    let mut state = seed;
    (0..count)
        .map(|_| {
            state = mix64(state);
            (state as usize) % account_count
        })
        .collect()
}

fn deterministic_key(index: u64) -> [u8; 32] {
    // SplitMix64 is bijective for the first word, guaranteeing unique keys for
    // every distinct benchmark index while randomizing hash-table placement.
    let mut pubkey = [0_u8; 32];
    let mut state = index ^ 0xe703_7ed1_a0b4_28db;
    for chunk in pubkey.chunks_exact_mut(8) {
        state = mix64(state);
        chunk.copy_from_slice(&state.to_be_bytes());
    }
    pubkey
}

fn lookup_accounts(accounts: &MemoryAccountStore, keys: &[[u8; 32]], indices: &[usize]) -> u64 {
    indices.iter().fold(0_u64, |fingerprint, &index| {
        let account = accounts
            .get(black_box(&keys[index]))
            .expect("lookup key was generated from this store");
        fold_account(fingerprint, account)
    })
}

fn toggle_updates(accounts: &mut MemoryAccountStore, keys: &[[u8; 32]], indices: &[usize]) -> u64 {
    let mut fingerprint = 0_u64;
    for (operation, &index) in indices.iter().enumerate() {
        let account = accounts
            .get_mut(black_box(&keys[index]))
            .expect("update key was generated from this store");
        account.lamports ^= mix64(UPDATE_DELTA_SEED ^ operation as u64) | 1;
        fingerprint = fold_account(fingerprint, account);
    }
    fingerprint
}

fn insert_delete_pairs(
    accounts: &mut MemoryAccountStore,
    transient_keys: &[[u8; 32]],
    transient_account: &mut Option<AccountSnapshot>,
) -> u64 {
    let mut fingerprint = 0_u64;
    for pubkey in transient_keys {
        let account = transient_account
            .take()
            .expect("one transient account moves between insert/delete pairs");
        let replaced = accounts.insert(*pubkey, account);
        assert!(replaced.is_none(), "transient key unexpectedly existed");
        *transient_account = Some(
            accounts
                .remove(pubkey)
                .expect("transient account was inserted immediately before removal"),
        );
        fingerprint = fold_account(
            fingerprint ^ key_prefix(pubkey),
            transient_account
                .as_ref()
                .expect("removed transient account remains staged"),
        );
    }
    black_box(fingerprint)
}

fn make_batch_templates(
    accounts: &MemoryAccountStore,
    keys: &[[u8; 32]],
    shape: BatchShape,
    data_bytes: usize,
) -> Result<(AccountWriteBatch, AccountWriteBatch), String> {
    let mut forward = AccountWriteBatch::new();
    let mut inverse = AccountWriteBatch::new();

    for (index, &pubkey) in keys[..shape.updates].iter().enumerate() {
        let original = accounts
            .get(&pubkey)
            .expect("batch update key exists")
            .clone();
        let mut updated = original.clone();
        updated.lamports ^= mix64(UPDATE_DELTA_SEED ^ index as u64) | 1;
        forward
            .put(pubkey, updated)
            .map_err(|error| error.to_string())?;
        inverse
            .put(pubkey, original)
            .map_err(|error| error.to_string())?;
    }

    let delete_start = shape.updates;
    let delete_end = delete_start + shape.deletes;
    for &pubkey in &keys[delete_start..delete_end] {
        let original = accounts
            .get(&pubkey)
            .expect("batch delete key exists")
            .clone();
        forward.delete(pubkey).map_err(|error| error.to_string())?;
        inverse
            .put(pubkey, original)
            .map_err(|error| error.to_string())?;
    }

    for index in 0..shape.inserts {
        let account_index = BATCH_INSERT_KEY_BASE + index as u64;
        let pubkey = deterministic_key(account_index);
        forward
            .put(pubkey, deterministic_account(account_index, data_bytes))
            .map_err(|error| error.to_string())?;
        inverse.delete(pubkey).map_err(|error| error.to_string())?;
    }

    debug_assert_eq!(forward.len(), shape.updates + shape.deletes + shape.inserts);
    debug_assert_eq!(inverse.len(), forward.len());
    Ok((forward, inverse))
}

fn batch_count(target_writes: usize, batch_size: usize) -> usize {
    let batches = target_writes.div_ceil(batch_size).max(2);
    if batches.is_multiple_of(2) {
        batches
    } else {
        batches + 1
    }
}

fn stage_batch_sequence(
    forward: &AccountWriteBatch,
    inverse: &AccountWriteBatch,
    count: usize,
) -> Vec<AccountWriteBatch> {
    (0..count)
        .map(|index| {
            if index.is_multiple_of(2) {
                forward.clone()
            } else {
                inverse.clone()
            }
        })
        .collect()
}

fn apply_batch_sequence(
    accounts: &mut MemoryAccountStore,
    batches: Vec<AccountWriteBatch>,
) -> Result<u64, String> {
    let mut fingerprint = 0_u64;
    for (index, batch) in batches.into_iter().enumerate() {
        let commit = accounts
            .apply_batch(batch)
            .map_err(|error| error.to_string())?;
        fingerprint = fold_commit(fingerprint, index, commit);
    }
    Ok(black_box(fingerprint))
}

fn fold_commit(previous: u64, index: usize, commit: AccountBatchCommit) -> u64 {
    previous.rotate_left(9)
        ^ mix64(index as u64)
        ^ (commit.inserted as u64).rotate_left(7)
        ^ (commit.updated as u64).rotate_left(19)
        ^ (commit.deleted as u64).rotate_left(31)
        ^ (commit.patched as u64).rotate_left(43)
}

fn fold_account(fingerprint: u64, account: &AccountSnapshot) -> u64 {
    let first = account.data.first().copied().unwrap_or_default() as u64;
    let last = account.data.last().copied().unwrap_or_default() as u64;
    fingerprint.rotate_left(7)
        ^ account.lamports
        ^ u64::from_le_bytes(
            account.owner[..8]
                .try_into()
                .expect("eight-byte owner prefix"),
        )
        ^ account.rent_epoch.rotate_left(17)
        ^ (account.executable as u64)
        ^ (account.data.len() as u64).rotate_left(29)
        ^ first.rotate_left(41)
        ^ last.rotate_left(53)
}

fn key_prefix(pubkey: &[u8; 32]) -> u64 {
    u64::from_le_bytes(pubkey[..8].try_into().expect("eight-byte pubkey prefix"))
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn print_measurement(
    cardinality: usize,
    operation: &str,
    operations: usize,
    operation_unit: &str,
    measurement: &Measurement,
    extra: &str,
) {
    let seconds = measurement.median.as_secs_f64();
    println!(
        "cardinality={cardinality} operation={operation} {extra} operations={operations} operation_unit={operation_unit} median_ms={:.3} ns_per_operation={:.1} operations_per_second={:.0} allocation_calls={} calls_per_operation={:.6} allocated_bytes={} bytes_per_operation={:.3} fingerprint={:016x}",
        millis(measurement.median),
        measurement.median.as_nanos() as f64 / operations as f64,
        operations as f64 / seconds,
        measurement.allocations.calls,
        measurement.allocations.calls as f64 / operations as f64,
        measurement.allocations.bytes,
        measurement.allocations.bytes as f64 / operations as f64,
        measurement.fingerprint,
    );
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

fn join_cardinalities(cardinalities: &[usize]) -> String {
    cardinalities
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_args() -> Result<Option<Config>, String> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from(arguments: impl IntoIterator<Item = String>) -> Result<Option<Config>, String> {
    let mut config = Config {
        cardinalities: DEFAULT_CARDINALITIES.to_vec(),
        lookups: DEFAULT_LOOKUPS,
        updates: DEFAULT_UPDATES,
        churn_pairs: DEFAULT_CHURN_PAIRS,
        batch_writes: DEFAULT_BATCH_WRITES,
        batch_size: DEFAULT_BATCH_SIZE,
        data_bytes: DEFAULT_DATA_BYTES,
        rounds: DEFAULT_ROUNDS,
    };
    let mut cardinality_option = None;
    let mut args = arguments.into_iter();

    while let Some(argument) = args.next() {
        if argument == "--help" || argument == "-h" {
            print_usage();
            return Ok(None);
        }

        let (name, inline_value) = argument
            .split_once('=')
            .map_or((argument.as_str(), None), |(name, value)| {
                (name, Some(value))
            });
        let value_text = match inline_value {
            Some(value) => value.to_owned(),
            None => args
                .next()
                .ok_or_else(|| format!("missing value for {name}"))?,
        };

        match name {
            "--accounts" => {
                set_cardinality_option(&mut cardinality_option, "--accounts")?;
                config.cardinalities = vec![parse_usize(name, &value_text)?];
            }
            "--cardinalities" => {
                set_cardinality_option(&mut cardinality_option, "--cardinalities")?;
                config.cardinalities = parse_cardinalities(&value_text)?;
            }
            "--lookups" => config.lookups = parse_usize(name, &value_text)?,
            "--updates" => config.updates = parse_usize(name, &value_text)?,
            "--churn-pairs" => config.churn_pairs = parse_usize(name, &value_text)?,
            "--batch-writes" => config.batch_writes = parse_usize(name, &value_text)?,
            "--batch-size" => config.batch_size = parse_usize(name, &value_text)?,
            "--data-bytes" => config.data_bytes = parse_usize(name, &value_text)?,
            "--rounds" => config.rounds = parse_usize(name, &value_text)?,
            _ => return Err(format!("unknown argument: {name}")),
        }
    }

    config.cardinalities.sort_unstable();
    config.cardinalities.dedup();
    if config.cardinalities.is_empty() || config.cardinalities[0] == 0 {
        return Err("account cardinalities must be greater than zero".to_owned());
    }
    if config.lookups == 0 {
        return Err("--lookups must be greater than zero".to_owned());
    }
    if config.updates == 0 {
        return Err("--updates must be greater than zero".to_owned());
    }
    if config.churn_pairs == 0 {
        return Err("--churn-pairs must be greater than zero".to_owned());
    }
    if config.batch_writes == 0 {
        return Err("--batch-writes must be greater than zero".to_owned());
    }
    if config.batch_size < 4 {
        return Err("--batch-size must be at least four for a mixed batch".to_owned());
    }
    if config.rounds == 0 {
        return Err("--rounds must be greater than zero".to_owned());
    }

    Ok(Some(config))
}

fn set_cardinality_option(
    selected: &mut Option<&'static str>,
    option: &'static str,
) -> Result<(), String> {
    if let Some(previous) = selected.replace(option) {
        return Err(format!("{previous} and {option} cannot be used together"));
    }
    Ok(())
}

fn parse_usize(name: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid non-negative integer for {name}: {value}"))
}

fn parse_cardinalities(value: &str) -> Result<Vec<usize>, String> {
    if value.is_empty() {
        return Err("--cardinalities requires at least one value".to_owned());
    }
    value
        .split(',')
        .map(|item| parse_usize("--cardinalities", item))
        .collect()
}

fn print_usage() {
    println!("Usage: account-store-bench [--accounts N | --cardinalities N,N,...]");
    println!("       [--lookups N] [--updates N] [--churn-pairs N] [--batch-writes N]");
    println!("       [--batch-size N] [--data-bytes N] [--rounds N]");
    println!(
        "Default cardinalities: {}",
        join_cardinalities(DEFAULT_CARDINALITIES)
    );
    println!("Operation defaults: --lookups {DEFAULT_LOOKUPS} --updates {DEFAULT_UPDATES}");
    println!(
        "                    --churn-pairs {DEFAULT_CHURN_PAIRS} --batch-writes {DEFAULT_BATCH_WRITES}"
    );
    println!(
        "                    --batch-size {DEFAULT_BATCH_SIZE} --data-bytes {DEFAULT_DATA_BYTES} --rounds {DEFAULT_ROUNDS}"
    );
    println!("Batch staging is excluded from batch-commit measurements; run with --release.");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn defaults_include_exact_live_registry_and_planning_cardinalities() {
        let config = parse_args_from(Vec::new()).unwrap().unwrap();
        assert_eq!(config.cardinalities, DEFAULT_CARDINALITIES);
        assert!(config.cardinalities.contains(&54_339));
        assert!(config.cardinalities.contains(&1_000_000));
    }

    #[test]
    fn single_and_sweep_cardinality_options_are_unambiguous() {
        let single = parse_args_from(args(&["--accounts", "54339"]))
            .unwrap()
            .unwrap();
        assert_eq!(single.cardinalities, [54_339]);

        let sweep = parse_args_from(args(&["--cardinalities=100000,1000,54339,1000"]))
            .unwrap()
            .unwrap();
        assert_eq!(sweep.cardinalities, [1_000, 54_339, 100_000]);

        assert!(
            parse_args_from(args(&[
                "--accounts",
                "54339",
                "--cardinalities",
                "1000,10000",
            ]))
            .is_err()
        );
    }

    #[test]
    fn reversible_mutation_workloads_restore_canonical_state() {
        let keys = generate_keys(64);
        let shape = BatchShape::new(8);
        let mut accounts = build_store(&keys, 16, 64 + shape.inserts);
        let initial_hash = accounts.canonical_hash();
        let indices = generate_indices(200, keys.len(), UPDATE_SEED);

        let first = toggle_updates(&mut accounts, &keys, &indices);
        let second = toggle_updates(&mut accounts, &keys, &indices);
        assert_ne!(first, second);

        let transient = (0..32)
            .map(|index| deterministic_key(TRANSIENT_KEY_BASE + index))
            .collect::<Vec<_>>();
        let mut transient_account = Some(deterministic_account(TRANSIENT_KEY_BASE, 16));
        assert_ne!(
            insert_delete_pairs(&mut accounts, &transient, &mut transient_account),
            0
        );

        let (forward, inverse) = make_batch_templates(&accounts, &keys, shape, 16).unwrap();
        let fingerprint =
            apply_batch_sequence(&mut accounts, stage_batch_sequence(&forward, &inverse, 4))
                .unwrap();
        assert_ne!(fingerprint, 0);
        assert_eq!(accounts.len(), 64);
        assert_eq!(accounts.canonical_hash(), initial_hash);
    }
}
