//! Microbenchmark for replay transaction-state costs.
//!
//! This harness isolates the state-management work around instruction
//! execution. It does not decode compact blocks or execute a program. The
//! deterministic sweep covers:
//!
//! - read-set transaction-overlay construction (production `AccountMap` and
//!   a side-by-side capacity-reserved hash-map candidate);
//! - writable-account clone/mutate/staging in both map layouts;
//! - `AccountWriteBatch` staging separately from atomic publication;
//! - `InstructionDiff` materialization;
//! - changed-key insertion into the runtime's `BTreeSet` and a capacity-
//!   reserved hash-set candidate.
//!
//! Setup, deterministic workload generation, publication-batch preparation,
//! and canonical hashing are outside timed and allocation-counted regions.
//! Timing and allocation samples are separate so the counting atomics do not
//! distort median latency. Every implementation pair is checked by a sorted,
//! canonical SHA-256 fingerprint before measurements are reported.
//!
//! Recommended run:
//!
//! ```text
//! cargo run --release -p blockzilla-replay --bin replay-state-bench
//! ```
//!
//! A focused epoch-era shape, including a larger account payload:
//!
//! ```text
//! cargo run --release -p blockzilla-replay --bin replay-state-bench -- \
//!   --accounts 54339 --data-sizes 128,1024,8192 --read-set 32 \
//!   --writable 8 --changed-events 64 --iterations 2000 --rounds 5
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
    hint::black_box,
    process,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use blockzilla_replay::{
    AccountBatchCommit, AccountMap, AccountSnapshot, AccountWriteBatch, DiffBoundary,
    DiffDisposition, DiffPolicy, InlineInstructionPath, InstructionDiff, MemoryAccountStore,
};
use hashbrown::{HashMap, HashSet};
use sha2::{Digest, Sha256};

type Pubkey = [u8; 32];
/// Legacy ordered overlay retained only for A/B comparison in this bench.
type BTreeOverlay = BTreeMap<Pubkey, AccountSnapshot>;
/// Production transaction overlay (hashbrown).
type ProdOverlay = AccountMap;
type HashOverlay = HashMap<Pubkey, AccountSnapshot>;

const DEFAULT_CARDINALITIES: &[usize] = &[1_000, 54_339];
const DEFAULT_DATA_SIZES: &[usize] = &[0, 128, 1_024];
const DEFAULT_READ_SET: usize = 32;
const DEFAULT_WRITABLE: usize = 8;
const DEFAULT_CHANGED_EVENTS: usize = 64;
const DEFAULT_ITERATIONS: usize = 2_000;
const DEFAULT_ROUNDS: usize = 5;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_INLINE_DIFF_BYTES: usize = 4 * 1_024;
const DEFAULT_SEED: u64 = 0x4d59_5df4_d0f3_3173;

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
    data_sizes: Vec<usize>,
    read_set: usize,
    writable: usize,
    changed_events: usize,
    iterations: usize,
    rounds: usize,
    warmups: usize,
    inline_diff_bytes: usize,
    seed: u64,
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

struct ShapeWorkload {
    accounts: MemoryAccountStore,
    read_keys: Vec<Pubkey>,
    writable_keys: Vec<Pubkey>,
    before_writable: Vec<(Pubkey, AccountSnapshot)>,
    after_writable: Vec<(Pubkey, AccountSnapshot)>,
    diff_before: ProdOverlay,
    diff_after: ProdOverlay,
    changed_base_keys: Vec<Pubkey>,
    changed_events: Vec<Pubkey>,
    changed_final_len: usize,
    workload_hash: [u8; 32],
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
        "replay-state-bench cardinalities={} data_sizes={} read_set={} writable={} changed_events={} iterations={} rounds={} warmups={} inline_diff_bytes={} seed=0x{:016x}",
        join_usizes(&config.cardinalities),
        join_usizes(&config.data_sizes),
        config.read_set,
        config.writable,
        config.changed_events,
        config.iterations,
        config.rounds,
        config.warmups,
        config.inline_diff_bytes,
        config.seed,
    );
    println!(
        "measurement_scope=operation-only setup=excluded canonical_hashing=excluded publication_batch_preparation=excluded timing_and_allocation_samples=separate allocation_bytes=requested_bytes"
    );
    if cfg!(debug_assertions) {
        println!("warning=debug-build rerun_with=--release");
    }

    for &cardinality in &config.cardinalities {
        for &data_bytes in &config.data_sizes {
            if let Err(error) = run_shape(&config, cardinality, data_bytes) {
                eprintln!("error: cardinality={cardinality} data_bytes={data_bytes}: {error}");
                process::exit(1);
            }
        }
    }
}

fn run_shape(config: &Config, cardinality: usize, data_bytes: usize) -> Result<(), String> {
    let setup_started = Instant::now();
    let workload = build_workload(config, cardinality, data_bytes)?;
    let account_state_hash = workload.accounts.canonical_hash();
    let setup_elapsed = setup_started.elapsed();

    println!(
        "shape cardinality={cardinality} data_bytes={data_bytes} phase=setup setup_ms={:.3} account_state_sha256={} workload_sha256={}",
        millis(setup_elapsed),
        hex(&account_state_hash),
        hex(&workload.workload_hash),
    );

    let current_read = build_read_overlay_btree(&workload.accounts, &workload.read_keys);
    let candidate_read = build_read_overlay_hash(&workload.accounts, &workload.read_keys);
    let current_read_hash = hash_btree_overlay(&current_read);
    let candidate_read_hash = hash_hash_overlay(&candidate_read);
    require_equal_hash("read-overlay", current_read_hash, candidate_read_hash)?;
    println!(
        "equivalence=PASS cardinality={cardinality} data_bytes={data_bytes} group=read-overlay current=btree candidate=hash-reserved canonical_sha256={}",
        hex(&current_read_hash),
    );

    let current_writable = stage_writable_btree(&workload.accounts, &workload.writable_keys);
    let candidate_writable = stage_writable_hash(&workload.accounts, &workload.writable_keys);
    let current_writable_hash = hash_btree_overlay(&current_writable);
    let candidate_writable_hash = hash_hash_overlay(&candidate_writable);
    require_equal_hash(
        "writable-clone-stage",
        current_writable_hash,
        candidate_writable_hash,
    )?;
    if current_writable_hash
        != hash_account_pairs(
            workload
                .after_writable
                .iter()
                .map(|(pubkey, account)| (pubkey, account)),
        )
    {
        return Err("writable staging did not match the pre-generated mutation oracle".to_owned());
    }
    println!(
        "equivalence=PASS cardinality={cardinality} data_bytes={data_bytes} group=writable-clone-stage current=btree candidate=hash-reserved canonical_sha256={}",
        hex(&current_writable_hash),
    );

    let (batch_forward_hash, batch_restored_hash) = verify_batch_round_trip(&workload)?;
    println!(
        "equivalence=PASS cardinality={cardinality} data_bytes={data_bytes} group=batch-stage-publication forward_state_sha256={} restored_state_sha256={}",
        hex(&batch_forward_hash),
        hex(&batch_restored_hash),
    );

    let diff = capture_instruction_diff(&workload, config.inline_diff_bytes);
    let diff_hash = hash_instruction_diff(&diff);
    if diff.accounts.len() != config.writable {
        return Err(format!(
            "diff oracle expected {} changed accounts, captured {}",
            config.writable,
            diff.accounts.len()
        ));
    }
    println!(
        "equivalence=PASS cardinality={cardinality} data_bytes={data_bytes} group=instruction-diff changed_accounts={} canonical_sha256={}",
        diff.accounts.len(),
        hex(&diff_hash),
    );

    let changed_hash = verify_changed_set_equivalence(&workload, config.iterations)?;
    println!(
        "equivalence=PASS cardinality={cardinality} data_bytes={data_bytes} group=changed-key-set current=btree candidate=hash-reserved base_keys={} final_keys={} canonical_sha256={}",
        workload.changed_base_keys.len(),
        workload.changed_final_len,
        hex(&changed_hash),
    );

    drop(current_read);
    drop(candidate_read);
    drop(current_writable);
    drop(candidate_writable);
    drop(diff);

    let read_btree = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        measure_once(count_allocations, || {
            exercise_read_overlay_btree(&workload.accounts, &workload.read_keys, config.iterations)
        })
    })?;
    let read_hash = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        measure_once(count_allocations, || {
            exercise_read_overlay_hash(&workload.accounts, &workload.read_keys, config.iterations)
        })
    })?;
    require_equal_u64(
        "read-overlay benchmark sink",
        read_btree.fingerprint,
        read_hash.fingerprint,
    )?;
    print_measurement(
        cardinality,
        data_bytes,
        "read-overlay",
        "current-btree",
        config.iterations,
        config.read_set,
        &read_btree,
    );
    print_measurement(
        cardinality,
        data_bytes,
        "read-overlay",
        "candidate-hash-reserved",
        config.iterations,
        config.read_set,
        &read_hash,
    );

    let writable_btree = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        measure_once(count_allocations, || {
            exercise_writable_stage_btree(
                &workload.accounts,
                &workload.writable_keys,
                config.iterations,
            )
        })
    })?;
    let writable_hash = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        measure_once(count_allocations, || {
            exercise_writable_stage_hash(
                &workload.accounts,
                &workload.writable_keys,
                config.iterations,
            )
        })
    })?;
    require_equal_u64(
        "writable staging benchmark sink",
        writable_btree.fingerprint,
        writable_hash.fingerprint,
    )?;
    print_measurement(
        cardinality,
        data_bytes,
        "writable-clone-stage",
        "current-btree",
        config.iterations,
        config.writable,
        &writable_btree,
    );
    print_measurement(
        cardinality,
        data_bytes,
        "writable-clone-stage",
        "candidate-hash-reserved",
        config.iterations,
        config.writable,
        &writable_hash,
    );

    let batch_stage = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        measure_once(count_allocations, || {
            exercise_batch_staging(&workload.after_writable, config.iterations)
        })
    })?;
    print_measurement(
        cardinality,
        data_bytes,
        "account-batch-stage",
        "current-btree-batch",
        config.iterations,
        config.writable,
        &batch_stage,
    );

    let mut publication_store = workload.accounts.clone();
    let publication = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        let batches = prepare_publication_batches(
            &workload.after_writable,
            &workload.before_writable,
            config.iterations,
        )
        .expect("validated deterministic batches can be staged");
        measure_once(count_allocations, || {
            apply_publication_batches(&mut publication_store, batches)
                .expect("alternating benchmark batches remain applicable")
        })
    })?;
    let publication_hash = publication_store.canonical_hash();
    require_equal_hash(
        "publication restored state",
        account_state_hash,
        publication_hash,
    )?;
    print_measurement(
        cardinality,
        data_bytes,
        "account-batch-publication",
        "memory-account-store",
        config.iterations,
        config.writable.saturating_mul(2),
        &publication,
    );

    let instruction_diff =
        benchmark_operation(config.rounds, config.warmups, |count_allocations| {
            measure_once(count_allocations, || {
                exercise_instruction_diff(&workload, config.inline_diff_bytes, config.iterations)
            })
        })?;
    print_measurement(
        cardinality,
        data_bytes,
        "instruction-diff-materialize",
        "current-btree-union-sha256",
        config.iterations,
        config.writable,
        &instruction_diff,
    );

    let changed_btree = benchmark_operation(config.rounds, config.warmups, |count_allocations| {
        let mut changed = workload
            .changed_base_keys
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        measure_once(count_allocations, || {
            exercise_changed_btree(&mut changed, &workload.changed_events, config.iterations)
        })
    })?;
    let changed_hash_candidate =
        benchmark_operation(config.rounds, config.warmups, |count_allocations| {
            let mut changed = HashSet::with_capacity(workload.changed_final_len);
            changed.extend(workload.changed_base_keys.iter().copied());
            measure_once(count_allocations, || {
                exercise_changed_hash(&mut changed, &workload.changed_events, config.iterations)
            })
        })?;
    require_equal_u64(
        "changed-key benchmark sink",
        changed_btree.fingerprint,
        changed_hash_candidate.fingerprint,
    )?;
    print_measurement(
        cardinality,
        data_bytes,
        "changed-key-insert",
        "current-btree-set",
        config.iterations,
        config.changed_events,
        &changed_btree,
    );
    print_measurement(
        cardinality,
        data_bytes,
        "changed-key-insert",
        "candidate-hash-set-reserved",
        config.iterations,
        config.changed_events,
        &changed_hash_candidate,
    );

    println!(
        "shape cardinality={cardinality} data_bytes={data_bytes} phase=complete state_equivalence=PASS account_state_sha256={} workload_sha256={}",
        hex(&account_state_hash),
        hex(&workload.workload_hash),
    );
    Ok(())
}

fn build_workload(
    config: &Config,
    cardinality: usize,
    data_bytes: usize,
) -> Result<ShapeWorkload, String> {
    let keys = generate_keys(cardinality, config.seed);
    let mut accounts = MemoryAccountStore::with_capacity(cardinality);
    for (index, &pubkey) in keys.iter().enumerate() {
        let replaced = accounts.insert(
            pubkey,
            deterministic_account(index as u64, data_bytes, config.seed),
        );
        if replaced.is_some() {
            return Err("deterministic pubkey generation produced a duplicate".to_owned());
        }
    }

    let read_indices = permutation_prefix(
        cardinality,
        config.read_set,
        config.seed ^ 0x0f82_6a6d_6f75_1d2b,
    );
    let read_keys = read_indices
        .iter()
        .map(|&index| keys[index])
        .collect::<Vec<_>>();
    let writable_keys = read_keys[..config.writable].to_vec();

    let mut before_writable = Vec::with_capacity(config.writable);
    let mut after_writable = Vec::with_capacity(config.writable);
    let mut diff_before = AccountMap::new();
    let mut diff_after = AccountMap::new();
    for &pubkey in &writable_keys {
        let before = accounts
            .get(&pubkey)
            .expect("selected writable key exists")
            .clone();
        let mut after = before.clone();
        mutate_staged_account(&mut after, &pubkey);
        before_writable.push((pubkey, before.clone()));
        after_writable.push((pubkey, after.clone()));
        diff_before.insert(pubkey, before);
        diff_after.insert(pubkey, after);
    }

    let changed_base_len = (cardinality / 2).max(1).min(cardinality - 1);
    let changed_base_keys = keys[..changed_base_len].to_vec();
    let changed_events = generate_changed_events(
        &keys,
        changed_base_len,
        config.changed_events,
        config.seed ^ 0xd134_2543_de82_ef95,
    );
    let mut changed_oracle = changed_base_keys.iter().copied().collect::<BTreeSet<_>>();
    changed_oracle.extend(changed_events.iter().copied());
    let changed_final_len = changed_oracle.len();

    let workload_hash = hash_workload(
        cardinality,
        data_bytes,
        &read_keys,
        &writable_keys,
        &changed_base_keys,
        &changed_events,
        &diff_before,
        &diff_after,
    );

    Ok(ShapeWorkload {
        accounts,
        read_keys,
        writable_keys,
        before_writable,
        after_writable,
        diff_before,
        diff_after,
        changed_base_keys,
        changed_events,
        changed_final_len,
        workload_hash,
    })
}

fn build_read_overlay_btree(accounts: &MemoryAccountStore, keys: &[Pubkey]) -> BTreeOverlay {
    let mut overlay = BTreeMap::new();
    for &pubkey in keys {
        let account = accounts
            .get(&pubkey)
            .expect("read-set key exists in the account registry")
            .clone();
        overlay.insert(pubkey, account);
    }
    overlay
}

fn build_read_overlay_hash(accounts: &MemoryAccountStore, keys: &[Pubkey]) -> HashOverlay {
    let mut overlay = HashMap::with_capacity(keys.len());
    for &pubkey in keys {
        let account = accounts
            .get(&pubkey)
            .expect("read-set key exists in the account registry")
            .clone();
        overlay.insert(pubkey, account);
    }
    overlay
}

fn stage_writable_btree(accounts: &MemoryAccountStore, keys: &[Pubkey]) -> BTreeOverlay {
    let mut overlay = BTreeMap::new();
    for &pubkey in keys {
        let mut account = accounts
            .get(&pubkey)
            .expect("writable key exists in the account registry")
            .clone();
        mutate_staged_account(&mut account, &pubkey);
        overlay.insert(pubkey, account);
    }
    overlay
}

fn stage_writable_hash(accounts: &MemoryAccountStore, keys: &[Pubkey]) -> HashOverlay {
    let mut overlay = HashMap::with_capacity(keys.len());
    for &pubkey in keys {
        let mut account = accounts
            .get(&pubkey)
            .expect("writable key exists in the account registry")
            .clone();
        mutate_staged_account(&mut account, &pubkey);
        overlay.insert(pubkey, account);
    }
    overlay
}

fn exercise_read_overlay_btree(
    accounts: &MemoryAccountStore,
    keys: &[Pubkey],
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        let overlay = build_read_overlay_btree(accounts, keys);
        fingerprint =
            fold_overlay_probe(fingerprint, iteration, keys, |pubkey| overlay.get(pubkey));
        black_box(&overlay);
    }
    black_box(fingerprint)
}

fn exercise_read_overlay_hash(
    accounts: &MemoryAccountStore,
    keys: &[Pubkey],
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        let overlay = build_read_overlay_hash(accounts, keys);
        fingerprint =
            fold_overlay_probe(fingerprint, iteration, keys, |pubkey| overlay.get(pubkey));
        black_box(&overlay);
    }
    black_box(fingerprint)
}

fn exercise_writable_stage_btree(
    accounts: &MemoryAccountStore,
    keys: &[Pubkey],
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        let overlay = stage_writable_btree(accounts, keys);
        fingerprint =
            fold_overlay_probe(fingerprint, iteration, keys, |pubkey| overlay.get(pubkey));
        black_box(&overlay);
    }
    black_box(fingerprint)
}

fn exercise_writable_stage_hash(
    accounts: &MemoryAccountStore,
    keys: &[Pubkey],
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        let overlay = stage_writable_hash(accounts, keys);
        fingerprint =
            fold_overlay_probe(fingerprint, iteration, keys, |pubkey| overlay.get(pubkey));
        black_box(&overlay);
    }
    black_box(fingerprint)
}

fn fold_overlay_probe<'a>(
    previous: u64,
    iteration: usize,
    keys: &[Pubkey],
    get: impl FnOnce(&Pubkey) -> Option<&'a AccountSnapshot>,
) -> u64 {
    let probe_key = &keys[iteration % keys.len()];
    let account = get(probe_key).expect("probe key was inserted into the overlay");
    mix64(previous ^ iteration as u64 ^ key_prefix(probe_key) ^ fold_account_light(account))
}

fn mutate_staged_account(account: &mut AccountSnapshot, pubkey: &Pubkey) {
    let key_word = key_prefix(pubkey);
    account.lamports ^= key_word | 1;
    account.rent_epoch ^= key_word.rotate_left(17) | 1;
    let data_len = account.data.len();
    if data_len == 0 {
        return;
    }

    let positions = [0, data_len / 2, data_len - 1];
    for (position_index, &position) in positions.iter().enumerate() {
        if positions[..position_index].contains(&position) {
            continue;
        }
        account.data[position] ^= (key_word.rotate_left((position_index * 11) as u32) as u8) | 1;
    }
}

fn stage_put_batch(values: &[(Pubkey, AccountSnapshot)]) -> Result<AccountWriteBatch, String> {
    let mut batch = AccountWriteBatch::new();
    for (pubkey, account) in values {
        batch
            .put(*pubkey, account.clone())
            .map_err(|error| error.to_string())?;
    }
    Ok(batch)
}

fn exercise_batch_staging(values: &[(Pubkey, AccountSnapshot)], iterations: usize) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        let batch = stage_put_batch(values).expect("deterministic batch keys are unique");
        fingerprint = mix64(fingerprint ^ iteration as u64 ^ batch.len() as u64);
        black_box(&batch);
    }
    black_box(fingerprint)
}

fn prepare_publication_batches(
    forward: &[(Pubkey, AccountSnapshot)],
    inverse: &[(Pubkey, AccountSnapshot)],
    iterations: usize,
) -> Result<Vec<AccountWriteBatch>, String> {
    let batch_count = iterations
        .checked_mul(2)
        .ok_or_else(|| "publication batch count overflow".to_owned())?;
    let mut batches = Vec::with_capacity(batch_count);
    for _ in 0..iterations {
        batches.push(stage_put_batch(forward)?);
        batches.push(stage_put_batch(inverse)?);
    }
    Ok(batches)
}

fn apply_publication_batches(
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

fn verify_batch_round_trip(workload: &ShapeWorkload) -> Result<([u8; 32], [u8; 32]), String> {
    let initial_hash = workload.accounts.canonical_hash();
    let mut published = workload.accounts.clone();
    let forward_commit = published
        .apply_batch(stage_put_batch(&workload.after_writable)?)
        .map_err(|error| error.to_string())?;
    if forward_commit.updated != workload.after_writable.len()
        || forward_commit.inserted != 0
        || forward_commit.deleted != 0
        || forward_commit.patched != 0
    {
        return Err(format!(
            "unexpected forward batch commit: {forward_commit:?}"
        ));
    }
    let published_hash = published.canonical_hash();

    let mut expected = workload.accounts.clone();
    for (pubkey, account) in &workload.after_writable {
        expected.insert(*pubkey, account.clone());
    }
    require_equal_hash(
        "batch publication against direct mutation oracle",
        expected.canonical_hash(),
        published_hash,
    )?;

    let inverse_commit = published
        .apply_batch(stage_put_batch(&workload.before_writable)?)
        .map_err(|error| error.to_string())?;
    if inverse_commit.updated != workload.before_writable.len()
        || inverse_commit.inserted != 0
        || inverse_commit.deleted != 0
        || inverse_commit.patched != 0
    {
        return Err(format!(
            "unexpected inverse batch commit: {inverse_commit:?}"
        ));
    }
    let restored_hash = published.canonical_hash();
    require_equal_hash("batch round-trip", initial_hash, restored_hash)?;
    Ok((published_hash, restored_hash))
}

fn fold_commit(previous: u64, index: usize, commit: AccountBatchCommit) -> u64 {
    mix64(
        previous
            ^ index as u64
            ^ (commit.inserted as u64).rotate_left(7)
            ^ (commit.updated as u64).rotate_left(19)
            ^ (commit.deleted as u64).rotate_left(31)
            ^ (commit.patched as u64).rotate_left(43),
    )
}

fn capture_instruction_diff(workload: &ShapeWorkload, inline_bytes: usize) -> InstructionDiff {
    InstructionDiff::capture(
        diff_boundary(),
        deterministic_key(0xf00d_cafe, DEFAULT_SEED),
        DiffDisposition::Speculative,
        &workload.diff_before,
        &workload.diff_after,
        DiffPolicy {
            include_lamports: false,
            max_inline_data_bytes: inline_bytes,
        },
    )
}

fn exercise_instruction_diff(
    workload: &ShapeWorkload,
    inline_bytes: usize,
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        let diff = capture_instruction_diff(workload, inline_bytes);
        fingerprint = mix64(fingerprint ^ iteration as u64 ^ fold_instruction_diff_light(&diff));
        black_box(&diff);
    }
    black_box(fingerprint)
}

fn diff_boundary() -> DiffBoundary {
    DiffBoundary {
        slot: 12_345_678,
        transaction_index: 17,
        trace_index: 3,
        stack_height: 2,
        instruction_path: InlineInstructionPath::from_slice(&[3, 1]),
    }
}

fn fold_instruction_diff_light(diff: &InstructionDiff) -> u64 {
    let mut fingerprint = diff.boundary.slot
        ^ u64::from(diff.boundary.transaction_index).rotate_left(7)
        ^ u64::from(diff.boundary.trace_index).rotate_left(13)
        ^ (diff.accounts.len() as u64).rotate_left(23)
        ^ key_prefix(&diff.program_id);
    for account in &diff.accounts {
        fingerprint = mix64(
            fingerprint
                ^ key_prefix(&account.pubkey)
                ^ (account.created as u64).rotate_left(31)
                ^ (account.deleted as u64).rotate_left(37),
        );
        if let Some(data) = &account.data {
            fingerprint ^= (data.ranges.len() as u64).rotate_left(17);
            fingerprint ^= (data.ranges_truncated as u64).rotate_left(29);
            if let Some(hash) = data.after_sha256 {
                fingerprint ^= key_prefix(&hash);
            }
        }
    }
    fingerprint
}

fn exercise_changed_btree(
    changed: &mut BTreeSet<Pubkey>,
    events: &[Pubkey],
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        for (event_index, &pubkey) in events.iter().enumerate() {
            let inserted = changed.insert(pubkey);
            fingerprint =
                fold_changed_event(fingerprint, iteration, event_index, &pubkey, inserted);
        }
    }
    black_box(fingerprint ^ (changed.len() as u64).rotate_left(11))
}

fn exercise_changed_hash(
    changed: &mut HashSet<Pubkey>,
    events: &[Pubkey],
    iterations: usize,
) -> u64 {
    let mut fingerprint = 0_u64;
    for iteration in 0..iterations {
        for (event_index, &pubkey) in events.iter().enumerate() {
            let inserted = changed.insert(pubkey);
            fingerprint =
                fold_changed_event(fingerprint, iteration, event_index, &pubkey, inserted);
        }
    }
    black_box(fingerprint ^ (changed.len() as u64).rotate_left(11))
}

fn fold_changed_event(
    previous: u64,
    iteration: usize,
    event_index: usize,
    pubkey: &Pubkey,
    inserted: bool,
) -> u64 {
    mix64(
        previous
            ^ iteration as u64
            ^ (event_index as u64).rotate_left(13)
            ^ key_prefix(pubkey)
            ^ (inserted as u64).rotate_left(47),
    )
}

fn verify_changed_set_equivalence(
    workload: &ShapeWorkload,
    iterations: usize,
) -> Result<[u8; 32], String> {
    let mut current = workload
        .changed_base_keys
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut candidate = HashSet::with_capacity(workload.changed_final_len);
    candidate.extend(workload.changed_base_keys.iter().copied());
    let current_sink = exercise_changed_btree(&mut current, &workload.changed_events, iterations);
    let candidate_sink =
        exercise_changed_hash(&mut candidate, &workload.changed_events, iterations);
    require_equal_u64("changed-key insertion sink", current_sink, candidate_sink)?;
    if current.len() != workload.changed_final_len || candidate.len() != workload.changed_final_len
    {
        return Err("changed-key final cardinality did not match its oracle".to_owned());
    }
    let current_hash = hash_btree_keys(&current);
    let candidate_hash = hash_hash_keys(&candidate);
    require_equal_hash("changed-key canonical set", current_hash, candidate_hash)?;
    Ok(current_hash)
}

fn benchmark_operation(
    rounds: usize,
    warmups: usize,
    mut sample: impl FnMut(bool) -> RawSample,
) -> Result<Measurement, String> {
    let mut expected_fingerprint = None;
    for _ in 0..warmups {
        let warmup = sample(false);
        validate_fingerprint(&mut expected_fingerprint, warmup.fingerprint)?;
    }

    let mut timings = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let measured = sample(false);
        validate_fingerprint(&mut expected_fingerprint, measured.fingerprint)?;
        timings.push(measured.elapsed);
    }

    let allocation_sample = sample(true);
    validate_fingerprint(&mut expected_fingerprint, allocation_sample.fingerprint)?;
    timings.sort_unstable();
    Ok(Measurement {
        median: timings[timings.len() / 2],
        allocations: allocation_sample.allocations,
        fingerprint: expected_fingerprint.expect("round count is positive"),
    })
}

fn validate_fingerprint(expected: &mut Option<u64>, found: u64) -> Result<(), String> {
    match expected {
        Some(expected) if *expected != found => Err(format!(
            "operation fingerprint changed between samples: expected={expected:016x} found={found:016x}"
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

fn print_measurement(
    cardinality: usize,
    data_bytes: usize,
    operation: &str,
    implementation: &str,
    iterations: usize,
    items_per_iteration: usize,
    measurement: &Measurement,
) {
    let total_items = iterations.saturating_mul(items_per_iteration);
    let elapsed_ns = measurement.median.as_nanos() as f64;
    let iterations_f64 = iterations as f64;
    let items_f64 = total_items as f64;
    println!(
        "metric cardinality={cardinality} data_bytes={data_bytes} operation={operation} implementation={implementation} iterations={iterations} items_per_iteration={items_per_iteration} total_items={total_items} median_ms={:.3} ns_per_iteration={:.3} ns_per_item={:.3} allocation_calls_total={} allocation_calls_per_iteration={:.3} allocated_bytes_total={} allocated_bytes_per_iteration={:.3} fingerprint={:016x}",
        millis(measurement.median),
        elapsed_ns / iterations_f64,
        elapsed_ns / items_f64,
        measurement.allocations.calls,
        measurement.allocations.calls as f64 / iterations_f64,
        measurement.allocations.bytes,
        measurement.allocations.bytes as f64 / iterations_f64,
        measurement.fingerprint,
    );
}

fn generate_keys(count: usize, seed: u64) -> Vec<Pubkey> {
    (0..count)
        .map(|index| deterministic_key(index as u64, seed))
        .collect()
}

fn deterministic_key(index: u64, seed: u64) -> Pubkey {
    // SplitMix64's output permutation makes the first word unique for unique
    // indices while distributing the key across the hash table.
    let mut pubkey = [0_u8; 32];
    let mut state = index ^ seed;
    for chunk in pubkey.chunks_exact_mut(8) {
        state = mix64(state);
        chunk.copy_from_slice(&state.to_be_bytes());
    }
    pubkey
}

fn deterministic_account(index: u64, data_bytes: usize, seed: u64) -> AccountSnapshot {
    let mut data = Vec::with_capacity(data_bytes);
    let mut state = index ^ seed ^ 0xa076_1d64_78bd_642f;
    for offset in 0..data_bytes {
        state = mix64(state ^ offset as u64);
        data.push(state as u8);
    }
    AccountSnapshot {
        lamports: mix64(index ^ seed) | 1,
        owner: deterministic_key(index ^ 0xe703_7ed1_a0b4_28db, seed),
        executable: index.is_multiple_of(97),
        rent_epoch: index % 432,
        data: data.into(),
    }
}

fn permutation_prefix(cardinality: usize, count: usize, seed: u64) -> Vec<usize> {
    if cardinality == 1 {
        return vec![0; count];
    }
    let start = (mix64(seed) as usize) % cardinality;
    let mut step = (mix64(seed ^ 0x9e37_79b9_7f4a_7c15) as usize) % cardinality;
    if step == 0 {
        step = 1;
    }
    while greatest_common_divisor(step, cardinality) != 1 {
        step += 1;
        if step == cardinality {
            step = 1;
        }
    }
    (0..count)
        .map(|index| (start + index.wrapping_mul(step)) % cardinality)
        .collect()
}

fn greatest_common_divisor(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn generate_changed_events(
    keys: &[Pubkey],
    base_len: usize,
    event_count: usize,
    seed: u64,
) -> Vec<Pubkey> {
    let mut state = seed;
    (0..event_count)
        .map(|event_index| {
            state = mix64(state ^ event_index as u64);
            if event_index.is_multiple_of(2) {
                keys[(state as usize) % base_len]
            } else {
                keys[base_len + (state as usize) % (keys.len() - base_len)]
            }
        })
        .collect()
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn fold_account_light(account: &AccountSnapshot) -> u64 {
    let first = account.data.first().copied().unwrap_or_default() as u64;
    let middle = account
        .data
        .get(account.data.len().saturating_sub(1) / 2)
        .copied()
        .unwrap_or_default() as u64;
    let last = account.data.last().copied().unwrap_or_default() as u64;
    account.lamports
        ^ key_prefix(&account.owner).rotate_left(7)
        ^ account.rent_epoch.rotate_left(17)
        ^ (account.executable as u64).rotate_left(23)
        ^ (account.data.len() as u64).rotate_left(29)
        ^ first.rotate_left(37)
        ^ middle.rotate_left(43)
        ^ last.rotate_left(53)
}

fn key_prefix(pubkey: &Pubkey) -> u64 {
    u64::from_le_bytes(pubkey[..8].try_into().expect("eight-byte pubkey prefix"))
}

fn hash_btree_overlay(overlay: &BTreeOverlay) -> [u8; 32] {
    hash_account_pairs(overlay.iter())
}

fn hash_prod_overlay(overlay: &ProdOverlay) -> [u8; 32] {
    hash_account_pairs(overlay.iter())
}

fn hash_hash_overlay(overlay: &HashOverlay) -> [u8; 32] {
    hash_account_pairs(overlay.iter())
}

fn hash_account_pairs<'a>(
    accounts: impl IntoIterator<Item = (&'a Pubkey, &'a AccountSnapshot)>,
) -> [u8; 32] {
    let mut entries = accounts.into_iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla/replay-state-bench/accounts/v1\0");
    hasher.update((entries.len() as u64).to_le_bytes());
    for (pubkey, account) in entries {
        update_account_hash(&mut hasher, pubkey, account);
    }
    hasher.finalize().into()
}

fn update_account_hash(hasher: &mut Sha256, pubkey: &Pubkey, account: &AccountSnapshot) {
    hasher.update(pubkey);
    hasher.update(account.lamports.to_le_bytes());
    hasher.update(account.owner);
    hasher.update([u8::from(account.executable)]);
    hasher.update(account.rent_epoch.to_le_bytes());
    hasher.update((account.data.len() as u64).to_le_bytes());
    hasher.update(&account.data);
}

fn hash_btree_keys(keys: &BTreeSet<Pubkey>) -> [u8; 32] {
    hash_sorted_keys(keys.iter().copied())
}

fn hash_hash_keys(keys: &HashSet<Pubkey>) -> [u8; 32] {
    let mut sorted = keys.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    hash_sorted_keys(sorted)
}

fn hash_sorted_keys(keys: impl IntoIterator<Item = Pubkey>) -> [u8; 32] {
    let keys = keys.into_iter().collect::<Vec<_>>();
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla/replay-state-bench/pubkeys/v1\0");
    hasher.update((keys.len() as u64).to_le_bytes());
    for pubkey in keys {
        hasher.update(pubkey);
    }
    hasher.finalize().into()
}

#[allow(clippy::too_many_arguments)]
fn hash_workload(
    cardinality: usize,
    data_bytes: usize,
    read_keys: &[Pubkey],
    writable_keys: &[Pubkey],
    changed_base_keys: &[Pubkey],
    changed_events: &[Pubkey],
    diff_before: &ProdOverlay,
    diff_after: &ProdOverlay,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla/replay-state-bench/workload/v1\0");
    hasher.update((cardinality as u64).to_le_bytes());
    hasher.update((data_bytes as u64).to_le_bytes());
    update_key_slice_hash(&mut hasher, read_keys);
    update_key_slice_hash(&mut hasher, writable_keys);
    update_key_slice_hash(&mut hasher, changed_base_keys);
    update_key_slice_hash(&mut hasher, changed_events);
    hasher.update(hash_prod_overlay(diff_before));
    hasher.update(hash_prod_overlay(diff_after));
    hasher.finalize().into()
}

fn update_key_slice_hash(hasher: &mut Sha256, keys: &[Pubkey]) {
    hasher.update((keys.len() as u64).to_le_bytes());
    for pubkey in keys {
        hasher.update(pubkey);
    }
}

fn hash_instruction_diff(diff: &InstructionDiff) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla/replay-state-bench/instruction-diff/v1\0");
    hasher.update(diff.boundary.slot.to_le_bytes());
    hasher.update(diff.boundary.transaction_index.to_le_bytes());
    hasher.update(diff.boundary.trace_index.to_le_bytes());
    hasher.update(diff.boundary.stack_height.to_le_bytes());
    hasher.update((diff.boundary.instruction_path.len() as u64).to_le_bytes());
    for component in &diff.boundary.instruction_path {
        hasher.update(component.to_le_bytes());
    }
    hasher.update(diff.program_id);
    hasher.update([match diff.disposition {
        DiffDisposition::Speculative => 0,
        DiffDisposition::Committed => 1,
        DiffDisposition::RolledBack => 2,
    }]);
    hasher.update((diff.accounts.len() as u64).to_le_bytes());
    for account in &diff.accounts {
        hasher.update(account.pubkey);
        hasher.update([u8::from(account.created), u8::from(account.deleted)]);
        update_optional_u64_diff(&mut hasher, account.lamports.as_ref());
        update_optional_pubkey_diff(&mut hasher, account.owner.as_ref());
        update_optional_bool_diff(&mut hasher, account.executable.as_ref());
        update_optional_u64_diff(&mut hasher, account.rent_epoch.as_ref());
        if let Some(data) = &account.data {
            hasher.update([1]);
            update_optional_usize(&mut hasher, data.before_len);
            update_optional_usize(&mut hasher, data.after_len);
            update_optional_hash(&mut hasher, data.before_sha256);
            update_optional_hash(&mut hasher, data.after_sha256);
            hasher.update([u8::from(data.ranges_truncated)]);
            hasher.update((data.ranges.len() as u64).to_le_bytes());
            for range in &data.ranges {
                hasher.update((range.offset as u64).to_le_bytes());
                hasher.update((range.before.len() as u64).to_le_bytes());
                hasher.update(&range.before);
                hasher.update((range.after.len() as u64).to_le_bytes());
                hasher.update(&range.after);
            }
        } else {
            hasher.update([0]);
        }
    }
    hasher.finalize().into()
}

fn update_optional_u64_diff(
    hasher: &mut Sha256,
    value: Option<&blockzilla_replay::ValueDiff<u64>>,
) {
    if let Some(value) = value {
        hasher.update([1]);
        update_optional_u64(hasher, value.before);
        update_optional_u64(hasher, value.after);
    } else {
        hasher.update([0]);
    }
}

fn update_optional_pubkey_diff(
    hasher: &mut Sha256,
    value: Option<&blockzilla_replay::ValueDiff<Pubkey>>,
) {
    if let Some(value) = value {
        hasher.update([1]);
        update_optional_pubkey(hasher, value.before);
        update_optional_pubkey(hasher, value.after);
    } else {
        hasher.update([0]);
    }
}

fn update_optional_bool_diff(
    hasher: &mut Sha256,
    value: Option<&blockzilla_replay::ValueDiff<bool>>,
) {
    if let Some(value) = value {
        hasher.update([1]);
        update_optional_bool(hasher, value.before);
        update_optional_bool(hasher, value.after);
    } else {
        hasher.update([0]);
    }
}

fn update_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    if let Some(value) = value {
        hasher.update([1]);
        hasher.update(value.to_le_bytes());
    } else {
        hasher.update([0]);
    }
}

fn update_optional_usize(hasher: &mut Sha256, value: Option<usize>) {
    update_optional_u64(hasher, value.map(|value| value as u64));
}

fn update_optional_pubkey(hasher: &mut Sha256, value: Option<Pubkey>) {
    if let Some(value) = value {
        hasher.update([1]);
        hasher.update(value);
    } else {
        hasher.update([0]);
    }
}

fn update_optional_bool(hasher: &mut Sha256, value: Option<bool>) {
    if let Some(value) = value {
        hasher.update([1, u8::from(value)]);
    } else {
        hasher.update([0]);
    }
}

fn update_optional_hash(hasher: &mut Sha256, value: Option<[u8; 32]>) {
    update_optional_pubkey(hasher, value);
}

fn require_equal_hash(label: &str, expected: [u8; 32], found: [u8; 32]) -> Result<(), String> {
    if expected == found {
        Ok(())
    } else {
        Err(format!(
            "{label} canonical mismatch: expected={} found={}",
            hex(&expected),
            hex(&found)
        ))
    }
}

fn require_equal_u64(label: &str, expected: u64, found: u64) -> Result<(), String> {
    if expected == found {
        Ok(())
    } else {
        Err(format!(
            "{label} mismatch: expected={expected:016x} found={found:016x}"
        ))
    }
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

fn join_usizes(values: &[usize]) -> String {
    values
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
        data_sizes: DEFAULT_DATA_SIZES.to_vec(),
        read_set: DEFAULT_READ_SET,
        writable: DEFAULT_WRITABLE,
        changed_events: DEFAULT_CHANGED_EVENTS,
        iterations: DEFAULT_ITERATIONS,
        rounds: DEFAULT_ROUNDS,
        warmups: DEFAULT_WARMUPS,
        inline_diff_bytes: DEFAULT_INLINE_DIFF_BYTES,
        seed: DEFAULT_SEED,
    };
    let mut cardinality_option = None;
    let mut data_size_option = None;
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
        let value = inline_value.map(str::to_owned).map_or_else(
            || {
                args.next()
                    .ok_or_else(|| format!("missing value for {name}"))
            },
            Ok,
        )?;

        match name {
            "--accounts" => {
                select_exclusive(&mut cardinality_option, "--accounts")?;
                config.cardinalities = vec![parse_usize(name, &value)?];
            }
            "--cardinalities" => {
                select_exclusive(&mut cardinality_option, "--cardinalities")?;
                config.cardinalities = parse_usize_list(name, &value)?;
            }
            "--data-bytes" => {
                select_exclusive(&mut data_size_option, "--data-bytes")?;
                config.data_sizes = vec![parse_usize(name, &value)?];
            }
            "--data-sizes" => {
                select_exclusive(&mut data_size_option, "--data-sizes")?;
                config.data_sizes = parse_usize_list(name, &value)?;
            }
            "--read-set" => config.read_set = parse_usize(name, &value)?,
            "--writable" => config.writable = parse_usize(name, &value)?,
            "--changed-events" => config.changed_events = parse_usize(name, &value)?,
            "--iterations" => config.iterations = parse_usize(name, &value)?,
            "--rounds" => config.rounds = parse_usize(name, &value)?,
            "--warmups" => config.warmups = parse_usize(name, &value)?,
            "--inline-diff-bytes" => config.inline_diff_bytes = parse_usize(name, &value)?,
            "--seed" => config.seed = parse_u64(name, &value)?,
            _ => return Err(format!("unknown argument: {name}")),
        }
    }

    config.cardinalities.sort_unstable();
    config.cardinalities.dedup();
    config.data_sizes.sort_unstable();
    config.data_sizes.dedup();
    validate_config(&config)?;
    Ok(Some(config))
}

fn validate_config(config: &Config) -> Result<(), String> {
    if config.cardinalities.is_empty() || config.cardinalities[0] < 2 {
        return Err("account cardinalities must be at least two".to_owned());
    }
    if config.data_sizes.is_empty() {
        return Err("at least one account data size is required".to_owned());
    }
    if config.read_set == 0 {
        return Err("--read-set must be greater than zero".to_owned());
    }
    if config.read_set > config.cardinalities[0] {
        return Err(format!(
            "--read-set {} exceeds the smallest account cardinality {}",
            config.read_set, config.cardinalities[0]
        ));
    }
    if config.writable == 0 || config.writable > config.read_set {
        return Err("--writable must be between one and --read-set".to_owned());
    }
    if config.changed_events == 0 {
        return Err("--changed-events must be greater than zero".to_owned());
    }
    if config.iterations == 0 {
        return Err("--iterations must be greater than zero".to_owned());
    }
    if config.rounds == 0 {
        return Err("--rounds must be greater than zero".to_owned());
    }
    Ok(())
}

fn select_exclusive(
    selected: &mut Option<&'static str>,
    option: &'static str,
) -> Result<(), String> {
    if let Some(previous) = selected.replace(option) {
        Err(format!("{previous} and {option} cannot be used together"))
    } else {
        Ok(())
    }
}

fn parse_usize(name: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid non-negative integer for {name}: {value}"))
}

fn parse_u64(name: &str, value: &str) -> Result<u64, String> {
    let parsed = value
        .strip_prefix("0x")
        .map_or_else(|| value.parse::<u64>(), |hex| u64::from_str_radix(hex, 16));
    parsed.map_err(|_| format!("invalid u64 for {name}: {value}"))
}

fn parse_usize_list(name: &str, value: &str) -> Result<Vec<usize>, String> {
    if value.is_empty() {
        return Err(format!("{name} requires at least one value"));
    }
    value
        .split(',')
        .map(|item| parse_usize(name, item))
        .collect()
}

fn print_usage() {
    println!("Usage: replay-state-bench [--accounts N | --cardinalities N,N,...]");
    println!("       [--data-bytes N | --data-sizes N,N,...]");
    println!("       [--read-set N] [--writable N] [--changed-events N]");
    println!("       [--iterations N] [--rounds N] [--warmups N]");
    println!("       [--inline-diff-bytes N] [--seed N|0xHEX]");
    println!(
        "Defaults: --cardinalities {} --data-sizes {} --read-set {DEFAULT_READ_SET}",
        join_usizes(DEFAULT_CARDINALITIES),
        join_usizes(DEFAULT_DATA_SIZES),
    );
    println!(
        "          --writable {DEFAULT_WRITABLE} --changed-events {DEFAULT_CHANGED_EVENTS} --iterations {DEFAULT_ITERATIONS}"
    );
    println!(
        "          --rounds {DEFAULT_ROUNDS} --warmups {DEFAULT_WARMUPS} --inline-diff-bytes {DEFAULT_INLINE_DIFF_BYTES} --seed 0x{DEFAULT_SEED:016x}"
    );
    println!("Run with --release for representative measurements.");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn small_config() -> Config {
        Config {
            cardinalities: vec![64],
            data_sizes: vec![16],
            read_set: 12,
            writable: 4,
            changed_events: 10,
            iterations: 3,
            rounds: 1,
            warmups: 0,
            inline_diff_bytes: 128,
            seed: DEFAULT_SEED,
        }
    }

    #[test]
    fn defaults_cover_epoch_era_registry_and_multiple_payload_sizes() {
        let config = parse_args_from(Vec::new()).unwrap().unwrap();
        assert_eq!(config.cardinalities, DEFAULT_CARDINALITIES);
        assert!(config.cardinalities.contains(&54_339));
        assert_eq!(config.data_sizes, DEFAULT_DATA_SIZES);
        assert!(config.data_sizes.contains(&0));
        assert!(config.data_sizes.contains(&1_024));
    }

    #[test]
    fn cli_sweeps_are_sorted_deduplicated_and_exclusive() {
        let config = parse_args_from(args(&[
            "--cardinalities=64,32,64",
            "--data-sizes",
            "1024,0,128,128",
            "--read-set",
            "16",
            "--writable=4",
            "--seed",
            "0x2a",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(config.cardinalities, [32, 64]);
        assert_eq!(config.data_sizes, [0, 128, 1_024]);
        assert_eq!(config.seed, 42);
        assert!(parse_args_from(args(&["--accounts", "64", "--cardinalities", "32,64"])).is_err());
        assert!(parse_args_from(args(&["--accounts", "8", "--read-set", "9"])).is_err());
    }

    #[test]
    fn deterministic_workload_and_overlay_candidates_are_canonical() {
        let config = small_config();
        let left = build_workload(&config, 64, 16).unwrap();
        let right = build_workload(&config, 64, 16).unwrap();
        assert_eq!(left.workload_hash, right.workload_hash);
        assert_eq!(
            left.accounts.canonical_hash(),
            right.accounts.canonical_hash()
        );
        assert_eq!(left.read_keys, right.read_keys);

        let read_btree = build_read_overlay_btree(&left.accounts, &left.read_keys);
        let read_hash = build_read_overlay_hash(&left.accounts, &left.read_keys);
        assert_eq!(
            hash_btree_overlay(&read_btree),
            hash_hash_overlay(&read_hash)
        );

        let writable_btree = stage_writable_btree(&left.accounts, &left.writable_keys);
        let writable_hash = stage_writable_hash(&left.accounts, &left.writable_keys);
        assert_eq!(
            hash_btree_overlay(&writable_btree),
            hash_hash_overlay(&writable_hash)
        );
        assert_eq!(
            hash_btree_overlay(&writable_btree),
            hash_account_pairs(
                left.after_writable
                    .iter()
                    .map(|(pubkey, account)| (pubkey, account))
            )
        );
    }

    #[test]
    fn staged_batches_publish_and_restore_exact_state() {
        let config = small_config();
        let workload = build_workload(&config, 64, 16).unwrap();
        let initial = workload.accounts.canonical_hash();
        let (forward, restored) = verify_batch_round_trip(&workload).unwrap();
        assert_ne!(forward, initial);
        assert_eq!(restored, initial);

        let mut accounts = workload.accounts.clone();
        let batches =
            prepare_publication_batches(&workload.after_writable, &workload.before_writable, 3)
                .unwrap();
        assert_ne!(
            apply_publication_batches(&mut accounts, batches).unwrap(),
            0
        );
        assert_eq!(accounts.canonical_hash(), initial);
    }

    #[test]
    fn instruction_diff_is_deterministic_and_tracks_every_writable_account() {
        let mut config = small_config();
        config.data_sizes = vec![0];
        let empty_data = build_workload(&config, 64, 0).unwrap();
        let first = capture_instruction_diff(&empty_data, config.inline_diff_bytes);
        let second = capture_instruction_diff(&empty_data, config.inline_diff_bytes);
        assert_eq!(first.accounts.len(), config.writable);
        assert_eq!(
            hash_instruction_diff(&first),
            hash_instruction_diff(&second)
        );
        assert!(
            first
                .accounts
                .iter()
                .all(|account| account.rent_epoch.is_some())
        );

        let data = build_workload(&small_config(), 64, 16).unwrap();
        let diff = capture_instruction_diff(&data, 128);
        assert!(diff.accounts.iter().all(|account| account.data.is_some()));
        assert!(diff.accounts.iter().all(|account| {
            account
                .data
                .as_ref()
                .is_some_and(|data| data.before_sha256 != data.after_sha256)
        }));
    }

    #[test]
    fn changed_key_structures_have_identical_insert_results_and_state() {
        let config = small_config();
        let workload = build_workload(&config, 64, 16).unwrap();
        let hash = verify_changed_set_equivalence(&workload, config.iterations).unwrap();
        assert_ne!(hash, [0; 32]);

        let mut current = workload
            .changed_base_keys
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut candidate = HashSet::with_capacity(workload.changed_final_len);
        candidate.extend(workload.changed_base_keys.iter().copied());
        assert_eq!(
            exercise_changed_btree(&mut current, &workload.changed_events, 2),
            exercise_changed_hash(&mut candidate, &workload.changed_events, 2)
        );
        assert_eq!(hash_btree_keys(&current), hash_hash_keys(&candidate));
    }
}
