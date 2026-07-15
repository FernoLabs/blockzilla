use std::{
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_format::{KeyIndex, KeyStore, framed::read_u32_varint};
use clap::Parser;
use gxhash::{GxBuildHasher, HashMap as GxHashMap, gxhash64};
use hashbrown::HashTable;

const HASH_SEED: i64 = 0x425a_4653_4931;
const FIRST_SEEN_BUILTIN_KEYS: [[u8; 32]; 1] =
    [solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()];

/// Microbenchmark candidate data structures for a one-pass first-seen registry.
#[derive(Debug, Parser)]
struct Args {
    /// Completed registry.bin; storage simulation expects first-seen key order.
    registry: PathBuf,

    /// LEB128 registry_counts.bin matching registry.bin.
    counts: PathBuf,

    /// Existing registry.mphf. Defaults to registry.mphf next to registry.bin.
    #[arg(long)]
    mphf: Option<PathBuf>,

    /// Number of real-distribution references in each timed lookup pass.
    #[arg(long, default_value_t = 25_000_000)]
    samples: usize,

    /// Full-epoch reference count used to extrapolate lookup CPU time.
    #[arg(long, default_value_t = 10_002_809_571)]
    epoch_refs: u64,

    /// Also benchmark a conventional GxHashMap that duplicates all 32-byte keys.
    #[arg(long)]
    include_gx_map: bool,

    /// Simulate alternative first-seen registry orders and their exact ID storage cost.
    #[arg(long, requires = "prior_frequency_registry")]
    simulate_storage: bool,

    /// Stop after storage simulation instead of running lookup microbenchmarks.
    #[arg(long, requires = "simulate_storage")]
    storage_only: bool,

    /// Prior-epoch frequency registry used by the input build and larger-seed simulations.
    #[arg(long, value_name = "PATH", requires = "simulate_storage")]
    prior_frequency_registry: Option<PathBuf>,

    /// Prior-registry row limits to simulate. May be repeated or comma-separated.
    /// Limits must be supersets of the seed used to build the completed input.
    #[arg(
        long = "seed-limit",
        value_delimiter = ',',
        default_value = "65536,2097151"
    )]
    seed_limits: Vec<usize>,

    /// Prior-registry row limit used to build the completed first-seen input.
    #[arg(long, default_value_t = 65_536, requires = "simulate_storage")]
    base_seed_limit: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.samples > 0, "--samples must be non-zero");

    let started = Instant::now();
    let store = KeyStore::load(&args.registry)
        .with_context(|| format!("load {}", args.registry.display()))?;
    let counts = read_counts(&args.counts)?;
    ensure!(
        store.len() == counts.len(),
        "registry has {} keys but counts has {} rows",
        store.len(),
        counts.len()
    );
    let total_refs = counts
        .iter()
        .try_fold(0u64, |sum, &count| sum.checked_add(u64::from(count)))
        .context("registry count sum overflow")?;
    ensure!(total_refs > 0, "registry counts are all zero");
    println!(
        "load keys={} counted_refs={} elapsed_s={:.3} rss_mib={:.1}",
        store.len(),
        total_refs,
        started.elapsed().as_secs_f64(),
        rss_mib()
    );
    let (input_order_refs, input_order_id_bytes) = weighted_bytes_for_input_order(&counts)?;
    println!(
        "input_order_storage registry_keys={} registry_bytes={} counted_refs={} weighted_uleb128_id_bytes={} average_id_bytes_per_ref={:.9}",
        store.len(),
        store.len() as u128 * 32,
        input_order_refs,
        input_order_id_bytes,
        input_order_id_bytes as f64 / input_order_refs as f64,
    );

    if args.simulate_storage {
        let prior_registry = args
            .prior_frequency_registry
            .as_deref()
            .context("--simulate-storage requires --prior-frequency-registry <registry.bin>")?;
        ensure!(
            !args.seed_limits.is_empty(),
            "--simulate-storage requires at least one --seed-limit"
        );
        ensure!(
            args.seed_limits
                .iter()
                .all(|&limit| limit >= args.base_seed_limit),
            "storage simulation can only reconstruct seed supersets of --base-seed-limit {}",
            args.base_seed_limit
        );
        run_storage_simulations(&store.keys, &counts, prior_registry, &args.seed_limits)?;
        if args.storage_only {
            return Ok(());
        }
    } else {
        ensure!(
            args.prior_frequency_registry.is_none(),
            "--prior-frequency-registry requires --simulate-storage"
        );
    }

    let sample_started = Instant::now();
    let samples = weighted_samples(&counts, total_refs, args.samples)?;
    drop(counts);
    trim_allocator();
    println!(
        "sample rows={} elapsed_s={:.3} bytes={} rss_mib={:.1}",
        samples.len(),
        sample_started.elapsed().as_secs_f64(),
        samples.len() * size_of::<u32>(),
        rss_mib()
    );

    let mphf_path = args.mphf.unwrap_or_else(|| {
        args.registry
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("registry.mphf")
    });
    let mphf_load_started = Instant::now();
    let mphf =
        KeyIndex::load(&mphf_path).with_context(|| format!("load {}", mphf_path.display()))?;
    ensure!(
        mphf.len() == store.len(),
        "MPHF and registry lengths differ"
    );
    println!(
        "mphf_load elapsed_s={:.3} rss_mib={:.1}",
        mphf_load_started.elapsed().as_secs_f64(),
        rss_mib()
    );
    let mphf_timing = bench_mphf(&mphf, &store.keys, &samples)?;
    print_timing("mphf_lookup", mphf_timing, args.epoch_refs);
    drop(mphf);
    trim_allocator();

    let table_build_started = Instant::now();
    let table = build_id_table(&store.keys)?;
    println!(
        "id_table_build keys={} capacity={} elapsed_s={:.3} keys_s={:.0} rss_mib={:.1}",
        table.len(),
        table.capacity(),
        table_build_started.elapsed().as_secs_f64(),
        store.len() as f64 / table_build_started.elapsed().as_secs_f64(),
        rss_mib()
    );
    let table_lookup = bench_id_table(&table, &store.keys, &samples, false)?;
    print_timing("id_table_lookup", table_lookup, args.epoch_refs);
    let table_count = bench_id_table(&table, &store.keys, &samples, true)?;
    print_timing("id_table_lookup_count", table_count, args.epoch_refs);
    drop(table);
    trim_allocator();

    if args.include_gx_map {
        let map_build_started = Instant::now();
        let mut map = GxHashMap::with_capacity_and_hasher(store.len(), GxBuildHasher::default());
        for (id, key) in store.keys.iter().copied().enumerate() {
            let old = map.insert(key, id as u32);
            ensure!(
                old.is_none(),
                "duplicate registry key at zero-based id {id}"
            );
        }
        println!(
            "gx_map_build keys={} capacity={} elapsed_s={:.3} keys_s={:.0} rss_mib={:.1}",
            map.len(),
            map.capacity(),
            map_build_started.elapsed().as_secs_f64(),
            store.len() as f64 / map_build_started.elapsed().as_secs_f64(),
            rss_mib()
        );
        let gx_count = bench_gx_map(&map, &store.keys, &samples)?;
        print_timing("gx_map_lookup_count", gx_count, args.epoch_refs);
    }

    Ok(())
}

fn read_counts(path: &Path) -> Result<Vec<u32>> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = std::io::BufReader::with_capacity(8 << 20, file);
    let mut counts = Vec::new();
    while let Some(count) =
        read_u32_varint(&mut reader).with_context(|| format!("read {}", path.display()))?
    {
        counts.push(count);
    }
    Ok(counts)
}

#[derive(Debug)]
struct StorageSimulation {
    requested_seed_limit: usize,
    prior_rows_taken: usize,
    seeded_keys: usize,
    zero_count_seeded_keys: usize,
    registry_keys: usize,
    registry_bytes: u128,
    counted_refs: u64,
    weighted_uleb128_id_bytes: u128,
    elapsed: Duration,
    rss_mib: f64,
}

impl StorageSimulation {
    fn average_id_bytes(&self) -> f64 {
        self.weighted_uleb128_id_bytes as f64 / self.counted_refs as f64
    }
}

fn run_storage_simulations(
    first_seen_keys: &[[u8; 32]],
    counts: &[u32],
    prior_registry_path: &Path,
    seed_limits: &[usize],
) -> Result<()> {
    let setup_started = Instant::now();
    let prior = KeyStore::load(prior_registry_path)
        .with_context(|| format!("load {}", prior_registry_path.display()))?;
    let first_seen_table = build_id_table(first_seen_keys)?;
    println!(
        "storage_sim_setup first_seen_keys={} prior_frequency_keys={} table_capacity={} elapsed_s={:.3} rss_mib={:.1}",
        first_seen_keys.len(),
        prior.len(),
        first_seen_table.capacity(),
        setup_started.elapsed().as_secs_f64(),
        rss_mib()
    );

    for &seed_limit in seed_limits {
        let simulation = simulate_storage_order(
            first_seen_keys,
            counts,
            &first_seen_table,
            &prior.keys,
            seed_limit,
            &FIRST_SEEN_BUILTIN_KEYS,
        )?;
        println!(
            "storage_sim seed_limit={} prior_rows_taken={} seeded_keys={} zero_count_seeded_keys={} registry_keys={} registry_bytes={} counted_refs={} weighted_uleb128_id_bytes={} average_id_bytes_per_ref={:.9} elapsed_s={:.3} rss_mib={:.1}",
            simulation.requested_seed_limit,
            simulation.prior_rows_taken,
            simulation.seeded_keys,
            simulation.zero_count_seeded_keys,
            simulation.registry_keys,
            simulation.registry_bytes,
            simulation.counted_refs,
            simulation.weighted_uleb128_id_bytes,
            simulation.average_id_bytes(),
            simulation.elapsed.as_secs_f64(),
            simulation.rss_mib,
        );
        trim_allocator();
    }

    drop(first_seen_table);
    drop(prior);
    trim_allocator();
    Ok(())
}

fn simulate_storage_order(
    first_seen_keys: &[[u8; 32]],
    counts: &[u32],
    first_seen_table: &HashTable<u32>,
    prior_frequency_keys: &[[u8; 32]],
    seed_limit: usize,
    builtin_keys: &[[u8; 32]],
) -> Result<StorageSimulation> {
    ensure!(
        first_seen_keys.len() == counts.len(),
        "first-seen registry has {} keys but counts has {} rows",
        first_seen_keys.len(),
        counts.len()
    );
    ensure!(
        first_seen_keys.len() <= u32::MAX as usize,
        "first-seen registry exceeds u32 ids"
    );
    let started = Instant::now();
    let prior_rows_taken = seed_limit.min(prior_frequency_keys.len());
    let seed_keys = build_seed_keys(builtin_keys, prior_frequency_keys, prior_rows_taken)?;

    let mut weighted_uleb128_id_bytes = 0u128;
    let mut counted_refs = 0u64;
    let order = visit_simulated_order(
        first_seen_keys,
        counts,
        first_seen_table,
        &seed_keys,
        |_key, one_based_id, count| {
            add_weighted_id_bytes(
                &mut weighted_uleb128_id_bytes,
                &mut counted_refs,
                one_based_id,
                count,
            )
        },
    )?;

    let registry_keys =
        usize::try_from(order.next_id - 1).context("registry key count overflow")?;
    ensure!(
        registry_keys <= u32::MAX as usize,
        "simulated registry exceeds u32 ids"
    );
    let expected_refs = counts
        .iter()
        .try_fold(0u64, |sum, &count| sum.checked_add(u64::from(count)))
        .context("registry count sum overflow")?;
    ensure!(
        counted_refs == expected_refs,
        "simulated reference count {counted_refs} differs from input {expected_refs}"
    );

    Ok(StorageSimulation {
        requested_seed_limit: seed_limit,
        prior_rows_taken,
        seeded_keys: seed_keys.len(),
        zero_count_seeded_keys: order.zero_count_seeded_keys,
        registry_keys,
        registry_bytes: registry_keys as u128 * 32,
        counted_refs,
        weighted_uleb128_id_bytes,
        elapsed: started.elapsed(),
        rss_mib: rss_mib(),
    })
}

fn build_seed_keys(
    builtin_keys: &[[u8; 32]],
    prior_frequency_keys: &[[u8; 32]],
    prior_rows_taken: usize,
) -> Result<Vec<[u8; 32]>> {
    ensure!(
        prior_rows_taken <= prior_frequency_keys.len(),
        "requested {prior_rows_taken} prior rows but registry has {}",
        prior_frequency_keys.len()
    );
    let requested_seed_capacity = builtin_keys
        .len()
        .checked_add(prior_rows_taken)
        .context("seed key capacity overflow")?;
    let mut seed_keys = Vec::<[u8; 32]>::with_capacity(requested_seed_capacity);
    let mut seed_table = HashTable::<u32>::with_capacity(requested_seed_capacity);
    for key in builtin_keys
        .iter()
        .chain(prior_frequency_keys[..prior_rows_taken].iter())
    {
        insert_unique_seed(&mut seed_keys, &mut seed_table, *key)?;
    }
    Ok(seed_keys)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SimulatedOrderStats {
    next_id: u64,
    zero_count_seeded_keys: usize,
}

fn visit_simulated_order<F>(
    first_seen_keys: &[[u8; 32]],
    counts: &[u32],
    first_seen_table: &HashTable<u32>,
    seed_keys: &[[u8; 32]],
    mut visit: F,
) -> Result<SimulatedOrderStats>
where
    F: FnMut(&[u8; 32], u64, u32) -> Result<()>,
{
    ensure!(
        first_seen_keys.len() == counts.len(),
        "first-seen registry has {} keys but counts has {} rows",
        first_seen_keys.len(),
        counts.len()
    );
    let mut seeded_existing = vec![false; first_seen_keys.len()];
    let mut next_id = 1u64;
    let mut zero_count_seeded_keys = 0usize;

    for key in seed_keys {
        let count = find_zero_based_id(first_seen_table, first_seen_keys, key)
            .map(|id| {
                seeded_existing[id as usize] = true;
                counts[id as usize]
            })
            .unwrap_or(0);
        if count == 0 {
            zero_count_seeded_keys += 1;
        }
        visit(key, next_id, count)?;
        next_id = next_id.checked_add(1).context("registry id overflow")?;
    }

    for (zero_based, (key, &count)) in first_seen_keys.iter().zip(counts).enumerate() {
        if seeded_existing[zero_based] || count == 0 {
            continue;
        }
        visit(key, next_id, count)?;
        next_id = next_id.checked_add(1).context("registry id overflow")?;
    }

    Ok(SimulatedOrderStats {
        next_id,
        zero_count_seeded_keys,
    })
}

fn insert_unique_seed(
    seed_keys: &mut Vec<[u8; 32]>,
    seed_table: &mut HashTable<u32>,
    key: [u8; 32],
) -> Result<()> {
    let hash = key_hash(&key);
    if seed_table
        .find(hash, |candidate| seed_keys[*candidate as usize] == key)
        .is_some()
    {
        return Ok(());
    }
    let zero_based = u32::try_from(seed_keys.len()).context("seed registry exceeds u32 ids")?;
    seed_keys.push(key);
    seed_table.insert_unique(hash, zero_based, |candidate| {
        key_hash(&seed_keys[*candidate as usize])
    });
    Ok(())
}

fn find_zero_based_id(table: &HashTable<u32>, keys: &[[u8; 32]], key: &[u8; 32]) -> Option<u32> {
    table
        .find(key_hash(key), |candidate| keys[*candidate as usize] == *key)
        .copied()
}

fn add_weighted_id_bytes(
    weighted_bytes: &mut u128,
    counted_refs: &mut u64,
    one_based_id: u64,
    count: u32,
) -> Result<()> {
    ensure!(one_based_id > 0, "compact registry id zero is reserved");
    ensure!(
        one_based_id <= u64::from(u32::MAX),
        "compact registry id {one_based_id} exceeds u32"
    );
    let width = unsigned_leb128_width(one_based_id as u32);
    *weighted_bytes = weighted_bytes
        .checked_add(u128::from(count) * u128::from(width))
        .context("weighted ID byte count overflow")?;
    *counted_refs = counted_refs
        .checked_add(u64::from(count))
        .context("reference count overflow")?;
    Ok(())
}

fn weighted_bytes_for_input_order(counts: &[u32]) -> Result<(u64, u128)> {
    let mut refs = 0u64;
    let mut bytes = 0u128;
    for (zero_based, &count) in counts.iter().enumerate() {
        let one_based_id = u64::try_from(zero_based)
            .context("registry id exceeds u64")?
            .checked_add(1)
            .context("registry id overflow")?;
        add_weighted_id_bytes(&mut bytes, &mut refs, one_based_id, count)?;
    }
    Ok((refs, bytes))
}

#[inline]
fn unsigned_leb128_width(value: u32) -> u8 {
    debug_assert!(value > 0);
    match value {
        0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0x0fff_ffff => 4,
        _ => 5,
    }
}

fn weighted_samples(counts: &[u32], total: u64, len: usize) -> Result<Vec<u32>> {
    ensure!(
        counts.len() <= u32::MAX as usize,
        "registry exceeds u32 ids"
    );
    let mut cumulative = Vec::with_capacity(counts.len());
    let mut sum = 0u64;
    for &count in counts {
        sum = sum
            .checked_add(u64::from(count))
            .context("registry count sum overflow")?;
        cumulative.push(sum);
    }
    ensure!(sum == total, "cumulative count mismatch");

    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let random = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
        let target = random % total;
        let index = cumulative.partition_point(|&end| end <= target);
        let index = u32::try_from(index).map_err(|_| anyhow!("sample id exceeds u32"))?;
        out.push(index);
    }
    Ok(out)
}

fn key_hash(key: &[u8; 32]) -> u64 {
    gxhash64(key, HASH_SEED)
}

fn build_id_table(keys: &[[u8; 32]]) -> Result<HashTable<u32>> {
    ensure!(keys.len() <= u32::MAX as usize, "registry exceeds u32 ids");
    let mut table = HashTable::with_capacity(keys.len());
    for (id, key) in keys.iter().enumerate() {
        let id = id as u32;
        let hash = key_hash(key);
        ensure!(
            table
                .find(hash, |candidate| keys[*candidate as usize] == *key)
                .is_none(),
            "duplicate registry key at zero-based id {id}"
        );
        table.insert_unique(hash, id, |candidate| key_hash(&keys[*candidate as usize]));
    }
    Ok(table)
}

#[derive(Clone, Copy)]
struct Timing {
    elapsed: Duration,
    refs: usize,
    checksum: u64,
}

fn bench_mphf(index: &KeyIndex, keys: &[[u8; 32]], samples: &[u32]) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = &keys[sample as usize];
        let id = index
            .lookup(black_box(key))
            .context("MPHF missed registry key")?;
        checksum = checksum.wrapping_add(u64::from(black_box(id)));
    }
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_id_table(
    table: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    increment_counts: bool,
) -> Result<Timing> {
    let mut counts = increment_counts.then(|| vec![0u32; keys.len()]);
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = &keys[sample as usize];
        let id = *table
            .find(key_hash(black_box(key)), |candidate| {
                keys[*candidate as usize] == *key
            })
            .context("id table missed registry key")?;
        if let Some(counts) = counts.as_mut() {
            counts[id as usize] = counts[id as usize].saturating_add(1);
        }
        checksum = checksum.wrapping_add(u64::from(black_box(id)) + 1);
    }
    if let Some(counts) = counts {
        checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    }
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_gx_map(
    map: &GxHashMap<[u8; 32], u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
) -> Result<Timing> {
    let mut counts = vec![0u32; keys.len()];
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = &keys[sample as usize];
        let id = *map
            .get(black_box(key))
            .context("GxHashMap missed registry key")?;
        counts[id as usize] = counts[id as usize].saturating_add(1);
        checksum = checksum.wrapping_add(u64::from(black_box(id)) + 1);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn print_timing(name: &str, timing: Timing, epoch_refs: u64) {
    let elapsed_s = timing.elapsed.as_secs_f64();
    let ns_per_ref = timing.elapsed.as_nanos() as f64 / timing.refs as f64;
    let epoch_cpu_s = ns_per_ref * epoch_refs as f64 / 1e9;
    println!(
        "{name} refs={} elapsed_s={elapsed_s:.6} refs_s={:.0} ns_ref={ns_per_ref:.3} epoch_cpu_s={epoch_cpu_s:.1} checksum={} rss_mib={:.1}",
        timing.refs,
        timing.refs as f64 / elapsed_s,
        timing.checksum,
        rss_mib()
    );
}

fn rss_mib() -> f64 {
    #[cfg(target_os = "linux")]
    {
        let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
            return f64::NAN;
        };
        let Some(pages) = statm.split_whitespace().nth(1) else {
            return f64::NAN;
        };
        let Ok(pages) = pages.parse::<u64>() else {
            return f64::NAN;
        };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        return pages as f64 * page_size as f64 / (1024.0 * 1024.0);
    }

    #[cfg(not(target_os = "linux"))]
    f64::NAN
}

fn trim_allocator() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: u32) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[..4].copy_from_slice(&value.to_le_bytes());
        key
    }

    #[test]
    fn default_seed_limits_cover_two_and_three_byte_id_ranges() {
        let args = Args::try_parse_from(["bench", "registry.bin", "registry_counts.bin"])
            .expect("default arguments should parse");
        assert!(!args.simulate_storage);
        assert!(!args.storage_only);
        assert_eq!(args.seed_limits, [65_536, 2_097_151]);
        assert_eq!(args.base_seed_limit, 65_536);
    }

    #[test]
    fn simulated_order_is_builtins_then_unique_prior_then_first_seen_remainder() -> Result<()> {
        let [a, b, c, d, old_zero_count_seed] = [key(1), key(2), key(3), key(4), key(5)];
        let first_seen = [a, b, c, old_zero_count_seed];
        let counts = [5, 7, 11, 0];
        let prior = [b, c, d, a];
        let builtins = [b];
        let seeds = build_seed_keys(&builtins, &prior, 3)?;
        assert_eq!(seeds, [b, c, d]);

        let table = build_id_table(&first_seen)?;
        let mut observed = Vec::new();
        let order =
            visit_simulated_order(&first_seen, &counts, &table, &seeds, |key, id, count| {
                observed.push((*key, id, count));
                Ok(())
            })?;
        assert_eq!(observed, vec![(b, 1, 7), (c, 2, 11), (d, 3, 0), (a, 4, 5)]);
        assert_eq!(
            order,
            SimulatedOrderStats {
                next_id: 5,
                zero_count_seeded_keys: 1,
            }
        );

        let simulation =
            simulate_storage_order(&first_seen, &counts, &table, &prior, 3, &builtins)?;
        assert_eq!(simulation.prior_rows_taken, 3);
        assert_eq!(simulation.seeded_keys, 3);
        assert_eq!(simulation.zero_count_seeded_keys, 1);
        assert_eq!(simulation.registry_keys, 4);
        assert_eq!(simulation.registry_bytes, 128);
        assert_eq!(simulation.counted_refs, 23);
        assert_eq!(simulation.weighted_uleb128_id_bytes, 23);
        Ok(())
    }

    #[test]
    fn unsigned_leb128_width_matches_u32_boundaries() {
        for value in [1, 127] {
            assert_eq!(unsigned_leb128_width(value), 1, "value={value}");
        }
        for value in [128, 16_383] {
            assert_eq!(unsigned_leb128_width(value), 2, "value={value}");
        }
        for value in [16_384, 2_097_151] {
            assert_eq!(unsigned_leb128_width(value), 3, "value={value}");
        }
        for value in [2_097_152, 268_435_455] {
            assert_eq!(unsigned_leb128_width(value), 4, "value={value}");
        }
        for value in [268_435_456, u32::MAX] {
            assert_eq!(unsigned_leb128_width(value), 5, "value={value}");
        }
    }

    #[test]
    fn simulation_uses_one_based_ids_for_weighted_widths() -> Result<()> {
        let first_seen: Vec<_> = (1..=128).map(key).collect();
        let mut counts = vec![1; first_seen.len()];
        counts[127] = 10;
        let table = build_id_table(&first_seen)?;

        let baseline = simulate_storage_order(&first_seen, &counts, &table, &[], 0, &[])?;
        assert_eq!(baseline.registry_keys, 128);
        assert_eq!(baseline.counted_refs, 137);
        assert_eq!(baseline.weighted_uleb128_id_bytes, 147);

        let seeded =
            simulate_storage_order(&first_seen, &counts, &table, &[first_seen[127]], 1, &[])?;
        assert_eq!(seeded.registry_keys, 128);
        assert_eq!(seeded.counted_refs, 137);
        assert_eq!(seeded.weighted_uleb128_id_bytes, 138);
        Ok(())
    }
}
