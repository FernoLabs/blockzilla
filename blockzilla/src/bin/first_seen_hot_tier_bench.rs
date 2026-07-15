use std::{
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_format::{KeyStore, framed::read_u32_varint};
use clap::Parser;
use gxhash::gxhash64;
use hashbrown::HashTable;

const HASH_SEED: i64 = 0x425a_4653_4931;
const EMPTY_LINEAR_SLOT: u32 = u32::MAX;
const EMPTY_LINEAR_U16_SLOT: u16 = u16::MAX;

/// Compare collision-exact hot tiers for the one-pass first-seen interner.
#[derive(Debug, Parser)]
struct Args {
    /// Completed first-seen registry.bin.
    registry: PathBuf,

    /// LEB128 registry_counts.bin matching registry.bin.
    counts: PathBuf,

    /// Real-distribution references in each timed pass.
    #[arg(long, default_value_t = 25_000_000)]
    samples: usize,

    /// Timed passes per lookup candidate.
    #[arg(long, default_value_t = 3)]
    passes: usize,

    /// Full-epoch reference count used to extrapolate lookup CPU time.
    #[arg(long, default_value_t = 10_003_134_293)]
    epoch_refs: u64,

    /// Seed prefixes to benchmark as individual narrow Swiss tables.
    #[arg(
        long = "hot-limit",
        value_delimiter = ',',
        default_value = "128,4096,16384,65536"
    )]
    hot_limits: Vec<usize>,

    /// Tiny direct prefixes to test before computing a hash.
    #[arg(
        long = "direct-prefix",
        value_delimiter = ',',
        default_value = "1,2,4,8"
    )]
    direct_prefixes: Vec<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.samples > 0, "--samples must be non-zero");
    ensure!(args.passes > 0, "--passes must be non-zero");

    let load_started = Instant::now();
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
        "load keys={} refs={} elapsed_s={:.3} rss_mib={:.1}",
        store.len(),
        total_refs,
        load_started.elapsed().as_secs_f64(),
        rss_mib()
    );

    let mut coverage_limits = args.hot_limits.clone();
    coverage_limits.extend(args.direct_prefixes.iter().copied());
    coverage_limits.extend([128, 65_536]);
    coverage_limits.sort_unstable();
    coverage_limits.dedup();
    for limit in coverage_limits {
        ensure!(
            limit <= store.len(),
            "hot limit {limit} exceeds registry length {}",
            store.len()
        );
        let refs = prefix_refs(&counts, limit)?;
        println!(
            "coverage keys={} refs={} pct={:.9}",
            limit,
            refs,
            refs as f64 * 100.0 / total_refs as f64
        );
    }

    let sample_started = Instant::now();
    let samples = weighted_samples(&counts, total_refs, args.samples)?;
    println!(
        "samples rows={} bytes={} elapsed_s={:.3} rss_mib={:.1}",
        samples.len(),
        samples.len() * size_of::<u32>(),
        sample_started.elapsed().as_secs_f64(),
        rss_mib()
    );
    drop(counts);

    let full_build_started = Instant::now();
    let full = build_full_table(&store.keys)?;
    println!(
        "full_table keys={} capacity={} approx_bytes={} elapsed_s={:.3} rss_mib={:.1}",
        full.len(),
        full.capacity(),
        approximate_swiss_bytes(full.capacity(), size_of::<u32>()),
        full_build_started.elapsed().as_secs_f64(),
        rss_mib()
    );

    let mut scratch_counts = vec![0u32; store.len()];
    run_candidate(
        "baseline_full_start",
        args.passes,
        args.epoch_refs,
        &mut scratch_counts,
        |counts| bench_full(&full, &store.keys, &samples, counts),
    )?;

    for &limit in &args.hot_limits {
        ensure!(
            limit <= usize::from(u16::MAX) + 1,
            "narrow Swiss hot limit {limit} exceeds 65,536"
        );
        ensure!(limit <= store.len(), "hot limit exceeds registry length");
        let built = Instant::now();
        let hot = build_hot_u16(&store.keys, 0, limit)?;
        println!(
            "hot_table kind=swiss_u16 keys={} capacity={} approx_bytes={} elapsed_s={:.6} rss_mib={:.1}",
            hot.len(),
            hot.capacity(),
            approximate_swiss_bytes(hot.capacity(), size_of::<u16>()),
            built.elapsed().as_secs_f64(),
            rss_mib()
        );
        run_candidate(
            &format!("swiss_u16_{limit}"),
            args.passes,
            args.epoch_refs,
            &mut scratch_counts,
            |counts| bench_swiss_hot(&hot, &full, &store.keys, &samples, counts),
        )?;

        let built = Instant::now();
        let linear = LinearHotTable::build(&store.keys[..limit])?;
        println!(
            "hot_table kind=linear_u32 keys={} slots={} bytes={} elapsed_s={:.6} rss_mib={:.1}",
            limit,
            linear.slots.len(),
            linear.slots.len() * size_of::<u32>(),
            built.elapsed().as_secs_f64(),
            rss_mib()
        );
        run_candidate(
            &format!("linear_u32_{limit}"),
            args.passes,
            args.epoch_refs,
            &mut scratch_counts,
            |counts| bench_linear_hot(&linear, &full, &store.keys, &samples, counts),
        )?;

        let narrow_limit = limit.min(usize::from(u16::MAX));
        if narrow_limit > 0 {
            let built = Instant::now();
            let linear = LinearHotTableU16::build(&store.keys[..narrow_limit])?;
            println!(
                "hot_table kind=linear_u16 keys={} slots={} bytes={} elapsed_s={:.6} rss_mib={:.1}",
                narrow_limit,
                linear.slots.len(),
                linear.slots.len() * size_of::<u16>(),
                built.elapsed().as_secs_f64(),
                rss_mib()
            );
            run_candidate(
                &format!("linear_u16_{narrow_limit}"),
                args.passes,
                args.epoch_refs,
                &mut scratch_counts,
                |counts| bench_linear_hot_u16(&linear, &full, &store.keys, &samples, counts),
            )?;
        }
    }

    for &prefix in &args.direct_prefixes {
        ensure!(
            prefix <= store.len(),
            "direct prefix exceeds registry length"
        );
        run_candidate(
            &format!("direct_prefix_{prefix}"),
            args.passes,
            args.epoch_refs,
            &mut scratch_counts,
            |counts| bench_direct_prefix(prefix, &full, &store.keys, &samples, counts),
        )?;
    }

    if store.len() >= 65_536 {
        let upper = build_hot_u16(&store.keys, 4, 65_536)?;
        run_candidate(
            "direct_prefix_4_then_swiss_u16_65536",
            args.passes,
            args.epoch_refs,
            &mut scratch_counts,
            |counts| bench_direct_then_swiss(4, &upper, &full, &store.keys, &samples, counts),
        )?;

        let tiny = build_hot_u8(&store.keys, 0, 128)?;
        let upper = build_hot_u16(&store.keys, 128, 65_536)?;
        println!(
            "hot_table kind=hierarchical_u8_u16 keys=65536 approx_bytes={} rss_mib={:.1}",
            approximate_swiss_bytes(tiny.capacity(), size_of::<u8>())
                + approximate_swiss_bytes(upper.capacity(), size_of::<u16>()),
            rss_mib()
        );
        run_candidate(
            "hierarchical_swiss_128_65536",
            args.passes,
            args.epoch_refs,
            &mut scratch_counts,
            |counts| bench_hierarchical_swiss(&tiny, &upper, &full, &store.keys, &samples, counts),
        )?;
    }

    run_candidate(
        "baseline_full_end",
        args.passes,
        args.epoch_refs,
        &mut scratch_counts,
        |counts| bench_full(&full, &store.keys, &samples, counts),
    )?;

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

fn prefix_refs(counts: &[u32], limit: usize) -> Result<u64> {
    counts[..limit]
        .iter()
        .try_fold(0u64, |sum, &count| sum.checked_add(u64::from(count)))
        .context("prefix reference count overflow")
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
        out.push(u32::try_from(index).map_err(|_| anyhow!("sample id exceeds u32"))?);
    }
    Ok(out)
}

#[inline]
fn key_hash(key: &[u8; 32]) -> u64 {
    gxhash64(key, HASH_SEED)
}

fn build_full_table(keys: &[[u8; 32]]) -> Result<HashTable<u32>> {
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

fn build_hot_u16(keys: &[[u8; 32]], start: usize, end: usize) -> Result<HashTable<u16>> {
    ensure!(start <= end && end <= keys.len(), "invalid u16 hot range");
    ensure!(end <= usize::from(u16::MAX) + 1, "u16 hot id overflow");
    let mut table = HashTable::with_capacity(end - start);
    for id in start..end {
        let id = id as u16;
        let hash = key_hash(&keys[usize::from(id)]);
        table.insert_unique(hash, id, |candidate| {
            key_hash(&keys[usize::from(*candidate)])
        });
    }
    Ok(table)
}

fn build_hot_u8(keys: &[[u8; 32]], start: usize, end: usize) -> Result<HashTable<u8>> {
    ensure!(start <= end && end <= keys.len(), "invalid u8 hot range");
    ensure!(end <= usize::from(u8::MAX) + 1, "u8 hot id overflow");
    let mut table = HashTable::with_capacity(end - start);
    for id in start..end {
        let id = id as u8;
        let hash = key_hash(&keys[usize::from(id)]);
        table.insert_unique(hash, id, |candidate| {
            key_hash(&keys[usize::from(*candidate)])
        });
    }
    Ok(table)
}

struct LinearHotTable {
    slots: Box<[u32]>,
    mask: usize,
}

struct LinearHotTableU16 {
    slots: Box<[u16]>,
    mask: usize,
}

impl LinearHotTableU16 {
    fn build(keys: &[[u8; 32]]) -> Result<Self> {
        ensure!(
            keys.len() <= usize::from(u16::MAX),
            "linear u16 hot id overflow"
        );
        let slots = keys
            .len()
            .checked_mul(2)
            .context("linear u16 table capacity overflow")?
            .max(1)
            .next_power_of_two();
        let mut table = Self {
            slots: vec![EMPTY_LINEAR_U16_SLOT; slots].into_boxed_slice(),
            mask: slots - 1,
        };
        for (id, key) in keys.iter().enumerate() {
            let mut slot = key_hash(key) as usize & table.mask;
            loop {
                if table.slots[slot] == EMPTY_LINEAR_U16_SLOT {
                    table.slots[slot] = id as u16;
                    break;
                }
                slot = (slot + 1) & table.mask;
            }
        }
        Ok(table)
    }

    #[inline]
    fn lookup(&self, hash: u64, key: &[u8; 32], keys: &[[u8; 32]]) -> Option<u32> {
        let mut slot = hash as usize & self.mask;
        loop {
            let candidate = self.slots[slot];
            if candidate == EMPTY_LINEAR_U16_SLOT {
                return None;
            }
            if keys[usize::from(candidate)] == *key {
                return Some(u32::from(candidate));
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

impl LinearHotTable {
    fn build(keys: &[[u8; 32]]) -> Result<Self> {
        ensure!(keys.len() <= u32::MAX as usize, "linear hot id overflow");
        let slots = keys
            .len()
            .checked_mul(2)
            .context("linear table capacity overflow")?
            .max(1)
            .next_power_of_two();
        let mut table = Self {
            slots: vec![EMPTY_LINEAR_SLOT; slots].into_boxed_slice(),
            mask: slots - 1,
        };
        for (id, key) in keys.iter().enumerate() {
            let mut slot = key_hash(key) as usize & table.mask;
            loop {
                if table.slots[slot] == EMPTY_LINEAR_SLOT {
                    table.slots[slot] = id as u32;
                    break;
                }
                slot = (slot + 1) & table.mask;
            }
        }
        Ok(table)
    }

    #[inline]
    fn lookup(&self, hash: u64, key: &[u8; 32], keys: &[[u8; 32]]) -> Option<u32> {
        let mut slot = hash as usize & self.mask;
        loop {
            let candidate = self.slots[slot];
            if candidate == EMPTY_LINEAR_SLOT {
                return None;
            }
            if keys[candidate as usize] == *key {
                return Some(candidate);
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

#[derive(Clone, Copy)]
struct Timing {
    elapsed: Duration,
    refs: usize,
    checksum: u64,
}

fn run_candidate<F>(
    name: &str,
    passes: usize,
    epoch_refs: u64,
    scratch_counts: &mut [u32],
    mut run: F,
) -> Result<()>
where
    F: FnMut(&mut [u32]) -> Result<Timing>,
{
    let mut timings = Vec::with_capacity(passes);
    for pass in 0..passes {
        scratch_counts.fill(0);
        let timing = run(scratch_counts)?;
        println!(
            "timing name={name} pass={} refs={} elapsed_s={:.6} ns_ref={:.3} checksum={}",
            pass + 1,
            timing.refs,
            timing.elapsed.as_secs_f64(),
            timing.elapsed.as_nanos() as f64 / timing.refs as f64,
            timing.checksum
        );
        timings.push(timing);
    }
    timings.sort_unstable_by_key(|timing| timing.elapsed);
    let median = timings[timings.len() / 2];
    let ns_ref = median.elapsed.as_nanos() as f64 / median.refs as f64;
    println!(
        "summary name={name} passes={} median_s={:.6} median_ns_ref={:.3} projected_epoch_cpu_s={:.3} rss_mib={:.1}",
        passes,
        median.elapsed.as_secs_f64(),
        ns_ref,
        ns_ref * epoch_refs as f64 / 1e9,
        rss_mib()
    );
    Ok(())
}

#[inline]
fn count_and_checksum(counts: &mut [u32], id: u32, checksum: &mut u64) {
    let count = &mut counts[id as usize];
    *count = count.saturating_add(1);
    *checksum = checksum.wrapping_add(u64::from(black_box(id)) + 1);
}

fn bench_full(
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let hash = key_hash(key);
        let id = *full
            .find(hash, |candidate| keys[*candidate as usize] == *key)
            .context("full table missed registry key")?;
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_swiss_hot(
    hot: &HashTable<u16>,
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let hash = key_hash(key);
        let id = hot
            .find(hash, |candidate| keys[usize::from(*candidate)] == *key)
            .map(|candidate| u32::from(*candidate))
            .or_else(|| {
                full.find(hash, |candidate| keys[*candidate as usize] == *key)
                    .copied()
            })
            .context("two-tier table missed registry key")?;
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_linear_hot(
    hot: &LinearHotTable,
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let hash = key_hash(key);
        let id = hot
            .lookup(hash, key, keys)
            .or_else(|| {
                full.find(hash, |candidate| keys[*candidate as usize] == *key)
                    .copied()
            })
            .context("linear two-tier table missed registry key")?;
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_linear_hot_u16(
    hot: &LinearHotTableU16,
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let hash = key_hash(key);
        let id = hot
            .lookup(hash, key, keys)
            .or_else(|| {
                full.find(hash, |candidate| keys[*candidate as usize] == *key)
                    .copied()
            })
            .context("linear u16 two-tier table missed registry key")?;
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_direct_prefix(
    prefix: usize,
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let mut found = None;
        for (id, candidate) in keys[..prefix].iter().enumerate() {
            if candidate == key {
                found = Some(id as u32);
                break;
            }
        }
        let id = if let Some(id) = found {
            id
        } else {
            let hash = key_hash(key);
            *full
                .find(hash, |candidate| keys[*candidate as usize] == *key)
                .context("direct-prefix fallback missed registry key")?
        };
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_direct_then_swiss(
    prefix: usize,
    hot: &HashTable<u16>,
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let mut found = None;
        for (id, candidate) in keys[..prefix].iter().enumerate() {
            if candidate == key {
                found = Some(id as u32);
                break;
            }
        }
        let id = if let Some(id) = found {
            id
        } else {
            let hash = key_hash(key);
            hot.find(hash, |candidate| keys[usize::from(*candidate)] == *key)
                .map(|candidate| u32::from(*candidate))
                .or_else(|| {
                    full.find(hash, |candidate| keys[*candidate as usize] == *key)
                        .copied()
                })
                .context("direct+Swiss table missed registry key")?
        };
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn bench_hierarchical_swiss(
    tiny: &HashTable<u8>,
    hot: &HashTable<u16>,
    full: &HashTable<u32>,
    keys: &[[u8; 32]],
    samples: &[u32],
    counts: &mut [u32],
) -> Result<Timing> {
    let started = Instant::now();
    let mut checksum = 0u64;
    for &sample in samples {
        let key = black_box(&keys[sample as usize]);
        let hash = key_hash(key);
        let id = tiny
            .find(hash, |candidate| keys[usize::from(*candidate)] == *key)
            .map(|candidate| u32::from(*candidate))
            .or_else(|| {
                hot.find(hash, |candidate| keys[usize::from(*candidate)] == *key)
                    .map(|candidate| u32::from(*candidate))
            })
            .or_else(|| {
                full.find(hash, |candidate| keys[*candidate as usize] == *key)
                    .copied()
            })
            .context("hierarchical table missed registry key")?;
        count_and_checksum(counts, id, &mut checksum);
    }
    checksum = checksum.wrapping_add(u64::from(*black_box(&counts[0])));
    Ok(Timing {
        elapsed: started.elapsed(),
        refs: samples.len(),
        checksum,
    })
}

fn approximate_swiss_bytes(capacity: usize, element_bytes: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = if capacity < 8 {
        capacity.next_power_of_two().max(4)
    } else {
        capacity / 7 * 8
    };
    buckets.saturating_mul(element_bytes + 1).saturating_add(16)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swiss_collision_is_resolved_by_full_key() {
        let keys = [[1u8; 32], [2u8; 32]];
        let forced_hash = 7;
        let mut table = HashTable::new();
        table.insert_unique(forced_hash, 0u16, |_| forced_hash);
        table.insert_unique(forced_hash, 1u16, |_| forced_hash);

        let found = table
            .find(forced_hash, |candidate| {
                keys[usize::from(*candidate)] == keys[1]
            })
            .copied();
        assert_eq!(found, Some(1));
        assert!(
            table
                .find(forced_hash, |candidate| {
                    keys[usize::from(*candidate)] == [3u8; 32]
                })
                .is_none()
        );
    }

    #[test]
    fn linear_collision_is_resolved_by_full_key() -> Result<()> {
        let keys = [[1u8; 32], [2u8; 32], [3u8; 32]];
        let table = LinearHotTable::build(&keys)?;
        for (id, key) in keys.iter().enumerate() {
            assert_eq!(table.lookup(key_hash(key), key, &keys), Some(id as u32));
        }
        assert_eq!(table.lookup(key_hash(&[9u8; 32]), &[9u8; 32], &keys), None);
        Ok(())
    }

    #[test]
    fn linear_u16_collision_is_resolved_by_full_key() -> Result<()> {
        let keys = [[1u8; 32], [2u8; 32], [3u8; 32]];
        let table = LinearHotTableU16::build(&keys)?;
        for (id, key) in keys.iter().enumerate() {
            assert_eq!(table.lookup(key_hash(key), key, &keys), Some(id as u32));
        }
        assert_eq!(table.lookup(key_hash(&[9u8; 32]), &[9u8; 32], &keys), None);
        Ok(())
    }
}
