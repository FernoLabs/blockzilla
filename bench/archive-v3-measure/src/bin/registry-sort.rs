//! Build the usage-sorted permutation for one epoch's pubkey registry.
//!
//! This is Phase 1 of `docs/design/registry-reorder-rebuild-plan.md`: it moves
//! no block bytes. It reads `registry.bin` and `registry_counts.bin`, computes
//! the ordering the archive wants, and proves the result before writing
//! anything.
//!
//! The same permutation serves both live paths — the converter applying it on
//! the way into the Index Archive (`registry-ordering-plan.md`), and any
//! in-place rewrite of a Compact V2 source. Producing it separately means the
//! sort can be checked on real data before either consumer trusts it.
//!
//! **The tie-break is not cosmetic.** Between 34% and 85% of accounts have a
//! reference count of exactly 1, and there are only tens of thousands of
//! distinct count values across tens of millions of keys, so ordering by count
//! alone leaves roughly a third of the registry unordered. Ties break on the
//! 32-byte key ascending, which makes the output reproducible across runs and
//! machines.
//!
//! **There is no sentinel entry in `registry.bin`.** `Registry::get` resolves an
//! ordinal with `keys.get(id.checked_sub(1)?)`, so ordinal 1 is file index 0 and
//! ordinal 0 — `CompactPubkey::RAW_SENTINEL` — never indexes the file at all.
//! The sentinel lives in the ordinal space, not in the key array. Every entry
//! from index 0 onward is a real, referenced key and takes part in the sort.
//!
//! `registry-reorder-rebuild-plan.md` states the opposite ("index 0 is a
//! reserved sentinel with count 0"). The data disagrees: epoch 277 has 18,610
//! references at index 0, and canonical epoch 500 has 805,040,655 there — its
//! maximum, exactly where a usage-sorted registry puts its hottest key.

use std::{
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};

/// Hot-set size to score, matching `seeded_keys` in `registry-first-seen.manifest`.
const HEAD: usize = 65_536;

const KEY_LEN: usize = 32;

fn read_counts(path: &Path) -> Result<Vec<u32>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut counts = Vec::new();
    let (mut value, mut shift) = (0_u32, 0_u32);
    for byte in bytes {
        value |= u32::from(byte & 0x7f)
            .checked_shl(shift)
            .context("registry count varint overflows u32")?;
        if byte & 0x80 == 0 {
            counts.push(value);
            value = 0;
            shift = 0;
        } else {
            shift += 7;
        }
    }
    ensure!(shift == 0, "registry counts end mid-varint");
    Ok(counts)
}

fn write_counts(path: &Path, counts: &[u32]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut out = BufWriter::new(file);
    for &count in counts {
        let mut value = count;
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.write_all(&[byte])?;
            if value == 0 {
                break;
            }
        }
    }
    out.flush()?;
    Ok(())
}

/// Share of the true top-`HEAD` accounts sitting in the first `HEAD` ordinals.
fn head_purity(counts: &[u32]) -> f64 {
    let head = HEAD.min(counts.len());
    if head == 0 {
        return 100.0;
    }
    let mut scratch: Vec<u32> = counts.to_vec();
    let (_, threshold, _) = scratch.select_nth_unstable_by(head - 1, |a, b| b.cmp(a));
    let threshold = *threshold;
    let hits = counts[..head].iter().filter(|&&c| c >= threshold).count();
    100.0 * hits as f64 / head as f64
}

fn key_at(keys: &[u8], index: usize) -> &[u8] {
    &keys[index * KEY_LEN..(index + 1) * KEY_LEN]
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: registry-sort <epoch-dir> [--write]");
        eprintln!();
        eprintln!("Reads registry.bin and registry_counts.bin, proves the sort, and");
        eprintln!("reports. With --write also emits registry.bin.sorted,");
        eprintln!("registry_counts.bin.sorted and registry-permutation.u32.");
        std::process::exit(2);
    }
    let dir = PathBuf::from(&args[1]);
    let write = args.iter().any(|a| a == "--write");

    let keys = fs::read(dir.join("registry.bin"))
        .with_context(|| format!("read {}", dir.join("registry.bin").display()))?;
    let counts = read_counts(&dir.join("registry_counts.bin"))?;

    ensure!(
        keys.len() % KEY_LEN == 0,
        "registry.bin is {} bytes, not a multiple of {KEY_LEN}",
        keys.len()
    );
    let n = keys.len() / KEY_LEN;
    ensure!(
        n == counts.len(),
        "registry.bin holds {n} keys but registry_counts.bin holds {} counts",
        counts.len()
    );
    ensure!(n > 1, "registry has {n} entries, nothing to order");

    let total_refs: u64 = counts.iter().map(|&c| u64::from(c)).sum();
    println!("keys           {n}");
    println!("references     {total_refs}");
    println!("counts[0]      {} (a real key: ordinal 1)", counts[0]);
    println!("head purity    {:.1}% before", head_purity(&counts));

    let already = (1..n).all(|i| counts[i] <= counts[i - 1]);
    if already {
        println!("\nalready usage-sorted — nothing to do");
        return Ok(());
    }

    // Sort indices rather than (count, key) pairs: the keys stay where they are
    // and only 4 bytes per entry move during the sort.
    let mut order: Vec<u32> = (0..n as u32).collect();
    order.sort_unstable_by(|&a, &b| {
        let (a, b) = (a as usize, b as usize);
        counts[b]
            .cmp(&counts[a])
            .then_with(|| key_at(&keys, a).cmp(key_at(&keys, b)))
    });

    // new_index[old_file_index] = new_file_index. Ordinals are these plus one.
    let mut new_index = vec![0_u32; n];
    for (rank, &old) in order.iter().enumerate() {
        new_index[old as usize] = rank as u32;
    }

    // ---- gates -------------------------------------------------------------
    // Each one has failed in some earlier form of this work, so none is
    // ceremonial. Nothing is written unless all of them pass.
    let mut failures = Vec::new();

    if !order
        .windows(2)
        .all(|w| counts[w[1] as usize] <= counts[w[0] as usize])
    {
        failures.push("permuted counts are not non-increasing");
    }
    {
        let mut seen = vec![false; n];
        let mut bijection = true;
        for &new in &new_index {
            let new = new as usize;
            if new >= n || seen[new] {
                bijection = false;
                break;
            }
            seen[new] = true;
        }
        if !bijection || !seen.iter().all(|&s| s) {
            failures.push("permutation is not a bijection over 0..n");
        }
    }
    {
        // The multiset of keys must be untouched. Folding an order-independent
        // digest over both sides proves that in constant memory — materialising
        // two sorted key lists would cost ~1.2 GiB on the largest epoch, which
        // this host does not have. XOR catches a swapped or dropped key; the
        // wrapping sum catches a key duplicated an even number of times, which
        // XOR alone would hide.
        let digest = |mut acc: (u64, u64), key: &[u8]| {
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for &b in key {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x1000_0000_01b3);
            }
            acc.0 ^= h;
            acc.1 = acc.1.wrapping_add(h);
            acc
        };
        let before = (0..n).fold((0u64, 0u64), |a, i| digest(a, key_at(&keys, i)));
        let after = order.iter().fold((0u64, 0u64), |a, &old| {
            digest(a, key_at(&keys, old as usize))
        });
        if before != after {
            failures.push("key multiset changed under the permutation");
        }
    }
    let permuted_counts: Vec<u32> = order.iter().map(|&old| counts[old as usize]).collect();
    let purity_after = head_purity(&permuted_counts);
    if purity_after < 100.0 {
        failures.push("head purity after sorting is below 100%");
    }

    println!("head purity    {purity_after:.1}% after");
    println!();
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("GATE FAILED: {f}");
        }
        bail!("{} gate(s) failed; nothing written", failures.len());
    }
    println!("all gates passed: counts non-increasing, bijection,");
    println!("key multiset unchanged, head purity 100%");

    if !write {
        println!("\n(dry run — pass --write to emit the sorted files)");
        return Ok(());
    }

    let perm_path = dir.join("registry-permutation.u32");
    let mut perm = BufWriter::new(File::create(&perm_path)?);
    for &new in &new_index {
        perm.write_all(&new.to_le_bytes())?;
    }
    perm.flush()?;

    let keys_path = dir.join("registry.bin.sorted");
    let mut sorted_keys = BufWriter::new(File::create(&keys_path)?);
    for &old in &order {
        sorted_keys.write_all(key_at(&keys, old as usize))?;
    }
    sorted_keys.flush()?;

    let counts_path = dir.join("registry_counts.bin.sorted");
    write_counts(&counts_path, &permuted_counts)?;

    println!("\nwrote {}", perm_path.display());
    println!("wrote {}", keys_path.display());
    println!("wrote {}", counts_path.display());
    Ok(())
}
