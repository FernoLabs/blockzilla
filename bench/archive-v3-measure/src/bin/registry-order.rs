//! Report whether each epoch's pubkey registry is in canonical usage order.
//!
//! `registry_counts.bin` holds one `u32` varint per registry entry, in registry
//! order, written by `write_registry_counts`. A canonical registry is
//! non-increasing in that value, so the check is a streaming comparison.
//!
//! Sortedness alone is not the interesting number. Two epochs can both be
//! "unsorted" while one keeps its hot accounts near the front and the other
//! scatters them. **Head purity** measures that directly: of the K
//! most-referenced accounts, how many actually sit in the first K ordinals?
//! That is exactly the property an ordinal-threshold index split depends on,
//! and the property the varint encoding is paid for.
//!
//! Only epochs that fail the cheap sortedness check pay for the head-purity
//! histogram.

use std::{env, fs, path::Path};

use anyhow::{Context, Result};

/// Hot-set size to score. Matches `seeded_keys` in `registry-first-seen.manifest`.
const HEAD: usize = 65_536;

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
    Ok(counts)
}

/// Share of the true top-`HEAD` accounts that already sit in the first `HEAD`
/// ordinals, as a percentage.
fn head_purity(counts: &[u32]) -> f64 {
    let head = HEAD.min(counts.len());
    if head == 0 {
        return 100.0;
    }
    let mut sorted: Vec<u32> = counts.to_vec();
    // Only the boundary value matters, so a select beats a full sort.
    let (_, threshold, _) = sorted.select_nth_unstable_by(head - 1, |a, b| b.cmp(a));
    let threshold = *threshold;
    let hits = counts[..head]
        .iter()
        .filter(|count| **count >= threshold)
        .count();
    hits as f64 / head as f64 * 100.0
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let root = std::path::PathBuf::from(args.next().context("usage: registry-order <root>")?);

    let mut epochs: Vec<u64> = fs::read_dir(&root)?
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().to_string_lossy().into_owned();
            name.strip_prefix("epoch-")?.parse::<u64>().ok()
        })
        .collect();
    epochs.sort_unstable();

    println!(
        "{:<7} {:>11} {:>12} {:>8} {:>10}  {}",
        "epoch", "keys", "inversions", "head%", "declared", "verdict"
    );
    let (mut canonical, mut needs_reorder, mut skipped) = (0_u64, 0_u64, 0_u64);
    for epoch in epochs {
        let dir = root.join(format!("epoch-{epoch}"));
        let counts_path = dir.join("registry_counts.bin");
        let registry_path = dir.join("registry.bin");
        if !counts_path.exists() || !registry_path.exists() {
            println!("{epoch:<7} {:>11} -- missing counts or registry", "-");
            skipped += 1;
            continue;
        }
        let keys = fs::metadata(&registry_path)?.len() / 32;
        let counts = read_counts(&counts_path)?;

        let declared = dir.join("registry-first-seen.manifest").exists();

        if counts.len() as u64 != keys {
            println!(
                "{epoch:<7} {keys:>11} {:>12} {:>8} {:>10}  COUNT/KEY MISMATCH ({} counts)",
                "-",
                "-",
                if declared { "first_seen" } else { "-" },
                counts.len()
            );
            skipped += 1;
            continue;
        }

        // Registry index 0 is a reserved sentinel with zero references -- the
        // new format's 1-based ordinals treat 0 as the inline-key marker. It is
        // followed by the most-referenced account, so a naive scan reports one
        // inversion on every canonical epoch. Score from index 1.
        let scored = if counts.first() == Some(&0) {
            &counts[1..]
        } else {
            &counts[..]
        };
        let inversions = scored.windows(2).filter(|pair| pair[0] < pair[1]).count();
        if inversions == 0 {
            canonical += 1;
            println!(
                "{epoch:<7} {keys:>11} {inversions:>12} {:>8.1} {:>10}  canonical",
                100.0,
                if declared { "first_seen" } else { "-" }
            );
        } else {
            needs_reorder += 1;
            println!(
                "{epoch:<7} {keys:>11} {inversions:>12} {:>8.1} {:>10}  NEEDS REORDER",
                head_purity(scored),
                if declared { "first_seen" } else { "-" }
            );
        }
    }
    println!("canonical {canonical}, needs reorder {needs_reorder}, skipped {skipped}");
    Ok(())
}
