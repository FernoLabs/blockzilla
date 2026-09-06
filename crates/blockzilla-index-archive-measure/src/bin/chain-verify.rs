//! Audit the blockhash chain of a Compact V2 archive.
//!
//! Two checks, each at the granularity where it is the cheap one:
//!
//! **Inside an epoch — by id.** Every block header carries `blockhash_id` and
//! `previous_blockhash_id`, both indexes into that epoch's
//! `blockhash_registry.bin`. If block *i*'s `previous_blockhash_id` is block
//! *i-1*'s `blockhash_id`, no block is missing between them. That is the only
//! sound test: Solana skips slots, so a gap in slot numbers proves nothing.
//! Comparing ids rather than 32-byte hashes is exact here because the registry
//! is the single owner of the hash for each ordinal.
//!
//! **Between epochs — by hash.** Ids are epoch-local and cannot link across a
//! boundary, so the join is checked against bytes:
//! `epoch-N/prev_blockhash_tail.bin` must repeat the last 300 records of
//! `epoch-(N-1)/blockhash_registry.bin`. 300 is Solana's recent-blockhash
//! window, which is why the tail exists at all.
//!
//! Blocks are read through `blockzilla-read-sdk`, which owns frame handling,
//! batched prefetch, decompressor reuse and every schema variant. An earlier
//! version of this tool parsed the block index and drove zstd itself, and
//! reintroduced a bug the SDK had already solved: it read a fixed-size header
//! window, so a block with no transactions -- whose payload is shorter than
//! that window -- was reported as undecodable. Do not re-add a private reader.

use std::{env, fs, os::unix::fs::FileExt, path::Path};

use anyhow::{Context, Result};
use blockzilla_archive_v2::ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE;
use blockzilla_compact_v2_reader::{
    ArchiveReader, HashVerification, OpenOptions, PinnedLocalRangeSource,
    manifest::TrustedGenerationIdentity,
};

const PREV_BLOCKHASH_TAIL_FILE: &str = "prev_blockhash_tail.bin";
const TAIL_RECORDS: usize = 300;
const TAIL_RECORD_LEN: usize = 40;
const HASH_LEN: usize = 32;

#[derive(Debug, Default)]
struct EpochReport {
    blocks: u64,
    faults: Vec<String>,
}

fn verify_epoch(dir: &Path, epoch: u64, slots_per_epoch: u64) -> Result<EpochReport> {
    // The archive has no generation manifest, so identity is asserted by the
    // caller and hashes are not verified. SizesOnly is what open_trusted
    // requires, and it is honest about what this audit does and does not prove.
    let reader = ArchiveReader::open_trusted(
        PinnedLocalRangeSource::new(dir),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".to_owned(),
            epoch,
            generation_id: format!("epoch-{epoch}"),
            slots_per_epoch,
        },
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .with_context(|| format!("open {}", dir.display()))?;

    let mut report = EpochReport::default();
    let registry_records =
        fs::metadata(dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?.len() / HASH_LEN as u64;
    // Epoch 0's registry carries one extra leading record for genesis, so its
    // ids run one ahead of the block ordinal. The converter special-cases the
    // same offset; deriving it here keeps any other value visible as a fault
    // instead of silently rebasing the check.
    let block_count = reader.index().rows.len() as u64;
    let registry_offset = registry_records.saturating_sub(block_count);
    if registry_offset > 1 {
        report.faults.push(format!(
            "registry has {registry_records} records for {block_count} blocks (offset {registry_offset})"
        ));
    }

    let mut previous: Option<(u64, u32)> = None; // (slot, blockhash_id)
    let mut position = 0_usize;
    let mut stream = reader.borrowed_blocks();
    while let Some(block) = stream.next_block() {
        let block = block.with_context(|| format!("decode block {position}"))?;
        let header = block.header();
        report.blocks += 1;

        // The registry owns one hash per block in block order, so a block's own
        // id must equal its ordinal. If that drifts, id comparison below stops
        // meaning what this audit assumes.
        if u64::from(header.blockhash_id) != position as u64 + registry_offset {
            report.faults.push(format!(
                "block {position} (slot {}): blockhash_id {} is not ordinal+{registry_offset}",
                header.slot, header.blockhash_id
            ));
        }
        if u64::from(header.blockhash_id) >= registry_records {
            report.faults.push(format!(
                "block {position}: blockhash_id {} outside {registry_records} registry records",
                header.blockhash_id
            ));
        }
        if let Some((previous_slot, previous_id)) = previous {
            if header.previous_blockhash_id != previous_id {
                report.faults.push(format!(
                    "block {position} (slot {}): previous_blockhash_id {} but block {} has id {previous_id}",
                    header.slot,
                    header.previous_blockhash_id,
                    position - 1
                ));
            }
            if header.parent_slot != previous_slot {
                report.faults.push(format!(
                    "block {position} (slot {}): parent_slot {} but previous block is slot {previous_slot}",
                    header.slot, header.parent_slot
                ));
            }
        }
        previous = Some((header.slot, header.blockhash_id));
        position += 1;
    }
    Ok(report)
}

/// Check one epoch boundary by hash: the tail repeats the predecessor's last 300.
fn verify_join(epoch_dir: &Path, previous_dir: &Path) -> Result<Option<String>> {
    let tail_path = epoch_dir.join(PREV_BLOCKHASH_TAIL_FILE);
    if !tail_path.exists() {
        return Ok(Some("no prev_blockhash_tail.bin".to_owned()));
    }
    let tail = fs::read(&tail_path).context("read predecessor tail")?;
    if tail.len() != TAIL_RECORDS * TAIL_RECORD_LEN {
        return Ok(Some(format!("tail is {} bytes", tail.len())));
    }
    let registry = fs::File::open(previous_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))
        .context("open predecessor registry")?;
    let size = registry.metadata()?.len();
    let span = (TAIL_RECORDS * HASH_LEN) as u64;
    if size < span {
        return Ok(Some(
            "predecessor registry shorter than 300 records".to_owned(),
        ));
    }
    let mut last = vec![0_u8; span as usize];
    registry.read_exact_at(&mut last, size - span)?;

    let mut zero_slots = 0_usize;
    for record in 0..TAIL_RECORDS {
        let at = record * TAIL_RECORD_LEN;
        if tail[at..at + HASH_LEN] != last[record * HASH_LEN..(record + 1) * HASH_LEN] {
            return Ok(Some(format!("hash mismatch at tail record {record}")));
        }
        let slot = u64::from_le_bytes(
            tail[at + HASH_LEN..at + TAIL_RECORD_LEN]
                .try_into()
                .expect("8 slot bytes"),
        );
        if slot == 0 {
            zero_slots += 1;
        }
    }
    if zero_slots == TAIL_RECORDS {
        // Hashes are right but slots were never written. The converter's
        // current-schema validator requires every slot inside the previous
        // epoch, so these epochs cannot convert until the slots are rebuilt.
        return Ok(Some(
            "hashes correct but all 300 tail slots are zero".to_owned(),
        ));
    }
    Ok(None)
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let root = std::path::PathBuf::from(
        args.next()
            .context("usage: chain-verify <archive-root> [epoch ...]")?,
    );
    let selected: Vec<u64> = args.map(|value| value.parse()).collect::<Result<_, _>>()?;
    let slots_per_epoch = 432_000_u64;

    let mut epochs: Vec<u64> = if selected.is_empty() {
        fs::read_dir(&root)?
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().to_string_lossy().into_owned();
                name.strip_prefix("epoch-")?.parse::<u64>().ok()
            })
            .collect()
    } else {
        selected
    };
    epochs.sort_unstable();

    let (mut clean, mut broken) = (0_u64, 0_u64);
    for epoch in epochs {
        let dir = root.join(format!("epoch-{epoch}"));
        let started = std::time::Instant::now();
        let report = match verify_epoch(&dir, epoch, slots_per_epoch) {
            Ok(report) => report,
            Err(error) => {
                println!("epoch-{epoch}: UNREADABLE: {error:#}");
                broken += 1;
                continue;
            }
        };
        let join = if epoch == 0 {
            None
        } else {
            verify_join(&dir, &root.join(format!("epoch-{}", epoch - 1)))?
        };
        let elapsed = started.elapsed().as_secs_f64();
        if report.faults.is_empty() && join.is_none() {
            clean += 1;
            println!(
                "epoch-{epoch}: OK  {} blocks  {:.1}s  {:.0} blocks/s",
                report.blocks,
                elapsed,
                report.blocks as f64 / elapsed.max(f64::MIN_POSITIVE)
            );
        } else {
            broken += 1;
            println!(
                "epoch-{epoch}: FAULTS  {} blocks  chain={}  join={}",
                report.blocks,
                report.faults.len(),
                join.clone().unwrap_or_else(|| "ok".to_owned())
            );
            for line in report.faults.iter().take(10) {
                println!("    {line}");
            }
        }
    }
    println!("clean epochs {clean}, epochs with faults {broken}");
    Ok(())
}
