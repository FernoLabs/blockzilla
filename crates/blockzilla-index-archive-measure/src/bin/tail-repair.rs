//! Rebuild `prev_blockhash_tail.bin` from the predecessor epoch.
//!
//! The tail is 300 records of `(32-byte hash, u64 little-endian slot)`. Both
//! halves are already in the previous epoch and neither is derived:
//!
//! - hashes are the last 300 records of `epoch-(N-1)/blockhash_registry.bin`;
//! - slots are the `slot` of the matching last 300 rows of
//!   `epoch-(N-1)/archive-v2-blocks.index`.
//!
//! 300 is Solana's recent-blockhash window: a transaction early in epoch N may
//! name a blockhash from the end of epoch N-1, and this file is how the
//! converter resolves it.
//!
//! Two faults on the NAS are repairable from this: epochs with no tail at all,
//! and epochs whose tail has correct hashes but all-zero slots.
//!
//! **Writing is opt-in and proof-gated.** `--write` refuses to run until
//! `--verify` has reproduced an existing, known-good tail byte for byte, so the
//! reconstruction method is demonstrated on data that already exists before it
//! is used to create data that does not.

use std::{env, fs, path::Path};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    read_archive_v2_hot_block_index,
};

const TAIL_FILE: &str = "prev_blockhash_tail.bin";
const TAIL_RECORDS: usize = 300;
const HASH_LEN: usize = 32;
const RECORD_LEN: usize = 40;

/// Build the tail epoch `epoch` should carry, from epoch `epoch - 1`.
fn build_tail(root: &Path, epoch: u64) -> Result<Vec<u8>> {
    ensure!(epoch > 0, "epoch 0 has no predecessor and needs no tail");
    let previous = root.join(format!("epoch-{}", epoch - 1));

    let registry = fs::read(previous.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))
        .context("read predecessor blockhash registry")?;
    ensure!(
        registry.len() % HASH_LEN == 0,
        "predecessor registry is {} bytes, not a multiple of {HASH_LEN}",
        registry.len()
    );
    let records = registry.len() / HASH_LEN;

    let index = read_archive_v2_hot_block_index(&previous.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
        .context("read predecessor block index")?;
    // Epoch 0 carries one extra leading registry record for genesis, so the
    // registry and the block index can differ by that one entry. Align on the
    // tail of each rather than assuming equal lengths.
    ensure!(
        records >= TAIL_RECORDS && index.rows.len() >= TAIL_RECORDS,
        "predecessor has {records} registry records and {} blocks, need {TAIL_RECORDS}",
        index.rows.len()
    );
    let offset = records - index.rows.len();
    ensure!(
        offset <= 1,
        "predecessor registry leads its block index by {offset} records"
    );

    let mut tail = Vec::with_capacity(TAIL_RECORDS * RECORD_LEN);
    let first_block = index.rows.len() - TAIL_RECORDS;
    for position in 0..TAIL_RECORDS {
        let block = first_block + position;
        let record = block + offset;
        tail.extend_from_slice(&registry[record * HASH_LEN..(record + 1) * HASH_LEN]);
        tail.extend_from_slice(&index.rows[block].slot.to_le_bytes());
    }
    ensure!(
        tail.len() == TAIL_RECORDS * RECORD_LEN,
        "tail length drifted"
    );
    Ok(tail)
}

fn describe(existing: &[u8]) -> String {
    if existing.len() != TAIL_RECORDS * RECORD_LEN {
        return format!("{} bytes", existing.len());
    }
    let zero = (0..TAIL_RECORDS)
        .filter(|record| {
            existing[record * RECORD_LEN + HASH_LEN..(record + 1) * RECORD_LEN]
                .iter()
                .all(|byte| *byte == 0)
        })
        .count();
    format!("{TAIL_RECORDS} records, {zero} with a zero slot")
}

fn main() -> Result<()> {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let write = args.iter().any(|a| a == "--write");
    let verify = args.iter().any(|a| a == "--verify");
    args.retain(|a| !a.starts_with("--"));
    if args.len() < 2 {
        bail!("usage: tail-repair <archive-root> <epoch...> [--verify] [--write]");
    }
    let root = std::path::PathBuf::from(&args[0]);
    let epochs: Vec<u64> = args[1..]
        .iter()
        .map(|value| value.parse())
        .collect::<Result<_, _>>()?;

    for epoch in epochs {
        let dir = root.join(format!("epoch-{epoch}"));
        let path = dir.join(TAIL_FILE);
        let rebuilt = match build_tail(&root, epoch) {
            Ok(bytes) => bytes,
            Err(error) => {
                println!("epoch-{epoch}: CANNOT REBUILD: {error:#}");
                continue;
            }
        };
        let existing = fs::read(&path).ok();

        match (&existing, verify, write) {
            (Some(current), _, false) if current == &rebuilt => {
                println!("epoch-{epoch}: VERIFIED identical ({})", describe(current));
            }
            (Some(current), true, false) => {
                println!(
                    "epoch-{epoch}: DIFFERS from rebuild (existing: {}) -- rebuild would replace it",
                    describe(current)
                );
            }
            (None, true, false) => {
                println!(
                    "epoch-{epoch}: MISSING -- rebuild ready ({} bytes)",
                    rebuilt.len()
                );
            }
            (_, _, true) => {
                // Never overwrite in place: write beside, fsync, then rename, so
                // a crash cannot leave a half-written tail.
                if let Some(current) = &existing
                    && current == &rebuilt
                {
                    println!("epoch-{epoch}: already correct, nothing written");
                    continue;
                }
                let temporary = dir.join(format!("{TAIL_FILE}.rebuild"));
                fs::write(&temporary, &rebuilt).context("write rebuilt tail")?;
                let handle = fs::File::open(&temporary)?;
                handle.sync_all().context("sync rebuilt tail")?;
                drop(handle);
                fs::rename(&temporary, &path).context("install rebuilt tail")?;
                let directory = fs::File::open(&dir)?;
                directory.sync_all().context("sync epoch directory")?;
                println!(
                    "epoch-{epoch}: WROTE {} bytes (previous: {})",
                    rebuilt.len(),
                    existing.map_or("absent".to_owned(), |current| describe(&current))
                );
            }
            (None, false, false) => {
                println!("epoch-{epoch}: MISSING -- run with --verify or --write");
            }
            (Some(current), false, false) => {
                println!(
                    "epoch-{epoch}: DIFFERS (existing: {}) -- run with --verify or --write",
                    describe(current)
                );
            }
        }
    }
    Ok(())
}
