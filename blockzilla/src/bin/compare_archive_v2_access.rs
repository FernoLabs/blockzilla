use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use anyhow::{Context, Result, ensure};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessIndexRow,
    KeyStore, read_archive_v2_block_access_index, wincode_leb128_config,
};
use clap::Parser;

/// Compare two access sidecars while ignoring epoch-local pubkey ID numbers.
#[derive(Debug, Parser)]
struct Args {
    left: PathBuf,
    right: PathBuf,
    #[arg(long)]
    max_blocks: Option<usize>,
    /// Permit left to contain additional pubkeys (for all-reference registries).
    #[arg(long)]
    allow_left_pubkey_superset: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let left_index =
        read_archive_v2_block_access_index(&args.left.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE))?;
    let right_index =
        read_archive_v2_block_access_index(&args.right.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE))?;
    let limit = comparison_limit(
        left_index.rows.len(),
        right_index.rows.len(),
        args.max_blocks,
    )?;

    let mut left_file = File::open(args.left.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;
    let mut right_file = File::open(args.right.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;
    let left_registry = KeyStore::load(&args.left.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let right_registry = KeyStore::load(&args.right.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let mut left_bytes = Vec::new();
    let mut right_bytes = Vec::new();
    let mut pubkey_rows = 0u64;
    let mut extra_pubkeys = 0u64;

    for block_index in 0..limit {
        let left_row = left_index.rows[block_index];
        let right_row = right_index.rows[block_index];
        compare_index_row(block_index, left_row, right_row)?;
        read_blob(&mut left_file, left_row, &mut left_bytes)?;
        read_blob(&mut right_file, right_row, &mut right_bytes)?;
        let left: ArchiveV2BlockAccessBlob =
            wincode::config::deserialize(&left_bytes, wincode_leb128_config())
                .with_context(|| format!("decode left access block {block_index}"))?;
        let right: ArchiveV2BlockAccessBlob =
            wincode::config::deserialize(&right_bytes, wincode_leb128_config())
                .with_context(|| format!("decode right access block {block_index}"))?;
        validate_pubkey_ids(block_index, "left", &left, &left_registry)?;
        validate_pubkey_ids(block_index, "right", &right, &right_registry)?;
        extra_pubkeys +=
            compare_blob(block_index, &left, &right, args.allow_left_pubkey_superset)? as u64;
        pubkey_rows += left.pubkeys.len() as u64;
    }

    println!(
        "access_semantic_equal blocks={} pubkey_rows={} extra_left_pubkeys={} left={} right={}",
        limit,
        pubkey_rows,
        extra_pubkeys,
        args.left.display(),
        args.right.display()
    );
    Ok(())
}

fn validate_pubkey_ids(
    block_index: usize,
    side: &str,
    blob: &ArchiveV2BlockAccessBlob,
    registry: &KeyStore,
) -> Result<()> {
    let mut ids = Vec::with_capacity(blob.pubkeys.len());
    for entry in &blob.pubkeys {
        let expected = registry.get(entry.id).with_context(|| {
            format!(
                "{side} access block {block_index} references invalid pubkey id {} (registry keys={})",
                entry.id,
                registry.len()
            )
        })?;
        ensure!(
            *expected == entry.pubkey,
            "{side} access block {block_index} pubkey id {} resolves to the wrong registry key",
            entry.id
        );
        ids.push(entry.id);
    }
    ids.sort_unstable();
    ensure!(
        ids.windows(2).all(|pair| pair[0] != pair[1]),
        "{side} access block {block_index} contains duplicate pubkey ids"
    );
    Ok(())
}

fn comparison_limit(
    left_rows: usize,
    right_rows: usize,
    max_blocks: Option<usize>,
) -> Result<usize> {
    if let Some(limit) = max_blocks {
        ensure!(limit > 0, "--max-blocks must be greater than zero");
        ensure!(
            left_rows >= limit && right_rows >= limit,
            "requested {limit} blocks but indexes contain left={left_rows} right={right_rows}"
        );
        return Ok(limit);
    }

    ensure!(
        left_rows == right_rows,
        "access index row-count mismatch: left={left_rows} right={right_rows}; use a positive --max-blocks for an explicit prefix comparison"
    );
    Ok(left_rows)
}

fn compare_index_row(
    index: usize,
    left: ArchiveV2BlockAccessIndexRow,
    right: ArchiveV2BlockAccessIndexRow,
) -> Result<()> {
    ensure!(
        (
            left.block_id,
            left.slot,
            left.tx_count,
            left.signature_count
        ) == (
            right.block_id,
            right.slot,
            right.tx_count,
            right.signature_count
        ),
        "access index semantic mismatch at row {index}"
    );
    Ok(())
}

fn read_blob(file: &mut File, row: ArchiveV2BlockAccessIndexRow, out: &mut Vec<u8>) -> Result<()> {
    file.seek(SeekFrom::Start(row.access_offset))?;
    out.resize(row.access_len as usize, 0);
    file.read_exact(out)?;
    Ok(())
}

fn compare_blob(
    block_index: usize,
    left: &ArchiveV2BlockAccessBlob,
    right: &ArchiveV2BlockAccessBlob,
    allow_left_pubkey_superset: bool,
) -> Result<usize> {
    ensure!(
        left.version == right.version,
        "version mismatch at block {block_index}"
    );
    ensure!(
        left.flags == right.flags,
        "flags mismatch at block {block_index}"
    );
    ensure!(
        left.blockhash == right.blockhash,
        "blockhash mismatch at block {block_index}"
    );
    ensure!(
        left.previous_blockhash == right.previous_blockhash,
        "previous blockhash mismatch at block {block_index}"
    );
    ensure!(
        left.signature_counts == right.signature_counts,
        "signature counts mismatch at block {block_index}"
    );
    ensure!(
        left.signatures == right.signatures,
        "signatures mismatch at block {block_index}"
    );

    let mut left_pubkeys = left
        .pubkeys
        .iter()
        .map(|entry| entry.pubkey)
        .collect::<Vec<_>>();
    let mut right_pubkeys = right
        .pubkeys
        .iter()
        .map(|entry| entry.pubkey)
        .collect::<Vec<_>>();
    left_pubkeys.sort_unstable();
    right_pubkeys.sort_unstable();
    ensure!(
        left_pubkeys.windows(2).all(|pair| pair[0] != pair[1]),
        "left access contains duplicate resolved pubkeys at block {block_index}"
    );
    ensure!(
        right_pubkeys.windows(2).all(|pair| pair[0] != pair[1]),
        "right access contains duplicate resolved pubkeys at block {block_index}"
    );
    let extra_pubkeys = if allow_left_pubkey_superset {
        ensure!(
            right_pubkeys
                .iter()
                .all(|pubkey| left_pubkeys.binary_search(pubkey).is_ok()),
            "left is missing a resolved pubkey at block {block_index}"
        );
        left_pubkeys.len().saturating_sub(right_pubkeys.len())
    } else {
        ensure!(
            left_pubkeys == right_pubkeys,
            "resolved pubkey set mismatch at block {block_index}"
        );
        0
    };

    ensure!(
        left.blockhashes.len() == right.blockhashes.len(),
        "blockhash reference length mismatch at block {block_index}"
    );
    for (left, right) in left.blockhashes.iter().zip(&right.blockhashes) {
        ensure!(
            left.id == right.id && left.blockhash == right.blockhash,
            "blockhash reference mismatch at block {block_index}"
        );
    }
    ensure!(
        left.vote_hashes.len() == right.vote_hashes.len(),
        "vote-hash reference length mismatch at block {block_index}"
    );
    for (left, right) in left.vote_hashes.iter().zip(&right.vote_hashes) {
        ensure!(
            left.block_id == right.block_id
                && left.bank_hash == right.bank_hash
                && left.block_id_hash == right.block_id_hash,
            "vote-hash reference mismatch at block {block_index}"
        );
    }
    Ok(extra_pubkeys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_comparison_requires_equal_row_counts() {
        assert_eq!(comparison_limit(7, 7, None).unwrap(), 7);
        assert!(comparison_limit(7, 6, None).is_err());
    }

    #[test]
    fn prefix_comparison_requires_a_positive_available_limit() {
        assert!(comparison_limit(7, 7, Some(0)).is_err());
        assert_eq!(comparison_limit(7, 9, Some(5)).unwrap(), 5);
        assert!(comparison_limit(7, 9, Some(8)).is_err());
    }
}
