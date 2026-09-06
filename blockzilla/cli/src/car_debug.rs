use crate::archive_verify::{EntryJob, recompute_entry_hash_standalone};
use anyhow::{Context, Result};
use of_car_reader::CarStream;
use std::path::Path;

/// Independently decode one PoH entry's num_hashes/hash/transactions straight from a CAR,
/// bypassing the Compact V2 compactor entirely. Used to cross-check compact archive output
/// against ground truth when `verify-archive-v2-poh` reports a mismatch.
pub(crate) fn dump_car_poh_entry(
    input: &Path,
    slot: u64,
    entry_index: Option<usize>,
) -> Result<()> {
    let is_zstd = input
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"));

    let mut plain_stream = if is_zstd {
        None
    } else {
        Some(CarStream::open(input).with_context(|| format!("open {}", input.display()))?)
    };
    let mut zstd_stream = if is_zstd {
        Some(CarStream::open_zstd(input).with_context(|| format!("open {}", input.display()))?)
    } else {
        None
    };

    loop {
        let group = if let Some(stream) = plain_stream.as_mut() {
            stream.next_group()?
        } else {
            zstd_stream.as_mut().unwrap().next_group()?
        };
        let Some(group) = group else {
            anyhow::bail!("reached end of CAR without finding slot {slot}");
        };
        if group.slot != Some(slot) {
            continue;
        }

        println!(
            "slot={slot} entries={} poh_num_hashes_len={} poh_hashes_len={} \
             blockhash={} has_blockhash={}",
            group.entry_count(),
            group.poh_num_hashes.len(),
            group.poh_hashes.len(),
            hex32(&group.blockhash),
            group.has_blockhash,
        );

        for (index, (&num_hashes, tx_count)) in group
            .poh_num_hashes
            .iter()
            .zip(group.entry_tx_counts.iter().copied())
            .enumerate()
        {
            println!(
                "  entry[{index}] num_hashes={num_hashes} tx_count={tx_count} hash={}",
                hex32(&group.poh_hashes[index])
            );
        }

        let Some(target_entry) = entry_index else {
            return Ok(());
        };
        anyhow::ensure!(
            target_entry < group.entry_tx_counts.len(),
            "entry {target_entry} is out of range; block has {} entries",
            group.entry_tx_counts.len()
        );

        let tx_start: usize = group.entry_tx_counts[..target_entry]
            .iter()
            .map(|&count| count as usize)
            .sum();
        let tx_end = tx_start + group.entry_tx_counts[target_entry] as usize;

        let expected_hash = group.poh_hashes[target_entry];
        let num_hashes = group.poh_num_hashes[target_entry];
        let tx_count = group.entry_tx_counts[target_entry];
        let start_hash = (target_entry > 0).then(|| group.poh_hashes[target_entry - 1]);

        println!("--- entry[{target_entry}] transactions [{tx_start}..{tx_end}) ---");
        let mut txs = group.transactions_no_meta();
        let mut position = 0usize;
        let mut flat_signatures = Vec::<u8>::new();
        while let Some(tx) = txs.next_tx().map_err(|err| anyhow::anyhow!("{err:?}"))? {
            if position >= tx_end {
                break;
            }
            if position >= tx_start {
                println!(
                    "  tx[{position}] signatures={}",
                    tx.signatures
                        .iter()
                        .map(|sig| hex64(sig))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                for signature in &tx.signatures {
                    flat_signatures.extend_from_slice(signature.as_slice());
                }
            }
            position += 1;
        }
        anyhow::ensure!(
            position >= tx_end,
            "block only had {position} transactions, expected at least {tx_end}"
        );

        if tx_count == 0 {
            println!("entry[{target_entry}] is a tick (tx_count=0); skipping recompute");
            return Ok(());
        }
        let Some(start_hash) = start_hash else {
            println!(
                "entry[0] needs the previous block's final hash, which this tool does not fetch; \
                 pass --entry on a nonzero index to recompute, or supply --slot for the prior block"
            );
            return Ok(());
        };

        let job = EntryJob {
            start_hash,
            num_hashes,
            transaction_count: tx_count,
            signatures: &flat_signatures,
        };
        let recomputed = recompute_entry_hash_standalone(&job);
        println!("flat_signatures_hex={}", hex_bytes(&flat_signatures));
        println!(
            "recompute-from-CAR: start_hash={} num_hashes={num_hashes} tx_count={tx_count} \
             signature_bytes={} recomputed={} expected(CAR)={} match={}",
            hex32(&start_hash),
            flat_signatures.len(),
            hex32(&recomputed),
            hex32(&expected_hash),
            recomputed == expected_hash,
        );
        return Ok(());
    }
}

fn hex32(value: &[u8; 32]) -> String {
    hex_bytes(value)
}

fn hex64(value: &[u8; 64]) -> String {
    hex_bytes(value)
}

fn hex_bytes(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
