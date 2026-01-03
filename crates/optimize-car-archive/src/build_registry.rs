use anyhow::{Context, Result};
use car_reader::{car_stream::CarStream, versioned_transaction::VersionedMessage};
use rustc_hash::FxBuildHasher;
use solana_pubkey::Pubkey;
use std::{path::Path, str::FromStr, time::Instant};
use tracing::info;

use rustc_hash::FxHashMap;

use car_reader::{
    car_block_group::CarBlockGroup,
    error::GroupError,
    node::{Node, decode_node},
};

use blockzilla_format::write_registry;

use crate::{Cli, ProgressTracker, epoch_paths};

pub(crate) fn run(cli: &Cli, epoch: u64) -> Result<()> {
    let (car_path, epoch_dir, registry_path, _, _) = epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }
    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building registry (counting phase) epoch={}", epoch);
    info!("  car:      {}", car_path.display());
    info!("  out:      {}", registry_path.display());

    let mut counter = PubkeyCounter::new(16_000_000);
    let mut progress = ProgressTracker::new("Phase 1/2");

    let mut stream = CarStream::open_zstd(Path::new(&car_path))?;
    while let Some(group) = stream.next_group()? {
        let (blocks_delta, txs_delta, slot) = registry_process_block(group, &mut counter)?;
        if let Some(s) = slot {
            progress.update_slot(s);
        }
        progress.update(blocks_delta, txs_delta);
    }

    progress.final_report();
    info!("Unique pubkeys: {}", counter.counts.len());

    info!("Sorting registry by usage frequency...");
    let sort_start = Instant::now();

    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(ka, ca), (kb, cb)| cb.cmp(ca).then_with(|| ka.cmp(kb)));

    let keys: Vec<[u8; 32]> = items.into_iter().map(|(k, _)| k).collect();

    info!(
        "Sorting completed in {:.2}s",
        sort_start.elapsed().as_secs_f64()
    );

    write_registry(&registry_path, &keys)?;
    info!("Registry written: {} keys", keys.len());

    Ok(())
}

struct PubkeyCounter {
    counts: FxHashMap<[u8; 32], u32>,
}

impl PubkeyCounter {
    fn new(cap: usize) -> Self {
        let counts = FxHashMap::with_capacity_and_hasher(cap, FxBuildHasher);
        Self { counts }
    }

    #[inline(always)]
    fn add32(&mut self, k32: &[u8; 32]) {
        *self.counts.entry(*k32).or_insert(0) += 1;
    }
}
fn registry_process_block(
    group: &CarBlockGroup,
    counter: &mut PubkeyCounter,
) -> Result<(u64, u64, Option<u64>), GroupError> {
    // Keep this if you need the slot for progress reporting.
    let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
        Node::Block(b) => b,
        _ => return Err(GroupError::WrongRootKind),
    };
    let block_slot = block.slot;

    let mut it = group.transactions().unwrap();

    let mut txs = 0u64;

    while let Some(r) = it.next_tx().unwrap() {
        let (vtx, maybe_meta) = r;
        txs += 1;

        match &vtx.message {
            VersionedMessage::Legacy(m) => {
                for k in &m.account_keys {
                    counter.add32(k);
                }
            }
            VersionedMessage::V0(m) => {
                for k in &m.account_keys {
                    counter.add32(k);
                }
                for l in &m.address_table_lookups {
                    counter.add32(l.account_key);
                }
            }
        }

        if let Some(meta) = maybe_meta {
            // loaded addresses
            for pk in &meta.loaded_writable_addresses {
                let key = pk.as_slice().try_into().unwrap();
                counter.add32(key);
            }
            for pk in &meta.loaded_readonly_addresses {
                let key = pk.as_slice().try_into().unwrap();
                counter.add32(key);
            }

            // token balances (string fields)
            for tb in meta
                .pre_token_balances
                .iter()
                .chain(meta.post_token_balances.iter())
            {
                if let Ok(pk) = Pubkey::from_str(&tb.mint) {
                    counter.add32(pk.as_array());
                }
                if !tb.owner.is_empty()
                    && let Ok(pk) = Pubkey::from_str(&tb.owner)
                {
                    counter.add32(pk.as_array());
                }
                if !tb.program_id.is_empty()
                    && let Ok(pk) = Pubkey::from_str(&tb.program_id)
                {
                    counter.add32(pk.as_array());
                }
            }
        }
    }

    Ok((1, txs, Some(block_slot)))
}
