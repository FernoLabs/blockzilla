use anyhow::{Context, Result};
use blockzilla_format::write_registry;
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::{
    car_block_group::CarBlockGroup, car_stream::CarStream, error::GroupError,
    versioned_transaction::VersionedMessage,
};
use solana_pubkey::{Pubkey, pubkey};
use std::{fs::File, io::Write, path::Path, str::FromStr, time::Instant};
use tracing::info;

use crate::{Cli, ProgressTracker, epoch_paths, genesis_epoch0};

const MAX_BLOCKHASHES_PER_EPOCH: usize = 432_000;

pub(crate) fn run(cli: &Cli, epoch: u64) -> Result<()> {
    // If prev epoch blockhash registry is missing, we MUST build it (transactions may reference it).
    if epoch > 0 {
        let (prev_car_path, _prev_dir, _prev_reg, prev_bh_path, _prev_compact) =
            epoch_paths(cli, epoch - 1);

        if !prev_bh_path.exists() {
            info!(
                "Prev epoch blockhash registry missing, building it now: epoch={} out={}",
                epoch - 1,
                prev_bh_path.display()
            );

            if !prev_car_path.exists() {
                anyhow::bail!(
                    "Prev epoch CAR not found, cannot build prev blockhash registry: epoch={} car={}",
                    epoch - 1,
                    prev_car_path.display()
                );
            }

            build_registry_and_blockhash_for_epoch(cli, epoch - 1).with_context(|| {
                format!(
                    "build registry + blockhash registry for epoch {}",
                    epoch - 1
                )
            })?;
        }
    }

    build_registry_and_blockhash_for_epoch(cli, epoch)
}

fn build_registry_and_blockhash_for_epoch(cli: &Cli, epoch: u64) -> Result<()> {
    let (car_path, epoch_dir, registry_path, bh_path, _compact_path) = epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }

    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building registry + blockhash registry epoch={}", epoch);
    info!("  car:      {}", car_path.display());
    info!("  registry: {}", registry_path.display());
    info!("  bh:       {}", bh_path.display());

    // Blockhash file image in memory: N * 32 bytes.
    let mut blockhash_out: Vec<u8> = Vec::with_capacity(MAX_BLOCKHASHES_PER_EPOCH * 32);

    let mut counter = PubkeyCounter::new(50_000_000);

    let mut progress = ProgressTracker::new("Registry + Blockhash");
    let genesis = genesis_epoch0::maybe_load_for_epoch(&cli.cache_dir, epoch)?;
    if let Some(genesis) = &genesis {
        blockhash_out.extend_from_slice(&genesis.genesis_hash);
        genesis_epoch0::add_pubkeys_to(genesis, |key| counter.add32(key));
        info!(
            "Seeded epoch-0 genesis: accounts={} reward_pools={} builtins={} genesis_hash={}",
            genesis.genesis.accounts.len(),
            genesis.genesis.reward_pools.len(),
            genesis.genesis.builtins.len(),
            hex32(&genesis.genesis_hash)
        );
    }

    let mut stream = CarStream::open_zstd(Path::new(&car_path))?;
    while let Some(group) = stream.next_group()? {
        let (blocks_delta, txs_delta, slot, blockhash32) =
            process_group_for_registry_and_blockhash(group, &mut counter)?;

        if let Some(s) = slot {
            progress.update_slot(s);
        }
        progress.update(blocks_delta, txs_delta);

        if let Some(bh) = blockhash32 {
            blockhash_out.extend_from_slice(&bh);
        }
    }

    progress.final_report();

    // Write blockhash registry first (fast)
    {
        let n = blockhash_out.len() / 32;
        let mut f =
            File::create(&bh_path).with_context(|| format!("create {}", bh_path.display()))?;
        f.write_all(&blockhash_out)
            .with_context(|| "write blockhash registry")?;
        f.flush().context("flush blockhash registry")?;
        info!("Blockhash registry written: {} hashes", n);
    }

    info!("Unique pubkeys: {}", counter.counts.len());

    info!("Sorting registry by usage frequency...");
    let sort_start = Instant::now();

    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(ka, ca), (kb, cb)| cb.cmp(ca).then_with(|| ka.cmp(kb)));

    let mut keys: Vec<[u8; 32]> = items.into_iter().map(|(k, _)| k).collect();

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] =
        &[pubkey!("ComputeBudget111111111111111111111111111111")];

    for b in BUILTIN_PROGRAM_KEYS {
        let b = b.to_bytes();
        if !keys.iter().any(|k| k == &b) {
            keys.insert(0, b);
        }
    }

    info!(
        "Sorting completed in {:.2}s",
        sort_start.elapsed().as_secs_f64()
    );

    write_registry(&registry_path, &keys)?;
    info!("Registry written: {} keys", keys.len());

    Ok(())
}

struct PubkeyCounter {
    counts: GxHashMap<[u8; 32], u32>,
}

impl PubkeyCounter {
    fn new(cap: usize) -> Self {
        let counts = GxHashMap::with_capacity_and_hasher(cap, GxBuildHasher::default());
        Self { counts }
    }

    #[inline(always)]
    fn add32(&mut self, k32: &[u8; 32]) {
        *self.counts.entry(*k32).or_insert(0) += 1;
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

type RegistryGroupStats = (u64, u64, Option<u64>, Option<[u8; 32]>);

fn process_group_for_registry_and_blockhash(
    group: &mut CarBlockGroup,
    counter: &mut PubkeyCounter,
) -> Result<RegistryGroupStats, GroupError> {
    let block_slot = group.slot.unwrap();
    let bh = group.blockhash;

    let mut it = group.transactions();
    let mut txs = 0u64;

    while let Some(r) = it.next_tx()? {
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
            for pk in &meta.loaded_writable_addresses {
                let key: &[u8; 32] = pk.as_slice().try_into().unwrap();
                counter.add32(key);
            }
            for pk in &meta.loaded_readonly_addresses {
                let key: &[u8; 32] = pk.as_slice().try_into().unwrap();
                counter.add32(key);
            }

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

    Ok((1, txs, Some(block_slot), Some(bh)))
}
