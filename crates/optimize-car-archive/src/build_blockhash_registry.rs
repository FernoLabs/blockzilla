use anyhow::{Context, Result};
use ahash::AHashMap;
use solana_message::Hash;
use std::{
    fs::File,
    io::{BufWriter, Write},
};
use tracing::{info, warn};

use car_reader::{
    error::GroupError,
    node::{decode_node, Node},
};

use crate::{epoch_paths, stream_car_blocks, Cli, ProgressTracker, BUFFER_SIZE};

/// Plain writer: writes raw 32-byte hashes back-to-back.
/// ID is implicit: position in file (0-based).
struct BlockhashRegistryWriter {
    w: BufWriter<File>,
    n: u32,
}

impl BlockhashRegistryWriter {
    fn create(path: &std::path::Path) -> Result<Self> {
        let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self {
            w: BufWriter::with_capacity(BUFFER_SIZE, f),
            n: 0,
        })
    }

    #[inline(always)]
    fn push_raw(&mut self, h: &[u8; 32]) -> Result<u32> {
        self.w.write_all(h).with_context(|| "write blockhash")?;
        let id = self.n;
        self.n += 1;
        Ok(id)
    }

    fn finish(mut self) -> Result<u32> {
        self.w.flush().context("flush blockhash registry")?;
        Ok(self.n)
    }
}

/// Inserts hash into registry if missing, returning its id.
#[inline(always)]
fn get_or_insert_hash(
    map: &mut AHashMap<[u8; 32], u32>,
    w: &mut BlockhashRegistryWriter,
    h: [u8; 32],
) -> Result<u32> {
    if let Some(&id) = map.get(&h) {
        return Ok(id);
    }
    let id = w.push_raw(&h)?;
    map.insert(h, id);
    Ok(id)
}

fn build_blockhash_registry_for_epoch(cli: &Cli, epoch: u64) -> Result<()> {
    let (car_path, epoch_dir, _registry_path, bh_path, _compact_path) = epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }

    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building blockhash registry epoch={}", epoch);
    info!("  car: {}", car_path.display());
    info!("  out: {}", bh_path.display());

    // Atomic write: tmp then rename
    let tmp_path = bh_path.with_extension("bin.tmp");
    let mut w = BlockhashRegistryWriter::create(&tmp_path)?;

    // hash -> id (dedup within this epoch file)
    let mut ids: AHashMap<[u8; 32], u32> = AHashMap::with_capacity(4_000_000);

    let mut progress = ProgressTracker::new("Blockhash Registry");

    stream_car_blocks(&car_path, |group| {
        let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
            Node::Block(bk) => bk,
            _ => return Err(GroupError::WrongRootKind.into()),
        };

        let slot = block.slot;

        // Match firehose/RPC semantics:
        // "blockhash" is the hash of the last Entry in the slot.
        let mut last_entry_hash: Option<[u8; 32]> = None;

        let mut entry_iter = block
            .entries
            .iter_stateful()
            .map_err(|e| GroupError::Node(car_reader::node::NodeDecodeError::from(e)))?;

        while let Some(entry_cid) = entry_iter.next_item() {
            let entry_cid = entry_cid.map_err(|e| GroupError::Node(e.into()))?;
            let Node::Entry(entry) = group.decode_by_hash(entry_cid.hash_bytes())? else {
                continue;
            };

            let h = entry.hash;
            if h.len() != 32 {
                warn!("entry.hash len != 32: slot={} len={}", slot, h.len());
                continue;
            }

            let mut bh = [0u8; 32];
            bh.copy_from_slice(h);
            last_entry_hash = Some(bh);
        }

        if let Some(bh) = last_entry_hash {
            let id = get_or_insert_hash(&mut ids, &mut w, bh).map_err(|_| GroupError::Io)?;
        } else {
            warn!("no entry hash found for slot={}", slot);
        }

        progress.update_slot(slot);
        progress.update(1, 0);
        Ok(())
    })?;

    let n = w.finish()?;
    std::fs::rename(&tmp_path, &bh_path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), bh_path.display()))?;

    progress.final_report();
    info!("Blockhash registry written: {} hashes", n);

    Ok(())
}

pub(crate) fn run(cli: &Cli, epoch: u64) -> Result<()> {
    // If prev epoch registry is missing, we MUST build it (transactions may reference it).
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

            build_blockhash_registry_for_epoch(cli, epoch - 1)
                .with_context(|| format!("build blockhash registry for epoch {}", epoch - 1))?;
        }
    }

    // Always build current epoch registry.
    build_blockhash_registry_for_epoch(cli, epoch)
}
