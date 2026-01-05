use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};
use tracing::info;

use car_reader::{
    car_stream::CarStream,
    error::GroupError,
    node::{CborCidRef, Node, decode_node},
};

use crate::{BUFFER_SIZE, Cli, ProgressTracker, epoch_paths};

const MAX_BLOCKHASHES_PER_EPOCH: usize = 432_000;

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

    // Final file image in memory: N * 32 bytes.
    let mut out: Vec<u8> = Vec::with_capacity(MAX_BLOCKHASHES_PER_EPOCH * 32);

    let mut progress = ProgressTracker::new("Blockhash Registry");

    let mut stream = CarStream::open_zstd(Path::new(&car_path))?;
    while let Some(group) = stream.next_group()? {
        let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
            Node::Block(bk) => bk,
            _ => return Err(GroupError::WrongRootKind.into()),
        };

        let slot = block.slot;

        // Decode last entry CID from the entries array.
        let mut decoder = minicbor::Decoder::new(block.entries.slice);

        let len_opt = decoder
            .array()
            .map_err(|e| GroupError::Other(format!("decode entries array header: {e}")))?;
        let Some(len_u64) = len_opt else {
            return Err(GroupError::Other(
                "indefinite-length entries array not supported here".to_string(),
            )
            .into());
        };

        let len = len_u64 as usize;
        if len == 0 {
            return Err(GroupError::Other("entries array is empty".to_string()).into());
        }

        for _ in 0..(len - 1) {
            decoder
                .skip()
                .map_err(|e| GroupError::Other(format!("skip entry cid: {e}")))?;
        }

        let last_entry_cid: CborCidRef = decoder
            .decode()
            .map_err(|e| GroupError::Other(format!("decode last entry cid: {e}")))?;

        let Node::Entry(entry) = group.decode_by_hash(last_entry_cid.hash_bytes())? else {
            return Err(GroupError::Other("expected entry node".to_string()).into());
        };

        if entry.hash.len() != 32 {
            return Err(GroupError::Other("entry.hash len != 32".to_string()).into());
        }

        out.extend_from_slice(entry.hash);

        progress.update_slot(slot);
        progress.update(1, 0);
    }

    let n = out.len() / 32;

    // Direct write (no tmp + rename)
    let mut f = File::create(&bh_path).with_context(|| format!("create {}", bh_path.display()))?;
    //let mut w = BufWriter::with_capacity(BUFFER_SIZE, f);
    f.write_all(&out)
        .with_context(|| "write blockhash registry")?;
    f.flush().context("flush blockhash registry")?;

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

    build_blockhash_registry_for_epoch(cli, epoch)
}
