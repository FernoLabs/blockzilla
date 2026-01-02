use anyhow::Result;
use tracing::info;

use crate::{
    build_blockhash_registry, build_registry, compact, epoch_paths, file_nonempty, Cli,
};

pub(crate) fn run(cli: &Cli, epoch: u64) -> Result<()> {
    let (_, _, registry_path, bh_path, compact_path) = epoch_paths(cli, epoch);

    if cli.resume && file_nonempty(&registry_path) {
        info!(
            "Resume: registry exists, skipping phase 1: {}",
            registry_path.display()
        );
    } else {
        build_registry::run(cli, epoch)?;
    }

    if cli.resume && file_nonempty(&bh_path) {
        info!(
            "Resume: blockhash registry exists, skipping: {}",
            bh_path.display()
        );
    } else {
        build_blockhash_registry::run(cli, epoch)?;
    }

    if cli.resume && file_nonempty(&compact_path) {
        info!(
            "Resume: compact exists, skipping phase 2: {}",
            compact_path.display()
        );
    } else {
        compact::run(cli, epoch)?;
    }

    Ok(())
}
