use anyhow::{Context, Result};
use std::{fs, path::Path, time::Instant};
use tracing::{error, info, warn};

use crate::{epoch_paths, file_nonempty, Cli};

pub(crate) fn run(cli: &Cli) -> Result<()> {
    info!(
        "Scanning cache directory for epochs: {}",
        cli.cache_dir.display()
    );

    if !cli.cache_dir.exists() {
        anyhow::bail!("Cache directory not found: {}", cli.cache_dir.display());
    }

    let epochs = discover_epochs(&cli.cache_dir)?;

    if epochs.is_empty() {
        warn!("No epoch files found in {}", cli.cache_dir.display());
        return Ok(());
    }

    info!("Found {} epoch(s) to process: {:?}", epochs.len(), epochs);

    let mut successful = 0;
    let mut failed = 0;
    let batch_start = Instant::now();

    for (idx, epoch) in epochs.iter().enumerate() {
        info!("========================================");
        info!("Processing epoch {} ({}/{})", epoch, idx + 1, epochs.len());
        info!("========================================");

        match process_single_epoch(cli, *epoch) {
            Ok(_) => {
                info!("✓ Successfully processed epoch {}", epoch);
                successful += 1;
            }
            Err(e) => {
                error!("✗ Failed to process epoch {}: {:?}", epoch, e);
                failed += 1;
            }
        }
    }

    let batch_elapsed = batch_start.elapsed().as_secs_f64();

    info!("========================================");
    info!("Batch processing complete!");
    info!("  Successful: {}", successful);
    info!("  Failed:     {}", failed);
    info!("  Total:      {}", successful + failed);
    info!(
        "  Time:       {:.1}s ({:.1}s per epoch avg)",
        batch_elapsed,
        batch_elapsed / epochs.len() as f64
    );
    info!("========================================");

    if failed > 0 {
        anyhow::bail!("{} epoch(s) failed to process", failed);
    }

    Ok(())
}

fn process_single_epoch(cli: &Cli, epoch: u64) -> Result<()> {
    let (_, _, registry_path, bh_path, compact_path) = epoch_paths(cli, epoch);

    if !(cli.resume && file_nonempty(&registry_path)) {
        crate::build_registry::run(cli, epoch)
            .with_context(|| format!("Failed to build registry for epoch {}", epoch))?;
    } else {
        info!(
            "Resume: registry exists, skipping: {}",
            registry_path.display()
        );
    }

    if !(cli.resume && file_nonempty(&bh_path)) {
        crate::build_blockhash_registry::run(cli, epoch)
            .with_context(|| format!("Failed to build blockhash registry for epoch {}", epoch))?;
    } else {
        info!(
            "Resume: blockhash registry exists, skipping: {}",
            bh_path.display()
        );
    }

    if !(cli.resume && file_nonempty(&compact_path)) {
        crate::compact::run(cli, epoch)
            .with_context(|| format!("Failed to build compact for epoch {}", epoch))?;
    } else {
        info!(
            "Resume: compact exists, skipping: {}",
            compact_path.display()
        );
    }

    Ok(())
}

fn discover_epochs(cache_dir: &Path) -> Result<Vec<u64>> {
    let mut epochs = Vec::new();

    let entries = fs::read_dir(cache_dir)
        .with_context(|| format!("Failed to read directory: {}", cache_dir.display()))?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => continue,
        };

        if let Some(epoch) = parse_epoch_filename(filename) {
            epochs.push(epoch);
        }
    }

    epochs.sort_unstable();
    Ok(epochs)
}

fn parse_epoch_filename(filename: &str) -> Option<u64> {
    if !filename.starts_with("epoch-") || !filename.ends_with(".car.zst") {
        return None;
    }

    let number_part = filename.strip_prefix("epoch-")?.strip_suffix(".car.zst")?;
    number_part.parse::<u64>().ok()
}
