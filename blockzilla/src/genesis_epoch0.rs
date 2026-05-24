use anyhow::{Context, Result, anyhow};
use of_car_reader::genesis::{GenesisArchive, MAINNET_GENESIS_URL, read_genesis_archive_from_file};
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tracing::info;

pub(crate) const GENESIS_FILE: &str = "genesis.tar.bz2";

pub(crate) fn maybe_load_for_input(input: &Path) -> Result<Option<GenesisArchive>> {
    if epoch_from_path(input) != Some(0) {
        return Ok(None);
    }
    let path = ensure_for_input(input)?;
    read_genesis_archive_from_file(&path)
        .map(Some)
        .map_err(|err| anyhow!("{err}"))
        .with_context(|| format!("read {}", path.display()))
}

pub(crate) fn ensure_for_input(input: &Path) -> Result<PathBuf> {
    let dir = input.parent().unwrap_or_else(|| Path::new("."));
    ensure_in_dir(dir)
}

pub(crate) fn blockhash_registry_starts_with(path: &Path, hash: &[u8; 32]) -> Result<bool> {
    let Ok(mut file) = File::open(path) else {
        return Ok(false);
    };
    let mut first = [0u8; 32];
    match file.read_exact(&mut first) {
        Ok(()) => Ok(&first == hash),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

pub(crate) fn add_pubkeys_to(archive: &GenesisArchive, mut add: impl FnMut(&[u8; 32])) {
    for entry in archive
        .genesis
        .accounts
        .iter()
        .chain(archive.genesis.reward_pools.iter())
    {
        add(&entry.pubkey);
        add(&entry.account.owner);
    }

    for builtin in &archive.genesis.builtins {
        add(&builtin.pubkey);
    }
}

fn ensure_in_dir(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(GENESIS_FILE);
    if path.is_file() && path.metadata().is_ok_and(|meta| meta.len() > 0) {
        return Ok(path);
    }

    let tmp = dir.join(format!("{GENESIS_FILE}.tmp"));
    info!(
        "genesis archive missing; downloading {} to {}",
        MAINNET_GENESIS_URL,
        path.display()
    );

    let mut response = reqwest::blocking::get(MAINNET_GENESIS_URL)
        .with_context(|| format!("download {MAINNET_GENESIS_URL}"))?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("download {MAINNET_GENESIS_URL}: HTTP {status}");
    }

    {
        let mut file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        std::io::copy(&mut response, &mut file)
            .with_context(|| format!("write {}", tmp.display()))?;
        file.flush()
            .with_context(|| format!("flush {}", tmp.display()))?;
    }

    std::fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} to {}", tmp.display(), path.display()))?;
    Ok(path)
}

fn epoch_from_path(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let rest = file_name.strip_prefix("epoch-")?;
    let number = rest
        .strip_suffix(".car.zst")
        .or_else(|| rest.strip_suffix(".car"))?;
    number.parse().ok()
}
