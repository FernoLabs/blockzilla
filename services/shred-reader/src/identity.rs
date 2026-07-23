use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use anyhow::{Context, Result, bail};
use solana_keypair::{Keypair, read_keypair_file, write_keypair};

pub fn load_or_create(path: &Path) -> Result<Keypair> {
    if path.exists() {
        return read_keypair_file(path)
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .with_context(|| format!("failed to read identity at {}", path.display()));
    }

    let parent = path
        .parent()
        .with_context(|| format!("identity path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create identity directory {}", parent.display()))?;

    let keypair = Keypair::new();
    let temporary_path = path.with_extension("json.tmp");
    if temporary_path.exists() {
        fs::remove_file(&temporary_path).with_context(|| {
            format!(
                "failed to remove stale temporary identity {}",
                temporary_path.display()
            )
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary_path)
        .with_context(|| {
            format!(
                "failed to create temporary identity {}",
                temporary_path.display()
            )
        })?;
    write_keypair(&keypair, &mut file).map_err(|error| anyhow::anyhow!(error.to_string()))?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temporary_path, path).with_context(|| {
        format!(
            "failed to persist identity from {} to {}",
            temporary_path.display(),
            path.display()
        )
    })?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;

    if !path.exists() {
        bail!("identity was not persisted at {}", path.display());
    }
    Ok(keypair)
}

#[cfg(test)]
mod tests {
    use solana_keypair::Signer;

    use super::*;

    #[test]
    fn created_identity_is_stable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identity.json");

        let first = load_or_create(&path).unwrap();
        let second = load_or_create(&path).unwrap();

        assert_eq!(first.pubkey(), second.pubkey());
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn malformed_identity_is_not_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identity.json");
        fs::write(&path, b"not a keypair").unwrap();

        assert!(load_or_create(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"not a keypair");
    }
}
