use anyhow::{Result, anyhow};
use async_compression::tokio::bufread::ZstdDecoder;
use std::path::Path;
use tokio::{
    fs,
    io::{AsyncRead, BufReader},
};

/// Opens a locally available CAR file for the given epoch.
///
/// The caller is responsible for ensuring the files are present on disk.
/// If both an uncompressed and a `.zst`-compressed variant exist, the
/// uncompressed file takes precedence.
pub async fn open_local_epoch(
    epoch: u64,
    cache_dir: impl AsRef<Path>,
) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    fs::create_dir_all(&cache_dir).await?;
    let file = format!("epoch-{epoch}.car");
    let path = cache_dir.as_ref().join(&file);

    if path.exists() {
        return Ok(Box::new(tokio::fs::File::open(&path).await?));
    }

    let zstd_path = cache_dir.as_ref().join(format!("{file}.zst"));
    if zstd_path.exists() {
        let file = tokio::fs::File::open(&zstd_path).await?;
        let reader = BufReader::new(file);
        let decoder = ZstdDecoder::new(reader);
        return Ok(Box::new(decoder));
    }

    Err(anyhow!(
        "CAR file not found (looked for {} and {})",
        path.display(),
        zstd_path.display()
    ))
}
