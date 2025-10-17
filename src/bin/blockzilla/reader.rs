use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use reqwest::Client;
use std::{fs, path::{Path, PathBuf}, sync::{Arc, Mutex}};
use tokio::{
    fs as tokio_fs,
    io::{BufReader as TokioBufReader},
};
use tokio_util::io::StreamReader;
use futures::{TryStreamExt};

use crate::{
    downloader::{download_with_parallel_ranges},
    types::{DownloadMode},
    config::HTTP_BUFFER_SIZE,
};

/// Source of the epoch file
pub enum EpochSource {
    Local(PathBuf),
    Stream(String),
}

/// Decide whether to use local file, stream, or cache download.
pub async fn resolve_epoch_source(
    base: &Path,
    epoch: u64,
    mode: DownloadMode,
    pb: ProgressBar,
    client: &Client,
) -> Result<EpochSource> {
    let local_path = base.join(format!("epoch-{epoch}.car"));
    if local_path.exists() {
        return Ok(EpochSource::Local(local_path));
    }

    match mode {
        DownloadMode::NoDownload => Err(anyhow!("epoch-{epoch}.car missing and downloads disabled")),
        DownloadMode::Stream => {
            let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
            pb.set_message(format!("🌐 streaming {}", url));
            Ok(EpochSource::Stream(url))
        }
        DownloadMode::Cache => {
            let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
            pb.set_message(format!("🌐 downloading {}", url));
            download_with_parallel_ranges(&url, &local_path, client, 16).await?;
            pb.set_message(format!("✅ cached epoch-{epoch}.car"));
            Ok(EpochSource::Local(local_path))
        }
    }
}

/// Build an async reader (local, cached, or streamed)
pub async fn build_epoch_reader(
    base: &Path,
    epoch: u64,
    mode: DownloadMode,
    pb: &ProgressBar,
    client: &Client
) -> Result<(Box<dyn tokio::io::AsyncRead + Unpin + Send>, u64)> {
    let epoch_source = resolve_epoch_source(base, epoch, mode, pb.clone(), client).await?;

    match epoch_source {
        EpochSource::Local(path) => {
            let file = tokio_fs::File::open(&path).await?;
            let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let buffered = TokioBufReader::with_capacity(HTTP_BUFFER_SIZE, file);
            Ok((Box::new(buffered), size))
        }
        EpochSource::Stream(url) => {
            let resp = client.get(&url).send().await?;
            if !resp.status().is_success() {
                return Err(anyhow!("HTTP error {} for {}", resp.status(), url));
            }

            let size = resp.content_length().unwrap_or(0);
            let stream = resp.bytes_stream();
            let reader = StreamReader::new(stream.map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("HTTP stream error: {e}"))
            }));
            let buffered = TokioBufReader::with_capacity(HTTP_BUFFER_SIZE, reader);
            Ok((Box::new(buffered), size))
        }
    }
}
