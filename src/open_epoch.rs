use anyhow::{Result, anyhow};
use clap::ValueEnum;
use futures::TryStreamExt;
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;

/// Fetching policy for epoch files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FetchMode {
    /// Always stream from the network (ignore cache).
    Network,
    /// Use cache if present; otherwise download and store it.
    Cache,
    /// Use only local cache (offline); fail if not found.
    Offline,
}

/// Open an epoch CAR file according to the chosen fetch mode.
///
/// Remote path: `https://files.old-faithful.net/{epoch}/epoch-{epoch}.car`
/// Local path:  `{cache_dir}/epoch-{epoch}.car`
pub async fn open_epoch(
    epoch: u64,
    cache_dir: impl AsRef<Path>,
    mode: FetchMode,
) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let cache_dir = cache_dir.as_ref();
    fs::create_dir_all(cache_dir).await?;

    let filename = format!("epoch-{epoch}.car");
    let cached_path = cache_dir.join(&filename);
    let url = format!("https://files.old-faithful.net/{epoch}/{}", filename);

    match mode {
        FetchMode::Offline => {
            println!("📦 Offline mode → {}", cached_path.display());
            if !cached_path.exists() {
                return Err(anyhow!(
                    "Cache miss in offline mode: {}",
                    cached_path.display()
                ));
            }
            let file = File::open(&cached_path).await?;
            Ok(Box::new(file))
        }

        FetchMode::Network => {
            println!("🌐 Network mode → fetching {url}");
            let response = reqwest::get(&url).await?;
            if !response.status().is_success() {
                return Err(anyhow!("HTTP error {}", response.status()));
            }

            let network_stream = response.bytes_stream().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("network error: {e}"))
            });
            let reader = StreamReader::new(network_stream);
            Ok(Box::new(reader))
        }

        FetchMode::Cache => {
            if cached_path.exists() {
                println!("📂 Using cached epoch {}", cached_path.display());
                let file = File::open(&cached_path).await?;
                return Ok(Box::new(file));
            }

            println!(
                "⬇️  Cache mode → downloading epoch {epoch} to {}",
                cached_path.display()
            );
            let response = reqwest::get(&url).await?;
            if !response.status().is_success() {
                return Err(anyhow!("HTTP error {}", response.status()));
            }

            let mut file_out = File::create(&cached_path).await?;
            let network_stream = response.bytes_stream().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("network error: {e}"))
            });
            let mut stream_reader = StreamReader::new(network_stream);

            // Tee to disk and stream
            let (pipe_r, mut pipe_w) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match stream_reader.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let _ = file_out.write_all(&buf[..n]).await;
                            let _ = pipe_w.write_all(&buf[..n]).await;
                        }
                        Err(e) => {
                            eprintln!("❌ download error: {e}");
                            break;
                        }
                    }
                }
            });

            Ok(Box::new(pipe_r))
        }
    }
}
