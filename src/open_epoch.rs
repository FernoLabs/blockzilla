use anyhow::{Context, Result, anyhow};
use clap::ValueEnum;
use futures::{StreamExt, TryStreamExt};
use reqwest::Client;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs,
    io::{self, AsyncRead, AsyncWriteExt},
};
use tokio_util::io::StreamReader;

// -----------------------------
// Fetch mode enum
// -----------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FetchMode {
    Network,
    Cache,
    Offline,
}

// -----------------------------
// Fast reqwest client
// -----------------------------
fn build_fast_client() -> Result<Client> {
    Ok(Client::builder()
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(15))
        .http2_adaptive_window(true)
        .build()?)
}

// -----------------------------
// HEAD helper to get total file size
// -----------------------------
async fn head_content_length(client: &Client, url: &str) -> Result<u64> {
    let resp = client.head(url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("HEAD failed: {}", resp.status()));
    }
    let len = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .context("Missing or invalid Content-Length")?;
    Ok(len)
}

async fn spawn_network_reader(url: String) -> Result<impl AsyncRead + Unpin + Send> {
    const CHUNK: u64 = 128 * 1024 * 1024;
    const PARALLEL: usize = 8;
    const PIPE_SIZE: usize = 512 * 1024 * 1024;

    let client = build_fast_client()?;
    let total_size = head_content_length(&client, &url).await?;
    println!(
        "🌐 Parallel streaming {} (size: {:.2} GB)",
        url,
        total_size as f64 / 1e9
    );

    let (mut pipe_r, mut pipe_w) = tokio::io::duplex(PIPE_SIZE);

    // Build queue of ranges
    let ranges: Arc<tokio::sync::Mutex<std::collections::VecDeque<(usize, u64, u64)>>> =
        Arc::new(tokio::sync::Mutex::new({
            let mut q = std::collections::VecDeque::new();
            let mut start = 0;
            let mut idx = 0;
            while start < total_size {
                let end = (start + CHUNK - 1).min(total_size - 1);
                q.push_back((idx, start, end));
                start += CHUNK;
                idx += 1;
            }
            q
        }));

    let client = Arc::new(client);
    let url = Arc::new(url);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, Vec<u8>)>(PARALLEL * 2);

    // Spawn bounded pool of worker tasks
    for _ in 0..PARALLEL {
        let client = client.clone();
        let url = url.clone();
        let ranges = ranges.clone();
        let tx = tx.clone();

        tokio::spawn(async move {
            loop {
                // Get next range
                let next = {
                    let mut q = ranges.lock().await;
                    q.pop_front()
                };

                let Some((i, s, e)) = next else { break };

                let range_header = format!("bytes={}-{}", s, e);
                match client
                    .get(&*url)
                    .header(reqwest::header::RANGE, &range_header)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let mut body = Vec::with_capacity((e - s + 1) as usize);
                        let mut stream = resp.bytes_stream();
                        while let Some(chunk) = stream.next().await {
                            match chunk {
                                Ok(b) => body.extend_from_slice(&b),
                                Err(e) => {
                                    eprintln!("❌ range {i} read error: {e}");
                                    return;
                                }
                            }
                        }
                        if tx.send((i, body)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("❌ range {i} ({range_header}) failed: {err}");
                        // Push it back for retry
                        let mut q = ranges.lock().await;
                        q.push_back((i, s, e));
                        // small delay avoids retry storms
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });
    }
    drop(tx);

    // Ordered writer
    tokio::spawn(async move {
        let mut buffer = BTreeMap::<usize, Vec<u8>>::new();
        let mut next = 0usize;

        while let Some((i, data)) = rx.recv().await {
            buffer.insert(i, data);
            while let Some(chunk) = buffer.remove(&next) {
                if let Err(e) = pipe_w.write_all(&chunk).await {
                    eprintln!("pipe write error: {e}");
                    return;
                }
                next += 1;
            }
        }

        let _ = pipe_w.shutdown().await;
        println!("✅ Finished ordered streaming of all ranges.");
    });

    Ok(pipe_r)
}

// -----------------------------
// Main open_epoch entry point
// -----------------------------
pub async fn open_epoch(
    epoch: u64,
    cache_dir: impl AsRef<Path>,
    mode: FetchMode,
) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let cache_dir = cache_dir.as_ref();
    fs::create_dir_all(cache_dir).await?;
    let filename = format!("epoch-{epoch}.car");
    let cached_path: PathBuf = cache_dir.join(&filename);
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
            let file = tokio::fs::File::open(&cached_path).await?;
            Ok(Box::new(file))
        }

        FetchMode::Cache => {
            if cached_path.exists() {
                println!("📂 Using cached epoch {}", cached_path.display());
                let file = tokio::fs::File::open(&cached_path).await?;
                return Ok(Box::new(file));
            }

            println!(
                "⬇️  Cache mode → downloading epoch {epoch} to {}",
                cached_path.display()
            );
            let mut reader = spawn_network_reader(url.clone()).await?;
            let mut file_out = tokio::fs::File::create(&cached_path).await?;
            io::copy(&mut reader, &mut file_out).await?;
            let file = tokio::fs::File::open(&cached_path).await?;
            Ok(Box::new(file))
        }

        FetchMode::Network => {
            let reader = spawn_network_reader(url).await?;
            Ok(Box::new(reader))
        }
    }
}
