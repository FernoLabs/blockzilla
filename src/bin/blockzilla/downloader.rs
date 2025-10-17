use anyhow::{Result, anyhow};
use futures::StreamExt;
use reqwest::Client;
use std::{
    fs,
    io::{BufReader, BufWriter, Write},
    path::Path,
};
use tokio::{fs as tokio_fs, io::AsyncWriteExt};

pub async fn download_with_parallel_ranges(
    url: &str,
    output_path: &Path,
    client: &Client,
    num_connections: usize,
) -> Result<()> {
    // --- 1. Determine remote file size ---
    let head_resp = client.head(url).send().await?;
    let total_size = head_resp
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("Could not determine file size for {url}"))?;

    let chunk_size = (total_size / num_connections as u64).max(1_048_576); // at least 1MB
    let mut handles = vec![];
    let mut temp_files = vec![];

    // --- 2. Spawn download tasks ---
    for i in 0..num_connections {
        let start = i as u64 * chunk_size;
        let end = if i == num_connections - 1 {
            total_size - 1
        } else {
            (i as u64 + 1) * chunk_size - 1
        };
        if start >= total_size {
            break;
        }

        let url = url.to_string();
        let client = client.clone();
        let temp_path = output_path.with_extension(format!("part-{}", i));
        temp_files.push(temp_path.clone());

        let handle = tokio::spawn(async move {
            let resp = client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end))
                .send()
                .await?;

            // Range responses usually return 206 Partial Content
            if !resp.status().is_success() && resp.status().as_u16() != 206 {
                return Err(anyhow!(
                    "HTTP error {} for range {}-{}",
                    resp.status(),
                    start,
                    end
                ));
            }

            let mut file = tokio_fs::File::create(&temp_path).await?;
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
            }
            Ok::<(), anyhow::Error>(())
        });

        handles.push(handle);
    }

    // --- 3. Wait for all parts to finish ---
    for handle in handles {
        handle.await??;
    }

    // --- 4. Concatenate parts into final file ---
    let mut output = BufWriter::new(std::fs::File::create(output_path)?);
    for temp_path in temp_files {
        let mut file = BufReader::new(std::fs::File::open(&temp_path)?);
        std::io::copy(&mut file, &mut output)?;
        let _ = fs::remove_file(&temp_path);
    }
    output.flush()?;

    Ok(())
}
