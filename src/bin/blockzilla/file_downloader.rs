use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;
use tokio::process::Command;
use tokio::{
    fs,
    io::{AsyncBufReadExt, BufReader},
    time::{sleep, Duration},
};

/// Base URL for all epochs.
/// Customize this if you ever mirror data elsewhere.
const BASE_URL: &str = "https://files.old-faithful.net";

/// Main entrypoint:
/// Download a given epoch file (e.g. 800) into a given cache directory.
/// Automatically builds the URL and output path.
/// Uses aria2c with high-performance flags.
pub async fn download_epoch(epoch: u64, cache_dir: &str, retries: usize) -> Result<PathBuf> {
    let url = format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch);
    let out_path = Path::new(cache_dir).join(format!("epoch-{}.car", epoch));

    download_with_retry(&url, &out_path, retries).await?;
    Ok(out_path)
}

/// Run aria2c to download a single file with tuned performance flags.
async fn download_with_aria2(url: &str, out_path: &Path) -> Result<()> {
    if which::which("aria2c").is_err() {
        return Err(anyhow!("aria2c not found in PATH"));
    }

    // Ensure parent directory exists
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).await.ok();
    }

    let out_dir = out_path.parent().unwrap_or(Path::new("."));
    let file_name = out_path.file_name().unwrap().to_string_lossy().to_string();

    // Tuned aria2c command: -c -x 16 -s 16 -j 8 --file-allocation=none
    let args = [
        "-c", "-x", "16", "-s", "16", "-j", "8",
        "--file-allocation=none",
        "-d", &out_dir.to_string_lossy(),
        "-o", &file_name,
        url,
    ];

    tracing::info!("🚀 aria2c starting: {}", url);

    let start = Instant::now();
    let mut child = Command::new("aria2c")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to start aria2c")?;

    // Capture stdout/stderr
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    // Pipe aria2c logs into tracing
    let stdout_task = tokio::spawn(async move {
        while let Ok(Some(line)) = stdout_reader.next_line().await {
            if !line.trim().is_empty() {
                tracing::info!("[aria2c] {}", line);
            }
        }
    });

    let stderr_task = tokio::spawn(async move {
        while let Ok(Some(line)) = stderr_reader.next_line().await {
            if !line.trim().is_empty() {
                tracing::warn!("[aria2c] {}", line);
            }
        }
    });

    let status = child.wait().await.context("aria2c execution failed")?;
    stdout_task.abort();
    stderr_task.abort();

    if !status.success() {
        return Err(anyhow!("aria2c exited with code {:?}", status.code()));
    }

    let elapsed = start.elapsed().as_secs_f64();
    tracing::info!("✅ aria2c completed in {:.1}s → {}", elapsed, out_path.display());
    Ok(())
}

/// Retry wrapper with exponential backoff
async fn download_with_retry(url: &str, out_path: &Path, retries: usize) -> Result<()> {
    for attempt in 1..=retries {
        match download_with_aria2(url, out_path).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < retries => {
                let delay = 3 * attempt as u64;
                tracing::warn!(
                    "⚠️ aria2c failed (attempt {}/{}): {} — retrying in {}s",
                    attempt,
                    retries,
                    e,
                    delay
                );
                sleep(Duration::from_secs(delay)).await;
            }
            Err(e) => return Err(e.context("All aria2c attempts failed")),
        }
    }
    Ok(())
}
