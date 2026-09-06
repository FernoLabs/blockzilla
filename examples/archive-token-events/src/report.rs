use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::layout::validate_report_target;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

static REPORT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize)]
pub struct StatusReport<'a> {
    pub schema: &'static str,
    pub status: &'a str,
    pub epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<&'a str>,
}

pub fn write_status(
    path: &Path,
    epoch: u64,
    format: Option<&str>,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    write_json_atomic(
        path,
        &StatusReport {
            schema: "blockzilla-archive-token-events/status-v1",
            status,
            epoch,
            format,
            error,
        },
    )
}

/// Replace one JSON report atomically. The temporary file is private, flushed,
/// and in the same directory as the final report.
pub fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    validate_report_target(path)?;
    let parent = path.parent().context("report path has no parent")?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("report file name is not valid UTF-8")?;
    let sequence = REPORT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));

    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&temp)
            .with_context(|| format!("create temporary report {}", temp.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value).context("encode JSON report")?;
        writer.write_all(b"\n").context("finish JSON report")?;
        writer.flush().context("flush JSON report")?;
        let file: File = writer
            .into_inner()
            .map_err(|error| error.into_error())
            .context("take JSON report file")?;
        file.sync_all().context("sync JSON report")?;
        std::fs::rename(&temp, path).with_context(|| {
            format!("replace report {} with {}", path.display(), temp.display())
        })?;
        File::open(parent)
            .with_context(|| format!("open report directory {}", parent.display()))?
            .sync_all()
            .context("sync report directory")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

pub const fn duration_ns(duration: std::time::Duration) -> u64 {
    let value = duration.as_nanos();
    if value > u64::MAX as u128 {
        u64::MAX
    } else {
        value as u64
    }
}
