//! Background task that periodically samples non-blockzilla-owned process
//! I/O on this host and feeds it into `state::set_local_process_io`.
//!
//! This used to be a separate publisher writing a JSON sidecar for the
//! retired proxy service. The collection now runs in-process and reuses
//! this monitor's tested procfs collector. See
//! `docs/operations/nas-deployment-layout.md`.

use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::process_telemetry::{
    ProcessIoEntry, ProcessIoStatus, ProcessSample, collect_processes, process_io_status,
};

use crate::{snapshot, state};

const PROC_ROOT: &str = "/proc";
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

pub fn start() {
    tokio::spawn(async move {
        let mut previous: BTreeMap<u32, ProcessSample> = BTreeMap::new();
        let mut previous_at: Option<u64> = None;
        loop {
            // Errors here mean no /proc (a non-Linux host, e.g. local dev on
            // macOS) or a transient read race -- retry on the next tick
            // rather than tearing down the task.
            if let Some(now) = unix_now()
                && let Ok(collection) = collect_processes(Path::new(PROC_ROOT))
            {
                let elapsed = previous_at
                    .map(|at| now.saturating_sub(at) as f64)
                    .unwrap_or(0.0);
                let status = process_io_status(
                    &collection.samples,
                    &previous,
                    elapsed,
                    collection.inaccessible,
                    collection.clock_ticks,
                    now,
                );
                state::set_local_process_io(to_snapshot(status)).await;
                previous = collection.samples;
                previous_at = Some(now);
            }
            tokio::time::sleep(SAMPLE_INTERVAL).await;
        }
    });
}

fn unix_now() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn to_snapshot(status: ProcessIoStatus) -> snapshot::ProcessIoSnapshot {
    snapshot::ProcessIoSnapshot {
        state: status.state.to_string(),
        sampled_unix_secs: Some(status.sampled_unix_secs),
        sample_window_secs: status.sample_window_secs.map(|secs| secs.round() as u64),
        active_count: status.active_count as u32,
        truncated: status.truncated,
        processes: status.processes.into_iter().map(to_entry).collect(),
    }
}

fn to_entry(process: ProcessIoEntry) -> snapshot::ProcessIoEntry {
    snapshot::ProcessIoEntry {
        id: process.id,
        pid: process.pid,
        name: process.name,
        read_mib_per_sec: Some(process.read_mib_per_sec),
        write_mib_per_sec: Some(process.write_mib_per_sec),
        cpu_percent: process.cpu_percent,
        rss_bytes: Some(process.rss_bytes),
        blockzilla_owned: Some(process.blockzilla_owned),
    }
}
