//! Scan-scoped resource accounting and an optional external-profiler handshake.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub(super) fn thread_cpu_seconds() -> f64 {
    let mut clock: libc::timespec = unsafe { std::mem::zeroed() };
    // The clock belongs to the calling worker and does not count its wait time.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut clock) };
    assert_eq!(result, 0, "thread CPU clock failed");
    clock.tv_sec as f64 + clock.tv_nsec as f64 * 1e-9
}

pub(super) struct Usage(libc::rusage);
impl Usage {
    pub(super) fn now() -> Result<Self> {
        let mut usage = unsafe { std::mem::zeroed() };
        ensure!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0,
            "getrusage failed: {}",
            std::io::Error::last_os_error()
        );
        Ok(Self(usage))
    }
    pub(super) fn since(&self, before: &Self, wall: f64) -> Value {
        let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 * 1e-6;
        let user = seconds(self.0.ru_utime) - seconds(before.0.ru_utime);
        let system = seconds(self.0.ru_stime) - seconds(before.0.ru_stime);
        json!({"user_cpu_seconds":user, "system_cpu_seconds":system,
            "cpu_seconds":user+system, "mean_cpu_cores":(user+system)/wall,
            "minor_faults":self.0.ru_minflt-before.0.ru_minflt,
            "major_faults":self.0.ru_majflt-before.0.ru_majflt,
            "voluntary_context_switches":self.0.ru_nvcsw-before.0.ru_nvcsw,
            "involuntary_context_switches":self.0.ru_nivcsw-before.0.ru_nivcsw,
            "scope":"RUSAGE_SELF delta around scan only; includes all reader threads; excludes preload, profiler handshake and final export"})
    }
}

pub(super) struct Gate {
    directory: PathBuf,
}
impl Gate {
    pub(super) fn prepare(directory: &Path) -> Result<Self> {
        match std::fs::create_dir(directory) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    std::fs::symlink_metadata(directory)?.file_type().is_dir()
                        && std::fs::read_dir(directory)?.next().is_none(),
                    "profile control directory must be an empty real directory"
                );
            }
            Err(error) => return Err(error).context("create profile control directory"),
        }
        let gate = Self {
            directory: directory.to_owned(),
        };
        gate.signal(
            "ready.json",
            json!({"pid":std::process::id(), "phase":"resident-input-ready"}),
        )?;
        gate.wait("start")?;
        Ok(gate)
    }
    pub(super) fn complete(&self, seconds: f64, success: bool) -> Result<()> {
        self.signal(
            "scan-done.json",
            json!({"pid":std::process::id(), "scan_seconds":seconds, "scan_returned_ok":success}),
        )?;
        self.wait("release")
    }
    fn signal(&self, name: &str, value: Value) -> Result<()> {
        let temporary = self.directory.join(format!(".{name}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, &value)?;
        file.write_all(b"\n")?;
        std::fs::rename(temporary, self.directory.join(name))?;
        Ok(())
    }
    fn wait(&self, name: &str) -> Result<()> {
        let start = Instant::now();
        while !self.directory.join(name).try_exists()? {
            ensure!(
                start.elapsed() < Duration::from_secs(180),
                "profiler handshake timed out waiting for {name}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}
