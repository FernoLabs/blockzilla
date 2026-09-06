//! Optional, bounded in-process CPU profiling for the extractor binary.

use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::Args;

const DEFAULT_PROFILE_FREQUENCY_HZ: i32 = 49;
const MAX_PROFILE_FREQUENCY_HZ: i32 = 1_000;
const DEFAULT_PROFILE_DURATION_SECONDS: u64 = 60;
const MAX_PROFILE_DURATION_SECONDS: u64 = 3_600;
const MAX_PROFILE_SKIP_SECONDS: u64 = 86_400;

#[derive(Debug, Clone, Args)]
pub(crate) struct CpuProfileArgs {
    /// Absolute output path for the CPU flamegraph SVG.
    #[arg(
        long,
        global = true,
        value_name = "ABSOLUTE_SVG",
        requires = "profile_top_out"
    )]
    pub(crate) profile_flamegraph_out: Option<PathBuf>,
    /// Absolute output path for the CPU leaf-sample table.
    #[arg(
        long,
        global = true,
        value_name = "ABSOLUTE_TSV",
        requires = "profile_flamegraph_out"
    )]
    pub(crate) profile_top_out: Option<PathBuf>,
    /// Sampling frequency used by the in-process profiler.
    #[arg(
        long,
        global = true,
        default_value_t = DEFAULT_PROFILE_FREQUENCY_HZ,
        requires = "profile_flamegraph_out"
    )]
    pub(crate) profile_frequency: i32,
    /// Wait this many seconds before sampling starts.
    #[arg(
        long,
        global = true,
        default_value_t = 0,
        requires = "profile_flamegraph_out"
    )]
    pub(crate) profile_skip_seconds: u64,
    /// Stop sampling after this many seconds and write the profile while the command continues.
    #[arg(
        long,
        global = true,
        default_value_t = DEFAULT_PROFILE_DURATION_SECONDS,
        requires = "profile_flamegraph_out"
    )]
    pub(crate) profile_duration_seconds: u64,
}

impl Default for CpuProfileArgs {
    fn default() -> Self {
        Self {
            profile_flamegraph_out: None,
            profile_top_out: None,
            profile_frequency: DEFAULT_PROFILE_FREQUENCY_HZ,
            profile_skip_seconds: 0,
            profile_duration_seconds: DEFAULT_PROFILE_DURATION_SECONDS,
        }
    }
}

#[derive(Debug, Clone)]
struct ProfileOutputs {
    flamegraph: PathBuf,
    top: PathBuf,
}

/// Run one command with an optional delayed and duration-bounded CPU profile.
///
/// The profiler stops early when the command ends. When the duration ends
/// first, it writes the two requested files and lets the command continue.
pub(crate) fn with_cpu_profile<T>(
    config: &CpuProfileArgs,
    command: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let Some(outputs) = validate_cpu_profile_args(config)? else {
        return command();
    };
    prepare_profile_outputs(&outputs)?;

    // Fail before a long extraction when this platform cannot install the
    // signal-based sampler. The retained guard is created in its owner thread.
    drop(start_profiler(config.profile_frequency)?);

    let (stop_sender, stop_receiver) = mpsc::sync_channel::<()>(1);
    let (ready_sender, ready_receiver) = mpsc::sync_channel::<Result<(), String>>(1);
    let frequency = config.profile_frequency;
    let skip_seconds = config.profile_skip_seconds;
    let duration_seconds = config.profile_duration_seconds;
    let profiler = std::thread::Builder::new()
        .name("token-dump-cpu-profiler".to_owned())
        .spawn(move || {
            let result = run_profile_thread(
                outputs,
                frequency,
                skip_seconds,
                duration_seconds,
                stop_receiver,
                ready_sender,
            );
            if let Err(error) = &result {
                eprintln!("cpu_profile_error {error:#}");
            }
            result
        })
        .context("spawn token-dump CPU profiler")?;

    // With no delay, do not start command work until sampling is active.
    if skip_seconds == 0 {
        match ready_receiver
            .recv()
            .context("CPU profiler stopped before its start result")?
        {
            Ok(()) => {}
            Err(error) => {
                let _ = profiler.join();
                bail!("start token-dump CPU profiler: {error}");
            }
        }
    }

    let command_result = command();
    let _ = stop_sender.send(());
    let profile_result = profiler
        .join()
        .map_err(|_| anyhow!("token-dump CPU profiler thread panicked"))
        .and_then(|result| result);

    combine_command_and_profile_results(command_result, profile_result)
}

fn run_profile_thread(
    outputs: ProfileOutputs,
    frequency: i32,
    skip_seconds: u64,
    duration_seconds: u64,
    stop_receiver: Receiver<()>,
    ready_sender: SyncSender<Result<(), String>>,
) -> Result<()> {
    if skip_seconds > 0 {
        match stop_receiver.recv_timeout(Duration::from_secs(skip_seconds)) {
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                let error = format!(
                    "command finished before --profile-skip-seconds={skip_seconds} elapsed"
                );
                let _ = ready_sender.send(Err(error.clone()));
                bail!(error);
            }
        }
    }

    let guard = match start_profiler(frequency) {
        Ok(guard) => {
            let _ = ready_sender.send(Ok(()));
            guard
        }
        Err(error) => {
            let message = format!("{error:#}");
            let _ = ready_sender.send(Err(message));
            return Err(error);
        }
    };
    let profile_started = Instant::now();

    match stop_receiver.recv_timeout(Duration::from_secs(duration_seconds)) {
        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => {}
    }
    write_profile_outputs(
        guard,
        &outputs,
        skip_seconds,
        duration_seconds,
        profile_started.elapsed(),
    )
}

fn combine_command_and_profile_results<T>(
    command_result: Result<T>,
    profile_result: Result<()>,
) -> Result<T> {
    match (command_result, profile_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(command_error), Ok(())) => Err(command_error),
        (Ok(_), Err(profile_error)) => Err(profile_error),
        (Err(command_error), Err(profile_error)) => Err(command_error.context(format!(
            "also failed to write CPU profile: {profile_error:#}"
        ))),
    }
}

fn start_profiler(frequency: i32) -> Result<pprof::ProfilerGuard<'static>> {
    let mut builder = pprof::ProfilerGuardBuilder::default().frequency(frequency);
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    {
        builder = builder.blocklist(&["libc", "libgcc", "pthread", "vdso"]);
    }
    builder
        .build()
        .map_err(|error| anyhow!("install pprof sampler: {error}"))
}

fn validate_cpu_profile_args(config: &CpuProfileArgs) -> Result<Option<ProfileOutputs>> {
    let paths = match (
        config.profile_flamegraph_out.as_deref(),
        config.profile_top_out.as_deref(),
    ) {
        (None, None) => {
            ensure!(
                config.profile_frequency == DEFAULT_PROFILE_FREQUENCY_HZ,
                "--profile-frequency requires both profile output paths"
            );
            ensure!(
                config.profile_skip_seconds == 0,
                "--profile-skip-seconds requires both profile output paths"
            );
            ensure!(
                config.profile_duration_seconds == DEFAULT_PROFILE_DURATION_SECONDS,
                "--profile-duration-seconds requires both profile output paths"
            );
            return Ok(None);
        }
        (Some(flamegraph), Some(top)) => (flamegraph, top),
        _ => bail!("--profile-flamegraph-out and --profile-top-out must be supplied together"),
    };

    ensure!(
        (1..=MAX_PROFILE_FREQUENCY_HZ).contains(&config.profile_frequency),
        "--profile-frequency must be between 1 and {MAX_PROFILE_FREQUENCY_HZ} Hz"
    );
    ensure!(
        config.profile_skip_seconds <= MAX_PROFILE_SKIP_SECONDS,
        "--profile-skip-seconds must be at most {MAX_PROFILE_SKIP_SECONDS}"
    );
    ensure!(
        (1..=MAX_PROFILE_DURATION_SECONDS).contains(&config.profile_duration_seconds),
        "--profile-duration-seconds must be between 1 and {MAX_PROFILE_DURATION_SECONDS}"
    );

    let flamegraph = validate_output_path(paths.0, OsStr::new("svg"), "flamegraph")?;
    let top = validate_output_path(paths.1, OsStr::new("tsv"), "top table")?;
    ensure!(
        flamegraph != top,
        "CPU profile flamegraph and top table resolve to the same path"
    );
    Ok(Some(ProfileOutputs { flamegraph, top }))
}

fn validate_output_path(path: &Path, extension: &OsStr, label: &str) -> Result<PathBuf> {
    ensure!(
        path.is_absolute(),
        "CPU profile {label} path must be absolute: {}",
        path.display()
    );
    ensure!(
        path.extension() == Some(extension),
        "CPU profile {label} path must end in .{}: {}",
        extension.to_string_lossy(),
        path.display()
    );
    let file_name = path
        .file_name()
        .with_context(|| format!("CPU profile {label} path must name a file"))?;
    let parent = path
        .parent()
        .with_context(|| format!("CPU profile {label} path has no parent"))?;
    let canonical_parent = parent.canonicalize().with_context(|| {
        format!(
            "CPU profile {label} parent must already exist: {}",
            parent.display()
        )
    })?;
    ensure!(
        canonical_parent.is_dir(),
        "CPU profile {label} parent is not a directory: {}",
        parent.display()
    );
    let normalized = canonical_parent.join(file_name);
    match fs::symlink_metadata(&normalized) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => bail!(
            "CPU profile {label} output already exists; refusing to overwrite: {}",
            path.display()
        ),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect CPU profile {label} output {}", path.display()));
        }
    }
    Ok(normalized)
}

fn prepare_profile_outputs(outputs: &ProfileOutputs) -> Result<()> {
    for path in [&outputs.flamegraph, &outputs.top] {
        let temporary = temporary_profile_path(path)?;
        let probe = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| {
                format!("reserve CPU profile temporary file {}", temporary.display())
            })?;
        drop(probe);
        fs::remove_file(&temporary).with_context(|| {
            format!("release CPU profile temporary file {}", temporary.display())
        })?;
    }
    Ok(())
}

fn write_profile_outputs(
    guard: pprof::ProfilerGuard<'static>,
    outputs: &ProfileOutputs,
    skip_seconds: u64,
    duration_seconds: u64,
    sampled_duration: Duration,
) -> Result<()> {
    let report = guard
        .report()
        .build()
        .map_err(|error| anyhow!("build pprof report: {error}"))?;
    ensure!(
        !report.data.is_empty(),
        "CPU profile contains no accepted samples"
    );
    ensure!(
        report.data.keys().any(|frames| !frames.frames.is_empty()),
        "CPU profile contains accepted samples but no symbolized stack frames; rebuild the release-debug profile binary with `-C force-frame-pointers=yes` and keep debug symbols"
    );
    let accepted_samples = report.data.values().copied().sum::<isize>();

    let flamegraph_temp = temporary_profile_path(&outputs.flamegraph)?;
    let top_temp = temporary_profile_path(&outputs.top)?;
    let result = (|| -> Result<()> {
        let flamegraph = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&flamegraph_temp)
            .with_context(|| format!("create {}", flamegraph_temp.display()))?;
        report
            .flamegraph(flamegraph)
            .map_err(|error| anyhow!("write flamegraph {}: {error}", flamegraph_temp.display()))?;
        write_pprof_top_tsv(&top_temp, &report)?;
        publish_profile_pair(
            &flamegraph_temp,
            &outputs.flamegraph,
            &top_temp,
            &outputs.top,
        )?;
        Ok(())
    })();

    for temporary in [&flamegraph_temp, &top_temp] {
        if let Err(error) = fs::remove_file(temporary)
            && error.kind() != std::io::ErrorKind::NotFound
            && result.is_ok()
        {
            return Err(error).with_context(|| {
                format!("remove CPU profile temporary file {}", temporary.display())
            });
        }
    }
    result?;

    eprintln!(
        "cpu_profile flamegraph={} top={} skipped_initial_seconds={skip_seconds} max_profile_seconds={duration_seconds} sampled_seconds={:.6} accepted_samples={accepted_samples}",
        outputs.flamegraph.display(),
        outputs.top.display(),
        sampled_duration.as_secs_f64(),
    );
    Ok(())
}

fn publish_profile_pair(
    flamegraph_temp: &Path,
    flamegraph: &Path,
    top_temp: &Path,
    top: &Path,
) -> Result<()> {
    // A hard link publishes without overwriting a path created after preflight.
    fs::hard_link(flamegraph_temp, flamegraph).with_context(|| {
        format!(
            "publish CPU flamegraph {} -> {}",
            flamegraph_temp.display(),
            flamegraph.display()
        )
    })?;
    if let Err(error) = fs::hard_link(top_temp, top) {
        let rollback = fs::remove_file(flamegraph);
        return Err(error).with_context(|| {
            let rollback_note = rollback
                .err()
                .map(|rollback_error| format!("; rollback also failed: {rollback_error}"))
                .unwrap_or_default();
            format!(
                "publish CPU top table {} -> {}{rollback_note}",
                top_temp.display(),
                top.display()
            )
        });
    }
    Ok(())
}

fn write_pprof_top_tsv(path: &Path, report: &pprof::Report) -> Result<()> {
    let mut leaves = BTreeMap::<String, isize>::new();
    let mut total_samples = 0isize;
    for (frames, count) in &report.data {
        total_samples += *count;
        let leaf = frames
            .frames
            .first()
            .and_then(|symbols| symbols.first())
            .map(|symbol| symbol.name())
            .unwrap_or_else(|| "unknown".to_owned());
        *leaves.entry(leaf).or_default() += *count;
    }
    let mut values = leaves.into_iter().collect::<Vec<_>>();
    values.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create {}", path.display()))?,
    );
    writeln!(writer, "rank\tleaf_samples\tpercent\tfunction")?;
    for (rank, (name, samples)) in values.into_iter().take(80).enumerate() {
        let percent = if total_samples > 0 {
            samples as f64 * 100.0 / total_samples as f64
        } else {
            0.0
        };
        writeln!(
            writer,
            "{}\t{}\t{percent:.2}\t{}",
            rank + 1,
            samples,
            name.replace(['\t', '\n'], " ")
        )?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn temporary_profile_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .context("CPU profile output must name a file")?
        .to_string_lossy();
    Ok(path.with_file_name(format!(".{file_name}.partial.{}", std::process::id())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_args(directory: &Path) -> CpuProfileArgs {
        CpuProfileArgs {
            profile_flamegraph_out: Some(directory.join("cpu.svg")),
            profile_top_out: Some(directory.join("cpu.top.tsv")),
            ..CpuProfileArgs::default()
        }
    }

    #[test]
    fn disabled_profile_runs_command_without_starting_sampler() {
        let mut called = false;
        let value = with_cpu_profile(&CpuProfileArgs::default(), || {
            called = true;
            Ok(17)
        })
        .unwrap();
        assert!(called);
        assert_eq!(value, 17);
    }

    #[test]
    fn profile_requires_both_absolute_typed_outputs() {
        let directory = tempfile::tempdir().unwrap();
        let mut args = enabled_args(directory.path());
        args.profile_top_out = None;
        assert!(validate_cpu_profile_args(&args).is_err());

        let mut args = enabled_args(directory.path());
        args.profile_flamegraph_out = Some(PathBuf::from("cpu.svg"));
        assert!(validate_cpu_profile_args(&args).is_err());

        let mut args = enabled_args(directory.path());
        args.profile_top_out = Some(directory.path().join("cpu.txt"));
        assert!(validate_cpu_profile_args(&args).is_err());
    }

    #[test]
    fn profile_bounds_frequency_delay_and_duration() {
        let directory = tempfile::tempdir().unwrap();
        let mut args = enabled_args(directory.path());
        args.profile_frequency = 0;
        assert!(validate_cpu_profile_args(&args).is_err());

        let mut args = enabled_args(directory.path());
        args.profile_skip_seconds = MAX_PROFILE_SKIP_SECONDS + 1;
        assert!(validate_cpu_profile_args(&args).is_err());

        let mut args = enabled_args(directory.path());
        args.profile_duration_seconds = 0;
        assert!(validate_cpu_profile_args(&args).is_err());
    }

    #[test]
    fn profile_refuses_existing_outputs_and_stale_temporary_files() {
        let directory = tempfile::tempdir().unwrap();
        let args = enabled_args(directory.path());
        std::fs::File::create(args.profile_top_out.as_ref().unwrap()).unwrap();
        assert!(validate_cpu_profile_args(&args).is_err());

        fs::remove_file(args.profile_top_out.as_ref().unwrap()).unwrap();
        let outputs = validate_cpu_profile_args(&args).unwrap().unwrap();
        std::fs::File::create(temporary_profile_path(&outputs.flamegraph).unwrap()).unwrap();
        assert!(prepare_profile_outputs(&outputs).is_err());
    }

    #[test]
    fn profile_validation_accepts_fresh_explicit_outputs() {
        let directory = tempfile::tempdir().unwrap();
        let args = enabled_args(directory.path());
        let outputs = validate_cpu_profile_args(&args).unwrap().unwrap();
        let canonical = directory.path().canonicalize().unwrap();
        assert_eq!(outputs.flamegraph, canonical.join("cpu.svg"));
        assert_eq!(outputs.top, canonical.join("cpu.top.tsv"));
        prepare_profile_outputs(&outputs).unwrap();
    }

    #[test]
    fn profile_pair_publication_does_not_overwrite_a_racing_output() {
        let directory = tempfile::tempdir().unwrap();
        let flamegraph_temp = directory.path().join("cpu.svg.partial");
        let top_temp = directory.path().join("cpu.top.tsv.partial");
        let flamegraph = directory.path().join("cpu.svg");
        let top = directory.path().join("cpu.top.tsv");
        fs::write(&flamegraph_temp, b"new-svg").unwrap();
        fs::write(&top_temp, b"new-top").unwrap();
        fs::write(&top, b"existing-top").unwrap();

        assert!(publish_profile_pair(&flamegraph_temp, &flamegraph, &top_temp, &top).is_err());
        assert!(!flamegraph.exists());
        assert_eq!(fs::read(top).unwrap(), b"existing-top");
    }
}
