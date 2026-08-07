use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use blockzilla_replay::launch_replay::{
    LaunchInstructionDiffCapture, resume_launch_chain_diagnostic_from_checkpoint,
    resume_launch_chain_diagnostic_from_checkpoint_with_generation_metrics,
    visit_launch_chain_diagnostic_with_checkpoint, visit_launch_chain_diagnostic_with_diff_capture,
    visit_launch_chain_diagnostic_with_generation_metrics,
    visit_launch_prefix_diagnostic_with_diff_capture,
};
use blockzilla_replay::{
    CompactGenerationContext, CompactInstructionData, CompactProbeConfig, CompactVisitConfig,
    CompilationBackend, ExecutionRequest, LaunchCheckpointPublication,
    LaunchCheckpointResumeConfig, LaunchGenerationMetrics, LaunchInstructionEffect,
    LaunchInstructionMutation, LaunchReplayError, LaunchReplayFailure, LaunchReplayOutcome,
    LaunchStakeError, LaunchTransactionFailureReason, LaunchVoteMutation, LoaderAccountKind,
    ReplayCompiler, probe_compact_generation, pubkey_to_base58, read_genesis_summary,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};

const DEFAULT_PROFILE_FREQUENCY_HZ: i32 = 99;
const MAX_PROFILE_FREQUENCY_HZ: i32 = 1_000;

#[derive(Debug, Parser)]
#[command(
    name = "blockzilla-replay-poc",
    about = "Program extraction/JIT and genesis experiments for replay-first Blockzilla"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the bundled SBPFv0 ELF through extraction, verification, compilation,
    /// and execution using the target's native backend when available.
    Demo {
        #[arg(long, default_value_t = 1)]
        input_byte: u8,
        #[arg(long, value_enum, default_value_t = CliEngine::Auto)]
        engine: CliEngine,
    },
    /// Extract and compile a bare ELF or loader account image.
    Compile {
        input: PathBuf,
        #[arg(long, value_enum, default_value_t = CliLoader::BareElf)]
        loader: CliLoader,
        /// Write the canonical, allocation-padding-free ELF to this path.
        #[arg(long)]
        extract_to: Option<PathBuf>,
        /// Also execute the self-contained minor-program ABI with this byte.
        #[arg(long)]
        execute_byte: Option<u8>,
        #[arg(long, value_enum, default_value_t = CliEngine::Auto)]
        engine: CliEngine,
    },
    /// Parse a Solana genesis archive and print the replay-critical fingerprint.
    Genesis { archive: PathBuf },
    /// Validate and inspect an ordered Blockzilla compact generation.
    ProbeCompact {
        generation: PathBuf,
        #[arg(long)]
        start_slot: Option<u64>,
        #[arg(long)]
        end_slot_exclusive: Option<u64>,
        #[arg(long, default_value_t = 10)]
        max_slots: usize,
        /// Retain this many decoded transactions for detailed output. All
        /// transactions in the selected slots are still counted.
        #[arg(long, default_value_t = 5)]
        sample_transactions: usize,
    },
    /// Execute the native-Config/System/Stake/trusted-Vote launch POC from Compact data.
    ReplayCompactPrefix {
        generation: PathBuf,
        #[arg(long)]
        start_slot: Option<u64>,
        #[arg(long)]
        end_slot_exclusive: Option<u64>,
        #[arg(long, default_value_t = 10)]
        max_slots: usize,
        #[arg(long, default_value_t = 10)]
        sample_diffs: usize,
        /// Print at most this many final changed-account summaries.
        #[arg(long, default_value_t = 10)]
        sample_accounts: usize,
    },
    /// Resume one Bank from a trusted checkpoint at a completed generation boundary.
    ResumeCompactChain {
        #[arg(long)]
        checkpoint: PathBuf,
        /// Trusted standard SHA-256 over the complete checkpoint file.
        #[arg(long)]
        expected_checkpoint_sha256: String,
        /// Exact completed Compact generation whose final row was checkpointed.
        #[arg(long)]
        completed_generation: PathBuf,
        /// Successor Compact generation directories in ledger order.
        #[arg(required = true, num_args = 1..)]
        generations: Vec<PathBuf>,
        #[arg(long)]
        end_slot_exclusive: Option<u64>,
        #[arg(long)]
        max_slots: Option<usize>,
        #[arg(long, default_value_t = 10)]
        sample_diffs: usize,
        #[arg(long, default_value_t = 10)]
        sample_accounts: usize,
        /// Opt-in worker count for independent direct-Vote replay. One keeps
        /// the established sequential executor.
        #[arg(long, default_value_t = 1)]
        replay_workers: usize,
        /// Atomically refresh this path at each newly completed boundary.
        #[arg(long)]
        checkpoint_out: Option<PathBuf>,
        /// Emit host-only phase timings immediately after each completed
        /// sealed generation boundary.
        #[arg(long)]
        generation_metrics: bool,
        #[command(flatten)]
        cpu_profile: CpuProfileArgs,
    },
    /// Execute one replay Bank across ordered Blockzilla Compact generations.
    ReplayCompactChain {
        /// Compact generation directories in ledger order.
        #[arg(required = true, num_args = 1..)]
        generations: Vec<PathBuf>,
        #[arg(long)]
        start_slot: Option<u64>,
        #[arg(long)]
        end_slot_exclusive: Option<u64>,
        /// Globally cap present Compact blocks across all generations. By
        /// default the complete selected chain is replayed.
        #[arg(long)]
        max_slots: Option<usize>,
        #[arg(long, default_value_t = 10)]
        sample_diffs: usize,
        /// Print at most this many final changed-account summaries.
        #[arg(long, default_value_t = 10)]
        sample_accounts: usize,
        /// Atomically refresh this frozen checkpoint after every fully consumed
        /// sealed Compact generation.
        #[arg(long)]
        checkpoint_out: Option<PathBuf>,
        /// Emit host-only phase timings immediately after each completed
        /// sealed generation boundary.
        #[arg(long)]
        generation_metrics: bool,
        #[command(flatten)]
        cpu_profile: CpuProfileArgs,
    },
}

#[derive(Debug, Clone, Args)]
struct CpuProfileArgs {
    /// Write an in-process sampled CPU flamegraph SVG. Use a symbolized build
    /// (for example the workspace `release-debug` profile) for useful names.
    #[arg(long)]
    flamegraph_out: Option<PathBuf>,
    /// Sampling frequency used with --flamegraph-out.
    #[arg(
        long,
        default_value_t = DEFAULT_PROFILE_FREQUENCY_HZ,
        requires = "flamegraph_out"
    )]
    profile_frequency: i32,
    /// Wait this many seconds before starting the sampler. This is useful for
    /// excluding trusted checkpoint load and hash verification.
    #[arg(long, default_value_t = 0, requires = "flamegraph_out")]
    profile_skip_seconds: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliLoader {
    BareElf,
    Legacy,
    UpgradeableBuffer,
    UpgradeableProgramdata,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliEngine {
    Auto,
    Interpreter,
    NativeRequired,
}

impl From<CliEngine> for ExecutionRequest {
    fn from(value: CliEngine) -> Self {
        match value {
            CliEngine::Auto => Self::Auto,
            CliEngine::Interpreter => Self::Interpreter,
            CliEngine::NativeRequired => Self::NativeRequired,
        }
    }
}

impl From<CliLoader> for LoaderAccountKind {
    fn from(value: CliLoader) -> Self {
        match value {
            CliLoader::BareElf => Self::BareElf,
            CliLoader::Legacy => Self::Legacy,
            CliLoader::UpgradeableBuffer => Self::UpgradeableBuffer,
            CliLoader::UpgradeableProgramdata => Self::UpgradeableProgramData,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Demo { input_byte, engine } => {
            let elf = STANDARD
                .decode(include_str!("../fixtures/relative_call_sbpfv0.so.b64").trim())
                .context("decode bundled SBPF fixture")?;
            compile_and_report(
                &elf,
                LoaderAccountKind::BareElf,
                Some(input_byte),
                None,
                engine.into(),
            )
        }
        Command::Compile {
            input,
            loader,
            extract_to,
            execute_byte,
            engine,
        } => {
            let bytes =
                std::fs::read(&input).with_context(|| format!("read {}", input.display()))?;
            compile_and_report(
                &bytes,
                loader.into(),
                execute_byte,
                extract_to,
                engine.into(),
            )
        }
        Command::Genesis { archive } => print_genesis(&archive),
        Command::ProbeCompact {
            generation,
            start_slot,
            end_slot_exclusive,
            max_slots,
            sample_transactions,
        } => print_compact_probe(
            &generation,
            CompactProbeConfig {
                start_slot,
                end_slot_exclusive,
                max_slots,
                max_transactions: sample_transactions,
            },
        ),
        Command::ReplayCompactPrefix {
            generation,
            start_slot,
            end_slot_exclusive,
            max_slots,
            sample_diffs,
            sample_accounts,
        } => replay_compact_prefix(
            &generation,
            CompactVisitConfig {
                start_slot,
                end_slot_exclusive,
                max_slots: Some(max_slots),
            },
            sample_diffs,
            sample_accounts,
        ),
        Command::ReplayCompactChain {
            generations,
            start_slot,
            end_slot_exclusive,
            max_slots,
            sample_diffs,
            sample_accounts,
            checkpoint_out,
            generation_metrics,
            cpu_profile,
        } => with_cpu_profile(&cpu_profile, || {
            replay_compact_chain(
                &generations,
                CompactVisitConfig {
                    start_slot,
                    end_slot_exclusive,
                    max_slots,
                },
                sample_diffs,
                sample_accounts,
                checkpoint_out.as_deref(),
                generation_metrics,
            )
        }),
        Command::ResumeCompactChain {
            checkpoint,
            expected_checkpoint_sha256,
            completed_generation,
            generations,
            end_slot_exclusive,
            max_slots,
            sample_diffs,
            sample_accounts,
            replay_workers,
            checkpoint_out,
            generation_metrics,
            cpu_profile,
        } => with_cpu_profile(&cpu_profile, || {
            resume_compact_chain(
                &checkpoint,
                parse_sha256(&expected_checkpoint_sha256)
                    .context("parse --expected-checkpoint-sha256")?,
                &completed_generation,
                &generations,
                CompactVisitConfig {
                    start_slot: None,
                    end_slot_exclusive,
                    max_slots,
                },
                sample_diffs,
                sample_accounts,
                replay_workers,
                checkpoint_out.as_deref(),
                generation_metrics,
            )
        }),
    }
}

fn with_cpu_profile(config: &CpuProfileArgs, replay: impl FnOnce() -> Result<()>) -> Result<()> {
    validate_cpu_profile_args(config)?;
    if config.flamegraph_out.is_none() {
        return replay();
    }
    prepare_cpu_profile_outputs(
        config
            .flamegraph_out
            .as_deref()
            .expect("validated profiler output path"),
    )?;
    if config.profile_skip_seconds > 0 {
        // Fail before a long replay if this platform cannot install the
        // profiler. Dropping the probe guard stops and clears its samples; the
        // delayed worker starts the retained profile after the requested wait.
        drop(start_profiler(
            config.flamegraph_out.as_ref(),
            config.profile_frequency,
        )?);
        return with_delayed_cpu_profile(config, replay);
    }

    let profiler = start_profiler(config.flamegraph_out.as_ref(), config.profile_frequency)?;
    let replay_result = replay();
    let profile_result = match (profiler, config.flamegraph_out.as_deref()) {
        (Some(guard), Some(path)) => {
            write_flamegraph_outputs(guard, path, config.profile_skip_seconds)
        }
        _ => Ok(()),
    };

    combine_replay_and_profile_results(replay_result, profile_result)
}

fn with_delayed_cpu_profile(
    config: &CpuProfileArgs,
    replay: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let path = config
        .flamegraph_out
        .clone()
        .expect("delayed profiler requires an output path");
    let frequency = config.profile_frequency;
    let delay_seconds = config.profile_skip_seconds;
    let (stop_sender, stop_receiver) = std::sync::mpsc::sync_channel::<()>(1);
    let profiler = std::thread::Builder::new()
        .name("replay-cpu-profiler".to_owned())
        .spawn(move || -> Result<()> {
            let result = (|| -> Result<()> {
                match stop_receiver.recv_timeout(Duration::from_secs(delay_seconds)) {
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        bail!(
                            "replay finished before --profile-skip-seconds={delay_seconds} elapsed"
                        );
                    }
                }
                let guard = start_profiler(Some(&path), frequency)?
                    .expect("profiler output path was supplied");
                stop_receiver
                    .recv()
                    .context("replay ended without stopping delayed CPU profiler")?;
                write_flamegraph_outputs(guard, &path, delay_seconds)
            })();
            if let Err(error) = &result {
                eprintln!("cpu_profile_error {error:#}");
            }
            result
        })
        .context("spawn delayed CPU profiler")?;

    let replay_result = replay();
    let _ = stop_sender.send(());
    let profile_result = profiler
        .join()
        .map_err(|_| anyhow!("delayed CPU profiler thread panicked"))
        .and_then(|result| result);

    combine_replay_and_profile_results(replay_result, profile_result)
}

fn combine_replay_and_profile_results(
    replay_result: Result<()>,
    profile_result: Result<()>,
) -> Result<()> {
    match (replay_result, profile_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(replay_error), Ok(())) => Err(replay_error),
        (Ok(()), Err(profile_error)) => Err(profile_error),
        (Err(replay_error), Err(profile_error)) => Err(replay_error.context(format!(
            "also failed to write CPU profile: {profile_error:#}"
        ))),
    }
}

fn start_profiler(
    flamegraph_out: Option<&PathBuf>,
    frequency: i32,
) -> Result<Option<pprof::ProfilerGuard<'static>>> {
    if flamegraph_out.is_none() {
        return Ok(None);
    }
    if !(1..=MAX_PROFILE_FREQUENCY_HZ).contains(&frequency) {
        bail!("--profile-frequency must be between 1 and {MAX_PROFILE_FREQUENCY_HZ} Hz");
    }
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
        .map(Some)
        .map_err(|err| anyhow!("start pprof profiler: {err}"))
}

fn write_flamegraph_outputs(
    guard: pprof::ProfilerGuard<'static>,
    path: &Path,
    skip_seconds: u64,
) -> Result<()> {
    let report = guard
        .report()
        .build()
        .map_err(|err| anyhow!("build pprof report: {err}"))?;
    if report.data.is_empty() {
        bail!("CPU profile contains no accepted samples");
    }

    let flamegraph_temp = temporary_profile_path(path)?;
    let flamegraph = File::create(&flamegraph_temp)
        .with_context(|| format!("create {}", flamegraph_temp.display()))?;
    report
        .flamegraph(flamegraph)
        .map_err(|err| anyhow!("write flamegraph {}: {err}", flamegraph_temp.display()))?;

    let mut top_path = path.to_path_buf();
    top_path.set_extension("top.tsv");
    let top_temp = temporary_profile_path(&top_path)?;
    write_pprof_top_tsv(&top_temp, &report)?;
    std::fs::rename(&top_temp, &top_path).with_context(|| {
        format!(
            "publish CPU profile top table {} -> {}",
            top_temp.display(),
            top_path.display()
        )
    })?;
    std::fs::rename(&flamegraph_temp, path).with_context(|| {
        format!(
            "publish CPU flamegraph {} -> {}",
            flamegraph_temp.display(),
            path.display()
        )
    })?;
    eprintln!(
        "cpu_profile flamegraph={} top={} skipped_initial_seconds={skip_seconds}",
        path.display(),
        top_path.display()
    );
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

    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
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

fn validate_cpu_profile_args(config: &CpuProfileArgs) -> Result<()> {
    if config.flamegraph_out.is_none() {
        if config.profile_frequency != DEFAULT_PROFILE_FREQUENCY_HZ {
            bail!("--profile-frequency requires --flamegraph-out");
        }
        if config.profile_skip_seconds != 0 {
            bail!("--profile-skip-seconds requires --flamegraph-out");
        }
        return Ok(());
    }
    if !(1..=MAX_PROFILE_FREQUENCY_HZ).contains(&config.profile_frequency) {
        bail!("--profile-frequency must be between 1 and {MAX_PROFILE_FREQUENCY_HZ} Hz");
    }
    Ok(())
}

fn prepare_cpu_profile_outputs(path: &Path) -> Result<()> {
    let mut top_path = path.to_path_buf();
    top_path.set_extension("top.tsv");
    for output in [path, top_path.as_path()] {
        if let Some(parent) = output.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        if output.exists() && !output.is_file() {
            bail!("CPU profile output is not a file: {}", output.display());
        }
        let temporary = temporary_profile_path(output)?;
        File::create(&temporary)
            .with_context(|| format!("reserve CPU profile output {}", temporary.display()))?;
        std::fs::remove_file(&temporary)
            .with_context(|| format!("release CPU profile output {}", temporary.display()))?;
    }
    Ok(())
}

fn temporary_profile_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .context("CPU profile output must name a file")?
        .to_string_lossy();
    Ok(path.with_file_name(format!(".{file_name}.partial.{}", std::process::id())))
}

fn replay_compact_prefix(
    path: &PathBuf,
    config: CompactVisitConfig,
    sample_diffs: usize,
    sample_accounts: usize,
) -> Result<()> {
    let mut sampled_mutations = Vec::with_capacity(sample_diffs);
    let streaming = visit_launch_prefix_diagnostic_with_diff_capture(
        path,
        config,
        instruction_diff_capture(sample_diffs),
        |mutation| {
        if sampled_mutations.len() < sample_diffs {
            sampled_mutations.push(mutation.clone());
        }
    },
    )
    .with_context(|| {
        format!(
            "execute native-Config/System/Stake/trusted-Vote launch replay from compact generation {}",
            path.display()
        )
    })?;
    print_launch_replay_report(
        "streaming-one-compact-block-at-a-time",
        std::slice::from_ref(&streaming.context),
        &streaming.replay,
        streaming.failure.as_ref(),
        &sampled_mutations,
        sample_diffs,
        sample_accounts,
    )
}

fn replay_compact_chain(
    paths: &[PathBuf],
    config: CompactVisitConfig,
    sample_diffs: usize,
    sample_accounts: usize,
    checkpoint_out: Option<&std::path::Path>,
    generation_metrics: bool,
) -> Result<()> {
    let mut sampled_mutations = Vec::with_capacity(sample_diffs);
    let mut record_mutation = |mutation: &LaunchInstructionMutation| {
        if sampled_mutations.len() < sample_diffs {
            sampled_mutations.push(mutation.clone());
        }
    };
    let streaming = if generation_metrics {
        visit_launch_chain_diagnostic_with_generation_metrics(
            paths,
            config,
            instruction_diff_capture(sample_diffs),
            checkpoint_out,
            print_generation_metrics,
            &mut record_mutation,
        )
    } else if let Some(checkpoint_out) = checkpoint_out {
        visit_launch_chain_diagnostic_with_checkpoint(
            paths,
            config,
            instruction_diff_capture(sample_diffs),
            checkpoint_out,
            &mut record_mutation,
        )
    } else {
        visit_launch_chain_diagnostic_with_diff_capture(
            paths,
            config,
            instruction_diff_capture(sample_diffs),
            &mut record_mutation,
        )
    }
    .with_context(|| {
        format!(
            "execute native-Config/System/Stake/trusted-Vote launch replay across {} ordered compact generations",
            paths.len()
        )
    })?;
    let report = print_launch_replay_report(
        "streaming-ordered-compact-generation-chain",
        &streaming.contexts,
        &streaming.replay,
        streaming.failure.as_ref(),
        &sampled_mutations,
        sample_diffs,
        sample_accounts,
    );
    print_checkpoint_report(
        streaming.checkpoint_source.as_ref(),
        &streaming.checkpoint_publications,
    );
    report
}

#[allow(clippy::too_many_arguments)]
fn resume_compact_chain(
    checkpoint: &std::path::Path,
    expected_checkpoint_sha256: [u8; 32],
    completed_generation: &std::path::Path,
    paths: &[PathBuf],
    config: CompactVisitConfig,
    sample_diffs: usize,
    sample_accounts: usize,
    replay_workers: usize,
    checkpoint_out: Option<&std::path::Path>,
    generation_metrics: bool,
) -> Result<()> {
    let mut sampled_mutations = Vec::with_capacity(sample_diffs);
    let mut record_mutation = |mutation: &LaunchInstructionMutation| {
        if sampled_mutations.len() < sample_diffs {
            sampled_mutations.push(mutation.clone());
        }
    };
    let resume = LaunchCheckpointResumeConfig {
        checkpoint_path: checkpoint,
        expected_checkpoint_file_sha256: expected_checkpoint_sha256,
        completed_generation,
        checkpoint_out,
        replay_workers,
    };
    let streaming = if generation_metrics {
        resume_launch_chain_diagnostic_from_checkpoint_with_generation_metrics(
            paths,
            config,
            instruction_diff_capture(sample_diffs),
            resume,
            print_generation_metrics,
            &mut record_mutation,
        )
    } else {
        resume_launch_chain_diagnostic_from_checkpoint(
            paths,
            config,
            instruction_diff_capture(sample_diffs),
            resume,
            &mut record_mutation,
        )
    }
    .with_context(|| {
        format!(
            "resume native-Config/System/Stake/trusted-Vote launch replay across {} successor compact generations",
            paths.len()
        )
    })?;
    let report = print_launch_replay_report(
        "streaming-ordered-compact-generation-chain-resume",
        &streaming.contexts,
        &streaming.replay,
        streaming.failure.as_ref(),
        &sampled_mutations,
        sample_diffs,
        sample_accounts,
    );
    print_checkpoint_report(
        streaming.checkpoint_source.as_ref(),
        &streaming.checkpoint_publications,
    );
    report
}

fn instruction_diff_capture(sample_diffs: usize) -> LaunchInstructionDiffCapture {
    if sample_diffs == 0 {
        LaunchInstructionDiffCapture::None
    } else {
        LaunchInstructionDiffCapture::First(sample_diffs)
    }
}

fn print_launch_replay_report(
    input_mode: &str,
    contexts: &[CompactGenerationContext],
    replay: &LaunchReplayOutcome,
    failure: Option<&LaunchReplayFailure>,
    sampled_mutations: &[LaunchInstructionMutation],
    sample_diffs: usize,
    sample_accounts: usize,
) -> Result<()> {
    let replay_status = if failure.is_some() {
        "failed"
    } else {
        "complete"
    };
    println!("input_format=blockzilla-compact-archive-v2");
    println!("input_mode={input_mode}");
    println!(
        "runtime_profile=launch-v1.0.7-bank-sysvars-native-config-system-v1.2.32-stable-epoch40-stake-v1.1.6-authorize-v1.3.3-merge-trusted-vote-v1.2.32-update-commission-vote-switch-v1.1.14-legacy-bpf-v1.3.3-pda-cpi-immutable-account-metadata-trusted-compact-outcomes-transient-covered-prebalances-historical-loader-suffix-fast-state-v17"
    );
    println!("replay_status={replay_status}");
    if let [context] = contexts {
        println!("generation_id={}", context.generation_id);
        println!(
            "generation_digest={}",
            hex(&context.binding.generation_digest)
        );
    } else {
        println!("generation_count={}", contexts.len());
        for (index, context) in contexts.iter().enumerate() {
            println!(
                "generation index={index} epoch={} generation_id={} generation_digest={}",
                context.epoch,
                context.generation_id,
                hex(&context.binding.generation_digest)
            );
        }
    }
    println!(
        "epoch={} completed_slot_range={:?}..={:?} completed_slots={} committed_transactions={} failed_transactions={} committed_instructions={} rolled_back_instructions={} vote_mutations={} config_mutations={} system_mutations={} stake_mutations={} bpf_loader_mutations={} state_changed_accounts={} bank_sysvar_writes={} bank_sysvar_accounts={} slot_hashes_unavailable={}",
        replay.epoch,
        replay.first_slot,
        replay.last_slot,
        replay.slots_processed,
        replay.transactions_processed,
        replay.failed_transactions,
        replay.instructions_processed,
        replay.rolled_back_instructions,
        replay.vote_mutations,
        replay.config_mutations,
        replay.system_mutations,
        replay.stake_mutations,
        replay.bpf_loader_mutations,
        replay.changed_accounts.len(),
        replay.bank_sysvar_writes,
        replay.bank_sysvar_accounts_written.len(),
        replay.slot_hashes_unavailable,
    );
    println!(
        "state_scope=serialized-genesis-plus-native-builtins-plus-bank-sysvars-plus-config-system-with-stable-epoch40-activation-stake-vote-and-legacy-bpf-loader-mutations-plus-compact-writable-post-balances commit_model=trusted-compact-known-failures-program-skipped-lamports-projected-unknown-outcomes-runtime-derived archived_outcomes=failed-consumed-success-asserted-with-narrow-structural-fee-only-system-noop-recoveries-unknown-derived bank_parity=false signatures_verified=false cu_metered=false fee_logic_executed=false fee_lamports_projected_from_compact=true fee_sysvar_advanced=true fee_signature_classification=implemented-subset rent_logic_executed=false rent_lamports_projected_from_compact=true genesis_sysvars_materialized=true child_bank_sysvars_materialized=clock-fees-recent-blockhashes-rewards-stake-history freeze_sysvars_materialized=slot-history slot_hashes_materialized=false bank_hash_computed=false"
    );
    println!(
        "parallel_vote_batches={} parallel_vote_transactions={} max_parallel_vote_batch={} lazy_vote_commits={} vote_state_materializations={}",
        replay.parallel_vote_batches,
        replay.parallel_vote_transactions,
        replay.max_parallel_vote_batch,
        replay.lazy_vote_commits,
        replay.vote_state_materializations,
    );
    println!(
        "replay_state accounts={} state_changed_accounts={} bank_sysvar_accounts={} sha256={}",
        replay.account_state.len(),
        replay.changed_accounts.len(),
        replay.bank_sysvar_accounts_written.len(),
        hex(&replay.account_state.canonical_hash())
    );
    if let Some(failed) = &replay.first_failed_transaction {
        let coordinate = format!(
            "slot={} transaction={} instruction={} rolled_back_instructions={}",
            failed.location.slot,
            failed
                .location
                .transaction_index
                .map_or_else(|| "none".to_owned(), |index| index.to_string()),
            failed
                .location
                .instruction_index
                .map_or_else(|| "none".to_owned(), |index| index.to_string()),
            failed.rolled_back_instructions,
        );
        if let LaunchTransactionFailureReason::Stake(LaunchStakeError::MissingRequiredSignature {
            pubkey,
        }) = &failed.reason
        {
            println!(
                "first_derived_transaction_failure {coordinate} reason=missing_required_signature authority={}",
                pubkey_to_base58(pubkey)
            );
        } else {
            println!(
                "first_derived_transaction_failure {coordinate} reason={}",
                failed.reason
            );
        }
    }
    if let Some(failure) = failure {
        let transaction_index = failure
            .location
            .transaction_index
            .map_or_else(|| "none".to_owned(), |index| index.to_string());
        let instruction_index = failure
            .location
            .instruction_index
            .map_or_else(|| "none".to_owned(), |index| index.to_string());
        if let LaunchReplayError::UnsupportedProgram { program_id, .. } = &failure.error {
            println!(
                "first_failure slot={} transaction={} instruction={} kind=unsupported_program program={}",
                failure.location.slot,
                transaction_index,
                instruction_index,
                pubkey_to_base58(program_id)
            );
        } else {
            println!(
                "first_failure slot={} transaction={} instruction={} kind=replay_semantic error={}",
                failure.location.slot, transaction_index, instruction_index, failure.error
            );
        }
        if let Some(rolled_back) = &failure.rolled_back_transaction {
            println!(
                "rolled_back_transaction slot={} transaction={} successful_instructions={}",
                rolled_back.slot,
                rolled_back.transaction_index,
                rolled_back.instruction_mutations.len()
            );
            for mutation in rolled_back.instruction_mutations.iter().take(sample_diffs) {
                print_instruction_mutation("rolled_back_mutation", mutation);
            }
        }
    }
    for mutation in sampled_mutations {
        print_instruction_mutation("mutation", mutation);
    }
    for pubkey in replay.changed_accounts.iter().take(sample_accounts) {
        let Some(account) = replay.account_state.get(pubkey) else {
            println!(
                "final_account account={} deleted=true",
                pubkey_to_base58(pubkey)
            );
            continue;
        };
        let data_hash: [u8; 32] = Sha256::digest(&account.data).into();
        println!(
            "final_account account={} lamports={} owner={} executable={} data_len={} data_sha256={}",
            pubkey_to_base58(pubkey),
            account.lamports,
            pubkey_to_base58(&account.owner),
            account.executable,
            account.data.len(),
            hex(&data_hash)
        );
    }
    if replay.changed_accounts.len() > sample_accounts {
        println!(
            "final_accounts_omitted={}",
            replay.changed_accounts.len() - sample_accounts
        );
    }
    if let Some(failure) = failure {
        bail!("launch replay stopped at first failure: {}", failure.error);
    }
    Ok(())
}

fn print_checkpoint_report(
    checkpoint_source: Option<&CompactGenerationContext>,
    checkpoint_publications: &[LaunchCheckpointPublication],
) {
    if let Some(source) = checkpoint_source {
        println!(
            "checkpoint_source epoch={} generation_id={} generation_digest={} final_slot={:?}",
            source.epoch,
            source.generation_id,
            hex(&source.binding.generation_digest),
            source.last_slot,
        );
    }
    for publication in checkpoint_publications {
        println!(
            "checkpoint_published path={} epoch={} last_slot={} generation_digest={} account_state_sha256={} checkpoint_file_sha256={}",
            publication.path.display(),
            publication.epoch,
            publication.last_slot,
            hex(&publication.generation_digest),
            hex(&publication.account_state_sha256),
            hex(&publication.checkpoint_file_sha256),
        );
    }
}

fn print_generation_metrics(metrics: &LaunchGenerationMetrics) {
    println!("{}", format_generation_metrics(metrics));
    let _ = std::io::stdout().flush();
}

fn format_generation_metrics(metrics: &LaunchGenerationMetrics) -> String {
    let checkpoint_total = metrics
        .checkpoint_encode
        .saturating_add(metrics.checkpoint_publish)
        .saturating_add(metrics.checkpoint_state_hash);
    let compact_seconds = metrics.compact_visit.as_secs_f64();
    let per_second = |count: u64| {
        if compact_seconds == 0.0 {
            0.0
        } else {
            count as f64 / compact_seconds
        }
    };
    let compressed_gb_per_second = if compact_seconds == 0.0 {
        0.0
    } else {
        metrics.compact_compressed_bytes as f64 / 1_000_000_000.0 / compact_seconds
    };
    let account_registry_delta =
        metrics.account_registry_end as i128 - metrics.account_registry_start as i128;
    let changed_accounts_delta =
        metrics.changed_accounts_end as i128 - metrics.changed_accounts_start as i128;
    format!(
        "generation_metrics epoch={} generation_id={} generation_digest={} slot_range={}..={} slots={} blocks_present={} transactions={} instructions={} compact_compressed_payload_bytes={} compressed_payload_scope=visited_blocks_bin_frames throughput_basis=compact_visit blocks_per_s={:.3} transactions_per_s={:.3} instructions_per_s={:.3} compressed_payload_gb_per_s={:.6} account_registry_start={} account_registry_end={} account_registry_delta={} changed_accounts_start={} changed_accounts_end={} changed_accounts_delta={} committed_transactions={} failed_transactions={} committed_instructions={} rolled_back_instructions={} account_batch_commits={} account_batch_inserted={} account_batch_updated={} account_batch_deleted={} account_batch_patched={} account_batch_commit_ms={:.3} checkpoint_published={} wall_ms={:.3} compact_visit_ms={:.3} compact_decode_visit_ms={:.3} replay_ms={:.3} checkpoint_total_ms={:.3} checkpoint_encode_ms={:.3} checkpoint_publish_ms={:.3} checkpoint_state_hash_ms={:.3}",
        metrics.epoch,
        metrics.generation_id,
        hex(&metrics.generation_digest),
        metrics.first_slot,
        metrics.last_slot,
        metrics.slots_visited,
        metrics.slots_visited,
        metrics.transactions_visited,
        metrics.instructions_visited,
        metrics.compact_compressed_bytes,
        per_second(metrics.slots_visited),
        per_second(metrics.transactions_visited),
        per_second(metrics.instructions_visited),
        compressed_gb_per_second,
        metrics.account_registry_start,
        metrics.account_registry_end,
        account_registry_delta,
        metrics.changed_accounts_start,
        metrics.changed_accounts_end,
        changed_accounts_delta,
        metrics.committed_transactions,
        metrics.failed_transactions,
        metrics.committed_instructions,
        metrics.rolled_back_instructions,
        metrics.account_batch_commits,
        metrics.account_batch_inserted,
        metrics.account_batch_updated,
        metrics.account_batch_deleted,
        metrics.account_batch_patched,
        duration_millis(metrics.account_batch_commit),
        metrics.checkpoint_published,
        duration_millis(metrics.generation_wall),
        duration_millis(metrics.compact_visit),
        duration_millis(metrics.compact_decode_visit),
        duration_millis(metrics.replay),
        duration_millis(checkpoint_total),
        duration_millis(metrics.checkpoint_encode),
        duration_millis(metrics.checkpoint_publish),
        duration_millis(metrics.checkpoint_state_hash),
    )
}

fn duration_millis(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn print_instruction_mutation(label: &str, mutation: &LaunchInstructionMutation) {
    match &mutation.effect {
        LaunchInstructionEffect::Vote {
            vote_account,
            mutation: vote,
        } => match vote {
            LaunchVoteMutation::Vote(vote) => println!(
                "{label} slot={} tx={} instruction={} kind=vote vote_account={} voted_slots={:?} root={:?} credits={} disposition={:?} account_diffs={}",
                mutation.slot,
                mutation.transaction_index,
                mutation.instruction_index,
                pubkey_to_base58(vote_account),
                vote.voted_slots,
                vote.root_slot,
                vote.credits,
                mutation.diff.disposition,
                mutation.diff.accounts.len()
            ),
            vote => println!(
                "{label} slot={} tx={} instruction={} kind=vote vote_account={} effect={vote:?} disposition={:?} account_diffs={}",
                mutation.slot,
                mutation.transaction_index,
                mutation.instruction_index,
                pubkey_to_base58(vote_account),
                mutation.diff.disposition,
                mutation.diff.accounts.len()
            ),
        },
        LaunchInstructionEffect::System(system) => println!(
            "{label} slot={} tx={} instruction={} kind=system effect={system:?} disposition={:?} account_diffs={}",
            mutation.slot,
            mutation.transaction_index,
            mutation.instruction_index,
            mutation.diff.disposition,
            mutation.diff.accounts.len()
        ),
        LaunchInstructionEffect::Config(config) => println!(
            "{label} slot={} tx={} instruction={} kind=config effect={config:?} disposition={:?} account_diffs={}",
            mutation.slot,
            mutation.transaction_index,
            mutation.instruction_index,
            mutation.diff.disposition,
            mutation.diff.accounts.len()
        ),
        LaunchInstructionEffect::Stake(stake) => println!(
            "{label} slot={} tx={} instruction={} kind=stake effect={stake:?} disposition={:?} account_diffs={}",
            mutation.slot,
            mutation.transaction_index,
            mutation.instruction_index,
            mutation.diff.disposition,
            mutation.diff.accounts.len()
        ),
        LaunchInstructionEffect::BpfLoader(loader) => println!(
            "{label} slot={} tx={} instruction={} kind=bpf_loader effect={loader:?} disposition={:?} account_diffs={}",
            mutation.slot,
            mutation.transaction_index,
            mutation.instruction_index,
            mutation.diff.disposition,
            mutation.diff.accounts.len()
        ),
        LaunchInstructionEffect::BpfProgram(execution) => println!(
            "{label} slot={} tx={} instruction={} kind=bpf_program program={} engine={:?} watchdog_instructions={} disposition={:?} account_diffs={}",
            mutation.slot,
            mutation.transaction_index,
            mutation.instruction_index,
            pubkey_to_base58(&execution.program_account),
            execution.engine,
            execution.watchdog_instructions,
            mutation.diff.disposition,
            mutation.diff.accounts.len()
        ),
    }
    for account in &mutation.diff.accounts {
        println!(
            "account_diff account={} created={} deleted={} lamports_included={} owner_changed={} executable_changed={} rent_epoch_changed={}",
            pubkey_to_base58(&account.pubkey),
            account.created,
            account.deleted,
            account.lamports.is_some(),
            account.owner.is_some(),
            account.executable.is_some(),
            account.rent_epoch.is_some()
        );
        if let Some(data) = &account.data {
            println!(
                "data_diff account={} before_len={:?} after_len={:?} before_sha256={} after_sha256={} ranges={} truncated={}",
                pubkey_to_base58(&account.pubkey),
                data.before_len,
                data.after_len,
                data.before_sha256
                    .as_ref()
                    .map(|hash| hex(hash))
                    .as_deref()
                    .unwrap_or("none"),
                data.after_sha256
                    .as_ref()
                    .map(|hash| hex(hash))
                    .as_deref()
                    .unwrap_or("none"),
                data.ranges.len(),
                data.ranges_truncated
            );
        }
    }
}

fn print_compact_probe(path: &PathBuf, config: CompactProbeConfig) -> Result<()> {
    let probe = probe_compact_generation(path, config)
        .with_context(|| format!("probe compact generation {}", path.display()))?;
    println!("input_format=blockzilla-compact-archive-v2");
    println!(
        "cluster={} epoch={} generation_id={} slots_per_epoch={}",
        probe.cluster_id, probe.epoch, probe.generation_id, probe.slots_per_epoch
    );
    println!(
        "generation_digest={} registry_sha256={}",
        hex(&probe.binding.generation_digest),
        hex(&probe.binding.registry_sha256)
    );
    if let Some(genesis) = &probe.genesis {
        println!(
            "genesis source={:?} hash={} bytes={} accounts={} reward_pools={} builtins={} ticks_per_slot={} slots_per_segment={:?}",
            genesis.source,
            pubkey_to_base58(&genesis.genesis_hash),
            genesis.genesis_bin_len,
            genesis.accounts.len(),
            genesis.reward_pools.len(),
            genesis.builtins.len(),
            genesis.ticks_per_slot,
            genesis.slots_per_segment
        );
        for builtin in &genesis.builtins {
            println!(
                "genesis_builtin={} {}",
                builtin.key,
                pubkey_to_base58(&builtin.pubkey)
            );
        }
    } else {
        println!("genesis=absent");
    }
    println!(
        "scanned slots={} transactions={} retained_transactions={} instructions={}",
        probe.totals.slots_scanned,
        probe.totals.transactions_scanned,
        probe.totals.transactions_retained,
        probe.totals.instructions_scanned
    );
    for (program_id, count) in &probe.program_instruction_counts {
        println!(
            "program={} instructions={count}",
            pubkey_to_base58(program_id)
        );
    }
    for slot in &probe.slots {
        println!(
            "slot={} parent={} block_id={} txs={} retained_txs={} blockhash={}",
            slot.slot,
            slot.parent_slot,
            slot.block_id,
            slot.transaction_count,
            slot.transactions.len(),
            pubkey_to_base58(&slot.blockhash)
        );
        for transaction in &slot.transactions {
            println!(
                "tx slot={} index={} version={:?} signatures={} row_flags=0x{:x} archived_outcome={:?} accounts={} instructions={}",
                slot.slot,
                transaction.tx_index,
                transaction.version,
                transaction.signature_count,
                transaction.row_flags,
                transaction.archived_outcome,
                transaction.account_keys.len(),
                transaction.instructions.len()
            );
            for instruction in &transaction.instructions {
                let (kind, bytes) = compact_instruction_data_summary(&instruction.data);
                let detail = match &instruction.data {
                    CompactInstructionData::System(system) => format!("{system:?}"),
                    CompactInstructionData::Raw(bytes)
                    | CompactInstructionData::UnknownSystem(bytes)
                    | CompactInstructionData::UnknownVote(bytes) => {
                        let preview_len = bytes.len().min(64);
                        let suffix = if bytes.len() > preview_len { "..." } else { "" };
                        format!("{}{}", hex(&bytes[..preview_len]), suffix)
                    }
                    _ => "none".to_owned(),
                };
                println!(
                    "instruction slot={} tx={} index={} program={} accounts={} data_kind={} data_bytes={:?} data_detail={}",
                    slot.slot,
                    transaction.tx_index,
                    instruction.instruction_index,
                    pubkey_to_base58(&instruction.program_id),
                    instruction.account_indexes.len(),
                    kind,
                    bytes,
                    detail
                );
                for (position, account_index) in instruction.account_indexes.iter().enumerate() {
                    let pubkey = transaction
                        .account_keys
                        .get(*account_index as usize)
                        .map_or_else(|| "unresolved".to_owned(), pubkey_to_base58);
                    println!(
                        "instruction_account slot={} tx={} instruction={} position={} account_index={} pubkey={}",
                        slot.slot,
                        transaction.tx_index,
                        instruction.instruction_index,
                        position,
                        account_index,
                        pubkey
                    );
                }
            }
        }
    }
    Ok(())
}

fn compact_instruction_data_summary(
    data: &CompactInstructionData,
) -> (&'static str, Option<usize>) {
    match data {
        CompactInstructionData::Raw(bytes) => ("raw", Some(bytes.len())),
        CompactInstructionData::UnknownSystem(bytes) => ("unknown-system", Some(bytes.len())),
        CompactInstructionData::UnknownVote(bytes) => ("unknown-vote", Some(bytes.len())),
        CompactInstructionData::ComputeBudget(_) => ("compute-budget", None),
        CompactInstructionData::System(_) => ("system", None),
        CompactInstructionData::VoteCompactUpdateVoteState(_) => ("vote-compact-update", None),
        CompactInstructionData::VoteCompactUpdateVoteStateSwitch { .. } => {
            ("vote-compact-update-switch", None)
        }
        CompactInstructionData::VoteTowerSync(_) => ("vote-tower-sync", None),
        CompactInstructionData::VoteTowerSyncSwitch { .. } => ("vote-tower-sync-switch", None),
    }
}

fn compile_and_report(
    bytes: &[u8],
    loader: LoaderAccountKind,
    execute_byte: Option<u8>,
    extract_to: Option<PathBuf>,
    execution_request: ExecutionRequest,
) -> Result<()> {
    let extracted = blockzilla_replay::extract_program(loader, bytes)?;
    if let Some(path) = extract_to {
        std::fs::write(&path, &extracted.elf)
            .with_context(|| format!("write extracted ELF to {}", path.display()))?;
        println!("extracted_elf={}", path.display());
    }
    let compiler = ReplayCompiler::new();
    let compiled = compiler.compile_extracted(&extracted)?;
    let manifest = &compiled.manifest;
    println!("loader={:?}", extracted.loader);
    println!(
        "account_data_len={} elf_offset={} canonical_elf_len={}",
        extracted.account_data_len,
        extracted.elf_offset,
        extracted.elf.len()
    );
    println!("elf_sha256={}", hex(&manifest.elf_sha256));
    println!("artifact_key={}", hex(&manifest.artifact_key));
    println!(
        "sbpf_version={} text_vaddr=0x{:x} text_len={} entrypoint_instruction={}",
        manifest.sbpf_version,
        manifest.text_virtual_address,
        manifest.text_len,
        manifest.entrypoint_instruction
    );
    println!(
        "profile={} verifier={} protocol_compute_accounting={} watchdog_instruction_limit={}",
        manifest.profile_id,
        manifest.verifier,
        manifest.protocol_compute_accounting_enabled,
        manifest.watchdog_instruction_limit
    );
    println!(
        "native_backend_id={} native_entry_abi_id={} native_isa_fingerprint={}",
        manifest.native_backend_id,
        manifest.native_entry_abi_id,
        manifest.native_isa_fingerprint.as_deref().unwrap_or("none")
    );
    match &manifest.backend {
        CompilationBackend::NativeJitX86_64 => println!(
            "backend=native-jit-x86_64 machine_code_len={}",
            manifest.native_machine_code_len.unwrap_or_default()
        ),
        CompilationBackend::NativeCraneliftAarch64Subset => println!(
            "backend=native-cranelift-aarch64-subset machine_code_len={} lowered_instructions={}",
            manifest.native_machine_code_len.unwrap_or_default(),
            manifest
                .native_lowered_instruction_count
                .unwrap_or_default()
        ),
        CompilationBackend::InterpreterOnly { reason } => {
            println!("backend=interpreter-only reason={reason}")
        }
    }
    if let Some(input_byte) = execute_byte {
        let outcome =
            compiler.execute_with_request(&compiled, vec![input_byte], execution_request)?;
        println!(
            "execution_engine={:?} input={} return={} watchdog_instructions={}",
            outcome.engine, input_byte, outcome.return_value, outcome.watchdog_instructions
        );
    }
    Ok(())
}

fn print_genesis(path: &PathBuf) -> Result<()> {
    let summary = read_genesis_summary(path)?;
    println!("genesis_hash_base58={}", summary.genesis_hash_base58);
    println!("genesis_hash_hex={}", summary.genesis_hash_hex);
    println!("mainnet_beta={}", summary.is_mainnet_beta);
    println!(
        "creation_time_unix={} operating_mode_discriminant={} genesis_bin_len={}",
        summary.creation_time_unix, summary.operating_mode_discriminant, summary.genesis_bin_len
    );
    println!(
        "accounts={} account_data_bytes={} executable_accounts={} capitalization_lamports={}",
        summary.account_count,
        summary.account_data_bytes,
        summary.executable_account_count,
        summary.capitalization_lamports
    );
    for builtin in &summary.builtins {
        println!("builtin={} {}", builtin.name, builtin.pubkey_base58);
    }
    println!(
        "poh ticks_per_slot={} slots_per_segment={} tick_duration={}s+{}ns tick_count={:?} hashes_per_tick={:?}",
        summary.ticks_per_slot,
        summary.slots_per_segment,
        summary.tick_duration_seconds,
        summary.tick_duration_nanoseconds,
        summary.tick_count,
        summary.hashes_per_tick
    );
    println!(
        "fees target_lamports_per_signature={} target_signatures_per_slot={} min={} max={} burn_percent={}",
        summary.fees.target_lamports_per_signature,
        summary.fees.target_signatures_per_slot,
        summary.fees.minimum_lamports_per_signature,
        summary.fees.maximum_lamports_per_signature,
        summary.fees.burn_percent
    );
    println!(
        "rent lamports_per_byte_year={} exemption_threshold={} burn_percent={}",
        summary.rent.lamports_per_byte_year,
        summary.rent.exemption_threshold,
        summary.rent.burn_percent
    );
    println!(
        "inflation initial={} terminal={} taper={} foundation={} foundation_term={} storage={}",
        summary.inflation.initial,
        summary.inflation.terminal,
        summary.inflation.taper,
        summary.inflation.foundation,
        summary.inflation.foundation_term,
        summary.inflation.storage
    );
    println!(
        "epoch_schedule slots_per_epoch={} leader_offset={} warmup={} first_normal_epoch={} first_normal_slot={}",
        summary.slots_per_epoch,
        summary.leader_schedule_slot_offset,
        summary.warmup,
        summary.first_normal_epoch,
        summary.first_normal_slot
    );
    println!(
        "epoch_0 slots=[{}, {}) count={}",
        summary.epoch_zero.first_slot,
        summary.epoch_zero.end_slot_exclusive,
        summary.epoch_zero.slots
    );
    println!(
        "epoch_1 slots=[{}, {}) count={}",
        summary.epoch_one.first_slot, summary.epoch_one.end_slot_exclusive, summary.epoch_one.slots
    );
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn parse_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        bail!("SHA-256 must contain exactly 64 hexadecimal characters");
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => bail!("SHA-256 contains a non-hexadecimal character"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ordered_compact_generation_chain_and_global_bounds() {
        let cli = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "replay-compact-chain",
            "/compact/epoch-0-a",
            "/compact/epoch-0-b",
            "--start-slot",
            "10",
            "--end-slot-exclusive",
            "30",
            "--max-slots",
            "7",
            "--sample-diffs",
            "3",
            "--sample-accounts",
            "4",
            "--generation-metrics",
        ])
        .expect("chain command should parse");

        let Command::ReplayCompactChain {
            generations,
            start_slot,
            end_slot_exclusive,
            max_slots,
            sample_diffs,
            sample_accounts,
            checkpoint_out,
            generation_metrics,
            cpu_profile,
        } = cli.command
        else {
            panic!("expected replay-compact-chain command");
        };
        assert_eq!(
            generations,
            [
                PathBuf::from("/compact/epoch-0-a"),
                PathBuf::from("/compact/epoch-0-b")
            ]
        );
        assert_eq!(start_slot, Some(10));
        assert_eq!(end_slot_exclusive, Some(30));
        assert_eq!(max_slots, Some(7));
        assert_eq!(sample_diffs, 3);
        assert_eq!(sample_accounts, 4);
        assert_eq!(checkpoint_out, None);
        assert!(generation_metrics);
        assert_eq!(cpu_profile.flamegraph_out, None);
        assert_eq!(cpu_profile.profile_frequency, 99);
        assert_eq!(cpu_profile.profile_skip_seconds, 0);
    }

    #[test]
    fn parses_bounded_cpu_profile_for_checkpoint_resume() {
        let digest = "ab".repeat(32);
        let cli = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "resume-compact-chain",
            "--checkpoint",
            "/state/replay.chk",
            "--expected-checkpoint-sha256",
            &digest,
            "--completed-generation",
            "/compact/epoch-72",
            "/compact/epoch-73",
            "--max-slots",
            "10000",
            "--flamegraph-out",
            "/profiles/epoch-73.svg",
            "--profile-frequency",
            "49",
            "--profile-skip-seconds",
            "40",
        ])
        .expect("profiled resume command should parse");

        let Command::ResumeCompactChain {
            max_slots,
            cpu_profile,
            ..
        } = cli.command
        else {
            panic!("expected resume-compact-chain command");
        };
        assert_eq!(max_slots, Some(10_000));
        assert_eq!(
            cpu_profile.flamegraph_out,
            Some(PathBuf::from("/profiles/epoch-73.svg"))
        );
        assert_eq!(cpu_profile.profile_frequency, 49);
        assert_eq!(cpu_profile.profile_skip_seconds, 40);
    }

    #[test]
    fn cpu_profile_options_require_output_and_bound_frequency() {
        let missing_output = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "replay-compact-chain",
            "/compact/epoch-0",
            "--profile-skip-seconds",
            "40",
        ])
        .expect_err("profile delay without output must be rejected");
        assert_eq!(
            missing_output.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );

        let mut cpu_profile = CpuProfileArgs {
            flamegraph_out: Some(PathBuf::from("/profiles/epoch.svg")),
            profile_frequency: MAX_PROFILE_FREQUENCY_HZ + 1,
            profile_skip_seconds: 0,
        };
        assert!(validate_cpu_profile_args(&cpu_profile).is_err());
        cpu_profile.profile_frequency = MAX_PROFILE_FREQUENCY_HZ;
        assert!(validate_cpu_profile_args(&cpu_profile).is_ok());
    }

    #[test]
    fn compact_chain_defaults_to_unbounded_replay() {
        let cli = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "replay-compact-chain",
            "/compact/epoch-0",
        ])
        .expect("chain command should parse");

        let Command::ReplayCompactChain {
            generations,
            max_slots,
            sample_diffs,
            sample_accounts,
            generation_metrics,
            ..
        } = cli.command
        else {
            panic!("expected replay-compact-chain command");
        };
        assert_eq!(generations, [PathBuf::from("/compact/epoch-0")]);
        assert_eq!(max_slots, None);
        assert_eq!(sample_diffs, 10);
        assert_eq!(sample_accounts, 10);
        assert!(!generation_metrics);
    }

    #[test]
    fn parses_checkpoint_boundary_save_and_trusted_resume() {
        let fresh = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "replay-compact-chain",
            "/compact/epoch-0",
            "--checkpoint-out",
            "/state/replay.chk",
        ])
        .unwrap();
        let Command::ReplayCompactChain { checkpoint_out, .. } = fresh.command else {
            panic!("expected replay-compact-chain command");
        };
        assert_eq!(checkpoint_out, Some(PathBuf::from("/state/replay.chk")));

        let digest = "ab".repeat(32);
        let resume = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "resume-compact-chain",
            "--checkpoint",
            "/state/replay.chk",
            "--expected-checkpoint-sha256",
            &digest,
            "--completed-generation",
            "/compact/epoch-10",
            "/compact/epoch-11",
            "/compact/epoch-12",
            "--checkpoint-out",
            "/state/continued.chk",
            "--generation-metrics",
        ])
        .unwrap();
        let Command::ResumeCompactChain {
            checkpoint,
            expected_checkpoint_sha256,
            completed_generation,
            generations,
            checkpoint_out,
            generation_metrics,
            ..
        } = resume.command
        else {
            panic!("expected resume-compact-chain command");
        };
        assert_eq!(checkpoint, PathBuf::from("/state/replay.chk"));
        assert_eq!(
            parse_sha256(&expected_checkpoint_sha256).unwrap(),
            [0xab; 32]
        );
        assert_eq!(completed_generation, PathBuf::from("/compact/epoch-10"));
        assert_eq!(
            generations,
            [
                PathBuf::from("/compact/epoch-11"),
                PathBuf::from("/compact/epoch-12")
            ]
        );
        assert_eq!(checkpoint_out, Some(PathBuf::from("/state/continued.chk")));
        assert!(generation_metrics);

        assert!(
            Cli::try_parse_from([
                "blockzilla-replay-poc",
                "resume-compact-chain",
                "--checkpoint",
                "/state/replay.chk",
                "--expected-checkpoint-sha256",
                &digest,
                "--completed-generation",
                "/compact/epoch-10",
                "/compact/epoch-11",
                "--start-slot",
                "1",
            ])
            .is_err(),
            "resume must not expose an operator-selected row cursor"
        );
    }

    #[test]
    fn checkpoint_sha256_parser_is_exact_and_case_insensitive() {
        assert_eq!(parse_sha256(&"A5".repeat(32)).unwrap(), [0xa5; 32]);
        assert!(parse_sha256(&"0".repeat(63)).is_err());
        assert!(parse_sha256(&format!("{}z", "0".repeat(63))).is_err());
    }

    #[test]
    fn generation_metrics_output_is_parseable_and_labels_its_throughput_basis() {
        let metrics = LaunchGenerationMetrics {
            epoch: 7,
            generation_id: "epoch-7".to_owned(),
            generation_digest: [7; 32],
            first_slot: 70,
            last_slot: 79,
            slots_visited: 10,
            transactions_visited: 20,
            instructions_visited: 30,
            compact_compressed_bytes: 1_000_000_000,
            account_registry_start: 100,
            account_registry_end: 106,
            changed_accounts_start: 40,
            changed_accounts_end: 43,
            committed_transactions: 18,
            failed_transactions: 2,
            committed_instructions: 27,
            rolled_back_instructions: 3,
            account_batch_commits: 16,
            account_batch_inserted: 4,
            account_batch_updated: 8,
            account_batch_deleted: 2,
            account_batch_patched: 2,
            account_batch_commit: std::time::Duration::from_millis(125),
            checkpoint_published: true,
            generation_wall: std::time::Duration::from_secs(3),
            compact_visit: std::time::Duration::from_secs(2),
            compact_decode_visit: std::time::Duration::from_millis(500),
            replay: std::time::Duration::from_millis(1_500),
            checkpoint_encode: std::time::Duration::from_millis(100),
            checkpoint_publish: std::time::Duration::from_millis(200),
            checkpoint_state_hash: std::time::Duration::from_millis(300),
        };

        let line = format_generation_metrics(&metrics);
        let mut fields = line.split_ascii_whitespace();
        assert_eq!(fields.next(), Some("generation_metrics"));
        assert!(fields.all(|field| field.contains('=')));
        assert!(line.contains("throughput_basis=compact_visit"));
        assert!(line.contains("blocks_per_s=5.000"));
        assert!(line.contains("transactions_per_s=10.000"));
        assert!(line.contains("instructions_per_s=15.000"));
        assert!(line.contains("blocks_present=10"));
        assert!(line.contains("compressed_payload_scope=visited_blocks_bin_frames"));
        assert!(line.contains("compressed_payload_gb_per_s=0.500000"));
        assert!(line.contains("account_registry_delta=6"));
        assert!(line.contains("changed_accounts_delta=3"));
        assert!(line.contains("account_batch_commits=16"));
        assert!(line.contains("account_batch_commit_ms=125.000"));
        assert!(line.contains("checkpoint_total_ms=600.000"));
    }

    #[test]
    fn compact_chain_requires_at_least_one_generation() {
        let error = Cli::try_parse_from(["blockzilla-replay-poc", "replay-compact-chain"])
            .expect_err("an empty generation chain must be rejected by clap");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn compact_prefix_retains_its_existing_default_limit() {
        let cli = Cli::try_parse_from([
            "blockzilla-replay-poc",
            "replay-compact-prefix",
            "/compact/prefix",
        ])
        .expect("prefix command should parse");

        let Command::ReplayCompactPrefix {
            generation,
            max_slots,
            sample_diffs,
            sample_accounts,
            ..
        } = cli.command
        else {
            panic!("expected replay-compact-prefix command");
        };
        assert_eq!(generation, PathBuf::from("/compact/prefix"));
        assert_eq!(max_slots, 10);
        assert_eq!(sample_diffs, 10);
        assert_eq!(sample_accounts, 10);
    }

    #[test]
    fn cli_has_no_alternate_ledger_source_or_conversion_path() {
        for command in ["replay-car", "replay-rpc", "replay-shreds", "convert"] {
            assert!(
                Cli::try_parse_from(["blockzilla-replay-poc", command]).is_err(),
                "unexpected alternate ledger command {command}"
            );
        }
        for option in ["--car", "--rpc-url"] {
            assert!(
                Cli::try_parse_from([
                    "blockzilla-replay-poc",
                    "replay-compact-chain",
                    "/compact/epoch-0",
                    option,
                    "forbidden",
                ])
                .is_err(),
                "unexpected alternate ledger option {option}"
            );
        }
    }
}
