mod allocator;
#[cfg(feature = "cpu-profile")]
mod profiling;

use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use blockzilla_read_sdk::ArchiveV2WireProfile;
use blockzilla_token_transaction_dump::{
    ConsolidateConfig, ExtractConfig, ExtractSourceMode, ProbeConfig, SPYX_MINT,
    SPYX_MINT_SIGNATURE, SPYX_MINT_SLOT, build_consolidated_token_history_report, consolidate_dump,
    extract_dump, inventory_consolidated_program_logs, inventory_consolidated_programs,
    measure_dex_parser_coverage, measure_identified_program_coverage,
    prepare_completed_single_read_extension, probe_epoch_speed, replay_consolidated_spyx_balances,
    validate_completed_consolidated_dump,
};
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(about = "Extract complete SPYX transactions from Compact V2 epochs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[cfg(feature = "cpu-profile")]
    #[command(flatten)]
    cpu_profile: profiling::CpuProfileArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Discover SPYX accounts, then copy matching raw transactions into epoch shards.
    Extract(ExtractArgs),
    /// Reopen a completed single-read extraction for a larger last epoch.
    PrepareExtension(ExtractArgs),
    /// Build the canonical schema-3 stream, registry, and signature sidecar.
    Consolidate(ConsolidateArgs),
    /// Perform a separate full audit of a completed consolidated dump.
    Validate(ValidateArgs),
    /// List every top-level and inner instruction program in a consolidated dump.
    ProgramInventory(ProgramInventoryArgs),
    /// Inventory attributed transaction logs for selected programs.
    ProgramLogInventory(ProgramLogInventoryArgs),
    /// Measure exact instruction and transaction coverage for identified programs.
    ProgramCoverage(ProgramCoverageArgs),
    /// Measure exact instruction and transaction coverage of the DEX parsers.
    DexParserCoverage(DexParserCoverageArgs),
    /// Build holder, public-balance, volume, and RPC-cost statistics.
    TokenReport(TokenReportArgs),
    /// Replay committed SPYx token instructions and compare with metadata.
    SpyxReplay(SpyxReplayArgs),
    /// Measure the first extraction pass on a bounded trusted-local row range.
    Probe(ProbeArgs),
}

#[derive(Debug, Clone, Args)]
struct SourceArgs {
    /// Directory that contains `epoch-N` Compact V2 generation directories.
    archive_root: PathBuf,
    /// SPYX mint. The default is the requested SPYX deployment.
    #[arg(long, default_value = SPYX_MINT)]
    mint: String,
    /// First slot to inspect.
    #[arg(long, default_value_t = SPYX_MINT_SLOT)]
    mint_slot: u64,
    /// Required mint-creation transaction signature.
    #[arg(long, default_value = SPYX_MINT_SIGNATURE)]
    mint_signature: String,
    /// Number of Compact V2 decode workers.
    #[arg(long, default_value_t = 12)]
    workers: usize,
    /// Stop after this epoch. The default is the newest epoch directory.
    #[arg(long)]
    last_epoch: Option<u64>,
    /// Cluster identity asserted in trusted-local mode.
    #[arg(long)]
    cluster_id: Option<String>,
    /// Epoch schedule asserted in trusted-local mode.
    #[arg(long)]
    slots_per_epoch: Option<u64>,
    /// Whole-generation message grammar asserted in trusted-local mode.
    #[arg(long, value_enum)]
    wire_profile: Option<WireProfileArg>,
    /// Resume from fully validated epoch shards and the authenticated checkpoint.
    #[arg(long)]
    resume: bool,
    /// Run discovery and raw copy per epoch, with a barrier between them.
    ///
    /// Default is all-pass discovery first, then all-pass copy.
    #[arg(long)]
    epoch_barrier: bool,
    /// Read each block batch once, then run discovery and raw copy against the
    /// same retained decompressed bytes.
    #[arg(long, conflicts_with = "epoch_barrier")]
    single_read_batches: bool,
    /// Record exact account-match hints in discovery and use them during raw copy.
    #[arg(long, requires = "single_read_batches")]
    single_read_match_hints: bool,
    /// Include opaque fallback records conservatively instead of stopping.
    /// The current schema cannot registry-remap opaque records, so this mode is reserved.
    #[arg(long, hide = true)]
    allow_indeterminate: bool,
}

#[derive(Debug, Args)]
struct ExtractArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// New or empty output directory for epoch shards.
    output: PathBuf,
}

#[derive(Debug, Args)]
struct ConsolidateArgs {
    /// Compact V2 archive root used to resolve raw source references.
    archive_root: PathBuf,
    /// Extraction root that contains the `epochs` directory.
    input: PathBuf,
    /// New or empty output directory for the consolidated dump.
    output: PathBuf,
    /// Accept a changed source generation when only archive metadata was replaced.
    /// The public-key registry and signature sidecar must be unchanged.
    #[arg(long)]
    allow_metadata_generation_drift: bool,
    /// Resume from the last durable consolidation epoch checkpoint.
    #[arg(long)]
    resume: bool,
}

#[derive(Debug, Args)]
struct ValidateArgs {
    /// Completed consolidated dump directory.
    output: PathBuf,
}

#[derive(Debug, Args)]
struct ProgramInventoryArgs {
    /// Completed consolidated dump directory.
    dump: PathBuf,
    /// New JSON report file. An existing file is never replaced.
    report: PathBuf,
}

#[derive(Debug, Args)]
struct ProgramLogInventoryArgs {
    /// Completed consolidated dump directory.
    dump: PathBuf,
    /// UTF-8 file with one base58 program ID per line.
    programs: PathBuf,
    /// New JSON report file. An existing file is never replaced.
    report: PathBuf,
}

#[derive(Debug, Args)]
struct ProgramCoverageArgs {
    /// Completed consolidated dump directory.
    dump: PathBuf,
    /// UTF-8 file with one base58 program ID per line.
    identified_programs: PathBuf,
    /// New JSON report file. An existing file is never replaced.
    report: PathBuf,
}

#[derive(Debug, Args)]
struct DexParserCoverageArgs {
    /// Completed consolidated dump directory.
    dump: PathBuf,
    /// New deterministic JSON report file. An existing file is never replaced.
    report: PathBuf,
}

#[derive(Debug, Args)]
struct TokenReportArgs {
    /// Completed consolidated dump directory.
    dump: PathBuf,
    /// New JSON report file. An existing file is never replaced.
    report: PathBuf,
}

#[derive(Debug, Args)]
struct SpyxReplayArgs {
    /// Completed consolidated dump directory.
    dump: PathBuf,
    /// New JSON report file. An existing file is never replaced.
    report: PathBuf,
    /// Stop after this many canonical transactions for a non-authoritative canary.
    #[arg(long)]
    max_transactions: Option<u64>,
}

#[derive(Debug, Args)]
struct ProbeArgs {
    /// Direct path to one manifest-less trusted local `epoch-N` directory.
    epoch_path: PathBuf,
    /// Epoch number asserted for this trusted local directory.
    #[arg(long)]
    epoch: u64,
    /// Cluster identity asserted for this trusted local directory.
    #[arg(long, default_value = "mainnet-beta")]
    cluster_id: String,
    /// Number of slots in the asserted epoch schedule.
    #[arg(long, default_value_t = 432_000)]
    slots_per_epoch: u64,
    /// Exact indexed slot at which the bounded scan starts.
    #[arg(long, default_value_t = SPYX_MINT_SLOT)]
    start_slot: u64,
    /// Optional exact row expected for `--start-slot`.
    #[arg(long)]
    start_row: Option<usize>,
    /// Maximum number of indexed blocks to scan.
    #[arg(long, default_value_t = 10_000)]
    max_blocks: usize,
    /// SPYX mint. The default is the requested SPYX deployment.
    #[arg(long, default_value = SPYX_MINT)]
    mint: String,
    /// Required first signature at the start slot.
    #[arg(long, default_value = SPYX_MINT_SIGNATURE)]
    mint_signature: String,
    /// Number of Compact V2 decode workers.
    #[arg(long, default_value_t = 12)]
    workers: usize,
    /// Trusted-local hot-message wire profile.
    #[arg(long, value_enum, default_value_t = WireProfileArg::Post)]
    wire_profile: WireProfileArg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WireProfileArg {
    Post,
    Pre,
}

impl From<WireProfileArg> for ArchiveV2WireProfile {
    fn from(value: WireProfileArg) -> Self {
        match value {
            WireProfileArg::Post => Self::PostUnknownInstructionFallbacksV1,
            WireProfileArg::Pre => Self::PreUnknownInstructionFallbacksV1,
        }
    }
}

fn extract_config(source: SourceArgs, output: PathBuf) -> Result<ExtractConfig> {
    let source_mode = {
        let cluster_id = source.cluster_id.context("--cluster-id is required")?;
        let slots_per_epoch = source
            .slots_per_epoch
            .context("--slots-per-epoch is required")?;
        let wire_profile = source
            .wire_profile
            .context("--wire-profile post|pre is required")?;
        ensure!(!cluster_id.is_empty(), "--cluster-id is required");
        ensure!(slots_per_epoch != 0, "--slots-per-epoch is required");
        ExtractSourceMode::TrustedLocal {
            cluster_id,
            slots_per_epoch,
            wire_profile: wire_profile.into(),
        }
    };
    Ok(ExtractConfig {
        archive_root: source.archive_root,
        output,
        mint: source.mint,
        mint_slot: source.mint_slot,
        mint_signature: source.mint_signature,
        workers: source.workers,
        last_epoch: source.last_epoch,
        source_mode,
        resume: source.resume,
        epoch_barrier: source.epoch_barrier,
        single_read_batches: source.single_read_batches,
        single_read_match_hints: source.single_read_match_hints,
        allow_indeterminate: source.allow_indeterminate,
    })
}

enum PreparedCommand {
    Extract(ExtractConfig),
    PrepareExtension(ExtractConfig),
    Consolidate(ConsolidateConfig),
    Validate(PathBuf),
    ProgramInventory {
        dump: PathBuf,
        report: PathBuf,
    },
    ProgramLogInventory {
        dump: PathBuf,
        programs: PathBuf,
        report: PathBuf,
    },
    ProgramCoverage {
        dump: PathBuf,
        identified_programs: PathBuf,
        report: PathBuf,
    },
    DexParserCoverage {
        dump: PathBuf,
        report: PathBuf,
    },
    TokenReport {
        dump: PathBuf,
        report: PathBuf,
    },
    SpyxReplay {
        dump: PathBuf,
        report: PathBuf,
        max_transactions: Option<u64>,
    },
    Probe(ProbeConfig),
}

fn prepare_command(command: Command) -> Result<PreparedCommand> {
    match command {
        Command::Extract(args) => Ok(PreparedCommand::Extract(extract_config(
            args.source,
            args.output,
        )?)),
        Command::PrepareExtension(args) => Ok(PreparedCommand::PrepareExtension(extract_config(
            args.source,
            args.output,
        )?)),
        Command::Consolidate(args) => Ok(PreparedCommand::Consolidate(ConsolidateConfig {
            archive_root: args.archive_root,
            input: args.input,
            output: args.output,
            allow_metadata_generation_drift: args.allow_metadata_generation_drift,
            resume: args.resume,
        })),
        Command::Validate(args) => Ok(PreparedCommand::Validate(args.output)),
        Command::ProgramInventory(args) => Ok(PreparedCommand::ProgramInventory {
            dump: args.dump,
            report: args.report,
        }),
        Command::ProgramLogInventory(args) => Ok(PreparedCommand::ProgramLogInventory {
            dump: args.dump,
            programs: args.programs,
            report: args.report,
        }),
        Command::ProgramCoverage(args) => Ok(PreparedCommand::ProgramCoverage {
            dump: args.dump,
            identified_programs: args.identified_programs,
            report: args.report,
        }),
        Command::DexParserCoverage(args) => Ok(PreparedCommand::DexParserCoverage {
            dump: args.dump,
            report: args.report,
        }),
        Command::TokenReport(args) => Ok(PreparedCommand::TokenReport {
            dump: args.dump,
            report: args.report,
        }),
        Command::SpyxReplay(args) => Ok(PreparedCommand::SpyxReplay {
            dump: args.dump,
            report: args.report,
            max_transactions: args.max_transactions,
        }),
        Command::Probe(args) => Ok(PreparedCommand::Probe(ProbeConfig {
            epoch_path: args.epoch_path,
            cluster_id: args.cluster_id,
            epoch: args.epoch,
            slots_per_epoch: args.slots_per_epoch,
            start_slot: args.start_slot,
            expected_start_row: args.start_row,
            max_blocks: args.max_blocks,
            mint: args.mint,
            mint_signature: args.mint_signature,
            workers: args.workers,
            wire_profile: args.wire_profile.into(),
        })),
    }
}

fn run_command_with_program_log_inventory(
    command: PreparedCommand,
    program_log_inventory: impl FnOnce(
        &std::path::Path,
        &std::path::Path,
        &std::path::Path,
    ) -> Result<()>,
) -> Result<()> {
    match command {
        PreparedCommand::Extract(config) => extract_dump(config),
        PreparedCommand::PrepareExtension(config) => {
            prepare_completed_single_read_extension(&config)
        }
        PreparedCommand::Consolidate(config) => consolidate_dump(config),
        PreparedCommand::Validate(output) => validate_completed_consolidated_dump(&output),
        PreparedCommand::ProgramInventory { dump, report } => {
            inventory_consolidated_programs(&dump, &report)
        }
        PreparedCommand::ProgramLogInventory {
            dump,
            programs,
            report,
        } => program_log_inventory(&dump, &programs, &report),
        PreparedCommand::ProgramCoverage {
            dump,
            identified_programs,
            report,
        } => measure_identified_program_coverage(&dump, &identified_programs, &report),
        PreparedCommand::DexParserCoverage { dump, report } => {
            measure_dex_parser_coverage(&dump, &report)
        }
        PreparedCommand::TokenReport { dump, report } => {
            build_consolidated_token_history_report(&dump, &report)
        }
        PreparedCommand::SpyxReplay {
            dump,
            report,
            max_transactions,
        } => replay_consolidated_spyx_balances(&dump, &report, max_transactions),
        PreparedCommand::Probe(config) => {
            let report = probe_epoch_speed(&config)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
    }
}

fn run_command(command: PreparedCommand) -> Result<()> {
    run_command_with_program_log_inventory(command, inventory_consolidated_program_logs)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    #[cfg(feature = "cpu-profile")]
    let cpu_profile = cli.cpu_profile;
    let command = prepare_command(cli.command)?;

    #[cfg(feature = "cpu-profile")]
    {
        profiling::with_cpu_profile(&cpu_profile, || run_command(command))
    }
    #[cfg(not(feature = "cpu-profile"))]
    {
        run_command(command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract_args(arguments: &[&str]) -> Result<ExtractConfig> {
        let cli = Cli::try_parse_from(arguments)?;
        let Command::Extract(args) = cli.command else {
            anyhow::bail!("test command is not extract")
        };
        extract_config(args.source, args.output)
    }

    #[test]
    fn consolidate_resume_flag_is_forwarded() {
        let cli = Cli::try_parse_from([
            "dump",
            "consolidate",
            "/archive",
            "/raw",
            "/output",
            "--allow-metadata-generation-drift",
            "--resume",
        ])
        .unwrap();
        let PreparedCommand::Consolidate(config) = prepare_command(cli.command).unwrap() else {
            panic!("prepared command is not consolidate")
        };
        assert!(config.resume);
        assert!(config.allow_metadata_generation_drift);
    }

    #[cfg(feature = "cpu-profile")]
    fn cpu_profile_args(arguments: &[&str]) -> Result<profiling::CpuProfileArgs> {
        Ok(Cli::try_parse_from(arguments)?.cpu_profile)
    }

    #[test]
    fn trusted_local_requires_explicit_identity_and_profile() {
        let error = extract_args(&["dump", "extract", "/archive", "/output"]).unwrap_err();
        assert!(error.to_string().contains("--cluster-id"));
    }

    #[test]
    fn trusted_local_keeps_explicit_assertions() {
        let config = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--cluster-id",
            "mainnet-beta",
            "--slots-per-epoch",
            "432000",
            "--wire-profile",
            "post",
        ])
        .unwrap();
        assert_eq!(
            config.source_mode,
            ExtractSourceMode::TrustedLocal {
                cluster_id: "mainnet-beta".to_owned(),
                slots_per_epoch: 432_000,
                wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            }
        );
        assert!(!config.resume);
    }

    #[test]
    fn resume_flag_is_forwarded_to_extraction() {
        let config = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--cluster-id",
            "mainnet-beta",
            "--slots-per-epoch",
            "432000",
            "--wire-profile",
            "post",
            "--resume",
        ])
        .unwrap();
        assert!(config.resume);
    }

    #[test]
    fn spyx_defaults_use_the_true_initialize_mint_anchor() {
        let config = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--cluster-id",
            "mainnet-beta",
            "--slots-per-epoch",
            "432000",
            "--wire-profile",
            "post",
        ])
        .unwrap();
        assert_eq!(config.mint_slot, 346_066_298);
        assert_eq!(
            config.mint_signature,
            "51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R"
        );
    }

    #[test]
    fn epoch_barrier_flag_is_forwarded_to_extraction() {
        let config = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--cluster-id",
            "mainnet-beta",
            "--slots-per-epoch",
            "432000",
            "--wire-profile",
            "post",
            "--epoch-barrier",
        ])
        .unwrap();
        assert!(config.epoch_barrier);
        assert!(!config.resume);
    }

    #[test]
    fn single_read_batches_flag_is_forwarded_and_conflicts_with_epoch_barrier() {
        let config = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--single-read-batches",
            "--resume",
            "--cluster-id",
            "mainnet-beta",
            "--slots-per-epoch",
            "432000",
            "--wire-profile",
            "post",
        ])
        .unwrap();
        assert!(config.single_read_batches);
        assert!(!config.single_read_match_hints);
        assert!(config.resume);

        assert!(
            extract_args(&[
                "dump",
                "extract",
                "/archive",
                "/output",
                "--single-read-batches",
                "--epoch-barrier",
                "--cluster-id",
                "mainnet-beta",
                "--slots-per-epoch",
                "432000",
                "--wire-profile",
                "post",
            ])
            .is_err()
        );
    }

    #[test]
    fn single_read_match_hints_require_single_read_batches() {
        let config = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--single-read-batches",
            "--single-read-match-hints",
            "--cluster-id",
            "mainnet-beta",
            "--slots-per-epoch",
            "432000",
            "--wire-profile",
            "post",
        ])
        .unwrap();
        assert!(config.single_read_batches);
        assert!(config.single_read_match_hints);

        assert!(
            extract_args(&[
                "dump",
                "extract",
                "/archive",
                "/output",
                "--single-read-match-hints",
                "--cluster-id",
                "mainnet-beta",
                "--slots-per-epoch",
                "432000",
                "--wire-profile",
                "post",
            ])
            .is_err()
        );
    }

    #[test]
    fn trusted_local_extract_rejects_missing_slots() {
        let error = extract_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--wire-profile",
            "post",
            "--cluster-id",
            "mainnet-beta",
        ])
        .unwrap_err();
        assert!(error.to_string().contains("--slots-per-epoch"));
    }

    #[test]
    fn program_inventory_requires_one_dump_and_one_new_report_path() {
        let cli = Cli::try_parse_from([
            "dump",
            "program-inventory",
            "/dump",
            "/reports/programs.json",
        ])
        .unwrap();
        let Command::ProgramInventory(args) = cli.command else {
            panic!("test command is not program-inventory")
        };
        assert_eq!(args.dump, PathBuf::from("/dump"));
        assert_eq!(args.report, PathBuf::from("/reports/programs.json"));
        assert!(Cli::try_parse_from(["dump", "program-inventory", "/dump"]).is_err());
    }

    #[test]
    fn program_log_inventory_requires_dump_program_set_and_report_paths() {
        let cli = Cli::try_parse_from([
            "dump",
            "program-log-inventory",
            "/dump",
            "/inputs/programs.txt",
            "/reports/program-logs.json",
        ])
        .unwrap();
        let Command::ProgramLogInventory(args) = cli.command else {
            panic!("test command is not program-log-inventory")
        };
        assert_eq!(args.dump, PathBuf::from("/dump"));
        assert_eq!(args.programs, PathBuf::from("/inputs/programs.txt"));
        assert_eq!(args.report, PathBuf::from("/reports/program-logs.json"));
        assert!(
            Cli::try_parse_from([
                "dump",
                "program-log-inventory",
                "/dump",
                "/inputs/programs.txt"
            ])
            .is_err()
        );
    }

    #[test]
    fn program_log_inventory_prepares_all_paths() {
        let cli = Cli::try_parse_from([
            "dump",
            "program-log-inventory",
            "/dump",
            "/inputs/programs.txt",
            "/reports/program-logs.json",
        ])
        .unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        let PreparedCommand::ProgramLogInventory {
            dump,
            programs,
            report,
        } = prepared
        else {
            panic!("prepared command is not program-log-inventory")
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(programs, PathBuf::from("/inputs/programs.txt"));
        assert_eq!(report, PathBuf::from("/reports/program-logs.json"));
    }

    #[test]
    fn program_log_inventory_dispatches_all_paths() {
        let command = PreparedCommand::ProgramLogInventory {
            dump: PathBuf::from("/dump"),
            programs: PathBuf::from("/inputs/programs.txt"),
            report: PathBuf::from("/reports/program-logs.json"),
        };
        let mut called = false;
        run_command_with_program_log_inventory(command, |dump, programs, report| {
            called = true;
            assert_eq!(dump, std::path::Path::new("/dump"));
            assert_eq!(programs, std::path::Path::new("/inputs/programs.txt"));
            assert_eq!(report, std::path::Path::new("/reports/program-logs.json"));
            Ok(())
        })
        .unwrap();
        assert!(called);
    }

    #[test]
    fn program_coverage_requires_dump_identified_set_and_report_paths() {
        let cli = Cli::try_parse_from([
            "dump",
            "program-coverage",
            "/dump",
            "/inputs/identified-programs.txt",
            "/reports/coverage.json",
        ])
        .unwrap();
        let Command::ProgramCoverage(args) = cli.command else {
            panic!("test command is not program-coverage")
        };
        assert_eq!(args.dump, PathBuf::from("/dump"));
        assert_eq!(
            args.identified_programs,
            PathBuf::from("/inputs/identified-programs.txt")
        );
        assert_eq!(args.report, PathBuf::from("/reports/coverage.json"));
        assert!(
            Cli::try_parse_from([
                "dump",
                "program-coverage",
                "/dump",
                "/inputs/identified-programs.txt"
            ])
            .is_err()
        );
    }

    #[test]
    fn dex_parser_coverage_requires_one_dump_and_one_new_report_path() {
        let cli = Cli::try_parse_from([
            "dump",
            "dex-parser-coverage",
            "/dump",
            "/reports/dex-parser-coverage.json",
        ])
        .unwrap();
        let Command::DexParserCoverage(args) = cli.command else {
            panic!("test command is not dex-parser-coverage")
        };
        assert_eq!(args.dump, PathBuf::from("/dump"));
        assert_eq!(
            args.report,
            PathBuf::from("/reports/dex-parser-coverage.json")
        );
        assert!(Cli::try_parse_from(["dump", "dex-parser-coverage", "/dump"]).is_err());
    }

    #[test]
    fn token_report_requires_one_dump_and_one_new_report_path() {
        let cli = Cli::try_parse_from([
            "dump",
            "token-report",
            "/dump",
            "/reports/token-history.json",
        ])
        .unwrap();
        let Command::TokenReport(args) = cli.command else {
            panic!("test command is not token-report")
        };
        assert_eq!(args.dump, PathBuf::from("/dump"));
        assert_eq!(args.report, PathBuf::from("/reports/token-history.json"));
        assert!(Cli::try_parse_from(["dump", "token-report", "/dump"]).is_err());
    }

    #[test]
    fn spyx_replay_accepts_an_optional_canary_limit() {
        let cli = Cli::try_parse_from([
            "dump",
            "spyx-replay",
            "/dump",
            "/reports/replay.json",
            "--max-transactions",
            "25000",
        ])
        .unwrap();
        let Command::SpyxReplay(args) = cli.command else {
            panic!("test command is not spyx-replay")
        };
        assert_eq!(args.dump, PathBuf::from("/dump"));
        assert_eq!(args.report, PathBuf::from("/reports/replay.json"));
        assert_eq!(args.max_transactions, Some(25_000));
        assert!(Cli::try_parse_from(["dump", "spyx-replay", "/dump"]).is_err());
    }

    #[cfg(feature = "cpu-profile")]
    #[test]
    fn profile_cli_accepts_explicit_pair_and_bounded_window() {
        let profile = cpu_profile_args(&[
            "dump",
            "extract",
            "/archive",
            "/output",
            "--profile-flamegraph-out",
            "/profiles/epoch812.svg",
            "--profile-top-out",
            "/profiles/epoch812.top.tsv",
            "--profile-frequency",
            "29",
            "--profile-skip-seconds",
            "2400",
            "--profile-duration-seconds",
            "45",
        ])
        .unwrap();
        assert_eq!(profile.profile_frequency, 29);
        assert_eq!(profile.profile_skip_seconds, 2_400);
        assert_eq!(profile.profile_duration_seconds, 45);
    }

    #[cfg(feature = "cpu-profile")]
    #[test]
    fn profile_cli_requires_both_outputs() {
        assert!(
            cpu_profile_args(&[
                "dump",
                "extract",
                "/archive",
                "/output",
                "--profile-flamegraph-out",
                "/profiles/epoch812.svg",
            ])
            .is_err()
        );
    }

    #[cfg(not(feature = "cpu-profile"))]
    #[test]
    fn default_binary_does_not_expose_profile_options() {
        assert!(
            Cli::try_parse_from([
                "dump",
                "extract",
                "/archive",
                "/output",
                "--profile-flamegraph-out",
                "/profiles/epoch812.svg",
            ])
            .is_err()
        );
    }
}
