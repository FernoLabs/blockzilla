use std::{env, fmt, path::PathBuf, str::FromStr};

use anyhow::{Context, Result, bail};
use blockzilla_archive_v2::{
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
};
use blockzilla_user_program_index::{
    build::{
        DEFAULT_MAX_ACCOUNTS_PER_CHUNK, DEFAULT_QUEUED_RELATION_BATCHES,
        DEFAULT_RELATION_BATCH_PAIRS, DenseIndexBuildOptions, build_dense_index_from_reader,
        default_scan_threads,
    },
    query::query_user_program_index,
};
use clap::{ArgAction, ArgGroup, Args, Parser, Subcommand, ValueEnum};
use solana_pubkey::Pubkey;

use crate::{
    database::{DumpDatabase, DumpKind, DumpState, OnIndeterminate},
    scan::{DumpRunConfig, SourceOptions, prepare_epoch, run_dump},
    verify::{CheckState, VerifyRunConfig, print_human_verify_report, run_verify},
};

pub const PUMP_FUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const DEFAULT_BEARER_TOKEN_ENV: &str = "BLOCKZILLA_ARCHIVE_TOKEN";

#[derive(Debug, Parser)]
#[command(
    name = "blockzilla-dump",
    version,
    about = "Read, verify, and export pinned Blockzilla Compact V2 reader sets"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Dump transactions that invoke a program directly or through recorded CPI.
    Program(ProgramArgs),
    /// Dump transactions that have recorded pre/post token balances for a mint.
    Token(TokenArgs),
    /// Show the durable status of a dump database.
    Status(StatusArgs),
    /// Verify one Archive V2 epoch or an inclusive adjacent epoch range.
    Verify(VerifyArgs),
    /// Build or query the signer user to reached-program index.
    UserProgramIndex {
        #[command(subcommand)]
        command: UserProgramCommand,
    },
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("epoch_selection")
        .required(true)
        .multiple(false)
        .args(["epoch", "epoch_range"])
))]
pub struct VerifyArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    /// Verify one epoch. A nonzero epoch also requires epoch N-1.
    #[arg(long)]
    pub epoch: Option<u64>,
    /// Verify every adjacent epoch in the strict inclusive form A..=B.
    #[arg(long = "epoch-range", value_name = "A..=B")]
    pub epoch_range: Option<InclusiveEpochRange>,
    /// Also recompute all PoH entry hashes. Local archive mode only for now.
    #[arg(long)]
    pub poh: bool,
    /// Also verify every required Ed25519 transaction signature.
    #[arg(long)]
    pub signatures: bool,
    /// Enable both --poh and --signatures.
    #[arg(long)]
    pub all_checks: bool,
    /// PoH ticks per slot. Used only when PoH is requested.
    #[arg(long, default_value_t = 64)]
    pub poh_ticks_per_slot: u64,
    /// Trusted PoH hashes per tick. Required when PoH is requested.
    #[arg(long)]
    pub poh_hashes_per_tick: Option<u64>,
    /// Published PoH sidecar wire profile.
    #[arg(long, value_enum, default_value_t = PohSchemaArg::Current)]
    pub poh_schema: PohSchemaArg,
    /// Maximum PoH hash rounds admitted for one block.
    #[arg(long, default_value_t = crate::verify::DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK)]
    pub poh_max_hash_rounds_per_block: u64,
    /// Ordered decode workers. The default uses all logical CPUs.
    #[arg(long, default_value_t = default_threads())]
    pub threads: usize,
    /// Print the complete machine-readable verification report.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InclusiveEpochRange {
    pub start: u64,
    pub end: u64,
}

impl FromStr for InclusiveEpochRange {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (start, end) = value
            .split_once("..=")
            .ok_or_else(|| "epoch range must use the exact inclusive form A..=B".to_owned())?;
        if start.is_empty() || end.is_empty() || end.contains("..=") {
            return Err("epoch range must use the exact inclusive form A..=B".to_owned());
        }
        let start = start
            .parse::<u64>()
            .map_err(|_| format!("invalid range start {start}"))?;
        let end = end
            .parse::<u64>()
            .map_err(|_| format!("invalid range end {end}"))?;
        if start > end {
            return Err(format!(
                "epoch range start {start} is greater than end {end}"
            ));
        }
        Ok(Self { start, end })
    }
}

impl fmt::Display for InclusiveEpochRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..={}", self.start, self.end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PohSchemaArg {
    Current,
    CurrentAllZeroDerived,
    LegacyNoSignatureCount,
}

#[derive(Debug, Args)]
pub struct ProgramArgs {
    #[command(flatten)]
    pub dump: DumpArgs,
    /// Program pubkey. The default is the Pump.fun program.
    #[arg(long, default_value = PUMP_FUN_PROGRAM)]
    pub program: String,
}

#[derive(Debug, Args)]
pub struct TokenArgs {
    #[command(flatten)]
    pub dump: DumpArgs,
    /// Token mint pubkey. The default is mainnet USDC.
    #[arg(long, default_value = USDC_MINT)]
    pub mint: String,
}

#[derive(Debug, Args)]
pub struct DumpArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    /// Epoch number. Repeat the option or use comma-separated values.
    #[arg(long = "epoch", required = true, value_delimiter = ',', action = ArgAction::Append)]
    pub epochs: Vec<u64>,
    /// Output SQLite file. An existing compatible partial dump resumes.
    #[arg(long)]
    pub output: PathBuf,
    /// Ordered decode workers. The default uses all logical CPUs.
    #[arg(long, default_value_t = default_threads())]
    pub threads: usize,
    /// Action when archive data cannot prove match or no-match.
    #[arg(long, value_enum, default_value_t = OnIndeterminateArg::Fail)]
    pub on_indeterminate: OnIndeterminateArg,
}

#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("archive_source")
        .required(true)
        .multiple(false)
        .args(["archive", "gateway"])
))]
pub struct SourceArgs {
    /// One reader-set directory, or a root with epoch-N children.
    #[arg(long)]
    pub archive: Option<PathBuf>,
    /// HTTPS Archive V2 gateway base URL.
    #[arg(long)]
    pub gateway: Option<String>,
    /// Local verified control-file cache. Required with --gateway.
    #[arg(long)]
    pub cache: Option<PathBuf>,
    /// Environment variable that contains the optional gateway bearer token.
    #[arg(long, default_value = DEFAULT_BEARER_TOKEN_ENV)]
    pub bearer_token_env: String,
    /// Operator label for the local file set. Change it when files are replaced.
    #[arg(long, requires = "archive")]
    pub source_generation_prefix: Option<String>,
    /// Cluster identity used by the reader contract.
    #[arg(long, default_value = "mainnet-beta")]
    pub cluster_id: String,
    /// First slot of epoch zero in this fixed-width archive series.
    #[arg(long, default_value_t = 0)]
    pub epoch_zero_first_slot: u64,
    /// Exact slot count for each epoch in this archive series.
    #[arg(long, default_value_t = 432_000)]
    pub slots_per_epoch: u64,
    /// Exact Compact V2 message grammar for all selected epochs.
    #[arg(long, value_enum)]
    pub message_schema: MessageSchemaArg,
    /// Exact Compact V2 transaction-metadata grammar for all selected epochs.
    #[arg(long, value_enum)]
    pub metadata_schema: MetadataSchemaArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MessageSchemaArg {
    Current,
    May24,
}

impl From<MessageSchemaArg> for blockzilla_compact_v2_reader::CompactV2MessageSchema {
    fn from(value: MessageSchemaArg) -> Self {
        match value {
            MessageSchemaArg::Current => Self::Current,
            MessageSchemaArg::May24 => Self::May24PreUnknownFallbacks,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MetadataSchemaArg {
    CurrentTypedError,
    LegacyRawError,
}

impl From<MetadataSchemaArg> for blockzilla_compact_v2_reader::CompactV2MetadataSchema {
    fn from(value: MetadataSchemaArg) -> Self {
        match value {
            MetadataSchemaArg::CurrentTypedError => Self::CurrentTypedError,
            MetadataSchemaArg::LegacyRawError => Self::LegacyRawError,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OnIndeterminateArg {
    Fail,
    Record,
    Skip,
}

impl From<OnIndeterminateArg> for OnIndeterminate {
    fn from(value: OnIndeterminateArg) -> Self {
        match value {
            OnIndeterminateArg::Fail => Self::Fail,
            OnIndeterminateArg::Record => Self::Record,
            OnIndeterminateArg::Skip => Self::Skip,
        }
    }
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// SQLite dump file.
    #[arg(long)]
    pub output: PathBuf,
    /// Print machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum UserProgramCommand {
    Build(UserProgramBuildArgs),
    Query(UserProgramQueryArgs),
}

#[derive(Debug, Args)]
pub struct UserProgramBuildArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    #[arg(long)]
    pub epoch: u64,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value_t = default_threads())]
    pub threads: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_ACCOUNTS_PER_CHUNK)]
    pub shard_width: u32,
    #[arg(long, default_value_t = DEFAULT_RELATION_BATCH_PAIRS)]
    pub batch_pairs: usize,
    #[arg(long, default_value_t = DEFAULT_QUEUED_RELATION_BATCHES)]
    pub queued_batches: usize,
}

#[derive(Debug, Args)]
pub struct UserProgramQueryArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    #[arg(long)]
    pub epoch: u64,
    #[arg(long)]
    pub index: PathBuf,
    #[arg(long)]
    pub user: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandOutcome {
    pub exit_code: u8,
}

pub fn run() -> Result<CommandOutcome> {
    run_command(Cli::parse())
}

pub fn run_command(cli: Cli) -> Result<CommandOutcome> {
    match cli.command {
        Command::Program(args) => run_transaction_dump(
            args.dump,
            DumpKind::Program,
            parse_pubkey(&args.program, "program")?,
        ),
        Command::Token(args) => run_transaction_dump(
            args.dump,
            DumpKind::Token,
            parse_pubkey(&args.mint, "mint")?,
        ),
        Command::Status(args) => run_status(args),
        Command::Verify(args) => run_verify_command(args),
        Command::UserProgramIndex { command } => match command {
            UserProgramCommand::Build(args) => run_user_program_build(args),
            UserProgramCommand::Query(args) => run_user_program_query(args),
        },
    }
}

fn run_verify_command(args: VerifyArgs) -> Result<CommandOutcome> {
    let source = source_options(args.source)?;
    let (start_epoch, end_epoch) = match (args.epoch, args.epoch_range) {
        (Some(epoch), None) => (epoch, epoch),
        (None, Some(range)) => (range.start, range.end),
        _ => unreachable!("clap requires exactly one epoch selector"),
    };
    let poh_requested = args.poh || args.all_checks;
    let signatures_requested = args.signatures || args.all_checks;
    let poh_bounds = if poh_requested {
        Some(
            blockzilla_compact_v2_reader::archive_integrity::PohProtocolBounds {
                ticks_per_slot: args.poh_ticks_per_slot,
                hashes_per_tick: args
                    .poh_hashes_per_tick
                    .context("--poh-hashes-per-tick is required with --poh or --all-checks")?,
            },
        )
    } else {
        None
    };
    let result = run_verify(VerifyRunConfig {
        source,
        start_epoch,
        end_epoch,
        threads: args.threads,
        poh_requested,
        signatures_requested,
        poh_bounds,
        poh_schema: args.poh_schema.into(),
        poh_max_hash_rounds_per_block: args.poh_max_hash_rounds_per_block,
    })?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&result.report)?);
    } else {
        print_human_verify_report(&result.report);
    }
    Ok(CommandOutcome {
        exit_code: if result.report.overall == CheckState::Passed {
            0
        } else {
            1
        },
    })
}

impl From<PohSchemaArg> for blockzilla_compact_v2_reader::archive_integrity::PohSidecarSchema {
    fn from(value: PohSchemaArg) -> Self {
        match value {
            PohSchemaArg::Current => Self::Current,
            PohSchemaArg::CurrentAllZeroDerived => Self::CurrentAllZeroDerived,
            PohSchemaArg::LegacyNoSignatureCount => Self::LegacyNoSignatureCount,
        }
    }
}

fn run_transaction_dump(
    args: DumpArgs,
    kind: DumpKind,
    target_pubkey: [u8; 32],
) -> Result<CommandOutcome> {
    let source = source_options(args.source)?;
    let result = run_dump(&DumpRunConfig {
        source,
        epochs: args.epochs,
        output: args.output.clone(),
        threads: args.threads,
        on_indeterminate: args.on_indeterminate.into(),
        kind,
        target_pubkey,
    })?;
    let status = DumpDatabase::read_status(&args.output)?;
    print_human_status(&status);
    Ok(CommandOutcome {
        exit_code: if result.partial { 2 } else { 0 },
    })
}

fn run_status(args: StatusArgs) -> Result<CommandOutcome> {
    let status = DumpDatabase::read_status(&args.output)
        .with_context(|| format!("read dump status from {}", args.output.display()))?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print_human_status(&status);
    }
    Ok(CommandOutcome { exit_code: 0 })
}

fn run_user_program_build(args: UserProgramBuildArgs) -> Result<CommandOutcome> {
    let source = source_options(args.source)?;
    let prepared = prepare_epoch(&source, args.epoch)?;
    let registry = prepared.source_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let registry_index = prepared
        .source_root
        .join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    build_dense_index_from_reader(
        &prepared.archive,
        &registry,
        &registry_index,
        &args.output,
        DenseIndexBuildOptions {
            shard_width: args.shard_width,
            threads: args.threads,
            batch_pairs: args.batch_pairs,
            queued_batches: args.queued_batches,
        },
    )
    .with_context(|| format!("build user-program index for epoch {}", args.epoch))?;
    println!("built {}", args.output.display());
    Ok(CommandOutcome { exit_code: 0 })
}

fn run_user_program_query(args: UserProgramQueryArgs) -> Result<CommandOutcome> {
    let source = source_options(args.source)?;
    let prepared = prepare_epoch(&source, args.epoch)?;
    let result = query_user_program_index(&args.index, &prepared.source_root, &args.user, false)
        .with_context(|| format!("query user-program index {}", args.index.display()))?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("user: {}", result.user);
        println!("epoch: {}", result.epoch);
        println!("programs: {}", result.programs.len());
        for program in result.programs {
            println!("{program}");
        }
    }
    Ok(CommandOutcome { exit_code: 0 })
}

fn source_options(args: SourceArgs) -> Result<SourceOptions> {
    if args.gateway.is_some() && args.cache.is_none() {
        bail!("--cache is required with --gateway");
    }
    if args.archive.is_some() && args.cache.is_some() {
        bail!("--cache is valid only with --gateway");
    }
    let bearer_token = if args.gateway.is_some() {
        match env::var(&args.bearer_token_env) {
            Ok(value) if value.is_empty() => {
                bail!("{} is set but empty", args.bearer_token_env)
            }
            Ok(value) => Some(value),
            Err(env::VarError::NotPresent) => None,
            Err(error) => return Err(error).context("read bearer token environment variable"),
        }
    } else {
        None
    };
    let options = SourceOptions {
        archive: args.archive,
        gateway: args.gateway,
        bearer_token,
        cache: args.cache,
        allow_insecure_http: false,
        cluster_id: args.cluster_id,
        local_generation_prefix: args.source_generation_prefix,
        epoch_zero_first_slot: args.epoch_zero_first_slot,
        slots_per_epoch: args.slots_per_epoch,
        message_schema: args.message_schema.into(),
        metadata_schema: args.metadata_schema.into(),
    };
    options.validate()?;
    Ok(options)
}

fn parse_pubkey(value: &str, name: &str) -> Result<[u8; 32]> {
    Pubkey::from_str(value)
        .map(|pubkey| pubkey.to_bytes())
        .with_context(|| format!("invalid {name} pubkey {value}"))
}

fn print_human_status(status: &crate::database::DumpStatus) {
    println!("state: {}", status.state.as_str());
    println!("kind: {}", status.kind.as_str());
    println!("target: {}", status.target_pubkey_base58);
    println!("source: {}", status.source);
    println!("transactions: {}", status.transaction_rows);
    println!("coverage issues: {}", status.coverage_issue_rows);
    for epoch in &status.epochs {
        println!(
            "epoch {}: {}, blocks {}/{}, transactions {}, matches {}, indeterminate {}",
            epoch.epoch,
            epoch.state.as_str(),
            epoch.checkpoint.scanned_blocks,
            epoch
                .block_rows_total
                .map(|value| value.to_string())
                .unwrap_or_else(|| "?".into()),
            epoch.checkpoint.scanned_transactions,
            epoch.checkpoint.matched_transactions,
            epoch.checkpoint.indeterminate_transactions,
        );
    }
    if matches!(status.state, DumpState::CompleteWithGaps) {
        println!("result: partial; review the indeterminate count and coverage_issues table");
    }
}

pub fn default_threads() -> usize {
    default_scan_threads()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_repeated_and_comma_separated_epochs() {
        let cli = Cli::try_parse_from([
            "blockzilla-dump",
            "program",
            "--gateway",
            "https://archive.example",
            "--cache",
            "/tmp/cache",
            "--message-schema",
            "current",
            "--metadata-schema",
            "current-typed-error",
            "--epoch",
            "0,100",
            "--epoch",
            "200",
            "--output",
            "/tmp/pump.sqlite",
        ])
        .unwrap();
        let Command::Program(args) = cli.command else {
            panic!("expected program command");
        };
        assert_eq!(args.dump.epochs, [0, 100, 200]);
        assert_eq!(args.program, PUMP_FUN_PROGRAM);
        assert_eq!(args.program, "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
        assert_eq!(args.dump.threads, default_threads());
    }

    #[test]
    fn source_group_requires_exactly_one_source() {
        assert!(
            Cli::try_parse_from([
                "blockzilla-dump",
                "token",
                "--epoch",
                "1",
                "--output",
                "/tmp/usdc.sqlite",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "blockzilla-dump",
                "token",
                "--archive",
                "/archive",
                "--gateway",
                "https://archive.example",
                "--epoch",
                "1",
                "--output",
                "/tmp/usdc.sqlite",
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_nested_user_program_commands() {
        let cli = Cli::try_parse_from([
            "blockzilla-dump",
            "user-program-index",
            "query",
            "--archive",
            "/archive",
            "--source-generation-prefix",
            "test",
            "--message-schema",
            "current",
            "--metadata-schema",
            "current-typed-error",
            "--epoch",
            "7",
            "--index",
            "/index",
            "--user",
            "11111111111111111111111111111111",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::UserProgramIndex {
                command: UserProgramCommand::Query(_)
            }
        ));
    }

    #[test]
    fn verify_defaults_to_continuity_only() {
        let cli = Cli::try_parse_from([
            "blockzilla-dump",
            "verify",
            "--archive",
            "/archive",
            "--source-generation-prefix",
            "test",
            "--message-schema",
            "current",
            "--metadata-schema",
            "current-typed-error",
            "--epoch",
            "7",
        ])
        .unwrap();
        let Command::Verify(args) = cli.command else {
            panic!("expected verify command");
        };
        assert_eq!(args.epoch, Some(7));
        assert_eq!(args.epoch_range, None);
        assert!(!args.poh);
        assert!(!args.signatures);
        assert!(!args.all_checks);
        assert_eq!(args.threads, default_threads());
    }

    #[test]
    fn verify_accepts_only_strict_inclusive_ranges() {
        for invalid in ["7-9", "7..9", "9..=7", "7..=8..=9", "..=9"] {
            assert!(
                Cli::try_parse_from([
                    "blockzilla-dump",
                    "verify",
                    "--archive",
                    "/archive",
                    "--epoch-range",
                    invalid,
                ])
                .is_err(),
                "accepted invalid range {invalid}"
            );
        }
        let cli = Cli::try_parse_from([
            "blockzilla-dump",
            "verify",
            "--gateway",
            "https://archive.example",
            "--cache",
            "/tmp/cache",
            "--message-schema",
            "current",
            "--metadata-schema",
            "current-typed-error",
            "--epoch-range",
            "7..=9",
            "--all-checks",
            "--poh-hashes-per-tick",
            "62500",
        ])
        .unwrap();
        let Command::Verify(args) = cli.command else {
            panic!("expected verify command");
        };
        assert_eq!(
            args.epoch_range,
            Some(InclusiveEpochRange { start: 7, end: 9 })
        );
        assert!(args.all_checks);
    }

    #[test]
    fn verify_rejects_two_epoch_selectors() {
        assert!(
            Cli::try_parse_from([
                "blockzilla-dump",
                "verify",
                "--archive",
                "/archive",
                "--epoch",
                "7",
                "--epoch-range",
                "7..=9",
            ])
            .is_err()
        );
    }
}
