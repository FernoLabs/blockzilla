use std::{fs, path::PathBuf};

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};

mod database;
mod scan;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Order {
    Ascending,
    Descending,
}

#[derive(Debug, Parser)]
#[command(
    about = "Enumerate historical approvals to the deBridge State PDA. Candidates require a separate current account read."
)]
struct Args {
    #[arg(
        long,
        conflicts_with = "gateway",
        required_unless_present = "gateway",
        requires = "source_generation_prefix"
    )]
    archive_root: Option<PathBuf>,
    /// Compact V2 HTTP range gateway; requires strong object ETags and a cache.
    #[arg(long, conflicts_with = "archive_root", requires = "cache")]
    gateway: Option<String>,
    #[arg(long, requires = "gateway")]
    cache: Option<PathBuf>,
    /// Permit HTTP only to a literal loopback address; public gateways require HTTPS.
    #[arg(long, requires = "gateway")]
    allow_insecure_http: bool,
    #[arg(long, default_value_t = 893)]
    epoch_start: u64,
    #[arg(long)]
    epoch_end: u64,
    #[arg(long, default_value_t = 386_160_031)]
    start_slot: u64,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 2)]
    threads: usize,
    /// Stop after this many new block rows across the entire invocation.
    #[arg(long)]
    max_blocks: Option<usize>,
    /// Must be selected from a producer record, not guessed per transaction.
    #[arg(long, value_parser = ["current", "may24-pre-unknown-fallbacks", "post-unknown-instruction-fallbacks-v1", "pre-unknown-instruction-fallbacks-v1"])]
    message_schema: String,
    #[arg(long, value_parser = ["current-typed-error", "legacy-raw-error", "unmarked-historical-compatibility"])]
    metadata_schema: String,
    /// Operator label. Actual pinned file identities are also recorded.
    #[arg(long, conflicts_with = "gateway")]
    source_generation_prefix: Option<String>,
    #[arg(long, value_enum, default_value_t = Order::Descending)]
    order: Order,
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u16).range(500..=1000))]
    checkpoint_blocks: u16,
    #[arg(long)]
    candidates: Option<PathBuf>,
    #[arg(long)]
    status: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.epoch_start <= args.epoch_end,
        "epoch-start exceeds epoch-end"
    );
    ensure!(args.epoch_start >= 893, "this audit starts at epoch 893");
    ensure!(
        args.start_slot >= 386_160_031,
        "start-slot precedes the selected deployment"
    );
    ensure!(args.max_blocks != Some(0), "max-blocks must be positive");
    scan::validate_source_arguments(&args)?;
    ensure!(
        args.threads > 0
            && args.threads <= blockzilla_compact_v2_reader::MAX_ORDERED_PARALLEL_DECODE_WORKERS,
        "invalid worker count"
    );
    if let Some(parent) = args.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent).context("create database parent")?;
    }
    let mut db = database::Database::open(&args.output, args.start_slot)?;
    let result = scan::run(&args, &mut db);
    let status = match &result {
        Ok(true) => "complete",
        Ok(false) => "pilot_partial",
        Err(_) => "coverage_unknown",
    };
    let error = result.as_ref().err().map(|error| format!("{error:#}"));
    db.export(
        &args
            .candidates
            .clone()
            .unwrap_or_else(|| args.output.with_extension("candidates.txt")),
        &args
            .status
            .clone()
            .unwrap_or_else(|| args.output.with_extension("status.json")),
        args.epoch_start,
        args.epoch_end,
        status,
        error.as_deref(),
    )?;
    result.map(|_| ())
}

#[cfg(test)]
mod source_cli_tests {
    use super::*;
    fn args(source: &[&str]) -> Vec<String> {
        [
            vec![
                "audit",
                "--epoch-end",
                "1053",
                "--output",
                "audit.sqlite",
                "--message-schema",
                "post-unknown-instruction-fallbacks-v1",
                "--metadata-schema",
                "unmarked-historical-compatibility",
            ],
            source.to_vec(),
        ]
        .concat()
        .into_iter()
        .map(String::from)
        .collect()
    }
    #[test]
    fn exactly_one_source_and_gateway_cache_are_required() {
        assert!(Args::try_parse_from(args(&[])).is_err());
        assert!(Args::try_parse_from(args(&["--archive-root", "archive"])).is_err());
        assert!(Args::try_parse_from(args(&["--gateway", "https://example.test"])).is_err());
        assert!(
            Args::try_parse_from(args(&[
                "--gateway",
                "https://example.test",
                "--cache",
                "cache",
                "--archive-root",
                "archive"
            ]))
            .is_err()
        );
        let local = Args::try_parse_from(args(&[
            "--archive-root",
            "archive",
            "--source-generation-prefix",
            "source-v1",
        ]))
        .unwrap();
        assert!(scan::validate_source_arguments(&local).is_ok());
        let remote = Args::try_parse_from(args(&[
            "--gateway",
            "https://example.test",
            "--cache",
            "cache",
        ]))
        .unwrap();
        assert!(scan::validate_source_arguments(&remote).is_ok());
    }
    #[test]
    fn insecure_http_is_explicit_and_only_literal_loopback() {
        let remote = |url: &str, allow: bool| {
            let mut source = vec!["--gateway", url, "--cache", "cache"];
            if allow {
                source.push("--allow-insecure-http");
            }
            Args::try_parse_from(args(&source)).unwrap()
        };
        assert!(scan::validate_source_arguments(&remote("http://127.0.0.1:8855", true)).is_ok());
        assert!(scan::validate_source_arguments(&remote("http://127.0.0.1:8855", false)).is_err());
        assert!(scan::validate_source_arguments(&remote("http://example.test", true)).is_err());
        assert!(scan::validate_source_arguments(&remote("http://localhost:8855", true)).is_err());
        assert!(
            scan::validate_source_arguments(&remote("https://user:secret@example.test", false))
                .is_err()
        );
    }
}
