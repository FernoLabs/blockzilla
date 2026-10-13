//! User-facing epoch scan. Archive discovery, cache binding, and ordered
//! decoding stay in the dedicated Compact V2 reader.

use std::{io::Write, num::NonZeroU32, path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSource, BlockSink, BlockView, CompactV2Archive, CompactV2LocalDescriptor,
    CompactV2ParallelScanConfig, QueryError, QueryResult, ScanRequest,
};
use clap::Args;
use serde::Serialize;

const DEFAULT_ORIGIN: &str = "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";

#[derive(Args, Debug)]
pub(crate) struct StreamArgs {
    #[command(flatten)]
    pub source: EpochSourceArgs,
}

#[derive(Args, Debug)]
pub(crate) struct EpochSourceArgs {
    /// Epoch to read. The public source supplies selected sample epochs.
    #[arg(long)]
    pub epoch: u64,
    /// Compatible HTTPS archive origin. Defaults to the public sample source.
    #[arg(long, env = "BLOCKZILLA_ORIGIN", conflicts_with = "archive_dir")]
    pub origin: Option<String>,
    /// Cache directory. Relative paths are resolved from the current directory.
    #[arg(long, default_value = ".blockzilla-cache")]
    pub cache_root: PathBuf,
    /// Read this local epoch directory instead of the HTTPS source.
    #[arg(long, requires = "source_id")]
    pub archive_dir: Option<PathBuf>,
    /// Stable ID for a local archive build. Change it when you replace that build.
    #[arg(long, requires = "archive_dir")]
    pub source_id: Option<String>,
    /// Decode workers (1 through 64).
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=64))]
    pub threads: u16,
    /// Read at most this many block rows. Omit to stream all admitted rows.
    #[arg(long)]
    pub max_blocks: Option<NonZeroU32>,
}

#[derive(Serialize)]
struct BlockSummary {
    epoch: u64,
    block_ordinal: u32,
    slot: u64,
    transactions: u64,
}

struct JsonSink<W> {
    output: W,
    started: Instant,
    last_report: Instant,
    expected_blocks: u64,
    blocks: u64,
    transactions: u64,
}

impl<W: Write> BlockSink for JsonSink<W> {
    fn visit_block(&mut self, block: BlockView<'_>) -> QueryResult<()> {
        let transactions = block
            .counts
            .map_or(block.transactions.len() as u64, |counts| {
                counts.transactions
            });
        let row = BlockSummary {
            epoch: block.header.epoch,
            block_ordinal: block.header.block_ordinal,
            slot: block.header.slot,
            transactions,
        };
        serde_json::to_writer(&mut self.output, &row).map_err(QueryError::sink)?;
        self.output.write_all(b"\n").map_err(QueryError::sink)?;
        // Make the completed block visible immediately to a downstream pipe.
        self.output.flush().map_err(QueryError::sink)?;
        self.blocks += 1;
        self.transactions += transactions;
        if self.last_report.elapsed().as_secs() >= 3 {
            eprintln!(
                "Read {}/{} blocks, {} transactions in {:.1}s",
                self.blocks,
                self.expected_blocks,
                self.transactions,
                self.started.elapsed().as_secs_f64(),
            );
            self.last_report = Instant::now();
        }
        Ok(())
    }
}

pub(crate) fn open(args: &EpochSourceArgs) -> Result<CompactV2Archive> {
    eprintln!("Opening epoch {}...", args.epoch);
    let archive = if let Some(root) = &args.archive_dir {
        let root = std::fs::canonicalize(&root)
            .with_context(|| format!("Cannot open archive directory {}", root.display()))?;
        let source_id = args
            .source_id
            .as_deref()
            .context("Local reads require --source-id")?;
        let descriptor = CompactV2LocalDescriptor::mainnet(args.epoch, source_id)?;
        CompactV2Archive::open_local(root, descriptor)?
    } else {
        let cache_root = if args.cache_root.is_absolute() {
            args.cache_root.clone()
        } else {
            std::env::current_dir()?.join(&args.cache_root)
        };
        let origin = args.origin.as_deref().unwrap_or(DEFAULT_ORIGIN);
        eprintln!("Source: {origin}\nPreparing archive index and key registry...");
        CompactV2Archive::open(origin, args.epoch, cache_root)
            .with_context(|| format!("Cannot open epoch {} at {origin}", args.epoch))?
    };
    Ok(archive)
}

pub(crate) fn request(archive: &CompactV2Archive, args: &EpochSourceArgs) -> Result<ScanRequest> {
    Ok(match args.max_blocks {
        Some(limit) => ScanRequest::bounded(archive.bounded_range(0, limit)?),
        None => ScanRequest::all(),
    })
}

pub(crate) fn run(args: StreamArgs) -> Result<()> {
    let started = Instant::now();
    let args = args.source;
    let mut archive = open(&args)?;
    let request = request(&archive, &args)?
        .without_instructions()
        .without_primary_signatures()
        .without_required_signers()
        .without_execution_status();
    let expected_blocks = request
        .range
        .map_or(u64::from(archive.identity().block_count), |range| {
            u64::from(range.block_count.get())
        });
    eprintln!("Streaming {expected_blocks} blocks. JSON summaries go to stdout.");
    let stdout = std::io::stdout();
    let mut sink = JsonSink {
        output: stdout.lock(),
        started,
        last_report: Instant::now(),
        expected_blocks,
        blocks: 0,
        transactions: 0,
    };
    let receipt = archive.scan_ordered_parallel(
        &request,
        &mut sink,
        CompactV2ParallelScanConfig::new(usize::from(args.threads)),
    )?;
    archive.verify_local_unchanged()?;
    anyhow::ensure!(
        receipt.scan.blocks == expected_blocks
            && receipt.scan.blocks == sink.blocks
            && receipt.scan.transactions == sink.transactions,
        "Stream counts do not match the selected archive rows",
    );
    eprintln!(
        "Stream complete: {} blocks, {} transactions in {:.1}s",
        receipt.scan.blocks,
        receipt.scan.transactions,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_model::{BlockHeader, CanonicalTransaction};

    #[test]
    fn summaries_include_empty_blocks_and_keep_one_json_record_per_line() {
        let now = Instant::now();
        let mut sink = JsonSink {
            output: Vec::new(),
            started: now,
            last_report: now,
            expected_blocks: 1,
            blocks: 0,
            transactions: 0,
        };
        let transactions: Vec<CanonicalTransaction> = Vec::new();
        sink.visit_block(BlockView {
            counts: None,
            header: BlockHeader {
                epoch: 900,
                block_ordinal: 7,
                slot: 388_800_010,
            },
            transactions: &transactions,
        })
        .unwrap();
        assert_eq!(sink.blocks, 1);
        assert_eq!(sink.transactions, 0);
        assert_eq!(
            std::str::from_utf8(&sink.output).unwrap(),
            "{\"epoch\":900,\"block_ordinal\":7,\"slot\":388800010,\"transactions\":0}\n"
        );
    }
}
