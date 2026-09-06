use std::path::PathBuf;

use anyhow::{Context, Result};
use blockzilla_firebase_indexer::{build, dense_accumulator, query};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "blockzilla-firebase-indexer",
    about = "Per-epoch reverse index: signer wallet -> programs reached by successful transactions (direct and CPI)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Build the strict successful-transaction signer -> reached-program index.
    Build {
        /// Epoch number, must match the opened archive's manifest.
        #[arg(long)]
        epoch: u64,
        /// Path to the epoch's Archive V2 Compact generation directory.
        #[arg(long)]
        archive: PathBuf,
        /// Output directory for the built index files.
        #[arg(long)]
        out: PathBuf,
        /// Open the archive without a published archive-v2-generation.json
        /// and without hashing file content, asserting its identity instead.
        /// Only for a filesystem you already trust (e.g. a local NAS).
        /// Requires --cluster-id and --generation-id.
        #[arg(long, requires_all = ["cluster_id", "generation_id"])]
        trust_local: bool,
        /// Asserted cluster identity for an unpublished trusted-local archive.
        #[arg(long, requires = "trust_local")]
        cluster_id: Option<String>,
        /// Asserted immutable generation identity for trusted-local mode.
        #[arg(long, requires = "trust_local")]
        generation_id: Option<String>,
        /// Epoch width used to validate a trusted-local archive layout.
        #[arg(long, requires = "trust_local", default_value_t = 432_000)]
        slots_per_epoch: u64,
        /// Cap on distinct account ids held in memory at once. The archive
        /// is streamed once per chunk of this many ids (a full re-decode
        /// each time), writing one shard per chunk, so peak memory stays
        /// bounded regardless of the epoch's registry size. Lower this on
        /// memory-constrained hardware; raise it (or pass the registry size)
        /// to build in a single pass when there's RAM to spare.
        #[arg(long, default_value_t = build::DEFAULT_MAX_ACCOUNTS_PER_CHUNK)]
        max_accounts_per_chunk: u32,
        /// Scan each shard with this many concurrent readers. 1 (the
        /// default) is fully sequential. Raising this multiplies that
        /// shard's peak memory by roughly the same factor — see
        /// `build::scan_into_builder_parallel`'s docs — so lower
        /// --max-accounts-per-chunk in step if memory is constrained. Only
        /// pays off if the underlying storage has spare concurrent-I/O
        /// throughput; bench with `discover-signers --threads N` first.
        #[arg(long, default_value_t = 1)]
        threads: usize,
    },
    /// Build with the two-pass signer-dense pipeline. This emits the exact
    /// same version-3 shard/query format as `build`, but scans the archive at
    /// most twice instead of once per account-id shard.
    BuildDense {
        #[arg(long)]
        epoch: u64,
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// Assert this unpublished archive is immutable and locally trusted.
        /// Requires --cluster-id and --generation-id.
        #[arg(long, requires_all = ["cluster_id", "generation_id"])]
        trust_local: bool,
        /// Asserted cluster identity for an unpublished trusted-local archive.
        #[arg(long, requires = "trust_local")]
        cluster_id: Option<String>,
        /// Asserted immutable generation identity for trusted-local mode.
        #[arg(long, requires = "trust_local")]
        generation_id: Option<String>,
        /// Epoch width used to validate a trusted-local archive layout.
        #[arg(long, requires = "trust_local", default_value_t = 432_000)]
        slots_per_epoch: u64,
        /// Reuse a `discover-signers --out` artifact. Published generations
        /// only. If omitted, pass 1 runs in this process against the same
        /// retained archive handles as pass 2.
        #[arg(long, conflicts_with = "trust_local")]
        signers: Option<PathBuf>,
        /// Registry-id width of each output shard. This affects output file
        /// layout only; it does not cause another archive scan.
        #[arg(long, default_value_t = build::DEFAULT_MAX_ACCOUNTS_PER_CHUNK)]
        shard_width: u32,
        /// Concurrent disjoint-range decoders used by both passes, or only
        /// pass 2 when --signers supplies an existing pass-1 artifact.
        #[arg(long, default_value_t = 1)]
        threads: usize,
        /// Maximum signer/program pairs in one decoder-to-accumulator batch.
        /// The single-thread direct path does not queue batches.
        #[arg(long, default_value_t = build::DEFAULT_RELATION_BATCH_PAIRS)]
        batch_pairs: usize,
        /// Maximum full relation batches waiting for the single accumulator.
        /// The single-thread direct path does not queue batches.
        #[arg(long, default_value_t = build::DEFAULT_QUEUED_RELATION_BATCHES)]
        queued_batches: usize,
    },
    /// Discover successful transactions' signer population without decoding
    /// instructions/metadata. Use it to size legacy chunks or persist the
    /// signer-rank artifact consumed by `build-dense`.
    DiscoverSigners {
        #[arg(long)]
        epoch: u64,
        #[arg(long)]
        archive: PathBuf,
        /// Assert this unpublished archive is immutable and locally trusted.
        /// Requires --cluster-id and --generation-id.
        #[arg(long, requires_all = ["cluster_id", "generation_id"])]
        trust_local: bool,
        /// Asserted cluster identity for an unpublished trusted-local archive.
        #[arg(long, requires = "trust_local")]
        cluster_id: Option<String>,
        /// Asserted immutable generation identity for trusted-local mode.
        #[arg(long, requires = "trust_local")]
        generation_id: Option<String>,
        /// Epoch width used to validate a trusted-local archive layout.
        #[arg(long, requires = "trust_local", default_value_t = 432_000)]
        slots_per_epoch: u64,
        /// Scan with this many threads, each decoding a disjoint block-row
        /// range concurrently. 1 (the default) matches `build`'s behavior
        /// today. Only pays off if the underlying storage has spare
        /// concurrent-I/O throughput beyond what one sequential reader
        /// already saturates — bench it against your actual storage before
        /// assuming a higher number helps.
        #[arg(long, default_value_t = 1)]
        threads: usize,
        /// Persist the generation-bound signer bitset and rank directory for
        /// a later `build-dense` pass. Published generations only; the path
        /// must not already exist.
        #[arg(long, conflicts_with = "trust_local")]
        out: Option<PathBuf>,
    },
    /// Look up a wallet's program-id interactions in a built index.
    Query {
        /// Base58 wallet pubkey.
        #[arg(long)]
        wallet: String,
        /// Path to an index directory produced by `build` or `build-dense`.
        #[arg(long)]
        index: PathBuf,
        /// Path to the epoch's Archive V2 Compact generation directory (the
        /// same one passed to `build --archive`). Needed to resolve between
        /// base58 pubkeys and the registry ids the index stores.
        #[arg(long)]
        archive: PathBuf,
        /// Print the result as JSON instead of plain text.
        #[arg(long)]
        json: bool,
        /// Explicitly acknowledge an index built with `build --trust-local`
        /// or `build-dense --trust-local`.
        /// The exact local registry file identity and wallet bytes are checked,
        /// but no published archive generation manifest exists to verify.
        #[arg(long)]
        trust_local: bool,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Build {
            epoch,
            archive,
            out,
            trust_local,
            cluster_id,
            generation_id,
            slots_per_epoch,
            max_accounts_per_chunk,
            threads,
        } => {
            anyhow::ensure!(threads > 0, "--threads must be greater than zero");
            anyhow::ensure!(
                threads <= build::MAX_SCAN_THREADS,
                "--threads must not exceed {}",
                build::MAX_SCAN_THREADS
            );
            let trust_local = if trust_local {
                Some(build::TrustLocal {
                    cluster_id: cluster_id
                        .context("--cluster-id is required with --trust-local")?,
                    generation_id: generation_id
                        .context("--generation-id is required with --trust-local")?,
                    slots_per_epoch,
                })
            } else {
                None
            };
            build::build_index(
                &archive,
                epoch,
                &out,
                trust_local,
                max_accounts_per_chunk,
                threads,
            )
        }
        Command::BuildDense {
            epoch,
            archive,
            out,
            trust_local,
            cluster_id,
            generation_id,
            slots_per_epoch,
            signers,
            shard_width,
            threads,
            batch_pairs,
            queued_batches,
        } => {
            anyhow::ensure!(
                !(trust_local && signers.is_some()),
                "--signers cannot be reused with --trust-local; omit it so both passes share retained handles"
            );
            let trust_local = if trust_local {
                Some(build::TrustLocal {
                    cluster_id: cluster_id
                        .context("--cluster-id is required with --trust-local")?,
                    generation_id: generation_id
                        .context("--generation-id is required with --trust-local")?,
                    slots_per_epoch,
                })
            } else {
                None
            };
            build::build_dense_index(
                &archive,
                epoch,
                &out,
                trust_local,
                signers.as_deref(),
                shard_width,
                threads,
                batch_pairs,
                queued_batches,
            )
        }
        Command::DiscoverSigners {
            epoch,
            archive,
            trust_local,
            cluster_id,
            generation_id,
            slots_per_epoch,
            threads,
            out,
        } => {
            anyhow::ensure!(threads > 0, "--threads must be greater than zero");
            anyhow::ensure!(
                threads <= build::MAX_SCAN_THREADS,
                "--threads must not exceed {}",
                build::MAX_SCAN_THREADS
            );
            anyhow::ensure!(
                !(trust_local && out.is_some()),
                "--out requires a published archive generation; trusted-local archives have no reusable content binding"
            );
            let trust_local = if trust_local {
                Some(build::TrustLocal {
                    cluster_id: cluster_id
                        .context("--cluster-id is required with --trust-local")?,
                    generation_id: generation_id
                        .context("--generation-id is required with --trust-local")?,
                    slots_per_epoch,
                })
            } else {
                None
            };
            let opened = build::open_archive(&archive, epoch, trust_local)?;
            let start = std::time::Instant::now();
            let stats = if let Some(out) = out {
                build::discover_signers_to_file(&opened, &out, threads)?
            } else if threads == 1 {
                build::discover_signers(&opened)?
            } else {
                build::discover_signers_parallel(&opened, threads)?
            };
            let elapsed = start.elapsed();
            let floor_bytes =
                u64::from(stats.registry_entries) * build::APPROX_BYTES_PER_EMPTY_SLOT;
            let registry_entries = u64::from(stats.registry_entries);
            let signer_rank_bytes = registry_entries.div_ceil(8)
                + (registry_entries.div_ceil(128) + 1) * std::mem::size_of::<u32>() as u64;
            let dense_fixed_bytes = stats.distinct_signers
                * dense_accumulator::EMPTY_SIGNER_SLOT_BYTES as u64
                + registry_entries.div_ceil(8)
                + signer_rank_bytes;
            println!("threads:               {threads}");
            println!("elapsed:               {:.3}s", elapsed.as_secs_f64());
            println!("registry_entries:      {}", stats.registry_entries);
            println!("distinct_signers:      {}", stats.distinct_signers);
            println!(
                "signers_of_registry:   {:.2}%",
                stats.distinct_signers as f64 * 100.0 / stats.registry_entries.max(1) as f64
            );
            println!("transactions_scanned:  {}", stats.transactions_scanned);
            println!(
                "failed_tx_excluded:     {}",
                stats.failed_transactions_excluded
            );
            println!("blocks_scanned:        {}", stats.blocks_scanned);
            println!(
                "legacy_builder_floor:  ~{:.1} MiB per full-registry builder (multiply by build threads; chunking divides by shard count)",
                floor_bytes as f64 / (1024.0 * 1024.0)
            );
            println!(
                "dense_builder_fixed:   ~{:.1} MiB (signer rank + heads + program bitset; each distinct relation adds {} bytes)",
                dense_fixed_bytes as f64 / (1024.0 * 1024.0),
                dense_accumulator::DISTINCT_RELATION_BYTES,
            );
            Ok(())
        }
        Command::Query {
            wallet,
            index,
            archive,
            json,
            trust_local,
        } => {
            let result = query::query_index(&index, &archive, &wallet, trust_local)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else if result.programs.is_empty() {
                println!(
                    "{} (epoch {}): no interactions found",
                    result.wallet, result.epoch
                );
            } else {
                println!("{} (epoch {}):", result.wallet, result.epoch);
                for program in &result.programs {
                    println!("  {program}");
                }
            }
            Ok(())
        }
    }
}
