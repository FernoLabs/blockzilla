//! Streaming semantic parity gate for Firewatch signer-to-program indexes.
//!
//! The production manifest intentionally records a build timestamp and may
//! describe a different shard layout after a format-preserving optimization.
//! Comparing directory bytes therefore cannot establish semantic parity. This
//! utility validates both indexes' table geometry and compares the canonical
//! `wallet_id -> sorted unique program usage` stream without loading either
//! index into memory. With paired registry flags it resolves IDs to pubkeys
//! and external-sorts the complete usage records, allowing parity checks
//! across registry reorderings with a bounded relation-sort buffer.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_firebase_indexer::format::{
    FORMAT_VERSION, IndexFileBinding, IndexManifest, PROGRAM_USAGE_RECORD_LEN, ProgramMapReader,
    ProgramUsage, RegistryFileIdentity as ManifestFileIdentity,
};
use blockzilla_format::{ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE};
use clap::Parser;
use rustix::fs::{Mode, OFlags};
use sha2::{Digest, Sha256};

const WALLETS_MAGIC: [u8; 4] = *b"FBIW";
const RELATIONS_MAGIC: [u8; 4] = *b"FBIR";
const HEADER_LEN: u64 = 16;
const WALLET_RECORD_LEN: u64 = 16;
const PROGRAM_USAGE_PAYLOAD_LEN: usize = PROGRAM_USAGE_RECORD_LEN - 4;
const RELATION_RECORD_LEN: u64 = PROGRAM_USAGE_RECORD_LEN as u64;
const HASH_DOMAIN: &[u8] = b"firewatch-index-canonical-program-usage-v1\0";
const PUBKEY_RELATION_RECORD_LEN: usize = 32 + 32 + PROGRAM_USAGE_PAYLOAD_LEN;
const PUBKEY_HASH_DOMAIN: &[u8] = b"firewatch-index-canonical-pubkey-program-usage-v1\0";
const DEFAULT_SORT_MEMORY_MIB: u64 = 256;
const MAX_FINAL_RUNS: usize = 32;
const MERGE_FAN_IN: usize = 8;

#[derive(Debug, Clone)]
struct BoundProgramMap {
    binding: IndexFileBinding,
    count: u64,
}

impl BoundProgramMap {
    fn from_manifest(manifest: &IndexManifest) -> Self {
        Self {
            binding: manifest.program_map.clone(),
            count: manifest.program_count,
        }
    }
}
const REGISTRY_FILE_NAME: &str = "registry.bin";
const REGISTRY_CACHE_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const WALLET_WINDOW_BYTES: usize = 1024 * 1024;
const PROGRAM_CACHE_BYTES: usize = 62 * 1024 * 1024;
const PROGRAM_CACHE_PAGE_BYTES: usize = 4096;
const PROGRAM_CACHE_WAYS: usize = 4;
const PROGRAM_CACHE_SLOTS: usize = PROGRAM_CACHE_BYTES / PROGRAM_CACHE_PAGE_BYTES;
const PROGRAM_CACHE_SETS: usize = PROGRAM_CACHE_SLOTS / PROGRAM_CACHE_WAYS;
const REGISTRY_CACHE_ACCOUNTED_BYTES: usize = WALLET_WINDOW_BYTES
    + PROGRAM_CACHE_BYTES
    + PROGRAM_CACHE_SLOTS * std::mem::size_of::<u64>()
    + PROGRAM_CACHE_SETS * std::mem::size_of::<u8>();
const _: () = assert!(PROGRAM_CACHE_BYTES % PROGRAM_CACHE_PAGE_BYTES == 0);
const _: () = assert!(PROGRAM_CACHE_SLOTS % PROGRAM_CACHE_WAYS == 0);
const _: () = assert!(WALLET_WINDOW_BYTES % 32 == 0);
const _: () = assert!(PROGRAM_CACHE_PAGE_BYTES % 32 == 0);
const _: () = assert!(PROGRAM_CACHE_WAYS > 0 && PROGRAM_CACHE_WAYS <= u8::MAX as usize);
const _: () = assert!(REGISTRY_CACHE_ACCOUNTED_BYTES <= REGISTRY_CACHE_LIMIT_BYTES);
const FIREWATCH_ATTEMPT_ID_ENV: &str = "BLOCKZILLA_FIREWATCH_ATTEMPT_ID";

fn program_usage_to_le_bytes(usage: ProgramUsage) -> [u8; PROGRAM_USAGE_RECORD_LEN] {
    let mut bytes = [0u8; PROGRAM_USAGE_RECORD_LEN];
    bytes[0..4].copy_from_slice(&usage.program_id.to_le_bytes());
    bytes[4..8].copy_from_slice(&usage.direct_instruction_count.to_le_bytes());
    bytes[8..12].copy_from_slice(&usage.inner_instruction_count.to_le_bytes());
    bytes[12..16].copy_from_slice(&usage.transaction_count.to_le_bytes());
    bytes[16..24].copy_from_slice(&usage.first_seen_slot.to_le_bytes());
    bytes[24..32].copy_from_slice(&usage.last_seen_slot.to_le_bytes());
    bytes[32..40].copy_from_slice(&usage.min_block_time.to_le_bytes());
    bytes[40..48].copy_from_slice(&usage.max_block_time.to_le_bytes());
    bytes[48..52].copy_from_slice(&usage.timed_transaction_count.to_le_bytes());
    bytes
}

fn program_usage_from_le_bytes(bytes: [u8; PROGRAM_USAGE_RECORD_LEN]) -> Result<ProgramUsage> {
    let usage = ProgramUsage {
        program_id: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        direct_instruction_count: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        inner_instruction_count: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        transaction_count: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        first_seen_slot: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        last_seen_slot: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        min_block_time: i64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        max_block_time: i64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        timed_transaction_count: u32::from_le_bytes(bytes[48..52].try_into().unwrap()),
    };
    usage
        .validate()
        .with_context(|| format!("invalid program usage record {usage:?}"))?;
    Ok(usage)
}

fn usage_payload(usage: ProgramUsage) -> [u8; PROGRAM_USAGE_PAYLOAD_LEN] {
    program_usage_to_le_bytes(usage)[4..]
        .try_into()
        .expect("program usage payload has a fixed size")
}

#[derive(Debug, Parser)]
#[command(
    name = "index-parity",
    about = "Compare two Firewatch indexes by canonical wallet-to-program usage"
)]
struct Args {
    /// Report the exact set-difference cardinalities instead of failing at
    /// the first mismatch. Both indexes are still fully validated.
    #[arg(long)]
    summarize_differences: bool,

    /// Registry used by the first index. Pass either registry.bin itself or
    /// its archive directory. Must be paired with --right-registry.
    #[arg(long, requires = "right_registry")]
    left_registry: Option<PathBuf>,

    /// Registry used by the second index. Pass either registry.bin itself or
    /// its archive directory. Must be paired with --left-registry.
    #[arg(long, requires = "left_registry")]
    right_registry: Option<PathBuf>,

    /// Maximum in-memory relation-sort buffer used by registry-aware mode.
    /// A separate retained-file registry cache uses at most about 64 MiB.
    #[arg(long, default_value_t = DEFAULT_SORT_MEMORY_MIB)]
    sort_memory_mib: u64,

    /// Parent directory for the private registry-aware sort workspace.
    /// Defaults to the first index's parent directory. Budget temporary disk
    /// for up to 112 bytes per relation on each side, plus bounded merge runs.
    #[arg(long)]
    temp_dir: Option<PathBuf>,

    /// First built index directory.
    left: PathBuf,
    /// Second built index directory.
    right: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let registries = match (&args.left_registry, &args.right_registry) {
        (None, None) => None,
        (Some(left), Some(right)) => Some((left.as_path(), right.as_path())),
        _ => bail!("--left-registry and --right-registry must be provided together"),
    };
    let sort_memory_bytes = if registries.is_some() {
        sort_memory_bytes(args.sort_memory_mib)?
    } else {
        0
    };
    let temp_parent = args.temp_dir.as_deref().unwrap_or_else(|| {
        let parent = args.left.parent().unwrap_or_else(|| Path::new("."));
        if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
    });
    // Create the managed attempt workspace before long content verification so an
    // external controller can bind, pause, or cancel this exact process safely.
    let workspace = registries
        .is_some()
        .then(|| PrivateTempDir::create(temp_parent))
        .transpose()?;
    let left_manifest = IndexManifest::verify_generation(&args.left)
        .context("verify left Firewatch index generation")?;
    let right_manifest = IndexManifest::verify_generation(&args.right)
        .context("verify right Firewatch index generation")?;
    let left_program_map = BoundProgramMap::from_manifest(&left_manifest);
    let right_program_map = BoundProgramMap::from_manifest(&right_manifest);
    let archive_guards = registries
        .map(|(left_registry, right_registry)| {
            Ok::<_, anyhow::Error>((
                ArchiveBindingGuard::open(left_registry, &left_manifest)?,
                ArchiveBindingGuard::open(right_registry, &right_manifest)?,
            ))
        })
        .transpose()?;

    if args.summarize_differences {
        let summary = if let Some((left_registry, right_registry)) = registries {
            summarize_registry_differences_in_workspace(
                &args.left,
                left_registry,
                &args.right,
                right_registry,
                sort_memory_bytes,
                workspace
                    .as_ref()
                    .expect("registry workspace exists")
                    .path(),
                left_program_map,
                right_program_map,
            )?
        } else {
            summarize_differences(&args.left, &args.right)?
        };
        println!("canonical_equal={}", summary.canonical_equal());
        println!("left_wallets={}", summary.left.wallets);
        println!("right_wallets={}", summary.right.wallets);
        println!("shared_wallets={}", summary.shared_wallets);
        println!("left_only_wallets={}", summary.left_only_wallets);
        println!("right_only_wallets={}", summary.right_only_wallets);
        println!("left_relations={}", summary.left.relations);
        println!("right_relations={}", summary.right.relations);
        println!("shared_relations={}", summary.shared_relations);
        println!("left_only_relations={}", summary.left_only_relations);
        println!("right_only_relations={}", summary.right_only_relations);
        println!("left_canonical_sha256={}", summary.left.sha256);
        println!("right_canonical_sha256={}", summary.right.sha256);
        if let Some((left_guard, right_guard)) = archive_guards.as_ref() {
            left_guard.verify_unchanged()?;
            right_guard.verify_unchanged()?;
        }
        return Ok(());
    }
    let summary = if let Some((left_registry, right_registry)) = registries {
        compare_registry_indexes_in_workspace(
            &args.left,
            left_registry,
            &args.right,
            right_registry,
            sort_memory_bytes,
            workspace
                .as_ref()
                .expect("registry workspace exists")
                .path(),
            left_program_map,
            right_program_map,
        )?
    } else {
        compare_indexes(&args.left, &args.right)?
    };
    println!("canonical_equal=true");
    println!("wallets={}", summary.wallets);
    println!("relations={}", summary.relations);
    println!("canonical_sha256={}", summary.sha256);
    if let Some((left_guard, right_guard)) = archive_guards.as_ref() {
        left_guard.verify_unchanged()?;
        right_guard.verify_unchanged()?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParitySummary {
    wallets: u64,
    relations: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DifferenceSummary {
    left: ParitySummary,
    right: ParitySummary,
    shared_wallets: u64,
    left_only_wallets: u64,
    right_only_wallets: u64,
    shared_relations: u64,
    left_only_relations: u64,
    right_only_relations: u64,
}

impl DifferenceSummary {
    fn canonical_equal(&self) -> bool {
        self.left_only_wallets == 0
            && self.right_only_wallets == 0
            && self.left_only_relations == 0
            && self.right_only_relations == 0
    }
}

fn compare_indexes(left: &Path, right: &Path) -> Result<ParitySummary> {
    let mut left = CanonicalIndex::open(left)?;
    let mut right = CanonicalIndex::open(right)?;

    let mut record_index = 0u64;
    loop {
        let left_header = left.next_wallet()?;
        let right_header = right.next_wallet()?;
        match (left_header, right_header) {
            (None, None) => break,
            (Some(left_header), Some(right_header)) => {
                ensure!(
                    left_header == right_header,
                    "canonical mismatch at wallet record {record_index}: left={left_header:?}, right={right_header:?}"
                );
                for program_index in 0..left_header.program_count {
                    let left_program = left.next_usage()?;
                    let right_program = right.next_usage()?;
                    ensure!(
                        left_program == right_program,
                        "canonical usage mismatch for wallet {} at program position {program_index}: left={left_program:?}, right={right_program:?}",
                        left_header.wallet_id
                    );
                }
            }
            (Some(header), None) => {
                bail!("right index ended before left wallet record {record_index}: left={header:?}")
            }
            (None, Some(header)) => bail!(
                "left index ended before right wallet record {record_index}: right={header:?}"
            ),
        }
        record_index += 1;
    }

    let left = left.finish()?;
    let right = right.finish()?;
    ensure!(
        left == right,
        "canonical digests disagree after relation comparison: left={left:?}, right={right:?}"
    );
    Ok(left)
}

fn summarize_differences(left: &Path, right: &Path) -> Result<DifferenceSummary> {
    let mut left = CanonicalIndex::open(left)?;
    let mut right = CanonicalIndex::open(right)?;
    let mut left_header = left.next_wallet()?;
    let mut right_header = right.next_wallet()?;
    let mut summary = DifferenceSummary {
        left: empty_parity_summary(),
        right: empty_parity_summary(),
        shared_wallets: 0,
        left_only_wallets: 0,
        right_only_wallets: 0,
        shared_relations: 0,
        left_only_relations: 0,
        right_only_relations: 0,
    };

    loop {
        match (left_header, right_header) {
            (None, None) => break,
            (Some(header), None) => {
                checked_increment(&mut summary.left_only_wallets, 1, "left-only wallets")?;
                drain_programs(
                    &mut left,
                    header.program_count,
                    &mut summary.left_only_relations,
                    "left-only relations",
                )?;
                left_header = left.next_wallet()?;
            }
            (None, Some(header)) => {
                checked_increment(&mut summary.right_only_wallets, 1, "right-only wallets")?;
                drain_programs(
                    &mut right,
                    header.program_count,
                    &mut summary.right_only_relations,
                    "right-only relations",
                )?;
                right_header = right.next_wallet()?;
            }
            (Some(left_row), Some(right_row)) => {
                match left_row.wallet_id.cmp(&right_row.wallet_id) {
                    std::cmp::Ordering::Less => {
                        checked_increment(&mut summary.left_only_wallets, 1, "left-only wallets")?;
                        drain_programs(
                            &mut left,
                            left_row.program_count,
                            &mut summary.left_only_relations,
                            "left-only relations",
                        )?;
                        left_header = left.next_wallet()?;
                    }
                    std::cmp::Ordering::Greater => {
                        checked_increment(
                            &mut summary.right_only_wallets,
                            1,
                            "right-only wallets",
                        )?;
                        drain_programs(
                            &mut right,
                            right_row.program_count,
                            &mut summary.right_only_relations,
                            "right-only relations",
                        )?;
                        right_header = right.next_wallet()?;
                    }
                    std::cmp::Ordering::Equal => {
                        checked_increment(&mut summary.shared_wallets, 1, "shared wallets")?;
                        summarize_program_differences(
                            &mut left,
                            left_row.program_count,
                            &mut right,
                            right_row.program_count,
                            &mut summary,
                        )?;
                        left_header = left.next_wallet()?;
                        right_header = right.next_wallet()?;
                    }
                }
            }
        }
    }

    summary.left = left.finish()?;
    summary.right = right.finish()?;
    Ok(summary)
}

fn summarize_program_differences(
    left: &mut CanonicalIndex,
    left_count: u32,
    right: &mut CanonicalIndex,
    right_count: u32,
    summary: &mut DifferenceSummary,
) -> Result<()> {
    let mut left_remaining = left_count;
    let mut right_remaining = right_count;
    let mut left_program = take_next_program(left, &mut left_remaining)?;
    let mut right_program = take_next_program(right, &mut right_remaining)?;

    loop {
        match (left_program, right_program) {
            (None, None) => return Ok(()),
            (Some(_), None) => {
                checked_increment(&mut summary.left_only_relations, 1, "left-only relations")?;
                left_program = take_next_program(left, &mut left_remaining)?;
            }
            (None, Some(_)) => {
                checked_increment(&mut summary.right_only_relations, 1, "right-only relations")?;
                right_program = take_next_program(right, &mut right_remaining)?;
            }
            (Some(left_usage), Some(right_usage)) => match left_usage
                .program_id
                .cmp(&right_usage.program_id)
            {
                std::cmp::Ordering::Less => {
                    checked_increment(&mut summary.left_only_relations, 1, "left-only relations")?;
                    left_program = take_next_program(left, &mut left_remaining)?;
                }
                std::cmp::Ordering::Greater => {
                    checked_increment(
                        &mut summary.right_only_relations,
                        1,
                        "right-only relations",
                    )?;
                    right_program = take_next_program(right, &mut right_remaining)?;
                }
                std::cmp::Ordering::Equal => {
                    if left_usage == right_usage {
                        checked_increment(&mut summary.shared_relations, 1, "shared relations")?;
                    } else {
                        checked_increment(
                            &mut summary.left_only_relations,
                            1,
                            "left-only relations",
                        )?;
                        checked_increment(
                            &mut summary.right_only_relations,
                            1,
                            "right-only relations",
                        )?;
                    }
                    left_program = take_next_program(left, &mut left_remaining)?;
                    right_program = take_next_program(right, &mut right_remaining)?;
                }
            },
        }
    }
}

fn take_next_program(
    index: &mut CanonicalIndex,
    remaining: &mut u32,
) -> Result<Option<ProgramUsage>> {
    if *remaining == 0 {
        return Ok(None);
    }
    *remaining -= 1;
    index.next_usage().map(Some)
}

fn drain_programs(
    index: &mut CanonicalIndex,
    count: u32,
    destination: &mut u64,
    label: &'static str,
) -> Result<()> {
    for _ in 0..count {
        index.next_usage()?;
    }
    checked_increment(destination, u64::from(count), label)
}

fn checked_increment(destination: &mut u64, amount: u64, label: &'static str) -> Result<()> {
    *destination = destination
        .checked_add(amount)
        .with_context(|| format!("{label} count overflow"))?;
    Ok(())
}

fn empty_parity_summary() -> ParitySummary {
    ParitySummary {
        wallets: 0,
        relations: 0,
        sha256: String::new(),
    }
}

type PubkeyRelation = [u8; PUBKEY_RELATION_RECORD_LEN];

fn sort_memory_bytes(memory_mib: u64) -> Result<usize> {
    ensure!(
        memory_mib > 0,
        "--sort-memory-mib must be greater than zero"
    );
    let bytes = memory_mib
        .checked_mul(1024 * 1024)
        .context("--sort-memory-mib is too large")?;
    let bytes = usize::try_from(bytes).context("--sort-memory-mib exceeds this platform")?;
    ensure!(
        bytes >= PUBKEY_RELATION_RECORD_LEN,
        "--sort-memory-mib is too small for one relation"
    );
    Ok(bytes)
}

#[cfg(test)]
fn compare_registry_indexes(
    left: &Path,
    left_registry: &Path,
    right: &Path,
    right_registry: &Path,
    sort_memory_bytes: usize,
    temp_parent: &Path,
) -> Result<ParitySummary> {
    let workspace = PrivateTempDir::create(temp_parent)?;
    let left_program_map = fixture_program_map(left, left_registry)?;
    let right_program_map = fixture_program_map(right, right_registry)?;
    compare_registry_indexes_in_workspace(
        left,
        left_registry,
        right,
        right_registry,
        sort_memory_bytes,
        workspace.path(),
        left_program_map,
        right_program_map,
    )
}

fn compare_registry_indexes_in_workspace(
    left: &Path,
    left_registry: &Path,
    right: &Path,
    right_registry: &Path,
    sort_memory_bytes: usize,
    workspace: &Path,
    left_program_map: BoundProgramMap,
    right_program_map: BoundProgramMap,
) -> Result<ParitySummary> {
    let left_runs = build_pubkey_relation_runs(
        left,
        left_registry,
        workspace,
        "left",
        sort_memory_bytes,
        &left_program_map,
    )?;
    let right_runs = build_pubkey_relation_runs(
        right,
        right_registry,
        workspace,
        "right",
        sort_memory_bytes,
        &right_program_map,
    )?;
    compare_pubkey_relation_runs(&left_runs, &right_runs)
}

#[cfg(test)]
fn summarize_registry_differences(
    left: &Path,
    left_registry: &Path,
    right: &Path,
    right_registry: &Path,
    sort_memory_bytes: usize,
    temp_parent: &Path,
) -> Result<DifferenceSummary> {
    let workspace = PrivateTempDir::create(temp_parent)?;
    let left_program_map = fixture_program_map(left, left_registry)?;
    let right_program_map = fixture_program_map(right, right_registry)?;
    summarize_registry_differences_in_workspace(
        left,
        left_registry,
        right,
        right_registry,
        sort_memory_bytes,
        workspace.path(),
        left_program_map,
        right_program_map,
    )
}

fn summarize_registry_differences_in_workspace(
    left: &Path,
    left_registry: &Path,
    right: &Path,
    right_registry: &Path,
    sort_memory_bytes: usize,
    workspace: &Path,
    left_program_map: BoundProgramMap,
    right_program_map: BoundProgramMap,
) -> Result<DifferenceSummary> {
    let left_runs = build_pubkey_relation_runs(
        left,
        left_registry,
        workspace,
        "left",
        sort_memory_bytes,
        &left_program_map,
    )?;
    let right_runs = build_pubkey_relation_runs(
        right,
        right_registry,
        workspace,
        "right",
        sort_memory_bytes,
        &right_program_map,
    )?;
    summarize_pubkey_relation_runs(&left_runs, &right_runs)
}

fn build_pubkey_relation_runs(
    index_root: &Path,
    registry_path: &Path,
    temp_root: &Path,
    side: &'static str,
    sort_memory_bytes: usize,
    program_binding: &BoundProgramMap,
) -> Result<Vec<PathBuf>> {
    let program_map = ProgramMapReader::open_verified(
        index_root,
        &program_binding.binding,
        program_binding.count,
    )
    .with_context(|| format!("open bound program map at {}", index_root.display()))?;
    let mut registry = PubkeyRegistry::load(registry_path)?;
    let mut index = CanonicalIndex::open(index_root)?;
    let mut sorter = PubkeyRelationSorter::new(temp_root, side, sort_memory_bytes)?;

    while let Some(header) = index.next_wallet()? {
        let wallet = registry.resolve_wallet(header.wallet_id, index_root)?;
        for _ in 0..header.program_count {
            let usage = index.next_usage()?;
            let program_id = usage.program_id;
            let program = program_map
                .resolve(program_id)
                .with_context(|| format!("resolve bound program id {program_id}"))?;
            let registry_program = registry.resolve_program(program_id, index_root)?;
            ensure!(
                program == registry_program,
                "{} programs.map id {program_id} does not match its bound registry",
                index_root.display()
            );
            let mut relation = [0u8; PUBKEY_RELATION_RECORD_LEN];
            relation[..32].copy_from_slice(&wallet);
            relation[32..64].copy_from_slice(&program);
            relation[64..].copy_from_slice(&usage_payload(usage));
            sorter.push(relation)?;
        }
    }
    index.finish()?;
    program_map.verify_unchanged()?;
    let runs = sorter.finish()?;
    registry.verify_unchanged()?;
    Ok(runs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegistryFileIdentity {
    size: u64,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl RegistryFileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            size: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }
}

struct ArchiveBindingGuard {
    registry_path: PathBuf,
    registry_file: File,
    registry_identity: RegistryFileIdentity,
    index_path: PathBuf,
    index_file: File,
    index_identity: RegistryFileIdentity,
}

impl ArchiveBindingGuard {
    fn open(registry_argument: &Path, manifest: &IndexManifest) -> Result<Self> {
        let registry_path = if registry_argument.is_dir() {
            registry_argument.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
        } else {
            registry_argument.to_path_buf()
        };
        ensure!(
            registry_path.file_name().and_then(|name| name.to_str())
                == Some(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            "registry argument must name the archive registry.bin"
        );
        let archive = fs::canonicalize(
            registry_path
                .parent()
                .context("registry path has no archive parent")?,
        )?;
        ensure!(
            archive == Path::new(&manifest.archive_root),
            "registry argument archive {} does not match index archive {}",
            archive.display(),
            manifest.archive_root
        );
        let registry_file = open_registry_file(&registry_path)?;
        let registry_identity = RegistryFileIdentity::from_metadata(&registry_file.metadata()?);
        ensure!(
            registry_identity_matches(&registry_identity, &manifest.registry_file_identity)
                && registry_identity.size == manifest.registry.size,
            "archive registry.bin identity does not match the Firewatch index manifest"
        );
        let index_path = archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        let index_file = open_registry_file(&index_path)?;
        let index_identity = RegistryFileIdentity::from_metadata(&index_file.metadata()?);
        ensure!(
            registry_identity_matches(&index_identity, &manifest.registry_index_file_identity)
                && index_identity.size == manifest.registry_index.size,
            "archive registry.mphf identity does not match the Firewatch index manifest"
        );
        let guard = Self {
            registry_path,
            registry_file,
            registry_identity,
            index_path,
            index_file,
            index_identity,
        };
        guard.verify_unchanged()?;
        Ok(guard)
    }

    fn verify_unchanged(&self) -> Result<()> {
        verify_retained_identity(
            &self.registry_file,
            &self.registry_path,
            self.registry_identity,
        )?;
        verify_retained_identity(&self.index_file, &self.index_path, self.index_identity)
    }
}

fn registry_identity_matches(
    actual: &RegistryFileIdentity,
    expected: &ManifestFileIdentity,
) -> bool {
    actual.size == expected.size
        && actual.device == expected.device
        && actual.inode == expected.inode
        && actual.modified_seconds == expected.modified_seconds
        && actual.modified_nanoseconds == expected.modified_nanoseconds
        && actual.changed_seconds == expected.changed_seconds
        && actual.changed_nanoseconds == expected.changed_nanoseconds
}

fn verify_retained_identity(
    file: &File,
    path: &Path,
    expected: RegistryFileIdentity,
) -> Result<()> {
    ensure!(
        RegistryFileIdentity::from_metadata(&file.metadata()?) == expected,
        "retained archive file changed: {}",
        path.display()
    );
    let current = open_registry_file(path)?;
    ensure!(
        RegistryFileIdentity::from_metadata(&current.metadata()?) == expected,
        "archive path no longer names the retained file: {}",
        path.display()
    );
    Ok(())
}

struct PubkeyRegistry {
    path: PathBuf,
    file: File,
    identity: RegistryFileIdentity,
    entries: u64,
    wallet_cache: WalletWindowCache,
    program_cache: ProgramPageCache,
}

impl PubkeyRegistry {
    fn load(path: &Path) -> Result<Self> {
        let path = if path.is_dir() {
            path.join(REGISTRY_FILE_NAME)
        } else {
            path.to_path_buf()
        };
        let file = open_registry_file(&path)?;
        let metadata = file
            .metadata()
            .with_context(|| format!("fstat {}", path.display()))?;
        let identity = RegistryFileIdentity::from_metadata(&metadata);
        let size = identity.size;
        ensure!(
            size % 32 == 0,
            "{} has length {size}, not a multiple of 32",
            path.display()
        );
        let entries = size / 32;
        ensure!(
            entries <= u64::from(u32::MAX),
            "{} has {entries} entries, more than u32 ids can address",
            path.display()
        );
        let registry = Self {
            path,
            file,
            identity,
            entries,
            wallet_cache: WalletWindowCache::new()?,
            program_cache: ProgramPageCache::new()?,
        };
        // Pair the retained descriptor with the exact pathname before any
        // relation run is emitted. The same check is repeated after the run.
        registry.verify_unchanged()?;
        Ok(registry)
    }

    fn resolve_wallet(&mut self, id: u32, index_root: &Path) -> Result<[u8; 32]> {
        let offset = self.key_offset(id, "wallet", index_root)?;
        self.wallet_cache
            .resolve(&self.file, self.identity.size, offset, &self.path)
    }

    fn resolve_program(&mut self, id: u32, index_root: &Path) -> Result<[u8; 32]> {
        let offset = self.key_offset(id, "program", index_root)?;
        self.program_cache
            .resolve(&self.file, self.identity.size, offset, &self.path)
    }

    fn key_offset(&self, id: u32, kind: &str, index_root: &Path) -> Result<u64> {
        ensure!(id != 0, "{} contains {kind} id 0", index_root.display());
        let index = u64::from(id - 1);
        ensure!(
            index < self.entries,
            "{} contains {kind} id {id}, but {} has only {} entries",
            index_root.display(),
            self.path.display(),
            self.entries
        );
        Ok(index * 32)
    }

    fn verify_unchanged(&self) -> Result<()> {
        let retained_metadata = self
            .file
            .metadata()
            .with_context(|| format!("fstat retained registry {}", self.path.display()))?;
        ensure!(
            retained_metadata.is_file()
                && RegistryFileIdentity::from_metadata(&retained_metadata) == self.identity,
            "retained registry {} changed while semantic runs were built",
            self.path.display()
        );

        let path_file = open_registry_file(&self.path).with_context(|| {
            format!(
                "reopen registry path {} for identity verification",
                self.path.display()
            )
        })?;
        let path_metadata = path_file
            .metadata()
            .with_context(|| format!("fstat reopened registry {}", self.path.display()))?;
        ensure!(
            RegistryFileIdentity::from_metadata(&path_metadata) == self.identity,
            "registry path {} no longer names the retained file",
            self.path.display()
        );
        Ok(())
    }
}

fn open_registry_file(path: &Path) -> Result<File> {
    let owned = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(std::io::Error::from)
    .with_context(|| format!("safely open registry {}", path.display()))?;
    let file = File::from(owned);
    let metadata = file
        .metadata()
        .with_context(|| format!("fstat registry {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "registry {} is not a regular file",
        path.display()
    );
    Ok(file)
}

struct WalletWindowCache {
    bytes: Vec<u8>,
    start: u64,
    len: usize,
    valid: bool,
    #[cfg(test)]
    reads: u64,
}

impl WalletWindowCache {
    fn new() -> Result<Self> {
        Ok(Self {
            bytes: allocate_zeroed(WALLET_WINDOW_BYTES, "wallet registry window")?,
            start: 0,
            len: 0,
            valid: false,
            #[cfg(test)]
            reads: 0,
        })
    }

    fn resolve(
        &mut self,
        file: &File,
        file_size: u64,
        offset: u64,
        path: &Path,
    ) -> Result<[u8; 32]> {
        let end = offset
            .checked_add(32)
            .context("registry key offset overflow")?;
        ensure!(end <= file_size, "registry key exceeds {}", path.display());
        let cached_end = self.start.saturating_add(self.len as u64);
        if !self.valid || offset < self.start || end > cached_end {
            self.start = offset / WALLET_WINDOW_BYTES as u64 * WALLET_WINDOW_BYTES as u64;
            self.len = usize::try_from((file_size - self.start).min(WALLET_WINDOW_BYTES as u64))
                .expect("wallet cache window fits usize");
            file.read_exact_at(&mut self.bytes[..self.len], self.start)
                .with_context(|| {
                    format!(
                        "positioned read of wallet registry window at {} in {}",
                        self.start,
                        path.display()
                    )
                })?;
            self.valid = true;
            #[cfg(test)]
            {
                self.reads += 1;
            }
        }
        let within = usize::try_from(offset - self.start).expect("offset is inside wallet window");
        Ok(self.bytes[within..within + 32].try_into().unwrap())
    }
}

struct ProgramPageCache {
    bytes: Vec<u8>,
    tags: Vec<u64>,
    next_victim: Vec<u8>,
    #[cfg(test)]
    reads: u64,
}

impl ProgramPageCache {
    fn new() -> Result<Self> {
        let mut tags = Vec::new();
        tags.try_reserve_exact(PROGRAM_CACHE_SLOTS)
            .context("reserve program registry cache tags")?;
        tags.resize(PROGRAM_CACHE_SLOTS, u64::MAX);
        Ok(Self {
            bytes: allocate_zeroed(PROGRAM_CACHE_BYTES, "program registry page cache")?,
            tags,
            next_victim: allocate_zeroed(PROGRAM_CACHE_SETS, "program cache victim cursors")?,
            #[cfg(test)]
            reads: 0,
        })
    }

    fn resolve(
        &mut self,
        file: &File,
        file_size: u64,
        offset: u64,
        path: &Path,
    ) -> Result<[u8; 32]> {
        let end = offset
            .checked_add(32)
            .context("registry key offset overflow")?;
        ensure!(end <= file_size, "registry key exceeds {}", path.display());
        let page = offset / PROGRAM_CACHE_PAGE_BYTES as u64;
        let page_start = page * PROGRAM_CACHE_PAGE_BYTES as u64;
        let set = (page % PROGRAM_CACHE_SETS as u64) as usize;
        let first_slot = set * PROGRAM_CACHE_WAYS;
        let slot = if let Some(way) = self.tags[first_slot..first_slot + PROGRAM_CACHE_WAYS]
            .iter()
            .position(|tag| *tag == page)
        {
            first_slot + way
        } else {
            let way = usize::from(self.next_victim[set]);
            self.next_victim[set] = ((way + 1) % PROGRAM_CACHE_WAYS) as u8;
            let slot = first_slot + way;
            let cache_start = slot * PROGRAM_CACHE_PAGE_BYTES;
            let read_len =
                usize::try_from((file_size - page_start).min(PROGRAM_CACHE_PAGE_BYTES as u64))
                    .expect("program cache page fits usize");
            file.read_exact_at(
                &mut self.bytes[cache_start..cache_start + read_len],
                page_start,
            )
            .with_context(|| {
                format!(
                    "positioned read of program registry page at {page_start} in {}",
                    path.display()
                )
            })?;
            self.tags[slot] = page;
            #[cfg(test)]
            {
                self.reads += 1;
            }
            slot
        };
        let within_page = usize::try_from(offset - page_start).expect("offset is inside page");
        let cache_start = slot * PROGRAM_CACHE_PAGE_BYTES + within_page;
        Ok(self.bytes[cache_start..cache_start + 32]
            .try_into()
            .unwrap())
    }
}

fn allocate_zeroed(len: usize, label: &'static str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .with_context(|| format!("reserve {len} bytes for {label}"))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

struct PubkeyRelationSorter<'a> {
    temp_root: &'a Path,
    side: &'static str,
    chunk_capacity: usize,
    records: Vec<PubkeyRelation>,
    runs: Vec<PathBuf>,
    next_run: u64,
}

impl<'a> PubkeyRelationSorter<'a> {
    fn new(temp_root: &'a Path, side: &'static str, sort_memory_bytes: usize) -> Result<Self> {
        let chunk_capacity = sort_memory_bytes / PUBKEY_RELATION_RECORD_LEN;
        ensure!(chunk_capacity > 0, "sort memory cannot hold one relation");
        let mut records = Vec::new();
        records
            .try_reserve_exact(chunk_capacity)
            .context("reserve external-sort relation buffer")?;
        Ok(Self {
            temp_root,
            side,
            chunk_capacity,
            records,
            runs: Vec::new(),
            next_run: 0,
        })
    }

    fn push(&mut self, relation: PubkeyRelation) -> Result<()> {
        self.records.push(relation);
        if self.records.len() == self.chunk_capacity {
            self.flush_run()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        self.records.dedup();
        let path = self.next_path("run");
        let file = create_private_file(&path)?;
        let mut writer = BufWriter::new(file);
        for record in &self.records {
            writer
                .write_all(record)
                .with_context(|| format!("write {}", path.display()))?;
        }
        writer
            .flush()
            .with_context(|| format!("flush {}", path.display()))?;
        self.runs.push(path);
        self.records.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<PathBuf>> {
        self.flush_run()?;
        // The merge phase only needs small per-run readers. Release the large
        // chunk allocation before opening those files.
        self.records = Vec::new();
        while self.runs.len() > MAX_FINAL_RUNS {
            let old_runs = std::mem::take(&mut self.runs);
            let mut next_runs = Vec::with_capacity(old_runs.len().div_ceil(MERGE_FAN_IN));
            for group in old_runs.chunks(MERGE_FAN_IN) {
                if group.len() == 1 {
                    next_runs.push(group[0].clone());
                    continue;
                }
                let output = self.next_path("merge");
                merge_runs_to_file(group, &output)?;
                for input in group {
                    fs::remove_file(input)
                        .with_context(|| format!("remove merged run {}", input.display()))?;
                }
                next_runs.push(output);
            }
            self.runs = next_runs;
        }
        Ok(self.runs)
    }

    fn next_path(&mut self, phase: &str) -> PathBuf {
        let path = self
            .temp_root
            .join(format!("{}-{phase}-{:08}.bin", self.side, self.next_run));
        self.next_run += 1;
        path
    }
}

fn create_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .with_context(|| format!("create private temporary file {}", path.display()))
}

fn merge_runs_to_file(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let mut merged = MergedPubkeyRelations::open(inputs)?;
    let file = create_private_file(output)?;
    let mut writer = BufWriter::new(file);
    while let Some(record) = merged.next_unique()? {
        writer
            .write_all(&record)
            .with_context(|| format!("write merged run {}", output.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush merged run {}", output.display()))?;
    Ok(())
}

struct PrivateTempDir {
    path: PathBuf,
}

impl PrivateTempDir {
    fn create(parent: &Path) -> Result<Self> {
        let firewatch_attempt_id = firewatch_attempt_id()?;
        ensure!(
            parent.is_dir(),
            "temporary parent {} is not a directory",
            parent.display()
        );
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for _ in 0..128 {
            let sequence = NEXT_TEMP.fetch_add(1, AtomicOrdering::Relaxed);
            let temp_name = parity_temp_dir_name(
                std::process::id(),
                timestamp,
                sequence,
                firewatch_attempt_id.as_deref(),
            );
            let path = parent.join(temp_name);
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("create private temporary directory {}", path.display())
                    });
                }
            }
        }
        bail!(
            "could not create a unique temporary directory in {}",
            parent.display()
        )
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

fn parity_temp_dir_name(
    pid: u32,
    timestamp: u128,
    sequence: u64,
    firewatch_attempt_id: Option<&str>,
) -> String {
    let mut name = format!(".index-parity-{pid}-{timestamp:x}-{sequence:x}.tmp");
    if let Some(firewatch_attempt_id) = firewatch_attempt_id {
        name.push('-');
        name.push_str(firewatch_attempt_id);
    }
    name
}

fn firewatch_attempt_id() -> Result<Option<String>> {
    validate_firewatch_attempt_id(std::env::var_os(FIREWATCH_ATTEMPT_ID_ENV))
}

fn validate_firewatch_attempt_id(value: Option<OsString>) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{FIREWATCH_ATTEMPT_ID_ENV} must be valid UTF-8"))?;
    ensure!(
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{FIREWATCH_ATTEMPT_ID_ENV} must be exactly 32 lowercase hexadecimal characters"
    );
    Ok(Some(value))
}

impl Drop for PrivateTempDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "warning: failed to remove temporary parity directory {}: {error}",
                self.path.display()
            );
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct HeapRelation {
    record: PubkeyRelation,
    run: usize,
}

impl Ord for HeapRelation {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .cmp(&self.record)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapRelation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct PubkeyRunReader {
    path: PathBuf,
    reader: BufReader<File>,
    remaining: u64,
}

impl PubkeyRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open run {}", path.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("stat run {}", path.display()))?
            .len();
        ensure!(
            size % PUBKEY_RELATION_RECORD_LEN as u64 == 0,
            "temporary run {} is truncated",
            path.display()
        );
        Ok(Self {
            path: path.to_path_buf(),
            reader: BufReader::new(file),
            remaining: size / PUBKEY_RELATION_RECORD_LEN as u64,
        })
    }

    fn next_record(&mut self) -> Result<Option<PubkeyRelation>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut record = [0u8; PUBKEY_RELATION_RECORD_LEN];
        self.reader
            .read_exact(&mut record)
            .with_context(|| format!("read run {}", self.path.display()))?;
        self.remaining -= 1;
        Ok(Some(record))
    }
}

struct MergedPubkeyRelations {
    readers: Vec<PubkeyRunReader>,
    heap: BinaryHeap<HeapRelation>,
    previous: Option<PubkeyRelation>,
}

impl MergedPubkeyRelations {
    fn open(paths: &[PathBuf]) -> Result<Self> {
        let mut readers = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::with_capacity(paths.len());
        for (run, path) in paths.iter().enumerate() {
            let mut reader = PubkeyRunReader::open(path)?;
            if let Some(record) = reader.next_record()? {
                heap.push(HeapRelation { record, run });
            }
            readers.push(reader);
        }
        Ok(Self {
            readers,
            heap,
            previous: None,
        })
    }

    fn next_unique(&mut self) -> Result<Option<PubkeyRelation>> {
        while let Some(entry) = self.heap.pop() {
            if let Some(record) = self.readers[entry.run].next_record()? {
                self.heap.push(HeapRelation {
                    record,
                    run: entry.run,
                });
            }
            if self.previous.as_ref() == Some(&entry.record) {
                continue;
            }
            self.previous = Some(entry.record);
            return Ok(Some(entry.record));
        }
        Ok(None)
    }
}

struct TrackedPubkeyRelations {
    merged: MergedPubkeyRelations,
    current: Option<PubkeyRelation>,
    previous_wallet: Option<[u8; 32]>,
    wallets: u64,
    relations: u64,
    hasher: Sha256,
}

impl TrackedPubkeyRelations {
    fn open(paths: &[PathBuf]) -> Result<Self> {
        let mut merged = MergedPubkeyRelations::open(paths)?;
        let current = merged.next_unique()?;
        let mut hasher = Sha256::new();
        hasher.update(PUBKEY_HASH_DOMAIN);
        Ok(Self {
            merged,
            current,
            previous_wallet: None,
            wallets: 0,
            relations: 0,
            hasher,
        })
    }

    fn current(&self) -> Option<&PubkeyRelation> {
        self.current.as_ref()
    }

    fn consume(&mut self) -> Result<PubkeyRelation> {
        let record = self
            .current
            .take()
            .context("internal parity reader error: consumed an exhausted relation stream")?;
        let wallet: [u8; 32] = record[..32].try_into().unwrap();
        if self.previous_wallet.as_ref() != Some(&wallet) {
            self.wallets = self
                .wallets
                .checked_add(1)
                .context("wallet count overflow")?;
            self.previous_wallet = Some(wallet);
        }
        self.relations = self
            .relations
            .checked_add(1)
            .context("relation count overflow")?;
        self.hasher.update(record);
        self.current = self.merged.next_unique()?;
        Ok(record)
    }

    fn finish(mut self) -> Result<ParitySummary> {
        ensure!(
            self.current.is_none(),
            "internal parity reader error: relation stream has unread records"
        );
        self.hasher.update(self.wallets.to_le_bytes());
        self.hasher.update(self.relations.to_le_bytes());
        let digest = self.hasher.finalize();
        Ok(ParitySummary {
            wallets: self.wallets,
            relations: self.relations,
            sha256: hex_lower(&digest),
        })
    }
}

fn compare_pubkey_relation_runs(
    left_paths: &[PathBuf],
    right_paths: &[PathBuf],
) -> Result<ParitySummary> {
    let mut left = TrackedPubkeyRelations::open(left_paths)?;
    let mut right = TrackedPubkeyRelations::open(right_paths)?;
    let mut relation_index = 0u64;
    loop {
        match (left.current(), right.current()) {
            (None, None) => break,
            (Some(left_record), Some(right_record)) => {
                ensure!(
                    left_record == right_record,
                    "canonical pubkey mismatch at relation {relation_index}: left={}, right={}",
                    display_pubkey_relation(left_record),
                    display_pubkey_relation(right_record)
                );
                left.consume()?;
                right.consume()?;
            }
            (Some(left_record), None) => bail!(
                "right index ended before left pubkey relation {relation_index}: left={}",
                display_pubkey_relation(left_record)
            ),
            (None, Some(right_record)) => bail!(
                "left index ended before right pubkey relation {relation_index}: right={}",
                display_pubkey_relation(right_record)
            ),
        }
        relation_index += 1;
    }
    let left = left.finish()?;
    let right = right.finish()?;
    ensure!(
        left == right,
        "canonical pubkey digests disagree after relation comparison: left={left:?}, right={right:?}"
    );
    Ok(left)
}

fn summarize_pubkey_relation_runs(
    left_paths: &[PathBuf],
    right_paths: &[PathBuf],
) -> Result<DifferenceSummary> {
    let mut left = TrackedPubkeyRelations::open(left_paths)?;
    let mut right = TrackedPubkeyRelations::open(right_paths)?;
    let mut shared_wallets = 0u64;
    let mut left_only_wallets = 0u64;
    let mut right_only_wallets = 0u64;
    let mut shared_relations = 0u64;
    let mut left_only_relations = 0u64;
    let mut right_only_relations = 0u64;

    loop {
        let left_wallet = left.current().map(relation_wallet);
        let right_wallet = right.current().map(relation_wallet);
        match (left_wallet, right_wallet) {
            (None, None) => break,
            (Some(wallet), None) => {
                checked_increment(&mut left_only_wallets, 1, "left-only wallets")?;
                drain_pubkey_wallet(&mut left, wallet, &mut left_only_relations)?;
            }
            (None, Some(wallet)) => {
                checked_increment(&mut right_only_wallets, 1, "right-only wallets")?;
                drain_pubkey_wallet(&mut right, wallet, &mut right_only_relations)?;
            }
            (Some(left_wallet), Some(right_wallet)) => match left_wallet.cmp(&right_wallet) {
                Ordering::Less => {
                    checked_increment(&mut left_only_wallets, 1, "left-only wallets")?;
                    drain_pubkey_wallet(&mut left, left_wallet, &mut left_only_relations)?;
                }
                Ordering::Greater => {
                    checked_increment(&mut right_only_wallets, 1, "right-only wallets")?;
                    drain_pubkey_wallet(&mut right, right_wallet, &mut right_only_relations)?;
                }
                Ordering::Equal => {
                    checked_increment(&mut shared_wallets, 1, "shared wallets")?;
                    summarize_pubkey_programs(
                        &mut left,
                        &mut right,
                        left_wallet,
                        &mut shared_relations,
                        &mut left_only_relations,
                        &mut right_only_relations,
                    )?;
                }
            },
        }
    }

    Ok(DifferenceSummary {
        left: left.finish()?,
        right: right.finish()?,
        shared_wallets,
        left_only_wallets,
        right_only_wallets,
        shared_relations,
        left_only_relations,
        right_only_relations,
    })
}

fn drain_pubkey_wallet(
    stream: &mut TrackedPubkeyRelations,
    wallet: [u8; 32],
    relation_count: &mut u64,
) -> Result<()> {
    while stream
        .current()
        .is_some_and(|record| relation_wallet(record) == wallet)
    {
        stream.consume()?;
        checked_increment(relation_count, 1, "pubkey relation difference")?;
    }
    Ok(())
}

fn summarize_pubkey_programs(
    left: &mut TrackedPubkeyRelations,
    right: &mut TrackedPubkeyRelations,
    wallet: [u8; 32],
    shared_relations: &mut u64,
    left_only_relations: &mut u64,
    right_only_relations: &mut u64,
) -> Result<()> {
    loop {
        let left_program = left
            .current()
            .filter(|record| relation_wallet(record) == wallet)
            .map(relation_program);
        let right_program = right
            .current()
            .filter(|record| relation_wallet(record) == wallet)
            .map(relation_program);
        match (left_program, right_program) {
            (None, None) => return Ok(()),
            (Some(_), None) => {
                left.consume()?;
                checked_increment(left_only_relations, 1, "left-only relations")?;
            }
            (None, Some(_)) => {
                right.consume()?;
                checked_increment(right_only_relations, 1, "right-only relations")?;
            }
            (Some(left_program), Some(right_program)) => match left_program.cmp(&right_program) {
                Ordering::Less => {
                    left.consume()?;
                    checked_increment(left_only_relations, 1, "left-only relations")?;
                }
                Ordering::Greater => {
                    right.consume()?;
                    checked_increment(right_only_relations, 1, "right-only relations")?;
                }
                Ordering::Equal => {
                    if left.current() == right.current() {
                        left.consume()?;
                        right.consume()?;
                        checked_increment(shared_relations, 1, "shared relations")?;
                    } else {
                        // A wallet/program pair is shared only when its complete
                        // count and timing aggregate is equal. Represent one
                        // changed aggregate as one removal plus one addition.
                        left.consume()?;
                        right.consume()?;
                        checked_increment(left_only_relations, 1, "left-only relations")?;
                        checked_increment(right_only_relations, 1, "right-only relations")?;
                    }
                }
            },
        }
    }
}

fn relation_wallet(record: &PubkeyRelation) -> [u8; 32] {
    record[..32].try_into().unwrap()
}

fn relation_program(record: &PubkeyRelation) -> [u8; 32] {
    record[32..64].try_into().unwrap()
}

fn relation_usage_payload(record: &PubkeyRelation) -> [u8; PROGRAM_USAGE_PAYLOAD_LEN] {
    record[64..].try_into().unwrap()
}

fn display_usage_payload(payload: [u8; PROGRAM_USAGE_PAYLOAD_LEN]) -> String {
    let direct_instruction_count = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let inner_instruction_count = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let transaction_count = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    let first_seen_slot = u64::from_le_bytes(payload[12..20].try_into().unwrap());
    let last_seen_slot = u64::from_le_bytes(payload[20..28].try_into().unwrap());
    let min_block_time = i64::from_le_bytes(payload[28..36].try_into().unwrap());
    let max_block_time = i64::from_le_bytes(payload[36..44].try_into().unwrap());
    let timed_transaction_count = u32::from_le_bytes(payload[44..48].try_into().unwrap());
    format!(
        "direct_instructions={direct_instruction_count}, inner_instructions={inner_instruction_count}, transactions={transaction_count}, slots={first_seen_slot}..={last_seen_slot}, block_times={min_block_time}..={max_block_time}, timed_transactions={timed_transaction_count}"
    )
}

fn display_pubkey_relation(record: &PubkeyRelation) -> String {
    format!(
        "wallet={}, program={}, {}",
        bs58::encode(&record[..32]).into_string(),
        bs58::encode(&record[32..64]).into_string(),
        display_usage_payload(relation_usage_payload(record))
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalletHeader {
    wallet_id: u32,
    program_count: u32,
}

struct CanonicalIndex {
    root: PathBuf,
    shard_paths: Vec<PathBuf>,
    next_shard: usize,
    shard: Option<ShardReader>,
    previous_wallet: Option<u32>,
    pending_programs: u32,
    previous_program: Option<u32>,
    wallets: u64,
    relations: u64,
    hasher: Sha256,
}

impl CanonicalIndex {
    fn open(root: &Path) -> Result<Self> {
        ensure!(
            root.is_dir(),
            "{} is not an index directory",
            root.display()
        );
        let shard_paths = shard_paths(root)?;
        ensure!(
            !shard_paths.is_empty(),
            "{} has no shard-N directories",
            root.display()
        );
        let mut hasher = Sha256::new();
        hasher.update(HASH_DOMAIN);
        Ok(Self {
            root: root.to_path_buf(),
            shard_paths,
            next_shard: 0,
            shard: None,
            previous_wallet: None,
            pending_programs: 0,
            previous_program: None,
            wallets: 0,
            relations: 0,
            hasher,
        })
    }

    fn next_wallet(&mut self) -> Result<Option<WalletHeader>> {
        ensure!(
            self.pending_programs == 0,
            "internal parity reader error: next wallet requested with {} unread programs",
            self.pending_programs
        );

        loop {
            if self.shard.is_none() {
                let Some(path) = self.shard_paths.get(self.next_shard) else {
                    return Ok(None);
                };
                self.shard = Some(ShardReader::open(path)?);
                self.next_shard += 1;
            }

            let shard = self.shard.as_mut().expect("shard opened above");
            let Some(header) = shard.next_wallet()? else {
                shard.finish()?;
                self.shard = None;
                continue;
            };

            ensure!(
                self.previous_wallet
                    .is_none_or(|previous| header.wallet_id > previous),
                "{} is not globally sorted by wallet id: {} follows {:?}",
                self.root.display(),
                header.wallet_id,
                self.previous_wallet
            );
            self.previous_wallet = Some(header.wallet_id);
            self.pending_programs = header.program_count;
            self.previous_program = None;
            self.wallets = self
                .wallets
                .checked_add(1)
                .context("wallet count overflow")?;
            self.hasher.update(header.wallet_id.to_le_bytes());
            self.hasher.update(header.program_count.to_le_bytes());
            return Ok(Some(header));
        }
    }

    fn next_usage(&mut self) -> Result<ProgramUsage> {
        ensure!(
            self.pending_programs > 0,
            "internal parity reader error: usage requested outside a wallet record"
        );
        let usage = self
            .shard
            .as_mut()
            .context("internal parity reader error: missing shard")?
            .next_usage()?;
        let program = usage.program_id;
        ensure!(
            self.previous_program
                .is_none_or(|previous| program > previous),
            "{} contains a non-sorted or duplicate program id {} after {:?} for wallet {:?}",
            self.root.display(),
            program,
            self.previous_program,
            self.previous_wallet
        );
        self.previous_program = Some(program);
        self.pending_programs -= 1;
        self.relations = self
            .relations
            .checked_add(1)
            .context("relation count overflow")?;
        self.hasher.update(program_usage_to_le_bytes(usage));
        Ok(usage)
    }

    fn finish(mut self) -> Result<ParitySummary> {
        ensure!(
            self.pending_programs == 0,
            "index ended within a program list"
        );
        ensure!(
            self.next_wallet()?.is_none(),
            "index has unread wallet rows"
        );
        self.hasher.update(self.wallets.to_le_bytes());
        self.hasher.update(self.relations.to_le_bytes());
        let digest = self.hasher.finalize();
        Ok(ParitySummary {
            wallets: self.wallets,
            relations: self.relations,
            sha256: hex_lower(&digest),
        })
    }
}

struct ShardReader {
    path: PathBuf,
    wallets: BufReader<File>,
    relations: BufReader<File>,
    wallet_rows_remaining: u64,
    relation_rows: u64,
    relation_cursor: u64,
    pending_programs: u32,
}

impl ShardReader {
    fn open(path: &Path) -> Result<Self> {
        let wallets_path = path.join("wallets.idx");
        let relations_path = path.join("programs.rel");
        let (wallets, wallet_rows, wallets_version) = open_table(
            &wallets_path,
            WALLETS_MAGIC,
            WALLET_RECORD_LEN,
            "wallets.idx",
        )?;
        let (relations, relation_rows, relations_version) = open_table(
            &relations_path,
            RELATIONS_MAGIC,
            RELATION_RECORD_LEN,
            "programs.rel",
        )?;
        ensure!(
            wallets_version == relations_version,
            "{} mixes table format versions: wallets.idx={wallets_version}, programs.rel={relations_version}",
            path.display()
        );
        Ok(Self {
            path: path.to_path_buf(),
            wallets,
            relations,
            wallet_rows_remaining: wallet_rows,
            relation_rows,
            relation_cursor: 0,
            pending_programs: 0,
        })
    }

    fn next_wallet(&mut self) -> Result<Option<WalletHeader>> {
        ensure!(
            self.pending_programs == 0,
            "{} requested a wallet with unread relations",
            self.path.display()
        );
        if self.wallet_rows_remaining == 0 {
            return Ok(None);
        }
        let mut record = [0u8; WALLET_RECORD_LEN as usize];
        self.wallets
            .read_exact(&mut record)
            .with_context(|| format!("read wallet row from {}", self.path.display()))?;
        self.wallet_rows_remaining -= 1;

        let wallet_id = u32::from_le_bytes(record[0..4].try_into().unwrap());
        let offset = u64::from_le_bytes(record[4..12].try_into().unwrap());
        let program_count = u32::from_le_bytes(record[12..16].try_into().unwrap());
        ensure!(
            wallet_id != 0,
            "{} contains wallet id 0",
            self.path.display()
        );
        ensure!(
            program_count != 0,
            "{} contains an empty wallet row",
            self.path.display()
        );
        ensure!(
            offset == self.relation_cursor,
            "{} has non-canonical relation offset {offset}; expected {}",
            self.path.display(),
            self.relation_cursor
        );
        let end = offset
            .checked_add(u64::from(program_count))
            .context("relation range overflow")?;
        ensure!(
            end <= self.relation_rows,
            "{} wallet {wallet_id} references relations {offset}..{end}, but only {} exist",
            self.path.display(),
            self.relation_rows
        );
        self.pending_programs = program_count;
        Ok(Some(WalletHeader {
            wallet_id,
            program_count,
        }))
    }

    fn next_usage(&mut self) -> Result<ProgramUsage> {
        ensure!(
            self.pending_programs > 0,
            "program usage requested outside a wallet row"
        );
        let mut bytes = [0u8; PROGRAM_USAGE_RECORD_LEN];
        self.relations
            .read_exact(&mut bytes)
            .with_context(|| format!("read relation from {}", self.path.display()))?;
        self.pending_programs -= 1;
        self.relation_cursor += 1;
        program_usage_from_le_bytes(bytes).with_context(|| {
            format!(
                "decode relation {} from {}",
                self.relation_cursor - 1,
                self.path.display()
            )
        })
    }

    fn finish(&mut self) -> Result<()> {
        ensure!(
            self.pending_programs == 0,
            "shard ended within a program list"
        );
        ensure!(
            self.wallet_rows_remaining == 0,
            "shard has unread wallet rows"
        );
        ensure!(
            self.relation_cursor == self.relation_rows,
            "{} has {} unreferenced trailing relations",
            self.path.display(),
            self.relation_rows - self.relation_cursor
        );
        Ok(())
    }
}

fn open_table(
    path: &Path,
    magic: [u8; 4],
    record_len: u64,
    kind: &str,
) -> Result<(BufReader<File>, u64, u32)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let size = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    ensure!(size >= HEADER_LEN, "{} is truncated", path.display());
    let mut reader = BufReader::new(file);
    let mut header = [0u8; HEADER_LEN as usize];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read {kind} header at {}", path.display()))?;
    ensure!(
        header[0..4] == magic,
        "{} has bad {kind} magic",
        path.display()
    );
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    ensure!(
        version == FORMAT_VERSION,
        "{} has unsupported {kind} format version {version}",
        path.display()
    );
    let rows = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let expected = HEADER_LEN
        .checked_add(
            rows.checked_mul(record_len)
                .context("table size overflow")?,
        )
        .context("table size overflow")?;
    ensure!(
        size == expected,
        "{} has length {size}; expected {expected} for {rows} rows",
        path.display()
    );
    Ok((reader, rows, version))
}

fn shard_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut shards = Vec::<(u32, PathBuf)>::new();
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", root.display()))?;
        if !entry
            .file_type()
            .with_context(|| format!("stat {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(suffix) = name.strip_prefix("shard-") else {
            continue;
        };
        let shard = suffix
            .parse::<u32>()
            .with_context(|| format!("invalid shard directory name {name}"))?;
        shards.push((shard, entry.path()));
    }
    shards.sort_unstable_by_key(|(shard, _)| *shard);
    for (expected, (found, _)) in (0u32..).zip(&shards) {
        ensure!(
            expected == *found,
            "{} has noncontiguous shard numbering: expected shard-{expected}, found shard-{found}",
            root.display()
        );
    }
    Ok(shards.into_iter().map(|(_, path)| path).collect())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn firewatch_attempt_id_validation_is_strict() {
        assert_eq!(validate_firewatch_attempt_id(None).unwrap(), None);
        assert_eq!(
            validate_firewatch_attempt_id(Some(OsString::from("0123456789abcdef0123456789abcdef")))
                .unwrap(),
            Some("0123456789abcdef0123456789abcdef".to_string())
        );

        for invalid in [
            "",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "0123456789ABCDEF0123456789ABCDEF",
            "0123456789abcdef0123456789abcdeg",
        ] {
            assert!(
                validate_firewatch_attempt_id(Some(OsString::from(invalid))).is_err(),
                "accepted invalid attempt id {invalid:?}"
            );
        }
    }

    #[test]
    fn parity_temp_name_preserves_legacy_form_and_ends_with_attempt_id() {
        assert_eq!(
            parity_temp_dir_name(12, 0x34, 0x5, None),
            ".index-parity-12-34-5.tmp"
        );
        assert_eq!(
            parity_temp_dir_name(12, 0x34, 0x5, Some("0123456789abcdef0123456789abcdef")),
            ".index-parity-12-34-5.tmp-0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn canonical_comparison_ignores_shard_layout() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        write_shard(left.path(), 0, &[(2, &[3, 9]), (7, &[4])]);
        write_shard(right.path(), 0, &[(2, &[3, 9])]);
        write_shard(right.path(), 1, &[(7, &[4])]);

        let summary = compare_indexes(left.path(), right.path()).unwrap();
        assert_eq!(summary.wallets, 2);
        assert_eq!(summary.relations, 3);
        assert_eq!(summary.sha256.len(), 64);
    }

    #[test]
    fn canonical_comparison_reports_the_exact_program_mismatch() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        write_shard(left.path(), 0, &[(2, &[3, 9])]);
        write_shard(right.path(), 0, &[(2, &[3, 10])]);

        let error = compare_indexes(left.path(), right.path()).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("wallet 2"));
        assert!(message.contains("program position 1"));
    }

    #[test]
    fn canonical_comparison_binds_every_usage_metric() {
        let baseline = detailed_usage(3);
        let mut variants = Vec::new();
        variants.push(ProgramUsage {
            direct_instruction_count: baseline.direct_instruction_count + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            inner_instruction_count: baseline.inner_instruction_count + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            transaction_count: baseline.transaction_count + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            first_seen_slot: baseline.first_seen_slot + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            last_seen_slot: baseline.last_seen_slot + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            min_block_time: baseline.min_block_time + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            max_block_time: baseline.max_block_time + 1,
            ..baseline
        });
        variants.push(ProgramUsage {
            timed_transaction_count: baseline.timed_transaction_count + 1,
            ..baseline
        });

        for variant in variants {
            variant.validate().unwrap();
            let left = TempDir::new().unwrap();
            let right = TempDir::new().unwrap();
            write_usage_shard(left.path(), 0, &[(2, vec![baseline])]);
            write_usage_shard(right.path(), 0, &[(2, vec![variant])]);

            let error = compare_indexes(left.path(), right.path()).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("canonical usage mismatch"), "{message}");
        }
    }

    #[test]
    fn difference_summary_counts_changed_usage_as_remove_and_add() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        let baseline = detailed_usage(3);
        let changed = ProgramUsage {
            inner_instruction_count: baseline.inner_instruction_count + 1,
            ..baseline
        };
        write_usage_shard(left.path(), 0, &[(2, vec![baseline])]);
        write_usage_shard(right.path(), 0, &[(2, vec![changed])]);

        let summary = summarize_differences(left.path(), right.path()).unwrap();
        assert_eq!(summary.shared_wallets, 1);
        assert_eq!(summary.shared_relations, 0);
        assert_eq!(summary.left_only_relations, 1);
        assert_eq!(summary.right_only_relations, 1);
    }

    #[test]
    fn clean_v4_reader_rejects_v3_tables() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        write_shard(left.path(), 0, &[(2, &[3])]);
        write_shard(right.path(), 0, &[(2, &[3])]);
        for name in ["wallets.idx", "programs.rel"] {
            let path = right.path().join("shard-0").join(name);
            let mut file = OpenOptions::new().write(true).open(path).unwrap();
            file.seek(SeekFrom::Start(4)).unwrap();
            file.write_all(&3u32.to_le_bytes()).unwrap();
        }

        let error = compare_indexes(left.path(), right.path()).unwrap_err();
        assert!(format!("{error:#}").contains("unsupported wallets.idx format version 3"));
    }

    #[test]
    fn reader_rejects_invalid_usage_timing_sentinel() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        let invalid = ProgramUsage {
            program_id: 3,
            direct_instruction_count: 1,
            inner_instruction_count: 0,
            transaction_count: 1,
            first_seen_slot: 100,
            last_seen_slot: 100,
            min_block_time: 1_000,
            max_block_time: 1_000,
            timed_transaction_count: 0,
        };
        write_usage_shard(left.path(), 0, &[(2, vec![usage(3)])]);
        write_usage_shard(right.path(), 0, &[(2, vec![invalid])]);

        let error = compare_indexes(left.path(), right.path()).unwrap_err();
        assert!(format!("{error:#}").contains("missing block-time sentinel is inconsistent"));
    }

    #[test]
    fn canonical_comparison_rejects_trailing_unreferenced_relations() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        write_shard(left.path(), 0, &[(2, &[3])]);
        write_shard(right.path(), 0, &[(2, &[3])]);
        let relations = right.path().join("shard-0/programs.rel");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&relations)
            .unwrap();
        file.write_all(&program_usage_to_le_bytes(usage(9)))
            .unwrap();

        let error = compare_indexes(left.path(), right.path()).unwrap_err();
        assert!(format!("{error:#}").contains("length"));
    }

    #[test]
    fn difference_summary_counts_wallet_and_relation_set_deltas() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        write_shard(left.path(), 0, &[(2, &[3, 9]), (7, &[4])]);
        write_shard(right.path(), 0, &[(2, &[3, 10]), (8, &[4, 5])]);

        let summary = summarize_differences(left.path(), right.path()).unwrap();
        assert!(!summary.canonical_equal());
        assert_eq!(summary.shared_wallets, 1);
        assert_eq!(summary.left_only_wallets, 1);
        assert_eq!(summary.right_only_wallets, 1);
        assert_eq!(summary.shared_relations, 1);
        assert_eq!(summary.left_only_relations, 2);
        assert_eq!(summary.right_only_relations, 3);
        assert_eq!(summary.left.relations, 3);
        assert_eq!(summary.right.relations, 4);
    }

    #[test]
    fn registry_aware_comparison_ignores_different_registry_id_orders() {
        let temp = TempDir::new().unwrap();
        let left = temp.path().join("left-index");
        let right = temp.path().join("right-index");
        fs::create_dir(&left).unwrap();
        fs::create_dir(&right).unwrap();

        let wallet = [7u8; 32];
        let program_a = [3u8; 32];
        let program_b = [11u8; 32];
        let left_registry = temp.path().join("left-registry.bin");
        let right_registry = temp.path().join("right-registry.bin");
        write_registry(&left_registry, &[wallet, program_a, program_b]);
        write_registry(&right_registry, &[program_b, wallet, program_a]);
        write_shard(&left, 0, &[(1, &[2, 3])]);
        // IDs remain sorted in the physical index even though their pubkeys
        // resolve in the opposite lexical order.
        write_shard(&right, 0, &[(2, &[1, 3])]);

        let summary = compare_registry_indexes(
            &left,
            &left_registry,
            &right,
            &right_registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap();
        assert_eq!(summary.wallets, 1);
        assert_eq!(summary.relations, 2);
        assert_eq!(summary.sha256.len(), 64);

        let difference = summarize_registry_differences(
            &left,
            &left_registry,
            &right,
            &right_registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap();
        assert!(difference.canonical_equal());
        assert_eq!(difference.shared_wallets, 1);
        assert_eq!(difference.shared_relations, 2);
    }

    #[test]
    fn registry_aware_comparison_reports_a_semantic_relation_mismatch() {
        let temp = TempDir::new().unwrap();
        let left = temp.path().join("left-index");
        let right = temp.path().join("right-index");
        fs::create_dir(&left).unwrap();
        fs::create_dir(&right).unwrap();

        let wallet = [7u8; 32];
        let shared_program = [3u8; 32];
        let left_only_program = [11u8; 32];
        let right_only_program = [13u8; 32];
        let left_registry = temp.path().join("left-registry.bin");
        let right_registry = temp.path().join("right-registry.bin");
        write_registry(&left_registry, &[wallet, shared_program, left_only_program]);
        write_registry(
            &right_registry,
            &[right_only_program, wallet, shared_program],
        );
        write_shard(&left, 0, &[(1, &[2, 3])]);
        write_shard(&right, 0, &[(2, &[1, 3])]);

        let error = compare_registry_indexes(
            &left,
            &left_registry,
            &right,
            &right_registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("canonical pubkey mismatch"));

        let difference = summarize_registry_differences(
            &left,
            &left_registry,
            &right,
            &right_registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap();
        assert!(!difference.canonical_equal());
        assert_eq!(difference.shared_wallets, 1);
        assert_eq!(difference.shared_relations, 1);
        assert_eq!(difference.left_only_relations, 1);
        assert_eq!(difference.right_only_relations, 1);
    }

    #[test]
    fn registry_aware_comparison_binds_usage_payload() {
        let temp = TempDir::new().unwrap();
        let left = temp.path().join("left-index");
        let right = temp.path().join("right-index");
        fs::create_dir(&left).unwrap();
        fs::create_dir(&right).unwrap();
        let registry = temp.path().join("registry.bin");
        write_registry(&registry, &[[7u8; 32], [3u8; 32]]);
        let baseline = detailed_usage(2);
        let changed = ProgramUsage {
            direct_instruction_count: baseline.direct_instruction_count + 1,
            ..baseline
        };
        write_usage_shard(&left, 0, &[(1, vec![baseline])]);
        write_usage_shard(&right, 0, &[(1, vec![changed])]);

        let error = compare_registry_indexes(
            &left,
            &registry,
            &right,
            &registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("canonical pubkey mismatch"), "{message}");
        assert!(message.contains("direct_instructions="), "{message}");

        let difference = summarize_registry_differences(
            &left,
            &registry,
            &right,
            &registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap();
        assert_eq!(difference.shared_wallets, 1);
        assert_eq!(difference.shared_relations, 0);
        assert_eq!(difference.left_only_relations, 1);
        assert_eq!(difference.right_only_relations, 1);
    }

    #[test]
    fn registry_aware_comparison_rejects_out_of_range_ids() {
        let temp = TempDir::new().unwrap();
        let left = temp.path().join("left-index");
        let right = temp.path().join("right-index");
        fs::create_dir(&left).unwrap();
        fs::create_dir(&right).unwrap();
        let registry = temp.path().join("registry.bin");
        write_registry(&registry, &[[7u8; 32]]);
        write_shard(&left, 0, &[(1, &[2])]);
        write_shard(&right, 0, &[(1, &[2])]);

        let error = compare_registry_indexes(
            &left,
            &registry,
            &right,
            &registry,
            PUBKEY_RELATION_RECORD_LEN,
            temp.path(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("absent from the bound programs.map"));
    }

    #[test]
    fn retained_registry_uses_separate_bounded_wallet_and_program_caches() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("registry.bin");
        let key_count = WALLET_WINDOW_BYTES / 32 + 2;
        let keys: Vec<[u8; 32]> = (0..key_count)
            .map(|index| {
                let mut key = [0u8; 32];
                key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                key
            })
            .collect();
        write_registry(&path, &keys);

        let mut registry = PubkeyRegistry::load(&path).unwrap();
        assert!(REGISTRY_CACHE_ACCOUNTED_BYTES <= REGISTRY_CACHE_LIMIT_BYTES);
        assert_eq!(registry.wallet_cache.reads, 0);
        assert_eq!(registry.program_cache.reads, 0);

        assert_eq!(registry.resolve_wallet(1, temp.path()).unwrap(), keys[0]);
        assert_eq!(registry.wallet_cache.reads, 1);
        assert_eq!(registry.resolve_wallet(2, temp.path()).unwrap(), keys[1]);
        assert_eq!(registry.wallet_cache.reads, 1);
        assert_eq!(
            registry
                .resolve_wallet((key_count - 1) as u32, temp.path())
                .unwrap(),
            keys[key_count - 2]
        );
        assert_eq!(registry.wallet_cache.reads, 2);

        assert_eq!(registry.resolve_program(1, temp.path()).unwrap(), keys[0]);
        assert_eq!(registry.program_cache.reads, 1);
        assert_eq!(registry.resolve_program(2, temp.path()).unwrap(), keys[1]);
        assert_eq!(registry.program_cache.reads, 1);
        let next_page_id = (PROGRAM_CACHE_PAGE_BYTES / 32 + 1) as u32;
        assert_eq!(
            registry.resolve_program(next_page_id, temp.path()).unwrap(),
            keys[next_page_id as usize - 1]
        );
        assert_eq!(registry.program_cache.reads, 2);
        assert_eq!(registry.resolve_program(1, temp.path()).unwrap(), keys[0]);
        assert_eq!(registry.program_cache.reads, 2);
        registry.verify_unchanged().unwrap();
    }

    #[test]
    fn retained_registry_rejects_mutation_and_path_replacement() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("registry.bin");
        let replacement = temp.path().join("replacement.bin");
        write_registry(&path, &[[1u8; 32], [2u8; 32]]);
        let registry = PubkeyRegistry::load(&path).unwrap();

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[3u8; 32])
            .unwrap();
        let error = registry.verify_unchanged().unwrap_err();
        assert!(
            format!("{error:#}").contains("changed while semantic runs were built"),
            "{error:#}"
        );
        drop(registry);

        write_registry(&path, &[[1u8; 32], [2u8; 32]]);
        write_registry(&replacement, &[[1u8; 32], [2u8; 32]]);
        let registry = PubkeyRegistry::load(&path).unwrap();
        fs::rename(&replacement, &path).unwrap();
        let error = registry.verify_unchanged().unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("changed while semantic runs were built")
                || message.contains("no longer names the retained file"),
            "{message}"
        );
    }

    #[test]
    fn retained_registry_rejects_a_symlink() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target.bin");
        let link = temp.path().join("registry.bin");
        write_registry(&target, &[[1u8; 32]]);
        symlink(&target, &link).unwrap();

        let error = match PubkeyRegistry::load(&link) {
            Ok(_) => panic!("symlink registry unexpectedly opened"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("safely open registry"));
    }

    #[test]
    fn registry_cli_flags_must_be_paired() {
        let error = Args::try_parse_from([
            "index-parity",
            "--left-registry",
            "left-registry.bin",
            "left-index",
            "right-index",
        ])
        .unwrap_err();
        assert!(error.to_string().contains("--right-registry"));

        Args::try_parse_from([
            "index-parity",
            "--left-registry",
            "left-registry.bin",
            "--right-registry",
            "right-registry.bin",
            "left-index",
            "right-index",
        ])
        .unwrap();
    }

    fn write_registry(path: &Path, keys: &[[u8; 32]]) {
        let mut bytes = Vec::with_capacity(keys.len() * 32);
        for key in keys {
            bytes.extend_from_slice(key);
        }
        fs::write(path, bytes).unwrap();
    }

    fn write_shard(root: &Path, shard: u32, rows: &[(u32, &[u32])]) {
        let usage_rows = rows
            .iter()
            .map(|(wallet, programs)| {
                (
                    *wallet,
                    programs.iter().copied().map(usage).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        write_usage_shard(root, shard, &usage_rows);
    }

    fn write_usage_shard(root: &Path, shard: u32, rows: &[(u32, Vec<ProgramUsage>)]) {
        let directory = root.join(format!("shard-{shard}"));
        fs::create_dir_all(&directory).unwrap();

        let mut wallets = Vec::new();
        wallets.extend_from_slice(&WALLETS_MAGIC);
        wallets.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        wallets.extend_from_slice(&(rows.len() as u64).to_le_bytes());
        let mut relations = Vec::new();
        relations.extend_from_slice(&RELATIONS_MAGIC);
        relations.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let relation_count: usize = rows.iter().map(|(_, programs)| programs.len()).sum();
        relations.extend_from_slice(&(relation_count as u64).to_le_bytes());

        let mut offset = 0u64;
        for (wallet, programs) in rows {
            wallets.extend_from_slice(&wallet.to_le_bytes());
            wallets.extend_from_slice(&offset.to_le_bytes());
            wallets.extend_from_slice(&(programs.len() as u32).to_le_bytes());
            for program in programs {
                relations.extend_from_slice(&program_usage_to_le_bytes(*program));
            }
            offset += programs.len() as u64;
        }

        fs::write(directory.join("wallets.idx"), wallets).unwrap();
        fs::write(directory.join("programs.rel"), relations).unwrap();
    }

    fn usage(program_id: u32) -> ProgramUsage {
        ProgramUsage::new_transaction(program_id, 1, 0, 100, Some(1_000)).unwrap()
    }

    fn detailed_usage(program_id: u32) -> ProgramUsage {
        let usage = ProgramUsage {
            program_id,
            direct_instruction_count: 5,
            inner_instruction_count: 7,
            transaction_count: 4,
            first_seen_slot: 100,
            last_seen_slot: 200,
            min_block_time: 1_000,
            max_block_time: 1_100,
            timed_transaction_count: 3,
        };
        usage.validate().unwrap();
        usage
    }
}

#[cfg(test)]
fn fixture_program_map(index_root: &Path, registry_path: &Path) -> Result<BoundProgramMap> {
    let bytes = fs::read(registry_path)?;
    ensure!(
        bytes.len() % 32 == 0,
        "fixture registry length is not a multiple of 32"
    );
    let entries = bytes
        .chunks_exact(32)
        .enumerate()
        .map(|(index, key)| {
            let id = u32::try_from(index + 1).context("fixture registry is too large")?;
            Ok((id, key.try_into().expect("chunk length is exact")))
        })
        .collect::<Result<Vec<_>>>()?;
    let binding = blockzilla_firebase_indexer::format::write_program_map(index_root, &entries)?;
    Ok(BoundProgramMap {
        binding,
        count: entries.len() as u64,
    })
}
