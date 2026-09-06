//! One-shot, no-clobber Compact Archive V2 Pre-to-Post message migration.
//!
//! The command admits one complete source generation as the historical Pre
//! message grammar, transcodes only typed message bytes, and publishes a new
//! Post generation. With `--fast-candidate`, it instead publishes an explicit
//! non-canonical candidate and defers the target audit and canonical controls.
//! Strict mode never changes the source. Fast-candidate mode changes only the
//! active blocks/index names under a durable pair-swap transaction and keeps
//! the old pair plus stale dependent controls in an explicit backup.

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_archive_gateway::{GenerateManifestOptions, generate_manifest};
use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GENESIS_BIN_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_RAW_BLOCKS_FILE, ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2HotBlockHeader, ArchiveV2HotTxRow, ArchiveV2WireIdentityVisitor, ArchiveV2WireRewriteLimits, BLOCK_TIME_GAP_FILE, transcode_archive_v2_hot_message_wire_pre_to_post, write_archive_v2_hot_block_index};
use blockzilla_primitives::wincode_leb128_config;
use blockzilla_read_sdk::{
    ARCHIVE_V2_PUBLICATION_LOCK_FILE, ArchiveReader, ArchiveV2MetadataWireProfile,
    ArchiveV2WireProfile, BorrowedDecodedBlock, Error as ReaderError,
    FullGenerationWireProfileAudit, HashVerification, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, OpenOptions as ReaderOpenOptions,
    OrderedParallelBlockConfig, OrderedParallelBlockStats, PinnedLocalRangeSource, RangeSource,
    SourceError, SourceResult, UnprovenWireProfileDecision, audit_full_generation_wire_profile,
    manifest::{GENERATION_MANIFEST_FILE, GenerationManifest, TrustedGenerationIdentity},
    wire_profile_marker,
};
use clap::{ArgGroup, Parser};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Seek, SeekFrom, Write},
    mem::MaybeUninit,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        mpsc::{SyncSender, sync_channel},
    },
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::{
    ffi::OsStrExt,
    fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
};

const DEFAULT_SLOTS_PER_EPOCH: u64 = 432_000;
const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 << 20;
const DEFAULT_PROGRESS_BLOCKS: u64 = 10_000;
const DEFAULT_FAST_THREADS: usize = 8;
const FAST_COMPRESSED_BATCH_TARGET_BYTES: usize = 16 << 20;
const FAST_UNCOMPRESSED_BATCH_BUDGET_BYTES: usize = 512 << 20;
const FAST_COMPRESSED_INPUT_BUFFERS: usize = 3;
const FAST_OUTPUT_BUFFER_COUNT: usize = 8_192;
const FAST_RETAINED_DECOMPRESSED_BYTES_PER_WORKER: usize = 64 << 20;
const FAST_RETAINED_WORK_BUFFER_BYTES: usize = 8 << 20;
const FAST_RETAINED_OUTPUT_BUFFER_BYTES: usize = 32 << 10;
const IO_BUFFER_BYTES: usize = 8 << 20;
const FREE_SPACE_MARGIN_MIN_BYTES: u64 = 256 << 20;
const FREE_SPACE_MARGIN_MAX_BYTES: u64 = 4 << 30;
const REGISTRY_REPROCESS_RECEIPT_FILE: &str = "archive-v2-registry-reprocess.receipt.json";
const PRE_TO_POST_RECEIPT_FILE: &str = "archive-v2-pre-to-post.receipt.json";
const PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE: &str = "archive-v2-pre-to-post.candidate.v1.json";
const PRE_TO_POST_SWITCH_INTENT_FILE: &str = "archive-v2-pre-to-post.switch-intent.v1.json";
const PRE_TO_POST_SWITCH_COMPLETE_FILE: &str = "archive-v2-pre-to-post.switch-complete.v1.json";
const PRE_TO_POST_DISABLED_DIRECTORY: &str = "disabled";
const PRE_TO_POST_ROOT_SWITCH_LOCK_FILE: &str = ".archive-v2-pre-to-post.switch.lock";
const SCHEDULER_COMPLETION_FILE: &str = ".hivezilla-pipeline-owned.v1.json";

fn parse_fast_thread_count(value: &str) -> std::result::Result<usize, String> {
    let threads = value
        .parse::<usize>()
        .map_err(|_| "thread count must be a positive integer".to_owned())?;
    if !(1..=MAX_ORDERED_PARALLEL_DECODE_WORKERS).contains(&threads) {
        return Err(format!(
            "thread count must be between 1 and {MAX_ORDERED_PARALLEL_DECODE_WORKERS}"
        ));
    }
    Ok(threads)
}

const REQUIRED_CANDIDATE_FINALIZATION_STEPS: &[&str] = &[
    "full-target-post-wire-profile-audit",
    "exact-registry-mphf-admission",
    "final-full-file-hash-verification",
    "canonical-migration-receipt",
    "canonical-post-wire-profile-marker",
    "canonical-generation-manifest",
    "atomic-canonical-publication",
];

const EDGE_TIER_FILES: &[&str] = &[
    ARCHIVE_V2_BLOCK_ACCESS_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
];

const LEGACY_EDGE_TIER_FILES: &[&str] = &[
    "archive-v2-block-access.index.pre-votehash-20260523T205501+0200",
    "archive-v2-get-block.index.pre-votehash-20260523T205501+0200",
];

const DURABLE_COPY_FILES: &[&str] = &[
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_GENESIS_BIN_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_SHREDDING_FILE,
    BLOCK_TIME_GAP_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
];

const SOURCE_CONTROL_FILES: &[&str] = &[
    GENERATION_MANIFEST_FILE,
    REGISTRY_REPROCESS_RECEIPT_FILE,
    PRE_TO_POST_RECEIPT_FILE,
    PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE,
    ARCHIVE_V2_PUBLICATION_LOCK_FILE,
    SCHEDULER_COMPLETION_FILE,
];

const LEGACY_SOURCE_CONTROL_FILES: &[&str] = &[
    ".block-time-gaps.bin.lock",
    ".complete-hot-v2-no-access-delete-car",
    ".complete-hot-v2-shredding-sidecar-v2",
];

const OBSOLETE_BLOCK_FILES: &[&str] =
    &[ARCHIVE_V2_RAW_BLOCKS_FILE, ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE];

#[derive(Debug, Parser)]
#[command(
    name = "archive-v2-pre-to-post",
    version,
    about = "Publish one canonical Compact Archive V2 Post generation from one proven Pre generation",
    group(
        ArgGroup::new("source_authority")
            .required(true)
            .multiple(false)
            .args(["source_snapshot_id", "source_lease_id"])
    )
)]
struct Args {
    /// Exact source epoch directory.
    #[arg(long)]
    source: PathBuf,

    /// Provider identity for a read-only snapshot or immutable mount.
    #[arg(long)]
    source_snapshot_id: Option<String>,

    /// Operator identity for Linux kernel read leases on every pinned source inode.
    #[arg(long)]
    source_lease_id: Option<String>,

    /// Fresh final generation directory. It must not exist. With
    /// --fast-candidate, this must be exactly the existing --source path; the
    /// atomic exchange keeps that epoch path stable.
    #[arg(long)]
    target: PathBuf,

    /// Fresh private sibling staging directory. It must be exactly
    /// TARGET_PARENT/.TARGET_NAME.pre-to-post.staging and must not exist.
    #[arg(long)]
    staging: PathBuf,

    /// Source and target epoch number.
    #[arg(long)]
    epoch: u64,

    /// Manifest cluster identity.
    #[arg(long)]
    cluster_id: String,

    /// New immutable manifest generation identity.
    #[arg(long)]
    generation_id: String,

    /// Slots in one epoch.
    #[arg(long, default_value_t = DEFAULT_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    /// Zstd level for target blocks. The source index level is used when this
    /// option is absent.
    #[arg(long)]
    zstd_level: Option<i32>,

    /// Per-message wire limit for both source audit and transcode.
    #[arg(long, default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,

    /// Print one progress record after this many blocks. Zero disables it.
    #[arg(long, default_value_t = DEFAULT_PROGRESS_BLOCKS)]
    progress_blocks: u64,

    /// Parallel block transcode/compression workers for --fast-candidate.
    /// Fast mode uses 8 workers when this option is absent.
    #[arg(
        long,
        requires = "fast_candidate",
        value_parser = parse_fast_thread_count
    )]
    threads: Option<usize>,

    /// Exact successful whole-epoch scanner report that admits this source as
    /// LegacyPre. Required with --fast-candidate and forbidden otherwise.
    #[arg(long)]
    source_audit_report: Option<PathBuf>,

    /// Expected SHA-256 of the admitted scanner report. Required with
    /// --fast-candidate and forbidden otherwise.
    #[arg(long)]
    source_audit_report_sha256: Option<String>,

    /// Replace only blocks and their hot index in the source epoch, keep the
    /// old pair and disabled stale controls in .epoch-N.pre-to-post.backup,
    /// and publish an explicit non-canonical candidate. This skips the second
    /// source audit, target Post audit, manifest hashes, canonical marker, and
    /// canonical receipt.
    #[arg(long)]
    fast_candidate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum SourceEntryDisposition {
    Rewrite,
    CopyDurable,
    OmitEdge,
    OmitControl,
    OmitObsoleteBlock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SourceEntry {
    bytes: u64,
    disposition: SourceEntryDisposition,
}

#[derive(Debug)]
struct ValidatedPaths {
    source: PathBuf,
    target: PathBuf,
    staging: PathBuf,
    parent: PathBuf,
    backup: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceAuthorityKind {
    ProviderSnapshot,
    LinuxReadLeases,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceAuthority {
    kind: SourceAuthorityKind,
    id: String,
}

impl SourceAuthority {
    fn from_ids(snapshot_id: Option<&str>, lease_id: Option<&str>) -> Result<Self> {
        let (kind, id) = match (snapshot_id, lease_id) {
            (Some(id), None) => (SourceAuthorityKind::ProviderSnapshot, id),
            (None, Some(id)) => (SourceAuthorityKind::LinuxReadLeases, id),
            (None, None) => anyhow::bail!(
                "exactly one of --source-snapshot-id or --source-lease-id is required"
            ),
            (Some(_), Some(_)) => {
                anyhow::bail!("--source-snapshot-id and --source-lease-id are mutually exclusive")
            }
        };
        ensure!(!id.is_empty(), "source authority ID must not be empty");
        Ok(Self {
            kind,
            id: id.to_owned(),
        })
    }

    fn kind_name(&self) -> &'static str {
        match self.kind {
            SourceAuthorityKind::ProviderSnapshot => "provider-read-only-snapshot",
            SourceAuthorityKind::LinuxReadLeases => "linux-kernel-read-leases",
        }
    }

    fn scope_name(&self) -> &'static str {
        match self.kind {
            SourceAuthorityKind::ProviderSnapshot => "provider-enforced-read-only-generation-path",
            SourceAuthorityKind::LinuxReadLeases => {
                "all-reviewed-source-inodes-pinned-and-read-leased-on-one-local-ext4-device"
            }
        }
    }

    fn filesystem_name(&self) -> &'static str {
        match self.kind {
            SourceAuthorityKind::ProviderSnapshot => "provider-defined-read-only-filesystem",
            SourceAuthorityKind::LinuxReadLeases => "linux-local-ext4",
        }
    }
}

#[cfg(target_os = "linux")]
const LEASE_BREAK_EXIT_CODE: i32 = 128 + libc::SIGIO;
#[cfg(target_os = "linux")]
const EXT4_SUPER_MAGIC: i128 = 0xEF53;
// Linux UAPI `F_SETSIG`; libc does not expose it for every Linux libc target.
#[cfg(target_os = "linux")]
const F_SETSIG_COMMAND: libc::c_int = 10;

#[cfg(target_os = "linux")]
extern "C" fn exit_on_source_lease_break(_signal: libc::c_int) {
    // SAFETY: `_exit` is async-signal-safe and does not run destructors or flush buffered output.
    unsafe { libc::_exit(LEASE_BREAK_EXIT_CODE) }
}

#[cfg(target_os = "linux")]
fn install_source_lease_break_handler() -> Result<()> {
    static INSTALL_ERRNO: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    let errno = *INSTALL_ERRNO.get_or_init(|| {
        // SAFETY: zero is a valid starting state for sigaction and sigset_t.
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = exit_on_source_lease_break as *const () as usize;
        action.sa_flags = 0;
        // SAFETY: all pointers refer to live local values of the required libc types.
        if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0 {
            return io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
        }
        // SAFETY: the handler and action remain valid after this call returns.
        if unsafe { libc::sigaction(libc::SIGIO, &action, std::ptr::null_mut()) } != 0 {
            return io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
        }
        0
    });
    ensure!(
        errno == 0,
        "install SIGIO source-lease break handler: {}",
        io::Error::from_raw_os_error(errno)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn unblock_source_lease_break_signal() -> Result<()> {
    // A blocked SIGIO would let a writer wait for the kernel lease timeout instead of stopping
    // this publication process immediately. This must run in every thread that acquires leases.
    // SAFETY: zero is a valid starting state for sigset_t.
    let mut signals = unsafe { std::mem::zeroed::<libc::sigset_t>() };
    // SAFETY: `signals` points to a live sigset_t.
    ensure!(
        unsafe { libc::sigemptyset(&mut signals) } == 0
            && unsafe { libc::sigaddset(&mut signals, libc::SIGIO) } == 0,
        "build SIGIO source-lease signal set: {}",
        io::Error::last_os_error()
    );
    // SAFETY: `signals` points to a live initialized set and no previous mask is requested.
    let result =
        unsafe { libc::pthread_sigmask(libc::SIG_UNBLOCK, &signals, std::ptr::null_mut()) };
    ensure!(
        result == 0,
        "unblock SIGIO source-lease break signal: {}",
        io::Error::from_raw_os_error(result)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
struct LinuxReadLease {
    name: String,
    file: File,
}

#[cfg(target_os = "linux")]
impl Drop for LinuxReadLease {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is live. Failure can only make the kernel retain the lease until
        // the descriptor closes immediately after this method returns.
        let _ = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_SETLEASE, libc::F_UNLCK) };
    }
}

#[cfg(target_os = "linux")]
struct SourceLeaseSet {
    directory: File,
    device_id: u64,
    leases: Vec<LinuxReadLease>,
}

#[cfg(target_os = "linux")]
impl SourceLeaseSet {
    fn acquire(
        source: &PinnedLocalRangeSource,
        entries: &BTreeMap<String, SourceEntry>,
    ) -> Result<Self> {
        use std::os::fd::AsRawFd;

        install_source_lease_break_handler()?;
        unblock_source_lease_break_signal()?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(source.root())
            .with_context(|| {
                format!(
                    "open local ext4 source directory {}",
                    source.root().display()
                )
            })?;
        let device_id = require_local_ext4(&directory, "source directory")?;
        let owner = unsafe { libc::getpid() };
        let mut leases = Vec::with_capacity(entries.len());
        for name in entries.keys() {
            let file = source
                .open_file(name)
                .map_err(|error| anyhow!(error))
                .with_context(|| format!("open pinned source object for read lease {name}"))?;
            ensure!(
                require_local_ext4(&file, name)? == device_id,
                "source object {name} is not on the source directory device"
            );
            // SAFETY: `file` is a live regular-file descriptor and `owner` is this process.
            ensure!(
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETOWN, owner) } != -1,
                "set SIGIO owner for source object {name}: {}",
                io::Error::last_os_error()
            );
            // SAFETY: the descriptor is live and Linux accepts SIGIO for lease notifications.
            ensure!(
                unsafe { libc::fcntl(file.as_raw_fd(), F_SETSIG_COMMAND, libc::SIGIO) } != -1,
                "select SIGIO lease-break notification for source object {name}: {}",
                io::Error::last_os_error()
            );
            // SAFETY: `file` is a live read-only regular-file descriptor. The kernel rejects this
            // call when a writer is already open or the filesystem does not support leases.
            ensure!(
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLEASE, libc::F_RDLCK) } != -1,
                "acquire Linux read lease for source object {name}: {}",
                io::Error::last_os_error()
            );
            leases.push(LinuxReadLease {
                name: name.clone(),
                file,
            });
        }
        let authority = Self {
            directory,
            device_id,
            leases,
        };
        authority.verify_all_held()?;
        Ok(authority)
    }

    fn verify_all_held(&self) -> Result<()> {
        use std::os::fd::AsRawFd;
        ensure!(
            require_local_ext4(&self.directory, "source directory")? == self.device_id,
            "source directory device changed while leases were held"
        );
        for lease in &self.leases {
            ensure!(
                require_local_ext4(&lease.file, &lease.name)? == self.device_id,
                "source object {} changed filesystem while its lease was held",
                lease.name
            );
            // SAFETY: the guard retains a live descriptor for the leased open-file description.
            let state = unsafe { libc::fcntl(lease.file.as_raw_fd(), libc::F_GETLEASE) };
            ensure!(
                state == libc::F_RDLCK,
                "Linux read lease is not held for source object {}: state={state} error={}",
                lease.name,
                io::Error::last_os_error()
            );
        }
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.leases.len()
    }

    fn device_id(&self) -> u64 {
        self.device_id
    }
}

#[cfg(target_os = "linux")]
fn require_local_ext4(file: &File, label: &str) -> Result<u64> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};

    let mut filesystem = MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `file` is live and `filesystem` points to writable storage.
    ensure!(
        unsafe { libc::fstatfs(file.as_raw_fd(), filesystem.as_mut_ptr()) } == 0,
        "inspect filesystem for {label}: {}",
        io::Error::last_os_error()
    );
    // SAFETY: fstatfs returned success and initialized the structure.
    let filesystem = unsafe { filesystem.assume_init() };
    let filesystem_magic = i128::from(filesystem.f_type);
    ensure!(
        filesystem_magic == EXT4_SUPER_MAGIC,
        "Linux read-lease authority requires NAS-local ext4; {label} has filesystem magic {:#x}",
        filesystem_magic
    );
    Ok(file.metadata()?.dev())
}

#[cfg(not(target_os = "linux"))]
struct SourceLeaseSet;

#[cfg(not(target_os = "linux"))]
impl SourceLeaseSet {
    fn acquire(
        _source: &PinnedLocalRangeSource,
        _entries: &BTreeMap<String, SourceEntry>,
    ) -> Result<Self> {
        anyhow::bail!("--source-lease-id requires Linux kernel file leases")
    }

    fn verify_all_held(&self) -> Result<()> {
        anyhow::bail!("Linux source read leases are unavailable on this operating system")
    }

    fn device_id(&self) -> u64 {
        unreachable!("Linux source read leases are unavailable on this operating system")
    }
}

fn verify_source_leases(leases: &Option<SourceLeaseSet>) -> Result<()> {
    if let Some(leases) = leases {
        leases.verify_all_held()?;
    }
    Ok(())
}

#[cfg(all(target_os = "linux", not(test)))]
fn recovery_payload_entries(directory: &Path) -> Result<BTreeMap<String, SourceEntry>> {
    let mut entries = BTreeMap::new();
    for name in [ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE] {
        let path = directory.join(name);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect recovery payload {}", path.display()))?;
        ensure!(
            metadata.file_type().is_file(),
            "recovery payload is not a real regular file: {}",
            path.display()
        );
        entries.insert(
            name.to_owned(),
            SourceEntry {
                bytes: metadata.len(),
                disposition: SourceEntryDisposition::Rewrite,
            },
        );
    }
    Ok(entries)
}

#[cfg(all(target_os = "linux", not(test)))]
struct RecoveryPayloadLeaseSet {
    active: SourceLeaseSet,
    workspace: SourceLeaseSet,
}

#[cfg(all(target_os = "linux", not(test)))]
impl RecoveryPayloadLeaseSet {
    fn acquire(active: &Path, workspace: &Path) -> Result<Self> {
        let active_source = PinnedLocalRangeSource::new(active);
        let workspace_source = PinnedLocalRangeSource::new(workspace);
        let active = SourceLeaseSet::acquire(
            &active_source,
            &recovery_payload_entries(active_source.root())?,
        )?;
        let workspace = SourceLeaseSet::acquire(
            &workspace_source,
            &recovery_payload_entries(workspace_source.root())?,
        )?;
        ensure!(
            active.device_id() == workspace.device_id(),
            "recovery payload workspaces are on different filesystems"
        );
        let leases = Self { active, workspace };
        leases.verify_all_held()?;
        Ok(leases)
    }

    fn verify_all_held(&self) -> Result<()> {
        self.active.verify_all_held()?;
        self.workspace.verify_all_held()
    }
}

#[cfg(any(not(target_os = "linux"), test))]
struct RecoveryPayloadLeaseSet;

#[cfg(any(not(target_os = "linux"), test))]
impl RecoveryPayloadLeaseSet {
    fn acquire(_active: &Path, _workspace: &Path) -> Result<Self> {
        if cfg!(test) {
            Ok(Self)
        } else {
            anyhow::bail!("fast-candidate recovery requires Linux read leases")
        }
    }

    fn verify_all_held(&self) -> Result<()> {
        if cfg!(test) {
            Ok(())
        } else {
            anyhow::bail!("fast-candidate recovery requires Linux read leases")
        }
    }
}

#[derive(Clone)]
struct SourceWithBlockHash {
    inner: PinnedLocalRangeSource,
    block_hash: Arc<Mutex<BlockHashState>>,
}

struct BlockHashState {
    expected_bytes: u64,
    next_offset: u64,
    hasher: Sha256,
    active: bool,
}

impl SourceWithBlockHash {
    fn new(inner: PinnedLocalRangeSource, expected_bytes: u64) -> Self {
        Self {
            inner,
            block_hash: Arc::new(Mutex::new(BlockHashState {
                expected_bytes,
                next_offset: 0,
                hasher: Sha256::new(),
                active: true,
            })),
        }
    }

    fn finish_block_hash(&self) -> Result<FileBinding> {
        let mut state = self
            .block_hash
            .lock()
            .map_err(|_| anyhow!("source block hash state is poisoned"))?;
        ensure!(state.active, "source block hash was already finished");
        ensure!(
            state.next_offset == state.expected_bytes,
            "source audit hashed {} of {} block bytes",
            state.next_offset,
            state.expected_bytes
        );
        state.active = false;
        Ok(FileBinding {
            bytes: state.expected_bytes,
            sha256: hex_lower(&state.hasher.clone().finalize()),
        })
    }

    fn restart_block_hash(&self) -> Result<()> {
        let mut state = self
            .block_hash
            .lock()
            .map_err(|_| anyhow!("source block hash state is poisoned"))?;
        ensure!(!state.active, "source block hash pass is still active");
        state.next_offset = 0;
        state.hasher = Sha256::new();
        state.active = true;
        Ok(())
    }

    fn add_block_range(&self, offset: u64, bytes: &[u8]) -> SourceResult<()> {
        let mut state = self
            .block_hash
            .lock()
            .map_err(|_| SourceError::Protocol("source block hash state is poisoned".to_owned()))?;
        if !state.active {
            return Ok(());
        }
        if offset != state.next_offset {
            return Err(SourceError::Protocol(format!(
                "source block audit read is not sequential: offset={offset} expected={}",
                state.next_offset
            )));
        }
        state.hasher.update(bytes);
        state.next_offset = state
            .next_offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| SourceError::Protocol("source block hash offset overflow".to_owned()))?;
        if state.next_offset > state.expected_bytes {
            return Err(SourceError::Protocol(
                "source block audit read exceeds the admitted block file".to_owned(),
            ));
        }
        Ok(())
    }
}

impl RangeSource for SourceWithBlockHash {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        self.inner.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_range_into(object, offset, length, &mut bytes)?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        self.inner
            .read_range_into(object, offset, length, destination)?;
        if object == ARCHIVE_V2_BLOCKS_FILE {
            self.add_block_range(offset, destination)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Serialize)]
struct RewriteReport {
    blocks: u64,
    borrowed_current_blocks: u64,
    owned_outer_fallbacks: u64,
    typed_messages: u64,
    raw_transaction_fallbacks: u64,
    message_input_bytes: u64,
    message_output_bytes: u64,
    message_mismatch_bytes: u64,
    source_instruction_data_tag_counts: [u64; 9],
    metadata_input_bytes: u64,
    metadata_output_bytes: u64,
    metadata_regions_byte_identical: bool,
    source_compressed_bytes: u64,
    target_compressed_bytes: u64,
    uncompressed_bytes: u64,
    target_blocks_sha256: String,
}

#[derive(Debug, Serialize)]
struct FinalReport {
    schema_version: u32,
    kind: &'static str,
    epoch: u64,
    cluster_id: String,
    generation_id: String,
    source: String,
    source_authority_kind: &'static str,
    source_authority_id: String,
    source_authority_scope: &'static str,
    source_authority_filesystem: &'static str,
    source_authority_device_id: Option<u64>,
    target: String,
    staging: String,
    source_profile: &'static str,
    target_profile: &'static str,
    source_profile_decision: &'static str,
    source_audit_blocks: u64,
    source_audit_typed_messages: u64,
    source_audit_selected_only: u64,
    source_audit_both_equivalent: u64,
    source_audit_both_divergent: u64,
    source_audit_raw_transaction_fallbacks: u64,
    source_audit_raw_metadata_fallbacks: u64,
    zstd_level: i32,
    rewrite: RewriteReport,
    copied_durable_files: Vec<String>,
    omitted_edge_files: Vec<String>,
    omitted_control_files: Vec<String>,
    omitted_obsolete_block_files: Vec<String>,
    target_post_audit_passed: bool,
    target_manifest: String,
    target_manifest_digest: String,
    migration_receipt: String,
    edge_rebuild_required: bool,
    staged_files_read_only: bool,
    staged_directory_read_only: bool,
    target_provider_immutability_required: bool,
    source_provider_snapshot_required: bool,
    source_linux_read_leases_required: bool,
    elapsed_seconds: f64,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum RunReport {
    Canonical(FinalReport),
    Candidate(CandidateReport),
    CandidateRecovery(CandidateRecoveryReport),
}

#[derive(Debug, Serialize)]
struct CandidateReport {
    schema_version: u32,
    kind: &'static str,
    state: &'static str,
    canonical: bool,
    epoch: u64,
    cluster_id: String,
    prospective_generation_id: String,
    source: String,
    source_authority_kind: &'static str,
    source_authority_id: String,
    source_authority_scope: &'static str,
    source_authority_filesystem: &'static str,
    source_authority_device_id: Option<u64>,
    candidate: String,
    backup: String,
    candidate_descriptor: String,
    candidate_descriptor_bytes: u64,
    candidate_descriptor_sha256: String,
    source_profile_evidence: &'static str,
    source_audit_report: String,
    source_audit_report_bytes: u64,
    source_audit_report_sha256: String,
    expected_wire_profile_after_rewrite: &'static str,
    source_scan_counts: ScannerCounts,
    source_full_audit_performed_in_this_run: bool,
    source_audit_report_reused: bool,
    single_decode_rewrite_pass: bool,
    outer_block_bytes_preserved_verbatim_except_messages: bool,
    sidecars_copied: bool,
    sidecars_rewritten: bool,
    pair_swap_requires_external_reader_quiescence: bool,
    archive_root_switch_lock: String,
    zstd_level: i32,
    rewrite: RewriteReport,
    retained_durable_files: Vec<String>,
    retained_edge_files: Vec<String>,
    retained_edge_files_authoritative: bool,
    retained_edge_validation_deferred: bool,
    get_block_index_rebuild_required: bool,
    moved_to_backup: Vec<String>,
    canonical_publication_deferred: bool,
    target_post_audit_performed: bool,
    canonical_manifest_written: bool,
    canonical_profile_marker_written: bool,
    canonical_migration_receipt_written: bool,
    required_finalization: &'static [&'static str],
    edge_rebuild_required: bool,
    rewritten_files_read_only: bool,
    source_provider_snapshot_required: bool,
    source_linux_read_leases_required: bool,
    elapsed_seconds: f64,
}

#[derive(Debug, Serialize)]
struct CandidateRecoveryReport {
    schema_version: u32,
    kind: &'static str,
    state: &'static str,
    canonical: bool,
    epoch: u64,
    cluster_id: String,
    prospective_generation_id: String,
    candidate: String,
    backup: String,
    candidate_descriptor: String,
    candidate_descriptor_bytes: u64,
    candidate_descriptor_sha256: String,
    source_audit_report: String,
    source_audit_report_bytes: u64,
    source_audit_report_sha256: String,
    recovered_switch: bool,
    already_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileBinding {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileIdentity {
    bytes: u64,
    device_id: u64,
    inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannerCounts {
    blocks: u64,
    owned_fallback_blocks: u64,
    compressed_block_bytes: u64,
    uncompressed_block_bytes: u64,
    typed_messages: u64,
    raw_transaction_fallbacks: u64,
    post_only: u64,
    pre_only: u64,
    both_equivalent: u64,
    both_divergent: u64,
    invalid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ScannerReportBinding {
    path: String,
    bytes: u64,
    sha256: String,
    completed_unix_seconds: u64,
    workers: u64,
    counts: ScannerCounts,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannerReportDocument {
    schema_version: u32,
    kind: String,
    archive: String,
    epoch: u64,
    workers: u64,
    classification: String,
    action: String,
    counts: ScannerCounts,
    first_evidence: ScannerFirstEvidence,
    error: Option<serde_json::Value>,
    elapsed_seconds: f64,
    completed_unix_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannerFirstEvidence {
    post_only: Option<ScannerLocation>,
    pre_only: Option<ScannerLocation>,
    both_divergent: Option<ScannerLocation>,
    invalid: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannerLocation {
    slot: u64,
    transaction_index: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairSwapIntent {
    schema_version: u32,
    kind: String,
    epoch: u64,
    cluster_id: String,
    prospective_generation_id: String,
    candidate: String,
    staging: String,
    backup: String,
    source_blocks: FileIdentity,
    source_blocks_binding: FileBinding,
    source_index: FileIdentity,
    source_index_binding: FileBinding,
    candidate_blocks: FileIdentity,
    candidate_blocks_binding: FileBinding,
    candidate_index: FileIdentity,
    candidate_index_binding: FileBinding,
    candidate_descriptor: FileBinding,
    moved_to_backup: Vec<String>,
    retained_edge_files: Vec<String>,
    source_audit_report_path: String,
    source_audit_report: FileBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairSwapComplete {
    schema_version: u32,
    kind: String,
    epoch: u64,
    canonical: bool,
    candidate: String,
    backup: String,
    intent_sha256: String,
    candidate_descriptor_sha256: String,
    source_audit_report_sha256: String,
    source_blocks_sha256: String,
    source_index_sha256: String,
    candidate_blocks_sha256: String,
    candidate_index_sha256: String,
}

#[derive(Serialize)]
struct MigrationReceipt<'a> {
    schema_version: u32,
    kind: &'static str,
    epoch: u64,
    cluster_id: &'a str,
    generation_id: &'a str,
    source: String,
    source_authority_kind: &'static str,
    source_authority_id: &'a str,
    source_authority_scope: &'static str,
    source_authority_filesystem: &'static str,
    source_authority_device_id: Option<u64>,
    target: String,
    source_profile: &'static str,
    target_profile: &'static str,
    source_profile_decision: &'static str,
    codec: &'static str,
    source_zstd_level: i32,
    target_zstd_level: i32,
    source_audit: SourceAuditReceipt,
    source_files: &'a BTreeMap<String, FileBinding>,
    target_files: &'a BTreeMap<String, FileBinding>,
    omitted_edge_files: &'a [String],
    omitted_control_files: &'a [String],
    omitted_obsolete_block_files: &'a [String],
    rewrite: &'a RewriteReport,
    exact_message_length_preserved: bool,
    exact_message_delta_proved: bool,
    metadata_regions_copied_verbatim: bool,
    edge_rebuild_required: bool,
    target_provider_immutability_required: bool,
    source_provider_snapshot_required: bool,
    source_linux_read_leases_required: bool,
}

#[derive(Serialize)]
struct CandidateDescriptor<'a> {
    schema_version: u32,
    kind: &'static str,
    state: &'static str,
    canonical: bool,
    epoch: u64,
    cluster_id: &'a str,
    prospective_generation_id: &'a str,
    source: String,
    source_authority_kind: &'static str,
    source_authority_id: &'a str,
    source_authority_scope: &'static str,
    source_authority_filesystem: &'static str,
    source_authority_device_id: Option<u64>,
    candidate: String,
    backup: String,
    source_profile_evidence: &'static str,
    source_audit_report: &'a ScannerReportBinding,
    expected_wire_profile_after_rewrite: &'static str,
    source_full_audit_performed_in_this_run: bool,
    source_audit_report_reused: bool,
    single_decode_rewrite_pass: bool,
    outer_block_bytes_preserved_verbatim_except_messages: bool,
    sidecars_copied: bool,
    sidecars_rewritten: bool,
    pair_swap_requires_external_reader_quiescence: bool,
    archive_root_switch_lock: String,
    codec: &'static str,
    source_zstd_level: i32,
    target_zstd_level: i32,
    source_files: &'a BTreeMap<String, FileBinding>,
    source_inventory: &'a BTreeMap<String, SourceEntry>,
    candidate_rewrite_files: &'a BTreeMap<String, FileBinding>,
    retained_durable_files: &'a [String],
    retained_edge_files: &'a [String],
    retained_edge_files_authoritative: bool,
    retained_edge_validation_deferred: bool,
    get_block_index_rebuild_required: bool,
    moved_to_backup: &'a [String],
    rewrite: &'a RewriteReport,
    exact_message_length_preserved: bool,
    exact_message_delta_proved: bool,
    metadata_regions_copied_verbatim: bool,
    canonical_publication_deferred: bool,
    target_post_audit_performed: bool,
    canonical_manifest_written: bool,
    canonical_profile_marker_written: bool,
    canonical_migration_receipt_written: bool,
    required_finalization: &'static [&'static str],
    edge_rebuild_required: bool,
    source_provider_snapshot_required: bool,
    source_linux_read_leases_required: bool,
}

#[derive(Clone, Copy, Serialize)]
struct SourceAuditReceipt {
    blocks: u64,
    typed_messages: u64,
    raw_transaction_fallbacks: u64,
    raw_metadata_fallbacks: u64,
    selected_only: u64,
    both_semantically_equivalent: u64,
    both_semantically_divergent: u64,
}

impl From<FullGenerationWireProfileAudit> for SourceAuditReceipt {
    fn from(audit: FullGenerationWireProfileAudit) -> Self {
        Self {
            blocks: audit.blocks,
            typed_messages: audit.typed_messages,
            raw_transaction_fallbacks: audit.raw_transaction_fallbacks,
            raw_metadata_fallbacks: audit.raw_metadata_fallbacks,
            selected_only: audit.selected_only,
            both_semantically_equivalent: audit.both_semantically_equivalent,
            both_semantically_divergent: audit.both_semantically_divergent,
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let report = run(args)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

fn run(args: Args) -> Result<RunReport> {
    let started = Instant::now();
    let source_authority = SourceAuthority::from_ids(
        args.source_snapshot_id.as_deref(),
        args.source_lease_id.as_deref(),
    )?;
    ensure!(
        !args.cluster_id.is_empty(),
        "--cluster-id must not be empty"
    );
    ensure!(
        !args.generation_id.is_empty(),
        "--generation-id must not be empty"
    );
    ensure!(
        args.slots_per_epoch > 0,
        "--slots-per-epoch must be positive"
    );
    ensure!(
        args.max_message_bytes > 0,
        "--max-message-bytes must be positive"
    );
    if args.fast_candidate {
        ensure!(
            matches!(source_authority.kind, SourceAuthorityKind::LinuxReadLeases),
            "--fast-candidate mutates directory entries and requires --source-lease-id"
        );
        ensure!(
            args.source_audit_report.is_some(),
            "--source-audit-report is required with --fast-candidate"
        );
        let report_sha256 = args
            .source_audit_report_sha256
            .as_deref()
            .context("--source-audit-report-sha256 is required with --fast-candidate")?;
        ensure_sha256_hex(report_sha256, "--source-audit-report-sha256")?;
    } else {
        ensure!(
            args.source_audit_report.is_none() && args.source_audit_report_sha256.is_none(),
            "--source-audit-report and --source-audit-report-sha256 are only valid with --fast-candidate"
        );
        ensure!(
            args.threads.is_none(),
            "--threads is only valid with --fast-candidate"
        );
    }

    let paths = validate_paths(
        &args.source,
        &args.target,
        &args.staging,
        args.epoch,
        args.fast_candidate,
    )?;
    if args.fast_candidate
        && (path_entry_exists_nofollow(&paths.staging)?
            || path_entry_exists_nofollow(
                paths
                    .backup
                    .as_ref()
                    .context("fast candidate backup path disappeared")?,
            )?)
    {
        return recover_fast_pair_swap(&args, &paths);
    }
    let source = PinnedLocalRangeSource::new(&paths.source);
    let initial_entries = inspect_and_pin_source(&paths.source, &source, source_authority.kind)?;
    validate_required_source_entries(&initial_entries, args.epoch)?;
    let source_leases = match source_authority.kind {
        SourceAuthorityKind::ProviderSnapshot => None,
        SourceAuthorityKind::LinuxReadLeases => {
            Some(SourceLeaseSet::acquire(&source, &initial_entries)?)
        }
    };
    verify_source_leases(&source_leases)?;
    let source_authority_device_id = source_leases.as_ref().map(SourceLeaseSet::device_id);
    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed while source authority was established")?;
    ensure_source_directory_unchanged(&paths.source, &initial_entries)?;
    let source_blocks_bytes = initial_entries
        .get(ARCHIVE_V2_BLOCKS_FILE)
        .context("source blocks entry disappeared")?
        .bytes;
    let audited_source = SourceWithBlockHash::new(source.clone(), source_blocks_bytes);

    let reader = ArchiveReader::open_trusted_with_metadata_profile(
        audited_source.clone(),
        TrustedGenerationIdentity {
            cluster_id: args.cluster_id.clone(),
            epoch: args.epoch,
            generation_id: format!("{}-source-preflight", args.generation_id),
            slots_per_epoch: args.slots_per_epoch,
            wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        },
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        ReaderOpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..ReaderOpenOptions::default()
        },
    )
    .map_err(|error| anyhow!(error))
    .context("admit source Archive V2 structure as the Pre profile")?;

    let scanner_report = if args.fast_candidate {
        Some(admit_scanner_report(
            args.source_audit_report
                .as_deref()
                .context("missing fast candidate scanner report")?,
            args.source_audit_report_sha256
                .as_deref()
                .context("missing fast candidate scanner report SHA-256")?,
            &paths.source,
            args.epoch,
        )?)
    } else {
        None
    };
    if let Some(report) = &scanner_report {
        let index_uncompressed_bytes =
            reader.index().rows.iter().try_fold(0u64, |total, row| {
                total
                    .checked_add(u64::from(row.uncompressed_len))
                    .context("source index uncompressed byte total overflow")
            })?;
        ensure!(
            report.counts.blocks == reader.index().rows.len() as u64
                && report.counts.compressed_block_bytes == reader.index().blob_file_bytes
                && report.counts.compressed_block_bytes == source_blocks_bytes
                && report.counts.uncompressed_block_bytes == index_uncompressed_bytes,
            "scanner report byte and block geometry differs from the admitted source index"
        );
    }
    let source_audit = if args.fast_candidate {
        None
    } else {
        let audit = audit_full_generation_wire_profile(&reader, args.max_message_bytes)
            .map_err(|error| anyhow!(error))
            .context("audit complete source generation as the Pre profile")?;
        let decision = audit
            .require_unproven_authority()
            .map_err(|error| anyhow!(error))
            .context("prove one whole-generation Pre grammar")?;
        ensure!(
            decision == UnprovenWireProfileDecision::UniqueFullGenerationDecode,
            "source is profile-equivalent, not uniquely proven Pre; no byte migration is authorized"
        );
        ensure!(
            audit.raw_transaction_fallbacks == 0,
            "source has {} raw transaction fallbacks; the canonical overnight cohort requires zero",
            audit.raw_transaction_fallbacks
        );
        ensure!(
            audit.raw_metadata_fallbacks == 0,
            "source has {} raw metadata fallbacks; the canonical overnight cohort requires zero",
            audit.raw_metadata_fallbacks
        );
        Some(audit)
    };
    let source_blocks_binding_from_audit = if args.fast_candidate {
        None
    } else {
        Some(
            audited_source
                .finish_block_hash()
                .context("finish source compressed-block hash from the audit pass")?,
        )
    };
    let source_index_binding = hash_open_file(
        source
            .open_file(ARCHIVE_V2_BLOCK_INDEX_FILE)
            .map_err(|error| anyhow!(error))?,
    )?;

    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed during source admission")?;
    ensure_source_directory_unchanged(&paths.source, &initial_entries)?;
    verify_source_leases(&source_leases)?;

    let copied_source_bytes = initial_entries.values().try_fold(0u64, |total, entry| {
        if entry.disposition == SourceEntryDisposition::CopyDurable {
            total
                .checked_add(entry.bytes)
                .context("durable sidecar byte total overflow")
        } else {
            Ok(total)
        }
    })?;
    preflight_free_space(
        &paths.parent,
        if args.fast_candidate {
            0
        } else {
            copied_source_bytes
        },
        source_blocks_bytes,
    )?;

    create_private_staging(&paths.staging)?;
    let copied_source_files = if args.fast_candidate {
        BTreeMap::new()
    } else {
        copy_durable_files(&paths.source, &paths.staging, &source, &initial_entries)?
    };

    let source_level = reader.index().level;
    let target_level = args.zstd_level.unwrap_or(source_level);
    if !args.fast_candidate {
        audited_source
            .restart_block_hash()
            .context("start the source block hash for the rewrite pass")?;
    }
    let rewrite = if args.fast_candidate {
        let parallel = rewrite_blocks_fast_parallel(
            &reader,
            &paths.staging,
            target_level,
            args.max_message_bytes,
            args.progress_blocks,
            args.threads.unwrap_or(DEFAULT_FAST_THREADS),
        )?;
        eprintln!(
            "{{\"kind\":\"archive-v2-pre-to-post-parallel-summary\",\"threads\":{},\"blocks\":{},\"batches\":{},\"read_calls\":{},\"source_bytes\":{},\"read_seconds\":{:.3},\"decode_transcode_compress_seconds\":{:.3}}}",
            args.threads.unwrap_or(DEFAULT_FAST_THREADS),
            parallel.pipeline.block_count,
            parallel.pipeline.batch_count,
            parallel.pipeline.read_call_count,
            parallel.pipeline.compressed_bytes,
            parallel.pipeline.producer_read_wall_time.as_secs_f64(),
            parallel
                .pipeline
                .coordinator_decode_project_wall_time
                .as_secs_f64(),
        );
        parallel.report
    } else {
        rewrite_blocks(
            &reader,
            &paths.staging,
            target_level,
            args.max_message_bytes,
            args.progress_blocks,
            false,
        )?
    };
    let rewrite_source_blocks_binding = audited_source
        .finish_block_hash()
        .context("finish source compressed-block hash from the rewrite pass")?;
    let source_blocks_binding = if let Some(admitted) = source_blocks_binding_from_audit {
        ensure!(
            rewrite_source_blocks_binding == admitted,
            "source compressed block bytes differed between the audit and rewrite passes"
        );
        admitted
    } else {
        rewrite_source_blocks_binding
    };
    if let Some(audit) = source_audit {
        ensure!(
            rewrite.blocks == audit.blocks
                && rewrite.typed_messages == audit.typed_messages
                && rewrite.raw_transaction_fallbacks == audit.raw_transaction_fallbacks,
            "rewrite coverage differs from the admitted source audit"
        );
    } else {
        let counts = &scanner_report
            .as_ref()
            .context("missing admitted scanner report")?
            .counts;
        ensure!(
            rewrite.blocks == counts.blocks
                && rewrite.typed_messages == counts.typed_messages
                && rewrite.raw_transaction_fallbacks == counts.raw_transaction_fallbacks
                && rewrite.owned_outer_fallbacks == counts.owned_fallback_blocks
                && rewrite.source_compressed_bytes == counts.compressed_block_bytes
                && rewrite.uncompressed_bytes == counts.uncompressed_block_bytes,
            "rewrite coverage differs from the admitted scanner report"
        );
    }

    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed during rewrite or durable copy")?;
    ensure_source_directory_unchanged(&paths.source, &initial_entries)?;
    verify_source_leases(&source_leases)?;
    ensure_edge_files_absent(&paths.staging)?;

    let omitted_edge_files =
        names_with_disposition(&initial_entries, SourceEntryDisposition::OmitEdge);
    let omitted_control_files =
        names_with_disposition(&initial_entries, SourceEntryDisposition::OmitControl);
    let omitted_obsolete_block_files =
        names_with_disposition(&initial_entries, SourceEntryDisposition::OmitObsoleteBlock);
    let mut source_file_bindings = copied_source_files.clone();
    ensure!(
        source_file_bindings
            .insert(
                ARCHIVE_V2_BLOCKS_FILE.to_owned(),
                source_blocks_binding.clone(),
            )
            .is_none()
    );
    ensure!(
        source_file_bindings
            .insert(
                ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
                source_index_binding.clone(),
            )
            .is_none()
    );
    let mut target_file_bindings = copied_source_files.clone();
    let target_blocks_binding = FileBinding {
        bytes: rewrite.target_compressed_bytes,
        sha256: rewrite.target_blocks_sha256.clone(),
    };
    ensure!(
        target_file_bindings
            .insert(
                ARCHIVE_V2_BLOCKS_FILE.to_owned(),
                target_blocks_binding.clone(),
            )
            .is_none()
    );
    let target_index_binding = hash_regular_file(&paths.staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    ensure!(
        target_file_bindings
            .insert(
                ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
                target_index_binding.clone(),
            )
            .is_none()
    );
    let copied_durable_files = copied_source_files.keys().cloned().collect::<Vec<_>>();
    let edge_rebuild_required = EDGE_TIER_FILES
        .iter()
        .chain(LEGACY_EDGE_TIER_FILES.iter())
        .any(|name| initial_entries.contains_key(*name));

    if args.fast_candidate {
        ensure_candidate_canonical_controls_absent(&paths.staging)?;
        let scanner_report = scanner_report
            .as_ref()
            .context("fast candidate scanner report disappeared")?;
        let retained_durable_files = initial_entries
            .iter()
            .filter(|(name, entry)| {
                entry.disposition == SourceEntryDisposition::CopyDurable
                    && name.as_str() != BLOCK_TIME_GAP_FILE
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let retained_edge_files = [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ]
        .into_iter()
        .filter(|name| initial_entries.contains_key(*name))
        .map(str::to_owned)
        .collect::<Vec<_>>();
        let moved_to_backup = candidate_moved_files(&initial_entries);
        let candidate_edge_rebuild_required = moved_to_backup.iter().any(|name| {
            name == ARCHIVE_V2_GET_BLOCK_INDEX_FILE
                || LEGACY_EDGE_TIER_FILES.contains(&name.as_str())
        });
        let descriptor_binding = write_candidate_descriptor(
            &paths.staging.join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE),
            &args,
            &paths,
            &source_authority,
            source_authority_device_id,
            &source_file_bindings,
            &target_file_bindings,
            &initial_entries,
            scanner_report,
            source_level,
            target_level,
            &retained_durable_files,
            &retained_edge_files,
            &moved_to_backup,
            &rewrite,
        )?;
        make_files_read_only(
            &paths.staging,
            [
                ARCHIVE_V2_BLOCKS_FILE,
                ARCHIVE_V2_BLOCK_INDEX_FILE,
                PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE,
            ],
        )?;
        let intent = PairSwapIntent {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-pair-swap-intent".to_owned(),
            epoch: args.epoch,
            cluster_id: args.cluster_id.clone(),
            prospective_generation_id: args.generation_id.clone(),
            candidate: paths.source.display().to_string(),
            staging: paths.staging.display().to_string(),
            backup: paths
                .backup
                .as_ref()
                .context("fast candidate backup path disappeared")?
                .display()
                .to_string(),
            source_blocks: file_identity(&paths.source.join(ARCHIVE_V2_BLOCKS_FILE))?,
            source_blocks_binding: source_blocks_binding.clone(),
            source_index: file_identity(&paths.source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?,
            source_index_binding: source_index_binding.clone(),
            candidate_blocks: file_identity(&paths.staging.join(ARCHIVE_V2_BLOCKS_FILE))?,
            candidate_blocks_binding: target_blocks_binding,
            candidate_index: file_identity(&paths.staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?,
            candidate_index_binding: target_index_binding,
            candidate_descriptor: descriptor_binding.clone(),
            moved_to_backup: moved_to_backup.clone(),
            retained_edge_files: retained_edge_files.clone(),
            source_audit_report_path: scanner_report.path.clone(),
            source_audit_report: FileBinding {
                bytes: scanner_report.bytes,
                sha256: scanner_report.sha256.clone(),
            },
        };
        write_json_create_new(&paths.staging.join(PRE_TO_POST_SWITCH_INTENT_FILE), &intent)?;
        sync_generation(&paths.staging)?;
        complete_fast_pair_swap(&paths, &intent)?;
        verify_source_leases(&source_leases)?;
        ensure_in_place_candidate_inventory(&paths.source, &initial_entries, &moved_to_backup)?;

        return Ok(RunReport::Candidate(CandidateReport {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-candidate-report",
            state: "unfinalized",
            canonical: false,
            epoch: args.epoch,
            cluster_id: args.cluster_id,
            prospective_generation_id: args.generation_id,
            source: paths.source.display().to_string(),
            source_authority_kind: source_authority.kind_name(),
            source_authority_scope: source_authority.scope_name(),
            source_authority_filesystem: source_authority.filesystem_name(),
            source_authority_id: source_authority.id,
            source_authority_device_id,
            candidate: paths.source.display().to_string(),
            backup: paths
                .backup
                .as_ref()
                .context("fast candidate backup path disappeared")?
                .display()
                .to_string(),
            candidate_descriptor: paths
                .source
                .join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE)
                .display()
                .to_string(),
            candidate_descriptor_bytes: descriptor_binding.bytes,
            candidate_descriptor_sha256: descriptor_binding.sha256,
            source_profile_evidence: "external-whole-generation-scan-report",
            source_audit_report: scanner_report.path.clone(),
            source_audit_report_bytes: scanner_report.bytes,
            source_audit_report_sha256: scanner_report.sha256.clone(),
            expected_wire_profile_after_rewrite: "post-unknown-instruction-fallbacks-v1",
            source_scan_counts: scanner_report.counts.clone(),
            source_full_audit_performed_in_this_run: false,
            source_audit_report_reused: true,
            single_decode_rewrite_pass: true,
            outer_block_bytes_preserved_verbatim_except_messages: true,
            sidecars_copied: false,
            sidecars_rewritten: false,
            pair_swap_requires_external_reader_quiescence: true,
            archive_root_switch_lock: paths
                .parent
                .join(PRE_TO_POST_ROOT_SWITCH_LOCK_FILE)
                .display()
                .to_string(),
            zstd_level: target_level,
            rewrite,
            retained_durable_files,
            retained_edge_files,
            retained_edge_files_authoritative: false,
            retained_edge_validation_deferred: true,
            get_block_index_rebuild_required: moved_to_backup
                .iter()
                .any(|name| name == ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
            moved_to_backup,
            canonical_publication_deferred: true,
            target_post_audit_performed: false,
            canonical_manifest_written: false,
            canonical_profile_marker_written: false,
            canonical_migration_receipt_written: false,
            required_finalization: REQUIRED_CANDIDATE_FINALIZATION_STEPS,
            edge_rebuild_required: candidate_edge_rebuild_required,
            rewritten_files_read_only: true,
            source_provider_snapshot_required: false,
            source_linux_read_leases_required: true,
            elapsed_seconds: started.elapsed().as_secs_f64(),
        }));
    }

    let source_audit = source_audit.context("strict source audit disappeared")?;
    let receipt_path = paths.staging.join(PRE_TO_POST_RECEIPT_FILE);
    write_migration_receipt(
        &receipt_path,
        &args,
        &paths,
        &source_authority,
        source_authority_device_id,
        &source_file_bindings,
        &target_file_bindings,
        source_audit,
        source_level,
        target_level,
        &omitted_edge_files,
        &omitted_control_files,
        &omitted_obsolete_block_files,
        &rewrite,
    )?;

    let mut additional_files = manifest_additional_files(&copied_durable_files);
    additional_files.push(PRE_TO_POST_RECEIPT_FILE.to_owned());
    let manifest_path = generate_manifest(GenerateManifestOptions {
        archive_dir: paths.staging.clone(),
        cluster_id: args.cluster_id.clone(),
        epoch: args.epoch,
        generation_id: args.generation_id.clone(),
        slots_per_epoch: args.slots_per_epoch,
        wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        additional_files,
        output: None,
    })
    .context("generate, audit, and publish the private Post manifest")?;
    ensure!(
        manifest_path == paths.staging.join(GENERATION_MANIFEST_FILE),
        "gateway returned an unexpected manifest path"
    );
    ensure_edge_files_absent(&paths.staging)?;

    let manifest_bytes = fs::read(&manifest_path)
        .with_context(|| format!("read target manifest {}", manifest_path.display()))?;
    let target_manifest = GenerationManifest::parse(&manifest_bytes)
        .map_err(|error| anyhow!(error))
        .context("parse published target manifest")?;
    ensure_manifest_binds_target(&target_manifest, &target_file_bindings)?;
    ensure_exact_target_inventory(&paths.staging, &target_file_bindings)?;

    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed during target Post audit")?;
    ensure_source_directory_unchanged(&paths.source, &initial_entries)?;
    verify_source_leases(&source_leases)?;

    sync_generation(&paths.staging)?;
    let (files_read_only, directory_read_only) = make_generation_read_only(&paths.staging)?;
    sync_generation(&paths.staging)?;
    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed before final target publication")?;
    ensure_source_directory_unchanged(&paths.source, &initial_entries)?;
    verify_source_leases(&source_leases)?;
    publish_directory_no_replace(&paths.staging, &paths.target)?;
    sync_directory(&paths.parent)?;

    Ok(RunReport::Canonical(FinalReport {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-migration",
        epoch: args.epoch,
        cluster_id: args.cluster_id,
        generation_id: args.generation_id,
        source: paths.source.display().to_string(),
        source_authority_kind: source_authority.kind_name(),
        source_authority_scope: source_authority.scope_name(),
        source_authority_filesystem: source_authority.filesystem_name(),
        source_authority_device_id,
        source_authority_id: source_authority.id,
        target: paths.target.display().to_string(),
        staging: paths.staging.display().to_string(),
        source_profile: "pre-unknown-instruction-fallbacks-v1",
        target_profile: "post-unknown-instruction-fallbacks-v1",
        source_profile_decision: "unique-full-generation-decode",
        source_audit_blocks: source_audit.blocks,
        source_audit_typed_messages: source_audit.typed_messages,
        source_audit_selected_only: source_audit.selected_only,
        source_audit_both_equivalent: source_audit.both_semantically_equivalent,
        source_audit_both_divergent: source_audit.both_semantically_divergent,
        source_audit_raw_transaction_fallbacks: source_audit.raw_transaction_fallbacks,
        source_audit_raw_metadata_fallbacks: source_audit.raw_metadata_fallbacks,
        zstd_level: target_level,
        rewrite,
        copied_durable_files,
        omitted_edge_files,
        omitted_control_files,
        omitted_obsolete_block_files,
        target_post_audit_passed: true,
        target_manifest: paths
            .target
            .join(GENERATION_MANIFEST_FILE)
            .display()
            .to_string(),
        target_manifest_digest: target_manifest.generation_digest,
        migration_receipt: paths
            .target
            .join(PRE_TO_POST_RECEIPT_FILE)
            .display()
            .to_string(),
        edge_rebuild_required,
        staged_files_read_only: files_read_only,
        staged_directory_read_only: directory_read_only,
        target_provider_immutability_required: true,
        source_provider_snapshot_required: matches!(
            source_authority.kind,
            SourceAuthorityKind::ProviderSnapshot
        ),
        source_linux_read_leases_required: matches!(
            source_authority.kind,
            SourceAuthorityKind::LinuxReadLeases
        ),
        elapsed_seconds: started.elapsed().as_secs_f64(),
    }))
}

fn validate_paths(
    source: &Path,
    target: &Path,
    staging: &Path,
    epoch: u64,
    fast_candidate: bool,
) -> Result<ValidatedPaths> {
    ensure!(source.is_absolute(), "--source must be absolute");
    ensure!(target.is_absolute(), "--target must be absolute");
    ensure!(staging.is_absolute(), "--staging must be absolute");

    let source_metadata = fs::symlink_metadata(source)
        .with_context(|| format!("inspect source {}", source.display()))?;
    ensure!(
        source_metadata.is_dir() && !source_metadata.file_type().is_symlink(),
        "source must be a real directory"
    );
    let canonical_source = source
        .canonicalize()
        .with_context(|| format!("canonicalize source {}", source.display()))?;
    ensure!(
        canonical_source == source,
        "--source must already be canonical: {}",
        canonical_source.display()
    );

    let source_name = canonical_source
        .file_name()
        .and_then(|name| name.to_str())
        .context("source has no UTF-8 directory name")?;
    ensure!(
        source_name == format!("epoch-{epoch}"),
        "source basename must be epoch-{epoch}"
    );

    if fast_candidate {
        ensure!(
            target == canonical_source,
            "with --fast-candidate, --target must equal --source"
        );
        let parent = canonical_source.parent().context("source has no parent")?;
        let expected_staging = parent.join(format!(".epoch-{epoch}.pre-to-post.staging"));
        let backup = parent.join(format!(".epoch-{epoch}.pre-to-post.backup"));
        ensure!(
            staging == expected_staging,
            "--staging must be the exact private sibling {}",
            expected_staging.display()
        );
        let staging_exists = path_entry_exists_nofollow(staging)?;
        let backup_exists = path_entry_exists_nofollow(&backup)?;
        ensure!(
            !(staging_exists && backup_exists),
            "both staging and backup exist; refuse ambiguous pair-swap state"
        );
        if staging_exists {
            validate_recovery_workspace(staging, parent, "staging")?;
        }
        if backup_exists {
            validate_recovery_workspace(&backup, parent, "backup")?;
        }
        let parent = parent.to_path_buf();
        return Ok(ValidatedPaths {
            source: canonical_source.clone(),
            target: canonical_source,
            staging: expected_staging,
            parent,
            backup: Some(backup),
        });
    }

    let target_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .context("target has no UTF-8 directory name")?;
    ensure!(
        target_name == format!("epoch-{epoch}"),
        "target basename must be epoch-{epoch}"
    );
    let target_parent = target.parent().context("target has no parent")?;
    let canonical_parent = target_parent
        .canonicalize()
        .with_context(|| format!("canonicalize target parent {}", target_parent.display()))?;
    ensure!(
        canonical_parent == target_parent,
        "target parent is not canonical"
    );
    let canonical_target = canonical_parent.join(target_name);
    ensure!(canonical_target == target, "--target is not canonical");
    ensure!(
        !target.try_exists()?,
        "target already exists and will not be replaced: {}",
        target.display()
    );
    let expected_staging = canonical_parent.join(format!(".{target_name}.pre-to-post.staging"));
    ensure!(
        staging == expected_staging,
        "--staging must be the exact private sibling {}",
        expected_staging.display()
    );
    ensure!(
        !staging.try_exists()?,
        "staging already exists and will not be reused: {}",
        staging.display()
    );
    ensure!(
        canonical_source != canonical_target
            && canonical_source != expected_staging
            && !canonical_target.starts_with(&canonical_source)
            && !expected_staging.starts_with(&canonical_source),
        "source, target, and staging must be distinct non-nested directories"
    );
    ensure!(
        canonical_source.parent() != Some(canonical_parent.as_path()),
        "target must be under a separate target root, not beside the source epoch"
    );
    Ok(ValidatedPaths {
        source: canonical_source,
        target: canonical_target,
        staging: expected_staging,
        parent: canonical_parent,
        backup: None,
    })
}

fn path_entry_exists_nofollow(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect path entry {}", path.display())),
    }
}

#[cfg(unix)]
fn validate_recovery_workspace(path: &Path, parent: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect fast-candidate {label} {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "fast-candidate {label} must be a real directory"
    );
    ensure!(
        path.canonicalize()? == path,
        "fast-candidate {label} must be its exact canonical path"
    );
    ensure!(
        metadata.dev() == fs::symlink_metadata(parent)?.dev(),
        "fast-candidate {label} must be on the archive-root filesystem"
    );
    ensure!(
        metadata.permissions().mode() & 0o777 == 0o700,
        "fast-candidate {label} must have private mode 0700"
    );
    Ok(())
}

#[cfg(not(unix))]
fn validate_recovery_workspace(_path: &Path, _parent: &Path, _label: &str) -> Result<()> {
    anyhow::bail!("fast-candidate recovery requires Unix")
}

fn inspect_and_pin_source(
    source_dir: &Path,
    source: &PinnedLocalRangeSource,
    authority: SourceAuthorityKind,
) -> Result<BTreeMap<String, SourceEntry>> {
    if authority == SourceAuthorityKind::ProviderSnapshot {
        require_read_only_source_directory(source_dir)?;
    }
    let mut entries = BTreeMap::new();
    for entry in fs::read_dir(source_dir)
        .with_context(|| format!("enumerate source {}", source_dir.display()))?
    {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("source contains a non-UTF-8 entry name"))?;
        let metadata = fs::symlink_metadata(entry.path())
            .with_context(|| format!("inspect source object {name}"))?;
        ensure!(
            metadata.file_type().is_file(),
            "source object is not a real regular file: {name}"
        );
        if authority == SourceAuthorityKind::ProviderSnapshot {
            require_read_only_source_file(&entry.path(), &name)?;
        }
        let disposition = source_entry_disposition(&name).with_context(|| {
            format!("unknown source data object {name:?}; review it explicitly")
        })?;
        let pinned = source
            .open_file(&name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("pin source object {name}"))?;
        let pinned_metadata = pinned.metadata()?;
        ensure!(
            pinned_metadata.is_file() && pinned_metadata.len() == metadata.len(),
            "source object changed while it was pinned: {name}"
        );
        ensure!(
            entries
                .insert(
                    name,
                    SourceEntry {
                        bytes: metadata.len(),
                        disposition,
                    },
                )
                .is_none(),
            "source contains a duplicate entry name"
        );
    }
    for name in reviewed_source_names() {
        let pinned_size = source
            .size(&name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("pin reviewed optional source object {name}"))?;
        let inventoried_size = entries.get(&name).map(|entry| entry.bytes);
        ensure!(
            pinned_size == inventoried_size,
            "source inventory changed while reviewed optional object {name} was pinned"
        );
    }
    Ok(entries)
}

fn reviewed_source_names() -> BTreeSet<String> {
    let mut names = BTreeSet::from([
        ARCHIVE_V2_BLOCKS_FILE.to_owned(),
        ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
    ]);
    names.extend(DURABLE_COPY_FILES.iter().map(|name| (*name).to_owned()));
    names.extend(EDGE_TIER_FILES.iter().map(|name| (*name).to_owned()));
    names.extend(LEGACY_EDGE_TIER_FILES.iter().map(|name| (*name).to_owned()));
    names.extend(OBSOLETE_BLOCK_FILES.iter().map(|name| (*name).to_owned()));
    names.extend(source_control_names());
    names
}

#[cfg(unix)]
fn require_read_only_source_directory(path: &Path) -> Result<()> {
    let path_c = CString::new(path.as_os_str().as_bytes()).context("source path contains NUL")?;
    // SAFETY: `path_c` is a live NUL-terminated path. `access` does not change the directory.
    if unsafe { libc::access(path_c.as_ptr(), libc::W_OK) } == 0 {
        anyhow::bail!(
            "source directory is writable; use a provider-enforced read-only snapshot: {}",
            path.display()
        );
    }
    let error = io::Error::last_os_error();
    ensure!(
        matches!(
            error.raw_os_error(),
            Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::EROFS)
        ),
        "cannot verify read-only source directory {}: {error}",
        path.display()
    );
    Ok(())
}

#[cfg(not(unix))]
fn require_read_only_source_directory(path: &Path) -> Result<()> {
    anyhow::bail!(
        "read-only snapshot admission is unsupported for source directory {}",
        path.display()
    )
}

#[cfg(unix)]
fn require_read_only_source_file(path: &Path, name: &str) -> Result<()> {
    let opened = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path);
    match opened {
        Ok(file) => {
            drop(file);
            anyhow::bail!(
                "source object is writable; use a provider-enforced read-only snapshot: {name}"
            )
        }
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::EROFS)
            ) =>
        {
            Ok(())
        }
        Err(error) => {
            Err(error).with_context(|| format!("verify read-only snapshot source object {name}"))
        }
    }
}

#[cfg(not(unix))]
fn require_read_only_source_file(_path: &Path, name: &str) -> Result<()> {
    anyhow::bail!("read-only snapshot admission is unsupported for source object {name}")
}

fn source_entry_disposition(name: &str) -> Option<SourceEntryDisposition> {
    if matches!(name, ARCHIVE_V2_BLOCKS_FILE | ARCHIVE_V2_BLOCK_INDEX_FILE) {
        return Some(SourceEntryDisposition::Rewrite);
    }
    if DURABLE_COPY_FILES.contains(&name) {
        return Some(SourceEntryDisposition::CopyDurable);
    }
    if EDGE_TIER_FILES.contains(&name) || LEGACY_EDGE_TIER_FILES.contains(&name) {
        return Some(SourceEntryDisposition::OmitEdge);
    }
    if SOURCE_CONTROL_FILES.contains(&name)
        || LEGACY_SOURCE_CONTROL_FILES.contains(&name)
        || name == wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name
        || name == wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name
    {
        return Some(SourceEntryDisposition::OmitControl);
    }
    if OBSOLETE_BLOCK_FILES.contains(&name) {
        return Some(SourceEntryDisposition::OmitObsoleteBlock);
    }
    None
}

fn validate_required_source_entries(
    entries: &BTreeMap<String, SourceEntry>,
    epoch: u64,
) -> Result<()> {
    for required in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        ensure!(
            entries.contains_key(required),
            "required source object is missing: {required}"
        );
    }
    if epoch == 0 && entries.contains_key(ARCHIVE_V2_GENESIS_BIN_FILE) {
        ensure!(
            entries[ARCHIVE_V2_GENESIS_BIN_FILE].disposition == SourceEntryDisposition::CopyDurable,
            "epoch-0 genesis.bin has an invalid disposition"
        );
    }
    ensure!(
        epoch == 0 || !entries.contains_key(ARCHIVE_V2_GENESIS_BIN_FILE),
        "nonzero epoch contains genesis.bin, which the canonical manifest would not publish"
    );
    Ok(())
}

fn ensure_source_directory_unchanged(
    source_dir: &Path,
    initial: &BTreeMap<String, SourceEntry>,
) -> Result<()> {
    let mut actual = BTreeMap::new();
    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("source gained a non-UTF-8 entry"))?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.file_type().is_file(),
            "source entry type changed: {name}"
        );
        actual.insert(name, metadata.len());
    }
    let expected = initial
        .iter()
        .map(|(name, entry)| (name.clone(), entry.bytes))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        actual == expected,
        "source directory changed during migration"
    );
    Ok(())
}

fn preflight_free_space(parent: &Path, copied_bytes: u64, source_blocks_bytes: u64) -> Result<()> {
    let percent_margin = source_blocks_bytes / 100;
    let margin = percent_margin.clamp(FREE_SPACE_MARGIN_MIN_BYTES, FREE_SPACE_MARGIN_MAX_BYTES);
    let required = copied_bytes
        .checked_add(source_blocks_bytes)
        .and_then(|bytes| bytes.checked_add(margin))
        .context("target free-space requirement overflow")?;
    let available = available_space(parent)?;
    ensure!(
        available >= required,
        "target filesystem has {available} free bytes; migration requires at least {required} bytes (durable copies={copied_bytes}, target blocks allowance={source_blocks_bytes}, margin={margin})"
    );
    Ok(())
}

#[cfg(unix)]
fn available_space(path: &Path) -> Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes()).context("target parent contains NUL")?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stat` points to writable storage.
    ensure!(
        unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } == 0,
        "statvfs failed: {}",
        io::Error::last_os_error()
    );
    // SAFETY: statvfs returned success and initialized the structure.
    let stat = unsafe { stat.assume_init() };
    let available_blocks = unsigned_fs_value(stat.f_bavail);
    let fragment_bytes = unsigned_fs_value(stat.f_frsize);
    available_blocks
        .checked_mul(fragment_bytes)
        .context("available target byte count overflow")
}

#[cfg(unix)]
fn unsigned_fs_value<T: Into<u64>>(value: T) -> u64 {
    value.into()
}

#[cfg(not(unix))]
fn available_space(_path: &Path) -> Result<u64> {
    bail!("target free-space preflight is unsupported on this operating system")
}

fn create_private_staging(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder
        .create(path)
        .with_context(|| format!("create private staging {}", path.display()))?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("set private staging mode on {}", path.display()))?;
    #[cfg(unix)]
    ensure!(
        fs::symlink_metadata(path)?.permissions().mode() & 0o777 == 0o700,
        "staging directory does not have mode 0700"
    );
    sync_directory(path.parent().context("staging has no parent")?)
}

fn copy_durable_files(
    source_dir: &Path,
    staging: &Path,
    source: &PinnedLocalRangeSource,
    entries: &BTreeMap<String, SourceEntry>,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut copied = BTreeMap::new();
    for (name, entry) in entries {
        if entry.disposition != SourceEntryDisposition::CopyDurable {
            continue;
        }
        let pinned = source
            .open_file(name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("reopen pinned durable source {name}"))?;
        let binding = clone_or_copy_pinned(
            &source_dir.join(name),
            pinned,
            &staging.join(name),
            entry.bytes,
        )
        .with_context(|| format!("preserve durable object {name}"))?;
        ensure!(
            binding.bytes == entry.bytes,
            "copied durable object {name} has an unexpected source byte count"
        );
        copied.insert(name.clone(), binding);
    }
    Ok(copied)
}

fn hash_regular_file(path: &Path) -> Result<FileBinding> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .with_context(|| format!("open {} for hashing", path.display()))?;
    hash_open_file(file)
}

fn hash_open_file(mut file: File) -> Result<FileBinding> {
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "hashed object is not a regular file");
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = std::io::Read::read(&mut reader, &mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("hashed object byte count overflow")?;
    }
    ensure!(
        bytes == metadata.len(),
        "object changed length while hashing"
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_lower(&hasher.finalize()),
    })
}

fn admit_scanner_report(
    path: &Path,
    expected_sha256: &str,
    source: &Path,
    epoch: u64,
) -> Result<ScannerReportBinding> {
    ensure_sha256_hex(expected_sha256, "expected scanner report SHA-256")?;
    ensure!(path.is_absolute(), "--source-audit-report must be absolute");
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect scanner report {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "scanner report must be a real regular file"
    );
    let canonical = path
        .canonicalize()
        .with_context(|| format!("canonicalize scanner report {}", path.display()))?;
    ensure!(
        canonical == path,
        "--source-audit-report must already be canonical: {}",
        canonical.display()
    );
    let bytes =
        fs::read(path).with_context(|| format!("read scanner report {}", path.display()))?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "scanner report changed length while it was read"
    );
    let actual_sha256 = hex_lower(&Sha256::digest(&bytes));
    ensure!(
        actual_sha256 == expected_sha256,
        "scanner report SHA-256 differs from --source-audit-report-sha256"
    );
    let report: ScannerReportDocument =
        serde_json::from_slice(&bytes).context("parse exact scanner report schema")?;
    ensure!(
        report.schema_version == 1,
        "unsupported scanner report schema"
    );
    ensure!(
        report.kind == "archive-v2-wire-profile-scan",
        "unexpected scanner report kind"
    );
    ensure!(report.epoch == epoch, "scanner report epoch differs");
    ensure!(
        report.archive == source.display().to_string(),
        "scanner report archive path differs from --source"
    );
    ensure!(report.error.is_none(), "scanner report contains an error");
    ensure!(report.workers > 0, "scanner report has no workers");
    ensure!(
        report.elapsed_seconds.is_finite() && report.elapsed_seconds >= 0.0,
        "scanner report elapsed time is invalid"
    );
    ensure!(
        report.classification == "legacy-pre" && report.action == "convert-to-post",
        "scanner report does not admit an exact LegacyPre conversion"
    );
    let counts = &report.counts;
    ensure!(
        counts.blocks > 0
            && counts.typed_messages > 0
            && counts.pre_only > 0
            && counts.post_only == 0
            && counts.owned_fallback_blocks == 0
            && counts.raw_transaction_fallbacks == 0
            && counts.both_divergent == 0
            && counts.invalid == 0
            && counts.typed_messages
                == counts
                    .pre_only
                    .checked_add(counts.both_equivalent)
                    .context("scanner typed-message count overflow")?,
        "scanner report counts do not prove one valid zero-fallback LegacyPre generation"
    );
    let pre_evidence = report
        .first_evidence
        .pre_only
        .as_ref()
        .context("scanner report has no first LegacyPre evidence")?;
    let _ = (pre_evidence.slot, pre_evidence.transaction_index);
    ensure!(
        report.first_evidence.post_only.is_none()
            && report.first_evidence.both_divergent.is_none()
            && report.first_evidence.invalid.is_none(),
        "scanner report evidence conflicts with LegacyPre admission"
    );
    Ok(ScannerReportBinding {
        path: path.display().to_string(),
        bytes: bytes.len() as u64,
        sha256: actual_sha256,
        completed_unix_seconds: report.completed_unix_seconds,
        workers: report.workers,
        counts: report.counts,
    })
}

fn ensure_sha256_hex(value: &str, label: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} must be exactly 64 lowercase hexadecimal characters"
    );
    Ok(())
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

#[cfg(target_os = "macos")]
fn clone_or_copy_pinned(
    _source_path: &Path,
    source_file: File,
    target: &Path,
    expected_bytes: u64,
) -> Result<FileBinding> {
    copy_pinned_file(source_file, target, expected_bytes)
}

#[cfg(target_os = "linux")]
fn clone_or_copy_pinned(
    _source_path: &Path,
    mut source_file: File,
    target: &Path,
    expected_bytes: u64,
) -> Result<FileBinding> {
    use std::os::fd::AsRawFd;
    let target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    // SAFETY: both descriptors are live regular files for the duration of FICLONE.
    if unsafe {
        libc::ioctl(
            target_file.as_raw_fd(),
            libc::FICLONE,
            source_file.as_raw_fd(),
        )
    } == 0
    {
        ensure!(target_file.metadata()?.len() == expected_bytes);
        target_file.sync_all()?;
        let binding = hash_open_file(source_file)?;
        ensure!(binding.bytes == expected_bytes);
        return Ok(binding);
    }
    target_file.set_len(0)?;
    source_file.seek(SeekFrom::Start(0))?;
    copy_open_files(source_file, target_file, expected_bytes)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn clone_or_copy_pinned(
    _source_path: &Path,
    source_file: File,
    target: &Path,
    expected_bytes: u64,
) -> Result<FileBinding> {
    copy_pinned_file(source_file, target, expected_bytes)
}

#[cfg(not(target_os = "linux"))]
fn copy_pinned_file(source_file: File, target: &Path, expected_bytes: u64) -> Result<FileBinding> {
    let target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    copy_open_files(source_file, target_file, expected_bytes)
}

fn copy_open_files(mut source: File, mut target: File, expected_bytes: u64) -> Result<FileBinding> {
    source.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut copied = 0u64;
    loop {
        let read = std::io::Read::read(&mut source, &mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        target.write_all(&buffer[..read])?;
        copied = copied
            .checked_add(read as u64)
            .context("copied durable object byte count overflow")?;
    }
    ensure!(
        copied == expected_bytes,
        "pinned source changed length while copying: copied={copied} expected={expected_bytes}"
    );
    target.sync_all()?;
    Ok(FileBinding {
        bytes: copied,
        sha256: hex_lower(&hasher.finalize()),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FastRewriteResources {
    threads: usize,
    output_buffer_count: usize,
    retained_decompressed_bytes_per_worker: usize,
    retained_work_buffer_bytes: usize,
    retained_output_buffer_bytes: usize,
}

fn fast_rewrite_resources(threads: usize) -> Result<FastRewriteResources> {
    ensure!(threads != 0, "--threads must be positive");
    ensure!(
        threads <= MAX_ORDERED_PARALLEL_DECODE_WORKERS,
        "--threads {threads} exceeds the {MAX_ORDERED_PARALLEL_DECODE_WORKERS} worker limit"
    );
    let output_buffer_count = FAST_OUTPUT_BUFFER_COUNT;

    // Reusable buffers stay below 896 MiB at every accepted worker count:
    // at most 512 MiB for SDK decompression buffers, 128 MiB for two
    // converter work buffers per worker, and 256 MiB for 8,192 compressed
    // output tokens retained at 32 KiB each. The high token count preserves
    // 16 MiB sequential reads for archives with very small compressed frames.
    // One active 512 MiB declared-uncompressed batch and one admitted
    // oversized block can add temporary memory above this retained bound.
    const DECOMPRESSED_RETAINED_BUDGET: usize = 512 << 20;
    const WORK_RETAINED_BUDGET: usize = 128 << 20;
    let retained_decompressed_bytes_per_worker = FAST_RETAINED_DECOMPRESSED_BYTES_PER_WORKER.min(
        DECOMPRESSED_RETAINED_BUDGET
            .checked_div(threads)
            .context("fast decompression worker count must be positive")?,
    );
    let work_buffers = threads
        .checked_mul(2)
        .context("fast work buffer count overflow")?;
    let retained_work_buffer_bytes = FAST_RETAINED_WORK_BUFFER_BYTES.min(
        WORK_RETAINED_BUDGET
            .checked_div(work_buffers)
            .context("fast work buffer count must be positive")?,
    );
    let configured_retained = retained_decompressed_bytes_per_worker
        .checked_mul(threads)
        .and_then(|bytes| {
            retained_work_buffer_bytes
                .checked_mul(work_buffers)
                .and_then(|work| bytes.checked_add(work))
        })
        .and_then(|bytes| {
            FAST_RETAINED_OUTPUT_BUFFER_BYTES
                .checked_mul(output_buffer_count)
                .and_then(|output| bytes.checked_add(output))
        })
        .context("fast configured retained byte count overflow")?;
    ensure!(
        configured_retained < MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES,
        "fast configured retained buffers exceed the 1 GiB safety bound"
    );
    Ok(FastRewriteResources {
        threads,
        output_buffer_count,
        retained_decompressed_bytes_per_worker,
        retained_work_buffer_bytes,
        retained_output_buffer_bytes: FAST_RETAINED_OUTPUT_BUFFER_BYTES,
    })
}

struct FastRewriteWorker {
    compressor: std::result::Result<zstd::bulk::Compressor<'static>, String>,
    target_messages: Vec<u8>,
    serialized: Vec<u8>,
}

impl FastRewriteWorker {
    fn new(zstd_level: i32) -> Self {
        Self {
            compressor: zstd::bulk::Compressor::new(zstd_level)
                .map_err(|error| format!("create target zstd compressor: {error}")),
            target_messages: Vec::new(),
            serialized: Vec::new(),
        }
    }

    fn release_large_work_buffers(&mut self, retained_bytes: usize) {
        release_large_vec(&mut self.target_messages, retained_bytes);
        release_large_vec(&mut self.serialized, retained_bytes);
    }
}

struct FastRewriteOutput {
    source_row: blockzilla_archive_v2::ArchiveV2HotBlockIndexRow,
    metadata_bytes: u64,
    message_stats: MessageRegionStats,
    compressed: FastOutputToken,
}

#[derive(Debug)]
struct FastParallelRewrite {
    report: RewriteReport,
    pipeline: OrderedParallelBlockStats,
}

fn release_large_vec(bytes: &mut Vec<u8>, retained_bytes: usize) {
    bytes.clear();
    if bytes.capacity() > retained_bytes {
        *bytes = Vec::new();
    }
}

fn clear_and_reserve_exact(bytes: &mut Vec<u8>, required_capacity: usize) -> Result<()> {
    bytes.clear();
    if bytes.capacity() < required_capacity {
        // Vec::try_reserve_exact() takes an additional length, not an
        // additional capacity. The vector is empty here, so request the full
        // required capacity even when a smaller allocation is being reused.
        bytes
            .try_reserve_exact(required_capacity)
            .context("reserve target zstd buffer")?;
    }
    Ok(())
}

struct FastOutputToken {
    bytes: Option<Vec<u8>>,
    retained_bytes: usize,
    recycler: SyncSender<Vec<u8>>,
}

impl FastOutputToken {
    fn new(bytes: Vec<u8>, retained_bytes: usize, recycler: SyncSender<Vec<u8>>) -> Self {
        Self {
            bytes: Some(bytes),
            retained_bytes,
            recycler,
        }
    }

    fn bytes(&self) -> &[u8] {
        self.bytes.as_deref().expect("fast output token has bytes")
    }

    fn bytes_mut(&mut self) -> &mut Vec<u8> {
        self.bytes.as_mut().expect("fast output token has bytes")
    }
}

impl Drop for FastOutputToken {
    fn drop(&mut self) {
        let Some(mut bytes) = self.bytes.take() else {
            return;
        };
        release_large_vec(&mut bytes, self.retained_bytes);
        let _ = self.recycler.send(bytes);
    }
}

fn fast_reader_error(slot: u64, error: impl std::fmt::Display) -> ReaderError {
    ReaderError::InvalidBlock {
        slot,
        message: error.to_string(),
    }
}

fn project_fast_block(
    worker: &mut FastRewriteWorker,
    block: BorrowedDecodedBlock<'_>,
    limits: ArchiveV2WireRewriteLimits,
    retained_work_buffer_bytes: usize,
    mut compressed: FastOutputToken,
) -> std::result::Result<FastRewriteOutput, ReaderError> {
    let source_row = block.index_row;
    let slot = source_row.slot;
    let projected = (|| -> Result<(MessageRegionStats, u64)> {
        ensure!(
            !block.uses_owned_fallback(),
            "slot {slot} uses a historical outer block schema; this byte-minimal converter accepts only the current outer schema"
        );
        let message_stats = rewrite_message_region(
            block.tx_rows(),
            block.message_bytes(),
            &mut worker.target_messages,
            limits,
            slot,
        )?;
        ensure!(
            message_stats.typed_messages == u64::from(block.tx_count()),
            "slot {slot} transaction-row count differs from its header"
        );
        ensure!(
            message_stats.raw_transaction_fallbacks == 0,
            "slot {slot} contains a raw transaction fallback after the zero-fallback source audit"
        );

        let uncompressed = block.uncompressed_bytes();
        let message_range = borrowed_subslice_range(uncompressed, block.message_bytes())?;
        ensure!(
            message_range.len() == worker.target_messages.len(),
            "slot {slot} changed the borrowed message region length"
        );
        worker.serialized.clear();
        if worker.serialized.capacity() < uncompressed.len() {
            worker
                .serialized
                .try_reserve_exact(uncompressed.len() - worker.serialized.capacity())
                .context("reserve verbatim target block")?;
        }
        worker.serialized.extend_from_slice(uncompressed);
        worker.serialized[message_range].copy_from_slice(&worker.target_messages);
        ensure!(
            worker.serialized.len() == source_row.uncompressed_len as usize,
            "slot {slot} changed outer uncompressed length: source={} target={}",
            source_row.uncompressed_len,
            worker.serialized.len()
        );

        let compressed_bytes = compressed.bytes_mut();
        let compress_bound = zstd::zstd_safe::compress_bound(worker.serialized.len());
        clear_and_reserve_exact(compressed_bytes, compress_bound)?;
        let compressor = worker
            .compressor
            .as_mut()
            .map_err(|error| anyhow!(error.clone()))?;
        let compressed_len = compressor
            .compress_to_buffer(&worker.serialized, compressed_bytes)
            .with_context(|| format!("compress target slot {slot}"))?;
        ensure!(compressed_len == compressed_bytes.len());
        u32::try_from(compressed_bytes.len())
            .context("target compressed block exceeds u32::MAX")?;
        Ok((message_stats, block.metadata_bytes().len() as u64))
    })();

    worker.release_large_work_buffers(retained_work_buffer_bytes);
    match projected {
        Ok((message_stats, metadata_bytes)) => Ok(FastRewriteOutput {
            source_row,
            metadata_bytes,
            message_stats,
            compressed,
        }),
        Err(error) => Err(fast_reader_error(slot, format!("{error:#}"))),
    }
}

fn rewrite_blocks_fast_parallel<S: RangeSource>(
    reader: &ArchiveReader<S>,
    staging: &Path,
    zstd_level: i32,
    max_message_bytes: usize,
    progress_blocks: u64,
    threads: usize,
) -> Result<FastParallelRewrite> {
    let resources = fast_rewrite_resources(threads)?;
    let target_blocks_path = staging.join(ARCHIVE_V2_BLOCKS_FILE);
    let target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target_blocks_path)
        .with_context(|| format!("create {}", target_blocks_path.display()))?;
    let mut target_file = BufWriter::with_capacity(IO_BUFFER_BYTES, target_file);
    let mut target_rows = Vec::with_capacity(reader.index().rows.len());
    let mut target_blocks_hasher = Sha256::new();
    let mut report = RewriteReport::default();
    let started = Instant::now();
    let limits = ArchiveV2WireRewriteLimits {
        max_input_bytes: max_message_bytes,
        max_output_bytes: max_message_bytes,
        ..ArchiveV2WireRewriteLimits::default()
    };

    let (free_output_sender, free_output_receiver) = sync_channel(resources.output_buffer_count);
    for _ in 0..resources.output_buffer_count {
        free_output_sender
            .send(Vec::new())
            .expect("the new fast output-buffer channel has a receiver");
    }
    let free_output_receiver = Arc::new(Mutex::new(free_output_receiver));
    let project_receiver = Arc::clone(&free_output_receiver);
    let project_recycler = free_output_sender.clone();

    let config = OrderedParallelBlockConfig {
        compressed_batch_target_bytes: FAST_COMPRESSED_BATCH_TARGET_BYTES,
        uncompressed_batch_budget_bytes: FAST_UNCOMPRESSED_BATCH_BUDGET_BYTES,
        max_blocks_per_batch: resources.output_buffer_count,
        compressed_buffer_count: FAST_COMPRESSED_INPUT_BUFFERS,
        decode_workers: resources.threads,
        retained_decompressed_bytes_per_worker: resources.retained_decompressed_bytes_per_worker,
        discard_rewards: true,
    };
    let pipeline = reader
        .process_borrowed_blocks_parallel_ordered(
            0..reader.index().rows.len(),
            config,
            |_| FastRewriteWorker::new(zstd_level),
            move |worker, _row_number, block| {
                let compressed = project_receiver
                    .lock()
                    .map_err(|_| {
                        fast_reader_error(block.header().slot, "fast output pool is poisoned")
                    })?
                    .recv()
                    .map_err(|_| {
                        fast_reader_error(block.header().slot, "fast output pool disconnected")
                    })?;
                let compressed = FastOutputToken::new(
                    compressed,
                    resources.retained_output_buffer_bytes,
                    project_recycler.clone(),
                );
                project_fast_block(
                    worker,
                    block,
                    limits,
                    resources.retained_work_buffer_bytes,
                    compressed,
                )
            },
            |_row_number, output| {
                let source_row = output.source_row;
                let metadata_bytes = output.metadata_bytes;
                let write_result = (|| -> Result<()> {
                    let compressed_len = u32::try_from(output.compressed.bytes().len())
                        .context("target compressed block exceeds u32::MAX")?;
                    target_file
                        .write_all(output.compressed.bytes())
                        .with_context(|| format!("write target slot {}", source_row.slot))?;
                    target_blocks_hasher.update(output.compressed.bytes());

                    let mut target_row = source_row;
                    target_row.compressed_offset = report.target_compressed_bytes;
                    target_row.compressed_len = compressed_len;
                    target_row.uncompressed_len = source_row.uncompressed_len;
                    target_rows.push(target_row);
                    accumulate_rewrite_report(
                        &mut report,
                        source_row,
                        compressed_len,
                        metadata_bytes,
                        output.message_stats,
                    )?;
                    print_rewrite_progress(
                        &report,
                        reader.index().rows.len(),
                        reader.index().blob_file_bytes,
                        progress_blocks,
                        started,
                    );
                    Ok(())
                })();
                write_result
                    .map_err(|error| fast_reader_error(source_row.slot, format!("{error:#}")))
            },
        )
        .map_err(|error| anyhow!(error))?;

    finalize_rewrite_report(reader, &mut report)?;
    target_file.flush()?;
    target_file.get_ref().sync_all()?;
    drop(target_file);
    write_archive_v2_hot_block_index(
        &staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        report.target_compressed_bytes,
        zstd_level,
        reader.index().flags,
        &target_rows,
    )?;
    report.target_blocks_sha256 = hex_lower(&target_blocks_hasher.finalize());
    Ok(FastParallelRewrite { report, pipeline })
}

fn accumulate_rewrite_report(
    report: &mut RewriteReport,
    source_row: blockzilla_archive_v2::ArchiveV2HotBlockIndexRow,
    compressed_len: u32,
    metadata_bytes: u64,
    message_stats: MessageRegionStats,
) -> Result<()> {
    report.blocks = report
        .blocks
        .checked_add(1)
        .context("block count overflow")?;
    report.borrowed_current_blocks = report
        .borrowed_current_blocks
        .checked_add(1)
        .context("borrowed block count overflow")?;
    report.typed_messages = report
        .typed_messages
        .checked_add(message_stats.typed_messages)
        .context("typed message count overflow")?;
    report.raw_transaction_fallbacks = report
        .raw_transaction_fallbacks
        .checked_add(message_stats.raw_transaction_fallbacks)
        .context("raw transaction fallback count overflow")?;
    report.message_input_bytes = report
        .message_input_bytes
        .checked_add(message_stats.input_bytes)
        .context("message input byte count overflow")?;
    report.message_output_bytes = report
        .message_output_bytes
        .checked_add(message_stats.output_bytes)
        .context("message output byte count overflow")?;
    report.message_mismatch_bytes = report
        .message_mismatch_bytes
        .checked_add(message_stats.mismatch_bytes)
        .context("message mismatch byte count overflow")?;
    for (target, source) in report
        .source_instruction_data_tag_counts
        .iter_mut()
        .zip(message_stats.source_instruction_data_tag_counts)
    {
        *target = target
            .checked_add(source)
            .context("source instruction tag count overflow")?;
    }
    report.metadata_input_bytes = report
        .metadata_input_bytes
        .checked_add(metadata_bytes)
        .context("metadata input byte count overflow")?;
    report.metadata_output_bytes = report
        .metadata_output_bytes
        .checked_add(metadata_bytes)
        .context("metadata output byte count overflow")?;
    report.source_compressed_bytes = report
        .source_compressed_bytes
        .checked_add(u64::from(source_row.compressed_len))
        .context("source compressed byte count overflow")?;
    report.target_compressed_bytes = report
        .target_compressed_bytes
        .checked_add(u64::from(compressed_len))
        .context("target compressed byte count overflow")?;
    report.uncompressed_bytes = report
        .uncompressed_bytes
        .checked_add(u64::from(source_row.uncompressed_len))
        .context("uncompressed byte count overflow")?;
    Ok(())
}

fn print_rewrite_progress(
    report: &RewriteReport,
    total_blocks: usize,
    source_total_bytes: u64,
    progress_blocks: u64,
    started: Instant,
) {
    if progress_blocks == 0
        || (!report.blocks.is_multiple_of(progress_blocks) && report.blocks != total_blocks as u64)
    {
        return;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let rate = if elapsed == 0.0 {
        0.0
    } else {
        report.source_compressed_bytes as f64 / elapsed
    };
    let remaining = source_total_bytes.saturating_sub(report.source_compressed_bytes);
    let eta_seconds = if rate == 0.0 {
        None
    } else {
        Some(remaining as f64 / rate)
    };
    eprintln!(
        "{{\"kind\":\"archive-v2-pre-to-post-progress\",\"blocks\":{},\"total_blocks\":{},\"source_bytes\":{},\"target_bytes\":{},\"mib_per_second\":{:.2},\"eta_seconds\":{}}}",
        report.blocks,
        total_blocks,
        report.source_compressed_bytes,
        report.target_compressed_bytes,
        rate / (1 << 20) as f64,
        eta_seconds
            .map(|value| format!("{value:.1}"))
            .unwrap_or_else(|| "null".to_owned()),
    );
}

fn finalize_rewrite_report<S: RangeSource>(
    reader: &ArchiveReader<S>,
    report: &mut RewriteReport,
) -> Result<()> {
    ensure!(
        report.blocks == reader.index().rows.len() as u64,
        "rewrite visited {} of {} blocks",
        report.blocks,
        reader.index().rows.len()
    );
    ensure!(
        report.source_compressed_bytes == reader.index().blob_file_bytes,
        "rewrite source-byte coverage differs from the admitted index"
    );
    ensure!(
        report.message_input_bytes == report.message_output_bytes,
        "aggregate message byte length changed"
    );
    let expected_mismatches = report.source_instruction_data_tag_counts[1..=6]
        .iter()
        .try_fold(0u64, |total, count| total.checked_add(*count))
        .context("expected mismatch count overflow")?;
    ensure!(
        report.source_instruction_data_tag_counts[7..]
            .iter()
            .all(|count| *count == 0)
            && report.message_mismatch_bytes == expected_mismatches,
        "aggregate Pre-to-Post tag delta proof failed"
    );
    report.metadata_regions_byte_identical = true;
    Ok(())
}

fn rewrite_blocks<S: RangeSource>(
    reader: &ArchiveReader<S>,
    staging: &Path,
    zstd_level: i32,
    max_message_bytes: usize,
    progress_blocks: u64,
    preserve_outer_bytes: bool,
) -> Result<RewriteReport> {
    let target_blocks_path = staging.join(ARCHIVE_V2_BLOCKS_FILE);
    let target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target_blocks_path)
        .with_context(|| format!("create {}", target_blocks_path.display()))?;
    let mut target_file = BufWriter::with_capacity(IO_BUFFER_BYTES, target_file);
    let mut compressor =
        zstd::bulk::Compressor::new(zstd_level).context("create target zstd compressor")?;
    let mut target_messages = Vec::new();
    let mut tx_rows = Vec::new();
    let mut serialized = Vec::new();
    let mut compressed = Vec::new();
    let mut target_rows = Vec::with_capacity(reader.index().rows.len());
    let mut target_blocks_hasher = Sha256::new();
    let mut report = RewriteReport::default();
    let started = Instant::now();
    let limits = ArchiveV2WireRewriteLimits {
        max_input_bytes: max_message_bytes,
        max_output_bytes: max_message_bytes,
        ..ArchiveV2WireRewriteLimits::default()
    };

    let mut blocks = if preserve_outer_bytes {
        reader
            .borrowed_blocks_without_rewards_range(0..reader.index().rows.len())
            .map_err(|error| anyhow!(error))?
    } else {
        reader.borrowed_blocks()
    };
    while let Some(block) = blocks.next_block() {
        let block = block.map_err(|error| anyhow!(error))?;
        ensure!(
            !block.uses_owned_fallback(),
            "slot {} uses a historical outer block schema; this byte-minimal converter accepts only the current outer schema",
            block.header().slot
        );
        let source_row = block.index_row;
        let message_stats = if preserve_outer_bytes {
            rewrite_message_region(
                block.tx_rows(),
                block.message_bytes(),
                &mut target_messages,
                limits,
                block.header().slot,
            )?
        } else {
            tx_rows.clear();
            tx_rows.extend(block.tx_rows());
            ensure!(tx_rows.len() == block.tx_count() as usize);
            rewrite_message_region(
                tx_rows.iter().copied(),
                block.message_bytes(),
                &mut target_messages,
                limits,
                block.header().slot,
            )?
        };
        ensure!(
            message_stats.typed_messages == u64::from(block.tx_count()),
            "slot {} transaction-row count differs from its header",
            block.header().slot
        );
        ensure!(
            message_stats.raw_transaction_fallbacks == 0,
            "slot {} contains a raw transaction fallback after the zero-fallback source audit",
            block.header().slot
        );

        if preserve_outer_bytes {
            let uncompressed = block.uncompressed_bytes();
            let message_range = borrowed_subslice_range(uncompressed, block.message_bytes())?;
            ensure!(
                message_range.len() == target_messages.len(),
                "slot {} changed the borrowed message region length",
                source_row.slot
            );
            serialized.clear();
            if serialized.capacity() < uncompressed.len() {
                serialized
                    .try_reserve_exact(uncompressed.len() - serialized.capacity())
                    .context("reserve verbatim target block")?;
            }
            serialized.extend_from_slice(uncompressed);
            serialized[message_range].copy_from_slice(&target_messages);
        } else {
            serialize_current_block_parts(
                block.header(),
                block.tx_count(),
                &tx_rows,
                &target_messages,
                block.metadata_bytes(),
                &mut serialized,
                source_row.uncompressed_len as usize,
            )?;
        }
        ensure!(
            serialized.len() == source_row.uncompressed_len as usize,
            "slot {} changed outer uncompressed length: source={} target={}",
            source_row.slot,
            source_row.uncompressed_len,
            serialized.len()
        );

        compressed.clear();
        let compress_bound = zstd::zstd_safe::compress_bound(serialized.len());
        if compressed.capacity() < compress_bound {
            compressed
                .try_reserve_exact(compress_bound - compressed.capacity())
                .context("reserve target zstd buffer")?;
        }
        let compressed_len = compressor
            .compress_to_buffer(&serialized, &mut compressed)
            .with_context(|| format!("compress target slot {}", source_row.slot))?;
        ensure!(compressed_len == compressed.len());
        let compressed_len =
            u32::try_from(compressed.len()).context("target compressed block exceeds u32::MAX")?;
        target_file
            .write_all(&compressed)
            .with_context(|| format!("write target slot {}", source_row.slot))?;
        target_blocks_hasher.update(&compressed);

        let mut target_row = source_row;
        target_row.compressed_offset = report.target_compressed_bytes;
        target_row.compressed_len = compressed_len;
        target_row.uncompressed_len = source_row.uncompressed_len;
        target_rows.push(target_row);

        report.blocks = report
            .blocks
            .checked_add(1)
            .context("block count overflow")?;
        report.borrowed_current_blocks = report
            .borrowed_current_blocks
            .checked_add(1)
            .context("borrowed block count overflow")?;
        report.typed_messages = report
            .typed_messages
            .checked_add(message_stats.typed_messages)
            .context("typed message count overflow")?;
        report.raw_transaction_fallbacks = report
            .raw_transaction_fallbacks
            .checked_add(message_stats.raw_transaction_fallbacks)
            .context("raw transaction fallback count overflow")?;
        report.message_input_bytes = report
            .message_input_bytes
            .checked_add(message_stats.input_bytes)
            .context("message input byte count overflow")?;
        report.message_output_bytes = report
            .message_output_bytes
            .checked_add(message_stats.output_bytes)
            .context("message output byte count overflow")?;
        report.message_mismatch_bytes = report
            .message_mismatch_bytes
            .checked_add(message_stats.mismatch_bytes)
            .context("message mismatch byte count overflow")?;
        for (target, source) in report
            .source_instruction_data_tag_counts
            .iter_mut()
            .zip(message_stats.source_instruction_data_tag_counts)
        {
            *target = target
                .checked_add(source)
                .context("source instruction tag count overflow")?;
        }
        let metadata_bytes = block.metadata_bytes().len() as u64;
        report.metadata_input_bytes = report
            .metadata_input_bytes
            .checked_add(metadata_bytes)
            .context("metadata input byte count overflow")?;
        report.metadata_output_bytes = report
            .metadata_output_bytes
            .checked_add(metadata_bytes)
            .context("metadata output byte count overflow")?;
        report.source_compressed_bytes = report
            .source_compressed_bytes
            .checked_add(u64::from(source_row.compressed_len))
            .context("source compressed byte count overflow")?;
        report.target_compressed_bytes = report
            .target_compressed_bytes
            .checked_add(u64::from(compressed_len))
            .context("target compressed byte count overflow")?;
        report.uncompressed_bytes = report
            .uncompressed_bytes
            .checked_add(u64::from(source_row.uncompressed_len))
            .context("uncompressed byte count overflow")?;

        if progress_blocks != 0
            && (report.blocks.is_multiple_of(progress_blocks)
                || report.blocks == reader.index().rows.len() as u64)
        {
            let elapsed = started.elapsed().as_secs_f64();
            let rate = if elapsed == 0.0 {
                0.0
            } else {
                report.source_compressed_bytes as f64 / elapsed
            };
            let remaining = reader
                .index()
                .blob_file_bytes
                .saturating_sub(report.source_compressed_bytes);
            let eta_seconds = if rate == 0.0 {
                None
            } else {
                Some(remaining as f64 / rate)
            };
            eprintln!(
                "{{\"kind\":\"archive-v2-pre-to-post-progress\",\"blocks\":{},\"total_blocks\":{},\"source_bytes\":{},\"target_bytes\":{},\"mib_per_second\":{:.2},\"eta_seconds\":{}}}",
                report.blocks,
                reader.index().rows.len(),
                report.source_compressed_bytes,
                report.target_compressed_bytes,
                rate / (1 << 20) as f64,
                eta_seconds
                    .map(|value| format!("{value:.1}"))
                    .unwrap_or_else(|| "null".to_owned()),
            );
        }
    }

    ensure!(
        report.blocks == reader.index().rows.len() as u64,
        "rewrite visited {} of {} blocks",
        report.blocks,
        reader.index().rows.len()
    );
    ensure!(
        report.source_compressed_bytes == reader.index().blob_file_bytes,
        "rewrite source-byte coverage differs from the admitted index"
    );
    ensure!(
        report.message_input_bytes == report.message_output_bytes,
        "aggregate message byte length changed"
    );
    let expected_mismatches = report.source_instruction_data_tag_counts[1..=6]
        .iter()
        .try_fold(0u64, |total, count| total.checked_add(*count))
        .context("expected mismatch count overflow")?;
    ensure!(
        report.source_instruction_data_tag_counts[7..]
            .iter()
            .all(|count| *count == 0)
            && report.message_mismatch_bytes == expected_mismatches,
        "aggregate Pre-to-Post tag delta proof failed"
    );
    report.metadata_regions_byte_identical = true;
    target_file.flush()?;
    target_file.get_ref().sync_all()?;
    drop(target_file);

    write_archive_v2_hot_block_index(
        &staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        report.target_compressed_bytes,
        zstd_level,
        reader.index().flags,
        &target_rows,
    )?;
    report.target_blocks_sha256 = hex_lower(&target_blocks_hasher.finalize());
    Ok(report)
}

#[derive(Debug, Default)]
struct MessageRegionStats {
    typed_messages: u64,
    raw_transaction_fallbacks: u64,
    input_bytes: u64,
    output_bytes: u64,
    mismatch_bytes: u64,
    source_instruction_data_tag_counts: [u64; 9],
}

fn rewrite_message_region(
    rows: impl IntoIterator<Item = ArchiveV2HotTxRow>,
    source: &[u8],
    target: &mut Vec<u8>,
    limits: ArchiveV2WireRewriteLimits,
    slot: u64,
) -> Result<MessageRegionStats> {
    target.clear();
    if target.capacity() < source.len() {
        target
            .try_reserve_exact(source.len() - target.capacity())
            .context("reserve target message region")?;
    }
    let mut aggregate = MessageRegionStats::default();
    let mut visitor = ArchiveV2WireIdentityVisitor;
    for row in rows {
        ensure!(
            row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0,
            "slot {slot} transaction {} is a raw transaction fallback",
            row.tx_index
        );
        let start = row.message_offset as usize;
        let length = row.message_len as usize;
        let end = start
            .checked_add(length)
            .context("source message range overflow")?;
        ensure!(
            start == target.len(),
            "slot {slot} transaction {} has a non-contiguous message offset",
            row.tx_index
        );
        let input = source.get(start..end).with_context(|| {
            format!(
                "slot {slot} transaction {} message range is outside its block",
                row.tx_index
            )
        })?;
        let before = target.len();
        let stats =
            transcode_archive_v2_hot_message_wire_pre_to_post(input, target, &mut visitor, limits)
                .with_context(|| {
                    format!(
                        "transcode slot {slot} transaction {} from Pre to Post",
                        row.tx_index
                    )
                })?;
        let written = target.len() - before;
        ensure!(
            stats.input_bytes == length
                && stats.output_bytes == length
                && written == length
                && written == row.message_len as usize,
            "slot {slot} transaction {} violated the length-preserving message contract",
            row.tx_index
        );
        let output = &target[before..];
        let mut mismatch_bytes = 0u64;
        for (&source_byte, &target_byte) in input.iter().zip(output) {
            if source_byte == target_byte {
                continue;
            }
            ensure!(
                (1..=6).contains(&source_byte)
                    && target_byte == source_byte.checked_add(2).unwrap(),
                "slot {slot} transaction {} changed a byte outside the frozen Pre-to-Post tag map: source={source_byte} target={target_byte}",
                row.tx_index
            );
            mismatch_bytes = mismatch_bytes
                .checked_add(1)
                .context("message mismatch byte count overflow")?;
        }
        let expected_mismatches = stats.source_instruction_data_tag_counts[1..=6]
            .iter()
            .try_fold(0u64, |total, count| total.checked_add(*count))
            .context("expected message mismatch count overflow")?;
        ensure!(
            stats.source_instruction_data_tag_counts[7..]
                .iter()
                .all(|count| *count == 0)
                && mismatch_bytes == expected_mismatches,
            "slot {slot} transaction {} failed the exact Pre-to-Post byte-delta proof: mismatches={mismatch_bytes} expected={expected_mismatches}",
            row.tx_index
        );
        aggregate.typed_messages = aggregate
            .typed_messages
            .checked_add(1)
            .context("typed message count overflow")?;
        aggregate.input_bytes = aggregate
            .input_bytes
            .checked_add(length as u64)
            .context("message input byte count overflow")?;
        aggregate.output_bytes = aggregate
            .output_bytes
            .checked_add(written as u64)
            .context("message output byte count overflow")?;
        aggregate.mismatch_bytes = aggregate
            .mismatch_bytes
            .checked_add(mismatch_bytes)
            .context("message mismatch byte count overflow")?;
        for (target, source) in aggregate
            .source_instruction_data_tag_counts
            .iter_mut()
            .zip(stats.source_instruction_data_tag_counts)
        {
            *target = target
                .checked_add(source)
                .context("source instruction tag count overflow")?;
        }
    }
    ensure!(
        target.len() == source.len(),
        "slot {slot} target message region changed length"
    );
    Ok(aggregate)
}

fn borrowed_subslice_range(parent: &[u8], child: &[u8]) -> Result<std::ops::Range<usize>> {
    let parent_start = parent.as_ptr() as usize;
    let parent_end = parent_start
        .checked_add(parent.len())
        .context("parent byte address overflow")?;
    let child_start = child.as_ptr() as usize;
    let child_end = child_start
        .checked_add(child.len())
        .context("child byte address overflow")?;
    ensure!(
        child_start >= parent_start && child_end <= parent_end,
        "borrowed message bytes are outside the decompressed frame"
    );
    Ok((child_start - parent_start)..(child_end - parent_start))
}

struct BoundedVecWriter<'a> {
    output: &'a mut Vec<u8>,
    max_len: usize,
}

impl wincode::io::Writer for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> wincode::io::WriteResult<()> {
        let next = self
            .output
            .len()
            .checked_add(bytes.len())
            .ok_or(wincode::io::WriteError::WriteSizeLimit(usize::MAX))?;
        if next > self.max_len {
            return Err(wincode::io::WriteError::WriteSizeLimit(next));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

fn serialize_current_block_parts(
    header: &ArchiveV2HotBlockHeader,
    tx_count: u32,
    tx_rows: &[ArchiveV2HotTxRow],
    message_bytes: &[u8],
    metadata_bytes: &[u8],
    output: &mut Vec<u8>,
    exact_len: usize,
) -> Result<()> {
    output.clear();
    if output.capacity() < exact_len {
        output
            .try_reserve_exact(exact_len - output.capacity())
            .context("reserve serialized target block")?;
    }
    let block = (header, tx_count, tx_rows, message_bytes, metadata_bytes);
    wincode::config::serialize_into(
        BoundedVecWriter {
            output,
            max_len: exact_len,
        },
        &block,
        wincode_leb128_config(),
    )?;
    ensure!(
        output.len() == exact_len,
        "current block reserialization wrote {} bytes, expected {exact_len}",
        output.len()
    );
    Ok(())
}

fn manifest_additional_files(copied: &[String]) -> Vec<String> {
    let automatic = BTreeSet::from([
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_GENESIS_BIN_FILE,
    ]);
    copied
        .iter()
        .filter(|name| !automatic.contains(name.as_str()))
        .cloned()
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn write_candidate_descriptor(
    path: &Path,
    args: &Args,
    paths: &ValidatedPaths,
    source_authority: &SourceAuthority,
    source_authority_device_id: Option<u64>,
    source_files: &BTreeMap<String, FileBinding>,
    candidate_rewrite_files: &BTreeMap<String, FileBinding>,
    source_inventory: &BTreeMap<String, SourceEntry>,
    source_audit_report: &ScannerReportBinding,
    source_zstd_level: i32,
    target_zstd_level: i32,
    retained_durable_files: &[String],
    retained_edge_files: &[String],
    moved_to_backup: &[String],
    rewrite: &RewriteReport,
) -> Result<FileBinding> {
    ensure!(
        args.fast_candidate,
        "candidate descriptor is only valid with --fast-candidate"
    );
    ensure!(
        rewrite.message_input_bytes == rewrite.message_output_bytes
            && rewrite.metadata_input_bytes == rewrite.metadata_output_bytes
            && rewrite.metadata_regions_byte_identical,
        "rewrite invariants are incomplete before candidate descriptor publication"
    );
    let expected_delta = rewrite.source_instruction_data_tag_counts[1..=6]
        .iter()
        .try_fold(0u64, |total, count| total.checked_add(*count))
        .context("candidate descriptor tag delta count overflow")?;
    ensure!(
        expected_delta == rewrite.message_mismatch_bytes,
        "candidate descriptor message delta count is inconsistent"
    );
    let descriptor = CandidateDescriptor {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-candidate",
        state: "unfinalized",
        canonical: false,
        epoch: args.epoch,
        cluster_id: &args.cluster_id,
        prospective_generation_id: &args.generation_id,
        source: paths.source.display().to_string(),
        source_authority_kind: source_authority.kind_name(),
        source_authority_id: &source_authority.id,
        source_authority_scope: source_authority.scope_name(),
        source_authority_filesystem: source_authority.filesystem_name(),
        source_authority_device_id,
        candidate: paths.source.display().to_string(),
        backup: paths
            .backup
            .as_ref()
            .context("fast candidate backup path disappeared")?
            .display()
            .to_string(),
        source_profile_evidence: "external-whole-generation-scan-report",
        source_audit_report,
        expected_wire_profile_after_rewrite: "post-unknown-instruction-fallbacks-v1",
        source_full_audit_performed_in_this_run: false,
        source_audit_report_reused: true,
        single_decode_rewrite_pass: true,
        outer_block_bytes_preserved_verbatim_except_messages: true,
        sidecars_copied: false,
        sidecars_rewritten: false,
        pair_swap_requires_external_reader_quiescence: true,
        archive_root_switch_lock: paths
            .parent
            .join(PRE_TO_POST_ROOT_SWITCH_LOCK_FILE)
            .display()
            .to_string(),
        codec: "wincode-leb128-current-block+independent-zstd-frames",
        source_zstd_level,
        target_zstd_level,
        source_files,
        source_inventory,
        candidate_rewrite_files,
        retained_durable_files,
        retained_edge_files,
        retained_edge_files_authoritative: false,
        retained_edge_validation_deferred: true,
        get_block_index_rebuild_required: moved_to_backup
            .iter()
            .any(|name| name == ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
        moved_to_backup,
        rewrite,
        exact_message_length_preserved: true,
        exact_message_delta_proved: true,
        metadata_regions_copied_verbatim: true,
        canonical_publication_deferred: true,
        target_post_audit_performed: false,
        canonical_manifest_written: false,
        canonical_profile_marker_written: false,
        canonical_migration_receipt_written: false,
        required_finalization: REQUIRED_CANDIDATE_FINALIZATION_STEPS,
        edge_rebuild_required: moved_to_backup.iter().any(|name| {
            name == ARCHIVE_V2_GET_BLOCK_INDEX_FILE
                || LEGACY_EDGE_TIER_FILES.contains(&name.as_str())
        }),
        source_provider_snapshot_required: false,
        source_linux_read_leases_required: true,
    };
    let mut bytes = serde_json::to_vec_pretty(&descriptor)?;
    bytes.push(b'\n');
    let binding = FileBinding {
        bytes: bytes.len() as u64,
        sha256: hex_lower(&Sha256::digest(&bytes)),
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create candidate descriptor {}", path.display()))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(binding)
}

#[allow(clippy::too_many_arguments)]
fn write_migration_receipt(
    path: &Path,
    args: &Args,
    paths: &ValidatedPaths,
    source_authority: &SourceAuthority,
    source_authority_device_id: Option<u64>,
    source_files: &BTreeMap<String, FileBinding>,
    target_files: &BTreeMap<String, FileBinding>,
    source_audit: FullGenerationWireProfileAudit,
    source_zstd_level: i32,
    target_zstd_level: i32,
    omitted_edge_files: &[String],
    omitted_control_files: &[String],
    omitted_obsolete_block_files: &[String],
    rewrite: &RewriteReport,
) -> Result<()> {
    ensure!(
        rewrite.message_input_bytes == rewrite.message_output_bytes
            && rewrite.metadata_input_bytes == rewrite.metadata_output_bytes
            && rewrite.metadata_regions_byte_identical,
        "rewrite invariants are incomplete before receipt publication"
    );
    let expected_delta = rewrite.source_instruction_data_tag_counts[1..=6]
        .iter()
        .try_fold(0u64, |total, count| total.checked_add(*count))
        .context("receipt tag delta count overflow")?;
    ensure!(
        expected_delta == rewrite.message_mismatch_bytes,
        "receipt message delta count is inconsistent"
    );
    let receipt = MigrationReceipt {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-receipt",
        epoch: args.epoch,
        cluster_id: &args.cluster_id,
        generation_id: &args.generation_id,
        source: paths.source.display().to_string(),
        source_authority_kind: source_authority.kind_name(),
        source_authority_id: &source_authority.id,
        source_authority_scope: source_authority.scope_name(),
        source_authority_filesystem: source_authority.filesystem_name(),
        source_authority_device_id,
        target: paths.target.display().to_string(),
        source_profile: "pre-unknown-instruction-fallbacks-v1",
        target_profile: "post-unknown-instruction-fallbacks-v1",
        source_profile_decision: "unique-full-generation-decode",
        codec: "wincode-leb128-current-block+independent-zstd-frames",
        source_zstd_level,
        target_zstd_level,
        source_audit: source_audit.into(),
        source_files,
        target_files,
        omitted_edge_files,
        omitted_control_files,
        omitted_obsolete_block_files,
        rewrite,
        exact_message_length_preserved: true,
        exact_message_delta_proved: true,
        metadata_regions_copied_verbatim: true,
        edge_rebuild_required: !omitted_edge_files.is_empty(),
        target_provider_immutability_required: true,
        source_provider_snapshot_required: matches!(
            source_authority.kind,
            SourceAuthorityKind::ProviderSnapshot
        ),
        source_linux_read_leases_required: matches!(
            source_authority.kind,
            SourceAuthorityKind::LinuxReadLeases
        ),
    };
    let mut bytes = serde_json::to_vec_pretty(&receipt)?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create migration receipt {}", path.display()))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn source_control_names() -> Vec<String> {
    SOURCE_CONTROL_FILES
        .iter()
        .chain(LEGACY_SOURCE_CONTROL_FILES.iter())
        .map(|name| (*name).to_owned())
        .chain([
            wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name,
            wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name,
        ])
        .collect()
}

fn ensure_edge_files_absent(directory: &Path) -> Result<()> {
    for name in EDGE_TIER_FILES.iter().chain(LEGACY_EDGE_TIER_FILES.iter()) {
        ensure!(
            !directory.join(name).try_exists()?,
            "derived edge-tier object must not be present in the canonical target: {name}"
        );
    }
    Ok(())
}

fn ensure_manifest_binds_target(
    manifest: &GenerationManifest,
    target_files: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, binding) in target_files {
        let entry = manifest
            .file(name)
            .with_context(|| format!("target manifest does not bind {name}"))?;
        ensure!(
            entry.size == binding.bytes && entry.sha256 == binding.sha256,
            "target manifest binding differs from the migration receipt for {name}"
        );
    }
    ensure!(
        manifest.file(PRE_TO_POST_RECEIPT_FILE).is_some(),
        "target manifest does not bind the migration receipt"
    );
    let marker = wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
    ensure!(
        manifest.file(&marker.name) == Some(&marker),
        "target manifest does not bind the canonical Post marker"
    );
    Ok(())
}

fn ensure_exact_target_inventory(
    directory: &Path,
    target_files: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    let mut expected = target_files.keys().cloned().collect::<BTreeSet<_>>();
    expected.insert(PRE_TO_POST_RECEIPT_FILE.to_owned());
    expected.insert(GENERATION_MANIFEST_FILE.to_owned());
    expected.insert(ARCHIVE_V2_PUBLICATION_LOCK_FILE.to_owned());
    expected
        .insert(wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name);
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("target contains a non-UTF-8 entry"))?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.file_type().is_file(),
            "target contains a non-file object: {name}"
        );
        actual.insert(name);
    }
    ensure!(
        actual == expected,
        "target inventory differs from the reviewed canonical inventory"
    );
    Ok(())
}

fn ensure_candidate_canonical_controls_absent(directory: &Path) -> Result<()> {
    let pre_marker =
        wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name;
    let post_marker =
        wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name;
    for name in [
        GENERATION_MANIFEST_FILE,
        PRE_TO_POST_RECEIPT_FILE,
        REGISTRY_REPROCESS_RECEIPT_FILE,
        ARCHIVE_V2_PUBLICATION_LOCK_FILE,
        SCHEDULER_COMPLETION_FILE,
        ".block-time-gaps.bin.lock",
        ".complete-hot-v2-no-access-delete-car",
        ".complete-hot-v2-shredding-sidecar-v2",
        pre_marker.as_str(),
        post_marker.as_str(),
    ] {
        ensure!(
            !directory.join(name).try_exists()?,
            "non-canonical candidate contains forbidden canonical control: {name}"
        );
    }
    Ok(())
}

fn candidate_moved_files(entries: &BTreeMap<String, SourceEntry>) -> Vec<String> {
    entries
        .iter()
        .filter(|(name, entry)| {
            name.as_str() == ARCHIVE_V2_GET_BLOCK_INDEX_FILE
                || LEGACY_EDGE_TIER_FILES.contains(&name.as_str())
                || name.as_str() == BLOCK_TIME_GAP_FILE
                || matches!(
                    entry.disposition,
                    SourceEntryDisposition::OmitControl | SourceEntryDisposition::OmitObsoleteBlock
                )
        })
        .map(|(name, _)| name.clone())
        .collect()
}

fn ensure_in_place_candidate_inventory(
    directory: &Path,
    initial: &BTreeMap<String, SourceEntry>,
    moved_to_backup: &[String],
) -> Result<()> {
    let moved = moved_to_backup
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut expected = initial
        .keys()
        .filter(|name| !moved.contains(name.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    expected.insert(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE.to_owned());
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("candidate contains a non-UTF-8 entry"))?;
        ensure!(
            fs::symlink_metadata(entry.path())?.file_type().is_file(),
            "candidate contains a non-file object: {name}"
        );
        actual.insert(name);
    }
    ensure!(
        actual == expected,
        "in-place candidate inventory differs from the reviewed transaction inventory"
    );
    Ok(())
}

#[cfg(unix)]
fn file_identity(path: &Path) -> Result<FileIdentity> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect file identity {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "identity target is not a file"
    );
    Ok(FileIdentity {
        bytes: metadata.len(),
        device_id: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn file_identity(_path: &Path) -> Result<FileIdentity> {
    anyhow::bail!("pair-swap file identities require Unix")
}

fn write_json_create_new<T: Serialize>(path: &Path, value: &T) -> Result<FileBinding> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    let binding = FileBinding {
        bytes: bytes.len() as u64,
        sha256: hex_lower(&Sha256::digest(&bytes)),
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create immutable JSON {}", path.display()))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_directory(path.parent().context("JSON path has no parent")?)?;
    Ok(binding)
}

#[cfg(unix)]
fn make_files_read_only<'a>(
    directory: &Path,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    for name in names {
        let path = directory.join(name);
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            metadata.file_type().is_file(),
            "candidate object is not a file"
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444))?;
        File::open(&path)?.sync_all()?;
        ensure!(
            fs::symlink_metadata(&path)?.permissions().mode() & 0o777 == 0o444,
            "candidate object is not read-only: {name}"
        );
    }
    sync_directory(directory)
}

#[cfg(not(unix))]
fn make_files_read_only<'a>(
    _directory: &Path,
    _names: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    anyhow::bail!("fast candidate permissions require Unix")
}

struct SwitchLock {
    file: File,
}

impl SwitchLock {
    #[cfg(unix)]
    fn acquire(root: &Path) -> Result<Self> {
        use std::os::fd::AsRawFd;
        let path = root.join(PRE_TO_POST_ROOT_SWITCH_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .with_context(|| format!("open archive-root switch lock {}", path.display()))?;
        let opened = file.metadata()?;
        let linked = fs::symlink_metadata(&path)?;
        ensure!(
            opened.is_file()
                && linked.file_type().is_file()
                && opened.dev() == linked.dev()
                && opened.ino() == linked.ino()
                && opened.nlink() == 1,
            "archive-root switch lock is not one stable regular-file authority"
        );
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0,
            "lock archive-root switch file: {}",
            io::Error::last_os_error()
        );
        let linked_after_lock = fs::symlink_metadata(&path)?;
        ensure!(
            linked_after_lock.file_type().is_file()
                && opened.dev() == linked_after_lock.dev()
                && opened.ino() == linked_after_lock.ino(),
            "archive-root switch lock path changed while the lock was acquired"
        );
        Ok(Self { file })
    }

    #[cfg(not(unix))]
    fn acquire(_root: &Path) -> Result<Self> {
        anyhow::bail!("archive-root switch lock requires Unix")
    }
}

impl Drop for SwitchLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: the descriptor remains live until this method returns.
            let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

#[cfg(target_os = "linux")]
fn exchange_paths(left: &Path, right: &Path) -> Result<()> {
    let left_c = CString::new(left.as_os_str().as_bytes()).context("left exchange path has NUL")?;
    let right_c =
        CString::new(right.as_os_str().as_bytes()).context("right exchange path has NUL")?;
    // SAFETY: both paths stay live and RENAME_EXCHANGE atomically swaps their names.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            left_c.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            right_c.as_ptr(),
            libc::RENAME_EXCHANGE as libc::c_long,
        )
    };
    ensure!(
        result == 0,
        "atomically exchange {} and {}: {}",
        left.display(),
        right.display(),
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn exchange_paths(left: &Path, right: &Path) -> Result<()> {
    let left_c = CString::new(left.as_os_str().as_bytes()).context("left exchange path has NUL")?;
    let right_c =
        CString::new(right.as_os_str().as_bytes()).context("right exchange path has NUL")?;
    // SAFETY: both paths stay live and RENAME_SWAP atomically swaps their names.
    let result = unsafe { libc::renamex_np(left_c.as_ptr(), right_c.as_ptr(), libc::RENAME_SWAP) };
    ensure!(
        result == 0,
        "atomically exchange {} and {}: {}",
        left.display(),
        right.display(),
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn exchange_paths(_left: &Path, _right: &Path) -> Result<()> {
    anyhow::bail!("atomic file exchange requires Linux or macOS")
}

fn ensure_pair_exchanged(
    active: &Path,
    workspace: &Path,
    source: &FileIdentity,
    candidate: &FileIdentity,
) -> Result<()> {
    let active_identity = file_identity(active)?;
    let workspace_identity = file_identity(workspace)?;
    if active_identity == *source && workspace_identity == *candidate {
        exchange_paths(active, workspace)?;
    } else {
        ensure!(
            active_identity == *candidate && workspace_identity == *source,
            "pair-swap files do not match either admitted pre-swap or post-swap identity"
        );
    }
    ensure!(
        file_identity(active)? == *candidate && file_identity(workspace)? == *source,
        "pair-swap identity verification failed"
    );
    Ok(())
}

fn verify_recovery_bound_pair(
    active: &Path,
    workspace: &Path,
    source_identity: &FileIdentity,
    source_binding: &FileBinding,
    candidate_identity: &FileIdentity,
    candidate_binding: &FileBinding,
) -> Result<()> {
    ensure!(
        source_identity != candidate_identity,
        "recovery source and candidate identities must differ"
    );
    let active_identity = file_identity(active)?;
    let workspace_identity = file_identity(workspace)?;
    let (active_binding, workspace_binding) =
        if active_identity == *source_identity && workspace_identity == *candidate_identity {
            (source_binding, candidate_binding)
        } else {
            ensure!(
                active_identity == *candidate_identity && workspace_identity == *source_identity,
                "recovery payloads do not match the admitted pair identities"
            );
            (candidate_binding, source_binding)
        };
    ensure!(
        hash_regular_file(active)? == *active_binding,
        "recovery active payload content differs from its durable binding: {}",
        active.display()
    );
    ensure!(
        hash_regular_file(workspace)? == *workspace_binding,
        "recovery workspace payload content differs from its durable binding: {}",
        workspace.display()
    );
    Ok(())
}

fn verify_recovery_payload_bindings(
    paths: &ValidatedPaths,
    workspace: &Path,
    intent: &PairSwapIntent,
) -> Result<()> {
    verify_recovery_bound_pair(
        &paths.source.join(ARCHIVE_V2_BLOCKS_FILE),
        &workspace.join(ARCHIVE_V2_BLOCKS_FILE),
        &intent.source_blocks,
        &intent.source_blocks_binding,
        &intent.candidate_blocks,
        &intent.candidate_blocks_binding,
    )?;
    verify_recovery_bound_pair(
        &paths.source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        &workspace.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        &intent.source_index,
        &intent.source_index_binding,
        &intent.candidate_index,
        &intent.candidate_index_binding,
    )
}

fn ensure_phase_marker(directory: &Path, name: &str) -> Result<()> {
    let path = directory.join(name);
    if path.try_exists()? {
        ensure!(fs::symlink_metadata(&path)?.file_type().is_file());
        return Ok(());
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.sync_all()?;
    sync_directory(directory)
}

fn move_to_disabled(active: &Path, disabled: &Path, names: &[String]) -> Result<()> {
    if !path_entry_exists_nofollow(disabled)? {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(disabled)?;
        sync_directory(
            disabled
                .parent()
                .context("disabled directory has no parent")?,
        )?;
    }
    let disabled_parent = disabled
        .parent()
        .context("disabled directory has no parent")?;
    validate_recovery_workspace(disabled, disabled_parent, "disabled directory")?;
    for name in names {
        let source = active.join(name);
        let target = disabled.join(name);
        match (
            path_entry_exists_nofollow(&source)?,
            path_entry_exists_nofollow(&target)?,
        ) {
            (true, false) => publish_directory_no_replace(&source, &target)?,
            (false, true) => {}
            _ => anyhow::bail!("ambiguous disabled-object state for {name}"),
        }
    }
    sync_directory(active)?;
    sync_directory(disabled)
}

fn complete_fast_pair_swap(paths: &ValidatedPaths, intent: &PairSwapIntent) -> Result<()> {
    let _lock = SwitchLock::acquire(&paths.parent)?;
    complete_fast_pair_swap_locked(paths, intent)
}

fn complete_fast_pair_swap_locked(paths: &ValidatedPaths, intent: &PairSwapIntent) -> Result<()> {
    let backup = paths
        .backup
        .as_ref()
        .context("fast backup path is missing")?;
    ensure!(intent.schema_version == 1);
    ensure!(intent.kind == "archive-v2-pre-to-post-pair-swap-intent");
    ensure!(intent.candidate == paths.source.display().to_string());
    ensure!(intent.staging == paths.staging.display().to_string());
    ensure!(intent.backup == backup.display().to_string());
    let workspace = if path_entry_exists_nofollow(&paths.staging)? {
        paths.staging.as_path()
    } else {
        ensure!(
            path_entry_exists_nofollow(backup)?,
            "pair-swap workspace disappeared"
        );
        backup.as_path()
    };
    validate_recovery_workspace(
        workspace,
        &paths.parent,
        if workspace == paths.staging.as_path() {
            "staging"
        } else {
            "backup"
        },
    )?;
    if workspace == paths.staging.as_path() {
        ensure!(
            !workspace
                .join(PRE_TO_POST_SWITCH_COMPLETE_FILE)
                .try_exists()?,
            "a completion marker cannot exist in pre-publication staging"
        );
    }
    if workspace == paths.staging.as_path() {
        ensure_pair_exchanged(
            &paths.source.join(ARCHIVE_V2_BLOCKS_FILE),
            &workspace.join(ARCHIVE_V2_BLOCKS_FILE),
            &intent.source_blocks,
            &intent.candidate_blocks,
        )?;
        sync_directory(&paths.source)?;
        sync_directory(workspace)?;
        ensure_phase_marker(workspace, "phase-1-blocks-exchanged")?;

        ensure_pair_exchanged(
            &paths.source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            &workspace.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            &intent.source_index,
            &intent.candidate_index,
        )?;
        sync_directory(&paths.source)?;
        sync_directory(workspace)?;
        ensure_phase_marker(workspace, "phase-2-index-exchanged")?;

        move_to_disabled(
            &paths.source,
            &workspace.join(PRE_TO_POST_DISABLED_DIRECTORY),
            &intent.moved_to_backup,
        )?;
        ensure_phase_marker(workspace, "phase-3-stale-disabled")?;

        let staged_descriptor = workspace.join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE);
        let active_descriptor = paths.source.join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE);
        match (
            staged_descriptor.try_exists()?,
            active_descriptor.try_exists()?,
        ) {
            (true, false) => publish_directory_no_replace(&staged_descriptor, &active_descriptor)?,
            (false, true) => {}
            _ => anyhow::bail!("ambiguous candidate descriptor publication state"),
        }
        ensure!(
            hash_regular_file(&active_descriptor)? == intent.candidate_descriptor,
            "candidate descriptor binding differs after publication"
        );
        ensure_phase_marker(workspace, "phase-4-descriptor-published")?;
        ensure_candidate_canonical_controls_absent(&paths.source)?;
        sync_directory(&paths.source)?;
        sync_directory(workspace)?;
        publish_directory_no_replace(workspace, backup)?;
        sync_directory(&paths.parent)?;
    }

    ensure!(
        file_identity(&paths.source.join(ARCHIVE_V2_BLOCKS_FILE))? == intent.candidate_blocks
            && file_identity(&paths.source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?
                == intent.candidate_index
            && file_identity(&backup.join(ARCHIVE_V2_BLOCKS_FILE))? == intent.source_blocks
            && file_identity(&backup.join(ARCHIVE_V2_BLOCK_INDEX_FILE))? == intent.source_index,
        "completed pair-swap identities do not match the durable intent"
    );
    ensure!(
        hash_regular_file(&paths.source.join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE))?
            == intent.candidate_descriptor,
        "completed candidate descriptor differs from the durable intent"
    );
    for name in &intent.moved_to_backup {
        ensure!(
            !paths.source.join(name).try_exists()?
                && backup
                    .join(PRE_TO_POST_DISABLED_DIRECTORY)
                    .join(name)
                    .try_exists()?,
            "stale candidate object was not moved to backup: {name}"
        );
    }
    for name in &intent.retained_edge_files {
        ensure!(
            paths.source.join(name).try_exists()?,
            "retained edge object disappeared: {name}"
        );
    }
    ensure_candidate_canonical_controls_absent(&paths.source)?;
    let intent_binding = hash_regular_file(&backup.join(PRE_TO_POST_SWITCH_INTENT_FILE))?;
    let complete = PairSwapComplete {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-pair-swap-complete".to_owned(),
        epoch: intent.epoch,
        canonical: false,
        candidate: intent.candidate.clone(),
        backup: intent.backup.clone(),
        intent_sha256: intent_binding.sha256,
        candidate_descriptor_sha256: intent.candidate_descriptor.sha256.clone(),
        source_audit_report_sha256: intent.source_audit_report.sha256.clone(),
        source_blocks_sha256: intent.source_blocks_binding.sha256.clone(),
        source_index_sha256: intent.source_index_binding.sha256.clone(),
        candidate_blocks_sha256: intent.candidate_blocks_binding.sha256.clone(),
        candidate_index_sha256: intent.candidate_index_binding.sha256.clone(),
    };
    let complete_path = backup.join(PRE_TO_POST_SWITCH_COMPLETE_FILE);
    if complete_path.try_exists()? {
        let existing: PairSwapComplete = serde_json::from_slice(&fs::read(&complete_path)?)
            .context("parse pair-swap completion marker")?;
        ensure!(
            existing.schema_version == complete.schema_version
                && existing.kind == complete.kind
                && existing.epoch == complete.epoch
                && existing.canonical == complete.canonical
                && existing.candidate == complete.candidate
                && existing.backup == complete.backup
                && existing.intent_sha256 == complete.intent_sha256
                && existing.candidate_descriptor_sha256 == complete.candidate_descriptor_sha256
                && existing.source_audit_report_sha256 == complete.source_audit_report_sha256
                && existing.source_blocks_sha256 == complete.source_blocks_sha256
                && existing.source_index_sha256 == complete.source_index_sha256
                && existing.candidate_blocks_sha256 == complete.candidate_blocks_sha256
                && existing.candidate_index_sha256 == complete.candidate_index_sha256,
            "existing pair-swap completion marker differs from the durable intent"
        );
    } else {
        write_json_create_new(&complete_path, &complete)?;
    }
    sync_directory(backup)?;
    sync_directory(&paths.parent)
}

fn recover_fast_pair_swap(args: &Args, paths: &ValidatedPaths) -> Result<RunReport> {
    let backup = paths
        .backup
        .as_ref()
        .context("fast backup path is missing")?;
    let _lock = SwitchLock::acquire(&paths.parent)?;
    let workspace = if path_entry_exists_nofollow(&paths.staging)? {
        paths.staging.as_path()
    } else {
        backup.as_path()
    };
    validate_recovery_workspace(
        workspace,
        &paths.parent,
        if workspace == paths.staging.as_path() {
            "staging"
        } else {
            "backup"
        },
    )?;
    let already_complete = workspace
        .join(PRE_TO_POST_SWITCH_COMPLETE_FILE)
        .try_exists()?;
    let intent_path = workspace.join(PRE_TO_POST_SWITCH_INTENT_FILE);
    let metadata = fs::symlink_metadata(&intent_path)
        .context("stale fast staging has no durable pair-swap intent")?;
    ensure!(
        metadata.file_type().is_file(),
        "pair-swap intent is not a file"
    );
    let intent: PairSwapIntent =
        serde_json::from_slice(&fs::read(&intent_path)?).context("parse pair-swap intent")?;
    ensure!(
        intent.epoch == args.epoch
            && intent.cluster_id == args.cluster_id
            && intent.prospective_generation_id == args.generation_id,
        "pair-swap recovery arguments differ from the durable intent"
    );
    let audit_path = args
        .source_audit_report
        .as_deref()
        .context("pair-swap recovery requires --source-audit-report")?;
    ensure!(
        audit_path.display().to_string() == intent.source_audit_report_path,
        "pair-swap recovery scanner-report path differs from the durable intent"
    );
    ensure!(
        hash_regular_file(audit_path)? == intent.source_audit_report,
        "pair-swap recovery scanner report differs from the durable intent"
    );
    ensure!(
        args.source_audit_report_sha256.as_deref()
            == Some(intent.source_audit_report.sha256.as_str()),
        "pair-swap recovery scanner-report SHA argument differs from the durable intent"
    );
    let recovery_leases = RecoveryPayloadLeaseSet::acquire(&paths.source, workspace)?;
    recovery_leases.verify_all_held()?;
    verify_recovery_payload_bindings(paths, workspace, &intent)?;
    recovery_leases.verify_all_held()?;
    complete_fast_pair_swap_locked(paths, &intent)?;
    recovery_leases.verify_all_held()?;
    Ok(RunReport::CandidateRecovery(CandidateRecoveryReport {
        schema_version: 1,
        kind: "archive-v2-pre-to-post-candidate-recovery-report",
        state: "unfinalized",
        canonical: false,
        epoch: args.epoch,
        cluster_id: intent.cluster_id,
        prospective_generation_id: intent.prospective_generation_id,
        candidate: paths.source.display().to_string(),
        backup: backup.display().to_string(),
        candidate_descriptor: paths
            .source
            .join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE)
            .display()
            .to_string(),
        candidate_descriptor_bytes: intent.candidate_descriptor.bytes,
        candidate_descriptor_sha256: intent.candidate_descriptor.sha256,
        source_audit_report: intent.source_audit_report_path,
        source_audit_report_bytes: intent.source_audit_report.bytes,
        source_audit_report_sha256: intent.source_audit_report.sha256,
        recovered_switch: true,
        already_complete,
    }))
}

fn names_with_disposition(
    entries: &BTreeMap<String, SourceEntry>,
    disposition: SourceEntryDisposition,
) -> Vec<String> {
    entries
        .iter()
        .filter(|(_, entry)| entry.disposition == disposition)
        .map(|(name, _)| name.clone())
        .collect()
}

fn sync_generation(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.file_type().is_file(),
            "staging contains a non-file object: {}",
            entry.path().display()
        );
        File::open(entry.path())?.sync_all()?;
    }
    sync_directory(directory)
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .with_context(|| format!("open directory {} for sync", directory.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", directory.display()))
}

#[cfg(unix)]
fn make_generation_read_only(directory: &Path) -> Result<(bool, bool)> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(metadata.file_type().is_file());
        fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o444))?;
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o555))?;
    let files_read_only = fs::read_dir(directory)?.try_fold(true, |all, entry| {
        let mode = fs::symlink_metadata(entry?.path())?.permissions().mode() & 0o777;
        Ok::<_, io::Error>(all && mode == 0o444)
    })?;
    let directory_read_only =
        fs::symlink_metadata(directory)?.permissions().mode() & 0o777 == 0o555;
    ensure!(
        files_read_only && directory_read_only,
        "failed to make staged generation read-only"
    );
    Ok((true, true))
}

#[cfg(not(unix))]
fn make_generation_read_only(_directory: &Path) -> Result<(bool, bool)> {
    Ok((false, false))
}

#[cfg(target_os = "linux")]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    let source_c = CString::new(source.as_os_str().as_bytes()).context("staging path has NUL")?;
    let target_c = CString::new(target.as_os_str().as_bytes()).context("target path has NUL")?;
    // SAFETY: both strings stay live. Paths have one parent, and RENAME_NOREPLACE is atomic.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            source_c.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            target_c.as_ptr(),
            libc::RENAME_NOREPLACE as libc::c_long,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                source.display(),
                target.display()
            )
        })
    }
}

#[cfg(target_os = "macos")]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    let source_c = CString::new(source.as_os_str().as_bytes()).context("staging path has NUL")?;
    let target_c = CString::new(target.as_os_str().as_bytes()).context("target path has NUL")?;
    // SAFETY: both strings stay live. RENAME_EXCL gives atomic no-replace publication.
    let result =
        unsafe { libc::renamex_np(source_c.as_ptr(), target_c.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                source.display(),
                target.display()
            )
        })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn publish_directory_no_replace(_source: &Path, _target: &Path) -> Result<()> {
    bail!("atomic no-replace directory publication is unsupported on this operating system")
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_archive_v2::{ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotBlockBlob, ArchiveV2HotBlockIndexRow, ArchiveV2HotMetaRecord, WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header, read_archive_v2_hot_block_index};
    use blockzilla_compact::{CompactMessageHeader, OwnedCompactRecentBlockhash};
    use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter};
    use blockzilla_registry::KeyIndex;
    use serde_json::Value;
    use tempfile::tempdir;
    use wincode::SchemaWrite;

    #[derive(SchemaWrite)]
    enum HistoricalMessagePayload {
        Legacy(HistoricalLegacyMessage),
    }

    #[derive(SchemaWrite)]
    struct HistoricalLegacyMessage {
        header: CompactMessageHeader,
        account_keys: Vec<CompactPubkey>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: Vec<HistoricalInstruction>,
    }

    #[derive(SchemaWrite)]
    struct HistoricalInstruction {
        program_id_index: u8,
        accounts: Vec<u8>,
        data: HistoricalInstructionData,
    }

    #[derive(SchemaWrite)]
    enum HistoricalInstructionData {
        Raw(Vec<u8>),
        ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    }

    fn historical_message(data: HistoricalInstructionData) -> Vec<u8> {
        wincode::config::serialize(
            &HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data,
                }],
            }),
            wincode_leb128_config(),
        )
        .unwrap()
    }

    #[test]
    fn message_region_transcode_keeps_offsets_and_lengths() {
        let first = historical_message(HistoricalInstructionData::Raw(vec![1, 2, 3]));
        let second = historical_message(HistoricalInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(200_000),
        ));
        let source = [first.as_slice(), second.as_slice()].concat();
        let rows = [
            ArchiveV2HotTxRow {
                tx_index: 0,
                flags: 0,
                message_offset: 0,
                message_len: first.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            },
            ArchiveV2HotTxRow {
                tx_index: 1,
                flags: 0,
                message_offset: first.len() as u32,
                message_len: second.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            },
        ];
        let mut target = Vec::new();
        let stats = rewrite_message_region(
            rows.iter().copied(),
            &source,
            &mut target,
            ArchiveV2WireRewriteLimits::default(),
            1,
        )
        .unwrap();
        assert_eq!(source.len(), target.len());
        assert_eq!(stats.typed_messages, 2);
        assert_eq!(stats.input_bytes, stats.output_bytes);
        assert_eq!(&source[..first.len()], &target[..first.len()]);
        assert_ne!(&source[first.len()..], &target[first.len()..]);
    }

    #[test]
    fn borrowed_current_tuple_serializes_as_the_owned_block() {
        let message = historical_message(HistoricalInstructionData::Raw(vec![]));
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 10,
                parent_slot: 9,
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: Some(1),
                block_height: Some(2),
                rewards: None,
            },
            tx_count: 1,
            tx_rows: vec![ArchiveV2HotTxRow {
                tx_index: 0,
                flags: 0,
                message_offset: 0,
                message_len: message.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            }],
            message_bytes: message,
            metadata_bytes: Vec::new(),
        };
        let owned = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let mut borrowed = Vec::new();
        serialize_current_block_parts(
            &block.header,
            block.tx_count,
            &block.tx_rows,
            &block.message_bytes,
            &block.metadata_bytes,
            &mut borrowed,
            owned.len(),
        )
        .unwrap();
        assert_eq!(borrowed, owned);
    }

    #[test]
    fn source_allowlist_omits_edge_and_rejects_unknown_data() {
        assert_eq!(
            source_entry_disposition(ARCHIVE_V2_BLOCK_ACCESS_FILE),
            Some(SourceEntryDisposition::OmitEdge)
        );
        for name in LEGACY_EDGE_TIER_FILES {
            assert_eq!(
                source_entry_disposition(name),
                Some(SourceEntryDisposition::OmitEdge)
            );
            assert!(reviewed_source_names().contains(*name));
        }
        assert_eq!(
            source_entry_disposition(ARCHIVE_V2_BLOCKS_FILE),
            Some(SourceEntryDisposition::Rewrite)
        );
        assert_eq!(source_entry_disposition("surprise.bin"), None);
    }

    #[test]
    fn source_allowlist_classifies_markers_controls_and_durable_registry_inputs() {
        assert_eq!(
            source_entry_disposition(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE),
            Some(SourceEntryDisposition::CopyDurable)
        );
        assert_eq!(
            source_entry_disposition(ARCHIVE_V2_PUBKEY_HOT_SEED_FILE),
            Some(SourceEntryDisposition::CopyDurable)
        );
        assert_eq!(
            source_entry_disposition(GENERATION_MANIFEST_FILE),
            Some(SourceEntryDisposition::OmitControl)
        );
        for name in LEGACY_SOURCE_CONTROL_FILES {
            assert_eq!(
                source_entry_disposition(name),
                Some(SourceEntryDisposition::OmitControl)
            );
            assert!(reviewed_source_names().contains(*name));
        }
        for profile in [
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ] {
            assert_eq!(
                source_entry_disposition(&wire_profile_marker(profile).name),
                Some(SourceEntryDisposition::OmitControl)
            );
        }
    }

    #[test]
    fn source_block_hash_requires_complete_sequential_coverage() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join(ARCHIVE_V2_BLOCKS_FILE), b"abcdef").unwrap();
        let source = PinnedLocalRangeSource::new(directory.path());
        let incomplete = SourceWithBlockHash::new(source.clone(), 6);
        assert_eq!(
            incomplete.read_range(ARCHIVE_V2_BLOCKS_FILE, 0, 3).unwrap(),
            b"abc"
        );
        assert!(incomplete.finish_block_hash().is_err());

        let nonsequential = SourceWithBlockHash::new(source, 6);
        assert!(
            nonsequential
                .read_range(ARCHIVE_V2_BLOCKS_FILE, 1, 3)
                .is_err()
        );
    }

    #[test]
    fn durable_copy_returns_the_pinned_source_binding() {
        let directory = tempdir().unwrap();
        let source_path = directory.path().join("source.bin");
        let target_path = directory.path().join("target.bin");
        let bytes = b"one-pass durable sidecar copy";
        fs::write(&source_path, bytes).unwrap();
        let target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target_path)
            .unwrap();

        let binding = copy_open_files(
            File::open(&source_path).unwrap(),
            target,
            bytes.len() as u64,
        )
        .unwrap();

        assert_eq!(binding, hash_regular_file(&source_path).unwrap());
        assert_eq!(binding, hash_regular_file(&target_path).unwrap());
    }

    #[test]
    fn durable_copy_rejects_an_unexpected_source_length() {
        let directory = tempdir().unwrap();
        let source_path = directory.path().join("source.bin");
        let target_path = directory.path().join("target.bin");
        fs::write(&source_path, b"short").unwrap();
        let target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target_path)
            .unwrap();

        assert!(copy_open_files(File::open(source_path).unwrap(), target, 6).is_err());
    }

    #[test]
    fn source_inventory_caches_reviewed_optional_absence_before_authority() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join(ARCHIVE_V2_BLOCKS_FILE), b"source").unwrap();
        let source = PinnedLocalRangeSource::new(directory.path());
        let entries = inspect_and_pin_source(
            directory.path(),
            &source,
            SourceAuthorityKind::LinuxReadLeases,
        )
        .unwrap();
        assert!(!entries.contains_key(ARCHIVE_V2_SIGNATURES_FILE));
        assert!(!entries.contains_key(ARCHIVE_V2_GENESIS_BIN_FILE));

        fs::write(directory.path().join(ARCHIVE_V2_SIGNATURES_FILE), [9u8; 64]).unwrap();
        fs::write(directory.path().join(ARCHIVE_V2_GENESIS_BIN_FILE), b"late").unwrap();
        assert_eq!(source.size(ARCHIVE_V2_SIGNATURES_FILE).unwrap(), None);
        assert_eq!(source.size(ARCHIVE_V2_GENESIS_BIN_FILE).unwrap(), None);
        assert!(ensure_source_directory_unchanged(directory.path(), &entries).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn writable_source_objects_are_rejected_before_audit() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source_path = root.join("source");
        fs::create_dir(&source_path).unwrap();
        fs::write(source_path.join(ARCHIVE_V2_BLOCKS_FILE), b"bytes").unwrap();
        let source = PinnedLocalRangeSource::new(&source_path);
        assert!(
            inspect_and_pin_source(&source_path, &source, SourceAuthorityKind::ProviderSnapshot,)
                .is_err()
        );

        fs::set_permissions(
            source_path.join(ARCHIVE_V2_BLOCKS_FILE),
            fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o555)).unwrap();
        let read_only = PinnedLocalRangeSource::new(&source_path);
        assert!(
            inspect_and_pin_source(
                &source_path,
                &read_only,
                SourceAuthorityKind::ProviderSnapshot,
            )
            .is_ok()
        );
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn message_region_rejects_raw_fallbacks_gaps_and_trailing_bytes() {
        let message = historical_message(HistoricalInstructionData::Raw(vec![]));
        let row = ArchiveV2HotTxRow {
            tx_index: 0,
            flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
            message_offset: 0,
            message_len: message.len() as u32,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count: 1,
            reserved: [0; 3],
        };
        assert!(
            rewrite_message_region(
                [row],
                &message,
                &mut Vec::new(),
                ArchiveV2WireRewriteLimits::default(),
                1,
            )
            .is_err()
        );

        let mut gap = row;
        gap.flags = 0;
        gap.message_offset = 1;
        gap.message_len -= 1;
        assert!(
            rewrite_message_region(
                [gap],
                &message,
                &mut Vec::new(),
                ArchiveV2WireRewriteLimits::default(),
                1,
            )
            .is_err()
        );

        let mut trailing = row;
        trailing.flags = 0;
        let mut source_with_trailing = message;
        source_with_trailing.push(0xff);
        assert!(
            rewrite_message_region(
                [trailing],
                &source_with_trailing,
                &mut Vec::new(),
                ArchiveV2WireRewriteLimits::default(),
                1,
            )
            .is_err()
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn atomic_publish_refuses_existing_target_without_changing_it() {
        let directory = tempdir().unwrap();
        let staging = directory.path().join("staging");
        let target = directory.path().join("target");
        fs::create_dir(&staging).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(staging.join("new"), b"new").unwrap();
        fs::write(target.join("sentinel"), b"old").unwrap();

        assert!(publish_directory_no_replace(&staging, &target).is_err());
        assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"old");
        assert_eq!(fs::read(staging.join("new")).unwrap(), b"new");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn pair_swap_recovers_one_exchanged_file_and_disables_stale_objects() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-7");
        let staging = root.join(".epoch-7.pre-to-post.staging");
        let backup = root.join(".epoch-7.pre-to-post.backup");
        fs::create_dir(&source).unwrap();
        create_private_staging(&staging).unwrap();

        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"old-blocks").unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCK_INDEX_FILE), b"old-index").unwrap();
        fs::write(source.join(ARCHIVE_V2_META_FILE), b"durable-meta").unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE), b"access-data").unwrap();
        fs::write(
            source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            b"access-index",
        )
        .unwrap();
        fs::write(source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE), b"stale-get").unwrap();
        fs::write(source.join(BLOCK_TIME_GAP_FILE), b"stale-gaps").unwrap();
        fs::write(source.join(GENERATION_MANIFEST_FILE), b"stale-manifest").unwrap();
        fs::write(staging.join(ARCHIVE_V2_BLOCKS_FILE), b"new-blocks").unwrap();
        fs::write(staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE), b"new-index").unwrap();
        let descriptor_path = staging.join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE);
        fs::write(&descriptor_path, b"candidate-descriptor\n").unwrap();
        let descriptor_binding = hash_regular_file(&descriptor_path).unwrap();
        let audit_path = root.join("audit.json");
        fs::write(&audit_path, b"admitted scanner report\n").unwrap();
        let audit_binding = hash_regular_file(&audit_path).unwrap();

        let intent = PairSwapIntent {
            schema_version: 1,
            kind: "archive-v2-pre-to-post-pair-swap-intent".to_owned(),
            epoch: 7,
            cluster_id: "mainnet-beta".to_owned(),
            prospective_generation_id: "epoch-7-candidate".to_owned(),
            candidate: source.display().to_string(),
            staging: staging.display().to_string(),
            backup: backup.display().to_string(),
            source_blocks: file_identity(&source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
            source_blocks_binding: hash_regular_file(&source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
            source_index: file_identity(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap(),
            source_index_binding: hash_regular_file(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
                .unwrap(),
            candidate_blocks: file_identity(&staging.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
            candidate_blocks_binding: hash_regular_file(&staging.join(ARCHIVE_V2_BLOCKS_FILE))
                .unwrap(),
            candidate_index: file_identity(&staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap(),
            candidate_index_binding: hash_regular_file(&staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
                .unwrap(),
            candidate_descriptor: descriptor_binding,
            moved_to_backup: vec![
                ARCHIVE_V2_GET_BLOCK_INDEX_FILE.to_owned(),
                BLOCK_TIME_GAP_FILE.to_owned(),
                GENERATION_MANIFEST_FILE.to_owned(),
            ],
            retained_edge_files: vec![
                ARCHIVE_V2_BLOCK_ACCESS_FILE.to_owned(),
                ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE.to_owned(),
            ],
            source_audit_report_path: audit_path.display().to_string(),
            source_audit_report: audit_binding,
        };
        write_json_create_new(&staging.join(PRE_TO_POST_SWITCH_INTENT_FILE), &intent).unwrap();

        // Simulate a crash after only the first exchange. The recovery helper
        // must identify this state from inode identities and finish it.
        exchange_paths(
            &source.join(ARCHIVE_V2_BLOCKS_FILE),
            &staging.join(ARCHIVE_V2_BLOCKS_FILE),
        )
        .unwrap();
        let paths = ValidatedPaths {
            source: source.clone(),
            target: source.clone(),
            staging: staging.clone(),
            parent: root.clone(),
            backup: Some(backup.clone()),
        };
        complete_fast_pair_swap(&paths, &intent).unwrap();

        assert_eq!(
            fs::read(source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
            b"new-blocks"
        );
        assert_eq!(
            fs::read(source.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap(),
            b"new-index"
        );
        assert_eq!(
            fs::read(backup.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
            b"old-blocks"
        );
        assert_eq!(
            fs::read(backup.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap(),
            b"old-index"
        );
        assert_eq!(
            fs::read(source.join(ARCHIVE_V2_META_FILE)).unwrap(),
            b"durable-meta"
        );
        assert_eq!(
            fs::read(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE)).unwrap(),
            b"access-data"
        );
        assert_eq!(
            fs::read(source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)).unwrap(),
            b"access-index"
        );
        for name in [
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
            BLOCK_TIME_GAP_FILE,
            GENERATION_MANIFEST_FILE,
        ] {
            assert!(!source.join(name).exists());
            assert!(
                backup
                    .join(PRE_TO_POST_DISABLED_DIRECTORY)
                    .join(name)
                    .is_file()
            );
        }
        assert!(source.join(PRE_TO_POST_CANDIDATE_DESCRIPTOR_FILE).is_file());
        assert!(!source.join(GENERATION_MANIFEST_FILE).exists());
        assert!(backup.join(PRE_TO_POST_SWITCH_COMPLETE_FILE).is_file());
        assert!(complete_fast_pair_swap(&paths, &intent).is_ok());

        let make_recovery_args = || Args {
            source: source.clone(),
            source_snapshot_id: None,
            source_lease_id: Some("test-recovery-lease".to_owned()),
            target: source.clone(),
            staging: staging.clone(),
            epoch: 7,
            cluster_id: "mainnet-beta".to_owned(),
            generation_id: "epoch-7-candidate".to_owned(),
            slots_per_epoch: DEFAULT_SLOTS_PER_EPOCH,
            zstd_level: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            progress_blocks: 0,
            threads: Some(DEFAULT_FAST_THREADS),
            source_audit_report: Some(audit_path.clone()),
            source_audit_report_sha256: Some(intent.source_audit_report.sha256.clone()),
            fast_candidate: true,
        };
        let mut wrong_audit_authority = make_recovery_args();
        wrong_audit_authority.source_audit_report_sha256 = Some("0".repeat(64));
        assert!(recover_fast_pair_swap(&wrong_audit_authority, &paths).is_err());
        let recovered = recover_fast_pair_swap(&make_recovery_args(), &paths).unwrap();
        let RunReport::CandidateRecovery(recovered) = recovered else {
            panic!("completed pair swap did not return a recovery report");
        };
        assert!(recovered.recovered_switch);
        assert!(recovered.already_complete);
        assert_eq!(
            recovered.candidate_descriptor_sha256,
            intent.candidate_descriptor.sha256
        );

        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"bad-blocks").unwrap();
        assert!(recover_fast_pair_swap(&make_recovery_args(), &paths).is_err());
    }

    #[test]
    fn scanner_report_admission_requires_exact_zero_fallback_legacy_pre() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-9");
        fs::create_dir(&source).unwrap();
        let report_path = root.join("epoch-9.json");
        let valid = serde_json::json!({
            "schema_version": 1,
            "kind": "archive-v2-wire-profile-scan",
            "archive": source.display().to_string(),
            "epoch": 9,
            "workers": 8,
            "classification": "legacy-pre",
            "action": "convert-to-post",
            "counts": {
                "blocks": 1,
                "owned_fallback_blocks": 0,
                "compressed_block_bytes": 10,
                "uncompressed_block_bytes": 20,
                "typed_messages": 2,
                "raw_transaction_fallbacks": 0,
                "post_only": 0,
                "pre_only": 1,
                "both_equivalent": 1,
                "both_divergent": 0,
                "invalid": 0
            },
            "first_evidence": {
                "post_only": null,
                "pre_only": {"slot": 1, "transaction_index": 0},
                "both_divergent": null,
                "invalid": null
            },
            "error": null,
            "elapsed_seconds": 1.0,
            "completed_unix_seconds": 1
        });
        let valid_bytes = serde_json::to_vec(&valid).unwrap();
        fs::write(&report_path, &valid_bytes).unwrap();
        let valid_sha256 = hex_lower(&Sha256::digest(&valid_bytes));
        let admitted = admit_scanner_report(&report_path, &valid_sha256, &source, 9).unwrap();
        assert_eq!(admitted.counts.pre_only, 1);
        assert!(admit_scanner_report(&report_path, &"0".repeat(64), &source, 9).is_err());

        let mut invalid = valid;
        invalid["counts"]["raw_transaction_fallbacks"] = Value::from(1);
        let invalid_path = root.join("epoch-9-invalid.json");
        let invalid_bytes = serde_json::to_vec(&invalid).unwrap();
        fs::write(&invalid_path, &invalid_bytes).unwrap();
        assert!(
            admit_scanner_report(
                &invalid_path,
                &hex_lower(&Sha256::digest(&invalid_bytes)),
                &source,
                9,
            )
            .is_err()
        );
    }

    #[test]
    fn paths_keep_strict_target_separate_and_fast_candidate_in_place() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source_root = root.join("source-root");
        let target_root = root.join("target-root");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&target_root).unwrap();
        let source = source_root.join("epoch-1");
        fs::create_dir(&source).unwrap();
        let target = target_root.join("epoch-1");
        let staging = target_root.join(".epoch-1.pre-to-post.staging");
        assert!(validate_paths(&source, &target, &staging, 1, false).is_ok());
        assert!(
            validate_paths(
                &source,
                &target,
                &target_root.join("wrong-staging"),
                1,
                false,
            )
            .is_err()
        );
        assert!(
            validate_paths(
                &source,
                &source_root.join("epoch-1-target"),
                &source_root.join(".epoch-1-target.pre-to-post.staging"),
                1,
                false,
            )
            .is_err()
        );

        let candidate_staging = source_root.join(".epoch-1.pre-to-post.staging");
        assert!(validate_paths(&source, &source, &candidate_staging, 1, true).is_ok());
        assert!(validate_paths(&source, &target, &staging, 1, true).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn fast_recovery_rejects_symlinked_workspaces_and_switch_lock() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-1");
        let outside = root.join("outside");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&outside).unwrap();
        let staging = root.join(".epoch-1.pre-to-post.staging");
        let backup = root.join(".epoch-1.pre-to-post.backup");

        symlink(&outside, &staging).unwrap();
        assert!(validate_paths(&source, &source, &staging, 1, true).is_err());
        fs::remove_file(&staging).unwrap();
        symlink(&outside, &backup).unwrap();
        assert!(validate_paths(&source, &source, &staging, 1, true).is_err());
        fs::remove_file(&backup).unwrap();

        let lock_path = root.join(PRE_TO_POST_ROOT_SWITCH_LOCK_FILE);
        let lock_target = root.join("lock-target");
        fs::write(&lock_target, b"not the lock authority").unwrap();
        symlink(&lock_target, &lock_path).unwrap();
        assert!(SwitchLock::acquire(&root).is_err());
    }

    fn authority_cli_prefix() -> Vec<&'static str> {
        vec![
            "archive-v2-pre-to-post",
            "--source",
            "/source/epoch-1",
            "--target",
            "/target/epoch-1",
            "--staging",
            "/target/.epoch-1.pre-to-post.staging",
            "--epoch",
            "1",
            "--cluster-id",
            "mainnet-beta",
            "--generation-id",
            "epoch-1-post",
        ]
    }

    #[test]
    fn cli_requires_exactly_one_source_authority() {
        assert!(Args::try_parse_from(authority_cli_prefix()).is_err());

        let mut both = authority_cli_prefix();
        both.extend([
            "--source-snapshot-id",
            "snapshot-1",
            "--source-lease-id",
            "lease-1",
        ]);
        assert!(Args::try_parse_from(both).is_err());

        let mut snapshot = authority_cli_prefix();
        snapshot.extend(["--source-snapshot-id", "snapshot-1"]);
        let snapshot = Args::try_parse_from(snapshot).unwrap();
        assert_eq!(snapshot.source_snapshot_id.as_deref(), Some("snapshot-1"));
        assert!(snapshot.source_lease_id.is_none());

        let mut lease = authority_cli_prefix();
        lease.extend(["--source-lease-id", "lease-1"]);
        let lease = Args::try_parse_from(lease).unwrap();
        assert_eq!(lease.source_lease_id.as_deref(), Some("lease-1"));
        assert!(lease.source_snapshot_id.is_none());

        let mut strict_threads = authority_cli_prefix();
        strict_threads.extend(["--source-snapshot-id", "snapshot-1", "--threads", "8"]);
        assert!(Args::try_parse_from(strict_threads).is_err());

        let mut fast_threads = authority_cli_prefix();
        fast_threads.extend([
            "--source-lease-id",
            "lease-1",
            "--fast-candidate",
            "--threads",
            "12",
        ]);
        assert_eq!(
            Args::try_parse_from(fast_threads).unwrap().threads,
            Some(12)
        );
        assert!(parse_fast_thread_count("0").is_err());
        assert!(parse_fast_thread_count("65").is_err());

        assert!(SourceAuthority::from_ids(Some(""), None).is_err());
        assert!(SourceAuthority::from_ids(None, Some("")).is_err());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn source_lease_mode_fails_closed_outside_linux() {
        let directory = tempdir().unwrap();
        let source = PinnedLocalRangeSource::new(directory.path());
        assert!(SourceLeaseSet::acquire(&source, &BTreeMap::new()).is_err());
    }

    #[cfg(target_os = "linux")]
    fn lease_test_entries(names: &[&str]) -> BTreeMap<String, SourceEntry> {
        names
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    SourceEntry {
                        bytes: 1,
                        disposition: SourceEntryDisposition::CopyDurable,
                    },
                )
            })
            .collect()
    }

    #[cfg(target_os = "linux")]
    const LEASE_HELPER_MODE_ENV: &str = "BLOCKZILLA_PRE_POST_LEASE_HELPER_MODE";
    #[cfg(target_os = "linux")]
    const LEASE_HELPER_PATH_ENV: &str = "BLOCKZILLA_PRE_POST_LEASE_HELPER_PATH";

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_lease_subprocess_helper() {
        let Ok(mode) = std::env::var(LEASE_HELPER_MODE_ENV) else {
            return;
        };
        let path = PathBuf::from(std::env::var_os(LEASE_HELPER_PATH_ENV).unwrap());
        let name = path.file_name().unwrap().to_str().unwrap();
        match mode.as_str() {
            "writer" => {
                let _writer = OpenOptions::new().write(true).open(&path).unwrap();
                println!("LEASE-HELPER-READY");
                io::stdout().flush().unwrap();
                loop {
                    std::hint::black_box(&_writer);
                    std::thread::park();
                }
            }
            "lease" => {
                let source = PinnedLocalRangeSource::new(path.parent().unwrap());
                source.open_file(name).unwrap();
                let leases =
                    SourceLeaseSet::acquire(&source, &lease_test_entries(&[name])).unwrap();
                leases.verify_all_held().unwrap();
                println!("LEASE-HELPER-READY");
                io::stdout().flush().unwrap();
                loop {
                    std::hint::black_box(&leases);
                    std::thread::park();
                }
            }
            other => panic!("unknown lease helper mode {other}"),
        }
    }

    #[cfg(target_os = "linux")]
    fn spawn_lease_helper(mode: &str, path: &Path) -> std::process::Child {
        use std::{io::BufRead, process::Stdio};

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::linux_lease_subprocess_helper",
                "--nocapture",
            ])
            .env(LEASE_HELPER_MODE_ENV, mode)
            .env(LEASE_HELPER_PATH_ENV, path)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            let read = output.read_line(&mut line).unwrap();
            assert!(read != 0, "lease helper exited before readiness");
            if line.contains("LEASE-HELPER-READY") {
                break;
            }
        }
        child
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_read_lease_rejects_an_existing_writer() {
        let directory = tempdir().unwrap();
        let name = ARCHIVE_V2_META_FILE;
        let path = directory.path().join(name);
        fs::write(&path, b"x").unwrap();
        let mut writer = spawn_lease_helper("writer", &path);
        let source = PinnedLocalRangeSource::new(directory.path());
        source.open_file(name).unwrap();
        let result = SourceLeaseSet::acquire(&source, &lease_test_entries(&[name]));
        writer.kill().unwrap();
        writer.wait().unwrap();
        assert!(result.is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_read_leases_are_retained_and_gettable_for_all_pinned_files() {
        let directory = tempdir().unwrap();
        let names = [ARCHIVE_V2_META_FILE, ARCHIVE_V2_BLOCKS_FILE];
        for name in names {
            fs::write(directory.path().join(name), b"x").unwrap();
        }
        let source = PinnedLocalRangeSource::new(directory.path());
        for name in names {
            source.open_file(name).unwrap();
        }
        let leases = SourceLeaseSet::acquire(&source, &lease_test_entries(&names)).unwrap();
        assert_eq!(leases.len(), names.len());
        leases.verify_all_held().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn conflicting_truncate_causes_immediate_sigio_exit_before_publication() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(ARCHIVE_V2_META_FILE);
        let unpublished = directory.path().join("must-not-publish");
        fs::write(&path, b"leased-source").unwrap();
        let mut lease_holder = spawn_lease_helper("lease", &path);

        assert!(!unpublished.exists());
        let break_error = OpenOptions::new()
            .write(true)
            .truncate(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(&path)
            .expect_err("a nonblocking conflicting open must request a lease break");
        assert_eq!(break_error.raw_os_error(), Some(libc::EWOULDBLOCK));

        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let status = loop {
            if let Some(status) = lease_holder.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                lease_holder.kill().unwrap();
                let status = lease_holder.wait().unwrap();
                panic!("lease holder did not exit after SIGIO: {status}");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert_eq!(status.code(), Some(LEASE_BREAK_EXIT_CODE));
        assert!(!unpublished.exists());

        let writer = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        drop(writer);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    fn write_pre_archive_fixture(source: &Path) {
        write_pre_archive_fixture_with_blocks(source, 1);
    }

    fn write_pre_archive_fixture_with_blocks(source: &Path, block_count: usize) {
        assert!(block_count > 0);
        fs::create_dir_all(source).unwrap();
        let message = historical_message(HistoricalInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(200_000),
        ));
        let mut blocks = Vec::new();
        let mut index_rows = Vec::with_capacity(block_count);
        for block_number in 0..block_count {
            let slot = 10 + block_number as u64;
            let row = ArchiveV2HotTxRow {
                tx_index: 0,
                flags: 0,
                message_offset: 0,
                message_len: message.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            };
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: block_number as u32 + 1,
                    previous_blockhash_id: block_number as u32,
                    block_time: Some(1_700_000_000 + block_number as i64),
                    block_height: Some(block_number as u64 + 1),
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![row],
                message_bytes: message.clone(),
                metadata_bytes: Vec::new(),
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
            index_rows.push(ArchiveV2HotBlockIndexRow {
                block_id: block_number as u32,
                slot,
                compressed_offset: blocks.len() as u64,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 1,
                first_tx_ordinal: block_number as u64,
                first_signature_ordinal: block_number as u64,
                signature_count: 1,
            });
            blocks.extend_from_slice(&compressed);
        }
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), &blocks).unwrap();
        write_archive_v2_hot_block_index(
            &source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            blocks.len() as u64,
            3,
            0,
            &index_rows,
        )
        .unwrap();

        let signer = [7u8; 32];
        let compute_budget =
            solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes();
        let registry = [signer, compute_budget];
        fs::write(
            source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            registry.concat(),
        )
        .unwrap();
        KeyIndex::build(registry.to_vec())
            .write(&source.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
        fs::write(
            source.join(ARCHIVE_V2_SIGNATURES_FILE),
            vec![9u8; block_count * 64],
        )
        .unwrap();

        let meta = File::create(source.join(ARCHIVE_V2_META_FILE)).unwrap();
        let mut meta = WincodeLeb128FramedWriter::new(meta);
        meta.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }))
        .unwrap();
        meta.write(&ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: block_count as u64,
            transactions: block_count as u64,
            ..WincodeArchiveV2Footer::default()
        }))
        .unwrap();
        meta.flush().unwrap();

        fs::write(
            source.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE),
            b"durable-first-seen",
        )
        .unwrap();
        fs::write(
            source.join(ARCHIVE_V2_PUBKEY_HOT_SEED_FILE),
            b"durable-hot-seed",
        )
        .unwrap();
        for edge in EDGE_TIER_FILES.iter().chain(LEGACY_EDGE_TIER_FILES.iter()) {
            fs::write(source.join(edge), b"stale-edge-offsets").unwrap();
        }
        for control in LEGACY_SOURCE_CONTROL_FILES {
            fs::write(source.join(control), b"legacy-control").unwrap();
        }
        #[cfg(unix)]
        {
            for entry in fs::read_dir(source).unwrap() {
                fs::set_permissions(entry.unwrap().path(), fs::Permissions::from_mode(0o444))
                    .unwrap();
            }
            fs::set_permissions(source, fs::Permissions::from_mode(0o555)).unwrap();
        }
    }

    fn directory_bytes(directory: &Path) -> BTreeMap<String, Vec<u8>> {
        fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().into_string().unwrap(),
                    fs::read(entry.path()).unwrap(),
                )
            })
            .collect()
    }

    fn normalized_message_bytes<S: RangeSource>(reader: &ArchiveReader<S>) -> Vec<u8> {
        let mut blocks = reader.borrowed_blocks();
        let block = blocks.next_block().unwrap().unwrap();
        let row = block.tx_rows().next().unwrap();
        let start = row.message_offset as usize;
        let end = start + row.message_len as usize;
        let message = reader
            .message_projector()
            .decode_owned_message(&block.message_bytes()[start..end])
            .unwrap();
        wincode::config::serialize(&message, wincode_leb128_config()).unwrap()
    }

    fn raw_message_bytes<S: RangeSource>(reader: &ArchiveReader<S>) -> Vec<u8> {
        let mut blocks = reader.borrowed_blocks();
        let block = blocks.next_block().unwrap().unwrap();
        let row = block.tx_rows().next().unwrap();
        let start = row.message_offset as usize;
        let end = start + row.message_len as usize;
        block.message_bytes()[start..end].to_vec()
    }

    fn restore_test_permissions(directory: &Path) {
        #[cfg(unix)]
        {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o755)).unwrap();
            for entry in fs::read_dir(directory).unwrap() {
                fs::set_permissions(entry.unwrap().path(), fs::Permissions::from_mode(0o644))
                    .unwrap();
            }
        }
    }

    #[test]
    fn fast_rewrite_preserves_outer_uncompressed_bytes_except_message_tags() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-1");
        let staging = root.join("staging");
        write_pre_archive_fixture(&source);
        fs::create_dir(&staging).unwrap();
        let reader = ArchiveReader::open_trusted_with_metadata_profile(
            PinnedLocalRangeSource::new(&source),
            TrustedGenerationIdentity {
                cluster_id: "mainnet-beta".to_owned(),
                epoch: 1,
                generation_id: "source-fast-test".to_owned(),
                slots_per_epoch: 10,
                wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            },
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            ReaderOpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..ReaderOpenOptions::default()
            },
        )
        .unwrap();
        let source_row = reader.index().rows[0];
        let report =
            rewrite_blocks(&reader, &staging, 3, DEFAULT_MAX_MESSAGE_BYTES, 0, true).unwrap();
        assert_eq!(report.blocks, 1);
        assert_eq!(report.owned_outer_fallbacks, 0);

        let source_compressed = fs::read(source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let source_uncompressed =
            zstd::bulk::decompress(&source_compressed, source_row.uncompressed_len as usize)
                .unwrap();
        let target_compressed = fs::read(staging.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let target_uncompressed =
            zstd::bulk::decompress(&target_compressed, source_row.uncompressed_len as usize)
                .unwrap();
        assert_eq!(source_uncompressed.len(), target_uncompressed.len());
        assert_eq!(
            source_uncompressed
                .iter()
                .zip(&target_uncompressed)
                .filter(|(source, target)| source != target)
                .count() as u64,
            report.message_mismatch_bytes
        );
        assert_eq!(report.message_mismatch_bytes, 1);
        restore_test_permissions(&source);
    }

    #[test]
    fn parallel_fast_rewrite_matches_serial_bytes_index_and_report() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-1");
        let serial_staging = root.join("serial-staging");
        let parallel_staging = root.join("parallel-staging");
        write_pre_archive_fixture_with_blocks(&source, 5);
        fs::create_dir(&serial_staging).unwrap();
        fs::create_dir(&parallel_staging).unwrap();
        let reader = ArchiveReader::open_trusted_with_metadata_profile(
            PinnedLocalRangeSource::new(&source),
            TrustedGenerationIdentity {
                cluster_id: "mainnet-beta".to_owned(),
                epoch: 1,
                generation_id: "source-fast-parity".to_owned(),
                slots_per_epoch: 10,
                wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            },
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            ReaderOpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..ReaderOpenOptions::default()
            },
        )
        .unwrap();

        let serial = rewrite_blocks(
            &reader,
            &serial_staging,
            3,
            DEFAULT_MAX_MESSAGE_BYTES,
            0,
            true,
        )
        .unwrap();
        let parallel = rewrite_blocks_fast_parallel(
            &reader,
            &parallel_staging,
            3,
            DEFAULT_MAX_MESSAGE_BYTES,
            0,
            2,
        )
        .unwrap();

        assert_eq!(parallel.pipeline.block_count, 5);
        assert_eq!(parallel.pipeline.batch_count, 1);
        assert_eq!(parallel.pipeline.read_call_count, 1);
        assert_eq!(
            serde_json::to_value(&parallel.report).unwrap(),
            serde_json::to_value(&serial).unwrap()
        );
        for name in [ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE] {
            assert_eq!(
                fs::read(serial_staging.join(name)).unwrap(),
                fs::read(parallel_staging.join(name)).unwrap(),
                "parallel output differs for {name}"
            );
        }
        restore_test_permissions(&source);
    }

    #[test]
    fn parallel_fast_rewrite_grows_a_reused_nonzero_compression_buffer() {
        let mut source = Vec::with_capacity(1_024);
        for counter in 0_u64..32 {
            source.extend_from_slice(&Sha256::digest(counter.to_le_bytes()));
        }
        assert_eq!(source.len(), 1_024);

        let mut compressed = Vec::with_capacity(source.len());
        let initial_capacity = compressed.capacity();
        let compress_bound = zstd::zstd_safe::compress_bound(source.len());
        assert!(initial_capacity < compress_bound);
        assert!(compress_bound < initial_capacity * 2);

        clear_and_reserve_exact(&mut compressed, compress_bound).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        compressor
            .compress_to_buffer(&source, &mut compressed)
            .unwrap();

        assert!(compressed.len() > initial_capacity);
        assert!(compressed.capacity() >= compress_bound);
    }

    #[test]
    fn parallel_fast_rewrite_bounds_resources_and_reports_the_first_row_error() {
        assert!(fast_rewrite_resources(0).is_err());
        assert!(fast_rewrite_resources(MAX_ORDERED_PARALLEL_DECODE_WORKERS + 1).is_err());
        let resources = fast_rewrite_resources(DEFAULT_FAST_THREADS).unwrap();
        assert_eq!(resources.output_buffer_count, FAST_OUTPUT_BUFFER_COUNT);
        let retained = resources
            .retained_decompressed_bytes_per_worker
            .checked_mul(resources.threads)
            .unwrap()
            + resources.retained_work_buffer_bytes * (resources.threads * 2)
            + resources.retained_output_buffer_bytes * resources.output_buffer_count;
        assert!(retained < MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES);

        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-1");
        let staging = root.join("parallel-error-staging");
        write_pre_archive_fixture_with_blocks(&source, 3);
        fs::create_dir(&staging).unwrap();
        let reader = ArchiveReader::open_trusted_with_metadata_profile(
            PinnedLocalRangeSource::new(&source),
            TrustedGenerationIdentity {
                cluster_id: "mainnet-beta".to_owned(),
                epoch: 1,
                generation_id: "source-fast-error-order".to_owned(),
                slots_per_epoch: 10,
                wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            },
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            ReaderOpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..ReaderOpenOptions::default()
            },
        )
        .unwrap();
        let error = rewrite_blocks_fast_parallel(&reader, &staging, 3, 1, 0, 3).unwrap_err();
        assert!(error.to_string().contains("slot 10"), "{error:#}");
        assert!(!staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE).exists());
        restore_test_permissions(&source);
    }

    #[test]
    fn parallel_fast_rewrite_recycles_all_tokens_for_a_second_batch() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("epoch-0");
        let staging = root.join("parallel-token-recycle-staging");
        let block_count = FAST_OUTPUT_BUFFER_COUNT + 1;
        write_pre_archive_fixture_with_blocks(&source, block_count);
        fs::create_dir(&staging).unwrap();
        let reader = ArchiveReader::open_trusted_with_metadata_profile(
            PinnedLocalRangeSource::new(&source),
            TrustedGenerationIdentity {
                cluster_id: "mainnet-beta".to_owned(),
                epoch: 0,
                generation_id: "source-fast-token-recycle".to_owned(),
                slots_per_epoch: 10_000,
                wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            },
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            ReaderOpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..ReaderOpenOptions::default()
            },
        )
        .unwrap();

        let parallel = rewrite_blocks_fast_parallel(
            &reader,
            &staging,
            3,
            DEFAULT_MAX_MESSAGE_BYTES,
            0,
            DEFAULT_FAST_THREADS,
        )
        .unwrap();
        let target_index =
            read_archive_v2_hot_block_index(&staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();

        assert_eq!(parallel.pipeline.batch_count, 2);
        assert_eq!(parallel.pipeline.read_call_count, 2);
        assert_eq!(parallel.pipeline.block_count, block_count as u64);
        assert_eq!(parallel.report.blocks, block_count as u64);
        assert_eq!(target_index.rows.len(), block_count);
        assert_eq!(target_index.rows[0].block_id, 0);
        assert_eq!(
            target_index.rows[block_count - 1].block_id,
            block_count as u32 - 1
        );
        assert_eq!(
            target_index.blob_file_bytes,
            parallel.report.target_compressed_bytes
        );
        restore_test_permissions(&source);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn run_converts_and_publishes_one_canonical_post_generation() {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source_root = root.join("source-root");
        let target_root = root.join("target-root");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&target_root).unwrap();
        let source = source_root.join("epoch-1");
        let target = target_root.join("epoch-1");
        let staging = target_root.join(".epoch-1.pre-to-post.staging");
        write_pre_archive_fixture(&source);
        let source_before = directory_bytes(&source);

        let make_args = || Args {
            source: source.clone(),
            source_snapshot_id: Some("test-snapshot".to_owned()),
            source_lease_id: None,
            target: target.clone(),
            staging: staging.clone(),
            epoch: 1,
            cluster_id: "mainnet-beta".to_owned(),
            generation_id: "epoch-1-post-test".to_owned(),
            slots_per_epoch: 10,
            zstd_level: Some(3),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            progress_blocks: 0,
            threads: None,
            source_audit_report: None,
            source_audit_report_sha256: None,
            fast_candidate: false,
        };
        let report = match run(make_args()).unwrap() {
            RunReport::Canonical(report) => report,
            RunReport::Candidate(_) | RunReport::CandidateRecovery(_) => {
                panic!("strict mode returned a candidate report")
            }
        };

        assert_eq!(directory_bytes(&source), source_before);
        assert!(!staging.exists());
        assert!(target.join(GENERATION_MANIFEST_FILE).is_file());
        assert!(
            target
                .join(
                    wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
                        .name
                )
                .is_file()
        );
        for edge in EDGE_TIER_FILES.iter().chain(LEGACY_EDGE_TIER_FILES.iter()) {
            assert!(!target.join(edge).exists());
            assert!(report.omitted_edge_files.iter().any(|name| name == *edge));
        }
        for control in LEGACY_SOURCE_CONTROL_FILES {
            assert!(!target.join(control).exists());
            assert!(
                report
                    .omitted_control_files
                    .iter()
                    .any(|name| name == *control)
            );
        }
        assert!(report.edge_rebuild_required);
        assert_eq!(report.rewrite.message_mismatch_bytes, 1);
        assert_eq!(report.rewrite.source_instruction_data_tag_counts[1], 1);
        assert_eq!(report.source_authority_kind, "provider-read-only-snapshot");
        assert_eq!(report.source_authority_id, "test-snapshot");
        assert!(report.source_provider_snapshot_required);
        assert!(!report.source_linux_read_leases_required);

        let target_manifest =
            GenerationManifest::parse(&fs::read(target.join(GENERATION_MANIFEST_FILE)).unwrap())
                .unwrap();
        let receipt_entry = target_manifest.file(PRE_TO_POST_RECEIPT_FILE).unwrap();
        assert_eq!(
            hash_regular_file(&target.join(PRE_TO_POST_RECEIPT_FILE)).unwrap(),
            FileBinding {
                bytes: receipt_entry.size,
                sha256: receipt_entry.sha256.clone(),
            }
        );
        let receipt: Value =
            serde_json::from_slice(&fs::read(target.join(PRE_TO_POST_RECEIPT_FILE)).unwrap())
                .unwrap();
        for name in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ] {
            assert!(receipt["source_files"][name].is_object());
            assert!(receipt["target_files"][name].is_object());
        }
        assert_eq!(
            receipt["source_profile_decision"],
            "unique-full-generation-decode"
        );
        assert_eq!(
            receipt["source_authority_kind"],
            "provider-read-only-snapshot"
        );
        assert_eq!(receipt["source_authority_id"], "test-snapshot");
        assert_eq!(receipt["source_provider_snapshot_required"], true);
        assert_eq!(receipt["source_linux_read_leases_required"], false);

        let target_reader = ArchiveReader::open_with_metadata_admission(
            PinnedLocalRangeSource::new(&target),
            blockzilla_read_sdk::ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
        )
        .unwrap();
        assert_eq!(
            target_reader.wire_profile(),
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );
        let target_audit =
            audit_full_generation_wire_profile(&target_reader, DEFAULT_MAX_MESSAGE_BYTES).unwrap();
        assert_eq!(
            target_audit.require_unproven_authority().unwrap(),
            UnprovenWireProfileDecision::UniqueFullGenerationDecode
        );

        let source_reader = ArchiveReader::open_trusted_with_metadata_profile(
            PinnedLocalRangeSource::new(&source),
            TrustedGenerationIdentity {
                cluster_id: "mainnet-beta".to_owned(),
                epoch: 1,
                generation_id: "source-test".to_owned(),
                slots_per_epoch: 10,
                wire_profile: ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            },
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            ReaderOpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..ReaderOpenOptions::default()
            },
        )
        .unwrap();
        let canonical_source_message = normalized_message_bytes(&source_reader);
        assert_eq!(
            canonical_source_message,
            normalized_message_bytes(&target_reader)
        );
        assert_eq!(canonical_source_message, raw_message_bytes(&target_reader));
        let mut source_blocks = source_reader.borrowed_blocks();
        let source_block = source_blocks.next_block().unwrap().unwrap();
        let source_header =
            wincode::config::serialize(source_block.header(), wincode_leb128_config()).unwrap();
        let source_rows = source_block.tx_rows().collect::<Vec<_>>();
        let source_metadata = source_block.metadata_bytes().to_vec();
        let mut target_blocks = target_reader.borrowed_blocks();
        let target_block = target_blocks.next_block().unwrap().unwrap();
        assert_eq!(
            source_header,
            wincode::config::serialize(target_block.header(), wincode_leb128_config()).unwrap()
        );
        assert_eq!(
            wincode::config::serialize(&source_rows, wincode_leb128_config()).unwrap(),
            wincode::config::serialize(
                &target_block.tx_rows().collect::<Vec<_>>(),
                wincode_leb128_config()
            )
            .unwrap()
        );
        assert_eq!(source_metadata, target_block.metadata_bytes());

        assert!(run(make_args()).is_err());
        assert_eq!(directory_bytes(&source), source_before);
        restore_test_permissions(&source);
        restore_test_permissions(&target);
    }
}
