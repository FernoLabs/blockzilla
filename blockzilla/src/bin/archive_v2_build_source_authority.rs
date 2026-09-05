//! Build an external, read-only authority inventory for one unmanifested
//! Archive V2 source directory.

#[path = "archive_v2_source_authority_common.rs"]
mod source_authority_common;

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
};
use blockzilla_read_sdk::{
    ArchiveV2MetadataWireProfile, ArchiveV2WireProfile, PinnedLocalEntryKind,
    PinnedLocalInventoryEntry, PinnedLocalRangeSource,
};
use clap::{Parser, ValueEnum};
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags};
use sha2::{Digest, Sha256};

use source_authority_common::{
    AuthorityDisposition, SOURCE_AUTHORITY_KIND, SOURCE_AUTHORITY_SCHEMA_VERSION,
    SourceAuthorityFile, SourceAuthorityInventory, compute_authority_digest, known_disposition,
    looks_like_archive_or_control, validate_flat_name,
};

const IO_BUFFER_BYTES: usize = 8 << 20;
static TEMPORARY_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);
const FIXED_CORE_FILES: [&str; 4] = [
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
];

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MessageWireProfileArg {
    #[value(name = "post-unknown-instruction-fallbacks-v1")]
    PostUnknownInstructionFallbacksV1,
    #[value(name = "pre-unknown-instruction-fallbacks-v1")]
    PreUnknownInstructionFallbacksV1,
}

impl MessageWireProfileArg {
    fn stable_name(self) -> &'static str {
        match self {
            Self::PostUnknownInstructionFallbacksV1 => ArchiveV2WireProfile::POST_UNKNOWN_NAME,
            Self::PreUnknownInstructionFallbacksV1 => ArchiveV2WireProfile::PRE_UNKNOWN_NAME,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetadataWireProfileArg {
    #[value(name = "unmarked-historical-compatibility")]
    UnmarkedHistoricalCompatibility,
}

impl MetadataWireProfileArg {
    fn stable_name(self) -> &'static str {
        match self {
            Self::UnmarkedHistoricalCompatibility => {
                ArchiveV2MetadataWireProfile::HISTORICAL_COMPATIBILITY_NAME
            }
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "archive-v2-build-source-authority",
    version,
    about = "Build an external content-bound authority inventory for an unmanifested Archive V2 source"
)]
struct Args {
    /// Existing Archive V2 source directory. It must be an absolute no-symlink path.
    #[arg(long)]
    source: PathBuf,

    /// New authority JSON file outside the source tree.
    #[arg(long)]
    output: PathBuf,

    /// Operator-selected stable identity for this source authority.
    #[arg(long)]
    source_authority_id: String,

    /// Exact source cluster identity.
    #[arg(long)]
    cluster_id: String,

    /// Exact source epoch.
    #[arg(long)]
    epoch: u64,

    /// Exact number of slots in the epoch.
    #[arg(long)]
    slots_per_epoch: u64,

    /// Complete-generation message wire profile established by external evidence.
    #[arg(long, value_enum)]
    message_wire_profile: MessageWireProfileArg,

    /// Explicit historical metadata compatibility admission for this unmarked source.
    #[arg(long, value_enum)]
    metadata_wire_profile: MetadataWireProfileArg,

    /// Explicit known sidecar or control name to bind. Repeat for each extra file.
    #[arg(long = "include")]
    includes: Vec<String>,
}

#[derive(Debug, Clone)]
struct BuildOptions {
    source: PathBuf,
    output: PathBuf,
    source_authority_id: String,
    cluster_id: String,
    epoch: u64,
    slots_per_epoch: u64,
    message_wire_profile: &'static str,
    metadata_wire_profile: &'static str,
    includes: Vec<String>,
}

struct PreparedOutput {
    display_path: PathBuf,
    parent_display_path: PathBuf,
    parent: File,
    parent_identity: DirectoryIdentity,
    source_identity: DirectoryIdentity,
    name: OsString,
}

struct PreparedAuthority {
    source: PinnedLocalRangeSource,
    source_inventory: Vec<PinnedLocalInventoryEntry>,
    output: PreparedOutput,
    bytes: Vec<u8>,
    sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

impl DirectoryIdentity {
    fn from_file(file: &File) -> Result<Self> {
        let metadata = file.metadata()?;
        ensure!(metadata.is_dir(), "descriptor is not a directory");
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    bytes: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl FileIdentity {
    fn from_file(file: &File) -> Result<Self> {
        let metadata = file.metadata()?;
        ensure!(metadata.is_file(), "descriptor is not a regular file");
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }

    fn same_object_and_size(self, other: Self) -> bool {
        self.device == other.device && self.inode == other.inode && self.bytes == other.bytes
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let options = BuildOptions {
        source: args.source,
        output: args.output,
        source_authority_id: args.source_authority_id,
        cluster_id: args.cluster_id,
        epoch: args.epoch,
        slots_per_epoch: args.slots_per_epoch,
        message_wire_profile: args.message_wire_profile.stable_name(),
        metadata_wire_profile: args.metadata_wire_profile.stable_name(),
        includes: args.includes,
    };
    let prepared = prepare_authority(&options)?;
    let sha256 = publish_authority(prepared)?;
    println!("{sha256}");
    Ok(())
}

fn prepare_authority(options: &BuildOptions) -> Result<PreparedAuthority> {
    validate_options(options)?;
    let source = PinnedLocalRangeSource::open_directory(&options.source)
        .map_err(|error| anyhow!(error))
        .with_context(|| format!("open source root {}", options.source.display()))?;
    let output = prepare_output(&options.output, &source)?;
    let selected = selected_names(&options.includes)?;
    let source_inventory = source
        .inventory()
        .map_err(|error| anyhow!(error))
        .context("inventory source root without following links")?;
    validate_source_inventory(&source_inventory, &selected)?;

    let mut files = Vec::with_capacity(selected.len());
    for name in selected {
        let disposition = known_disposition(&name)
            .with_context(|| format!("selected source name is not known: {name}"))?;
        let file = source
            .open_file(&name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("pin selected source file {name}"))?;
        let (bytes, sha256) = hash_pinned_file(file).with_context(|| format!("hash {name}"))?;
        files.push(SourceAuthorityFile {
            name,
            bytes,
            sha256,
            disposition,
        });
    }

    let mut inventory = SourceAuthorityInventory {
        schema_version: SOURCE_AUTHORITY_SCHEMA_VERSION,
        kind: SOURCE_AUTHORITY_KIND.to_owned(),
        complete: true,
        authority_id: options.source_authority_id.clone(),
        authority_digest: "0".repeat(64),
        cluster_id: options.cluster_id.clone(),
        epoch: options.epoch,
        slots_per_epoch: options.slots_per_epoch,
        message_wire_profile: options.message_wire_profile.to_owned(),
        metadata_wire_profile: options.metadata_wire_profile.to_owned(),
        files,
    };
    inventory.authority_digest = compute_authority_digest(&inventory)?;
    inventory.validate()?;
    let mut bytes = serde_json::to_vec(&inventory)?;
    bytes.push(b'\n');
    let sha256 = hex_lower(&Sha256::digest(&bytes));

    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed while authority bindings were hashed")?;
    ensure!(
        source.inventory().map_err(|error| anyhow!(error))? == source_inventory,
        "source inventory changed while authority bindings were hashed"
    );
    revalidate_output_parent(&output)?;

    Ok(PreparedAuthority {
        source,
        source_inventory,
        output,
        bytes,
        sha256,
    })
}

fn publish_authority(prepared: PreparedAuthority) -> Result<String> {
    revalidate_prepared_source(&prepared)?;
    revalidate_output_parent(&prepared.output)?;

    let temporary_name = unique_temporary_name(&prepared.output.name);
    let result = (|| -> Result<FileIdentity> {
        let descriptor = rustix::fs::openat(
            &prepared.output.parent,
            &temporary_name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )
        .map_err(std::io::Error::from)
        .with_context(|| format!("create temporary authority output {temporary_name:?}"))?;
        let mut file = File::from(descriptor);
        file.write_all(&prepared.bytes)?;
        file.sync_all()?;
        let temporary_identity = FileIdentity::from_file(&file)?;
        ensure!(
            temporary_identity.bytes == prepared.bytes.len() as u64,
            "temporary authority output changed while it was written"
        );
        prepared.output.parent.sync_all()?;

        revalidate_prepared_source(&prepared)?;
        revalidate_output_parent(&prepared.output)?;
        let named_temporary = open_regular_at(&prepared.output.parent, &temporary_name)?;
        ensure!(
            FileIdentity::from_file(&named_temporary)? == temporary_identity,
            "temporary authority output path changed before publication"
        );
        rustix::fs::renameat_with(
            &prepared.output.parent,
            &temporary_name,
            &prepared.output.parent,
            &prepared.output.name,
            RenameFlags::NOREPLACE,
        )
        .map_err(std::io::Error::from)
        .with_context(|| {
            format!(
                "publish authority output without replacing {}",
                prepared.output.display_path.display()
            )
        })?;
        prepared.output.parent.sync_all()?;
        Ok(temporary_identity)
    })();
    if result.is_err() {
        let _ = rustix::fs::unlinkat(&prepared.output.parent, &temporary_name, AtFlags::empty());
        let _ = prepared.output.parent.sync_all();
    }
    let temporary_identity = result?;

    revalidate_prepared_source(&prepared)?;
    revalidate_output_parent(&prepared.output)?;
    let published = open_regular_at(&prepared.output.parent, &prepared.output.name)?;
    ensure!(
        FileIdentity::from_file(&published)?.same_object_and_size(temporary_identity),
        "published authority output identity differs from the prepared file"
    );
    let (bytes, sha256) = hash_pinned_file(published)?;
    ensure!(
        bytes == prepared.bytes.len() as u64 && sha256 == prepared.sha256,
        "published authority output differs from its prepared bytes"
    );
    Ok(prepared.sha256)
}

fn validate_options(options: &BuildOptions) -> Result<()> {
    ensure!(options.source.is_absolute(), "--source must be absolute");
    ensure!(options.output.is_absolute(), "--output must be absolute");
    ensure!(
        !options.source_authority_id.is_empty(),
        "--source-authority-id must not be empty"
    );
    ensure!(
        !options.cluster_id.is_empty(),
        "--cluster-id must not be empty"
    );
    ensure!(
        options.slots_per_epoch > 0,
        "--slots-per-epoch must be positive"
    );
    ensure!(
        matches!(
            options.message_wire_profile,
            ArchiveV2WireProfile::POST_UNKNOWN_NAME | ArchiveV2WireProfile::PRE_UNKNOWN_NAME
        ),
        "unsupported message wire profile"
    );
    ensure!(
        options.metadata_wire_profile
            == ArchiveV2MetadataWireProfile::HISTORICAL_COMPATIBILITY_NAME,
        "unmanifested source authority requires explicit historical metadata compatibility"
    );
    Ok(())
}

fn selected_names(includes: &[String]) -> Result<BTreeSet<String>> {
    let mut selected = FIXED_CORE_FILES
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    for name in includes {
        validate_flat_name(name)?;
        let disposition = known_disposition(name)
            .with_context(|| format!("--include name is not a known archive sidecar: {name}"))?;
        ensure!(
            !matches!(
                disposition,
                AuthorityDisposition::RewriteBlocks | AuthorityDisposition::RewriteHotIndex
            ),
            "core source file {name} is selected automatically"
        );
        ensure!(selected.insert(name.clone()), "duplicate --include {name}");
    }
    Ok(selected)
}

fn validate_source_inventory(
    inventory: &[PinnedLocalInventoryEntry],
    selected: &BTreeSet<String>,
) -> Result<()> {
    let mut observed = BTreeMap::new();
    for entry in inventory {
        let Some(name) = entry.name.to_str() else {
            continue;
        };
        ensure!(
            observed.insert(name.to_owned(), entry).is_none(),
            "source inventory contains a duplicate name"
        );
        if selected.contains(name) {
            ensure!(
                entry.kind == PinnedLocalEntryKind::RegularFile,
                "selected source entry {name} is not a real regular file"
            );
        } else if looks_like_archive_or_control(name) {
            bail!(
                "source contains unbound archive or control entry {name}; add an explicit --include if it is a known source artifact"
            );
        }
    }
    for name in selected {
        ensure!(
            observed.contains_key(name),
            "selected source entry is missing: {name}"
        );
    }
    Ok(())
}

fn hash_pinned_file(mut file: File) -> Result<(u64, String)> {
    let before = FileIdentity::from_file(&file)?;
    file.seek(SeekFrom::Start(0))?;
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("hashed byte count overflow")?;
    }
    let after = FileIdentity::from_file(&file)?;
    ensure!(
        before == after && bytes == before.bytes,
        "file changed while it was hashed"
    );
    Ok((bytes, hex_lower(&hasher.finalize())))
}

fn prepare_output(path: &Path, source: &PinnedLocalRangeSource) -> Result<PreparedOutput> {
    let name = path
        .file_name()
        .context("--output has no final component")?
        .to_os_string();
    ensure!(
        name != OsStr::new(".") && name != OsStr::new("..") && !name.as_bytes().contains(&0),
        "--output has an invalid final component"
    );
    let parent_display_path = path
        .parent()
        .context("--output has no parent")?
        .to_path_buf();
    let parent = open_absolute_directory_nofollow(&parent_display_path)
        .with_context(|| format!("open output parent {}", parent_display_path.display()))?;
    let parent_identity = DirectoryIdentity::from_file(&parent)?;
    let source_identity = source
        .directory_identity()
        .map_err(|error| anyhow!(error))?;
    let source_identity = DirectoryIdentity {
        device: source_identity.device,
        inode: source_identity.inode,
    };
    ensure!(
        !directory_is_at_or_below(&parent, source_identity)?,
        "authority output must be outside the source directory tree"
    );
    match rustix::fs::statat(&parent, &name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => bail!("authority output already exists: {}", path.display()),
        Err(error) if error == rustix::io::Errno::NOENT => {}
        Err(error) => {
            return Err(std::io::Error::from(error))
                .with_context(|| format!("inspect authority output {}", path.display()));
        }
    }
    Ok(PreparedOutput {
        display_path: path.to_path_buf(),
        parent_display_path,
        parent,
        parent_identity,
        source_identity,
        name,
    })
}

fn revalidate_output_parent(output: &PreparedOutput) -> Result<()> {
    ensure!(
        DirectoryIdentity::from_file(&output.parent)? == output.parent_identity,
        "pinned output parent identity changed"
    );
    ensure!(
        !directory_is_at_or_below(&output.parent, output.source_identity)?,
        "authority output parent moved into the source directory tree"
    );
    let reopened = open_absolute_directory_nofollow(&output.parent_display_path)?;
    ensure!(
        DirectoryIdentity::from_file(&reopened)? == output.parent_identity,
        "output parent path changed after it was pinned"
    );
    Ok(())
}

fn revalidate_prepared_source(prepared: &PreparedAuthority) -> Result<()> {
    prepared
        .source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("source changed after authority preparation")?;
    ensure!(
        prepared
            .source
            .inventory()
            .map_err(|error| anyhow!(error))?
            == prepared.source_inventory,
        "source inventory changed after authority preparation"
    );
    Ok(())
}

fn directory_is_at_or_below(
    directory: &File,
    possible_ancestor: DirectoryIdentity,
) -> Result<bool> {
    let mut current = directory.try_clone()?;
    loop {
        let identity = DirectoryIdentity::from_file(&current)?;
        if identity == possible_ancestor {
            return Ok(true);
        }
        let parent = File::from(
            rustix::fs::openat(&current, "..", directory_open_flags(), Mode::empty())
                .map_err(std::io::Error::from)?,
        );
        let parent_identity = DirectoryIdentity::from_file(&parent)?;
        if parent_identity == identity {
            return Ok(false);
        }
        current = parent;
    }
}

fn open_absolute_directory_nofollow(path: &Path) -> Result<File> {
    ensure!(path.is_absolute(), "directory path must be absolute");
    let mut directory = File::from(
        rustix::fs::open("/", directory_open_flags(), Mode::empty())
            .map_err(std::io::Error::from)?,
    );
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = File::from(
                    rustix::fs::openat(&directory, name, directory_open_flags(), Mode::empty())
                        .map_err(std::io::Error::from)?,
                );
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                bail!("directory path must be normalized and contain no parent traversal")
            }
        }
    }
    Ok(directory)
}

fn directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn open_regular_at(directory: &File, name: &OsStr) -> Result<File> {
    let file = File::from(
        rustix::fs::openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)?,
    );
    ensure!(
        file.metadata()?.is_file(),
        "published output is not a regular file"
    );
    Ok(file)
}

fn unique_temporary_name(final_name: &OsStr) -> OsString {
    let mut name = OsString::from(".");
    name.push(final_name);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let counter = TEMPORARY_NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    name.push(format!(
        ".tmp.{}.{}.{}",
        std::process::id(),
        timestamp,
        counter
    ));
    name
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
    use super::*;
    use std::{fs, os::unix::fs::symlink};
    use tempfile::TempDir;

    fn write_source(root: &Path) {
        fs::create_dir(root).unwrap();
        for (name, bytes) in [
            (ARCHIVE_V2_BLOCKS_FILE, b"blocks".as_slice()),
            (ARCHIVE_V2_BLOCK_INDEX_FILE, b"index".as_slice()),
            (ARCHIVE_V2_META_FILE, b"meta".as_slice()),
            (ARCHIVE_V2_PUBKEY_REGISTRY_FILE, b"registry".as_slice()),
        ] {
            fs::write(root.join(name), bytes).unwrap();
        }
    }

    fn options(source: &Path, output: &Path) -> BuildOptions {
        BuildOptions {
            source: source.to_path_buf(),
            output: output.to_path_buf(),
            source_authority_id: "test-authority".to_owned(),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 900,
            slots_per_epoch: 432_000,
            message_wire_profile: ArchiveV2WireProfile::POST_UNKNOWN_NAME,
            metadata_wire_profile: ArchiveV2MetadataWireProfile::HISTORICAL_COMPATIBILITY_NAME,
            includes: Vec::new(),
        }
    }

    fn directory_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
        fs::read_dir(root)
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

    fn directory_stat(root: &Path) -> (u64, u64, i64, i64, i64, i64) {
        let metadata = fs::metadata(root).unwrap();
        (
            metadata.dev(),
            metadata.ino(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    }

    #[test]
    fn deterministic_authority_bytes_and_hash_are_exact() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let first_source = root.join("source-a");
        let second_source = root.join("source-b");
        write_source(&first_source);
        write_source(&second_source);
        let first_output = root.join("authority-a.json");
        let second_output = root.join("authority-b.json");

        let first_hash =
            publish_authority(prepare_authority(&options(&first_source, &first_output)).unwrap())
                .unwrap();
        let second_hash =
            publish_authority(prepare_authority(&options(&second_source, &second_output)).unwrap())
                .unwrap();
        let first = fs::read(&first_output).unwrap();
        let second = fs::read(&second_output).unwrap();

        const EXPECTED: &str = concat!(
            "{\"schema_version\":1,\"kind\":\"archive-v2-source-authority-inventory\",",
            "\"complete\":true,\"authority_id\":\"test-authority\",",
            "\"authority_digest\":\"8bb74c7dbd91afcdec200ec51bfc125f07a3a41b8e887d64e32a7da968729125\",",
            "\"cluster_id\":\"mainnet-beta\",\"epoch\":900,\"slots_per_epoch\":432000,",
            "\"message_wire_profile\":\"post-unknown-instruction-fallbacks-v1\",",
            "\"metadata_wire_profile\":\"unmarked-historical-compatibility\",\"files\":[",
            "{\"name\":\"archive-v2-blocks.index\",\"bytes\":5,",
            "\"sha256\":\"1bc04b5291c26a46d918139138b992d2de976d6851d0893b0476b85bfbdfc6e6\",",
            "\"disposition\":\"rewrite-hot-index\"},",
            "{\"name\":\"archive-v2-blocks.zstd\",\"bytes\":6,",
            "\"sha256\":\"2a12da17d27cd05ab0f3148816c1b4a702334202e82c5ad0dff734cb45db8017\",",
            "\"disposition\":\"rewrite-blocks\"},",
            "{\"name\":\"archive-v2-meta.wincode\",\"bytes\":4,",
            "\"sha256\":\"ea3bd73e2b506e00527232b3ed743c066da83a8e3066f62a71e75eb9b4aa1db6\",",
            "\"disposition\":\"copy-sidecar\"},",
            "{\"name\":\"registry.bin\",\"bytes\":8,",
            "\"sha256\":\"872491a30d60d598962de6e7b834ab76b2aa65fbab102c6ebaaae6acdc238822\",",
            "\"disposition\":\"copy-sidecar\"}]}\n"
        );

        assert_eq!(first, second);
        assert_eq!(first, EXPECTED.as_bytes());
        assert_eq!(
            first_hash,
            "3f85cff1150fc97501de5ee13a0dfbfd40655e27396f6d9f3fdaf3ed6e31f9ac"
        );
        assert_eq!(first_hash, second_hash);
        assert_eq!(first_hash, hex_lower(&Sha256::digest(&first)));
        assert_eq!(first.last(), Some(&b'\n'));
        let inventory: SourceAuthorityInventory = serde_json::from_slice(&first).unwrap();
        inventory.validate().unwrap();
    }

    #[test]
    fn source_is_never_modified() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);
        let before = directory_bytes(&source);
        let before_stat = directory_stat(&source);
        let before_inventory = PinnedLocalRangeSource::open_directory(&source)
            .unwrap()
            .inventory()
            .unwrap();

        publish_authority(
            prepare_authority(&options(&source, &root.join("authority.json"))).unwrap(),
        )
        .unwrap();

        assert_eq!(directory_bytes(&source), before);
        assert_eq!(directory_stat(&source), before_stat);
        assert_eq!(
            PinnedLocalRangeSource::open_directory(&source)
                .unwrap()
                .inventory()
                .unwrap(),
            before_inventory
        );
    }

    #[test]
    fn unknown_include_and_unbound_archive_entries_are_rejected() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);
        let mut unknown = options(&source, &root.join("unknown.json"));
        unknown.includes.push("unknown-sidecar.bin".to_owned());
        assert!(prepare_authority(&unknown).is_err());

        fs::write(source.join("archive-v2-unbound.bin"), b"unbound").unwrap();
        assert!(prepare_authority(&options(&source, &root.join("unbound.json"))).is_err());
    }

    #[test]
    fn symlink_source_and_root_swap_are_rejected() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);
        symlink(&source, root.join("source-link")).unwrap();
        assert!(
            prepare_authority(&options(
                &root.join("source-link"),
                &root.join("linked.json")
            ))
            .is_err()
        );

        let output = root.join("swapped.json");
        let prepared = prepare_authority(&options(&source, &output)).unwrap();
        fs::rename(&source, root.join("source-old")).unwrap();
        write_source(&source);
        assert!(publish_authority(prepared).is_err());
        assert!(!output.exists());
    }

    #[test]
    fn output_collision_never_replaces_existing_bytes() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);
        let output = root.join("authority.json");
        fs::write(&output, b"existing").unwrap();

        assert!(prepare_authority(&options(&source, &output)).is_err());
        assert_eq!(fs::read(output).unwrap(), b"existing");

        let late_output = root.join("late-authority.json");
        let prepared = prepare_authority(&options(&source, &late_output)).unwrap();
        fs::write(&late_output, b"late-racer").unwrap();
        assert!(publish_authority(prepared).is_err());
        assert_eq!(fs::read(&late_output).unwrap(), b"late-racer");
        assert!(!fs::read_dir(&root).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".late-authority.json.tmp.")
        }));
    }

    #[test]
    fn selected_object_and_output_parent_swaps_are_rejected() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);

        let object_output = root.join("object-swap.json");
        let object_prepared = prepare_authority(&options(&source, &object_output)).unwrap();
        fs::rename(
            source.join(ARCHIVE_V2_META_FILE),
            source.join("old-meta.wincode"),
        )
        .unwrap();
        fs::write(source.join(ARCHIVE_V2_META_FILE), b"replacement").unwrap();
        assert!(publish_authority(object_prepared).is_err());
        assert!(!object_output.exists());

        fs::remove_file(source.join(ARCHIVE_V2_META_FILE)).unwrap();
        fs::rename(
            source.join("old-meta.wincode"),
            source.join(ARCHIVE_V2_META_FILE),
        )
        .unwrap();
        let output_parent = root.join("output-parent");
        fs::create_dir(&output_parent).unwrap();
        let parent_output = output_parent.join("authority.json");
        let parent_prepared = prepare_authority(&options(&source, &parent_output)).unwrap();
        let moved_parent = root.join("old-output-parent");
        fs::rename(&output_parent, &moved_parent).unwrap();
        fs::create_dir(&output_parent).unwrap();
        assert!(publish_authority(parent_prepared).is_err());
        assert!(!parent_output.exists());
        assert!(!moved_parent.join("authority.json").exists());
    }

    #[test]
    fn output_alias_containment_and_symlink_parent_are_rejected() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);
        assert!(prepare_authority(&options(&source, &source.join("authority.json"))).is_err());
        assert!(prepare_authority(&options(&source, &source)).is_err());

        let real_output = root.join("real-output");
        fs::create_dir(&real_output).unwrap();
        symlink(&real_output, root.join("linked-output")).unwrap();
        assert!(
            prepare_authority(&options(
                &source,
                &root.join("linked-output/authority.json")
            ))
            .is_err()
        );
    }

    #[test]
    fn explicit_known_sidecar_is_bound_and_unrelated_debris_is_ignored() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let source = root.join("source");
        write_source(&source);
        fs::write(
            source.join(blockzilla_format::ARCHIVE_V2_SIGNATURES_FILE),
            b"signatures",
        )
        .unwrap();
        fs::write(source.join("notes.txt"), b"unrelated").unwrap();
        let output = root.join("authority.json");
        let mut options = options(&source, &output);
        options
            .includes
            .push(blockzilla_format::ARCHIVE_V2_SIGNATURES_FILE.to_owned());

        publish_authority(prepare_authority(&options).unwrap()).unwrap();
        let inventory: SourceAuthorityInventory =
            serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        assert!(
            inventory
                .files
                .iter()
                .any(|file| file.name == blockzilla_format::ARCHIVE_V2_SIGNATURES_FILE)
        );
        assert!(!inventory.files.iter().any(|file| file.name == "notes.txt"));
    }
}
