use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use blockzilla_token_transaction_dump::{
    ACCOUNTS_FILE, DUMP_MANIFEST_FILE, DUMP_SCHEMA_VERSION, DumpArtifactKind, DumpManifest,
    DumpSourceBinding, DumpWireProfile, PUBKEY_REGISTRY_FILE, SIGNATURES_FILE, TRANSACTIONS_FILE,
};
use sha2::{Digest, Sha256};

use crate::index_format::{hex_digest, parse_hex_digest};

const MAX_MANIFEST_BYTES: u64 = 16 << 20;
const KEY_BYTES: u64 = 32;
const SIGNATURE_BYTES: u64 = 64;
const IO_BUFFER_BYTES: usize = 8 << 20;

#[derive(Debug, Clone)]
pub(crate) struct PinnedSourceFile {
    path: PathBuf,
    file: Arc<File>,
    stamp: FileStamp,
}

impl PinnedSourceFile {
    pub(crate) fn open(path: &Path, label: &str) -> Result<Self> {
        ensure_regular_file(path, label)?;
        let file =
            Arc::new(File::open(path).with_context(|| format!("open {label} {}", path.display()))?);
        let stamp = FileStamp::from_metadata(&file.metadata()?);
        let current = FileStamp::from_metadata(&fs::metadata(path)?);
        ensure!(
            stamp == current,
            "{label} path changed while its handle was opened"
        );
        Ok(Self {
            path: path.to_path_buf(),
            file,
            stamp,
        })
    }

    pub fn file(&self) -> &File {
        self.file.as_ref()
    }

    pub const fn len(&self) -> u64 {
        self.stamp.len
    }

    pub(crate) fn read_bounded(&self, maximum: u64) -> Result<Vec<u8>> {
        ensure!(
            self.len() <= maximum,
            "{} exceeds its size limit",
            self.path.display()
        );
        let capacity = usize::try_from(self.len()).context("input length exceeds usize")?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .context("reserve bounded input buffer")?;
        bytes.resize(capacity, 0);
        positioned_read_exact(self.file(), &mut bytes, 0)?;
        ensure!(
            FileStamp::from_metadata(&self.file.metadata()?) == self.stamp,
            "{} changed while it was read",
            self.path.display()
        );
        Ok(bytes)
    }

    pub fn verify_identity(&self, label: &str) -> Result<()> {
        ensure!(
            FileStamp::from_metadata(&self.file.metadata()?) == self.stamp,
            "open {label} changed during use"
        );
        ensure_regular_file(&self.path, label)?;
        ensure!(
            FileStamp::from_metadata(&fs::metadata(&self.path)?) == self.stamp,
            "current {label} path no longer names the pinned source file"
        );
        Ok(())
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
}

#[cfg(unix)]
impl FileStamp {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;

        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mtime: metadata.mtime(),
            mtime_nsec: metadata.mtime_nsec(),
        }
    }
}

#[cfg(not(unix))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<std::time::SystemTime>,
}

#[cfg(not(unix))]
impl FileStamp {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourceDump {
    pub root: PathBuf,
    pub manifest: DumpManifest,
    pub manifest_sha256: [u8; 32],
    pub transaction_sha256: [u8; 32],
    pub signature_sha256: [u8; 32],
    pub registry_sha256: [u8; 32],
    pub accounts_sha256: [u8; 32],
    pub transaction_bytes: u64,
    pub signature_bytes: u64,
    pub registry_bytes: u64,
    pub accounts_bytes: u64,
    pub signatures: u64,
    pub pubkeys: u64,
    pub mint: [u8; 32],
    pub mint_signature: [u8; 64],
    pub manifest_handle: PinnedSourceFile,
    pub transaction_handle: PinnedSourceFile,
    pub signature_handle: PinnedSourceFile,
    pub registry_handle: PinnedSourceFile,
    pub accounts_handle: PinnedSourceFile,
}

impl SourceDump {
    pub fn verify_file_identities(&self) -> Result<()> {
        for (file, label) in [
            (&self.manifest_handle, "source manifest"),
            (&self.transaction_handle, "transaction stream"),
            (&self.signature_handle, "signature stream"),
            (&self.registry_handle, "public-key registry"),
            (&self.accounts_handle, "account list"),
        ] {
            file.verify_identity(label)?;
        }
        Ok(())
    }

    pub fn validate_record_binding(
        &self,
        epoch: u64,
        slot: u64,
        source_block_id: u32,
        wire_profile: DumpWireProfile,
    ) -> Result<()> {
        ensure!(
            (self.manifest.first_epoch..=self.manifest.last_epoch).contains(&epoch),
            "transaction epoch is outside the manifest range"
        );
        let DumpSourceBinding::TrustedLocalSizesOnly {
            slots_per_epoch,
            wire_profile: expected_profile,
            ..
        } = &self.manifest.source_binding;
        let first_slot = epoch
            .checked_mul(*slots_per_epoch)
            .context("source epoch first slot overflow")?;
        ensure!(
            wire_profile == *expected_profile
                && slot >= first_slot
                && slot - first_slot < *slots_per_epoch
                && u64::from(source_block_id) < *slots_per_epoch,
            "transaction differs from its trusted source binding"
        );
        Ok(())
    }
}

pub(crate) fn load_source_dump(path: &Path) -> Result<SourceDump> {
    let root = fs::canonicalize(path)
        .with_context(|| format!("resolve consolidated dump {}", path.display()))?;
    ensure!(root.is_dir(), "consolidated dump is not a directory");
    let manifest_path = root.join(DUMP_MANIFEST_FILE);
    let manifest_handle = PinnedSourceFile::open(&manifest_path, "source manifest")?;
    let manifest_bytes = manifest_handle.read_bounded(MAX_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    let manifest: DumpManifest =
        serde_json::from_slice(&manifest_bytes).context("parse consolidated manifest")?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.workers != 0
            && manifest.first_epoch <= manifest.last_epoch
            && manifest.transactions != 0,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.signature_stream.as_deref() == Some(SIGNATURES_FILE)
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "consolidated manifest file bindings are not the exact relative names"
    );
    validate_source_binding(&manifest.source_binding)?;

    let transaction_sha256 = parse_hex_digest(
        manifest
            .transaction_stream_sha256
            .as_deref()
            .context("consolidated manifest has no transaction digest")?,
        "transaction digest",
    )?;
    let signature_sha256 = parse_hex_digest(
        manifest
            .signature_stream_sha256
            .as_deref()
            .context("consolidated manifest has no signature digest")?,
        "signature digest",
    )?;
    let registry_sha256 = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("consolidated manifest has no registry digest")?,
        "registry digest",
    )?;
    let accounts_sha256 = parse_hex_digest(
        manifest
            .discovered_accounts_sha256
            .as_deref()
            .context("consolidated manifest has no accounts digest")?,
        "accounts digest",
    )?;
    let signatures = manifest
        .signatures
        .context("consolidated manifest has no signature count")?;
    let pubkeys = manifest
        .pubkeys
        .context("consolidated manifest has no public-key count")?;
    ensure!(
        signatures != 0 && pubkeys != 0 && pubkeys < u64::from(u32::MAX),
        "consolidated manifest has invalid sidecar counts"
    );

    let transaction_path = root.join(TRANSACTIONS_FILE);
    let signature_path = root.join(SIGNATURES_FILE);
    let registry_path = root.join(PUBKEY_REGISTRY_FILE);
    let accounts_path = root.join(ACCOUNTS_FILE);
    let transaction_handle = PinnedSourceFile::open(&transaction_path, "transaction stream")?;
    let signature_handle = PinnedSourceFile::open(&signature_path, "signature stream")?;
    let registry_handle = PinnedSourceFile::open(&registry_path, "public-key registry")?;
    let accounts_handle = PinnedSourceFile::open(&accounts_path, "account list")?;
    let transaction_bytes = transaction_handle.len();
    let signature_bytes = signature_handle.len();
    let registry_bytes = registry_handle.len();
    let accounts_bytes = accounts_handle.len();
    ensure!(transaction_bytes != 0, "transaction stream is empty");
    ensure!(accounts_bytes != 0, "account list is empty");
    ensure!(
        signature_bytes
            == signatures
                .checked_mul(SIGNATURE_BYTES)
                .context("signature sidecar byte length overflow")?,
        "signature sidecar size differs from its manifest"
    );
    ensure!(
        registry_bytes
            == pubkeys
                .checked_mul(KEY_BYTES)
                .context("registry byte length overflow")?,
        "registry size differs from its manifest"
    );

    let mint = decode_base58_exact::<32>(&manifest.mint, "mint")?;
    let mint_signature = decode_base58_exact::<64>(&manifest.mint_signature, "mint signature")?;
    Ok(SourceDump {
        root,
        manifest,
        manifest_sha256,
        transaction_sha256,
        signature_sha256,
        registry_sha256,
        accounts_sha256,
        transaction_bytes,
        signature_bytes,
        registry_bytes,
        accounts_bytes,
        signatures,
        pubkeys,
        mint,
        mint_signature,
        manifest_handle,
        transaction_handle,
        signature_handle,
        registry_handle,
        accounts_handle,
    })
}

pub(crate) fn require_hash(
    file: &PinnedSourceFile,
    expected_sha256: [u8; 32],
    label: &str,
) -> Result<()> {
    let actual = hash_pinned_file(file)?;
    ensure!(
        actual == expected_sha256,
        "{label} digest differs: expected {}, got {}",
        hex_digest(expected_sha256),
        hex_digest(actual)
    );
    Ok(())
}

pub(crate) fn hash_pinned_file(file: &PinnedSourceFile) -> Result<[u8; 32]> {
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    while offset < file.len() {
        let remaining = usize::try_from((file.len() - offset).min(IO_BUFFER_BYTES as u64))
            .expect("bounded hash buffer length fits usize");
        let read = positioned_read(file.file(), &mut buffer[..remaining], offset)?;
        ensure!(
            read != 0,
            "{} became shorter while hashing",
            file.path.display()
        );
        hasher.update(&buffer[..read]);
        offset = offset
            .checked_add(u64::try_from(read).context("hashed byte count exceeds u64")?)
            .context("hashed byte count overflow")?;
    }
    ensure!(offset == file.len(), "{} size changed", file.path.display());
    file.verify_identity("hashed source file")?;
    Ok(hasher.finalize().into())
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn validate_source_binding(binding: &DumpSourceBinding) -> Result<()> {
    let DumpSourceBinding::TrustedLocalSizesOnly {
        cluster_id,
        slots_per_epoch,
        ..
    } = binding;
    ensure!(
        !cluster_id.is_empty() && (1..=1_000_000).contains(slots_per_epoch),
        "invalid trusted-local source binding"
    );
    Ok(())
}

fn ensure_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "{label} is not a regular file"
    );
    Ok(())
}

fn decode_base58_exact<const N: usize>(value: &str, label: &str) -> Result<[u8; N]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode {label} as base58"))?;
    ensure!(bytes.len() == N, "{label} byte length differs");
    Ok(bytes.try_into().expect("validated base58 byte length"))
}

#[cfg(unix)]
fn positioned_read(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(bytes, offset)
}

#[cfg(windows)]
fn positioned_read(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(bytes, offset)
}

#[cfg(not(any(unix, windows)))]
fn positioned_read(_file: &File, _bytes: &mut [u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positioned file reads are not supported on this platform",
    ))
}

fn positioned_read_exact(file: &File, mut bytes: &mut [u8], mut offset: u64) -> Result<()> {
    while !bytes.is_empty() {
        let read = positioned_read(file, bytes, offset)?;
        ensure!(read != 0, "positioned source read reached end of file");
        offset = offset
            .checked_add(u64::try_from(read).context("source read length exceeds u64")?)
            .context("source read offset overflow")?;
        bytes = &mut bytes[read..];
    }
    Ok(())
}
