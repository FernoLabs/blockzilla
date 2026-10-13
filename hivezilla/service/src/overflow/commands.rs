//! Explicit bounded R2 transfer commands. No ACK or cleanup.

use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use clap::Args;
use hivezilla_object_store::{
    r2::{MAX_R2_OBJECT_BYTES, R2ObjectStore},
    uploader::{S3Client, StorageSettings},
};
use hivezilla_protocol::{OverflowNamespaceSha256, SealedSegmentV1, StreamId, StreamManifestV1};
use serde::Serialize;

use super::grpc::{
    GrpcRawOverflowLimits, GrpcRawOverflowReport, GrpcRawOverflowSource, spill_grpc_raw,
};
use super::{OverflowCatalog, OverflowLimits, hex, validate_reader_id};

#[derive(Debug, Args)]
pub struct R2OverflowArgs {
    /// Private overflow catalog directory. Recovery requires an existing catalog.
    #[arg(long)]
    pub state_dir: PathBuf,
    /// R2 settings file. Otherwise use the existing uploader's environment settings.
    #[arg(long)]
    pub credentials_file: Option<PathBuf>,
    /// Private R2 object prefix, without a leading or trailing slash.
    #[arg(long)]
    pub remote_prefix: String,
    #[arg(long, default_value_t = MAX_R2_OBJECT_BYTES)]
    pub max_segment_bytes: u64,
    #[arg(long, default_value_t = 3)]
    pub retries: u32,
    #[arg(long)]
    pub reader_id: String,
}

impl R2OverflowArgs {
    fn validate(&self) -> Result<()> {
        validate_reader_id(&self.reader_id)?;
        ensure!(
            (1..=MAX_R2_OBJECT_BYTES).contains(&self.max_segment_bytes),
            "segment byte limit must be 1..={MAX_R2_OBJECT_BYTES}"
        );
        Ok(())
    }

    fn store(&self) -> Result<R2ObjectStore> {
        let settings = StorageSettings::load(self.credentials_file.as_deref(), Some("r2"))?;
        let client = S3Client::new(settings, self.retries)?;
        Ok(R2ObjectStore::new(
            client,
            self.remote_prefix.clone(),
            self.max_segment_bytes,
        )?)
    }

    fn limits(&self) -> OverflowLimits {
        OverflowLimits {
            max_segment_bytes: self.max_segment_bytes,
            ..OverflowLimits::default()
        }
    }
}

#[derive(Debug, Args)]
pub struct UploadOverflowSegmentArgs {
    #[command(flatten)]
    pub storage: R2OverflowArgs,
    /// One sealed HIVESEG1 segment. Legacy recorder WAL files require an explicit adapter.
    #[arg(long)]
    pub segment: PathBuf,
}

#[derive(Debug, Args)]
pub struct RecoverOverflowSegmentArgs {
    #[command(flatten)]
    pub storage: R2OverflowArgs,
    /// Stream identity as 32 hexadecimal characters.
    #[arg(long, value_parser = parse_stream_id)]
    pub stream_id: StreamId,
    /// Record sequence contained in the segment to recover.
    #[arg(long)]
    pub sequence: u64,
    /// New output file in an existing directory. Existing files are never replaced.
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct SpillGrpcRawOverflowArgs {
    #[command(flatten)]
    pub storage: R2OverflowArgs,
    /// Trusted canonical StreamManifestV1, with this adapter's producer descriptor.
    #[arg(long)]
    pub manifest: PathBuf,
    /// One committed recorder generation; use --cache-root to follow rotation.
    #[arg(
        long,
        conflicts_with = "cache_root",
        required_unless_present = "cache_root"
    )]
    pub source_dir: Option<PathBuf>,
    /// Hot recorder root containing active/ and sealed/ generations.
    #[arg(
        long,
        conflicts_with = "source_dir",
        required_unless_present = "source_dir"
    )]
    pub cache_root: Option<PathBuf>,
    #[arg(long, default_value_t = 256, value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub max_records_per_segment: u16,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub max_segments: u16,
}

pub fn spill_grpc(args: SpillGrpcRawOverflowArgs) -> Result<GrpcRawOverflowReport> {
    args.storage.validate()?;
    let manifest = StreamManifestV1::decode(&read_segment_file(&args.manifest, 1024 * 1024)?)
        .context("input must be a canonical StreamManifestV1")?;
    let source = match (args.source_dir, args.cache_root) {
        (Some(path), None) => GrpcRawOverflowSource::Generation(path),
        (None, Some(path)) => GrpcRawOverflowSource::HotCache(path),
        _ => anyhow::bail!("select exactly one of source-dir and cache-root"),
    };
    let store = args.storage.store()?;
    let mut catalog = OverflowCatalog::open(
        &args.storage.state_dir,
        OverflowNamespaceSha256::new(store.namespace_sha256()),
        args.storage.limits(),
    )?;
    spill_grpc_raw(
        &mut catalog,
        &args.storage.reader_id,
        &manifest,
        source,
        GrpcRawOverflowLimits {
            max_records_per_segment: usize::from(args.max_records_per_segment),
            max_segments_per_pass: usize::from(args.max_segments),
        },
        &store,
    )
}

#[derive(Debug, Serialize)]
pub struct OverflowTransferReport {
    pub reader_id: String,
    pub stream_id: String,
    pub first_sequence: u64,
    pub next_sequence: u64,
    pub encoded_bytes: u64,
    pub encoded_sha256: String,
}

pub fn upload(args: UploadOverflowSegmentArgs) -> Result<OverflowTransferReport> {
    args.storage.validate()?;
    let bytes = read_segment_file(&args.segment, args.storage.max_segment_bytes)?;
    let segment = SealedSegmentV1::decode(&bytes)
        .context("input must be a complete sealed HIVESEG1 segment")?;
    drop(bytes);
    let store = args.storage.store()?;
    let mut catalog = OverflowCatalog::open(
        &args.storage.state_dir,
        OverflowNamespaceSha256::new(store.namespace_sha256()),
        args.storage.limits(),
    )?;
    let object = catalog.upload(&args.storage.reader_id, &segment, &store)?;
    Ok(report(args.storage.reader_id, &object))
}

pub fn recover(args: RecoverOverflowSegmentArgs) -> Result<OverflowTransferReport> {
    args.storage.validate()?;
    ensure!(
        fs::symlink_metadata(&args.output)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        "recovery output already exists or cannot be inspected"
    );
    let parent = args
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = parent
        .canonicalize()
        .context("recovery output directory must exist")?;
    let output = parent.join(
        args.output
            .file_name()
            .context("recovery output must name a file")?,
    );
    let store = args.storage.store()?;
    let catalog = OverflowCatalog::open_read_only(
        &args.storage.state_dir,
        OverflowNamespaceSha256::new(store.namespace_sha256()),
        args.storage.limits(),
    )?;
    let segment = catalog.read_segment(
        &args.storage.reader_id,
        args.stream_id,
        args.sequence,
        &store,
    )?;
    let object = catalog
        .containing(&args.storage.reader_id, args.stream_id, args.sequence)?
        .context("recovered range disappeared")?
        .object;
    // Publish only after full segment validation. On failure the destination stays absent.
    let mut temporary = tempfile::NamedTempFile::new_in(&parent)?;
    temporary.write_all(&segment.encode())?;
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(&output)
        .context("cannot publish recovered segment")?;
    fs::File::open(&parent)?.sync_all()?;
    Ok(report(args.storage.reader_id, &object))
}

fn read_segment_file(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "segment input must be a regular file");
    ensure!(
        metadata.len() <= maximum,
        "segment input exceeds byte limit"
    );
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "segment input length changed"
    );
    ensure!(
        bytes.len() as u64 <= maximum,
        "segment input exceeds byte limit"
    );
    Ok(bytes)
}

fn report(
    reader_id: String,
    object: &hivezilla_protocol::OverflowObjectV1,
) -> OverflowTransferReport {
    OverflowTransferReport {
        reader_id,
        stream_id: hex(object.stream_id().as_bytes()),
        first_sequence: object.start().next_sequence(),
        next_sequence: object.end().next_sequence(),
        encoded_bytes: object.encoded_len(),
        encoded_sha256: hex(object.encoded_sha256()),
    }
}

fn parse_stream_id(value: &str) -> Result<StreamId, String> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("stream ID must be 32 hexadecimal characters".into());
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "invalid stream ID")?;
    }
    Ok(StreamId::new(bytes))
}
