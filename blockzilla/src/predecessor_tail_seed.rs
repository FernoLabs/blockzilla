use anyhow::{Context, Result, anyhow, bail};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC, ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_HOT_INDEX_HEADER_LEN, ARCHIVE_V2_HOT_INDEX_MAGIC,
    ARCHIVE_V2_HOT_INDEX_ROW_LEN, ARCHIVE_V2_HOT_INDEX_VERSION, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, Metadata, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const TAIL_CAPACITY: usize = 300;
const TAIL_ROW_LEN: usize = 32 + 8;
const REGISTRY_KEY_BYTES: u64 = 32;
const OWNERSHIP_FILE: &str = ".hivezilla-pipeline-owned.v1.json";
const LEGACY_BLOCKHASH_LOCK_DIR: &str = ".blockhash.lock";
const TAIL_STAGING_DIR: &str = ".tail-seed-staging";
const REGISTRY_INDEX_MAGIC: &[u8; 8] = b"BZKIDX1!";
const REGISTRY_INDEX_VERSION: u16 = 2;
const REGISTRY_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8;
const VERIFY_CHUNK_ROWS: usize = 8_192;
const RECEIPT_SCHEMA_VERSION: u64 = 4;
const MAX_RECEIPT_BYTES: u64 = 64 * 1024;
const MAX_OWNERSHIP_BYTES: u64 = 64 * 1024;
const OWNERSHIP_SCHEMA_VERSION: u32 = 2;
const HOT_SOURCE_FORMAT: &str = "hot_index_v1";
const V3_SOURCE_FORMAT: &str = "blockhash_index_v3";
const FIRST_SEEN_REGISTRY_ORDER: &str = "registry_order=first_seen_v1";

const REQUIRED_TARGET_FILES: &[&str] = &[
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
];

const ALLOWED_TARGET_FILES: &[&str] = &[
    OWNERSHIP_FILE,
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    "block-time-gaps.bin",
    ".block-time-gaps.bin.lock",
    ".complete-hot-v2-shredding-sidecar-v2",
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    "poh.wincode",
    ARCHIVE_V2_SHREDDING_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
];

const COMPLETE_TARGET_NONEMPTY_FILES: &[&str] = &[
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_SHREDDING_FILE,
];

#[derive(Debug, Deserialize)]
struct OwnershipMarker {
    schema_version: u32,
    kind: String,
    id: String,
    state: String,
    #[serde(default)]
    pid: Option<u32>,
}

pub struct SeedPreviousBlockhashTailsConfig<'a> {
    pub archive_root: &'a Path,
    pub epochs: &'a [u64],
    pub discover: bool,
    pub start_epoch: Option<u64>,
    pub end_epoch: Option<u64>,
    pub receipt_dir: Option<&'a Path>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedRunOutcome {
    pub candidates: usize,
    pub errors: usize,
}

type Receipt = BTreeMap<String, Value>;

pub fn seed_previous_blockhash_tails(
    config: SeedPreviousBlockhashTailsConfig<'_>,
) -> Result<SeedRunOutcome> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    seed_previous_blockhash_tails_to(config, &mut output)
}

fn seed_previous_blockhash_tails_to<W: Write>(
    config: SeedPreviousBlockhashTailsConfig<'_>,
    output: &mut W,
) -> Result<SeedRunOutcome> {
    let mut epochs = if config.discover {
        if !config.epochs.is_empty() {
            bail!("--epochs cannot be combined with --discover");
        }
        let start_epoch = config
            .start_epoch
            .ok_or_else(|| anyhow!("--discover requires --start-epoch and --end-epoch"))?;
        let end_epoch = config
            .end_epoch
            .ok_or_else(|| anyhow!("--discover requires --start-epoch and --end-epoch"))?;
        if config.receipt_dir.is_none() && !config.dry_run {
            bail!("--discover requires --receipt-dir unless --dry-run is used");
        }
        discover_epochs(
            config.archive_root,
            start_epoch,
            end_epoch,
            config.receipt_dir,
        )?
    } else {
        if config.start_epoch.is_some() || config.end_epoch.is_some() {
            bail!("--start-epoch/--end-epoch are only valid with --discover");
        }
        if config.epochs.is_empty() {
            bail!("one of --epochs or --discover is required");
        }
        let mut epochs = config.epochs.to_vec();
        epochs.sort_unstable();
        epochs.dedup();
        epochs
    };

    let candidates = epochs.len();
    let mut errors = 0usize;
    for target_epoch in epochs.drain(..) {
        match seed_epoch(
            config.archive_root,
            target_epoch,
            config.dry_run,
            config.receipt_dir,
        ) {
            Ok(receipt) => emit_json(output, &receipt)?,
            Err(error) if config.discover => {
                errors += 1;
                let mut record = Receipt::new();
                record.insert("action".to_owned(), json!("error"));
                record.insert("error".to_owned(), json!(format!("{error:#}")));
                record.insert("target_epoch".to_owned(), json!(target_epoch));
                emit_json(output, &record)?;
            }
            Err(error) => return Err(error),
        }
    }

    if config.discover {
        let mut summary = Receipt::new();
        summary.insert("action".to_owned(), json!("discovery_complete"));
        summary.insert("candidates".to_owned(), json!(candidates));
        summary.insert("end_epoch".to_owned(), json!(config.end_epoch));
        summary.insert("errors".to_owned(), json!(errors));
        summary.insert("start_epoch".to_owned(), json!(config.start_epoch));
        emit_json(output, &summary)?;
    }

    Ok(SeedRunOutcome { candidates, errors })
}

fn emit_json<W: Write>(output: &mut W, value: &Receipt) -> Result<()> {
    serde_json::to_writer(&mut *output, value).context("serialize predecessor-tail result")?;
    output
        .write_all(b"\n")
        .context("write predecessor-tail result")?;
    output.flush().context("flush predecessor-tail result")
}

fn seed_epoch(
    archive_root: &Path,
    target_epoch: u64,
    dry_run: bool,
    receipt_dir: Option<&Path>,
) -> Result<Receipt> {
    if target_epoch == 0 {
        bail!("target epoch must be positive: {target_epoch}");
    }
    let source_epoch = target_epoch - 1;
    let source_dir = archive_root.join(format!("epoch-{source_epoch}"));
    let target_dir = archive_root.join(format!("epoch-{target_epoch}"));
    let target = target_dir.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);

    ensure_target_safe(&target_dir, target_epoch)?;
    ensure_source_stable(&source_dir, source_epoch)?;
    let (payload, mut receipt) = load_best_tail(&source_dir, source_epoch)?;
    receipt.insert("schema_version".to_owned(), json!(RECEIPT_SCHEMA_VERSION));
    receipt.insert("target".to_owned(), json!(path_text(&target)?));
    receipt.insert("target_epoch".to_owned(), json!(target_epoch));
    receipt.insert("tail_bytes".to_owned(), json!(payload.len()));
    receipt.insert("tail_sha256".to_owned(), json!(sha256_hex(&payload)));

    if entry_exists(&target)? {
        let existing = read_regular_file(&target)?;
        if existing != payload {
            bail!(
                "existing tail differs from verified V3 source: {}",
                target.display()
            );
        }
        receipt.insert("action".to_owned(), json!("verified_existing"));
    } else if dry_run {
        receipt.insert("action".to_owned(), json!("would_write"));
    } else {
        // Recheck both sides immediately before the no-replace publication.
        ensure_target_safe(&target_dir, target_epoch)?;
        ensure_source_stable(&source_dir, source_epoch)?;
        if entry_exists(&target)? {
            bail!(
                "target appeared during verification; refusing race: {}",
                target.display()
            );
        }
        atomic_publish_no_replace(&target, &payload, &archive_root.join(TAIL_STAGING_DIR))?;
        let published = read_regular_file(&target)?;
        if published != payload {
            bail!(
                "published tail failed exact read-back verification: {}",
                target.display()
            );
        }
        receipt.insert("action".to_owned(), json!("written"));
    }

    if let Some(receipt_dir) = receipt_dir.filter(|_| !dry_run) {
        write_receipt(receipt_dir, target_epoch, &receipt)?;
    }
    Ok(receipt)
}

fn discover_epochs(
    archive_root: &Path,
    start_epoch: u64,
    end_epoch: u64,
    receipt_dir: Option<&Path>,
) -> Result<Vec<u64>> {
    if start_epoch == 0 {
        bail!("--start-epoch must be positive in discovery mode");
    }
    if end_epoch < start_epoch {
        bail!("--end-epoch must not precede --start-epoch");
    }

    let mut epochs = Vec::new();
    for target_epoch in start_epoch..=end_epoch {
        let target_dir = archive_root.join(format!("epoch-{target_epoch}"));
        let source_dir = archive_root.join(format!("epoch-{}", target_epoch - 1));
        if entry_exists(&target_dir.join(OWNERSHIP_FILE))? {
            continue;
        }

        if receipt_proves_current_tail(receipt_dir, archive_root, target_epoch)? {
            continue;
        }

        // Active, incomplete, or unstable predecessors are expected during
        // discovery and are retried by the next timer invocation.
        if ensure_source_stable(&source_dir, target_epoch - 1).is_err() {
            continue;
        }
        if (is_regular_file(&source_dir.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE))?
            || is_regular_file(&source_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?)
            && is_regular_file(&source_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?
        {
            epochs.push(target_epoch);
        }
    }
    Ok(epochs)
}

/// Return true only when a schema-current receipt binds this exact tail to the
/// current predecessor V3 and blockhash-registry files.
///
/// This deliberately reads only the receipt, the V3 header, file metadata, and
/// the at-most-12-KiB tail. Older or malformed receipts return false so the
/// normal seeding path performs one full source validation and upgrades them.
fn receipt_proves_current_tail(
    receipt_dir: Option<&Path>,
    archive_root: &Path,
    target_epoch: u64,
) -> Result<bool> {
    let Some(receipt_dir) = receipt_dir else {
        return Ok(false);
    };
    let receipt_path = receipt_dir.join(format!("epoch-{target_epoch}.json"));
    if !is_regular_file(&receipt_path)? {
        return Ok(false);
    }

    let mut receipt_file = open_regular_read(&receipt_path)?;
    let receipt_before = file_identity(&receipt_file.metadata()?);
    if receipt_before.len == 0 || receipt_before.len > MAX_RECEIPT_BYTES {
        return Ok(false);
    }
    let mut receipt_bytes = Vec::with_capacity(usize::try_from(receipt_before.len)?);
    (&mut receipt_file)
        .take(MAX_RECEIPT_BYTES + 1)
        .read_to_end(&mut receipt_bytes)
        .with_context(|| format!("read tail receipt {}", receipt_path.display()))?;
    if receipt_bytes.len() as u64 != receipt_before.len
        || file_identity(&receipt_file.metadata()?) != receipt_before
    {
        return Ok(false);
    }
    let Ok(receipt) = serde_json::from_slice::<Value>(&receipt_bytes) else {
        return Ok(false);
    };
    let Some(receipt) = receipt.as_object() else {
        return Ok(false);
    };
    if receipt_u64(receipt, "schema_version") != Some(RECEIPT_SCHEMA_VERSION)
        || receipt_u64(receipt, "target_epoch") != Some(target_epoch)
    {
        return Ok(false);
    }

    let Some(source_epoch) = target_epoch.checked_sub(1) else {
        return Ok(false);
    };
    if receipt_u64(receipt, "source_epoch") != Some(source_epoch) {
        return Ok(false);
    }

    let source_dir = archive_root.join(format!("epoch-{source_epoch}"));
    let source_format = receipt_str(receipt, "source_format");
    let source_name = match source_format {
        Some(V3_SOURCE_FORMAT) => ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
        Some(HOT_SOURCE_FORMAT) => ARCHIVE_V2_BLOCK_INDEX_FILE,
        _ => return Ok(false),
    };
    let source = source_dir.join(source_name);
    let blockhash_registry = source.with_file_name(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    let target = archive_root
        .join(format!("epoch-{target_epoch}"))
        .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    if receipt_str(receipt, "source") != Some(path_text(&source)?)
        || receipt_str(receipt, "blockhash_registry") != Some(path_text(&blockhash_registry)?)
        || receipt_str(receipt, "target") != Some(path_text(&target)?)
        || !receipt_sha256_is_well_formed(receipt, "source_sha256")
        || !receipt_sha256_is_well_formed(receipt, "blockhash_registry_sha256")
        || !receipt_sha256_is_well_formed(receipt, "tail_sha256")
    {
        return Ok(false);
    }

    if !is_regular_file(&source)?
        || !is_regular_file(&blockhash_registry)?
        || !is_regular_file(&target)?
    {
        return Ok(false);
    }
    let mut source_file = open_regular_read(&source)?;
    let registry_file = open_regular_read(&blockhash_registry)?;
    let mut tail_file = open_regular_read(&target)?;
    let source_before = file_identity(&source_file.metadata()?);
    let registry_before = file_identity(&registry_file.metadata()?);
    let tail_before = file_identity(&tail_file.metadata()?);
    if receipt.get("source_identity") != Some(&file_identity_value(source_before))
        || receipt.get("blockhash_registry_identity") != Some(&file_identity_value(registry_before))
        || receipt_u64(receipt, "source_bytes") != Some(source_before.len)
        || receipt_u64(receipt, "blockhash_registry_bytes") != Some(registry_before.len)
        || receipt_u64(receipt, "tail_bytes") != Some(tail_before.len)
        || !valid_tail_len(tail_before.len)
    {
        return Ok(false);
    }

    let Some((rows, expected_source_bytes, supported_registry_bytes, hot_blob_bytes)) =
        receipt_source_shape(&mut source_file, source_format.expect("matched above"))?
    else {
        return Ok(false);
    };
    if rows == 0
        || source_before.len != expected_source_bytes
        || !supported_registry_bytes.contains(&registry_before.len)
        || receipt_u64(receipt, "source_rows") != Some(rows)
        || receipt_u64(receipt, "blockhash_registry_offset")
            != Some((registry_before.len / REGISTRY_KEY_BYTES).saturating_sub(rows))
    {
        return Ok(false);
    }
    if let Some(hot_blob_bytes) = hot_blob_bytes {
        let block_blob = source_dir.join(ARCHIVE_V2_BLOCKS_FILE);
        if receipt_str(receipt, "block_blob") != Some(path_text(&block_blob)?)
            || !is_regular_file(&block_blob)?
        {
            return Ok(false);
        }
        let blob_file = open_regular_read(&block_blob)?;
        let blob_identity = file_identity(&blob_file.metadata()?);
        if blob_identity.len != hot_blob_bytes
            || receipt_u64(receipt, "block_blob_bytes") != Some(blob_identity.len)
            || receipt.get("block_blob_identity") != Some(&file_identity_value(blob_identity))
        {
            return Ok(false);
        }
    }

    let mut tail = Vec::with_capacity(usize::try_from(tail_before.len)?);
    (&mut tail_file)
        .take((TAIL_CAPACITY * TAIL_ROW_LEN + 1) as u64)
        .read_to_end(&mut tail)
        .with_context(|| format!("read predecessor tail {}", target.display()))?;
    let tail_sha256 = sha256_hex(&tail);
    if file_identity(&source_file.metadata()?) != source_before
        || file_identity(&registry_file.metadata()?) != registry_before
        || file_identity(&tail_file.metadata()?) != tail_before
        || tail.len() as u64 != tail_before.len
        || receipt_str(receipt, "tail_sha256") != Some(tail_sha256.as_str())
    {
        return Ok(false);
    }

    let tail_rows = tail.len() / TAIL_ROW_LEN;
    let first_slot = u64::from_le_bytes(tail[32..TAIL_ROW_LEN].try_into().expect("valid tail row"));
    let last_offset = (tail_rows - 1) * TAIL_ROW_LEN + 32;
    let last_slot = u64::from_le_bytes(
        tail[last_offset..last_offset + 8]
            .try_into()
            .expect("valid tail row"),
    );
    Ok(receipt_u64(receipt, "tail_rows") == Some(tail_rows as u64)
        && receipt_u64(receipt, "tail_first_slot") == Some(first_slot)
        && receipt_u64(receipt, "tail_last_slot") == Some(last_slot))
}

fn receipt_u64(receipt: &serde_json::Map<String, Value>, key: &str) -> Option<u64> {
    receipt.get(key)?.as_u64()
}

fn receipt_str<'a>(receipt: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    receipt.get(key)?.as_str()
}

fn receipt_sha256_is_well_formed(receipt: &serde_json::Map<String, Value>, key: &str) -> bool {
    receipt_str(receipt, key).is_some_and(|digest| {
        digest.len() == 64
            && digest
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_hexdigit())
    })
}

type ReceiptSourceShape = (u64, u64, Vec<u64>, Option<u64>);

fn receipt_source_shape(
    source: &mut File,
    source_format: &str,
) -> Result<Option<ReceiptSourceShape>> {
    match source_format {
        V3_SOURCE_FORMAT => {
            let mut header = [0u8; ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN];
            if source.read_exact(&mut header).is_err() {
                return Ok(None);
            }
            let rows = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
            let expected_bytes = rows
                .checked_mul(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN as u64)
                .and_then(|bytes| {
                    bytes.checked_add(ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN as u64)
                });
            let registry_bytes = rows.checked_mul(REGISTRY_KEY_BYTES);
            let valid = &header[..8] == ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC
                && u16::from_le_bytes(header[8..10].try_into().expect("fixed header"))
                    == ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION
                && usize::from(u16::from_le_bytes(
                    header[10..12].try_into().expect("fixed header"),
                )) == ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN
                && rows > 0;
            Ok(match (valid, expected_bytes, registry_bytes) {
                (true, Some(expected), Some(registry)) => {
                    Some((rows, expected, vec![registry], None))
                }
                _ => None,
            })
        }
        HOT_SOURCE_FORMAT => {
            let mut header = [0u8; ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
            if source.read_exact(&mut header).is_err() {
                return Ok(None);
            }
            let rows = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
            let blob_bytes = u64::from_le_bytes(header[20..28].try_into().expect("fixed header"));
            let flags = u32::from_le_bytes(header[32..36].try_into().expect("fixed header"));
            let expected_bytes = rows
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .and_then(|bytes| bytes.checked_add(ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64));
            let registry = rows.checked_mul(REGISTRY_KEY_BYTES);
            let registry_with_genesis = rows
                .checked_add(1)
                .and_then(|rows| rows.checked_mul(REGISTRY_KEY_BYTES));
            let valid = &header[..8] == ARCHIVE_V2_HOT_INDEX_MAGIC
                && u16::from_le_bytes(header[8..10].try_into().expect("fixed header"))
                    == ARCHIVE_V2_HOT_INDEX_VERSION
                && header[10..12] == [0, 0]
                && rows > 0
                && blob_bytes > 0
                && flags == 0;
            Ok(valid.then_some((
                rows,
                match expected_bytes {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                },
                match (registry, registry_with_genesis) {
                    (Some(registry), Some(with_genesis)) => vec![registry, with_genesis],
                    _ => return Ok(None),
                },
                Some(blob_bytes),
            )))
        }
        _ => Ok(None),
    }
}

fn ensure_target_safe(target_dir: &Path, target_epoch: u64) -> Result<()> {
    let target_metadata = fs::symlink_metadata(target_dir)
        .with_context(|| format!("target directory is missing: {}", target_dir.display()))?;
    if !target_metadata.file_type().is_dir() {
        bail!("target directory is missing: {}", target_dir.display());
    }

    let ownership = read_optional_ownership(target_dir)?;

    let mut unknown = Vec::new();
    let mut non_regular = Vec::new();
    for entry in fs::read_dir(target_dir)
        .with_context(|| format!("read target directory {}", target_dir.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", target_dir.display()))?;
        let name = entry.file_name();
        let is_legacy_lock = name == LEGACY_BLOCKHASH_LOCK_DIR;
        let is_allowed = ALLOWED_TARGET_FILES.iter().any(|allowed| name == *allowed);
        if !is_allowed && !is_legacy_lock {
            unknown.push(name.to_string_lossy().into_owned());
            continue;
        }
        if !is_legacy_lock {
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.file_type().is_file() {
                non_regular.push(name.to_string_lossy().into_owned());
            }
        }
    }
    unknown.sort();
    non_regular.sort();
    if !unknown.is_empty() {
        bail!(
            "target {} has files outside the reusable scheduler shape: {}",
            target_dir.display(),
            unknown.join(", ")
        );
    }
    if !non_regular.is_empty() {
        bail!(
            "target {} has non-regular entries: {}",
            target_dir.display(),
            non_regular.join(", ")
        );
    }

    let mut has_complete_archive_files = false;
    for name in [
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
        ARCHIVE_V2_SHREDDING_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ] {
        has_complete_archive_files |= entry_exists(&target_dir.join(name))?;
    }
    if ownership.is_some() || has_complete_archive_files {
        validate_complete_target(target_dir, target_epoch, ownership.as_ref())?;
    }

    let mut missing = Vec::new();
    for name in REQUIRED_TARGET_FILES {
        let path = target_dir.join(name);
        match regular_file_len(&path)? {
            Some(len) if len > 0 => {}
            _ => missing.push(*name),
        }
    }
    if !missing.is_empty() {
        bail!(
            "target {} lacks reusable registry sidecars: {}",
            target_dir.display(),
            missing.join(", ")
        );
    }

    let legacy_lock = target_dir.join(LEGACY_BLOCKHASH_LOCK_DIR);
    if entry_exists(&legacy_lock)? {
        let metadata = fs::symlink_metadata(&legacy_lock)?;
        if !metadata.file_type().is_dir()
            || fs::read_dir(&legacy_lock)
                .with_context(|| format!("read legacy lock {}", legacy_lock.display()))?
                .next()
                .transpose()?
                .is_some()
        {
            bail!(
                "legacy blockhash lock is not an empty real directory: {}",
                legacy_lock.display()
            );
        }
    }

    validate_target_registry_sidecars(target_dir)
}

fn validate_complete_target(
    target_dir: &Path,
    target_epoch: u64,
    ownership: Option<&OwnershipMarker>,
) -> Result<()> {
    if let Some(owner) = ownership {
        validate_complete_owner(owner, target_epoch, &target_dir.join(OWNERSHIP_FILE))?;
    }
    for name in COMPLETE_TARGET_NONEMPTY_FILES {
        let path = target_dir.join(name);
        if !regular_file_len(&path)?.is_some_and(|bytes| bytes > 0) {
            bail!(
                "complete target lacks regular nonempty archive file: {}",
                path.display()
            );
        }
    }
    for name in [
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ] {
        let path = target_dir.join(name);
        if !is_regular_file(&path)? {
            bail!(
                "complete target lacks regular archive file: {}",
                path.display()
            );
        }
    }
    let access = is_regular_file(&target_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;
    let access_index = is_regular_file(&target_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE))?;
    if access != access_index {
        bail!(
            "complete target has only one block-access sidecar: {}",
            target_dir.display()
        );
    }
    if access {
        for name in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ] {
            let path = target_dir.join(name);
            if regular_file_len(&path)? == Some(0) {
                bail!(
                    "complete target has an empty block-access file: {}",
                    path.display()
                );
            }
        }
    }
    validate_first_seen_manifest(&target_dir.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE))?;
    load_hot_tail(target_dir, target_epoch)
        .with_context(|| format!("validate complete target {}", target_dir.display()))?;
    Ok(())
}

fn validate_first_seen_manifest(path: &Path) -> Result<()> {
    let bytes = read_regular_file(path)?;
    let text = std::str::from_utf8(&bytes)
        .with_context(|| format!("first-seen manifest is not UTF-8: {}", path.display()))?;
    let mut values = text
        .lines()
        .filter(|line| line.starts_with("registry_order="));
    if values.next() != Some(FIRST_SEEN_REGISTRY_ORDER) || values.next().is_some() {
        bail!(
            "first-seen manifest has no unique {FIRST_SEEN_REGISTRY_ORDER}: {}",
            path.display()
        );
    }
    Ok(())
}

fn read_optional_ownership(target_dir: &Path) -> Result<Option<OwnershipMarker>> {
    let path = target_dir.join(OWNERSHIP_FILE);
    if !entry_exists(&path)? {
        return Ok(None);
    }
    let mut file = open_regular_read(&path)?;
    let before = file_identity(&file.metadata()?);
    let bytes = before.len;
    if bytes == 0 || bytes > MAX_OWNERSHIP_BYTES {
        bail!(
            "ownership marker has invalid byte length {bytes}: {}",
            path.display()
        );
    }
    let mut payload = Vec::with_capacity(usize::try_from(bytes)?);
    (&mut file)
        .take(MAX_OWNERSHIP_BYTES + 1)
        .read_to_end(&mut payload)
        .with_context(|| format!("read ownership marker {}", path.display()))?;
    if payload.len() as u64 != bytes || file_identity(&file.metadata()?) != before {
        bail!(
            "ownership marker changed during verification: {}",
            path.display()
        );
    }
    let owner = serde_json::from_slice(&payload)
        .with_context(|| format!("decode ownership marker {}", path.display()))?;
    Ok(Some(owner))
}

fn validate_complete_owner(owner: &OwnershipMarker, epoch: u64, path: &Path) -> Result<()> {
    if owner.schema_version != OWNERSHIP_SCHEMA_VERSION
        || !matches!(
            owner.kind.as_str(),
            "historical_scan" | "historical_finalizer"
        )
        || owner.id != epoch.to_string()
        || owner.state != "complete"
        || owner.pid.is_some()
    {
        bail!(
            "ownership marker is not a completed historical archive for epoch {epoch}: {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_target_registry_sidecars(target_dir: &Path) -> Result<()> {
    let registry = target_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let registry_bytes = open_regular_read(&registry)?.metadata()?.len();
    if registry_bytes == 0 || registry_bytes % REGISTRY_KEY_BYTES != 0 {
        bail!(
            "target registry has invalid byte length {registry_bytes}: {}",
            registry.display()
        );
    }

    let index = target_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let (header, index_bytes) = read_regular_file_prefix::<REGISTRY_INDEX_HEADER_LEN>(&index)?;
    let magic = &header[..8];
    let version = u16::from_le_bytes(header[8..10].try_into().expect("fixed header"));
    let header_len = u16::from_le_bytes(header[10..12].try_into().expect("fixed header"));
    let keys = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
    let expected_keys = registry_bytes / REGISTRY_KEY_BYTES;
    let minimum_index_bytes = u64::try_from(REGISTRY_INDEX_HEADER_LEN)?
        .checked_add(
            keys.checked_mul(12)
                .ok_or_else(|| anyhow!("target registry index length overflow"))?,
        )
        .ok_or_else(|| anyhow!("target registry index length overflow"))?;
    if magic != REGISTRY_INDEX_MAGIC
        || version != REGISTRY_INDEX_VERSION
        || usize::from(header_len) != REGISTRY_INDEX_HEADER_LEN
        || keys != expected_keys
        || index_bytes <= minimum_index_bytes
    {
        bail!(
            "target registry index does not match registry.bin: {}",
            index.display()
        );
    }

    let blockhash_registry = target_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    let blockhash_bytes = open_regular_read(&blockhash_registry)?.metadata()?.len();
    if blockhash_bytes == 0 || blockhash_bytes % REGISTRY_KEY_BYTES != 0 {
        bail!(
            "target blockhash registry has invalid byte length {blockhash_bytes}: {}",
            blockhash_registry.display()
        );
    }

    let optional_v3 = target_dir.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
    if is_regular_file(&optional_v3)? {
        let (header, v3_bytes) =
            read_regular_file_prefix::<ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN>(&optional_v3)?;
        let magic = &header[..8];
        let version = u16::from_le_bytes(header[8..10].try_into().expect("fixed header"));
        let row_len = u16::from_le_bytes(header[10..12].try_into().expect("fixed header"));
        let rows = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
        let expected_bytes = u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN)?
            .checked_add(
                rows.checked_mul(u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN)?)
                    .ok_or_else(|| anyhow!("target V3 length overflow"))?,
            )
            .ok_or_else(|| anyhow!("target V3 length overflow"))?;
        if magic != ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC
            || version != ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION
            || usize::from(row_len) != ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN
            || rows != blockhash_bytes / REGISTRY_KEY_BYTES
            || v3_bytes != expected_bytes
        {
            bail!(
                "target V3 index is structurally invalid: {}",
                optional_v3.display()
            );
        }
    }

    let tail = target_dir.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    if is_regular_file(&tail)? {
        let tail_bytes = open_regular_read(&tail)?.metadata()?.len();
        if !valid_tail_len(tail_bytes) {
            bail!(
                "target previous-blockhash tail is malformed: {}",
                tail.display()
            );
        }
    }
    Ok(())
}

fn ensure_source_stable(source_dir: &Path, source_epoch: u64) -> Result<()> {
    let metadata = fs::symlink_metadata(source_dir)
        .with_context(|| format!("predecessor directory is missing: {}", source_dir.display()))?;
    if !metadata.file_type().is_dir() {
        bail!(
            "predecessor directory is not a real directory: {}",
            source_dir.display()
        );
    }
    if let Some(owner) = read_optional_ownership(source_dir)? {
        validate_complete_owner(&owner, source_epoch, &source_dir.join(OWNERSHIP_FILE))?;
    }
    Ok(())
}

fn load_best_tail(source_dir: &Path, source_epoch: u64) -> Result<(Vec<u8>, Receipt)> {
    let v3 = source_dir.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
    if is_regular_file(&v3)? {
        return load_v3_tail(&v3, source_epoch);
    }
    load_hot_tail(source_dir, source_epoch)
}

fn load_hot_tail(source_dir: &Path, source_epoch: u64) -> Result<(Vec<u8>, Receipt)> {
    let source = source_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let block_blob = source_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let blockhash_registry = source_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    for path in [&source, &block_blob, &blockhash_registry] {
        if !is_regular_file(path)? {
            bail!(
                "required predecessor hot archive file is missing: {}",
                path.display()
            );
        }
    }

    let mut source_file = open_regular_read(&source)?;
    let blob_file = open_regular_read(&block_blob)?;
    let mut registry_file = open_regular_read(&blockhash_registry)?;
    let source_before = file_identity(&source_file.metadata()?);
    let blob_before = file_identity(&blob_file.metadata()?);
    let registry_before = file_identity(&registry_file.metadata()?);

    let mut header = [0u8; ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
    read_exact_described(&mut source_file, &mut header, "hot-index header", &source)?;
    let mut source_digest = Sha256::new();
    source_digest.update(header);
    let rows = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
    let blob_bytes = u64::from_le_bytes(header[20..28].try_into().expect("fixed header"));
    let flags = u32::from_le_bytes(header[32..36].try_into().expect("fixed header"));
    if &header[..8] != ARCHIVE_V2_HOT_INDEX_MAGIC {
        bail!("invalid hot-index magic in {}", source.display());
    }
    if u16::from_le_bytes(header[8..10].try_into().expect("fixed header"))
        != ARCHIVE_V2_HOT_INDEX_VERSION
        || header[10..12] != [0, 0]
    {
        bail!(
            "unsupported hot-index version or reserved bytes in {}",
            source.display()
        );
    }
    if flags != 0 {
        bail!(
            "hot-index flags must be zero for canonical compact blocks in {}: {flags:#x}",
            source.display()
        );
    }
    if rows == 0 || blob_bytes == 0 {
        bail!(
            "hot index or block blob is empty in {}",
            source_dir.display()
        );
    }
    let expected_source_bytes = rows
        .checked_mul(u64::try_from(ARCHIVE_V2_HOT_INDEX_ROW_LEN)?)
        .and_then(|bytes| bytes.checked_add(ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64))
        .ok_or_else(|| anyhow!("hot-index length overflow"))?;
    if source_before.len != expected_source_bytes {
        bail!(
            "hot-index size mismatch in {}: expected {expected_source_bytes}, got {}",
            source.display(),
            source_before.len
        );
    }
    if blob_before.len != blob_bytes {
        bail!(
            "hot-index blob size differs from {}: expected {blob_bytes}, got {}",
            block_blob.display(),
            blob_before.len
        );
    }
    let registry_rows = registry_before.len / REGISTRY_KEY_BYTES;
    if registry_before.len % REGISTRY_KEY_BYTES != 0
        || !(registry_rows == rows || registry_rows == rows + 1)
    {
        bail!(
            "blockhash registry rows {} do not match hot-index rows {rows} in {}",
            registry_rows,
            blockhash_registry.display()
        );
    }
    let registry_offset = registry_rows - rows;

    let epoch_first_slot = source_epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("source epoch slot range overflow: {source_epoch}"))?;
    let epoch_last_slot_exclusive = source_epoch
        .checked_add(1)
        .and_then(|epoch| epoch.checked_mul(crate::SLOTS_PER_EPOCH))
        .ok_or_else(|| anyhow!("source epoch slot range overflow: {source_epoch}"))?;
    let mut registry_digest = Sha256::new();
    if registry_offset == 1 {
        let mut genesis = [0u8; REGISTRY_KEY_BYTES as usize];
        read_exact_described(
            &mut registry_file,
            &mut genesis,
            "blockhash registry genesis row",
            &blockhash_registry,
        )?;
        registry_digest.update(genesis);
    }

    let mut tail = VecDeque::<([u8; 32], u64)>::with_capacity(TAIL_CAPACITY);
    let mut source_rows = vec![0u8; VERIFY_CHUNK_ROWS * ARCHIVE_V2_HOT_INDEX_ROW_LEN];
    let mut registry_rows_buf = vec![0u8; VERIFY_CHUNK_ROWS * REGISTRY_KEY_BYTES as usize];
    let mut rows_left = rows;
    let mut row_number = 0u64;
    let mut previous_slot = None;
    let mut next_blob_offset = 0u64;
    let mut next_tx_ordinal = 0u64;
    let mut next_signature_ordinal = 0u64;
    while rows_left > 0 {
        let chunk_rows = usize::try_from(rows_left.min(VERIFY_CHUNK_ROWS as u64))?;
        let source_bytes = chunk_rows * ARCHIVE_V2_HOT_INDEX_ROW_LEN;
        let registry_bytes = chunk_rows * REGISTRY_KEY_BYTES as usize;
        let source_chunk = &mut source_rows[..source_bytes];
        let registry_chunk = &mut registry_rows_buf[..registry_bytes];
        read_exact_described(&mut source_file, source_chunk, "hot-index rows", &source)?;
        read_exact_described(
            &mut registry_file,
            registry_chunk,
            "blockhash registry rows",
            &blockhash_registry,
        )?;
        source_digest.update(&*source_chunk);
        registry_digest.update(&*registry_chunk);

        for chunk_row in 0..chunk_rows {
            let row_id = row_number + u64::try_from(chunk_row)?;
            let source_offset = chunk_row * ARCHIVE_V2_HOT_INDEX_ROW_LEN;
            let row = &source_chunk[source_offset..source_offset + ARCHIVE_V2_HOT_INDEX_ROW_LEN];
            let block_id = u32::from_le_bytes(row[0..4].try_into().expect("fixed hot row"));
            let slot = u64::from_le_bytes(row[4..12].try_into().expect("fixed hot row"));
            let compressed_offset =
                u64::from_le_bytes(row[12..20].try_into().expect("fixed hot row"));
            let compressed_len = u32::from_le_bytes(row[20..24].try_into().expect("fixed hot row"));
            let uncompressed_len =
                u32::from_le_bytes(row[24..28].try_into().expect("fixed hot row"));
            let tx_count = u32::from_le_bytes(row[28..32].try_into().expect("fixed hot row"));
            let first_tx_ordinal =
                u64::from_le_bytes(row[32..40].try_into().expect("fixed hot row"));
            let first_signature_ordinal =
                u64::from_le_bytes(row[40..48].try_into().expect("fixed hot row"));
            let signature_count =
                u32::from_le_bytes(row[48..52].try_into().expect("fixed hot row"));
            if u64::from(block_id) != row_id {
                bail!("hot-index block id {block_id} is not contiguous at row {row_id}");
            }
            if !(epoch_first_slot..epoch_last_slot_exclusive).contains(&slot)
                || previous_slot.is_some_and(|previous| slot <= previous)
            {
                bail!(
                    "hot-index slot {slot} is outside epoch {source_epoch} or not strictly increasing at row {row_id}"
                );
            }
            if compressed_len == 0
                || uncompressed_len == 0
                || compressed_offset != next_blob_offset
                || first_tx_ordinal != next_tx_ordinal
                || first_signature_ordinal != next_signature_ordinal
            {
                bail!("hot-index offsets or ordinals are inconsistent at row {row_id}");
            }
            next_blob_offset = next_blob_offset
                .checked_add(u64::from(compressed_len))
                .ok_or_else(|| anyhow!("hot-index compressed byte count overflow"))?;
            next_tx_ordinal = next_tx_ordinal
                .checked_add(u64::from(tx_count))
                .ok_or_else(|| anyhow!("hot-index transaction count overflow"))?;
            next_signature_ordinal = next_signature_ordinal
                .checked_add(u64::from(signature_count))
                .ok_or_else(|| anyhow!("hot-index signature count overflow"))?;
            previous_slot = Some(slot);

            let registry_offset = chunk_row * REGISTRY_KEY_BYTES as usize;
            let blockhash: [u8; 32] = registry_chunk
                [registry_offset..registry_offset + REGISTRY_KEY_BYTES as usize]
                .try_into()
                .expect("fixed registry row");
            if tail.len() == TAIL_CAPACITY {
                tail.pop_front();
            }
            tail.push_back((blockhash, slot));
        }
        rows_left -= u64::try_from(chunk_rows)?;
        row_number += u64::try_from(chunk_rows)?;
    }
    if next_blob_offset != blob_bytes {
        bail!(
            "hot-index rows cover {next_blob_offset} blob bytes, expected {blob_bytes}: {}",
            source.display()
        );
    }

    let source_after = file_identity(&source_file.metadata()?);
    let blob_after = file_identity(&blob_file.metadata()?);
    let registry_after = file_identity(&registry_file.metadata()?);
    if source_before != source_after {
        bail!(
            "hot index changed during verification: {}",
            source.display()
        );
    }
    if blob_before != blob_after {
        bail!(
            "block blob changed during verification: {}",
            block_blob.display()
        );
    }
    if registry_before != registry_after {
        bail!(
            "blockhash registry changed during verification: {}",
            blockhash_registry.display()
        );
    }

    let mut payload = Vec::with_capacity(tail.len() * TAIL_ROW_LEN);
    for (blockhash, slot) in &tail {
        payload.extend_from_slice(blockhash);
        payload.extend_from_slice(&slot.to_le_bytes());
    }
    let mut receipt = Receipt::new();
    receipt.insert("source_format".to_owned(), json!(HOT_SOURCE_FORMAT));
    receipt.insert("source".to_owned(), json!(path_text(&source)?));
    receipt.insert("source_bytes".to_owned(), json!(source_after.len));
    receipt.insert(
        "source_identity".to_owned(),
        file_identity_value(source_after),
    );
    receipt.insert(
        "source_sha256".to_owned(),
        json!(digest_hex(source_digest.finalize())),
    );
    receipt.insert("block_blob".to_owned(), json!(path_text(&block_blob)?));
    receipt.insert("block_blob_bytes".to_owned(), json!(blob_after.len));
    receipt.insert(
        "block_blob_identity".to_owned(),
        file_identity_value(blob_after),
    );
    receipt.insert(
        "blockhash_registry".to_owned(),
        json!(path_text(&blockhash_registry)?),
    );
    receipt.insert(
        "blockhash_registry_bytes".to_owned(),
        json!(registry_after.len),
    );
    receipt.insert(
        "blockhash_registry_identity".to_owned(),
        file_identity_value(registry_after),
    );
    receipt.insert(
        "blockhash_registry_sha256".to_owned(),
        json!(digest_hex(registry_digest.finalize())),
    );
    receipt.insert(
        "blockhash_registry_offset".to_owned(),
        json!(registry_offset),
    );
    receipt.insert("source_epoch".to_owned(), json!(source_epoch));
    receipt.insert("source_rows".to_owned(), json!(rows));
    receipt.insert("tail_rows".to_owned(), json!(tail.len()));
    receipt.insert(
        "tail_first_slot".to_owned(),
        json!(tail.front().expect("non-empty hot tail").1),
    );
    receipt.insert(
        "tail_last_slot".to_owned(),
        json!(tail.back().expect("non-empty hot tail").1),
    );
    Ok((payload, receipt))
}

fn load_v3_tail(source: &Path, source_epoch: u64) -> Result<(Vec<u8>, Receipt)> {
    if !is_regular_file(source)? {
        bail!(
            "finalized predecessor V3 index is not a regular file: {}",
            source.display()
        );
    }
    let blockhash_registry = source.with_file_name(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    if !is_regular_file(&blockhash_registry)? {
        bail!(
            "predecessor blockhash registry is missing: {}",
            blockhash_registry.display()
        );
    }

    let mut source_file = open_regular_read(source)?;
    let mut registry_file = open_regular_read(&blockhash_registry)?;
    let source_before = file_identity(&source_file.metadata()?);
    let registry_before = file_identity(&registry_file.metadata()?);
    let source_size = source_before.len;

    let mut header = [0u8; ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN];
    read_exact_described(&mut source_file, &mut header, "V3 header", source)?;
    let mut source_digest = Sha256::new();
    source_digest.update(header);
    let magic = &header[..8];
    let version = u16::from_le_bytes(header[8..10].try_into().expect("fixed header"));
    let row_len = u16::from_le_bytes(header[10..12].try_into().expect("fixed header"));
    let rows = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
    if magic != ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC {
        bail!("invalid V3 magic in {}", source.display());
    }
    if version != ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION {
        bail!("unsupported V3 version in {}: {version}", source.display());
    }
    if usize::from(row_len) != ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN {
        bail!("invalid V3 row length in {}: {row_len}", source.display());
    }
    if rows == 0 {
        bail!("V3 index has no rows: {}", source.display());
    }

    let expected_size = u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN)?
        .checked_add(
            rows.checked_mul(u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN)?)
                .ok_or_else(|| anyhow!("V3 source length overflow"))?,
        )
        .ok_or_else(|| anyhow!("V3 source length overflow"))?;
    if source_size != expected_size {
        bail!(
            "V3 size mismatch in {}: expected {expected_size}, got {source_size}",
            source.display()
        );
    }
    let expected_registry_size = rows
        .checked_mul(REGISTRY_KEY_BYTES)
        .ok_or_else(|| anyhow!("blockhash registry length overflow"))?;
    if registry_before.len != expected_registry_size {
        bail!(
            "blockhash registry size mismatch in {}: expected {expected_registry_size}, got {}",
            blockhash_registry.display(),
            registry_before.len
        );
    }

    let epoch_first_slot = source_epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("source epoch slot range overflow: {source_epoch}"))?;
    let epoch_last_slot_exclusive = source_epoch
        .checked_add(1)
        .and_then(|epoch| epoch.checked_mul(crate::SLOTS_PER_EPOCH))
        .ok_or_else(|| anyhow!("source epoch slot range overflow: {source_epoch}"))?;
    let mut tail = VecDeque::<([u8; 32], u64)>::with_capacity(TAIL_CAPACITY);
    let mut previous_slot = None;
    let mut source_rows = vec![0u8; VERIFY_CHUNK_ROWS * ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN];
    let mut registry_rows = vec![0u8; VERIFY_CHUNK_ROWS * REGISTRY_KEY_BYTES as usize];
    let mut registry_digest = Sha256::new();
    let mut rows_left = rows;
    let mut row_number = 0u64;
    while rows_left > 0 {
        let chunk_rows = usize::try_from(rows_left.min(VERIFY_CHUNK_ROWS as u64))?;
        let source_bytes = chunk_rows * ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN;
        let registry_bytes = chunk_rows * REGISTRY_KEY_BYTES as usize;
        let source_chunk = &mut source_rows[..source_bytes];
        let registry_chunk = &mut registry_rows[..registry_bytes];
        read_exact_described(&mut source_file, source_chunk, "V3 rows", source)?;
        read_exact_described(
            &mut registry_file,
            registry_chunk,
            "blockhash registry rows",
            &blockhash_registry,
        )?;
        source_digest.update(&*source_chunk);
        registry_digest.update(&*registry_chunk);

        for chunk_row in 0..chunk_rows {
            let source_offset = chunk_row * ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN;
            let row =
                &source_chunk[source_offset..source_offset + ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN];
            let slot = u64::from_le_bytes(row[..8].try_into().expect("fixed V3 row"));
            let blockhash: [u8; 32] = row[8..40].try_into().expect("fixed V3 row");
            let registry_offset = chunk_row * REGISTRY_KEY_BYTES as usize;
            if blockhash.as_slice()
                != &registry_chunk[registry_offset..registry_offset + REGISTRY_KEY_BYTES as usize]
            {
                bail!(
                    "V3 blockhash differs from blockhash registry at row {} in {}",
                    row_number + u64::try_from(chunk_row)?,
                    source.display()
                );
            }
            if !(epoch_first_slot..epoch_last_slot_exclusive).contains(&slot) {
                bail!(
                    "slot {slot} in {} is outside epoch {source_epoch} range [{epoch_first_slot}, {epoch_last_slot_exclusive})",
                    source.display()
                );
            }
            if previous_slot.is_some_and(|previous| slot <= previous) {
                bail!(
                    "V3 slots are not strictly increasing in {}: {} then {slot}",
                    source.display(),
                    previous_slot.expect("checked as some")
                );
            }
            previous_slot = Some(slot);
            if tail.len() == TAIL_CAPACITY {
                tail.pop_front();
            }
            tail.push_back((blockhash, slot));
        }
        rows_left -= u64::try_from(chunk_rows)?;
        row_number += u64::try_from(chunk_rows)?;
    }

    let source_after = file_identity(&source_file.metadata()?);
    let registry_after = file_identity(&registry_file.metadata()?);
    if source_before != source_after {
        bail!(
            "V3 source changed during verification: {}",
            source.display()
        );
    }
    if registry_before != registry_after {
        bail!(
            "blockhash registry changed during verification: {}",
            blockhash_registry.display()
        );
    }

    let mut payload = Vec::with_capacity(tail.len() * TAIL_ROW_LEN);
    for (blockhash, slot) in &tail {
        payload.extend_from_slice(blockhash);
        payload.extend_from_slice(&slot.to_le_bytes());
    }
    if payload.len() != tail.len() * TAIL_ROW_LEN {
        bail!("internal tail length mismatch for {}", source.display());
    }

    let mut receipt = Receipt::new();
    receipt.insert("source_format".to_owned(), json!(V3_SOURCE_FORMAT));
    receipt.insert("source".to_owned(), json!(path_text(source)?));
    receipt.insert("source_bytes".to_owned(), json!(source_size));
    receipt.insert(
        "source_identity".to_owned(),
        file_identity_value(source_after),
    );
    receipt.insert(
        "source_sha256".to_owned(),
        json!(digest_hex(source_digest.finalize())),
    );
    receipt.insert(
        "blockhash_registry".to_owned(),
        json!(path_text(&blockhash_registry)?),
    );
    receipt.insert(
        "blockhash_registry_bytes".to_owned(),
        json!(registry_after.len),
    );
    receipt.insert(
        "blockhash_registry_identity".to_owned(),
        file_identity_value(registry_after),
    );
    receipt.insert(
        "blockhash_registry_sha256".to_owned(),
        json!(digest_hex(registry_digest.finalize())),
    );
    receipt.insert("blockhash_registry_offset".to_owned(), json!(0));
    receipt.insert("source_epoch".to_owned(), json!(source_epoch));
    receipt.insert("source_rows".to_owned(), json!(rows));
    receipt.insert("tail_rows".to_owned(), json!(tail.len()));
    receipt.insert(
        "tail_first_slot".to_owned(),
        json!(tail.front().expect("non-empty V3 tail").1),
    );
    receipt.insert(
        "tail_last_slot".to_owned(),
        json!(tail.back().expect("non-empty V3 tail").1),
    );
    Ok((payload, receipt))
}

fn read_exact_described(
    reader: &mut File,
    bytes: &mut [u8],
    description: &str,
    path: &Path,
) -> Result<()> {
    reader.read_exact(bytes).with_context(|| {
        format!(
            "short {description}: expected {} bytes while reading {}",
            bytes.len(),
            path.display()
        )
    })
}

fn write_receipt(receipt_dir: &Path, target_epoch: u64, receipt: &Receipt) -> Result<()> {
    ensure_real_directory(receipt_dir, true)?;
    let mut payload = serde_json::to_vec_pretty(receipt).context("serialize tail receipt")?;
    payload.push(b'\n');
    atomic_write_replace(
        &receipt_dir.join(format!("epoch-{target_epoch}.json")),
        &payload,
    )
}

fn atomic_write_replace(path: &Path, payload: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("receipt has no parent: {}", path.display()))?;
    ensure_real_directory(parent, false)?;
    let (temporary, mut file) = create_temporary(parent, &format!(".{}.", file_name(path)?))?;
    let result = (|| -> Result<()> {
        file.write_all(payload)
            .with_context(|| format!("write temporary receipt {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temporary receipt {}", temporary.display()))?;
        set_mode_0644(&temporary)?;
        drop(file);
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "atomically publish receipt {} -> {}",
                temporary.display(),
                path.display()
            )
        })?;
        sync_directory(parent)
    })();
    cleanup_temporary(&temporary, result)
}

fn atomic_publish_no_replace(path: &Path, payload: &[u8], staging_dir: &Path) -> Result<()> {
    ensure_real_directory(staging_dir, true)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("tail target has no parent: {}", path.display()))?;
    ensure_real_directory(parent, false)?;
    ensure_same_filesystem(staging_dir, parent)?;
    let (temporary, mut file) = create_temporary(
        staging_dir,
        &format!(
            "epoch-{}-tail.",
            parent
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown")
        ),
    )?;
    let result = (|| -> Result<()> {
        file.write_all(payload)
            .with_context(|| format!("write temporary tail {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temporary tail {}", temporary.display()))?;
        set_mode_0644(&temporary)?;
        drop(file);
        match fs::hard_link(&temporary, path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => bail!(
                "target appeared during verification; refusing race: {}",
                path.display()
            ),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "atomically publish tail {} -> {}",
                        temporary.display(),
                        path.display()
                    )
                });
            }
        }
        sync_directory(parent)
    })();
    cleanup_temporary(&temporary, result)
}

fn cleanup_temporary(path: &Path, result: Result<()>) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => result,
        Err(error) if error.kind() == io::ErrorKind::NotFound => result,
        Err(error) => match result {
            Ok(()) => Err(error).with_context(|| format!("remove temporary {}", path.display())),
            Err(primary) => Err(primary.context(format!(
                "also failed to remove temporary {}: {error}",
                path.display()
            ))),
        },
    }
}

fn create_temporary(parent: &Path, prefix: &str) -> Result<(PathBuf, File)> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..128u16 {
        let path = parent.join(format!(
            "{prefix}{}-{nonce}-{attempt}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600).custom_flags(libc::O_CLOEXEC);
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create temporary file {}", path.display()));
            }
        }
    }
    bail!(
        "could not allocate a unique temporary file in {}",
        parent.display()
    )
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>> {
    let mut file = open_regular_read(path)?;
    let len = usize::try_from(file.metadata()?.len())
        .with_context(|| format!("file is too large to read: {}", path.display()))?;
    let mut bytes = Vec::with_capacity(len);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read regular file {}", path.display()))?;
    Ok(bytes)
}

fn read_regular_file_prefix<const N: usize>(path: &Path) -> Result<([u8; N], u64)> {
    let mut file = open_regular_read(path)?;
    let bytes = file.metadata()?.len();
    let mut prefix = [0u8; N];
    read_exact_described(&mut file, &mut prefix, "header", path)?;
    Ok((prefix, bytes))
}

fn open_regular_read(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path).with_context(|| {
        format!(
            "open regular file without following links: {}",
            path.display()
        )
    })?;
    if !file.metadata()?.file_type().is_file() {
        bail!("not a regular file: {}", path.display());
    }
    Ok(file)
}

fn regular_file_len(path: &Path) -> Result<Option<u64>> {
    if !is_regular_file(path)? {
        return Ok(None);
    }
    Ok(Some(open_regular_read(path)?.metadata()?.len()))
}

fn is_regular_file(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_file()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn ensure_real_directory(path: &Path, create: bool) -> Result<()> {
    if create {
        fs::create_dir_all(path).with_context(|| format!("create directory {}", path.display()))?;
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect directory {}", path.display()))?;
    if !metadata.file_type().is_dir() {
        bail!("directory is not a real directory: {}", path.display());
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_same_filesystem(left: &Path, right: &Path) -> Result<()> {
    if fs::metadata(left)?.dev() != fs::metadata(right)?.dev() {
        bail!(
            "staging and target are not on the same filesystem: {}",
            left.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_same_filesystem(_left: &Path, _right: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_mode_0644(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o644))
        .with_context(|| format!("set permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_mode_0644(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory for sync {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn valid_tail_len(bytes: u64) -> bool {
    bytes > 0
        && bytes.is_multiple_of(TAIL_ROW_LEN as u64)
        && bytes <= (TAIL_CAPACITY * TAIL_ROW_LEN) as u64
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", path.display()))
}

fn file_name(path: &Path) -> Result<&str> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("path has no UTF-8 file name: {}", path.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest_hex(Sha256::digest(bytes))
}

fn digest_hex(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    len: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(not(unix))]
    modified: Option<SystemTime>,
}

fn file_identity(metadata: &Metadata) -> FileIdentity {
    FileIdentity {
        len: metadata.len(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
        #[cfg(unix)]
        modified_seconds: metadata.mtime(),
        #[cfg(unix)]
        modified_nanoseconds: metadata.mtime_nsec(),
        #[cfg(unix)]
        changed_seconds: metadata.ctime(),
        #[cfg(unix)]
        changed_nanoseconds: metadata.ctime_nsec(),
        #[cfg(not(unix))]
        modified: metadata.modified().ok(),
    }
}

#[cfg(unix)]
fn file_identity_value(identity: FileIdentity) -> Value {
    json!({
        "bytes": identity.len,
        "device": identity.device,
        "inode": identity.inode,
        "modified_seconds": identity.modified_seconds,
        "modified_nanoseconds": identity.modified_nanoseconds,
        "changed_seconds": identity.changed_seconds,
        "changed_nanoseconds": identity.changed_nanoseconds,
    })
}

#[cfg(not(unix))]
fn file_identity_value(identity: FileIdentity) -> Value {
    let modified = identity.modified.map(|modified| {
        modified
            .duration_since(UNIX_EPOCH)
            .map(|duration| format!("{}.{:09}", duration.as_secs(), duration.subsec_nanos()))
            .unwrap_or_else(|duration| {
                format!(
                    "-{}.{:09}",
                    duration.duration().as_secs(),
                    duration.duration().subsec_nanos()
                )
            })
    });
    json!({
        "bytes": identity.len,
        "modified": modified,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "blockzilla-predecessor-tail-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_hash(epoch: u64, row: u64) -> [u8; 32] {
        Sha256::digest(format!("epoch={epoch};row={row}").as_bytes()).into()
    }

    fn write_source(root: &Path, epoch: u64, rows: u64) -> Vec<([u8; 32], u64)> {
        let directory = root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&directory).unwrap();
        let values = (0..rows)
            .map(|row| (test_hash(epoch, row), epoch * crate::SLOTS_PER_EPOCH + row))
            .collect::<Vec<_>>();
        let mut v3 = Vec::new();
        v3.extend_from_slice(ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC);
        v3.extend_from_slice(&ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION.to_le_bytes());
        v3.extend_from_slice(&(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN as u16).to_le_bytes());
        v3.extend_from_slice(&rows.to_le_bytes());
        let mut registry = Vec::new();
        for (blockhash, slot) in &values {
            v3.extend_from_slice(&slot.to_le_bytes());
            v3.extend_from_slice(blockhash);
            v3.extend_from_slice(&0i64.to_le_bytes());
            registry.extend_from_slice(blockhash);
        }
        fs::write(directory.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE), v3).unwrap();
        fs::write(directory.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), registry).unwrap();
        values
    }

    fn write_target(root: &Path, epoch: u64) -> PathBuf {
        let directory = root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&directory).unwrap();
        let keys = 2u64;
        fs::write(
            directory.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            vec![b'r'; keys as usize * 32],
        )
        .unwrap();
        fs::write(
            directory.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            b"counts",
        )
        .unwrap();
        let mut mphf = Vec::new();
        mphf.extend_from_slice(REGISTRY_INDEX_MAGIC);
        mphf.extend_from_slice(&REGISTRY_INDEX_VERSION.to_le_bytes());
        mphf.extend_from_slice(&(REGISTRY_INDEX_HEADER_LEN as u16).to_le_bytes());
        mphf.extend_from_slice(&keys.to_le_bytes());
        mphf.extend_from_slice(&vec![0; keys as usize * 12]);
        mphf.extend_from_slice(b"mphf");
        fs::write(directory.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE), mphf).unwrap();
        fs::write(
            directory.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            vec![b'b'; 3 * 32],
        )
        .unwrap();
        directory
    }

    fn write_hot_archive(
        root: &Path,
        epoch: u64,
        rows: u64,
        registry_offset: u64,
    ) -> Vec<([u8; 32], u64)> {
        let directory = root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&directory).unwrap();
        let values = (0..rows)
            .map(|row| (test_hash(epoch, row), epoch * crate::SLOTS_PER_EPOCH + row))
            .collect::<Vec<_>>();
        let mut index = Vec::new();
        index.extend_from_slice(ARCHIVE_V2_HOT_INDEX_MAGIC);
        index.extend_from_slice(&ARCHIVE_V2_HOT_INDEX_VERSION.to_le_bytes());
        index.extend_from_slice(&0u16.to_le_bytes());
        index.extend_from_slice(&rows.to_le_bytes());
        index.extend_from_slice(&rows.to_le_bytes());
        index.extend_from_slice(&1i32.to_le_bytes());
        index.extend_from_slice(&0u32.to_le_bytes());
        let mut registry = Vec::new();
        if registry_offset == 1 {
            registry.extend_from_slice(&[0xabu8; 32]);
        }
        for (row, (blockhash, slot)) in values.iter().enumerate() {
            let row = row as u64;
            index.extend_from_slice(&(row as u32).to_le_bytes());
            index.extend_from_slice(&slot.to_le_bytes());
            index.extend_from_slice(&row.to_le_bytes());
            index.extend_from_slice(&1u32.to_le_bytes());
            index.extend_from_slice(&1u32.to_le_bytes());
            index.extend_from_slice(&1u32.to_le_bytes());
            index.extend_from_slice(&row.to_le_bytes());
            index.extend_from_slice(&row.to_le_bytes());
            index.extend_from_slice(&1u32.to_le_bytes());
            registry.extend_from_slice(blockhash);
        }
        fs::write(directory.join(ARCHIVE_V2_BLOCK_INDEX_FILE), index).unwrap();
        fs::write(
            directory.join(ARCHIVE_V2_BLOCKS_FILE),
            vec![1u8; rows as usize],
        )
        .unwrap();
        fs::write(directory.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), registry).unwrap();
        values
    }

    fn make_complete_target(root: &Path, epoch: u64, owned: bool) -> PathBuf {
        let directory = write_target(root, epoch);
        write_hot_archive(root, epoch, 3, 0);
        for (name, bytes) in [
            (ARCHIVE_V2_META_FILE, b"meta".as_slice()),
            (ARCHIVE_V2_POH_FILE, b"poh".as_slice()),
            (ARCHIVE_V2_SHREDDING_FILE, b"shreds".as_slice()),
        ] {
            fs::write(directory.join(name), bytes).unwrap();
        }
        fs::write(
            directory.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE),
            b"schema_version=1\nregistry_order=first_seen_v1\n",
        )
        .unwrap();
        fs::write(directory.join(ARCHIVE_V2_SIGNATURES_FILE), []).unwrap();
        fs::write(directory.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE), []).unwrap();
        if owned {
            fs::write(
                directory.join(OWNERSHIP_FILE),
                format!(
                    "{{\"schema_version\":2,\"kind\":\"historical_scan\",\"id\":\"{epoch}\",\"state\":\"complete\",\"pid\":null}}"
                ),
            )
            .unwrap();
        }
        directory
    }

    fn config<'a>(
        root: &'a Path,
        epochs: &'a [u64],
        receipt_dir: Option<&'a Path>,
    ) -> SeedPreviousBlockhashTailsConfig<'a> {
        SeedPreviousBlockhashTailsConfig {
            archive_root: root,
            epochs,
            discover: false,
            start_epoch: None,
            end_epoch: None,
            receipt_dir,
            dry_run: false,
        }
    }

    fn discovery_config<'a>(
        root: &'a Path,
        epoch: u64,
        receipt_dir: &'a Path,
    ) -> SeedPreviousBlockhashTailsConfig<'a> {
        SeedPreviousBlockhashTailsConfig {
            archive_root: root,
            epochs: &[],
            discover: true,
            start_epoch: Some(epoch),
            end_epoch: Some(epoch),
            receipt_dir: Some(receipt_dir),
            dry_run: false,
        }
    }

    fn tail_payload(rows: &[([u8; 32], u64)]) -> Vec<u8> {
        let mut payload = Vec::new();
        for (blockhash, slot) in rows.iter().rev().take(TAIL_CAPACITY).rev() {
            payload.extend_from_slice(blockhash);
            payload.extend_from_slice(&slot.to_le_bytes());
        }
        payload
    }

    #[test]
    fn exact_tail_is_atomic_idempotent_and_receipted() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        let rows = write_source(&root, 7, 305);
        let target = write_target(&root, 8);
        fs::create_dir(target.join(LEGACY_BLOCKHASH_LOCK_DIR)).unwrap();
        let receipts = temp.0.join("receipts");
        let epochs = [8];

        let mut output = Vec::new();
        let first =
            seed_previous_blockhash_tails_to(config(&root, &epochs, Some(&receipts)), &mut output)
                .unwrap();
        assert_eq!(first.errors, 0);
        let first_record: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(first_record["action"], "written");
        assert_eq!(first_record["schema_version"], RECEIPT_SCHEMA_VERSION);
        assert!(first_record["source_identity"].is_object());
        assert!(first_record["blockhash_registry_identity"].is_object());

        let mut expected = Vec::new();
        for (blockhash, slot) in rows.iter().skip(5) {
            expected.extend_from_slice(blockhash);
            expected.extend_from_slice(&slot.to_le_bytes());
        }
        assert_eq!(
            fs::read(target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)).unwrap(),
            expected
        );
        let receipt: Value =
            serde_json::from_slice(&fs::read(receipts.join("epoch-8.json")).unwrap()).unwrap();
        assert_eq!(receipt["tail_sha256"], sha256_hex(&expected));

        output.clear();
        seed_previous_blockhash_tails_to(config(&root, &epochs, Some(&receipts)), &mut output)
            .unwrap();
        let second: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(second["action"], "verified_existing");
    }

    #[test]
    fn hot_index_fallback_seeds_complete_unowned_target() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        let rows = write_hot_archive(&root, 50, 305, 0);
        let target = make_complete_target(&root, 51, false);
        let receipts = temp.0.join("receipts");
        let epochs = [51];

        let mut output = Vec::new();
        let mut dry_run = config(&root, &epochs, Some(&receipts));
        dry_run.dry_run = true;
        seed_previous_blockhash_tails_to(dry_run, &mut output).unwrap();
        let dry_record: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(dry_record["action"], "would_write");
        assert!(!target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE).exists());
        assert!(!receipts.exists());

        output.clear();
        seed_previous_blockhash_tails_to(config(&root, &epochs, Some(&receipts)), &mut output)
            .unwrap();
        let record: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(record["action"], "written");
        assert_eq!(record["source_format"], HOT_SOURCE_FORMAT);
        assert_eq!(record["blockhash_registry_offset"], 0);
        assert_eq!(
            fs::read(target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)).unwrap(),
            tail_payload(&rows)
        );
        let skipped = seed_previous_blockhash_tails_to(
            discovery_config(&root, 51, &receipts),
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!(skipped.candidates, 0);
    }

    #[test]
    fn hot_index_fallback_accepts_complete_owned_target_and_genesis_offset() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        let rows = write_hot_archive(&root, 52, 305, 1);
        let target = make_complete_target(&root, 53, true);
        let epochs = [53];

        let mut output = Vec::new();
        seed_previous_blockhash_tails_to(config(&root, &epochs, None), &mut output).unwrap();
        let record: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(record["blockhash_registry_offset"], 1);
        assert_eq!(
            fs::read(target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)).unwrap(),
            tail_payload(&rows)
        );
    }

    #[test]
    fn active_owned_complete_shape_is_rejected_without_writing() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_hot_archive(&root, 54, 305, 0);
        let target = make_complete_target(&root, 55, true);
        fs::write(
            target.join(OWNERSHIP_FILE),
            b"{\"schema_version\":2,\"kind\":\"historical_scan\",\"id\":\"55\",\"state\":\"running\",\"pid\":123}",
        )
        .unwrap();
        let epochs = [55];

        let error = seed_previous_blockhash_tails_to(config(&root, &epochs, None), &mut Vec::new())
            .unwrap_err();
        assert!(format!("{error:#}").contains("not a completed historical archive"));
        assert!(!target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE).exists());
    }

    #[test]
    fn malformed_hot_index_fails_before_publication() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_hot_archive(&root, 56, 305, 0);
        let source_index = root.join("epoch-56").join(ARCHIVE_V2_BLOCK_INDEX_FILE);
        let mut index = fs::read(&source_index).unwrap();
        let last_row = ARCHIVE_V2_HOT_INDEX_HEADER_LEN + 304 * ARCHIVE_V2_HOT_INDEX_ROW_LEN;
        index[last_row..last_row + 4].copy_from_slice(&303u32.to_le_bytes());
        fs::write(source_index, index).unwrap();
        let target = make_complete_target(&root, 57, false);
        let epochs = [57];

        let error = seed_previous_blockhash_tails_to(config(&root, &epochs, None), &mut Vec::new())
            .unwrap_err();
        assert!(format!("{error:#}").contains("not contiguous"));
        assert!(!target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE).exists());
    }

    #[test]
    fn discovery_recovers_tail_published_before_receipt_then_skips_it() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        let rows = write_source(&root, 40, 305);
        let target = write_target(&root, 41);
        let receipts = temp.0.join("receipts");
        fs::write(
            target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
            tail_payload(&rows),
        )
        .unwrap();

        let mut output = Vec::new();
        let recovered =
            seed_previous_blockhash_tails_to(discovery_config(&root, 41, &receipts), &mut output)
                .unwrap();
        assert_eq!(recovered.candidates, 1);
        assert_eq!(recovered.errors, 0);
        let records = output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records[0]["action"], "verified_existing");
        assert_eq!(records[0]["schema_version"], RECEIPT_SCHEMA_VERSION);

        output.clear();
        let skipped =
            seed_previous_blockhash_tails_to(discovery_config(&root, 41, &receipts), &mut output)
                .unwrap();
        assert_eq!(skipped.candidates, 0);
        assert_eq!(skipped.errors, 0);
        let summary: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(summary["action"], "discovery_complete");
        assert_eq!(summary["candidates"], 0);
    }

    #[test]
    fn discovery_reports_same_length_wrong_existing_tail() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        let rows = write_source(&root, 42, 305);
        let target = write_target(&root, 43);
        let receipts = temp.0.join("receipts");
        let mut wrong_tail = tail_payload(&rows);
        wrong_tail[0] ^= 0xff;
        fs::write(target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), wrong_tail).unwrap();

        let mut output = Vec::new();
        let outcome =
            seed_previous_blockhash_tails_to(discovery_config(&root, 43, &receipts), &mut output)
                .unwrap();
        assert_eq!(outcome.candidates, 1);
        assert_eq!(outcome.errors, 1);
        let records = output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records[0]["action"], "error");
        assert!(
            records[0]["error"]
                .as_str()
                .unwrap()
                .contains("existing tail differs")
        );
        assert!(!receipts.join("epoch-43.json").exists());
    }

    #[test]
    fn current_receipt_detects_tail_tampering_in_discovery() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_source(&root, 44, 305);
        let target = write_target(&root, 45);
        let receipts = temp.0.join("receipts");
        let epochs = [45];
        seed_previous_blockhash_tails_to(config(&root, &epochs, Some(&receipts)), &mut Vec::new())
            .unwrap();
        let tail = target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
        let mut tampered = fs::read(&tail).unwrap();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        fs::write(&tail, tampered).unwrap();

        let mut output = Vec::new();
        let outcome =
            seed_previous_blockhash_tails_to(discovery_config(&root, 45, &receipts), &mut output)
                .unwrap();
        assert_eq!(outcome.candidates, 1);
        assert_eq!(outcome.errors, 1);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("existing tail differs")
        );
    }

    #[test]
    fn legacy_receipt_is_revalidated_and_upgraded_once() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_source(&root, 46, 305);
        write_target(&root, 47);
        let receipts = temp.0.join("receipts");
        let epochs = [47];
        seed_previous_blockhash_tails_to(config(&root, &epochs, Some(&receipts)), &mut Vec::new())
            .unwrap();
        let receipt_path = receipts.join("epoch-47.json");
        let mut legacy: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        legacy["schema_version"] = json!(2);
        fs::write(&receipt_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

        let mut output = Vec::new();
        let upgraded =
            seed_previous_blockhash_tails_to(discovery_config(&root, 47, &receipts), &mut output)
                .unwrap();
        assert_eq!(upgraded.candidates, 1);
        assert_eq!(upgraded.errors, 0);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("verified_existing")
        );
        let current: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(current["schema_version"], RECEIPT_SCHEMA_VERSION);

        let skipped = seed_previous_blockhash_tails_to(
            discovery_config(&root, 47, &receipts),
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!(skipped.candidates, 0);
    }

    #[test]
    fn current_receipt_revalidates_after_source_identities_change() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_source(&root, 48, 305);
        write_target(&root, 49);
        let receipts = temp.0.join("receipts");
        let epochs = [49];
        seed_previous_blockhash_tails_to(config(&root, &epochs, Some(&receipts)), &mut Vec::new())
            .unwrap();
        let receipt_path = receipts.join("epoch-49.json");
        let before: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();

        for name in [
            ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ] {
            let path = root.join("epoch-48").join(name);
            let replacement = root.join("epoch-48").join(format!(".{name}.replacement"));
            fs::write(&replacement, fs::read(&path).unwrap()).unwrap();
            fs::rename(replacement, path).unwrap();
        }

        let mut output = Vec::new();
        let revalidated =
            seed_previous_blockhash_tails_to(discovery_config(&root, 49, &receipts), &mut output)
                .unwrap();
        assert_eq!(revalidated.candidates, 1);
        assert_eq!(revalidated.errors, 0);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("verified_existing")
        );
        let after: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_ne!(before["source_identity"], after["source_identity"]);
        assert_ne!(
            before["blockhash_registry_identity"],
            after["blockhash_registry_identity"]
        );

        let skipped = seed_previous_blockhash_tails_to(
            discovery_config(&root, 49, &receipts),
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!(skipped.candidates, 0);
    }

    #[test]
    fn source_registry_mismatch_fails_closed() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_source(&root, 10, 305);
        let target = write_target(&root, 11);
        let registry = root
            .join("epoch-10")
            .join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
        let mut payload = fs::read(&registry).unwrap();
        payload[32] ^= 0xff;
        fs::write(&registry, payload).unwrap();
        let epochs = [11];
        let mut config = config(&root, &epochs, None);
        config.dry_run = true;

        let error = seed_previous_blockhash_tails_to(config, &mut Vec::new()).unwrap_err();
        assert!(format!("{error:#}").contains("V3 blockhash differs"));
        assert!(!target.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE).exists());
    }

    #[test]
    fn discovery_continues_after_bad_target_and_skips_owned_source() {
        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        let receipts = temp.0.join("receipts");
        write_source(&root, 20, 305);
        let good_target = write_target(&root, 21);
        write_source(&root, 22, 305);
        let bad_target = write_target(&root, 23);
        fs::write(
            bad_target.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE),
            b"invalid",
        )
        .unwrap();
        write_source(&root, 25, 305);
        fs::write(root.join("epoch-25").join(OWNERSHIP_FILE), b"{}\n").unwrap();
        let owned_target = write_target(&root, 26);
        let config = SeedPreviousBlockhashTailsConfig {
            archive_root: &root,
            epochs: &[],
            discover: true,
            start_epoch: Some(21),
            end_epoch: Some(26),
            receipt_dir: Some(&receipts),
            dry_run: false,
        };

        let mut output = Vec::new();
        let outcome = seed_previous_blockhash_tails_to(config, &mut output).unwrap();
        assert_eq!(outcome.candidates, 2);
        assert_eq!(outcome.errors, 1);
        let records = output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.last().unwrap()["action"], "discovery_complete");
        assert_eq!(records.last().unwrap()["candidates"], 2);
        assert_eq!(records.last().unwrap()["errors"], 1);
        assert!(
            good_target
                .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                .is_file()
        );
        assert!(
            !bad_target
                .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                .exists()
        );
        assert!(
            !owned_target
                .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                .exists()
        );
    }

    #[cfg(unix)]
    #[test]
    fn target_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;

        let temp = TestDir::new();
        let root = temp.0.join("archives");
        fs::create_dir(&root).unwrap();
        write_source(&root, 30, 3);
        let target = write_target(&root, 31);
        fs::remove_file(target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE)).unwrap();
        symlink(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        )
        .unwrap();
        let epochs = [31];

        let error = seed_previous_blockhash_tails_to(config(&root, &epochs, None), &mut Vec::new())
            .unwrap_err();
        assert!(format!("{error:#}").contains("non-regular entries"));
    }
}
