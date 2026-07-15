use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use of_car_reader::versioned_transaction::VersionedTransaction;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const START_SLOT: u64 = 432_864_000;
const END_SLOT: u64 = 432_865_472;
const EXPECTED_FILES: u64 = END_SLOT - START_SLOT + 1;
const EPOCH: u64 = 1002;
const MAX_RPC_FILE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct BackfillReport {
    start_slot: u64,
    end_slot: u64,
    slots_requested: u64,
    blocks_written: u64,
    skipped_existing: u64,
    null_blocks: u64,
    rpc_errors: u64,
    first_epoch: u64,
    last_epoch: u64,
}

#[derive(Debug, Deserialize)]
struct BackfillJournalRow {
    slot: u64,
    epoch: u64,
    status: String,
    path: Option<PathBuf>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CaptureJournalRow {
    slot: u64,
    parent_slot: u64,
    block_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct HeaderView {
    parent_slot: u64,
    blockhash: String,
    previous_blockhash: String,
    block_height: Option<u64>,
    block_time: Option<i64>,
}

#[derive(Debug, Clone)]
struct StableFingerprint {
    len: u64,
    mtime_ns: i128,
    dev: u64,
    ino: u64,
}

#[derive(Debug, Clone)]
struct ValidatedBlock {
    slot: u64,
    parent_slot: u64,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    blockhash_text: String,
    previous_blockhash_text: String,
    transactions: u64,
    bytes: u64,
    sha256: [u8; 32],
}

fn fingerprint(path: &Path) -> Result<StableFingerprint> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "not a regular non-symlink file: {}",
        path.display()
    );
    ensure!(metadata.len() > 0, "empty file: {}", path.display());
    Ok(StableFingerprint {
        len: metadata.len(),
        mtime_ns: i128::from(metadata.mtime()) * 1_000_000_000 + i128::from(metadata.mtime_nsec()),
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

fn fingerprints_equal(left: &StableFingerprint, right: &StableFingerprint) -> bool {
    left.len == right.len
        && left.mtime_ns == right.mtime_ns
        && left.dev == right.dev
        && left.ino == right.ino
}

fn read_stable(path: &Path, max_bytes: Option<u64>) -> Result<Vec<u8>> {
    File::open(path)
        .with_context(|| format!("open {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", path.display()))?;
    let before = fingerprint(path)?;
    if let Some(maximum) = max_bytes {
        ensure!(
            before.len <= maximum,
            "{} is {} bytes, above limit {}",
            path.display(),
            before.len,
            maximum
        );
    }
    let mut bytes = Vec::with_capacity(usize::try_from(before.len).unwrap_or(0));
    File::open(path)
        .with_context(|| format!("open {}", path.display()))?
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    let after = fingerprint(path)?;
    ensure!(
        fingerprints_equal(&before, &after),
        "file changed while validating: {}",
        path.display()
    );
    ensure!(
        bytes.len() as u64 == before.len,
        "short read: {}",
        path.display()
    );
    Ok(bytes)
}

fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hash_file(path: &Path) -> Result<[u8; 32]> {
    Ok(sha256_bytes(&read_stable(path, None)?))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").unwrap();
    }
    out
}

fn decode_hash(value: &str, field: &str, slot: u64) -> Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    five8::decode_32(value, &mut bytes)
        .map_err(|error| anyhow!("slot {slot} invalid {field}: {error:?}"))?;
    ensure!(
        encode_hash(&bytes) == value,
        "slot {slot} noncanonical {field}"
    );
    Ok(bytes)
}

fn encode_hash(bytes: &[u8; 32]) -> String {
    let mut out = [0u8; 44];
    let len = five8::encode_32(bytes, &mut out) as usize;
    std::str::from_utf8(&out[..len]).unwrap().to_string()
}

fn object_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    key: &str,
    slot: u64,
) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("slot {slot} missing string {key}"))
}

fn validate_rpc_file(path: &Path, slot: u64) -> Result<ValidatedBlock> {
    let bytes = read_stable(path, Some(MAX_RPC_FILE_BYTES))?;
    let sha256 = sha256_bytes(&bytes);
    let value: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode JSON {}", path.display()))?;
    let object = value
        .as_object()
        .with_context(|| format!("slot {slot} getBlock result is not an object"))?;
    let parent_slot = object
        .get("parentSlot")
        .and_then(Value::as_u64)
        .with_context(|| format!("slot {slot} missing parentSlot"))?;
    let blockhash_text = object_string(object, "blockhash", slot)?.to_string();
    let previous_blockhash_text = object_string(object, "previousBlockhash", slot)?.to_string();
    let blockhash = decode_hash(&blockhash_text, "blockhash", slot)?;
    let previous_blockhash = decode_hash(&previous_blockhash_text, "previousBlockhash", slot)?;
    ensure!(
        object.contains_key("blockHeight"),
        "slot {slot} missing blockHeight"
    );
    ensure!(
        object.contains_key("blockTime"),
        "slot {slot} missing blockTime"
    );
    ensure!(
        object.contains_key("rewards"),
        "slot {slot} missing rewards"
    );
    let transactions = object
        .get("transactions")
        .and_then(Value::as_array)
        .with_context(|| format!("slot {slot} missing transactions array"))?;
    for (tx_index, transaction) in transactions.iter().enumerate() {
        let transaction = transaction
            .as_object()
            .with_context(|| format!("slot {slot} transaction {tx_index} is not an object"))?;
        ensure!(
            transaction.contains_key("meta"),
            "slot {slot} transaction {tx_index} has no meta field"
        );
        let tuple = transaction
            .get("transaction")
            .and_then(Value::as_array)
            .with_context(|| format!("slot {slot} transaction {tx_index} has no base64 tuple"))?;
        ensure!(
            tuple.len() == 2 && tuple.get(1).and_then(Value::as_str) == Some("base64"),
            "slot {slot} transaction {tx_index} has invalid encoding tuple"
        );
        let payload = tuple[0]
            .as_str()
            .with_context(|| format!("slot {slot} transaction {tx_index} payload is not text"))?;
        let decoded = BASE64_STANDARD
            .decode(payload)
            .with_context(|| format!("slot {slot} transaction {tx_index} invalid base64"))?;
        ensure!(
            BASE64_STANDARD.encode(&decoded) == payload,
            "slot {slot} transaction {tx_index} base64 is not canonical"
        );
        wincode::deserialize::<VersionedTransaction<'_>>(&decoded).with_context(|| {
            format!("slot {slot} transaction {tx_index} invalid Solana wire transaction")
        })?;
    }
    Ok(ValidatedBlock {
        slot,
        parent_slot,
        blockhash,
        previous_blockhash,
        blockhash_text,
        previous_blockhash_text,
        transactions: transactions.len() as u64,
        bytes: bytes.len() as u64,
        sha256,
    })
}

fn read_last_capture_row(path: &Path) -> Result<(CaptureJournalRow, u64)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut last = None;
    let mut rows = 0u64;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        last = Some(serde_json::from_str::<CaptureJournalRow>(&line)?);
        rows += 1;
    }
    Ok((last.context("previous capture journal is empty")?, rows))
}

fn read_first_capture_row(path: &Path) -> Result<CaptureJournalRow> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if !line.trim().is_empty() {
            return serde_json::from_str(&line).context("decode active capture first journal row");
        }
    }
    bail!("active capture journal is empty")
}

fn registry_hash(path: &Path, first: bool) -> Result<([u8; 32], u64)> {
    let fingerprint = fingerprint(path)?;
    ensure!(
        fingerprint.len % 32 == 0,
        "blockhash registry size is not a multiple of 32: {}",
        path.display()
    );
    let records = fingerprint.len / 32;
    let mut file = File::open(path)?;
    if !first {
        file.seek(SeekFrom::End(-32))?;
    }
    let mut hash = [0u8; 32];
    file.read_exact(&mut hash)?;
    Ok((hash, records))
}

fn provider_header(label: &str, url: &str, slot: u64) -> Result<HeaderView> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": slot,
        "method": "getBlock",
        "params": [slot, {
            "encoding": "base64",
            "transactionDetails": "none",
            "rewards": false,
            "commitment": "finalized",
            "maxSupportedTransactionVersion": 0
        }]
    })
    .to_string();
    let output = Command::new("/usr/bin/wget")
        .args([
            "--quiet",
            "--output-document=-",
            "--timeout=60",
            "--tries=1",
            "--header=content-type: application/json",
            &format!("--post-data={body}"),
            url,
        ])
        .output()
        .with_context(|| format!("start {label} header probe for slot {slot}"))?;
    ensure!(
        output.status.success(),
        "{label} header probe failed for slot {slot} with status {:?}",
        output.status.code()
    );
    let response: Value = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("decode {label} header response for slot {slot}"))?;
    ensure!(
        response.get("error").is_none_or(Value::is_null),
        "{label} returned JSON-RPC error for slot {slot}"
    );
    let object = response
        .get("result")
        .and_then(Value::as_object)
        .with_context(|| format!("{label} returned no block for slot {slot}"))?;
    let blockhash = object_string(object, "blockhash", slot)?.to_string();
    let previous_blockhash = object_string(object, "previousBlockhash", slot)?.to_string();
    decode_hash(&blockhash, "provider blockhash", slot)?;
    decode_hash(&previous_blockhash, "provider previousBlockhash", slot)?;
    Ok(HeaderView {
        parent_slot: object
            .get("parentSlot")
            .and_then(Value::as_u64)
            .with_context(|| format!("{label} slot {slot} missing parentSlot"))?,
        blockhash,
        previous_blockhash,
        block_height: object.get("blockHeight").and_then(Value::as_u64),
        block_time: object.get("blockTime").and_then(Value::as_i64),
    })
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    ensure!(
        !path.exists(),
        "refusing to replace existing {}",
        path.display()
    );
    let parent = path.parent().context("atomic output has no parent")?;
    let temp = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)
        .with_context(|| format!("create {}", temp.display()))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&temp, path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn required_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<PathBuf> {
    args.next()
        .map(PathBuf::from)
        .with_context(|| format!("missing argument {name}"))
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let rpc_dir = required_arg(&mut args, "rpc_dir")?;
    let report_path = required_arg(&mut args, "backfill_report")?;
    let journal_path = required_arg(&mut args, "backfill_journal")?;
    let previous_journal = required_arg(&mut args, "previous_capture_journal")?;
    let previous_registry = required_arg(&mut args, "previous_blockhash_registry")?;
    let active_journal = required_arg(&mut args, "active_capture_journal")?;
    let active_registry = required_arg(&mut args, "active_blockhash_registry")?;
    let complete_path = required_arg(&mut args, "complete_output")?;
    ensure!(args.next().is_none(), "unexpected extra arguments");

    let report_bytes = read_stable(&report_path, Some(1024 * 1024))?;
    let report: BackfillReport = serde_json::from_slice(&report_bytes)?;
    ensure!(
        report.start_slot == START_SLOT && report.end_slot == END_SLOT,
        "backfill report range mismatch"
    );
    ensure!(
        report.slots_requested == EXPECTED_FILES,
        "backfill report slot count mismatch"
    );
    ensure!(
        report.null_blocks == 0,
        "backfill reported {} null blocks",
        report.null_blocks
    );
    ensure!(
        report.rpc_errors == 0,
        "backfill reported {} RPC errors",
        report.rpc_errors
    );
    ensure!(
        report.blocks_written + report.skipped_existing == EXPECTED_FILES,
        "backfill success count mismatch"
    );
    ensure!(
        report.first_epoch == EPOCH && report.last_epoch == EPOCH,
        "backfill report epoch mismatch"
    );

    let journal_bytes = read_stable(&journal_path, Some(16 * 1024 * 1024))?;
    let mut journal_slots = BTreeSet::new();
    let mut journal_rows = 0u64;
    for (line_index, line) in journal_bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let row: BackfillJournalRow = serde_json::from_slice(line)
            .with_context(|| format!("decode backfill journal line {}", line_index + 1))?;
        ensure!(
            row.epoch == EPOCH,
            "journal slot {} has wrong epoch",
            row.slot
        );
        ensure!(
            row.status == "Written" || row.status == "SkippedExisting",
            "journal slot {} has failure status {}",
            row.slot,
            row.status
        );
        ensure!(
            row.error.is_none(),
            "journal slot {} has an error",
            row.slot
        );
        ensure!(
            row.path.is_some(),
            "journal slot {} has no output path",
            row.slot
        );
        ensure!(
            journal_slots.insert(row.slot),
            "duplicate journal slot {}",
            row.slot
        );
        journal_rows += 1;
    }
    ensure!(
        journal_rows == EXPECTED_FILES,
        "backfill journal row count {journal_rows} != {EXPECTED_FILES}"
    );
    ensure!(
        journal_slots == (START_SLOT..=END_SLOT).collect(),
        "backfill journal slot set is incomplete"
    );

    let mut paths = BTreeMap::new();
    for entry in fs::read_dir(&rpc_dir).with_context(|| format!("read {}", rpc_dir.display()))? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.file_type().is_file(),
            "unexpected non-file in RPC directory: {}",
            entry.path().display()
        );
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let slot = name
            .strip_prefix("slot-")
            .and_then(|value| value.strip_suffix(".getBlock.json"))
            .with_context(|| format!("unexpected RPC directory entry {name}"))?
            .parse::<u64>()?;
        ensure!(
            (START_SLOT..=END_SLOT).contains(&slot),
            "out-of-range RPC file slot {slot}"
        );
        ensure!(
            paths.insert(slot, entry.path()).is_none(),
            "duplicate RPC file slot {slot}"
        );
    }
    ensure!(
        paths.len() as u64 == EXPECTED_FILES,
        "RPC file count {} != {EXPECTED_FILES}",
        paths.len()
    );

    let (previous_row, previous_rows) = read_last_capture_row(&previous_journal)?;
    let active_row = read_first_capture_row(&active_journal)?;
    ensure!(
        previous_row.slot == START_SLOT - 1,
        "previous capture ends at {}, expected {}",
        previous_row.slot,
        START_SLOT - 1
    );
    ensure!(
        active_row.slot == END_SLOT + 1,
        "active capture begins at {}, expected {}",
        active_row.slot,
        END_SLOT + 1
    );
    ensure!(
        active_row.parent_slot == END_SLOT,
        "active first row parent {} != {}",
        active_row.parent_slot,
        END_SLOT
    );
    ensure!(
        active_row.block_id == 0,
        "active first row block_id {} != 0",
        active_row.block_id
    );
    let (previous_hash, previous_hash_records) = registry_hash(&previous_registry, false)?;
    ensure!(
        previous_hash_records == previous_rows,
        "previous capture journal/hash record count mismatch"
    );
    let (active_hash, _) = registry_hash(&active_registry, true)?;

    let mut previous_slot = previous_row.slot;
    let mut chain_hash = previous_hash;
    let mut manifest = Sha256::new();
    let mut total_bytes = 0u64;
    let mut total_transactions = 0u64;
    let mut first_block = None;
    let mut last_block = None;
    for slot in START_SLOT..=END_SLOT {
        let block = validate_rpc_file(paths.get(&slot).unwrap(), slot)?;
        ensure!(block.slot == slot, "internal slot mismatch");
        ensure!(
            block.parent_slot == previous_slot,
            "slot {slot} parent {} != previous produced slot {previous_slot}",
            block.parent_slot
        );
        ensure!(
            block.previous_blockhash == chain_hash,
            "slot {slot} previousBlockhash does not match slot {previous_slot}"
        );
        manifest.update(slot.to_le_bytes());
        manifest.update(block.bytes.to_le_bytes());
        manifest.update(block.sha256);
        total_bytes = total_bytes
            .checked_add(block.bytes)
            .context("byte count overflow")?;
        total_transactions = total_transactions
            .checked_add(block.transactions)
            .context("transaction count overflow")?;
        if first_block.is_none() {
            first_block = Some(block.clone());
        }
        previous_slot = slot;
        chain_hash = block.blockhash;
        last_block = Some(block);
    }
    let first_block = first_block.context("no first RPC block")?;
    let last_block = last_block.context("no last RPC block")?;

    let helius_url =
        env::var("BLOCKZILLA_HELIUS_RPC_URL").context("missing configured Helius URL")?;
    let triton_url =
        env::var("BLOCKZILLA_TRITON_RPC_URL").context("missing configured Triton URL")?;
    let boundary_slots = [START_SLOT - 1, START_SLOT, END_SLOT, END_SLOT + 1];
    let mut agreed_headers = BTreeMap::new();
    for slot in boundary_slots {
        let helius = provider_header("helius", &helius_url, slot)?;
        let triton = provider_header("triton", &triton_url, slot)?;
        ensure!(
            helius == triton,
            "configured providers disagree at slot {slot}"
        );
        agreed_headers.insert(slot, helius);
    }
    let previous_header = &agreed_headers[&(START_SLOT - 1)];
    ensure!(
        previous_header.blockhash == encode_hash(&previous_hash),
        "previous NAS capture tail hash disagrees with both providers"
    );
    let first_header = &agreed_headers[&START_SLOT];
    ensure!(
        first_header.parent_slot == previous_row.slot,
        "provider first repair parent mismatch"
    );
    ensure!(
        first_header.blockhash == first_block.blockhash_text,
        "first repair file disagrees with providers"
    );
    ensure!(
        first_header.previous_blockhash == first_block.previous_blockhash_text,
        "first repair previous hash disagrees with providers"
    );
    let last_header = &agreed_headers[&END_SLOT];
    ensure!(
        last_header.blockhash == last_block.blockhash_text,
        "last repair file disagrees with providers"
    );
    ensure!(
        last_header.previous_blockhash == last_block.previous_blockhash_text,
        "last repair previous hash disagrees with providers"
    );
    let active_header = &agreed_headers[&(END_SLOT + 1)];
    ensure!(
        active_header.parent_slot == END_SLOT,
        "provider active-first parent mismatch"
    );
    ensure!(
        active_header.previous_blockhash == last_block.blockhash_text,
        "active-first previous hash does not link to repaired tail"
    );
    ensure!(
        active_header.blockhash == encode_hash(&active_hash),
        "active NAS first blockhash disagrees with both providers"
    );

    let report_sha = sha256_bytes(&report_bytes);
    let journal_sha = sha256_bytes(&journal_bytes);
    let manifest_sha: [u8; 32] = manifest.finalize().into();
    let validator_sha = hash_file(&env::current_exe()?)?;
    let mut evidence = Sha256::new();
    evidence.update(report_sha);
    evidence.update(journal_sha);
    evidence.update(manifest_sha);
    evidence.update(validator_sha);
    evidence.update(previous_hash);
    evidence.update(first_block.blockhash);
    evidence.update(last_block.blockhash);
    evidence.update(active_hash);
    let evidence_sha: [u8; 32] = evidence.finalize().into();
    let validated_unix_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let receipt = json!({
        "version": 1,
        "state": "rpc_block_coverage_complete_missing_poh_and_shredding",
        "epoch": EPOCH,
        "start_slot": START_SLOT,
        "end_slot": END_SLOT,
        "rpc_blocks": EXPECTED_FILES,
        "rpc_bytes": total_bytes,
        "transactions_wire_validated": total_transactions,
        "validation": {
            "all_files_regular_non_symlinks": true,
            "file_size_limit_bytes": MAX_RPC_FILE_BYTES,
            "canonical_base64_and_solana_wire_transactions": true,
            "full_parent_and_previous_blockhash_chain": true,
            "backfill_report_rpc_errors": report.rpc_errors,
            "backfill_report_null_blocks": report.null_blocks,
            "backfill_journal_rows": journal_rows,
            "dual_provider_boundary_headers_agree": true,
            "dual_provider_boundary_slots": boundary_slots,
            "nas_previous_capture_tail_matches": true,
            "nas_active_capture_first_row_matches": true
        },
        "boundaries": {
            "previous_live_slot": previous_row.slot,
            "previous_live_blockhash": encode_hash(&previous_hash),
            "first_rpc_slot": first_block.slot,
            "first_rpc_blockhash": first_block.blockhash_text,
            "last_rpc_slot": last_block.slot,
            "last_rpc_blockhash": last_block.blockhash_text,
            "active_first_slot": active_row.slot,
            "active_first_blockhash": encode_hash(&active_hash),
            "agreed_provider_headers": agreed_headers
        },
        "sha256": {
            "backfill_report": hex(&report_sha),
            "backfill_journal": hex(&journal_sha),
            "rpc_file_manifest_slot_size_content": hex(&manifest_sha),
            "validator_binary": hex(&validator_sha),
            "combined_validation_evidence": hex(&evidence_sha)
        },
        "limitations": [
            "RPC-only blocks do not contain original PoH entries.",
            "RPC-only blocks do not contain shred-boundary metadata.",
            "This receipt does not publish or modify the active capture or a canonical archive."
        ],
        "validated_unix_secs": validated_unix_secs
    });
    let mut receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
    receipt_bytes.push(b'\n');
    atomic_write(&complete_path, &receipt_bytes)?;
    let complete_sha = sha256_bytes(&receipt_bytes);
    let sha_path = complete_path.with_extension("sha256");
    let sha_line = format!(
        "{}  {}\n",
        hex(&complete_sha),
        complete_path.file_name().unwrap().to_string_lossy()
    );
    atomic_write(&sha_path, sha_line.as_bytes())?;

    println!(
        "{}",
        serde_json::to_string(&json!({
            "state": "complete",
            "rpc_blocks": EXPECTED_FILES,
            "rpc_bytes": total_bytes,
            "transactions_wire_validated": total_transactions,
            "complete_path": complete_path,
            "complete_sha256": hex(&complete_sha),
            "manifest_sha256": hex(&manifest_sha),
            "validator_sha256": hex(&validator_sha)
        }))?
    );
    Ok(())
}
