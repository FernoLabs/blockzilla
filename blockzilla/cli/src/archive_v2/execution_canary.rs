//! Finite execution-only canary build. This does not enable the legacy canonical finalizer.
use super::*;
use anyhow::{Context, Result, ensure};
use blockzilla_live_format::execution_canary_v1::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path},
};

const MAX_JSON: u64 = 16 << 20;
const MAX_FILE: u64 = 512 << 20;
// Normalized input can be larger than the compressed publisher objects.
const MAX_INPUT_FILE: u64 = 1 << 30;
const MAX_BLOCKS: usize = 4096;

pub(crate) fn build(input: &Path, output: &Path, level: i32) -> Result<()> {
    build_with_options(input, output, level, false)
}

pub(crate) fn build_with_options(
    input: &Path,
    output: &Path,
    level: i32,
    skip_verification: bool,
) -> Result<()> {
    ensure!(!output.exists(), "canary output already exists");
    let source_path = input.join(NORMALIZED_EXECUTION_FILE);
    let bytes = read_bounded(&source_path, MAX_JSON)?;
    let receipt: NormalizedExecutionV1 = serde_json::from_slice(&bytes)?;
    validate_receipt(&receipt)?;
    verify_files(input, &receipt.files)?;
    validate_provenance(input, &receipt)?;
    validate_normalized_blocks(input, &receipt)?;
    validate_available_poh(input, &receipt)?;
    let parent = output.parent().context("output has no parent")?;
    let name = output
        .file_name()
        .context("output has no filename")?
        .to_string_lossy();
    let stage = parent.join(format!(".{name}.execution-canary"));
    fs::create_dir(&stage)
        .context("create fresh canary stage; inspect an old stage before retry")?;
    // The existing writer is used only inside this private stage. It emits no canonical PoH/shred files.
    let built = build_degraded_hot_blocks_inner(
        input,
        &stage,
        level,
        None,
        false,
        LiveRegistrySource::Scan,
    )?;
    ensure!(
        built.blocks == receipt.blocks.len() as u64
            && built.transactions == receipt.transactions
            && built.signatures == receipt.signatures,
        "compact writer counts differ from source receipt"
    );
    let first = receipt.blocks.first().context("empty canary")?;
    write_prev_blockhash_tail(
        &stage,
        &[PreviousBlockhash {
            hash: first.parent_blockhash,
            slot: first.parent_slot,
        }],
    )?;
    let mut evidence_files = Vec::new();
    for record in &receipt.files {
        if record.path.starts_with("evidence/") {
            let name = Path::new(&record.path)
                .file_name()
                .context("evidence filename missing")?
                .to_str()
                .context("non-UTF8 evidence filename")?;
            ensure!(safe_flat_name(name), "unsafe evidence filename");
            let target = stage.join(name);
            ensure!(
                !target.exists(),
                "evidence filename collides with compact object"
            );
            copy_exact(&input.join(&record.path), &target, record)?;
            evidence_files.push(name.to_string());
        }
    }
    let mut source = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(stage.join(NORMALIZED_EXECUTION_FILE))?;
    source.write_all(&bytes)?;
    source.sync_all()?;
    // Verify every input again after the writer has finished reading it.
    verify_files(input, &receipt.files)?;
    ensure!(
        read_bounded(&source_path, MAX_JSON)? == bytes,
        "normalization receipt changed during build"
    );
    if !skip_verification {
        verify_archive(&stage, &receipt)?;
    }
    ensure!(
        !stage.join(SHREDDING_FILE).exists() && !stage.join(POH_FILE).exists(),
        "canary emitted a canonical evidence sidecar"
    );
    let mut paths = fs::read_dir(&stage)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let mut files = Vec::new();
    for path in paths {
        let name = path
            .file_name()
            .context("missing filename")?
            .to_str()
            .context("non-UTF8 filename")?;
        ensure!(
            safe_flat_name(name) && fs::symlink_metadata(&path)?.file_type().is_file(),
            "unexpected non-flat canary object {}",
            path.display()
        );
        let record = hash_file(&path, name)?;
        ensure!(
            record.bytes <= MAX_FILE,
            "canary object exceeds publisher's 512 MiB bound: {name}"
        );
        make_readonly(&path)?;
        files.push(record);
    }
    let marker = json!({
        "schema": EXECUTION_CANARY_SCHEMA, "source_profile": EXECUTION_SOURCE_PROFILE, "state": "COMPLETE",
        "canonical": false, "publication_ready": false, "finalized": true, "archive_verified": !skip_verification,
        "archive_verification_scope": if skip_verification { "not run; source integrity and writer counts checked" } else { "every hot frame/message/metadata decoded; source/index/count/signature geometry checked; no full semantic or cryptographic parity claim" },
        "finality_basis": receipt.finality_basis, "source_receipt_sha256": hash_bytes(&bytes),
        "raw_capture_receipt_sha256": receipt.source_receipt_sha256,
        "coverage_receipt_sha256": receipt.coverage_receipt_sha256, "rpc_response_sha256": receipt.rpc_response_sha256,
        "genesis_hash": receipt.genesis_hash, "node_identity": receipt.node_identity,
        "epoch": receipt.epoch, "start_slot": receipt.start_slot, "end_slot_exclusive": receipt.end_slot_exclusive,
        "blocks": receipt.blocks.len(), "transactions": receipt.transactions, "signatures": receipt.signatures,
        "entries": receipt.entries, "produced_blocks": receipt.blocks,
        "slot_coverage": "exact produced-slot set from bound operator-retained same-validator finalized getBlocks reply; omitted in-range slots are skipped under that source authority",
        "shred_evidence": "absent", "poh_structure_verified": true, "poh_hashes_recomputed": false,
        "signatures_cryptographically_verified": false, "footer_completeness_verified": false,
        "execution_projection": receipt.execution_projection,
        "authoritative_execution": "events.wal plus CURRENT-GRPC-SEALED.v1.json and CURRENT-GRPC-INVENTORY.v1.json; full current known-schema envelopes, not the hot projection",
        "evidence_files": evidence_files, "files": files,
    });
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(stage.join(EXECUTION_CANARY_FILE))?;
    serde_json::to_writer_pretty(&mut file, &marker)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    make_readonly(&stage.join(EXECUTION_CANARY_FILE))?;
    File::open(&stage)?.sync_all()?;
    fs::rename(&stage, output)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn validate_receipt(r: &NormalizedExecutionV1) -> Result<()> {
    ensure!(
        r.schema == NORMALIZED_EXECUTION_SCHEMA
            && r.source_profile == EXECUTION_SOURCE_PROFILE
            && r.state == "COMPLETE"
            && !r.canonical
            && r.finalized
            && r.poh_structure_verified,
        "not a complete noncanonical finalized-execution normalization"
    );
    ensure!(
        r.finality_basis == "operator-trusted-own-validator-finalized-subscription-and-rpc"
            && r.shred_evidence == "absent"
            && !r.poh_hashes_recomputed
            && !r.signatures_cryptographically_verified,
        "unsupported canary authority or capability claims"
    );
    ensure!(
        !r.blocks.is_empty()
            && r.blocks.len() <= MAX_BLOCKS
            && r.start_slot > 0
            && r.start_slot < r.end_slot_exclusive,
        "empty or oversized canary range"
    );
    ensure!(
        r.epoch == r.start_slot / 432_000 && r.epoch == (r.end_slot_exclusive - 1) / 432_000,
        "epoch/range mismatch"
    );
    let mut transactions = 0u64;
    let mut signatures = 0u64;
    let mut entries = 0u64;
    for (index, row) in r.blocks.iter().enumerate() {
        ensure!(
            row.block_id as usize == index
                && (r.start_slot..r.end_slot_exclusive).contains(&row.slot)
                && row.parent_slot < row.slot
                && row.entries > 0,
            "invalid source block row"
        );
        if index == 0 {
            ensure!(
                row.parent_slot < r.start_slot,
                "range does not start at an anchored produced chain"
            );
        } else {
            let previous = &r.blocks[index - 1];
            ensure!(
                row.parent_slot == previous.slot && row.parent_blockhash == previous.blockhash,
                "source parent chain mismatch"
            );
        }
        transactions = transactions
            .checked_add(row.transactions)
            .context("transaction total overflow")?;
        signatures = signatures
            .checked_add(row.signatures)
            .context("signature total overflow")?;
        entries = entries
            .checked_add(row.entries)
            .context("entry total overflow")?;
    }
    ensure!(
        (transactions, signatures, entries) == (r.transactions, r.signatures, r.entries),
        "source receipt totals mismatch"
    );
    ensure!(r.files.len() <= 32, "too many input objects");
    Ok(())
}
fn verify_files(root: &Path, records: &[ExecutionFileV1]) -> Result<()> {
    let mut names = BTreeSet::new();
    let mut total = 0u64;
    for record in records {
        ensure!(
            safe_relative(&record.path) && names.insert(record.path.as_str()),
            "unsafe or duplicate input object"
        );
        reject_symlink_components(root, &record.path)?;
        total = total
            .checked_add(record.bytes)
            .context("input total overflow")?;
        ensure!(
            record.bytes <= MAX_INPUT_FILE && total <= 8u64 << 30,
            "canary input exceeds byte bounds"
        );
        let actual = hash_file(&root.join(&record.path), &record.path)?;
        ensure!(
            actual.bytes == record.bytes && actual.sha256 == record.sha256,
            "input object identity mismatch: {}",
            record.path
        );
    }
    for required in [
        "blocks/live-no-registry-blocks.bin",
        "index/signatures.bin",
        "index/blockhash_registry.bin",
        "evidence/available-poh.wincode",
        "evidence/events.wal",
        "evidence/CURRENT-GRPC-SEALED.v1.json",
        "evidence/CURRENT-GRPC-INVENTORY.v1.json",
        "evidence/finalized-coverage.json",
        "evidence/rpc-get-blocks.json",
        "evidence/validator-rpc-before.json",
        "evidence/validator-rpc-after.json",
    ] {
        ensure!(names.contains(required), "missing source object {required}");
    }
    Ok(())
}
fn validate_provenance(root: &Path, r: &NormalizedExecutionV1) -> Result<()> {
    let seal_bytes = read_bounded(&root.join("evidence/CURRENT-GRPC-SEALED.v1.json"), MAX_JSON)?;
    let coverage_bytes = read_bounded(&root.join("evidence/finalized-coverage.json"), MAX_JSON)?;
    let rpc_bytes = read_bounded(&root.join("evidence/rpc-get-blocks.json"), MAX_JSON)?;
    ensure!(
        hash_bytes(&seal_bytes) == r.source_receipt_sha256
            && hash_bytes(&coverage_bytes) == r.coverage_receipt_sha256
            && hash_bytes(&rpc_bytes) == r.rpc_response_sha256,
        "retained source receipts differ from declared provenance"
    );
    let coverage: FinalizedRpcCoverageV1 = serde_json::from_slice(&coverage_bytes)?;
    ensure!(
        coverage.schema == "hivezilla-finalized-rpc-coverage-v1"
            && coverage.commitment == "finalized"
            && coverage.genesis_hash == r.genesis_hash
            && coverage.node_identity == r.node_identity
            && coverage.start_slot == r.start_slot
            && coverage.end_slot_exclusive == r.end_slot_exclusive
            && coverage.finalized_slot >= r.end_slot_exclusive - 1
            && coverage.produced_slots == r.blocks.iter().map(|b| b.slot).collect::<Vec<_>>()
            && coverage.source_receipt_sha256 == r.source_receipt_sha256
            && coverage.rpc_response_sha256 == r.rpc_response_sha256,
        "retained coverage differs from normalized range, identity, or produced slots"
    );
    let rpc: serde_json::Value = serde_json::from_slice(&rpc_bytes)?;
    ensure!(rpc.get("error").is_none(), "retained RPC reply has error");
    let slots: Vec<u64> = serde_json::from_value(
        rpc.get("result")
            .context("retained RPC reply lacks result")?
            .clone(),
    )?;
    ensure!(
        slots == coverage.produced_slots,
        "retained RPC slots differ"
    );
    let seal: serde_json::Value = serde_json::from_slice(&seal_bytes)?;
    ensure!(
        seal.get("state").and_then(|v| v.as_str()) == Some("SEALED")
            && seal.get("source_profile").and_then(|v| v.as_str())
                == Some(EXECUTION_SOURCE_PROFILE)
            && seal.get("requested_commitment").and_then(|v| v.as_str()) == Some("finalized"),
        "retained capture has the wrong state or authority"
    );
    let identity = seal
        .get("validator_identity")
        .context("retained source lacks identity")?;
    ensure!(
        identity.get("genesis_hash").and_then(|v| v.as_str()) == Some(r.genesis_hash.as_str())
            && identity.get("node_identity").and_then(|v| v.as_str())
                == Some(r.node_identity.as_str()),
        "retained capture identity differs"
    );
    let files = seal
        .get("files")
        .and_then(|v| v.as_array())
        .context("retained source lacks files")?;
    for name in [
        "events.wal",
        "validator-rpc-before.json",
        "validator-rpc-after.json",
    ] {
        let matches = files
            .iter()
            .filter(|f| f.get("path").and_then(|v| v.as_str()) == Some(name))
            .collect::<Vec<_>>();
        ensure!(matches.len() == 1, "retained source lacks unique {name}");
        let object = r
            .files
            .iter()
            .find(|f| f.path == format!("evidence/{name}"))
            .context("source object absent from normalization")?;
        ensure!(
            matches[0].get("bytes").and_then(|v| v.as_u64()) == Some(object.bytes)
                && matches[0].get("sha256").and_then(|v| v.as_str())
                    == Some(object.sha256.as_str()),
            "retained source object differs from capture seal"
        );
    }
    Ok(())
}
fn validate_normalized_blocks(root: &Path, receipt: &NormalizedExecutionV1) -> Result<()> {
    validate_normalized_block_rows(root, &receipt.blocks)
}
pub(super) fn validate_normalized_block_rows(
    root: &Path,
    blocks: &[ExecutionBlockV1],
) -> Result<()> {
    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(File::open(
        root.join("blocks/live-no-registry-blocks.bin"),
    )?));
    let mut hashes = File::open(root.join("index/blockhash_registry.bin"))?;
    let mut signatures = File::open(root.join("index/signatures.bin"))?;
    for expected in blocks {
        let (_, block) =
            read_live_no_registry_block(&mut reader)?.context("normalized block missing")?;
        ensure!(
            block.header.compact.slot == expected.slot
                && block.header.compact.parent_slot == expected.parent_slot
                && block.header.compact.blockhash == expected.block_id
                && block.txs.len() as u64 == expected.transactions,
            "normalized block identity/count mismatch"
        );
        let mut hash = [0u8; 32];
        hashes.read_exact(&mut hash)?;
        ensure!(hash == expected.blockhash, "blockhash registry mismatch");
        let mut count = 0u64;
        for (index, tx) in block.txs.iter().enumerate() {
            ensure!(
                tx.tx_index as usize == index
                    && matches!(tx.metadata, Some(WincodeArchiveV2Payload::Decoded { .. })),
                "normalized transaction index/metadata missing"
            );
            let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
                anyhow::bail!("raw transaction cannot enter hot canary");
            };
            for signature in &value.signatures {
                let mut actual = [0u8; 64];
                signatures.read_exact(&mut actual)?;
                ensure!(
                    signature.as_slice() == actual,
                    "signature sidecar differs from source transaction"
                );
                count += 1;
            }
        }
        ensure!(count == expected.signatures, "signature count mismatch");
    }
    ensure!(
        read_live_no_registry_block(&mut reader)?.is_none(),
        "extra normalized blocks"
    );
    let mut extra = [0];
    ensure!(
        hashes.read(&mut extra)? == 0 && signatures.read(&mut extra)? == 0,
        "extra identity sidecar bytes"
    );
    Ok(())
}
fn validate_available_poh(root: &Path, receipt: &NormalizedExecutionV1) -> Result<()> {
    validate_available_poh_rows(root, &receipt.blocks)
}
pub(super) fn validate_available_poh_rows(root: &Path, blocks: &[ExecutionBlockV1]) -> Result<()> {
    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(File::open(
        root.join("evidence/available-poh.wincode"),
    )?));
    let parse = |bytes: &[u8]| -> Result<WincodeArchiveV2PohRecord> {
        Ok(wincode::config::deserialize(
            bytes,
            wincode_leb128_config(),
        )?)
    };
    for expected in blocks {
        let (_, row) = reader
            .read_bytes_with_limit(LIVE_FINALIZER_MAX_FRAME_SIZE, parse)?
            .context("available PoH block missing")?;
        let transactions = row.entries.iter().try_fold(0u64, |n, e| {
            n.checked_add(u64::from(e.tx_count))
                .context("PoH count overflow")
        })?;
        let signatures = row.entries.iter().try_fold(0u64, |n, e| {
            n.checked_add(u64::from(e.signature_count))
                .context("PoH signature count overflow")
        })?;
        ensure!(
            row.block_id == expected.block_id
                && row.slot == expected.slot
                && row.entries.len() as u64 == expected.entries
                && transactions == expected.transactions
                && signatures == expected.signatures
                && row
                    .entries
                    .last()
                    .is_some_and(|e| e.hash == expected.blockhash),
            "available PoH identity/count/final-hash mismatch"
        );
    }
    ensure!(
        reader
            .read_bytes_with_limit(LIVE_FINALIZER_MAX_FRAME_SIZE, parse)?
            .is_none(),
        "extra available PoH records"
    );
    Ok(())
}
fn verify_archive(root: &Path, receipt: &NormalizedExecutionV1) -> Result<()> {
    let index = read_archive_v2_hot_block_index(&root.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    ensure!(
        index.rows.len() == receipt.blocks.len(),
        "hot index row count mismatch"
    );
    let mut file = File::open(root.join(ARCHIVE_V2_BLOCKS_FILE))?;
    let mut offset = 0u64;
    let mut tx_ordinal = 0u64;
    let mut sig_ordinal = 0u64;
    let mut decoder = zstd::bulk::Decompressor::new()?;
    for (row, expected) in index.rows.iter().zip(&receipt.blocks) {
        ensure!(
            row.block_id == expected.block_id
                && row.slot == expected.slot
                && row.tx_count as u64 == expected.transactions
                && row.signature_count as u64 == expected.signatures
                && row.compressed_offset == offset
                && row.first_tx_ordinal == tx_ordinal
                && row.first_signature_ordinal == sig_ordinal,
            "hot index identity/count/offset mismatch"
        );
        ensure!(
            row.uncompressed_len as usize <= LIVE_FINALIZER_MAX_FRAME_SIZE
                && row.compressed_len as usize <= LIVE_FINALIZER_MAX_FRAME_SIZE,
            "hot frame exceeds verification bound"
        );
        let mut compressed = vec![0u8; row.compressed_len as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut compressed)?;
        let bytes = decoder.decompress(&compressed, row.uncompressed_len as usize)?;
        ensure!(
            bytes.len() == row.uncompressed_len as usize,
            "hot decompressed length mismatch"
        );
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&bytes, wincode_leb128_config())?;
        ensure!(
            block.header.slot == expected.slot
                && block.header.parent_slot == expected.parent_slot
                && block.header.blockhash_id == expected.block_id
                && block.header.previous_blockhash_id == expected.block_id.saturating_sub(1)
                && block.tx_count == row.tx_count
                && block.tx_rows.len() == row.tx_count as usize,
            "hot block identity mismatch"
        );
        let mut msg = 0usize;
        let mut meta = 0usize;
        let mut sigs = 0u64;
        for (position, tx) in block.tx_rows.iter().enumerate() {
            ensure!(
                tx.tx_index as usize == position
                    && tx.message_offset as usize == msg
                    && tx.metadata_offset as usize == meta
                    && tx.reserved == [0; 3]
                    && tx.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0,
                "hot transaction geometry mismatch"
            );
            let msg_end = msg
                .checked_add(tx.message_len as usize)
                .context("message bound overflow")?;
            let meta_end = meta
                .checked_add(tx.metadata_len as usize)
                .context("metadata bound overflow")?;
            let _: ArchiveV2HotMessagePayload = wincode::config::deserialize(
                block
                    .message_bytes
                    .get(msg..msg_end)
                    .context("message out of bounds")?,
                wincode_leb128_config(),
            )?;
            let _: CompactMetaV1 = wincode::config::deserialize(
                block
                    .metadata_bytes
                    .get(meta..meta_end)
                    .context("metadata out of bounds")?,
                wincode_leb128_config(),
            )?;
            msg = msg_end;
            meta = meta_end;
            sigs += u64::from(tx.signature_count);
        }
        ensure!(
            msg == block.message_bytes.len()
                && meta == block.metadata_bytes.len()
                && sigs == expected.signatures,
            "hot block has trailing data or wrong signatures"
        );
        offset += u64::from(row.compressed_len);
        tx_ordinal += expected.transactions;
        sig_ordinal += expected.signatures;
    }
    ensure!(
        file.metadata()?.len() == offset
            && index.blob_file_bytes == offset
            && tx_ordinal == receipt.transactions
            && sig_ordinal == receipt.signatures,
        "hot archive totals mismatch"
    );
    let mut meta = WincodeLeb128FramedReader::new(BufReader::new(File::open(
        root.join(ARCHIVE_V2_META_FILE),
    )?));
    let parse = |bytes: &[u8]| -> Result<ArchiveV2HotMetaRecord> {
        Ok(wincode::config::deserialize(
            bytes,
            wincode_leb128_config(),
        )?)
    };
    let (_, head) = meta
        .read_bytes_with_limit(MAX_JSON as usize, parse)?
        .context("missing metadata header")?;
    ensure!(
        matches!(head, ArchiveV2HotMetaRecord::Header(_)),
        "bad metadata header"
    );
    let (_, tail) = meta
        .read_bytes_with_limit(MAX_JSON as usize, parse)?
        .context("missing metadata footer")?;
    let ArchiveV2HotMetaRecord::Footer(footer) = tail else {
        anyhow::bail!("bad metadata footer");
    };
    ensure!(
        footer.blocks == receipt.blocks.len() as u64
            && footer.transactions == receipt.transactions
            && meta
                .read_bytes_with_limit(MAX_JSON as usize, parse)?
                .is_none(),
        "metadata totals or trailing records mismatch"
    );
    Ok(())
}
pub(super) fn safe_flat_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
fn safe_relative(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}
pub(super) fn reject_symlink_components(root: &Path, name: &str) -> Result<()> {
    let mut path = root.to_path_buf();
    ensure!(
        fs::symlink_metadata(&path)?.file_type().is_dir(),
        "input root is not a directory"
    );
    for component in Path::new(name).components() {
        path.push(component.as_os_str());
        ensure!(
            !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "symlink in input path"
        );
    }
    Ok(())
}
pub(super) fn make_readonly(path: &Path) -> Result<()> {
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    File::open(path)?.sync_all()?;
    Ok(())
}
fn hash_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
pub(super) fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "JSON exceeds limit");
    Ok(bytes)
}
pub(super) fn hash_file(path: &Path, name: &str) -> Result<ExecutionFileV1> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "source is not a regular file"
    );
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    ensure!(before.len() <= MAX_INPUT_FILE, "file exceeds 1 GiB bound");
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        bytes += n as u64;
    }
    let after = file.metadata()?;
    ensure!(
        bytes == before.len()
            && before.len() == after.len()
            && before.modified()? == after.modified()?,
        "source changed while hashing"
    );
    Ok(ExecutionFileV1 {
        path: name.into(),
        bytes,
        sha256: hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    })
}
pub(super) fn copy_exact(source: &Path, target: &Path, record: &ExecutionFileV1) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target)?;
    std::io::copy(&mut File::open(source)?, &mut file)?;
    file.sync_all()?;
    let actual = hash_file(target, &record.path)?;
    ensure!(
        actual.bytes == record.bytes && actual.sha256 == record.sha256,
        "copied evidence differs"
    );
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    fn receipt() -> NormalizedExecutionV1 {
        NormalizedExecutionV1 {
            schema: NORMALIZED_EXECUTION_SCHEMA.into(),
            source_profile: EXECUTION_SOURCE_PROFILE.into(),
            state: "COMPLETE".into(),
            canonical: false,
            finalized: true,
            finality_basis: "operator-trusted-own-validator-finalized-subscription-and-rpc".into(),
            epoch: 0,
            start_slot: 41,
            end_slot_exclusive: 43,
            genesis_hash: bs58::encode([1; 32]).into_string(),
            node_identity: bs58::encode([2; 32]).into_string(),
            source_receipt_sha256: "a".repeat(64),
            coverage_receipt_sha256: "b".repeat(64),
            rpc_response_sha256: "c".repeat(64),
            transactions: 1,
            signatures: 1,
            entries: 1,
            blocks: vec![ExecutionBlockV1 {
                block_id: 0,
                slot: 42,
                bank_id: 91,
                blockhash: [8; 32],
                parent_slot: 40,
                parent_blockhash: [7; 32],
                transactions: 1,
                signatures: 1,
                entries: 1,
                source_sequence: 0,
                source_raw_sha256: "d".repeat(64),
            }],
            files: vec![],
            poh_structure_verified: true,
            poh_hashes_recomputed: false,
            signatures_cryptographically_verified: false,
            shred_evidence: "absent".into(),
            execution_projection: "compact-v2-known-fields; raw source retained".into(),
            authoritative_execution: "evidence/events.wal".into(),
        }
    }
    pub(crate) fn fixture(root: &Path) -> NormalizedExecutionV1 {
        for dir in ["blocks", "index", "evidence"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        let block = WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: CompactBlockHeader {
                    slot: 42,
                    parent_slot: 40,
                    blockhash: 0,
                    previous_blockhash: 0,
                    block_time: Some(100),
                    block_height: Some(30),
                    shredding: vec![],
                    poh_entries: vec![],
                    rewards: None,
                },
                rewards: None,
            },
            txs: vec![WincodeArchiveV2NoRegistryTransaction {
                tx_index: 0,
                tx: WincodeArchiveV2Payload::Decoded {
                    source_len: 1,
                    value: WincodeArchiveV2NoRegistryTx {
                        signatures: vec![vec![4; 64]],
                        message: WincodeArchiveV2NoRegistryMessage::V1(
                            WincodeArchiveV2NoRegistryV1Message {
                                header: CompactMessageHeader {
                                    num_required_signatures: 1,
                                    num_readonly_signed_accounts: 0,
                                    num_readonly_unsigned_accounts: 0,
                                },
                                config: CompactTransactionConfig {
                                    priority_fee: Some(123),
                                    compute_unit_limit: Some(456),
                                    loaded_accounts_data_size_limit: None,
                                    heap_size: None,
                                },
                                account_keys: vec![[2; 32]],
                                recent_blockhash: [3; 32],
                                instructions: vec![],
                            },
                        ),
                    },
                },
                metadata: Some(WincodeArchiveV2Payload::Decoded {
                    source_len: 1,
                    value: WincodeArchiveV2NoRegistryMeta {
                        err: None,
                        fee: 1,
                        pre_balances: vec![10],
                        post_balances: vec![9],
                        inner_instructions: None,
                        logs: None,
                        pre_token_balances: vec![],
                        post_token_balances: vec![],
                        rewards: vec![],
                        loaded_writable_addresses: vec![],
                        loaded_readonly_addresses: vec![],
                        return_data: None,
                        compute_units_consumed: Some(10),
                        cost_units: Some(20),
                    },
                }),
            }],
        };
        let mut writer = WincodeLeb128FramedWriter::new(
            File::create(root.join("blocks/live-no-registry-blocks.bin")).unwrap(),
        );
        writer.write(&block).unwrap();
        writer.flush().unwrap();
        fs::write(root.join("index/signatures.bin"), [4; 64]).unwrap();
        fs::write(root.join("index/blockhash_registry.bin"), [8; 32]).unwrap();
        let poh = WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: 42,
            entries: vec![CompactPohEntry {
                num_hashes: 7,
                hash: [8; 32],
                tx_count: 1,
                signature_count: 1,
            }],
        };
        let mut writer = WincodeLeb128FramedWriter::new(
            File::create(root.join("evidence/available-poh.wincode")).unwrap(),
        );
        writer.write(&poh).unwrap();
        writer.flush().unwrap();
        // The wrapper consumes a trusted normalization receipt; capture parsing is tested by hivezilla.
        for name in [
            "events.wal",
            "CURRENT-GRPC-SEALED.v1.json",
            "CURRENT-GRPC-INVENTORY.v1.json",
            "finalized-coverage.json",
            "rpc-get-blocks.json",
            "validator-rpc-before.json",
            "validator-rpc-after.json",
        ] {
            fs::write(
                root.join("evidence").join(name),
                b"fixture source evidence\n",
            )
            .unwrap();
        }
        let mut r = receipt();
        let sealed_files = [
            "events.wal",
            "validator-rpc-before.json",
            "validator-rpc-after.json",
        ]
        .iter()
        .map(|name| hash_file(&root.join("evidence").join(name), name).unwrap())
        .collect::<Vec<_>>();
        let seal_bytes = serde_json::to_vec(&json!({"state":"SEALED", "source_profile":EXECUTION_SOURCE_PROFILE, "requested_commitment":"finalized", "validator_identity":{"genesis_hash":r.genesis_hash,"node_identity":r.node_identity}, "files":sealed_files})).unwrap();
        fs::write(
            root.join("evidence/CURRENT-GRPC-SEALED.v1.json"),
            &seal_bytes,
        )
        .unwrap();
        r.source_receipt_sha256 = hash_bytes(&seal_bytes);
        let rpc_bytes = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":[42]})).unwrap();
        fs::write(root.join("evidence/rpc-get-blocks.json"), &rpc_bytes).unwrap();
        r.rpc_response_sha256 = hash_bytes(&rpc_bytes);
        let coverage = FinalizedRpcCoverageV1 {
            schema: "hivezilla-finalized-rpc-coverage-v1".into(),
            genesis_hash: r.genesis_hash.clone(),
            node_identity: r.node_identity.clone(),
            commitment: "finalized".into(),
            start_slot: r.start_slot,
            end_slot_exclusive: r.end_slot_exclusive,
            finalized_slot: 42,
            produced_slots: vec![42],
            rpc_response_sha256: r.rpc_response_sha256.clone(),
            source_receipt_sha256: r.source_receipt_sha256.clone(),
        };
        let coverage_bytes = serde_json::to_vec(&coverage).unwrap();
        fs::write(
            root.join("evidence/finalized-coverage.json"),
            &coverage_bytes,
        )
        .unwrap();
        r.coverage_receipt_sha256 = hash_bytes(&coverage_bytes);
        for dir in ["blocks", "index", "evidence"] {
            for path in fs::read_dir(root.join(dir)).unwrap() {
                let path = path.unwrap().path();
                let relative = format!("{dir}/{}", path.file_name().unwrap().to_str().unwrap());
                r.files.push(hash_file(&path, &relative).unwrap());
            }
        }
        fs::write(
            root.join(NORMALIZED_EXECUTION_FILE),
            serde_json::to_vec_pretty(&r).unwrap(),
        )
        .unwrap();
        r
    }
    #[test]
    fn canary_receipt_rejects_canonical_or_unsupported_proof_claims() {
        assert!(validate_receipt(&receipt()).is_ok());
        let mut r = receipt();
        r.canonical = true;
        assert!(validate_receipt(&r).is_err());
        let mut r = receipt();
        r.finalized = false;
        assert!(validate_receipt(&r).is_err());
        let mut r = receipt();
        r.shred_evidence = "verified".into();
        assert!(validate_receipt(&r).is_err());
        let mut r = receipt();
        r.poh_hashes_recomputed = true;
        assert!(validate_receipt(&r).is_err());
        let mut r = receipt();
        r.transactions += 1;
        assert!(validate_receipt(&r).is_err());
        let mut r = receipt();
        r.blocks[0].parent_slot = 41;
        assert!(validate_receipt(&r).is_err());
    }
    #[test]
    fn canary_file_set_rejects_traversal_duplicates_and_changed_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("normalized");
        let r = fixture(&root);
        verify_files(&root, &r.files).unwrap();
        let mut records = r.files.clone();
        records[0].path = "../outside".into();
        assert!(verify_files(&root, &records).is_err());
        let mut records = r.files.clone();
        records.push(records[0].clone());
        assert!(verify_files(&root, &records).is_err());
        fs::write(root.join("evidence/events.wal"), b"changed").unwrap();
        assert!(verify_files(&root, &r.files).is_err());
    }
    #[test]
    fn available_poh_rejects_wrong_counts_and_trailing_records() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("normalized");
        let mut r = fixture(&root);
        validate_available_poh(&root, &r).unwrap();
        r.blocks[0].entries = 2;
        assert!(validate_available_poh(&root, &r).is_err());
        r.blocks[0].entries = 1;
        let path = root.join("evidence/available-poh.wincode");
        let bytes = fs::read(&path).unwrap();
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&bytes).unwrap();
        drop(file);
        assert!(validate_available_poh(&root, &r).is_err());
    }
    #[test]
    fn declared_provenance_must_match_retained_receipts_and_bank_identity() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("normalized");
        let r = fixture(&root);
        validate_provenance(&root, &r).unwrap();
        let mut wrong = r.clone();
        wrong.source_receipt_sha256 = "f".repeat(64);
        assert!(validate_provenance(&root, &wrong).is_err());
        let mut wrong = r.clone();
        wrong.node_identity = bs58::encode([9; 32]).into_string();
        assert!(validate_provenance(&root, &wrong).is_err());
        let mut wrong = r;
        wrong.blocks[0].slot = 41;
        assert!(validate_provenance(&root, &wrong).is_err());
    }
    #[test]
    fn compact_canary_writes_verified_v1_and_retains_explicit_source_scope() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("normalized");
        let r = fixture(&input);
        let output = temp.path().join("canary");
        build(&input, &output, 1).unwrap();
        verify_archive(&output, &r).unwrap();
        let marker: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join(EXECUTION_CANARY_FILE)).unwrap()).unwrap();
        assert_eq!(marker["canonical"], false);
        assert_eq!(marker["publication_ready"], false);
        assert_eq!(marker["archive_verified"], true);
        assert_eq!(marker["shred_evidence"], "absent");
        assert_eq!(
            marker["source_receipt_sha256"],
            hash_bytes(&fs::read(input.join(NORMALIZED_EXECUTION_FILE)).unwrap())
        );
        assert_eq!(
            fs::read(output.join("events.wal")).unwrap(),
            fs::read(input.join("evidence/events.wal")).unwrap()
        );
        assert!(!output.join(SHREDDING_FILE).exists());
        assert!(!output.join(POH_FILE).exists());
        assert!(
            fs::metadata(output.join(EXECUTION_CANARY_FILE))
                .unwrap()
                .permissions()
                .readonly()
        );
        let path = output.join(ARCHIVE_V2_BLOCKS_FILE);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        }
        fs::write(&path, b"corrupt frame").unwrap();
        assert!(verify_archive(&output, &r).is_err());
    }
}
