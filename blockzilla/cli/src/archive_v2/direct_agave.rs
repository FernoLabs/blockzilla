//! Compact execution projection from native Agave callbacks, without gRPC receipts.
use super::execution_canary::{
    copy_exact, hash_file, make_readonly, read_bounded, reject_symlink_components, safe_flat_name,
    validate_available_poh_rows, validate_normalized_block_rows,
};
use super::*;
use anyhow::{Context, Result, ensure};
use blockzilla_live_format::execution_canary_v1::{ExecutionBlockV1, ExecutionFileV1};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

const RECEIPT: &str = "DIRECT-AGAVE-NORMALIZED.v1.json";
const MARKER: &str = "DIRECT-AGAVE-COMPACT.v1.json";
const MAX_JSON: u64 = 16 << 20;
const MAX_INPUT_FILE: u64 = 1 << 30;
const MAX_OUTPUT_FILE: u64 = 512 << 20;
const INPUT_PATHS: [&str; 5] = [
    "blocks/live-no-registry-blocks.bin",
    "index/blockhash_registry.bin",
    "index/signatures.bin",
    "evidence/source-events.json",
    "evidence/available-poh.wincode",
];

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Block {
    block_id: u32,
    slot: u64,
    bank_id: u64,
    blockhash: [u8; 32],
    parent_slot: u64,
    parent_blockhash: [u8; 32],
    transactions: u64,
    signatures: u64,
    entries: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: String,
    canonical: bool,
    finalized: bool,
    source_profile: String,
    session_id: String,
    agave_revision: String,
    genesis_hash: String,
    node_identity: String,
    epoch: u64,
    start_slot: u64,
    end_slot_exclusive: u64,
    blocks: Vec<Block>,
    files: Vec<ExecutionFileV1>,
}

fn totals(r: &Receipt) -> Result<(u64, u64, u64)> {
    r.blocks
        .iter()
        .try_fold((0u64, 0u64, 0u64), |(txs, sigs, entries), b| {
            Ok((
                txs.checked_add(b.transactions)
                    .context("transaction count overflow")?,
                sigs.checked_add(b.signatures)
                    .context("signature count overflow")?,
                entries
                    .checked_add(b.entries)
                    .context("entry count overflow")?,
            ))
        })
}

fn validate(r: &Receipt) -> Result<()> {
    ensure!(
        ((r.schema == "hivezilla-direct-agave-normalized-v1" && r.finalized)
            || (r.schema == "hivezilla-direct-agave-processed-normalized-v1"
                && !r.finalized
                && r.blocks.len() == 1))
            && r.source_profile == "agave-bank-callbacks-v1"
            && !r.canonical,
        "not a supported direct Agave normalization"
    );
    ensure!(
        !r.session_id.is_empty() && r.session_id.len() <= 128,
        "invalid direct capture session identity"
    );
    ensure!(
        r.agave_revision == "825efd18292aff6ffcf9daa0f7612f21b3531a72"
            || r.agave_revision == "a203300045b405f86c9c84308b2eae76af69ac3b"
            || option_env!("HIVEZILLA_AGAVE_REVISION")
                .is_some_and(|revision| r.agave_revision == revision),
        "unsupported Agave source revision"
    );
    for value in [&r.genesis_hash, &r.node_identity] {
        ensure!(
            bs58::decode(value).into_vec()?.len() == 32,
            "invalid source identity"
        );
    }
    ensure!(
        (1..=4096).contains(&r.blocks.len()) && r.start_slot < r.end_slot_exclusive,
        "invalid direct compact batch bounds"
    );
    let mut bank_keys = BTreeSet::new();
    for (index, block) in r.blocks.iter().enumerate() {
        ensure!(
            block.block_id as usize == index
                && (r.start_slot..r.end_slot_exclusive).contains(&block.slot)
                && block.parent_slot < block.slot
                && bank_keys.insert((block.slot, block.bank_id)),
            "invalid direct block identity"
        );
        if index > 0 {
            let parent = &r.blocks[index - 1];
            ensure!(
                block.slot > parent.slot
                    && block.parent_slot == parent.slot
                    && block.parent_blockhash == parent.blockhash,
                "direct rooted chain is incomplete"
            );
        }
    }
    ensure!(
        r.blocks[0].slot == r.start_slot
            && r.blocks.last().unwrap().slot.checked_add(1) == Some(r.end_slot_exclusive),
        "direct range is not anchored to its produced blocks"
    );
    totals(r)?;
    Ok(())
}

fn verify_input(root: &Path, r: &Receipt) -> Result<()> {
    ensure!(
        fs::symlink_metadata(root)?.file_type().is_dir(),
        "input root is not a directory"
    );
    let mut names = BTreeSet::new();
    let mut total = 0u64;
    for file in &r.files {
        ensure!(
            INPUT_PATHS.contains(&file.path.as_str()) && names.insert(file.path.as_str()),
            "unexpected or duplicate direct input object"
        );
        reject_symlink_components(root, &file.path)?;
        total = total
            .checked_add(file.bytes)
            .context("direct input size overflow")?;
        ensure!(
            file.bytes <= MAX_INPUT_FILE && total <= 8u64 << 30,
            "direct input exceeds byte bounds"
        );
        let actual = hash_file(&root.join(&file.path), &file.path)?;
        ensure!(
            actual.bytes == file.bytes && actual.sha256 == file.sha256,
            "direct source object identity differs: {}",
            file.path
        );
    }
    ensure!(
        names.len() == INPUT_PATHS.len(),
        "direct source file set incomplete"
    );
    let rows = r
        .blocks
        .iter()
        .map(|b| ExecutionBlockV1 {
            block_id: b.block_id,
            slot: b.slot,
            bank_id: b.bank_id,
            blockhash: b.blockhash,
            parent_slot: b.parent_slot,
            parent_blockhash: b.parent_blockhash,
            transactions: b.transactions,
            signatures: b.signatures,
            entries: b.entries,
            // This adapter only uses the shared geometry validator. Source authority
            // remains the native receipt, not these unused gRPC-specific fields.
            source_sequence: 0,
            source_raw_sha256: String::new(),
        })
        .collect::<Vec<_>>();
    validate_normalized_block_rows(root, &rows)?;
    validate_available_poh_rows(root, &rows)
}

fn compress_exact(input: &Path, output: &Path, expected: &ExecutionFileV1) -> Result<()> {
    let mut source = File::open(input)?;
    let target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    let mut encoder = zstd::stream::write::Encoder::new(target, 1)?;
    encoder.include_checksum(true)?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let size = source.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        bytes = bytes
            .checked_add(size as u64)
            .context("source size overflow")?;
        ensure!(
            bytes <= expected.bytes && bytes <= MAX_INPUT_FILE,
            "source grew during compression"
        );
        digest.update(&buffer[..size]);
        encoder.write_all(&buffer[..size])?;
    }
    let sha256 = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    ensure!(
        bytes == expected.bytes && sha256 == expected.sha256,
        "source changed during compression"
    );
    encoder.finish()?.sync_all()?;
    Ok(())
}

pub(crate) fn build(input: &Path, output: &Path, level: i32) -> Result<()> {
    ensure!(!output.exists(), "direct compact output already exists");
    let receipt_path = input.join(RECEIPT);
    reject_symlink_components(input, RECEIPT)?;
    let bytes = read_bounded(&receipt_path, MAX_JSON)?;
    let receipt: Receipt = serde_json::from_slice(&bytes)?;
    validate(&receipt)?;
    verify_input(input, &receipt)?;
    let parent = output.parent().context("compact output has no parent")?;
    let name = output
        .file_name()
        .context("compact output has no name")?
        .to_string_lossy();
    let stage = parent.join(format!(".{name}.direct-agave-stage"));
    if stage.exists() {
        ensure!(
            fs::symlink_metadata(&stage)?.file_type().is_dir(),
            "interrupted stage is not a directory"
        );
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let preserved = parent.join(format!(
            ".{name}.direct-agave-interrupted-{stamp}-{}",
            std::process::id()
        ));
        fs::rename(&stage, &preserved)?;
        File::open(parent)?.sync_all()?;
    }
    fs::create_dir(&stage).context("create fresh direct compact stage; preserve failed stages")?;
    let built = build_degraded_hot_blocks_inner(
        input,
        &stage,
        level,
        None,
        false,
        LiveRegistrySource::Scan,
    )?;
    let (transactions, signatures, entries) = totals(&receipt)?;
    ensure!(
        built.blocks == receipt.blocks.len() as u64
            && built.transactions == transactions
            && built.signatures == signatures,
        "direct compact writer counts differ from source"
    );
    let first = &receipt.blocks[0];
    write_prev_blockhash_tail(
        &stage,
        &[PreviousBlockhash {
            hash: first.parent_blockhash,
            slot: first.parent_slot,
        }],
    )?;
    let source_record = receipt
        .files
        .iter()
        .find(|f| f.path == "evidence/source-events.json")
        .unwrap();
    compress_exact(
        &input.join(&source_record.path),
        &stage.join("source-events.json.zst"),
        source_record,
    )?;
    let poh_record = receipt
        .files
        .iter()
        .find(|f| f.path == "evidence/available-poh.wincode")
        .unwrap();
    copy_exact(
        &input.join(&poh_record.path),
        &stage.join("available-poh.wincode"),
        poh_record,
    )?;
    let mut source = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(stage.join(RECEIPT))?;
    source.write_all(&bytes)?;
    source.sync_all()?;
    verify_input(input, &receipt)?;
    ensure!(
        read_bounded(&receipt_path, MAX_JSON)? == bytes,
        "direct receipt changed during build"
    );
    ensure!(
        !stage.join(SHREDDING_FILE).exists() && !stage.join(POH_FILE).exists(),
        "execution projection must not claim canonical shred or PoH evidence"
    );
    let mut paths = fs::read_dir(&stage)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let mut files = Vec::new();
    for path in paths {
        let name = path
            .file_name()
            .context("object name missing")?
            .to_str()
            .context("object name is not UTF8")?;
        ensure!(
            safe_flat_name(name) && fs::symlink_metadata(&path)?.file_type().is_file(),
            "unexpected direct compact object"
        );
        let record = hash_file(&path, name)?;
        ensure!(
            record.bytes <= MAX_OUTPUT_FILE,
            "compact object exceeds 512 MiB bound"
        );
        make_readonly(&path)?;
        files.push(record);
    }
    let marker = json!({
        "schema":if receipt.finalized {"hivezilla-direct-agave-compact-v1"} else {"hivezilla-direct-agave-processed-compact-v1"}, "state":"COMPLETE",
        "source_profile":receipt.source_profile, "session_id":receipt.session_id,
        "agave_revision":receipt.agave_revision, "genesis_hash":receipt.genesis_hash,
        "node_identity":receipt.node_identity, "epoch":receipt.epoch,
        "start_slot":receipt.start_slot, "end_slot_exclusive":receipt.end_slot_exclusive,
        "canonical":false, "publication_ready":false, "finalized":receipt.finalized,
        "commitment":if receipt.finalized {"finalized"} else {"processed"},
        "finality_basis":if receipt.finalized {"operator-trusted-agave-rooted-bank-callback"} else {"operator-trusted-agave-processed-bank-callback"},
        "archive_verified":false, "archive_verification":"SKIPPED",
        "blocks":receipt.blocks.len(), "transactions":transactions,
        "signatures":signatures, "entries":entries, "produced_blocks":receipt.blocks,
        "source_receipt_sha256":Sha256::digest(&bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "authoritative_execution":"source-events.json.zst plus native journal session and durable commit",
        "source_events_encoding":"zstd-json-v1",
        "source_events_uncompressed_bytes":source_record.bytes,
        "source_events_uncompressed_sha256":source_record.sha256,
        "shred_evidence":"not included in this execution projection",
        "signatures_cryptographically_verified":false, "poh_hashes_recomputed":false,
        "files":files,
    });
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(stage.join(MARKER))?;
    serde_json::to_writer_pretty(&mut file, &marker)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    make_readonly(&stage.join(MARKER))?;
    File::open(&stage)?.sync_all()?;
    fs::rename(&stage, output)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(root: &Path) -> Receipt {
        let old = super::super::execution_canary::tests::fixture(root);
        fs::write(root.join("evidence/source-events.json"), b"[]\n").unwrap();
        let blocks = old
            .blocks
            .iter()
            .map(|b| Block {
                block_id: b.block_id,
                slot: b.slot,
                bank_id: b.bank_id,
                blockhash: b.blockhash,
                parent_slot: b.parent_slot,
                parent_blockhash: b.parent_blockhash,
                transactions: b.transactions,
                signatures: b.signatures,
                entries: b.entries,
            })
            .collect::<Vec<_>>();
        let receipt = Receipt {
            schema: "hivezilla-direct-agave-normalized-v1".into(),
            canonical: false,
            finalized: true,
            source_profile: "agave-bank-callbacks-v1".into(),
            session_id: "fixture-session".into(),
            agave_revision: "825efd18292aff6ffcf9daa0f7612f21b3531a72".into(),
            genesis_hash: old.genesis_hash,
            node_identity: old.node_identity,
            epoch: old.epoch,
            start_slot: 42,
            end_slot_exclusive: 43,
            blocks,
            files: INPUT_PATHS
                .iter()
                .map(|name| hash_file(&root.join(name), name).unwrap())
                .collect(),
        };
        fs::write(root.join(RECEIPT), serde_json::to_vec(&receipt).unwrap()).unwrap();
        receipt
    }
    #[test]
    fn rejects_incomplete_parent_chain_and_false_source_claims() {
        let temp = tempfile::tempdir().unwrap();
        let mut r = fixture(&temp.path().join("input"));
        validate(&r).unwrap();
        r.blocks.push(Block {
            block_id: 1,
            slot: 44,
            bank_id: 92,
            blockhash: [9; 32],
            parent_slot: 43,
            parent_blockhash: [8; 32],
            transactions: 0,
            signatures: 0,
            entries: 0,
        });
        r.end_slot_exclusive = 45;
        assert!(validate(&r).is_err());
        r.blocks[1].parent_slot = 42;
        validate(&r).unwrap();
        r.canonical = true;
        assert!(validate(&r).is_err());
        r.canonical = false;
        r.source_profile = "validator-finalized-execution-v1".into();
        assert!(validate(&r).is_err());
    }
    #[test]
    fn rejects_changed_source_and_outside_file_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("input");
        let mut r = fixture(&root);
        verify_input(&root, &r).unwrap();
        r.files[0].path = "../source-events.json".into();
        assert!(verify_input(&root, &r).is_err());
        r = fixture(&temp.path().join("input2"));
        fs::write(
            temp.path().join("input2/evidence/source-events.json"),
            b"changed",
        )
        .unwrap();
        assert!(verify_input(&temp.path().join("input2"), &r).is_err());
    }
    #[test]
    fn writes_compact_projection_with_native_receipt_and_no_grpc_authority() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input");
        fixture(&input);
        let output = temp.path().join("compact");
        build(&input, &output, 1).unwrap();
        let marker: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join(MARKER)).unwrap()).unwrap();
        assert_eq!(marker["blocks"], 1);
        assert_eq!(marker["transactions"], 1);
        assert_eq!(marker["archive_verified"], false);
        assert_eq!(marker["canonical"], false);
        assert_eq!(marker["publication_ready"], false);
        assert_eq!(marker["source_events_encoding"], "zstd-json-v1");
        assert_eq!(
            zstd::stream::decode_all(File::open(output.join("source-events.json.zst")).unwrap())
                .unwrap(),
            b"[]\n"
        );
        assert!(!output.join("CURRENT-GRPC-SEALED.v1.json").exists());
        assert!(!output.join(SHREDDING_FILE).exists());
        assert!(build(&input, &output, 1).is_err());
    }

    #[test]
    fn processed_writer_keeps_the_same_compact_codec_and_explicit_commitment() {
        let t = tempfile::tempdir().unwrap();
        let input = t.path().join("input");
        let mut r = fixture(&input);
        r.schema = "hivezilla-direct-agave-processed-normalized-v1".into();
        r.finalized = false;
        fs::write(input.join(RECEIPT), serde_json::to_vec(&r).unwrap()).unwrap();
        let output = t.path().join("processed");
        build(&input, &output, 1).unwrap();
        let marker: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join(MARKER)).unwrap()).unwrap();
        assert_eq!(
            marker["schema"],
            "hivezilla-direct-agave-processed-compact-v1"
        );
        assert_eq!(marker["finalized"], false);
        assert_eq!(marker["transactions"], 1);
        r.finalized = true;
        assert!(validate(&r).is_err());
    }
    #[test]
    fn rejects_changed_source_during_compression() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("source.json");
        fs::write(&input, b"original").unwrap();
        let expected = hash_file(&input, "source.json").unwrap();
        fs::write(&input, b"modified").unwrap();
        assert!(compress_exact(&input, &temp.path().join("source.json.zst"), &expected).is_err());
    }

    #[test]
    fn interrupted_build_preserves_stage_and_rebuilds_from_durable_source() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input");
        fixture(&input);
        let interrupted = temp.path().join(".compact.direct-agave-stage");
        fs::create_dir(&interrupted).unwrap();
        fs::write(
            interrupted.join("partial-output"),
            b"preserve this evidence",
        )
        .unwrap();
        let output = temp.path().join("compact");
        build(&input, &output, 1).unwrap();
        assert!(output.join(MARKER).is_file());
        let preserved = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".compact.direct-agave-interrupted-")
            })
            .unwrap()
            .path();
        assert_eq!(
            fs::read(preserved.join("partial-output")).unwrap(),
            b"preserve this evidence"
        );
        assert!(input.join(RECEIPT).is_file());
    }
}
