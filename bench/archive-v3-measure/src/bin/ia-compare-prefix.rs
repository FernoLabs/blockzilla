//! Read-only semantic comparison of two non-publishable benchmark prefixes.

#![cfg_attr(test, recursion_limit = "256")]

use std::{
    collections::BTreeMap,
    fs::{self, File},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v3::{
    ArchiveId, FILE_HEADER_LEN, FileHeader,
    catalog::blocks::{self as catalog_blocks, BlockRow, FactLocator, PageSpan},
    dictionary::{account_flags, blockhashes, pubkeys},
    ledger::transactions::{self as transactions, EffectKind},
    runtime::{
        balances, block_rewards, inner_instructions, logs, outcomes, rewards, token_balances,
    },
    sidecars::{framing, poh, shredding},
};
use blockzilla_archive_v3_convert::{
    canonical_reader::{CanonicalReader, DEFAULT_MAX_BLOCK_DECODED_BYTES, validate_all_effects},
    container::{decode_zstd_exact, validate_open_file},
};
use serde::{Deserialize, Serialize};

const DEFAULT_EXPECTED_BLOCKS: u64 = 10_000;
const COMPARE_BUFFER_BYTES: usize = 8 << 20;
const MAX_REPORT_BYTES: u64 = 1 << 20;
const BENCHMARK_REPORT: &str = "benchmark-report.json";
const BENCHMARK_STATUS: &str = "benchmark-prefix-not-publishable";

#[derive(Debug)]
struct Args {
    validated_a: PathBuf,
    candidate_b: PathBuf,
    expected_blocks: u64,
    max_decoded_bytes: usize,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ReportSemantics {
    source_published: bool,
    source_generation_digest: Option<String>,
    epoch: u64,
    slots_per_epoch: u64,
    source_profile: String,
    metadata_source_profile: String,
    output_status: String,
    fixture_previous_blockhash: Option<String>,
    fixture_previous_slot: Option<u64>,
    blocks: u64,
    transactions: u64,
    top_level_instructions: u64,
    inner_instructions: u64,
    account_references: u64,
    raw_fallback_transactions: u64,
    loaded_addresses_unavailable: u64,
    cpi_not_recorded: u64,
    raw_account_keys: u64,
    nonce_blockhashes: u64,
    instruction_data_variants: BTreeMap<String, u64>,
    instructions_bytes_retained: u64,
    instructions_bytes_rederived: u64,
    retained_payload_bytes: u64,
    instruction_data_bytes: u64,
    inner_instruction_data_bytes: u64,
    token_balances_paired: u64,
    token_balances_total: u64,
    signatures: u64,
    blockhash_dictionary_records: u64,
    poh_source_schema: String,
    poh_entries: u64,
    poh_signature_count_recovered_blocks: u64,
    poh_signature_count_legacy_unknown_blocks: u64,
    shredding_boundaries: u64,
    shredding_recorded_empty_blocks: u64,
    nonce_hashes_interned: u64,
    pubkey_dictionary_records: u64,
    block_rewards_stored: u64,
    program_accounts: u64,
    signer_accounts: u64,
    unused_accounts: u64,
    source_block_bytes: u64,
    source_decoded_block_bytes: u64,
    source_first_slot: u64,
    source_last_slot: u64,
    benchmark_prefix_blocks: Option<usize>,
    source_total_blocks: usize,
}

#[derive(Debug, Deserialize)]
struct BenchmarkReport {
    archive_id: String,
    #[serde(flatten)]
    semantics: ReportSemantics,
}

#[derive(Debug, PartialEq, Eq)]
struct CatalogFacts {
    slot: u64,
    parent_slot: u64,
    first_transaction: u64,
    transaction_count: u32,
    blockhash: transactions::HashRef,
    previous_blockhash: transactions::HashRef,
    block_time: Option<i64>,
    block_height: Option<u64>,
    first_signature: u64,
    transaction_decoded_len: u32,
    block_rewards: FactLocator,
    poh: FactLocator,
    shredding: FactLocator,
}

impl From<BlockRow> for CatalogFacts {
    fn from(row: BlockRow) -> Self {
        Self {
            slot: row.slot,
            parent_slot: row.parent_slot,
            first_transaction: row.first_transaction,
            transaction_count: row.transaction_count,
            blockhash: row.blockhash,
            previous_blockhash: row.previous_blockhash,
            block_time: row.block_time,
            block_height: row.block_height,
            first_signature: row.first_signature,
            transaction_decoded_len: row.transactions.decoded_len,
            block_rewards: row.block_rewards,
            poh: row.poh,
            shredding: row.shredding,
        }
    }
}

struct OpenObject {
    file: File,
    header: FileHeader,
    file_len: u64,
}

#[derive(Debug, Serialize)]
struct PlaneReceipt {
    path: &'static str,
    payload_bytes: u64,
}

#[derive(Debug, Serialize)]
struct OptionalPlaneReceipt {
    path: &'static str,
    comparison: &'static str,
    record_count: Option<u64>,
    decoded_bytes: Option<u64>,
    payload_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct EffectReceipt {
    path: &'static str,
    records: u64,
    decoded_bytes: u64,
}

#[derive(Debug, Serialize)]
struct AcceptanceReceipt {
    status: &'static str,
    comparison: &'static str,
    validated_a: String,
    candidate_b: String,
    archive_id_a: String,
    archive_id_b: String,
    blocks: u64,
    transactions: u64,
    signatures_declared: u64,
    first_slot: u64,
    last_slot: u64,
    transaction_span_differences: u64,
    split_frame_pages: u64,
    split_frame_witness_ordinal: u64,
    effect_states: u64,
    effects: Vec<EffectReceipt>,
    block_reward_records: u64,
    poh_frames: u64,
    poh_entries: u64,
    shredding_frames: u64,
    shredding_boundaries: u64,
    unchanged_planes: Vec<PlaneReceipt>,
    optional_dictionary_planes: Vec<OptionalPlaneReceipt>,
    max_decoded_bytes: usize,
    elapsed_ms: u64,
}

fn usage() -> &'static str {
    "usage: ia-compare-prefix <validated-a-dir> <candidate-b-dir> \
     [--expected-blocks N] [--max-decoded-mib N]"
}

fn parse_args() -> Result<Args> {
    let mut positional = Vec::new();
    let mut expected_blocks = DEFAULT_EXPECTED_BLOCKS;
    let mut max_decoded_bytes = DEFAULT_MAX_BLOCK_DECODED_BYTES;
    let mut args = std::env::args_os().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--expected-blocks" {
            expected_blocks = args
                .next()
                .context("--expected-blocks requires a value")?
                .to_str()
                .context("--expected-blocks is not valid UTF-8")?
                .parse()
                .context("--expected-blocks must be an unsigned integer")?;
        } else if argument == "--max-decoded-mib" {
            let mib: usize = args
                .next()
                .context("--max-decoded-mib requires a value")?
                .to_str()
                .context("--max-decoded-mib is not valid UTF-8")?
                .parse()
                .context("--max-decoded-mib must be an unsigned integer")?;
            max_decoded_bytes = mib
                .checked_mul(1 << 20)
                .context("--max-decoded-mib overflows usize")?;
        } else if argument.to_string_lossy().starts_with('-') {
            bail!(
                "unknown option {}; {usage}",
                argument.to_string_lossy(),
                usage = usage()
            );
        } else {
            positional.push(PathBuf::from(argument));
        }
    }
    ensure!(positional.len() == 2, usage());
    ensure!(
        expected_blocks != 0,
        "--expected-blocks must be greater than zero"
    );
    ensure!(
        max_decoded_bytes != 0,
        "--max-decoded-mib must be greater than zero"
    );
    Ok(Args {
        validated_a: positional.remove(0),
        candidate_b: positional.remove(0),
        expected_blocks,
        max_decoded_bytes,
    })
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("inspect {label}"))?;
    ensure!(
        metadata.file_type().is_file(),
        "{label} is not a regular file"
    );
    Ok(())
}

fn open_object(root: &Path, path: &'static str, archive_id: ArchiveId) -> Result<OpenObject> {
    let full_path = root.join(path);
    require_regular_file(&full_path, path)?;
    let file = File::open(&full_path).with_context(|| format!("open {}", full_path.display()))?;
    let header = validate_open_file(&file, path, archive_id)?;
    let file_len = file.metadata()?.len();
    Ok(OpenObject {
        file,
        header,
        file_len,
    })
}

fn read_report(root: &Path) -> Result<BenchmarkReport> {
    let path = root.join(BENCHMARK_REPORT);
    require_regular_file(&path, BENCHMARK_REPORT)?;
    let len = path.metadata()?.len();
    ensure!(
        len <= MAX_REPORT_BYTES,
        "{} has {len} bytes, above the {MAX_REPORT_BYTES}-byte guard",
        path.display()
    );
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))
}

fn ensure_same_header_semantics(
    path: &str,
    a: FileHeader,
    b: FileHeader,
    compare_payload_bytes: bool,
) -> Result<()> {
    ensure!(
        a.format_major == b.format_major,
        "{path} format major differs"
    );
    ensure!(a.schema == b.schema, "{path} schema differs");
    ensure!(a.role == b.role, "{path} role differs");
    ensure!(a.flags == b.flags, "{path} flags differ");
    ensure!(
        a.record_count == b.record_count,
        "{path} record count differs"
    );
    ensure!(
        a.decoded_bytes == b.decoded_bytes,
        "{path} decoded byte count differs"
    );
    if compare_payload_bytes {
        ensure!(
            a.payload_bytes == b.payload_bytes,
            "{path} payload byte count differs"
        );
    }
    Ok(())
}

fn compare_payloads(path: &'static str, a: &OpenObject, b: &OpenObject) -> Result<PlaneReceipt> {
    ensure_same_header_semantics(path, a.header, b.header, true)?;
    let mut a_bytes = vec![0_u8; COMPARE_BUFFER_BYTES];
    let mut b_bytes = vec![0_u8; COMPARE_BUFFER_BYTES];
    let mut compared = 0_u64;
    while compared < a.header.payload_bytes {
        let count =
            usize::try_from((a.header.payload_bytes - compared).min(COMPARE_BUFFER_BYTES as u64))
                .expect("comparison chunk is bounded by the buffer size");
        let offset = (FILE_HEADER_LEN as u64)
            .checked_add(compared)
            .context("payload comparison offset overflow")?;
        a.file
            .read_exact_at(&mut a_bytes[..count], offset)
            .with_context(|| format!("read validated A {path} at payload byte {compared}"))?;
        b.file
            .read_exact_at(&mut b_bytes[..count], offset)
            .with_context(|| format!("read candidate B {path} at payload byte {compared}"))?;
        if a_bytes[..count] != b_bytes[..count] {
            let within = a_bytes[..count]
                .iter()
                .zip(&b_bytes[..count])
                .position(|(a, b)| a != b)
                .expect("different slices have a different byte");
            bail!(
                "unchanged plane {path} payload differs at byte {}",
                compared + within as u64
            );
        }
        compared += count as u64;
    }
    Ok(PlaneReceipt {
        path,
        payload_bytes: compared,
    })
}

fn regular_file_presence(root: &Path, path: &'static str) -> Result<bool> {
    let full_path = root.join(path);
    match fs::symlink_metadata(&full_path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file(),
                "optional plane {path} is present but is not a regular file"
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", full_path.display())),
    }
}

fn compare_optional_dictionary_plane(
    canonical_a: &Path,
    archive_id_a: ArchiveId,
    canonical_b: &Path,
    archive_id_b: ArchiveId,
    path: &'static str,
    record_len: u64,
) -> Result<OptionalPlaneReceipt> {
    let present_a = regular_file_presence(canonical_a, path)?;
    let present_b = regular_file_presence(canonical_b, path)?;
    ensure!(
        present_a == present_b,
        "optional dictionary plane {path} presence differs between A and B; exact comparison is not possible"
    );
    if !present_a {
        return Ok(OptionalPlaneReceipt {
            path,
            comparison: "absent-in-both-not-compared",
            record_count: None,
            decoded_bytes: None,
            payload_bytes: None,
        });
    }

    let object_a = open_object(canonical_a, path, archive_id_a)?;
    let object_b = open_object(canonical_b, path, archive_id_b)?;
    for (label, object) in [("validated A", &object_a), ("candidate B", &object_b)] {
        let expected_payload = object
            .header
            .record_count
            .checked_mul(record_len)
            .context("dictionary payload length overflow")?;
        ensure!(
            object.header.payload_bytes == expected_payload,
            "{label} {path} payload byte count does not match its record count"
        );
        ensure!(
            object.header.decoded_bytes == expected_payload,
            "{label} {path} decoded byte count does not match its record count"
        );
    }
    let receipt = compare_payloads(path, &object_a, &object_b)?;
    Ok(OptionalPlaneReceipt {
        path,
        comparison: "exact-header-and-payload-parity",
        record_count: Some(object_a.header.record_count),
        decoded_bytes: Some(object_a.header.decoded_bytes),
        payload_bytes: Some(receipt.payload_bytes),
    })
}

fn read_span(
    object: &OpenObject,
    span: PageSpan,
    max_decoded_bytes: usize,
    label: &str,
) -> Result<Vec<u8>> {
    ensure!(
        span.offset >= FILE_HEADER_LEN as u64,
        "{label} points into the common header"
    );
    ensure!(
        span.stored_len != 0 && span.decoded_len != 0,
        "{label} has an empty span"
    );
    let stored_len = span.stored_len as usize;
    let decoded_len = span.decoded_len as usize;
    ensure!(
        stored_len <= max_decoded_bytes && decoded_len <= max_decoded_bytes,
        "{label} is above the {max_decoded_bytes}-byte guard"
    );
    let end = span
        .offset
        .checked_add(u64::from(span.stored_len))
        .context("page extent overflow")?;
    ensure!(end <= object.file_len, "{label} ends outside its object");
    let mut stored = vec![0_u8; stored_len];
    object
        .file
        .read_exact_at(&mut stored, span.offset)
        .with_context(|| format!("read {label}"))?;
    if span.is_compressed() {
        decode_zstd_exact(&stored, decoded_len, label)
    } else {
        ensure!(stored_len == decoded_len, "raw {label} length differs");
        Ok(stored)
    }
}

fn read_stored_span(object: &OpenObject, span: PageSpan, label: &str) -> Result<Vec<u8>> {
    let end = span
        .offset
        .checked_add(u64::from(span.stored_len))
        .context("stored page extent overflow")?;
    ensure!(
        span.offset >= FILE_HEADER_LEN as u64 && end <= object.file_len,
        "{label} is outside its object"
    );
    let mut bytes = vec![0_u8; span.stored_len as usize];
    object
        .file
        .read_exact_at(&mut bytes, span.offset)
        .with_context(|| format!("read {label}"))?;
    Ok(bytes)
}

fn zstd_frames<'a>(mut bytes: &'a [u8], label: &str) -> Result<Vec<&'a [u8]>> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let len = zstd::zstd_safe::find_frame_compressed_size(bytes).map_err(|code| {
            anyhow::anyhow!(
                "{label} has an invalid zstd frame: {}",
                zstd::zstd_safe::get_error_name(code)
            )
        })?;
        ensure!(
            len != 0 && len <= bytes.len(),
            "{label} has an invalid frame extent"
        );
        frames.push(&bytes[..len]);
        bytes = &bytes[len..];
    }
    Ok(frames)
}

fn zstd_frame_count(bytes: &[u8], label: &str) -> Result<usize> {
    Ok(zstd_frames(bytes, label)?.len())
}

fn validate_split_transaction_page(
    stored: &[u8],
    expected_prefix: &[u8],
    expected_rows: &[u8],
    label: &str,
) -> Result<()> {
    let frames = zstd_frames(stored, label)?;
    ensure!(
        frames.len() == 2,
        "{label} has {} zstd frames, expected exactly two",
        frames.len()
    );
    let prefix = decode_zstd_exact(frames[0], expected_prefix.len(), &format!("{label} prefix"))?;
    ensure!(
        prefix == expected_prefix,
        "{label} first frame is not the exact transaction-block prefix"
    );
    let rows = decode_zstd_exact(
        frames[1],
        expected_rows.len(),
        &format!("{label} row arena"),
    )?;
    ensure!(
        rows == expected_rows,
        "{label} second frame is not the exact transaction-row arena"
    );
    Ok(())
}

fn effect_path(kind: EffectKind) -> &'static str {
    match kind {
        EffectKind::InnerInstructions => inner_instructions::PATH,
        EffectKind::Outcome => outcomes::PATH,
        EffectKind::Balances => balances::PATH,
        EffectKind::TokenBalances => token_balances::PATH,
        EffectKind::Logs => logs::PATH,
        EffectKind::Rewards => rewards::PATH,
    }
}

fn validate_block_rewards(
    object: &OpenObject,
    rows: &[BlockRow],
    max_decoded_bytes: usize,
) -> Result<u64> {
    let mut next_offset = FILE_HEADER_LEN as u64;
    let mut records = 0_u64;
    let mut decoded_bytes = 0_u64;
    for (ordinal, row) in rows.iter().enumerate() {
        match row.block_rewards {
            FactLocator::Unavailable => {}
            FactLocator::Source(span) => {
                ensure!(
                    span.offset == next_offset,
                    "block-reward page for ordinal {ordinal} starts at {}, expected {next_offset}",
                    span.offset
                );
                let bytes = read_span(
                    object,
                    span,
                    max_decoded_bytes,
                    &format!("block-reward page for ordinal {ordinal}"),
                )?;
                block_rewards::decode_record(&bytes)
                    .with_context(|| format!("decode block rewards for ordinal {ordinal}"))?;
                next_offset = next_offset
                    .checked_add(u64::from(span.stored_len))
                    .context("block-reward extent overflow")?;
                decoded_bytes = decoded_bytes
                    .checked_add(u64::from(span.decoded_len))
                    .context("block-reward decoded-byte count overflow")?;
                records = records
                    .checked_add(1)
                    .context("block-reward count overflow")?;
            }
            FactLocator::Backfilled(_) => {
                bail!("benchmark block-reward ordinal {ordinal} is unexpectedly backfilled")
            }
        }
    }
    ensure!(
        next_offset == object.file_len,
        "block-reward pages do not cover their object exactly"
    );
    ensure!(
        object.header.record_count == records,
        "block-reward header count differs"
    );
    ensure!(
        object.header.decoded_bytes == decoded_bytes,
        "block-reward decoded-byte total differs"
    );
    Ok(records)
}

fn validate_poh(object: &OpenObject, rows: &[BlockRow], block_signatures: &[u64]) -> Result<u64> {
    ensure!(
        object.header.record_count == rows.len() as u64,
        "PoH header block count differs"
    );
    ensure!(
        object.header.payload_bytes == object.header.decoded_bytes,
        "PoH must remain raw"
    );
    let mut preamble = [0_u8; poh::PREAMBLE_LEN];
    object
        .file
        .read_exact_at(&mut preamble, FILE_HEADER_LEN as u64)
        .context("read PoH preamble")?;
    let profile = poh::PohPreamble::decode(&preamble)?.profile;
    let mut next_offset = (FILE_HEADER_LEN + poh::PREAMBLE_LEN) as u64;
    let mut entries = 0_u64;
    for (ordinal, (row, expected_signatures)) in rows.iter().zip(block_signatures).enumerate() {
        let span = match row.poh {
            FactLocator::Source(span) => span,
            FactLocator::Unavailable => bail!("catalog ordinal {ordinal} has no PoH frame"),
            FactLocator::Backfilled(_) => bail!("catalog ordinal {ordinal} has backfilled PoH"),
        };
        ensure!(
            span.offset == next_offset,
            "PoH frame {ordinal} is not contiguous"
        );
        ensure!(
            span.stored_len == span.decoded_len,
            "PoH frame {ordinal} is not raw"
        );
        ensure!(
            span.stored_len as usize <= framing::MAX_FRAME_BYTES + framing::MAX_PREFIX_BYTES,
            "PoH frame {ordinal} is above its format guard"
        );
        let frame = read_stored_span(object, span, &format!("PoH frame {ordinal}"))?;
        let decoded = poh::decode_frame(profile, &frame)
            .with_context(|| format!("decode PoH frame {ordinal}"))?;
        let (block_id, slot) = decoded.identity();
        ensure!(
            u64::from(block_id) == ordinal as u64 && slot == row.slot,
            "PoH frame {ordinal} identity differs from the catalog"
        );
        let (transactions, signatures) = match &decoded {
            poh::DecodedPohFrame::Current(record) => (
                record
                    .entries
                    .iter()
                    .map(|entry| u64::from(entry.transaction_count))
                    .sum::<u64>(),
                Some(
                    record
                        .entries
                        .iter()
                        .map(|entry| u64::from(entry.signature_count))
                        .sum::<u64>(),
                ),
            ),
            poh::DecodedPohFrame::LegacyNoSignatureCount(record) => (
                record
                    .entries
                    .iter()
                    .map(|entry| u64::from(entry.transaction_count))
                    .sum::<u64>(),
                None,
            ),
        };
        ensure!(
            decoded.entry_count() != 0 && decoded.final_hash().is_some(),
            "PoH frame {ordinal} has no final entry"
        );
        ensure!(
            transactions == u64::from(row.transaction_count),
            "PoH frame {ordinal} transaction count differs"
        );
        if let Some(signatures) = signatures {
            ensure!(
                signatures == *expected_signatures || signatures == 0,
                "PoH frame {ordinal} signature count is neither exact nor legacy-unknown zero"
            );
        }
        entries = entries
            .checked_add(decoded.entry_count() as u64)
            .context("PoH entry count overflow")?;
        next_offset = next_offset
            .checked_add(u64::from(span.stored_len))
            .context("PoH extent overflow")?;
    }
    ensure!(
        next_offset == object.file_len,
        "PoH frames do not cover their object exactly"
    );
    Ok(entries)
}

fn validate_shredding(object: &OpenObject, rows: &[BlockRow]) -> Result<u64> {
    ensure!(
        object.header.record_count == rows.len() as u64,
        "shredding header block count differs"
    );
    ensure!(
        object.header.payload_bytes == object.header.decoded_bytes,
        "shredding must remain raw"
    );
    let mut preamble = [0_u8; shredding::PREAMBLE_LEN];
    object
        .file
        .read_exact_at(&mut preamble, FILE_HEADER_LEN as u64)
        .context("read shredding preamble")?;
    let profile = shredding::ShreddingPreamble::decode(&preamble)?.profile;
    let mut next_offset = (FILE_HEADER_LEN + shredding::PREAMBLE_LEN) as u64;
    let mut boundaries = 0_u64;
    for (ordinal, row) in rows.iter().enumerate() {
        let span = match row.shredding {
            FactLocator::Source(span) => span,
            FactLocator::Unavailable => bail!("catalog ordinal {ordinal} has no shredding frame"),
            FactLocator::Backfilled(_) => {
                bail!("catalog ordinal {ordinal} has backfilled shredding")
            }
        };
        ensure!(
            span.offset == next_offset,
            "shredding frame {ordinal} is not contiguous"
        );
        ensure!(
            span.stored_len == span.decoded_len,
            "shredding frame {ordinal} is not raw"
        );
        ensure!(
            span.stored_len as usize <= framing::MAX_FRAME_BYTES + framing::MAX_PREFIX_BYTES,
            "shredding frame {ordinal} is above its format guard"
        );
        let frame = read_stored_span(object, span, &format!("shredding frame {ordinal}"))?;
        let decoded = shredding::decode_frame(profile, &frame)
            .with_context(|| format!("decode shredding frame {ordinal}"))?;
        ensure!(
            u64::from(decoded.block_id) == ordinal as u64 && decoded.slot == row.slot,
            "shredding frame {ordinal} identity differs from the catalog"
        );
        ensure!(
            !decoded.boundaries.is_empty(),
            "shredding frame {ordinal} is recorded empty"
        );
        boundaries = boundaries
            .checked_add(decoded.boundaries.len() as u64)
            .context("shredding boundary count overflow")?;
        next_offset = next_offset
            .checked_add(u64::from(span.stored_len))
            .context("shredding extent overflow")?;
    }
    ensure!(
        next_offset == object.file_len,
        "shredding frames do not cover their object exactly"
    );
    Ok(boundaries)
}

fn validate_report(
    report: &BenchmarkReport,
    archive_id: ArchiveId,
    expected_blocks: u64,
) -> Result<()> {
    let expected_blocks_usize =
        usize::try_from(expected_blocks).context("expected block count does not fit in memory")?;
    ensure!(
        report.archive_id == archive_id.to_hex(),
        "benchmark report archive ID differs from its catalog"
    );
    ensure!(
        report.semantics.output_status == BENCHMARK_STATUS,
        "output is not a benchmark prefix"
    );
    ensure!(
        report.semantics.blocks == expected_blocks,
        "benchmark report block count differs"
    );
    ensure!(
        report.semantics.benchmark_prefix_blocks == Some(expected_blocks_usize),
        "benchmark report prefix binding differs"
    );
    ensure!(
        report.semantics.source_total_blocks > expected_blocks_usize,
        "benchmark report does not describe a strict generation prefix"
    );
    Ok(())
}

fn compare_prefix(args: &Args) -> Result<AcceptanceReceipt> {
    let started = Instant::now();
    for (root, label) in [
        (&args.validated_a, "validated A root"),
        (&args.candidate_b, "candidate B root"),
    ] {
        let metadata = fs::symlink_metadata(root).with_context(|| format!("inspect {label}"))?;
        ensure!(
            metadata.file_type().is_dir(),
            "{label} is not a real directory"
        );
    }
    let canonical_a =
        fs::canonicalize(&args.validated_a).context("canonicalize validated A root")?;
    let canonical_b =
        fs::canonicalize(&args.candidate_b).context("canonicalize candidate B root")?;
    ensure!(
        canonical_a != canonical_b,
        "validated A and candidate B are the same directory"
    );

    let report_a = read_report(&canonical_a)?;
    let report_b = read_report(&canonical_b)?;
    ensure!(
        report_a.semantics == report_b.semantics,
        "benchmark report semantics differ between A and B"
    );

    let reader_a = CanonicalReader::open(&canonical_a, args.max_decoded_bytes)
        .context("open validated A canonical reader")?;
    let reader_b = CanonicalReader::open(&canonical_b, args.max_decoded_bytes)
        .context("open candidate B canonical reader")?;
    ensure!(
        reader_a.block_count() == args.expected_blocks,
        "validated A block count differs"
    );
    ensure!(
        reader_b.block_count() == args.expected_blocks,
        "candidate B block count differs"
    );
    validate_report(&report_a, reader_a.archive_id(), args.expected_blocks)?;
    validate_report(&report_b, reader_b.archive_id(), args.expected_blocks)?;

    let catalog_a = open_object(&canonical_a, catalog_blocks::PATH, reader_a.archive_id())?;
    let catalog_b = open_object(&canonical_b, catalog_blocks::PATH, reader_b.archive_id())?;
    ensure_same_header_semantics(
        catalog_blocks::PATH,
        catalog_a.header,
        catalog_b.header,
        true,
    )?;
    let transactions_a = open_object(&canonical_a, transactions::PATH, reader_a.archive_id())?;
    let transactions_b = open_object(&canonical_b, transactions::PATH, reader_b.archive_id())?;
    ensure_same_header_semantics(
        transactions::PATH,
        transactions_a.header,
        transactions_b.header,
        false,
    )?;

    let mut next_transaction_a = FILE_HEADER_LEN as u64;
    let mut next_transaction_b = FILE_HEADER_LEN as u64;
    let mut decoded_transaction_bytes = 0_u64;
    let mut expected_transaction = 0_u64;
    let mut expected_signature = 0_u64;
    let mut transaction_span_differences = 0_u64;
    let mut split_frame_pages = 0_u64;
    let mut split_frame_witness = None;
    let mut effect_next_offsets = [FILE_HEADER_LEN as u64; transactions::EFFECT_KIND_COUNT];
    let mut effect_records = [0_u64; transactions::EFFECT_KIND_COUNT];
    let expected_block_capacity = usize::try_from(args.expected_blocks)
        .context("expected block count does not fit in memory")?;
    let mut rows = Vec::with_capacity(expected_block_capacity);
    let mut block_signatures = Vec::with_capacity(expected_block_capacity);

    for ordinal in 0..args.expected_blocks {
        let a = reader_a
            .read_block(ordinal)
            .with_context(|| format!("read validated A block {ordinal}"))?;
        let b = reader_b
            .read_block(ordinal)
            .with_context(|| format!("read candidate B block {ordinal}"))?;
        ensure!(
            CatalogFacts::from(a.catalog) == CatalogFacts::from(b.catalog),
            "catalog facts differ at ordinal {ordinal}, slots {} and {}",
            a.catalog.slot,
            b.catalog.slot
        );
        ensure!(
            a.catalog.first_transaction == expected_transaction,
            "catalog transaction range is not contiguous at ordinal {ordinal}"
        );
        ensure!(
            a.catalog.first_signature == expected_signature,
            "catalog signature range is not contiguous at ordinal {ordinal}"
        );
        ensure!(
            a.catalog.transactions.offset == next_transaction_a,
            "validated A transaction pages are not contiguous at ordinal {ordinal}"
        );
        ensure!(
            b.catalog.transactions.offset == next_transaction_b,
            "candidate B transaction pages are not contiguous at ordinal {ordinal}"
        );
        if a.catalog.transactions.offset != b.catalog.transactions.offset
            || a.catalog.transactions.stored_len != b.catalog.transactions.stored_len
        {
            transaction_span_differences += 1;
        }
        if b.catalog.transactions.is_compressed() {
            let b_stored = read_stored_span(
                &transactions_b,
                b.catalog.transactions,
                &format!("candidate B transaction page {ordinal}"),
            )?;
            let expected_prefix = transactions::encode_block_prefix(
                &b.index.header(),
                b.index.transaction_rows.len(),
            )
            .with_context(|| {
                format!("encode candidate B transaction prefix at ordinal {ordinal}")
            })?;
            validate_split_transaction_page(
                &b_stored,
                &expected_prefix,
                &b.index.transaction_rows,
                &format!("candidate B transaction page {ordinal}"),
            )?;
            split_frame_pages = split_frame_pages
                .checked_add(1)
                .context("split transaction-page count overflow")?;
            if split_frame_witness.is_none() && a.catalog.transactions.is_compressed() {
                let a_stored = read_stored_span(
                    &transactions_a,
                    a.catalog.transactions,
                    "validated A transaction page witness",
                )?;
                if zstd_frame_count(&a_stored, "validated A transaction page witness")? == 1 {
                    split_frame_witness = Some(ordinal);
                }
            }
        }
        ensure!(
            a.index.effect_states == b.index.effect_states,
            "effect states differ at ordinal {ordinal}, slot {}",
            a.catalog.slot
        );
        ensure!(
            a.index.row_restarts == b.index.row_restarts,
            "transaction row restarts differ at ordinal {ordinal}, slot {}",
            a.catalog.slot
        );
        ensure!(
            a.index.effect_files == b.index.effect_files,
            "effect-file indexes differ at ordinal {ordinal}, slot {}",
            a.catalog.slot
        );
        ensure!(
            a.index.transaction_rows == b.index.transaction_rows,
            "canonical transaction rows differ at ordinal {ordinal}, slot {}",
            a.catalog.slot
        );
        ensure!(
            a.transactions == b.transactions,
            "decoded transactions differ at ordinal {ordinal}, slot {}",
            a.catalog.slot
        );

        let signature_count = a
            .transactions
            .iter()
            .try_fold(0_u64, |count, transaction| {
                count
                    .checked_add(u64::from(transaction.header.num_required_signatures))
                    .context("signature count overflow")
            })?;
        expected_signature = expected_signature
            .checked_add(signature_count)
            .context("signature ordinal overflow")?;
        block_signatures.push(signature_count);
        expected_transaction = a.catalog.transactions_end()?;
        next_transaction_a = next_transaction_a
            .checked_add(u64::from(a.catalog.transactions.stored_len))
            .context("validated A transaction extent overflow")?;
        next_transaction_b = next_transaction_b
            .checked_add(u64::from(b.catalog.transactions.stored_len))
            .context("candidate B transaction extent overflow")?;
        decoded_transaction_bytes = decoded_transaction_bytes
            .checked_add(u64::from(a.catalog.transactions.decoded_len))
            .context("transaction decoded-byte count overflow")?;

        for kind in EffectKind::ALL {
            let index = &a.index.effect_files[kind.index()];
            if index.chunks.iter().any(|frame| !frame.is_empty()) {
                ensure!(
                    index.first_chunk_offset == effect_next_offsets[kind.index()],
                    "{} chunks are not contiguous at block ordinal {ordinal}",
                    effect_path(kind)
                );
            }
            for frame in &index.chunks {
                effect_next_offsets[kind.index()] = effect_next_offsets[kind.index()]
                    .checked_add(u64::from(frame.stored_len()))
                    .context("effect extent overflow")?;
            }
            for state in &a.index.effect_states {
                effect_records[kind.index()] = effect_records[kind.index()]
                    .checked_add(u64::from(state.has_record(kind)?))
                    .context("effect record count overflow")?;
            }
        }
        rows.push(b.catalog);
    }

    ensure!(
        next_transaction_a == transactions_a.file_len,
        "validated A transaction pages do not end at EOF"
    );
    ensure!(
        next_transaction_b == transactions_b.file_len,
        "candidate B transaction pages do not end at EOF"
    );
    for object in [&transactions_a, &transactions_b] {
        ensure!(
            object.header.record_count == expected_transaction,
            "transaction header record count differs"
        );
        ensure!(
            object.header.decoded_bytes == decoded_transaction_bytes,
            "transaction header decoded-byte total differs"
        );
    }
    ensure!(
        transaction_span_differences != 0,
        "A and B have no physical transaction-page difference"
    );
    let split_frame_witness =
        split_frame_witness.context("no single-frame A to split-frame B witness was found")?;

    let mut unchanged_planes = Vec::new();
    for kind in EffectKind::ALL {
        let path = effect_path(kind);
        let object_a = open_object(&canonical_a, path, reader_a.archive_id())?;
        let object_b = open_object(&canonical_b, path, reader_b.archive_id())?;
        unchanged_planes.push(compare_payloads(path, &object_a, &object_b)?);
        ensure!(
            effect_next_offsets[kind.index()] == object_a.file_len,
            "{path} indexes do not end at EOF"
        );
        ensure!(
            object_a.header.record_count == effect_records[kind.index()],
            "{path} header record count differs from effect states"
        );
    }

    let block_rewards_a = open_object(&canonical_a, block_rewards::PATH, reader_a.archive_id())?;
    let block_rewards_b = open_object(&canonical_b, block_rewards::PATH, reader_b.archive_id())?;
    unchanged_planes.push(compare_payloads(
        block_rewards::PATH,
        &block_rewards_a,
        &block_rewards_b,
    )?);
    let poh_a = open_object(&canonical_a, poh::PATH, reader_a.archive_id())?;
    let poh_b = open_object(&canonical_b, poh::PATH, reader_b.archive_id())?;
    unchanged_planes.push(compare_payloads(poh::PATH, &poh_a, &poh_b)?);
    let shredding_a = open_object(&canonical_a, shredding::PATH, reader_a.archive_id())?;
    let shredding_b = open_object(&canonical_b, shredding::PATH, reader_b.archive_id())?;
    unchanged_planes.push(compare_payloads(
        shredding::PATH,
        &shredding_a,
        &shredding_b,
    )?);

    // Benchmark prefixes made by A and B1 stop before dictionary finalization,
    // so absence on both sides is expected and is explicit in the receipt. A
    // future prefix that includes these planes must include them on both sides;
    // their fixed-width raw payloads and semantic headers must then be exact.
    let optional_dictionary_planes = [
        (pubkeys::PATH, pubkeys::RECORD_LEN as u64),
        (account_flags::PATH, 1_u64),
        (blockhashes::PATH, blockhashes::RECORD_LEN as u64),
    ]
    .into_iter()
    .map(|(path, record_len)| {
        compare_optional_dictionary_plane(
            &canonical_a,
            reader_a.archive_id(),
            &canonical_b,
            reader_b.archive_id(),
            path,
            record_len,
        )
    })
    .collect::<Result<Vec<_>>>()?;

    let effect_audit = validate_all_effects(&canonical_b, args.max_decoded_bytes)
        .context("validate all candidate B effect objects")?;
    ensure!(
        effect_audit.blocks == args.expected_blocks,
        "effect audit block count differs"
    );
    ensure!(
        effect_audit.transactions == expected_transaction,
        "effect audit transaction count differs"
    );
    ensure!(
        effect_audit.records == effect_records,
        "effect audit record totals differ from states"
    );
    let effects = EffectKind::ALL
        .into_iter()
        .map(|kind| EffectReceipt {
            path: effect_path(kind),
            records: effect_audit.records_for(kind),
            decoded_bytes: effect_audit.decoded_bytes_for(kind),
        })
        .collect::<Vec<_>>();

    let block_reward_records =
        validate_block_rewards(&block_rewards_b, &rows, args.max_decoded_bytes)?;
    let poh_entries = validate_poh(&poh_b, &rows, &block_signatures)?;
    let shredding_boundaries = validate_shredding(&shredding_b, &rows)?;

    ensure!(
        report_b.semantics.transactions == expected_transaction,
        "report transaction count differs"
    );
    ensure!(
        report_b.semantics.signatures == expected_signature,
        "report signature count differs"
    );
    ensure!(
        report_b.semantics.block_rewards_stored == block_reward_records,
        "report block-reward count differs"
    );
    ensure!(
        report_b.semantics.poh_entries == poh_entries,
        "report PoH entry count differs"
    );
    ensure!(
        report_b.semantics.shredding_boundaries == shredding_boundaries,
        "report shredding boundary count differs"
    );
    ensure!(
        report_b.semantics.shredding_recorded_empty_blocks == 0,
        "report records empty shredding blocks"
    );
    let first_slot = rows.first().context("prefix has no first row")?.slot;
    let last_slot = rows.last().context("prefix has no last row")?.slot;
    ensure!(
        report_b.semantics.source_first_slot == first_slot,
        "report first slot differs"
    );
    ensure!(
        report_b.semantics.source_last_slot == last_slot,
        "report last slot differs"
    );

    Ok(AcceptanceReceipt {
        status: "pass",
        comparison: "exact-logical-prefix-parity",
        validated_a: canonical_a.display().to_string(),
        candidate_b: canonical_b.display().to_string(),
        archive_id_a: reader_a.archive_id().to_hex(),
        archive_id_b: reader_b.archive_id().to_hex(),
        blocks: args.expected_blocks,
        transactions: expected_transaction,
        signatures_declared: expected_signature,
        first_slot,
        last_slot,
        transaction_span_differences,
        split_frame_pages,
        split_frame_witness_ordinal: split_frame_witness,
        effect_states: expected_transaction,
        effects,
        block_reward_records,
        poh_frames: args.expected_blocks,
        poh_entries,
        shredding_frames: args.expected_blocks,
        shredding_boundaries,
        unchanged_planes,
        optional_dictionary_planes,
        max_decoded_bytes: args.max_decoded_bytes,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

fn main() -> Result<()> {
    let receipt = compare_prefix(&parse_args()?)?;
    println!("{}", serde_json::to_string_pretty(&receipt)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write, os::unix::fs::FileExt};

    use blockzilla_archive_v3::{
        catalog::blocks::FactLocator,
        ledger::transactions::{
            ChunkFrame, CpiState, EFFECT_CHUNK_TRANSACTIONS, EffectFileIndex, EffectState,
            HashOwner, HashRef, Instruction, Message, MessageHeader, PubkeyId,
            ROW_RESTART_INTERVAL, RowRestart, Transaction, TransactionBlock,
            TransactionBlockHeader, append_transaction, encode_block, encode_block_prefix,
        },
        sidecars::{
            framing,
            poh::{CurrentPohEntry, CurrentPohRecord, PohPreamble, PohWireProfile},
            shredding::{
                ShreddingBoundary, ShreddingPreamble, ShreddingRecord, ShreddingWireProfile,
            },
        },
        wincode as archive_wincode,
    };
    use blockzilla_archive_v3_convert::container::{HeaderedWriter, write_payload};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[derive(Clone, Copy)]
    enum TransactionStorage {
        SingleFrame,
        SplitFrames,
    }

    fn zstd_frame(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.include_contentsize(true).unwrap();
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn transaction(data_byte: u8) -> Transaction {
        Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 1,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: vec![PubkeyId::new(1).unwrap(), PubkeyId::new(2).unwrap()],
                instructions: vec![Instruction {
                    program_position: 1,
                    account_positions: vec![0],
                    data: vec![data_byte; 16],
                }],
            },
        }
    }

    fn write_object(
        root: &Path,
        path: &'static str,
        archive_id: ArchiveId,
        record_count: u64,
        stored: &[u8],
        decoded_len: usize,
    ) -> PageSpan {
        let mut writer = HeaderedWriter::create(root, path, 1 << 20).unwrap();
        let offset = writer.append(stored, decoded_len as u64).unwrap();
        writer.finish(archive_id, record_count).unwrap();
        PageSpan {
            offset,
            stored_len: stored.len() as u32,
            decoded_len: decoded_len as u32,
        }
    }

    fn write_fixture_with_poh_signature_coverage(
        root: &Path,
        archive_id: ArchiveId,
        storage: TransactionStorage,
        data_byte: u8,
        exact_poh_signatures: bool,
    ) {
        fs::create_dir_all(root).unwrap();
        let count = 512_u32;
        let transactions_to_write = vec![transaction(data_byte); count as usize];
        let mut rows = Vec::new();
        let mut restarts = Vec::new();
        let mut signature_delta = 0_u32;
        for (index, transaction) in transactions_to_write.iter().enumerate() {
            if (index as u32).is_multiple_of(ROW_RESTART_INTERVAL) {
                restarts.push(RowRestart {
                    row_byte_offset: rows.len() as u32,
                    signature_delta,
                });
            }
            append_transaction(&mut rows, transaction).unwrap();
            signature_delta += u32::from(transaction.header.num_required_signatures);
        }
        let effect_states = vec![EffectState::new(CpiState::Unavailable); count as usize];
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY; count.div_ceil(EFFECT_CHUNK_TRANSACTIONS) as usize],
        });
        let header = TransactionBlockHeader {
            effect_states,
            row_restarts: restarts,
            effect_files,
        };
        let logical =
            encode_block(&TransactionBlock::from_parts(header.clone(), rows.clone())).unwrap();
        let stored = match storage {
            TransactionStorage::SingleFrame => zstd_frame(&logical),
            TransactionStorage::SplitFrames => {
                let mut stored = zstd_frame(&encode_block_prefix(&header, rows.len()).unwrap());
                stored.extend_from_slice(&zstd_frame(&rows));
                stored
            }
        };
        assert!(stored.len() < logical.len());
        let transaction_span = write_object(
            root,
            transactions::PATH,
            archive_id,
            count as u64,
            &stored,
            logical.len(),
        );

        for kind in EffectKind::ALL {
            write_payload(root, effect_path(kind), archive_id, 0, &[]).unwrap();
        }
        write_payload(root, block_rewards::PATH, archive_id, 0, &[]).unwrap();

        let poh_record = CurrentPohRecord {
            block_id: 0,
            slot: 100,
            entries: vec![CurrentPohEntry {
                num_hashes: 1,
                hash: [9; 32],
                transaction_count: count,
                signature_count: if exact_poh_signatures { count } else { 0 },
            }],
        };
        let poh_frame =
            framing::encode_frame(&archive_wincode::encode(&poh_record).unwrap()).unwrap();
        let mut poh_writer = HeaderedWriter::create(root, poh::PATH, 1 << 20).unwrap();
        let preamble = PohPreamble {
            profile: PohWireProfile::ArchiveV2CurrentWincode055,
        }
        .encode();
        poh_writer.append(&preamble, preamble.len() as u64).unwrap();
        let poh_offset = poh_writer
            .append(&poh_frame, poh_frame.len() as u64)
            .unwrap();
        poh_writer.finish(archive_id, 1).unwrap();
        let poh_span = PageSpan {
            offset: poh_offset,
            stored_len: poh_frame.len() as u32,
            decoded_len: poh_frame.len() as u32,
        };

        let shredding_record = ShreddingRecord {
            block_id: 0,
            slot: 100,
            boundaries: vec![ShreddingBoundary {
                entry_end_index: 0,
                shred_end_index: 0,
            }],
        };
        let shredding_frame =
            framing::encode_frame(&archive_wincode::encode(&shredding_record).unwrap()).unwrap();
        let mut shredding_writer = HeaderedWriter::create(root, shredding::PATH, 1 << 20).unwrap();
        let preamble = ShreddingPreamble {
            profile: ShreddingWireProfile::ArchiveV2Wincode055,
        }
        .encode();
        shredding_writer
            .append(&preamble, preamble.len() as u64)
            .unwrap();
        let shredding_offset = shredding_writer
            .append(&shredding_frame, shredding_frame.len() as u64)
            .unwrap();
        shredding_writer.finish(archive_id, 1).unwrap();
        let shredding_span = PageSpan {
            offset: shredding_offset,
            stored_len: shredding_frame.len() as u32,
            decoded_len: shredding_frame.len() as u32,
        };

        let catalog = catalog_blocks::encode_table(&[BlockRow {
            slot: 100,
            parent_slot: 99,
            first_transaction: 0,
            transaction_count: count,
            blockhash: HashRef {
                owner: HashOwner::PohBlockFinal,
                ordinal: 0,
            },
            previous_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            block_time: Some(1),
            block_height: Some(2),
            first_signature: 0,
            transactions: transaction_span,
            block_rewards: FactLocator::Unavailable,
            poh: FactLocator::Source(poh_span),
            shredding: FactLocator::Source(shredding_span),
        }])
        .unwrap();
        write_payload(root, catalog_blocks::PATH, archive_id, 1, &catalog).unwrap();

        let report = json!({
            "archive_id": archive_id.to_hex(),
            "source_published": false,
            "source_generation_digest": null,
            "epoch": 900,
            "slots_per_epoch": 432000,
            "source_profile": "archive-v2-current-hot-v1",
            "metadata_source_profile": "archive-v2-legacy-raw-error-v1",
            "output_status": BENCHMARK_STATUS,
            "fixture_previous_blockhash": null,
            "fixture_previous_slot": null,
            "blocks": 1,
            "transactions": count,
            "top_level_instructions": count,
            "inner_instructions": 0,
            "account_references": count * 2,
            "raw_fallback_transactions": 0,
            "loaded_addresses_unavailable": 0,
            "cpi_not_recorded": 0,
            "raw_account_keys": 0,
            "nonce_blockhashes": 0,
            "instruction_data_variants": { "Raw": count },
            "instructions_bytes_retained": count,
            "instructions_bytes_rederived": 0,
            "retained_payload_bytes": count * 16,
            "instruction_data_bytes": count * 16,
            "inner_instruction_data_bytes": 0,
            "token_balances_paired": 0,
            "token_balances_total": 0,
            "signatures": count,
            "blockhash_dictionary_records": 0,
            "poh_source_schema": "archive-v2-current-wincode-0.5.5",
            "poh_entries": 1,
            "poh_signature_count_recovered_blocks": 0,
            "poh_signature_count_legacy_unknown_blocks": 0,
            "shredding_boundaries": 1,
            "shredding_recorded_empty_blocks": 0,
            "nonce_hashes_interned": 0,
            "pubkey_dictionary_records": 0,
            "block_rewards_stored": 0,
            "program_accounts": 0,
            "signer_accounts": 0,
            "unused_accounts": 0,
            "source_block_bytes": 1,
            "source_decoded_block_bytes": 1,
            "source_first_slot": 100,
            "source_last_slot": 100,
            "benchmark_prefix_blocks": 1,
            "source_total_blocks": 2
        });
        fs::write(
            root.join(BENCHMARK_REPORT),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }

    fn write_fixture(
        root: &Path,
        archive_id: ArchiveId,
        storage: TransactionStorage,
        data_byte: u8,
    ) {
        write_fixture_with_poh_signature_coverage(root, archive_id, storage, data_byte, true);
    }

    fn write_dictionary_planes(
        root: &Path,
        archive_id: ArchiveId,
        pubkey_payload: &[u8],
        account_flag_payload: &[u8],
        blockhash_payload: &[u8],
    ) {
        assert!(pubkey_payload.len().is_multiple_of(pubkeys::RECORD_LEN));
        assert!(
            blockhash_payload
                .len()
                .is_multiple_of(blockhashes::RECORD_LEN)
        );
        write_payload(
            root,
            pubkeys::PATH,
            archive_id,
            (pubkey_payload.len() / pubkeys::RECORD_LEN) as u64,
            pubkey_payload,
        )
        .unwrap();
        write_payload(
            root,
            account_flags::PATH,
            archive_id,
            account_flag_payload.len() as u64,
            account_flag_payload,
        )
        .unwrap();
        write_payload(
            root,
            blockhashes::PATH,
            archive_id,
            (blockhash_payload.len() / blockhashes::RECORD_LEN) as u64,
            blockhash_payload,
        )
        .unwrap();
    }

    fn fixture_args(a: &Path, b: &Path) -> Args {
        Args {
            validated_a: a.to_owned(),
            candidate_b: b.to_owned(),
            expected_blocks: 1,
            max_decoded_bytes: 8 << 20,
        }
    }

    fn set_report_field(root: &Path, field: &str, value: serde_json::Value) {
        let path = root.join(BENCHMARK_REPORT);
        let mut report: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        report
            .as_object_mut()
            .expect("fixture report is an object")
            .insert(field.to_owned(), value);
        fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }

    #[test]
    fn split_and_single_frames_have_exact_logical_parity() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        write_fixture(
            &a,
            ArchiveId::new([1; 16]),
            TransactionStorage::SingleFrame,
            7,
        );
        write_fixture(
            &b,
            ArchiveId::new([2; 16]),
            TransactionStorage::SplitFrames,
            7,
        );
        let receipt = compare_prefix(&fixture_args(&a, &b)).unwrap();
        assert_eq!(receipt.status, "pass");
        assert_eq!(receipt.blocks, 1);
        assert_eq!(receipt.transactions, 512);
        assert_eq!(receipt.split_frame_pages, 1);
        assert_eq!(receipt.split_frame_witness_ordinal, 0);
        assert_eq!(receipt.optional_dictionary_planes.len(), 3);
        assert!(receipt.optional_dictionary_planes.iter().all(|plane| {
            plane.comparison == "absent-in-both-not-compared"
                && plane.record_count.is_none()
                && plane.payload_bytes.is_none()
        }));
    }

    #[test]
    fn split_layout_rejects_full_logical_first_frame_and_empty_arena() {
        let header = TransactionBlockHeader {
            effect_states: Vec::new(),
            row_restarts: Vec::new(),
            effect_files: Default::default(),
        };
        let rows = vec![1, 2, 3, 4];
        let prefix = encode_block_prefix(&header, rows.len()).unwrap();
        let mut logical = prefix.clone();
        logical.extend_from_slice(&rows);
        let mut stored = zstd_frame(&logical);
        stored.extend_from_slice(&zstd_frame(&[]));

        let error = validate_split_transaction_page(&stored, &prefix, &rows, "bad page")
            .unwrap_err()
            .to_string();
        assert!(error.contains("bad page prefix"), "{error}");
    }

    #[test]
    fn present_dictionary_planes_require_exact_header_and_payload_parity() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        let a_id = ArchiveId::new([1; 16]);
        let b_id = ArchiveId::new([2; 16]);
        write_fixture(&a, a_id, TransactionStorage::SingleFrame, 7);
        write_fixture(&b, b_id, TransactionStorage::SplitFrames, 7);
        let mut pubkeys = vec![1_u8; pubkeys::RECORD_LEN];
        pubkeys.extend_from_slice(&[2_u8; pubkeys::RECORD_LEN]);
        let mut blockhashes = vec![3_u8; blockhashes::RECORD_LEN];
        blockhashes.extend_from_slice(&[4_u8; blockhashes::RECORD_LEN]);
        write_dictionary_planes(&a, a_id, &pubkeys, &[1, 2], &blockhashes);
        write_dictionary_planes(&b, b_id, &pubkeys, &[1, 2], &blockhashes);

        let receipt = compare_prefix(&fixture_args(&a, &b)).unwrap();
        assert!(receipt.optional_dictionary_planes.iter().all(|plane| {
            plane.comparison == "exact-header-and-payload-parity"
                && plane.record_count == Some(2)
                && plane.payload_bytes.is_some()
        }));
    }

    #[test]
    fn dictionary_first_use_order_mismatch_is_rejected() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        let a_id = ArchiveId::new([1; 16]);
        let b_id = ArchiveId::new([2; 16]);
        write_fixture(&a, a_id, TransactionStorage::SingleFrame, 7);
        write_fixture(&b, b_id, TransactionStorage::SplitFrames, 7);
        let mut pubkeys_a = vec![1_u8; pubkeys::RECORD_LEN];
        pubkeys_a.extend_from_slice(&[2_u8; pubkeys::RECORD_LEN]);
        let mut pubkeys_b = vec![2_u8; pubkeys::RECORD_LEN];
        pubkeys_b.extend_from_slice(&[1_u8; pubkeys::RECORD_LEN]);
        let blockhashes = vec![3_u8; blockhashes::RECORD_LEN];
        write_dictionary_planes(&a, a_id, &pubkeys_a, &[1, 2], &blockhashes);
        write_dictionary_planes(&b, b_id, &pubkeys_b, &[1, 2], &blockhashes);

        let error = compare_prefix(&fixture_args(&a, &b)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unchanged plane dictionary/pubkeys.pages payload differs")
        );
    }

    #[test]
    fn one_sided_dictionary_presence_is_rejected() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        let a_id = ArchiveId::new([1; 16]);
        write_fixture(&a, a_id, TransactionStorage::SingleFrame, 7);
        write_fixture(
            &b,
            ArchiveId::new([2; 16]),
            TransactionStorage::SplitFrames,
            7,
        );
        write_payload(&a, pubkeys::PATH, a_id, 1, &[1_u8; pubkeys::RECORD_LEN]).unwrap();

        let error = compare_prefix(&fixture_args(&a, &b)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("optional dictionary plane dictionary/pubkeys.pages presence differs")
        );
    }

    #[test]
    fn dictionary_header_geometry_is_validated_even_when_payloads_match() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        let a_id = ArchiveId::new([1; 16]);
        let b_id = ArchiveId::new([2; 16]);
        write_fixture(&a, a_id, TransactionStorage::SingleFrame, 7);
        write_fixture(&b, b_id, TransactionStorage::SplitFrames, 7);
        let malformed = [1_u8; pubkeys::RECORD_LEN * 2];
        write_payload(&a, pubkeys::PATH, a_id, 1, &malformed).unwrap();
        write_payload(&b, pubkeys::PATH, b_id, 1, &malformed).unwrap();

        let error = compare_prefix(&fixture_args(&a, &b)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("payload byte count does not match its record count")
        );
    }

    #[test]
    fn current_poh_all_zero_signature_counts_are_accepted_as_legacy_unknown() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        write_fixture_with_poh_signature_coverage(
            &a,
            ArchiveId::new([1; 16]),
            TransactionStorage::SingleFrame,
            7,
            false,
        );
        write_fixture_with_poh_signature_coverage(
            &b,
            ArchiveId::new([2; 16]),
            TransactionStorage::SplitFrames,
            7,
            false,
        );
        let receipt = compare_prefix(&fixture_args(&a, &b)).unwrap();
        assert_eq!(receipt.status, "pass");
        assert_eq!(receipt.signatures_declared, 512);
    }

    #[test]
    fn semantic_report_counter_mismatch_is_rejected() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        write_fixture(
            &a,
            ArchiveId::new([1; 16]),
            TransactionStorage::SingleFrame,
            7,
        );
        write_fixture(
            &b,
            ArchiveId::new([2; 16]),
            TransactionStorage::SplitFrames,
            7,
        );
        set_report_field(&b, "raw_account_keys", json!(1));

        let error = compare_prefix(&fixture_args(&a, &b)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("benchmark report semantics differ between A and B")
        );
    }

    #[test]
    fn logical_transaction_mismatch_is_rejected() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        write_fixture(
            &a,
            ArchiveId::new([1; 16]),
            TransactionStorage::SingleFrame,
            7,
        );
        write_fixture(
            &b,
            ArchiveId::new([2; 16]),
            TransactionStorage::SplitFrames,
            8,
        );
        let error = compare_prefix(&fixture_args(&a, &b)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("canonical transaction rows differ")
        );
    }

    #[test]
    fn unchanged_sidecar_payload_mismatch_is_rejected() {
        let root = tempdir().unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        write_fixture(
            &a,
            ArchiveId::new([1; 16]),
            TransactionStorage::SingleFrame,
            7,
        );
        write_fixture(
            &b,
            ArchiveId::new([2; 16]),
            TransactionStorage::SplitFrames,
            7,
        );
        let path = b.join(poh::PATH);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let offset = file.metadata().unwrap().len() - 1;
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= 1;
        file.write_all_at(&byte, offset).unwrap();
        let error = compare_prefix(&fixture_args(&a, &b)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unchanged plane sidecars/poh.wincode payload differs")
        );
    }
}
