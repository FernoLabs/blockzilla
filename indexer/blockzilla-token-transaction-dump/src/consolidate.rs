//! Validation for immutable raw epoch shards.
//!
//! Phase 2 is intentionally not enabled yet. The schema-2 consolidator changed transaction
//! payloads during extraction and cannot safely consume schema-3 exact-byte shards.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, Read},
    path::Path,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
};
use blockzilla_primitives::{WincodeLeb128FramedReader, bounded_wincode_leb128_config};
use sha2::{Digest, Sha256};

use crate::{
    format::{
        ACCOUNT_ID_LOG_FILE, AccountIdRole, DUMP_MANIFEST_FILE, DUMP_SCHEMA_VERSION,
        DiscoveredAccount, DumpArtifactKind, DumpManifest, DumpStreamKind, EpochAccountIdLog,
        PUBKEY_REGISTRY_ID_BASE, SourceTransactionCoordinate, TRANSACTIONS_FILE,
        TokenTransactionDumpFooter, TokenTransactionDumpRecord, TokenTransactionRecord,
    },
    resume::{ResumeCounters, ResumeShardBinding},
};

const IO_BUFFER_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResumeTargetBinding {
    pub mint: [u8; 32],
    pub mint_slot: u64,
    pub mint_signature: [u8; 64],
    pub workers: usize,
}

fn ensure_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect required file {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "required path {} is not a regular file",
        path.display()
    );
    Ok(())
}

fn ensure_exact_shard_files(directory: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(directory)
        .with_context(|| format!("inspect raw shard {}", directory.display()))?;
    ensure!(
        metadata.file_type().is_dir(),
        "raw shard is not a direct directory"
    );
    let expected = [ACCOUNT_ID_LOG_FILE, DUMP_MANIFEST_FILE, TRANSACTIONS_FILE]
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        ensure!(
            entry.file_type()?.is_file(),
            "raw shard member {} is not a regular file",
            entry.path().display()
        );
        observed.insert(
            entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("raw shard contains a non-UTF-8 file name"))?,
        );
    }
    ensure!(
        observed == expected,
        "raw shard has unexpected or missing files"
    );
    Ok(())
}

fn sha256_file(path: &Path) -> Result<[u8; 32]> {
    ensure_regular_file(path)?;
    let file = File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn hex_digest(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_source_binding(binding: &crate::format::DumpSourceBinding) -> Result<()> {
    let crate::format::DumpSourceBinding::TrustedLocalSizesOnly {
        cluster_id,
        slots_per_epoch,
        ..
    } = binding;
    ensure!(!cluster_id.is_empty(), "trusted-local cluster ID is empty");
    ensure!(
        *slots_per_epoch != 0,
        "trusted-local slots per epoch must not be zero"
    );
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "keep every raw source-coordinate and generation bound explicit at validation"
)]
fn validate_raw_record(
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    slots_per_epoch: u64,
    source_generation_digest: [u8; 32],
    source_wire_profile: crate::format::DumpWireProfile,
    record: &TokenTransactionRecord,
    state: &mut RawStreamValidationState,
) -> Result<()> {
    ensure!(
        record.source_epoch == epoch,
        "epoch {epoch} stream contains transaction for epoch {}",
        record.source_epoch
    );
    ensure!(
        record.source_generation_digest == source_generation_digest,
        "epoch {epoch} transaction source generation differs from its header"
    );
    ensure!(
        record.source_wire_profile == source_wire_profile,
        "epoch {epoch} transaction wire profile differs from its header"
    );
    ensure!(
        (epoch_start_slot..=epoch_end_slot).contains(&record.block.slot),
        "epoch {epoch} stream contains slot {} outside {epoch_start_slot}..={epoch_end_slot}",
        record.block.slot,
    );
    ensure!(
        record.block.parent_slot < record.block.slot,
        "epoch {epoch} slot {} has parent slot {} that is not earlier",
        record.block.slot,
        record.block.parent_slot,
    );
    ensure!(
        u64::from(record.source_block_id) < slots_per_epoch,
        "epoch {epoch} source block ID {} is outside 0..{slots_per_epoch}",
        record.source_block_id,
    );
    ensure!(
        record.block.transaction_count != 0,
        "epoch {epoch} slot {} has a zero source transaction count",
        record.block.slot,
    );
    ensure!(
        record.tx_index < record.block.transaction_count,
        "epoch {epoch} slot {} transaction index {} is outside source transaction count {}",
        record.block.slot,
        record.tx_index,
        record.block.transaction_count
    );
    ensure!(
        record.signature_count != 0,
        "epoch {epoch} slot {} transaction {} has no source signatures",
        record.block.slot,
        record.tx_index
    );
    record
        .source_first_signature_ordinal
        .checked_add(u64::from(record.signature_count))
        .with_context(|| {
            format!(
                "epoch {epoch} slot {} transaction {} source signature range overflows",
                record.block.slot, record.tx_index,
            )
        })?;
    ensure!(
        record.dump_signature_ordinal.is_none(),
        "raw epoch {epoch} transaction contains a final signature-sidecar reference"
    );
    ensure!(
        !record.message_bytes.is_empty(),
        "epoch {epoch} slot {} transaction {} has empty message bytes",
        record.block.slot,
        record.tx_index
    );
    let has_metadata = record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    ensure!(
        has_metadata == !record.metadata_bytes.is_empty(),
        "epoch {epoch} slot {} transaction {} metadata flag differs from raw bytes",
        record.block.slot,
        record.tx_index
    );
    ensure!(
        record.flags
            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
            == 0,
        "epoch {epoch} transaction contains an opaque fallback flag"
    );
    let transaction_key = (
        record.source_epoch,
        record.block.slot,
        record.source_block_id,
        record.tx_index,
    );
    ensure!(
        state.transaction_keys.insert(transaction_key),
        "epoch {epoch} raw stream repeats source transaction ({}, {}, {})",
        record.block.slot,
        record.source_block_id,
        record.tx_index,
    );
    let block_identity = RawBlockIdentity::from(record);
    if let Some(previous) = state
        .blocks_by_id
        .insert(record.source_block_id, block_identity)
    {
        ensure!(
            previous == block_identity,
            "epoch {epoch} source block ID {} has conflicting block context",
            record.source_block_id,
        );
    }
    if let Some(previous) = state
        .block_ids_by_slot
        .insert(record.block.slot, record.source_block_id)
    {
        ensure!(
            previous == record.source_block_id,
            "epoch {epoch} slot {} has conflicting source block IDs {previous} and {}",
            record.block.slot,
            record.source_block_id,
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawBlockIdentity {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    transaction_count: u32,
}

impl From<&TokenTransactionRecord> for RawBlockIdentity {
    fn from(record: &TokenTransactionRecord) -> Self {
        Self {
            slot: record.block.slot,
            parent_slot: record.block.parent_slot,
            blockhash_id: record.block.blockhash_id,
            previous_blockhash_id: record.block.previous_blockhash_id,
            block_time: record.block.block_time,
            block_height: record.block.block_height,
            transaction_count: record.block.transaction_count,
        }
    }
}

#[derive(Debug, Default)]
struct RawStreamValidationState {
    transaction_keys: BTreeSet<(u64, u64, u32, u32)>,
    blocks_by_id: BTreeMap<u32, RawBlockIdentity>,
    block_ids_by_slot: BTreeMap<u64, u32>,
}

pub(crate) fn read_epoch_account_id_log(path: &Path) -> Result<EpochAccountIdLog> {
    ensure_regular_file(path)?;
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    wincode::config::deserialize_exact(
        &bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .with_context(|| format!("decode {}", path.display()))
}

fn validate_epoch_account_ids(
    path: &Path,
    epoch: u64,
    generation_digest: [u8; 32],
    mint: [u8; 32],
    accounts: &[DiscoveredAccount],
) -> Result<[u8; 32]> {
    let log = read_epoch_account_id_log(path)?;
    ensure!(
        log.schema_version == DUMP_SCHEMA_VERSION,
        "epoch {epoch} account-ID log uses schema {}, expected {DUMP_SCHEMA_VERSION}",
        log.schema_version
    );
    ensure!(
        log.epoch == epoch,
        "epoch {epoch} account-ID log epoch differs"
    );
    ensure!(
        log.source_generation_digest == generation_digest,
        "epoch {epoch} account-ID log source generation differs"
    );
    ensure!(
        log.entries
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "epoch {epoch} account-ID log is not strictly sorted and unique"
    );
    let mint_entries = log
        .entries
        .iter()
        .filter(|entry| entry.role == AccountIdRole::TargetMint)
        .collect::<Vec<_>>();
    ensure!(
        mint_entries.len() == 1
            && mint_entries[0].raw_pubkey == mint
            && mint_entries[0].first_creation.is_none(),
        "epoch {epoch} account-ID log does not contain one target mint"
    );
    let expected_account_count = accounts
        .iter()
        .filter(|account| account.first_creation.epoch <= epoch)
        .count();
    ensure!(
        log.entries.len() == expected_account_count.saturating_add(1),
        "epoch {epoch} account-ID log does not cover the account prefix known by that epoch"
    );
    for entry in &log.entries {
        if let Some(id) = entry.local_id {
            ensure!(
                id != 0,
                "epoch {epoch} account-ID log contains local ID zero"
            );
        }
        if entry.role == AccountIdRole::TokenAccount {
            ensure!(
                entry.raw_pubkey != mint,
                "epoch {epoch} account-ID log labels the target mint as a token account"
            );
            ensure!(
                entry.first_creation.is_some(),
                "epoch {epoch} token account has no creation coordinate"
            );
            let expected = accounts
                .binary_search_by_key(&entry.raw_pubkey, |account| account.raw_pubkey)
                .ok()
                .and_then(|index| accounts.get(index))
                .with_context(|| {
                    format!(
                        "epoch {epoch} account-ID log contains a key outside the frozen account list"
                    )
                })?;
            ensure!(
                expected.first_creation.epoch <= epoch,
                "epoch {epoch} account-ID log contains an account created in a later epoch"
            );
            ensure!(
                entry.first_creation == Some(expected.first_creation),
                "epoch {epoch} account-ID log creation coordinate differs from the frozen account list"
            );
        }
    }
    sha256_file(path)
}

fn validate_manifest(
    directory: &Path,
    epoch: u64,
    target: ResumeTargetBinding,
    source_binding: &crate::format::DumpSourceBinding,
    transaction_count: u64,
    stream_digest: [u8; 32],
    transition_digest: [u8; 32],
) -> Result<DumpManifest> {
    let path = directory.join(DUMP_MANIFEST_FILE);
    ensure_regular_file(&path)?;
    let manifest: DumpManifest = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::RawEpochShard
            && manifest.complete,
        "epoch {epoch} manifest is not a complete schema-{DUMP_SCHEMA_VERSION} raw shard"
    );
    validate_source_binding(&manifest.source_binding)?;
    ensure!(
        &manifest.source_binding == source_binding,
        "epoch {epoch} shard source admission differs from the resumed run"
    );
    ensure!(
        manifest.mint == bs58::encode(target.mint).into_string()
            && manifest.mint_slot == target.mint_slot
            && manifest.mint_signature == bs58::encode(target.mint_signature).into_string(),
        "epoch {epoch} manifest target differs from the resumed run"
    );
    ensure!(
        manifest.workers == target.workers,
        "epoch {epoch} shard worker count differs from the resumed run"
    );
    ensure!(
        manifest.first_epoch == epoch
            && manifest.last_epoch == epoch
            && manifest.transactions == transaction_count,
        "epoch {epoch} manifest range or transaction count differs from its stream"
    );
    ensure!(
        manifest.signatures.is_none()
            && manifest.pubkeys.is_none()
            && manifest.signature_stream.is_none()
            && manifest.signature_stream_sha256.is_none()
            && manifest.pubkey_registry.is_none()
            && manifest.pubkey_registry_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "epoch {epoch} raw manifest claims a phase-2 accounting sidecar"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.transaction_stream_sha256.as_deref()
                == Some(hex_digest(stream_digest).as_str()),
        "epoch {epoch} raw transaction stream name or digest differs"
    );
    ensure!(
        manifest.account_id_log.as_deref() == Some(ACCOUNT_ID_LOG_FILE)
            && manifest.account_id_log_sha256.as_deref()
                == Some(hex_digest(transition_digest).as_str()),
        "epoch {epoch} account-ID log name or digest differs"
    );
    Ok(manifest)
}

/// Fully validate one committed or partial raw shard before resume trusts it.
#[allow(
    clippy::too_many_arguments,
    reason = "keep the resume artifact, source, slot schedule, account list, and anchor bindings explicit"
)]
pub(crate) fn validate_epoch_shard_for_resume(
    epoch: u64,
    directory: &Path,
    target: ResumeTargetBinding,
    source_binding: &crate::format::DumpSourceBinding,
    source_generation_digest: [u8; 32],
    slots_per_epoch: u64,
    accounts: &[DiscoveredAccount],
    anchor_position: SourceTransactionCoordinate,
) -> Result<ResumeShardBinding> {
    ensure!(slots_per_epoch != 0, "epoch slot count is zero");
    let epoch_start_slot = epoch
        .checked_mul(slots_per_epoch)
        .context("epoch start slot overflow")?;
    let epoch_end_slot = epoch_start_slot
        .checked_add(slots_per_epoch - 1)
        .context("epoch end slot overflow")?;
    ensure_exact_shard_files(directory)?;
    let stream_path = directory.join(TRANSACTIONS_FILE);
    ensure_regular_file(&stream_path)?;
    let file = File::open(&stream_path)
        .with_context(|| format!("open raw stream {}", stream_path.display()))?;
    let mut reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(IO_BUFFER_BYTES, file));
    let (_, first): (_, TokenTransactionDumpRecord) =
        reader.read()?.context("raw transaction stream is empty")?;
    let TokenTransactionDumpRecord::Header(header) = first else {
        bail!("epoch {epoch} raw stream does not start with a header");
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::RawEpochShard
            && header.source_epoch == Some(epoch)
            && header.source_generation_digest == Some(source_generation_digest)
            && header.source_wire_profile.is_some()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "epoch {epoch} raw stream header is not canonical"
    );
    ensure!(
        header.mint == target.mint
            && header.mint_slot == target.mint_slot
            && header.mint_signature == target.mint_signature,
        "epoch {epoch} raw stream target differs from the resumed run"
    );
    let source_wire_profile = header
        .source_wire_profile
        .context("validated raw header has no source wire profile")?;

    let mut validation = RawStreamValidationState::default();
    let mut transactions = 0u64;
    let mut anchor_transactions = 0u64;
    let footer = loop {
        let (_, record): (_, TokenTransactionDumpRecord) = reader
            .read()?
            .with_context(|| format!("epoch {epoch} raw stream has no footer"))?;
        match record {
            TokenTransactionDumpRecord::Header(_) => {
                bail!("epoch {epoch} raw stream has more than one header")
            }
            TokenTransactionDumpRecord::Transaction(record) => {
                validate_raw_record(
                    epoch,
                    epoch_start_slot,
                    epoch_end_slot,
                    slots_per_epoch,
                    source_generation_digest,
                    source_wire_profile,
                    &record,
                    &mut validation,
                )?;
                transactions = transactions
                    .checked_add(1)
                    .context("raw transaction count overflow")?;
                if (
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    record.tx_index,
                ) == (
                    anchor_position.epoch,
                    anchor_position.slot,
                    anchor_position.source_block_id,
                    anchor_position.tx_index,
                ) {
                    ensure!(
                        record.source_first_signature_ordinal
                            == anchor_position.source_first_signature_ordinal
                            && record.signature_count == anchor_position.signature_count,
                        "epoch {epoch} mint-anchor signature reference differs from the frozen account artifact"
                    );
                    anchor_transactions = anchor_transactions
                        .checked_add(1)
                        .context("raw anchor transaction count overflow")?;
                }
            }
            TokenTransactionDumpRecord::Footer(footer) => break footer,
        }
    };
    ensure!(
        reader.read::<TokenTransactionDumpRecord>()?.is_none(),
        "epoch {epoch} raw stream has records after its footer"
    );
    validate_raw_footer(
        epoch,
        slots_per_epoch,
        validation.blocks_by_id.len(),
        footer,
        transactions,
    )?;
    ensure!(
        anchor_transactions == u64::from(epoch == anchor_position.epoch),
        "epoch {epoch} raw stream has {anchor_transactions} mint-anchor transactions"
    );

    let stream_digest = sha256_file(&stream_path)?;
    let transition_digest = validate_epoch_account_ids(
        &directory.join(ACCOUNT_ID_LOG_FILE),
        epoch,
        source_generation_digest,
        target.mint,
        accounts,
    )?;
    validate_manifest(
        directory,
        epoch,
        target,
        source_binding,
        transactions,
        stream_digest,
        transition_digest,
    )?;

    Ok(ResumeShardBinding {
        epoch,
        source_generation_digest: hex_digest(source_generation_digest),
        transaction_stream_sha256: hex_digest(stream_digest),
        account_id_log_sha256: hex_digest(transition_digest),
        counters: ResumeCounters {
            transactions: footer.transactions_written,
            anchor_transactions,
            blocks_scanned: footer.blocks_scanned,
            transactions_scanned: footer.transactions_scanned,
            owned_block_fallbacks: footer.owned_block_fallbacks,
        },
    })
}

fn validate_raw_footer(
    epoch: u64,
    slots_per_epoch: u64,
    selected_blocks: usize,
    footer: TokenTransactionDumpFooter,
    transactions: u64,
) -> Result<()> {
    ensure!(
        footer.epochs == 1
            && footer.blocks_scanned <= slots_per_epoch
            && u64::try_from(selected_blocks).is_ok_and(|count| count <= footer.blocks_scanned)
            && footer.transactions_written == transactions
            && footer.transactions_scanned >= transactions
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.pubkeys == 0
            && footer.signatures == 0
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "epoch {epoch} raw footer counters are invalid"
    );
    Ok(())
}

/// Consolidate validated schema-3 raw shards into one canonical transaction
/// stream, one occurrence-ordered signature stream, and one sorted registry.
pub fn consolidate_epoch_shards(
    archive_root: &Path,
    input: &Path,
    output: &Path,
    allow_metadata_generation_drift: bool,
    resume: bool,
) -> Result<()> {
    crate::consolidate_v3::consolidate_epoch_shards_v3(
        archive_root,
        input,
        output,
        allow_metadata_generation_drift,
        resume,
    )
}

pub fn validate_completed_consolidated_dump(output: &Path) -> Result<()> {
    crate::consolidate_v3::validate_completed_consolidated_dump_v3(output)
}

/// Build a deterministic outer-and-inner program inventory for one completed dump.
pub fn inventory_consolidated_programs(dump: &Path, report: &Path) -> Result<()> {
    crate::consolidate_v3::inventory_consolidated_programs_v3(dump, report)
}

/// Build one metadata-derived token history report for a completed dump.
pub fn build_consolidated_token_history_report(dump: &Path, report: &Path) -> Result<()> {
    crate::consolidate_v3::build_consolidated_token_history_report_v3(dump, report)
}

/// Replay SPYx public balances from committed token instructions and compare
/// them with transaction metadata. Unknown effects remain explicit blockers.
pub fn replay_consolidated_spyx_balances(
    dump: &Path,
    report: &Path,
    max_transactions: Option<u64>,
) -> Result<()> {
    crate::consolidate_v3::replay_consolidated_spyx_balances_v3(dump, report, max_transactions)
}

/// One source-bound result from the strict SPYx instruction replay used by
/// owner-linked transaction indexes.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SpyxOwnerReplaySummary {
    pub complete: bool,
    pub transactions: u64,
    pub transaction_bytes_scanned: u64,
    pub manifest_sha256: String,
    pub transaction_sha256: String,
    pub registry_sha256: String,
    pub accounts_sha256: String,
    pub replay_state_sha256: String,
}

/// One exact transaction-final public SPYx balance change for one owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpyxOwnerBalanceChange {
    pub owner_registry_id: u32,
    pub raw_delta: i128,
    pub post_raw_balance: u128,
}

/// A borrowed, allocation-free view of one replayed transaction and its owner
/// projections. Balance changes contain only owners with a non-zero net delta.
#[derive(Debug, Clone, Copy)]
pub struct SpyxOwnerBalanceTransaction<'a> {
    pub transaction_id: u64,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub linked_owner_registry_ids: &'a [u32],
    pub balance_changes: &'a [SpyxOwnerBalanceChange],
}

/// Visit the owner-linked target-account projection of each transaction while
/// running the same fail-closed instruction replay as the SPYx balance proof.
///
/// The callback receives the zero-based transaction ordinal and sorted unique
/// registry IDs for owners of target token accounts that are present in the
/// resolved message. An owner is included when that account is open directly
/// before or directly after the validated transaction.
pub fn visit_consolidated_spyx_owner_postings<F>(
    dump: &Path,
    max_transactions: Option<u64>,
    visit: F,
) -> Result<SpyxOwnerReplaySummary>
where
    F: FnMut(u64, &[u32]) -> Result<()>,
{
    crate::consolidate_v3::visit_consolidated_spyx_owner_postings_v3(dump, max_transactions, visit)
}

/// Visit owner-linked transactions and exact sparse owner balance changes
/// while running one strict, fail-closed SPYx instruction replay.
///
/// The callback is invoked once for every accepted source transaction. Its
/// borrowed slices are reused by the scanner and must not be retained.
pub fn visit_consolidated_spyx_owner_balance_history<F>(
    dump: &Path,
    max_transactions: Option<u64>,
    visit: F,
) -> Result<SpyxOwnerReplaySummary>
where
    F: for<'a> FnMut(SpyxOwnerBalanceTransaction<'a>) -> Result<()>,
{
    crate::consolidate_v3::visit_consolidated_spyx_owner_balance_history_v3(
        dump,
        max_transactions,
        visit,
    )
}

/// Measure exact DEX parser instruction and transaction coverage.
pub fn measure_dex_parser_coverage(dump: &Path, report: &Path) -> Result<()> {
    crate::consolidate_v3::measure_dex_parser_coverage_v3(dump, report)
}

/// Inventory attributed compact-log evidence for selected program IDs.
pub fn inventory_consolidated_program_logs(
    dump: &Path,
    programs: &Path,
    report: &Path,
) -> Result<()> {
    crate::consolidate_v3::inventory_consolidated_program_logs_v3(dump, programs, report)
}

/// Measure exact instruction and transaction coverage for identified program IDs.
pub fn measure_identified_program_coverage(
    dump: &Path,
    identified_programs: &Path,
    report: &Path,
) -> Result<()> {
    crate::consolidate_v3::measure_identified_program_coverage_v3(dump, identified_programs, report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{DumpWireProfile, TokenTransactionBlockContext};

    const TEST_EPOCH: u64 = 801;
    const TEST_SLOTS_PER_EPOCH: u64 = 432_000;
    const TEST_GENERATION: [u8; 32] = [7; 32];

    fn test_record(tx_index: u32) -> TokenTransactionRecord {
        let slot = TEST_EPOCH * TEST_SLOTS_PER_EPOCH + 5;
        TokenTransactionRecord {
            source_epoch: TEST_EPOCH,
            source_generation_digest: TEST_GENERATION,
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 3,
            block: TokenTransactionBlockContext {
                slot,
                parent_slot: slot - 1,
                blockhash_id: 11,
                previous_blockhash_id: 10,
                block_time: Some(1_700_000_000),
                block_height: Some(99),
                transaction_count: 2,
            },
            tx_index,
            flags: 0,
            source_first_signature_ordinal: 20 + u64::from(tx_index),
            signature_count: 1,
            dump_signature_ordinal: None,
            message_bytes: vec![1],
            metadata_bytes: Vec::new(),
        }
    }

    fn validate_test_record(
        record: &TokenTransactionRecord,
        state: &mut RawStreamValidationState,
    ) -> Result<()> {
        let start = TEST_EPOCH * TEST_SLOTS_PER_EPOCH;
        validate_raw_record(
            TEST_EPOCH,
            start,
            start + TEST_SLOTS_PER_EPOCH - 1,
            TEST_SLOTS_PER_EPOCH,
            TEST_GENERATION,
            DumpWireProfile::PostUnknownInstructionFallbacksV1,
            record,
            state,
        )
    }

    #[test]
    fn raw_record_validation_accepts_storage_order_and_rejects_duplicates() {
        let mut state = RawStreamValidationState::default();
        validate_test_record(&test_record(1), &mut state).unwrap();
        validate_test_record(&test_record(0), &mut state).unwrap();
        assert_eq!(state.transaction_keys.len(), 2);
        assert_eq!(state.blocks_by_id.len(), 1);

        let error = validate_test_record(&test_record(0), &mut state).unwrap_err();
        assert!(error.to_string().contains("repeats source transaction"));
    }

    #[test]
    fn raw_record_validation_enforces_source_coordinate_bounds() {
        let mut outside_slot = test_record(0);
        outside_slot.block.slot = TEST_EPOCH * TEST_SLOTS_PER_EPOCH - 1;
        assert!(
            validate_test_record(&outside_slot, &mut RawStreamValidationState::default())
                .unwrap_err()
                .to_string()
                .contains("outside")
        );

        let mut outside_block = test_record(0);
        outside_block.source_block_id = TEST_SLOTS_PER_EPOCH as u32;
        assert!(
            validate_test_record(&outside_block, &mut RawStreamValidationState::default())
                .unwrap_err()
                .to_string()
                .contains("source block ID")
        );

        let mut outside_transaction = test_record(0);
        outside_transaction.tx_index = outside_transaction.block.transaction_count;
        assert!(
            validate_test_record(
                &outside_transaction,
                &mut RawStreamValidationState::default()
            )
            .unwrap_err()
            .to_string()
            .contains("outside source transaction count")
        );

        let mut overflowing_signature = test_record(0);
        overflowing_signature.source_first_signature_ordinal = u64::MAX;
        assert!(
            validate_test_record(
                &overflowing_signature,
                &mut RawStreamValidationState::default()
            )
            .unwrap_err()
            .to_string()
            .contains("signature range overflows")
        );
    }

    #[test]
    fn raw_record_validation_rejects_conflicting_block_context() {
        let mut state = RawStreamValidationState::default();
        validate_test_record(&test_record(0), &mut state).unwrap();

        let mut conflict = test_record(1);
        conflict.block.block_time = Some(1_700_000_001);
        let error = validate_test_record(&conflict, &mut state).unwrap_err();
        assert!(error.to_string().contains("conflicting block context"));

        let mut state = RawStreamValidationState::default();
        validate_test_record(&test_record(0), &mut state).unwrap();
        let mut conflict = test_record(1);
        conflict.source_block_id = 4;
        let error = validate_test_record(&conflict, &mut state).unwrap_err();
        assert!(error.to_string().contains("conflicting source block IDs"));
    }

    #[test]
    fn raw_footer_bounds_selected_blocks_by_scanned_blocks() {
        let valid = TokenTransactionDumpFooter {
            epochs: 1,
            blocks_scanned: 2,
            transactions_scanned: 3,
            transactions_written: 2,
            ..TokenTransactionDumpFooter::default()
        };
        validate_raw_footer(TEST_EPOCH, TEST_SLOTS_PER_EPOCH, 2, valid, 2).unwrap();
        assert!(
            validate_raw_footer(TEST_EPOCH, TEST_SLOTS_PER_EPOCH, 3, valid, 2)
                .unwrap_err()
                .to_string()
                .contains("footer counters")
        );
    }
}
