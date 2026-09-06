use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES;
use blockzilla_primitives::bounded_wincode_leb128_config;
use blockzilla_token_transaction_dump::{
    DUMP_SCHEMA_VERSION, DiscoveredAccountList, DumpSourceBinding,
    consolidated_posting_projection::{
        ConsolidatedPostingProjectionScratch,
        PROGRAM_INSTRUCTION_SCOPE_DIRECT as PROJECTION_SCOPE_DIRECT,
        PROGRAM_INSTRUCTION_SCOPE_INNER as PROJECTION_SCOPE_INNER,
        project_consolidated_transaction_postings,
    },
    consolidated_reader::{BorrowedDumpRecord, BorrowedTransactionRecord, ConsolidatedFrameReader},
    visit_consolidated_spyx_owner_balance_history,
};
use sha2::{Digest, Sha256};

#[cfg(test)]
use blockzilla_token_transaction_dump::DumpWireProfile;

use crate::{
    builder::{
        DigestFileWriter, HashingReader, IO_BUFFER_BYTES, ObservedBlock, create_new_file,
        prepare_output, sync_directory, validate_block_context, validate_footer,
        validate_stream_header, validate_transaction_record,
    },
    index_format::{IndexFileBinding, TransactionCoordinate, hex_digest},
    owner_balance_history_format::{
        OWNER_BALANCE_DIRECTORY_FILE, OWNER_BALANCE_EVENTS_FILE,
        OWNER_BALANCE_HISTORY_MANIFEST_FILE, OWNER_BALANCE_HISTORY_SCHEMA_VERSION,
        OWNER_BALANCE_HISTORY_SEMANTIC_VERSION, OwnerBalanceEventRecord,
        OwnerBalanceHistoryFileHeader, OwnerBalanceHistoryFileKind, OwnerBalanceHistoryManifest,
        OwnerBalanceHistorySemanticHasher,
    },
    owner_postings_format::{
        OWNER_DIRECTORY_FILE, OWNER_POSTINGS_FILE, OWNER_POSTINGS_MANIFEST_FILE,
        OWNER_POSTINGS_SCHEMA_VERSION, OWNER_REPLAY_SEMANTIC_VERSION,
        OwnerBalanceHistoryManifestBinding, OwnerPostingsFileHeader, OwnerPostingsFileKind,
        OwnerPostingsManifest,
    },
    postings_format::{
        POSTINGS_DIRECTORY_RECORD_BYTES, POSTINGS_MANIFEST_FILE, PROGRAM_DIRECT_DIRECTORY_FILE,
        PROGRAM_DIRECT_POSTINGS_FILE, PROGRAM_DIRECTORY_FILE, PROGRAM_INNER_DIRECTORY_FILE,
        PROGRAM_INNER_POSTINGS_FILE, PROGRAM_INSTRUCTION_SCOPE_DIRECT,
        PROGRAM_INSTRUCTION_SCOPE_INNER, PROGRAM_INSTRUCTION_SCOPE_MASK, PROGRAM_POSTINGS_FILE,
        PostingRecord, PostingsDirectoryKind, PostingsDirectoryRecord, PostingsFileHeader,
        PostingsFileKind, PostingsManifest, PostingsSemanticHasher, PostingsSourceBinding,
        ProgramInstructionScope, ProgramPostingRecord, ProgramPostingsSemanticHasher,
        TARGET_ADDRESS_DIRECTORY_FILE, TARGET_ADDRESS_FLAG_MINT, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
        TARGET_ADDRESS_POSTINGS_FILE,
    },
    source::{SourceDump, load_source_dump, sha256_bytes},
};

const WORK_DIRECTORY: &str = ".postings-build-v1";
const RUNS_DIRECTORY: &str = "runs";
const BALANCE_HISTORY_RUNS_DIRECTORY: &str = "balance-history-runs";
const SORT_MEMORY_BYTES: usize = 256 << 20;
const MERGE_READER_BUFFER_BUDGET_BYTES: usize = 64 << 20;
const MIN_RUN_READER_BUFFER_BYTES: usize = 64 << 10;
const MAX_MERGE_RUNS: usize = MERGE_READER_BUFFER_BUDGET_BYTES / MIN_RUN_READER_BUFFER_BYTES;
const WORK_ROW_BYTES: usize = 16;
const BALANCE_HISTORY_WORK_ROW_BYTES: usize = 64;
const KEY_BYTES: usize = 32;
const MAX_TRANSACTION_TARGETS: usize = u8::MAX as usize + 1;
const PROGRESS_INTERVAL: u64 = 250_000;

const _: () = {
    assert!(PROJECTION_SCOPE_DIRECT == PROGRAM_INSTRUCTION_SCOPE_DIRECT);
    assert!(PROJECTION_SCOPE_INNER == PROGRAM_INSTRUCTION_SCOPE_INNER);
};

const FULL_SOURCE_TRANSACTION_SHA256: &str =
    "2849a8e8fbe7d8dbb553022355cfd33d0e50971166242534a398334e79d977de";
const FULL_TRANSACTIONS: u64 = 7_311_137;
const FULL_ACCOUNTS: u64 = 134_942;
const FULL_TARGET_KEYS: u64 = 134_943;
const FULL_TARGET_POSTINGS: u64 = 29_060_229;
const FULL_PROGRAM_KEYS: u64 = 1_070;
const FULL_PROGRAM_POSTINGS: u64 = 39_753_473;
const FULL_TOTAL_POSTINGS: u64 = 68_813_702;

#[derive(Debug, Clone)]
pub struct PostingsBuildConfig {
    pub dump: PathBuf,
    pub output: PathBuf,
    pub max_transactions: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PostingsBuildSummary {
    pub output: PathBuf,
    pub complete: bool,
    pub transactions: u64,
    pub transactions_with_target: u64,
    pub sort_runs: u64,
    pub total_postings: u64,
    pub target_keys: u64,
    pub target_postings: u64,
    pub program_keys: u64,
    pub program_postings: u64,
    pub program_direct_postings: u64,
    pub program_inner_postings: u64,
    pub transaction_bytes_scanned: u64,
    pub artifact_bytes: u64,
    pub target_address_semantic_sha256: String,
    pub program_semantic_sha256: String,
    pub program_direct_semantic_sha256: String,
    pub program_inner_semantic_sha256: String,
}

#[derive(Debug, Clone)]
pub struct OwnerPostingsBuildConfig {
    pub dump: PathBuf,
    pub output: PathBuf,
    pub max_transactions: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OwnerPostingsBuildSummary {
    pub output: PathBuf,
    pub complete: bool,
    pub transactions: u64,
    pub transactions_with_owner: u64,
    pub sort_runs: u64,
    pub owner_keys: u64,
    pub owner_postings: u64,
    pub balance_history_owner_keys: u64,
    pub balance_history_events: u64,
    pub transaction_bytes_scanned: u64,
    pub artifact_bytes: u64,
    pub replay_state_sha256: String,
    pub owner_semantic_sha256: String,
    pub balance_history_semantic_sha256: String,
}

pub fn build_postings(config: &PostingsBuildConfig) -> Result<PostingsBuildSummary> {
    if let Some(maximum) = config.max_transactions {
        ensure!(maximum != 0, "--max-transactions must be positive");
    }
    let source = load_source_dump(&config.dump)?;
    let accounts = load_and_validate_accounts(&source)?;
    let account_count =
        u64::try_from(accounts.accounts.len()).context("discovered account count exceeds u64")?;
    let complete = config.max_transactions.is_none();
    if complete {
        validate_full_source_gate(
            source.transaction_sha256,
            source.manifest.transactions,
            account_count,
        )?;
    }
    let registry_entries = u32::try_from(source.pubkeys).context("registry size exceeds u32")?;
    let TargetRegistry {
        entries: target_entries,
        membership: target_membership,
        mint_registry_id,
    } = map_targets_and_hash_registry(&source, &accounts, registry_entries)?;
    drop(accounts);
    let target_key_count =
        u64::try_from(target_entries.len()).context("target registry entry count exceeds u64")?;
    source.verify_file_identities()?;

    let output = prepare_output(&config.output, &source.root)?;
    let work = output.join(WORK_DIRECTORY);
    fs::create_dir(&work).with_context(|| format!("create {}", work.display()))?;
    let runs_root = work.join(RUNS_DIRECTORY);
    fs::create_dir(&runs_root).with_context(|| format!("create {}", runs_root.display()))?;

    let target_transactions = config
        .max_transactions
        .map_or(source.manifest.transactions, |maximum| {
            maximum.min(source.manifest.transactions)
        });
    let mut sorter = PostingSorter::new(&runs_root)?;
    let mut projection_scratch = ConsolidatedPostingProjectionScratch::new(registry_entries)?;
    let mut program_seen = DenseBitSet::new(registry_entries)?;
    let transaction_reader = HashingReader::new(source.transaction_handle.file());
    let mut transactions = ConsolidatedFrameReader::new(transaction_reader);
    validate_stream_header(&source, &mut transactions)?;

    let started = Instant::now();
    let mut transaction_count = 0u64;
    let mut signature_count = 0u64;
    let mut transactions_with_target = 0u64;
    let mut previous_coordinate = None;
    let mut previous_block = None::<ObservedBlock>;
    while transaction_count < target_transactions {
        let frame = transactions
            .next_frame()?
            .context("consolidated stream ended before the requested transaction count")?;
        let BorrowedDumpRecord::Transaction(record) = frame.record else {
            bail!("consolidated stream has a non-transaction before the requested count")
        };
        let coordinate = TransactionCoordinate {
            epoch: record.source_epoch,
            slot: record.block.slot,
            source_block_id: record.source_block_id,
            tx_index: record.tx_index,
        };
        ensure!(
            previous_coordinate.is_none_or(|previous| previous < coordinate),
            "consolidated transactions are not in canonical coordinate order"
        );
        previous_coordinate = Some(coordinate);
        validate_transaction_record(&source, &record, signature_count)?;
        validate_block_context(&record.block, coordinate, &mut previous_block)?;

        let emitted = emit_transaction_rows(
            &record,
            transaction_count,
            registry_entries,
            &target_membership,
            mint_registry_id,
            &mut projection_scratch,
            |row| {
                if row.kind() == WorkKind::Program {
                    program_seen.insert(row.registry_id())?;
                }
                sorter.push(row)
            },
        )?;
        if emitted.target_rows != 0 {
            transactions_with_target = transactions_with_target
                .checked_add(1)
                .context("transactions-with-target count overflow")?;
        }
        signature_count = signature_count
            .checked_add(u64::from(record.signature_count))
            .context("posting scan signature count overflow")?;
        transaction_count = transaction_count
            .checked_add(1)
            .context("posting scan transaction count overflow")?;
        if transaction_count.is_multiple_of(PROGRESS_INTERVAL)
            || transaction_count == target_transactions
        {
            report_scan_progress(
                transaction_count,
                target_transactions,
                sorter.total,
                transactions.logical_offset(),
                started,
            );
        }
    }
    ensure!(
        transaction_count == target_transactions,
        "posting transaction count differs from its target"
    );

    if complete {
        let footer_frame = transactions
            .next_frame()?
            .context("consolidated transaction stream has no footer")?;
        let BorrowedDumpRecord::Footer(footer) = footer_frame.record else {
            bail!("consolidated stream does not end after the manifest transaction count")
        };
        ensure!(
            transactions.next_frame()?.is_none(),
            "consolidated stream has records after its footer"
        );
        validate_footer(&source, footer, transaction_count, signature_count)?;
        ensure!(
            transactions.logical_offset() == source.transaction_bytes,
            "transaction stream byte length changed while it was scanned"
        );
        ensure!(
            transactions.get_ref().digest() == source.transaction_sha256,
            "transaction stream digest differs from its manifest"
        );
    }
    source.verify_file_identities()?;
    let transaction_bytes_scanned = transactions.logical_offset();
    drop(transactions);
    sorter.flush_run()?;

    let program_key_count = program_seen.count();
    drop(program_seen);
    drop(target_membership);
    if complete {
        validate_full_projection_gate(
            transaction_count,
            transactions_with_target,
            target_key_count,
            sorter.target_rows,
            program_key_count,
            sorter.program_rows,
            sorter.total,
        )?;
    }
    sorter.release_buffer()?;
    let sort_runs = u64::try_from(sorter.runs.len()).context("sort run count exceeds u64")?;
    let merged = merge_posting_runs(
        &sorter.runs,
        &work,
        complete,
        source.manifest_sha256,
        source.transaction_sha256,
        &target_entries,
        program_key_count,
        sorter.target_rows,
        sorter.program_rows,
        sorter.program_direct_rows,
        sorter.program_inner_rows,
        sorter.total,
        transaction_count,
    )?;
    ensure!(
        merged.target_keys == target_key_count
            && merged.program_keys == program_key_count
            && merged.target_postings == sorter.target_rows
            && merged.program_postings == sorter.program_rows
            && merged.program_direct_postings == sorter.program_direct_rows
            && merged.program_inner_postings == sorter.program_inner_rows,
        "merged posting counts differ from their scan counts"
    );
    if complete {
        ensure!(
            merged.nonempty_target_keys == FULL_TARGET_KEYS,
            "complete SPYx postings contain a target key without a posting"
        );
    }

    let source_binding = PostingsSourceBinding {
        manifest_file: blockzilla_token_transaction_dump::DUMP_MANIFEST_FILE.to_owned(),
        manifest_bytes: source.manifest_handle.len(),
        manifest_sha256: hex_digest(source.manifest_sha256),
        transaction_file: source.manifest.transaction_stream.clone(),
        transaction_bytes: source.transaction_bytes,
        transaction_sha256: hex_digest(source.transaction_sha256),
        registry_file: source
            .manifest
            .pubkey_registry
            .clone()
            .expect("validated source registry binding"),
        registry_bytes: source.registry_bytes,
        registry_sha256: hex_digest(source.registry_sha256),
        accounts_file: source
            .manifest
            .discovered_accounts
            .clone()
            .expect("validated source account binding"),
        accounts_bytes: source.accounts_bytes,
        accounts_sha256: hex_digest(source.accounts_sha256),
        transactions: source.manifest.transactions,
        pubkeys: source.pubkeys,
        accounts: account_count,
    };
    let target_semantic = hex_digest(merged.target_semantic_sha256);
    let program_semantic = hex_digest(merged.program_semantic_sha256);
    let program_direct_semantic = hex_digest(merged.program_direct_semantic_sha256);
    let program_inner_semantic = hex_digest(merged.program_inner_semantic_sha256);
    let manifest = PostingsManifest {
        schema_version: crate::postings_format::POSTINGS_SCHEMA_VERSION,
        artifact_kind: PostingsManifest::ARTIFACT_KIND.to_owned(),
        complete,
        canary_max_transactions: config.max_transactions,
        transactions: transaction_count,
        created_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time is before Unix epoch")?
            .as_secs(),
        source: source_binding.clone(),
        target_address_semantic_sha256: target_semantic.clone(),
        program_semantic_sha256: program_semantic.clone(),
        program_direct_semantic_sha256: program_direct_semantic.clone(),
        program_inner_semantic_sha256: program_inner_semantic.clone(),
        target_address_directory: merged.target_directory,
        target_address_postings: merged.target_postings_binding,
        program_directory: merged.program_directory,
        program_postings: merged.program_postings_binding,
        program_direct_directory: merged.program_direct_directory,
        program_direct_postings: merged.program_direct_postings_binding,
        program_inner_directory: merged.program_inner_directory,
        program_inner_postings: merged.program_inner_postings_binding,
    };
    manifest.validate()?;
    validate_manifest_headers(&manifest)?;
    let artifact_bytes = [
        manifest.target_address_directory.bytes,
        manifest.target_address_postings.bytes,
        manifest.program_directory.bytes,
        manifest.program_postings.bytes,
        manifest.program_direct_directory.bytes,
        manifest.program_direct_postings.bytes,
        manifest.program_inner_directory.bytes,
        manifest.program_inner_postings.bytes,
    ]
    .into_iter()
    .try_fold(0u64, |sum, bytes| sum.checked_add(bytes))
    .context("posting artifact byte count overflow")?;
    publish_postings(&output, &work, &runs_root, &sorter.runs, &manifest, &source)?;

    Ok(PostingsBuildSummary {
        output,
        complete,
        transactions: transaction_count,
        transactions_with_target,
        sort_runs,
        total_postings: sorter.total,
        target_keys: target_key_count,
        target_postings: sorter.target_rows,
        program_keys: program_key_count,
        program_postings: sorter.program_rows,
        program_direct_postings: sorter.program_direct_rows,
        program_inner_postings: sorter.program_inner_rows,
        transaction_bytes_scanned,
        artifact_bytes,
        target_address_semantic_sha256: target_semantic,
        program_semantic_sha256: program_semantic,
        program_direct_semantic_sha256: program_direct_semantic,
        program_inner_semantic_sha256: program_inner_semantic,
    })
}

pub fn build_owner_postings(
    config: &OwnerPostingsBuildConfig,
) -> Result<OwnerPostingsBuildSummary> {
    if let Some(maximum) = config.max_transactions {
        ensure!(maximum != 0, "--max-transactions must be positive");
    }
    let source = load_source_dump(&config.dump)?;
    let accounts = load_and_validate_accounts(&source)?;
    let account_count =
        u64::try_from(accounts.accounts.len()).context("discovered account count exceeds u64")?;
    drop(accounts);
    let complete = config.max_transactions.is_none();
    if complete {
        validate_full_source_gate(
            source.transaction_sha256,
            source.manifest.transactions,
            account_count,
        )?;
    }
    let registry_entries = u32::try_from(source.pubkeys).context("registry size exceeds u32")?;
    let output = prepare_output(&config.output, &source.root)?;
    let work = output.join(WORK_DIRECTORY);
    fs::create_dir(&work).with_context(|| format!("create {}", work.display()))?;
    let runs_root = work.join(RUNS_DIRECTORY);
    fs::create_dir(&runs_root).with_context(|| format!("create {}", runs_root.display()))?;
    let balance_history_runs_root = work.join(BALANCE_HISTORY_RUNS_DIRECTORY);
    fs::create_dir(&balance_history_runs_root)
        .with_context(|| format!("create {}", balance_history_runs_root.display()))?;

    // The two external sorters share the previous fixed 256 MiB sort budget.
    let mut sorter = PostingSorter::new_with_memory(&runs_root, SORT_MEMORY_BYTES / 2)?;
    let mut balance_history_sorter =
        BalanceHistorySorter::new(&balance_history_runs_root, SORT_MEMORY_BYTES / 2)?;
    let mut owner_seen = DenseBitSet::new(registry_entries)?;
    let mut balance_history_owner_seen = DenseBitSet::new(registry_entries)?;
    let mut transactions_with_owner = 0u64;
    let replay = visit_consolidated_spyx_owner_balance_history(
        &source.root,
        config.max_transactions,
        |transaction| {
            ensure!(
                transaction
                    .linked_owner_registry_ids
                    .windows(2)
                    .all(|pair| pair[0] < pair[1]),
                "strict replay owner IDs are not sorted and unique"
            );
            ensure!(
                transaction
                    .balance_changes
                    .windows(2)
                    .all(|pair| pair[0].owner_registry_id < pair[1].owner_registry_id),
                "strict replay owner balance changes are not sorted and unique"
            );
            if !transaction.linked_owner_registry_ids.is_empty() {
                transactions_with_owner = transactions_with_owner
                    .checked_add(1)
                    .context("transactions-with-owner count overflow")?;
            }
            for &registry_id in transaction.linked_owner_registry_ids {
                owner_seen.insert(registry_id)?;
                sorter.push(WorkRow::new(
                    WorkKind::Owner,
                    registry_id,
                    0,
                    transaction.transaction_id,
                )?)?;
            }
            for change in transaction.balance_changes {
                balance_history_owner_seen.insert(change.owner_registry_id)?;
                balance_history_sorter.push(BalanceHistoryWorkRow {
                    owner_registry_id: change.owner_registry_id,
                    event: OwnerBalanceEventRecord {
                        transaction_id: transaction.transaction_id,
                        slot: transaction.slot,
                        block_time: transaction.block_time,
                        raw_delta: change.raw_delta,
                        post_raw_balance: change.post_raw_balance,
                    },
                })?;
            }
            Ok(())
        },
    )?;
    ensure!(
        replay.transactions
            == config
                .max_transactions
                .map_or(source.manifest.transactions, |limit| {
                    limit.min(source.manifest.transactions)
                })
            && replay.manifest_sha256 == hex_digest(source.manifest_sha256)
            && replay.transaction_sha256 == hex_digest(source.transaction_sha256)
            && replay.registry_sha256 == hex_digest(source.registry_sha256)
            && replay.accounts_sha256 == hex_digest(source.accounts_sha256),
        "strict owner replay differs from the admitted source"
    );
    source.verify_file_identities()?;
    sorter.flush_run()?;
    balance_history_sorter.flush_run()?;
    ensure!(
        sorter.total == sorter.owner_rows,
        "owner sorter contains a non-owner posting"
    );
    let owner_key_count = owner_seen.count();
    let balance_history_owner_key_count = balance_history_owner_seen.count();
    drop(owner_seen);
    drop(balance_history_owner_seen);
    sorter.release_buffer()?;
    balance_history_sorter.release_buffer()?;
    let sort_runs = u64::try_from(sorter.runs.len()).context("sort run count exceeds u64")?;
    let merged = merge_owner_posting_runs(
        &sorter.runs,
        &work,
        complete,
        source.manifest_sha256,
        source.transaction_sha256,
        owner_key_count,
        sorter.owner_rows,
        replay.transactions,
    )?;
    let merged_balance_history = merge_owner_balance_history_runs(
        &balance_history_sorter.runs,
        &work,
        complete,
        source.manifest_sha256,
        source.transaction_sha256,
        balance_history_owner_key_count,
        balance_history_sorter.total,
        replay.transactions,
    )?;
    ensure!(
        merged.owner_keys == owner_key_count && merged.owner_postings == sorter.owner_rows,
        "merged owner counts differ from the strict replay scan"
    );
    ensure!(
        merged_balance_history.owner_keys == balance_history_owner_key_count
            && merged_balance_history.events == balance_history_sorter.total,
        "merged owner balance-history counts differ from the strict replay scan"
    );

    let source_binding = PostingsSourceBinding {
        manifest_file: blockzilla_token_transaction_dump::DUMP_MANIFEST_FILE.to_owned(),
        manifest_bytes: source.manifest_handle.len(),
        manifest_sha256: hex_digest(source.manifest_sha256),
        transaction_file: source.manifest.transaction_stream.clone(),
        transaction_bytes: source.transaction_bytes,
        transaction_sha256: hex_digest(source.transaction_sha256),
        registry_file: source
            .manifest
            .pubkey_registry
            .clone()
            .expect("validated source registry binding"),
        registry_bytes: source.registry_bytes,
        registry_sha256: hex_digest(source.registry_sha256),
        accounts_file: source
            .manifest
            .discovered_accounts
            .clone()
            .expect("validated source account binding"),
        accounts_bytes: source.accounts_bytes,
        accounts_sha256: hex_digest(source.accounts_sha256),
        transactions: source.manifest.transactions,
        pubkeys: source.pubkeys,
        accounts: account_count,
    };
    let owner_semantic_sha256 = hex_digest(merged.owner_semantic_sha256);
    let balance_history_semantic_sha256 = hex_digest(merged_balance_history.semantic_sha256);
    let created_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_secs();
    let balance_history_manifest = OwnerBalanceHistoryManifest {
        schema_version: OWNER_BALANCE_HISTORY_SCHEMA_VERSION,
        artifact_kind: OwnerBalanceHistoryManifest::ARTIFACT_KIND.to_owned(),
        complete,
        canary_max_transactions: config.max_transactions,
        transactions: replay.transactions,
        created_unix_seconds,
        source: source_binding.clone(),
        replay_semantic_version: OWNER_REPLAY_SEMANTIC_VERSION.to_owned(),
        replay_state_sha256: replay.replay_state_sha256.clone(),
        owner_postings_semantic_sha256: owner_semantic_sha256.clone(),
        history_semantic_version: OWNER_BALANCE_HISTORY_SEMANTIC_VERSION.to_owned(),
        history_semantic_sha256: balance_history_semantic_sha256.clone(),
        owner_directory: merged_balance_history.owner_directory,
        balance_events: merged_balance_history.balance_events,
    };
    balance_history_manifest.validate()?;
    validate_owner_balance_history_manifest_headers(&balance_history_manifest)?;
    let balance_history_manifest_bytes = pretty_json_bytes(&balance_history_manifest)?;
    let manifest = OwnerPostingsManifest {
        schema_version: OWNER_POSTINGS_SCHEMA_VERSION,
        artifact_kind: OwnerPostingsManifest::ARTIFACT_KIND.to_owned(),
        complete,
        canary_max_transactions: config.max_transactions,
        transactions: replay.transactions,
        created_unix_seconds,
        source: source_binding,
        replay_semantic_version: OWNER_REPLAY_SEMANTIC_VERSION.to_owned(),
        replay_state_sha256: replay.replay_state_sha256.clone(),
        owner_semantic_sha256: owner_semantic_sha256.clone(),
        owner_directory: merged.owner_directory,
        owner_postings: merged.owner_postings_binding,
        balance_history_manifest: Some(OwnerBalanceHistoryManifestBinding {
            file: OWNER_BALANCE_HISTORY_MANIFEST_FILE.to_owned(),
            bytes: u64::try_from(balance_history_manifest_bytes.len())
                .context("owner balance-history manifest byte count exceeds u64")?,
            sha256: hex_digest(Sha256::digest(&balance_history_manifest_bytes).into()),
        }),
    };
    manifest.validate()?;
    validate_owner_manifest_headers(&manifest)?;
    let artifact_bytes = manifest
        .owner_directory
        .bytes
        .checked_add(manifest.owner_postings.bytes)
        .and_then(|bytes| bytes.checked_add(balance_history_manifest.owner_directory.bytes))
        .and_then(|bytes| bytes.checked_add(balance_history_manifest.balance_events.bytes))
        .context("owner posting artifact byte count overflow")?;
    publish_owner_postings_with_history(
        &output,
        &work,
        &runs_root,
        &sorter.runs,
        &balance_history_runs_root,
        &balance_history_sorter.runs,
        &manifest,
        &balance_history_manifest,
        &balance_history_manifest_bytes,
        &source,
    )?;

    Ok(OwnerPostingsBuildSummary {
        output,
        complete,
        transactions: replay.transactions,
        transactions_with_owner,
        sort_runs,
        owner_keys: owner_key_count,
        owner_postings: sorter.owner_rows,
        balance_history_owner_keys: balance_history_owner_key_count,
        balance_history_events: balance_history_sorter.total,
        transaction_bytes_scanned: replay.transaction_bytes_scanned,
        artifact_bytes,
        replay_state_sha256: replay.replay_state_sha256,
        owner_semantic_sha256,
        balance_history_semantic_sha256,
    })
}

fn report_scan_progress(
    transactions: u64,
    target_transactions: u64,
    rows: u64,
    transaction_bytes: u64,
    started: Instant,
) {
    let elapsed = started.elapsed().as_secs_f64();
    let mib_per_second = if elapsed > 0.0 {
        transaction_bytes as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    let eta = if transactions == 0 {
        0.0
    } else {
        elapsed * target_transactions.saturating_sub(transactions) as f64 / transactions as f64
    };
    eprintln!(
        "postings scan: tx {transactions}/{target_transactions}, rows {rows}, transaction bytes {transaction_bytes}, {mib_per_second:.1} MiB/s, elapsed {elapsed:.0}s, ETA {eta:.0}s"
    );
}

fn load_and_validate_accounts(source: &SourceDump) -> Result<DiscoveredAccountList> {
    let bytes = source
        .accounts_handle
        .read_bounded(ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64)?;
    ensure!(
        sha256_bytes(&bytes) == source.accounts_sha256,
        "account digest differs from its manifest"
    );
    let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
        &bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .context("decode exact discovered-account list")?;
    let account_count =
        u64::try_from(accounts.accounts.len()).context("discovered account count exceeds u64")?;
    ensure!(
        accounts.schema_version == DUMP_SCHEMA_VERSION
            && accounts.mint == source.mint
            && source.manifest.discovered_account_count == Some(account_count)
            && accounts.anchor_position.slot == source.manifest.mint_slot
            && accounts.anchor_position.signature_count != 0
            && accounts
                .anchor_position
                .source_first_signature_ordinal
                .checked_add(u64::from(accounts.anchor_position.signature_count))
                .is_some(),
        "discovered-account header or anchor differs from its manifest"
    );
    validate_source_coordinate(
        source,
        accounts.anchor_position.epoch,
        accounts.anchor_position.slot,
        accounts.anchor_position.source_block_id,
    )?;
    ensure!(
        accounts
            .accounts
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "discovered accounts are not strictly sorted and unique"
    );
    for account in &accounts.accounts {
        ensure!(
            account.raw_pubkey != source.mint
                && account.first_creation.slot >= source.manifest.mint_slot,
            "discovered account has an invalid target key or creation slot"
        );
        validate_source_coordinate(
            source,
            account.first_creation.epoch,
            account.first_creation.slot,
            account.first_creation.source_block_id,
        )?;
    }
    source.accounts_handle.verify_identity("account list")?;
    Ok(accounts)
}

fn validate_source_coordinate(
    source: &SourceDump,
    epoch: u64,
    slot: u64,
    source_block_id: u32,
) -> Result<()> {
    let DumpSourceBinding::TrustedLocalSizesOnly { wire_profile, .. } =
        &source.manifest.source_binding;
    let profile = *wire_profile;
    source.validate_record_binding(epoch, slot, source_block_id, profile)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawTargetEntry {
    key: [u8; KEY_BYTES],
    flags: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TargetRegistryEntry {
    registry_id: u32,
    flags: u32,
}

struct TargetRegistry {
    entries: Vec<TargetRegistryEntry>,
    membership: DenseBitSet,
    mint_registry_id: u32,
}

fn map_targets_and_hash_registry(
    source: &SourceDump,
    accounts: &DiscoveredAccountList,
    registry_entries: u32,
) -> Result<TargetRegistry> {
    let target_capacity = accounts
        .accounts
        .len()
        .checked_add(1)
        .context("target key count overflow")?;
    let mut raw_targets = Vec::new();
    raw_targets
        .try_reserve_exact(target_capacity)
        .context("reserve sorted target keys")?;
    raw_targets.push(RawTargetEntry {
        key: source.mint,
        flags: TARGET_ADDRESS_FLAG_MINT,
    });
    raw_targets.extend(accounts.accounts.iter().map(|account| RawTargetEntry {
        key: account.raw_pubkey,
        flags: TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
    }));
    raw_targets.sort_unstable_by_key(|entry| entry.key);
    ensure!(
        raw_targets.windows(2).all(|pair| pair[0].key < pair[1].key),
        "target keys are not strictly sorted and unique"
    );

    let mut membership = DenseBitSet::new(registry_entries)?;
    let mut mapped = Vec::new();
    mapped
        .try_reserve_exact(raw_targets.len())
        .context("reserve mapped target keys")?;
    let mut target_index = 0usize;
    let mut mint_registry_id = None;
    let mut previous_registry_key = None;
    let mut hasher = Sha256::new();
    let mut registry = BufReader::with_capacity(IO_BUFFER_BYTES, source.registry_handle.file());
    for ordinal in 0..registry_entries {
        let mut key = [0u8; KEY_BYTES];
        registry
            .read_exact(&mut key)
            .context("read public-key registry row")?;
        hasher.update(key);
        ensure!(
            previous_registry_key.is_none_or(|previous| previous < key),
            "public-key registry is not strictly sorted and unique"
        );
        previous_registry_key = Some(key);
        if target_index < raw_targets.len() && raw_targets[target_index].key < key {
            bail!("one target key is absent from the public-key registry")
        }
        if target_index < raw_targets.len() && raw_targets[target_index].key == key {
            let registry_id = ordinal.checked_add(1).context("registry ID overflow")?;
            ensure!(
                membership.insert(registry_id)?,
                "one target key maps to a duplicate registry ID"
            );
            let flags = raw_targets[target_index].flags;
            if flags == TARGET_ADDRESS_FLAG_MINT {
                ensure!(
                    mint_registry_id.is_none(),
                    "target mint occurs more than once"
                );
                mint_registry_id = Some(registry_id);
            }
            mapped.push(TargetRegistryEntry { registry_id, flags });
            target_index += 1;
        }
    }
    let mut extra = [0u8; 1];
    ensure!(
        registry.read(&mut extra)? == 0,
        "public-key registry has bytes after its declared rows"
    );
    ensure!(
        target_index == raw_targets.len(),
        "one target key is absent from the public-key registry"
    );
    let observed_sha256: [u8; 32] = hasher.finalize().into();
    ensure!(
        observed_sha256 == source.registry_sha256,
        "public-key registry digest differs from its manifest"
    );
    ensure!(
        mapped
            .windows(2)
            .all(|pair| pair[0].registry_id < pair[1].registry_id),
        "mapped target registry IDs are not strictly sorted"
    );
    source
        .registry_handle
        .verify_identity("public-key registry")?;
    Ok(TargetRegistry {
        entries: mapped,
        membership,
        mint_registry_id: mint_registry_id.context("target mint is absent from the registry")?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmittedTransactionRows {
    target_rows: u16,
    program_rows: u16,
}

fn emit_transaction_rows(
    record: &BorrowedTransactionRecord<'_>,
    transaction_ordinal: u64,
    registry_entries: u32,
    target_membership: &DenseBitSet,
    mint_registry_id: u32,
    projection_scratch: &mut ConsolidatedPostingProjectionScratch,
    mut emit: impl FnMut(WorkRow) -> Result<()>,
) -> Result<EmittedTransactionRows> {
    let projection =
        project_consolidated_transaction_postings(record, registry_entries, projection_scratch)?;
    ensure!(
        projection.resolved_account_registry_ids.len() <= MAX_TRANSACTION_TARGETS,
        "resolved transaction target candidates exceed fixed scratch"
    );
    let mut target_ids = [0u32; MAX_TRANSACTION_TARGETS];
    let mut candidate_count = 0usize;
    for registry_id in projection.resolved_account_registry_ids {
        if target_membership.contains(*registry_id) {
            target_ids[candidate_count] = *registry_id;
            candidate_count += 1;
        }
    }
    target_ids[..candidate_count].sort_unstable();
    let mut target_count = 0usize;
    for read in 0..candidate_count {
        let registry_id = target_ids[read];
        if target_count == 0 || target_ids[target_count - 1] != registry_id {
            target_ids[target_count] = registry_id;
            target_count += 1;
        }
    }
    for registry_id in &target_ids[..target_count] {
        let flags = if *registry_id == mint_registry_id {
            TARGET_ADDRESS_FLAG_MINT
        } else {
            TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT
        };
        emit(WorkRow::new(
            WorkKind::TargetAddress,
            *registry_id,
            flags,
            transaction_ordinal,
        )?)?;
    }
    for program in projection.program_postings {
        emit(WorkRow::new(
            WorkKind::Program,
            program.registry_id,
            u32::from(program.instruction_scope_mask),
            transaction_ordinal,
        )?)?;
    }
    Ok(EmittedTransactionRows {
        target_rows: u16::try_from(target_count).expect("target count is bounded by 256"),
        program_rows: u16::try_from(projection.program_postings.len())
            .expect("program count is bounded by 256"),
    })
}

#[derive(Debug, Clone)]
struct DenseBitSet {
    entries: u32,
    words: Vec<u64>,
    count: u64,
}

impl DenseBitSet {
    fn new(entries: u32) -> Result<Self> {
        ensure!(entries != 0, "dense bitset registry size is zero");
        let words = usize::try_from(u64::from(entries).div_ceil(u64::BITS.into()))
            .context("dense bitset word count exceeds usize")?;
        let mut storage = Vec::new();
        storage
            .try_reserve_exact(words)
            .context("reserve dense registry bitset")?;
        storage.resize(words, 0);
        Ok(Self {
            entries,
            words: storage,
            count: 0,
        })
    }

    fn insert(&mut self, registry_id: u32) -> Result<bool> {
        ensure!(
            registry_id != 0 && registry_id <= self.entries,
            "registry ID is outside the dense bitset"
        );
        let index = usize::try_from(registry_id - 1).expect("u32 registry index fits usize");
        let word = &mut self.words[index / u64::BITS as usize];
        let mask = 1u64 << (index % u64::BITS as usize);
        if *word & mask != 0 {
            return Ok(false);
        }
        *word |= mask;
        self.count = self.count.checked_add(1).context("bitset count overflow")?;
        Ok(true)
    }

    fn contains(&self, registry_id: u32) -> bool {
        if registry_id == 0 || registry_id > self.entries {
            return false;
        }
        let index = usize::try_from(registry_id - 1).expect("u32 registry index fits usize");
        self.words[index / u64::BITS as usize] & (1u64 << (index % u64::BITS as usize)) != 0
    }

    const fn count(&self) -> u64 {
        self.count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WorkKind {
    TargetAddress,
    Program,
    Owner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WorkRow {
    sort_key: u64,
    posting_key: u64,
}

impl WorkRow {
    const KIND_SHIFT: u32 = 32;
    const TRANSACTION_SHIFT: u32 = 2;
    const FLAGS_MASK: u64 = 0b11;
    const KNOWN_SORT_BITS: u64 = (1u64 << 34) - 1;

    fn new(kind: WorkKind, registry_id: u32, flags: u32, transaction_ordinal: u64) -> Result<Self> {
        ensure!(registry_id != 0, "posting work row registry ID is zero");
        match kind {
            WorkKind::TargetAddress => ensure!(
                flags == TARGET_ADDRESS_FLAG_MINT || flags == TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                "target posting work row has invalid flags"
            ),
            WorkKind::Program => ensure!(
                flags != 0 && flags & !u32::from(PROGRAM_INSTRUCTION_SCOPE_MASK) == 0,
                "program posting work row has an invalid instruction scope"
            ),
            WorkKind::Owner => ensure!(flags == 0, "owner posting work row has flags"),
        }
        let kind_key = match kind {
            WorkKind::TargetAddress => 0u64,
            WorkKind::Program => 1u64,
            WorkKind::Owner => 2u64,
        };
        ensure!(
            transaction_ordinal <= (u64::MAX >> Self::TRANSACTION_SHIFT),
            "posting work row transaction ordinal exceeds 62 bits"
        );
        Ok(Self {
            sort_key: (kind_key << Self::KIND_SHIFT) | u64::from(registry_id),
            posting_key: (transaction_ordinal << Self::TRANSACTION_SHIFT) | u64::from(flags),
        })
    }

    fn encode(self) -> [u8; WORK_ROW_BYTES] {
        let mut bytes = [0u8; WORK_ROW_BYTES];
        bytes[0..8].copy_from_slice(&self.sort_key.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.posting_key.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8; WORK_ROW_BYTES]) -> Result<Self> {
        let row = Self {
            sort_key: u64::from_le_bytes(bytes[0..8].try_into().expect("fixed work-key bytes")),
            posting_key: u64::from_le_bytes(
                bytes[8..16].try_into().expect("fixed work-ordinal bytes"),
            ),
        };
        ensure!(
            row.sort_key & !Self::KNOWN_SORT_BITS == 0,
            "posting work row has reserved sort-key bits"
        );
        ensure!(
            row.sort_key >> Self::KIND_SHIFT <= 2,
            "posting work row has an unknown kind"
        );
        WorkRow::new(
            row.kind(),
            row.registry_id(),
            row.flags(),
            row.transaction_ordinal(),
        )
        .and_then(|canonical| {
            ensure!(canonical == row, "posting work row key is not canonical");
            Ok(row)
        })
    }

    fn kind(self) -> WorkKind {
        match self.sort_key >> Self::KIND_SHIFT {
            0 => WorkKind::TargetAddress,
            1 => WorkKind::Program,
            2 => WorkKind::Owner,
            _ => unreachable!("validated posting work kind"),
        }
    }

    const fn registry_id(self) -> u32 {
        (self.sort_key & u32::MAX as u64) as u32
    }

    const fn flags(self) -> u32 {
        (self.posting_key & Self::FLAGS_MASK) as u32
    }

    const fn transaction_ordinal(self) -> u64 {
        self.posting_key >> Self::TRANSACTION_SHIFT
    }
}

struct PostingSorter {
    root: PathBuf,
    capacity: usize,
    rows: Vec<WorkRow>,
    runs: Vec<PostingRunBinding>,
    total: u64,
    target_rows: u64,
    program_rows: u64,
    program_direct_rows: u64,
    program_inner_rows: u64,
    owner_rows: u64,
}

impl PostingSorter {
    fn new(root: &Path) -> Result<Self> {
        Self::new_with_memory(root, SORT_MEMORY_BYTES)
    }

    fn new_with_memory(root: &Path, memory_bytes: usize) -> Result<Self> {
        ensure!(
            std::mem::size_of::<WorkRow>() == WORK_ROW_BYTES,
            "posting work row memory size differs"
        );
        let capacity = memory_bytes / WORK_ROW_BYTES;
        ensure!(capacity != 0, "posting sort memory cannot hold one row");
        let mut rows = Vec::new();
        rows.try_reserve_exact(capacity)
            .context("reserve bounded posting sort buffer")?;
        Ok(Self {
            root: root.to_path_buf(),
            capacity,
            rows,
            runs: Vec::new(),
            total: 0,
            target_rows: 0,
            program_rows: 0,
            program_direct_rows: 0,
            program_inner_rows: 0,
            owner_rows: 0,
        })
    }

    fn push(&mut self, row: WorkRow) -> Result<()> {
        if self.rows.len() == self.capacity {
            self.flush_run()?;
        }
        self.rows.push(row);
        ensure!(
            self.rows.len() <= self.capacity,
            "posting sort buffer exceeds its memory bound"
        );
        self.total = self.total.checked_add(1).context("posting row overflow")?;
        let count = match row.kind() {
            WorkKind::TargetAddress => &mut self.target_rows,
            WorkKind::Program => &mut self.program_rows,
            WorkKind::Owner => &mut self.owner_rows,
        };
        *count = count.checked_add(1).context("posting kind row overflow")?;
        if row.kind() == WorkKind::Program {
            if ProgramInstructionScope::Direct.includes(row.flags() as u8) {
                self.program_direct_rows = self
                    .program_direct_rows
                    .checked_add(1)
                    .context("direct program posting row overflow")?;
            }
            if ProgramInstructionScope::Inner.includes(row.flags() as u8) {
                self.program_inner_rows = self
                    .program_inner_rows
                    .checked_add(1)
                    .context("inner program posting row overflow")?;
            }
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_unstable();
        ensure!(
            self.rows.windows(2).all(|pair| pair[0] < pair[1]),
            "posting sort run contains a duplicate row"
        );
        let path = self.root.join(format!("run-{:06}.bin", self.runs.len()));
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(&path)?);
        let mut hasher = Sha256::new();
        for row in &self.rows {
            let encoded = row.encode();
            writer.write_all(&encoded)?;
            hasher.update(encoded);
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        eprintln!(
            "postings sort: flushed run {} with {} rows",
            self.runs.len() + 1,
            self.rows.len()
        );
        let rows = u64::try_from(self.rows.len()).context("posting run row count exceeds u64")?;
        self.rows.clear();
        self.runs.push(PostingRunBinding {
            path,
            rows,
            sha256: hasher.finalize().into(),
        });
        Ok(())
    }

    fn release_buffer(&mut self) -> Result<()> {
        ensure!(
            self.rows.is_empty(),
            "posting sort buffer is not empty before merge"
        );
        self.rows = Vec::new();
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PostingRunBinding {
    path: PathBuf,
    rows: u64,
    sha256: [u8; 32],
}

struct PostingRunReader {
    reader: BufReader<File>,
    remaining: u64,
    current: Option<WorkRow>,
    hasher: Sha256,
    expected_sha256: [u8; 32],
    verified: bool,
}

impl PostingRunReader {
    fn open(binding: &PostingRunBinding, buffer_bytes: usize) -> Result<Self> {
        let file = File::open(&binding.path)
            .with_context(|| format!("open posting run {}", binding.path.display()))?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes
                == binding
                    .rows
                    .checked_mul(WORK_ROW_BYTES as u64)
                    .context("posting run byte count overflow")?,
            "posting sort run byte count differs from its binding"
        );
        let mut this = Self {
            reader: BufReader::with_capacity(buffer_bytes, file),
            remaining: binding.rows,
            current: None,
            hasher: Sha256::new(),
            expected_sha256: binding.sha256,
            verified: false,
        };
        this.advance()?;
        Ok(this)
    }

    fn advance(&mut self) -> Result<()> {
        if self.remaining == 0 {
            if !self.verified {
                ensure!(
                    <[u8; 32]>::from(self.hasher.clone().finalize()) == self.expected_sha256,
                    "posting sort run digest differs from its binding"
                );
                self.verified = true;
            }
            self.current = None;
            return Ok(());
        }
        let mut bytes = [0u8; WORK_ROW_BYTES];
        self.reader.read_exact(&mut bytes)?;
        self.hasher.update(bytes);
        self.current = Some(WorkRow::decode(&bytes)?);
        self.remaining -= 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct BalanceHistoryWorkRow {
    owner_registry_id: u32,
    event: OwnerBalanceEventRecord,
}

impl BalanceHistoryWorkRow {
    const fn key(self) -> (u32, u64) {
        (self.owner_registry_id, self.event.transaction_id)
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.owner_registry_id != 0,
            "owner balance-history work row has registry ID zero"
        );
        self.event.validate()
    }

    fn encode(self) -> Result<[u8; BALANCE_HISTORY_WORK_ROW_BYTES]> {
        self.validate()?;
        let mut bytes = [0u8; BALANCE_HISTORY_WORK_ROW_BYTES];
        bytes[0..4].copy_from_slice(&self.owner_registry_id.to_le_bytes());
        bytes[8..].copy_from_slice(&self.event.encode()?);
        Ok(bytes)
    }

    fn decode(bytes: &[u8; BALANCE_HISTORY_WORK_ROW_BYTES]) -> Result<Self> {
        ensure!(
            bytes[4..8].iter().all(|byte| *byte == 0),
            "owner balance-history work row has non-zero reserved bytes"
        );
        let row = Self {
            owner_registry_id: u32::from_le_bytes(
                bytes[0..4]
                    .try_into()
                    .expect("fixed owner registry ID range"),
            ),
            event: OwnerBalanceEventRecord::decode(&bytes[8..])?,
        };
        row.validate()?;
        Ok(row)
    }
}

impl PartialEq for BalanceHistoryWorkRow {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for BalanceHistoryWorkRow {}

impl PartialOrd for BalanceHistoryWorkRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BalanceHistoryWorkRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

#[derive(Debug, Clone)]
struct BalanceHistoryRunBinding {
    path: PathBuf,
    rows: u64,
    sha256: [u8; 32],
}

struct BalanceHistorySorter {
    root: PathBuf,
    capacity: usize,
    rows: Vec<BalanceHistoryWorkRow>,
    runs: Vec<BalanceHistoryRunBinding>,
    total: u64,
}

impl BalanceHistorySorter {
    fn new(root: &Path, memory_bytes: usize) -> Result<Self> {
        let capacity = memory_bytes / BALANCE_HISTORY_WORK_ROW_BYTES;
        ensure!(
            capacity != 0,
            "owner balance-history sort memory cannot hold one row"
        );
        let mut rows = Vec::new();
        rows.try_reserve_exact(capacity)
            .context("reserve bounded owner balance-history sort buffer")?;
        Ok(Self {
            root: root.to_path_buf(),
            capacity,
            rows,
            runs: Vec::new(),
            total: 0,
        })
    }

    fn push(&mut self, row: BalanceHistoryWorkRow) -> Result<()> {
        row.validate()?;
        if self.rows.len() == self.capacity {
            self.flush_run()?;
        }
        self.rows.push(row);
        ensure!(
            self.rows.len() <= self.capacity,
            "owner balance-history sort buffer exceeds its memory bound"
        );
        self.total = self
            .total
            .checked_add(1)
            .context("owner balance-history row count overflow")?;
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_unstable();
        ensure!(
            self.rows
                .windows(2)
                .all(|pair| pair[0].key() < pair[1].key()),
            "owner balance-history sort run contains a duplicate owner transaction"
        );
        let path = self.root.join(format!("run-{:06}.bin", self.runs.len()));
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(&path)?);
        let mut hasher = Sha256::new();
        for row in &self.rows {
            let encoded = row.encode()?;
            writer.write_all(&encoded)?;
            hasher.update(encoded);
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let rows = u64::try_from(self.rows.len())
            .context("owner balance-history run row count exceeds u64")?;
        self.rows.clear();
        self.runs.push(BalanceHistoryRunBinding {
            path,
            rows,
            sha256: hasher.finalize().into(),
        });
        Ok(())
    }

    fn release_buffer(&mut self) -> Result<()> {
        ensure!(
            self.rows.is_empty(),
            "owner balance-history sort buffer is not empty before merge"
        );
        self.rows = Vec::new();
        Ok(())
    }
}

struct BalanceHistoryRunReader {
    reader: BufReader<File>,
    remaining: u64,
    current: Option<BalanceHistoryWorkRow>,
    hasher: Sha256,
    expected_sha256: [u8; 32],
    verified: bool,
}

impl BalanceHistoryRunReader {
    fn open(binding: &BalanceHistoryRunBinding, buffer_bytes: usize) -> Result<Self> {
        let file = File::open(&binding.path).with_context(|| {
            format!("open owner balance-history run {}", binding.path.display())
        })?;
        ensure!(
            file.metadata()?.len()
                == binding
                    .rows
                    .checked_mul(BALANCE_HISTORY_WORK_ROW_BYTES as u64)
                    .context("owner balance-history run byte count overflow")?,
            "owner balance-history run byte count differs from its binding"
        );
        let mut reader = Self {
            reader: BufReader::with_capacity(buffer_bytes, file),
            remaining: binding.rows,
            current: None,
            hasher: Sha256::new(),
            expected_sha256: binding.sha256,
            verified: false,
        };
        reader.advance()?;
        Ok(reader)
    }

    fn advance(&mut self) -> Result<()> {
        if self.remaining == 0 {
            if !self.verified {
                ensure!(
                    <[u8; 32]>::from(self.hasher.clone().finalize()) == self.expected_sha256,
                    "owner balance-history run digest differs from its binding"
                );
                self.verified = true;
            }
            self.current = None;
            return Ok(());
        }
        let mut bytes = [0u8; BALANCE_HISTORY_WORK_ROW_BYTES];
        self.reader.read_exact(&mut bytes)?;
        self.hasher.update(bytes);
        self.current = Some(BalanceHistoryWorkRow::decode(&bytes)?);
        self.remaining -= 1;
        Ok(())
    }
}

fn merge_sorted_balance_history_runs(
    runs: &[BalanceHistoryRunBinding],
    expected_rows: u64,
    source_transaction_count: u64,
    mut visit: impl FnMut(BalanceHistoryWorkRow) -> Result<()>,
) -> Result<()> {
    ensure!(
        runs.len() <= MAX_MERGE_RUNS,
        "owner balance-history sort produced too many runs for the bounded merge"
    );
    let reader_buffer_bytes = if runs.is_empty() {
        MIN_RUN_READER_BUFFER_BYTES
    } else {
        (MERGE_READER_BUFFER_BUDGET_BYTES / runs.len())
            .clamp(MIN_RUN_READER_BUFFER_BYTES, IO_BUFFER_BYTES)
    };
    let mut readers = runs
        .iter()
        .map(|run| BalanceHistoryRunReader::open(run, reader_buffer_bytes))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(BalanceHistoryWorkRow, usize)>>::new();
    for (index, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.current {
            heap.push(Reverse((row, index)));
        }
    }
    let mut previous_key = None;
    let mut written = 0u64;
    while let Some(Reverse((row, reader_index))) = heap.pop() {
        ensure!(
            row.event.transaction_id < source_transaction_count,
            "owner balance-history transaction ID is outside the indexed prefix"
        );
        ensure!(
            previous_key.is_none_or(|key| key < row.key()),
            "merged owner balance history is not strictly sorted and unique"
        );
        visit(row)?;
        previous_key = Some(row.key());
        written = written
            .checked_add(1)
            .context("merged owner balance-history count overflow")?;
        readers[reader_index].advance()?;
        if let Some(next) = readers[reader_index].current {
            heap.push(Reverse((next, reader_index)));
        }
    }
    ensure!(
        written == expected_rows,
        "merged owner balance-history count differs from its scan count"
    );
    Ok(())
}

fn merge_sorted_runs(
    runs: &[PostingRunBinding],
    expected_rows: u64,
    source_transaction_count: u64,
    mut visit: impl FnMut(WorkRow) -> Result<()>,
) -> Result<()> {
    ensure!(
        runs.len() <= MAX_MERGE_RUNS,
        "posting sort produced too many runs for the bounded merge"
    );
    let reader_buffer_bytes = if runs.is_empty() {
        MIN_RUN_READER_BUFFER_BYTES
    } else {
        (MERGE_READER_BUFFER_BUDGET_BYTES / runs.len())
            .clamp(MIN_RUN_READER_BUFFER_BYTES, IO_BUFFER_BYTES)
    };
    let mut readers = runs
        .iter()
        .map(|run| PostingRunReader::open(run, reader_buffer_bytes))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(WorkRow, usize)>>::new();
    for (index, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.current {
            heap.push(Reverse((row, index)));
        }
    }
    let started = Instant::now();
    let mut previous = None;
    let mut written = 0u64;
    eprintln!(
        "postings sort: merging {} runs and {expected_rows} rows",
        runs.len()
    );
    while let Some(Reverse((row, reader_index))) = heap.pop() {
        ensure!(
            row.transaction_ordinal() < source_transaction_count,
            "posting work row transaction ordinal is outside the indexed prefix"
        );
        ensure!(
            previous.is_none_or(|value| value < row),
            "merged postings are not strictly sorted and unique"
        );
        visit(row)?;
        previous = Some(row);
        written = written
            .checked_add(1)
            .context("merged posting count overflow")?;
        if written.is_multiple_of(PROGRESS_INTERVAL) || written == expected_rows {
            report_merge_progress(written, expected_rows, started);
        }
        readers[reader_index].advance()?;
        if let Some(next) = readers[reader_index].current {
            heap.push(Reverse((next, reader_index)));
        }
    }
    ensure!(
        written == expected_rows,
        "merged posting count differs from its scan count"
    );
    Ok(())
}

fn report_merge_progress(rows: u64, expected_rows: u64, started: Instant) {
    let elapsed = started.elapsed().as_secs_f64();
    let bytes = rows.saturating_mul(WORK_ROW_BYTES as u64);
    let mib_per_second = if elapsed > 0.0 {
        bytes as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    let eta = if rows == 0 {
        0.0
    } else {
        elapsed * expected_rows.saturating_sub(rows) as f64 / rows as f64
    };
    eprintln!(
        "postings merge: rows {rows}/{expected_rows}, input bytes {bytes}, {mib_per_second:.1} MiB/s, elapsed {elapsed:.0}s, ETA {eta:.0}s"
    );
}

#[derive(Debug, Clone, Copy)]
struct DirectoryGroup {
    kind: WorkKind,
    registry_id: u32,
    flags: u32,
    first_posting_row: u64,
    posting_count: u64,
    direct_first_posting_row: u64,
    direct_posting_count: u64,
    inner_first_posting_row: u64,
    inner_posting_count: u64,
}

impl DirectoryGroup {
    fn matches(self, row: WorkRow) -> bool {
        self.kind == row.kind()
            && self.registry_id == row.registry_id()
            && (self.kind != WorkKind::TargetAddress || self.flags == row.flags())
    }
}

struct MergedArtifacts {
    target_directory: IndexFileBinding,
    target_postings_binding: IndexFileBinding,
    program_directory: IndexFileBinding,
    program_postings_binding: IndexFileBinding,
    program_direct_directory: IndexFileBinding,
    program_direct_postings_binding: IndexFileBinding,
    program_inner_directory: IndexFileBinding,
    program_inner_postings_binding: IndexFileBinding,
    target_semantic_sha256: [u8; 32],
    program_semantic_sha256: [u8; 32],
    program_direct_semantic_sha256: [u8; 32],
    program_inner_semantic_sha256: [u8; 32],
    target_keys: u64,
    nonempty_target_keys: u64,
    target_postings: u64,
    program_keys: u64,
    program_postings: u64,
    program_direct_postings: u64,
    program_inner_postings: u64,
}

struct MergedOwnerArtifacts {
    owner_directory: IndexFileBinding,
    owner_postings_binding: IndexFileBinding,
    owner_semantic_sha256: [u8; 32],
    owner_keys: u64,
    owner_postings: u64,
}

struct MergedOwnerBalanceHistoryArtifacts {
    owner_directory: IndexFileBinding,
    balance_events: IndexFileBinding,
    semantic_sha256: [u8; 32],
    owner_keys: u64,
    events: u64,
}

#[allow(clippy::too_many_arguments)]
fn merge_owner_balance_history_runs(
    runs: &[BalanceHistoryRunBinding],
    work: &Path,
    complete: bool,
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
    expected_owner_keys: u64,
    expected_events: u64,
    source_transaction_count: u64,
) -> Result<MergedOwnerBalanceHistoryArtifacts> {
    let mut directory = create_owner_balance_history_writer(
        work,
        OwnerBalanceHistoryFileKind::Directory,
        complete,
        expected_owner_keys,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut events = create_owner_balance_history_writer(
        work,
        OwnerBalanceHistoryFileKind::Events,
        complete,
        expected_events,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut semantic = OwnerBalanceHistorySemanticHasher::new(expected_events);
    let mut current_registry_id = None::<u32>;
    let mut current_first_event = 0u64;
    let mut current_event_count = 0u64;
    let mut owner_key_count = 0u64;
    let mut event_count = 0u64;

    merge_sorted_balance_history_runs(runs, expected_events, source_transaction_count, |row| {
        if current_registry_id.is_some_and(|registry_id| registry_id != row.owner_registry_id) {
            let registry_id = current_registry_id
                .take()
                .expect("checked current owner balance-history group");
            directory.write_all(
                &PostingsDirectoryRecord {
                    registry_id,
                    flags: 0,
                    first_posting_row: current_first_event,
                    posting_count: current_event_count,
                }
                .encode(PostingsDirectoryKind::Owner)?,
            )?;
            owner_key_count = owner_key_count
                .checked_add(1)
                .context("owner balance-history directory count overflow")?;
            current_first_event = event_count;
            current_event_count = 0;
        }
        current_registry_id.get_or_insert(row.owner_registry_id);
        events.write_all(&row.event.encode()?)?;
        semantic.update(row.owner_registry_id, row.event)?;
        event_count = event_count
            .checked_add(1)
            .context("owner balance-history event count overflow")?;
        current_event_count = current_event_count
            .checked_add(1)
            .context("owner balance-history owner range count overflow")?;
        Ok(())
    })?;
    if let Some(registry_id) = current_registry_id {
        directory.write_all(
            &PostingsDirectoryRecord {
                registry_id,
                flags: 0,
                first_posting_row: current_first_event,
                posting_count: current_event_count,
            }
            .encode(PostingsDirectoryKind::Owner)?,
        )?;
        owner_key_count = owner_key_count
            .checked_add(1)
            .context("owner balance-history directory count overflow")?;
    }
    ensure!(
        owner_key_count == expected_owner_keys && event_count == expected_events,
        "merged owner balance-history directory or event count differs"
    );
    let semantic_sha256 = semantic.finish()?;
    let owner_directory = directory.finish(
        OWNER_BALANCE_DIRECTORY_FILE,
        owner_key_count,
        POSTINGS_DIRECTORY_RECORD_BYTES as u16,
    )?;
    let balance_events = events.finish(
        OWNER_BALANCE_EVENTS_FILE,
        event_count,
        crate::owner_balance_history_format::OWNER_BALANCE_EVENT_RECORD_BYTES as u16,
    )?;
    Ok(MergedOwnerBalanceHistoryArtifacts {
        owner_directory,
        balance_events,
        semantic_sha256,
        owner_keys: owner_key_count,
        events: event_count,
    })
}

fn create_owner_balance_history_writer(
    work: &Path,
    kind: OwnerBalanceHistoryFileKind,
    complete: bool,
    records: u64,
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
) -> Result<DigestFileWriter> {
    let path = work.join(format!("{}.partial", kind.file_name()));
    let mut writer = DigestFileWriter::create(&path)?;
    writer.write_all(
        &OwnerBalanceHistoryFileHeader {
            kind,
            complete,
            record_count: records,
            source_manifest_sha256,
            source_transaction_sha256,
        }
        .encode(),
    )?;
    Ok(writer)
}

#[allow(clippy::too_many_arguments)]
fn merge_owner_posting_runs(
    runs: &[PostingRunBinding],
    work: &Path,
    complete: bool,
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
    expected_owner_keys: u64,
    expected_owner_postings: u64,
    source_transaction_count: u64,
) -> Result<MergedOwnerArtifacts> {
    let mut directory = create_owner_posting_writer(
        work,
        OwnerPostingsFileKind::Directory,
        complete,
        expected_owner_keys,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut postings = create_owner_posting_writer(
        work,
        OwnerPostingsFileKind::Postings,
        complete,
        expected_owner_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut semantic =
        PostingsSemanticHasher::new(PostingsDirectoryKind::Owner, expected_owner_postings);
    let mut current_registry_id = None::<u32>;
    let mut current_first_posting = 0u64;
    let mut current_posting_count = 0u64;
    let mut owner_key_count = 0u64;
    let mut owner_posting_count = 0u64;

    merge_sorted_runs(
        runs,
        expected_owner_postings,
        source_transaction_count,
        |row| {
            ensure!(
                row.kind() == WorkKind::Owner && row.flags() == 0,
                "owner-only merge received another posting kind"
            );
            if current_registry_id.is_some_and(|registry_id| registry_id != row.registry_id()) {
                let registry_id = current_registry_id
                    .take()
                    .expect("checked current owner directory group");
                directory.write_all(
                    &PostingsDirectoryRecord {
                        registry_id,
                        flags: 0,
                        first_posting_row: current_first_posting,
                        posting_count: current_posting_count,
                    }
                    .encode(PostingsDirectoryKind::Owner)?,
                )?;
                owner_key_count = owner_key_count
                    .checked_add(1)
                    .context("owner directory count overflow")?;
                current_first_posting = owner_posting_count;
                current_posting_count = 0;
            }
            current_registry_id.get_or_insert(row.registry_id());
            postings.write_all(
                &PostingRecord {
                    transaction_ordinal: row.transaction_ordinal(),
                }
                .encode(),
            )?;
            semantic.update(row.registry_id(), 0, row.transaction_ordinal())?;
            owner_posting_count = owner_posting_count
                .checked_add(1)
                .context("owner posting count overflow")?;
            current_posting_count = current_posting_count
                .checked_add(1)
                .context("owner posting range count overflow")?;
            Ok(())
        },
    )?;
    if let Some(registry_id) = current_registry_id {
        directory.write_all(
            &PostingsDirectoryRecord {
                registry_id,
                flags: 0,
                first_posting_row: current_first_posting,
                posting_count: current_posting_count,
            }
            .encode(PostingsDirectoryKind::Owner)?,
        )?;
        owner_key_count = owner_key_count
            .checked_add(1)
            .context("owner directory count overflow")?;
    }
    ensure!(
        owner_key_count == expected_owner_keys && owner_posting_count == expected_owner_postings,
        "merged owner directory or body count differs"
    );
    let owner_semantic_sha256 = semantic.finish()?;
    let owner_directory = directory.finish(
        OWNER_DIRECTORY_FILE,
        owner_key_count,
        POSTINGS_DIRECTORY_RECORD_BYTES as u16,
    )?;
    let owner_postings_binding = postings.finish(
        OWNER_POSTINGS_FILE,
        owner_posting_count,
        crate::postings_format::POSTINGS_BODY_RECORD_BYTES as u16,
    )?;
    Ok(MergedOwnerArtifacts {
        owner_directory,
        owner_postings_binding,
        owner_semantic_sha256,
        owner_keys: owner_key_count,
        owner_postings: owner_posting_count,
    })
}

fn create_owner_posting_writer(
    work: &Path,
    kind: OwnerPostingsFileKind,
    complete: bool,
    records: u64,
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
) -> Result<DigestFileWriter> {
    let path = work.join(format!("{}.partial", kind.file_name()));
    let mut writer = DigestFileWriter::create(&path)?;
    writer.write_all(
        &OwnerPostingsFileHeader {
            kind,
            complete,
            record_count: records,
            source_manifest_sha256,
            source_transaction_sha256,
        }
        .encode(),
    )?;
    Ok(writer)
}

#[allow(clippy::too_many_arguments)]
fn merge_posting_runs(
    runs: &[PostingRunBinding],
    work: &Path,
    complete: bool,
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
    target_entries: &[TargetRegistryEntry],
    expected_program_keys: u64,
    expected_target_postings: u64,
    expected_program_postings: u64,
    expected_program_direct_postings: u64,
    expected_program_inner_postings: u64,
    expected_total: u64,
    source_transaction_count: u64,
) -> Result<MergedArtifacts> {
    let expected_target_keys =
        u64::try_from(target_entries.len()).context("target key count exceeds u64")?;
    let mut target_directory = create_posting_writer(
        work,
        PostingsFileKind::TargetAddressDirectory,
        complete,
        expected_target_keys,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut target_postings = create_posting_writer(
        work,
        PostingsFileKind::TargetAddressPostings,
        complete,
        expected_target_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut program_directory = create_posting_writer(
        work,
        PostingsFileKind::ProgramDirectory,
        complete,
        expected_program_keys,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut program_postings = create_posting_writer(
        work,
        PostingsFileKind::ProgramPostings,
        complete,
        expected_program_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut program_direct_directory = create_posting_writer(
        work,
        PostingsFileKind::ProgramDirectDirectory,
        complete,
        expected_program_keys,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut program_direct_postings = create_posting_writer(
        work,
        PostingsFileKind::ProgramDirectPostings,
        complete,
        expected_program_direct_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut program_inner_directory = create_posting_writer(
        work,
        PostingsFileKind::ProgramInnerDirectory,
        complete,
        expected_program_keys,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut program_inner_postings = create_posting_writer(
        work,
        PostingsFileKind::ProgramInnerPostings,
        complete,
        expected_program_inner_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    )?;
    let mut target_semantic = PostingsSemanticHasher::new(
        PostingsDirectoryKind::TargetAddress,
        expected_target_postings,
    );
    let mut program_semantic =
        ProgramPostingsSemanticHasher::new(ProgramInstructionScope::All, expected_program_postings);
    let mut program_direct_semantic = ProgramPostingsSemanticHasher::new(
        ProgramInstructionScope::Direct,
        expected_program_direct_postings,
    );
    let mut program_inner_semantic = ProgramPostingsSemanticHasher::new(
        ProgramInstructionScope::Inner,
        expected_program_inner_postings,
    );
    let mut current = None::<DirectoryGroup>;
    let mut next_target = 0usize;
    let mut target_phase_complete = false;
    let mut target_key_count = 0u64;
    let mut nonempty_target_key_count = 0u64;
    let mut target_posting_count = 0u64;
    let mut program_key_count = 0u64;
    let mut program_posting_count = 0u64;
    let mut program_direct_posting_count = 0u64;
    let mut program_inner_posting_count = 0u64;

    merge_sorted_runs(runs, expected_total, source_transaction_count, |row| {
        if row.kind() == WorkKind::Program && !target_phase_complete {
            if let Some(group) = current.take() {
                ensure!(
                    group.kind == WorkKind::TargetAddress,
                    "posting work kinds are not canonical"
                );
                flush_directory_group(
                    group,
                    target_entries,
                    &mut next_target,
                    &mut target_directory,
                    &mut program_directory,
                    &mut program_direct_directory,
                    &mut program_inner_directory,
                    &mut target_key_count,
                    &mut nonempty_target_key_count,
                    &mut program_key_count,
                )?;
            }
            flush_remaining_target_directories(
                target_entries,
                &mut next_target,
                target_posting_count,
                &mut target_directory,
                &mut target_key_count,
            )?;
            target_phase_complete = true;
        }
        if current.is_some_and(|group| !group.matches(row)) {
            flush_directory_group(
                current.take().expect("checked current posting group"),
                target_entries,
                &mut next_target,
                &mut target_directory,
                &mut program_directory,
                &mut program_direct_directory,
                &mut program_inner_directory,
                &mut target_key_count,
                &mut nonempty_target_key_count,
                &mut program_key_count,
            )?;
        }
        if current.is_none() {
            current = Some(DirectoryGroup {
                kind: row.kind(),
                registry_id: row.registry_id(),
                flags: if row.kind() == WorkKind::TargetAddress {
                    row.flags()
                } else {
                    0
                },
                first_posting_row: match row.kind() {
                    WorkKind::TargetAddress => target_posting_count,
                    WorkKind::Program => program_posting_count,
                    WorkKind::Owner => unreachable!("owner rows use the owner-only merge"),
                },
                posting_count: 0,
                direct_first_posting_row: program_direct_posting_count,
                direct_posting_count: 0,
                inner_first_posting_row: program_inner_posting_count,
                inner_posting_count: 0,
            });
        }
        match row.kind() {
            WorkKind::TargetAddress => {
                ensure!(
                    !target_phase_complete,
                    "target posting occurs after program postings"
                );
                target_postings.write_all(
                    &PostingRecord {
                        transaction_ordinal: row.transaction_ordinal(),
                    }
                    .encode(),
                )?;
                target_semantic.update(
                    row.registry_id(),
                    row.flags(),
                    row.transaction_ordinal(),
                )?;
                target_posting_count = target_posting_count
                    .checked_add(1)
                    .context("merged target posting count overflow")?;
            }
            WorkKind::Program => {
                let posting = ProgramPostingRecord {
                    transaction_ordinal: row.transaction_ordinal(),
                    instruction_scope_mask: row.flags() as u8,
                };
                program_postings.write_all(&posting.encode()?)?;
                program_semantic.update(
                    row.registry_id(),
                    posting.instruction_scope_mask,
                    posting.transaction_ordinal,
                )?;
                program_posting_count = program_posting_count
                    .checked_add(1)
                    .context("merged program posting count overflow")?;
                let group = current.as_mut().expect("posting group was initialized");
                if ProgramInstructionScope::Direct.includes(posting.instruction_scope_mask) {
                    program_direct_postings.write_all(&posting.encode()?)?;
                    program_direct_semantic.update(
                        row.registry_id(),
                        posting.instruction_scope_mask,
                        posting.transaction_ordinal,
                    )?;
                    program_direct_posting_count = program_direct_posting_count
                        .checked_add(1)
                        .context("merged direct program posting count overflow")?;
                    group.direct_posting_count = group
                        .direct_posting_count
                        .checked_add(1)
                        .context("direct program posting range count overflow")?;
                }
                if ProgramInstructionScope::Inner.includes(posting.instruction_scope_mask) {
                    program_inner_postings.write_all(&posting.encode()?)?;
                    program_inner_semantic.update(
                        row.registry_id(),
                        posting.instruction_scope_mask,
                        posting.transaction_ordinal,
                    )?;
                    program_inner_posting_count = program_inner_posting_count
                        .checked_add(1)
                        .context("merged inner program posting count overflow")?;
                    group.inner_posting_count = group
                        .inner_posting_count
                        .checked_add(1)
                        .context("inner program posting range count overflow")?;
                }
            }
            WorkKind::Owner => unreachable!("owner rows use the owner-only merge"),
        }
        let group = current.as_mut().expect("posting group was initialized");
        group.posting_count = group
            .posting_count
            .checked_add(1)
            .context("posting directory group count overflow")?;
        Ok(())
    })?;
    if let Some(group) = current.take() {
        flush_directory_group(
            group,
            target_entries,
            &mut next_target,
            &mut target_directory,
            &mut program_directory,
            &mut program_direct_directory,
            &mut program_inner_directory,
            &mut target_key_count,
            &mut nonempty_target_key_count,
            &mut program_key_count,
        )?;
    }
    if !target_phase_complete {
        flush_remaining_target_directories(
            target_entries,
            &mut next_target,
            target_posting_count,
            &mut target_directory,
            &mut target_key_count,
        )?;
    }
    ensure!(
        next_target == target_entries.len()
            && target_key_count == expected_target_keys
            && target_posting_count == expected_target_postings
            && program_key_count == expected_program_keys
            && program_posting_count == expected_program_postings
            && program_direct_posting_count == expected_program_direct_postings
            && program_inner_posting_count == expected_program_inner_postings,
        "merged posting directory or body count differs"
    );

    let target_semantic_sha256 = target_semantic.finish()?;
    let program_semantic_sha256 = program_semantic.finish()?;
    let program_direct_semantic_sha256 = program_direct_semantic.finish()?;
    let program_inner_semantic_sha256 = program_inner_semantic.finish()?;
    let target_directory = target_directory.finish(
        TARGET_ADDRESS_DIRECTORY_FILE,
        target_key_count,
        POSTINGS_DIRECTORY_RECORD_BYTES as u16,
    )?;
    let target_postings_binding = target_postings.finish(
        TARGET_ADDRESS_POSTINGS_FILE,
        target_posting_count,
        crate::postings_format::POSTINGS_BODY_RECORD_BYTES as u16,
    )?;
    let program_directory = program_directory.finish(
        PROGRAM_DIRECTORY_FILE,
        program_key_count,
        POSTINGS_DIRECTORY_RECORD_BYTES as u16,
    )?;
    let program_postings_binding = program_postings.finish(
        PROGRAM_POSTINGS_FILE,
        program_posting_count,
        crate::postings_format::POSTINGS_BODY_RECORD_BYTES as u16,
    )?;
    let program_direct_directory = program_direct_directory.finish(
        PROGRAM_DIRECT_DIRECTORY_FILE,
        program_key_count,
        POSTINGS_DIRECTORY_RECORD_BYTES as u16,
    )?;
    let program_direct_postings_binding = program_direct_postings.finish(
        PROGRAM_DIRECT_POSTINGS_FILE,
        program_direct_posting_count,
        crate::postings_format::POSTINGS_BODY_RECORD_BYTES as u16,
    )?;
    let program_inner_directory = program_inner_directory.finish(
        PROGRAM_INNER_DIRECTORY_FILE,
        program_key_count,
        POSTINGS_DIRECTORY_RECORD_BYTES as u16,
    )?;
    let program_inner_postings_binding = program_inner_postings.finish(
        PROGRAM_INNER_POSTINGS_FILE,
        program_inner_posting_count,
        crate::postings_format::POSTINGS_BODY_RECORD_BYTES as u16,
    )?;
    Ok(MergedArtifacts {
        target_directory,
        target_postings_binding,
        program_directory,
        program_postings_binding,
        program_direct_directory,
        program_direct_postings_binding,
        program_inner_directory,
        program_inner_postings_binding,
        target_semantic_sha256,
        program_semantic_sha256,
        program_direct_semantic_sha256,
        program_inner_semantic_sha256,
        target_keys: target_key_count,
        nonempty_target_keys: nonempty_target_key_count,
        target_postings: target_posting_count,
        program_keys: program_key_count,
        program_postings: program_posting_count,
        program_direct_postings: program_direct_posting_count,
        program_inner_postings: program_inner_posting_count,
    })
}

fn create_posting_writer(
    work: &Path,
    kind: PostingsFileKind,
    complete: bool,
    records: u64,
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
) -> Result<DigestFileWriter> {
    let path = work.join(format!("{}.partial", kind.file_name()));
    let mut writer = DigestFileWriter::create(&path)?;
    writer.write_all(
        &PostingsFileHeader {
            kind,
            complete,
            record_count: records,
            source_manifest_sha256,
            source_transaction_sha256,
        }
        .encode(),
    )?;
    Ok(writer)
}

#[allow(clippy::too_many_arguments)]
fn flush_directory_group(
    group: DirectoryGroup,
    target_entries: &[TargetRegistryEntry],
    next_target: &mut usize,
    target_directory: &mut DigestFileWriter,
    program_directory: &mut DigestFileWriter,
    program_direct_directory: &mut DigestFileWriter,
    program_inner_directory: &mut DigestFileWriter,
    target_key_count: &mut u64,
    nonempty_target_key_count: &mut u64,
    program_key_count: &mut u64,
) -> Result<()> {
    ensure!(group.posting_count != 0, "posting directory group is empty");
    match group.kind {
        WorkKind::TargetAddress => {
            ensure!(
                group.direct_posting_count == 0 && group.inner_posting_count == 0,
                "target directory group has program scope counts"
            );
            while target_entries
                .get(*next_target)
                .is_some_and(|entry| entry.registry_id < group.registry_id)
            {
                write_empty_target_directory(
                    target_entries[*next_target],
                    group.first_posting_row,
                    target_directory,
                )?;
                *next_target += 1;
                *target_key_count = target_key_count
                    .checked_add(1)
                    .context("target directory count overflow")?;
            }
            let expected = target_entries
                .get(*next_target)
                .context("target posting key is not in the target registry")?;
            ensure!(
                expected.registry_id == group.registry_id && expected.flags == group.flags,
                "target posting key or role differs from the target registry"
            );
            target_directory.write_all(
                &PostingsDirectoryRecord {
                    registry_id: group.registry_id,
                    flags: group.flags,
                    first_posting_row: group.first_posting_row,
                    posting_count: group.posting_count,
                }
                .encode(PostingsDirectoryKind::TargetAddress)?,
            )?;
            *next_target += 1;
            *target_key_count = target_key_count
                .checked_add(1)
                .context("target directory count overflow")?;
            *nonempty_target_key_count = nonempty_target_key_count
                .checked_add(1)
                .context("nonempty target directory count overflow")?;
        }
        WorkKind::Program => {
            ensure!(group.flags == 0, "program directory group has flags");
            for (writer, first_posting_row, posting_count) in [
                (
                    program_directory,
                    group.first_posting_row,
                    group.posting_count,
                ),
                (
                    program_direct_directory,
                    group.direct_first_posting_row,
                    group.direct_posting_count,
                ),
                (
                    program_inner_directory,
                    group.inner_first_posting_row,
                    group.inner_posting_count,
                ),
            ] {
                writer.write_all(
                    &PostingsDirectoryRecord {
                        registry_id: group.registry_id,
                        flags: 0,
                        first_posting_row,
                        posting_count,
                    }
                    .encode(PostingsDirectoryKind::Program)?,
                )?;
            }
            *program_key_count = program_key_count
                .checked_add(1)
                .context("program directory count overflow")?;
        }
        WorkKind::Owner => unreachable!("owner rows use the owner-only merge"),
    }
    Ok(())
}

fn flush_remaining_target_directories(
    target_entries: &[TargetRegistryEntry],
    next_target: &mut usize,
    first_posting_row: u64,
    target_directory: &mut DigestFileWriter,
    target_key_count: &mut u64,
) -> Result<()> {
    while let Some(entry) = target_entries.get(*next_target).copied() {
        write_empty_target_directory(entry, first_posting_row, target_directory)?;
        *next_target += 1;
        *target_key_count = target_key_count
            .checked_add(1)
            .context("target directory count overflow")?;
    }
    Ok(())
}

fn write_empty_target_directory(
    entry: TargetRegistryEntry,
    first_posting_row: u64,
    writer: &mut DigestFileWriter,
) -> Result<()> {
    writer.write_all(
        &PostingsDirectoryRecord {
            registry_id: entry.registry_id,
            flags: entry.flags,
            first_posting_row,
            posting_count: 0,
        }
        .encode(PostingsDirectoryKind::TargetAddress)?,
    )
}

fn validate_full_source_gate(
    transaction_sha256: [u8; 32],
    transactions: u64,
    accounts: u64,
) -> Result<()> {
    ensure!(
        hex_digest(transaction_sha256) == FULL_SOURCE_TRANSACTION_SHA256
            && transactions == FULL_TRANSACTIONS
            && accounts == FULL_ACCOUNTS,
        "complete posting builds require the exact final SPYx source"
    );
    Ok(())
}

fn validate_full_projection_gate(
    transactions: u64,
    transactions_with_target: u64,
    target_keys: u64,
    target_postings: u64,
    program_keys: u64,
    program_postings: u64,
    total_postings: u64,
) -> Result<()> {
    ensure!(
        transactions == FULL_TRANSACTIONS
            && transactions_with_target == FULL_TRANSACTIONS
            && target_keys == FULL_TARGET_KEYS
            && target_postings == FULL_TARGET_POSTINGS
            && program_keys == FULL_PROGRAM_KEYS
            && program_postings == FULL_PROGRAM_POSTINGS
            && total_postings == FULL_TOTAL_POSTINGS
            && target_postings
                .checked_add(program_postings)
                .is_some_and(|total| total == total_postings),
        "complete posting counts differ from the exact final SPYx gates"
    );
    Ok(())
}

fn validate_manifest_headers(manifest: &PostingsManifest) -> Result<()> {
    for kind in [
        PostingsFileKind::TargetAddressDirectory,
        PostingsFileKind::TargetAddressPostings,
        PostingsFileKind::ProgramDirectory,
        PostingsFileKind::ProgramPostings,
        PostingsFileKind::ProgramDirectDirectory,
        PostingsFileKind::ProgramDirectPostings,
        PostingsFileKind::ProgramInnerDirectory,
        PostingsFileKind::ProgramInnerPostings,
    ] {
        manifest.validate_header(PostingsFileHeader {
            kind,
            complete: manifest.complete,
            record_count: manifest.binding(kind).records,
            source_manifest_sha256: crate::index_format::parse_hex_digest(
                &manifest.source.manifest_sha256,
                "source manifest digest",
            )?,
            source_transaction_sha256: crate::index_format::parse_hex_digest(
                &manifest.source.transaction_sha256,
                "source transaction digest",
            )?,
        })?;
    }
    Ok(())
}

fn validate_owner_manifest_headers(manifest: &OwnerPostingsManifest) -> Result<()> {
    for kind in [
        OwnerPostingsFileKind::Directory,
        OwnerPostingsFileKind::Postings,
    ] {
        manifest.validate_header(OwnerPostingsFileHeader {
            kind,
            complete: manifest.complete,
            record_count: manifest.binding(kind).records,
            source_manifest_sha256: crate::index_format::parse_hex_digest(
                &manifest.source.manifest_sha256,
                "source manifest digest",
            )?,
            source_transaction_sha256: crate::index_format::parse_hex_digest(
                &manifest.source.transaction_sha256,
                "source transaction digest",
            )?,
        })?;
    }
    Ok(())
}

fn validate_owner_balance_history_manifest_headers(
    manifest: &OwnerBalanceHistoryManifest,
) -> Result<()> {
    for kind in [
        OwnerBalanceHistoryFileKind::Directory,
        OwnerBalanceHistoryFileKind::Events,
    ] {
        manifest.validate_header(OwnerBalanceHistoryFileHeader {
            kind,
            complete: manifest.complete,
            record_count: manifest.binding(kind).records,
            source_manifest_sha256: crate::index_format::parse_hex_digest(
                &manifest.source.manifest_sha256,
                "source manifest digest",
            )?,
            source_transaction_sha256: crate::index_format::parse_hex_digest(
                &manifest.source.transaction_sha256,
                "source transaction digest",
            )?,
        })?;
    }
    Ok(())
}

fn pretty_json_bytes(value: &impl serde::Serialize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn publish_owner_postings_with_history(
    output: &Path,
    work: &Path,
    runs_root: &Path,
    runs: &[PostingRunBinding],
    balance_history_runs_root: &Path,
    balance_history_runs: &[BalanceHistoryRunBinding],
    manifest: &OwnerPostingsManifest,
    balance_history_manifest: &OwnerBalanceHistoryManifest,
    balance_history_manifest_bytes: &[u8],
    source: &SourceDump,
) -> Result<()> {
    for kind in [
        OwnerPostingsFileKind::Directory,
        OwnerPostingsFileKind::Postings,
    ] {
        fs::rename(
            work.join(format!("{}.partial", kind.file_name())),
            output.join(kind.file_name()),
        )?;
    }
    for kind in [
        OwnerBalanceHistoryFileKind::Directory,
        OwnerBalanceHistoryFileKind::Events,
    ] {
        fs::rename(
            work.join(format!("{}.partial", kind.file_name())),
            output.join(kind.file_name()),
        )?;
    }
    for run in runs {
        fs::remove_file(&run.path)?;
    }
    for run in balance_history_runs {
        fs::remove_file(&run.path)?;
    }
    ensure!(
        fs::read_dir(runs_root)?.next().is_none(),
        "owner posting run directory is not empty after merge"
    );
    fs::remove_dir(runs_root)?;
    ensure!(
        fs::read_dir(balance_history_runs_root)?.next().is_none(),
        "owner balance-history run directory is not empty after merge"
    );
    fs::remove_dir(balance_history_runs_root)?;
    fs::remove_dir(work)?;
    sync_directory(output)?;
    source.verify_file_identities()?;

    ensure!(
        pretty_json_bytes(balance_history_manifest)? == balance_history_manifest_bytes,
        "owner balance-history manifest bytes changed before publication"
    );
    let balance_manifest_partial =
        output.join(format!("{OWNER_BALANCE_HISTORY_MANIFEST_FILE}.partial"));
    let mut writer = BufWriter::new(create_new_file(&balance_manifest_partial)?);
    writer.write_all(balance_history_manifest_bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(
        &balance_manifest_partial,
        output.join(OWNER_BALANCE_HISTORY_MANIFEST_FILE),
    )?;
    sync_directory(output)?;

    // Publish the legacy-compatible owner manifest last. Its optional binding
    // makes the history extension mandatory for this new artifact while old
    // owner-postings v1 manifests without the binding remain readable.
    let manifest_partial = output.join(format!("{OWNER_POSTINGS_MANIFEST_FILE}.partial"));
    let bytes = pretty_json_bytes(manifest)?;
    let mut writer = BufWriter::new(create_new_file(&manifest_partial)?);
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&manifest_partial, output.join(OWNER_POSTINGS_MANIFEST_FILE))?;
    sync_directory(output)?;
    Ok(())
}

#[cfg(test)]
fn publish_owner_postings(
    output: &Path,
    work: &Path,
    runs_root: &Path,
    runs: &[PostingRunBinding],
    manifest: &OwnerPostingsManifest,
    source: &SourceDump,
) -> Result<()> {
    for kind in [
        OwnerPostingsFileKind::Directory,
        OwnerPostingsFileKind::Postings,
    ] {
        fs::rename(
            work.join(format!("{}.partial", kind.file_name())),
            output.join(kind.file_name()),
        )?;
    }
    for run in runs {
        fs::remove_file(&run.path)?;
    }
    ensure!(
        fs::read_dir(runs_root)?.next().is_none(),
        "owner posting run directory is not empty after merge"
    );
    fs::remove_dir(runs_root)?;
    fs::remove_dir(work)?;
    sync_directory(output)?;
    source.verify_file_identities()?;

    let manifest_partial = output.join(format!("{OWNER_POSTINGS_MANIFEST_FILE}.partial"));
    let mut bytes = serde_json::to_vec_pretty(manifest)?;
    bytes.push(b'\n');
    let mut writer = BufWriter::new(create_new_file(&manifest_partial)?);
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&manifest_partial, output.join(OWNER_POSTINGS_MANIFEST_FILE))?;
    sync_directory(output)?;
    Ok(())
}

fn publish_postings(
    output: &Path,
    work: &Path,
    runs_root: &Path,
    runs: &[PostingRunBinding],
    manifest: &PostingsManifest,
    source: &SourceDump,
) -> Result<()> {
    for kind in [
        PostingsFileKind::TargetAddressDirectory,
        PostingsFileKind::TargetAddressPostings,
        PostingsFileKind::ProgramDirectory,
        PostingsFileKind::ProgramPostings,
        PostingsFileKind::ProgramDirectDirectory,
        PostingsFileKind::ProgramDirectPostings,
        PostingsFileKind::ProgramInnerDirectory,
        PostingsFileKind::ProgramInnerPostings,
    ] {
        fs::rename(
            work.join(format!("{}.partial", kind.file_name())),
            output.join(kind.file_name()),
        )?;
    }
    for run in runs {
        fs::remove_file(&run.path)?;
    }
    ensure!(
        fs::read_dir(runs_root)?.next().is_none(),
        "posting run directory is not empty after merge"
    );
    fs::remove_dir(runs_root)?;
    fs::remove_dir(work)?;
    sync_directory(output)?;
    source.verify_file_identities()?;

    let manifest_partial = output.join(format!("{POSTINGS_MANIFEST_FILE}.partial"));
    let mut bytes = serde_json::to_vec_pretty(manifest)?;
    bytes.push(b'\n');
    let mut writer = BufWriter::new(create_new_file(&manifest_partial)?);
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&manifest_partial, output.join(POSTINGS_MANIFEST_FILE))?;
    sync_directory(output)?;
    Ok(())
}

const _: () = assert!(std::mem::size_of::<WorkRow>() == WORK_ROW_BYTES);

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use axum::{body::to_bytes, http::Request};
    use blockzilla_archive_v2::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
        ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
    };
    use blockzilla_compact::{
        CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
        CompactTransactionError, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
    };
    use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter, wincode_leb128_config};
    use blockzilla_token_transaction_dump::{
        ACCOUNTS_FILE, DUMP_MANIFEST_FILE, DumpArtifactKind, DumpManifest, DumpStreamKind,
        PUBKEY_REGISTRY_FILE, PUBKEY_REGISTRY_ID_BASE, SIGNATURES_FILE,
        SourceInstructionCoordinate, SourceTransactionCoordinate, TRANSACTIONS_FILE,
        TokenTransactionBlockContext, TokenTransactionDumpFooter, TokenTransactionDumpHeader,
        TokenTransactionDumpRecord, TokenTransactionRecord,
    };
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::postings_format::{POSTINGS_HEADER_BYTES, PostingRecord};
    use crate::{
        BuildConfig, MAX_POSTINGS_PAGE_ROWS, OwnerPostingsStore, PostingsOpenOptions,
        PostingsStore, QueryOpenOptions, QueryStore, build_index, router_with_all_indexes,
        router_with_postings,
    };

    fn instruction(program_id_index: u8) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: vec![0],
            data: ArchiveV2HotInstructionData::Raw(vec![1]),
        }
    }

    fn legacy_message(keys: Vec<CompactPubkey>, program_index: u8) -> Vec<u8> {
        wincode::config::serialize(
            &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: keys,
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![instruction(program_index)],
            }),
            wincode_leb128_config(),
        )
        .unwrap()
    }

    fn v0_message() -> Vec<u8> {
        wincode::config::serialize(
            &ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![
                    CompactPubkey::Id(2),
                    CompactPubkey::Id(3),
                    CompactPubkey::Id(1),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![instruction(2)],
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: CompactPubkey::Id(5),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![0],
                }],
            }),
            wincode_leb128_config(),
        )
        .unwrap()
    }

    fn metadata(
        error: Option<CompactTransactionError>,
        total_accounts: usize,
        inner_program_index: u32,
        writable: Vec<CompactPubkey>,
        readonly: Vec<CompactPubkey>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 5_000,
            pre_balances: vec![0; total_accounts],
            post_balances: vec![0; total_accounts],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: inner_program_index,
                    accounts: vec![0],
                    data: vec![1],
                    stack_height: Some(2),
                }],
            }]),
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: writable,
            loaded_readonly_addresses: readonly,
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn current_metadata(value: &CompactMetaV1) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn legacy_error_metadata(successful_tail: &CompactMetaV1) -> Vec<u8> {
        let successful = current_metadata(successful_tail);
        let mut legacy =
            wincode::config::serialize(&Some(vec![0u8, 0, 0, 0]), wincode_leb128_config()).unwrap();
        legacy.extend_from_slice(&successful[1..]);
        legacy
    }

    fn borrowed_record<'a>(
        message_bytes: &'a [u8],
        metadata_bytes: &'a [u8],
        flags: u32,
    ) -> BorrowedTransactionRecord<'a> {
        BorrowedTransactionRecord {
            source_epoch: 1,
            source_generation_digest: [9; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 1,
            block: block(1_001, 1),
            tx_index: 0,
            flags,
            source_first_signature_ordinal: 0,
            signature_count: 1,
            dump_signature_ordinal: Some(0),
            message_bytes,
            metadata_bytes,
        }
    }

    fn block(slot: u64, transaction_count: u32) -> TokenTransactionBlockContext {
        TokenTransactionBlockContext {
            slot,
            parent_slot: slot - 1,
            blockhash_id: u32::try_from(slot).unwrap(),
            previous_blockhash_id: u32::try_from(slot - 1).unwrap(),
            block_time: Some(i64::try_from(slot).unwrap()),
            block_height: Some(slot),
            transaction_count,
        }
    }

    fn target_membership(entries: u32) -> DenseBitSet {
        let mut membership = DenseBitSet::new(entries).unwrap();
        membership.insert(2).unwrap();
        membership.insert(3).unwrap();
        membership
    }

    #[test]
    fn emitter_includes_failed_current_and_legacy_records_and_loaded_addresses() {
        let legacy_message = legacy_message(
            vec![
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
                CompactPubkey::Id(3),
                CompactPubkey::Id(1),
            ],
            3,
        );
        let failed = metadata(
            Some(CompactTransactionError::AccountInUse),
            4,
            3,
            Vec::new(),
            Vec::new(),
        );
        let current = current_metadata(&failed);
        let successful_tail = metadata(None, 4, 3, Vec::new(), Vec::new());
        let legacy = legacy_error_metadata(&successful_tail);
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
        let membership = target_membership(6);
        let mut scratch = ConsolidatedPostingProjectionScratch::new(6).unwrap();
        let mut current_rows = Vec::new();
        let current_counts = emit_transaction_rows(
            &borrowed_record(&legacy_message, &current, flags),
            7,
            6,
            &membership,
            2,
            &mut scratch,
            |row| {
                current_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        let mut legacy_rows = Vec::new();
        let legacy_counts = emit_transaction_rows(
            &borrowed_record(&legacy_message, &legacy, flags),
            7,
            6,
            &membership,
            2,
            &mut scratch,
            |row| {
                legacy_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            current_counts,
            EmittedTransactionRows {
                target_rows: 2,
                program_rows: 1
            }
        );
        assert_eq!(legacy_counts, current_counts);
        assert_eq!(legacy_rows, current_rows);
        assert_eq!(
            current_rows,
            vec![
                WorkRow::new(WorkKind::TargetAddress, 2, TARGET_ADDRESS_FLAG_MINT, 7).unwrap(),
                WorkRow::new(
                    WorkKind::TargetAddress,
                    3,
                    TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                    7,
                )
                .unwrap(),
                WorkRow::new(
                    WorkKind::Program,
                    1,
                    u32::from(PROGRAM_INSTRUCTION_SCOPE_MASK),
                    7
                )
                .unwrap(),
            ]
        );

        let v0 = v0_message();
        let loaded = current_metadata(&metadata(
            None,
            5,
            4,
            vec![CompactPubkey::Id(3)],
            vec![CompactPubkey::Id(4)],
        ));
        let loaded_flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        let mut loaded_rows = Vec::new();
        let loaded_counts = emit_transaction_rows(
            &borrowed_record(&v0, &loaded, loaded_flags),
            8,
            6,
            &membership,
            2,
            &mut scratch,
            |row| {
                loaded_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            loaded_counts,
            EmittedTransactionRows {
                target_rows: 2,
                program_rows: 2
            }
        );
        assert_eq!(
            loaded_rows,
            vec![
                WorkRow::new(WorkKind::TargetAddress, 2, TARGET_ADDRESS_FLAG_MINT, 8).unwrap(),
                WorkRow::new(
                    WorkKind::TargetAddress,
                    3,
                    TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                    8,
                )
                .unwrap(),
                WorkRow::new(
                    WorkKind::Program,
                    1,
                    u32::from(PROGRAM_INSTRUCTION_SCOPE_DIRECT),
                    8,
                )
                .unwrap(),
                WorkRow::new(
                    WorkKind::Program,
                    4,
                    u32::from(PROGRAM_INSTRUCTION_SCOPE_INNER),
                    8,
                )
                .unwrap(),
            ]
        );
    }

    struct PostingFixture {
        _temporary: TempDir,
        dump: PathBuf,
        index: PathBuf,
        output: PathBuf,
        owner_output: PathBuf,
    }

    fn posting_fixture() -> PostingFixture {
        let temporary = tempfile::tempdir().unwrap();
        let dump = temporary.path().join("dump");
        let index = temporary.path().join("index");
        let output = temporary.path().join("postings");
        let owner_output = temporary.path().join("owner-postings");
        fs::create_dir(&dump).unwrap();

        let mint = [2u8; 32];
        let mint_signature = [8u8; 64];
        let registry = (1u8..=6).flat_map(|byte| [byte; 32]).collect::<Vec<_>>();
        let discovered = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint,
            anchor_position: SourceTransactionCoordinate {
                epoch: 1,
                slot: 1_001,
                source_block_id: 1,
                tx_index: 0,
                source_first_signature_ordinal: 0,
                signature_count: 1,
            },
            accounts: vec![
                blockzilla_token_transaction_dump::DiscoveredAccount {
                    raw_pubkey: [3; 32],
                    first_creation: SourceInstructionCoordinate {
                        epoch: 1,
                        slot: 1_001,
                        source_block_id: 1,
                        tx_index: 0,
                        instruction_index: 0,
                    },
                },
                blockzilla_token_transaction_dump::DiscoveredAccount {
                    raw_pubkey: [6; 32],
                    first_creation: SourceInstructionCoordinate {
                        epoch: 1,
                        slot: 1_002,
                        source_block_id: 2,
                        tx_index: 0,
                        instruction_index: 0,
                    },
                },
            ],
        };
        let account_bytes =
            wincode::config::serialize(&discovered, wincode_leb128_config()).unwrap();

        let first_message = legacy_message(
            vec![
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
                CompactPubkey::Id(3),
                CompactPubkey::Id(1),
            ],
            3,
        );
        let first_metadata = current_metadata(&metadata(
            Some(CompactTransactionError::AccountInUse),
            4,
            3,
            Vec::new(),
            Vec::new(),
        ));
        let first_flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
        let second_message = v0_message();
        let second_metadata = current_metadata(&metadata(
            None,
            5,
            4,
            vec![CompactPubkey::Id(3)],
            vec![CompactPubkey::Id(4)],
        ));
        let second_flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        let records = [
            TokenTransactionDumpRecord::Header(TokenTransactionDumpHeader {
                schema_version: DUMP_SCHEMA_VERSION,
                stream_kind: DumpStreamKind::Consolidated,
                mint,
                mint_slot: 1_001,
                mint_signature,
                source_epoch: None,
                source_generation_digest: None,
                source_wire_profile: None,
                pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
            }),
            TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
                source_epoch: 1,
                source_generation_digest: [9; 32],
                source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
                source_block_id: 1,
                block: block(1_001, 1),
                tx_index: 0,
                flags: first_flags,
                source_first_signature_ordinal: 0,
                signature_count: 1,
                dump_signature_ordinal: Some(0),
                message_bytes: first_message,
                metadata_bytes: first_metadata,
            }),
            TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
                source_epoch: 1,
                source_generation_digest: [9; 32],
                source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
                source_block_id: 2,
                block: block(1_002, 1),
                tx_index: 0,
                flags: second_flags,
                source_first_signature_ordinal: 1,
                signature_count: 1,
                dump_signature_ordinal: Some(1),
                message_bytes: second_message,
                metadata_bytes: second_metadata,
            }),
            TokenTransactionDumpRecord::Footer(TokenTransactionDumpFooter {
                epochs: 1,
                blocks_scanned: 2,
                transactions_scanned: 2,
                transactions_written: 2,
                pubkeys: 6,
                signatures: 2,
                owned_block_fallbacks: 0,
                raw_transaction_fallbacks: 0,
                raw_metadata_fallbacks: 0,
            }),
        ];
        let mut framed = WincodeLeb128FramedWriter::new(Vec::new());
        for record in &records {
            framed.write(record).unwrap();
        }
        let transaction_bytes = framed.into_inner();
        let signatures = [mint_signature, [7u8; 64]].concat();
        fs::write(dump.join(TRANSACTIONS_FILE), &transaction_bytes).unwrap();
        fs::write(dump.join(SIGNATURES_FILE), &signatures).unwrap();
        fs::write(dump.join(PUBKEY_REGISTRY_FILE), &registry).unwrap();
        fs::write(dump.join(ACCOUNTS_FILE), &account_bytes).unwrap();
        let manifest = DumpManifest {
            schema_version: DUMP_SCHEMA_VERSION,
            artifact_kind: DumpArtifactKind::Consolidated,
            complete: true,
            mint: bs58::encode(mint).into_string(),
            mint_slot: 1_001,
            mint_signature: bs58::encode(mint_signature).into_string(),
            workers: 1,
            source_binding: DumpSourceBinding::TrustedLocalSizesOnly {
                cluster_id: "posting-fixture".to_owned(),
                slots_per_epoch: 1_000,
                wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            },
            first_epoch: 1,
            last_epoch: 1,
            transactions: 2,
            signatures: Some(2),
            pubkeys: Some(6),
            transaction_stream: TRANSACTIONS_FILE.to_owned(),
            transaction_stream_sha256: Some(hex_digest(sha256_bytes(&transaction_bytes))),
            account_id_log: None,
            account_id_log_sha256: None,
            discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
            discovered_accounts_sha256: Some(hex_digest(sha256_bytes(&account_bytes))),
            discovered_account_count: Some(2),
            signature_stream: Some(SIGNATURES_FILE.to_owned()),
            signature_stream_sha256: Some(hex_digest(sha256_bytes(&signatures))),
            pubkey_registry: Some(PUBKEY_REGISTRY_FILE.to_owned()),
            pubkey_registry_sha256: Some(hex_digest(sha256_bytes(&registry))),
            registry_maps: None,
        };
        fs::write(
            dump.join(DUMP_MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        PostingFixture {
            _temporary: temporary,
            dump,
            index,
            output,
            owner_output,
        }
    }

    fn build_manual_owner_artifact(fixture: &PostingFixture) {
        let source = load_source_dump(&fixture.dump).unwrap();
        let output = prepare_output(&fixture.owner_output, &source.root).unwrap();
        let work = output.join(WORK_DIRECTORY);
        let runs_root = work.join(RUNS_DIRECTORY);
        fs::create_dir_all(&runs_root).unwrap();
        let mut sorter = PostingSorter::new(&runs_root).unwrap();
        for row in [
            WorkRow::new(WorkKind::Owner, 5, 0, 1).unwrap(),
            WorkRow::new(WorkKind::Owner, 4, 0, 1).unwrap(),
            WorkRow::new(WorkKind::Owner, 4, 0, 0).unwrap(),
        ] {
            sorter.push(row).unwrap();
        }
        sorter.flush_run().unwrap();
        let merged = merge_owner_posting_runs(
            &sorter.runs,
            &work,
            false,
            source.manifest_sha256,
            source.transaction_sha256,
            2,
            3,
            2,
        )
        .unwrap();
        let manifest = OwnerPostingsManifest {
            schema_version: OWNER_POSTINGS_SCHEMA_VERSION,
            artifact_kind: OwnerPostingsManifest::ARTIFACT_KIND.to_owned(),
            complete: false,
            canary_max_transactions: Some(2),
            transactions: 2,
            created_unix_seconds: 1,
            source: PostingsSourceBinding {
                manifest_file: DUMP_MANIFEST_FILE.to_owned(),
                manifest_bytes: source.manifest_handle.len(),
                manifest_sha256: hex_digest(source.manifest_sha256),
                transaction_file: TRANSACTIONS_FILE.to_owned(),
                transaction_bytes: source.transaction_bytes,
                transaction_sha256: hex_digest(source.transaction_sha256),
                registry_file: PUBKEY_REGISTRY_FILE.to_owned(),
                registry_bytes: source.registry_bytes,
                registry_sha256: hex_digest(source.registry_sha256),
                accounts_file: ACCOUNTS_FILE.to_owned(),
                accounts_bytes: source.accounts_bytes,
                accounts_sha256: hex_digest(source.accounts_sha256),
                transactions: source.manifest.transactions,
                pubkeys: source.pubkeys,
                accounts: source.manifest.discovered_account_count.unwrap(),
            },
            replay_semantic_version: OWNER_REPLAY_SEMANTIC_VERSION.to_owned(),
            replay_state_sha256: "11".repeat(32),
            owner_semantic_sha256: hex_digest(merged.owner_semantic_sha256),
            owner_directory: merged.owner_directory,
            owner_postings: merged.owner_postings_binding,
            balance_history_manifest: None,
        };
        manifest.validate().unwrap();
        validate_owner_manifest_headers(&manifest).unwrap();
        publish_owner_postings(&output, &work, &runs_root, &sorter.runs, &manifest, &source)
            .unwrap();
    }

    fn add_manual_owner_balance_history(fixture: &PostingFixture) {
        let source = load_source_dump(&fixture.dump).unwrap();
        let mut owner_manifest: OwnerPostingsManifest = serde_json::from_slice(
            &fs::read(fixture.owner_output.join(OWNER_POSTINGS_MANIFEST_FILE)).unwrap(),
        )
        .unwrap();
        let work = fixture.owner_output.join(".balance-history-test");
        let runs_root = work.join("runs");
        fs::create_dir_all(&runs_root).unwrap();
        let mut sorter =
            BalanceHistorySorter::new(&runs_root, BALANCE_HISTORY_WORK_ROW_BYTES * 2).unwrap();
        for row in [
            BalanceHistoryWorkRow {
                owner_registry_id: 5,
                event: OwnerBalanceEventRecord {
                    transaction_id: 1,
                    slot: 1_002,
                    block_time: Some(1_002),
                    raw_delta: 3,
                    post_raw_balance: 3,
                },
            },
            BalanceHistoryWorkRow {
                owner_registry_id: 4,
                event: OwnerBalanceEventRecord {
                    transaction_id: 1,
                    slot: 1_002,
                    block_time: Some(1_002),
                    raw_delta: -3,
                    post_raw_balance: 7,
                },
            },
            BalanceHistoryWorkRow {
                owner_registry_id: 4,
                event: OwnerBalanceEventRecord {
                    transaction_id: 0,
                    slot: 1_001,
                    block_time: Some(1_001),
                    raw_delta: 10,
                    post_raw_balance: 10,
                },
            },
        ] {
            sorter.push(row).unwrap();
        }
        sorter.flush_run().unwrap();
        assert_eq!(sorter.runs.len(), 2);
        let merged = merge_owner_balance_history_runs(
            &sorter.runs,
            &work,
            false,
            source.manifest_sha256,
            source.transaction_sha256,
            2,
            3,
            2,
        )
        .unwrap();
        let manifest = OwnerBalanceHistoryManifest {
            schema_version: OWNER_BALANCE_HISTORY_SCHEMA_VERSION,
            artifact_kind: OwnerBalanceHistoryManifest::ARTIFACT_KIND.to_owned(),
            complete: false,
            canary_max_transactions: Some(2),
            transactions: 2,
            created_unix_seconds: 1,
            source: owner_manifest.source.clone(),
            replay_semantic_version: owner_manifest.replay_semantic_version.clone(),
            replay_state_sha256: owner_manifest.replay_state_sha256.clone(),
            owner_postings_semantic_sha256: owner_manifest.owner_semantic_sha256.clone(),
            history_semantic_version: OWNER_BALANCE_HISTORY_SEMANTIC_VERSION.to_owned(),
            history_semantic_sha256: hex_digest(merged.semantic_sha256),
            owner_directory: merged.owner_directory,
            balance_events: merged.balance_events,
        };
        manifest.validate().unwrap();
        validate_owner_balance_history_manifest_headers(&manifest).unwrap();
        for kind in [
            OwnerBalanceHistoryFileKind::Directory,
            OwnerBalanceHistoryFileKind::Events,
        ] {
            fs::rename(
                work.join(format!("{}.partial", kind.file_name())),
                fixture.owner_output.join(kind.file_name()),
            )
            .unwrap();
        }
        for run in sorter.runs {
            fs::remove_file(run.path).unwrap();
        }
        fs::remove_dir(runs_root).unwrap();
        fs::remove_dir(work).unwrap();
        let bytes = pretty_json_bytes(&manifest).unwrap();
        fs::write(
            fixture
                .owner_output
                .join(OWNER_BALANCE_HISTORY_MANIFEST_FILE),
            bytes,
        )
        .unwrap();
        owner_manifest.balance_history_manifest = Some(OwnerBalanceHistoryManifestBinding {
            file: OWNER_BALANCE_HISTORY_MANIFEST_FILE.to_owned(),
            bytes: fs::metadata(
                fixture
                    .owner_output
                    .join(OWNER_BALANCE_HISTORY_MANIFEST_FILE),
            )
            .unwrap()
            .len(),
            sha256: hex_digest(sha256_bytes(
                &fs::read(
                    fixture
                        .owner_output
                        .join(OWNER_BALANCE_HISTORY_MANIFEST_FILE),
                )
                .unwrap(),
            )),
        });
        owner_manifest.validate().unwrap();
        fs::write(
            fixture.owner_output.join(OWNER_POSTINGS_MANIFEST_FILE),
            pretty_json_bytes(&owner_manifest).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn owner_merge_store_and_verifier_round_trip_paged_ranges() {
        let fixture = posting_fixture();
        build_manual_owner_artifact(&fixture);
        let store = OwnerPostingsStore::open_with_options(
            &fixture.dump,
            &fixture.owner_output,
            PostingsOpenOptions {
                allow_incomplete: true,
            },
        )
        .unwrap();
        assert!(!store.complete());
        assert_eq!(store.owner_key_count(), 2);
        assert_eq!(store.owner_posting_count(), 3);
        assert!(!store.has_balance_history());

        let first = store.lookup([4; 32], 0, 1).unwrap().unwrap();
        assert_eq!(first.registry_id, 4);
        assert_eq!(first.total, 2);
        assert_eq!(first.transaction_ordinals, [0]);
        assert_eq!(first.next_offset, Some(1));
        let second = store.lookup([4; 32], 1, 1).unwrap().unwrap();
        assert_eq!(second.transaction_ordinals, [1]);
        assert_eq!(second.next_offset, None);
        assert_eq!(
            store
                .lookup([5; 32], 0, MAX_POSTINGS_PAGE_ROWS)
                .unwrap()
                .unwrap()
                .transaction_ordinals,
            [1]
        );
        assert!(store.lookup([6; 32], 0, 1).unwrap().is_none());
        drop(store);

        let verified =
            crate::verify_owner_postings_artifact(&fixture.dump, &fixture.owner_output, true)
                .unwrap();
        assert_eq!(verified.owner_keys, 2);
        assert_eq!(verified.owner_postings, 3);

        let path = fixture.owner_output.join(OWNER_DIRECTORY_FILE);
        let mut bytes = fs::read(&path).unwrap();
        bytes[24] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(
            OwnerPostingsStore::open_with_options(
                &fixture.dump,
                &fixture.owner_output,
                PostingsOpenOptions {
                    allow_incomplete: true,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn owner_balance_history_is_exact_sparse_ranged_and_sampled() {
        let fixture = posting_fixture();
        build_manual_owner_artifact(&fixture);
        add_manual_owner_balance_history(&fixture);
        let store = OwnerPostingsStore::open_with_options(
            &fixture.dump,
            &fixture.owner_output,
            PostingsOpenOptions {
                allow_incomplete: true,
            },
        )
        .unwrap();
        assert!(store.has_balance_history());
        assert_eq!(store.balance_history_owner_key_count(), 2);
        assert_eq!(store.balance_history_event_count(), 3);

        let first = store
            .lookup_balance_history([4; 32], 0, 1)
            .unwrap()
            .unwrap();
        assert_eq!(first.total, 2);
        assert_eq!(first.events[0].raw_delta, 10);
        assert_eq!(first.events[0].post_raw_balance, 10);
        assert_eq!(first.next_offset, Some(1));
        let last_only = store
            .lookup_balance_history_range(
                [4; 32],
                crate::postings_store::OwnerBalanceHistoryRangeQuery {
                    transaction_id_from: None,
                    transaction_id_to: None,
                    max_points: 1,
                },
            )
            .unwrap()
            .unwrap();
        assert!(last_only.sampled);
        assert_eq!(last_only.matching_events, 2);
        assert_eq!(last_only.events[0].transaction_id, 1);
        assert_eq!(last_only.events[0].raw_delta, -3);
        assert_eq!(last_only.events[0].post_raw_balance, 7);
        let bounded = store
            .lookup_balance_history_range(
                [4; 32],
                crate::postings_store::OwnerBalanceHistoryRangeQuery {
                    transaction_id_from: Some(1),
                    transaction_id_to: Some(1),
                    max_points: 10,
                },
            )
            .unwrap()
            .unwrap();
        assert!(!bounded.sampled);
        assert_eq!(bounded.events.len(), 1);
        assert_eq!(bounded.events[0].transaction_id, 1);
        assert!(
            store
                .lookup_balance_history([6; 32], 0, 1)
                .unwrap()
                .is_none()
        );
        drop(store);

        let verified =
            crate::verify_owner_postings_artifact(&fixture.dump, &fixture.owner_output, true)
                .unwrap();
        assert!(verified.balance_history_available);
        assert_eq!(verified.balance_history_owner_keys, 2);
        assert_eq!(verified.balance_history_events, 3);

        let event_path = fixture.owner_output.join(OWNER_BALANCE_EVENTS_FILE);
        let mut bytes = fs::read(&event_path).unwrap();
        let second_post_balance =
            crate::owner_balance_history_format::OWNER_BALANCE_HISTORY_HEADER_BYTES
                + crate::owner_balance_history_format::OWNER_BALANCE_EVENT_RECORD_BYTES
                + 40;
        bytes[second_post_balance] ^= 1;
        fs::write(event_path, bytes).unwrap();
        assert!(
            OwnerPostingsStore::open_with_options(
                &fixture.dump,
                &fixture.owner_output,
                PostingsOpenOptions {
                    allow_incomplete: true,
                },
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn owner_api_serves_health_details_and_manifest_bound_pagination() {
        let fixture = posting_fixture();
        build_index(&BuildConfig {
            dump: fixture.dump.clone(),
            output: fixture.index.clone(),
            max_transactions: Some(2),
        })
        .unwrap();
        build_manual_owner_artifact(&fixture);
        let query = Arc::new(
            QueryStore::open_with_options(
                &fixture.dump,
                &fixture.index,
                QueryOpenOptions {
                    allow_incomplete: true,
                },
            )
            .unwrap(),
        );
        let owners = Arc::new(
            OwnerPostingsStore::open_with_options(
                &fixture.dump,
                &fixture.owner_output,
                PostingsOpenOptions {
                    allow_incomplete: true,
                },
            )
            .unwrap(),
        );
        let app = router_with_all_indexes(query, None, Some(owners), None, None, "*", 2).unwrap();

        let health = app
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), 200);
        let health: serde_json::Value =
            serde_json::from_slice(&to_bytes(health.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(health["postings"]["owner"], true);
        assert_eq!(health["postings"]["owner_keys"], 2);
        assert_eq!(health["postings"]["owner_postings"], 3);
        assert_eq!(health["postings"]["owner_balance_history"], false);
        assert_eq!(health["postings"]["owner_balance_history_keys"], 0);
        assert_eq!(health["postings"]["owner_balance_history_events"], 0);

        let owner = bs58::encode([4u8; 32]).into_string();
        let unavailable = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/accounts/{owner}/balance-history?max_points=10"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unavailable.status(), 501);
        let unavailable: serde_json::Value =
            serde_json::from_slice(&to_bytes(unavailable.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(unavailable["error"], "owner_balance_history_not_available");

        let first = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/postings/owner/{owner}?limit=1"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), 200);
        let first: serde_json::Value =
            serde_json::from_slice(&to_bytes(first.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(first["kind"], "owner");
        assert_eq!(first["registry_id"], 4);
        assert_eq!(first["total"], 2);
        assert_eq!(first["items"][0]["transaction_id"], 0);
        let cursor = first["next_cursor"].as_str().unwrap();

        let second = app
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/owner/{owner}?limit=1&cursor={cursor}"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), 200);
        let second: serde_json::Value =
            serde_json::from_slice(&to_bytes(second.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(second["items"][0]["transaction_id"], 1);
        assert!(second["next_cursor"].is_null());
    }

    #[tokio::test]
    async fn owner_balance_history_api_serves_exact_strings_ranges_and_health() {
        let fixture = posting_fixture();
        build_index(&BuildConfig {
            dump: fixture.dump.clone(),
            output: fixture.index.clone(),
            max_transactions: Some(2),
        })
        .unwrap();
        build_manual_owner_artifact(&fixture);
        add_manual_owner_balance_history(&fixture);
        let query = Arc::new(
            QueryStore::open_with_options(
                &fixture.dump,
                &fixture.index,
                QueryOpenOptions {
                    allow_incomplete: true,
                },
            )
            .unwrap(),
        );
        let owners = Arc::new(
            OwnerPostingsStore::open_with_options(
                &fixture.dump,
                &fixture.owner_output,
                PostingsOpenOptions {
                    allow_incomplete: true,
                },
            )
            .unwrap(),
        );
        let app = router_with_all_indexes(query, None, Some(owners), None, None, "*", 2).unwrap();

        let health = app
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let health: serde_json::Value =
            serde_json::from_slice(&to_bytes(health.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(health["postings"]["owner_balance_history"], true);
        assert_eq!(health["postings"]["owner_balance_history_keys"], 2);
        assert_eq!(health["postings"]["owner_balance_history_events"], 3);

        let owner = bs58::encode([4u8; 32]).into_string();
        let response = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/accounts/{owner}/balance-history?transaction_id_from=0&transaction_id_to=1&max_points=1"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let response: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(response["supported"], true);
        assert_eq!(response["artifact_complete"], false);
        assert_eq!(response["address"], owner);
        assert_eq!(response["registry_id"], 4);
        assert_eq!(response["matching_events"], 2);
        assert_eq!(response["sampled"], true);
        assert_eq!(response["items"][0]["transaction_id"], 1);
        assert_eq!(response["items"][0]["slot"], 1_002);
        assert_eq!(response["items"][0]["raw_delta"], "-3");
        assert_eq!(response["items"][0]["post_raw_balance"], "7");

        let reversed = app
            .oneshot(
                Request::get(format!(
                    "/api/v1/accounts/{owner}/balance-history?transaction_id_from=2&transaction_id_to=1"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reversed.status(), 400);
        let reversed: serde_json::Value =
            serde_json::from_slice(&to_bytes(reversed.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(reversed["error"], "invalid_balance_history_range");
    }

    #[test]
    fn canary_build_writes_exact_canonical_files_and_semantic_digests() {
        let fixture = posting_fixture();
        let summary = build_postings(&PostingsBuildConfig {
            dump: fixture.dump.clone(),
            output: fixture.output.clone(),
            max_transactions: Some(2),
        })
        .unwrap();
        assert!(!summary.complete);
        assert_eq!(summary.transactions, 2);
        assert_eq!(summary.transactions_with_target, 2);
        assert_eq!(summary.sort_runs, 1);
        assert_eq!(summary.target_keys, 3);
        assert_eq!(summary.target_postings, 4);
        assert_eq!(summary.program_keys, 2);
        assert_eq!(summary.program_postings, 3);
        assert_eq!(summary.program_direct_postings, 2);
        assert_eq!(summary.program_inner_postings, 2);
        assert_eq!(summary.total_postings, 7);
        assert!(!fixture.output.join(WORK_DIRECTORY).exists());

        let manifest: PostingsManifest =
            serde_json::from_slice(&fs::read(fixture.output.join(POSTINGS_MANIFEST_FILE)).unwrap())
                .unwrap();
        manifest.validate().unwrap();
        assert!(!manifest.complete);
        assert_eq!(manifest.canary_max_transactions, Some(2));
        assert_eq!(manifest.transactions, 2);
        assert_eq!(manifest.source.accounts, 2);
        assert_eq!(
            manifest.target_address_semantic_sha256,
            "735581bf8245f0465897d1b2f1da7dfe22b9a54ebf06b9074cbef1a3d37b98b4"
        );
        assert_eq!(
            manifest.program_semantic_sha256,
            "8ae999c99383e01409209b75bc46ac7edaa7fb401a2b77f6d481926c51e18117"
        );

        let (target_directory, target_postings) = read_posting_pair(
            &fixture.output,
            PostingsDirectoryKind::TargetAddress,
            &manifest,
        );
        assert_eq!(
            target_directory,
            vec![
                PostingsDirectoryRecord {
                    registry_id: 2,
                    flags: TARGET_ADDRESS_FLAG_MINT,
                    first_posting_row: 0,
                    posting_count: 2,
                },
                PostingsDirectoryRecord {
                    registry_id: 3,
                    flags: TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                    first_posting_row: 2,
                    posting_count: 2,
                },
                PostingsDirectoryRecord {
                    registry_id: 6,
                    flags: TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                    first_posting_row: 4,
                    posting_count: 0,
                },
            ]
        );
        assert_eq!(
            target_postings,
            vec![
                PostingRecord {
                    transaction_ordinal: 0
                },
                PostingRecord {
                    transaction_ordinal: 1
                },
                PostingRecord {
                    transaction_ordinal: 0
                },
                PostingRecord {
                    transaction_ordinal: 1
                },
            ]
        );
        let (program_directory, program_postings) =
            read_program_posting_pair(&fixture.output, ProgramInstructionScope::All, &manifest);
        assert_eq!(
            program_directory,
            vec![
                PostingsDirectoryRecord {
                    registry_id: 1,
                    flags: 0,
                    first_posting_row: 0,
                    posting_count: 2,
                },
                PostingsDirectoryRecord {
                    registry_id: 4,
                    flags: 0,
                    first_posting_row: 2,
                    posting_count: 1,
                },
            ]
        );
        assert_eq!(
            program_postings,
            vec![
                ProgramPostingRecord {
                    transaction_ordinal: 0,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_MASK,
                },
                ProgramPostingRecord {
                    transaction_ordinal: 1,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
                },
                ProgramPostingRecord {
                    transaction_ordinal: 1,
                    instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_INNER,
                },
            ]
        );
        assert_eq!(
            hex_digest(
                crate::postings_format::postings_semantic_sha256(
                    PostingsDirectoryKind::TargetAddress,
                    &target_directory,
                    &target_postings,
                    2,
                )
                .unwrap(),
            ),
            manifest.target_address_semantic_sha256
        );
        assert_eq!(
            hex_digest(
                crate::postings_format::program_postings_semantic_sha256(
                    ProgramInstructionScope::All,
                    &program_directory,
                    &program_postings,
                    2,
                )
                .unwrap(),
            ),
            manifest.program_semantic_sha256
        );
        assert_eq!(
            summary.target_address_semantic_sha256,
            manifest.target_address_semantic_sha256
        );
        assert_eq!(
            summary.program_semantic_sha256,
            manifest.program_semantic_sha256
        );
        let (direct_directory, direct_postings) =
            read_program_posting_pair(&fixture.output, ProgramInstructionScope::Direct, &manifest);
        assert_eq!(direct_directory[0].posting_count, 2);
        assert_eq!(direct_directory[1].posting_count, 0);
        assert_eq!(
            direct_postings
                .iter()
                .map(|posting| posting.transaction_ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let (inner_directory, inner_postings) =
            read_program_posting_pair(&fixture.output, ProgramInstructionScope::Inner, &manifest);
        assert_eq!(inner_directory[0].posting_count, 1);
        assert_eq!(inner_directory[1].posting_count, 1);
        assert_eq!(
            inner_postings
                .iter()
                .map(|posting| posting.transaction_ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    #[test]
    fn canary_build_contains_only_the_requested_transaction_prefix() {
        let fixture = posting_fixture();
        let summary = build_postings(&PostingsBuildConfig {
            dump: fixture.dump.clone(),
            output: fixture.output.clone(),
            max_transactions: Some(1),
        })
        .unwrap();
        assert_eq!(summary.transactions, 1);
        assert_eq!(summary.target_postings, 2);
        assert_eq!(summary.program_postings, 1);
        assert_eq!(summary.program_direct_postings, 1);
        assert_eq!(summary.program_inner_postings, 1);
        let manifest: PostingsManifest =
            serde_json::from_slice(&fs::read(fixture.output.join(POSTINGS_MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(manifest.transactions, 1);
        assert_eq!(manifest.canary_max_transactions, Some(1));
        let (_, target_postings) = read_posting_pair(
            &fixture.output,
            PostingsDirectoryKind::TargetAddress,
            &manifest,
        );
        let (_, program_postings) =
            read_program_posting_pair(&fixture.output, ProgramInstructionScope::All, &manifest);
        assert_eq!(
            target_postings,
            vec![
                PostingRecord {
                    transaction_ordinal: 0,
                },
                PostingRecord {
                    transaction_ordinal: 0,
                },
            ]
        );
        assert_eq!(
            program_postings,
            vec![ProgramPostingRecord {
                transaction_ordinal: 0,
                instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_MASK,
            }]
        );
    }

    #[tokio::test]
    async fn query_and_posting_canaries_serve_role_cursor_and_transaction_details() {
        let fixture = posting_fixture();
        build_index(&BuildConfig {
            dump: fixture.dump.clone(),
            output: fixture.index.clone(),
            max_transactions: Some(2),
        })
        .unwrap();
        build_postings(&PostingsBuildConfig {
            dump: fixture.dump.clone(),
            output: fixture.output.clone(),
            max_transactions: Some(2),
        })
        .unwrap();
        let query = Arc::new(
            QueryStore::open_with_options(
                &fixture.dump,
                &fixture.index,
                QueryOpenOptions {
                    allow_incomplete: true,
                },
            )
            .unwrap(),
        );
        let postings = Arc::new(
            PostingsStore::open_with_options(
                &fixture.dump,
                &fixture.output,
                PostingsOpenOptions {
                    allow_incomplete: true,
                },
            )
            .unwrap(),
        );
        let app = router_with_postings(query, Some(postings), "*", 2).unwrap();
        let mint = bs58::encode([2u8; 32]).into_string();
        let first = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/postings/target-address/{mint}?limit=1"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), 200);
        let first: serde_json::Value =
            serde_json::from_slice(&to_bytes(first.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(first["kind"], "target-address");
        assert_eq!(first["flags"], TARGET_ADDRESS_FLAG_MINT);
        assert_eq!(first["total"], 2);
        assert_eq!(first["items"][0]["transaction_id"], 0);
        assert_eq!(first["items"][0]["coordinate"]["slot"], 1_001);
        assert_eq!(
            first["items"][0]["first_signature"],
            bs58::encode([8u8; 64]).into_string()
        );
        let cursor = first["next_cursor"].as_str().unwrap();
        let second = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/target-address/{mint}?limit=1&cursor={cursor}"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), 200);
        let second: serde_json::Value =
            serde_json::from_slice(&to_bytes(second.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(second["items"][0]["transaction_id"], 1);
        assert!(second["next_cursor"].is_null());

        let mint_as_token = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/postings/token-account/{mint}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mint_as_token.status(), 404);
        let token = bs58::encode([3u8; 32]).into_string();
        let token_response = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/postings/token-account/{token}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(token_response.status(), 200);
        let program = bs58::encode([4u8; 32]).into_string();
        let program_response = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/postings/program/{program}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(program_response.status(), 200);
        let program_body: serde_json::Value = serde_json::from_slice(
            &to_bytes(program_response.into_body(), 1 << 20)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(program_body["total"], 1);
        assert_eq!(program_body["instruction_scope"], "all");
        assert_eq!(program_body["items"][0]["transaction_id"], 1);

        let direct_empty = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/program/{program}?instruction_scope=direct"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(direct_empty.status(), 200);
        let direct_empty: serde_json::Value =
            serde_json::from_slice(&to_bytes(direct_empty.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(direct_empty["instruction_scope"], "direct");
        assert_eq!(direct_empty["total"], 0);
        assert!(direct_empty["items"].as_array().unwrap().is_empty());

        let inner = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/program/{program}?instruction_scope=inner"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(inner.status(), 200);
        let inner: serde_json::Value =
            serde_json::from_slice(&to_bytes(inner.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(inner["instruction_scope"], "inner");
        assert_eq!(inner["items"][0]["transaction_id"], 1);

        let both_program = bs58::encode([1u8; 32]).into_string();
        let direct_first = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/program/{both_program}?instruction_scope=direct&limit=1"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(direct_first.status(), 200);
        let direct_first: serde_json::Value =
            serde_json::from_slice(&to_bytes(direct_first.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(direct_first["total"], 2);
        let direct_cursor = direct_first["next_cursor"].as_str().unwrap();
        let cross_scope = app
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/program/{both_program}?instruction_scope=inner&cursor={direct_cursor}"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_scope.status(), 400);
    }

    fn read_posting_pair(
        output: &Path,
        kind: PostingsDirectoryKind,
        manifest: &PostingsManifest,
    ) -> (Vec<PostingsDirectoryRecord>, Vec<PostingRecord>) {
        let (directory_kind, postings_kind) = match kind {
            PostingsDirectoryKind::TargetAddress => (
                PostingsFileKind::TargetAddressDirectory,
                PostingsFileKind::TargetAddressPostings,
            ),
            PostingsDirectoryKind::Program => (
                PostingsFileKind::ProgramDirectory,
                PostingsFileKind::ProgramPostings,
            ),
            PostingsDirectoryKind::Owner => panic!("owner postings use their separate format"),
        };
        let directory_bytes = fs::read(output.join(directory_kind.file_name())).unwrap();
        let postings_bytes = fs::read(output.join(postings_kind.file_name())).unwrap();
        let directory_header =
            PostingsFileHeader::decode(&directory_bytes, directory_kind).unwrap();
        let postings_header = PostingsFileHeader::decode(&postings_bytes, postings_kind).unwrap();
        assert!(!directory_header.complete);
        assert!(!postings_header.complete);
        manifest.validate_header(directory_header).unwrap();
        manifest.validate_header(postings_header).unwrap();
        assert_eq!(
            hex_digest(sha256_bytes(&directory_bytes)),
            manifest.binding(directory_kind).sha256
        );
        assert_eq!(
            hex_digest(sha256_bytes(&postings_bytes)),
            manifest.binding(postings_kind).sha256
        );
        let directory = directory_bytes[POSTINGS_HEADER_BYTES..]
            .chunks_exact(POSTINGS_DIRECTORY_RECORD_BYTES)
            .map(|bytes| PostingsDirectoryRecord::decode(bytes, kind).unwrap())
            .collect();
        let postings = postings_bytes[POSTINGS_HEADER_BYTES..]
            .chunks_exact(crate::postings_format::POSTINGS_BODY_RECORD_BYTES)
            .map(|bytes| PostingRecord::decode(bytes).unwrap())
            .collect();
        (directory, postings)
    }

    fn read_program_posting_pair(
        output: &Path,
        scope: ProgramInstructionScope,
        manifest: &PostingsManifest,
    ) -> (Vec<PostingsDirectoryRecord>, Vec<ProgramPostingRecord>) {
        let (directory_kind, postings_kind) = match scope {
            ProgramInstructionScope::All => (
                PostingsFileKind::ProgramDirectory,
                PostingsFileKind::ProgramPostings,
            ),
            ProgramInstructionScope::Direct => (
                PostingsFileKind::ProgramDirectDirectory,
                PostingsFileKind::ProgramDirectPostings,
            ),
            ProgramInstructionScope::Inner => (
                PostingsFileKind::ProgramInnerDirectory,
                PostingsFileKind::ProgramInnerPostings,
            ),
        };
        let directory_bytes = fs::read(output.join(directory_kind.file_name())).unwrap();
        let postings_bytes = fs::read(output.join(postings_kind.file_name())).unwrap();
        let directory_header =
            PostingsFileHeader::decode(&directory_bytes, directory_kind).unwrap();
        let postings_header = PostingsFileHeader::decode(&postings_bytes, postings_kind).unwrap();
        manifest.validate_header(directory_header).unwrap();
        manifest.validate_header(postings_header).unwrap();
        let directory = directory_bytes[POSTINGS_HEADER_BYTES..]
            .chunks_exact(POSTINGS_DIRECTORY_RECORD_BYTES)
            .map(|bytes| {
                PostingsDirectoryRecord::decode(bytes, PostingsDirectoryKind::Program).unwrap()
            })
            .collect();
        let postings = postings_bytes[POSTINGS_HEADER_BYTES..]
            .chunks_exact(crate::postings_format::POSTINGS_BODY_RECORD_BYTES)
            .map(|bytes| ProgramPostingRecord::decode(bytes).unwrap())
            .collect();
        (directory, postings)
    }

    #[test]
    fn work_rows_and_merge_fail_closed_for_corruption_and_duplicates() {
        let target = WorkRow::new(
            WorkKind::TargetAddress,
            u32::MAX,
            TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
            u64::MAX >> WorkRow::TRANSACTION_SHIFT,
        )
        .unwrap();
        let program = WorkRow::new(
            WorkKind::Program,
            1,
            u32::from(PROGRAM_INSTRUCTION_SCOPE_DIRECT),
            0,
        )
        .unwrap();
        assert_eq!(WorkRow::decode(&target.encode()).unwrap(), target);
        assert!(target < program);

        let temporary = tempfile::tempdir().unwrap();
        let truncated = temporary.path().join("truncated.bin");
        fs::write(&truncated, [0u8; WORK_ROW_BYTES - 1]).unwrap();
        assert!(merge_sorted_runs(&[run_binding(truncated, 1)], 0, 1, |_| Ok(())).is_err());

        let corrupt = temporary.path().join("corrupt.bin");
        let mut corrupt_bytes = target.encode();
        corrupt_bytes[7] = 0x80;
        fs::write(&corrupt, corrupt_bytes).unwrap();
        assert!(merge_sorted_runs(&[run_binding(corrupt, 1)], 1, 1, |_| Ok(())).is_err());

        let duplicate_a = temporary.path().join("duplicate-a.bin");
        let duplicate_b = temporary.path().join("duplicate-b.bin");
        let duplicate_row = WorkRow::new(
            WorkKind::TargetAddress,
            u32::MAX,
            TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
            0,
        )
        .unwrap();
        fs::write(&duplicate_a, duplicate_row.encode()).unwrap();
        fs::write(&duplicate_b, duplicate_row.encode()).unwrap();
        assert!(
            merge_sorted_runs(
                &[run_binding(duplicate_a, 1), run_binding(duplicate_b, 1)],
                2,
                u64::MAX,
                |_| Ok(()),
            )
            .is_err()
        );

        let out_of_range = temporary.path().join("out-of-range.bin");
        fs::write(
            &out_of_range,
            WorkRow::new(
                WorkKind::Program,
                1,
                u32::from(PROGRAM_INSTRUCTION_SCOPE_DIRECT),
                2,
            )
            .unwrap()
            .encode(),
        )
        .unwrap();
        assert!(merge_sorted_runs(&[run_binding(out_of_range, 1)], 1, 2, |_| Ok(())).is_err());

        let changed = temporary.path().join("changed.bin");
        fs::write(
            &changed,
            WorkRow::new(
                WorkKind::Program,
                1,
                u32::from(PROGRAM_INSTRUCTION_SCOPE_DIRECT),
                0,
            )
            .unwrap()
            .encode(),
        )
        .unwrap();
        let changed_binding = run_binding(changed.clone(), 1);
        fs::write(
            &changed,
            WorkRow::new(
                WorkKind::Program,
                2,
                u32::from(PROGRAM_INSTRUCTION_SCOPE_DIRECT),
                0,
            )
            .unwrap()
            .encode(),
        )
        .unwrap();
        assert!(merge_sorted_runs(&[changed_binding], 1, 1, |_| Ok(())).is_err());
    }

    fn run_binding(path: PathBuf, rows: u64) -> PostingRunBinding {
        PostingRunBinding {
            sha256: sha256_bytes(&fs::read(&path).unwrap()),
            path,
            rows,
        }
    }

    #[test]
    fn full_spyx_gates_are_exact_and_canary_limit_must_be_positive() {
        let expected_digest = crate::index_format::parse_hex_digest(
            FULL_SOURCE_TRANSACTION_SHA256,
            "test full source digest",
        )
        .unwrap();
        validate_full_source_gate(expected_digest, FULL_TRANSACTIONS, FULL_ACCOUNTS).unwrap();
        assert!(validate_full_source_gate([0; 32], FULL_TRANSACTIONS, FULL_ACCOUNTS).is_err());
        assert!(
            validate_full_source_gate(expected_digest, FULL_TRANSACTIONS - 1, FULL_ACCOUNTS)
                .is_err()
        );
        assert!(
            validate_full_source_gate(expected_digest, FULL_TRANSACTIONS, FULL_ACCOUNTS - 1)
                .is_err()
        );
        validate_full_projection_gate(
            FULL_TRANSACTIONS,
            FULL_TRANSACTIONS,
            FULL_TARGET_KEYS,
            FULL_TARGET_POSTINGS,
            FULL_PROGRAM_KEYS,
            FULL_PROGRAM_POSTINGS,
            FULL_TOTAL_POSTINGS,
        )
        .unwrap();
        assert!(
            validate_full_projection_gate(
                FULL_TRANSACTIONS,
                FULL_TRANSACTIONS - 1,
                FULL_TARGET_KEYS,
                FULL_TARGET_POSTINGS,
                FULL_PROGRAM_KEYS,
                FULL_PROGRAM_POSTINGS,
                FULL_TOTAL_POSTINGS,
            )
            .is_err()
        );
        assert!(
            build_postings(&PostingsBuildConfig {
                dump: PathBuf::from("unused"),
                output: PathBuf::from("unused"),
                max_transactions: Some(0),
            })
            .is_err()
        );
    }
}
