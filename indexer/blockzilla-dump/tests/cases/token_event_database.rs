use std::{num::NonZeroU32, path::Path};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use blockzilla_dump::{
    BlockCommitOutcome, TokenEventDatabase, TokenEventDatabaseError, TokenEventRunSpec,
};
use blockzilla_model::{
    ArchiveFormat, BlockHeader, CanonicalBlock, CanonicalTransaction, CoverageReason, CpiCoverage,
    ExecutionStatus, InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    ResolvedInstruction, ScanRange, SourceIdentity, SourceVerification, TransactionHeader,
    token::{
        CLASSIC_SPL_TOKEN_PROGRAM_ID, HistoryCoverage, MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
        TargetMintTracker, TargetMintTrackerSnapshot, TokenAccountLifecycle, TokenAccountState,
    },
};
use rusqlite::{Connection, types::ValueRef};
struct TempDir(tempfile::TempDir);

impl TempDir {
    fn new() -> std::io::Result<Self> {
        let base = std::fs::canonicalize(std::env::temp_dir())?;
        let directory = tempfile::TempDir::new_in(base)?;
        #[cfg(unix)]
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
        Ok(Self(directory))
    }

    fn path(&self) -> &Path {
        self.0.path()
    }
}

const TARGET: [u8; 32] = [1; 32];
const SOURCE_ACCOUNT: [u8; 32] = [2; 32];
const DESTINATION_ACCOUNT: [u8; 32] = [3; 32];
const AUTHORITY: [u8; 32] = [4; 32];
const RENT_SYSVAR: [u8; 32] = [5; 32];

fn source(block_count: u32) -> SourceIdentity {
    SourceIdentity {
        format: ArchiveFormat::CompactV2,
        label: "test-epoch".into(),
        cluster_id: Some("test-cluster".into()),
        epoch: 7,
        first_slot: 100,
        slots_per_epoch: 32,
        block_count,
        verification: SourceVerification::ObjectSetBound,
        binding: Some("sha256:test-binding".into()),
    }
}

fn spec(block_count: u32, opening_tracker: TargetMintTrackerSnapshot) -> TokenEventRunSpec {
    TokenEventRunSpec::classic(
        source(block_count),
        TARGET,
        ScanRange {
            first_block: 0,
            block_count: NonZeroU32::new(block_count).unwrap(),
        },
        opening_tracker,
    )
}

fn database_path(directory: &TempDir, name: &str) -> std::path::PathBuf {
    directory.path().join(name)
}

fn sqlite_sidecar(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

fn empty_block(block_ordinal: u32) -> CanonicalBlock {
    CanonicalBlock {
        counts: None,
        header: BlockHeader {
            epoch: 7,
            block_ordinal,
            slot: 100 + u64::from(block_ordinal),
        },
        transactions: Vec::new(),
    }
}

fn token_block(
    block_ordinal: u32,
    status: ExecutionStatus,
    accounts: Vec<[u8; 32]>,
    data: Vec<u8>,
) -> CanonicalBlock {
    CanonicalBlock {
        counts: None,
        header: BlockHeader {
            epoch: 7,
            block_ordinal,
            slot: 100 + u64::from(block_ordinal),
        },
        transactions: vec![CanonicalTransaction {
            header: TransactionHeader {
                tx_index: 0,
                status,
                failed_outer_instruction_index: matches!(status, ExecutionStatus::Failed)
                    .then_some(0),
                instruction_coverage: InstructionCoverage::Complete,
                cpi_coverage: CpiCoverage::Complete,
            },
            primary_signature: Some([block_ordinal as u8 + 10; 64]),
            token_balance_coverage: blockzilla_model::TokenBalanceCoverage::NotRequested,
            token_balances: Vec::new(),
            required_signers: vec![AUTHORITY],
            instructions: vec![ResolvedInstruction {
                coordinate: InstructionCoordinate {
                    order: 0,
                    outer_index: 0,
                    inner_index: None,
                    stack_height: None,
                },
                program_id: Some(CLASSIC_SPL_TOKEN_PROGRAM_ID),
                accounts,
                data_coverage: InstructionDataCoverage::Exact,
                data,
            }],
        }],
    }
}

fn transfer_data(amount: u64) -> Vec<u8> {
    let mut data = vec![3];
    data.extend_from_slice(&amount.to_le_bytes());
    data
}

fn commit_processed(
    database: &mut TokenEventDatabase,
    block: &CanonicalBlock,
) -> BlockCommitOutcome {
    database.track_and_commit_block(block.as_view()).unwrap()
}

fn committed_transfer_database(path: &Path) -> (TokenEventRunSpec, CanonicalBlock) {
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(8),
    );
    let mut database = TokenEventDatabase::create(path, run.clone()).unwrap();
    commit_processed(&mut database, &block);
    drop(database);
    (run, block)
}

fn count(path: &Path, table: &str) -> i64 {
    let connection = Connection::open(path).unwrap();
    connection
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

#[test]
fn empty_block_and_checkpoint_commit_together() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "empty.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let block = empty_block(0);

    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::Committed
    );
    let resume = database.resume_state().unwrap();
    assert_eq!(resume.next_block_ordinal, 1);
    assert_eq!(resume.tracker, tracker.snapshot());
    drop(database);
    assert_eq!(count(&path, "blocks"), 1);
    assert_eq!(count(&path, "transactions"), 0);
}

#[test]
fn block_sink_recovers_after_a_tracker_error() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "block-sink-recovery.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let valid = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(5),
    );
    let mut invalid = valid.clone();
    invalid.transactions[0].instructions[0].coordinate.order = 1;

    assert!(blockzilla_model::BlockSink::visit_block(&mut database, invalid.as_view()).is_err());
    blockzilla_model::BlockSink::visit_block(&mut database, valid.as_view()).unwrap();
    assert_eq!(database.next_block_ordinal(), 1);
}

#[test]
fn replay_of_a_committed_block_is_idempotent() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "replay.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(8),
    );
    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::Committed
    );
    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::AlreadyCommitted
    );
    drop(database);
    assert_eq!(count(&path, "blocks"), 1);
    assert_eq!(count(&path, "transactions"), 1);
    assert_eq!(count(&path, "events"), 1);
    assert_eq!(count(&path, "delta_legs"), 2);
}

#[test]
fn replay_rejects_changed_source_payload_accounts_coverage_and_signature() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "replay-source-digest.sqlite");
    let (run, block) = committed_transfer_database(&path);
    let mut database = TokenEventDatabase::open(&path, run).unwrap();

    let mut changed_payload = block.clone();
    changed_payload.transactions[0].instructions[0].data[1] ^= 1;
    assert!(
        database
            .track_and_commit_block(changed_payload.as_view())
            .is_err()
    );

    let mut changed_accounts = block.clone();
    changed_accounts.transactions[0].instructions[0]
        .accounts
        .swap(0, 1);
    assert!(
        database
            .track_and_commit_block(changed_accounts.as_view())
            .is_err()
    );

    let mut changed_coverage = block.clone();
    changed_coverage.transactions[0].header.cpi_coverage = CpiCoverage::NotRecorded;
    assert!(
        database
            .track_and_commit_block(changed_coverage.as_view())
            .is_err()
    );

    let mut changed_signature = block.clone();
    changed_signature.transactions[0].primary_signature = Some([99; 64]);
    assert!(
        database
            .track_and_commit_block(changed_signature.as_view())
            .is_err()
    );
    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::AlreadyCommitted
    );
}

#[test]
fn replay_reads_only_the_target_block_digest() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "targeted-replay.sqlite");
    let opening =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(2, opening.snapshot());
    let first = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(8),
    );
    let second = token_block(
        1,
        ExecutionStatus::Succeeded,
        vec![DESTINATION_ACCOUNT, SOURCE_ACCOUNT, AUTHORITY],
        transfer_data(3),
    );
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    commit_processed(&mut database, &first);
    commit_processed(&mut database, &second);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE events SET ui_amount = 'later-block-corruption'
              WHERE block_ordinal = 1",
            [],
        )
        .unwrap();
    drop(connection);

    assert_eq!(
        database.track_and_commit_block(first.as_view()).unwrap(),
        BlockCommitOutcome::AlreadyCommitted
    );
    assert!(database.resume_state().is_err());
}

#[test]
fn digest_audit_rejects_effect_kind_and_added_or_deleted_rows() {
    let directory = TempDir::new().unwrap();

    let effect_path = database_path(&directory, "digest-effect.sqlite");
    let (effect_run, _) = committed_transfer_database(&effect_path);
    let connection = Connection::open(&effect_path).unwrap();
    connection
        .execute_batch(
            "UPDATE event_effects SET effect_kind = 'burn', checked = NULL;
             DELETE FROM delta_legs WHERE leg_index = 1;
             UPDATE delta_legs SET transfer_role = NULL WHERE leg_index = 0;",
        )
        .unwrap();
    drop(connection);
    assert!(TokenEventDatabase::open(&effect_path, effect_run).is_err());

    let added_path = database_path(&directory, "digest-added.sqlite");
    let (added_run, _) = committed_transfer_database(&added_path);
    let connection = Connection::open(&added_path).unwrap();
    connection
        .execute(
            "INSERT INTO coverage_issues (
                block_ordinal, tx_index, issue_index, issue_kind
             ) VALUES (0, 0, 0, 'cpi-not-recorded')",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(TokenEventDatabase::open(&added_path, added_run).is_err());

    let deleted_path = database_path(&directory, "digest-deleted.sqlite");
    let (deleted_run, _) = committed_transfer_database(&deleted_path);
    let connection = Connection::open(&deleted_path).unwrap();
    connection
        .execute("DELETE FROM event_accounts WHERE binding_index = 0", [])
        .unwrap();
    drop(connection);
    assert!(TokenEventDatabase::open(&deleted_path, deleted_run).is_err());
}

#[test]
fn read_only_audit_matches_the_writer_audit() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "read-only-audit.sqlite");
    let (run, _) = committed_transfer_database(&path);

    let stored = TokenEventDatabase::audit_read_only(&path).unwrap();
    let expected = TokenEventDatabase::audit_read_only_expected(&path, &run).unwrap();
    let writer = TokenEventDatabase::open(&path, run.clone()).unwrap();

    assert_eq!(stored, expected);
    assert_eq!(stored.spec, run);
    assert_eq!(stored.resume, writer.resume_state().unwrap());
    assert_ne!(stored.digest_head, [0; 32]);
    assert_ne!(stored.tracker_digest, [0; 32]);
}

#[test]
fn read_only_expected_audit_rejects_a_wrong_specification() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "read-only-wrong-spec.sqlite");
    let (mut run, _) = committed_transfer_database(&path);
    run.source.binding = Some("sha256:different-source".into());

    assert!(matches!(
        TokenEventDatabase::audit_read_only_expected(&path, &run),
        Err(TokenEventDatabaseError::SpecificationMismatch(_))
    ));
}

#[test]
fn read_only_audit_rejects_a_tampered_prior_lifetime() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "read-only-prior-lifetime.sqlite");
    let tracker = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let run = spec(2, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let close = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        vec![9],
    );
    let reuse = token_block(
        1,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, TARGET, AUTHORITY, RENT_SYSVAR],
        vec![1],
    );
    commit_processed(&mut database, &close);
    commit_processed(&mut database, &reuse);
    drop(database);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE account_lifetimes
                SET account_state = 'active-target', state_mint_pubkey_id = NULL
              WHERE generation_text = '1'",
            [],
        )
        .unwrap();
    drop(connection);

    let error = TokenEventDatabase::audit_read_only(&path).unwrap_err();
    assert!(
        matches!(
        &error,
        TokenEventDatabaseError::InvalidCheckpoint(reason)
            if reason.contains("materialized lifetime")
        ),
        "unexpected audit error: {error:?}"
    );
}

#[test]
fn exact_materialization_rejects_an_extra_lifetime() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "read-only-extra-lifetime.sqlite");
    let tracker = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    drop(TokenEventDatabase::create(&path, run).unwrap());

    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "INSERT INTO account_lifetimes (
                pubkey_id, generation_le, generation_text, account_state,
                state_mint_pubkey_id, confirmed_revision_le,
                confirmed_revision_text
             )
             SELECT pubkey_id, ?1, '2', 'closed', NULL, ?2, '0'
               FROM pubkeys WHERE address = ?3",
            rusqlite::params![
                2u64.to_le_bytes().as_slice(),
                0u64.to_le_bytes().as_slice(),
                SOURCE_ACCOUNT.as_slice(),
            ],
        )
        .unwrap();
    drop(connection);

    let error = TokenEventDatabase::audit_read_only(&path).unwrap_err();
    assert!(
        matches!(
            &error,
            TokenEventDatabaseError::InvalidCheckpoint(reason)
                if reason.contains("has no opening row, lifecycle effect, or tracker update")
        ),
        "unexpected audit error: {error:?}"
    );
}

#[test]
fn read_only_audit_rejects_an_orphan_pubkey() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "read-only-orphan-pubkey.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    drop(TokenEventDatabase::create(&path, run).unwrap());

    let connection = Connection::open(&path).unwrap();
    connection
        .execute("INSERT INTO pubkeys (address) VALUES (?1)", [[99u8; 32]])
        .unwrap();
    drop(connection);

    let error = TokenEventDatabase::audit_read_only(&path).unwrap_err();
    assert!(
        matches!(
        &error,
        TokenEventDatabaseError::InvalidCheckpoint(reason)
            if reason.contains("is unreferenced")
        ),
        "unexpected audit error: {error:?}"
    );
}

#[test]
fn replay_fails_closed_when_durable_event_rows_changed() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "replay-corruption.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(8),
    );
    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::Committed
    );
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("UPDATE events SET ui_amount = 'fabricated'", [])
        .unwrap();
    drop(connection);

    assert!(matches!(
        database.track_and_commit_block(block.as_view()),
        Err(TokenEventDatabaseError::Poisoned(_))
    ));
}

#[test]
fn a_late_block_error_rolls_back_every_row_and_checkpoint() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "rollback.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run.clone()).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(9),
    );
    let failing_connection = Connection::open(&path).unwrap();
    failing_connection
        .execute_batch(
            "CREATE TRIGGER fail_event_insert
             BEFORE INSERT ON events
             BEGIN
                 SELECT RAISE(ABORT, 'forced test failure');
             END;",
        )
        .unwrap();
    drop(failing_connection);

    assert!(matches!(
        database.track_and_commit_block(block.as_view()),
        Err(TokenEventDatabaseError::Poisoned(_))
    ));
    assert_eq!(count(&path, "blocks"), 0);
    assert_eq!(count(&path, "transactions"), 0);
    assert_eq!(count(&path, "events"), 0);

    let repairing_connection = Connection::open(&path).unwrap();
    repairing_connection
        .execute_batch("DROP TRIGGER fail_event_insert")
        .unwrap();
    drop(repairing_connection);
    drop(database);
    let mut database = TokenEventDatabase::open(&path, run).unwrap();
    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::Committed
    );
    drop(database);
    assert_eq!(count(&path, "blocks"), 1);
    assert_eq!(count(&path, "transactions"), 1);
    assert_eq!(count(&path, "events"), 1);
}

#[test]
fn a_second_transaction_failure_rolls_back_the_first_transaction_too() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "second-transaction-rollback.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run.clone()).unwrap();
    let mut block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(9),
    );
    let mut second = block.transactions[0].clone();
    second.header.tx_index = 1;
    second.primary_signature = Some([77; 64]);
    second.instructions[0].data = transfer_data(4);
    block.transactions.push(second);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_second_transaction
             BEFORE INSERT ON events WHEN NEW.tx_index = 1
             BEGIN
                 SELECT RAISE(ABORT, 'forced second transaction failure');
             END;",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        database.track_and_commit_block(block.as_view()),
        Err(TokenEventDatabaseError::Poisoned(_))
    ));
    assert_eq!(count(&path, "blocks"), 0);
    assert_eq!(count(&path, "transactions"), 0);
    assert_eq!(count(&path, "events"), 0);
    assert_eq!(count(&path, "tracker_account_updates"), 0);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_second_transaction")
        .unwrap();
    drop(connection);
    drop(database);
    assert_eq!(
        TokenEventDatabase::open(&path, run)
            .unwrap()
            .next_block_ordinal(),
        0
    );
}

#[test]
fn wal_checkpoint_rejects_a_busy_reader_and_then_completes() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "busy-checkpoint.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(2, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    commit_processed(&mut database, &empty_block(0));

    let reader = Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader
        .query_row("SELECT count(*) FROM blocks", [], |row| row.get(0))
        .unwrap();
    commit_processed(&mut database, &empty_block(1));

    assert!(database.checkpoint_wal().is_err());
    reader.execute_batch("COMMIT").unwrap();
    database.checkpoint_wal().unwrap();
}

#[test]
fn open_rejects_unexpected_triggers_and_schema_objects() {
    let directory = TempDir::new().unwrap();

    let trigger_path = database_path(&directory, "unexpected-trigger.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let trigger_run = spec(1, tracker.snapshot());
    drop(TokenEventDatabase::create(&trigger_path, trigger_run.clone()).unwrap());
    let connection = Connection::open(&trigger_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER unexpected_trigger
             AFTER INSERT ON blocks BEGIN SELECT 1; END;",
        )
        .unwrap();
    drop(connection);
    assert!(TokenEventDatabase::open(&trigger_path, trigger_run).is_err());

    let table_path = database_path(&directory, "unexpected-table.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let table_run = spec(1, tracker.snapshot());
    drop(TokenEventDatabase::create(&table_path, table_run.clone()).unwrap());
    let connection = Connection::open(&table_path).unwrap();
    connection
        .execute_batch("CREATE TABLE unexpected_table (value INTEGER) STRICT")
        .unwrap();
    drop(connection);
    assert!(TokenEventDatabase::open(&table_path, table_run).is_err());
}

#[test]
fn reopen_rejects_durable_rows_above_the_resource_limit() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "durable-over-limit.sqlite");
    let (run, _) = committed_transfer_database(&path);
    let oversized = MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE events SET trailing_data = zeroblob(?1)",
            [i64::try_from(oversized).unwrap()],
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        TokenEventDatabase::open(&path, run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(reason))
            if reason.contains("storage bounds")
    ));
}

#[test]
fn reopen_rejects_an_overlong_source_identity_string() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "overlong-source-identity.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    drop(TokenEventDatabase::create(&path, run.clone()).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE run_identity SET source_label = ?1",
            ["x".repeat(4_097)],
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        TokenEventDatabase::open(&path, run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(reason))
            if reason.contains("source identity string")
    ));
}

#[test]
fn oversized_source_payload_is_rejected_before_block_insert() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "oversize-event.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        vec![0; MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION + 1],
    );

    assert!(database.track_and_commit_block(block.as_view()).is_err());
    assert_eq!(database.resume_state().unwrap().next_block_ordinal, 0);
    drop(database);
    assert_eq!(count(&path, "blocks"), 0);
    assert_eq!(count(&path, "events"), 0);
}

#[test]
fn one_shot_and_resume_have_byte_identical_logical_rows() {
    let directory = TempDir::new().unwrap();
    let one_shot_path = database_path(&directory, "one-shot.sqlite");
    let resumed_path = database_path(&directory, "resumed.sqlite");
    let opening =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(2, opening.snapshot());
    let blocks = [
        token_block(
            0,
            ExecutionStatus::Succeeded,
            vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
            transfer_data(11),
        ),
        token_block(
            1,
            ExecutionStatus::Succeeded,
            vec![DESTINATION_ACCOUNT, SOURCE_ACCOUNT, AUTHORITY],
            transfer_data(7),
        ),
    ];

    let mut one_shot = TokenEventDatabase::create(&one_shot_path, run.clone()).unwrap();
    for block in &blocks {
        commit_processed(&mut one_shot, block);
    }
    one_shot.checkpoint_wal().unwrap();
    drop(one_shot);

    let mut resumed = TokenEventDatabase::create(&resumed_path, run.clone()).unwrap();
    commit_processed(&mut resumed, &blocks[0]);
    drop(resumed);
    let mut resumed = TokenEventDatabase::open(&resumed_path, run).unwrap();
    commit_processed(&mut resumed, &blocks[1]);
    resumed.checkpoint_wal().unwrap();
    drop(resumed);

    assert_eq!(
        logical_database_bytes(&one_shot_path),
        logical_database_bytes(&resumed_path)
    );
}

#[test]
fn resume_rejects_a_wrong_source_or_opening_generation() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "binding.sqlite");
    let opening = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let run = spec(1, opening.snapshot());
    drop(TokenEventDatabase::create(&path, run.clone()).unwrap());

    let mut wrong_source = run.clone();
    wrong_source.source.binding = Some("sha256:wrong".into());
    assert!(matches!(
        TokenEventDatabase::open(&path, wrong_source),
        Err(TokenEventDatabaseError::SpecificationMismatch(_))
    ));

    let wrong_opening = TargetMintTrackerSnapshot::from_parts(
        TARGET,
        HistoryCoverage::Complete,
        1,
        [(
            SOURCE_ACCOUNT,
            blockzilla_model::token::TargetAccountSnapshot {
                lifecycle: TokenAccountLifecycle {
                    generation: 2,
                    state: TokenAccountState::ActiveTarget,
                },
                confirmed_revision: 1,
            },
        )],
    )
    .unwrap();
    let wrong_generation = spec(1, wrong_opening);
    assert!(matches!(
        TokenEventDatabase::open(&path, wrong_generation),
        Err(TokenEventDatabaseError::SpecificationMismatch(_))
    ));
}

#[test]
fn create_rejects_a_source_without_an_immutable_binding() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "missing-binding.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let mut run = spec(1, tracker.snapshot());
    run.source.verification = SourceVerification::OperatorTrusted;
    run.source.binding = None;

    assert!(matches!(
        TokenEventDatabase::create(&path, run),
        Err(TokenEventDatabaseError::InvalidSpecification(_))
    ));
    assert!(std::fs::symlink_metadata(path).is_err());
}

#[cfg(unix)]
#[test]
fn create_and_open_reject_final_and_ancestor_symlinks() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().unwrap();
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    let real_database = database_path(&directory, "real.sqlite");
    drop(TokenEventDatabase::create(&real_database, run.clone()).unwrap());
    let final_link = database_path(&directory, "final-link.sqlite");
    symlink(&real_database, &final_link).unwrap();
    assert!(matches!(
        TokenEventDatabase::open(&final_link, run.clone()),
        Err(TokenEventDatabaseError::UnsafePath { .. })
    ));

    let real_parent = directory.path().join("real-parent");
    let nested_parent = real_parent.join("nested");
    std::fs::create_dir(&real_parent).unwrap();
    std::fs::create_dir(&nested_parent).unwrap();
    std::fs::set_permissions(&real_parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&nested_parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    let parent_link = directory.path().join("parent-link");
    symlink(&real_parent, &parent_link).unwrap();
    let through_ancestor = parent_link.join("nested").join("database.sqlite");
    assert!(matches!(
        TokenEventDatabase::create(&through_ancestor, run),
        Err(TokenEventDatabaseError::UnsafePath { .. })
    ));
}

#[cfg(unix)]
#[test]
fn open_rejects_nonprivate_main_and_sidecar_files() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().unwrap();
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    let public_path = database_path(&directory, "public.sqlite");
    drop(TokenEventDatabase::create(&public_path, run.clone()).unwrap());
    std::fs::set_permissions(&public_path, std::fs::Permissions::from_mode(0o640)).unwrap();
    assert!(matches!(
        TokenEventDatabase::open(&public_path, run.clone()),
        Err(TokenEventDatabaseError::UnsafePath { .. })
    ));

    let sidecar_path = database_path(&directory, "sidecar.sqlite");
    let database = TokenEventDatabase::create(&sidecar_path, run.clone()).unwrap();
    database.checkpoint_wal().unwrap();
    drop(database);
    let wal = sqlite_sidecar(&sidecar_path, "-wal");
    let _ = std::fs::remove_file(&wal);
    symlink(&sidecar_path, &wal).unwrap();
    assert!(matches!(
        TokenEventDatabase::open(&sidecar_path, run),
        Err(TokenEventDatabaseError::UnsafePath { .. })
    ));
}

#[test]
fn create_or_open_resumes_while_valid_wal_sidecars_exist() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "live-wal.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    let database = TokenEventDatabase::create(&path, run.clone()).unwrap();
    assert!(std::fs::symlink_metadata(sqlite_sidecar(&path, "-wal")).is_ok());
    assert!(std::fs::symlink_metadata(sqlite_sidecar(&path, "-shm")).is_ok());

    let reopened = TokenEventDatabase::create_or_open(&path, run).unwrap();
    assert_eq!(reopened.next_block_ordinal(), 0);
    drop(reopened);
    drop(database);
}

#[test]
fn concurrent_create_has_one_no_clobber_winner() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "collision.sqlite");
    let tracker = TargetMintTracker::from_complete_start(TARGET);
    let run = spec(1, tracker.snapshot());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let handles = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let path = path.clone();
                let run = run.clone();
                scope.spawn(move || {
                    barrier.wait();
                    match TokenEventDatabase::create(path, run) {
                        Ok(database) => {
                            drop(database);
                            "created"
                        }
                        Err(TokenEventDatabaseError::AlreadyExists(_)) => "exists",
                        Err(error) => panic!("unexpected create result: {error}"),
                    }
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == "created")
            .count(),
        1
    );
    assert_eq!(
        results.iter().filter(|result| **result == "exists").count(),
        1
    );
}

#[test]
fn resume_rejects_a_corrupt_current_generation() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "corrupt-generation.sqlite");
    let opening = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let run = spec(1, opening.snapshot());
    drop(TokenEventDatabase::create(&path, run.clone()).unwrap());

    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    connection
        .execute(
            "UPDATE tracker_accounts SET generation_le = ?1",
            [2u64.to_le_bytes().as_slice()],
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        TokenEventDatabase::open(&path, run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(_))
    ));
}

#[test]
fn resume_rejects_a_mismatched_historical_u64_pair() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "corrupt-u64-pair.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run.clone()).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(42),
    );
    commit_processed(&mut database, &block);
    drop(database);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute("UPDATE events SET amount_text = '43'", [])
        .unwrap();
    drop(connection);
    assert!(matches!(
        TokenEventDatabase::open(&path, run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(_))
    ));
}

#[test]
fn resume_rejects_invalid_event_and_coverage_variants() {
    let directory = TempDir::new().unwrap();
    let event_path = database_path(&directory, "corrupt-event-variant.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let event_run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&event_path, event_run.clone()).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(7),
    );
    commit_processed(&mut database, &block);
    drop(database);
    let connection = Connection::open(&event_path).unwrap();
    connection
        .execute("UPDATE events SET ui_amount = 'invalid'", [])
        .unwrap();
    drop(connection);
    assert!(matches!(
        TokenEventDatabase::open(&event_path, event_run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(_))
    ));

    let coverage_path = database_path(&directory, "corrupt-coverage-variant.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let coverage_run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&coverage_path, coverage_run.clone()).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Unknown(CoverageReason::RawMetadata),
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(8),
    );
    commit_processed(&mut database, &block);
    drop(database);
    let connection = Connection::open(&coverage_path).unwrap();
    connection
        .execute(
            "UPDATE coverage_issues
                SET first_pubkey_id = (SELECT min(pubkey_id) FROM pubkeys)
              WHERE issue_kind = 'unknown-execution'",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        TokenEventDatabase::open(&coverage_path, coverage_run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(_))
    ));
}

#[test]
fn resume_rejects_corrupt_effects_and_historical_tracker_updates() {
    let directory = TempDir::new().unwrap();
    let effect_path = database_path(&directory, "corrupt-effect.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let effect_run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&effect_path, effect_run.clone()).unwrap();
    let transfer = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(7),
    );
    commit_processed(&mut database, &transfer);
    drop(database);
    let connection = Connection::open(&effect_path).unwrap();
    connection
        .execute(
            "UPDATE delta_legs
                SET amount_le = ?1, amount_text = '8'
              WHERE leg_index = 0",
            [8u64.to_le_bytes().as_slice()],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        TokenEventDatabase::open(&effect_path, effect_run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(_))
    ));

    let tracker_path = database_path(&directory, "corrupt-tracker-update.sqlite");
    let tracker = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let tracker_run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&tracker_path, tracker_run.clone()).unwrap();
    let close = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        vec![9],
    );
    commit_processed(&mut database, &close);
    drop(database);
    let connection = Connection::open(&tracker_path).unwrap();
    connection
        .execute(
            "UPDATE tracker_account_updates
                SET account_state = 'active-target', state_mint_pubkey_id = NULL",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        TokenEventDatabase::open(&tracker_path, tracker_run),
        Err(TokenEventDatabaseError::InvalidCheckpoint(_))
    ));
}

#[test]
fn lifecycle_before_and_after_generations_have_composite_foreign_keys() {
    let directory = TempDir::new().unwrap();
    for generation_side in ["before", "after"] {
        let path = database_path(
            &directory,
            &format!("lifecycle-{generation_side}-foreign-key.sqlite"),
        );
        let tracker = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
        let run = spec(1, tracker.snapshot());
        let mut database = TokenEventDatabase::create(&path, run.clone()).unwrap();
        let close = token_block(
            0,
            ExecutionStatus::Succeeded,
            vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
            vec![9],
        );
        commit_processed(&mut database, &close);
        drop(database);

        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        connection
            .execute(
                &format!(
                    "UPDATE lifecycle_effects
                        SET {generation_side}_generation_le = ?1,
                            {generation_side}_generation_text = '99'"
                ),
                [99u64.to_le_bytes().as_slice()],
            )
            .unwrap();
        drop(connection);
        assert!(matches!(
            TokenEventDatabase::open(&path, run),
            Err(TokenEventDatabaseError::InvalidCheckpoint(_))
        ));
    }
}

#[test]
fn maximum_u64_is_stored_as_exact_little_endian_bytes_and_text() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "max.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(u64::MAX),
    );
    commit_processed(&mut database, &block);
    drop(database);

    let connection = Connection::open(&path).unwrap();
    let event: (Vec<u8>, String, String, String) = connection
        .query_row(
            "SELECT amount_le, amount_text, typeof(amount_le), typeof(amount_text) FROM events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(event.0, u64::MAX.to_le_bytes());
    assert_eq!(event.1, u64::MAX.to_string());
    assert_eq!((event.2.as_str(), event.3.as_str()), ("blob", "text"));
    let legs = connection
        .prepare("SELECT amount_le, amount_text FROM delta_legs ORDER BY leg_index")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(legs.len(), 2);
    assert!(legs.iter().all(|(bytes, text)| {
        bytes.as_slice() == u64::MAX.to_le_bytes() && text == &u64::MAX.to_string()
    }));
}

#[test]
fn failed_and_unknown_transactions_never_store_delta_legs() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "uncommitted.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(2, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let failed = token_block(
        0,
        ExecutionStatus::Failed,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(10),
    );
    let unknown = token_block(
        1,
        ExecutionStatus::Unknown(CoverageReason::RawMetadata),
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(20),
    );
    commit_processed(&mut database, &failed);
    commit_processed(&mut database, &unknown);
    assert_eq!(
        database.resume_state().unwrap().tracker.history_coverage(),
        HistoryCoverage::Partial
    );
    drop(database);

    let connection = Connection::open(&path).unwrap();
    assert_eq!(count(&path, "events"), 2);
    assert_eq!(count(&path, "event_effects"), 0);
    assert_eq!(count(&path, "delta_legs"), 0);
    assert_eq!(count(&path, "coverage_issues"), 1);
    let states = connection
        .prepare("SELECT commit_state FROM events ORDER BY block_ordinal")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(states, ["rolled-back", "unknown"]);
}

#[test]
fn close_and_reuse_keep_distinct_lifetime_generations() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "generations.sqlite");
    let tracker = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let run = spec(2, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let close = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        vec![9],
    );
    let reuse = token_block(
        1,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, TARGET, AUTHORITY, RENT_SYSVAR],
        vec![1],
    );
    commit_processed(&mut database, &close);
    commit_processed(&mut database, &reuse);
    assert_eq!(
        database.tracker().lifecycle(&SOURCE_ACCOUNT),
        Some(TokenAccountLifecycle {
            generation: 2,
            state: TokenAccountState::ActiveTarget,
        })
    );
    drop(database);

    let connection = Connection::open(&path).unwrap();
    let rows = connection
        .prepare(
            "SELECT generation_text, account_state
               FROM account_lifetimes l JOIN pubkeys p USING (pubkey_id)
              WHERE p.address = ?1 ORDER BY generation_text",
        )
        .unwrap()
        .query_map([SOURCE_ACCOUNT.as_slice()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            ("1".into(), "closed".into()),
            ("2".into(), "active-target".into())
        ]
    );
    let current_generation: Vec<u8> = connection
        .query_row(
            "SELECT current.generation_le
               FROM tracker_accounts current JOIN pubkeys p USING (pubkey_id)
              WHERE p.address = ?1",
            [SOURCE_ACCOUNT.as_slice()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(current_generation, 2u64.to_le_bytes());
}

#[test]
fn self_transfer_keeps_two_legs_in_one_event() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "self-transfer.sqlite");
    let tracker = TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, SOURCE_ACCOUNT, AUTHORITY],
        transfer_data(42),
    );
    commit_processed(&mut database, &block);
    drop(database);

    let connection = Connection::open(&path).unwrap();
    assert_eq!(count(&path, "events"), 1);
    assert_eq!(count(&path, "event_effects"), 1);
    let rows = connection
        .prepare(
            "SELECT leg_index, direction, transfer_role, p.address
               FROM delta_legs d JOIN pubkeys p ON p.pubkey_id = d.account_pubkey_id
              ORDER BY leg_index",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].0, rows[0].1.as_str(), rows[0].2.as_str()),
        (0, "debit", "source")
    );
    assert_eq!(
        (rows[1].0, rows[1].1.as_str(), rows[1].2.as_str()),
        (1, "credit", "destination")
    );
    assert!(rows.iter().all(|row| row.3 == SOURCE_ACCOUNT));
}

#[test]
fn large_block_streams_many_transactions_into_one_atomic_commit() {
    const TRANSACTION_COUNT: u32 = 512;

    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "large-block.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let template = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(1),
    )
    .transactions
    .into_iter()
    .next()
    .unwrap();
    let transactions = (0..TRANSACTION_COUNT)
        .map(|tx_index| {
            let mut transaction = template.clone();
            transaction.header.tx_index = tx_index;
            transaction.primary_signature = Some([tx_index as u8; 64]);
            transaction
        })
        .collect();
    let block = CanonicalBlock {
        counts: None,
        header: BlockHeader {
            epoch: 7,
            block_ordinal: 0,
            slot: 100,
        },
        transactions,
    };
    block.validate().unwrap();

    assert_eq!(
        database.track_and_commit_block(block.as_view()).unwrap(),
        BlockCommitOutcome::Committed
    );
    assert_eq!(database.next_block_ordinal(), 1);
    assert_eq!(database.resume_state().unwrap().next_block_ordinal, 1);
    drop(database);
    assert_eq!(count(&path, "transactions"), i64::from(TRANSACTION_COUNT));
    assert_eq!(count(&path, "events"), i64::from(TRANSACTION_COUNT));
    assert_eq!(count(&path, "delta_legs"), i64::from(TRANSACTION_COUNT) * 2);
}

#[test]
fn batch_children_keep_tracker_order() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "batch-order.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let first = transfer_data(5);
    let second = transfer_data(6);
    let mut batch = vec![255, 3, u8::try_from(first.len()).unwrap()];
    batch.extend_from_slice(&first);
    batch.extend_from_slice(&[3, u8::try_from(second.len()).unwrap()]);
    batch.extend_from_slice(&second);
    let block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![
            SOURCE_ACCOUNT,
            DESTINATION_ACCOUNT,
            AUTHORITY,
            DESTINATION_ACCOUNT,
            SOURCE_ACCOUNT,
            AUTHORITY,
        ],
        batch,
    );
    commit_processed(&mut database, &block);
    drop(database);

    let connection = Connection::open(&path).unwrap();
    let rows = connection
        .prepare(
            "SELECT event_index, batch_index, raw_kind, amount_text
               FROM events ORDER BY event_index",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            (0, Some(0), "classic".into(), Some("5".into())),
            (1, Some(1), "classic".into(), Some("6".into())),
        ]
    );
    assert_eq!(count(&path, "delta_legs"), 4);
    assert_eq!(count(&path, "coverage_issues"), 0);
}

#[test]
fn outer_and_inner_instruction_coordinates_keep_runtime_order() {
    let directory = TempDir::new().unwrap();
    let path = database_path(&directory, "inner-order.sqlite");
    let tracker =
        TargetMintTracker::from_active_account_seed(TARGET, [SOURCE_ACCOUNT, DESTINATION_ACCOUNT]);
    let run = spec(1, tracker.snapshot());
    let mut database = TokenEventDatabase::create(&path, run).unwrap();
    let mut block = token_block(
        0,
        ExecutionStatus::Succeeded,
        vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
        transfer_data(3),
    );
    block.transactions[0].instructions = vec![
        ResolvedInstruction {
            coordinate: InstructionCoordinate {
                order: 0,
                outer_index: 0,
                inner_index: None,
                stack_height: None,
            },
            program_id: Some([99; 32]),
            accounts: Vec::new(),
            data_coverage: InstructionDataCoverage::Exact,
            data: Vec::new(),
        },
        ResolvedInstruction {
            coordinate: InstructionCoordinate {
                order: 1,
                outer_index: 0,
                inner_index: Some(0),
                stack_height: Some(2),
            },
            program_id: Some(CLASSIC_SPL_TOKEN_PROGRAM_ID),
            accounts: vec![SOURCE_ACCOUNT, DESTINATION_ACCOUNT, AUTHORITY],
            data_coverage: InstructionDataCoverage::Exact,
            data: transfer_data(3),
        },
        ResolvedInstruction {
            coordinate: InstructionCoordinate {
                order: 2,
                outer_index: 1,
                inner_index: None,
                stack_height: None,
            },
            program_id: Some(CLASSIC_SPL_TOKEN_PROGRAM_ID),
            accounts: vec![DESTINATION_ACCOUNT, SOURCE_ACCOUNT, AUTHORITY],
            data_coverage: InstructionDataCoverage::Exact,
            data: transfer_data(2),
        },
    ];
    block.validate().unwrap();
    commit_processed(&mut database, &block);
    drop(database);

    let connection = Connection::open(&path).unwrap();
    let rows = connection
        .prepare(
            "SELECT event_index, instruction_order, outer_index, inner_index, stack_height
               FROM events ORDER BY event_index",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, [(0, 1, 0, Some(0), Some(2)), (1, 2, 1, None, None),]);
}

fn logical_database_bytes(path: &Path) -> Vec<u8> {
    const TABLES: &[&str] = &[
        "pubkeys",
        "run_identity",
        "opening_tracker_state",
        "opening_tracker_accounts",
        "checkpoint",
        "blocks",
        "transactions",
        "account_lifetimes",
        "tracker_state",
        "tracker_accounts",
        "tracker_account_updates",
        "events",
        "event_accounts",
        "event_effects",
        "lifecycle_effects",
        "delta_legs",
        "coverage_issues",
    ];
    let connection = Connection::open(path).unwrap();
    let mut output = Vec::new();
    for table in TABLES {
        output.extend_from_slice(table.as_bytes());
        output.push(0);
        let mut statement = connection
            .prepare(&format!("SELECT * FROM {table}"))
            .unwrap();
        let column_count = statement.column_count();
        let mut rows = statement.query([]).unwrap();
        let mut encoded_rows = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let mut encoded = Vec::new();
            for index in 0..column_count {
                match row.get_ref(index).unwrap() {
                    ValueRef::Null => encoded.push(0),
                    ValueRef::Integer(value) => {
                        encoded.push(1);
                        encoded.extend_from_slice(&value.to_le_bytes());
                    }
                    ValueRef::Real(value) => {
                        encoded.push(2);
                        encoded.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                    ValueRef::Text(value) => {
                        encoded.push(3);
                        encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
                        encoded.extend_from_slice(value);
                    }
                    ValueRef::Blob(value) => {
                        encoded.push(4);
                        encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
                        encoded.extend_from_slice(value);
                    }
                }
            }
            encoded_rows.push(encoded);
        }
        encoded_rows.sort();
        for row in encoded_rows {
            output.extend_from_slice(&(row.len() as u64).to_le_bytes());
            output.extend_from_slice(&row);
        }
    }
    output
}
