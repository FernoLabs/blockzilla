use std::path::Path;

use blockzilla_dump::{
    Checkpoint, CheckpointBatch, CoverageIssue, DumpDatabase, DumpError, DumpKind, DumpSpec,
    DumpState, EpochBinding, EpochState, MatchRecord, MessageState, MetadataState, OnIndeterminate,
    ProgramMatch, TokenBalanceRecord, TokenBalanceSide, TokenMatch, TransactionAccountRecord,
    TransactionAccountSource,
};
use rusqlite::Connection;
use tempfile::TempDir;

fn program_spec(epochs: Vec<u64>) -> DumpSpec {
    DumpSpec {
        kind: DumpKind::Program,
        target_pubkey: [7; 32],
        source: "https://archive.example.test/root".into(),
        on_indeterminate: OnIndeterminate::Fail,
        epochs,
    }
}

fn token_spec(policy: OnIndeterminate) -> DumpSpec {
    DumpSpec {
        kind: DumpKind::Token,
        target_pubkey: [9; 32],
        source: "https://archive.example.test/root".into(),
        on_indeterminate: policy,
        epochs: vec![8],
    }
}

fn binding(epoch: u64, rows: u64) -> EpochBinding {
    EpochBinding {
        epoch,
        source_identity: format!("archive:/epochs/epoch-{epoch}"),
        cluster_id: "mainnet-beta".into(),
        generation_id: format!("epoch-{epoch}-generation"),
        generation_digest: [epoch as u8; 32],
        slots_per_epoch: 432_000,
        message_schema: "compact-v2-current".into(),
        metadata_schema: "compact-v2-current-typed-error".into(),
        manifest_json: format!(r#"{{"epoch":{epoch},"cluster_id":"mainnet-beta"}}"#),
        block_rows_total: rows,
    }
}

fn matched_transaction(epoch: u64, slot: u64, tx_index: u32) -> MatchRecord {
    MatchRecord {
        epoch,
        slot,
        block_id: 11,
        tx_index,
        source_flags: 3,
        first_signature_ordinal: 100,
        signatures: vec![[3; 64], [4; 64]],
        message_state: MessageState::Decoded,
        message_bytes: vec![1, 2, 3],
        metadata_state: MetadataState::Absent,
        metadata_wincode: None,
    }
}

fn account(epoch: u64, slot: u64, tx_index: u32) -> TransactionAccountRecord {
    TransactionAccountRecord {
        epoch,
        slot,
        tx_index,
        account_index: 0,
        pubkey: [5; 32],
        source: TransactionAccountSource::Static,
        is_signer: true,
        is_writable: true,
    }
}

fn sqlite(path: &Path) -> Connection {
    Connection::open(path).expect("open test database")
}

#[test]
fn create_open_and_status_bind_the_scope() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("pump.sqlite");
    let spec = program_spec(vec![300, 0, 100]);

    let database = DumpDatabase::create(&path, &spec).unwrap();
    let status = database.status().unwrap();
    assert_eq!(status.schema_version, 1);
    assert_eq!(status.kind, DumpKind::Program);
    assert_eq!(status.state, DumpState::Building);
    assert_eq!(
        status
            .epochs
            .iter()
            .map(|row| row.epoch)
            .collect::<Vec<_>>(),
        vec![0, 100, 300]
    );
    assert_eq!(status.transaction_rows, 0);
    drop(database);

    let reopened = DumpDatabase::open(&path).unwrap();
    assert_eq!(reopened.status().unwrap(), status);
    assert!(matches!(
        DumpDatabase::create(&path, &spec),
        Err(DumpError::AlreadyExists(_))
    ));
    assert!(matches!(
        DumpDatabase::open_or_create(&path, &program_spec(vec![0])),
        Err(DumpError::SpecificationMismatch(_))
    ));

    let connection = sqlite(&path);
    let stored_target: String = connection
        .query_row("SELECT target_pubkey_base58 FROM dump", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stored_target, bs58::encode([7; 32]).into_string());
}

#[test]
fn open_rejects_an_unrelated_sqlite_database() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("plain.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute("CREATE TABLE unrelated (value INTEGER)", [])
        .unwrap();

    assert!(matches!(
        DumpDatabase::open(&path),
        Err(DumpError::WrongApplication(0))
    ));
}

#[test]
fn checkpoint_rows_and_resume_point_commit_together() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("pump.sqlite");
    let spec = program_spec(vec![7]);
    let exact_binding = binding(7, 2);
    let mut database = DumpDatabase::create(&path, &spec).unwrap();
    assert_eq!(
        database.begin_epoch(&exact_binding).unwrap(),
        Checkpoint::default()
    );

    let mut batch = CheckpointBatch {
        checkpoint: Checkpoint {
            next_block_row: 1,
            scanned_blocks: 1,
            scanned_transactions: 4,
            matched_transactions: 1,
            indeterminate_transactions: 0,
        },
        ..CheckpointBatch::default()
    };
    batch.transactions.push(matched_transaction(7, 45, 2));
    batch.transaction_accounts.push(account(7, 45, 2));
    batch.program_matches.push(ProgramMatch {
        epoch: 7,
        slot: 45,
        tx_index: 2,
        direct_count: 1,
        cpi_count: 2,
    });
    database.commit_checkpoint(7, &batch).unwrap();
    drop(database);

    let mut resumed = DumpDatabase::open_or_create(&path, &spec).unwrap();
    assert_eq!(
        resumed.begin_epoch(&exact_binding).unwrap(),
        batch.checkpoint
    );
    let status = resumed.status().unwrap();
    assert_eq!(status.transaction_rows, 1);
    assert_eq!(
        status.epochs[0].message_schema.as_deref(),
        Some("compact-v2-current")
    );
    assert_eq!(
        status.epochs[0].metadata_schema.as_deref(),
        Some("compact-v2-current-typed-error")
    );

    let connection = sqlite(&path);
    let (primary, metadata_state, signature_rows, account_key): (String, String, i64, String) =
        connection
            .query_row(
                "SELECT t.primary_signature_base58, t.metadata_state,
                    (SELECT count(*) FROM transaction_signatures s
                     WHERE s.epoch=t.epoch AND s.slot=t.slot AND s.tx_index=t.tx_index),
                    (SELECT pubkey_base58 FROM transaction_accounts a
                     WHERE a.epoch=t.epoch AND a.slot=t.slot AND a.tx_index=t.tx_index)
                 FROM transactions t",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    assert_eq!(primary, bs58::encode([3; 64]).into_string());
    assert_eq!(metadata_state, "absent");
    assert_eq!(signature_rows, 2);
    assert_eq!(account_key, bs58::encode([5; 32]).into_string());

    let mut changed_binding = exact_binding.clone();
    changed_binding.metadata_schema = "different".into();
    assert!(matches!(
        resumed.begin_epoch(&changed_binding),
        Err(DumpError::GenerationMismatch { epoch: 7 })
    ));

    let no_progress = CheckpointBatch {
        checkpoint: batch.checkpoint,
        ..CheckpointBatch::default()
    };
    assert!(matches!(
        resumed.commit_checkpoint(7, &no_progress),
        Err(DumpError::InvalidCheckpoint { epoch: 7, .. })
    ));
    assert!(matches!(
        resumed.complete_epoch(7),
        Err(DumpError::InvalidCheckpoint { epoch: 7, .. })
    ));

    resumed
        .commit_checkpoint(
            7,
            &CheckpointBatch {
                checkpoint: Checkpoint {
                    next_block_row: 2,
                    scanned_blocks: 2,
                    ..batch.checkpoint
                },
                ..CheckpointBatch::default()
            },
        )
        .unwrap();
    assert_eq!(resumed.complete_epoch(7).unwrap(), EpochState::Complete);
    assert_eq!(resumed.complete_dump().unwrap(), DumpState::Complete);
    resumed.integrity_check().unwrap();
}

#[test]
fn failed_insert_rolls_back_rows_and_checkpoint() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("atomic.sqlite");
    let spec = program_spec(vec![7]);
    let mut database = DumpDatabase::create(&path, &spec).unwrap();
    database.begin_epoch(&binding(7, 1)).unwrap();

    let transaction = matched_transaction(7, 45, 2);
    let summary = ProgramMatch {
        epoch: 7,
        slot: 45,
        tx_index: 2,
        direct_count: 1,
        cpi_count: 0,
    };
    let batch = CheckpointBatch {
        checkpoint: Checkpoint {
            next_block_row: 1,
            scanned_blocks: 1,
            scanned_transactions: 2,
            matched_transactions: 2,
            indeterminate_transactions: 0,
        },
        transactions: vec![transaction.clone(), transaction],
        program_matches: vec![summary, summary],
        ..CheckpointBatch::default()
    };
    assert!(matches!(
        database.commit_checkpoint(7, &batch),
        Err(DumpError::Sqlite(_))
    ));
    let status = database.status().unwrap();
    assert_eq!(status.transaction_rows, 0);
    assert_eq!(status.epochs[0].checkpoint.next_block_row, 0);
}

#[test]
fn token_amount_is_queryable_decimal_and_recorded_gaps_are_explicit() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("usdc.sqlite");
    let spec = token_spec(OnIndeterminate::Record);
    let mut database = DumpDatabase::create(&path, &spec).unwrap();
    database.begin_epoch(&binding(8, 1)).unwrap();

    let mut transaction = matched_transaction(8, 88, 0);
    transaction.metadata_state = MetadataState::Decoded;
    transaction.metadata_wincode = Some(vec![8, 9]);
    let batch = CheckpointBatch {
        checkpoint: Checkpoint {
            next_block_row: 1,
            scanned_blocks: 1,
            scanned_transactions: 3,
            matched_transactions: 1,
            indeterminate_transactions: 1,
        },
        transactions: vec![transaction],
        token_matches: vec![TokenMatch {
            epoch: 8,
            slot: 88,
            tx_index: 0,
            pre_count: 1,
            post_count: 0,
        }],
        token_balances: vec![TokenBalanceRecord {
            epoch: 8,
            slot: 88,
            tx_index: 0,
            side: TokenBalanceSide::Pre,
            balance_index: 0,
            account_index: 4,
            mint: [9; 32],
            owner: Some([6; 32]),
            token_program: Some([1; 32]),
            amount: u64::MAX,
            decimals: 6,
        }],
        coverage_issues: vec![CoverageIssue {
            epoch: 8,
            slot: 89,
            tx_index: 1,
            reason: "raw-transaction-fallback".into(),
            detail: Some("program accounts cannot be decoded".into()),
        }],
        ..CheckpointBatch::default()
    };
    database.commit_checkpoint(8, &batch).unwrap();
    assert_eq!(
        database.complete_epoch(8).unwrap(),
        EpochState::CompleteWithGaps
    );
    assert_eq!(
        database.complete_dump().unwrap(),
        DumpState::CompleteWithGaps
    );

    let connection = sqlite(&path);
    let (amount, side, mint): (String, String, String) = connection
        .query_row(
            "SELECT amount_u64, side, mint_base58 FROM token_balances",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(amount, u64::MAX.to_string());
    assert_eq!(side, "pre");
    assert_eq!(mint, bs58::encode([9; 32]).into_string());
    database.integrity_check().unwrap();
}
