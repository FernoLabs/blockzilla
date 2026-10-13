use super::*;

fn block(slot: u64, parent: u64) -> Value {
    json!({"slot":slot,"parentSlot":parent,"epoch":0,"blockhash":format!("h{slot}"),"parentBlockhash":format!("h{parent}"),
        "transactionCount":1,"transactions":[{"index":0,"signature":format!("sig{slot}"),"success":false,
            "tokenBalances":[{"side":"post","balanceIndex":7,"accountIndex":2,"account":null,"mint":null,"owner":"owner",
                "tokenProgram":null,"amount":u64::MAX.to_string(),"decimals":9}]}]})
}
fn config() -> Value {
    json!({"test":true})
}
fn source() -> Value {
    json!({"kind":"test"})
}

#[test]
fn processed_head_is_rejected_before_history_or_checkpoint_changes() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = Db::open(&directory.path().join("db"), &config(), None).unwrap();
    db.apply(&[block(100, 99)], &source(), Some("session:100:-"))
        .unwrap();
    let args = IndexArgs {
        db: directory.path().join("db"),
        url: Some("http://127.0.0.1:1".into()),
        api_key_file: None,
        native_history_dir: None,
        epoch: None,
        from_slot: None,
        to_slot: None,
        mint: None,
        origin: "http://127.0.0.1:1".into(),
        cache_root: directory.path().join("cache"),
        threads: 1,
        max_blocks: None,
    };
    for head in [
        json!({"profile":"native-processed-compact-v1","latest_cursor":"session:p1000:-"}),
        json!({"latest_cursor":"session:p1000:-"}),
    ] {
        let error = recover(&mut db, &args, &head).unwrap_err().to_string();
        assert!(error.contains("processed fork undo"), "{error}");
        assert_eq!(db.count().unwrap(), 1);
        assert_eq!(db.position().unwrap().slot, Some(100));
        assert_eq!(
            db.position().unwrap().cursor.as_deref(),
            Some("session:100:-")
        );
        assert_eq!(
            db.connection
                .query_row("SELECT count(*) FROM recovery", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn duplicate_and_restart_keep_exact_amounts_and_one_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("tokens.sqlite");
    {
        let mut db = Db::open(&file, &config(), None).unwrap();
        assert_eq!(
            db.apply(&[block(100, 99)], &source(), Some("session:100:-"))
                .unwrap(),
            1
        );
        assert_eq!(
            db.apply(&[block(100, 99)], &source(), Some("session:100:-"))
                .unwrap(),
            0
        );
        let amount: String = db
            .connection
            .query_row("SELECT amount FROM token_balances", [], |r| r.get(0))
            .unwrap();
        assert_eq!(amount, u64::MAX.to_string());
        let success: bool = db
            .connection
            .query_row("SELECT success FROM transactions", [], |r| r.get(0))
            .unwrap();
        assert!(!success);
    }
    let db = Db::open(&file, &config(), None).unwrap();
    assert_eq!(db.count().unwrap(), 1);
    assert_eq!(
        db.position().unwrap().cursor.as_deref(),
        Some("session:100:-")
    );
}

#[test]
fn late_bad_row_rolls_back_every_block_and_checkpoint_in_the_chunk() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = Db::open(&directory.path().join("db"), &config(), None).unwrap();
    db.apply(&[block(100, 99)], &source(), Some("session:100:-"))
        .unwrap();
    let mut bad = block(102, 101);
    bad["transactions"][0]["tokenBalances"][0]["amount"] = json!("18446744073709551616");
    assert!(
        db.apply(&[block(101, 100), bad], &source(), Some("session:102:-"))
            .is_err()
    );
    assert_eq!(db.count().unwrap(), 1);
    assert_eq!(db.position().unwrap().slot, Some(100));
    assert_eq!(
        db.position().unwrap().cursor.as_deref(),
        Some("session:100:-")
    );
}

#[test]
fn wrong_parent_and_conflicting_duplicate_stop_before_commit() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = Db::open(&directory.path().join("db"), &config(), None).unwrap();
    db.apply(&[block(100, 99)], &source(), None).unwrap();
    assert!(db.apply(&[block(103, 102)], &source(), None).is_err());
    let mut changed = block(100, 99);
    changed["blockhash"] = json!("another-hash");
    assert!(db.apply(&[changed], &source(), None).is_err());
    assert_eq!(db.count().unwrap(), 1);
    // A skipped slot is valid when the produced parent link stays continuous.
    db.apply(&[block(103, 100)], &source(), None).unwrap();
}

#[test]
fn unknown_mint_stays_visible_and_filter_is_bound_to_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("db");
    {
        let mut db = Db::open(&file, &config(), Some("wanted".into())).unwrap();
        let mut b = block(100, 99);
        let mut other = b["transactions"][0]["tokenBalances"][0].clone();
        other["balanceIndex"] = json!(8);
        other["mint"] = json!("other");
        b["transactions"][0]["tokenBalances"]
            .as_array_mut()
            .unwrap()
            .push(other);
        db.apply(&[b], &source(), None).unwrap();
        let count: u64 = db
            .connection
            .query_row("SELECT count(*) FROM token_balances", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        assert!(Db::open(&file, &config(), None).is_err());
    }
    assert!(Db::open(&file, &json!({"test":false}), None).is_err());
}

#[test]
fn query_readers_see_only_complete_commits_and_empty_blocks_advance() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("db");
    let mut db = Db::open(&file, &config(), None).unwrap();
    let reader =
        Connection::open_with_flags(&file, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let b = json!({"slot":100,"parentSlot":99,"epoch":0,"blockhash":"h100","parentBlockhash":"h99","transactionCount":0,"transactions":[]});
    db.apply(&[b], &source(), Some("session:100:-")).unwrap();
    let count: u64 = reader
        .query_row("SELECT count(*) FROM blocks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(db.position().unwrap().slot, Some(100));
}

#[test]
fn unrelated_sqlite_files_are_not_modified() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("db");
    let other = Connection::open(&file).unwrap();
    other
        .execute_batch("CREATE TABLE unrelated(value TEXT)")
        .unwrap();
    assert!(Db::open(&file, &config(), None).is_err());
    let id: i64 = other
        .query_row("PRAGMA application_id", [], |r| r.get(0))
        .unwrap();
    assert_eq!(id, 0);
}

#[test]
fn archive_and_live_overlap_compare_shared_balance_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = Db::open(&directory.path().join("db"), &config(), None).unwrap();
    let archive = block(100, 99);
    db.apply(&[archive.clone()], &json!({"kind":"history"}), None)
        .unwrap();
    let mut live = archive.clone();
    live["finality"] = json!("finalized");
    live["transactions"][0]["tokenBalances"][0]["account"] = json!("resolved-live-account");
    assert_eq!(
        db.apply(
            &[live.clone()],
            &json!({"kind":"live"}),
            Some("session:100:-")
        )
        .unwrap(),
        0
    );
    live["transactions"][0]["tokenBalances"][0]["amount"] = json!("1");
    assert!(db.apply(&[live], &source(), None).is_err());
    assert_eq!(db.count().unwrap(), 1);
}

#[test]
fn replay_before_the_selected_start_does_not_backdate_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = Db::open(&directory.path().join("db"), &config(), None).unwrap();
    db.anchor("session:100:-", true).unwrap();
    assert_eq!(
        db.apply(
            &[block(99, 98), block(100, 99)],
            &source(),
            Some("session:100:-")
        )
        .unwrap(),
        0
    );
    db.apply(&[block(102, 100)], &source(), Some("session:102:-"))
        .unwrap();
    assert_eq!(db.count().unwrap(), 1);
    // Slot 101 was never indexed and was outside the selected produced chain.
    assert_eq!(db.apply(&[block(99, 98)], &source(), None).unwrap(), 0);
}

#[test]
fn an_explicit_start_rejects_a_missing_produced_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = Db::open(&directory.path().join("db"), &json!({"fromSlot":100}), None).unwrap();
    assert!(db.apply(&[block(102, 101)], &source(), None).is_err());
    assert_eq!(db.count().unwrap(), 0);
    db.apply(&[block(101, 99)], &source(), None).unwrap();
}
