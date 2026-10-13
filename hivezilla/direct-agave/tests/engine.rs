use blockzilla_archive_v2::WincodeArchiveV2NoRegistryRewards;
use hivezilla_direct_agave::{
    event::{Event, digest},
    journal::{Config, Journal, open_reader},
    worker::{Evidence, assemble},
};
use std::{fs, path::Path, time::Duration};
fn config(root: &Path) -> Config {
    Config {
        bank_v2: false,
        libpath: "fixture.so".into(),
        output_root: root.to_path_buf(),
        genesis_hash: "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),
        node_identity: "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ".into(),
        queue_events: 512,
        queue_bytes: 16 * 1024 * 1024,
        max_event_bytes: 4 * 1024 * 1024,
        max_journal_bytes: 128 * 1024 * 1024,
        reserve_disk_bytes: 0,
        commit_interval_ms: 10,
    }
}
fn status(slot: u64) -> Event {
    Event::Status {
        slot,
        bank_id: Some(7),
        parent: Some(slot - 1),
        status: "rooted".into(),
        dead_error: None,
    }
}
fn records() -> Vec<Evidence> {
    vec![
        status(10),
        Event::Block {
            slot: 10,
            bank_id: 7,
            parent_slot: 9,
            parent_blockhash: [9; 32],
            blockhash: [10; 32],
            transactions: 0,
            entries: 1,
            block_time: Some(123),
            block_height: Some(5),
            native_rewards: serde_json::json!({"rewards":[]}),
            rewards: WincodeArchiveV2NoRegistryRewards {
                source_len: 0,
                num_partitions: None,
                decoded: Some(vec![]),
                raw_fallback: None,
                decode_error: None,
            },
        },
        Event::Entry {
            slot: 10,
            bank_id: 7,
            index: 0,
            num_hashes: 123,
            hash: [10; 32],
            transactions: 0,
            starting_transaction_index: 0,
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(i, event)| Evidence {
        sequence: i as u64 + 1,
        sha256: digest(&serde_json::to_vec(&event).unwrap()),
        event,
    })
    .collect()
}
#[test]
fn bank_requires_root_metadata_and_dense_entries() {
    let full = records();
    assert!(assemble(10, 7, full).unwrap().is_some());
    let mut incomplete = records();
    incomplete.pop();
    assert!(assemble(10, 7, incomplete).unwrap().is_none());
    let mut no_root = records();
    no_root.remove(0);
    assert!(assemble(10, 7, no_root).unwrap().is_none());
    let mut bad = records();
    if let Event::Entry { index, .. } = &mut bad[2].event {
        *index = 1;
    }
    assert!(assemble(10, 7, bad).is_err());
}
#[test]
fn same_slot_forks_and_cleared_banks_cannot_mix() {
    assert!(assemble(10, 8, records()).is_err());
    let mut bad = records();
    bad.push(Evidence {
        sequence: 4,
        sha256: "clear".into(),
        event: Event::ClearBank {
            slot: 10,
            bank_id: 7,
            parent_slot: 8,
            parent_blockhash: [8; 32],
        },
    });
    assert!(assemble(10, 7, bad).is_err());
}
#[test]
fn duplicate_callbacks_must_be_identical() {
    let mut events = records();
    let duplicate = records().pop().unwrap();
    events.push(duplicate);
    assert!(assemble(10, 7, events).unwrap().is_some());
    let mut events = records();
    let mut duplicate = records().pop().unwrap();
    duplicate.sha256 = "different".into();
    events.push(duplicate);
    assert!(assemble(10, 7, events).is_err());
}
#[test]
fn journal_commits_and_reopens_without_losing_sequence() {
    let tmp = tempfile::tempdir().unwrap();
    let mut journal = Journal::start(config(tmp.path())).unwrap();
    for slot in 1..=100 {
        journal.append(&status(slot)).unwrap();
    }
    let path = journal.session_dir.clone();
    journal.close().unwrap();
    let db = open_reader(&path.join("events.sqlite")).unwrap();
    let (count, last): (u64, u64) = db
        .query_row("SELECT count(*),max(sequence) FROM events", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!((count, last), (100, 100));
    assert!(path.join("CLOSED.json").exists());
    assert_eq!(
        db.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}
#[test]
fn oversize_event_latches_failure_and_never_resumes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config(tmp.path());
    cfg.max_event_bytes = 4096;
    let mut journal = Journal::start(cfg).unwrap();
    assert!(
        journal
            .append(&Event::Footer {
                slot: 1,
                bank_id: 1,
                wire_footer: vec![8; 10000]
            })
            .is_err()
    );
    assert!(journal.append(&status(2)).is_err());
    assert!(journal.close().is_err());
    let failure: serde_json::Value =
        serde_json::from_slice(&fs::read(journal.session_dir.join("FAILURE.json")).unwrap())
            .unwrap();
    assert_eq!(failure["state"], "FAILED");
    assert!(!journal.session_dir.join("CLOSED.json").exists());
}
#[test]
fn disk_reserve_failure_is_visible_with_service_alive() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config(tmp.path());
    cfg.reserve_disk_bytes = u64::MAX - 8 * 1024 * 1024;
    let mut journal = Journal::start(cfg).unwrap();
    journal.append(&status(1)).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(journal.close().is_err());
    let db = open_reader(&journal.session_dir.join("events.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        0
    );
}
#[test]
fn concurrent_capture_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let first = Journal::start(config(tmp.path())).unwrap();
    assert!(Journal::start(config(tmp.path())).is_err());
    drop(first);
    assert!(Journal::start(config(tmp.path())).is_ok());
}
#[test]
fn new_session_records_previous_unclean_session() {
    let tmp = tempfile::tempdir().unwrap();
    let old = tmp.path().join("unclean");
    fs::create_dir(&old).unwrap();
    fs::write(old.join("SESSION.json"), b"{}").unwrap();
    let journal = Journal::start(config(tmp.path())).unwrap();
    let meta: serde_json::Value =
        serde_json::from_slice(&fs::read(journal.session_dir.join("SESSION.json")).unwrap())
            .unwrap();
    assert_eq!(
        meta["previous_unclean_sessions"],
        serde_json::json!(["unclean"])
    );
}
#[test]
fn queue_saturation_latches_failure_instead_of_silent_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config(tmp.path());
    cfg.max_event_bytes = 4096;
    cfg.queue_bytes = 4096;
    cfg.commit_interval_ms = 1000;
    let mut journal = Journal::start(cfg).unwrap();
    let mut rejected = false;
    for slot in 1..=1000 {
        if journal.append(&status(slot)).is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    assert!(journal.close().is_err());
    assert!(journal.append(&status(1001)).is_err());
}
#[test]
fn journal_quota_failure_does_not_publish_an_accepted_record() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config(tmp.path());
    cfg.max_journal_bytes = 1024 * 1024;
    let mut journal = Journal::start(cfg).unwrap();
    journal
        .append(&Event::Footer {
            slot: 1,
            bank_id: 1,
            wire_footer: vec![255; 500000],
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(journal.close().is_err());
    let db = open_reader(&journal.session_dir.join("events.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn compressed_payload_is_bounded_and_corruption_fails() {
    use hivezilla_direct_agave::journal::decode_payload;
    let bytes = vec![42u8; 10000];
    let encoded = zstd::bulk::compress(&bytes, 1).unwrap();
    assert_eq!(
        decode_payload(&encoded, "json-zstd-v1", 10000).unwrap(),
        bytes
    );
    assert!(decode_payload(&encoded, "json-zstd-v1", 9999).is_err());
    assert!(decode_payload(b"bad frame", "json-zstd-v1", 10000).is_err());
    assert!(decode_payload(&encoded, "unknown", 10000).is_err());
}
