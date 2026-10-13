#![cfg(feature = "plugin")]
use blockzilla_archive_v2::WincodeArchiveV2NoRegistryRewards;
use hivezilla_direct_agave::{
    event::{Event, hash},
    journal::{Config, Journal},
    plugin::transaction_event,
    worker,
};
use solana_message::{Message, MessageHeader, VersionedMessage};
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::TransactionStatusMeta;
use std::{fs, path::PathBuf};

#[test]
fn real_compact_writer_crosses_epoch_and_restart_does_not_duplicate() {
    let builder = std::env::var("BLOCKZILLA_TEST_BINARY")
        .expect("set BLOCKZILLA_TEST_BINARY to the tested direct compact CLI");
    let temp = tempfile::tempdir().unwrap();
    let mut journal = Journal::start(Config {
        bank_v2: false,
        libpath: "fixture".into(),
        output_root: temp.path().join("capture"),
        genesis_hash: "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),
        node_identity: "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ".into(),
        queue_events: 512,
        queue_bytes: 16 * 1024 * 1024,
        max_event_bytes: 1024 * 1024,
        max_journal_bytes: 128 * 1024 * 1024,
        reserve_disk_bytes: 0,
        commit_interval_ms: 1,
    })
    .unwrap();
    let tx = VersionedTransaction {
        signatures: vec![Default::default()],
        message: VersionedMessage::Legacy(Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..Default::default()
            },
            account_keys: vec![Default::default()],
            ..Default::default()
        }),
    };
    let meta = TransactionStatusMeta {
        pre_balances: vec![1],
        post_balances: vec![1],
        ..Default::default()
    };
    for (slot, bank, parent_hash, blockhash) in [
        (431998, 6, [7; 32], [8; 32]),
        (431999, 7, [8; 32], [9; 32]),
        (432000, 8, [9; 32], [10; 32]),
    ] {
        journal
            .append(&transaction_event(&tx, &meta, slot, bank, 0, false, vec![3; 32]).unwrap())
            .unwrap();
        journal
            .append(&Event::Entry {
                slot,
                bank_id: bank,
                index: 0,
                num_hashes: 1,
                hash: blockhash,
                transactions: 1,
                starting_transaction_index: 0,
            })
            .unwrap();
        journal
            .append(&Event::Block {
                slot,
                bank_id: bank,
                parent_slot: slot - 1,
                parent_blockhash: parent_hash,
                blockhash,
                transactions: 1,
                entries: 1,
                block_time: None,
                block_height: None,
                native_rewards: serde_json::json!({"rewards":[]}),
                rewards: WincodeArchiveV2NoRegistryRewards {
                    source_len: 0,
                    num_partitions: None,
                    decoded: Some(vec![]),
                    raw_fallback: None,
                    decode_error: None,
                },
            })
            .unwrap();
        journal
            .append(&Event::Status {
                slot,
                bank_id: Some(bank),
                parent: Some(slot - 1),
                status: "rooted".into(),
                dead_error: None,
            })
            .unwrap();
    }
    journal.close().unwrap();
    let output = temp.path().join("output");
    fs::create_dir(&output).unwrap();
    let session: serde_json::Value =
        serde_json::from_slice(&fs::read(journal.session_dir.join("SESSION.json")).unwrap())
            .unwrap();
    fs::write(
        output.join("PENDING.json"),
        serde_json::to_vec(&serde_json::json!({
            "session_id":session["session_id"], "first_slot":431998, "last_slot":431998
        }))
        .unwrap(),
    )
    .unwrap();
    worker::run(
        &journal.session_dir,
        &output,
        &PathBuf::from(&builder),
        32,
        false,
    )
    .unwrap();
    assert!(
        output
            .join("epoch-0/431998-431998.compact/DIRECT-AGAVE-COMPACT.v1.json")
            .is_file()
    );
    assert!(
        output
            .join("epoch-0/431999-431999.compact/DIRECT-AGAVE-COMPACT.v1.json")
            .is_file()
    );
    assert!(
        output
            .join("epoch-1/432000-432000.compact/DIRECT-AGAVE-COMPACT.v1.json")
            .is_file()
    );
    let before = fs::read(output.join("CURSOR.json")).unwrap();
    let cursor: serde_json::Value = serde_json::from_slice(&before).unwrap();
    assert!(!output.join("PENDING.json").exists());
    assert_eq!(cursor["compacted_blocks"], 3);
    assert_eq!(cursor["compacted_transactions"], 3);
    assert_eq!(cursor["last_slot"], 432000);
    worker::run(
        &journal.session_dir,
        &output,
        &PathBuf::from(builder),
        32,
        false,
    )
    .unwrap();
    assert_eq!(before, fs::read(output.join("CURSOR.json")).unwrap());
    assert_eq!(hash("11111111111111111111111111111111").unwrap(), [0; 32]);
}
