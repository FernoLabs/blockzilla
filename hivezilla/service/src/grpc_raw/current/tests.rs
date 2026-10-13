use super::*;
use proto::prelude::{
    Message as TransactionMessage, Reward, Rewards, SubscribeUpdateBlock,
    SubscribeUpdateBlockFooter, SubscribeUpdateEntry, SubscribeUpdateEntryUpdateParent,
    SubscribeUpdateSlot, SubscribeUpdateTransactionInfo, Transaction, TransactionConfig,
};

fn config(root: &Path) -> CurrentGrpcConfig {
    CurrentGrpcConfig {
        output_dir: root.join("capture"),
        validator_rpc_endpoint: None,
        max_blocks: 1,
        min_free_bytes: 0,
        max_record_bytes: 1024 * 1024,
        ..Default::default()
    }
}
fn fixtures() -> Vec<SubscribeUpdate> {
    vec![
        UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42,
            parent: Some(40),
            bank_id: Some(91),
            status: 5,
            dead_error: Some("source status text".into()),
        }),
        UpdateOneof::Entry(SubscribeUpdateEntry {
            slot: 42,
            bank_id: 91,
            index: 0,
            num_hashes: 7,
            hash: vec![8; 32],
            executed_transaction_count: 1,
            starting_transaction_index: 0,
        }),
        UpdateOneof::EntryUpdateParent(SubscribeUpdateEntryUpdateParent {
            slot: 42,
            cleared_bank_id: 90,
            parent_slot: 40,
            parent_block_id: vec![4; 32],
        }),
        UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot: 42,
            bank_id: 91,
            bank_hash: vec![9; 32],
            block_producer_time_nanos: 12,
            block_user_agent: vec![0, 255, 1],
            block_final_cert: Some(vec![3, 0, 7]),
            skip_reward_cert: Some(vec![]),
            notar_reward_cert: Some(vec![4, 1]),
        }),
        UpdateOneof::Block(SubscribeUpdateBlock {
            slot: 42,
            parent_slot: 40,
            bank_id: 91,
            blockhash: "source-hash".into(),
            rewards: Some(Rewards {
                rewards: vec![Reward {
                    pubkey: "source-pubkey".into(),
                    lamports: -8,
                    post_balance: 9,
                    reward_type: 5,
                    commission: "2".into(),
                    commission_bps: "250".into(),
                }],
                ..Default::default()
            }),
            transactions: vec![SubscribeUpdateTransactionInfo {
                transaction: Some(Transaction {
                    message: Some(TransactionMessage {
                        versioned: true,
                        config: Some(TransactionConfig {
                            priority_fee: Some(123),
                            compute_unit_limit: Some(456),
                            loaded_accounts_data_size_limit: Some(789),
                            heap_size: Some(1024),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
    ]
    .into_iter()
    .map(|event| SubscribeUpdate {
        filters: vec!["retained-filter".into()],
        update_oneof: Some(event),
        created_at: Some(proto::prost_types::Timestamp {
            seconds: 99,
            nanos: 123,
        }),
    })
    .collect()
}

#[test]
fn current_schema_preserves_bank_parent_footer_v1_and_reward_fields() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let expected = fixtures();
    let mut writer = CaptureWriter::create(&config).unwrap();
    for update in &expected {
        writer.append(update).unwrap();
    }
    let seal = writer.seal(&config, 0.01).unwrap();
    assert_eq!(seal.counts.events, 5);
    assert_eq!(seal.counts.blocks, 1);
    assert_eq!(seal.counts.footers, 1);
    assert_eq!(seal.counts.parent_updates, 1);
    assert!(!seal.archive_publication_allowed && !seal.canonical_coverage_verified);
    let mut actual = Vec::new();
    let mut offsets = Vec::new();
    let read = read_sealed_current_events(
        &config.output_dir,
        config.max_record_bytes,
        |reference, update| {
            assert_eq!(reference.sequence as usize, actual.len());
            assert_eq!(reference.raw_sha256, digest(&update.encode_to_vec()));
            offsets.push(reference.offset);
            actual.push(update);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(read.counts, seal.counts);
    assert!(offsets.windows(2).all(|pair| pair[0] < pair[1]));
    let inventory: CurrentGrpcInventory =
        read_json(&config.output_dir.join(INVENTORY_FILE)).unwrap();
    assert_eq!(
        inventory.source_receipt,
        file_evidence(&config.output_dir, SEAL_FILE).unwrap()
    );
    assert_eq!(inventory.files, seal.files);
}

#[test]
fn current_subscription_requests_all_required_events_and_commitment() {
    for commitment in [CaptureCommitment::Confirmed, CaptureCommitment::Finalized] {
        let config = CurrentGrpcConfig {
            commitment,
            from_slot: Some(42),
            ..Default::default()
        };
        let request = current_subscription(&config);
        assert_eq!(request.commitment, Some(commitment.proto() as i32));
        assert_eq!(request.from_slot, Some(42));
        assert_eq!(
            request.blocks.values().next().unwrap().include_entries,
            Some(true)
        );
        assert_eq!(
            request.entry.values().next().unwrap().include_update_parent,
            Some(true)
        );
        assert_eq!(
            request
                .block_footer
                .values()
                .next()
                .unwrap()
                .include_certificates,
            Some(true)
        );
        assert_eq!(
            request.slots.values().next().unwrap().filter_by_commitment,
            Some(false)
        );
    }
}

#[test]
fn no_resume_and_no_read_of_unsealed_capture() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[0]).unwrap();
    assert!(CaptureWriter::create(&config).is_err());
    assert!(
        read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| Ok(()))
            .is_err()
    );
    drop(writer);
    assert!(
        read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| Ok(()))
            .is_err()
    );
}

#[test]
fn grouped_sync_preserves_frames_and_syncs_at_block_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    let events = fixtures();
    writer.append(&events[1]).unwrap();
    assert!(writer.pending_sync_bytes > 0);
    writer.last_sync = Instant::now() - WAL_SYNC_INTERVAL;
    writer.append(&events[1]).unwrap();
    assert_eq!(writer.pending_sync_bytes, 0);
    writer.append(&events[1]).unwrap();
    assert!(writer.pending_sync_bytes > 0);
    writer.append(&events[4]).unwrap();
    assert_eq!(writer.pending_sync_bytes, 0);
    let seal = writer.seal(&config, 0.01).unwrap();
    let mut decoded = Vec::new();
    read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, event| {
        decoded.push(event);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        decoded,
        vec![
            events[1].clone(),
            events[1].clone(),
            events[1].clone(),
            events[4].clone()
        ]
    );
    assert_eq!(seal.counts.events, 4);
}

#[test]
fn grouped_sync_flushes_on_byte_limit() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.pending_sync_bytes = WAL_SYNC_BYTES - 1;
    writer.append(&fixtures()[1]).unwrap();
    assert_eq!(writer.pending_sync_bytes, 0);
    assert!(!config.output_dir.join(SEAL_FILE).exists());
}

#[test]
fn writer_enforces_record_and_capture_bounds_before_append() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path());
    config.max_record_bytes = 1;
    let mut writer = CaptureWriter::create(&config).unwrap();
    assert!(writer.append(&fixtures()[0]).is_err());
    assert_eq!(writer.bytes, 8);
    drop(writer);
    config.output_dir = temp.path().join("capture2");
    config.max_record_bytes = 1024 * 1024;
    config.max_capture_bytes = 100;
    let mut writer = CaptureWriter::create(&config).unwrap();
    assert!(writer.append(&fixtures()[0]).is_err());
    assert_eq!(writer.file.metadata().unwrap().len(), 8);
}

#[test]
fn read_rejects_damage_truncation_wrong_sequence_and_limits() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[4]).unwrap();
    let original = fs::read(config.output_dir.join(EVENTS_FILE)).unwrap();
    drop(writer);
    let check = |bytes: &[u8], limit| {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        read_frames(&mut file, limit, |_, _| Ok(()))
    };
    assert!(check(&original, config.max_record_bytes).is_ok());
    assert!(check(&original, 1).is_err());
    for end in [0, 7, 9, 90, original.len() - 1] {
        assert!(check(&original[..end], config.max_record_bytes).is_err());
    }
    let mut wrong_sequence = original.clone();
    wrong_sequence[8] = 1;
    assert!(check(&wrong_sequence, config.max_record_bytes).is_err());
    let mut damage = original.clone();
    damage[96] ^= 1;
    assert!(check(&damage, config.max_record_bytes).is_err());
    let mut wrong_len = original.clone();
    wrong_len[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(check(&wrong_len, config.max_record_bytes).is_err());
}

#[test]
fn seal_digest_failure_prevents_callbacks_and_callback_errors_propagate() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[4]).unwrap();
    writer.seal(&config, 0.01).unwrap();
    let error = read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| {
        bail!("consumer stopped")
    })
    .unwrap_err();
    assert!(error.to_string().contains("consumer stopped"));
    #[cfg(unix)]
    fs::set_permissions(
        config.output_dir.join(EVENTS_FILE),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .open(config.output_dir.join(EVENTS_FILE))
        .unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&[77]).unwrap();
    file.sync_all().unwrap();
    let mut calls = 0;
    assert!(
        read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| {
            calls += 1;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(calls, 0);
}

#[cfg(unix)]
#[test]
fn sealed_reader_rejects_live_lock_and_symlink_objects() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[4]).unwrap();
    writer.seal(&config, 0.01).unwrap();
    let held = open_regular(&config.output_dir.join(EVENTS_FILE), false).unwrap();
    lock(&held, true).unwrap();
    assert!(
        read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| Ok(()))
            .is_err()
    );
    drop(held);
    let moved = config.output_dir.join("moved.wal");
    fs::rename(config.output_dir.join(EVENTS_FILE), &moved).unwrap();
    symlink(&moved, config.output_dir.join(EVENTS_FILE)).unwrap();
    assert!(
        read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| Ok(()))
            .is_err()
    );
}

#[test]
fn format_two_guard_stays_independent() {
    let bytes = fixtures()[4].encode_to_vec();
    let old = yellowstone_grpc_proto::prelude::SubscribeUpdate::decode(bytes.as_slice()).unwrap();
    assert!(crate::grpc_raw::validate_raw_grpc_update_v1(&old).is_err());
    assert_eq!(super::super::PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1, 2);
    assert_eq!(PAYLOAD_FORMAT, 3);
}

fn rpc_fixture() -> ValidatorRpcEvidence {
    let genesis_response =
        serde_json::json!({"jsonrpc":"2.0", "id":1, "result":"11111111111111111111111111111111"});
    let identity_response = serde_json::json!({"jsonrpc":"2.0", "id":1, "result":{"identity":"11111111111111111111111111111111"}});
    ValidatorRpcEvidence {
        schema: "hivezilla-validator-rpc-identity-v1".into(),
        rpc_endpoint: "http://127.0.0.1:8899".into(),
        genesis_response_body: genesis_response.to_string(),
        identity_response_body: identity_response.to_string(),
        genesis_response,
        identity_response,
    }
}

#[test]
fn trusted_profile_requires_matching_before_after_local_validator_identity() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path());
    config.validator_rpc_endpoint = Some("http://127.0.0.1:8899".into());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[4]).unwrap();
    let evidence = rpc_fixture();
    let identity = rpc_identity(&evidence).unwrap();
    writer.validator_identity = Some(identity.clone());
    for name in [RPC_BEFORE_FILE, RPC_AFTER_FILE] {
        write_json_new(&config.output_dir, name, &evidence).unwrap();
    }
    let seal = writer.seal(&config, 0.01).unwrap();
    assert_eq!(seal.source_profile, "validator-finalized-execution-v1");
    assert_eq!(seal.validator_identity, Some(identity));
    assert_eq!(seal.files.len(), 3);
    read_sealed_current_events(&config.output_dir, config.max_record_bytes, |_, _| Ok(())).unwrap();

    config.output_dir = temp.path().join("changed");
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[4]).unwrap();
    writer.validator_identity = Some(rpc_identity(&evidence).unwrap());
    write_json_new(&config.output_dir, RPC_BEFORE_FILE, &evidence).unwrap();
    let mut changed = rpc_fixture();
    changed.identity_response["result"]["identity"] =
        serde_json::json!("11111111111111111111111111111112");
    changed.identity_response_body = changed.identity_response.to_string();
    write_json_new(&config.output_dir, RPC_AFTER_FILE, &changed).unwrap();
    assert!(writer.seal(&config, 0.01).is_err());
    assert!(!config.output_dir.join(INVENTORY_FILE).exists());
}

#[test]
fn source_binding_and_rpc_response_guards_reject_ambiguous_or_error_inputs() {
    for endpoint in [
        "http://localhost:8899",
        "http://127.1:8899",
        "http://127.0.0.1.example:8899",
        "http://127.0.0.1:8899/?secret=x",
        "http://name@127.0.0.1:8899",
        "http://213.239.141.61:8899",
    ] {
        assert!(
            validate_loopback_endpoint(endpoint).is_err(),
            "accepted {endpoint}"
        );
    }
    for endpoint in ["http://127.0.0.1:8899", "http://[::1]:8899"] {
        validate_loopback_endpoint(endpoint).unwrap();
    }
    let mut evidence = rpc_fixture();
    evidence.identity_response["error"] = serde_json::json!({"code":-32000});
    evidence.identity_response_body = evidence.identity_response.to_string();
    assert!(rpc_identity(&evidence).is_err());
    let mut evidence = rpc_fixture();
    evidence.identity_response["id"] = serde_json::json!(2);
    evidence.identity_response_body = evidence.identity_response.to_string();
    assert!(rpc_identity(&evidence).is_err());
    let mut evidence = rpc_fixture();
    evidence.genesis_response["result"] = serde_json::json!("bad");
    evidence.genesis_response_body = evidence.genesis_response.to_string();
    assert!(rpc_identity(&evidence).is_err());
    let mut evidence = rpc_fixture();
    evidence.genesis_response_body = "{}".into();
    assert!(rpc_identity(&evidence).is_err());
}

#[test]
fn incomplete_capture_cannot_be_sealed_even_with_valid_frames() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let mut writer = CaptureWriter::create(&config).unwrap();
    writer.append(&fixtures()[0]).unwrap();
    assert!(writer.seal(&config, 0.01).is_err());
    assert!(!config.output_dir.join(SEAL_FILE).exists());
}

#[test]
fn sealed_current_capture_normalizes_with_retained_authoritative_evidence() {
    use blockzilla_live_format::execution_canary_v1::{
        FinalizedRpcCoverageV1, NORMALIZED_EXECUTION_FILE, NormalizedExecutionV1,
    };
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path());
    config.validator_rpc_endpoint = Some("http://127.0.0.1:8899".into());
    let mut writer = CaptureWriter::create(&config).unwrap();
    let evidence = rpc_fixture();
    let identity = rpc_identity(&evidence).unwrap();
    writer.validator_identity = Some(identity.clone());
    for name in [RPC_BEFORE_FILE, RPC_AFTER_FILE] {
        write_json_new(&config.output_dir, name, &evidence).unwrap();
    }
    // Include all standalone event families as well as a complete V1 execution block.
    for event in fixtures().into_iter().take(4) {
        writer.append(&event).unwrap();
    }
    let block = crate::execution_canary::current_test_block();
    writer
        .append(&SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Block(block)),
            ..Default::default()
        })
        .unwrap();
    let seal = writer.seal(&config, 0.01).unwrap();
    let reply = serde_json::json!({"jsonrpc":"2.0", "id":1, "result":[42]});
    write_json_new(temp.path(), "rpc-get-blocks.json", &reply).unwrap();
    let coverage = FinalizedRpcCoverageV1 {
        schema: "hivezilla-finalized-rpc-coverage-v1".into(),
        genesis_hash: identity.genesis_hash,
        node_identity: identity.node_identity,
        commitment: "finalized".into(),
        start_slot: 41,
        end_slot_exclusive: 43,
        finalized_slot: 42,
        produced_slots: vec![42],
        rpc_response_sha256: file_evidence(temp.path(), "rpc-get-blocks.json")
            .unwrap()
            .sha256,
        source_receipt_sha256: file_evidence(&config.output_dir, SEAL_FILE).unwrap().sha256,
    };
    write_json_new(temp.path(), "coverage.json", &coverage).unwrap();
    let output = temp.path().join("normalized");
    crate::execution_canary::normalize_current_execution(
        &config.output_dir,
        &temp.path().join("coverage.json"),
        &output,
        config.max_record_bytes,
        1,
    )
    .unwrap();
    let receipt: NormalizedExecutionV1 =
        read_json(&output.join(NORMALIZED_EXECUTION_FILE)).unwrap();
    assert_eq!(receipt.state, "COMPLETE");
    assert!(!receipt.canonical && receipt.finalized);
    assert_eq!(receipt.blocks.len(), 1);
    assert_eq!(
        (receipt.transactions, receipt.signatures, receipt.entries),
        (1, 1, 2)
    );
    assert_eq!(receipt.blocks[0].bank_id, 91);
    assert_eq!(receipt.blocks[0].source_sequence, 4);
    assert_eq!(
        file_evidence(&config.output_dir, EVENTS_FILE).unwrap(),
        file_evidence(&output.join("evidence"), EVENTS_FILE).unwrap()
    );
    let copied =
        read_sealed_current_events(&output.join("evidence"), config.max_record_bytes, |_, _| {
            Ok(())
        })
        .unwrap();
    assert_eq!(copied.counts, seal.counts);
    assert!(receipt.authoritative_execution.contains("events.wal"));
    assert!(!output.join("LIVE-EXECUTION-CANARY.json").exists());
}
