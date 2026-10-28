use super::*;
use blockzilla_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader,
    OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
};
use blockzilla_primitives::wincode_leb128_config;

fn meta() -> CompactMetaV1 {
    CompactMetaV1 {
        err: None,
        fee: 5000,
        pre_balances: vec![9_000_000_000_000_000_001, 0, 0],
        post_balances: vec![9_000_000_000_000_000_000, 0, 0],
        inner_instructions: None,
        logs: None,
        pre_token_balances: vec![],
        post_token_balances: vec![],
        rewards: vec![],
        loaded_writable_addresses: vec![],
        loaded_readonly_addresses: vec![],
        return_data: None,
        compute_units_consumed: Some(12),
        cost_units: None,
    }
}
fn fixture(loaded: bool, failed: bool, id: u32) -> Vec<u8> {
    fixture_with_tokens(loaded, failed, id, false)
}
fn fixture_with_tokens(loaded: bool, failed: bool, id: u32, tokens: bool) -> Vec<u8> {
    let header = CompactMessageHeader {
        num_required_signatures: 1,
        num_readonly_signed_accounts: 0,
        num_readonly_unsigned_accounts: 1,
    };
    let ix = ArchiveV2HotInstruction {
        program_id_index: 1,
        accounts: vec![0],
        data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3]),
    };
    let keys = vec![CompactPubkey::Raw([1; 32]), CompactPubkey::Id(id)];
    let message = if loaded {
        ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header,
            account_keys: keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![ix],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Raw([4; 32]),
                writable_indexes: vec![],
                readonly_indexes: vec![0],
            }],
        })
    } else {
        ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header,
            account_keys: keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![ix],
        })
    };
    let mut m = meta();
    if tokens {
        m.post_token_balances
            .push(blockzilla_compact::CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(id)),
                owner: None,
                program_id: Some(CompactPubkey::Raw([7; 32])),
                amount: u64::MAX,
                decimals: 9,
            });
    }
    if !loaded {
        m.pre_balances.pop();
        m.post_balances.pop();
    }
    if failed {
        m.err = Some(blockzilla_compact::CompactTransactionError::AccountNotFound);
    }
    if loaded {
        m.loaded_readonly_addresses = vec![CompactPubkey::Id(2)];
        m.inner_instructions = Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 2,
                accounts: vec![0],
                data: vec![9],
                stack_height: Some(2),
            }],
        }]);
    }
    let message_bytes = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
    let metadata_bytes = wincode::config::serialize(&m, wincode_leb128_config()).unwrap();
    let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
        | if loaded {
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
        } else {
            0
        }
        | if failed {
            ARCHIVE_V2_TX_FLAG_HAS_ERROR
        } else {
            0
        };
    let block = ArchiveV2HotBlockBlob {
        header: ArchiveV2HotBlockHeader {
            slot: 453200001,
            parent_slot: 453200000,
            blockhash_id: 0,
            previous_blockhash_id: 0,
            block_time: None,
            block_height: None,
            rewards: None,
        },
        tx_count: 1,
        tx_rows: vec![ArchiveV2HotTxRow {
            tx_index: 0,
            flags,
            message_offset: 0,
            message_len: message_bytes.len() as u32,
            metadata_offset: 0,
            metadata_len: metadata_bytes.len() as u32,
            signature_count: 1,
            reserved: [0; 3],
        }],
        message_bytes,
        metadata_bytes,

        consensus_block_id: None,
        block_markers: Vec::new(),
    };
    let raw = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
    let mut index = b"BZV2HIX1".to_vec();
    index.extend_from_slice(&[1, 0, 0, 0]);
    index.extend_from_slice(&1u64.to_le_bytes());
    index.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    index.extend_from_slice(&0i32.to_le_bytes());
    index.extend_from_slice(&2u32.to_le_bytes());
    index.extend_from_slice(&0u32.to_le_bytes());
    index.extend_from_slice(&453200001u64.to_le_bytes());
    index.extend_from_slice(&0u64.to_le_bytes());
    index.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    index.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    index.extend_from_slice(&1u32.to_le_bytes());
    index.extend_from_slice(&0u64.to_le_bytes());
    index.extend_from_slice(&0u64.to_le_bytes());
    index.extend_from_slice(&1u32.to_le_bytes());
    let files = [
        ("archive-v2-blocks.index", index),
        ("archive-v2-blocks.zstd", raw),
        ("registry.bin", [[7; 32], [8; 32]].concat()),
        ("blockhash_registry.bin", vec![2; 32]),
        (
            "prev_blockhash_tail.bin",
            [vec![3; 32], 453200000u64.to_le_bytes().to_vec()].concat(),
        ),
        ("signatures.bin", vec![9; 64]),
    ];
    let mut payload = vec![];
    let mut objects = vec![];
    for (path, bytes) in files {
        objects.push(json!({"path":path,"offset":payload.len(),"bytes":bytes.len()}));
        payload.extend(bytes);
    }
    let e = json!({"profile":"native-compact-projection-v1","marker":{"epoch":1049,"session_id":"test","finalized":true,"canonical":false,"publication_ready":false,
        "produced_blocks":[{"block_id":0,"slot":453200001,"parent_slot":453200000,"blockhash":vec![2;32],"parent_blockhash":vec![3;32]}]},"objects":objects});
    let encoded = serde_json::to_vec(&e).unwrap();
    let mut wire = b"BZSTRM01".to_vec();
    wire.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    wire.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    wire.extend(encoded);
    wire.extend(payload);
    wire
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn native_token_projection_preserves_exact_balances_keys_and_failure() {
    let bytes = decode_token_balances(&fixture_with_tokens(false, true, 1, true)).unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    let tx = &value["blocks"][0]["transactions"][0];
    assert_eq!(tx["success"], false);
    assert!(tx.get("metadata").is_none());
    assert!(tx.get("message").is_none());
    let b = &tx["tokenBalances"][0];
    assert_eq!(b["amount"], u64::MAX.to_string());
    assert_eq!(b["account"], base58(&[1; 32]));
    assert_eq!(b["side"], "post");
    assert_eq!(b["balanceIndex"], 0);
    assert_eq!(b["owner"], Value::Null);
    assert_eq!(b["tokenProgram"], base58(&[7; 32]));
}
fn read(wire: &[u8], o: Options) -> Value {
    serde_json::from_slice(&decode(wire, &o).unwrap()).unwrap()
}
#[test]
fn resolves_program_and_preserves_large_balances() {
    let v = read(
        &fixture(false, false, 1),
        Options {
            programs: vec![base58(&[7; 32])],
            ..Options::default()
        },
    );
    let t = &v["blocks"][0]["transactions"][0];
    assert_eq!(t["topLevelPrograms"][0], base58(&[7; 32]));
    assert_eq!(t["metadata"]["pre_balances"][0], "9000000000000000001");
    assert_eq!(t["success"], true);
}
#[test]
fn filters_non_matching_transactions() {
    let v = read(
        &fixture(false, false, 1),
        Options {
            programs: vec![base58(&[6; 32])],
            ..Options::default()
        },
    );
    assert_eq!(v["transactionCount"], 1);
    assert_eq!(v["blocks"][0]["transactions"], json!([]));
}
#[test]
fn includes_loaded_cpi_program_and_accounts() {
    let v = read(
        &fixture(true, false, 1),
        Options {
            programs: vec![base58(&[8; 32])],
            ..Options::default()
        },
    );
    let t = &v["blocks"][0]["transactions"][0];
    assert_eq!(t["innerPrograms"][0], base58(&[8; 32]));
    assert_eq!(t["accountKeys"][2], base58(&[8; 32]));
}
#[test]
fn failed_transactions_are_visible_unless_excluded() {
    let wire = fixture(false, true, 1);
    let v = read(&wire, Options::default());
    assert_eq!(v["blocks"][0]["transactions"][0]["success"], false);
    assert_eq!(
        read(
            &wire,
            Options {
                successful_only: true,
                ..Options::default()
            }
        )["blocks"][0]["transactions"],
        json!([])
    );
}
#[test]
fn receipt_mode_omits_heavy_details() {
    let v = read(
        &fixture(false, false, 1),
        Options {
            include_details: false,
            ..Options::default()
        },
    );
    let t = &v["blocks"][0]["transactions"][0];
    assert!(t.get("message").is_none());
    assert_eq!(t["signature"], base58(&[9; 64]));
}
#[test]
fn account_filter_matches_raw_address() {
    let v = read(
        &fixture(false, false, 1),
        Options {
            accounts: vec![base58(&[1; 32])],
            ..Options::default()
        },
    );
    assert_eq!(v["blocks"][0]["transactions"].as_array().unwrap().len(), 1);
}
#[test]
fn program_filters_bind_to_addresses_after_dictionary_id_changes() {
    let program = base58(&[8; 32]);
    assert_eq!(
        read(
            &fixture(false, false, 1),
            Options {
                programs: vec![program.clone()],
                ..Options::default()
            }
        )["blocks"][0]["transactions"],
        json!([])
    );
    assert_eq!(
        read(
            &fixture(false, false, 2),
            Options {
                programs: vec![program],
                ..Options::default()
            }
        )["blocks"][0]["transactions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn unknown_dictionary_id_and_truncation_fail() {
    assert!(
        decode(&fixture(false, false, 3), &Options::default())
            .unwrap_err()
            .to_string()
            .contains("registry_id_outside")
    );
    let mut wire = fixture(false, false, 1);
    wire.pop();
    assert!(decode(&wire, &Options::default()).is_err());
}
#[test]
fn rejects_invalid_filter_and_preserves_integer_extremes() {
    assert!(
        decode(
            &fixture(false, false, 1),
            &Options {
                programs: vec!["bad".into()],
                ..Options::default()
            }
        )
        .is_err()
    );
    let mut v = json!({"a":u64::MAX,"b":i64::MIN});
    preserve_large_integers(&mut v);
    assert_eq!(v["a"], u64::MAX.to_string());
    assert_eq!(v["b"], i64::MIN.to_string());
}
#[test]
fn first_parent_tail_is_checked_independently_of_header_placeholder() {
    let mut wire = fixture(false, false, 1);
    let n = u32_at(&wire, 8).unwrap() as usize;
    let e: Envelope = serde_json::from_slice(&wire[16..16 + n]).unwrap();
    let offset = e
        .objects
        .iter()
        .find(|o| o.path == "prev_blockhash_tail.bin")
        .unwrap()
        .offset;
    wire[16 + n + offset] ^= 1;
    assert!(
        decode(&wire, &Options::default())
            .unwrap_err()
            .to_string()
            .contains("native_parent_tail_mismatch")
    );
}

#[test]
fn processed_decode_preserves_bank_identity_and_token_index_rejects_it() {
    let wire = fixture(false, false, 1);
    let n = u32::from_le_bytes(wire[8..12].try_into().unwrap()) as usize;
    let mut e: Value = serde_json::from_slice(&wire[16..16 + n]).unwrap();
    e["profile"] = "native-processed-compact-v1".into();
    e["marker"]["finalized"] = false.into();
    e["marker"]["produced_blocks"][0]["bank_id"] = 7.into();
    e["event"] = json!({"kind":"block"});
    let meta = serde_json::to_vec(&e).unwrap();
    let mut processed = wire[..16].to_vec();
    processed[8..12].copy_from_slice(&(meta.len() as u32).to_le_bytes());
    processed.extend(meta);
    processed.extend(&wire[16 + n..]);
    let value: Value =
        serde_json::from_slice(&decode(&processed, &Options::default()).unwrap()).unwrap();
    assert_eq!(value["blocks"][0]["bankId"], 7);
    assert_eq!(value["blocks"][0]["finality"], "processed");
    #[cfg(not(target_arch = "wasm32"))]
    assert!(decode_token_balances(&processed).is_err());
}

fn bank_fixture(failed: bool) -> blockzilla_bank_stream_v2::Frame {
    use blockzilla_bank_stream_v2 as v2;
    let wire = fixture(true, failed, 1);
    let meta = u32_at(&wire, 8).unwrap() as usize;
    let mut e: Value = serde_json::from_slice(&wire[16..16 + meta]).unwrap();
    let old = &wire[16 + meta..];
    let mut body = old.to_vec();
    let source = v2::SourceIdentity {
        genesis_hash: base58(&[5; 32]),
        node_identity: base58(&[6; 32]),
        agave_revision: "native-test-host".into(),
        session_id: "native-test-session".into(),
    };
    let bank = v2::BankIdentity {
        slot: 453200001,
        bank_id: 7,
        blockhash: [2; 32],
        parent_slot: 453200000,
        parent_blockhash: [3; 32],
    };
    // Re-encode the same current Rust compact schema with its actual height.
    let objects = e["objects"].as_array_mut().unwrap();
    let mut objects_v2 = Vec::new();
    let mut files = Vec::new();
    let mut rebuilt = Vec::new();
    for o in objects.iter() {
        let start = o["offset"].as_u64().unwrap() as usize;
        let end = start + o["bytes"].as_u64().unwrap() as usize;
        let mut bytes = old[start..end].to_vec();
        if o["path"] == "archive-v2-blocks.zstd" {
            let mut block: ArchiveV2HotBlockBlob =
                wincode::config::deserialize(&bytes, wincode_leb128_config()).unwrap();
            block.header.block_height = Some(400000000);
            bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        }
        // The height adds encoded bytes; update the index's bounded row lengths.
        if o["path"] == "archive-v2-blocks.index" {
            let blob_o = objects
                .iter()
                .find(|o| o["path"] == "archive-v2-blocks.zstd")
                .unwrap();
            let at = blob_o["offset"].as_u64().unwrap() as usize;
            let len = blob_o["bytes"].as_u64().unwrap() as usize;
            let mut block: ArchiveV2HotBlockBlob =
                wincode::config::deserialize(&old[at..at + len], wincode_leb128_config()).unwrap();
            block.header.block_height = Some(400000000);
            let n = wincode::config::serialize(&block, wincode_leb128_config())
                .unwrap()
                .len();
            bytes[20..28].copy_from_slice(&(n as u64).to_le_bytes());
            bytes[56..60].copy_from_slice(&(n as u32).to_le_bytes());
            bytes[60..64].copy_from_slice(&(n as u32).to_le_bytes());
        }
        let path = o["path"].as_str().unwrap().to_owned();
        files.push(json!({"path":path,"bytes":bytes.len(),"sha256":v2::digest(&bytes)}));
        objects_v2.push(v2::Object {
            path,
            offset: rebuilt.len(),
            bytes: bytes.len(),
            sha256: v2::digest(&bytes),
        });
        rebuilt.extend(bytes);
    }
    for name in v2::REQUIRED {
        if !objects_v2.iter().any(|o| o.path == *name) {
            let bytes = b"test metadata";
            files.push(json!({"path":name,"bytes":bytes.len(),"sha256":v2::digest(bytes)}));
            objects_v2.push(v2::Object {
                path: (*name).into(),
                offset: rebuilt.len(),
                bytes: bytes.len(),
                sha256: v2::digest(bytes),
            });
            rebuilt.extend(bytes);
        }
    }
    body.clear();
    body.extend(rebuilt);
    let mut marker = e["marker"].clone();
    marker["schema"] = "hivezilla-direct-agave-processed-compact-v1".into();
    marker["state"] = "COMPLETE".into();
    marker["source_profile"] = "agave-bank-callbacks-v1".into();
    marker["finalized"] = false.into();
    marker["session_id"] = source.session_id.clone().into();
    marker["genesis_hash"] = source.genesis_hash.clone().into();
    marker["node_identity"] = source.node_identity.clone().into();
    marker["agave_revision"] = source.agave_revision.clone().into();
    marker["produced_blocks"][0]["bank_id"] = 7.into();
    marker["files"] = files.into();
    let marker_bytes = serde_json::to_vec_pretty(&marker).unwrap();
    let event = v2::BankEvent {
        sequence: 1,
        source_sequence: 3,
        kind: "block".into(),
        bank,
        payload_sha256: v2::digest(&marker_bytes),
        payload_frame_sha256: None,
        commitment: Some("processed".into()),
        confirmation_basis: Some("operator-trusted-agave-processed-bank-callback".into()),
        block_height: Some(400000000),
        recent_blockhash: None,
        observed_at_unix_ms: 1000,
        reason: None,
    };
    v2::encode(
        v2::Envelope {
            schema: v2::ENVELOPE.into(),
            profile: v2::PROFILE.into(),
            evidence_included: false,
            marker,
            objects: objects_v2,
            event,
        },
        &body,
    )
    .unwrap()
}
#[test]
fn real_encoded_bank_v2_preserves_failed_error_loaded_keys_and_height() {
    let f = bank_fixture(true);
    assert!(decode(&f.wire, &Options::default()).is_err());
    let v: Value =
        serde_json::from_slice(&decode_banks(&f.wire, &Options::default()).unwrap()).unwrap();
    assert_eq!(v["blocks"][0]["blockHeight"], 400000000);
    let tx = &v["blocks"][0]["transactions"][0];
    assert_eq!(tx["success"], false);
    assert!(!tx["metadata"]["err"].is_null());
    assert_eq!(tx["accountKeys"].as_array().unwrap().len(), 3);
    let mut e = f.envelope.clone();
    e.event.block_height = Some(400000001);
    let n = u32_at(&f.wire, 8).unwrap() as usize;
    let bad = blockzilla_bank_stream_v2::encode(e, &f.wire[16 + n..]).unwrap();
    assert!(decode_banks(&bad.wire, &Options::default()).is_err());
}

/// Synthetic current Rust compact objects, not live provider data.
#[test]
#[ignore = "explicit fixture export; BLOCKZILLA_BANK_FIXTURES required"]
fn export_bank_sdk_fixtures() {
    use blockzilla_bank_stream_v2 as v2;
    let dir = std::path::PathBuf::from(std::env::var("BLOCKZILLA_BANK_FIXTURES").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let b = bank_fixture(true);
    let mut event = b.envelope.event.clone();
    event.sequence = 2;
    event.source_sequence = 9;
    event.kind = "confirmed".into();
    event.commitment = Some("confirmed".into());
    event.confirmation_basis = Some("operator-trusted-agave-confirmed-bank-callback".into());
    event.payload_frame_sha256 = Some(b.sha256.clone());
    event.recent_blockhash = Some(v2::RecentBlockhash {
        blockhash: base58(&event.bank.blockhash),
        context_slot: event.bank.slot,
        last_valid_block_height: 400000150,
    });
    let c = v2::encode(
        v2::Envelope {
            schema: v2::ENVELOPE.into(),
            profile: v2::PROFILE.into(),
            evidence_included: false,
            marker: v2::control_marker(&b.source, &event),
            objects: vec![],
            event: event.clone(),
        },
        &[],
    )
    .unwrap();
    event.sequence = 3;
    event.source_sequence = 10;
    event.kind = "root".into();
    event.commitment = Some("finalized".into());
    event.confirmation_basis = Some("operator-trusted-agave-rooted-bank-callback".into());
    let r = v2::encode(
        v2::Envelope {
            schema: v2::ENVELOPE.into(),
            profile: v2::PROFILE.into(),
            evidence_included: false,
            marker: v2::control_marker(&b.source, &event),
            objects: vec![],
            event,
        },
        &[],
    )
    .unwrap();
    for (name, f) in [("block", &b), ("confirmed", &c), ("root", &r)] {
        std::fs::write(dir.join(format!("{name}.bin")), &f.wire).unwrap();
        std::fs::write(
            dir.join(format!("{name}.decoded.json")),
            decode_banks(&f.wire, &Default::default()).unwrap(),
        )
        .unwrap();
    }
    let head = v2::Head {
        schema: v2::HEAD.into(),
        profile: v2::PROFILE.into(),
        source: b.source.clone(),
        cursor: Some(c.cursor.clone()),
        earliest_after: Some(v2::cursor(&b.source.session_id, 0, "-")),
        status: "live".into(),
        sampled_at_unix_ms: 1000,
        source_observed_at_unix_ms: Some(1000),
        source_lag_ms: Some(0),
        latest_confirmed: Some(v2::ConfirmedObservation::from_frame(&c).unwrap()),
    };
    std::fs::write(
        dir.join("head.json"),
        serde_json::to_vec_pretty(&head).unwrap(),
    )
    .unwrap();
}
