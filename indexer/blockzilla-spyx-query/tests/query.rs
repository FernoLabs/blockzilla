use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    sync::Arc,
};

use axum::{body::to_bytes, http::Request};
use blockzilla_archive_v2::{
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
};
use blockzilla_compact::{
    CompactMessageHeader, CompactMetaV1, OwnedCompactAddressTableLookup,
    OwnedCompactRecentBlockhash,
};
use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter, wincode_leb128_config};
use blockzilla_spyx_query::index_format::{
    INDEX_HEADER_BYTES, INDEX_MANIFEST_FILE, IndexManifest, SIGNATURE_LOOKUP_FILE,
    SIGNATURE_RECORD_BYTES,
};
use blockzilla_spyx_query::{
    BuildConfig, QueryOpenOptions, QueryStore, TransactionCoordinate, build_index, router,
    verify_index,
};
use blockzilla_token_transaction_dump::{
    ACCOUNTS_FILE, DUMP_MANIFEST_FILE, DUMP_SCHEMA_VERSION, DumpArtifactKind, DumpManifest,
    DumpSourceBinding, DumpStreamKind, DumpWireProfile, PUBKEY_REGISTRY_FILE,
    PUBKEY_REGISTRY_ID_BASE, SIGNATURES_FILE, TRANSACTIONS_FILE, TokenTransactionBlockContext,
    TokenTransactionDumpFooter, TokenTransactionDumpHeader, TokenTransactionDumpRecord,
    TokenTransactionRecord,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tower::ServiceExt;

const LOCATOR_HEADER_BYTES: u64 = 128;
const LOCATOR_FLAGS_OFFSET: u64 = 36;

struct Fixture {
    _temporary: TempDir,
    dump: std::path::PathBuf,
    index: std::path::PathBuf,
    signature_a: [u8; 64],
    signature_b: [u8; 64],
}

fn instruction(program_id_index: u8) -> ArchiveV2HotInstruction {
    ArchiveV2HotInstruction {
        program_id_index,
        accounts: vec![0],
        data: ArchiveV2HotInstructionData::Raw(vec![1]),
    }
}

fn legacy_message(required_signatures: u8, account_ids: &[u32]) -> Vec<u8> {
    wincode::config::serialize(
        &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: required_signatures,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: account_ids.iter().copied().map(CompactPubkey::Id).collect(),
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(
                u8::try_from(account_ids.len() - 1).expect("test account count fits u8"),
            )],
        }),
        wincode_leb128_config(),
    )
    .unwrap()
}

fn loaded_message_and_metadata() -> (Vec<u8>, Vec<u8>) {
    let message = wincode::config::serialize(
        &ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(2), CompactPubkey::Id(3)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![instruction(1)],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(3),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        }),
        wincode_leb128_config(),
    )
    .unwrap();
    let metadata = wincode::config::serialize(
        &CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![0; 4],
            post_balances: vec![0; 4],
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![CompactPubkey::Id(1)],
            loaded_readonly_addresses: vec![CompactPubkey::Id(4)],
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        },
        wincode_leb128_config(),
    )
    .unwrap();
    (message, metadata)
}

fn fixture(sentinel_collision: bool) -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let dump = temporary.path().join("dump");
    let index = temporary.path().join("index");
    fs::create_dir(&dump).unwrap();

    let mint = [7u8; 32];
    let mint_signature = [8u8; 64];
    let signature_a = [1u8; 64];
    let signature_b = [2u8; 64];
    let signature_c = [3u8; 64];
    let signatures = [signature_a, signature_b, signature_a, signature_c]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let registry = [[11u8; 32], [12u8; 32], [13u8; 32], [14u8; 32]].concat();
    let accounts = vec![13u8];
    let message_one = legacy_message(2, &[1, 2, 3]);
    let message_two = legacy_message(1, &[2, 1]);
    let (message_three, metadata_three) = loaded_message_and_metadata();

    let block_one = TokenTransactionBlockContext {
        slot: 1_001,
        parent_slot: 1_000,
        blockhash_id: 2,
        previous_blockhash_id: 1,
        block_time: sentinel_collision.then_some(i64::MIN).or(Some(50)),
        block_height: Some(70),
        transaction_count: 2,
    };
    let block_two = TokenTransactionBlockContext {
        slot: 1_002,
        parent_slot: 1_001,
        blockhash_id: 3,
        previous_blockhash_id: 2,
        block_time: Some(51),
        block_height: Some(71),
        transaction_count: 1,
    };
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
            source_generation_digest: [21; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 1,
            block: block_one.clone(),
            tx_index: 0,
            flags: 0,
            source_first_signature_ordinal: 10,
            signature_count: 2,
            dump_signature_ordinal: Some(0),
            message_bytes: message_one,
            metadata_bytes: Vec::new(),
        }),
        TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
            source_epoch: 1,
            source_generation_digest: [21; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 1,
            block: block_one,
            tx_index: 1,
            flags: 0,
            source_first_signature_ordinal: 12,
            signature_count: 1,
            dump_signature_ordinal: Some(2),
            message_bytes: message_two,
            metadata_bytes: Vec::new(),
        }),
        TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
            source_epoch: 1,
            source_generation_digest: [21; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 2,
            block: block_two,
            tx_index: 0,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
            source_first_signature_ordinal: 13,
            signature_count: 1,
            dump_signature_ordinal: Some(3),
            message_bytes: message_three,
            metadata_bytes: metadata_three,
        }),
        TokenTransactionDumpRecord::Footer(TokenTransactionDumpFooter {
            epochs: 1,
            blocks_scanned: 2,
            transactions_scanned: 3,
            transactions_written: 3,
            pubkeys: 4,
            signatures: 4,
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
    fs::write(dump.join(TRANSACTIONS_FILE), &transaction_bytes).unwrap();
    fs::write(dump.join(SIGNATURES_FILE), &signatures).unwrap();
    fs::write(dump.join(PUBKEY_REGISTRY_FILE), &registry).unwrap();
    fs::write(dump.join(ACCOUNTS_FILE), &accounts).unwrap();

    let manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::Consolidated,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: 1_001,
        mint_signature: bs58::encode(mint_signature).into_string(),
        workers: 2,
        source_binding: DumpSourceBinding::TrustedLocalSizesOnly {
            cluster_id: "synthetic-test".to_owned(),
            slots_per_epoch: 1_000,
            wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
        },
        first_epoch: 1,
        last_epoch: 1,
        transactions: 3,
        signatures: Some(4),
        pubkeys: Some(4),
        transaction_stream: TRANSACTIONS_FILE.to_owned(),
        transaction_stream_sha256: Some(digest(&transaction_bytes)),
        account_id_log: None,
        account_id_log_sha256: None,
        discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
        discovered_accounts_sha256: Some(digest(&accounts)),
        discovered_account_count: Some(1),
        signature_stream: Some(SIGNATURES_FILE.to_owned()),
        signature_stream_sha256: Some(digest(&signatures)),
        pubkey_registry: Some(PUBKEY_REGISTRY_FILE.to_owned()),
        pubkey_registry_sha256: Some(digest(&registry)),
        registry_maps: None,
    };
    fs::write(
        dump.join(DUMP_MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    Fixture {
        _temporary: temporary,
        dump,
        index,
        signature_a,
        signature_b,
    }
}

fn digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn build(fixture: &Fixture, max_transactions: Option<u64>) {
    build_index(&BuildConfig {
        dump: fixture.dump.clone(),
        output: fixture.index.clone(),
        max_transactions,
    })
    .unwrap();
}

fn read_index_manifest(fixture: &Fixture) -> IndexManifest {
    serde_json::from_slice(&fs::read(fixture.index.join(INDEX_MANIFEST_FILE)).unwrap()).unwrap()
}

fn write_index_manifest(fixture: &Fixture, manifest: &IndexManifest) {
    let mut bytes = serde_json::to_vec_pretty(manifest).unwrap();
    bytes.push(b'\n');
    fs::write(fixture.index.join(INDEX_MANIFEST_FILE), bytes).unwrap();
}

#[test]
fn build_lookup_and_positioned_detail_round_trip() {
    let fixture = fixture(false);
    build(&fixture, None);
    let store = QueryStore::open(&fixture.dump, &fixture.index).unwrap();

    assert_eq!(store.transaction_count(), 3);
    assert_eq!(store.signature_occurrence_count(), 4);
    let signature_a = store.lookup_signature(fixture.signature_a).unwrap();
    assert_eq!(signature_a.transaction_ids, [0, 1]);
    assert_eq!(signature_a.occurrences.len(), 2);
    assert_eq!(signature_a.occurrences[0].transaction_id, 0);
    assert_eq!(signature_a.occurrences[1].transaction_id, 1);
    assert_eq!(
        store
            .lookup_signature(fixture.signature_b)
            .unwrap()
            .transaction_ids,
        [0]
    );
    let coordinate = TransactionCoordinate {
        epoch: 1,
        slot: 1_002,
        source_block_id: 2,
        tx_index: 0,
    };
    assert_eq!(store.lookup_coordinate(coordinate).unwrap(), Some(2));

    let mut scratch = Vec::new();
    let detail = store.transaction_detail(0, &mut scratch).unwrap();
    assert_eq!(detail.coordinate.slot, 1_001);
    assert!(!detail.message_bytes_base64.is_empty());
    assert_eq!(detail.metadata_bytes_base64, "");
    assert_eq!(detail.signatures.len(), 2);
    assert_eq!(detail.accounts.len(), 3);
    assert_eq!(detail.accounts[0].account_index, 0);
    assert_eq!(detail.accounts[0].registry_id, 1);
    assert_eq!(
        detail.accounts[0].address,
        bs58::encode([11u8; 32]).into_string()
    );
    assert_eq!(detail.accounts[1].account_index, 1);
    assert_eq!(detail.accounts[1].registry_id, 2);
    assert_eq!(
        detail.accounts[1].address,
        bs58::encode([12u8; 32]).into_string()
    );
    assert_eq!(detail.accounts[2].account_index, 2);
    assert_eq!(detail.accounts[2].registry_id, 3);

    let loaded_detail = store.transaction_detail(2, &mut scratch).unwrap();
    assert_eq!(loaded_detail.accounts.len(), 4);
    assert_eq!(loaded_detail.accounts[0].account_index, 0);
    assert_eq!(loaded_detail.accounts[0].registry_id, 2);
    assert_eq!(loaded_detail.accounts[1].account_index, 1);
    assert_eq!(loaded_detail.accounts[1].registry_id, 3);
    assert_eq!(loaded_detail.accounts[2].account_index, 2);
    assert_eq!(loaded_detail.accounts[2].registry_id, 1);
    assert_eq!(
        loaded_detail.accounts[2].address,
        bs58::encode([11u8; 32]).into_string()
    );
    assert_eq!(loaded_detail.accounts[3].account_index, 3);
    assert_eq!(loaded_detail.accounts[3].registry_id, 4);
    assert_eq!(
        loaded_detail.accounts[3].address,
        bs58::encode([14u8; 32]).into_string()
    );
    let posting_detail = store.posting_transaction_detail(0).unwrap();
    assert_eq!(posting_detail.transaction_id, 0);
    assert_eq!(posting_detail.coordinate.slot, 1_001);
    assert_eq!(
        posting_detail.first_signature,
        bs58::encode(fixture.signature_a).into_string()
    );

    let verified = verify_index(&fixture.dump, &fixture.index, false).unwrap();
    assert!(verified.complete);
    assert_eq!(verified.transactions, 3);
}

#[tokio::test]
async fn canary_requires_explicit_permission_and_reports_its_state() {
    let fixture = fixture(false);
    build(&fixture, Some(2));
    assert!(QueryStore::open(&fixture.dump, &fixture.index).is_err());
    let store = QueryStore::open_with_options(
        &fixture.dump,
        &fixture.index,
        QueryOpenOptions {
            allow_incomplete: true,
        },
    )
    .unwrap();
    assert!(!store.complete());
    assert_eq!(store.transaction_count(), 2);
    assert!(verify_index(&fixture.dump, &fixture.index, false).is_err());
    assert!(verify_index(&fixture.dump, &fixture.index, true).is_ok());

    let response = router(Arc::new(store), "*", 1)
        .unwrap()
        .oneshot(
            Request::get("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let health: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["index"]["complete"], false);
}

#[test]
fn corrupt_truncated_and_source_binding_inputs_fail_closed() {
    let truncated = fixture(false);
    build(&truncated, None);
    let locator_path = truncated.index.join("locators.bin");
    let locator = OpenOptions::new().write(true).open(locator_path).unwrap();
    locator
        .set_len(locator.metadata().unwrap().len() - 1)
        .unwrap();
    assert!(QueryStore::open(&truncated.dump, &truncated.index).is_err());

    let unknown_flags = fixture(false);
    build(&unknown_flags, None);
    let mut locator = OpenOptions::new()
        .read(true)
        .write(true)
        .open(unknown_flags.index.join("locators.bin"))
        .unwrap();
    locator
        .seek(SeekFrom::Start(LOCATOR_HEADER_BYTES + LOCATOR_FLAGS_OFFSET))
        .unwrap();
    locator.write_all(&u32::MAX.to_le_bytes()).unwrap();
    locator.sync_all().unwrap();
    assert!(QueryStore::open(&unknown_flags.dump, &unknown_flags.index).is_err());

    let rebound = fixture(false);
    build(&rebound, None);
    let mut manifest = OpenOptions::new()
        .append(true)
        .open(rebound.dump.join(DUMP_MANIFEST_FILE))
        .unwrap();
    manifest.write_all(b"\n").unwrap();
    manifest.sync_all().unwrap();
    assert!(QueryStore::open(&rebound.dump, &rebound.index).is_err());
}

#[test]
fn forged_complete_prefix_and_index_digest_mismatch_fail_closed() {
    let forged_complete = fixture(false);
    build(&forged_complete, Some(2));
    let mut manifest = read_index_manifest(&forged_complete);
    manifest.complete = true;
    manifest.canary_max_transactions = None;
    manifest.source.transaction_hash_verified_during_build = true;
    manifest.source.signature_hash_verified_during_build = true;
    write_index_manifest(&forged_complete, &manifest);
    let error = QueryStore::open(&forged_complete.dump, &forged_complete.index)
        .err()
        .expect("forged complete prefix must fail");
    assert!(
        error
            .to_string()
            .contains("complete query index counts differ")
    );

    let forged_canary_limit = fixture(false);
    build(&forged_canary_limit, Some(2));
    let mut manifest = read_index_manifest(&forged_canary_limit);
    manifest.canary_max_transactions = Some(1);
    write_index_manifest(&forged_canary_limit, &manifest);
    let error = QueryStore::open_with_options(
        &forged_canary_limit.dump,
        &forged_canary_limit.index,
        QueryOpenOptions {
            allow_incomplete: true,
        },
    )
    .err()
    .expect("forged canary limit must fail");
    assert!(
        error
            .to_string()
            .contains("canary query index counts differ")
    );

    let bad_digest = fixture(false);
    build(&bad_digest, None);
    let mut manifest = read_index_manifest(&bad_digest);
    manifest.signature_lookup.sha256 = "00".repeat(32);
    write_index_manifest(&bad_digest, &manifest);
    let error = QueryStore::open(&bad_digest.dump, &bad_digest.index)
        .err()
        .expect("index digest mismatch must fail");
    assert!(error.to_string().contains("signature index digest differs"));
}

#[cfg(unix)]
#[test]
fn open_store_keeps_using_its_pinned_source_and_index_handles_after_path_replacement() {
    let fixture = fixture(false);
    build(&fixture, None);
    let store = QueryStore::open(&fixture.dump, &fixture.index).unwrap();
    let signature_path = fixture.index.join(SIGNATURE_LOOKUP_FILE);
    let replacement_bytes = fs::metadata(&signature_path).unwrap().len();
    fs::rename(
        &signature_path,
        fixture.index.join("detached-signature-index.bin"),
    )
    .unwrap();
    fs::write(
        &signature_path,
        vec![0u8; usize::try_from(replacement_bytes).unwrap()],
    )
    .unwrap();
    let registry_path = fixture.dump.join(PUBKEY_REGISTRY_FILE);
    let registry_bytes = fs::metadata(&registry_path).unwrap().len();
    fs::rename(
        &registry_path,
        fixture.dump.join("detached-pubkey-registry.bin"),
    )
    .unwrap();
    fs::write(
        &registry_path,
        vec![0u8; usize::try_from(registry_bytes).unwrap()],
    )
    .unwrap();

    assert_eq!(
        store
            .lookup_signature(fixture.signature_b)
            .unwrap()
            .transaction_ids,
        [0]
    );
    let detail = store.transaction_detail(2, &mut Vec::new()).unwrap();
    assert_eq!(detail.accounts[0].registry_id, 2);
    assert_eq!(
        detail.accounts[0].address,
        bs58::encode([12u8; 32]).into_string()
    );
}

#[test]
fn full_verifier_rejects_structurally_valid_wrong_signature_mapping() {
    let fixture = fixture(false);
    build(&fixture, None);
    let signature_path = fixture.index.join(SIGNATURE_LOOKUP_FILE);
    let mut signatures = fs::read(&signature_path).unwrap();
    let transaction_id_offset = INDEX_HEADER_BYTES + 2 * SIGNATURE_RECORD_BYTES + 64;
    signatures[transaction_id_offset..transaction_id_offset + 8]
        .copy_from_slice(&2u64.to_le_bytes());
    signatures[transaction_id_offset + 8] = 0;
    fs::write(&signature_path, &signatures).unwrap();

    let mut manifest = read_index_manifest(&fixture);
    manifest.signature_lookup.sha256 = digest(&signatures);
    write_index_manifest(&fixture, &manifest);

    assert!(QueryStore::open(&fixture.dump, &fixture.index).is_ok());
    let error = verify_index(&fixture.dump, &fixture.index, false)
        .expect_err("wrong signature mapping must fail full verification");
    assert!(
        error
            .to_string()
            .contains("signature index occurrence differs")
    );
}

#[test]
fn builder_rejects_option_sentinel_collision() {
    let fixture = fixture(true);
    assert!(
        build_index(&BuildConfig {
            dump: fixture.dump,
            output: fixture.index,
            max_transactions: None,
        })
        .is_err()
    );
}

#[tokio::test]
async fn http_contract_covers_health_lookup_not_found_and_conflict() {
    let fixture = fixture(false);
    build(&fixture, None);
    let store = Arc::new(QueryStore::open(&fixture.dump, &fixture.index).unwrap());
    let app = router(store, "https://explorer.example", 2).unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::get("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let health: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["index"]["complete"], true);
    assert_eq!(health["index"]["transactions"], 3);

    let signature_b = bs58::encode(fixture.signature_b).into_string();
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/transactions/by-signature/{signature_b}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let signature_a = bs58::encode(fixture.signature_a).into_string();
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/transactions/by-signature/{signature_a}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let conflict: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(conflict["error"], "signature_has_multiple_transactions");

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/transactions/by-signature/not-base58!")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let response = app
        .clone()
        .oneshot(
            Request::get(
                "/api/v1/transactions/by-coordinate?epoch=1&slot=9999&source_block_id=1&tx_index=0",
            )
            .body(axum::body::Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/transactions/2")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let transaction: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(transaction["transaction"]["id"], 2);
    assert_eq!(
        transaction["transaction"]["accounts"][0]["account_index"],
        0
    );
    assert_eq!(transaction["transaction"]["accounts"][0]["registry_id"], 2);
    assert_eq!(
        transaction["transaction"]["accounts"][0]["address"],
        bs58::encode([12u8; 32]).into_string()
    );
    assert_eq!(
        transaction["transaction"]["accounts"][1]["account_index"],
        1
    );
    assert_eq!(transaction["transaction"]["accounts"][1]["registry_id"], 3);
    assert_eq!(
        transaction["transaction"]["accounts"][2]["account_index"],
        2
    );
    assert_eq!(transaction["transaction"]["accounts"][2]["registry_id"], 1);
    assert_eq!(
        transaction["transaction"]["accounts"][3]["account_index"],
        3
    );
    assert_eq!(transaction["transaction"]["accounts"][3]["registry_id"], 4);

    for (id, expected_status, expected_error) in [
        ("3", 404, "transaction_not_found"),
        ("+1", 400, "invalid_transaction_id"),
        ("-1", 400, "invalid_transaction_id"),
        ("18446744073709551616", 400, "invalid_transaction_id"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/transactions/{id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(error["error"], expected_error);
    }

    for kind in ["token-account", "target-address", "owner", "program"] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/postings/{kind}/11111111111111111111111111111111?cursor=&limit=50"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 501);
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(error["error"], "postings_not_available");
    }

    for uri in [
        "/api/v1/market/slot-candles?quote_mint=11111111111111111111111111111111&max_points=10",
        "/api/v1/market/program-volume?interval=60&time_from=0&time_to=119&max_points=2",
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 501);
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(error["error"], "market_not_available");
    }

    for (uri, expected_error) in [
        (
            "/api/v1/market/slot-candles?quote_mint=11111111111111111111111111111111&slot_from=20&slot_to=10",
            "invalid_market_slot_range",
        ),
        (
            "/api/v1/market/slot-candles?quote_mint=11111111111111111111111111111111&max_points=0",
            "invalid_market_max_points",
        ),
        (
            "/api/v1/market/program-volume?interval=60&time_from=0",
            "market_time_range_required",
        ),
        (
            "/api/v1/market/program-volume?interval=60&time_from=0&time_to=120&max_points=2",
            "market_program_volume_limit_exceeded",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(error["error"], expected_error);
    }
}
