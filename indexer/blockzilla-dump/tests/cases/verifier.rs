use std::{fs, path::Path};

use super::support::{MockGateway, hex_lower};

use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SIGNATURES_FILE, ArchiveV2HotBlockBlob,
    ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction,
    ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
    ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, WINCODE_ARCHIVE_V2_FLAG_LEB128,
    WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Genesis,
    WincodeArchiveV2GenesisEpochSchedule, WincodeArchiveV2GenesisFeeParams,
    WincodeArchiveV2GenesisInflationParams, WincodeArchiveV2GenesisPohParams,
    WincodeArchiveV2GenesisRentParams, WincodeArchiveV2Header, WincodeArchiveV2PohRecord,
    write_archive_v2_hot_block_index,
};
use blockzilla_compact::{CompactMessageHeader, CompactPohEntry, OwnedCompactRecentBlockhash};
use blockzilla_compact_v2_reader::poh::{SignatureMixinBuilder, derive_entry_hash};
use blockzilla_compact_v2_reader::{
    COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES, COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
    SignedInstruction, SignedMessage, SignedMessageVersion,
    archive_integrity::{PohProtocolBounds, PohSidecarSchema},
    manifest::{
        GENERATION_MANIFEST_FILE, GenerationFile, GenerationManifest, compute_generation_digest,
    },
    serialize_signed_message,
};
use blockzilla_compact_v2_reader::{CompactV2MessageSchema, CompactV2MetadataSchema};
use blockzilla_dump::{
    scan::SourceOptions,
    verify::{CheckState, DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK, VerifyRunConfig, run_verify},
};
use blockzilla_primitives::{CompactPubkey, wincode_leb128_config, write_u32_varint};
use blockzilla_registry::{KeyIndex, write_registry};
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[test]
fn local_and_gateway_verify_continuity_and_every_signature() {
    let archive = TempDir::new().unwrap();
    build_epoch_zero(archive.path());
    let local = run_verify(VerifyRunConfig {
        source: SourceOptions {
            archive: Some(archive.path().to_path_buf()),
            gateway: None,
            bearer_token: None,
            cache: None,
            allow_insecure_http: false,
            cluster_id: "testnet".into(),
            local_generation_prefix: Some("verifier-test".into()),
            epoch_zero_first_slot: 0,
            slots_per_epoch: 100,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
        },
        start_epoch: 0,
        end_epoch: 0,
        threads: 2,
        poh_requested: true,
        signatures_requested: true,
        poh_bounds: Some(PohProtocolBounds {
            ticks_per_slot: 64,
            hashes_per_tick: 1,
        }),
        poh_schema: PohSidecarSchema::Current,
        poh_max_hash_rounds_per_block: DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK,
    })
    .unwrap();
    assert_eq!(local.report.overall, CheckState::Passed);
    assert_eq!(local.report.epochs[0].continuity.state, CheckState::Passed);
    assert_eq!(local.report.epochs[0].poh.state, CheckState::Passed);
    assert_eq!(local.report.epochs[0].signatures.state, CheckState::Passed);
    assert!(!local.report.epochs[0].predecessor_boundary_checked);
    assert_eq!(local.report.epochs[0].signatures_verified, 2);

    let gateway = MockGateway::start(archive.path(), 0);
    let cache = TempDir::new().unwrap();
    let remote = run_verify(VerifyRunConfig {
        source: SourceOptions {
            archive: None,
            gateway: Some(gateway.base_url.clone()),
            bearer_token: None,
            cache: Some(cache.path().to_path_buf()),
            allow_insecure_http: true,
            cluster_id: "testnet".into(),
            local_generation_prefix: None,
            epoch_zero_first_slot: 0,
            slots_per_epoch: 100,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
        },
        start_epoch: 0,
        end_epoch: 0,
        threads: 2,
        poh_requested: false,
        signatures_requested: true,
        poh_bounds: None,
        poh_schema: PohSidecarSchema::Current,
        poh_max_hash_rounds_per_block: DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK,
    })
    .unwrap();
    assert_eq!(remote.report.overall, CheckState::Passed);
    assert_eq!(remote.report.epochs[0].continuity.state, CheckState::Passed);
    assert_eq!(remote.report.epochs[0].signatures.state, CheckState::Passed);
    assert_eq!(remote.report.epochs[0].signatures_verified, 2);

    let remote_poh = run_verify(VerifyRunConfig {
        source: SourceOptions {
            archive: None,
            gateway: Some(gateway.base_url.clone()),
            bearer_token: None,
            cache: Some(cache.path().to_path_buf()),
            allow_insecure_http: true,
            cluster_id: "testnet".into(),
            local_generation_prefix: None,
            epoch_zero_first_slot: 0,
            slots_per_epoch: 100,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
        },
        start_epoch: 0,
        end_epoch: 0,
        threads: 2,
        poh_requested: true,
        signatures_requested: false,
        poh_bounds: Some(PohProtocolBounds {
            ticks_per_slot: 64,
            hashes_per_tick: 1,
        }),
        poh_schema: PohSidecarSchema::Current,
        poh_max_hash_rounds_per_block: DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK,
    })
    .unwrap();
    assert_eq!(remote_poh.report.overall, CheckState::Failed);
    assert_eq!(
        remote_poh.report.epochs[0].continuity.state,
        CheckState::Passed
    );
    assert_eq!(remote_poh.report.epochs[0].poh.state, CheckState::Failed);
    assert_eq!(
        remote_poh.report.epochs[0].signatures.state,
        CheckState::NotRequested
    );
    assert_eq!(
        remote_poh.report.epochs[0].poh_max_hash_rounds_per_block,
        Some(DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK)
    );
    assert_eq!(
        remote_poh.report.epochs[0].poh_max_total_hash_rounds,
        Some(6_400)
    );
}

fn build_epoch_zero(root: &Path) {
    fs::write(
        root.join(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE),
        COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
    )
    .unwrap();
    let first = SigningKey::from_bytes(&[7; 32]);
    let second = SigningKey::from_bytes(&[9; 32]);
    let keys = [
        first.verifying_key().to_bytes(),
        second.verifying_key().to_bytes(),
        [4; 32],
    ];
    write_registry(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), &keys).unwrap();
    KeyIndex::build_from_slice(&keys)
        .write(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
        .unwrap();

    let genesis_hash = [2; 32];
    let header = CompactMessageHeader {
        num_required_signatures: 2,
        num_readonly_signed_accounts: 1,
        num_readonly_unsigned_accounts: 1,
    };
    let instruction_accounts = [0, 1];
    let instruction_data = [5, 6, 7];
    let signed_instructions = [SignedInstruction {
        program_id_index: 2,
        accounts: &instruction_accounts,
        data: &instruction_data,
    }];
    let signed_message = serialize_signed_message(&SignedMessage {
        version: SignedMessageVersion::Legacy,
        header,
        static_account_keys: &keys,
        recent_blockhash: genesis_hash,
        instructions: &signed_instructions,
    })
    .unwrap();
    let signatures = [
        first.sign(&signed_message).to_bytes(),
        second.sign(&signed_message).to_bytes(),
    ];
    fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), signatures.concat()).unwrap();
    let mut signature_mixin = SignatureMixinBuilder::new();
    for signature in &signatures {
        signature_mixin.push_signature(signature).unwrap();
    }
    let final_hash = derive_entry_hash(genesis_hash, 1, 1, Some(signature_mixin.finish())).unwrap();

    let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
        header,
        account_keys: vec![
            CompactPubkey::Id(1),
            CompactPubkey::Id(2),
            CompactPubkey::Id(3),
        ],
        recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
        instructions: vec![ArchiveV2HotInstruction {
            program_id_index: 2,
            accounts: instruction_accounts.to_vec(),
            data: ArchiveV2HotInstructionData::Raw(instruction_data.to_vec()),
        }],
    });
    let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
    let block = ArchiveV2HotBlockBlob {
        header: ArchiveV2HotBlockHeader {
            slot: 0,
            parent_slot: 0,
            blockhash_id: 1,
            previous_blockhash_id: 0,
            block_time: None,
            block_height: None,
            rewards: None,
        },
        tx_count: 1,
        tx_rows: vec![ArchiveV2HotTxRow {
            tx_index: 0,
            flags: 0,
            message_offset: 0,
            message_len: message.len() as u32,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count: 2,
            reserved: [0; 3],
        }],
        message_bytes: message,
        metadata_bytes: Vec::new(),
    };
    let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
    let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
    fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
    write_archive_v2_hot_block_index(
        &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        compressed.len() as u64,
        1,
        0,
        &[ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 0,
            compressed_offset: 0,
            compressed_len: compressed.len() as u32,
            uncompressed_len: uncompressed.len() as u32,
            tx_count: 1,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 2,
        }],
    )
    .unwrap();
    fs::write(
        root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
        [genesis_hash, final_hash].concat(),
    )
    .unwrap();
    let poh = WincodeArchiveV2PohRecord {
        block_id: 0,
        slot: 0,
        entries: vec![CompactPohEntry {
            num_hashes: 1,
            hash: final_hash,
            tx_count: 1,
            signature_count: 2,
        }],
    };
    let payload = wincode::config::serialize(&poh, wincode_leb128_config()).unwrap();
    let mut framed = Vec::new();
    write_u32_varint(&mut framed, payload.len() as u32).unwrap();
    framed.extend_from_slice(&payload);
    fs::write(root.join(ARCHIVE_V2_POH_FILE), framed).unwrap();
    write_metadata(root, genesis_hash);
    write_manifest(root);
}

fn write_metadata(root: &Path, genesis_hash: [u8; 32]) {
    let records = [
        ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }),
        ArchiveV2HotMetaRecord::Genesis(WincodeArchiveV2Genesis {
            genesis_hash,
            genesis_bin_len: 0,
            creation_time_unix: 0,
            cluster_id: 0,
            ticks_per_slot: 64,
            poh_params: WincodeArchiveV2GenesisPohParams {
                tick_duration_secs: 0,
                tick_duration_nanos: 400_000_000,
                tick_count: None,
                hashes_per_tick: Some(1),
            },
            fees: WincodeArchiveV2GenesisFeeParams {
                target_lamports_per_sig: 10_000,
                target_sigs_per_slot: 20_000,
                min_lamports_per_sig: 5_000,
                max_lamports_per_sig: 100_000,
                burn_percent: 100,
            },
            rent: WincodeArchiveV2GenesisRentParams {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
                burn_percent: 100,
            },
            inflation: WincodeArchiveV2GenesisInflationParams {
                initial: 0.0,
                terminal: 0.0,
                taper: 0.0,
                foundation: 0.0,
                foundation_term: 0.0,
                padding: [0; 8],
            },
            epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
                slots_per_epoch: 100,
                leader_schedule_slot_offset: 100,
                warmup: false,
                first_normal_epoch: 0,
                first_normal_slot: 0,
            },
            accounts: Vec::new(),
            builtins: Vec::new(),
            reward_pools: Vec::new(),
        }),
        ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: 1,
            transactions: 1,
            ..WincodeArchiveV2Footer::default()
        }),
    ];
    let mut bytes = Vec::new();
    for record in records {
        let payload = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
        write_u32_varint(&mut bytes, payload.len() as u32).unwrap();
        bytes.extend_from_slice(&payload);
    }
    fs::write(root.join(ARCHIVE_V2_META_FILE), bytes).unwrap();
}

fn write_manifest(root: &Path) {
    let mut files = Vec::new();
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_POH_FILE,
        COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
    ] {
        let bytes = fs::read(root.join(name)).unwrap();
        files.push(GenerationFile {
            name: name.into(),
            size: bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(&bytes)),
        });
    }
    let mut manifest = GenerationManifest {
        schema_version: 1,
        cluster_id: "testnet".into(),
        epoch: 0,
        generation_id: "verify-test-0".into(),
        generation_digest: "0".repeat(64),
        slots_per_epoch: 100,
        complete: true,
        files,
    };
    manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
    fs::write(
        root.join(GENERATION_MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}
