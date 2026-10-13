use super::*;
use blockzilla_archive_v2::{
    ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
    ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
};
use blockzilla_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
    CompactTransactionError, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
};
use blockzilla_primitives::wincode_leb128_config;

fn approval(tag: u8, amount: u64) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&amount.to_le_bytes());
    if tag == 13 {
        bytes.push(6);
    }
    bytes
}
fn header() -> CompactMessageHeader {
    CompactMessageHeader {
        num_required_signatures: 1,
        num_readonly_signed_accounts: 0,
        num_readonly_unsigned_accounts: 0,
    }
}
fn metadata(count: usize) -> CompactMetaV1 {
    CompactMetaV1 {
        err: None,
        fee: 5000,
        pre_balances: vec![1; count],
        post_balances: vec![1; count],
        inner_instructions: Some(Vec::new()),
        logs: None,
        pre_token_balances: vec![],
        post_token_balances: vec![],
        rewards: vec![],
        loaded_writable_addresses: vec![],
        loaded_readonly_addresses: vec![],
        return_data: None,
        compute_units_consumed: Some(100),
        cost_units: None,
    }
}
fn serialize<T: wincode::SchemaWrite<blockzilla_primitives::WincodeLeb128Config, Src = T>>(
    value: &T,
) -> Vec<u8> {
    wincode::config::serialize(value, wincode_leb128_config()).unwrap()
}
fn fixture() -> (ArchiveV2HotMessagePayload, CompactMetaV1) {
    let targets = Targets::new().unwrap();
    let keys = vec![
        CompactPubkey::Raw([1; 32]),
        CompactPubkey::Raw([2; 32]),
        CompactPubkey::Raw(targets.state),
        CompactPubkey::Raw(targets.classic),
        CompactPubkey::Raw(targets.token_2022),
        CompactPubkey::Raw([6; 32]),
        CompactPubkey::Raw([7; 32]),
    ];
    let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
        header: header(),
        account_keys: keys,
        recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
        instructions: vec![
            ArchiveV2HotInstruction {
                program_id_index: 3,
                accounts: vec![1, 2, 0],
                data: ArchiveV2HotInstructionData::Raw(approval(4, u64::MAX)),
            },
            ArchiveV2HotInstruction {
                program_id_index: 6,
                accounts: vec![1, 2, 0],
                data: ArchiveV2HotInstructionData::Raw(vec![99]),
            },
        ],
    });
    let mut metadata = metadata(7);
    metadata.inner_instructions = Some(vec![CompactInnerInstructions {
        index: 1,
        instructions: vec![CompactInnerInstruction {
            program_id_index: 4,
            accounts: vec![1, 5, 2, 0],
            data: approval(13, 0),
            stack_height: Some(2),
        }],
    }]);
    (message, metadata)
}
fn project(message: &[u8], metadata: &[u8], flags: u32) -> Result<Vec<Approval>> {
    let targets = Targets::new()?;
    project_transaction(
        CompactV2MessageProjector::new(CompactV2MessageSchema::Current, 0),
        &[
            CompactPubkey::Raw(targets.classic),
            CompactPubkey::Raw(targets.token_2022),
        ],
        MetadataPolicy::HistoricalCompatibility,
        &targets,
        flags,
        message,
        metadata,
        |reference, key| *reference == CompactPubkey::Raw(*key),
        |reference| match reference {
            CompactPubkey::Raw(key) => Ok(*key),
            _ => bail!("fixture unknown registry ID"),
        },
        1049,
        453_169_000,
        1,
    )
}

#[test]
fn outer_and_cpi_exact_u64_max_and_zero_approval() {
    let (message, metadata) = fixture();
    let events = project(
        &serialize(&message),
        &serialize(&metadata),
        ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        (events[0].outer_ix, events[0].inner_ix, events[0].tag),
        (0, -1, 4)
    );
    assert_eq!(events[0].amount, "18446744073709551615");
    assert_eq!(events[0].data, approval(4, u64::MAX));
    assert_eq!(
        (events[1].outer_ix, events[1].inner_ix, events[1].tag),
        (1, 0, 13)
    );
    assert_eq!(events[1].amount, "0");
    assert_eq!(events[1].decimals, Some(6));
    assert_eq!(events[1].mint, Some(bs58::encode([6; 32]).into_string()));
    assert_eq!(events[1].token_program, TOKEN_2022_ADDRESS);
}

#[test]
fn standalone_token_approval_does_not_need_intents_invocation() {
    let (mut message, mut metadata) = fixture();
    let ArchiveV2HotMessagePayload::Legacy(ref mut legacy) = message else {
        unreachable!()
    };
    legacy.instructions.truncate(1);
    metadata.inner_instructions = Some(vec![]);
    assert_eq!(
        project(
            &serialize(&message),
            &serialize(&metadata),
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
        )
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn known_failed_raw_transaction_does_not_change_candidates() {
    assert!(
        project(
            &[],
            &[],
            ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn failure_metadata_without_error_flag_is_not_success() {
    let (message, mut metadata) = fixture();
    metadata.err = Some(CompactTransactionError::AccountNotFound);
    assert!(
        project(
            &serialize(&message),
            &serialize(&metadata),
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
        )
        .is_err()
    );
}

#[test]
fn state_absent_static_space_proves_no_match_without_cpi_metadata() {
    let (mut message, _) = fixture();
    let ArchiveV2HotMessagePayload::Legacy(ref mut legacy) = message else {
        unreachable!()
    };
    legacy.account_keys[2] = CompactPubkey::Raw([8; 32]);
    assert!(project(&serialize(&message), &[], 0).unwrap().is_empty());
}

#[test]
fn selected_state_with_missing_cpi_or_raw_metadata_fails() {
    let (message, mut metadata) = fixture();
    metadata.inner_instructions = None;
    let message = serialize(&message);
    let metadata = serialize(&metadata);
    assert!(project(&message, &metadata, ARCHIVE_V2_TX_FLAG_HAS_METADATA).is_err());
    assert!(
        project(
            &message,
            &metadata,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
        )
        .is_err()
    );
    assert!(project(&message, &[], 0).is_err());
}

#[test]
fn incomplete_bytes_fail_and_valid_trailing_bytes_are_preserved() {
    assert!(decode_approval(&approval(4, 100)[..8]).is_err());
    let mut padded = approval(13, 100);
    padded.push(0);
    assert_eq!(decode_approval(&padded).unwrap().unwrap().amount, 100);
    let (mut message, metadata) = fixture();
    let ArchiveV2HotMessagePayload::Legacy(ref mut legacy) = message else {
        unreachable!()
    };
    let mut trailing = approval(4, 100);
    trailing.extend_from_slice(&[99, 0]);
    legacy.instructions[0].data = ArchiveV2HotInstructionData::Raw(trailing.clone());
    let events = project(
        &serialize(&message),
        &serialize(&metadata),
        ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    )
    .unwrap();
    assert_eq!(events[0].data, trailing);
    assert_eq!(events[0].amount, "100");
    let mut bytes = serialize(&message);
    bytes.push(0);
    assert!(
        project(
            &bytes,
            &serialize(&metadata),
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
        )
        .is_err()
    );
}

#[test]
fn token_batch_children_keep_distinct_coordinates_and_parent_bytes() {
    let (mut message, mut metadata) = fixture();
    let ArchiveV2HotMessagePayload::Legacy(ref mut legacy) = message else {
        unreachable!()
    };
    let mut batch = vec![255, 3, 9];
    batch.extend_from_slice(&approval(4, 5));
    batch.extend_from_slice(&[3, 9]);
    batch.extend_from_slice(&approval(4, 9));
    legacy.instructions = vec![ArchiveV2HotInstruction {
        program_id_index: 3,
        accounts: vec![1, 2, 0, 5, 2, 0],
        data: ArchiveV2HotInstructionData::Raw(batch.clone()),
    }];
    metadata.inner_instructions = Some(vec![]);
    let events = project(
        &serialize(&message),
        &serialize(&metadata),
        ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!((events[0].batch_ix, events[1].batch_ix), (0, 1));
    assert_eq!((events[0].outer_ix, events[1].outer_ix), (0, 0));
    assert_eq!(events[1].token_account, bs58::encode([6; 32]).into_string());
    assert_eq!(events[1].parent_data, Some(batch));
    assert_eq!(events[1].parent_accounts, Some(vec![1, 2, 0, 5, 2, 0]));
}

#[test]
fn malformed_and_nested_batch_fail_without_saving_valid_prefix() {
    let mut batch = vec![255, 3, 9];
    batch.extend_from_slice(&approval(4, 5));
    batch.extend_from_slice(&[1, 1, 255]);
    assert!(visit_token_children(&[1, 2, 0, 1], &batch, |_, _, _| Ok(())).is_err());
    assert!(visit_token_children(&[1, 2], &[255, 3, 9, 4], |_, _, _| Ok(())).is_err());
    assert!(visit_token_children(&[1, 2], &[255, 2, 0], |_, _, _| Ok(())).is_err());
}

#[test]
fn only_gateway_uses_bounded_parallel_network_input() {
    assert!(scan_config(2, false).network_input.is_none());
    let network = scan_config(8, true).network_input.unwrap();
    assert_eq!(network.workers, 8);
    assert_eq!(network.range_bytes, 32 * 1024 * 1024);
    assert_eq!(network.max_buffer_bytes, 256 * 1024 * 1024);
}

#[test]
fn loaded_keys_use_static_then_writable_then_readonly() {
    let targets = Targets::new().unwrap();
    let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
        header: header(),
        account_keys: vec![
            CompactPubkey::Raw([1; 32]),
            CompactPubkey::Raw([2; 32]),
            CompactPubkey::Raw(targets.classic),
            CompactPubkey::Raw([6; 32]),
            CompactPubkey::Raw([7; 32]),
        ],
        recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
        instructions: vec![ArchiveV2HotInstruction {
            program_id_index: 2,
            accounts: vec![1, 5, 0],
            data: ArchiveV2HotInstructionData::Raw(approval(4, 10)),
        }],
        address_table_lookups: vec![OwnedCompactAddressTableLookup {
            account_key: CompactPubkey::Raw([9; 32]),
            writable_indexes: vec![0, 1],
            readonly_indexes: vec![2],
        }],
    });
    let mut metadata = metadata(8);
    metadata.loaded_writable_addresses = vec![
        CompactPubkey::Raw(targets.state),
        CompactPubkey::Raw([10; 32]),
    ];
    metadata.loaded_readonly_addresses = vec![CompactPubkey::Raw(targets.token_2022)];
    metadata.inner_instructions = Some(vec![CompactInnerInstructions {
        index: 0,
        instructions: vec![CompactInnerInstruction {
            program_id_index: 7,
            accounts: vec![6, 3, 5, 0],
            data: approval(13, 20),
            stack_height: Some(2),
        }],
    }]);
    let events = project(
        &serialize(&message),
        &serialize(&metadata),
        ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[1].token_account,
        bs58::encode([10; 32]).into_string()
    );
    metadata.loaded_writable_addresses[0] = CompactPubkey::Raw([11; 32]);
    metadata.inner_instructions = None;
    assert!(
        project(
            &serialize(&message),
            &serialize(&metadata),
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
        )
        .unwrap()
        .is_empty()
    );
}
