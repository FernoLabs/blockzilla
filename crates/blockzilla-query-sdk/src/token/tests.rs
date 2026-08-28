use crate::{
    BlockHeader, CpiCoverage, ExecutionStatus, InstructionCoordinate, InstructionCoverage,
    InstructionDataCoverage, MAX_CANONICAL_SHORT_VEC_ITEMS, ResolvedInstruction, TransactionHeader,
    TransactionView,
};

use super::decode::validate_classic_token_instruction_structure;
use super::*;

const TARGET_MINT: PubkeyBytes = [42; 32];
const OTHER_MINT: PubkeyBytes = [43; 32];

const fn key(value: u8) -> PubkeyBytes {
    [value; 32]
}

fn indexed_key(value: u32) -> PubkeyBytes {
    let mut address = [0u8; 32];
    address[..4].copy_from_slice(&value.to_le_bytes());
    address[31] = 1;
    address
}

fn outer(order: u32, accounts: Vec<PubkeyBytes>, data: Vec<u8>) -> ResolvedInstruction {
    ResolvedInstruction {
        coordinate: InstructionCoordinate {
            order,
            outer_index: order,
            inner_index: None,
            stack_height: None,
        },
        program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
        accounts,
        data_coverage: InstructionDataCoverage::Exact,
        data,
    }
}

fn inner(
    order: u32,
    outer_index: u32,
    inner_index: u32,
    stack_height: u32,
    accounts: Vec<PubkeyBytes>,
    data: Vec<u8>,
) -> ResolvedInstruction {
    ResolvedInstruction {
        coordinate: InstructionCoordinate {
            order,
            outer_index,
            inner_index: Some(inner_index),
            stack_height: Some(stack_height),
        },
        program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
        accounts,
        data_coverage: InstructionDataCoverage::Exact,
        data,
    }
}

fn non_token_outer(order: u32) -> ResolvedInstruction {
    let mut instruction = outer(order, vec![key(200)], vec![99]);
    instruction.program_id = key(201);
    instruction
}

fn amount_data(tag: u8, amount: u64) -> Vec<u8> {
    let mut data = vec![tag];
    data.extend_from_slice(&amount.to_le_bytes());
    data
}

fn checked_data(tag: u8, amount: u64, decimals: u8) -> Vec<u8> {
    let mut data = amount_data(tag, amount);
    data.push(decimals);
    data
}

fn owner_data(tag: u8, owner: PubkeyBytes) -> Vec<u8> {
    let mut data = vec![tag];
    data.extend_from_slice(&owner);
    data
}

fn process(
    tracker: &mut TargetMintTracker,
    status: ExecutionStatus,
    instructions: &[ResolvedInstruction],
) -> TrackedTokenTransaction {
    process_with_failed_outer(tracker, status, None, instructions)
}

fn process_with_failed_outer(
    tracker: &mut TargetMintTracker,
    status: ExecutionStatus,
    failed_outer_instruction_index: Option<u32>,
    instructions: &[ResolvedInstruction],
) -> TrackedTokenTransaction {
    try_process_with_failed_outer(
        tracker,
        status,
        failed_outer_instruction_index,
        instructions,
    )
    .unwrap()
}

fn try_process_with_failed_outer(
    tracker: &mut TargetMintTracker,
    status: ExecutionStatus,
    failed_outer_instruction_index: Option<u32>,
    instructions: &[ResolvedInstruction],
) -> Result<TrackedTokenTransaction, TargetMintTrackerError> {
    tracker.process_transaction(TransactionView {
        block: BlockHeader {
            epoch: 1,
            block_ordinal: 2,
            slot: 3,
        },
        header: TransactionHeader {
            tx_index: 0,
            status,
            failed_outer_instruction_index,
            instruction_coverage: InstructionCoverage::Complete,
            cpi_coverage: CpiCoverage::Complete,
        },
        primary_signature: None,
        required_signers: &[],
        instructions,
    })
}

fn init_account(order: u32, account: PubkeyBytes, mint: PubkeyBytes) -> ResolvedInstruction {
    outer(order, vec![account, mint, key(90), key(91)], vec![1])
}

fn unchecked_transfer(
    order: u32,
    source: PubkeyBytes,
    destination: PubkeyBytes,
    amount: u64,
) -> ResolvedInstruction {
    outer(
        order,
        vec![source, destination, key(92)],
        amount_data(3, amount),
    )
}

fn batch_outer(order: u32, children: Vec<(Vec<PubkeyBytes>, Vec<u8>)>) -> ResolvedInstruction {
    let mut accounts = Vec::new();
    let mut data = vec![255];
    for (child_accounts, child_data) in children {
        data.push(u8::try_from(child_accounts.len()).unwrap());
        data.push(u8::try_from(child_data.len()).unwrap());
        data.extend_from_slice(&child_data);
        accounts.extend(child_accounts);
    }
    outer(order, accounts, data)
}

#[test]
fn decodes_all_classic_tags_and_all_account_roles() {
    for tag in (0u8..=24).chain([38, 45, 255]) {
        let data = valid_data(tag);
        let account_count = minimum_account_count(tag) + 1;
        let accounts = (0..account_count)
            .map(|index| key(u8::try_from(index + 1).unwrap()))
            .collect::<Vec<_>>();
        let decoded = decode_classic_token_instruction(&accounts, &data).unwrap();
        assert_eq!(decoded.instruction.tag(), tag, "tag {tag}");
        assert_eq!(decoded.roles.len(), accounts.len(), "tag {tag}");
        assert_eq!(
            decoded
                .roles
                .iter()
                .map(|binding| binding.role)
                .collect::<Vec<_>>(),
            expected_roles(tag, account_count),
            "tag {tag}"
        );
        for (index, role) in decoded.roles.iter().enumerate() {
            assert_eq!(role.account_index, index as u32, "tag {tag}");
            assert_eq!(role.address, accounts[index], "tag {tag}");
        }
    }
}

#[test]
fn decodes_exact_fields_and_preserves_accepted_trailing_data() {
    let amount = u64::MAX;
    let decoded = decode_classic_token_instruction(
        &[key(1), TARGET_MINT, key(2), key(3)],
        &[checked_data(12, amount, 6), vec![8, 9]].concat(),
    )
    .unwrap();
    assert_eq!(decoded.instruction.amount(), Some(amount));
    assert_eq!(decoded.instruction.decimals(), Some(6));
    assert_eq!(decoded.trailing_data, [8, 9]);
    assert_eq!(decoded.roles[0].role, TokenAccountRole::Source);
    assert_eq!(decoded.roles[1].role, TokenAccountRole::Mint);
    assert_eq!(decoded.roles[2].role, TokenAccountRole::Destination);

    let mut mint_data = vec![0, 6];
    mint_data.extend_from_slice(&key(4));
    mint_data.push(1);
    mint_data.extend_from_slice(&key(5));
    let mint = decode_classic_token_instruction(&[TARGET_MINT, key(6)], &mint_data).unwrap();
    assert_eq!(mint.instruction.decimals(), Some(6));
    assert!(matches!(
        mint.instruction,
        ClassicTokenInstruction::InitializeMint {
            mint_authority,
            freeze_authority: Some(freeze_authority),
            ..
        } if mint_authority == key(4) && freeze_authority == key(5)
    ));

    let ui = decode_classic_token_instruction(&[TARGET_MINT], b"\x1812.500001").unwrap();
    assert!(matches!(
        ui.instruction,
        ClassicTokenInstruction::UiAmountToAmount { ref ui_amount }
            if ui_amount == "12.500001"
    ));
}

#[test]
fn decodes_lamport_instructions_with_exact_roles_and_options() {
    let accounts = [key(1), key(2), key(3), key(4)];
    let withdraw = decode_classic_token_instruction(&accounts, &[38]).unwrap();
    assert_eq!(
        withdraw.instruction,
        ClassicTokenInstruction::WithdrawExcessLamports
    );
    assert_eq!(withdraw.roles[0].role, TokenAccountRole::Source);
    assert_eq!(withdraw.roles[1].role, TokenAccountRole::LamportDestination);
    assert_eq!(withdraw.roles[2].role, TokenAccountRole::Authority);
    assert_eq!(withdraw.roles[3].role, TokenAccountRole::MultisigSigner);

    let none = decode_classic_token_instruction(&accounts[..3], &[45, 0]).unwrap();
    assert_eq!(none.instruction.unwrap_lamport_amount(), Some(None));

    let mut some_data = vec![45, 1];
    some_data.extend_from_slice(&u64::MAX.to_le_bytes());
    let some = decode_classic_token_instruction(&accounts[..3], &some_data).unwrap();
    assert_eq!(
        some.instruction.unwrap_lamport_amount(),
        Some(Some(u64::MAX))
    );
    assert_eq!(
        decode_classic_token_instruction(&accounts[..3], &[45, 2]),
        Err(ClassicTokenDecodeError::InvalidOptionalU64Tag { tag: 45, value: 2 })
    );
}

#[test]
fn matches_spl_token_interface_3_0_golden_wire_vectors() {
    // These bytes come from the pack and batch builders in the pinned
    // spl-token-interface 3.0.0 source.
    assert_eq!(
        decode_classic_token_instruction(&[key(1), key(2), key(3)], &[38])
            .unwrap()
            .instruction,
        ClassicTokenInstruction::WithdrawExcessLamports
    );
    assert_eq!(
        decode_classic_token_instruction(&[key(1), key(2), key(3)], &[45, 0])
            .unwrap()
            .instruction,
        ClassicTokenInstruction::UnwrapLamports { amount: None }
    );

    let amount = 500u64;
    let mut unwrap_some = vec![45, 1];
    unwrap_some.extend_from_slice(&amount.to_le_bytes());
    assert_eq!(
        decode_classic_token_instruction(&[key(1), key(2), key(3)], &unwrap_some)
            .unwrap()
            .instruction,
        ClassicTokenInstruction::UnwrapLamports {
            amount: Some(amount),
        }
    );

    let batch = decode_classic_token_batch(&[key(1), key(2), key(3)], &[255, 3, 1, 38]).unwrap();
    assert_eq!(batch.children.len(), 1);
    assert_eq!(batch.children[0].accounts, [key(1), key(2), key(3)]);
    assert_eq!(batch.children[0].data, [38]);
    assert_eq!(batch.terminal_error, None);
}

#[test]
fn sync_native_binds_the_rent_sysvar_and_additional_accounts() {
    let account = key(5);
    let rent_sysvar = key(6);
    let additional = key(7);
    let account_only = decode_classic_token_instruction(&[account], &[17]).unwrap();
    assert_eq!(account_only.roles.len(), 1);
    assert_eq!(account_only.roles[0].role, TokenAccountRole::TokenAccount);

    let decoded =
        decode_classic_token_instruction(&[account, rent_sysvar, additional], &[17]).unwrap();

    assert_eq!(decoded.roles[0].role, TokenAccountRole::TokenAccount);
    assert_eq!(decoded.roles[1].role, TokenAccountRole::RentSysvar);
    assert_eq!(decoded.roles[2].role, TokenAccountRole::Additional);
}

#[test]
fn public_decoders_reject_source_geometry_over_the_limit() {
    let oversized_data = vec![0; MAX_TOKEN_INSTRUCTION_DATA_BYTES + 1];
    assert_eq!(
        decode_classic_token_instruction(&[], &oversized_data),
        Err(ClassicTokenDecodeError::InstructionDataLimit {
            limit: MAX_TOKEN_INSTRUCTION_DATA_BYTES,
            actual: oversized_data.len(),
        })
    );
    assert_eq!(
        decode_classic_token_batch(&[], &oversized_data),
        Err(ClassicTokenDecodeError::InstructionDataLimit {
            limit: MAX_TOKEN_INSTRUCTION_DATA_BYTES,
            actual: oversized_data.len(),
        })
    );

    let oversized_accounts = vec![key(8); MAX_TOKEN_INSTRUCTION_ACCOUNTS + 1];
    assert_eq!(
        decode_classic_token_instruction(&oversized_accounts, &[17]),
        Err(ClassicTokenDecodeError::InstructionAccountLimit {
            limit: MAX_TOKEN_INSTRUCTION_ACCOUNTS,
            actual: oversized_accounts.len(),
        })
    );
    assert_eq!(
        decode_classic_token_batch(&oversized_accounts, &[255]),
        Err(ClassicTokenDecodeError::InstructionAccountLimit {
            limit: MAX_TOKEN_INSTRUCTION_ACCOUNTS,
            actual: oversized_accounts.len(),
        })
    );
}

#[test]
fn allocation_free_validation_matches_known_structural_decode_errors() {
    let mut invalid_optional_pubkey = vec![0, 6];
    invalid_optional_pubkey.extend_from_slice(&key(9));
    invalid_optional_pubkey.push(2);
    let cases = vec![
        (
            vec![key(1), key(2)],
            invalid_optional_pubkey,
            ClassicTokenDecodeError::InvalidOptionalPubkeyTag { tag: 0, value: 2 },
        ),
        (
            vec![key(1), key(2)],
            vec![6, 4, 0],
            ClassicTokenDecodeError::InvalidAuthorityType { value: 4 },
        ),
        (
            vec![key(1)],
            vec![24, 0xff],
            ClassicTokenDecodeError::InvalidUiAmountUtf8,
        ),
        (
            vec![key(1), key(2)],
            amount_data(3, 1),
            ClassicTokenDecodeError::InsufficientAccounts {
                tag: 3,
                needed: 3,
                actual: 2,
            },
        ),
    ];

    for (accounts, data, expected) in cases {
        assert_eq!(
            validate_classic_token_instruction_structure(&accounts, &data),
            Err(expected.clone())
        );
        assert_eq!(
            decode_classic_token_instruction(&accounts, &data),
            Err(expected)
        );
    }
}

#[test]
fn validates_batch_geometry_before_returning_children() {
    let child_accounts = vec![key(1), key(2), key(3)];
    let instruction = batch_outer(0, vec![(child_accounts.clone(), amount_data(3, u64::MAX))]);
    let batch = decode_classic_token_batch(&instruction.accounts, &instruction.data).unwrap();
    assert_eq!(batch.children.len(), 1);
    assert_eq!(batch.children[0].batch_index, 0);
    assert_eq!(batch.children[0].accounts, child_accounts);
    assert_eq!(batch.children[0].data, amount_data(3, u64::MAX));
    assert_eq!(batch.terminal_error, None);
    assert_eq!(batch.consumed_account_count, 3);

    assert_eq!(
        decode_classic_token_batch(&[], &[17]),
        Err(ClassicTokenDecodeError::NotBatch)
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255, 0])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::TruncatedBatchHeader { batch_index: 0 })
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255, 0, 0])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::EmptyBatchChildData { batch_index: 0 })
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255, 0, 2, 17])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::BatchDataOverrun {
            batch_index: 0,
            declared: 2,
            available: 1,
        })
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255, 1, 1, 17])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::BatchAccountOverrun {
            batch_index: 0,
            declared: 1,
            available: 0,
        })
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::EmptyBatch)
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255, 0, 1, 255])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::NestedBatch { batch_index: 0 })
    );
    assert_eq!(
        decode_classic_token_batch(&[], &[255, 1, 1, 255])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::BatchAccountOverrun {
            batch_index: 0,
            declared: 1,
            available: 0,
        })
    );
    assert_eq!(
        decode_classic_token_batch(&[key(1)], &[255, 1, 1, 255])
            .unwrap()
            .terminal_error,
        Some(ClassicTokenDecodeError::NestedBatch { batch_index: 0 })
    );

    let trailing = decode_classic_token_batch(&[key(1)], &[255, 0, 1, 17]).unwrap();
    assert_eq!(trailing.children.len(), 1);
    assert!(trailing.children[0].accounts.is_empty());
    assert_eq!(trailing.consumed_account_count, 0);
    assert_eq!(trailing.terminal_error, None);

    let mut many = vec![255];
    for _ in 0..300 {
        many.extend_from_slice(&[0, 1, 17]);
    }
    let many = decode_classic_token_batch(&[], &many).unwrap();
    assert_eq!(many.children.len(), 300);
    assert_eq!(many.terminal_error, None);

    let mut prefix_then_error = instruction.data.clone();
    prefix_then_error.push(0);
    let prefix = decode_classic_token_batch(&instruction.accounts, &prefix_then_error).unwrap();
    assert_eq!(prefix.children.len(), 1);
    assert_eq!(prefix.children[0].batch_index, 0);
    assert_eq!(
        prefix.terminal_error,
        Some(ClassicTokenDecodeError::TruncatedBatchHeader { batch_index: 1 })
    );
}

#[test]
fn initialize_then_unchecked_transfer_propagates_target_state() {
    let source = key(1);
    let destination = key(2);
    let instructions = vec![
        init_account(0, source, TARGET_MINT),
        unchecked_transfer(1, source, destination, 55),
    ];
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &instructions);

    assert_eq!(result.execution_status, ExecutionStatus::Succeeded);
    assert!(result.coverage_issues.is_empty());
    assert!(tracker.is_active_target(&source));
    assert!(tracker.is_active_target(&destination));
    let transfers = result.transfers().collect::<Vec<_>>();
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].1.amount, 55);
    assert!(!transfers[0].1.checked);
}

#[test]
fn batch_children_apply_in_order_and_keep_the_parent_coordinate() {
    let source = key(5);
    let destination = key(6);
    let instruction = batch_outer(
        0,
        vec![
            (vec![source, TARGET_MINT, key(7), key(8)], vec![1]),
            (vec![source, destination, key(9)], amount_data(3, 55)),
        ],
    );
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &[instruction]);

    assert!(result.coverage_issues.is_empty());
    assert_eq!(result.events.len(), 2);
    assert_eq!(result.events[0].batch_index, Some(0));
    assert_eq!(result.events[1].batch_index, Some(1));
    assert_eq!(result.events[0].coordinate, result.events[1].coordinate);
    let (event, transfer) = result.transfers().next().unwrap();
    assert_eq!(event.batch_index, Some(1));
    assert_eq!(transfer.amount, 55);
    assert_eq!(transfer.legs[0].generation, 1);
    assert_eq!(transfer.legs[1].generation, 1);
    assert!(tracker.is_active_target(&source));
    assert!(tracker.is_active_target(&destination));
}

#[test]
fn batch_keeps_valid_prefix_children_before_a_terminal_error() {
    let account = key(9);
    let mut instruction = batch_outer(0, vec![(vec![account, key(10), key(11)], vec![38])]);
    instruction.data.push(0);

    let mut unknown_tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let unknown = process(
        &mut unknown_tracker,
        ExecutionStatus::Unknown(crate::CoverageReason::MetadataAbsent),
        std::slice::from_ref(&instruction),
    );
    assert!(unknown.events.iter().any(|event| {
        event.batch_index == Some(0)
            && event.commit == TokenCommitState::Unknown
            && event.invocation == TokenInvocationEvidence::Unknown
            && event.effects.is_empty()
    }));
    assert!(unknown.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::TruncatedBatchHeader {
            batch_index: 1
        })
    )));
    assert_eq!(unknown.history_after, HistoryCoverage::Partial);

    let mut failed_tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let failed = process_with_failed_outer(
        &mut failed_tracker,
        ExecutionStatus::Failed,
        Some(0),
        &[instruction],
    );
    let prefix_child = failed
        .events
        .iter()
        .find(|event| event.batch_index == Some(0))
        .unwrap();
    assert_eq!(prefix_child.commit, TokenCommitState::NotCommitted);
    assert_eq!(prefix_child.invocation, TokenInvocationEvidence::Unknown);
    assert!(prefix_child.effects.is_empty());
    assert_eq!(failed_tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn completed_batch_terminal_errors_are_atomic_tracker_conflicts() {
    let account = key(12);
    let mut malformed_batch = batch_outer(
        1,
        vec![(vec![account, TARGET_MINT, key(13), key(14)], vec![1])],
    );
    malformed_batch.data.push(0);
    let instructions = vec![
        init_account(0, account, TARGET_MINT),
        malformed_batch,
        non_token_outer(2),
    ];

    for (status, failed_outer_instruction_index) in [
        (ExecutionStatus::Succeeded, None),
        (ExecutionStatus::Failed, Some(2)),
    ] {
        let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
        let before = tracker.snapshot();
        let work_before = tracker.last_transaction_work();
        let error = try_process_with_failed_outer(
            &mut tracker,
            status,
            failed_outer_instruction_index,
            &instructions,
        )
        .unwrap_err();

        assert_eq!(
            error,
            TargetMintTrackerError::CompletedBatchHasTerminalError {
                coordinate: instructions[1].coordinate,
                error: ClassicTokenDecodeError::TruncatedBatchHeader { batch_index: 1 },
            }
        );
        assert_eq!(tracker.snapshot(), before);
        assert_eq!(tracker.last_transaction_work(), work_before);
        assert!(!tracker.is_active_target(&account));
    }

    let later_instructions = vec![non_token_outer(0), instructions[1].clone()];
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let later = process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Failed,
        Some(0),
        &later_instructions,
    );
    let prefix_child = later
        .events
        .iter()
        .find(|event| event.batch_index == Some(0))
        .unwrap();
    assert_eq!(prefix_child.commit, TokenCommitState::NotCommitted);
    assert_eq!(prefix_child.invocation, TokenInvocationEvidence::NotInvoked);
    assert!(prefix_child.effects.is_empty());
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn completed_batch_known_child_errors_are_atomic_tracker_conflicts() {
    let source = key(16);
    let destination = key(17);
    let batch = batch_outer(
        0,
        vec![
            (vec![source, TARGET_MINT, key(18), key(19)], vec![1]),
            (vec![source, destination, key(20)], vec![3, 1]),
            (vec![source, destination, key(21)], amount_data(3, 9)),
        ],
    );
    let instructions = vec![batch, non_token_outer(1)];

    for (status, failed_outer_instruction_index) in [
        (ExecutionStatus::Succeeded, None),
        (ExecutionStatus::Failed, Some(1)),
    ] {
        let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
        let before = tracker.snapshot();
        let error = try_process_with_failed_outer(
            &mut tracker,
            status,
            failed_outer_instruction_index,
            &instructions,
        )
        .unwrap_err();

        assert_eq!(
            error,
            TargetMintTrackerError::CompletedBatchChildHasStructuralError {
                coordinate: instructions[0].coordinate,
                batch_index: 1,
                error: ClassicTokenDecodeError::TruncatedData {
                    tag: 3,
                    needed: 9,
                    actual: 2,
                },
            }
        );
        assert_eq!(tracker.snapshot(), before);
        assert_eq!(tracker.last_transaction_work(), TokenTrackerWork::default());
        assert!(!tracker.is_active_target(&source));
        assert!(!tracker.is_active_target(&destination));
    }

    let mut boundary_tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [source]);
    let before = boundary_tracker.snapshot();
    let boundary = process_with_failed_outer(
        &mut boundary_tracker,
        ExecutionStatus::Failed,
        Some(0),
        std::slice::from_ref(&instructions[0]),
    );
    assert!(boundary.events.iter().any(|event| {
        event.batch_index == Some(1)
            && event.commit == TokenCommitState::NotCommitted
            && event.invocation == TokenInvocationEvidence::Unknown
            && matches!(event.raw, ObservedTokenInstruction::Unknown(_))
    }));
    assert_eq!(boundary_tracker.snapshot(), before);
}

#[test]
fn completed_batch_unknown_child_tag_remains_forward_compatible_coverage() {
    let source = key(22);
    let destination = key(23);
    let batch = batch_outer(
        0,
        vec![
            (vec![source, TARGET_MINT, key(24), key(25)], vec![1]),
            (vec![source], vec![250]),
            (vec![source, destination, key(26)], amount_data(3, 11)),
        ],
    );
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &[batch]);

    assert!(result.events.iter().any(|event| {
        event.batch_index == Some(1) && matches!(event.raw, ObservedTokenInstruction::Unknown(_))
    }));
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::UnknownTag { tag: 250 })
    )));
    assert_eq!(result.history_after, HistoryCoverage::Partial);
    assert_eq!(result.transfers().count(), 0);
}

#[test]
fn aggregate_batch_expansion_limit_is_checked_before_expansion() {
    const CHILDREN_PER_BATCH: usize = 256;
    const BATCH_COUNT: usize = 256;

    let mut batch_data = vec![255];
    for _ in 0..CHILDREN_PER_BATCH {
        batch_data.extend_from_slice(&[0, 1, 21]);
    }
    let instructions = (0..BATCH_COUNT)
        .map(|order| outer(order as u32, Vec::new(), batch_data.clone()))
        .collect::<Vec<_>>();
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let before = tracker.snapshot();
    let error =
        try_process_with_failed_outer(&mut tracker, ExecutionStatus::Failed, None, &instructions)
            .unwrap_err();

    assert_eq!(
        error,
        TargetMintTrackerError::ExpandedTokenLeafLimit {
            limit: MAX_EXPANDED_TOKEN_LEAVES,
            actual: CHILDREN_PER_BATCH * BATCH_COUNT,
        }
    );
    assert_eq!(tracker.snapshot(), before);
    assert_eq!(tracker.last_transaction_work(), TokenTrackerWork::default());
}

#[test]
fn tracker_preflights_direct_instruction_geometry_atomically() {
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let before = tracker.snapshot();
    let oversized_data = vec![0; MAX_TOKEN_INSTRUCTION_DATA_BYTES + 1];
    let data_instruction = outer(0, Vec::new(), oversized_data);
    let error = try_process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Failed,
        None,
        std::slice::from_ref(&data_instruction),
    )
    .unwrap_err();
    assert_eq!(
        error,
        TargetMintTrackerError::InstructionGeometry {
            coordinate: data_instruction.coordinate,
            error: ClassicTokenDecodeError::InstructionDataLimit {
                limit: MAX_TOKEN_INSTRUCTION_DATA_BYTES,
                actual: MAX_TOKEN_INSTRUCTION_DATA_BYTES + 1,
            },
        }
    );
    assert_eq!(tracker.snapshot(), before);

    let oversized_accounts = vec![key(15); MAX_TOKEN_INSTRUCTION_ACCOUNTS + 1];
    let account_instruction = outer(0, oversized_accounts, vec![17]);
    let error = try_process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Failed,
        None,
        std::slice::from_ref(&account_instruction),
    )
    .unwrap_err();
    assert_eq!(
        error,
        TargetMintTrackerError::InstructionGeometry {
            coordinate: account_instruction.coordinate,
            error: ClassicTokenDecodeError::InstructionAccountLimit {
                limit: MAX_TOKEN_INSTRUCTION_ACCOUNTS,
                actual: MAX_TOKEN_INSTRUCTION_ACCOUNTS + 1,
            },
        }
    );
    assert_eq!(tracker.snapshot(), before);
}

#[test]
fn tracker_rejects_too_many_direct_instructions_atomically() {
    let instructions = (0..=MAX_CANONICAL_SHORT_VEC_ITEMS)
        .map(|order| ResolvedInstruction {
            coordinate: InstructionCoordinate {
                order: order as u32,
                outer_index: order as u32,
                inner_index: None,
                stack_height: None,
            },
            program_id: key(201),
            accounts: Vec::new(),
            data_coverage: InstructionDataCoverage::Exact,
            data: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let before = tracker.snapshot();
    let error =
        try_process_with_failed_outer(&mut tracker, ExecutionStatus::Failed, None, &instructions)
            .unwrap_err();

    assert_eq!(
        error,
        TargetMintTrackerError::TransactionInstructionLimit {
            limit: MAX_CANONICAL_SHORT_VEC_ITEMS,
            actual: MAX_CANONICAL_SHORT_VEC_ITEMS + 1,
        }
    );
    assert_eq!(tracker.snapshot(), before);
    assert_eq!(tracker.last_transaction_work(), TokenTrackerWork::default());
}

#[test]
fn tracker_bounds_aggregate_token_input_before_lookahead() {
    const FULL_INSTRUCTIONS: usize = 256;
    const FINAL_DATA_BYTES: usize = 257;

    let mut instructions = Vec::with_capacity(FULL_INSTRUCTIONS + 1);
    for order in 0..FULL_INSTRUCTIONS {
        instructions.push(outer(
            order as u32,
            Vec::new(),
            vec![21; MAX_TOKEN_INSTRUCTION_DATA_BYTES],
        ));
    }
    instructions.push(outer(
        FULL_INSTRUCTIONS as u32,
        Vec::new(),
        vec![21; FINAL_DATA_BYTES],
    ));
    let expected_bytes = FULL_INSTRUCTIONS * MAX_TOKEN_INSTRUCTION_DATA_BYTES + FINAL_DATA_BYTES;
    assert_eq!(expected_bytes, MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION + 1);

    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let before = tracker.snapshot();
    let error =
        try_process_with_failed_outer(&mut tracker, ExecutionStatus::Failed, None, &instructions)
            .unwrap_err();
    assert_eq!(
        error,
        TargetMintTrackerError::TokenInputByteLimit {
            limit: MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
            actual: expected_bytes,
        }
    );
    assert_eq!(tracker.snapshot(), before);
    assert_eq!(tracker.last_transaction_work(), TokenTrackerWork::default());
}

#[test]
fn invalid_target_batch_is_raw_and_unrelated_batch_is_ignored() {
    let nested = outer(0, vec![TARGET_MINT], vec![255, 1, 1, 255]);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result =
        process_with_failed_outer(&mut tracker, ExecutionStatus::Failed, Some(0), &[nested]);
    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].batch_index, None);
    assert!(matches!(
        result.events[0].raw,
        ObservedTokenInstruction::Unknown(_)
    ));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);
    assert!(matches!(
        result.coverage_issues.as_slice(),
        [TokenCoverageIssue {
            kind: TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::NestedBatch {
                batch_index: 0
            }),
            ..
        }]
    ));

    let unrelated = batch_outer(
        0,
        vec![
            (vec![key(10), OTHER_MINT, key(11), key(12)], vec![1]),
            (vec![key(10), key(13)], vec![17]),
        ],
    );
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &[unrelated]);
    assert!(result.events.is_empty());
    assert!(result.coverage_issues.is_empty());
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);

    let unrelated_invalid = outer(0, vec![key(12)], vec![255, 1, 1, 255]);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let before = tracker.snapshot();
    let error = try_process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Succeeded,
        None,
        &[unrelated_invalid],
    )
    .unwrap_err();
    assert!(matches!(
        error,
        TargetMintTrackerError::CompletedBatchHasTerminalError {
            error: ClassicTokenDecodeError::NestedBatch { batch_index: 0 },
            ..
        }
    ));
    assert_eq!(tracker.snapshot(), before);
}

#[test]
fn unknown_target_batch_child_keeps_its_child_index_and_raw_data() {
    let account = key(13);
    let instruction = batch_outer(
        0,
        vec![
            (vec![account, TARGET_MINT, key(14), key(15)], vec![1]),
            (vec![account], vec![250, 7, 8]),
        ],
    );
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &[instruction]);

    assert_eq!(result.events.len(), 2);
    assert_eq!(result.events[1].batch_index, Some(1));
    assert!(matches!(
        &result.events[1].raw,
        ObservedTokenInstruction::Unknown(raw) if raw.data == [250, 7, 8]
    ));
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::UnknownTag { tag: 250 })
    )));
}

#[test]
fn lamport_instructions_are_target_events_without_token_delta_effects() {
    let account = key(16);
    let mut unwrap_all = vec![45, 0];
    // Keep an accepted trailing byte to confirm the interface decoder rule.
    unwrap_all.push(9);
    let instructions = vec![
        outer(0, vec![account, key(17), key(18)], vec![38]),
        outer(1, vec![account, key(19), key(20)], unwrap_all),
    ];
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &instructions);

    assert_eq!(result.events.len(), 2);
    assert!(
        result.events.iter().all(|event| {
            event.commit == TokenCommitState::Committed && event.effects.is_empty()
        })
    );
    assert_eq!(
        result.events[0].raw.classic().unwrap().instruction.tag(),
        38
    );
    assert_eq!(
        result.events[1].raw.classic().unwrap().instruction.tag(),
        45
    );
}

#[test]
fn checked_transfer_discovers_both_accounts_from_a_sparse_start() {
    let source = key(10);
    let destination = key(11);
    let instructions = vec![outer(
        0,
        vec![source, TARGET_MINT, destination, key(12)],
        checked_data(12, 900, 6),
    )];
    let mut tracker = TargetMintTracker::from_sparse_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &instructions);

    assert!(tracker.is_active_target(&source));
    assert!(tracker.is_active_target(&destination));
    let transfer = result.transfers().next().unwrap().1;
    assert_eq!(transfer.amount, 900);
    assert_eq!(transfer.decimals, Some(6));
    assert!(transfer.checked);
}

#[test]
fn close_then_reuse_for_another_mint_stays_out_of_target_state() {
    let account = key(20);
    let destination = key(21);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, account, TARGET_MINT)],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![account, key(22), key(23)], vec![9])],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![account, OTHER_MINT], owner_data(18, key(24)))],
    );

    assert_eq!(
        tracker.lifecycle(&account),
        Some(TokenAccountLifecycle {
            generation: 2,
            state: TokenAccountState::ActiveOther { mint: OTHER_MINT },
        })
    );
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, account, destination, 12)],
    );
    assert_eq!(result.transfers().count(), 0);
    assert!(!tracker.is_active_target(&account));
    assert!(!tracker.is_active_target(&destination));
}

#[test]
fn snapshot_restores_closed_and_reused_lifetimes() {
    let account = key(25);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, account, TARGET_MINT)],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![account, key(26), key(27)], vec![9])],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![account, OTHER_MINT], owner_data(18, key(28)))],
    );

    let snapshot = tracker.snapshot();
    assert_eq!(
        snapshot
            .accounts()
            .get(&account)
            .map(|state| state.lifecycle),
        Some(TokenAccountLifecycle {
            generation: 2,
            state: TokenAccountState::ActiveOther { mint: OTHER_MINT },
        })
    );
    assert!(
        !snapshot
            .confirmed_active_accounts()
            .any(|found| found == account)
    );
    let rebuilt_snapshot = TargetMintTrackerSnapshot::from_parts(
        snapshot.target_mint(),
        snapshot.history_coverage(),
        snapshot.certainty_revision(),
        snapshot
            .accounts()
            .iter()
            .map(|(address, state)| (*address, *state)),
    )
    .unwrap();
    let mut restored = TargetMintTracker::from_snapshot(rebuilt_snapshot);
    assert_eq!(restored.snapshot(), snapshot);

    let mut replaced = TargetMintTracker::from_sparse_start(key(200));
    replaced.restore(snapshot.clone());
    assert_eq!(replaced.snapshot(), snapshot);

    let continuation = [outer(
        0,
        vec![account, TARGET_MINT, key(201), key(202)],
        checked_data(12, 7, 6),
    )];
    let restored_result = process(&mut restored, ExecutionStatus::Succeeded, &continuation);
    let replaced_result = process(&mut replaced, ExecutionStatus::Succeeded, &continuation);
    assert_eq!(restored_result, replaced_result);
    assert_eq!(restored.snapshot(), replaced.snapshot());
}

#[test]
fn snapshot_parts_reject_invalid_lifecycle_records() {
    let invalid = TargetAccountSnapshot {
        lifecycle: TokenAccountLifecycle {
            generation: 0,
            state: TokenAccountState::ActiveTarget,
        },
        confirmed_revision: 1,
    };
    assert_eq!(
        TargetMintTrackerSnapshot::from_parts(
            TARGET_MINT,
            HistoryCoverage::Complete,
            1,
            [(key(1), invalid)],
        ),
        Err(TargetMintSnapshotError::ZeroGeneration { account: key(1) })
    );
}

#[test]
fn exhausted_tracker_counters_return_typed_errors_without_state_change() {
    let revision_snapshot =
        TargetMintTrackerSnapshot::from_parts(TARGET_MINT, HistoryCoverage::Partial, u64::MAX, [])
            .unwrap();
    let mut revision_tracker = TargetMintTracker::from_snapshot(revision_snapshot.clone());
    let revision_error = try_process_with_failed_outer(
        &mut revision_tracker,
        ExecutionStatus::Succeeded,
        None,
        &[outer(0, vec![key(203)], vec![250])],
    )
    .unwrap_err();
    assert_eq!(
        revision_error,
        TargetMintTrackerError::CertaintyRevisionExhausted
    );
    assert_eq!(revision_tracker.snapshot(), revision_snapshot);

    let account = key(204);
    let generation_snapshot = TargetMintTrackerSnapshot::from_parts(
        TARGET_MINT,
        HistoryCoverage::Complete,
        1,
        [(
            account,
            TargetAccountSnapshot {
                lifecycle: TokenAccountLifecycle {
                    generation: u64::MAX,
                    state: TokenAccountState::ActiveTarget,
                },
                confirmed_revision: 1,
            },
        )],
    )
    .unwrap();
    let mut generation_tracker = TargetMintTracker::from_snapshot(generation_snapshot.clone());
    let generation_error = try_process_with_failed_outer(
        &mut generation_tracker,
        ExecutionStatus::Succeeded,
        None,
        &[init_account(0, account, TARGET_MINT)],
    )
    .unwrap_err();
    assert_eq!(
        generation_error,
        TargetMintTrackerError::LifecycleGenerationExhausted { account }
    );
    assert_eq!(generation_tracker.snapshot(), generation_snapshot);
}

#[test]
fn transfer_and_amount_effects_keep_lifecycle_generations() {
    let source = key(29);
    let destination = key(30);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, source, TARGET_MINT)],
    );
    assert!(tracker.is_active_target(&source));
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![source, key(31), key(32)], vec![9])],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, source, TARGET_MINT)],
    );
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[
            outer(
                0,
                vec![TARGET_MINT, destination, key(33)],
                amount_data(7, 5),
            ),
            unchecked_transfer(1, source, destination, 2),
        ],
    );

    let mint_generation = result
        .events
        .iter()
        .flat_map(|event| &event.effects)
        .find_map(|effect| match effect {
            TargetMintEffect::Mint { generation, .. } => Some(*generation),
            _ => None,
        });
    assert_eq!(mint_generation, Some(1));
    let transfer = result.transfers().next().unwrap().1;
    assert_eq!(transfer.legs[0].generation, 2);
    assert_eq!(transfer.legs[1].generation, 1);
}

#[test]
fn immutable_owner_uses_a_later_target_initialization_as_bounded_lookahead() {
    let account = key(34);
    let instructions = vec![
        outer(0, vec![account], vec![22]),
        outer(1, vec![account, TARGET_MINT], owner_data(18, key(35))),
    ];
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &instructions);

    assert_eq!(result.events.len(), 2);
    assert_eq!(
        result.events[0].raw.classic().unwrap().instruction,
        ClassicTokenInstruction::InitializeImmutableOwner
    );
    assert_eq!(result.events[0].coordinate.order, 0);
    assert!(tracker.is_active_target(&account));
}

#[test]
fn failed_transaction_records_attempts_but_rolls_back_all_state() {
    let source = key(30);
    let destination = key(31);
    let instructions = vec![
        init_account(0, source, TARGET_MINT),
        unchecked_transfer(1, source, destination, 77),
    ];
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Failed, &instructions);

    assert_eq!(result.execution_status, ExecutionStatus::Failed);
    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].coordinate.order, 0);
    assert_eq!(
        result.events[0].raw.classic().unwrap().instruction,
        ClassicTokenInstruction::InitializeAccount
    );
    assert!(result.events.iter().all(|event| {
        event.commit == TokenCommitState::NotCommitted
            && event.invocation == TokenInvocationEvidence::Unknown
            && event.effects.is_empty()
    }));
    assert_eq!(result.transfers().count(), 0);
    assert!(result.account_updates.is_empty());
    assert!(!tracker.is_active_target(&source));
    assert!(!tracker.is_active_target(&destination));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn unknown_execution_is_not_labeled_as_a_rollback_or_commit() {
    let source = key(36);
    let destination = key(37);
    let instructions = vec![
        init_account(0, source, TARGET_MINT),
        unchecked_transfer(1, source, destination, 9),
    ];
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Unknown(crate::CoverageReason::MetadataAbsent),
        &instructions,
    );

    assert_eq!(
        result.execution_status,
        ExecutionStatus::Unknown(crate::CoverageReason::MetadataAbsent)
    );
    assert!(result.events.iter().all(|event| {
        event.commit == TokenCommitState::Unknown
            && event.invocation == TokenInvocationEvidence::Unknown
            && event.effects.is_empty()
    }));
    assert!(result.account_updates.is_empty());
    assert_eq!(result.transfers().count(), 0);
    assert!(!tracker.is_active_target(&source));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Partial);
    assert_eq!(
        result.certainty_revision_after,
        tracker.snapshot().certainty_revision()
    );
}

#[test]
fn failed_cpi_has_invocation_and_rollback_evidence() {
    let account = key(38);
    let instruction = inner(
        0,
        0,
        0,
        2,
        vec![account, TARGET_MINT, key(39), key(40)],
        vec![1],
    );
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Failed, &[instruction]);

    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].commit, TokenCommitState::RolledBack);
    assert_eq!(
        result.events[0].invocation,
        TokenInvocationEvidence::Invoked
    );
    assert!(result.events[0].effects.is_empty());
    assert!(result.account_updates.is_empty());
}

#[test]
fn known_failure_boundary_classifies_normal_outer_and_inner_events() {
    let account = key(41);
    let outer_instructions = vec![
        outer(0, vec![account, key(42), key(43)], vec![38]),
        outer(1, vec![account, key(44), key(45)], vec![38]),
        outer(2, vec![account, key(46), key(47)], vec![38]),
    ];
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let result = process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Failed,
        Some(1),
        &outer_instructions,
    );
    assert_eq!(
        result
            .events
            .iter()
            .map(|event| (event.commit, event.invocation))
            .collect::<Vec<_>>(),
        vec![
            (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            (
                TokenCommitState::NotCommitted,
                TokenInvocationEvidence::NotInvoked,
            ),
        ]
    );
    assert!(result.events.iter().all(|event| event.effects.is_empty()));

    let inner_instructions = vec![
        inner(0, 0, 0, 2, vec![account, key(48), key(49)], vec![38]),
        inner(1, 1, 0, 2, vec![account, key(50), key(51)], vec![38]),
        inner(2, 2, 0, 2, vec![account, key(52), key(53)], vec![38]),
    ];
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let result = process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Failed,
        Some(1),
        &inner_instructions,
    );
    assert_eq!(
        result
            .events
            .iter()
            .map(|event| (event.commit, event.invocation))
            .collect::<Vec<_>>(),
        vec![
            (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            (
                TokenCommitState::RolledBack,
                TokenInvocationEvidence::Invoked,
            ),
            (
                TokenCommitState::NotCommitted,
                TokenInvocationEvidence::NotInvoked,
            ),
        ]
    );
}

#[test]
fn known_failure_boundary_keeps_batch_parent_uncertainty() {
    let account = key(54);
    let child = |destination: u8, authority: u8| {
        (vec![account, key(destination), key(authority)], vec![38])
    };
    let instructions = vec![
        batch_outer(0, vec![child(55, 56)]),
        batch_outer(1, vec![child(57, 58), child(59, 60)]),
        batch_outer(2, vec![child(61, 62)]),
    ];
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let result = process_with_failed_outer(
        &mut tracker,
        ExecutionStatus::Failed,
        Some(1),
        &instructions,
    );

    assert_eq!(result.events.len(), 4);
    assert_eq!(result.events[0].batch_index, Some(0));
    assert_eq!(
        (result.events[0].commit, result.events[0].invocation),
        (
            TokenCommitState::RolledBack,
            TokenInvocationEvidence::Invoked,
        )
    );
    assert!(result.events[1..3].iter().all(|event| {
        event.coordinate.outer_index == 1
            && event.commit == TokenCommitState::NotCommitted
            && event.invocation == TokenInvocationEvidence::Unknown
    }));
    assert_eq!(
        (result.events[3].commit, result.events[3].invocation),
        (
            TokenCommitState::NotCommitted,
            TokenInvocationEvidence::NotInvoked,
        )
    );
    assert!(result.events.iter().all(|event| event.effects.is_empty()));
}

#[test]
fn absent_failure_boundary_and_unknown_status_keep_batch_uncertainty() {
    let account = key(63);
    let batch = batch_outer(0, vec![(vec![account, key(64), key(65)], vec![38])]);
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let failed = process(&mut tracker, ExecutionStatus::Failed, &[batch]);
    assert_eq!(failed.events.len(), 1);
    assert_eq!(
        (failed.events[0].commit, failed.events[0].invocation),
        (
            TokenCommitState::NotCommitted,
            TokenInvocationEvidence::Unknown,
        )
    );

    let mut batch_inner = batch_outer(2, vec![(vec![account, key(66), key(67)], vec![38])]);
    batch_inner.coordinate.outer_index = 0;
    batch_inner.coordinate.inner_index = Some(1);
    batch_inner.coordinate.stack_height = Some(2);
    let unknown_instructions = vec![
        outer(0, vec![account, key(68), key(69)], vec![38]),
        inner(1, 0, 0, 2, vec![account, key(70), key(71)], vec![38]),
        batch_inner,
    ];
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let unknown = process(
        &mut tracker,
        ExecutionStatus::Unknown(crate::CoverageReason::MetadataAbsent),
        &unknown_instructions,
    );
    assert_eq!(unknown.events.len(), 3);
    assert_eq!(
        (unknown.events[0].commit, unknown.events[0].invocation),
        (TokenCommitState::Unknown, TokenInvocationEvidence::Unknown,)
    );
    assert_eq!(
        (unknown.events[1].commit, unknown.events[1].invocation),
        (TokenCommitState::Unknown, TokenInvocationEvidence::Invoked,)
    );
    assert_eq!(unknown.events[2].batch_index, Some(0));
    assert_eq!(
        (unknown.events[2].commit, unknown.events[2].invocation),
        (TokenCommitState::Unknown, TokenInvocationEvidence::Unknown,)
    );
}

#[test]
fn cpi_events_keep_outer_inner_stack_and_execution_order() {
    let source = key(40);
    let destination = key(41);
    let instructions = vec![
        non_token_outer(0),
        inner(
            1,
            0,
            0,
            2,
            vec![source, TARGET_MINT, key(42), key(43)],
            vec![1],
        ),
        inner(
            2,
            0,
            1,
            3,
            vec![source, destination, key(44)],
            amount_data(3, 8),
        ),
        outer(3, vec![source, key(45), key(46)], vec![9]),
    ];
    // The helper uses order as the outer index. Correct the final outer index.
    let mut instructions = instructions;
    instructions[3].coordinate.outer_index = 1;
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Succeeded, &instructions);

    let coordinates = result
        .events
        .iter()
        .map(|event| event.coordinate)
        .collect::<Vec<_>>();
    assert_eq!(coordinates[0].order, 1);
    assert_eq!(coordinates[0].inner_index, Some(0));
    assert_eq!(coordinates[0].stack_height, Some(2));
    assert_eq!(coordinates[1].order, 2);
    assert_eq!(coordinates[1].inner_index, Some(1));
    assert_eq!(coordinates[1].stack_height, Some(3));
    assert_eq!(coordinates[2].outer_index, 1);
    assert_eq!(result.transfers().next().unwrap().0.coordinate.order, 2);
}

#[test]
fn self_transfer_has_two_roles_and_net_zero() {
    let account = key(50);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, account, TARGET_MINT)],
    );
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, account, account, 123)],
    );
    let transfer = result.transfers().next().unwrap().1;

    assert_eq!(transfer.legs.len(), 2);
    assert_eq!(transfer.legs[0].role, TransferLegRole::Source);
    assert_eq!(transfer.legs[1].role, TransferLegRole::Destination);
    assert_eq!(transfer.net_change_for(&account), 0);
    assert!(tracker.is_active_target(&account));
}

#[test]
fn maximum_u64_amount_is_exact() {
    let source = key(60);
    let destination = key(61);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(
            0,
            vec![source, TARGET_MINT, destination, key(62)],
            checked_data(12, u64::MAX, 6),
        )],
    );
    let (event, transfer) = result.transfers().next().unwrap();

    assert_eq!(
        event.raw.classic().unwrap().instruction.amount(),
        Some(u64::MAX)
    );
    assert_eq!(transfer.amount, u64::MAX);
    assert_eq!(transfer.net_change_for(&source), -i128::from(u64::MAX));
    assert_eq!(transfer.net_change_for(&destination), i128::from(u64::MAX));
}

#[test]
fn sparse_start_marks_unchecked_unknown_transfer_as_partial() {
    let source = key(70);
    let destination = key(71);
    let mut tracker = TargetMintTracker::from_sparse_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, source, destination, 1)],
    );

    assert_eq!(result.transfers().count(), 0);
    assert!(matches!(
        result.coverage_issues.as_slice(),
        [TokenCoverageIssue {
            kind: TokenCoverageIssueKind::InsufficientHistory { .. },
            ..
        }]
    ));
    assert_eq!(result.history_after, HistoryCoverage::Partial);
}

#[test]
fn partial_history_requires_new_mint_evidence_for_unchecked_transfer() {
    let source = key(72);
    let destination = key(73);
    let next = key(74);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, source, TARGET_MINT)],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![source], vec![250])],
    );
    assert!(!tracker.is_active_target(&source));
    assert_eq!(
        tracker.account_state(&source).unwrap().lifecycle.state,
        TokenAccountState::ActiveTarget
    );

    let uncertain = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, source, destination, 3)],
    );
    assert_eq!(uncertain.transfers().count(), 0);
    assert!(uncertain.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::InsufficientHistory { .. }
    )));

    let explicit = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(
            0,
            vec![source, TARGET_MINT, destination, key(75)],
            checked_data(12, 4, 6),
        )],
    );
    assert_eq!(explicit.transfers().count(), 1);
    assert!(tracker.is_active_target(&source));
    assert!(explicit.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::InsufficientHistory {
            first_account,
            second_account: None,
        } if first_account == source
    )));
    assert_eq!(explicit.account_updates.len(), 2);
    assert!(explicit.account_updates.iter().any(|update| {
        update.account == source
            && update.state.lifecycle.state == TokenAccountState::ActiveTarget
            && update.state.lifecycle.generation == 2
            && update.state.confirmed_revision == explicit.certainty_revision_after
    }));
    let transfer = explicit.transfers().next().unwrap().1;
    assert_eq!(transfer.legs[0].generation, 2);
    assert_eq!(transfer.legs[1].generation, 1);
    let propagated = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, destination, next, 2)],
    );
    assert_eq!(propagated.transfers().count(), 1);
    assert!(tracker.is_active_target(&next));
}

#[test]
fn successful_sync_native_on_a_known_target_is_a_conflict() {
    let account = key(76);
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![account, key(77)], vec![17])],
    );

    assert_eq!(result.events.len(), 1);
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::SyncNativeOnTargetAccount { account: found }
            if found == account
    )));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Partial);

    let mut failed_tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [account]);
    let failed = process(
        &mut failed_tracker,
        ExecutionStatus::Failed,
        &[outer(0, vec![account, key(77)], vec![17])],
    );
    assert!(failed.coverage_issues.is_empty());
    assert_eq!(failed_tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn unknown_tag_and_unavailable_data_are_coverage_issues() {
    let active = key(80);
    let mut tracker = TargetMintTracker::from_active_account_seed(TARGET_MINT, [active]);
    let mut unavailable = unchecked_transfer(1, active, key(81), 2);
    unavailable.data_coverage = InstructionDataCoverage::NotRequested;
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![TARGET_MINT], vec![250]), unavailable],
    );

    assert_eq!(result.events.len(), 2);
    assert!(
        result
            .events
            .iter()
            .all(|event| matches!(event.raw, ObservedTokenInstruction::Unknown(_)))
    );
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::UnknownTag { tag: 250 })
    )));
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::InstructionDataUnavailable(InstructionDataCoverage::NotRequested)
    )));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Partial);
}

#[test]
fn unknown_unrelated_tag_has_no_event_but_invalidates_completeness() {
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![key(83)], vec![250])],
    );

    assert!(result.events.is_empty());
    assert!(matches!(
        result.coverage_issues.as_slice(),
        [TokenCoverageIssue {
            kind: TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::UnknownTag { tag: 250 }),
            ..
        }]
    ));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Partial);
}

#[test]
fn non_exact_or_malformed_unrelated_token_data_fails_closed() {
    let mut unavailable = outer(0, vec![key(81)], Vec::new());
    unavailable.data_coverage = InstructionDataCoverage::NotRequested;
    let malformed = outer(1, vec![key(82), key(83), key(84)], vec![3, 1]);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unavailable, malformed],
    );

    assert!(result.events.is_empty());
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::InstructionDataUnavailable(InstructionDataCoverage::NotRequested)
    )));
    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::Decode(ClassicTokenDecodeError::TruncatedData { tag: 3, .. })
    )));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Partial);
}

#[test]
fn known_unrelated_instruction_is_not_a_target_event() {
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, key(88), OTHER_MINT)],
    );

    assert!(result.events.is_empty());
    assert!(result.coverage_issues.is_empty());
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn additional_account_does_not_select_a_known_instruction_as_target_related() {
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![OTHER_MINT, TARGET_MINT], vec![21])],
    );

    assert!(result.events.is_empty());
    assert!(result.coverage_issues.is_empty());
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn transaction_overlay_work_is_bounded_to_changed_accounts() {
    let seed = (0..4096).map(indexed_key).collect::<Vec<_>>();
    let mut tracker =
        TargetMintTracker::from_active_account_seed(TARGET_MINT, seed.iter().copied());
    assert_eq!(tracker.retained_account_count(), seed.len());

    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, indexed_key(10_000), OTHER_MINT)],
    );
    assert!(result.events.is_empty());
    assert_eq!(tracker.retained_account_count(), seed.len());
    assert_eq!(tracker.last_transaction_work().overlay_accounts, 0);

    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, seed[0], indexed_key(10_001), 1)],
    );
    assert_eq!(tracker.last_transaction_work().overlay_accounts, 2);
}

#[test]
fn non_token_order_issues_fit_the_instruction_inclusive_reservation() {
    const INSTRUCTION_COUNT: usize = 64;
    let instructions = (0..INSTRUCTION_COUNT)
        .map(|order| {
            let mut instruction = non_token_outer(order as u32);
            instruction.coordinate.order += 1;
            instruction
        })
        .collect::<Vec<_>>();
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    let result = process(&mut tracker, ExecutionStatus::Failed, &instructions);

    assert_eq!(result.events.len(), 0);
    assert_eq!(result.coverage_issues.len(), INSTRUCTION_COUNT);
    assert!(result.coverage_issues.iter().all(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::InvalidInstructionOrder { .. }
    )));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Complete);
}

#[test]
fn unchecked_transfer_from_a_closed_account_marks_a_history_gap() {
    let account = key(84);
    let mut tracker = TargetMintTracker::from_complete_start(TARGET_MINT);
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[init_account(0, account, TARGET_MINT)],
    );
    process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[outer(0, vec![account, key(85), key(86)], vec![9])],
    );
    let result = process(
        &mut tracker,
        ExecutionStatus::Succeeded,
        &[unchecked_transfer(0, account, key(87), 4)],
    );

    assert!(result.coverage_issues.iter().any(|issue| matches!(
        issue.kind,
        TokenCoverageIssueKind::InsufficientHistory { .. }
    )));
    assert_eq!(tracker.history_coverage(), HistoryCoverage::Partial);
}

fn valid_data(tag: u8) -> Vec<u8> {
    match tag {
        0 | 20 => {
            let mut data = vec![tag, 6];
            data.extend_from_slice(&key(100));
            data.push(0);
            data
        }
        1 | 5 | 9 | 10 | 11 | 17 | 21 | 22 | 38 | 255 => vec![tag],
        2 | 19 => vec![tag, 1],
        3 | 4 | 7 | 8 | 23 => amount_data(tag, 9),
        6 => vec![tag, 0, 0],
        12..=15 => checked_data(tag, 9, 6),
        16 | 18 => owner_data(tag, key(101)),
        24 => b"\x181.5".to_vec(),
        45 => vec![45, 0],
        _ => unreachable!(),
    }
}

const fn minimum_account_count(tag: u8) -> usize {
    match tag {
        0 => 2,
        1 => 4,
        2 => 2,
        3 | 4 | 7 | 8 | 9 | 10 | 11 | 14 | 15 | 16 | 38 | 45 => 3,
        5 | 6 | 18 => 2,
        12 | 13 => 4,
        19..=24 => 1,
        17 => 1,
        _ => 0,
    }
}

fn expected_roles(tag: u8, count: usize) -> Vec<TokenAccountRole> {
    use TokenAccountRole::{
        Additional, Authority, AuthoritySubject, Delegate, Destination, LamportDestination, Mint,
        MultisigAccount, MultisigSigner, Owner, RentSysvar, Source, TokenAccount,
    };

    let (fixed, trailing) = match tag {
        0 => (&[Mint, RentSysvar][..], Additional),
        1 => (&[TokenAccount, Mint, Owner, RentSysvar][..], Additional),
        2 => (&[MultisigAccount, RentSysvar][..], MultisigSigner),
        3 => (&[Source, Destination, Authority][..], MultisigSigner),
        4 => (&[Source, Delegate, Authority][..], MultisigSigner),
        5 => (&[Source, Authority][..], MultisigSigner),
        6 => (&[AuthoritySubject, Authority][..], MultisigSigner),
        7 => (&[Mint, Destination, Authority][..], MultisigSigner),
        8 => (&[Source, Mint, Authority][..], MultisigSigner),
        9 => (
            &[TokenAccount, LamportDestination, Authority][..],
            MultisigSigner,
        ),
        10 | 11 => (&[TokenAccount, Mint, Authority][..], MultisigSigner),
        12 => (&[Source, Mint, Destination, Authority][..], MultisigSigner),
        13 => (&[Source, Mint, Delegate, Authority][..], MultisigSigner),
        14 => (&[Mint, Destination, Authority][..], MultisigSigner),
        15 => (&[Source, Mint, Authority][..], MultisigSigner),
        16 => (&[TokenAccount, Mint, RentSysvar][..], Additional),
        17 => (&[TokenAccount, RentSysvar][..], Additional),
        18 => (&[TokenAccount, Mint][..], Additional),
        19 => (&[MultisigAccount][..], MultisigSigner),
        20 | 21 | 23 | 24 => (&[Mint][..], Additional),
        22 => (&[TokenAccount][..], Additional),
        38 | 45 => (&[Source, LamportDestination, Authority][..], MultisigSigner),
        255 => (&[][..], Additional),
        _ => unreachable!(),
    };
    (0..count)
        .map(|index| fixed.get(index).copied().unwrap_or(trailing))
        .collect()
}
