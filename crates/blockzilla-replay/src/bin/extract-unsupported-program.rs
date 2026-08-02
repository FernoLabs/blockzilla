//! Preserve exact deployment and invocation evidence for a legacy program from
//! a trusted launch replay checkpoint and Blockzilla Compact V2 input.

use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_replay::{
    BPF_LOADER_PROGRAM_ID, CompactArchivedTransactionOutcome, CompactGenerationContext,
    CompactInstructionData, CompactInstructionProbe, CompactMessageVersion,
    CompactRecentBlockhashProbe, CompactSlotProbe, CompactTransactionProbe, CompactVisitConfig,
    CompactVisitControl, CompactVisitEvent, LaunchCheckpointResumeConfig, LaunchReplayError,
    LoaderAccountKind, extract_program,
    launch_replay::{LaunchInstructionDiffCapture, resume_launch_chain_diagnostic_from_checkpoint},
    read_compact_generation_context, visit_compact_generation,
};
use clap::Parser;
use object::{Object, ObjectSymbol};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Parser)]
#[command(
    name = "extract-unsupported-program",
    about = "Resume Compact V2 replay and extract exact evidence for its first unsupported program"
)]
struct Cli {
    /// Trusted frozen replay checkpoint.
    #[arg(long)]
    checkpoint: PathBuf,
    /// Trusted standard SHA-256 over the complete checkpoint file.
    #[arg(long)]
    expected_checkpoint_sha256: String,
    /// Exact completed Compact generation bound by the checkpoint.
    #[arg(long)]
    completed_generation: PathBuf,
    /// Successor Compact generation in ledger order. Repeat for a chain.
    #[arg(long = "successor-generation", required = true)]
    successor_generations: Vec<PathBuf>,
    /// Destination for the canonical padding-free legacy ELF.
    #[arg(long)]
    elf_out: PathBuf,
    /// Destination for the complete JSON reproduction evidence.
    #[arg(long)]
    evidence_out: PathBuf,
    /// Fail unless the extracted canonical ELF has this exact length.
    #[arg(long, default_value_t = 15_464)]
    expected_canonical_elf_len: usize,
    /// Fail unless replay committed this many loader mutations before the stop.
    #[arg(long, default_value_t = 18)]
    expected_bpf_loader_mutations: u64,
    /// Fail unless replay commits this total number of legacy-BPF
    /// instructions (deployment plus executable invocations).
    #[arg(long, default_value_t = 459)]
    expected_replay_bpf_instructions: u64,
    /// Fail unless Compact contains this many Writes for the target account.
    #[arg(long, default_value_t = 17)]
    expected_loader_writes: usize,
    /// Fail unless Compact contains this many Finalize instructions.
    #[arg(long, default_value_t = 1)]
    expected_loader_finalizes: usize,
    /// Already-supported program to extract when replay completes. Requires
    /// the three exact target coordinate arguments below.
    #[arg(long)]
    target_program_id: Option<String>,
    #[arg(long)]
    target_slot: Option<u64>,
    #[arg(long)]
    target_transaction_index: Option<u32>,
    #[arg(long)]
    target_instruction_index: Option<u32>,
    /// Stop after printing Compact loader mutations for the selected program.
    /// This remains useful when the program account is absent from replay
    /// state because an earlier deployment transaction was not committed.
    #[arg(long)]
    deployment_scan_only: bool,
    /// In deployment-scan mode, trace non-loader instructions touching this
    /// account instead of the selected program account.
    #[arg(long, requires = "deployment_scan_only")]
    trace_account_id: Option<String>,
    /// Additional Compact V2 generations to scan for the trace account.
    #[arg(long, requires = "trace_account_id")]
    trace_generation: Vec<PathBuf>,
}

#[derive(Debug, Serialize)]
struct Evidence {
    schema: &'static str,
    input_format: &'static str,
    checkpoint: CheckpointEvidence,
    generation: GenerationEvidence,
    coordinate: CoordinateEvidence,
    block: BlockEvidence,
    transaction: TransactionEvidence,
    instruction: InstructionEvidence,
    deployment: DeploymentEvidence,
    program_account: ProgramAccountEvidence,
    replay_prefix: ReplayPrefixEvidence,
}

#[derive(Debug, Serialize)]
struct CheckpointEvidence {
    path: String,
    trusted_file_sha256: String,
    completed_generation: String,
    successor_generations: Vec<String>,
}

#[derive(Debug, Serialize)]
struct GenerationEvidence {
    root: String,
    cluster_id: String,
    epoch: u64,
    generation_id: String,
    generation_digest: String,
    registry_sha256: String,
}

#[derive(Debug, Serialize)]
struct CoordinateEvidence {
    slot: u64,
    transaction_index: u32,
    instruction_index: u32,
    failure_kind: &'static str,
    program_id: KeyEvidence,
}

#[derive(Debug, Serialize)]
struct BlockEvidence {
    block_id: u32,
    slot: u64,
    parent_slot: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    blockhash_id: u32,
    blockhash: String,
    previous_blockhash_id: u32,
    previous_blockhash: String,
    declared_transaction_count: u32,
}

#[derive(Debug, Serialize)]
struct TransactionEvidence {
    transaction_index: u32,
    row_flags: u32,
    archived_outcome: &'static str,
    signature_count: u8,
    signature_bytes_present_in_compact_hot_row: bool,
    message_version: &'static str,
    header: HeaderEvidence,
    recent_blockhash: RecentBlockhashEvidence,
    account_keys: Vec<TransactionKeyEvidence>,
    address_table_lookups: Vec<AddressTableLookupEvidence>,
    instruction_count: usize,
}

#[derive(Debug, Serialize)]
struct HeaderEvidence {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Serialize)]
struct RecentBlockhashEvidence {
    kind: &'static str,
    registry_id: Option<i32>,
    hash: String,
}

#[derive(Debug, Serialize)]
struct TransactionKeyEvidence {
    index: usize,
    key: KeyEvidence,
    is_signer: bool,
    is_writable: bool,
    is_invoked_program_id: bool,
    is_instruction_account: bool,
}

#[derive(Debug, Serialize)]
struct AddressTableLookupEvidence {
    account_key: KeyEvidence,
    writable_indexes: Vec<u8>,
    readonly_indexes: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct InstructionEvidence {
    instruction_index: u32,
    program_id_index: u8,
    program_id: KeyEvidence,
    data_kind: &'static str,
    data_len: usize,
    data_hex: String,
    accounts: Vec<InstructionAccountEvidence>,
}

#[derive(Debug, Serialize)]
struct InstructionAccountEvidence {
    position: usize,
    account_index: u8,
    key: KeyEvidence,
    is_signer: bool,
    is_writable: bool,
}

#[derive(Debug, Serialize)]
struct KeyEvidence {
    base58: String,
    hex: String,
}

#[derive(Debug, Serialize)]
struct ProgramAccountEvidence {
    pubkey: KeyEvidence,
    owner: KeyEvidence,
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
    account_data_len: usize,
    account_data_sha256: String,
    loader_layout: &'static str,
    expected_canonical_elf_len: usize,
    canonical_elf_len: usize,
    canonical_elf_sha256: String,
    imports: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DeploymentEvidence {
    expected_deployment_mutation_count: u64,
    expected_write_count: usize,
    expected_finalize_count: usize,
    observed_replay_bpf_instruction_count: u64,
    observed_compact_mutation_count: usize,
    observed_write_count: usize,
    observed_finalize_count: usize,
    reconstructed_account_data_sha256: String,
    reconstructed_account_matches_replay: bool,
    canonical_elf_bytes_covered_by_writes: bool,
    mutations: Vec<LoaderMutationEvidence>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LoaderMutationEvidence {
    Write {
        coordinate: LoaderCoordinateEvidence,
        archived_outcome: &'static str,
        signature_count: u8,
        header: HeaderEvidence,
        account_keys: Vec<TransactionKeyEvidence>,
        instruction_accounts: Vec<InstructionAccountEvidence>,
        target_is_signer: bool,
        target_is_writable: bool,
        raw_instruction_len: usize,
        raw_instruction_hex: String,
        offset: u32,
        chunk_len: usize,
        chunk_sha256: String,
        trailing_len: usize,
        trailing_sha256: String,
        #[serde(skip)]
        chunk: Vec<u8>,
    },
    Finalize {
        coordinate: LoaderCoordinateEvidence,
        archived_outcome: &'static str,
        signature_count: u8,
        header: HeaderEvidence,
        account_keys: Vec<TransactionKeyEvidence>,
        instruction_accounts: Vec<InstructionAccountEvidence>,
        target_is_signer: bool,
        target_is_writable: bool,
        raw_instruction_len: usize,
        raw_instruction_hex: String,
    },
}

#[derive(Debug, Serialize)]
struct LoaderCoordinateEvidence {
    slot: u64,
    transaction_index: u32,
    instruction_index: u32,
}

#[derive(Debug, Serialize)]
struct ReplayPrefixEvidence {
    epoch: u64,
    last_completed_slot: Option<u64>,
    slots_processed: u64,
    committed_transactions: u64,
    failed_transactions: u64,
    committed_instructions: u64,
    expected_legacy_bpf_instructions: u64,
    legacy_bpf_instructions: u64,
    accounts: usize,
    account_state_sha256: String,
}

#[derive(Debug)]
struct CapturedFailureEvidence {
    block: BlockEvidence,
    transaction: TransactionEvidence,
    instruction: InstructionEvidence,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    ensure!(
        cli.elf_out != cli.evidence_out,
        "--elf-out and --evidence-out must be distinct paths"
    );
    let expected_compact_mutations = cli
        .expected_loader_writes
        .checked_add(cli.expected_loader_finalizes)
        .context("expected loader mutation count overflow")?;
    ensure!(
        u64::try_from(expected_compact_mutations).ok() == Some(cli.expected_bpf_loader_mutations),
        "expected Writes ({}) plus Finalizes ({}) must equal expected BPF-loader mutations ({})",
        cli.expected_loader_writes,
        cli.expected_loader_finalizes,
        cli.expected_bpf_loader_mutations
    );
    let trusted_checkpoint_sha256 = parse_sha256(&cli.expected_checkpoint_sha256)
        .context("parse --expected-checkpoint-sha256")?;

    let replay = resume_launch_chain_diagnostic_from_checkpoint(
        &cli.successor_generations,
        CompactVisitConfig::default(),
        LaunchInstructionDiffCapture::None,
        LaunchCheckpointResumeConfig {
            checkpoint_path: &cli.checkpoint,
            expected_checkpoint_file_sha256: trusted_checkpoint_sha256,
            completed_generation: &cli.completed_generation,
            checkpoint_out: None,
            replay_workers: 1,
        },
        |_| unreachable!("diff capture None never invokes the mutation visitor"),
    )
    .context("resume mutation-only replay from trusted checkpoint")?;

    let (slot, transaction_index, instruction_index, program_id, coordinate_kind) = if let Some(
        failure,
    ) =
        replay.failure.as_ref()
    {
        let coordinate = match &failure.error {
            LaunchReplayError::UnsupportedProgram {
                slot,
                transaction_index,
                instruction_index,
                program_id,
            } => (*slot, *transaction_index, *instruction_index, *program_id),
            error => bail!(
                "replay stopped for a non-UnsupportedProgram reason; refusing extraction: {error}"
            ),
        };
        ensure!(
            failure.location.slot == coordinate.0
                && failure.location.transaction_index == Some(coordinate.1)
                && failure.location.instruction_index == Some(coordinate.2),
            "failure coordinate disagrees with UnsupportedProgram payload"
        );
        (
            coordinate.0,
            coordinate.1,
            coordinate.2,
            coordinate.3,
            "UnsupportedProgram",
        )
    } else {
        let program_id = parse_pubkey(
            cli.target_program_id
                .as_deref()
                .context("completed replay requires --target-program-id")?,
        )
        .context("parse --target-program-id")?;
        let slot = cli
            .target_slot
            .context("completed replay requires --target-slot")?;
        let transaction_index = cli
            .target_transaction_index
            .context("completed replay requires --target-transaction-index")?;
        let instruction_index = cli
            .target_instruction_index
            .context("completed replay requires --target-instruction-index")?;
        (
            slot,
            transaction_index,
            instruction_index,
            program_id,
            "ExecutedProgram",
        )
    };

    let generation = replay
        .contexts
        .iter()
        .find(|context| {
            context.first_slot.is_some_and(|first| first <= slot)
                && context.last_slot.is_some_and(|last| slot <= last)
        })
        .with_context(|| format!("no visited successor generation contains failure slot {slot}"))?;

    let captured = capture_exact_compact_instruction(
        generation,
        slot,
        transaction_index,
        instruction_index,
        program_id,
    )?;

    if cli.deployment_scan_only {
        let loader_mutations = capture_loader_deployment(generation, slot, program_id)?;
        let writes = loader_mutations
            .iter()
            .filter(|mutation| matches!(mutation, LoaderMutationEvidence::Write { .. }))
            .count();
        let finalizes = loader_mutations
            .iter()
            .filter(|mutation| matches!(mutation, LoaderMutationEvidence::Finalize { .. }))
            .count();
        println!("input_format=blockzilla-compact-archive-v2");
        println!(
            "coordinate kind={coordinate_kind} slot={slot} transaction={transaction_index} instruction={instruction_index} program={}",
            bs58::encode(program_id).into_string()
        );
        println!(
            "loader_deployment_scan mutations={} writes={writes} finalizes={finalizes}",
            loader_mutations.len()
        );
        for (index, mutation) in loader_mutations.iter().enumerate() {
            let is_boundary_sample = index < 5 || index.saturating_add(5) >= loader_mutations.len();
            match mutation {
                LoaderMutationEvidence::Write {
                    coordinate,
                    offset,
                    chunk_len,
                    archived_outcome,
                    ..
                } if is_boundary_sample => println!(
                    "loader_write slot={} transaction={} instruction={} offset={offset} bytes={chunk_len} archived_outcome={archived_outcome}",
                    coordinate.slot, coordinate.transaction_index, coordinate.instruction_index
                ),
                LoaderMutationEvidence::Write { .. } => {}
                LoaderMutationEvidence::Finalize {
                    coordinate,
                    archived_outcome,
                    ..
                } => println!(
                    "loader_finalize slot={} transaction={} instruction={} archived_outcome={archived_outcome}",
                    coordinate.slot, coordinate.transaction_index, coordinate.instruction_index
                ),
            }
        }
        let trace_account = cli
            .trace_account_id
            .as_deref()
            .map(parse_pubkey)
            .transpose()
            .context("parse --trace-account-id")?
            .unwrap_or(program_id);
        println!(
            "non_loader_trace_account={}",
            bs58::encode(trace_account).into_string()
        );
        let event_accounts = print_non_loader_target_events(generation, slot, trace_account)?;
        for path in &cli.trace_generation {
            let context = read_compact_generation_context(path)
                .with_context(|| format!("read trace generation {}", path.display()))?;
            let last_slot = context
                .last_slot
                .context("trace generation has no last slot")?;
            println!(
                "additional_trace_generation epoch={} generation_id={} last_slot={last_slot}",
                context.epoch, context.generation_id
            );
            let _ = print_non_loader_target_events(&context, last_slot, trace_account)?;
        }
        println!("non_loader_target_accounts={}", event_accounts.len());
        for pubkey in event_accounts {
            if let Some(account) = replay.replay.account_state.get(&pubkey) {
                println!(
                    "target_event_account pubkey={} present=true lamports={} owner={} executable={} data_len={}",
                    bs58::encode(pubkey).into_string(),
                    account.lamports,
                    bs58::encode(account.owner).into_string(),
                    account.executable,
                    account.data.len(),
                );
            } else {
                println!(
                    "target_event_account pubkey={} present=false",
                    bs58::encode(pubkey).into_string()
                );
            }
        }
        return Ok(());
    }

    let program_account = replay
        .replay
        .account_state
        .get(&program_id)
        .with_context(|| {
            format!(
                "unsupported program account {} is absent from committed replay state",
                bs58::encode(program_id).into_string()
            )
        })?;
    ensure!(
        program_account.executable,
        "unsupported program account {} is not executable",
        bs58::encode(program_id).into_string()
    );
    ensure!(
        program_account.owner == BPF_LOADER_PROGRAM_ID,
        "unsupported executable {} is owned by {}, not the legacy BPF loader {}",
        bs58::encode(program_id).into_string(),
        bs58::encode(program_account.owner).into_string(),
        bs58::encode(BPF_LOADER_PROGRAM_ID).into_string()
    );
    let extracted = extract_program(LoaderAccountKind::Legacy, &program_account.data)
        .context("extract canonical legacy ELF from executable replay account")?;
    ensure!(
        extracted.elf.len() == cli.expected_canonical_elf_len,
        "canonical ELF length is {}, expected {}",
        extracted.elf.len(),
        cli.expected_canonical_elf_len
    );
    ensure!(
        replay.replay.bpf_loader_mutations == cli.expected_replay_bpf_instructions,
        "replay committed {} legacy-BPF instructions, expected {}",
        replay.replay.bpf_loader_mutations,
        cli.expected_replay_bpf_instructions
    );
    let loader_mutations = capture_loader_deployment(generation, slot, program_id)?;
    let observed_write_count = loader_mutations
        .iter()
        .filter(|mutation| matches!(mutation, LoaderMutationEvidence::Write { .. }))
        .count();
    let observed_finalize_count = loader_mutations
        .iter()
        .filter(|mutation| matches!(mutation, LoaderMutationEvidence::Finalize { .. }))
        .count();
    ensure!(
        loader_mutations.len() as u64 == cli.expected_bpf_loader_mutations,
        "Compact contains {} target loader mutations, expected {}",
        loader_mutations.len(),
        cli.expected_bpf_loader_mutations
    );
    ensure!(
        observed_write_count == cli.expected_loader_writes
            && observed_finalize_count == cli.expected_loader_finalizes,
        "expected {} legacy loader Writes plus {} Finalize instructions, found {observed_write_count} Writes and {observed_finalize_count} Finalize",
        cli.expected_loader_writes,
        cli.expected_loader_finalizes,
    );
    let (reconstructed_account_data, canonical_elf_bytes_covered_by_writes) =
        reconstruct_loader_account(
            program_account.data.len(),
            extracted.elf.len(),
            &loader_mutations,
        )?;
    ensure!(
        canonical_elf_bytes_covered_by_writes,
        "loader Writes do not cover every byte of the canonical ELF"
    );
    ensure!(
        reconstructed_account_data == program_account.data,
        "bytes reconstructed from Compact loader Writes disagree with replay account data"
    );
    let reconstructed_account_data_sha256: [u8; 32] =
        Sha256::digest(&reconstructed_account_data).into();
    let imports =
        elf_imports(&extracted.elf).context("read undefined symbols from canonical ELF")?;
    let account_data_sha256: [u8; 32] = Sha256::digest(&program_account.data).into();

    let evidence = Evidence {
        schema: "blockzilla-legacy-program-evidence-v1",
        input_format: "blockzilla-compact-archive-v2",
        checkpoint: CheckpointEvidence {
            path: display_path(&cli.checkpoint),
            trusted_file_sha256: hex(&trusted_checkpoint_sha256),
            completed_generation: display_path(&cli.completed_generation),
            successor_generations: cli
                .successor_generations
                .iter()
                .map(|path| display_path(path))
                .collect(),
        },
        generation: GenerationEvidence {
            root: display_path(&generation.root),
            cluster_id: generation.cluster_id.clone(),
            epoch: generation.epoch,
            generation_id: generation.generation_id.clone(),
            generation_digest: hex(&generation.binding.generation_digest),
            registry_sha256: hex(&generation.binding.registry_sha256),
        },
        coordinate: CoordinateEvidence {
            slot,
            transaction_index,
            instruction_index,
            failure_kind: coordinate_kind,
            program_id: key(program_id),
        },
        block: captured.block,
        transaction: captured.transaction,
        instruction: captured.instruction,
        deployment: DeploymentEvidence {
            expected_deployment_mutation_count: cli.expected_bpf_loader_mutations,
            expected_write_count: cli.expected_loader_writes,
            expected_finalize_count: cli.expected_loader_finalizes,
            observed_replay_bpf_instruction_count: replay.replay.bpf_loader_mutations,
            observed_compact_mutation_count: loader_mutations.len(),
            observed_write_count,
            observed_finalize_count,
            reconstructed_account_data_sha256: hex(&reconstructed_account_data_sha256),
            reconstructed_account_matches_replay: true,
            canonical_elf_bytes_covered_by_writes,
            mutations: loader_mutations,
        },
        program_account: ProgramAccountEvidence {
            pubkey: key(program_id),
            owner: key(program_account.owner),
            lamports: program_account.lamports,
            executable: program_account.executable,
            rent_epoch: program_account.rent_epoch,
            account_data_len: program_account.data.len(),
            account_data_sha256: hex(&account_data_sha256),
            loader_layout: "legacy-elf-at-offset-zero",
            expected_canonical_elf_len: cli.expected_canonical_elf_len,
            canonical_elf_len: extracted.elf.len(),
            canonical_elf_sha256: hex(&extracted.elf_sha256),
            imports,
        },
        replay_prefix: ReplayPrefixEvidence {
            epoch: replay.replay.epoch,
            last_completed_slot: replay.replay.last_slot,
            slots_processed: replay.replay.slots_processed,
            committed_transactions: replay.replay.transactions_processed,
            failed_transactions: replay.replay.failed_transactions,
            committed_instructions: replay.replay.instructions_processed,
            expected_legacy_bpf_instructions: cli.expected_replay_bpf_instructions,
            legacy_bpf_instructions: replay.replay.bpf_loader_mutations,
            accounts: replay.replay.account_state.len(),
            account_state_sha256: hex(&replay.replay.account_state.canonical_hash()),
        },
    };
    let mut evidence_json =
        serde_json::to_vec_pretty(&evidence).context("serialize JSON evidence")?;
    evidence_json.push(b'\n');

    write_atomic(&cli.elf_out, &extracted.elf)
        .with_context(|| format!("write canonical ELF to {}", cli.elf_out.display()))?;
    write_atomic(&cli.evidence_out, &evidence_json)
        .with_context(|| format!("write JSON evidence to {}", cli.evidence_out.display()))?;

    println!("input_format=blockzilla-compact-archive-v2");
    println!(
        "coordinate kind={coordinate_kind} slot={slot} transaction={transaction_index} instruction={instruction_index} program={}",
        bs58::encode(program_id).into_string()
    );
    println!(
        "canonical_elf path={} bytes={} sha256={}",
        cli.elf_out.display(),
        extracted.elf.len(),
        hex(&extracted.elf_sha256)
    );
    println!(
        "loader_deployment mutations={} writes={} finalizes={} reconstructed_account_sha256={}",
        cli.expected_bpf_loader_mutations,
        observed_write_count,
        observed_finalize_count,
        hex(&reconstructed_account_data_sha256)
    );
    println!("evidence path={}", cli.evidence_out.display());
    Ok(())
}

fn capture_loader_deployment(
    generation: &CompactGenerationContext,
    failure_slot: u64,
    program_id: [u8; 32],
) -> Result<Vec<LoaderMutationEvidence>> {
    let end_slot_exclusive = failure_slot
        .checked_add(1)
        .context("cannot form an exclusive bound after u64::MAX failure slot")?;
    let mut mutations = Vec::new();
    visit_compact_generation(
        &generation.root,
        CompactVisitConfig {
            start_slot: generation.first_slot,
            end_slot_exclusive: Some(end_slot_exclusive),
            max_slots: None,
        },
        |event| {
            let CompactVisitEvent::Slot { slot: block, .. } = event else {
                return Ok(CompactVisitControl::Continue);
            };
            for transaction in &block.transactions {
                for instruction in &transaction.instructions {
                    if instruction.program_id != BPF_LOADER_PROGRAM_ID {
                        continue;
                    }
                    let Some(target_index) = instruction.account_indexes.first().copied() else {
                        continue;
                    };
                    let Some(target) = transaction.account_keys.get(usize::from(target_index))
                    else {
                        continue;
                    };
                    if *target != program_id {
                        continue;
                    }
                    let mutation = capture_loader_mutation(block, transaction, instruction)
                        .map_err(|error| {
                            blockzilla_replay::CompactProbeError::Visitor(error.to_string())
                        })?;
                    mutations.push(mutation);
                }
            }
            Ok(CompactVisitControl::Continue)
        },
    )
    .with_context(|| {
        format!("scan Compact V2 loader provenance through failure slot {failure_slot}")
    })?;
    Ok(mutations)
}

fn print_non_loader_target_events(
    generation: &CompactGenerationContext,
    failure_slot: u64,
    program_id: [u8; 32],
) -> Result<BTreeSet<[u8; 32]>> {
    let end_slot_exclusive = failure_slot
        .checked_add(1)
        .context("cannot form an exclusive bound after u64::MAX failure slot")?;
    let mut event_count = 0_usize;
    let mut event_accounts = BTreeSet::new();
    visit_compact_generation(
        &generation.root,
        CompactVisitConfig {
            start_slot: generation.first_slot,
            end_slot_exclusive: Some(end_slot_exclusive),
            max_slots: None,
        },
        |event| {
            let CompactVisitEvent::Slot { slot: block, .. } = event else {
                return Ok(CompactVisitControl::Continue);
            };
            for transaction in &block.transactions {
                for instruction in &transaction.instructions {
                    if instruction.program_id == BPF_LOADER_PROGRAM_ID {
                        continue;
                    }
                    let touches_target = instruction.account_indexes.iter().any(|account_index| {
                        transaction
                            .account_keys
                            .get(usize::from(*account_index))
                            .is_some_and(|pubkey| *pubkey == program_id)
                    });
                    if !touches_target {
                        continue;
                    }
                    event_count = event_count.saturating_add(1);
                    event_accounts.extend(transaction.account_keys.iter().copied());
                    println!(
                        "target_event slot={} transaction={} instruction={} program={} archived_outcome={} data={:?}",
                        block.slot,
                        transaction.tx_index,
                        instruction.instruction_index,
                        bs58::encode(instruction.program_id).into_string(),
                        archived_outcome(transaction.archived_outcome),
                        instruction.data,
                    );
                }
            }
            Ok(CompactVisitControl::Continue)
        },
    )
    .with_context(|| {
        format!("scan non-loader target-account events through failure slot {failure_slot}")
    })?;
    println!("non_loader_target_events={event_count}");
    Ok(event_accounts)
}

fn capture_loader_mutation(
    block: &CompactSlotProbe,
    transaction: &CompactTransactionProbe,
    instruction: &CompactInstructionProbe,
) -> Result<LoaderMutationEvidence> {
    ensure!(
        transaction.version == CompactMessageVersion::Legacy,
        "legacy BPF-loader deployment unexpectedly uses {:?}",
        transaction.version
    );
    ensure!(
        transaction.archived_outcome == CompactArchivedTransactionOutcome::Succeeded,
        "loader mutation at slot {} tx {} instruction {} was not archived successful",
        block.slot,
        transaction.tx_index,
        instruction.instruction_index
    );
    let raw = raw_instruction_data(instruction)?;
    ensure!(
        raw.len() >= 4,
        "legacy loader instruction is shorter than its tag"
    );
    let tag = u32::from_le_bytes(raw[..4].try_into().expect("four-byte loader tag"));
    let coordinate = LoaderCoordinateEvidence {
        slot: block.slot,
        transaction_index: transaction.tx_index,
        instruction_index: instruction.instruction_index,
    };
    let target_index = usize::from(
        *instruction
            .account_indexes
            .first()
            .context("target loader instruction has no program account")?,
    );
    let flags = account_flags(transaction)?;
    let (target_is_signer, target_is_writable) = *flags
        .get(target_index)
        .context("target loader account index is unresolved")?;
    let account_keys = transaction
        .account_keys
        .iter()
        .copied()
        .enumerate()
        .map(|(index, pubkey)| {
            let (is_signer, is_writable) = flags[index];
            TransactionKeyEvidence {
                index,
                key: key(pubkey),
                is_signer,
                is_writable,
                is_invoked_program_id: index == usize::from(instruction.program_id_index),
                is_instruction_account: instruction
                    .account_indexes
                    .iter()
                    .any(|account_index| usize::from(*account_index) == index),
            }
        })
        .collect();
    let mut instruction_accounts = Vec::with_capacity(instruction.account_indexes.len());
    for (position, account_index) in instruction.account_indexes.iter().copied().enumerate() {
        let index = usize::from(account_index);
        let pubkey = transaction.account_keys.get(index).with_context(|| {
            format!("loader account position {position} index {account_index} is unresolved")
        })?;
        let (is_signer, is_writable) = flags[index];
        instruction_accounts.push(InstructionAccountEvidence {
            position,
            account_index,
            key: key(*pubkey),
            is_signer,
            is_writable,
        });
    }
    let header = || HeaderEvidence {
        num_required_signatures: transaction.header.num_required_signatures,
        num_readonly_signed_accounts: transaction.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: transaction.header.num_readonly_unsigned_accounts,
    };
    match tag {
        0 => {
            ensure!(
                raw.len() >= 16,
                "legacy loader Write is shorter than its offset and vector length"
            );
            let offset = u32::from_le_bytes(raw[4..8].try_into().expect("four-byte write offset"));
            let declared_len =
                u64::from_le_bytes(raw[8..16].try_into().expect("eight-byte vector length"));
            let declared_len = usize::try_from(declared_len)
                .context("legacy loader Write length does not fit usize")?;
            let chunk_end = 16_usize
                .checked_add(declared_len)
                .context("legacy loader Write payload range overflow")?;
            ensure!(
                chunk_end <= raw.len(),
                "legacy loader Write declares {declared_len} bytes but carries only {}",
                raw.len() - 16
            );
            // Launch-era `limited_deserialize` permits trailing instruction
            // bytes. The final observed Write declares 552 bytes inside a
            // 932-byte payload; only the declared prefix mutates the account.
            let chunk = &raw[16..chunk_end];
            let trailing = &raw[chunk_end..];
            let chunk_sha256: [u8; 32] = Sha256::digest(chunk).into();
            let trailing_sha256: [u8; 32] = Sha256::digest(trailing).into();
            Ok(LoaderMutationEvidence::Write {
                coordinate,
                archived_outcome: archived_outcome(transaction.archived_outcome),
                signature_count: transaction.signature_count,
                header: header(),
                account_keys,
                instruction_accounts,
                target_is_signer,
                target_is_writable,
                raw_instruction_len: raw.len(),
                raw_instruction_hex: hex(raw),
                offset,
                chunk_len: chunk.len(),
                chunk_sha256: hex(&chunk_sha256),
                trailing_len: trailing.len(),
                trailing_sha256: hex(&trailing_sha256),
                chunk: chunk.to_vec(),
            })
        }
        1 => {
            ensure!(
                raw.len() == 4,
                "legacy loader Finalize has {} trailing bytes",
                raw.len() - 4
            );
            Ok(LoaderMutationEvidence::Finalize {
                coordinate,
                archived_outcome: archived_outcome(transaction.archived_outcome),
                signature_count: transaction.signature_count,
                header: header(),
                account_keys,
                instruction_accounts,
                target_is_signer,
                target_is_writable,
                raw_instruction_len: raw.len(),
                raw_instruction_hex: hex(raw),
            })
        }
        other => bail!("unsupported legacy loader instruction tag {other}"),
    }
}

fn reconstruct_loader_account(
    account_data_len: usize,
    canonical_elf_len: usize,
    mutations: &[LoaderMutationEvidence],
) -> Result<(Vec<u8>, bool)> {
    ensure!(
        canonical_elf_len <= account_data_len,
        "canonical ELF exceeds loader account allocation"
    );
    let mut reconstructed = vec![0_u8; account_data_len];
    let mut covered = vec![false; canonical_elf_len];
    let mut finalized = false;
    for mutation in mutations {
        match mutation {
            LoaderMutationEvidence::Write { offset, chunk, .. } => {
                ensure!(!finalized, "loader Write appears after Finalize");
                let start = usize::try_from(*offset).context("loader Write offset does not fit")?;
                let end = start
                    .checked_add(chunk.len())
                    .context("loader Write range overflow")?;
                ensure!(
                    end <= reconstructed.len(),
                    "loader Write range {start}..{end} exceeds account length {}",
                    reconstructed.len()
                );
                reconstructed[start..end].copy_from_slice(chunk);
                let covered_end = end.min(canonical_elf_len);
                if start < covered_end {
                    covered[start..covered_end].fill(true);
                }
            }
            LoaderMutationEvidence::Finalize { .. } => {
                ensure!(
                    !finalized,
                    "loader deployment contains multiple Finalize records"
                );
                finalized = true;
            }
        }
    }
    ensure!(finalized, "loader deployment has no Finalize record");
    Ok((reconstructed, covered.iter().all(|covered| *covered)))
}

fn elf_imports(elf: &[u8]) -> Result<Vec<String>> {
    let file = object::File::parse(elf)?;
    let mut imports = BTreeSet::new();
    for symbol in file.symbols().chain(file.dynamic_symbols()) {
        if symbol.is_undefined()
            && let Ok(name) = symbol.name()
            && !name.is_empty()
        {
            imports.insert(name.to_owned());
        }
    }
    Ok(imports.into_iter().collect())
}

fn capture_exact_compact_instruction(
    generation: &CompactGenerationContext,
    slot: u64,
    transaction_index: u32,
    instruction_index: u32,
    program_id: [u8; 32],
) -> Result<CapturedFailureEvidence> {
    let end_slot_exclusive = slot
        .checked_add(1)
        .context("cannot form an exclusive bound after u64::MAX failure slot")?;
    let mut captured = None;
    let mut observed_generation = false;
    visit_compact_generation(
        &generation.root,
        CompactVisitConfig {
            start_slot: Some(slot),
            end_slot_exclusive: Some(end_slot_exclusive),
            max_slots: Some(1),
        },
        |event| match event {
            CompactVisitEvent::Generation(context) => {
                if context.binding.generation_digest != generation.binding.generation_digest {
                    return Err(blockzilla_replay::CompactProbeError::Visitor(
                        "generation digest changed between replay and evidence scan".to_owned(),
                    ));
                }
                observed_generation = true;
                Ok(CompactVisitControl::Continue)
            }
            CompactVisitEvent::Slot { slot: block, .. } => {
                if block.slot != slot {
                    return Err(blockzilla_replay::CompactProbeError::Visitor(format!(
                        "requested exact slot {slot}, Compact yielded {}",
                        block.slot
                    )));
                }
                let evidence =
                    capture_from_slot(block, transaction_index, instruction_index, program_id)
                        .map_err(|error| {
                            blockzilla_replay::CompactProbeError::Visitor(error.to_string())
                        })?;
                captured = Some(evidence);
                Ok(CompactVisitControl::Stop)
            }
        },
    )
    .with_context(|| format!("rescan exact Compact V2 failure slot {slot}"))?;
    ensure!(
        observed_generation,
        "Compact visitor omitted generation event"
    );
    captured.with_context(|| format!("Compact generation has no block row for exact slot {slot}"))
}

fn capture_from_slot(
    block: &CompactSlotProbe,
    transaction_index: u32,
    instruction_index: u32,
    program_id: [u8; 32],
) -> Result<CapturedFailureEvidence> {
    let transaction = block
        .transactions
        .iter()
        .find(|transaction| transaction.tx_index == transaction_index)
        .with_context(|| {
            format!(
                "slot {} has no transaction index {transaction_index}",
                block.slot
            )
        })?;
    ensure!(
        transaction.version == CompactMessageVersion::Legacy,
        "UnsupportedProgram evidence requires a fully resolved legacy message; found {:?}",
        transaction.version
    );
    ensure!(
        transaction.address_table_lookups.is_empty(),
        "legacy failure transaction unexpectedly carries address table lookups"
    );
    let instruction = transaction
        .instructions
        .iter()
        .find(|instruction| instruction.instruction_index == instruction_index)
        .with_context(|| {
            format!(
                "slot {} transaction {transaction_index} has no instruction index {instruction_index}",
                block.slot
            )
        })?;
    ensure!(
        instruction.program_id == program_id,
        "Compact instruction program {} disagrees with replay failure program {}",
        bs58::encode(instruction.program_id).into_string(),
        bs58::encode(program_id).into_string()
    );
    let program_key = transaction
        .account_keys
        .get(usize::from(instruction.program_id_index))
        .context("failing instruction program_id_index is out of bounds")?;
    ensure!(
        *program_key == program_id,
        "program_id_index resolves to a key different from the decoded instruction program"
    );

    let account_flags = account_flags(transaction)?;
    let instruction_data = raw_instruction_data(instruction)?;
    let mut instruction_accounts = Vec::with_capacity(instruction.account_indexes.len());
    for (position, account_index) in instruction.account_indexes.iter().copied().enumerate() {
        let index = usize::from(account_index);
        let pubkey = transaction.account_keys.get(index).with_context(|| {
            format!("instruction account position {position} index {account_index} is unresolved")
        })?;
        let (is_signer, is_writable) = account_flags[index];
        instruction_accounts.push(InstructionAccountEvidence {
            position,
            account_index,
            key: key(*pubkey),
            is_signer,
            is_writable,
        });
    }

    let transaction_keys = transaction
        .account_keys
        .iter()
        .copied()
        .enumerate()
        .map(|(index, pubkey)| {
            let (is_signer, is_writable) = account_flags[index];
            TransactionKeyEvidence {
                index,
                key: key(pubkey),
                is_signer,
                is_writable,
                is_invoked_program_id: index == usize::from(instruction.program_id_index),
                is_instruction_account: instruction
                    .account_indexes
                    .iter()
                    .any(|account_index| usize::from(*account_index) == index),
            }
        })
        .collect();
    let address_table_lookups = transaction
        .address_table_lookups
        .iter()
        .map(|lookup| AddressTableLookupEvidence {
            account_key: key(lookup.account_key),
            writable_indexes: lookup.writable_indexes.clone(),
            readonly_indexes: lookup.readonly_indexes.clone(),
        })
        .collect();

    Ok(CapturedFailureEvidence {
        block: BlockEvidence {
            block_id: block.block_id,
            slot: block.slot,
            parent_slot: block.parent_slot,
            block_time: block.block_time,
            block_height: block.block_height,
            blockhash_id: block.blockhash_id,
            blockhash: hex(&block.blockhash),
            previous_blockhash_id: block.previous_blockhash_id,
            previous_blockhash: hex(&block.previous_blockhash),
            declared_transaction_count: block.transaction_count,
        },
        transaction: TransactionEvidence {
            transaction_index: transaction.tx_index,
            row_flags: transaction.row_flags,
            archived_outcome: archived_outcome(transaction.archived_outcome),
            signature_count: transaction.signature_count,
            signature_bytes_present_in_compact_hot_row: false,
            message_version: "legacy",
            header: HeaderEvidence {
                num_required_signatures: transaction.header.num_required_signatures,
                num_readonly_signed_accounts: transaction.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: transaction.header.num_readonly_unsigned_accounts,
            },
            recent_blockhash: recent_blockhash(&transaction.recent_blockhash),
            account_keys: transaction_keys,
            address_table_lookups,
            instruction_count: transaction.instructions.len(),
        },
        instruction: InstructionEvidence {
            instruction_index: instruction.instruction_index,
            program_id_index: instruction.program_id_index,
            program_id: key(instruction.program_id),
            data_kind: instruction_data_kind(&instruction.data),
            data_len: instruction_data.len(),
            data_hex: hex(instruction_data),
            accounts: instruction_accounts,
        },
    })
}

fn account_flags(transaction: &CompactTransactionProbe) -> Result<Vec<(bool, bool)>> {
    let key_count = transaction.account_keys.len();
    let required = usize::from(transaction.header.num_required_signatures);
    let readonly_signed = usize::from(transaction.header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(transaction.header.num_readonly_unsigned_accounts);
    ensure!(
        required <= key_count,
        "required signer count exceeds account keys"
    );
    ensure!(
        readonly_signed <= required,
        "readonly signed count exceeds required signer count"
    );
    ensure!(
        readonly_unsigned <= key_count - required,
        "readonly unsigned count exceeds unsigned account count"
    );
    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = key_count - readonly_unsigned;
    Ok((0..key_count)
        .map(|index| {
            let is_signer = index < required;
            let is_writable = if is_signer {
                index < writable_signed_end
            } else {
                index < writable_unsigned_end
            };
            (is_signer, is_writable)
        })
        .collect())
}

fn raw_instruction_data(instruction: &CompactInstructionProbe) -> Result<&[u8]> {
    match &instruction.data {
        CompactInstructionData::Raw(bytes)
        | CompactInstructionData::UnknownSystem(bytes)
        | CompactInstructionData::UnknownVote(bytes) => Ok(bytes),
        data => bail!(
            "failing instruction data is a semantic Compact variant ({data:?}); exact original bytes are unavailable"
        ),
    }
}

fn instruction_data_kind(data: &CompactInstructionData) -> &'static str {
    match data {
        CompactInstructionData::Raw(_) => "raw",
        CompactInstructionData::UnknownSystem(_) => "unknown-system",
        CompactInstructionData::UnknownVote(_) => "unknown-vote",
        CompactInstructionData::ComputeBudget(_) => "compute-budget-semantic",
        CompactInstructionData::System(_) => "system-semantic",
        CompactInstructionData::VoteCompactUpdateVoteState(_) => "vote-compact-update-semantic",
        CompactInstructionData::VoteCompactUpdateVoteStateSwitch { .. } => {
            "vote-compact-update-switch-semantic"
        }
        CompactInstructionData::VoteTowerSync(_) => "vote-tower-sync-semantic",
        CompactInstructionData::VoteTowerSyncSwitch { .. } => "vote-tower-sync-switch-semantic",
    }
}

fn archived_outcome(outcome: CompactArchivedTransactionOutcome) -> &'static str {
    match outcome {
        CompactArchivedTransactionOutcome::Unknown => "unknown",
        CompactArchivedTransactionOutcome::Succeeded => "succeeded",
        CompactArchivedTransactionOutcome::Failed => "failed",
    }
}

fn recent_blockhash(blockhash: &CompactRecentBlockhashProbe) -> RecentBlockhashEvidence {
    match blockhash {
        CompactRecentBlockhashProbe::Registry { id, hash } => RecentBlockhashEvidence {
            kind: "registry",
            registry_id: Some(*id),
            hash: hex(hash),
        },
        CompactRecentBlockhashProbe::Nonce(hash) => RecentBlockhashEvidence {
            kind: "nonce",
            registry_id: None,
            hash: hex(hash),
        },
    }
}

fn key(bytes: [u8; 32]) -> KeyEvidence {
    KeyEvidence {
        base58: bs58::encode(bytes).into_string(),
        hex: hex(&bytes),
    }
}

fn parse_pubkey(value: &str) -> Result<[u8; 32]> {
    let decoded = bs58::decode(value)
        .into_vec()
        .context("program id is not valid base58")?;
    decoded.try_into().map_err(|bytes: Vec<u8>| {
        anyhow::anyhow!("program id decodes to {} bytes, expected 32", bytes.len())
    })
}

fn parse_sha256(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "SHA-256 must contain exactly 64 hex digits"
    );
    let mut digest = [0_u8; 32];
    for (index, output) in digest.iter_mut().enumerate() {
        let start = index * 2;
        *output = u8::from_str_radix(&value[start..start + 2], 16)
            .with_context(|| format!("invalid hex at byte {index}"))?;
    }
    Ok(digest)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .context("output path has no file name")?
        .to_string_lossy();
    let mut staged = None;
    for attempt in 0..128_u32 {
        let temporary = parent.join(format!(
            ".{file_name}.blockzilla-extract.tmp.{}.{}",
            std::process::id(),
            attempt
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => {
                staged = Some((temporary, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let (temporary, mut file) = staged.context("could not reserve temporary output file")?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        sync_parent(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> Result<()> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_parent: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use blockzilla_format::CompactMessageHeader;
    use smallvec::smallvec;

    use super::*;

    fn transaction() -> CompactTransactionProbe {
        CompactTransactionProbe {
            tx_index: 7,
            row_flags: 9,
            archived_outcome: CompactArchivedTransactionOutcome::Succeeded,
            balance_oracle: None,
            signature_count: 2,
            version: CompactMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: smallvec![[1; 32], [2; 32], [3; 32], [4; 32], [5; 32]],
            recent_blockhash: CompactRecentBlockhashProbe::Registry {
                id: 11,
                hash: [6; 32],
            },
            address_table_lookups: Vec::new(),
            instructions: smallvec![CompactInstructionProbe {
                instruction_index: 3,
                program_id_index: 4,
                program_id: [5; 32],
                account_indexes: smallvec![0, 2, 4],
                data: CompactInstructionData::Raw(smallvec![0xde, 0xad, 0xbe, 0xef]),
            }],
        }
    }

    fn slot(transaction: CompactTransactionProbe) -> CompactSlotProbe {
        CompactSlotProbe {
            block_id: 1,
            slot: 42,
            parent_slot: 41,
            block_time: Some(100),
            block_height: Some(40),
            blockhash_id: 4,
            blockhash: [7; 32],
            previous_blockhash_id: 3,
            previous_blockhash: [8; 32],
            transaction_count: 1,
            transactions: vec![transaction],
        }
    }

    #[test]
    fn captures_exact_bytes_archived_outcome_keys_and_runtime_metas() {
        let captured = capture_from_slot(&slot(transaction()), 7, 3, [5; 32]).unwrap();

        assert_eq!(captured.transaction.archived_outcome, "succeeded");
        assert_eq!(captured.instruction.data_hex, "deadbeef");
        assert_eq!(captured.instruction.accounts.len(), 3);
        assert!(captured.instruction.accounts[0].is_signer);
        assert!(captured.instruction.accounts[0].is_writable);
        assert!(!captured.instruction.accounts[1].is_signer);
        assert!(captured.instruction.accounts[1].is_writable);
        assert!(!captured.instruction.accounts[2].is_writable);
        assert!(captured.transaction.account_keys[4].is_invoked_program_id);
    }

    #[test]
    fn fails_closed_when_compact_program_disagrees_with_replay_failure() {
        let error = capture_from_slot(&slot(transaction()), 7, 3, [9; 32]).unwrap_err();
        assert!(error.to_string().contains("disagrees"));
    }

    #[test]
    fn decodes_loader_write_bytes_and_reconstructs_out_of_order_chunks() {
        let mut transaction = transaction();
        transaction.account_keys[4] = BPF_LOADER_PROGRAM_ID;
        let mut raw = Vec::new();
        raw.extend_from_slice(&0_u32.to_le_bytes());
        raw.extend_from_slice(&2_u32.to_le_bytes());
        raw.extend_from_slice(&2_u64.to_le_bytes());
        raw.extend_from_slice(&[1, 2]);
        raw.extend_from_slice(&[9, 9]);
        transaction.instructions[0] = CompactInstructionProbe {
            instruction_index: 3,
            program_id_index: 4,
            program_id: BPF_LOADER_PROGRAM_ID,
            account_indexes: smallvec![0],
            data: CompactInstructionData::Raw(raw.clone().into()),
        };
        let block = slot(transaction);
        let first = capture_loader_mutation(
            &block,
            &block.transactions[0],
            &block.transactions[0].instructions[0],
        )
        .unwrap();
        assert!(matches!(
            &first,
            LoaderMutationEvidence::Write {
                offset: 2,
                chunk_len: 2,
                trailing_len: 2,
                raw_instruction_hex,
                ..
            } if raw_instruction_hex == &hex(&raw)
        ));
        let second = LoaderMutationEvidence::Write {
            coordinate: LoaderCoordinateEvidence {
                slot: 43,
                transaction_index: 0,
                instruction_index: 0,
            },
            archived_outcome: "succeeded",
            signature_count: 1,
            header: HeaderEvidence {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: Vec::new(),
            instruction_accounts: Vec::new(),
            target_is_signer: true,
            target_is_writable: true,
            raw_instruction_len: 18,
            raw_instruction_hex: String::new(),
            offset: 0,
            chunk_len: 2,
            chunk_sha256: hex(&<[u8; 32]>::from(Sha256::digest([3, 4]))),
            trailing_len: 0,
            trailing_sha256: hex(&<[u8; 32]>::from(Sha256::digest([]))),
            chunk: vec![3, 4],
        };
        let finalize = LoaderMutationEvidence::Finalize {
            coordinate: LoaderCoordinateEvidence {
                slot: 44,
                transaction_index: 0,
                instruction_index: 0,
            },
            archived_outcome: "succeeded",
            signature_count: 1,
            header: HeaderEvidence {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: Vec::new(),
            instruction_accounts: Vec::new(),
            target_is_signer: true,
            target_is_writable: true,
            raw_instruction_len: 4,
            raw_instruction_hex: "01000000".to_owned(),
        };

        let (reconstructed, covered) =
            reconstruct_loader_account(4, 4, &[first, second, finalize]).unwrap();
        assert_eq!(reconstructed, [3, 4, 1, 2]);
        assert!(covered);
    }

    #[test]
    fn validates_complete_sha256_hex() {
        assert_eq!(parse_sha256(&"ab".repeat(32)).unwrap(), [0xab; 32]);
        assert!(parse_sha256("ab").is_err());
        assert!(parse_sha256(&"zz".repeat(32)).is_err());
    }
}
