use crate::format::{
    INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED, IndexMeta, InstructionRecord, NO_STACK_HEIGHT,
    NO_TX_INDEX, OUTER_INSTRUCTION_SENTINEL, ProgramKind, TX_FLAG_HAS_ERROR, TX_FLAG_VERSIONED,
    TX_RECORD_BYTES, TransactionRecord,
};
use anyhow::{Context, Result};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::{InnerInstruction, TransactionStatusMeta},
    metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame},
    node::{Node, decode_node, peek_node_type},
    versioned_transaction::{CompiledInstruction, VersionedMessage, VersionedTransaction},
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

const CAR_BUF: usize = 128 << 20;

#[derive(Debug, Default)]
struct ScanStats {
    scanned_txs: usize,
    skipped_txs: usize,
    skipped_tx_data_continuations: usize,
    skipped_metadata_continuations: usize,
    tx_decode_failures: usize,
    missing_metadata_txs: usize,
    metadata_decode_failures: usize,
    transactions_with_unresolved_program_ids: usize,
    instructions_with_unresolved_program_ids: usize,
    instructions_with_unresolved_accounts: usize,
    matched_txs: usize,
    failed_matched_txs: usize,
    matched_instructions: usize,
    token_instructions: usize,
    token_2022_instructions: usize,
}

pub(crate) fn build_index(car_path: &Path, out_dir: &Path, compressed: bool) -> Result<IndexMeta> {
    let tx_path = out_dir.join("transactions.bin");
    let instruction_path = out_dir.join("instructions.bin");
    let tx_file =
        File::create(&tx_path).with_context(|| format!("create {}", tx_path.display()))?;
    let instruction_file = File::create(&instruction_path)
        .with_context(|| format!("create {}", instruction_path.display()))?;
    let mut tx_writer = BufWriter::new(tx_file);
    let mut instruction_writer = BufWriter::new(instruction_file);

    let mut state = BuildState::default();
    scan_car(
        car_path,
        compressed,
        &mut tx_writer,
        &mut instruction_writer,
        &mut state,
    )?;

    tx_writer.flush().context("flush transactions.bin")?;
    instruction_writer
        .flush()
        .context("flush instructions.bin")?;

    Ok(IndexMeta {
        format_version: 1,
        transaction_record_bytes: TX_RECORD_BYTES,
        car_path: car_path.display().to_string(),
        compressed,
        offset_kind: "car_file_offset".to_string(),
        scanned_txs: state.stats.scanned_txs,
        skipped_txs: state.stats.skipped_txs,
        skipped_tx_data_continuations: state.stats.skipped_tx_data_continuations,
        skipped_metadata_continuations: state.stats.skipped_metadata_continuations,
        tx_decode_failures: state.stats.tx_decode_failures,
        missing_metadata_txs: state.stats.missing_metadata_txs,
        metadata_decode_failures: state.stats.metadata_decode_failures,
        transactions_with_unresolved_program_ids: state
            .stats
            .transactions_with_unresolved_program_ids,
        instructions_with_unresolved_program_ids: state
            .stats
            .instructions_with_unresolved_program_ids,
        instructions_with_unresolved_accounts: state.stats.instructions_with_unresolved_accounts,
        matched_txs: state.stats.matched_txs,
        failed_matched_txs: state.stats.failed_matched_txs,
        matched_instructions: state.stats.matched_instructions,
        token_instructions: state.stats.token_instructions,
        token_2022_instructions: state.stats.token_2022_instructions,
        transactions_file_bytes: state.tx_bytes_written,
        instructions_file_bytes: state.instruction_bytes_written,
    })
}

#[derive(Debug, Default)]
struct BuildState {
    stats: ScanStats,
    tx_bytes_written: u64,
    instruction_bytes_written: u64,
}

fn scan_car(
    path: &Path,
    compressed: bool,
    tx_writer: &mut BufWriter<File>,
    instruction_writer: &mut BufWriter<File>,
    state: &mut BuildState,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;

    if compressed {
        let reader = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(reader)
            .with_context(|| format!("open {}", path.display()))?;
        visit_reader(decoder, tx_writer, instruction_writer, state)
    } else {
        visit_reader(file, tx_writer, instruction_writer, state)
    }
}

fn visit_reader<R: Read>(
    reader: R,
    tx_writer: &mut BufWriter<File>,
    instruction_writer: &mut BufWriter<File>,
    state: &mut BuildState,
) -> Result<()> {
    let mut reader = CarBlockReader::with_capacity(reader, CAR_BUF);
    reader.skip_header().context("skip CAR header")?;

    let mut scratch = Vec::with_capacity(1 << 20);
    let mut zstd = ZstdReusableDecoder::new();
    let mut meta = TransactionStatusMeta::default();

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .context("read CAR entry")?
    {
        if peek_node_type(entry.payload).context("peek node type")? != 0 {
            continue;
        }

        let Node::Transaction(tx) =
            decode_node(entry.payload).context("decode transaction node")?
        else {
            continue;
        };

        state.stats.scanned_txs += 1;

        if tx.data.next.is_some() {
            state.stats.skipped_txs += 1;
            state.stats.skipped_tx_data_continuations += 1;
            continue;
        }
        if tx.metadata.next.is_some() {
            state.stats.skipped_txs += 1;
            state.stats.skipped_metadata_continuations += 1;
            continue;
        }
        if tx.metadata.data.is_empty() {
            state.stats.skipped_txs += 1;
            state.stats.missing_metadata_txs += 1;
            continue;
        }

        let versioned_tx = match wincode::deserialize::<VersionedTransaction<'_>>(tx.data.data) {
            Ok(tx) => tx,
            Err(_) => {
                state.stats.skipped_txs += 1;
                state.stats.tx_decode_failures += 1;
                continue;
            }
        };

        let Some(signature) = versioned_tx.signatures.first().copied() else {
            state.stats.skipped_txs += 1;
            state.stats.tx_decode_failures += 1;
            continue;
        };

        if decode_transaction_status_meta_from_frame(
            tx.slot,
            tx.metadata.data,
            &mut meta,
            &mut zstd,
        )
        .is_err()
        {
            state.stats.skipped_txs += 1;
            state.stats.metadata_decode_failures += 1;
            continue;
        }

        let tx_ordinal = u32::try_from(state.stats.matched_txs)
            .context("matched transaction count exceeds u32")?;
        let tx_match =
            collect_matching_instructions(tx_ordinal, &versioned_tx, &meta, &mut state.stats);
        if tx_match.instructions.is_empty() {
            continue;
        }

        let first_instruction_offset = state.instruction_bytes_written;
        for record in &tx_match.instructions {
            let written = record.encode_into(instruction_writer)?;
            state.instruction_bytes_written = state
                .instruction_bytes_written
                .checked_add(written as u64)
                .context("instruction bytes written overflow")?;
        }

        let entry_size = u32::try_from(entry.total_len)
            .with_context(|| format!("entry too large at offset {}", entry.location.car_offset))?;
        let tx_record = TransactionRecord {
            signature: *signature,
            slot: tx.slot,
            tx_index: tx.index.unwrap_or(NO_TX_INDEX),
            car_offset: entry.location.car_offset,
            car_size: entry_size,
            first_instruction_offset,
            instruction_count: u32::try_from(tx_match.instructions.len())
                .context("instruction count exceeds u32")?,
            flags: tx_match.flags,
            program_mask: tx_match.program_mask,
        };

        tx_writer.write_all(&tx_record.encode())?;
        state.tx_bytes_written = state
            .tx_bytes_written
            .checked_add(TX_RECORD_BYTES as u64)
            .context("transaction bytes written overflow")?;
        state.stats.matched_txs += 1;
        if tx_record.has_error() {
            state.stats.failed_matched_txs += 1;
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct TxMatch {
    instructions: Vec<InstructionRecord>,
    flags: u8,
    program_mask: u8,
}

fn collect_matching_instructions(
    tx_ordinal: u32,
    tx: &VersionedTransaction<'_>,
    meta: &TransactionStatusMeta,
    stats: &mut ScanStats,
) -> TxMatch {
    let mut out = TxMatch::default();
    let resolver = KeyResolver::new(tx, meta);
    if resolver.has_unresolved_program_inputs() {
        stats.transactions_with_unresolved_program_ids += 1;
    }

    let (instructions, is_versioned) = match &tx.message {
        VersionedMessage::Legacy(message) => (&message.instructions[..], false),
        VersionedMessage::V0(message) => (&message.instructions[..], true),
    };

    let mut inner_groups: Vec<Vec<&InnerInstruction>> = vec![Vec::new(); instructions.len()];
    let mut trailing_inner_groups = Vec::new();
    for group in &meta.inner_instructions {
        if let Some(slot) = inner_groups.get_mut(group.index as usize) {
            slot.extend(group.instructions.iter());
        } else {
            trailing_inner_groups.push(group);
        }
    }

    if meta.err.is_some() {
        out.flags |= TX_FLAG_HAS_ERROR;
    }
    if is_versioned {
        out.flags |= TX_FLAG_VERSIONED;
    }

    for (outer_index, instruction) in instructions.iter().enumerate() {
        if let Some(record) = build_outer_record(
            tx_ordinal,
            outer_index as u32,
            instruction,
            &resolver,
            stats,
        ) {
            out.program_mask |= record.program_kind.mask();
            count_program(record.program_kind, stats);
            out.instructions.push(record);
        }

        for (inner_index, inner_instruction) in inner_groups[outer_index].iter().enumerate() {
            if let Some(record) = build_inner_record(
                tx_ordinal,
                outer_index as u32,
                inner_index as u32,
                inner_instruction,
                &resolver,
                stats,
            ) {
                out.program_mask |= record.program_kind.mask();
                count_program(record.program_kind, stats);
                out.instructions.push(record);
            }
        }
    }

    for group in trailing_inner_groups {
        for (inner_index, inner_instruction) in group.instructions.iter().enumerate() {
            if let Some(record) = build_inner_record(
                tx_ordinal,
                group.index,
                inner_index as u32,
                inner_instruction,
                &resolver,
                stats,
            ) {
                out.program_mask |= record.program_kind.mask();
                count_program(record.program_kind, stats);
                out.instructions.push(record);
            }
        }
    }

    stats.matched_instructions += out.instructions.len();
    out
}

fn count_program(program_kind: ProgramKind, stats: &mut ScanStats) {
    match program_kind {
        ProgramKind::Token => stats.token_instructions += 1,
        ProgramKind::Token2022 => stats.token_2022_instructions += 1,
    }
}

fn build_outer_record(
    tx_ordinal: u32,
    outer_instruction_index: u32,
    instruction: &CompiledInstruction,
    resolver: &KeyResolver<'_>,
    stats: &mut ScanStats,
) -> Option<InstructionRecord> {
    build_instruction_record(
        tx_ordinal,
        outer_instruction_index,
        OUTER_INSTRUCTION_SENTINEL,
        NO_STACK_HEIGHT,
        instruction.program_id_index as u32,
        &instruction.accounts,
        &instruction.data,
        resolver,
        stats,
    )
}

fn build_inner_record(
    tx_ordinal: u32,
    outer_instruction_index: u32,
    inner_instruction_index: u32,
    instruction: &InnerInstruction,
    resolver: &KeyResolver<'_>,
    stats: &mut ScanStats,
) -> Option<InstructionRecord> {
    build_instruction_record(
        tx_ordinal,
        outer_instruction_index,
        inner_instruction_index,
        instruction.stack_height.unwrap_or(NO_STACK_HEIGHT),
        instruction.program_id_index,
        &instruction.accounts,
        &instruction.data,
        resolver,
        stats,
    )
}

fn build_instruction_record(
    tx_ordinal: u32,
    outer_instruction_index: u32,
    inner_instruction_index: u32,
    stack_height: u32,
    program_id_index: u32,
    raw_account_indices: &[u8],
    data: &[u8],
    resolver: &KeyResolver<'_>,
    stats: &mut ScanStats,
) -> Option<InstructionRecord> {
    let program_kind = match resolver.program_kind(program_id_index) {
        ResolveProgram::Program(program_kind) => program_kind,
        ResolveProgram::Other => return None,
        ResolveProgram::Unresolved => {
            stats.instructions_with_unresolved_program_ids += 1;
            return None;
        }
    };

    let mut flags = 0u8;
    let mut account_pubkeys = Vec::with_capacity(raw_account_indices.len());
    let mut all_accounts_resolved = true;
    let mut unresolved_accounts_for_instruction = false;

    for &account_index in raw_account_indices {
        match resolver.resolve(account_index as u32) {
            Some(pubkey) => account_pubkeys.push(pubkey),
            None => {
                all_accounts_resolved = false;
                unresolved_accounts_for_instruction = true;
                account_pubkeys.push([0u8; 32]);
            }
        }
    }

    if unresolved_accounts_for_instruction {
        stats.instructions_with_unresolved_accounts += 1;
    }
    if all_accounts_resolved {
        flags |= INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED;
    }

    Some(InstructionRecord {
        tx_ordinal,
        outer_instruction_index,
        inner_instruction_index,
        stack_height,
        program_kind,
        flags,
        raw_account_indices: raw_account_indices.to_vec(),
        account_pubkeys,
        data: data.to_vec(),
    })
}

enum ResolveProgram {
    Program(ProgramKind),
    Other,
    Unresolved,
}

struct KeyResolver<'a> {
    static_keys: &'a [&'a [u8; 32]],
    loaded_writable: &'a [Vec<u8>],
    loaded_readonly: &'a [Vec<u8>],
    expected_loaded_keys: usize,
}

impl<'a> KeyResolver<'a> {
    fn new(tx: &'a VersionedTransaction<'a>, meta: &'a TransactionStatusMeta) -> Self {
        match &tx.message {
            VersionedMessage::Legacy(message) => Self {
                static_keys: &message.account_keys,
                loaded_writable: &[],
                loaded_readonly: &[],
                expected_loaded_keys: 0,
            },
            VersionedMessage::V0(message) => {
                let expected_loaded_keys = message
                    .address_table_lookups
                    .iter()
                    .map(|lookup| lookup.writable_indexes.len() + lookup.readonly_indexes.len())
                    .sum();
                Self {
                    static_keys: &message.account_keys,
                    loaded_writable: &meta.loaded_writable_addresses,
                    loaded_readonly: &meta.loaded_readonly_addresses,
                    expected_loaded_keys,
                }
            }
        }
    }

    fn has_unresolved_program_inputs(&self) -> bool {
        self.expected_loaded_keys != self.loaded_writable.len() + self.loaded_readonly.len()
    }

    fn resolve(&self, index: u32) -> Option<[u8; 32]> {
        let index = index as usize;
        if let Some(key) = self.static_keys.get(index) {
            return Some(**key);
        }

        let loaded_index = index.checked_sub(self.static_keys.len())?;
        if let Some(key) = self.loaded_writable.get(loaded_index) {
            return slice_to_pubkey(key);
        }

        let readonly_index = loaded_index.checked_sub(self.loaded_writable.len())?;
        let key = self.loaded_readonly.get(readonly_index)?;
        slice_to_pubkey(key)
    }

    fn program_kind(&self, program_id_index: u32) -> ResolveProgram {
        match self.resolve(program_id_index) {
            Some(pubkey) => ProgramKind::from_program_pubkey(pubkey)
                .map(ResolveProgram::Program)
                .unwrap_or(ResolveProgram::Other),
            None => ResolveProgram::Unresolved,
        }
    }
}

fn slice_to_pubkey(bytes: &[u8]) -> Option<[u8; 32]> {
    (bytes.len() == 32).then(|| {
        let mut out = [0u8; 32];
        out.copy_from_slice(bytes);
        out
    })
}

#[cfg(test)]
mod tests {
    use super::{collect_matching_instructions, slice_to_pubkey};
    use crate::format::{
        INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED, OUTER_INSTRUCTION_SENTINEL, ProgramKind,
        token_2022_program_id, token_program_id,
    };
    use of_car_reader::{
        confirmed_block::{InnerInstruction, InnerInstructions, TransactionStatusMeta},
        versioned_transaction::{
            CompiledInstruction, LegacyMessage, MessageAddressTableLookup, MessageHeader,
            V0Message, VersionedMessage, VersionedTransaction,
        },
    };

    #[test]
    fn collects_outer_and_inner_matches_in_order() {
        let payer = &[1u8; 32];
        let token = &token_program_id();
        let owner = &[3u8; 32];
        let token_2022 = &token_2022_program_id();
        let account = &[5u8; 32];

        let tx = VersionedTransaction {
            signatures: vec![&[9u8; 64]],
            message: VersionedMessage::Legacy(LegacyMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![payer, token, owner, token_2022, account],
                recent_blockhash: &[7u8; 32],
                instructions: vec![
                    CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![2, 4],
                        data: vec![1, 2, 3],
                    },
                    CompiledInstruction {
                        program_id_index: 0,
                        accounts: vec![],
                        data: vec![],
                    },
                ],
            }),
        };

        let meta = TransactionStatusMeta {
            inner_instructions: vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    program_id_index: 3,
                    accounts: vec![4],
                    data: vec![8],
                    stack_height: Some(2),
                }],
            }],
            ..TransactionStatusMeta::default()
        };

        let mut stats = Default::default();
        let tx_match = collect_matching_instructions(0, &tx, &meta, &mut stats);

        assert_eq!(tx_match.instructions.len(), 2);
        assert_eq!(tx_match.instructions[0].program_kind, ProgramKind::Token);
        assert_eq!(
            tx_match.instructions[0].inner_instruction_index,
            OUTER_INSTRUCTION_SENTINEL
        );
        assert!(tx_match.instructions[0].flags & INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED != 0);
        assert_eq!(
            tx_match.instructions[1].program_kind,
            ProgramKind::Token2022
        );
        assert_eq!(tx_match.instructions[1].stack_height, 2);
    }

    #[test]
    fn resolves_loaded_addresses_for_v0_programs() {
        let payer = &[1u8; 32];
        let owner = &[2u8; 32];
        let lookup_account = &[3u8; 32];
        let loaded_program = token_program_id().to_vec();

        let tx = VersionedTransaction {
            signatures: vec![&[9u8; 64]],
            message: VersionedMessage::V0(V0Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![payer, owner],
                recent_blockhash: &[7u8; 32],
                instructions: vec![CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 1],
                    data: vec![10],
                }],
                address_table_lookups: vec![MessageAddressTableLookup {
                    account_key: lookup_account,
                    writable_indexes: vec![1],
                    readonly_indexes: vec![],
                }],
            }),
        };

        let meta = TransactionStatusMeta {
            loaded_writable_addresses: vec![loaded_program],
            ..TransactionStatusMeta::default()
        };

        let mut stats = Default::default();
        let tx_match = collect_matching_instructions(0, &tx, &meta, &mut stats);
        assert_eq!(tx_match.instructions.len(), 1);
        assert_eq!(tx_match.instructions[0].program_kind, ProgramKind::Token);
    }

    #[test]
    fn rejects_invalid_pubkey_slices() {
        assert!(slice_to_pubkey(&[1u8; 31]).is_none());
        assert_eq!(slice_to_pubkey(&[2u8; 32]), Some([2u8; 32]));
    }
}
