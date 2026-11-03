use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use blockzilla_archive_v2::{
    write_archive_v2_hot_block_index, ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader,
    ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotRewards,
    ArchiveV2HotTxRow, ArchiveV2HotV0Message, ArchiveV2HotV1Message, WincodeArchiveV2PohRecord,
    WincodeArchiveV2ShreddingRecord, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_SHREDDING_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_LOGS,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
};
use blockzilla_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
    CompactPohEntry, CompactPubkey, CompactReturnData, CompactReward, CompactShredding,
    CompactTokenBalance, CompactTransactionConfig, OwnedCompactAddressTableLookup,
    OwnedCompactRecentBlockhash,
};
use blockzilla_primitives::{wincode_leb128_config, WincodeLeb128FramedWriter};
use blockzilla_registry::RawPubkeyCompactor;
use blockzilla_compact::parse_logs_with_compactor;

use crate::check::Inspected;
use crate::markers::footer_bank_hash;
use crate::source::{
    LedgerView, SlotData, SourceInnerGroup, SourceMessage, SourceMessageBody, SourceReward,
    SourceStatus, SourceTokenBalance, SourceTx, SourceV1Message,
};

pub(crate) const ZSTD_LEVEL: i32 = 3;
const INDEX_FLAGS: u32 = 0;
const VOTE_HASH_ROW_LEN: usize = 65;

pub(crate) fn write_archive(output: &Path, view: &LedgerView, inspected: &Inspected) -> Result<()> {
    fs::create_dir_all(output)
        .with_context(|| format!("create {}", output.display()))?;

    let blocks_tmp = output.join(format!("{ARCHIVE_V2_BLOCKS_FILE}.tmp"));
    let signatures_tmp = output.join(format!("{ARCHIVE_V2_SIGNATURES_FILE}.tmp"));
    let blockhash_tmp = output.join(format!("{ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE}.tmp"));
    let vote_tmp = output.join(format!("{ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE}.tmp"));
    let poh_tmp = output.join(format!("{ARCHIVE_V2_POH_FILE}.tmp"));
    let shredding_tmp = output.join(format!("{ARCHIVE_V2_SHREDDING_FILE}.tmp"));
    let temps = [
        &blocks_tmp,
        &signatures_tmp,
        &blockhash_tmp,
        &vote_tmp,
        &poh_tmp,
        &shredding_tmp,
    ];

    let write_result = write_archive_files(
        output,
        view,
        inspected,
        &blocks_tmp,
        &signatures_tmp,
        &blockhash_tmp,
        &vote_tmp,
        &poh_tmp,
        &shredding_tmp,
    );
    if write_result.is_err() {
        for path in temps {
            let _ = fs::remove_file(path);
        }
    }
    write_result
}

fn write_archive_files(
    output: &Path,
    view: &LedgerView,
    inspected: &Inspected,
    blocks_tmp: &Path,
    signatures_tmp: &Path,
    blockhash_tmp: &Path,
    vote_tmp: &Path,
    poh_tmp: &Path,
    shredding_tmp: &Path,
) -> Result<()> {
    let mut blocks = BufWriter::new(
        File::create(blocks_tmp).with_context(|| format!("create {}", blocks_tmp.display()))?,
    );
    let mut signatures = Vec::new();
    let mut blockhashes = Vec::with_capacity(32 * (inspected.block_slots.len() + 1));
    blockhashes.extend(inspected.boundary_hash);
    let mut vote_hashes = Vec::with_capacity(VOTE_HASH_ROW_LEN * inspected.block_slots.len());
    let mut poh = WincodeLeb128FramedWriter::new(BufWriter::new(
        File::create(poh_tmp).with_context(|| format!("create {}", poh_tmp.display()))?,
    ));
    let mut shredding = WincodeLeb128FramedWriter::new(BufWriter::new(
        File::create(shredding_tmp)
            .with_context(|| format!("create {}", shredding_tmp.display()))?,
    ));

    let mut rows = Vec::with_capacity(inspected.block_slots.len());
    let mut compressed_offset = 0u64;
    let mut next_tx_ordinal = 0u64;
    let mut next_signature_ordinal = 0u64;

    for (ordinal, slot) in inspected.block_slots.iter().copied().enumerate() {
        let block = &view.slots[&slot];
        let block_id = u32::try_from(ordinal).context("archive block id exceeds u32")?;
        let uncompressed = encode_hot_block(block, block_id)?;
        let compressed = zstd::bulk::compress(&uncompressed, ZSTD_LEVEL)
            .with_context(|| format!("compress slot {slot}"))?;
        let compressed_len = u32::try_from(compressed.len()).context("compressed block exceeds u32")?;
        let uncompressed_len =
            u32::try_from(uncompressed.len()).context("block bytes exceed u32")?;
        blocks
            .write_all(&compressed)
            .with_context(|| format!("write slot {slot}"))?;

        let signature_count = append_signatures(&mut signatures, block)?;
        blockhashes.extend(block.blockhash);
        vote_hashes.extend(vote_hash_row(block)?);
        poh.write(&poh_record(block, block_id))
            .with_context(|| format!("write PoH for slot {slot}"))?;
        shredding
            .write(&shredding_record(block, block_id))
            .with_context(|| format!("write shredding for slot {slot}"))?;

        let tx_count = u32::try_from(block.transactions.len()).context("transaction count exceeds u32")?;
        rows.push(ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset,
            compressed_len,
            uncompressed_len,
            tx_count,
            first_tx_ordinal: next_tx_ordinal,
            first_signature_ordinal: next_signature_ordinal,
            signature_count,
        });
        compressed_offset = compressed_offset
            .checked_add(u64::from(compressed_len))
            .context("compressed archive exceeds u64")?;
        next_tx_ordinal = next_tx_ordinal
            .checked_add(u64::from(tx_count))
            .context("transaction ordinal exceeds u64")?;
        next_signature_ordinal = next_signature_ordinal
            .checked_add(u64::from(signature_count))
            .context("signature ordinal exceeds u64")?;
    }

    blocks
        .flush()
        .with_context(|| format!("flush {}", blocks_tmp.display()))?;
    drop(blocks);
    poh.flush().context("flush PoH sidecar")?;
    let poh_writer = poh.into_inner();
    poh_writer
        .into_inner()
        .map_err(|err| anyhow::anyhow!(err))
        .context("flush PoH sidecar")?;
    shredding.flush().context("flush shredding sidecar")?;
    let shredding_writer = shredding.into_inner();
    shredding_writer
        .into_inner()
        .map_err(|err| anyhow::anyhow!(err))
        .context("flush shredding sidecar")?;

    fs::write(signatures_tmp, &signatures)
        .with_context(|| format!("write {}", signatures_tmp.display()))?;
    fs::write(blockhash_tmp, &blockhashes)
        .with_context(|| format!("write {}", blockhash_tmp.display()))?;
    fs::write(vote_tmp, &vote_hashes)
        .with_context(|| format!("write {}", vote_tmp.display()))?;

    rename_finished(blocks_tmp, &output.join(ARCHIVE_V2_BLOCKS_FILE))?;
    rename_finished(signatures_tmp, &output.join(ARCHIVE_V2_SIGNATURES_FILE))?;
    rename_finished(
        blockhash_tmp,
        &output.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
    )?;
    rename_finished(vote_tmp, &output.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE))?;
    rename_finished(poh_tmp, &output.join(ARCHIVE_V2_POH_FILE))?;
    rename_finished(shredding_tmp, &output.join(ARCHIVE_V2_SHREDDING_FILE))?;
    write_archive_v2_hot_block_index(
        &output.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        compressed_offset,
        ZSTD_LEVEL,
        INDEX_FLAGS,
        &rows,
    )?;
    Ok(())
}

fn rename_finished(from: &Path, to: &Path) -> Result<()> {
    fs::rename(from, to)
        .with_context(|| format!("rename {} to {}", from.display(), to.display()))
}

pub(crate) fn encode_hot_block(block: &SlotData, block_id: u32) -> Result<Vec<u8>> {
    let entry_txs = block.entries.iter().try_fold(0u32, |total, entry| {
        total
            .checked_add(entry.tx_count)
            .context("entry transaction count overflows")
    })?;
    if usize::from(entry_txs) != block.transactions.len() {
        bail!(
            "slot {} entry transaction count {entry_txs} does not match {}",
            block.slot,
            block.transactions.len()
        );
    }

    let mut message_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut rows = Vec::with_capacity(block.transactions.len());
    for (tx_index, tx) in block.transactions.iter().enumerate() {
        let status = tx.status.as_ref().with_context(|| {
            format!("slot {} transaction {tx_index} is missing status", block.slot)
        })?;
        let row = encode_tx_row(
            tx,
            status,
            u32::try_from(tx_index).context("transaction index exceeds u32")?,
            &mut message_bytes,
            &mut metadata_bytes,
        )?;
        rows.push(row);
    }

    let rewards = if block.rewards.is_empty() && block.num_partitions.is_none() {
        None
    } else {
        Some(ArchiveV2HotRewards {
            num_partitions: block.num_partitions,
            decoded: block.rewards.iter().map(compact_reward).collect(),
        })
    };
    let blob = ArchiveV2HotBlockBlob {
        header: ArchiveV2HotBlockHeader {
            slot: block.slot,
            parent_slot: block
                .parent_slot
                .with_context(|| format!("slot {} has no parent slot", block.slot))?,
            blockhash_id: block_id
                .checked_add(1)
                .context("blockhash id exceeds u32")?,
            previous_blockhash_id: block_id,
            block_time: block.block_time,
            block_height: block.block_height,
            rewards,
        },
        tx_count: u32::try_from(block.transactions.len()).context("transaction count exceeds u32")?,
        tx_rows: rows,
        message_bytes,
        metadata_bytes,
        consensus_block_id: block.consensus_block_id,
        block_markers: block.markers.clone(),
    };
    wincode::config::serialize(&blob, wincode_leb128_config()).context("serialize hot block")
}

fn encode_tx_row(
    tx: &SourceTx,
    status: &SourceStatus,
    tx_index: u32,
    message_bytes: &mut Vec<u8>,
    metadata_bytes: &mut Vec<u8>,
) -> Result<ArchiveV2HotTxRow> {
    let (payload, mut flags) = encode_message(&tx.message)?;
    flags |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
    if status.err.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
    }
    if status.return_data.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
    }
    if status.logs.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
    }
    if status.inner_instructions.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
    }
    if !status.pre_token_balances.is_empty() || !status.post_token_balances.is_empty() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    }
    if !status.loaded_writable.is_empty() || !status.loaded_readonly.is_empty() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
    }

    let message = wincode::config::serialize(&payload, wincode_leb128_config())
        .context("serialize transaction message")?;
    let metadata = wincode::config::serialize(&compact_meta(status), wincode_leb128_config())
        .context("serialize transaction metadata")?;
    let message_offset = u32::try_from(message_bytes.len()).context("message region exceeds u32")?;
    let message_len = u32::try_from(message.len()).context("message exceeds u32")?;
    let metadata_offset =
        u32::try_from(metadata_bytes.len()).context("metadata region exceeds u32")?;
    let metadata_len = u32::try_from(metadata.len()).context("metadata exceeds u32")?;
    message_bytes.extend(message);
    metadata_bytes.extend(metadata);
    let signature_count = u8::try_from(tx.signatures.len())
        .context("transaction has more than 255 signatures")?;

    Ok(ArchiveV2HotTxRow {
        tx_index,
        flags,
        message_offset,
        message_len,
        metadata_offset,
        metadata_len,
        signature_count,
        reserved: [0; 3],
    })
}

fn encode_message(message: &SourceMessage) -> Result<(ArchiveV2HotMessagePayload, u32)> {
    match message {
        SourceMessage::Legacy(body) => Ok((
            ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: compact_header(body),
                account_keys: raw_keys(&body.account_keys),
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce(body.recent_blockhash),
                instructions: raw_instructions(&body.instructions),
            }),
            0,
        )),
        SourceMessage::V0(body) => Ok((
            ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: compact_header(body),
                account_keys: raw_keys(&body.account_keys),
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce(body.recent_blockhash),
                instructions: raw_instructions(&body.instructions),
                address_table_lookups: body
                    .lookups
                    .iter()
                    .map(|lookup| OwnedCompactAddressTableLookup {
                        account_key: CompactPubkey::raw(lookup.account_key),
                        writable_indexes: lookup.writable_indexes.clone(),
                        readonly_indexes: lookup.readonly_indexes.clone(),
                    })
                    .collect(),
            }),
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
        )),
        SourceMessage::V1(body) => Ok((
            ArchiveV2HotMessagePayload::V1(ArchiveV2HotV1Message {
                header: compact_header_v1(body),
                config: CompactTransactionConfig {
                    priority_fee: body.config.priority_fee,
                    compute_unit_limit: body.config.compute_unit_limit,
                    loaded_accounts_data_size_limit: body.config.loaded_accounts_data_size_limit,
                    heap_size: body.config.heap_size,
                },
                account_keys: raw_keys(&body.account_keys),
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce(body.recent_blockhash),
                instructions: raw_instructions(&body.instructions),
            }),
            0,
        )),
    }
}

fn compact_header(body: &SourceMessageBody) -> CompactMessageHeader {
    CompactMessageHeader {
        num_required_signatures: body.header.num_required_signatures,
        num_readonly_signed_accounts: body.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: body.header.num_readonly_unsigned_accounts,
    }
}

fn compact_header_v1(body: &SourceV1Message) -> CompactMessageHeader {
    CompactMessageHeader {
        num_required_signatures: body.header.num_required_signatures,
        num_readonly_signed_accounts: body.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: body.header.num_readonly_unsigned_accounts,
    }
}

fn raw_keys(keys: &[[u8; 32]]) -> Vec<CompactPubkey> {
    keys.iter().copied().map(CompactPubkey::raw).collect()
}

fn raw_instructions(instructions: &[crate::source::SourceInstruction]) -> Vec<ArchiveV2HotInstruction> {
    instructions
        .iter()
        .map(|instruction| ArchiveV2HotInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.clone(),
            data: ArchiveV2HotInstructionData::Raw(instruction.data.clone()),
        })
        .collect()
}

fn compact_meta(status: &SourceStatus) -> CompactMetaV1 {
    CompactMetaV1 {
        err: status.err.clone(),
        fee: status.fee,
        pre_balances: status.pre_balances.clone(),
        post_balances: status.post_balances.clone(),
        inner_instructions: status
            .inner_instructions
            .as_ref()
            .map(|groups| groups.iter().map(compact_inner_group).collect()),
        logs: status
            .logs
            .as_ref()
            .map(|lines| parse_logs_with_compactor(lines, &RawPubkeyCompactor)),
        pre_token_balances: status
            .pre_token_balances
            .iter()
            .map(compact_token_balance)
            .collect(),
        post_token_balances: status
            .post_token_balances
            .iter()
            .map(compact_token_balance)
            .collect(),
        rewards: status.rewards.iter().map(compact_reward).collect(),
        loaded_writable_addresses: raw_keys(&status.loaded_writable),
        loaded_readonly_addresses: raw_keys(&status.loaded_readonly),
        return_data: status.return_data.as_ref().map(|data| CompactReturnData {
            program_id: CompactPubkey::raw(data.program_id),
            data: data.data.clone(),
        }),
        compute_units_consumed: status.compute_units_consumed,
        cost_units: status.cost_units,
    }
}

fn compact_inner_group(group: &SourceInnerGroup) -> CompactInnerInstructions {
    CompactInnerInstructions {
        index: group.index,
        instructions: group
            .instructions
            .iter()
            .map(|instruction| CompactInnerInstruction {
                program_id_index: instruction.program_id_index,
                accounts: instruction.accounts.clone(),
                data: instruction.data.clone(),
                stack_height: instruction.stack_height,
            })
            .collect(),
    }
}

fn compact_token_balance(balance: &SourceTokenBalance) -> CompactTokenBalance {
    CompactTokenBalance {
        account_index: balance.account_index,
        mint: Some(CompactPubkey::raw(balance.mint)),
        owner: balance.owner.map(CompactPubkey::raw),
        program_id: balance.program_id.map(CompactPubkey::raw),
        amount: balance.amount,
        decimals: balance.decimals,
    }
}

fn compact_reward(reward: &SourceReward) -> CompactReward {
    CompactReward {
        pubkey: CompactPubkey::raw(reward.pubkey),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission,
    }
}

fn append_signatures(out: &mut Vec<u8>, block: &SlotData) -> Result<u32> {
    let mut count = 0u32;
    for tx in &block.transactions {
        for signature in &tx.signatures {
            out.extend(signature);
            count = count.checked_add(1).context("signature count exceeds u32")?;
        }
    }
    Ok(count)
}

fn poh_record(block: &SlotData, block_id: u32) -> WincodeArchiveV2PohRecord {
    WincodeArchiveV2PohRecord {
        block_id,
        slot: block.slot,
        entries: block
            .entries
            .iter()
            .map(|entry| CompactPohEntry {
                num_hashes: entry.num_hashes,
                hash: entry.hash,
                tx_count: entry.tx_count,
                signature_count: entry.signature_count,
            })
            .collect(),
    }
}

pub(crate) fn shredding_record(block: &SlotData, block_id: u32) -> WincodeArchiveV2ShreddingRecord {
    WincodeArchiveV2ShreddingRecord {
        block_id,
        slot: block.slot,
        shredding: block
            .shredding
            .iter()
            .map(|group| CompactShredding {
                entry_end_idx: group.entry_end_idx,
                shred_end_idx: group.shred_end_idx,
            })
            .collect(),
    }
}

fn vote_hash_row(block: &SlotData) -> Result<[u8; VOTE_HASH_ROW_LEN]> {
    let bank_hash = footer_bank_hash(&block.markers)
        .with_context(|| format!("slot {} footer does not parse", block.slot))?;
    let mut row = [0u8; VOTE_HASH_ROW_LEN];
    row[0] = u8::from(bank_hash.is_some()) | (u8::from(block.consensus_block_id.is_some()) << 1);
    if let Some(hash) = bank_hash {
        row[1..33].copy_from_slice(&hash);
    }
    if let Some(hash) = block.consensus_block_id {
        row[33..65].copy_from_slice(&hash);
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::inspect;
    use crate::source::{test_block, LedgerView, SourceShredGroup, REWARD_SLOT_DISTANCE};
    use blockzilla_archive_v2::deserialize_archive_v2_hot_block_blob;
    use blockzilla_primitives::WincodeLeb128FramedReader;
    use of_car_reader::blockmarker::parent_consensus_block_id;
    use std::io::Cursor;

    fn header_marker(parent_slot: u64, parent_id: [u8; 32]) -> Vec<u8> {
        let mut payload = vec![1u8];
        payload.extend_from_slice(&parent_slot.to_le_bytes());
        payload.extend_from_slice(&parent_id);
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(1);
        out.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
        out.extend(payload);
        out
    }

    #[test]
    fn empty_tower_tail_is_two_zero_bytes_and_has_no_consensus_id() {
        let block = test_block(10, 9, [2; 32], [1; 32]);
        let bytes = encode_hot_block(&block, 0).unwrap();
        assert_eq!(&bytes[bytes.len() - 2..], &[0, 0]);
        let decoded = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        assert_eq!(decoded.consensus_block_id, None);
        assert!(decoded.block_markers.is_empty());
        assert_eq!(decoded.header.previous_blockhash_id, 0);
        assert_eq!(decoded.header.blockhash_id, 1);
    }

    #[test]
    fn header_marker_fills_the_consensus_id_and_parent_id() {
        let parent_id = [7; 32];
        let consensus_id = [9; 32];
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.consensus_block_id = Some(consensus_id);
        block.markers = vec![header_marker(9, parent_id)];

        let bytes = encode_hot_block(&block, 0).unwrap();
        let decoded = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        assert_eq!(decoded.consensus_block_id, Some(consensus_id));
        assert_eq!(
            parent_consensus_block_id(&decoded.block_markers),
            Some(parent_id)
        );
    }

    #[test]
    fn two_shredding_groups_become_two_rows() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.shredding = vec![
            SourceShredGroup {
                entry_end_idx: 4,
                shred_end_idx: 31,
            },
            SourceShredGroup {
                entry_end_idx: 11,
                shred_end_idx: 95,
            },
        ];
        let record = shredding_record(&block, 0);
        assert_eq!(record.shredding.len(), 2);
        assert_eq!(record.shredding[0].entry_end_idx, 4);
        assert_eq!(record.shredding[0].shred_end_idx, 31);
        assert_eq!(record.shredding[1].entry_end_idx, 11);
        assert_eq!(record.shredding[1].shred_end_idx, 95);
    }

    #[test]
    fn a_proven_skip_writes_no_blob() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let second = test_block(12, 10, [5; 32], [4; 32]);
        let mut carrier = test_block(11 + REWARD_SLOT_DISTANCE, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_footer(11)];
        let view = LedgerView {
            start_slot: 10,
            end_slot: 12,
            slots: [first, second, carrier]
                .into_iter()
                .map(|slot| (slot.slot, slot))
                .collect(),
        };
        let inspected = inspect(&view).unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_archive(dir.path(), &view, &inspected).unwrap();

        let index = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(&index[..8], b"BZV2HIX1");
        assert_eq!(u64::from_le_bytes(index[12..20].try_into().unwrap()), 2);
        assert_eq!(i32::from_le_bytes(index[28..32].try_into().unwrap()), ZSTD_LEVEL);
        assert_eq!(u32::from_le_bytes(index[32..36].try_into().unwrap()), 0);
        let row_len = 52;
        let slots: Vec<u64> = index[36..]
            .chunks_exact(row_len)
            .map(|row| u64::from_le_bytes(row[4..12].try_into().unwrap()))
            .collect();
        assert_eq!(slots, vec![10, 12]);

        let hashes = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)).unwrap();
        assert_eq!(hashes.len(), 96);
        assert_eq!(&hashes[..32], &[3; 32]);
        assert_eq!(&hashes[32..64], &[4; 32]);
        assert_eq!(&hashes[64..], &[5; 32]);

        let mut reader = WincodeLeb128FramedReader::new(Cursor::new(
            std::fs::read(dir.path().join(ARCHIVE_V2_SHREDDING_FILE)).unwrap(),
        ));
        let first_record = reader
            .read::<WincodeArchiveV2ShreddingRecord>()
            .unwrap()
            .unwrap()
            .1;
        let second_record = reader
            .read::<WincodeArchiveV2ShreddingRecord>()
            .unwrap()
            .unwrap()
            .1;
        assert!(reader
            .read::<WincodeArchiveV2ShreddingRecord>()
            .unwrap()
            .is_none());
        assert_eq!(first_record.slot, 10);
        assert_eq!(second_record.slot, 12);
    }

    fn skip_footer(skipped_slot: u64) -> Vec<u8> {
        let mut certificates = vec![0, 1];
        certificates.extend_from_slice(&skipped_slot.to_le_bytes());
        certificates.extend(std::iter::repeat_n(7u8, 96));
        certificates.push(0);
        certificates.push(0);
        let mut payload = vec![1u8];
        payload.extend(std::iter::repeat_n(0xabu8, 32));
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.push(0);
        payload.extend(certificates);
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(0);
        out.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
        out.extend(payload);
        out
    }
}
