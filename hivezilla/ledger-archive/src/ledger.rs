use std::io::{self, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use blockzilla_compact::{CompactInstructionError, CompactTransactionError};
use solana_entry::block_component::BlockComponent;
use solana_instruction_error::InstructionError;
use solana_ledger::blockstore::Blockstore;
use solana_ledger::blockstore::error::BlockstoreError;
use solana_ledger::blockstore_meta::BlockLocation;
use solana_ledger::blockstore_options::{
    AccessType, BLOCKSTORE_DIRECTORY_ROCKS_LEVEL, BlockstoreOptions,
};
use solana_message::VersionedMessage;
use solana_reward_info::RewardType;
use solana_transaction_error::TransactionError;
use solana_transaction_status::{Reward, TransactionStatusMeta};

use crate::check::{
    ChainRelation, Inspected, PreviousBlock, accept_block_against, classify_abandoned_fork,
    classify_hole, parent_chain_relation,
};
use crate::source::{
    REWARD_SLOT_DISTANCE, RawComponent, SlotData, SourceEntry, SourceHeader, SourceInnerGroup,
    SourceInnerInstruction, SourceInstruction, SourceLookup, SourceMessage, SourceMessageBody,
    SourceReturnData, SourceReward, SourceStatus, SourceTokenBalance, SourceTx, SourceTxConfig,
    SourceV1Message,
};

/// Open a ledger only in `AccessType::ReadOnly`.
///
/// `Blockstore::open` creates a missing ledger directory. This function
/// returns before that call when `rocksdb` is not already a directory.
pub(crate) fn read_only_options(ledger: &Path) -> Result<BlockstoreOptions> {
    let rocks = ledger.join(BLOCKSTORE_DIRECTORY_ROCKS_LEVEL);
    if !rocks.is_dir() {
        bail!("rocksdb directory {} does not exist", rocks.display());
    }
    let mut options = BlockstoreOptions::default();
    options.access_type = AccessType::ReadOnly;
    reject_non_readonly(&options.access_type)?;
    Ok(options)
}

pub(crate) fn reject_non_readonly(access: &AccessType) -> Result<()> {
    if *access == AccessType::ReadOnly {
        Ok(())
    } else {
        bail!("ledger archive opens AccessType::ReadOnly only");
    }
}

pub(crate) fn open_read_only(ledger: &Path) -> Result<Blockstore> {
    let options = read_only_options(ledger)?;
    Blockstore::open_with_options(ledger, options)
        .with_context(|| format!("open ledger {}", ledger.display()))
}

/// Read each slot once. A rooted full block is checked, passed to `on_block`,
/// then dropped. A non-root slot is left out when the next root jumps over it
/// and slot N+8 has a skip certificate. A hole is proved from the footer of
/// slot N+8.
///
/// When that footer belongs to a later block in the window, the block is kept
/// until the scan reaches it, so its shreds are not read a second time.
pub(crate) fn scan_range<F>(
    blockstore: &Blockstore,
    start_slot: u64,
    end_slot: u64,
    mut on_block: F,
) -> Result<Inspected>
where
    F: FnMut(&SlotData) -> Result<()>,
{
    if start_slot > end_slot {
        bail!("start slot {start_slot} is after end slot {end_slot}");
    }
    progress(format!("reading slots {start_slot} to {end_slot}"));
    let mut ahead = std::collections::BTreeMap::<u64, SlotData>::new();
    let mut block_slots = Vec::new();
    let mut skips = Vec::new();
    let mut forks = Vec::new();
    let mut previous = None;
    let mut boundary_hash = None;
    let mut scanned = 0u64;
    for slot in start_slot..=end_slot {
        scanned += 1;
        if let Some(data) = ahead.remove(&slot) {
            accept_one(
                data,
                start_slot,
                end_slot,
                &mut previous,
                &mut boundary_hash,
                &mut block_slots,
                &mut on_block,
            )?;
        } else if let Some(meta) = blockstore
            .meta(slot)
            .with_context(|| format!("read SlotMeta for slot {slot}"))?
        {
            if !blockstore.is_root(slot) {
                leave_out_abandoned_fork(blockstore, slot, end_slot, &mut ahead, &mut forks)?;
            } else if !meta.is_full() {
                bail!("slot {slot} is rooted and is not full");
            } else {
                let data = load_rooted_block(blockstore, slot)?;
                accept_one(
                    data,
                    start_slot,
                    end_slot,
                    &mut previous,
                    &mut boundary_hash,
                    &mut block_slots,
                    &mut on_block,
                )?;
            }
        } else {
            let markers = proof_markers(blockstore, slot, end_slot, &mut ahead)?;
            classify_hole(slot, markers.as_deref(), &mut skips)?;
        }
        if scanned % 1000 == 0 {
            progress(format!(
                "read {scanned} slots, latest slot {slot}, {} blocks accepted",
                block_slots.len()
            ));
        }
    }
    progress(format!(
        "read finished: {scanned} slots scanned, {} blocks accepted",
        block_slots.len()
    ));
    let Some(boundary_hash) = boundary_hash else {
        bail!("slots {start_slot} to {end_slot} have no block");
    };
    Ok(Inspected {
        block_slots,
        skips,
        forks,
        boundary_hash,
    })
}

fn leave_out_abandoned_fork(
    blockstore: &Blockstore,
    slot: u64,
    end_slot: u64,
    ahead: &mut std::collections::BTreeMap<u64, SlotData>,
    forks: &mut Vec<u64>,
) -> Result<()> {
    let relation = rooted_chain_relation(blockstore, slot)?;
    let markers = match relation {
        ChainRelation::JumpsOver { .. } => proof_markers(blockstore, slot, end_slot, ahead)?,
        ChainRelation::DescendsFrom { .. } => None,
    };
    let next_root = match relation {
        ChainRelation::JumpsOver { next_root } => Some(next_root),
        ChainRelation::DescendsFrom { .. } => None,
    };
    classify_abandoned_fork(slot, relation, markers.as_deref(), forks)?;
    if let Some(next_root) = next_root {
        progress(format!(
            "slot {slot} left out, rooted chain jumps to {next_root}"
        ));
    }
    Ok(())
}

fn rooted_chain_relation(blockstore: &Blockstore, slot: u64) -> Result<ChainRelation> {
    let start = slot.checked_add(1).context("slot + 1 overflows")?;
    let Some(next_root) = blockstore
        .rooted_slot_iterator(start)
        .with_context(|| format!("read roots after slot {slot}"))?
        .next()
    else {
        bail!("slot {slot} has data and is not rooted");
    };
    parent_chain_relation(slot, next_root, |cursor| {
        let meta = blockstore
            .meta(cursor)
            .with_context(|| format!("read SlotMeta for slot {cursor}"))?;
        let Some(meta) = meta else {
            bail!("slot {cursor} has no SlotMeta");
        };
        Ok(meta.parent_slot)
    })
}

fn accept_one<F>(
    data: SlotData,
    start_slot: u64,
    end_slot: u64,
    previous: &mut Option<PreviousBlock>,
    boundary_hash: &mut Option<[u8; 32]>,
    block_slots: &mut Vec<u64>,
    on_block: &mut F,
) -> Result<()>
where
    F: FnMut(&SlotData) -> Result<()>,
{
    accept_block_against(&data, start_slot, end_slot, previous.as_ref())?;
    if boundary_hash.is_none() {
        *boundary_hash = data.previous_blockhash;
    }
    on_block(&data)?;
    *previous = Some(PreviousBlock {
        slot: data.slot,
        blockhash: data.blockhash,
    });
    block_slots.push(data.slot);
    Ok(())
}

fn proof_markers(
    blockstore: &Blockstore,
    slot: u64,
    end_slot: u64,
    ahead: &mut std::collections::BTreeMap<u64, SlotData>,
) -> Result<Option<Vec<Vec<u8>>>> {
    let carrier = slot
        .checked_add(REWARD_SLOT_DISTANCE)
        .context("slot + 8 overflows")?;
    if let Some(data) = ahead.get(&carrier) {
        return Ok(Some(data.markers.clone()));
    }
    let Some(meta) = blockstore
        .meta(carrier)
        .with_context(|| format!("read SlotMeta for slot {carrier}"))?
    else {
        return Ok(None);
    };
    let is_root = blockstore.is_root(carrier);
    if carrier > end_slot {
        return Ok(Some(read_markers(blockstore, carrier)?));
    }
    if is_root && meta.is_full() {
        let data = load_rooted_block(blockstore, carrier)?;
        let markers = data.markers.clone();
        ahead.insert(carrier, data);
        return Ok(Some(markers));
    }
    Ok(Some(Vec::new()))
}

fn progress(message: impl std::fmt::Display) {
    if cfg!(test) {
        return;
    }
    eprintln!("{message}");
    let _ = io::stderr().flush();
}

fn load_rooted_block(blockstore: &Blockstore, slot: u64) -> Result<SlotData> {
    let (components, ranges, is_full) = blockstore
        .get_slot_components_with_shred_info(slot, 0, false)
        .with_context(|| format!("read shreds for slot {slot}"))?;
    if !is_full {
        bail!("slot {slot} is not full");
    }
    if components.len() != ranges.len() {
        bail!("slot {slot} component count does not match its shred ranges");
    }

    let mut markers = Vec::new();
    let mut raw_entries = Vec::new();
    let mut raw_components = Vec::with_capacity(components.len());
    for (component, range) in components.into_iter().zip(ranges) {
        let entry_count = match component {
            BlockComponent::EntryBatch(entries) => {
                let count = u64::try_from(entries.len()).context("entry batch exceeds u64")?;
                raw_entries.extend(entries);
                count
            }
            BlockComponent::BlockMarker(marker) => {
                let bytes = wincode::serialize(&marker)
                    .with_context(|| format!("serialize a block marker in slot {slot}"))?;
                markers.push(bytes);
                0
            }
        };
        raw_components.push(RawComponent {
            entry_count,
            shred_range_end: range.end,
        });
    }
    let shredding = crate::source::shred_groups(&raw_components)
        .with_context(|| format!("shred ranges for slot {slot}"))?;

    let block = match blockstore.get_rooted_block(slot, true) {
        Ok(block) => block,
        Err(BlockstoreError::ParentEntriesUnavailable) => {
            bail!("slot {slot} is missing parent entries");
        }
        Err(BlockstoreError::MissingTransactionMetadata) => {
            bail!("slot {slot} is missing transaction status");
        }
        Err(err) => return Err(anyhow!(err).context(format!("read rooted block {slot}"))),
    };

    let has_markers = !markers.is_empty();
    let consensus_block_id = consensus_block_id(blockstore, slot, has_markers)?;
    let mut tx_cursor = 0usize;
    let mut entries = Vec::with_capacity(raw_entries.len());
    for entry in &raw_entries {
        let tx_count = entry.transactions.len();
        let rooted = block
            .transactions
            .get(tx_cursor..tx_cursor + tx_count)
            .with_context(|| {
                format!("slot {slot} entry transactions do not match the rooted block")
            })?;
        let mut signature_count = 0u32;
        for (entry_tx, rooted_tx) in entry.transactions.iter().zip(rooted) {
            if entry_tx.signatures != rooted_tx.transaction.signatures {
                bail!("slot {slot} entry transactions do not match the rooted block");
            }
            let count = u32::try_from(entry_tx.signatures.len())
                .context("entry signature count exceeds u32")?;
            signature_count = signature_count
                .checked_add(count)
                .context("entry signature count overflows")?;
        }
        entries.push(SourceEntry {
            num_hashes: entry.num_hashes,
            hash: copy32(entry.hash.as_ref())?,
            tx_count: u32::try_from(tx_count).context("entry has more than u32 transactions")?,
            signature_count,
        });
        tx_cursor += tx_count;
    }
    if tx_cursor != block.transactions.len() {
        bail!("slot {slot} rooted block has transactions that are not in its entries");
    }

    let transactions = block
        .transactions
        .iter()
        .enumerate()
        .map(|(index, tx)| map_transaction(slot, index, tx))
        .collect::<Result<Vec<_>>>()?;

    Ok(SlotData {
        slot,
        is_root: true,
        is_full: true,
        parent_slot: Some(block.parent_slot),
        previous_blockhash: Some(decode_hash(
            &block.previous_blockhash,
            &format!("slot {slot} previous blockhash"),
        )?),
        blockhash: decode_hash(&block.blockhash, &format!("slot {slot} blockhash"))?,
        block_time: block.block_time,
        block_height: block.block_height,
        num_partitions: block.num_partitions,
        rewards: block
            .rewards
            .iter()
            .map(|reward| map_reward(slot, reward))
            .collect::<Result<Vec<_>>>()?,
        consensus_block_id: Some(consensus_block_id),
        markers,
        entries,
        shredding,
        transactions,
    })
}

fn consensus_block_id(blockstore: &Blockstore, slot: u64, has_markers: bool) -> Result<[u8; 32]> {
    let hash = if has_markers {
        blockstore
            .get_double_merkle_root(slot, BlockLocation::Original)
            .with_context(|| format!("read double merkle root for slot {slot}"))?
    } else {
        blockstore
            .get_last_shred_merkle_root(slot)
            .with_context(|| format!("read last shred merkle root for slot {slot}"))?
    };
    let Some(hash) = hash else {
        bail!("slot {slot} has no consensus block id");
    };
    copy32(hash.as_ref())
}

fn read_markers(blockstore: &Blockstore, slot: u64) -> Result<Vec<Vec<u8>>> {
    let (components, _, _) = blockstore
        .get_slot_components_with_shred_info(slot, 0, false)
        .with_context(|| format!("read markers for slot {slot}"))?;
    let mut markers = Vec::new();
    for component in components {
        if let BlockComponent::BlockMarker(marker) = component {
            markers.push(
                wincode::serialize(&marker)
                    .with_context(|| format!("serialize a block marker in slot {slot}"))?,
            );
        }
    }
    Ok(markers)
}

fn map_transaction(
    slot: u64,
    index: usize,
    tx: &solana_transaction_status::VersionedTransactionWithStatusMeta,
) -> Result<SourceTx> {
    if tx.transaction.signatures.len() > usize::from(u8::MAX) {
        bail!("slot {slot} transaction {index} has more than 255 signatures");
    }
    let signatures = tx
        .transaction
        .signatures
        .iter()
        .map(|signature| copy64(signature.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    Ok(SourceTx {
        signatures,
        message: map_message(&tx.transaction.message)?,
        status: Some(map_status(slot, index, &tx.meta)?),
    })
}

fn map_message(message: &VersionedMessage) -> Result<SourceMessage> {
    match message {
        VersionedMessage::Legacy(message) => Ok(SourceMessage::Legacy(SourceMessageBody {
            header: map_header(&message.header),
            account_keys: message
                .account_keys
                .iter()
                .map(|key| copy32(key.as_ref()))
                .collect::<Result<Vec<_>>>()?,
            recent_blockhash: copy32(message.recent_blockhash.as_ref())?,
            instructions: message.instructions.iter().map(map_instruction).collect(),
            lookups: Vec::new(),
        })),
        VersionedMessage::V0(message) => Ok(SourceMessage::V0(SourceMessageBody {
            header: map_header(&message.header),
            account_keys: message
                .account_keys
                .iter()
                .map(|key| copy32(key.as_ref()))
                .collect::<Result<Vec<_>>>()?,
            recent_blockhash: copy32(message.recent_blockhash.as_ref())?,
            instructions: message.instructions.iter().map(map_instruction).collect(),
            lookups: message
                .address_table_lookups
                .iter()
                .map(|lookup| {
                    Ok(SourceLookup {
                        account_key: copy32(lookup.account_key.as_ref())?,
                        writable_indexes: lookup.writable_indexes.clone(),
                        readonly_indexes: lookup.readonly_indexes.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })),
        VersionedMessage::V1(message) => Ok(SourceMessage::V1(SourceV1Message {
            header: map_header(&message.header),
            config: SourceTxConfig {
                priority_fee: message.config.priority_fee,
                compute_unit_limit: message.config.compute_unit_limit,
                loaded_accounts_data_size_limit: message.config.loaded_accounts_data_size_limit,
                heap_size: message.config.heap_size,
            },
            account_keys: message
                .account_keys
                .iter()
                .map(|key| copy32(key.as_ref()))
                .collect::<Result<Vec<_>>>()?,
            recent_blockhash: copy32(message.lifetime_specifier.as_ref())?,
            instructions: message.instructions.iter().map(map_instruction).collect(),
        })),
    }
}

fn map_header(header: &solana_message::MessageHeader) -> SourceHeader {
    SourceHeader {
        num_required_signatures: header.num_required_signatures,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
    }
}

fn map_instruction(
    instruction: &solana_message::compiled_instruction::CompiledInstruction,
) -> SourceInstruction {
    SourceInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts.clone(),
        data: instruction.data.clone(),
    }
}

fn map_status(slot: u64, index: usize, meta: &TransactionStatusMeta) -> Result<SourceStatus> {
    let err = match &meta.status {
        Ok(()) => None,
        Err(err) => Some(
            map_transaction_error(err)
                .with_context(|| format!("slot {slot} transaction {index} error"))?,
        ),
    };
    Ok(SourceStatus {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        inner_instructions: meta.inner_instructions.as_ref().map(|groups| {
            groups
                .iter()
                .map(|group| SourceInnerGroup {
                    index: u32::from(group.index),
                    instructions: group
                        .instructions
                        .iter()
                        .map(|instruction| SourceInnerInstruction {
                            program_id_index: u32::from(instruction.instruction.program_id_index),
                            accounts: instruction.instruction.accounts.clone(),
                            data: instruction.instruction.data.clone(),
                            stack_height: instruction.stack_height,
                        })
                        .collect(),
                })
                .collect()
        }),
        logs: meta.log_messages.clone(),
        pre_token_balances: map_token_balances(slot, index, meta.pre_token_balances.as_deref())?,
        post_token_balances: map_token_balances(slot, index, meta.post_token_balances.as_deref())?,
        rewards: meta
            .rewards
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|reward| map_reward(slot, reward))
            .collect::<Result<Vec<_>>>()?,
        loaded_writable: meta
            .loaded_addresses
            .writable
            .iter()
            .map(|key| copy32(key.as_ref()))
            .collect::<Result<Vec<_>>>()?,
        loaded_readonly: meta
            .loaded_addresses
            .readonly
            .iter()
            .map(|key| copy32(key.as_ref()))
            .collect::<Result<Vec<_>>>()?,
        return_data: meta
            .return_data
            .as_ref()
            .map(|data| -> Result<SourceReturnData> {
                Ok(SourceReturnData {
                    program_id: copy32(data.program_id.as_ref())?,
                    data: data.data.clone(),
                })
            })
            .transpose()?,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn map_token_balances(
    slot: u64,
    index: usize,
    balances: Option<&[solana_transaction_status::TransactionTokenBalance]>,
) -> Result<Vec<SourceTokenBalance>> {
    let Some(balances) = balances else {
        return Ok(Vec::new());
    };
    balances
        .iter()
        .map(|balance| {
            let amount = balance
                .ui_token_amount
                .amount
                .parse::<u64>()
                .with_context(|| {
                    format!(
                        "slot {slot} transaction {index} token amount {}",
                        balance.ui_token_amount.amount
                    )
                })?;
            Ok(SourceTokenBalance {
                account_index: u32::from(balance.account_index),
                mint: decode_key(&balance.mint, "token mint")?,
                owner: decode_optional_key(&balance.owner, "token owner")?,
                program_id: decode_optional_key(&balance.program_id, "token program")?,
                amount,
                decimals: balance.ui_token_amount.decimals,
            })
        })
        .collect()
}

fn map_reward(slot: u64, reward: &Reward) -> Result<SourceReward> {
    if reward.commission.is_none() && reward.commission_bps.is_some() {
        bail!("slot {slot} reward stores commission_bps without a u8 commission");
    }
    if let (Some(commission), Some(bps)) = (reward.commission, reward.commission_bps) {
        if u16::from(commission).checked_mul(100) != Some(bps) {
            bail!("slot {slot} reward commission {commission} does not match commission_bps {bps}");
        }
    }
    Ok(SourceReward {
        pubkey: decode_key(&reward.pubkey, "reward pubkey")?,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward_type_code(reward.reward_type),
        commission: reward.commission,
    })
}

pub(crate) fn reward_type_code(kind: Option<RewardType>) -> i32 {
    match kind {
        None => 0,
        Some(RewardType::Fee) => 1,
        Some(RewardType::Rent) => 2,
        Some(RewardType::Staking) => 3,
        Some(RewardType::Voting) => 4,
        Some(RewardType::DeactivatedStake) => 5,
        Some(RewardType::VATDebit) => 6,
    }
}

pub(crate) fn map_transaction_error(err: &TransactionError) -> Result<CompactTransactionError> {
    Ok(match err {
        TransactionError::AccountInUse => CompactTransactionError::AccountInUse,
        TransactionError::AccountLoadedTwice => CompactTransactionError::AccountLoadedTwice,
        TransactionError::AccountNotFound => CompactTransactionError::AccountNotFound,
        TransactionError::ProgramAccountNotFound => CompactTransactionError::ProgramAccountNotFound,
        TransactionError::InsufficientFundsForFee => {
            CompactTransactionError::InsufficientFundsForFee
        }
        TransactionError::InvalidAccountForFee => CompactTransactionError::InvalidAccountForFee,
        TransactionError::AlreadyProcessed => CompactTransactionError::AlreadyProcessed,
        TransactionError::BlockhashNotFound => CompactTransactionError::BlockhashNotFound,
        TransactionError::InstructionError(index, err) => {
            CompactTransactionError::InstructionError(*index, map_instruction_error(err)?)
        }
        TransactionError::CallChainTooDeep => CompactTransactionError::CallChainTooDeep,
        TransactionError::MissingSignatureForFee => CompactTransactionError::MissingSignatureForFee,
        TransactionError::InvalidAccountIndex => CompactTransactionError::InvalidAccountIndex,
        TransactionError::SignatureFailure => CompactTransactionError::SignatureFailure,
        TransactionError::InvalidProgramForExecution => {
            CompactTransactionError::InvalidProgramForExecution
        }
        TransactionError::SanitizeFailure => CompactTransactionError::SanitizeFailure,
        TransactionError::ClusterMaintenance => CompactTransactionError::ClusterMaintenance,
        TransactionError::AccountBorrowOutstanding => {
            CompactTransactionError::AccountBorrowOutstanding
        }
        TransactionError::WouldExceedMaxBlockCostLimit => {
            CompactTransactionError::WouldExceedMaxBlockCostLimit
        }
        TransactionError::UnsupportedVersion => CompactTransactionError::UnsupportedVersion,
        TransactionError::InvalidWritableAccount => CompactTransactionError::InvalidWritableAccount,
        TransactionError::WouldExceedMaxAccountCostLimit => {
            CompactTransactionError::WouldExceedMaxAccountCostLimit
        }
        TransactionError::WouldExceedAccountDataBlockLimit => {
            CompactTransactionError::WouldExceedAccountDataBlockLimit
        }
        TransactionError::TooManyAccountLocks => CompactTransactionError::TooManyAccountLocks,
        TransactionError::AddressLookupTableNotFound => {
            CompactTransactionError::AddressLookupTableNotFound
        }
        TransactionError::InvalidAddressLookupTableOwner => {
            CompactTransactionError::InvalidAddressLookupTableOwner
        }
        TransactionError::InvalidAddressLookupTableData => {
            CompactTransactionError::InvalidAddressLookupTableData
        }
        TransactionError::InvalidAddressLookupTableIndex => {
            CompactTransactionError::InvalidAddressLookupTableIndex
        }
        TransactionError::InvalidRentPayingAccount => {
            CompactTransactionError::InvalidRentPayingAccount
        }
        TransactionError::WouldExceedMaxVoteCostLimit => {
            CompactTransactionError::WouldExceedMaxVoteCostLimit
        }
        TransactionError::WouldExceedAccountDataTotalLimit => {
            CompactTransactionError::WouldExceedAccountDataTotalLimit
        }
        TransactionError::DuplicateInstruction(index) => {
            CompactTransactionError::DuplicateInstruction(*index)
        }
        TransactionError::InsufficientFundsForRent { account_index } => {
            CompactTransactionError::InsufficientFundsForRent {
                account_index: *account_index,
            }
        }
        TransactionError::MaxLoadedAccountsDataSizeExceeded => {
            CompactTransactionError::MaxLoadedAccountsDataSizeExceeded
        }
        TransactionError::InvalidLoadedAccountsDataSizeLimit => {
            CompactTransactionError::InvalidLoadedAccountsDataSizeLimit
        }
        TransactionError::ResanitizationNeeded => CompactTransactionError::ResanitizationNeeded,
        TransactionError::ProgramExecutionTemporarilyRestricted { account_index } => {
            CompactTransactionError::ProgramExecutionTemporarilyRestricted {
                account_index: *account_index,
            }
        }
        TransactionError::UnbalancedTransaction => CompactTransactionError::UnbalancedTransaction,
        TransactionError::ProgramCacheHitMaxLimit => {
            CompactTransactionError::ProgramCacheHitMaxLimit
        }
        TransactionError::CommitCancelled => CompactTransactionError::CommitCancelled,
        TransactionError::BailOut => {
            bail!("transaction error BailOut has no compact encoding");
        }
        other => bail!("unsupported transaction error {other:?}"),
    })
}

#[allow(deprecated)]
pub(crate) fn map_instruction_error(err: &InstructionError) -> Result<CompactInstructionError> {
    Ok(match err {
        InstructionError::GenericError => CompactInstructionError::GenericError,
        InstructionError::InvalidArgument => CompactInstructionError::InvalidArgument,
        InstructionError::InvalidInstructionData => CompactInstructionError::InvalidInstructionData,
        InstructionError::InvalidAccountData => CompactInstructionError::InvalidAccountData,
        InstructionError::AccountDataTooSmall => CompactInstructionError::AccountDataTooSmall,
        InstructionError::InsufficientFunds => CompactInstructionError::InsufficientFunds,
        InstructionError::IncorrectProgramId => CompactInstructionError::IncorrectProgramId,
        InstructionError::MissingRequiredSignature => {
            CompactInstructionError::MissingRequiredSignature
        }
        InstructionError::AccountAlreadyInitialized => {
            CompactInstructionError::AccountAlreadyInitialized
        }
        InstructionError::UninitializedAccount => CompactInstructionError::UninitializedAccount,
        InstructionError::UnbalancedInstruction => CompactInstructionError::UnbalancedInstruction,
        InstructionError::ModifiedProgramId => CompactInstructionError::ModifiedProgramId,
        InstructionError::ExternalAccountLamportSpend => {
            CompactInstructionError::ExternalAccountLamportSpend
        }
        InstructionError::ExternalAccountDataModified => {
            CompactInstructionError::ExternalAccountDataModified
        }
        InstructionError::ReadonlyLamportChange => CompactInstructionError::ReadonlyLamportChange,
        InstructionError::ReadonlyDataModified => CompactInstructionError::ReadonlyDataModified,
        InstructionError::DuplicateAccountIndex => CompactInstructionError::DuplicateAccountIndex,
        InstructionError::ExecutableModified => CompactInstructionError::ExecutableModified,
        InstructionError::RentEpochModified => CompactInstructionError::RentEpochModified,
        InstructionError::NotEnoughAccountKeys => CompactInstructionError::NotEnoughAccountKeys,
        InstructionError::AccountDataSizeChanged => CompactInstructionError::AccountDataSizeChanged,
        InstructionError::AccountNotExecutable => CompactInstructionError::AccountNotExecutable,
        InstructionError::AccountBorrowFailed => CompactInstructionError::AccountBorrowFailed,
        InstructionError::AccountBorrowOutstanding => {
            CompactInstructionError::AccountBorrowOutstanding
        }
        InstructionError::DuplicateAccountOutOfSync => {
            CompactInstructionError::DuplicateAccountOutOfSync
        }
        InstructionError::Custom(code) => CompactInstructionError::Custom(*code),
        InstructionError::InvalidError => CompactInstructionError::InvalidError,
        InstructionError::ExecutableDataModified => CompactInstructionError::ExecutableDataModified,
        InstructionError::ExecutableLamportChange => {
            CompactInstructionError::ExecutableLamportChange
        }
        InstructionError::ExecutableAccountNotRentExempt => {
            CompactInstructionError::ExecutableAccountNotRentExempt
        }
        InstructionError::UnsupportedProgramId => CompactInstructionError::UnsupportedProgramId,
        InstructionError::CallDepth => CompactInstructionError::CallDepth,
        InstructionError::MissingAccount => CompactInstructionError::MissingAccount,
        InstructionError::ReentrancyNotAllowed => CompactInstructionError::ReentrancyNotAllowed,
        InstructionError::MaxSeedLengthExceeded => CompactInstructionError::MaxSeedLengthExceeded,
        InstructionError::InvalidSeeds => CompactInstructionError::InvalidSeeds,
        InstructionError::InvalidRealloc => CompactInstructionError::InvalidRealloc,
        InstructionError::ComputationalBudgetExceeded => {
            CompactInstructionError::ComputationalBudgetExceeded
        }
        InstructionError::PrivilegeEscalation => CompactInstructionError::PrivilegeEscalation,
        InstructionError::ProgramEnvironmentSetupFailure => {
            CompactInstructionError::ProgramEnvironmentSetupFailure
        }
        InstructionError::ProgramFailedToComplete => {
            CompactInstructionError::ProgramFailedToComplete
        }
        InstructionError::ProgramFailedToCompile => CompactInstructionError::ProgramFailedToCompile,
        InstructionError::Immutable => CompactInstructionError::Immutable,
        InstructionError::IncorrectAuthority => CompactInstructionError::IncorrectAuthority,
        InstructionError::BorshIoError => CompactInstructionError::BorshIoError(String::new()),
        InstructionError::AccountNotRentExempt => CompactInstructionError::AccountNotRentExempt,
        InstructionError::InvalidAccountOwner => CompactInstructionError::InvalidAccountOwner,
        InstructionError::ArithmeticOverflow => CompactInstructionError::ArithmeticOverflow,
        InstructionError::UnsupportedSysvar => CompactInstructionError::UnsupportedSysvar,
        InstructionError::IllegalOwner => CompactInstructionError::IllegalOwner,
        InstructionError::MaxAccountsDataAllocationsExceeded => {
            CompactInstructionError::MaxAccountsDataAllocationsExceeded
        }
        InstructionError::MaxAccountsExceeded => CompactInstructionError::MaxAccountsExceeded,
        InstructionError::MaxInstructionTraceLengthExceeded => {
            CompactInstructionError::MaxInstructionTraceLengthExceeded
        }
        InstructionError::BuiltinProgramsMustConsumeComputeUnits => {
            CompactInstructionError::BuiltinProgramsMustConsumeComputeUnits
        }
        InstructionError::BailOut => {
            bail!("instruction error BailOut has no compact encoding");
        }
        other => bail!("unsupported instruction error {other:?}"),
    })
}

fn decode_hash(text: &str, what: &str) -> Result<[u8; 32]> {
    decode_key(text, what)
}

fn decode_key(text: &str, what: &str) -> Result<[u8; 32]> {
    if text.is_empty() {
        bail!("{what} is empty");
    }
    let bytes = bs58::decode(text)
        .into_vec()
        .with_context(|| format!("{what} is not base58"))?;
    <[u8; 32]>::try_from(bytes).map_err(|_| anyhow!("{what} is not 32 bytes"))
}

fn decode_optional_key(text: &str, what: &str) -> Result<Option<[u8; 32]>> {
    if text.is_empty() {
        Ok(None)
    } else {
        decode_key(text, what).map(Some)
    }
}

fn copy32(bytes: &[u8]) -> Result<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).map_err(|_| anyhow!("key is not 32 bytes"))
}

fn copy64(bytes: &[u8]) -> Result<[u8; 64]> {
    <[u8; 64]>::try_from(bytes).map_err(|_| anyhow!("signature is not 64 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::block_component::{BlockHeaderV1, VersionedBlockMarker};
    use solana_hash::Hash;

    #[test]
    fn read_only_options_do_not_create_a_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let error = read_only_options(dir.path()).unwrap_err();
        assert!(error.to_string().contains("does not exist"), "{error:#}");
        assert!(!dir.path().join(BLOCKSTORE_DIRECTORY_ROCKS_LEVEL).exists());
    }

    #[test]
    fn read_only_options_require_readonly_access() {
        let dir = tempfile::tempdir().unwrap();
        let rocks = dir.path().join(BLOCKSTORE_DIRECTORY_ROCKS_LEVEL);
        std::fs::create_dir(&rocks).unwrap();
        let options = read_only_options(dir.path()).unwrap();
        assert_eq!(options.access_type, AccessType::ReadOnly);
        assert!(reject_non_readonly(&AccessType::Primary).is_err());
        assert!(reject_non_readonly(&AccessType::PrimaryForMaintenance).is_err());
        assert!(reject_non_readonly(&AccessType::ReadOnly).is_ok());
    }

    #[test]
    fn agave_header_marker_roundtrips_the_parent_block_id() {
        let parent = [0x3c; 32];
        let marker = VersionedBlockMarker::from_block_header(BlockHeaderV1 {
            parent_slot: 40,
            parent_block_id: Hash::new_from_array(parent),
        });
        let bytes = wincode::serialize(&marker).unwrap();
        assert_eq!(
            of_car_reader::blockmarker::parent_consensus_block_id(&[bytes]),
            Some(parent)
        );
    }

    #[test]
    fn known_transaction_error_maps_and_bailout_fails() {
        assert!(matches!(
            map_transaction_error(&TransactionError::AccountInUse).unwrap(),
            CompactTransactionError::AccountInUse
        ));
        assert!(map_transaction_error(&TransactionError::BailOut).is_err());
        assert!(map_instruction_error(&InstructionError::BailOut).is_err());
        assert!(matches!(
            map_instruction_error(&InstructionError::BorshIoError).unwrap(),
            CompactInstructionError::BorshIoError(ref message) if message.is_empty()
        ));
    }

    #[test]
    fn reward_type_codes_keep_the_archive_numbers() {
        assert_eq!(reward_type_code(Some(RewardType::Fee)), 1);
        assert_eq!(reward_type_code(Some(RewardType::Rent)), 2);
        assert_eq!(reward_type_code(Some(RewardType::Staking)), 3);
        assert_eq!(reward_type_code(Some(RewardType::Voting)), 4);
        assert_eq!(reward_type_code(Some(RewardType::DeactivatedStake)), 5);
        assert_eq!(reward_type_code(Some(RewardType::VATDebit)), 6);
    }
}
