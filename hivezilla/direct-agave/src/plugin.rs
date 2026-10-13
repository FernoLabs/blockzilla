//! This adapter is compiled against the exact Agave source revision, not a semver range.
use crate::{
    event::{Event, hash},
    journal::{Config, Journal},
};
use agave_geyser_plugin_interface::geyser_plugin_interface::*;
use anyhow::{Context, ensure};
use blockzilla_archive_v2::*;
use blockzilla_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader,
    CompactTransactionConfig, CompactTransactionError,
};
use serde_json::json;
use solana_message::VersionedMessage;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::{
    Reward, RewardType, TransactionStatusMeta, TransactionTokenBalance,
};

#[derive(Debug, Default)]
pub struct Plugin {
    journal: Option<Journal>,
    bank_v2: bool,
}
fn plugin_error(error: anyhow::Error) -> GeyserPluginError {
    GeyserPluginError::Custom(error.into())
}
impl Plugin {
    fn status(
        &self,
        slot: u64,
        bank_id: Option<u64>,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> Event {
        if !self.bank_v2 {
            return status_event(slot, bank_id, parent, status);
        }
        Event::BankStatusV2 {
            schema: crate::event::BANK_V2.into(),
            slot,
            bank_id,
            parent,
            parent_blockhash: None,
            status: status.as_str().into(),
            dead_error: if let SlotStatus::Dead(e) = status {
                Some(e.clone())
            } else {
                None
            },
            observed_at_unix_ms: source_now_ms(),
        }
    }
    fn emit(&self, event: impl FnOnce() -> anyhow::Result<Event>) -> Result<()> {
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| plugin_error(anyhow::anyhow!("plugin is not loaded")))?;
        if journal.is_failed() {
            return Ok(());
        }
        match event() {
            Ok(event) => journal.append(&event).map_err(plugin_error),
            Err(error) => journal
                .reject(&format!("callback conversion failed: {error:#}"))
                .map_err(plugin_error),
        }
    }
}
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    Box::into_raw(Box::new(Plugin::default()))
}
impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        "hivezilla-direct-agave-v1"
    }
    fn on_load(&mut self, path: &str, _reload: bool) -> Result<()> {
        let bytes = std::fs::read(path)?;
        if bytes.len() > 1024 * 1024 {
            return Err(plugin_error(anyhow::anyhow!("config exceeds 1 MiB")));
        }
        let config: Config = serde_json::from_slice(&bytes).map_err(|e| plugin_error(e.into()))?;
        if config.bank_v2 && !cfg!(feature = "bank-v2") {
            return Err(plugin_error(anyhow::anyhow!(
                "bank_v2 requires the patched Agave interface"
            )));
        }
        self.bank_v2 = config.bank_v2;
        self.journal = Some(Journal::start(config).map_err(plugin_error)?);
        Ok(())
    }
    fn on_unload(&mut self) {
        if let Some(mut journal) = self.journal.take() {
            if let Err(error) = journal.close() {
                eprintln!("HIVEZILLA_CAPTURE_UNLOAD_FAILED: {error:#}");
            }
        }
    }
    fn account_data_notifications_enabled(&self) -> bool {
        false
    }
    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }
    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
    fn entry_notifications_enabled(&self) -> bool {
        true
    }
    fn block_footer_notifications_enabled(&self) -> bool {
        true
    }
    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> Result<()> {
        self.emit(|| Ok(self.status(slot, None, parent, status)))
    }
    fn update_bank_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
        bank_id: u64,
    ) -> Result<()> {
        self.emit(|| Ok(self.status(slot, Some(bank_id), parent, status)))
    }
    #[cfg(feature = "bank-v2")]
    fn notify_bank_state_v2(&self, bank: &ReplicaBankStateInfoV2) -> Result<()> {
        if !self.bank_v2 {
            return Ok(());
        }
        self.emit(|| {
            let state = crate::event::NativeBankState {
                schema: crate::event::BANK_V2.into(),
                slot: bank.slot,
                bank_id: bank.bank_id,
                parent_slot: bank.parent_slot,
                blockhash: bank.blockhash,
                parent_blockhash: bank.parent_blockhash,
                block_height: bank.block_height,
                last_valid_block_height: bank
                    .last_valid_block_height
                    .context("bank has no native blockhash expiry")?,
                observed_at_unix_ms: bank.observed_at_unix_ms,
                confirmed: bank.confirmed,
            };
            state.validate()?;
            Ok(Event::BankStateV2(state))
        })
    }
    fn notify_transaction_for_bank(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
        bank_id: u64,
    ) -> Result<()> {
        self.emit(|| match transaction {
            ReplicaTransactionInfoVersions::V0_0_3(v) => {
                ensure!(
                    v.transaction.signatures.first() == Some(v.signature),
                    "callback signature mismatch"
                );
                transaction_event(
                    v.transaction,
                    v.transaction_status_meta,
                    slot,
                    bank_id,
                    v.index,
                    v.is_vote,
                    v.message_hash.as_ref().to_vec(),
                )
            }
            ReplicaTransactionInfoVersions::V0_0_2(v) => {
                let tx = v.transaction.to_versioned_transaction();
                ensure!(
                    tx.signatures.first() == Some(v.signature),
                    "callback signature mismatch"
                );
                transaction_event(
                    &tx,
                    v.transaction_status_meta,
                    slot,
                    bank_id,
                    v.index,
                    v.is_vote,
                    v.transaction.message_hash().as_ref().to_vec(),
                )
            }
            ReplicaTransactionInfoVersions::V0_0_1(_) => anyhow::bail!(
                "transaction callback v1 has no block index; cannot build complete blocks"
            ),
        })
    }
    fn notify_entry_for_bank(&self, entry: ReplicaEntryInfoVersions, bank_id: u64) -> Result<()> {
        self.emit(|| match entry {
            ReplicaEntryInfoVersions::V0_0_2(e) => Ok(Event::Entry {
                slot: e.slot,
                bank_id,
                index: e.index as u64,
                num_hashes: e.num_hashes,
                hash: e.hash.try_into().context("entry hash length")?,
                transactions: e.executed_transaction_count,
                starting_transaction_index: e.starting_transaction_index as u64,
            }),
            ReplicaEntryInfoVersions::V0_0_1(_) => {
                anyhow::bail!("entry callback v1 has no starting transaction index")
            }
        })
    }
    fn notify_block_metadata_for_bank(
        &self,
        info: ReplicaBlockInfoVersions,
        bank_id: u64,
    ) -> Result<()> {
        self.emit(|| match info {
            ReplicaBlockInfoVersions::V0_0_4(b) => block_event(
                b.slot,
                bank_id,
                b.parent_slot,
                b.parent_blockhash,
                b.blockhash,
                b.executed_transaction_count,
                b.entry_count,
                b.block_time,
                b.block_height,
                &b.rewards.rewards,
                b.rewards.num_partitions,
            ),
            ReplicaBlockInfoVersions::V0_0_3(b) => block_event(
                b.slot,
                bank_id,
                b.parent_slot,
                b.parent_blockhash,
                b.blockhash,
                b.executed_transaction_count,
                b.entry_count,
                b.block_time,
                b.block_height,
                b.rewards,
                None,
            ),
            ReplicaBlockInfoVersions::V0_0_1(_) | ReplicaBlockInfoVersions::V0_0_2(_) => {
                anyhow::bail!("old metadata callback lacks block completeness fields")
            }
        })
    }
    fn notify_entry_update_parent(&self, info: ReplicaEntryUpdateParentInfoVersions) -> Result<()> {
        let ReplicaEntryUpdateParentInfoVersions::V0_0_1(v) = info;
        self.emit(|| {
            if self.bank_v2 {
                return Ok(Event::BankStatusV2 {
                    schema: crate::event::BANK_V2.into(),
                    slot: v.slot,
                    bank_id: Some(v.cleared_bank_id),
                    parent: Some(v.parent_slot),
                    parent_blockhash: Some(v.parent_block_id.to_bytes()),
                    status: "clear".into(),
                    dead_error: Some("agave_bank_invalidated".into()),
                    observed_at_unix_ms: source_now_ms(),
                });
            }
            Ok(Event::ClearBank {
                slot: v.slot,
                bank_id: v.cleared_bank_id,
                parent_slot: v.parent_slot,
                parent_blockhash: v.parent_block_id.to_bytes(),
            })
        })
    }
    fn notify_block_footer(
        &self,
        info: ReplicaBlockFooterInfoVersions,
        bank_id: u64,
    ) -> Result<()> {
        let ReplicaBlockFooterInfoVersions::V0_0_1(v) = info;
        self.emit(|| {
            wincode::serialize(v.block_footer)
                .map(|wire_footer| Event::Footer {
                    slot: v.slot,
                    bank_id,
                    wire_footer,
                })
                .map_err(Into::into)
        })
    }
}
fn source_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn status_event(
    slot: u64,
    bank_id: Option<u64>,
    parent: Option<u64>,
    status: &SlotStatus,
) -> Event {
    Event::Status {
        slot,
        bank_id,
        parent,
        status: status.as_str().into(),
        dead_error: if let SlotStatus::Dead(e) = status {
            Some(e.clone())
        } else {
            None
        },
    }
}
#[allow(clippy::too_many_arguments)]
fn block_event(
    slot: u64,
    bank_id: u64,
    parent_slot: u64,
    parent: &str,
    block: &str,
    transactions: u64,
    entries: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: &[Reward],
    num_partitions: Option<u64>,
) -> anyhow::Result<Event> {
    let native_rewards = json!({"rewards":rewards,"num_partitions":num_partitions});
    let source_len = serde_json::to_vec(&native_rewards)?.len() as u64;
    Ok(Event::Block {
        slot,
        bank_id,
        parent_slot,
        parent_blockhash: hash(parent)?,
        blockhash: hash(block)?,
        transactions,
        entries,
        block_time,
        block_height,
        native_rewards,
        rewards: WincodeArchiveV2NoRegistryRewards {
            source_len,
            num_partitions,
            decoded: Some(rewards.iter().map(reward).collect::<anyhow::Result<_>>()?),
            raw_fallback: None,
            decode_error: None,
        },
    })
}
pub fn transaction_event(
    tx: &VersionedTransaction,
    m: &TransactionStatusMeta,
    slot: u64,
    bank_id: u64,
    index: usize,
    is_vote: bool,
    message_hash: Vec<u8>,
) -> anyhow::Result<Event> {
    let wire_transaction = wincode::serialize(tx)?;
    let source_len = wire_transaction.len() as u64;
    let header = tx.message.header();
    ensure!(
        tx.signatures.len() == header.num_required_signatures as usize && !tx.signatures.is_empty(),
        "signature count mismatch"
    );
    let native_meta = json!({"status":m.status,"fee":m.fee,"pre_balances":m.pre_balances,"post_balances":m.post_balances,"inner_instructions":m.inner_instructions,"log_messages":m.log_messages,"pre_token_balances":m.pre_token_balances.as_ref().map(|v|v.iter().map(token_json).collect::<Vec<_>>()),"post_token_balances":m.post_token_balances.as_ref().map(|v|v.iter().map(token_json).collect::<Vec<_>>()),"rewards":m.rewards,"loaded_addresses":{"writable":m.loaded_addresses.writable.iter().map(|v|v.to_bytes()).collect::<Vec<_>>(),"readonly":m.loaded_addresses.readonly.iter().map(|v|v.to_bytes()).collect::<Vec<_>>()},"return_data":m.return_data.as_ref().map(|r|json!({"program_id":r.program_id.to_bytes(),"data":r.data})),"compute_units_consumed":m.compute_units_consumed,"cost_units":m.cost_units});
    let metadata_len = serde_json::to_vec(&native_meta)?.len() as u64;
    let value = WincodeArchiveV2NoRegistryTx {
        signatures: tx.signatures.iter().map(|s| s.as_ref().to_vec()).collect(),
        message: message(&tx.message)?,
    };
    let projection = WincodeArchiveV2NoRegistryTransaction {
        tx_index: u32::try_from(index)?,
        tx: WincodeArchiveV2Payload::Decoded { source_len, value },
        metadata: Some(WincodeArchiveV2Payload::Decoded {
            source_len: metadata_len,
            value: metadata(m)?,
        }),
    };
    Ok(Event::Transaction {
        slot,
        bank_id,
        index: index as u64,
        is_vote,
        message_hash,
        wire_transaction,
        native_meta,
        projection,
    })
}
fn token_json(t: &TransactionTokenBalance) -> serde_json::Value {
    json!({"account_index":t.account_index,"mint":t.mint,"ui_token_amount":t.ui_token_amount,"owner":t.owner,"program_id":t.program_id})
}
fn message(m: &VersionedMessage) -> anyhow::Result<WincodeArchiveV2NoRegistryMessage> {
    let h = m.header();
    let header = CompactMessageHeader {
        num_required_signatures: h.num_required_signatures,
        num_readonly_signed_accounts: h.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: h.num_readonly_unsigned_accounts,
    };
    let account_keys = m
        .static_account_keys()
        .iter()
        .map(|v| v.to_bytes())
        .collect();
    let recent_blockhash = m.recent_blockhash().to_bytes();
    let instructions = m
        .instructions()
        .iter()
        .map(|i| WincodeArchiveV2NoRegistryInstruction {
            program_id_index: i.program_id_index,
            accounts: i.accounts.clone(),
            data: i.data.clone(),
        })
        .collect();
    Ok(match m {
        VersionedMessage::Legacy(_) => {
            WincodeArchiveV2NoRegistryMessage::Legacy(WincodeArchiveV2NoRegistryLegacyMessage {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            })
        }
        VersionedMessage::V0(m) => {
            WincodeArchiveV2NoRegistryMessage::V0(WincodeArchiveV2NoRegistryV0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups: m
                    .address_table_lookups
                    .iter()
                    .map(|l| WincodeArchiveV2NoRegistryAddressTableLookup {
                        account_key: l.account_key.to_bytes(),
                        writable_indexes: l.writable_indexes.clone(),
                        readonly_indexes: l.readonly_indexes.clone(),
                    })
                    .collect(),
            })
        }
        VersionedMessage::V1(m) => {
            WincodeArchiveV2NoRegistryMessage::V1(WincodeArchiveV2NoRegistryV1Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                config: CompactTransactionConfig {
                    priority_fee: m.config.priority_fee,
                    compute_unit_limit: m.config.compute_unit_limit,
                    loaded_accounts_data_size_limit: m.config.loaded_accounts_data_size_limit,
                    heap_size: m.config.heap_size,
                },
            })
        }
    })
}
fn metadata(m: &TransactionStatusMeta) -> anyhow::Result<WincodeArchiveV2NoRegistryMeta> {
    ensure!(
        m.pre_balances.len() == m.post_balances.len(),
        "pre/post balance count differs"
    );
    let err = m
        .status
        .as_ref()
        .err()
        .map(|e| -> anyhow::Result<_> {
            Ok(CompactTransactionError::from_stored_wincode_bytes(
                &wincode::serialize(e)?,
            )?)
        })
        .transpose()?;
    Ok(WincodeArchiveV2NoRegistryMeta {
        err,
        fee: m.fee,
        pre_balances: m.pre_balances.clone(),
        post_balances: m.post_balances.clone(),
        inner_instructions: m.inner_instructions.as_ref().map(|v| {
            v.iter()
                .map(|r| CompactInnerInstructions {
                    index: u32::from(r.index),
                    instructions: r
                        .instructions
                        .iter()
                        .map(|i| CompactInnerInstruction {
                            program_id_index: u32::from(i.instruction.program_id_index),
                            accounts: i.instruction.accounts.clone(),
                            data: i.instruction.data.clone(),
                            stack_height: i.stack_height,
                        })
                        .collect(),
                })
                .collect()
        }),
        logs: m
            .log_messages
            .clone()
            .map(WincodeArchiveV2NoRegistryLogs::Raw),
        pre_token_balances: m
            .pre_token_balances
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(token)
            .collect::<anyhow::Result<_>>()?,
        post_token_balances: m
            .post_token_balances
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(token)
            .collect::<anyhow::Result<_>>()?,
        rewards: m
            .rewards
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(reward)
            .collect::<anyhow::Result<_>>()?,
        loaded_writable_addresses: m
            .loaded_addresses
            .writable
            .iter()
            .map(|v| v.to_bytes())
            .collect(),
        loaded_readonly_addresses: m
            .loaded_addresses
            .readonly
            .iter()
            .map(|v| v.to_bytes())
            .collect(),
        return_data: m
            .return_data
            .as_ref()
            .map(|r| WincodeArchiveV2NoRegistryReturnData {
                program_id: r.program_id.to_bytes(),
                data: r.data.clone(),
            }),
        compute_units_consumed: m.compute_units_consumed,
        cost_units: m.cost_units,
    })
}
fn token(t: &TransactionTokenBalance) -> anyhow::Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let optional = |s: &str| {
        if s.is_empty() {
            Ok(None)
        } else {
            hash(s).map(Some)
        }
    };
    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: u32::from(t.account_index),
        mint: optional(&t.mint)?,
        owner: optional(&t.owner)?,
        program_id: optional(&t.program_id)?,
        amount: t.ui_token_amount.amount.parse()?,
        decimals: t.ui_token_amount.decimals,
    })
}
fn reward(r: &Reward) -> anyhow::Result<WincodeArchiveV2NoRegistryReward> {
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: hash(&r.pubkey)?,
        lamports: r.lamports,
        post_balance: r.post_balance,
        // These codes match the pinned Agave confirmed_block.proto. Exhaustive
        // matching makes a new native variant a compile error during an upgrade.
        reward_type: match r.reward_type {
            None => 0,
            Some(RewardType::Fee) => 1,
            Some(RewardType::Rent) => 2,
            Some(RewardType::Staking) => 3,
            Some(RewardType::Voting) => 4,
            Some(RewardType::DeactivatedStake) => 5,
            Some(RewardType::VATDebit) => 6,
        },
        commission: r.commission,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn epoch_rewards_preserve_every_native_type_and_signed_amount() {
        let types = [
            None,
            Some(RewardType::Fee),
            Some(RewardType::Rent),
            Some(RewardType::Staking),
            Some(RewardType::Voting),
            Some(RewardType::DeactivatedStake),
            Some(RewardType::VATDebit),
        ];
        let rewards: Vec<_> = types
            .into_iter()
            .enumerate()
            .map(|(i, reward_type)| Reward {
                pubkey: "11111111111111111111111111111111".into(),
                lamports: if reward_type == Some(RewardType::VATDebit) {
                    -5000
                } else {
                    i as i64
                },
                post_balance: 10000,
                reward_type,
                commission: Some(7),
                commission_bps: Some(725),
            })
            .collect();
        let Event::Block {
            native_rewards,
            rewards: projected,
            ..
        } = block_event(
            452736000,
            9,
            452735999,
            "11111111111111111111111111111111",
            "11111111111111111111111111111111",
            0,
            0,
            Some(1790977800),
            Some(1),
            &rewards,
            Some(4),
        )
        .unwrap()
        else {
            panic!("wrong event")
        };
        let preserved: Vec<Reward> =
            serde_json::from_value(native_rewards["rewards"].clone()).unwrap();
        assert_eq!(preserved, rewards);
        assert_eq!(native_rewards["num_partitions"], json!(4));
        let bytes = wincode::serialize(&projected).unwrap();
        let decoded: WincodeArchiveV2NoRegistryRewards = wincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.num_partitions, Some(4));
        let rows = decoded.decoded.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.reward_type).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6]
        );
        assert_eq!(rows[6].lamports, -5000);
        assert_eq!(rows[5].post_balance, 10000);
        assert_eq!(rows[5].commission, Some(7));
    }
    #[test]
    fn native_wire_error_and_absent_metadata_survive_projection() {
        let tx = VersionedTransaction {
            signatures: vec![Default::default()],
            message: VersionedMessage::Legacy(solana_message::Message {
                header: solana_message::MessageHeader {
                    num_required_signatures: 1,
                    ..Default::default()
                },
                account_keys: vec![Default::default()],
                ..Default::default()
            }),
        };
        let mut meta = TransactionStatusMeta::default();
        meta.status = Err(solana_transaction_error::TransactionError::AccountInUse);
        meta.pre_token_balances = Some(vec![]);
        meta.post_token_balances = None;
        meta.fee = 5000;
        meta.pre_balances = vec![10000];
        meta.post_balances = vec![5000];
        let event = transaction_event(&tx, &meta, 123, 77, 0, false, vec![8; 32]).unwrap();
        let Event::Transaction {
            wire_transaction,
            native_meta,
            projection,
            ..
        } = event
        else {
            panic!("wrong event")
        };
        let decoded: VersionedTransaction = wincode::deserialize(&wire_transaction).unwrap();
        assert_eq!(decoded, tx);
        assert_eq!(native_meta["pre_token_balances"], json!([]));
        assert_eq!(native_meta["post_token_balances"], serde_json::Value::Null);
        let Some(WincodeArchiveV2Payload::Decoded { value, .. }) = projection.metadata else {
            panic!("missing metadata")
        };
        assert!(value.err.is_some());
        assert_eq!(value.fee, 5000);
    }
}
