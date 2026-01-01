use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::registry::Registry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTransaction {
    pub signatures: Vec<solana_transaction::Signature>,
    pub message: CompactMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactMessage {
    Legacy(CompactLegacyMessage),
    V0(CompactV0Message),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactMessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactLegacyMessage {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>, // registry indices
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<CompactInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub account_key: u32, // registry index of the table address
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactV0Message {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>, // registry indices of static keys
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<CompactInstruction>,
    pub address_table_lookups: Vec<CompactAddressTableLookup>,
}

#[cfg(feature = "solana")]
pub fn to_compact_transaction(
    vtx: &solana_transaction::versioned::VersionedTransaction,
    registry: &Registry,
) -> Result<CompactTransaction> {
    use solana_message::VersionedMessage;

    let signatures = vtx.signatures.clone();

    let message = match &vtx.message {
        VersionedMessage::Legacy(m) => {
            let header = CompactMessageHeader {
                num_required_signatures: m.header.num_required_signatures,
                num_readonly_signed_accounts: m.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: m.header.num_readonly_unsigned_accounts,
            };

            let account_keys = m
                .account_keys
                .iter()
                .map(|k| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(k.as_ref());
                    registry
                        .lookup(&arr)
                        .ok_or_else(|| anyhow::anyhow!("pubkey missing from registry"))
                })
                .collect::<Result<Vec<u32>>>()?;

            let recent_blockhash: [u8; 32] = m
                .recent_blockhash
                .as_ref()
                .try_into()
                .map_err(|_| anyhow::anyhow!("blockhash len != 32"))?;

            let instructions = m
                .instructions
                .iter()
                .map(|ix| CompactInstruction {
                    program_id_index: ix.program_id_index,
                    accounts: ix.accounts.clone(),
                    data: ix.data.clone(),
                })
                .collect();

            CompactMessage::Legacy(CompactLegacyMessage {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            })
        }

        VersionedMessage::V0(m) => {
            let header = CompactMessageHeader {
                num_required_signatures: m.header.num_required_signatures,
                num_readonly_signed_accounts: m.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: m.header.num_readonly_unsigned_accounts,
            };

            let account_keys = m
                .account_keys
                .iter()
                .map(|k| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(k.as_ref());
                    registry
                        .lookup(&arr)
                        .ok_or_else(|| anyhow::anyhow!("pubkey missing from registry"))
                })
                .collect::<Result<Vec<u32>>>()?;

            let recent_blockhash: [u8; 32] = m
                .recent_blockhash
                .as_ref()
                .try_into()
                .map_err(|_| anyhow::anyhow!("blockhash len != 32"))?;

            let instructions = m
                .instructions
                .iter()
                .map(|ix| CompactInstruction {
                    program_id_index: ix.program_id_index,
                    accounts: ix.accounts.clone(),
                    data: ix.data.clone(),
                })
                .collect();

            let address_table_lookups = m
                .address_table_lookups
                .iter()
                .map(|l| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(l.account_key.as_ref());
                    let table_idx = registry
                        .lookup(&arr)
                        .ok_or_else(|| anyhow::anyhow!("lookup table key missing from registry"))?;

                    Ok(CompactAddressTableLookup {
                        account_key: table_idx,
                        writable_indexes: l.writable_indexes.clone(),
                        readonly_indexes: l.readonly_indexes.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            CompactMessage::V0(CompactV0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            })
        }
    };

    Ok(CompactTransaction {
        signatures,
        message,
    })
}
