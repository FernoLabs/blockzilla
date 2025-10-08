use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use prost::Message;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::{TransactionError, VersionedTransaction};
use solana_storage_proto::convert::generated;

use crate::block_stream::SolanaBlock;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlock {
    pub slot: u64,
    pub blockhash: String,
    pub previous_blockhash: String,
    pub parent_slot: Option<u64>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transactions: Vec<RpcTransactionWithMeta>,
    pub rewards: Option<Vec<RpcReward>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionWithMeta {
    pub transaction: String,
    pub meta: RpcTransactionMeta,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionMeta {
    pub fee: u64,
    pub err: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcReward {
    pub pubkey: String,
    pub lamports: i64,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}

fn decode_transaction_meta(buffer: &[u8]) -> Result<(u64, Option<String>)> {
    // try protobuf (solana-storage-proto)
    if let Ok(proto) = generated::TransactionStatusMeta::decode(buffer) {
        let err = if proto.err.is_some() {
            Some("InstructionError".to_string())
        } else {
            None
        };
        return Ok((proto.fee, err));
    }

    // fallback to legacy bincode path
    #[derive(Deserialize)]
    struct StoredTransactionMeta {
        err: Result<(), TransactionError>,
        fee: u64,
    }

    if let Ok(meta) = bincode::deserialize::<StoredTransactionMeta>(buffer) {
        let err = if meta.err.is_ok() {
            None
        } else {
            Some("Error".into())
        };
        return Ok((meta.fee, err));
    }

    Ok((0, None))
}

pub fn solana_block_to_rpc(
    block: &SolanaBlock,
    blockhash: String,
    previous_blockhash: String,
    parent_slot: Option<u64>,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Option<Vec<RpcReward>>,
) -> Result<RpcBlock> {
    let mut rpc_txs = Vec::with_capacity(block.transactions.len());

    for tx_node in &block.transactions {
        let vt: VersionedTransaction = bincode::deserialize(&tx_node.data.data)
            .context("Failed to decode VersionedTransaction")?;

        let (fee, err) = if !tx_node.metadata.data.is_empty() {
            match zstd::decode_all(tx_node.metadata.data.as_slice()) {
                Ok(decoded_meta) => decode_transaction_meta(&decoded_meta).unwrap_or((0, None)),
                Err(_) => (0, None),
            }
        } else {
            (0, None)
        };

        let mut wire = Vec::new();
        bincode::serialize_into(&mut wire, &vt)?;
        let encoded_tx = B64.encode(wire);

        rpc_txs.push(RpcTransactionWithMeta {
            transaction: encoded_tx,
            meta: RpcTransactionMeta { fee, err },
        });
    }

    Ok(RpcBlock {
        slot: block.slot,
        blockhash,
        previous_blockhash,
        parent_slot,
        block_time,
        block_height,
        rewards,
        transactions: rpc_txs,
    })
}
