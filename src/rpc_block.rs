use anyhow::{Context, anyhow};
use cid::Cid;
use prost::Message;
use serde::Deserialize;
use solana_sdk::transaction::{TransactionVersion, VersionedTransaction};
use solana_storage_proto::convert::generated;
use solana_transaction_status_client_types::{
    EncodedConfirmedBlock, EncodedTransaction, EncodedTransactionWithStatusMeta,
    TransactionBinaryEncoding,
};

use crate::{Node, block_stream::CarBlock};

impl TryInto<EncodedConfirmedBlock> for CarBlock {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<EncodedConfirmedBlock, Self::Error> {
        // todo compute block hash
        let previous_blockhash = format!("missing");
        let blockhash = format!("missing");

        let parent_slot = self
            .block
            .meta
            .parent_slot
            .ok_or(anyhow!("missing parent block"))?;

        let transactions_cid: Vec<Cid> = self
            .block
            .entries
            .iter()
            .flat_map(|e| {
                if let Some(Node::Entry(entry)) = self.entries.get(e) {
                    entry.transactions.clone()
                } else {
                    println!("Not an entry");
                    vec![]
                }
            })
            .collect();
    
        let transactions: Result<Vec<EncodedTransactionWithStatusMeta>, anyhow::Error> =
            transactions_cid
                .iter()
                .map(|cid| match self.entries.get(cid) {
                    Some(crate::Node::Transaction(tx)) => {
                        let meta_byte = self.merge_dataframe(&tx.metadata)?;
                        let tx_bytes = self.merge_dataframe(&tx.data)?;
                        let vt: VersionedTransaction = bincode::deserialize(&tx_bytes)
                            .context("Failed to decode VersionedTransaction")?;

                        let (fee, err) = if !meta_byte.is_empty() {
                            match zstd::decode_all(meta_byte.as_slice()) {
                                Ok(decoded_meta) => {
                                    decode_transaction_meta(&decoded_meta).unwrap_or((0, None))
                                }
                                Err(_) => (0, None),
                            }
                        } else {
                            (0, None)
                        };

                        let mut wire = Vec::new();
                        bincode::serialize_into(&mut wire, &vt)?;
                        let encoded_tx = base64::encode(wire);

                        Ok(EncodedTransactionWithStatusMeta {
                            transaction: EncodedTransaction::Binary(
                                encoded_tx,
                                TransactionBinaryEncoding::Base64,
                            ),
                            meta: None,
                            version: Some(TransactionVersion::Number(0)),
                        })
                    }
                    Some(b) => Err(anyhow!("block entry not a transaction ({b:?})")),
                    None => Err(anyhow!("block entry not found")),
                })
                .collect();

        let rewards = match self.block.rewards.and_then(|cid| self.entries.get(&cid)) {
            Some(crate::Node::DataFrame(df)) => {
                let bytes = self.merge_dataframe(df)?;
                bincode::deserialize(&bytes)?
            }
            _ => vec![],
        };

        Ok(EncodedConfirmedBlock {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions: transactions?,
            rewards: rewards,
            num_partitions: None, // todo what is this ?
            block_time: self.block.meta.blocktime,
            block_height: self.block.meta.block_height,
        })
    }
}

fn decode_transaction_meta(buffer: &[u8]) -> Result<(u64, Option<String>), anyhow::Error> {
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
        err: Result<(), solana_sdk::transaction::TransactionError>,
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
