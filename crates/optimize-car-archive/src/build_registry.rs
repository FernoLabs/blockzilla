use anyhow::{Context, Result};
use solana_pubkey::Pubkey;
use std::{str::FromStr, time::Instant};
use tracing::info;
use tracing::error;
use wincode::Deserialize;

use ahash::AHashMap;

use solana_message::VersionedMessage;
use solana_transaction::versioned::VersionedTransaction;

use car_reader::{
    car_block_group::CarBlockGroup,
    error::GroupError,
    metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame},
    node::{Node, decode_node},
    versioned_transaction::VersionedTransactionSchema,
};

use blockzilla_format::write_registry;

use crate::{epoch_paths, stream_car_blocks, Cli, ProgressTracker, hex_prefix};

pub(crate) fn run(cli: &Cli, epoch: u64) -> Result<()> {
    let (car_path, epoch_dir, registry_path, _, _) = epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }
    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building registry (counting phase) epoch={}", epoch);
    info!("  car:      {}", car_path.display());
    info!("  out:      {}", registry_path.display());

    let mut counter = PubkeyCounter::new(16_000_000);
    let mut progress = ProgressTracker::new("Phase 1/2");

    stream_car_blocks(&car_path, |group| {
        let (blocks_delta, txs_delta, slot) = registry_process_block(group, &mut counter)?;
        if let Some(s) = slot {
            progress.update_slot(s);
        }
        progress.update(blocks_delta, txs_delta);
        Ok(())
    })?;

    progress.final_report();
    info!("Unique pubkeys: {}", counter.counts.len());

    info!("Sorting registry by usage frequency...");
    let sort_start = Instant::now();

    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(ka, ca), (kb, cb)| cb.cmp(ca).then_with(|| ka.cmp(kb)));

    let keys: Vec<[u8; 32]> = items.into_iter().map(|(k, _)| k).collect();

    info!(
        "Sorting completed in {:.2}s",
        sort_start.elapsed().as_secs_f64()
    );

    write_registry(&registry_path, &keys)?;
    info!("Registry written: {} keys", keys.len());

    Ok(())
}

struct PubkeyCounter {
    counts: AHashMap<[u8; 32], u32>,
}

impl PubkeyCounter {
    fn new(cap: usize) -> Self {
        Self {
            counts: AHashMap::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn add32(&mut self, k32: &[u8; 32]) {
        *self.counts.entry(*k32).or_insert(0) += 1;
    }
}

fn registry_process_block(
    group: &CarBlockGroup,
    counter: &mut PubkeyCounter,
) -> Result<(u64, u64, Option<u64>), GroupError> {
    let mut tx_scratch = RegistryTxDecodeScratch::new();
    let mut txs = 0u64;

    let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
        Node::Block(b) => b,
        _ => return Err(GroupError::WrongRootKind),
    };

    let block_slot = block.slot;

    let mut entry_iter = block
        .entries
        .iter_stateful()
        .map_err(|e| GroupError::Node(car_reader::node::NodeDecodeError::from(e)))?;

    while let Some(entry_cid) = entry_iter.next_item() {
        let entry_cid = entry_cid.map_err(|e| GroupError::Node(e.into()))?;
        let Node::Entry(entry) = group.decode_by_hash(entry_cid.hash_bytes())? else {
            continue;
        };

        let mut tx_iter = entry
            .transactions
            .iter_stateful()
            .map_err(|e| GroupError::Node(e.into()))?;

        while let Some(tx_cid) = tx_iter.next_item() {
            let tx_cid = tx_cid.map_err(|e| GroupError::Node(e.into()))?;
            let Node::Transaction(tx) = group.decode_by_hash(tx_cid.hash_bytes())? else {
                continue;
            };

            txs += 1;

            let vtx = tx_scratch.decode_tx(tx.data.data)?;

            match &vtx.message {
                VersionedMessage::Legacy(m) => {
                    for k in &m.account_keys {
                        counter.add32(k.as_array());
                    }
                }
                VersionedMessage::V0(m) => {
                    for k in &m.account_keys {
                        counter.add32(k.as_array());
                    }
                    for l in &m.address_table_lookups {
                        counter.add32(l.account_key.as_array());
                    }
                }
            }

            if !tx.metadata.data.is_empty() {
                let meta = tx_scratch.decode_meta(tx.slot, tx.metadata.data)?;

                for pk in &meta.loaded_writable_addresses {
                    counter.add32(pk.as_slice().try_into().unwrap());
                }
                for pk in &meta.loaded_readonly_addresses {
                    counter.add32(pk.as_slice().try_into().unwrap());
                }

                for tb in meta
                    .pre_token_balances
                    .iter()
                    .chain(meta.post_token_balances.iter())
                {
                    if let Ok(pk) = Pubkey::from_str(&tb.mint) {
                        counter.add32(pk.as_array());
                    }
                    if !tb.owner.is_empty()
                        && let Ok(pk) = Pubkey::from_str(&tb.owner)
                    {
                        counter.add32(pk.as_array());
                    }
                    if !tb.program_id.is_empty()
                        && let Ok(pk) = Pubkey::from_str(&tb.program_id)
                    {
                        counter.add32(pk.as_array());
                    }
                }
            }
        }
    }

    Ok((1, txs, Some(block_slot)))
}

struct RegistryTxDecodeScratch {
    reusable_tx: std::mem::MaybeUninit<VersionedTransaction>,
    has_tx: bool,
    meta_out: car_reader::confirmed_block::TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
}

impl RegistryTxDecodeScratch {
    fn new() -> Self {
        Self {
            reusable_tx: std::mem::MaybeUninit::uninit(),
            has_tx: false,
            meta_out: car_reader::confirmed_block::TransactionStatusMeta::default(),
            zstd: ZstdReusableDecoder::new(256 * 1024),
        }
    }

    #[inline(always)]
    fn decode_tx(&mut self, bytes: &[u8]) -> Result<&VersionedTransaction, GroupError> {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
            self.has_tx = false;
        }

        if let Err(e) = VersionedTransactionSchema::deserialize_into(bytes, &mut self.reusable_tx) {
            error!(
                "TX_DECODE (registry) failed: len={} prefix={}",
                bytes.len(),
                hex_prefix(bytes, 32)
            );
            error!("TX_DECODE (registry) error: {:?}", e);
            return Err(GroupError::TxDecode);
        }

        self.has_tx = true;
        Ok(unsafe { self.reusable_tx.assume_init_ref() })
    }

    #[inline(always)]
    fn decode_meta(
        &mut self,
        slot: u64,
        frame: &[u8],
    ) -> Result<&car_reader::confirmed_block::TransactionStatusMeta, GroupError> {
        decode_transaction_status_meta_from_frame(slot, frame, &mut self.meta_out, &mut self.zstd)
            .map_err(|_| GroupError::TxMetaDecode)?;
        Ok(&self.meta_out)
    }
}

impl Drop for RegistryTxDecodeScratch {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}
