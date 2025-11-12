use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::{CarBlock, CarBlockReader},
    carblock_to_compact::{
        CompactMetadataPayload, CompactVersionedTx, MetadataMode, PubkeyIdProvider,
        transaction_node_to_compact,
    },
    node::{Node, TransactionNode},
    open_epoch::{self, FetchMode},
    transaction_parser::{VersionedTransaction, parse_account_keys_only},
};
use cid::Cid;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use spl_token::instruction::TokenInstruction;
use std::io::{ErrorKind, Read};
use std::time::{Duration, Instant};
use std::{mem::MaybeUninit, path::Path, str::FromStr};
use tokio::fs;
use wincode::{SchemaRead, SchemaWrite};

use crate::LOG_INTERVAL_SECS;

#[derive(Debug, Default)]
struct DynamicRegistry {
    map: AHashMap<[u8; 32], u32>,
    keys: Vec<[u8; 32]>,
}

impl DynamicRegistry {
    fn ensure_id(&mut self, key: &[u8; 32]) -> u32 {
        if let Some(id) = self.map.get(key) {
            *id
        } else {
            let id = self.keys.len() as u32;
            self.keys.push(*key);
            self.map.insert(*key, id);
            id
        }
    }

    fn key_for(&self, id: u32) -> Option<&[u8; 32]> {
        self.keys.get(id as usize)
    }

    fn into_registry(self) -> Vec<[u8; 32]> {
        self.keys
    }
}

impl PubkeyIdProvider for DynamicRegistry {
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32> {
        Some(self.ensure_id(key))
    }
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct TokenTransactionRecord {
    pub slot: u64,
    pub tx: CompactVersionedTx,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct TokenTransactionDump {
    pub mint: [u8; 32],
    pub registry: Vec<[u8; 32]>,
    pub transactions: Vec<TokenTransactionRecord>,
}

pub async fn dump_token_transactions(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    mint_str: &str,
    output_path: &Path,
) -> Result<()> {
    let mint =
        Pubkey::from_str(mint_str).map_err(|e| anyhow!("invalid mint pubkey '{mint_str}': {e}"))?;
    let mint_bytes = mint.to_bytes();

    let mut registry = DynamicRegistry::default();
    let mint_id = registry.ensure_id(&mint_bytes);

    let cache_path = Path::new(cache_dir);
    let Some(last_epoch) = find_last_cached_epoch(cache_path).await? else {
        return Err(anyhow!(
            "no cached epochs found in {}",
            cache_path.display()
        ));
    };

    if start_epoch > last_epoch {
        return Err(anyhow!(
            "start epoch {start_epoch:04} is beyond last cached epoch {last_epoch:04}"
        ));
    }

    let mut buf_tx = Vec::with_capacity(128 * 1024);
    let mut buf_meta = Vec::with_capacity(128 * 1024);

    let mut tracked_account_ids: AHashSet<u32> = AHashSet::new();
    let mut tracked_account_keys: AHashSet<[u8; 32]> = AHashSet::new();
    let mut collected: Vec<TokenTransactionRecord> = Vec::new();

    let token_program_bytes = spl_token::ID.to_bytes();

    // logging totals
    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();

    let mut blocks_scanned = 0u64;
    let mut txs_seen = 0u64; // total transactions scanned
    let mut txs_kept = 0u64; // transactions where instruction matched our filter
    let mut _accounts_tracked_new = 0u64; // number of new tracked accounts discovered
    let mut bytes_count = 0u64;
    let mut _entry_count = 0u64;

    let mut min_slot = start_slot;
    let mut last_epoch_processed = None;

    for epoch in start_epoch..=last_epoch {
        let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
            .await
            .with_context(|| format!("failed to open epoch {epoch:04} from {cache_dir}"))?;
        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;
        last_epoch_processed = Some(epoch);

        while let Some(block) = car.next_block().await? {
            // align metrics with block reader
            _entry_count += block.entries.len() as u64;
            bytes_count += block.entries.iter().map(|(_, a)| a.len()).sum::<usize>() as u64;

            let slot = match peek_block_slot(&block) {
                Ok(slot) => slot,
                Err(e) => {
                    tracing::warn!("failed to decode block: {e}");
                    continue;
                }
            };

            if slot < min_slot {
                continue;
            }

            blocks_scanned += 1;

            let block_node = match block.block() {
                Ok(node) => node,
                Err(e) => {
                    tracing::warn!("failed to decode block: {e}");
                    continue;
                }
            };

            let mut reusable_tx = MaybeUninit::<VersionedTransaction>::uninit();
            let mut account_keys = SmallVec::<[Pubkey; 256]>::new();

            for entry_cid_res in block_node.entries.iter() {
                let entry_cid = match entry_cid_res {
                    Ok(cid) => cid,
                    Err(e) => {
                        tracing::warn!("failed to decode entry cid: {e}");
                        continue;
                    }
                };

                let entry = match block.decode(entry_cid.hash_bytes()) {
                    Ok(Node::Entry(entry)) => entry,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!("failed to decode entry: {e}");
                        continue;
                    }
                };

                for tx_cid_res in entry.transactions.iter() {
                    let tx_cid = match tx_cid_res {
                        Ok(cid) => cid,
                        Err(e) => {
                            tracing::warn!("failed to decode transaction cid: {e}");
                            continue;
                        }
                    };

                    let tx_node = match block.decode(tx_cid.hash_bytes()) {
                        Ok(Node::Transaction(tx_node)) => tx_node,
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::warn!("failed to decode transaction node: {e}");
                            continue;
                        }
                    };

                    let keep_tx = match transaction_bytes(&block, &tx_node, &mut buf_tx) {
                        Ok(tx_bytes) => {
                            txs_seen += 1;

                            if let Err(e) = parse_account_keys_only(tx_bytes, &mut account_keys) {
                                tracing::warn!("failed to parse account keys: {e}");
                                false
                            } else {
                                transaction_matches(
                                    &account_keys,
                                    &mint_bytes,
                                    &tracked_account_keys,
                                )
                            }
                        }
                        Err(e) => {
                            tracing::warn!("failed to read transaction bytes: {e}");
                            continue;
                        }
                    };

                    if !keep_tx {
                        continue;
                    }

                    let tx = match transaction_node_to_compact(
                        &block,
                        &tx_node,
                        &mut registry,
                        MetadataMode::Compact,
                        &mut buf_tx,
                        &mut buf_meta,
                        &mut reusable_tx,
                    ) {
                        Ok(tx) => tx,
                        Err(e) => {
                            tracing::warn!("failed to compact transaction: {e}");
                            continue;
                        }
                    };

                    let added_keys = update_tracked_accounts(
                        &tx,
                        mint_id,
                        &token_program_bytes,
                        &mut tracked_account_ids,
                        &registry,
                    );
                    if !added_keys.is_empty() {
                        _accounts_tracked_new += added_keys.len() as u64;
                        for key in added_keys {
                            tracked_account_keys.insert(key);
                        }
                    }

                    collected.push(TokenTransactionRecord { slot, tx });
                    txs_kept += 1;
                }
            }

            // periodic log - same style as block reader, but with tx stats
            let now = Instant::now();
            if now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS) {
                last_log = now;
                let elapsed = now.duration_since(start);
                let secs = elapsed.as_secs_f64().max(1e-6);

                let blk_s = blocks_scanned as f64 / secs;
                let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;
                let tps = if elapsed.as_secs() > 0 {
                    txs_seen / elapsed.as_secs()
                } else {
                    0
                };
                let tracked_accounts_len = tracked_account_keys.len();

                pb.set_message(format!(
                    "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | tx={} kept={} tracked={}",
                    blocks_scanned,
                    blk_s,
                    mb_s,
                    tps,
                    txs_seen,
                    txs_kept,
                    tracked_accounts_len
                ));
            }
        }

        min_slot = 0;
    }

    let Some(last_epoch_processed) = last_epoch_processed else {
        return Err(anyhow!(
            "no epochs processed between {start_epoch:04} and {last_epoch:04}"
        ));
    };

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let dump = TokenTransactionDump {
        mint: mint_bytes,
        registry: registry.into_registry(),
        transactions: collected,
    };

    let encoded = wincode::serialize(&dump)?;
    fs::write(output_path, encoded).await?;

    // final line like block reader, with tx stats
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-6);
    let blk_s = blocks_scanned as f64 / secs;
    let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;
    let tps = if elapsed.as_secs() > 0 {
        txs_seen / elapsed.as_secs()
    } else {
        0
    };
    let tracked_accounts_len = tracked_account_keys.len();

    let range_label = if start_epoch == last_epoch_processed {
        format!("epoch {start_epoch:04}")
    } else {
        format!("epochs {start_epoch:04}-{last_epoch_processed:04}")
    };

    tracing::info!(
        "{range_label} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | tx={} kept={} tracked={}",
        blocks_scanned,
        blk_s,
        mb_s,
        tps,
        txs_seen,
        txs_kept,
        tracked_accounts_len
    );

    Ok(())
}

async fn find_last_cached_epoch(cache_dir: &Path) -> Result<Option<u64>> {
    let mut rd = match fs::read_dir(cache_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut max_epoch = None;
    while let Some(entry) = rd.next_entry().await? {
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        let Some(epoch_str) = name.strip_prefix("epoch-").and_then(|s| {
            s.strip_suffix(".car")
                .or_else(|| s.strip_suffix(".car.zst"))
        }) else {
            continue;
        };

        let Ok(epoch) = epoch_str.parse::<u64>() else {
            continue;
        };

        max_epoch = Some(max_epoch.map_or(epoch, |current: u64| current.max(epoch)));
    }

    Ok(max_epoch)
}

fn peek_block_slot(block: &CarBlock) -> Result<u64> {
    let mut decoder = minicbor::Decoder::new(block.block_bytes.as_ref());
    decoder
        .array()
        .map_err(|e| anyhow!("failed to decode block header array: {e}"))?;
    let kind = decoder
        .u64()
        .map_err(|e| anyhow!("failed to decode block kind: {e}"))?;
    if kind != 2 {
        return Err(anyhow!("unexpected block kind {kind}, expected Block"));
    }
    let slot = decoder
        .u64()
        .map_err(|e| anyhow!("failed to decode block slot: {e}"))?;
    Ok(slot)
}

fn copy_dataframe_into<'a>(
    block: &CarBlock,
    cid: &Cid,
    dst: &'a mut Vec<u8>,
    inline_prefix: Option<&[u8]>,
) -> Result<&'a [u8]> {
    let prefix_len = inline_prefix.map_or(0, |p| p.len());
    dst.clear();
    dst.reserve(prefix_len + (64 << 10));

    if let Some(prefix) = inline_prefix {
        dst.extend_from_slice(prefix);
    }

    let mut rdr = block.dataframe_reader(cid);
    rdr.read_to_end(dst)
        .map_err(|e| anyhow!("read dataframe chain: {e}"))?;
    Ok(&*dst)
}

fn transaction_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'_>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.data.next {
        None => {
            buf.clear();
            buf.extend_from_slice(tx_node.data.data);
            Ok(&*buf)
        }
        Some(df_cbor) => {
            let df_cid = df_cbor.to_cid()?;
            copy_dataframe_into(block, &df_cid, buf, None)
        }
    }
}

fn transaction_matches(
    account_keys: &[Pubkey],
    mint: &[u8; 32],
    tracked: &AHashSet<[u8; 32]>,
) -> bool {
    account_keys.iter().any(|pk| {
        let key = pk.to_bytes();
        key == *mint || tracked.contains(&key)
    })
}

fn update_tracked_accounts(
    tx: &CompactVersionedTx,
    mint_id: u32,
    token_program_bytes: &[u8; 32],
    tracked: &mut AHashSet<u32>,
    registry: &DynamicRegistry,
) -> Vec<[u8; 32]> {
    let mut added = Vec::new();

    let mut handle_instruction = |program_idx: usize, accounts: &[u8], data: &[u8]| {
        if program_idx >= tx.account_ids.len() {
            return;
        }
        let program_account_id = tx.account_ids[program_idx];
        let Some(program_key) = registry.key_for(program_account_id) else {
            return;
        };
        if program_key != token_program_bytes {
            return;
        }

        if let Ok(instr) = TokenInstruction::unpack(data) {
            match instr {
                TokenInstruction::InitializeAccount
                | TokenInstruction::InitializeAccount2 { .. }
                | TokenInstruction::InitializeAccount3 { .. } => {
                    if accounts.len() < 2 {
                        return;
                    }
                    let account_idx = accounts[0] as usize;
                    let mint_idx = accounts[1] as usize;

                    if account_idx >= tx.account_ids.len() || mint_idx >= tx.account_ids.len() {
                        return;
                    }

                    let candidate_id = tx.account_ids[account_idx];
                    let mint_account_id = tx.account_ids[mint_idx];
                    if mint_account_id != mint_id {
                        return;
                    }

                    if tracked.insert(candidate_id) {
                        if let Some(candidate_key) = registry.key_for(candidate_id) {
                            added.push(*candidate_key);
                        }
                    }
                }
                _ => {}
            }
        }
    };

    for ix in &tx.instructions {
        handle_instruction(ix.program_id_index as usize, &ix.accounts, &ix.data);
    }

    if let Some(CompactMetadataPayload::Compact(meta)) = &tx.metadata {
        if let Some(inner_sets) = &meta.inner_instructions {
            for inner in inner_sets {
                for ix in &inner.instructions {
                    handle_instruction(ix.program_id_index as usize, &ix.accounts, &ix.data);
                }
            }
        }
    }

    added
}
