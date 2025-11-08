use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    carblock_to_compact::{
        CompactBlock, CompactMetadataPayload, CompactVersionedTx, MetadataMode, PubkeyIdProvider,
        carblock_to_compactblock_inplace,
    },
    open_epoch::{self, FetchMode},
};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use spl_token::instruction::TokenInstruction;
use std::{path::Path, str::FromStr};
use tokio::fs;
use tracing::info;
use wincode::{SchemaRead, SchemaWrite};

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
    epoch: u64,
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

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
        .await
        .with_context(|| format!("failed to open epoch {epoch:04} from {cache_dir}"))?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let mut block_buf = CompactBlock {
        slot: 0,
        txs: Vec::new(),
        rewards: Vec::new(),
    };
    let mut buf_tx = Vec::with_capacity(128 * 1024);
    let mut buf_meta = Vec::with_capacity(128 * 1024);
    let mut buf_rewards = Vec::new();

    let mut tracked_accounts: AHashSet<u32> = AHashSet::new();
    let mut collected: Vec<TokenTransactionRecord> = Vec::new();

    let token_program_bytes = spl_token::ID.to_bytes();

    let mut blocks_scanned = 0u64;
    let mut txs_seen = 0u64;
    let mut txs_kept = 0u64;
    let mut accounts_tracked = 0u64;

    while let Some(block) = car.next_block().await? {
        let slot = match block.block() {
            Ok(b) => b.slot,
            Err(e) => {
                tracing::warn!("failed to decode block: {e}");
                continue;
            }
        };

        if slot < start_slot {
            continue;
        }

        carblock_to_compactblock_inplace(
            &block,
            &mut registry,
            MetadataMode::Compact,
            &mut buf_tx,
            &mut buf_meta,
            &mut buf_rewards,
            &mut block_buf,
        )?;

        blocks_scanned += 1;
        txs_seen += block_buf.txs.len() as u64;

        for tx in block_buf.txs.drain(..) {
            if transaction_matches(&tx, mint_id, &tracked_accounts) {
                let added = update_tracked_accounts(
                    &tx,
                    mint_id,
                    &token_program_bytes,
                    &mut tracked_accounts,
                    &registry,
                );
                if added > 0 {
                    accounts_tracked += added as u64;
                }

                collected.push(TokenTransactionRecord { slot, tx });
                txs_kept += 1;
            }
        }
    }

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

    info!(
        "🪙 token dump complete: epoch={epoch:04} start_slot={start_slot} blocks_scanned={blocks_scanned} txs_seen={txs_seen} txs_kept={txs_kept} tracked_accounts={tracked_accounts.len()} new_accounts={accounts_tracked} output={}",
        output_path.display()
    );

    Ok(())
}

fn transaction_matches(tx: &CompactVersionedTx, mint_id: u32, tracked: &AHashSet<u32>) -> bool {
    tx.account_ids
        .iter()
        .any(|id| *id == mint_id || tracked.contains(id))
}

fn update_tracked_accounts(
    tx: &CompactVersionedTx,
    mint_id: u32,
    token_program_bytes: &[u8; 32],
    tracked: &mut AHashSet<u32>,
    registry: &DynamicRegistry,
) -> usize {
    let mut added = 0usize;

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
                        added += 1;
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
