use anyhow::{Context, Result, anyhow};
use indicatif::ProgressBar;
use solana_pubkey::Pubkey;
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, BufReader},
};

use crate::optimizer::{BlockWithIds, CompactInstruction, CompactReward, CompactTokenBalance};
pub const LOG_INTERVAL_SECS: u64 = 2;

pub async fn read_compressed_blocks(epoch: u64, input_dir: &str, registry_dir: &str) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let registry_pubkeys = load_registry_pubkeys(registry_dir, epoch).await?;
    let mut registry_usage = RegistryUsage::new(registry_pubkeys);

    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();

    let mut blocks_count = 0u64;
    let mut tx_count = 0u64;
    let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);

    loop {
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;

        decompress_buf.clear();
        zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)
            .context("Failed to decompress block")?;

        let block: BlockWithIds =
            postcard::from_bytes(&decompress_buf).context("Failed to deserialize BlockWithIds")?;

        tx_count += block.num_transactions;
        blocks_count += 1;

        registry_usage.record_block(&block)?;

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                tx_count,
                if elapsed.as_secs() > 0 {
                    tx_count / elapsed.as_secs()
                } else {
                    0
                },
            ));
        }
    }

    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS | {:.1}s",
        blocks_count,
        blocks_count as f64 / start.elapsed().as_secs_f64(),
        tx_count,
        if start.elapsed().as_secs() > 0 {
            tx_count / start.elapsed().as_secs()
        } else {
            0
        },
        start.elapsed().as_secs_f64()
    ));

    registry_usage.print_summary(epoch);

    Ok(())
}

pub async fn analyze_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let mut blocks_count = 0u64;
    let mut total_txs = 0u64;
    let mut total_instructions = 0u64;
    let mut total_inner_instructions = 0u64;
    let mut blocks_with_rewards = 0u64;
    let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);

    let pb = ProgressBar::new_spinner();

    loop {
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;

        decompress_buf.clear();
        zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)?;

        let block: BlockWithIds = postcard::from_bytes(&decompress_buf)?;

        blocks_count += 1;
        total_txs += block.num_transactions;
        if !block.rewards.is_empty() {
            blocks_with_rewards += 1;
        }

        for tx in &block.transactions {
            total_instructions += tx.instructions.len() as u64;
            if let Some(inner) = tx.meta.as_ref().and_then(|m| m.inner_instructions.as_ref()) {
                for ii in inner {
                    total_inner_instructions += ii.instructions.len() as u64;
                }
            }
        }

        if blocks_count % 1000 == 0 {
            pb.set_message(format!("Analyzed {} blocks...", blocks_count));
        }
    }

    pb.finish();

    println!("\n📊 Analysis Results:");
    println!("  Epoch:              {}", epoch);
    println!("  Total Blocks:       {}", blocks_count);
    println!("  Total Transactions: {}", total_txs);
    println!(
        "  Avg TXs/Block:      {:.2}",
        total_txs as f64 / blocks_count as f64
    );
    println!("  Total Instructions: {}", total_instructions);
    println!("  Total Inner Ixs:    {}", total_inner_instructions);
    println!("  Blocks w/ Rewards:  {}", blocks_with_rewards);

    Ok(())
}

async fn load_registry_pubkeys(registry_dir: &str, epoch: u64) -> Result<Vec<Pubkey>> {
    let path = Path::new(registry_dir).join(format!("registry-pubkeys-{epoch:04}.bin"));
    if !path.exists() {
        anyhow::bail!("registry pubkey file not found: {}", path.display());
    }

    let data = fs::read(&path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    if data.len() % 32 != 0 {
        return Err(anyhow!(
            "registry pubkey file {} has invalid length {}",
            path.display(),
            data.len()
        ));
    }

    let mut keys = Vec::with_capacity(data.len() / 32);
    for chunk in data.chunks_exact(32) {
        let mut buf = [0u8; 32];
        buf.copy_from_slice(chunk);
        keys.push(Pubkey::new_from_array(buf));
    }

    Ok(keys)
}

struct RegistryUsage {
    pubkeys: Vec<Pubkey>,
    used: Vec<bool>,
    seen: u64,
    invalid: u64,
    sample_accounts: Option<Vec<String>>,
    sample_rewards: Option<Vec<String>>,
}

impl RegistryUsage {
    fn new(pubkeys: Vec<Pubkey>) -> Self {
        let used = vec![false; pubkeys.len()];
        Self {
            pubkeys,
            used,
            seen: 0,
            invalid: 0,
            sample_accounts: None,
            sample_rewards: None,
        }
    }

    fn len(&self) -> usize {
        self.pubkeys.len()
    }

    fn mark(&mut self, id: u32) -> Option<&Pubkey> {
        let idx = id as usize;
        if idx >= self.pubkeys.len() {
            self.invalid = self.invalid.saturating_add(1);
            return None;
        }

        if !self.used[idx] {
            self.used[idx] = true;
            self.seen = self.seen.saturating_add(1);
        }

        self.pubkeys.get(idx)
    }

    fn mark_reward(&mut self, reward: &CompactReward) {
        let maybe_key = self.mark(reward.pubkey).map(|k| k.to_string());

        if let Some(key) = maybe_key {
            if self.sample_rewards.is_none() {
                self.sample_rewards = Some(Vec::new());
            }
            if let Some(rewards) = self.sample_rewards.as_mut() {
                if rewards.len() < 5 {
                    rewards.push(format!("{key} -> {}", reward.lamports));
                }
            }
        }
    }

    fn record_instruction(&mut self, ix: &CompactInstruction) {
        let _ = self.mark(ix.program_id);
    }

    fn record_token_balance(&mut self, tb: &CompactTokenBalance) {
        let _ = self.mark(tb.mint);
        let _ = self.mark(tb.owner);
        let _ = self.mark(tb.program_id);
    }

    fn record_block(&mut self, block: &BlockWithIds) -> Result<()> {
        for reward in &block.rewards {
            self.mark_reward(reward);
        }

        for tx in &block.transactions {
            if self.sample_accounts.is_none() {
                let mut accounts = Vec::new();
                for &id in tx.account_keys.iter().take(12) {
                    if let Some(key) = self.mark(id).map(|pk| pk.to_string()) {
                        accounts.push(key);
                    }
                }
                if !accounts.is_empty() {
                    self.sample_accounts = Some(accounts);
                }
            } else {
                for &id in &tx.account_keys {
                    let _ = self.mark(id);
                }
            }

            for ix in &tx.instructions {
                self.record_instruction(ix);
            }

            if let Some(lookups) = &tx.address_table_lookups {
                for lookup in lookups {
                    let _ = self.mark(lookup.account_key);
                }
            }

            if let Some(meta) = &tx.meta {
                if let Some(inner) = &meta.inner_instructions {
                    for ix in inner {
                        for nested in &ix.instructions {
                            self.record_instruction(nested);
                        }
                    }
                }

                if let Some(rewards) = &meta.rewards {
                    for reward in rewards {
                        self.mark_reward(reward);
                    }
                }

                if let Some(loaded) = &meta.loaded_addresses {
                    for &id in &loaded.writable {
                        let _ = self.mark(id);
                    }
                    for &id in &loaded.readonly {
                        let _ = self.mark(id);
                    }
                }

                if let Some((program_id, _)) = &meta.return_data {
                    let _ = self.mark(*program_id);
                }

                if let Some(balances) = &meta.pre_token_balances {
                    for tb in balances {
                        self.record_token_balance(tb);
                    }
                }

                if let Some(balances) = &meta.post_token_balances {
                    for tb in balances {
                        self.record_token_balance(tb);
                    }
                }
            }
        }

        Ok(())
    }

    fn print_summary(&self, epoch: u64) {
        println!("\n📚 Registry usage summary");
        println!("  Epoch:                   {epoch}");
        println!("  Registry entries:        {}", self.len());
        println!("  Referenced entries:      {}", self.seen);
        if self.invalid > 0 {
            println!("  ⚠️  Invalid references:   {}", self.invalid);
        }

        if let Some(accounts) = &self.sample_accounts {
            println!("  Sample transaction accounts:");
            for key in accounts {
                println!("    - {key}");
            }
        }

        if let Some(rewards) = &self.sample_rewards {
            println!("  Sample rewards:");
            for entry in rewards {
                println!("    - {entry}");
            }
        }
    }
}
