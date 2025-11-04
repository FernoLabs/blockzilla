use anyhow::{Context, Result, anyhow};
use indicatif::ProgressBar;
use solana_pubkey::Pubkey;
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::{Mutex, mpsc},
    task,
};

use crate::{
    LOG_INTERVAL_SECS,
    optimizer::{BlockWithIds, CompactInstruction, CompactReward, CompactTokenBalance},
};

pub async fn read_compressed_blocks(
    epoch: u64,
    input_dir: &str,
    registry_dir: &str,
    jobs: usize,
) -> Result<()> {
    let registry_pubkeys = load_registry_pubkeys(registry_dir, epoch).await?;
    let usage = RegistryUsage::new(registry_pubkeys);
    let jobs = jobs.max(1);

    let usage = if jobs == 1 {
        read_compressed_blocks_seq(epoch, input_dir, usage).await?
    } else {
        read_compressed_blocks_parallel(epoch, input_dir, usage, jobs).await?
    };

    usage.print_summary(epoch);

    Ok(())
}

async fn read_compressed_blocks_seq(
    epoch: u64,
    input_dir: &str,
    mut usage: RegistryUsage,
) -> Result<RegistryUsage> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let start = Instant::now();
    let mut stats = ReaderStats::new(start);
    let pb = ProgressBar::new_spinner();
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

        stats.record_block(&block, &mut usage, epoch, start, &pb)?;
    }

    stats.finish(epoch, start, &pb);

    Ok(usage)
}

async fn read_compressed_blocks_parallel(
    epoch: u64,
    input_dir: &str,
    mut usage: RegistryUsage,
    jobs: usize,
) -> Result<RegistryUsage> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let start = Instant::now();
    let mut stats = ReaderStats::new(start);
    let pb = ProgressBar::new_spinner();

    let (work_tx, work_rx) = mpsc::channel::<Vec<u8>>(jobs * 4);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (result_tx, mut result_rx) = mpsc::channel::<WorkerMessage>(jobs * 4);

    for _ in 0..jobs {
        let work_rx = Arc::clone(&work_rx);
        let result_tx = result_tx.clone();
        task::spawn_blocking(move || {
            let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);

            loop {
                let compressed = {
                    let mut guard = work_rx.blocking_lock();
                    guard.blocking_recv()
                };

                let Some(compressed) = compressed else {
                    break;
                };

                decompress_buf.clear();
                let message = (|| {
                    zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)
                        .map_err(|e| format!("Failed to decompress block: {e}"))?;
                    postcard::from_bytes(&decompress_buf)
                        .map_err(|e| format!("Failed to deserialize BlockWithIds: {e}"))
                })();

                match message {
                    Ok(block) => {
                        if result_tx
                            .blocking_send(WorkerMessage::Block(block))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = result_tx.blocking_send(WorkerMessage::Error(err));
                        break;
                    }
                }
            }
        });
    }
    drop(result_tx);

    let mut pending = 0usize;

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

        if work_tx.send(compressed).await.is_err() {
            break;
        }

        pending = pending.saturating_add(1);
        drain_ready_results(
            &mut result_rx,
            &mut pending,
            &mut usage,
            &mut stats,
            epoch,
            start,
            &pb,
        )?;
    }

    drop(work_tx);

    drain_ready_results(
        &mut result_rx,
        &mut pending,
        &mut usage,
        &mut stats,
        epoch,
        start,
        &pb,
    )?;

    while pending > 0 {
        let message = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("worker channel closed unexpectedly"))?;
        process_worker_message(message, &mut usage, &mut stats, epoch, start, &pb)?;
        if pending > 0 {
            pending -= 1;
        }
    }

    stats.finish(epoch, start, &pb);

    Ok(usage)
}

struct ReaderStats {
    blocks_count: u64,
    tx_count: u64,
    last_log: Instant,
}

impl ReaderStats {
    fn new(start: Instant) -> Self {
        Self {
            blocks_count: 0,
            tx_count: 0,
            last_log: start,
        }
    }

    fn record_block(
        &mut self,
        block: &BlockWithIds,
        usage: &mut RegistryUsage,
        epoch: u64,
        start: Instant,
        pb: &ProgressBar,
    ) -> Result<()> {
        usage.record_block(block)?;
        self.blocks_count = self.blocks_count.saturating_add(1);
        self.tx_count = self.tx_count.saturating_add(block.num_transactions);

        let now = Instant::now();
        if now.duration_since(self.last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            self.last_log = now;
            let elapsed = now.duration_since(start);
            let block_rate = if elapsed.as_secs_f64() > 0.0 {
                self.blocks_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS",
                self.blocks_count,
                block_rate,
                self.tx_count,
                if elapsed.as_secs() > 0 {
                    self.tx_count / elapsed.as_secs()
                } else {
                    0
                },
            ));
        }

        Ok(())
    }

    fn finish(&self, epoch: u64, start: Instant, pb: &ProgressBar) {
        let elapsed = start.elapsed();
        pb.finish_with_message(format!(
            "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS | {:.1}s",
            self.blocks_count,
            if elapsed.as_secs_f64() > 0.0 {
                self.blocks_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            self.tx_count,
            if elapsed.as_secs() > 0 {
                self.tx_count / elapsed.as_secs()
            } else {
                0
            },
            elapsed.as_secs_f64()
        ));
    }
}

enum WorkerMessage {
    Block(BlockWithIds),
    Error(String),
}

fn process_worker_message(
    message: WorkerMessage,
    usage: &mut RegistryUsage,
    stats: &mut ReaderStats,
    epoch: u64,
    start: Instant,
    pb: &ProgressBar,
) -> Result<()> {
    match message {
        WorkerMessage::Block(block) => {
            stats.record_block(&block, usage, epoch, start, pb)?;
        }
        WorkerMessage::Error(err) => return Err(anyhow!(err)),
    }

    Ok(())
}

fn drain_ready_results(
    result_rx: &mut mpsc::Receiver<WorkerMessage>,
    pending: &mut usize,
    usage: &mut RegistryUsage,
    stats: &mut ReaderStats,
    epoch: u64,
    start: Instant,
    pb: &ProgressBar,
) -> Result<()> {
    loop {
        match result_rx.try_recv() {
            Ok(message) => {
                process_worker_message(message, usage, stats, epoch, start, pb)?;
                if *pending > 0 {
                    *pending -= 1;
                }
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                return Err(anyhow!("worker channel closed unexpectedly"));
            }
        }
    }

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
