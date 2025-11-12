// Extract new token accounts from InitializeAccount instructions
// account_keys_buf is already parsed and reused
fn extract_new_tracked_accounts(
    tx: &VersionedTransaction,
    account_keys: &[Pubkey],
    mint_bytes: &[u8; 32],
    token_program_bytes: &[u8; 32],
    tracked: &mut AHashSet<[u8; 32]>,
    added: &mut Vec<[u8; 32]>,
) {
    added.clear();

    let mut handle_instruction = |program_idx: usize, accounts: &[u8], data: &[u8]| {
        if program_idx >= account_keys.len() {
            return;
        }
        
        let program_key = account_keys[program_idx].to_bytes();
        if &program_key != token_program_bytes {
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

                    if account_idx >= account_keys.len() || mint_idx >= account_keys.len() {
                        return;
                    }

                    let candidate_key = account_keys[account_idx].to_bytes();
                    let mint_key = account_keys[mint_idx].to_bytes();
                    
                    if &mint_key != mint_bytes {
                        return;
                    }

                    if tracked.insert(candidate_key) {
                        added.push(candidate_key);
                    }
                }
                _ => {}
            }
        }
    };

    // Handle top-level instructions using message API
    for ix in tx.message.instructions_iter() {
        handle_instruction(
            ix.program_id_index as usize,
            &ix.accounts,
            &ix.data,
        );
    }
}use ahash::AHashSet;
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::{CarBlock, CarBlockReader},
    node::{Node, TransactionNode},
    open_epoch::{self, FetchMode},
    partial_meta::extract_metadata_pubkeys,
    transaction_parser::{VersionedTransaction, parse_account_keys_only},
};
use cid::Cid;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use spl_token::instruction::TokenInstruction;
use std::fmt::Write as FmtWrite;
use std::io::{ErrorKind, IsTerminal, Read};
use std::time::{Duration, Instant};
use std::{
    mem::MaybeUninit,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::fs;
use wincode::Deserialize as _;
use wincode::{SchemaRead, SchemaWrite};

use crate::LOG_INTERVAL_SECS;

const BLOCK_LOG_CHUNK: u64 = 500;

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct TokenTransactionRecord {
    pub slot: u64,
    pub tx_bytes: Vec<u8>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct TokenTransactionDump {
    pub mint: [u8; 32],
    pub transactions: Vec<TokenTransactionRecord>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct TokenTransactionEpochDump {
    pub mint: [u8; 32],
    pub epoch: u64,
    pub last_slot: u64,
    pub tracked_account_keys: Vec<[u8; 32]>,
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

    let parts_dir = output_path.with_extension("parts");

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

    // Pre-allocate collections
    let mut tracked_account_keys: AHashSet<[u8; 32]> = AHashSet::with_capacity(10_000);
    
    // Estimate capacity
    let epoch_span = (last_epoch - start_epoch + 1) as usize;
    let estimated_capacity = (epoch_span * 500_000).min(10_000_000);
    let mut collected: Vec<TokenTransactionRecord> = Vec::with_capacity(estimated_capacity);
    
    let mut processed_epochs: AHashSet<u64> = AHashSet::with_capacity(epoch_span);
    let mut blocks_scanned = 0u64;
    let mut txs_seen = 0u64;
    let mut txs_kept = 0u64;
    let mut accounts_tracked_new_win = 0u64;
    let mut bytes_count = 0u64;
    let mut _entry_count = 0u64;
    let mut last_processed_slot = 0u64;
    let mut last_epoch_processed = None;

    let token_program_bytes = spl_token::ID.to_bytes();

    // Reusable buffers
    let mut added_keys_buf = Vec::with_capacity(64);
    let mut msg_buf = String::with_capacity(256);
    let mut account_keys_buf = SmallVec::<[Pubkey; 256]>::new();

    // Load cached epoch dumps
    if let Ok(mut rd) = fs::read_dir(&parts_dir).await {
        let mut part_entries: Vec<(u64, PathBuf)> = Vec::new();
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
            let Some(epoch_str) = name
                .strip_prefix("epoch-")
                .and_then(|s| s.strip_suffix(".bin"))
            else {
                continue;
            };
            let Ok(epoch) = epoch_str.parse::<u64>() else {
                continue;
            };
            part_entries.push((epoch, entry.path()));
        }
        part_entries.sort_unstable_by_key(|(epoch, _)| *epoch);

        for (epoch, path) in part_entries {
            if epoch < start_epoch || epoch > last_epoch {
                continue;
            }

            let data = match fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!("failed to read cached epoch dump {}: {}", path.display(), e);
                    continue;
                }
            };

            let part: TokenTransactionEpochDump = match wincode::deserialize(&data) {
                Ok(part) => part,
                Err(e) => {
                    tracing::warn!(
                        "failed to decode cached epoch dump {}: {}",
                        path.display(),
                        e
                    );
                    continue;
                }
            };

            if part.mint != mint_bytes {
                return Err(anyhow!(
                    "cached epoch dump mint mismatch for {}",
                    path.display()
                ));
            }

            tracked_account_keys.clear();
            tracked_account_keys.extend(part.tracked_account_keys.iter().copied());

            last_processed_slot = part.last_slot;
            let new_txs = part.transactions.len() as u64;
            txs_kept += new_txs;
            txs_seen += new_txs;
            collected.extend(part.transactions);
            processed_epochs.insert(epoch);
            last_epoch_processed = Some(epoch);
        }
    }

    // progress bar setup
    let draw_target = if std::io::stdout().is_terminal() {
        ProgressDrawTarget::stderr_with_hz(8)
    } else {
        ProgressDrawTarget::hidden()
    };
    let pb = ProgressBar::with_draw_target(Some(0), draw_target);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    pb.enable_steady_tick(Duration::from_millis(120));

    // timers and counters
    let start = Instant::now();
    let mut last_log = start - Duration::from_secs(LOG_INTERVAL_SECS);

    // skip-phase controls for first epoch
    let mut min_slot = if processed_epochs.contains(&start_epoch) {
        0
    } else {
        start_slot
    };
    let mut skip_first_slot: Option<u64> = None;
    let mut skip_first_instant: Option<Instant> = None;
    let mut skipped_blocks = 0u64;
    let mut skipped_bytes = 0u64;
    let mut last_skipped_slot = 0u64;
    let mut printed_skipped_summary = false;

    msg_buf.clear();
    write!(
        &mut msg_buf,
        "starting dump | mint={} | start_epoch={:04} start_slot={} | scanning up to {:04}",
        mint_str, start_epoch, start_slot, last_epoch
    ).unwrap();
    pb.set_message(msg_buf.clone());

    for epoch in start_epoch..=last_epoch {
        if processed_epochs.contains(&epoch) {
            if pb.is_hidden() {
                tracing::info!("skipping epoch {epoch:04} (already cached)");
            } else {
                pb.println(format!("skipping epoch {epoch:04} (already cached)"));
            }
            last_epoch_processed = Some(epoch);
            continue;
        }

        let epoch_start_idx = collected.len();

        // announce epoch
        if pb.is_hidden() {
            tracing::info!("opening epoch {epoch:04}");
        } else {
            pb.println(format!("opening epoch {epoch:04}"));
        }

        let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
            .await
            .with_context(|| format!("failed to open epoch {epoch:04} from {cache_dir}"))?;
        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;
        last_epoch_processed = Some(epoch);

        while let Some(block) = car.next_block().await? {
            _entry_count += block.entries.len() as u64;
            let block_bytes = block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;
            bytes_count += block_bytes;

            let slot = match peek_block_slot(&block) {
                Ok(slot) => slot,
                Err(e) => {
                    tracing::warn!("failed to decode block: {e}");
                    continue;
                }
            };

            // skip phase until start_slot in the first epoch
            if min_slot > 0 && slot < min_slot {
                if skip_first_slot.is_none() {
                    skip_first_slot = Some(slot);
                    skip_first_instant = Some(Instant::now());
                }
                skipped_blocks += 1;
                skipped_bytes += block_bytes;
                last_skipped_slot = slot;

                let now = Instant::now();
                if now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS) {
                    last_log = now;
                    
                    msg_buf.clear();
                    write!(
                        &mut msg_buf,
                        "epoch {:04} | skipping... slot={} -> start_slot={} | skipped={} ({:.2} MB) | ",
                        epoch, slot, min_slot, skipped_blocks,
                        skipped_bytes as f64 / (1024.0 * 1024.0)
                    ).unwrap();

                    if let (Some(s0), Some(t0)) = (skip_first_slot, skip_first_instant) {
                        let s_delta = slot.saturating_sub(s0);
                        let secs = now.saturating_duration_since(t0).as_secs_f64();
                        if s_delta > 0 && secs > 0.0 && min_slot > slot {
                            let rate = s_delta as f64 / secs;
                            if rate > 0.0 {
                                let remain = (min_slot - slot) as f64;
                                write_human_eta(&mut msg_buf, remain / rate).unwrap();
                            } else {
                                msg_buf.push_str("eta n/a");
                            }
                        } else {
                            msg_buf.push_str("eta n/a");
                        }
                    } else {
                        msg_buf.push_str("eta n/a");
                    }

                    if pb.is_hidden() {
                        tracing::info!("{}", msg_buf);
                    } else {
                        pb.set_message(msg_buf.clone());
                        pb.tick();
                    }
                }
                continue;
            }

            // we just reached the start slot
            if !printed_skipped_summary && skipped_blocks > 0 {
                printed_skipped_summary = true;
                let line = format!(
                    "reached start_slot={min_slot} | skipped {} blocks ({:.2} MB), last skipped slot {last_skipped_slot}",
                    skipped_blocks,
                    skipped_bytes as f64 / (1024.0 * 1024.0)
                );
                if pb.is_hidden() {
                    tracing::info!("{line}");
                } else {
                    pb.println(line);
                }
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

                    // Read transaction bytes into buf_tx (reusable buffer)
                    let tx_bytes_slice = match transaction_bytes(&block, &tx_node, &mut buf_tx) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::warn!("failed to read transaction bytes: {e}");
                            continue;
                        }
                    };

                    txs_seen += 1;

                    // PASS 1: Fast path - parse account keys only (cheap)
                    account_keys_buf.clear();
                    if parse_account_keys_only(tx_bytes_slice, &mut account_keys_buf)
                        .ok()
                        .flatten()
                        .is_none()
                    {
                        continue;
                    }

                    // For V0 transactions, also extract loaded addresses from metadata
                    if let Some(_lookups) = tx_bytes_slice.get(0).filter(|&&b| b & 0x80 != 0) {
                        // This is a V0 transaction, need to load addresses from metadata
                        let meta_bytes = match metadata_bytes(&block, &tx_node, &mut buf_meta) {
                            Ok(bytes) => bytes,
                            Err(_) => &[][..],
                        };
                        
                        if !meta_bytes.is_empty() {
                            let _ = extract_metadata_pubkeys(meta_bytes, &mut account_keys_buf);
                        }
                    }

                    // Now account_keys_buf has ALL keys: static + loaded
                    // Quick check: does transaction reference mint or any tracked account?
                    let mut involves_token = false;
                    for key in &account_keys_buf {
                        let key_bytes = key.to_bytes();
                        if &key_bytes == &mint_bytes || tracked_account_keys.contains(&key_bytes) {
                            involves_token = true;
                            break;
                        }
                    }

                    if !involves_token {
                        continue;
                    }

                    // PASS 2: Full deserialization only for token-involved transactions
                    if let Err(e) =
                        VersionedTransaction::deserialize_into(tx_bytes_slice, &mut reusable_tx)
                    {
                        tracing::warn!("failed to parse transaction: {e}");
                        continue;
                    }

                    let tx_ref = unsafe { reusable_tx.assume_init_ref() };

                    // Extract new tracked accounts from InitializeAccount instructions
                    // account_keys_buf already has ALL keys (static + loaded)
                    extract_new_tracked_accounts(
                        tx_ref,
                        &account_keys_buf,
                        &mint_bytes,
                        &token_program_bytes,
                        &mut tracked_account_keys,
                        &mut added_keys_buf,
                    );

                    unsafe { std::ptr::drop_in_place(reusable_tx.as_mut_ptr()) };

                    if !added_keys_buf.is_empty() {
                        accounts_tracked_new_win += added_keys_buf.len() as u64;
                        added_keys_buf.clear();
                    }

                    // Store the raw transaction bytes (copy from buf_tx which still has them)
                    collected.push(TokenTransactionRecord {
                        slot,
                        tx_bytes: tx_bytes_slice.to_vec(),
                    });
                    txs_kept += 1;
                }
            }

            last_processed_slot = slot;

            // periodic metrics by time or every N blocks
            let now = Instant::now();
            let time_due = now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS);
            let chunk_due = blocks_scanned % BLOCK_LOG_CHUNK == 0;
            if time_due || chunk_due {
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

                msg_buf.clear();
                write!(
                    &mut msg_buf,
                    "epoch {:04} | slot={} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | tx={} kept={} tracked={} (+{} new)",
                    epoch,
                    last_processed_slot,
                    blocks_scanned,
                    blk_s,
                    mb_s,
                    tps,
                    txs_seen,
                    txs_kept,
                    tracked_accounts_len,
                    accounts_tracked_new_win
                ).unwrap();

                if pb.is_hidden() {
                    tracing::info!("{}", msg_buf);
                } else {
                    pb.set_message(msg_buf.clone());
                    pb.tick();
                }
                accounts_tracked_new_win = 0;
            }
        }

        // Save epoch checkpoint
        let mut tracked_keys_vec: Vec<[u8; 32]> = tracked_account_keys.iter().copied().collect();
        tracked_keys_vec.sort_unstable();
        let epoch_transactions = collected[epoch_start_idx..].to_vec();
        let epoch_dump = TokenTransactionEpochDump {
            mint: mint_bytes,
            epoch,
            last_slot: last_processed_slot,
            tracked_account_keys: tracked_keys_vec,
            transactions: epoch_transactions,
        };

        fs::create_dir_all(&parts_dir).await?;
        let part_path = parts_dir.join(format!("epoch-{epoch:04}.bin"));
        let tmp_path = part_path.with_extension("bin.tmp");
        let encoded_epoch = wincode::serialize(&epoch_dump)?;
        fs::write(&tmp_path, &encoded_epoch).await?;
        fs::rename(&tmp_path, &part_path).await?;
        processed_epochs.insert(epoch);

        // after the first epoch, clear min_slot so next epochs start from 0
        min_slot = 0;
        skip_first_slot = None;
        skip_first_instant = None;
        skipped_blocks = 0;
        skipped_bytes = 0;
        last_skipped_slot = 0;
        printed_skipped_summary = false;
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
        transactions: collected,
    };
    let total_transactions = dump.transactions.len() as u64;

    let encoded = wincode::serialize(&dump)?;
    fs::write(output_path, encoded).await?;

    // final summary
    txs_kept = total_transactions;
    if txs_seen < txs_kept {
        txs_seen = txs_kept;
    }
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

    let final_line = format!(
        "{range_label} | slot={} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | tx={} kept={} tracked={}",
        last_processed_slot,
        blocks_scanned,
        blk_s,
        mb_s,
        tps,
        txs_seen,
        txs_kept,
        tracked_accounts_len
    );

    if pb.is_hidden() {
        tracing::info!("{final_line}");
    } else {
        pb.finish_and_clear();
        eprintln!("{final_line}");
    }

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

#[inline]
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

#[inline]
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

#[inline]
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

#[inline]
fn metadata_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'a>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.metadata.next {
        None => Ok(tx_node.metadata.data),
        Some(df_cbor) => {
            let df_cid = df_cbor.to_cid()?;
            copy_dataframe_into(block, &df_cid, buf, None)
        }
    }
}

// Single-pass: check if transaction involves token AND extract new tracked accounts
// Returns true if transaction should be kept
// NOTE: This function is not currently used - we use the fast path with parse_account_keys_only instead
#[allow(dead_code)]
fn check_and_extract_accounts(
    tx: &VersionedTransaction,
    account_keys: &[Pubkey],
    mint_bytes: &[u8; 32],
    token_program_bytes: &[u8; 32],
    tracked: &mut AHashSet<[u8; 32]>,
    added: &mut Vec<[u8; 32]>,
) -> bool {
    added.clear();
    let mut involves_token = false;

    // Quick check: does transaction reference mint or any tracked account?
    for key in account_keys {
        let key_bytes = key.to_bytes();
        if &key_bytes == mint_bytes || tracked.contains(&key_bytes) {
            involves_token = true;
            break;
        }
    }

    // Process instructions to find new accounts and confirm token program usage
    let mut handle_instruction = |program_idx: usize, accounts: &[u8], data: &[u8]| {
        if program_idx >= account_keys.len() {
            return;
        }
        
        let program_key = account_keys[program_idx].to_bytes();
        if &program_key != token_program_bytes {
            return;
        }

        // Found token program - transaction is involved
        involves_token = true;

        // Try to extract InitializeAccount instructions
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

                    if account_idx >= account_keys.len() || mint_idx >= account_keys.len() {
                        return;
                    }

                    let candidate_key = account_keys[account_idx].to_bytes();
                    let mint_key = account_keys[mint_idx].to_bytes();
                    
                    if &mint_key != mint_bytes {
                        return;
                    }

                    if tracked.insert(candidate_key) {
                        added.push(candidate_key);
                    }
                }
                _ => {}
            }
        }
    };

    // Handle top-level instructions
    for ix in tx.message.instructions_iter() {
        handle_instruction(
            ix.program_id_index as usize,
            &ix.accounts,
            &ix.data,
        );
    }

    involves_token
}

// Write ETA directly to formatter to avoid allocation
fn write_human_eta(f: &mut impl FmtWrite, secs: f64) -> std::fmt::Result {
    let mut s = secs.max(0.0) as u64;
    let h = s / 3600;
    s %= 3600;
    let m = s / 60;
    let sec = s % 60;
    if h > 0 {
        write!(f, "eta ~ {}h{:02}m{:02}s", h, m, sec)
    } else if m > 0 {
        write!(f, "eta ~ {}m{:02}s", m, sec)
    } else {
        write!(f, "eta ~ {}s", sec)
    }
}