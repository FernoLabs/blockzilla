use ahash::AHashSet;
use anyhow::{Context, Result, anyhow};
use cid::Cid;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use minicbor;
use prost::Message;
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

use blockzilla::{
    car_block_reader::{CarBlock, CarBlockReader},
    confirmed_block::TransactionStatusMeta,
    node::{Node, TransactionNode},
    open_epoch::{self, FetchMode},
    partial_meta::extract_metadata_pubkeys,
    transaction_parser::{VersionedTransaction, parse_account_keys_only},
};

use crate::LOG_INTERVAL_SECS;

// ======================= Token instruction stats (mint-only + tracked-only) =======================

#[derive(Default, Clone)]
struct TokenInstrStats {
    // Only for our mint (instruction accounts explicitly include the mint)
    mint_init_acct: u64,        // InitializeAccount, InitializeAccount2, InitializeAccount3
    mint_init_mint: u64,        // InitializeMint, InitializeMint2
    mint_transfer_checked: u64, // TransferChecked
    mint_mint_to: u64,          // MintTo
    mint_burn: u64,             // Burn
    mint_freeze: u64,           // FreezeAccount
    mint_thaw: u64,             // ThawAccount

    // For tracked accounts but the mint is NOT present in account metas
    tracked_only_transfer: u64,       // Transfer (no mint in accounts)
    tracked_only_approve: u64,
    tracked_only_revoke: u64,
    tracked_only_close: u64,
    tracked_only_set_authority: u64,
    tracked_only_sync_native: u64,
}

impl TokenInstrStats {
    fn reset(&mut self) { *self = Self::default(); }
    fn any(&self) -> bool {
        self.mint_init_acct
            + self.mint_init_mint
            + self.mint_transfer_checked
            + self.mint_mint_to
            + self.mint_burn
            + self.mint_freeze
            + self.mint_thaw
            + self.tracked_only_transfer
            + self.tracked_only_approve
            + self.tracked_only_revoke
            + self.tracked_only_close
            + self.tracked_only_set_authority
            + self.tracked_only_sync_native > 0
    }
}

// ======================= Helpers: token ix counting (mint or tracked-only) =======================

fn count_token_ix_for_mint_or_tracked(
    instr: &TokenInstruction,
    accounts: &[u8],
    resolved_keys: &[Pubkey],
    mint_bytes: &[u8; 32],
    tracked: &AHashSet<[u8; 32]>,
    acc: &mut TokenInstrStats,
) {
    use TokenInstruction as TI;

    let acc_at = |i: usize| -> Option<Pubkey> {
        let idx = *accounts.get(i)? as usize;
        resolved_keys.get(idx).copied()
    };
    let acc_matches_mint = |i: usize| -> bool {
        acc_at(i).map(|p| p.to_bytes() == *mint_bytes).unwrap_or(false)
    };
    let any_tracked_in_accounts = || -> bool {
        accounts.iter().copied().any(|b| {
            let idx = b as usize;
            if idx >= resolved_keys.len() { return false; }
            tracked.contains(&resolved_keys[idx].to_bytes())
        })
    };
    let mint_present_in_accounts = || -> bool {
        accounts.iter().copied().any(|b| {
            let idx = b as usize;
            idx < resolved_keys.len() && resolved_keys[idx].to_bytes() == *mint_bytes
        })
    };

    match instr {
        // [account, mint, ...]
        TI::InitializeAccount | TI::InitializeAccount2 { .. } | TI::InitializeAccount3 { .. } => {
            if acc_matches_mint(1) { acc.mint_init_acct += 1; }
        }
        // [mint, ...]
        TI::InitializeMint { .. } | TI::InitializeMint2 { .. } => {
            if acc_matches_mint(0) { acc.mint_init_mint += 1; }
        }
        // [src, mint, dst, owner, ...]
        TI::TransferChecked { .. } => {
            if acc_matches_mint(1) { acc.mint_transfer_checked += 1; }
        }
        // [mint, account, mint_authority, ...]
        TI::MintTo { .. } => {
            if acc_matches_mint(0) { acc.mint_mint_to += 1; }
        }
        // [account, mint, owner, ...]
        TI::Burn { .. } => {
            if acc_matches_mint(1) { acc.mint_burn += 1; }
        }
        // [account, mint, authority, ...]
        TI::FreezeAccount => {
            if acc_matches_mint(1) { acc.mint_freeze += 1; }
        }
        // [account, mint, authority, ...]
        TI::ThawAccount => {
            if acc_matches_mint(1) { acc.mint_thaw += 1; }
        }

        // Variants without explicit mint in accounts — count as tracked_only if any tracked account is referenced
        TI::Transfer { .. } => {
            if !mint_present_in_accounts() && any_tracked_in_accounts() {
                acc.tracked_only_transfer += 1;
            }
        }
        TI::Approve { .. } => {
            if any_tracked_in_accounts() {
                acc.tracked_only_approve += 1;
            }
        }
        TI::Revoke { .. } => {
            if any_tracked_in_accounts() {
                acc.tracked_only_revoke += 1;
            }
        }
        TI::CloseAccount => {
            if any_tracked_in_accounts() {
                acc.tracked_only_close += 1;
            }
        }
        TI::SetAuthority { .. } => {
            if any_tracked_in_accounts() {
                acc.tracked_only_set_authority += 1;
            }
        }
        TI::SyncNative => {
            if any_tracked_in_accounts() {
                acc.tracked_only_sync_native += 1;
            }
        }

        _ => {}
    }
}

// ======================= Extract new tracked accounts + count instructions =======================

fn extend_with_loaded_addresses(dest: &mut SmallVec<[Pubkey; 512]>, addrs: &[Vec<u8>]) {
    for addr in addrs {
        if addr.len() == 32 {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(addr);
            dest.push(Pubkey::new_from_array(bytes));
        } else {
            tracing::warn!("loaded address has invalid length: expected 32 bytes, got {}", addr.len());
        }
    }
}

fn decode_transaction_meta(bytes: &[u8]) -> Option<TransactionStatusMeta> {
    if bytes.is_empty() { return None; }
    let raw = if is_zstd(bytes) {
        match zstd::decode_all(bytes) {
            Ok(data) => data,
            Err(e) => { tracing::warn!("failed to decompress metadata: {e}"); return None; }
        }
    } else {
        bytes.to_vec()
    };
    match TransactionStatusMeta::decode(raw.as_slice()) {
        Ok(meta) => Some(meta),
        Err(e) => { tracing::warn!("failed to decode TransactionStatusMeta protobuf: {e}"); None }
    }
}

#[inline]
fn is_zstd(buf: &[u8]) -> bool {
    buf.get(0..4).map(|m| m == [0x28, 0xB5, 0x2F, 0xFD]).unwrap_or(false)
}

/// Only SPL Token classic (no Token-2022), no ATA heuristic.
/// - Count token instructions either for our mint (when mint is present in accounts)
///   or for tracked accounts (but not mint).
/// - Track new token accounts for InitializeAccount* when mint matches.
fn extract_new_tracked_accounts_and_count(
    tx: &VersionedTransaction,
    metadata_bytes: Option<&[u8]>,
    mint_bytes: &[u8; 32],
    token_program_bytes: &[u8; 32], // spl_token::ID
    tracked: &mut AHashSet<[u8; 32]>,
    added: &mut Vec<[u8; 32]>,
    tok_ix: &mut TokenInstrStats,
) {
    added.clear();

    let static_keys = tx.message.static_account_keys();
    let mut resolved_keys = SmallVec::<[Pubkey; 512]>::with_capacity(static_keys.len() + 16);
    for key in static_keys {
        resolved_keys.push(Pubkey::new_from_array(*key));
    }

    let mut inner_instructions = None;

    if let Some(bytes) = metadata_bytes {
        if let Some(meta) = decode_transaction_meta(bytes) {
            extend_with_loaded_addresses(&mut resolved_keys, &meta.loaded_writable_addresses);
            extend_with_loaded_addresses(&mut resolved_keys, &meta.loaded_readonly_addresses);
            if !meta.inner_instructions_none && !meta.inner_instructions.is_empty() {
                inner_instructions = Some(meta.inner_instructions);
            }
        }
    }

    let mut handle_ix = |program_idx: usize, accounts: &[u8], data: &[u8]| {
        if program_idx >= resolved_keys.len() { return; }
        let program_key = resolved_keys[program_idx].to_bytes();
        if &program_key != token_program_bytes { return; }

        if let Ok(instr) = TokenInstruction::unpack(data) {
            // Count by mint or tracked-only
            count_token_ix_for_mint_or_tracked(
                &instr, accounts, &resolved_keys, mint_bytes, tracked, tok_ix,
            );

            // Track new accounts for InitializeAccount* when the mint matches
            match instr {
                TokenInstruction::InitializeAccount
                | TokenInstruction::InitializeAccount2 { .. }
                | TokenInstruction::InitializeAccount3 { .. } => {
                    if accounts.len() < 2 { return; }
                    let account_idx = accounts[0] as usize;
                    let mint_idx = accounts[1] as usize;
                    if account_idx >= resolved_keys.len() || mint_idx >= resolved_keys.len() { return; }

                    let candidate_key = resolved_keys[account_idx].to_bytes();
                    let mint_key = resolved_keys[mint_idx].to_bytes();
                    if &mint_key != mint_bytes { return; }

                    if tracked.insert(candidate_key) {
                        added.push(candidate_key);
                    }
                }
                _ => {}
            }
        }
    };

    // Top-level
    for ix in tx.message.instructions_iter() {
        handle_ix(ix.program_id_index as usize, &ix.accounts, &ix.data);
    }
    // Inner
    if let Some(groups) = inner_instructions {
        for group in groups {
            for ix in group.instructions {
                handle_ix(ix.program_id_index as usize, &ix.accounts, &ix.data);
            }
        }
    }
}

// ======================= Token dump main =======================

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
        return Err(anyhow!("no cached epochs found in {}", cache_path.display()));
    };

    if start_epoch > last_epoch {
        return Err(anyhow!("start epoch {start_epoch:04} is beyond last cached epoch {last_epoch:04}"));
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
    let mut blocks_scanned = 0u64; // processed blocks (excludes skipped)
    let mut txs_kept = 0u64;       // cumulative kept
    let mut accounts_tracked_new_win = 0u64; // per-interval new tracked
    let mut bytes_count = 0u64; // processed bytes for final summary
    let mut _entry_count = 0u64;
    let mut last_processed_slot = 0u64;
    let mut last_epoch_processed = None;

    // Reusable buffers
    let mut added_keys_buf = Vec::with_capacity(64);
    let mut msg_buf = String::with_capacity(256);
    let mut account_keys_buf = SmallVec::<[Pubkey; 256]>::new();

    // Token program (classic only)
    let token_program_bytes = spl_token::ID.to_bytes();

    // Load cached epoch dumps (UNION tracked accounts; accumulate kept)
    if let Ok(mut rd) = fs::read_dir(&parts_dir).await {
        let mut part_entries: Vec<(u64, PathBuf)> = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let Ok(file_type) = entry.file_type().await else { continue; };
            if !file_type.is_file() { continue; }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue; };
            let Some(epoch_str) = name.strip_prefix("epoch-").and_then(|s| s.strip_suffix(".bin")) else { continue; };
            let Ok(epoch) = epoch_str.parse::<u64>() else { continue; };
            part_entries.push((epoch, entry.path()));
        }
        part_entries.sort_unstable_by_key(|(epoch, _)| *epoch);

        for (epoch, path) in part_entries {
            if epoch < start_epoch || epoch > last_epoch { continue; }

            let data = match fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(e) => { tracing::warn!("failed to read cached epoch dump {}: {}", path.display(), e); continue; }
            };

            let part: TokenTransactionEpochDump = match wincode::deserialize(&data) {
                Ok(part) => part,
                Err(e) => { tracing::warn!("failed to decode cached epoch dump {}: {}", path.display(), e); continue; }
            };

            if part.mint != mint_bytes {
                return Err(anyhow!("cached epoch dump mint mismatch for {}", path.display()));
            }

            // UNION tracked accounts from cached part
            for k in part.tracked_account_keys {
                tracked_account_keys.insert(k);
            }

            last_processed_slot = part.last_slot;
            let new_txs = part.transactions.len() as u64;
            txs_kept += new_txs;
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
    let mut last_log_time = start;

    // skip-phase control
    let mut min_slot = if processed_epochs.contains(&start_epoch) { 0 } else { start_slot };

    // signed slot display support
    let mut first_target_slot_opt: Option<u64> = if processed_epochs.contains(&start_epoch) { None } else { Some(start_slot) };
    let mut last_seen_slot = 0u64;

    msg_buf.clear();
    write!(
        &mut msg_buf,
        "starting dump | mint={} | start_epoch={:04} start_slot={} | scanning up to {:04}",
        mint_str, start_epoch, start_slot, last_epoch
    ).unwrap();
    pb.set_message(msg_buf.clone());

    for epoch in start_epoch..=last_epoch {
        if processed_epochs.contains(&epoch) {
            if pb.is_hidden() { tracing::info!("skipping epoch {epoch:04} (already cached)"); }
            else { pb.println(format!("skipping epoch {epoch:04} (already cached)")); }
            last_epoch_processed = Some(epoch);
            continue;
        }

        // ========== reset interval counters when changing epoch ==========
        let mut int_blocks: u64 = 0;           // includes skipped
        let mut int_bytes: u64 = 0;            // includes skipped
        let mut int_txs_seen: u64 = 0;
        let mut int_txs_kept: u64 = 0;         // only per-interval
        let mut int_blocks_with_kept: u64 = 0;
        let mut tok_ix = TokenInstrStats::default();
        accounts_tracked_new_win = 0;          // reset per-interval new tracked
        last_log_time = Instant::now();

        let epoch_start_idx = collected.len();

        if pb.is_hidden() { tracing::info!("opening epoch {epoch:04}"); }
        else { pb.println(format!("opening epoch {epoch:04}")); }

        let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
            .await
            .with_context(|| format!("failed to open epoch {epoch:04} from {cache_dir}"))?;
        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;
        last_epoch_processed = Some(epoch);

        while let Some(block) = car.next_block().await? {
            _entry_count += block.entries.len() as u64;
            let block_bytes = block.entries.iter().map(|e| e.len()).sum::<usize>() as u64;

            // interval counts include skipped
            int_blocks += 1;
            int_bytes += block_bytes;

            let slot = match peek_block_slot(&block) {
                Ok(slot) => slot,
                Err(e) => { tracing::warn!("failed to decode block: {e}"); continue; }
            };
            last_seen_slot = slot;

            let mut kept_in_this_block = 0u64;
            let mut seen_in_this_block = 0u64;

            // skip phase until start_slot in the first epoch
            let is_skipped = min_slot > 0 && slot < min_slot;
            if !is_skipped {
                // processed block
                blocks_scanned += 1;
                bytes_count += block_bytes;

                let block_node = match block.block() {
                    Ok(node) => node,
                    Err(e) => { tracing::warn!("failed to decode block: {e}"); continue; }
                };

                let mut reusable_tx = MaybeUninit::<VersionedTransaction>::uninit();

                for entry_cid_res in block_node.entries.iter() {
                    let entry_cid = match entry_cid_res {
                        Ok(cid) => cid,
                        Err(e) => { tracing::warn!("failed to decode entry cid: {e}"); continue; }
                    };

                    let entry = match block.decode(entry_cid.hash_bytes()) {
                        Ok(Node::Entry(entry)) => entry,
                        Ok(_) => continue,
                        Err(e) => { tracing::warn!("failed to decode entry: {e}"); continue; }
                    };

                    for tx_cid_res in entry.transactions.iter() {
                        let tx_cid = match tx_cid_res {
                            Ok(cid) => cid,
                            Err(e) => { tracing::warn!("failed to decode transaction cid: {e}"); continue; }
                        };

                        let tx_node = match block.decode(tx_cid.hash_bytes()) {
                            Ok(Node::Transaction(tx_node)) => tx_node,
                            Ok(_) => continue,
                            Err(e) => { tracing::warn!("failed to decode transaction node: {e}"); continue; }
                        };

                        // Read transaction bytes into buf_tx
                        let tx_bytes_slice = match transaction_bytes(&block, &tx_node, &mut buf_tx) {
                            Ok(bytes) => bytes,
                            Err(e) => { tracing::warn!("failed to read transaction bytes: {e}"); continue; }
                        };

                        seen_in_this_block += 1;

                        // Fast path: parse account keys only (cheap)
                        account_keys_buf.clear();
                        if parse_account_keys_only(tx_bytes_slice, &mut account_keys_buf).ok().flatten().is_none() {
                            continue;
                        }

                        // For V0 transactions, also extract loaded addresses from metadata
                        if let Some(_lookups) = tx_bytes_slice.get(0).filter(|&&b| b & 0x80 != 0) {
                            let meta_bytes = match metadata_bytes(&block, &tx_node, &mut buf_meta) {
                                Ok(bytes) => bytes,
                                Err(_) => &[][..],
                            };
                            if !meta_bytes.is_empty() {
                                let _ = extract_metadata_pubkeys(meta_bytes, &mut account_keys_buf);
                            }
                        }

                        // quick involvement: mint or tracked
                        let mut involves_token = false;
                        for key in &account_keys_buf {
                            let key_bytes = key.to_bytes();
                            if &key_bytes == &mint_bytes || tracked_account_keys.contains(&key_bytes) {
                                involves_token = true;
                                break;
                            }
                        }
                        if !involves_token { continue; }

                        // Full deserialize only for involved tx
                        if let Err(e) = VersionedTransaction::deserialize_into(tx_bytes_slice, &mut reusable_tx) {
                            tracing::warn!("failed to parse transaction: {e}");
                            continue;
                        }
                        let tx_ref = unsafe { reusable_tx.assume_init_ref() };

                        let meta_bytes_opt = metadata_bytes(&block, &tx_node, &mut buf_meta).ok();

                        // Extract new tracked accounts and count token instructions
                        extract_new_tracked_accounts_and_count(
                            tx_ref,
                            meta_bytes_opt,
                            &mint_bytes,
                            &token_program_bytes,
                            &mut tracked_account_keys,
                            &mut added_keys_buf,
                            &mut tok_ix,
                        );

                        unsafe { std::ptr::drop_in_place(reusable_tx.as_mut_ptr()) };

                        if !added_keys_buf.is_empty() {
                            accounts_tracked_new_win += added_keys_buf.len() as u64;
                            added_keys_buf.clear();
                        }

                        // Keep the tx bytes
                        collected.push(TokenTransactionRecord { slot, tx_bytes: tx_bytes_slice.to_vec() });
                        kept_in_this_block += 1;
                        txs_kept += 1; // cumulative kept
                    }
                }

                // Aggregate interval counts
                int_txs_seen += seen_in_this_block;
                int_txs_kept += kept_in_this_block;
                if kept_in_this_block > 0 { int_blocks_with_kept += 1; }

                last_processed_slot = slot;
            }

            // periodic metrics by time or when enough blocks in the interval
            let now = Instant::now();
            let dt = now.duration_since(last_log_time).as_secs_f64();
            let time_due = dt >= LOG_INTERVAL_SECS as f64;
            let chunk_due = int_blocks >= BLOCK_LOG_CHUNK;

            if time_due || chunk_due {
                let blk_s = (int_blocks as f64) / dt.max(1e-6);
                let mb_s = ((int_bytes as f64) / (1024.0 * 1024.0)) / dt.max(1e-6);
                let tps   = (int_txs_seen as f64) / dt.max(1e-6);

                let tracked_accounts_len = tracked_account_keys.len();

                // Signed slot delta:
                // - While skipping (min_slot > 0): show (current_slot - target_slot) => negative
                // - After crossing target: show (last_processed_slot - target_slot)  => non-negative
                // - For other epochs (no target): show absolute last_processed_slot
                let slot_disp: i64 = if let Some(target) = first_target_slot_opt {
                    if min_slot > 0 {
                        (last_seen_slot as i64) - (target as i64)
                    } else {
                        (last_processed_slot.saturating_sub(target)) as i64
                    }
                } else {
                    last_processed_slot as i64
                };

                msg_buf.clear();
                write!(
                    &mut msg_buf,
                    "epoch {:04} | slot_delta={} | {:>6.1} blk/s | {:>5.2} MB/s | {:>6.1} TPS | kept={} | blocks_with_kept={} | tracked={} (+{} new)",
                    epoch,
                    slot_disp,
                    blk_s,
                    mb_s,
                    tps,
                    txs_kept,                // cumulative kept
                    int_blocks_with_kept,    // per-interval
                    tracked_accounts_len,    // cumulative tracked size
                    accounts_tracked_new_win // per-interval new tracked
                ).unwrap();

                if pb.is_hidden() { tracing::info!("{}", msg_buf); }
                else { pb.set_message(msg_buf.clone()); pb.tick(); }

                // second line: mint-only and tracked-only instruction breakdown
                if tok_ix.any() {
                    let mut tok_line = String::with_capacity(200);
                    write!(
                        &mut tok_line,
                        "  tok[mint] init_acct:{} init_mint:{} xfer_chk:{} mint_to:{} burn:{} freeze:{} thaw:{} | tok[tracked_only] xfer:{} approve:{} revoke:{} close:{} set_auth:{} sync:{}",
                        tok_ix.mint_init_acct,
                        tok_ix.mint_init_mint,
                        tok_ix.mint_transfer_checked,
                        tok_ix.mint_mint_to,
                        tok_ix.mint_burn,
                        tok_ix.mint_freeze,
                        tok_ix.mint_thaw,
                        tok_ix.tracked_only_transfer,
                        tok_ix.tracked_only_approve,
                        tok_ix.tracked_only_revoke,
                        tok_ix.tracked_only_close,
                        tok_ix.tracked_only_set_authority,
                        tok_ix.tracked_only_sync_native
                    ).unwrap();

                    if pb.is_hidden() { tracing::info!("{}", tok_line); }
                    else { pb.println(tok_line); }
                }

                // reset interval
                int_blocks = 0;
                int_bytes = 0;
                int_txs_seen = 0;
                int_txs_kept = 0;
                int_blocks_with_kept = 0;
                accounts_tracked_new_win = 0;
                tok_ix.reset();
                last_log_time = now;
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
        // after first epoch done, stop using signed delta — show absolute slots
        first_target_slot_opt = None;
    }

    let Some(last_epoch_processed) = last_epoch_processed else {
        return Err(anyhow!("no epochs processed between {start_epoch:04} and {last_epoch:04}"));
    };

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let dump = TokenTransactionDump { mint: mint_bytes, transactions: collected };
    let total_transactions = dump.transactions.len() as u64;

    let encoded = wincode::serialize(&dump)?;
    fs::write(output_path, encoded).await?;

    // final summary (lean)
    let range_label = if start_epoch == last_epoch_processed {
        format!("epoch {start_epoch:04}")
    } else {
        format!("epochs {start_epoch:04}-{last_epoch_processed:04}")
    };

    let final_line = format!(
        "{range_label} | slot={} | kept_tx={} | tracked_accounts={}",
        last_processed_slot,
        total_transactions,
        tracked_account_keys.len(),
    );

    if pb.is_hidden() { tracing::info!("{final_line}"); }
    else { pb.finish_and_clear(); eprintln!("{final_line}"); }

    Ok(())
}

// ======================= misc utils =======================

async fn find_last_cached_epoch(cache_dir: &Path) -> Result<Option<u64>> {
    let mut rd = match fs::read_dir(cache_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut max_epoch = None;
    while let Some(entry) = rd.next_entry().await? {
        let Ok(file_type) = entry.file_type().await else { continue; };
        if !file_type.is_file() { continue; }

        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue; };

        let Some(epoch_str) = name.strip_prefix("epoch-").and_then(|s| {
            s.strip_suffix(".car").or_else(|| s.strip_suffix(".car.zst"))
        }) else { continue; };

        let Ok(epoch) = epoch_str.parse::<u64>() else { continue; };

        max_epoch = Some(max_epoch.map_or(epoch, |current: u64| current.max(epoch)));
    }

    Ok(max_epoch)
}

#[inline]
fn peek_block_slot(block: &CarBlock) -> Result<u64> {
    let mut decoder = minicbor::Decoder::new(block.block_bytes.as_ref());
    decoder.array().map_err(|e| anyhow!("failed to decode block header array: {e}"))?;
    let kind = decoder.u64().map_err(|e| anyhow!("failed to decode block kind: {e}"))?;
    if kind != 2 { return Err(anyhow!("unexpected block kind {kind}, expected Block")); }
    let slot = decoder.u64().map_err(|e| anyhow!("failed to decode block slot: {e}"))?;
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
    if let Some(prefix) = inline_prefix { dst.extend_from_slice(prefix); }
    let mut rdr = block.dataframe_reader(cid);
    rdr.read_to_end(dst).map_err(|e| anyhow!("read dataframe chain: {e}"))?;
    Ok(&*dst)
}

#[inline]
fn transaction_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'_>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.data.next {
        None => { buf.clear(); buf.extend_from_slice(tx_node.data.data); Ok(&*buf) }
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
