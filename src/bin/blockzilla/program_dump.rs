use ahash::AHashSet;
use anyhow::{Context, Result, anyhow};
use cid::Cid;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use minicbor;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::io::{IsTerminal, Read};
use std::mem::MaybeUninit;
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::fs;
use wincode::Deserialize as _;
use wincode::SchemaRead;
use wincode::SchemaWrite;

use blockzilla::car_block_reader::{CarBlock, CarBlockReader};
use blockzilla::confirmed_block::TransactionStatusMeta;
use blockzilla::node::{Node, TransactionNode};
use blockzilla::open_epoch::{self, FetchMode};
use blockzilla::transaction_parser::{VersionedMessage, VersionedTransaction};

use crate::LOG_INTERVAL_SECS;

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct ProgramTransactionRecord {
    pub slot: u64,
    #[wincode(with = "wincode::containers::Vec<[u8; 32], wincode::len::short_vec::ShortU16Len>")]
    pub matched_programs: Vec<[u8; 32]>,
    #[wincode(with = "wincode::containers::Vec<u8, wincode::len::BincodeLen<{ 64 << 20 }>>")]
    pub tx_bytes: Vec<u8>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct ProgramTransactionDump {
    #[wincode(with = "wincode::containers::Vec<[u8; 32], wincode::len::short_vec::ShortU16Len>")]
    pub programs: Vec<[u8; 32]>,
    pub start_slot: u64,
    pub end_slot: u64,
    #[wincode(
        with = "wincode::containers::Vec<ProgramTransactionRecord, wincode::len::BincodeLen<{ 1 << 28 }>>"
    )]
    pub transactions: Vec<ProgramTransactionRecord>,
}

pub async fn dump_program_transactions(
    start_epoch: u64,
    start_slot: u64,
    end_slot: u64,
    cache_dir: &str,
    program_ids: &[String],
    output_path: &Path,
) -> Result<()> {
    if start_slot > end_slot {
        return Err(anyhow!("start_slot must be <= end_slot"));
    }

    if program_ids.is_empty() {
        return Err(anyhow!("at least one program id must be provided"));
    }

    let mut programs: Vec<[u8; 32]> = Vec::with_capacity(program_ids.len());
    for pid in program_ids {
        let pk = Pubkey::from_str(pid).map_err(|e| anyhow!("invalid program id '{pid}': {e}"))?;
        programs.push(pk.to_bytes());
    }

    let target_set: AHashSet<[u8; 32]> = programs.iter().copied().collect();

    let output_file_name = output_path
        .file_name()
        .ok_or_else(|| anyhow!("output path must include a file name"))?;

    let base_output_dir = match output_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => Path::new(".").to_path_buf(),
    };

    fs::create_dir_all(&base_output_dir).await?;
    let output_path = base_output_dir.join(output_file_name);

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

    let draw_target = if std::io::stdout().is_terminal() {
        ProgressDrawTarget::stderr_with_hz(8)
    } else {
        ProgressDrawTarget::hidden()
    };
    let pb = ProgressBar::with_draw_target(Some(0), draw_target);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    pb.enable_steady_tick(Duration::from_millis(120));

    let mut buf_tx = Vec::with_capacity(128 * 1024);
    let mut buf_meta = Vec::with_capacity(64 * 1024);
    let mut reusable_tx: MaybeUninit<VersionedTransaction> = MaybeUninit::uninit();
    let mut collected: Vec<ProgramTransactionRecord> = Vec::new();

    let mut last_slot = 0u64;
    let start_time = Instant::now();
    let mut last_log = start_time;
    let mut finished = false;

    for epoch in start_epoch..=last_epoch {
        if finished {
            break;
        }

        let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
            .await
            .with_context(|| format!("failed to open cached epoch {epoch:04}"))?;
        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;

        while let Some(block) = car.next_block().await? {
            let slot = peek_block_slot(&block)?;
            if slot < start_slot {
                continue;
            }
            if slot > end_slot {
                finished = true;
                break;
            }

            let block_node = block.block()?;
            for entry_cid_res in block_node.entries.iter() {
                let entry_cid = entry_cid_res?;
                let Node::Entry(entry) = block.decode(entry_cid.hash_bytes())? else {
                    continue;
                };

                for tx_cid_res in entry.transactions.iter() {
                    let tx_cid = tx_cid_res?;
                    let tx_node = match block.decode(tx_cid.hash_bytes()) {
                        Ok(Node::Transaction(tx)) => tx,
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::warn!("failed to decode transaction node: {e}");
                            continue;
                        }
                    };

                    let tx_bytes_slice = match transaction_bytes(&block, &tx_node, &mut buf_tx) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::warn!("failed to read transaction bytes: {e}");
                            continue;
                        }
                    };

                    let meta_bytes = metadata_bytes(&block, &tx_node, &mut buf_meta).ok();
                    let meta = meta_bytes.and_then(decode_transaction_meta);

                    if let Err(e) =
                        VersionedTransaction::deserialize_into(tx_bytes_slice, &mut reusable_tx)
                    {
                        tracing::warn!("failed to parse transaction: {e}");
                        continue;
                    }
                    let tx_ref = unsafe { reusable_tx.assume_init_ref() };

                    let matched = find_matched_programs(tx_ref, meta.as_ref(), &target_set);
                    unsafe { std::ptr::drop_in_place(reusable_tx.as_mut_ptr()) };

                    if matched.is_empty() {
                        continue;
                    }

                    collected.push(ProgramTransactionRecord {
                        slot,
                        matched_programs: matched,
                        tx_bytes: tx_bytes_slice.to_vec(),
                    });
                }
            }

            last_slot = slot;

            let now = Instant::now();
            if now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS) {
                let elapsed = now.duration_since(start_time).as_secs_f64();
                let tps = if elapsed > 0.0 {
                    (collected.len() as f64) / elapsed
                } else {
                    0.0
                };
                let msg = format!(
                    "epoch {:04} | slot {} | kept {} tx | {:.1} tx/s",
                    epoch,
                    last_slot,
                    collected.len(),
                    tps
                );
                if pb.is_hidden() {
                    tracing::info!("{}", msg);
                } else {
                    pb.set_message(msg);
                    pb.tick();
                }
                last_log = now;
            }
        }
    }

    if !finished {
        tracing::warn!(
            "ended scan at epoch {:04} slot {} before reaching end_slot {}",
            last_epoch,
            last_slot,
            end_slot
        );
    }

    let dump = ProgramTransactionDump {
        programs: programs.clone(),
        start_slot,
        end_slot,
        transactions: collected,
    };

    let data = wincode::serialize(&dump)?;
    fs::write(&output_path, data).await?;

    pb.finish_and_clear();
    tracing::info!(
        "📝 wrote {} transactions to {}",
        dump.transactions.len(),
        output_path.display()
    );

    Ok(())
}

fn find_matched_programs(
    tx: &VersionedTransaction,
    meta: Option<&TransactionStatusMeta>,
    targets: &AHashSet<[u8; 32]>,
) -> Vec<[u8; 32]> {
    let mut resolved = SmallVec::<[[u8; 32]; 256]>::new();
    for k in tx.message.static_account_keys() {
        resolved.push(*k);
    }

    if let (VersionedMessage::V0(_), Some(meta)) = (&tx.message, meta) {
        for addr in &meta.loaded_writable_addresses {
            if addr.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(addr);
                resolved.push(arr);
            }
        }
        for addr in &meta.loaded_readonly_addresses {
            if addr.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(addr);
                resolved.push(arr);
            }
        }
    }

    let mut matched: AHashSet<[u8; 32]> = AHashSet::new();

    for ix in tx.message.instructions_iter() {
        if let Some(pk) = resolved.get(ix.program_id_index as usize) {
            if targets.contains(pk) {
                matched.insert(*pk);
            }
        }
    }

    if let Some(meta) = meta {
        for inner in &meta.inner_instructions {
            for ix in &inner.instructions {
                if let Some(pk) = resolved.get(ix.program_id_index as usize) {
                    if targets.contains(pk) {
                        matched.insert(*pk);
                    }
                }
            }
        }
    }

    matched.into_iter().collect()
}

fn decode_transaction_meta(bytes: &[u8]) -> Option<TransactionStatusMeta> {
    match blockzilla::meta_decode::decode_transaction_status_meta_bytes(bytes) {
        Ok(meta) => Some(meta),
        Err(e) => {
            tracing::warn!("failed to decode transaction metadata: {e}");
            None
        }
    }
}

async fn find_last_cached_epoch(cache_dir: &Path) -> Result<Option<u64>> {
    let mut rd = match fs::read_dir(cache_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
