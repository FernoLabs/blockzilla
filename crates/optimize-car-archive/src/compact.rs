use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
use tracing::{error, info, warn};
use wincode::Deserialize;

use solana_message::VersionedMessage;
use solana_transaction::versioned::VersionedTransaction;

use car_reader::{
    car_block_group::CarBlockGroup,
    error::GroupError,
    metadata_decoder::{decode_transaction_status_meta_from_frame, ZstdReusableDecoder},
    node::{decode_node, Node},
    versioned_transaction::VersionedTransactionSchema,
};

use blockzilla_format::{
    compact_meta_from_proto, load_registry, to_compact_transaction, BlockhashRegistry,
    CompactBlockHeader, CompactBlockRecord, CompactTxWithMeta, PostcardFramedWriter, Registry,
};

use crate::{epoch_paths, hex_prefix, stream_car_blocks, Cli, ProgressTracker, BUFFER_SIZE};

pub const PREV_TAIL_LEN: usize = 150;

fn tx_kind(vtx: &VersionedTransaction) -> &'static str {
    match &vtx.message {
        VersionedMessage::Legacy(_) => "legacy",
        VersionedMessage::V0(_) => "v0",
    }
}

/// Loads a plain blockhash registry file:
/// - format: raw concatenated [u8;32] hashes
/// - id: position in file (0-based)
fn load_blockhash_registry_plain(path: &Path) -> Result<Vec<[u8; 32]>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut r = BufReader::with_capacity(BUFFER_SIZE, f);

    let mut bytes = Vec::new();
    r.read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;

    if bytes.len() % 32 != 0 {
        anyhow::bail!(
            "Invalid blockhash registry length: {} (not multiple of 32) path={}",
            bytes.len(),
            path.display()
        );
    }

    let n = bytes.len() / 32;
    let mut hashes = Vec::with_capacity(n);

    for i in 0..n {
        let off = i * 32;
        let mut h = [0u8; 32];
        h.copy_from_slice(&bytes[off..off + 32]);
        hashes.push(h);
    }

    Ok(hashes)
}

/// Load exactly the last PREV_TAIL_LEN blockhashes from a previous epoch registry file.
/// Returns fewer if the file has fewer than PREV_TAIL_LEN hashes.
fn load_prev_epoch_tail(path: &Path) -> Result<Vec<[u8; 32]>> {
    let mut f = File::open(path).with_context(|| format!("open {}", path.display()))?;

    let len = f
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();

    if len % 32 != 0 {
        anyhow::bail!(
            "Invalid blockhash registry length: {} (not multiple of 32) path={}",
            len,
            path.display()
        );
    }

    let total = (len / 32) as usize;
    if total == 0 {
        return Ok(Vec::new());
    }

    let take = total.min(PREV_TAIL_LEN);
    let offset = (total - take) as u64 * 32;

    f.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek {} to {}", path.display(), offset))?;

    let mut r = BufReader::with_capacity(BUFFER_SIZE, f);
    let mut bytes = vec![0u8; take * 32];
    r.read_exact(&mut bytes)
        .with_context(|| format!("read tail from {}", path.display()))?;

    let mut out = Vec::with_capacity(take);
    for i in 0..take {
        let mut h = [0u8; 32];
        h.copy_from_slice(&bytes[i * 32..(i + 1) * 32]);
        out.push(h);
    }
    Ok(out)
}

/// Extract tx recent blockhash bytes from a VersionedTransaction.
fn tx_recent_blockhash_bytes(vtx: &VersionedTransaction) -> [u8; 32] {
    match &vtx.message {
        VersionedMessage::Legacy(m) => m.recent_blockhash.to_bytes(),
        VersionedMessage::V0(m) => m.recent_blockhash.to_bytes(),
    }
}

pub(crate) fn run(cli: &Cli, epoch: u64) -> Result<()> {
    // epoch_paths: (car, dir, registry, blockhash_registry, compact)
    let (car_path, epoch_dir, registry_path, bh_registry_path, compact_path) =
        epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }
    if !registry_path.exists() {
        anyhow::bail!(
            "Registry not found: {}. Run registry build first.",
            registry_path.display()
        );
    }
    if !bh_registry_path.exists() {
        anyhow::bail!(
            "Blockhash registry not found: {}. Run blockhash registry build first.",
            bh_registry_path.display()
        );
    }

    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building compact blocks epoch={}", epoch);
    info!("  car:      {}", car_path.display());
    info!("  registry: {}", registry_path.display());
    info!("  bh-reg:   {}", bh_registry_path.display());
    info!("  out:      {}", compact_path.display());

    let registry = load_registry(&registry_path)?;
    info!("Registry loaded: {} keys", registry.keys.len());

    let hashes = load_blockhash_registry_plain(&bh_registry_path)?;
    info!("Blockhash registry loaded: {} hashes", hashes.len());

    // Load previous epoch tail if possible.
    // We derive the prev path from epoch_paths(epoch - 1) so it matches your folder layout.
    let prev_tail = if epoch == 0 {
        Vec::new()
    } else {
        let (_, _prev_dir, _prev_reg, prev_bh_path, _prev_compact) = epoch_paths(cli, epoch - 1);
        if prev_bh_path.exists() {
            let tail = load_prev_epoch_tail(&prev_bh_path)?;
            info!(
                "Prev epoch tail loaded: {} hashes (epoch={}) from {}",
                tail.len(),
                epoch - 1,
                prev_bh_path.display()
            );
            tail
        } else {
            warn!(
                "Prev epoch blockhash registry missing (epoch={} path={}), prev tail disabled",
                epoch - 1,
                prev_bh_path.display()
            );
            Vec::new()
        }
    };

    let bh = BlockhashRegistry::new(hashes, prev_tail);

    let tmp_path = compact_path.with_extension("bin.tmp");

    let out = File::create(&tmp_path)
        .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
    let out = BufWriter::with_capacity(BUFFER_SIZE, out);
    let mut writer = PostcardFramedWriter::new(out);

    let mut progress = ProgressTracker::new("Phase 2/2");
    let mut scratch = CompactTxDecodeScratch::new();

    // Blockhash ids are implicit for CompactBlockHeader:
    // block_i is the id, previous is block_i-1 (0 for first).
    let mut block_count: u32 = 0;

    stream_car_blocks(&car_path, |group| {
        let (blocks_delta, txs_delta, slot) =
            compact_process_block(group, &registry, &bh, &mut writer, &mut scratch, block_count)?;

        block_count = block_count.wrapping_add(1);

        progress.update(blocks_delta, txs_delta);
        if let Some(s) = slot {
            progress.update_slot(s);
        }
        Ok(())
    })?;

    writer.flush()?;
    std::fs::rename(&tmp_path, &compact_path).with_context(|| {
        format!(
            "rename {} -> {}",
            tmp_path.display(),
            compact_path.display()
        )
    })?;

    progress.final_report();
    Ok(())
}

struct CompactTxDecodeScratch {
    reusable_tx: std::mem::MaybeUninit<VersionedTransaction>,
    has_tx: bool,
    meta_out: car_reader::confirmed_block::TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
}

impl CompactTxDecodeScratch {
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
                "TX_DECODE (compact) failed: len={} prefix={}",
                bytes.len(),
                hex_prefix(bytes, 32)
            );
            error!("TX_DECODE (compact) error: {:?}", e);
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

impl Drop for CompactTxDecodeScratch {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

/// Returns:
/// - blocks_delta
/// - txs_delta
/// - slot
fn compact_process_block<W: std::io::Write>(
    group: &CarBlockGroup,
    registry: &Registry,
    bh: &BlockhashRegistry,
    writer: &mut PostcardFramedWriter<W>,
    scratch: &mut CompactTxDecodeScratch,
    block_i: u32,
) -> Result<(u64, u64, Option<u64>), GroupError> {
    let mut txs = 0u64;
    let mut tx_index_in_block: u32 = 0;

    let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
        Node::Block(b) => b,
        _ => return Err(GroupError::WrongRootKind),
    };

    let block_slot = block.slot;

    // CompactBlockHeader: keep your implicit id convention.
    let this_blockhash_id = block_i;
    let previous_blockhash_id = block_i.saturating_sub(1);

    let header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: this_blockhash_id,
        previous_blockhash: previous_blockhash_id,
        block_time: block.meta.blocktime,
        block_height: block.meta.block_height,
    };

    let mut txs_out: Vec<CompactTxWithMeta> = Vec::with_capacity(4096);

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
            tx_index_in_block += 1;

            let tx_bytes = tx.data.data;

            let vtx = scratch.decode_tx(tx_bytes).inspect_err(|_e| {
                error!(
                    "FAIL decode_tx: block_slot={} tx_slot={} tx_index_in_block={} tx_len={} tx_prefix={} cid_digest_prefix={}",
                    block_slot,
                    tx.slot,
                    tx_index_in_block,
                    tx_bytes.len(),
                    hex_prefix(tx_bytes, 32),
                    hex_prefix(tx_cid.hash_bytes(), 16),
                );
            })?;

            // Uses BlockhashRegistry inside, so it checks current epoch hashes and prev tail.
            let compact_tx = to_compact_transaction(vtx, registry, &bh.index).map_err(|conv_err| {
                error!(
                    "FAIL to_compact_transaction: block_slot={} tx_slot={} tx_index_in_block={} kind={} sigs={} tx_len={} tx_prefix={} cid_digest_prefix={}",
                    block_slot,
                    tx.slot,
                    tx_index_in_block,
                    tx_kind(vtx),
                    vtx.signatures.len(),
                    tx_bytes.len(),
                    hex_prefix(tx_bytes, 32),
                    hex_prefix(tx_cid.hash_bytes(), 16),
                );
                error!("to_compact_transaction error: {:?}", conv_err);
                GroupError::TxDecode
            })?;

            // Optional extra visibility: warn when recent blockhash is missing in both current + tail.
            let recent_bh = tx_recent_blockhash_bytes(vtx);
            if bh.lookup(&recent_bh).is_none() {
                warn!(
                    "recent_blockhash missing from blockhash registry (current + prev tail): block_slot={} tx_slot={} tx_index_in_block={}",
                    block_slot,
                    tx.slot,
                    tx_index_in_block
                );
            }

            let metadata_opt = if tx.metadata.data.is_empty() {
                None
            } else {
                let meta = scratch.decode_meta(tx.slot, tx.metadata.data).inspect_err(|_e| {
                    error!(
                        "FAIL decode_meta: block_slot={} tx_slot={} tx_index_in_block={} meta_len={} cid_digest_prefix={}",
                        block_slot,
                        tx.slot,
                        tx_index_in_block,
                        tx.metadata.data.len(),
                        hex_prefix(tx_cid.hash_bytes(), 16),
                    );
                })?;

                let compact_meta = compact_meta_from_proto(meta, registry).map_err(|e| {
                    error!(
                        "FAIL compact_meta_from_proto: block_slot={} tx_slot={} tx_index_in_block={} cid_digest_prefix={}",
                        block_slot,
                        tx.slot,
                        tx_index_in_block,
                        hex_prefix(tx_cid.hash_bytes(), 16),
                    );
                    error!("compact_meta_from_proto error: {:?}", e);
                    GroupError::TxMetaDecode
                })?;
                Some(compact_meta)
            };

            txs_out.push(CompactTxWithMeta {
                tx: compact_tx,
                metadata: metadata_opt,
            });
        }
    }

    let rec = CompactBlockRecord { header, txs: txs_out };
    writer.write(&rec).map_err(|_| GroupError::Io)?;

    Ok((1, txs, Some(block_slot)))
}
