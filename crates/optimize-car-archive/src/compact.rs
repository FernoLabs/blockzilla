use anyhow::{Context, Result};
use car_reader::car_stream::CarStream;
use car_reader::versioned_transaction::{VersionedMessage, VersionedTransaction};
use rustc_hash::FxHashMap;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom},
    path::Path,
};
use tracing::{error, info, warn};

use car_reader::{
    car_block_group::CarBlockGroup,
    error::GroupError,
    node::{Node, decode_node},
};

use blockzilla_format::{
    BlockhashRegistry, CompactAddressTableLookup, CompactBlockHeader, CompactInstruction,
    CompactLegacyMessage, CompactMessage, CompactMessageHeader, CompactRecentBlockhash,
    CompactTransaction, CompactTxWithMeta, CompactV0Message, KeyIndex, KeyStore,
    PostcardFramedWriter, Signature, compact_meta_from_proto,
};

use crate::{BUFFER_SIZE, Cli, ProgressTracker, epoch_paths};

pub const PREV_TAIL_LEN: usize = 200;

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

    let store = KeyStore::load(&registry_path)?;
    let len = store.len();
    let index = KeyIndex::build(store.keys);
    info!("Registry loaded: {} keys", len);

    let hashes = load_blockhash_registry_plain(&bh_registry_path)?;
    info!("Blockhash registry loaded: {} hashes", hashes.len());

    // Load previous epoch tail if possible.
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

    let mut block_count: u32 = 0;

    // Reusable per-block buffers
    let mut tx_payload: Vec<u8> = Vec::with_capacity(8 << 20);
    let mut block_payload: Vec<u8> = Vec::with_capacity(8 << 20);
    let mut varint_buf: [u8; varint_max::<usize>()] = [0u8; varint_max::<usize>()];

    let mut stream = CarStream::open_zstd(Path::new(&car_path))?;
    while let Some(group) = stream.next_group()? {
        let (blocks_delta, txs_delta, slot) = compact_process_block_manual(
            group,
            &index,
            &bh.index,
            &mut writer,
            block_count,
            &mut tx_payload,
            &mut block_payload,
            &mut varint_buf,
        )?;
        block_count = block_count.wrapping_add(1);
        progress.update(blocks_delta, txs_delta);
        if let Some(s) = slot {
            progress.update_slot(s);
        }
    }

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

fn compact_process_block_manual<W: std::io::Write>(
    group: &CarBlockGroup,
    index: &KeyIndex,
    bh_index: &FxHashMap<[u8; 32], i32>,
    writer: &mut PostcardFramedWriter<W>,
    block_i: u32,
    tx_payload: &mut Vec<u8>,
    block_payload: &mut Vec<u8>,
    varint_tmp: &mut [u8; varint_max::<usize>()],
) -> Result<(u64, u64, Option<u64>), GroupError> {
    let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
        Node::Block(b) => b,
        _ => return Err(GroupError::WrongRootKind),
    };
    let block_slot = block.slot;

    let header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: block_i,
        previous_blockhash: block_i.saturating_sub(1),
        block_time: block.meta.blocktime,
        block_height: block.meta.block_height,
    };

    tx_payload.clear();

    let mut txs: u64 = 0;
    let mut tx_index_in_block: u32 = 0;

    let mut it = group.transactions()?;

    while let Some((vtx, maybe_meta)) = it.next_tx()? {
        txs += 1;
        tx_index_in_block += 1;

        let compact_tx = to_compact_transaction(vtx, index, bh_index).map_err(|e| {
            error!(
                "FAIL to_compact_transaction: block_slot={} tx_index_in_block={} kind={} sigs={}",
                block_slot,
                tx_index_in_block,
                tx_kind(vtx),
                vtx.signatures.len(),
            );
            error!("to_compact_transaction error: {:?}", e);
            GroupError::TxDecode
        })?;

        let metadata_opt = if let Some(meta) = maybe_meta {
            let compact_meta = compact_meta_from_proto(meta, index).map_err(|e| {
                error!(
                    "FAIL compact_meta_from_proto: block_slot={} tx_index_in_block={}",
                    block_slot, tx_index_in_block
                );
                error!("compact_meta_from_proto error: {:?}", e);
                GroupError::TxMetaDecode
            })?;
            Some(compact_meta)
        } else {
            None
        };

        let elem = CompactTxWithMeta {
            tx: compact_tx,
            metadata: metadata_opt,
        };

        postcard::to_io(&elem, &mut *tx_payload).map_err(|_| GroupError::Io)?;
    }

    block_payload.clear();
    postcard::to_io(&header, &mut *block_payload).map_err(|_| GroupError::Io)?;

    let tx_count = txs as usize;
    let len_bytes = varint_usize(tx_count, varint_tmp);
    block_payload.extend_from_slice(len_bytes);
    block_payload.extend_from_slice(&*tx_payload);

    writer
        .write_bytes(block_payload)
        .map_err(|_| GroupError::Io)?;

    Ok((1, txs, Some(block_slot)))
}

/// Returns the maximum number of bytes required to encode T.
pub const fn varint_max<T: Sized>() -> usize {
    const BITS_PER_BYTE: usize = 8;
    const BITS_PER_VARINT_BYTE: usize = 7;

    let bits = core::mem::size_of::<T>() * BITS_PER_BYTE;
    let roundup_bits = bits + (BITS_PER_VARINT_BYTE - 1);
    roundup_bits / BITS_PER_VARINT_BYTE
}

#[inline]
pub fn varint_usize(n: usize, out: &mut [u8; varint_max::<usize>()]) -> &mut [u8] {
    let mut value = n;
    for i in 0..varint_max::<usize>() {
        out[i] = value.to_le_bytes()[0];
        if value < 128 {
            return &mut out[..=i];
        }

        out[i] |= 0x80;
        value >>= 7;
    }
    debug_assert_eq!(value, 0);
    &mut out[..]
}

pub fn to_compact_transaction<'a>(
    vtx: &'a car_reader::versioned_transaction::VersionedTransaction,
    index: &KeyIndex,
    bh_index: &FxHashMap<[u8; 32], i32>,
) -> Result<CompactTransaction<'a>> {
    let mut signatures = Vec::with_capacity(vtx.signatures.len());
    for s in &vtx.signatures {
        signatures.push(Signature(s));
    }

    let message = match &vtx.message {
        VersionedMessage::Legacy(m) => {
            let header = CompactMessageHeader {
                num_required_signatures: m.header.num_required_signatures,
                num_readonly_signed_accounts: m.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: m.header.num_readonly_unsigned_accounts,
            };

            let mut account_keys = Vec::with_capacity(m.account_keys.len());
            for key in &m.account_keys {
                let idx = index.lookup_unchecked(*key);
                account_keys.push(idx);
            }

            let recent_blockhash: [u8; 32] = m
                .recent_blockhash
                .as_ref()
                .try_into()
                .map_err(|_| anyhow::anyhow!("blockhash len != 32"))?;

            let recent_blockhash = bh_index
                .get(&recent_blockhash)
                .copied()
                .map(CompactRecentBlockhash::Id)
                .unwrap_or_else(|| CompactRecentBlockhash::Nonce(recent_blockhash));

            let mut instructions = Vec::with_capacity(m.instructions.len());
            for ix in &m.instructions {
                instructions.push(CompactInstruction {
                    program_id_index: ix.program_id_index,
                    accounts: ix.accounts.as_ref(),
                    data: ix.data.as_ref(),
                });
            }

            CompactMessage::Legacy(CompactLegacyMessage {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            })
        }

        VersionedMessage::V0(m) => {
            let header = CompactMessageHeader {
                num_required_signatures: m.header.num_required_signatures,
                num_readonly_signed_accounts: m.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: m.header.num_readonly_unsigned_accounts,
            };

            let mut account_keys = Vec::with_capacity(m.account_keys.len());
            for key in &m.account_keys {
                let idx = index.lookup_unchecked(*key);
                account_keys.push(idx);
            }

            let recent_blockhash: [u8; 32] = m
                .recent_blockhash
                .as_ref()
                .try_into()
                .map_err(|_| anyhow::anyhow!("blockhash len != 32"))?;

            let recent_blockhash = bh_index
                .get(&recent_blockhash)
                .copied()
                .map(CompactRecentBlockhash::Id)
                .unwrap_or_else(|| CompactRecentBlockhash::Nonce(recent_blockhash));

            let mut instructions = Vec::with_capacity(m.instructions.len());
            for ix in &m.instructions {
                instructions.push(CompactInstruction {
                    program_id_index: ix.program_id_index,
                    accounts: ix.accounts.as_ref(),
                    data: ix.data.as_ref(),
                });
            }

            let mut address_table_lookups = Vec::with_capacity(m.address_table_lookups.len());
            for lookup in &m.address_table_lookups {
                let table_idx = index.lookup_unchecked(lookup.account_key);

                address_table_lookups.push(CompactAddressTableLookup {
                    account_key: table_idx,
                    writable_indexes: lookup.writable_indexes.as_ref(),
                    readonly_indexes: lookup.readonly_indexes.as_ref(),
                });
            }

            CompactMessage::V0(CompactV0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            })
        }
    };

    Ok(CompactTransaction {
        signatures,
        message,
    })
}
