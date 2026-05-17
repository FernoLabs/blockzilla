use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    CompactBlockHeader, CompactDataFrame, CompactPohEntry, CompactShredding, KeyIndex, KeyStore,
    OwnedCompactTransaction, SplitCompactBlockRecord, SplitCompactIndexHeader,
    SplitCompactIndexRecord, SplitCompactRuntimeRecord, WincodeLeb128FramedWriter,
    compact_meta_from_proto, write_registry, write_u32_varint,
};
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::{Rewards, TransactionStatusMeta},
    metadata_decoder::{
        ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor, ZstdReusableDecoder,
        decode_transaction_status_meta_from_frame, slot_uses_protobuf_metadata,
        visit_protobuf_transaction_status_meta,
    },
    node::{Node, decode_node},
    reconstruct::LosslessCarBlock,
    versioned_transaction::VersionedTransaction,
};
use prost::Message;
use solana_pubkey::{Pubkey, pubkey};
use solana_short_vec::decode_shortu16_len;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};
use tracing::info;

use crate::genesis_epoch0;
use crate::{BUFFER_SIZE, ProgressTracker, compact::to_compact_transaction, file_nonempty};

const BLOCK_FILE: &str = "block.bin";
const RUNTIME_FILE: &str = "runtime.bin";
const INDEX_FILE: &str = "block-index.bin";
const REGISTRY_FILE: &str = "registry.bin";
const REGISTRY_COUNTS_FILE: &str = "registry_counts.bin";
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
const INDEX_VERSION: u32 = 1;
const MAX_BLOCKHASHES_PER_EPOCH: usize = 432_000;
const MESSAGE_VERSION_PREFIX: u8 = 0x80;

fn frame_len_u64(payload_len: u32) -> u64 {
    u64::from(payload_len) + u64::from(u32_varint_len(payload_len))
}

fn u32_varint_len(mut value: u32) -> u8 {
    let mut len = 1u8;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

pub(crate) fn build(input: &Path, output_dir: &Path, resume: bool) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    info!("Split compact build starting");
    info!("  input: {}", input.display());
    info!("  out:   {}", output_dir.display());

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let have_registry = file_nonempty(&registry_path);
    let have_registry_counts = file_nonempty(&registry_counts_path);
    let have_blockhash_registry = file_nonempty(&blockhash_registry_path);
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    let blockhash_has_genesis = match &genesis {
        Some(genesis) if have_blockhash_registry => genesis_epoch0::blockhash_registry_starts_with(
            &blockhash_registry_path,
            &genesis.genesis_hash,
        )?,
        Some(_) => false,
        None => true,
    };
    if resume
        && have_registry
        && have_registry_counts
        && have_blockhash_registry
        && blockhash_has_genesis
    {
        info!(
            "Reusing existing registry + counts + blockhash outputs: {}, {} and {}",
            registry_path.display(),
            registry_counts_path.display(),
            blockhash_registry_path.display()
        );
    } else {
        if resume && (have_registry || have_registry_counts || have_blockhash_registry) {
            info!(
                "Existing phase-1 outputs were incomplete; rebuilding registry + counts + blockhash"
            );
        }
        build_registry_and_blockhash_for_input(
            input,
            &registry_path,
            Some(&registry_counts_path),
            &blockhash_registry_path,
        )?;
    }
    info!("Phase 1/2 complete: registry + blockhash ready");

    let store = KeyStore::load(&registry_path)?;
    let key_count = store.len();
    let key_index = KeyIndex::build(store.keys);
    info!("Registry loaded: {} keys", key_count);

    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;
    let blockhash_id_offset = match &genesis {
        Some(genesis) if blockhashes.first() == Some(&genesis.genesis_hash) => 1,
        Some(genesis) => {
            anyhow::bail!(
                "epoch-0 blockhash registry does not start with genesis hash {}",
                hex32(&genesis.genesis_hash)
            );
        }
        None => 0,
    };
    let mut bh_index =
        GxHashMap::with_capacity_and_hasher(blockhashes.len(), GxBuildHasher::default());
    for (index, hash) in blockhashes.iter().enumerate() {
        bh_index.insert(*hash, index as i32);
    }
    info!("Blockhash registry loaded: {} hashes", blockhashes.len());
    info!("Phase 2/2 starting: writing block.bin + runtime.bin + block-index.bin");

    let block_path = output_dir.join(BLOCK_FILE);
    let runtime_path = output_dir.join(RUNTIME_FILE);
    let index_path = output_dir.join(INDEX_FILE);

    let block_file =
        File::create(&block_path).with_context(|| format!("create {}", block_path.display()))?;
    let runtime_file = File::create(&runtime_path)
        .with_context(|| format!("create {}", runtime_path.display()))?;
    let index_file =
        File::create(&index_path).with_context(|| format!("create {}", index_path.display()))?;

    let mut block_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, block_file));
    let mut runtime_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, runtime_file));
    let mut index_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, index_file));

    index_writer.write(&SplitCompactIndexHeader {
        version: INDEX_VERSION,
    })?;

    let mut progress = ProgressTracker::new("Split Compact Write");
    let mut block_offset = 0u64;
    let mut runtime_offset = 0u64;
    let mut block_id: u32 = 0;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let mut runtime_scratch = Vec::with_capacity(8 << 20);

    with_lossless_block_stream(input, |block| {
        let slot = require_block(block)?.slot;
        let (block_record, runtime_record) = build_records(
            block,
            &key_index,
            &bh_index,
            block_id.saturating_add(blockhash_id_offset),
        )?;

        let block_len =
            u32::try_from(block_writer.write_with_scratch(&block_record, &mut block_scratch)?)
                .context("split compact block exceeds u32::MAX")?;
        let runtime_len = u32::try_from(
            runtime_writer.write_with_scratch(&runtime_record, &mut runtime_scratch)?,
        )
        .context("split compact runtime record exceeds u32::MAX")?;
        let tx_count = u32::try_from(runtime_record.tx_metadata.len())
            .map_err(|_| anyhow!("slot {} transaction count exceeds u32::MAX", slot))?;

        index_writer.write(&SplitCompactIndexRecord {
            slot,
            block_id,
            block_offset,
            block_len,
            runtime_offset,
            runtime_len,
            tx_count,
        })?;

        block_offset += frame_len_u64(block_len);
        runtime_offset += frame_len_u64(runtime_len);
        block_id = block_id.wrapping_add(1);

        progress.update_slot(slot);
        progress.update(1, tx_count as u64);
        Ok(())
    })?;

    block_writer.flush()?;
    runtime_writer.flush()?;
    index_writer.flush()?;
    progress.final_report();
    info!("Split compact build complete");
    info!("  block:    {}", block_path.display());
    info!("  runtime:  {}", runtime_path.display());
    info!("  index:    {}", index_path.display());
    Ok(())
}

fn build_records(
    block: &LosslessCarBlock,
    key_index: &KeyIndex,
    bh_index: &GxHashMap<[u8; 32], i32>,
    block_id: u32,
) -> Result<(SplitCompactBlockRecord, SplitCompactRuntimeRecord)> {
    let raw_block = require_block(block)?;
    let rewards = build_rewards_frame(block)?;

    let header = CompactBlockHeader {
        slot: raw_block.slot,
        parent_slot: raw_block.meta.parent_slot.unwrap_or(0),
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time: raw_block.meta.blocktime,
        block_height: raw_block.meta.block_height,
        shredding: raw_block
            .shredding
            .iter()
            .map(|item| CompactShredding {
                entry_end_idx: item.entry_end_idx,
                shred_end_idx: item.shred_end_idx,
            })
            .collect(),
        poh_entries: block
            .entries
            .iter()
            .map(|entry| {
                Ok(CompactPohEntry {
                    num_hashes: entry.num_hashes,
                    hash: entry.hash,
                    tx_count: u32::try_from(entry.transactions.len()).map_err(|_| {
                        anyhow!(
                            "slot {} entry {} transaction count exceeds u32::MAX",
                            raw_block.slot,
                            entry.location.entry_index
                        )
                    })?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        rewards,
    };

    let mut txs = Vec::with_capacity(block.transactions.len());
    let mut tx_metadata = Vec::with_capacity(block.transactions.len());
    let mut zstd = ZstdReusableDecoder::new();

    for (tx_index, tx_node) in block.transactions.iter().enumerate() {
        let tx_bytes = tx_node
            .transaction_bytes(&block.dataframes)
            .with_context(|| {
                tx_context(raw_block.slot, tx_index, "reassemble transaction bytes")
            })?;
        let versioned =
            wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes).map_err(|err| {
                anyhow!(
                    "{}: {}",
                    tx_context(raw_block.slot, tx_index, "decode transaction"),
                    err
                )
            })?;
        let compact_tx =
            to_compact_transaction(&versioned, key_index, bh_index).with_context(|| {
                tx_context(
                    raw_block.slot,
                    tx_index,
                    "convert transaction to compact form",
                )
            })?;
        txs.push(OwnedCompactTransaction::from_borrowed(&compact_tx));

        let metadata_bytes = tx_node
            .metadata_bytes(&block.dataframes)
            .with_context(|| tx_context(raw_block.slot, tx_index, "reassemble metadata bytes"))?;
        if metadata_bytes.is_empty() {
            tx_metadata.push(None);
            continue;
        }

        let mut meta = TransactionStatusMeta::default();
        decode_transaction_status_meta_from_frame(
            raw_block.slot,
            &metadata_bytes,
            &mut meta,
            &mut zstd,
        )
        .map_err(|err| {
            anyhow!(
                "{}: {}",
                tx_context(raw_block.slot, tx_index, "decode metadata"),
                err
            )
        })?;
        let compact_meta = compact_meta_from_proto(&meta, key_index).with_context(|| {
            tx_context(raw_block.slot, tx_index, "convert metadata to compact form")
        })?;
        tx_metadata.push(Some(compact_meta));
    }

    Ok((
        SplitCompactBlockRecord { header, txs },
        SplitCompactRuntimeRecord {
            slot: raw_block.slot,
            tx_metadata,
        },
    ))
}

fn build_rewards_frame(block: &LosslessCarBlock) -> Result<Option<CompactDataFrame>> {
    let raw_block = require_block(block)?;
    if let Some(rewards) = block.rewards.as_ref() {
        let data = rewards
            .rewards_bytes(&block.dataframes)
            .with_context(|| format!("slot {} reassemble rewards bytes", raw_block.slot))?;
        return Ok(Some(CompactDataFrame {
            hash: rewards.data.hash,
            index: rewards.data.index,
            total: rewards.data.total,
            data,
        }));
    }

    let Some(rewards_ref) = raw_block.rewards.as_ref() else {
        return Ok(None);
    };
    let Some(inline) = rewards_ref.inline_raw_bytes() else {
        anyhow::bail!(
            "slot {} rewards reference did not resolve to inline raw bytes",
            raw_block.slot
        );
    };

    inline_rewards_frame(raw_block.slot, inline)
}

fn require_block(block: &LosslessCarBlock) -> Result<&of_car_reader::reconstruct::RawBlockNode> {
    block
        .block
        .as_ref()
        .ok_or_else(|| anyhow!("lossless block was missing its terminal block node"))
}

fn tx_context(slot: u64, tx_index: usize, action: &str) -> String {
    format!("slot {slot} tx#{tx_index} {action}")
}

fn inline_rewards_frame(slot: u64, inline: &[u8]) -> Result<Option<CompactDataFrame>> {
    if let Ok(Node::Rewards(rewards)) = decode_node(inline) {
        if rewards.slot != slot {
            anyhow::bail!(
                "slot {} inline rewards node claimed slot {}",
                slot,
                rewards.slot
            );
        }
        if rewards.data.next.is_some() {
            anyhow::bail!(
                "slot {} inline rewards node used dataframe continuations that split compact does not encode yet",
                slot
            );
        }
        return Ok(Some(CompactDataFrame {
            hash: rewards.data.hash,
            index: rewards.data.index,
            total: rewards.data.total,
            data: rewards.data.data.to_vec(),
        }));
    }

    if let Ok(Node::DataFrame(frame)) = decode_node(inline) {
        if frame.next.is_some() {
            anyhow::bail!(
                "slot {} inline rewards dataframe used continuations that split compact does not encode yet",
                slot
            );
        }
        return Ok(Some(CompactDataFrame {
            hash: frame.hash,
            index: frame.index,
            total: frame.total,
            data: frame.data.to_vec(),
        }));
    }

    if Rewards::decode(inline).is_ok() {
        return Ok(Some(CompactDataFrame {
            hash: None,
            index: None,
            total: None,
            data: inline.to_vec(),
        }));
    }

    anyhow::bail!(
        "slot {} inline rewards payload was neither a rewards node, dataframe, nor rewards protobuf",
        slot
    );
}

fn with_lossless_block_stream<F>(input: &Path, mut f: F) -> Result<()>
where
    F: FnMut(&LosslessCarBlock) -> Result<()>,
{
    let input = open_car_input(input)?;
    let mut reader = CarBlockReader::with_capacity(input, BUFFER_SIZE);
    reader.skip_header()?;

    let mut block = LosslessCarBlock::default();
    while reader.read_until_block_lossless(&mut block)? {
        f(&block)?;
    }

    Ok(())
}

fn open_car_input(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let file = BufReader::with_capacity(BUFFER_SIZE, file);

    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
    {
        let decoder = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("init zstd decoder for {}", path.display()))?;
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(file))
    }
}

pub(crate) fn build_registry_and_blockhash_for_input(
    input: &Path,
    registry_path: &Path,
    registry_counts_path: Option<&Path>,
    blockhash_registry_path: &Path,
) -> Result<()> {
    info!("Building registry + blockhash for {}", input.display());

    let mut blockhash_out: Vec<u8> = Vec::with_capacity(MAX_BLOCKHASHES_PER_EPOCH * 32);
    // Start near the expected unique-key scale for a busy epoch and let the
    // table grow if needed. Preallocating 50M buckets was fast but wastes
    // precious RAM on the 8 GB NAS during concurrent runs.
    let mut counter = PubkeyCounter::new(8_000_000);
    let start = Instant::now();
    let mut progress = ProgressTracker::new("Split Compact Registry");
    let mut timings = RegistryBuildTimings::default();
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    if let Some(genesis) = &genesis {
        blockhash_out.extend_from_slice(&genesis.genesis_hash);
        genesis_epoch0::add_pubkeys_to(genesis, |key| counter.add32(key));
        info!(
            "Seeded epoch-0 genesis: accounts={} reward_pools={} builtins={} genesis_hash={}",
            genesis.genesis.accounts.len(),
            genesis.genesis.reward_pools.len(),
            genesis.genesis.builtins.len(),
            hex32(&genesis.genesis_hash)
        );
    }

    let stream_started = Instant::now();
    with_lossless_block_stream(input, |block| {
        let raw_block = require_block(block)?;
        let blockhash = blockhash_for_block(block);
        blockhash_out.extend_from_slice(&blockhash);

        let count_started = Instant::now();
        let txs = count_block_pubkeys(block, &mut counter, &mut timings)?;
        timings.count_block_total += count_started.elapsed();
        timings.blocks += 1;
        timings.txs += txs;
        progress.update_slot(raw_block.slot);
        progress.update(1, txs);
        Ok(())
    })?;
    timings.stream_total = stream_started.elapsed();

    progress.final_report();

    {
        let write_started = Instant::now();
        let mut file = File::create(blockhash_registry_path)
            .with_context(|| format!("create {}", blockhash_registry_path.display()))?;
        file.write_all(&blockhash_out)
            .with_context(|| format!("write {}", blockhash_registry_path.display()))?;
        file.flush()
            .with_context(|| format!("flush {}", blockhash_registry_path.display()))?;
        timings.write_blockhash = write_started.elapsed();
    }

    let sort_started = Instant::now();
    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    timings.sort_registry = sort_started.elapsed();
    let mut keys: Vec<[u8; 32]> = Vec::with_capacity(items.len());
    let mut counts: Vec<u32> = Vec::with_capacity(items.len());
    for (key, count) in items {
        keys.push(key);
        counts.push(count);
    }

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] =
        &[pubkey!("ComputeBudget111111111111111111111111111111")];
    for builtin in BUILTIN_PROGRAM_KEYS {
        let builtin = builtin.to_bytes();
        if !keys.iter().any(|value| value == &builtin) {
            keys.insert(0, builtin);
            counts.insert(0, 0);
        }
    }

    {
        let write_started = Instant::now();
        write_registry(registry_path, &keys)
            .with_context(|| format!("write {}", registry_path.display()))?;
        if let Some(path) = registry_counts_path {
            write_registry_counts(path, &counts)
                .with_context(|| format!("write {}", path.display()))?;
        }
        timings.write_registry = write_started.elapsed();
    }
    info!(
        "Registry + counts + blockhash built in {:.2}s: keys={} blockhashes={}",
        start.elapsed().as_secs_f64(),
        keys.len(),
        blockhash_out.len() / 32
    );
    info!(
        "Registry instrumentation: stream_total={:.3}s stream_other={:.3}s count_block={:.3}s tx_reassemble={:.3}s tx_pubkey_scan={:.3}s metadata_reassemble={:.3}s metadata_decode_count={:.3}s sort={:.3}s write_blockhash={:.3}s write_registry={:.3}s blocks={} txs={} metadata_frames={} tx_scratch_max={} metadata_scratch_max={}",
        timings.stream_total.as_secs_f64(),
        timings
            .stream_total
            .saturating_sub(timings.count_block_total)
            .as_secs_f64(),
        timings.count_block_total.as_secs_f64(),
        timings.tx_reassemble.as_secs_f64(),
        timings.tx_pubkey_scan.as_secs_f64(),
        timings.metadata_reassemble.as_secs_f64(),
        timings.metadata_decode_count.as_secs_f64(),
        timings.sort_registry.as_secs_f64(),
        timings.write_blockhash.as_secs_f64(),
        timings.write_registry.as_secs_f64(),
        timings.blocks,
        timings.txs,
        timings.metadata_frames,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
    );
    Ok(())
}

fn write_registry_counts(path: &Path, counts: &[u32]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    for count in counts {
        write_u32_varint(&mut writer, *count)?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn blockhash_for_block(block: &LosslessCarBlock) -> [u8; 32] {
    block
        .entries
        .last()
        .map(|entry| entry.hash)
        .unwrap_or([0u8; 32])
}

fn count_block_pubkeys(
    block: &LosslessCarBlock,
    counter: &mut PubkeyCounter,
    timings: &mut RegistryBuildTimings,
) -> Result<u64> {
    let raw_block = require_block(block)?;
    let mut zstd = ZstdReusableDecoder::new();
    let mut txs = 0u64;
    let mut tx_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut reassemble_visited = std::collections::HashSet::new();

    for (tx_index, tx_node) in block.transactions.iter().enumerate() {
        txs += 1;

        let reassemble_started = Instant::now();
        tx_node
            .transaction_bytes_into(&block.dataframes, &mut tx_bytes, &mut reassemble_visited)
            .with_context(|| {
                tx_context(raw_block.slot, tx_index, "reassemble transaction bytes")
            })?;
        timings.tx_reassemble += reassemble_started.elapsed();
        timings.tx_scratch_max = timings.tx_scratch_max.max(tx_bytes.capacity());
        let scan_started = Instant::now();
        count_transaction_pubkeys_from_bytes(&tx_bytes, counter)
            .with_context(|| tx_context(raw_block.slot, tx_index, "count transaction pubkeys"))?;
        timings.tx_pubkey_scan += scan_started.elapsed();

        let reassemble_started = Instant::now();
        tx_node
            .metadata_bytes_into(
                &block.dataframes,
                &mut metadata_bytes,
                &mut reassemble_visited,
            )
            .with_context(|| tx_context(raw_block.slot, tx_index, "reassemble metadata bytes"))?;
        timings.metadata_reassemble += reassemble_started.elapsed();
        timings.metadata_scratch_max = timings.metadata_scratch_max.max(metadata_bytes.capacity());
        if metadata_bytes.is_empty() {
            continue;
        }

        timings.metadata_frames += 1;
        let metadata_started = Instant::now();
        count_metadata_pubkeys_from_frame(raw_block.slot, &metadata_bytes, &mut zstd, counter)
            .with_context(|| tx_context(raw_block.slot, tx_index, "count metadata pubkeys"))?;
        timings.metadata_decode_count += metadata_started.elapsed();
    }

    Ok(txs)
}

#[derive(Default)]
struct RegistryBuildTimings {
    stream_total: Duration,
    count_block_total: Duration,
    tx_reassemble: Duration,
    tx_pubkey_scan: Duration,
    metadata_reassemble: Duration,
    metadata_decode_count: Duration,
    sort_registry: Duration,
    write_blockhash: Duration,
    write_registry: Duration,
    blocks: u64,
    txs: u64,
    metadata_frames: u64,
    tx_scratch_max: usize,
    metadata_scratch_max: usize,
}

fn count_transaction_pubkeys_from_bytes(tx_data: &[u8], counter: &mut PubkeyCounter) -> Result<()> {
    let mut cursor = TxPubkeyCursor::new(tx_data);
    let signature_count = cursor.read_short_len()?;
    cursor.skip(
        signature_count
            .checked_mul(64)
            .context("signature byte count overflow")?,
    )?;

    let first = cursor.read_u8()?;
    if first & MESSAGE_VERSION_PREFIX != 0 {
        let version = first & !MESSAGE_VERSION_PREFIX;
        if version != 0 {
            anyhow::bail!("unsupported transaction message version {version}");
        }
        cursor.skip(3)?;
        count_compiled_message_pubkeys(&mut cursor, counter)?;
        skip_compiled_instructions(&mut cursor)?;
        count_address_table_lookup_pubkeys(&mut cursor, counter)?;
    } else {
        cursor.skip(2)?;
        count_compiled_message_pubkeys(&mut cursor, counter)?;
        skip_compiled_instructions(&mut cursor)?;
    }

    if !cursor.is_empty() {
        anyhow::bail!("transaction has {} trailing bytes", cursor.remaining());
    }
    Ok(())
}

fn count_compiled_message_pubkeys(
    cursor: &mut TxPubkeyCursor<'_>,
    counter: &mut PubkeyCounter,
) -> Result<()> {
    let account_count = cursor.read_short_len()?;
    for _ in 0..account_count {
        counter.add32(cursor.take_array::<32>()?);
    }
    cursor.skip(32)?;
    Ok(())
}

fn skip_compiled_instructions(cursor: &mut TxPubkeyCursor<'_>) -> Result<()> {
    let instruction_count = cursor.read_short_len()?;
    for _ in 0..instruction_count {
        cursor.skip(1)?;
        let account_index_count = cursor.read_short_len()?;
        cursor.skip(account_index_count)?;
        let data_len = cursor.read_short_len()?;
        cursor.skip(data_len)?;
    }
    Ok(())
}

fn count_address_table_lookup_pubkeys(
    cursor: &mut TxPubkeyCursor<'_>,
    counter: &mut PubkeyCounter,
) -> Result<()> {
    let lookup_count = cursor.read_short_len()?;
    for _ in 0..lookup_count {
        counter.add32(cursor.take_array::<32>()?);
        let writable_count = cursor.read_short_len()?;
        cursor.skip(writable_count)?;
        let readonly_count = cursor.read_short_len()?;
        cursor.skip(readonly_count)?;
    }
    Ok(())
}

fn count_metadata_pubkeys_from_frame(
    slot: u64,
    metadata_bytes: &[u8],
    zstd: &mut ZstdReusableDecoder,
    counter: &mut PubkeyCounter,
) -> Result<()> {
    if slot_uses_protobuf_metadata(slot) {
        let protobuf_bytes = if zstd.decompress_if_zstd(metadata_bytes)? {
            zstd.output()
        } else {
            metadata_bytes
        };
        let mut visitor = RegistryPubkeyVisitor::new(counter);
        visit_protobuf_transaction_status_meta(protobuf_bytes, &mut visitor)
            .map_err(|err| anyhow!("protobuf visit: {err}"))?;
        visitor.finish()
    } else {
        let mut meta = TransactionStatusMeta::default();
        decode_transaction_status_meta_from_frame(slot, metadata_bytes, &mut meta, zstd)
            .map_err(|err| anyhow!("{err}"))?;
        count_metadata_pubkeys(&meta, counter)
    }
}

fn count_metadata_pubkeys(meta: &TransactionStatusMeta, counter: &mut PubkeyCounter) -> Result<()> {
    for pk in &meta.loaded_writable_addresses {
        let key: &[u8; 32] = pk
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("loaded writable address had len {}", pk.len()))?;
        counter.add32(key);
    }

    for pk in &meta.loaded_readonly_addresses {
        let key: &[u8; 32] = pk
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("loaded readonly address had len {}", pk.len()))?;
        counter.add32(key);
    }

    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        maybe_count_pubkey_string(counter, &balance.mint);
        maybe_count_pubkey_string(counter, &balance.owner);
        maybe_count_pubkey_string(counter, &balance.program_id);
    }

    for reward in &meta.rewards {
        maybe_count_pubkey_string(counter, &reward.pubkey);
    }

    if !meta.return_data_none
        && let Some(return_data) = meta.return_data.as_ref()
    {
        let key: &[u8; 32] = return_data.program_id.as_slice().try_into().map_err(|_| {
            anyhow!(
                "return data program_id had len {}",
                return_data.program_id.len()
            )
        })?;
        counter.add32(key);
    }

    Ok(())
}

fn maybe_count_pubkey_string(counter: &mut PubkeyCounter, value: &str) {
    if value.is_empty() {
        return;
    }
    if let Ok(pubkey) = Pubkey::from_str(value) {
        counter.add32(pubkey.as_array());
    }
}

struct RegistryPubkeyVisitor<'a> {
    counter: &'a mut PubkeyCounter,
    error: Option<anyhow::Error>,
}

impl<'a> RegistryPubkeyVisitor<'a> {
    fn new(counter: &'a mut PubkeyCounter) -> Self {
        Self {
            counter,
            error: None,
        }
    }

    fn record_error(&mut self, err: anyhow::Error) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    fn finish(self) -> Result<()> {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl<'a, 'b> TransactionStatusMetaVisitor<'b> for RegistryPubkeyVisitor<'a> {
    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_rewards(&self) -> bool {
        true
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        true
    }

    #[inline]
    fn wants_return_data(&self) -> bool {
        true
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'b>) {
        maybe_count_pubkey_string(self.counter, balance.mint);
        maybe_count_pubkey_string(self.counter, balance.owner);
        maybe_count_pubkey_string(self.counter, balance.program_id);
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'b>) {
        maybe_count_pubkey_string(self.counter, balance.mint);
        maybe_count_pubkey_string(self.counter, balance.owner);
        maybe_count_pubkey_string(self.counter, balance.program_id);
    }

    #[inline]
    fn reward_raw(&mut self, bytes: &'b [u8]) {
        match of_car_reader::confirmed_block::Reward::decode(bytes) {
            Ok(reward) => maybe_count_pubkey_string(self.counter, &reward.pubkey),
            Err(err) => self.record_error(anyhow!("decode reward: {err}")),
        }
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'b [u8]) {
        match address.try_into() {
            Ok(address) => self.counter.add32(address),
            Err(_) => {
                self.record_error(anyhow!("loaded writable address had len {}", address.len()))
            }
        }
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'b [u8]) {
        match address.try_into() {
            Ok(address) => self.counter.add32(address),
            Err(_) => {
                self.record_error(anyhow!("loaded readonly address had len {}", address.len()))
            }
        }
    }

    #[inline]
    fn return_data(&mut self, return_data: ReturnDataVisit<'b>) {
        match return_data.program_id.try_into() {
            Ok(program_id) => self.counter.add32(program_id),
            Err(_) => self.record_error(anyhow!(
                "return data program_id had len {}",
                return_data.program_id.len()
            )),
        }
    }
}

struct TxPubkeyCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TxPubkeyCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_short_len(&mut self) -> Result<usize> {
        let (len, used) = decode_shortu16_len(&self.bytes[self.position..])
            .map_err(|()| anyhow!("decode short vec len"))?;
        self.position += used;
        Ok(len)
    }

    fn take_array<const N: usize>(&mut self) -> Result<&'a [u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| anyhow!("invalid fixed array length"))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .context("transaction cursor overflow")?;
        if end > self.bytes.len() {
            anyhow::bail!(
                "transaction cursor out of bounds: need {}, have {}",
                len,
                self.remaining()
            );
        }
        let out = &self.bytes[self.position..end];
        self.position = end;
        Ok(out)
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.take(len).map(|_| ())
    }
}

struct PubkeyCounter {
    counts: GxHashMap<[u8; 32], u32>,
}

impl PubkeyCounter {
    fn new(capacity: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
        }
    }

    fn add32(&mut self, key: &[u8; 32]) {
        *self.counts.entry(*key).or_insert(0) += 1;
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn load_blockhash_registry_plain(path: &Path) -> Result<Vec<[u8; 32]>> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() % 32 != 0 {
        anyhow::bail!(
            "invalid blockhash registry length: {} (not multiple of 32) path={}",
            bytes.len(),
            path.display()
        );
    }

    let mut hashes = Vec::with_capacity(bytes.len() / 32);
    for chunk in bytes.chunks_exact(32) {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(chunk);
        hashes.push(hash);
    }
    Ok(hashes)
}
