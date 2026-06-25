use anyhow::{Context, Result, anyhow};
use blockzilla_format::{write_registry, write_u32_varint};
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::{Reward, TransactionStatusMeta},
    metadata_decoder::{
        ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor, ZstdReusableDecoder,
        decode_transaction_status_meta_from_frame, slot_uses_protobuf_metadata,
        visit_protobuf_transaction_status_meta,
    },
    reconstruct::LosslessCarBlock,
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

use crate::{BUFFER_SIZE, ProgressTracker, genesis_epoch0};

const MAX_BLOCKHASHES_PER_EPOCH: usize = 432_000;
const MESSAGE_VERSION_PREFIX: u8 = 0x80;
const ZSTD_WINDOW_LOG_MAX: u32 = 31;

fn require_block(block: &LosslessCarBlock) -> Result<&of_car_reader::reconstruct::RawBlockNode> {
    block
        .block
        .as_ref()
        .ok_or_else(|| anyhow!("lossless block was missing its terminal block node"))
}

fn tx_context(slot: u64, tx_index: usize, action: &str) -> String {
    format!("slot {slot} tx#{tx_index} {action}")
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
        let mut decoder = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("init zstd decoder for {}", path.display()))?;
        decoder
            .window_log_max(ZSTD_WINDOW_LOG_MAX)
            .with_context(|| format!("set zstd window_log_max for {}", path.display()))?;
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
        match Reward::decode(bytes) {
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
