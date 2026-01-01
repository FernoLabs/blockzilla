use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use solana_pubkey::Pubkey;
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};
use tracing::{Level, error, info, warn};
use wincode::Deserialize;

use ahash::AHashMap;

use solana_message::VersionedMessage;
use solana_transaction::versioned::VersionedTransaction;

use car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    error::GroupError,
    metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame},
    node::{Node, decode_node},
    versioned_transaction::VersionedTransactionSchema,
};

use blockzilla_format::{
    CompactBlockHeader, CompactBlockRecord, CompactTxWithMeta, PostcardFramedWriter, Registry,
    compact_meta_from_proto, load_registry, to_compact_transaction, write_registry,
};

const BUFFER_SIZE: usize = 256 << 20;
const PROGRESS_REPORT_INTERVAL_SECS: u64 = 3;
const SLOTS_PER_EPOCH: u64 = 432_000;

// ----- NEW: tiny debug helpers -----

fn file_nonempty(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
}

fn hex_prefix(data: &[u8], n: usize) -> String {
    let n = n.min(data.len());
    let mut s = String::with_capacity(n * 2);
    for b in &data[..n] {
        use core::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

fn tx_kind(vtx: &VersionedTransaction) -> &'static str {
    match &vtx.message {
        VersionedMessage::Legacy(_) => "legacy",
        VersionedMessage::V0(_) => "v0",
    }
}

// ----- CLI -----

#[derive(Parser)]
#[command(name = "blockzilla-optimizer")]
#[command(
    about = "Two-pass optimizer: build registry (counts) then build compact (postcard framed)"
)]
#[command(version)]
struct Cli {
    /// Cache directory containing CAR files
    #[arg(long, default_value = "cache", global = true)]
    cache_dir: PathBuf,

    /// Output directory for blockzilla archives
    #[arg(long, default_value = "blockzilla-v1", global = true)]
    output_dir: PathBuf,

    /// Resume: if outputs exist, skip finished phases
    #[arg(long, default_value_t = true, global = true)]
    resume: bool,

    /// Skip transactions that fail decode/convert instead of aborting the whole run
    #[arg(long, default_value_t = false, global = true)]
    skip_bad_txs: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run both passes: build registry.bin then compact.bin
    Build {
        /// Epoch number
        epoch: u64,

        /// Keep registry and compact, do not remove anything
        #[arg(long, default_value_t = true)]
        keep: bool,
    },

    /// Pass 1 only: build registry.bin from CAR
    BuildRegistry { epoch: u64 },

    /// Pass 2 only: build compact.bin from CAR + registry.bin
    Compact { epoch: u64 },

    /// Process all epochs found in the cache directory
    BuildAll,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Build { epoch, .. } => {
            let (_, _, registry_path, compact_path) = epoch_paths(&cli, epoch);

            if cli.resume && file_nonempty(&registry_path) {
                info!(
                    "Resume: registry exists, skipping phase 1: {}",
                    registry_path.display()
                );
            } else {
                build_registry(&cli, epoch)?;
            }

            if cli.resume && file_nonempty(&compact_path) {
                info!(
                    "Resume: compact exists, skipping phase 2: {}",
                    compact_path.display()
                );
            } else {
                build_compact(&cli, epoch)?;
            }
        }
        Cmd::BuildRegistry { epoch } => build_registry(&cli, epoch)?,
        Cmd::Compact { epoch } => build_compact(&cli, epoch)?,
        Cmd::BuildAll => process_all_epochs(&cli)?,
    }

    Ok(())
}

fn epoch_paths(cli: &Cli, epoch: u64) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let car_path = cli.cache_dir.join(format!("epoch-{}.car.zst", epoch));
    let epoch_dir = cli.output_dir.join(format!("epoch-{}", epoch));
    let registry_path = epoch_dir.join("registry.bin");
    let compact_path = epoch_dir.join("compact.bin");
    (car_path, epoch_dir, registry_path, compact_path)
}

// ----- progress -----

fn format_duration(seconds: f64) -> String {
    let total_secs = seconds as u64;

    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

struct ProgressTracker {
    start_time: Instant,
    last_report: Instant,
    blocks: u64,
    txs: u64,
    report_interval: Duration,
    estimated_total_blocks: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    blocks_since_report: u64,
    txs_since_report: u64,
    phase: &'static str,
}

impl ProgressTracker {
    fn new(phase: &'static str) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_report: now,
            blocks: 0,
            txs: 0,
            report_interval: Duration::from_secs(PROGRESS_REPORT_INTERVAL_SECS),
            estimated_total_blocks: SLOTS_PER_EPOCH,
            first_slot: None,
            last_slot: None,
            blocks_since_report: 0,
            txs_since_report: 0,
            phase,
        }
    }

    #[inline(always)]
    fn update_slot(&mut self, slot: u64) {
        if self.first_slot.is_none() {
            self.first_slot = Some(slot);
        }
        self.last_slot = Some(slot);
    }

    #[inline(always)]
    fn update(&mut self, blocks_delta: u64, txs_delta: u64) {
        self.blocks += blocks_delta;
        self.txs += txs_delta;
        self.blocks_since_report += blocks_delta;
        self.txs_since_report += txs_delta;

        let now = Instant::now();
        if now.duration_since(self.last_report) >= self.report_interval {
            self.report();
            self.last_report = now;
            self.blocks_since_report = 0;
            self.txs_since_report = 0;
        }
    }

    fn report(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return;
        }

        let blocks_per_sec = self.blocks as f64 / elapsed;
        let txs_per_sec = self.txs as f64 / elapsed;

        if let (Some(first), Some(last)) = (self.first_slot, self.last_slot) {
            let slots_processed = last.saturating_sub(first);
            let progress_pct =
                (slots_processed as f64 / self.estimated_total_blocks as f64) * 100.0;

            if blocks_per_sec > 0.0 && slots_processed > 0 {
                let slots_remaining = self.estimated_total_blocks.saturating_sub(slots_processed);
                let blocks_per_slot = self.blocks as f64 / slots_processed as f64;
                let estimated_remaining_blocks = (slots_remaining as f64 * blocks_per_slot) as u64;
                let eta_seconds = estimated_remaining_blocks as f64 / blocks_per_sec;

                let global_eta_msg = if self.phase == "Phase 1/2" {
                    let global_eta = (eta_seconds + elapsed) * 2.0;
                    format!(" (global ETA: {})", format_duration(global_eta))
                } else {
                    String::new()
                };

                info!(
                    "[{}] progress={:.1}% ETA={}{} | blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | slots={}-{} ({}) | elapsed={}",
                    self.phase,
                    progress_pct,
                    format_duration(eta_seconds),
                    global_eta_msg,
                    self.blocks,
                    blocks_per_sec,
                    self.txs,
                    txs_per_sec,
                    first,
                    last,
                    slots_processed,
                    format_duration(elapsed)
                );
            } else {
                info!(
                    "[{}] progress={:.1}% | blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | slots={}-{} ({}) | elapsed={}",
                    self.phase,
                    progress_pct,
                    self.blocks,
                    blocks_per_sec,
                    self.txs,
                    txs_per_sec,
                    first,
                    last,
                    slots_processed,
                    format_duration(elapsed)
                );
            }
        } else {
            info!(
                "[{}] blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | elapsed={}",
                self.phase,
                self.blocks,
                blocks_per_sec,
                self.txs,
                txs_per_sec,
                format_duration(elapsed)
            );
        }
    }

    fn final_report(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let blocks_per_sec = self.blocks as f64 / elapsed;
        let txs_per_sec = self.txs as f64 / elapsed;

        let mut msg = format!(
            "[{}] Complete: blocks={} txs={} | {:.0} blk/s, {:.0} tx/s | elapsed={}",
            self.phase,
            self.blocks,
            self.txs,
            blocks_per_sec,
            txs_per_sec,
            format_duration(elapsed)
        );

        if let (Some(first), Some(last)) = (self.first_slot, self.last_slot) {
            let slots_processed = last.saturating_sub(first);
            msg.push_str(&format!(
                " | slots={}-{} ({})",
                first, last, slots_processed
            ));
        }

        info!("{}", msg);
    }
}

// ----- phase 1: registry -----

fn build_registry(cli: &Cli, epoch: u64) -> Result<()> {
    let (car_path, epoch_dir, registry_path, _) = epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }
    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building registry (counting phase) epoch={}", epoch);
    info!("  car:      {}", car_path.display());
    info!("  out:      {}", registry_path.display());

    let mut counter = PubkeyCounter::new(16_000_000);
    let mut progress = ProgressTracker::new("Phase 1/2");

    stream_car_blocks(&car_path, |group| {
        let (blocks_delta, txs_delta, slot) = registry_process_block(group, &mut counter)?;
        if let Some(s) = slot {
            progress.update_slot(s);
        }
        progress.update(blocks_delta, txs_delta);
        Ok(())
    })?;

    progress.final_report();
    info!("Unique pubkeys: {}", counter.counts.len());

    info!("Sorting registry by usage frequency...");
    let sort_start = Instant::now();

    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(ka, ca), (kb, cb)| cb.cmp(ca).then_with(|| ka.cmp(kb)));

    let keys: Vec<[u8; 32]> = items.into_iter().map(|(k, _)| k).collect();

    info!(
        "Sorting completed in {:.2}s",
        sort_start.elapsed().as_secs_f64()
    );

    write_registry(&registry_path, &keys)?;
    info!("Registry written: {} keys", keys.len());

    Ok(())
}

struct PubkeyCounter {
    counts: AHashMap<[u8; 32], u32>,
}

impl PubkeyCounter {
    fn new(cap: usize) -> Self {
        Self {
            counts: AHashMap::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn add32(&mut self, k32: &[u8; 32]) {
        *self.counts.entry(*k32).or_insert(0) += 1;
    }
}

fn registry_process_block(
    group: &CarBlockGroup,
    counter: &mut PubkeyCounter,
) -> Result<(u64, u64, Option<u64>), GroupError> {
    let mut tx_scratch = RegistryTxDecodeScratch::new();
    let mut txs = 0u64;

    let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
        Node::Block(b) => b,
        _ => return Err(GroupError::WrongRootKind),
    };

    let block_slot = block.slot;

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

            let vtx = tx_scratch.decode_tx(tx.data.data)?;

            match &vtx.message {
                VersionedMessage::Legacy(m) => {
                    for k in &m.account_keys {
                        counter.add32(k.as_array());
                    }
                }
                VersionedMessage::V0(m) => {
                    for k in &m.account_keys {
                        counter.add32(k.as_array());
                    }
                    for l in &m.address_table_lookups {
                        counter.add32(l.account_key.as_array());
                    }
                }
            }

            if !tx.metadata.data.is_empty() {
                let meta = tx_scratch.decode_meta(tx.slot, &tx.metadata.data)?;

                for pk in &meta.loaded_writable_addresses {
                    counter.add32(pk.as_slice().try_into().unwrap());
                }
                for pk in &meta.loaded_readonly_addresses {
                    counter.add32(pk.as_slice().try_into().unwrap());
                }
                // NEW: count token balance pubkeys (mint/owner/program_id)
                for tb in meta
                    .pre_token_balances
                    .iter()
                    .chain(meta.post_token_balances.iter())
                {
                    // mint (required string but may be malformed)
                    if let Ok(pk) = Pubkey::from_str(&tb.mint) {
                        counter.add32(pk.as_array());
                    }
                    // owner optional
                    if !tb.owner.is_empty() {
                        if let Ok(pk) = Pubkey::from_str(&tb.owner) {
                            counter.add32(pk.as_array());
                        }
                    }
                    // program_id optional
                    if !tb.program_id.is_empty() {
                        if let Ok(pk) = Pubkey::from_str(&tb.program_id) {
                            counter.add32(pk.as_array());
                        }
                    }
                }
            }
        }
    }

    Ok((1, txs, Some(block_slot)))
}

struct RegistryTxDecodeScratch {
    reusable_tx: std::mem::MaybeUninit<VersionedTransaction>,
    has_tx: bool,
    meta_out: car_reader::confirmed_block::TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
}

impl RegistryTxDecodeScratch {
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
                "TX_DECODE (registry) failed: len={} prefix={}",
                bytes.len(),
                hex_prefix(bytes, 32)
            );
            error!("TX_DECODE (registry) error: {:?}", e);
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

impl Drop for RegistryTxDecodeScratch {
    fn drop(&mut self) {
        if self.has_tx {
            unsafe { self.reusable_tx.assume_init_drop() };
        }
    }
}

// ----- phase 2: compact -----

fn build_compact(cli: &Cli, epoch: u64) -> Result<()> {
    let (car_path, epoch_dir, registry_path, compact_path) = epoch_paths(cli, epoch);

    if !car_path.exists() {
        anyhow::bail!("Input not found: {}", car_path.display());
    }
    if !registry_path.exists() {
        anyhow::bail!(
            "Registry not found: {}. Run registry build first.",
            registry_path.display()
        );
    }

    std::fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("Failed to create {}", epoch_dir.display()))?;

    info!("Building compact blocks epoch={}", epoch);
    info!("  car:      {}", car_path.display());
    info!("  registry: {}", registry_path.display());
    info!("  out:      {}", compact_path.display());

    let registry = load_registry(&registry_path)?;
    info!("Registry loaded: {} keys", registry.keys.len());

    // NEW: atomic write to avoid partial compact.bin being considered "done"
    let tmp_path = compact_path.with_extension("bin.tmp");

    let out = File::create(&tmp_path)
        .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
    let out = BufWriter::with_capacity(BUFFER_SIZE, out);
    let mut writer = PostcardFramedWriter::new(out);

    let mut progress = ProgressTracker::new("Phase 2/2");
    let mut scratch = CompactTxDecodeScratch::new();

    stream_car_blocks(&car_path, |group| {
        let (blocks_delta, txs_delta, slot) =
            compact_process_block(group, &registry, &mut writer, &mut scratch)?;
        progress.update(blocks_delta, txs_delta);
        if let Some(s) = slot {
            progress.update_slot(s);
        }
        Ok(())
    })?;

    writer.flush()?;
    // NEW: rename temp -> final (atomic on same filesystem)
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

fn compact_process_block<W: std::io::Write>(
    group: &CarBlockGroup,
    registry: &Registry,
    writer: &mut PostcardFramedWriter<W>,
    scratch: &mut CompactTxDecodeScratch,
) -> Result<(u64, u64, Option<u64>), GroupError> {
    let mut txs = 0u64;
    let mut tx_index_in_block: u32 = 0;

    let block = match decode_node(group.block_payload.as_ref()).map_err(GroupError::Node)? {
        Node::Block(b) => b,
        _ => return Err(GroupError::WrongRootKind),
    };

    let block_slot = block.slot;

    let header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: 0,
        previous_blockhash: 0,
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

            // Fail fast on bad tx decode
            let vtx = scratch.decode_tx(tx_bytes).map_err(|e| {
                error!(
                    "FAIL decode_tx: block_slot={} tx_slot={} tx_index_in_block={} tx_len={} tx_prefix={} cid_digest_prefix={}",
                    block_slot,
                    tx.slot,
                    tx_index_in_block,
                    tx_bytes.len(),
                    hex_prefix(tx_bytes, 32),
                    hex_prefix(tx_cid.hash_bytes(), 16),
                );
                e
            })?;

            // Fail fast on compact tx conversion
            let compact_tx = to_compact_transaction(vtx, registry).map_err(|conv_err| {
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

            // Metadata + logs
            let metadata_opt = if tx.metadata.data.is_empty() {
                None
            } else {
                // Decode meta (fail fast)
                let meta = scratch.decode_meta(tx.slot, &tx.metadata.data).map_err(|e| {
                    error!(
                        "FAIL decode_meta: block_slot={} tx_slot={} tx_index_in_block={} meta_len={} cid_digest_prefix={}",
                        block_slot,
                        tx.slot,
                        tx_index_in_block,
                        tx.metadata.data.len(),
                        hex_prefix(tx_cid.hash_bytes(), 16),
                    );
                    e
                })?;

                // Convert to CompactMetaV1 (this already encodes compact logs into CompactMetaV1.logs)
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

    let rec = CompactBlockRecord {
        header,
        txs: txs_out,
    };
    writer.write(&rec).map_err(|_| GroupError::Io)?;

    Ok((1, txs, Some(block_slot)))
}

// ----- build all -----

fn process_all_epochs(cli: &Cli) -> Result<()> {
    info!(
        "Scanning cache directory for epochs: {}",
        cli.cache_dir.display()
    );

    if !cli.cache_dir.exists() {
        anyhow::bail!("Cache directory not found: {}", cli.cache_dir.display());
    }

    let epochs = discover_epochs(&cli.cache_dir)?;

    if epochs.is_empty() {
        warn!("No epoch files found in {}", cli.cache_dir.display());
        return Ok(());
    }

    info!("Found {} epoch(s) to process: {:?}", epochs.len(), epochs);

    let mut successful = 0;
    let mut failed = 0;
    let batch_start = Instant::now();

    for (idx, epoch) in epochs.iter().enumerate() {
        info!("========================================");
        info!("Processing epoch {} ({}/{})", epoch, idx + 1, epochs.len());
        info!("========================================");

        match process_single_epoch(cli, *epoch) {
            Ok(_) => {
                info!("✓ Successfully processed epoch {}", epoch);
                successful += 1;
            }
            Err(e) => {
                error!("✗ Failed to process epoch {}: {:?}", epoch, e);
                failed += 1;
            }
        }
    }

    let batch_elapsed = batch_start.elapsed().as_secs_f64();

    info!("========================================");
    info!("Batch processing complete!");
    info!("  Successful: {}", successful);
    info!("  Failed:     {}", failed);
    info!("  Total:      {}", successful + failed);
    info!(
        "  Time:       {:.1}s ({:.1}s per epoch avg)",
        batch_elapsed,
        batch_elapsed / epochs.len() as f64
    );
    info!("========================================");

    if failed > 0 {
        anyhow::bail!("{} epoch(s) failed to process", failed);
    }

    Ok(())
}

fn process_single_epoch(cli: &Cli, epoch: u64) -> Result<()> {
    let (_, _, registry_path, compact_path) = epoch_paths(cli, epoch);

    if cli.resume && file_nonempty(&registry_path) {
        info!(
            "Resume: registry exists, skipping phase 1: {}",
            registry_path.display()
        );
    } else {
        build_registry(cli, epoch)
            .with_context(|| format!("Failed to build registry for epoch {}", epoch))?;
    }

    if cli.resume && file_nonempty(&compact_path) {
        info!(
            "Resume: compact exists, skipping phase 2: {}",
            compact_path.display()
        );
    } else {
        build_compact(cli, epoch)
            .with_context(|| format!("Failed to build compact for epoch {}", epoch))?;
    }

    Ok(())
}

fn discover_epochs(cache_dir: &Path) -> Result<Vec<u64>> {
    let mut epochs = Vec::new();

    let entries = fs::read_dir(cache_dir)
        .with_context(|| format!("Failed to read directory: {}", cache_dir.display()))?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => continue,
        };

        if let Some(epoch) = parse_epoch_filename(filename) {
            epochs.push(epoch);
        }
    }

    epochs.sort_unstable();
    Ok(epochs)
}

fn parse_epoch_filename(filename: &str) -> Option<u64> {
    if !filename.starts_with("epoch-") || !filename.ends_with(".car.zst") {
        return None;
    }

    let number_part = filename.strip_prefix("epoch-")?.strip_suffix(".car.zst")?;

    number_part.parse::<u64>().ok()
}

fn stream_car_blocks<F>(car_path: &Path, mut f: F) -> Result<()>
where
    F: FnMut(&CarBlockGroup) -> Result<()>,
{
    let file = File::open(car_path).with_context(|| format!("open {}", car_path.display()))?;
    let file = BufReader::with_capacity(BUFFER_SIZE, file);

    let zstd = zstd::Decoder::with_buffer(file).context("init zstd decoder")?;
    let mut reader = CarBlockReader::with_capacity(zstd, BUFFER_SIZE);
    reader.skip_header().context("skip CAR header")?;

    let mut group = CarBlockGroup::new();
    while let Ok(_) = reader.read_until_block_into(&mut group) {
        f(&group)?;
        group.clear();
    }
    Ok(())
}
