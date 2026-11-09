use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2HotBlockBlob,
    ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction,
    ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
    ArchiveV2HotRewards, ArchiveV2HotTxRow, ArchiveV2HotV0Message, ArchiveV2HotV1Message,
    WincodeArchiveV2PohRecord, WincodeArchiveV2ShreddingRecord, write_archive_v2_hot_block_index,
};
use blockzilla_compact::parse_logs_with_compactor;
use blockzilla_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
    CompactPohEntry, CompactReturnData, CompactReward, CompactShredding, CompactTokenBalance,
    CompactTransactionConfig, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
};
use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter, wincode_leb128_config};
use blockzilla_registry::{KeyIndex, write_registry};
use solana_entry::block_component::BlockComponent;
use solana_ledger::blockstore::Blockstore;

use crate::check::Inspected;
use crate::ledger::{open_read_only, scan_range};
use crate::source::{
    SlotData, SourceInnerGroup, SourceMessage, SourceMessageBody, SourceReward, SourceStatus,
    SourceTokenBalance, SourceTx, SourceV1Message,
};

pub(crate) const ZSTD_LEVEL: i32 = 1;

fn check_zstd_level(level: i32) -> Result<()> {
    let range = zstd::compression_level_range();
    if range.contains(&level) {
        return Ok(());
    }
    bail!("zstd level {level} is outside {} to {}", range.start(), range.end());
}
const INDEX_FLAGS: u32 = 0;
const VOTE_HASH_ROW_LEN: usize = 65;
const RECENT_BLOCKHASH_SLOT_WINDOW: u64 = 150;
const ROLLING_BLOCKHASH_CAPACITY: usize = 300;
type PubkeyCounts = HashMap<[u8; 32], u32>;

pub(crate) fn build_archive(
    ledger: &Path,
    output: &Path,
    start_slot: u64,
    end_slot: u64,
    zstd_level: i32,
    read_threads: u8,
) -> Result<Inspected> {
    check_zstd_level(zstd_level)?;
    let read_threads = usize::from(read_threads);
    if !(1..=32).contains(&read_threads) {
        bail!("read threads {read_threads} is outside 1 to 32");
    }
    let blockstore = open_read_only(ledger)?;
    fs::create_dir_all(output).with_context(|| format!("create {}", output.display()))?;
    let temps = temp_paths(output);
    let result = (|| {
        create_temp_files(&temps)?;
        progress(format!(
            "counting pubkeys in slots {start_slot} to {end_slot}"
        ));
        let mut counts = PubkeyCounts::new();
        let inspected = scan_range(&blockstore, start_slot, end_slot, read_threads, |block| {
            count_slot_pubkeys(block, &mut counts);
            Ok(())
        })?;
        let key_index = prepare_registry(&temps, counts)?;
        let first_slot = inspected
            .block_slots
            .first()
            .copied()
            .context("range has no block")?;
        progress(format!(
            "reading previous blockhashes before slot {first_slot}"
        ));
        let tail = read_previous_tail(&blockstore, first_slot)?;
        let newest = tail
            .last()
            .context("first block is missing parent entries")?;
        if newest.hash != inspected.boundary_hash {
            bail!("slot {first_slot} parent blockhash does not match the rooted block");
        }
        progress(format!("read {} previous blockhashes", tail.len()));
        progress(format!(
            "writing blocks for slots {start_slot} to {end_slot} at zstd level {zstd_level}"
        ));
        let mut writer = ArchiveWriter::create(&temps, key_index, tail, false, zstd_level)?;
        scan_range(
            &blockstore,
            start_slot,
            end_slot,
            read_threads,
            |block| writer.write_block(block),
        )?;
        writer.finish(output)?;
        Ok(inspected)
    })();
    if result.is_err() {
        remove_temps(&temps);
    }
    result
}

pub(crate) fn write_archive<F>(
    output: &Path,
    inspected: &Inspected,
    zstd_level: i32,
    mut load_block: F,
) -> Result<()>
where
    F: FnMut(u64) -> Result<SlotData>,
{
    check_zstd_level(zstd_level)?;
    fs::create_dir_all(output).with_context(|| format!("create {}", output.display()))?;
    let temps = temp_paths(output);
    let result = (|| {
        create_temp_files(&temps)?;
        let mut counts = PubkeyCounts::new();
        for slot in &inspected.block_slots {
            let block = load_block(*slot)?;
            count_slot_pubkeys(&block, &mut counts);
        }
        let key_index = prepare_registry(&temps, counts)?;
        let mut writer = ArchiveWriter::create(&temps, key_index, Vec::new(), true, zstd_level)?;
        for slot in &inspected.block_slots {
            let block = load_block(*slot)?;
            writer.write_block(&block)?;
        }
        writer.finish(output)
    })();
    if result.is_err() {
        remove_temps(&temps);
    }
    result
}

struct ArchivePaths {
    blocks: PathBuf,
    signatures: PathBuf,
    blockhash: PathBuf,
    vote: PathBuf,
    poh: PathBuf,
    shredding: PathBuf,
    registry: PathBuf,
    tail: PathBuf,
}

fn temp_paths(output: &Path) -> ArchivePaths {
    ArchivePaths {
        blocks: output.join(format!("{ARCHIVE_V2_BLOCKS_FILE}.tmp")),
        signatures: output.join(format!("{ARCHIVE_V2_SIGNATURES_FILE}.tmp")),
        blockhash: output.join(format!("{ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE}.tmp")),
        vote: output.join(format!("{ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE}.tmp")),
        poh: output.join(format!("{ARCHIVE_V2_POH_FILE}.tmp")),
        shredding: output.join(format!("{ARCHIVE_V2_SHREDDING_FILE}.tmp")),
        registry: output.join(format!("{ARCHIVE_V2_PUBKEY_REGISTRY_FILE}.tmp")),
        tail: output.join(format!("{ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE}.tmp")),
    }
}

fn temp_file_list(temps: &ArchivePaths) -> [&Path; 8] {
    [
        &temps.blocks,
        &temps.signatures,
        &temps.blockhash,
        &temps.vote,
        &temps.poh,
        &temps.shredding,
        &temps.registry,
        &temps.tail,
    ]
}

fn create_temp_files(temps: &ArchivePaths) -> Result<()> {
    for path in temp_file_list(temps) {
        File::create(path).with_context(|| format!("create {}", path.display()))?;
    }
    Ok(())
}

fn remove_temps(temps: &ArchivePaths) {
    for path in temp_file_list(temps) {
        let _ = fs::remove_file(path);
    }
}

struct ArchiveWriter {
    blocks: BufWriter<File>,
    signatures: BufWriter<File>,
    poh: WincodeLeb128FramedWriter<BufWriter<File>>,
    shredding: WincodeLeb128FramedWriter<BufWriter<File>>,
    blockhashes: Vec<u8>,
    vote_hashes: Vec<u8>,
    tail: Vec<PreviousBlockhash>,
    rolling: RollingBlockhashes,
    seed_parent_on_first_block: bool,
    zstd_level: i32,
    rows: Vec<ArchiveV2HotBlockIndexRow>,
    compressed_offset: u64,
    next_tx_ordinal: u64,
    next_signature_ordinal: u64,
    key_index: KeyIndex,
    paths: ArchivePaths,
}

impl ArchiveWriter {
    fn create(
        temps: &ArchivePaths,
        key_index: KeyIndex,
        tail: Vec<PreviousBlockhash>,
        seed_parent_on_first_block: bool,
        zstd_level: i32,
    ) -> Result<Self> {
        let mut rolling = RollingBlockhashes::default();
        rolling.seed(&tail)?;
        Ok(Self {
            blocks: BufWriter::new(
                File::create(&temps.blocks)
                    .with_context(|| format!("create {}", temps.blocks.display()))?,
            ),
            signatures: BufWriter::new(
                File::create(&temps.signatures)
                    .with_context(|| format!("create {}", temps.signatures.display()))?,
            ),
            poh: WincodeLeb128FramedWriter::new(BufWriter::new(
                File::create(&temps.poh)
                    .with_context(|| format!("create {}", temps.poh.display()))?,
            )),
            shredding: WincodeLeb128FramedWriter::new(BufWriter::new(
                File::create(&temps.shredding)
                    .with_context(|| format!("create {}", temps.shredding.display()))?,
            )),
            blockhashes: Vec::new(),
            vote_hashes: Vec::new(),
            tail,
            rolling,
            seed_parent_on_first_block,
            zstd_level,
            rows: Vec::new(),
            compressed_offset: 0,
            next_tx_ordinal: 0,
            next_signature_ordinal: 0,
            key_index,
            paths: ArchivePaths {
                blocks: temps.blocks.clone(),
                signatures: temps.signatures.clone(),
                blockhash: temps.blockhash.clone(),
                vote: temps.vote.clone(),
                poh: temps.poh.clone(),
                shredding: temps.shredding.clone(),
                registry: temps.registry.clone(),
                tail: temps.tail.clone(),
            },
        })
    }

    fn write_block(&mut self, block: &SlotData) -> Result<()> {
        let slot = block.slot;
        let block_id = u32::try_from(self.rows.len()).context("archive block id exceeds u32")?;
        if self.seed_parent_on_first_block && self.rows.is_empty() {
            let parent_slot = block
                .parent_slot
                .with_context(|| format!("slot {slot} has no parent slot"))?;
            let hash = block
                .previous_blockhash
                .context("first block is missing parent entries")?;
            self.tail = vec![PreviousBlockhash {
                hash,
                slot: parent_slot,
            }];
            self.rolling.seed(&self.tail)?;
        }
        self.rolling.prune_for_slot(slot)?;
        let uncompressed = encode_hot_block(block, block_id, &self.key_index, &self.rolling)?;
        let compressed = zstd::bulk::compress(&uncompressed, self.zstd_level)
            .with_context(|| format!("compress slot {slot}"))?;
        let compressed_len =
            u32::try_from(compressed.len()).context("compressed block exceeds u32")?;
        let uncompressed_len =
            u32::try_from(uncompressed.len()).context("block bytes exceed u32")?;
        self.blocks
            .write_all(&compressed)
            .with_context(|| format!("write slot {slot}"))?;
        let signature_count = append_signatures(&mut self.signatures, block)?;
        self.blockhashes.extend(block.blockhash);
        self.rolling.insert(
            block.blockhash,
            i32::try_from(block_id).context("blockhash id exceeds i32")?,
            slot,
        )?;
        self.vote_hashes.extend(empty_vote_hash_row());
        self.poh
            .write(&poh_record(block, block_id))
            .with_context(|| format!("write PoH for slot {slot}"))?;
        self.shredding
            .write(&shredding_record(block, block_id))
            .with_context(|| format!("write shredding for slot {slot}"))?;
        let tx_count =
            u32::try_from(block.transactions.len()).context("transaction count exceeds u32")?;
        self.rows.push(ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset: self.compressed_offset,
            compressed_len,
            uncompressed_len,
            tx_count,
            first_tx_ordinal: self.next_tx_ordinal,
            first_signature_ordinal: self.next_signature_ordinal,
            signature_count,
        });
        self.compressed_offset = self
            .compressed_offset
            .checked_add(u64::from(compressed_len))
            .context("compressed archive exceeds u64")?;
        self.next_tx_ordinal = self
            .next_tx_ordinal
            .checked_add(u64::from(tx_count))
            .context("transaction ordinal exceeds u64")?;
        self.next_signature_ordinal = self
            .next_signature_ordinal
            .checked_add(u64::from(signature_count))
            .context("signature ordinal exceeds u64")?;
        let written = self.rows.len();
        if written % 1000 == 0 {
            progress(format!("wrote {written} blocks, latest slot {slot}"));
        }
        Ok(())
    }

    fn finish(mut self, output: &Path) -> Result<()> {
        progress(format!("writing finished: {} blocks", self.rows.len()));
        self.blocks
            .flush()
            .with_context(|| format!("flush {}", self.paths.blocks.display()))?;
        self.signatures
            .flush()
            .with_context(|| format!("flush {}", self.paths.signatures.display()))?;
        self.poh.flush().context("flush PoH sidecar")?;
        let poh_writer = self.poh.into_inner();
        poh_writer
            .into_inner()
            .map_err(|err| anyhow::anyhow!(err))
            .context("flush PoH sidecar")?;
        self.shredding.flush().context("flush shredding sidecar")?;
        let shredding_writer = self.shredding.into_inner();
        shredding_writer
            .into_inner()
            .map_err(|err| anyhow::anyhow!(err))
            .context("flush shredding sidecar")?;
        fs::write(&self.paths.blockhash, &self.blockhashes)
            .with_context(|| format!("write {}", self.paths.blockhash.display()))?;
        fs::write(&self.paths.vote, &self.vote_hashes)
            .with_context(|| format!("write {}", self.paths.vote.display()))?;
        fs::write(&self.paths.tail, tail_bytes(&self.tail))
            .with_context(|| format!("write {}", self.paths.tail.display()))?;
        rename_finished(&self.paths.blocks, &output.join(ARCHIVE_V2_BLOCKS_FILE))?;
        rename_finished(
            &self.paths.signatures,
            &output.join(ARCHIVE_V2_SIGNATURES_FILE),
        )?;
        rename_finished(
            &self.paths.blockhash,
            &output.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
        )?;
        rename_finished(
            &self.paths.tail,
            &output.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
        )?;
        rename_finished(
            &self.paths.vote,
            &output.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
        )?;
        rename_finished(&self.paths.poh, &output.join(ARCHIVE_V2_POH_FILE))?;
        rename_finished(
            &self.paths.shredding,
            &output.join(ARCHIVE_V2_SHREDDING_FILE),
        )?;
        rename_finished(
            &self.paths.registry,
            &output.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
        )?;
        write_archive_v2_hot_block_index(
            &output.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            self.compressed_offset,
            self.zstd_level,
            INDEX_FLAGS,
            &self.rows,
        )?;
        Ok(())
    }
}

fn progress(message: impl std::fmt::Display) {
    if cfg!(test) {
        return;
    }
    eprintln!("{message}");
    let _ = std::io::stderr().flush();
}

fn rename_finished(from: &Path, to: &Path) -> Result<()> {
    fs::rename(from, to).with_context(|| format!("rename {} to {}", from.display(), to.display()))
}

#[derive(Clone, Copy)]
struct PreviousBlockhash {
    hash: [u8; 32],
    slot: u64,
}

#[derive(Clone, Copy)]
struct RollingBlockhash {
    hash: [u8; 32],
    block_id: i32,
    slot: u64,
}

struct RollingBlockhashes {
    map: HashMap<[u8; 32], (i32, u64)>,
    order: VecDeque<RollingBlockhash>,
}

impl Default for RollingBlockhashes {
    fn default() -> Self {
        Self {
            map: HashMap::with_capacity(ROLLING_BLOCKHASH_CAPACITY),
            order: VecDeque::with_capacity(ROLLING_BLOCKHASH_CAPACITY),
        }
    }
}

impl RollingBlockhashes {
    fn seed(&mut self, tail: &[PreviousBlockhash]) -> Result<()> {
        for (index, item) in tail.iter().enumerate() {
            let distance_from_newest =
                i32::try_from(tail.len() - index).context("previous blockhash tail exceeds i32")?;
            self.insert(item.hash, -distance_from_newest, item.slot)?;
        }
        Ok(())
    }

    fn insert(&mut self, hash: [u8; 32], block_id: i32, slot: u64) -> Result<()> {
        if self.map.contains_key(&hash) {
            bail!(
                "duplicate blockhash {} at block_id {block_id} slot {slot}",
                hash_text(&hash)
            );
        }
        self.map.insert(hash, (block_id, slot));
        self.order.push_back(RollingBlockhash {
            hash,
            block_id,
            slot,
        });
        self.enforce_capacity();
        Ok(())
    }

    fn prune_for_slot(&mut self, current_slot: u64) -> Result<()> {
        loop {
            let Some(entry) = self.order.front().copied() else {
                return Ok(());
            };
            if entry.slot > current_slot {
                bail!(
                    "block slots went backwards: rolling blockhash slot {} is ahead of current slot {current_slot}",
                    entry.slot
                );
            }
            if current_slot - entry.slot <= RECENT_BLOCKHASH_SLOT_WINDOW {
                return Ok(());
            }
            self.remove_front();
        }
    }

    fn resolve(
        &self,
        hash: &[u8; 32],
        slot: u64,
        tx_index: usize,
    ) -> Result<OwnedCompactRecentBlockhash> {
        let Some((block_id, hash_slot)) = self.map.get(hash).copied() else {
            return Ok(OwnedCompactRecentBlockhash::Nonce(*hash));
        };
        if hash_slot > slot {
            bail!(
                "slot {slot} tx#{tx_index} recent_blockhash {} points at future block slot {hash_slot}",
                hash_text(hash)
            );
        }
        if slot - hash_slot > RECENT_BLOCKHASH_SLOT_WINDOW {
            return Ok(OwnedCompactRecentBlockhash::Nonce(*hash));
        }
        Ok(OwnedCompactRecentBlockhash::Id(block_id))
    }

    fn enforce_capacity(&mut self) {
        while self.order.len() > ROLLING_BLOCKHASH_CAPACITY {
            self.remove_front();
        }
    }

    fn remove_front(&mut self) {
        if let Some(old) = self.order.pop_front()
            && self.map.get(&old.hash).copied() == Some((old.block_id, old.slot))
        {
            self.map.remove(&old.hash);
        }
    }
}

fn tail_bytes(tail: &[PreviousBlockhash]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(tail.len() * 40);
    for item in tail {
        bytes.extend_from_slice(&item.hash);
        bytes.extend_from_slice(&item.slot.to_le_bytes());
    }
    bytes
}

fn hash_text(hash: &[u8; 32]) -> String {
    bs58::encode(hash).into_string()
}

fn copy32(bytes: &[u8]) -> Result<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).context("hash is not 32 bytes")
}

/// Blockhashes of at most 300 rooted blocks before `first_slot`, oldest first.
///
/// The parent of `first_slot` is required. An older missing block ends the walk.
fn read_previous_tail(blockstore: &Blockstore, first_slot: u64) -> Result<Vec<PreviousBlockhash>> {
    let meta = blockstore
        .meta(first_slot)
        .with_context(|| format!("read SlotMeta for slot {first_slot}"))?;
    let Some(meta) = meta else {
        bail!("slot {first_slot} has no SlotMeta");
    };
    let Some(mut slot) = meta.parent_slot else {
        bail!("slot {first_slot} has no parent slot");
    };

    let mut newest_first = Vec::new();
    let mut required = true;
    while newest_first.len() < ROLLING_BLOCKHASH_CAPACITY {
        let Some(ancestor) = read_ancestor(blockstore, slot)? else {
            if required {
                bail!("slot {slot} is missing parent entries");
            }
            break;
        };
        newest_first.push(PreviousBlockhash {
            hash: ancestor.hash,
            slot,
        });
        let Some(parent) = ancestor.parent_slot else {
            break;
        };
        if parent >= slot {
            bail!("slot {slot} parent {parent} does not move backward");
        }
        slot = parent;
        required = false;
    }
    newest_first.reverse();
    Ok(newest_first)
}

struct AncestorBlock {
    hash: [u8; 32],
    parent_slot: Option<u64>,
}

fn read_ancestor(blockstore: &Blockstore, slot: u64) -> Result<Option<AncestorBlock>> {
    let Some(meta) = blockstore
        .meta(slot)
        .with_context(|| format!("read SlotMeta for slot {slot}"))?
    else {
        return Ok(None);
    };
    if !blockstore.is_root(slot) || !meta.is_full() {
        return Ok(None);
    }
    let Some(hash) = blockhash_from_shreds(blockstore, slot)? else {
        return Ok(None);
    };
    Ok(Some(AncestorBlock {
        hash,
        parent_slot: meta.parent_slot,
    }))
}

fn blockhash_from_shreds(blockstore: &Blockstore, slot: u64) -> Result<Option<[u8; 32]>> {
    let (components, _, is_full) = blockstore
        .get_slot_components_with_shred_info(slot, 0, false)
        .with_context(|| format!("read shreds for slot {slot}"))?;
    if !is_full {
        return Ok(None);
    }
    let mut hash = None;
    for component in components {
        if let BlockComponent::EntryBatch(entries) = component
            && let Some(entry) = entries.last()
        {
            hash = Some(copy32(entry.hash.as_ref())?);
        }
    }
    Ok(hash)
}

fn prepare_registry(temps: &ArchivePaths, counts: PubkeyCounts) -> Result<KeyIndex> {
    let keys = ordered_registry_keys(counts);
    write_registry(&temps.registry, &keys)
        .with_context(|| format!("write {}", temps.registry.display()))?;
    Ok(KeyIndex::build_from_slice_low_memory(&keys))
}

fn ordered_registry_keys(counts: PubkeyCounts) -> Vec<[u8; 32]> {
    let mut items: Vec<([u8; 32], u32)> = counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    let compute_budget = compute_budget_program_id();
    let mut keys = Vec::with_capacity(items.len() + 1);
    if !items.iter().any(|(key, _)| *key == compute_budget) {
        keys.push(compute_budget);
    }
    keys.extend(items.into_iter().map(|(key, _)| key));
    keys
}

fn compute_budget_program_id() -> [u8; 32] {
    let bytes = bs58::decode("ComputeBudget111111111111111111111111111111")
        .into_vec()
        .expect("ComputeBudget program id is valid base58");
    bytes
        .as_slice()
        .try_into()
        .expect("ComputeBudget program id is 32 bytes")
}

fn count_slot_pubkeys(block: &SlotData, counts: &mut PubkeyCounts) {
    for tx in &block.transactions {
        match &tx.message {
            SourceMessage::Legacy(body) => count_keys(&body.account_keys, counts),
            SourceMessage::V0(body) => {
                count_keys(&body.account_keys, counts);
                for lookup in &body.lookups {
                    bump_key(counts, lookup.account_key);
                }
            }
            SourceMessage::V1(body) => count_keys(&body.account_keys, counts),
        }
        let Some(status) = &tx.status else {
            continue;
        };
        count_keys(&status.loaded_writable, counts);
        count_keys(&status.loaded_readonly, counts);
        for balance in status
            .pre_token_balances
            .iter()
            .chain(status.post_token_balances.iter())
        {
            bump_key(counts, balance.mint);
            if let Some(owner) = balance.owner {
                bump_key(counts, owner);
            }
            if let Some(program_id) = balance.program_id {
                bump_key(counts, program_id);
            }
        }
        for reward in &status.rewards {
            bump_key(counts, reward.pubkey);
        }
        if let Some(return_data) = &status.return_data {
            bump_key(counts, return_data.program_id);
        }
    }
}

fn count_keys(keys: &[[u8; 32]], counts: &mut PubkeyCounts) {
    for key in keys {
        bump_key(counts, *key);
    }
}

fn bump_key(counts: &mut PubkeyCounts, key: [u8; 32]) {
    counts
        .entry(key)
        .and_modify(|count| *count = count.saturating_add(1))
        .or_insert(1);
}

pub(crate) fn encode_hot_block(
    block: &SlotData,
    block_id: u32,
    key_index: &KeyIndex,
    rolling: &RollingBlockhashes,
) -> Result<Vec<u8>> {
    let entry_txs = block.entries.iter().try_fold(0u32, |total, entry| {
        total
            .checked_add(entry.tx_count)
            .context("entry transaction count overflows")
    })?;
    if entry_txs as usize != block.transactions.len() {
        bail!(
            "slot {} entry transaction count {entry_txs} does not match {}",
            block.slot,
            block.transactions.len()
        );
    }

    let mut message_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut rows = Vec::with_capacity(block.transactions.len());
    for (tx_index, tx) in block.transactions.iter().enumerate() {
        let status = tx.status.as_ref().with_context(|| {
            format!(
                "slot {} transaction {tx_index} is missing status",
                block.slot
            )
        })?;
        let row = encode_tx_row(
            tx,
            status,
            tx_index,
            key_index,
            rolling,
            block.slot,
            &mut message_bytes,
            &mut metadata_bytes,
        )?;
        rows.push(row);
    }

    let rewards = if block.rewards.is_empty() && block.num_partitions.is_none() {
        None
    } else {
        Some(ArchiveV2HotRewards {
            num_partitions: block.num_partitions,
            decoded: block
                .rewards
                .iter()
                .map(|reward| compact_reward(reward, key_index))
                .collect(),
        })
    };
    let blob = ArchiveV2HotBlockBlob {
        header: ArchiveV2HotBlockHeader {
            slot: block.slot,
            parent_slot: block
                .parent_slot
                .with_context(|| format!("slot {} has no parent slot", block.slot))?,
            blockhash_id: block_id,
            previous_blockhash_id: block_id.saturating_sub(1),
            block_time: block.block_time,
            block_height: block.block_height,
            rewards,
        },
        tx_count: u32::try_from(block.transactions.len())
            .context("transaction count exceeds u32")?,
        tx_rows: rows,
        message_bytes,
        metadata_bytes,
        consensus_block_id: block.consensus_block_id,
        block_markers: block.markers.clone(),
    };
    wincode::config::serialize(&blob, wincode_leb128_config()).context("serialize hot block")
}

fn encode_tx_row(
    tx: &SourceTx,
    status: &SourceStatus,
    tx_index: usize,
    key_index: &KeyIndex,
    rolling: &RollingBlockhashes,
    slot: u64,
    message_bytes: &mut Vec<u8>,
    metadata_bytes: &mut Vec<u8>,
) -> Result<ArchiveV2HotTxRow> {
    let (payload, mut flags) = encode_message(&tx.message, key_index, rolling, slot, tx_index)?;
    flags |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
    if status.err.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
    }
    if status.return_data.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
    }
    if status.logs.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
    }
    if status.inner_instructions.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
    }
    if !status.pre_token_balances.is_empty() || !status.post_token_balances.is_empty() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    }
    if !status.loaded_writable.is_empty() || !status.loaded_readonly.is_empty() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
    }

    let message = wincode::config::serialize(&payload, wincode_leb128_config())
        .context("serialize transaction message")?;
    let metadata =
        wincode::config::serialize(&compact_meta(status, key_index), wincode_leb128_config())
            .context("serialize transaction metadata")?;
    let message_offset =
        u32::try_from(message_bytes.len()).context("message region exceeds u32")?;
    let message_len = u32::try_from(message.len()).context("message exceeds u32")?;
    let metadata_offset =
        u32::try_from(metadata_bytes.len()).context("metadata region exceeds u32")?;
    let metadata_len = u32::try_from(metadata.len()).context("metadata exceeds u32")?;
    message_bytes.extend(message);
    metadata_bytes.extend(metadata);
    let signature_count =
        u8::try_from(tx.signatures.len()).context("transaction has more than 255 signatures")?;

    Ok(ArchiveV2HotTxRow {
        tx_index: u32::try_from(tx_index).context("transaction index exceeds u32")?,
        flags,
        message_offset,
        message_len,
        metadata_offset,
        metadata_len,
        signature_count,
        reserved: [0; 3],
    })
}

fn encode_message(
    message: &SourceMessage,
    key_index: &KeyIndex,
    rolling: &RollingBlockhashes,
    slot: u64,
    tx_index: usize,
) -> Result<(ArchiveV2HotMessagePayload, u32)> {
    let recent_blockhash = match message {
        SourceMessage::Legacy(body) => body.recent_blockhash,
        SourceMessage::V0(body) => body.recent_blockhash,
        SourceMessage::V1(body) => body.recent_blockhash,
    };
    let recent_blockhash = rolling.resolve(&recent_blockhash, slot, tx_index)?;
    match message {
        SourceMessage::Legacy(body) => Ok((
            ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: compact_header(body),
                account_keys: compact_keys(&body.account_keys, key_index),
                recent_blockhash,
                instructions: raw_instructions(&body.instructions),
            }),
            0,
        )),
        SourceMessage::V0(body) => Ok((
            ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: compact_header(body),
                account_keys: compact_keys(&body.account_keys, key_index),
                recent_blockhash,
                instructions: raw_instructions(&body.instructions),
                address_table_lookups: body
                    .lookups
                    .iter()
                    .map(|lookup| OwnedCompactAddressTableLookup {
                        account_key: key_index.compact(&lookup.account_key),
                        writable_indexes: lookup.writable_indexes.clone(),
                        readonly_indexes: lookup.readonly_indexes.clone(),
                    })
                    .collect(),
            }),
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
        )),
        SourceMessage::V1(body) => Ok((
            ArchiveV2HotMessagePayload::V1(ArchiveV2HotV1Message {
                header: compact_header_v1(body),
                config: CompactTransactionConfig {
                    priority_fee: body.config.priority_fee,
                    compute_unit_limit: body.config.compute_unit_limit,
                    loaded_accounts_data_size_limit: body.config.loaded_accounts_data_size_limit,
                    heap_size: body.config.heap_size,
                },
                account_keys: compact_keys(&body.account_keys, key_index),
                recent_blockhash,
                instructions: raw_instructions(&body.instructions),
            }),
            0,
        )),
    }
}

fn compact_header(body: &SourceMessageBody) -> CompactMessageHeader {
    CompactMessageHeader {
        num_required_signatures: body.header.num_required_signatures,
        num_readonly_signed_accounts: body.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: body.header.num_readonly_unsigned_accounts,
    }
}

fn compact_header_v1(body: &SourceV1Message) -> CompactMessageHeader {
    CompactMessageHeader {
        num_required_signatures: body.header.num_required_signatures,
        num_readonly_signed_accounts: body.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: body.header.num_readonly_unsigned_accounts,
    }
}

fn compact_keys(keys: &[[u8; 32]], key_index: &KeyIndex) -> Vec<CompactPubkey> {
    keys.iter().map(|key| key_index.compact(key)).collect()
}

fn raw_instructions(
    instructions: &[crate::source::SourceInstruction],
) -> Vec<ArchiveV2HotInstruction> {
    instructions
        .iter()
        .map(|instruction| ArchiveV2HotInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.clone(),
            data: ArchiveV2HotInstructionData::Raw(instruction.data.clone()),
        })
        .collect()
}

fn compact_meta(status: &SourceStatus, key_index: &KeyIndex) -> CompactMetaV1 {
    CompactMetaV1 {
        err: status.err.clone(),
        fee: status.fee,
        pre_balances: status.pre_balances.clone(),
        post_balances: status.post_balances.clone(),
        inner_instructions: status
            .inner_instructions
            .as_ref()
            .map(|groups| groups.iter().map(compact_inner_group).collect()),
        logs: status
            .logs
            .as_ref()
            .map(|lines| parse_logs_with_compactor(lines, key_index)),
        pre_token_balances: status
            .pre_token_balances
            .iter()
            .map(|balance| compact_token_balance(balance, key_index))
            .collect(),
        post_token_balances: status
            .post_token_balances
            .iter()
            .map(|balance| compact_token_balance(balance, key_index))
            .collect(),
        rewards: status
            .rewards
            .iter()
            .map(|reward| compact_reward(reward, key_index))
            .collect(),
        loaded_writable_addresses: compact_keys(&status.loaded_writable, key_index),
        loaded_readonly_addresses: compact_keys(&status.loaded_readonly, key_index),
        return_data: status.return_data.as_ref().map(|data| CompactReturnData {
            program_id: key_index.compact(&data.program_id),
            data: data.data.clone(),
        }),
        compute_units_consumed: status.compute_units_consumed,
        cost_units: status.cost_units,
    }
}

fn compact_inner_group(group: &SourceInnerGroup) -> CompactInnerInstructions {
    CompactInnerInstructions {
        index: group.index,
        instructions: group
            .instructions
            .iter()
            .map(|instruction| CompactInnerInstruction {
                program_id_index: instruction.program_id_index,
                accounts: instruction.accounts.clone(),
                data: instruction.data.clone(),
                stack_height: instruction.stack_height,
            })
            .collect(),
    }
}

fn compact_token_balance(
    balance: &SourceTokenBalance,
    key_index: &KeyIndex,
) -> CompactTokenBalance {
    CompactTokenBalance {
        account_index: balance.account_index,
        mint: Some(key_index.compact(&balance.mint)),
        owner: balance.owner.map(|owner| key_index.compact(&owner)),
        program_id: balance
            .program_id
            .map(|program_id| key_index.compact(&program_id)),
        amount: balance.amount,
        decimals: balance.decimals,
    }
}

fn compact_reward(reward: &SourceReward, key_index: &KeyIndex) -> CompactReward {
    CompactReward {
        pubkey: key_index.compact(&reward.pubkey),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission,
    }
}

fn append_signatures(out: &mut impl Write, block: &SlotData) -> Result<u32> {
    let mut count = 0u32;
    for tx in &block.transactions {
        for signature in &tx.signatures {
            out.write_all(signature)
                .with_context(|| format!("write a signature for slot {}", block.slot))?;
            count = count
                .checked_add(1)
                .context("signature count exceeds u32")?;
        }
    }
    Ok(count)
}

fn poh_record(block: &SlotData, block_id: u32) -> WincodeArchiveV2PohRecord {
    WincodeArchiveV2PohRecord {
        block_id,
        slot: block.slot,
        entries: block
            .entries
            .iter()
            .map(|entry| CompactPohEntry {
                num_hashes: entry.num_hashes,
                hash: entry.hash,
                tx_count: entry.tx_count,
                signature_count: entry.signature_count,
            })
            .collect(),
    }
}

pub(crate) fn shredding_record(block: &SlotData, block_id: u32) -> WincodeArchiveV2ShreddingRecord {
    WincodeArchiveV2ShreddingRecord {
        block_id,
        slot: block.slot,
        shredding: block
            .shredding
            .iter()
            .map(|group| CompactShredding {
                entry_end_idx: group.entry_end_idx,
                shred_end_idx: group.shred_end_idx,
            })
            .collect(),
    }
}

fn empty_vote_hash_row() -> [u8; VOTE_HASH_ROW_LEN] {
    [0; VOTE_HASH_ROW_LEN]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::inspect;
    use crate::source::{
        LedgerView, REWARD_SLOT_DISTANCE, SourceHeader, SourceMessage, SourceMessageBody,
        SourceReward, SourceShredGroup, SourceStatus, SourceTx, test_block,
    };
    use blockzilla_archive_v2::deserialize_archive_v2_hot_block_blob;
    use blockzilla_primitives::WincodeLeb128FramedReader;
    use of_car_reader::blockmarker::parent_consensus_block_id;
    use std::io::Cursor;

    fn header_marker(parent_slot: u64, parent_id: [u8; 32]) -> Vec<u8> {
        let mut payload = vec![1u8];
        payload.extend_from_slice(&parent_slot.to_le_bytes());
        payload.extend_from_slice(&parent_id);
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(1);
        out.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
        out.extend(payload);
        out
    }

    #[test]
    fn empty_tower_tail_is_two_zero_bytes_and_has_no_consensus_id() {
        let block = test_block(10, 9, [2; 32], [1; 32]);
        let bytes = encode_hot_block(
            &block,
            0,
            &index_for(&block),
            &RollingBlockhashes::default(),
        )
        .unwrap();
        assert_eq!(&bytes[bytes.len() - 2..], &[0, 0]);
        let decoded = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        assert_eq!(decoded.consensus_block_id, None);
        assert!(decoded.block_markers.is_empty());
        assert_eq!(decoded.header.previous_blockhash_id, 0);
        assert_eq!(decoded.header.blockhash_id, 0);
    }

    #[test]
    fn header_marker_fills_the_consensus_id_and_parent_id() {
        let parent_id = [7; 32];
        let consensus_id = [9; 32];
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.consensus_block_id = Some(consensus_id);
        block.markers = vec![header_marker(9, parent_id)];

        let bytes = encode_hot_block(
            &block,
            0,
            &index_for(&block),
            &RollingBlockhashes::default(),
        )
        .unwrap();
        let decoded = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        assert_eq!(decoded.consensus_block_id, Some(consensus_id));
        assert_eq!(
            parent_consensus_block_id(&decoded.block_markers),
            Some(parent_id)
        );
    }

    #[test]
    fn two_shredding_groups_become_two_rows() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.shredding = vec![
            SourceShredGroup {
                entry_end_idx: 4,
                shred_end_idx: 31,
            },
            SourceShredGroup {
                entry_end_idx: 11,
                shred_end_idx: 95,
            },
        ];
        let record = shredding_record(&block, 0);
        assert_eq!(record.shredding.len(), 2);
        assert_eq!(record.shredding[0].entry_end_idx, 4);
        assert_eq!(record.shredding[0].shred_end_idx, 31);
        assert_eq!(record.shredding[1].entry_end_idx, 11);
        assert_eq!(record.shredding[1].shred_end_idx, 95);
    }

    #[test]
    fn a_parent_jump_skip_writes_no_blob() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let next = test_block(14, 10, [5; 32], [4; 32]);
        let view = LedgerView {
            start_slot: 10,
            end_slot: 14,
            slots: [first, next]
                .into_iter()
                .map(|slot| (slot.slot, slot))
                .collect(),
        };
        let inspected = inspect(&view).unwrap();
        assert_eq!(inspected.skips, vec![11, 12, 13]);
        let dir = tempfile::tempdir().unwrap();
        let mut loaded = Vec::new();
        write_archive(dir.path(), &inspected, ZSTD_LEVEL, |slot| {
            loaded.push(slot);
            view.slots
                .get(&slot)
                .cloned()
                .with_context(|| format!("test block {slot} is missing"))
        })
        .unwrap();
        assert_eq!(loaded, vec![10, 14, 10, 14]);
        let index = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        let slots: Vec<u64> = index[36..]
            .chunks_exact(52)
            .map(|row| u64::from_le_bytes(row[4..12].try_into().unwrap()))
            .collect();
        assert_eq!(slots, vec![10, 14]);
    }

    #[test]
    fn a_proven_skip_writes_no_blob() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let second = test_block(12, 10, [5; 32], [4; 32]);
        let mut carrier = test_block(11 + REWARD_SLOT_DISTANCE, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_footer(11)];
        let view = LedgerView {
            start_slot: 10,
            end_slot: 12,
            slots: [first, second, carrier]
                .into_iter()
                .map(|slot| (slot.slot, slot))
                .collect(),
        };
        let inspected = inspect(&view).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut loaded = Vec::new();
        write_archive(dir.path(), &inspected, ZSTD_LEVEL, |slot| {
            loaded.push(slot);
            view.slots
                .get(&slot)
                .cloned()
                .with_context(|| format!("test block {slot} is missing"))
        })
        .unwrap();
        assert_eq!(loaded, vec![10, 12, 10, 12]);

        let index = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(&index[..8], b"BZV2HIX1");
        assert_eq!(u64::from_le_bytes(index[12..20].try_into().unwrap()), 2);
        assert_eq!(
            i32::from_le_bytes(index[28..32].try_into().unwrap()),
            ZSTD_LEVEL
        );
        assert_eq!(u32::from_le_bytes(index[32..36].try_into().unwrap()), 0);
        let row_len = 52;
        let slots: Vec<u64> = index[36..]
            .chunks_exact(row_len)
            .map(|row| u64::from_le_bytes(row[4..12].try_into().unwrap()))
            .collect();
        assert_eq!(slots, vec![10, 12]);

        let hashes = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)).unwrap();
        assert_eq!(hashes.len(), 64);
        assert_eq!(&hashes[..32], &[4; 32]);
        assert_eq!(&hashes[32..], &[5; 32]);
        let tail = std::fs::read(dir.path().join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)).unwrap();
        assert_eq!(tail.len(), 40);
        assert_eq!(&tail[..32], &[3; 32]);
        assert_eq!(u64::from_le_bytes(tail[32..40].try_into().unwrap()), 9);

        let votes = std::fs::read(dir.path().join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE)).unwrap();
        assert_eq!(votes, vec![0u8; 65 * 2]);

        let registry = std::fs::read(dir.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)).unwrap();
        assert_eq!(registry.as_slice(), compute_budget_program_id().as_slice());

        let mut reader = WincodeLeb128FramedReader::new(Cursor::new(
            std::fs::read(dir.path().join(ARCHIVE_V2_SHREDDING_FILE)).unwrap(),
        ));
        let first_record = reader
            .read::<WincodeArchiveV2ShreddingRecord>()
            .unwrap()
            .unwrap()
            .1;
        let second_record = reader
            .read::<WincodeArchiveV2ShreddingRecord>()
            .unwrap()
            .unwrap()
            .1;
        assert!(
            reader
                .read::<WincodeArchiveV2ShreddingRecord>()
                .unwrap()
                .is_none()
        );
        assert_eq!(first_record.slot, 10);
        assert_eq!(second_record.slot, 12);
    }

    #[test]
    fn a_block_load_failure_removes_temp_files_and_skips_the_index() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let second = test_block(11, 10, [5; 32], [4; 32]);
        let view = LedgerView {
            start_slot: 10,
            end_slot: 11,
            slots: [first, second]
                .into_iter()
                .map(|slot| (slot.slot, slot))
                .collect(),
        };
        let inspected = inspect(&view).unwrap();
        let dir = tempfile::tempdir().unwrap();

        let error = write_archive(dir.path(), &inspected, ZSTD_LEVEL, |slot| {
            if slot == 11 {
                bail!("slot 11 is missing transaction status");
            }
            view.slots
                .get(&slot)
                .cloned()
                .with_context(|| format!("test block {slot} is missing"))
        })
        .unwrap_err();

        assert!(
            error.to_string().contains("missing transaction status"),
            "{error:#}"
        );
        assert!(!dir.path().join(ARCHIVE_V2_BLOCK_INDEX_FILE).exists());
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(names.is_empty(), "{names:?}");
    }

    #[test]
    fn repeated_account_key_gets_the_lowest_counted_id() {
        let repeated = [4u8; 32];
        let once = [9u8; 32];
        let block = block_with_keys(vec![vec![repeated, repeated], vec![once]]);
        let keys = registry_keys_for(&block);
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], compute_budget_program_id());
        assert_eq!(keys[1], repeated);
        assert_eq!(keys[2], once);
        assert_eq!(keys.len() * 32, 96);

        let (message, _) = encode_message(
            &block.transactions[0].message,
            &index_for(&block),
            &RollingBlockhashes::default(),
            block.slot,
            0,
        )
        .unwrap();
        let ArchiveV2HotMessagePayload::Legacy(message) = message else {
            panic!("expected a legacy message");
        };
        assert_eq!(message.account_keys[0], CompactPubkey::id(2));
        assert_eq!(message.account_keys[1], CompactPubkey::id(2));
        let (other, _) = encode_message(
            &block.transactions[1].message,
            &index_for(&block),
            &RollingBlockhashes::default(),
            block.slot,
            1,
        )
        .unwrap();
        let ArchiveV2HotMessagePayload::Legacy(other) = other else {
            panic!("expected a legacy message");
        };
        assert_eq!(other.account_keys[0], CompactPubkey::id(3));
    }

    #[test]
    fn a_seen_compute_budget_key_is_not_inserted_again() {
        let compute_budget = compute_budget_program_id();
        let other = [5u8; 32];
        let block = block_with_keys(vec![vec![compute_budget, compute_budget], vec![other]]);
        let keys = registry_keys_for(&block);
        assert_eq!(keys, vec![compute_budget, other]);
    }

    #[test]
    fn a_block_reward_absent_from_transactions_stays_raw() {
        let reward_key = [6u8; 32];
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.rewards.push(SourceReward {
            pubkey: reward_key,
            lamports: 1,
            post_balance: 2,
            reward_type: 1,
            commission: None,
        });
        let keys = registry_keys_for(&block);
        assert_eq!(keys, vec![compute_budget_program_id()]);
        let decoded = deserialize_archive_v2_hot_block_blob(
            &encode_hot_block(
                &block,
                0,
                &index_for(&block),
                &RollingBlockhashes::default(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            decoded.header.rewards.unwrap().decoded[0].pubkey,
            CompactPubkey::raw(reward_key)
        );
    }

    #[test]
    fn a_block_reward_seen_in_a_transaction_uses_its_id() {
        let reward_key = [6u8; 32];
        let mut block = block_with_keys(vec![vec![reward_key]]);
        block.rewards.push(SourceReward {
            pubkey: reward_key,
            lamports: 1,
            post_balance: 2,
            reward_type: 1,
            commission: None,
        });
        let decoded = deserialize_archive_v2_hot_block_blob(
            &encode_hot_block(
                &block,
                0,
                &index_for(&block),
                &RollingBlockhashes::default(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            decoded.header.rewards.unwrap().decoded[0].pubkey,
            CompactPubkey::id(2)
        );
    }

    #[test]
    fn a_recent_blockhash_from_the_previous_block_uses_its_id() {
        let mut rolling = RollingBlockhashes::default();
        rolling.insert([4; 32], 0, 10).unwrap();
        let mut block = block_with_keys(vec![vec![[1; 32]]]);
        block.slot = 11;
        block.parent_slot = Some(10);
        set_recent_blockhash(&mut block, [4; 32]);
        let bytes = encode_hot_block(&block, 1, &index_for(&block), &rolling).unwrap();
        let decoded = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        assert_eq!(decoded.header.blockhash_id, 1);
        assert_eq!(decoded.header.previous_blockhash_id, 0);
        assert_recent_id(&bytes, 0);
    }

    #[test]
    fn an_unknown_recent_blockhash_stays_raw() {
        let block = block_with_keys(vec![vec![[1; 32]]]);
        let bytes = encode_hot_block(
            &block,
            0,
            &index_for(&block),
            &RollingBlockhashes::default(),
        )
        .unwrap();
        assert_recent_nonce(&bytes, [8; 32]);
    }

    #[test]
    fn a_blockhash_older_than_the_window_stays_raw() {
        let mut rolling = RollingBlockhashes::default();
        rolling.insert([8; 32], 0, 10).unwrap();
        let mut block = block_with_keys(vec![vec![[1; 32]]]);
        block.slot = 160;
        block.parent_slot = Some(159);
        let bytes = encode_hot_block(&block, 1, &index_for(&block), &rolling).unwrap();
        assert_recent_id(&bytes, 0);

        block.slot = 161;
        block.parent_slot = Some(160);
        let bytes = encode_hot_block(&block, 1, &index_for(&block), &rolling).unwrap();
        assert_recent_nonce(&bytes, [8; 32]);
    }

    #[test]
    fn a_parent_hash_before_the_window_uses_a_negative_id() {
        let mut block = block_with_keys(vec![vec![[1; 32]]]);
        set_recent_blockhash(&mut block, [1; 32]);
        let view = LedgerView {
            start_slot: 10,
            end_slot: 10,
            slots: [(block.slot, block.clone())].into_iter().collect(),
        };
        let inspected = inspect(&view).unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_archive(dir.path(), &inspected, ZSTD_LEVEL, |_| Ok(block.clone())).unwrap();

        let tail = std::fs::read(dir.path().join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)).unwrap();
        assert_eq!(tail.len(), 40);
        assert_eq!(&tail[..32], &[1; 32]);
        assert_eq!(u64::from_le_bytes(tail[32..40].try_into().unwrap()), 9);
        let hashes = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)).unwrap();
        assert_eq!(hashes.as_slice(), &[2; 32]);
        let compressed = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let raw = zstd::bulk::decompress(&compressed, 1_000_000).unwrap();
        assert_recent_id(&raw, -1);
    }

    #[test]
    fn a_duplicate_blockhash_stops_the_archive() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let second = test_block(11, 10, [4; 32], [4; 32]);
        let view = LedgerView {
            start_slot: 10,
            end_slot: 11,
            slots: [(10, first.clone()), (11, second.clone())]
                .into_iter()
                .collect(),
        };
        let inspected = inspect(&view).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let error = write_archive(dir.path(), &inspected, ZSTD_LEVEL, |slot| {
            Ok(if slot == 10 {
                first.clone()
            } else {
                second.clone()
            })
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("duplicate blockhash"),
            "{error:#}"
        );
    }

    #[test]
    fn a_recent_blockhash_from_a_future_slot_is_an_error() {
        let mut rolling = RollingBlockhashes::default();
        rolling.insert([8; 32], 1, 20).unwrap();
        let block = block_with_keys(vec![vec![[1; 32]]]);
        let error = encode_hot_block(&block, 0, &index_for(&block), &rolling).unwrap_err();
        assert!(error.to_string().contains("future block slot"), "{error:#}");
    }

    fn registry_keys_for(block: &SlotData) -> Vec<[u8; 32]> {
        let mut counts = PubkeyCounts::new();
        count_slot_pubkeys(block, &mut counts);
        ordered_registry_keys(counts)
    }

    fn index_for(block: &SlotData) -> KeyIndex {
        KeyIndex::build_from_slice_low_memory(&registry_keys_for(block))
    }

    fn block_with_keys(key_groups: Vec<Vec<[u8; 32]>>) -> SlotData {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.transactions = key_groups
            .into_iter()
            .map(|account_keys| SourceTx {
                signatures: vec![[7; 64]],
                message: SourceMessage::Legacy(SourceMessageBody {
                    header: SourceHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys,
                    recent_blockhash: [8; 32],
                    instructions: Vec::new(),
                    lookups: Vec::new(),
                }),
                status: Some(SourceStatus::success()),
            })
            .collect();
        block.entries[0].tx_count = u32::try_from(block.transactions.len()).unwrap();
        block.entries[0].signature_count = block.entries[0].tx_count;
        block
    }

    fn set_recent_blockhash(block: &mut SlotData, hash: [u8; 32]) {
        let SourceMessage::Legacy(body) = &mut block.transactions[0].message else {
            panic!("expected a legacy message");
        };
        body.recent_blockhash = hash;
    }

    #[test]
    fn the_requested_zstd_level_is_stored_in_the_index() {
        let block = test_block(10, 9, [2; 32], [1; 32]);
        let view = LedgerView {
            start_slot: 10,
            end_slot: 10,
            slots: [(block.slot, block.clone())].into_iter().collect(),
        };
        let inspected = inspect(&view).unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_archive(dir.path(), &inspected, 3, |_| Ok(block.clone())).unwrap();
        let index = std::fs::read(dir.path().join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(i32::from_le_bytes(index[28..32].try_into().unwrap()), 3);

        let error = write_archive(dir.path(), &inspected, 10_000, |_| Ok(block.clone())).unwrap_err();
        assert!(error.to_string().contains("zstd level"));
    }

    fn assert_recent_id(bytes: &[u8], id: i32) {
        match recent_blockhash_of(bytes) {
            OwnedCompactRecentBlockhash::Id(found) => assert_eq!(found, id),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("expected recent blockhash id {id}"),
        }
    }

    fn assert_recent_nonce(bytes: &[u8], hash: [u8; 32]) {
        match recent_blockhash_of(bytes) {
            OwnedCompactRecentBlockhash::Nonce(found) => assert_eq!(found, hash),
            OwnedCompactRecentBlockhash::Id(id) => {
                panic!("expected a raw recent blockhash, found id {id}")
            }
        }
    }

    fn recent_blockhash_of(bytes: &[u8]) -> OwnedCompactRecentBlockhash {
        let decoded = deserialize_archive_v2_hot_block_blob(bytes).unwrap();
        let row = &decoded.tx_rows[0];
        let start = row.message_offset as usize;
        let end = start + row.message_len as usize;
        let message: ArchiveV2HotMessagePayload = wincode::config::deserialize(
            &decoded.message_bytes[start..end],
            wincode_leb128_config(),
        )
        .unwrap();
        match message {
            ArchiveV2HotMessagePayload::Legacy(message) => message.recent_blockhash,
            ArchiveV2HotMessagePayload::V0(message) => message.recent_blockhash,
            ArchiveV2HotMessagePayload::V1(message) => message.recent_blockhash,
        }
    }

    fn skip_footer(skipped_slot: u64) -> Vec<u8> {
        let mut certificates = vec![0, 1];
        certificates.extend_from_slice(&skipped_slot.to_le_bytes());
        certificates.extend(std::iter::repeat_n(7u8, 96));
        certificates.push(0);
        certificates.push(0);
        let mut payload = vec![1u8];
        payload.extend(std::iter::repeat_n(0xabu8, 32));
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.push(0);
        payload.extend(certificates);
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(0);
        out.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
        out.extend(payload);
        out
    }
}
