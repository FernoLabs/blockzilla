use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_format::{
    HistoricalArchiveRecord, HistoricalBlockRecord, HistoricalTransactionRecord, LosslessV2Header,
    Pass1BlockIndexHeader, Pass1BlockIndexRecord, PostcardFramedReader, PostcardFramedWriter,
    ReconstructionArchiveRecord, ReconstructionNodeKind, ReconstructionNodeRecord,
    RuntimeArchiveRecord, RuntimeBlockRecord, RuntimeTransactionRecord, write_registry,
};
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::{
    CarBlockReader,
    reconstruct::{
        Cid36, NodeLocation, RawBlockNode, RawEntryNode, RawEpochNode, RawNode, RawRewardsNode,
        RawSubsetNode, RawTransactionNode, StandaloneDataFrame,
    },
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use solana_pubkey::{Pubkey, pubkey};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::{BUFFER_SIZE, ProgressTracker};

const HISTORICAL_FILE: &str = "historical.bin.zst";
const METADATA_FILE: &str = "metadata-unoptimized.bin.zst";
const RECONSTRUCT_FILE: &str = "reconstruct.bin.zst";
const BLOCK_INDEX_FILE: &str = "block-index.bin";
const REGISTRY_FILE: &str = "registry.bin";
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
const PASS1_BLOCK_INDEX_VERSION: u32 = 1;
const ZSTD_LEVEL: i32 = 3;

pub(crate) fn build(input: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let historical_path = output_dir.join(HISTORICAL_FILE);
    let metadata_path = output_dir.join(METADATA_FILE);
    let reconstruct_path = output_dir.join(RECONSTRUCT_FILE);
    let block_index_path = output_dir.join(BLOCK_INDEX_FILE);
    let registry_path = output_dir.join(REGISTRY_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);

    let input = open_car_input(input)?;
    let mut reader = CarBlockReader::with_capacity(input, BUFFER_SIZE);
    let encoded_car_header = reader.read_header_bytes()?;

    let historical_file = File::create(&historical_path)
        .with_context(|| format!("create {}", historical_path.display()))?;
    let historical_file = BufWriter::with_capacity(BUFFER_SIZE, historical_file);
    let historical_file = zstd::Encoder::new(historical_file, ZSTD_LEVEL)
        .with_context(|| format!("create zstd encoder {}", historical_path.display()))?
        .auto_finish();

    let metadata_file = File::create(&metadata_path)
        .with_context(|| format!("create {}", metadata_path.display()))?;
    let metadata_file = BufWriter::with_capacity(BUFFER_SIZE, metadata_file);
    let metadata_file = zstd::Encoder::new(metadata_file, ZSTD_LEVEL)
        .with_context(|| format!("create zstd encoder {}", metadata_path.display()))?
        .auto_finish();

    let reconstruct_file = File::create(&reconstruct_path)
        .with_context(|| format!("create {}", reconstruct_path.display()))?;
    let reconstruct_file = BufWriter::with_capacity(BUFFER_SIZE, reconstruct_file);
    let reconstruct_file = zstd::Encoder::new(reconstruct_file, ZSTD_LEVEL)
        .with_context(|| format!("create zstd encoder {}", reconstruct_path.display()))?
        .auto_finish();

    let block_index_file = File::create(&block_index_path)
        .with_context(|| format!("create {}", block_index_path.display()))?;
    let block_index_file = BufWriter::with_capacity(BUFFER_SIZE, block_index_file);

    let mut historical = PostcardFramedWriter::new(historical_file);
    let mut metadata = PostcardFramedWriter::new(metadata_file);
    let mut reconstruct = PostcardFramedWriter::new(reconstruct_file);
    let mut block_index = PostcardFramedWriter::new(block_index_file);

    reconstruct.write(&ReconstructionArchiveRecord::Header(LosslessV2Header {
        encoded_car_header,
    }))?;
    block_index.write(&Pass1BlockIndexHeader {
        version: PASS1_BLOCK_INDEX_VERSION,
    })?;

    let mut pending = PendingBlock::default();
    let mut scratch = Vec::new();
    let mut historical_records = 0u64;
    let mut metadata_records = 0u64;
    let mut reconstruct_records = 0u64;
    let mut blockhashes = Vec::new();
    let mut counter = PubkeyCounter::new(50_000_000);
    let mut progress = ProgressTracker::new("Pass1");

    while let Some(node) = reader.read_lossless_node_with_scratch(&mut scratch)? {
        let location = node.location();
        let node_kind = raw_node_kind_name(&node);

        if matches!(
            node,
            RawNode::Transaction(_)
                | RawNode::Entry(_)
                | RawNode::Rewards(_)
                | RawNode::Block(_)
                | RawNode::DataFrame(_)
        ) {
            pending.start_if_empty(location, reconstruct_records);
        } else {
            ensure!(
                pending.is_empty(),
                "encountered {node_kind} while a block group was still pending"
            );
        }

        reconstruct.write(&ReconstructionArchiveRecord::Node(
            ReconstructionNodeRecord::from_raw(&node),
        ))?;
        reconstruct_records += 1;

        match node {
            RawNode::Transaction(tx) => {
                insert_unique(&mut pending.tx_by_cid, tx.cid, tx, "pending transaction")?
            }
            RawNode::Entry(entry) => {
                insert_unique(&mut pending.entry_by_cid, entry.cid, entry, "pending entry")?
            }
            RawNode::Rewards(rewards) => insert_unique(
                &mut pending.rewards_by_cid,
                rewards.cid,
                rewards,
                "pending rewards",
            )?,
            RawNode::DataFrame(frame) => insert_unique(
                &mut pending.dataframes,
                frame.cid,
                frame,
                "pending dataframe",
            )?,
            RawNode::Block(block) => {
                let historical_start = historical_records;
                let metadata_start = metadata_records;
                let finalized = pending.finalize(block)?;

                for tx in &finalized.transactions {
                    count_transaction_pubkeys(tx, &finalized.dataframes, &mut counter)?;
                    historical.write(&HistoricalArchiveRecord::Transaction(
                        HistoricalTransactionRecord::from_raw(tx),
                    ))?;
                    historical_records += 1;

                    metadata.write(&RuntimeArchiveRecord::Transaction(
                        RuntimeTransactionRecord::from_raw(tx),
                    ))?;
                    metadata_records += 1;
                }

                for entry in &finalized.entries {
                    historical.write(&HistoricalArchiveRecord::Entry(entry.clone()))?;
                    historical_records += 1;
                }

                let mut ordered_dataframes: Vec<_> =
                    finalized.dataframes.values().cloned().collect();
                ordered_dataframes
                    .sort_by_key(|frame| (frame.location.entry_index, frame.location.car_offset));
                for frame in &ordered_dataframes {
                    historical.write(&HistoricalArchiveRecord::DataFrame(frame.clone()))?;
                    historical_records += 1;
                }

                historical.write(&HistoricalArchiveRecord::Block(
                    HistoricalBlockRecord::from_raw(&finalized.block),
                ))?;
                historical_records += 1;

                if let Some(rewards) = &finalized.rewards {
                    metadata.write(&RuntimeArchiveRecord::Rewards(rewards.clone()))?;
                    metadata_records += 1;
                }

                metadata.write(&RuntimeArchiveRecord::Block(RuntimeBlockRecord::from_raw(
                    &finalized.block,
                )))?;
                metadata_records += 1;

                blockhashes.push(finalized.blockhash);
                progress.update_slot(finalized.block.slot);
                progress.update(1, finalized.transactions.len() as u64);

                let reconstruct_start = finalized.reconstruct_record_start;
                block_index.write(&Pass1BlockIndexRecord {
                    slot: finalized.block.slot,
                    block_cid: finalized.block.cid,
                    blockhash: finalized.blockhash,
                    first_car_entry_index: finalized.first_location.entry_index,
                    first_car_offset: finalized.first_location.car_offset,
                    block_car_entry_index: finalized.block.location.entry_index,
                    block_car_offset: finalized.block.location.car_offset,
                    reconstruct_record_start: reconstruct_start,
                    reconstruct_record_len: reconstruct_records - reconstruct_start,
                    historical_record_start: historical_start,
                    historical_record_len: historical_records - historical_start,
                    metadata_record_start: metadata_start,
                    metadata_record_len: metadata_records - metadata_start,
                    entry_count: finalized.entries.len() as u64,
                    transaction_count: finalized.transactions.len() as u64,
                    dataframe_count: ordered_dataframes.len() as u64,
                    has_rewards: finalized.rewards.is_some() || finalized.block.rewards.is_some(),
                })?;
            }
            RawNode::Subset(subset) => {
                historical.write(&HistoricalArchiveRecord::Subset(subset))?;
                historical_records += 1;
            }
            RawNode::Epoch(epoch) => {
                historical.write(&HistoricalArchiveRecord::Epoch(epoch))?;
                historical_records += 1;
            }
        }
    }

    ensure!(
        pending.is_empty(),
        "unterminated block group at EOF: {}",
        pending.summary()
    );

    progress.final_report();
    write_registry_and_blockhash(
        &registry_path,
        &blockhash_registry_path,
        counter,
        blockhashes,
    )?;

    historical.flush()?;
    metadata.flush()?;
    reconstruct.flush()?;
    block_index.flush()?;
    Ok(())
}

pub(crate) fn extract(input_dir: &Path, output: &Path) -> Result<()> {
    let historical_path = input_dir.join(HISTORICAL_FILE);
    let metadata_path = input_dir.join(METADATA_FILE);
    let reconstruct_path = input_dir.join(RECONSTRUCT_FILE);

    let mut historical = load_historical(open_zstd_input(&historical_path)?)?;
    let mut runtime = load_runtime(open_zstd_input(&metadata_path)?)?;
    let mut reconstruct = PostcardFramedReader::new(open_zstd_input(&reconstruct_path)?);

    let output_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut output_file = BufWriter::with_capacity(BUFFER_SIZE, output_file);

    let Some(ReconstructionArchiveRecord::Header(header)) =
        reconstruct.read::<ReconstructionArchiveRecord>()?
    else {
        anyhow::bail!(
            "reconstruction file {} did not start with a header record",
            reconstruct_path.display()
        );
    };

    output_file
        .write_all(&header.encoded_car_header)
        .with_context(|| format!("write CAR header to {}", output.display()))?;

    while let Some(record) = reconstruct.read::<ReconstructionArchiveRecord>()? {
        let ReconstructionArchiveRecord::Node(record) = record else {
            anyhow::bail!(
                "unexpected extra reconstruction header in {}",
                reconstruct_path.display()
            );
        };

        let node = match record.kind {
            ReconstructionNodeKind::Transaction => {
                let tx = historical
                    .transactions
                    .remove(&record.cid)
                    .with_context(|| format!("missing historical transaction {}", record.cid))?;
                let meta = runtime
                    .transactions
                    .remove(&record.cid)
                    .with_context(|| format!("missing runtime transaction {}", record.cid))?;
                ensure_matching_location_and_cid(
                    "transaction",
                    &record,
                    tx.location,
                    tx.cid,
                    meta.location,
                    meta.cid,
                )?;
                RawNode::Transaction(tx.with_runtime(meta))
            }
            ReconstructionNodeKind::Entry => {
                let entry = historical
                    .entries
                    .remove(&record.cid)
                    .with_context(|| format!("missing entry {}", record.cid))?;
                ensure_matching_node("entry", &record, entry.location, entry.cid)?;
                RawNode::Entry(entry)
            }
            ReconstructionNodeKind::Rewards => {
                let rewards = runtime
                    .rewards
                    .remove(&record.cid)
                    .with_context(|| format!("missing rewards {}", record.cid))?;
                ensure_matching_node("rewards", &record, rewards.location, rewards.cid)?;
                RawNode::Rewards(rewards)
            }
            ReconstructionNodeKind::Block => {
                let block = historical
                    .blocks
                    .remove(&record.cid)
                    .with_context(|| format!("missing historical block {}", record.cid))?;
                let runtime_block = runtime
                    .blocks
                    .remove(&record.cid)
                    .with_context(|| format!("missing runtime block {}", record.cid))?;
                ensure_matching_location_and_cid(
                    "block",
                    &record,
                    block.location,
                    block.cid,
                    runtime_block.location,
                    runtime_block.cid,
                )?;
                RawNode::Block(block.with_runtime(runtime_block))
            }
            ReconstructionNodeKind::Subset => {
                let subset = historical
                    .subsets
                    .remove(&record.cid)
                    .with_context(|| format!("missing subset {}", record.cid))?;
                ensure_matching_node("subset", &record, subset.location, subset.cid)?;
                RawNode::Subset(subset)
            }
            ReconstructionNodeKind::Epoch => {
                let epoch = historical
                    .epochs
                    .remove(&record.cid)
                    .with_context(|| format!("missing epoch {}", record.cid))?;
                ensure_matching_node("epoch", &record, epoch.location, epoch.cid)?;
                RawNode::Epoch(epoch)
            }
            ReconstructionNodeKind::DataFrame => {
                let frame = historical
                    .dataframes
                    .remove(&record.cid)
                    .with_context(|| format!("missing dataframe {}", record.cid))?;
                ensure_matching_node("dataframe", &record, frame.location, frame.cid)?;
                RawNode::DataFrame(frame)
            }
        };

        node.validate_cid()?;
        let payload = node.encode_payload();
        write_car_entry(&mut output_file, node.cid(), &payload)?;
    }

    output_file.flush().context("flush reconstructed car")?;

    historical.ensure_empty()?;
    runtime.ensure_empty()?;
    Ok(())
}

#[derive(Default)]
struct PendingBlock {
    first_location: Option<NodeLocation>,
    reconstruct_record_start: Option<u64>,
    tx_by_cid: HashMap<Cid36, RawTransactionNode>,
    entry_by_cid: HashMap<Cid36, RawEntryNode>,
    rewards_by_cid: HashMap<Cid36, RawRewardsNode>,
    dataframes: HashMap<Cid36, StandaloneDataFrame>,
}

impl PendingBlock {
    fn is_empty(&self) -> bool {
        self.tx_by_cid.is_empty()
            && self.entry_by_cid.is_empty()
            && self.rewards_by_cid.is_empty()
            && self.dataframes.is_empty()
            && self.first_location.is_none()
            && self.reconstruct_record_start.is_none()
    }

    fn start_if_empty(&mut self, location: NodeLocation, reconstruct_record_start: u64) {
        if self.first_location.is_none() {
            self.first_location = Some(location);
            self.reconstruct_record_start = Some(reconstruct_record_start);
        }
    }

    fn clear(&mut self) {
        self.first_location = None;
        self.reconstruct_record_start = None;
        self.tx_by_cid.clear();
        self.entry_by_cid.clear();
        self.rewards_by_cid.clear();
        self.dataframes.clear();
    }

    fn summary(&self) -> String {
        format!(
            "txs={} entries={} rewards={} dataframes={}",
            self.tx_by_cid.len(),
            self.entry_by_cid.len(),
            self.rewards_by_cid.len(),
            self.dataframes.len()
        )
    }

    fn finalize(&mut self, block: RawBlockNode) -> Result<FinalizedBlock> {
        let first_location = self
            .first_location
            .context("pending block group was missing its first location")?;
        let reconstruct_record_start = self
            .reconstruct_record_start
            .context("pending block group was missing its reconstruct start")?;

        let mut entries = Vec::with_capacity(block.entries.len());
        let mut transactions = Vec::new();

        for entry_ref in &block.entries {
            let entry_cid = entry_ref
                .cid
                .with_context(|| format!("block {} had a non-CAR entry reference", block.cid))?;
            let entry = self
                .entry_by_cid
                .get(&entry_cid)
                .cloned()
                .with_context(|| format!("missing entry {} for block {}", entry_cid, block.cid))?;

            for tx_ref in &entry.transactions {
                let tx_cid = tx_ref.cid.with_context(|| {
                    format!("entry {} had a non-CAR transaction reference", entry.cid)
                })?;
                let tx = self.tx_by_cid.get(&tx_cid).cloned().with_context(|| {
                    format!("missing transaction {} for entry {}", tx_cid, entry.cid)
                })?;
                transactions.push(tx);
            }

            entries.push(entry);
        }

        let rewards = match block.rewards.as_ref().and_then(|value| value.cid) {
            Some(rewards_cid) => Some(
                self.rewards_by_cid
                    .get(&rewards_cid)
                    .cloned()
                    .with_context(|| {
                        format!("missing rewards {} for block {}", rewards_cid, block.cid)
                    })?,
            ),
            None => None,
        };

        let blockhash = entries.last().map(|entry| entry.hash).unwrap_or([0u8; 32]);
        let dataframes = self.dataframes.clone();
        self.clear();

        Ok(FinalizedBlock {
            first_location,
            reconstruct_record_start,
            block,
            entries,
            transactions,
            rewards,
            dataframes,
            blockhash,
        })
    }
}

struct FinalizedBlock {
    first_location: NodeLocation,
    reconstruct_record_start: u64,
    block: RawBlockNode,
    entries: Vec<RawEntryNode>,
    transactions: Vec<RawTransactionNode>,
    rewards: Option<RawRewardsNode>,
    dataframes: HashMap<Cid36, StandaloneDataFrame>,
    blockhash: [u8; 32],
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

fn count_transaction_pubkeys(
    tx: &RawTransactionNode,
    dataframes: &HashMap<Cid36, StandaloneDataFrame>,
    counter: &mut PubkeyCounter,
) -> Result<()> {
    let tx_bytes = tx
        .transaction_bytes(dataframes)
        .with_context(|| format!("reassemble tx {}", tx.cid))?;
    let vtx = wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes)
        .map_err(|err| anyhow!("decode transaction {}: {}", tx.cid, err))?;

    match &vtx.message {
        VersionedMessage::Legacy(message) => {
            for key in &message.account_keys {
                counter.add32(key);
            }
        }
        VersionedMessage::V0(message) => {
            for key in &message.account_keys {
                counter.add32(key);
            }
            for lookup in &message.address_table_lookups {
                counter.add32(lookup.account_key);
            }
        }
    }

    Ok(())
}

fn write_registry_and_blockhash(
    registry_path: &Path,
    blockhash_registry_path: &Path,
    counter: PubkeyCounter,
    blockhashes: Vec<[u8; 32]>,
) -> Result<()> {
    {
        let mut f = File::create(blockhash_registry_path)
            .with_context(|| format!("create {}", blockhash_registry_path.display()))?;
        for blockhash in &blockhashes {
            f.write_all(blockhash)
                .with_context(|| format!("write {}", blockhash_registry_path.display()))?;
        }
        f.flush()
            .with_context(|| format!("flush {}", blockhash_registry_path.display()))?;
    }

    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    let mut keys: Vec<[u8; 32]> = items.into_iter().map(|(key, _)| key).collect();

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] =
        &[pubkey!("ComputeBudget111111111111111111111111111111")];

    for builtin in BUILTIN_PROGRAM_KEYS {
        let builtin = builtin.to_bytes();
        if !keys.iter().any(|value| value == &builtin) {
            keys.insert(0, builtin);
        }
    }

    write_registry(registry_path, &keys)
        .with_context(|| format!("write {}", registry_path.display()))
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

fn open_zstd_input(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let file = BufReader::with_capacity(BUFFER_SIZE, file);
    let decoder = zstd::Decoder::with_buffer(file)
        .with_context(|| format!("init zstd decoder for {}", path.display()))?;
    Ok(Box::new(decoder))
}

fn write_car_entry<W: Write>(writer: &mut W, cid: Cid36, payload: &[u8]) -> Result<()> {
    write_uvarint(writer, (cid.car_bytes().len() + payload.len()) as u64)?;
    writer.write_all(cid.car_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}

fn write_uvarint<W: Write>(writer: &mut W, mut value: u64) -> Result<()> {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            return Ok(());
        }
    }
}

fn ensure_matching_node(
    kind: &str,
    record: &ReconstructionNodeRecord,
    location: NodeLocation,
    cid: Cid36,
) -> Result<()> {
    ensure!(
        record.location == location,
        "{kind} location mismatch: reconstruct={:?} stored={:?}",
        record.location,
        location
    );
    ensure!(
        record.cid == cid,
        "{kind} cid mismatch: reconstruct={} stored={}",
        record.cid,
        cid
    );
    Ok(())
}

fn ensure_matching_location_and_cid(
    kind: &str,
    record: &ReconstructionNodeRecord,
    lhs_location: NodeLocation,
    lhs_cid: Cid36,
    rhs_location: NodeLocation,
    rhs_cid: Cid36,
) -> Result<()> {
    ensure_matching_node(kind, record, lhs_location, lhs_cid)?;
    ensure!(
        lhs_location == rhs_location,
        "{kind} split location mismatch: left={lhs_location:?} right={rhs_location:?}"
    );
    ensure!(
        lhs_cid == rhs_cid,
        "{kind} split cid mismatch: left={} right={}",
        lhs_cid,
        rhs_cid
    );
    Ok(())
}

#[derive(Default)]
struct HistoricalMaps {
    transactions: HashMap<Cid36, HistoricalTransactionRecord>,
    entries: HashMap<Cid36, RawEntryNode>,
    blocks: HashMap<Cid36, HistoricalBlockRecord>,
    dataframes: HashMap<Cid36, StandaloneDataFrame>,
    subsets: HashMap<Cid36, RawSubsetNode>,
    epochs: HashMap<Cid36, RawEpochNode>,
}

impl HistoricalMaps {
    fn ensure_empty(self) -> Result<()> {
        ensure!(
            self.transactions.is_empty()
                && self.entries.is_empty()
                && self.blocks.is_empty()
                && self.dataframes.is_empty()
                && self.subsets.is_empty()
                && self.epochs.is_empty(),
            "unused historical records remained after extraction"
        );
        Ok(())
    }
}

#[derive(Default)]
struct RuntimeMaps {
    transactions: HashMap<Cid36, RuntimeTransactionRecord>,
    rewards: HashMap<Cid36, RawRewardsNode>,
    blocks: HashMap<Cid36, RuntimeBlockRecord>,
}

impl RuntimeMaps {
    fn ensure_empty(self) -> Result<()> {
        ensure!(
            self.transactions.is_empty() && self.rewards.is_empty() && self.blocks.is_empty(),
            "unused metadata records remained after extraction"
        );
        Ok(())
    }
}

fn load_historical(reader: Box<dyn Read>) -> Result<HistoricalMaps> {
    let mut reader = PostcardFramedReader::new(reader);
    let mut out = HistoricalMaps::default();

    while let Some(record) = reader.read::<HistoricalArchiveRecord>()? {
        match record {
            HistoricalArchiveRecord::Transaction(tx) => {
                insert_unique(&mut out.transactions, tx.cid, tx, "historical transaction")?
            }
            HistoricalArchiveRecord::Entry(entry) => {
                insert_unique(&mut out.entries, entry.cid, entry, "entry")?
            }
            HistoricalArchiveRecord::Block(block) => {
                insert_unique(&mut out.blocks, block.cid, block, "historical block")?
            }
            HistoricalArchiveRecord::DataFrame(frame) => {
                insert_unique(&mut out.dataframes, frame.cid, frame, "dataframe")?
            }
            HistoricalArchiveRecord::Subset(subset) => {
                insert_unique(&mut out.subsets, subset.cid, subset, "subset")?
            }
            HistoricalArchiveRecord::Epoch(epoch) => {
                insert_unique(&mut out.epochs, epoch.cid, epoch, "epoch")?
            }
        }
    }

    Ok(out)
}

fn load_runtime(reader: Box<dyn Read>) -> Result<RuntimeMaps> {
    let mut reader = PostcardFramedReader::new(reader);
    let mut out = RuntimeMaps::default();

    while let Some(record) = reader.read::<RuntimeArchiveRecord>()? {
        match record {
            RuntimeArchiveRecord::Transaction(tx) => {
                insert_unique(&mut out.transactions, tx.cid, tx, "metadata transaction")?
            }
            RuntimeArchiveRecord::Rewards(rewards) => {
                insert_unique(&mut out.rewards, rewards.cid, rewards, "rewards")?
            }
            RuntimeArchiveRecord::Block(block) => {
                insert_unique(&mut out.blocks, block.cid, block, "metadata block")?
            }
        }
    }

    Ok(out)
}

fn insert_unique<T>(map: &mut HashMap<Cid36, T>, cid: Cid36, value: T, kind: &str) -> Result<()> {
    ensure!(
        map.insert(cid, value).is_none(),
        "duplicate {kind} record for {cid}"
    );
    Ok(())
}

fn raw_node_kind_name(node: &RawNode) -> &'static str {
    match node {
        RawNode::Transaction(_) => "transaction",
        RawNode::Entry(_) => "entry",
        RawNode::Block(_) => "block",
        RawNode::Subset(_) => "subset",
        RawNode::Epoch(_) => "epoch",
        RawNode::Rewards(_) => "rewards",
        RawNode::DataFrame(_) => "dataframe",
    }
}

#[cfg(test)]
mod tests {
    use super::{BLOCK_INDEX_FILE, RECONSTRUCT_FILE, build, extract, open_zstd_input};
    use blockzilla_format::{Pass1BlockIndexHeader, Pass1BlockIndexRecord, PostcardFramedReader};
    use std::{
        fs,
        io::Read,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn round_trips_synthetic_car_through_pass1() {
        let tmp = unique_temp_dir();
        fs::create_dir_all(&tmp).expect("create temp dir");

        let input = tmp.join("input.car");
        let bundle = tmp.join("bundle");
        let output = tmp.join("output.car");
        let original = build_synthetic_car();
        fs::write(&input, &original).expect("write input car");

        build(&input, &bundle).expect("build pass1 bundle");
        extract(&bundle, &output).expect("extract pass1 bundle");

        let rebuilt = fs::read(&output).expect("read rebuilt car");
        assert_eq!(original, rebuilt);

        let mut index_reader = PostcardFramedReader::new(
            fs::File::open(bundle.join(BLOCK_INDEX_FILE)).expect("open block index"),
        );
        let header = index_reader
            .read::<Pass1BlockIndexHeader>()
            .expect("read block index header")
            .expect("block index header");
        assert_eq!(header.version, 1);
        let record = index_reader
            .read::<Pass1BlockIndexRecord>()
            .expect("read block index record")
            .expect("block index record");
        assert_eq!(record.slot, 42);
        assert_eq!(record.transaction_count, 1);

        let mut reconstruct = Vec::new();
        open_zstd_input(bundle.join(RECONSTRUCT_FILE).as_path())
            .expect("open reconstruct")
            .read_to_end(&mut reconstruct)
            .expect("read reconstruct bytes");
        assert!(!reconstruct.is_empty());
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("blockzilla-pass1-{}-{}", std::process::id(), nanos))
    }

    fn build_synthetic_car() -> Vec<u8> {
        use of_car_reader::confirmed_block;
        use of_car_reader::reconstruct::Cid36;
        use prost::Message;

        let tx_payload = encode_transaction_node(42, Some(0), &minimal_legacy_transaction());
        let tx_cid = Cid36::compute(&tx_payload);

        let rewards_bytes = confirmed_block::Rewards {
            rewards: Vec::new(),
            num_partitions: None,
        }
        .encode_to_vec();
        let rewards_payload = encode_rewards_node(42, &rewards_bytes);
        let rewards_cid = Cid36::compute(&rewards_payload);

        let entry_ref = cid_ref(tx_cid);
        let entry_payload = encode_entry_node(1, [0x44; 32], &[entry_ref]);
        let entry_cid = Cid36::compute(&entry_payload);

        let block_payload = encode_block_node(42, entry_cid, rewards_cid);
        let block_cid = Cid36::compute(&block_payload);

        let subset_payload = encode_subset_node(42, 42, &[cid_ref(block_cid)]);
        let subset_cid = Cid36::compute(&subset_payload);

        let epoch_payload = encode_epoch_node(0, &[cid_ref(subset_cid)]);

        let mut out = Vec::new();
        out.push(0);
        push_car_entry(&mut out, tx_cid, &tx_payload);
        push_car_entry(&mut out, entry_cid, &entry_payload);
        push_car_entry(&mut out, rewards_cid, &rewards_payload);
        push_car_entry(&mut out, block_cid, &block_payload);
        push_car_entry(&mut out, subset_cid, &subset_payload);
        push_car_entry(&mut out, Cid36::compute(&epoch_payload), &epoch_payload);
        out
    }

    fn push_car_entry(out: &mut Vec<u8>, cid: of_car_reader::reconstruct::Cid36, payload: &[u8]) {
        push_uvarint(out, (cid.car_bytes().len() + payload.len()) as u64);
        out.extend_from_slice(cid.car_bytes());
        out.extend_from_slice(payload);
    }

    fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                return;
            }
        }
    }

    fn cid_ref(cid: of_car_reader::reconstruct::Cid36) -> of_car_reader::reconstruct::RawCidRef {
        let cbor_bytes = cid.cbor_bytes();
        of_car_reader::reconstruct::RawCidRef {
            cid: Some(cid),
            normalized_bytes: cid.car_bytes().to_vec(),
            cbor_bytes: cbor_bytes.to_vec(),
            tagged: true,
        }
    }

    fn encode_dataframe(data: &[u8]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("vec encoder is infallible");
        e.u64(6).expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.bytes(data).expect("vec encoder is infallible");
        e.into_writer()
    }

    fn encode_transaction_node(slot: u64, index: Option<u64>, tx_bytes: &[u8]) -> Vec<u8> {
        let data_frame = encode_dataframe(tx_bytes);
        let meta_frame = encode_dataframe(&[]);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("vec encoder is infallible");
        e.u64(0).expect("vec encoder is infallible");
        e.writer_mut().extend_from_slice(&data_frame);
        e.writer_mut().extend_from_slice(&meta_frame);
        e.u64(slot).expect("vec encoder is infallible");
        if let Some(index) = index {
            e.u64(index).expect("vec encoder is infallible");
        } else {
            e.null().expect("vec encoder is infallible");
        }
        e.into_writer()
    }

    fn encode_rewards_node(slot: u64, rewards_bytes: &[u8]) -> Vec<u8> {
        let frame = encode_dataframe(rewards_bytes);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(5).expect("vec encoder is infallible");
        e.u64(slot).expect("vec encoder is infallible");
        e.writer_mut().extend_from_slice(&frame);
        e.into_writer()
    }

    fn encode_entry_node(
        num_hashes: u64,
        hash: [u8; 32],
        txs: &[of_car_reader::reconstruct::RawCidRef],
    ) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(1).expect("vec encoder is infallible");
        e.u64(num_hashes).expect("vec encoder is infallible");
        e.bytes(&hash).expect("vec encoder is infallible");
        e.array(txs.len() as u64)
            .expect("vec encoder is infallible");
        for tx in txs {
            encode_cid_ref(&mut e, tx);
        }
        e.into_writer()
    }

    fn encode_block_node(
        slot: u64,
        entry_cid: of_car_reader::reconstruct::Cid36,
        rewards_cid: of_car_reader::reconstruct::Cid36,
    ) -> Vec<u8> {
        let entry_ref = cid_ref(entry_cid);
        let rewards_ref = cid_ref(rewards_cid);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(6).expect("vec encoder is infallible");
        e.u64(2).expect("vec encoder is infallible");
        e.u64(slot).expect("vec encoder is infallible");
        e.array(1).expect("vec encoder is infallible");
        e.array(2).expect("vec encoder is infallible");
        e.i64(1).expect("vec encoder is infallible");
        e.i64(2).expect("vec encoder is infallible");
        e.array(1).expect("vec encoder is infallible");
        encode_cid_ref(&mut e, &entry_ref);
        e.array(1).expect("vec encoder is infallible");
        e.u64(slot - 1).expect("vec encoder is infallible");
        encode_cid_ref(&mut e, &rewards_ref);
        e.into_writer()
    }

    fn encode_subset_node(
        first: u64,
        last: u64,
        blocks: &[of_car_reader::reconstruct::RawCidRef],
    ) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(3).expect("vec encoder is infallible");
        e.u64(first).expect("vec encoder is infallible");
        e.u64(last).expect("vec encoder is infallible");
        e.array(blocks.len() as u64)
            .expect("vec encoder is infallible");
        for block in blocks {
            encode_cid_ref(&mut e, block);
        }
        e.into_writer()
    }

    fn encode_epoch_node(epoch: u64, subsets: &[of_car_reader::reconstruct::RawCidRef]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(4).expect("vec encoder is infallible");
        e.u64(epoch).expect("vec encoder is infallible");
        e.array(subsets.len() as u64)
            .expect("vec encoder is infallible");
        for subset in subsets {
            encode_cid_ref(&mut e, subset);
        }
        e.into_writer()
    }

    fn encode_cid_ref(
        e: &mut minicbor::Encoder<Vec<u8>>,
        cid: &of_car_reader::reconstruct::RawCidRef,
    ) {
        if cid.tagged {
            e.tag(minicbor::data::Tag::new(42))
                .expect("vec encoder is infallible");
        }
        e.bytes(&cid.cbor_bytes).expect("vec encoder is infallible");
    }

    fn minimal_legacy_transaction() -> Vec<u8> {
        let mut out = Vec::new();
        out.push(1); // signatures len
        out.extend_from_slice(&[7u8; 64]);
        out.extend_from_slice(&[1, 0, 0]); // header
        out.push(1); // account keys len
        out.extend_from_slice(&[9u8; 32]);
        out.extend_from_slice(&[0; 32]); // recent blockhash
        out.push(0); // instruction len
        out
    }
}
