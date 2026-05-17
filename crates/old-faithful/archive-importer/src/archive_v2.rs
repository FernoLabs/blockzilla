use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_RAW_BLOCKS_FILE, ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE,
    ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX,
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_LOGS,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2ComputeBudgetInstructionData,
    ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow,
    ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
    ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord, ArchiveV2HotRewards, ArchiveV2HotTxRow,
    ArchiveV2HotV0Message, ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef,
    ArchiveV2VoteLockoutOffset, ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync,
    CompactBlockHeader, CompactInnerInstruction, CompactInnerInstructions, CompactLogStream,
    CompactMessageHeader, CompactMetaV1, CompactPohEntry, CompactPubkey, CompactReturnData,
    CompactReward, CompactShredding, CompactTokenBalance, KeyIndex, KeyStore,
    OwnedCompactAddressTableLookup, OwnedCompactInstruction, OwnedCompactLegacyMessage,
    OwnedCompactMessage, OwnedCompactRecentBlockhash, OwnedCompactTransaction,
    OwnedCompactV0Message, SplitCompactIndexRecord, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
    WINCODE_ARCHIVE_V2_VERSION, WincodeArchiveV2Block, WincodeArchiveV2BlockHeader,
    WincodeArchiveV2Footer, WincodeArchiveV2Genesis, WincodeArchiveV2GenesisAccount,
    WincodeArchiveV2GenesisBuiltin, WincodeArchiveV2GenesisEpochSchedule,
    WincodeArchiveV2GenesisFeeParams, WincodeArchiveV2GenesisInflationParams,
    WincodeArchiveV2GenesisPohParams, WincodeArchiveV2GenesisRentParams, WincodeArchiveV2Header,
    WincodeArchiveV2NoRegistryAddressTableLookup, WincodeArchiveV2NoRegistryBlock,
    WincodeArchiveV2NoRegistryBlockHeader, WincodeArchiveV2NoRegistryGenesis,
    WincodeArchiveV2NoRegistryGenesisAccount, WincodeArchiveV2NoRegistryGenesisBuiltin,
    WincodeArchiveV2NoRegistryInstruction, WincodeArchiveV2NoRegistryLegacyMessage,
    WincodeArchiveV2NoRegistryMessage, WincodeArchiveV2NoRegistryMeta,
    WincodeArchiveV2NoRegistryRecord, WincodeArchiveV2NoRegistryReturnData,
    WincodeArchiveV2NoRegistryReward, WincodeArchiveV2NoRegistryRewards,
    WincodeArchiveV2NoRegistryTokenBalance, WincodeArchiveV2NoRegistryTransaction,
    WincodeArchiveV2NoRegistryTx, WincodeArchiveV2NoRegistryV0Message, WincodeArchiveV2Payload,
    WincodeArchiveV2PohRecord, WincodeArchiveV2Record, WincodeArchiveV2Rewards,
    WincodeArchiveV2ShreddingRecord, WincodeArchiveV2Transaction, WincodeLeb128FramedReader,
    WincodeLeb128FramedWriter, archive_v2_hot_index_path, encode_with_scratch,
    read_archive_v2_hot_block_index, wincode_leb128_config, write_archive_v2_hot_block_index,
    write_registry,
};
use core::mem::MaybeUninit;
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::{Rewards, TransactionStatusMeta},
    genesis::{GenesisAccountEntry, GenesisArchive},
    metadata_decoder::{
        InnerInstructionVisit, ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
        ZstdReusableDecoder, decode_rewards_from_frame, decode_transaction_status_meta_from_frame,
        slot_uses_protobuf_metadata, visit_protobuf_transaction_status_meta,
    },
    node::{CborCidRef, decode_entry_summary, is_block_node, is_entry_node},
    reconstruct::{
        Cid36, RawBlockNode, RawEntryNode, RawNode, RawRewardsNode, RawTransactionNode,
        StandaloneDataFrame, decode_raw_node,
    },
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use prost::Message;
use solana_pubkey::Pubkey;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::File,
    io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tracing::info;
use wincode::{
    ReadResult, SchemaRead,
    config::{Config, ConfigCore},
    io::Reader,
    len::SeqLen,
};

use crate::{
    BUFFER_SIZE, CarBenchInputFormat, ProgressTracker, genesis_epoch0,
    split_compact::{build_registry_and_blockhash_for_input, load_blockhash_registry_plain},
};

const ARCHIVE_FILE: &str = "archive-v2.wincode";
const ARCHIVE_NO_REGISTRY_FILE: &str = "archive-v2-no-registry.wincode";
const ARCHIVE_ZSTD_BLOCKS_FILE: &str = "archive-v2-blocks.zstd";
const ARCHIVE_ZSTD_INDEX_FILE: &str = "archive-v2-blocks.index";
const ARCHIVE_ZSTD_META_FILE: &str = "archive-v2-meta.wincode";
const ARCHIVE_RAW_BLOCKS_FILE: &str = ARCHIVE_V2_RAW_BLOCKS_FILE;
const ARCHIVE_RAW_BLOCKS_ZSTD_FILE: &str = ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE;
const POH_FILE: &str = ARCHIVE_V2_POH_FILE;
const SHREDDING_FILE: &str = ARCHIVE_V2_SHREDDING_FILE;
const REGISTRY_FILE: &str = ARCHIVE_V2_PUBKEY_REGISTRY_FILE;
const REGISTRY_COUNTS_FILE: &str = ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE;
const BLOCKHASH_REGISTRY_FILE: &str = ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE;
const PREV_BLOCKHASH_TAIL_FILE: &str = ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE;
const ARCHIVE_INDEX_FILE: &str = "archive-v2.index";
const ARCHIVE_FLAGS_LEB128: u32 = 1 << 0;
const ARCHIVE_FLAGS_NO_REGISTRY: u32 = 1 << 1;
const ROLLING_BLOCKHASH_CAPACITY: usize = 300;
const RECENT_BLOCKHASH_SLOT_WINDOW: u64 = 150;
const ARCHIVE_V2_INDEX_MAGIC: &[u8; 8] = b"BZV2IDX1";
const ARCHIVE_V2_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8;
const ARCHIVE_V2_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_ZSTD_INDEX_MAGIC: &[u8; 8] = b"BZV2ZIX1";
const ARCHIVE_V2_ZSTD_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY: u32 = 1 << 0;
const ARCHIVE_V2_INDEX_SCAN_BUFFER_SIZE: usize = 4 << 10;
const WINCODE_ARCHIVE_V2_RECORD_HEADER_TAG: u8 = 0;
const WINCODE_ARCHIVE_V2_RECORD_BLOCK_TAG: u8 = 1;
const WINCODE_ARCHIVE_V2_RECORD_INDEX_TAG: u8 = 2;
const WINCODE_ARCHIVE_V2_RECORD_FOOTER_TAG: u8 = 3;
const WINCODE_ARCHIVE_V2_RECORD_GENESIS_TAG: u8 = 4;

pub(crate) fn build(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    resume: bool,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let blockhash_has_required_genesis = match &genesis {
        Some(genesis) if crate::file_nonempty(&blockhash_registry_path) => {
            genesis_epoch0::blockhash_registry_starts_with(
                &blockhash_registry_path,
                &genesis.genesis_hash,
            )?
        }
        Some(_) => false,
        None => true,
    };
    if !(resume
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
        && crate::file_nonempty(&blockhash_registry_path)
        && blockhash_has_required_genesis)
    {
        build_registry_and_blockhash_for_input(
            input,
            &registry_path,
            Some(&registry_counts_path),
            &blockhash_registry_path,
        )?;
    }

    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys);
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;
    let blockhash_id_offset = blockhash_id_offset_for_genesis(&genesis, &blockhashes)?;

    let archive_path = output_dir.join(ARCHIVE_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let archive_file = File::create(&archive_path)
        .with_context(|| format!("create {}", archive_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, archive_file));
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, poh_file));
    writer.write(&WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
        version: WINCODE_ARCHIVE_V2_VERSION,
        flags: ARCHIVE_FLAGS_LEB128,
    }))?;
    if let Some(genesis) = &genesis {
        writer.write(&WincodeArchiveV2Record::Genesis(compact_genesis_record(
            genesis, &key_index,
        )?))?;
    }

    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::default();
    let mut progress = ProgressTracker::new("Archive V2 Write");
    let mut block_offset = 0u64;
    let mut block_id = 0u32;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    if let Some(genesis) = &genesis {
        rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
    }

    while let Some(raw) = scanner.next_node_timed(Some(&mut timings))? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = Instant::now();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                rolling_blockhashes.prune_for_slot(block.slot)?;
                timings.classify += classify_started.elapsed();
                let (record, tx_count, sidecar) = build_block_record(
                    &mut pending,
                    block,
                    &key_index,
                    &rolling_blockhashes,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut footer,
                    &mut timings,
                )?;
                let blockhash_index = block_id as usize + blockhash_id_offset as usize;
                let expected_blockhash = blockhashes.get(blockhash_index).with_context(|| {
                    format!("missing blockhash registry entry for blockhash id {blockhash_index}")
                })?;
                anyhow::ensure!(
                    sidecar.blockhash == *expected_blockhash,
                    "blockhash registry mismatch at block_id {} slot {}",
                    block_id,
                    pending.last_slot
                );
                let encode_started = Instant::now();
                let record = WincodeArchiveV2Record::Block(record);
                encode_with_scratch(&record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                writer.write(&WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                    slot: pending.last_slot,
                    block_id,
                    block_offset,
                    block_len,
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count,
                }))?;
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot: pending.last_slot,
                    entries: sidecar.poh_entries,
                })?;
                timings.wincode_encode += encode_started.elapsed();
                let current_block_id = i32::try_from(block_id.saturating_add(blockhash_id_offset))
                    .context("blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(
                    sidecar.blockhash,
                    current_block_id,
                    pending.last_slot,
                )?;
                block_offset += block_len as u64;
                block_id = block_id.wrapping_add(1);
                progress.update_slot(pending.last_slot);
                progress.update(1, tx_count as u64);
                pending.clear();
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    let encode_started = Instant::now();
    writer.write(&WincodeArchiveV2Record::Footer(footer.clone()))?;
    timings.wincode_encode += encode_started.elapsed();
    writer.flush()?;
    poh_writer.flush()?;
    progress.final_report();
    info!("Archive V2 build complete");
    info!("  archive: {}", archive_path.display());
    info!("  poh: {}", poh_path.display());
    info!(
        "  blockhash_registry: {}",
        blockhash_registry_path.display()
    );
    info!("  pubkey_registry: {}", registry_path.display());
    info!(
        "  coverage: entries={} payload_bytes={} decoded_payload_bytes={} tx_raw_fallbacks={} metadata_raw_fallbacks={} rewards_raw_fallbacks={} nonce_recent_blockhashes={}",
        footer.car_entries,
        footer.car_payload_bytes,
        footer.decoded_node_payload_bytes,
        footer.tx_raw_fallbacks,
        footer.metadata_raw_fallbacks,
        footer.rewards_raw_fallbacks,
        footer.nonce_recent_blockhashes
    );
    info!(
        "  timings: scan/decode_node={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s rewards_decode_compact={:.3}s wincode_encode={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.wincode_encode.as_secs_f64(),
    );
    info!(
        "  archive_v2_stats: tx_reassembled={} metadata_reassembled={} metadata_protobuf_visit={} metadata_owned_fallback={} tx_scratch_max={} metadata_scratch_max={}",
        timings.tx_reassembled,
        timings.metadata_reassembled,
        timings.metadata_protobuf_visit,
        timings.metadata_owned_fallback,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
    );
    Ok(())
}

pub(crate) fn build_hot_blocks(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    level: i32,
    max_blocks: Option<u64>,
    resume: bool,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;

    let registry_path = output_dir.join(REGISTRY_FILE);
    let registry_counts_path = output_dir.join(REGISTRY_COUNTS_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let blockhash_has_required_genesis = match &genesis {
        Some(genesis) if crate::file_nonempty(&blockhash_registry_path) => {
            genesis_epoch0::blockhash_registry_starts_with(
                &blockhash_registry_path,
                &genesis.genesis_hash,
            )?
        }
        Some(_) => false,
        None => true,
    };
    if !(resume
        && crate::file_nonempty(&registry_path)
        && crate::file_nonempty(&registry_counts_path)
        && crate::file_nonempty(&blockhash_registry_path)
        && blockhash_has_required_genesis)
    {
        build_registry_and_blockhash_for_input(
            input,
            &registry_path,
            Some(&registry_counts_path),
            &blockhash_registry_path,
        )?;
    }

    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys.clone());
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;
    let blockhash_id_offset = blockhash_id_offset_for_genesis(&genesis, &blockhashes)?;

    let blocks_path = output_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_V2_META_FILE);
    let signatures_path = output_dir.join(ARCHIVE_V2_SIGNATURES_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let shredding_path = output_dir.join(SHREDDING_FILE);
    let vote_hash_registry_path = output_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);

    let blocks_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(BUFFER_SIZE, blocks_file);
    let meta_file =
        File::create(&meta_path).with_context(|| format!("create {}", meta_path.display()))?;
    let mut meta_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, meta_file));
    let signatures_file = File::create(&signatures_path)
        .with_context(|| format!("create {}", signatures_path.display()))?;
    let mut signatures_writer = BufWriter::with_capacity(BUFFER_SIZE, signatures_file);
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, poh_file));
    let shredding_file = File::create(&shredding_path)
        .with_context(|| format!("create {}", shredding_path.display()))?;
    let mut shredding_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, shredding_file));
    let mut compressor = zstd::bulk::Compressor::new(level).context("create zstd compressor")?;
    let mut block_bytes = Vec::new();
    let mut compressed_buf = Vec::new();

    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: ARCHIVE_FLAGS_LEB128,
        }),
    )?;
    if let Some(genesis) = &genesis {
        write_hot_meta(
            &mut meta_writer,
            &ArchiveV2HotMetaRecord::Genesis(compact_genesis_record(genesis, &key_index)?),
        )?;
    }

    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::default();
    let mut progress = ProgressTracker::new("Archive V2 Hot Write");
    let mut block_id = 0u32;
    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    if let Some(genesis) = &genesis {
        rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
    }

    let mut rows = Vec::new();
    let mut blob_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut first_tx_ordinal = 0u64;
    let mut first_signature_ordinal = 0u64;
    let mut slot_to_block_id: GxHashMap<u64, u32> =
        GxHashMap::with_hasher(GxBuildHasher::default());
    let mut vote_hashes = VoteHashRegistryBuilder::default();
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(Some(&mut timings))? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = Instant::now();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                rolling_blockhashes.prune_for_slot(block.slot)?;
                timings.classify += classify_started.elapsed();
                let (record, tx_count, sidecar) = build_block_record(
                    &mut pending,
                    block,
                    &key_index,
                    &rolling_blockhashes,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut footer,
                    &mut timings,
                )?;
                let slot = pending.last_slot;
                let blockhash_index = block_id as usize + blockhash_id_offset as usize;
                let expected_blockhash = blockhashes.get(blockhash_index).with_context(|| {
                    format!("missing blockhash registry entry for blockhash id {blockhash_index}")
                })?;
                anyhow::ensure!(
                    sidecar.blockhash == *expected_blockhash,
                    "blockhash registry mismatch at block_id {} slot {}",
                    block_id,
                    slot
                );

                let encode_started = Instant::now();
                vote_hashes.ensure_block(block_id);
                let block_shredding = record.header.compact.shredding.clone();
                let (hot_block, block_signature_count) = hot_block_from_archive_block(
                    record,
                    &store,
                    &slot_to_block_id,
                    &mut vote_hashes,
                    &mut signatures_writer,
                    &mut timings,
                )
                .with_context(|| format!("slot {slot} hot block encode"))?;
                block_bytes.clear();
                let block_serialize_started = Instant::now();
                wincode::config::serialize_into(
                    &mut block_bytes,
                    &hot_block,
                    wincode_leb128_config(),
                )?;
                timings.hot_block_serialize += block_serialize_started.elapsed();
                let uncompressed_len = u32::try_from(block_bytes.len())
                    .context("hot archive v2 block payload exceeds u32::MAX")?;
                let compress_bound = zstd::zstd_safe::compress_bound(block_bytes.len());
                if compressed_buf.capacity() < compress_bound {
                    compressed_buf.reserve(compress_bound - compressed_buf.capacity());
                    timings.hot_zstd_buffer_reserves += 1;
                }
                timings.hot_zstd_buffer_capacity_max = timings
                    .hot_zstd_buffer_capacity_max
                    .max(compressed_buf.capacity());
                let compress_started = Instant::now();
                compressor
                    .compress_to_buffer(&block_bytes, &mut compressed_buf)
                    .with_context(|| format!("zstd compress hot block_id {block_id}"))?;
                timings.hot_zstd_compress += compress_started.elapsed();
                let compressed_len = u32::try_from(compressed_buf.len())
                    .context("compressed hot archive v2 block exceeds u32::MAX")?;
                let write_started = Instant::now();
                blocks_writer
                    .write_all(&compressed_buf)
                    .with_context(|| format!("write {}", blocks_path.display()))?;
                timings.hot_block_write += write_started.elapsed();
                let poh_started = Instant::now();
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot,
                    entries: sidecar.poh_entries,
                })?;
                shredding_writer.write(&WincodeArchiveV2ShreddingRecord {
                    block_id,
                    slot,
                    shredding: block_shredding,
                })?;
                timings.hot_poh_write += poh_started.elapsed();
                timings.wincode_encode += encode_started.elapsed();

                rows.push(ArchiveV2HotBlockIndexRow {
                    block_id,
                    slot,
                    compressed_offset: blob_offset,
                    compressed_len,
                    uncompressed_len,
                    tx_count,
                    first_tx_ordinal,
                    first_signature_ordinal,
                    signature_count: block_signature_count,
                });
                blob_offset += compressed_len as u64;
                uncompressed_bytes += uncompressed_len as u64;
                compressed_bytes += compressed_len as u64;
                first_tx_ordinal += tx_count as u64;
                first_signature_ordinal += block_signature_count as u64;

                let current_block_id = i32::try_from(block_id.saturating_add(blockhash_id_offset))
                    .context("blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(sidecar.blockhash, current_block_id, slot)?;
                slot_to_block_id.insert(slot, block_id);
                block_id = block_id.wrapping_add(1);
                progress.update_slot(slot);
                progress.update(1, tx_count as u64);
                pending.clear();
                if max_blocks.is_some_and(|limit| rows.len() as u64 >= limit) {
                    break;
                }
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    signatures_writer
        .flush()
        .with_context(|| format!("flush {}", signatures_path.display()))?;
    poh_writer.flush()?;
    shredding_writer.flush()?;
    vote_hashes.write(&vote_hash_registry_path)?;
    write_archive_v2_hot_block_index(&index_path, blob_offset, level, 0, &rows)?;
    write_hot_meta(
        &mut meta_writer,
        &ArchiveV2HotMetaRecord::Footer(footer.clone()),
    )?;
    meta_writer.flush()?;
    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let ratio = if uncompressed_bytes > 0 {
        compressed_bytes as f64 * 100.0 / uncompressed_bytes as f64
    } else {
        0.0
    };
    info!(
        "Archive V2 hot-block build complete in {:.2}s: blocks={} txs={} signatures={} level={} max_blocks={:?} uncompressed_bytes={} compressed_bytes={} ratio_pct={:.2} compact_vote_ix={} vote_bank_hash_refs={} vote_bank_hash_raw={} vote_bank_hash_conflict_raw={} vote_block_id_refs={} vote_block_id_raw={} vote_block_id_zero={} vote_block_id_conflict_raw={} blocks_file={} index={} meta={} signatures={} vote_hash_registry={} poh={} shredding={}",
        elapsed,
        rows.len(),
        first_tx_ordinal,
        first_signature_ordinal,
        level,
        max_blocks,
        uncompressed_bytes,
        compressed_bytes,
        ratio,
        vote_hashes.compact_vote_ix,
        vote_hashes.bank_hash_refs,
        vote_hashes.bank_hash_raw,
        vote_hashes.bank_hash_conflict_raw,
        vote_hashes.block_id_refs,
        vote_hashes.block_id_raw,
        vote_hashes.block_id_zero,
        vote_hashes.block_id_conflict_raw,
        blocks_path.display(),
        index_path.display(),
        meta_path.display(),
        signatures_path.display(),
        vote_hash_registry_path.display(),
        poh_path.display(),
        shredding_path.display()
    );
    info!(
        "Archive V2 hot timings: scan/decode_node={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode_compact={:.3}s metadata_decode_compact={:.3}s rewards_decode_compact={:.3}s hot_message_build={:.3}s hot_message_encode={:.3}s hot_metadata_encode={:.3}s hot_signature_write={:.3}s hot_block_serialize={:.3}s hot_zstd_compress={:.3}s hot_block_write={:.3}s hot_poh_write={:.3}s total_hot_encode_scope={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.hot_message_build.as_secs_f64(),
        timings.hot_message_encode.as_secs_f64(),
        timings.hot_metadata_encode.as_secs_f64(),
        timings.hot_signature_write.as_secs_f64(),
        timings.hot_block_serialize.as_secs_f64(),
        timings.hot_zstd_compress.as_secs_f64(),
        timings.hot_block_write.as_secs_f64(),
        timings.hot_poh_write.as_secs_f64(),
        timings.wincode_encode.as_secs_f64(),
    );
    info!(
        "Archive V2 hot stats: tx_reassembled={} metadata_reassembled={} metadata_protobuf_visit={} metadata_owned_fallback={} tx_scratch_max={} metadata_scratch_max={} zstd_buffer_reserves={} zstd_buffer_capacity_max={}",
        timings.tx_reassembled,
        timings.metadata_reassembled,
        timings.metadata_protobuf_visit,
        timings.metadata_owned_fallback,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
        timings.hot_zstd_buffer_reserves,
        timings.hot_zstd_buffer_capacity_max,
    );
    Ok(())
}

fn write_hot_meta<W: Write>(
    writer: &mut WincodeLeb128FramedWriter<W>,
    record: &ArchiveV2HotMetaRecord,
) -> Result<()> {
    let bytes = wincode::config::serialize(record, wincode_leb128_config())?;
    writer.write_bytes(&bytes)
}

#[derive(Default)]
struct VoteHashRegistryBuilder {
    rows: Vec<VoteHashRegistryRow>,
    compact_vote_ix: u64,
    bank_hash_refs: u64,
    bank_hash_raw: u64,
    bank_hash_conflict_raw: u64,
    block_id_refs: u64,
    block_id_raw: u64,
    block_id_zero: u64,
    block_id_conflict_raw: u64,
}

#[derive(Clone, Copy, Default)]
struct VoteHashRegistryRow {
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>,
}

impl VoteHashRegistryBuilder {
    fn ensure_block(&mut self, block_id: u32) {
        let needed = block_id as usize + 1;
        if self.rows.len() < needed {
            self.rows.resize(needed, VoteHashRegistryRow::default());
        }
    }

    fn ref_bank_hash(
        &mut self,
        slot: Option<u64>,
        hash: [u8; 32],
        slot_to_block_id: &GxHashMap<u64, u32>,
    ) -> Result<ArchiveV2VoteHashRef> {
        if hash == [0u8; 32] {
            return Ok(ArchiveV2VoteHashRef::Zero);
        }
        let Some(slot) = slot else {
            self.bank_hash_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        let Some(&block_id) = slot_to_block_id.get(&slot) else {
            self.bank_hash_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        self.ensure_block(block_id);
        let row = &mut self.rows[block_id as usize];
        if let Some(existing) = row.bank_hash {
            if existing != hash {
                self.bank_hash_raw += 1;
                self.bank_hash_conflict_raw += 1;
                return Ok(ArchiveV2VoteHashRef::Raw(hash));
            }
        } else {
            row.bank_hash = Some(hash);
        }
        self.bank_hash_refs += 1;
        Ok(ArchiveV2VoteHashRef::Block(block_id))
    }

    fn ref_block_id_hash(
        &mut self,
        slot: Option<u64>,
        hash: [u8; 32],
        slot_to_block_id: &GxHashMap<u64, u32>,
    ) -> Result<ArchiveV2VoteHashRef> {
        if hash == [0u8; 32] {
            self.block_id_zero += 1;
            return Ok(ArchiveV2VoteHashRef::Zero);
        }
        let Some(slot) = slot else {
            self.block_id_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        let Some(&block_id) = slot_to_block_id.get(&slot) else {
            self.block_id_raw += 1;
            return Ok(ArchiveV2VoteHashRef::Raw(hash));
        };
        self.ensure_block(block_id);
        let row = &mut self.rows[block_id as usize];
        if let Some(existing) = row.block_id_hash {
            if existing != hash {
                self.block_id_raw += 1;
                self.block_id_conflict_raw += 1;
                return Ok(ArchiveV2VoteHashRef::Raw(hash));
            }
        } else {
            row.block_id_hash = Some(hash);
        }
        self.block_id_refs += 1;
        Ok(ArchiveV2VoteHashRef::Block(block_id))
    }

    fn ref_aux_hash(&self, hash: [u8; 32]) -> ArchiveV2VoteHashRef {
        if hash == [0u8; 32] {
            ArchiveV2VoteHashRef::Zero
        } else {
            ArchiveV2VoteHashRef::Raw(hash)
        }
    }

    fn write(&self, path: &Path) -> Result<()> {
        let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        for row in &self.rows {
            let flags =
                u8::from(row.bank_hash.is_some()) | (u8::from(row.block_id_hash.is_some()) << 1);
            writer.write_all(&[flags])?;
            writer.write_all(&row.bank_hash.unwrap_or([0u8; 32]))?;
            writer.write_all(&row.block_id_hash.unwrap_or([0u8; 32]))?;
        }
        writer
            .flush()
            .with_context(|| format!("flush {}", path.display()))?;
        Ok(())
    }
}

fn hot_block_from_archive_block(
    block: WincodeArchiveV2Block,
    store: &KeyStore,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
    signatures_writer: &mut impl Write,
    timings: &mut ArchiveV2Timings,
) -> Result<(ArchiveV2HotBlockBlob, u32)> {
    let compact = block.header.compact;
    let rewards = match block.header.rewards {
        Some(rewards) => Some(hot_rewards_from_archive(rewards)?),
        None => None,
    };
    let header = ArchiveV2HotBlockHeader {
        slot: compact.slot,
        parent_slot: compact.parent_slot,
        blockhash_id: compact.blockhash,
        previous_blockhash_id: compact.previous_blockhash,
        block_time: compact.block_time,
        block_height: compact.block_height,
        rewards,
    };

    let mut tx_rows = Vec::with_capacity(block.txs.len());
    let mut message_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut block_signature_count = 0u32;
    for transaction in block.txs {
        let WincodeArchiveV2Transaction {
            tx_index,
            tx,
            metadata,
        } = transaction;
        let WincodeArchiveV2Payload::Decoded { value: tx, .. } = tx else {
            anyhow::bail!(
                "hot block writer requires decoded transactions; raw transaction at tx_index {}",
                tx_index
            );
        };
        let message_offset = u32::try_from(message_bytes.len())
            .context("hot message byte region exceeds u32::MAX")?;
        let message_build_started = Instant::now();
        let (message, mut flags) =
            hot_message_from_owned(tx.message, store, slot_to_block_id, vote_hashes)?;
        timings.hot_message_build += message_build_started.elapsed();
        let message_encode_started = Instant::now();
        let before_message_len = message_bytes.len();
        wincode::config::serialize_into(&mut message_bytes, &message, wincode_leb128_config())?;
        let message_len = u32::try_from(message_bytes.len() - before_message_len)
            .context("hot message payload exceeds u32::MAX")?;
        timings.hot_message_encode += message_encode_started.elapsed();

        let signature_count =
            u8::try_from(tx.signatures.len()).context("signature count exceeds u8::MAX")?;
        let signature_write_started = Instant::now();
        for (signature_index, signature) in tx.signatures.iter().enumerate() {
            anyhow::ensure!(
                signature.len() == 64,
                "tx_index {} signature#{} is {} bytes, expected 64",
                tx_index,
                signature_index,
                signature.len()
            );
            signatures_writer.write_all(signature)?;
        }
        timings.hot_signature_write += signature_write_started.elapsed();
        block_signature_count = block_signature_count
            .checked_add(u32::from(signature_count))
            .context("block signature count overflow")?;

        let metadata_offset = u32::try_from(metadata_bytes.len())
            .context("hot metadata byte region exceeds u32::MAX")?;
        let mut metadata_len = 0u32;
        if let Some(metadata) = metadata {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
            let metadata_encode_started = Instant::now();
            match metadata {
                WincodeArchiveV2Payload::Decoded { value, .. } => {
                    flags |= hot_metadata_flags(&value);
                    let before_metadata_len = metadata_bytes.len();
                    wincode::config::serialize_into(
                        &mut metadata_bytes,
                        &value,
                        wincode_leb128_config(),
                    )?;
                    metadata_len = u32::try_from(metadata_bytes.len() - before_metadata_len)
                        .context("hot metadata payload exceeds u32::MAX")?;
                }
                WincodeArchiveV2Payload::Raw { bytes, .. } => {
                    flags |= ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK;
                    metadata_len = u32::try_from(bytes.len())
                        .context("hot raw metadata payload exceeds u32::MAX")?;
                    metadata_bytes.extend_from_slice(&bytes);
                }
            }
            timings.hot_metadata_encode += metadata_encode_started.elapsed();
        }

        tx_rows.push(ArchiveV2HotTxRow {
            tx_index,
            flags,
            message_offset,
            message_len,
            metadata_offset,
            metadata_len,
            signature_count,
            reserved: [0; 3],
        });
    }

    let tx_count = u32::try_from(tx_rows.len()).context("hot block tx count exceeds u32::MAX")?;
    Ok((
        ArchiveV2HotBlockBlob {
            header,
            tx_count,
            tx_rows,
            message_bytes,
            metadata_bytes,
        },
        block_signature_count,
    ))
}

fn hot_rewards_from_archive(rewards: WincodeArchiveV2Rewards) -> Result<ArchiveV2HotRewards> {
    anyhow::ensure!(
        rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
        "hot block writer requires decoded rewards"
    );
    Ok(ArchiveV2HotRewards {
        num_partitions: rewards.num_partitions,
        decoded: rewards.decoded.unwrap_or_default(),
    })
}

fn hot_message_from_owned(
    message: OwnedCompactMessage,
    store: &KeyStore,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<(ArchiveV2HotMessagePayload, u32)> {
    match message {
        OwnedCompactMessage::Legacy(message) => {
            let (instructions, has_compact_vote) = hot_instructions_from_owned(
                message.instructions,
                &message.account_keys,
                store,
                slot_to_block_id,
                vote_hashes,
            )?;
            let flags = if has_compact_vote {
                ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX
            } else {
                0
            };
            Ok((
                ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                    header: message.header,
                    account_keys: message.account_keys,
                    recent_blockhash: message.recent_blockhash,
                    instructions,
                }),
                flags,
            ))
        }
        OwnedCompactMessage::V0(message) => {
            let (instructions, has_compact_vote) = hot_instructions_from_owned(
                message.instructions,
                &message.account_keys,
                store,
                slot_to_block_id,
                vote_hashes,
            )?;
            let mut flags = ARCHIVE_V2_TX_FLAG_MESSAGE_V0;
            if has_compact_vote {
                flags |= ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;
            }
            Ok((
                ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                    header: message.header,
                    account_keys: message.account_keys,
                    recent_blockhash: message.recent_blockhash,
                    instructions,
                    address_table_lookups: message.address_table_lookups,
                }),
                flags,
            ))
        }
    }
}

fn hot_instructions_from_owned(
    instructions: Vec<OwnedCompactInstruction>,
    account_keys: &[CompactPubkey],
    store: &KeyStore,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<(Vec<ArchiveV2HotInstruction>, bool)> {
    let mut has_compact_vote = false;
    let mut out = Vec::with_capacity(instructions.len());
    for instruction in instructions {
        let program_id =
            resolve_static_program_id(account_keys, instruction.program_id_index, store);
        let data = if program_id.is_some_and(|program_id| program_id == vote_program_id_bytes()) {
            let data =
                parse_hot_vote_instruction_data(&instruction.data, slot_to_block_id, vote_hashes)
                    .with_context(|| "parse vote instruction data")?;
            if !matches!(data, ArchiveV2HotInstructionData::Raw(_)) {
                has_compact_vote = true;
                vote_hashes.compact_vote_ix += 1;
            }
            data
        } else if program_id
            .is_some_and(|program_id| program_id == compute_budget_program_id_bytes())
        {
            parse_hot_compute_budget_instruction_data(&instruction.data)
                .with_context(|| "parse compute budget instruction data")?
        } else if program_id.is_some_and(|program_id| program_id == system_program_id_bytes()) {
            parse_hot_system_instruction_data(&instruction.data)
                .with_context(|| "parse system instruction data")?
        } else {
            ArchiveV2HotInstructionData::Raw(instruction.data)
        };
        out.push(ArchiveV2HotInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts,
            data,
        });
    }
    Ok((out, has_compact_vote))
}

fn resolve_static_program_id(
    account_keys: &[CompactPubkey],
    program_id_index: u8,
    store: &KeyStore,
) -> Option<[u8; 32]> {
    account_keys
        .get(program_id_index as usize)
        .and_then(|key| key.resolve(store))
}

fn vote_program_id_bytes() -> [u8; 32] {
    solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111").to_bytes()
}

fn compute_budget_program_id_bytes() -> [u8; 32] {
    solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()
}

fn system_program_id_bytes() -> [u8; 32] {
    solana_pubkey::pubkey!("11111111111111111111111111111111").to_bytes()
}

fn hot_metadata_flags(metadata: &CompactMetaV1) -> u32 {
    let mut flags = 0;
    if metadata.err.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
    }
    if metadata.return_data.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
    }
    if metadata.logs.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
    }
    if metadata.inner_instructions.is_some() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
    }
    if !metadata.pre_token_balances.is_empty() || !metadata.post_token_balances.is_empty() {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    }
    if !metadata.loaded_writable_addresses.is_empty()
        || !metadata.loaded_readonly_addresses.is_empty()
    {
        flags |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
    }
    flags
}

fn parse_hot_vote_instruction_data(
    data: &[u8],
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2HotInstructionData> {
    let Some((&tag0, rest)) = data.split_first() else {
        return Ok(ArchiveV2HotInstructionData::Raw(Vec::new()));
    };
    if rest.len() < 3 {
        return Ok(ArchiveV2HotInstructionData::Raw(data.to_vec()));
    }
    let variant = u32::from_le_bytes([tag0, rest[0], rest[1], rest[2]]);
    let mut cursor = VoteInstructionCursor::new(&data[4..]);
    let parsed = match variant {
        12 => {
            let update = parse_vote_state_update(&mut cursor, slot_to_block_id, vote_hashes)?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update)
        }
        13 => {
            let update = parse_vote_state_update(&mut cursor, slot_to_block_id, vote_hashes)?;
            let switch_proof_hash = vote_hashes.ref_aux_hash(cursor.read_hash()?);
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            }
        }
        14 => {
            let tower = parse_tower_sync(&mut cursor, slot_to_block_id, vote_hashes)?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::VoteTowerSync(tower)
        }
        15 => {
            let tower = parse_tower_sync(&mut cursor, slot_to_block_id, vote_hashes)?;
            let switch_proof_hash = vote_hashes.ref_aux_hash(cursor.read_hash()?);
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            }
        }
        _ => ArchiveV2HotInstructionData::Raw(data.to_vec()),
    };
    Ok(parsed)
}

fn parse_hot_compute_budget_instruction_data(data: &[u8]) -> Result<ArchiveV2HotInstructionData> {
    let Ok(Some(parsed)) = try_parse_hot_compute_budget_instruction_data(data) else {
        return Ok(ArchiveV2HotInstructionData::Raw(data.to_vec()));
    };
    Ok(parsed)
}

fn try_parse_hot_compute_budget_instruction_data(
    data: &[u8],
) -> Result<Option<ArchiveV2HotInstructionData>> {
    let Some((&tag, rest)) = data.split_first() else {
        return Ok(None);
    };
    let mut cursor = VoteInstructionCursor::new(rest);
    let parsed = match tag {
        0 => {
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::Unused,
            )
        }
        1 => {
            let bytes = cursor.read_u32_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(bytes),
            )
        }
        2 => {
            let units = cursor.read_u32_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(units),
            )
        }
        3 => {
            let micro_lamports = cursor.read_u64_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(micro_lamports),
            )
        }
        4 => {
            let bytes = cursor.read_u32_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(bytes),
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}

fn parse_hot_system_instruction_data(data: &[u8]) -> Result<ArchiveV2HotInstructionData> {
    let Ok(Some(parsed)) = try_parse_hot_system_instruction_data(data) else {
        return Ok(ArchiveV2HotInstructionData::Raw(data.to_vec()));
    };
    Ok(parsed)
}

fn try_parse_hot_system_instruction_data(
    data: &[u8],
) -> Result<Option<ArchiveV2HotInstructionData>> {
    let mut cursor = VoteInstructionCursor::new(data);
    let variant = cursor.read_u32_le()?;
    let parsed = match variant {
        0 => {
            let lamports = cursor.read_u64_le()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::CreateAccount {
                lamports,
                space,
                owner,
            })
        }
        1 => {
            let owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Assign { owner })
        }
        2 => {
            let lamports = cursor.read_u64_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports,
            })
        }
        3 => {
            let base = cursor.read_pubkey()?;
            let seed = cursor.read_system_seed()?;
            let lamports = cursor.read_u64_le()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                    base,
                    seed,
                    lamports,
                    space,
                    owner,
                },
            )
        }
        4 => {
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AdvanceNonceAccount)
        }
        5 => {
            let lamports = cursor.read_u64_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports },
            )
        }
        6 => {
            let authority = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::InitializeNonceAccount { authority },
            )
        }
        7 => {
            let authority = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AuthorizeNonceAccount { authority },
            )
        }
        8 => {
            let space = cursor.read_u64_le()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Allocate { space })
        }
        9 => {
            let base = cursor.read_pubkey()?;
            let seed = cursor.read_system_seed()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AllocateWithSeed {
                base,
                seed,
                space,
                owner,
            })
        }
        10 => {
            let base = cursor.read_pubkey()?;
            let seed = cursor.read_system_seed()?;
            let owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::AssignWithSeed {
                base,
                seed,
                owner,
            })
        }
        11 => {
            let lamports = cursor.read_u64_le()?;
            let from_seed = cursor.read_system_seed()?;
            let from_owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::TransferWithSeed {
                lamports,
                from_seed,
                from_owner,
            })
        }
        12 => {
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::UpgradeNonceAccount)
        }
        13 => {
            let lamports = cursor.read_u64_le()?;
            let space = cursor.read_u64_le()?;
            let owner = cursor.read_pubkey()?;
            cursor.ensure_eof()?;
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccountAllowPrefund {
                    lamports,
                    space,
                    owner,
                },
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}

fn parse_tower_sync(
    cursor: &mut VoteInstructionCursor<'_>,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2VoteTowerSync> {
    let (update, last_slot) = parse_vote_state_update_parts(cursor, slot_to_block_id, vote_hashes)?;
    let block_id_hash =
        vote_hashes.ref_block_id_hash(last_slot, cursor.read_hash()?, slot_to_block_id)?;
    Ok(ArchiveV2VoteTowerSync {
        update,
        block_id_hash,
    })
}

fn parse_vote_state_update(
    cursor: &mut VoteInstructionCursor<'_>,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<ArchiveV2VoteStateUpdate> {
    Ok(parse_vote_state_update_parts(cursor, slot_to_block_id, vote_hashes)?.0)
}

fn parse_vote_state_update_parts(
    cursor: &mut VoteInstructionCursor<'_>,
    slot_to_block_id: &GxHashMap<u64, u32>,
    vote_hashes: &mut VoteHashRegistryBuilder,
) -> Result<(ArchiveV2VoteStateUpdate, Option<u64>)> {
    let root_raw = cursor.read_u64_le()?;
    let root = (root_raw != u64::MAX).then_some(root_raw);
    let len = cursor.read_short_vec_len()?;
    anyhow::ensure!(
        len <= 31,
        "vote lockout history length {len} exceeds MAX_LOCKOUT_HISTORY"
    );
    let mut lockout_offsets = Vec::with_capacity(len);
    let mut slot = root.unwrap_or_default();
    let mut last_slot = None;
    for _ in 0..len {
        let offset = cursor.read_var_u64()?;
        slot = slot
            .checked_add(offset)
            .context("vote lockout slot overflow")?;
        let confirmation_count = cursor.read_u8()?;
        lockout_offsets.push(ArchiveV2VoteLockoutOffset {
            offset,
            confirmation_count,
        });
        last_slot = Some(slot);
    }
    let hash = vote_hashes.ref_bank_hash(last_slot, cursor.read_hash()?, slot_to_block_id)?;
    let timestamp = cursor.read_option_i64()?;
    Ok((
        ArchiveV2VoteStateUpdate {
            root,
            lockout_offsets,
            hash,
            timestamp,
        },
        last_slot,
    ))
}

struct VoteInstructionCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> VoteInstructionCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn ensure_eof(&self) -> Result<()> {
        anyhow::ensure!(
            self.pos == self.bytes.len(),
            "trailing {} bytes in instruction data",
            self.bytes.len().saturating_sub(self.pos)
        );
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.pos)
            .ok_or_else(|| anyhow!("unexpected EOF in instruction data"))?;
        self.pos += 1;
        Ok(byte)
    }

    fn read_u32_le(&mut self) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64_le(&mut self) -> Result<u64> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64_le(&mut self) -> Result<i64> {
        let bytes = self.read_array::<8>()?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_hash(&mut self) -> Result<[u8; 32]> {
        self.read_array::<32>()
    }

    fn read_pubkey(&mut self) -> Result<[u8; 32]> {
        self.read_array::<32>()
    }

    fn read_system_seed(&mut self) -> Result<String> {
        let len = self.read_u64_le()?;
        let bytes = self.read_slice(len as usize)?;
        String::from_utf8(bytes.to_vec()).context("system instruction seed is not UTF-8")
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .context("instruction cursor overflow")?;
        anyhow::ensure!(
            end <= self.bytes.len(),
            "unexpected EOF in instruction data while reading {len} bytes"
        );
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .pos
            .checked_add(N)
            .context("instruction cursor overflow")?;
        anyhow::ensure!(
            end <= self.bytes.len(),
            "unexpected EOF in instruction data while reading {N} bytes"
        );
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(out)
    }

    fn read_option_i64(&mut self) -> Result<Option<i64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_i64_le()?)),
            tag => anyhow::bail!("invalid vote timestamp option tag {tag}"),
        }
    }

    fn read_short_vec_len(&mut self) -> Result<usize> {
        let mut value = 0u16;
        for nth in 0..3 {
            let byte = self.read_u8()?;
            if byte == 0 && nth != 0 {
                anyhow::bail!("alias short_vec length encoding in vote instruction");
            }
            if nth == 2 && (byte & 0x7c) != 0 {
                anyhow::bail!("short_vec length overflows u16 in vote instruction");
            }
            let payload = u16::from(byte & 0x7f);
            value |= payload
                .checked_shl((nth * 7) as u32)
                .context("short_vec length shift overflow")?;
            if byte & 0x80 == 0 {
                return Ok(value as usize);
            }
            anyhow::ensure!(nth < 2, "short_vec length extends past three bytes");
        }
        anyhow::bail!("invalid short_vec length")
    }

    fn read_var_u64(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            anyhow::ensure!(shift < u64::BITS, "vote varint left shift overflow");
            let byte = self.read_u8()?;
            let payload = u64::from(byte & 0x7f);
            anyhow::ensure!(
                payload <= (u64::MAX >> shift),
                "vote varint value overflows u64"
            );
            value |= payload
                .checked_shl(shift)
                .context("vote varint shift overflow")?;
            if byte & 0x80 == 0 {
                anyhow::ensure!(
                    !(byte == 0 && (shift != 0 || value != 0)),
                    "vote varint has invalid trailing zero"
                );
                return Ok(value);
            }
            shift += 7;
        }
    }
}

pub(crate) fn build_no_registry(input: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;
    let blockhash_id_offset = genesis.as_ref().map(|_| 1).unwrap_or(0);

    let archive_path = output_dir.join(ARCHIVE_NO_REGISTRY_FILE);
    let poh_path = output_dir.join(POH_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let archive_file = File::create(&archive_path)
        .with_context(|| format!("create {}", archive_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, archive_file));
    let poh_file =
        File::create(&poh_path).with_context(|| format!("create {}", poh_path.display()))?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, poh_file));
    let blockhash_registry_file = File::create(&blockhash_registry_path)
        .with_context(|| format!("create {}", blockhash_registry_path.display()))?;
    let mut blockhash_registry_writer =
        BufWriter::with_capacity(BUFFER_SIZE, blockhash_registry_file);
    writer.write(&WincodeArchiveV2NoRegistryRecord::Header(
        WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_VERSION,
            flags: ARCHIVE_FLAGS_LEB128 | ARCHIVE_FLAGS_NO_REGISTRY,
        },
    ))?;
    if let Some(genesis) = &genesis {
        writer.write(&WincodeArchiveV2NoRegistryRecord::Genesis(
            no_registry_genesis_record(genesis),
        ))?;
        blockhash_registry_writer
            .write_all(&genesis.genesis_hash)
            .with_context(|| format!("write {}", blockhash_registry_path.display()))?;
    }

    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;
    let mut pending = PendingBlock::default();
    let mut footer = WincodeArchiveV2Footer::default();
    let mut timings = ArchiveV2Timings::default();
    let mut progress = ProgressTracker::new("Archive V2 NoRegistry Write");
    let mut block_offset = 0u64;
    let mut block_id = 0u32;
    let mut block_scratch = Vec::with_capacity(8 << 20);

    while let Some(raw) = scanner.next_node_timed(Some(&mut timings))? {
        footer.car_entries += 1;
        footer.car_payload_bytes += raw.payload_len as u64;
        footer.decoded_node_payload_bytes += raw.payload_len as u64;

        let classify_started = Instant::now();
        match raw.node {
            RawNode::Transaction(tx) => {
                footer.transactions += 1;
                pending.transactions.push(PendingTx {
                    tx,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Entry(entry) => {
                footer.entries += 1;
                pending.entries.push(PendingEntry {
                    entry,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::Rewards(rewards) => {
                footer.rewards += 1;
                anyhow::ensure!(
                    pending.rewards.is_none(),
                    "duplicate rewards node before block"
                );
                pending.rewards = Some(PendingRewards {
                    rewards,
                    payload_len: raw.payload_len,
                });
            }
            RawNode::DataFrame(frame) => {
                footer.dataframes += 1;
                pending.dataframes.insert(frame.cid, frame);
            }
            RawNode::Block(block) => {
                footer.blocks += 1;
                timings.classify += classify_started.elapsed();
                let (record, tx_count, sidecar) = build_no_registry_block_record(
                    &mut pending,
                    block,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut footer,
                    &mut timings,
                )?;
                let encode_started = Instant::now();
                let record = WincodeArchiveV2NoRegistryRecord::Block(record);
                encode_with_scratch(&record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 no-registry frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                writer.write(&WincodeArchiveV2NoRegistryRecord::Index(
                    SplitCompactIndexRecord {
                        slot: pending.last_slot,
                        block_id,
                        block_offset,
                        block_len,
                        runtime_offset: 0,
                        runtime_len: 0,
                        tx_count,
                    },
                ))?;
                poh_writer.write(&WincodeArchiveV2PohRecord {
                    block_id,
                    slot: pending.last_slot,
                    entries: sidecar.poh_entries,
                })?;
                blockhash_registry_writer
                    .write_all(&sidecar.blockhash)
                    .with_context(|| {
                        format!(
                            "write {} block_id {}",
                            blockhash_registry_path.display(),
                            block_id
                        )
                    })?;
                timings.wincode_encode += encode_started.elapsed();
                block_offset += block_len as u64;
                block_id = block_id.wrapping_add(1);
                progress.update_slot(pending.last_slot);
                progress.update(1, tx_count as u64);
                pending.clear();
                continue;
            }
            RawNode::Subset(_) => footer.subset_nodes_ignored += 1,
            RawNode::Epoch(_) => footer.epoch_nodes_ignored += 1,
        }
        timings.classify += classify_started.elapsed();
    }

    let encode_started = Instant::now();
    writer.write(&WincodeArchiveV2NoRegistryRecord::Footer(footer.clone()))?;
    timings.wincode_encode += encode_started.elapsed();
    writer.flush()?;
    poh_writer.flush()?;
    blockhash_registry_writer
        .flush()
        .with_context(|| format!("flush {}", blockhash_registry_path.display()))?;
    progress.final_report();
    info!("Archive V2 no-registry build complete");
    info!("  archive: {}", archive_path.display());
    info!("  poh: {}", poh_path.display());
    info!(
        "  blockhash_registry: {}",
        blockhash_registry_path.display()
    );
    info!(
        "  coverage: entries={} payload_bytes={} decoded_payload_bytes={} tx_raw_fallbacks={} metadata_raw_fallbacks={} rewards_raw_fallbacks={} nonce_recent_blockhashes={}",
        footer.car_entries,
        footer.car_payload_bytes,
        footer.decoded_node_payload_bytes,
        footer.tx_raw_fallbacks,
        footer.metadata_raw_fallbacks,
        footer.rewards_raw_fallbacks,
        footer.nonce_recent_blockhashes
    );
    info!(
        "  timings: scan/decode_node={:.3}s classify={:.3}s dataframe_assemble={:.3}s tx_decode={:.3}s metadata_decode={:.3}s rewards_decode={:.3}s wincode_encode={:.3}s",
        timings.scan_decode_node.as_secs_f64(),
        timings.classify.as_secs_f64(),
        timings.dataframe_assemble.as_secs_f64(),
        timings.tx_decode_compact.as_secs_f64(),
        timings.metadata_decode_compact.as_secs_f64(),
        timings.rewards_decode_compact.as_secs_f64(),
        timings.wincode_encode.as_secs_f64(),
    );
    info!(
        "  archive_v2_stats: tx_reassembled={} metadata_reassembled={} metadata_protobuf_visit={} metadata_owned_fallback={} tx_scratch_max={} metadata_scratch_max={}",
        timings.tx_reassembled,
        timings.metadata_reassembled,
        timings.metadata_protobuf_visit,
        timings.metadata_owned_fallback,
        timings.tx_scratch_max,
        timings.metadata_scratch_max,
    );
    Ok(())
}

pub(crate) fn build_blockhash_registry(input: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let genesis = genesis_epoch0::maybe_load_for_input(input)?;

    let poh_path = output_dir.join(POH_FILE);
    let blockhash_registry_path = output_dir.join(BLOCKHASH_REGISTRY_FILE);
    let poh_tmp = output_dir.join(format!("{POH_FILE}.tmp"));
    let blockhash_tmp = output_dir.join(format!("{BLOCKHASH_REGISTRY_FILE}.tmp"));

    let poh_file =
        File::create(&poh_tmp).with_context(|| format!("create {}", poh_tmp.display()))?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, poh_file));
    let blockhash_file = File::create(&blockhash_tmp)
        .with_context(|| format!("create {}", blockhash_tmp.display()))?;
    let mut blockhash_writer = BufWriter::with_capacity(BUFFER_SIZE, blockhash_file);
    if let Some(genesis) = &genesis {
        blockhash_writer
            .write_all(&genesis.genesis_hash)
            .with_context(|| format!("write {}", blockhash_tmp.display()))?;
    }

    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;
    let mut entries: HashMap<Cid36, FastEntrySummary> = HashMap::new();
    let mut progress = ProgressTracker::new("Blockhash Registry");
    let mut block_id = 0u32;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut entry_nodes = 0u64;
    let mut skipped_nodes = 0u64;
    let mut car_payload_bytes = 0u64;
    let started = Instant::now();

    while let Some(raw) = scanner.next_payload()? {
        car_payload_bytes += raw.payload_len as u64;
        if is_entry_node(raw.payload) {
            let (num_hashes, hash, tx_count) = decode_entry_summary(raw.payload)
                .map_err(|err| anyhow!("{err}"))
                .with_context(|| format!("entry {} decode summary", raw.cid))?;
            let hash = hash_32(hash).with_context(|| format!("entry {} hash", raw.cid))?;
            entries.insert(
                raw.cid,
                FastEntrySummary {
                    num_hashes,
                    hash,
                    tx_count: u32::try_from(tx_count).context("entry tx_count exceeds u32::MAX")?,
                },
            );
            entry_nodes += 1;
        } else if is_block_node(raw.payload) {
            let (slot, entry_cids) = decode_block_entry_cids(raw.payload)
                .with_context(|| format!("block_id {block_id} decode block entries"))?;
            anyhow::ensure!(
                !entry_cids.is_empty(),
                "slot {slot} block_id {block_id} has no PoH entries"
            );

            let mut poh_entries = Vec::with_capacity(entry_cids.len());
            let mut block_txs = 0u64;
            for cid in entry_cids {
                let summary = entries.remove(&cid).ok_or_else(|| {
                    anyhow!("slot {slot} block_id {block_id} missing entry {cid}")
                })?;
                block_txs += summary.tx_count as u64;
                poh_entries.push(CompactPohEntry {
                    num_hashes: summary.num_hashes,
                    hash: summary.hash,
                    tx_count: summary.tx_count,
                });
            }
            let blockhash = poh_entries
                .last()
                .map(|entry| entry.hash)
                .ok_or_else(|| anyhow!("slot {slot} block_id {block_id} has no PoH entries"))?;
            blockhash_writer
                .write_all(&blockhash)
                .with_context(|| format!("write {}", blockhash_tmp.display()))?;
            poh_writer.write(&WincodeArchiveV2PohRecord {
                block_id,
                slot,
                entries: poh_entries,
            })?;

            blocks += 1;
            txs += block_txs;
            progress.update_slot(slot);
            progress.update(1, block_txs);
            block_id = block_id.wrapping_add(1);
        } else {
            skipped_nodes += 1;
        }
    }

    poh_writer.flush()?;
    blockhash_writer
        .flush()
        .with_context(|| format!("flush {}", blockhash_tmp.display()))?;
    std::fs::rename(&poh_tmp, &poh_path)
        .with_context(|| format!("rename {} to {}", poh_tmp.display(), poh_path.display()))?;
    std::fs::rename(&blockhash_tmp, &blockhash_registry_path).with_context(|| {
        format!(
            "rename {} to {}",
            blockhash_tmp.display(),
            blockhash_registry_path.display()
        )
    })?;

    progress.final_report();
    info!(
        "Blockhash registry complete in {:.2}s: blocks={} entries={} tx_refs={} skipped_nodes={} dangling_entries={} payload_bytes={} poh={} blockhash_registry={}",
        started.elapsed().as_secs_f64(),
        blocks,
        entry_nodes,
        txs,
        skipped_nodes,
        entries.len(),
        car_payload_bytes,
        poh_path.display(),
        blockhash_registry_path.display()
    );
    Ok(())
}

pub(crate) fn optimize_no_registry(
    input: &Path,
    output_dir: &Path,
    previous_car: Option<&Path>,
    resume: bool,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let previous_tail = load_or_build_previous_tail(output_dir, previous_car, resume)?;

    let registry_path = output_dir.join(REGISTRY_FILE);
    if resume && crate::file_nonempty(&registry_path) {
        info!(
            "Reusing existing pubkey registry: {}",
            registry_path.display()
        );
    } else {
        build_no_registry_pubkey_registry(input, &registry_path)?;
    }

    let store = KeyStore::load(&registry_path)?;
    let key_count = store.len();
    let key_index = KeyIndex::build(store.keys);
    info!("Archive V2 pubkey registry loaded: {} keys", key_count);

    let input_dir = input.parent().unwrap_or_else(|| Path::new("."));
    let poh_path = copy_required_sidecar(input_dir, output_dir, POH_FILE)?;
    let blockhash_registry_path =
        copy_required_sidecar(input_dir, output_dir, BLOCKHASH_REGISTRY_FILE)?;
    let blockhashes = load_blockhash_registry_plain(&blockhash_registry_path)?;

    let archive_path = output_dir.join(ARCHIVE_FILE);
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file = File::create(&archive_path)
        .with_context(|| format!("create {}", archive_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, archive_file));

    let mut rolling_blockhashes = RollingBlockhashIndex::new(ROLLING_BLOCKHASH_CAPACITY);
    rolling_blockhashes.seed_previous_tail(&previous_tail)?;
    let mut progress = ProgressTracker::new("Archive V2 Optimize");
    let mut block_offset = 0u64;
    let mut block_id = 0u32;
    let mut blockhash_id_offset = 0u32;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut records = 0u64;
    let mut nonce_recent_blockhashes = 0u64;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2NoRegistryRecord>()? {
        records += 1;
        match record {
            WincodeArchiveV2NoRegistryRecord::Header(header) => {
                anyhow::ensure!(
                    header.version == WINCODE_ARCHIVE_V2_VERSION,
                    "unsupported no-registry archive version {}",
                    header.version
                );
                writer.write(&WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_VERSION,
                    flags: ARCHIVE_FLAGS_LEB128,
                }))?;
            }
            WincodeArchiveV2NoRegistryRecord::Genesis(genesis) => {
                anyhow::ensure!(
                    block_id == 0,
                    "genesis record appeared after archive blocks had started"
                );
                anyhow::ensure!(
                    blockhashes.first() == Some(&genesis.genesis_hash),
                    "blockhash registry does not start with no-registry genesis hash"
                );
                blockhash_id_offset = 1;
                rolling_blockhashes.insert(genesis.genesis_hash, 0, 0)?;
                writer.write(&WincodeArchiveV2Record::Genesis(
                    compact_no_registry_genesis(genesis, &key_index)?,
                ))?;
            }
            WincodeArchiveV2NoRegistryRecord::Block(block) => {
                let slot = block.header.compact.slot;
                let blockhash_index = block_id as usize + blockhash_id_offset as usize;
                let current_blockhash = *blockhashes.get(blockhash_index).with_context(|| {
                    format!(
                        "missing blockhash registry entry for blockhash id {blockhash_index} slot {slot}"
                    )
                })?;
                rolling_blockhashes.prune_for_slot(slot)?;
                let (record, tx_count) = optimize_no_registry_block(
                    block,
                    &key_index,
                    &rolling_blockhashes,
                    block_id.saturating_add(blockhash_id_offset),
                    &mut nonce_recent_blockhashes,
                )
                .with_context(|| format!("slot {slot} optimize block"))?;

                let block_record = WincodeArchiveV2Record::Block(record);
                encode_with_scratch(&block_record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 optimized frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                writer.write(&WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                    slot,
                    block_id,
                    block_offset,
                    block_len,
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count,
                }))?;

                let current_block_id = i32::try_from(block_id.saturating_add(blockhash_id_offset))
                    .context("blockhash id exceeds i32::MAX")?;
                rolling_blockhashes.insert(current_blockhash, current_block_id, slot)?;
                block_offset += block_len as u64;
                block_id = block_id.wrapping_add(1);
                blocks += 1;
                txs += tx_count as u64;
                progress.update_slot(slot);
                progress.update(1, tx_count as u64);
            }
            WincodeArchiveV2NoRegistryRecord::Index(_) => {}
            WincodeArchiveV2NoRegistryRecord::Footer(mut footer) => {
                footer.nonce_recent_blockhashes += nonce_recent_blockhashes;
                writer.write(&WincodeArchiveV2Record::Footer(footer))?;
            }
        }
    }

    anyhow::ensure!(
        blocks as usize + blockhash_id_offset as usize == blockhashes.len(),
        "blockhash registry count {} does not match archive blocks {} plus genesis offset {}",
        blockhashes.len(),
        blocks,
        blockhash_id_offset
    );
    writer.flush()?;
    progress.final_report();
    info!(
        "Archive V2 optimize complete in {:.2}s: records={} blocks={} txs={} prev_tail={} nonce_recent_blockhashes={} archive={} registry={} poh={} blockhash_registry={}",
        started.elapsed().as_secs_f64(),
        records,
        blocks,
        txs,
        previous_tail.len(),
        nonce_recent_blockhashes,
        archive_path.display(),
        registry_path.display(),
        poh_path.display(),
        blockhash_registry_path.display()
    );
    Ok(())
}

fn copy_required_sidecar(input_dir: &Path, output_dir: &Path, file_name: &str) -> Result<PathBuf> {
    let source = input_dir.join(file_name);
    anyhow::ensure!(
        crate::file_nonempty(&source),
        "required Archive V2 sidecar missing or empty: {}",
        source.display()
    );
    let dest = output_dir.join(file_name);
    if source != dest {
        std::fs::copy(&source, &dest)
            .with_context(|| format!("copy {} to {}", source.display(), dest.display()))?;
    }
    Ok(dest)
}

#[derive(Clone, Copy)]
struct PreviousBlockhash {
    hash: [u8; 32],
    slot: u64,
}

fn load_or_build_previous_tail(
    output_dir: &Path,
    previous_car: Option<&Path>,
    resume: bool,
) -> Result<Vec<PreviousBlockhash>> {
    let Some(previous_car) = previous_car else {
        return Ok(Vec::new());
    };

    if resume && let Some(tail) = read_prev_blockhash_tail(output_dir)? {
        info!(
            "Reusing previous blockhash tail: tail={} path={}",
            tail.len(),
            output_dir.join(PREV_BLOCKHASH_TAIL_FILE).display()
        );
        return Ok(tail);
    }

    if let Some(previous_epoch) = parse_epoch_from_path(previous_car) {
        if let Some(sidecar_dir) = find_previous_epoch_sidecar_dir(output_dir, previous_epoch)? {
            let tail = read_blockhash_tail_from_sidecars(&sidecar_dir, ROLLING_BLOCKHASH_CAPACITY)
                .with_context(|| {
                    format!(
                        "read previous blockhash tail from sidecars {}",
                        sidecar_dir.display()
                    )
                })?;
            write_prev_blockhash_tail(output_dir, &tail)?;
            info!(
                "Loaded previous blockhash tail from sidecars: epoch={} tail={} dir={}",
                previous_epoch,
                tail.len(),
                sidecar_dir.display()
            );
            return Ok(tail);
        }

        if let Some(sidecar_dir) = default_previous_epoch_sidecar_dir(output_dir, previous_epoch) {
            ensure_blockhash_seed_sidecars(previous_car, &sidecar_dir, resume)?;
            let tail = read_blockhash_tail_from_sidecars(&sidecar_dir, ROLLING_BLOCKHASH_CAPACITY)
                .with_context(|| {
                    format!(
                        "read previous blockhash tail from sidecars {}",
                        sidecar_dir.display()
                    )
                })?;
            write_prev_blockhash_tail(output_dir, &tail)?;
            info!(
                "Loaded previous blockhash tail from built sidecars: epoch={} tail={} dir={}",
                previous_epoch,
                tail.len(),
                sidecar_dir.display()
            );
            return Ok(tail);
        }
    }

    let tail = read_blockhash_tail_from_car(previous_car, ROLLING_BLOCKHASH_CAPACITY)
        .with_context(|| {
            format!(
                "read previous blockhash tail from {}",
                previous_car.display()
            )
        })?;
    write_prev_blockhash_tail(output_dir, &tail)?;
    Ok(tail)
}

fn parse_epoch_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_string_lossy();
    let (_, after) = name.split_once("epoch-")?;
    let digits: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

fn find_previous_epoch_sidecar_dir(
    output_dir: &Path,
    previous_epoch: u64,
) -> Result<Option<PathBuf>> {
    let Some(parent) = output_dir.parent() else {
        return Ok(None);
    };
    let marker = format!("epoch-{previous_epoch}");
    let mut candidates = Vec::new();
    for entry in
        std::fs::read_dir(parent).with_context(|| format!("read dir {}", parent.display()))?
    {
        let entry = entry.with_context(|| format!("read dir entry from {}", parent.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.contains(&marker) {
            continue;
        }
        if has_blockhash_seed_sidecars(&path) {
            let has_hot_index = crate::file_nonempty(&path.join(ARCHIVE_V2_BLOCK_INDEX_FILE));
            candidates.push((has_hot_index, name.to_string(), path));
        }
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    Ok(candidates.pop().map(|(_, _, path)| path))
}

fn default_previous_epoch_sidecar_dir(output_dir: &Path, previous_epoch: u64) -> Option<PathBuf> {
    output_dir
        .parent()
        .map(|parent| parent.join(format!("epoch-{previous_epoch}")))
}

fn has_blockhash_seed_sidecars(sidecar_dir: &Path) -> bool {
    crate::file_nonempty(&sidecar_dir.join(BLOCKHASH_REGISTRY_FILE))
        && (crate::file_nonempty(&sidecar_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
            || crate::file_nonempty(&sidecar_dir.join(POH_FILE)))
}

fn ensure_blockhash_seed_sidecars(input: &Path, sidecar_dir: &Path, resume: bool) -> Result<()> {
    if resume && has_blockhash_seed_sidecars(sidecar_dir) {
        info!(
            "Reusing previous epoch blockhash sidecars: {}",
            sidecar_dir.display()
        );
        return Ok(());
    }

    info!(
        "Building previous epoch blockhash sidecars from {} into {}",
        input.display(),
        sidecar_dir.display()
    );
    build_blockhash_registry(input, sidecar_dir)
}

fn read_blockhash_tail_from_sidecars(
    sidecar_dir: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let blockhashes = load_blockhash_registry_plain(&sidecar_dir.join(BLOCKHASH_REGISTRY_FILE))?;
    let hot_index_path = sidecar_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    if crate::file_nonempty(&hot_index_path) {
        return read_blockhash_tail_from_hot_index(&blockhashes, &hot_index_path, max_entries);
    }
    read_blockhash_tail_from_poh_sidecar(&blockhashes, &sidecar_dir.join(POH_FILE), max_entries)
}

fn read_blockhash_tail_from_hot_index(
    blockhashes: &[[u8; 32]],
    index_path: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let index = read_archive_v2_hot_block_index(index_path)?;
    if index.rows.is_empty() {
        return Ok(Vec::new());
    }
    let offset = infer_blockhash_registry_offset(blockhashes.len(), index.rows.len())?;
    let start = index.rows.len().saturating_sub(max_entries);
    let mut tail = Vec::with_capacity(index.rows.len() - start);
    for row in &index.rows[start..] {
        let hash_index = row.block_id as usize + offset;
        let hash = *blockhashes.get(hash_index).with_context(|| {
            format!(
                "{} references missing blockhash index {} for block_id {}",
                index_path.display(),
                hash_index,
                row.block_id
            )
        })?;
        tail.push(PreviousBlockhash {
            hash,
            slot: row.slot,
        });
    }
    Ok(tail)
}

fn read_blockhash_tail_from_poh_sidecar(
    blockhashes: &[[u8; 32]],
    poh_path: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let file = File::open(poh_path).with_context(|| format!("open {}", poh_path.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let mut tail = VecDeque::with_capacity(max_entries);
    let mut rows = 0usize;
    while let Some((_len, record)) = reader.read::<WincodeArchiveV2PohRecord>()? {
        rows += 1;
        tail.push_back((record.block_id, record.slot));
        while tail.len() > max_entries {
            tail.pop_front();
        }
    }
    let offset = infer_blockhash_registry_offset(blockhashes.len(), rows)?;
    tail.into_iter()
        .map(|(block_id, slot)| {
            let hash_index = block_id as usize + offset;
            let hash = *blockhashes.get(hash_index).with_context(|| {
                format!(
                    "{} references missing blockhash index {} for block_id {}",
                    poh_path.display(),
                    hash_index,
                    block_id
                )
            })?;
            Ok(PreviousBlockhash { hash, slot })
        })
        .collect()
}

fn infer_blockhash_registry_offset(blockhash_count: usize, block_count: usize) -> Result<usize> {
    if blockhash_count == block_count {
        Ok(0)
    } else if blockhash_count == block_count + 1 {
        Ok(1)
    } else {
        anyhow::bail!(
            "blockhash registry count {} does not match block count {} with supported genesis offsets",
            blockhash_count,
            block_count
        )
    }
}

fn read_blockhash_tail_from_car(
    input: &Path,
    max_entries: usize,
) -> Result<Vec<PreviousBlockhash>> {
    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;
    let mut entries: HashMap<Cid36, FastEntrySummary> = HashMap::new();
    let mut tail: VecDeque<PreviousBlockhash> = VecDeque::with_capacity(max_entries);
    let mut progress = ProgressTracker::new("Prev Blockhash Seed");
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let started = Instant::now();

    while let Some(raw) = scanner.next_payload()? {
        if is_entry_node(raw.payload) {
            let (num_hashes, hash, tx_count) = decode_entry_summary(raw.payload)
                .map_err(|err| anyhow!("{err}"))
                .with_context(|| format!("entry {} decode summary", raw.cid))?;
            entries.insert(
                raw.cid,
                FastEntrySummary {
                    num_hashes,
                    hash: hash_32(hash).with_context(|| format!("entry {} hash", raw.cid))?,
                    tx_count: u32::try_from(tx_count).context("entry tx_count exceeds u32::MAX")?,
                },
            );
        } else if is_block_node(raw.payload) {
            let (slot, entry_cids) = decode_block_entry_cids(raw.payload)
                .with_context(|| format!("previous seed block #{blocks} decode block entries"))?;
            let mut block_txs = 0u64;
            let mut blockhash = None;
            for cid in entry_cids {
                let summary = entries
                    .remove(&cid)
                    .ok_or_else(|| anyhow!("previous seed slot {slot} missing entry {cid}"))?;
                block_txs += summary.tx_count as u64;
                blockhash = Some(summary.hash);
            }
            let blockhash =
                blockhash.ok_or_else(|| anyhow!("previous seed slot {slot} has no PoH entries"))?;
            tail.push_back(PreviousBlockhash {
                hash: blockhash,
                slot,
            });
            while tail.len() > max_entries {
                tail.pop_front();
            }
            blocks += 1;
            txs += block_txs;
            progress.update_slot(slot);
            progress.update(1, block_txs);
        }
    }

    progress.final_report();
    info!(
        "Previous blockhash seed complete in {:.2}s: blocks={} tx_refs={} tail={} dangling_entries={} input={}",
        started.elapsed().as_secs_f64(),
        blocks,
        txs,
        tail.len(),
        entries.len(),
        input.display()
    );
    Ok(tail.into_iter().collect())
}

fn read_prev_blockhash_tail(output_dir: &Path) -> Result<Option<Vec<PreviousBlockhash>>> {
    let path = output_dir.join(PREV_BLOCKHASH_TAIL_FILE);
    let Ok(mut file) = File::open(&path) else {
        return Ok(None);
    };
    let len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(None);
    }
    if len % 40 != 0 {
        if len % 32 == 0 {
            info!(
                "Ignoring legacy hash-only previous blockhash tail without slots: {}",
                path.display()
            );
            return Ok(None);
        }
        anyhow::bail!(
            "malformed previous blockhash tail {}: length {} is not a multiple of 40",
            path.display(),
            len
        );
    }

    let mut tail = Vec::with_capacity((len / 40) as usize);
    let mut row = [0u8; 40];
    loop {
        match file.read_exact(&mut row) {
            Ok(()) => {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&row[..32]);
                let slot = u64::from_le_bytes(row[32..40].try_into().unwrap());
                tail.push(PreviousBlockhash { hash, slot });
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
    Ok(Some(tail))
}

fn write_prev_blockhash_tail(output_dir: &Path, tail: &[PreviousBlockhash]) -> Result<()> {
    let path = output_dir.join(PREV_BLOCKHASH_TAIL_FILE);
    let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    for item in tail {
        writer
            .write_all(&item.hash)
            .with_context(|| format!("write {}", path.display()))?;
        writer
            .write_all(&item.slot.to_le_bytes())
            .with_context(|| format!("write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn build_no_registry_pubkey_registry(input: &Path, registry_path: &Path) -> Result<()> {
    info!(
        "Building Archive V2 pubkey registry from {}",
        input.display()
    );
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let mut counter = ArchivePubkeyCounter::new(8_000_000);
    let mut progress = ProgressTracker::new("Archive V2 Registry");
    let started = Instant::now();
    let mut blocks = 0u64;
    let mut txs = 0u64;

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2NoRegistryRecord>()? {
        match record {
            WincodeArchiveV2NoRegistryRecord::Block(block) => {
                let tx_count = count_no_registry_block_pubkeys(&block, &mut counter)
                    .with_context(|| format!("slot {} count pubkeys", block.header.compact.slot))?;
                blocks += 1;
                txs += tx_count;
                progress.update_slot(block.header.compact.slot);
                progress.update(1, tx_count);
            }
            WincodeArchiveV2NoRegistryRecord::Header(header) => {
                anyhow::ensure!(
                    header.version == WINCODE_ARCHIVE_V2_VERSION,
                    "unsupported no-registry archive version {}",
                    header.version
                );
            }
            WincodeArchiveV2NoRegistryRecord::Genesis(genesis) => {
                count_no_registry_genesis_pubkeys(&genesis, &mut counter);
            }
            WincodeArchiveV2NoRegistryRecord::Index(_)
            | WincodeArchiveV2NoRegistryRecord::Footer(_) => {}
        }
    }

    progress.final_report();
    let mut items: Vec<([u8; 32], u32)> = counter.counts.into_iter().collect();
    items.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    let mut keys: Vec<[u8; 32]> = items.into_iter().map(|(key, _)| key).collect();

    const BUILTIN_PROGRAM_KEYS: &[Pubkey] = &[solana_pubkey::pubkey!(
        "ComputeBudget111111111111111111111111111111"
    )];
    for builtin in BUILTIN_PROGRAM_KEYS {
        let builtin = builtin.to_bytes();
        if !keys.iter().any(|value| value == &builtin) {
            keys.insert(0, builtin);
        }
    }

    write_registry(registry_path, &keys)
        .with_context(|| format!("write {}", registry_path.display()))?;
    info!(
        "Archive V2 pubkey registry built in {:.2}s: blocks={} txs={} keys={} path={}",
        started.elapsed().as_secs_f64(),
        blocks,
        txs,
        keys.len(),
        registry_path.display()
    );
    Ok(())
}

fn count_no_registry_block_pubkeys(
    block: &WincodeArchiveV2NoRegistryBlock,
    counter: &mut ArchivePubkeyCounter,
) -> Result<u64> {
    if let Some(rewards) = &block.header.rewards {
        count_no_registry_rewards_pubkeys(rewards, counter)?;
    }

    for (tx_index, tx) in block.txs.iter().enumerate() {
        let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
            anyhow::bail!(
                "slot {} tx#{tx_index} raw transaction payload in no-registry archive",
                block.header.compact.slot
            );
        };
        count_no_registry_tx_pubkeys(value, counter);

        if let Some(metadata) = &tx.metadata {
            let WincodeArchiveV2Payload::Decoded { value, .. } = metadata else {
                anyhow::bail!(
                    "slot {} tx#{tx_index} raw metadata payload in no-registry archive",
                    block.header.compact.slot
                );
            };
            count_no_registry_meta_pubkeys(value, counter);
        }
    }

    Ok(block.txs.len() as u64)
}

fn count_no_registry_tx_pubkeys(
    tx: &WincodeArchiveV2NoRegistryTx,
    counter: &mut ArchivePubkeyCounter,
) {
    match &tx.message {
        WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
            for key in &message.account_keys {
                counter.add32(key);
            }
        }
        WincodeArchiveV2NoRegistryMessage::V0(message) => {
            for key in &message.account_keys {
                counter.add32(key);
            }
            for lookup in &message.address_table_lookups {
                counter.add32(&lookup.account_key);
            }
        }
    }
}

fn count_no_registry_genesis_pubkeys(
    genesis: &WincodeArchiveV2NoRegistryGenesis,
    counter: &mut ArchivePubkeyCounter,
) {
    for account in genesis.accounts.iter().chain(genesis.reward_pools.iter()) {
        counter.add32(&account.pubkey);
        counter.add32(&account.owner);
    }
    for builtin in &genesis.builtins {
        counter.add32(&builtin.pubkey);
    }
}

fn count_no_registry_meta_pubkeys(
    meta: &WincodeArchiveV2NoRegistryMeta,
    counter: &mut ArchivePubkeyCounter,
) {
    for key in &meta.loaded_writable_addresses {
        counter.add32(key);
    }
    for key in &meta.loaded_readonly_addresses {
        counter.add32(key);
    }
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Some(key) = &balance.mint {
            counter.add32(key);
        }
        if let Some(key) = &balance.owner {
            counter.add32(key);
        }
        if let Some(key) = &balance.program_id {
            counter.add32(key);
        }
    }
    for reward in &meta.rewards {
        counter.add32(&reward.pubkey);
    }
    if let Some(return_data) = &meta.return_data {
        counter.add32(&return_data.program_id);
    }
}

fn count_no_registry_rewards_pubkeys(
    rewards: &WincodeArchiveV2NoRegistryRewards,
    counter: &mut ArchivePubkeyCounter,
) -> Result<()> {
    anyhow::ensure!(
        rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
        "raw rewards fallback in no-registry archive"
    );
    let Some(decoded) = &rewards.decoded else {
        anyhow::bail!("rewards record present without decoded rewards");
    };
    for reward in decoded {
        counter.add32(&reward.pubkey);
    }
    Ok(())
}

fn optimize_no_registry_block(
    block: WincodeArchiveV2NoRegistryBlock,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    nonce_recent_blockhashes: &mut u64,
) -> Result<(WincodeArchiveV2Block, u32)> {
    let slot = block.header.compact.slot;
    let rewards = block
        .header
        .rewards
        .as_ref()
        .map(|rewards| optimize_no_registry_rewards(rewards, key_index))
        .transpose()?;
    let mut compact = block.header.compact;
    compact.blockhash = block_id;
    compact.previous_blockhash = block_id.saturating_sub(1);
    compact.poh_entries.clear();
    compact.rewards = None;

    let mut txs = Vec::with_capacity(block.txs.len());
    for (tx_index, tx) in block.txs.into_iter().enumerate() {
        txs.push(optimize_no_registry_transaction(
            slot,
            tx_index,
            tx,
            key_index,
            rolling_blockhashes,
            nonce_recent_blockhashes,
        )?);
    }
    let tx_count = u32::try_from(txs.len()).context("transaction count exceeds u32::MAX")?;

    Ok((
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader { compact, rewards },
            txs,
        },
        tx_count,
    ))
}

fn optimize_no_registry_transaction(
    slot: u64,
    tx_index: usize,
    tx: WincodeArchiveV2NoRegistryTransaction,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<WincodeArchiveV2Transaction> {
    let tx_payload = match tx.tx {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            WincodeArchiveV2Payload::Decoded {
                source_len,
                value: optimize_no_registry_tx(
                    slot,
                    tx_index,
                    value,
                    key_index,
                    rolling_blockhashes,
                    nonce_recent_blockhashes,
                )?,
            }
        }
        WincodeArchiveV2Payload::Raw { error, .. } => {
            anyhow::bail!("slot {slot} tx#{tx_index} raw transaction payload: {error}");
        }
    };

    let metadata = tx
        .metadata
        .map(
            |payload| -> Result<WincodeArchiveV2Payload<blockzilla_format::CompactMetaV1>> {
                match payload {
                    WincodeArchiveV2Payload::Decoded { source_len, value } => {
                        Ok(WincodeArchiveV2Payload::Decoded {
                            source_len,
                            value: optimize_no_registry_meta(slot, tx_index, value, key_index)?,
                        })
                    }
                    WincodeArchiveV2Payload::Raw { error, .. } => {
                        anyhow::bail!("slot {slot} tx#{tx_index} raw metadata payload: {error}");
                    }
                }
            },
        )
        .transpose()?;

    Ok(WincodeArchiveV2Transaction {
        tx_index: tx.tx_index,
        tx: tx_payload,
        metadata,
    })
}

fn optimize_no_registry_tx(
    slot: u64,
    tx_index: usize,
    tx: WincodeArchiveV2NoRegistryTx,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactTransaction> {
    let message = match tx.message {
        WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
            OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                header: message.header,
                account_keys: compact_required_keys(
                    key_index,
                    &message.account_keys,
                    "transaction account key",
                )?,
                recent_blockhash: resolve_recent_blockhash(
                    rolling_blockhashes,
                    &message.recent_blockhash,
                    slot,
                    tx_index,
                    nonce_recent_blockhashes,
                )?,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(owned_no_registry_instruction)
                    .collect(),
            })
        }
        WincodeArchiveV2NoRegistryMessage::V0(message) => {
            OwnedCompactMessage::V0(OwnedCompactV0Message {
                header: message.header,
                account_keys: compact_required_keys(
                    key_index,
                    &message.account_keys,
                    "transaction account key",
                )?,
                recent_blockhash: resolve_recent_blockhash(
                    rolling_blockhashes,
                    &message.recent_blockhash,
                    slot,
                    tx_index,
                    nonce_recent_blockhashes,
                )?,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(owned_no_registry_instruction)
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .into_iter()
                    .map(|lookup| optimize_no_registry_lookup(lookup, key_index))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    };

    Ok(OwnedCompactTransaction {
        signatures: tx.signatures,
        message,
    })
}

fn optimize_no_registry_lookup(
    lookup: WincodeArchiveV2NoRegistryAddressTableLookup,
    key_index: &KeyIndex,
) -> Result<OwnedCompactAddressTableLookup> {
    Ok(OwnedCompactAddressTableLookup {
        account_key: compact_required(key_index, &lookup.account_key, "address table lookup")?,
        writable_indexes: lookup.writable_indexes,
        readonly_indexes: lookup.readonly_indexes,
    })
}

fn owned_no_registry_instruction(
    instruction: WincodeArchiveV2NoRegistryInstruction,
) -> OwnedCompactInstruction {
    OwnedCompactInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts,
        data: instruction.data,
    }
}

fn optimize_no_registry_meta(
    _slot: u64,
    _tx_index: usize,
    meta: WincodeArchiveV2NoRegistryMeta,
    key_index: &KeyIndex,
) -> Result<blockzilla_format::CompactMetaV1> {
    Ok(blockzilla_format::CompactMetaV1 {
        err: meta.err,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions: meta.inner_instructions,
        logs: meta
            .logs
            .map(|logs| blockzilla_format::parse_logs(&logs, key_index)),
        pre_token_balances: meta
            .pre_token_balances
            .into_iter()
            .map(|balance| optimize_no_registry_token_balance(balance, key_index))
            .collect::<Result<Vec<_>>>()?,
        post_token_balances: meta
            .post_token_balances
            .into_iter()
            .map(|balance| optimize_no_registry_token_balance(balance, key_index))
            .collect::<Result<Vec<_>>>()?,
        rewards: meta
            .rewards
            .into_iter()
            .map(|reward| optimize_no_registry_reward(&reward, key_index))
            .collect::<Result<Vec<_>>>()?,
        loaded_writable_addresses: meta
            .loaded_writable_addresses
            .into_iter()
            .map(|key| compact_required(key_index, &key, "loaded writable address"))
            .collect::<Result<Vec<_>>>()?,
        loaded_readonly_addresses: meta
            .loaded_readonly_addresses
            .into_iter()
            .map(|key| compact_required(key_index, &key, "loaded readonly address"))
            .collect::<Result<Vec<_>>>()?,
        return_data: meta
            .return_data
            .map(|return_data| {
                Ok::<CompactReturnData, anyhow::Error>(CompactReturnData {
                    program_id: compact_required(
                        key_index,
                        &return_data.program_id,
                        "return data program id",
                    )?,
                    data: return_data.data,
                })
            })
            .transpose()?,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn optimize_no_registry_token_balance(
    balance: WincodeArchiveV2NoRegistryTokenBalance,
    key_index: &KeyIndex,
) -> Result<CompactTokenBalance> {
    Ok(CompactTokenBalance {
        account_index: balance.account_index,
        mint: balance
            .mint
            .map(|key| compact_required(key_index, &key, "token balance mint"))
            .transpose()?,
        owner: balance
            .owner
            .map(|key| compact_required(key_index, &key, "token balance owner"))
            .transpose()?,
        program_id: balance
            .program_id
            .map(|key| compact_required(key_index, &key, "token balance program id"))
            .transpose()?,
        amount: balance.amount,
        decimals: balance.decimals,
    })
}

fn optimize_no_registry_reward(
    reward: &WincodeArchiveV2NoRegistryReward,
    key_index: &KeyIndex,
) -> Result<CompactReward> {
    Ok(CompactReward {
        pubkey: compact_required(key_index, &reward.pubkey, "reward pubkey")?,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission,
    })
}

fn optimize_no_registry_rewards(
    rewards: &WincodeArchiveV2NoRegistryRewards,
    key_index: &KeyIndex,
) -> Result<WincodeArchiveV2Rewards> {
    anyhow::ensure!(
        rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
        "raw rewards fallback in no-registry archive"
    );
    let Some(decoded) = &rewards.decoded else {
        anyhow::bail!("rewards record present without decoded rewards");
    };
    Ok(WincodeArchiveV2Rewards {
        source_len: rewards.source_len,
        num_partitions: rewards.num_partitions,
        decoded: Some(
            decoded
                .iter()
                .map(|reward| optimize_no_registry_reward(reward, key_index))
                .collect::<Result<Vec<_>>>()?,
        ),
        raw_fallback: None,
        decode_error: None,
    })
}

fn compact_required_keys(
    key_index: &KeyIndex,
    keys: &[[u8; 32]],
    label: &str,
) -> Result<Vec<CompactPubkey>> {
    keys.iter()
        .map(|key| compact_required(key_index, key, label))
        .collect()
}

fn compact_required(key_index: &KeyIndex, key: &[u8; 32], label: &str) -> Result<CompactPubkey> {
    key_index
        .lookup(key)
        .map(CompactPubkey::id)
        .ok_or_else(|| anyhow!("pubkey registry missing {label}: {}", hex32(key)))
}

fn blockhash_id_offset_for_genesis(
    genesis: &Option<GenesisArchive>,
    blockhashes: &[[u8; 32]],
) -> Result<u32> {
    let Some(genesis) = genesis else {
        return Ok(0);
    };
    anyhow::ensure!(
        blockhashes.first() == Some(&genesis.genesis_hash),
        "epoch-0 blockhash registry does not start with genesis hash {}",
        hex32(&genesis.genesis_hash)
    );
    Ok(1)
}

fn compact_genesis_record(
    archive: &GenesisArchive,
    key_index: &KeyIndex,
) -> Result<WincodeArchiveV2Genesis> {
    let genesis = &archive.genesis;
    Ok(WincodeArchiveV2Genesis {
        genesis_hash: archive.genesis_hash,
        genesis_bin_len: archive.genesis_bin_len as u64,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        poh_params: WincodeArchiveV2GenesisPohParams {
            tick_duration_secs: genesis.poh_params.tick_duration_secs,
            tick_duration_nanos: genesis.poh_params.tick_duration_nanos,
            tick_count: genesis.poh_params.tick_count,
            hashes_per_tick: genesis.poh_params.hashes_per_tick,
        },
        fees: WincodeArchiveV2GenesisFeeParams {
            target_lamports_per_sig: genesis.fees.target_lamports_per_sig,
            target_sigs_per_slot: genesis.fees.target_sigs_per_slot,
            min_lamports_per_sig: genesis.fees.min_lamports_per_sig,
            max_lamports_per_sig: genesis.fees.max_lamports_per_sig,
            burn_percent: genesis.fees.burn_percent,
        },
        rent: WincodeArchiveV2GenesisRentParams {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        inflation: WincodeArchiveV2GenesisInflationParams {
            initial: genesis.inflation.initial,
            terminal: genesis.inflation.terminal,
            taper: genesis.inflation.taper,
            foundation: genesis.inflation.foundation,
            foundation_term: genesis.inflation.foundation_term,
            padding: genesis.inflation.padding,
        },
        epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
            warmup: genesis.epoch_schedule.warmup,
            first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
            first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        },
        accounts: compact_genesis_accounts(&genesis.accounts, key_index, "genesis account")?,
        builtins: genesis
            .builtins
            .iter()
            .enumerate()
            .map(|(index, builtin)| {
                Ok(WincodeArchiveV2GenesisBuiltin {
                    key: builtin.key.clone(),
                    pubkey: compact_required(
                        key_index,
                        &builtin.pubkey,
                        &format!("genesis builtin #{index} pubkey"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        reward_pools: compact_genesis_accounts(
            &genesis.reward_pools,
            key_index,
            "genesis reward pool",
        )?,
    })
}

fn compact_genesis_accounts(
    accounts: &[GenesisAccountEntry],
    key_index: &KeyIndex,
    label: &str,
) -> Result<Vec<WincodeArchiveV2GenesisAccount>> {
    accounts
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            Ok(WincodeArchiveV2GenesisAccount {
                pubkey: compact_required(
                    key_index,
                    &entry.pubkey,
                    &format!("{label} #{index} pubkey"),
                )?,
                lamports: entry.account.lamports,
                owner: compact_required(
                    key_index,
                    &entry.account.owner,
                    &format!("{label} #{index} owner"),
                )?,
                executable: entry.account.executable,
                rent_epoch: entry.account.rent_epoch,
                data: entry.account.data.clone(),
            })
        })
        .collect()
}

fn no_registry_genesis_record(archive: &GenesisArchive) -> WincodeArchiveV2NoRegistryGenesis {
    let genesis = &archive.genesis;
    WincodeArchiveV2NoRegistryGenesis {
        genesis_hash: archive.genesis_hash,
        genesis_bin_len: archive.genesis_bin_len as u64,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        poh_params: WincodeArchiveV2GenesisPohParams {
            tick_duration_secs: genesis.poh_params.tick_duration_secs,
            tick_duration_nanos: genesis.poh_params.tick_duration_nanos,
            tick_count: genesis.poh_params.tick_count,
            hashes_per_tick: genesis.poh_params.hashes_per_tick,
        },
        fees: WincodeArchiveV2GenesisFeeParams {
            target_lamports_per_sig: genesis.fees.target_lamports_per_sig,
            target_sigs_per_slot: genesis.fees.target_sigs_per_slot,
            min_lamports_per_sig: genesis.fees.min_lamports_per_sig,
            max_lamports_per_sig: genesis.fees.max_lamports_per_sig,
            burn_percent: genesis.fees.burn_percent,
        },
        rent: WincodeArchiveV2GenesisRentParams {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        inflation: WincodeArchiveV2GenesisInflationParams {
            initial: genesis.inflation.initial,
            terminal: genesis.inflation.terminal,
            taper: genesis.inflation.taper,
            foundation: genesis.inflation.foundation,
            foundation_term: genesis.inflation.foundation_term,
            padding: genesis.inflation.padding,
        },
        epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
            slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
            leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
            warmup: genesis.epoch_schedule.warmup,
            first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
            first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        },
        accounts: no_registry_genesis_accounts(&genesis.accounts),
        builtins: genesis
            .builtins
            .iter()
            .map(|builtin| WincodeArchiveV2NoRegistryGenesisBuiltin {
                key: builtin.key.clone(),
                pubkey: builtin.pubkey,
            })
            .collect(),
        reward_pools: no_registry_genesis_accounts(&genesis.reward_pools),
    }
}

fn no_registry_genesis_accounts(
    accounts: &[GenesisAccountEntry],
) -> Vec<WincodeArchiveV2NoRegistryGenesisAccount> {
    accounts
        .iter()
        .map(|entry| WincodeArchiveV2NoRegistryGenesisAccount {
            pubkey: entry.pubkey,
            lamports: entry.account.lamports,
            owner: entry.account.owner,
            executable: entry.account.executable,
            rent_epoch: entry.account.rent_epoch,
            data: entry.account.data.clone(),
        })
        .collect()
}

fn compact_no_registry_genesis(
    genesis: WincodeArchiveV2NoRegistryGenesis,
    key_index: &KeyIndex,
) -> Result<WincodeArchiveV2Genesis> {
    Ok(WincodeArchiveV2Genesis {
        genesis_hash: genesis.genesis_hash,
        genesis_bin_len: genesis.genesis_bin_len,
        creation_time_unix: genesis.creation_time_unix,
        cluster_id: genesis.cluster_id,
        ticks_per_slot: genesis.ticks_per_slot,
        poh_params: genesis.poh_params,
        fees: genesis.fees,
        rent: genesis.rent,
        inflation: genesis.inflation,
        epoch_schedule: genesis.epoch_schedule,
        accounts: compact_no_registry_genesis_accounts(
            genesis.accounts,
            key_index,
            "genesis account",
        )?,
        builtins: genesis
            .builtins
            .into_iter()
            .enumerate()
            .map(|(index, builtin)| {
                Ok(WincodeArchiveV2GenesisBuiltin {
                    key: builtin.key,
                    pubkey: compact_required(
                        key_index,
                        &builtin.pubkey,
                        &format!("genesis builtin #{index} pubkey"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        reward_pools: compact_no_registry_genesis_accounts(
            genesis.reward_pools,
            key_index,
            "genesis reward pool",
        )?,
    })
}

fn compact_no_registry_genesis_accounts(
    accounts: Vec<WincodeArchiveV2NoRegistryGenesisAccount>,
    key_index: &KeyIndex,
    label: &str,
) -> Result<Vec<WincodeArchiveV2GenesisAccount>> {
    accounts
        .into_iter()
        .enumerate()
        .map(|(index, account)| {
            Ok(WincodeArchiveV2GenesisAccount {
                pubkey: compact_required(
                    key_index,
                    &account.pubkey,
                    &format!("{label} #{index} pubkey"),
                )?,
                lamports: account.lamports,
                owner: compact_required(
                    key_index,
                    &account.owner,
                    &format!("{label} #{index} owner"),
                )?,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data,
            })
        })
        .collect()
}

fn resolve_recent_blockhash(
    rolling_blockhashes: &RollingBlockhashIndex,
    hash: &[u8; 32],
    slot: u64,
    tx_index: usize,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactRecentBlockhash> {
    let resolved = rolling_blockhashes.resolve_or_nonce(hash, slot, tx_index)?;
    if matches!(resolved, OwnedCompactRecentBlockhash::Nonce(_)) {
        *nonce_recent_blockhashes += 1;
    }
    Ok(resolved)
}

struct RollingBlockhashIndex {
    max_entries: usize,
    map: GxHashMap<[u8; 32], (i32, u64)>,
    order: VecDeque<RollingBlockhashEntry>,
}

/// Staged Archive V2 stores recent blockhashes as producing block ids when they
/// are in the live blockhash window. Durable nonce values are stored inline as
/// raw 32-byte hashes and counted in the footer for later nonce-account checks.
#[derive(Clone, Copy)]
struct RollingBlockhashEntry {
    hash: [u8; 32],
    block_id: i32,
    slot: u64,
}

impl RollingBlockhashIndex {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            map: GxHashMap::with_capacity_and_hasher(max_entries, GxBuildHasher::default()),
            order: VecDeque::with_capacity(max_entries),
        }
    }

    fn seed_previous_tail(&mut self, tail: &[PreviousBlockhash]) -> Result<()> {
        for (index, item) in tail.iter().enumerate() {
            let distance_from_newest =
                i32::try_from(tail.len() - index).context("previous tail exceeds i32::MAX")?;
            self.insert(item.hash, -distance_from_newest, item.slot)?;
        }
        Ok(())
    }

    fn insert(&mut self, hash: [u8; 32], block_id: i32, slot: u64) -> Result<()> {
        anyhow::ensure!(
            !self.map.contains_key(&hash),
            "duplicate blockhash {} at block_id {} slot {}",
            hex32(&hash),
            block_id,
            slot
        );
        self.map.insert(hash, (block_id, slot));
        self.order.push_back(RollingBlockhashEntry {
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
            anyhow::ensure!(
                entry.slot <= current_slot,
                "block slots went backwards: rolling blockhash slot {} is ahead of current slot {}",
                entry.slot,
                current_slot
            );
            if current_slot - entry.slot <= RECENT_BLOCKHASH_SLOT_WINDOW {
                return Ok(());
            }
            self.remove_front();
        }
    }

    fn enforce_capacity(&mut self) {
        while self.order.len() > self.max_entries {
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

    fn resolve_or_nonce(
        &self,
        hash: &[u8; 32],
        slot: u64,
        tx_index: usize,
    ) -> Result<OwnedCompactRecentBlockhash> {
        let Some((block_id, hash_slot)) = self.map.get(hash).copied() else {
            return Ok(OwnedCompactRecentBlockhash::Nonce(*hash));
        };
        anyhow::ensure!(
            hash_slot <= slot,
            "slot {slot} tx#{tx_index} recent_blockhash {} points at future block slot {}",
            hex32(hash),
            hash_slot
        );
        if slot - hash_slot > RECENT_BLOCKHASH_SLOT_WINDOW {
            return Ok(OwnedCompactRecentBlockhash::Nonce(*hash));
        }
        Ok(OwnedCompactRecentBlockhash::Id(block_id))
    }
}

struct ArchivePubkeyCounter {
    counts: GxHashMap<[u8; 32], u32>,
}

impl ArchivePubkeyCounter {
    fn new(capacity: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(capacity, GxBuildHasher::default()),
        }
    }

    fn add32(&mut self, key: &[u8; 32]) {
        let count = self.counts.entry(*key).or_insert(0);
        *count = count.saturating_add(1);
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

pub(crate) fn bench(input: &Path, iterations: usize) -> Result<()> {
    let iterations = iterations.max(1);
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let start = Instant::now();
    let mut bytes = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut records = 0u64;

    for _ in 0..iterations {
        let mut reader = WincodeLeb128FramedReader::new(open_wincode_bench_input(input)?);
        while let Some((len, record)) = reader.read::<WincodeArchiveV2Record>()? {
            records += 1;
            bytes += len as u64;
            if let WincodeArchiveV2Record::Block(block) = record {
                blocks += 1;
                txs += block.txs.len() as u64;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let input_mib_s = if elapsed > 0.0 {
        input_bytes as f64 * iterations as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        txs as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "archive_v2_read iterations={iterations} records={records} blocks={blocks} txs={txs} input_bytes={input_bytes} payload_bytes={bytes} elapsed_s={elapsed:.3} input_MiB_s={input_mib_s:.2} payload_MiB_s={mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}"
    );
    Ok(())
}

pub(crate) fn bench_no_registry(input: &Path, iterations: usize) -> Result<()> {
    let iterations = iterations.max(1);
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let start = Instant::now();
    let mut bytes = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut records = 0u64;

    for _ in 0..iterations {
        let mut reader = WincodeLeb128FramedReader::new(open_wincode_bench_input(input)?);
        while let Some((len, record)) = reader.read::<WincodeArchiveV2NoRegistryRecord>()? {
            records += 1;
            bytes += len as u64;
            if let WincodeArchiveV2NoRegistryRecord::Block(block) = record {
                blocks += 1;
                txs += block.txs.len() as u64;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let input_mib_s = if elapsed > 0.0 {
        input_bytes as f64 * iterations as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        txs as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "archive_v2_no_registry_read iterations={iterations} records={records} blocks={blocks} txs={txs} input_bytes={input_bytes} payload_bytes={bytes} elapsed_s={elapsed:.3} input_MiB_s={input_mib_s:.2} payload_MiB_s={mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}"
    );
    Ok(())
}

fn open_wincode_bench_input(input: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    if input.extension().is_some_and(|ext| ext == "zst") {
        return Ok(Box::new(
            zstd::stream::read::Decoder::new(reader)
                .with_context(|| format!("zstd decode {}", input.display()))?,
        ));
    }
    Ok(Box::new(reader))
}

#[derive(Debug)]
struct ArchiveV2BlockIndex {
    archive_file_bytes: u64,
    rows: Vec<ArchiveV2BlockIndexRow>,
}

#[derive(Debug, Clone, Copy)]
struct ArchiveV2BlockIndexRow {
    block_id: u32,
    slot: u64,
    frame_offset: u64,
    payload_offset: u64,
    payload_len: u32,
    tx_count: u32,
}

#[derive(Debug)]
struct ArchiveV2ZstdBlockIndex {
    blob_file_bytes: u64,
    level: i32,
    flags: u32,
    rows: Vec<ArchiveV2ZstdBlockIndexRow>,
}

#[derive(Debug, Clone, Copy)]
struct ArchiveV2ZstdBlockIndexRow {
    block_id: u32,
    slot: u64,
    compressed_offset: u64,
    compressed_len: u32,
    uncompressed_len: u32,
    tx_count: u32,
}

#[derive(Debug, Default)]
struct ArchiveV2IndexedBenchStats {
    payload_bytes: u64,
    blocks: u64,
    txs: u64,
}

#[derive(Debug, Default)]
struct ArchiveV2ZstdBenchStats {
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    blocks: u64,
    txs: u64,
}

pub(crate) fn build_index(input: &Path, output: Option<&Path>) -> Result<()> {
    anyhow::ensure!(
        input.extension().is_none_or(|ext| ext != "zst"),
        "indexed Archive V2 reads require an uncompressed .wincode file, got {}",
        input.display()
    );

    let output_path = output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_index_path(input));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = BufReader::with_capacity(ARCHIVE_V2_INDEX_SCAN_BUFFER_SIZE, file);
    let mut frame_buf = Vec::with_capacity(256);
    let mut rows = Vec::new();
    let mut pending_block: Option<ArchiveV2BlockIndexRow> = None;
    let mut records = 0u64;
    let mut embedded_indexes = 0u64;
    let mut stale_embedded_indexes = 0u64;
    let mut offset = 0u64;
    let mut expected_logical_block_offset = 0u64;
    let mut progress = ProgressTracker::new("Archive V2 Index");
    let started = Instant::now();

    while let Some((payload_len, prefix_len)) = read_u32_varint_with_encoded_len(&mut reader)? {
        let frame_offset = offset;
        let payload_offset = frame_offset + prefix_len as u64;
        offset = payload_offset;
        anyhow::ensure!(
            payload_len > 0,
            "empty archive v2 frame at offset {frame_offset}"
        );
        let mut tag = [0u8; 1];
        reader
            .read_exact(&mut tag)
            .with_context(|| format!("read frame tag at offset {payload_offset}"))?;
        offset += 1;
        records += 1;

        if tag[0] == WINCODE_ARCHIVE_V2_RECORD_BLOCK_TAG {
            anyhow::ensure!(
                pending_block.is_none(),
                "block at offset {frame_offset} appeared before previous block index"
            );
            let block_id = u32::try_from(rows.len()).context("block_id exceeds u32::MAX")?;
            pending_block = Some(ArchiveV2BlockIndexRow {
                block_id,
                slot: 0,
                frame_offset,
                payload_offset,
                payload_len,
                tx_count: 0,
            });
            reader
                .seek(SeekFrom::Current(i64::from(payload_len - 1)))
                .with_context(|| format!("skip block payload at offset {payload_offset}"))?;
            offset += u64::from(payload_len - 1);
            continue;
        }
        anyhow::ensure!(
            matches!(
                tag[0],
                WINCODE_ARCHIVE_V2_RECORD_HEADER_TAG
                    | WINCODE_ARCHIVE_V2_RECORD_INDEX_TAG
                    | WINCODE_ARCHIVE_V2_RECORD_FOOTER_TAG
                    | WINCODE_ARCHIVE_V2_RECORD_GENESIS_TAG
            ),
            "unknown archive v2 record tag {} at offset {payload_offset}",
            tag[0]
        );

        frame_buf.resize(payload_len as usize, 0);
        frame_buf[0] = tag[0];
        reader
            .read_exact(&mut frame_buf[1..])
            .with_context(|| format!("read frame payload at offset {payload_offset}"))?;
        offset += u64::from(payload_len - 1);
        let record: WincodeArchiveV2Record =
            wincode::config::deserialize(&frame_buf, wincode_leb128_config())
                .with_context(|| format!("decode archive v2 frame at offset {frame_offset}"))?;
        match record {
            WincodeArchiveV2Record::Block(_) => unreachable!("block tag handled before decode"),
            WincodeArchiveV2Record::Index(index) => {
                embedded_indexes += 1;
                let Some(mut row) = pending_block.take() else {
                    anyhow::bail!(
                        "embedded index appeared before any block at offset {frame_offset}"
                    );
                };
                anyhow::ensure!(
                    index.block_id == row.block_id,
                    "embedded index mismatch at offset {frame_offset}: embedded={index:?} physical={row:?}"
                );
                if index.block_len != row.payload_len
                    || index.block_offset != expected_logical_block_offset
                {
                    stale_embedded_indexes += 1;
                }
                row.slot = index.slot;
                row.tx_count = index.tx_count;
                expected_logical_block_offset += row.payload_len as u64;
                progress.update_slot(row.slot);
                progress.update(1, row.tx_count as u64);
                rows.push(row);
            }
            WincodeArchiveV2Record::Header(_) | WincodeArchiveV2Record::Genesis(_) => {
                anyhow::ensure!(
                    pending_block.is_none(),
                    "non-index record at offset {frame_offset} appeared before previous block index"
                );
            }
            WincodeArchiveV2Record::Footer(_) => {
                anyhow::ensure!(
                    pending_block.is_none(),
                    "footer at offset {frame_offset} appeared before previous block index"
                );
            }
        }
    }

    anyhow::ensure!(
        pending_block.is_none(),
        "archive ended before the last block index record"
    );
    anyhow::ensure!(
        offset == archive_file_bytes,
        "archive scan ended at byte {offset}, but file size is {archive_file_bytes}"
    );
    write_archive_v2_block_index(&output_path, archive_file_bytes, &rows)?;
    progress.final_report();
    info!(
        "Archive V2 index complete in {:.2}s: records={} blocks={} embedded_indexes={} stale_embedded_indexes={} archive_bytes={} index={} index_bytes={}",
        started.elapsed().as_secs_f64(),
        records,
        rows.len(),
        embedded_indexes,
        stale_embedded_indexes,
        archive_file_bytes,
        output_path.display(),
        std::fs::metadata(&output_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    );
    Ok(())
}

pub(crate) fn bench_indexed(
    input: &Path,
    index: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (input, index, workers, chunk_size);
        anyhow::bail!("indexed Archive V2 benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_indexed_unix(input, index, workers, chunk_size)
    }
}

#[cfg(unix)]
fn bench_indexed_unix(
    input: &Path,
    index: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    anyhow::ensure!(
        input.extension().is_none_or(|ext| ext != "zst"),
        "indexed Archive V2 reads require an uncompressed .wincode file, got {}",
        input.display()
    );
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_index_path(input));
    let index = read_archive_v2_block_index(&index_path)?;
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        archive_file_bytes == index.archive_file_bytes,
        "index was built for {} bytes, but archive is {} bytes",
        index.archive_file_bytes,
        archive_file_bytes
    );

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(
            move || -> Result<ArchiveV2IndexedBenchStats> {
                let mut stats = ArchiveV2IndexedBenchStats::default();
                let mut frame_buf = Vec::with_capacity(2 << 20);
                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    for row in &rows[start..end] {
                        frame_buf.resize(row.payload_len as usize, 0);
                        read_exact_at(&file, &mut frame_buf, row.payload_offset).with_context(
                            || {
                                format!(
                                    "worker {worker_id} read block_id {} at payload offset {}",
                                    row.block_id, row.payload_offset
                                )
                            },
                        )?;
                        let record: WincodeArchiveV2Record =
                            wincode::config::deserialize(&frame_buf, wincode_leb128_config())
                                .with_context(|| {
                                    format!("worker {worker_id} decode block_id {}", row.block_id)
                                })?;
                        let WincodeArchiveV2Record::Block(block) = record else {
                            anyhow::bail!(
                                "worker {worker_id} expected block at payload offset {}, got different record",
                                row.payload_offset
                            );
                        };
                        anyhow::ensure!(
                            block.header.compact.slot == row.slot,
                            "worker {worker_id} block_id {} slot mismatch: decoded={} index={}",
                            row.block_id,
                            block.header.compact.slot,
                            row.slot
                        );
                        anyhow::ensure!(
                            block.txs.len() == row.tx_count as usize,
                            "worker {worker_id} block_id {} tx count mismatch: decoded={} index={}",
                            row.block_id,
                            block.txs.len(),
                            row.tx_count
                        );
                        stats.payload_bytes += row.payload_len as u64;
                        stats.blocks += 1;
                        stats.txs += block.txs.len() as u64;
                    }
                }
                Ok(stats)
            },
        ));
    }

    let mut stats = ArchiveV2IndexedBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("indexed Archive V2 benchmark worker panicked"))??;
        stats.payload_bytes += worker_stats.payload_bytes;
        stats.blocks += worker_stats.blocks;
        stats.txs += worker_stats.txs;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        stats.payload_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        stats.blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        stats.txs as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "archive_v2_indexed_read workers={workers} chunk_size={chunk_size} index_rows={} blocks={} txs={} archive_bytes={archive_file_bytes} payload_bytes={} elapsed_s={elapsed:.3} payload_MiB_s={mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}",
        rows.len(),
        stats.blocks,
        stats.txs,
        stats.payload_bytes
    );
    Ok(())
}

pub(crate) fn repack_zstd_blocks(
    input: &Path,
    output_dir: &Path,
    level: i32,
    dict: Option<&Path>,
    max_blocks: Option<u64>,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let blocks_path = output_dir.join(ARCHIVE_ZSTD_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_ZSTD_INDEX_FILE);
    let meta_path = output_dir.join(ARCHIVE_ZSTD_META_FILE);
    let dict_bytes = load_optional_dict(dict)?;
    let flags = if dict_bytes.is_some() {
        ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY
    } else {
        0
    };

    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        BUFFER_SIZE,
        open_wincode_bench_input(input)?,
    ));
    let blocks_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(BUFFER_SIZE, blocks_file);
    let meta_file =
        File::create(&meta_path).with_context(|| format!("create {}", meta_path.display()))?;
    let mut meta_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, meta_file));
    let mut compressor = if let Some(dict_bytes) = dict_bytes.as_deref() {
        zstd::bulk::Compressor::with_dictionary(level, dict_bytes)
            .context("create zstd dictionary compressor")?
    } else {
        zstd::bulk::Compressor::new(level).context("create zstd compressor")?
    };

    let mut rows = Vec::new();
    let mut progress = ProgressTracker::new("Archive V2 Zstd Repack");
    let mut records = 0u64;
    let mut embedded_indexes_skipped = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut blob_offset = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut compressed_bytes = 0u64;
    let mut largest_row: Option<ArchiveV2ZstdBlockIndexRow> = None;
    let mut block_scratch = Vec::with_capacity(8 << 20);
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        records += 1;
        match record {
            WincodeArchiveV2Record::Header(_)
            | WincodeArchiveV2Record::Genesis(_)
            | WincodeArchiveV2Record::Footer(_) => {
                meta_writer.write(&record)?;
            }
            WincodeArchiveV2Record::Index(_) => {
                embedded_indexes_skipped += 1;
            }
            WincodeArchiveV2Record::Block(block) => {
                let slot = block.header.compact.slot;
                let tx_count =
                    u32::try_from(block.txs.len()).context("block tx count exceeds u32::MAX")?;
                let block_record = WincodeArchiveV2Record::Block(block);
                encode_with_scratch(&block_record, &mut block_scratch)?;
                let uncompressed_len = u32::try_from(block_scratch.len())
                    .context("archive v2 block payload exceeds u32::MAX")?;
                let compressed = compressor
                    .compress(&block_scratch)
                    .with_context(|| format!("zstd compress block_id {}", rows.len()))?;
                let compressed_len = u32::try_from(compressed.len())
                    .context("compressed archive v2 block exceeds u32::MAX")?;
                blocks_writer
                    .write_all(&compressed)
                    .with_context(|| format!("write {}", blocks_path.display()))?;

                let block_id = u32::try_from(rows.len()).context("block id exceeds u32::MAX")?;
                let row = ArchiveV2ZstdBlockIndexRow {
                    block_id,
                    slot,
                    compressed_offset: blob_offset,
                    compressed_len,
                    uncompressed_len,
                    tx_count,
                };
                if largest_row
                    .as_ref()
                    .is_none_or(|largest| row.uncompressed_len > largest.uncompressed_len)
                {
                    largest_row = Some(row);
                }
                rows.push(row);
                blob_offset += compressed_len as u64;
                uncompressed_bytes += uncompressed_len as u64;
                compressed_bytes += compressed_len as u64;
                blocks += 1;
                txs += tx_count as u64;
                progress.update_slot(slot);
                progress.update(1, tx_count as u64);
                if max_blocks.is_some_and(|limit| blocks >= limit) {
                    break;
                }
            }
        }
    }

    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    meta_writer.flush()?;
    write_archive_v2_zstd_block_index(&index_path, blob_offset, level, flags, &rows)?;
    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let ratio = if uncompressed_bytes > 0 {
        compressed_bytes as f64 * 100.0 / uncompressed_bytes as f64
    } else {
        0.0
    };
    if let Some(row) = largest_row {
        info!(
            "Archive V2 zstd-block largest block: block_id={} slot={} txs={} uncompressed_len={} compressed_len={}",
            row.block_id, row.slot, row.tx_count, row.uncompressed_len, row.compressed_len
        );
    }
    info!(
        "Archive V2 zstd-block repack complete in {:.2}s: records={} blocks={} txs={} embedded_indexes_skipped={} level={} dict={} max_blocks={:?} uncompressed_bytes={} compressed_bytes={} ratio_pct={:.2} blocks_file={} index={} meta={}",
        elapsed,
        records,
        blocks,
        txs,
        embedded_indexes_skipped,
        level,
        dict.map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
        max_blocks,
        uncompressed_bytes,
        compressed_bytes,
        ratio,
        blocks_path.display(),
        index_path.display(),
        meta_path.display()
    );
    Ok(())
}

pub(crate) fn bench_zstd_blocks(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (input, index, dict, workers, chunk_size);
        anyhow::bail!("zstd-block Archive V2 benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_zstd_blocks_unix(input, index, dict, workers, chunk_size)
    }
}

#[derive(Default)]
struct CarArchiveBenchStats {
    nodes: u64,
    payload_bytes: u64,
    transactions: u64,
    entries: u64,
    blocks: u64,
    rewards: u64,
    dataframes: u64,
    subsets: u64,
    epochs: u64,
}

pub(crate) fn bench_car_archive(
    input: &Path,
    format: CarBenchInputFormat,
    max_blocks: Option<u64>,
) -> Result<()> {
    let compressed = match format {
        CarBenchInputFormat::Auto => input
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zst")),
        CarBenchInputFormat::Car => false,
        CarBenchInputFormat::CarZstd => true,
    };
    let input_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let mut scanner = RawCarScanner::open_with_compression(input, compressed)?;
    scanner.skip_header()?;

    let mut stats = CarArchiveBenchStats::default();
    let mut progress = ProgressTracker::new(if compressed {
        "CAR-ZSTD Read"
    } else {
        "CAR Read"
    });
    let block_limit = max_blocks.unwrap_or(u64::MAX);
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(None)? {
        stats.nodes += 1;
        stats.payload_bytes = stats.payload_bytes.saturating_add(raw.payload_len as u64);
        match raw.node {
            RawNode::Transaction(tx) => {
                stats.transactions += 1;
                progress.update_slot(tx.slot);
                progress.update(0, 1);
            }
            RawNode::Entry(_) => {
                stats.entries += 1;
            }
            RawNode::Block(block) => {
                stats.blocks += 1;
                progress.update_slot(block.slot);
                progress.update(1, 0);
                if stats.blocks >= block_limit {
                    break;
                }
            }
            RawNode::Rewards(_) => {
                stats.rewards += 1;
            }
            RawNode::DataFrame(_) => {
                stats.dataframes += 1;
            }
            RawNode::Subset(_) => {
                stats.subsets += 1;
            }
            RawNode::Epoch(_) => {
                stats.epochs += 1;
            }
        }
    }

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    let full_scan = max_blocks.is_none();
    let input_mib_s = if full_scan && elapsed > 0.0 {
        input_file_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let payload_mib_s = if elapsed > 0.0 {
        stats.payload_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let blocks_s = if elapsed > 0.0 {
        stats.blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        stats.transactions as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "car_archive_read format={} workers=1 full_scan={} nodes={} blocks={} txs={} entries={} rewards={} dataframes={} subsets={} epochs={} input_file_bytes={} payload_bytes={} elapsed_s={elapsed:.3} input_MiB_s={input_mib_s:.2} payload_MiB_s={payload_mib_s:.2} blocks_s={blocks_s:.2} tx_s={tx_s:.2}",
        if compressed { "car-zstd" } else { "car" },
        full_scan,
        stats.nodes,
        stats.blocks,
        stats.transactions,
        stats.entries,
        stats.rewards,
        stats.dataframes,
        stats.subsets,
        stats.epochs,
        input_file_bytes,
        stats.payload_bytes
    );
    Ok(())
}

#[cfg(unix)]
fn bench_zstd_blocks_unix(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        index.blob_file_bytes,
        blob_file_bytes
    );
    let dict_bytes = Arc::new(load_optional_dict(dict)?.unwrap_or_default());
    anyhow::ensure!(
        (index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let level = index.level;
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        let dict_bytes = Arc::clone(&dict_bytes);
        handles.push(thread::spawn(
            move || -> Result<ArchiveV2ZstdBenchStats> {
                let mut stats = ArchiveV2ZstdBenchStats::default();
                let mut compressed_buf = Vec::with_capacity(2 << 20);
                let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
                    .context("create zstd dictionary decompressor")?;
                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    for row in &rows[start..end] {
                        compressed_buf.resize(row.compressed_len as usize, 0);
                        read_exact_at(&file, &mut compressed_buf, row.compressed_offset)
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} read compressed block_id {} at offset {}",
                                    row.block_id, row.compressed_offset
                                )
                            })?;
                        let block_bytes = decompressor
                            .decompress(&compressed_buf, row.uncompressed_len as usize)
                            .with_context(|| {
                                format!("worker {worker_id} zstd decompress block_id {}", row.block_id)
                            })?;
                        anyhow::ensure!(
                            block_bytes.len() == row.uncompressed_len as usize,
                            "worker {worker_id} block_id {} decompressed length mismatch: decoded={} index={}",
                            row.block_id,
                            block_bytes.len(),
                            row.uncompressed_len
                        );
                        let block: ArchiveV2HotBlockBlob = wincode::config::deserialize(
                            &block_bytes,
                            wincode_leb128_config(),
                        )
                        .with_context(|| {
                            format!("worker {worker_id} decode hot block_id {}", row.block_id)
                        })?;
                        anyhow::ensure!(
                            block.header.slot == row.slot,
                            "worker {worker_id} hot block_id {} slot mismatch: decoded={} index={}",
                            row.block_id,
                            block.header.slot,
                            row.slot
                        );
                        let decoded_txs = block.tx_rows.len();
                        anyhow::ensure!(
                            decoded_txs == row.tx_count as usize,
                            "worker {worker_id} block_id {} tx count mismatch: decoded={} index={}",
                            row.block_id,
                            decoded_txs,
                            row.tx_count
                        );
                        stats.compressed_bytes += row.compressed_len as u64;
                        stats.uncompressed_bytes += row.uncompressed_len as u64;
                        stats.blocks += 1;
                        stats.txs += decoded_txs as u64;
                    }
                }
                Ok(stats)
            },
        ));
    }

    let mut stats = ArchiveV2ZstdBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("zstd-block Archive V2 benchmark worker panicked"))??;
        stats.compressed_bytes += worker_stats.compressed_bytes;
        stats.uncompressed_bytes += worker_stats.uncompressed_bytes;
        stats.blocks += worker_stats.blocks;
        stats.txs += worker_stats.txs;
    }

    print_zstd_block_bench(
        "archive_v2_hot_block_read",
        workers,
        chunk_size,
        level,
        blob_file_bytes,
        &stats,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

#[derive(Debug, Default)]
struct HotBlockAccountBenchStats {
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    blocks: u64,
    txs: u64,
    account_refs: u64,
    unique_account_refs: u64,
    raw_pubkeys: u64,
    metadata_decoded: u64,
    metadata_skipped: u64,
    varint_bytes: u64,
    fixed_u32_bytes: u64,
    candidate_blocks: u64,
    candidate_txs: u64,
}

impl HotBlockAccountBenchStats {
    fn merge(&mut self, other: Self) {
        self.compressed_bytes += other.compressed_bytes;
        self.uncompressed_bytes += other.uncompressed_bytes;
        self.blocks += other.blocks;
        self.txs += other.txs;
        self.account_refs += other.account_refs;
        self.unique_account_refs += other.unique_account_refs;
        self.raw_pubkeys += other.raw_pubkeys;
        self.metadata_decoded += other.metadata_decoded;
        self.metadata_skipped += other.metadata_skipped;
        self.varint_bytes += other.varint_bytes;
        self.fixed_u32_bytes += other.fixed_u32_bytes;
        self.candidate_blocks += other.candidate_blocks;
        self.candidate_txs += other.candidate_txs;
    }
}

#[derive(Debug, SchemaRead)]
struct CompactMetaLoadedAddressView {
    _err: Option<SkipBytes>,
    _fee: SkipU64,
    _pre_balances: Vec<SkipU64>,
    _post_balances: Vec<SkipU64>,
    _inner_instructions: Option<Vec<SkipCompactInnerInstructions>>,
    // In the current hot-block payload this sits before loaded addresses. A future
    // metadata prefix should move loaded addresses and inner instructions above logs.
    _logs: Option<CompactLogStream>,
    _pre_token_balances: Vec<SkipCompactTokenBalance>,
    _post_token_balances: Vec<SkipCompactTokenBalance>,
    _rewards: Vec<SkipCompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
}

#[derive(Debug)]
struct SkipU8;

#[derive(Debug)]
struct SkipU32;

#[derive(Debug)]
struct SkipU64;

#[derive(Debug)]
struct SkipI32;

#[derive(Debug)]
struct SkipI64;

macro_rules! impl_skip_primitive {
    ($name:ty, $primitive:ty) => {
        unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for $name {
            type Dst = Self;

            #[inline]
            fn read(reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
                let _ = <$primitive as SchemaRead<'de, C>>::get(reader)?;
                dst.write(Self);
                Ok(())
            }
        }
    };
}

impl_skip_primitive!(SkipU8, u8);
impl_skip_primitive!(SkipU32, u32);
impl_skip_primitive!(SkipU64, u64);
impl_skip_primitive!(SkipI32, i32);
impl_skip_primitive!(SkipI64, i64);

#[derive(Debug)]
struct SkipBytes;

unsafe impl<'de, C: Config> SchemaRead<'de, C> for SkipBytes {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let len = <C::LengthEncoding as SeqLen<C>>::read(reader.by_ref())?;
        let _ = reader.take_scoped(len)?;
        dst.write(Self);
        Ok(())
    }
}

#[derive(Debug)]
struct SkipCompactPubkey;

unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for SkipCompactPubkey {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let id = <u32 as SchemaRead<'de, C>>::get(reader.by_ref())?;
        if id == CompactPubkey::RAW_SENTINEL {
            let _ = reader.take_scoped(32)?;
        }
        dst.write(Self);
        Ok(())
    }
}

#[derive(Debug, SchemaRead)]
struct SkipCompactInnerInstructions {
    _index: SkipU32,
    _instructions: Vec<SkipCompactInnerInstruction>,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactInnerInstruction {
    _program_id_index: SkipU32,
    _accounts: SkipBytes,
    _data: SkipBytes,
    _stack_height: Option<SkipU32>,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactTokenBalance {
    _account_index: SkipU32,
    _mint: Option<SkipCompactPubkey>,
    _owner: Option<SkipCompactPubkey>,
    _program_id: Option<SkipCompactPubkey>,
    _amount: SkipU64,
    _decimals: SkipU8,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactReward {
    _pubkey: SkipCompactPubkey,
    _lamports: SkipI64,
    _post_balance: SkipU64,
    _reward_type: SkipI32,
    _commission: Option<SkipU8>,
}

pub(crate) fn bench_hot_block_accounts(
    input: &Path,
    index: Option<&Path>,
    registry: Option<&Path>,
    target: Option<&str>,
    include_metadata: bool,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (
            input,
            index,
            registry,
            target,
            include_metadata,
            workers,
            chunk_size,
        );
        anyhow::bail!("hot-block account benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_hot_block_accounts_unix(
            input,
            index,
            registry,
            target,
            include_metadata,
            workers,
            chunk_size,
        )
    }
}

#[cfg(unix)]
fn bench_hot_block_accounts_unix(
    input: &Path,
    index: Option<&Path>,
    registry: Option<&Path>,
    target: Option<&str>,
    include_metadata: bool,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed hot blocks are not supported by this prototype"
    );
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
        "{} is a raw-block index, not independent zstd blocks",
        index_path.display()
    );
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        index.blob_file_bytes,
        blob_file_bytes
    );

    let target_id = match target {
        Some(target) => {
            let registry_path = registry.map(Path::to_path_buf).unwrap_or_else(|| {
                input
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            });
            let key_index = KeyIndex::build(
                KeyStore::load(&registry_path)
                    .with_context(|| format!("load registry {}", registry_path.display()))?
                    .keys,
            );
            let target_pubkey = Pubkey::from_str(target)
                .with_context(|| format!("parse target pubkey {target}"))?
                .to_bytes();
            let id = key_index.lookup(&target_pubkey).with_context(|| {
                format!(
                    "target pubkey {target} is not in {}",
                    registry_path.display()
                )
            })?;
            Some(id)
        }
        None => None,
    };

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(
            move || -> Result<HotBlockAccountBenchStats> {
                let mut stats = HotBlockAccountBenchStats::default();
                let mut compressed_buf = Vec::with_capacity(2 << 20);
                let mut ids = Vec::with_capacity(4096);
                let mut decompressor =
                    zstd::bulk::Decompressor::new().context("create zstd decompressor")?;

                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    for row in &rows[start..end] {
                        compressed_buf.resize(row.compressed_len as usize, 0);
                        read_exact_at(&file, &mut compressed_buf, row.compressed_offset)
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} read compressed block_id {} at offset {}",
                                    row.block_id, row.compressed_offset
                                )
                            })?;
                        let block_bytes = decompressor
                            .decompress(&compressed_buf, row.uncompressed_len as usize)
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} zstd decompress block_id {}",
                                    row.block_id
                                )
                            })?;
                        let block: ArchiveV2HotBlockBlob =
                            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                                .with_context(|| {
                                    format!(
                                        "worker {worker_id} decode hot block_id {}",
                                        row.block_id
                                    )
                                })?;
                        anyhow::ensure!(
                            block.header.slot == row.slot
                                && block.tx_rows.len() == row.tx_count as usize,
                            "worker {worker_id} block_id {} validation failed",
                            row.block_id
                        );
                        ids.clear();
                        collect_hot_block_account_ids(
                            &block,
                            include_metadata,
                            &mut ids,
                            &mut stats,
                        )
                        .with_context(|| {
                            format!(
                                "worker {worker_id} collect accounts block_id {} slot {}",
                                row.block_id, row.slot
                            )
                        })?;
                        stats.account_refs += ids.len() as u64;
                        ids.sort_unstable();
                        ids.dedup();
                        let unique = ids.len() as u64;
                        stats.unique_account_refs += unique;
                        stats.fixed_u32_bytes += unique * 4;
                        stats.varint_bytes += estimated_delta_varint_block_bytes(&ids);
                        if target_id.is_some_and(|id| ids.binary_search(&id).is_ok()) {
                            stats.candidate_blocks += 1;
                            stats.candidate_txs += row.tx_count as u64;
                        }
                        stats.compressed_bytes += row.compressed_len as u64;
                        stats.uncompressed_bytes += row.uncompressed_len as u64;
                        stats.blocks += 1;
                        stats.txs += row.tx_count as u64;
                    }
                }
                Ok(stats)
            },
        ));
    }

    let mut stats = HotBlockAccountBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("hot account benchmark worker panicked"))??;
        stats.merge(worker_stats);
    }

    let elapsed = started.elapsed().as_secs_f64();
    let sidecar_index_bytes = stats.blocks * 12;
    let sidecar_bytes = stats.varint_bytes + sidecar_index_bytes;
    let avg_unique = if stats.blocks > 0 {
        stats.unique_account_refs as f64 / stats.blocks as f64
    } else {
        0.0
    };
    let avg_refs = if stats.blocks > 0 {
        stats.account_refs as f64 / stats.blocks as f64
    } else {
        0.0
    };
    println!(
        "archive_v2_hot_block_account_table workers={workers} chunk_size={chunk_size} include_metadata={include_metadata} target_id={} blocks={} txs={} compressed_bytes={} uncompressed_bytes={} elapsed_s={elapsed:.3} tx_s={:.2} blocks_s={:.2} compressed_MiB_s={:.2} account_refs={} unique_account_refs={} avg_refs_per_block={avg_refs:.2} avg_unique_per_block={avg_unique:.2} raw_pubkeys={} metadata_decoded={} metadata_skipped={} fixed_u32_bytes={} delta_varint_bytes={} sidecar_index_bytes={} estimated_sidecar_bytes={} sidecar_bytes_per_block={:.2} target_candidate_blocks={} target_candidate_txs={} target_skip_blocks_pct={:.2} target_skip_txs_pct={:.2}",
        target_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        stats.blocks,
        stats.txs,
        stats.compressed_bytes,
        stats.uncompressed_bytes,
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        stats.account_refs,
        stats.unique_account_refs,
        stats.raw_pubkeys,
        stats.metadata_decoded,
        stats.metadata_skipped,
        stats.fixed_u32_bytes,
        stats.varint_bytes,
        sidecar_index_bytes,
        sidecar_bytes,
        if stats.blocks > 0 {
            sidecar_bytes as f64 / stats.blocks as f64
        } else {
            0.0
        },
        stats.candidate_blocks,
        stats.candidate_txs,
        if stats.blocks > 0 {
            100.0 - (stats.candidate_blocks as f64 * 100.0 / stats.blocks as f64)
        } else {
            0.0
        },
        if stats.txs > 0 {
            100.0 - (stats.candidate_txs as f64 * 100.0 / stats.txs as f64)
        } else {
            0.0
        },
    );
    Ok(())
}

fn collect_hot_block_account_ids(
    block: &ArchiveV2HotBlockBlob,
    include_metadata: bool,
    ids: &mut Vec<u32>,
    stats: &mut HotBlockAccountBenchStats,
) -> Result<()> {
    for tx_row in &block.tx_rows {
        if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            continue;
        }
        let message_slice = hot_block_region(
            &block.message_bytes,
            tx_row.message_offset,
            tx_row.message_len,
            "message",
            tx_row.tx_index,
        )?;
        let message: ArchiveV2HotMessagePayload =
            wincode::config::deserialize(message_slice, wincode_leb128_config())
                .with_context(|| format!("decode hot message tx_index={}", tx_row.tx_index))?;
        for key in hot_message_account_keys_for_analysis(&message) {
            push_compact_pubkey_id(*key, ids, stats);
        }
        if include_metadata && tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0 {
            if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                stats.metadata_skipped += 1;
                continue;
            }
            let metadata_slice = hot_block_region(
                &block.metadata_bytes,
                tx_row.metadata_offset,
                tx_row.metadata_len,
                "metadata",
                tx_row.tx_index,
            )?;
            let meta: CompactMetaLoadedAddressView =
                wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                    .with_context(|| {
                        format!(
                            "decode hot metadata loaded-address view tx_index={}",
                            tx_row.tx_index
                        )
                    })?;
            stats.metadata_decoded += 1;
            for key in meta
                .loaded_writable_addresses
                .iter()
                .chain(meta.loaded_readonly_addresses.iter())
            {
                push_compact_pubkey_id(*key, ids, stats);
            }
        }
    }
    Ok(())
}

fn push_compact_pubkey_id(
    key: CompactPubkey,
    ids: &mut Vec<u32>,
    stats: &mut HotBlockAccountBenchStats,
) {
    match key {
        CompactPubkey::Id(id) => ids.push(id),
        CompactPubkey::Raw(_) => stats.raw_pubkeys += 1,
    }
}

fn estimated_delta_varint_block_bytes(ids: &[u32]) -> u64 {
    let mut bytes = uleb128_size(ids.len() as u64);
    let mut previous = 0u32;
    for &id in ids {
        bytes += uleb128_size(id.saturating_sub(previous) as u64);
        previous = id;
    }
    bytes
}

fn uleb128_size(mut value: u64) -> u64 {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn hot_block_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    label: &str,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let len = len as usize;
    let end = start
        .checked_add(len)
        .with_context(|| format!("{label} region overflow tx_index={tx_index}"))?;
    bytes
        .get(start..end)
        .with_context(|| format!("{label} region out of bounds tx_index={tx_index}"))
}

pub(crate) fn repack_hot_blocks_raw(
    input: &Path,
    output_dir: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    level: i32,
    max_blocks: Option<u64>,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "whole-file zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let input_index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let hot_index = read_archive_v2_hot_block_index(&input_index_path)?;
    let input_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        input_bytes == hot_index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        hot_index.blob_file_bytes,
        input_bytes
    );
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (hot_index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    copy_hot_archive_sidecars(input.parent().unwrap_or_else(|| Path::new(".")), output_dir)?;

    let raw_path = output_dir.join(ARCHIVE_RAW_BLOCKS_FILE);
    let raw_zstd_path = output_dir.join(ARCHIVE_RAW_BLOCKS_ZSTD_FILE);
    let index_path = output_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    anyhow::ensure!(
        input != raw_path && input != raw_zstd_path,
        "input and output paths must differ"
    );

    let mut input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let raw_file =
        File::create(&raw_path).with_context(|| format!("create {}", raw_path.display()))?;
    let mut raw_writer = BufWriter::with_capacity(BUFFER_SIZE, raw_file);
    let zstd_file = File::create(&raw_zstd_path)
        .with_context(|| format!("create {}", raw_zstd_path.display()))?;
    let zstd_writer = BufWriter::with_capacity(BUFFER_SIZE, zstd_file);
    let mut whole_zstd = zstd::stream::write::Encoder::new(zstd_writer, level)
        .with_context(|| format!("create zstd encoder {}", raw_zstd_path.display()))?;
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let mut compressed_buf = Vec::with_capacity(2 << 20);

    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Hot Raw Repack");
    let mut current_input_offset = 0u64;
    let mut output_offset = 0u64;
    let mut output_rows = Vec::with_capacity(hot_index.rows.len().min(limit));
    let mut input_compressed_bytes = 0u64;
    let mut raw_bytes = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;

    for row in hot_index.rows.iter().take(limit) {
        if current_input_offset != row.compressed_offset {
            input_file
                .seek(SeekFrom::Start(row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to compressed offset {}",
                        input.display(),
                        row.compressed_offset
                    )
                })?;
            current_input_offset = row.compressed_offset;
        }
        compressed_buf.resize(row.compressed_len as usize, 0);
        input_file
            .read_exact(&mut compressed_buf)
            .with_context(|| {
                format!(
                    "read hot archive block_id {} at compressed offset {}",
                    row.block_id, row.compressed_offset
                )
            })?;
        current_input_offset = current_input_offset.saturating_add(row.compressed_len as u64);

        let block_bytes = decompressor
            .decompress(&compressed_buf, row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block_bytes.len() == row.uncompressed_len as usize,
            "block_id {} decompressed length mismatch: decoded={} index={}",
            row.block_id,
            block_bytes.len(),
            row.uncompressed_len
        );
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                .with_context(|| format!("decode hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
            "hot block validation failed for block_id {} slot {}",
            row.block_id,
            row.slot
        );

        raw_writer
            .write_all(&block_bytes)
            .with_context(|| format!("write {}", raw_path.display()))?;
        whole_zstd
            .write_all(&block_bytes)
            .with_context(|| format!("write {}", raw_zstd_path.display()))?;
        let raw_len = u32::try_from(block_bytes.len()).context("raw hot block exceeds u32::MAX")?;
        output_rows.push(ArchiveV2HotBlockIndexRow {
            block_id: row.block_id,
            slot: row.slot,
            compressed_offset: output_offset,
            compressed_len: raw_len,
            uncompressed_len: raw_len,
            tx_count: row.tx_count,
            first_tx_ordinal: row.first_tx_ordinal,
            first_signature_ordinal: row.first_signature_ordinal,
            signature_count: row.signature_count,
        });
        output_offset += raw_len as u64;
        input_compressed_bytes += row.compressed_len as u64;
        raw_bytes += raw_len as u64;
        blocks += 1;
        txs += row.tx_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }

    raw_writer
        .flush()
        .with_context(|| format!("flush {}", raw_path.display()))?;
    let mut zstd_writer = whole_zstd
        .finish()
        .with_context(|| format!("finish {}", raw_zstd_path.display()))?;
    zstd_writer
        .flush()
        .with_context(|| format!("flush {}", raw_zstd_path.display()))?;
    let whole_zstd_bytes = std::fs::metadata(&raw_zstd_path)
        .with_context(|| format!("stat {}", raw_zstd_path.display()))?
        .len();
    write_archive_v2_hot_block_index(
        &index_path,
        output_offset,
        0,
        ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
        &output_rows,
    )?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let input_ratio = if raw_bytes > 0 {
        input_compressed_bytes as f64 * 100.0 / raw_bytes as f64
    } else {
        0.0
    };
    let whole_ratio = if raw_bytes > 0 {
        whole_zstd_bytes as f64 * 100.0 / raw_bytes as f64
    } else {
        0.0
    };
    let raw_mib_s = if elapsed > 0.0 {
        raw_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    info!(
        "Archive V2 hot raw repack complete in {:.2}s: blocks={} txs={} max_blocks={:?} input_compressed_bytes={} raw_bytes={} whole_zstd_bytes={} input_ratio_pct={:.2} whole_zstd_ratio_pct={:.2} raw_MiB_s={:.2} raw={} raw_zstd={} index={}",
        elapsed,
        blocks,
        txs,
        max_blocks,
        input_compressed_bytes,
        raw_bytes,
        whole_zstd_bytes,
        input_ratio,
        whole_ratio,
        raw_mib_s,
        raw_path.display(),
        raw_zstd_path.display(),
        index_path.display(),
    );
    println!(
        "archive_v2_hot_raw_repack blocks={blocks} txs={txs} elapsed_s={elapsed:.3} input_compressed_bytes={input_compressed_bytes} raw_bytes={raw_bytes} whole_zstd_bytes={whole_zstd_bytes} input_ratio_pct={input_ratio:.2} whole_zstd_ratio_pct={whole_ratio:.2} raw_MiB_s={raw_mib_s:.2}"
    );
    Ok(())
}

pub(crate) fn bench_raw_hot_blocks(
    input: &Path,
    index: Option<&Path>,
    whole_zstd: bool,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    if whole_zstd {
        return bench_raw_hot_blocks_whole_zstd(input, index);
    }

    #[cfg(not(unix))]
    {
        let _ = (input, index, workers, chunk_size);
        anyhow::bail!("raw Archive V2 benchmark currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        bench_raw_hot_blocks_unix(input, index, workers, chunk_size)
    }
}

#[cfg(unix)]
fn bench_raw_hot_blocks_unix(
    input: &Path,
    index: Option<&Path>,
    workers: usize,
    chunk_size: usize,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_hot_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
        "{} is not a raw-block Archive V2 index",
        index_path.display()
    );
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        file_bytes == index.blob_file_bytes,
        "index was built for {} raw bytes, but archive is {} bytes",
        index.blob_file_bytes,
        file_bytes
    );

    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let rows = Arc::new(index.rows);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let file = Arc::clone(&file);
        let rows = Arc::clone(&rows);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(move || -> Result<ArchiveV2ZstdBenchStats> {
            let mut stats = ArchiveV2ZstdBenchStats::default();
            let mut block_buf = Vec::with_capacity(2 << 20);
            loop {
                let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                if start >= rows.len() {
                    break;
                }
                let end = start.saturating_add(chunk_size).min(rows.len());
                for row in &rows[start..end] {
                    block_buf.resize(row.uncompressed_len as usize, 0);
                    read_exact_at(&file, &mut block_buf, row.compressed_offset).with_context(
                        || {
                            format!(
                                "worker {worker_id} read raw block_id {} at offset {}",
                                row.block_id, row.compressed_offset
                            )
                        },
                    )?;
                    let block: ArchiveV2HotBlockBlob =
                        wincode::config::deserialize(&block_buf, wincode_leb128_config())
                            .with_context(|| {
                                format!(
                                    "worker {worker_id} decode raw hot block_id {}",
                                    row.block_id
                                )
                            })?;
                    anyhow::ensure!(
                        block.header.slot == row.slot
                            && block.tx_rows.len() == row.tx_count as usize,
                        "worker {worker_id} raw hot block validation failed for block_id {}",
                        row.block_id
                    );
                    stats.compressed_bytes += row.uncompressed_len as u64;
                    stats.uncompressed_bytes += row.uncompressed_len as u64;
                    stats.blocks += 1;
                    stats.txs += row.tx_count as u64;
                }
            }
            Ok(stats)
        }));
    }

    let mut stats = ArchiveV2ZstdBenchStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow!("raw Archive V2 benchmark worker panicked"))??;
        stats.compressed_bytes += worker_stats.compressed_bytes;
        stats.uncompressed_bytes += worker_stats.uncompressed_bytes;
        stats.blocks += worker_stats.blocks;
        stats.txs += worker_stats.txs;
    }

    print_zstd_block_bench(
        "archive_v2_hot_raw_block_read",
        workers,
        chunk_size,
        0,
        file_bytes,
        &stats,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

fn bench_raw_hot_blocks_whole_zstd(input: &Path, index: Option<&Path>) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_hot_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
        "{} is not a raw-block Archive V2 index",
        index_path.display()
    );
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut decoder = zstd::stream::read::Decoder::new(reader)
        .with_context(|| format!("zstd decode {}", input.display()))?;
    let mut block_buf = Vec::with_capacity(2 << 20);
    let mut stats = ArchiveV2ZstdBenchStats::default();
    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Hot Raw Whole-Zstd Read");

    for row in &index.rows {
        block_buf.resize(row.uncompressed_len as usize, 0);
        decoder.read_exact(&mut block_buf).with_context(|| {
            format!(
                "read whole-zstd raw block_id {} at decompressed offset {}",
                row.block_id, row.compressed_offset
            )
        })?;
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_buf, wincode_leb128_config())
                .with_context(|| format!("decode whole-zstd raw hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
            "whole-zstd raw hot block validation failed for block_id {}",
            row.block_id
        );
        stats.uncompressed_bytes += row.uncompressed_len as u64;
        stats.blocks += 1;
        stats.txs += row.tx_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }
    let mut trailing = [0u8; 1];
    anyhow::ensure!(
        decoder.read(&mut trailing)? == 0,
        "{} has trailing decompressed bytes after indexed raw blocks",
        input.display()
    );
    stats.compressed_bytes = file_bytes;
    progress.final_report();
    print_zstd_block_bench(
        "archive_v2_hot_raw_whole_zstd_read",
        1,
        1,
        0,
        file_bytes,
        &stats,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

fn copy_hot_archive_sidecars(input_dir: &Path, output_dir: &Path) -> Result<()> {
    for name in [
        ARCHIVE_ZSTD_META_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        POH_FILE,
        SHREDDING_FILE,
        REGISTRY_FILE,
        REGISTRY_COUNTS_FILE,
        BLOCKHASH_REGISTRY_FILE,
        PREV_BLOCKHASH_TAIL_FILE,
    ] {
        let source = input_dir.join(name);
        if !crate::file_nonempty(&source) {
            continue;
        }
        let dest = output_dir.join(name);
        if source != dest {
            std::fs::copy(&source, &dest)
                .with_context(|| format!("copy {} to {}", source.display(), dest.display()))?;
        }
    }
    Ok(())
}

pub(crate) fn extract_largest_zstd_block(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    output: &Path,
    by_tx_count: bool,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (input, index, dict, output, by_tx_count);
        anyhow::bail!("zstd-block extraction currently requires Unix pread support");
    }

    #[cfg(unix)]
    {
        extract_largest_zstd_block_unix(input, index, dict, output, by_tx_count)
    }
}

#[cfg(unix)]
fn extract_largest_zstd_block_unix(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    output: &Path,
    by_tx_count: bool,
) -> Result<()> {
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    let row = index
        .rows
        .iter()
        .max_by_key(|row| {
            if by_tx_count {
                (row.tx_count as u64, row.uncompressed_len as u64)
            } else {
                (row.uncompressed_len as u64, row.tx_count as u64)
            }
        })
        .copied()
        .context("zstd-block index has no block rows")?;
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut compressed = vec![0u8; row.compressed_len as usize];
    read_exact_at(&file, &mut compressed, row.compressed_offset).with_context(|| {
        format!(
            "read largest block_id {} at compressed offset {}",
            row.block_id, row.compressed_offset
        )
    })?;
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let block_bytes = decompressor
        .decompress(&compressed, row.uncompressed_len as usize)
        .with_context(|| format!("zstd decompress block_id {}", row.block_id))?;
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(&block_bytes, wincode_leb128_config())
            .with_context(|| format!("decode hot block_id {}", row.block_id))?;
    anyhow::ensure!(
        block.header.slot == row.slot && block.tx_rows.len() == row.tx_count as usize,
        "largest hot block validation failed for block_id {}",
        row.block_id
    );
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let output_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, output_file));
    writer.write_bytes(&block_bytes)?;
    writer.flush()?;
    println!(
        "archive_v2_largest_hot_block block_id={} slot={} txs={} uncompressed_len={} compressed_len={} by_tx_count={} output={}",
        row.block_id,
        row.slot,
        row.tx_count,
        row.uncompressed_len,
        row.compressed_len,
        by_tx_count,
        output.display()
    );
    Ok(())
}

pub(crate) fn bench_single_block(input: &Path, iterations: usize) -> Result<()> {
    let iterations = iterations.max(1);
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let Some((payload_len, payload)) = reader.read_bytes()? else {
        anyhow::bail!("{} is empty", input.display());
    };
    anyhow::ensure!(
        reader.read_bytes()?.is_none(),
        "{} contains more than one framed record",
        input.display()
    );
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(&payload, wincode_leb128_config())
            .with_context(|| format!("{} is not a single Archive V2 hot block", input.display()))?;
    let slot = block.header.slot;
    let tx_count = block.tx_rows.len();
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iterations {
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&payload, wincode_leb128_config())?;
        checksum = checksum.wrapping_add(block.tx_rows.len());
    }
    let elapsed = started.elapsed().as_secs_f64();
    let mib_s = if elapsed > 0.0 {
        payload.len() as f64 * iterations as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    println!(
        "archive_v2_hot_single_block_decode iterations={iterations} slot={slot} txs={tx_count} file_bytes={file_bytes} payload_len={payload_len} elapsed_s={elapsed:.6} payload_MiB_s={mib_s:.2} checksum={checksum}"
    );
    Ok(())
}

fn scan_archive_v2_hot_blocks<F>(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    max_blocks: Option<u64>,
    progress_label: &'static str,
    mut visit: F,
) -> Result<ArchiveV2ZstdBenchStats>
where
    F: FnMut(&ArchiveV2ZstdBlockIndexRow, &ArchiveV2HotBlockBlob) -> Result<()>,
{
    let index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let index = read_archive_v2_zstd_block_index(&index_path)?;
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        index.blob_file_bytes,
        blob_file_bytes
    );
    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );

    let mut file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut compressed_buf = Vec::with_capacity(2 << 20);
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let mut progress = ProgressTracker::new(progress_label);
    let mut stats = ArchiveV2ZstdBenchStats::default();
    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    let mut current_offset = 0u64;

    for row in index.rows.iter().take(limit) {
        if current_offset != row.compressed_offset {
            file.seek(SeekFrom::Start(row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to compressed offset {}",
                        input.display(),
                        row.compressed_offset
                    )
                })?;
            current_offset = row.compressed_offset;
        }
        compressed_buf.resize(row.compressed_len as usize, 0);
        file.read_exact(&mut compressed_buf).with_context(|| {
            format!(
                "read hot archive block_id {} at compressed offset {}",
                row.block_id, row.compressed_offset
            )
        })?;
        current_offset = current_offset.saturating_add(row.compressed_len as u64);

        let block_bytes = decompressor
            .decompress(&compressed_buf, row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block_bytes.len() == row.uncompressed_len as usize,
            "block_id {} decompressed length mismatch: decoded={} index={}",
            row.block_id,
            block_bytes.len(),
            row.uncompressed_len
        );
        let block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                .with_context(|| format!("decode hot block_id {}", row.block_id))?;
        anyhow::ensure!(
            block.header.slot == row.slot,
            "hot block_id {} slot mismatch: decoded={} index={}",
            row.block_id,
            block.header.slot,
            row.slot
        );
        anyhow::ensure!(
            block.tx_rows.len() == row.tx_count as usize,
            "hot block_id {} tx count mismatch: decoded={} index={}",
            row.block_id,
            block.tx_rows.len(),
            row.tx_count
        );

        visit(row, &block)?;
        stats.compressed_bytes += row.compressed_len as u64;
        stats.uncompressed_bytes += row.uncompressed_len as u64;
        stats.blocks += 1;
        stats.txs += row.tx_count as u64;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }

    progress.final_report();
    Ok(stats)
}

fn hot_block_message_slice<'a>(
    block: &'a ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    block_id: u32,
) -> Result<&'a [u8]> {
    hot_block_region_slice(
        &block.message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        block_id,
        row.tx_index,
    )
}

fn hot_block_metadata_slice<'a>(
    block: &'a ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    block_id: u32,
) -> Result<&'a [u8]> {
    hot_block_region_slice(
        &block.metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        block_id,
        row.tx_index,
    )
}

fn hot_block_region_slice<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    region: &'static str,
    block_id: u32,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let len = len as usize;
    let end = start.checked_add(len).with_context(|| {
        format!("{region} slice overflow block_id={block_id} tx_index={tx_index}")
    })?;
    bytes.get(start..end).with_context(|| {
        format!(
            "{region} slice out of bounds block_id={block_id} tx_index={tx_index} offset={offset} len={len} region_len={}",
            bytes.len()
        )
    })
}

fn print_zstd_block_bench(
    label: &'static str,
    workers: usize,
    chunk_size: usize,
    level: i32,
    blob_file_bytes: u64,
    stats: &ArchiveV2ZstdBenchStats,
    elapsed: f64,
) {
    let compressed_mib_s = if elapsed > 0.0 {
        stats.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let uncompressed_mib_s = if elapsed > 0.0 {
        stats.uncompressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let block_s = if elapsed > 0.0 {
        stats.blocks as f64 / elapsed
    } else {
        0.0
    };
    let tx_s = if elapsed > 0.0 {
        stats.txs as f64 / elapsed
    } else {
        0.0
    };
    let ratio = if stats.uncompressed_bytes > 0 {
        stats.compressed_bytes as f64 * 100.0 / stats.uncompressed_bytes as f64
    } else {
        0.0
    };
    println!(
        "{label} workers={workers} chunk_size={chunk_size} level={level} blocks={} txs={} blob_file_bytes={blob_file_bytes} compressed_bytes={} uncompressed_bytes={} ratio_pct={ratio:.2} elapsed_s={elapsed:.3} compressed_MiB_s={compressed_mib_s:.2} uncompressed_MiB_s={uncompressed_mib_s:.2} blocks_s={block_s:.2} tx_s={tx_s:.2}",
        stats.blocks, stats.txs, stats.compressed_bytes, stats.uncompressed_bytes
    );
}

fn default_archive_v2_index_path(input: &Path) -> PathBuf {
    let index_name = input
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            name.strip_suffix(".wincode")
                .map(|stem| format!("{stem}.index"))
                .unwrap_or_else(|| format!("{name}.index"))
        })
        .unwrap_or_else(|| ARCHIVE_INDEX_FILE.to_string());
    input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(index_name)
}

fn default_archive_v2_zstd_index_path(input: &Path) -> PathBuf {
    archive_v2_hot_index_path(input)
}

fn read_u32_varint_with_encoded_len<R: Read>(reader: &mut R) -> Result<Option<(u32, usize)>> {
    let mut value = 0u32;
    let mut shift = 0;
    let mut encoded_len = 0usize;

    loop {
        let mut byte = [0u8; 1];
        match reader.read(&mut byte) {
            Ok(0) if encoded_len == 0 => return Ok(None),
            Ok(0) => anyhow::bail!("unexpected EOF in archive v2 frame length varint"),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err).context("read archive v2 frame length varint"),
        }
        encoded_len += 1;
        value |= ((byte[0] & 0x7f) as u32) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(Some((value, encoded_len)));
        }
        shift += 7;
        anyhow::ensure!(shift <= 28, "archive v2 frame length varint overflow");
    }
}

fn write_archive_v2_block_index(
    path: &Path,
    archive_file_bytes: u64,
    rows: &[ArchiveV2BlockIndexRow],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(ARCHIVE_INDEX_FILE)
    ));
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    writer.write_all(ARCHIVE_V2_INDEX_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    writer.write_all(&archive_file_bytes.to_le_bytes())?;
    for row in rows {
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.frame_offset.to_le_bytes())?;
        writer.write_all(&row.payload_offset.to_le_bytes())?;
        writer.write_all(&row.payload_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn write_archive_v2_zstd_block_index(
    path: &Path,
    blob_file_bytes: u64,
    level: i32,
    flags: u32,
    rows: &[ArchiveV2ZstdBlockIndexRow],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(ARCHIVE_ZSTD_INDEX_FILE)
    ));
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    writer.write_all(ARCHIVE_V2_ZSTD_INDEX_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_ZSTD_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    writer.write_all(&blob_file_bytes.to_le_bytes())?;
    writer.write_all(&level.to_le_bytes())?;
    writer.write_all(&flags.to_le_bytes())?;
    for row in rows {
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.compressed_offset.to_le_bytes())?;
        writer.write_all(&row.compressed_len.to_le_bytes())?;
        writer.write_all(&row.uncompressed_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn read_archive_v2_block_index(path: &Path) -> Result<ArchiveV2BlockIndex> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut header = [0u8; ARCHIVE_V2_INDEX_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        &header[..8] == ARCHIVE_V2_INDEX_MAGIC,
        "{} is not an Archive V2 block index",
        path.display()
    );
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    anyhow::ensure!(
        version == ARCHIVE_V2_INDEX_VERSION,
        "{} has unsupported Archive V2 index version {version}",
        path.display()
    );
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let archive_file_bytes = u64::from_le_bytes(header[20..28].try_into().unwrap());
    let row_count_usize = usize::try_from(row_count).context("index row count exceeds usize")?;
    let expected_len =
        ARCHIVE_V2_INDEX_HEADER_LEN as u64 + row_count * ARCHIVE_V2_INDEX_ROW_LEN as u64;
    let actual_len = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    anyhow::ensure!(
        actual_len == expected_len,
        "{} has size {}, expected {} for {} rows",
        path.display(),
        actual_len,
        expected_len,
        row_count
    );

    let mut rows = Vec::with_capacity(row_count_usize);
    let mut row_buf = [0u8; ARCHIVE_V2_INDEX_ROW_LEN];
    for _ in 0..row_count_usize {
        reader
            .read_exact(&mut row_buf)
            .with_context(|| format!("read row from {}", path.display()))?;
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(row_buf[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row_buf[4..12].try_into().unwrap()),
            frame_offset: u64::from_le_bytes(row_buf[12..20].try_into().unwrap()),
            payload_offset: u64::from_le_bytes(row_buf[20..28].try_into().unwrap()),
            payload_len: u32::from_le_bytes(row_buf[28..32].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row_buf[32..36].try_into().unwrap()),
        });
    }
    Ok(ArchiveV2BlockIndex {
        archive_file_bytes,
        rows,
    })
}

fn read_archive_v2_zstd_block_index(path: &Path) -> Result<ArchiveV2ZstdBlockIndex> {
    let hot_index = read_archive_v2_hot_block_index(path)?;
    let rows = hot_index
        .rows
        .into_iter()
        .map(|row| ArchiveV2ZstdBlockIndexRow {
            block_id: row.block_id,
            slot: row.slot,
            compressed_offset: row.compressed_offset,
            compressed_len: row.compressed_len,
            uncompressed_len: row.uncompressed_len,
            tx_count: row.tx_count,
        })
        .collect();
    Ok(ArchiveV2ZstdBlockIndex {
        blob_file_bytes: hot_index.blob_file_bytes,
        level: hot_index.level,
        flags: hot_index.flags,
        rows,
    })
}

fn load_optional_dict(path: Option<&Path>) -> Result<Option<Vec<u8>>> {
    path.map(|path| {
        std::fs::read(path).with_context(|| format!("read zstd dict {}", path.display()))
    })
    .transpose()
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut read = 0usize;
    while read < buf.len() {
        match file.read_at(&mut buf[read..], offset + read as u64) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "short read from archive v2 file",
                ));
            }
            Ok(n) => read += n,
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveV2LogReparseMode {
    Targeted,
    Full,
}

pub(crate) fn reparse_logs(
    input: &Path,
    output: &Path,
    registry: Option<&Path>,
    mode: ArchiveV2LogReparseMode,
) -> Result<()> {
    anyhow::ensure!(
        input != output,
        "input and output must be different paths: {}",
        input.display()
    );

    let registry_path = registry.map(Path::to_path_buf).unwrap_or_else(|| {
        input
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(REGISTRY_FILE)
    });
    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys.clone());

    let input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, input_file));

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let output_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(BUFFER_SIZE, output_file));

    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Log Reparse");
    let mut records = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut metadata_with_logs = 0u64;
    let mut log_streams_reparsed = 0u64;
    let mut log_streams_skipped = 0u64;
    let mut before_log_bytes = 0u64;
    let mut after_log_bytes = 0u64;
    let mut output_block_offset = 0u64;
    let mut pending_index_update: Option<(u64, u32, u32)> = None;
    let mut index_records_rewritten = 0u64;
    let mut block_scratch = Vec::with_capacity(8 << 20);

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        records += 1;
        match record {
            WincodeArchiveV2Record::Block(mut block) => {
                anyhow::ensure!(
                    pending_index_update.is_none(),
                    "archive v2 log reparse saw a block before the previous block index"
                );
                blocks += 1;
                progress.update_slot(block.header.compact.slot);
                txs += block.txs.len() as u64;
                for tx in &mut block.txs {
                    let Some(WincodeArchiveV2Payload::Decoded { value: meta, .. }) =
                        tx.metadata.as_mut()
                    else {
                        continue;
                    };
                    let Some(logs) = meta.logs.as_mut() else {
                        continue;
                    };

                    metadata_with_logs += 1;
                    if mode == ArchiveV2LogReparseMode::Targeted
                        && !log_stream_needs_targeted_reparse(logs)
                    {
                        log_streams_skipped += 1;
                        continue;
                    }
                    before_log_bytes +=
                        wincode::config::serialized_size(&*logs, wincode_leb128_config())?;
                    let rendered = blockzilla_format::render_logs(logs, &store);
                    let reparsed = blockzilla_format::parse_logs(&rendered, &key_index);
                    after_log_bytes +=
                        wincode::config::serialized_size(&reparsed, wincode_leb128_config())?;
                    *logs = reparsed;
                    log_streams_reparsed += 1;
                }
                let slot = block.header.compact.slot;
                let tx_count =
                    u32::try_from(block.txs.len()).context("block tx count exceeds u32::MAX")?;
                progress.update(1, block.txs.len() as u64);

                let record = WincodeArchiveV2Record::Block(block);
                encode_with_scratch(&record, &mut block_scratch)?;
                let block_len = u32::try_from(block_scratch.len())
                    .context("archive v2 frame exceeds u32::MAX")?;
                writer.write_bytes(&block_scratch)?;
                pending_index_update = Some((slot, block_len, tx_count));
            }
            WincodeArchiveV2Record::Index(mut index) => {
                if let Some((slot, block_len, tx_count)) = pending_index_update.take() {
                    index.slot = slot;
                    index.block_offset = output_block_offset;
                    index.block_len = block_len;
                    index.tx_count = tx_count;
                    output_block_offset += block_len as u64;
                    index_records_rewritten += 1;
                }
                writer.write(&WincodeArchiveV2Record::Index(index))?;
            }
            record => {
                writer.write(&record)?;
            }
        }
    }
    anyhow::ensure!(
        pending_index_update.is_none(),
        "archive v2 log reparse ended before the last block index"
    );
    writer.flush()?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let delta = before_log_bytes as i128 - after_log_bytes as i128;
    info!(
        "Archive V2 log reparse complete in {:.2}s: mode={:?} records={} blocks={} txs={} metadata_with_logs={} streams_reparsed={} streams_skipped={} index_records_rewritten={} before_log_bytes={} after_log_bytes={} delta_bytes={} output={}",
        elapsed,
        mode,
        records,
        blocks,
        txs,
        metadata_with_logs,
        log_streams_reparsed,
        log_streams_skipped,
        index_records_rewritten,
        before_log_bytes,
        after_log_bytes,
        delta,
        output.display()
    );
    Ok(())
}

pub(crate) fn repack_hot_logs(
    input: &Path,
    output_dir: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    level: i32,
    mode: ArchiveV2LogReparseMode,
) -> Result<()> {
    anyhow::ensure!(
        level >= 0,
        "zstd compression level must be non-negative, got {level}"
    );
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;
    let blocks_path = output_dir.join(ARCHIVE_ZSTD_BLOCKS_FILE);
    let index_path = output_dir.join(ARCHIVE_ZSTD_INDEX_FILE);
    anyhow::ensure!(
        input != blocks_path,
        "input and output blocks must be different paths: {}",
        input.display()
    );

    let input_index_path = index
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_archive_v2_zstd_index_path(input));
    let hot_index = read_archive_v2_hot_block_index(&input_index_path)?;
    let blob_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        blob_file_bytes == hot_index.blob_file_bytes,
        "index was built for {} blob bytes, but archive is {} bytes",
        hot_index.blob_file_bytes,
        blob_file_bytes
    );

    let dict_bytes = load_optional_dict(dict)?.unwrap_or_default();
    anyhow::ensure!(
        (hot_index.flags & ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY == 0) || !dict_bytes.is_empty(),
        "index requires a zstd dictionary; pass --dict"
    );
    let output_flags = if dict_bytes.is_empty() {
        0
    } else {
        ARCHIVE_V2_ZSTD_INDEX_FLAG_DICTIONARY
    };

    let registry_path = registry.map(Path::to_path_buf).unwrap_or_else(|| {
        input
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(REGISTRY_FILE)
    });
    let store = KeyStore::load(&registry_path)?;
    let key_index = KeyIndex::build(store.keys.clone());

    let mut input_file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let output_file =
        File::create(&blocks_path).with_context(|| format!("create {}", blocks_path.display()))?;
    let mut blocks_writer = BufWriter::with_capacity(BUFFER_SIZE, output_file);
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_bytes)
        .context("create zstd dictionary decompressor")?;
    let mut compressor = if dict_bytes.is_empty() {
        zstd::bulk::Compressor::new(level).context("create zstd compressor")?
    } else {
        zstd::bulk::Compressor::with_dictionary(level, &dict_bytes)
            .context("create zstd dictionary compressor")?
    };

    let started = Instant::now();
    let mut progress = ProgressTracker::new("Archive V2 Hot Log Repack");
    let limit = max_blocks
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    let mut current_offset = 0u64;
    let mut output_offset = 0u64;
    let mut output_rows = Vec::with_capacity(hot_index.rows.len().min(limit));
    let mut compressed_buf = Vec::with_capacity(2 << 20);
    let mut block_scratch = Vec::with_capacity(8 << 20);

    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut metadata_raw = 0u64;
    let mut metadata_none = 0u64;
    let mut metadata_decoded = 0u64;
    let mut metadata_without_logs = 0u64;
    let mut metadata_copied_without_decode = 0u64;
    let mut metadata_with_logs = 0u64;
    let mut log_streams_reparsed = 0u64;
    let mut log_streams_skipped = 0u64;
    let mut before_log_bytes = 0u64;
    let mut after_log_bytes = 0u64;
    let mut before_metadata_bytes = 0u64;
    let mut after_metadata_bytes = 0u64;
    let mut before_uncompressed_bytes = 0u64;
    let mut after_uncompressed_bytes = 0u64;
    let mut before_compressed_bytes = 0u64;
    let mut after_compressed_bytes = 0u64;

    for input_row in hot_index.rows.iter().take(limit) {
        if current_offset != input_row.compressed_offset {
            input_file
                .seek(SeekFrom::Start(input_row.compressed_offset))
                .with_context(|| {
                    format!(
                        "seek hot archive {} to compressed offset {}",
                        input.display(),
                        input_row.compressed_offset
                    )
                })?;
            current_offset = input_row.compressed_offset;
        }
        compressed_buf.resize(input_row.compressed_len as usize, 0);
        input_file
            .read_exact(&mut compressed_buf)
            .with_context(|| {
                format!(
                    "read hot archive block_id {} at compressed offset {}",
                    input_row.block_id, input_row.compressed_offset
                )
            })?;
        current_offset = current_offset.saturating_add(input_row.compressed_len as u64);

        let block_bytes = decompressor
            .decompress(&compressed_buf, input_row.uncompressed_len as usize)
            .with_context(|| format!("zstd decompress hot block_id {}", input_row.block_id))?;
        anyhow::ensure!(
            block_bytes.len() == input_row.uncompressed_len as usize,
            "block_id {} decompressed length mismatch: decoded={} index={}",
            input_row.block_id,
            block_bytes.len(),
            input_row.uncompressed_len
        );
        let mut block: ArchiveV2HotBlockBlob =
            wincode::config::deserialize(&block_bytes, wincode_leb128_config())
                .with_context(|| format!("decode hot block_id {}", input_row.block_id))?;
        anyhow::ensure!(
            block.header.slot == input_row.slot,
            "hot block_id {} slot mismatch: decoded={} index={}",
            input_row.block_id,
            block.header.slot,
            input_row.slot
        );
        anyhow::ensure!(
            block.tx_rows.len() == input_row.tx_count as usize,
            "hot block_id {} tx count mismatch: decoded={} index={}",
            input_row.block_id,
            block.tx_rows.len(),
            input_row.tx_count
        );

        before_uncompressed_bytes += input_row.uncompressed_len as u64;
        before_compressed_bytes += input_row.compressed_len as u64;

        let old_tx_rows = std::mem::take(&mut block.tx_rows);
        let old_metadata_bytes = std::mem::take(&mut block.metadata_bytes);
        let mut new_tx_rows = Vec::with_capacity(old_tx_rows.len());
        let mut new_metadata_bytes = Vec::with_capacity(old_metadata_bytes.len());

        for tx_row in old_tx_rows {
            let mut new_tx_row = tx_row;
            new_tx_row.metadata_offset = u32::try_from(new_metadata_bytes.len())
                .context("hot metadata offset exceeds u32::MAX")?;

            if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                metadata_none += 1;
                new_tx_row.metadata_len = 0;
                new_tx_rows.push(new_tx_row);
                continue;
            }

            let old_metadata_slice = hot_block_region_slice(
                &old_metadata_bytes,
                tx_row.metadata_offset,
                tx_row.metadata_len,
                "metadata",
                input_row.block_id,
                tx_row.tx_index,
            )?;
            before_metadata_bytes += old_metadata_slice.len() as u64;
            let start = new_metadata_bytes.len();

            if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                metadata_raw += 1;
                new_metadata_bytes.extend_from_slice(old_metadata_slice);
            } else if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS == 0 {
                metadata_copied_without_decode += 1;
                new_metadata_bytes.extend_from_slice(old_metadata_slice);
            } else {
                let mut meta: CompactMetaV1 =
                    wincode::config::deserialize(old_metadata_slice, wincode_leb128_config())
                        .with_context(|| {
                            format!(
                                "decode hot metadata block_id={} slot={} tx_index={}",
                                input_row.block_id, input_row.slot, tx_row.tx_index
                            )
                        })?;
                metadata_decoded += 1;
                if let Some(logs) = meta.logs.as_mut() {
                    metadata_with_logs += 1;
                    if mode == ArchiveV2LogReparseMode::Targeted
                        && !log_stream_needs_targeted_reparse(logs)
                    {
                        log_streams_skipped += 1;
                        new_metadata_bytes.extend_from_slice(old_metadata_slice);
                    } else {
                        before_log_bytes +=
                            wincode::config::serialized_size(&*logs, wincode_leb128_config())?;
                        let rendered = blockzilla_format::render_logs(logs, &store);
                        let reparsed = blockzilla_format::parse_logs(&rendered, &key_index);
                        after_log_bytes +=
                            wincode::config::serialized_size(&reparsed, wincode_leb128_config())?;
                        *logs = reparsed;
                        wincode::config::serialize_into(
                            &mut new_metadata_bytes,
                            &meta,
                            wincode_leb128_config(),
                        )?;
                        log_streams_reparsed += 1;
                    }
                } else {
                    metadata_without_logs += 1;
                    new_metadata_bytes.extend_from_slice(old_metadata_slice);
                }
            }

            let metadata_len = new_metadata_bytes.len() - start;
            new_tx_row.metadata_len =
                u32::try_from(metadata_len).context("hot metadata length exceeds u32::MAX")?;
            after_metadata_bytes += metadata_len as u64;
            new_tx_rows.push(new_tx_row);
        }

        block.tx_rows = new_tx_rows;
        block.metadata_bytes = new_metadata_bytes;
        encode_with_scratch(&block, &mut block_scratch)?;
        let uncompressed_len =
            u32::try_from(block_scratch.len()).context("hot block payload exceeds u32::MAX")?;
        let compressed = compressor
            .compress(&block_scratch)
            .with_context(|| format!("zstd compress hot block_id {}", input_row.block_id))?;
        let compressed_len =
            u32::try_from(compressed.len()).context("compressed hot block exceeds u32::MAX")?;
        blocks_writer
            .write_all(&compressed)
            .with_context(|| format!("write {}", blocks_path.display()))?;

        output_rows.push(ArchiveV2HotBlockIndexRow {
            block_id: input_row.block_id,
            slot: input_row.slot,
            compressed_offset: output_offset,
            compressed_len,
            uncompressed_len,
            tx_count: input_row.tx_count,
            first_tx_ordinal: input_row.first_tx_ordinal,
            first_signature_ordinal: input_row.first_signature_ordinal,
            signature_count: input_row.signature_count,
        });
        output_offset += compressed_len as u64;
        after_uncompressed_bytes += uncompressed_len as u64;
        after_compressed_bytes += compressed_len as u64;
        blocks += 1;
        txs += input_row.tx_count as u64;
        progress.update_slot(input_row.slot);
        progress.update(1, input_row.tx_count as u64);
    }

    blocks_writer
        .flush()
        .with_context(|| format!("flush {}", blocks_path.display()))?;
    write_archive_v2_hot_block_index(
        &index_path,
        output_offset,
        level,
        output_flags,
        &output_rows,
    )?;
    progress.final_report();

    let elapsed = started.elapsed().as_secs_f64();
    let log_delta = before_log_bytes as i128 - after_log_bytes as i128;
    let metadata_delta = before_metadata_bytes as i128 - after_metadata_bytes as i128;
    let uncompressed_delta = before_uncompressed_bytes as i128 - after_uncompressed_bytes as i128;
    let compressed_delta = before_compressed_bytes as i128 - after_compressed_bytes as i128;
    let before_ratio = if before_uncompressed_bytes > 0 {
        before_compressed_bytes as f64 * 100.0 / before_uncompressed_bytes as f64
    } else {
        0.0
    };
    let after_ratio = if after_uncompressed_bytes > 0 {
        after_compressed_bytes as f64 * 100.0 / after_uncompressed_bytes as f64
    } else {
        0.0
    };
    info!(
        "Archive V2 hot log repack complete in {:.2}s: mode={:?} blocks={} txs={} metadata_decoded={} metadata_raw={} metadata_none={} metadata_copied_without_decode={} metadata_without_logs={} metadata_with_logs={} streams_reparsed={} streams_skipped={} before_log_bytes={} after_log_bytes={} log_delta_bytes={} before_metadata_bytes={} after_metadata_bytes={} metadata_delta_bytes={} before_uncompressed_bytes={} after_uncompressed_bytes={} uncompressed_delta_bytes={} before_compressed_bytes={} after_compressed_bytes={} compressed_delta_bytes={} before_ratio_pct={:.2} after_ratio_pct={:.2} level={} dict={} blocks_file={} index={}",
        elapsed,
        mode,
        blocks,
        txs,
        metadata_decoded,
        metadata_raw,
        metadata_none,
        metadata_copied_without_decode,
        metadata_without_logs,
        metadata_with_logs,
        log_streams_reparsed,
        log_streams_skipped,
        before_log_bytes,
        after_log_bytes,
        log_delta,
        before_metadata_bytes,
        after_metadata_bytes,
        metadata_delta,
        before_uncompressed_bytes,
        after_uncompressed_bytes,
        uncompressed_delta,
        before_compressed_bytes,
        after_compressed_bytes,
        compressed_delta,
        before_ratio,
        after_ratio,
        level,
        dict.map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
        blocks_path.display(),
        index_path.display(),
    );
    println!(
        "archive_v2_hot_log_repack mode={mode:?} blocks={blocks} txs={txs} elapsed_s={elapsed:.3} streams_reparsed={log_streams_reparsed} streams_skipped={log_streams_skipped} before_log_bytes={before_log_bytes} after_log_bytes={after_log_bytes} log_delta_bytes={log_delta} before_metadata_bytes={before_metadata_bytes} after_metadata_bytes={after_metadata_bytes} metadata_delta_bytes={metadata_delta} before_uncompressed_bytes={before_uncompressed_bytes} after_uncompressed_bytes={after_uncompressed_bytes} uncompressed_delta_bytes={uncompressed_delta} before_compressed_bytes={before_compressed_bytes} after_compressed_bytes={after_compressed_bytes} compressed_delta_bytes={compressed_delta} before_ratio_pct={before_ratio:.2} after_ratio_pct={after_ratio:.2}"
    );
    Ok(())
}

fn log_stream_needs_targeted_reparse(logs: &blockzilla_format::CompactLogStream) -> bool {
    logs.events.iter().any(log_event_needs_targeted_reparse)
}

fn log_event_needs_targeted_reparse(event: &blockzilla_format::LogEvent) -> bool {
    match event {
        blockzilla_format::LogEvent::ProgramLog(log)
        | blockzilla_format::LogEvent::ProgramPlainLog(log) => program_log_is_unknown(log),
        blockzilla_format::LogEvent::ProgramIdLog { log, .. } => program_log_is_unknown(log),
        blockzilla_format::LogEvent::UnknownProgram { .. }
        | blockzilla_format::LogEvent::UnknownAccount { .. }
        | blockzilla_format::LogEvent::Plain { .. }
        | blockzilla_format::LogEvent::Unparsed { .. } => true,
        _ => false,
    }
}

fn program_log_is_unknown(log: &blockzilla_format::program_logs::ProgramLog) -> bool {
    matches!(log, blockzilla_format::program_logs::ProgramLog::Unknown(_))
}

pub(crate) fn analyze_logs(
    input: &Path,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2LogPatternStats::new(max_keys.max(1));
    let mut progress = ProgressTracker::new("Archive V2 Log Analyze");
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        stats.records += 1;
        let WincodeArchiveV2Record::Block(block) = record else {
            continue;
        };

        stats.blocks += 1;
        stats.txs += block.txs.len() as u64;
        progress.update_slot(block.header.compact.slot);

        for tx in &block.txs {
            match &tx.metadata {
                Some(WincodeArchiveV2Payload::Decoded { value: meta, .. }) => {
                    stats.metadata_decoded += 1;
                    if let Some(logs) = &meta.logs {
                        stats.metadata_with_logs += 1;
                        analyze_log_stream_patterns(logs, store.as_ref(), &mut stats)?;
                    } else {
                        stats.metadata_without_logs += 1;
                    }
                }
                Some(WincodeArchiveV2Payload::Raw { .. }) => stats.metadata_raw += 1,
                None => stats.metadata_none += 1,
            }
        }

        progress.update(1, block.txs.len() as u64);
        if max_blocks.is_some_and(|limit| stats.blocks >= limit) {
            break;
        }
    }

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_log_patterns records={} blocks={} txs={} archive_file_bytes={} elapsed_s={elapsed:.3} MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        if elapsed > 0.0 {
            archive_file_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "known_program_logs_enabled={}",
        blockzilla_format::program_logs::KNOWN_PROGRAM_LOGS_ENABLED
    );
    println!(
        "log_pattern_summary metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_logs={} metadata_without_logs={} log_streams={} log_events={} unknown_program_events={} unknown_account_events={} unparsed_events={} plain_events={} program_log_unknown_events={} program_plain_log_unknown_events={} program_id_log_unknown_events={} program_log_unknown_with_active_program={} program_log_unknown_without_active_program={}",
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_logs,
        stats.metadata_without_logs,
        stats.log_streams,
        stats.log_events,
        stats.unknown_program_events,
        stats.unknown_account_events,
        stats.unparsed_events,
        stats.plain_events,
        stats.program_log_unknown_events,
        stats.program_plain_log_unknown_events,
        stats.program_id_log_unknown_events,
        stats.program_log_unknown_with_active_program,
        stats.program_log_unknown_without_active_program,
    );
    stats.print(top.max(1));
    Ok(())
}

pub(crate) fn analyze_hot_logs(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
) -> Result<()> {
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2LogPatternStats::new(max_keys.max(1));
    let started = Instant::now();

    let scan = scan_archive_v2_hot_blocks(
        input,
        index,
        dict,
        max_blocks,
        "Archive V2 Hot Log Analyze",
        |row, block| {
            stats.records += 1;
            stats.blocks += 1;
            stats.txs += block.tx_rows.len() as u64;
            for tx_row in &block.tx_rows {
                if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                    stats.metadata_none += 1;
                    continue;
                }
                if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                    stats.metadata_raw += 1;
                    continue;
                }

                let metadata_slice = hot_block_metadata_slice(block, tx_row, row.block_id)?;
                let meta: CompactMetaV1 =
                    wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                        .with_context(|| {
                            format!(
                                "decode hot metadata block_id={} slot={} tx_index={}",
                                row.block_id, row.slot, tx_row.tx_index
                            )
                        })?;
                stats.metadata_decoded += 1;
                if let Some(logs) = &meta.logs {
                    stats.metadata_with_logs += 1;
                    analyze_log_stream_patterns(logs, store.as_ref(), &mut stats)?;
                } else {
                    stats.metadata_without_logs += 1;
                }
            }
            Ok(())
        },
    )?;

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_hot_log_patterns records={} blocks={} txs={} archive_file_bytes={} compressed_bytes={} uncompressed_bytes={} elapsed_s={elapsed:.3} compressed_MiB_s={:.2} uncompressed_MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        scan.compressed_bytes,
        scan.uncompressed_bytes,
        if elapsed > 0.0 {
            scan.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            scan.uncompressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "known_program_logs_enabled={}",
        blockzilla_format::program_logs::KNOWN_PROGRAM_LOGS_ENABLED
    );
    println!(
        "log_pattern_summary metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_logs={} metadata_without_logs={} log_streams={} log_events={} unknown_program_events={} unknown_account_events={} unparsed_events={} plain_events={} program_log_unknown_events={} program_plain_log_unknown_events={} program_id_log_unknown_events={} program_log_unknown_with_active_program={} program_log_unknown_without_active_program={}",
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_logs,
        stats.metadata_without_logs,
        stats.log_streams,
        stats.log_events,
        stats.unknown_program_events,
        stats.unknown_account_events,
        stats.unparsed_events,
        stats.plain_events,
        stats.program_log_unknown_events,
        stats.program_plain_log_unknown_events,
        stats.program_id_log_unknown_events,
        stats.program_log_unknown_with_active_program,
        stats.program_log_unknown_without_active_program,
    );
    stats.print(top.max(1));
    Ok(())
}

fn load_archive_v2_analysis_registry(
    input: &Path,
    registry: Option<&Path>,
) -> Result<Option<KeyStore>> {
    if let Some(path) = registry {
        info!(
            "Archive V2 log analysis loading registry {}",
            path.display()
        );
        return KeyStore::load(path).map(Some);
    }

    let default_path = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(REGISTRY_FILE);
    if crate::file_nonempty(&default_path) {
        info!(
            "Archive V2 log analysis loading default registry {}",
            default_path.display()
        );
        KeyStore::load(&default_path).map(Some)
    } else {
        info!(
            "Archive V2 log analysis has no registry at {}; compact ids will be printed as id:<n>",
            default_path.display()
        );
        Ok(None)
    }
}

fn analyze_log_stream_patterns(
    logs: &blockzilla_format::CompactLogStream,
    store: Option<&KeyStore>,
    stats: &mut ArchiveV2LogPatternStats,
) -> Result<()> {
    stats.log_streams += 1;
    let strings = logs.strings.iter().collect::<Vec<_>>();
    let mut program_stack = Vec::<CompactPubkey>::new();

    for event in &logs.events {
        stats.log_events += 1;
        match event {
            blockzilla_format::LogEvent::UnknownProgram { program } => {
                let text = log_string(&strings, *program, "UnknownProgram.program")?;
                stats.unknown_program_events += 1;
                stats.unknown_program_exact.bump(text, text);
                stats
                    .unknown_program_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::UnknownAccount { account } => {
                let text = log_string(&strings, *account, "UnknownAccount.account")?;
                stats.unknown_account_events += 1;
                stats.unknown_account_exact.bump(text, text);
                stats
                    .unknown_account_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::Unparsed { text } => {
                let text = log_string(&strings, *text, "Unparsed.text")?;
                stats.unparsed_events += 1;
                stats.unparsed_exact.bump(text, text);
                stats
                    .unparsed_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::Plain { text } => {
                let text = log_string(&strings, *text, "Plain.text")?;
                stats.plain_events += 1;
                stats.plain_exact.bump(text, text);
                stats
                    .plain_pattern
                    .bump_owned(normalize_log_text_pattern(text), text);
            }
            blockzilla_format::LogEvent::ProgramLog(log) => {
                if let Some(text) = unknown_program_log_text(log, &strings, "ProgramLog")? {
                    stats.program_log_unknown_events += 1;
                    stats.program_unknown_exact.bump(text, text);
                    stats
                        .program_unknown_pattern
                        .bump_owned(normalize_log_text_pattern(text), text);
                    let program = if let Some(program) = program_stack.last().copied() {
                        stats.program_log_unknown_with_active_program += 1;
                        render_compact_pubkey_for_analysis(program, store)?
                    } else {
                        stats.program_log_unknown_without_active_program += 1;
                        "<no-active-program>".to_string()
                    };
                    stats.program_unknown_by_program.bump(&program, &program);
                }
            }
            blockzilla_format::LogEvent::ProgramPlainLog(log) => {
                if let Some(text) = unknown_program_log_text(log, &strings, "ProgramPlainLog")? {
                    stats.program_plain_log_unknown_events += 1;
                    stats.program_unknown_exact.bump(text, text);
                    stats
                        .program_unknown_pattern
                        .bump_owned(normalize_log_text_pattern(text), text);
                    let program = if let Some(program) = program_stack.last().copied() {
                        stats.program_log_unknown_with_active_program += 1;
                        render_compact_pubkey_for_analysis(program, store)?
                    } else {
                        stats.program_log_unknown_without_active_program += 1;
                        "<no-active-program>".to_string()
                    };
                    stats.program_unknown_by_program.bump(&program, &program);
                }
            }
            blockzilla_format::LogEvent::ProgramIdLog { program, log } => {
                if let Some(text) = unknown_program_log_text(log, &strings, "ProgramIdLog")? {
                    stats.program_id_log_unknown_events += 1;
                    stats.program_unknown_exact.bump(text, text);
                    stats
                        .program_unknown_pattern
                        .bump_owned(normalize_log_text_pattern(text), text);
                    let rendered = render_compact_pubkey_for_analysis(*program, store)?;
                    stats.program_unknown_by_program.bump(&rendered, &rendered);
                }
            }
            _ => {}
        }

        update_log_analysis_program_stack(event, &mut program_stack);
    }

    Ok(())
}

fn unknown_program_log_text<'a>(
    log: &blockzilla_format::program_logs::ProgramLog,
    strings: &'a [&'a str],
    context: &'static str,
) -> Result<Option<&'a str>> {
    if let blockzilla_format::program_logs::ProgramLog::Unknown(id) = log {
        return log_string(strings, *id, context).map(Some);
    }
    Ok(None)
}

fn log_string<'a>(strings: &'a [&'a str], id: u32, context: &'static str) -> Result<&'a str> {
    strings.get(id as usize).copied().with_context(|| {
        format!(
            "{context} string id {id} out of bounds len={}",
            strings.len()
        )
    })
}

fn update_log_analysis_program_stack(
    event: &blockzilla_format::LogEvent,
    stack: &mut Vec<CompactPubkey>,
) {
    match event {
        blockzilla_format::LogEvent::Invoke { program, depth } => {
            let depth = *depth as usize;
            if depth == 0 {
                stack.clear();
                stack.push(*program);
                return;
            }
            stack.truncate(depth.saturating_sub(1));
            stack.push(*program);
        }
        blockzilla_format::LogEvent::BpfInvoke { program } => {
            stack.push(*program);
        }
        blockzilla_format::LogEvent::Success { program }
        | blockzilla_format::LogEvent::Failure { program, .. }
        | blockzilla_format::LogEvent::FailureCustomProgramError { program, .. }
        | blockzilla_format::LogEvent::FailureInvalidAccountData { program }
        | blockzilla_format::LogEvent::FailureInvalidProgramArgument { program }
        | blockzilla_format::LogEvent::BpfSuccess { program }
        | blockzilla_format::LogEvent::BpfFailure { program, .. }
        | blockzilla_format::LogEvent::BpfFailureCustomProgramError { program, .. }
        | blockzilla_format::LogEvent::BpfFailureInvalidAccountData { program }
        | blockzilla_format::LogEvent::BpfFailureInvalidProgramArgument { program } => {
            pop_log_analysis_program_stack(stack, *program);
        }
        _ => {}
    }
}

fn pop_log_analysis_program_stack(stack: &mut Vec<CompactPubkey>, program: CompactPubkey) {
    if stack.last().is_some_and(|active| *active == program) {
        stack.pop();
    } else if let Some(position) = stack.iter().rposition(|active| *active == program) {
        stack.truncate(position);
    } else {
        stack.clear();
    }
}

fn render_compact_pubkey_for_analysis(
    key: CompactPubkey,
    store: Option<&KeyStore>,
) -> Result<String> {
    let bytes = match key {
        CompactPubkey::Id(id) => {
            let Some(store) = store else {
                return Ok(format!("id:{id}"));
            };
            *store.get(id).with_context(|| {
                format!("compact pubkey id {id} out of bounds len={}", store.len())
            })?
        }
        CompactPubkey::Raw(bytes) => bytes,
    };
    Ok(Pubkey::new_from_array(bytes).to_string())
}

pub(crate) fn analyze_instruction_data(
    input: &Path,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
    prefix_len: usize,
) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2InstructionDataStats::new(max_keys.max(1), prefix_len.clamp(1, 32));
    let mut progress = ProgressTracker::new("Archive V2 Instruction Data Analyze");
    let started = Instant::now();

    while let Some((_len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        stats.records += 1;
        let WincodeArchiveV2Record::Block(block) = record else {
            continue;
        };

        stats.blocks += 1;
        stats.txs += block.txs.len() as u64;
        progress.update_slot(block.header.compact.slot);

        for tx_record in &block.txs {
            let decoded_meta = match &tx_record.metadata {
                Some(WincodeArchiveV2Payload::Decoded { value, .. }) => {
                    stats.metadata_decoded += 1;
                    Some(value)
                }
                Some(WincodeArchiveV2Payload::Raw { .. }) => {
                    stats.metadata_raw += 1;
                    None
                }
                None => {
                    stats.metadata_none += 1;
                    None
                }
            };

            let decoded_tx = match &tx_record.tx {
                WincodeArchiveV2Payload::Decoded { value, .. } => {
                    stats.tx_payload_decoded += 1;
                    Some(value)
                }
                WincodeArchiveV2Payload::Raw { .. } => {
                    stats.tx_payload_raw += 1;
                    None
                }
            };

            let full_keys = decoded_tx.map(|tx| collect_instruction_message_keys(tx, decoded_meta));
            if let (Some(tx), Some(full_keys)) = (decoded_tx, full_keys.as_deref()) {
                analyze_top_level_instruction_data(tx, decoded_meta, full_keys, &mut stats)
                    .with_context(|| {
                        format!(
                            "analyze tx instruction data slot={} tx_index={}",
                            block.header.compact.slot, tx_record.tx_index
                        )
                    })?;
            }

            if let Some(meta) = decoded_meta {
                analyze_inner_instruction_data(
                    meta,
                    full_keys.as_deref().unwrap_or(&[]),
                    decoded_tx.is_none(),
                    &mut stats,
                )
                .with_context(|| {
                    format!(
                        "analyze inner instruction data slot={} tx_index={}",
                        block.header.compact.slot, tx_record.tx_index
                    )
                })?;
            }
        }

        progress.update(1, block.txs.len() as u64);
        if max_blocks.is_some_and(|limit| stats.blocks >= limit) {
            break;
        }
    }

    progress.final_report();
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_instruction_data_patterns records={} blocks={} txs={} archive_file_bytes={} elapsed_s={elapsed:.3} MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        if elapsed > 0.0 {
            archive_file_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "instruction_data_payload_summary tx_payload_decoded={} tx_payload_raw={} metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_inner={} inner_groups={} prefix_len={} max_keys={}",
        stats.tx_payload_decoded,
        stats.tx_payload_raw,
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_inner,
        stats.inner_groups,
        stats.prefix_len,
        stats.max_keys,
    );
    stats
        .tx
        .print("tx_instruction_data", top.max(1), store.as_ref())?;
    stats
        .inner
        .print("inner_instruction_data", top.max(1), store.as_ref())?;
    Ok(())
}

pub(crate) fn analyze_hot_instruction_data(
    input: &Path,
    index: Option<&Path>,
    dict: Option<&Path>,
    registry: Option<&Path>,
    max_blocks: Option<u64>,
    top: usize,
    max_keys: usize,
    prefix_len: usize,
) -> Result<()> {
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    let store = load_archive_v2_analysis_registry(input, registry)?;
    let mut stats = ArchiveV2InstructionDataStats::new(max_keys.max(1), prefix_len.clamp(1, 32));
    let started = Instant::now();

    let scan = scan_archive_v2_hot_blocks(
        input,
        index,
        dict,
        max_blocks,
        "Archive V2 Hot Instruction Data Analyze",
        |row, block| {
            stats.records += 1;
            stats.blocks += 1;
            stats.txs += block.tx_rows.len() as u64;

            for tx_row in &block.tx_rows {
                let decoded_meta = if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                    stats.metadata_none += 1;
                    None
                } else if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                    stats.metadata_raw += 1;
                    None
                } else {
                    let metadata_slice = hot_block_metadata_slice(block, tx_row, row.block_id)?;
                    let meta: CompactMetaV1 =
                        wincode::config::deserialize(metadata_slice, wincode_leb128_config())
                            .with_context(|| {
                                format!(
                                    "decode hot metadata block_id={} slot={} tx_index={}",
                                    row.block_id, row.slot, tx_row.tx_index
                                )
                            })?;
                    stats.metadata_decoded += 1;
                    Some(meta)
                };

                let message_slice = hot_block_message_slice(block, tx_row, row.block_id)?;
                let message: ArchiveV2HotMessagePayload =
                    wincode::config::deserialize(message_slice, wincode_leb128_config())
                        .with_context(|| {
                            format!(
                                "decode hot message block_id={} slot={} tx_index={}",
                                row.block_id, row.slot, tx_row.tx_index
                            )
                        })?;
                stats.tx_payload_decoded += 1;

                let full_keys = collect_hot_message_keys(&message, decoded_meta.as_ref());
                analyze_hot_top_level_instruction_data(
                    &message,
                    decoded_meta.as_ref(),
                    &full_keys,
                    &mut stats,
                )
                .with_context(|| {
                    format!(
                        "analyze hot tx instruction data slot={} tx_index={}",
                        row.slot, tx_row.tx_index
                    )
                })?;

                if let Some(meta) = decoded_meta.as_ref() {
                    analyze_inner_instruction_data(meta, &full_keys, false, &mut stats)
                        .with_context(|| {
                            format!(
                                "analyze hot inner instruction data slot={} tx_index={}",
                                row.slot, tx_row.tx_index
                            )
                        })?;
                }
            }
            Ok(())
        },
    )?;

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "archive_v2_hot_instruction_data_patterns records={} blocks={} txs={} archive_file_bytes={} compressed_bytes={} uncompressed_bytes={} elapsed_s={elapsed:.3} compressed_MiB_s={:.2} uncompressed_MiB_s={:.2} blocks_s={:.2} tx_s={:.2}",
        stats.records,
        stats.blocks,
        stats.txs,
        archive_file_bytes,
        scan.compressed_bytes,
        scan.uncompressed_bytes,
        if elapsed > 0.0 {
            scan.compressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            scan.uncompressed_bytes as f64 / elapsed / (1024.0 * 1024.0)
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.blocks as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            stats.txs as f64 / elapsed
        } else {
            0.0
        },
    );
    println!(
        "instruction_data_payload_summary tx_payload_decoded={} tx_payload_raw={} metadata_decoded={} metadata_raw={} metadata_none={} metadata_with_inner={} inner_groups={} hot_compact_vote_update_vote_state={} hot_compact_vote_update_vote_state_switch={} hot_compact_vote_tower_sync={} hot_compact_vote_tower_sync_switch={} hot_compact_compute_budget={} hot_compact_system={} prefix_len={} max_keys={}",
        stats.tx_payload_decoded,
        stats.tx_payload_raw,
        stats.metadata_decoded,
        stats.metadata_raw,
        stats.metadata_none,
        stats.metadata_with_inner,
        stats.inner_groups,
        stats.hot_compact_vote_update_vote_state,
        stats.hot_compact_vote_update_vote_state_switch,
        stats.hot_compact_vote_tower_sync,
        stats.hot_compact_vote_tower_sync_switch,
        stats.hot_compact_compute_budget_total(),
        stats.hot_compact_system_total(),
        stats.prefix_len,
        stats.max_keys,
    );
    stats.print_hot_compact_instruction_summary();
    stats
        .tx
        .print("tx_instruction_data", top.max(1), store.as_ref())?;
    stats
        .inner
        .print("inner_instruction_data", top.max(1), store.as_ref())?;
    Ok(())
}

fn analyze_top_level_instruction_data(
    tx: &OwnedCompactTransaction,
    meta: Option<&CompactMetaV1>,
    full_keys: &[CompactPubkey],
    stats: &mut ArchiveV2InstructionDataStats,
) -> Result<()> {
    let allow_unresolved_program = meta.is_none();
    for instruction in owned_message_instructions_for_analysis(&tx.message) {
        let program = resolve_instruction_program_key(
            instruction.program_id_index as u32,
            full_keys,
            allow_unresolved_program,
        )?;
        stats.tx.bump(&instruction.data, program);
    }
    Ok(())
}

fn analyze_hot_top_level_instruction_data(
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
    full_keys: &[CompactPubkey],
    stats: &mut ArchiveV2InstructionDataStats,
) -> Result<()> {
    let allow_unresolved_program = meta.is_none();
    for instruction in hot_message_instructions_for_analysis(message) {
        match &instruction.data {
            ArchiveV2HotInstructionData::Raw(data) => {
                let program = resolve_instruction_program_key(
                    instruction.program_id_index as u32,
                    full_keys,
                    allow_unresolved_program,
                )?;
                stats.tx.bump(data, program);
            }
            ArchiveV2HotInstructionData::ComputeBudget(data) => {
                stats.bump_hot_compact_compute_budget(data);
            }
            ArchiveV2HotInstructionData::System(data) => {
                stats.bump_hot_compact_system(data);
            }
            data => stats.bump_hot_compact_vote(data),
        }
    }
    Ok(())
}

fn analyze_inner_instruction_data(
    meta: &CompactMetaV1,
    full_keys: &[CompactPubkey],
    allow_unresolved_program: bool,
    stats: &mut ArchiveV2InstructionDataStats,
) -> Result<()> {
    let Some(groups) = &meta.inner_instructions else {
        return Ok(());
    };
    stats.metadata_with_inner += 1;
    stats.inner_groups += groups.len() as u64;
    for group in groups {
        for instruction in &group.instructions {
            let program = resolve_instruction_program_key(
                instruction.program_id_index,
                full_keys,
                allow_unresolved_program,
            )
            .with_context(|| {
                format!(
                    "inner instruction group index={} program_id_index={}",
                    group.index, instruction.program_id_index
                )
            })?;
            stats.inner.bump(&instruction.data, program);
        }
    }
    Ok(())
}

fn collect_instruction_message_keys(
    tx: &OwnedCompactTransaction,
    meta: Option<&CompactMetaV1>,
) -> Vec<CompactPubkey> {
    let mut keys = owned_message_account_keys_for_analysis(&tx.message).to_vec();
    if let Some(meta) = meta {
        keys.extend_from_slice(&meta.loaded_writable_addresses);
        keys.extend_from_slice(&meta.loaded_readonly_addresses);
    }
    keys
}

fn collect_hot_message_keys(
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
) -> Vec<CompactPubkey> {
    let mut keys = hot_message_account_keys_for_analysis(message).to_vec();
    if let Some(meta) = meta {
        keys.extend_from_slice(&meta.loaded_writable_addresses);
        keys.extend_from_slice(&meta.loaded_readonly_addresses);
    }
    keys
}

fn owned_message_account_keys_for_analysis(message: &OwnedCompactMessage) -> &[CompactPubkey] {
    match message {
        OwnedCompactMessage::Legacy(message) => &message.account_keys,
        OwnedCompactMessage::V0(message) => &message.account_keys,
    }
}

fn hot_message_account_keys_for_analysis(message: &ArchiveV2HotMessagePayload) -> &[CompactPubkey] {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.account_keys,
        ArchiveV2HotMessagePayload::V0(message) => &message.account_keys,
    }
}

fn owned_message_instructions_for_analysis(
    message: &OwnedCompactMessage,
) -> &[OwnedCompactInstruction] {
    match message {
        OwnedCompactMessage::Legacy(message) => &message.instructions,
        OwnedCompactMessage::V0(message) => &message.instructions,
    }
}

fn hot_message_instructions_for_analysis(
    message: &ArchiveV2HotMessagePayload,
) -> &[ArchiveV2HotInstruction] {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.instructions,
        ArchiveV2HotMessagePayload::V0(message) => &message.instructions,
    }
}

fn resolve_instruction_program_key(
    program_id_index: u32,
    full_keys: &[CompactPubkey],
    allow_unresolved_program: bool,
) -> Result<InstructionProgramKey> {
    if let Some(key) = full_keys.get(program_id_index as usize).copied() {
        return Ok(InstructionProgramKey::Pubkey(key));
    }
    if allow_unresolved_program {
        return Ok(InstructionProgramKey::Unresolved(program_id_index));
    }
    anyhow::bail!(
        "program id index {} out of bounds for {} message keys",
        program_id_index,
        full_keys.len()
    );
}

struct ArchiveV2InstructionDataStats {
    records: u64,
    blocks: u64,
    txs: u64,
    tx_payload_decoded: u64,
    tx_payload_raw: u64,
    metadata_decoded: u64,
    metadata_raw: u64,
    metadata_none: u64,
    metadata_with_inner: u64,
    inner_groups: u64,
    hot_compact_vote_update_vote_state: u64,
    hot_compact_vote_update_vote_state_switch: u64,
    hot_compact_vote_tower_sync: u64,
    hot_compact_vote_tower_sync_switch: u64,
    hot_compute_budget_unused: u64,
    hot_compute_budget_request_heap_frame: u64,
    hot_compute_budget_set_compute_unit_limit: u64,
    hot_compute_budget_set_compute_unit_price: u64,
    hot_compute_budget_set_loaded_accounts_data_size_limit: u64,
    hot_system_create_account: u64,
    hot_system_assign: u64,
    hot_system_transfer: u64,
    hot_system_create_account_with_seed: u64,
    hot_system_advance_nonce_account: u64,
    hot_system_withdraw_nonce_account: u64,
    hot_system_initialize_nonce_account: u64,
    hot_system_authorize_nonce_account: u64,
    hot_system_allocate: u64,
    hot_system_allocate_with_seed: u64,
    hot_system_assign_with_seed: u64,
    hot_system_transfer_with_seed: u64,
    hot_system_upgrade_nonce_account: u64,
    hot_system_create_account_allow_prefund: u64,
    tx: InstructionDataPatternStats,
    inner: InstructionDataPatternStats,
    prefix_len: usize,
    max_keys: usize,
}

impl ArchiveV2InstructionDataStats {
    fn new(max_keys: usize, prefix_len: usize) -> Self {
        Self {
            records: 0,
            blocks: 0,
            txs: 0,
            tx_payload_decoded: 0,
            tx_payload_raw: 0,
            metadata_decoded: 0,
            metadata_raw: 0,
            metadata_none: 0,
            metadata_with_inner: 0,
            inner_groups: 0,
            hot_compact_vote_update_vote_state: 0,
            hot_compact_vote_update_vote_state_switch: 0,
            hot_compact_vote_tower_sync: 0,
            hot_compact_vote_tower_sync_switch: 0,
            hot_compute_budget_unused: 0,
            hot_compute_budget_request_heap_frame: 0,
            hot_compute_budget_set_compute_unit_limit: 0,
            hot_compute_budget_set_compute_unit_price: 0,
            hot_compute_budget_set_loaded_accounts_data_size_limit: 0,
            hot_system_create_account: 0,
            hot_system_assign: 0,
            hot_system_transfer: 0,
            hot_system_create_account_with_seed: 0,
            hot_system_advance_nonce_account: 0,
            hot_system_withdraw_nonce_account: 0,
            hot_system_initialize_nonce_account: 0,
            hot_system_authorize_nonce_account: 0,
            hot_system_allocate: 0,
            hot_system_allocate_with_seed: 0,
            hot_system_assign_with_seed: 0,
            hot_system_transfer_with_seed: 0,
            hot_system_upgrade_nonce_account: 0,
            hot_system_create_account_allow_prefund: 0,
            tx: InstructionDataPatternStats::new(max_keys, prefix_len),
            inner: InstructionDataPatternStats::new(max_keys, prefix_len),
            prefix_len,
            max_keys,
        }
    }

    fn bump_hot_compact_vote(&mut self, data: &ArchiveV2HotInstructionData) {
        match data {
            ArchiveV2HotInstructionData::Raw(_) => {}
            ArchiveV2HotInstructionData::ComputeBudget(_) => {}
            ArchiveV2HotInstructionData::System(_) => {}
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_) => {
                self.hot_compact_vote_update_vote_state += 1;
            }
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. } => {
                self.hot_compact_vote_update_vote_state_switch += 1;
            }
            ArchiveV2HotInstructionData::VoteTowerSync(_) => {
                self.hot_compact_vote_tower_sync += 1;
            }
            ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. } => {
                self.hot_compact_vote_tower_sync_switch += 1;
            }
        }
    }

    fn bump_hot_compact_compute_budget(&mut self, data: &ArchiveV2ComputeBudgetInstructionData) {
        match data {
            ArchiveV2ComputeBudgetInstructionData::Unused => self.hot_compute_budget_unused += 1,
            ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(_) => {
                self.hot_compute_budget_request_heap_frame += 1;
            }
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(_) => {
                self.hot_compute_budget_set_compute_unit_limit += 1;
            }
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(_) => {
                self.hot_compute_budget_set_compute_unit_price += 1;
            }
            ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(_) => {
                self.hot_compute_budget_set_loaded_accounts_data_size_limit += 1;
            }
        }
    }

    fn bump_hot_compact_system(&mut self, data: &ArchiveV2SystemInstructionData) {
        match data {
            ArchiveV2SystemInstructionData::CreateAccount { .. } => {
                self.hot_system_create_account += 1;
            }
            ArchiveV2SystemInstructionData::Assign { .. } => self.hot_system_assign += 1,
            ArchiveV2SystemInstructionData::Transfer { .. } => self.hot_system_transfer += 1,
            ArchiveV2SystemInstructionData::CreateAccountWithSeed { .. } => {
                self.hot_system_create_account_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::AdvanceNonceAccount => {
                self.hot_system_advance_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::WithdrawNonceAccount { .. } => {
                self.hot_system_withdraw_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::InitializeNonceAccount { .. } => {
                self.hot_system_initialize_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::AuthorizeNonceAccount { .. } => {
                self.hot_system_authorize_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::Allocate { .. } => self.hot_system_allocate += 1,
            ArchiveV2SystemInstructionData::AllocateWithSeed { .. } => {
                self.hot_system_allocate_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::AssignWithSeed { .. } => {
                self.hot_system_assign_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::TransferWithSeed { .. } => {
                self.hot_system_transfer_with_seed += 1;
            }
            ArchiveV2SystemInstructionData::UpgradeNonceAccount => {
                self.hot_system_upgrade_nonce_account += 1;
            }
            ArchiveV2SystemInstructionData::CreateAccountAllowPrefund { .. } => {
                self.hot_system_create_account_allow_prefund += 1;
            }
        }
    }

    fn hot_compact_compute_budget_total(&self) -> u64 {
        self.hot_compute_budget_unused
            + self.hot_compute_budget_request_heap_frame
            + self.hot_compute_budget_set_compute_unit_limit
            + self.hot_compute_budget_set_compute_unit_price
            + self.hot_compute_budget_set_loaded_accounts_data_size_limit
    }

    fn hot_compact_system_total(&self) -> u64 {
        self.hot_system_create_account
            + self.hot_system_assign
            + self.hot_system_transfer
            + self.hot_system_create_account_with_seed
            + self.hot_system_advance_nonce_account
            + self.hot_system_withdraw_nonce_account
            + self.hot_system_initialize_nonce_account
            + self.hot_system_authorize_nonce_account
            + self.hot_system_allocate
            + self.hot_system_allocate_with_seed
            + self.hot_system_assign_with_seed
            + self.hot_system_transfer_with_seed
            + self.hot_system_upgrade_nonce_account
            + self.hot_system_create_account_allow_prefund
    }

    fn print_hot_compact_instruction_summary(&self) {
        println!(
            "hot_compute_budget_variants unused={} request_heap_frame={} set_compute_unit_limit={} set_compute_unit_price={} set_loaded_accounts_data_size_limit={}",
            self.hot_compute_budget_unused,
            self.hot_compute_budget_request_heap_frame,
            self.hot_compute_budget_set_compute_unit_limit,
            self.hot_compute_budget_set_compute_unit_price,
            self.hot_compute_budget_set_loaded_accounts_data_size_limit,
        );
        println!(
            "hot_system_variants create_account={} assign={} transfer={} create_account_with_seed={} advance_nonce_account={} withdraw_nonce_account={} initialize_nonce_account={} authorize_nonce_account={} allocate={} allocate_with_seed={} assign_with_seed={} transfer_with_seed={} upgrade_nonce_account={} create_account_allow_prefund={}",
            self.hot_system_create_account,
            self.hot_system_assign,
            self.hot_system_transfer,
            self.hot_system_create_account_with_seed,
            self.hot_system_advance_nonce_account,
            self.hot_system_withdraw_nonce_account,
            self.hot_system_initialize_nonce_account,
            self.hot_system_authorize_nonce_account,
            self.hot_system_allocate,
            self.hot_system_allocate_with_seed,
            self.hot_system_assign_with_seed,
            self.hot_system_transfer_with_seed,
            self.hot_system_upgrade_nonce_account,
            self.hot_system_create_account_allow_prefund,
        );
    }
}

struct InstructionDataPatternStats {
    instructions: u64,
    data_bytes: u128,
    zero_len: u64,
    prefix_len: usize,
    exact: CappedInstructionDataCounts,
    prefix: CappedInstructionPrefixCounts,
    by_program: CappedInstructionProgramCounts,
    by_length: InstructionLengthCounts,
}

impl InstructionDataPatternStats {
    fn new(max_keys: usize, prefix_len: usize) -> Self {
        Self {
            instructions: 0,
            data_bytes: 0,
            zero_len: 0,
            prefix_len,
            exact: CappedInstructionDataCounts::new(max_keys),
            prefix: CappedInstructionPrefixCounts::new(max_keys),
            by_program: CappedInstructionProgramCounts::new(max_keys),
            by_length: InstructionLengthCounts::default(),
        }
    }

    fn bump(&mut self, data: &[u8], program: InstructionProgramKey) {
        self.instructions += 1;
        self.data_bytes += data.len() as u128;
        if data.is_empty() {
            self.zero_len += 1;
        }
        self.exact.bump(data, program);
        self.prefix.bump(data, self.prefix_len);
        self.by_program.bump(program, data.len() as u64);
        self.by_length.bump(data.len() as u32);
    }

    fn print(&self, name: &'static str, top: usize, store: Option<&KeyStore>) -> Result<()> {
        println!(
            "instruction_data_summary kind={name} instructions={} data_bytes={} zero_len={} nonzero_len={} avg_data_len={:.2}",
            self.instructions,
            self.data_bytes,
            self.zero_len,
            self.instructions.saturating_sub(self.zero_len),
            if self.instructions > 0 {
                self.data_bytes as f64 / self.instructions as f64
            } else {
                0.0
            },
        );
        self.exact.print(name, top, store)?;
        self.prefix.print(name, top)?;
        self.by_program.print(name, top, store)?;
        self.by_length.print(name, top);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InstructionDataKey {
    len: u32,
    hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InstructionPrefixKey {
    len: u8,
    bytes: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum InstructionProgramKey {
    Pubkey(CompactPubkey),
    Unresolved(u32),
}

impl InstructionProgramKey {
    fn render(self, store: Option<&KeyStore>) -> Result<String> {
        match self {
            Self::Pubkey(key) => render_compact_pubkey_for_analysis(key, store),
            Self::Unresolved(index) => Ok(format!("unresolved_program_index:{index}")),
        }
    }
}

struct CountedInstructionData {
    count: u64,
    sample_prefix: [u8; 32],
    sample_prefix_len: u8,
    program_example: InstructionProgramKey,
}

struct CappedInstructionDataCounts {
    counts: GxHashMap<InstructionDataKey, CountedInstructionData>,
    observations: u64,
    observed_bytes: u128,
    skipped_new_keys: u64,
    skipped_new_key_bytes: u128,
    max_keys: usize,
}

impl CappedInstructionDataCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            observed_bytes: 0,
            skipped_new_keys: 0,
            skipped_new_key_bytes: 0,
            max_keys,
        }
    }

    fn bump(&mut self, data: &[u8], program: InstructionProgramKey) {
        self.observations += 1;
        self.observed_bytes += data.len() as u128;
        let key = InstructionDataKey {
            len: data.len() as u32,
            hash: stable_hash_bytes(data),
        };
        if let Some(entry) = self.counts.get_mut(&key) {
            entry.count = entry.count.saturating_add(1);
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            self.skipped_new_key_bytes += data.len() as u128;
            return;
        }
        let (sample_prefix, sample_prefix_len) = instruction_data_prefix(data, 32);
        self.counts.insert(
            key,
            CountedInstructionData {
                count: 1,
                sample_prefix,
                sample_prefix_len,
                program_example: program,
            },
        );
    }

    fn print(&self, parent: &'static str, top: usize, store: Option<&KeyStore>) -> Result<()> {
        let tracked_bytes = self.tracked_bytes();
        let duplicate_bytes = self.tracked_duplicate_bytes();
        let repeated_keys = self.counts.values().filter(|entry| entry.count > 1).count();
        println!(
            "instruction_data_exact_bucket kind={parent}_exact observations={} observed_bytes={} distinct_tracked={} repeated_keys={} skipped_new_keys={} skipped_new_key_bytes={} tracked_bytes={} tracked_duplicate_bytes={} duplicate_pct_observed={:.4} duplicate_pct_tracked={:.4} max_keys={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
            repeated_keys,
            self.skipped_new_keys,
            self.skipped_new_key_bytes,
            tracked_bytes,
            duplicate_bytes,
            pct_u128(duplicate_bytes, self.observed_bytes),
            pct_u128(duplicate_bytes, tracked_bytes),
            self.max_keys,
        );

        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            let a_duplicate = exact_duplicate_bytes(*a.0, a.1.count);
            let b_duplicate = exact_duplicate_bytes(*b.0, b.1.count);
            b_duplicate
                .cmp(&a_duplicate)
                .then_with(|| {
                    exact_total_bytes(*b.0, b.1.count).cmp(&exact_total_bytes(*a.0, a.1.count))
                })
                .then_with(|| b.1.count.cmp(&a.1.count))
                .then_with(|| a.0.hash.cmp(&b.0.hash))
        });
        for (rank, (key, entry)) in rows.into_iter().take(top).enumerate() {
            let total_bytes = exact_total_bytes(*key, entry.count);
            let duplicate_bytes = exact_duplicate_bytes(*key, entry.count);
            println!(
                "instruction_data_exact_top kind={parent}_exact rank={} count={} len={} total_bytes={} duplicate_bytes={} hash={:016x} pct_observations={:.4} pct_bytes={:.4} sample_hex={} program_example={:?}",
                rank + 1,
                entry.count,
                key.len,
                total_bytes,
                duplicate_bytes,
                key.hash,
                pct_u64(entry.count, self.observations),
                pct_u128(total_bytes, self.observed_bytes),
                render_instruction_prefix(entry.sample_prefix, entry.sample_prefix_len),
                entry.program_example.render(store)?,
            );
        }
        Ok(())
    }

    fn tracked_bytes(&self) -> u128 {
        self.counts
            .iter()
            .map(|(key, entry)| exact_total_bytes(*key, entry.count))
            .sum()
    }

    fn tracked_duplicate_bytes(&self) -> u128 {
        self.counts
            .iter()
            .map(|(key, entry)| exact_duplicate_bytes(*key, entry.count))
            .sum()
    }
}

struct CappedInstructionPrefixCounts {
    counts: GxHashMap<InstructionPrefixKey, CountedBytes>,
    observations: u64,
    observed_bytes: u128,
    skipped_new_keys: u64,
    skipped_new_key_bytes: u128,
    max_keys: usize,
}

impl CappedInstructionPrefixCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            observed_bytes: 0,
            skipped_new_keys: 0,
            skipped_new_key_bytes: 0,
            max_keys,
        }
    }

    fn bump(&mut self, data: &[u8], prefix_len: usize) {
        self.observations += 1;
        self.observed_bytes += data.len() as u128;
        let key = InstructionPrefixKey::new(data, prefix_len);
        if let Some(entry) = self.counts.get_mut(&key) {
            entry.count = entry.count.saturating_add(1);
            entry.bytes += data.len() as u128;
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            self.skipped_new_key_bytes += data.len() as u128;
            return;
        }
        self.counts.insert(
            key,
            CountedBytes {
                count: 1,
                bytes: data.len() as u128,
            },
        );
    }

    fn print(&self, parent: &'static str, top: usize) -> Result<()> {
        println!(
            "instruction_data_prefix_bucket kind={parent}_prefix observations={} observed_bytes={} distinct_tracked={} skipped_new_keys={} skipped_new_key_bytes={} max_keys={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
            self.skipped_new_keys,
            self.skipped_new_key_bytes,
            self.max_keys,
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.1.bytes
                .cmp(&a.1.bytes)
                .then_with(|| b.1.count.cmp(&a.1.count))
                .then_with(|| a.0.len.cmp(&b.0.len))
        });
        for (rank, (key, entry)) in rows.into_iter().take(top).enumerate() {
            println!(
                "instruction_data_prefix_top kind={parent}_prefix rank={} count={} bytes={} pct_observations={:.4} pct_bytes={:.4} prefix_hex={}",
                rank + 1,
                entry.count,
                entry.bytes,
                pct_u64(entry.count, self.observations),
                pct_u128(entry.bytes, self.observed_bytes),
                render_instruction_prefix(key.bytes, key.len),
            );
        }
        Ok(())
    }
}

impl InstructionPrefixKey {
    fn new(data: &[u8], prefix_len: usize) -> Self {
        let (bytes, len) = instruction_data_prefix(data, prefix_len);
        Self { len, bytes }
    }
}

struct CappedInstructionProgramCounts {
    counts: GxHashMap<InstructionProgramKey, CountedBytes>,
    observations: u64,
    observed_bytes: u128,
    skipped_new_keys: u64,
    skipped_new_key_bytes: u128,
    max_keys: usize,
}

impl CappedInstructionProgramCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            observed_bytes: 0,
            skipped_new_keys: 0,
            skipped_new_key_bytes: 0,
            max_keys,
        }
    }

    fn bump(&mut self, program: InstructionProgramKey, data_len: u64) {
        self.observations += 1;
        self.observed_bytes += data_len as u128;
        if let Some(entry) = self.counts.get_mut(&program) {
            entry.count = entry.count.saturating_add(1);
            entry.bytes += data_len as u128;
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            self.skipped_new_key_bytes += data_len as u128;
            return;
        }
        self.counts.insert(
            program,
            CountedBytes {
                count: 1,
                bytes: data_len as u128,
            },
        );
    }

    fn print(&self, parent: &'static str, top: usize, store: Option<&KeyStore>) -> Result<()> {
        println!(
            "instruction_data_program_bucket kind={parent}_program observations={} observed_bytes={} distinct_tracked={} skipped_new_keys={} skipped_new_key_bytes={} max_keys={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
            self.skipped_new_keys,
            self.skipped_new_key_bytes,
            self.max_keys,
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.1.bytes
                .cmp(&a.1.bytes)
                .then_with(|| b.1.count.cmp(&a.1.count))
        });
        for (rank, (program, entry)) in rows.into_iter().take(top).enumerate() {
            println!(
                "instruction_data_program_top kind={parent}_program rank={} count={} bytes={} pct_observations={:.4} pct_bytes={:.4} program={:?}",
                rank + 1,
                entry.count,
                entry.bytes,
                pct_u64(entry.count, self.observations),
                pct_u128(entry.bytes, self.observed_bytes),
                program.render(store)?,
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct InstructionLengthCounts {
    counts: BTreeMap<u32, CountedBytes>,
    observations: u64,
    observed_bytes: u128,
}

impl InstructionLengthCounts {
    fn bump(&mut self, len: u32) {
        self.observations += 1;
        self.observed_bytes += len as u128;
        let entry = self.counts.entry(len).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.bytes += len as u128;
    }

    fn print(&self, parent: &'static str, top: usize) {
        println!(
            "instruction_data_length_bucket kind={parent}_length observations={} observed_bytes={} distinct_lengths={}",
            self.observations,
            self.observed_bytes,
            self.counts.len(),
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.1.bytes
                .cmp(&a.1.bytes)
                .then_with(|| b.1.count.cmp(&a.1.count))
                .then_with(|| a.0.cmp(b.0))
        });
        for (rank, (len, entry)) in rows.into_iter().take(top).enumerate() {
            println!(
                "instruction_data_length_top kind={parent}_length rank={} len={} count={} bytes={} pct_observations={:.4} pct_bytes={:.4}",
                rank + 1,
                len,
                entry.count,
                entry.bytes,
                pct_u64(entry.count, self.observations),
                pct_u128(entry.bytes, self.observed_bytes),
            );
        }
    }
}

#[derive(Default)]
struct CountedBytes {
    count: u64,
    bytes: u128,
}

fn exact_total_bytes(key: InstructionDataKey, count: u64) -> u128 {
    key.len as u128 * count as u128
}

fn exact_duplicate_bytes(key: InstructionDataKey, count: u64) -> u128 {
    key.len as u128 * count.saturating_sub(1) as u128
}

fn stable_hash_bytes(data: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn instruction_data_prefix(data: &[u8], max_len: usize) -> ([u8; 32], u8) {
    let len = data.len().min(max_len).min(32);
    let mut bytes = [0u8; 32];
    bytes[..len].copy_from_slice(&data[..len]);
    (bytes, len as u8)
}

fn render_instruction_prefix(bytes: [u8; 32], len: u8) -> String {
    if len == 0 {
        return "<empty>".to_string();
    }
    let mut out = String::with_capacity(len as usize * 2);
    for byte in &bytes[..len as usize] {
        out.push(hex_nibble(byte >> 4));
        out.push(hex_nibble(byte & 0x0f));
    }
    out
}

fn hex_nibble(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("hex nibble out of range"),
    }
}

fn pct_u64(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn pct_u128(part: u128, total: u128) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

struct ArchiveV2LogPatternStats {
    records: u64,
    blocks: u64,
    txs: u64,
    metadata_decoded: u64,
    metadata_raw: u64,
    metadata_none: u64,
    metadata_with_logs: u64,
    metadata_without_logs: u64,
    log_streams: u64,
    log_events: u64,
    unknown_program_events: u64,
    unknown_account_events: u64,
    unparsed_events: u64,
    plain_events: u64,
    program_log_unknown_events: u64,
    program_plain_log_unknown_events: u64,
    program_id_log_unknown_events: u64,
    program_log_unknown_with_active_program: u64,
    program_log_unknown_without_active_program: u64,
    unknown_program_exact: CappedTextCounts,
    unknown_program_pattern: CappedTextCounts,
    unknown_account_exact: CappedTextCounts,
    unknown_account_pattern: CappedTextCounts,
    unparsed_exact: CappedTextCounts,
    unparsed_pattern: CappedTextCounts,
    plain_exact: CappedTextCounts,
    plain_pattern: CappedTextCounts,
    program_unknown_exact: CappedTextCounts,
    program_unknown_pattern: CappedTextCounts,
    program_unknown_by_program: CappedTextCounts,
}

impl ArchiveV2LogPatternStats {
    fn new(max_keys: usize) -> Self {
        Self {
            records: 0,
            blocks: 0,
            txs: 0,
            metadata_decoded: 0,
            metadata_raw: 0,
            metadata_none: 0,
            metadata_with_logs: 0,
            metadata_without_logs: 0,
            log_streams: 0,
            log_events: 0,
            unknown_program_events: 0,
            unknown_account_events: 0,
            unparsed_events: 0,
            plain_events: 0,
            program_log_unknown_events: 0,
            program_plain_log_unknown_events: 0,
            program_id_log_unknown_events: 0,
            program_log_unknown_with_active_program: 0,
            program_log_unknown_without_active_program: 0,
            unknown_program_exact: CappedTextCounts::new(max_keys),
            unknown_program_pattern: CappedTextCounts::new(max_keys),
            unknown_account_exact: CappedTextCounts::new(max_keys),
            unknown_account_pattern: CappedTextCounts::new(max_keys),
            unparsed_exact: CappedTextCounts::new(max_keys),
            unparsed_pattern: CappedTextCounts::new(max_keys),
            plain_exact: CappedTextCounts::new(max_keys),
            plain_pattern: CappedTextCounts::new(max_keys),
            program_unknown_exact: CappedTextCounts::new(max_keys),
            program_unknown_pattern: CappedTextCounts::new(max_keys),
            program_unknown_by_program: CappedTextCounts::new(max_keys),
        }
    }

    fn print(&self, top: usize) {
        self.unknown_program_exact
            .print("unknown_program_exact", top);
        self.unknown_program_pattern
            .print("unknown_program_pattern", top);
        self.unknown_account_exact
            .print("unknown_account_exact", top);
        self.unknown_account_pattern
            .print("unknown_account_pattern", top);
        self.unparsed_exact.print("unparsed_exact", top);
        self.unparsed_pattern.print("unparsed_pattern", top);
        self.plain_exact.print("plain_exact", top);
        self.plain_pattern.print("plain_pattern", top);
        self.program_unknown_by_program
            .print("program_unknown_by_program", top);
        self.program_unknown_exact
            .print("program_unknown_exact", top);
        self.program_unknown_pattern
            .print("program_unknown_pattern", top);
    }
}

struct CountedText {
    count: u64,
    example: String,
}

struct CappedTextCounts {
    counts: GxHashMap<String, CountedText>,
    observations: u64,
    skipped_new_keys: u64,
    max_keys: usize,
}

impl CappedTextCounts {
    fn new(max_keys: usize) -> Self {
        Self {
            counts: GxHashMap::with_capacity_and_hasher(1024, GxBuildHasher::default()),
            observations: 0,
            skipped_new_keys: 0,
            max_keys,
        }
    }

    fn bump(&mut self, key: &str, example: &str) {
        self.bump_owned(key.to_string(), example);
    }

    fn bump_owned(&mut self, key: String, example: &str) {
        self.observations += 1;
        if let Some(entry) = self.counts.get_mut(&key) {
            entry.count = entry.count.saturating_add(1);
            return;
        }
        if self.counts.len() >= self.max_keys {
            self.skipped_new_keys += 1;
            return;
        }
        self.counts.insert(
            key,
            CountedText {
                count: 1,
                example: truncate_report_text(example, 360),
            },
        );
    }

    fn print(&self, name: &'static str, top: usize) {
        println!(
            "log_pattern_bucket kind={name} observations={} distinct_tracked={} skipped_new_keys={} max_keys={}",
            self.observations,
            self.counts.len(),
            self.skipped_new_keys,
            self.max_keys
        );
        let mut rows = self.counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0)));
        for (rank, (key, entry)) in rows.into_iter().take(top).enumerate() {
            let pct = if self.observations > 0 {
                entry.count as f64 * 100.0 / self.observations as f64
            } else {
                0.0
            };
            println!(
                "log_pattern_top kind={name} rank={} count={} pct={pct:.4} key={:?} example={:?}",
                rank + 1,
                entry.count,
                truncate_report_text(key, 360),
                entry.example
            );
        }
    }
}

fn normalize_log_text_pattern(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len().min(512));
    for (idx, token) in text.split_whitespace().enumerate() {
        if idx > 0 {
            normalized.push(' ');
        }
        normalized.push_str(normalize_log_token(token));
        if normalized.len() >= 512 {
            return truncate_report_text(&normalized, 512);
        }
    }
    if normalized.is_empty() {
        "<empty>".to_string()
    } else {
        normalized
    }
}

fn normalize_log_token(token: &str) -> &str {
    let trimmed = token.trim_matches(|c: char| {
        matches!(
            c,
            ',' | ';' | ':' | '.' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
        )
    });
    if looks_like_pubkey(trimmed) {
        "<pubkey>"
    } else if looks_like_hex(trimmed) {
        "<hex>"
    } else if looks_like_decimal(trimmed) {
        "<num>"
    } else if looks_like_base64_blob(trimmed) {
        "<blob>"
    } else {
        token
    }
}

fn looks_like_pubkey(token: &str) -> bool {
    (32..=44).contains(&token.len())
        && token.bytes().all(|byte| {
            b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&byte)
        })
}

fn looks_like_hex(token: &str) -> bool {
    let Some(rest) = token
        .strip_prefix("0x")
        .or_else(|| token.strip_prefix("0X"))
    else {
        return false;
    };
    !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn looks_like_decimal(token: &str) -> bool {
    token.len() >= 3 && token.bytes().all(|byte| byte.is_ascii_digit())
}

fn looks_like_base64_blob(token: &str) -> bool {
    token.len() >= 32
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
}

fn truncate_report_text(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

pub(crate) fn inspect(input: &Path, max_blocks: Option<u64>, top: usize) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
    let archive_file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();

    let mut records = 0u64;
    let mut genesis_records = 0u64;
    let mut genesis_accounts = 0u64;
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut poh_entries = 0u64;
    let mut poh_tx_count_sum = 0u64;
    let mut poh_tx_count_zero = 0u64;
    let mut poh_tx_count_one = 0u64;
    let mut poh_tx_count_multi = 0u64;
    let mut poh_tx_count_max = 0u32;
    let mut poh_num_hashes_zero = 0u64;
    let mut poh_num_hashes_one = 0u64;
    let mut poh_num_hashes_sum = 0u128;
    let mut max_poh_entries_per_block = 0usize;
    let mut poh_entries_serialized_bytes = 0u64;
    let mut shredding_serialized_bytes = 0u64;
    let mut compact_header_serialized_bytes = 0u64;
    let mut legacy_raw_rewards_frames = 0u64;

    let mut reward_blocks = 0u64;
    let mut reward_items = 0u64;
    let mut reward_num_partitions_present = 0u64;
    let mut reward_source_bytes = 0u64;
    let mut reward_serialized_bytes = 0u64;
    let mut reward_raw_fallbacks = 0u64;
    let mut reward_raw_fallback_bytes = 0u64;
    let mut space = ArchiveV2SpaceStats::default();

    let sidecar_dir = input.parent().unwrap_or_else(|| Path::new("."));
    let poh_sidecar_path = sidecar_dir.join(POH_FILE);
    let blockhash_registry_path = sidecar_dir.join(BLOCKHASH_REGISTRY_FILE);
    let mut poh_sidecar_records = 0u64;
    let mut has_poh_sidecar = false;
    if crate::file_nonempty(&poh_sidecar_path) {
        has_poh_sidecar = true;
        let file = File::open(&poh_sidecar_path)
            .with_context(|| format!("open {}", poh_sidecar_path.display()))?;
        let mut poh_reader =
            WincodeLeb128FramedReader::new(BufReader::with_capacity(BUFFER_SIZE, file));
        while let Some((len, record)) = poh_reader.read::<WincodeArchiveV2PohRecord>()? {
            poh_sidecar_records += 1;
            poh_entries_serialized_bytes += len as u64;
            max_poh_entries_per_block = max_poh_entries_per_block.max(record.entries.len());
            poh_entries += record.entries.len() as u64;
            for entry in &record.entries {
                poh_tx_count_sum += entry.tx_count as u64;
                poh_tx_count_max = poh_tx_count_max.max(entry.tx_count);
                match entry.tx_count {
                    0 => poh_tx_count_zero += 1,
                    1 => poh_tx_count_one += 1,
                    _ => poh_tx_count_multi += 1,
                }
                poh_num_hashes_sum += entry.num_hashes as u128;
                if entry.num_hashes == 0 {
                    poh_num_hashes_zero += 1;
                } else if entry.num_hashes == 1 {
                    poh_num_hashes_one += 1;
                }
            }
        }
    }
    let blockhash_registry_bytes = std::fs::metadata(&blockhash_registry_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let blockhash_registry_entries = blockhash_registry_bytes / 32;

    while let Some((len, record)) = reader.read::<WincodeArchiveV2Record>()? {
        records += 1;
        space.observed_record_bytes += len as u64;
        let block = match record {
            WincodeArchiveV2Record::Header(_) => {
                space.add("record/header", len as u64);
                continue;
            }
            WincodeArchiveV2Record::Genesis(genesis) => {
                space.add("record/genesis", len as u64);
                genesis_records += 1;
                genesis_accounts += (genesis.accounts.len() + genesis.reward_pools.len()) as u64;
                continue;
            }
            WincodeArchiveV2Record::Block(block) => {
                space.add("record/block", len as u64);
                block
            }
            WincodeArchiveV2Record::Index(_) => {
                space.add("record/index", len as u64);
                continue;
            }
            WincodeArchiveV2Record::Footer(_) => {
                space.add("record/footer", len as u64);
                continue;
            }
        };

        blocks += 1;
        txs += block.txs.len() as u64;
        analyze_archive_v2_block_space(&block, &mut space)?;

        let compact = &block.header.compact;
        if !has_poh_sidecar {
            max_poh_entries_per_block = max_poh_entries_per_block.max(compact.poh_entries.len());
            poh_entries += compact.poh_entries.len() as u64;
            poh_entries_serialized_bytes +=
                wincode::config::serialized_size(&compact.poh_entries, wincode_leb128_config())?;
            for entry in &compact.poh_entries {
                poh_tx_count_sum += entry.tx_count as u64;
                poh_tx_count_max = poh_tx_count_max.max(entry.tx_count);
                match entry.tx_count {
                    0 => poh_tx_count_zero += 1,
                    1 => poh_tx_count_one += 1,
                    _ => poh_tx_count_multi += 1,
                }
                poh_num_hashes_sum += entry.num_hashes as u128;
                if entry.num_hashes == 0 {
                    poh_num_hashes_zero += 1;
                } else if entry.num_hashes == 1 {
                    poh_num_hashes_one += 1;
                }
            }
        }
        shredding_serialized_bytes +=
            wincode::config::serialized_size(&compact.shredding, wincode_leb128_config())?;
        compact_header_serialized_bytes +=
            wincode::config::serialized_size(compact, wincode_leb128_config())?;
        if compact.rewards.is_some() {
            legacy_raw_rewards_frames += 1;
        }

        reward_serialized_bytes +=
            wincode::config::serialized_size(&block.header.rewards, wincode_leb128_config())?;
        if let Some(rewards) = &block.header.rewards {
            reward_blocks += 1;
            reward_source_bytes += rewards.source_len;
            if rewards.num_partitions.is_some() {
                reward_num_partitions_present += 1;
            }
            if let Some(decoded) = &rewards.decoded {
                reward_items += decoded.len() as u64;
            }
            if let Some(raw) = &rewards.raw_fallback {
                reward_raw_fallbacks += 1;
                reward_raw_fallback_bytes += raw.len() as u64;
            }
        }

        if max_blocks.is_some_and(|limit| blocks >= limit) {
            break;
        }
    }

    println!(
        "archive_v2_inspect records={records} genesis_records={genesis_records} genesis_accounts={genesis_accounts} blocks={blocks} txs={txs} archive_file_bytes={archive_file_bytes} observed_record_bytes={}",
        space.observed_record_bytes
    );
    println!(
        "known_program_logs_enabled={}",
        blockzilla_format::program_logs::KNOWN_PROGRAM_LOGS_ENABLED
    );
    println!(
        "sidecars poh_path={} poh_records={} blockhash_registry_path={} blockhash_entries={} blockhash_bytes={}",
        poh_sidecar_path.display(),
        poh_sidecar_records,
        blockhash_registry_path.display(),
        blockhash_registry_entries,
        blockhash_registry_bytes
    );
    println!(
        "poh entries={poh_entries} tx_count_sum={poh_tx_count_sum} tx_count_zero={poh_tx_count_zero} tx_count_one={poh_tx_count_one} tx_count_multi={poh_tx_count_multi} tx_count_max={poh_tx_count_max} max_entries_per_block={max_poh_entries_per_block} num_hashes_zero={poh_num_hashes_zero} num_hashes_one={poh_num_hashes_one} num_hashes_sum={poh_num_hashes_sum}"
    );
    println!(
        "poh serialized_bytes={poh_entries_serialized_bytes} avg_bytes_per_entry={:.2} compact_header_bytes={compact_header_serialized_bytes} shredding_bytes={shredding_serialized_bytes}",
        if poh_entries > 0 {
            poh_entries_serialized_bytes as f64 / poh_entries as f64
        } else {
            0.0
        }
    );
    println!(
        "rewards blocks={reward_blocks} reward_items={reward_items} num_partitions_present={reward_num_partitions_present} source_bytes={reward_source_bytes} serialized_bytes={reward_serialized_bytes} raw_fallbacks={reward_raw_fallbacks} raw_fallback_bytes={reward_raw_fallback_bytes} legacy_raw_frames={legacy_raw_rewards_frames}"
    );
    space.print(top);
    Ok(())
}

macro_rules! add_wincode_size {
    ($stats:expr, $name:expr, $value:expr) => {{
        let size = wincode::config::serialized_size($value, wincode_leb128_config())?;
        $stats.add($name, size);
    }};
}

#[derive(Default)]
struct ArchiveV2SpaceStats {
    observed_record_bytes: u64,
    components: BTreeMap<&'static str, u128>,
    log_events: BTreeMap<&'static str, u64>,
    program_logs: BTreeMap<&'static str, u64>,
    log_streams: u64,
    log_events_total: u64,
    program_logs_total: u64,
    pubkey_id_refs: u64,
    pubkey_raw_refs: u64,
    recent_blockhash_ids: u64,
    recent_blockhash_nonces: u64,
    instructions: u64,
    inner_instructions: u64,
}

impl ArchiveV2SpaceStats {
    fn add(&mut self, name: &'static str, bytes: u64) {
        *self.components.entry(name).or_default() += bytes as u128;
    }

    fn bump_log_event(&mut self, name: &'static str) {
        *self.log_events.entry(name).or_default() += 1;
        self.log_events_total += 1;
    }

    fn bump_program_log(&mut self, name: &'static str) {
        *self.program_logs.entry(name).or_default() += 1;
        self.program_logs_total += 1;
    }

    fn print(&self, top: usize) {
        println!(
            "space_summary log_streams={} log_events={} program_logs={} instructions={} inner_instructions={} pubkey_id_refs={} pubkey_raw_refs={} recent_blockhash_ids={} recent_blockhash_nonces={}",
            self.log_streams,
            self.log_events_total,
            self.program_logs_total,
            self.instructions,
            self.inner_instructions,
            self.pubkey_id_refs,
            self.pubkey_raw_refs,
            self.recent_blockhash_ids,
            self.recent_blockhash_nonces
        );

        let mut components = self.components.iter().collect::<Vec<_>>();
        components.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (name, bytes) in components.into_iter().take(top.max(1)) {
            let pct = if self.observed_record_bytes > 0 {
                *bytes as f64 * 100.0 / self.observed_record_bytes as f64
            } else {
                0.0
            };
            println!("space_bucket name={name} bytes={bytes} pct_observed={pct:.2}");
        }

        let mut events = self.log_events.iter().collect::<Vec<_>>();
        events.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (name, count) in events.into_iter().take(top.max(1)) {
            println!("log_event_kind name={name} count={count}");
        }

        let mut program_logs = self.program_logs.iter().collect::<Vec<_>>();
        program_logs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (name, count) in program_logs.into_iter().take(top.max(1)) {
            println!("program_log_kind name={name} count={count}");
        }
    }
}

fn analyze_archive_v2_block_space(
    block: &WincodeArchiveV2Block,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    add_wincode_size!(stats, "block/header", &block.header);
    add_wincode_size!(stats, "block/header_compact", &block.header.compact);
    add_wincode_size!(stats, "block/header_rewards", &block.header.rewards);
    add_wincode_size!(stats, "block/tx_vec_full", &block.txs);

    for tx in &block.txs {
        add_wincode_size!(stats, "tx/index", &tx.tx_index);
        add_wincode_size!(stats, "tx/payload", &tx.tx);
        add_wincode_size!(stats, "meta/option_payload", &tx.metadata);
        analyze_tx_payload_space(&tx.tx, stats)?;
        if let Some(metadata) = &tx.metadata {
            analyze_meta_payload_space(metadata, stats)?;
        }
    }

    Ok(())
}

fn analyze_tx_payload_space(
    payload: &WincodeArchiveV2Payload<OwnedCompactTransaction>,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    match payload {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            stats.add("tx/source_bytes", *source_len);
            add_wincode_size!(stats, "tx/signatures", &value.signatures);
            add_wincode_size!(stats, "tx/message", &value.message);
            analyze_owned_message_space(&value.message, stats)?;
        }
        WincodeArchiveV2Payload::Raw { bytes, error } => {
            stats.add("tx/raw_fallback_bytes", bytes.len() as u64);
            stats.add("tx/raw_fallback_error", error.len() as u64);
        }
    }
    Ok(())
}

fn analyze_owned_message_space(
    message: &OwnedCompactMessage,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    match message {
        OwnedCompactMessage::Legacy(message) => {
            add_wincode_size!(stats, "tx/message_legacy_header", &message.header);
            add_wincode_size!(stats, "tx/account_keys", &message.account_keys);
            add_wincode_size!(stats, "tx/recent_blockhash", &message.recent_blockhash);
            add_wincode_size!(stats, "tx/instructions", &message.instructions);
            for key in &message.account_keys {
                count_compact_pubkey(*key, stats);
            }
            count_recent_blockhash(&message.recent_blockhash, stats);
            analyze_owned_instructions(&message.instructions, stats)?;
        }
        OwnedCompactMessage::V0(message) => {
            add_wincode_size!(stats, "tx/message_v0_header", &message.header);
            add_wincode_size!(stats, "tx/account_keys", &message.account_keys);
            add_wincode_size!(stats, "tx/recent_blockhash", &message.recent_blockhash);
            add_wincode_size!(stats, "tx/instructions", &message.instructions);
            add_wincode_size!(
                stats,
                "tx/address_table_lookups",
                &message.address_table_lookups
            );
            for key in &message.account_keys {
                count_compact_pubkey(*key, stats);
            }
            for lookup in &message.address_table_lookups {
                count_compact_pubkey(lookup.account_key, stats);
            }
            count_recent_blockhash(&message.recent_blockhash, stats);
            analyze_owned_instructions(&message.instructions, stats)?;
        }
    }
    Ok(())
}

fn analyze_owned_instructions(
    instructions: &[OwnedCompactInstruction],
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    stats.instructions += instructions.len() as u64;
    for instruction in instructions {
        add_wincode_size!(stats, "tx/instruction", instruction);
        stats.add(
            "tx/instruction_accounts_raw_bytes",
            instruction.accounts.len() as u64,
        );
        stats.add(
            "tx/instruction_data_raw_bytes",
            instruction.data.len() as u64,
        );
    }
    Ok(())
}

fn analyze_meta_payload_space(
    payload: &WincodeArchiveV2Payload<blockzilla_format::CompactMetaV1>,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    match payload {
        WincodeArchiveV2Payload::Decoded { source_len, value } => {
            stats.add("meta/source_bytes", *source_len);
            analyze_meta_space(value, stats)?;
        }
        WincodeArchiveV2Payload::Raw { bytes, error } => {
            stats.add("meta/raw_fallback_bytes", bytes.len() as u64);
            stats.add("meta/raw_fallback_error", error.len() as u64);
        }
    }
    Ok(())
}

fn analyze_meta_space(
    meta: &blockzilla_format::CompactMetaV1,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    add_wincode_size!(stats, "meta/err", &meta.err);
    add_wincode_size!(stats, "meta/fee", &meta.fee);
    add_wincode_size!(stats, "meta/pre_balances", &meta.pre_balances);
    add_wincode_size!(stats, "meta/post_balances", &meta.post_balances);
    add_wincode_size!(stats, "meta/inner_instructions", &meta.inner_instructions);
    add_wincode_size!(stats, "meta/logs", &meta.logs);
    add_wincode_size!(stats, "meta/pre_token_balances", &meta.pre_token_balances);
    add_wincode_size!(stats, "meta/post_token_balances", &meta.post_token_balances);
    add_wincode_size!(stats, "meta/rewards", &meta.rewards);
    add_wincode_size!(
        stats,
        "meta/loaded_writable_addresses",
        &meta.loaded_writable_addresses
    );
    add_wincode_size!(
        stats,
        "meta/loaded_readonly_addresses",
        &meta.loaded_readonly_addresses
    );
    add_wincode_size!(stats, "meta/return_data", &meta.return_data);
    add_wincode_size!(
        stats,
        "meta/compute_units_consumed",
        &meta.compute_units_consumed
    );
    add_wincode_size!(stats, "meta/cost_units", &meta.cost_units);

    if let Some(inner) = &meta.inner_instructions {
        for group in inner {
            stats.inner_instructions += group.instructions.len() as u64;
            for instruction in &group.instructions {
                stats.add(
                    "meta/inner_instruction_accounts_raw_bytes",
                    instruction.accounts.len() as u64,
                );
                stats.add(
                    "meta/inner_instruction_data_raw_bytes",
                    instruction.data.len() as u64,
                );
            }
        }
    }

    for key in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        count_compact_pubkey(*key, stats);
    }
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Some(key) = balance.mint {
            count_compact_pubkey(key, stats);
        }
        if let Some(key) = balance.owner {
            count_compact_pubkey(key, stats);
        }
        if let Some(key) = balance.program_id {
            count_compact_pubkey(key, stats);
        }
    }
    for reward in &meta.rewards {
        count_compact_pubkey(reward.pubkey, stats);
    }
    if let Some(return_data) = &meta.return_data {
        count_compact_pubkey(return_data.program_id, stats);
        stats.add("meta/return_data_raw_bytes", return_data.data.len() as u64);
    }

    if let Some(logs) = &meta.logs {
        analyze_log_stream_space(logs, stats)?;
    }

    Ok(())
}

fn analyze_log_stream_space(
    logs: &blockzilla_format::CompactLogStream,
    stats: &mut ArchiveV2SpaceStats,
) -> Result<()> {
    stats.log_streams += 1;
    add_wincode_size!(stats, "logs/events", &logs.events);
    add_wincode_size!(stats, "logs/string_table", &logs.strings);
    add_wincode_size!(stats, "logs/string_lengths", &logs.strings.lengths);
    stats.add("logs/string_utf8_bytes", logs.strings.bytes.len() as u64);
    add_wincode_size!(stats, "logs/data_table", &logs.data);
    add_wincode_size!(stats, "logs/data_arrays", &logs.data.arrays);
    add_wincode_size!(stats, "logs/data_chunk_lengths", &logs.data.chunk_lengths);
    stats.add("logs/data_raw_bytes", logs.data.bytes.len() as u64);

    for event in &logs.events {
        stats.bump_log_event(log_event_kind(event));
        match event {
            blockzilla_format::LogEvent::System(_)
            | blockzilla_format::LogEvent::ProgramAccountNotWritable
            | blockzilla_format::LogEvent::ProgramIdMismatch
            | blockzilla_format::LogEvent::ProgramNotUpgradeable
            | blockzilla_format::LogEvent::ProgramAndProgramDataAccountMismatch
            | blockzilla_format::LogEvent::ProgramWasExtendedInThisBlockAlready
            | blockzilla_format::LogEvent::LogTruncated
            | blockzilla_format::LogEvent::StakeMergingAccounts
            | blockzilla_format::LogEvent::VerifyEd25519
            | blockzilla_format::LogEvent::VerifySecp256k1
            | blockzilla_format::LogEvent::CloseContextState => {}
            blockzilla_format::LogEvent::ProgramLog(log)
            | blockzilla_format::LogEvent::ProgramPlainLog(log) => {
                stats.bump_program_log(program_log_kind(log));
            }
            blockzilla_format::LogEvent::ProgramIdLog { program, log } => {
                count_compact_pubkey(*program, stats);
                stats.bump_program_log(program_log_kind(log));
            }
            blockzilla_format::LogEvent::LoaderUpgradedProgram { program }
            | blockzilla_format::LogEvent::LoaderFinalizedAccount { account: program }
            | blockzilla_format::LogEvent::Invoke { program, .. }
            | blockzilla_format::LogEvent::BpfInvoke { program }
            | blockzilla_format::LogEvent::Consumed { program, .. }
            | blockzilla_format::LogEvent::Success { program }
            | blockzilla_format::LogEvent::BpfSuccess { program }
            | blockzilla_format::LogEvent::Failure { program, .. }
            | blockzilla_format::LogEvent::BpfFailure { program, .. }
            | blockzilla_format::LogEvent::FailureCustomProgramError { program, .. }
            | blockzilla_format::LogEvent::BpfFailureCustomProgramError { program, .. }
            | blockzilla_format::LogEvent::FailureInvalidAccountData { program }
            | blockzilla_format::LogEvent::BpfFailureInvalidAccountData { program }
            | blockzilla_format::LogEvent::FailureInvalidProgramArgument { program }
            | blockzilla_format::LogEvent::BpfFailureInvalidProgramArgument { program }
            | blockzilla_format::LogEvent::RuntimeWritablePrivilegeEscalated { account: program }
            | blockzilla_format::LogEvent::RuntimeSignerPrivilegeEscalated { account: program }
            | blockzilla_format::LogEvent::RuntimeAccountOwnerBalanceVerificationFailed {
                account: program,
            } => {
                count_compact_pubkey(*program, stats);
            }
            blockzilla_format::LogEvent::Return { program, .. } => {
                count_compact_pubkey(*program, stats);
            }
            blockzilla_format::LogEvent::ProgramNotDeployed { program }
            | blockzilla_format::LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    count_compact_pubkey(*program, stats);
                }
            }
            blockzilla_format::LogEvent::ProgramLogError { .. }
            | blockzilla_format::LogEvent::FailedToComplete { .. }
            | blockzilla_format::LogEvent::CustomProgramError { .. }
            | blockzilla_format::LogEvent::Data { .. }
            | blockzilla_format::LogEvent::Consumption { .. }
            | blockzilla_format::LogEvent::CbRequestUnits { .. }
            | blockzilla_format::LogEvent::UnknownProgram { .. }
            | blockzilla_format::LogEvent::UnknownAccount { .. }
            | blockzilla_format::LogEvent::Plain { .. }
            | blockzilla_format::LogEvent::Unparsed { .. }
            | blockzilla_format::LogEvent::BpfConsumed { .. } => {}
        }
    }

    Ok(())
}

fn count_compact_pubkey(key: CompactPubkey, stats: &mut ArchiveV2SpaceStats) {
    match key {
        CompactPubkey::Id(_) => stats.pubkey_id_refs += 1,
        CompactPubkey::Raw(_) => stats.pubkey_raw_refs += 1,
    }
}

fn count_recent_blockhash(hash: &OwnedCompactRecentBlockhash, stats: &mut ArchiveV2SpaceStats) {
    match hash {
        OwnedCompactRecentBlockhash::Id(_) => stats.recent_blockhash_ids += 1,
        OwnedCompactRecentBlockhash::Nonce(_) => stats.recent_blockhash_nonces += 1,
    }
}

fn log_event_kind(event: &blockzilla_format::LogEvent) -> &'static str {
    match event {
        blockzilla_format::LogEvent::System(_) => "System",
        blockzilla_format::LogEvent::LogTruncated => "LogTruncated",
        blockzilla_format::LogEvent::StakeMergingAccounts => "StakeMergingAccounts",
        blockzilla_format::LogEvent::LoaderUpgradedProgram { .. } => "LoaderUpgradedProgram",
        blockzilla_format::LogEvent::LoaderFinalizedAccount { .. } => "LoaderFinalizedAccount",
        blockzilla_format::LogEvent::ProgramLog(_) => "ProgramLog",
        blockzilla_format::LogEvent::ProgramLogError { .. } => "ProgramLogError",
        blockzilla_format::LogEvent::ProgramIdLog { .. } => "ProgramIdLog",
        blockzilla_format::LogEvent::ProgramPlainLog(_) => "ProgramPlainLog",
        blockzilla_format::LogEvent::ProgramAccountNotWritable => "ProgramAccountNotWritable",
        blockzilla_format::LogEvent::ProgramIdMismatch => "ProgramIdMismatch",
        blockzilla_format::LogEvent::ProgramNotUpgradeable => "ProgramNotUpgradeable",
        blockzilla_format::LogEvent::ProgramAndProgramDataAccountMismatch => {
            "ProgramAndProgramDataAccountMismatch"
        }
        blockzilla_format::LogEvent::ProgramWasExtendedInThisBlockAlready => {
            "ProgramWasExtendedInThisBlockAlready"
        }
        blockzilla_format::LogEvent::Invoke { .. } => "Invoke",
        blockzilla_format::LogEvent::BpfInvoke { .. } => "BpfInvoke",
        blockzilla_format::LogEvent::Consumed { .. } => "Consumed",
        blockzilla_format::LogEvent::BpfConsumed { .. } => "BpfConsumed",
        blockzilla_format::LogEvent::Success { .. } => "Success",
        blockzilla_format::LogEvent::BpfSuccess { .. } => "BpfSuccess",
        blockzilla_format::LogEvent::Failure { .. } => "Failure",
        blockzilla_format::LogEvent::BpfFailure { .. } => "BpfFailure",
        blockzilla_format::LogEvent::FailureCustomProgramError { .. } => {
            "FailureCustomProgramError"
        }
        blockzilla_format::LogEvent::BpfFailureCustomProgramError { .. } => {
            "BpfFailureCustomProgramError"
        }
        blockzilla_format::LogEvent::FailureInvalidAccountData { .. } => {
            "FailureInvalidAccountData"
        }
        blockzilla_format::LogEvent::BpfFailureInvalidAccountData { .. } => {
            "BpfFailureInvalidAccountData"
        }
        blockzilla_format::LogEvent::FailureInvalidProgramArgument { .. } => {
            "FailureInvalidProgramArgument"
        }
        blockzilla_format::LogEvent::BpfFailureInvalidProgramArgument { .. } => {
            "BpfFailureInvalidProgramArgument"
        }
        blockzilla_format::LogEvent::FailedToComplete { .. } => "FailedToComplete",
        blockzilla_format::LogEvent::CustomProgramError { .. } => "CustomProgramError",
        blockzilla_format::LogEvent::Return { .. } => "Return",
        blockzilla_format::LogEvent::Data { .. } => "Data",
        blockzilla_format::LogEvent::Consumption { .. } => "Consumption",
        blockzilla_format::LogEvent::CbRequestUnits { .. } => "CbRequestUnits",
        blockzilla_format::LogEvent::ProgramNotDeployed { .. } => "ProgramNotDeployed",
        blockzilla_format::LogEvent::ProgramNotCached { .. } => "ProgramNotCached",
        blockzilla_format::LogEvent::UnknownProgram { .. } => "UnknownProgram",
        blockzilla_format::LogEvent::UnknownAccount { .. } => "UnknownAccount",
        blockzilla_format::LogEvent::VerifyEd25519 => "VerifyEd25519",
        blockzilla_format::LogEvent::VerifySecp256k1 => "VerifySecp256k1",
        blockzilla_format::LogEvent::RuntimeWritablePrivilegeEscalated { .. } => {
            "RuntimeWritablePrivilegeEscalated"
        }
        blockzilla_format::LogEvent::RuntimeSignerPrivilegeEscalated { .. } => {
            "RuntimeSignerPrivilegeEscalated"
        }
        blockzilla_format::LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { .. } => {
            "RuntimeAccountOwnerBalanceVerificationFailed"
        }
        blockzilla_format::LogEvent::CloseContextState => "CloseContextState",
        blockzilla_format::LogEvent::Plain { .. } => "Plain",
        blockzilla_format::LogEvent::Unparsed { .. } => "Unparsed",
    }
}

fn program_log_kind(log: &blockzilla_format::program_logs::ProgramLog) -> &'static str {
    match log {
        blockzilla_format::program_logs::ProgramLog::Empty => "Empty",
        blockzilla_format::program_logs::ProgramLog::Token(_) => "Token",
        blockzilla_format::program_logs::ProgramLog::Token2022(_) => "Token2022",
        blockzilla_format::program_logs::ProgramLog::Ata(_) => "Ata",
        blockzilla_format::program_logs::ProgramLog::AddressLookupTable(_) => "AddressLookupTable",
        blockzilla_format::program_logs::ProgramLog::LoaderV3(_) => "LoaderV3",
        blockzilla_format::program_logs::ProgramLog::LoaderV4(_) => "LoaderV4",
        blockzilla_format::program_logs::ProgramLog::Memo(_) => "Memo",
        blockzilla_format::program_logs::ProgramLog::Record(_) => "Record",
        blockzilla_format::program_logs::ProgramLog::TransferHook(_) => "TransferHook",
        blockzilla_format::program_logs::ProgramLog::AccountCompression(_) => "AccountCompression",
        blockzilla_format::program_logs::ProgramLog::Stake(_) => "Stake",
        blockzilla_format::program_logs::ProgramLog::ZkElgamalProof(_) => "ZkElgamalProof",
        blockzilla_format::program_logs::ProgramLog::AnchorInstruction { .. } => {
            "AnchorInstruction"
        }
        blockzilla_format::program_logs::ProgramLog::AnchorErrorOccurred { .. } => {
            "AnchorErrorOccurred"
        }
        blockzilla_format::program_logs::ProgramLog::AnchorErrorThrown { .. } => {
            "AnchorErrorThrown"
        }
        blockzilla_format::program_logs::ProgramLog::Unknown(_) => "Unknown",
        blockzilla_format::program_logs::ProgramLog::Known(known) => known_program_log_kind(known),
    }
}

fn known_program_log_kind(
    log: &blockzilla_format::program_logs::known_programs::KnownProgramLog,
) -> &'static str {
    match log {
        blockzilla_format::program_logs::known_programs::KnownProgramLog::Drift(_) => "Known/Drift",
        blockzilla_format::program_logs::known_programs::KnownProgramLog::OkxRouter(_) => {
            "Known/OkxRouter"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::PhoenixPerps(_) => {
            "Known/PhoenixPerps"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::PhoenixV1(_) => {
            "Known/PhoenixV1"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::RaydiumAmm(_) => {
            "Known/RaydiumAmm"
        }
        blockzilla_format::program_logs::known_programs::KnownProgramLog::Static(_) => {
            "Known/Static"
        }
    }
}

struct ArchiveV2BlockSidecar {
    poh_entries: Vec<CompactPohEntry>,
    blockhash: [u8; 32],
}

#[derive(Clone, Copy)]
struct FastEntrySummary {
    num_hashes: u64,
    hash: [u8; 32],
    tx_count: u32,
}

fn block_sidecar_from_entries(
    slot: u64,
    entries: &[PendingEntry],
) -> Result<ArchiveV2BlockSidecar> {
    anyhow::ensure!(!entries.is_empty(), "slot {slot} block has no PoH entries");
    let poh_entries = entries
        .iter()
        .map(|entry| CompactPohEntry {
            num_hashes: entry.entry.num_hashes,
            hash: entry.entry.hash,
            tx_count: entry.entry.transactions.len() as u32,
        })
        .collect::<Vec<_>>();
    let blockhash = poh_entries
        .last()
        .map(|entry| entry.hash)
        .ok_or_else(|| anyhow!("slot {slot} block has no PoH entries"))?;
    Ok(ArchiveV2BlockSidecar {
        poh_entries,
        blockhash,
    })
}

fn decode_block_entry_cids(payload: &[u8]) -> Result<(u64, Vec<Cid36>)> {
    let mut decoder = minicbor::Decoder::new(payload);
    let array_len = decoder
        .array()?
        .ok_or_else(|| anyhow!("indefinite block arrays are not supported"))?;
    anyhow::ensure!(array_len >= 4, "block array too short: {array_len}");
    let kind = decoder.u64()?;
    anyhow::ensure!(kind == 2, "expected block kind 2, got {kind}");
    let slot = decoder.u64()?;
    decoder.skip()?;

    let entry_count = decoder
        .array()?
        .ok_or_else(|| anyhow!("indefinite block entry arrays are not supported"))?;
    let entry_capacity =
        usize::try_from(entry_count).context("block entry count exceeds usize::MAX")?;
    let mut entries = Vec::with_capacity(entry_capacity);
    for index in 0..entry_count {
        let cid_ref: CborCidRef<'_> = decoder.decode_with(&mut ())?;
        let cid = Cid36::from_ref_bytes(cid_ref.bytes)
            .ok_or_else(|| anyhow!("slot {slot} entry ref #{index} is not a CAR CID"))?;
        entries.push(cid);
    }

    Ok((slot, entries))
}

fn hash_32(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    anyhow::ensure!(
        bytes.len() == out.len(),
        "expected 32 byte hash, got {}",
        bytes.len()
    );
    out.copy_from_slice(bytes);
    Ok(out)
}

fn build_block_record(
    pending: &mut PendingBlock,
    block: RawBlockNode,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    block_id: u32,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) -> Result<(WincodeArchiveV2Block, u32, ArchiveV2BlockSidecar)> {
    pending.last_slot = block.slot;
    let entries = ordered_entries(pending, &block)?;
    let sidecar = block_sidecar_from_entries(block.slot, entries)?;
    validate_ordered_transactions(pending, entries, block.slot)?;
    let ordered_txs = &pending.transactions;
    let mut zstd = ZstdReusableDecoder::new();
    let rewards = block_rewards(pending, &block, key_index, &mut zstd, footer, timings)?;

    let compact_header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time: block.meta.blocktime,
        block_height: block.meta.block_height,
        shredding: block
            .shredding
            .iter()
            .map(|item| CompactShredding {
                entry_end_idx: item.entry_end_idx,
                shred_end_idx: item.shred_end_idx,
            })
            .collect(),
        poh_entries: Vec::new(),
        rewards: None,
    };

    let mut txs = Vec::with_capacity(ordered_txs.len());
    let mut tx_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut reassemble_visited = HashSet::new();
    for (tx_index, pending_tx) in ordered_txs.iter().enumerate() {
        let assemble_started = Instant::now();
        pending_tx
            .tx
            .transaction_bytes_into(&pending.dataframes, &mut tx_bytes, &mut reassemble_visited)
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble transaction bytes",
                    block.slot
                )
            })?;
        timings.dataframe_assemble += assemble_started.elapsed();
        footer.tx_source_bytes += tx_bytes.len() as u64;
        timings.tx_reassembled += 1;
        timings.tx_scratch_max = timings.tx_scratch_max.max(tx_bytes.capacity());
        let tx_started = Instant::now();
        let value = wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes)
            .map_err(|err| anyhow!("{err}"))
            .and_then(|versioned| {
                to_owned_compact_transaction(
                    block.slot,
                    tx_index,
                    &versioned,
                    key_index,
                    rolling_blockhashes,
                    &mut footer.nonce_recent_blockhashes,
                )
            })
            .with_context(|| format!("slot {} tx#{tx_index} transaction", block.slot))?;
        let tx = WincodeArchiveV2Payload::Decoded {
            source_len: tx_bytes.len() as u64,
            value,
        };
        timings.tx_decode_compact += tx_started.elapsed();

        let assemble_started = Instant::now();
        pending_tx
            .tx
            .metadata_bytes_into(
                &pending.dataframes,
                &mut metadata_bytes,
                &mut reassemble_visited,
            )
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble metadata bytes",
                    block.slot
                )
            })?;
        timings.dataframe_assemble += assemble_started.elapsed();
        footer.metadata_source_bytes += metadata_bytes.len() as u64;
        timings.metadata_reassembled += 1;
        timings.metadata_scratch_max = timings.metadata_scratch_max.max(metadata_bytes.capacity());
        let metadata = if metadata_bytes.is_empty() {
            None
        } else {
            let metadata_started = Instant::now();
            let payload = decode_metadata_payload(
                block.slot,
                tx_index,
                &metadata_bytes,
                key_index,
                &mut zstd,
                timings,
            )?;
            timings.metadata_decode_compact += metadata_started.elapsed();
            Some(payload)
        };

        txs.push(WincodeArchiveV2Transaction {
            tx_index: pending_tx.tx.index.unwrap_or(tx_index as u64) as u32,
            tx,
            metadata,
        });
    }

    Ok((
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader {
                compact: compact_header,
                rewards,
            },
            txs,
        },
        ordered_txs.len() as u32,
        sidecar,
    ))
}

fn build_no_registry_block_record(
    pending: &mut PendingBlock,
    block: RawBlockNode,
    block_id: u32,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) -> Result<(WincodeArchiveV2NoRegistryBlock, u32, ArchiveV2BlockSidecar)> {
    pending.last_slot = block.slot;
    let entries = ordered_entries(pending, &block)?;
    let sidecar = block_sidecar_from_entries(block.slot, entries)?;
    validate_ordered_transactions(pending, entries, block.slot)?;
    let ordered_txs = &pending.transactions;
    let mut zstd = ZstdReusableDecoder::new();
    let rewards = block_rewards_no_registry(pending, &block, &mut zstd, footer, timings)?;

    let compact_header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.meta.parent_slot.unwrap_or(0),
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time: block.meta.blocktime,
        block_height: block.meta.block_height,
        shredding: block
            .shredding
            .iter()
            .map(|item| CompactShredding {
                entry_end_idx: item.entry_end_idx,
                shred_end_idx: item.shred_end_idx,
            })
            .collect(),
        poh_entries: Vec::new(),
        rewards: None,
    };

    let mut txs = Vec::with_capacity(ordered_txs.len());
    let mut tx_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut reassemble_visited = HashSet::new();
    for (tx_index, pending_tx) in ordered_txs.iter().enumerate() {
        let assemble_started = Instant::now();
        pending_tx
            .tx
            .transaction_bytes_into(&pending.dataframes, &mut tx_bytes, &mut reassemble_visited)
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble transaction bytes",
                    block.slot
                )
            })?;
        timings.dataframe_assemble += assemble_started.elapsed();
        footer.tx_source_bytes += tx_bytes.len() as u64;
        timings.tx_reassembled += 1;
        timings.tx_scratch_max = timings.tx_scratch_max.max(tx_bytes.capacity());
        let tx_started = Instant::now();
        let value = wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes)
            .map_err(|err| anyhow!("{err}"))
            .map(|versioned| to_no_registry_transaction(&versioned))
            .with_context(|| format!("slot {} tx#{tx_index} transaction", block.slot))?;
        let tx = WincodeArchiveV2Payload::Decoded {
            source_len: tx_bytes.len() as u64,
            value,
        };
        timings.tx_decode_compact += tx_started.elapsed();

        let assemble_started = Instant::now();
        pending_tx
            .tx
            .metadata_bytes_into(
                &pending.dataframes,
                &mut metadata_bytes,
                &mut reassemble_visited,
            )
            .with_context(|| {
                format!(
                    "slot {} tx#{tx_index} reassemble metadata bytes",
                    block.slot
                )
            })?;
        timings.dataframe_assemble += assemble_started.elapsed();
        footer.metadata_source_bytes += metadata_bytes.len() as u64;
        timings.metadata_reassembled += 1;
        timings.metadata_scratch_max = timings.metadata_scratch_max.max(metadata_bytes.capacity());
        let metadata = if metadata_bytes.is_empty() {
            None
        } else {
            let metadata_started = Instant::now();
            let payload = decode_no_registry_metadata_payload(
                block.slot,
                tx_index,
                &metadata_bytes,
                &mut zstd,
                timings,
            )?;
            timings.metadata_decode_compact += metadata_started.elapsed();
            Some(payload)
        };

        txs.push(WincodeArchiveV2NoRegistryTransaction {
            tx_index: pending_tx.tx.index.unwrap_or(tx_index as u64) as u32,
            tx,
            metadata,
        });
    }

    Ok((
        WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: compact_header,
                rewards,
            },
            txs,
        },
        ordered_txs.len() as u32,
        sidecar,
    ))
}

fn to_owned_compact_transaction(
    slot: u64,
    tx_index: usize,
    vtx: &VersionedTransaction<'_>,
    key_index: &KeyIndex,
    rolling_blockhashes: &RollingBlockhashIndex,
    nonce_recent_blockhashes: &mut u64,
) -> Result<OwnedCompactTransaction> {
    let signatures = vtx.signatures.iter().map(|sig| sig.to_vec()).collect();
    let message = match &vtx.message {
        VersionedMessage::Legacy(message) => {
            OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                account_keys: message
                    .account_keys
                    .iter()
                    .map(|key| key_index.compact(key))
                    .collect(),
                recent_blockhash: resolve_recent_blockhash(
                    rolling_blockhashes,
                    message.recent_blockhash,
                    slot,
                    tx_index,
                    nonce_recent_blockhashes,
                )?,
                instructions: message.instructions.iter().map(owned_instruction).collect(),
            })
        }
        VersionedMessage::V0(message) => OwnedCompactMessage::V0(OwnedCompactV0Message {
            header: CompactMessageHeader {
                num_required_signatures: message.header.num_required_signatures,
                num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
            },
            account_keys: message
                .account_keys
                .iter()
                .map(|key| key_index.compact(key))
                .collect(),
            recent_blockhash: resolve_recent_blockhash(
                rolling_blockhashes,
                message.recent_blockhash,
                slot,
                tx_index,
                nonce_recent_blockhashes,
            )?,
            instructions: message.instructions.iter().map(owned_instruction).collect(),
            address_table_lookups: message
                .address_table_lookups
                .iter()
                .map(|lookup| OwnedCompactAddressTableLookup {
                    account_key: key_index.compact(lookup.account_key),
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                })
                .collect(),
        }),
    };

    Ok(OwnedCompactTransaction {
        signatures,
        message,
    })
}

fn owned_instruction(
    instruction: &of_car_reader::versioned_transaction::CompiledInstruction,
) -> OwnedCompactInstruction {
    OwnedCompactInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts.clone(),
        data: instruction.data.clone(),
    }
}

fn to_no_registry_transaction(vtx: &VersionedTransaction<'_>) -> WincodeArchiveV2NoRegistryTx {
    let signatures = vtx.signatures.iter().map(|sig| sig.to_vec()).collect();
    let message = match &vtx.message {
        VersionedMessage::Legacy(message) => {
            WincodeArchiveV2NoRegistryMessage::Legacy(WincodeArchiveV2NoRegistryLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                account_keys: message.account_keys.iter().map(|key| **key).collect(),
                recent_blockhash: *message.recent_blockhash,
                instructions: message
                    .instructions
                    .iter()
                    .map(no_registry_instruction)
                    .collect(),
            })
        }
        VersionedMessage::V0(message) => {
            WincodeArchiveV2NoRegistryMessage::V0(WincodeArchiveV2NoRegistryV0Message {
                header: CompactMessageHeader {
                    num_required_signatures: message.header.num_required_signatures,
                    num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
                    num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
                },
                account_keys: message.account_keys.iter().map(|key| **key).collect(),
                recent_blockhash: *message.recent_blockhash,
                instructions: message
                    .instructions
                    .iter()
                    .map(no_registry_instruction)
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .iter()
                    .map(|lookup| WincodeArchiveV2NoRegistryAddressTableLookup {
                        account_key: *lookup.account_key,
                        writable_indexes: lookup.writable_indexes.clone(),
                        readonly_indexes: lookup.readonly_indexes.clone(),
                    })
                    .collect(),
            })
        }
    };

    WincodeArchiveV2NoRegistryTx {
        signatures,
        message,
    }
}

fn no_registry_instruction(
    instruction: &of_car_reader::versioned_transaction::CompiledInstruction,
) -> WincodeArchiveV2NoRegistryInstruction {
    WincodeArchiveV2NoRegistryInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts.clone(),
        data: instruction.data.clone(),
    }
}

fn decode_metadata_payload(
    slot: u64,
    tx_index: usize,
    metadata_bytes: &[u8],
    key_index: &KeyIndex,
    zstd: &mut ZstdReusableDecoder,
    timings: &mut ArchiveV2Timings,
) -> Result<WincodeArchiveV2Payload<blockzilla_format::CompactMetaV1>> {
    let mut meta = TransactionStatusMeta::default();
    let decoded = if slot_uses_protobuf_metadata(slot) {
        timings.metadata_protobuf_visit += 1;
        let protobuf_bytes = if zstd
            .decompress_if_zstd(metadata_bytes)
            .with_context(|| format!("slot {slot} tx#{tx_index} metadata zstd"))?
        {
            zstd.output()
        } else {
            metadata_bytes
        };
        blockzilla_format::compact_meta_from_protobuf_visit(protobuf_bytes, key_index)
    } else {
        timings.metadata_owned_fallback += 1;
        decode_transaction_status_meta_from_frame(slot, metadata_bytes, &mut meta, zstd)
            .map_err(|err| anyhow!("{err}"))
            .and_then(|()| blockzilla_format::compact_meta_from_proto(&meta, key_index))
    };

    let value = decoded.with_context(|| format!("slot {slot} tx#{tx_index} metadata"))?;
    Ok(WincodeArchiveV2Payload::Decoded {
        source_len: metadata_bytes.len() as u64,
        value,
    })
}

fn decode_no_registry_metadata_payload(
    slot: u64,
    tx_index: usize,
    metadata_bytes: &[u8],
    zstd: &mut ZstdReusableDecoder,
    timings: &mut ArchiveV2Timings,
) -> Result<WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryMeta>> {
    let mut meta = TransactionStatusMeta::default();
    let decoded = if slot_uses_protobuf_metadata(slot) {
        timings.metadata_protobuf_visit += 1;
        let protobuf_bytes = if zstd
            .decompress_if_zstd(metadata_bytes)
            .with_context(|| format!("slot {slot} tx#{tx_index} metadata zstd"))?
        {
            zstd.output()
        } else {
            metadata_bytes
        };
        no_registry_meta_from_protobuf_visit(protobuf_bytes)
    } else {
        timings.metadata_owned_fallback += 1;
        decode_transaction_status_meta_from_frame(slot, metadata_bytes, &mut meta, zstd)
            .map_err(|err| anyhow!("{err}"))
            .and_then(|()| no_registry_meta_from_proto(&meta))
    };

    let value = decoded.with_context(|| format!("slot {slot} tx#{tx_index} metadata"))?;
    Ok(WincodeArchiveV2Payload::Decoded {
        source_len: metadata_bytes.len() as u64,
        value,
    })
}

fn no_registry_meta_from_protobuf_visit(bytes: &[u8]) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let mut visitor = NoRegistryMetaVisitor::default();
    visit_protobuf_transaction_status_meta(bytes, &mut visitor)
        .map_err(|err| anyhow!("protobuf visit: {err}"))?;
    visitor.finish()
}

fn no_registry_meta_from_proto(
    meta: &TransactionStatusMeta,
) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let err = meta.err.as_ref().map(|err| err.err.clone());
    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        Some(
            meta.inner_instructions
                .iter()
                .map(|group| CompactInnerInstructions {
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
                })
                .collect(),
        )
    };
    let logs = if meta.log_messages_none {
        None
    } else {
        Some(meta.log_messages.clone())
    };
    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .map(no_registry_token_balance_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let post_token_balances = meta
        .post_token_balances
        .iter()
        .map(no_registry_token_balance_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let rewards = meta
        .rewards
        .iter()
        .map(no_registry_reward_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let loaded_writable_addresses = meta
        .loaded_writable_addresses
        .iter()
        .map(|address| bytes_to_pubkey(address, "loaded writable address"))
        .collect::<Result<Vec<_>>>()?;
    let loaded_readonly_addresses = meta
        .loaded_readonly_addresses
        .iter()
        .map(|address| bytes_to_pubkey(address, "loaded readonly address"))
        .collect::<Result<Vec<_>>>()?;
    let return_data = if meta.return_data_none {
        None
    } else {
        meta.return_data
            .as_ref()
            .map(
                |return_data| -> Result<WincodeArchiveV2NoRegistryReturnData> {
                    Ok(WincodeArchiveV2NoRegistryReturnData {
                        program_id: bytes_to_pubkey(
                            &return_data.program_id,
                            "return data program id",
                        )?,
                        data: return_data.data.clone(),
                    })
                },
            )
            .transpose()?
    };

    Ok(WincodeArchiveV2NoRegistryMeta {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        inner_instructions,
        logs,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn block_rewards(
    pending: &PendingBlock,
    block: &RawBlockNode,
    key_index: &KeyIndex,
    zstd: &mut ZstdReusableDecoder,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) -> Result<Option<WincodeArchiveV2Rewards>> {
    let Some(rewards_ref) = block.rewards.as_ref() else {
        return Ok(None);
    };
    let assemble_started = Instant::now();
    let bytes = if let Some(cid) = rewards_ref.cid {
        let rewards = pending
            .rewards
            .as_ref()
            .filter(|rewards| rewards.rewards.cid == cid)
            .ok_or_else(|| anyhow!("slot {} missing rewards node {}", block.slot, cid))?;
        rewards.rewards.rewards_bytes(&pending.dataframes)?
    } else if let Some(inline) = rewards_ref.inline_raw_bytes() {
        inline.to_vec()
    } else {
        anyhow::bail!("slot {} unsupported rewards reference", block.slot);
    };
    timings.dataframe_assemble += assemble_started.elapsed();
    footer.rewards_source_bytes += bytes.len() as u64;

    let rewards_started = Instant::now();
    let mut decoded = Rewards::default();
    match decode_rewards_from_frame(&bytes, &mut decoded, zstd).map(|()| decoded) {
        Ok(decoded) => {
            let num_partitions = decoded
                .num_partitions
                .as_ref()
                .map(|partitions| partitions.num_partitions);
            let mut out = Vec::with_capacity(decoded.rewards.len());
            for (reward_index, reward) in decoded.rewards.into_iter().enumerate() {
                out.push(compact_reward_from_proto(&reward, key_index).with_context(|| {
                    format!(
                        "slot {} reward#{reward_index} pubkey_len={} reward_type={} lamports={} post_balance={}",
                        block.slot,
                        reward.pubkey.len(),
                        reward.reward_type,
                        reward.lamports,
                        reward.post_balance
                    )
                })?);
            }
            let result = Ok(Some(WincodeArchiveV2Rewards {
                source_len: bytes.len() as u64,
                num_partitions,
                decoded: Some(out),
                raw_fallback: None,
                decode_error: None,
            }));
            timings.rewards_decode_compact += rewards_started.elapsed();
            result
        }
        Err(err) => {
            timings.rewards_decode_compact += rewards_started.elapsed();
            anyhow::bail!("slot {} rewards: {err}", block.slot);
        }
    }
}

fn block_rewards_no_registry(
    pending: &PendingBlock,
    block: &RawBlockNode,
    zstd: &mut ZstdReusableDecoder,
    footer: &mut WincodeArchiveV2Footer,
    timings: &mut ArchiveV2Timings,
) -> Result<Option<WincodeArchiveV2NoRegistryRewards>> {
    let Some(rewards_ref) = block.rewards.as_ref() else {
        return Ok(None);
    };
    let assemble_started = Instant::now();
    let bytes = if let Some(cid) = rewards_ref.cid {
        let rewards = pending
            .rewards
            .as_ref()
            .filter(|rewards| rewards.rewards.cid == cid)
            .ok_or_else(|| anyhow!("slot {} missing rewards node {}", block.slot, cid))?;
        rewards.rewards.rewards_bytes(&pending.dataframes)?
    } else if let Some(inline) = rewards_ref.inline_raw_bytes() {
        inline.to_vec()
    } else {
        anyhow::bail!("slot {} unsupported rewards reference", block.slot);
    };
    timings.dataframe_assemble += assemble_started.elapsed();
    footer.rewards_source_bytes += bytes.len() as u64;

    let rewards_started = Instant::now();
    let mut decoded = Rewards::default();
    match decode_rewards_from_frame(&bytes, &mut decoded, zstd).map(|()| decoded) {
        Ok(decoded) => {
            let num_partitions = decoded
                .num_partitions
                .as_ref()
                .map(|partitions| partitions.num_partitions);
            let mut out = Vec::with_capacity(decoded.rewards.len());
            for (reward_index, reward) in decoded.rewards.into_iter().enumerate() {
                out.push(no_registry_reward_from_proto(&reward).with_context(|| {
                    format!(
                        "slot {} reward#{reward_index} pubkey_len={} reward_type={} lamports={} post_balance={}",
                        block.slot,
                        reward.pubkey.len(),
                        reward.reward_type,
                        reward.lamports,
                        reward.post_balance
                    )
                })?);
            }
            let result = Ok(Some(WincodeArchiveV2NoRegistryRewards {
                source_len: bytes.len() as u64,
                num_partitions,
                decoded: Some(out),
                raw_fallback: None,
                decode_error: None,
            }));
            timings.rewards_decode_compact += rewards_started.elapsed();
            result
        }
        Err(err) => {
            timings.rewards_decode_compact += rewards_started.elapsed();
            anyhow::bail!("slot {} rewards: {err}", block.slot);
        }
    }
}

fn compact_reward_from_proto(
    reward: &of_car_reader::confirmed_block::Reward,
    key_index: &KeyIndex,
) -> Result<CompactReward> {
    let pubkey = Pubkey::from_str(&reward.pubkey)
        .with_context(|| format!("parse reward pubkey {}", reward.pubkey))?
        .to_bytes();
    Ok(CompactReward {
        pubkey: key_index.compact(&pubkey),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission.parse::<u8>().ok(),
    })
}

fn no_registry_token_balance_from_proto(
    tb: &of_car_reader::confirmed_block::TokenBalance,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let (amount, decimals) = match &tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: tb.account_index,
        mint: optional_pubkey(&tb.mint)?,
        owner: optional_pubkey(&tb.owner)?,
        program_id: optional_pubkey(&tb.program_id)?,
        amount,
        decimals,
    })
}

fn no_registry_token_balance_from_visit(
    tb: TokenBalanceVisit<'_>,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let (amount, decimals) = match tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: tb.account_index,
        mint: optional_pubkey(tb.mint)?,
        owner: optional_pubkey(tb.owner)?,
        program_id: optional_pubkey(tb.program_id)?,
        amount,
        decimals,
    })
}

fn no_registry_reward_from_proto(
    reward: &of_car_reader::confirmed_block::Reward,
) -> Result<WincodeArchiveV2NoRegistryReward> {
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: Pubkey::from_str(&reward.pubkey)
            .with_context(|| format!("parse reward pubkey {}", reward.pubkey))?
            .to_bytes(),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission.parse::<u8>().ok(),
    })
}

fn optional_pubkey(value: &str) -> Result<Option<[u8; 32]>> {
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            Pubkey::from_str(value)
                .with_context(|| format!("parse pubkey {value}"))?
                .to_bytes(),
        ))
    }
}

fn bytes_to_pubkey(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .with_context(|| format!("{label} has invalid length {}", bytes.len()))
}

#[derive(Default)]
struct NoRegistryMetaVisitor {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Vec<CompactInnerInstructions>,
    inner_instructions_none: bool,
    log_messages: Vec<String>,
    log_messages_none: bool,
    pre_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    post_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    rewards: Vec<WincodeArchiveV2NoRegistryReward>,
    loaded_writable_addresses: Vec<[u8; 32]>,
    loaded_readonly_addresses: Vec<[u8; 32]>,
    return_data: Option<WincodeArchiveV2NoRegistryReturnData>,
    return_data_none: bool,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
    error: Option<anyhow::Error>,
}

impl NoRegistryMetaVisitor {
    fn record_error(&mut self, err: anyhow::Error) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    fn finish(self) -> Result<WincodeArchiveV2NoRegistryMeta> {
        if let Some(err) = self.error {
            return Err(err);
        }

        let inner_instructions = if self.inner_instructions_none {
            None
        } else {
            Some(self.inner_instructions)
        };
        let logs = if self.log_messages_none {
            None
        } else {
            Some(self.log_messages)
        };
        let return_data = if self.return_data_none {
            None
        } else {
            self.return_data
        };

        Ok(WincodeArchiveV2NoRegistryMeta {
            err: self.err,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions,
            logs,
            pre_token_balances: self.pre_token_balances,
            post_token_balances: self.post_token_balances,
            rewards: self.rewards,
            loaded_writable_addresses: self.loaded_writable_addresses,
            loaded_readonly_addresses: self.loaded_readonly_addresses,
            return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        })
    }
}

impl<'a> TransactionStatusMetaVisitor<'a> for NoRegistryMetaVisitor {
    #[inline]
    fn wants_status_error(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_inner_instructions(&self) -> bool {
        true
    }

    #[inline]
    fn wants_log_messages(&self) -> bool {
        true
    }

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
    fn status_error(&mut self, err: &'a [u8]) {
        self.err = Some(err.to_vec());
    }

    #[inline]
    fn fee(&mut self, fee: u64) {
        self.fee = fee;
    }

    #[inline]
    fn pre_balance(&mut self, _index: usize, lamports: u64) {
        self.pre_balances.push(lamports);
    }

    #[inline]
    fn post_balance(&mut self, _index: usize, lamports: u64) {
        self.post_balances.push(lamports);
    }

    #[inline]
    fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'a>) {
        if self
            .inner_instructions
            .last()
            .is_none_or(|group| group.index != instruction.outer_instruction_index)
        {
            self.inner_instructions.push(CompactInnerInstructions {
                index: instruction.outer_instruction_index,
                instructions: Vec::new(),
            });
        }

        let Some(group) = self.inner_instructions.last_mut() else {
            return;
        };
        group.instructions.push(CompactInnerInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.to_vec(),
            data: instruction.data.to_vec(),
            stack_height: instruction.stack_height,
        });
    }

    #[inline]
    fn inner_instructions_none(&mut self, none: bool) {
        self.inner_instructions_none = none;
    }

    #[inline]
    fn log_message(&mut self, message: &'a str) {
        self.log_messages.push(message.to_owned());
    }

    #[inline]
    fn log_messages_none(&mut self, none: bool) {
        self.log_messages_none = none;
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        match no_registry_token_balance_from_visit(balance) {
            Ok(balance) => self.pre_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        match no_registry_token_balance_from_visit(balance) {
            Ok(balance) => self.post_token_balances.push(balance),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn reward_raw(&mut self, bytes: &'a [u8]) {
        match of_car_reader::confirmed_block::Reward::decode(bytes)
            .map_err(anyhow::Error::from)
            .and_then(|reward| no_registry_reward_from_proto(&reward))
        {
            Ok(reward) => self.rewards.push(reward),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'a [u8]) {
        match bytes_to_pubkey(address, "loaded writable address") {
            Ok(address) => self.loaded_writable_addresses.push(address),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'a [u8]) {
        match bytes_to_pubkey(address, "loaded readonly address") {
            Ok(address) => self.loaded_readonly_addresses.push(address),
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn return_data(&mut self, return_data: ReturnDataVisit<'a>) {
        match bytes_to_pubkey(return_data.program_id, "return data program id") {
            Ok(program_id) => {
                self.return_data = Some(WincodeArchiveV2NoRegistryReturnData {
                    program_id,
                    data: return_data.data.to_vec(),
                });
            }
            Err(err) => self.record_error(err),
        }
    }

    #[inline]
    fn return_data_none(&mut self, none: bool) {
        self.return_data_none = none;
    }

    #[inline]
    fn compute_units_consumed(&mut self, units: u64) {
        self.compute_units_consumed = Some(units);
    }

    #[inline]
    fn cost_units(&mut self, units: u64) {
        self.cost_units = Some(units);
    }
}

fn ordered_entries<'a>(
    pending: &'a PendingBlock,
    block: &RawBlockNode,
) -> Result<&'a [PendingEntry]> {
    anyhow::ensure!(
        pending.entries.len() == block.entries.len(),
        "slot {} entry count mismatch: stream={} block_refs={}",
        block.slot,
        pending.entries.len(),
        block.entries.len()
    );
    for (index, (entry_ref, pending_entry)) in
        block.entries.iter().zip(pending.entries.iter()).enumerate()
    {
        let cid = entry_ref
            .cid
            .ok_or_else(|| anyhow!("slot {} inline entry ref unsupported", block.slot))?;
        anyhow::ensure!(
            pending_entry.entry.cid == cid,
            "slot {} entry order mismatch at entry #{}",
            block.slot,
            index
        );
    }
    Ok(&pending.entries)
}

fn validate_ordered_transactions(
    pending: &PendingBlock,
    entries: &[PendingEntry],
    slot: u64,
) -> Result<()> {
    let mut tx_index = 0usize;
    for (entry_index, entry) in entries.iter().enumerate() {
        for tx_ref in &entry.entry.transactions {
            let cid = tx_ref
                .cid
                .ok_or_else(|| anyhow!("slot {slot} inline transaction ref unsupported"))?;
            let pending_tx = pending.transactions.get(tx_index).ok_or_else(|| {
                anyhow!(
                    "slot {slot} missing transaction at stream tx #{} referenced by entry #{}",
                    tx_index,
                    entry_index
                )
            })?;
            anyhow::ensure!(
                pending_tx.tx.cid == cid,
                "slot {slot} transaction order mismatch at tx #{} entry #{}",
                tx_index,
                entry_index
            );
            tx_index += 1;
        }
    }
    anyhow::ensure!(
        tx_index == pending.transactions.len(),
        "slot {slot} transaction count mismatch: stream={} entry_refs={}",
        pending.transactions.len(),
        tx_index
    );
    Ok(())
}

pub(crate) fn inspect_car_order(input: &Path, max_blocks: Option<u64>) -> Result<()> {
    let mut scanner = RawCarScanner::open(input)?;
    scanner.skip_header()?;

    let mut pending_txs = Vec::<Cid36>::new();
    let mut pending_entries = Vec::<PendingEntryOrder>::new();
    let mut pending_rewards = Vec::<Cid36>::new();
    let mut pending_dataframes = 0u64;
    let mut stats = CarOrderStats::default();
    let started = Instant::now();

    while let Some(raw) = scanner.next_node_timed(None)? {
        stats.car_entries += 1;
        stats.car_payload_bytes += raw.payload_len as u64;
        match raw.node {
            RawNode::Transaction(tx) => {
                stats.transactions += 1;
                stats.tx_data_continuation_refs += tx.data.next.len() as u64;
                stats.tx_metadata_continuation_refs += tx.metadata.next.len() as u64;
                pending_txs.push(tx.cid);
            }
            RawNode::Entry(entry) => {
                stats.entries += 1;
                let mut tx_cids = Vec::with_capacity(entry.transactions.len());
                for tx_ref in &entry.transactions {
                    let cid = tx_ref
                        .cid
                        .ok_or_else(|| anyhow!("inline transaction ref unsupported"))?;
                    tx_cids.push(cid);
                }
                pending_entries.push(PendingEntryOrder {
                    cid: entry.cid,
                    tx_cids,
                });
            }
            RawNode::Rewards(rewards) => {
                stats.rewards += 1;
                stats.rewards_continuation_refs += rewards.data.next.len() as u64;
                pending_rewards.push(rewards.cid);
            }
            RawNode::DataFrame(frame) => {
                stats.dataframes += 1;
                stats.dataframe_continuation_refs += frame.frame.next.len() as u64;
                pending_dataframes += 1;
            }
            RawNode::Block(block) => {
                stats.blocks += 1;
                stats.max_pending_txs = stats.max_pending_txs.max(pending_txs.len() as u64);
                stats.max_pending_entries =
                    stats.max_pending_entries.max(pending_entries.len() as u64);
                stats.max_pending_rewards =
                    stats.max_pending_rewards.max(pending_rewards.len() as u64);
                stats.max_pending_dataframes = stats.max_pending_dataframes.max(pending_dataframes);

                let mut block_entry_cids = Vec::with_capacity(block.entries.len());
                for entry_ref in &block.entries {
                    let cid = entry_ref.cid.ok_or_else(|| {
                        anyhow!("slot {} inline entry ref unsupported", block.slot)
                    })?;
                    block_entry_cids.push(cid);
                }

                if block_entry_cids
                    .iter()
                    .copied()
                    .eq(pending_entries.iter().map(|entry| entry.cid))
                {
                    stats.entry_order_matches += 1;
                } else {
                    stats.entry_order_mismatches += 1;
                    if stats.first_entry_order_mismatch_slot.is_none() {
                        stats.first_entry_order_mismatch_slot = Some(block.slot);
                    }
                }

                let mut entry_by_cid = GxHashMap::with_capacity_and_hasher(
                    pending_entries.len(),
                    GxBuildHasher::default(),
                );
                for entry in &pending_entries {
                    entry_by_cid.insert(entry.cid, entry);
                }
                let expected_tx_count = pending_entries
                    .iter()
                    .map(|entry| entry.tx_cids.len())
                    .sum::<usize>();
                let mut block_order_txs = Vec::with_capacity(expected_tx_count);
                let mut missing_entry = false;
                for entry_cid in &block_entry_cids {
                    let Some(entry) = entry_by_cid.get(entry_cid) else {
                        missing_entry = true;
                        continue;
                    };
                    block_order_txs.extend(entry.tx_cids.iter().copied());
                }
                if !missing_entry && block_order_txs == pending_txs {
                    stats.tx_order_matches += 1;
                } else {
                    stats.tx_order_mismatches += 1;
                    if stats.first_tx_order_mismatch_slot.is_none() {
                        stats.first_tx_order_mismatch_slot = Some(block.slot);
                    }
                }

                match block.rewards.as_ref().and_then(|rewards| rewards.cid) {
                    Some(rewards_cid) if pending_rewards.as_slice() == [rewards_cid] => {
                        stats.rewards_order_matches += 1;
                    }
                    Some(_) => {
                        stats.rewards_order_mismatches += 1;
                        if stats.first_rewards_order_mismatch_slot.is_none() {
                            stats.first_rewards_order_mismatch_slot = Some(block.slot);
                        }
                    }
                    None if pending_rewards.is_empty() => {
                        stats.rewards_order_matches += 1;
                    }
                    None => {
                        stats.rewards_order_mismatches += 1;
                        if stats.first_rewards_order_mismatch_slot.is_none() {
                            stats.first_rewards_order_mismatch_slot = Some(block.slot);
                        }
                    }
                }

                pending_txs.clear();
                pending_entries.clear();
                pending_rewards.clear();
                pending_dataframes = 0;

                if max_blocks.is_some_and(|limit| stats.blocks >= limit) {
                    break;
                }
            }
            RawNode::Subset(_) => stats.subsets += 1,
            RawNode::Epoch(_) => stats.epochs += 1,
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    info!(
        "CAR order inspection complete in {:.2}s: blocks={} car_entries={} payload_bytes={} txs={} entries={} rewards={} dataframes={} subsets={} epochs={}",
        elapsed,
        stats.blocks,
        stats.car_entries,
        stats.car_payload_bytes,
        stats.transactions,
        stats.entries,
        stats.rewards,
        stats.dataframes,
        stats.subsets,
        stats.epochs,
    );
    info!(
        "CAR order matches: entries={}/{} mismatches={} first_entry_mismatch_slot={:?} txs={}/{} mismatches={} first_tx_mismatch_slot={:?} rewards={}/{} mismatches={} first_rewards_mismatch_slot={:?}",
        stats.entry_order_matches,
        stats.blocks,
        stats.entry_order_mismatches,
        stats.first_entry_order_mismatch_slot,
        stats.tx_order_matches,
        stats.blocks,
        stats.tx_order_mismatches,
        stats.first_tx_order_mismatch_slot,
        stats.rewards_order_matches,
        stats.blocks,
        stats.rewards_order_mismatches,
        stats.first_rewards_order_mismatch_slot,
    );
    info!(
        "CAR order pending maxima: txs={} entries={} rewards={} dataframes={} continuation_refs tx_data={} tx_metadata={} rewards={} dataframe={}",
        stats.max_pending_txs,
        stats.max_pending_entries,
        stats.max_pending_rewards,
        stats.max_pending_dataframes,
        stats.tx_data_continuation_refs,
        stats.tx_metadata_continuation_refs,
        stats.rewards_continuation_refs,
        stats.dataframe_continuation_refs,
    );

    Ok(())
}

struct PendingEntryOrder {
    cid: Cid36,
    tx_cids: Vec<Cid36>,
}

#[derive(Default)]
struct CarOrderStats {
    car_entries: u64,
    car_payload_bytes: u64,
    blocks: u64,
    entries: u64,
    transactions: u64,
    rewards: u64,
    dataframes: u64,
    subsets: u64,
    epochs: u64,
    entry_order_matches: u64,
    entry_order_mismatches: u64,
    tx_order_matches: u64,
    tx_order_mismatches: u64,
    rewards_order_matches: u64,
    rewards_order_mismatches: u64,
    first_entry_order_mismatch_slot: Option<u64>,
    first_tx_order_mismatch_slot: Option<u64>,
    first_rewards_order_mismatch_slot: Option<u64>,
    max_pending_txs: u64,
    max_pending_entries: u64,
    max_pending_rewards: u64,
    max_pending_dataframes: u64,
    tx_data_continuation_refs: u64,
    tx_metadata_continuation_refs: u64,
    rewards_continuation_refs: u64,
    dataframe_continuation_refs: u64,
}

#[derive(Default)]
struct PendingBlock {
    transactions: Vec<PendingTx>,
    entries: Vec<PendingEntry>,
    rewards: Option<PendingRewards>,
    dataframes: HashMap<Cid36, StandaloneDataFrame>,
    last_slot: u64,
}

impl PendingBlock {
    fn clear(&mut self) {
        self.transactions.clear();
        self.entries.clear();
        self.rewards = None;
        self.dataframes.clear();
    }
}

struct PendingTx {
    tx: RawTransactionNode,
    #[allow(dead_code)]
    payload_len: usize,
}

struct PendingEntry {
    entry: RawEntryNode,
    #[allow(dead_code)]
    payload_len: usize,
}

struct PendingRewards {
    rewards: RawRewardsNode,
    #[allow(dead_code)]
    payload_len: usize,
}

struct RawNodeWithLen {
    node: RawNode,
    payload_len: usize,
}

struct RawPayload<'a> {
    cid: Cid36,
    payload: &'a [u8],
    payload_len: usize,
}

struct RawCarScanner<R: Read> {
    reader: CarBlockReader<R>,
    scratch: Vec<u8>,
}

impl RawCarScanner<Box<dyn Read>> {
    fn open(path: &Path) -> Result<Self> {
        let compressed = path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"));
        Self::open_with_compression(path, compressed)
    }

    fn open_with_compression(path: &Path, compressed: bool) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let input: Box<dyn Read> = if compressed {
            Box::new(
                zstd::stream::read::Decoder::new(reader)
                    .with_context(|| format!("zstd decode {}", path.display()))?,
            )
        } else {
            Box::new(reader)
        };
        Ok(RawCarScanner {
            reader: CarBlockReader::with_capacity(input, BUFFER_SIZE),
            scratch: Vec::new(),
        })
    }
}

impl<R: Read> RawCarScanner<R> {
    fn skip_header(&mut self) -> Result<()> {
        self.reader.skip_header().context("skip CAR header")
    }

    fn next_payload(&mut self) -> Result<Option<RawPayload<'_>>> {
        self.reader
            .read_entry_payload_with_scratch(&mut self.scratch)
            .map(|entry| {
                entry.map(|entry| RawPayload {
                    cid: entry.cid,
                    payload: entry.payload,
                    payload_len: entry.payload_len,
                })
            })
            .map_err(|err| anyhow!("{err}"))
    }

    fn next_node_timed(
        &mut self,
        timings: Option<&mut ArchiveV2Timings>,
    ) -> Result<Option<RawNodeWithLen>> {
        let started = Instant::now();
        let Some(entry) = self
            .reader
            .read_entry_payload_with_scratch(&mut self.scratch)
            .map_err(|err| anyhow!("{err}"))?
        else {
            return Ok(None);
        };
        let node =
            decode_raw_node(entry.location, entry.cid, entry.payload).with_context(|| {
                format!(
                    "decode node at entry {} offset {}",
                    entry.location.entry_index, entry.location.car_offset
                )
            })?;
        if let Some(timings) = timings {
            timings.scan_decode_node += started.elapsed();
        }
        Ok(Some(RawNodeWithLen {
            node,
            payload_len: entry.payload_len,
        }))
    }
}

#[derive(Default)]
struct ArchiveV2Timings {
    scan_decode_node: Duration,
    classify: Duration,
    dataframe_assemble: Duration,
    tx_decode_compact: Duration,
    metadata_decode_compact: Duration,
    rewards_decode_compact: Duration,
    wincode_encode: Duration,
    hot_message_build: Duration,
    hot_message_encode: Duration,
    hot_metadata_encode: Duration,
    hot_signature_write: Duration,
    hot_block_serialize: Duration,
    hot_zstd_compress: Duration,
    hot_block_write: Duration,
    hot_poh_write: Duration,
    tx_reassembled: u64,
    metadata_reassembled: u64,
    metadata_protobuf_visit: u64,
    metadata_owned_fallback: u64,
    tx_scratch_max: usize,
    metadata_scratch_max: usize,
    hot_zstd_buffer_reserves: u64,
    hot_zstd_buffer_capacity_max: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_v2_record_tags_match_index_fast_scanner() {
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_VERSION,
                flags: ARCHIVE_FLAGS_LEB128,
            })),
            WINCODE_ARCHIVE_V2_RECORD_HEADER_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Block(empty_archive_v2_block(0))),
            WINCODE_ARCHIVE_V2_RECORD_BLOCK_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                slot: 0,
                block_id: 0,
                block_offset: 0,
                block_len: 0,
                runtime_offset: 0,
                runtime_len: 0,
                tx_count: 0,
            })),
            WINCODE_ARCHIVE_V2_RECORD_INDEX_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Footer(
                WincodeArchiveV2Footer::default()
            )),
            WINCODE_ARCHIVE_V2_RECORD_FOOTER_TAG
        );
        assert_eq!(
            archive_v2_record_tag(WincodeArchiveV2Record::Genesis(WincodeArchiveV2Genesis {
                genesis_hash: [0; 32],
                genesis_bin_len: 0,
                creation_time_unix: 0,
                cluster_id: 0,
                ticks_per_slot: 0,
                poh_params: WincodeArchiveV2GenesisPohParams {
                    tick_duration_secs: 0,
                    tick_duration_nanos: 0,
                    tick_count: None,
                    hashes_per_tick: None,
                },
                fees: WincodeArchiveV2GenesisFeeParams {
                    target_lamports_per_sig: 0,
                    target_sigs_per_slot: 0,
                    min_lamports_per_sig: 0,
                    max_lamports_per_sig: 0,
                    burn_percent: 0,
                },
                rent: WincodeArchiveV2GenesisRentParams {
                    lamports_per_byte_year: 0,
                    exemption_threshold: 0.0,
                    burn_percent: 0,
                },
                inflation: WincodeArchiveV2GenesisInflationParams {
                    initial: 0.0,
                    terminal: 0.0,
                    taper: 0.0,
                    foundation: 0.0,
                    foundation_term: 0.0,
                    padding: [0; 8],
                },
                epoch_schedule: WincodeArchiveV2GenesisEpochSchedule {
                    slots_per_epoch: 0,
                    leader_schedule_slot_offset: 0,
                    warmup: false,
                    first_normal_epoch: 0,
                    first_normal_slot: 0,
                },
                accounts: Vec::new(),
                builtins: Vec::new(),
                reward_pools: Vec::new(),
            })),
            WINCODE_ARCHIVE_V2_RECORD_GENESIS_TAG
        );
    }

    fn archive_v2_record_tag(record: WincodeArchiveV2Record) -> u8 {
        wincode::config::serialize(&record, wincode_leb128_config()).unwrap()[0]
    }

    #[test]
    fn build_index_records_physical_block_offsets() {
        let dir = std::env::temp_dir().join(format!(
            "blockzilla-archive-v2-index-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let archive_path = dir.join("archive-v2.wincode");
        let index_path = dir.join("archive-v2.index");

        let block_record = WincodeArchiveV2Record::Block(empty_archive_v2_block(123));
        let block_bytes =
            wincode::config::serialize(&block_record, wincode_leb128_config()).unwrap();
        {
            let file = File::create(&archive_path).unwrap();
            let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(file));
            writer
                .write(&WincodeArchiveV2Record::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_VERSION,
                    flags: ARCHIVE_FLAGS_LEB128,
                }))
                .unwrap();
            writer.write_bytes(&block_bytes).unwrap();
            writer
                .write(&WincodeArchiveV2Record::Index(SplitCompactIndexRecord {
                    slot: 123,
                    block_id: 0,
                    block_offset: 0,
                    block_len: u32::try_from(block_bytes.len()).unwrap(),
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count: 0,
                }))
                .unwrap();
            writer
                .write(&WincodeArchiveV2Record::Footer(WincodeArchiveV2Footer {
                    blocks: 1,
                    ..WincodeArchiveV2Footer::default()
                }))
                .unwrap();
            writer.flush().unwrap();
        }

        build_index(&archive_path, Some(&index_path)).unwrap();
        let index = read_archive_v2_block_index(&index_path).unwrap();
        assert_eq!(index.rows.len(), 1);
        let row = index.rows[0];
        assert_eq!(row.block_id, 0);
        assert_eq!(row.slot, 123);
        assert_eq!(row.payload_len as usize, block_bytes.len());
        assert_eq!(row.tx_count, 0);
        assert!(row.frame_offset < row.payload_offset);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn empty_archive_v2_block(slot: u64) -> WincodeArchiveV2Block {
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader {
                compact: CompactBlockHeader {
                    slot,
                    parent_slot: 0,
                    blockhash: 0,
                    previous_blockhash: 0,
                    block_time: None,
                    block_height: None,
                    shredding: Vec::new(),
                    poh_entries: Vec::new(),
                    rewards: None,
                },
                rewards: None,
            },
            txs: Vec::new(),
        }
    }

    #[test]
    fn rolling_blockhash_resolves_block_id_and_falls_back_to_nonce() {
        let mut rolling = RollingBlockhashIndex::new(300);
        let hash = [7u8; 32];
        rolling.insert(hash, 42, 100).unwrap();

        match rolling.resolve_or_nonce(&hash, 101, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 42),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }

        let missing = [8u8; 32];
        match rolling.resolve_or_nonce(&missing, 101, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, missing),
        }
    }

    #[test]
    fn rolling_blockhash_evicts_old_hashes() {
        let mut rolling = RollingBlockhashIndex::new(2);
        let first = [1u8; 32];
        let second = [2u8; 32];
        let third = [3u8; 32];

        rolling.insert(first, 1, 1).unwrap();
        rolling.insert(second, 2, 2).unwrap();
        rolling.insert(third, 3, 3).unwrap();

        match rolling.resolve_or_nonce(&first, 3, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, first),
        }
        match rolling.resolve_or_nonce(&second, 3, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 2),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
        match rolling.resolve_or_nonce(&third, 3, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 3),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
    }

    #[test]
    fn rolling_blockhash_uses_nonce_for_hash_outside_slot_window() {
        let mut rolling = RollingBlockhashIndex::new(300);
        let hash = [7u8; 32];
        rolling.insert(hash, 42, 10).unwrap();

        match rolling.resolve_or_nonce(&hash, 160, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 42),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
        match rolling.resolve_or_nonce(&hash, 161, 0).unwrap() {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, hash),
        }
    }

    #[test]
    fn rolling_blockhash_rejects_future_hash() {
        let mut rolling = RollingBlockhashIndex::new(300);
        let hash = [7u8; 32];
        rolling.insert(hash, 42, 20).unwrap();

        let err = rolling.resolve_or_nonce(&hash, 19, 0).unwrap_err();
        assert!(err.to_string().contains("future block slot"));
    }

    #[test]
    fn optimize_no_registry_tx_uses_required_recent_block_id() {
        let account = [9u8; 32];
        let recent_hash = [4u8; 32];
        let key_index = KeyIndex::build(vec![account]);
        let mut rolling = RollingBlockhashIndex::new(300);
        rolling.insert(recent_hash, 7, 9).unwrap();

        let tx = WincodeArchiveV2NoRegistryTx {
            signatures: vec![vec![1u8; 64]],
            message: WincodeArchiveV2NoRegistryMessage::Legacy(
                WincodeArchiveV2NoRegistryLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![account],
                    recent_blockhash: recent_hash,
                    instructions: Vec::new(),
                },
            ),
        };

        let mut nonce_recent_blockhashes = 0;
        let optimized = optimize_no_registry_tx(
            10,
            0,
            tx,
            &key_index,
            &rolling,
            &mut nonce_recent_blockhashes,
        )
        .unwrap();
        let OwnedCompactMessage::Legacy(message) = optimized.message else {
            panic!("expected legacy message");
        };
        assert_eq!(message.account_keys, vec![CompactPubkey::id(1)]);
        assert_eq!(nonce_recent_blockhashes, 0);
        match message.recent_blockhash {
            OwnedCompactRecentBlockhash::Id(id) => assert_eq!(id, 7),
            OwnedCompactRecentBlockhash::Nonce(_) => panic!("unexpected nonce fallback"),
        }
    }

    #[test]
    fn optimize_no_registry_tx_uses_nonce_for_missing_recent_blockhash() {
        let account = [9u8; 32];
        let recent_hash = [4u8; 32];
        let key_index = KeyIndex::build(vec![account]);
        let rolling = RollingBlockhashIndex::new(300);

        let tx = WincodeArchiveV2NoRegistryTx {
            signatures: vec![vec![1u8; 64]],
            message: WincodeArchiveV2NoRegistryMessage::Legacy(
                WincodeArchiveV2NoRegistryLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![account],
                    recent_blockhash: recent_hash,
                    instructions: Vec::new(),
                },
            ),
        };

        let mut nonce_recent_blockhashes = 0;
        let optimized = optimize_no_registry_tx(
            10,
            0,
            tx,
            &key_index,
            &rolling,
            &mut nonce_recent_blockhashes,
        )
        .unwrap();
        let OwnedCompactMessage::Legacy(message) = optimized.message else {
            panic!("expected legacy message");
        };
        assert_eq!(nonce_recent_blockhashes, 1);
        match message.recent_blockhash {
            OwnedCompactRecentBlockhash::Id(_) => panic!("unexpected blockhash id"),
            OwnedCompactRecentBlockhash::Nonce(nonce) => assert_eq!(nonce, recent_hash),
        }
    }

    #[test]
    fn hot_vote_parser_compacts_tower_sync_hashes_by_voted_block() {
        let root = 1_000u64;
        let last_slot = root + 31;
        let mut data = Vec::new();
        data.extend_from_slice(&14u32.to_le_bytes());
        data.extend_from_slice(&root.to_le_bytes());
        data.push(31);
        for confirmation_count in (1u8..=31).rev() {
            data.push(1);
            data.push(confirmation_count);
        }
        data.extend_from_slice(&[7u8; 32]);
        data.push(1);
        data.extend_from_slice(&1_234_567i64.to_le_bytes());
        data.extend_from_slice(&[8u8; 32]);
        assert_eq!(data.len(), 148);

        let mut slots = GxHashMap::with_hasher(GxBuildHasher::default());
        slots.insert(last_slot, 42);
        let mut vote_hashes = VoteHashRegistryBuilder::default();
        let parsed = parse_hot_vote_instruction_data(&data, &slots, &mut vote_hashes).unwrap();
        let ArchiveV2HotInstructionData::VoteTowerSync(tower) = parsed else {
            panic!("expected TowerSync");
        };
        assert_eq!(tower.update.root, Some(root));
        assert_eq!(tower.update.lockout_offsets.len(), 31);
        assert_eq!(tower.update.timestamp, Some(1_234_567));
        assert_eq!(tower.update.hash, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(tower.block_id_hash, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(vote_hashes.rows[42].bank_hash, Some([7u8; 32]));
        assert_eq!(vote_hashes.rows[42].block_id_hash, Some([8u8; 32]));
    }

    #[test]
    fn hot_compute_budget_parser_compacts_observed_wire_forms() {
        let parsed =
            parse_hot_compute_budget_instruction_data(&[3, 0x10, 0x27, 0, 0, 0, 0, 0, 0]).unwrap();
        let ArchiveV2HotInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(price),
        ) = parsed
        else {
            panic!("expected compact compute budget price");
        };
        assert_eq!(price, 10_000);

        let parsed = parse_hot_compute_budget_instruction_data(&[2, 0xf1, 0x01, 0, 0]).unwrap();
        let ArchiveV2HotInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(limit),
        ) = parsed
        else {
            panic!("expected compact compute unit limit");
        };
        assert_eq!(limit, 497);
    }

    #[test]
    fn hot_system_parser_compacts_common_bincode_forms() {
        let mut transfer = Vec::new();
        transfer.extend_from_slice(&2u32.to_le_bytes());
        transfer.extend_from_slice(&1u64.to_le_bytes());
        let parsed = parse_hot_system_instruction_data(&transfer).unwrap();
        let ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
            lamports,
        }) = parsed
        else {
            panic!("expected compact system transfer");
        };
        assert_eq!(lamports, 1);

        let parsed = parse_hot_system_instruction_data(&4u32.to_le_bytes()).unwrap();
        assert!(matches!(
            parsed,
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AdvanceNonceAccount
            )
        ));
    }

    #[test]
    fn hot_system_parser_compacts_seed_forms_without_extra_allocation_guessing() {
        let mut create = Vec::new();
        create.extend_from_slice(&3u32.to_le_bytes());
        create.extend_from_slice(&[1u8; 32]);
        create.extend_from_slice(&3u64.to_le_bytes());
        create.extend_from_slice(b"abc");
        create.extend_from_slice(&5u64.to_le_bytes());
        create.extend_from_slice(&9u64.to_le_bytes());
        create.extend_from_slice(&[2u8; 32]);

        let parsed = parse_hot_system_instruction_data(&create).unwrap();
        let ArchiveV2HotInstructionData::System(
            ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                base,
                seed,
                lamports,
                space,
                owner,
            },
        ) = parsed
        else {
            panic!("expected compact create account with seed");
        };
        assert_eq!(base, [1u8; 32]);
        assert_eq!(seed, "abc");
        assert_eq!(lamports, 5);
        assert_eq!(space, 9);
        assert_eq!(owner, [2u8; 32]);
    }

    #[test]
    fn hot_known_program_parsers_keep_invalid_instruction_payloads_raw() {
        let parsed = parse_hot_compute_budget_instruction_data(&[2, 1]).unwrap();
        assert!(matches!(parsed, ArchiveV2HotInstructionData::Raw(raw) if raw == vec![2, 1]));

        let parsed = parse_hot_system_instruction_data(&[2, 0, 0]).unwrap();
        assert!(matches!(parsed, ArchiveV2HotInstructionData::Raw(raw) if raw == vec![2, 0, 0]));
    }

    #[test]
    fn vote_hash_registry_keeps_conflicting_tower_block_id_hash_raw() {
        let mut slots = GxHashMap::with_hasher(GxBuildHasher::default());
        slots.insert(1_000, 42);
        let mut vote_hashes = VoteHashRegistryBuilder::default();

        let first = vote_hashes
            .ref_block_id_hash(Some(1_000), [8u8; 32], &slots)
            .unwrap();
        let second = vote_hashes
            .ref_block_id_hash(Some(1_000), [9u8; 32], &slots)
            .unwrap();

        assert_eq!(first, ArchiveV2VoteHashRef::Block(42));
        assert_eq!(second, ArchiveV2VoteHashRef::Raw([9u8; 32]));
        assert_eq!(vote_hashes.rows[42].block_id_hash, Some([8u8; 32]));
        assert_eq!(vote_hashes.block_id_refs, 1);
        assert_eq!(vote_hashes.block_id_raw, 1);
        assert_eq!(vote_hashes.block_id_conflict_raw, 1);
    }
}
