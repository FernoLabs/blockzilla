use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash,
    ArchiveV2BlockAccessIndexRow, ArchiveV2BlockAccessPubkey, ArchiveV2BlockAccessVoteHash,
    ArchiveV2GetBlockIndexRow, ArchiveV2HotBlockBlob, ArchiveV2HotInstructionData,
    ArchiveV2HotMessagePayload, ArchiveV2HotTxRow, ArchiveV2VoteHashRef, CompactInnerInstructions,
    CompactMetaV1, CompactPubkey, CompactReturnData, CompactReward, CompactTokenBalance,
    CompactTransactionError, KeyStore, LogEvent, OwnedCompactRecentBlockhash,
    WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION, archive_v2_hot_index_path,
    deserialize_archive_v2_hot_block_blob,
    program_logs::{ProgramLog, token_2022::Token2022Log},
    read_archive_v2_hot_block_index, wincode_leb128_config, write_archive_v2_block_access_index,
    write_archive_v2_get_block_index,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use wincode::{SchemaRead, SchemaWrite};

const SLOTS_PER_EPOCH: u64 = 432_000;

#[derive(Debug, Parser)]
struct Args {
    /// Epoch directory containing archive-v2-blocks.zstd, v1 access, and vote_hash_registry.bin.
    epoch_dir: PathBuf,
    /// Output directory for upgraded v2 access and get-block index.
    output_dir: PathBuf,
    /// Hot-block index path. Defaults to archive-v2-blocks.index in the epoch dir.
    #[arg(long)]
    hot_index: Option<PathBuf>,
    /// Existing v1 block-access index path.
    #[arg(long)]
    access_index: Option<PathBuf>,
    /// Existing v1 block-access blob path.
    #[arg(long)]
    access_file: Option<PathBuf>,
    /// Optional block limit for quick testing.
    #[arg(long)]
    max_blocks: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyBlockAccessBlobV1 {
    version: u16,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyBlockAccessBlobV2NoVotes {
    version: u16,
    flags: u32,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

impl From<LegacyBlockAccessBlobV1> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV1) -> Self {
        Self {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: 0,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

impl From<LegacyBlockAccessBlobV2NoVotes> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV2NoVotes) -> Self {
        Self {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: value.flags,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyCompactMetaV1 {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    logs: Option<blockzilla_format::CompactLogStream>,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<CompactReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

impl TryFrom<LegacyCompactMetaV1> for CompactMetaV1 {
    type Error = anyhow::Error;

    fn try_from(value: LegacyCompactMetaV1) -> Result<Self> {
        let err = value
            .err
            .as_deref()
            .map(CompactTransactionError::from_stored_wincode_bytes)
            .transpose()?;
        Ok(Self {
            err,
            fee: value.fee,
            pre_balances: value.pre_balances,
            post_balances: value.post_balances,
            inner_instructions: value.inner_instructions,
            logs: value.logs,
            pre_token_balances: value.pre_token_balances,
            post_token_balances: value.post_token_balances,
            rewards: value.rewards,
            loaded_writable_addresses: value.loaded_writable_addresses,
            loaded_readonly_addresses: value.loaded_readonly_addresses,
            return_data: value.return_data,
            compute_units_consumed: value.compute_units_consumed,
            cost_units: value.cost_units,
        })
    }
}

#[derive(Clone, Copy)]
struct VoteHashRegistryRow {
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let epoch_dir = args.epoch_dir;
    let output_dir = args.output_dir;
    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("create {}", output_dir.display()))?;

    let blocks_path = epoch_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let hot_index_path = args
        .hot_index
        .unwrap_or_else(|| archive_v2_hot_index_path(&blocks_path));
    let access_index_path = args
        .access_index
        .unwrap_or_else(|| epoch_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE));
    let access_file_path = args
        .access_file
        .unwrap_or_else(|| epoch_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE));
    let vote_hash_registry_path = epoch_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);
    let pubkey_registry_path = epoch_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let blockhash_registry_path = epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    let previous_tail_path = epoch_dir.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    let signatures_path = epoch_dir.join(ARCHIVE_V2_SIGNATURES_FILE);

    let hot_index = read_archive_v2_hot_block_index(&hot_index_path)?;
    let vote_hash_rows = load_vote_hash_registry(&vote_hash_registry_path)?;
    let pubkey_store = KeyStore::load(&pubkey_registry_path)
        .with_context(|| format!("load {}", pubkey_registry_path.display()))?;
    let blockhashes = load_blockhash_registry(&blockhash_registry_path)?;
    let previous_tail = load_previous_blockhash_tail(&previous_tail_path)?;
    let _ = (access_index_path, access_file_path);

    let raw_blocks = hot_index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
    let mut block_file =
        File::open(&blocks_path).with_context(|| format!("open {}", blocks_path.display()))?;
    let mut signatures_file = File::open(&signatures_path)
        .with_context(|| format!("open {}", signatures_path.display()))?;
    let output_access_path = output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let mut output_access = BufWriter::with_capacity(
        8 << 20,
        File::create(&output_access_path)
            .with_context(|| format!("create {}", output_access_path.display()))?,
    );
    let mut decompressor = if raw_blocks {
        None
    } else {
        Some(zstd::bulk::Decompressor::new().context("create zstd decompressor")?)
    };
    let mut block_input = Vec::new();
    let mut block_bytes = Vec::new();
    let mut signature_bytes = Vec::new();
    let mut new_access_bytes = Vec::new();
    let mut access_rows = Vec::new();
    let mut current_block_offset = 0u64;
    let mut access_offset = 0u64;
    let mut embedded_vote_rows = 0u64;
    let started = Instant::now();
    let limit = args.max_blocks.unwrap_or(usize::MAX);

    for (row_index, hot_row) in hot_index.rows.iter().take(limit).enumerate() {
        read_hot_block(
            &mut block_file,
            &mut current_block_offset,
            &mut block_input,
            &mut block_bytes,
            decompressor.as_mut(),
            raw_blocks,
            hot_row.compressed_offset,
            hot_row.compressed_len,
            hot_row.uncompressed_len,
            hot_row.block_id,
        )?;
        let block = deserialize_archive_v2_hot_block_blob(&block_bytes)
            .with_context(|| format!("decode hot block_id {}", hot_row.block_id))?;

        read_block_signatures(&mut signatures_file, hot_row, &mut signature_bytes)?;
        let new_access = build_access_for_block(
            &block,
            &pubkey_store,
            &blockhashes,
            &previous_tail,
            &signature_bytes,
            &vote_hash_rows,
        )
        .with_context(|| format!("build access block_id {}", hot_row.block_id))?;
        embedded_vote_rows += new_access.vote_hashes.len() as u64;

        new_access_bytes.clear();
        wincode::config::serialize_into(
            &mut new_access_bytes,
            &new_access,
            wincode_leb128_config(),
        )?;
        anyhow::ensure!(
            u64::try_from(new_access_bytes.len()).context("new access payload size exceeds u64")?
                <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
            "new access payload for block_id {} is {} bytes, exceeding the shared {} byte limit",
            hot_row.block_id,
            new_access_bytes.len(),
            ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES
        );
        let access_len =
            u32::try_from(new_access_bytes.len()).context("new access payload exceeds u32::MAX")?;
        output_access
            .write_all(&new_access_bytes)
            .with_context(|| format!("write {}", output_access_path.display()))?;
        access_rows.push(ArchiveV2BlockAccessIndexRow {
            block_id: hot_row.block_id,
            slot: hot_row.slot,
            access_offset,
            access_len,
            tx_count: hot_row.tx_count,
            signature_count: hot_row.signature_count,
        });
        access_offset += access_len as u64;

        if row_index % 10_000 == 0 && row_index > 0 {
            eprintln!(
                "upgraded rows={} slot={} output_gib={:.2} vote_rows={} elapsed_s={:.1}",
                row_index,
                hot_row.slot,
                access_offset as f64 / (1024.0 * 1024.0 * 1024.0),
                embedded_vote_rows,
                started.elapsed().as_secs_f64()
            );
        }
    }
    output_access
        .flush()
        .with_context(|| format!("flush {}", output_access_path.display()))?;

    write_archive_v2_block_access_index(
        &output_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
        access_offset,
        0,
        &access_rows,
    )?;
    let epoch = infer_epoch(&hot_index.rows)?;
    let get_block_rows = build_get_block_index_rows(&hot_index.rows, &access_rows, epoch)?;
    write_archive_v2_get_block_index(
        &output_dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
        &get_block_rows,
    )?;

    eprintln!(
        "done rows={} bytes={} vote_rows={} elapsed_s={:.1}",
        access_rows.len(),
        access_offset,
        embedded_vote_rows,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn read_hot_block(
    file: &mut File,
    current_offset: &mut u64,
    input_buf: &mut Vec<u8>,
    block_bytes: &mut Vec<u8>,
    decompressor: Option<&mut zstd::bulk::Decompressor<'_>>,
    raw_blocks: bool,
    offset: u64,
    compressed_len: u32,
    uncompressed_len: u32,
    block_id: u32,
) -> Result<()> {
    if *current_offset != offset {
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seek hot archive block_id {block_id}"))?;
        *current_offset = offset;
    }
    if raw_blocks {
        block_bytes.resize(uncompressed_len as usize, 0);
        file.read_exact(block_bytes)
            .with_context(|| format!("read raw hot block_id {block_id}"))?;
        *current_offset += uncompressed_len as u64;
    } else {
        input_buf.resize(compressed_len as usize, 0);
        file.read_exact(input_buf)
            .with_context(|| format!("read compressed hot block_id {block_id}"))?;
        *current_offset += compressed_len as u64;
        *block_bytes = decompressor
            .expect("zstd decompressor")
            .decompress(input_buf, uncompressed_len as usize)
            .with_context(|| format!("decompress hot block_id {block_id}"))?;
    }
    Ok(())
}

fn read_block_signatures(
    file: &mut File,
    row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
    out: &mut Vec<u8>,
) -> Result<()> {
    let offset = row
        .first_signature_ordinal
        .checked_mul(64)
        .context("signature file offset overflow")?;
    let len = (row.signature_count as usize)
        .checked_mul(64)
        .context("signature byte length overflow")?;
    file.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek signatures block_id {}", row.block_id))?;
    out.resize(len, 0);
    file.read_exact(out)
        .with_context(|| format!("read signatures block_id {}", row.block_id))?;
    Ok(())
}

fn build_access_for_block(
    block: &ArchiveV2HotBlockBlob,
    store: &KeyStore,
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
    block_signature_bytes: &[u8],
    vote_hash_rows: &[VoteHashRegistryRow],
) -> Result<ArchiveV2BlockAccessBlob> {
    let expected_signature_bytes = block.tx_rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.signature_count as usize * 64)
            .context("block signature bytes overflow")
    })?;
    anyhow::ensure!(
        expected_signature_bytes == block_signature_bytes.len(),
        "block access signature length mismatch for slot {}: rows={} bytes={}",
        block.header.slot,
        expected_signature_bytes,
        block_signature_bytes.len()
    );

    let blockhash =
        resolve_access_blockhash_id(block.header.blockhash_id as i32, blockhashes, previous_tail)
            .with_context(|| {
            format!(
                "resolve blockhash id {} for slot {}",
                block.header.blockhash_id, block.header.slot
            )
        })?;
    let previous_blockhash = resolve_access_previous_blockhash(block, blockhashes, previous_tail)
        .with_context(|| {
        format!(
            "resolve previous blockhash id {} for slot {}",
            block.header.previous_blockhash_id, block.header.slot
        )
    })?;

    let mut pubkey_ids = HashSet::new();
    let mut blockhash_ids = HashSet::new();
    let mut vote_hash_ids = HashSet::new();
    collect_access_blockhash_id(block.header.blockhash_id as i32, &mut blockhash_ids);
    collect_access_blockhash_id(
        block.header.previous_blockhash_id as i32,
        &mut blockhash_ids,
    );
    if let Some(rewards) = &block.header.rewards {
        for reward in &rewards.decoded {
            collect_access_pubkey_id(reward.pubkey, &mut pubkey_ids);
        }
    }

    for row in &block.tx_rows {
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0 {
            let message_slice = &block.message_bytes
                [row.message_offset as usize..(row.message_offset + row.message_len) as usize];
            let message: ArchiveV2HotMessagePayload =
                wincode::config::deserialize(message_slice, wincode_leb128_config())
                    .with_context(|| format!("decode hot message tx_index={}", row.tx_index))?;
            collect_access_message_refs(&message, &mut pubkey_ids, &mut blockhash_ids);
            collect_access_message_vote_hash_refs(&message, &mut vote_hash_ids);
        }

        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
            || row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
            || row.metadata_len == 0
        {
            continue;
        }
        let metadata_slice = &block.metadata_bytes
            [row.metadata_offset as usize..(row.metadata_offset + row.metadata_len) as usize];
        let metadata = decode_compact_metadata(metadata_slice)
            .with_context(|| format!("decode hot metadata tx_index={}", row.tx_index))?;
        collect_access_metadata_refs(&metadata, &mut pubkey_ids);
    }

    let mut pubkey_ids = pubkey_ids.into_iter().collect::<Vec<_>>();
    pubkey_ids.sort_unstable();
    let pubkeys = pubkey_ids
        .into_iter()
        .map(|id| {
            let pubkey = *store
                .get(id)
                .with_context(|| format!("pubkey registry id {id} is outside loaded registry"))?;
            Ok(ArchiveV2BlockAccessPubkey { id, pubkey })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut blockhash_ids = blockhash_ids.into_iter().collect::<Vec<_>>();
    blockhash_ids.sort_unstable();
    let blockhashes = blockhash_ids
        .into_iter()
        .map(|id| {
            let blockhash = resolve_access_blockhash_id(id, blockhashes, previous_tail)
                .with_context(|| format!("blockhash id {id} is outside loaded registry"))?;
            Ok(ArchiveV2BlockAccessBlockhash { id, blockhash })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut vote_hash_ids = vote_hash_ids.into_iter().collect::<Vec<_>>();
    vote_hash_ids.sort_unstable();
    let vote_hashes = vote_hash_ids
        .into_iter()
        .map(|block_id| {
            let row = vote_hash_rows
                .get(block_id as usize)
                .copied()
                .with_context(|| {
                    format!("vote hash registry row {block_id} is outside loaded registry")
                })?;
            Ok(ArchiveV2BlockAccessVoteHash {
                block_id,
                bank_hash: row.bank_hash,
                block_id_hash: row.block_id_hash,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArchiveV2BlockAccessBlob {
        version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
        flags: 0,
        blockhash,
        previous_blockhash,
        signature_counts: block
            .tx_rows
            .iter()
            .map(|row| row.signature_count)
            .collect(),
        signatures: block_signature_bytes.to_vec(),
        pubkeys,
        blockhashes,
        vote_hashes,
    })
}

fn collect_vote_hashes_for_block(
    block: &ArchiveV2HotBlockBlob,
    vote_hash_rows: &[VoteHashRegistryRow],
) -> Result<Vec<ArchiveV2BlockAccessVoteHash>> {
    let mut ids = HashSet::new();
    for row in &block.tx_rows {
        collect_vote_hashes_for_tx(block, row, &mut ids)?;
    }
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    ids.into_iter()
        .map(|block_id| {
            let row = vote_hash_rows
                .get(block_id as usize)
                .copied()
                .with_context(|| format!("vote hash row {block_id} is outside registry"))?;
            Ok(ArchiveV2BlockAccessVoteHash {
                block_id,
                bank_hash: row.bank_hash,
                block_id_hash: row.block_id_hash,
            })
        })
        .collect()
}

fn collect_vote_hashes_for_tx(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    ids: &mut HashSet<u32>,
) -> Result<()> {
    let message_slice = &block.message_bytes
        [row.message_offset as usize..(row.message_offset + row.message_len) as usize];
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_slice, wincode_leb128_config())
            .with_context(|| format!("decode hot message tx_index={}", row.tx_index))?;
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            for instruction in message.instructions {
                collect_vote_hashes_from_instruction(&instruction.data, ids);
            }
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            for instruction in message.instructions {
                collect_vote_hashes_from_instruction(&instruction.data, ids);
            }
        }
    }
    Ok(())
}

fn collect_vote_hashes_from_instruction(
    data: &ArchiveV2HotInstructionData,
    ids: &mut HashSet<u32>,
) {
    match data {
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
            collect_vote_hash_ref(&update.hash, ids);
        }
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
            update,
            switch_proof_hash,
        } => {
            collect_vote_hash_ref(&update.hash, ids);
            collect_vote_hash_ref(switch_proof_hash, ids);
        }
        ArchiveV2HotInstructionData::VoteTowerSync(tower) => {
            collect_vote_hash_ref(&tower.update.hash, ids);
            collect_vote_hash_ref(&tower.block_id_hash, ids);
        }
        ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
            tower,
            switch_proof_hash,
        } => {
            collect_vote_hash_ref(&tower.update.hash, ids);
            collect_vote_hash_ref(&tower.block_id_hash, ids);
            collect_vote_hash_ref(switch_proof_hash, ids);
        }
        _ => {}
    }
}

fn collect_vote_hash_ref(value: &ArchiveV2VoteHashRef, ids: &mut HashSet<u32>) {
    if let ArchiveV2VoteHashRef::Block(block_id) = *value {
        ids.insert(block_id);
    }
}

fn collect_access_message_refs(
    message: &ArchiveV2HotMessagePayload,
    pubkey_ids: &mut HashSet<u32>,
    blockhash_ids: &mut HashSet<i32>,
) {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            collect_access_pubkeys(message.account_keys.iter().copied(), pubkey_ids);
            collect_access_recent_blockhash_id(&message.recent_blockhash, blockhash_ids);
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            collect_access_pubkeys(message.account_keys.iter().copied(), pubkey_ids);
            collect_access_recent_blockhash_id(&message.recent_blockhash, blockhash_ids);
            for lookup in &message.address_table_lookups {
                collect_access_pubkey_id(lookup.account_key, pubkey_ids);
            }
        }
    }
}

fn collect_access_message_vote_hash_refs(
    message: &ArchiveV2HotMessagePayload,
    ids: &mut HashSet<u32>,
) {
    let instructions = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.instructions.as_slice(),
    };
    for instruction in instructions {
        collect_vote_hashes_from_instruction(&instruction.data, ids);
    }
}

fn collect_access_metadata_refs(metadata: &CompactMetaV1, pubkey_ids: &mut HashSet<u32>) {
    collect_access_pubkeys(
        metadata
            .loaded_writable_addresses
            .iter()
            .chain(metadata.loaded_readonly_addresses.iter())
            .copied(),
        pubkey_ids,
    );
    for balance in metadata
        .pre_token_balances
        .iter()
        .chain(metadata.post_token_balances.iter())
    {
        if let Some(key) = balance.mint {
            collect_access_pubkey_id(key, pubkey_ids);
        }
        if let Some(key) = balance.owner {
            collect_access_pubkey_id(key, pubkey_ids);
        }
        if let Some(key) = balance.program_id {
            collect_access_pubkey_id(key, pubkey_ids);
        }
    }
    for reward in &metadata.rewards {
        collect_access_pubkey_id(reward.pubkey, pubkey_ids);
    }
    if let Some(return_data) = &metadata.return_data {
        collect_access_pubkey_id(return_data.program_id, pubkey_ids);
    }
    if let Some(logs) = &metadata.logs {
        collect_access_log_refs(logs, pubkey_ids);
    }
}

fn collect_access_pubkeys(keys: impl Iterator<Item = CompactPubkey>, ids: &mut HashSet<u32>) {
    for key in keys {
        collect_access_pubkey_id(key, ids);
    }
}

fn collect_access_recent_blockhash_id(value: &OwnedCompactRecentBlockhash, ids: &mut HashSet<i32>) {
    if let OwnedCompactRecentBlockhash::Id(id) = *value {
        collect_access_blockhash_id(id, ids);
    }
}

fn collect_access_blockhash_id(id: i32, ids: &mut HashSet<i32>) {
    ids.insert(id);
}

fn merge_access_log_pubkeys(
    old_pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    block: &ArchiveV2HotBlockBlob,
    store: &KeyStore,
) -> Result<Vec<ArchiveV2BlockAccessPubkey>> {
    let mut ids = old_pubkeys
        .iter()
        .map(|entry| entry.id)
        .collect::<HashSet<_>>();

    for row in &block.tx_rows {
        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
            || row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
            || row.metadata_len == 0
        {
            continue;
        }
        let metadata_slice = &block.metadata_bytes
            [row.metadata_offset as usize..(row.metadata_offset + row.metadata_len) as usize];
        let metadata = decode_compact_metadata(metadata_slice)
            .with_context(|| format!("decode hot metadata tx_index={}", row.tx_index))?;
        if let Some(logs) = &metadata.logs {
            collect_access_log_refs(logs, &mut ids);
        }
    }

    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    ids.into_iter()
        .map(|id| {
            let pubkey = *store
                .get(id)
                .with_context(|| format!("pubkey registry id {id} is outside loaded registry"))?;
            Ok(ArchiveV2BlockAccessPubkey { id, pubkey })
        })
        .collect()
}

fn decode_compact_metadata(bytes: &[u8]) -> Result<CompactMetaV1> {
    match wincode::config::deserialize::<LegacyCompactMetaV1, _>(bytes, wincode_leb128_config()) {
        Ok(legacy) => legacy.try_into(),
        Err(legacy_err) => wincode::config::deserialize(bytes, wincode_leb128_config())
            .with_context(|| format!("current metadata decode after legacy failed: {legacy_err}")),
    }
}

fn decode_existing_access_blob(bytes: &[u8], block_id: u32) -> Result<ArchiveV2BlockAccessBlob> {
    let version = bytes
        .get(..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| anyhow!("access block_id {block_id} is too short for version"))?;
    match version {
        1 => {
            let legacy: LegacyBlockAccessBlobV1 =
                wincode::config::deserialize(bytes, wincode_leb128_config())
                    .with_context(|| format!("decode legacy v1 access block_id {block_id}"))?;
            Ok(legacy.into())
        }
        WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION => {
            match wincode::config::deserialize(bytes, wincode_leb128_config()) {
                Ok(access) => Ok(access),
                Err(primary_err) => {
                    let legacy: LegacyBlockAccessBlobV2NoVotes =
                    wincode::config::deserialize(bytes, wincode_leb128_config()).with_context(
                        || format!("decode v2-no-votes access block_id {block_id}: current v2 decode failed: {primary_err}"),
                    )?;
                    Ok(legacy.into())
                }
            }
        }
        _ => anyhow::bail!("access block_id {block_id} has unsupported version {version}"),
    }
}

fn collect_access_log_refs(logs: &blockzilla_format::CompactLogStream, ids: &mut HashSet<u32>) {
    for event in &logs.events {
        match event {
            LogEvent::LoaderUpgradedProgram { program }
            | LogEvent::BpfInvoke { program }
            | LogEvent::Consumed { program, .. }
            | LogEvent::Success { program }
            | LogEvent::BpfSuccess { program }
            | LogEvent::Failure { program, .. }
            | LogEvent::BpfFailure { program, .. }
            | LogEvent::FailureCustomProgramError { program, .. }
            | LogEvent::BpfFailureCustomProgramError { program, .. }
            | LogEvent::FailureInvalidAccountData { program }
            | LogEvent::BpfFailureInvalidAccountData { program }
            | LogEvent::FailureInvalidProgramArgument { program }
            | LogEvent::BpfFailureInvalidProgramArgument { program }
            | LogEvent::Return { program, .. }
            | LogEvent::Invoke { program, .. } => collect_access_pubkey_id(*program, ids),
            LogEvent::LoaderFinalizedAccount { account }
            | LogEvent::RuntimeWritablePrivilegeEscalated { account }
            | LogEvent::RuntimeSignerPrivilegeEscalated { account }
            | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
                collect_access_pubkey_id(*account, ids)
            }
            LogEvent::ProgramIdLog { program, log } => {
                collect_access_pubkey_id(*program, ids);
                collect_access_program_log_refs(log, ids);
            }
            LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
                collect_access_program_log_refs(log, ids);
            }
            LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    collect_access_pubkey_id(*program, ids);
                }
            }
            LogEvent::System(_)
            | LogEvent::LogTruncated
            | LogEvent::StakeMergingAccounts
            | LogEvent::ProgramLogError { .. }
            | LogEvent::ProgramAccountNotWritable
            | LogEvent::ProgramIdMismatch
            | LogEvent::ProgramNotUpgradeable
            | LogEvent::ProgramAndProgramDataAccountMismatch
            | LogEvent::ProgramWasExtendedInThisBlockAlready
            | LogEvent::BpfConsumed { .. }
            | LogEvent::FailedToComplete { .. }
            | LogEvent::CustomProgramError { .. }
            | LogEvent::Data { .. }
            | LogEvent::Consumption { .. }
            | LogEvent::CbRequestUnits { .. }
            | LogEvent::UnknownProgram { .. }
            | LogEvent::UnknownAccount { .. }
            | LogEvent::VerifyEd25519
            | LogEvent::VerifySecp256k1
            | LogEvent::CloseContextState
            | LogEvent::Plain { .. }
            | LogEvent::Unparsed { .. } => {}
        }
    }
}

fn collect_access_program_log_refs(log: &ProgramLog, ids: &mut HashSet<u32>) {
    if let ProgramLog::Token2022(log) = log {
        match log {
            Token2022Log::ErrorHarvestingFrom { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom4 { account_key, .. } => {
                collect_access_pubkey_id(*account_key, ids);
            }
            _ => {}
        }
    }
}

fn collect_access_pubkey_id(key: CompactPubkey, ids: &mut HashSet<u32>) {
    if let CompactPubkey::Id(id) = key {
        ids.insert(id);
    }
}

#[derive(Clone, Copy)]
struct PreviousBlockhash {
    hash: [u8; 32],
    _slot: u64,
}

fn load_blockhash_registry(path: &Path) -> Result<Vec<[u8; 32]>> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() % 32 == 0,
        "{} has invalid blockhash registry byte length {}",
        path.display(),
        bytes.len()
    );
    Ok(bytes
        .chunks_exact(32)
        .map(|chunk| {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(chunk);
            hash
        })
        .collect())
}

fn load_previous_blockhash_tail(path: &Path) -> Result<Vec<PreviousBlockhash>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() % 40 == 0 {
        return Ok(bytes
            .chunks_exact(40)
            .map(|chunk| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&chunk[..32]);
                let slot = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
                PreviousBlockhash { hash, _slot: slot }
            })
            .collect());
    }
    anyhow::ensure!(
        bytes.len() % 32 == 0,
        "{} has invalid previous blockhash tail byte length {}",
        path.display(),
        bytes.len()
    );
    Ok(bytes
        .chunks_exact(32)
        .map(|chunk| {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(chunk);
            PreviousBlockhash { hash, _slot: 0 }
        })
        .collect())
}

fn resolve_access_previous_blockhash(
    block: &ArchiveV2HotBlockBlob,
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
) -> Result<[u8; 32]> {
    if block.header.blockhash_id == 0
        && block.header.previous_blockhash_id == 0
        && let Some(previous) = previous_tail.last()
    {
        return Ok(previous.hash);
    }
    resolve_access_blockhash_id(
        block.header.previous_blockhash_id as i32,
        blockhashes,
        previous_tail,
    )
}

fn resolve_access_blockhash_id(
    id: i32,
    blockhashes: &[[u8; 32]],
    previous_tail: &[PreviousBlockhash],
) -> Result<[u8; 32]> {
    if id >= 0 {
        return blockhashes
            .get(id as usize)
            .copied()
            .with_context(|| format!("blockhash id {id} is outside loaded registry"));
    }
    let index = i32::try_from(previous_tail.len())
        .context("previous blockhash tail exceeds i32::MAX")?
        .checked_add(id)
        .filter(|index| *index >= 0)
        .with_context(|| format!("previous-tail blockhash id {id} is outside loaded tail"))?
        as usize;
    previous_tail
        .get(index)
        .map(|entry| entry.hash)
        .with_context(|| {
            format!("previous-tail blockhash id {id} resolved to missing index {index}")
        })
}

fn load_vote_hash_registry(path: &Path) -> Result<Vec<VoteHashRegistryRow>> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() % 65 == 0,
        "{} has invalid vote hash registry byte length {}",
        path.display(),
        bytes.len()
    );
    Ok(bytes
        .chunks_exact(65)
        .map(|chunk| {
            let flags = chunk[0];
            let mut bank_hash = [0u8; 32];
            bank_hash.copy_from_slice(&chunk[1..33]);
            let mut block_id_hash = [0u8; 32];
            block_id_hash.copy_from_slice(&chunk[33..65]);
            VoteHashRegistryRow {
                bank_hash: (flags & 1 != 0).then_some(bank_hash),
                block_id_hash: (flags & 2 != 0).then_some(block_id_hash),
            }
        })
        .collect())
}

fn infer_epoch(rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow]) -> Result<u64> {
    let first = rows
        .first()
        .ok_or_else(|| anyhow!("empty hot-block index"))?;
    Ok(first.slot / SLOTS_PER_EPOCH)
}

fn build_get_block_index_rows(
    hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    access_rows: &[ArchiveV2BlockAccessIndexRow],
    epoch: u64,
) -> Result<Vec<ArchiveV2GetBlockIndexRow>> {
    let mut rows = vec![ArchiveV2GetBlockIndexRow::missing(); SLOTS_PER_EPOCH as usize];
    for hot_row in hot_rows {
        if hot_row.slot / SLOTS_PER_EPOCH != epoch {
            anyhow::bail!("slot {} is outside epoch {}", hot_row.slot, epoch);
        }
        let access_row = access_rows
            .get(hot_row.block_id as usize)
            .ok_or_else(|| anyhow!("missing access row for block_id {}", hot_row.block_id))?;
        anyhow::ensure!(
            access_row.block_id == hot_row.block_id
                && access_row.slot == hot_row.slot
                && access_row.tx_count == hot_row.tx_count
                && access_row.signature_count == hot_row.signature_count,
            "access row mismatch for block_id {}",
            hot_row.block_id
        );
        let slot_index = (hot_row.slot % SLOTS_PER_EPOCH) as usize;
        rows[slot_index] = ArchiveV2GetBlockIndexRow {
            block_offset: hot_row.compressed_offset,
            block_len: hot_row.compressed_len,
            access_offset: access_row.access_offset,
            access_len: access_row.access_len,
        };
    }
    Ok(rows)
}
