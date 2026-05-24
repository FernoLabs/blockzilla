use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash, ArchiveV2BlockAccessPubkey,
    ArchiveV2HotBlockBlob, ArchiveV2HotInstructionData, ArchiveV2HotMessagePayload,
    ArchiveV2VoteHashRef, archive_v2_hot_index_path, deserialize_archive_v2_hot_block_blob,
    read_archive_v2_block_access_index, read_archive_v2_hot_block_index, wincode_leb128_config,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Parser)]
struct Args {
    epoch_dir: PathBuf,
    #[arg(long, required = true)]
    slot: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyBlockAccessBlobV1 {
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
            version: value.version,
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

#[derive(Clone, Copy)]
struct VoteHashRegistryRow {
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let blocks_path = args.epoch_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let hot_index = read_archive_v2_hot_block_index(&archive_v2_hot_index_path(&blocks_path))?;
    let access_index = read_archive_v2_block_access_index(
        &args.epoch_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
    )?;
    let vote_hash_rows =
        load_vote_hash_registry(&args.epoch_dir.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE))?;
    let raw_blocks = hot_index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
    let mut block_file =
        File::open(&blocks_path).with_context(|| format!("open {}", blocks_path.display()))?;
    let mut access_file = File::open(args.epoch_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;

    println!(
        "slot\tblock_id\trequired_vote_hashes\tembedded_vote_hashes\tmissing_ids\tmismatched_ids\tmissing_registry_ids"
    );
    for slot in args.slot {
        let hot_row = hot_index
            .rows
            .iter()
            .find(|row| row.slot == slot)
            .ok_or_else(|| anyhow!("slot {slot} not found in hot index"))?;
        let access_row = access_index
            .rows
            .iter()
            .find(|row| row.slot == slot)
            .ok_or_else(|| anyhow!("slot {slot} not found in access index"))?;

        let block_bytes = read_hot_block(&mut block_file, raw_blocks, hot_row)?;
        let block = deserialize_archive_v2_hot_block_blob(&block_bytes)
            .with_context(|| format!("decode hot block slot {slot}"))?;
        let required = collect_vote_hash_ids_for_block(&block)?;
        let access = read_access_blob(&mut access_file, access_row)?;
        let embedded = access
            .vote_hashes
            .iter()
            .map(|row| (row.block_id, *row))
            .collect::<HashMap<_, _>>();

        let mut missing = Vec::new();
        let mut mismatched = Vec::new();
        let mut missing_registry = Vec::new();
        for id in &required {
            let Some(expected) = vote_hash_rows.get(*id as usize).copied() else {
                missing_registry.push(*id);
                continue;
            };
            match embedded.get(id) {
                None => missing.push(*id),
                Some(actual)
                    if actual.bank_hash != expected.bank_hash
                        || actual.block_id_hash != expected.block_id_hash =>
                {
                    mismatched.push(*id);
                }
                Some(_) => {}
            }
        }
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            slot,
            hot_row.block_id,
            required.len(),
            access.vote_hashes.len(),
            csv_list(&missing),
            csv_list(&mismatched),
            csv_list(&missing_registry)
        );
    }
    Ok(())
}

fn read_hot_block(
    file: &mut File,
    raw_blocks: bool,
    row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(row.compressed_offset))?;
    if raw_blocks {
        let mut bytes = vec![0; row.uncompressed_len as usize];
        file.read_exact(&mut bytes)?;
        return Ok(bytes);
    }
    let mut input = vec![0; row.compressed_len as usize];
    file.read_exact(&mut input)?;
    Ok(zstd::bulk::decompress(
        &input,
        row.uncompressed_len as usize,
    )?)
}

fn read_access_blob(
    file: &mut File,
    row: &blockzilla_format::ArchiveV2BlockAccessIndexRow,
) -> Result<ArchiveV2BlockAccessBlob> {
    file.seek(SeekFrom::Start(row.access_offset))?;
    let mut bytes = vec![0; row.access_len as usize];
    file.read_exact(&mut bytes)?;
    match wincode::config::deserialize(&bytes, wincode_leb128_config()) {
        Ok(access) => Ok(access),
        Err(_) => {
            let legacy: LegacyBlockAccessBlobV1 =
                wincode::config::deserialize(&bytes, wincode_leb128_config())?;
            Ok(legacy.into())
        }
    }
}

fn collect_vote_hash_ids_for_block(block: &ArchiveV2HotBlockBlob) -> Result<Vec<u32>> {
    let mut ids = HashSet::new();
    for row in &block.tx_rows {
        let message_slice = &block.message_bytes
            [row.message_offset as usize..(row.message_offset + row.message_len) as usize];
        let message: ArchiveV2HotMessagePayload =
            wincode::config::deserialize(message_slice, wincode_leb128_config())
                .with_context(|| format!("decode hot message tx_index={}", row.tx_index))?;
        match message {
            ArchiveV2HotMessagePayload::Legacy(message) => {
                for instruction in message.instructions {
                    collect_vote_hashes_from_instruction(&instruction.data, &mut ids);
                }
            }
            ArchiveV2HotMessagePayload::V0(message) => {
                for instruction in message.instructions {
                    collect_vote_hashes_from_instruction(&instruction.data, &mut ids);
                }
            }
        }
    }
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    Ok(ids)
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
        .map(|chunk| VoteHashRegistryRow {
            bank_hash: (chunk[0] & 1 != 0).then(|| chunk[1..33].try_into().unwrap()),
            block_id_hash: (chunk[0] & 2 != 0).then(|| chunk[33..65].try_into().unwrap()),
        })
        .collect())
}

fn csv_list(values: &[u32]) -> String {
    values
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
