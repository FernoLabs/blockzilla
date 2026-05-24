use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ArchiveV2BlockAccessBlob, ArchiveV2HotBlockBlob,
    ArchiveV2HotTxRow, CompactInnerInstructions, CompactLogStream, CompactMetaV1, CompactPubkey,
    CompactReturnData, CompactReward, CompactTokenBalance, CompactTransactionError, LogEvent,
    deserialize_archive_v2_hot_block_blob, read_archive_v2_block_access_index,
    read_archive_v2_hot_block_index, wincode_leb128_config,
};
use clap::Parser;
use serde::Deserialize;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};
use wincode::SchemaRead;

#[derive(Debug, Parser)]
struct Args {
    epoch_dir: PathBuf,
    #[arg(long)]
    slot: u64,
    #[arg(long)]
    tx_index: u32,
    #[arg(long)]
    access_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyCompactMetaV1 {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    logs: Option<CompactLogStream>,
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

fn main() -> Result<()> {
    let args = Args::parse();
    let blocks_path = args.epoch_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let index = read_archive_v2_hot_block_index(&blockzilla_format::archive_v2_hot_index_path(
        &blocks_path,
    ))?;
    let row = index
        .rows
        .iter()
        .find(|row| row.slot == args.slot)
        .ok_or_else(|| anyhow!("slot {} not found", args.slot))?;
    let raw_blocks = index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
    let mut file =
        File::open(&blocks_path).with_context(|| format!("open {}", blocks_path.display()))?;
    let block_bytes = read_hot_block(&mut file, raw_blocks, row)?;
    let block = deserialize_archive_v2_hot_block_blob(&block_bytes)
        .with_context(|| format!("decode slot {}", args.slot))?;
    let access = if let Some(access_dir) = args.access_dir {
        Some(read_access_blob(&access_dir, args.slot)?)
    } else {
        None
    };
    inspect_tx(&block, args.tx_index, access.as_ref())
}

fn inspect_tx(
    block: &ArchiveV2HotBlockBlob,
    tx_index: u32,
    access: Option<&ArchiveV2BlockAccessBlob>,
) -> Result<()> {
    let row = block
        .tx_rows
        .iter()
        .find(|row| row.tx_index == tx_index)
        .ok_or_else(|| anyhow!("tx_index {tx_index} not found"))?;
    println!(
        "tx_index={} flags={} metadata_offset={} metadata_len={}",
        row.tx_index, row.flags, row.metadata_offset, row.metadata_len
    );
    let Some(meta) = decode_metadata(block, row)? else {
        println!("metadata=None");
        return Ok(());
    };
    match meta.logs.as_ref() {
        None => println!("logs=None"),
        Some(logs) => {
            println!(
                "logs=Some events={} strings={} data={}",
                logs.events.len(),
                logs.strings.len(),
                logs.data.len()
            );
            if let Some(access) = access {
                let mut missing = Vec::new();
                for event in &logs.events {
                    collect_event_pubkey_ids(event, &mut missing, access);
                }
                missing.sort_unstable();
                missing.dedup();
                println!("access_pubkeys={}", access.pubkeys.len());
                println!("missing_log_pubkey_ids={missing:?}");
            }
            for (index, event) in logs.events.iter().take(8).enumerate() {
                println!("event[{index}]={event:?}");
            }
        }
    }
    Ok(())
}

fn collect_event_pubkey_ids(
    event: &LogEvent,
    missing: &mut Vec<u32>,
    access: &ArchiveV2BlockAccessBlob,
) {
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
        | LogEvent::Invoke { program, .. } => check_pubkey(*program, missing, access),
        LogEvent::LoaderFinalizedAccount { account }
        | LogEvent::RuntimeWritablePrivilegeEscalated { account }
        | LogEvent::RuntimeSignerPrivilegeEscalated { account }
        | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
            check_pubkey(*account, missing, access);
        }
        LogEvent::ProgramIdLog { program, .. } => check_pubkey(*program, missing, access),
        LogEvent::ProgramNotDeployed {
            program: Some(program),
        }
        | LogEvent::ProgramNotCached {
            program: Some(program),
        } => check_pubkey(*program, missing, access),
        _ => {}
    }
}

fn check_pubkey(key: CompactPubkey, missing: &mut Vec<u32>, access: &ArchiveV2BlockAccessBlob) {
    let CompactPubkey::Id(id) = key else {
        return;
    };
    if access.pubkeys.iter().all(|entry| entry.id != id) {
        missing.push(id);
    }
}

fn decode_metadata(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> Result<Option<CompactMetaV1>> {
    if row.metadata_len == 0 {
        return Ok(None);
    }
    let start = row.metadata_offset as usize;
    let end = start + row.metadata_len as usize;
    let bytes = block
        .metadata_bytes
        .get(start..end)
        .ok_or_else(|| anyhow!("metadata slice out of bounds"))?;
    match wincode::config::deserialize(bytes, wincode_leb128_config()) {
        Ok(meta) => Ok(Some(meta)),
        Err(primary_err) => {
            let legacy: LegacyCompactMetaV1 =
                wincode::config::deserialize(bytes, wincode_leb128_config()).with_context(
                    || format!("legacy decode after primary failed: {primary_err}"),
                )?;
            Ok(Some(legacy.try_into()?))
        }
    }
}

fn read_access_blob(access_dir: &PathBuf, slot: u64) -> Result<ArchiveV2BlockAccessBlob> {
    let index =
        read_archive_v2_block_access_index(&access_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE))?;
    let row = index
        .rows
        .iter()
        .find(|row| row.slot == slot)
        .ok_or_else(|| anyhow!("access slot {slot} not found"))?;
    let mut file = File::open(access_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;
    file.seek(SeekFrom::Start(row.access_offset))?;
    let mut bytes = vec![0; row.access_len as usize];
    file.read_exact(&mut bytes)?;
    Ok(wincode::config::deserialize(
        &bytes,
        wincode_leb128_config(),
    )?)
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
