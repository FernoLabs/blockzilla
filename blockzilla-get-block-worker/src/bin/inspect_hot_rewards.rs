use anyhow::{Context, Result, bail};
use blockzilla_format::{
    ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN,
    CompactPubkey, deserialize_archive_v2_hot_block_blob,
};
use std::collections::BTreeMap;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

const SLOTS_PER_EPOCH: u64 = 432_000;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let archive_root = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "/home/ach/dev/blockzilla-v2".to_string()),
    );
    let slots = args
        .next()
        .context("usage: inspect-hot-rewards ARCHIVE_ROOT slots_csv")?;

    println!(
        "slot\tepoch\tstatus\treward_count\ttypes\tlamports_sum\tpositive\tnegative\tzero\tsamples"
    );
    for slot in parse_slots(&slots)? {
        match inspect_slot(&archive_root, slot) {
            Ok(row) => println!(
                "{}\t{}\tok\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                slot,
                slot / SLOTS_PER_EPOCH,
                row.reward_count,
                row.types,
                row.lamports_sum,
                row.positive,
                row.negative,
                row.zero,
                row.samples,
            ),
            Err(err) => println!("{}\t{}\terror\t\t\t{}", slot, slot / SLOTS_PER_EPOCH, err),
        }
    }
    Ok(())
}

struct RewardRow {
    reward_count: usize,
    types: String,
    lamports_sum: i128,
    positive: usize,
    negative: usize,
    zero: usize,
    samples: String,
}

fn inspect_slot(root: &Path, slot: u64) -> Result<RewardRow> {
    let epoch = slot / SLOTS_PER_EPOCH;
    let slot_offset = slot % SLOTS_PER_EPOCH;
    let dir = root.join(format!("epoch-{epoch}"));

    let mut index = File::open(dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)).with_context(|| {
        format!(
            "open {}",
            dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE).display()
        )
    })?;
    let index_offset = slot_offset
        .checked_mul(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN as u64)
        .context("index offset overflow")?;
    index
        .seek(SeekFrom::Start(index_offset))
        .context("seek get-block index")?;
    let mut row = [0u8; ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN];
    index
        .read_exact(&mut row)
        .context("read get-block index row")?;

    let block_offset = u64::from_le_bytes(row[0..8].try_into().unwrap());
    let block_len = u32::from_le_bytes(row[8..12].try_into().unwrap()) as usize;
    if block_len == 0 {
        bail!("missing get-block index row");
    }

    let mut blocks = File::open(dir.join(ARCHIVE_V2_BLOCKS_FILE))
        .with_context(|| format!("open {}", dir.join(ARCHIVE_V2_BLOCKS_FILE).display()))?;
    blocks
        .seek(SeekFrom::Start(block_offset))
        .context("seek hot block blob")?;
    let mut compressed = vec![0u8; block_len];
    blocks
        .read_exact(&mut compressed)
        .context("read hot block blob")?;

    let mut bytes = Vec::new();
    zstd::stream::read::Decoder::new(&compressed[..])
        .context("init zstd decoder")?
        .read_to_end(&mut bytes)
        .context("decode zstd hot block")?;
    let block =
        deserialize_archive_v2_hot_block_blob(&bytes).context("decode hot block wincode")?;
    if block.header.slot != slot {
        bail!("decoded slot {} but requested {slot}", block.header.slot);
    }

    let decoded = block
        .header
        .rewards
        .as_ref()
        .map(|rewards| rewards.decoded.as_slice())
        .unwrap_or(&[]);
    let mut types = BTreeMap::<&'static str, usize>::new();
    let mut lamports_sum = 0i128;
    let mut positive = 0usize;
    let mut negative = 0usize;
    let mut zero = 0usize;
    for reward in decoded {
        *types
            .entry(reward_type_label(reward.reward_type))
            .or_default() += 1;
        lamports_sum += i128::from(reward.lamports);
        match reward.lamports.cmp(&0) {
            std::cmp::Ordering::Greater => positive += 1,
            std::cmp::Ordering::Less => negative += 1,
            std::cmp::Ordering::Equal => zero += 1,
        }
    }
    let sample_indexes = sample_indexes(decoded.len());
    let samples = sample_indexes
        .into_iter()
        .filter_map(|index| decoded.get(index).map(|reward| sample_label(index, reward)))
        .collect::<Vec<_>>()
        .join(" | ");
    Ok(RewardRow {
        reward_count: decoded.len(),
        types: types
            .into_iter()
            .map(|(kind, count)| format!("{kind}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
        lamports_sum,
        positive,
        negative,
        zero,
        samples,
    })
}

fn sample_indexes(len: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let mut indexes = vec![0, len / 2, len.saturating_sub(1)];
    indexes.sort_unstable();
    indexes.dedup();
    indexes
}

fn sample_label(index: usize, reward: &blockzilla_format::CompactReward) -> String {
    format!(
        "#{index}:{}:{}:{}:{}:{:?}",
        compact_pubkey_label(reward.pubkey),
        reward.lamports,
        reward.post_balance,
        reward_type_label(reward.reward_type),
        reward.commission
    )
}

fn reward_type_label(value: i32) -> &'static str {
    match value {
        1 => "Fee",
        2 => "Rent",
        3 => "Staking",
        4 => "Voting",
        _ => "Unknown",
    }
}

fn compact_pubkey_label(value: CompactPubkey) -> String {
    match value {
        CompactPubkey::Id(id) => format!("id:{id}"),
        CompactPubkey::Raw(bytes) => format!("raw:{}", bs58::encode(bytes).into_string()),
    }
}

fn parse_slots(input: &str) -> Result<Vec<u64>> {
    input
        .split([',', ' ', '\n', '\t'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<u64>()
                .with_context(|| format!("parse slot {part}"))
        })
        .collect()
}
