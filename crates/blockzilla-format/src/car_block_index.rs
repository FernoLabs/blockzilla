use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};
use wincode::{SchemaRead, SchemaWrite};

use crate::WincodeLeb128FramedReader;

pub const CAR_BLOCK_INDEX_VERSION: u16 = 1;
pub const CAR_BLOCK_INDEX_FILENAME: &str = "car-block-index.wincode";
pub const CAR_SLOT_RANGES_FILENAME: &str = "car-slot-ranges.raw";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum CarBlockIndexSourceKind {
    PlainCar,
    ZstdCar,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CarBlockIndexHeader {
    pub version: u16,
    pub source_kind: CarBlockIndexSourceKind,
    pub source_epoch: Option<u64>,
    pub source_file_len: u64,
    pub slots_per_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CarBlockIndexRow {
    pub slot: u64,
    pub block_id: u32,
    pub first_entry_index: u64,
    pub block_entry_index: u64,
    pub first_car_offset: u64,
    pub block_car_offset: u64,
    pub car_range_len: u64,
    pub node_count: u32,
    pub transaction_node_count: u32,
    pub entry_node_count: u32,
    pub dataframe_node_count: u32,
    pub rewards_node_count: u32,
    pub other_node_count: u32,
    pub expected_transaction_count: u32,
    pub poh_entry_count: u32,
    pub shredding_count: u32,
    pub dataframe_ref_count: u32,
    pub transaction_payload_bytes: u64,
    pub transaction_data_bytes: u64,
    pub transaction_metadata_bytes: u64,
    pub entry_payload_bytes: u64,
    pub dataframe_payload_bytes: u64,
    pub dataframe_data_bytes: u64,
    pub rewards_payload_bytes: u64,
    pub rewards_data_bytes: u64,
    pub block_payload_len: u32,
    pub max_payload_len: u32,
    pub max_entry_total_len: u32,
}

impl CarBlockIndexRow {
    #[inline]
    pub fn car_range_end(&self) -> u64 {
        self.first_car_offset.saturating_add(self.car_range_len)
    }

    #[inline]
    pub fn max_entry_buffer_hint(&self) -> usize {
        self.max_payload_len.max(self.max_entry_total_len) as usize
    }

    #[inline]
    pub fn block_range_buffer_hint(&self) -> usize {
        usize::try_from(self.car_range_len).unwrap_or(usize::MAX)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CarBlockIndexFooter {
    pub version: u16,
    pub truncated: bool,
    pub blocks: u64,
    pub car_entries: u64,
    pub source_logical_bytes: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub max_car_range_len: u64,
    pub max_payload_len: u32,
    pub max_entry_total_len: u32,
    pub transaction_node_count: u64,
    pub entry_node_count: u64,
    pub dataframe_node_count: u64,
    pub rewards_node_count: u64,
    pub other_node_count: u64,
    pub expected_transaction_count: u64,
    pub poh_entry_count: u64,
    pub shredding_count: u64,
    pub dataframe_ref_count: u64,
    pub transaction_payload_bytes: u64,
    pub transaction_data_bytes: u64,
    pub transaction_metadata_bytes: u64,
    pub entry_payload_bytes: u64,
    pub dataframe_payload_bytes: u64,
    pub dataframe_data_bytes: u64,
    pub rewards_payload_bytes: u64,
    pub rewards_data_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum CarBlockIndexRecord {
    Header(CarBlockIndexHeader),
    Row(CarBlockIndexRow),
    Footer(CarBlockIndexFooter),
}

#[derive(Debug, Clone)]
pub struct CarBlockIndex {
    pub header: CarBlockIndexHeader,
    pub rows: Vec<CarBlockIndexRow>,
    pub footer: CarBlockIndexFooter,
}

#[inline]
pub fn car_block_index_path(epoch_dir: &Path) -> PathBuf {
    epoch_dir.join(CAR_BLOCK_INDEX_FILENAME)
}

#[inline]
pub fn car_slot_ranges_path(epoch_dir: &Path) -> PathBuf {
    epoch_dir.join(CAR_SLOT_RANGES_FILENAME)
}

pub fn read_car_block_index(path: &Path) -> anyhow::Result<CarBlockIndex> {
    let file = File::open(path)?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(file));
    let mut header = None;
    let mut rows = Vec::new();
    let mut footer = None;

    while let Some((_, record)) = reader.read::<CarBlockIndexRecord>()? {
        match record {
            CarBlockIndexRecord::Header(value) => header = Some(value),
            CarBlockIndexRecord::Row(value) => rows.push(value),
            CarBlockIndexRecord::Footer(value) => footer = Some(value),
        }
    }

    let header = header.ok_or_else(|| anyhow::anyhow!("missing CAR block index header"))?;
    let footer = footer.ok_or_else(|| anyhow::anyhow!("missing CAR block index footer"))?;
    Ok(CarBlockIndex {
        header,
        rows,
        footer,
    })
}
