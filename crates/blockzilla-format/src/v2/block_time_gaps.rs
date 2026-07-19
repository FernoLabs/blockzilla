//! Fixed-width sidecar describing slot and block-time gaps between archived blocks.
//!
//! This format records facts present in an archive: the slots and stored block times on either
//! side of each gap. A slot gap does not by itself distinguish a slot skipped by the chain from a
//! block missing from the archive, and consumers should not label it as an outage without more
//! evidence.

use std::io::{Read, Write};

use anyhow::{Context, Result, bail, ensure};

use super::{ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN, ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN};

pub const BLOCK_TIME_GAP_FILE: &str = "block-time-gaps.bin";
pub const BLOCK_TIME_GAP_MAGIC: [u8; 8] = *b"BZBTGAP1";
pub const BLOCK_TIME_GAP_VERSION: u16 = 1;
pub const BLOCK_TIME_GAP_HEADER_LEN: usize = 160;
pub const BLOCK_TIME_GAP_ROW_LEN: usize = 40;
pub const BLOCK_TIME_GAP_MISSING_TIME: i64 = i64::MIN;
/// With whole-second source timestamps, an adjacent-block delta above this value is a time gap.
pub const BLOCK_TIME_GAP_TIME_THRESHOLD_SECS: u64 = 1;

pub const BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING: u32 = 1 << 0;
pub const BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING: u32 = 1 << 1;
pub const BLOCK_TIME_GAP_FLAG_TIME_DECREASING: u32 = 1 << 2;

const BLOCK_TIME_GAP_KNOWN_ROW_FLAGS: u32 = BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING
    | BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING
    | BLOCK_TIME_GAP_FLAG_TIME_DECREASING;

/// The local artifact from which a block-time gap sidecar was derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
#[non_exhaustive]
pub enum BlockTimeGapSourceKind {
    BlockhashIndexV3 = 1,
    Car = 2,
    ArchiveV2Hot = 3,
}

impl BlockTimeGapSourceKind {
    fn from_disk(value: u32) -> Result<Self> {
        match value {
            1 => Ok(Self::BlockhashIndexV3),
            2 => Ok(Self::Car),
            3 => Ok(Self::ArchiveV2Hot),
            _ => bail!("unknown block-time gap source kind {value}"),
        }
    }
}

/// The 160-byte little-endian header of [`BLOCK_TIME_GAP_FILE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTimeGapHeader {
    pub magic: [u8; 8],
    pub version: u16,
    pub header_len: u16,
    pub row_len: u16,
    pub flags: u16,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub source_kind: BlockTimeGapSourceKind,
    /// Emit consecutive-slot rows when their positive time delta exceeds this value.
    pub time_gap_threshold_secs: u32,
    pub source_bytes: u64,
    pub source_sha256: [u8; 32],
    pub block_count: u64,
    pub gap_count: u64,
    pub missing_slot_count: u64,
    pub first_slot: u64,
    pub first_block_time: i64,
    pub last_slot: u64,
    pub last_block_time: i64,
    pub timed_gap_count: u64,
    pub missing_time_gap_count: u64,
    pub decreasing_time_gap_count: u64,
}

/// A 40-byte gap row. Its endpoints are consecutive blocks in the source artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockTimeGapRow {
    pub previous_slot: u64,
    pub next_slot: u64,
    pub previous_block_time: i64,
    pub next_block_time: i64,
    pub flags: u32,
    pub reserved: u32,
}

impl BlockTimeGapRow {
    /// Number of slots absent strictly between this row's endpoints.
    pub fn missing_slots(&self) -> Option<u64> {
        self.next_slot
            .checked_sub(self.previous_slot)?
            .checked_sub(1)
    }

    /// Stored wall-clock seconds between the endpoint blocks when both times are usable.
    pub fn elapsed_seconds(&self) -> Option<u64> {
        if self.previous_block_time == BLOCK_TIME_GAP_MISSING_TIME
            || self.next_block_time == BLOCK_TIME_GAP_MISSING_TIME
            || self.next_block_time < self.previous_block_time
        {
            return None;
        }

        Some((i128::from(self.next_block_time) - i128::from(self.previous_block_time)) as u64)
    }

    /// Flags implied by the two stored block times.
    pub fn classified_flags(&self) -> u32 {
        classify_row_flags(self.previous_block_time, self.next_block_time)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTimeGapSidecar {
    pub header: BlockTimeGapHeader,
    pub rows: Vec<BlockTimeGapRow>,
}

/// Write a validated sidecar using its deterministic fixed-width representation.
pub fn write_block_time_gap_sidecar(
    mut writer: impl Write,
    sidecar: &BlockTimeGapSidecar,
) -> Result<()> {
    validate_sidecar(sidecar)?;

    let header = encode_header(&sidecar.header);
    writer
        .write_all(&header)
        .context("write block-time gap header")?;
    for row in &sidecar.rows {
        let encoded = encode_row(row);
        writer
            .write_all(&encoded)
            .context("write block-time gap row")?;
    }
    Ok(())
}

/// Read and strictly validate one sidecar, including truncation and trailing bytes.
pub fn read_block_time_gap_sidecar(mut reader: impl Read) -> Result<BlockTimeGapSidecar> {
    let mut header_bytes = [0u8; BLOCK_TIME_GAP_HEADER_LEN];
    reader
        .read_exact(&mut header_bytes)
        .context("read block-time gap header")?;
    let header = decode_header(&header_bytes)?;
    validate_static_header(&header)?;

    ensure!(
        header.block_count <= header.slots_per_epoch,
        "block-time gap block count exceeds slots per epoch"
    );
    let maximum_gap_count = header.block_count.saturating_sub(1);
    ensure!(
        header.gap_count <= maximum_gap_count,
        "block-time gap row count {} exceeds maximum {maximum_gap_count} for {} blocks",
        header.gap_count,
        header.block_count
    );

    let row_count = usize::try_from(header.gap_count)
        .context("block-time gap row count exceeds this platform's address space")?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .context("allocate block-time gap rows")?;
    for row_index in 0..row_count {
        let mut row_bytes = [0u8; BLOCK_TIME_GAP_ROW_LEN];
        reader
            .read_exact(&mut row_bytes)
            .with_context(|| format!("read block-time gap row {row_index}"))?;
        rows.push(decode_row(&row_bytes));
    }

    let mut trailing = [0u8; 1];
    loop {
        match reader.read(&mut trailing) {
            Ok(0) => break,
            Ok(_) => bail!("block-time gap sidecar has trailing data"),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error).context("check block-time gap trailing data"),
        }
    }

    let sidecar = BlockTimeGapSidecar { header, rows };
    validate_sidecar(&sidecar)?;
    Ok(sidecar)
}

fn validate_static_header(header: &BlockTimeGapHeader) -> Result<()> {
    ensure!(
        header.magic == BLOCK_TIME_GAP_MAGIC,
        "invalid block-time gap magic"
    );
    ensure!(
        header.version == BLOCK_TIME_GAP_VERSION,
        "unsupported block-time gap version {}",
        header.version
    );
    ensure!(
        usize::from(header.header_len) == BLOCK_TIME_GAP_HEADER_LEN,
        "invalid block-time gap header length {}",
        header.header_len
    );
    ensure!(
        usize::from(header.row_len) == BLOCK_TIME_GAP_ROW_LEN,
        "invalid block-time gap row length {}",
        header.row_len
    );
    ensure!(header.flags == 0, "unknown block-time gap header flags");
    ensure!(
        header.time_gap_threshold_secs != 0,
        "block-time gap threshold must be positive"
    );
    ensure!(
        header.slots_per_epoch != 0,
        "block-time gap slots_per_epoch must be non-zero"
    );
    Ok(())
}

fn validate_sidecar(sidecar: &BlockTimeGapSidecar) -> Result<()> {
    let header = &sidecar.header;
    validate_static_header(header)?;
    ensure!(
        header.gap_count == sidecar.rows.len() as u64,
        "block-time gap header declares {} rows but {} were supplied",
        header.gap_count,
        sidecar.rows.len()
    );
    if header.source_kind == BlockTimeGapSourceKind::BlockhashIndexV3 {
        let expected_source_bytes = u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN)?
            .checked_add(
                header
                    .block_count
                    .checked_mul(u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN)?)
                    .context("blockhash index V3 source length overflows u64")?,
            )
            .context("blockhash index V3 source length overflows u64")?;
        ensure!(
            header.source_bytes == expected_source_bytes,
            "blockhash index V3 source length {} does not match {} blocks ({expected_source_bytes} bytes)",
            header.source_bytes,
            header.block_count
        );
    }
    let epoch_start = header
        .epoch
        .checked_mul(header.slots_per_epoch)
        .context("block-time gap epoch start overflows u64")?;
    let epoch_end = epoch_start
        .checked_add(header.slots_per_epoch)
        .context("block-time gap epoch end overflows u64")?;

    if header.block_count == 0 {
        ensure!(
            header.gap_count == 0 && header.missing_slot_count == 0,
            "an empty block-time gap source cannot contain gaps"
        );
        ensure!(
            header.first_slot == 0 && header.last_slot == 0,
            "an empty block-time gap source must use zero first and last slots"
        );
        ensure!(
            header.first_block_time == BLOCK_TIME_GAP_MISSING_TIME
                && header.last_block_time == BLOCK_TIME_GAP_MISSING_TIME,
            "an empty block-time gap source must use missing first and last times"
        );
    } else {
        ensure!(
            header.block_count <= header.slots_per_epoch,
            "block count exceeds slots per epoch"
        );
        ensure!(
            header.first_slot >= epoch_start && header.last_slot < epoch_end,
            "block-time gap endpoints lie outside epoch {} range {}..{}",
            header.epoch,
            epoch_start,
            epoch_end
        );
        ensure!(
            header.first_slot <= header.last_slot,
            "block-time gap first slot is after last slot"
        );
        if header.block_count == 1 {
            ensure!(
                header.first_slot == header.last_slot
                    && header.first_block_time == header.last_block_time,
                "a one-block source must have identical first and last block metadata"
            );
        }

        let slot_span = header
            .last_slot
            .checked_sub(header.first_slot)
            .and_then(|span| span.checked_add(1))
            .context("block-time gap source slot span overflows u64")?;
        let accounted_slots = header
            .block_count
            .checked_add(header.missing_slot_count)
            .context("block-time gap accounted slot count overflows u64")?;
        ensure!(
            accounted_slots == slot_span,
            "block count {} plus missing slots {} does not match source span {}",
            header.block_count,
            header.missing_slot_count,
            slot_span
        );
    }

    let mut missing_slot_count = 0u64;
    let mut timed_gap_count = 0u64;
    let mut missing_time_gap_count = 0u64;
    let mut decreasing_time_gap_count = 0u64;
    let mut previous_row: Option<&BlockTimeGapRow> = None;

    for (row_index, row) in sidecar.rows.iter().enumerate() {
        ensure!(
            row.reserved == 0,
            "block-time gap row {row_index} reserved field is non-zero"
        );
        ensure!(
            row.flags & !BLOCK_TIME_GAP_KNOWN_ROW_FLAGS == 0,
            "block-time gap row {row_index} has unknown flags {:#x}",
            row.flags & !BLOCK_TIME_GAP_KNOWN_ROW_FLAGS
        );
        let expected_flags = row.classified_flags();
        ensure!(
            row.flags == expected_flags,
            "block-time gap row {row_index} flags {:#x} do not match times (expected {expected_flags:#x})",
            row.flags
        );
        let row_missing_slots = row.missing_slots().with_context(|| {
            format!("block-time gap row {row_index} does not advance by at least one slot")
        })?;
        if row_missing_slots == 0 {
            let elapsed = row.elapsed_seconds();
            ensure!(
                expected_flags & BLOCK_TIME_GAP_FLAG_TIME_DECREASING != 0
                    || elapsed.is_some_and(|seconds| {
                        seconds > u64::from(header.time_gap_threshold_secs)
                    }),
                "block-time gap row {row_index} has consecutive slots without a time discontinuity"
            );
        }
        missing_slot_count = missing_slot_count
            .checked_add(row_missing_slots)
            .context("block-time gap missing slot count overflows u64")?;

        ensure!(
            header.block_count != 0
                && row.previous_slot >= header.first_slot
                && row.next_slot <= header.last_slot,
            "block-time gap row {row_index} lies outside the source's first and last slots"
        );
        if row.previous_slot == header.first_slot {
            ensure!(
                row.previous_block_time == header.first_block_time,
                "block-time gap row {row_index} disagrees with the first block time"
            );
        }
        if row.next_slot == header.last_slot {
            ensure!(
                row.next_block_time == header.last_block_time,
                "block-time gap row {row_index} disagrees with the last block time"
            );
        }

        if let Some(previous) = previous_row {
            ensure!(
                row.previous_slot >= previous.next_slot,
                "block-time gap row {row_index} overlaps or precedes the previous row"
            );
            if row.previous_slot == previous.next_slot {
                ensure!(
                    row.previous_block_time == previous.next_block_time,
                    "adjacent block-time gap rows disagree about their shared endpoint time"
                );
            }
        }
        previous_row = Some(row);

        if expected_flags
            & (BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING | BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING)
            != 0
        {
            missing_time_gap_count += 1;
        } else if expected_flags & BLOCK_TIME_GAP_FLAG_TIME_DECREASING != 0 {
            decreasing_time_gap_count += 1;
        } else {
            timed_gap_count += 1;
        }
    }

    ensure!(
        missing_slot_count == header.missing_slot_count,
        "derived missing slot count {missing_slot_count} does not match header {}",
        header.missing_slot_count
    );
    ensure!(
        timed_gap_count == header.timed_gap_count,
        "derived timed gap count {timed_gap_count} does not match header {}",
        header.timed_gap_count
    );
    ensure!(
        missing_time_gap_count == header.missing_time_gap_count,
        "derived missing-time gap count {missing_time_gap_count} does not match header {}",
        header.missing_time_gap_count
    );
    ensure!(
        decreasing_time_gap_count == header.decreasing_time_gap_count,
        "derived decreasing-time gap count {decreasing_time_gap_count} does not match header {}",
        header.decreasing_time_gap_count
    );
    ensure!(
        timed_gap_count
            .checked_add(missing_time_gap_count)
            .and_then(|count| count.checked_add(decreasing_time_gap_count))
            == Some(header.gap_count),
        "block-time gap timing classifications do not account for every row"
    );

    Ok(())
}

fn classify_row_flags(previous_block_time: i64, next_block_time: i64) -> u32 {
    let mut flags = 0;
    if previous_block_time == BLOCK_TIME_GAP_MISSING_TIME {
        flags |= BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING;
    }
    if next_block_time == BLOCK_TIME_GAP_MISSING_TIME {
        flags |= BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING;
    }
    if flags == 0 && next_block_time < previous_block_time {
        flags |= BLOCK_TIME_GAP_FLAG_TIME_DECREASING;
    }
    flags
}

fn encode_header(header: &BlockTimeGapHeader) -> [u8; BLOCK_TIME_GAP_HEADER_LEN] {
    let mut bytes = [0u8; BLOCK_TIME_GAP_HEADER_LEN];
    bytes[0..8].copy_from_slice(&header.magic);
    bytes[8..10].copy_from_slice(&header.version.to_le_bytes());
    bytes[10..12].copy_from_slice(&header.header_len.to_le_bytes());
    bytes[12..14].copy_from_slice(&header.row_len.to_le_bytes());
    bytes[14..16].copy_from_slice(&header.flags.to_le_bytes());
    bytes[16..24].copy_from_slice(&header.epoch.to_le_bytes());
    bytes[24..32].copy_from_slice(&header.slots_per_epoch.to_le_bytes());
    bytes[32..36].copy_from_slice(&(header.source_kind as u32).to_le_bytes());
    bytes[36..40].copy_from_slice(&header.time_gap_threshold_secs.to_le_bytes());
    bytes[40..48].copy_from_slice(&header.source_bytes.to_le_bytes());
    bytes[48..80].copy_from_slice(&header.source_sha256);
    bytes[80..88].copy_from_slice(&header.block_count.to_le_bytes());
    bytes[88..96].copy_from_slice(&header.gap_count.to_le_bytes());
    bytes[96..104].copy_from_slice(&header.missing_slot_count.to_le_bytes());
    bytes[104..112].copy_from_slice(&header.first_slot.to_le_bytes());
    bytes[112..120].copy_from_slice(&header.first_block_time.to_le_bytes());
    bytes[120..128].copy_from_slice(&header.last_slot.to_le_bytes());
    bytes[128..136].copy_from_slice(&header.last_block_time.to_le_bytes());
    bytes[136..144].copy_from_slice(&header.timed_gap_count.to_le_bytes());
    bytes[144..152].copy_from_slice(&header.missing_time_gap_count.to_le_bytes());
    bytes[152..160].copy_from_slice(&header.decreasing_time_gap_count.to_le_bytes());
    bytes
}

fn decode_header(bytes: &[u8; BLOCK_TIME_GAP_HEADER_LEN]) -> Result<BlockTimeGapHeader> {
    Ok(BlockTimeGapHeader {
        magic: bytes[0..8].try_into().unwrap(),
        version: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
        header_len: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
        row_len: u16::from_le_bytes(bytes[12..14].try_into().unwrap()),
        flags: u16::from_le_bytes(bytes[14..16].try_into().unwrap()),
        epoch: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        slots_per_epoch: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        source_kind: BlockTimeGapSourceKind::from_disk(u32::from_le_bytes(
            bytes[32..36].try_into().unwrap(),
        ))?,
        time_gap_threshold_secs: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        source_bytes: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        source_sha256: bytes[48..80].try_into().unwrap(),
        block_count: u64::from_le_bytes(bytes[80..88].try_into().unwrap()),
        gap_count: u64::from_le_bytes(bytes[88..96].try_into().unwrap()),
        missing_slot_count: u64::from_le_bytes(bytes[96..104].try_into().unwrap()),
        first_slot: u64::from_le_bytes(bytes[104..112].try_into().unwrap()),
        first_block_time: i64::from_le_bytes(bytes[112..120].try_into().unwrap()),
        last_slot: u64::from_le_bytes(bytes[120..128].try_into().unwrap()),
        last_block_time: i64::from_le_bytes(bytes[128..136].try_into().unwrap()),
        timed_gap_count: u64::from_le_bytes(bytes[136..144].try_into().unwrap()),
        missing_time_gap_count: u64::from_le_bytes(bytes[144..152].try_into().unwrap()),
        decreasing_time_gap_count: u64::from_le_bytes(bytes[152..160].try_into().unwrap()),
    })
}

fn encode_row(row: &BlockTimeGapRow) -> [u8; BLOCK_TIME_GAP_ROW_LEN] {
    let mut bytes = [0u8; BLOCK_TIME_GAP_ROW_LEN];
    bytes[0..8].copy_from_slice(&row.previous_slot.to_le_bytes());
    bytes[8..16].copy_from_slice(&row.next_slot.to_le_bytes());
    bytes[16..24].copy_from_slice(&row.previous_block_time.to_le_bytes());
    bytes[24..32].copy_from_slice(&row.next_block_time.to_le_bytes());
    bytes[32..36].copy_from_slice(&row.flags.to_le_bytes());
    bytes[36..40].copy_from_slice(&row.reserved.to_le_bytes());
    bytes
}

fn decode_row(bytes: &[u8; BLOCK_TIME_GAP_ROW_LEN]) -> BlockTimeGapRow {
    BlockTimeGapRow {
        previous_slot: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        next_slot: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        previous_block_time: i64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        next_block_time: i64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        flags: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
        reserved: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_sidecar() -> BlockTimeGapSidecar {
        BlockTimeGapSidecar {
            header: BlockTimeGapHeader {
                magic: BLOCK_TIME_GAP_MAGIC,
                version: BLOCK_TIME_GAP_VERSION,
                header_len: BLOCK_TIME_GAP_HEADER_LEN as u16,
                row_len: BLOCK_TIME_GAP_ROW_LEN as u16,
                flags: 0,
                epoch: 0,
                slots_per_epoch: 432_000,
                source_kind: BlockTimeGapSourceKind::BlockhashIndexV3,
                time_gap_threshold_secs: BLOCK_TIME_GAP_TIME_THRESHOLD_SECS as u32,
                source_bytes: (ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN
                    + 6 * ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN)
                    as u64,
                source_sha256: [0x5a; 32],
                block_count: 6,
                gap_count: 3,
                missing_slot_count: 4,
                first_slot: 10,
                first_block_time: 1_700_000_000,
                last_slot: 19,
                last_block_time: BLOCK_TIME_GAP_MISSING_TIME,
                timed_gap_count: 1,
                missing_time_gap_count: 1,
                decreasing_time_gap_count: 1,
            },
            rows: vec![
                BlockTimeGapRow {
                    previous_slot: 10,
                    next_slot: 12,
                    previous_block_time: 1_700_000_000,
                    next_block_time: 1_700_000_002,
                    flags: 0,
                    reserved: 0,
                },
                BlockTimeGapRow {
                    previous_slot: 13,
                    next_slot: 16,
                    previous_block_time: 1_700_000_004,
                    next_block_time: 1_700_000_003,
                    flags: BLOCK_TIME_GAP_FLAG_TIME_DECREASING,
                    reserved: 0,
                },
                BlockTimeGapRow {
                    previous_slot: 16,
                    next_slot: 18,
                    previous_block_time: 1_700_000_003,
                    next_block_time: BLOCK_TIME_GAP_MISSING_TIME,
                    flags: BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING,
                    reserved: 0,
                },
            ],
        }
    }

    fn encoded(sidecar: &BlockTimeGapSidecar) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_block_time_gap_sidecar(&mut bytes, sidecar).unwrap();
        bytes
    }

    #[test]
    fn fixed_width_roundtrip_is_deterministic() {
        let sidecar = sample_sidecar();
        let first = encoded(&sidecar);
        let second = encoded(&sidecar);

        assert_eq!(first, second);
        assert_eq!(
            first.len(),
            BLOCK_TIME_GAP_HEADER_LEN + sidecar.rows.len() * BLOCK_TIME_GAP_ROW_LEN
        );
        assert_eq!(&first[..8], b"BZBTGAP1");
        assert_eq!(&first[8..10], &1u16.to_le_bytes());
        assert_eq!(&first[88..96], &3u64.to_le_bytes());
        assert_eq!(
            read_block_time_gap_sidecar(first.as_slice()).unwrap(),
            sidecar
        );
    }

    #[test]
    fn archive_v2_hot_source_kind_roundtrips_without_changing_sidecar_version() {
        let mut sidecar = sample_sidecar();
        sidecar.header.source_kind = BlockTimeGapSourceKind::ArchiveV2Hot;
        sidecar.header.source_bytes = 987_654;

        let bytes = encoded(&sidecar);
        assert_eq!(&bytes[8..10], &BLOCK_TIME_GAP_VERSION.to_le_bytes());
        assert_eq!(&bytes[32..36], &3u32.to_le_bytes());
        assert_eq!(
            read_block_time_gap_sidecar(bytes.as_slice()).unwrap(),
            sidecar
        );
    }

    #[test]
    fn row_helpers_are_checked_and_classified() {
        let timed = sample_sidecar().rows[0];
        assert_eq!(timed.missing_slots(), Some(1));
        assert_eq!(timed.elapsed_seconds(), Some(2));

        let reversed_slots = BlockTimeGapRow {
            previous_slot: 12,
            next_slot: 10,
            ..timed
        };
        assert_eq!(reversed_slots.missing_slots(), None);

        let missing_time = sample_sidecar().rows[2];
        assert_eq!(missing_time.elapsed_seconds(), None);
        assert_eq!(
            missing_time.classified_flags(),
            BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING
        );

        let decreasing = sample_sidecar().rows[1];
        assert_eq!(decreasing.elapsed_seconds(), None);
        assert_eq!(
            decreasing.classified_flags(),
            BLOCK_TIME_GAP_FLAG_TIME_DECREASING
        );

        let equal_time = BlockTimeGapRow {
            previous_block_time: 10,
            next_block_time: 10,
            flags: 0,
            ..timed
        };
        assert_eq!(equal_time.elapsed_seconds(), Some(0));
        assert_eq!(equal_time.classified_flags(), 0);
    }

    #[test]
    fn reader_rejects_corruption_truncation_and_trailing_bytes() {
        let valid = encoded(&sample_sidecar());

        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 0xff;
        assert!(read_block_time_gap_sidecar(bad_magic.as_slice()).is_err());

        let mut bad_source_kind = valid.clone();
        bad_source_kind[32..36].copy_from_slice(&99u32.to_le_bytes());
        assert!(read_block_time_gap_sidecar(bad_source_kind.as_slice()).is_err());

        let mut unknown_row_flag = valid.clone();
        unknown_row_flag[BLOCK_TIME_GAP_HEADER_LEN + 32..BLOCK_TIME_GAP_HEADER_LEN + 36]
            .copy_from_slice(&(1u32 << 31).to_le_bytes());
        assert!(read_block_time_gap_sidecar(unknown_row_flag.as_slice()).is_err());

        assert!(read_block_time_gap_sidecar(&valid[..valid.len() - 1]).is_err());

        let mut trailing = valid;
        trailing.push(0);
        assert!(read_block_time_gap_sidecar(trailing.as_slice()).is_err());
    }

    #[test]
    fn writer_rejects_bad_counts_and_classifications() {
        let mut bad_missing_count = sample_sidecar();
        bad_missing_count.header.missing_slot_count += 1;
        assert!(write_block_time_gap_sidecar(Vec::new(), &bad_missing_count).is_err());

        let mut bad_time_count = sample_sidecar();
        bad_time_count.header.timed_gap_count += 1;
        assert!(write_block_time_gap_sidecar(Vec::new(), &bad_time_count).is_err());

        let mut bad_flags = sample_sidecar();
        bad_flags.rows[2].flags = 0;
        assert!(write_block_time_gap_sidecar(Vec::new(), &bad_flags).is_err());

        let mut overlapping = sample_sidecar();
        overlapping.rows[1].previous_slot = 11;
        assert!(write_block_time_gap_sidecar(Vec::new(), &overlapping).is_err());
    }

    #[test]
    fn empty_and_one_block_sources_have_unambiguous_endpoints() {
        let empty = BlockTimeGapSidecar {
            header: BlockTimeGapHeader {
                magic: BLOCK_TIME_GAP_MAGIC,
                version: BLOCK_TIME_GAP_VERSION,
                header_len: BLOCK_TIME_GAP_HEADER_LEN as u16,
                row_len: BLOCK_TIME_GAP_ROW_LEN as u16,
                flags: 0,
                epoch: 0,
                slots_per_epoch: 432_000,
                source_kind: BlockTimeGapSourceKind::Car,
                time_gap_threshold_secs: BLOCK_TIME_GAP_TIME_THRESHOLD_SECS as u32,
                source_bytes: 0,
                source_sha256: [0; 32],
                block_count: 0,
                gap_count: 0,
                missing_slot_count: 0,
                first_slot: 0,
                first_block_time: BLOCK_TIME_GAP_MISSING_TIME,
                last_slot: 0,
                last_block_time: BLOCK_TIME_GAP_MISSING_TIME,
                timed_gap_count: 0,
                missing_time_gap_count: 0,
                decreasing_time_gap_count: 0,
            },
            rows: Vec::new(),
        };
        assert_eq!(
            read_block_time_gap_sidecar(encoded(&empty).as_slice()).unwrap(),
            empty
        );

        let mut one = empty.clone();
        one.header.block_count = 1;
        one.header.first_slot = 123;
        one.header.last_slot = 123;
        one.header.first_block_time = 1_700_000_000;
        one.header.last_block_time = 1_700_000_000;
        assert!(write_block_time_gap_sidecar(Vec::new(), &one).is_ok());

        one.header.last_block_time += 1;
        assert!(write_block_time_gap_sidecar(Vec::new(), &one).is_err());

        let mut impossible_epoch = empty;
        impossible_epoch.header.epoch = u64::MAX;
        assert!(write_block_time_gap_sidecar(Vec::new(), &impossible_epoch).is_err());
    }

    #[test]
    fn consecutive_slots_require_a_real_time_discontinuity() {
        let mut sidecar = sample_sidecar();
        sidecar.header.block_count = 2;
        sidecar.header.source_bytes = (ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN
            + 2 * ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN)
            as u64;
        sidecar.header.gap_count = 1;
        sidecar.header.missing_slot_count = 0;
        sidecar.header.first_slot = 10;
        sidecar.header.last_slot = 11;
        sidecar.header.first_block_time = 100;
        sidecar.header.last_block_time = 102;
        sidecar.header.timed_gap_count = 1;
        sidecar.header.missing_time_gap_count = 0;
        sidecar.header.decreasing_time_gap_count = 0;
        sidecar.rows = vec![BlockTimeGapRow {
            previous_slot: 10,
            next_slot: 11,
            previous_block_time: 100,
            next_block_time: 102,
            flags: 0,
            reserved: 0,
        }];
        assert!(write_block_time_gap_sidecar(Vec::new(), &sidecar).is_ok());

        sidecar.rows[0].next_block_time = 101;
        sidecar.header.last_block_time = 101;
        assert!(write_block_time_gap_sidecar(Vec::new(), &sidecar).is_err());
    }
}
