//! Transaction-directory v3 codec for the additive split-ledger v3 path.
//!
//! The active v2 writer remains unchanged. Production v3 output always uses
//! canonical varint-delta checkpoints. The fixed 56-byte checkpoint form is
//! retained only as a measurement baseline.

use std::{array, error::Error, fmt, ops::Range};

pub const MAGIC: [u8; 8] = *b"BZV2D003";
pub const VERSION: u16 = 1;
pub const FIXED_PREFIX_LEN: usize = 10;
pub const FIXED_CHECKPOINT_ROW_LEN: usize = 56;
pub const V2_DIRECTORY_ROW_LEN: usize = 40;
pub const DENSE_FIELD_COUNT: usize = 7;
pub const SPARSE_LANE_COUNT: usize = 2;
pub const OBJECT_COUNT: usize = 9;
pub const SUPPORTED_STRIDES: [u16; 3] = [32, 64, 128];

pub const SOURCE_FLAG_HAS_METADATA: u16 = 1 << 0;
pub const SOURCE_FLAG_METADATA_RAW_FALLBACK: u16 = 1 << 3;
pub const SOURCE_FLAG_MASK: u16 = (1 << 11) - 1;
pub const EFFECT_STATE_SEMANTIC_REWARDS: u8 = 1 << 7;

const CHECKPOINT_SECTION: usize = 0;
const DENSE_SECTION: usize = 1;
const REWARD_SECTION: usize = 2;
const RAW_SECTION: usize = 3;
const SECTION_COUNT: usize = 4;
const SIGNATURE_BYTES: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CheckpointCodec {
    /// Measurement baseline that retains the prior 56-byte absolute row.
    Fixed56 = 0,
    /// Canonical ULEB128 deltas from the preceding checkpoint.
    VarintDelta = 1,
}

impl CheckpointCodec {
    fn from_u32(value: u32) -> DirectoryResult<Self> {
        match value {
            0 => Ok(Self::Fixed56),
            1 => Ok(Self::VarintDelta),
            _ => Err(DirectoryError::new("unknown checkpoint codec")),
        }
    }
}

pub type DirectoryResult<T> = Result<T, DirectoryError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryError(String);

impl DirectoryError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for DirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for DirectoryError {}

fn require(condition: bool, message: impl Into<String>) -> DirectoryResult<()> {
    if condition {
        Ok(())
    } else {
        Err(DirectoryError::new(message))
    }
}

fn checked_add_u32(left: u32, right: u32, name: &str) -> DirectoryResult<u32> {
    left.checked_add(right)
        .ok_or_else(|| DirectoryError::new(format!("{name} exceeds u32")))
}

fn as_u32(value: u64, name: &str) -> DirectoryResult<u32> {
    u32::try_from(value).map_err(|_| DirectoryError::new(format!("{name} exceeds u32")))
}

fn supported_stride(stride: u16) -> bool {
    SUPPORTED_STRIDES.contains(&stride)
}

fn group_count(tx_count: u32, stride: u16) -> u32 {
    if tx_count == 0 {
        0
    } else {
        1 + (tx_count - 1) / u32::from(stride)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    pub tx_count: u32,
    pub stride: u16,
    pub group_count: u64,
    pub checkpoint_count: u64,
    pub minimum_varint_checkpoint_bytes: u64,
    pub minimum_header_bytes: u64,
    pub minimum_dense_bytes: u64,
    pub minimum_encoded_bytes: u64,
    pub worst_transaction_scan: u16,
    pub minimum_fits_u32_block_chunk: bool,
}

/// Calculate lower-bound geometry without allocating transaction storage.
pub fn geometry_for_tx_count(tx_count: u32, stride: u16) -> DirectoryResult<Geometry> {
    require(supported_stride(stride), "unsupported checkpoint stride")?;
    let groups = u64::from(group_count(tx_count, stride));
    let checkpoints = groups + 1;
    // Each delta checkpoint contains dense and sparse offsets, a signature
    // delta, and nine object-end deltas. Every canonical ULEB needs at least
    // one byte. The initial all-zero checkpoint is implicit.
    let checkpoint_bytes = groups
        .checked_mul(13)
        .ok_or_else(|| DirectoryError::new("checkpoint geometry overflow"))?;
    // One control varint and seven one-byte length varints is the minimum.
    let minimum_dense_bytes = u64::from(tx_count)
        .checked_mul(8)
        .ok_or_else(|| DirectoryError::new("dense geometry overflow"))?;
    // Fixed magic/version plus canonical ULEBs for codec, stride, transaction
    // and group counts, and four section lengths.
    let minimum_header_bytes = (FIXED_PREFIX_LEN as u64)
        + 1
        + canonical_uleb_len_u64(u64::from(stride)) as u64
        + canonical_uleb_len_u64(u64::from(tx_count)) as u64
        + canonical_uleb_len_u64(groups) as u64
        + canonical_uleb_len_u64(checkpoint_bytes) as u64
        + canonical_uleb_len_u64(minimum_dense_bytes) as u64
        + 1
        + 1;
    let minimum_encoded_bytes = minimum_header_bytes
        .checked_add(checkpoint_bytes)
        .and_then(|value| value.checked_add(minimum_dense_bytes))
        .ok_or_else(|| DirectoryError::new("directory geometry overflow"))?;
    let worst_transaction_scan = if tx_count == 0 {
        0
    } else {
        u16::try_from(u32::from(stride).min(tx_count)).expect("supported strides fit u16")
    };
    Ok(Geometry {
        tx_count,
        stride,
        group_count: groups,
        checkpoint_count: checkpoints,
        minimum_varint_checkpoint_bytes: checkpoint_bytes,
        minimum_header_bytes,
        minimum_dense_bytes,
        minimum_encoded_bytes,
        worst_transaction_scan,
        minimum_fits_u32_block_chunk: minimum_encoded_bytes <= u64::from(u32::MAX),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionReward {
    /// Metadata is missing or is stored in the raw fallback lane.
    Absent,
    /// The exact source Vec field is the canonical one-byte empty encoding.
    CanonicalEmpty,
    /// The stored range contains semantic, nonempty transaction rewards.
    SemanticStored(u64),
    /// The stored range contains an accepted noncanonical empty Vec encoding.
    NoncanonicalEmptyStored(u64),
}

impl TransactionReward {
    pub const fn stored_len(self) -> Option<u64> {
        match self {
            Self::SemanticStored(length) | Self::NoncanonicalEmptyStored(length) => Some(length),
            Self::Absent | Self::CanonicalEmpty => None,
        }
    }
}

/// The directory input for one source transaction.
///
/// `dense_lengths` follows the object order messages, loaded addresses, inner
/// instructions, logs, token balances, balances, and outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionLayout {
    pub source_flags: u16,
    pub effect_state: u8,
    pub signature_count: u8,
    pub dense_lengths: [u64; DENSE_FIELD_COUNT],
    pub reward: TransactionReward,
    pub raw_metadata_fallback_len: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataKind {
    Missing,
    RawFallback,
    Decoded,
}

fn metadata_kind(source_flags: u16) -> DirectoryResult<MetadataKind> {
    require(
        source_flags & !SOURCE_FLAG_MASK == 0,
        "transaction has unknown source flags",
    )?;
    let has_metadata = source_flags & SOURCE_FLAG_HAS_METADATA != 0;
    let raw_fallback = source_flags & SOURCE_FLAG_METADATA_RAW_FALLBACK != 0;
    require(
        has_metadata || !raw_fallback,
        "raw metadata flag is set without metadata",
    )?;
    Ok(if !has_metadata {
        MetadataKind::Missing
    } else if raw_fallback {
        MetadataKind::RawFallback
    } else {
        MetadataKind::Decoded
    })
}

fn validate_effect_state(effect_state: u8) -> DirectoryResult<()> {
    require(
        matches!(effect_state & 0b111, 1..=3) && effect_state & 0b0001_1000 == 0b0001_1000,
        "decoded metadata effect state is invalid",
    )
}

fn validate_layout(layout: &TransactionLayout) -> DirectoryResult<()> {
    require(layout.dense_lengths[0] != 0, "message length is zero")?;
    let kind = metadata_kind(layout.source_flags)?;
    match kind {
        MetadataKind::Missing => {
            require(
                layout.effect_state == 0,
                "missing metadata has effect state",
            )?;
            require(
                layout.dense_lengths[1..].iter().all(|length| *length == 0),
                "missing metadata has dense effect bytes",
            )?;
            require(
                layout.reward == TransactionReward::Absent,
                "missing metadata has a reward encoding",
            )?;
            require(
                layout.raw_metadata_fallback_len.is_none(),
                "missing metadata has raw fallback bytes",
            )?;
        }
        MetadataKind::RawFallback => {
            require(layout.effect_state == 0, "raw metadata has effect state")?;
            require(
                layout.dense_lengths[1..].iter().all(|length| *length == 0),
                "raw metadata has dense effect bytes",
            )?;
            require(
                layout.reward == TransactionReward::Absent,
                "raw metadata has a reward encoding",
            )?;
            require(
                layout
                    .raw_metadata_fallback_len
                    .is_some_and(|length| length != 0),
                "raw metadata fallback is absent or empty",
            )?;
        }
        MetadataKind::Decoded => {
            validate_effect_state(layout.effect_state)?;
            require(
                layout.dense_lengths[1..].iter().all(|length| *length != 0),
                "decoded metadata has an empty dense field",
            )?;
            require(
                layout.raw_metadata_fallback_len.is_none(),
                "decoded metadata has raw fallback bytes",
            )?;
            let semantic = layout.effect_state & EFFECT_STATE_SEMANTIC_REWARDS != 0;
            match layout.reward {
                TransactionReward::Absent => {
                    return Err(DirectoryError::new(
                        "decoded metadata has no transaction-reward encoding",
                    ));
                }
                TransactionReward::CanonicalEmpty => require(
                    !semantic,
                    "semantic reward state uses an implicit empty reward",
                )?,
                TransactionReward::SemanticStored(length) => {
                    require(length != 0, "semantic reward range is empty")?;
                    require(semantic, "semantic reward bytes have no semantic state")?;
                }
                TransactionReward::NoncanonicalEmptyStored(length) => {
                    require(length != 0, "noncanonical reward range is empty")?;
                    require(
                        !semantic,
                        "noncanonical empty reward has semantic reward state",
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_wire_record(
    record: &DenseRecord,
    reward_len: Option<u32>,
    raw_len: Option<u32>,
) -> DirectoryResult<()> {
    require(record.lengths[0] != 0, "message length is zero")?;
    let kind = metadata_kind(record.source_flags)?;
    match kind {
        MetadataKind::Missing => {
            require(
                record.effect_state == 0,
                "missing metadata has effect state",
            )?;
            require(
                record.lengths[1..].iter().all(|length| *length == 0),
                "missing metadata has dense effect bytes",
            )?;
            require(reward_len.is_none(), "missing metadata has reward bytes")?;
            require(raw_len.is_none(), "missing metadata has raw fallback bytes")?;
        }
        MetadataKind::RawFallback => {
            require(record.effect_state == 0, "raw metadata has effect state")?;
            require(
                record.lengths[1..].iter().all(|length| *length == 0),
                "raw metadata has dense effect bytes",
            )?;
            require(reward_len.is_none(), "raw metadata has reward bytes")?;
            require(raw_len.is_some(), "raw metadata has no raw fallback bytes")?;
        }
        MetadataKind::Decoded => {
            validate_effect_state(record.effect_state)?;
            require(
                record.lengths[1..].iter().all(|length| *length != 0),
                "decoded metadata has an empty dense field",
            )?;
            require(raw_len.is_none(), "decoded metadata has raw fallback bytes")?;
            let semantic = record.effect_state & EFFECT_STATE_SEMANTIC_REWARDS != 0;
            require(
                !semantic || reward_len.is_some(),
                "semantic reward state has no stored reward bytes",
            )?;
        }
    }
    Ok(())
}

fn put_uleb_u32(mut value: u32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn put_uleb_u64(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn canonical_uleb_len_u64(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

fn read_uleb_u32(input: &[u8], cursor: &mut usize, limit: usize) -> DirectoryResult<u32> {
    let value = read_uleb_u64(input, cursor, limit)?;
    u32::try_from(value).map_err(|_| DirectoryError::new("ULEB128 value exceeds u32"))
}

fn read_uleb_u64(input: &[u8], cursor: &mut usize, limit: usize) -> DirectoryResult<u64> {
    let start = *cursor;
    let mut value = 0_u64;
    for index in 0..10 {
        require(*cursor < limit, "truncated ULEB128 value")?;
        let byte = input[*cursor];
        *cursor += 1;
        let payload = u64::from(byte & 0x7f);
        if index == 9 {
            require(payload <= 1, "ULEB128 value exceeds u64")?;
        }
        value |= payload << (index * 7);
        if byte & 0x80 == 0 {
            require(
                *cursor - start == canonical_uleb_len_u64(value),
                "noncanonical ULEB128 value",
            )?;
            return Ok(value);
        }
    }
    Err(DirectoryError::new("ULEB128 value exceeds ten bytes"))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Section {
    offset: u32,
    len: u32,
}

impl Section {
    fn end(self) -> DirectoryResult<u32> {
        checked_add_u32(self.offset, self.len, "section end")
    }

    fn range(self) -> Range<usize> {
        self.offset as usize..(self.offset + self.len) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryHeader {
    pub checkpoint_codec: CheckpointCodec,
    pub stride: u16,
    pub tx_count: u32,
    pub group_count: u32,
    pub header_len: u32,
    sections: [Section; SECTION_COUNT],
}

impl DirectoryHeader {
    pub fn section_lengths(self) -> SectionSizes {
        SectionSizes {
            header_bytes: u64::from(self.header_len),
            checkpoint_bytes: u64::from(self.sections[CHECKPOINT_SECTION].len),
            dense_bytes: u64::from(self.sections[DENSE_SECTION].len),
            reward_bytes: u64::from(self.sections[REWARD_SECTION].len),
            raw_fallback_bytes: u64::from(self.sections[RAW_SECTION].len),
            total_bytes: u64::from(self.sections[RAW_SECTION].offset)
                + u64::from(self.sections[RAW_SECTION].len),
        }
    }

    fn decode(input: &[u8]) -> DirectoryResult<Self> {
        require(
            input.len() <= u32::MAX as usize,
            "directory block chunk exceeds u32",
        )?;
        require(
            input.len() >= FIXED_PREFIX_LEN,
            "directory header is truncated",
        )?;
        require(input[0..8] == MAGIC, "directory magic differs")?;
        require(
            read_fixed_u16(input, 8) == VERSION,
            "directory version differs",
        )?;
        let mut cursor = FIXED_PREFIX_LEN;
        let checkpoint_codec =
            CheckpointCodec::from_u32(read_uleb_u32(input, &mut cursor, input.len())?)?;
        let stride_value = read_uleb_u32(input, &mut cursor, input.len())?;
        let stride = u16::try_from(stride_value)
            .map_err(|_| DirectoryError::new("checkpoint stride exceeds u16"))?;
        require(supported_stride(stride), "unsupported checkpoint stride")?;
        let tx_count = read_uleb_u32(input, &mut cursor, input.len())?;
        let groups = read_uleb_u32(input, &mut cursor, input.len())?;
        require(
            groups == group_count(tx_count, stride),
            "directory group count differs",
        )?;
        let mut section_lengths = [0_u32; SECTION_COUNT];
        for length in &mut section_lengths {
            *length = read_uleb_u32(input, &mut cursor, input.len())?;
        }
        if checkpoint_codec == CheckpointCodec::Fixed56 {
            let expected_checkpoint_len = (u64::from(groups) + 1)
                .checked_mul(FIXED_CHECKPOINT_ROW_LEN as u64)
                .ok_or_else(|| DirectoryError::new("checkpoint section length overflow"))?;
            require(
                expected_checkpoint_len == u64::from(section_lengths[CHECKPOINT_SECTION]),
                "fixed checkpoint section length differs",
            )?;
        }
        let header_len = u32::try_from(cursor)
            .map_err(|_| DirectoryError::new("directory header exceeds u32"))?;
        let mut sections = [Section::default(); SECTION_COUNT];
        let mut expected_offset = header_len;
        for (index, section) in sections.iter_mut().enumerate() {
            *section = Section {
                offset: expected_offset,
                len: section_lengths[index],
            };
            expected_offset = section.end()?;
        }
        require(
            expected_offset as usize == input.len(),
            "directory byte length differs from sections",
        )?;
        Ok(Self {
            checkpoint_codec,
            stride,
            tx_count,
            group_count: groups,
            header_len,
            sections,
        })
    }
}

fn encode_header(
    checkpoint_codec: CheckpointCodec,
    stride: u16,
    tx_count: u32,
    groups: u32,
    section_lengths: [u32; SECTION_COUNT],
) -> DirectoryResult<(DirectoryHeader, Vec<u8>)> {
    let mut output = Vec::new();
    output.extend_from_slice(&MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    put_uleb_u32(checkpoint_codec as u32, &mut output);
    put_uleb_u32(u32::from(stride), &mut output);
    put_uleb_u32(tx_count, &mut output);
    put_uleb_u32(groups, &mut output);
    for length in section_lengths {
        put_uleb_u32(length, &mut output);
    }
    let header_len = u32::try_from(output.len())
        .map_err(|_| DirectoryError::new("directory header exceeds u32"))?;
    let mut sections = [Section::default(); SECTION_COUNT];
    let mut offset = header_len;
    for (index, section) in sections.iter_mut().enumerate() {
        *section = Section {
            offset,
            len: section_lengths[index],
        };
        offset = section.end()?;
    }
    Ok((
        DirectoryHeader {
            checkpoint_codec,
            stride,
            tx_count,
            group_count: groups,
            header_len,
            sections,
        },
        output,
    ))
}

fn read_fixed_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        input[offset..offset + 2]
            .try_into()
            .expect("fixed header range"),
    )
}

fn read_fixed_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        input[offset..offset + 4]
            .try_into()
            .expect("fixed header range"),
    )
}

fn read_fixed_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("fixed row range"),
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Checkpoint {
    dense_offset: u32,
    signature_prefix: u64,
    object_ends: [u32; OBJECT_COUNT],
    reward_offset: u32,
    raw_offset: u32,
}

impl Checkpoint {
    fn encode_fixed(self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.dense_offset.to_le_bytes());
        output.extend_from_slice(&self.signature_prefix.to_le_bytes());
        for end in self.object_ends {
            output.extend_from_slice(&end.to_le_bytes());
        }
        output.extend_from_slice(&self.reward_offset.to_le_bytes());
        output.extend_from_slice(&self.raw_offset.to_le_bytes());
    }

    fn decode_fixed(input: &[u8]) -> DirectoryResult<Self> {
        require(
            input.len() == FIXED_CHECKPOINT_ROW_LEN,
            "checkpoint row length differs",
        )?;
        let mut object_ends = [0_u32; OBJECT_COUNT];
        for (index, end) in object_ends.iter_mut().enumerate() {
            *end = read_fixed_u32(input, 12 + index * 4);
        }
        Ok(Self {
            dense_offset: read_fixed_u32(input, 0),
            signature_prefix: read_fixed_u64(input, 4),
            object_ends,
            reward_offset: read_fixed_u32(input, 48),
            raw_offset: read_fixed_u32(input, 52),
        })
    }

    fn encode_delta(self, prior: Self, output: &mut Vec<u8>) -> DirectoryResult<()> {
        put_uleb_u32(
            self.dense_offset
                .checked_sub(prior.dense_offset)
                .ok_or_else(|| DirectoryError::new("checkpoint dense offset decreases"))?,
            output,
        );
        put_uleb_u64(
            self.signature_prefix
                .checked_sub(prior.signature_prefix)
                .ok_or_else(|| DirectoryError::new("checkpoint signature prefix decreases"))?,
            output,
        );
        for (end, prior_end) in self.object_ends.into_iter().zip(prior.object_ends) {
            put_uleb_u32(
                end.checked_sub(prior_end)
                    .ok_or_else(|| DirectoryError::new("checkpoint object end decreases"))?,
                output,
            );
        }
        put_uleb_u32(
            self.reward_offset
                .checked_sub(prior.reward_offset)
                .ok_or_else(|| DirectoryError::new("checkpoint reward offset decreases"))?,
            output,
        );
        put_uleb_u32(
            self.raw_offset
                .checked_sub(prior.raw_offset)
                .ok_or_else(|| DirectoryError::new("checkpoint raw offset decreases"))?,
            output,
        );
        Ok(())
    }

    fn decode_delta(
        input: &[u8],
        cursor: &mut usize,
        limit: usize,
        prior: Self,
    ) -> DirectoryResult<Self> {
        let dense_offset = checked_add_u32(
            prior.dense_offset,
            read_uleb_u32(input, cursor, limit)?,
            "checkpoint dense offset",
        )?;
        let signature_prefix = prior
            .signature_prefix
            .checked_add(read_uleb_u64(input, cursor, limit)?)
            .ok_or_else(|| DirectoryError::new("checkpoint signature prefix overflow"))?;
        let mut object_ends = prior.object_ends;
        for end in &mut object_ends {
            *end = checked_add_u32(
                *end,
                read_uleb_u32(input, cursor, limit)?,
                "checkpoint object end",
            )?;
        }
        let reward_offset = checked_add_u32(
            prior.reward_offset,
            read_uleb_u32(input, cursor, limit)?,
            "checkpoint reward offset",
        )?;
        let raw_offset = checked_add_u32(
            prior.raw_offset,
            read_uleb_u32(input, cursor, limit)?,
            "checkpoint raw offset",
        )?;
        Ok(Self {
            dense_offset,
            signature_prefix,
            object_ends,
            reward_offset,
            raw_offset,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionSizes {
    pub header_bytes: u64,
    pub checkpoint_bytes: u64,
    pub dense_bytes: u64,
    pub reward_bytes: u64,
    pub raw_fallback_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointMeasurement {
    pub codec: CheckpointCodec,
    pub sizes: SectionSizes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrideMeasurement {
    pub stride: u16,
    pub worst_transaction_scan: u16,
    pub fixed56: CheckpointMeasurement,
    pub varint_delta: CheckpointMeasurement,
    pub selected_checkpoint_codec: CheckpointCodec,
    pub sizes: SectionSizes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedDirectory {
    pub bytes: Vec<u8>,
    pub measurement: StrideMeasurement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BestEncoding {
    pub encoded: EncodedDirectory,
    pub measurements: [StrideMeasurement; 3],
}

#[derive(Debug, Clone, Copy)]
pub struct V2BlockAdapterInput<'a> {
    pub directory: &'a [u8],
    pub transaction_rewards: &'a [u8],
    pub raw_metadata_fallbacks: &'a [u8],
    pub first_signature_ordinal: u64,
    pub signature_count: u32,
    pub final_object_decoded_lengths: [u32; OBJECT_COUNT],
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct V2BlockAdapterOptions {
    /// Retain the winning directory bytes. Size-only callers can leave this
    /// false so each stride candidate is dropped after it is measured.
    pub include_encoded_winner: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WinningDirectoryMeasurement {
    pub stride: u16,
    pub checkpoint_codec: CheckpointCodec,
    pub worst_transaction_scan: u16,
    pub sizes: SectionSizes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2BlockAdapterResult {
    pub tx_count: u32,
    pub first_signature_ordinal: u64,
    pub signature_count: u32,
    pub source_v2_object_lengths: [u32; OBJECT_COUNT],
    pub v3_object_lengths: [u32; OBJECT_COUNT],
    pub canonical_reward_fields_elided: u32,
    pub canonical_reward_bytes_elided: u32,
    pub stored_reward_records: u32,
    pub raw_fallback_records: u32,
    pub measurements: [StrideMeasurement; 3],
    pub winner: WinningDirectoryMeasurement,
    pub encoded_winner: Option<EncodedDirectory>,
}

/// Encode and measure all supported strides. Equal byte counts select the
/// smaller stride.
pub fn encode_best(transactions: &[TransactionLayout]) -> DirectoryResult<BestEncoding> {
    let mut encodings = Vec::with_capacity(SUPPORTED_STRIDES.len());
    for stride in SUPPORTED_STRIDES {
        encodings.push(encode_with_stride(transactions, stride)?);
    }
    let measurements = [
        encodings[0].measurement,
        encodings[1].measurement,
        encodings[2].measurement,
    ];
    let mut best_index = 0;
    for index in 1..encodings.len() {
        if encodings[index].bytes.len() < encodings[best_index].bytes.len() {
            best_index = index;
        }
    }
    Ok(BestEncoding {
        encoded: encodings.swap_remove(best_index),
        measurements,
    })
}

/// Encode all supported strides with canonical varint-delta checkpoints and
/// select the smallest result. Equal byte counts select the smaller stride.
///
/// This is the only directory encoder used by the split-ledger v3 writer.
/// `encode_best` remains available for measurement of the fixed checkpoint
/// baseline.
pub fn encode_best_varint_delta(
    transactions: &[TransactionLayout],
) -> DirectoryResult<BestEncoding> {
    let mut encodings = Vec::with_capacity(SUPPORTED_STRIDES.len());
    for stride in SUPPORTED_STRIDES {
        encodings.push(encode_with_stride_varint_delta(transactions, stride)?);
    }
    let measurements = [
        encodings[0].measurement,
        encodings[1].measurement,
        encodings[2].measurement,
    ];
    let mut best_index = 0;
    for index in 1..encodings.len() {
        if encodings[index].bytes.len() < encodings[best_index].bytes.len() {
            best_index = index;
        }
    }
    Ok(BestEncoding {
        encoded: encodings.swap_remove(best_index),
        measurements,
    })
}

/// Adapt one decoded standalone v2 block directory without reading the source
/// archive again.
///
/// Allocation is bounded by the exact 40-byte row count and every count stays
/// within the v2 u32 locator domain. The reward bytes are inspected only to
/// distinguish the exact canonical empty field (`[0]`) from a stored semantic
/// or noncanonical field. All stored bytes remain source slices; this adapter
/// does not rewrite an object chunk.
pub fn measure_v2_block_directory(
    input: V2BlockAdapterInput<'_>,
    options: V2BlockAdapterOptions,
) -> DirectoryResult<V2BlockAdapterResult> {
    require(
        input.directory.len().is_multiple_of(V2_DIRECTORY_ROW_LEN),
        "v2 directory length is not a multiple of 40",
    )?;
    let tx_count_usize = input.directory.len() / V2_DIRECTORY_ROW_LEN;
    let tx_count = u32::try_from(tx_count_usize)
        .map_err(|_| DirectoryError::new("v2 directory transaction count exceeds u32"))?;
    require(
        input.transaction_rewards.len() == input.final_object_decoded_lengths[7] as usize,
        "v2 transaction-reward chunk length differs from final directory end",
    )?;
    require(
        input.raw_metadata_fallbacks.len() == input.final_object_decoded_lengths[8] as usize,
        "v2 raw-fallback chunk length differs from final directory end",
    )?;
    let signature_end = input
        .first_signature_ordinal
        .checked_add(u64::from(input.signature_count))
        .ok_or_else(|| DirectoryError::new("v2 block signature ordinal range overflows u64"))?;
    input
        .first_signature_ordinal
        .checked_mul(SIGNATURE_BYTES)
        .ok_or_else(|| DirectoryError::new("v2 first signature byte offset overflows u64"))?;
    signature_end
        .checked_mul(SIGNATURE_BYTES)
        .ok_or_else(|| DirectoryError::new("v2 final signature byte offset overflows u64"))?;

    let mut layouts = Vec::new();
    layouts
        .try_reserve_exact(tx_count_usize)
        .map_err(|_| DirectoryError::new("cannot reserve adapted v2 transaction layouts"))?;
    let mut prior_ends = [0_u32; OBJECT_COUNT];
    let mut counted_signatures = 0_u64;
    let mut v3_reward_len = 0_u32;
    let mut canonical_reward_fields_elided = 0_u32;
    let mut canonical_reward_bytes_elided = 0_u32;
    let mut stored_reward_records = 0_u32;
    let mut raw_fallback_records = 0_u32;

    for (tx_index, row) in input
        .directory
        .chunks_exact(V2_DIRECTORY_ROW_LEN)
        .enumerate()
    {
        let source_flags = read_fixed_u16(row, 0);
        let effect_state = row[2];
        let signature_count = row[3];
        let mut ends = [0_u32; OBJECT_COUNT];
        let mut lengths = [0_u32; OBJECT_COUNT];
        for index in 0..OBJECT_COUNT {
            ends[index] = read_fixed_u32(row, 4 + index * 4);
            require(
                ends[index] >= prior_ends[index],
                format!("v2 object end decreases at transaction {tx_index}"),
            )?;
            require(
                ends[index] <= input.final_object_decoded_lengths[index],
                format!("v2 object end exceeds final length at transaction {tx_index}"),
            )?;
            lengths[index] = ends[index] - prior_ends[index];
        }
        let reward_bytes = v2_object_slice(
            input.transaction_rewards,
            prior_ends[7],
            ends[7],
            "transaction reward",
        )?;
        let raw_bytes = v2_object_slice(
            input.raw_metadata_fallbacks,
            prior_ends[8],
            ends[8],
            "raw metadata fallback",
        )?;
        let kind = metadata_kind(source_flags)?;
        let semantic_reward = effect_state & EFFECT_STATE_SEMANTIC_REWARDS != 0;
        let reward = match kind {
            MetadataKind::Missing | MetadataKind::RawFallback => {
                require(
                    reward_bytes.is_empty(),
                    format!("non-decoded transaction {tx_index} has reward bytes"),
                )?;
                TransactionReward::Absent
            }
            MetadataKind::Decoded if semantic_reward => {
                require(
                    !reward_bytes.is_empty() && reward_bytes != [0],
                    format!("semantic reward transaction {tx_index} has an empty Vec field"),
                )?;
                stored_reward_records = stored_reward_records
                    .checked_add(1)
                    .ok_or_else(|| DirectoryError::new("stored reward record count overflow"))?;
                v3_reward_len =
                    checked_add_u32(v3_reward_len, lengths[7], "adapted v3 reward object length")?;
                TransactionReward::SemanticStored(u64::from(lengths[7]))
            }
            MetadataKind::Decoded if reward_bytes == [0] => {
                canonical_reward_fields_elided = canonical_reward_fields_elided
                    .checked_add(1)
                    .ok_or_else(|| DirectoryError::new("canonical reward count overflow"))?;
                canonical_reward_bytes_elided = canonical_reward_bytes_elided
                    .checked_add(1)
                    .ok_or_else(|| DirectoryError::new("canonical reward bytes overflow"))?;
                TransactionReward::CanonicalEmpty
            }
            MetadataKind::Decoded => {
                require(
                    !reward_bytes.is_empty(),
                    format!("decoded transaction {tx_index} has no reward Vec field"),
                )?;
                stored_reward_records = stored_reward_records
                    .checked_add(1)
                    .ok_or_else(|| DirectoryError::new("stored reward record count overflow"))?;
                v3_reward_len =
                    checked_add_u32(v3_reward_len, lengths[7], "adapted v3 reward object length")?;
                TransactionReward::NoncanonicalEmptyStored(u64::from(lengths[7]))
            }
        };
        let raw_metadata_fallback_len = match kind {
            MetadataKind::RawFallback => {
                require(
                    !raw_bytes.is_empty(),
                    format!("raw metadata transaction {tx_index} has an empty fallback"),
                )?;
                raw_fallback_records = raw_fallback_records
                    .checked_add(1)
                    .ok_or_else(|| DirectoryError::new("raw fallback record count overflow"))?;
                Some(u64::from(lengths[8]))
            }
            MetadataKind::Missing | MetadataKind::Decoded => {
                require(
                    raw_bytes.is_empty(),
                    format!("non-raw transaction {tx_index} has raw fallback bytes"),
                )?;
                None
            }
        };
        let layout = TransactionLayout {
            source_flags,
            effect_state,
            signature_count,
            dense_lengths: array::from_fn(|index| u64::from(lengths[index])),
            reward,
            raw_metadata_fallback_len,
        };
        validate_layout(&layout)?;
        counted_signatures = counted_signatures
            .checked_add(u64::from(signature_count))
            .ok_or_else(|| DirectoryError::new("adapted signature count overflow"))?;
        layouts.push(layout);
        prior_ends = ends;
    }
    require(
        prior_ends == input.final_object_decoded_lengths,
        "v2 final directory ends differ from supplied object lengths",
    )?;
    require(
        counted_signatures == u64::from(input.signature_count),
        "v2 directory signature total differs from block",
    )?;
    require(
        input.final_object_decoded_lengths[7].checked_sub(canonical_reward_bytes_elided)
            == Some(v3_reward_len),
        "adapted v3 reward length accounting differs",
    )?;
    let mut v3_object_lengths = input.final_object_decoded_lengths;
    v3_object_lengths[7] = v3_reward_len;

    let mut measurements = Vec::with_capacity(SUPPORTED_STRIDES.len());
    let mut winning: Option<WinningDirectoryMeasurement> = None;
    let mut encoded_winner = None;
    for stride in SUPPORTED_STRIDES {
        let encoded = encode_with_stride(&layouts, stride)?;
        let measurement = encoded.measurement;
        measurements.push(measurement);
        let is_better = winning
            .as_ref()
            .is_none_or(|winner| measurement.sizes.total_bytes < winner.sizes.total_bytes);
        if is_better {
            winning = Some(WinningDirectoryMeasurement {
                stride,
                checkpoint_codec: measurement.selected_checkpoint_codec,
                worst_transaction_scan: measurement.worst_transaction_scan,
                sizes: measurement.sizes,
            });
            if options.include_encoded_winner {
                encoded_winner = Some(encoded);
            }
        }
    }
    let measurements: [StrideMeasurement; 3] = measurements
        .try_into()
        .map_err(|_| DirectoryError::new("internal stride measurement count differs"))?;
    let winner = winning.expect("three supported strides were measured");
    if let Some(encoded) = &encoded_winner {
        let decoded = DecodedDirectory::decode(&encoded.bytes)?;
        decoded.verify_external_totals(v3_object_lengths, counted_signatures)?;
    }
    Ok(V2BlockAdapterResult {
        tx_count,
        first_signature_ordinal: input.first_signature_ordinal,
        signature_count: input.signature_count,
        source_v2_object_lengths: input.final_object_decoded_lengths,
        v3_object_lengths,
        canonical_reward_fields_elided,
        canonical_reward_bytes_elided,
        stored_reward_records,
        raw_fallback_records,
        measurements,
        winner,
        encoded_winner,
    })
}

fn v2_object_slice<'a>(
    bytes: &'a [u8],
    start: u32,
    end: u32,
    name: &str,
) -> DirectoryResult<&'a [u8]> {
    require(start <= end, format!("v2 {name} range decreases"))?;
    let range = start as usize..end as usize;
    bytes
        .get(range)
        .ok_or_else(|| DirectoryError::new(format!("v2 {name} range exceeds chunk")))
}

pub fn encode_with_stride(
    transactions: &[TransactionLayout],
    stride: u16,
) -> DirectoryResult<EncodedDirectory> {
    encode_with_stride_mode(transactions, stride, false, false)
}

/// Encode one production v3 stride with canonical varint-delta checkpoints.
pub fn encode_with_stride_varint_delta(
    transactions: &[TransactionLayout],
    stride: u16,
) -> DirectoryResult<EncodedDirectory> {
    encode_with_stride_mode(transactions, stride, true, false)
}

#[cfg(test)]
pub(crate) fn encode_with_stride_fixed56_for_test(
    transactions: &[TransactionLayout],
    stride: u16,
) -> DirectoryResult<EncodedDirectory> {
    encode_with_stride_mode(transactions, stride, false, true)
}

fn encode_with_stride_mode(
    transactions: &[TransactionLayout],
    stride: u16,
    require_varint_delta: bool,
    require_fixed56: bool,
) -> DirectoryResult<EncodedDirectory> {
    require(
        !(require_varint_delta && require_fixed56),
        "checkpoint codec selection conflicts",
    )?;
    require(supported_stride(stride), "unsupported checkpoint stride")?;
    let tx_count = u32::try_from(transactions.len())
        .map_err(|_| DirectoryError::new("transaction count exceeds u32"))?;
    let groups = group_count(tx_count, stride);
    let mut dense = Vec::new();
    let mut rewards = Vec::new();
    let mut raw_fallbacks = Vec::new();
    let mut checkpoints = Vec::with_capacity(groups as usize + 1);
    let mut state = Checkpoint::default();
    checkpoints.push(state);

    for group in 0..groups {
        let first = group * u32::from(stride);
        let end = tx_count.min(first + u32::from(stride));
        let mut prior_reward_local = None;
        let mut prior_raw_local = None;
        for tx_index in first..end {
            let layout = &transactions[tx_index as usize];
            validate_layout(layout)?;
            let control = u32::from(layout.source_flags)
                | (u32::from(layout.effect_state) << 11)
                | (u32::from(layout.signature_count) << 19);
            put_uleb_u32(control, &mut dense);
            let mut lengths = [0_u32; DENSE_FIELD_COUNT];
            for (index, length) in layout.dense_lengths.into_iter().enumerate() {
                lengths[index] = as_u32(length, "dense field length")?;
                put_uleb_u32(lengths[index], &mut dense);
                state.object_ends[index] =
                    checked_add_u32(state.object_ends[index], lengths[index], "object end")?;
            }
            state.signature_prefix = state
                .signature_prefix
                .checked_add(u64::from(layout.signature_count))
                .ok_or_else(|| DirectoryError::new("signature prefix overflow"))?;
            let local = tx_index - first;
            if let Some(length) = layout.reward.stored_len() {
                let length = as_u32(length, "transaction reward length")?;
                put_sparse_record(local, length, &mut prior_reward_local, &mut rewards)?;
                state.object_ends[7] = checked_add_u32(
                    state.object_ends[7],
                    length,
                    "transaction reward object end",
                )?;
            }
            if let Some(length) = layout.raw_metadata_fallback_len {
                let length = as_u32(length, "raw metadata fallback length")?;
                put_sparse_record(local, length, &mut prior_raw_local, &mut raw_fallbacks)?;
                state.object_ends[8] =
                    checked_add_u32(state.object_ends[8], length, "raw metadata object end")?;
            }
        }
        state.dense_offset = u32::try_from(dense.len())
            .map_err(|_| DirectoryError::new("dense section exceeds u32"))?;
        state.reward_offset = u32::try_from(rewards.len())
            .map_err(|_| DirectoryError::new("reward section exceeds u32"))?;
        state.raw_offset = u32::try_from(raw_fallbacks.len())
            .map_err(|_| DirectoryError::new("raw fallback section exceeds u32"))?;
        checkpoints.push(state);
    }

    let mut fixed_checkpoint_bytes = Vec::new();
    for checkpoint in checkpoints.iter().copied() {
        checkpoint.encode_fixed(&mut fixed_checkpoint_bytes);
    }
    let expected_checkpoint_len = (u64::from(groups) + 1) * FIXED_CHECKPOINT_ROW_LEN as u64;
    require(
        fixed_checkpoint_bytes.len() as u64 == expected_checkpoint_len,
        "encoded fixed checkpoint geometry differs",
    )?;
    let mut varint_checkpoint_bytes = Vec::new();
    let mut prior = Checkpoint::default();
    // The initial zero checkpoint is implicit for the delta codec.
    for checkpoint in checkpoints.iter().copied().skip(1) {
        checkpoint.encode_delta(prior, &mut varint_checkpoint_bytes)?;
        prior = checkpoint;
    }

    let fixed = assemble_candidate(
        CheckpointCodec::Fixed56,
        stride,
        tx_count,
        groups,
        CandidateSections {
            checkpoints: &fixed_checkpoint_bytes,
            dense: &dense,
            rewards: &rewards,
            raw_fallbacks: &raw_fallbacks,
        },
    )?;
    let varint = assemble_candidate(
        CheckpointCodec::VarintDelta,
        stride,
        tx_count,
        groups,
        CandidateSections {
            checkpoints: &varint_checkpoint_bytes,
            dense: &dense,
            rewards: &rewards,
            raw_fallbacks: &raw_fallbacks,
        },
    )?;
    let fixed_measurement = CheckpointMeasurement {
        codec: CheckpointCodec::Fixed56,
        sizes: fixed.sizes,
    };
    let varint_measurement = CheckpointMeasurement {
        codec: CheckpointCodec::VarintDelta,
        sizes: varint.sizes,
    };
    // Prefer the varint form for an exact tie because it follows the compact
    // numeric wire and has no fixed-width checkpoint dependency.
    let selected = if require_fixed56 {
        fixed
    } else if require_varint_delta || varint.bytes.len() <= fixed.bytes.len() {
        varint
    } else {
        fixed
    };
    let selected_checkpoint_codec = selected.codec;
    let sizes = selected.sizes;
    Ok(EncodedDirectory {
        bytes: selected.bytes,
        measurement: StrideMeasurement {
            stride,
            worst_transaction_scan: geometry_for_tx_count(tx_count, stride)?.worst_transaction_scan,
            fixed56: fixed_measurement,
            varint_delta: varint_measurement,
            selected_checkpoint_codec,
            sizes,
        },
    })
}

#[derive(Debug)]
struct CandidateEncoding {
    codec: CheckpointCodec,
    bytes: Vec<u8>,
    sizes: SectionSizes,
}

#[derive(Clone, Copy)]
struct CandidateSections<'a> {
    checkpoints: &'a [u8],
    dense: &'a [u8],
    rewards: &'a [u8],
    raw_fallbacks: &'a [u8],
}

fn assemble_candidate(
    codec: CheckpointCodec,
    stride: u16,
    tx_count: u32,
    groups: u32,
    sections: CandidateSections<'_>,
) -> DirectoryResult<CandidateEncoding> {
    let data = [
        sections.checkpoints,
        sections.dense,
        sections.rewards,
        sections.raw_fallbacks,
    ];
    let mut lengths = [0_u32; SECTION_COUNT];
    for (index, section) in data.iter().enumerate() {
        lengths[index] = u32::try_from(section.len())
            .map_err(|_| DirectoryError::new("directory section exceeds u32"))?;
    }
    let (header, header_bytes) = encode_header(codec, stride, tx_count, groups, lengths)?;
    let total = header.sections[RAW_SECTION].end()?;
    let mut bytes = Vec::with_capacity(total as usize);
    bytes.extend_from_slice(&header_bytes);
    for section in data {
        bytes.extend_from_slice(section);
    }
    require(
        bytes.len() == total as usize,
        "encoded directory length differs",
    )?;
    Ok(CandidateEncoding {
        codec,
        bytes,
        sizes: header.section_lengths(),
    })
}

fn put_sparse_record(
    local: u32,
    length: u32,
    prior_local: &mut Option<u32>,
    output: &mut Vec<u8>,
) -> DirectoryResult<()> {
    require(length != 0, "sparse range length is zero")?;
    let gap = if let Some(prior) = *prior_local {
        local
            .checked_sub(prior)
            .ok_or_else(|| DirectoryError::new("sparse transaction order decreases"))?
    } else {
        local
            .checked_add(1)
            .ok_or_else(|| DirectoryError::new("sparse first gap overflow"))?
    };
    require(gap != 0, "sparse transaction index is duplicated")?;
    put_uleb_u32(gap, output);
    put_uleb_u32(length, output);
    *prior_local = Some(local);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectSlice {
    Absent,
    ImplicitCanonicalEmpty,
    Stored(Range<u32>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewardSlice {
    Absent,
    ImplicitCanonicalEmpty,
    SemanticStored(Range<u32>),
    NoncanonicalEmptyStored(Range<u32>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedTransaction {
    pub tx_index: u32,
    pub source_flags: u16,
    pub effect_state: u8,
    pub signature_count: u8,
    pub objects: [ObjectSlice; OBJECT_COUNT],
    pub reward: RewardSlice,
    pub relative_signature_ordinals: Range<u64>,
    pub absolute_signature_ordinals: Range<u64>,
    pub absolute_signature_bytes: Range<u64>,
    pub dense_records_scanned: u16,
    pub reward_records_scanned: u16,
    pub raw_records_scanned: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DenseRecord {
    source_flags: u16,
    effect_state: u8,
    signature_count: u8,
    lengths: [u32; DENSE_FIELD_COUNT],
}

#[derive(Debug)]
struct DecodedGroup {
    records: Vec<DenseRecord>,
    rewards: Vec<Option<u32>>,
    raw_fallbacks: Vec<Option<u32>>,
    reward_record_count: u16,
    raw_record_count: u16,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryScanCounters {
    pub dense_records: u64,
    pub reward_records: u64,
    pub raw_fallback_records: u64,
}

#[derive(Debug)]
pub struct DecodedDirectory<'a> {
    bytes: &'a [u8],
    pub header: DirectoryHeader,
    checkpoints: Vec<Checkpoint>,
    validation_scans: DirectoryScanCounters,
}

impl<'a> DecodedDirectory<'a> {
    /// Decode and strictly validate the full directory, including every final
    /// checkpoint total and each sparse lane sentinel.
    pub fn decode(bytes: &'a [u8]) -> DirectoryResult<Self> {
        let header = DirectoryHeader::decode(bytes)?;
        Self::decode_with_header(bytes, header)
    }

    /// Decode one production V3 directory.
    ///
    /// The fixed 56-byte checkpoint form is a measurement baseline only. A
    /// production standalone V3 reader must admit only canonical varint-delta
    /// checkpoints.
    pub fn decode_production(bytes: &'a [u8]) -> DirectoryResult<Self> {
        let header = DirectoryHeader::decode(bytes)?;
        require(
            header.checkpoint_codec == CheckpointCodec::VarintDelta,
            "production v3 directory requires varint-delta checkpoints",
        )?;
        Self::decode_with_header(bytes, header)
    }

    fn decode_with_header(bytes: &'a [u8], header: DirectoryHeader) -> DirectoryResult<Self> {
        let checkpoint_section = header.sections[CHECKPOINT_SECTION];
        let checkpoint_bytes = &bytes[checkpoint_section.range()];
        let checkpoint_count = header.group_count as usize + 1;
        let mut checkpoints = Vec::with_capacity(checkpoint_count);
        match header.checkpoint_codec {
            CheckpointCodec::Fixed56 => {
                for index in 0..checkpoint_count {
                    let start = index * FIXED_CHECKPOINT_ROW_LEN;
                    checkpoints.push(Checkpoint::decode_fixed(
                        &checkpoint_bytes[start..start + FIXED_CHECKPOINT_ROW_LEN],
                    )?);
                }
            }
            CheckpointCodec::VarintDelta => {
                let mut cursor = 0;
                let mut prior = Checkpoint::default();
                checkpoints.push(prior);
                for _ in 0..header.group_count {
                    let checkpoint = Checkpoint::decode_delta(
                        checkpoint_bytes,
                        &mut cursor,
                        checkpoint_bytes.len(),
                        prior,
                    )?;
                    checkpoints.push(checkpoint);
                    prior = checkpoint;
                }
                require(
                    cursor == checkpoint_bytes.len(),
                    "varint checkpoint section has trailing bytes",
                )?;
            }
        }
        require(
            checkpoints.first() == Some(&Checkpoint::default()),
            "first checkpoint is not zero",
        )?;
        let dense_len = header.sections[DENSE_SECTION].len;
        let reward_len = header.sections[REWARD_SECTION].len;
        let raw_len = header.sections[RAW_SECTION].len;
        for pair in checkpoints.windows(2) {
            let prior = pair[0];
            let next = pair[1];
            require(
                next.dense_offset >= prior.dense_offset,
                "checkpoint dense offsets decrease",
            )?;
            require(
                next.reward_offset >= prior.reward_offset,
                "checkpoint reward offsets decrease",
            )?;
            require(
                next.raw_offset >= prior.raw_offset,
                "checkpoint raw offsets decrease",
            )?;
            require(
                next.signature_prefix >= prior.signature_prefix,
                "checkpoint signature prefixes decrease",
            )?;
            require(
                next.object_ends
                    .iter()
                    .zip(prior.object_ends)
                    .all(|(next, prior)| *next >= prior),
                "checkpoint object ends decrease",
            )?;
        }
        let final_checkpoint = *checkpoints
            .last()
            .expect("a directory always has its zero checkpoint");
        require(
            final_checkpoint.dense_offset == dense_len,
            "final dense checkpoint differs from section length",
        )?;
        require(
            final_checkpoint.reward_offset == reward_len,
            "final reward checkpoint differs from section length",
        )?;
        require(
            final_checkpoint.raw_offset == raw_len,
            "final raw checkpoint differs from section length",
        )?;
        let mut decoded = Self {
            bytes,
            header,
            checkpoints,
            validation_scans: DirectoryScanCounters::default(),
        };
        for group in 0..decoded.header.group_count {
            let decoded_group = decoded.decode_group(group)?;
            decoded.validation_scans.dense_records = decoded
                .validation_scans
                .dense_records
                .checked_add(decoded_group.records.len() as u64)
                .ok_or_else(|| DirectoryError::new("validation dense scan count overflow"))?;
            decoded.validation_scans.reward_records = decoded
                .validation_scans
                .reward_records
                .checked_add(u64::from(decoded_group.reward_record_count))
                .ok_or_else(|| DirectoryError::new("validation reward scan count overflow"))?;
            decoded.validation_scans.raw_fallback_records = decoded
                .validation_scans
                .raw_fallback_records
                .checked_add(u64::from(decoded_group.raw_record_count))
                .ok_or_else(|| DirectoryError::new("validation raw scan count overflow"))?;
        }
        Ok(decoded)
    }

    pub const fn validation_scan_counters(&self) -> DirectoryScanCounters {
        self.validation_scans
    }

    pub fn object_lengths(&self) -> [u32; OBJECT_COUNT] {
        self.checkpoints
            .last()
            .expect("validated final checkpoint")
            .object_ends
    }

    pub fn signature_count(&self) -> u64 {
        self.checkpoints
            .last()
            .expect("validated final checkpoint")
            .signature_prefix
    }

    pub fn verify_external_totals(
        &self,
        object_lengths: [u32; OBJECT_COUNT],
        signature_count: u64,
    ) -> DirectoryResult<()> {
        require(
            self.object_lengths() == object_lengths,
            "directory object totals differ from external chunks",
        )?;
        require(
            self.signature_count() == signature_count,
            "directory signature total differs from block",
        )
    }

    pub fn lookup(
        &self,
        tx_index: u32,
        first_signature_ordinal: u64,
    ) -> DirectoryResult<SelectedTransaction> {
        require(
            tx_index < self.header.tx_count,
            "transaction index is outside block",
        )?;
        let stride = u32::from(self.header.stride);
        let group_index = tx_index / stride;
        let local_index = (tx_index % stride) as usize;
        let group = self.decode_group(group_index)?;
        let checkpoint = self.checkpoints[group_index as usize];
        let mut object_ends = checkpoint.object_ends;
        let mut signature_prefix = checkpoint.signature_prefix;

        for (local, record) in group.records.iter().copied().enumerate() {
            let starts = object_ends;
            for (index, length) in record.lengths.into_iter().enumerate() {
                object_ends[index] =
                    checked_add_u32(object_ends[index], length, "lookup object end")?;
            }
            if let Some(length) = group.rewards[local] {
                object_ends[7] =
                    checked_add_u32(object_ends[7], length, "lookup reward object end")?;
            }
            if let Some(length) = group.raw_fallbacks[local] {
                object_ends[8] = checked_add_u32(object_ends[8], length, "lookup raw object end")?;
            }
            let relative_start = signature_prefix;
            signature_prefix = signature_prefix
                .checked_add(u64::from(record.signature_count))
                .ok_or_else(|| DirectoryError::new("lookup signature prefix overflow"))?;
            if local == local_index {
                let kind = metadata_kind(record.source_flags)?;
                let mut objects = array::from_fn(|_| ObjectSlice::Absent);
                objects[0] = ObjectSlice::Stored(starts[0]..object_ends[0]);
                if kind == MetadataKind::Decoded {
                    for index in 1..DENSE_FIELD_COUNT {
                        objects[index] = ObjectSlice::Stored(starts[index]..object_ends[index]);
                    }
                }
                let reward = match (kind, group.rewards[local]) {
                    (MetadataKind::Decoded, Some(_))
                        if record.effect_state & EFFECT_STATE_SEMANTIC_REWARDS != 0 =>
                    {
                        objects[7] = ObjectSlice::Stored(starts[7]..object_ends[7]);
                        RewardSlice::SemanticStored(starts[7]..object_ends[7])
                    }
                    (MetadataKind::Decoded, Some(_)) => {
                        objects[7] = ObjectSlice::Stored(starts[7]..object_ends[7]);
                        RewardSlice::NoncanonicalEmptyStored(starts[7]..object_ends[7])
                    }
                    (MetadataKind::Decoded, None) => {
                        objects[7] = ObjectSlice::ImplicitCanonicalEmpty;
                        RewardSlice::ImplicitCanonicalEmpty
                    }
                    (MetadataKind::Missing | MetadataKind::RawFallback, None) => {
                        RewardSlice::Absent
                    }
                    _ => return Err(DirectoryError::new("invalid reward sparse semantics")),
                };
                if group.raw_fallbacks[local].is_some() {
                    objects[8] = ObjectSlice::Stored(starts[8]..object_ends[8]);
                }
                let absolute_start = first_signature_ordinal
                    .checked_add(relative_start)
                    .ok_or_else(|| DirectoryError::new("absolute signature start overflow"))?;
                let absolute_end = first_signature_ordinal
                    .checked_add(signature_prefix)
                    .ok_or_else(|| DirectoryError::new("absolute signature end overflow"))?;
                let byte_start = absolute_start
                    .checked_mul(SIGNATURE_BYTES)
                    .ok_or_else(|| DirectoryError::new("signature byte start overflow"))?;
                let byte_end = absolute_end
                    .checked_mul(SIGNATURE_BYTES)
                    .ok_or_else(|| DirectoryError::new("signature byte end overflow"))?;
                return Ok(SelectedTransaction {
                    tx_index,
                    source_flags: record.source_flags,
                    effect_state: record.effect_state,
                    signature_count: record.signature_count,
                    objects,
                    reward,
                    relative_signature_ordinals: relative_start..signature_prefix,
                    absolute_signature_ordinals: absolute_start..absolute_end,
                    absolute_signature_bytes: byte_start..byte_end,
                    dense_records_scanned: u16::try_from(local + 1)
                        .expect("group length is at most 128"),
                    reward_records_scanned: group.reward_record_count,
                    raw_records_scanned: group.raw_record_count,
                });
            }
        }
        Err(DirectoryError::new(
            "validated group did not contain selected transaction",
        ))
    }

    fn decode_group(&self, group_index: u32) -> DirectoryResult<DecodedGroup> {
        require(
            group_index < self.header.group_count,
            "group index is outside directory",
        )?;
        let checkpoint = self.checkpoints[group_index as usize];
        let next = self.checkpoints[group_index as usize + 1];
        let first_tx = group_index * u32::from(self.header.stride);
        let tx_in_group =
            (self.header.tx_count - first_tx).min(u32::from(self.header.stride)) as usize;
        let (rewards, reward_record_count) = self.parse_sparse_group(
            REWARD_SECTION,
            checkpoint.reward_offset,
            next.reward_offset,
            tx_in_group,
        )?;
        let (raw_fallbacks, raw_record_count) = self.parse_sparse_group(
            RAW_SECTION,
            checkpoint.raw_offset,
            next.raw_offset,
            tx_in_group,
        )?;
        let dense_section = self.header.sections[DENSE_SECTION];
        let dense_bytes = &self.bytes[dense_section.range()];
        let mut cursor = checkpoint.dense_offset as usize;
        let limit = next.dense_offset as usize;
        require(
            limit <= dense_bytes.len(),
            "checkpoint dense offset exceeds section",
        )?;
        let mut records = Vec::with_capacity(tx_in_group);
        for _ in 0..tx_in_group {
            let control = read_uleb_u32(dense_bytes, &mut cursor, limit)?;
            require(
                control < (1 << 27),
                "packed transaction control has high bits",
            )?;
            let source_flags = (control & u32::from(SOURCE_FLAG_MASK)) as u16;
            let effect_state = ((control >> 11) & 0xff) as u8;
            let signature_count = ((control >> 19) & 0xff) as u8;
            let mut lengths = [0_u32; DENSE_FIELD_COUNT];
            for length in &mut lengths {
                *length = read_uleb_u32(dense_bytes, &mut cursor, limit)?;
            }
            records.push(DenseRecord {
                source_flags,
                effect_state,
                signature_count,
                lengths,
            });
        }
        require(
            cursor == limit,
            "dense checkpoint does not end on a record boundary",
        )?;

        let mut object_ends = checkpoint.object_ends;
        let mut signature_prefix = checkpoint.signature_prefix;
        for local in 0..tx_in_group {
            let record = records[local];
            validate_wire_record(&record, rewards[local], raw_fallbacks[local])?;
            for (index, length) in record.lengths.into_iter().enumerate() {
                object_ends[index] =
                    checked_add_u32(object_ends[index], length, "decoded object end")?;
            }
            if let Some(length) = rewards[local] {
                object_ends[7] =
                    checked_add_u32(object_ends[7], length, "decoded reward object end")?;
            }
            if let Some(length) = raw_fallbacks[local] {
                object_ends[8] = checked_add_u32(object_ends[8], length, "decoded raw object end")?;
            }
            signature_prefix = signature_prefix
                .checked_add(u64::from(record.signature_count))
                .ok_or_else(|| DirectoryError::new("decoded signature prefix overflow"))?;
        }
        require(
            object_ends == next.object_ends,
            "checkpoint object totals differ from decoded records",
        )?;
        require(
            signature_prefix == next.signature_prefix,
            "checkpoint signature prefix differs from decoded records",
        )?;
        Ok(DecodedGroup {
            records,
            rewards,
            raw_fallbacks,
            reward_record_count,
            raw_record_count,
        })
    }

    fn parse_sparse_group(
        &self,
        section_index: usize,
        start: u32,
        end: u32,
        tx_in_group: usize,
    ) -> DirectoryResult<(Vec<Option<u32>>, u16)> {
        require(start <= end, "sparse checkpoint offsets decrease")?;
        let section = self.header.sections[section_index];
        require(
            end <= section.len,
            "sparse checkpoint offset exceeds section",
        )?;
        let bytes = &self.bytes[section.range()];
        let mut cursor = start as usize;
        let limit = end as usize;
        let mut lengths = vec![None; tx_in_group];
        let mut prior_local: Option<u32> = None;
        let mut count = 0_u16;
        while cursor < limit {
            let gap = read_uleb_u32(bytes, &mut cursor, limit)?;
            require(gap != 0, "sparse transaction gap is zero")?;
            let local = if let Some(prior) = prior_local {
                prior
                    .checked_add(gap)
                    .ok_or_else(|| DirectoryError::new("sparse transaction index overflow"))?
            } else {
                gap - 1
            };
            require(
                local < tx_in_group as u32,
                "sparse transaction index is outside checkpoint group",
            )?;
            let length = read_uleb_u32(bytes, &mut cursor, limit)?;
            require(length != 0, "sparse object length is zero")?;
            require(
                lengths[local as usize].is_none(),
                "sparse transaction index is duplicated",
            )?;
            lengths[local as usize] = Some(length);
            prior_local = Some(local);
            count = count
                .checked_add(1)
                .ok_or_else(|| DirectoryError::new("sparse record count overflow"))?;
        }
        require(cursor == limit, "sparse checkpoint splits a record")?;
        Ok((lengths, count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn missing(message_len: u64, signatures: u8) -> TransactionLayout {
        TransactionLayout {
            source_flags: 0,
            effect_state: 0,
            signature_count: signatures,
            dense_lengths: [message_len, 0, 0, 0, 0, 0, 0],
            reward: TransactionReward::Absent,
            raw_metadata_fallback_len: None,
        }
    }

    fn raw(message_len: u64, signatures: u8, raw_len: u64) -> TransactionLayout {
        TransactionLayout {
            source_flags: SOURCE_FLAG_HAS_METADATA | SOURCE_FLAG_METADATA_RAW_FALLBACK,
            effect_state: 0,
            signature_count: signatures,
            dense_lengths: [message_len, 0, 0, 0, 0, 0, 0],
            reward: TransactionReward::Absent,
            raw_metadata_fallback_len: Some(raw_len),
        }
    }

    fn decoded(
        message_len: u64,
        signatures: u8,
        semantic_reward_len: Option<u64>,
    ) -> TransactionLayout {
        TransactionLayout {
            source_flags: SOURCE_FLAG_HAS_METADATA,
            effect_state: 0b0001_1001
                | if semantic_reward_len.is_some() {
                    EFFECT_STATE_SEMANTIC_REWARDS
                } else {
                    0
                },
            signature_count: signatures,
            dense_lengths: [message_len, 1, 2, 3, 4, 5, 6],
            reward: semantic_reward_len.map_or(
                TransactionReward::CanonicalEmpty,
                TransactionReward::SemanticStored,
            ),
            raw_metadata_fallback_len: None,
        }
    }

    #[derive(Default)]
    struct AdapterFixture {
        directory: Vec<u8>,
        rewards: Vec<u8>,
        raw_fallbacks: Vec<u8>,
        ends: [u32; OBJECT_COUNT],
        layouts: Vec<TransactionLayout>,
    }

    fn append_v2_fixture(
        fixture: &mut AdapterFixture,
        layout: TransactionLayout,
        reward_bytes: &[u8],
        raw_bytes: &[u8],
    ) {
        for (index, length) in layout.dense_lengths.into_iter().enumerate() {
            fixture.ends[index] += u32::try_from(length).unwrap();
        }
        fixture.ends[7] += u32::try_from(reward_bytes.len()).unwrap();
        fixture.ends[8] += u32::try_from(raw_bytes.len()).unwrap();
        fixture
            .directory
            .extend_from_slice(&layout.source_flags.to_le_bytes());
        fixture.directory.push(layout.effect_state);
        fixture.directory.push(layout.signature_count);
        for end in fixture.ends {
            fixture.directory.extend_from_slice(&end.to_le_bytes());
        }
        fixture.rewards.extend_from_slice(reward_bytes);
        fixture.raw_fallbacks.extend_from_slice(raw_bytes);
        fixture.layouts.push(layout);
    }

    fn adapter_fixture() -> AdapterFixture {
        let mut fixture = AdapterFixture::default();
        append_v2_fixture(&mut fixture, missing(2, 1), &[], &[]);
        append_v2_fixture(&mut fixture, decoded(3, 2, None), &[0], &[]);
        let mut noncanonical = decoded(4, 1, None);
        noncanonical.reward = TransactionReward::NoncanonicalEmptyStored(2);
        append_v2_fixture(&mut fixture, noncanonical, &[0x80, 0], &[]);
        append_v2_fixture(&mut fixture, decoded(5, 3, Some(3)), &[1, 0xaa, 0xbb], &[]);
        append_v2_fixture(&mut fixture, raw(6, 1, 2), &[], &[0xcc, 0xdd]);
        fixture
    }

    fn adapter_input(fixture: &AdapterFixture) -> V2BlockAdapterInput<'_> {
        V2BlockAdapterInput {
            directory: &fixture.directory,
            transaction_rewards: &fixture.rewards,
            raw_metadata_fallbacks: &fixture.raw_fallbacks,
            first_signature_ordinal: 100,
            signature_count: 8,
            final_object_decoded_lengths: fixture.ends,
        }
    }

    #[test]
    fn deterministic_encoding_and_stride_tie_choose_smaller_stride() {
        let layouts = vec![missing(1, 1); 3];
        let first = encode_best(&layouts).unwrap();
        let second = encode_best(&layouts).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.encoded.measurement.stride, 32);
        assert_eq!(
            first.encoded.measurement.selected_checkpoint_codec,
            CheckpointCodec::VarintDelta
        );
        assert_eq!(first.measurements[0].sizes.total_bytes, 61);
        assert_eq!(first.measurements[0].sizes, first.measurements[1].sizes);
        assert_eq!(first.measurements[2].sizes.total_bytes, 62);
        DecodedDirectory::decode(&first.encoded.bytes).unwrap();
    }

    #[test]
    fn production_best_is_varint_only_and_keeps_fixed_baseline_measurement() {
        let layouts = vec![missing(1, 1); 65];
        let first = encode_best_varint_delta(&layouts).unwrap();
        let second = encode_best_varint_delta(&layouts).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.encoded.measurement.selected_checkpoint_codec,
            CheckpointCodec::VarintDelta
        );
        assert!(first.measurements.iter().all(|measurement| {
            measurement.selected_checkpoint_codec == CheckpointCodec::VarintDelta
                && measurement.fixed56.sizes.total_bytes != 0
                && measurement.varint_delta.sizes.total_bytes == measurement.sizes.total_bytes
        }));
        assert_eq!(
            DecodedDirectory::decode(&first.encoded.bytes)
                .unwrap()
                .header
                .checkpoint_codec,
            CheckpointCodec::VarintDelta
        );
    }

    #[test]
    fn first_last_and_checkpoint_transactions_keep_exact_ranges_and_signatures() {
        let mut layouts = vec![missing(1, 1); 129];
        layouts[0] = raw(3, 2, 4);
        layouts[64] = decoded(5, 3, Some(7));
        layouts[128] = decoded(9, 4, None);
        let encoded = encode_with_stride(&layouts, 64).unwrap();
        let decoded = DecodedDirectory::decode(&encoded.bytes).unwrap();

        let first = decoded.lookup(0, 10).unwrap();
        assert_eq!(first.objects[0], ObjectSlice::Stored(0..3));
        assert_eq!(first.objects[8], ObjectSlice::Stored(0..4));
        assert_eq!(first.reward, RewardSlice::Absent);
        assert_eq!(first.relative_signature_ordinals, 0..2);
        assert_eq!(first.absolute_signature_ordinals, 10..12);
        assert_eq!(first.absolute_signature_bytes, 640..768);

        let boundary = decoded.lookup(64, 10).unwrap();
        assert_eq!(boundary.objects[0], ObjectSlice::Stored(66..71));
        assert_eq!(boundary.objects[1], ObjectSlice::Stored(0..1));
        assert_eq!(boundary.objects[7], ObjectSlice::Stored(0..7));
        assert_eq!(boundary.reward, RewardSlice::SemanticStored(0..7));
        assert_eq!(boundary.relative_signature_ordinals, 65..68);
        assert_eq!(boundary.absolute_signature_ordinals, 75..78);
        assert_eq!(boundary.dense_records_scanned, 1);

        let suffix = decoded.lookup(127, 10).unwrap();
        assert_eq!(suffix.dense_records_scanned, 64);

        let last = decoded.lookup(128, 10).unwrap();
        assert_eq!(last.objects[0], ObjectSlice::Stored(134..143));
        assert_eq!(last.objects[7], ObjectSlice::ImplicitCanonicalEmpty);
        assert_eq!(last.reward, RewardSlice::ImplicitCanonicalEmpty);
        assert_eq!(last.relative_signature_ordinals, 131..135);
        assert_eq!(last.absolute_signature_ordinals, 141..145);
        assert_eq!(last.dense_records_scanned, 1);
        decoded
            .verify_external_totals(decoded.object_lengths(), decoded.signature_count())
            .unwrap();
        let mut wrong = decoded.object_lengths();
        wrong[0] += 1;
        assert!(
            decoded
                .verify_external_totals(wrong, decoded.signature_count())
                .is_err()
        );
    }

    #[test]
    fn all_strides_have_exact_size_fixture_and_beat_v2_rows() {
        let layouts = vec![missing(1, 1); 129];
        let best = encode_best(&layouts).unwrap();
        let totals: Vec<u64> = best
            .measurements
            .iter()
            .map(|measurement| measurement.sizes.total_bytes)
            .collect();
        assert_eq!(totals, vec![1_379, 1_351, 1_340]);
        assert_eq!(
            best.measurements
                .iter()
                .map(|measurement| measurement.fixed56.sizes.total_bytes)
                .collect::<Vec<_>>(),
            vec![1_647, 1_535, 1_480]
        );
        assert!(best.measurements.iter().all(|measurement| {
            measurement.varint_delta.sizes.total_bytes < measurement.fixed56.sizes.total_bytes
                && measurement.selected_checkpoint_codec == CheckpointCodec::VarintDelta
        }));
        assert_eq!(
            best.measurements
                .iter()
                .map(|measurement| measurement.worst_transaction_scan)
                .collect::<Vec<_>>(),
            vec![32, 64, 128]
        );
        assert_eq!(best.encoded.measurement.stride, 128);
        assert!(best.encoded.bytes.len() < layouts.len() * 40);
    }

    #[test]
    fn fixed56_baseline_is_measured_and_decodes_with_the_same_totals() {
        let encoded = encode_with_stride(&[raw(2, 2, 3), decoded(4, 1, Some(5))], 32).unwrap();
        let selected = DecodedDirectory::decode(&encoded.bytes).unwrap();
        assert_eq!(
            selected.header.checkpoint_codec,
            CheckpointCodec::VarintDelta
        );
        let mut fixed_checkpoints = Vec::new();
        for checkpoint in selected.checkpoints.iter().copied() {
            checkpoint.encode_fixed(&mut fixed_checkpoints);
        }
        let section = |index: usize| {
            let range = selected.header.sections[index].range();
            &encoded.bytes[range]
        };
        let fixed = assemble_candidate(
            CheckpointCodec::Fixed56,
            selected.header.stride,
            selected.header.tx_count,
            selected.header.group_count,
            CandidateSections {
                checkpoints: &fixed_checkpoints,
                dense: section(DENSE_SECTION),
                rewards: section(REWARD_SECTION),
                raw_fallbacks: section(RAW_SECTION),
            },
        )
        .unwrap();
        assert_eq!(
            fixed.bytes.len() as u64,
            encoded.measurement.fixed56.sizes.total_bytes
        );
        let fixed_decoded = DecodedDirectory::decode(&fixed.bytes).unwrap();
        assert_eq!(fixed_decoded.object_lengths(), selected.object_lengths());
        assert_eq!(fixed_decoded.signature_count(), selected.signature_count());
        assert!(DecodedDirectory::decode_production(&fixed.bytes).is_err());
        assert!(DecodedDirectory::decode_production(&encoded.bytes).is_ok());
    }

    #[test]
    fn v2_adapter_reconstructs_sparse_semantics_sizes_and_winner() {
        let fixture = adapter_fixture();
        let result = measure_v2_block_directory(
            adapter_input(&fixture),
            V2BlockAdapterOptions {
                include_encoded_winner: true,
            },
        )
        .unwrap();
        let expected = encode_best(&fixture.layouts).unwrap();
        assert_eq!(result.tx_count, 5);
        assert_eq!(result.signature_count, 8);
        assert_eq!(result.source_v2_object_lengths, fixture.ends);
        assert_eq!(result.source_v2_object_lengths[7], 6);
        assert_eq!(result.v3_object_lengths[7], 5);
        assert_eq!(result.canonical_reward_fields_elided, 1);
        assert_eq!(result.canonical_reward_bytes_elided, 1);
        assert_eq!(result.stored_reward_records, 2);
        assert_eq!(result.raw_fallback_records, 1);
        assert_eq!(result.measurements, expected.measurements);
        assert_eq!(result.winner.stride, expected.encoded.measurement.stride);
        assert_eq!(
            result.winner.checkpoint_codec,
            expected.encoded.measurement.selected_checkpoint_codec
        );
        let encoded = result.encoded_winner.unwrap();
        assert_eq!(encoded.bytes, expected.encoded.bytes);
        let decoded = DecodedDirectory::decode(&encoded.bytes).unwrap();
        decoded
            .verify_external_totals(result.v3_object_lengths, 8)
            .unwrap();
        assert_eq!(
            decoded.lookup(1, 100).unwrap().reward,
            RewardSlice::ImplicitCanonicalEmpty
        );
        assert_eq!(
            decoded.lookup(2, 100).unwrap().reward,
            RewardSlice::NoncanonicalEmptyStored(0..2)
        );
        assert_eq!(
            decoded.lookup(3, 100).unwrap().reward,
            RewardSlice::SemanticStored(2..5)
        );
        let last = decoded.lookup(4, 100).unwrap();
        assert_eq!(last.objects[8], ObjectSlice::Stored(0..2));
        assert_eq!(last.absolute_signature_ordinals, 107..108);

        let size_only =
            measure_v2_block_directory(adapter_input(&fixture), V2BlockAdapterOptions::default())
                .unwrap();
        assert!(size_only.encoded_winner.is_none());
        assert_eq!(size_only.measurements, result.measurements);
        assert_eq!(size_only.winner, result.winner);
    }

    #[test]
    fn v2_adapter_rejects_bounds_totals_and_sparse_semantic_corruption() {
        let fixture = adapter_fixture();
        let base = adapter_input(&fixture);

        let mut input = base;
        input.directory = &fixture.directory[..fixture.directory.len() - 1];
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut input = base;
        input.transaction_rewards = &fixture.rewards[..fixture.rewards.len() - 1];
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut input = base;
        input.final_object_decoded_lengths[0] += 1;
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut input = base;
        input.signature_count -= 1;
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut input = base;
        input.first_signature_ordinal = u64::MAX;
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut decreasing = fixture.directory.clone();
        let second_message_end = V2_DIRECTORY_ROW_LEN + 4;
        decreasing[second_message_end..second_message_end + 4]
            .copy_from_slice(&1_u32.to_le_bytes());
        let mut input = base;
        input.directory = &decreasing;
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut semantic_empty = AdapterFixture::default();
        append_v2_fixture(&mut semantic_empty, decoded(1, 1, Some(1)), &[0], &[]);
        let input = V2BlockAdapterInput {
            directory: &semantic_empty.directory,
            transaction_rewards: &semantic_empty.rewards,
            raw_metadata_fallbacks: &semantic_empty.raw_fallbacks,
            first_signature_ordinal: 0,
            signature_count: 1,
            final_object_decoded_lengths: semantic_empty.ends,
        };
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut missing_with_reward = AdapterFixture::default();
        append_v2_fixture(&mut missing_with_reward, missing(1, 1), &[0], &[]);
        let input = V2BlockAdapterInput {
            directory: &missing_with_reward.directory,
            transaction_rewards: &missing_with_reward.rewards,
            raw_metadata_fallbacks: &missing_with_reward.raw_fallbacks,
            first_signature_ordinal: 0,
            signature_count: 1,
            final_object_decoded_lengths: missing_with_reward.ends,
        };
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());

        let mut raw_without_bytes = AdapterFixture::default();
        append_v2_fixture(&mut raw_without_bytes, raw(1, 1, 1), &[], &[]);
        let input = V2BlockAdapterInput {
            directory: &raw_without_bytes.directory,
            transaction_rewards: &raw_without_bytes.rewards,
            raw_metadata_fallbacks: &raw_without_bytes.raw_fallbacks,
            first_signature_ordinal: 0,
            signature_count: 1,
            final_object_decoded_lengths: raw_without_bytes.ends,
        };
        assert!(measure_v2_block_directory(input, V2BlockAdapterOptions::default()).is_err());
    }

    #[test]
    fn strict_decoder_rejects_header_checkpoint_varint_and_sparse_corruption() {
        let encoded = encode_with_stride(&[missing(1, 1)], 32).unwrap();

        let mut corrupt = encoded.bytes.clone();
        corrupt[0] ^= 1;
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let mut corrupt = encoded.bytes.clone();
        corrupt[FIXED_PREFIX_LEN] = 2;
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let mut corrupt = encoded.bytes.clone();
        // Make the one-byte codec value an overlong two-byte ULEB.
        corrupt[FIXED_PREFIX_LEN] = 0x81;
        corrupt.insert(FIXED_PREFIX_LEN + 1, 0);
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let parsed = DirectoryHeader::decode(&encoded.bytes).unwrap();
        let mut corrupt = encoded.bytes.clone();
        let dense_offset = parsed.sections[DENSE_SECTION].offset as usize;
        // Control is three bytes. Extend the message-length value into the
        // following zero to make an overlong encoding of one.
        corrupt[dense_offset + 3] = 0x81;
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let mut corrupt = encoded.bytes.clone();
        let checkpoint_offset = parsed.sections[CHECKPOINT_SECTION].offset as usize;
        // The third delta is the message object end. It must agree with the
        // dense message length.
        corrupt[checkpoint_offset + 2] = 2;
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let mut corrupt = encoded.bytes.clone();
        // Increase the checkpoint section length by one, then insert an
        // overlong zero terminator into the first delta.
        corrupt[14] = 14;
        corrupt[checkpoint_offset] = 0x8a;
        corrupt.insert(checkpoint_offset + 1, 0);
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let raw_encoded = encode_with_stride(&[raw(1, 1, 3)], 32).unwrap();
        let raw_header = DirectoryHeader::decode(&raw_encoded.bytes).unwrap();
        let mut corrupt = raw_encoded.bytes.clone();
        let raw_offset = raw_header.sections[RAW_SECTION].offset as usize;
        corrupt[raw_offset] = 2;
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let two_raw = encode_with_stride(&[raw(1, 1, 2), raw(1, 1, 2)], 32).unwrap();
        let two_raw_header = DirectoryHeader::decode(&two_raw.bytes).unwrap();
        let mut corrupt = two_raw.bytes.clone();
        let raw_offset = two_raw_header.sections[RAW_SECTION].offset as usize;
        corrupt[raw_offset + 2] = 0;
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let mut corrupt = encoded.bytes.clone();
        corrupt.extend_from_slice(&[0]);
        assert!(DecodedDirectory::decode(&corrupt).is_err());

        let mut corrupt = encoded.bytes.clone();
        // For this one-transaction fixture, byte 15 is the one-byte dense
        // section length. It is part of the canonical varint header.
        corrupt[15] += 1;
        assert!(DecodedDirectory::decode(&corrupt).is_err());
    }

    #[test]
    fn overflow_and_maximum_count_geometry_fail_cleanly() {
        let mut too_large_field = missing(u64::from(u32::MAX) + 1, 1);
        assert!(encode_with_stride(&[too_large_field.clone()], 32).is_err());
        too_large_field.dense_lengths[0] = u64::from(u32::MAX);
        assert!(encode_with_stride(&[too_large_field, missing(1, 1)], 32).is_err());

        let encoded = encode_with_stride(&[missing(1, 1)], 32).unwrap();
        let decoded = DecodedDirectory::decode(&encoded.bytes).unwrap();
        assert!(decoded.lookup(0, u64::MAX).is_err());

        for stride in SUPPORTED_STRIDES {
            let geometry = geometry_for_tx_count(u32::MAX, stride).unwrap();
            assert_eq!(geometry.tx_count, u32::MAX);
            assert!(!geometry.minimum_fits_u32_block_chunk);
            assert!(geometry.minimum_encoded_bytes > u64::from(u32::MAX));
        }
    }

    #[test]
    fn transaction_count_is_not_limited_to_three_thousand() {
        let layouts = vec![missing(1, 1); 4_097];
        let encoded = encode_with_stride(&layouts, 128).unwrap();
        let decoded = DecodedDirectory::decode(&encoded.bytes).unwrap();
        let last = decoded.lookup(4_096, 0).unwrap();
        assert_eq!(last.tx_index, 4_096);
        assert_eq!(last.relative_signature_ordinals, 4_096..4_097);
        assert_eq!(decoded.header.tx_count, 4_097);
    }

    #[test]
    fn exact_noncanonical_empty_reward_semantics_survive_lookup() {
        let mut layout = decoded(2, 1, None);
        layout.reward = TransactionReward::NoncanonicalEmptyStored(3);
        let encoded = encode_with_stride(&[layout], 32).unwrap();
        let decoded = DecodedDirectory::decode(&encoded.bytes).unwrap();
        let selected = decoded.lookup(0, 0).unwrap();
        assert_eq!(selected.reward, RewardSlice::NoncanonicalEmptyStored(0..3));
        assert_eq!(selected.objects[7], ObjectSlice::Stored(0..3));
    }
}
