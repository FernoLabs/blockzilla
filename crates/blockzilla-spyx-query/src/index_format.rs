use anyhow::{Context, Result, bail, ensure};
use blockzilla_token_transaction_dump::{DumpWireProfile, consolidated_reader::FrameLocator};
use serde::{Deserialize, Serialize};

pub const INDEX_SCHEMA_VERSION: u16 = 1;
pub const INDEX_MANIFEST_FILE: &str = "index-manifest.json";
pub const LOCATORS_FILE: &str = "locators.bin";
pub const SIGNATURE_LOOKUP_FILE: &str = "signature-lookup.bin";
pub const INDEX_HEADER_BYTES: usize = 128;
pub const LOCATOR_RECORD_BYTES: usize = 80;
pub const SIGNATURE_RECORD_BYTES: usize = 80;
pub const INDEX_FLAG_COMPLETE: u16 = 1;
pub const LOCATOR_MAGIC: [u8; 8] = *b"BZSLOC01";
pub const SIGNATURE_MAGIC: [u8; 8] = *b"BZSSIG01";

pub(crate) const BLOCK_TIME_NONE: i64 = i64::MIN;
pub(crate) const BLOCK_HEIGHT_NONE: u64 = u64::MAX;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexManifest {
    pub schema_version: u16,
    pub artifact_kind: String,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_max_transactions: Option<u64>,
    pub transactions: u64,
    pub signature_occurrences: u64,
    pub created_unix_seconds: u64,
    pub source: SourceIndexBinding,
    pub locators: IndexFileBinding,
    pub signature_lookup: IndexFileBinding,
}

impl IndexManifest {
    pub const ARTIFACT_KIND: &'static str = "blockzilla_spyx_query_index";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIndexBinding {
    pub manifest_sha256: String,
    pub transaction_file: String,
    pub transaction_bytes: u64,
    pub transaction_sha256: String,
    pub signature_file: String,
    pub signature_bytes: u64,
    pub signature_sha256: String,
    pub registry_file: String,
    pub registry_bytes: u64,
    pub registry_sha256: String,
    pub accounts_file: String,
    pub accounts_bytes: u64,
    pub accounts_sha256: String,
    pub manifest_transactions: u64,
    pub manifest_signatures: u64,
    pub manifest_pubkeys: u64,
    pub transaction_hash_verified_during_build: bool,
    pub signature_hash_verified_during_build: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexFileBinding {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
    pub records: u64,
    pub record_bytes: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexHeader {
    pub magic: [u8; 8],
    pub flags: u16,
    pub record_bytes: u16,
    pub record_count: u64,
    pub source_manifest_sha256: [u8; 32],
    pub source_transaction_sha256: [u8; 32],
}

impl IndexHeader {
    pub fn encode(self) -> [u8; INDEX_HEADER_BYTES] {
        let mut bytes = [0u8; INDEX_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&self.magic);
        bytes[8..10].copy_from_slice(&INDEX_SCHEMA_VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(INDEX_HEADER_BYTES as u16).to_le_bytes());
        bytes[12..14].copy_from_slice(&self.record_bytes.to_le_bytes());
        bytes[14..16].copy_from_slice(&self.flags.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        bytes[24..56].copy_from_slice(&self.source_manifest_sha256);
        bytes[56..88].copy_from_slice(&self.source_transaction_sha256);
        bytes
    }

    pub fn decode(
        bytes: &[u8],
        expected_magic: [u8; 8],
        expected_record_bytes: u16,
    ) -> Result<Self> {
        ensure!(
            bytes.len() >= INDEX_HEADER_BYTES,
            "index file is shorter than its header"
        );
        let header = &bytes[..INDEX_HEADER_BYTES];
        ensure!(header[0..8] == expected_magic, "index file magic differs");
        ensure!(
            read_u16(header, 8) == INDEX_SCHEMA_VERSION,
            "index file schema version differs"
        );
        ensure!(
            usize::from(read_u16(header, 10)) == INDEX_HEADER_BYTES,
            "index header byte length differs"
        );
        ensure!(
            read_u16(header, 12) == expected_record_bytes,
            "index record byte length differs"
        );
        let flags = read_u16(header, 14);
        ensure!(
            flags & !INDEX_FLAG_COMPLETE == 0,
            "index file has unknown flags"
        );
        ensure!(
            header[88..INDEX_HEADER_BYTES].iter().all(|byte| *byte == 0),
            "index header has non-zero reserved bytes"
        );
        Ok(Self {
            magic: expected_magic,
            flags,
            record_bytes: expected_record_bytes,
            record_count: read_u64(header, 16),
            source_manifest_sha256: header[24..56]
                .try_into()
                .expect("fixed source manifest digest range"),
            source_transaction_sha256: header[56..88]
                .try_into()
                .expect("fixed source transaction digest range"),
        })
    }

    pub const fn complete(self) -> bool {
        self.flags & INDEX_FLAG_COMPLETE != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TransactionCoordinate {
    pub epoch: u64,
    pub slot: u64,
    pub source_block_id: u32,
    pub tx_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocatorRecord {
    pub coordinate: TransactionCoordinate,
    pub frame: FrameLocator,
    pub first_signature_ordinal: u64,
    pub flags: u32,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transaction_count: u32,
    pub signature_count: u8,
    pub source_wire_profile: DumpWireProfile,
}

impl LocatorRecord {
    pub fn encode(self) -> [u8; LOCATOR_RECORD_BYTES] {
        let mut bytes = [0u8; LOCATOR_RECORD_BYTES];
        bytes[0..8].copy_from_slice(&self.coordinate.epoch.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.coordinate.slot.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.coordinate.source_block_id.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.coordinate.tx_index.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.frame.payload_offset.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.frame.payload_len.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.flags.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.first_signature_ordinal.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.parent_slot.to_le_bytes());
        bytes[56..64].copy_from_slice(&self.block_time.unwrap_or(BLOCK_TIME_NONE).to_le_bytes());
        bytes[64..72]
            .copy_from_slice(&self.block_height.unwrap_or(BLOCK_HEIGHT_NONE).to_le_bytes());
        bytes[72..76].copy_from_slice(&self.transaction_count.to_le_bytes());
        bytes[76] = self.signature_count;
        bytes[77] = encode_wire_profile(self.source_wire_profile);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == LOCATOR_RECORD_BYTES,
            "locator row byte length differs"
        );
        ensure!(
            bytes[78..80].iter().all(|byte| *byte == 0),
            "locator row has non-zero reserved bytes"
        );
        let block_time = read_i64(bytes, 56);
        let block_height = read_u64(bytes, 64);
        Ok(Self {
            coordinate: TransactionCoordinate {
                epoch: read_u64(bytes, 0),
                slot: read_u64(bytes, 8),
                source_block_id: read_u32(bytes, 16),
                tx_index: read_u32(bytes, 20),
            },
            frame: FrameLocator {
                payload_offset: read_u64(bytes, 24),
                payload_len: read_u32(bytes, 32),
            },
            flags: read_u32(bytes, 36),
            first_signature_ordinal: read_u64(bytes, 40),
            parent_slot: read_u64(bytes, 48),
            block_time: (block_time != BLOCK_TIME_NONE).then_some(block_time),
            block_height: (block_height != BLOCK_HEIGHT_NONE).then_some(block_height),
            transaction_count: read_u32(bytes, 72),
            signature_count: bytes[76],
            source_wire_profile: decode_wire_profile(bytes[77])?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SignatureIndexRecord {
    pub signature: [u8; 64],
    pub transaction_id: u64,
    pub signature_position: u8,
}

impl SignatureIndexRecord {
    pub fn encode(self) -> [u8; SIGNATURE_RECORD_BYTES] {
        let mut bytes = [0u8; SIGNATURE_RECORD_BYTES];
        bytes[0..64].copy_from_slice(&self.signature);
        bytes[64..72].copy_from_slice(&self.transaction_id.to_le_bytes());
        bytes[72] = self.signature_position;
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == SIGNATURE_RECORD_BYTES,
            "signature row byte length differs"
        );
        ensure!(
            bytes[73..80].iter().all(|byte| *byte == 0),
            "signature row has non-zero reserved bytes"
        );
        Ok(Self {
            signature: bytes[0..64].try_into().expect("fixed signature byte range"),
            transaction_id: read_u64(bytes, 64),
            signature_position: bytes[72],
        })
    }
}

pub(crate) fn encoded_file_bytes(record_count: u64, record_bytes: usize) -> Result<u64> {
    u64::try_from(INDEX_HEADER_BYTES)
        .expect("header size fits u64")
        .checked_add(
            record_count
                .checked_mul(u64::try_from(record_bytes).expect("record size fits u64"))
                .context("index record byte length overflow")?,
        )
        .context("index file byte length overflow")
}

pub(crate) fn parse_hex_digest(value: &str, label: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "{label} is not a 32-byte hex digest");
    let mut output = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0], label)? << 4) | hex_nibble(pair[1], label)?;
    }
    Ok(output)
}

pub(crate) fn hex_digest(value: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn hex_nibble(value: u8, label: &str) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => bail!("{label} is not canonical lowercase hex"),
    }
}

const fn encode_wire_profile(profile: DumpWireProfile) -> u8 {
    match profile {
        DumpWireProfile::PostUnknownInstructionFallbacksV1 => 0,
        DumpWireProfile::PreUnknownInstructionFallbacksV1 => 1,
    }
}

fn decode_wire_profile(value: u8) -> Result<DumpWireProfile> {
    match value {
        0 => Ok(DumpWireProfile::PostUnknownInstructionFallbacksV1),
        1 => Ok(DumpWireProfile::PreUnknownInstructionFallbacksV1),
        _ => bail!("locator row has an unknown wire profile"),
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 byte range"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 byte range"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 byte range"),
    )
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed i64 byte range"),
    )
}
