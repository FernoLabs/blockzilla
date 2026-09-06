use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    index_format::{IndexFileBinding, parse_hex_digest},
    owner_postings_format::{FINAL_SPYX_REPLAY_STATE_SHA256, OWNER_REPLAY_SEMANTIC_VERSION},
    postings_format::{POSTINGS_DIRECTORY_RECORD_BYTES, PostingsSourceBinding},
};

pub const OWNER_BALANCE_HISTORY_SCHEMA_VERSION: u16 = 1;
pub const OWNER_BALANCE_HISTORY_MANIFEST_FILE: &str = "owner-balance-history-manifest-v1.json";
pub const OWNER_BALANCE_DIRECTORY_FILE: &str = "owner-balance-directory-v1.bin";
pub const OWNER_BALANCE_EVENTS_FILE: &str = "owner-balance-events-v1.bin";
pub const OWNER_BALANCE_HISTORY_HEADER_BYTES: usize = 128;
pub const OWNER_BALANCE_EVENT_RECORD_BYTES: usize = 56;
pub const OWNER_BALANCE_HISTORY_FLAG_COMPLETE: u16 = 1;
pub const OWNER_BALANCE_HISTORY_SEMANTIC_VERSION: &str = "spyx-owner-balance-history-v1";

const OWNER_BALANCE_DIRECTORY_MAGIC: [u8; 8] = *b"BZSOHD01";
const OWNER_BALANCE_EVENTS_MAGIC: [u8; 8] = *b"BZSOHE01";
const BLOCK_TIME_NONE: i64 = i64::MIN;
const SEMANTIC_DOMAIN: &[u8] = b"blockzilla-spyx-owner-balance-history-v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerBalanceHistoryFileKind {
    Directory,
    Events,
}

impl OwnerBalanceHistoryFileKind {
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::Directory => OWNER_BALANCE_DIRECTORY_FILE,
            Self::Events => OWNER_BALANCE_EVENTS_FILE,
        }
    }

    const fn magic(self) -> [u8; 8] {
        match self {
            Self::Directory => OWNER_BALANCE_DIRECTORY_MAGIC,
            Self::Events => OWNER_BALANCE_EVENTS_MAGIC,
        }
    }

    pub const fn record_bytes(self) -> u16 {
        match self {
            Self::Directory => POSTINGS_DIRECTORY_RECORD_BYTES as u16,
            Self::Events => OWNER_BALANCE_EVENT_RECORD_BYTES as u16,
        }
    }

    pub fn encoded_file_bytes(self, records: u64) -> Result<u64> {
        records
            .checked_mul(u64::from(self.record_bytes()))
            .and_then(|body| body.checked_add(OWNER_BALANCE_HISTORY_HEADER_BYTES as u64))
            .context("owner balance-history file byte length overflow")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerBalanceHistoryFileHeader {
    pub kind: OwnerBalanceHistoryFileKind,
    pub complete: bool,
    pub record_count: u64,
    pub source_manifest_sha256: [u8; 32],
    pub source_transaction_sha256: [u8; 32],
}

impl OwnerBalanceHistoryFileHeader {
    pub fn encode(self) -> [u8; OWNER_BALANCE_HISTORY_HEADER_BYTES] {
        let mut bytes = [0u8; OWNER_BALANCE_HISTORY_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&self.kind.magic());
        bytes[8..10].copy_from_slice(&OWNER_BALANCE_HISTORY_SCHEMA_VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(OWNER_BALANCE_HISTORY_HEADER_BYTES as u16).to_le_bytes());
        bytes[12..14].copy_from_slice(&self.kind.record_bytes().to_le_bytes());
        let flags = if self.complete {
            OWNER_BALANCE_HISTORY_FLAG_COMPLETE
        } else {
            0
        };
        bytes[14..16].copy_from_slice(&flags.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        bytes[24..56].copy_from_slice(&self.source_manifest_sha256);
        bytes[56..88].copy_from_slice(&self.source_transaction_sha256);
        bytes
    }

    pub fn decode(bytes: &[u8], expected_kind: OwnerBalanceHistoryFileKind) -> Result<Self> {
        ensure!(
            bytes.len() >= OWNER_BALANCE_HISTORY_HEADER_BYTES,
            "owner balance-history file is shorter than its header"
        );
        let header = &bytes[..OWNER_BALANCE_HISTORY_HEADER_BYTES];
        ensure!(
            header[0..8] == expected_kind.magic(),
            "owner balance-history magic differs"
        );
        ensure!(
            read_u16(header, 8) == OWNER_BALANCE_HISTORY_SCHEMA_VERSION
                && usize::from(read_u16(header, 10)) == OWNER_BALANCE_HISTORY_HEADER_BYTES
                && read_u16(header, 12) == expected_kind.record_bytes(),
            "owner balance-history fixed format differs"
        );
        let flags = read_u16(header, 14);
        ensure!(
            flags & !OWNER_BALANCE_HISTORY_FLAG_COMPLETE == 0,
            "owner balance-history header has unknown flags"
        );
        ensure!(
            header[88..].iter().all(|byte| *byte == 0),
            "owner balance-history header has non-zero reserved bytes"
        );
        Ok(Self {
            kind: expected_kind,
            complete: flags & OWNER_BALANCE_HISTORY_FLAG_COMPLETE != 0,
            record_count: read_u64(header, 16),
            source_manifest_sha256: header[24..56]
                .try_into()
                .expect("fixed source manifest digest range"),
            source_transaction_sha256: header[56..88]
                .try_into()
                .expect("fixed source transaction digest range"),
        })
    }
}

/// One exact transaction-final balance change for one SPYx token-account owner.
///
/// `transaction_id` is the zero-based ordinal in the bound consolidated
/// transaction stream. Rows are stored by owner and then by transaction ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct OwnerBalanceEventRecord {
    pub transaction_id: u64,
    pub slot: u64,
    pub block_time: Option<i64>,
    #[serde(serialize_with = "serialize_i128_decimal")]
    pub raw_delta: i128,
    #[serde(serialize_with = "serialize_u128_decimal")]
    pub post_raw_balance: u128,
}

fn serialize_i128_decimal<S>(value: &i128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_u128_decimal<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

impl OwnerBalanceEventRecord {
    pub fn validate(self) -> Result<()> {
        ensure!(self.raw_delta != 0, "owner balance event has a zero delta");
        ensure!(
            self.block_time != Some(BLOCK_TIME_NONE),
            "owner balance event uses the reserved block-time value"
        );
        Ok(())
    }

    pub fn encode(self) -> Result<[u8; OWNER_BALANCE_EVENT_RECORD_BYTES]> {
        self.validate()?;
        let mut bytes = [0u8; OWNER_BALANCE_EVENT_RECORD_BYTES];
        bytes[0..8].copy_from_slice(&self.transaction_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.slot.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.block_time.unwrap_or(BLOCK_TIME_NONE).to_le_bytes());
        bytes[24..40].copy_from_slice(&self.raw_delta.to_le_bytes());
        bytes[40..56].copy_from_slice(&self.post_raw_balance.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == OWNER_BALANCE_EVENT_RECORD_BYTES,
            "owner balance event has an invalid byte length"
        );
        let block_time = read_i64(bytes, 16);
        let event = Self {
            transaction_id: read_u64(bytes, 0),
            slot: read_u64(bytes, 8),
            block_time: (block_time != BLOCK_TIME_NONE).then_some(block_time),
            raw_delta: i128::from_le_bytes(bytes[24..40].try_into().expect("fixed i128 range")),
            post_raw_balance: u128::from_le_bytes(
                bytes[40..56].try_into().expect("fixed u128 range"),
            ),
        };
        event.validate()?;
        Ok(event)
    }
}

pub struct OwnerBalanceHistorySemanticHasher {
    hasher: Sha256,
    expected_events: u64,
    observed_events: u64,
}

impl OwnerBalanceHistorySemanticHasher {
    pub fn new(expected_events: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(SEMANTIC_DOMAIN);
        hasher.update(expected_events.to_le_bytes());
        Self {
            hasher,
            expected_events,
            observed_events: 0,
        }
    }

    pub fn update(&mut self, owner_registry_id: u32, event: OwnerBalanceEventRecord) -> Result<()> {
        ensure!(
            owner_registry_id != 0,
            "owner balance event has registry ID zero"
        );
        self.hasher.update(owner_registry_id.to_le_bytes());
        self.hasher.update(event.encode()?);
        self.observed_events = self
            .observed_events
            .checked_add(1)
            .context("owner balance semantic event count overflow")?;
        Ok(())
    }

    pub fn finish(self) -> Result<[u8; 32]> {
        ensure!(
            self.observed_events == self.expected_events,
            "owner balance semantic event count differs"
        );
        Ok(self.hasher.finalize().into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerBalanceHistoryManifest {
    pub schema_version: u16,
    pub artifact_kind: String,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_max_transactions: Option<u64>,
    pub transactions: u64,
    pub created_unix_seconds: u64,
    pub source: PostingsSourceBinding,
    pub replay_semantic_version: String,
    pub replay_state_sha256: String,
    pub owner_postings_semantic_sha256: String,
    pub history_semantic_version: String,
    pub history_semantic_sha256: String,
    pub owner_directory: IndexFileBinding,
    pub balance_events: IndexFileBinding,
}

impl OwnerBalanceHistoryManifest {
    pub const ARTIFACT_KIND: &'static str = "blockzilla_spyx_owner_balance_history";

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == OWNER_BALANCE_HISTORY_SCHEMA_VERSION
                && self.artifact_kind == Self::ARTIFACT_KIND
                && self.transactions != 0
                && self.created_unix_seconds != 0
                && self.replay_semantic_version == OWNER_REPLAY_SEMANTIC_VERSION
                && self.history_semantic_version == OWNER_BALANCE_HISTORY_SEMANTIC_VERSION,
            "invalid owner balance-history manifest header"
        );
        self.source.validate()?;
        parse_hex_digest(&self.replay_state_sha256, "owner replay state digest")?;
        parse_hex_digest(
            &self.owner_postings_semantic_sha256,
            "owner postings semantic digest",
        )?;
        parse_hex_digest(
            &self.history_semantic_sha256,
            "owner balance-history semantic digest",
        )?;
        ensure!(
            self.transactions <= self.source.transactions,
            "owner balance-history transaction count exceeds its source"
        );
        match (self.complete, self.canary_max_transactions) {
            (true, None) => ensure!(
                self.transactions == self.source.transactions
                    && self.replay_state_sha256 == FINAL_SPYX_REPLAY_STATE_SHA256,
                "complete owner balance history does not cover the accepted replay source"
            ),
            (false, Some(maximum)) => ensure!(
                maximum != 0 && self.transactions == maximum.min(self.source.transactions),
                "owner balance-history canary has an invalid transaction limit"
            ),
            _ => bail!("owner balance-history completion markers are inconsistent"),
        }
        ensure!(
            self.owner_directory.records <= self.balance_events.records,
            "owner balance-history directory has more owners than events"
        );
        for (binding, kind) in [
            (
                &self.owner_directory,
                OwnerBalanceHistoryFileKind::Directory,
            ),
            (&self.balance_events, OwnerBalanceHistoryFileKind::Events),
        ] {
            ensure!(
                binding.file == kind.file_name()
                    && binding.record_bytes == kind.record_bytes()
                    && binding.bytes == kind.encoded_file_bytes(binding.records)?,
                "owner balance-history file binding differs from its fixed format"
            );
            parse_hex_digest(&binding.sha256, "owner balance-history file digest")?;
        }
        Ok(())
    }

    pub fn binding(&self, kind: OwnerBalanceHistoryFileKind) -> &IndexFileBinding {
        match kind {
            OwnerBalanceHistoryFileKind::Directory => &self.owner_directory,
            OwnerBalanceHistoryFileKind::Events => &self.balance_events,
        }
    }

    pub fn validate_header(&self, header: OwnerBalanceHistoryFileHeader) -> Result<()> {
        self.validate()?;
        let binding = self.binding(header.kind);
        ensure!(
            header.complete == self.complete
                && header.record_count == binding.records
                && header.source_manifest_sha256
                    == parse_hex_digest(&self.source.manifest_sha256, "source manifest digest")?
                && header.source_transaction_sha256
                    == parse_hex_digest(
                        &self.source.transaction_sha256,
                        "source transaction digest",
                    )?,
            "owner balance-history header differs from its manifest binding"
        );
        Ok(())
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 range"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 range"),
    )
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed i64 range"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trip_preserves_signed_delta_and_optional_time() {
        for event in [
            OwnerBalanceEventRecord {
                transaction_id: 7,
                slot: 42,
                block_time: Some(1_800_000_000),
                raw_delta: -123,
                post_raw_balance: 456,
            },
            OwnerBalanceEventRecord {
                transaction_id: 8,
                slot: 43,
                block_time: None,
                raw_delta: 999,
                post_raw_balance: 999,
            },
        ] {
            assert_eq!(
                OwnerBalanceEventRecord::decode(&event.encode().unwrap()).unwrap(),
                event
            );
        }
        let json = serde_json::to_value(OwnerBalanceEventRecord {
            transaction_id: 7,
            slot: 42,
            block_time: None,
            raw_delta: i128::MIN + 1,
            post_raw_balance: u128::MAX,
        })
        .unwrap();
        assert_eq!(json["raw_delta"], (i128::MIN + 1).to_string());
        assert_eq!(json["post_raw_balance"], u128::MAX.to_string());
    }

    #[test]
    fn event_rejects_zero_delta_and_reserved_time() {
        let event = OwnerBalanceEventRecord {
            transaction_id: 1,
            slot: 2,
            block_time: None,
            raw_delta: 0,
            post_raw_balance: 0,
        };
        assert!(event.encode().is_err());
        assert!(
            OwnerBalanceEventRecord {
                raw_delta: 1,
                block_time: Some(i64::MIN),
                ..event
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn headers_are_kind_and_source_bound() {
        let header = OwnerBalanceHistoryFileHeader {
            kind: OwnerBalanceHistoryFileKind::Events,
            complete: false,
            record_count: 12,
            source_manifest_sha256: [3; 32],
            source_transaction_sha256: [4; 32],
        };
        assert_eq!(
            OwnerBalanceHistoryFileHeader::decode(
                &header.encode(),
                OwnerBalanceHistoryFileKind::Events,
            )
            .unwrap(),
            header
        );
        assert!(
            OwnerBalanceHistoryFileHeader::decode(
                &header.encode(),
                OwnerBalanceHistoryFileKind::Directory,
            )
            .is_err()
        );
    }

    #[test]
    fn semantic_digest_binds_owner_and_exact_event_values() {
        let event = OwnerBalanceEventRecord {
            transaction_id: 7,
            slot: 42,
            block_time: Some(100),
            raw_delta: -9,
            post_raw_balance: 11,
        };
        let digest = |owner, value| {
            let mut hasher = OwnerBalanceHistorySemanticHasher::new(1);
            hasher.update(owner, value).unwrap();
            hasher.finish().unwrap()
        };
        assert_ne!(digest(3, event), digest(4, event));
        assert_ne!(
            digest(3, event),
            digest(
                3,
                OwnerBalanceEventRecord {
                    post_raw_balance: 12,
                    ..event
                }
            )
        );
    }
}
