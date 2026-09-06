use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    index_format::{IndexFileBinding, parse_hex_digest},
    postings_format::{
        POSTINGS_BODY_RECORD_BYTES, POSTINGS_DIRECTORY_RECORD_BYTES, PostingsSourceBinding,
    },
};

pub const OWNER_POSTINGS_SCHEMA_VERSION: u16 = 1;
pub const OWNER_POSTINGS_MANIFEST_FILE: &str = "owner-postings-manifest-v1.json";
pub const OWNER_DIRECTORY_FILE: &str = "owner-directory-v1.bin";
pub const OWNER_POSTINGS_FILE: &str = "owner-postings-v1.bin";
pub const OWNER_POSTINGS_HEADER_BYTES: usize = 128;
pub const OWNER_POSTINGS_FLAG_COMPLETE: u16 = 1;
pub const OWNER_REPLAY_SEMANTIC_VERSION: &str = "spyx-owner-linked-target-replay-v1";
pub const FINAL_SPYX_REPLAY_STATE_SHA256: &str =
    "3570f9fb1ebe7e18fbda9d20c80fc16b80edbc0bdae3579347dd89419fb1bfe6";
pub const FINAL_SPYX_OWNER_KEYS: u64 = 112_352;
pub const FINAL_SPYX_OWNER_POSTINGS: u64 = 21_691_712;
pub const FINAL_SPYX_OWNER_SEMANTIC_SHA256: &str =
    "ce0872523a204c83a97353a9bc75d00a293421de8a2369254d9430773301154a";

const OWNER_DIRECTORY_MAGIC: [u8; 8] = *b"BZSOOD01";
const OWNER_POSTINGS_MAGIC: [u8; 8] = *b"BZSOOP01";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerPostingsFileKind {
    Directory,
    Postings,
}

impl OwnerPostingsFileKind {
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::Directory => OWNER_DIRECTORY_FILE,
            Self::Postings => OWNER_POSTINGS_FILE,
        }
    }

    const fn magic(self) -> [u8; 8] {
        match self {
            Self::Directory => OWNER_DIRECTORY_MAGIC,
            Self::Postings => OWNER_POSTINGS_MAGIC,
        }
    }

    pub const fn record_bytes(self) -> u16 {
        match self {
            Self::Directory => POSTINGS_DIRECTORY_RECORD_BYTES as u16,
            Self::Postings => POSTINGS_BODY_RECORD_BYTES as u16,
        }
    }

    pub fn encoded_file_bytes(self, records: u64) -> Result<u64> {
        records
            .checked_mul(u64::from(self.record_bytes()))
            .and_then(|body| body.checked_add(OWNER_POSTINGS_HEADER_BYTES as u64))
            .context("owner postings file byte length overflow")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerPostingsFileHeader {
    pub kind: OwnerPostingsFileKind,
    pub complete: bool,
    pub record_count: u64,
    pub source_manifest_sha256: [u8; 32],
    pub source_transaction_sha256: [u8; 32],
}

impl OwnerPostingsFileHeader {
    pub fn encode(self) -> [u8; OWNER_POSTINGS_HEADER_BYTES] {
        let mut bytes = [0u8; OWNER_POSTINGS_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&self.kind.magic());
        bytes[8..10].copy_from_slice(&OWNER_POSTINGS_SCHEMA_VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(OWNER_POSTINGS_HEADER_BYTES as u16).to_le_bytes());
        bytes[12..14].copy_from_slice(&self.kind.record_bytes().to_le_bytes());
        let flags = if self.complete {
            OWNER_POSTINGS_FLAG_COMPLETE
        } else {
            0
        };
        bytes[14..16].copy_from_slice(&flags.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        bytes[24..56].copy_from_slice(&self.source_manifest_sha256);
        bytes[56..88].copy_from_slice(&self.source_transaction_sha256);
        bytes
    }

    pub fn decode(bytes: &[u8], expected_kind: OwnerPostingsFileKind) -> Result<Self> {
        ensure!(
            bytes.len() >= OWNER_POSTINGS_HEADER_BYTES,
            "owner postings file is shorter than its header"
        );
        let header = &bytes[..OWNER_POSTINGS_HEADER_BYTES];
        ensure!(
            header[0..8] == expected_kind.magic(),
            "owner postings magic differs"
        );
        ensure!(
            read_u16(header, 8) == OWNER_POSTINGS_SCHEMA_VERSION
                && usize::from(read_u16(header, 10)) == OWNER_POSTINGS_HEADER_BYTES
                && read_u16(header, 12) == expected_kind.record_bytes(),
            "owner postings fixed format differs"
        );
        let flags = read_u16(header, 14);
        ensure!(
            flags & !OWNER_POSTINGS_FLAG_COMPLETE == 0,
            "owner postings header has unknown flags"
        );
        ensure!(
            header[88..].iter().all(|byte| *byte == 0),
            "owner postings header has non-zero reserved bytes"
        );
        Ok(Self {
            kind: expected_kind,
            complete: flags & OWNER_POSTINGS_FLAG_COMPLETE != 0,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPostingsManifest {
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
    pub owner_semantic_sha256: String,
    pub owner_directory: IndexFileBinding,
    pub owner_postings: IndexFileBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance_history_manifest: Option<OwnerBalanceHistoryManifestBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerBalanceHistoryManifestBinding {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
}

impl OwnerBalanceHistoryManifestBinding {
    pub const FILE: &'static str = "owner-balance-history-manifest-v1.json";

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.file == Self::FILE && self.bytes != 0,
            "owner balance-history manifest binding is invalid"
        );
        parse_hex_digest(&self.sha256, "owner balance-history manifest digest")?;
        Ok(())
    }
}

impl OwnerPostingsManifest {
    pub const ARTIFACT_KIND: &'static str = "blockzilla_spyx_owner_postings";

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == OWNER_POSTINGS_SCHEMA_VERSION
                && self.artifact_kind == Self::ARTIFACT_KIND
                && self.transactions != 0
                && self.created_unix_seconds != 0
                && self.replay_semantic_version == OWNER_REPLAY_SEMANTIC_VERSION,
            "invalid owner postings manifest header"
        );
        self.source.validate()?;
        parse_hex_digest(&self.replay_state_sha256, "owner replay state digest")?;
        parse_hex_digest(&self.owner_semantic_sha256, "owner semantic digest")?;
        ensure!(
            self.transactions <= self.source.transactions,
            "owner postings transaction count exceeds its source"
        );
        match (self.complete, self.canary_max_transactions) {
            (true, None) => {
                ensure!(
                    self.transactions == self.source.transactions
                        && self.replay_state_sha256 == FINAL_SPYX_REPLAY_STATE_SHA256,
                    "complete owner postings do not cover the exact accepted replay source"
                );
                ensure!(
                    self.owner_directory.records == FINAL_SPYX_OWNER_KEYS
                        && self.owner_postings.records == FINAL_SPYX_OWNER_POSTINGS
                        && self.owner_semantic_sha256 == FINAL_SPYX_OWNER_SEMANTIC_SHA256,
                    "complete owner postings differ from the accepted owner projection"
                );
                // Second release gate: after the first accepted full NAS build,
                // pin owner_directory.records, owner_postings.records, and
                // owner_semantic_sha256 here before deployment.
            }
            (false, Some(maximum)) => ensure!(
                maximum != 0 && self.transactions == maximum.min(self.source.transactions),
                "owner postings canary has an invalid transaction limit"
            ),
            _ => bail!("owner postings completion markers are inconsistent"),
        }
        for (binding, kind) in [
            (&self.owner_directory, OwnerPostingsFileKind::Directory),
            (&self.owner_postings, OwnerPostingsFileKind::Postings),
        ] {
            ensure!(
                binding.file == kind.file_name()
                    && binding.record_bytes == kind.record_bytes()
                    && binding.bytes == kind.encoded_file_bytes(binding.records)?,
                "owner postings file binding differs from its fixed format"
            );
            parse_hex_digest(&binding.sha256, "owner postings file digest")?;
        }
        if let Some(binding) = &self.balance_history_manifest {
            binding.validate()?;
        }
        Ok(())
    }

    pub fn binding(&self, kind: OwnerPostingsFileKind) -> &IndexFileBinding {
        match kind {
            OwnerPostingsFileKind::Directory => &self.owner_directory,
            OwnerPostingsFileKind::Postings => &self.owner_postings,
        }
    }

    pub fn validate_header(&self, header: OwnerPostingsFileHeader) -> Result<()> {
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
            "owner postings header differs from its manifest binding"
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

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn owner_headers_are_kind_and_source_bound() {
        let header = OwnerPostingsFileHeader {
            kind: OwnerPostingsFileKind::Directory,
            complete: true,
            record_count: 7,
            source_manifest_sha256: [3; 32],
            source_transaction_sha256: [4; 32],
        };
        assert_eq!(
            OwnerPostingsFileHeader::decode(&header.encode(), OwnerPostingsFileKind::Directory)
                .unwrap(),
            header
        );
        assert!(
            OwnerPostingsFileHeader::decode(&header.encode(), OwnerPostingsFileKind::Postings)
                .is_err()
        );
    }

    #[test]
    fn complete_owner_manifest_rejects_empty_ranges() {
        let binding = |kind: OwnerPostingsFileKind| IndexFileBinding {
            file: kind.file_name().to_owned(),
            bytes: kind.encoded_file_bytes(0).unwrap(),
            sha256: DIGEST.to_owned(),
            records: 0,
            record_bytes: kind.record_bytes(),
        };
        let manifest = OwnerPostingsManifest {
            schema_version: OWNER_POSTINGS_SCHEMA_VERSION,
            artifact_kind: OwnerPostingsManifest::ARTIFACT_KIND.to_owned(),
            complete: true,
            canary_max_transactions: None,
            transactions: 1,
            created_unix_seconds: 1,
            source: PostingsSourceBinding {
                manifest_file: blockzilla_token_transaction_dump::DUMP_MANIFEST_FILE.to_owned(),
                manifest_bytes: 1,
                manifest_sha256: DIGEST.to_owned(),
                transaction_file: blockzilla_token_transaction_dump::TRANSACTIONS_FILE.to_owned(),
                transaction_bytes: 1,
                transaction_sha256: DIGEST.to_owned(),
                registry_file: blockzilla_token_transaction_dump::PUBKEY_REGISTRY_FILE.to_owned(),
                registry_bytes: 32,
                registry_sha256: DIGEST.to_owned(),
                accounts_file: blockzilla_token_transaction_dump::ACCOUNTS_FILE.to_owned(),
                accounts_bytes: 1,
                accounts_sha256: DIGEST.to_owned(),
                transactions: 1,
                pubkeys: 1,
                accounts: 1,
            },
            replay_semantic_version: OWNER_REPLAY_SEMANTIC_VERSION.to_owned(),
            replay_state_sha256: FINAL_SPYX_REPLAY_STATE_SHA256.to_owned(),
            owner_semantic_sha256: DIGEST.to_owned(),
            owner_directory: binding(OwnerPostingsFileKind::Directory),
            owner_postings: binding(OwnerPostingsFileKind::Postings),
            balance_history_manifest: None,
        };
        assert!(manifest.validate().is_err());

        let accepted_binding = |kind: OwnerPostingsFileKind, records| IndexFileBinding {
            file: kind.file_name().to_owned(),
            bytes: kind.encoded_file_bytes(records).unwrap(),
            sha256: DIGEST.to_owned(),
            records,
            record_bytes: kind.record_bytes(),
        };
        let mut accepted = manifest;
        accepted.owner_semantic_sha256 = FINAL_SPYX_OWNER_SEMANTIC_SHA256.to_owned();
        accepted.owner_directory =
            accepted_binding(OwnerPostingsFileKind::Directory, FINAL_SPYX_OWNER_KEYS);
        accepted.owner_postings =
            accepted_binding(OwnerPostingsFileKind::Postings, FINAL_SPYX_OWNER_POSTINGS);
        accepted.validate().unwrap();

        let mut wrong_semantic = accepted.clone();
        wrong_semantic.owner_semantic_sha256 = DIGEST.to_owned();
        assert!(wrong_semantic.validate().is_err());

        let mut wrong_count = accepted;
        wrong_count.owner_directory =
            accepted_binding(OwnerPostingsFileKind::Directory, FINAL_SPYX_OWNER_KEYS - 1);
        assert!(wrong_count.validate().is_err());
    }
}
