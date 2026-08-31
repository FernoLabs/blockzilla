//! Exact Compact V2 hot-message grammar selection and decoding.
//!
//! Compact V2 did not change its outer format version when two instruction
//! fallback variants were added. Manifest-bound marker objects select the
//! exact enum order. Readers must never infer this choice from message data.

use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
    ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate,
    ArchiveV2VoteTowerSync, CompactMessageHeader, CompactPubkey, OwnedCompactAddressTableLookup,
    OwnedCompactRecentBlockhash, wincode_leb128_config,
};
use sha2::{Digest as _, Sha256};
use smallvec::SmallVec;
use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    RangeSource, SourceError,
    manifest::{GenerationFile, GenerationManifest},
};

/// The manifest object that explicitly selects the current/Post message enum order.
pub const COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE: &str =
    "archive-v2-message-schema-current-v1.marker";
pub const COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SIZE: u64 = 52;
pub const COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SHA256: &str =
    "68a1662310dcb2af23c1de1ace2f8f067e77e3d8601fe6377f140d27ee351142";
pub const COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES: &[u8; 52] =
    include_bytes!("../assets/archive-v2-message-schema-current-v1.marker");

/// The manifest object that selects the exact 2026-05-24 message enum order.
pub const COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE: &str =
    "archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker";
pub const COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE: u64 = 87;
pub const COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SHA256: &str =
    "2a3aa5808085bc7b869c7536508227f19e6b9d9e3f5fb34b65ebda9936bf0206";
pub const COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES: &[u8; 87] =
    include_bytes!("../assets/archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker");

const MAY24_INLINE_ACCOUNT_KEYS: usize = 8;
const MAY24_INLINE_INSTRUCTIONS: usize = 2;
const MAY24_INLINE_INSTRUCTION_ACCOUNTS: usize = 8;
const MAY24_INLINE_RAW_INSTRUCTION_BYTES: usize = 64;
const MAY24_MAINNET_EPOCHS: [u64; 3] = [0, 1, 2];

/// The one message grammar selected for a complete Compact V2 generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactV2MessageSchema {
    Current,
    /// The enum order used before `UnknownSystem` and `UnknownVote` existed.
    May24PreUnknownFallbacks,
}

/// Select a grammar from the fixed marker objects without a publication file.
///
/// A present marker is read once and compared directly with the repository
/// asset. No digest is computed.
pub fn detect_compact_v2_message_schema<S: RangeSource>(
    source: &S,
    epoch: u64,
    cluster_id: &str,
) -> Result<CompactV2MessageSchema, CompactV2MessageSchemaError> {
    let may24_size = source.size(COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE)?;
    let current_size = source.size(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE)?;
    if may24_size.is_some() && current_size.is_some() {
        return Err(CompactV2MessageSchemaError::MessageSchemaMarkerConflict);
    }
    if let Some(actual) = may24_size {
        validate_marker_bytes(
            source,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            actual,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES,
        )?;
        return Ok(CompactV2MessageSchema::May24PreUnknownFallbacks);
    }
    if let Some(actual) = current_size {
        validate_marker_bytes(
            source,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            actual,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SIZE,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
        )?;
        return Ok(CompactV2MessageSchema::Current);
    }
    if cluster_id == "mainnet-beta" && MAY24_MAINNET_EPOCHS.contains(&epoch) {
        return Err(CompactV2MessageSchemaError::HistoricalMainnetMarkerMissing { epoch });
    }
    Ok(CompactV2MessageSchema::Current)
}

fn validate_marker_bytes<S: RangeSource>(
    source: &S,
    file: &'static str,
    actual_size: u64,
    expected_size: u64,
    expected_bytes: &'static [u8],
) -> Result<(), CompactV2MessageSchemaError> {
    if actual_size != expected_size {
        return Err(CompactV2MessageSchemaError::MarkerObjectSize {
            expected: expected_size,
            actual: actual_size,
        });
    }
    let bytes = source.read_all_bounded(file, expected_size as usize)?;
    if bytes.as_slice() != expected_bytes {
        return Err(CompactV2MessageSchemaError::MarkerObjectBytes);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum CompactV2MessageSchemaError {
    #[error("invalid Compact V2 generation manifest: {0}")]
    InvalidManifest(String),

    #[error("cannot read Compact V2 message-schema marker: {0}")]
    Source(#[from] SourceError),

    #[error(
        "Compact V2 message-schema marker is present but {COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE} is not bound by the generation manifest"
    )]
    MarkerNotManifestBound,

    #[error(
        "generation manifest binds {COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE}, but the source object is missing"
    )]
    BoundMarkerMissing,

    #[error(
        "Compact V2 message-schema marker is present but {COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE} is not bound by the generation manifest"
    )]
    CurrentMarkerNotManifestBound,

    #[error(
        "generation manifest binds {COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE}, but the source object is missing"
    )]
    BoundCurrentMarkerMissing,

    #[error(
        "an unpublished source cannot contain an explicit current Compact V2 message-schema marker"
    )]
    UnpublishedCurrentMarker,

    #[error(
        "generation contains both current and May24 Compact V2 message-schema markers; exactly one is permitted"
    )]
    MessageSchemaMarkerConflict,

    #[error("an unpublished source cannot select a historical Compact V2 message grammar")]
    UnpublishedHistoricalMarker,

    #[error("message-schema marker manifest size is {actual}, expected {expected}")]
    MarkerManifestSize { expected: u64, actual: u64 },

    #[error("message-schema marker manifest SHA-256 is {actual}, expected {expected}")]
    MarkerManifestDigest {
        expected: &'static str,
        actual: String,
    },

    #[error("message-schema marker object size is {actual}, expected {expected}")]
    MarkerObjectSize { expected: u64, actual: u64 },

    #[error("message-schema marker object SHA-256 is {actual}, expected {expected}")]
    MarkerObjectDigest {
        expected: &'static str,
        actual: String,
    },

    #[error("message-schema marker bytes do not equal the required marker bytes")]
    MarkerObjectBytes,

    #[error(
        "mainnet epoch {epoch} requires an explicit Compact V2 message grammar but has no manifest-bound current or May24 marker"
    )]
    HistoricalMainnetMarkerMissing { epoch: u64 },

    #[error("cannot decode exact {schema:?} Compact V2 message: {message}")]
    Decode {
        schema: CompactV2MessageSchema,
        message: String,
    },
}

/// Select one grammar from a published generation manifest and its bytes.
///
/// Exactly one present marker is effective only when the manifest binds its
/// exact name, size, and digest. The object bytes are checked independently.
pub fn select_compact_v2_message_schema<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
) -> Result<CompactV2MessageSchema, CompactV2MessageSchemaError> {
    manifest
        .validate()
        .map_err(|error| CompactV2MessageSchemaError::InvalidManifest(error.to_string()))?;
    select_message_schema(source, Some(manifest), manifest.epoch, &manifest.cluster_id)
}

/// Select the current grammar for an explicit unpublished fixture.
///
/// An unpublished fixture cannot opt into a historical grammar because it has
/// no manifest that can bind the marker. The early mainnet epochs still fail
/// closed when the caller gives the mainnet identity.
pub fn select_unpublished_compact_v2_message_schema<S: RangeSource>(
    source: &S,
    epoch: u64,
    cluster_id: &str,
) -> Result<CompactV2MessageSchema, CompactV2MessageSchemaError> {
    select_message_schema(source, None, epoch, cluster_id)
}

fn select_message_schema<S: RangeSource>(
    source: &S,
    manifest: Option<&GenerationManifest>,
    epoch: u64,
    cluster_id: &str,
) -> Result<CompactV2MessageSchema, CompactV2MessageSchemaError> {
    let may24_source_size = source.size(COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE)?;
    let current_source_size = source.size(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE)?;
    let may24_binding =
        manifest.and_then(|manifest| manifest.file(COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE));
    let current_binding =
        manifest.and_then(|manifest| manifest.file(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE));

    let may24_active = validate_marker_presence(
        manifest.is_some(),
        MessageSchemaMarkerKind::May24,
        may24_source_size,
        may24_binding,
    )?;
    let current_active = validate_marker_presence(
        manifest.is_some(),
        MessageSchemaMarkerKind::Current,
        current_source_size,
        current_binding,
    )?;

    if may24_active && current_active {
        return Err(CompactV2MessageSchemaError::MessageSchemaMarkerConflict);
    }
    if may24_active {
        validate_marker_object(
            source,
            may24_source_size.expect("active marker has a source object"),
            may24_binding.expect("active marker has a manifest binding"),
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SHA256,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES,
        )?;
        return Ok(CompactV2MessageSchema::May24PreUnknownFallbacks);
    }
    if current_active {
        validate_marker_object(
            source,
            current_source_size.expect("active marker has a source object"),
            current_binding.expect("active marker has a manifest binding"),
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SIZE,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SHA256,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
        )?;
        return Ok(CompactV2MessageSchema::Current);
    }
    if cluster_id == "mainnet-beta" && MAY24_MAINNET_EPOCHS.contains(&epoch) {
        return Err(CompactV2MessageSchemaError::HistoricalMainnetMarkerMissing { epoch });
    }
    Ok(CompactV2MessageSchema::Current)
}

#[derive(Debug, Clone, Copy)]
enum MessageSchemaMarkerKind {
    Current,
    May24,
}

fn validate_marker_presence(
    published: bool,
    kind: MessageSchemaMarkerKind,
    source_size: Option<u64>,
    binding: Option<&GenerationFile>,
) -> Result<bool, CompactV2MessageSchemaError> {
    match (source_size, binding) {
        (Some(_), Some(_)) => Ok(true),
        (None, None) => Ok(false),
        (Some(_), None) if !published => Err(match kind {
            MessageSchemaMarkerKind::Current => {
                CompactV2MessageSchemaError::UnpublishedCurrentMarker
            }
            MessageSchemaMarkerKind::May24 => {
                CompactV2MessageSchemaError::UnpublishedHistoricalMarker
            }
        }),
        (Some(_), None) => Err(match kind {
            MessageSchemaMarkerKind::Current => {
                CompactV2MessageSchemaError::CurrentMarkerNotManifestBound
            }
            MessageSchemaMarkerKind::May24 => CompactV2MessageSchemaError::MarkerNotManifestBound,
        }),
        (None, Some(_)) => Err(match kind {
            MessageSchemaMarkerKind::Current => {
                CompactV2MessageSchemaError::BoundCurrentMarkerMissing
            }
            MessageSchemaMarkerKind::May24 => CompactV2MessageSchemaError::BoundMarkerMissing,
        }),
    }
}

fn validate_marker_object<S: RangeSource>(
    source: &S,
    source_size: u64,
    binding: &GenerationFile,
    file: &'static str,
    expected_size: u64,
    expected_digest: &'static str,
    expected_bytes: &'static [u8],
) -> Result<(), CompactV2MessageSchemaError> {
    if binding.size != expected_size {
        return Err(CompactV2MessageSchemaError::MarkerManifestSize {
            expected: expected_size,
            actual: binding.size,
        });
    }
    if binding.sha256 != expected_digest {
        return Err(CompactV2MessageSchemaError::MarkerManifestDigest {
            expected: expected_digest,
            actual: binding.sha256.clone(),
        });
    }
    if source_size != expected_size {
        return Err(CompactV2MessageSchemaError::MarkerObjectSize {
            expected: expected_size,
            actual: source_size,
        });
    }
    let bytes = source.read_all_bounded(file, expected_size as usize)?;
    let actual_digest = hex_lower_sha256(&bytes);
    if actual_digest != expected_digest {
        return Err(CompactV2MessageSchemaError::MarkerObjectDigest {
            expected: expected_digest,
            actual: actual_digest,
        });
    }
    if bytes.as_slice() != expected_bytes {
        return Err(CompactV2MessageSchemaError::MarkerObjectBytes);
    }
    Ok(())
}

/// Decode one complete message with the generation-selected exact grammar.
pub fn decode_compact_v2_message(
    schema: CompactV2MessageSchema,
    bytes: &[u8],
) -> Result<ArchiveV2HotMessagePayload, CompactV2MessageSchemaError> {
    match schema {
        CompactV2MessageSchema::Current => {
            wincode::config::deserialize_exact(bytes, wincode_leb128_config()).map_err(|error| {
                CompactV2MessageSchemaError::Decode {
                    schema,
                    message: error.to_string(),
                }
            })
        }
        CompactV2MessageSchema::May24PreUnknownFallbacks => {
            let historical: May24ArchiveV2HotMessagePayload =
                wincode::config::deserialize_exact(bytes, wincode_leb128_config()).map_err(
                    |error| CompactV2MessageSchemaError::Decode {
                        schema,
                        message: error.to_string(),
                    },
                )?;
            Ok(historical.into())
        }
    }
}

fn hex_lower_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(char::from_digit((byte >> 4) as u32, 16).expect("hex nibble"));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).expect("hex nibble"));
    }
    out
}

#[derive(Debug, SchemaRead, SchemaWrite)]
enum May24ArchiveV2HotMessagePayload {
    Legacy(May24ArchiveV2HotLegacyMessage),
    V0(May24ArchiveV2HotV0Message),
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct May24ArchiveV2HotLegacyMessage {
    header: CompactMessageHeader,
    account_keys: SmallVec<[CompactPubkey; MAY24_INLINE_ACCOUNT_KEYS]>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: SmallVec<[May24ArchiveV2HotInstruction; MAY24_INLINE_INSTRUCTIONS]>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct May24ArchiveV2HotV0Message {
    header: CompactMessageHeader,
    account_keys: SmallVec<[CompactPubkey; MAY24_INLINE_ACCOUNT_KEYS]>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: SmallVec<[May24ArchiveV2HotInstruction; MAY24_INLINE_INSTRUCTIONS]>,
    address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct May24ArchiveV2HotInstruction {
    program_id_index: u8,
    accounts: SmallVec<[u8; MAY24_INLINE_INSTRUCTION_ACCOUNTS]>,
    data: May24ArchiveV2HotInstructionData,
}

/// Exact pre-2026-06-25 enum order. Do not add variants to this enum.
#[derive(Debug, SchemaRead, SchemaWrite)]
enum May24ArchiveV2HotInstructionData {
    Raw(SmallVec<[u8; MAY24_INLINE_RAW_INSTRUCTION_BYTES]>),
    ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    System(ArchiveV2SystemInstructionData),
    VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
    VoteCompactUpdateVoteStateSwitch {
        update: ArchiveV2VoteStateUpdate,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
    VoteTowerSync(ArchiveV2VoteTowerSync),
    VoteTowerSyncSwitch {
        tower: ArchiveV2VoteTowerSync,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
}

impl From<May24ArchiveV2HotMessagePayload> for ArchiveV2HotMessagePayload {
    fn from(value: May24ArchiveV2HotMessagePayload) -> Self {
        match value {
            May24ArchiveV2HotMessagePayload::Legacy(message) => {
                Self::Legacy(ArchiveV2HotLegacyMessage {
                    header: message.header,
                    account_keys: message.account_keys.into_vec(),
                    recent_blockhash: message.recent_blockhash,
                    instructions: message
                        .instructions
                        .into_iter()
                        .map(ArchiveV2HotInstruction::from)
                        .collect(),
                })
            }
            May24ArchiveV2HotMessagePayload::V0(message) => Self::V0(ArchiveV2HotV0Message {
                header: message.header,
                account_keys: message.account_keys.into_vec(),
                recent_blockhash: message.recent_blockhash,
                instructions: message
                    .instructions
                    .into_iter()
                    .map(ArchiveV2HotInstruction::from)
                    .collect(),
                address_table_lookups: message.address_table_lookups,
            }),
        }
    }
}

impl From<May24ArchiveV2HotInstruction> for ArchiveV2HotInstruction {
    fn from(value: May24ArchiveV2HotInstruction) -> Self {
        Self {
            program_id_index: value.program_id_index,
            accounts: value.accounts.into_vec(),
            data: value.data.into(),
        }
    }
}

impl From<May24ArchiveV2HotInstructionData> for ArchiveV2HotInstructionData {
    fn from(value: May24ArchiveV2HotInstructionData) -> Self {
        match value {
            May24ArchiveV2HotInstructionData::Raw(bytes) => Self::Raw(bytes.into_vec()),
            May24ArchiveV2HotInstructionData::ComputeBudget(data) => Self::ComputeBudget(data),
            May24ArchiveV2HotInstructionData::System(data) => Self::System(data),
            May24ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
                Self::VoteCompactUpdateVoteState(update)
            }
            May24ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            } => Self::VoteCompactUpdateVoteStateSwitch {
                update,
                switch_proof_hash,
            },
            May24ArchiveV2HotInstructionData::VoteTowerSync(tower) => Self::VoteTowerSync(tower),
            May24ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            } => Self::VoteTowerSyncSwitch {
                tower,
                switch_proof_hash,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use blockzilla_format::{ArchiveV2HotInstructionData, ArchiveV2HotMessagePayload};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        LocalRangeSource,
        manifest::{GenerationFile, compute_generation_digest},
    };

    fn manifest(cluster_id: &str, epoch: u64, files: Vec<GenerationFile>) -> GenerationManifest {
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: cluster_id.to_owned(),
            epoch,
            generation_id: "test".to_owned(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        manifest
    }

    fn may24_marker_binding() -> GenerationFile {
        GenerationFile {
            name: COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE.to_owned(),
            size: COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE,
            sha256: COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SHA256.to_owned(),
        }
    }

    fn current_marker_binding() -> GenerationFile {
        GenerationFile {
            name: COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE.to_owned(),
            size: COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SIZE,
            sha256: COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SHA256.to_owned(),
        }
    }

    fn write_marker(root: &Path, name: &str, bytes: &[u8]) {
        std::fs::write(root.join(name), bytes).unwrap();
    }

    #[test]
    fn marker_constants_bind_the_exact_bytes() {
        assert_eq!(
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES.len() as u64,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SIZE
        );
        assert_eq!(
            hex_lower_sha256(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES),
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SHA256
        );
        assert_eq!(
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES.len() as u64,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE
        );
        assert_eq!(
            hex_lower_sha256(COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES),
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SHA256
        );
    }

    #[test]
    fn marker_must_be_manifest_bound() {
        let dir = TempDir::new().unwrap();
        write_marker(
            dir.path(),
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let source = LocalRangeSource::new(dir.path());
        let error = select_compact_v2_message_schema(&source, &manifest("mainnet-beta", 0, vec![]))
            .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::MarkerNotManifestBound
        ));
        let error =
            select_unpublished_compact_v2_message_schema(&source, 0, "mainnet-beta").unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::UnpublishedHistoricalMarker
        ));
    }

    #[test]
    fn bound_marker_checks_manifest_and_object() {
        let dir = TempDir::new().unwrap();
        write_marker(
            dir.path(),
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let source = LocalRangeSource::new(dir.path());
        assert_eq!(
            select_compact_v2_message_schema(
                &source,
                &manifest("mainnet-beta", 0, vec![may24_marker_binding()]),
            )
            .unwrap(),
            CompactV2MessageSchema::May24PreUnknownFallbacks
        );

        let mut wrong_binding = may24_marker_binding();
        wrong_binding.sha256 = "0".repeat(64);
        let error = select_compact_v2_message_schema(
            &source,
            &manifest("mainnet-beta", 0, vec![wrong_binding]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::MarkerManifestDigest { .. }
        ));

        let corrupt = TempDir::new().unwrap();
        let mut bytes = COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES.to_vec();
        bytes[0] ^= 1;
        write_marker(
            corrupt.path(),
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            &bytes,
        );
        let error = select_compact_v2_message_schema(
            &LocalRangeSource::new(corrupt.path()),
            &manifest("mainnet-beta", 0, vec![may24_marker_binding()]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::MarkerObjectDigest { .. }
        ));
    }

    #[test]
    fn explicit_current_marker_selects_current_for_early_mainnet() {
        let dir = TempDir::new().unwrap();
        write_marker(
            dir.path(),
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let source = LocalRangeSource::new(dir.path());
        assert_eq!(
            select_compact_v2_message_schema(
                &source,
                &manifest("mainnet-beta", 0, vec![current_marker_binding()]),
            )
            .unwrap(),
            CompactV2MessageSchema::Current
        );

        let mut wrong_binding = current_marker_binding();
        wrong_binding.sha256 = "0".repeat(64);
        let error = select_compact_v2_message_schema(
            &source,
            &manifest("mainnet-beta", 0, vec![wrong_binding]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::MarkerManifestDigest { .. }
        ));

        let corrupt = TempDir::new().unwrap();
        let mut bytes = COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES.to_vec();
        bytes[0] ^= 1;
        write_marker(
            corrupt.path(),
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            &bytes,
        );
        let error = select_compact_v2_message_schema(
            &LocalRangeSource::new(corrupt.path()),
            &manifest("mainnet-beta", 0, vec![current_marker_binding()]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::MarkerObjectDigest { .. }
        ));
    }

    #[test]
    fn current_marker_must_be_bound_and_present() {
        let unbound = TempDir::new().unwrap();
        write_marker(
            unbound.path(),
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let error = select_compact_v2_message_schema(
            &LocalRangeSource::new(unbound.path()),
            &manifest("mainnet-beta", 0, vec![]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::CurrentMarkerNotManifestBound
        ));

        let missing = TempDir::new().unwrap();
        let error = select_compact_v2_message_schema(
            &LocalRangeSource::new(missing.path()),
            &manifest("mainnet-beta", 0, vec![current_marker_binding()]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::BoundCurrentMarkerMissing
        ));
    }

    #[test]
    fn current_and_may24_markers_conflict() {
        let dir = TempDir::new().unwrap();
        write_marker(
            dir.path(),
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        write_marker(
            dir.path(),
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
            COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES,
        );
        let error = select_compact_v2_message_schema(
            &LocalRangeSource::new(dir.path()),
            &manifest(
                "mainnet-beta",
                0,
                vec![current_marker_binding(), may24_marker_binding()],
            ),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::MessageSchemaMarkerConflict
        ));
    }

    #[test]
    fn unmarked_early_mainnet_fails_closed() {
        let dir = TempDir::new().unwrap();
        let source = LocalRangeSource::new(dir.path());
        for epoch in MAY24_MAINNET_EPOCHS {
            let error =
                select_compact_v2_message_schema(&source, &manifest("mainnet-beta", epoch, vec![]))
                    .unwrap_err();
            assert!(matches!(
                error,
                CompactV2MessageSchemaError::HistoricalMainnetMarkerMissing { .. }
            ));
        }
        assert_eq!(
            select_compact_v2_message_schema(&source, &manifest("mainnet-beta", 822, vec![]),)
                .unwrap(),
            CompactV2MessageSchema::Current
        );
    }

    #[test]
    fn unmarked_mainnet_epoch_two_fails_closed() {
        let dir = TempDir::new().unwrap();
        let source = LocalRangeSource::new(dir.path());
        let error = select_compact_v2_message_schema(&source, &manifest("mainnet-beta", 2, vec![]))
            .unwrap_err();
        assert!(matches!(
            error,
            CompactV2MessageSchemaError::HistoricalMainnetMarkerMissing { epoch: 2 }
        ));
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid lowercase hex fixture"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    #[test]
    fn historical_decoder_uses_the_exact_may24_enum_order() {
        let bytes = decode_hex(
            "0002010206121813150e0d00c0e60c02040202000209ccf1736d29ad6e301871d2d5a34e01709272ebdc60b9b855a31b7c3036fae9360131c80106a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc0000000000503030201000c030000000080c6a47e8d0300",
        );
        let historical: May24ArchiveV2HotMessagePayload =
            wincode::config::deserialize_exact(&bytes, wincode_leb128_config()).unwrap();
        assert_eq!(
            wincode::config::serialize(&historical, wincode_leb128_config()).unwrap(),
            bytes,
            "the private compatibility schema must keep its exact wire form"
        );

        assert!(matches!(
            decode_compact_v2_message(CompactV2MessageSchema::Current, &bytes),
            Err(CompactV2MessageSchemaError::Decode {
                schema: CompactV2MessageSchema::Current,
                ..
            })
        ));

        let decoded =
            decode_compact_v2_message(CompactV2MessageSchema::May24PreUnknownFallbacks, &bytes)
                .unwrap();
        let ArchiveV2HotMessagePayload::Legacy(message) = decoded else {
            panic!("expected a legacy message");
        };
        assert!(matches!(
            &message.instructions[0].data,
            ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AllocateWithSeed { seed, space: 200, .. }
            ) if seed == "1"
        ));
        assert!(matches!(
            &message.instructions[1].data,
            ArchiveV2HotInstructionData::Raw(bytes)
                if bytes.as_slice() == decode_hex("030000000080c6a47e8d0300")
        ));

        let mut trailing = bytes;
        trailing.push(0);
        assert!(
            decode_compact_v2_message(CompactV2MessageSchema::May24PreUnknownFallbacks, &trailing,)
                .is_err()
        );
    }
}
