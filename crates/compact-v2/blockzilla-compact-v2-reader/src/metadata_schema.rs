//! Exact Compact V2 transaction-metadata grammar selection and decoding.
//!
//! Compact V2 changed `CompactMetaV1.err` from a stored raw byte string to a
//! typed `CompactTransactionError` without changing the outer block format
//! version. A caller must select one grammar for the complete generation. The
//! decoder must not infer the grammar from an individual metadata record:
//! records with `err == None` have the same bytes in both grammars.

use blockzilla_compact::{
    CompactInnerInstructions, CompactLogStream, CompactMetaV1, CompactReturnData, CompactReward,
    CompactTokenBalance, CompactTransactionError,
};
use blockzilla_primitives::{CompactPubkey, historical_source_wincode_leb128_config};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{RangeSource, SourceError, manifest::GenerationManifest};

/// The manifest object that selects the legacy raw transaction-error grammar.
pub const COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE: &str =
    "archive-v2-metadata-schema-legacy-raw-error-v1.marker";
pub const COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE: u64 = 58;
pub const COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256: &str =
    "5ac1903c359744924575edb73cfda3b73e819bb0b23914356906ecd3e0884567";
pub const COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES: &[u8; 58] =
    include_bytes!("../assets/archive-v2-metadata-schema-legacy-raw-error-v1.marker");

/// The one transaction-metadata grammar selected for a Compact V2 generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactV2MetadataSchema {
    /// `CompactMetaV1.err` is `Option<CompactTransactionError>`.
    /// This selects the schema; source integers may still be padded.
    CurrentTypedError,
    /// `CompactMetaV1.err` is `Option<Vec<u8>>` containing a stored Wincode
    /// transaction error.
    LegacyRawError,
}

/// Select the metadata grammar from the fixed marker object without a
/// publication file. The marker is compared directly; no digest is computed.
pub fn detect_compact_v2_metadata_schema<S: RangeSource>(
    source: &S,
) -> Result<CompactV2MetadataSchema, CompactV2MetadataSchemaError> {
    let Some(actual_size) = source.size(COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE)? else {
        return Ok(CompactV2MetadataSchema::CurrentTypedError);
    };
    if actual_size != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE {
        return Err(CompactV2MetadataSchemaError::MarkerObjectSize {
            expected: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE,
            actual: actual_size,
        });
    }
    let bytes = source.read_all_bounded(
        COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE,
        COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE as usize,
    )?;
    if bytes.as_slice() != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES {
        return Err(CompactV2MetadataSchemaError::MarkerObjectBytes);
    }
    Ok(CompactV2MetadataSchema::LegacyRawError)
}

#[derive(Debug, Error)]
pub enum CompactV2MetadataSchemaError {
    #[error("invalid Compact V2 generation manifest: {0}")]
    InvalidManifest(String),

    #[error("cannot read Compact V2 metadata-schema marker: {0}")]
    Source(#[from] SourceError),

    #[error(
        "Compact V2 metadata-schema marker is present but {COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE} is not bound by the generation manifest"
    )]
    MarkerNotManifestBound,

    #[error(
        "generation manifest binds {COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE}, but the source object is missing"
    )]
    BoundMarkerMissing,

    #[error("an unpublished source cannot select a historical Compact V2 metadata grammar")]
    UnpublishedHistoricalMarker,

    #[error("metadata-schema marker manifest size is {actual}, expected {expected}")]
    MarkerManifestSize { expected: u64, actual: u64 },

    #[error("metadata-schema marker manifest SHA-256 is {actual}, expected {expected}")]
    MarkerManifestDigest {
        expected: &'static str,
        actual: String,
    },

    #[error("metadata-schema marker object size is {actual}, expected {expected}")]
    MarkerObjectSize { expected: u64, actual: u64 },

    #[error("metadata-schema marker object SHA-256 is {actual}, expected {expected}")]
    MarkerObjectDigest {
        expected: &'static str,
        actual: String,
    },

    #[error("metadata-schema marker bytes do not equal the required marker bytes")]
    MarkerObjectBytes,

    #[error("cannot decode exact {schema:?} Compact V2 metadata: {message}")]
    Decode {
        schema: CompactV2MetadataSchema,
        message: String,
    },

    #[error("cannot decode the stored transaction error in legacy Compact V2 metadata: {0}")]
    LegacyTransactionError(String),
}

/// Select one metadata grammar from a published generation manifest.
///
/// A missing marker selects the current typed-error grammar. A present marker
/// is effective only when the manifest binds its exact name, size, and digest.
pub fn select_compact_v2_metadata_schema<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
) -> Result<CompactV2MetadataSchema, CompactV2MetadataSchemaError> {
    manifest
        .validate()
        .map_err(|error| CompactV2MetadataSchemaError::InvalidManifest(error.to_string()))?;
    select_metadata_schema(source, Some(manifest))
}

/// Select the current grammar for an explicit unpublished fixture.
///
/// Historical grammar selection for a trusted unpublished source must be an
/// explicit caller choice through `ArchiveReader::open_trusted_with_schemas`.
pub fn select_unpublished_compact_v2_metadata_schema<S: RangeSource>(
    source: &S,
) -> Result<CompactV2MetadataSchema, CompactV2MetadataSchemaError> {
    select_metadata_schema(source, None)
}

fn select_metadata_schema<S: RangeSource>(
    source: &S,
    manifest: Option<&GenerationManifest>,
) -> Result<CompactV2MetadataSchema, CompactV2MetadataSchemaError> {
    let source_size = source.size(COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE)?;
    let binding =
        manifest.and_then(|manifest| manifest.file(COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE));

    match (source_size, binding) {
        (Some(_), None) if manifest.is_some() => {
            return Err(CompactV2MetadataSchemaError::MarkerNotManifestBound);
        }
        (Some(_), None) => {
            return Err(CompactV2MetadataSchemaError::UnpublishedHistoricalMarker);
        }
        (None, Some(_)) => return Err(CompactV2MetadataSchemaError::BoundMarkerMissing),
        (None, None) => return Ok(CompactV2MetadataSchema::CurrentTypedError),
        (Some(source_size), Some(binding)) => {
            if binding.size != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE {
                return Err(CompactV2MetadataSchemaError::MarkerManifestSize {
                    expected: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE,
                    actual: binding.size,
                });
            }
            if binding.sha256 != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256 {
                return Err(CompactV2MetadataSchemaError::MarkerManifestDigest {
                    expected: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256,
                    actual: binding.sha256.clone(),
                });
            }
            if source_size != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE {
                return Err(CompactV2MetadataSchemaError::MarkerObjectSize {
                    expected: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE,
                    actual: source_size,
                });
            }
        }
    }

    let bytes = source.read_all_bounded(
        COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE,
        COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE as usize,
    )?;
    let actual_digest = hex_lower_sha256(&bytes);
    if actual_digest != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256 {
        return Err(CompactV2MetadataSchemaError::MarkerObjectDigest {
            expected: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256,
            actual: actual_digest,
        });
    }
    if bytes.as_slice() != COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES {
        return Err(CompactV2MetadataSchemaError::MarkerObjectBytes);
    }
    Ok(CompactV2MetadataSchema::LegacyRawError)
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

/// Exact historical shape before `CompactMetaV1.err` became typed.
///
/// Do not add, remove or reorder fields. This structure is a frozen wire
/// grammar, not a second in-memory metadata model.
#[derive(Debug, SchemaRead, SchemaWrite)]
struct LegacyRawErrorCompactMetaV1 {
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

impl TryFrom<LegacyRawErrorCompactMetaV1> for CompactMetaV1 {
    type Error = CompactV2MetadataSchemaError;

    fn try_from(value: LegacyRawErrorCompactMetaV1) -> Result<Self, Self::Error> {
        let err = value
            .err
            .as_deref()
            .map(CompactTransactionError::from_stored_wincode_bytes)
            .transpose()
            .map_err(|error| {
                CompactV2MetadataSchemaError::LegacyTransactionError(error.to_string())
            })?;
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

/// Decode one complete metadata record with the generation-selected grammar.
/// Both source schemas admit padded historical integers while still requiring
/// the complete selected record and rejecting trailing bytes.
pub fn decode_compact_v2_metadata(
    schema: CompactV2MetadataSchema,
    bytes: &[u8],
) -> Result<CompactMetaV1, CompactV2MetadataSchemaError> {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => {
            wincode::config::deserialize_exact(bytes, historical_source_wincode_leb128_config())
                .map_err(|error| CompactV2MetadataSchemaError::Decode {
                    schema,
                    message: error.to_string(),
                })
        }
        CompactV2MetadataSchema::LegacyRawError => {
            let metadata: LegacyRawErrorCompactMetaV1 = wincode::config::deserialize_exact(
                bytes,
                historical_source_wincode_leb128_config(),
            )
            .map_err(|error| CompactV2MetadataSchemaError::Decode {
                schema,
                message: error.to_string(),
            })?;
            metadata.try_into()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use blockzilla_compact::{CompactInstructionError, CompactTransactionError};
    use blockzilla_primitives::wincode_leb128_config;
    use tempfile::TempDir;

    use crate::{
        LocalRangeSource,
        manifest::{GenerationFile, compute_generation_digest},
    };

    fn manifest(marker: Option<GenerationFile>) -> GenerationManifest {
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 900,
            generation_id: "test".to_owned(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files: marker.into_iter().collect(),
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        manifest
    }

    fn marker_binding() -> GenerationFile {
        GenerationFile {
            name: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE.to_owned(),
            size: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE,
            sha256: COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256.to_owned(),
        }
    }

    fn write_marker(root: &Path, bytes: &[u8]) {
        std::fs::write(
            root.join(COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE),
            bytes,
        )
        .unwrap();
    }

    #[test]
    fn marker_constants_bind_the_exact_metadata_schema_bytes() {
        assert_eq!(
            COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES.len() as u64,
            COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE
        );
        assert_eq!(
            hex_lower_sha256(COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES),
            COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256
        );
    }

    #[test]
    fn absent_metadata_marker_selects_current_schema() {
        let dir = TempDir::new().unwrap();
        let source = LocalRangeSource::new(dir.path());
        assert_eq!(
            select_compact_v2_metadata_schema(&source, &manifest(None)).unwrap(),
            CompactV2MetadataSchema::CurrentTypedError
        );
    }

    #[test]
    fn legacy_metadata_marker_must_be_manifest_bound() {
        let dir = TempDir::new().unwrap();
        write_marker(dir.path(), COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES);
        let source = LocalRangeSource::new(dir.path());
        assert!(matches!(
            select_compact_v2_metadata_schema(&source, &manifest(None)).unwrap_err(),
            CompactV2MetadataSchemaError::MarkerNotManifestBound
        ));
        assert!(matches!(
            select_unpublished_compact_v2_metadata_schema(&source).unwrap_err(),
            CompactV2MetadataSchemaError::UnpublishedHistoricalMarker
        ));
    }

    #[test]
    fn bound_legacy_metadata_marker_checks_manifest_and_object() {
        let dir = TempDir::new().unwrap();
        write_marker(dir.path(), COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES);
        let source = LocalRangeSource::new(dir.path());
        assert_eq!(
            select_compact_v2_metadata_schema(&source, &manifest(Some(marker_binding()))).unwrap(),
            CompactV2MetadataSchema::LegacyRawError
        );

        let mut wrong_binding = marker_binding();
        wrong_binding.sha256 = "0".repeat(64);
        assert!(matches!(
            select_compact_v2_metadata_schema(&source, &manifest(Some(wrong_binding))).unwrap_err(),
            CompactV2MetadataSchemaError::MarkerManifestDigest { .. }
        ));

        let missing = TempDir::new().unwrap();
        assert!(matches!(
            select_compact_v2_metadata_schema(
                &LocalRangeSource::new(missing.path()),
                &manifest(Some(marker_binding())),
            )
            .unwrap_err(),
            CompactV2MetadataSchemaError::BoundMarkerMissing
        ));

        let corrupt = TempDir::new().unwrap();
        let mut bytes = COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES.to_vec();
        bytes[0] ^= 1;
        write_marker(corrupt.path(), &bytes);
        assert!(matches!(
            select_compact_v2_metadata_schema(
                &LocalRangeSource::new(corrupt.path()),
                &manifest(Some(marker_binding())),
            )
            .unwrap_err(),
            CompactV2MetadataSchemaError::MarkerObjectDigest { .. }
        ));
    }

    fn current_metadata(err: Option<CompactTransactionError>) -> CompactMetaV1 {
        CompactMetaV1 {
            err,
            fee: 5_000,
            pre_balances: vec![10, 20],
            post_balances: vec![9, 21],
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: Some(42),
            cost_units: Some(84),
        }
    }

    fn legacy_metadata(err: Option<Vec<u8>>) -> LegacyRawErrorCompactMetaV1 {
        let current = current_metadata(None);
        LegacyRawErrorCompactMetaV1 {
            err,
            fee: current.fee,
            pre_balances: current.pre_balances,
            post_balances: current.post_balances,
            inner_instructions: current.inner_instructions,
            logs: current.logs,
            pre_token_balances: current.pre_token_balances,
            post_token_balances: current.post_token_balances,
            rewards: current.rewards,
            loaded_writable_addresses: current.loaded_writable_addresses,
            loaded_readonly_addresses: current.loaded_readonly_addresses,
            return_data: current.return_data,
            compute_units_consumed: current.compute_units_consumed,
            cost_units: current.cost_units,
        }
    }

    #[test]
    fn selected_current_typed_error_schema_decodes_exactly() {
        let bytes = wincode::config::serialize(
            &current_metadata(Some(CompactTransactionError::InvalidProgramForExecution)),
            wincode_leb128_config(),
        )
        .unwrap();

        let metadata =
            decode_compact_v2_metadata(CompactV2MetadataSchema::CurrentTypedError, &bytes).unwrap();
        assert!(matches!(
            metadata.err,
            Some(CompactTransactionError::InvalidProgramForExecution)
        ));
        assert_eq!(metadata.fee, 5_000);
        assert_eq!(metadata.compute_units_consumed, Some(42));
        assert_eq!(metadata.cost_units, Some(84));
    }

    #[test]
    fn selected_legacy_raw_error_schema_decodes_and_types_stored_error() {
        // StoredTransactionError::InstructionError(
        //     0,
        //     StoredInstructionError::Custom(0),
        // ) in the historical default-Wincode encoding.
        let stored_error = vec![
            8, 0, 0, 0, // transaction-error enum tag
            0, // instruction index
            25, 0, 0, 0, // instruction-error enum tag
            0, 0, 0, 0, // custom error code
        ];
        let bytes = wincode::config::serialize(
            &legacy_metadata(Some(stored_error)),
            wincode_leb128_config(),
        )
        .unwrap();

        let metadata =
            decode_compact_v2_metadata(CompactV2MetadataSchema::LegacyRawError, &bytes).unwrap();
        assert!(matches!(
            metadata.err,
            Some(CompactTransactionError::InstructionError(
                0,
                CompactInstructionError::Custom(0)
            ))
        ));
        assert_eq!(metadata.fee, 5_000);
        assert_eq!(metadata.pre_balances, [10, 20]);
        assert_eq!(metadata.post_balances, [9, 21]);
        assert_eq!(metadata.compute_units_consumed, Some(42));
        assert_eq!(metadata.cost_units, Some(84));
    }

    #[test]
    fn epoch_900_legacy_raw_error_record_uses_the_selected_grammar() {
        // Epoch 900, source index row 2, transaction 5. This real record was
        // the first dense canary failure. Byte 0x90 is the fee after the
        // 13-byte stored error; a current typed-error decoder loses alignment
        // and later reports 0x90 as an invalid tag.
        let bytes = [
            0x01, 0x0d, 0x08, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x90, 0x4e, 0x03, 0xcc, 0xef, 0xf0, 0xf2, 0x32, 0x80, 0xf6, 0xed, 0xdf, 0x09,
            0x01, 0x03, 0xbc, 0xa1, 0xf0, 0xf2, 0x32, 0x80, 0xf6, 0xed, 0xdf, 0x09, 0x01, 0x01,
            0x00, 0x01, 0x02, 0x0e, 0x03, 0x01, 0x16, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xb4, 0x10, 0x01, 0xe4, 0x1a,
        ];

        let current_error =
            decode_compact_v2_metadata(CompactV2MetadataSchema::CurrentTypedError, &bytes)
                .unwrap_err();
        assert!(
            current_error
                .to_string()
                .contains("Invalid tag encoding: 144")
        );

        let metadata =
            decode_compact_v2_metadata(CompactV2MetadataSchema::LegacyRawError, &bytes).unwrap();
        assert!(matches!(
            metadata.err,
            Some(CompactTransactionError::InstructionError(
                0,
                CompactInstructionError::Custom(0)
            ))
        ));
        assert_eq!(metadata.fee, 10_000);
        assert_eq!(metadata.pre_balances, [13_662_697_420, 2_616_949_504, 1]);
        assert_eq!(metadata.post_balances, [13_662_687_420, 2_616_949_504, 1]);
        assert!(matches!(
            metadata.inner_instructions,
            Some(ref instructions) if instructions.is_empty()
        ));
        assert!(metadata.logs.is_some());
        assert!(metadata.pre_token_balances.is_empty());
        assert!(metadata.post_token_balances.is_empty());
        assert!(metadata.rewards.is_empty());
        assert!(metadata.loaded_writable_addresses.is_empty());
        assert!(metadata.loaded_readonly_addresses.is_empty());
        assert!(metadata.return_data.is_none());
        assert_eq!(metadata.compute_units_consumed, Some(2_100));
        assert_eq!(metadata.cost_units, Some(3_428));
    }

    #[test]
    fn schema_selection_is_not_inferred_per_record() {
        let none_bytes =
            wincode::config::serialize(&legacy_metadata(None), wincode_leb128_config()).unwrap();

        // `err == None` has the same bytes in both grammars. Both explicit
        // selections are valid; the decoder cannot use this record to infer a
        // generation-wide schema.
        decode_compact_v2_metadata(CompactV2MetadataSchema::CurrentTypedError, &none_bytes)
            .unwrap();
        decode_compact_v2_metadata(CompactV2MetadataSchema::LegacyRawError, &none_bytes).unwrap();
    }

    #[test]
    fn selected_schema_requires_the_complete_record() {
        let mut bytes = wincode::config::serialize(
            &legacy_metadata(Some(vec![0, 0, 0, 0])),
            wincode_leb128_config(),
        )
        .unwrap();
        bytes.push(0xff);

        assert!(
            decode_compact_v2_metadata(CompactV2MetadataSchema::LegacyRawError, &bytes).is_err()
        );
    }
}
