//! Generation-wide Archive V2 metadata error-schema authority.
//!
//! `CompactMetaV1` has one historical wire difference: a present transaction
//! error was stored as length-delimited `StoredTransactionError` bytes. The
//! current schema stores a typed `CompactTransactionError` directly. A marker
//! is authoritative only after every metadata record in the generation has
//! passed the exact classification and admission rules in this module.

use std::fmt;

use blockzilla_format::{
    ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ArchiveV2WireIdentityVisitor, ArchiveV2WireRewriteLimits, CompactInnerInstructions,
    CompactLogStream, CompactMetaV1, CompactPubkey, CompactReturnData, CompactReward,
    CompactTokenBalance, CompactTransactionError, bounded_wincode_leb128_config,
    rewrite_archive_v2_successful_metadata_wire, wincode_leb128_config,
};
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    ArchiveReader, Error, ProfiledGenerationBinding, RangeSource, Result,
    manifest::{GenerationFile, GenerationManifest},
};

/// The only metadata schema marker emitted by current producers.
pub const CURRENT_TYPED_ERRORS_MARKER_FILE: &str =
    "archive-v2-metadata-schema-current-typed-errors-v1.marker";
const METADATA_SCHEMA_MARKER_PREFIX: &str = "archive-v2-metadata-schema-";
pub const CURRENT_TYPED_ERRORS_MARKER_BYTES: &[u8] =
    b"blockzilla/archive-v2-metadata-schema/current-typed-errors/v1\n";
pub const CURRENT_TYPED_ERRORS_MARKER_SIZE: u64 = 62;
pub const CURRENT_TYPED_ERRORS_MARKER_SHA256: &str =
    "f49c05f2021856a66542da4b84e31b2567b653a113acd05e0a0791cd620f0305";

/// The metadata error grammar selected for one complete generation.
///
/// An unmarked generation does not prove one uniform grammar. The historical
/// variant is therefore an explicit compatibility mode, not a producer format
/// that new archives can publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArchiveV2MetadataWireProfile {
    CurrentTypedErrorsV1,
    UnmarkedHistoricalCompatibility,
}

impl ArchiveV2MetadataWireProfile {
    pub const CURRENT_NAME: &'static str = "current-typed-errors-v1";
    pub const HISTORICAL_COMPATIBILITY_NAME: &'static str = "unmarked-historical-compatibility";

    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::CurrentTypedErrorsV1 => Self::CURRENT_NAME,
            Self::UnmarkedHistoricalCompatibility => Self::HISTORICAL_COMPATIBILITY_NAME,
        }
    }

    /// Select a generation-wide profile from the manifest and an explicit
    /// admission policy. A present marker must always have the exact canonical
    /// name, size, and digest binding.
    pub fn for_manifest(
        manifest: &GenerationManifest,
        admission: ArchiveV2MetadataProfileAdmission,
    ) -> Result<Self> {
        manifest.validate()?;
        if let Some(unknown) = manifest.files.iter().find(|file| {
            is_metadata_schema_marker_name(&file.name)
                && file.name != CURRENT_TYPED_ERRORS_MARKER_FILE
        }) {
            return Err(Error::InvalidManifest(format!(
                "generation binds unsupported or conflicting metadata wire-profile marker {}",
                unknown.name
            )));
        }
        match manifest.file(CURRENT_TYPED_ERRORS_MARKER_FILE) {
            Some(marker) => {
                validate_current_marker_binding(marker)?;
                Ok(Self::CurrentTypedErrorsV1)
            }
            None if admission == ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical => {
                Ok(Self::UnmarkedHistoricalCompatibility)
            }
            None => Err(Error::InvalidManifest(format!(
                "generation has no authenticated current metadata wire-profile binding; publish the exact {CURRENT_TYPED_ERRORS_MARKER_FILE} marker or request unmarked historical compatibility explicitly"
            ))),
        }
    }

    /// Apply this profile to complete-generation classification counts.
    pub fn admit_counts(self, counts: ArchiveV2MetadataSchemaCounts) -> Result<()> {
        counts.checked_total()?;
        match self {
            Self::CurrentTypedErrorsV1 => {
                if counts.legacy_only != 0
                    || counts.both_different != 0
                    || counts.invalid != 0
                    || counts.raw_fallback != 0
                {
                    return Err(Error::InvalidMetadata(format!(
                        "current metadata marker rejects generation counts: legacy_only={}, both_different={}, invalid={}, raw_fallback={}",
                        counts.legacy_only,
                        counts.both_different,
                        counts.invalid,
                        counts.raw_fallback,
                    )));
                }
            }
            Self::UnmarkedHistoricalCompatibility => {
                if counts.both_different != 0 || counts.invalid != 0 {
                    return Err(Error::InvalidMetadata(format!(
                        "historical metadata compatibility rejects ambiguous or invalid typed records: both_different={}, invalid={}",
                        counts.both_different, counts.invalid,
                    )));
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for ArchiveV2MetadataWireProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.stable_name())
    }
}

/// Caller authority for an unmarked historical generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2MetadataProfileAdmission {
    /// Require the exact current marker. This is the publication and normal
    /// read policy for all newly normalized generations.
    RequireCurrentTypedErrors,
    /// Permit an unmarked old generation to use the dual exact decoder. This
    /// choice must be present at the call site.
    AllowUnmarkedHistorical,
}

/// Exact classification of one metadata record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveV2MetadataSchemaClassification {
    /// `err=None`; current and historical wire layouts are identical.
    NoError,
    /// Only the canonical current typed-error record is valid.
    CurrentOnly,
    /// Only the canonical historical raw-error record is valid.
    LegacyOnly,
    /// Both canonical grammars decode to the same current semantic value.
    BothEqual,
    /// Both canonical grammars decode but produce different semantic values.
    BothDifferent,
    /// Neither canonical grammar accepts the complete record.
    Invalid,
    /// The hot transaction row declares opaque metadata bytes.
    RawFallback,
}

/// Exact record counts for one complete generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveV2MetadataSchemaCounts {
    pub no_error: u64,
    pub current_only: u64,
    pub legacy_only: u64,
    pub both_equal: u64,
    pub both_different: u64,
    pub invalid: u64,
    pub raw_fallback: u64,
}

impl ArchiveV2MetadataSchemaCounts {
    pub fn checked_observe(
        &mut self,
        classification: ArchiveV2MetadataSchemaClassification,
    ) -> Result<()> {
        let counter = match classification {
            ArchiveV2MetadataSchemaClassification::NoError => &mut self.no_error,
            ArchiveV2MetadataSchemaClassification::CurrentOnly => &mut self.current_only,
            ArchiveV2MetadataSchemaClassification::LegacyOnly => &mut self.legacy_only,
            ArchiveV2MetadataSchemaClassification::BothEqual => &mut self.both_equal,
            ArchiveV2MetadataSchemaClassification::BothDifferent => &mut self.both_different,
            ArchiveV2MetadataSchemaClassification::Invalid => &mut self.invalid,
            ArchiveV2MetadataSchemaClassification::RawFallback => &mut self.raw_fallback,
        };
        *counter = counter
            .checked_add(1)
            .ok_or(Error::Overflow("metadata schema classification count"))?;
        Ok(())
    }

    pub fn checked_typed_total(self) -> Result<u64> {
        [
            self.no_error,
            self.current_only,
            self.legacy_only,
            self.both_equal,
            self.both_different,
            self.invalid,
        ]
        .into_iter()
        .try_fold(0u64, |total, count| {
            total
                .checked_add(count)
                .ok_or(Error::Overflow("typed metadata schema count total"))
        })
    }

    pub fn checked_total(self) -> Result<u64> {
        self.checked_typed_total()?
            .checked_add(self.raw_fallback)
            .ok_or(Error::Overflow("metadata schema count total"))
    }
}

/// Complete-generation facts produced by the exact scanner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullGenerationMetadataWireProfileAudit {
    pub blocks: u64,
    pub counts: ArchiveV2MetadataSchemaCounts,
}

/// Proof that one exact generation scan admitted the current metadata schema.
///
/// The private fields prevent a producer from creating publication authority
/// from manifest facts. A producer gets this value only from
/// [`audit_current_metadata_for_marker_publication`].
#[derive(Debug)]
pub struct AuditedCurrentMetadataMarkerPublication {
    source_binding: ProfiledGenerationBinding,
    audit: FullGenerationMetadataWireProfileAudit,
}

impl AuditedCurrentMetadataMarkerPublication {
    /// Identity of the complete source generation that was scanned.
    pub fn source_binding(&self) -> ProfiledGenerationBinding {
        self.source_binding
    }

    /// Exact complete-generation classification facts.
    pub fn audit(&self) -> FullGenerationMetadataWireProfileAudit {
        self.audit
    }

    /// Exact immutable bytes that this audit permits a producer to publish.
    pub fn marker_bytes(&self) -> &'static [u8] {
        CURRENT_TYPED_ERRORS_MARKER_BYTES
    }

    /// Exact manifest entry that this audit permits a producer to bind.
    pub fn marker_manifest_entry(&self) -> GenerationFile {
        current_typed_errors_marker()
    }
}

/// Reusable, allocation-light classifier.
///
/// The common `err=None` path uses the streaming wire validator and reuses one
/// output buffer. Only a present error runs the two bounded owned decoders.
#[derive(Debug, Default)]
pub struct ArchiveV2MetadataSchemaClassifier {
    current_canonical: Vec<u8>,
    legacy_canonical: Vec<u8>,
}

impl ArchiveV2MetadataSchemaClassifier {
    pub fn classify(&mut self, input: &[u8]) -> ArchiveV2MetadataSchemaClassification {
        match input.first().copied() {
            Some(0) => self.classify_no_error(input),
            Some(1) => self.classify_present_error(input),
            _ => ArchiveV2MetadataSchemaClassification::Invalid,
        }
    }

    fn classify_no_error(&mut self, input: &[u8]) -> ArchiveV2MetadataSchemaClassification {
        self.current_canonical.clear();
        let mut visitor = ArchiveV2WireIdentityVisitor;
        let limits = ArchiveV2WireRewriteLimits {
            max_input_bytes: input.len(),
            max_output_bytes: input.len(),
            ..ArchiveV2WireRewriteLimits::default()
        };
        if rewrite_archive_v2_successful_metadata_wire(
            input,
            &mut self.current_canonical,
            &mut visitor,
            limits,
        )
        .is_ok()
            && self.current_canonical == input
        {
            ArchiveV2MetadataSchemaClassification::NoError
        } else {
            ArchiveV2MetadataSchemaClassification::Invalid
        }
    }

    fn classify_present_error(&mut self, input: &[u8]) -> ArchiveV2MetadataSchemaClassification {
        if input.len() > ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES {
            return ArchiveV2MetadataSchemaClassification::Invalid;
        }

        let current_valid = self.decode_canonical_current(input);
        let legacy_valid = self.decode_canonical_legacy(input);
        match (current_valid, legacy_valid) {
            (true, false) => ArchiveV2MetadataSchemaClassification::CurrentOnly,
            (false, true) => ArchiveV2MetadataSchemaClassification::LegacyOnly,
            (true, true) if self.current_canonical == self.legacy_canonical => {
                ArchiveV2MetadataSchemaClassification::BothEqual
            }
            (true, true) => ArchiveV2MetadataSchemaClassification::BothDifferent,
            (false, false) => ArchiveV2MetadataSchemaClassification::Invalid,
        }
    }

    fn decode_canonical_current(&mut self, input: &[u8]) -> bool {
        self.current_canonical.clear();
        let Ok(value) = wincode::config::deserialize_exact::<CompactMetaV1, _>(
            input,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        ) else {
            return false;
        };
        if value.err.is_none()
            || wincode::config::serialize_into(
                &mut self.current_canonical,
                &value,
                wincode_leb128_config(),
            )
            .is_err()
        {
            return false;
        }
        self.current_canonical == input
    }

    fn decode_canonical_legacy(&mut self, input: &[u8]) -> bool {
        self.legacy_canonical.clear();
        let Ok(value) = wincode::config::deserialize_exact::<LegacyArchiveV2CompactMetaV1, _>(
            input,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        ) else {
            return false;
        };
        if value.err.is_none()
            || wincode::config::serialize_into(
                &mut self.legacy_canonical,
                &value,
                wincode_leb128_config(),
            )
            .is_err()
            || self.legacy_canonical != input
        {
            return false;
        }
        let Ok(value) = value.into_current() else {
            return false;
        };
        self.legacy_canonical.clear();
        wincode::config::serialize_into(&mut self.legacy_canonical, &value, wincode_leb128_config())
            .is_ok()
    }
}

/// Classify one complete metadata record with exact consumption and canonical
/// reserialization. Use [`ArchiveV2MetadataSchemaClassifier`] directly when a
/// scan can reuse its buffers across many records.
pub fn classify_archive_v2_metadata_schema_exact(
    input: &[u8],
) -> ArchiveV2MetadataSchemaClassification {
    ArchiveV2MetadataSchemaClassifier::default().classify(input)
}

/// Build the fixed binding after publication authority has been established.
/// Tests in this crate also use it to make authenticated reader fixtures.
pub(crate) fn current_typed_errors_marker() -> GenerationFile {
    GenerationFile {
        name: CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        size: CURRENT_TYPED_ERRORS_MARKER_SIZE,
        sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.to_owned(),
    }
}

pub(crate) fn is_metadata_schema_marker_name(name: &str) -> bool {
    name.starts_with(METADATA_SCHEMA_MARKER_PREFIX)
}

/// Scan and classify every metadata record in an already structurally admitted
/// generation. This function does not infer authority from record bytes.
pub fn audit_full_generation_metadata_wire_profile<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> Result<FullGenerationMetadataWireProfileAudit> {
    let mut audit = FullGenerationMetadataWireProfileAudit::default();
    let mut classifier = ArchiveV2MetadataSchemaClassifier::default();
    let mut blocks = reader.borrowed_blocks();
    while let Some(block) = blocks.next_block() {
        let block = block?;
        audit.blocks = audit
            .blocks
            .checked_add(1)
            .ok_or(Error::Overflow("metadata wire-profile block count"))?;
        for row in block.tx_rows() {
            let has_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
            let raw_fallback = row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
            let has_error_flag = row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
            if raw_fallback && !has_metadata {
                return Err(Error::InvalidMetadata(format!(
                    "slot {} transaction {} declares raw metadata without metadata",
                    block.header().slot,
                    row.tx_index,
                )));
            }
            if !has_metadata {
                if has_error_flag {
                    return Err(Error::InvalidMetadata(format!(
                        "slot {} transaction {} declares an error without metadata",
                        block.header().slot,
                        row.tx_index,
                    )));
                }
                continue;
            }
            let classification = if raw_fallback {
                if has_error_flag {
                    return Err(Error::InvalidMetadata(format!(
                        "slot {} transaction {} declares a typed error fact for opaque metadata",
                        block.header().slot,
                        row.tx_index,
                    )));
                }
                ArchiveV2MetadataSchemaClassification::RawFallback
            } else {
                let start = row.metadata_offset as usize;
                let end = start
                    .checked_add(row.metadata_len as usize)
                    .ok_or(Error::Overflow("metadata wire-profile record range"))?;
                let bytes =
                    block
                        .metadata_bytes()
                        .get(start..end)
                        .ok_or_else(|| Error::InvalidBlock {
                            slot: block.header().slot,
                            message: format!(
                                "metadata range for transaction {} is outside its block",
                                row.tx_index
                            ),
                        })?;
                let classification = classifier.classify(bytes);
                let bytes_have_error = bytes.first() == Some(&1);
                if bytes_have_error != has_error_flag {
                    return Err(Error::InvalidMetadata(format!(
                        "slot {} transaction {} error option disagrees with its row flag",
                        block.header().slot,
                        row.tx_index,
                    )));
                }
                classification
            };
            audit.counts.checked_observe(classification)?;
        }
    }
    if audit.blocks != reader.index().rows.len() as u64 {
        return Err(Error::InvalidMetadata(
            "metadata schema audit did not visit every validated block".into(),
        ));
    }
    if audit.counts.raw_fallback != reader.metadata_footer().metadata_raw_fallbacks {
        return Err(Error::InvalidMetadata(format!(
            "audited raw metadata fallback count {} differs from footer count {}",
            audit.counts.raw_fallback,
            reader.metadata_footer().metadata_raw_fallbacks,
        )));
    }
    audit.counts.checked_total()?;
    Ok(audit)
}

/// Scan an unmarked or marked candidate and create marker publication
/// authority only when every record is valid under the current schema.
///
/// This is the producer cutover API. The caller must keep the audited files
/// immutable until it publishes `marker_bytes()` and the returned manifest
/// binding. A later strict reopen authenticates that binding and selects the
/// current-only decoder.
pub fn audit_current_metadata_for_marker_publication<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> Result<AuditedCurrentMetadataMarkerPublication> {
    let audit = audit_full_generation_metadata_wire_profile(reader)?;
    ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1.admit_counts(audit.counts)?;
    Ok(AuditedCurrentMetadataMarkerPublication {
        source_binding: reader.profiled_binding(),
        audit,
    })
}

/// Resolve authority, scan the complete generation, and enforce the selected
/// profile. A current marker is never accepted from manifest facts alone.
pub fn audit_and_admit_full_generation_metadata_wire_profile<S: RangeSource>(
    reader: &ArchiveReader<S>,
    admission: ArchiveV2MetadataProfileAdmission,
) -> Result<(
    ArchiveV2MetadataWireProfile,
    FullGenerationMetadataWireProfileAudit,
)> {
    let profile = ArchiveV2MetadataWireProfile::for_manifest(reader.manifest(), admission)?;
    if profile != reader.metadata_wire_profile() {
        return Err(Error::InvalidMetadata(format!(
            "requested metadata profile {profile} differs from reader-bound profile {}",
            reader.metadata_wire_profile()
        )));
    }
    let audit = audit_and_admit_selected_metadata_wire_profile(reader)?;
    Ok((profile, audit))
}

/// Scan and enforce the metadata profile already bound to a reader. This is
/// the trusted-local audit path; its authority comes from the explicit trusted
/// identity, not from a synthetic manifest marker binding.
pub fn audit_and_admit_selected_metadata_wire_profile<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> Result<FullGenerationMetadataWireProfileAudit> {
    let audit = audit_full_generation_metadata_wire_profile(reader)?;
    reader.metadata_wire_profile().admit_counts(audit.counts)?;
    Ok(audit)
}

fn validate_current_marker_binding(marker: &GenerationFile) -> Result<()> {
    if marker.size != CURRENT_TYPED_ERRORS_MARKER_SIZE
        || marker.sha256 != CURRENT_TYPED_ERRORS_MARKER_SHA256
    {
        return Err(Error::InvalidManifest(format!(
            "malformed current metadata wire-profile binding: {CURRENT_TYPED_ERRORS_MARKER_FILE} must have size {CURRENT_TYPED_ERRORS_MARKER_SIZE} and sha256 {CURRENT_TYPED_ERRORS_MARKER_SHA256}, found size {} and sha256 {}",
            marker.size, marker.sha256,
        )));
    }
    Ok(())
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct LegacyArchiveV2CompactMetaV1 {
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

impl LegacyArchiveV2CompactMetaV1 {
    fn into_current(self) -> std::result::Result<CompactMetaV1, ()> {
        let err = self
            .err
            .as_deref()
            .map(CompactTransactionError::from_stored_wincode_bytes)
            .transpose()
            .map_err(|_| ())?;
        Ok(CompactMetaV1 {
            err,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions: self.inner_instructions,
            logs: self.logs,
            pre_token_balances: self.pre_token_balances,
            post_token_balances: self.post_token_balances,
            rewards: self.rewards,
            loaded_writable_addresses: self.loaded_writable_addresses,
            loaded_readonly_addresses: self.loaded_readonly_addresses,
            return_data: self.return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        })
    }
}

// Keep the duplicated historical tail schema coupled to the current field
// list. An added `CompactMetaV1` field must cause a compile-time review here.
#[allow(dead_code)]
fn assert_current_metadata_shape(value: CompactMetaV1) {
    let CompactMetaV1 {
        err: _,
        fee: _,
        pre_balances: _,
        post_balances: _,
        inner_instructions: _,
        logs: _,
        pre_token_balances: _,
        post_token_balances: _,
        rewards: _,
        loaded_writable_addresses: _,
        loaded_readonly_addresses: _,
        return_data: _,
        compute_units_consumed: _,
        cost_units: _,
    } = value;
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::manifest::{
        BLOCK_INDEX_FILE, BLOCKS_FILE, META_FILE, REGISTRY_FILE, compute_generation_digest,
    };

    fn current_metadata(err: Option<CompactTransactionError>) -> CompactMetaV1 {
        CompactMetaV1 {
            err,
            fee: 5_000,
            pre_balances: vec![10],
            post_balances: vec![9],
            inner_instructions: None,
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![],
            loaded_readonly_addresses: vec![],
            return_data: None,
            compute_units_consumed: Some(1),
            cost_units: None,
        }
    }

    fn legacy_metadata(err: Option<Vec<u8>>) -> LegacyArchiveV2CompactMetaV1 {
        let current = current_metadata(None);
        LegacyArchiveV2CompactMetaV1 {
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

    fn manifest() -> GenerationManifest {
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".into(),
            epoch: 900,
            generation_id: "metadata-profile-test".into(),
            generation_digest: "00".repeat(32),
            slots_per_epoch: 432_000,
            complete: true,
            files: [BLOCKS_FILE, BLOCK_INDEX_FILE, META_FILE, REGISTRY_FILE]
                .into_iter()
                .map(|name| GenerationFile {
                    name: name.into(),
                    size: 0,
                    sha256: "11".repeat(32),
                })
                .collect(),
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        manifest
    }

    fn refresh_digest(manifest: &mut GenerationManifest) {
        manifest.generation_digest = compute_generation_digest(manifest).unwrap();
    }

    #[test]
    fn fixed_marker_binding_matches_exact_bytes() {
        assert_eq!(CURRENT_TYPED_ERRORS_MARKER_BYTES.len() as u64, 62);
        let digest: [u8; 32] = Sha256::digest(CURRENT_TYPED_ERRORS_MARKER_BYTES).into();
        assert_eq!(
            crate::manifest::hex_lower(&digest),
            CURRENT_TYPED_ERRORS_MARKER_SHA256
        );
        assert_eq!(
            current_typed_errors_marker(),
            GenerationFile {
                name: CURRENT_TYPED_ERRORS_MARKER_FILE.into(),
                size: CURRENT_TYPED_ERRORS_MARKER_SIZE,
                sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.into(),
            }
        );
    }

    #[test]
    fn manifest_admission_is_strict_and_historical_compatibility_is_explicit() {
        let mut unmarked = manifest();
        assert!(
            ArchiveV2MetadataWireProfile::for_manifest(
                &unmarked,
                ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
            )
            .is_err()
        );
        assert_eq!(
            ArchiveV2MetadataWireProfile::for_manifest(
                &unmarked,
                ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
            )
            .unwrap(),
            ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
        );

        unmarked.files.push(current_typed_errors_marker());
        refresh_digest(&mut unmarked);
        for admission in [
            ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
            ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
        ] {
            assert_eq!(
                ArchiveV2MetadataWireProfile::for_manifest(&unmarked, admission).unwrap(),
                ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
            );
        }

        let marker = unmarked
            .files
            .iter_mut()
            .find(|file| file.name == CURRENT_TYPED_ERRORS_MARKER_FILE)
            .unwrap();
        marker.size += 1;
        refresh_digest(&mut unmarked);
        assert!(
            ArchiveV2MetadataWireProfile::for_manifest(
                &unmarked,
                ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
            )
            .is_err()
        );

        let mut unknown = manifest();
        unknown.files.push(GenerationFile {
            name: "archive-v2-metadata-schema-future-v2.marker".into(),
            size: 0,
            sha256: "22".repeat(32),
        });
        refresh_digest(&mut unknown);
        assert!(
            ArchiveV2MetadataWireProfile::for_manifest(
                &unknown,
                ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
            )
            .is_err()
        );
    }

    #[test]
    fn classifier_separates_common_current_legacy_and_invalid_records() {
        let no_error =
            wincode::config::serialize(&current_metadata(None), wincode_leb128_config()).unwrap();
        let current = wincode::config::serialize(
            &current_metadata(Some(CompactTransactionError::AccountInUse)),
            wincode_leb128_config(),
        )
        .unwrap();
        // StoredTransactionError::AccountInUse has fixed-width tag zero.
        let legacy = wincode::config::serialize(
            &legacy_metadata(Some(vec![0, 0, 0, 0])),
            wincode_leb128_config(),
        )
        .unwrap();

        let mut classifier = ArchiveV2MetadataSchemaClassifier::default();
        assert_eq!(
            classifier.classify(&no_error),
            ArchiveV2MetadataSchemaClassification::NoError
        );
        assert_eq!(
            classifier.classify(&current),
            ArchiveV2MetadataSchemaClassification::CurrentOnly
        );
        assert_eq!(
            classifier.classify(&legacy),
            ArchiveV2MetadataSchemaClassification::LegacyOnly
        );
        assert_eq!(
            classifier.classify(&current[..current.len() - 1]),
            ArchiveV2MetadataSchemaClassification::Invalid
        );
        assert_eq!(
            classify_archive_v2_metadata_schema_exact(&no_error),
            ArchiveV2MetadataSchemaClassification::NoError
        );
    }

    #[test]
    fn warmed_common_path_reuses_its_wire_buffer() {
        let bytes =
            wincode::config::serialize(&current_metadata(None), wincode_leb128_config()).unwrap();
        let mut classifier = ArchiveV2MetadataSchemaClassifier::default();
        assert_eq!(
            classifier.classify(&bytes),
            ArchiveV2MetadataSchemaClassification::NoError
        );
        let (classification, allocations) =
            crate::test_allocations::count_current_thread_allocations(|| {
                classifier.classify(&bytes)
            });
        assert_eq!(
            classification,
            ArchiveV2MetadataSchemaClassification::NoError
        );
        assert_eq!(allocations, 0);
    }

    #[test]
    fn count_admission_rejects_non_current_and_ambiguous_records() {
        let current = ArchiveV2MetadataSchemaCounts {
            no_error: 10,
            current_only: 2,
            both_equal: 1,
            ..ArchiveV2MetadataSchemaCounts::default()
        };
        ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
            .admit_counts(current)
            .unwrap();

        for rejected in [
            ArchiveV2MetadataSchemaClassification::LegacyOnly,
            ArchiveV2MetadataSchemaClassification::BothDifferent,
            ArchiveV2MetadataSchemaClassification::Invalid,
            ArchiveV2MetadataSchemaClassification::RawFallback,
        ] {
            let mut counts = current;
            counts.checked_observe(rejected).unwrap();
            assert!(
                ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
                    .admit_counts(counts)
                    .is_err()
            );
        }

        let historical = ArchiveV2MetadataSchemaCounts {
            no_error: 10,
            current_only: 1,
            legacy_only: 2,
            both_equal: 1,
            raw_fallback: 3,
            ..ArchiveV2MetadataSchemaCounts::default()
        };
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
            .admit_counts(historical)
            .unwrap();
        for rejected in [
            ArchiveV2MetadataSchemaClassification::BothDifferent,
            ArchiveV2MetadataSchemaClassification::Invalid,
        ] {
            let mut counts = historical;
            counts.checked_observe(rejected).unwrap();
            assert!(
                ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility
                    .admit_counts(counts)
                    .is_err()
            );
        }
    }
}
