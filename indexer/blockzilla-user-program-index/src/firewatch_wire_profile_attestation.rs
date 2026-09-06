//! Shared schema and structural validation for Firewatch wire-profile attestations.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
};
use blockzilla_read_sdk_legacy::ArchiveV2WireProfile;
use serde::{Deserialize, Serialize};

use crate::format::RegistryFileIdentity;

pub const WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION: u32 = 2;
pub const WIRE_PROFILE_ATTESTATION_KIND: &str = "archive_v2_wire_profile_attestation";
pub const WIRE_PROFILE_AUDIT_ALGORITHM: &str =
    "archive-v2-borrowed-dual-profile-full-generation-v2";
pub const WIRE_PROFILE_AUDITED_PROFILES: [ArchiveV2WireProfile; 2] = [
    ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
];
pub const DIRECT_ATTESTATION_GENERATION_KIND: &str = "direct-file-identity-v1";
pub const RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND: &str = "registry-receipt-source-files-v1";
pub const RECEIPT_TARGET_ATTESTATION_GENERATION_KIND: &str = "registry-receipt-target-files-v1";
pub const FULL_GENERATION_AUDIT_EVIDENCE_V3: &str = "full-generation-borrowed-dual-sdk-audit-v3";
/// Canonical marker for a generation whose two historical grammars are
/// semantically equivalent for every typed message.
pub const ALL_EQUIVALENT_CANONICAL_WIRE_PROFILE: ArchiveV2WireProfile =
    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullGenerationAuditDecisionV3 {
    ProducerMarker,
    ProfileBoundReceipt,
    UniqueFullGenerationDecode,
    AllSemanticallyEquivalent,
}

impl FullGenerationAuditDecisionV3 {
    pub const fn stable_id(self) -> &'static str {
        match self {
            Self::ProducerMarker => "producer_marker",
            Self::ProfileBoundReceipt => "profile_bound_receipt",
            Self::UniqueFullGenerationDecode => "unique_full_generation_decode",
            Self::AllSemanticallyEquivalent => "all_semantically_equivalent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullGenerationAuditEvidenceV3 {
    pub generation_kind: String,
    pub blocks: u64,
    pub messages: u64,
    pub raw_transaction_fallbacks: u64,
    pub alternate_profile_failures: u64,
    pub both_semantically_equivalent: u64,
    pub both_semantically_divergent: u64,
    pub decision: FullGenerationAuditDecisionV3,
}

/// Encode the exact evidence format emitted by the full-generation producer.
pub fn encode_full_generation_audit_evidence_v3(
    evidence: &FullGenerationAuditEvidenceV3,
) -> Result<String> {
    ensure!(
        matches!(
            evidence.generation_kind.as_str(),
            DIRECT_ATTESTATION_GENERATION_KIND
                | RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND
                | RECEIPT_TARGET_ATTESTATION_GENERATION_KIND
        ),
        "full-generation evidence generation kind is invalid"
    );
    ensure!(
        evidence.blocks > 0 && evidence.messages > 0,
        "full-generation evidence is empty"
    );
    ensure!(
        evidence
            .alternate_profile_failures
            .checked_add(evidence.both_semantically_equivalent)
            .and_then(|value| value.checked_add(evidence.both_semantically_divergent))
            == Some(evidence.messages),
        "full-generation evidence classification is incomplete"
    );
    if evidence.generation_kind == RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND {
        ensure!(
            evidence.raw_transaction_fallbacks == 0,
            "receipt-source evidence has raw transaction fallbacks"
        );
        match evidence.decision {
            FullGenerationAuditDecisionV3::UniqueFullGenerationDecode => ensure!(
                evidence.alternate_profile_failures > 0,
                "receipt-source unique decision has no alternate-profile rejection"
            ),
            FullGenerationAuditDecisionV3::AllSemanticallyEquivalent => ensure!(
                evidence.alternate_profile_failures == 0
                    && evidence.both_semantically_divergent == 0
                    && evidence.both_semantically_equivalent == evidence.messages,
                "receipt-source all-equivalent decision is ambiguous"
            ),
            FullGenerationAuditDecisionV3::ProducerMarker
            | FullGenerationAuditDecisionV3::ProfileBoundReceipt => {
                bail!("receipt-source evidence must use a neutral dual-profile decision")
            }
        }
    }
    match evidence.decision {
        FullGenerationAuditDecisionV3::UniqueFullGenerationDecode => ensure!(
            evidence.alternate_profile_failures > 0,
            "unique full-generation decision has no alternate-profile rejection"
        ),
        FullGenerationAuditDecisionV3::AllSemanticallyEquivalent => ensure!(
            evidence.alternate_profile_failures == 0
                && evidence.both_semantically_divergent == 0
                && evidence.both_semantically_equivalent == evidence.messages,
            "all-equivalent decision contains ambiguous or divergent evidence"
        ),
        FullGenerationAuditDecisionV3::ProducerMarker
        | FullGenerationAuditDecisionV3::ProfileBoundReceipt => {}
    }
    Ok(format!(
        "{FULL_GENERATION_AUDIT_EVIDENCE_V3};generation_kind={};blocks={};messages={};raw_transaction_fallbacks={};selected_profile_failures=0;alternate_profile_failures={};both_semantically_equivalent={};both_semantically_divergent={};decision_basis={};pinned_inputs_unchanged=true;exact_input_before_after_equal=true",
        evidence.generation_kind,
        evidence.blocks,
        evidence.messages,
        evidence.raw_transaction_fallbacks,
        evidence.alternate_profile_failures,
        evidence.both_semantically_equivalent,
        evidence.both_semantically_divergent,
        evidence.decision.stable_id(),
    ))
}

/// Decode and validate the exact schema-v3 full-generation evidence format.
///
/// This parser accepts all three generation kinds and all four decision
/// variants. A use point must apply its own authority rule after this exact
/// structural check.
pub fn decode_full_generation_audit_evidence_v3(
    evidence: &str,
) -> Result<FullGenerationAuditEvidenceV3> {
    let mut fields = evidence.split(';');
    ensure!(
        fields.next() == Some(FULL_GENERATION_AUDIT_EVIDENCE_V3),
        "attestation evidence is not the full-generation v3 contract"
    );
    let mut values = BTreeMap::new();
    for field in fields {
        let (name, value) = field
            .split_once('=')
            .context("attestation evidence field has no value")?;
        ensure!(
            !name.is_empty() && !value.is_empty() && values.insert(name, value).is_none(),
            "attestation evidence has an empty or duplicate field"
        );
    }
    let exact_keys = BTreeSet::from([
        "generation_kind",
        "blocks",
        "messages",
        "raw_transaction_fallbacks",
        "selected_profile_failures",
        "alternate_profile_failures",
        "both_semantically_equivalent",
        "both_semantically_divergent",
        "decision_basis",
        "pinned_inputs_unchanged",
        "exact_input_before_after_equal",
    ]);
    ensure!(
        values.keys().copied().collect::<BTreeSet<_>>() == exact_keys,
        "attestation evidence field set is not exact"
    );
    ensure!(
        values["selected_profile_failures"] == "0"
            && values["pinned_inputs_unchanged"] == "true"
            && values["exact_input_before_after_equal"] == "true",
        "attestation evidence does not prove a strict unchanged selected-profile audit"
    );
    let decision = match values["decision_basis"] {
        "producer_marker" => FullGenerationAuditDecisionV3::ProducerMarker,
        "profile_bound_receipt" => FullGenerationAuditDecisionV3::ProfileBoundReceipt,
        "unique_full_generation_decode" => {
            FullGenerationAuditDecisionV3::UniqueFullGenerationDecode
        }
        "all_semantically_equivalent" => FullGenerationAuditDecisionV3::AllSemanticallyEquivalent,
        _ => bail!("attestation evidence decision is invalid"),
    };
    let decoded = FullGenerationAuditEvidenceV3 {
        generation_kind: values["generation_kind"].to_owned(),
        blocks: parse_evidence_count(&values, "blocks")?,
        messages: parse_evidence_count(&values, "messages")?,
        raw_transaction_fallbacks: parse_evidence_count(&values, "raw_transaction_fallbacks")?,
        alternate_profile_failures: parse_evidence_count(&values, "alternate_profile_failures")?,
        both_semantically_equivalent: parse_evidence_count(
            &values,
            "both_semantically_equivalent",
        )?,
        both_semantically_divergent: parse_evidence_count(&values, "both_semantically_divergent")?,
        decision,
    };
    ensure!(
        encode_full_generation_audit_evidence_v3(&decoded)? == evidence,
        "attestation evidence is not canonical"
    );
    Ok(decoded)
}

/// Validate the only direct-generation evidence that can authorize a marker
/// transition without circular producer provenance.
pub fn validate_neutral_direct_generation_evidence(
    evidence: &str,
    selected_profile: ArchiveV2WireProfile,
) -> Result<FullGenerationAuditEvidenceV3> {
    let decoded = decode_full_generation_audit_evidence_v3(evidence)?;
    ensure!(
        decoded.generation_kind == DIRECT_ATTESTATION_GENERATION_KIND,
        "marker transition authority is not a direct-generation audit"
    );
    ensure!(
        decoded.raw_transaction_fallbacks == 0,
        "marker transition authority has raw transaction fallbacks"
    );
    match decoded.decision {
        FullGenerationAuditDecisionV3::UniqueFullGenerationDecode => {}
        FullGenerationAuditDecisionV3::AllSemanticallyEquivalent => ensure!(
            selected_profile == ALL_EQUIVALENT_CANONICAL_WIRE_PROFILE,
            "all-equivalent direct generation must use the canonical Post profile"
        ),
        FullGenerationAuditDecisionV3::ProducerMarker
        | FullGenerationAuditDecisionV3::ProfileBoundReceipt => {
            bail!("marker transition authority uses circular producer provenance")
        }
    }
    Ok(decoded)
}

/// Produce the only full-generation evidence that can authorize a receipt-
/// source recovery. The decision is derived from the neutral dual-profile
/// result. A receipt or marker cannot select it.
pub fn encode_receipt_source_recovery_evidence_v3(
    blocks: u64,
    messages: u64,
    raw_transaction_fallbacks: u64,
    alternate_profile_failures: u64,
    both_semantically_equivalent: u64,
    both_semantically_divergent: u64,
) -> Result<String> {
    ensure!(
        raw_transaction_fallbacks == 0,
        "receipt-source recovery has raw transaction fallbacks"
    );
    let decision = if alternate_profile_failures > 0 {
        FullGenerationAuditDecisionV3::UniqueFullGenerationDecode
    } else {
        ensure!(
            both_semantically_divergent == 0 && both_semantically_equivalent == messages,
            "receipt-source recovery is ambiguous between wire profiles"
        );
        FullGenerationAuditDecisionV3::AllSemanticallyEquivalent
    };
    let encoded = encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
        generation_kind: RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into(),
        blocks,
        messages,
        raw_transaction_fallbacks,
        alternate_profile_failures,
        both_semantically_equivalent,
        both_semantically_divergent,
        decision,
    })?;
    validate_receipt_source_recovery_evidence(&encoded)?;
    Ok(encoded)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireProfileAttestation {
    pub schema_version: u32,
    pub kind: String,
    pub audit_algorithm: String,
    pub audited_profiles: [ArchiveV2WireProfile; 2],
    pub cluster_id: String,
    pub epoch: u64,
    pub archive: PathBuf,
    pub registry_order: String,
    pub generation_kind: String,
    pub content_generation_sha256: String,
    pub archive_files: BTreeMap<String, RegistryFileIdentity>,
    pub wire_profile: ArchiveV2WireProfile,
    pub evidence: String,
    pub attested_unix_secs: u64,
}

/// Validate fields that are independent of the current filesystem snapshot.
///
/// Callers must additionally bind `archive_files` to the live generation and
/// validate the canonical archive path at the point of use.
pub fn validate_wire_profile_attestation_structure(
    attestation: &WireProfileAttestation,
) -> Result<()> {
    ensure!(
        attestation.schema_version == WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
        "wire-profile attestation schema is not version 2"
    );
    ensure!(
        attestation.kind == WIRE_PROFILE_ATTESTATION_KIND,
        "wire-profile attestation kind is invalid"
    );
    ensure!(
        attestation.audit_algorithm == WIRE_PROFILE_AUDIT_ALGORITHM,
        "wire-profile attestation audit algorithm is invalid"
    );
    ensure!(
        attestation.audited_profiles == WIRE_PROFILE_AUDITED_PROFILES,
        "wire-profile attestation audited profile set is invalid"
    );
    ensure!(
        attestation.cluster_id == "mainnet-beta",
        "wire-profile attestation cluster is invalid"
    );
    ensure!(
        matches!(
            attestation.registry_order.as_str(),
            "first_seen" | "usage_sorted"
        ),
        "wire-profile attestation registry order is invalid"
    );
    ensure!(
        matches!(
            attestation.generation_kind.as_str(),
            DIRECT_ATTESTATION_GENERATION_KIND
                | RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND
                | RECEIPT_TARGET_ATTESTATION_GENERATION_KIND
        ),
        "wire-profile attestation generation kind is invalid"
    );
    ensure!(
        match attestation.generation_kind.as_str() {
            RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND => {
                attestation.registry_order == "first_seen"
            }
            RECEIPT_TARGET_ATTESTATION_GENERATION_KIND => {
                attestation.registry_order == "usage_sorted"
            }
            DIRECT_ATTESTATION_GENERATION_KIND => true,
            _ => false,
        },
        "wire-profile attestation registry order differs from its generation kind"
    );
    ensure!(
        is_sha256(&attestation.content_generation_sha256),
        "wire-profile attestation content generation is invalid"
    );
    ensure!(
        !attestation.archive_files.is_empty(),
        "wire-profile attestation file binding is empty"
    );
    ensure!(
        attestation
            .archive_files
            .contains_key(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            && attestation
                .archive_files
                .contains_key(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE),
        "wire-profile attestation must bind registry.bin and registry.mphf"
    );
    ensure!(
        attestation.archive_files.keys().all(|name| {
            !name.is_empty()
                && std::path::Path::new(name).components().count() == 1
                && name != "."
                && name != ".."
        }),
        "wire-profile attestation contains an invalid file name"
    );
    ensure!(
        attestation.archive_files.iter().all(|(name, identity)| {
            (identity.size > 0 || name == ARCHIVE_V2_SIGNATURES_FILE)
                && identity.device > 0
                && identity.inode > 0
                && (0..1_000_000_000).contains(&identity.modified_nanoseconds)
                && (0..1_000_000_000).contains(&identity.changed_nanoseconds)
        }),
        "wire-profile attestation contains invalid file timestamps"
    );
    ensure!(
        attestation.archive.is_absolute(),
        "wire-profile attestation archive is not absolute"
    );
    ensure!(
        attestation.attested_unix_secs > 0,
        "wire-profile attestation has no time"
    );
    ensure!(
        !attestation.evidence.is_empty()
            && attestation.evidence.len() <= 1_024
            && !attestation.evidence.chars().any(char::is_control),
        "wire-profile attestation evidence is invalid"
    );
    match attestation.generation_kind.as_str() {
        DIRECT_ATTESTATION_GENERATION_KIND => {
            validate_neutral_direct_generation_evidence(
                &attestation.evidence,
                attestation.wire_profile,
            )?;
        }
        RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND => {
            validate_receipt_source_recovery_evidence(&attestation.evidence)?;
            let evidence = decode_full_generation_audit_evidence_v3(&attestation.evidence)?;
            if evidence.decision == FullGenerationAuditDecisionV3::AllSemanticallyEquivalent {
                ensure!(
                    attestation.wire_profile == ALL_EQUIVALENT_CANONICAL_WIRE_PROFILE,
                    "an all-equivalent receipt source must use the canonical Post profile"
                );
            }
        }
        RECEIPT_TARGET_ATTESTATION_GENERATION_KIND => {
            let evidence = decode_full_generation_audit_evidence_v3(&attestation.evidence)?;
            ensure!(
                evidence.generation_kind == RECEIPT_TARGET_ATTESTATION_GENERATION_KIND
                    && evidence.decision == FullGenerationAuditDecisionV3::ProfileBoundReceipt,
                "receipt-target evidence is not bound to a registry-reprocess receipt"
            );
        }
        _ => unreachable!("generation kind was validated above"),
    }
    Ok(())
}

/// Validate the exact schema-v3 full-generation evidence that can certify a
/// receipt-source recovery. This contract proves that Post decoded the full
/// pinned generation without raw fallbacks and that the dual-profile result
/// was not ambiguous.
pub fn validate_receipt_source_recovery_evidence(evidence: &str) -> Result<()> {
    let mut fields = evidence.split(';');
    ensure!(
        fields.next() == Some(FULL_GENERATION_AUDIT_EVIDENCE_V3),
        "receipt-source attestation evidence is not the full-generation v3 contract"
    );
    let mut values = BTreeMap::new();
    for field in fields {
        let (name, value) = field
            .split_once('=')
            .context("receipt-source attestation evidence field has no value")?;
        ensure!(
            !name.is_empty() && !value.is_empty() && values.insert(name, value).is_none(),
            "receipt-source attestation evidence has an empty or duplicate field"
        );
    }
    let exact_keys = BTreeSet::from([
        "generation_kind",
        "blocks",
        "messages",
        "raw_transaction_fallbacks",
        "selected_profile_failures",
        "alternate_profile_failures",
        "both_semantically_equivalent",
        "both_semantically_divergent",
        "decision_basis",
        "pinned_inputs_unchanged",
        "exact_input_before_after_equal",
    ]);
    ensure!(
        values.keys().copied().collect::<BTreeSet<_>>() == exact_keys,
        "receipt-source attestation evidence field set is not exact"
    );
    ensure!(
        values["generation_kind"] == RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND
            && values["raw_transaction_fallbacks"] == "0"
            && values["selected_profile_failures"] == "0"
            && values["pinned_inputs_unchanged"] == "true"
            && values["exact_input_before_after_equal"] == "true",
        "receipt-source attestation evidence does not prove a strict unchanged Post audit"
    );
    let blocks = parse_evidence_count(&values, "blocks")?;
    let messages = parse_evidence_count(&values, "messages")?;
    let alternate = parse_evidence_count(&values, "alternate_profile_failures")?;
    let equivalent = parse_evidence_count(&values, "both_semantically_equivalent")?;
    let divergent = parse_evidence_count(&values, "both_semantically_divergent")?;
    ensure!(
        blocks > 0 && messages > 0,
        "receipt-source audit evidence is empty"
    );
    ensure!(
        alternate
            .checked_add(equivalent)
            .and_then(|value| value.checked_add(divergent))
            == Some(messages),
        "receipt-source audit evidence classification is incomplete"
    );
    match values["decision_basis"] {
        "unique_full_generation_decode" => ensure!(
            alternate > 0,
            "unique full-generation decision has no alternate-profile rejection"
        ),
        "all_semantically_equivalent" => ensure!(
            alternate == 0 && divergent == 0 && equivalent == messages,
            "all-equivalent decision contains ambiguous or divergent evidence"
        ),
        _ => bail!("receipt-source audit evidence decision is not recovery-safe"),
    }
    Ok(())
}

fn parse_evidence_count(values: &BTreeMap<&str, &str>, name: &str) -> Result<u64> {
    let text = values
        .get(name)
        .context("receipt-source audit evidence count is missing")?;
    let value = text
        .parse::<u64>()
        .with_context(|| format!("receipt-source audit evidence count is invalid: {name}"))?;
    ensure!(
        *text == value.to_string(),
        "receipt-source audit evidence count is not canonical: {name}"
    );
    Ok(value)
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> WireProfileAttestation {
        let evidence = encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 1,
            raw_transaction_fallbacks: 0,
            alternate_profile_failures: 0,
            both_semantically_equivalent: 1,
            both_semantically_divergent: 0,
            decision: FullGenerationAuditDecisionV3::AllSemanticallyEquivalent,
        })
        .unwrap();
        WireProfileAttestation {
            schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
            kind: WIRE_PROFILE_ATTESTATION_KIND.into(),
            audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
            audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
            cluster_id: "mainnet-beta".into(),
            epoch: 700,
            archive: "/archive/epoch-700".into(),
            registry_order: "usage_sorted".into(),
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            content_generation_sha256: "a".repeat(64),
            archive_files: BTreeMap::from([
                (
                    "archive-v2-blocks.zstd".into(),
                    RegistryFileIdentity {
                        size: 1,
                        device: 2,
                        inode: 3,
                        modified_seconds: 4,
                        modified_nanoseconds: 5,
                        changed_seconds: 6,
                        changed_nanoseconds: 7,
                    },
                ),
                (
                    ARCHIVE_V2_PUBKEY_REGISTRY_FILE.into(),
                    RegistryFileIdentity {
                        size: 32,
                        device: 2,
                        inode: 4,
                        modified_seconds: 4,
                        modified_nanoseconds: 5,
                        changed_seconds: 6,
                        changed_nanoseconds: 7,
                    },
                ),
                (
                    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE.into(),
                    RegistryFileIdentity {
                        size: 64,
                        device: 2,
                        inode: 5,
                        modified_seconds: 4,
                        modified_nanoseconds: 5,
                        changed_seconds: 6,
                        changed_nanoseconds: 7,
                    },
                ),
            ]),
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            evidence,
            attested_unix_secs: 1,
        }
    }

    fn valid_receipt_source_evidence() -> String {
        encode_receipt_source_recovery_evidence_v3(1, 2, 0, 0, 2, 0).unwrap()
    }

    #[test]
    fn accepts_exact_schema_two_shape() {
        validate_wire_profile_attestation_structure(&valid()).unwrap();
    }

    #[test]
    fn rejects_pre_hardening_algorithm_and_unsafe_file_name() {
        let mut value = valid();
        value.audit_algorithm = "archive-v2-borrowed-dual-profile-full-generation-v1".into();
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
        let mut value = valid();
        value.archive_files = BTreeMap::from([(
            "../escape".into(),
            value.archive_files.values().next().unwrap().clone(),
        )]);
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
    }

    #[test]
    fn attestation_requires_both_registry_mapping_files() {
        let mut value = valid();
        value
            .archive_files
            .remove(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        assert!(validate_wire_profile_attestation_structure(&value).is_err());

        let mut value = valid();
        value.archive_files.remove(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
    }

    #[test]
    fn permits_an_empty_archive_signatures_file_only() {
        let mut value = valid();
        let mut identity = value.archive_files.values().next().unwrap().clone();
        identity.size = 0;
        value
            .archive_files
            .insert(ARCHIVE_V2_SIGNATURES_FILE.into(), identity.clone());
        validate_wire_profile_attestation_structure(&value).unwrap();
        value
            .archive_files
            .insert("archive-v2-blocks.zstd".into(), identity);
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
    }

    #[test]
    fn receipt_source_requires_exact_full_generation_v3_recovery_evidence() {
        let mut value = valid();
        value.registry_order = "first_seen".into();
        value.generation_kind = RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into();
        value.evidence = valid_receipt_source_evidence();
        validate_wire_profile_attestation_structure(&value).unwrap();

        value.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
        value.wire_profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;

        value.evidence = "arbitrary printable evidence".into();
        assert!(validate_wire_profile_attestation_structure(&value).is_err());

        value.evidence = valid_receipt_source_evidence().replace(
            "decision_basis=all_semantically_equivalent",
            "decision_basis=profile_bound_receipt",
        );
        assert!(validate_wire_profile_attestation_structure(&value).is_err());

        value.evidence = valid_receipt_source_evidence().replace(
            "both_semantically_equivalent=2",
            "both_semantically_equivalent=1",
        );
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
    }

    #[test]
    fn receipt_target_requires_exact_profile_bound_evidence() {
        let mut value = valid();
        value.generation_kind = RECEIPT_TARGET_ATTESTATION_GENERATION_KIND.into();
        value.evidence = encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: RECEIPT_TARGET_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 1,
            raw_transaction_fallbacks: 0,
            alternate_profile_failures: 0,
            both_semantically_equivalent: 1,
            both_semantically_divergent: 0,
            decision: FullGenerationAuditDecisionV3::ProfileBoundReceipt,
        })
        .unwrap();
        validate_wire_profile_attestation_structure(&value).unwrap();

        value.evidence = "arbitrary printable evidence".into();
        assert!(validate_wire_profile_attestation_structure(&value).is_err());
    }

    #[test]
    fn receipt_source_producer_rejects_provenance_and_ambiguous_decisions() {
        for decision in [
            FullGenerationAuditDecisionV3::ProducerMarker,
            FullGenerationAuditDecisionV3::ProfileBoundReceipt,
        ] {
            assert!(
                encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
                    generation_kind: RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into(),
                    blocks: 1,
                    messages: 1,
                    raw_transaction_fallbacks: 0,
                    alternate_profile_failures: 0,
                    both_semantically_equivalent: 1,
                    both_semantically_divergent: 0,
                    decision,
                })
                .is_err()
            );
        }
        assert!(encode_receipt_source_recovery_evidence_v3(1, 1, 0, 0, 0, 1).is_err());
        assert!(encode_receipt_source_recovery_evidence_v3(1, 1, 1, 0, 1, 0).is_err());
    }

    fn direct_evidence(decision: FullGenerationAuditDecisionV3) -> String {
        let (alternate, equivalent, divergent) = match decision {
            FullGenerationAuditDecisionV3::UniqueFullGenerationDecode => (1, 0, 1),
            FullGenerationAuditDecisionV3::AllSemanticallyEquivalent => (0, 2, 0),
            FullGenerationAuditDecisionV3::ProducerMarker
            | FullGenerationAuditDecisionV3::ProfileBoundReceipt => (0, 1, 1),
        };
        encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 2,
            raw_transaction_fallbacks: 0,
            alternate_profile_failures: alternate,
            both_semantically_equivalent: equivalent,
            both_semantically_divergent: divergent,
            decision,
        })
        .unwrap()
    }

    #[test]
    fn exact_full_generation_evidence_round_trips_and_rejects_noncanonical_fields() {
        let evidence = direct_evidence(FullGenerationAuditDecisionV3::UniqueFullGenerationDecode);
        let decoded = decode_full_generation_audit_evidence_v3(&evidence).unwrap();
        assert_eq!(
            decoded.decision,
            FullGenerationAuditDecisionV3::UniqueFullGenerationDecode
        );
        assert_eq!(decoded.messages, 2);

        assert!(
            decode_full_generation_audit_evidence_v3(
                &evidence.replace(";blocks=1;", ";blocks=01;")
            )
            .is_err()
        );
        assert!(
            decode_full_generation_audit_evidence_v3(&format!("{evidence};extra=true")).is_err()
        );
        assert!(
            decode_full_generation_audit_evidence_v3(
                &evidence.replace(";messages=2;", ";messages=2;messages=2;")
            )
            .is_err()
        );
    }

    #[test]
    fn direct_marker_transition_rejects_circular_authority() {
        for decision in [
            FullGenerationAuditDecisionV3::ProducerMarker,
            FullGenerationAuditDecisionV3::ProfileBoundReceipt,
        ] {
            assert!(
                validate_neutral_direct_generation_evidence(
                    &direct_evidence(decision),
                    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                )
                .is_err()
            );
            let mut attestation = valid();
            attestation.evidence = direct_evidence(decision);
            assert!(validate_wire_profile_attestation_structure(&attestation).is_err());
        }
    }

    #[test]
    fn all_equivalent_direct_transition_has_one_canonical_profile() {
        let evidence = direct_evidence(FullGenerationAuditDecisionV3::AllSemanticallyEquivalent);
        validate_neutral_direct_generation_evidence(
            &evidence,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        )
        .unwrap();
        assert!(
            validate_neutral_direct_generation_evidence(
                &evidence,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            )
            .is_err()
        );
    }

    #[test]
    fn direct_transition_rejects_raw_fallback_evidence() {
        let evidence = encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 1,
            raw_transaction_fallbacks: 1,
            alternate_profile_failures: 1,
            both_semantically_equivalent: 0,
            both_semantically_divergent: 0,
            decision: FullGenerationAuditDecisionV3::UniqueFullGenerationDecode,
        })
        .unwrap();
        assert!(
            validate_neutral_direct_generation_evidence(
                &evidence,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            )
            .is_err()
        );
    }
}
