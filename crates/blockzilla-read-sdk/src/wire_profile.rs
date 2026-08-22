//! Immutable Archive V2 wire-profile selection.
//!
//! Archive V2's outer block version did not change when `UnknownSystem` and
//! `UnknownVote` were inserted into `ArchiveV2HotInstructionData`. Because
//! wincode uses declaration-order enum tags, generations written before that
//! insertion need a different instruction-data tag table. Profile selection
//! belongs to the generation admission boundary, not to individual message
//! decoders: trying both layouts per message can silently accept a mixed or
//! ambiguous generation.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    Error, Result,
    manifest::{GenerationFile, GenerationManifest},
};

/// Manifest object binding for the pre-fallback instruction-data tag table.
///
/// The generation digest binds this exact filename, size, and digest. A
/// similarly named object, or this filename with a different binding, never
/// changes the decoder.
pub const PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE: &str =
    "archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker";
pub const PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE: u64 = 87;
pub const PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256: &str =
    "2a3aa5808085bc7b869c7536508227f19e6b9d9e3f5fb34b65ebda9936bf0206";

/// Explicit binding for the post-fallback message grammar. New producers
/// should publish this object instead of relying on the legacy no-marker rule.
pub const POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE: &str =
    "archive-v2-message-schema-post-unknown-fallbacks-v1.marker";
pub const POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE: u64 = 77;
pub const POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256: &str =
    "c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd";

const PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_BYTES: &[u8] =
    include_bytes!("../assets/archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker");
const POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_BYTES: &[u8] =
    include_bytes!("../assets/archive-v2-message-schema-post-unknown-fallbacks-v1.marker");

/// Return the exact object bytes a producer must publish for `profile`.
///
/// Publish these bytes under the name and binding returned by
/// [`wire_profile_marker`].
pub fn wire_profile_marker_bytes(profile: ArchiveV2WireProfile) -> &'static [u8] {
    match profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_BYTES
        }
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_BYTES
        }
    }
}

/// Return the exact manifest entry a producer must publish for `profile`.
/// Publish [`wire_profile_marker_bytes`] as the corresponding object.
pub fn wire_profile_marker(profile: ArchiveV2WireProfile) -> GenerationFile {
    let (name, size, sha256) = match profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => (
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE,
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
        ),
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => (
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
        ),
    };
    GenerationFile {
        name: name.to_owned(),
        size,
        sha256: sha256.to_owned(),
    }
}

/// One immutable Archive V2 hot-message wire grammar.
///
/// There is deliberately no `Default` implementation. Published generations
/// get a profile from their manifest-bound identity. Trusted-local generations
/// require the caller to state one explicitly. If another enum-order change is
/// made, add a new profile instead of changing either existing profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArchiveV2WireProfile {
    /// Tag order with `Raw`, `UnknownSystem`, and `UnknownVote` at tags 0..=2.
    PostUnknownInstructionFallbacksV1,
    /// Historical tag order with only `Raw` before the semantic variants.
    PreUnknownInstructionFallbacksV1,
}

impl ArchiveV2WireProfile {
    pub const POST_UNKNOWN_NAME: &'static str = "post-unknown-instruction-fallbacks-v1";
    pub const PRE_UNKNOWN_NAME: &'static str = "pre-unknown-instruction-fallbacks-v1";

    /// Resolve a published generation's profile from authenticated manifest
    /// bindings. This is generation-wide and never probes message bytes.
    pub fn for_published_manifest(manifest: &GenerationManifest) -> Result<Self> {
        let pre_marker = manifest.file(PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE);
        let post_marker = manifest.file(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE);
        if pre_marker.is_some() && post_marker.is_some() {
            return Err(Error::InvalidManifest(
                "generation lists conflicting Archive V2 message wire-profile markers".into(),
            ));
        }

        if let Some(marker) = pre_marker {
            if marker.size != PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE
                || marker.sha256 != PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256
            {
                return Err(Error::InvalidManifest(format!(
                    "malformed historical message wire-profile binding: {} must have size {} and sha256 {}, found size {} and sha256 {}",
                    PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
                    PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE,
                    PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
                    marker.size,
                    marker.sha256,
                )));
            }
            return Ok(Self::PreUnknownInstructionFallbacksV1);
        }
        if let Some(marker) = post_marker {
            if marker.size != POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE
                || marker.sha256 != POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256
            {
                return Err(Error::InvalidManifest(format!(
                    "malformed current message wire-profile binding: {} must have size {} and sha256 {}, found size {} and sha256 {}",
                    POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
                    POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE,
                    POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
                    marker.size,
                    marker.sha256,
                )));
            }
            return Ok(Self::PostUnknownInstructionFallbacksV1);
        }

        Err(Error::InvalidManifest(format!(
            "generation has no authenticated Archive V2 message wire-profile binding; publish the exact {} or {} marker",
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
        )))
    }
}

impl fmt::Display for ArchiveV2WireProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PostUnknownInstructionFallbacksV1 => Self::POST_UNKNOWN_NAME,
            Self::PreUnknownInstructionFallbacksV1 => Self::PRE_UNKNOWN_NAME,
        })
    }
}

impl FromStr for ArchiveV2WireProfile {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            Self::POST_UNKNOWN_NAME => Ok(Self::PostUnknownInstructionFallbacksV1),
            Self::PRE_UNKNOWN_NAME => Ok(Self::PreUnknownInstructionFallbacksV1),
            other => Err(format!(
                "unsupported Archive V2 wire profile {other:?}; expected {} or {}",
                Self::POST_UNKNOWN_NAME,
                Self::PRE_UNKNOWN_NAME,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::manifest::{
        BLOCK_INDEX_FILE, BLOCKS_FILE, GenerationFile, compute_generation_digest,
    };

    fn manifest_with_message_objects(
        cluster_id: &str,
        epoch: u64,
        blocks_sha256: &str,
        index_sha256: &str,
    ) -> GenerationManifest {
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: cluster_id.to_owned(),
            epoch,
            generation_id: "test".to_owned(),
            generation_digest: "00".repeat(32),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![
                GenerationFile {
                    name: BLOCKS_FILE.to_owned(),
                    size: 0,
                    sha256: blocks_sha256.to_owned(),
                },
                GenerationFile {
                    name: BLOCK_INDEX_FILE.to_owned(),
                    size: 0,
                    sha256: index_sha256.to_owned(),
                },
            ],
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        manifest
    }

    fn add_marker(manifest: &mut GenerationManifest, size: u64, sha256: &str) {
        manifest.files.push(GenerationFile {
            name: PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE.to_owned(),
            size,
            sha256: sha256.to_owned(),
        });
        manifest.generation_digest = compute_generation_digest(manifest).unwrap();
    }

    #[test]
    fn profile_names_are_stable_and_parse_exactly() {
        for profile in [
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ] {
            assert_eq!(profile.to_string().parse(), Ok(profile));
        }
        assert!("current".parse::<ArchiveV2WireProfile>().is_err());
    }

    #[test]
    fn historical_marker_asset_matches_manifest_binding() {
        assert_eq!(
            wire_profile_marker_bytes(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,).len()
                as u64,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE
        );
        let digest: [u8; 32] = Sha256::digest(wire_profile_marker_bytes(
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ))
        .into();
        assert_eq!(
            crate::manifest::hex_lower(&digest),
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256
        );
        assert_eq!(
            wire_profile_marker_bytes(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,)
                .len() as u64,
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE
        );
        let digest: [u8; 32] = Sha256::digest(wire_profile_marker_bytes(
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ))
        .into();
        assert_eq!(
            crate::manifest::hex_lower(&digest),
            POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256
        );
    }

    #[test]
    fn published_profile_requires_an_exact_marker() {
        let mut marked =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        add_marker(
            &mut marked,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
        );
        assert_eq!(
            ArchiveV2WireProfile::for_published_manifest(&marked).unwrap(),
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        );
    }

    #[test]
    fn unknown_unmarked_published_profile_is_rejected() {
        let unmarked =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        let error = ArchiveV2WireProfile::for_published_manifest(&unmarked).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("no authenticated Archive V2 message wire-profile binding"));
        assert!(message.contains(PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE));
        assert!(message.contains(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE));
    }

    #[test]
    fn malformed_marker_bindings_are_rejected() {
        let mut malformed =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        add_marker(
            &mut malformed,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE + 1,
            PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
        );
        assert!(
            ArchiveV2WireProfile::for_published_manifest(&malformed)
                .unwrap_err()
                .to_string()
                .contains("malformed historical message wire-profile binding")
        );

        let mut malformed_post =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        let mut marker =
            wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        marker.sha256 = "44".repeat(32);
        malformed_post.files.push(marker);
        malformed_post.generation_digest = compute_generation_digest(&malformed_post).unwrap();
        assert!(
            ArchiveV2WireProfile::for_published_manifest(&malformed_post)
                .unwrap_err()
                .to_string()
                .contains("malformed current message wire-profile binding")
        );
    }

    #[test]
    fn explicit_post_marker_selects_post_and_conflicts_are_rejected() {
        let mut explicit =
            manifest_with_message_objects("mainnet-beta", 2, &"22".repeat(32), &"33".repeat(32));
        explicit.files.push(wire_profile_marker(
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ));
        explicit.generation_digest = compute_generation_digest(&explicit).unwrap();
        assert_eq!(
            ArchiveV2WireProfile::for_published_manifest(&explicit).unwrap(),
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );

        explicit.files.push(wire_profile_marker(
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ));
        explicit.generation_digest = compute_generation_digest(&explicit).unwrap();
        assert!(
            ArchiveV2WireProfile::for_published_manifest(&explicit)
                .unwrap_err()
                .to_string()
                .contains("conflicting")
        );
    }
}
