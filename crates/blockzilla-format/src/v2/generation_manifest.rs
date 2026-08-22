//! WASM-safe Archive V2 generation identity and message-profile binding.

use std::collections::HashSet;

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ARCHIVE_V2_GENERATION_MANIFEST_FILE: &str = "archive-v2-generation.json";
pub const ARCHIVE_V2_GENERATION_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const ARCHIVE_V2_GENERATION_DIGEST_DOMAIN: &[u8] = b"blockzilla/archive-v2-generation\0";

pub const ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_FILE: &str =
    "archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker";
pub const ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_BYTES: &[u8] =
    b"blockzilla/archive-v2-hot-message-schema/may24-2026-pre-unknown-system-unknown-vote/v1\n";
pub const ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_SHA256: &str =
    "2a3aa5808085bc7b869c7536508227f19e6b9d9e3f5fb34b65ebda9936bf0206";

pub const ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_FILE: &str =
    "archive-v2-message-schema-post-unknown-fallbacks-v1.marker";
pub const ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_BYTES: &[u8] =
    b"blockzilla/archive-v2-hot-message-schema/post-unknown-system-unknown-vote/v1\n";
pub const ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_SHA256: &str =
    "c870c4b0940b05b7bd18a134fba496c5c376f539ef7668f137112526d5c61edd";

/// Stable, generation-wide instruction-data tag table.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
#[serde(rename_all = "kebab-case")]
#[wincode(tag_encoding = "u8")]
pub enum ArchiveV2MessageWireProfile {
    #[wincode(tag = 0)]
    PreUnknownInstructionFallbacksV1,
    #[wincode(tag = 1)]
    PostUnknownInstructionFallbacksV1,
}

impl ArchiveV2MessageWireProfile {
    pub const PRE_UNKNOWN_NAME: &'static str = "pre-unknown-instruction-fallbacks-v1";
    pub const POST_UNKNOWN_NAME: &'static str = "post-unknown-instruction-fallbacks-v1";

    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::PreUnknownInstructionFallbacksV1 => Self::PRE_UNKNOWN_NAME,
            Self::PostUnknownInstructionFallbacksV1 => Self::POST_UNKNOWN_NAME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveV2GenerationManifest {
    pub schema_version: u32,
    pub cluster_id: String,
    pub epoch: u64,
    pub generation_id: String,
    pub generation_digest: String,
    pub slots_per_epoch: u64,
    pub complete: bool,
    pub files: Vec<ArchiveV2GenerationFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveV2GenerationFile {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveV2MessageWireProfileMarker {
    pub name: &'static str,
    pub bytes: &'static [u8],
    pub sha256: &'static str,
}

impl ArchiveV2GenerationManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == ARCHIVE_V2_GENERATION_MANIFEST_SCHEMA_VERSION,
            "unsupported Archive V2 generation manifest schema version {}",
            self.schema_version
        );
        validate_identity("cluster_id", &self.cluster_id)?;
        validate_identity("generation_id", &self.generation_id)?;
        ensure!(self.slots_per_epoch > 0, "slots_per_epoch must be positive");
        self.epoch
            .checked_mul(self.slots_per_epoch)
            .and_then(|start| start.checked_add(self.slots_per_epoch - 1))
            .ok_or_else(|| anyhow::anyhow!("epoch slot range overflows u64"))?;

        let mut names = HashSet::with_capacity(self.files.len());
        for file in &self.files {
            validate_object_name(&file.name)?;
            ensure!(
                file.name != ARCHIVE_V2_GENERATION_MANIFEST_FILE,
                "generation manifest cannot list itself"
            );
            ensure!(names.insert(file.name.as_str()), "duplicate generation file {}", file.name);
            decode_archive_v2_sha256(&file.sha256)?;
        }
        decode_archive_v2_sha256(&self.generation_digest)?;
        let expected = compute_archive_v2_generation_digest(self)?;
        ensure!(
            self.generation_digest == expected,
            "generation digest is {}, expected {expected}",
            self.generation_digest
        );
        Ok(())
    }

    pub fn file(&self, name: &str) -> Option<&ArchiveV2GenerationFile> {
        self.files.iter().find(|file| file.name == name)
    }

    pub fn required_file(&self, name: &str) -> Result<&ArchiveV2GenerationFile> {
        self.file(name)
            .ok_or_else(|| anyhow::anyhow!("generation manifest does not bind required file {name}"))
    }

    pub fn message_wire_profile(&self) -> Result<ArchiveV2MessageWireProfile> {
        let pre = self.file(ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_FILE);
        let post = self.file(ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_FILE);
        let profile = match (pre, post) {
            (Some(_), Some(_)) => bail!("generation binds conflicting message wire profiles"),
            (None, None) => bail!("generation has no message wire-profile marker"),
            (Some(file), None) => {
                validate_marker_binding(
                    file,
                    ArchiveV2MessageWireProfile::PreUnknownInstructionFallbacksV1,
                )?;
                ArchiveV2MessageWireProfile::PreUnknownInstructionFallbacksV1
            }
            (None, Some(file)) => {
                validate_marker_binding(
                    file,
                    ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1,
                )?;
                ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1
            }
        };
        Ok(profile)
    }
}

pub fn archive_v2_message_wire_profile_marker(
    profile: ArchiveV2MessageWireProfile,
) -> ArchiveV2MessageWireProfileMarker {
    match profile {
        ArchiveV2MessageWireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2MessageWireProfileMarker {
                name: ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_FILE,
                bytes: ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_BYTES,
                sha256: ARCHIVE_V2_PRE_UNKNOWN_FALLBACKS_MARKER_SHA256,
            }
        }
        ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2MessageWireProfileMarker {
                name: ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_FILE,
                bytes: ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_BYTES,
                sha256: ARCHIVE_V2_POST_UNKNOWN_FALLBACKS_MARKER_SHA256,
            }
        }
    }
}

pub fn compute_archive_v2_generation_digest(
    manifest: &ArchiveV2GenerationManifest,
) -> Result<String> {
    validate_identity("cluster_id", &manifest.cluster_id)?;
    validate_identity("generation_id", &manifest.generation_id)?;
    let file_count = u32::try_from(manifest.files.len())
        .map_err(|_| anyhow::anyhow!("generation has too many files"))?;
    let mut files = manifest.files.iter().collect::<Vec<_>>();
    files.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));

    let mut hasher = Sha256::new();
    hasher.update(ARCHIVE_V2_GENERATION_DIGEST_DOMAIN);
    hasher.update(manifest.schema_version.to_le_bytes());
    hash_string(&mut hasher, &manifest.cluster_id)?;
    hasher.update(manifest.epoch.to_le_bytes());
    hash_string(&mut hasher, &manifest.generation_id)?;
    hasher.update(manifest.slots_per_epoch.to_le_bytes());
    hasher.update([u8::from(manifest.complete)]);
    hasher.update(file_count.to_le_bytes());
    for file in files {
        validate_object_name(&file.name)?;
        hash_string(&mut hasher, &file.name)?;
        hasher.update(file.size.to_le_bytes());
        hasher.update(decode_archive_v2_sha256(&file.sha256)?);
    }
    Ok(hex_lower(&hasher.finalize()))
}

pub fn decode_archive_v2_sha256(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "SHA-256 must be 64 lowercase hexadecimal characters"
    );
    let mut output = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(output)
}

fn validate_marker_binding(
    file: &ArchiveV2GenerationFile,
    profile: ArchiveV2MessageWireProfile,
) -> Result<()> {
    let marker = archive_v2_message_wire_profile_marker(profile);
    ensure!(
        file.name == marker.name
            && file.size == marker.bytes.len() as u64
            && file.sha256 == marker.sha256,
        "message wire-profile marker binding is not canonical"
    );
    Ok(())
}

fn validate_object_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains('/')
            && !name.contains('\\')
            && !name
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control()),
        "generation file name is not one safe path component"
    );
    Ok(())
}

fn validate_identity(label: &str, value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 4096
            && !value
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control()),
        "{label} is empty, too long, or contains a control character"
    );
    Ok(())
}

fn hash_string(hasher: &mut Sha256, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| anyhow::anyhow!("string exceeds u32::MAX"))?;
    hasher.update(len.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("hexadecimal input was validated"),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker_file(profile: ArchiveV2MessageWireProfile) -> ArchiveV2GenerationFile {
        let marker = archive_v2_message_wire_profile_marker(profile);
        ArchiveV2GenerationFile {
            name: marker.name.into(),
            size: marker.bytes.len() as u64,
            sha256: marker.sha256.into(),
        }
    }

    fn manifest(profile: ArchiveV2MessageWireProfile) -> ArchiveV2GenerationManifest {
        let mut manifest = ArchiveV2GenerationManifest {
            schema_version: ARCHIVE_V2_GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: "mainnet-beta".into(),
            epoch: 2,
            generation_id: "generation-a".into(),
            generation_digest: "00".repeat(32),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![marker_file(profile)],
        };
        manifest.generation_digest = compute_archive_v2_generation_digest(&manifest).unwrap();
        manifest
    }

    #[test]
    fn canonical_markers_select_one_exact_profile() {
        for profile in [
            ArchiveV2MessageWireProfile::PreUnknownInstructionFallbacksV1,
            ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1,
        ] {
            let manifest = manifest(profile);
            assert_eq!(manifest.message_wire_profile().unwrap(), profile);
            let bytes = serde_json::to_vec(&manifest).unwrap();
            assert_eq!(ArchiveV2GenerationManifest::parse(&bytes).unwrap(), manifest);
        }
    }

    #[test]
    fn marker_constants_match_their_bytes() {
        for profile in [
            ArchiveV2MessageWireProfile::PreUnknownInstructionFallbacksV1,
            ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1,
        ] {
            let marker = archive_v2_message_wire_profile_marker(profile);
            assert_eq!(hex_lower(&Sha256::digest(marker.bytes)), marker.sha256);
        }
    }

    #[test]
    fn missing_conflicting_and_malformed_markers_fail_closed() {
        let mut missing = manifest(ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1);
        missing.files.clear();
        missing.generation_digest = compute_archive_v2_generation_digest(&missing).unwrap();
        assert!(missing.message_wire_profile().is_err());

        let mut conflicting = manifest(ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1);
        conflicting.files.push(marker_file(
            ArchiveV2MessageWireProfile::PreUnknownInstructionFallbacksV1,
        ));
        conflicting.generation_digest = compute_archive_v2_generation_digest(&conflicting).unwrap();
        assert!(conflicting.message_wire_profile().is_err());

        let mut malformed = manifest(ArchiveV2MessageWireProfile::PostUnknownInstructionFallbacksV1);
        malformed.files[0].size += 1;
        malformed.generation_digest = compute_archive_v2_generation_digest(&malformed).unwrap();
        assert!(malformed.message_wire_profile().is_err());
    }
}
