use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub const GENERATION_MANIFEST_FILE: &str = "archive-v2-generation.json";
pub const GENERATION_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const GENERATION_DIGEST_DOMAIN: &[u8] = b"blockzilla/archive-v2-generation\0";

pub const BLOCKS_FILE: &str = blockzilla_format::ARCHIVE_V2_BLOCKS_FILE;
pub const BLOCK_INDEX_FILE: &str = blockzilla_format::ARCHIVE_V2_BLOCK_INDEX_FILE;
pub const META_FILE: &str = blockzilla_format::ARCHIVE_V2_META_FILE;
pub const REGISTRY_FILE: &str = blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_FILE;
pub const SIGNATURES_FILE: &str = blockzilla_format::ARCHIVE_V2_SIGNATURES_FILE;

pub const REQUIRED_GENERATION_FILES: [&str; 4] =
    [BLOCKS_FILE, BLOCK_INDEX_FILE, META_FILE, REGISTRY_FILE];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationManifest {
    pub schema_version: u32,
    pub cluster_id: String,
    pub epoch: u64,
    pub generation_id: String,
    pub generation_digest: String,
    pub slots_per_epoch: u64,
    pub complete: bool,
    pub files: Vec<GenerationFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationFile {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

impl GenerationManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|error| Error::InvalidManifest(format!("JSON decode failed: {error}")))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != GENERATION_MANIFEST_SCHEMA_VERSION {
            return Err(Error::InvalidManifest(format!(
                "unsupported schema_version {}",
                self.schema_version
            )));
        }
        validate_identity("cluster_id", &self.cluster_id)?;
        validate_identity("generation_id", &self.generation_id)?;
        if self.slots_per_epoch == 0 {
            return Err(Error::InvalidManifest(
                "slots_per_epoch must be greater than zero".into(),
            ));
        }
        self.epoch
            .checked_mul(self.slots_per_epoch)
            .and_then(|start| start.checked_add(self.slots_per_epoch - 1))
            .ok_or_else(|| Error::InvalidManifest("epoch slot range overflows u64".into()))?;

        let mut names = HashSet::with_capacity(self.files.len());
        for file in &self.files {
            validate_object_name(&file.name)
                .map_err(|message| Error::InvalidManifest(message.to_string()))?;
            if file.name == GENERATION_MANIFEST_FILE {
                return Err(Error::InvalidManifest(format!(
                    "{} cannot list itself in files[]",
                    GENERATION_MANIFEST_FILE
                )));
            }
            if !names.insert(file.name.as_str()) {
                return Err(Error::InvalidManifest(format!(
                    "duplicate file entry {}",
                    file.name
                )));
            }
            decode_sha256(&file.sha256).map_err(Error::InvalidManifest)?;
        }

        decode_sha256(&self.generation_digest).map_err(Error::InvalidManifest)?;
        let expected = compute_generation_digest(self)?;
        if self.generation_digest != expected {
            return Err(Error::InvalidManifest(format!(
                "generation_digest is {}, expected {}",
                self.generation_digest, expected
            )));
        }
        Ok(())
    }

    pub fn file(&self, name: &str) -> Option<&GenerationFile> {
        self.files.iter().find(|file| file.name == name)
    }

    pub fn required_file(&self, name: &str) -> Result<&GenerationFile> {
        self.file(name)
            .ok_or_else(|| Error::MissingFile(name.to_owned()))
    }

    pub fn epoch_start_slot(&self) -> u64 {
        // validate() proves this multiplication cannot overflow.
        self.epoch * self.slots_per_epoch
    }

    pub fn epoch_end_slot(&self) -> u64 {
        self.epoch_start_slot() + self.slots_per_epoch - 1
    }
}

/// Compute the non-circular generation identity digest.
///
/// The exact preimage is:
///
/// 1. `blockzilla/archive-v2-generation\0`;
/// 2. schema version as little-endian `u32`;
/// 3. cluster id as little-endian `u32` byte length followed by UTF-8 bytes;
/// 4. epoch as little-endian `u64`;
/// 5. generation id in the same length-prefixed UTF-8 form;
/// 6. slots per epoch as little-endian `u64`;
/// 7. complete as one `0` or `1` byte;
/// 8. file count as little-endian `u32`;
/// 9. files sorted by raw UTF-8 name bytes, each encoded as length-prefixed name,
///    little-endian `u64` size, and the 32 decoded SHA-256 bytes.
///
/// `generation_digest` itself is deliberately excluded.
pub fn compute_generation_digest(manifest: &GenerationManifest) -> Result<String> {
    validate_identity("cluster_id", &manifest.cluster_id)?;
    validate_identity("generation_id", &manifest.generation_id)?;
    let file_count = u32::try_from(manifest.files.len())
        .map_err(|_| Error::InvalidManifest("too many file entries".into()))?;
    let mut files: Vec<&GenerationFile> = manifest.files.iter().collect();
    files.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));

    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DIGEST_DOMAIN);
    hasher.update(manifest.schema_version.to_le_bytes());
    hash_string(&mut hasher, &manifest.cluster_id)?;
    hasher.update(manifest.epoch.to_le_bytes());
    hash_string(&mut hasher, &manifest.generation_id)?;
    hasher.update(manifest.slots_per_epoch.to_le_bytes());
    hasher.update([u8::from(manifest.complete)]);
    hasher.update(file_count.to_le_bytes());
    for file in files {
        validate_object_name(&file.name)
            .map_err(|message| Error::InvalidManifest(message.to_string()))?;
        hash_string(&mut hasher, &file.name)?;
        hasher.update(file.size.to_le_bytes());
        hasher.update(decode_sha256(&file.sha256).map_err(Error::InvalidManifest)?);
    }
    Ok(hex_lower(&hasher.finalize()))
}

pub(crate) fn validate_object_name(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("file name is empty");
    }
    if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err("file name must be one safe path component");
    }
    if name
        .bytes()
        .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err("file name contains a control character");
    }
    Ok(())
}

pub(crate) fn decode_sha256(value: &str) -> std::result::Result<[u8; 32], String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "SHA-256 must be exactly 64 lowercase hexadecimal characters: {value}"
        ));
    }
    let mut out = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        out[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(out)
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn validate_identity(field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidManifest(format!("{field} is empty")));
    }
    if value.len() > 4096
        || value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err(Error::InvalidManifest(format!(
            "{field} is too long or contains a control character"
        )));
    }
    Ok(())
}

fn hash_string(hasher: &mut Sha256, value: &str) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::InvalidManifest("string exceeds u32::MAX".into()))?;
    hasher.update(len.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("decode_sha256 validates lowercase hexadecimal"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_digest_is_file_order_independent_and_non_circular() {
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".into(),
            epoch: 999,
            generation_id: "generation-a".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![
                GenerationFile {
                    name: "z".into(),
                    size: 3,
                    sha256: "11".repeat(32),
                },
                GenerationFile {
                    name: "a".into(),
                    size: 7,
                    sha256: "22".repeat(32),
                },
            ],
        };
        let first = compute_generation_digest(&manifest).unwrap();
        manifest.generation_digest = "f".repeat(64);
        manifest.files.swap(0, 1);
        assert_eq!(compute_generation_digest(&manifest).unwrap(), first);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn manifest_rejects_path_traversal_and_uppercase_hashes() {
        assert!(validate_object_name("../registry.bin").is_err());
        assert!(decode_sha256(&"AA".repeat(32)).is_err());
    }

    #[test]
    fn manifest_rejects_circular_manifest_file_entry() {
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".into(),
            epoch: 1,
            generation_id: "generation-a".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![GenerationFile {
                name: GENERATION_MANIFEST_FILE.into(),
                size: 1,
                sha256: "11".repeat(32),
            }],
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        assert!(manifest.validate().is_err());
    }
}
