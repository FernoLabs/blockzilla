use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GENESIS_BIN_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, BLOCK_TIME_GAP_FILE,
};
use blockzilla_read_sdk::{
    ARCHIVE_V2_PUBLICATION_LOCK_FILE, CURRENT_TYPED_ERRORS_MARKER_FILE,
    POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE, PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
    manifest::GENERATION_MANIFEST_FILE,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SOURCE_AUTHORITY_SCHEMA_VERSION: u32 = 1;
pub const SOURCE_AUTHORITY_KIND: &str = "archive-v2-source-authority-inventory";
pub const SOURCE_AUTHORITY_DIGEST_DOMAIN: &[u8] =
    b"blockzilla/archive-v2-source-authority-inventory/v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthorityDisposition {
    RewriteBlocks,
    RewriteHotIndex,
    RebuildGetBlockIndex,
    CopySidecar,
    OmitControl,
}

impl AuthorityDisposition {
    fn stable_name(self) -> &'static str {
        match self {
            Self::RewriteBlocks => "rewrite-blocks",
            Self::RewriteHotIndex => "rewrite-hot-index",
            Self::RebuildGetBlockIndex => "rebuild-get-block-index",
            Self::CopySidecar => "copy-sidecar",
            Self::OmitControl => "omit-control",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceAuthorityFile {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
    pub disposition: AuthorityDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceAuthorityInventory {
    pub schema_version: u32,
    pub kind: String,
    pub complete: bool,
    pub authority_id: String,
    pub authority_digest: String,
    pub cluster_id: String,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub message_wire_profile: String,
    pub metadata_wire_profile: String,
    pub files: Vec<SourceAuthorityFile>,
}

impl SourceAuthorityInventory {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SOURCE_AUTHORITY_SCHEMA_VERSION,
            "unsupported source-authority schema version {}",
            self.schema_version
        );
        ensure!(
            self.kind == SOURCE_AUTHORITY_KIND,
            "wrong source-authority kind"
        );
        ensure!(self.complete, "source-authority inventory is incomplete");
        ensure!(!self.authority_id.is_empty(), "authority_id is empty");
        ensure!(!self.cluster_id.is_empty(), "cluster_id is empty");
        ensure!(self.slots_per_epoch > 0, "slots_per_epoch is zero");
        ensure!(
            matches!(
                self.message_wire_profile.as_str(),
                "post-unknown-instruction-fallbacks-v1" | "pre-unknown-instruction-fallbacks-v1"
            ),
            "unknown source-authority message wire profile"
        );
        ensure!(
            matches!(
                self.metadata_wire_profile.as_str(),
                "unmarked-historical-compatibility" | "current-typed-errors-v1"
            ),
            "unknown source-authority metadata wire profile"
        );
        let mut previous = None;
        let mut names = BTreeSet::new();
        for file in &self.files {
            validate_flat_name(&file.name)?;
            ensure!(
                known_disposition(&file.name) == Some(file.disposition),
                "source-authority disposition for {} is not canonical",
                file.name
            );
            decode_sha256(&file.sha256)
                .with_context(|| format!("invalid source-authority hash for {}", file.name))?;
            ensure!(names.insert(&file.name), "duplicate source-authority file");
            if let Some(previous) = previous {
                ensure!(
                    previous < file.name.as_str(),
                    "source-authority files are not in canonical byte order"
                );
            }
            previous = Some(file.name.as_str());
        }
        for required in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ] {
            ensure!(
                names.iter().any(|name| name.as_str() == required),
                "source-authority is missing {required}"
            );
        }
        ensure!(
            compute_authority_digest(self)? == self.authority_digest,
            "source-authority digest is invalid"
        );
        Ok(())
    }
}

pub fn compute_authority_digest(inventory: &SourceAuthorityInventory) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_AUTHORITY_DIGEST_DOMAIN);
    hasher.update(inventory.schema_version.to_le_bytes());
    hash_string(&mut hasher, &inventory.kind)?;
    hasher.update([u8::from(inventory.complete)]);
    hash_string(&mut hasher, &inventory.authority_id)?;
    hash_string(&mut hasher, &inventory.cluster_id)?;
    hasher.update(inventory.epoch.to_le_bytes());
    hasher.update(inventory.slots_per_epoch.to_le_bytes());
    hash_string(&mut hasher, &inventory.message_wire_profile)?;
    hash_string(&mut hasher, &inventory.metadata_wire_profile)?;
    let count = u32::try_from(inventory.files.len()).context("too many authority files")?;
    hasher.update(count.to_le_bytes());
    for file in &inventory.files {
        hash_string(&mut hasher, &file.name)?;
        hash_string(&mut hasher, file.disposition.stable_name())?;
        hasher.update(file.bytes.to_le_bytes());
        hasher.update(decode_sha256(&file.sha256)?);
    }
    Ok(hex_lower(&hasher.finalize()))
}

pub fn known_disposition(name: &str) -> Option<AuthorityDisposition> {
    match name {
        ARCHIVE_V2_BLOCKS_FILE => Some(AuthorityDisposition::RewriteBlocks),
        ARCHIVE_V2_BLOCK_INDEX_FILE => Some(AuthorityDisposition::RewriteHotIndex),
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE => Some(AuthorityDisposition::RebuildGetBlockIndex),
        CURRENT_TYPED_ERRORS_MARKER_FILE
        | PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
        | POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE => Some(AuthorityDisposition::OmitControl),
        ARCHIVE_V2_META_FILE
        | ARCHIVE_V2_PUBKEY_REGISTRY_FILE
        | ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE
        | ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE
        | ARCHIVE_V2_SIGNATURES_FILE
        | ARCHIVE_V2_GENESIS_BIN_FILE
        | ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE
        | ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE
        | ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE
        | ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE
        | ARCHIVE_V2_POH_FILE
        | ARCHIVE_V2_SHREDDING_FILE
        | BLOCK_TIME_GAP_FILE
        | ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE
        | ARCHIVE_V2_PUBKEY_HOT_SEED_FILE
        | ARCHIVE_V2_BLOCK_ACCESS_FILE
        | ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE => Some(AuthorityDisposition::CopySidecar),
        GENERATION_MANIFEST_FILE => Some(AuthorityDisposition::OmitControl),
        _ if is_known_control(name) => Some(AuthorityDisposition::OmitControl),
        _ => None,
    }
}

pub fn is_known_control(name: &str) -> bool {
    name == ARCHIVE_V2_PUBLICATION_LOCK_FILE
        || name == ".archive-v2-publication.lock"
        || name == ".hivezilla-pipeline-owned.v1.json"
        || name.ends_with(".receipt.json")
        || (name.contains(".receipt.") && name.ends_with(".json"))
        || name.contains(".candidate.")
        || name.contains(".switch-intent.")
        || name.contains(".switch-complete.")
        || name.ends_with(".lock")
        || name.starts_with(".complete")
        || name.starts_with("repair")
        || name.contains("registry-reprocess")
        || name.ends_with(".tmp")
}

pub fn looks_like_archive_or_control(name: &str) -> bool {
    known_disposition(name).is_some()
        || name.starts_with("archive-v2-")
        || name.starts_with("registry")
        || name.ends_with(".wincode")
        || name.ends_with(".zstd")
        || name.ends_with(".mphf")
        || name.ends_with(".marker")
        || name.ends_with(".receipt.json")
        || name.starts_with('.')
}

pub fn validate_flat_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains('/')
            && !name.as_bytes().contains(&0),
        "invalid flat authority object name {name:?}"
    );
    Ok(())
}

fn hash_string(hasher: &mut Sha256, value: &str) -> Result<()> {
    let length = u32::try_from(value.len()).context("authority string is too long")?;
    hasher.update(length.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "SHA-256 has the wrong length");
    let mut bytes = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    ensure!(
        hex_lower(&bytes) == value,
        "SHA-256 is not canonical lowercase"
    );
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => anyhow::bail!("SHA-256 is not lowercase hexadecimal"),
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

    fn valid_inventory() -> SourceAuthorityInventory {
        let mut files = [
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
        ]
        .into_iter()
        .map(|name| SourceAuthorityFile {
            name: name.to_owned(),
            bytes: if name == ARCHIVE_V2_SIGNATURES_FILE {
                0
            } else {
                1
            },
            sha256: "0".repeat(64),
            disposition: known_disposition(name).unwrap(),
        })
        .collect::<Vec<_>>();
        files.sort_by(|left, right| left.name.cmp(&right.name));
        let mut inventory = SourceAuthorityInventory {
            schema_version: SOURCE_AUTHORITY_SCHEMA_VERSION,
            kind: SOURCE_AUTHORITY_KIND.to_owned(),
            complete: true,
            authority_id: "authority".to_owned(),
            authority_digest: "0".repeat(64),
            cluster_id: "cluster".to_owned(),
            epoch: 1,
            slots_per_epoch: 10,
            message_wire_profile: "post-unknown-instruction-fallbacks-v1".to_owned(),
            metadata_wire_profile: "unmarked-historical-compatibility".to_owned(),
            files,
        };
        inventory.authority_digest = compute_authority_digest(&inventory).unwrap();
        inventory
    }

    #[test]
    fn inventory_accepts_zero_length_optional_sidecars() {
        valid_inventory().validate().unwrap();
    }

    #[test]
    fn inventory_rejects_unknown_profiles_even_with_a_matching_digest() {
        for (message_profile, metadata_profile) in [
            (Some("future-message-profile"), None),
            (None, Some("future-metadata-profile")),
        ] {
            let mut inventory = valid_inventory();
            if let Some(profile) = message_profile {
                inventory.message_wire_profile = profile.to_owned();
            }
            if let Some(profile) = metadata_profile {
                inventory.metadata_wire_profile = profile.to_owned();
            }
            inventory.authority_digest = compute_authority_digest(&inventory).unwrap();
            assert!(inventory.validate().is_err());
        }
    }
}
