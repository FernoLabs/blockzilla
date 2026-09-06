//! Local admission proof for the canonical Archive V2 pubkey registry.

use blockzilla_registry::FileBackedKeyIndex;

use crate::{
    Error, PinnedLocalRangeSource, RangeSource, Result,
    manifest::{GenerationManifest, REGISTRY_FILE, REGISTRY_INDEX_FILE},
};

/// Bounded result of the exact registry/index correspondence scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRegistryIndexValidation {
    pub entries: u32,
    pub registry_bytes: u64,
    pub registry_index_bytes: u64,
}

/// Prove that a manifest-bound local `registry.mphf` maps every registry key
/// to the key's exact one-based `registry.bin` row.
///
/// The caller must first authenticate the manifest, including the SHA-256 of
/// `registry.mphf`. `ArchiveReader::open_candidate` with
/// `HashVerification::AllFiles` does this for a published generation.
pub fn validate_manifest_bound_pinned_local_registry_index(
    source: &PinnedLocalRangeSource,
    manifest: &GenerationManifest,
) -> Result<LocalRegistryIndexValidation> {
    let registry = manifest.required_file(REGISTRY_FILE)?;
    let registry_index = manifest.required_file(REGISTRY_INDEX_FILE)?;
    if registry.size % 32 != 0 {
        return Err(Error::InvalidRegistry(format!(
            "{REGISTRY_FILE} is {} bytes, not a multiple of 32",
            registry.size
        )));
    }
    let entries = u32::try_from(registry.size / 32).map_err(|_| {
        Error::InvalidRegistry("registry.bin entry count exceeds the u32 ID space".into())
    })?;
    let validated = validate_pinned_local_registry_index_mapping(source, entries)?;
    if validated.registry_bytes != registry.size {
        return Err(Error::InvalidRegistry(format!(
            "retained {REGISTRY_FILE} is {} bytes, manifest binds {}",
            validated.registry_bytes, registry.size
        )));
    }
    if validated.registry_index_bytes != registry_index.size {
        return Err(Error::InvalidRegistry(format!(
            "retained {REGISTRY_INDEX_FILE} is {} bytes, manifest binds {}",
            validated.registry_index_bytes, registry_index.size
        )));
    }
    Ok(validated)
}

/// Prove the exact mapping for a pinned local generation.
///
/// This function proves canonical membership and rejects duplicate registry
/// keys. It does not choose an identity authority. A trusted-local caller must
/// separately bind both retained files in its receipt or attestation evidence.
pub fn validate_pinned_local_registry_index_mapping(
    source: &PinnedLocalRangeSource,
    expected_entries: u32,
) -> Result<LocalRegistryIndexValidation> {
    let registry_size = source
        .size(REGISTRY_FILE)?
        .ok_or_else(|| Error::MissingFile(REGISTRY_FILE.into()))?;
    let expected_registry_size = u64::from(expected_entries)
        .checked_mul(32)
        .ok_or(Error::Overflow("registry byte length"))?;
    if registry_size != expected_registry_size {
        return Err(Error::InvalidRegistry(format!(
            "{REGISTRY_FILE} is {registry_size} bytes, expected {expected_registry_size} for {expected_entries} entries"
        )));
    }
    let registry_index_size = source
        .size(REGISTRY_INDEX_FILE)?
        .ok_or_else(|| Error::MissingFile(REGISTRY_INDEX_FILE.into()))?;
    let registry_file = source.open_file(REGISTRY_FILE)?;
    let registry_index_file = source.open_file(REGISTRY_INDEX_FILE)?;
    source.verify_unchanged()?;

    let registry_path = source.root().join(REGISTRY_FILE);
    let registry_index_path = source.root().join(REGISTRY_INDEX_FILE);
    let key_index = FileBackedKeyIndex::load_file(registry_index_file, &registry_index_path)
        .map_err(|error| Error::InvalidRegistry(error.to_string()))?;
    if key_index.len() != expected_entries as usize {
        return Err(Error::InvalidRegistry(format!(
            "{REGISTRY_INDEX_FILE} has {} entries, expected {expected_entries}",
            key_index.len()
        )));
    }
    let validated = key_index
        .validate_registry_file_order(&registry_file, &registry_path)
        .map_err(|error| Error::InvalidRegistry(error.to_string()))?;
    if validated.registry_index_bytes != registry_index_size {
        return Err(Error::InvalidRegistry(format!(
            "retained {REGISTRY_INDEX_FILE} is {} bytes, expected pinned size {registry_index_size}",
            validated.registry_index_bytes
        )));
    }
    source.verify_unchanged()?;

    Ok(LocalRegistryIndexValidation {
        entries: validated.entries,
        registry_bytes: validated.registry_bytes,
        registry_index_bytes: validated.registry_index_bytes,
    })
}
