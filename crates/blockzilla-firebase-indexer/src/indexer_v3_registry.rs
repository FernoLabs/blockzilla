//! Bounded public-key to source-registry lookup for Indexer V3.
//!
//! The MPHF control object supplies the candidate registry ID. The reader then
//! verifies the exact 32-byte row in `registry.bin` before it returns that ID.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use blockzilla_format::{
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    registry::{KeyIndexRangeSource, RangeBackedKeyIndex},
};
use blockzilla_compact_v2_reader::RangeSource;

const REGISTRY_KEY_BYTES: u64 = 32;

#[derive(Clone)]
struct RegistryIndexObject {
    source: Arc<dyn RangeSource>,
}

impl KeyIndexRangeSource for RegistryIndexObject {
    fn object_len(&self) -> Result<u64> {
        self.source
            .size(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)
            .context("read V3 registry index size")?
            .context("V3 registry index is missing")
    }

    fn read_exact_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.source
            .read_range(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, offset, length)
            .context("read V3 registry index range")
    }
}

/// A bounded public-key lookup over one immutable V3 object set.
pub struct IndexerV3RegistryIndex {
    index: RangeBackedKeyIndex<RegistryIndexObject>,
    source: Arc<dyn RangeSource>,
}

impl std::fmt::Debug for IndexerV3RegistryIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexerV3RegistryIndex")
            .field("entries", &self.index.len())
            .finish_non_exhaustive()
    }
}

impl IndexerV3RegistryIndex {
    /// Open the MPHF from the same bound range source as the V3 ledger.
    pub fn open(source: Arc<dyn RangeSource>, expected_entries: u32) -> Result<Self> {
        let registry_size = source
            .size(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            .context("read V3 registry size")?
            .context("V3 registry is missing")?;
        let expected_registry_size = u64::from(expected_entries)
            .checked_mul(REGISTRY_KEY_BYTES)
            .context("V3 registry size overflow")?;
        ensure!(
            registry_size == expected_registry_size,
            "V3 registry has {registry_size} bytes; expected {expected_registry_size}"
        );

        let index = RangeBackedKeyIndex::load(RegistryIndexObject {
            source: Arc::clone(&source),
        })
        .context("open V3 registry index")?;
        ensure!(
            index.len() == expected_entries as usize,
            "V3 registry index has {} entries; expected {expected_entries}",
            index.len()
        );
        Ok(Self { index, source })
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Resolve one public key and verify the returned ID against `registry.bin`.
    pub fn lookup(&self, pubkey: &[u8; 32]) -> Result<Option<u32>> {
        let Some(id) = self.index.lookup(pubkey)? else {
            return Ok(None);
        };
        let offset = u64::from(id - 1)
            .checked_mul(REGISTRY_KEY_BYTES)
            .context("V3 registry row offset overflow")?;
        let resolved = self
            .source
            .read_range(
                ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
                offset,
                REGISTRY_KEY_BYTES as usize,
            )
            .context("verify V3 registry lookup row")?;
        ensure!(
            resolved.as_slice() == pubkey,
            "V3 registry index ID {id} does not resolve to the requested public key"
        );
        Ok(Some(id))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use blockzilla_format::registry::KeyIndex;
    use blockzilla_compact_v2_reader::{SourceError, SourceResult};
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug)]
    struct MemorySource {
        objects: HashMap<String, Vec<u8>>,
    }

    impl RangeSource for MemorySource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            Ok(self.objects.get(object).map(|bytes| bytes.len() as u64))
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            let bytes = self
                .objects
                .get(object)
                .ok_or_else(|| SourceError::NotFound(object.to_owned()))?;
            let start = usize::try_from(offset).map_err(|_| SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: bytes.len() as u64,
            })?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| SourceError::OutOfBounds {
                    object: object.to_owned(),
                    offset,
                    length,
                    size: bytes.len() as u64,
                })?;
            bytes
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| SourceError::OutOfBounds {
                    object: object.to_owned(),
                    offset,
                    length,
                    size: bytes.len() as u64,
                })
        }
    }

    fn fixture(keys: &[[u8; 32]]) -> (TempDir, Arc<dyn RangeSource>) {
        let directory = TempDir::new().unwrap();
        let index_path = directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        KeyIndex::build_from_slice(keys).write(&index_path).unwrap();
        let registry = keys.iter().flatten().copied().collect::<Vec<_>>();
        let source = MemorySource {
            objects: HashMap::from([
                (
                    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE.to_owned(),
                    std::fs::read(index_path).unwrap(),
                ),
                (ARCHIVE_V2_PUBKEY_REGISTRY_FILE.to_owned(), registry),
            ]),
        };
        (directory, Arc::new(source))
    }

    #[test]
    fn resolves_members_and_rejects_nonmembers() {
        let keys = [[1u8; 32], [2u8; 32], [3u8; 32]];
        let (_directory, source) = fixture(&keys);
        let index = IndexerV3RegistryIndex::open(source, keys.len() as u32).unwrap();

        assert_eq!(index.lookup(&keys[0]).unwrap(), Some(1));
        assert_eq!(index.lookup(&keys[2]).unwrap(), Some(3));
        assert_eq!(index.lookup(&[9u8; 32]).unwrap(), None);
    }

    #[test]
    fn verifies_the_registry_row_after_the_mphf_lookup() {
        let keys = [[4u8; 32], [5u8; 32]];
        let (directory, source) = fixture(&keys);
        let index_path = directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        let index_bytes = std::fs::read(index_path).unwrap();
        let corrupt = MemorySource {
            objects: HashMap::from([
                (
                    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE.to_owned(),
                    index_bytes,
                ),
                (
                    ARCHIVE_V2_PUBKEY_REGISTRY_FILE.to_owned(),
                    [[8u8; 32], keys[1]].into_iter().flatten().collect(),
                ),
            ]),
        };
        let index = IndexerV3RegistryIndex::open(Arc::new(corrupt), keys.len() as u32).unwrap();
        assert!(index.lookup(&keys[0]).is_err());

        drop(source);
    }
}
