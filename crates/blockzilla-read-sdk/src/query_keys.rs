//! Bind the few query keys once. Scanning then compares compact IDs, not keys.
use std::cell::Cell;

use crate::RangeSource;
use anyhow::{Result, ensure};
use blockzilla_format::{
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE as REGISTRY, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE as INDEX,
    CompactPubkey,
    registry::{KeyIndexRangeSource, RangeBackedKeyIndex},
};
use blockzilla_query_sdk::{InstructionDataRequirement, ScanRequest, TokenBalanceRequirement};

#[derive(Debug, Default)]
pub struct BoundQueryKeys {
    keys: Vec<([u8; 32], Option<u32>)>,
    pub read_calls: u64,
    pub read_bytes: u64,
}

struct IndexSource<'a> {
    source: &'a dyn RangeSource,
    calls: Cell<u64>,
    bytes: Cell<u64>,
}

impl IndexSource<'_> {
    fn read(&self, object: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
        let bytes = self.source.read_range(object, offset, length)?;
        ensure!(bytes.len() == length, "short registry read");
        self.calls.set(self.calls.get() + 1);
        self.bytes.set(self.bytes.get() + length as u64);
        Ok(bytes)
    }
}

impl KeyIndexRangeSource for &IndexSource<'_> {
    fn object_len(&self) -> Result<u64> {
        self.source
            .size(INDEX)?
            .ok_or_else(|| anyhow::anyhow!("missing registry index"))
    }
    fn read_exact_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.read(INDEX, offset, length)
    }
}

impl BoundQueryKeys {
    fn targets(request: &ScanRequest) -> Vec<[u8; 32]> {
        let mut targets = Vec::new();
        for requirement in [&request.instruction_programs, &request.instruction_data] {
            if let InstructionDataRequirement::Programs(keys) = requirement {
                targets.extend_from_slice(keys);
            }
        }
        if let TokenBalanceRequirement::Mints(keys) = &request.token_balances {
            targets.extend_from_slice(keys);
        }
        targets.extend(request.required_signer);
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    pub fn covers(&self, request: &ScanRequest) -> bool {
        Self::targets(request)
            .iter()
            .all(|key| self.registry_id(key).is_some())
    }

    /// Outer None means this key was not bound. Inner None means absent.
    pub fn registry_id(&self, key: &[u8; 32]) -> Option<Option<u32>> {
        self.keys
            .iter()
            .find(|(target, _)| target == key)
            .map(|(_, id)| *id)
    }

    pub fn bind(source: &dyn RangeSource, entries: u32, request: &ScanRequest) -> Result<Self> {
        Self::bind_with_registry(source, entries, request, None)
    }

    pub fn bind_with_registry(
        source: &dyn RangeSource,
        entries: u32,
        request: &ScanRequest,
        registry: Option<&[u8]>,
    ) -> Result<Self> {
        let targets = Self::targets(request);
        let mut result = Self {
            keys: targets.into_iter().map(|key| (key, None)).collect(),
            ..Self::default()
        };
        if result.keys.is_empty() || entries == 0 {
            return Ok(result);
        }
        if let Some(bytes) = registry {
            ensure!(
                bytes.len() as u64 == u64::from(entries) * 32,
                "registry size mismatch"
            );
            for (position, key) in bytes.chunks_exact(32).enumerate() {
                if let Some((_, id)) = result.keys.iter_mut().find(|(target, _)| target == key) {
                    ensure!(id.is_none(), "duplicate query key in registry");
                    *id = Some(position as u32 + 1);
                }
            }
            return Ok(result);
        }
        let input = IndexSource {
            source,
            calls: Cell::new(0),
            bytes: Cell::new(0),
        };
        if source.size(INDEX)?.is_some() {
            let index = RangeBackedKeyIndex::load(&input)?;
            ensure!(
                index.len() == entries as usize,
                "registry index entry count mismatch"
            );
            for (key, id) in &mut result.keys {
                *id = index.lookup(key)?;
                if let Some(value) = *id {
                    ensure!(
                        value > 0 && value <= entries,
                        "registry index ID out of range"
                    );
                    let row = input.read(REGISTRY, u64::from(value - 1) * 32, 32)?;
                    ensure!(row.as_slice() == key, "registry index key mismatch");
                }
            }
        } else {
            // The index is optional. A single bounded registry pass preserves
            // compatibility; never repeat this pass per block or worker.
            let size = u64::from(entries) * 32;
            let mut offset = 0;
            while offset < size {
                let length = (size - offset).min(4 << 20) as usize;
                let bytes = input.read(REGISTRY, offset, length)?;
                for (position, key) in bytes.chunks_exact(32).enumerate() {
                    if let Some((_, id)) = result.keys.iter_mut().find(|(target, _)| target == key)
                    {
                        ensure!(id.is_none(), "duplicate query key in registry");
                        *id = Some((offset / 32 + position as u64 + 1) as u32);
                    }
                }
                offset += length as u64;
            }
        }
        result.read_calls = input.calls.get();
        result.read_bytes = input.bytes.get();
        Ok(result)
    }

    pub fn matches(&self, reference: CompactPubkey, key: &[u8; 32]) -> bool {
        match reference {
            CompactPubkey::Raw(raw) => &raw == key,
            CompactPubkey::Id(id) => self
                .keys
                .iter()
                .any(|(target, value)| target == key && *value == Some(id)),
        }
    }

    pub fn selected(&self, reference: CompactPubkey, keys: &[[u8; 32]]) -> Option<[u8; 32]> {
        keys.iter()
            .find(|key| self.matches(reference, key))
            .copied()
    }
}
