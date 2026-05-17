use anyhow::{Context, Result};
use ph::fmph;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::CompactPubkey;

pub struct KeyIndex {
    /// Minimal perfect hash over all pubkeys
    mphf: fmph::GOFunction,

    /// mphf_index -> 1-based id
    values: Vec<u32>,

    /// mphf_index -> exact key, used to distinguish misses from arbitrary MPHF outputs.
    keys_by_mphf: Vec<[u8; 32]>,
}

impl KeyIndex {
    /// Build index over keys in file order.
    ///
    /// All lookups are assumed to be members of the registry.
    pub fn build(keys_in_file_order: Vec<[u8; 32]>) -> Self {
        let n = keys_in_file_order.len();

        // MPHF build
        let mphf: fmph::GOFunction = keys_in_file_order.as_slice().into();

        let mut values = vec![0u32; n];
        let mut keys_by_mphf = vec![[0u8; 32]; n];

        for (i, k) in keys_in_file_order.iter().enumerate() {
            let id = i as u32 + 1;

            let idx = mphf.get_or_panic(k) as usize;
            debug_assert!(idx < n);
            values[idx] = id;
            keys_by_mphf[idx] = *k;
        }

        Self {
            mphf,
            values,
            keys_by_mphf,
        }
    }

    /// Checked lookup. Returns None when `k` is not in the registry.
    #[inline(always)]
    pub fn lookup(&self, k: &[u8; 32]) -> Option<u32> {
        let idx = self.mphf.get(k)? as usize;
        if self.keys_by_mphf.get(idx)? != k {
            return None;
        }
        let id = self.values[idx];
        (id != 0).then_some(id)
    }

    /// Fast path: key MUST exist.
    #[inline(always)]
    pub fn lookup_unchecked(&self, k: &[u8; 32]) -> u32 {
        self.lookup(k).expect("registry key missing")
    }

    #[inline(always)]
    pub fn compact(&self, k: &[u8; 32]) -> CompactPubkey {
        self.lookup(k)
            .map(CompactPubkey::id)
            .unwrap_or_else(|| CompactPubkey::raw(*k))
    }

    /// Lookup from base58 string.
    pub fn lookup_str(&self, k: &str) -> Option<u32> {
        let pk = Pubkey::from_str(k).ok()?;
        self.lookup(pk.as_array())
    }

    pub fn compact_str(&self, k: &str) -> Option<CompactPubkey> {
        let pk = Pubkey::from_str(k).ok()?;
        Some(self.compact(pk.as_array()))
    }
}

/// Owns keys in file order. Ids are 1-based (0 reserved).
#[derive(Debug, Clone)]
pub struct KeyStore {
    pub keys: Vec<[u8; 32]>,
}

impl KeyStore {
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// 1-based id -> key
    #[inline]
    pub fn get(&self, id: u32) -> Option<&[u8; 32]> {
        self.keys.get(id.checked_sub(1)? as usize)
    }

    /// Sequential load, no extra buffers.
    pub fn load(path: &Path) -> Result<Self> {
        let f = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
        let len_bytes = f.metadata().context("stat registry")?.len() as usize;

        anyhow::ensure!(
            len_bytes.is_multiple_of(32),
            "invalid registry size {} (not multiple of 32)",
            len_bytes
        );

        let n = len_bytes / 32;
        let mut r = BufReader::with_capacity(64 << 20, f);

        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            let mut a = [0u8; 32];
            r.read_exact(&mut a).context("read pubkey")?;
            keys.push(a);
        }

        Ok(Self { keys })
    }
}

/// Write registry.bin (raw 32-byte pubkeys, no header)
pub fn write_registry(path: &Path, keys: &[[u8; 32]]) -> Result<()> {
    let f = File::create(path).with_context(|| format!("Failed to create {}", path.display()))?;
    let mut w = BufWriter::with_capacity(64 << 20, f);

    for k in keys {
        w.write_all(k).context("write pubkey")?;
    }

    w.flush().context("flush registry")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_ids_are_one_based_and_missing_keys_fall_back_to_raw() {
        let first = [0u8; 32];
        let second = [1u8; 32];
        let missing = [2u8; 32];
        let index = KeyIndex::build(vec![first, second]);

        assert_eq!(index.lookup(&first), Some(1));
        assert_eq!(index.lookup(&second), Some(2));
        assert_eq!(index.lookup(&missing), None);
        assert_eq!(index.compact(&missing), CompactPubkey::raw(missing));
    }
}
