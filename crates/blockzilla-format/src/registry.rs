use anyhow::{Context, Result};
use boomphf::hashmap::NoKeyBoomHashMap;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

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

    #[inline]
    pub fn get(&self, id: u32) -> Option<&[u8; 32]> {
        self.keys.get(id.checked_sub(1)? as usize)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let f = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
        let mut r = BufReader::with_capacity(64 << 20, f);

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).context("read registry")?;
        anyhow::ensure!(
            buf.len() % 32 == 0,
            "invalid registry size {} (not multiple of 32)",
            buf.len()
        );

        let mut keys = Vec::with_capacity(buf.len() / 32);
        for c in buf.chunks_exact(32) {
            let mut a = [0u8; 32];
            a.copy_from_slice(c);
            keys.push(a);
        }

        Ok(Self { keys })
    }
}

/// Key -> id index that does NOT store keys.
/// Assumes lookups are always hits.
/// Ids are 1-based (0 reserved).
#[derive(Debug, Clone)]
pub struct KeyIndex {
    index: NoKeyBoomHashMap<[u8; 32], u32>,
}

impl KeyIndex {
    pub fn build(keys_in_file_order: Vec<[u8; 32]>) -> Self {
        // Store (file_index + 1) to reserve 0
        let values: Vec<u32> = (0..keys_in_file_order.len())
            .map(|i| (i as u32) + 1)
            .collect();

        let index = NoKeyBoomHashMap::new(keys_in_file_order, values);
        Self { index }
    }

    /// Returns 1-based id. Undefined behavior (wrong id) if key is not in the set.
    #[inline(always)]
    pub fn lookup_unchecked(&self, k: &[u8; 32]) -> u32 {
        self.index.get(k).copied().unwrap()
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
