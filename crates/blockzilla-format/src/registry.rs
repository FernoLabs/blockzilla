use anyhow::{Context, Result};
use rustc_hash::{FxBuildHasher, FxHashMap};
use solana_pubkey::Pubkey;
use std::hash::{Hash, Hasher};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    str::FromStr,
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

/// Key -> id index using 128-bit prefix fingerprint.
/// Ids are 1-based (0 reserved).
#[derive(Debug, Clone)]
pub struct KeyIndex {
    index: FxHashMap<[u8; 32], u32>, // fingerprint -> id
    index_hot: FxHashMap<[u8; 32], u32>,
    cache: FxHashMap<u64, u32>, // string to pubk cache for most use pubk
}

#[inline]
fn fxhash(bytes: &[u8]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    bytes.hash(&mut hasher);
    hasher.finish()
}

impl KeyIndex {
    /// Build index over keys in file order.
    pub fn build(keys_in_file_order: Vec<[u8; 32]>) -> Self {
        let total = keys_in_file_order.len();
        let hot_cap = total.min(10_000);

        let mut index =
            FxHashMap::with_capacity_and_hasher(total.saturating_sub(hot_cap), FxBuildHasher);
        let mut index_hot = FxHashMap::with_capacity_and_hasher(hot_cap, FxBuildHasher);
        let mut cache = FxHashMap::with_capacity_and_hasher(hot_cap, FxBuildHasher);

        // fill hot (first hot_cap keys)
        for (i, k) in keys_in_file_order.iter().enumerate().take(hot_cap) {
            let id = i as u32 + 1;
            let pubk_str = Pubkey::new_from_array(*k).to_string();
            cache.insert(fxhash(pubk_str.as_bytes()), id);
            index_hot.insert(*k, id);
        }

        // fill cold (rest)
        for (i, k) in keys_in_file_order.into_iter().enumerate().skip(hot_cap) {
            let id = i as u32 + 1;
            index.insert(k, id);
        }

        index.shrink_to_fit();

        Self {
            index,
            index_hot,
            cache,
        }
    }

    pub fn lookup_str(&self, k: &str) -> Option<u32> {
        if let Some(id) = self.cache.get(&fxhash(k.as_bytes())) {
            return Some(*id);
        }

        let pk = Pubkey::from_str(k).ok()?;
        let a = pk.as_array();

        self.index_hot
            .get(a)
            .copied()
            .or_else(|| self.index.get(a).copied())
    }

    #[inline(always)]
    pub fn lookup_unchecked(&self, k: &[u8; 32]) -> u32 {
        match self.index_hot.get(k) {
            Some(id) => *id,
            None => *self.index.get(k).expect("missing key"),
        }
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
