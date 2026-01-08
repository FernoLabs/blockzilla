use anyhow::{Context, Result};
use gxhash::GxHasher;
use ph::fmph;
use solana_pubkey::Pubkey;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

#[inline]
fn gxhash64<T: Hash + ?Sized>(v: &T) -> u64 {
    let mut h = GxHasher::default();
    v.hash(&mut h);
    h.finish()
}

pub struct KeyIndex {
    /// Minimal perfect hash over all pubkeys
    mphf: fmph::GOFunction,

    /// mphf_index -> 1-based id
    values: Vec<u32>,

    /// Small hot cache for base58 string lookups
    cache: HotCache,
}

impl KeyIndex {
    /// Build index over keys in file order.
    ///
    /// All lookups are assumed to be members of the registry.
    pub fn build(keys_in_file_order: Vec<[u8; 32]>) -> Self {
        let n = keys_in_file_order.len();
        let hot_cap = n.min(10_000);

        // MPHF build
        let mphf: fmph::GOFunction = keys_in_file_order.as_slice().into();

        let mut values = vec![0u32; n];

        // size cache at ~50% load
        let mut cache = HotCache::new(hot_cap * 2);

        for (i, k) in keys_in_file_order.iter().enumerate() {
            let id = i as u32 + 1;

            let idx = mphf.get_or_panic(k) as usize;
            debug_assert!(idx < n);
            values[idx] = id;

            // populate hot string cache
            if i < hot_cap {
                let s = Pubkey::new_from_array(*k).to_string();
                cache.insert(gxhash64(s.as_bytes()), id);
            }
        }

        Self {
            mphf,
            values,
            cache,
        }
    }

    /// Fast path: key MUST exist.
    #[inline(always)]
    pub fn lookup_unchecked(&self, k: &[u8; 32]) -> u32 {
        let idx = self.mphf.get_or_panic(k) as usize;
        let id = self.values[idx];
        debug_assert!(id != 0);
        id
    }

    /// Lookup from base58 string.
    ///
    /// Safe as long as all inputs belong to the registry.
    pub fn lookup_str(&self, k: &str) -> Option<u32> {
        if let Some(id) = self.cache.get(gxhash64(k.as_bytes())) {
            return Some(id);
        }

        let pk = Pubkey::from_str(k).ok()?;
        Some(self.lookup_unchecked(pk.as_array()))
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

#[derive(Debug, Clone)]
struct HotCache {
    keys: Vec<u64>,
    values: Vec<u32>,
    mask: usize,
}

impl HotCache {
    fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(8);
        Self {
            keys: vec![0; cap],
            values: vec![0; cap],
            mask: cap - 1,
        }
    }

    #[inline(always)]
    fn insert(&mut self, k: u64, v: u32) {
        let mut i = k as usize & self.mask;
        loop {
            if self.keys[i] == 0 {
                self.keys[i] = k;
                self.values[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = k as usize & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == 0 {
                return None;
            }
            if kk == k {
                return Some(self.values[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}
