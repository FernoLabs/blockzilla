use anyhow::{Context, Result};
#[cfg(not(target_arch = "wasm32"))]
use ph::fmph;
use solana_pubkey::Pubkey;
#[cfg(target_arch = "wasm32")]
use std::collections::HashMap;
use std::str::FromStr;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::CompactPubkey;

const KEY_INDEX_MAGIC: &[u8; 8] = b"BZKIDX1!";
const KEY_INDEX_VERSION: u16 = 2;
const KEY_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8;

pub struct KeyIndex {
    /// Minimal perfect hash over all pubkeys
    #[cfg(not(target_arch = "wasm32"))]
    mphf: fmph::GOFunction,

    /// mphf_index -> 1-based id
    #[cfg(not(target_arch = "wasm32"))]
    values: Vec<u32>,

    /// mphf_index -> stable key tag, used to distinguish misses from arbitrary MPHF outputs.
    #[cfg(not(target_arch = "wasm32"))]
    tags: Vec<u64>,

    /// Key -> 1-based id fallback for wasm builds, where the native MPHF dependency is unavailable.
    #[cfg(target_arch = "wasm32")]
    ids: HashMap<[u8; 32], u32>,
}

impl KeyIndex {
    /// Build index over keys in file order.
    ///
    /// All lookups are assumed to be members of the registry.
    pub fn build(keys_in_file_order: Vec<[u8; 32]>) -> Self {
        Self::build_from_slice(&keys_in_file_order)
    }

    /// Build index over keys in file order without requiring an owned clone.
    ///
    /// All lookups are assumed to be members of the registry.
    pub fn build_from_slice(keys_in_file_order: &[[u8; 32]]) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let n = keys_in_file_order.len();

            // MPHF build
            let mphf: fmph::GOFunction = keys_in_file_order.into();

            let mut values = vec![0u32; n];
            let mut tags = vec![0u64; n];

            for (i, k) in keys_in_file_order.iter().enumerate() {
                let id = i as u32 + 1;

                let idx = mphf.get_or_panic(k) as usize;
                debug_assert!(idx < n);
                values[idx] = id;
                tags[idx] = key_tag(k);
            }

            Self { mphf, values, tags }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let ids = keys_in_file_order
                .iter()
                .copied()
                .enumerate()
                .map(|(i, key)| (key, i as u32 + 1))
                .collect();
            Self { ids }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.values.len()
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.ids.len()
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn write(&self, path: &Path) -> Result<()> {
        let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(64 << 20, file);
        writer.write_all(KEY_INDEX_MAGIC)?;
        writer.write_all(&KEY_INDEX_VERSION.to_le_bytes())?;
        writer.write_all(&(KEY_INDEX_HEADER_LEN as u16).to_le_bytes())?;
        writer.write_all(&(self.values.len() as u64).to_le_bytes())?;
        for value in &self.values {
            writer.write_all(&value.to_le_bytes())?;
        }
        for tag in &self.tags {
            writer.write_all(&tag.to_le_bytes())?;
        }
        self.mphf
            .write(&mut writer)
            .context("write registry MPHF")?;
        writer
            .flush()
            .with_context(|| format!("flush {}", path.display()))?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut reader = BufReader::with_capacity(64 << 20, file);

        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        anyhow::ensure!(
            &magic == KEY_INDEX_MAGIC,
            "invalid registry index magic in {}",
            path.display()
        );

        let version = read_u16_le(&mut reader)?;
        anyhow::ensure!(
            version == KEY_INDEX_VERSION,
            "unsupported registry index version {version} in {}",
            path.display()
        );
        let header_len = read_u16_le(&mut reader)? as usize;
        anyhow::ensure!(
            header_len == KEY_INDEX_HEADER_LEN,
            "unsupported registry index header length {header_len} in {}",
            path.display()
        );

        let len = read_u64_le(&mut reader)?;
        anyhow::ensure!(
            len <= u32::MAX as u64,
            "registry index key count {len} exceeds compact id range"
        );
        let len = len as usize;

        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(read_u32_le(&mut reader)?);
        }

        let mut tags = Vec::with_capacity(len);
        for _ in 0..len {
            tags.push(read_u64_le(&mut reader)?);
        }

        let mphf = fmph::GOFunction::read(&mut reader).context("read registry MPHF")?;
        Ok(Self { mphf, values, tags })
    }

    /// Checked lookup. Returns None when `k` is not in the registry.
    #[inline(always)]
    pub fn lookup(&self, k: &[u8; 32]) -> Option<u32> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let idx = self.mphf.get(k)? as usize;
            if self.tags.get(idx)? != &key_tag(k) {
                return None;
            }
            let id = self.values[idx];
            (id != 0).then_some(id)
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.ids.get(k).copied()
        }
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

#[cfg(not(target_arch = "wasm32"))]
fn key_tag(key: &[u8; 32]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn read_u16_le(reader: &mut impl Read) -> Result<u16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32_le(reader: &mut impl Read) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le(reader: &mut impl Read) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
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
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn key_index_round_trips_to_sidecar() {
        let first = [0u8; 32];
        let second = [1u8; 32];
        let third = [2u8; 32];
        let missing = [3u8; 32];
        let index = KeyIndex::build(vec![first, second, third]);

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("blockzilla-key-index-{unique}.mphf"));

        index.write(&path).unwrap();
        let loaded = KeyIndex::load(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.lookup(&first), Some(1));
        assert_eq!(loaded.lookup(&second), Some(2));
        assert_eq!(loaded.lookup(&third), Some(3));
        assert_eq!(loaded.lookup(&missing), None);
    }
}
