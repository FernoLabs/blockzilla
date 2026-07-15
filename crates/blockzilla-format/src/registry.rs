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
const REGISTRY_IO_BUFFER_SIZE: usize = 8 << 20;

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

pub trait PubkeyCompactor {
    fn compact_str(&self, k: &str) -> Option<CompactPubkey>;
}

pub struct RawPubkeyCompactor;

impl PubkeyCompactor for RawPubkeyCompactor {
    #[inline]
    fn compact_str(&self, k: &str) -> Option<CompactPubkey> {
        let bytes = known_raw_pubkey(k).or_else(|| decode_pubkey_base58_32(k))?;
        Some(CompactPubkey::raw(bytes))
    }
}

#[inline]
fn decode_pubkey_base58_32(k: &str) -> Option<[u8; 32]> {
    let mut bytes = [0u8; 32];
    five8::decode_32(k, &mut bytes).ok()?;
    Some(bytes)
}

#[inline]
fn known_raw_pubkey(k: &str) -> Option<[u8; 32]> {
    let pk = match k {
        "11111111111111111111111111111111" => {
            solana_pubkey::pubkey!("11111111111111111111111111111111")
        }
        "ComputeBudget111111111111111111111111111111" => {
            solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111")
        }
        "Vote111111111111111111111111111111111111111" => {
            solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111")
        }
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => {
            solana_pubkey::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        }
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL" => {
            solana_pubkey::pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
        }
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" => {
            solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
        }
        _ => return None,
    };
    Some(pk.to_bytes())
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
            // MPHF build
            let mphf: fmph::GOFunction = keys_in_file_order.into();
            Self::from_mphf_and_keys(mphf, keys_in_file_order)
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

    /// Build an index without caching one `u64` hash per key during MPHF
    /// construction. This is slower than [`Self::build_from_slice`], but avoids
    /// an 8-byte-per-key peak allocation and is intended for full-epoch
    /// registries that are already backed by immutable storage.
    pub fn build_from_slice_low_memory(keys_in_file_order: &[[u8; 32]]) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let conf = fmph::GOBuildConf::with_ct(fmph::GOConf::default(), 0);
            let mphf = fmph::GOFunction::from_slice_with_conf(keys_in_file_order, conf);
            Self::from_mphf_and_keys(mphf, keys_in_file_order)
        }

        #[cfg(target_arch = "wasm32")]
        {
            Self::build_from_slice(keys_in_file_order)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn from_mphf_and_keys(mphf: fmph::GOFunction, keys_in_file_order: &[[u8; 32]]) -> Self {
        let n = keys_in_file_order.len();
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
        let mut writer = BufWriter::with_capacity(REGISTRY_IO_BUFFER_SIZE, file);
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
        let mut reader = BufReader::with_capacity(REGISTRY_IO_BUFFER_SIZE, file);

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

impl PubkeyCompactor for KeyIndex {
    #[inline]
    fn compact_str(&self, k: &str) -> Option<CompactPubkey> {
        KeyIndex::compact_str(self, k)
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
        let mut r = BufReader::with_capacity(REGISTRY_IO_BUFFER_SIZE, f);

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
    write_registry_iter(path, keys.iter().copied())
}

/// Write registry.bin from a streaming key source.
pub fn write_registry_iter<I>(path: &Path, keys: I) -> Result<()>
where
    I: IntoIterator<Item = [u8; 32]>,
{
    let f = File::create(path).with_context(|| format!("Failed to create {}", path.display()))?;
    let mut w = BufWriter::with_capacity(REGISTRY_IO_BUFFER_SIZE, f);

    for k in keys {
        w.write_all(&k).context("write pubkey")?;
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

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn low_memory_key_index_is_deterministic_and_format_compatible() {
        let keys = (0u64..10_000)
            .map(|value| {
                let mut key = [0u8; 32];
                key[..8].copy_from_slice(&value.to_le_bytes());
                key[8..16].copy_from_slice(&value.wrapping_mul(17).to_be_bytes());
                key
            })
            .collect::<Vec<_>>();
        let default = KeyIndex::build_from_slice(&keys);
        let low_memory_a = KeyIndex::build_from_slice_low_memory(&keys);
        let low_memory_b = KeyIndex::build_from_slice_low_memory(&keys);

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "blockzilla-low-memory-key-index-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let default_path = root.join("default.mphf");
        let low_a_path = root.join("low-a.mphf");
        let low_b_path = root.join("low-b.mphf");
        default.write(&default_path).unwrap();
        low_memory_a.write(&low_a_path).unwrap();
        low_memory_b.write(&low_b_path).unwrap();

        let default_bytes = std::fs::read(&default_path).unwrap();
        let low_a_bytes = std::fs::read(&low_a_path).unwrap();
        let low_b_bytes = std::fs::read(&low_b_path).unwrap();
        assert_eq!(low_a_bytes, low_b_bytes);
        assert_eq!(low_a_bytes, default_bytes);

        let loaded = KeyIndex::load(&low_a_path).unwrap();
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(loaded.lookup(key), Some(index as u32 + 1));
        }
        assert_eq!(loaded.lookup(&[0xff; 32]), None);
        std::fs::remove_dir_all(root).unwrap();
    }
}
