use ahash::AHashMap;
use anyhow::{Context, Result};
use solana_pubkey::{Pubkey, pubkey};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

#[derive(Debug, Clone)]
pub struct Registry {
    pub keys: Vec<[u8; 32]>,
    index: AHashMap<[u8; 32], u32>,
}

impl Registry {
    pub fn build(mut keys: Vec<[u8; 32]>) -> Self {
        const BUILTIN_PROGRAM_KEYS: &[Pubkey] =
            &[pubkey!("ComputeBudget111111111111111111111111111111")];

        // Prepend missing builtins
        for b in BUILTIN_PROGRAM_KEYS {
            let b = b.to_bytes();
            if !keys.contains(&b) {
                keys.insert(0, b);
            }
        }

        let mut index = AHashMap::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            index.insert(*k, i as u32);
        }

        Self { keys, index }
    }

    pub fn get(&self, ix: u32) -> Option<&[u8; 32]> {
        self.keys.get((ix - 1) as usize)
    }

    #[inline(always)]
    pub fn lookup(&self, k: &[u8; 32]) -> Option<u32> {
        self.index.get(k).map(|&ix| ix + 1)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

/// Write registry.bin (raw sorted 32-byte pubkeys, no header)
pub fn write_registry(path: &Path, keys: &[[u8; 32]]) -> Result<()> {
    let f = File::create(path).with_context(|| format!("Failed to create {}", path.display()))?;
    let mut w = BufWriter::with_capacity(64 << 20, f);

    for k in keys {
        w.write_all(k).context("write pubkey")?;
    }

    w.flush().context("flush registry")?;
    Ok(())
}

/// Load registry.bin into memory and build lookup index
pub fn load_registry(path: &Path) -> Result<Registry> {
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

    Ok(Registry::build(keys))
}
