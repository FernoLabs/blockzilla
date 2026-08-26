use anyhow::{Context, Result};
#[cfg(not(target_arch = "wasm32"))]
use ph::fmph;
use solana_pubkey::Pubkey;
#[cfg(target_arch = "wasm32")]
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Cursor, Seek, SeekFrom};
#[cfg(all(not(target_arch = "wasm32"), unix))]
use std::os::unix::fs::FileExt as _;
#[cfg(all(not(target_arch = "wasm32"), windows))]
use std::os::windows::fs::FileExt as _;
use std::str::FromStr;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::{CompactPubkey, PubkeyResolver};

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

/// Read-only registry lookup that keeps the large MPHF value/tag tables
/// file-backed. Only the compact [`fmph::GOFunction`] tail is decoded into
/// owned memory; a member lookup performs one positioned read for its `u64`
/// membership tag and one for its `u32` value.
#[cfg(not(target_arch = "wasm32"))]
pub struct FileBackedKeyIndex {
    file: File,
    mphf: fmph::GOFunction,
    len: usize,
    values_offset: u64,
    tags_offset: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
struct KeyIndexLayout {
    len: usize,
    values_offset: u64,
    tags_offset: u64,
    mphf_offset: u64,
    file_len: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, PartialEq, Eq)]
struct GoFunctionLayout {
    level_count: usize,
    group_count: usize,
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
        Self::load_file(file, path)
    }

    /// Load and validate an index from one already pinned file descriptor.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_file(file: File, path: &Path) -> Result<Self> {
        let layout = key_index_layout(&file, path)?;
        let mphf = load_preflighted_go_function(&file, layout, path)?;
        let mut reader = BufReader::with_capacity(REGISTRY_IO_BUFFER_SIZE, &file);
        reader.seek(SeekFrom::Start(layout.values_offset))?;

        let mut values = Vec::new();
        values
            .try_reserve_exact(layout.len)
            .context("reserve registry index value table")?;
        let seen_words = layout
            .len
            .checked_add(63)
            .context("registry index id-set length overflow")?
            / 64;
        let mut seen_ids = Vec::new();
        seen_ids
            .try_reserve_exact(seen_words)
            .context("reserve registry index id-set")?;
        seen_ids.resize(seen_words, 0u64);
        for _ in 0..layout.len {
            let id = read_u32_le(&mut reader)?;
            anyhow::ensure!(
                id != 0 && id as usize <= layout.len,
                "registry index id {id} is outside 1..={} in {}",
                layout.len,
                path.display()
            );
            let zero_based = id as usize - 1;
            let word = &mut seen_ids[zero_based / 64];
            let mask = 1u64 << (zero_based % 64);
            anyhow::ensure!(
                *word & mask == 0,
                "registry index contains duplicate id {id} in {}",
                path.display()
            );
            *word |= mask;
            values.push(id);
        }

        let mut tags = Vec::new();
        tags.try_reserve_exact(layout.len)
            .context("reserve registry index tag table")?;
        for _ in 0..layout.len {
            tags.push(read_u64_le(&mut reader)?);
        }

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
            (id != 0 && id as usize <= self.values.len()).then_some(id)
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
impl FileBackedKeyIndex {
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        Self::load_file(file, path)
    }

    /// Load from an already-open file handle.
    ///
    /// Callers that authenticate `registry.mphf` can hash and identify one
    /// retained handle, then pass that same file generation here without a
    /// path reopen (and its accompanying swap race).
    pub fn load_file(file: File, path: &Path) -> Result<Self> {
        let layout = key_index_layout(&file, path)?;
        let mphf = load_preflighted_go_function(&file, layout, path)?;
        Ok(Self {
            file,
            mphf,
            len: layout.len,
            values_offset: layout.values_offset,
            tags_offset: layout.tags_offset,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn lookup(&self, key: &[u8; 32]) -> Result<Option<u32>> {
        let Some(index) = self
            .mphf
            .get(key)
            .and_then(|index| usize::try_from(index).ok())
        else {
            return Ok(None);
        };
        if index >= self.len {
            return Ok(None);
        }
        let tag_offset = self
            .tags_offset
            .checked_add(
                (index as u64)
                    .checked_mul(8)
                    .context("registry tag offset overflow")?,
            )
            .context("registry tag offset overflow")?;
        let mut tag = [0u8; 8];
        read_exact_at(&self.file, &mut tag, tag_offset).context("read registry index tag")?;
        let tag = u64::from_le_bytes(tag);
        if tag != key_tag(key) {
            return Ok(None);
        }
        let value_offset = self
            .values_offset
            .checked_add(
                (index as u64)
                    .checked_mul(4)
                    .context("registry value offset overflow")?,
            )
            .context("registry value offset overflow")?;
        let mut id = [0u8; 4];
        read_exact_at(&self.file, &mut id, value_offset).context("read registry index value")?;
        let id = u32::from_le_bytes(id);
        anyhow::ensure!(
            id != 0 && id as usize <= self.len,
            "registry index id {id} is outside 1..={}",
            self.len
        );
        Ok(Some(id))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn key_index_layout(file: &File, path: &Path) -> Result<KeyIndexLayout> {
    let file_len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    anyhow::ensure!(
        file_len >= KEY_INDEX_HEADER_LEN as u64,
        "registry index is shorter than its header in {}",
        path.display()
    );

    let mut header = [0u8; KEY_INDEX_HEADER_LEN];
    read_exact_at(file, &mut header, 0)
        .with_context(|| format!("read registry index header in {}", path.display()))?;
    anyhow::ensure!(
        &header[..KEY_INDEX_MAGIC.len()] == KEY_INDEX_MAGIC,
        "invalid registry index magic in {}",
        path.display()
    );

    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    anyhow::ensure!(
        version == KEY_INDEX_VERSION,
        "unsupported registry index version {version} in {}",
        path.display()
    );
    let header_len = u16::from_le_bytes(header[10..12].try_into().unwrap()) as usize;
    anyhow::ensure!(
        header_len == KEY_INDEX_HEADER_LEN,
        "unsupported registry index header length {header_len} in {}",
        path.display()
    );

    let len_u64 = u64::from_le_bytes(header[12..20].try_into().unwrap());
    anyhow::ensure!(
        len_u64 <= u32::MAX as u64,
        "registry index key count {len_u64} exceeds compact id range"
    );
    let len = usize::try_from(len_u64).context("registry index key count exceeds usize")?;
    let values_offset = header_len as u64;
    let values_bytes = len_u64
        .checked_mul(4)
        .context("registry index value table length overflow")?;
    let tags_offset = values_offset
        .checked_add(values_bytes)
        .context("registry index tag offset overflow")?;
    let tags_bytes = len_u64
        .checked_mul(8)
        .context("registry index tag table length overflow")?;
    let mphf_offset = tags_offset
        .checked_add(tags_bytes)
        .context("registry index MPHF offset overflow")?;
    anyhow::ensure!(
        mphf_offset <= file_len,
        "registry index tables exceed file length in {}",
        path.display()
    );

    Ok(KeyIndexLayout {
        len,
        values_offset,
        tags_offset,
        mphf_offset,
        file_len,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn load_preflighted_go_function(
    file: &File,
    layout: KeyIndexLayout,
    path: &Path,
) -> Result<fmph::GOFunction> {
    let tail_len_u64 = layout
        .file_len
        .checked_sub(layout.mphf_offset)
        .context("registry index MPHF offset exceeds file length")?;

    // Validate directly from the bounded file region before trusting its
    // length for allocation. This makes a short valid tail followed by a huge
    // amount of junk fail without allocating the declared tail size.
    let expected = {
        let mut reader = BufReader::with_capacity(REGISTRY_IO_BUFFER_SIZE, file);
        reader.seek(SeekFrom::Start(layout.mphf_offset))?;
        preflight_go_function(&mut reader, tail_len_u64, layout.len)
            .with_context(|| format!("preflight registry MPHF in {}", path.display()))?
    };

    let tail_len =
        usize::try_from(tail_len_u64).context("registry index MPHF tail length exceeds usize")?;
    let mut tail = Vec::new();
    tail.try_reserve_exact(tail_len)
        .context("reserve registry index MPHF tail")?;
    tail.resize(tail_len, 0);
    read_exact_at(file, &mut tail, layout.mphf_offset)
        .with_context(|| format!("read registry MPHF in {}", path.display()))?;

    let copied = preflight_go_function(tail.as_slice(), tail_len_u64, layout.len)
        .with_context(|| format!("preflight copied registry MPHF in {}", path.display()))?;
    anyhow::ensure!(
        copied == expected,
        "registry MPHF changed while it was being loaded in {}",
        path.display()
    );
    let mut cursor = Cursor::new(tail.as_slice());
    let mphf = fmph::GOFunction::read(&mut cursor).context("read registry MPHF")?;
    anyhow::ensure!(
        cursor.position() == tail_len_u64,
        "registry MPHF decoder did not consume the exact tail in {}",
        path.display()
    );
    let decoded_group_count = mphf.level_sizes().iter().try_fold(0usize, |sum, groups| {
        sum.checked_add(*groups)
            .context("decoded registry MPHF group count overflow")
    })?;
    anyhow::ensure!(
        mphf.level_sizes().len() == expected.level_count
            && decoded_group_count == expected.group_count,
        "decoded registry MPHF geometry differs from its preflight in {}",
        path.display()
    );
    Ok(mphf)
}

#[cfg(not(target_arch = "wasm32"))]
/// Validates the exact default `ph` 0.11.0 `GOFunction` wire layout before its
/// decoder sees any serialized allocation count or lookup geometry.
fn preflight_go_function(
    input: impl Read,
    serialized_len: u64,
    expected_keys: usize,
) -> Result<GoFunctionLayout> {
    let mut reader = TailReader::new(input, serialized_len);
    let group_bits = reader.read_u8().context("read MPHF group size")?;
    anyhow::ensure!(group_bits == 16, "registry MPHF group size must be 16 bits");

    let level_count = reader.read_vbyte_usize().context("read MPHF level count")?;
    anyhow::ensure!(
        level_count <= usize::try_from(reader.remaining()).unwrap_or(usize::MAX),
        "registry MPHF level count exceeds its remaining bytes"
    );
    anyhow::ensure!(
        (expected_keys == 0) == (level_count == 0),
        "registry MPHF must have no levels exactly when the key count is zero"
    );

    let mut group_count = 0usize;
    for _ in 0..level_count {
        let groups = reader.read_vbyte_usize().context("read MPHF level size")?;
        anyhow::ensure!(groups != 0, "registry MPHF levels must be non-empty");
        anyhow::ensure!(
            groups.is_multiple_of(4),
            "registry MPHF level group count must preserve 64-bit padding"
        );
        group_count = group_count
            .checked_add(groups)
            .context("registry MPHF group count overflow")?;
    }

    let array_bits = group_count
        .checked_mul(16)
        .context("registry MPHF array bit count overflow")?;
    let array_words = array_bits
        .checked_add(63)
        .context("registry MPHF array word count overflow")?
        / 64;
    let mut cardinality = 0usize;
    for _ in 0..array_words {
        cardinality = cardinality
            .checked_add(
                reader
                    .read_u64_le()
                    .context("read registry MPHF bit array")?
                    .count_ones() as usize,
            )
            .context("registry MPHF cardinality overflow")?;
    }
    anyhow::ensure!(
        cardinality == expected_keys,
        "registry MPHF cardinality {cardinality} does not match key count {expected_keys}"
    );

    let seed_bits = reader.read_u8().context("read MPHF seed size")?;
    anyhow::ensure!(seed_bits == 4, "registry MPHF seed size must be 4 bits");
    let seed_bits_total = group_count
        .checked_mul(4)
        .context("registry MPHF seed bit count overflow")?;
    let seed_words = seed_bits_total
        .checked_add(63)
        .context("registry MPHF seed word count overflow")?
        / 64;
    let seed_bytes = seed_words
        .checked_mul(8)
        .context("registry MPHF seed byte count overflow")?;
    reader
        .skip(seed_bytes as u64)
        .context("read registry MPHF seed array")?;
    anyhow::ensure!(reader.remaining() == 0, "registry MPHF has trailing bytes");

    Ok(GoFunctionLayout {
        level_count,
        group_count,
    })
}

#[cfg(not(target_arch = "wasm32"))]
struct TailReader<R> {
    input: R,
    remaining: u64,
    position: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl<R: Read> TailReader<R> {
    fn new(input: R, serialized_len: u64) -> Self {
        Self {
            input,
            remaining: serialized_len,
            position: 0,
        }
    }

    fn remaining(&self) -> u64 {
        self.remaining
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut byte = [0u8; 1];
        self.read_exact(&mut byte)?;
        Ok(byte[0])
    }

    fn read_u64_le(&mut self) -> Result<u64> {
        let mut bytes = [0u8; 8];
        self.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_vbyte_usize(&mut self) -> Result<usize> {
        let mut value = 0u64;
        for index in 0..8u32 {
            let byte = self.read_u8()?;
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte < 0x80 {
                anyhow::ensure!(
                    index as usize + 1 == vbyte_len(value),
                    "registry MPHF uses a non-canonical variable-length integer"
                );
                return usize::try_from(value).context("registry MPHF integer exceeds usize");
            }
        }
        let byte = self.read_u8()?;
        value |= u64::from(byte) << 56;
        anyhow::ensure!(
            vbyte_len(value) == 9,
            "registry MPHF uses a non-canonical variable-length integer"
        );
        usize::try_from(value).context("registry MPHF integer exceeds usize")
    }

    fn skip(&mut self, mut byte_count: u64) -> Result<()> {
        let mut buffer = [0u8; 8192];
        while byte_count != 0 {
            let chunk_len = usize::try_from(byte_count.min(buffer.len() as u64)).unwrap();
            self.read_exact(&mut buffer[..chunk_len])?;
            byte_count -= chunk_len as u64;
        }
        Ok(())
    }

    fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        let byte_count = bytes.len() as u64;
        anyhow::ensure!(
            byte_count <= self.remaining,
            "unexpected end of registry MPHF"
        );
        self.input
            .read_exact(bytes)
            .context("unexpected end of registry MPHF")?;
        self.remaining -= byte_count;
        self.position = self
            .position
            .checked_add(byte_count)
            .context("registry MPHF read offset overflow")?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn vbyte_len(value: u64) -> usize {
    if value >= (1u64 << 56) {
        9
    } else {
        let significant_bits = (u64::BITS - value.leading_zeros()) as usize;
        significant_bits.div_ceil(7).max(1)
    }
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    file.read_exact_at(buffer, offset)
}

#[cfg(all(not(target_arch = "wasm32"), windows))]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buffer.is_empty() {
        let read = file.seek_read(buffer, offset)?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        offset = offset
            .checked_add(read as u64)
            .ok_or(std::io::ErrorKind::InvalidInput)?;
        buffer = &mut buffer[read..];
    }
    Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), not(any(unix, windows))))]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.read_exact(buffer)
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

impl PubkeyResolver for KeyStore {
    #[inline]
    fn resolve_pubkey(&self, id: u32) -> Option<[u8; 32]> {
        self.get(id).copied()
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

    #[cfg(not(target_arch = "wasm32"))]
    fn temporary_index_path(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-key-index-{label}-{}-{unique}.mphf",
            std::process::id()
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn assert_native_loaders_reject(path: &Path) {
        assert!(KeyIndex::load(path).is_err());
        assert!(FileBackedKeyIndex::load(path).is_err());
    }

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

        let path = temporary_index_path("round-trip");

        index.write(&path).unwrap();
        let loaded = KeyIndex::load(&path).unwrap();
        let file_backed = FileBackedKeyIndex::load(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.lookup(&first), Some(1));
        assert_eq!(loaded.lookup(&second), Some(2));
        assert_eq!(loaded.lookup(&third), Some(3));
        assert_eq!(loaded.lookup(&missing), None);
        assert_eq!(file_backed.len(), loaded.len());
        assert_eq!(file_backed.lookup(&first).unwrap(), Some(1));
        assert_eq!(file_backed.lookup(&second).unwrap(), Some(2));
        assert_eq!(file_backed.lookup(&third).unwrap(), Some(3));
        assert_eq!(file_backed.lookup(&missing).unwrap(), None);
        drop(file_backed);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    #[cfg(all(not(target_arch = "wasm32"), unix))]
    fn file_backed_loader_uses_the_supplied_retained_generation() {
        let first = [7u8; 32];
        let second = [8u8; 32];
        let path = temporary_index_path("retained-handle");
        let old_path = path.with_extension("old.mphf");
        KeyIndex::build(vec![first, second]).write(&path).unwrap();
        let retained = File::open(&path).unwrap();

        std::fs::rename(&path, &old_path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();

        let loaded = FileBackedKeyIndex::load_file(retained, &path).unwrap();
        assert_eq!(loaded.lookup(&first).unwrap(), Some(1));
        assert_eq!(loaded.lookup(&second).unwrap(), Some(2));

        drop(loaded);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(old_path).unwrap();
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn empty_key_index_round_trips_to_both_loaders() {
        let path = temporary_index_path("empty");
        KeyIndex::build(Vec::new()).write(&path).unwrap();

        let owned = KeyIndex::load(&path).unwrap();
        let file_backed = FileBackedKeyIndex::load(&path).unwrap();
        assert!(owned.is_empty());
        assert!(file_backed.is_empty());
        assert_eq!(file_backed.lookup(&[1; 32]).unwrap(), None);

        drop(file_backed);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn loaders_reject_truncation_tail_corruption_and_trailing_bytes() {
        let keys = vec![[11u8; 32], [22u8; 32], [33u8; 32]];
        let index = KeyIndex::build(keys);
        let path = temporary_index_path("corruption");
        index.write(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let mphf_offset = KEY_INDEX_HEADER_LEN + index.len() * 12;

        for truncated_len in [
            0,
            KEY_INDEX_HEADER_LEN - 1,
            mphf_offset - 1,
            original.len() - 1,
        ] {
            std::fs::write(&path, &original[..truncated_len]).unwrap();
            assert_native_loaders_reject(&path);
        }

        let mut corrupted = original.clone();
        corrupted[mphf_offset] = 8;
        std::fs::write(&path, &corrupted).unwrap();
        assert_native_loaders_reject(&path);

        let mut corrupted = original.clone();
        corrupted[mphf_offset + 1] = 0xff;
        std::fs::write(&path, &corrupted).unwrap();
        assert_native_loaders_reject(&path);

        let tail_bytes = &original[mphf_offset..];
        let mut tail = TailReader::new(tail_bytes, tail_bytes.len() as u64);
        assert_eq!(tail.read_u8().unwrap(), 16);
        let level_count = tail.read_vbyte_usize().unwrap();
        let mut group_count = 0usize;
        for _ in 0..level_count {
            group_count += tail.read_vbyte_usize().unwrap();
        }
        let content_offset = mphf_offset + tail.position as usize;
        let content_bytes = group_count / 4 * 8;

        let mut corrupted = original.clone();
        corrupted[content_offset..content_offset + content_bytes].fill(0);
        std::fs::write(&path, &corrupted).unwrap();
        assert_native_loaders_reject(&path);

        let mut corrupted = original.clone();
        corrupted[content_offset + content_bytes] = 8;
        std::fs::write(&path, &corrupted).unwrap();
        assert_native_loaders_reject(&path);

        let mut corrupted = original.clone();
        corrupted.push(0);
        std::fs::write(&path, &corrupted).unwrap();
        assert_native_loaders_reject(&path);

        std::fs::write(&path, &original).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(original.len() as u64 + (4u64 << 30))
            .unwrap();
        assert_native_loaders_reject(&path);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn invalid_value_ids_are_rejected_without_out_of_bounds_lookup() {
        let first = [41u8; 32];
        let keys = vec![first, [42u8; 32], [43u8; 32]];
        let index = KeyIndex::build(keys);
        let value_index = index.mphf.get_or_panic(&first) as usize;
        let path = temporary_index_path("invalid-id");
        index.write(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let value_offset = KEY_INDEX_HEADER_LEN + value_index * 4;

        for invalid_id in [0, index.len() as u32 + 1] {
            let mut corrupted = original.clone();
            corrupted[value_offset..value_offset + 4].copy_from_slice(&invalid_id.to_le_bytes());
            std::fs::write(&path, corrupted).unwrap();

            assert!(KeyIndex::load(&path).is_err());
            let file_backed = FileBackedKeyIndex::load(&path).unwrap();
            assert!(file_backed.lookup(&first).is_err());
        }

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn file_backed_lookup_reports_post_load_truncation() {
        let first = [51u8; 32];
        let index = KeyIndex::build(vec![first, [52u8; 32]]);
        let path = temporary_index_path("post-load-truncate");
        index.write(&path).unwrap();
        let file_backed = FileBackedKeyIndex::load(&path).unwrap();

        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(KEY_INDEX_HEADER_LEN as u64)
            .unwrap();
        assert!(file_backed.lookup(&first).is_err());

        drop(file_backed);
        std::fs::remove_file(path).unwrap();
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
        let file_backed = FileBackedKeyIndex::load(&low_a_path).unwrap();
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(loaded.lookup(key), Some(index as u32 + 1));
            assert_eq!(file_backed.lookup(key).unwrap(), Some(index as u32 + 1));
        }
        assert_eq!(loaded.lookup(&[0xff; 32]), None);
        assert_eq!(file_backed.lookup(&[0xff; 32]).unwrap(), None);
        drop(file_backed);
        std::fs::remove_dir_all(root).unwrap();
    }
}
