use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use std::os::unix::fs::FileExt;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use xxhash_rust::xxh64::xxh64;

pub const COMPACT_INDEX_MAGIC: &[u8; 8] = b"compiszd";
pub const COMPACT_INDEX_FIXED_HEADER_SIZE: usize = 25;
pub const BUCKET_HEADER_SIZE: usize = 16;
pub const HASH_PREFIX_SIZE: usize = 32;

pub type CompactIndexResult<T> = Result<T, CompactIndexError>;

#[derive(Debug)]
pub enum CompactIndexError {
    Io(std::io::Error),
    InvalidData(String),
}

impl fmt::Display for CompactIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactIndexError::Io(err) => write!(f, "compact index io: {err}"),
            CompactIndexError::InvalidData(message) => {
                write!(f, "invalid compact index: {message}")
            }
        }
    }
}

impl std::error::Error for CompactIndexError {}

impl From<std::io::Error> for CompactIndexError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketHeader {
    pub hash_domain: u32,
    pub num_entries: u32,
    pub hash_len: u8,
    pub data_offset: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactIndexHeader {
    pub header_len: u32,
    pub total_header_size: u64,
    pub value_size: usize,
    pub num_buckets: u32,
    pub version: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactIndexMeta {
    value_size: usize,
    num_buckets: u32,
    version: u8,
    bucket_headers_offset: u64,
    bucket_headers_len: u64,
    buckets: Vec<BucketHeader>,
}

impl CompactIndexMeta {
    pub fn parse_header_prefix(
        prefix: &[u8],
        source: &str,
    ) -> CompactIndexResult<CompactIndexHeader> {
        if prefix.len() < COMPACT_INDEX_FIXED_HEADER_SIZE {
            return Err(CompactIndexError::InvalidData(format!(
                "{source} compact index header prefix too short: got {}, need {}",
                prefix.len(),
                COMPACT_INDEX_FIXED_HEADER_SIZE
            )));
        }

        if &prefix[..8] != COMPACT_INDEX_MAGIC {
            return Err(CompactIndexError::InvalidData(format!(
                "{source} has invalid compact index magic"
            )));
        }

        let header_len = u32::from_le_bytes(prefix[8..12].try_into().expect("slice len"));
        let total_header_size = 8u64
            .checked_add(4)
            .and_then(|value| value.checked_add(header_len as u64))
            .ok_or_else(|| {
                CompactIndexError::InvalidData("compact index header size overflow".to_string())
            })?;
        let value_size = u64::from_le_bytes(prefix[12..20].try_into().expect("slice len")) as usize;
        let num_buckets = u32::from_le_bytes(prefix[20..24].try_into().expect("slice len"));
        let version = prefix[24];

        if total_header_size < COMPACT_INDEX_FIXED_HEADER_SIZE as u64 {
            return Err(CompactIndexError::InvalidData(format!(
                "{source} invalid compact index header size {total_header_size}"
            )));
        }
        if value_size == 0 {
            return Err(CompactIndexError::InvalidData(format!(
                "{source} compact index has zero value size"
            )));
        }
        if num_buckets == 0 {
            return Err(CompactIndexError::InvalidData(format!(
                "{source} compact index has zero buckets"
            )));
        }

        Ok(CompactIndexHeader {
            header_len,
            total_header_size,
            value_size,
            num_buckets,
            version,
        })
    }

    pub fn parse_bucket_headers(
        header: CompactIndexHeader,
        raw: &[u8],
        source: &str,
    ) -> CompactIndexResult<Self> {
        let bucket_headers_len = (header.num_buckets as u64)
            .checked_mul(BUCKET_HEADER_SIZE as u64)
            .ok_or_else(|| {
                CompactIndexError::InvalidData("bucket headers size overflow".to_string())
            })?;
        if raw.len() != bucket_headers_len as usize {
            return Err(CompactIndexError::InvalidData(format!(
                "{source} bucket header length mismatch: expected {bucket_headers_len}, got {}",
                raw.len()
            )));
        }

        let mut buckets = Vec::with_capacity(header.num_buckets as usize);
        for bucket in 0..header.num_buckets as usize {
            let off = bucket * BUCKET_HEADER_SIZE;
            let bytes = &raw[off..off + BUCKET_HEADER_SIZE];

            let hash_domain = u32::from_le_bytes(bytes[0..4].try_into().expect("slice len"));
            let num_entries = u32::from_le_bytes(bytes[4..8].try_into().expect("slice len"));
            let hash_len = bytes[8];

            let mut data_offset = [0u8; 8];
            data_offset[..6].copy_from_slice(&bytes[10..16]);
            let data_offset = u64::from_le_bytes(data_offset);

            buckets.push(BucketHeader {
                hash_domain,
                num_entries,
                hash_len,
                data_offset,
            });
        }

        Ok(Self {
            value_size: header.value_size,
            num_buckets: header.num_buckets,
            version: header.version,
            bucket_headers_offset: header.total_header_size,
            bucket_headers_len,
            buckets,
        })
    }

    pub fn parse(prefix: &[u8], bucket_headers: &[u8], source: &str) -> CompactIndexResult<Self> {
        let header = Self::parse_header_prefix(prefix, source)?;
        Self::parse_bucket_headers(header, bucket_headers, source)
    }

    #[inline]
    pub fn value_size(&self) -> usize {
        self.value_size
    }

    #[inline]
    pub fn num_buckets(&self) -> u32 {
        self.num_buckets
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn bucket_headers_offset(&self) -> u64 {
        self.bucket_headers_offset
    }

    #[inline]
    pub fn bucket_headers_len(&self) -> u64 {
        self.bucket_headers_len
    }

    #[inline]
    pub fn bucket_header(&self, bucket: u32) -> CompactIndexResult<BucketHeader> {
        self.buckets
            .get(bucket as usize)
            .copied()
            .ok_or_else(|| CompactIndexError::InvalidData(format!("bucket {bucket} out of bounds")))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct CompactIndexFile {
    file: File,
    meta: CompactIndexMeta,
}

#[cfg(not(target_arch = "wasm32"))]
impl CompactIndexFile {
    pub fn open(path: &Path) -> CompactIndexResult<Self> {
        let file = File::open(path)?;

        let mut first = [0u8; COMPACT_INDEX_FIXED_HEADER_SIZE];
        file.read_exact_at(&mut first, 0)?;
        let header = CompactIndexMeta::parse_header_prefix(&first, &path.display().to_string())?;
        let bucket_headers_off = header.total_header_size;
        let bucket_headers_len = (header.num_buckets as u64)
            .checked_mul(BUCKET_HEADER_SIZE as u64)
            .ok_or_else(|| {
                CompactIndexError::InvalidData("bucket headers size overflow".to_string())
            })?;

        let mut raw = vec![0u8; bucket_headers_len as usize];
        file.read_exact_at(&mut raw, bucket_headers_off)?;
        let meta =
            CompactIndexMeta::parse_bucket_headers(header, &raw, &path.display().to_string())?;

        Ok(Self { file, meta })
    }

    #[inline]
    pub fn value_size(&self) -> usize {
        self.meta.value_size()
    }

    #[inline]
    pub fn num_buckets(&self) -> u32 {
        self.meta.num_buckets()
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.meta.version()
    }

    #[inline]
    pub fn bucket_header(&self, bucket: u32) -> CompactIndexResult<BucketHeader> {
        self.meta.bucket_header(bucket)
    }

    pub fn read_bucket_payload_into(&self, bucket: u32, buf: &mut [u8]) -> CompactIndexResult<()> {
        let header = self.bucket_header(bucket)?;
        self.file.read_exact_at(buf, header.data_offset)?;
        Ok(())
    }

    pub fn lookup_into_node_reads(&self, key: &[u8], out: &mut [u8]) -> CompactIndexResult<bool> {
        if out.len() != self.value_size() {
            return Err(CompactIndexError::InvalidData(format!(
                "lookup output length {} does not match index value size {}",
                out.len(),
                self.value_size()
            )));
        }

        let bucket = bucket_hash(key, self.num_buckets());
        let header = self.bucket_header(bucket)?;

        let hash_len = header.hash_len as usize;
        let stride = hash_len.checked_add(self.value_size()).ok_or_else(|| {
            CompactIndexError::InvalidData("compact index stride overflow".to_string())
        })?;
        let entries = header.num_entries as usize;

        if entries == 0 {
            return Ok(false);
        }

        let target = truncate_entry_hash(header.hash_domain, key, hash_len);
        let mut index = 0usize;
        let mut node = vec![0u8; stride];

        while index < entries {
            let off = header
                .data_offset
                .checked_add((index * stride) as u64)
                .ok_or_else(|| {
                    CompactIndexError::InvalidData("compact index node offset overflow".to_string())
                })?;
            self.file.read_exact_at(&mut node, off)?;

            let hash = read_hash(&node[..hash_len]);
            if hash == target {
                out.copy_from_slice(&node[hash_len..hash_len + self.value_size()]);
                return Ok(true);
            }

            index = (index << 1) | 1;
            if hash < target {
                index += 1;
            }
        }

        Ok(false)
    }
}

#[inline]
pub fn bucket_hash(key: &[u8], num_buckets: u32) -> u32 {
    debug_assert!(num_buckets > 0);
    let hash = xxh64(key, 0);
    let buckets = num_buckets as u64;
    let mut bucket = hash % buckets;
    if ((hash - bucket) / buckets) < bucket {
        bucket = hash_uint64(bucket);
    }
    (bucket % buckets) as u32
}

#[inline]
const fn hash_uint64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51afd7ed558ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ceb9fe1a85ec53);
    value ^= value >> 33;
    value
}

pub fn entry_hash64(hash_domain: u32, key: &[u8]) -> u64 {
    use xxhash_rust::xxh64::Xxh64;

    let mut prefix = [0u8; HASH_PREFIX_SIZE];
    prefix[..4].copy_from_slice(&hash_domain.to_le_bytes());

    let mut hasher = Xxh64::new(0);
    hasher.update(&prefix);
    hasher.update(key);
    hasher.digest()
}

#[inline]
pub fn truncate_entry_hash(hash_domain: u32, key: &[u8], hash_len: usize) -> u64 {
    let raw = entry_hash64(hash_domain, key);
    if hash_len >= 8 {
        raw
    } else {
        let bits = hash_len * 8;
        let mask = if bits == 64 {
            u64::MAX
        } else {
            (1u64 << bits) - 1
        };
        raw & mask
    }
}

#[inline]
fn read_hash(bytes: &[u8]) -> u64 {
    let mut value = 0u64;
    for (index, byte) in bytes.iter().enumerate() {
        value |= (*byte as u64) << (index * 8);
    }
    value
}

#[inline]
pub fn bst_lookup(
    data: &[u8],
    num_entries: u32,
    hash_len: usize,
    value_size: usize,
    target: u64,
) -> Option<&[u8]> {
    let stride = hash_len + value_size;
    let max = num_entries as usize;
    let mut index = 0usize;

    while index < max {
        let base = index * stride;
        let hash = read_hash(&data[base..base + hash_len]);

        if hash == target {
            let value = base + hash_len;
            return Some(&data[value..value + value_size]);
        }

        index = (index << 1) | 1;
        if hash < target {
            index += 1;
        }
    }

    None
}

#[inline]
pub fn decode_offset_and_size(bytes: &[u8]) -> CompactIndexResult<(u64, u32)> {
    if bytes.len() != 9 {
        return Err(CompactIndexError::InvalidData(format!(
            "invalid offset-and-size value length {}",
            bytes.len()
        )));
    }

    let mut offset = [0u8; 8];
    offset[..6].copy_from_slice(&bytes[..6]);
    let offset = u64::from_le_bytes(offset);

    let mut size = [0u8; 4];
    size[..3].copy_from_slice(&bytes[6..9]);
    let size = u32::from_le_bytes(size);

    Ok((offset, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn compact_index_lookup_reads_value() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "blockzilla-compact-index-test-{}-{}.index",
            std::process::id(),
            unique_suffix()
        ));

        let key = b"slot-key";
        let value = [0xaa, 0xbb];
        let hash_domain = 7u32;
        let hash_len = 8u8;
        let bucket_count = 1u32;
        let value_size = value.len() as u64;
        let header_len = 13u32;
        let bucket_header_offset = 25usize;
        let data_offset = bucket_header_offset + BUCKET_HEADER_SIZE;
        let target = truncate_entry_hash(hash_domain, key, hash_len as usize);

        let mut file = File::create(&path).unwrap();
        file.write_all(COMPACT_INDEX_MAGIC).unwrap();
        file.write_all(&header_len.to_le_bytes()).unwrap();
        file.write_all(&value_size.to_le_bytes()).unwrap();
        file.write_all(&bucket_count.to_le_bytes()).unwrap();
        file.write_all(&[1u8]).unwrap();

        let mut bucket_header = [0u8; BUCKET_HEADER_SIZE];
        bucket_header[0..4].copy_from_slice(&hash_domain.to_le_bytes());
        bucket_header[4..8].copy_from_slice(&1u32.to_le_bytes());
        bucket_header[8] = hash_len;
        bucket_header[10..16].copy_from_slice(&(data_offset as u64).to_le_bytes()[..6]);
        file.write_all(&bucket_header).unwrap();
        file.write_all(&target.to_le_bytes()).unwrap();
        file.write_all(&value).unwrap();
        drop(file);

        let index = CompactIndexFile::open(&path).unwrap();
        let mut out = [0u8; 2];
        assert!(index.lookup_into_node_reads(key, &mut out).unwrap());
        assert_eq!(out, value);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn decodes_offset_and_size() {
        let mut bytes = [0u8; 9];
        bytes[..6].copy_from_slice(&0x0102_0304_0506u64.to_le_bytes()[..6]);
        bytes[6..9].copy_from_slice(&0x0007_0809u32.to_le_bytes()[..3]);
        assert_eq!(
            decode_offset_and_size(&bytes).unwrap(),
            (0x0102_0304_0506, 0x0007_0809)
        );
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
