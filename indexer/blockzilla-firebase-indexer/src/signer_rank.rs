//! Persisted signer discovery and constant-time registry-id -> dense-rank lookup.
//!
//! Registry id `N` is represented by bit `N - 1` in the raw bitset.  A little-
//! endian `u32` prefix count is stored at the start of every 128-bit block, plus
//! one terminal count.  A lookup therefore reads one prefix and popcounts at
//! most the preceding 127 bits.  For the measured 50.9-million-entry epoch
//! registry, the raw bitset is about 6.36 MB and the persisted directory is
//! about 1.59 MB, or roughly 7.95 MB together before the fixed header.
//!
//! The artifact is deliberately self-binding: its fixed header records the V1
//! Firewatch semantic policy, archive-generation digest, exact registry size
//! and digest, registry entry count, and a digest of the bitset + rank payload.
//! Readers require the expected binding, validate the exact file geometry, and
//! recompute every prefix before exposing a lookup.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    iter::FusedIterator,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rustix::fs::{CWD, Mode, OFlags, RenameFlags, renameat_with};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SIGNER_SET_FILE_NAME: &str = "signers.bits";
pub const SIGNER_SET_FORMAT_VERSION: u32 = 1;
pub const SIGNER_SET_SEMANTICS_VERSION: u32 = 1;

/// Keeps a hostile artifact from requesting an arbitrarily large allocation.
/// At the limit the raw bitset is 32 MiB and the rank directory is 8 MiB.
pub const MAX_SIGNER_SET_REGISTRY_ENTRIES: u32 = 268_435_456;

const MAGIC: [u8; 8] = *b"FWSIGNR\0";
const HEADER_LEN: usize = 160;
const RANK_STRIDE_BITS: u32 = 128;
const RANK_STRIDE_BYTES: usize = (RANK_STRIDE_BITS / 8) as usize;
const REGISTRY_ENTRY_BYTES: u64 = 32;

const POLICY_ALL_TRANSACTION_SIGNERS: u32 = 1 << 0;
const POLICY_DIRECT_AND_CPI_PROGRAMS: u32 = 1 << 1;
const POLICY_EXCLUDE_FAILED_TRANSACTIONS: u32 = 1 << 2;
const POLICY_INCLUDE_VOTE_TRANSACTIONS: u32 = 1 << 3;
const SEMANTIC_POLICY_V1: u32 = POLICY_ALL_TRANSACTION_SIGNERS
    | POLICY_DIRECT_AND_CPI_PROGRAMS
    | POLICY_EXCLUDE_FAILED_TRANSACTIONS
    | POLICY_INCLUDE_VOTE_TRANSACTIONS;

const OFFSET_FORMAT_VERSION: usize = 8;
const OFFSET_HEADER_LEN: usize = 12;
const OFFSET_SEMANTICS_VERSION: usize = 16;
const OFFSET_SEMANTIC_POLICY: usize = 20;
const OFFSET_REGISTRY_ENTRIES: usize = 24;
const OFFSET_RANK_STRIDE: usize = 28;
const OFFSET_SIGNER_COUNT: usize = 32;
const OFFSET_RESERVED: usize = 36;
const OFFSET_REGISTRY_SIZE: usize = 40;
const OFFSET_BITSET_BYTES: usize = 48;
const OFFSET_RANK_ENTRIES: usize = 56;
const OFFSET_GENERATION_DIGEST: usize = 64;
const OFFSET_REGISTRY_SHA256: usize = 96;
const OFFSET_PAYLOAD_SHA256: usize = 128;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignerSetBinding {
    pub registry_entries: u32,
    pub generation_digest: [u8; 32],
    pub registry_size: u64,
    pub registry_sha256: [u8; 32],
}

impl SignerSetBinding {
    fn validate(self) -> Result<(), SignerSetError> {
        if self.registry_entries == 0 {
            return Err(SignerSetError::InvalidArgument {
                message: "registry_entries must be nonzero".into(),
            });
        }
        if self.registry_entries > MAX_SIGNER_SET_REGISTRY_ENTRIES {
            return Err(SignerSetError::InvalidArgument {
                message: format!(
                    "registry_entries {} exceeds the signer-set limit {}",
                    self.registry_entries, MAX_SIGNER_SET_REGISTRY_ENTRIES
                ),
            });
        }
        let expected_size = u64::from(self.registry_entries) * REGISTRY_ENTRY_BYTES;
        if self.registry_size != expected_size {
            return Err(SignerSetError::InvalidArgument {
                message: format!(
                    "registry_size {} does not match {} fixed-size registry entries ({expected_size})",
                    self.registry_size, self.registry_entries
                ),
            });
        }
        if self.generation_digest == [0; 32] {
            return Err(SignerSetError::InvalidArgument {
                message: "generation_digest must be a real nonzero SHA-256 binding".into(),
            });
        }
        if self.registry_sha256 == [0; 32] {
            return Err(SignerSetError::InvalidArgument {
                message: "registry_sha256 must be a real nonzero SHA-256 binding".into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SignerSetError {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("invalid signer-set argument: {message}")]
    InvalidArgument { message: String },
    #[error("invalid signer-set artifact at {path}: {message}")]
    InvalidArtifact { path: String, message: String },
    #[error("signer-set binding mismatch for {field} at {path}")]
    BindingMismatch { path: String, field: &'static str },
    #[error(
        "registry id {registry_id} is outside the signer-set registry range 1..={registry_entries}"
    )]
    InvalidRegistryId {
        registry_id: u32,
        registry_entries: u32,
    },
    #[error(
        "cannot merge signer sets with registry entry counts {left_registry_entries} and {right_registry_entries}"
    )]
    MergeRegistryMismatch {
        left_registry_entries: u32,
        right_registry_entries: u32,
    },
}

/// Mutable pass-1 signer discovery state.  Insert order and merge order do not
/// affect the finalized bytes.
pub struct SignerSetBuilder {
    registry_entries: u32,
    bitset: Vec<u8>,
    signer_count: u32,
}

impl fmt::Debug for SignerSetBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignerSetBuilder")
            .field("registry_entries", &self.registry_entries)
            .field("signer_count", &self.signer_count)
            .finish_non_exhaustive()
    }
}

impl SignerSetBuilder {
    pub fn new(registry_entries: u32) -> Result<Self, SignerSetError> {
        validate_registry_entries(registry_entries)?;
        let bitset_len = bitset_len(registry_entries);
        let mut bitset = Vec::new();
        bitset
            .try_reserve_exact(bitset_len)
            .map_err(|_| SignerSetError::InvalidArgument {
                message: format!("could not reserve {bitset_len} bytes for the signer bitset"),
            })?;
        bitset.resize(bitset_len, 0);
        Ok(Self {
            registry_entries,
            bitset,
            signer_count: 0,
        })
    }

    /// Insert a 1-based registry id, returning `true` only for its first
    /// insertion.
    #[inline]
    pub fn insert(&mut self, registry_id: u32) -> Result<bool, SignerSetError> {
        let bit_index = checked_bit_index(registry_id, self.registry_entries)?;
        let byte = &mut self.bitset[bit_index / 8];
        let mask = 1u8 << (bit_index % 8);
        if *byte & mask != 0 {
            return Ok(false);
        }
        *byte |= mask;
        self.signer_count += 1;
        Ok(true)
    }

    /// Union a parallel discovery result covering the exact same registry.
    pub fn merge(&mut self, other: Self) -> Result<(), SignerSetError> {
        if self.registry_entries != other.registry_entries {
            return Err(SignerSetError::MergeRegistryMismatch {
                left_registry_entries: self.registry_entries,
                right_registry_entries: other.registry_entries,
            });
        }
        for (mine, theirs) in self.bitset.iter_mut().zip(other.bitset) {
            let newly_set = (theirs & !*mine).count_ones();
            *mine |= theirs;
            self.signer_count += newly_set;
        }
        Ok(())
    }

    pub fn registry_entries(&self) -> u32 {
        self.registry_entries
    }

    pub fn signer_count(&self) -> u32 {
        self.signer_count
    }

    /// Attach the immutable generation/registry provenance and construct the
    /// validated constant-time rank view.
    pub fn finish(self, binding: SignerSetBinding) -> Result<SignerRank, SignerSetError> {
        binding.validate()?;
        if binding.registry_entries != self.registry_entries {
            return Err(SignerSetError::InvalidArgument {
                message: format!(
                    "builder registry entry count {} does not match binding count {}",
                    self.registry_entries, binding.registry_entries
                ),
            });
        }
        let ranks = build_rank_directory(&self.bitset, self.registry_entries)?;
        let rank = SignerRank {
            binding,
            bitset: self.bitset,
            ranks,
            signer_count: self.signer_count,
        };
        rank.validate_payload("<memory>")?;
        Ok(rank)
    }
}

/// Fully validated signer membership and rank state.
pub struct SignerRank {
    binding: SignerSetBinding,
    bitset: Vec<u8>,
    ranks: Vec<u32>,
    signer_count: u32,
}

impl fmt::Debug for SignerRank {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignerRank")
            .field("binding", &self.binding)
            .field("signer_count", &self.signer_count)
            .finish_non_exhaustive()
    }
}

impl SignerRank {
    /// Load a retained regular file, require its exact provenance, verify its
    /// payload digest, and recompute its complete rank directory.
    pub fn open(path: &Path, expected: SignerSetBinding) -> Result<Self, SignerSetError> {
        expected.validate()?;
        let file = open_regular_file(path)?;
        let initial_identity = file_identity(&file, path)?;
        let maximum_len = maximum_artifact_len();
        if initial_identity.size > maximum_len {
            return Err(invalid_artifact(
                path,
                format!(
                    "file size {} exceeds the bounded maximum {maximum_len}",
                    initial_identity.size
                ),
            ));
        }
        if initial_identity.size < HEADER_LEN as u64 {
            return Err(invalid_artifact(
                path,
                format!(
                    "truncated header: expected {HEADER_LEN} bytes, found {}",
                    initial_identity.size
                ),
            ));
        }

        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut header = [0u8; HEADER_LEN];
        read_exact(&mut reader, &mut header, path)?;
        let decoded = decode_header(path, &header)?;
        compare_binding(path, decoded.binding, expected)?;

        let expected_bitset_bytes = bitset_len(decoded.binding.registry_entries) as u64;
        if decoded.bitset_bytes != expected_bitset_bytes {
            return Err(invalid_artifact(
                path,
                format!(
                    "bitset length {} does not match registry geometry {expected_bitset_bytes}",
                    decoded.bitset_bytes
                ),
            ));
        }
        let expected_rank_entries = rank_entry_count(decoded.binding.registry_entries) as u64;
        if decoded.rank_entries != expected_rank_entries {
            return Err(invalid_artifact(
                path,
                format!(
                    "rank entry count {} does not match registry geometry {expected_rank_entries}",
                    decoded.rank_entries
                ),
            ));
        }
        let expected_len = artifact_len(decoded.binding.registry_entries);
        if initial_identity.size != expected_len {
            return Err(invalid_artifact(
                path,
                format!(
                    "invalid length: expected exactly {expected_len} bytes, found {}",
                    initial_identity.size
                ),
            ));
        }

        let bitset_len = usize::try_from(decoded.bitset_bytes).map_err(|_| {
            invalid_artifact(path, "bitset length does not fit this platform".into())
        })?;
        let rank_count = usize::try_from(decoded.rank_entries).map_err(|_| {
            invalid_artifact(path, "rank entry count does not fit this platform".into())
        })?;
        let mut bitset = allocate_zeroed(bitset_len, path, "bitset")?;
        read_exact(&mut reader, &mut bitset, path)?;
        let mut payload_hasher = Sha256::new();
        payload_hasher.update(&bitset);
        let mut ranks = Vec::new();
        ranks
            .try_reserve_exact(rank_count)
            .map_err(|_| invalid_artifact(path, "could not reserve rank directory".into()))?;
        for _ in 0..rank_count {
            let mut bytes = [0u8; 4];
            read_exact(&mut reader, &mut bytes, path)?;
            payload_hasher.update(bytes);
            ranks.push(u32::from_le_bytes(bytes));
        }

        let mut trailing = [0u8; 1];
        if reader
            .read(&mut trailing)
            .map_err(|source| SignerSetError::Io {
                path: path.display().to_string(),
                source,
            })?
            != 0
        {
            return Err(invalid_artifact(path, "trailing bytes".into()));
        }
        if file_identity(reader.get_ref(), path)? != initial_identity {
            return Err(invalid_artifact(
                path,
                "file changed while it was being read".into(),
            ));
        }

        let actual_payload_sha256: [u8; 32] = payload_hasher.finalize().into();
        if actual_payload_sha256 != decoded.payload_sha256 {
            return Err(invalid_artifact(path, "payload SHA-256 mismatch".into()));
        }
        let rank = Self {
            binding: decoded.binding,
            bitset,
            ranks,
            signer_count: decoded.signer_count,
        };
        rank.validate_payload(&path.display().to_string())?;
        Ok(rank)
    }

    /// Persist to a unique same-directory temporary file, sync the file,
    /// atomically rename without replacing an existing artifact, then sync
    /// the containing directory.
    pub fn write_atomic(&self, path: &Path) -> Result<(), SignerSetError> {
        self.binding.validate()?;
        self.validate_payload("<memory>")?;
        let parent = output_parent(path);
        let file_name = path
            .file_name()
            .ok_or_else(|| SignerSetError::InvalidArgument {
                message: format!("artifact path {} has no file name", path.display()),
            })?;
        let (temporary_path, mut temporary) = create_temporary(parent, file_name)?;

        let result = (|| {
            let mut payload_hasher = Sha256::new();
            payload_hasher.update(&self.bitset);
            for rank in &self.ranks {
                payload_hasher.update(rank.to_le_bytes());
            }
            let payload_sha256: [u8; 32] = payload_hasher.finalize().into();
            let header = encode_header(
                self.binding,
                self.signer_count,
                self.bitset.len() as u64,
                self.ranks.len() as u64,
                payload_sha256,
            );

            {
                let mut writer = BufWriter::new(&mut temporary);
                writer
                    .write_all(&header)
                    .and_then(|()| writer.write_all(&self.bitset))
                    .map_err(|source| SignerSetError::Io {
                        path: temporary_path.display().to_string(),
                        source,
                    })?;
                for rank in &self.ranks {
                    writer
                        .write_all(&rank.to_le_bytes())
                        .map_err(|source| SignerSetError::Io {
                            path: temporary_path.display().to_string(),
                            source,
                        })?;
                }
                writer.flush().map_err(|source| SignerSetError::Io {
                    path: temporary_path.display().to_string(),
                    source,
                })?;
            }
            temporary.sync_all().map_err(|source| SignerSetError::Io {
                path: temporary_path.display().to_string(),
                source,
            })?;
            renameat_with(CWD, &temporary_path, CWD, path, RenameFlags::NOREPLACE)
                .map_err(io::Error::from)
                .map_err(|source| SignerSetError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
            sync_directory(parent)?;
            Ok(())
        })();

        if result.is_err() && temporary_path.exists() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }

    /// Map a 1-based registry id to its zero-based dense signer rank.
    /// Non-signers and ids outside this artifact return `None`.
    #[inline]
    pub fn rank(&self, registry_id: u32) -> Option<u32> {
        let bit_index = registry_id
            .checked_sub(1)
            .filter(|index| *index < self.binding.registry_entries)?
            as usize;
        let byte_index = bit_index / 8;
        let bit_in_byte = bit_index % 8;
        if self.bitset[byte_index] & (1 << bit_in_byte) == 0 {
            return None;
        }

        let rank_block = bit_index / RANK_STRIDE_BITS as usize;
        let block_start_byte = rank_block * RANK_STRIDE_BYTES;
        let full_bytes = bit_in_rank_block(bit_index) / 8;
        let mut local_rank = 0u32;
        for byte in &self.bitset[block_start_byte..block_start_byte + full_bytes] {
            local_rank += byte.count_ones();
        }
        let bits_before = bit_in_byte;
        if bits_before != 0 {
            let mask = (1u8 << bits_before) - 1;
            local_rank += (self.bitset[byte_index] & mask).count_ones();
        }
        Some(self.ranks[rank_block] + local_rank)
    }

    /// Iterate `(zero_based_dense_rank, registry_id)` without allocating a
    /// dense-rank -> registry-id table.
    pub fn iter_ids(&self) -> impl ExactSizeIterator<Item = (u32, u32)> + FusedIterator + '_ {
        SignerIdIter {
            bitset: &self.bitset,
            registry_entries: self.binding.registry_entries,
            byte_index: 0,
            pending: 0,
            next_rank: 0,
            remaining: self.signer_count,
        }
    }

    pub fn binding(&self) -> SignerSetBinding {
        self.binding
    }

    pub fn registry_entries(&self) -> u32 {
        self.binding.registry_entries
    }

    pub fn signer_count(&self) -> u32 {
        self.signer_count
    }

    fn validate_payload(&self, path: &str) -> Result<(), SignerSetError> {
        if self.bitset.len() != bitset_len(self.binding.registry_entries) {
            return Err(invalid_artifact_str(
                path,
                "invalid in-memory bitset length",
            ));
        }
        if self.ranks.len() != rank_entry_count(self.binding.registry_entries) {
            return Err(invalid_artifact_str(
                path,
                "invalid in-memory rank directory length",
            ));
        }
        validate_padding(&self.bitset, self.binding.registry_entries)
            .map_err(|message| invalid_artifact_str(path, message))?;

        let blocks = self.binding.registry_entries.div_ceil(RANK_STRIDE_BITS) as usize;
        let mut cumulative = 0u32;
        for block in 0..blocks {
            if self.ranks[block] != cumulative {
                return Err(invalid_artifact_str(
                    path,
                    format!(
                        "rank prefix {block} is {}, expected {cumulative}",
                        self.ranks[block]
                    ),
                ));
            }
            let start = block * RANK_STRIDE_BYTES;
            let end = (start + RANK_STRIDE_BYTES).min(self.bitset.len());
            for byte in &self.bitset[start..end] {
                cumulative += byte.count_ones();
            }
        }
        if self.ranks[blocks] != cumulative {
            return Err(invalid_artifact_str(
                path,
                format!(
                    "terminal rank is {}, expected {cumulative}",
                    self.ranks[blocks]
                ),
            ));
        }
        if self.signer_count != cumulative {
            return Err(invalid_artifact_str(
                path,
                format!(
                    "declared signer count is {}, bitset contains {cumulative}",
                    self.signer_count
                ),
            ));
        }
        Ok(())
    }
}

struct SignerIdIter<'a> {
    bitset: &'a [u8],
    registry_entries: u32,
    byte_index: usize,
    pending: u8,
    next_rank: u32,
    remaining: u32,
}

impl Iterator for SignerIdIter<'_> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pending != 0 {
                let bit = self.pending.trailing_zeros() as usize;
                self.pending &= self.pending - 1;
                let registry_id = ((self.byte_index - 1) * 8 + bit + 1) as u32;
                if registry_id > self.registry_entries {
                    return None;
                }
                let rank = self.next_rank;
                self.next_rank += 1;
                self.remaining -= 1;
                return Some((rank, registry_id));
            }
            let byte = *self.bitset.get(self.byte_index)?;
            self.byte_index += 1;
            self.pending = byte;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining as usize;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for SignerIdIter<'_> {}
impl FusedIterator for SignerIdIter<'_> {}

#[derive(Debug)]
struct DecodedHeader {
    binding: SignerSetBinding,
    signer_count: u32,
    bitset_bytes: u64,
    rank_entries: u64,
    payload_sha256: [u8; 32],
}

fn decode_header(path: &Path, header: &[u8; HEADER_LEN]) -> Result<DecodedHeader, SignerSetError> {
    if header[..MAGIC.len()] != MAGIC {
        return Err(invalid_artifact(path, "bad magic".into()));
    }
    require_header_u32(
        path,
        header,
        OFFSET_FORMAT_VERSION,
        SIGNER_SET_FORMAT_VERSION,
        "format version",
    )?;
    require_header_u32(
        path,
        header,
        OFFSET_HEADER_LEN,
        HEADER_LEN as u32,
        "header length",
    )?;
    require_header_u32(
        path,
        header,
        OFFSET_SEMANTICS_VERSION,
        SIGNER_SET_SEMANTICS_VERSION,
        "semantics version",
    )?;
    require_header_u32(
        path,
        header,
        OFFSET_SEMANTIC_POLICY,
        SEMANTIC_POLICY_V1,
        "semantic policy",
    )?;
    require_header_u32(
        path,
        header,
        OFFSET_RANK_STRIDE,
        RANK_STRIDE_BITS,
        "rank stride",
    )?;
    require_header_u32(path, header, OFFSET_RESERVED, 0, "reserved field")?;

    let binding = SignerSetBinding {
        registry_entries: header_u32(header, OFFSET_REGISTRY_ENTRIES),
        generation_digest: header[OFFSET_GENERATION_DIGEST..OFFSET_GENERATION_DIGEST + 32]
            .try_into()
            .unwrap(),
        registry_size: header_u64(header, OFFSET_REGISTRY_SIZE),
        registry_sha256: header[OFFSET_REGISTRY_SHA256..OFFSET_REGISTRY_SHA256 + 32]
            .try_into()
            .unwrap(),
    };
    binding
        .validate()
        .map_err(|error| invalid_artifact(path, error.to_string()))?;
    Ok(DecodedHeader {
        binding,
        signer_count: header_u32(header, OFFSET_SIGNER_COUNT),
        bitset_bytes: header_u64(header, OFFSET_BITSET_BYTES),
        rank_entries: header_u64(header, OFFSET_RANK_ENTRIES),
        payload_sha256: header[OFFSET_PAYLOAD_SHA256..OFFSET_PAYLOAD_SHA256 + 32]
            .try_into()
            .unwrap(),
    })
}

fn encode_header(
    binding: SignerSetBinding,
    signer_count: u32,
    bitset_bytes: u64,
    rank_entries: u64,
    payload_sha256: [u8; 32],
) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[..MAGIC.len()].copy_from_slice(&MAGIC);
    put_u32(
        &mut header,
        OFFSET_FORMAT_VERSION,
        SIGNER_SET_FORMAT_VERSION,
    );
    put_u32(&mut header, OFFSET_HEADER_LEN, HEADER_LEN as u32);
    put_u32(
        &mut header,
        OFFSET_SEMANTICS_VERSION,
        SIGNER_SET_SEMANTICS_VERSION,
    );
    put_u32(&mut header, OFFSET_SEMANTIC_POLICY, SEMANTIC_POLICY_V1);
    put_u32(
        &mut header,
        OFFSET_REGISTRY_ENTRIES,
        binding.registry_entries,
    );
    put_u32(&mut header, OFFSET_RANK_STRIDE, RANK_STRIDE_BITS);
    put_u32(&mut header, OFFSET_SIGNER_COUNT, signer_count);
    put_u64(&mut header, OFFSET_REGISTRY_SIZE, binding.registry_size);
    put_u64(&mut header, OFFSET_BITSET_BYTES, bitset_bytes);
    put_u64(&mut header, OFFSET_RANK_ENTRIES, rank_entries);
    header[OFFSET_GENERATION_DIGEST..OFFSET_GENERATION_DIGEST + 32]
        .copy_from_slice(&binding.generation_digest);
    header[OFFSET_REGISTRY_SHA256..OFFSET_REGISTRY_SHA256 + 32]
        .copy_from_slice(&binding.registry_sha256);
    header[OFFSET_PAYLOAD_SHA256..OFFSET_PAYLOAD_SHA256 + 32].copy_from_slice(&payload_sha256);
    header
}

fn build_rank_directory(bitset: &[u8], registry_entries: u32) -> Result<Vec<u32>, SignerSetError> {
    let count = rank_entry_count(registry_entries);
    let mut ranks = Vec::new();
    ranks
        .try_reserve_exact(count)
        .map_err(|_| SignerSetError::InvalidArgument {
            message: format!("could not reserve {count} rank entries"),
        })?;
    let mut cumulative = 0u32;
    for block in bitset.chunks(RANK_STRIDE_BYTES) {
        ranks.push(cumulative);
        for byte in block {
            cumulative += byte.count_ones();
        }
    }
    ranks.push(cumulative);
    Ok(ranks)
}

fn validate_registry_entries(registry_entries: u32) -> Result<(), SignerSetError> {
    if registry_entries == 0 || registry_entries > MAX_SIGNER_SET_REGISTRY_ENTRIES {
        return Err(SignerSetError::InvalidArgument {
            message: format!(
                "registry_entries must be in 1..={MAX_SIGNER_SET_REGISTRY_ENTRIES}, found {registry_entries}"
            ),
        });
    }
    Ok(())
}

fn checked_bit_index(registry_id: u32, registry_entries: u32) -> Result<usize, SignerSetError> {
    registry_id
        .checked_sub(1)
        .filter(|index| *index < registry_entries)
        .map(|index| index as usize)
        .ok_or(SignerSetError::InvalidRegistryId {
            registry_id,
            registry_entries,
        })
}

fn bitset_len(registry_entries: u32) -> usize {
    registry_entries.div_ceil(8) as usize
}

fn rank_entry_count(registry_entries: u32) -> usize {
    registry_entries.div_ceil(RANK_STRIDE_BITS) as usize + 1
}

fn artifact_len(registry_entries: u32) -> u64 {
    HEADER_LEN as u64
        + bitset_len(registry_entries) as u64
        + rank_entry_count(registry_entries) as u64 * 4
}

fn maximum_artifact_len() -> u64 {
    artifact_len(MAX_SIGNER_SET_REGISTRY_ENTRIES)
}

fn bit_in_rank_block(bit_index: usize) -> usize {
    bit_index % RANK_STRIDE_BITS as usize
}

fn validate_padding(bitset: &[u8], registry_entries: u32) -> Result<(), String> {
    let used_in_last_byte = registry_entries % 8;
    if used_in_last_byte == 0 {
        return Ok(());
    }
    let used_mask = (1u8 << used_in_last_byte) - 1;
    if bitset.last().is_some_and(|byte| byte & !used_mask != 0) {
        return Err("nonzero padding bits after the last registry id".into());
    }
    Ok(())
}

fn compare_binding(
    path: &Path,
    actual: SignerSetBinding,
    expected: SignerSetBinding,
) -> Result<(), SignerSetError> {
    for (field, matches) in [
        (
            "registry_entries",
            actual.registry_entries == expected.registry_entries,
        ),
        (
            "generation_digest",
            actual.generation_digest == expected.generation_digest,
        ),
        (
            "registry_size",
            actual.registry_size == expected.registry_size,
        ),
        (
            "registry_sha256",
            actual.registry_sha256 == expected.registry_sha256,
        ),
    ] {
        if !matches {
            return Err(SignerSetError::BindingMismatch {
                path: path.display().to_string(),
                field,
            });
        }
    }
    Ok(())
}

fn require_header_u32(
    path: &Path,
    header: &[u8; HEADER_LEN],
    offset: usize,
    expected: u32,
    field: &'static str,
) -> Result<(), SignerSetError> {
    let found = header_u32(header, offset);
    if found != expected {
        return Err(invalid_artifact(
            path,
            format!("{field} is {found}, expected {expected}"),
        ));
    }
    Ok(())
}

fn header_u32(header: &[u8; HEADER_LEN], offset: usize) -> u32 {
    u32::from_le_bytes(header[offset..offset + 4].try_into().unwrap())
}

fn header_u64(header: &[u8; HEADER_LEN], offset: usize) -> u64 {
    u64::from_le_bytes(header[offset..offset + 8].try_into().unwrap())
}

fn put_u32(header: &mut [u8; HEADER_LEN], offset: usize, value: u32) {
    header[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(header: &mut [u8; HEADER_LEN], offset: usize, value: u64) {
    header[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    size: u64,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

fn open_regular_file(path: &Path) -> Result<File, SignerSetError> {
    let owned = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(io::Error::from)
    .map_err(|source| SignerSetError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let file = File::from(owned);
    let metadata = file.metadata().map_err(|source| SignerSetError::Io {
        path: path.display().to_string(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(SignerSetError::Io {
            path: path.display().to_string(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "not a regular file"),
        });
    }
    Ok(file)
}

fn file_identity(file: &File, path: &Path) -> Result<FileIdentity, SignerSetError> {
    let metadata = file.metadata().map_err(|source| SignerSetError::Io {
        path: path.display().to_string(),
        source,
    })?;
    Ok(FileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn read_exact(reader: &mut impl Read, bytes: &mut [u8], path: &Path) -> Result<(), SignerSetError> {
    reader
        .read_exact(bytes)
        .map_err(|source| SignerSetError::Io {
            path: path.display().to_string(),
            source,
        })
}

fn allocate_zeroed(len: usize, path: &Path, kind: &'static str) -> Result<Vec<u8>, SignerSetError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| invalid_artifact(path, format!("could not reserve {len} bytes for {kind}")))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn create_temporary(
    parent: &Path,
    file_name: &std::ffi::OsStr,
) -> Result<(PathBuf, File), SignerSetError> {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let display_name = file_name.to_string_lossy();
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(
            ".{display_name}.tmp-{}-{sequence}-{attempt}",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(SignerSetError::Io {
                    path: candidate.display().to_string(),
                    source,
                });
            }
        }
    }
    Err(SignerSetError::InvalidArgument {
        message: format!(
            "could not allocate a unique temporary file under {}",
            parent.display()
        ),
    })
}

fn sync_directory(path: &Path) -> Result<(), SignerSetError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| SignerSetError::Io {
            path: path.display().to_string(),
            source,
        })
}

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn invalid_artifact(path: &Path, message: String) -> SignerSetError {
    SignerSetError::InvalidArtifact {
        path: path.display().to_string(),
        message,
    }
}

fn invalid_artifact_str(path: &str, message: impl Into<String>) -> SignerSetError {
    SignerSetError::InvalidArtifact {
        path: path.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(registry_entries: u32) -> SignerSetBinding {
        SignerSetBinding {
            registry_entries,
            generation_digest: [0x47; 32],
            registry_size: u64::from(registry_entries) * REGISTRY_ENTRY_BYTES,
            registry_sha256: [0xa9; 32],
        }
    }

    fn rank_with_ids(registry_entries: u32, ids: &[u32]) -> SignerRank {
        let mut builder = SignerSetBuilder::new(registry_entries).unwrap();
        for &id in ids {
            builder.insert(id).unwrap();
        }
        builder.finish(binding(registry_entries)).unwrap()
    }

    fn rewrite_payload_digest(bytes: &mut [u8]) {
        let digest: [u8; 32] = Sha256::digest(&bytes[HEADER_LEN..]).into();
        bytes[OFFSET_PAYLOAD_SHA256..OFFSET_PAYLOAD_SHA256 + 32].copy_from_slice(&digest);
    }

    #[test]
    fn rank_and_iteration_cover_stride_boundaries() {
        let mut builder = SignerSetBuilder::new(300).unwrap();
        for id in [300, 1, 129, 128, 2, 255, 256, 257, 129] {
            builder.insert(id).unwrap();
        }
        assert_eq!(builder.signer_count(), 8);
        let rank = builder.finish(binding(300)).unwrap();

        assert_eq!(rank.rank(0), None);
        assert_eq!(rank.rank(1), Some(0));
        assert_eq!(rank.rank(2), Some(1));
        assert_eq!(rank.rank(127), None);
        assert_eq!(rank.rank(128), Some(2));
        assert_eq!(rank.rank(129), Some(3));
        assert_eq!(rank.rank(255), Some(4));
        assert_eq!(rank.rank(256), Some(5));
        assert_eq!(rank.rank(257), Some(6));
        assert_eq!(rank.rank(300), Some(7));
        assert_eq!(rank.rank(301), None);
        assert_eq!(
            rank.iter_ids().collect::<Vec<_>>(),
            vec![
                (0, 1),
                (1, 2),
                (2, 128),
                (3, 129),
                (4, 255),
                (5, 256),
                (6, 257),
                (7, 300),
            ]
        );
    }

    #[test]
    fn merge_is_a_deterministic_set_union() {
        let mut left = SignerSetBuilder::new(300).unwrap();
        let mut right = SignerSetBuilder::new(300).unwrap();
        for id in [1, 130, 300] {
            left.insert(id).unwrap();
        }
        for id in [2, 130, 299] {
            right.insert(id).unwrap();
        }
        left.merge(right).unwrap();
        assert_eq!(left.signer_count(), 5);
        assert_eq!(
            left.finish(binding(300))
                .unwrap()
                .iter_ids()
                .collect::<Vec<_>>(),
            vec![(0, 1), (1, 2), (2, 130), (3, 299), (4, 300)]
        );

        let error = SignerSetBuilder::new(300)
            .unwrap()
            .merge(SignerSetBuilder::new(301).unwrap())
            .unwrap_err();
        assert!(matches!(
            error,
            SignerSetError::MergeRegistryMismatch { .. }
        ));
    }

    #[test]
    fn writer_is_deterministic_and_reader_requires_binding() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first.bits");
        let second = directory.path().join("second.bits");
        let rank = rank_with_ids(300, &[300, 128, 1, 129]);
        rank.write_atomic(&first).unwrap();
        rank_with_ids(300, &[129, 1, 128, 300])
            .write_atomic(&second)
            .unwrap();
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());

        let reopened = SignerRank::open(&first, binding(300)).unwrap();
        assert_eq!(reopened.binding(), binding(300));
        assert_eq!(reopened.signer_count(), 4);
        assert_eq!(reopened.rank(129), Some(2));

        let mut wrong = binding(300);
        wrong.generation_digest[0] ^= 1;
        assert!(matches!(
            SignerRank::open(&first, wrong),
            Err(SignerSetError::BindingMismatch {
                field: "generation_digest",
                ..
            })
        ));
    }

    #[test]
    fn rank_matches_a_linear_oracle_across_partial_and_complete_blocks() {
        let registry_entries = 1_025;
        let expected_ids: Vec<u32> = (1..=registry_entries)
            .filter(|id| id % 3 == 0 || id % 127 == 1)
            .collect();
        let mut builder = SignerSetBuilder::new(registry_entries).unwrap();
        for &id in expected_ids.iter().rev() {
            builder.insert(id).unwrap();
        }
        let rank = builder.finish(binding(registry_entries)).unwrap();

        let mut expected_rank = 0u32;
        for id in 1..=registry_entries {
            if expected_ids.binary_search(&id).is_ok() {
                assert_eq!(rank.rank(id), Some(expected_rank), "registry id {id}");
                expected_rank += 1;
            } else {
                assert_eq!(rank.rank(id), None, "registry id {id}");
            }
        }
        assert_eq!(expected_rank, rank.signer_count());
    }

    #[test]
    fn reader_rejects_truncation_trailing_bytes_and_payload_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original.bits");
        rank_with_ids(130, &[1, 128, 130])
            .write_atomic(&original)
            .unwrap();
        let bytes = fs::read(&original).unwrap();

        let truncated = directory.path().join("truncated.bits");
        fs::write(&truncated, &bytes[..bytes.len() - 1]).unwrap();
        assert!(SignerRank::open(&truncated, binding(130)).is_err());

        let trailing = directory.path().join("trailing.bits");
        let mut with_trailing = bytes.clone();
        with_trailing.push(0);
        fs::write(&trailing, with_trailing).unwrap();
        assert!(SignerRank::open(&trailing, binding(130)).is_err());

        let corrupted = directory.path().join("corrupted.bits");
        let mut corrupted_bytes = bytes;
        corrupted_bytes[HEADER_LEN] ^= 1;
        fs::write(&corrupted, corrupted_bytes).unwrap();
        let error = SignerRank::open(&corrupted, binding(130)).unwrap_err();
        assert!(error.to_string().contains("payload SHA-256 mismatch"));
    }

    #[test]
    fn reader_recomputes_rank_directory_and_rejects_padding_bits() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original.bits");
        rank_with_ids(130, &[1, 128, 130])
            .write_atomic(&original)
            .unwrap();
        let original_bytes = fs::read(&original).unwrap();

        let bad_rank = directory.path().join("bad-rank.bits");
        let mut bytes = original_bytes.clone();
        let first_rank = HEADER_LEN + bitset_len(130);
        bytes[first_rank] = 1;
        rewrite_payload_digest(&mut bytes);
        fs::write(&bad_rank, bytes).unwrap();
        let error = SignerRank::open(&bad_rank, binding(130)).unwrap_err();
        assert!(error.to_string().contains("rank prefix 0"));

        let bad_padding = directory.path().join("bad-padding.bits");
        let mut bytes = original_bytes;
        bytes[HEADER_LEN + bitset_len(130) - 1] |= 0x80;
        rewrite_payload_digest(&mut bytes);
        fs::write(&bad_padding, bytes).unwrap();
        let error = SignerRank::open(&bad_padding, binding(130)).unwrap_err();
        assert!(error.to_string().contains("nonzero padding bits"));
    }

    #[test]
    fn reader_rejects_semantic_and_geometry_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original.bits");
        rank_with_ids(130, &[1]).write_atomic(&original).unwrap();
        let original_bytes = fs::read(&original).unwrap();

        for (name, offset) in [
            ("version", OFFSET_FORMAT_VERSION),
            ("semantics", OFFSET_SEMANTICS_VERSION),
            ("policy", OFFSET_SEMANTIC_POLICY),
            ("stride", OFFSET_RANK_STRIDE),
            ("reserved", OFFSET_RESERVED),
            ("bitset-len", OFFSET_BITSET_BYTES),
            ("rank-count", OFFSET_RANK_ENTRIES),
        ] {
            let path = directory.path().join(format!("bad-{name}.bits"));
            let mut bytes = original_bytes.clone();
            bytes[offset] ^= 1;
            fs::write(&path, bytes).unwrap();
            assert!(
                SignerRank::open(&path, binding(130)).is_err(),
                "corrupt {name} was accepted"
            );
        }
    }

    #[test]
    fn atomic_writer_never_replaces_an_existing_target() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(SIGNER_SET_FILE_NAME);
        fs::write(&path, b"belongs to another build").unwrap();
        let error = rank_with_ids(8, &[1]).write_atomic(&path).unwrap_err();
        assert!(matches!(error, SignerSetError::Io { .. }));
        assert_eq!(fs::read(&path).unwrap(), b"belongs to another build");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn invalid_ids_and_unbounded_registry_sizes_are_rejected() {
        assert!(SignerSetBuilder::new(0).is_err());
        assert!(SignerSetBuilder::new(MAX_SIGNER_SET_REGISTRY_ENTRIES + 1).is_err());
        let mut builder = SignerSetBuilder::new(8).unwrap();
        assert!(matches!(
            builder.insert(0),
            Err(SignerSetError::InvalidRegistryId { .. })
        ));
        assert!(matches!(
            builder.insert(9),
            Err(SignerSetError::InvalidRegistryId { .. })
        ));

        let directory = tempfile::tempdir().unwrap();
        let oversized = directory.path().join("oversized.bits");
        let file = File::create(&oversized).unwrap();
        file.set_len(maximum_artifact_len() + 1).unwrap();
        let error = SignerRank::open(&oversized, binding(8)).unwrap_err();
        assert!(error.to_string().contains("bounded maximum"));
    }

    #[test]
    fn reader_rejects_fifo_without_blocking() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(SIGNER_SET_FILE_NAME);
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );

        let (sender, receiver) = std::sync::mpsc::channel();
        let worker_path = path.clone();
        let worker = std::thread::spawn(move || {
            sender
                .send(SignerRank::open(&worker_path, binding(8)))
                .unwrap();
        });
        let result = match receiver.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(result) => result,
            Err(error) => {
                let _peer = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let _ = receiver.recv_timeout(std::time::Duration::from_secs(2));
                worker.join().unwrap();
                panic!("signer-set reader blocked while opening FIFO: {error}");
            }
        };
        worker.join().unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not a regular file")
        );
    }
}
