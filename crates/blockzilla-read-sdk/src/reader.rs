use std::{collections::HashSet, io::Read};

use blockzilla_format::{
    ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
    ARCHIVE_V2_HOT_INDEX_HEADER_LEN, ARCHIVE_V2_HOT_INDEX_MAGIC, ARCHIVE_V2_HOT_INDEX_ROW_LEN,
    ARCHIVE_V2_HOT_INDEX_VERSION, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2HotBlockBlob, ArchiveV2HotBlockIndex, ArchiveV2HotBlockIndexRow,
    ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, CompactMetaV1,
    CompactPubkey, WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS,
    WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY, WINCODE_ARCHIVE_V2_FLAG_LEB128,
    WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
    WincodeArchiveV2Footer, deserialize_archive_v2_hot_block_blob, wincode_leb128_config,
};
use sha2::{Digest, Sha256};

use crate::{
    Error, Result,
    manifest::{
        BLOCK_INDEX_FILE, BLOCKS_FILE, GENERATION_MANIFEST_FILE, GenerationManifest, META_FILE,
        REGISTRY_FILE, REQUIRED_GENERATION_FILES, SIGNATURES_FILE, decode_sha256, hex_lower,
    },
    source::{RangeSource, RangeSourceReader},
};

const DEFAULT_IO_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_BLOCK_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_COMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PREFETCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_GATEWAY_RANGE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_META_FRAME_BYTES: usize = 256 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const KNOWN_HOT_TX_FLAGS: u32 = (1 << 11) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashVerification {
    /// Check object presence and exact lengths, then hash every manifest file.
    AllFiles,
    /// Hash the downloaded/cacheable control plane (`registry.bin`, the block
    /// index and publication metadata), while size-checking remote blocks and
    /// signatures. This is the intended HTTP streaming policy. The gateway
    /// must serve an immutable generation over authenticated TLS.
    ControlFiles,
    /// Check object presence and exact lengths only. This is useful when the
    /// transport already verified downloaded immutable files; block decoding
    /// and all structural checks remain enabled.
    SizesOnly,
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub hash_verification: HashVerification,
    pub io_chunk_size: usize,
    pub max_block_bytes: usize,
    pub max_compressed_frame_bytes: usize,
    pub max_meta_frame_bytes: usize,
    /// Maximum contiguous blocks range fetched by sequential iterators. The
    /// gateway contract caps a single range at 64 MiB.
    pub prefetch_bytes: usize,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            hash_verification: HashVerification::AllFiles,
            io_chunk_size: DEFAULT_IO_CHUNK_SIZE,
            max_block_bytes: DEFAULT_MAX_BLOCK_BYTES,
            max_compressed_frame_bytes: DEFAULT_MAX_COMPRESSED_FRAME_BYTES,
            max_meta_frame_bytes: DEFAULT_MAX_META_FRAME_BYTES,
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GenerationBinding {
    pub generation_digest: [u8; 32],
    pub registry_sha256: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct CompiledPubkeyFilter {
    binding: GenerationBinding,
    registry_ids: HashSet<u32>,
    raw_pubkeys: HashSet<[u8; 32]>,
}

impl CompiledPubkeyFilter {
    pub fn binding(&self) -> GenerationBinding {
        self.binding
    }

    pub fn pubkey_count(&self) -> usize {
        self.raw_pubkeys.len()
    }

    pub fn registry_id_count(&self) -> usize {
        self.registry_ids.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndeterminateReason {
    RawTransactionFallback,
    InvalidRegistryReference,
    V0LoadedAddressesUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMatch {
    Match {
        static_account: bool,
        loaded_address: bool,
    },
    NoMatch,
    Indeterminate(IndeterminateReason),
}

#[derive(Debug)]
pub enum MetadataState {
    NotRead,
    Absent,
    RawFallback,
    Decoded(Box<CompactMetaV1>),
}

impl MetadataState {
    pub fn decoded(&self) -> Option<&CompactMetaV1> {
        match self {
            Self::Decoded(metadata) => Some(metadata),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignatureReference {
    pub generation_digest: [u8; 32],
    pub first_ordinal: u64,
    pub count: u8,
}

#[derive(Debug)]
pub struct ScannedTransaction {
    pub slot: u64,
    pub tx_index: u32,
    pub row: ArchiveV2HotTxRow,
    pub outcome: TransactionMatch,
    pub message: Option<ArchiveV2HotMessagePayload>,
    pub metadata: MetadataState,
    pub signatures: SignatureReference,
}

#[derive(Debug)]
pub struct ScannedBlock {
    pub block_id: u32,
    pub slot: u64,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transactions: Vec<ScannedTransaction>,
}

#[derive(Debug)]
pub struct DecodedBlock {
    pub index_row: ArchiveV2HotBlockIndexRow,
    pub block: ArchiveV2HotBlockBlob,
}

/// Structural publication audit shared by the gateway publisher and readers.
///
/// The helper validates the candidate manifest, required object sizes/hashes,
/// registry layout, hot index, optional signature length, and the metadata
/// footer. It does not require the manifest JSON itself to have been published.
#[derive(Debug)]
pub struct ValidatedGeneration {
    pub index: ArchiveV2HotBlockIndex,
    pub metadata_footer: WincodeArchiveV2Footer,
    pub binding: GenerationBinding,
    pub registry_entries: u32,
    pub total_signatures: u64,
    pub signatures_available: bool,
}

#[derive(Debug)]
pub struct ArchiveReader<S> {
    source: S,
    manifest: GenerationManifest,
    index: ArchiveV2HotBlockIndex,
    metadata_footer: WincodeArchiveV2Footer,
    binding: GenerationBinding,
    registry_entries: u32,
    total_signatures: u64,
    signatures_available: bool,
    options: OpenOptions,
}

impl<S: RangeSource> ArchiveReader<S> {
    pub fn open(source: S) -> Result<Self> {
        Self::open_with_options(source, OpenOptions::default())
    }

    pub fn open_with_options(source: S, options: OpenOptions) -> Result<Self> {
        let manifest_bytes =
            source.read_all_bounded(GENERATION_MANIFEST_FILE, MAX_MANIFEST_BYTES)?;
        let manifest = GenerationManifest::parse(&manifest_bytes)?;
        let validated = validate_generation_structure(&source, &manifest, &options)?;

        Ok(Self {
            source,
            manifest,
            index: validated.index,
            metadata_footer: validated.metadata_footer,
            binding: validated.binding,
            registry_entries: validated.registry_entries,
            total_signatures: validated.total_signatures,
            signatures_available: validated.signatures_available,
            options,
        })
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    pub fn manifest(&self) -> &GenerationManifest {
        &self.manifest
    }

    pub fn index(&self) -> &ArchiveV2HotBlockIndex {
        &self.index
    }

    pub fn metadata_footer(&self) -> &WincodeArchiveV2Footer {
        &self.metadata_footer
    }

    pub fn binding(&self) -> GenerationBinding {
        self.binding
    }

    pub fn registry_entries(&self) -> u32 {
        self.registry_entries
    }

    pub fn total_signatures(&self) -> u64 {
        self.total_signatures
    }

    pub fn signatures_available(&self) -> bool {
        self.signatures_available
    }

    /// Compile an include-any pubkey filter by scanning `registry.bin` once.
    /// Memory is O(number of requested pubkeys), not O(registry size). Queried
    /// bytes are retained as well as resolved IDs so inline raw pubkeys match.
    pub fn compile_pubkey_filter(
        &self,
        pubkeys: impl IntoIterator<Item = [u8; 32]>,
    ) -> Result<CompiledPubkeyFilter> {
        let raw_pubkeys: HashSet<[u8; 32]> = pubkeys.into_iter().collect();
        let mut registry_ids = HashSet::with_capacity(raw_pubkeys.len());
        let mut resolved_pubkeys = HashSet::with_capacity(raw_pubkeys.len());
        if !raw_pubkeys.is_empty() && self.registry_entries != 0 {
            let mut offset = 0u64;
            let registry_size = self.manifest.required_file(REGISTRY_FILE)?.size;
            let chunk_size = (self.options.io_chunk_size / 32).max(1) * 32;
            while offset < registry_size {
                let length = usize::try_from((registry_size - offset).min(chunk_size as u64))
                    .expect("registry chunk is bounded by usize");
                let bytes = self.source.read_range(REGISTRY_FILE, offset, length)?;
                if bytes.len() % 32 != 0 {
                    return Err(Error::InvalidRegistry(
                        "range source split registry on a partial pubkey".into(),
                    ));
                }
                for (position, key_bytes) in bytes.chunks_exact(32).enumerate() {
                    let mut key = [0u8; 32];
                    key.copy_from_slice(key_bytes);
                    if raw_pubkeys.contains(&key) {
                        if !resolved_pubkeys.insert(key) {
                            return Err(Error::InvalidRegistry(
                                "a requested pubkey occurs more than once in registry.bin".into(),
                            ));
                        }
                        let zero_based = offset / 32 + position as u64;
                        let id = u32::try_from(zero_based + 1)
                            .map_err(|_| Error::InvalidRegistry("registry id overflow".into()))?;
                        registry_ids.insert(id);
                    }
                }
                offset += length as u64;
            }
        }
        Ok(CompiledPubkeyFilter {
            binding: self.binding,
            registry_ids,
            raw_pubkeys,
        })
    }

    pub fn blocks(&self) -> BlockIterator<'_, S> {
        BlockIterator {
            archive: self,
            next: 0,
            batch_first: 0,
            batch_end: 0,
            batch_offset: 0,
            batch: Vec::new(),
        }
    }

    pub fn scan<'a>(&'a self, filter: &'a CompiledPubkeyFilter) -> Result<ScanIterator<'a, S>> {
        self.ensure_filter_binding(filter)?;
        Ok(ScanIterator {
            archive: self,
            filter,
            blocks: self.blocks(),
        })
    }

    pub fn read_block(&self, row_number: usize) -> Result<DecodedBlock> {
        let row = *self.index.rows.get(row_number).ok_or_else(|| {
            Error::InvalidIndex(format!("block row {row_number} is out of bounds"))
        })?;
        let compressed = self.source.read_range(
            BLOCKS_FILE,
            row.compressed_offset,
            row.compressed_len as usize,
        )?;
        self.decode_compressed_block(row, &compressed)
    }

    fn decode_compressed_block(
        &self,
        row: ArchiveV2HotBlockIndexRow,
        compressed: &[u8],
    ) -> Result<DecodedBlock> {
        if compressed.len() != row.compressed_len as usize {
            return Err(Error::InvalidBlock {
                slot: row.slot,
                message: format!(
                    "compressed frame is {} bytes, expected {}",
                    compressed.len(),
                    row.compressed_len
                ),
            });
        }
        let expected_length = row.uncompressed_len as usize;
        let bytes = zstd::bulk::decompress(compressed, expected_length).map_err(|error| {
            Error::DecodeBlock {
                slot: row.slot,
                message: format!("zstd frame: {error}"),
            }
        })?;
        if bytes.len() != expected_length {
            return Err(Error::InvalidBlock {
                slot: row.slot,
                message: format!(
                    "zstd output is {} bytes, expected {}",
                    bytes.len(),
                    expected_length
                ),
            });
        }
        let block =
            deserialize_archive_v2_hot_block_blob(&bytes).map_err(|error| Error::DecodeBlock {
                slot: row.slot,
                message: error.to_string(),
            })?;
        validate_decoded_block(&row, &block)?;
        Ok(DecodedBlock {
            index_row: row,
            block,
        })
    }

    pub fn scan_decoded_block(
        &self,
        filter: &CompiledPubkeyFilter,
        decoded: DecodedBlock,
    ) -> Result<ScannedBlock> {
        self.ensure_filter_binding(filter)?;
        let DecodedBlock { index_row, block } = decoded;
        let mut first_signature_ordinal = index_row.first_signature_ordinal;
        let mut transactions = Vec::with_capacity(block.tx_rows.len());
        for row in block.tx_rows.iter().copied() {
            let signatures = SignatureReference {
                generation_digest: self.binding.generation_digest,
                first_ordinal: first_signature_ordinal,
                count: row.signature_count,
            };
            first_signature_ordinal = first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .ok_or(Error::Overflow("transaction signature ordinal"))?;
            transactions.push(scan_transaction(
                index_row.slot,
                row,
                &block,
                filter,
                self.registry_entries,
                signatures,
            )?);
        }
        Ok(ScannedBlock {
            block_id: index_row.block_id,
            slot: block.header.slot,
            parent_slot: block.header.parent_slot,
            block_time: block.header.block_time,
            block_height: block.header.block_height,
            transactions,
        })
    }

    pub fn read_signature_ordinal(&self, ordinal: u64) -> Result<[u8; 64]> {
        if !self.signatures_available {
            return Err(Error::SignaturesUnavailable);
        }
        if ordinal >= self.total_signatures {
            return Err(Error::InvalidIndex(format!(
                "signature ordinal {ordinal} is outside {} signatures",
                self.total_signatures
            )));
        }
        let offset = ordinal
            .checked_mul(64)
            .ok_or(Error::Overflow("signature byte offset"))?;
        let bytes = self.source.read_range(SIGNATURES_FILE, offset, 64)?;
        let mut signature = [0u8; 64];
        signature.copy_from_slice(&bytes);
        Ok(signature)
    }

    pub fn read_transaction_signatures(
        &self,
        reference: SignatureReference,
    ) -> Result<Vec<[u8; 64]>> {
        if reference.generation_digest != self.binding.generation_digest {
            return Err(Error::FilterBindingMismatch);
        }
        if !self.signatures_available {
            return Err(Error::SignaturesUnavailable);
        }
        let end = reference
            .first_ordinal
            .checked_add(u64::from(reference.count))
            .ok_or(Error::Overflow("transaction signature range"))?;
        if end > self.total_signatures {
            return Err(Error::InvalidIndex(format!(
                "signature range {}..{} is outside {} signatures",
                reference.first_ordinal, end, self.total_signatures
            )));
        }
        let offset = reference
            .first_ordinal
            .checked_mul(64)
            .ok_or(Error::Overflow("signature byte offset"))?;
        let length = usize::from(reference.count) * 64;
        let bytes = self.source.read_range(SIGNATURES_FILE, offset, length)?;
        Ok(bytes
            .chunks_exact(64)
            .map(|bytes| {
                let mut signature = [0u8; 64];
                signature.copy_from_slice(bytes);
                signature
            })
            .collect())
    }

    fn ensure_filter_binding(&self, filter: &CompiledPubkeyFilter) -> Result<()> {
        if filter.binding != self.binding {
            return Err(Error::FilterBindingMismatch);
        }
        Ok(())
    }
}

pub struct BlockIterator<'a, S> {
    archive: &'a ArchiveReader<S>,
    next: usize,
    batch_first: usize,
    batch_end: usize,
    batch_offset: u64,
    batch: Vec<u8>,
}

impl<S: RangeSource> Iterator for BlockIterator<'_, S> {
    type Item = Result<DecodedBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.archive.index.rows.len() {
            return None;
        }
        if self.next < self.batch_first || self.next >= self.batch_end {
            match self.refill() {
                Ok(()) => {}
                Err(error) => {
                    self.next = self.archive.index.rows.len();
                    return Some(Err(error));
                }
            }
        }
        let row_number = self.next;
        self.next += 1;
        let row = self.archive.index.rows[row_number];
        let relative_offset = match row.compressed_offset.checked_sub(self.batch_offset) {
            Some(offset) => offset as usize,
            None => {
                return Some(Err(Error::InvalidIndex(
                    "prefetched block offset underflow".into(),
                )));
            }
        };
        let end = match relative_offset.checked_add(row.compressed_len as usize) {
            Some(end) => end,
            None => return Some(Err(Error::Overflow("prefetched block range"))),
        };
        let Some(compressed) = self.batch.get(relative_offset..end) else {
            return Some(Err(Error::InvalidIndex(
                "prefetched block range is outside batch".into(),
            )));
        };
        Some(self.archive.decode_compressed_block(row, compressed))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.archive.index.rows.len() - self.next;
        (remaining, Some(remaining))
    }
}

impl<S: RangeSource> ExactSizeIterator for BlockIterator<'_, S> {}

impl<S: RangeSource> BlockIterator<'_, S> {
    fn refill(&mut self) -> Result<()> {
        self.batch_first = self.next;
        let first = self.archive.index.rows[self.next];
        let mut end = self.next + 1;
        let mut length = first.compressed_len as usize;
        while end < self.archive.index.rows.len() {
            let next_length = self.archive.index.rows[end].compressed_len as usize;
            let Some(combined) = length.checked_add(next_length) else {
                break;
            };
            if combined > self.archive.options.prefetch_bytes {
                break;
            }
            length = combined;
            end += 1;
        }
        self.batch_offset = first.compressed_offset;
        self.batch = self
            .archive
            .source
            .read_range(BLOCKS_FILE, self.batch_offset, length)?;
        if self.batch.len() != length {
            return Err(Error::InvalidIndex(format!(
                "prefetched block range returned {} bytes, expected {length}",
                self.batch.len()
            )));
        }
        self.batch_end = end;
        Ok(())
    }
}

pub struct ScanIterator<'a, S> {
    archive: &'a ArchiveReader<S>,
    filter: &'a CompiledPubkeyFilter,
    blocks: BlockIterator<'a, S>,
}

impl<S: RangeSource> Iterator for ScanIterator<'_, S> {
    type Item = Result<ScannedBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        self.blocks.next().map(|block| {
            block.and_then(|block| self.archive.scan_decoded_block(self.filter, block))
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.blocks.size_hint()
    }
}

impl<S: RangeSource> ExactSizeIterator for ScanIterator<'_, S> {}

pub fn validate_generation_structure<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    options: &OpenOptions,
) -> Result<ValidatedGeneration> {
    validate_options(options)?;
    manifest.validate()?;
    if !manifest.complete {
        return Err(Error::IncompleteGeneration);
    }
    for required in REQUIRED_GENERATION_FILES {
        manifest.required_file(required)?;
    }
    validate_manifest_files(source, manifest, options)?;

    let registry_file = manifest.required_file(REGISTRY_FILE)?;
    if registry_file.size % 32 != 0 {
        return Err(Error::InvalidRegistry(format!(
            "registry.bin is {} bytes, not a multiple of 32",
            registry_file.size
        )));
    }
    let registry_entries_u64 = registry_file.size / 32;
    let registry_entries = u32::try_from(registry_entries_u64).map_err(|_| {
        Error::InvalidRegistry(format!(
            "registry has {registry_entries_u64} entries, exceeding the u32 id space"
        ))
    })?;

    let index_file = manifest.required_file(BLOCK_INDEX_FILE)?;
    let max_index_size = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            manifest
                .slots_per_epoch
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .ok_or(Error::Overflow("maximum block index size"))?,
        )
        .ok_or(Error::Overflow("maximum block index size"))?;
    if index_file.size > max_index_size {
        return Err(Error::InvalidIndex(format!(
            "index is {} bytes, above the epoch maximum {}",
            index_file.size, max_index_size
        )));
    }
    let index_length = usize::try_from(index_file.size)
        .map_err(|_| Error::InvalidIndex("index size exceeds usize".into()))?;
    let index_bytes = source.read_range(BLOCK_INDEX_FILE, 0, index_length)?;
    let blocks_size = manifest.required_file(BLOCKS_FILE)?.size;
    let (index, total_signatures) =
        parse_and_validate_index(&index_bytes, blocks_size, manifest, options)?;

    let signatures_available = if let Some(signatures) = manifest.file(SIGNATURES_FILE) {
        let expected = total_signatures
            .checked_mul(64)
            .ok_or(Error::Overflow("signature sidecar size"))?;
        if signatures.size != expected {
            return Err(Error::InvalidIndex(format!(
                "signatures.bin is {} bytes, expected {} for {} signatures",
                signatures.size, expected, total_signatures
            )));
        }
        true
    } else {
        false
    };

    let metadata_footer = validate_metadata(source, manifest, &index, options)?;
    let binding = GenerationBinding {
        generation_digest: decode_sha256(&manifest.generation_digest)
            .map_err(Error::InvalidManifest)?,
        registry_sha256: decode_sha256(&registry_file.sha256).map_err(Error::InvalidManifest)?,
    };
    Ok(ValidatedGeneration {
        index,
        metadata_footer,
        binding,
        registry_entries,
        total_signatures,
        signatures_available,
    })
}

fn validate_options(options: &OpenOptions) -> Result<()> {
    if options.io_chunk_size == 0
        || options.max_block_bytes == 0
        || options.max_compressed_frame_bytes == 0
        || options.max_meta_frame_bytes == 0
        || options.prefetch_bytes == 0
    {
        return Err(Error::InvalidManifest(
            "reader size limits must be non-zero".into(),
        ));
    }
    if options.prefetch_bytes > MAX_GATEWAY_RANGE_BYTES {
        return Err(Error::InvalidManifest(format!(
            "prefetch_bytes {} exceeds the gateway's {} byte range limit",
            options.prefetch_bytes, MAX_GATEWAY_RANGE_BYTES
        )));
    }
    Ok(())
}

fn validate_manifest_files<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    options: &OpenOptions,
) -> Result<()> {
    for file in &manifest.files {
        let actual = source
            .size(&file.name)?
            .ok_or_else(|| Error::MissingFile(file.name.clone()))?;
        if actual != file.size {
            return Err(Error::FileSize {
                name: file.name.clone(),
                expected: file.size,
                actual,
            });
        }
        let verify_hash = match options.hash_verification {
            HashVerification::AllFiles => true,
            HashVerification::ControlFiles => {
                matches!(
                    file.name.as_str(),
                    BLOCK_INDEX_FILE | META_FILE | REGISTRY_FILE
                )
            }
            HashVerification::SizesOnly => false,
        };
        if verify_hash {
            let actual_hash =
                hash_source_file(source, &file.name, file.size, options.io_chunk_size)?;
            if actual_hash != file.sha256 {
                return Err(Error::FileHash {
                    name: file.name.clone(),
                    expected: file.sha256.clone(),
                    actual: actual_hash,
                });
            }
        }
    }
    Ok(())
}

fn hash_source_file<S: RangeSource>(
    source: &S,
    name: &str,
    size: u64,
    chunk_size: usize,
) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    while offset < size {
        let length = usize::try_from((size - offset).min(chunk_size as u64))
            .expect("hash chunk is bounded by usize");
        hasher.update(source.read_range(name, offset, length)?);
        offset += length as u64;
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn parse_and_validate_index(
    bytes: &[u8],
    blocks_size: u64,
    manifest: &GenerationManifest,
    options: &OpenOptions,
) -> Result<(ArchiveV2HotBlockIndex, u64)> {
    if bytes.len() < ARCHIVE_V2_HOT_INDEX_HEADER_LEN {
        return Err(Error::InvalidIndex("index header is truncated".into()));
    }
    if &bytes[..8] != ARCHIVE_V2_HOT_INDEX_MAGIC {
        return Err(Error::InvalidIndex("bad index magic".into()));
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != ARCHIVE_V2_HOT_INDEX_VERSION {
        return Err(Error::InvalidIndex(format!(
            "unsupported index version {version}"
        )));
    }
    if bytes[10..12] != [0, 0] {
        return Err(Error::InvalidIndex(
            "index header reserved bytes are non-zero".into(),
        ));
    }
    let row_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let blob_file_bytes = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    let level = i32::from_le_bytes(bytes[28..32].try_into().unwrap());
    let flags = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
    let unsupported_flags =
        flags & (ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY | ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS);
    if unsupported_flags != 0 {
        return Err(Error::InvalidIndex(format!(
            "reader requires independent dictionary-free zstd frames; flags={flags:#x}"
        )));
    }
    if flags != 0 {
        return Err(Error::InvalidIndex(format!(
            "unknown index flags {flags:#x}"
        )));
    }
    if blob_file_bytes != blocks_size {
        return Err(Error::InvalidIndex(format!(
            "index declares {blob_file_bytes} block bytes, manifest declares {blocks_size}"
        )));
    }
    if row_count > manifest.slots_per_epoch {
        return Err(Error::InvalidIndex(format!(
            "index has {row_count} rows for {} epoch slots",
            manifest.slots_per_epoch
        )));
    }
    let expected_length = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            row_count
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .ok_or(Error::Overflow("block index rows"))?,
        )
        .ok_or(Error::Overflow("block index length"))?;
    if bytes.len() as u64 != expected_length {
        return Err(Error::InvalidIndex(format!(
            "index is {} bytes, expected {expected_length}",
            bytes.len()
        )));
    }

    let mut rows = Vec::with_capacity(row_count as usize);
    let mut expected_offset = 0u64;
    let mut expected_tx_ordinal = 0u64;
    let mut expected_signature_ordinal = 0u64;
    let mut previous_slot = None;
    for (number, row_bytes) in bytes[ARCHIVE_V2_HOT_INDEX_HEADER_LEN..]
        .chunks_exact(ARCHIVE_V2_HOT_INDEX_ROW_LEN)
        .enumerate()
    {
        let row = ArchiveV2HotBlockIndexRow {
            block_id: u32::from_le_bytes(row_bytes[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row_bytes[4..12].try_into().unwrap()),
            compressed_offset: u64::from_le_bytes(row_bytes[12..20].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(row_bytes[20..24].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(row_bytes[24..28].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row_bytes[28..32].try_into().unwrap()),
            first_tx_ordinal: u64::from_le_bytes(row_bytes[32..40].try_into().unwrap()),
            first_signature_ordinal: u64::from_le_bytes(row_bytes[40..48].try_into().unwrap()),
            signature_count: u32::from_le_bytes(row_bytes[48..52].try_into().unwrap()),
        };
        let expected_block_id = u32::try_from(number)
            .map_err(|_| Error::InvalidIndex("block id exceeds u32".into()))?;
        if row.block_id != expected_block_id {
            return Err(Error::InvalidIndex(format!(
                "row {number} has block_id {}, expected {expected_block_id}",
                row.block_id
            )));
        }
        if row.slot < manifest.epoch_start_slot() || row.slot > manifest.epoch_end_slot() {
            return Err(Error::InvalidIndex(format!(
                "slot {} is outside epoch {} range {}..={}",
                row.slot,
                manifest.epoch,
                manifest.epoch_start_slot(),
                manifest.epoch_end_slot()
            )));
        }
        if previous_slot.is_some_and(|slot| row.slot <= slot) {
            return Err(Error::InvalidIndex(format!(
                "slots are not strictly increasing at {}",
                row.slot
            )));
        }
        if row.compressed_len == 0 || row.uncompressed_len == 0 {
            return Err(Error::InvalidIndex(format!(
                "slot {} has an empty block frame",
                row.slot
            )));
        }
        if row.uncompressed_len as usize > options.max_block_bytes {
            return Err(Error::InvalidIndex(format!(
                "slot {} declares {} uncompressed bytes above the {} byte limit",
                row.slot, row.uncompressed_len, options.max_block_bytes
            )));
        }
        if row.compressed_len as usize > options.max_compressed_frame_bytes {
            return Err(Error::InvalidIndex(format!(
                "slot {} declares {} compressed bytes above the {} byte limit",
                row.slot, row.compressed_len, options.max_compressed_frame_bytes
            )));
        }
        if row.compressed_offset != expected_offset {
            return Err(Error::InvalidIndex(format!(
                "slot {} starts at {}, expected contiguous offset {}",
                row.slot, row.compressed_offset, expected_offset
            )));
        }
        expected_offset = expected_offset
            .checked_add(u64::from(row.compressed_len))
            .ok_or(Error::Overflow("compressed block range"))?;
        if expected_offset > blocks_size {
            return Err(Error::InvalidIndex(format!(
                "slot {} block range exceeds blocks file",
                row.slot
            )));
        }
        if row.first_tx_ordinal != expected_tx_ordinal {
            return Err(Error::InvalidIndex(format!(
                "slot {} first_tx_ordinal is {}, expected {}",
                row.slot, row.first_tx_ordinal, expected_tx_ordinal
            )));
        }
        expected_tx_ordinal = expected_tx_ordinal
            .checked_add(u64::from(row.tx_count))
            .ok_or(Error::Overflow("transaction ordinal"))?;
        if row.first_signature_ordinal != expected_signature_ordinal {
            return Err(Error::InvalidIndex(format!(
                "slot {} first_signature_ordinal is {}, expected {}",
                row.slot, row.first_signature_ordinal, expected_signature_ordinal
            )));
        }
        expected_signature_ordinal = expected_signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .ok_or(Error::Overflow("signature ordinal"))?;
        previous_slot = Some(row.slot);
        rows.push(row);
    }
    if expected_offset != blocks_size {
        return Err(Error::InvalidIndex(format!(
            "indexed frames cover {expected_offset} bytes, blocks file has {blocks_size}"
        )));
    }
    Ok((
        ArchiveV2HotBlockIndex {
            blob_file_bytes,
            level,
            flags,
            rows,
        },
        expected_signature_ordinal,
    ))
}

fn validate_metadata<S: RangeSource>(
    source: &S,
    manifest: &GenerationManifest,
    index: &ArchiveV2HotBlockIndex,
    options: &OpenOptions,
) -> Result<WincodeArchiveV2Footer> {
    let meta = manifest.required_file(META_FILE)?;
    let mut reader = RangeSourceReader::new(source, META_FILE, meta.size, options.io_chunk_size);
    let mut position = 0usize;
    let mut saw_genesis = false;
    let mut footer = None;
    while let Some(frame) = read_frame(&mut reader, options.max_meta_frame_bytes)? {
        let record: ArchiveV2HotMetaRecord =
            wincode::config::deserialize(&frame, wincode_leb128_config()).map_err(|error| {
                Error::InvalidMetadata(format!("decode record {position}: {error}"))
            })?;
        if footer.is_some() {
            return Err(Error::InvalidMetadata(
                "metadata contains records after its footer".into(),
            ));
        }
        match (position, record) {
            (0, ArchiveV2HotMetaRecord::Header(header)) => {
                if header.version != WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION {
                    return Err(Error::InvalidMetadata(format!(
                        "unsupported hot-block metadata version {}",
                        header.version
                    )));
                }
                if header.flags & WINCODE_ARCHIVE_V2_FLAG_LEB128 == 0 {
                    return Err(Error::InvalidMetadata(
                        "metadata header does not declare LEB128 encoding".into(),
                    ));
                }
                if header.flags & WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY != 0 {
                    return Err(Error::InvalidMetadata(
                        "metadata describes a no-registry archive".into(),
                    ));
                }
                let known_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
                    | WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY
                    | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
                    | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
                if header.flags & !known_flags != 0 {
                    return Err(Error::InvalidMetadata(format!(
                        "metadata header has unknown flags {:#x}",
                        header.flags & !known_flags
                    )));
                }
            }
            (0, _) => {
                return Err(Error::InvalidMetadata(
                    "metadata does not begin with a header".into(),
                ));
            }
            (_, ArchiveV2HotMetaRecord::Header(_)) => {
                return Err(Error::InvalidMetadata("duplicate metadata header".into()));
            }
            (_, ArchiveV2HotMetaRecord::Genesis(_)) => {
                if saw_genesis || manifest.epoch != 0 {
                    return Err(Error::InvalidMetadata(
                        "unexpected or duplicate genesis metadata".into(),
                    ));
                }
                saw_genesis = true;
            }
            (_, ArchiveV2HotMetaRecord::Footer(value)) => footer = Some(value),
        }
        position += 1;
    }
    let footer =
        footer.ok_or_else(|| Error::InvalidMetadata("metadata does not end in a footer".into()))?;
    let transactions = index.rows.iter().try_fold(0u64, |total, row| {
        total
            .checked_add(u64::from(row.tx_count))
            .ok_or(Error::Overflow("metadata transaction total"))
    })?;
    if footer.blocks != index.rows.len() as u64 || footer.transactions != transactions {
        return Err(Error::InvalidMetadata(format!(
            "footer reports {} blocks/{} transactions; index reports {}/{}",
            footer.blocks,
            footer.transactions,
            index.rows.len(),
            transactions
        )));
    }
    Ok(footer)
}

fn read_frame(reader: &mut impl Read, max_length: usize) -> Result<Option<Vec<u8>>> {
    let Some(first) = read_byte(reader)? else {
        return Ok(None);
    };
    let mut length = u32::from(first & 0x7f);
    let mut byte = first;
    let mut shift = 7u32;
    while byte & 0x80 != 0 {
        if shift > 28 {
            return Err(Error::InvalidMetadata(
                "metadata frame length varint overflows u32".into(),
            ));
        }
        byte = read_byte(reader)?
            .ok_or_else(|| Error::InvalidMetadata("truncated metadata frame length".into()))?;
        if shift == 28 && byte & 0xf0 != 0 {
            return Err(Error::InvalidMetadata(
                "metadata frame length varint overflows u32".into(),
            ));
        }
        length |= u32::from(byte & 0x7f) << shift;
        shift += 7;
    }
    let length = length as usize;
    if length > max_length {
        return Err(Error::InvalidMetadata(format!(
            "metadata frame is {length} bytes, above the {max_length} byte limit"
        )));
    }
    let mut bytes = vec![0u8; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| Error::InvalidMetadata(format!("truncated metadata frame: {error}")))?;
    Ok(Some(bytes))
}

fn read_byte(reader: &mut impl Read) -> Result<Option<u8>> {
    let mut byte = [0u8; 1];
    match reader.read(&mut byte) {
        Ok(0) => Ok(None),
        Ok(1) => Ok(Some(byte[0])),
        Ok(_) => unreachable!("one-byte buffer"),
        Err(error) => Err(Error::InvalidMetadata(format!("read metadata: {error}"))),
    }
}

fn validate_decoded_block(
    index: &ArchiveV2HotBlockIndexRow,
    block: &ArchiveV2HotBlockBlob,
) -> Result<()> {
    let fail = |message: String| Error::InvalidBlock {
        slot: index.slot,
        message,
    };
    if block.header.slot != index.slot {
        return Err(fail(format!(
            "payload slot {} does not match index",
            block.header.slot
        )));
    }
    if block.tx_count != index.tx_count || block.tx_rows.len() != index.tx_count as usize {
        return Err(fail(format!(
            "payload has tx_count {}/{} rows, index declares {}",
            block.tx_count,
            block.tx_rows.len(),
            index.tx_count
        )));
    }
    let mut signatures = 0u32;
    let mut expected_message_offset = 0u32;
    let mut expected_metadata_offset = 0u32;
    for (number, row) in block.tx_rows.iter().enumerate() {
        if row.tx_index != number as u32 {
            return Err(fail(format!(
                "transaction row {number} has tx_index {}",
                row.tx_index
            )));
        }
        if row.reserved != [0; 3] {
            return Err(fail(format!(
                "transaction {} has non-zero reserved bytes",
                row.tx_index
            )));
        }
        if row.flags & !KNOWN_HOT_TX_FLAGS != 0 {
            return Err(fail(format!(
                "transaction {} has unknown flags {:#x}",
                row.tx_index,
                row.flags & !KNOWN_HOT_TX_FLAGS
            )));
        }
        if row.message_len == 0 || row.message_offset != expected_message_offset {
            return Err(fail(format!(
                "transaction {} has an empty or non-contiguous message range",
                row.tx_index
            )));
        }
        checked_region(
            &block.message_bytes,
            row.message_offset,
            row.message_len,
            "message",
            row.tx_index,
            index.slot,
        )?;
        expected_message_offset = row
            .message_offset
            .checked_add(row.message_len)
            .ok_or_else(|| fail("message offset overflow".into()))?;

        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            if row.metadata_len != 0 {
                return Err(fail(format!(
                    "transaction {} has metadata bytes without HAS_METADATA",
                    row.tx_index
                )));
            }
        } else {
            if row.metadata_len == 0 || row.metadata_offset != expected_metadata_offset {
                return Err(fail(format!(
                    "transaction {} has an empty or non-contiguous metadata range",
                    row.tx_index
                )));
            }
            checked_region(
                &block.metadata_bytes,
                row.metadata_offset,
                row.metadata_len,
                "metadata",
                row.tx_index,
                index.slot,
            )?;
            expected_metadata_offset = row
                .metadata_offset
                .checked_add(row.metadata_len)
                .ok_or_else(|| fail("metadata offset overflow".into()))?;
        }
        signatures = signatures
            .checked_add(u32::from(row.signature_count))
            .ok_or_else(|| fail("signature count overflow".into()))?;
    }
    if expected_message_offset as usize != block.message_bytes.len() {
        return Err(fail("message region has unindexed trailing bytes".into()));
    }
    if expected_metadata_offset as usize != block.metadata_bytes.len() {
        return Err(fail("metadata region has unindexed trailing bytes".into()));
    }
    if signatures != index.signature_count {
        return Err(fail(format!(
            "transaction rows report {signatures} signatures, index reports {}",
            index.signature_count
        )));
    }
    Ok(())
}

fn scan_transaction(
    slot: u64,
    row: ArchiveV2HotTxRow,
    block: &ArchiveV2HotBlockBlob,
    filter: &CompiledPubkeyFilter,
    registry_entries: u32,
    signatures: SignatureReference,
) -> Result<ScannedTransaction> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Ok(ScannedTransaction {
            slot,
            tx_index: row.tx_index,
            row,
            outcome: TransactionMatch::Indeterminate(IndeterminateReason::RawTransactionFallback),
            message: None,
            metadata: metadata_state(block, &row, slot, false)?,
            signatures,
        });
    }
    let message_bytes = checked_region(
        &block.message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        row.tx_index,
        slot,
    )?;
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_bytes, wincode_leb128_config()).map_err(|error| {
            Error::InvalidBlock {
                slot,
                message: format!("decode message for tx {}: {error}", row.tx_index),
            }
        })?;
    let is_v0 = matches!(message, ArchiveV2HotMessagePayload::V0(_));
    if is_v0 != (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0) {
        return Err(Error::InvalidBlock {
            slot,
            message: format!(
                "message version does not agree with flags for tx {}",
                row.tx_index
            ),
        });
    }
    let static_keys = match &message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.account_keys.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.account_keys.as_slice(),
    };
    let static_result = evaluate_keys(static_keys, filter, registry_entries);
    let needs_loaded = is_v0 && row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0;
    let read_metadata = static_result.matched || needs_loaded;
    let metadata = metadata_state(block, &row, slot, read_metadata)?;

    let mut loaded_result = KeyEvaluation::default();
    let loaded_unavailable = if needs_loaded {
        match &metadata {
            MetadataState::Decoded(metadata) => {
                loaded_result = evaluate_keys(
                    metadata
                        .loaded_writable_addresses
                        .iter()
                        .chain(&metadata.loaded_readonly_addresses),
                    filter,
                    registry_entries,
                );
                false
            }
            MetadataState::RawFallback | MetadataState::Absent | MetadataState::NotRead => true,
        }
    } else {
        false
    };

    let outcome = if static_result.matched || loaded_result.matched {
        TransactionMatch::Match {
            static_account: static_result.matched,
            loaded_address: loaded_result.matched,
        }
    } else if loaded_unavailable {
        TransactionMatch::Indeterminate(IndeterminateReason::V0LoadedAddressesUnavailable)
    } else if static_result.invalid || loaded_result.invalid {
        TransactionMatch::Indeterminate(IndeterminateReason::InvalidRegistryReference)
    } else {
        TransactionMatch::NoMatch
    };

    Ok(ScannedTransaction {
        slot,
        tx_index: row.tx_index,
        row,
        outcome,
        message: Some(message),
        metadata,
        signatures,
    })
}

fn metadata_state(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
    slot: u64,
    read: bool,
) -> Result<MetadataState> {
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 || row.metadata_len == 0 {
        return Ok(MetadataState::Absent);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Ok(MetadataState::RawFallback);
    }
    if !read {
        return Ok(MetadataState::NotRead);
    }
    let bytes = checked_region(
        &block.metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        row.tx_index,
        slot,
    )?;
    let metadata =
        wincode::config::deserialize(bytes, wincode_leb128_config()).map_err(|error| {
            Error::InvalidBlock {
                slot,
                message: format!("decode metadata for tx {}: {error}", row.tx_index),
            }
        })?;
    Ok(MetadataState::Decoded(Box::new(metadata)))
}

#[derive(Default)]
struct KeyEvaluation {
    matched: bool,
    invalid: bool,
}

fn evaluate_keys<'a>(
    keys: impl IntoIterator<Item = &'a CompactPubkey>,
    filter: &CompiledPubkeyFilter,
    registry_entries: u32,
) -> KeyEvaluation {
    let mut result = KeyEvaluation::default();
    for key in keys {
        match key {
            CompactPubkey::Id(id) => {
                if *id == 0 || *id > registry_entries {
                    result.invalid = true;
                } else if filter.registry_ids.contains(id) {
                    result.matched = true;
                }
            }
            CompactPubkey::Raw(pubkey) => {
                if filter.raw_pubkeys.contains(pubkey) {
                    result.matched = true;
                }
            }
        }
    }
    result
}

fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    length: u32,
    kind: &str,
    tx_index: u32,
    slot: u64,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(length as usize)
        .ok_or_else(|| Error::InvalidBlock {
            slot,
            message: format!("{kind} range overflow for tx {tx_index}"),
        })?;
    bytes.get(start..end).ok_or_else(|| Error::InvalidBlock {
        slot,
        message: format!(
            "{kind} range {start}..{end} is outside {} bytes for tx {tx_index}",
            bytes.len()
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::{Arc, Mutex},
    };

    use blockzilla_format::{
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ArchiveV2HotBlockHeader, ArchiveV2HotLegacyMessage,
        ArchiveV2HotV0Message, CompactMessageHeader, OwnedCompactRecentBlockhash,
        WincodeArchiveV2Header, write_archive_v2_hot_block_index,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{
        LocalRangeSource, SourceResult,
        manifest::{GenerationFile, compute_generation_digest},
    };

    const EPOCH: u64 = 1;
    const SLOTS_PER_EPOCH: u64 = 100;
    const RAW_KEY: [u8; 32] = [3; 32];
    const REGISTRY_KEY_ONE: [u8; 32] = [1; 32];
    const REGISTRY_KEY_TWO: [u8; 32] = [2; 32];

    struct Fixture {
        directory: TempDir,
    }

    impl Fixture {
        fn build() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            fs::write(
                root.join(REGISTRY_FILE),
                [REGISTRY_KEY_ONE.as_slice(), REGISTRY_KEY_TWO.as_slice()].concat(),
            )
            .unwrap();

            let first_message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: message_header(),
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw(RAW_KEY)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: Vec::new(),
            });
            let first_message =
                wincode::config::serialize(&first_message, wincode_leb128_config()).unwrap();
            let first_messages = [first_message.as_slice(), first_message.as_slice()].concat();
            let first_block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 101,
                    parent_slot: 100,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: Some(1_700_000_001),
                    block_height: Some(1000),
                    rewards: None,
                },
                tx_count: 2,
                tx_rows: vec![
                    ArchiveV2HotTxRow {
                        tx_index: 0,
                        flags: 0,
                        message_offset: 0,
                        message_len: first_message.len() as u32,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 2,
                        reserved: [0; 3],
                    },
                    ArchiveV2HotTxRow {
                        tx_index: 1,
                        flags: 0,
                        message_offset: first_message.len() as u32,
                        message_len: first_message.len() as u32,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 1,
                        reserved: [0; 3],
                    },
                ],
                message_bytes: first_messages,
                metadata_bytes: Vec::new(),
            };

            let second_message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: message_header(),
                account_keys: vec![CompactPubkey::Id(1)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
                instructions: Vec::new(),
                address_table_lookups: Vec::new(),
            });
            let second_message =
                wincode::config::serialize(&second_message, wincode_leb128_config()).unwrap();
            let second_metadata = CompactMetaV1 {
                err: None,
                fee: 5000,
                pre_balances: Vec::new(),
                post_balances: Vec::new(),
                inner_instructions: None,
                logs: None,
                pre_token_balances: Vec::new(),
                post_token_balances: Vec::new(),
                rewards: Vec::new(),
                loaded_writable_addresses: vec![CompactPubkey::Id(2)],
                loaded_readonly_addresses: Vec::new(),
                return_data: None,
                compute_units_consumed: Some(42),
                cost_units: None,
            };
            let second_metadata =
                wincode::config::serialize(&second_metadata, wincode_leb128_config()).unwrap();
            let second_block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 102,
                    parent_slot: 101,
                    blockhash_id: 2,
                    previous_blockhash_id: 1,
                    block_time: Some(1_700_000_002),
                    block_height: Some(1001),
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                    message_offset: 0,
                    message_len: second_message.len() as u32,
                    metadata_offset: 0,
                    metadata_len: second_metadata.len() as u32,
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes: second_message,
                metadata_bytes: second_metadata,
            };

            let first_uncompressed =
                wincode::config::serialize(&first_block, wincode_leb128_config()).unwrap();
            let second_uncompressed =
                wincode::config::serialize(&second_block, wincode_leb128_config()).unwrap();
            let first_compressed = zstd::bulk::compress(&first_uncompressed, 3).unwrap();
            let second_compressed = zstd::bulk::compress(&second_uncompressed, 3).unwrap();
            let blocks = [first_compressed.as_slice(), second_compressed.as_slice()].concat();
            fs::write(root.join(BLOCKS_FILE), &blocks).unwrap();

            let rows = vec![
                ArchiveV2HotBlockIndexRow {
                    block_id: 0,
                    slot: 101,
                    compressed_offset: 0,
                    compressed_len: first_compressed.len() as u32,
                    uncompressed_len: first_uncompressed.len() as u32,
                    tx_count: 2,
                    first_tx_ordinal: 0,
                    first_signature_ordinal: 0,
                    signature_count: 3,
                },
                ArchiveV2HotBlockIndexRow {
                    block_id: 1,
                    slot: 102,
                    compressed_offset: first_compressed.len() as u64,
                    compressed_len: second_compressed.len() as u32,
                    uncompressed_len: second_uncompressed.len() as u32,
                    tx_count: 1,
                    first_tx_ordinal: 2,
                    first_signature_ordinal: 3,
                    signature_count: 1,
                },
            ];
            write_archive_v2_hot_block_index(
                &root.join(BLOCK_INDEX_FILE),
                blocks.len() as u64,
                3,
                0,
                &rows,
            )
            .unwrap();
            fs::write(
                root.join(SIGNATURES_FILE),
                [
                    [7u8; 64].as_slice(),
                    [70u8; 64].as_slice(),
                    [9u8; 64].as_slice(),
                    [8u8; 64].as_slice(),
                ]
                .concat(),
            )
            .unwrap();

            let metadata_records = [
                ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                    version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                    flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
                }),
                ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                    blocks: 2,
                    transactions: 3,
                    ..WincodeArchiveV2Footer::default()
                }),
            ];
            let mut metadata = Vec::new();
            for record in metadata_records {
                let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
                write_u32_varint(&mut metadata, bytes.len() as u32);
                metadata.extend_from_slice(&bytes);
            }
            fs::write(root.join(META_FILE), metadata).unwrap();

            write_manifest(root, true, None);
            Self { directory }
        }

        fn source(&self) -> LocalRangeSource {
            LocalRangeSource::new(self.directory.path())
        }
    }

    #[derive(Clone)]
    struct CountingSource {
        inner: LocalRangeSource,
        reads: Arc<Mutex<Vec<(String, u64, usize)>>>,
    }

    impl CountingSource {
        fn new(inner: LocalRangeSource) -> Self {
            Self {
                inner,
                reads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn clear(&self) {
            self.reads.lock().unwrap().clear();
        }

        fn reads_for(&self, object: &str) -> Vec<(u64, usize)> {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, _, _)| name == object)
                .map(|(_, offset, length)| (*offset, *length))
                .collect()
        }
    }

    impl RangeSource for CountingSource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            self.reads
                .lock()
                .unwrap()
                .push((object.to_owned(), offset, length));
            self.inner.read_range(object, offset, length)
        }
    }

    #[test]
    fn strict_reader_matches_registry_raw_and_v0_loaded_pubkeys_and_reads_signatures() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        assert_eq!(archive.index().rows.len(), 2);
        assert_eq!(archive.metadata_footer().transactions, 3);

        let raw_filter = archive.compile_pubkey_filter([RAW_KEY]).unwrap();
        let first = archive.scan(&raw_filter).unwrap().next().unwrap().unwrap();
        assert_eq!(
            first.transactions[0].outcome,
            TransactionMatch::Match {
                static_account: true,
                loaded_address: false
            }
        );
        assert_eq!(
            archive
                .read_transaction_signatures(first.transactions[0].signatures)
                .unwrap(),
            vec![[7u8; 64], [70u8; 64]]
        );
        assert_eq!(first.transactions[1].signatures.first_ordinal, 2);
        assert_eq!(
            archive
                .read_transaction_signatures(first.transactions[1].signatures)
                .unwrap(),
            vec![[9u8; 64]]
        );

        let loaded_filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();
        let blocks: Vec<_> = archive
            .scan(&loaded_filter)
            .unwrap()
            .map(|block| block.unwrap())
            .collect();
        assert_eq!(blocks[0].transactions[0].outcome, TransactionMatch::NoMatch);
        assert_eq!(
            blocks[1].transactions[0].outcome,
            TransactionMatch::Match {
                static_account: false,
                loaded_address: true
            }
        );
        assert!(matches!(
            blocks[1].transactions[0].metadata,
            MetadataState::Decoded(_)
        ));
        assert_eq!(archive.read_signature_ordinal(3).unwrap(), [8u8; 64]);
    }

    #[test]
    fn sequential_iterator_coalesces_adjacent_frames_into_one_range_read() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        source.clear();
        let blocks: Vec<_> = archive.blocks().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(blocks.len(), 2);
        let reads = source.reads_for(BLOCKS_FILE);
        assert_eq!(reads.len(), 1, "reads were {reads:?}");
        assert_eq!(reads[0].0, 0);
        assert_eq!(reads[0].1 as u64, archive.index().blob_file_bytes);
    }

    #[test]
    fn control_file_policy_does_not_download_blocks_or_signatures_during_open() {
        let fixture = Fixture::build();
        let source = CountingSource::new(fixture.source());
        let options = OpenOptions {
            hash_verification: HashVerification::ControlFiles,
            ..OpenOptions::default()
        };
        let _archive = ArchiveReader::open_with_options(source.clone(), options).unwrap();
        assert!(source.reads_for(BLOCKS_FILE).is_empty());
        assert!(source.reads_for(SIGNATURES_FILE).is_empty());
        assert!(!source.reads_for(REGISTRY_FILE).is_empty());
        assert!(!source.reads_for(BLOCK_INDEX_FILE).is_empty());
        assert!(!source.reads_for(META_FILE).is_empty());
    }

    #[test]
    fn compressed_frame_limit_is_checked_before_reading_blocks() {
        let fixture = Fixture::build();
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            max_compressed_frame_bytes: 1,
            ..OpenOptions::default()
        };
        let error = ArchiveReader::open_with_options(fixture.source(), options).unwrap_err();
        assert!(matches!(error, Error::InvalidIndex(_)));
        assert!(error.to_string().contains("compressed bytes"));
    }

    #[test]
    fn raw_transaction_and_unavailable_v0_loaded_addresses_are_indeterminate() {
        let fixture = Fixture::build();
        let options = OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(fixture.source(), options).unwrap();
        let filter = archive.compile_pubkey_filter([REGISTRY_KEY_TWO]).unwrap();

        let v0_message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: message_header(),
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
            address_table_lookups: Vec::new(),
        });
        let v0_message = wincode::config::serialize(&v0_message, wincode_leb128_config()).unwrap();
        let raw_fallback = vec![0xff];
        let message_bytes = [raw_fallback.as_slice(), v0_message.as_slice()].concat();
        let rows = vec![
            ArchiveV2HotTxRow {
                tx_index: 0,
                flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                message_offset: 0,
                message_len: raw_fallback.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            },
            ArchiveV2HotTxRow {
                tx_index: 1,
                flags: ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                    | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message_offset: raw_fallback.len() as u32,
                message_len: v0_message.len() as u32,
                metadata_offset: 0,
                metadata_len: 1,
                signature_count: 1,
                reserved: [0; 3],
            },
        ];
        let decoded = DecodedBlock {
            index_row: ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 101,
                compressed_offset: 0,
                compressed_len: 1,
                uncompressed_len: 1,
                tx_count: 2,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 2,
            },
            block: ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: 101,
                    parent_slot: 100,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 2,
                tx_rows: rows,
                message_bytes,
                metadata_bytes: vec![0xaa],
            },
        };
        let scanned = archive.scan_decoded_block(&filter, decoded).unwrap();
        assert_eq!(
            scanned.transactions[0].outcome,
            TransactionMatch::Indeterminate(IndeterminateReason::RawTransactionFallback)
        );
        assert_eq!(
            scanned.transactions[1].outcome,
            TransactionMatch::Indeterminate(IndeterminateReason::V0LoadedAddressesUnavailable)
        );
        assert!(matches!(
            scanned.transactions[1].metadata,
            MetadataState::RawFallback
        ));
    }

    #[test]
    fn open_rejects_incomplete_and_missing_required_generation() {
        let fixture = Fixture::build();
        write_manifest(fixture.directory.path(), false, None);
        assert!(matches!(
            ArchiveReader::open(fixture.source()).unwrap_err(),
            Error::IncompleteGeneration
        ));

        write_manifest(fixture.directory.path(), true, Some(META_FILE));
        assert!(matches!(
            ArchiveReader::open(fixture.source()).unwrap_err(),
            Error::MissingFile(name) if name == META_FILE
        ));
    }

    fn message_header() -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        }
    }

    fn write_manifest(root: &Path, complete: bool, omit: Option<&str>) {
        let mut files = Vec::new();
        for name in [
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            META_FILE,
            REGISTRY_FILE,
            SIGNATURES_FILE,
        ] {
            if omit == Some(name) {
                continue;
            }
            let bytes = fs::read(root.join(name)).unwrap();
            files.push(GenerationFile {
                name: name.into(),
                size: bytes.len() as u64,
                sha256: hex_lower(&Sha256::digest(&bytes)),
            });
        }
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "fixture-generation".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: SLOTS_PER_EPOCH,
            complete,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        fs::write(
            root.join(GENERATION_MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn write_u32_varint(output: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
}
