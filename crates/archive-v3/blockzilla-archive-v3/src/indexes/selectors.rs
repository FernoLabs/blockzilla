//! `indexes/selectors.pages`: program and instruction-data selector postings.
//!
//! A selector is the first `min(instruction_data.len(), 8)` bytes. The key also
//! carries that length, so empty, short, and eight-byte selectors are distinct.
//! Data after byte eight does not enter the key. A posting names its transaction
//! and the zero-based instruction ordinal in one of the two canonical data
//! owners. A reader can therefore verify every candidate against either
//! `ledger/transactions.wincode` or `runtime/inner_instructions.wincode`
//! without trusting this derived file.
//!
//! Pages end on key boundaries. Only a key with too many postings can continue
//! into another page. Continuation flags are symmetric and the directory keeps
//! both boundary keys, which gives an exact binary-search point lookup.

use std::{fmt, ops::Range};

use thiserror::Error;

use crate::varint::{VarintError, read_uleb128, write_uleb128};

pub const PATH: &str = "indexes/selectors.pages";
pub const SCHEMA: u16 = 1;

pub const MAX_SELECTOR_LEN: usize = 8;
pub const KEY_LEN: usize = 16;
pub const DIRECTORY_ENTRY_LEN: usize = 64;
pub const DIRECTORY_FOOTER_LEN: usize = 24;
pub const DIRECTORY_FOOTER_MAGIC: [u8; 8] = *b"BZIASDIR";
/// Allocation guard for one independently decoded selector page.
pub const MAX_PAGE_DECODED_BYTES: u32 = 64 << 20;

pub const PAGE_FLAG_ZSTD: u16 = 1 << 0;
pub const PAGE_FLAG_CONTINUED_FROM_PREVIOUS: u16 = 1 << 1;
pub const PAGE_FLAG_CONTINUES_IN_NEXT: u16 = 1 << 2;
pub const KNOWN_PAGE_FLAGS: u16 =
    PAGE_FLAG_ZSTD | PAGE_FLAG_CONTINUED_FROM_PREVIOUS | PAGE_FLAG_CONTINUES_IN_NEXT;

/// One fixed-width selector key.
///
/// Wire bytes are the little-endian program registry ID, one selector length,
/// eight selector bytes padded with zero, and three reserved zero bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectorKey {
    pub program_id: u32,
    selector_len: u8,
    selector: [u8; MAX_SELECTOR_LEN],
}

impl SelectorKey {
    /// Build the key owned by one canonical instruction payload.
    pub fn from_instruction(program_id: u32, data: &[u8]) -> Self {
        let selector_len = data.len().min(MAX_SELECTOR_LEN);
        let mut selector = [0_u8; MAX_SELECTOR_LEN];
        selector[..selector_len].copy_from_slice(&data[..selector_len]);
        Self {
            program_id,
            selector_len: selector_len as u8,
            selector,
        }
    }

    /// Build an exact query key. A query selector cannot exceed eight bytes.
    pub fn new(program_id: u32, selector: &[u8]) -> Result<Self, SelectorIndexError> {
        if selector.len() > MAX_SELECTOR_LEN {
            return Err(SelectorIndexError::SelectorTooLong(selector.len()));
        }
        Ok(Self::from_instruction(program_id, selector))
    }

    pub const fn selector_len(self) -> u8 {
        self.selector_len
    }

    pub fn selector(&self) -> &[u8] {
        &self.selector[..usize::from(self.selector_len)]
    }

    pub fn encode(self) -> [u8; KEY_LEN] {
        let mut out = [0_u8; KEY_LEN];
        out[0..4].copy_from_slice(&self.program_id.to_le_bytes());
        out[4] = self.selector_len;
        out[5..13].copy_from_slice(&self.selector);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, SelectorIndexError> {
        let bytes: &[u8; KEY_LEN] = input
            .get(..KEY_LEN)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(SelectorIndexError::KeyTruncated(input.len()))?;
        let selector_len = bytes[4];
        if usize::from(selector_len) > MAX_SELECTOR_LEN {
            return Err(SelectorIndexError::SelectorTooLong(usize::from(
                selector_len,
            )));
        }
        if bytes[5 + usize::from(selector_len)..13]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(SelectorIndexError::SelectorPaddingSet);
        }
        if bytes[13..16] != [0; 3] {
            return Err(SelectorIndexError::KeyReservedBytesSet);
        }
        let mut selector = [0_u8; MAX_SELECTOR_LEN];
        selector.copy_from_slice(&bytes[5..13]);
        Ok(Self {
            program_id: u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")),
            selector_len,
            selector,
        })
    }
}

/// Which canonical instruction and data roles own a posting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum InstructionScope {
    TopLevel = 0,
    Cpi = 1,
}

impl TryFrom<u8> for InstructionScope {
    type Error = SelectorIndexError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::TopLevel),
            1 => Ok(Self::Cpi),
            other => Err(SelectorIndexError::UnknownInstructionScope(other)),
        }
    }
}

/// One selector occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Posting {
    pub transaction_ordinal: u64,
    pub scope: InstructionScope,
    /// Zero-based ordinal in the instruction and data planes selected by
    /// `scope`. Top-level and CPI ordinals are independent.
    pub role_local_instruction_ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPostings {
    pub key: SelectorKey,
    pub postings: Vec<Posting>,
}

/// One independently decodable page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageDirectoryEntry {
    pub first_key: SelectorKey,
    pub last_key: SelectorKey,
    pub offset: u64,
    pub stored_len: u32,
    pub decoded_len: u32,
    pub key_count: u32,
    pub posting_count: u32,
    pub flags: u16,
}

impl PageDirectoryEntry {
    pub fn encode(self) -> [u8; DIRECTORY_ENTRY_LEN] {
        let mut out = [0_u8; DIRECTORY_ENTRY_LEN];
        out[0..16].copy_from_slice(&self.first_key.encode());
        out[16..32].copy_from_slice(&self.last_key.encode());
        out[32..40].copy_from_slice(&self.offset.to_le_bytes());
        out[40..44].copy_from_slice(&self.stored_len.to_le_bytes());
        out[44..48].copy_from_slice(&self.decoded_len.to_le_bytes());
        out[48..52].copy_from_slice(&self.key_count.to_le_bytes());
        out[52..56].copy_from_slice(&self.posting_count.to_le_bytes());
        out[56..58].copy_from_slice(&self.flags.to_le_bytes());
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, SelectorIndexError> {
        let bytes: &[u8; DIRECTORY_ENTRY_LEN] = input
            .get(..DIRECTORY_ENTRY_LEN)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(SelectorIndexError::DirectoryEntryTruncated(input.len()))?;
        if bytes[58..64] != [0; 6] {
            return Err(SelectorIndexError::DirectoryReservedBytesSet);
        }
        let entry = Self {
            first_key: SelectorKey::decode(&bytes[0..16])?,
            last_key: SelectorKey::decode(&bytes[16..32])?,
            offset: u64::from_le_bytes(bytes[32..40].try_into().expect("8 bytes")),
            stored_len: u32::from_le_bytes(bytes[40..44].try_into().expect("4 bytes")),
            decoded_len: u32::from_le_bytes(bytes[44..48].try_into().expect("4 bytes")),
            key_count: u32::from_le_bytes(bytes[48..52].try_into().expect("4 bytes")),
            posting_count: u32::from_le_bytes(bytes[52..56].try_into().expect("4 bytes")),
            flags: u16::from_le_bytes(bytes[56..58].try_into().expect("2 bytes")),
        };
        entry.validate()?;
        Ok(entry)
    }

    pub const fn is_zstd(self) -> bool {
        self.flags & PAGE_FLAG_ZSTD != 0
    }

    pub const fn continued_from_previous(self) -> bool {
        self.flags & PAGE_FLAG_CONTINUED_FROM_PREVIOUS != 0
    }

    pub const fn continues_in_next(self) -> bool {
        self.flags & PAGE_FLAG_CONTINUES_IN_NEXT != 0
    }

    pub fn may_contain(self, key: SelectorKey) -> bool {
        key >= self.first_key && key <= self.last_key
    }

    fn validate(self) -> Result<(), SelectorIndexError> {
        if self.flags & !KNOWN_PAGE_FLAGS != 0 {
            return Err(SelectorIndexError::UnknownPageFlags(self.flags));
        }
        if self.first_key > self.last_key {
            return Err(SelectorIndexError::DirectoryKeyRangeInverted);
        }
        if self.stored_len == 0 || self.decoded_len == 0 {
            return Err(SelectorIndexError::EmptyStoredPage);
        }
        if (self.is_zstd() && self.stored_len >= self.decoded_len)
            || (!self.is_zstd() && self.stored_len != self.decoded_len)
        {
            return Err(SelectorIndexError::StoredLengthDisagreesWithCodec {
                stored: self.stored_len,
                decoded: self.decoded_len,
                zstd: self.is_zstd(),
            });
        }
        if self.decoded_len > MAX_PAGE_DECODED_BYTES {
            return Err(SelectorIndexError::PageAboveDecodeGuard(self.decoded_len));
        }
        if self.key_count == 0 || self.posting_count == 0 {
            return Err(SelectorIndexError::EmptyPage);
        }
        if (self.continued_from_previous() || self.continues_in_next())
            && (self.key_count != 1 || self.first_key != self.last_key)
        {
            return Err(SelectorIndexError::ContinuationHasSeveralKeys);
        }
        Ok(())
    }
}

/// Footer at the end of the object payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryFooter {
    pub directory_offset: u64,
    pub page_count: u64,
}

impl DirectoryFooter {
    pub fn encode(self) -> [u8; DIRECTORY_FOOTER_LEN] {
        let mut out = [0_u8; DIRECTORY_FOOTER_LEN];
        out[0..8].copy_from_slice(&DIRECTORY_FOOTER_MAGIC);
        out[8..16].copy_from_slice(&self.directory_offset.to_le_bytes());
        out[16..24].copy_from_slice(&self.page_count.to_le_bytes());
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, SelectorIndexError> {
        let bytes: &[u8; DIRECTORY_FOOTER_LEN] = input
            .get(..DIRECTORY_FOOTER_LEN)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(SelectorIndexError::DirectoryFooterTruncated(input.len()))?;
        if bytes[0..8] != DIRECTORY_FOOTER_MAGIC {
            return Err(SelectorIndexError::WrongDirectoryMagic);
        }
        Ok(Self {
            directory_offset: u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes")),
            page_count: u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes")),
        })
    }
}

/// Encode one independent page. Keys and postings must be in strict canonical
/// order. A continuation page is supplied as one key fragment.
pub fn encode_page(keys: &[KeyPostings]) -> Result<Vec<u8>, SelectorIndexError> {
    if keys.is_empty() {
        return Err(SelectorIndexError::EmptyPage);
    }
    let mut out = Vec::new();
    let mut previous_key = None;
    for entry in keys {
        if let Some(previous) = previous_key
            && entry.key <= previous
        {
            return Err(SelectorIndexError::KeysNotAscending);
        }
        previous_key = Some(entry.key);
        if entry.postings.is_empty() {
            return Err(SelectorIndexError::KeyWithNoPostings(entry.key));
        }
        out.extend_from_slice(&entry.key.encode());
        write_uleb128(&mut out, entry.postings.len() as u64);
        encode_postings(&mut out, entry.key, &entry.postings)?;
    }
    Ok(out)
}

fn encode_postings(
    out: &mut Vec<u8>,
    key: SelectorKey,
    postings: &[Posting],
) -> Result<(), SelectorIndexError> {
    let mut previous = None;
    let mut previous_transaction = 0_u64;
    let mut previous_role_ordinals = [None, None];
    for posting in postings.iter().copied() {
        if let Some(previous) = previous
            && posting <= previous
        {
            return Err(SelectorIndexError::PostingsNotAscending { key });
        }
        let transaction_gap = if previous.is_none() {
            posting.transaction_ordinal
        } else {
            posting
                .transaction_ordinal
                .checked_sub(previous_transaction)
                .ok_or(SelectorIndexError::PostingsNotAscending { key })?
        };
        let packed = transaction_gap
            .checked_shl(1)
            .filter(|packed| *packed >> 1 == transaction_gap)
            .ok_or(SelectorIndexError::TransactionGapOverflow(transaction_gap))?
            | posting.scope as u64;
        write_uleb128(out, packed);

        let scope = posting.scope as usize;
        let role_gap = match previous_role_ordinals[scope] {
            None => posting.role_local_instruction_ordinal,
            Some(previous_ordinal) => posting
                .role_local_instruction_ordinal
                .checked_sub(previous_ordinal)
                .filter(|gap| *gap > 0)
                .ok_or(SelectorIndexError::RoleOrdinalsNotAscending {
                    scope: posting.scope,
                })?,
        };
        write_uleb128(out, role_gap);
        previous_role_ordinals[scope] = Some(posting.role_local_instruction_ordinal);
        previous_transaction = posting.transaction_ordinal;
        previous = Some(posting);
    }
    Ok(())
}

/// Decode a page and require its directory counts and byte extent to match.
pub fn decode_page(
    payload: &[u8],
    key_count: u32,
    posting_count: u32,
) -> Result<Vec<KeyPostings>, SelectorIndexError> {
    if key_count == 0 || posting_count == 0 {
        return Err(SelectorIndexError::EmptyPage);
    }
    let mut cursor = 0_usize;
    let mut keys = Vec::with_capacity(key_count as usize);
    let mut previous_key = None;
    let mut decoded_postings = 0_u32;
    for _ in 0..key_count {
        let key_end = cursor
            .checked_add(KEY_LEN)
            .ok_or(SelectorIndexError::PageKeyTruncated)?;
        let key_bytes = payload
            .get(cursor..key_end)
            .ok_or(SelectorIndexError::PageKeyTruncated)?;
        let key = SelectorKey::decode(key_bytes)?;
        cursor = key_end;
        if let Some(previous) = previous_key
            && key <= previous
        {
            return Err(SelectorIndexError::KeysNotAscending);
        }
        previous_key = Some(key);
        let count = read_uleb128(payload, &mut cursor)?;
        if count == 0 {
            return Err(SelectorIndexError::KeyWithNoPostings(key));
        }
        let count = u32::try_from(count).map_err(|_| SelectorIndexError::PostingCountOverflow)?;
        decoded_postings = decoded_postings
            .checked_add(count)
            .ok_or(SelectorIndexError::PostingCountOverflow)?;
        let postings = decode_postings(payload, &mut cursor, key, count)?;
        keys.push(KeyPostings { key, postings });
    }
    if decoded_postings != posting_count {
        return Err(SelectorIndexError::PostingCountMismatch {
            declared: posting_count,
            decoded: decoded_postings,
        });
    }
    if cursor != payload.len() {
        return Err(SelectorIndexError::TrailingBytes {
            consumed: cursor,
            total: payload.len(),
        });
    }
    Ok(keys)
}

fn decode_postings(
    payload: &[u8],
    cursor: &mut usize,
    key: SelectorKey,
    count: u32,
) -> Result<Vec<Posting>, SelectorIndexError> {
    let remaining = payload.len() - *cursor;
    if count as usize > remaining / 2 {
        return Err(SelectorIndexError::PostingCountExceedsPage { count, remaining });
    }
    let mut postings = Vec::with_capacity(count as usize);
    let mut transaction_ordinal = 0_u64;
    let mut previous_role_ordinals: [Option<u64>; 2] = [None, None];
    let mut previous = None;
    for position in 0..count {
        let packed = read_uleb128(payload, cursor)?;
        let scope = InstructionScope::try_from((packed & 1) as u8)?;
        let transaction_gap = packed >> 1;
        transaction_ordinal = if position == 0 {
            transaction_gap
        } else {
            transaction_ordinal
                .checked_add(transaction_gap)
                .ok_or(SelectorIndexError::TransactionOrdinalOverflow)?
        };
        let role_gap = read_uleb128(payload, cursor)?;
        let scope_index = scope as usize;
        let role_local_instruction_ordinal = match previous_role_ordinals[scope_index] {
            None => role_gap,
            Some(previous_ordinal) => {
                if role_gap == 0 {
                    return Err(SelectorIndexError::RoleOrdinalsNotAscending { scope });
                }
                previous_ordinal
                    .checked_add(role_gap)
                    .ok_or(SelectorIndexError::RoleOrdinalOverflow)?
            }
        };
        let posting = Posting {
            transaction_ordinal,
            scope,
            role_local_instruction_ordinal,
        };
        if let Some(previous) = previous
            && posting <= previous
        {
            return Err(SelectorIndexError::PostingsNotAscending { key });
        }
        previous_role_ordinals[scope_index] = Some(role_local_instruction_ordinal);
        previous = Some(posting);
        postings.push(posting);
    }
    Ok(postings)
}

pub fn find_key(page: &[KeyPostings], key: SelectorKey) -> Option<&KeyPostings> {
    page.binary_search_by_key(&key, |entry| entry.key)
        .ok()
        .map(|index| &page[index])
}

/// Validate page ordering, continuation symmetry, and contiguous page extents.
pub fn validate_directory(
    entries: &[PageDirectoryEntry],
    first_page_offset: u64,
    directory_offset: u64,
) -> Result<(), SelectorIndexError> {
    let mut next_offset = first_page_offset;
    for (index, entry) in entries.iter().copied().enumerate() {
        entry.validate()?;
        if entry.offset != next_offset {
            return Err(SelectorIndexError::PageExtentGap {
                expected: next_offset,
                actual: entry.offset,
            });
        }
        next_offset = entry
            .offset
            .checked_add(u64::from(entry.stored_len))
            .ok_or(SelectorIndexError::PageExtentOverflow)?;

        let previous = index.checked_sub(1).and_then(|at| entries.get(at));
        if entry.continued_from_previous() {
            let previous = previous.ok_or(SelectorIndexError::BrokenContinuation)?;
            if !previous.continues_in_next()
                || previous.last_key != entry.first_key
                || previous.first_key != previous.last_key
            {
                return Err(SelectorIndexError::BrokenContinuation);
            }
        } else if let Some(previous) = previous
            && (previous.continues_in_next() || previous.last_key >= entry.first_key)
        {
            return Err(SelectorIndexError::DirectoryPagesNotAscending);
        }
    }
    if entries
        .last()
        .is_some_and(|entry| entry.continues_in_next())
    {
        return Err(SelectorIndexError::BrokenContinuation);
    }
    if next_offset != directory_offset {
        return Err(SelectorIndexError::DirectoryOffsetMismatch {
            expected: next_offset,
            actual: directory_offset,
        });
    }
    Ok(())
}

/// Return the exact directory range that can hold `key`.
///
/// The binary search is logarithmic. More than one result is possible only for
/// explicit continuation pages of the same key.
pub fn candidate_page_range(entries: &[PageDirectoryEntry], key: SelectorKey) -> Range<usize> {
    let mut lo = 0_usize;
    let mut hi = entries.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].last_key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let start = lo;
    while lo < entries.len() && entries[lo].first_key <= key && key <= entries[lo].last_key {
        lo += 1;
    }
    start..lo
}

/// Join one key from its normal page or continuation pages.
pub fn point_lookup(
    key: SelectorKey,
    pages: impl IntoIterator<Item = Vec<KeyPostings>>,
) -> Result<Vec<Posting>, SelectorIndexError> {
    let mut result = Vec::new();
    for page in pages {
        if let Some(found) = find_key(&page, key) {
            if let (Some(previous), Some(current)) = (result.last(), found.postings.first())
                && current <= previous
            {
                return Err(SelectorIndexError::PostingsNotAscending { key });
            }
            result.extend_from_slice(&found.postings);
        }
    }
    Ok(result)
}

/// Canonical data returned by a verifier's role-local lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalInstruction<B> {
    pub transaction_ordinal: u64,
    pub program_id: u32,
    pub data: B,
}

/// Verify that postings point at canonical instructions with the same exact
/// selector key. A wrong transaction, program, scope ordinal, or data prefix is
/// rejected.
pub fn verify_canonical_postings<E, B>(
    key: SelectorKey,
    postings: &[Posting],
    mut load: impl FnMut(InstructionScope, u64) -> Result<CanonicalInstruction<B>, E>,
) -> Result<(), CanonicalVerificationError<E>>
where
    B: AsRef<[u8]>,
{
    let mut previous = None;
    for posting in postings.iter().copied() {
        if let Some(previous) = previous
            && posting <= previous
        {
            return Err(CanonicalVerificationError::PostingsNotAscending);
        }
        let canonical = load(posting.scope, posting.role_local_instruction_ordinal)
            .map_err(CanonicalVerificationError::Load)?;
        let actual_key =
            SelectorKey::from_instruction(canonical.program_id, canonical.data.as_ref());
        if canonical.transaction_ordinal != posting.transaction_ordinal || actual_key != key {
            return Err(CanonicalVerificationError::CanonicalMismatch {
                posting,
                actual_transaction_ordinal: canonical.transaction_ordinal,
                actual_key,
            });
        }
        previous = Some(posting);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalVerificationError<E> {
    Load(E),
    PostingsNotAscending,
    CanonicalMismatch {
        posting: Posting,
        actual_transaction_ordinal: u64,
        actual_key: SelectorKey,
    },
}

impl<E: fmt::Display> fmt::Display for CanonicalVerificationError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(error) => write!(formatter, "load canonical instruction: {error}"),
            Self::PostingsNotAscending => write!(formatter, "selector postings do not ascend"),
            Self::CanonicalMismatch {
                posting,
                actual_transaction_ordinal,
                actual_key,
            } => write!(
                formatter,
                "selector posting {posting:?} resolves to transaction {actual_transaction_ordinal} and key {actual_key:?}"
            ),
        }
    }
}

impl<E> std::error::Error for CanonicalVerificationError<E> where E: std::error::Error + 'static {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SelectorIndexError {
    #[error("selector stream: {0}")]
    Varint(#[from] VarintError),
    #[error("selector length is {0}, above the 8-byte schema limit")]
    SelectorTooLong(usize),
    #[error("selector key needs {KEY_LEN} bytes, found {0}")]
    KeyTruncated(usize),
    #[error("selector key has non-zero bytes after its declared selector")]
    SelectorPaddingSet,
    #[error("selector key has non-zero reserved bytes")]
    KeyReservedBytesSet,
    #[error("unknown instruction scope {0}")]
    UnknownInstructionScope(u8),
    #[error("selector directory entry needs {DIRECTORY_ENTRY_LEN} bytes, found {0}")]
    DirectoryEntryTruncated(usize),
    #[error("selector directory entry has non-zero reserved bytes")]
    DirectoryReservedBytesSet,
    #[error("selector directory footer needs {DIRECTORY_FOOTER_LEN} bytes, found {0}")]
    DirectoryFooterTruncated(usize),
    #[error("wrong selector directory magic")]
    WrongDirectoryMagic,
    #[error("selector page has unknown flags {0:#x}")]
    UnknownPageFlags(u16),
    #[error("selector directory key range is inverted")]
    DirectoryKeyRangeInverted,
    #[error("selector page has no stored or decoded bytes")]
    EmptyStoredPage,
    #[error(
        "selector page stores {stored} bytes for {decoded} decoded bytes but zstd flag is {zstd}"
    )]
    StoredLengthDisagreesWithCodec {
        stored: u32,
        decoded: u32,
        zstd: bool,
    },
    #[error("selector page declares {0} decoded bytes, above the decode guard")]
    PageAboveDecodeGuard(u32),
    #[error("selector page must contain at least one key and posting")]
    EmptyPage,
    #[error("a continuation page must contain exactly one key")]
    ContinuationHasSeveralKeys,
    #[error("selector keys must strictly ascend")]
    KeysNotAscending,
    #[error("selector key {0:?} has no postings")]
    KeyWithNoPostings(SelectorKey),
    #[error("selector postings for key {key:?} must strictly ascend")]
    PostingsNotAscending { key: SelectorKey },
    #[error("transaction gap {0} cannot be packed with its instruction scope")]
    TransactionGapOverflow(u64),
    #[error("transaction ordinal overflows")]
    TransactionOrdinalOverflow,
    #[error("{scope:?} role-local instruction ordinals must strictly ascend")]
    RoleOrdinalsNotAscending { scope: InstructionScope },
    #[error("role-local instruction ordinal overflows")]
    RoleOrdinalOverflow,
    #[error("selector page key is truncated")]
    PageKeyTruncated,
    #[error("selector posting count overflows u32")]
    PostingCountOverflow,
    #[error("selector page declares {count} postings but has only {remaining} bytes left")]
    PostingCountExceedsPage { count: u32, remaining: usize },
    #[error("selector page declares {declared} postings but decodes {decoded}")]
    PostingCountMismatch { declared: u32, decoded: u32 },
    #[error("page has {consumed} of {total} bytes consumed after its declared keys")]
    TrailingBytes { consumed: usize, total: usize },
    #[error("selector page offset is {actual}, expected {expected}")]
    PageExtentGap { expected: u64, actual: u64 },
    #[error("selector page extent overflows u64")]
    PageExtentOverflow,
    #[error("selector directory continuation flags or keys do not agree")]
    BrokenContinuation,
    #[error("selector directory pages do not strictly ascend")]
    DirectoryPagesNotAscending,
    #[error("selector directory starts at {actual}, expected {expected}")]
    DirectoryOffsetMismatch { expected: u64, actual: u64 },
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    fn posting(transaction: u64, scope: InstructionScope, ordinal: u64) -> Posting {
        Posting {
            transaction_ordinal: transaction,
            scope,
            role_local_instruction_ordinal: ordinal,
        }
    }

    #[test]
    fn payload_lengths_zero_through_more_than_eight_have_exact_keys() {
        for length in 0..=16 {
            let data: Vec<u8> = (0..length as u8).collect();
            let key = SelectorKey::from_instruction(0x0403_0201, &data);
            assert_eq!(usize::from(key.selector_len()), length.min(8));
            assert_eq!(key.selector(), &data[..length.min(8)]);
            assert_eq!(SelectorKey::decode(&key.encode()).unwrap(), key);
        }
        let eight = SelectorKey::from_instruction(7, b"abcdefgh");
        let longer = SelectorKey::from_instruction(7, b"abcdefgh-tail");
        assert_eq!(eight, longer);
        assert_ne!(SelectorKey::from_instruction(7, b"abcdefg"), eight);
    }

    #[test]
    fn key_has_one_frozen_wire_encoding() {
        let key = SelectorKey::new(0x0403_0201, &[0xaa, 0xbb, 0xcc]).unwrap();
        assert_eq!(
            key.encode(),
            [1, 2, 3, 4, 3, 0xaa, 0xbb, 0xcc, 0, 0, 0, 0, 0, 0, 0, 0,]
        );
        let mut corrupt = key.encode();
        corrupt[12] = 1;
        assert_eq!(
            SelectorKey::decode(&corrupt),
            Err(SelectorIndexError::SelectorPaddingSet)
        );
        corrupt = key.encode();
        corrupt[15] = 1;
        assert_eq!(
            SelectorKey::decode(&corrupt),
            Err(SelectorIndexError::KeyReservedBytesSet)
        );
    }

    #[test]
    fn page_round_trips_scopes_repeated_transactions_and_role_gaps() {
        let key_a = SelectorKey::new(7, b"").unwrap();
        let key_b = SelectorKey::new(7, b"anchor00").unwrap();
        let page = vec![
            KeyPostings {
                key: key_a,
                postings: vec![
                    posting(0, InstructionScope::TopLevel, 0),
                    posting(0, InstructionScope::Cpi, 3),
                    posting(9, InstructionScope::TopLevel, 12),
                ],
            },
            KeyPostings {
                key: key_b,
                postings: vec![posting(15, InstructionScope::Cpi, 700)],
            },
        ];
        let encoded = encode_page(&page).unwrap();
        assert_eq!(decode_page(&encoded, 2, 4).unwrap(), page);
        assert_eq!(encode_page(&page).unwrap(), encoded);
    }

    #[test]
    fn directory_and_continuations_are_frozen_and_exact() {
        let key = SelectorKey::new(9, b"12345678").unwrap();
        let first = PageDirectoryEntry {
            first_key: key,
            last_key: key,
            offset: 64,
            stored_len: 20,
            decoded_len: 30,
            key_count: 1,
            posting_count: 2,
            flags: PAGE_FLAG_ZSTD | PAGE_FLAG_CONTINUES_IN_NEXT,
        };
        let second = PageDirectoryEntry {
            offset: 84,
            flags: PAGE_FLAG_ZSTD | PAGE_FLAG_CONTINUED_FROM_PREVIOUS,
            ..first
        };
        let bytes = first.encode();
        assert_eq!(PageDirectoryEntry::decode(&bytes).unwrap(), first);
        validate_directory(&[first, second], 64, 104).unwrap();
        assert_eq!(candidate_page_range(&[first, second], key), 0..2);

        let mut broken = second;
        broken.flags = PAGE_FLAG_ZSTD;
        assert_eq!(
            validate_directory(&[first, broken], 64, 104),
            Err(SelectorIndexError::DirectoryPagesNotAscending)
        );

        let footer = DirectoryFooter {
            directory_offset: 0x1112_1314_1516_1718,
            page_count: 0x2122_2324_2526_2728,
        };
        let mut expected = [0_u8; DIRECTORY_FOOTER_LEN];
        expected[0..8].copy_from_slice(b"BZIASDIR");
        expected[8..16].copy_from_slice(&footer.directory_offset.to_le_bytes());
        expected[16..24].copy_from_slice(&footer.page_count.to_le_bytes());
        assert_eq!(footer.encode(), expected);
        assert_eq!(DirectoryFooter::decode(&expected).unwrap(), footer);
    }

    #[test]
    fn point_lookup_joins_continuations_and_rejects_wrong_order() {
        let key = SelectorKey::new(5, b"abc").unwrap();
        let first = vec![KeyPostings {
            key,
            postings: vec![posting(1, InstructionScope::TopLevel, 2)],
        }];
        let second = vec![KeyPostings {
            key,
            postings: vec![posting(4, InstructionScope::Cpi, 9)],
        }];
        assert_eq!(
            point_lookup(key, [first.clone(), second.clone()]).unwrap(),
            vec![
                posting(1, InstructionScope::TopLevel, 2),
                posting(4, InstructionScope::Cpi, 9),
            ]
        );
        assert!(point_lookup(key, [second, first]).is_err());
    }

    #[test]
    fn canonical_verification_checks_owner_transaction_program_and_bytes() {
        let key = SelectorKey::new(77, b"12345678").unwrap();
        let postings = [
            posting(4, InstructionScope::TopLevel, 10),
            posting(9, InstructionScope::Cpi, 20),
        ];
        verify_canonical_postings(key, &[], |_, _| {
            Ok::<_, Infallible>(CanonicalInstruction {
                transaction_ordinal: 0,
                program_id: 0,
                data: Vec::new(),
            })
        })
        .unwrap();
        verify_canonical_postings(key, &postings, |scope, ordinal| {
            assert_eq!(
                (scope, ordinal),
                if ordinal == 10 {
                    (InstructionScope::TopLevel, 10)
                } else {
                    (InstructionScope::Cpi, 20)
                }
            );
            Ok::<_, Infallible>(CanonicalInstruction {
                transaction_ordinal: if ordinal == 10 { 4 } else { 9 },
                program_id: 77,
                data: b"12345678-and-more".to_vec(),
            })
        })
        .unwrap();

        let mismatch = verify_canonical_postings(key, &postings[..1], |_, _| {
            Ok::<_, Infallible>(CanonicalInstruction {
                transaction_ordinal: 4,
                program_id: 77,
                data: b"wrong".to_vec(),
            })
        });
        assert!(matches!(
            mismatch,
            Err(CanonicalVerificationError::CanonicalMismatch { .. })
        ));
    }

    #[test]
    fn corrupt_counts_varints_flags_and_order_are_rejected() {
        let key = SelectorKey::new(1, b"a").unwrap();
        let repeated = vec![KeyPostings {
            key,
            postings: vec![
                posting(3, InstructionScope::TopLevel, 4),
                posting(3, InstructionScope::TopLevel, 4),
            ],
        }];
        assert!(matches!(
            encode_page(&repeated),
            Err(SelectorIndexError::PostingsNotAscending { .. })
        ));

        let valid = encode_page(&[KeyPostings {
            key,
            postings: vec![posting(3, InstructionScope::TopLevel, 4)],
        }])
        .unwrap();
        assert!(matches!(
            decode_page(&valid, 1, 2),
            Err(SelectorIndexError::PostingCountMismatch { .. })
        ));
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(matches!(
            decode_page(&trailing, 1, 1),
            Err(SelectorIndexError::TrailingBytes { .. })
        ));

        let entry = PageDirectoryEntry {
            first_key: key,
            last_key: key,
            offset: 64,
            stored_len: 1,
            decoded_len: 1,
            key_count: 1,
            posting_count: 1,
            flags: 0x8000,
        };
        assert_eq!(
            PageDirectoryEntry::decode(&entry.encode()),
            Err(SelectorIndexError::UnknownPageFlags(0x8000))
        );
    }
}
