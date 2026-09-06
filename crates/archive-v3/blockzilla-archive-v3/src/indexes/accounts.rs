//! `indexes/accounts.pages`: account-presence transaction postings.
//!
//! The reverse of resolved account IDs in
//! [`crate::ledger::transactions::Message`]: registry ID → the transactions
//! that reference it, with a role mask. The posting itself means
//! that the account is present. A zero mask means read-only, non-signer
//! presence; it must not be changed into a signer role. This is derived data. It
//! holds no pubkey bytes, no signatures, and no instruction payloads — only
//! IDs, ordinals, and flags — and it must be reproducible from the canonical
//! columns alone, which is what `rebuild-indexes --verify` checks.
//!
//! Measured on `epoch-822-biggest.car` (44,152 postings): **0.36 bytes per
//! posting** after zstd-3, or ~7.8% on top of the shared dictionary plus the
//! forward column. Keeping both directions is cheap; scanning the archive to
//! avoid keeping them is not.
//!
//! Each page decodes independently. A directory entry carries its own
//! `first_key`, so a reader can range-read one page over HTTP and start
//! decoding key gaps immediately instead of replaying every earlier page.
//! A hot account can continue through a bounded chain of single-key pages.

use std::ops::Range;

use thiserror::Error;

use crate::varint::{VarintError, read_uleb128, read_uleb128_u32, write_uleb128};

pub const PATH: &str = "indexes/accounts.pages";
pub const SCHEMA: u16 = 2;

pub const ROLE_SIGNER: u8 = 1 << 0;
pub const ROLE_WRITABLE: u8 = 1 << 1;
pub const ROLE_TOP_LEVEL_PROGRAM: u8 = 1 << 2;
pub const ROLE_CPI_PROGRAM: u8 = 1 << 3;
pub const ROLE_MASK: u8 = 0x0f;

/// Bytes of a page-directory entry.
pub const DIRECTORY_ENTRY_LEN: usize = 32;
pub const DIRECTORY_FOOTER_LEN: usize = 24;
pub const DIRECTORY_FOOTER_MAGIC: [u8; 8] = *b"BZIAADIR";

/// A hard allocation guard for one independently decoded posting page.
pub const MAX_PAGE_DECODED_BYTES: u32 = 64 << 20;
/// Maximum complete keys in one schema-2 page.
pub const MAX_KEYS_PER_PAGE: u32 = 4096;
/// Maximum logical postings in one schema-2 page, including all keys.
pub const MAX_POSTINGS_PER_PAGE: u32 = 64 * 1024;

pub const PAGE_FLAG_CONTINUED_FROM_PREVIOUS: u16 = 1 << 0;
pub const PAGE_FLAG_CONTINUES_IN_NEXT: u16 = 1 << 1;
pub const KNOWN_PAGE_FLAGS: u16 = PAGE_FLAG_CONTINUED_FROM_PREVIOUS | PAGE_FLAG_CONTINUES_IN_NEXT;

/// One account's appearance in one transaction.
///
/// Roles are merged per transaction: an account that is both a signer and
/// writable, or that appears in several instructions, produces exactly one
/// posting with the union of its role bits. That keeps transaction ordinals
/// strictly increasing within a key, which is what makes gap encoding valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Posting {
    pub transaction_ordinal: u64,
    /// Optional roles for this presence. Zero is a valid read-only,
    /// non-signer account reference.
    pub roles: u8,
}

/// All postings for one account, in ascending transaction order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPostings {
    pub key: u32,
    pub postings: Vec<Posting>,
}

/// Locates one independently decodable page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageDirectoryEntry {
    pub first_key: u32,
    pub last_key: u32,
    pub offset: u64,
    pub stored_len: u32,
    pub decoded_len: u32,
    pub key_count: u32,
    pub flags: u16,
}

impl PageDirectoryEntry {
    pub fn encode(self) -> [u8; DIRECTORY_ENTRY_LEN] {
        let mut out = [0u8; DIRECTORY_ENTRY_LEN];
        out[0..4].copy_from_slice(&self.first_key.to_le_bytes());
        out[4..8].copy_from_slice(&self.last_key.to_le_bytes());
        out[8..16].copy_from_slice(&self.offset.to_le_bytes());
        out[16..20].copy_from_slice(&self.stored_len.to_le_bytes());
        out[20..24].copy_from_slice(&self.decoded_len.to_le_bytes());
        out[24..28].copy_from_slice(&self.key_count.to_le_bytes());
        out[28..30].copy_from_slice(&self.flags.to_le_bytes());
        // 30..32 is reserved and stays zero.
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, PostingsError> {
        let bytes: [u8; DIRECTORY_ENTRY_LEN] = input
            .get(..DIRECTORY_ENTRY_LEN)
            .and_then(|slice| slice.try_into().ok())
            .ok_or(PostingsError::DirectoryEntryTruncated(input.len()))?;
        if bytes[30..32] != [0, 0] {
            return Err(PostingsError::DirectoryReservedBytes);
        }
        let entry = Self {
            first_key: u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")),
            last_key: u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")),
            offset: u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes")),
            stored_len: u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes")),
            decoded_len: u32::from_le_bytes(bytes[20..24].try_into().expect("4 bytes")),
            key_count: u32::from_le_bytes(bytes[24..28].try_into().expect("4 bytes")),
            flags: u16::from_le_bytes(bytes[28..30].try_into().expect("2 bytes")),
        };
        entry.validate_shape()?;
        Ok(entry)
    }

    fn validate_shape(self) -> Result<(), PostingsError> {
        if self.key_count == 0 {
            return Err(PostingsError::EmptyPage);
        }
        if self.key_count > MAX_KEYS_PER_PAGE {
            return Err(PostingsError::TooManyKeys(self.key_count));
        }
        if self.first_key == 0 {
            return Err(PostingsError::ReservedKey);
        }
        if self.first_key > self.last_key {
            return Err(PostingsError::DirectoryKeyRangeInverted {
                first: self.first_key,
                last: self.last_key,
            });
        }
        let available_keys = u64::from(self.last_key) - u64::from(self.first_key) + 1;
        if u64::from(self.key_count) > available_keys {
            return Err(PostingsError::DirectoryKeyCountExceedsRange {
                count: self.key_count,
                first: self.first_key,
                last: self.last_key,
            });
        }
        if self.key_count == 1 && self.first_key != self.last_key {
            return Err(PostingsError::SingleKeyPageRangeMismatch {
                first: self.first_key,
                last: self.last_key,
            });
        }
        if self.stored_len == 0 || self.decoded_len == 0 {
            return Err(PostingsError::EmptyStoredPage);
        }
        if self.stored_len > self.decoded_len {
            return Err(PostingsError::StoredPageLargerThanDecoded {
                stored: self.stored_len,
                decoded: self.decoded_len,
            });
        }
        if self.decoded_len > MAX_PAGE_DECODED_BYTES {
            return Err(PostingsError::PageAboveDecodeGuard(self.decoded_len));
        }
        if self.flags & !KNOWN_PAGE_FLAGS != 0 {
            return Err(PostingsError::UnknownPageFlags(self.flags));
        }
        if self.flags != 0 && (self.key_count != 1 || self.first_key != self.last_key) {
            return Err(PostingsError::ContinuationPageHasSeveralKeys);
        }
        Ok(())
    }

    pub const fn is_compressed(self) -> bool {
        self.stored_len != self.decoded_len
    }

    /// Whether this page can hold `key`, used to skip a range read entirely.
    pub fn may_contain(self, key: u32) -> bool {
        key >= self.first_key && key <= self.last_key
    }

    pub const fn continued_from_previous(self) -> bool {
        self.flags & PAGE_FLAG_CONTINUED_FROM_PREVIOUS != 0
    }

    pub const fn continues_in_next(self) -> bool {
        self.flags & PAGE_FLAG_CONTINUES_IN_NEXT != 0
    }
}

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

    pub fn decode(input: &[u8]) -> Result<Self, PostingsError> {
        let bytes: &[u8; DIRECTORY_FOOTER_LEN] = input
            .get(..DIRECTORY_FOOTER_LEN)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(PostingsError::DirectoryFooterTruncated(input.len()))?;
        if bytes[0..8] != DIRECTORY_FOOTER_MAGIC {
            return Err(PostingsError::WrongDirectoryMagic);
        }
        Ok(Self {
            directory_offset: u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes")),
            page_count: u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes")),
        })
    }
}

/// Encode one page. `keys` must be strictly ascending and non-empty.
pub fn encode_page(keys: &[KeyPostings]) -> Result<Vec<u8>, PostingsError> {
    if keys.is_empty() {
        return Err(PostingsError::EmptyPage);
    }
    if keys.len() > MAX_KEYS_PER_PAGE as usize {
        return Err(PostingsError::TooManyKeys(
            u32::try_from(keys.len()).unwrap_or(u32::MAX),
        ));
    }
    let mut out = Vec::new();
    let mut previous_key: Option<u32> = None;
    let mut total_postings = 0_usize;
    for entry in keys {
        if entry.key == 0 {
            return Err(PostingsError::ReservedKey);
        }
        let key_gap = match previous_key {
            None => 0,
            Some(previous) => entry
                .key
                .checked_sub(previous)
                .filter(|gap| *gap > 0)
                .ok_or(PostingsError::KeysNotAscending {
                    previous,
                    current: entry.key,
                })?,
        };
        previous_key = Some(entry.key);

        if entry.postings.is_empty() {
            return Err(PostingsError::KeyWithNoPostings(entry.key));
        }
        total_postings = total_postings
            .checked_add(entry.postings.len())
            .ok_or(PostingsError::TooManyPostings(usize::MAX))?;
        if total_postings > MAX_POSTINGS_PER_PAGE as usize {
            return Err(PostingsError::TooManyPostings(total_postings));
        }
        write_uleb128(&mut out, u64::from(key_gap));
        write_uleb128(&mut out, entry.postings.len() as u64);

        let mut previous_ordinal: Option<u64> = None;
        for posting in &entry.postings {
            if posting.roles & !ROLE_MASK != 0 {
                return Err(PostingsError::UnknownRoleBits(posting.roles));
            }
            let gap = match previous_ordinal {
                None => posting.transaction_ordinal,
                Some(previous) => posting
                    .transaction_ordinal
                    .checked_sub(previous)
                    .filter(|gap| *gap > 0)
                    .ok_or(PostingsError::OrdinalsNotAscending {
                        key: entry.key,
                        previous,
                        current: posting.transaction_ordinal,
                    })?,
            };
            previous_ordinal = Some(posting.transaction_ordinal);
            let packed = gap
                .checked_shl(4)
                .filter(|shifted| shifted >> 4 == gap)
                .ok_or(PostingsError::OrdinalGapOverflow(gap))?;
            write_uleb128(&mut out, packed | u64::from(posting.roles));
        }
    }
    if out.len() > MAX_PAGE_DECODED_BYTES as usize {
        return Err(PostingsError::PageAboveDecodeGuard(
            u32::try_from(out.len()).unwrap_or(u32::MAX),
        ));
    }
    Ok(out)
}

/// Decode a page holding exactly `key_count` keys, starting at `first_key`.
pub fn decode_page(
    payload: &[u8],
    first_key: u32,
    key_count: u32,
) -> Result<Vec<KeyPostings>, PostingsError> {
    if payload.len() > MAX_PAGE_DECODED_BYTES as usize {
        return Err(PostingsError::PageAboveDecodeGuard(
            u32::try_from(payload.len()).unwrap_or(u32::MAX),
        ));
    }
    if key_count == 0 {
        return Err(PostingsError::EmptyPage);
    }
    if key_count > MAX_KEYS_PER_PAGE {
        return Err(PostingsError::TooManyKeys(key_count));
    }
    if first_key == 0 {
        return Err(PostingsError::ReservedKey);
    }
    let capacity = usize::try_from(key_count).map_err(|_| PostingsError::KeyOverflow)?;
    if capacity > payload.len() {
        return Err(PostingsError::KeyCountExceedsPage {
            count: key_count,
            remaining: payload.len(),
        });
    }
    let mut keys = Vec::with_capacity(capacity);
    let mut cursor = 0_usize;
    let mut key = first_key;
    let mut total_postings = 0_usize;
    for index in 0..key_count {
        let key_gap = read_uleb128_u32(payload, &mut cursor)?;
        if index == 0 {
            if key_gap != 0 {
                return Err(PostingsError::FirstKeyGapNotZero(key_gap));
            }
        } else {
            if key_gap == 0 {
                return Err(PostingsError::KeysNotAscending {
                    previous: key,
                    current: key,
                });
            }
            key = key.checked_add(key_gap).ok_or(PostingsError::KeyOverflow)?;
        }

        let count = read_uleb128(payload, &mut cursor)?;
        if count == 0 {
            return Err(PostingsError::KeyWithNoPostings(key));
        }
        let count = usize::try_from(count).map_err(|_| PostingsError::KeyOverflow)?;
        total_postings = total_postings
            .checked_add(count)
            .ok_or(PostingsError::TooManyPostings(usize::MAX))?;
        if total_postings > MAX_POSTINGS_PER_PAGE as usize {
            return Err(PostingsError::TooManyPostings(total_postings));
        }
        // A page cannot describe more postings than it has bytes left to hold,
        // so this bound cannot be inflated by a corrupt count.
        if count > payload.len() - cursor {
            return Err(PostingsError::PostingCountExceedsPage {
                key,
                count,
                remaining: payload.len() - cursor,
            });
        }

        let mut postings = Vec::with_capacity(count);
        let mut ordinal = 0_u64;
        for position in 0..count {
            let packed = read_uleb128(payload, &mut cursor)?;
            let roles = (packed & u64::from(ROLE_MASK)) as u8;
            let gap = packed >> 4;
            if position == 0 {
                ordinal = gap;
            } else {
                if gap == 0 {
                    return Err(PostingsError::OrdinalsNotAscending {
                        key,
                        previous: ordinal,
                        current: ordinal,
                    });
                }
                ordinal = ordinal.checked_add(gap).ok_or(PostingsError::KeyOverflow)?;
            }
            postings.push(Posting {
                transaction_ordinal: ordinal,
                roles,
            });
        }
        keys.push(KeyPostings { key, postings });
    }
    if cursor != payload.len() {
        return Err(PostingsError::TrailingBytes {
            consumed: cursor,
            total: payload.len(),
        });
    }
    Ok(keys)
}

/// Look one key up in a decoded page.
pub fn find_key(page: &[KeyPostings], key: u32) -> Option<&KeyPostings> {
    page.binary_search_by_key(&key, |entry| entry.key)
        .ok()
        .map(|index| &page[index])
}

/// Validate page extents, key order, and every continuation link.
pub fn validate_directory(
    entries: &[PageDirectoryEntry],
    pages_offset: u64,
    directory_offset: u64,
) -> Result<(), PostingsError> {
    let mut next_offset = pages_offset;
    let mut previous: Option<PageDirectoryEntry> = None;
    for (index, entry) in entries.iter().copied().enumerate() {
        entry.validate_shape()?;
        if entry.offset != next_offset {
            return Err(PostingsError::PageOffsetsNotContiguous {
                index,
                expected: next_offset,
                actual: entry.offset,
            });
        }
        next_offset = entry
            .offset
            .checked_add(u64::from(entry.stored_len))
            .ok_or(PostingsError::PageRangeOverflow)?;

        match previous {
            None => {
                if entry.continued_from_previous() {
                    return Err(PostingsError::ContinuationChainBroken(index));
                }
            }
            Some(previous_entry) => {
                let continues = previous_entry.continues_in_next();
                let continued = entry.continued_from_previous();
                if continues != continued {
                    return Err(PostingsError::ContinuationChainBroken(index));
                }
                if continues {
                    if previous_entry.first_key != previous_entry.last_key
                        || entry.first_key != entry.last_key
                        || previous_entry.first_key != entry.first_key
                    {
                        return Err(PostingsError::ContinuationKeyChanged(index));
                    }
                } else if entry.first_key <= previous_entry.last_key {
                    return Err(PostingsError::DirectoryKeysNotAscending {
                        previous: previous_entry.last_key,
                        current: entry.first_key,
                    });
                }
            }
        }
        previous = Some(entry);
    }
    if previous.is_some_and(PageDirectoryEntry::continues_in_next) {
        return Err(PostingsError::ContinuationChainBroken(entries.len()));
    }
    if next_offset != directory_offset {
        return Err(PostingsError::PagesDoNotEndAtDirectory {
            actual: next_offset,
            expected: directory_offset,
        });
    }
    Ok(())
}

/// Return all consecutive pages that can contain `key`.
pub fn pages_for_key(entries: &[PageDirectoryEntry], key: u32) -> Range<usize> {
    let mut low = 0_usize;
    let mut high = entries.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if entries[middle].last_key < key {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let start = low;
    while low < entries.len() && entries[low].may_contain(key) {
        low += 1;
    }
    start..low
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PostingsError {
    #[error("posting stream: {0}")]
    Varint(#[from] VarintError),
    #[error("a posting page must hold at least one key")]
    EmptyPage,
    #[error("account page declares {0} keys, above the schema limit")]
    TooManyKeys(u32),
    #[error("account page declares {0} postings, above the schema limit")]
    TooManyPostings(usize),
    #[error("account dictionary key zero is reserved")]
    ReservedKey,
    #[error("directory entry needs {DIRECTORY_ENTRY_LEN} bytes, found {0}")]
    DirectoryEntryTruncated(usize),
    #[error("directory footer needs {DIRECTORY_FOOTER_LEN} bytes, found {0}")]
    DirectoryFooterTruncated(usize),
    #[error("account directory has the wrong magic")]
    WrongDirectoryMagic,
    #[error("account directory entry has non-zero reserved bytes")]
    DirectoryReservedBytes,
    #[error("directory entry key range {first}..={last} is inverted")]
    DirectoryKeyRangeInverted { first: u32, last: u32 },
    #[error("directory declares {count} keys in range {first}..={last}")]
    DirectoryKeyCountExceedsRange { count: u32, first: u32, last: u32 },
    #[error("one-key account page has range {first}..={last}")]
    SingleKeyPageRangeMismatch { first: u32, last: u32 },
    #[error("account directory key ranges do not ascend: {previous} then {current}")]
    DirectoryKeysNotAscending { previous: u32, current: u32 },
    #[error("an account page has no stored or decoded bytes")]
    EmptyStoredPage,
    #[error("account page stores {stored} bytes for {decoded} decoded bytes")]
    StoredPageLargerThanDecoded { stored: u32, decoded: u32 },
    #[error("account page declares {0} decoded bytes, above the decode guard")]
    PageAboveDecodeGuard(u32),
    #[error("account page has unknown flags {0:#x}")]
    UnknownPageFlags(u16),
    #[error("an account continuation page must contain exactly one key")]
    ContinuationPageHasSeveralKeys,
    #[error("keys must strictly ascend: {previous} then {current}")]
    KeysNotAscending { previous: u32, current: u32 },
    #[error("the first key in a page must have gap zero, found {0}")]
    FirstKeyGapNotZero(u32),
    #[error("key {0} has no postings")]
    KeyWithNoPostings(u32),
    #[error("key overflows u32 while decoding gaps")]
    KeyOverflow,
    #[error("account key count {count} exceeds the {remaining}-byte page")]
    KeyCountExceedsPage { count: u32, remaining: usize },
    #[error("key {key} declares {count} postings but the page has {remaining} bytes left")]
    PostingCountExceedsPage {
        key: u32,
        count: usize,
        remaining: usize,
    },
    #[error("transaction ordinals for key {key} must strictly ascend: {previous} then {current}")]
    OrdinalsNotAscending {
        key: u32,
        previous: u64,
        current: u64,
    },
    #[error("transaction ordinal gap {0} does not fit alongside the role nibble")]
    OrdinalGapOverflow(u64),
    #[error("posting carries unknown role bits: {0:#06b}")]
    UnknownRoleBits(u8),
    #[error("page has {consumed} of {total} bytes consumed after the last key")]
    TrailingBytes { consumed: usize, total: usize },
    #[error("account page {index} starts at {actual}, expected {expected}")]
    PageOffsetsNotContiguous {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("account page range overflows u64")]
    PageRangeOverflow,
    #[error("account continuation chain is broken at page {0}")]
    ContinuationChainBroken(usize),
    #[error("account continuation changes key at page {0}")]
    ContinuationKeyChanged(usize),
    #[error("account pages end at {actual}, directory starts at {expected}")]
    PagesDoNotEndAtDirectory { actual: u64, expected: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(transaction_ordinal: u64, roles: u8) -> Posting {
        Posting {
            transaction_ordinal,
            roles,
        }
    }

    fn sample() -> Vec<KeyPostings> {
        vec![
            KeyPostings {
                key: 7,
                postings: vec![
                    posting(12, ROLE_SIGNER | ROLE_WRITABLE),
                    posting(47, ROLE_WRITABLE),
                    posting(1_000_003, ROLE_TOP_LEVEL_PROGRAM),
                ],
            },
            KeyPostings {
                key: 8,
                postings: vec![posting(0, ROLE_CPI_PROGRAM)],
            },
            KeyPostings {
                key: 900_001,
                postings: vec![posting(5, ROLE_SIGNER)],
            },
        ]
    }

    #[test]
    fn page_round_trips() {
        let keys = sample();
        let page = encode_page(&keys).unwrap();
        assert_eq!(
            decode_page(&page, keys[0].key, keys.len() as u32).unwrap(),
            keys
        );
    }

    #[test]
    fn encoding_is_deterministic() {
        let keys = sample();
        assert_eq!(encode_page(&keys).unwrap(), encode_page(&keys).unwrap());
    }

    #[test]
    fn a_page_decodes_without_reading_earlier_pages() {
        // The directory supplies first_key, so a mid-file page is self-contained.
        let keys = vec![KeyPostings {
            key: 4_000_000,
            postings: vec![posting(9, ROLE_WRITABLE)],
        }];
        let page = encode_page(&keys).unwrap();
        assert_eq!(decode_page(&page, 4_000_000, 1).unwrap(), keys);
    }

    #[test]
    fn roles_survive_the_nibble_packing() {
        let keys = vec![KeyPostings {
            key: 1,
            postings: vec![posting(
                u64::MAX >> 4,
                ROLE_SIGNER | ROLE_WRITABLE | ROLE_TOP_LEVEL_PROGRAM | ROLE_CPI_PROGRAM,
            )],
        }];
        let page = encode_page(&keys).unwrap();
        let decoded = decode_page(&page, 1, 1).unwrap();
        assert_eq!(decoded[0].postings[0].roles, ROLE_MASK);
        assert_eq!(decoded[0].postings[0].transaction_ordinal, u64::MAX >> 4);
    }

    #[test]
    fn an_ordinal_gap_that_would_lose_bits_is_rejected() {
        let keys = vec![KeyPostings {
            key: 1,
            postings: vec![posting(u64::MAX, ROLE_SIGNER)],
        }];
        assert!(matches!(
            encode_page(&keys),
            Err(PostingsError::OrdinalGapOverflow(_))
        ));
    }

    #[test]
    fn keys_and_ordinals_must_strictly_ascend() {
        let repeated_key = vec![
            KeyPostings {
                key: 5,
                postings: vec![posting(1, ROLE_SIGNER)],
            },
            KeyPostings {
                key: 5,
                postings: vec![posting(2, ROLE_SIGNER)],
            },
        ];
        assert_eq!(
            encode_page(&repeated_key),
            Err(PostingsError::KeysNotAscending {
                previous: 5,
                current: 5,
            })
        );

        let repeated_ordinal = vec![KeyPostings {
            key: 5,
            postings: vec![posting(3, ROLE_SIGNER), posting(3, ROLE_WRITABLE)],
        }];
        assert_eq!(
            encode_page(&repeated_ordinal),
            Err(PostingsError::OrdinalsNotAscending {
                key: 5,
                previous: 3,
                current: 3,
            })
        );
    }

    #[test]
    fn a_role_free_posting_is_readonly_presence() {
        let keys = vec![KeyPostings {
            key: 5,
            postings: vec![posting(0, 0)],
        }];
        let page = encode_page(&keys).unwrap();
        assert_eq!(page, vec![0, 1, 0]);
        assert_eq!(decode_page(&page, 5, 1).unwrap(), keys);
    }

    #[test]
    fn unknown_role_bits_are_rejected() {
        let keys = vec![KeyPostings {
            key: 5,
            postings: vec![posting(1, 0x10)],
        }];
        assert_eq!(
            encode_page(&keys),
            Err(PostingsError::UnknownRoleBits(0x10))
        );
    }

    #[test]
    fn a_corrupt_posting_count_cannot_force_a_large_allocation() {
        // count = u64::MAX in a two-byte page.
        let mut payload = Vec::new();
        write_uleb128(&mut payload, 0);
        write_uleb128(&mut payload, u64::MAX);
        assert!(matches!(
            decode_page(&payload, 1, 1),
            Err(PostingsError::KeyOverflow
                | PostingsError::TooManyPostings(_)
                | PostingsError::PostingCountExceedsPage { .. })
        ));
    }

    #[test]
    fn schema_count_limits_apply_before_page_allocations() {
        assert_eq!(
            decode_page(&[], 1, MAX_KEYS_PER_PAGE + 1),
            Err(PostingsError::TooManyKeys(MAX_KEYS_PER_PAGE + 1))
        );

        let mut payload = Vec::new();
        write_uleb128(&mut payload, 0);
        write_uleb128(&mut payload, u64::from(MAX_POSTINGS_PER_PAGE) + 1);
        assert_eq!(
            decode_page(&payload, 1, 1),
            Err(PostingsError::TooManyPostings(
                MAX_POSTINGS_PER_PAGE as usize + 1
            ))
        );
    }

    #[test]
    fn a_wrong_key_count_is_detected() {
        let keys = sample();
        let page = encode_page(&keys).unwrap();
        assert!(matches!(
            decode_page(&page, keys[0].key, 2),
            Err(PostingsError::TrailingBytes { .. })
        ));
        assert!(decode_page(&page, keys[0].key, 4).is_err());
    }

    #[test]
    fn directory_entries_round_trip_and_bound_lookups() {
        let entry = PageDirectoryEntry {
            first_key: 7,
            last_key: 900_001,
            offset: 1 << 40,
            stored_len: 4096,
            decoded_len: 8192,
            key_count: 3,
            flags: 0,
        };
        assert_eq!(PageDirectoryEntry::decode(&entry.encode()).unwrap(), entry);
        assert!(entry.may_contain(7));
        assert!(entry.may_contain(900_001));
        assert!(!entry.may_contain(6));
        assert!(!entry.may_contain(900_002));
    }

    #[test]
    fn an_inverted_directory_range_is_rejected() {
        let mut bytes = PageDirectoryEntry {
            first_key: 10,
            last_key: 900,
            offset: 0,
            stored_len: 1,
            decoded_len: 1,
            key_count: 2,
            flags: 0,
        }
        .encode();
        bytes[4..8].copy_from_slice(&9_u32.to_le_bytes());
        assert_eq!(
            PageDirectoryEntry::decode(&bytes),
            Err(PostingsError::DirectoryKeyRangeInverted { first: 10, last: 9 })
        );
    }

    #[test]
    fn lookup_finds_and_misses_correctly() {
        let keys = sample();
        assert_eq!(find_key(&keys, 8).unwrap().postings.len(), 1);
        assert!(find_key(&keys, 9).is_none());
    }

    #[test]
    fn continuation_directory_is_valid_and_searchable() {
        let entries = vec![
            PageDirectoryEntry {
                first_key: 1,
                last_key: 7,
                offset: 64,
                stored_len: 10,
                decoded_len: 20,
                key_count: 3,
                flags: 0,
            },
            PageDirectoryEntry {
                first_key: 9,
                last_key: 9,
                offset: 74,
                stored_len: 5,
                decoded_len: 10,
                key_count: 1,
                flags: PAGE_FLAG_CONTINUES_IN_NEXT,
            },
            PageDirectoryEntry {
                first_key: 9,
                last_key: 9,
                offset: 79,
                stored_len: 5,
                decoded_len: 10,
                key_count: 1,
                flags: PAGE_FLAG_CONTINUED_FROM_PREVIOUS,
            },
        ];
        validate_directory(&entries, 64, 84).unwrap();
        assert_eq!(pages_for_key(&entries, 3), 0..1);
        assert_eq!(pages_for_key(&entries, 8), 1..1);
        assert_eq!(pages_for_key(&entries, 9), 1..3);
        assert_eq!(pages_for_key(&entries, 10), 3..3);
    }

    #[test]
    fn corrupt_continuation_and_reserved_bytes_are_rejected() {
        let first = PageDirectoryEntry {
            first_key: 7,
            last_key: 7,
            offset: 64,
            stored_len: 5,
            decoded_len: 8,
            key_count: 1,
            flags: PAGE_FLAG_CONTINUES_IN_NEXT,
        };
        let wrong_key = PageDirectoryEntry {
            first_key: 8,
            last_key: 8,
            offset: 69,
            stored_len: 5,
            decoded_len: 8,
            key_count: 1,
            flags: PAGE_FLAG_CONTINUED_FROM_PREVIOUS,
        };
        assert_eq!(
            validate_directory(&[first, wrong_key], 64, 74),
            Err(PostingsError::ContinuationKeyChanged(1))
        );

        let missing_back_link = PageDirectoryEntry {
            first_key: 7,
            last_key: 7,
            offset: 69,
            stored_len: 5,
            decoded_len: 8,
            key_count: 1,
            flags: 0,
        };
        assert_eq!(
            validate_directory(&[first, missing_back_link], 64, 74),
            Err(PostingsError::ContinuationChainBroken(1))
        );

        let mut encoded = first.encode();
        encoded[31] = 1;
        assert_eq!(
            PageDirectoryEntry::decode(&encoded),
            Err(PostingsError::DirectoryReservedBytes)
        );
    }

    #[test]
    fn footer_has_frozen_bytes() {
        let footer = DirectoryFooter {
            directory_offset: 0x0102_0304_0506_0708,
            page_count: 0x1112_1314_1516_1718,
        };
        let bytes = footer.encode();
        assert_eq!(&bytes[0..8], b"BZIAADIR");
        assert_eq!(DirectoryFooter::decode(&bytes).unwrap(), footer);
    }
}
