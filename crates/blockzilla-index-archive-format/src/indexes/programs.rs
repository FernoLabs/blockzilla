//! `indexes/programs.pages`: program to transaction postings.
//!
//! The key is the 1-based ID in `dictionary/pubkeys.pages`. Each posting names
//! one transaction and says whether the program was invoked by a signed,
//! top-level instruction, by CPI, or by both. Repeated invocations in one
//! transaction are merged, so transaction ordinals strictly ascend per key.
//!
//! Pages end on key boundaries. A key that is too hot for one bounded page can
//! use a chain of single-key continuation pages. The directory carries that
//! chain explicitly, so a point lookup reads all and only the pages for the
//! requested program.

use std::ops::Range;

use thiserror::Error;

use crate::varint::{VarintError, read_uleb128, read_uleb128_u32, write_uleb128};

pub const PATH: &str = "indexes/programs.pages";
pub const SCHEMA: u16 = 1;

pub const ROLE_TOP_LEVEL: u8 = 1 << 0;
pub const ROLE_CPI: u8 = 1 << 1;
pub const ROLE_MASK: u8 = ROLE_TOP_LEVEL | ROLE_CPI;

pub const DIRECTORY_ENTRY_LEN: usize = 32;
pub const DIRECTORY_FOOTER_LEN: usize = 24;
pub const DIRECTORY_FOOTER_MAGIC: [u8; 8] = *b"BZIAPDIR";

/// A hard allocation guard for an independently decoded posting page.
pub const MAX_PAGE_DECODED_BYTES: u32 = 64 << 20;

pub const PAGE_FLAG_CONTINUED_FROM_PREVIOUS: u16 = 1 << 0;
pub const PAGE_FLAG_CONTINUES_IN_NEXT: u16 = 1 << 1;
pub const KNOWN_PAGE_FLAGS: u16 = PAGE_FLAG_CONTINUED_FROM_PREVIOUS | PAGE_FLAG_CONTINUES_IN_NEXT;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Posting {
    pub transaction_ordinal: u64,
    pub roles: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPostings {
    pub key: u32,
    pub postings: Vec<Posting>,
}

/// One independently stored posting page.
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
        let mut out = [0_u8; DIRECTORY_ENTRY_LEN];
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

    pub fn decode(input: &[u8]) -> Result<Self, ProgramPostingsError> {
        let bytes: &[u8; DIRECTORY_ENTRY_LEN] = input
            .get(..DIRECTORY_ENTRY_LEN)
            .and_then(|slice| slice.try_into().ok())
            .ok_or(ProgramPostingsError::DirectoryEntryTruncated(input.len()))?;
        if bytes[30..32] != [0, 0] {
            return Err(ProgramPostingsError::DirectoryReservedBytes);
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

    fn validate_shape(self) -> Result<(), ProgramPostingsError> {
        if self.key_count == 0 {
            return Err(ProgramPostingsError::EmptyPage);
        }
        if self.first_key == 0 {
            return Err(ProgramPostingsError::ReservedKey);
        }
        if self.first_key > self.last_key {
            return Err(ProgramPostingsError::DirectoryKeyRangeInverted {
                first: self.first_key,
                last: self.last_key,
            });
        }
        let available_keys = u64::from(self.last_key) - u64::from(self.first_key) + 1;
        if u64::from(self.key_count) > available_keys {
            return Err(ProgramPostingsError::DirectoryKeyCountExceedsRange {
                count: self.key_count,
                first: self.first_key,
                last: self.last_key,
            });
        }
        if self.key_count == 1 && self.first_key != self.last_key {
            return Err(ProgramPostingsError::SingleKeyPageRangeMismatch {
                first: self.first_key,
                last: self.last_key,
            });
        }
        if self.stored_len == 0 || self.decoded_len == 0 {
            return Err(ProgramPostingsError::EmptyStoredPage);
        }
        if self.stored_len > self.decoded_len {
            return Err(ProgramPostingsError::StoredPageLargerThanDecoded {
                stored: self.stored_len,
                decoded: self.decoded_len,
            });
        }
        if self.decoded_len > MAX_PAGE_DECODED_BYTES {
            return Err(ProgramPostingsError::PageAboveDecodeGuard(self.decoded_len));
        }
        if self.flags & !KNOWN_PAGE_FLAGS != 0 {
            return Err(ProgramPostingsError::UnknownPageFlags(self.flags));
        }
        if self.flags != 0 && (self.key_count != 1 || self.first_key != self.last_key) {
            return Err(ProgramPostingsError::ContinuationPageHasSeveralKeys);
        }
        Ok(())
    }

    pub const fn is_compressed(self) -> bool {
        self.stored_len != self.decoded_len
    }

    pub const fn may_contain(self, key: u32) -> bool {
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

    pub fn decode(input: &[u8]) -> Result<Self, ProgramPostingsError> {
        let bytes: &[u8; DIRECTORY_FOOTER_LEN] = input
            .get(..DIRECTORY_FOOTER_LEN)
            .and_then(|slice| slice.try_into().ok())
            .ok_or(ProgramPostingsError::DirectoryFooterTruncated(input.len()))?;
        if bytes[0..8] != DIRECTORY_FOOTER_MAGIC {
            return Err(ProgramPostingsError::WrongDirectoryMagic);
        }
        Ok(Self {
            directory_offset: u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes")),
            page_count: u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes")),
        })
    }
}

/// Encode one independently decodable page.
pub fn encode_page(keys: &[KeyPostings]) -> Result<Vec<u8>, ProgramPostingsError> {
    if keys.is_empty() {
        return Err(ProgramPostingsError::EmptyPage);
    }
    let mut out = Vec::new();
    let mut previous_key = None;
    for entry in keys {
        if entry.key == 0 {
            return Err(ProgramPostingsError::ReservedKey);
        }
        let gap = match previous_key {
            None => 0,
            Some(previous) => entry
                .key
                .checked_sub(previous)
                .filter(|gap| *gap > 0)
                .ok_or(ProgramPostingsError::KeysNotAscending {
                    previous,
                    current: entry.key,
                })?,
        };
        previous_key = Some(entry.key);
        if entry.postings.is_empty() {
            return Err(ProgramPostingsError::KeyWithNoPostings(entry.key));
        }
        write_uleb128(&mut out, u64::from(gap));
        write_uleb128(&mut out, entry.postings.len() as u64);

        let mut previous_ordinal = None;
        for posting in &entry.postings {
            validate_roles(posting.roles)?;
            let ordinal_gap = match previous_ordinal {
                None => posting.transaction_ordinal,
                Some(previous) => posting
                    .transaction_ordinal
                    .checked_sub(previous)
                    .filter(|gap| *gap > 0)
                    .ok_or(ProgramPostingsError::OrdinalsNotAscending {
                        key: entry.key,
                        previous,
                        current: posting.transaction_ordinal,
                    })?,
            };
            previous_ordinal = Some(posting.transaction_ordinal);
            let packed = ordinal_gap
                .checked_shl(2)
                .filter(|shifted| shifted >> 2 == ordinal_gap)
                .ok_or(ProgramPostingsError::OrdinalGapOverflow(ordinal_gap))?
                | u64::from(posting.roles);
            write_uleb128(&mut out, packed);
        }
    }
    if out.len() > MAX_PAGE_DECODED_BYTES as usize {
        return Err(ProgramPostingsError::PageAboveDecodeGuard(
            u32::try_from(out.len()).unwrap_or(u32::MAX),
        ));
    }
    Ok(out)
}

/// Decode a page whose first key and key count came from its directory entry.
pub fn decode_page(
    payload: &[u8],
    first_key: u32,
    key_count: u32,
) -> Result<Vec<KeyPostings>, ProgramPostingsError> {
    if payload.len() > MAX_PAGE_DECODED_BYTES as usize {
        return Err(ProgramPostingsError::PageAboveDecodeGuard(
            u32::try_from(payload.len()).unwrap_or(u32::MAX),
        ));
    }
    if key_count == 0 {
        return Err(ProgramPostingsError::EmptyPage);
    }
    if first_key == 0 {
        return Err(ProgramPostingsError::ReservedKey);
    }
    let capacity = usize::try_from(key_count).map_err(|_| ProgramPostingsError::KeyOverflow)?;
    if capacity > payload.len() {
        return Err(ProgramPostingsError::KeyCountExceedsPage {
            count: key_count,
            remaining: payload.len(),
        });
    }
    let mut keys = Vec::with_capacity(capacity);
    let mut cursor = 0_usize;
    let mut key = first_key;
    for index in 0..key_count {
        let gap = read_uleb128_u32(payload, &mut cursor)?;
        if index == 0 {
            if gap != 0 {
                return Err(ProgramPostingsError::FirstKeyGapNotZero(gap));
            }
        } else {
            if gap == 0 {
                return Err(ProgramPostingsError::KeysNotAscending {
                    previous: key,
                    current: key,
                });
            }
            key = key
                .checked_add(gap)
                .ok_or(ProgramPostingsError::KeyOverflow)?;
        }
        let count = read_uleb128(payload, &mut cursor)?;
        if count == 0 {
            return Err(ProgramPostingsError::KeyWithNoPostings(key));
        }
        let count = usize::try_from(count).map_err(|_| ProgramPostingsError::KeyOverflow)?;
        if count > payload.len() - cursor {
            return Err(ProgramPostingsError::PostingCountExceedsPage {
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
            validate_roles(roles)?;
            let gap = packed >> 2;
            if position == 0 {
                ordinal = gap;
            } else {
                if gap == 0 {
                    return Err(ProgramPostingsError::OrdinalsNotAscending {
                        key,
                        previous: ordinal,
                        current: ordinal,
                    });
                }
                ordinal = ordinal
                    .checked_add(gap)
                    .ok_or(ProgramPostingsError::OrdinalOverflow)?;
            }
            postings.push(Posting {
                transaction_ordinal: ordinal,
                roles,
            });
        }
        keys.push(KeyPostings { key, postings });
    }
    if cursor != payload.len() {
        return Err(ProgramPostingsError::TrailingBytes {
            consumed: cursor,
            total: payload.len(),
        });
    }
    Ok(keys)
}

fn validate_roles(roles: u8) -> Result<(), ProgramPostingsError> {
    if roles == 0 {
        return Err(ProgramPostingsError::EmptyRoles);
    }
    if roles & !ROLE_MASK != 0 {
        return Err(ProgramPostingsError::UnknownRoleBits(roles));
    }
    Ok(())
}

/// Validate directory order, page extents, and continuation chains.
pub fn validate_directory(
    entries: &[PageDirectoryEntry],
    pages_offset: u64,
    directory_offset: u64,
) -> Result<(), ProgramPostingsError> {
    let mut next_offset = pages_offset;
    let mut previous: Option<PageDirectoryEntry> = None;
    for (index, entry) in entries.iter().copied().enumerate() {
        entry.validate_shape()?;
        if entry.offset != next_offset {
            return Err(ProgramPostingsError::PageOffsetsNotContiguous {
                index,
                expected: next_offset,
                actual: entry.offset,
            });
        }
        next_offset = entry
            .offset
            .checked_add(u64::from(entry.stored_len))
            .ok_or(ProgramPostingsError::PageRangeOverflow)?;

        match previous {
            None => {
                if entry.continued_from_previous() {
                    return Err(ProgramPostingsError::ContinuationChainBroken(index));
                }
            }
            Some(previous_entry) => {
                let continues = previous_entry.continues_in_next();
                let continued = entry.continued_from_previous();
                if continues != continued {
                    return Err(ProgramPostingsError::ContinuationChainBroken(index));
                }
                if continues {
                    if previous_entry.first_key != previous_entry.last_key
                        || entry.first_key != entry.last_key
                        || previous_entry.first_key != entry.first_key
                    {
                        return Err(ProgramPostingsError::ContinuationKeyChanged(index));
                    }
                } else if entry.first_key <= previous_entry.last_key {
                    return Err(ProgramPostingsError::DirectoryKeysNotAscending {
                        previous: previous_entry.last_key,
                        current: entry.first_key,
                    });
                }
            }
        }
        previous = Some(entry);
    }
    if previous.is_some_and(PageDirectoryEntry::continues_in_next) {
        return Err(ProgramPostingsError::ContinuationChainBroken(entries.len()));
    }
    if next_offset != directory_offset {
        return Err(ProgramPostingsError::PagesDoNotEndAtDirectory {
            actual: next_offset,
            expected: directory_offset,
        });
    }
    Ok(())
}

/// Return the consecutive directory entries that can contain `key`.
///
/// A normal key has zero or one entry. A hot key returns its full continuation
/// chain. Callers still decode the page because a multi-key page can contain
/// gaps inside its range.
pub fn pages_for_key(entries: &[PageDirectoryEntry], key: u32) -> Range<usize> {
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
    while lo < entries.len() && entries[lo].may_contain(key) {
        lo += 1;
    }
    start..lo
}

pub fn find_key(page: &[KeyPostings], key: u32) -> Option<&KeyPostings> {
    page.binary_search_by_key(&key, |entry| entry.key)
        .ok()
        .map(|index| &page[index])
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProgramPostingsError {
    #[error("program posting stream: {0}")]
    Varint(#[from] VarintError),
    #[error("a program posting page must hold at least one key")]
    EmptyPage,
    #[error("program dictionary key zero is reserved")]
    ReservedKey,
    #[error("program key {0} has no postings")]
    KeyWithNoPostings(u32),
    #[error("a program posting must have at least one invocation role")]
    EmptyRoles,
    #[error("program posting has unknown role bits {0:#010b}")]
    UnknownRoleBits(u8),
    #[error("program keys must strictly ascend: {previous} then {current}")]
    KeysNotAscending { previous: u32, current: u32 },
    #[error("the first key in a page must have gap zero, found {0}")]
    FirstKeyGapNotZero(u32),
    #[error("program key overflows u32")]
    KeyOverflow,
    #[error("program posting transaction ordinal overflows u64")]
    OrdinalOverflow,
    #[error(
        "transaction ordinals for program {key} must strictly ascend: {previous} then {current}"
    )]
    OrdinalsNotAscending {
        key: u32,
        previous: u64,
        current: u64,
    },
    #[error("transaction ordinal gap {0} does not fit beside the role bits")]
    OrdinalGapOverflow(u64),
    #[error("program key count {count} exceeds the {remaining}-byte page")]
    KeyCountExceedsPage { count: u32, remaining: usize },
    #[error("program {key} declares {count} postings but the page has {remaining} bytes left")]
    PostingCountExceedsPage {
        key: u32,
        count: usize,
        remaining: usize,
    },
    #[error("program page has {consumed} of {total} bytes consumed")]
    TrailingBytes { consumed: usize, total: usize },
    #[error("program directory entry needs {DIRECTORY_ENTRY_LEN} bytes, found {0}")]
    DirectoryEntryTruncated(usize),
    #[error("program directory footer needs {DIRECTORY_FOOTER_LEN} bytes, found {0}")]
    DirectoryFooterTruncated(usize),
    #[error("program directory has the wrong magic")]
    WrongDirectoryMagic,
    #[error("program directory entry has non-zero reserved bytes")]
    DirectoryReservedBytes,
    #[error("program directory key range {first}..={last} is inverted")]
    DirectoryKeyRangeInverted { first: u32, last: u32 },
    #[error("program directory declares {count} keys in range {first}..={last}")]
    DirectoryKeyCountExceedsRange { count: u32, first: u32, last: u32 },
    #[error("one-key program page has range {first}..={last}")]
    SingleKeyPageRangeMismatch { first: u32, last: u32 },
    #[error("program directory key ranges do not ascend: {previous} then {current}")]
    DirectoryKeysNotAscending { previous: u32, current: u32 },
    #[error("a program page has no stored or decoded bytes")]
    EmptyStoredPage,
    #[error("program page stores {stored} bytes for {decoded} decoded bytes")]
    StoredPageLargerThanDecoded { stored: u32, decoded: u32 },
    #[error("program page declares {0} decoded bytes, above the decode guard")]
    PageAboveDecodeGuard(u32),
    #[error("program page has unknown flags {0:#x}")]
    UnknownPageFlags(u16),
    #[error("a continuation page must contain exactly one key")]
    ContinuationPageHasSeveralKeys,
    #[error("program page {index} starts at {actual}, expected {expected}")]
    PageOffsetsNotContiguous {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("program page range overflows u64")]
    PageRangeOverflow,
    #[error("program continuation chain is broken at page {0}")]
    ContinuationChainBroken(usize),
    #[error("program continuation changes key at page {0}")]
    ContinuationKeyChanged(usize),
    #[error("program pages end at {actual}, directory starts at {expected}")]
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

    #[test]
    fn page_round_trips_both_roles() {
        let keys = vec![
            KeyPostings {
                key: 1,
                postings: vec![
                    posting(0, ROLE_TOP_LEVEL),
                    posting(9, ROLE_TOP_LEVEL | ROLE_CPI),
                ],
            },
            KeyPostings {
                key: 400,
                postings: vec![posting(7, ROLE_CPI)],
            },
        ];
        let encoded = encode_page(&keys).unwrap();
        assert_eq!(decode_page(&encoded, 1, 2).unwrap(), keys);
        assert_eq!(encode_page(&keys).unwrap(), encoded);
    }

    #[test]
    fn invalid_roles_and_ordinals_are_rejected() {
        let empty_role = vec![KeyPostings {
            key: 1,
            postings: vec![posting(0, 0)],
        }];
        assert_eq!(
            encode_page(&empty_role),
            Err(ProgramPostingsError::EmptyRoles)
        );

        let duplicate = vec![KeyPostings {
            key: 1,
            postings: vec![posting(5, ROLE_CPI), posting(5, ROLE_TOP_LEVEL)],
        }];
        assert!(matches!(
            encode_page(&duplicate),
            Err(ProgramPostingsError::OrdinalsNotAscending { .. })
        ));
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
    fn corrupt_continuations_and_directory_bytes_are_rejected() {
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
            Err(ProgramPostingsError::ContinuationKeyChanged(1))
        );

        let mut encoded = first.encode();
        encoded[31] = 1;
        assert_eq!(
            PageDirectoryEntry::decode(&encoded),
            Err(ProgramPostingsError::DirectoryReservedBytes)
        );
    }

    #[test]
    fn footer_has_frozen_bytes() {
        let footer = DirectoryFooter {
            directory_offset: 0x0102_0304_0506_0708,
            page_count: 0x1112_1314_1516_1718,
        };
        let bytes = footer.encode();
        assert_eq!(&bytes[0..8], b"BZIAPDIR");
        assert_eq!(DirectoryFooter::decode(&bytes).unwrap(), footer);
    }

    #[test]
    fn corrupt_counts_cannot_request_large_allocations() {
        let payload = [0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1];
        assert!(decode_page(&payload, 1, 1).is_err());
        assert!(matches!(
            decode_page(&[0], 1, 2),
            Err(ProgramPostingsError::KeyCountExceedsPage { .. })
        ));
    }
}
