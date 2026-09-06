//! `catalog/blocks.wincode`: fixed-address Wincode block rows.
//!
//! The catalog owns block identity and four block-level byte ranges:
//! transactions, block rewards, PoH, and shredding. `TransactionBlock` is the
//! sole owner of the six transaction-effect chunk locators. A row has a fixed
//! 144-byte slot for direct remote point reads. Its 139-byte Wincode payload
//! uses explicit little-endian byte arrays for fixed-width catalog numbers.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    ledger::transactions::{HashOwner, HashRef},
    wincode as wire,
};

pub const PATH: &str = "catalog/blocks.wincode";
pub const SCHEMA: u16 = 1;
pub const COLUMNS: [&str; 4] = [
    crate::ledger::transactions::PATH,
    crate::runtime::block_rewards::PATH,
    crate::sidecars::poh::PATH,
    crate::sidecars::shredding::PATH,
];
pub const ROW_LEN: usize = 144;
const ROW_PAYLOAD_LEN: usize = 139;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageSpan {
    pub offset: u64,
    pub stored_len: u32,
    pub decoded_len: u32,
}

impl PageSpan {
    pub fn byte_range(self) -> std::ops::Range<u64> {
        self.offset..self.offset + u64::from(self.stored_len)
    }

    pub const fn is_compressed(self) -> bool {
        self.stored_len != self.decoded_len
    }

    fn validate(self, name: &'static str) -> Result<(), BlockRowError> {
        if self.stored_len == 0 || self.decoded_len == 0 {
            return Err(BlockRowError::EmptySpan(name));
        }
        self.offset
            .checked_add(u64::from(self.stored_len))
            .ok_or(BlockRowError::SpanOverflow(name))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FactLocator {
    #[default]
    Unavailable,
    Source(PageSpan),
    Backfilled(PageSpan),
}

impl FactLocator {
    pub const fn span(self) -> Option<PageSpan> {
        match self {
            Self::Unavailable => None,
            Self::Source(span) | Self::Backfilled(span) => Some(span),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRow {
    pub slot: u64,
    pub parent_slot: u64,
    pub first_transaction: u64,
    pub transaction_count: u32,
    pub blockhash: HashRef,
    pub previous_blockhash: HashRef,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    /// First fixed 64-byte signature in `sidecars/signatures.bin`.
    pub first_signature: u64,
    pub transactions: PageSpan,
    pub block_rewards: FactLocator,
    /// Full self-delimiting PoH block frame. A `PohBlockFinal` hash ordinal is
    /// a catalog block ordinal, so this span makes that hash an O(1) read.
    pub poh: FactLocator,
    pub shredding: FactLocator,
}

impl Default for BlockRow {
    fn default() -> Self {
        Self {
            slot: 0,
            parent_slot: 0,
            first_transaction: 0,
            transaction_count: 0,
            blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            previous_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            block_time: None,
            block_height: None,
            first_signature: 0,
            transactions: PageSpan::default(),
            block_rewards: FactLocator::Unavailable,
            poh: FactLocator::Unavailable,
            shredding: FactLocator::Unavailable,
        }
    }
}

impl BlockRow {
    pub fn validate(self) -> Result<(), BlockRowError> {
        if self.parent_slot >= self.slot && self.slot != 0 {
            return Err(BlockRowError::ParentNotBeforeSlot {
                slot: self.slot,
                parent_slot: self.parent_slot,
            });
        }
        self.transactions.validate("transactions")?;
        for (name, locator) in [
            ("block_rewards", self.block_rewards),
            ("poh", self.poh),
            ("shredding", self.shredding),
        ] {
            if let Some(span) = locator.span() {
                span.validate(name)?;
            }
        }
        self.transactions_end()?;
        Ok(())
    }

    pub fn encode(self) -> Result<[u8; ROW_LEN], BlockRowError> {
        self.validate()?;
        let payload = wire::encode(&WireBlockRow::from(self))?;
        if payload.len() != ROW_PAYLOAD_LEN {
            return Err(BlockRowError::RowPayloadLength(payload.len()));
        }
        let mut bytes = [0_u8; ROW_LEN];
        bytes[..ROW_PAYLOAD_LEN].copy_from_slice(&payload);
        Ok(bytes)
    }

    pub fn decode(input: &[u8]) -> Result<Self, BlockRowError> {
        let bytes: &[u8; ROW_LEN] = input
            .get(..ROW_LEN)
            .and_then(|value| value.try_into().ok())
            .ok_or(BlockRowError::Truncated(input.len()))?;
        if bytes[ROW_PAYLOAD_LEN..].iter().any(|byte| *byte != 0) {
            return Err(BlockRowError::NonzeroPadding);
        }
        let wire_row: WireBlockRow = wire::decode_exact(&bytes[..ROW_PAYLOAD_LEN])?;
        let row = Self::try_from(wire_row)?;
        row.validate()?;
        Ok(row)
    }

    pub fn transactions_end(self) -> Result<u64, BlockRowError> {
        self.first_transaction
            .checked_add(u64::from(self.transaction_count))
            .ok_or(BlockRowError::TransactionRangeOverflow)
    }

    pub fn contains_transaction(self, ordinal: u64) -> bool {
        self.transactions_end()
            .is_ok_and(|end| ordinal >= self.first_transaction && ordinal < end)
    }

    pub fn page(&self, column: usize) -> Option<PageSpan> {
        match column {
            0 => Some(self.transactions),
            1 => self.block_rewards.span(),
            2 => self.poh.span(),
            3 => self.shredding.span(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
struct WireBlockRow {
    slot: [u8; 8],
    parent_slot: [u8; 8],
    first_transaction: [u8; 8],
    transaction_count: [u8; 4],
    blockhash: WireHashRef,
    previous_blockhash: WireHashRef,
    block_time: WireOptionalI64,
    block_height: WireOptionalU64,
    first_signature: [u8; 8],
    transactions: WirePageSpan,
    block_rewards: WireFactLocator,
    poh: WireFactLocator,
    shredding: WireFactLocator,
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
struct WireHashRef {
    owner: u8,
    ordinal: [u8; 8],
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
struct WireOptionalI64 {
    present: u8,
    value: [u8; 8],
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
struct WireOptionalU64 {
    present: u8,
    value: [u8; 8],
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
struct WirePageSpan {
    offset: [u8; 8],
    stored_len: [u8; 4],
    decoded_len: [u8; 4],
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
struct WireFactLocator {
    state: u8,
    span: WirePageSpan,
}

impl From<BlockRow> for WireBlockRow {
    fn from(row: BlockRow) -> Self {
        Self {
            slot: row.slot.to_le_bytes(),
            parent_slot: row.parent_slot.to_le_bytes(),
            first_transaction: row.first_transaction.to_le_bytes(),
            transaction_count: row.transaction_count.to_le_bytes(),
            blockhash: row.blockhash.into(),
            previous_blockhash: row.previous_blockhash.into(),
            block_time: row.block_time.into(),
            block_height: row.block_height.into(),
            first_signature: row.first_signature.to_le_bytes(),
            transactions: row.transactions.into(),
            block_rewards: row.block_rewards.into(),
            poh: row.poh.into(),
            shredding: row.shredding.into(),
        }
    }
}

impl TryFrom<WireBlockRow> for BlockRow {
    type Error = BlockRowError;

    fn try_from(row: WireBlockRow) -> Result<Self, Self::Error> {
        Ok(Self {
            slot: u64::from_le_bytes(row.slot),
            parent_slot: u64::from_le_bytes(row.parent_slot),
            first_transaction: u64::from_le_bytes(row.first_transaction),
            transaction_count: u32::from_le_bytes(row.transaction_count),
            blockhash: row.blockhash.try_into()?,
            previous_blockhash: row.previous_blockhash.try_into()?,
            block_time: row.block_time.try_into()?,
            block_height: row.block_height.try_into()?,
            first_signature: u64::from_le_bytes(row.first_signature),
            transactions: row.transactions.into(),
            block_rewards: row.block_rewards.try_into()?,
            poh: row.poh.try_into()?,
            shredding: row.shredding.try_into()?,
        })
    }
}

impl From<HashRef> for WireHashRef {
    fn from(value: HashRef) -> Self {
        Self {
            owner: value.owner as u8,
            ordinal: value.ordinal.to_le_bytes(),
        }
    }
}

impl TryFrom<WireHashRef> for HashRef {
    type Error = BlockRowError;

    fn try_from(value: WireHashRef) -> Result<Self, Self::Error> {
        let owner = match value.owner {
            0 => HashOwner::NonPoh,
            1 => HashOwner::PohBlockFinal,
            other => return Err(BlockRowError::UnknownHashOwner(other)),
        };
        Ok(Self {
            owner,
            ordinal: u64::from_le_bytes(value.ordinal),
        })
    }
}

macro_rules! optional_wire {
    ($wire:ty, $value:ty) => {
        impl From<Option<$value>> for $wire {
            fn from(value: Option<$value>) -> Self {
                match value {
                    None => Self {
                        present: 0,
                        value: [0; 8],
                    },
                    Some(value) => Self {
                        present: 1,
                        value: value.to_le_bytes(),
                    },
                }
            }
        }

        impl TryFrom<$wire> for Option<$value> {
            type Error = BlockRowError;

            fn try_from(value: $wire) -> Result<Self, Self::Error> {
                match value.present {
                    0 if value.value == [0; 8] => Ok(None),
                    0 => Err(BlockRowError::NonzeroAbsentOptional),
                    1 => Ok(Some(<$value>::from_le_bytes(value.value))),
                    other => Err(BlockRowError::UnknownOptionalTag(other)),
                }
            }
        }
    };
}

optional_wire!(WireOptionalI64, i64);
optional_wire!(WireOptionalU64, u64);

impl From<PageSpan> for WirePageSpan {
    fn from(value: PageSpan) -> Self {
        Self {
            offset: value.offset.to_le_bytes(),
            stored_len: value.stored_len.to_le_bytes(),
            decoded_len: value.decoded_len.to_le_bytes(),
        }
    }
}

impl From<WirePageSpan> for PageSpan {
    fn from(value: WirePageSpan) -> Self {
        Self {
            offset: u64::from_le_bytes(value.offset),
            stored_len: u32::from_le_bytes(value.stored_len),
            decoded_len: u32::from_le_bytes(value.decoded_len),
        }
    }
}

impl From<FactLocator> for WireFactLocator {
    fn from(value: FactLocator) -> Self {
        match value {
            FactLocator::Unavailable => Self {
                state: 0,
                span: PageSpan::default().into(),
            },
            FactLocator::Source(span) => Self {
                state: 1,
                span: span.into(),
            },
            FactLocator::Backfilled(span) => Self {
                state: 2,
                span: span.into(),
            },
        }
    }
}

impl TryFrom<WireFactLocator> for FactLocator {
    type Error = BlockRowError;

    fn try_from(value: WireFactLocator) -> Result<Self, Self::Error> {
        let span = PageSpan::from(value.span);
        match value.state {
            0 if span == PageSpan::default() => Ok(Self::Unavailable),
            0 => Err(BlockRowError::NonzeroUnavailableSpan),
            1 => Ok(Self::Source(span)),
            2 => Ok(Self::Backfilled(span)),
            other => Err(BlockRowError::UnknownFactState(other)),
        }
    }
}

pub fn column_index(path: &str) -> Option<usize> {
    COLUMNS.iter().position(|column| *column == path)
}

pub fn encode_table(rows: &[BlockRow]) -> Result<Vec<u8>, BlockRowError> {
    let capacity = rows
        .len()
        .checked_mul(ROW_LEN)
        .ok_or(BlockRowError::RowIndexOverflow)?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut previous = None;
    for (index, row) in rows.iter().enumerate() {
        row.validate_at(u64::try_from(index).map_err(|_| BlockRowError::RowIndexOverflow)?)?;
        if let Some(previous) = previous {
            validate_successor(previous, *row)?;
        }
        bytes.extend_from_slice(&row.encode()?);
        previous = Some(*row);
    }
    Ok(bytes)
}

pub fn decode_table(table: &[u8]) -> Result<Vec<BlockRow>, BlockRowError> {
    if !table.len().is_multiple_of(ROW_LEN) {
        return Err(BlockRowError::TableNotRowAligned(table.len()));
    }
    let mut rows = Vec::with_capacity(table.len() / ROW_LEN);
    for (index, bytes) in table.chunks_exact(ROW_LEN).enumerate() {
        let row = BlockRow::decode(bytes)?;
        row.validate_at(u64::try_from(index).map_err(|_| BlockRowError::RowIndexOverflow)?)?;
        if let Some(previous) = rows.last().copied() {
            validate_successor(previous, row)?;
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Validate the relation between two adjacent catalog rows.
///
/// Point readers can call this after two fixed-size row reads. They do not
/// need to decode the complete catalog to prove the parent, hash, and
/// transaction-range links for the selected row.
pub fn validate_successor(previous: BlockRow, row: BlockRow) -> Result<(), BlockRowError> {
    if row.slot <= previous.slot {
        return Err(BlockRowError::SlotsNotAscending {
            previous: previous.slot,
            current: row.slot,
        });
    }
    if row.parent_slot != previous.slot {
        return Err(BlockRowError::ParentLinkDisagrees {
            slot: row.slot,
            parent_slot: row.parent_slot,
            expected_parent: previous.slot,
        });
    }
    if row.previous_blockhash != previous.blockhash {
        return Err(BlockRowError::PreviousBlockhashDisagrees { slot: row.slot });
    }
    let expected = previous.transactions_end()?;
    if row.first_transaction != expected {
        return Err(BlockRowError::TransactionRangeNotContiguous {
            expected,
            found: row.first_transaction,
        });
    }
    Ok(())
}

impl BlockRow {
    /// Validate facts whose meaning depends on this row's catalog ordinal.
    pub fn validate_at(self, block_ordinal: u64) -> Result<(), BlockRowError> {
        if self.blockhash.owner == HashOwner::PohBlockFinal
            && self.blockhash.ordinal != block_ordinal
        {
            return Err(BlockRowError::PohBlockOrdinalDisagrees {
                field: "blockhash",
                expected: block_ordinal,
                found: self.blockhash.ordinal,
            });
        }
        if self.previous_blockhash.owner == HashOwner::PohBlockFinal {
            let expected = block_ordinal.checked_sub(1).ok_or(
                BlockRowError::PohBlockOrdinalBeforeCatalog {
                    field: "previous_blockhash",
                },
            )?;
            if self.previous_blockhash.ordinal != expected {
                return Err(BlockRowError::PohBlockOrdinalDisagrees {
                    field: "previous_blockhash",
                    expected,
                    found: self.previous_blockhash.ordinal,
                });
            }
        }
        Ok(())
    }
}

pub fn row_at(table: &[u8], index: usize) -> Result<BlockRow, BlockRowError> {
    let start = index
        .checked_mul(ROW_LEN)
        .ok_or(BlockRowError::RowIndexOverflow)?;
    let row = BlockRow::decode(table.get(start..).unwrap_or_default())?;
    row.validate_at(u64::try_from(index).map_err(|_| BlockRowError::RowIndexOverflow)?)?;
    Ok(row)
}

pub fn block_of_transaction(rows: &[BlockRow], ordinal: u64) -> Option<usize> {
    rows.binary_search_by(|row| {
        if ordinal < row.first_transaction {
            std::cmp::Ordering::Greater
        } else if !row.contains_transaction(ordinal) {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    })
    .ok()
}

pub fn search_slot(
    row_count: u64,
    slot: u64,
    mut fetch: impl FnMut(u64) -> Result<BlockRow, BlockRowError>,
) -> Result<Option<(u64, BlockRow)>, BlockRowError> {
    let (mut low, mut high) = (0, row_count);
    while low < high {
        let middle = low + (high - low) / 2;
        let row = fetch(middle)?;
        match row.slot.cmp(&slot) {
            std::cmp::Ordering::Equal => return Ok(Some((middle, row))),
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
        }
    }
    Ok(None)
}

pub fn block_of_slot(rows: &[BlockRow], slot: u64) -> Option<usize> {
    rows.binary_search_by_key(&slot, |row| row.slot).ok()
}

#[derive(Debug, Error)]
pub enum BlockRowError {
    #[error("block catalog Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("block catalog Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("block row needs {ROW_LEN} bytes, found {0}")]
    Truncated(usize),
    #[error("block table has {0} bytes and is not row-aligned")]
    TableNotRowAligned(usize),
    #[error("row index overflows a byte offset")]
    RowIndexOverflow,
    #[error("row Wincode payload has {0} bytes, expected {ROW_PAYLOAD_LEN}")]
    RowPayloadLength(usize),
    #[error("row padding is not zero")]
    NonzeroPadding,
    #[error("unknown blockhash owner tag {0}")]
    UnknownHashOwner(u8),
    #[error("unknown optional-value tag {0}")]
    UnknownOptionalTag(u8),
    #[error("an absent optional value has nonzero bytes")]
    NonzeroAbsentOptional,
    #[error("unknown fact-locator state {0}")]
    UnknownFactState(u8),
    #[error("an unavailable fact locator has a nonzero span")]
    NonzeroUnavailableSpan,
    #[error("{0} span has a zero length")]
    EmptySpan(&'static str),
    #[error("{0} span end overflows u64")]
    SpanOverflow(&'static str),
    #[error("slot {slot} has parent {parent_slot}, which is not before it")]
    ParentNotBeforeSlot { slot: u64, parent_slot: u64 },
    #[error("slots must strictly ascend: {previous} then {current}")]
    SlotsNotAscending { previous: u64, current: u64 },
    #[error("slot {slot} links to parent {parent_slot}, expected {expected_parent}")]
    ParentLinkDisagrees {
        slot: u64,
        parent_slot: u64,
        expected_parent: u64,
    },
    #[error("slot {slot} previous blockhash does not match the prior catalog row")]
    PreviousBlockhashDisagrees { slot: u64 },
    #[error("{field} PoH block ordinal is {found}, expected {expected}")]
    PohBlockOrdinalDisagrees {
        field: &'static str,
        expected: u64,
        found: u64,
    },
    #[error("{field} PoH block ordinal points before the catalog")]
    PohBlockOrdinalBeforeCatalog { field: &'static str },
    #[error("transaction range overflows u64")]
    TransactionRangeOverflow,
    #[error("transaction ranges must be contiguous: expected {expected}, found {found}")]
    TransactionRangeNotContiguous { expected: u64, found: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(offset: u64) -> PageSpan {
        PageSpan {
            offset,
            stored_len: 40,
            decoded_len: 100,
        }
    }

    fn row(slot: u64, first_transaction: u64) -> BlockRow {
        let block_ordinal = first_transaction / 3;
        BlockRow {
            slot,
            parent_slot: if slot == 102 { 100 } else { slot - 1 },
            first_transaction,
            transaction_count: 3,
            blockhash: HashRef {
                owner: HashOwner::PohBlockFinal,
                ordinal: block_ordinal,
            },
            previous_blockhash: if block_ordinal == 0 {
                HashRef {
                    owner: HashOwner::NonPoh,
                    ordinal: slot - 1,
                }
            } else {
                HashRef {
                    owner: HashOwner::PohBlockFinal,
                    ordinal: block_ordinal - 1,
                }
            },
            block_time: Some(1_700_000_000),
            block_height: None,
            first_signature: first_transaction,
            transactions: span(slot * 10),
            block_rewards: FactLocator::Source(span(slot * 10 + 5)),
            poh: FactLocator::Source(span(slot * 10 + 6)),
            shredding: FactLocator::Unavailable,
        }
    }

    #[test]
    fn fixed_slots_round_trip_and_support_point_reads() {
        let rows = [row(100, 0), row(102, 3)];
        let table = encode_table(&rows).unwrap();
        assert_eq!(table.len(), ROW_LEN * 2);
        assert_eq!(decode_table(&table).unwrap(), rows);
        assert_eq!(row_at(&table, 1).unwrap(), rows[1]);
    }

    #[test]
    fn four_block_locators_have_one_owner() {
        assert_eq!(
            COLUMNS,
            [
                crate::ledger::transactions::PATH,
                crate::runtime::block_rewards::PATH,
                crate::sidecars::poh::PATH,
                crate::sidecars::shredding::PATH,
            ]
        );
        let row = row(100, 0);
        assert_eq!(row.page(0), Some(row.transactions));
        assert_eq!(row.page(1), row.block_rewards.span());
        assert_eq!(row.page(2), row.poh.span());
        assert_eq!(row.page(3), row.shredding.span());
        assert_eq!(row.page(4), None);
    }

    #[test]
    fn maximum_form_is_exactly_139_payload_bytes_in_a_144_byte_slot() {
        let maximum = BlockRow {
            slot: u64::MAX,
            parent_slot: u64::MAX - 1,
            first_transaction: u64::MAX - u64::from(u32::MAX),
            transaction_count: u32::MAX,
            blockhash: HashRef {
                owner: HashOwner::PohBlockFinal,
                ordinal: u64::MAX,
            },
            previous_blockhash: HashRef {
                owner: HashOwner::PohBlockFinal,
                ordinal: u64::MAX,
            },
            block_time: Some(i64::MIN),
            block_height: Some(u64::MAX),
            first_signature: u64::MAX,
            transactions: PageSpan {
                offset: u64::MAX - u64::from(u32::MAX),
                stored_len: u32::MAX,
                decoded_len: u32::MAX,
            },
            block_rewards: FactLocator::Backfilled(PageSpan {
                offset: u64::MAX - u64::from(u32::MAX),
                stored_len: u32::MAX,
                decoded_len: u32::MAX,
            }),
            poh: FactLocator::Backfilled(PageSpan {
                offset: u64::MAX - u64::from(u32::MAX),
                stored_len: u32::MAX,
                decoded_len: u32::MAX,
            }),
            shredding: FactLocator::Backfilled(PageSpan {
                offset: u64::MAX - u64::from(u32::MAX),
                stored_len: u32::MAX,
                decoded_len: u32::MAX,
            }),
        };
        let payload = wire::encode(&WireBlockRow::from(maximum)).unwrap();
        assert_eq!(payload.len(), 139);
        let bytes = maximum.encode().unwrap();
        assert_eq!(&bytes[..8], &u64::MAX.to_le_bytes());
        assert_eq!(&bytes[139..], &[0; 5]);
        assert_eq!(BlockRow::decode(&bytes).unwrap(), maximum);
    }

    #[test]
    fn searches_and_contiguity_work() {
        let rows = [row(100, 0), row(102, 3)];
        assert_eq!(block_of_transaction(&rows, 4), Some(1));
        assert_eq!(block_of_slot(&rows, 101), None);
        let table = encode_table(&rows).unwrap();
        assert_eq!(
            search_slot(2, 102, |index| row_at(&table, index as usize))
                .unwrap()
                .map(|(index, _)| index),
            Some(1)
        );
        assert!(matches!(
            encode_table(&[row(100, 0), row(102, 4)]),
            Err(BlockRowError::TransactionRangeNotContiguous { .. })
        ));
    }

    #[test]
    fn successor_must_link_to_prior_slot_and_hash() {
        let first = row(100, 0);
        let mut bad_parent = row(102, 3);
        bad_parent.parent_slot = 99;
        assert!(matches!(
            encode_table(&[first, bad_parent]),
            Err(BlockRowError::ParentLinkDisagrees { .. })
        ));

        let mut bad_hash = row(102, 3);
        bad_hash.previous_blockhash = HashRef {
            owner: HashOwner::NonPoh,
            ordinal: 77,
        };
        assert!(matches!(
            encode_table(&[first, bad_hash]),
            Err(BlockRowError::PreviousBlockhashDisagrees { .. })
        ));
    }

    #[test]
    fn poh_hashes_resolve_by_catalog_block_ordinal() {
        let mut wrong_current = row(100, 0);
        wrong_current.blockhash.ordinal = 9;
        assert!(matches!(
            encode_table(&[wrong_current]),
            Err(BlockRowError::PohBlockOrdinalDisagrees {
                field: "blockhash",
                expected: 0,
                found: 9,
            })
        ));

        let mut wrong_previous = row(102, 3);
        wrong_previous.previous_blockhash.ordinal = 9;
        assert!(matches!(
            encode_table(&[row(100, 0), wrong_previous]),
            Err(BlockRowError::PohBlockOrdinalDisagrees {
                field: "previous_blockhash",
                expected: 0,
                found: 9,
            })
        ));

        let mut before_catalog = row(100, 0);
        before_catalog.previous_blockhash = HashRef {
            owner: HashOwner::PohBlockFinal,
            ordinal: 0,
        };
        assert!(matches!(
            encode_table(&[before_catalog]),
            Err(BlockRowError::PohBlockOrdinalBeforeCatalog {
                field: "previous_blockhash"
            })
        ));
    }
}
