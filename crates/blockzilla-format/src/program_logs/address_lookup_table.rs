use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

/// TODO: confirm program id
pub const STR_ID: &str = "AddressLookupTab1e1111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum AddressLookupTableLog {
    /// entrypoint.rs:18 msg!(error.to_str::<AddressLookupTableError>())
    Error(AddressLookupTableErrorLog),

    /// processor.rs:157
    NotARecentSlot {
        /// TODO: type
        untrusted_recent_slot: StrId,
    },

    /// processor.rs:173
    TableAddressMustMatchDerivedAddress {
        /// TODO: type (Pubkey)
        derived_table_key: StrId,
    },

    /// processor.rs:327
    ExtendedLookupTableLengthWouldExceedMaxCapacity {
        /// TODO: type (usize)
        new_table_addresses_len: StrId,
        /// TODO: type (usize const)
        lookup_table_max_addresses: StrId,
    },

    /// processor.rs:531
    TableCannotBeClosedUntilFullyDeactivatedInBlocks {
        /// TODO: type (u64/usize)
        remaining_blocks: StrId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum AddressLookupTableErrorLog {
    /// "Length of the seed is too long for address generation"
    PubkeyErrorMaxSeedLengthExceeded,
    /// "Provided seeds do not result in a valid address"
    PubkeyErrorInvalidSeeds,
    /// "Provided owner is not allowed"
    PubkeyErrorIllegalOwner,
    /// "Instruction modified data of a read-only account"
    ReadonlyDataModified,
    /// "Instruction changed the balance of a read-only account"
    ReadonlyLamportsChanged,
}

impl AddressLookupTableErrorLog {
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "Length of the seed is too long for address generation" => {
                Some(Self::PubkeyErrorMaxSeedLengthExceeded)
            }
            "Provided seeds do not result in a valid address" => {
                Some(Self::PubkeyErrorInvalidSeeds)
            }
            "Provided owner is not allowed" => Some(Self::PubkeyErrorIllegalOwner),
            "Instruction modified data of a read-only account" => Some(Self::ReadonlyDataModified),
            "Instruction changed the balance of a read-only account" => {
                Some(Self::ReadonlyLamportsChanged)
            }
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PubkeyErrorMaxSeedLengthExceeded => {
                "Length of the seed is too long for address generation"
            }
            Self::PubkeyErrorInvalidSeeds => "Provided seeds do not result in a valid address",
            Self::PubkeyErrorIllegalOwner => "Provided owner is not allowed",
            Self::ReadonlyDataModified => "Instruction modified data of a read-only account",
            Self::ReadonlyLamportsChanged => {
                "Instruction changed the balance of a read-only account"
            }
        }
    }
}

impl AddressLookupTableLog {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        // errors first
        if let Some(e) = AddressLookupTableErrorLog::parse(payload) {
            return Some(Self::Error(e));
        }

        // "{} is not a recent slot"
        if let Some(x) = parse_one_braced(payload, " is not a recent slot") {
            return Some(Self::NotARecentSlot {
                untrusted_recent_slot: st.push(x),
            });
        }

        // "Table address must match derived address: {}"
        if let Some(x) = payload.strip_prefix("Table address must match derived address: ") {
            return Some(Self::TableAddressMustMatchDerivedAddress {
                derived_table_key: st.push(x.trim()),
            });
        }

        // "Extended lookup table length {} would exceed max capacity of {}"
        if let Some((a, b)) = parse_two_braced(
            payload,
            "Extended lookup table length ",
            " would exceed max capacity of ",
        ) {
            return Some(Self::ExtendedLookupTableLengthWouldExceedMaxCapacity {
                new_table_addresses_len: st.push(a),
                lookup_table_max_addresses: st.push(b),
            });
        }

        // "Table cannot be closed until it's fully deactivated in {} blocks"
        if let Some(x) = parse_one_braced_3(
            payload,
            "Table cannot be closed until it's fully deactivated in ",
            " blocks",
        ) {
            return Some(Self::TableCannotBeClosedUntilFullyDeactivatedInBlocks {
                remaining_blocks: st.push(x),
            });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::Error(e) => e.as_str().to_string(),
            Self::NotARecentSlot {
                untrusted_recent_slot,
            } => format!(
                "{} is not a recent slot",
                st.resolve(*untrusted_recent_slot)
            ),
            Self::TableAddressMustMatchDerivedAddress { derived_table_key } => format!(
                "Table address must match derived address: {}",
                st.resolve(*derived_table_key)
            ),
            Self::ExtendedLookupTableLengthWouldExceedMaxCapacity {
                new_table_addresses_len,
                lookup_table_max_addresses,
            } => format!(
                "Extended lookup table length {} would exceed max capacity of {}",
                st.resolve(*new_table_addresses_len),
                st.resolve(*lookup_table_max_addresses),
            ),
            Self::TableCannotBeClosedUntilFullyDeactivatedInBlocks { remaining_blocks } => format!(
                "Table cannot be closed until it's fully deactivated in {} blocks",
                st.resolve(*remaining_blocks)
            ),
        }
    }
}

#[inline]
fn parse_one_braced<'a>(text: &'a str, suffix: &str) -> Option<&'a str> {
    let inner = text.strip_suffix(suffix)?;
    Some(inner.trim())
}

#[inline]
fn parse_one_braced_3<'a>(text: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(suffix)?;
    Some(inner.trim())
}

#[inline]
fn parse_two_braced<'a>(text: &'a str, prefix: &str, mid: &str) -> Option<(&'a str, &'a str)> {
    let rest = text.strip_prefix(prefix)?;
    let (a, b) = rest.split_once(mid)?;
    Some((a.trim(), b.trim()))
}
