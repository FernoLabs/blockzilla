use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

/// TODO: confirm account-compression program id
pub const STR_ID: &str = "AccountCompression11111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum AccountCompressionLog {
    /// canopy.rs: "Canopy byte length {} is not a multiple of {}"
    CanopyLengthMismatch {
        /// TODO: type (usize)
        canopy_bytes_len: StrId,
        /// TODO: type (usize)
        node_size: StrId,
    },

    /// error.rs: error_msg<T>
    /// "Failed to load {}. Size is {}, expected {}"
    FailedToLoadTypeSizeMismatch {
        /// TODO: type (type_name::<T>())
        type_name: StrId,
        /// TODO: type (usize)
        data_len: StrId,
        /// TODO: type (usize)
        expected_size: StrId,
    },
}

impl AccountCompressionLog {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some((a, b)) =
            parse_two_braced(payload, "Canopy byte length ", " is not a multiple of ")
        {
            return Some(Self::CanopyLengthMismatch {
                canopy_bytes_len: st.push(a),
                node_size: st.push(b),
            });
        }

        if let Some((t, rest)) = payload
            .strip_prefix("Failed to load ")?
            .split_once(". Size is ")
        {
            let (data_len, expected) = rest.split_once(", expected ")?;
            return Some(Self::FailedToLoadTypeSizeMismatch {
                type_name: st.push(t.trim()),
                data_len: st.push(data_len.trim()),
                expected_size: st.push(expected.trim()),
            });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::CanopyLengthMismatch {
                canopy_bytes_len,
                node_size,
            } => format!(
                "Canopy byte length {} is not a multiple of {}",
                st.resolve(*canopy_bytes_len),
                st.resolve(*node_size)
            ),
            Self::FailedToLoadTypeSizeMismatch {
                type_name,
                data_len,
                expected_size,
            } => format!(
                "Failed to load {}. Size is {}, expected {}",
                st.resolve(*type_name),
                st.resolve(*data_len),
                st.resolve(*expected_size)
            ),
        }
    }
}

#[inline]
fn parse_two_braced<'a>(text: &'a str, prefix: &str, mid: &str) -> Option<(&'a str, &'a str)> {
    let rest = text.strip_prefix(prefix)?;
    let (a, b) = rest.split_once(mid)?;
    Some((a.trim(), b.trim()))
}
