use serde::{Deserialize, Serialize};

use crate::{StrId, StringTable};

/// TODO: confirm loader-v4 id
pub const STR_ID: &str = "LoaderV411111111111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoaderV4Log {
    /// processor.rs:150 "Insufficient lamports, {} are required."
    InsufficientLamportsRequired {
        /// TODO: type (u64)
        required_lamports: StrId,
    },
}

impl LoaderV4Log {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(x) = parse_one_braced(payload, "Insufficient lamports, ", " are required.") {
            return Some(Self::InsufficientLamportsRequired {
                required_lamports: st.push(x),
            });
        }
        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::InsufficientLamportsRequired { required_lamports } => format!(
                "Insufficient lamports, {} are required.",
                st.resolve(*required_lamports)
            ),
        }
    }
}

#[inline]
fn parse_one_braced<'a>(text: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(suffix)?;
    Some(inner.trim())
}
