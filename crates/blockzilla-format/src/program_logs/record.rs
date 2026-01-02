use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

/// TODO: confirm record program id
pub const STR_ID: &str = "Record111111111111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum RecordLog {
    /// processor.rs:171 "reallocating +{:?} bytes"
    ReallocatingPlusBytesDebug {
        /// TODO: type (usize)
        bytes: StrId,
    },
}

impl RecordLog {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(x) = parse_one_braced(payload, "reallocating +", " bytes") {
            return Some(Self::ReallocatingPlusBytesDebug { bytes: st.push(x) });
        }
        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::ReallocatingPlusBytesDebug { bytes } => {
                format!("reallocating +{:?} bytes", st.resolve(*bytes))
            }
        }
    }
}

#[inline]
fn parse_one_braced<'a>(text: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(suffix)?;
    Some(inner.trim())
}
