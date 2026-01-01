use serde::{Deserialize, Serialize};

use crate::{StrId, StringTable};

pub const STR_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoLog {
    /// processor.rs:18 "Signed by {:?}"
    SignedByDebug {
        /// TODO: type (Pubkey)
        address: StrId,
    },

    /// processor.rs:28 "Invalid UTF-8, from byte {}"
    InvalidUtf8FromByte {
        /// TODO: type (usize)
        valid_up_to: StrId,
    },

    /// processor.rs:31 "Memo (len {}): {:?}"
    MemoLenAndDebug {
        /// TODO: type (usize)
        len: StrId,
        /// TODO: type (bytes/string)
        memo: StrId,
    },
}

impl MemoLog {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(x) = payload.strip_prefix("Signed by ") {
            return Some(Self::SignedByDebug {
                address: st.push(x.trim()),
            });
        }

        if let Some(x) = parse_one_braced(payload, "Invalid UTF-8, from byte ", "") {
            // note: format string has no trailing punctuation
            return Some(Self::InvalidUtf8FromByte {
                valid_up_to: st.push(x),
            });
        }

        if let Some((a, b)) = parse_two_braced(payload, "Memo (len ", "): ") {
            return Some(Self::MemoLenAndDebug {
                len: st.push(a),
                memo: st.push(b),
            });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::SignedByDebug { address } => format!("Signed by {:?}", st.resolve(*address)),
            Self::InvalidUtf8FromByte { valid_up_to } => {
                format!("Invalid UTF-8, from byte {}", st.resolve(*valid_up_to))
            }
            Self::MemoLenAndDebug { len, memo } => {
                format!("Memo (len {}): {:?}", st.resolve(*len), st.resolve(*memo))
            }
        }
    }
}

#[inline]
fn parse_one_braced<'a>(text: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(prefix)?;
    if suffix.is_empty() {
        return Some(rest.trim());
    }
    let inner = rest.strip_suffix(suffix)?;
    Some(inner.trim())
}

#[inline]
fn parse_two_braced<'a>(text: &'a str, prefix: &str, mid: &str) -> Option<(&'a str, &'a str)> {
    let rest = text.strip_prefix(prefix)?;
    let (a, b) = rest.split_once(mid)?;
    Some((a.trim(), b.trim()))
}
