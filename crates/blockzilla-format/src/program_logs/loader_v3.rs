use serde::{Deserialize, Serialize};

use crate::{StrId, StringTable};

/// BPF Upgradeable Loader (commonly referred to as loader-v3)
/// TODO: confirm id in your environment (Agave/Solana)
pub const STR_ID: &str = "BPFLoaderUpgradeab1e11111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoaderV3Log {
    /// processor.rs:111 "Write overflow: {} < {}"
    WriteOverflow {
        /// TODO: type (usize)
        buffer_data_len: StrId,
        /// TODO: type (usize)
        end_offset: StrId,
    },

    /// processor.rs:307 "Deployed program: {}"
    DeployedProgram {
        /// TODO: type (Pubkey)
        program_key: StrId,
    },

    /// processor.rs:492 "Upgraded program: {}"
    UpgradedProgram {
        /// TODO: type (Pubkey)
        program_key: StrId,
    },

    /// processor.rs:600 "New authority: {:?}"
    NewAuthorityDebug {
        /// TODO: type (Option<Pubkey>)
        new_authority: StrId,
    },

    /// processor.rs:733 "Closed Uninitialized {}"
    ClosedUninitialized {
        /// TODO: type (Pubkey)
        key: StrId,
    },

    /// processor.rs:736 "Closed Buffer {}"
    ClosedBuffer {
        /// TODO: type (Pubkey)
        key: StrId,
    },

    /// processor.rs:739 "Closed Program {}"
    ClosedProgram {
        /// TODO: type (Pubkey)
        key: StrId,
    },

    /// processor.rs:795 "Extended ProgramData length of {} bytes exceeds max account data length of {} bytes"
    ExtendedProgramDataLengthExceedsMax {
        /// TODO: type (usize)
        new_len: StrId,
        /// TODO: type (usize const)
        max_permitted_data_length: StrId,
    },

    /// processor.rs:871 "Extended ProgramData account by {} bytes"
    ExtendedProgramDataAccountBy {
        /// TODO: type (usize)
        additional_bytes: StrId,
    },

    /// processor.rs:970 "New authority: {:?}"
    NewAuthorityDebug2 {
        /// TODO: type (Pubkey)
        new_authority: StrId,
    },
}

impl LoaderV3Log {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        // "Write overflow: {} < {}"
        if let Some((a, b)) = parse_two_braced(payload, "Write overflow: ", " < ") {
            return Some(Self::WriteOverflow {
                buffer_data_len: st.push(a),
                end_offset: st.push(b),
            });
        }

        if let Some(x) = payload.strip_prefix("Deployed program: ") {
            return Some(Self::DeployedProgram {
                program_key: st.push(x.trim()),
            });
        }

        if let Some(x) = payload.strip_prefix("Upgraded program: ") {
            return Some(Self::UpgradedProgram {
                program_key: st.push(x.trim()),
            });
        }

        // "New authority: {:?}"
        if let Some(x) = payload.strip_prefix("New authority: ") {
            // covers both :600 and :970 variants
            return Some(Self::NewAuthorityDebug {
                new_authority: st.push(x.trim()),
            });
        }

        if let Some(x) = payload.strip_prefix("Closed Uninitialized ") {
            return Some(Self::ClosedUninitialized {
                key: st.push(x.trim()),
            });
        }
        if let Some(x) = payload.strip_prefix("Closed Buffer ") {
            return Some(Self::ClosedBuffer {
                key: st.push(x.trim()),
            });
        }
        if let Some(x) = payload.strip_prefix("Closed Program ") {
            return Some(Self::ClosedProgram {
                key: st.push(x.trim()),
            });
        }

        if let Some((a, b)) = parse_two_braced(
            payload,
            "Extended ProgramData length of ",
            " bytes exceeds max account data length of ",
        ) {
            // b still includes trailing " bytes" in the real string, so handle exact suffix.
            let b = b.strip_suffix(" bytes")?.trim();
            return Some(Self::ExtendedProgramDataLengthExceedsMax {
                new_len: st.push(a),
                max_permitted_data_length: st.push(b),
            });
        }

        if let Some(x) = parse_one_braced(payload, "Extended ProgramData account by ", " bytes") {
            return Some(Self::ExtendedProgramDataAccountBy {
                additional_bytes: st.push(x),
            });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::WriteOverflow {
                buffer_data_len,
                end_offset,
            } => format!(
                "Write overflow: {} < {}",
                st.resolve(*buffer_data_len),
                st.resolve(*end_offset)
            ),
            Self::DeployedProgram { program_key } => {
                format!("Deployed program: {}", st.resolve(*program_key))
            }
            Self::UpgradedProgram { program_key } => {
                format!("Upgraded program: {}", st.resolve(*program_key))
            }
            Self::NewAuthorityDebug { new_authority } => {
                format!("New authority: {:?}", st.resolve(*new_authority))
            }
            Self::ClosedUninitialized { key } => {
                format!("Closed Uninitialized {}", st.resolve(*key))
            }
            Self::ClosedBuffer { key } => format!("Closed Buffer {}", st.resolve(*key)),
            Self::ClosedProgram { key } => format!("Closed Program {}", st.resolve(*key)),
            Self::ExtendedProgramDataLengthExceedsMax {
                new_len,
                max_permitted_data_length,
            } => format!(
                "Extended ProgramData length of {} bytes exceeds max account data length of {} bytes",
                st.resolve(*new_len),
                st.resolve(*max_permitted_data_length)
            ),
            Self::ExtendedProgramDataAccountBy { additional_bytes } => format!(
                "Extended ProgramData account by {} bytes",
                st.resolve(*additional_bytes)
            ),
            Self::NewAuthorityDebug2 { new_authority } => {
                format!("New authority: {:?}", st.resolve(*new_authority))
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

#[inline]
fn parse_two_braced<'a>(text: &'a str, prefix: &str, mid: &str) -> Option<(&'a str, &'a str)> {
    let rest = text.strip_prefix(prefix)?;
    let (a, b) = rest.split_once(mid)?;
    Some((a.trim(), b.trim()))
}
