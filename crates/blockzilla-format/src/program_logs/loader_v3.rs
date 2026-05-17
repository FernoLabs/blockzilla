use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

/// BPF Upgradeable Loader.
pub const STR_ID: &str = "BPFLoaderUpgradeab1e11111111111111111111111";
pub const V1_STR_ID: &str = "BPFLoader1111111111111111111111111111111111";
pub const V2_STR_ID: &str = "BPFLoader2111111111111111111111111111111111";

#[inline]
pub fn is_bpf_loader_id(program: &str) -> bool {
    matches!(program, STR_ID | V1_STR_ID | V2_STR_ID)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LoaderV3Log {
    Static(LoaderV3StaticLog),

    /// processor.rs:111 "Write overflow: {} < {}"
    WriteOverflow {
        buffer_data_len: StrId,
        end_offset: StrId,
    },

    /// processor.rs:307 "Deployed program: {}"
    DeployedProgram {
        program_key: StrId,
    },

    /// processor.rs:358 "Deployed program {:?}"
    DeployedProgramPlain {
        program_key: StrId,
    },

    /// processor.rs:492 "Upgraded program: {}"
    UpgradedProgram {
        program_key: StrId,
    },

    /// processor.rs:534 "Upgraded program {:?}"
    UpgradedProgramPlain {
        program_key: StrId,
    },

    /// processor.rs:600 "New authority: {:?}"
    NewAuthorityDebug {
        new_authority: StrId,
    },

    /// processor.rs:591/659 "New authority {:?}"
    NewAuthorityDebugPlain {
        new_authority: StrId,
    },

    /// processor.rs:733 "Closed Uninitialized {}"
    ClosedUninitialized {
        key: StrId,
    },

    /// processor.rs:736 "Closed Buffer {}"
    ClosedBuffer {
        key: StrId,
    },

    /// processor.rs:739 "Closed Program {}"
    ClosedProgram {
        key: StrId,
    },

    /// processor.rs:795 "Extended ProgramData length of {} bytes exceeds max account data length of {} bytes"
    ExtendedProgramDataLengthExceedsMax {
        new_len: StrId,
        max_permitted_data_length: StrId,
    },

    /// processor.rs:871 "Extended ProgramData account by {} bytes"
    ExtendedProgramDataAccountBy {
        additional_bytes: StrId,
    },

    /// processor.rs:970 "New authority: {:?}"
    NewAuthorityDebug2 {
        new_authority: StrId,
    },
}

macro_rules! loader_v3_static_logs {
    ($( $variant:ident => $text:literal, )+ ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
        pub enum LoaderV3StaticLog {
            $( $variant, )+
        }

        impl LoaderV3StaticLog {
            #[inline]
            pub fn parse(text: &str) -> Option<Self> {
                match text {
                    $( $text => Some(Self::$variant), )+
                    _ => None,
                }
            }

            #[inline]
            pub fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $text, )+
                }
            }
        }
    };
}

loader_v3_static_logs! {
    BpfLoaderManagementInstructionsNoLongerSupported => "BPF loader management instructions are no longer supported",
    DeprecatedLoaderNoLongerSupported => "Deprecated loader is no longer supported",
    InvalidBpfLoaderId => "Invalid BPF loader id",
    ProgramIsNotCached => "Program is not cached",
    ProgramIsNotDeployed => "Program is not deployed",
    BufferAccountAlreadyInitialized => "Buffer account already initialized",
    BufferIsImmutable => "Buffer is immutable",
    IncorrectBufferAuthorityProvided => "Incorrect buffer authority provided",
    BufferAuthorityDidNotSign => "Buffer authority did not sign",
    InvalidBufferAccount => "Invalid Buffer account",
    ProgramAccountAlreadyInitialized => "Program account already initialized",
    ProgramAccountTooSmall => "Program account too small",
    ProgramAccountNotRentExempt => "Program account not rent-exempt",
    BufferAndUpgradeAuthorityDontMatch => "Buffer and upgrade authority don't match",
    UpgradeAuthorityDidNotSign => "Upgrade authority did not sign",
    BufferAccountTooSmall => "Buffer account too small",
    MaxDataLengthTooSmallToHoldBufferData => "Max data length is too small to hold Buffer data",
    MaxDataLengthTooLarge => "Max data length is too large",
    ProgramDataAddressIsNotDerived => "ProgramData address is not derived",
    ProgramAccountNotWriteable => "Program account not writeable",
    ProgramAccountNotOwnedByLoader => "Program account not owned by loader",
    ProgramAndProgramDataAccountMismatch => "Program and ProgramData account mismatch",
    InvalidProgramAccount => "Invalid Program account",
    ProgramDataAccountNotLargeEnough => "ProgramData account not large enough",
    BufferAccountBalanceTooLowToFundUpgrade => "Buffer account balance too low to fund upgrade",
    ProgramWasDeployedInThisBlockAlready => "Program was deployed in this block already",
    ProgramNotUpgradeable => "Program not upgradeable",
    IncorrectUpgradeAuthorityProvided => "Incorrect upgrade authority provided",
    InvalidProgramDataAccount => "Invalid ProgramData account",
    BufferAuthorityIsNotOptional => "Buffer authority is not optional",
    AccountDoesNotSupportAuthorities => "Account does not support authorities",
    NewAuthorityDidNotSign => "New authority did not sign",
    RecipientSameAsAccountBeingClosed => "Recipient is the same as the account being closed",
    ProgramAccountIsNotWritable => "Program account is not writable",
    ProgramDataAccountDoesNotMatchProgramDataAccount => "ProgramData account does not match ProgramData account",
    AccountDoesNotSupportClosing => "Account does not support closing",
    AdditionalBytesMustBeGreaterThanZero => "Additional bytes must be greater than 0",
    ProgramDataOwnerIsInvalid => "ProgramData owner is invalid",
    ProgramDataIsNotWritable => "ProgramData is not writable",
    ProgramAccountDoesNotMatchProgramDataAccount => "Program account does not match ProgramData account",
    ProgramWasExtendedInThisBlockAlready => "Program was extended in this block already",
    CannotExtendProgramDataAccountsThatAreNotUpgradeable => "Cannot extend ProgramData accounts that are not upgradeable",
    ProgramDataStateIsInvalid => "ProgramData state is invalid",
    AccountIsImmutable => "Account is immutable",
    IncorrectAuthorityProvided => "Incorrect authority provided",
    AuthorityDidNotSign => "Authority did not sign",
}

impl LoaderV3Log {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(log) = LoaderV3StaticLog::parse(payload) {
            return Some(Self::Static(log));
        }

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
        if let Some(x) = payload.strip_prefix("Deployed program ") {
            return Some(Self::DeployedProgramPlain {
                program_key: st.push(x.trim()),
            });
        }

        if let Some(x) = payload.strip_prefix("Upgraded program: ") {
            return Some(Self::UpgradedProgram {
                program_key: st.push(x.trim()),
            });
        }
        if let Some(x) = payload.strip_prefix("Upgraded program ") {
            return Some(Self::UpgradedProgramPlain {
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
        if let Some(x) = payload.strip_prefix("New authority ") {
            return Some(Self::NewAuthorityDebugPlain {
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
            Self::Static(log) => log.as_str().to_string(),
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
            Self::DeployedProgramPlain { program_key } => {
                format!("Deployed program {}", st.resolve(*program_key))
            }
            Self::UpgradedProgram { program_key } => {
                format!("Upgraded program: {}", st.resolve(*program_key))
            }
            Self::UpgradedProgramPlain { program_key } => {
                format!("Upgraded program {}", st.resolve(*program_key))
            }
            Self::NewAuthorityDebug { new_authority } => {
                format!("New authority: {}", st.resolve(*new_authority))
            }
            Self::NewAuthorityDebugPlain { new_authority } => {
                format!("New authority {}", st.resolve(*new_authority))
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
                format!("New authority: {}", st.resolve(*new_authority))
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
