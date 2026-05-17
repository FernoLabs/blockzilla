use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

pub const STR_ID: &str = "LoaderV411111111111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LoaderV4Log {
    Static(LoaderV4StaticLog),

    /// processor.rs:150 "Insufficient lamports, {} are required."
    InsufficientLamportsRequired {
        required_lamports: StrId,
    },
}

macro_rules! loader_v4_static_logs {
    ($( $variant:ident => $text:literal, )+ ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
        pub enum LoaderV4StaticLog {
            $( $variant, )+
        }

        impl LoaderV4StaticLog {
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

loader_v4_static_logs! {
    ProgramNotOwnedByLoader => "Program not owned by loader",
    ProgramIsNotWriteable => "Program is not writeable",
    AuthorityDidNotSign => "Authority did not sign",
    IncorrectAuthorityProvided => "Incorrect authority provided",
    ProgramIsFinalized => "Program is finalized",
    ProgramIsNotRetracted => "Program is not retracted",
    WriteOutOfBounds => "Write out of bounds",
    SourceIsNotAProgram => "Source is not a program",
    ReadOutOfBounds => "Read out of bounds",
    RecipientIsNotWriteable => "Recipient is not writeable",
    ClosingProgramRequiresRecipientAccount => "Closing a program requires a recipient account",
    ProgramWasDeployedRecentlyCooldownStillInEffect => "Program was deployed recently, cooldown still in effect",
    DestinationProgramIsNotRetracted => "Destination program is not retracted",
    ProgramIsNotDeployed => "Program is not deployed",
    NewAuthorityDidNotSign => "New authority did not sign",
    NoChange => "No change",
    ProgramMustBeDeployedToBeFinalized => "Program must be deployed to be finalized",
    NextVersionIsNotOwnedByLoader => "Next version is not owned by loader",
    NextVersionHasADifferentAuthority => "Next version has a different authority",
    NextVersionIsFinalized => "Next version is finalized",
    ProgramIsNotCached => "Program is not cached",
}

impl LoaderV4Log {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(log) = LoaderV4StaticLog::parse(payload) {
            return Some(Self::Static(log));
        }

        if let Some(x) = parse_one_braced(payload, "Insufficient lamports, ", " are required")
            .or_else(|| parse_one_braced(payload, "Insufficient lamports, ", " are required."))
        {
            return Some(Self::InsufficientLamportsRequired {
                required_lamports: st.push(x),
            });
        }
        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::Static(log) => log.as_str().to_string(),
            Self::InsufficientLamportsRequired { required_lamports } => format!(
                "Insufficient lamports, {} are required",
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
