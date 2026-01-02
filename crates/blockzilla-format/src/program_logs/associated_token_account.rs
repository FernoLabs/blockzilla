use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

pub const STR_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum TokenErrorLog {
    InvalidSeeds,
    InvalidOwnerSeeds,
    InvalidNestedSeeds,
    InvalidDestinationSeeds,
    MissingRequiredSignature,
    IllegalMintOwner,
    IllegalAtaProgramOwner,

    IllegalNestedProgramOwner,
    IllegalNestedOwner,
    IllegalNestedMintOwner,
    IllegalAtaOwner,
    InvalidOwner,
}

impl TokenErrorLog {
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "Error: Associated address does not match seed derivation" => Some(Self::InvalidSeeds),
            "Error: Associated token account owner does not match address derivation" => {
                Some(Self::InvalidOwner)
            }
            "Error: Owner associated address does not match seed derivation" => {
                Some(Self::InvalidOwnerSeeds)
            }
            "Error: Nested associated address does not match seed derivation" => {
                Some(Self::InvalidNestedSeeds)
            }
            "Error: Destination associated address does not match seed derivation" => {
                Some(Self::InvalidDestinationSeeds)
            }
            "Wallet of the owner associated token account must sign" => {
                Some(Self::MissingRequiredSignature)
            }
            "Owner mint not owned by provided token program" => Some(Self::IllegalMintOwner),
            "Owner associated token account not owned by provided token program, recreate the owner associated token account first" => {
                Some(Self::IllegalAtaProgramOwner)
            }
            "Owner associated token account not owned by provided wallet" => {
                Some(Self::IllegalAtaOwner)
            }
            "Nested associated token account not owned by provided token program" => {
                Some(Self::IllegalNestedProgramOwner)
            }
            "Nested associated token account not owned by provided associated token account" => {
                Some(Self::IllegalNestedOwner)
            }
            "Nested mint account not owned by provided token program" => {
                Some(Self::IllegalNestedMintOwner)
            }
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidOwnerSeeds => {
                "Error: Owner associated address does not match seed derivation"
            }
            Self::InvalidSeeds => "Error: Associated address does not match seed derivation",
            Self::InvalidNestedSeeds => {
                "Error: Nested associated address does not match seed derivation"
            }
            Self::InvalidOwner => {
                "Error: Associated token account owner does not match address derivation"
            }
            Self::InvalidDestinationSeeds => {
                "Error: Destination associated address does not match seed derivation"
            }
            Self::MissingRequiredSignature => {
                "Wallet of the owner associated token account must sign"
            }
            Self::IllegalMintOwner => "Owner mint not owned by provided token program",
            Self::IllegalAtaProgramOwner => {
                "Owner associated token account not owned by provided token program, recreate the owner associated token account first"
            }
            Self::IllegalNestedProgramOwner => {
                "Nested associated token account not owned by provided token program"
            }
            Self::IllegalNestedOwner => {
                "Nested associated token account not owned by provided associated token account"
            }
            Self::IllegalAtaOwner => "Owner associated token account not owned by provided wallet",
            Self::IllegalNestedMintOwner => {
                "Nested mint account not owned by provided token program"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenLog {
    Error(TokenErrorLog),
    InstructionCreate,
    InstructionCreateIdempotent,
    InstructionRecoverNested,

    InitAta,
}

impl TokenLog {
    /// `text` is the payload after "Program log: "
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        if let Some(e) = TokenErrorLog::parse(text) {
            return Some(Self::Error(e));
        }

        if text == "Initialize the associated token account" {
            return Some(Self::InitAta);
        }

        let name = text.strip_prefix("Instruction: ")?.trim();
        match name {
            "Create" => Some(Self::InstructionCreate),
            "CreateIdempotent" => Some(Self::InstructionCreateIdempotent),
            "RecoverNested" => Some(Self::InstructionRecoverNested),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error(e) => e.as_str(),
            Self::InstructionCreate => "Instruction: Create",
            Self::InstructionCreateIdempotent => "Instruction: CreateIdempotent",
            Self::InstructionRecoverNested => "Instruction: RecoverNested",
            Self::InitAta => "Initialize the associated token account",
        }
    }
}
