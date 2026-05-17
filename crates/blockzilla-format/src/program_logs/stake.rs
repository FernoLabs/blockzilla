use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

pub const STR_ID: &str = "Stake11111111111111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum StakeProgramLog {
    Static(StakeStaticLog),
    Error { msg: StrId },
}

macro_rules! stake_static_logs {
    ($( $variant:ident => $text:literal, )+ ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
        pub enum StakeStaticLog {
            $( $variant, )+
        }

        impl StakeStaticLog {
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

stake_static_logs! {
    CheckingDestinationStakeMergeable => "Checking if destination stake is mergeable",
    CheckingSourceStakeMergeable => "Checking if source stake is mergeable",
    MergingStakeAccounts => "Merging stake accounts",
    DelegationCalculationsViolatedLamportBalanceAssumptions => "Delegation calculations violated lamport balance assumptions",
    UnableToMergeMetadataMismatch => "Unable to merge due to metadata mismatch",
    UnableToMergeVoterMismatch => "Unable to merge due to voter mismatch",
    UnableToMergeStakeDeactivation => "Unable to merge due to stake deactivation",
    InstructionInitialize => "Instruction: Initialize",
    InstructionAuthorize => "Instruction: Authorize",
    InstructionDelegateStake => "Instruction: DelegateStake",
    InstructionSplit => "Instruction: Split",
    InstructionWithdraw => "Instruction: Withdraw",
    InstructionDeactivate => "Instruction: Deactivate",
    InstructionSetLockup => "Instruction: SetLockup",
    InstructionMerge => "Instruction: Merge",
    InstructionAuthorizeWithSeed => "Instruction: AuthorizeWithSeed",
    InstructionInitializeChecked => "Instruction: InitializeChecked",
    InstructionAuthorizeChecked => "Instruction: AuthorizeChecked",
    InstructionAuthorizeCheckedWithSeed => "Instruction: AuthorizeCheckedWithSeed",
    InstructionSetLockupChecked => "Instruction: SetLockupChecked",
    InstructionGetMinimumDelegation => "Instruction: GetMinimumDelegation",
    InstructionDeactivateDelinquent => "Instruction: DeactivateDelinquent",
    InstructionMoveStake => "Instruction: MoveStake",
    InstructionMoveLamports => "Instruction: MoveLamports",
}

impl StakeProgramLog {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(log) = StakeStaticLog::parse(payload) {
            return Some(Self::Static(log));
        }

        if let Some(msg) = payload.strip_prefix("ERROR: ") {
            return Some(Self::Error { msg: st.push(msg) });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::Static(log) => log.as_str().to_string(),
            Self::Error { msg } => format!("ERROR: {}", st.resolve(*msg)),
        }
    }
}
