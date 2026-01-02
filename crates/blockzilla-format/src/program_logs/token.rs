#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum TokenLog {
    Error(TokenErrorLog),
    PleaseUpgrade,

    // missing in parse/as_str previously
    GetAccountDataSize,

    // newly added from msg! list
    InstructionBatch,
    InstructionInitializeMint,
    InstructionInitializeMint2,
    InstructionInitializeAccount,
    InstructionInitializeAccount2,
    InstructionInitializeAccount3,
    InstructionInitializeMultisig,
    InstructionInitializeMultisig2,
    InstructionInitializeImmutableOwner,
    InstructionTransfer,
    InstructionTransferChecked,
    InstructionApprove,
    InstructionRevoke,
    InstructionSetAuthority,
    InstructionMintTo,
    InstructionMintToChecked,
    InstructionBurn,
    InstructionBurnChecked,
    InstructionCloseAccount,
    InstructionFreezeAccount,
    InstructionThawAccount,
    InstructionSyncNative,
    InstructionAmountToUiAmount,
    InstructionUiAmountToAmount,
    InstructionWithdrawExcessLamports,
    InstructionUnwrapLamports,
}

impl TokenLog {
    /// `text` is the payload after "Program log: "
    #[inline]
    pub fn parse(text: &str) -> Option<Self> {
        if let Some(e) = TokenErrorLog::parse(text) {
            return Some(Self::Error(e));
        }

        if text == "Please upgrade to SPL Token 2022 for immutable owner support" {
            return Some(Self::PleaseUpgrade);
        }

        let name = text.strip_prefix("Instruction: ")?.trim();
        match name {
            "Batch" => Some(Self::InstructionBatch),

            "InitializeMint" => Some(Self::InstructionInitializeMint),
            "InitializeMint2" => Some(Self::InstructionInitializeMint2),
            "InitializeAccount" => Some(Self::InstructionInitializeAccount),
            "InitializeAccount2" => Some(Self::InstructionInitializeAccount2),
            "InitializeAccount3" => Some(Self::InstructionInitializeAccount3),
            "InitializeMultisig" => Some(Self::InstructionInitializeMultisig),
            "InitializeMultisig2" => Some(Self::InstructionInitializeMultisig2),
            "InitializeImmutableOwner" => Some(Self::InstructionInitializeImmutableOwner),

            "GetAccountDataSize" => Some(Self::GetAccountDataSize),
            "AmountToUiAmount" => Some(Self::InstructionAmountToUiAmount),
            "UiAmountToAmount" => Some(Self::InstructionUiAmountToAmount),

            "Transfer" => Some(Self::InstructionTransfer),
            "TransferChecked" => Some(Self::InstructionTransferChecked),
            "Approve" => Some(Self::InstructionApprove),
            "Revoke" => Some(Self::InstructionRevoke),
            "SetAuthority" => Some(Self::InstructionSetAuthority),
            "MintTo" => Some(Self::InstructionMintTo),
            "MintToChecked" => Some(Self::InstructionMintToChecked),
            "Burn" => Some(Self::InstructionBurn),
            "BurnChecked" => Some(Self::InstructionBurnChecked),
            "CloseAccount" => Some(Self::InstructionCloseAccount),
            "FreezeAccount" => Some(Self::InstructionFreezeAccount),
            "ThawAccount" => Some(Self::InstructionThawAccount),
            "SyncNative" => Some(Self::InstructionSyncNative),

            "WithdrawExcessLamports" => Some(Self::InstructionWithdrawExcessLamports),
            "UnwrapLamports" => Some(Self::InstructionUnwrapLamports),

            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error(e) => e.as_str(),
            Self::PleaseUpgrade => "Please upgrade to SPL Token 2022 for immutable owner support",

            Self::GetAccountDataSize => "Instruction: GetAccountDataSize",

            Self::InstructionBatch => "Instruction: Batch",

            Self::InstructionInitializeMint => "Instruction: InitializeMint",
            Self::InstructionInitializeMint2 => "Instruction: InitializeMint2",
            Self::InstructionInitializeAccount => "Instruction: InitializeAccount",
            Self::InstructionInitializeAccount2 => "Instruction: InitializeAccount2",
            Self::InstructionInitializeAccount3 => "Instruction: InitializeAccount3",
            Self::InstructionInitializeMultisig => "Instruction: InitializeMultisig",
            Self::InstructionInitializeMultisig2 => "Instruction: InitializeMultisig2",
            Self::InstructionInitializeImmutableOwner => "Instruction: InitializeImmutableOwner",

            Self::InstructionAmountToUiAmount => "Instruction: AmountToUiAmount",
            Self::InstructionUiAmountToAmount => "Instruction: UiAmountToAmount",

            Self::InstructionTransfer => "Instruction: Transfer",
            Self::InstructionTransferChecked => "Instruction: TransferChecked",
            Self::InstructionApprove => "Instruction: Approve",
            Self::InstructionRevoke => "Instruction: Revoke",
            Self::InstructionSetAuthority => "Instruction: SetAuthority",
            Self::InstructionMintTo => "Instruction: MintTo",
            Self::InstructionMintToChecked => "Instruction: MintToChecked",
            Self::InstructionBurn => "Instruction: Burn",
            Self::InstructionBurnChecked => "Instruction: BurnChecked",
            Self::InstructionCloseAccount => "Instruction: CloseAccount",
            Self::InstructionFreezeAccount => "Instruction: FreezeAccount",
            Self::InstructionThawAccount => "Instruction: ThawAccount",
            Self::InstructionSyncNative => "Instruction: SyncNative",

            Self::InstructionWithdrawExcessLamports => "Instruction: WithdrawExcessLamports",
            Self::InstructionUnwrapLamports => "Instruction: UnwrapLamports",
        }
    }
}
