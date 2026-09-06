//! Allocation-free SPL Token instruction classification.
//!
//! The discriminator tables in this module follow `spl-token-interface`
//! 2.0.0, `spl-token-2022-interface` 3.1.1,
//! `spl-token-metadata-interface` 0.8.0, and
//! `spl-token-group-interface` 0.7.2. Decoding keeps the complete input as a
//! borrowed slice. Unknown, future, and malformed instructions are therefore
//! still available to the caller and can be written to a lossless event
//! stream.
//!
//! This is a classifier, not an execution-validity checker.  It validates the
//! framing and the public amount fields that it reads, but it does not validate
//! every proof or extension payload byte.  A reducer must also use successful
//! transaction status and account positions before it applies an effect.

/// The token program whose instruction data is being decoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenProgram {
    /// The original SPL Token program.
    Legacy,
    /// The SPL Token-2022 program.
    Token2022,
}

/// The effect that an instruction can have on a public token amount.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InstructionEffect {
    /// The instruction has no public token-balance effect.
    NoPublicBalanceEffect,
    /// The instruction changes token state but does not directly change a
    /// public base-unit amount.
    StateRelevant,
    /// The instruction can create, remove, or change a public base-unit
    /// amount.
    BalanceRelevant,
}

impl InstructionEffect {
    /// Combines effects without losing the most important effect.
    #[must_use]
    pub const fn combine(self, other: Self) -> Self {
        if (self as u8) >= (other as u8) {
            self
        } else {
            other
        }
    }
}

/// The result of discriminator and framing classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeStatus {
    /// The discriminator is known and all fields used by this classifier are
    /// present.
    Known,
    /// The discriminator is known, but required framing or a required public
    /// amount field is invalid or truncated.
    Malformed,
    /// The top-level discriminator is not known for the selected program.
    UnknownTopLevel,
    /// The top-level extension discriminator is known, but its subtype is not.
    UnknownExtensionSubtype,
    /// A valid batch contains an unknown child instruction.
    BatchContainsUnknown,
}

impl DecodeStatus {
    /// Returns true only when the complete classification is known.
    #[must_use]
    pub const fn is_known(self) -> bool {
        matches!(self, Self::Known)
    }
}

/// A known top-level SPL Token instruction discriminator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TopLevelInstruction {
    InitializeMint = 0,
    InitializeAccount = 1,
    InitializeMultisig = 2,
    Transfer = 3,
    Approve = 4,
    Revoke = 5,
    SetAuthority = 6,
    MintTo = 7,
    Burn = 8,
    CloseAccount = 9,
    FreezeAccount = 10,
    ThawAccount = 11,
    TransferChecked = 12,
    ApproveChecked = 13,
    MintToChecked = 14,
    BurnChecked = 15,
    InitializeAccount2 = 16,
    SyncNative = 17,
    InitializeAccount3 = 18,
    InitializeMultisig2 = 19,
    InitializeMint2 = 20,
    GetAccountDataSize = 21,
    InitializeImmutableOwner = 22,
    AmountToUiAmount = 23,
    UiAmountToAmount = 24,
    InitializeMintCloseAuthority = 25,
    TransferFeeExtension = 26,
    ConfidentialTransferExtension = 27,
    DefaultAccountStateExtension = 28,
    Reallocate = 29,
    MemoTransferExtension = 30,
    CreateNativeMint = 31,
    InitializeNonTransferableMint = 32,
    InterestBearingMintExtension = 33,
    CpiGuardExtension = 34,
    InitializePermanentDelegate = 35,
    TransferHookExtension = 36,
    ConfidentialTransferFeeExtension = 37,
    WithdrawExcessLamports = 38,
    MetadataPointerExtension = 39,
    GroupPointerExtension = 40,
    GroupMemberPointerExtension = 41,
    ConfidentialMintBurnExtension = 42,
    ScaledUiAmountExtension = 43,
    PausableExtension = 44,
    UnwrapLamports = 45,
    PermissionedBurnExtension = 46,
    Batch = 255,
}

impl TopLevelInstruction {
    /// Returns the on-chain discriminator.
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Returns the stable official instruction name without allocating.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::InitializeMint => "InitializeMint",
            Self::InitializeAccount => "InitializeAccount",
            Self::InitializeMultisig => "InitializeMultisig",
            Self::Transfer => "Transfer",
            Self::Approve => "Approve",
            Self::Revoke => "Revoke",
            Self::SetAuthority => "SetAuthority",
            Self::MintTo => "MintTo",
            Self::Burn => "Burn",
            Self::CloseAccount => "CloseAccount",
            Self::FreezeAccount => "FreezeAccount",
            Self::ThawAccount => "ThawAccount",
            Self::TransferChecked => "TransferChecked",
            Self::ApproveChecked => "ApproveChecked",
            Self::MintToChecked => "MintToChecked",
            Self::BurnChecked => "BurnChecked",
            Self::InitializeAccount2 => "InitializeAccount2",
            Self::SyncNative => "SyncNative",
            Self::InitializeAccount3 => "InitializeAccount3",
            Self::InitializeMultisig2 => "InitializeMultisig2",
            Self::InitializeMint2 => "InitializeMint2",
            Self::GetAccountDataSize => "GetAccountDataSize",
            Self::InitializeImmutableOwner => "InitializeImmutableOwner",
            Self::AmountToUiAmount => "AmountToUiAmount",
            Self::UiAmountToAmount => "UiAmountToAmount",
            Self::InitializeMintCloseAuthority => "InitializeMintCloseAuthority",
            Self::TransferFeeExtension => "TransferFeeExtension",
            Self::ConfidentialTransferExtension => "ConfidentialTransferExtension",
            Self::DefaultAccountStateExtension => "DefaultAccountStateExtension",
            Self::Reallocate => "Reallocate",
            Self::MemoTransferExtension => "MemoTransferExtension",
            Self::CreateNativeMint => "CreateNativeMint",
            Self::InitializeNonTransferableMint => "InitializeNonTransferableMint",
            Self::InterestBearingMintExtension => "InterestBearingMintExtension",
            Self::CpiGuardExtension => "CpiGuardExtension",
            Self::InitializePermanentDelegate => "InitializePermanentDelegate",
            Self::TransferHookExtension => "TransferHookExtension",
            Self::ConfidentialTransferFeeExtension => "ConfidentialTransferFeeExtension",
            Self::WithdrawExcessLamports => "WithdrawExcessLamports",
            Self::MetadataPointerExtension => "MetadataPointerExtension",
            Self::GroupPointerExtension => "GroupPointerExtension",
            Self::GroupMemberPointerExtension => "GroupMemberPointerExtension",
            Self::ConfidentialMintBurnExtension => "ConfidentialMintBurnExtension",
            Self::ScaledUiAmountExtension => "ScaledUiAmountExtension",
            Self::PausableExtension => "PausableExtension",
            Self::UnwrapLamports => "UnwrapLamports",
            Self::PermissionedBurnExtension => "PermissionedBurnExtension",
            Self::Batch => "Batch",
        }
    }

    /// Returns the extension family for prefix instructions.
    #[must_use]
    pub const fn extension_family(self) -> Option<ExtensionFamily> {
        match self {
            Self::TransferFeeExtension => Some(ExtensionFamily::TransferFee),
            Self::ConfidentialTransferExtension => Some(ExtensionFamily::ConfidentialTransfer),
            Self::DefaultAccountStateExtension => Some(ExtensionFamily::DefaultAccountState),
            Self::MemoTransferExtension => Some(ExtensionFamily::MemoTransfer),
            Self::InterestBearingMintExtension => Some(ExtensionFamily::InterestBearingMint),
            Self::CpiGuardExtension => Some(ExtensionFamily::CpiGuard),
            Self::TransferHookExtension => Some(ExtensionFamily::TransferHook),
            Self::ConfidentialTransferFeeExtension => {
                Some(ExtensionFamily::ConfidentialTransferFee)
            }
            Self::MetadataPointerExtension => Some(ExtensionFamily::MetadataPointer),
            Self::GroupPointerExtension => Some(ExtensionFamily::GroupPointer),
            Self::GroupMemberPointerExtension => Some(ExtensionFamily::GroupMemberPointer),
            Self::ConfidentialMintBurnExtension => Some(ExtensionFamily::ConfidentialMintBurn),
            Self::ScaledUiAmountExtension => Some(ExtensionFamily::ScaledUiAmount),
            Self::PausableExtension => Some(ExtensionFamily::Pausable),
            Self::PermissionedBurnExtension => Some(ExtensionFamily::PermissionedBurn),
            _ => None,
        }
    }
}

/// A Token-2022 extension instruction family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtensionFamily {
    TransferFee,
    ConfidentialTransfer,
    DefaultAccountState,
    MemoTransfer,
    InterestBearingMint,
    CpiGuard,
    TransferHook,
    ConfidentialTransferFee,
    MetadataPointer,
    GroupPointer,
    GroupMemberPointer,
    ConfidentialMintBurn,
    ScaledUiAmount,
    Pausable,
    PermissionedBurn,
}

/// An interface instruction family dispatched by the Token-2022 program.
///
/// These instructions use an eight-byte SPL discriminator instead of the
/// one-byte [`TopLevelInstruction`] discriminator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Token2022InterfaceFamily {
    TokenMetadata,
    TokenGroup,
}

impl Token2022InterfaceFamily {
    /// Returns a stable family name without allocating.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::TokenMetadata => "TokenMetadata",
            Self::TokenGroup => "TokenGroup",
        }
    }
}

/// A known SPL interface instruction dispatched by the Token-2022 program.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Token2022InterfaceInstruction {
    /// The interface that owns the discriminator.
    pub family: Token2022InterfaceFamily,
    /// The complete eight-byte SPL discriminator.
    pub discriminator: [u8; 8],
    /// The official interface instruction name.
    pub name: &'static str,
}

impl Token2022InterfaceInstruction {
    const fn new(
        family: Token2022InterfaceFamily,
        discriminator: [u8; 8],
        name: &'static str,
    ) -> Self {
        Self {
            family,
            discriminator,
            name,
        }
    }
}

const TOKEN_METADATA_INITIALIZE_DISCRIMINATOR: [u8; 8] =
    [0xd2, 0xe1, 0x1e, 0xa2, 0x58, 0xb8, 0x4d, 0x8d];
const TOKEN_METADATA_UPDATE_FIELD_DISCRIMINATOR: [u8; 8] =
    [0xdd, 0xe9, 0x31, 0x2d, 0xb5, 0xca, 0xdc, 0xc8];
const TOKEN_METADATA_REMOVE_KEY_DISCRIMINATOR: [u8; 8] =
    [0xea, 0x12, 0x20, 0x38, 0x59, 0x8d, 0x25, 0xb5];
const TOKEN_METADATA_UPDATE_AUTHORITY_DISCRIMINATOR: [u8; 8] =
    [0xd7, 0xe4, 0xa6, 0xe4, 0x54, 0x64, 0x56, 0x7b];
const TOKEN_METADATA_EMIT_DISCRIMINATOR: [u8; 8] = [0xfa, 0xa6, 0xb4, 0xfa, 0x0d, 0x0c, 0xb8, 0x46];

const TOKEN_GROUP_INITIALIZE_GROUP_DISCRIMINATOR: [u8; 8] =
    [0x79, 0x71, 0x6c, 0x27, 0x36, 0x33, 0x00, 0x04];
const TOKEN_GROUP_UPDATE_MAX_SIZE_DISCRIMINATOR: [u8; 8] =
    [0x6c, 0x25, 0xab, 0x8f, 0xf8, 0x1e, 0x12, 0x6e];
const TOKEN_GROUP_UPDATE_AUTHORITY_DISCRIMINATOR: [u8; 8] =
    [0xa1, 0x69, 0x58, 0x01, 0xed, 0xdd, 0xd8, 0xcb];
const TOKEN_GROUP_INITIALIZE_MEMBER_DISCRIMINATOR: [u8; 8] =
    [0x98, 0x20, 0xde, 0xb0, 0xdf, 0xed, 0x74, 0x86];

impl ExtensionFamily {
    /// Returns a stable family name without allocating.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::TransferFee => "TransferFee",
            Self::ConfidentialTransfer => "ConfidentialTransfer",
            Self::DefaultAccountState => "DefaultAccountState",
            Self::MemoTransfer => "MemoTransfer",
            Self::InterestBearingMint => "InterestBearingMint",
            Self::CpiGuard => "CpiGuard",
            Self::TransferHook => "TransferHook",
            Self::ConfidentialTransferFee => "ConfidentialTransferFee",
            Self::MetadataPointer => "MetadataPointer",
            Self::GroupPointer => "GroupPointer",
            Self::GroupMemberPointer => "GroupMemberPointer",
            Self::ConfidentialMintBurn => "ConfidentialMintBurn",
            Self::ScaledUiAmount => "ScaledUiAmount",
            Self::Pausable => "Pausable",
            Self::PermissionedBurn => "PermissionedBurn",
        }
    }
}

/// A known extension subtype.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtensionInstruction {
    /// The top-level extension family.
    pub family: ExtensionFamily,
    /// The extension-local discriminator byte.
    pub subtype: u8,
    /// The official extension instruction name.
    pub name: &'static str,
}

impl ExtensionInstruction {
    const fn new(family: ExtensionFamily, subtype: u8, name: &'static str) -> Self {
        Self {
            family,
            subtype,
            name,
        }
    }
}

/// A borrowed, lossless instruction classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedInstruction<'a> {
    /// The selected token program.
    pub program: TokenProgram,
    /// The complete original instruction data.  This slice is never shortened.
    pub raw: &'a [u8],
    /// The known top-level instruction, if one exists for the selected program.
    pub top_level: Option<TopLevelInstruction>,
    /// The known extension subtype, if this is an extension instruction.
    pub extension: Option<ExtensionInstruction>,
    /// The known SPL interface instruction, if Token-2022 dispatched this
    /// instruction through its metadata or token-group interface.
    pub interface: Option<Token2022InterfaceInstruction>,
    /// The conservative public-balance effect.
    pub effect: InstructionEffect,
    /// The discriminator and framing status.
    pub status: DecodeStatus,
    /// A decoded public amount when the instruction has a clear public `u64`
    /// amount field.
    pub amount: Option<u64>,
    /// A decoded decimals byte when it accompanies `amount`.
    pub decimals: Option<u8>,
}

impl<'a> DecodedInstruction<'a> {
    /// Returns the first raw byte, including an unknown discriminator or the
    /// first byte of an interface discriminator.
    #[must_use]
    pub const fn top_level_tag(self) -> Option<u8> {
        self.raw.first().copied()
    }

    /// Returns the raw subtype for a known extension family, including an
    /// unknown subtype.
    #[must_use]
    pub fn extension_subtype(self) -> Option<u8> {
        self.top_level
            .and_then(TopLevelInstruction::extension_family)
            .and_then(|_| self.raw.get(1).copied())
    }

    /// Returns the complete discriminator for a known Token-2022 interface
    /// instruction.
    #[must_use]
    pub const fn interface_discriminator(self) -> Option<[u8; 8]> {
        match self.interface {
            Some(interface) => Some(interface.discriminator),
            None => None,
        }
    }

    /// Returns the most precise allocation-free name available.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self.interface {
            Some(interface) => interface.name,
            None => match self.extension {
                Some(extension) => extension.name,
                None => match self.top_level {
                    Some(top_level) => top_level.name(),
                    None => "Unknown",
                },
            },
        }
    }

    /// Returns a borrowed iterator over Batch (255) children.
    #[must_use]
    pub fn batch_items(self) -> Option<BatchIter<'a>> {
        if self.top_level == Some(TopLevelInstruction::Batch) {
            Some(BatchIter {
                program: self.program,
                remaining: &self.raw[1..],
                offset: 1,
                finished: false,
            })
        } else {
            None
        }
    }
}

/// One borrowed child of a Token-2022 Batch instruction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchItem<'a> {
    /// Byte offset of the child header in the complete outer instruction.
    pub offset: usize,
    /// Number of accounts assigned to this child.
    pub account_count: u8,
    /// The complete, borrowed child instruction data.
    pub raw: &'a [u8],
    /// The child classification.
    pub instruction: DecodedInstruction<'a>,
}

/// A Batch framing error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchDecodeError {
    /// Byte offset of the bad child header in the complete outer instruction.
    pub offset: usize,
    /// The framing error.
    pub kind: BatchDecodeErrorKind,
}

/// The reason why Batch framing is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchDecodeErrorKind {
    /// Fewer than two child-header bytes remain.
    TruncatedHeader,
    /// A child declares an instruction-data length of zero.
    EmptyInstruction,
    /// The child data is shorter than its declared length.
    TruncatedInstruction { declared: u8, available: usize },
    /// Token-2022 does not permit a Batch child to contain another Batch.
    NestedBatch,
}

/// Allocation-free iterator over the children of a Token-2022 Batch.
#[derive(Clone, Debug)]
pub struct BatchIter<'a> {
    program: TokenProgram,
    remaining: &'a [u8],
    offset: usize,
    finished: bool,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = Result<BatchItem<'a>, BatchDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.remaining.is_empty() {
            return None;
        }

        let offset = self.offset;
        if self.remaining.len() < 2 {
            self.finished = true;
            return Some(Err(BatchDecodeError {
                offset,
                kind: BatchDecodeErrorKind::TruncatedHeader,
            }));
        }

        let account_count = self.remaining[0];
        let declared = self.remaining[1];
        if declared == 0 {
            self.finished = true;
            return Some(Err(BatchDecodeError {
                offset,
                kind: BatchDecodeErrorKind::EmptyInstruction,
            }));
        }

        let available = self.remaining.len() - 2;
        if available < declared as usize {
            self.finished = true;
            return Some(Err(BatchDecodeError {
                offset,
                kind: BatchDecodeErrorKind::TruncatedInstruction {
                    declared,
                    available,
                },
            }));
        }

        let child_raw = &self.remaining[2..2 + declared as usize];
        if child_raw.first() == Some(&(TopLevelInstruction::Batch as u8)) {
            self.finished = true;
            return Some(Err(BatchDecodeError {
                offset,
                kind: BatchDecodeErrorKind::NestedBatch,
            }));
        }

        self.remaining = &self.remaining[2 + declared as usize..];
        self.offset += 2 + declared as usize;
        Some(Ok(BatchItem {
            offset,
            account_count,
            raw: child_raw,
            instruction: decode_token_instruction(self.program, child_raw),
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.finished || self.remaining.is_empty() {
            (0, Some(0))
        } else {
            // Each item needs at least a two-byte header and one data byte.
            // A final malformed fragment can also yield one error item.
            (1, Some(self.remaining.len().div_ceil(3)))
        }
    }
}

/// Decodes and classifies one SPL Token instruction without allocating.
///
/// Unknown and malformed instructions use `BalanceRelevant` as a conservative
/// effect.  This rule prevents a filter from silently excluding future
/// instructions that can change balances.  Callers must inspect `status` and
/// keep the raw bytes.
#[must_use]
pub fn decode_token_instruction(program: TokenProgram, raw: &[u8]) -> DecodedInstruction<'_> {
    if program == TokenProgram::Token2022
        && let Some(decoded) = decode_token_2022_interface(raw)
    {
        return decoded;
    }

    let Some(&tag) = raw.first() else {
        return unknown(program, raw, DecodeStatus::Malformed);
    };
    let Some(top_level) = top_level_from_tag(program, tag) else {
        return unknown(program, raw, DecodeStatus::UnknownTopLevel);
    };

    if top_level == TopLevelInstruction::Batch {
        return decode_batch(program, raw);
    }

    if let Some(family) = top_level.extension_family() {
        return decode_extension(program, raw, top_level, family);
    }

    let (effect, amount, decimals, valid) = classify_top_level(program, top_level, raw);
    DecodedInstruction {
        program,
        raw,
        top_level: Some(top_level),
        extension: None,
        interface: None,
        effect,
        status: if valid {
            DecodeStatus::Known
        } else {
            DecodeStatus::Malformed
        },
        amount,
        decimals,
    }
}

/// Short alias for [`decode_token_instruction`].
#[must_use]
pub fn decode(program: TokenProgram, raw: &[u8]) -> DecodedInstruction<'_> {
    decode_token_instruction(program, raw)
}

fn decode_token_2022_interface(raw: &[u8]) -> Option<DecodedInstruction<'_>> {
    use InstructionEffect::{NoPublicBalanceEffect, StateRelevant};
    use Token2022InterfaceFamily::{TokenGroup, TokenMetadata};

    let discriminator: [u8; 8] = raw.get(..8)?.try_into().ok()?;
    let (family, name, effect) = match discriminator {
        TOKEN_METADATA_INITIALIZE_DISCRIMINATOR => (TokenMetadata, "Initialize", StateRelevant),
        TOKEN_METADATA_UPDATE_FIELD_DISCRIMINATOR => (TokenMetadata, "UpdateField", StateRelevant),
        TOKEN_METADATA_REMOVE_KEY_DISCRIMINATOR => (TokenMetadata, "RemoveKey", StateRelevant),
        TOKEN_METADATA_UPDATE_AUTHORITY_DISCRIMINATOR => {
            (TokenMetadata, "UpdateAuthority", StateRelevant)
        }
        TOKEN_METADATA_EMIT_DISCRIMINATOR => (TokenMetadata, "Emit", NoPublicBalanceEffect),
        TOKEN_GROUP_INITIALIZE_GROUP_DISCRIMINATOR => {
            (TokenGroup, "InitializeGroup", StateRelevant)
        }
        TOKEN_GROUP_UPDATE_MAX_SIZE_DISCRIMINATOR => {
            (TokenGroup, "UpdateGroupMaxSize", StateRelevant)
        }
        TOKEN_GROUP_UPDATE_AUTHORITY_DISCRIMINATOR => {
            (TokenGroup, "UpdateGroupAuthority", StateRelevant)
        }
        TOKEN_GROUP_INITIALIZE_MEMBER_DISCRIMINATOR => {
            (TokenGroup, "InitializeMember", StateRelevant)
        }
        _ => return None,
    };

    Some(DecodedInstruction {
        program: TokenProgram::Token2022,
        raw,
        top_level: None,
        extension: None,
        interface: Some(Token2022InterfaceInstruction::new(
            family,
            discriminator,
            name,
        )),
        effect,
        status: DecodeStatus::Known,
        amount: None,
        decimals: None,
    })
}

fn unknown<'a>(
    program: TokenProgram,
    raw: &'a [u8],
    status: DecodeStatus,
) -> DecodedInstruction<'a> {
    DecodedInstruction {
        program,
        raw,
        top_level: None,
        extension: None,
        interface: None,
        // Unknown instructions are retained by balance-first filters.
        effect: InstructionEffect::BalanceRelevant,
        status,
        amount: None,
        decimals: None,
    }
}

fn top_level_from_tag(program: TokenProgram, tag: u8) -> Option<TopLevelInstruction> {
    let instruction = match tag {
        0 => TopLevelInstruction::InitializeMint,
        1 => TopLevelInstruction::InitializeAccount,
        2 => TopLevelInstruction::InitializeMultisig,
        3 => TopLevelInstruction::Transfer,
        4 => TopLevelInstruction::Approve,
        5 => TopLevelInstruction::Revoke,
        6 => TopLevelInstruction::SetAuthority,
        7 => TopLevelInstruction::MintTo,
        8 => TopLevelInstruction::Burn,
        9 => TopLevelInstruction::CloseAccount,
        10 => TopLevelInstruction::FreezeAccount,
        11 => TopLevelInstruction::ThawAccount,
        12 => TopLevelInstruction::TransferChecked,
        13 => TopLevelInstruction::ApproveChecked,
        14 => TopLevelInstruction::MintToChecked,
        15 => TopLevelInstruction::BurnChecked,
        16 => TopLevelInstruction::InitializeAccount2,
        17 => TopLevelInstruction::SyncNative,
        18 => TopLevelInstruction::InitializeAccount3,
        19 => TopLevelInstruction::InitializeMultisig2,
        20 => TopLevelInstruction::InitializeMint2,
        21 => TopLevelInstruction::GetAccountDataSize,
        22 => TopLevelInstruction::InitializeImmutableOwner,
        23 => TopLevelInstruction::AmountToUiAmount,
        24 => TopLevelInstruction::UiAmountToAmount,
        25 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::InitializeMintCloseAuthority
        }
        26 if program == TokenProgram::Token2022 => TopLevelInstruction::TransferFeeExtension,
        27 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::ConfidentialTransferExtension
        }
        28 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::DefaultAccountStateExtension
        }
        29 if program == TokenProgram::Token2022 => TopLevelInstruction::Reallocate,
        30 if program == TokenProgram::Token2022 => TopLevelInstruction::MemoTransferExtension,
        31 if program == TokenProgram::Token2022 => TopLevelInstruction::CreateNativeMint,
        32 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::InitializeNonTransferableMint
        }
        33 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::InterestBearingMintExtension
        }
        34 if program == TokenProgram::Token2022 => TopLevelInstruction::CpiGuardExtension,
        35 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::InitializePermanentDelegate
        }
        36 if program == TokenProgram::Token2022 => TopLevelInstruction::TransferHookExtension,
        37 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::ConfidentialTransferFeeExtension
        }
        38 if program == TokenProgram::Token2022 => TopLevelInstruction::WithdrawExcessLamports,
        39 if program == TokenProgram::Token2022 => TopLevelInstruction::MetadataPointerExtension,
        40 if program == TokenProgram::Token2022 => TopLevelInstruction::GroupPointerExtension,
        41 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::GroupMemberPointerExtension
        }
        42 if program == TokenProgram::Token2022 => {
            TopLevelInstruction::ConfidentialMintBurnExtension
        }
        43 if program == TokenProgram::Token2022 => TopLevelInstruction::ScaledUiAmountExtension,
        44 if program == TokenProgram::Token2022 => TopLevelInstruction::PausableExtension,
        45 if program == TokenProgram::Token2022 => TopLevelInstruction::UnwrapLamports,
        46 if program == TokenProgram::Token2022 => TopLevelInstruction::PermissionedBurnExtension,
        255 if program == TokenProgram::Token2022 => TopLevelInstruction::Batch,
        _ => return None,
    };
    Some(instruction)
}

fn classify_top_level(
    program: TokenProgram,
    instruction: TopLevelInstruction,
    raw: &[u8],
) -> (InstructionEffect, Option<u64>, Option<u8>, bool) {
    use InstructionEffect::{BalanceRelevant, NoPublicBalanceEffect, StateRelevant};
    use TopLevelInstruction as I;

    let mut amount = None;
    let mut decimals = None;
    let valid = match instruction {
        I::InitializeMint | I::InitializeMint2 => {
            decimals = raw.get(1).copied();
            valid_address_option(raw, 34)
        }
        I::InitializeAccount | I::CloseAccount | I::SyncNative => true,
        I::InitializeMultisig | I::InitializeMultisig2 => raw.get(1).is_some(),
        I::Transfer | I::Approve | I::MintTo | I::Burn | I::AmountToUiAmount => {
            amount = read_u64(raw, 1);
            amount.is_some()
        }
        I::Revoke
        | I::FreezeAccount
        | I::ThawAccount
        | I::InitializeImmutableOwner
        | I::CreateNativeMint
        | I::InitializeNonTransferableMint
        | I::WithdrawExcessLamports => true,
        I::SetAuthority => raw.get(1).is_some() && valid_address_option(raw, 2),
        I::TransferChecked | I::ApproveChecked | I::MintToChecked | I::BurnChecked => {
            amount = read_u64(raw, 1);
            decimals = raw.get(9).copied();
            amount.is_some() && decimals.is_some()
        }
        I::InitializeAccount2 | I::InitializeAccount3 | I::InitializePermanentDelegate => {
            raw.len() >= 33
        }
        I::GetAccountDataSize => {
            program == TokenProgram::Legacy || (raw.len() - 1).is_multiple_of(2)
        }
        I::UiAmountToAmount => core::str::from_utf8(&raw[1..]).is_ok(),
        I::InitializeMintCloseAuthority => valid_address_option(raw, 1),
        I::Reallocate => (raw.len() - 1).is_multiple_of(2),
        I::UnwrapLamports => match raw.get(1) {
            Some(0) => true,
            Some(1) => {
                amount = read_u64(raw, 2);
                amount.is_some()
            }
            _ => false,
        },
        I::TransferFeeExtension
        | I::ConfidentialTransferExtension
        | I::DefaultAccountStateExtension
        | I::MemoTransferExtension
        | I::InterestBearingMintExtension
        | I::CpiGuardExtension
        | I::TransferHookExtension
        | I::ConfidentialTransferFeeExtension
        | I::MetadataPointerExtension
        | I::GroupPointerExtension
        | I::GroupMemberPointerExtension
        | I::ConfidentialMintBurnExtension
        | I::ScaledUiAmountExtension
        | I::PausableExtension
        | I::PermissionedBurnExtension
        | I::Batch => unreachable!("prefix and batch instructions are classified separately"),
    };

    let effect = match instruction {
        I::InitializeAccount
        | I::Transfer
        | I::MintTo
        | I::Burn
        | I::CloseAccount
        | I::TransferChecked
        | I::MintToChecked
        | I::BurnChecked
        | I::InitializeAccount2
        | I::SyncNative
        | I::InitializeAccount3
        | I::UnwrapLamports => BalanceRelevant,
        I::GetAccountDataSize | I::AmountToUiAmount | I::UiAmountToAmount => NoPublicBalanceEffect,
        // This moves lamports only.  It does not change the token amount.
        I::WithdrawExcessLamports => NoPublicBalanceEffect,
        _ => StateRelevant,
    };
    (effect, amount, decimals, valid)
}

fn decode_extension<'a>(
    program: TokenProgram,
    raw: &'a [u8],
    top_level: TopLevelInstruction,
    family: ExtensionFamily,
) -> DecodedInstruction<'a> {
    let Some(&subtype) = raw.get(1) else {
        return DecodedInstruction {
            program,
            raw,
            top_level: Some(top_level),
            extension: None,
            interface: None,
            effect: InstructionEffect::BalanceRelevant,
            status: DecodeStatus::Malformed,
            amount: None,
            decimals: None,
        };
    };
    let Some((extension, effect)) = extension_from_subtype(family, subtype) else {
        return DecodedInstruction {
            program,
            raw,
            top_level: Some(top_level),
            extension: None,
            interface: None,
            effect: InstructionEffect::BalanceRelevant,
            status: DecodeStatus::UnknownExtensionSubtype,
            amount: None,
            decimals: None,
        };
    };

    let (amount, decimals, valid) = extension_public_fields(extension, raw);
    DecodedInstruction {
        program,
        raw,
        top_level: Some(top_level),
        extension: Some(extension),
        interface: None,
        effect,
        status: if valid {
            DecodeStatus::Known
        } else {
            DecodeStatus::Malformed
        },
        amount,
        decimals,
    }
}

fn extension_from_subtype(
    family: ExtensionFamily,
    subtype: u8,
) -> Option<(ExtensionInstruction, InstructionEffect)> {
    use ExtensionFamily as F;
    use InstructionEffect::{BalanceRelevant, StateRelevant};

    let (name, effect) = match (family, subtype) {
        (F::TransferFee, 0) => ("InitializeTransferFeeConfig", StateRelevant),
        (F::TransferFee, 1) => ("TransferCheckedWithFee", BalanceRelevant),
        (F::TransferFee, 2) => ("WithdrawWithheldTokensFromMint", BalanceRelevant),
        (F::TransferFee, 3) => ("WithdrawWithheldTokensFromAccounts", BalanceRelevant),
        (F::TransferFee, 4) => ("HarvestWithheldTokensToMint", StateRelevant),
        (F::TransferFee, 5) => ("SetTransferFee", StateRelevant),

        (F::ConfidentialTransfer, 0) => ("InitializeMint", StateRelevant),
        (F::ConfidentialTransfer, 1) => ("UpdateMint", StateRelevant),
        (F::ConfidentialTransfer, 2) => ("ConfigureAccount", StateRelevant),
        (F::ConfidentialTransfer, 3) => ("ApproveAccount", StateRelevant),
        (F::ConfidentialTransfer, 4) => ("EmptyAccount", StateRelevant),
        (F::ConfidentialTransfer, 5) => ("Deposit", BalanceRelevant),
        (F::ConfidentialTransfer, 6) => ("Withdraw", BalanceRelevant),
        (F::ConfidentialTransfer, 7) => ("Transfer", StateRelevant),
        (F::ConfidentialTransfer, 8) => ("ApplyPendingBalance", StateRelevant),
        (F::ConfidentialTransfer, 9) => ("EnableConfidentialCredits", StateRelevant),
        (F::ConfidentialTransfer, 10) => ("DisableConfidentialCredits", StateRelevant),
        (F::ConfidentialTransfer, 11) => ("EnableNonConfidentialCredits", StateRelevant),
        (F::ConfidentialTransfer, 12) => ("DisableNonConfidentialCredits", StateRelevant),
        (F::ConfidentialTransfer, 13) => ("TransferWithFee", StateRelevant),
        (F::ConfidentialTransfer, 14) => ("ConfigureAccountWithRegistry", StateRelevant),

        (F::DefaultAccountState, 0) => ("Initialize", StateRelevant),
        (F::DefaultAccountState, 1) => ("Update", StateRelevant),
        (F::MemoTransfer, 0) => ("Enable", StateRelevant),
        (F::MemoTransfer, 1) => ("Disable", StateRelevant),
        (F::InterestBearingMint, 0) => ("Initialize", StateRelevant),
        (F::InterestBearingMint, 1) => ("UpdateRate", StateRelevant),
        (F::CpiGuard, 0) => ("Enable", StateRelevant),
        (F::CpiGuard, 1) => ("Disable", StateRelevant),
        (F::TransferHook, 0) => ("Initialize", StateRelevant),
        (F::TransferHook, 1) => ("Update", StateRelevant),

        (F::ConfidentialTransferFee, 0) => {
            ("InitializeConfidentialTransferFeeConfig", StateRelevant)
        }
        (F::ConfidentialTransferFee, 1) => ("WithdrawWithheldTokensFromMint", StateRelevant),
        (F::ConfidentialTransferFee, 2) => ("WithdrawWithheldTokensFromAccounts", StateRelevant),
        (F::ConfidentialTransferFee, 3) => ("HarvestWithheldTokensToMint", StateRelevant),
        (F::ConfidentialTransferFee, 4) => ("EnableHarvestToMint", StateRelevant),
        (F::ConfidentialTransferFee, 5) => ("DisableHarvestToMint", StateRelevant),

        (F::MetadataPointer, 0) => ("Initialize", StateRelevant),
        (F::MetadataPointer, 1) => ("Update", StateRelevant),
        (F::GroupPointer, 0) => ("Initialize", StateRelevant),
        (F::GroupPointer, 1) => ("Update", StateRelevant),
        (F::GroupMemberPointer, 0) => ("Initialize", StateRelevant),
        (F::GroupMemberPointer, 1) => ("Update", StateRelevant),

        (F::ConfidentialMintBurn, 0) => ("InitializeMint", StateRelevant),
        (F::ConfidentialMintBurn, 1) => ("RotateSupplyElGamalPubkey", StateRelevant),
        (F::ConfidentialMintBurn, 2) => ("UpdateDecryptableSupply", StateRelevant),
        (F::ConfidentialMintBurn, 3) => ("Mint", StateRelevant),
        (F::ConfidentialMintBurn, 4) => ("Burn", StateRelevant),
        (F::ConfidentialMintBurn, 5) => ("ApplyPendingBurn", StateRelevant),

        (F::ScaledUiAmount, 0) => ("Initialize", StateRelevant),
        (F::ScaledUiAmount, 1) => ("UpdateMultiplier", StateRelevant),
        (F::Pausable, 0) => ("Initialize", StateRelevant),
        (F::Pausable, 1) => ("Pause", StateRelevant),
        (F::Pausable, 2) => ("Resume", StateRelevant),

        (F::PermissionedBurn, 0) => ("Initialize", StateRelevant),
        (F::PermissionedBurn, 1) => ("Burn", BalanceRelevant),
        (F::PermissionedBurn, 2) => ("BurnChecked", BalanceRelevant),
        (F::PermissionedBurn, 3) => ("ConfidentialBurn", StateRelevant),
        _ => return None,
    };
    Some((ExtensionInstruction::new(family, subtype, name), effect))
}

fn extension_public_fields(
    extension: ExtensionInstruction,
    raw: &[u8],
) -> (Option<u64>, Option<u8>, bool) {
    use ExtensionFamily as F;

    match (extension.family, extension.subtype) {
        // amount + decimals + expected fee
        (F::TransferFee, 1) => {
            let amount = read_u64(raw, 2);
            let decimals = raw.get(10).copied();
            (
                amount,
                decimals,
                amount.is_some() && decimals.is_some() && raw.len() >= 19,
            )
        }
        // number of harvested accounts
        (F::TransferFee, 3) => (None, None, raw.get(2).is_some()),
        // fee basis points + maximum fee
        (F::TransferFee, 5) => (None, None, raw.len() >= 12),
        // Public amount moves into or out of a confidential balance.  The
        // fixed public prefix is amount + decimals.  Proof payload validation
        // belongs to the official processor, not this classifier.
        (F::ConfidentialTransfer, 5) => {
            let amount = read_u64(raw, 2);
            let decimals = raw.get(10).copied();
            (amount, decimals, amount.is_some() && decimals.is_some())
        }
        // Withdraw also contains a 36-byte decryptable balance and two proof
        // offsets after the public prefix (47 subtype-payload bytes total).
        (F::ConfidentialTransfer, 6) => {
            let amount = read_u64(raw, 2);
            let decimals = raw.get(10).copied();
            (
                amount,
                decimals,
                amount.is_some() && decimals.is_some() && raw.len() >= 49,
            )
        }
        // Permissioned public burn.
        (F::PermissionedBurn, 1) => {
            let amount = read_u64(raw, 2);
            (amount, None, amount.is_some())
        }
        (F::PermissionedBurn, 2) => {
            let amount = read_u64(raw, 2);
            let decimals = raw.get(10).copied();
            (amount, decimals, amount.is_some() && decimals.is_some())
        }
        _ => (None, None, true),
    }
}

fn decode_batch<'a>(program: TokenProgram, raw: &'a [u8]) -> DecodedInstruction<'a> {
    let mut effect = InstructionEffect::NoPublicBalanceEffect;
    let mut status = DecodeStatus::Known;
    let mut iterator = BatchIter {
        program,
        remaining: &raw[1..],
        offset: 1,
        finished: false,
    };

    for child in &mut iterator {
        match child {
            Ok(child) => {
                effect = effect.combine(child.instruction.effect);
                match child.instruction.status {
                    DecodeStatus::Known => {}
                    DecodeStatus::Malformed => status = DecodeStatus::Malformed,
                    DecodeStatus::UnknownTopLevel
                    | DecodeStatus::UnknownExtensionSubtype
                    | DecodeStatus::BatchContainsUnknown => {
                        if status == DecodeStatus::Known {
                            status = DecodeStatus::BatchContainsUnknown;
                        }
                    }
                }
            }
            Err(_) => {
                status = DecodeStatus::Malformed;
                effect = effect.combine(InstructionEffect::BalanceRelevant);
                break;
            }
        }
    }

    DecodedInstruction {
        program,
        raw,
        top_level: Some(TopLevelInstruction::Batch),
        extension: None,
        interface: None,
        effect,
        status,
        amount: None,
        decimals: None,
    }
}

fn read_u64(raw: &[u8], offset: usize) -> Option<u64> {
    let bytes: [u8; 8] = raw.get(offset..offset + 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

fn valid_address_option(raw: &[u8], offset: usize) -> bool {
    match raw.get(offset) {
        Some(0) => true,
        Some(1) => raw.len() >= offset + 1 + 32,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn amount_instruction(tag: u8, amount: u64) -> [u8; 9] {
        let mut raw = [0_u8; 9];
        raw[0] = tag;
        raw[1..].copy_from_slice(&amount.to_le_bytes());
        raw
    }

    fn checked_instruction(tag: u8, amount: u64, decimals: u8) -> [u8; 10] {
        let mut raw = [0_u8; 10];
        raw[0] = tag;
        raw[1..9].copy_from_slice(&amount.to_le_bytes());
        raw[9] = decimals;
        raw
    }

    #[test]
    fn classic_balance_instructions_are_lossless() {
        for (tag, expected) in [
            (3, TopLevelInstruction::Transfer),
            (7, TopLevelInstruction::MintTo),
            (8, TopLevelInstruction::Burn),
        ] {
            let raw = amount_instruction(tag, 42);
            let decoded = decode(TokenProgram::Legacy, &raw);
            assert_eq!(decoded.raw, raw.as_slice());
            assert_eq!(decoded.raw.as_ptr(), raw.as_ptr());
            assert_eq!(decoded.top_level, Some(expected));
            assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);
            assert_eq!(decoded.amount, Some(42));
            assert_eq!(decoded.status, DecodeStatus::Known);
        }

        let close = decode(TokenProgram::Legacy, &[9]);
        assert_eq!(close.top_level, Some(TopLevelInstruction::CloseAccount));
        assert_eq!(close.effect, InstructionEffect::BalanceRelevant);

        let sync = decode(TokenProgram::Legacy, &[17]);
        assert_eq!(sync.top_level, Some(TopLevelInstruction::SyncNative));
        assert_eq!(sync.effect, InstructionEffect::BalanceRelevant);

        let checked = checked_instruction(12, 500, 6);
        let checked = decode(TokenProgram::Legacy, &checked);
        assert_eq!(checked.amount, Some(500));
        assert_eq!(checked.decimals, Some(6));
    }

    #[test]
    fn classic_does_not_accept_token_2022_prefixes() {
        let raw = [26, 1, 0];
        let decoded = decode(TokenProgram::Legacy, &raw);
        assert_eq!(decoded.raw, &raw);
        assert_eq!(decoded.status, DecodeStatus::UnknownTopLevel);
        assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);
    }

    #[test]
    fn token_metadata_interface_instructions_are_lossless() {
        for (discriminator, name, effect) in [
            (
                TOKEN_METADATA_INITIALIZE_DISCRIMINATOR,
                "Initialize",
                InstructionEffect::StateRelevant,
            ),
            (
                TOKEN_METADATA_UPDATE_FIELD_DISCRIMINATOR,
                "UpdateField",
                InstructionEffect::StateRelevant,
            ),
            (
                TOKEN_METADATA_REMOVE_KEY_DISCRIMINATOR,
                "RemoveKey",
                InstructionEffect::StateRelevant,
            ),
            (
                TOKEN_METADATA_UPDATE_AUTHORITY_DISCRIMINATOR,
                "UpdateAuthority",
                InstructionEffect::StateRelevant,
            ),
            (
                TOKEN_METADATA_EMIT_DISCRIMINATOR,
                "Emit",
                InstructionEffect::NoPublicBalanceEffect,
            ),
        ] {
            let mut raw = discriminator.to_vec();
            raw.extend_from_slice(&[0xaa, 0xbb, 0xcc]);

            let decoded = decode(TokenProgram::Token2022, &raw);
            let interface = decoded.interface.expect("known metadata interface");
            assert_eq!(decoded.raw, raw.as_slice());
            assert_eq!(decoded.raw.as_ptr(), raw.as_ptr());
            assert_eq!(decoded.top_level, None);
            assert_eq!(decoded.extension, None);
            assert_eq!(interface.family, Token2022InterfaceFamily::TokenMetadata);
            assert_eq!(interface.discriminator, discriminator);
            assert_eq!(decoded.interface_discriminator(), Some(discriminator));
            assert_eq!(interface.name, name);
            assert_eq!(decoded.name(), name);
            assert_eq!(decoded.effect, effect);
            assert_eq!(decoded.status, DecodeStatus::Known);
            assert_eq!(decoded.amount, None);
            assert_eq!(decoded.decimals, None);
        }
    }

    #[test]
    fn token_group_interface_instructions_are_lossless() {
        for (discriminator, name) in [
            (
                TOKEN_GROUP_INITIALIZE_GROUP_DISCRIMINATOR,
                "InitializeGroup",
            ),
            (
                TOKEN_GROUP_UPDATE_MAX_SIZE_DISCRIMINATOR,
                "UpdateGroupMaxSize",
            ),
            (
                TOKEN_GROUP_UPDATE_AUTHORITY_DISCRIMINATOR,
                "UpdateGroupAuthority",
            ),
            (
                TOKEN_GROUP_INITIALIZE_MEMBER_DISCRIMINATOR,
                "InitializeMember",
            ),
        ] {
            let mut raw = discriminator.to_vec();
            raw.extend_from_slice(&[0x11, 0x22]);

            let decoded = decode(TokenProgram::Token2022, &raw);
            let interface = decoded.interface.expect("known group interface");
            assert_eq!(decoded.raw, raw.as_slice());
            assert_eq!(decoded.raw.as_ptr(), raw.as_ptr());
            assert_eq!(decoded.top_level, None);
            assert_eq!(decoded.extension, None);
            assert_eq!(interface.family, Token2022InterfaceFamily::TokenGroup);
            assert_eq!(interface.discriminator, discriminator);
            assert_eq!(interface.name, name);
            assert_eq!(decoded.name(), name);
            assert_eq!(decoded.effect, InstructionEffect::StateRelevant);
            assert_eq!(decoded.status, DecodeStatus::Known);
            assert_eq!(decoded.amount, None);
            assert_eq!(decoded.decimals, None);
        }
    }

    #[test]
    fn legacy_does_not_dispatch_token_2022_interfaces() {
        let raw = TOKEN_METADATA_INITIALIZE_DISCRIMINATOR;
        let decoded = decode(TokenProgram::Legacy, &raw);
        assert_eq!(decoded.raw, &raw);
        assert_eq!(decoded.interface, None);
        assert_eq!(decoded.status, DecodeStatus::UnknownTopLevel);
        assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);
    }

    #[test]
    fn transfer_fee_transfer_decodes_public_amount() {
        let mut raw = [0_u8; 19];
        raw[0] = 26;
        raw[1] = 1;
        raw[2..10].copy_from_slice(&1_000_u64.to_le_bytes());
        raw[10] = 6;
        raw[11..19].copy_from_slice(&25_u64.to_le_bytes());

        let decoded = decode(TokenProgram::Token2022, &raw);
        let extension = decoded.extension.expect("known transfer-fee subtype");
        assert_eq!(extension.family, ExtensionFamily::TransferFee);
        assert_eq!(extension.subtype, 1);
        assert_eq!(extension.name, "TransferCheckedWithFee");
        assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);
        assert_eq!(decoded.amount, Some(1_000));
        assert_eq!(decoded.decimals, Some(6));
        assert_eq!(decoded.status, DecodeStatus::Known);
    }

    #[test]
    fn confidential_deposit_and_withdraw_decode_public_amounts() {
        for (subtype, name, length) in [(5, "Deposit", 11), (6, "Withdraw", 49)] {
            let mut raw = vec![0_u8; length];
            raw[0] = 27;
            raw[1] = subtype;
            raw[2..10].copy_from_slice(&77_u64.to_le_bytes());
            raw[10] = 9;

            let decoded = decode(TokenProgram::Token2022, &raw);
            let extension = decoded.extension.expect("known confidential subtype");
            assert_eq!(extension.family, ExtensionFamily::ConfidentialTransfer);
            assert_eq!(extension.name, name);
            assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);
            assert_eq!(decoded.amount, Some(77));
            assert_eq!(decoded.decimals, Some(9));
            assert_eq!(decoded.status, DecodeStatus::Known);
        }
    }

    #[test]
    fn token_2022_tags_45_and_46_are_classified() {
        let unwrap_all = [45, 0];
        let decoded = decode(TokenProgram::Token2022, &unwrap_all);
        assert_eq!(decoded.raw, &unwrap_all);
        assert_eq!(decoded.top_level, Some(TopLevelInstruction::UnwrapLamports));
        assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);
        assert_eq!(decoded.amount, None);
        assert_eq!(decoded.status, DecodeStatus::Known);

        let mut unwrap_some = [0_u8; 10];
        unwrap_some[0] = 45;
        unwrap_some[1] = 1;
        unwrap_some[2..].copy_from_slice(&123_u64.to_le_bytes());
        let decoded = decode(TokenProgram::Token2022, &unwrap_some);
        assert_eq!(decoded.amount, Some(123));

        let mut burn = [0_u8; 10];
        burn[0] = 46;
        burn[1] = 1;
        burn[2..].copy_from_slice(&55_u64.to_le_bytes());
        let decoded = decode(TokenProgram::Token2022, &burn);
        let extension = decoded.extension.expect("known permissioned burn");
        assert_eq!(extension.family, ExtensionFamily::PermissionedBurn);
        assert_eq!(extension.name, "Burn");
        assert_eq!(decoded.amount, Some(55));
        assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);

        let confidential = decode(TokenProgram::Token2022, &[46, 3]);
        assert_eq!(confidential.name(), "ConfidentialBurn");
        assert_eq!(confidential.effect, InstructionEffect::StateRelevant);
    }

    #[test]
    fn batch_255_iterates_borrowed_children_and_aggregates_effect() {
        let transfer = amount_instruction(3, 900);
        let mut raw = vec![255, 3, transfer.len() as u8];
        raw.extend_from_slice(&transfer);
        raw.extend_from_slice(&[1, 1, 17]);

        let decoded = decode(TokenProgram::Token2022, &raw);
        assert_eq!(decoded.raw, raw.as_slice());
        assert_eq!(decoded.raw.as_ptr(), raw.as_ptr());
        assert_eq!(decoded.top_level, Some(TopLevelInstruction::Batch));
        assert_eq!(decoded.status, DecodeStatus::Known);
        assert_eq!(decoded.effect, InstructionEffect::BalanceRelevant);

        let mut children = decoded.batch_items().expect("batch iterator");
        let first = children.next().expect("first child").expect("valid child");
        assert_eq!(first.account_count, 3);
        assert_eq!(first.raw, transfer.as_slice());
        assert_eq!(first.instruction.amount, Some(900));
        let second = children.next().expect("second child").expect("valid child");
        assert_eq!(second.account_count, 1);
        assert_eq!(
            second.instruction.top_level,
            Some(TopLevelInstruction::SyncNative)
        );
        assert!(children.next().is_none());
    }

    #[test]
    fn batch_rejects_nested_and_keeps_outer_raw() {
        let raw = [255, 0, 1, 255];
        let decoded = decode(TokenProgram::Token2022, &raw);
        assert_eq!(decoded.raw, &raw);
        assert_eq!(decoded.status, DecodeStatus::Malformed);
        let error = decoded
            .batch_items()
            .expect("batch iterator")
            .next()
            .expect("one result")
            .expect_err("nested batch must fail");
        assert_eq!(error.kind, BatchDecodeErrorKind::NestedBatch);
    }

    #[test]
    fn unknown_and_malformed_bytes_are_never_dropped() {
        let unknown_raw = [200, 9, 8, 7];
        let unknown = decode(TokenProgram::Token2022, &unknown_raw);
        assert_eq!(unknown.raw, &unknown_raw);
        assert_eq!(unknown.status, DecodeStatus::UnknownTopLevel);
        assert_eq!(unknown.effect, InstructionEffect::BalanceRelevant);

        let empty: [u8; 0] = [];
        let malformed = decode(TokenProgram::Token2022, &empty);
        assert_eq!(malformed.raw, &empty);
        assert_eq!(malformed.status, DecodeStatus::Malformed);

        let short_transfer = [3, 1, 2];
        let malformed = decode(TokenProgram::Legacy, &short_transfer);
        assert_eq!(malformed.raw, &short_transfer);
        assert_eq!(malformed.top_level, Some(TopLevelInstruction::Transfer));
        assert_eq!(malformed.status, DecodeStatus::Malformed);

        let unknown_extension = [27, 250, 1, 2, 3];
        let decoded = decode(TokenProgram::Token2022, &unknown_extension);
        assert_eq!(decoded.raw, &unknown_extension);
        assert_eq!(decoded.extension_subtype(), Some(250));
        assert_eq!(decoded.status, DecodeStatus::UnknownExtensionSubtype);
    }
}
