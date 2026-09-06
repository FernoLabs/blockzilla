//! Borrowed, bounds-checked instruction classifiers for Solana DEX programs.
//!
//! The crate does not infer a trade from transaction-wide balance changes.
//! It identifies the economic instruction and its account roles. A caller must
//! still reconcile the result with committed CPI token transfers and pre/post
//! balances before it publishes a swap, price, or volume record.

mod dispatch;
mod reader;

pub mod programs;

pub use dispatch::{DispatchTable, PROGRAM_SPECS, ProgramSpec};
pub use reader::{read_prefix, read_u8, read_u32_le, read_u64_le};

/// Version of the parser result semantics and evidence contract.
///
/// Change this value when a decoder change can alter coverage or event output.
pub const PARSER_SEMANTIC_VERSION: &str = "1.0.0";

/// SHA-256 of a deterministic SHA-256 manifest for `dispatch.rs`, `reader.rs`,
/// then every sorted Rust source path below `programs/` for version 1.0.0.
pub const PARSER_IMPLEMENTATION_FINGERPRINT: &str =
    "ecaee7f901cbfe8b9ba779b022aa1499bce9c03ed04270d9ec3e678361fd7835";

/// Compact account or program ID resolved by the archive registry.
pub type CompactId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Program {
    RaydiumClmm,
    RaydiumCpmm,
    RaydiumAmmV4,
    OrcaWhirlpool,
    MeteoraDlmm,
    MeteoraDammV2,
    PancakeSwap,
    JupiterV6,
    PumpSwap,
    OkxRouterV2,
    OkxRouterV3,
    RaydiumRoute,
    LifinityV2,
    RaydiumStable,
    PumpFun,
    OrcaV2,
    Legacy9tke,
    Aldrin,
    BonkSwap,
    CremaClmm,
    LegacyCtma,
    AldrinV2,
    LegacyD3bb,
    OneDex,
    OrcaV1,
    StepnDex,
    MeteoraPools,
    Fluxbeam,
    Cropper,
    Invariant,
    RaydiumLaunchlab,
    Phoenix,
    Byreal,
    Saros,
    StepFinanceSwap,
    Saber,
    SolFi,
    ZeroFi,
    MeteoraDbc,
    ObricV2,
    OpenBookV2,
    PlasmaGavel,
    StabbleWeighted,
    StabbleStable,
    RaydiumLegacyV2,
    SymmetryV2,
    Legacy2Nz,
    CremaFinance,
    GooseFxSsl,
    SerumDexV3,
    LifinityV1,
    GooseFxV2,
    PenguinFinance,
    Sencha,
    Cykura,
    Dradex,
    OpenBookV1,
}

impl Program {
    /// Tells the reducer if this program executes a venue swap or only routes it.
    pub const fn role(self) -> ProgramRole {
        match self {
            Self::JupiterV6 | Self::OkxRouterV2 | Self::OkxRouterV3 | Self::RaydiumRoute => {
                ProgramRole::Router
            }
            Self::RaydiumClmm
            | Self::RaydiumCpmm
            | Self::RaydiumAmmV4
            | Self::OrcaWhirlpool
            | Self::MeteoraDlmm
            | Self::MeteoraDammV2
            | Self::PancakeSwap
            | Self::PumpSwap
            | Self::LifinityV2
            | Self::RaydiumStable
            | Self::PumpFun
            | Self::OrcaV2
            | Self::Legacy9tke
            | Self::Aldrin
            | Self::BonkSwap
            | Self::CremaClmm
            | Self::LegacyCtma
            | Self::AldrinV2
            | Self::LegacyD3bb
            | Self::OneDex
            | Self::OrcaV1
            | Self::StepnDex
            | Self::MeteoraPools
            | Self::Fluxbeam
            | Self::Cropper
            | Self::Invariant
            | Self::RaydiumLaunchlab
            | Self::Phoenix
            | Self::Byreal
            | Self::Saros
            | Self::StepFinanceSwap
            | Self::Saber
            | Self::SolFi
            | Self::ZeroFi
            | Self::MeteoraDbc
            | Self::ObricV2
            | Self::OpenBookV2
            | Self::PlasmaGavel
            | Self::StabbleWeighted
            | Self::StabbleStable
            | Self::RaydiumLegacyV2
            | Self::SymmetryV2
            | Self::Legacy2Nz
            | Self::CremaFinance
            | Self::GooseFxSsl
            | Self::SerumDexV3
            | Self::LifinityV1
            | Self::GooseFxV2
            | Self::PenguinFinance
            | Self::Sencha
            | Self::Cykura
            | Self::Dradex
            | Self::OpenBookV1 => ProgramRole::Venue,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramRole {
    Venue,
    Router,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapKind {
    /// A swap is proven, but its amount mode is not.
    Unspecified,
    ExactIn,
    ExactOut,
    PartialFill,
    TwoHopExactIn,
    TwoHopExactOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionClass {
    Swap(SwapKind),
    Route,
    Order(OrderKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKind {
    Place,
    PlaceTake,
    Settle,
}

/// The discriminator bytes observed at the start of an instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Discriminator {
    pub bytes: [u8; 8],
    pub len: u8,
}

impl Discriminator {
    pub const fn one(byte: u8) -> Self {
        Self {
            bytes: [byte, 0, 0, 0, 0, 0, 0, 0],
            len: 1,
        }
    }

    pub const fn eight(bytes: [u8; 8]) -> Self {
        Self { bytes, len: 8 }
    }

    /// Version byte plus a little-endian u32 selector, as used by Serum DEX.
    pub const fn five(bytes: [u8; 5]) -> Self {
        Self {
            bytes: [bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], 0, 0, 0],
            len: 5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AccountRoles {
    pub pool: Option<CompactId>,
    pub second_pool: Option<CompactId>,
    /// Program-derived authority for the pool or its vaults.
    pub authority: Option<CompactId>,
    /// Trader, payer, or delegated token authority for the user accounts.
    pub user_authority: Option<CompactId>,
    pub user_source: Option<CompactId>,
    pub user_destination: Option<CompactId>,
    /// Canonical side-A vault when the protocol names a stable A/B pair.
    pub vault_a: Option<CompactId>,
    /// Canonical side-B vault when the protocol names a stable A/B pair.
    pub vault_b: Option<CompactId>,
    /// A second canonical side-A vault when the protocol defines one.
    pub second_vault_a: Option<CompactId>,
    /// A second canonical side-B vault when the protocol defines one.
    pub second_vault_b: Option<CompactId>,
    /// Vault that receives the input for this instruction.
    pub input_vault: Option<CompactId>,
    /// Vault that supplies the output for this instruction.
    pub output_vault: Option<CompactId>,
    pub input_mint: Option<CompactId>,
    pub intermediate_mint: Option<CompactId>,
    pub output_mint: Option<CompactId>,
    pub fee_account: Option<CompactId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Amounts {
    /// The instruction has no simple fixed-width amount pair, or a later
    /// reducer must derive the values from committed token flows.
    Unknown,
    /// Declared exact input and minimum output. These are not execution totals.
    ExactIn {
        amount_in: u64,
        minimum_amount_out: u64,
    },
    /// Declared exact output and maximum input. These are not execution totals.
    ExactOut {
        maximum_amount_in: u64,
        amount_out: u64,
    },
    /// Declared partial-fill input and minimum output constraints.
    PartialFill {
        amount_in: u64,
        minimum_amount_out: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Evidence(u16);

impl Evidence {
    pub const ACCOUNT_LAYOUT: Self = Self(1 << 0);
    pub const AMOUNTS: Self = Self(1 << 1);
    pub const ROUTE_CONTAINER: Self = Self(1 << 2);
    pub const TOKEN_FLOW_REQUIRED: Self = Self(1 << 3);
    /// Only the program and discriminator are proven. The reducer must not
    /// treat this as a complete instruction-body validation.
    pub const STRUCTURAL_ONLY: Self = Self(1 << 4);

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn bits(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedInstruction {
    pub program: Program,
    pub role: ProgramRole,
    pub name: &'static str,
    pub class: InstructionClass,
    pub discriminator: Discriminator,
    pub accounts: AccountRoles,
    pub amounts: Amounts,
    pub evidence: Evidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MalformedReason {
    InstructionDataTooShort { needed: usize, actual: usize },
    InstructionAccountsTooShort { needed: usize, actual: usize },
    InvalidInstructionData { offset: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeOutcome {
    Decoded(DecodedInstruction),
    Unsupported { discriminator: Discriminator },
    Malformed(MalformedReason),
    UnknownProgram,
}

/// Decodes an instruction when the caller already resolved its program.
///
/// This function does not allocate. `DispatchTable::decode` is the normal
/// entry point for archive scans that start with compact registry IDs.
#[inline]
pub fn decode_program(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    programs::decode(program, data, accounts)
}

pub(crate) fn account(accounts: &[CompactId], index: usize) -> Result<CompactId, MalformedReason> {
    accounts
        .get(index)
        .copied()
        .ok_or(MalformedReason::InstructionAccountsTooShort {
            needed: index.saturating_add(1),
            actual: accounts.len(),
        })
}

pub(crate) fn anchor_discriminator(data: &[u8]) -> Result<Discriminator, MalformedReason> {
    let Some(bytes) = read_prefix::<8>(data) else {
        return Err(MalformedReason::InstructionDataTooShort {
            needed: 8,
            actual: data.len(),
        });
    };
    Ok(Discriminator::eight(bytes))
}

pub(crate) fn one_byte_discriminator(data: &[u8]) -> Result<Discriminator, MalformedReason> {
    let Some(byte) = data.first().copied() else {
        return Err(MalformedReason::InstructionDataTooShort {
            needed: 1,
            actual: 0,
        });
    };
    Ok(Discriminator::one(byte))
}
