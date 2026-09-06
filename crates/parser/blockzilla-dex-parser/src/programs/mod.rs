mod additional_anchor;
mod additional_classic;
mod additional_markets;
mod additional_unobserved;
mod jupiter;
mod meteora;
mod okx;
mod orca;
mod pancake;
mod pump;
mod raydium;
mod raydium_route;

use crate::{CompactId, DecodeOutcome, Program};

pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    match program {
        Program::RaydiumClmm | Program::RaydiumCpmm | Program::RaydiumAmmV4 => {
            raydium::decode(program, data, accounts)
        }
        Program::OrcaWhirlpool => orca::decode(data, accounts),
        Program::MeteoraDlmm | Program::MeteoraDammV2 => meteora::decode(program, data, accounts),
        Program::PancakeSwap => pancake::decode(data, accounts),
        Program::JupiterV6 => jupiter::decode(data, accounts),
        Program::PumpSwap => pump::decode(data, accounts),
        Program::OkxRouterV2 | Program::OkxRouterV3 => okx::decode(program, data, accounts),
        Program::RaydiumRoute => raydium_route::decode(data, accounts),
        Program::LifinityV2
        | Program::Legacy9tke
        | Program::Aldrin
        | Program::BonkSwap
        | Program::CremaClmm
        | Program::LegacyCtma
        | Program::AldrinV2
        | Program::LegacyD3bb
        | Program::OneDex
        | Program::MeteoraPools
        | Program::Cropper
        | Program::Invariant
        | Program::Byreal
        | Program::MeteoraDbc
        | Program::ObricV2
        | Program::StabbleWeighted
        | Program::StabbleStable => additional_anchor::decode(program, data, accounts),
        Program::RaydiumStable
        | Program::OrcaV2
        | Program::OrcaV1
        | Program::StepnDex
        | Program::Fluxbeam
        | Program::Phoenix
        | Program::Saros
        | Program::StepFinanceSwap
        | Program::Saber
        | Program::SolFi
        | Program::ZeroFi
        | Program::PlasmaGavel => additional_classic::decode(program, data, accounts),
        Program::PumpFun | Program::RaydiumLaunchlab | Program::OpenBookV2 => {
            additional_markets::decode(program, data, accounts)
        }
        Program::RaydiumLegacyV2
        | Program::SymmetryV2
        | Program::Legacy2Nz
        | Program::CremaFinance
        | Program::GooseFxSsl
        | Program::SerumDexV3
        | Program::LifinityV1
        | Program::GooseFxV2
        | Program::PenguinFinance
        | Program::Sencha
        | Program::Cykura
        | Program::Dradex
        | Program::OpenBookV1 => additional_unobserved::decode(program, data, accounts),
    }
}
