use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[cfg(feature = "known-drift")]
pub mod drift;
#[cfg(any(feature = "known-drift", feature = "known-phoenix-perps"))]
mod generic_base64;
#[cfg(feature = "known-okx-router")]
pub mod okx_router;
#[cfg(feature = "known-phoenix-perps")]
pub mod phoenix_perps;
#[cfg(feature = "known-phoenix-v1")]
pub mod phoenix_v1;
#[cfg(feature = "known-raydium-amm")]
pub mod raydium_amm;
#[cfg(feature = "known-static-programs")]
pub mod static_programs;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum KnownProgramLog {
    #[cfg(feature = "known-drift")]
    Drift(drift::DriftLog),
    #[cfg(feature = "known-okx-router")]
    OkxRouter(okx_router::OkxRouterLog),
    #[cfg(feature = "known-phoenix-perps")]
    PhoenixPerps(phoenix_perps::PhoenixPerpsLog),
    #[cfg(feature = "known-phoenix-v1")]
    PhoenixV1(phoenix_v1::PhoenixLog),
    #[cfg(feature = "known-raydium-amm")]
    RaydiumAmm(raydium_amm::RaydiumAmmLog),
    #[cfg(feature = "known-static-programs")]
    Static(static_programs::StaticProgramLog),
}

#[inline]
pub fn parse_for_program(program: &str, payload: &str) -> Option<KnownProgramLog> {
    #[cfg(feature = "known-drift")]
    if program == drift::STR_ID
        && let Some(log) = drift::DriftLog::parse(payload)
    {
        return Some(KnownProgramLog::Drift(log));
    }

    #[cfg(feature = "known-okx-router")]
    if okx_router::is_known_router_id(program)
        && let Some(log) = okx_router::OkxRouterLog::parse(payload)
    {
        return Some(KnownProgramLog::OkxRouter(log));
    }

    #[cfg(feature = "known-phoenix-perps")]
    if phoenix_perps::is_known_program_id(program)
        && let Some(log) = phoenix_perps::PhoenixPerpsLog::parse(payload)
    {
        return Some(KnownProgramLog::PhoenixPerps(log));
    }

    #[cfg(feature = "known-phoenix-v1")]
    if program == phoenix_v1::STR_ID
        && let Some(log) = phoenix_v1::PhoenixLog::parse(payload)
    {
        return Some(KnownProgramLog::PhoenixV1(log));
    }

    #[cfg(feature = "known-raydium-amm")]
    if program == raydium_amm::STR_ID
        && let Some(log) = raydium_amm::RaydiumAmmLog::parse(payload)
    {
        return Some(KnownProgramLog::RaydiumAmm(log));
    }

    #[cfg(feature = "known-static-programs")]
    if let Some(log) = static_programs::StaticProgramLog::parse(program, payload) {
        return Some(KnownProgramLog::Static(log));
    }

    let _ = (program, payload);
    None
}

#[inline]
pub fn has_known_binary_form(program: &str, payload: &str) -> bool {
    parse_for_program(program, payload).is_some()
}

#[inline]
pub fn render(log: &KnownProgramLog) -> String {
    match log {
        #[cfg(feature = "known-drift")]
        KnownProgramLog::Drift(log) => log.as_str(),
        #[cfg(feature = "known-okx-router")]
        KnownProgramLog::OkxRouter(log) => log.as_str(),
        #[cfg(feature = "known-phoenix-perps")]
        KnownProgramLog::PhoenixPerps(log) => log.as_str(),
        #[cfg(feature = "known-phoenix-v1")]
        KnownProgramLog::PhoenixV1(log) => log.as_str(),
        #[cfg(feature = "known-raydium-amm")]
        KnownProgramLog::RaydiumAmm(log) => log.as_str(),
        #[cfg(feature = "known-static-programs")]
        KnownProgramLog::Static(log) => log.as_str(),
    }
}
