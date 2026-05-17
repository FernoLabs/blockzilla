use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

pub const V2_STR_ID: &str = "6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma";
pub const V3_STR_ID: &str = "proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u";
pub const ROUTE_STR_ID: &str = "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum OkxRouterLog {
    DexAmountIn {
        dex: String,
        amount_in: u64,
        offset: u64,
        spelling: AmountInSpelling,
    },
    DexAbort {
        dex: String,
    },
    DataLen(u64),
    Res(u64),
    CalculateSolAmountOut(u64),
    TokenAccountAlreadyInitialized,
    InsufficientSenderBalance {
        sender_balance: u64,
        actual_transfer_amount: u64,
    },
    InvalidActualAmountIn {
        actual_amount_in: u64,
        amount_in: u64,
    },
    SolTransferredWithRentExemption {
        actual_transfer_amount: u64,
        requested_amount: u64,
        to: String,
    },
    SwapEvent {
        dex: u8,
        amount_in: u64,
        amount_out: u64,
    },
    RouteLabel(OkxRouteLabel),
    NoProfitableRoute,
    Marker(OkxMarker),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum AmountInSpelling {
    Underscore,
    Space,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum OkxRouteLabel {
    V5ToV2Upper,
    V2ToV5Upper,
    V5ToV10Arrow,
    V5ToV5Arrow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum OkxMarker {
    Fish,
    TropicalFish,
    Dolphin,
    Shrimp,
    Shark,
    Crab,
}

#[inline]
pub fn is_known_router_id(program: &str) -> bool {
    matches!(program, V2_STR_ID | V3_STR_ID | ROUTE_STR_ID)
}

impl OkxRouterLog {
    pub fn parse(payload: &str) -> Option<Self> {
        if let Some(rest) = payload.strip_prefix("Dex::") {
            if let Some(dex) = rest.strip_suffix(" ABORT") {
                return Some(Self::DexAbort {
                    dex: dex.to_string(),
                });
            }

            if let Some((dex, rest)) = rest.split_once(" amount_in: ") {
                let (amount_in, offset) = parse_amount_offset(rest)?;
                return Some(Self::DexAmountIn {
                    dex: dex.to_string(),
                    amount_in,
                    offset,
                    spelling: AmountInSpelling::Underscore,
                });
            }

            if let Some((dex, rest)) = rest.split_once(" amount in: ") {
                let (amount_in, offset) = parse_amount_offset(rest)?;
                return Some(Self::DexAmountIn {
                    dex: dex.to_string(),
                    amount_in,
                    offset,
                    spelling: AmountInSpelling::Space,
                });
            }
        }

        if let Some(value) = payload.strip_prefix("data.len: ") {
            return Some(Self::DataLen(value.trim().parse().ok()?));
        }
        if let Some(value) = payload.strip_prefix("res: ") {
            return Some(Self::Res(value.trim().parse().ok()?));
        }
        if let Some(value) = payload.strip_prefix("calculate_sol_amount_out: ") {
            return Some(Self::CalculateSolAmountOut(value.trim().parse().ok()?));
        }
        if payload == "Token account already initialized" {
            return Some(Self::TokenAccountAlreadyInitialized);
        }
        if let Some(rest) = payload.strip_prefix("Insufficient sender balance: ") {
            let (sender_balance, actual_transfer_amount) = rest.split_once(" < ")?;
            return Some(Self::InsufficientSenderBalance {
                sender_balance: sender_balance.trim().parse().ok()?,
                actual_transfer_amount: actual_transfer_amount.trim().parse().ok()?,
            });
        }
        if let Some(rest) = payload.strip_prefix("InvalidActualAmountIn: actual_amount_in=") {
            let (actual_amount_in, amount_in) = rest.split_once(", amount_in=")?;
            return Some(Self::InvalidActualAmountIn {
                actual_amount_in: actual_amount_in.trim().parse().ok()?,
                amount_in: amount_in.trim().parse().ok()?,
            });
        }
        if let Some(rest) = payload.strip_prefix("SOL transferred with rent exemption: ") {
            let (actual_transfer_amount, rest) = rest.split_once(" lamports (requested: ")?;
            let (requested_amount, to) = rest.split_once(") to ")?;
            return Some(Self::SolTransferredWithRentExemption {
                actual_transfer_amount: actual_transfer_amount.trim().parse().ok()?,
                requested_amount: requested_amount.trim().parse().ok()?,
                to: to.trim().to_string(),
            });
        }
        if let Some(rest) = payload.strip_prefix("SwapEvent { dex: ") {
            let (dex, rest) = rest.split_once(", amount_in: ")?;
            let (amount_in, rest) = rest.split_once(", amount_out: ")?;
            let amount_out = rest.strip_suffix(" }")?;
            return Some(Self::SwapEvent {
                dex: dex.trim().parse().ok()?,
                amount_in: amount_in.trim().parse().ok()?,
                amount_out: amount_out.trim().parse().ok()?,
            });
        }

        if let Some(label) = OkxRouteLabel::parse(payload) {
            return Some(Self::RouteLabel(label));
        }
        if payload == "No profitable route" {
            return Some(Self::NoProfitableRoute);
        }
        if let Some(marker) = OkxMarker::parse(payload) {
            return Some(Self::Marker(marker));
        }

        None
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::DexAmountIn {
                dex,
                amount_in,
                offset,
                spelling,
            } => match spelling {
                AmountInSpelling::Underscore => {
                    format!("Dex::{dex} amount_in: {amount_in}, offset: {offset}")
                }
                AmountInSpelling::Space => {
                    format!("Dex::{dex} amount in: {amount_in}, offset: {offset}")
                }
            },
            Self::DexAbort { dex } => format!("Dex::{dex} ABORT"),
            Self::DataLen(value) => format!("data.len: {value}"),
            Self::Res(value) => format!("res: {value}"),
            Self::CalculateSolAmountOut(value) => format!("calculate_sol_amount_out: {value}"),
            Self::TokenAccountAlreadyInitialized => "Token account already initialized".to_string(),
            Self::InsufficientSenderBalance {
                sender_balance,
                actual_transfer_amount,
            } => {
                format!("Insufficient sender balance: {sender_balance} < {actual_transfer_amount}")
            }
            Self::InvalidActualAmountIn {
                actual_amount_in,
                amount_in,
            } => {
                format!(
                    "InvalidActualAmountIn: actual_amount_in={actual_amount_in}, amount_in={amount_in}"
                )
            }
            Self::SolTransferredWithRentExemption {
                actual_transfer_amount,
                requested_amount,
                to,
            } => format!(
                "SOL transferred with rent exemption: {actual_transfer_amount} lamports (requested: {requested_amount}) to {to}"
            ),
            Self::SwapEvent {
                dex,
                amount_in,
                amount_out,
            } => format!(
                "SwapEvent {{ dex: {dex}, amount_in: {amount_in}, amount_out: {amount_out} }}"
            ),
            Self::RouteLabel(label) => label.as_str().to_string(),
            Self::NoProfitableRoute => "No profitable route".to_string(),
            Self::Marker(marker) => marker.as_str().to_string(),
        }
    }
}

impl OkxRouteLabel {
    fn parse(payload: &str) -> Option<Self> {
        match payload {
            "V5 TO V2" => Some(Self::V5ToV2Upper),
            "V2 TO V5" => Some(Self::V2ToV5Upper),
            "V5 -> V10" => Some(Self::V5ToV10Arrow),
            "V5 -> V5" => Some(Self::V5ToV5Arrow),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::V5ToV2Upper => "V5 TO V2",
            Self::V2ToV5Upper => "V2 TO V5",
            Self::V5ToV10Arrow => "V5 -> V10",
            Self::V5ToV5Arrow => "V5 -> V5",
        }
    }
}

impl OkxMarker {
    fn parse(payload: &str) -> Option<Self> {
        match payload {
            "🐟" => Some(Self::Fish),
            "🐠" => Some(Self::TropicalFish),
            "🐬" => Some(Self::Dolphin),
            "🦐" => Some(Self::Shrimp),
            "🦈" => Some(Self::Shark),
            "🦀" => Some(Self::Crab),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fish => "🐟",
            Self::TropicalFish => "🐠",
            Self::Dolphin => "🐬",
            Self::Shrimp => "🦐",
            Self::Shark => "🦈",
            Self::Crab => "🦀",
        }
    }
}

fn parse_amount_offset(rest: &str) -> Option<(u64, u64)> {
    let (amount_in, offset) = rest.split_once(", offset: ")?;
    Some((amount_in.trim().parse().ok()?, offset.trim().parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn okx_router_logs_round_trip() {
        let samples = [
            "Dex::MeteoraDlmm amount_in: 100, offset: 3",
            "Dex::PerpetualsSwap amount in: 100, offset: 3",
            "Dex::Tessera ABORT",
            "data.len: 42",
            "res: 7",
            "calculate_sol_amount_out: 123",
            "Token account already initialized",
            "Insufficient sender balance: 1 < 2",
            "InvalidActualAmountIn: actual_amount_in=1, amount_in=2",
            "SwapEvent { dex: 4, amount_in: 10, amount_out: 11 }",
            "V5 TO V2",
            "V2 TO V5",
            "V5 -> V10",
            "V5 -> V5",
            "No profitable route",
            "🐟",
            "🐠",
            "🐬",
            "🦐",
            "🦈",
            "🦀",
        ];

        for sample in samples {
            let log = OkxRouterLog::parse(sample).unwrap();
            assert_eq!(log.as_str(), sample);
        }
    }
}
