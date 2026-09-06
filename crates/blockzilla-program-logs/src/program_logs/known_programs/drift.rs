use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use super::generic_base64::{parse_canonical_base64, render_base64};

pub const STR_ID: &str = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum DriftLog {
    Event(Vec<u8>),
    PriceUpdatedTo(u64),
    PostingNewLazerUpdate {
        current_ts: u64,
        next_timestamp: u64,
    },
    SkippingLazerPriceUpdateNextTimestampIsNone,
    PythLazerPriceIsZeroNotEnoughPublishers,
    PostingNewLazerUpdateNextTs {
        current_ts: u64,
        next_ts: u64,
    },
    SkippingNewLazerUpdate {
        current_ts: u64,
        next_ts: u64,
    },
    NoFulfillmentMethodsFound,
    NoBaseFilledOnPhoenix,
    NoPartialFill,
    ErrorRevertFillAtKeeper215,
    ErrorPlaceAndTakeOrderSuccessConditionFailedAtUser2803,
    FillerLastActiveSlotMismatch {
        filler_last_active_slot: u64,
        current_slot: u64,
    },
    InvalidPerp0MmOracleStaleImmediate,
    InvalidPerp0SafeMmOracleStaleImmediate,
    AmmCannotFillOrderOracleNotValidForImmediateFills,
    TakerDoesNotCrossAmm {
        taker_price: u64,
        amm_price: u64,
    },
}

impl DriftLog {
    pub fn parse(payload: &str) -> Option<Self> {
        if let Some(bytes) = parse_canonical_base64(payload) {
            return Some(Self::Event(bytes));
        }

        if let Some(price) = payload.strip_prefix("Price updated to ") {
            return Some(Self::PriceUpdatedTo(price.trim().parse().ok()?));
        }

        if let Some(rest) = payload.strip_prefix("Posting new lazer update. current ts ") {
            if let Some((current_ts, next_timestamp)) = rest.split_once(" < next_timestamp ") {
                return Some(Self::PostingNewLazerUpdate {
                    current_ts: current_ts.trim().parse().ok()?,
                    next_timestamp: next_timestamp.trim().parse().ok()?,
                });
            }
            if let Some((current_ts, next_ts)) = rest.split_once(" < next ts ") {
                return Some(Self::PostingNewLazerUpdateNextTs {
                    current_ts: current_ts.trim().parse().ok()?,
                    next_ts: next_ts.trim().parse().ok()?,
                });
            }
        }

        if let Some(rest) = payload.strip_prefix("Skipping new lazer update. current ts ") {
            let (current_ts, next_ts) = rest.split_once(" >= next ts ")?;
            return Some(Self::SkippingNewLazerUpdate {
                current_ts: current_ts.trim().parse().ok()?,
                next_ts: next_ts.trim().parse().ok()?,
            });
        }

        if let Some(rest) = payload.strip_prefix("filler last active slot (") {
            let (filler_last_active_slot, rest) = rest.split_once(") != current slot (")?;
            let current_slot = rest.strip_suffix(')')?;
            return Some(Self::FillerLastActiveSlotMismatch {
                filler_last_active_slot: filler_last_active_slot.trim().parse().ok()?,
                current_slot: current_slot.trim().parse().ok()?,
            });
        }

        if let Some(rest) = payload.strip_prefix("taker does not cross amm: taker price ") {
            let (taker_price, amm_price) = rest.split_once(" amm price ")?;
            return Some(Self::TakerDoesNotCrossAmm {
                taker_price: taker_price.trim().parse().ok()?,
                amm_price: amm_price.trim().parse().ok()?,
            });
        }

        match payload {
            "Skipping lazer price update. next_timestamp is None" => {
                Some(Self::SkippingLazerPriceUpdateNextTimestampIsNone)
            }
            "Pyth lazer price is zero, not enough publishers" => {
                Some(Self::PythLazerPriceIsZeroNotEnoughPublishers)
            }
            "no fulfillment methods found" => Some(Self::NoFulfillmentMethodsFound),
            "No base filled on phoenix" => Some(Self::NoBaseFilledOnPhoenix),
            "no partial fill" => Some(Self::NoPartialFill),
            "Error RevertFill thrown at programs/drift/src/instructions/keeper.rs:215" => {
                Some(Self::ErrorRevertFillAtKeeper215)
            }
            "Error Place and take order success condition failed thrown at programs/drift/src/instructions/user.rs:2803" => {
                Some(Self::ErrorPlaceAndTakeOrderSuccessConditionFailedAtUser2803)
            }
            "Invalid Perp 0 MM Oracle: Stale (oracle_delay=1), (stale_for_amm_immediate=true, stale_for_amm_low_risk=false, stale_for_margin=false)" => {
                Some(Self::InvalidPerp0MmOracleStaleImmediate)
            }
            "Invalid Perp 0 SafeMM Oracle: Stale (oracle_delay=1), (stale_for_amm_immediate=true, stale_for_amm_low_risk=false, stale_for_margin=false)" => {
                Some(Self::InvalidPerp0SafeMmOracleStaleImmediate)
            }
            "AMM cannot fill order: oracle not valid for immediate fills" => {
                Some(Self::AmmCannotFillOrderOracleNotValidForImmediateFills)
            }
            _ => None,
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::Event(bytes) => render_base64(bytes),
            Self::PriceUpdatedTo(price) => format!("Price updated to {price}"),
            Self::PostingNewLazerUpdate {
                current_ts,
                next_timestamp,
            } => {
                format!(
                    "Posting new lazer update. current ts {current_ts} < next_timestamp {next_timestamp}"
                )
            }
            Self::SkippingLazerPriceUpdateNextTimestampIsNone => {
                "Skipping lazer price update. next_timestamp is None".to_string()
            }
            Self::PythLazerPriceIsZeroNotEnoughPublishers => {
                "Pyth lazer price is zero, not enough publishers".to_string()
            }
            Self::PostingNewLazerUpdateNextTs {
                current_ts,
                next_ts,
            } => {
                format!("Posting new lazer update. current ts {current_ts} < next ts {next_ts}")
            }
            Self::SkippingNewLazerUpdate {
                current_ts,
                next_ts,
            } => {
                format!("Skipping new lazer update. current ts {current_ts} >= next ts {next_ts}")
            }
            Self::NoFulfillmentMethodsFound => "no fulfillment methods found".to_string(),
            Self::NoBaseFilledOnPhoenix => "No base filled on phoenix".to_string(),
            Self::NoPartialFill => "no partial fill".to_string(),
            Self::ErrorRevertFillAtKeeper215 => {
                "Error RevertFill thrown at programs/drift/src/instructions/keeper.rs:215"
                    .to_string()
            }
            Self::ErrorPlaceAndTakeOrderSuccessConditionFailedAtUser2803 => {
                "Error Place and take order success condition failed thrown at programs/drift/src/instructions/user.rs:2803".to_string()
            }
            Self::FillerLastActiveSlotMismatch {
                filler_last_active_slot,
                current_slot,
            } => {
                format!("filler last active slot ({filler_last_active_slot}) != current slot ({current_slot})")
            }
            Self::InvalidPerp0MmOracleStaleImmediate => {
                "Invalid Perp 0 MM Oracle: Stale (oracle_delay=1), (stale_for_amm_immediate=true, stale_for_amm_low_risk=false, stale_for_margin=false)".to_string()
            }
            Self::InvalidPerp0SafeMmOracleStaleImmediate => {
                "Invalid Perp 0 SafeMM Oracle: Stale (oracle_delay=1), (stale_for_amm_immediate=true, stale_for_amm_low_risk=false, stale_for_margin=false)".to_string()
            }
            Self::AmmCannotFillOrderOracleNotValidForImmediateFills => {
                "AMM cannot fill order: oracle not valid for immediate fills".to_string()
            }
            Self::TakerDoesNotCrossAmm {
                taker_price,
                amm_price,
            } => format!("taker does not cross amm: taker price {taker_price} amm price {amm_price}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drift_logs_round_trip() {
        let samples = [
            "AQIDBAUGBwgJCgsMDQ4PEA==",
            "Price updated to 12345",
            "Posting new lazer update. current ts 1753307455800000 < next_timestamp 1753307456200000",
            "Posting new lazer update. current ts 1756622079000000 < next ts 1756622092000000",
            "Skipping new lazer update. current ts 1756622092550000 >= next ts 1756622092000000",
            "Skipping lazer price update. next_timestamp is None",
            "Pyth lazer price is zero, not enough publishers",
            "no fulfillment methods found",
            "No base filled on phoenix",
            "no partial fill",
            "Error RevertFill thrown at programs/drift/src/instructions/keeper.rs:215",
            "Error Place and take order success condition failed thrown at programs/drift/src/instructions/user.rs:2803",
            "filler last active slot (397439999) != current slot (397440002)",
            "Invalid Perp 0 MM Oracle: Stale (oracle_delay=1), (stale_for_amm_immediate=true, stale_for_amm_low_risk=false, stale_for_margin=false)",
            "Invalid Perp 0 SafeMM Oracle: Stale (oracle_delay=1), (stale_for_amm_immediate=true, stale_for_amm_low_risk=false, stale_for_margin=false)",
            "AMM cannot fill order: oracle not valid for immediate fills",
            "taker does not cross amm: taker price 175800 amm price 175567",
        ];

        for sample in samples {
            let log = DriftLog::parse(sample).unwrap();
            assert_eq!(log.as_str(), sample);
        }
    }
}
