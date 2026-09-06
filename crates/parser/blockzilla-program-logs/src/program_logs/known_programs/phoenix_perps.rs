use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use super::generic_base64::{parse_canonical_base64, render_base64};

pub const PERPS_STR_ID: &str = "phDEVv4w6BcfkLrLNeXr8HhhgQxnxziVGXpGPcaadMf";
pub const ETERNAL_STR_ID: &str = "EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PhoenixPerpsLog {
    Event(Vec<u8>),
    Static(PhoenixPerpsStaticLog),
    BidPlacedSuccessfully(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PhoenixPerpsStaticLog {
    MatchingEnginePlaceOrderReduceOnlyCheck,
    MatchingEngineInsertLimitOrder,
    MatchingEngineRemoveOrderFromBookInner,
    MatchingEngineReduceOrderInner,
    MatchingEngineCheckForCross,
    MatchingEngineUncrossBook,
    MatchingEngineMatchSplinesAgainstOrderbook,
    MatchingEngineHandleCross,
    MatchingEngineMatchOrder,
    MatchingEngineMatchSplinesUntil,
    MatchingEngineMatchOrderbook,
    InteractingWithMatchingEngine,
    SettlingFundingAndUpdatingSnapshotAndInitializingTraderPosition,
    PreMarginCheck,
    PlacingOrder,
    OrderPlaced,
    PostMarginCheck,
    AttemptingToActivateTrader,
    SuccessfullyActivatedSelectedTrader,
    CancelExecuted,
    CancelWithMatchingEngine,
    ExecutingCancel,
    SettlingFundingForCancel,
    CheckPermissions,
    PhoenixEternalUpdateSplinePrice,
    SplinePriceUpdatedSuccessfully,
    PhoenixEternalUpdateSplineParameters,
    SplineParametersUpdatedSuccessfully,
    PhoenixEternalPlaceLimitOrder,
    PhoenixEternalPlaceMultipleLimitOrdersInBatch,
    PhoenixEternalCancelUpTo,
}

#[inline]
pub fn is_known_program_id(program: &str) -> bool {
    matches!(program, PERPS_STR_ID | ETERNAL_STR_ID)
}

impl PhoenixPerpsLog {
    pub fn parse(payload: &str) -> Option<Self> {
        if let Some(log) = PhoenixPerpsStaticLog::parse(payload) {
            return Some(Self::Static(log));
        }
        if let Some(index) = parse_bid_placed_successfully(payload) {
            return Some(Self::BidPlacedSuccessfully(index));
        }
        parse_canonical_base64(payload).map(Self::Event)
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::Event(bytes) => render_base64(bytes),
            Self::Static(log) => log.as_str().to_string(),
            Self::BidPlacedSuccessfully(index) => format!("Bid {index} placed successfully"),
        }
    }
}

impl PhoenixPerpsStaticLog {
    fn parse(payload: &str) -> Option<Self> {
        match payload {
            "MatchingEngine: PlaceOrder: Reduce-only check" => {
                Some(Self::MatchingEnginePlaceOrderReduceOnlyCheck)
            }
            "MatchingEngine: Insert limit order" => Some(Self::MatchingEngineInsertLimitOrder),
            "MatchingEngine: Remove order from book inner" => {
                Some(Self::MatchingEngineRemoveOrderFromBookInner)
            }
            "MatchingEngine: Reduce order inner" => Some(Self::MatchingEngineReduceOrderInner),
            "MatchingEngine: Check for cross" => Some(Self::MatchingEngineCheckForCross),
            "MatchingEngine: Uncross book" => Some(Self::MatchingEngineUncrossBook),
            "MatchingEngine: Match splines against orderbook" => {
                Some(Self::MatchingEngineMatchSplinesAgainstOrderbook)
            }
            "MatchingEngine: Handle cross" => Some(Self::MatchingEngineHandleCross),
            "MatchingEngine: Match order" => Some(Self::MatchingEngineMatchOrder),
            "MatchingEngine: Match splines until" => Some(Self::MatchingEngineMatchSplinesUntil),
            "MatchingEngine: Match orderbook" => Some(Self::MatchingEngineMatchOrderbook),
            "Interacting with matching engine" => Some(Self::InteractingWithMatchingEngine),
            "Settling funding and updating snapshot and initializing trader position" => {
                Some(Self::SettlingFundingAndUpdatingSnapshotAndInitializingTraderPosition)
            }
            "Pre-margin check" => Some(Self::PreMarginCheck),
            "Placing order" => Some(Self::PlacingOrder),
            "Order placed" => Some(Self::OrderPlaced),
            "Post-margin check" => Some(Self::PostMarginCheck),
            "Attempting to activate trader" => Some(Self::AttemptingToActivateTrader),
            "Successfully activated selected trader" => {
                Some(Self::SuccessfullyActivatedSelectedTrader)
            }
            "Cancel executed" => Some(Self::CancelExecuted),
            "Cancel with matching engine" => Some(Self::CancelWithMatchingEngine),
            "Executing cancel" => Some(Self::ExecutingCancel),
            "Settling funding for cancel" => Some(Self::SettlingFundingForCancel),
            "Check permissions" => Some(Self::CheckPermissions),
            "Phoenix Eternal 🐦‍🔥: Update Spline Price" => {
                Some(Self::PhoenixEternalUpdateSplinePrice)
            }
            "Spline price updated successfully" => Some(Self::SplinePriceUpdatedSuccessfully),
            "Phoenix Eternal 🐦‍🔥: Update Spline Parameters" => {
                Some(Self::PhoenixEternalUpdateSplineParameters)
            }
            "Spline parameters updated successfully" => {
                Some(Self::SplineParametersUpdatedSuccessfully)
            }
            "Phoenix: Eternal: Place limit order" => Some(Self::PhoenixEternalPlaceLimitOrder),
            "Phoenix: Eternal: Place multiple limit orders in batch" => {
                Some(Self::PhoenixEternalPlaceMultipleLimitOrdersInBatch)
            }
            "Phoenix Eternal: Cancel Up To" => Some(Self::PhoenixEternalCancelUpTo),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::MatchingEnginePlaceOrderReduceOnlyCheck => {
                "MatchingEngine: PlaceOrder: Reduce-only check"
            }
            Self::MatchingEngineInsertLimitOrder => "MatchingEngine: Insert limit order",
            Self::MatchingEngineRemoveOrderFromBookInner => {
                "MatchingEngine: Remove order from book inner"
            }
            Self::MatchingEngineReduceOrderInner => "MatchingEngine: Reduce order inner",
            Self::MatchingEngineCheckForCross => "MatchingEngine: Check for cross",
            Self::MatchingEngineUncrossBook => "MatchingEngine: Uncross book",
            Self::MatchingEngineMatchSplinesAgainstOrderbook => {
                "MatchingEngine: Match splines against orderbook"
            }
            Self::MatchingEngineHandleCross => "MatchingEngine: Handle cross",
            Self::MatchingEngineMatchOrder => "MatchingEngine: Match order",
            Self::MatchingEngineMatchSplinesUntil => "MatchingEngine: Match splines until",
            Self::MatchingEngineMatchOrderbook => "MatchingEngine: Match orderbook",
            Self::InteractingWithMatchingEngine => "Interacting with matching engine",
            Self::SettlingFundingAndUpdatingSnapshotAndInitializingTraderPosition => {
                "Settling funding and updating snapshot and initializing trader position"
            }
            Self::PreMarginCheck => "Pre-margin check",
            Self::PlacingOrder => "Placing order",
            Self::OrderPlaced => "Order placed",
            Self::PostMarginCheck => "Post-margin check",
            Self::AttemptingToActivateTrader => "Attempting to activate trader",
            Self::SuccessfullyActivatedSelectedTrader => "Successfully activated selected trader",
            Self::CancelExecuted => "Cancel executed",
            Self::CancelWithMatchingEngine => "Cancel with matching engine",
            Self::ExecutingCancel => "Executing cancel",
            Self::SettlingFundingForCancel => "Settling funding for cancel",
            Self::CheckPermissions => "Check permissions",
            Self::PhoenixEternalUpdateSplinePrice => "Phoenix Eternal 🐦‍🔥: Update Spline Price",
            Self::SplinePriceUpdatedSuccessfully => "Spline price updated successfully",
            Self::PhoenixEternalUpdateSplineParameters => {
                "Phoenix Eternal 🐦‍🔥: Update Spline Parameters"
            }
            Self::SplineParametersUpdatedSuccessfully => "Spline parameters updated successfully",
            Self::PhoenixEternalPlaceLimitOrder => "Phoenix: Eternal: Place limit order",
            Self::PhoenixEternalPlaceMultipleLimitOrdersInBatch => {
                "Phoenix: Eternal: Place multiple limit orders in batch"
            }
            Self::PhoenixEternalCancelUpTo => "Phoenix Eternal: Cancel Up To",
        }
    }
}

fn parse_bid_placed_successfully(payload: &str) -> Option<u64> {
    let rest = payload.strip_prefix("Bid ")?;
    rest.strip_suffix(" placed successfully")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phoenix_perps_logs_round_trip() {
        let samples = [
            "AQIDBAUGBwgJCgsMDQ4PEA==",
            "MatchingEngine: PlaceOrder: Reduce-only check",
            "MatchingEngine: Insert limit order",
            "MatchingEngine: Remove order from book inner",
            "MatchingEngine: Reduce order inner",
            "MatchingEngine: Check for cross",
            "Interacting with matching engine",
            "Settling funding and updating snapshot and initializing trader position",
            "Pre-margin check",
            "Placing order",
            "Order placed",
            "Post-margin check",
            "Attempting to activate trader",
            "Successfully activated selected trader",
            "Cancel executed",
            "Cancel with matching engine",
            "Executing cancel",
            "Settling funding for cancel",
            "Check permissions",
            "Phoenix Eternal 🐦‍🔥: Update Spline Price",
            "Spline price updated successfully",
            "Phoenix Eternal 🐦‍🔥: Update Spline Parameters",
            "Spline parameters updated successfully",
            "Phoenix: Eternal: Place limit order",
            "Phoenix: Eternal: Place multiple limit orders in batch",
            "Phoenix Eternal: Cancel Up To",
            "Bid 3 placed successfully",
        ];

        for sample in samples {
            let log = PhoenixPerpsLog::parse(sample).unwrap();
            assert_eq!(log.as_str(), sample);
        }
    }
}
