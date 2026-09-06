use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

pub const STR_ID: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PhoenixLog {
    Instruction(PhoenixInstructionLog),
    SendingBatch {
        batch: u64,
        market_events: u64,
        total_events_sent: u64,
    },
    Discriminant {
        type_name: String,
        value: u64,
    },
    Static(PhoenixStaticLog),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PhoenixInstructionLog {
    Initialize,
    Swap,
    SwapWithFreeFunds,
    PlaceLimitOrder,
    PlaceLimitOrderWithFreeFunds,
    PlaceMultiplePostOnlyOrders,
    PlaceMultiplePostOnlyOrdersWithFreeFunds,
    ReduceOrder,
    ReduceOrderWithFreeFunds,
    CancelAllOrders,
    CancelAllOrdersWithFreeFunds,
    CancelMultipleOrders,
    CancelUpToWithFreeFunds,
    CancelMultipleOrdersById,
    CancelMultipleOrdersByIdWithFreeFunds,
    WithdrawFunds,
    DepositFunds,
    ForceCancelOrders,
    EvictSeat,
    ClaimAuthority,
    NameSuccessor,
    ChangeMarketStatus,
    RequestSeatAuthorized,
    RequestSeat,
    ChangeSeatStatus,
    CollectFees,
    ChangeFeeRecipient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PhoenixStaticLog {
    FailedToConvertDiscriminantHashToU64,
    InvalidParametersForMarket,
    MarketHasBeenRemoved,
    SeatStatusIsUnchanged,
    NoOrdersToCancel,
    FailedToDecodeOrderPacket,
    WarningVaultContextWasNotProvided,
    WarningDepositedAmountOfFundsWereInsufficient,
    DepositedAmountOfFundsWereInsufficient,
    InvalidInputMultipleOrderPacketContainsCrossingBidsAndAsks,
    WarningEmptyOrderFoundInCheckForCross,
    MarketIsUninitialized,
    SequenceNumberExceededMaximum,
    BidPriceIsTooLow,
    EitherNumBaseLotsOrNumQuoteLotsMustBeNonzero,
    OrderParametersExpired,
    PostOnlyOrderCrossesBookRejected,
    PostOnlyOrderCrossesBookCannotBeAmendedRejected,
    PostOnlyOrderCrossesBookAmended,
    EncounteredErrorMatchingOrder,
    BookIsFullEvictingOrder,
    FailedToInsertOrderIntoBook,
    TraderDoesNotHaveEnoughFundsToProcessOrder,
    MatchingEngineResponseWithdrawsBaseOrQuoteLots,
    NewOrderIsNotAggressiveEnoughToEvict,
    BookIsEmpty,
}

impl PhoenixLog {
    pub fn parse(payload: &str) -> Option<Self> {
        if let Some(name) = payload.strip_prefix("PhoenixInstruction::") {
            return Some(Self::Instruction(PhoenixInstructionLog::parse(name)?));
        }

        if let Some(rest) = payload.strip_prefix("Sending batch ") {
            let (batch, rest) = rest.split_once(" with header and ")?;
            let (market_events, total_events_sent) =
                rest.split_once(" market events, total events sent: ")?;
            return Some(Self::SendingBatch {
                batch: batch.trim().parse().ok()?,
                market_events: market_events.trim().parse().ok()?,
                total_events_sent: total_events_sent.trim().parse().ok()?,
            });
        }

        if let Some(rest) = payload.strip_prefix("Discriminant for ") {
            let (type_name, value) = rest.split_once(" is ")?;
            return Some(Self::Discriminant {
                type_name: type_name.trim().to_string(),
                value: value.trim().parse().ok()?,
            });
        }

        PhoenixStaticLog::parse(payload).map(Self::Static)
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::Instruction(instruction) => {
                format!("PhoenixInstruction::{}", instruction.as_str())
            }
            Self::SendingBatch {
                batch,
                market_events,
                total_events_sent,
            } => format!(
                "Sending batch {batch} with header and {market_events} market events, total events sent: {total_events_sent}"
            ),
            Self::Discriminant { type_name, value } => {
                format!("Discriminant for {type_name} is {value}")
            }
            Self::Static(log) => log.as_str().to_string(),
        }
    }
}

impl PhoenixInstructionLog {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "Initialize" => Some(Self::Initialize),
            "Swap" => Some(Self::Swap),
            "SwapWithFreeFunds" => Some(Self::SwapWithFreeFunds),
            "PlaceLimitOrder" => Some(Self::PlaceLimitOrder),
            "PlaceLimitOrderWithFreeFunds" => Some(Self::PlaceLimitOrderWithFreeFunds),
            "PlaceMultiplePostOnlyOrders" => Some(Self::PlaceMultiplePostOnlyOrders),
            "PlaceMultiplePostOnlyOrdersWithFreeFunds" => {
                Some(Self::PlaceMultiplePostOnlyOrdersWithFreeFunds)
            }
            "ReduceOrder" => Some(Self::ReduceOrder),
            "ReduceOrderWithFreeFunds" => Some(Self::ReduceOrderWithFreeFunds),
            "CancelAllOrders" => Some(Self::CancelAllOrders),
            "CancelAllOrdersWithFreeFunds" => Some(Self::CancelAllOrdersWithFreeFunds),
            "CancelMultipleOrders" => Some(Self::CancelMultipleOrders),
            "CancelUpToWithFreeFunds" => Some(Self::CancelUpToWithFreeFunds),
            "CancelMultipleOrdersById" => Some(Self::CancelMultipleOrdersById),
            "CancelMultipleOrdersByIdWithFreeFunds" => {
                Some(Self::CancelMultipleOrdersByIdWithFreeFunds)
            }
            "WithdrawFunds" => Some(Self::WithdrawFunds),
            "DepositFunds" => Some(Self::DepositFunds),
            "ForceCancelOrders" => Some(Self::ForceCancelOrders),
            "EvictSeat" => Some(Self::EvictSeat),
            "ClaimAuthority" => Some(Self::ClaimAuthority),
            "NameSuccessor" => Some(Self::NameSuccessor),
            "ChangeMarketStatus" => Some(Self::ChangeMarketStatus),
            "RequestSeatAuthorized" => Some(Self::RequestSeatAuthorized),
            "RequestSeat" => Some(Self::RequestSeat),
            "ChangeSeatStatus" => Some(Self::ChangeSeatStatus),
            "CollectFees" => Some(Self::CollectFees),
            "ChangeFeeRecipient" => Some(Self::ChangeFeeRecipient),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Initialize => "Initialize",
            Self::Swap => "Swap",
            Self::SwapWithFreeFunds => "SwapWithFreeFunds",
            Self::PlaceLimitOrder => "PlaceLimitOrder",
            Self::PlaceLimitOrderWithFreeFunds => "PlaceLimitOrderWithFreeFunds",
            Self::PlaceMultiplePostOnlyOrders => "PlaceMultiplePostOnlyOrders",
            Self::PlaceMultiplePostOnlyOrdersWithFreeFunds => {
                "PlaceMultiplePostOnlyOrdersWithFreeFunds"
            }
            Self::ReduceOrder => "ReduceOrder",
            Self::ReduceOrderWithFreeFunds => "ReduceOrderWithFreeFunds",
            Self::CancelAllOrders => "CancelAllOrders",
            Self::CancelAllOrdersWithFreeFunds => "CancelAllOrdersWithFreeFunds",
            Self::CancelMultipleOrders => "CancelMultipleOrders",
            Self::CancelUpToWithFreeFunds => "CancelUpToWithFreeFunds",
            Self::CancelMultipleOrdersById => "CancelMultipleOrdersById",
            Self::CancelMultipleOrdersByIdWithFreeFunds => "CancelMultipleOrdersByIdWithFreeFunds",
            Self::WithdrawFunds => "WithdrawFunds",
            Self::DepositFunds => "DepositFunds",
            Self::ForceCancelOrders => "ForceCancelOrders",
            Self::EvictSeat => "EvictSeat",
            Self::ClaimAuthority => "ClaimAuthority",
            Self::NameSuccessor => "NameSuccessor",
            Self::ChangeMarketStatus => "ChangeMarketStatus",
            Self::RequestSeatAuthorized => "RequestSeatAuthorized",
            Self::RequestSeat => "RequestSeat",
            Self::ChangeSeatStatus => "ChangeSeatStatus",
            Self::CollectFees => "CollectFees",
            Self::ChangeFeeRecipient => "ChangeFeeRecipient",
        }
    }
}

impl PhoenixStaticLog {
    fn parse(payload: &str) -> Option<Self> {
        match payload {
            "Failed to convert discriminant hash to u64" => {
                Some(Self::FailedToConvertDiscriminantHashToU64)
            }
            "Invalid parameters for market" => Some(Self::InvalidParametersForMarket),
            "Market has been removed" => Some(Self::MarketHasBeenRemoved),
            "Seat status is unchanged" => Some(Self::SeatStatusIsUnchanged),
            "No orders to cancel" => Some(Self::NoOrdersToCancel),
            "Failed to decode order packet" => Some(Self::FailedToDecodeOrderPacket),
            "WARNING: Vault context was not provided" => {
                Some(Self::WarningVaultContextWasNotProvided)
            }
            "WARNING: Deposited amount of funds were insufficient to execute the order" => {
                Some(Self::WarningDepositedAmountOfFundsWereInsufficient)
            }
            "Deposited amount of funds were insufficient to execute the order" => {
                Some(Self::DepositedAmountOfFundsWereInsufficient)
            }
            "Invalid input. MultipleOrderPacket contains crossing bids and asks" => {
                Some(Self::InvalidInputMultipleOrderPacketContainsCrossingBidsAndAsks)
            }
            "WARNING: Empty order found in check_for_cross" => {
                Some(Self::WarningEmptyOrderFoundInCheckForCross)
            }
            "Market is uninitialized" => Some(Self::MarketIsUninitialized),
            "Sequence number exceeded maximum" => Some(Self::SequenceNumberExceededMaximum),
            "Bid price is too low" => Some(Self::BidPriceIsTooLow),
            "Either num_base_lots or num_quote_lots must be nonzero" => {
                Some(Self::EitherNumBaseLotsOrNumQuoteLotsMustBeNonzero)
            }
            "Order parameters include a last_valid_slot or last_valid_unix_timestamp_in_seconds in the past, skipping matching and posting" => {
                Some(Self::OrderParametersExpired)
            }
            "PostOnly order crosses the book - order rejected" => {
                Some(Self::PostOnlyOrderCrossesBookRejected)
            }
            "PostOnly order crosses the book and can not be amended to a valid price - order rejected" => {
                Some(Self::PostOnlyOrderCrossesBookCannotBeAmendedRejected)
            }
            "PostOnly order crosses the book - order amended" => {
                Some(Self::PostOnlyOrderCrossesBookAmended)
            }
            "Encountered error matching order" => Some(Self::EncounteredErrorMatchingOrder),
            "Book is full. Evicting order" => Some(Self::BookIsFullEvictingOrder),
            "Failed to insert order into book" => Some(Self::FailedToInsertOrderIntoBook),
            "Trader does not have enough deposited funds to process order" => {
                Some(Self::TraderDoesNotHaveEnoughFundsToProcessOrder)
            }
            "Matching engine response withdraws base or quote lots" => {
                Some(Self::MatchingEngineResponseWithdrawsBaseOrQuoteLots)
            }
            "New order is not aggressive enough to evict an existing order" => {
                Some(Self::NewOrderIsNotAggressiveEnoughToEvict)
            }
            "Book is empty" => Some(Self::BookIsEmpty),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::FailedToConvertDiscriminantHashToU64 => {
                "Failed to convert discriminant hash to u64"
            }
            Self::InvalidParametersForMarket => "Invalid parameters for market",
            Self::MarketHasBeenRemoved => "Market has been removed",
            Self::SeatStatusIsUnchanged => "Seat status is unchanged",
            Self::NoOrdersToCancel => "No orders to cancel",
            Self::FailedToDecodeOrderPacket => "Failed to decode order packet",
            Self::WarningVaultContextWasNotProvided => "WARNING: Vault context was not provided",
            Self::WarningDepositedAmountOfFundsWereInsufficient => {
                "WARNING: Deposited amount of funds were insufficient to execute the order"
            }
            Self::DepositedAmountOfFundsWereInsufficient => {
                "Deposited amount of funds were insufficient to execute the order"
            }
            Self::InvalidInputMultipleOrderPacketContainsCrossingBidsAndAsks => {
                "Invalid input. MultipleOrderPacket contains crossing bids and asks"
            }
            Self::WarningEmptyOrderFoundInCheckForCross => {
                "WARNING: Empty order found in check_for_cross"
            }
            Self::MarketIsUninitialized => "Market is uninitialized",
            Self::SequenceNumberExceededMaximum => "Sequence number exceeded maximum",
            Self::BidPriceIsTooLow => "Bid price is too low",
            Self::EitherNumBaseLotsOrNumQuoteLotsMustBeNonzero => {
                "Either num_base_lots or num_quote_lots must be nonzero"
            }
            Self::OrderParametersExpired => {
                "Order parameters include a last_valid_slot or last_valid_unix_timestamp_in_seconds in the past, skipping matching and posting"
            }
            Self::PostOnlyOrderCrossesBookRejected => {
                "PostOnly order crosses the book - order rejected"
            }
            Self::PostOnlyOrderCrossesBookCannotBeAmendedRejected => {
                "PostOnly order crosses the book and can not be amended to a valid price - order rejected"
            }
            Self::PostOnlyOrderCrossesBookAmended => {
                "PostOnly order crosses the book - order amended"
            }
            Self::EncounteredErrorMatchingOrder => "Encountered error matching order",
            Self::BookIsFullEvictingOrder => "Book is full. Evicting order",
            Self::FailedToInsertOrderIntoBook => "Failed to insert order into book",
            Self::TraderDoesNotHaveEnoughFundsToProcessOrder => {
                "Trader does not have enough deposited funds to process order"
            }
            Self::MatchingEngineResponseWithdrawsBaseOrQuoteLots => {
                "Matching engine response withdraws base or quote lots"
            }
            Self::NewOrderIsNotAggressiveEnoughToEvict => {
                "New order is not aggressive enough to evict an existing order"
            }
            Self::BookIsEmpty => "Book is empty",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phoenix_logs_round_trip() {
        let samples = [
            "PhoenixInstruction::CancelUpToWithFreeFunds",
            "Sending batch 1 with header and 32 market events, total events sent: 32",
            "Discriminant for phoenix::program::accounts::MarketHeader is 8167313896524341111",
            "PostOnly order crosses the book - order rejected",
        ];

        for sample in samples {
            let log = PhoenixLog::parse(sample).unwrap();
            assert_eq!(log.as_str(), sample);
        }
    }
}
