/// A canonical workload rejected incomplete input or could not write output.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    InvalidInput(String),
    #[error("write canonical workload output")]
    Io(#[from] std::io::Error),

    #[error(
        "USDC token balances at epoch {epoch}, slot {slot}, transaction {tx_index} are duplicate or not in source order on the {side} side"
    )]
    TokenBalanceOrder {
        epoch: u64,
        slot: u64,
        tx_index: u32,
        side: &'static str,
    },

    #[error(
        "{workload} transactions are duplicate or not in ledger order at epoch {epoch}, slot {slot}, transaction {tx_index}"
    )]
    TransactionOrder {
        workload: &'static str,
        epoch: u64,
        slot: u64,
        tx_index: u32,
    },

    #[error(
        "transaction identity dump requires a primary signature at epoch {epoch}, slot {slot}, transaction {tx_index}"
    )]
    TransactionIdentityPrimarySignatureMissing {
        epoch: u64,
        slot: u64,
        tx_index: u32,
    },

    #[error(
        "transaction identity dump received epoch {actual_epoch}, expected epoch {expected_epoch}"
    )]
    TransactionIdentityEpoch {
        expected_epoch: u64,
        actual_epoch: u64,
    },

    #[error(
        "transaction identity dump slot {slot} is outside [{start_slot}, {end_slot_exclusive})"
    )]
    TransactionIdentitySlotRange {
        slot: u64,
        start_slot: u64,
        end_slot_exclusive: u64,
    },

    #[error(
        "transaction identity dump range [{start_slot}, {end_slot_exclusive}) is empty or inverted"
    )]
    TransactionIdentityInvalidRange {
        start_slot: u64,
        end_slot_exclusive: u64,
    },

    #[error(
        "transaction identity dump is not in canonical order at slot {slot}, transaction {tx_index}; expected {expected_tx_index}"
    )]
    TransactionIdentityOrder {
        slot: u64,
        tx_index: u32,
        expected_tx_index: u32,
    },

    #[error("{0} counter overflow")]
    CounterOverflow(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
