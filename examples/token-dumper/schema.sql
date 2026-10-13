CREATE DATABASE IF NOT EXISTS blockzilla;

CREATE TABLE IF NOT EXISTS blockzilla.token_balances
(
    source_id String,
    epoch UInt64,
    block_ordinal UInt32,
    slot UInt64,
    tx_index UInt32,
    execution_status LowCardinality(String),
    side LowCardinality(String),
    balance_index UInt32,
    account_index UInt32,
    mint Nullable(String),
    owner Nullable(String),
    token_program Nullable(String),
    amount String,
    decimals UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY epoch
ORDER BY (source_id, epoch, block_ordinal, tx_index, side, balance_index);
