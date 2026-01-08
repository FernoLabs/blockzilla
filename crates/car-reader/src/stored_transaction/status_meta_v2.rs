use wincode::{SchemaRead, containers, len::BincodeLen};

use crate::{confirmed_block, convert_metadata, stored_transaction::StoredTransactionStatusMeta};

use super::{
    InnerInstructions, OptionEof, StoredExtendedReward, TransactionResult, TransactionReturnData,
};

#[derive(SchemaRead)]
pub struct StoredTransactionStatusMetaV2 {
    pub status: TransactionResult,
    pub fee: u64,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub pre_balances: Vec<u64>,
    #[wincode(with = "containers::Vec<_, BincodeLen>")]
    pub post_balances: Vec<u64>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub inner_instructions: OptionEof<Vec<InnerInstructions>>,
    #[wincode(with = "OptionEof<containers::Vec<String, BincodeLen>>")]
    pub log_messages: OptionEof<Vec<String>>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub pre_token_balances: OptionEof<Vec<StoredTransactionTokenBalanceV2>>,
    #[wincode(with = "OptionEof<containers::Vec<_, BincodeLen>>")]
    pub post_token_balances: OptionEof<Vec<StoredTransactionTokenBalanceV2>>,
    pub rewards: OptionEof<Vec<StoredExtendedReward>>,
    pub return_data: OptionEof<TransactionReturnData>,
    pub compute_units_consumed: OptionEof<u64>,
    pub cost_units: OptionEof<u64>,
}

impl From<StoredTransactionStatusMetaV2> for confirmed_block::TransactionStatusMeta {
    fn from(m: StoredTransactionStatusMetaV2) -> Self {
        let v1 = StoredTransactionStatusMeta {
            status: m.status,
            fee: m.fee,
            pre_balances: m.pre_balances,
            post_balances: m.post_balances,
            inner_instructions: m.inner_instructions,
            log_messages: m.log_messages,
            pre_token_balances: m
                .pre_token_balances
                .map(|v| v.into_iter().map(|v| v.into()).collect()),
            post_token_balances: m
                .post_token_balances
                .map(|v| v.into_iter().map(|v| v.into()).collect()),
            rewards: m.rewards,
            return_data: m.return_data,
            compute_units_consumed: m.compute_units_consumed,
            cost_units: m.cost_units,
        };
        convert_metadata::stored_meta_to_proto(v1)
    }
}

#[derive(SchemaRead, Clone)]
pub struct StoredTransactionTokenBalanceV2 {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: StoredTokenAmountV2,
}

impl From<StoredTransactionTokenBalanceV2> for super::StoredTransactionTokenBalance {
    fn from(m: StoredTransactionTokenBalanceV2) -> Self {
        super::StoredTransactionTokenBalance {
            account_index: m.account_index,
            mint: m.mint,
            ui_token_amount: m.ui_token_amount.into(),
        }
    }
}

#[derive(SchemaRead, Clone, Debug, PartialEq)]
pub struct StoredTokenAmountV2 {
    pub ui_amount: OptionEof<f64>,
    pub decimals: u8,
    pub amount: String,
    pub ui_amount_string: String,
}

impl From<StoredTokenAmountV2> for super::StoredTokenAmount {
    fn from(m: StoredTokenAmountV2) -> Self {
        super::StoredTokenAmount {
            ui_amount: m.ui_amount.unwrap_or_default(),
            decimals: m.decimals,
            amount: m.amount,
        }
    }
}
