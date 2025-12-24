use wincode::{containers, len::ShortU16Len, SchemaRead};

use crate::{confirmed_block, convert_metadata};

#[derive(SchemaRead, Clone)]
pub struct TransactionReturnData {
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct InnerInstruction {
    pub instruction: CompiledInstruction,
    pub stack_height: Option<u32>,
}

#[derive(SchemaRead, Clone)]
pub struct InnerInstructions {
    pub index: u8,
    pub instructions: Vec<InnerInstruction>,
}

#[derive(SchemaRead, Clone)]
pub struct StoredExtendedReward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<u8>,
    pub commission: Option<u8>,
}

#[derive(SchemaRead, Clone)]
pub struct StoredTokenAmount {
    pub ui_amount: f64,
    pub decimals: u8,
    pub amount: String,
}

#[derive(SchemaRead, Clone)]
pub struct StoredTransactionTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: StoredTokenAmount,
    pub owner: String,
    pub program_id: String,
}

#[derive(SchemaRead)]
pub struct StoredTransactionError {
    #[wincode(with = "containers::Vec<_, ShortU16Len>")]
    pub error_bytes: Vec<u8>,
}

#[derive(SchemaRead)]
pub enum TransactionResult {
    Ok,
    Err(StoredTransactionError),
}

#[derive(SchemaRead)]
pub struct StoredTransactionStatusMeta {
    pub status: TransactionResult,
    pub fee: u64,
    #[wincode(with = "containers::Vec<_, ShortU16Len>")]
    pub pre_balances: Vec<u64>,
    #[wincode(with = "containers::Vec<_, ShortU16Len>")]
    pub post_balances: Vec<u64>,
    #[wincode(with = "Option<containers::Vec<_, ShortU16Len>>")]
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    #[wincode(with = "Option<containers::Vec<_, ShortU16Len>>")]
    pub log_messages: Option<Vec<String>>,
    #[wincode(with = "Option<containers::Vec<_, ShortU16Len>>")]
    pub pre_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
    #[wincode(with = "Option<containers::Vec<_, ShortU16Len>>")]
    pub post_token_balances: Option<Vec<StoredTransactionTokenBalance>>,
    #[wincode(with = "Option<containers::Vec<_, ShortU16Len>>")]
    pub rewards: Option<Vec<StoredExtendedReward>>,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

impl From<StoredTransactionStatusMeta> for confirmed_block::TransactionStatusMeta {
    fn from(m: StoredTransactionStatusMeta) -> Self {
        convert_metadata::stored_meta_to_proto(m)
    }
}
