use crate::{
    confirmed_block::{self, TransactionStatusMeta},
    stored_transaction_status_meta as stored,
    stored_transaction_status_meta::StoredTransactionStatusMeta,
};

#[inline]
pub fn stored_meta_to_proto(m: StoredTransactionStatusMeta) -> TransactionStatusMeta {
    // status -> err
    let err = match &m.status {
        stored::TransactionResult::Ok => None,
        stored::TransactionResult::Err(err) => Some(confirmed_block::TransactionError {
            err: wincode::serialize(err).unwrap(),
        }),
    };

    // inner_instructions Option<Vec<..>> -> Vec + none flag
    let (inner_instructions, inner_instructions_none) = match &m.inner_instructions {
        stored::OptionEof::None => (Vec::new(), true),
        stored::OptionEof::Some(v) => (v.iter().map(inner_instructions_to_proto).collect(), false),
    };

    // log_messages Option<Vec<String>> -> Vec + none flag
    let (log_messages, log_messages_none) = match &m.log_messages {
        stored::OptionEof::None => (Vec::new(), true),
        stored::OptionEof::Some(v) => (v.clone(), false),
    };

    // return_data Option<..> -> Option + none flag
    let (return_data, return_data_none) = match &m.return_data {
        stored::OptionEof::None => (None, true),
        stored::OptionEof::Some(rd) => (Some(return_data_to_proto(rd)), false),
    };

    // Token balances: stored uses Option, proto uses Vec without *_none flags
    let pre_token_balances = m
        .pre_token_balances
        .as_ref()
        .map(|v| v.iter().map(token_balance_to_proto).collect())
        .unwrap_or_default();

    let post_token_balances = m
        .post_token_balances
        .as_ref()
        .map(|v| v.iter().map(token_balance_to_proto).collect())
        .unwrap_or_default();

    // Rewards: stored Option, proto Vec
    let rewards = m
        .rewards
        .as_ref()
        .map(|v| v.iter().map(reward_to_proto).collect())
        .unwrap_or_default();

    confirmed_block::TransactionStatusMeta {
        err,
        fee: m.fee,
        pre_balances: m.pre_balances.clone(),
        post_balances: m.post_balances.clone(),

        inner_instructions,
        inner_instructions_none,

        log_messages,
        log_messages_none,

        pre_token_balances,
        post_token_balances,

        rewards,

        // You do not store these, so default empty.
        loaded_writable_addresses: Vec::new(),
        loaded_readonly_addresses: Vec::new(),

        return_data,
        return_data_none,

        compute_units_consumed: None,
        cost_units: None,
    }
}

#[inline]
fn return_data_to_proto(rd: &stored::TransactionReturnData) -> confirmed_block::ReturnData {
    confirmed_block::ReturnData {
        program_id: rd.program_id.to_vec(),
        data: rd.data.clone(),
    }
}

#[inline]
fn inner_instructions_to_proto(
    ii: &stored::InnerInstructions,
) -> confirmed_block::InnerInstructions {
    confirmed_block::InnerInstructions {
        index: ii.index as u32,
        instructions: ii
            .instructions
            .iter()
            .map(inner_instruction_to_proto)
            .collect(),
    }
}

#[inline]
fn inner_instruction_to_proto(
    i: &stored::CompiledInstruction,
) -> confirmed_block::InnerInstruction {
    confirmed_block::InnerInstruction {
        program_id_index: i.program_id_index as u32,
        accounts: i.accounts.clone(),
        data: i.data.clone(),
        stack_height: None,
    }
}

#[inline]
fn token_balance_to_proto(
    tb: &stored::StoredTransactionTokenBalance,
) -> confirmed_block::TokenBalance {
    confirmed_block::TokenBalance {
        account_index: tb.account_index as u32,
        mint: tb.mint.clone(),
        ui_token_amount: Some(ui_token_amount_to_proto(&tb.ui_token_amount)),
        //TODO: fixme as this should be an option
        owner: String::default(),
        //TODO: fixme as this should be an option
        program_id: String::default(),
    }
}

#[inline]
fn ui_token_amount_to_proto(a: &stored::StoredTokenAmount) -> confirmed_block::UiTokenAmount {
    // You have: ui_amount (f64), decimals (u8), amount (String)
    // Prost expects: ui_amount_string also. Best effort:
    // - keep the raw integer string in `amount`
    // - keep `ui_amount` as-is
    // - set `ui_amount_string` from ui_amount to preserve human formatting
    confirmed_block::UiTokenAmount {
        ui_amount: a.ui_amount,
        decimals: a.decimals as u32,
        amount: a.amount.clone(),
        ui_amount_string: a.ui_amount.to_string(),
    }
}

#[inline]
fn reward_to_proto(r: &stored::StoredExtendedReward) -> confirmed_block::Reward {
    confirmed_block::Reward {
        pubkey: r.pubkey.clone(),
        lamports: r.lamports,
        post_balance: r.post_balance,
        reward_type: map_reward_type(r.reward_type) as i32,
        // Stored is Option<u8>, proto field is String
        commission: r.commission.map(|c| c.to_string()).unwrap_or_default(),
    }
}

#[inline]
fn map_reward_type(rt: Option<u8>) -> confirmed_block::RewardType {
    match rt {
        Some(1) => confirmed_block::RewardType::Fee,
        Some(2) => confirmed_block::RewardType::Rent,
        Some(3) => confirmed_block::RewardType::Staking,
        Some(4) => confirmed_block::RewardType::Voting,
        _ => confirmed_block::RewardType::Unspecified,
    }
}
