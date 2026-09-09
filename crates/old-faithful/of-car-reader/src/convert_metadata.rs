use crate::{
    confirmed_block::{self, TransactionStatusMeta},
    stored_transaction as stored,
    stored_transaction::StoredTokenAmount,
    stored_transaction::StoredTransactionStatusMeta,
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
    let (inner_instructions, inner_instructions_none) = match m.inner_instructions {
        stored::OptionEof::None => (Vec::new(), true),
        stored::OptionEof::Some(v) => (
            v.into_iter().map(inner_instructions_to_proto).collect(),
            false,
        ),
    };

    // log_messages Option<Vec<String>> -> Vec + none flag
    let (log_messages, log_messages_none) = match m.log_messages {
        stored::OptionEof::None => (Vec::new(), true),
        stored::OptionEof::Some(v) => (v, false),
    };

    // return_data Option<..> -> Option + none flag
    let (return_data, return_data_none) = match m.return_data {
        stored::OptionEof::None => (None, true),
        stored::OptionEof::Some(rd) => (Some(return_data_to_proto(rd)), false),
    };

    // Token balances: stored uses Option, proto uses Vec without *_none flags
    let pre_token_balances = m
        .pre_token_balances
        .into_option()
        .map(|v| v.into_iter().map(token_balance_to_proto).collect())
        .unwrap_or_default();

    let post_token_balances = m
        .post_token_balances
        .into_option()
        .map(|v| v.into_iter().map(token_balance_to_proto).collect())
        .unwrap_or_default();

    // Rewards: stored Option, proto Vec
    let rewards = m
        .rewards
        .into_option()
        .map(|v| v.into_iter().map(stored_reward_into_proto).collect())
        .unwrap_or_default();

    confirmed_block::TransactionStatusMeta {
        err,
        fee: m.fee,
        pre_balances: m.pre_balances,
        post_balances: m.post_balances,

        inner_instructions,
        inner_instructions_none,

        log_messages,
        log_messages_none,

        pre_token_balances,
        post_token_balances,

        rewards,

        loaded_writable_addresses: Vec::new(),
        loaded_readonly_addresses: Vec::new(),

        return_data,
        return_data_none,

        compute_units_consumed: None,
        cost_units: None,
    }
}

#[inline]
fn return_data_to_proto(rd: stored::TransactionReturnData) -> confirmed_block::ReturnData {
    confirmed_block::ReturnData {
        program_id: rd.program_id.to_vec(),
        data: rd.data,
    }
}

#[inline]
fn inner_instructions_to_proto(
    ii: stored::InnerInstructions,
) -> confirmed_block::InnerInstructions {
    confirmed_block::InnerInstructions {
        index: ii.index as u32,
        instructions: ii
            .instructions
            .into_iter()
            .map(inner_instruction_to_proto)
            .collect(),
    }
}

#[inline]
fn inner_instruction_to_proto(i: stored::CompiledInstruction) -> confirmed_block::InnerInstruction {
    confirmed_block::InnerInstruction {
        program_id_index: i.program_id_index as u32,
        accounts: i.accounts,
        data: i.data,
        stack_height: None,
    }
}

#[inline]
fn token_balance_to_proto(
    tb: stored::StoredTransactionTokenBalance,
) -> confirmed_block::TokenBalance {
    confirmed_block::TokenBalance {
        account_index: tb.account_index as u32,
        mint: tb.mint,
        ui_token_amount: Some(ui_token_amount_to_proto(tb.ui_token_amount)),
        owner: String::default(),
        program_id: String::default(),
    }
}

#[inline]
fn ui_token_amount_to_proto(a: StoredTokenAmount) -> confirmed_block::UiTokenAmount {
    confirmed_block::UiTokenAmount {
        ui_amount: a.ui_amount,
        decimals: a.decimals as u32,
        amount: a.amount,
        ui_amount_string: a.ui_amount.to_string(),
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

/// Consume a legacy reward without copying its public key string.
#[inline]
pub(crate) fn stored_reward_into_proto(r: stored::StoredExtendedReward) -> confirmed_block::Reward {
    confirmed_block::Reward {
        pubkey: r.pubkey,
        lamports: r.lamports,
        post_balance: r.post_balance,
        reward_type: map_reward_type(r.reward_type) as i32,
        commission: r.commission.map(|c| c.to_string()).unwrap_or_default(),
    }
}

#[inline]
pub(crate) fn stored_confirmed_block_reward_into_proto(
    r: stored::StoredConfirmedBlockReward,
) -> confirmed_block::Reward {
    confirmed_block::Reward {
        pubkey: r.pubkey,
        lamports: r.lamports,
        post_balance: 0,
        reward_type: confirmed_block::RewardType::Unspecified as i32,
        commission: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stored::OptionEof::{None, Some};

    #[test]
    fn legacy_conversion_moves_payload_buffers() {
        let pre = vec![40, 50];
        let post = vec![30, 60];
        let logs = vec!["Program log: transfer".to_owned()];
        let accounts = vec![1, 2];
        let data = vec![3, 4, 5];
        let mint = "mint".to_owned();
        let amount = "100".to_owned();
        let return_bytes = vec![9, 8];
        let reward_key = "reward".to_owned();
        let pointers = (
            pre.as_ptr(),
            post.as_ptr(),
            logs.as_ptr(),
            logs[0].as_ptr(),
            accounts.as_ptr(),
            data.as_ptr(),
            mint.as_ptr(),
            amount.as_ptr(),
            return_bytes.as_ptr(),
            reward_key.as_ptr(),
        );
        let meta = stored_meta_to_proto(StoredTransactionStatusMeta {
            status: stored::TransactionResult::Ok,
            fee: 10,
            pre_balances: pre,
            post_balances: post,
            log_messages: Some(logs),
            inner_instructions: Some(vec![stored::InnerInstructions {
                index: 2,
                instructions: vec![stored::CompiledInstruction {
                    program_id_index: 3,
                    accounts,
                    data,
                }],
            }]),
            pre_token_balances: Some(vec![stored::StoredTransactionTokenBalance {
                account_index: 1,
                mint,
                ui_token_amount: StoredTokenAmount {
                    ui_amount: 1.0,
                    decimals: 2,
                    amount,
                },
            }]),
            post_token_balances: None,
            rewards: Some(vec![stored::StoredExtendedReward {
                pubkey: reward_key,
                lamports: -3,
                post_balance: 7,
                reward_type: std::option::Option::Some(2),
                commission: std::option::Option::Some(5),
            }]),
            return_data: Some(stored::TransactionReturnData {
                program_id: [7; 32],
                data: return_bytes,
            }),
            compute_units_consumed: None,
            cost_units: None,
        });
        let ix = &meta.inner_instructions[0].instructions[0];
        let token = &meta.pre_token_balances[0];
        assert_eq!(
            pointers,
            (
                meta.pre_balances.as_ptr(),
                meta.post_balances.as_ptr(),
                meta.log_messages.as_ptr(),
                meta.log_messages[0].as_ptr(),
                ix.accounts.as_ptr(),
                ix.data.as_ptr(),
                token.mint.as_ptr(),
                token.ui_token_amount.as_ref().unwrap().amount.as_ptr(),
                meta.return_data.as_ref().unwrap().data.as_ptr(),
                meta.rewards[0].pubkey.as_ptr()
            )
        );
        assert_eq!(meta.pre_balances, [40, 50]);
        assert_eq!(meta.post_balances, [30, 60]);
        assert!(!meta.log_messages_none && !meta.inner_instructions_none && !meta.return_data_none);
        assert_eq!(
            meta.rewards[0].reward_type,
            confirmed_block::RewardType::Rent as i32
        );
        assert_eq!(meta.rewards[0].commission, "5");
        assert_eq!(
            token.ui_token_amount.as_ref().unwrap().ui_amount_string,
            "1"
        );
    }
}
