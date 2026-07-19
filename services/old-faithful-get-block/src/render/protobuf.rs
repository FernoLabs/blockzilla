use super::base58;
use of_car_reader::confirmed_block::{self as proto, Rewards};
use of_car_reader::confirmed_block_borrowed::solana::storage::ConfirmedBlock as quick_proto;
use of_car_reader::metadata_decoder::{ZstdReusableDecoder, decode_rewards_from_frame};
use of_car_reader::node::OwnedDataFrame;
use of_car_reader::versioned_transaction::{VersionedMessage, VersionedTransaction};
use of_car_reader::{CarBlockReader, car_block_group::CarBlockGroup};
use prost::Message as _;
use quick_protobuf::Writer;
use std::borrow::Cow;
use std::io::Cursor;

pub fn car_bytes_to_protobuf(
    slot: u64,
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
    include_transaction_meta: bool,
) -> Result<Vec<u8>, String> {
    if !include_transaction_meta {
        return car_bytes_to_quick_protobuf_no_meta(
            slot,
            bytes,
            previous_blockhash,
            include_rewards,
        );
    }

    let mut block = read_car_block(bytes, include_rewards)?;
    let rewards = if include_rewards {
        decode_block_rewards_proto(block.rewards.as_ref())?
    } else {
        None
    };

    let transactions = if include_transaction_meta {
        let mut tx_iter = block.transactions();
        let mut transactions = Vec::new();
        while let Some((tx, meta)) = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?
        {
            transactions.push(proto::ConfirmedTransaction {
                transaction: Some(proto_transaction(tx)),
                meta: meta.cloned(),
            });
        }
        transactions
    } else {
        let mut tx_iter = block.transactions_no_meta();
        let mut transactions = Vec::new();
        while let Some(tx) = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?
        {
            transactions.push(proto::ConfirmedTransaction {
                transaction: Some(proto_transaction(tx)),
                meta: None,
            });
        }
        transactions
    };

    let confirmed = proto::ConfirmedBlock {
        previous_blockhash: encode_previous_blockhash(previous_blockhash),
        blockhash: base58::encode_string(&block.blockhash)?,
        parent_slot: block.parent_slot.unwrap_or_default(),
        transactions,
        rewards: rewards
            .as_ref()
            .map(|rewards| rewards.rewards.clone())
            .unwrap_or_default(),
        block_time: rpc_block_time(block.block_time)
            .map(|timestamp| proto::UnixTimestamp { timestamp }),
        block_height: block
            .block_height
            .map(|block_height| proto::BlockHeight { block_height }),
        num_partitions: rewards.and_then(|rewards| rewards.num_partitions),
    };

    Ok(confirmed.encode_to_vec())
}

fn car_bytes_to_quick_protobuf_no_meta(
    slot: u64,
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    include_rewards: bool,
) -> Result<Vec<u8>, String> {
    let input_len = bytes.len();
    let mut block = read_car_block(bytes, include_rewards)?;
    let rewards = if include_rewards {
        decode_block_rewards_proto(block.rewards.as_ref())?
    } else {
        None
    };
    let parent_slot = block.parent_slot.unwrap_or_default();
    let block_time = block.block_time;
    let block_height = block.block_height;

    let previous_blockhash = encode_previous_blockhash(previous_blockhash);
    let blockhash = base58::encode_string(&block.blockhash)?;
    let mut out = Vec::with_capacity(input_len);
    let mut writer = Writer::new(&mut out);

    if previous_blockhash != "" {
        writer
            .write_with_tag(10, |writer| writer.write_string(&previous_blockhash))
            .map_err(|err| format!("Failed to encode protobuf previous_blockhash: {err}"))?;
    }
    writer
        .write_with_tag(18, |writer| writer.write_string(&blockhash))
        .map_err(|err| format!("Failed to encode protobuf blockhash: {err}"))?;
    if parent_slot != 0 {
        writer
            .write_with_tag(24, |writer| writer.write_uint64(parent_slot))
            .map_err(|err| format!("Failed to encode protobuf parent_slot: {err}"))?;
    }

    {
        let mut tx_iter = block.transactions_no_meta();
        while let Some(tx) = tx_iter
            .next_tx()
            .map_err(|err| format!("Failed to parse transaction: {err}"))?
        {
            let confirmed = quick_proto::ConfirmedTransaction {
                transaction: Some(quick_transaction(tx)),
                meta: None,
            };
            writer
                .write_with_tag(34, |writer| writer.write_message(&confirmed))
                .map_err(|err| format!("Failed to encode protobuf transaction: {err}"))?;
        }
    }

    if let Some(rewards) = rewards {
        for reward in rewards.rewards {
            let reward = quick_reward(reward);
            writer
                .write_with_tag(42, |writer| writer.write_message(&reward))
                .map_err(|err| format!("Failed to encode protobuf rewards: {err}"))?;
        }
        if let Some(num_partitions) = rewards.num_partitions {
            let num_partitions = quick_proto::NumPartitions {
                num_partitions: num_partitions.num_partitions,
            };
            writer
                .write_with_tag(66, |writer| writer.write_message(&num_partitions))
                .map_err(|err| format!("Failed to encode protobuf num_partitions: {err}"))?;
        }
    }

    if let Some(timestamp) = rpc_block_time(block_time) {
        let block_time = quick_proto::UnixTimestamp { timestamp };
        writer
            .write_with_tag(50, |writer| writer.write_message(&block_time))
            .map_err(|err| format!("Failed to encode protobuf block_time: {err}"))?;
    }
    if let Some(block_height) = block_height {
        let block_height = quick_proto::BlockHeight { block_height };
        writer
            .write_with_tag(58, |writer| writer.write_message(&block_height))
            .map_err(|err| format!("Failed to encode protobuf block_height: {err}"))?;
    }

    let _ = slot;
    Ok(out)
}

fn read_car_block(bytes: Vec<u8>, include_rewards: bool) -> Result<CarBlockGroup, String> {
    let len = bytes.len();
    let cursor = Cursor::new(bytes);
    let mut reader = CarBlockReader::with_capacity(cursor, len);
    let mut block = if include_rewards {
        CarBlockGroup::new()
    } else {
        CarBlockGroup::without_rewards()
    };

    let res = reader
        .read_until_block_into(&mut block)
        .map_err(|err| format!("Failed to read CAR block: {err}"))?;

    if !res {
        return Err("CAR reader returned false".to_string());
    }

    Ok(block)
}

fn decode_block_rewards_proto(rewards: Option<&OwnedDataFrame>) -> Result<Option<Rewards>, String> {
    let Some(rewards) = rewards else {
        return Ok(None);
    };

    let mut decoded = Rewards::default();
    let mut zstd = ZstdReusableDecoder::new();
    decode_rewards_from_frame(&rewards.data, &mut decoded, &mut zstd)
        .map_err(|err| format!("Failed to decode rewards: {err}"))?;
    Ok(Some(decoded))
}

fn rpc_block_time(block_time: Option<i64>) -> Option<i64> {
    block_time.filter(|value| *value != 0)
}

fn encode_previous_blockhash(previous_blockhash: Option<[u8; 32]>) -> String {
    previous_blockhash
        .as_ref()
        .map(|blockhash| base58::encode_string(blockhash).expect("32-byte base58 encoding"))
        .unwrap_or_default()
}

fn quick_transaction<'a>(tx: &'a VersionedTransaction<'a>) -> quick_proto::Transaction<'a> {
    quick_proto::Transaction {
        signatures: tx
            .signatures
            .iter()
            .map(|signature| Cow::Borrowed(signature.as_slice()))
            .collect(),
        message: Some(quick_message(&tx.message)),
    }
}

fn quick_message<'a>(message: &'a VersionedMessage<'a>) -> quick_proto::Message<'a> {
    let (account_keys, header, instructions, recent_blockhash, versioned, address_table_lookups) =
        match message {
            VersionedMessage::Legacy(m) => (
                m.account_keys.as_slice(),
                m.header,
                m.instructions.as_slice(),
                m.recent_blockhash.as_slice(),
                false,
                [].as_slice(),
            ),
            VersionedMessage::V0(m) => (
                m.account_keys.as_slice(),
                m.header,
                m.instructions.as_slice(),
                m.recent_blockhash.as_slice(),
                true,
                m.address_table_lookups.as_slice(),
            ),
        };

    quick_proto::Message {
        header: Some(quick_proto::MessageHeader {
            num_required_signatures: header.num_required_signatures as u32,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
        }),
        account_keys: account_keys
            .iter()
            .map(|account_key| Cow::Borrowed(account_key.as_slice()))
            .collect(),
        recent_blockhash: Cow::Borrowed(recent_blockhash),
        instructions: instructions
            .iter()
            .map(|ix| quick_proto::CompiledInstruction {
                program_id_index: ix.program_id_index as u32,
                accounts: Cow::Borrowed(ix.accounts.as_slice()),
                data: Cow::Borrowed(ix.data.as_slice()),
            })
            .collect(),
        versioned,
        address_table_lookups: address_table_lookups
            .iter()
            .map(|lookup| quick_proto::MessageAddressTableLookup {
                account_key: Cow::Borrowed(lookup.account_key.as_slice()),
                writable_indexes: Cow::Borrowed(lookup.writable_indexes.as_slice()),
                readonly_indexes: Cow::Borrowed(lookup.readonly_indexes.as_slice()),
            })
            .collect(),
    }
}

fn quick_reward(reward: proto::Reward) -> quick_proto::Reward<'static> {
    quick_proto::Reward {
        pubkey: Cow::Owned(reward.pubkey),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: quick_proto::RewardType::from(reward.reward_type),
        commission: Cow::Owned(reward.commission),
    }
}

fn proto_transaction(tx: &VersionedTransaction<'_>) -> proto::Transaction {
    let (account_keys, header, instructions, recent_blockhash, versioned, address_table_lookups) =
        match &tx.message {
            VersionedMessage::Legacy(m) => (
                m.account_keys.clone(),
                m.header,
                m.instructions.clone(),
                m.recent_blockhash,
                false,
                Vec::new(),
            ),
            VersionedMessage::V0(m) => (
                m.account_keys.clone(),
                m.header,
                m.instructions.clone(),
                m.recent_blockhash,
                true,
                m.address_table_lookups.clone(),
            ),
        };

    proto::Transaction {
        signatures: tx.signatures.iter().map(|s| s.to_vec()).collect(),
        message: Some(proto::Message {
            header: Some(proto::MessageHeader {
                num_required_signatures: header.num_required_signatures as u32,
                num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
                num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
            }),
            account_keys: account_keys.into_iter().map(|a| a.to_vec()).collect(),
            recent_blockhash: recent_blockhash.to_vec(),
            instructions: instructions
                .into_iter()
                .map(|ix| proto::CompiledInstruction {
                    program_id_index: ix.program_id_index as u32,
                    accounts: ix.accounts,
                    data: ix.data,
                })
                .collect(),
            versioned,
            address_table_lookups: address_table_lookups
                .into_iter()
                .map(|lookup| proto::MessageAddressTableLookup {
                    account_key: lookup.account_key.to_vec(),
                    writable_indexes: lookup.writable_indexes,
                    readonly_indexes: lookup.readonly_indexes,
                })
                .collect(),
        }),
    }
}
