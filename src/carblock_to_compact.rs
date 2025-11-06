use ahash::AHashMap;
use anyhow::{Result, anyhow};
use serde::Deserialize;
use serde::Serialize;
use solana_pubkey::Pubkey;
use std::{io::Read, mem::MaybeUninit};
use wincode::Deserialize as WincodeDeserialize;
use wincode::SchemaRead;

use crate::{car_block_reader::CarBlock, node::Node};

use crate::transaction_parser::{
    CompiledInstruction, MessageAddressTableLookup, MessageHeader, VersionedMessage,
    VersionedTransaction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead)]
#[repr(transparent)]
pub struct CompactSignature(pub [u8; 64]);

// Fully-qualified serde impls; avoids `use serde::*`
impl serde::Serialize for CompactSignature {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}
impl<'de> serde::Deserialize<'de> for CompactSignature {
    #[inline]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SigVisitor;
        impl<'de> serde::de::Visitor<'de> for SigVisitor {
            type Value = CompactSignature;
            fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(f, "a 64-byte signature")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v.len() != 64 {
                    return Err(E::invalid_length(v.len(), &self));
                }
                let mut arr = [0u8; 64];
                arr.copy_from_slice(v);
                Ok(CompactSignature(arr))
            }
            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut arr = [0u8; 64];
                for i in 0..64 {
                    arr[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(CompactSignature(arr))
            }
        }
        deserializer.deserialize_bytes(SigVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompactRewardType {
    Fee = 0,
    Rent = 1,
    Staking = 2,
    Voting = 3,
    Revoked = 4,
    Unspecified = 255,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactReward {
    pub account_id: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: CompactRewardType,
    pub commission: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactMessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub table_account_id: u32,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

/// ===== TransactionError (compact) =====
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub enum CompactInstructionError {
    Builtin(u32),
    Custom(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub enum CompactTxError {
    InstructionError {
        index: u32,
        error: CompactInstructionError,
    },
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    SanitizedTransactionError,
    ProgramError,
    Other {
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactTokenBalanceMeta {
    pub mint_id: u32,
    pub owner_id: u32,
    pub program_id_id: u32,
    pub amount: u128,
    pub decimals: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactReturnData {
    pub program_id_id: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactInnerInstructions {
    pub index: u32,
    pub instructions: Vec<CompactInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactMetadata {
    pub fee: u64,
    pub err: Option<CompactTxError>,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub pre_token_balances: Vec<CompactTokenBalanceMeta>,
    pub post_token_balances: Vec<CompactTokenBalanceMeta>,
    pub loaded_writable_ids: Vec<u32>,
    pub loaded_readonly_ids: Vec<u32>,
    pub return_data: Option<CompactReturnData>,
    pub inner_instructions: Option<Vec<CompactInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactVersionedTx {
    pub signatures: Vec<CompactSignature>,
    pub header: CompactMessageHeader,
    pub account_ids: Vec<u32>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<CompactInstruction>,
    pub address_table_lookups: Option<Vec<CompactAddressTableLookup>>,
    pub is_versioned: bool,
    pub metadata: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactBlock {
    pub slot: u64,
    pub txs: Vec<CompactVersionedTx>,
    pub rewards: Vec<CompactReward>,
}

/// ===============================
/// Helpers
/// ===============================
#[inline(always)]
fn header_to_compact(h: &MessageHeader) -> CompactMessageHeader {
    CompactMessageHeader {
        num_required_signatures: h.num_required_signatures,
        num_readonly_signed_accounts: h.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: h.num_readonly_unsigned_accounts,
    }
}

#[inline(always)]
fn fp128_from_bytes(b: &[u8; 32]) -> u128 {
    let a = u128::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ]);
    let c = u128::from_le_bytes([
        b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23], b[24], b[25], b[26], b[27], b[28],
        b[29], b[30], b[31],
    ]);
    a ^ c.rotate_left(64)
}
#[inline(always)]
fn id_for_pubkey(fp2id: &AHashMap<u128, u32>, key: &[u8; 32]) -> Option<u32> {
    fp2id.get(&fp128_from_bytes(key)).copied()
}

#[inline(always)]
fn map_static_keys_to_ids(
    static_keys: &[[u8; 32]],
    fp2id: &AHashMap<u128, u32>,
) -> Result<Vec<u32>> {
    let mut ids = Vec::with_capacity(static_keys.len());
    for k in static_keys {
        if let Some(uid) = id_for_pubkey(fp2id, k) {
            ids.push(uid);
        } else {
            return Err(anyhow!(
                "registry miss for pubkey {}",
                Pubkey::new_from_array(*k)
            ));
        }
    }
    Ok(ids)
}

#[inline(always)]
fn copy_instructions<'a>(
    ixs: impl Iterator<Item = &'a CompiledInstruction>,
) -> Vec<CompactInstruction> {
    let mut out = Vec::with_capacity(8);
    for ix in ixs {
        out.push(CompactInstruction {
            program_id_index: ix.program_id_index,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        });
    }
    out
}

#[inline(always)]
fn map_lookups_to_ids(
    lookups: &[MessageAddressTableLookup],
    fp2id: &AHashMap<u128, u32>,
) -> Result<Vec<CompactAddressTableLookup>> {
    let mut out = Vec::with_capacity(lookups.len());
    for l in lookups {
        if let Some(uid) = id_for_pubkey(fp2id, &l.account_key) {
            out.push(CompactAddressTableLookup {
                table_account_id: uid,
                writable_indexes: l.writable_indexes.clone(),
                readonly_indexes: l.readonly_indexes.clone(),
            });
        } else {
            return Err(anyhow!(
                "registry miss for address table account {}",
                Pubkey::new_from_array(l.account_key)
            ));
        }
    }
    Ok(out)
}

/// ===============================
/// Rewards — decode via Rewards node (CarBlock pattern)
/// ===============================

/// Client reward payloads we expect to read with wincode + SchemaRead.
#[derive(Debug, Clone, SchemaRead)]
struct ClientRewardBytesPk {
    pub pubkey: [u8; 32],
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: u8, // 0..=4 or 255
    pub commission: Option<u8>,
}
#[derive(Debug, Clone, SchemaRead)]
struct ClientRewardStringPk {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: u8,
    pub commission: Option<u8>,
}

#[inline(always)]
fn reward_tag_to_compact(tag: u8) -> CompactRewardType {
    match tag {
        0 => CompactRewardType::Fee,
        1 => CompactRewardType::Rent,
        2 => CompactRewardType::Staking,
        3 => CompactRewardType::Voting,
        4 => CompactRewardType::Revoked,
        _ => CompactRewardType::Unspecified,
    }
}
fn decode_rewards_via_node(
    block: &CarBlock,
    fp2id: &AHashMap<u128, u32>,
    buf: &mut Vec<u8>,
) -> Result<Vec<CompactReward>> {
    let mut out = Vec::new();

    let Some(reward_cid) = block.block()?.rewards else {
        return Ok(out);
    };

    let Node::Rewards(reward_node) = block.decode(reward_cid.hash_bytes())? else {
        return Ok(out);
    };

    // DataFrame chaining exactly as you had
    buf.clear();
    let bytes: &[u8] = if let Some(next) = reward_node.data.next {
        let mut rdr = block.dataframe_reader(&next.to_cid()?);
        rdr.read_to_end(buf)?;
        buf.as_slice()
    } else {
        reward_node.data.data
    };

    // 1) Try bytes-pubkey payloads
    if let Ok(v) = wincode::deserialize::<Vec<ClientRewardBytesPk>>(bytes) {
        out.reserve(v.len());
        for r in v {
            if let Some(account_id) = id_for_pubkey(fp2id, &r.pubkey) {
                out.push(CompactReward {
                    account_id,
                    lamports: r.lamports,
                    post_balance: r.post_balance,
                    reward_type: reward_tag_to_compact(r.reward_type),
                    commission: r.commission,
                });
            }
        }
        return Ok(out);
    }

    // 2) Fallback to string-pubkey payloads
    if let Ok(v) = wincode::deserialize::<Vec<ClientRewardStringPk>>(bytes) {
        out.reserve(v.len());
        for r in v {
            if let Ok(pk) = Pubkey::try_from(r.pubkey.as_str()) {
                if let Some(account_id) = id_for_pubkey(fp2id, &pk.to_bytes()) {
                    out.push(CompactReward {
                        account_id,
                        lamports: r.lamports,
                        post_balance: r.post_balance,
                        reward_type: reward_tag_to_compact(r.reward_type),
                        commission: r.commission,
                    });
                }
            }
        }
        return Ok(out);
    }

    Err(anyhow!(
        "unrecognized rewards dataframe encoding (expected wincode Vec<ClientReward*>)"
    ))
}

/// ===============================
/// Metadata extraction helpers
/// ===============================
fn metadata_proto_bytes(bytes: &[u8]) -> Vec<u8> {
    match zstd::bulk::decompress(bytes, 512 * 1024) {
        Ok(buf) => buf,
        Err(_) => {
            let mut tmp = Vec::new();
            if let Ok(mut dec) = zstd::stream::read::Decoder::new(bytes) {
                if std::io::copy(&mut dec, &mut tmp).is_ok() {
                    return tmp;
                }
            }
            bytes.to_vec()
        }
    }
}

/// ===============================
/// tx (already decoded) -> CompactVersionedTx
/// ===============================
pub fn tx_decoded_to_compact_full(
    tx: &VersionedTransaction,
    fp2id: &AHashMap<u128, u32>,
    meta_bytes_opt: Option<&[u8]>,
) -> Result<CompactVersionedTx> {
    let signatures = tx
        .signatures
        .iter()
        .map(|s| CompactSignature(s.0))
        .collect::<Vec<_>>();

    let header = header_to_compact(tx.message.header());
    let is_versioned = matches!(tx.message, VersionedMessage::V0(_));
    let static_keys = tx.message.static_account_keys();
    let account_ids = map_static_keys_to_ids(static_keys, fp2id)?;
    let recent_blockhash = match &tx.message {
        VersionedMessage::Legacy(m) => m.recent_blockhash,
        VersionedMessage::V0(m) => m.recent_blockhash,
    };
    let instructions = copy_instructions(tx.message.instructions_iter());
    let address_table_lookups = match tx.message.address_table_lookups() {
        None => None,
        Some(lookups) => Some(map_lookups_to_ids(lookups, fp2id)?),
    };

    let metadata = meta_bytes_opt.map(metadata_proto_bytes);

    Ok(CompactVersionedTx {
        signatures,
        header,
        account_ids,
        recent_blockhash,
        instructions,
        address_table_lookups,
        is_versioned,
        metadata,
    })
}

/// ===============================
/// CarBlock -> CompactBlock (EXACT reader pattern)
/// ===============================
pub fn carblock_to_compactblock(
    block: &CarBlock,
    fp2id: &AHashMap<u128, u32>,
    include_metadata: bool,
) -> Result<CompactBlock> {
    let slot = block.block()?.slot;

    // Rewards via Rewards node
    let mut rewards_buf = Vec::<u8>::with_capacity(64 * 1024);
    let rewards = decode_rewards_via_node(block, fp2id, &mut rewards_buf).unwrap_or_default();

    // Transactions
    let mut txs: Vec<CompactVersionedTx> = Vec::new();
    let mut reusable_tx = MaybeUninit::<VersionedTransaction>::uninit();
    let mut buf_tx = Vec::<u8>::with_capacity(128 * 1024);
    let mut buf_meta = Vec::<u8>::with_capacity(128 * 1024);

    // EXACT iteration pattern as in your `read_block` example
    for entry_cid_res in block.block()?.entries.iter() {
        let entry_cid = entry_cid_res?;
        let Node::Entry(entry) = block.decode(entry_cid.hash_bytes())? else {
            continue;
        };

        for tx_cid_res in entry.transactions.iter() {
            let tx_cid = tx_cid_res?;
            let Node::Transaction(tx_node) = block.decode(tx_cid.hash_bytes())? else {
                continue;
            };

            // tx bytes: inline or streamed
            let tx_bytes: &[u8] = match tx_node.data.next {
                None => tx_node.data.data,
                Some(df_cbor) => {
                    let df_cid = df_cbor.to_cid()?;
                    let mut rdr = block.dataframe_reader(&df_cid);
                    buf_tx.clear();
                    rdr.read_to_end(&mut buf_tx)?;
                    &buf_tx
                }
            };

            // Decode VersionedTransaction into reusable slot
            VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)
                .map_err(|_| anyhow!("failed to decode VersionedTransaction"))?;
            let tx_ref = unsafe { reusable_tx.assume_init_ref() };

            // Metadata bytes come from TransactionNode.metadata (DataFrame)
            let meta_bytes_opt: Option<&[u8]> = if include_metadata {
                let df = &tx_node.metadata;
                if let Some(next) = df.next {
                    let df_cid = next.to_cid()?;
                    let mut rdr = block.dataframe_reader(&df_cid);
                    buf_meta.clear();
                    rdr.read_to_end(&mut buf_meta)?;
                    Some(&buf_meta)
                } else {
                    Some(df.data)
                }
            } else {
                None
            };

            // Build compact tx
            let compact_tx = tx_decoded_to_compact_full(tx_ref, fp2id, meta_bytes_opt)?;

            // Drop decoded tx in-place (we reused its memory)
            unsafe { std::ptr::drop_in_place(tx_ref as *const _ as *mut VersionedTransaction) };

            txs.push(compact_tx);
        }
    }

    Ok(CompactBlock { slot, txs, rewards })
}
