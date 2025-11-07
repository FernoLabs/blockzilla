use ahash::AHashMap;
use anyhow::{Result, anyhow};
use cid::Cid;
use prost::Message;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::{io::Read, mem::MaybeUninit};
use wincode::Deserialize as WincodeDeserialize;
use wincode::SchemaRead;

use crate::compact_log::{CompactLogStream, EncodeConfig, encode_logs};
use crate::transaction_parser::Signature;
use crate::{car_block_reader::CarBlock, confirmed_block, node::Node};

pub trait PubkeyIdProvider {
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32>;
}

pub struct StaticPubkeyIdProvider<'a> {
    map: &'a AHashMap<u128, u32>,
}

impl<'a> StaticPubkeyIdProvider<'a> {
    pub fn new(map: &'a AHashMap<u128, u32>) -> Self {
        Self { map }
    }
}

impl<'a> PubkeyIdProvider for StaticPubkeyIdProvider<'a> {
    #[inline(always)]
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32> {
        let fp = fp128_from_bytes(key);
        self.map.get(&fp).copied()
    }
}

use crate::transaction_parser::{
    CompiledInstruction, MessageHeader, VersionedMessage, VersionedTransaction,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataMode {
    Compact,
    Raw,
    None,
}

trait CarBlockExt {
    fn dataframe_copy_into(
        &self,
        cid: &Cid,
        dst: &mut Vec<u8>,
        inline_prefix: Option<&[u8]>,
    ) -> anyhow::Result<()>;
}

impl CarBlockExt for CarBlock {
    fn dataframe_copy_into(
        &self,
        cid: &Cid,
        dst: &mut Vec<u8>,
        inline_prefix: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let prefix_len = inline_prefix.map_or(0, |p| p.len());
        dst.clear();
        dst.reserve(prefix_len + (64 << 10));

        if let Some(p) = inline_prefix {
            dst.extend_from_slice(p);
        }

        let mut rdr = self.dataframe_reader(cid);
        rdr.read_to_end(dst)
            .map_err(|e| anyhow!("read dataframe chain: {e}"))?;
        Ok(())
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

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub table_account_id: u32,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

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
    pub instructions: Vec<CompactInnerInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactInnerInstruction {
    pub program_id_index: u32,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
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
    pub log_messages: Option<CompactLogStream>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub enum CompactMetadataPayload {
    Compact(CompactMetadata),
    Raw(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactVersionedTx {
    pub signatures: Vec<Signature>,
    pub header: MessageHeader,
    pub account_ids: Vec<u32>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<CompiledInstruction>,
    pub address_table_lookups: Option<Vec<CompactAddressTableLookup>>,
    pub is_versioned: bool,
    pub metadata: Option<CompactMetadataPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, Serialize, Deserialize)]
pub struct CompactBlock {
    pub slot: u64,
    pub txs: Vec<CompactVersionedTx>,
    pub rewards: Vec<CompactReward>,
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
fn id_for_pubkey<P: PubkeyIdProvider>(resolver: &mut P, key: &[u8; 32]) -> Option<u32> {
    resolver.resolve(key)
}

#[derive(Debug, Clone, SchemaRead)]
struct ClientRewardBytesPk {
    pub pubkey: [u8; 32],
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: u8,
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
fn decode_rewards_via_node<P: PubkeyIdProvider>(
    block: &CarBlock,
    resolver: &mut P,
    buf: &mut Vec<u8>,
) -> Result<Vec<CompactReward>> {
    let mut out = Vec::new();

    let Some(reward_cid) = block.block()?.rewards else {
        return Ok(out);
    };

    let Node::Rewards(reward_node) = block.decode(reward_cid.hash_bytes())? else {
        return Ok(out);
    };

    buf.clear();
    let bytes: &[u8] = if let Some(next) = reward_node.data.next {
        let mut rdr = block.dataframe_reader(&next.to_cid()?);
        rdr.read_to_end(buf)?;
        buf.as_slice()
    } else {
        reward_node.data.data
    };

    if let Ok(v) = wincode::deserialize::<Vec<ClientRewardBytesPk>>(bytes) {
        out.reserve(v.len());
        for r in v {
            if let Some(account_id) = id_for_pubkey(resolver, &r.pubkey) {
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

    if let Ok(v) = wincode::deserialize::<Vec<ClientRewardStringPk>>(bytes) {
        out.reserve(v.len());
        for r in v {
            if let Ok(pk) = Pubkey::try_from(r.pubkey.as_str())
                && let Some(account_id) = id_for_pubkey(resolver, &pk.to_bytes())
            {
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

    Err(anyhow!(
        "unrecognized rewards dataframe encoding (expected wincode Vec<ClientReward*>)"
    ))
}

#[inline(always)]
fn looks_like_zstd(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0] == 0x28 && bytes[1] == 0xB5 && bytes[2] == 0x2F && bytes[3] == 0xFD
}

fn metadata_proto_into<'a>(src: &[u8], buf: &'a mut Vec<u8>) -> &'a [u8] {
    buf.clear();
    if looks_like_zstd(src) {
        if let Ok(out) = zstd::bulk::decompress(src, 512 * 1024) {
            *buf = out;
            return buf.as_slice();
        }
        if let Ok(mut dec) = zstd::stream::read::Decoder::new(src) {
            let _ = std::io::copy(&mut dec, buf);
            return buf.as_slice();
        }
    }
    buf.extend_from_slice(src);
    buf.as_slice()
}

fn tx_error_from_meta(meta: &confirmed_block::TransactionStatusMeta) -> Option<CompactTxError> {
    let err = meta.err.as_ref()?;
    let raw = err.err.as_slice();

    if raw.is_empty() {
        return None;
    }

    #[inline(always)]
    fn has(hay: &[u8], needle: &str) -> bool {
        hay.windows(needle.len()).any(|w| w == needle.as_bytes())
    }

    macro_rules! chk {
        ($s:literal, $e:expr) => {
            if has(raw, $s) {
                return Some($e);
            }
        };
    }

    chk!("AccountInUse", CompactTxError::AccountInUse);
    chk!("AccountLoadedTwice", CompactTxError::AccountLoadedTwice);
    chk!(
        "ProgramAccountNotFound",
        CompactTxError::ProgramAccountNotFound
    );
    if has(raw, "AccountNotFound") {
        return Some(CompactTxError::AccountNotFound);
    }
    chk!(
        "InsufficientFundsForFee",
        CompactTxError::InsufficientFundsForFee
    );
    chk!("InvalidAccountForFee", CompactTxError::InvalidAccountForFee);
    chk!("AlreadyProcessed", CompactTxError::AlreadyProcessed);
    chk!("BlockhashNotFound", CompactTxError::BlockhashNotFound);
    chk!("CallChainTooDeep", CompactTxError::CallChainTooDeep);
    chk!(
        "MissingSignatureForFee",
        CompactTxError::MissingSignatureForFee
    );
    chk!("InvalidAccountIndex", CompactTxError::InvalidAccountIndex);
    chk!("SignatureFailure", CompactTxError::SignatureFailure);
    chk!(
        "SanitizedTransaction",
        CompactTxError::SanitizedTransactionError
    );

    if let Some(pos) = raw
        .windows(b"InstructionError(".len())
        .position(|w| w == b"InstructionError(")
    {
        let rest = &raw[pos + "InstructionError(".len()..];
        if let Some(end) = rest.iter().position(|&b| b == b')') {
            let body = &rest[..end];
            if let Some(comma) = body.iter().position(|&b| b == b',') {
                let idx = std::str::from_utf8(&body[..comma])
                    .ok()
                    .and_then(|s| s.trim().parse::<u32>().ok())
                    .unwrap_or(0);
                let code_str = std::str::from_utf8(&body[comma + 1..]).unwrap_or("").trim();
                let error = if let Some(cust) = code_str
                    .strip_prefix("Custom(")
                    .and_then(|s| s.strip_suffix(')'))
                {
                    CompactInstructionError::Custom(cust.parse::<u32>().unwrap_or(0))
                } else {
                    let builtin = match code_str {
                        "InvalidArgument" => 1,
                        "InvalidInstructionData" => 2,
                        "MissingRequiredSignature" => 3,
                        "IncorrectProgramId" => 4,
                        "AccountNotRentExempt" => 5,
                        "UninitializedAccount" => 6,
                        "InvalidAccountData" => 7,
                        "InsufficientFunds" => 8,
                        "ProgramFailedToComplete" => 9,
                        "ProgramFailedToCompile" => 10,
                        "Immutable" => 11,
                        "ArithmeticOverflow" => 12,
                        "DuplicateInstruction" => 13,
                        _ => 0,
                    };
                    CompactInstructionError::Builtin(builtin)
                };
                return Some(CompactTxError::InstructionError { index: idx, error });
            }
        }
    }

    if has(raw, "ProgramError") || has(raw, "ProgramFailed") || has(raw, "RuntimeError") {
        return Some(CompactTxError::ProgramError);
    }

    Some(CompactTxError::Other {
        name: if raw.len() <= 64 {
            String::from_utf8_lossy(raw).to_string()
        } else {
            format!("err[{} bytes]", raw.len())
        },
    })
}

#[inline(always)]
fn parse_ui_amount_to_int(amount_str: &str) -> Option<u128> {
    amount_str.parse::<u128>().ok()
}

fn token_balance_to_compact<P: PubkeyIdProvider>(
    tb: &confirmed_block::TokenBalance,
    resolver: &mut P,
) -> Option<CompactTokenBalanceMeta> {
    let mint_id = Pubkey::try_from(tb.mint.as_str())
        .ok()
        .and_then(|p| id_for_pubkey(resolver, &p.to_bytes()))?;
    let owner_id = Pubkey::try_from(tb.owner.as_str())
        .ok()
        .and_then(|p| id_for_pubkey(resolver, &p.to_bytes()))?;
    let program_id_id = Pubkey::try_from(tb.program_id.as_str())
        .ok()
        .and_then(|p| id_for_pubkey(resolver, &p.to_bytes()))?;

    let (amount, decimals) = match &tb.ui_token_amount {
        Some(uta) => {
            let amt = parse_ui_amount_to_int(&uta.amount).unwrap_or(0);
            let dec = uta.decimals as u8;
            (amt, dec)
        }
        None => (0, 0),
    };

    Some(CompactTokenBalanceMeta {
        mint_id,
        owner_id,
        program_id_id,
        amount,
        decimals,
    })
}

fn meta_to_compact<P: PubkeyIdProvider>(
    meta: &confirmed_block::TransactionStatusMeta,
    resolver: &mut P,
) -> CompactMetadata {
    let pre_balances = meta.pre_balances.clone();
    let post_balances = meta.post_balances.clone();

    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .filter_map(|tb| token_balance_to_compact(tb, resolver))
        .collect();

    let post_token_balances = meta
        .post_token_balances
        .iter()
        .filter_map(|tb| token_balance_to_compact(tb, resolver))
        .collect();

    let mut loaded_writable_ids = Vec::new();
    let mut loaded_readonly_ids = Vec::new();
    for addr in &meta.loaded_writable_addresses {
        if let Ok(pk) = Pubkey::try_from(addr.as_slice())
            && let Some(id) = id_for_pubkey(resolver, &pk.to_bytes())
        {
            loaded_writable_ids.push(id);
        }
    }
    for addr in &meta.loaded_readonly_addresses {
        if let Ok(pk) = Pubkey::try_from(addr.as_slice())
            && let Some(id) = id_for_pubkey(resolver, &pk.to_bytes())
        {
            loaded_readonly_ids.push(id);
        }
    }

    let return_data = if let Some(rd) = &meta.return_data {
        if let Ok(pk) = Pubkey::try_from(rd.program_id.as_slice()) {
            id_for_pubkey(resolver, &pk.to_bytes()).map(|pid| CompactReturnData {
                program_id_id: pid,
                data: rd.data.clone(),
            })
        } else {
            None
        }
    } else {
        None
    };

    let inner_instructions: Vec<CompactInnerInstructions> = meta
        .inner_instructions
        .clone()
        .into_iter()
        .map(|ii| {
            let instructions = ii
                .instructions
                .into_iter()
                .map(|instr| CompactInnerInstruction {
                    program_id_index: instr.program_id_index,
                    accounts: instr.accounts,
                    data: instr.data,
                    stack_height: instr.stack_height,
                })
                .collect::<Vec<_>>();
            CompactInnerInstructions {
                index: ii.index,
                instructions,
            }
        })
        .collect::<Vec<_>>();

    let log_messages = if meta.log_messages.is_empty() {
        None
    } else {
        let mut lookup_pid = |base58: &str| -> Option<u32> {
            Pubkey::try_from(base58)
                .ok()
                .and_then(|pk| resolver.resolve(&pk.to_bytes()))
        };

        Some(encode_logs(
            &meta.log_messages,
            &mut lookup_pid,
            EncodeConfig::default(),
        ))
    };

    let err = tx_error_from_meta(meta);

    CompactMetadata {
        fee: meta.fee,
        err,
        pre_balances,
        post_balances,
        pre_token_balances,
        post_token_balances,
        loaded_writable_ids,
        loaded_readonly_ids,
        return_data,
        inner_instructions: Some(inner_instructions),
        log_messages,
    }
}

pub fn carblock_to_compactblock_inplace<P: PubkeyIdProvider>(
    block: &CarBlock,
    resolver: &mut P,
    metadata_mode: MetadataMode,
    buf_tx: &mut Vec<u8>,
    buf_meta: &mut Vec<u8>,
    out: &mut CompactBlock,
) -> Result<()> {
    out.slot = block.block()?.slot;

    out.rewards.clear();
    {
        let mut rewards_buf = Vec::<u8>::with_capacity(64 << 10);
        let rewards =
            decode_rewards_via_node(block, resolver, &mut rewards_buf).unwrap_or_default();
        out.rewards.reserve(rewards.len());
        out.rewards.extend(rewards);
    }

    let mut target_len = 0usize;
    for entry_cid_res in block.block()?.entries.iter() {
        let entry_cid = entry_cid_res?;
        let Node::Entry(entry) = block.decode(entry_cid.hash_bytes())? else {
            continue;
        };
        target_len += entry.transactions.len();
    }
    if out.txs.capacity() < target_len {
        out.txs.reserve(target_len - out.txs.len());
    }
    let mut written = 0usize;

    let mut reusable_tx = MaybeUninit::<VersionedTransaction>::uninit();

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

            let tx_bytes: &[u8] = match tx_node.data.next {
                None => tx_node.data.data,
                Some(df_cbor) => {
                    let df_cid = df_cbor.to_cid()?;
                    buf_tx.clear();
                    block.dataframe_copy_into(&df_cid, buf_tx, None)?;
                    &*buf_tx
                }
            };

            VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)
                .map_err(|_| anyhow!("failed to decode VersionedTransaction"))?;
            let tx_ref = unsafe { reusable_tx.assume_init_ref() };

            if out.txs.len() == written {
                out.txs.push(CompactVersionedTx {
                    signatures: Vec::with_capacity(2),
                    header: MessageHeader {
                        num_required_signatures: 0,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_ids: Vec::with_capacity(32),
                    recent_blockhash: [0; 32],
                    instructions: Vec::with_capacity(8),
                    address_table_lookups: None,
                    is_versioned: false,
                    metadata: None,
                });
            }
            let vtx = &mut out.txs[written];

            vtx.signatures.clear();
            vtx.account_ids.clear();
            vtx.instructions.clear();
            if let Some(ref mut v) = vtx.address_table_lookups {
                v.clear();
            }
            vtx.metadata = None;

            vtx.signatures.reserve(tx_ref.signatures.len());
            for s in &tx_ref.signatures {
                vtx.signatures.push(Signature(s.0));
            }

            vtx.header = *tx_ref.message.header();
            vtx.is_versioned = matches!(tx_ref.message, VersionedMessage::V0(_));
            vtx.recent_blockhash = match &tx_ref.message {
                VersionedMessage::Legacy(m) => m.recent_blockhash,
                VersionedMessage::V0(m) => m.recent_blockhash,
            };

            let static_keys = tx_ref.message.static_account_keys();
            vtx.account_ids.reserve(static_keys.len());
            for k in static_keys {
                if let Some(uid) = id_for_pubkey(resolver, k) {
                    vtx.account_ids.push(uid);
                } else {
                    return Err(anyhow!(
                        "registry miss for pubkey {}",
                        Pubkey::new_from_array(*k)
                    ));
                }
            }

            {
                vtx.instructions.reserve(tx_ref.message.instructions_len());
                for ix in tx_ref.message.instructions_iter() {
                    vtx.instructions.push(ix.clone());
                }
            }

            vtx.address_table_lookups = match tx_ref.message.address_table_lookups() {
                None => None,
                Some(lookups) => {
                    let mut vec = vtx
                        .address_table_lookups
                        .take()
                        .unwrap_or_else(|| Vec::with_capacity(2));
                    for l in lookups {
                        if let Some(uid) = id_for_pubkey(resolver, &l.account_key) {
                            vec.push(CompactAddressTableLookup {
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
                    Some(vec)
                }
            };

            let df = &tx_node.metadata;

            vtx.metadata = if matches!(metadata_mode, MetadataMode::None) {
                None
            } else if let Some(next) = df.next {
                let df_cid = next.to_cid()?;
                buf_meta.clear();
                block.dataframe_copy_into(&df_cid, buf_meta, Some(df.data))?;

                let raw_slice = metadata_proto_into(&*buf_meta, buf_tx);

                match metadata_mode {
                    MetadataMode::Compact => {
                        match confirmed_block::TransactionStatusMeta::decode(raw_slice) {
                            Ok(parsed) => Some(CompactMetadataPayload::Compact(meta_to_compact(
                                &parsed, resolver,
                            ))),
                            Err(_) => {
                                let owned = std::mem::take(buf_tx);
                                Some(CompactMetadataPayload::Raw(owned))
                            }
                        }
                    }
                    MetadataMode::Raw => {
                        let owned = std::mem::take(buf_tx);
                        Some(CompactMetadataPayload::Raw(owned))
                    }
                    MetadataMode::None => None,
                }
            } else {
                let raw_slice = metadata_proto_into(df.data, buf_meta);

                match metadata_mode {
                    MetadataMode::Compact => {
                        match confirmed_block::TransactionStatusMeta::decode(raw_slice) {
                            Ok(parsed) => Some(CompactMetadataPayload::Compact(meta_to_compact(
                                &parsed, resolver,
                            ))),
                            Err(_) => {
                                let owned = std::mem::take(buf_meta);
                                Some(CompactMetadataPayload::Raw(owned))
                            }
                        }
                    }
                    MetadataMode::Raw => {
                        let owned = std::mem::take(buf_meta);
                        Some(CompactMetadataPayload::Raw(owned))
                    }
                    MetadataMode::None => None,
                }
            };

            unsafe { std::ptr::drop_in_place(tx_ref as *const _ as *mut VersionedTransaction) };

            written += 1;
        }
    }

    out.txs.truncate(written);
    Ok(())
}
