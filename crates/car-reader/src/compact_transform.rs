use ahash::{AHashMap, AHashSet};
use anyhow::{Result, anyhow};
use cid::Cid;
use optimized_archive::{
    compact_block::{
        CompactAddressTableLookup, CompactBlock, CompactInnerInstruction, CompactInnerInstructions,
        CompactInstructionError, CompactMetadata, CompactMetadataPayload, CompactReturnData,
        CompactReward, CompactRewardType, CompactTokenBalanceMeta, CompactTxError,
        CompactVersionedTx,
    },
    compact_log::{CompactLogStream, EncodeConfig, encode_logs},
    confirmed_block,
    meta_decode::decode_transaction_status_meta_bytes,
    partial_meta::extract_metadata_pubkeys,
    transaction_parser::{
        CompiledInstruction, MessageHeader, Signature, VersionedMessage, VersionedTransaction,
    },
};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::{convert::TryFrom, io::Read, mem::MaybeUninit, str::FromStr};

use crate::{
    car_block_reader::CarBlock,
    node::{Node, TransactionNode},
};

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

/// Fast filter that checks if a parsed transaction involves the target mint or tracked accounts
/// This checks:
/// 1. Static account keys (always present)
/// 2. Address table lookup accounts (v0 transactions)
/// 3. Loaded addresses from metadata (requires parsing metadata)
#[inline]
pub fn transaction_involves_token(
    tx: &VersionedTransaction,
    mint: &[u8; 32],
    tracked_accounts: &AHashSet<[u8; 32]>,
    token_program: &[u8; 32],
) -> bool {
    let static_keys = tx.message.static_account_keys();

    // Fast reject: check if token program is even present
    let has_token_program = static_keys.iter().any(|k| k == token_program);
    if !has_token_program {
        return false;
    }

    // Check static keys for mint or tracked accounts
    for key in static_keys {
        if key == mint || tracked_accounts.contains(key) {
            return true;
        }
    }

    // Check address table lookups if this is a versioned transaction
    if let VersionedMessage::V0(msg) = &tx.message {
        for lookup in &msg.address_table_lookups {
            if &lookup.account_key == mint || tracked_accounts.contains(&lookup.account_key) {
                return true;
            }
        }
    }

    false
}

/// More comprehensive filter that also checks loaded addresses from metadata
/// This is slower but catches all possible token interactions including those
/// that use address lookup tables to load the mint or token accounts
pub fn transaction_involves_token_with_metadata(
    tx: &VersionedTransaction,
    metadata_bytes: &[u8],
    mint: &[u8; 32],
    tracked_accounts: &AHashSet<[u8; 32]>,
    token_program: &[u8; 32],
    keys_buffer: &mut SmallVec<[Pubkey; 256]>,
) -> bool {
    let static_keys = tx.message.static_account_keys();
    if !static_keys.iter().any(|k| k == token_program) {
        return false;
    }

    if transaction_involves_token(tx, mint, tracked_accounts, token_program) {
        return true;
    }

    // The fast path already covers static keys and address table accounts
    // If it fails, we only need to inspect metadata
    keys_buffer.clear();
    if extract_metadata_pubkeys(metadata_bytes, keys_buffer).is_ok() {
        for pk in keys_buffer.iter() {
            let key = pk.to_bytes();
            if key == *mint || tracked_accounts.contains(&key) {
                return true;
            }
        }
    }

    false
}

/// Optimized version that converts parsed VersionedTransaction to CompactVersionedTx
/// Assumes the transaction has already been filtered
pub fn versioned_transaction_to_compact<P: PubkeyIdProvider>(
    tx: &VersionedTransaction,
    resolver: &mut P,
    metadata_mode: MetadataMode,
    block: &CarBlock,
    tx_node: &TransactionNode<'_>,
    buf_meta: &mut Vec<u8>,
    buf_tx: &mut Vec<u8>,
) -> Result<CompactVersionedTx> {
    let static_keys = tx.message.static_account_keys();
    let header = tx.message.header();
    let is_versioned = matches!(tx.message, VersionedMessage::V0(_));
    let recent_blockhash = match &tx.message {
        VersionedMessage::Legacy(m) => m.recent_blockhash,
        VersionedMessage::V0(m) => m.recent_blockhash,
    };

    let mut signatures = Vec::with_capacity(tx.signatures.len());
    for s in &tx.signatures {
        signatures.push(Signature(s.0));
    }

    let mut account_ids = Vec::with_capacity(static_keys.len());
    for k in static_keys {
        if let Some(uid) = resolver.resolve(k) {
            account_ids.push(uid);
        } else {
            return Err(anyhow!(
                "registry miss for pubkey {}",
                Pubkey::new_from_array(*k)
            ));
        }
    }

    let mut instructions = Vec::with_capacity(tx.message.instructions_len());
    for ix in tx.message.instructions_iter() {
        instructions.push(ix.clone());
    }

    let address_table_lookups = match tx.message.address_table_lookups() {
        None => None,
        Some(lookups) => {
            let mut vec = Vec::with_capacity(lookups.len());
            for l in lookups {
                if let Some(uid) = resolver.resolve(&l.account_key) {
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

    // Handle metadata
    let df = &tx_node.metadata;
    let metadata = match metadata_mode {
        MetadataMode::None => None,
        mode @ (MetadataMode::Compact | MetadataMode::Raw) => {
            let src = if let Some(next) = df.next {
                let df_cid = next.to_cid()?;
                buf_meta.clear();
                block.dataframe_copy_into(&df_cid, buf_meta, Some(df.data))?;
                &*buf_meta
            } else {
                df.data
            };

            let raw_slice = match mode {
                MetadataMode::Compact => metadata_proto_into(src, buf_tx),
                MetadataMode::Raw => src,
                MetadataMode::None => unreachable!(),
            };

            process_metadata(mode, raw_slice, resolver)
        }
    };

    Ok(CompactVersionedTx {
        signatures,
        header: *header,
        account_ids,
        recent_blockhash,
        instructions,
        address_table_lookups,
        is_versioned,
        metadata,
    })
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
    accounts: &[Pubkey],
    rewards: &Node<'_>,
    resolver: &mut P,
    buf: &mut Vec<u8>,
) -> Result<Vec<CompactReward>> {
    let data = match rewards.next {
        None => rewards.data,
        Some(next) => {
            let cid = next.to_cid()?;
            block.dataframe_copy_into(&cid, buf, Some(rewards.data))?;
            &*buf
        }
    };

    let cb: confirmed_block::Rewards = confirmed_block::Rewards::decode(data)?;
    cb.iter()
        .map(|r| {
            let pubkey = Pubkey::from_str(&r.pubkey)?;
            id_for_pubkey(resolver, &pubkey.to_bytes())
                .map(|id| CompactReward {
                    account_id: id,
                    lamports: r.lamports,
                    post_balance: r.post_balance,
                    reward_type: reward_tag_to_compact(r.reward_type),
                    commission: r.commission.map(|v| v as u8),
                })
                .ok_or(anyhow!("reward account not in registry: {pubkey}"))
        })
        .collect()
}

#[inline(always)]
fn metadata_proto_into<'a>(src: &'a [u8], dst: &'a mut Vec<u8>) -> &'a [u8] {
    dst.clear();
    dst.reserve(64 << 10);
    let mut proto_buf = MaybeUninit::<confirmed_block::TransactionStatusMeta>::uninit();
    let parsed = confirmed_block::TransactionStatusMeta::decode_into(src, &mut proto_buf)
        .expect("failed to decode proto even though no error");
    dst.extend_from_slice(parsed.try_as_slice().unwrap());
    &*dst
}

fn token_balance_to_compact_moved<P: PubkeyIdProvider>(
    tb: &confirmed_block::TokenBalance,
    resolver: &mut P,
) -> Option<CompactTokenBalanceMeta> {
    let mint_id = id_for_pubkey(resolver, &Pubkey::from_str(&tb.mint).unwrap().to_bytes());
    let owner_id = id_for_pubkey(
        resolver,
        &Pubkey::from_str(tb.owner.as_ref()?).unwrap().to_bytes(),
    );
    let program_id_id = id_for_pubkey(
        resolver,
        &Pubkey::from_str(tb.program_id.as_ref()?)
            .unwrap()
            .to_bytes(),
    );
    if let (Some(mint_id), Some(owner_id), Some(program_id_id)) = (mint_id, owner_id, program_id_id)
    {
        Some(CompactTokenBalanceMeta {
            mint_id,
            owner_id,
            program_id_id,
            amount: tb.ui_token_amount.as_ref()?.amount.parse().ok()?,
            decimals: tb.ui_token_amount.as_ref()?.decimals as u8,
        })
    } else {
        None
    }
}

fn meta_to_compact<P: PubkeyIdProvider>(
    meta: &confirmed_block::TransactionStatusMeta,
    resolver: &mut P,
) -> Option<CompactMetadata> {
    let pre_balances = meta.pre_balances.iter().copied().collect();
    let post_balances = meta.post_balances.iter().copied().collect();
    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .filter_map(|tb| token_balance_to_compact_moved(tb, resolver))
        .collect();
    let post_token_balances = meta
        .post_token_balances
        .iter()
        .filter_map(|tb| token_balance_to_compact_moved(tb, resolver))
        .collect();
    let loaded_writable_ids = meta
        .loaded_writable_addresses
        .iter()
        .filter_map(|pk| id_for_pubkey(resolver, &pk.to_bytes()))
        .collect();
    let loaded_readonly_ids = meta
        .loaded_readonly_addresses
        .iter()
        .filter_map(|pk| id_for_pubkey(resolver, &pk.to_bytes()))
        .collect();
    let return_data = meta.return_data.as_ref().and_then(|r| {
        id_for_pubkey(resolver, &r.program_id.to_bytes()).map(|program_id_id| CompactReturnData {
            program_id_id,
            data: r.data.clone(),
        })
    });

    let inner_instructions = meta.inner_instructions.as_ref().map(|v| {
        v.iter()
            .filter_map(|i| {
                let instructions = i
                    .instructions
                    .iter()
                    .map(|ix| CompactInnerInstruction {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts.clone(),
                        data: ix.data.clone(),
                        stack_height: ix.stack_height,
                    })
                    .collect();
                Some(CompactInnerInstructions {
                    index: i.index,
                    instructions,
                })
            })
            .collect()
    });

    let log_messages = meta.log_messages.as_ref().map(|logs| {
        let config = EncodeConfig::default();
        let mut stream = CompactLogStream::default();
        encode_logs(logs.iter().map(|s| s.as_str()), &mut stream, config);
        stream
    });

    Some(CompactMetadata {
        fee: meta.fee?,
        err: meta.err.as_ref().map(|e| e.clone().into()),
        pre_balances,
        post_balances,
        pre_token_balances,
        post_token_balances,
        loaded_writable_ids,
        loaded_readonly_ids,
        return_data,
        inner_instructions,
        log_messages,
    })
}

fn process_metadata<P: PubkeyIdProvider>(
    mode: MetadataMode,
    raw_meta: &[u8],
    resolver: &mut P,
) -> Option<CompactMetadataPayload> {
    match mode {
        MetadataMode::Raw => Some(CompactMetadataPayload::Raw(raw_meta.to_vec())),
        MetadataMode::Compact => match decode_transaction_status_meta_bytes(raw_meta) {
            Ok(meta) => meta_to_compact(&meta, resolver).map(CompactMetadataPayload::Compact),
            Err(err) => {
                tracing::warn!(target: "carblock_to_compact", "failed to decode metadata: {err}");
                None
            }
        },
        MetadataMode::None => None,
    }
}

pub fn transaction_node_to_compact<P: PubkeyIdProvider>(
    tx_node: &TransactionNode<'_>,
    block: &CarBlock,
    meta_buf: &mut Vec<u8>,
    tx_buf: &mut Vec<u8>,
    resolver: &mut P,
    metadata_mode: MetadataMode,
) -> Result<CompactVersionedTx> {
    if tx_node.metadata.value.tag == 1 {
        let tx = VersionedTransaction::decode(tx_buf, tx_node)?;
        versioned_transaction_to_compact(
            &tx,
            resolver,
            metadata_mode,
            block,
            tx_node,
            meta_buf,
            tx_buf,
        )
    } else {
        Err(anyhow!("unsupported transaction encoding"))
    }
}

pub fn carblock_to_compactblock_inplace<P: PubkeyIdProvider>(
    block: &CarBlock,
    buf: &mut Vec<u8>,
    resolver: &mut P,
    metadata_mode: MetadataMode,
) -> Result<CompactBlock> {
    let mut rewards: Vec<CompactReward> = Vec::new();
    let mut txs = Vec::new();
    let mut keys_buffer = SmallVec::<[Pubkey; 256]>::new();

    let header = block.header()?;
    let slot = header.slot();
    let accounts: Vec<Pubkey> = header.accounts().into_iter().map(|p| p.key()).collect();

    let tx_data = block.transactions();
    let tx_nodes = tx_data.children();

    // We need to buffer metadata and transactions separately as they might span multiple dataframes
    let mut meta_buf = Vec::with_capacity(64 << 10);
    let mut tx_buf = Vec::with_capacity(64 << 10);

    // decode rewards list
    rewards = decode_rewards_via_node(block, &accounts, tx_data.rewards(), resolver, buf)?;

    for tx_node in tx_nodes {
        let tx = transaction_node_to_compact(
            tx_node,
            block,
            &mut meta_buf,
            &mut tx_buf,
            resolver,
            metadata_mode,
        )?;
        txs.push(tx);
    }

    Ok(CompactBlock { slot, txs, rewards })
}
