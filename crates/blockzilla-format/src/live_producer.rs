use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    CompactMetaV1, CompactPohEntry, CompactReward, CompactShredding, OwnedCompactTransaction,
    WincodeArchiveV2Block, WincodeArchiveV2Footer, WincodeArchiveV2Header,
};

pub const LIVE_PRE_HOT_BLOCKS_FILE: &str = "live-pre-hot-blocks.bin";
pub const LIVE_PUBKEY_RUNS_DIR: &str = "pubkey-runs";
pub const LIVE_PUBKEY_RUN_HOT_FILE: &str = "hot-run.bin";
pub const LIVE_PUBKEY_RUN_RECORD_LEN: usize = 36;
pub const LIVE_PRE_HOT_BLOCK_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveArchiveSource {
    OwnGrpc,
    Fumarole,
    LaserStream,
    DoubleZeroShred,
    ShredStream,
    RpcGetBlock,
    TritonCar,
    LocalCar,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveBlockState {
    Pending,
    CompleteLive,
    IncompleteLive,
    RpcFallback,
    CarRepaired,
    CarOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveBlockMissingField {
    BlockMeta,
    Transactions,
    TransactionStatus,
    Rewards,
    PohEntries,
    Shredding,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveBlockCompleteness {
    pub state: LiveBlockState,
    pub has_block_meta: bool,
    pub has_transactions: bool,
    pub has_transaction_status: bool,
    pub has_rewards: bool,
    pub has_poh_entries: bool,
    pub has_shredding: bool,
    pub source_rank: u8,
    pub repair_required: bool,
    pub missing: Vec<LiveBlockMissingField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveSourceProvenance {
    pub source: LiveArchiveSource,
    pub received_at_unix_micros: Option<i64>,
    pub source_slot: Option<u64>,
    pub sequence: Option<u64>,
    pub endpoint: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveBlockDraft {
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub blockhash: Option<[u8; 32]>,
    pub previous_blockhash: Option<[u8; 32]>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transactions: Vec<LiveTransactionDraft>,
    pub rewards: Option<LiveRewardsPayload>,
    pub poh_entries: Option<Vec<CompactPohEntry>>,
    pub shredding: Option<Vec<CompactShredding>>,
    pub completeness: LiveBlockCompleteness,
    pub provenance: Vec<LiveSourceProvenance>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveTransactionDraft {
    pub tx_index: u32,
    pub payload: LiveTransactionPayload,
    pub metadata: Option<LiveTransactionMetadataPayload>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveTransactionPayload {
    Compact(OwnedCompactTransaction),
    Raw(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[allow(clippy::large_enum_variant)]
pub enum LiveTransactionMetadataPayload {
    Compact(CompactMetaV1),
    Raw(Vec<u8>),
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveRewardsPayload {
    Compact(Vec<CompactReward>),
    Raw(Vec<u8>),
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveShredRecord {
    pub slot: u64,
    pub shred_index: u32,
    pub fec_set_index: Option<u32>,
    pub is_coding: bool,
    pub is_last_in_slot: Option<bool>,
    pub payload: Vec<u8>,
    pub provenance: LiveSourceProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveBlockRepairRecord {
    pub slot: u64,
    pub block_id: Option<u32>,
    pub source: LiveArchiveSource,
    pub repaired_fields: Vec<LiveBlockMissingField>,
    pub poh_entries: Option<Vec<CompactPohEntry>>,
    pub shredding: Option<Vec<CompactShredding>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveSignatureIndexRecord {
    pub signature_ordinal: u64,
    pub slot: u64,
    pub block_id: u32,
    pub tx_index: u32,
    pub signature_index: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LivePubkeyCountRecord {
    pub pubkey: [u8; 32],
    pub count: u64,
}

/// Live pre-hot block artifact.
///
/// This record is the intended next live capture format for fast epoch close:
/// capture performs transaction/metadata/log optimization and writes a typed
/// `WincodeArchiveV2Block`, but pubkey references inside messages, metadata,
/// rewards, and compact logs remain `CompactPubkey::Raw([u8; 32])`. The
/// finalizer builds the global registry from `LIVE_PUBKEY_RUNS_DIR`, rewrites
/// raw pubkey refs to global `CompactPubkey::Id(u32)`, then builds and compresses
/// the canonical hot block.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LivePreHotBlock {
    pub version: u16,
    pub block_id: u32,
    pub block: WincodeArchiveV2Block,
    pub raw_pubkey_refs: u32,
}

impl LivePreHotBlock {
    pub fn new(block_id: u32, block: WincodeArchiveV2Block, raw_pubkey_refs: u32) -> Self {
        Self {
            version: LIVE_PRE_HOT_BLOCK_VERSION,
            block_id,
            block,
            raw_pubkey_refs,
        }
    }
}

/// Versioned record stream for pre-hot live and CAR capture artifacts.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LivePreHotRecord {
    Header(WincodeArchiveV2Header),
    Block(LivePreHotBlock),
    Footer(WincodeArchiveV2Footer),
}

impl LiveBlockCompleteness {
    pub fn complete_live(source_rank: u8) -> Self {
        Self {
            state: LiveBlockState::CompleteLive,
            has_block_meta: true,
            has_transactions: true,
            has_transaction_status: true,
            has_rewards: true,
            has_poh_entries: true,
            has_shredding: true,
            source_rank,
            repair_required: false,
            missing: Vec::new(),
        }
    }

    pub fn rpc_fallback(missing: Vec<LiveBlockMissingField>) -> Self {
        Self {
            state: LiveBlockState::RpcFallback,
            has_block_meta: !missing.contains(&LiveBlockMissingField::BlockMeta),
            has_transactions: !missing.contains(&LiveBlockMissingField::Transactions),
            has_transaction_status: !missing.contains(&LiveBlockMissingField::TransactionStatus),
            has_rewards: !missing.contains(&LiveBlockMissingField::Rewards),
            has_poh_entries: !missing.contains(&LiveBlockMissingField::PohEntries),
            has_shredding: !missing.contains(&LiveBlockMissingField::Shredding),
            source_rank: u8::MAX,
            repair_required: true,
            missing,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CompactBlockHeader, CompactMessageHeader, CompactPubkey, OwnedCompactInstruction,
        OwnedCompactLegacyMessage, OwnedCompactMessage, OwnedCompactRecentBlockhash,
        WincodeArchiveV2BlockHeader, WincodeArchiveV2Payload, WincodeArchiveV2Transaction,
        WincodeLeb128FramedReader, WincodeLeb128FramedWriter, wincode_leb128_config,
    };
    use std::io::Cursor;

    fn sample_block(slot: u64) -> WincodeArchiveV2Block {
        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader {
                compact: CompactBlockHeader {
                    slot,
                    parent_slot: slot.saturating_sub(1),
                    blockhash: 7,
                    previous_blockhash: 6,
                    block_time: Some(1_725_000_000),
                    block_height: Some(123_456),
                    shredding: Vec::new(),
                    poh_entries: Vec::new(),
                    rewards: None,
                },
                rewards: None,
            },
            txs: vec![WincodeArchiveV2Transaction {
                tx_index: 0,
                tx: WincodeArchiveV2Payload::Decoded {
                    source_len: 128,
                    value: OwnedCompactTransaction {
                        signatures: vec![vec![9; 64]],
                        message: OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                            header: CompactMessageHeader {
                                num_required_signatures: 1,
                                num_readonly_signed_accounts: 0,
                                num_readonly_unsigned_accounts: 0,
                            },
                            account_keys: vec![CompactPubkey::raw([3; 32])],
                            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([4; 32]),
                            instructions: vec![OwnedCompactInstruction {
                                program_id_index: 0,
                                accounts: vec![0],
                                data: vec![1, 2, 3],
                            }],
                        }),
                    },
                },
                metadata: None,
            }],
        }
    }

    #[test]
    fn live_pre_hot_block_roundtrips() {
        let value = LivePreHotBlock::new(7, sample_block(42), 19);
        let encoded = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let decoded: LivePreHotBlock =
            wincode::config::deserialize_exact(&encoded, wincode_leb128_config()).unwrap();

        assert_eq!(decoded.version, LIVE_PRE_HOT_BLOCK_VERSION);
        assert_eq!(decoded.block_id, 7);
        assert_eq!(decoded.raw_pubkey_refs, 19);
        assert_eq!(decoded.block.header.compact.slot, 42);
        assert_eq!(decoded.block.header.compact.blockhash, 7);
        assert_eq!(decoded.block.txs.len(), 1);
        let WincodeArchiveV2Payload::Decoded { value: tx, .. } = &decoded.block.txs[0].tx else {
            panic!("expected decoded pre-hot transaction");
        };
        let OwnedCompactMessage::Legacy(message) = &tx.message else {
            panic!("expected legacy pre-hot message");
        };
        assert_eq!(message.account_keys, [CompactPubkey::raw([3; 32])]);
        assert_eq!(tx.signatures, [vec![9; 64]]);
    }

    #[test]
    fn live_pre_hot_record_stream_roundtrips() {
        let mut encoded = Vec::new();
        {
            let mut writer = WincodeLeb128FramedWriter::new(&mut encoded);
            writer
                .write(&LivePreHotRecord::Header(WincodeArchiveV2Header {
                    version: LIVE_PRE_HOT_BLOCK_VERSION,
                    flags: 0,
                }))
                .unwrap();
            writer
                .write(&LivePreHotRecord::Block(LivePreHotBlock::new(
                    11,
                    sample_block(99),
                    23,
                )))
                .unwrap();
            writer
                .write(&LivePreHotRecord::Footer(WincodeArchiveV2Footer {
                    blocks: 1,
                    transactions: 1,
                    ..WincodeArchiveV2Footer::default()
                }))
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = WincodeLeb128FramedReader::new(Cursor::new(encoded));
        let (_, header) = reader.read::<LivePreHotRecord>().unwrap().unwrap();
        let (_, block) = reader.read::<LivePreHotRecord>().unwrap().unwrap();
        let (_, footer) = reader.read::<LivePreHotRecord>().unwrap().unwrap();
        assert!(reader.read::<LivePreHotRecord>().unwrap().is_none());

        match header {
            LivePreHotRecord::Header(header) => {
                assert_eq!(header.version, LIVE_PRE_HOT_BLOCK_VERSION);
                assert_eq!(header.flags, 0);
            }
            _ => panic!("expected pre-hot header"),
        }
        match block {
            LivePreHotRecord::Block(block) => {
                assert_eq!(block.version, LIVE_PRE_HOT_BLOCK_VERSION);
                assert_eq!(block.block_id, 11);
                assert_eq!(block.raw_pubkey_refs, 23);
                assert_eq!(block.block.header.compact.slot, 99);
            }
            _ => panic!("expected pre-hot block"),
        }
        match footer {
            LivePreHotRecord::Footer(footer) => {
                assert_eq!(footer.blocks, 1);
                assert_eq!(footer.transactions, 1);
            }
            _ => panic!("expected pre-hot footer"),
        }
    }
}
