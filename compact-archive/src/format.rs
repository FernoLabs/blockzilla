use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompactRewardType {
    Fee = 0,
    Rent = 1,
    Staking = 2,
    Voting = 3,
    Revoked = 4,
    Unspecified = 255,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactReward {
    pub account_id: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: CompactRewardType,
    pub commission: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub table_account_id: u32,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub enum CompactInstructionError {
    Builtin(u32),
    Custom(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactTokenBalanceMeta {
    pub mint_id: u32,
    pub owner_id: u32,
    pub program_id_id: u32,
    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactReturnData {
    pub program_id_id: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactInnerInstructions {
    pub index: u32,
    pub instructions: Vec<CompactInnerInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactInnerInstruction {
    pub program_id_index: u32,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub enum CompactMetadataPayload {
    Compact(CompactMetadata),
    Raw(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct Signature(pub [u8; 64]);

impl Serialize for Signature {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SigVisitor;

        impl<'de> serde::de::Visitor<'de> for SigVisitor {
            type Value = Signature;

            fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(f, "a 64-byte signature")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v.len() != 64 {
                    return Err(E::invalid_length(v.len(), &self));
                }
                let mut arr = [0u8; 64];
                arr.copy_from_slice(v);
                Ok(Signature(arr))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut arr = [0u8; 64];
                for i in 0..64 {
                    arr[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(Signature(arr))
            }
        }

        deserializer.deserialize_bytes(SigVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactBlock {
    pub slot: u64,
    pub txs: Vec<CompactVersionedTx>,
    pub rewards: Vec<CompactReward>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct CompactLogStream {
    pub bytes: Vec<u8>,
    pub strings: Vec<String>,
}
