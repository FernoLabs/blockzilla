use serde::{Deserialize, Serialize, Deserializer, Serializer};

/// Wrapper for 64-byte arrays to support serde serialization
#[derive(Debug, Clone, Copy)]
pub struct Signature([u8; 64]);

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let array = <[u8; 64]>::try_from(bytes)
            .map_err(|_| serde::de::Error::custom("expected 64 bytes"))?;
        Ok(Signature(array))
    }
}

/// Pubkey registry mapping IDs to pubkeys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    /// Sorted list of most-used pubkeys in the epoch
    pub pubkeys: Vec<[u8; 32]>,
}

/// Slot/Block index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotIndex {
    /// Slot number
    pub slot: u64,
    /// Whether this slot was skipped
    pub skipped: bool,
    /// Block hash
    pub blockhash: [u8; 32],
    /// Offset in block data file
    pub data_offset: u64,
    /// Transaction signatures
    pub tx_signatures: Vec<Signature>,
    /// Loaded addresses (as registry IDs)
    pub loaded_addresses: Vec<u32>,
}

/// Block data (instructions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockData {
    /// Instructions in this block
    pub instructions: Vec<Instruction>,
}

/// Instruction data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    /// Address index (registry ID)
    pub address_idx: u32,
    /// Instruction data
    pub data: Vec<u8>,
    /// Transaction index
    pub tx_idx: u32,
}

/// Runtime information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    /// Inner instructions
    pub inner_instructions: Vec<InnerInstruction>,
    /// Logs
    pub logs: Vec<Log>,
    /// SOL balance changes
    pub sol_balances: Vec<BalanceChange>,
    /// Token balance changes
    pub token_balances: Vec<TokenBalanceChange>,
}

/// Inner instruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerInstruction {
    /// Transaction ID
    pub tx_id: u32,
    /// Stack level
    pub stack_level: u32,
    /// Address index (registry ID)
    pub address_idx: u32,
    /// Instruction data
    pub data: Vec<u8>,
}

/// Structured log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    /// Transaction ID
    pub tx_id: u32,
    /// Log messages (with pubkeys replaced by registry IDs)
    pub messages: Vec<LogMessage>,
}

/// Log message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogMessage {
    /// Text segment
    Text(String),
    /// Pubkey reference (registry ID)
    PubkeyRef(u32),
}

/// Balance change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceChange {
    /// Account (registry ID)
    pub account_idx: u32,
    /// Pre balance
    pub pre: u64,
    /// Post balance
    pub post: u64,
}

/// Token balance change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalanceChange {
    /// Account (registry ID)
    pub account_idx: u32,
    /// Mint (registry ID)
    pub mint_idx: u32,
    /// Pre amount
    pub pre_amount: u64,
    /// Post amount
    pub post_amount: u64,
}

/// Epoch metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochMetadata {
    /// Epoch number
    pub epoch: u64,
    /// First slot in epoch
    pub first_slot: u64,
    /// Last slot in epoch
    pub last_slot: u64,
}
