use blockzilla_archive_v2::{
    WincodeArchiveV2NoRegistryRewards, WincodeArchiveV2NoRegistryTransaction,
};
use serde::{Deserialize, Serialize};

pub const REVISION: &str = match option_env!("HIVEZILLA_AGAVE_REVISION") {
    Some(revision) => revision,
    None => "825efd18292aff6ffcf9daa0f7612f21b3531a72",
};
pub const SCHEMA: &str = "hivezilla-agave-callback-event-v1";
pub const BANK_V2: &str = "hivezilla-agave-bank-observation-v2";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeBankState {
    pub schema: String,
    pub slot: u64,
    pub bank_id: u64,
    pub parent_slot: u64,
    pub blockhash: [u8; 32],
    pub parent_blockhash: [u8; 32],
    pub block_height: u64,
    pub last_valid_block_height: u64,
    pub observed_at_unix_ms: u64,
    pub confirmed: bool,
}
impl NativeBankState {
    pub fn bank(&self) -> blockzilla_bank_stream_v2::BankIdentity {
        blockzilla_bank_stream_v2::BankIdentity {
            slot: self.slot,
            bank_id: self.bank_id,
            parent_slot: self.parent_slot,
            blockhash: self.blockhash,
            parent_blockhash: self.parent_blockhash,
        }
    }
    pub fn recent(&self) -> blockzilla_bank_stream_v2::RecentBlockhash {
        blockzilla_bank_stream_v2::RecentBlockhash {
            blockhash: self.blockhash_string(),
            context_slot: self.slot,
            last_valid_block_height: self.last_valid_block_height,
        }
    }
    fn blockhash_string(&self) -> String {
        let mut out = [0; 44];
        let len = five8::encode_32(&self.blockhash, &mut out);
        std::str::from_utf8(&out[..len as usize]).unwrap().into()
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.schema == BANK_V2
                && self.block_height <= blockzilla_bank_stream_v2::SAFE
                && self.observed_at_unix_ms <= blockzilla_bank_stream_v2::SAFE,
            "invalid_native_bank_state"
        );
        self.bank().validate()?;
        self.recent().validate(&self.bank(), self.block_height)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Event {
    BankStateV2(NativeBankState),
    BankStatusV2 {
        schema: String,
        slot: u64,
        bank_id: Option<u64>,
        parent: Option<u64>,
        parent_blockhash: Option<[u8; 32]>,
        status: String,
        dead_error: Option<String>,
        observed_at_unix_ms: u64,
    },
    Transaction {
        slot: u64,
        bank_id: u64,
        index: u64,
        is_vote: bool,
        message_hash: Vec<u8>,
        wire_transaction: Vec<u8>,
        native_meta: serde_json::Value,
        projection: WincodeArchiveV2NoRegistryTransaction,
    },
    Entry {
        slot: u64,
        bank_id: u64,
        index: u64,
        num_hashes: u64,
        hash: [u8; 32],
        transactions: u64,
        starting_transaction_index: u64,
    },
    Block {
        slot: u64,
        bank_id: u64,
        parent_slot: u64,
        parent_blockhash: [u8; 32],
        blockhash: [u8; 32],
        transactions: u64,
        entries: u64,
        block_time: Option<i64>,
        block_height: Option<u64>,
        native_rewards: serde_json::Value,
        rewards: WincodeArchiveV2NoRegistryRewards,
    },
    Status {
        slot: u64,
        bank_id: Option<u64>,
        parent: Option<u64>,
        status: String,
        dead_error: Option<String>,
    },
    ClearBank {
        slot: u64,
        bank_id: u64,
        parent_slot: u64,
        parent_blockhash: [u8; 32],
    },
    Footer {
        slot: u64,
        bank_id: u64,
        wire_footer: Vec<u8>,
    },
}
impl Event {
    pub fn key(&self) -> (u64, Option<u64>, &'static str) {
        match self {
            Self::BankStateV2(state) => (state.slot, Some(state.bank_id), "bank_state_v2"),
            Self::BankStatusV2 {
                slot,
                bank_id,
                status,
                ..
            } => (
                *slot,
                *bank_id,
                match status.as_str() {
                    "rooted" => "rooted",
                    "dead" => "dead",
                    "clear" => "clear",
                    _ => "status",
                },
            ),
            Self::Transaction { slot, bank_id, .. } => (*slot, Some(*bank_id), "transaction"),
            Self::Entry { slot, bank_id, .. } => (*slot, Some(*bank_id), "entry"),
            Self::Block { slot, bank_id, .. } => (*slot, Some(*bank_id), "block"),
            Self::Status {
                slot,
                bank_id,
                status,
                ..
            } => (
                *slot,
                *bank_id,
                if status == "rooted" {
                    "rooted"
                } else if status == "dead" {
                    "dead"
                } else {
                    "status"
                },
            ),
            Self::ClearBank { slot, bank_id, .. } => (*slot, Some(*bank_id), "clear"),
            Self::Footer { slot, bank_id, .. } => (*slot, Some(*bank_id), "footer"),
        }
    }
}

pub fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub fn hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let mut bytes = [0; 32];
    five8::decode_32(s, &mut bytes).map_err(|e| anyhow::anyhow!("invalid hash: {e:?}"))?;
    Ok(bytes)
}
