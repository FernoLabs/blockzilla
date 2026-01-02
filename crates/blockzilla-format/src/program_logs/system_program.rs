use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::str::FromStr;

use crate::Registry;
use crate::log::{StrId, StringTable};

/// System Program id
pub const STR_ID: &str = "11111111111111111111111111111111";

/// Registry-backed pubkey id (1-based, like your log.rs pid_to_pubkey convention).
pub type PubkeyId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemProgramLog {
    /// `Instruction: <name>`
    Instruction(SystemInstructionLog),

    /// `Create: address <provided> does not match derived address <derived>`
    CreateAddressMismatch {
        provided_addr: PubkeyId,
        derived_addr: PubkeyId,
    },

    /// `Create Account: account <addr> already in use`
    CreateAccountAlreadyInUse { addr: PubkeyId },

    /// `Allocate: account <addr> already in use`
    AllocateAlreadyInUse { addr: PubkeyId },

    /// `Allocate: 'to' account <addr> must sign`
    AllocateToMustSign { addr: PubkeyId },

    /// `Allocate: account <addr> already in use` (explicit alias)
    AllocateAccountAlreadyInUse { addr: PubkeyId },

    /// `Allocate: requested <space>, max allowed <max>`
    AllocateRequestedTooLarge { requested: u64, max_allowed: u64 },

    /// `Assign: account <addr> must sign`
    AssignAccountMustSign { addr: PubkeyId },

    /// `Create Account: account <addr> already in use` (explicit alias)
    CreateAccountAccountAlreadyInUse { addr: PubkeyId },

    /// `Transfer: \`from\` must not carry data`
    TransferFromMustNotCarryData,

    /// `Transfer: \`from\` account <pubkey> must sign`
    TransferFromMustSign { from: PubkeyId },

    /// `Transfer: insufficient lamports <have>, need <need>`
    TransferInsufficient { have: u64, need: u64 },

    /// `Transfer: 'from' address <provided> does not match derived address <derived>`
    TransferFromAddressMismatch {
        provided_addr: PubkeyId,
        derived_addr: PubkeyId,
    },

    /// `Advance nonce account: recent blockhash list is empty`
    AdvanceNonceRecentBlockhashesEmpty,

    /// `Initialize nonce account: recent blockhash list is empty`
    InitializeNonceRecentBlockhashesEmpty,

    /// `Authorize nonce account: <free text>`
    AuthorizeNonceAccount { msg: StrId },

    /// Anything else we decided to keep as plain text for now.
    Unparsed { text: StrId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemInstructionLog {
    RevokePendingActivation,
}

impl SystemInstructionLog {
    #[inline]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "RevokePendingActivation" => Some(Self::RevokePendingActivation),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RevokePendingActivation => "Instruction: RevokePendingActivation",
        }
    }
}

#[inline]
fn parse_u64_commas(s: &str) -> Option<u64> {
    s.trim().replace(',', "").parse().ok()
}

/// Parse a pubkey string and convert to registry-backed PubkeyId (1-based).
#[inline]
fn parse_pubkey_id(registry: &Registry, pk_txt: &str) -> Option<PubkeyId> {
    let pk = Pubkey::from_str(pk_txt.trim()).ok()?;
    let ix0 = registry.lookup(&pk.to_bytes())?;
    Some(ix0 + 1)
}

#[inline]
fn pubkey_id_to_pubkey(registry: &Registry, id: PubkeyId) -> Pubkey {
    assert!(id != 0, "SystemProgramLog: PubkeyId=0 is reserved/invalid");
    let ix = (id - 1) as usize;
    let bytes = registry.keys.get(ix).unwrap_or_else(|| {
        panic!(
            "SystemProgramLog: PubkeyId out of bounds: id={} len={}",
            id,
            registry.len()
        )
    });
    Pubkey::new_from_array(*bytes)
}

#[inline]
fn parse_between<'a>(line: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let b = line.as_bytes();
    if !b.starts_with(prefix.as_bytes()) || !b.ends_with(suffix.as_bytes()) {
        return None;
    }
    Some(&line[prefix.len()..line.len() - suffix.len()])
}

/// Parse the `{:?}`-formatted `Address` that system program prints in logs.
///
/// In Solana, `to_address` is an `Address` wrapper around a `Pubkey`, and logs print it via `{:?}`.
/// Depending on version, this often looks like either:
/// - `Address { address: <PUBKEY> }`
/// - `<PUBKEY>`
///
/// We accept both and return the registry-backed PubkeyId.
#[inline]
fn parse_debug_address_to_pubkey_id(registry: &Registry, addr_txt: &str) -> Option<PubkeyId> {
    let s = addr_txt.trim();

    // Case A: "Address { address: <PK> }" (or possibly more spaces)
    if let Some(inner) = s.strip_prefix("Address {") {
        // Find "address:" then take the token that follows
        let inner = inner.trim();
        let inner = inner
            .strip_prefix("address:")
            .or_else(|| inner.strip_prefix("address :"))?;
        let inner = inner.trim();

        // token ends at whitespace or '}'
        let end = inner
            .find(|c: char| c.is_whitespace() || c == '}')
            .unwrap_or(inner.len());
        let pk_txt = inner[..end].trim().trim_end_matches('}');
        return parse_pubkey_id(registry, pk_txt);
    }

    // Case B: plain pubkey
    parse_pubkey_id(registry, s)
}

impl SystemProgramLog {
    /// `text` is the payload after "Program log: " or after "Program <id> log: "
    #[inline]
    pub fn parse(text: &str, registry: &Registry, st: &mut StringTable) -> Option<Self> {
        let text = text.trim();

        // Instruction: <name>
        if let Some(name) = text.strip_prefix("Instruction: ") {
            let name = name.trim();
            if let Some(ix) = SystemInstructionLog::parse(name) {
                return Some(Self::Instruction(ix));
            }
            return None;
        }

        // Create: address X does not match derived address Y
        if let Some(rest) = text.strip_prefix("Create: address ")
            && let Some(mid) = rest.find(" does not match derived address ")
        {
            let provided_txt = rest[..mid].trim();
            let derived_txt = rest[mid + " does not match derived address ".len()..].trim();
            return Some(Self::CreateAddressMismatch {
                provided_addr: parse_pubkey_id(registry, provided_txt)?,
                derived_addr: parse_pubkey_id(registry, derived_txt)?,
            });
        }

        // Transfer: 'from' address X does not match derived address Y
        if let Some(rest) = text.strip_prefix("Transfer: 'from' address ")
            && let Some(mid) = rest.find(" does not match derived address ")
        {
            let provided_txt = rest[..mid].trim();
            let derived_txt = rest[mid + " does not match derived address ".len()..].trim();
            return Some(Self::TransferFromAddressMismatch {
                provided_addr: parse_pubkey_id(registry, provided_txt)?,
                derived_addr: parse_pubkey_id(registry, derived_txt)?,
            });
        }

        // Create Account: account {:?} already in use  (prints Address via Debug)
        if let Some(addr_txt) = parse_between(text, "Create Account: account ", " already in use") {
            let addr = parse_debug_address_to_pubkey_id(registry, addr_txt)?;
            return Some(Self::CreateAccountAlreadyInUse { addr });
        }

        // Allocate: account {:?} already in use
        if let Some(addr_txt) = parse_between(text, "Allocate: account ", " already in use") {
            let addr = parse_debug_address_to_pubkey_id(registry, addr_txt)?;
            return Some(Self::AllocateAlreadyInUse { addr });
        }

        // Allocate: 'to' account {:?} must sign
        if let Some(addr_txt) = parse_between(text, "Allocate: 'to' account ", " must sign") {
            let addr = parse_debug_address_to_pubkey_id(registry, addr_txt)?;
            return Some(Self::AllocateToMustSign { addr });
        }

        // Assign: account {:?} must sign
        if let Some(addr_txt) = parse_between(text, "Assign: account ", " must sign") {
            let addr = parse_debug_address_to_pubkey_id(registry, addr_txt)?;
            return Some(Self::AssignAccountMustSign { addr });
        }

        // Allocate: requested <space>, max allowed <max>
        if let Some(rest) = text.strip_prefix("Allocate: requested ")
            && let Some(pos) = rest.find(", max allowed ")
        {
            return Some(Self::AllocateRequestedTooLarge {
                requested: parse_u64_commas(&rest[..pos])?,
                max_allowed: parse_u64_commas(&rest[pos + ", max allowed ".len()..])?,
            });
        }

        // Transfer: `from` must not carry data
        if text == "Transfer: `from` must not carry data" {
            return Some(Self::TransferFromMustNotCarryData);
        }

        // Transfer: `from` account <pubkey> must sign
        if let Some(rest) = text.strip_prefix("Transfer: `from` account ")
            && let Some(pk_txt) = rest.strip_suffix(" must sign")
        {
            return Some(Self::TransferFromMustSign {
                from: parse_pubkey_id(registry, pk_txt.trim())?,
            });
        }

        // Transfer: insufficient lamports <have>, need <need>
        if let Some(rest) = text.strip_prefix("Transfer: insufficient lamports ") {
            if let Some(pos) = rest.find(", need ") {
                return Some(Self::TransferInsufficient {
                    have: parse_u64_commas(&rest[..pos])?,
                    need: parse_u64_commas(&rest[pos + ", need ".len()..])?,
                });
            }
            return Some(Self::Unparsed {
                text: st.push(text),
            });
        }

        // Advance nonce account: recent blockhash list is empty
        if text == "Advance nonce account: recent blockhash list is empty" {
            return Some(Self::AdvanceNonceRecentBlockhashesEmpty);
        }

        // Initialize nonce account: recent blockhash list is empty
        if text == "Initialize nonce account: recent blockhash list is empty" {
            return Some(Self::InitializeNonceRecentBlockhashesEmpty);
        }

        // Authorize nonce account: <free text>
        if let Some(msg) = text.strip_prefix("Authorize nonce account: ") {
            return Some(Self::AuthorizeNonceAccount { msg: st.push(msg) });
        }

        None
    }

    #[inline]
    pub fn render(&self, st: &StringTable, registry: &Registry) -> String {
        match self {
            Self::Instruction(ix) => ix.as_str().to_string(),

            Self::CreateAddressMismatch {
                provided_addr,
                derived_addr,
            } => format!(
                "Create: address {} does not match derived address {}",
                pubkey_id_to_pubkey(registry, *provided_addr),
                pubkey_id_to_pubkey(registry, *derived_addr),
            ),

            Self::TransferFromAddressMismatch {
                provided_addr,
                derived_addr,
            } => format!(
                "Transfer: 'from' address {} does not match derived address {}",
                pubkey_id_to_pubkey(registry, *provided_addr),
                pubkey_id_to_pubkey(registry, *derived_addr),
            ),

            Self::CreateAccountAlreadyInUse { addr }
            | Self::CreateAccountAccountAlreadyInUse { addr } => format!(
                "Create Account: account {:?} already in use",
                // Keep `{:?}`-ish feel, but minimal: just the pubkey.
                pubkey_id_to_pubkey(registry, *addr),
            ),

            Self::AllocateAlreadyInUse { addr } | Self::AllocateAccountAlreadyInUse { addr } => {
                format!(
                    "Allocate: account {:?} already in use",
                    pubkey_id_to_pubkey(registry, *addr),
                )
            }

            Self::AllocateToMustSign { addr } => format!(
                "Allocate: 'to' account {:?} must sign",
                pubkey_id_to_pubkey(registry, *addr),
            ),

            Self::AssignAccountMustSign { addr } => format!(
                "Assign: account {:?} must sign",
                pubkey_id_to_pubkey(registry, *addr),
            ),

            Self::AllocateRequestedTooLarge {
                requested,
                max_allowed,
            } => format!(
                "Allocate: requested {}, max allowed {}",
                requested, max_allowed
            ),

            Self::TransferFromMustNotCarryData => {
                "Transfer: `from` must not carry data".to_string()
            }

            Self::TransferFromMustSign { from } => format!(
                "Transfer: `from` account {} must sign",
                pubkey_id_to_pubkey(registry, *from),
            ),

            Self::TransferInsufficient { have, need } => {
                format!("Transfer: insufficient lamports {}, need {}", have, need)
            }

            Self::AdvanceNonceRecentBlockhashesEmpty => {
                "Advance nonce account: recent blockhash list is empty".to_string()
            }

            Self::InitializeNonceRecentBlockhashesEmpty => {
                "Initialize nonce account: recent blockhash list is empty".to_string()
            }

            Self::AuthorizeNonceAccount { msg } => {
                format!("Authorize nonce account: {}", st.resolve(*msg))
            }

            Self::Unparsed { text } => st.resolve(*text).to_string(),
        }
    }
}
