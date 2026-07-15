use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use wincode::{SchemaRead, SchemaWrite};

use crate::log::{StrId, StringTable};
use crate::{CompactPubkey, KeyStore, PubkeyCompactor};

/// System Program id
pub const STR_ID: &str = "11111111111111111111111111111111";

/// Registry-backed 1-based pubkey id.
pub type PubkeyId = CompactPubkey;

/// Either a registry-backed pubkey id (when we can resolve it),
/// or a raw string id (when the pubkey was not in the registry).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PubkeyOrString {
    Pubkey(PubkeyId),
    Text(StrId),
}

impl PubkeyOrString {
    #[inline]
    pub fn from_pubkey_id(id: PubkeyId) -> Self {
        Self::Pubkey(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SystemAddress {
    Pubkey(PubkeyOrString),
    Debug {
        address: PubkeyOrString,
        base: Option<PubkeyOrString>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SystemProgramLog {
    /// `Instruction: <name>`
    Instruction(SystemInstructionLog),

    /// `Create: address <provided> does not match derived address <derived>`
    CreateAddressMismatch {
        provided_addr: PubkeyId,
        derived_addr: PubkeyOrString,
    },

    /// `Create Account: account <addr> already in use`
    CreateAccountAlreadyInUse { addr: SystemAddress },

    /// `Allocate: account <addr> already in use`
    AllocateAlreadyInUse { addr: SystemAddress },

    /// `Allocate: 'to' account <addr> must sign`
    AllocateToMustSign { addr: SystemAddress },

    /// `Allocate: account <addr> already in use` (explicit alias)
    AllocateAccountAlreadyInUse { addr: SystemAddress },

    /// `Allocate: requested <space>, max allowed <max>`
    AllocateRequestedTooLarge { requested: u64, max_allowed: u64 },

    /// `Assign: account <addr> must sign`
    AssignAccountMustSign { addr: SystemAddress },

    /// `Create Account: account <addr> already in use` (explicit alias)
    CreateAccountAccountAlreadyInUse { addr: SystemAddress },

    /// `SystemProgram::CreateAccount data size limited to <limit> in inner instructions`
    CreateAccountDataSizeLimitedInInnerInstructions { limit: u64 },

    /// `Transfer: `from` must not carry data`
    TransferFromMustNotCarryData,

    /// `Transfer: `from` account <pubkey> must sign`
    TransferFromMustSign { from: PubkeyId },

    /// `Transfer: insufficient lamports <have>, need <need>`
    TransferInsufficient { have: u64, need: u64 },

    /// `Transfer: 'from' address <provided> does not match derived address <derived>`
    TransferFromAddressMismatch {
        provided_addr: PubkeyId,
        derived_addr: PubkeyOrString,
    },

    /// `Advance nonce account: recent blockhash list is empty`
    AdvanceNonceRecentBlockhashesEmpty,

    /// `Initialize nonce account: recent blockhash list is empty`
    InitializeNonceRecentBlockhashesEmpty,

    /// `Authorize nonce account: <free text>`
    AuthorizeNonceAccount { msg: StrId },

    /// `<action> nonce account: Account <pubkey> must be writeable`
    NonceAccountMustBeWriteable {
        action: NonceAction,
        account: PubkeyOrString,
    },

    /// `<action> nonce account: Account <pubkey> must be a signer`
    NonceAccountMustBeSigner {
        action: NonceAction,
        account: PubkeyOrString,
    },

    /// `<action> nonce account: Account <pubkey> must sign`
    NonceAccountMustSign {
        action: NonceAction,
        account: PubkeyOrString,
    },

    /// `<action> nonce account: Account <pubkey> state is invalid`
    NonceAccountStateInvalid {
        action: NonceAction,
        account: PubkeyOrString,
    },

    /// `<action> nonce account: insufficient lamports <have>, need <need>`
    NonceInsufficientLamports {
        action: NonceAction,
        have: u64,
        need: u64,
    },

    /// `<action> nonce account: nonce can only advance once per slot`
    NonceCanOnlyAdvanceOncePerSlot { action: NonceAction },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SystemInstructionLog {
    RevokePendingActivation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum NonceAction {
    Advance,
    Withdraw,
    Initialize,
    Authorize,
}

impl NonceAction {
    #[inline]
    fn as_str(self) -> &'static str {
        match self {
            Self::Advance => "Advance",
            Self::Withdraw => "Withdraw",
            Self::Initialize => "Initialize",
            Self::Authorize => "Authorize",
        }
    }

    #[inline]
    fn prefix(self) -> &'static str {
        match self {
            Self::Advance => "Advance nonce account: ",
            Self::Withdraw => "Withdraw nonce account: ",
            Self::Initialize => "Initialize nonce account: ",
            Self::Authorize => "Authorize nonce account: ",
        }
    }
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
    let mut out = 0u64;
    let mut saw_digit = false;

    for b in s.trim().bytes() {
        match b {
            b'0'..=b'9' => {
                saw_digit = true;
                out = out.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
            }
            b',' => {}
            _ => return None,
        }
    }

    saw_digit.then_some(out)
}

#[inline]
fn pubkey_id_to_pubkey(store: &KeyStore, id: PubkeyId) -> Pubkey {
    id.to_pubkey(store).unwrap_or_else(|| {
        panic!(
            "SystemProgramLog: PubkeyId out of bounds: id={:?} len={}",
            id,
            store.len()
        )
    })
}

#[inline]
fn pubkey_or_string_to_string(v: PubkeyOrString, st: &StringTable, store: &KeyStore) -> String {
    match v {
        PubkeyOrString::Pubkey(id) => pubkey_id_to_pubkey(store, id).to_string(),
        PubkeyOrString::Text(sid) => st.resolve(sid).to_string(),
    }
}

#[inline]
fn system_address_to_string(v: SystemAddress, st: &StringTable, store: &KeyStore) -> String {
    match v {
        SystemAddress::Pubkey(value) => pubkey_or_string_to_string(value, st, store),
        SystemAddress::Debug { address, base } => {
            let address = pubkey_or_string_to_string(address, st, store);
            let base = match base {
                Some(base) => format!("Some({})", pubkey_or_string_to_string(base, st, store)),
                None => "None".to_string(),
            };
            format!("Address {{ address: {address}, base: {base} }}")
        }
    }
}

#[inline]
fn parse_between<'a>(line: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let b = line.as_bytes();
    if !b.starts_with(prefix.as_bytes()) || !b.ends_with(suffix.as_bytes()) {
        return None;
    }
    Some(&line[prefix.len()..line.len() - suffix.len()])
}

/// Parse the `Address { ... }` debug shape that system program prints in logs.
#[inline]
fn parse_system_address(
    index: &impl PubkeyCompactor,
    st: &mut StringTable,
    addr_txt: &str,
) -> Option<SystemAddress> {
    let s = addr_txt.trim();

    if let Some(inner) = s.strip_prefix("Address {") {
        let inner = inner.trim().strip_suffix('}')?.trim();
        let rest = inner
            .strip_prefix("address:")
            .or_else(|| inner.strip_prefix("address :"))?
            .trim();
        let (address_txt, base) = if let Some((address_txt, base_txt)) = rest.split_once(", base:")
        {
            (address_txt, parse_address_base(index, st, base_txt.trim())?)
        } else {
            (rest, None)
        };
        return Some(SystemAddress::Debug {
            address: parse_pubkey_or_string(index, st, address_txt),
            base,
        });
    }

    Some(SystemAddress::Pubkey(parse_pubkey_or_string(index, st, s)))
}

#[inline]
fn parse_address_base(
    index: &impl PubkeyCompactor,
    st: &mut StringTable,
    base_txt: &str,
) -> Option<Option<PubkeyOrString>> {
    if base_txt == "None" {
        return Some(None);
    }

    let base = base_txt.strip_prefix("Some(")?.strip_suffix(')')?.trim();
    Some(Some(parse_pubkey_or_string(index, st, base)))
}

/// Try to resolve a pubkey text into the registry. If missing, store the raw text in StringTable.
#[inline]
fn parse_pubkey_or_string(
    index: &impl PubkeyCompactor,
    st: &mut StringTable,
    s: &str,
) -> PubkeyOrString {
    let s = s.trim();
    if let Some(id) = index.compact_str(s) {
        PubkeyOrString::Pubkey(id)
    } else {
        PubkeyOrString::Text(st.push(s))
    }
}

#[inline]
fn parse_nonce_account_log(
    text: &str,
    index: &impl PubkeyCompactor,
    st: &mut StringTable,
) -> Option<SystemProgramLog> {
    let (action, rest) = parse_nonce_action_rest(text)?;

    if let Some(account) = parse_between(rest, "Account ", "must be writeable") {
        return Some(SystemProgramLog::NonceAccountMustBeWriteable {
            action,
            account: parse_pubkey_or_string(index, st, account),
        });
    }

    if let Some(account) = parse_between(rest, "Account ", "must be a signer") {
        return Some(SystemProgramLog::NonceAccountMustBeSigner {
            action,
            account: parse_pubkey_or_string(index, st, account),
        });
    }

    if let Some(account) = parse_between(rest, "Account ", "must sign") {
        return Some(SystemProgramLog::NonceAccountMustSign {
            action,
            account: parse_pubkey_or_string(index, st, account),
        });
    }

    if let Some(account) = parse_between(rest, "Account ", "state is invalid") {
        return Some(SystemProgramLog::NonceAccountStateInvalid {
            action,
            account: parse_pubkey_or_string(index, st, account),
        });
    }

    if rest == "nonce can only advance once per slot" {
        return Some(SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { action });
    }

    if let Some(rest) = rest.strip_prefix("insufficient lamports ")
        && let Some((have, need)) = rest.split_once(", need ")
    {
        return Some(SystemProgramLog::NonceInsufficientLamports {
            action,
            have: parse_u64_commas(have)?,
            need: parse_u64_commas(need)?,
        });
    }

    None
}

#[inline]
fn parse_nonce_action_rest(text: &str) -> Option<(NonceAction, &str)> {
    match text.as_bytes().first().copied()? {
        b'A' => text
            .strip_prefix(NonceAction::Advance.prefix())
            .map(|rest| (NonceAction::Advance, rest))
            .or_else(|| {
                text.strip_prefix(NonceAction::Authorize.prefix())
                    .map(|rest| (NonceAction::Authorize, rest))
            }),
        b'W' => text
            .strip_prefix(NonceAction::Withdraw.prefix())
            .map(|rest| (NonceAction::Withdraw, rest)),
        b'I' => text
            .strip_prefix(NonceAction::Initialize.prefix())
            .map(|rest| (NonceAction::Initialize, rest)),
        _ => None,
    }
}

#[inline]
pub fn has_known_binary_form(text: &str) -> bool {
    if text == "Advance nonce account: recent blockhash list is empty"
        || text == "Initialize nonce account: recent blockhash list is empty"
        || text == "Transfer: `from` must not carry data"
        || text.strip_prefix("Authorize nonce account: ").is_some()
        || has_known_nonce_binary_form(text)
    {
        return true;
    }

    if text
        .strip_prefix("Instruction: ")
        .and_then(SystemInstructionLog::parse)
        .is_some()
    {
        return true;
    }

    text.starts_with("Create: address ")
        || text.starts_with("SystemProgram::CreateAccount data size limited to ")
        || text.starts_with("Transfer: 'from' address ")
        || parse_between(text, "Create Account: account ", " already in use").is_some()
        || parse_between(text, "Allocate: account ", " already in use").is_some()
        || parse_between(text, "Allocate: 'to' account ", " must sign").is_some()
        || parse_between(text, "Assign: account ", " must sign").is_some()
        || text.starts_with("Allocate: requested ")
        || text.starts_with("Transfer: `from` account ")
        || text.starts_with("Transfer: insufficient lamports ")
}

#[inline]
fn has_known_nonce_binary_form(text: &str) -> bool {
    let Some((_action, rest)) = parse_nonce_action_rest(text) else {
        return false;
    };

    if parse_between(rest, "Account ", "must be writeable").is_some()
        || parse_between(rest, "Account ", "must be a signer").is_some()
        || parse_between(rest, "Account ", "must sign").is_some()
        || parse_between(rest, "Account ", "state is invalid").is_some()
        || rest == "nonce can only advance once per slot"
    {
        return true;
    }

    if let Some(rest) = rest.strip_prefix("insufficient lamports ")
        && let Some((have, need)) = rest.split_once(", need ")
    {
        return parse_u64_commas(have).is_some() && parse_u64_commas(need).is_some();
    }

    false
}

impl SystemProgramLog {
    /// `text` is the payload after "Program log: " or after "Program <id> log: "
    #[inline]
    pub fn parse(text: &str, index: &impl PubkeyCompactor, st: &mut StringTable) -> Option<Self> {
        let text = text.trim();

        if let Some(nonce) = parse_nonce_account_log(text, index, st) {
            return Some(nonce);
        }

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
                provided_addr: index.compact_str(provided_txt)?,
                derived_addr: parse_pubkey_or_string(index, st, derived_txt),
            });
        }

        // Transfer: 'from' address X does not match derived address Y
        if let Some(rest) = text.strip_prefix("Transfer: 'from' address ")
            && let Some(mid) = rest.find(" does not match derived address ")
        {
            let provided_txt = rest[..mid].trim();
            let derived_txt = rest[mid + " does not match derived address ".len()..].trim();
            return Some(Self::TransferFromAddressMismatch {
                provided_addr: index.compact_str(provided_txt)?,
                derived_addr: parse_pubkey_or_string(index, st, derived_txt),
            });
        }

        // Create Account: account {:?} already in use
        if let Some(addr_txt) = parse_between(text, "Create Account: account ", " already in use") {
            let addr = parse_system_address(index, st, addr_txt)?;
            return Some(Self::CreateAccountAlreadyInUse { addr });
        }

        // Allocate: account {:?} already in use
        if let Some(addr_txt) = parse_between(text, "Allocate: account ", " already in use") {
            let addr = parse_system_address(index, st, addr_txt)?;
            return Some(Self::AllocateAlreadyInUse { addr });
        }

        // Allocate: 'to' account {:?} must sign
        if let Some(addr_txt) = parse_between(text, "Allocate: 'to' account ", " must sign") {
            let addr = parse_system_address(index, st, addr_txt)?;
            return Some(Self::AllocateToMustSign { addr });
        }

        // Assign: account {:?} must sign
        if let Some(addr_txt) = parse_between(text, "Assign: account ", " must sign") {
            let addr = parse_system_address(index, st, addr_txt)?;
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

        if let Some(limit) = parse_between(
            text,
            "SystemProgram::CreateAccount data size limited to ",
            " in inner instructions",
        )
        .and_then(parse_u64_commas)
        {
            return Some(Self::CreateAccountDataSizeLimitedInInnerInstructions { limit });
        }

        // Transfer: `from` account <pubkey> must sign
        if let Some(rest) = text.strip_prefix("Transfer: `from` account ")
            && let Some(pk_txt) = rest.strip_suffix(" must sign")
        {
            return Some(Self::TransferFromMustSign {
                from: index.compact_str(pk_txt.trim())?,
            });
        }

        // Transfer: insufficient lamports <have>, need <need>
        if let Some(rest) = text.strip_prefix("Transfer: insufficient lamports ")
            && let Some(pos) = rest.find(", need ")
        {
            return Some(Self::TransferInsufficient {
                have: parse_u64_commas(&rest[..pos])?,
                need: parse_u64_commas(&rest[pos + ", need ".len()..])?,
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
    pub fn render(&self, st: &StringTable, store: &KeyStore) -> String {
        match self {
            Self::Instruction(ix) => ix.as_str().to_string(),

            Self::CreateAddressMismatch {
                provided_addr,
                derived_addr,
            } => format!(
                "Create: address {} does not match derived address {}",
                pubkey_id_to_pubkey(store, *provided_addr),
                pubkey_or_string_to_string(*derived_addr, st, store),
            ),

            Self::TransferFromAddressMismatch {
                provided_addr,
                derived_addr,
            } => format!(
                "Transfer: 'from' address {} does not match derived address {}",
                pubkey_id_to_pubkey(store, *provided_addr),
                pubkey_or_string_to_string(*derived_addr, st, store),
            ),

            Self::CreateAccountAlreadyInUse { addr }
            | Self::CreateAccountAccountAlreadyInUse { addr } => format!(
                "Create Account: account {} already in use",
                system_address_to_string(*addr, st, store),
            ),

            Self::AllocateAlreadyInUse { addr } | Self::AllocateAccountAlreadyInUse { addr } => {
                format!(
                    "Allocate: account {} already in use",
                    system_address_to_string(*addr, st, store),
                )
            }

            Self::AllocateToMustSign { addr } => format!(
                "Allocate: 'to' account {} must sign",
                system_address_to_string(*addr, st, store),
            ),

            Self::AssignAccountMustSign { addr } => format!(
                "Assign: account {} must sign",
                system_address_to_string(*addr, st, store),
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

            Self::CreateAccountDataSizeLimitedInInnerInstructions { limit } => {
                format!(
                    "SystemProgram::CreateAccount data size limited to {} in inner instructions",
                    limit
                )
            }

            Self::TransferFromMustSign { from } => format!(
                "Transfer: `from` account {} must sign",
                pubkey_id_to_pubkey(store, *from),
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

            Self::NonceAccountMustBeWriteable { action, account } => format!(
                "{} nonce account: Account {} must be writeable",
                action.as_str(),
                pubkey_or_string_to_string(*account, st, store),
            ),

            Self::NonceAccountMustBeSigner { action, account } => format!(
                "{} nonce account: Account {} must be a signer",
                action.as_str(),
                pubkey_or_string_to_string(*account, st, store),
            ),

            Self::NonceAccountMustSign { action, account } => format!(
                "{} nonce account: Account {} must sign",
                action.as_str(),
                pubkey_or_string_to_string(*account, st, store),
            ),

            Self::NonceAccountStateInvalid { action, account } => format!(
                "{} nonce account: Account {} state is invalid",
                action.as_str(),
                pubkey_or_string_to_string(*account, st, store),
            ),

            Self::NonceInsufficientLamports { action, have, need } => format!(
                "{} nonce account: insufficient lamports {}, need {}",
                action.as_str(),
                have,
                need,
            ),

            Self::NonceCanOnlyAdvanceOncePerSlot { action } => {
                format!(
                    "{} nonce account: nonce can only advance once per slot",
                    action.as_str()
                )
            }
        }
    }
}
