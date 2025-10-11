use ahash::AHashSet;
use anyhow::{Result, anyhow};
use bincode::serde::Compat;
use solana_sdk::pubkey::Pubkey;
use std::mem::MaybeUninit;
use tracing::{debug, warn};
use wincode::ReadResult;
use wincode::SchemaRead;
use wincode::containers::{self, Elem, Pod};
use wincode::io::Reader;
use wincode::len::ShortU16Len;

pub fn parse_bincode_tx_static_accounts(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<()> {
    //wincode_parse(tx, out)
    bincode_parse(tx, out)
}

fn wincode_parse(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<()> {
    let vt: VersionedTransaction = wincode::deserialize(tx)?;
    // Static account keys
    out.extend(
        vt.message
            .static_account_keys()
            .iter()
            .map(|p| Pubkey::new_from_array(*p)),
    );

    // Address lookup table accounts (v0 only)
    if let Some(lookups) = vt.message.address_table_lookups() {
        out.extend(
            lookups
                .iter()
                .map(|l| Pubkey::new_from_array(l.account_key)),
        );
    }

    Ok(())
}

pub fn bincode_parse(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<()> {
    let (Compat(vt), _): (Compat<solana_sdk::transaction::VersionedTransaction>, _) =
        bincode::decode_from_slice(tx, bincode::config::legacy())?;

    // Static account keys
    out.extend(vt.message.static_account_keys());

    // Address lookup table accounts (v0 only)
    if let Some(lookups) = vt.message.address_table_lookups() {
        out.extend(lookups.iter().map(|l| l.account_key));
    }

    Ok(())
}

// ------------------- Types -------------------

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct VersionedTransaction {
    #[wincode(with = "containers::Vec<Pod<Signature>, ShortU16Len>")] // ✅ short_vec
    pub signatures: Vec<Signature>,
    pub message: VersionedMessage,
}

/// 64-byte signature (plain-old-data)
#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead)]
#[repr(transparent)]
pub struct Signature(pub [u8; 64]);

pub const PUBKEY_BYTES: usize = 32;

/// Single program instruction
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct CompiledInstruction {
    pub program_id_index: u8,

    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub accounts: Vec<u8>,

    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub data: Vec<u8>,
}

/// Message header metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

/// Address table lookups for v0 messages
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct MessageAddressTableLookup {
    pub account_key: [u8; PUBKEY_BYTES],

    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub writable_indexes: Vec<u8>,

    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub readonly_indexes: Vec<u8>,
}

/// Legacy message (pre-v0)
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct LegacyMessage {
    pub header: MessageHeader,

    #[wincode(with = "containers::Vec<Pod<[u8; PUBKEY_BYTES]>, ShortU16Len>")]
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,

    pub recent_blockhash: [u8; 32],

    #[wincode(with = "containers::Vec<Elem<CompiledInstruction>, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,
}

/// Version 0 message (adds lookups)
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct V0Message {
    pub header: MessageHeader,

    #[wincode(with = "containers::Vec<Pod<[u8; PUBKEY_BYTES]>, ShortU16Len>")]
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,

    pub recent_blockhash: [u8; 32],

    #[wincode(with = "containers::Vec<Elem<CompiledInstruction>, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,

    #[wincode(with = "containers::Vec<Elem<MessageAddressTableLookup>, ShortU16Len>")]
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

/// Unified versioned message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedMessage {
    Legacy(LegacyMessage),
    V0(V0Message),
}

impl<'de> SchemaRead<'de> for VersionedMessage {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let buf = reader.as_slice();
        if buf.is_empty() {
            return Err(wincode::ReadError::PointerSizedReadError);
        }

        let first = buf[0];

        if first != 0x80 && first < 0x01 {
            tracing::warn!("Suspicious message prefix: 0x{:02x}", first);
        }

        let value = if first & 0x80 == 0 {
            let mut inner = MaybeUninit::uninit();
            LegacyMessage::read(reader, &mut inner)?;
            VersionedMessage::Legacy(unsafe { inner.assume_init() })
        } else {
            let _version = first & 0x7F;
            // Consume prefix byte manually
            reader.read_exact(&mut vec![MaybeUninit::uninit()])?; // advances by one
            let mut inner = MaybeUninit::uninit();
            V0Message::read(reader, &mut inner)?;
            VersionedMessage::V0(unsafe { inner.assume_init() })
        };

        dst.write(value);
        Ok(())
    }
}

impl VersionedMessage {
    #[inline]
    pub fn static_account_keys(&self) -> &[[u8; PUBKEY_BYTES]] {
        match self {
            VersionedMessage::Legacy(m) => &m.account_keys,
            VersionedMessage::V0(m) => &m.account_keys,
        }
    }

    #[inline]
    pub fn address_table_lookups(&self) -> Option<&[MessageAddressTableLookup]> {
        match self {
            VersionedMessage::Legacy(_) => None,
            VersionedMessage::V0(m) => Some(&m.address_table_lookups),
        }
    }

    #[inline]
    pub fn header(&self) -> &MessageHeader {
        match self {
            VersionedMessage::Legacy(m) => &m.header,
            VersionedMessage::V0(m) => &m.header,
        }
    }
}

fn compare(
    theirs: &solana_sdk::transaction::VersionedTransaction,
    ours: &VersionedTransaction,
) -> Result<()> {
    // --- Compare signature list ---
    if ours.signatures.len() != theirs.signatures.len() {
        return Err(anyhow!(
            "Signature length mismatch: ours={} theirs={}",
            ours.signatures.len(),
            theirs.signatures.len()
        ));
    }
    for (i, (a, b)) in ours.signatures.iter().zip(&theirs.signatures).enumerate() {
        if a.0 != b.as_ref() {
            return Err(anyhow!("Signature #{i} mismatch"));
        }
    }

    // --- Compare static account keys ---
    let ours_keys = ours.message.static_account_keys();
    let theirs_keys: Vec<[u8; 32]> = theirs
        .message
        .static_account_keys()
        .iter()
        .map(|k| *k.as_array())
        .collect();

    if ours_keys != theirs_keys.as_slice() {
        return Err(anyhow!(
            "Account key mismatch (ours={} theirs={})",
            ours_keys.len(),
            theirs_keys.len()
        ));
    }

    // --- Compare recent blockhash ---
    let ours_hash = match &ours.message {
        VersionedMessage::Legacy(m) => m.recent_blockhash,
        VersionedMessage::V0(m) => m.recent_blockhash,
    };
    let theirs_hash = theirs.message.recent_blockhash().as_ref();
    if ours_hash != theirs_hash {
        return Err(anyhow!("Recent blockhash mismatch"));
    }

    // --- Compare message type ---
    let ours_ver = match &ours.message {
        VersionedMessage::Legacy(_) => "Legacy",
        VersionedMessage::V0(_) => "V0",
    };
    let theirs_ver = match theirs.version() {
        solana_sdk::transaction::TransactionVersion::Legacy(_) => "Legacy",
        solana_sdk::transaction::TransactionVersion::Number(_) => "V0",
    };

    if ours_ver != theirs_ver {
        return Err(anyhow!(
            "Message version mismatch (ours={ours_ver}, theirs={theirs_ver})"
        ));
    }

    Ok(())
}
