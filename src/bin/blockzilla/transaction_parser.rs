use ahash::AHashSet;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::mem::MaybeUninit;
use wincode::ReadResult;
use wincode::SchemaRead;
use wincode::containers::{self, Elem, Pod};
use wincode::io::Reader;
use wincode::len::SeqLen;
use wincode::len::ShortU16Len;

pub fn parse_bincode_tx_static_accounts(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<()> {
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

pub fn parse_account_keys_only(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<()> {
    let mut reader = Reader::new(tx);

    // 1️⃣ Read signatures (skip their bytes)
    let sig_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
    let sig_bytes = sig_len.saturating_mul(64);
    reader.consume(sig_bytes)?;

    // 2️⃣ Peek message prefix
    let buf = reader.as_slice();
    if buf.is_empty() {
        return Ok(());
    }
    let prefix = buf[0];
    let is_v0 = prefix & 0x80 != 0;
    if is_v0 {
        // skip prefix byte
        reader.consume(1)?;
    }

    // 3️⃣ Read header
    let mut header = [MaybeUninit::zeroed(); 3];
    reader.read_exact(&mut header)?;

    // 4️⃣ Read account_keys short vec
    let n_keys = ShortU16Len::read::<usize>(&mut reader)? as usize;
    for _ in 0..n_keys {
        let mut key_bytes = [MaybeUninit::zeroed(); 32];
        reader.read_exact(&mut key_bytes)?;
        // SAFETY: All bytes have been initialized by read_exact.
        let key_bytes: [u8; 32] = unsafe { std::mem::transmute_copy(&key_bytes) };
        out.insert(Pubkey::new_from_array(key_bytes));
    }

    // 5️⃣ Skip recent_blockhash (32 bytes)
    reader.consume(32)?;

    if is_v0 {
        // Skip instructions entirely — read len and skip per-instruction payload
        let n_instructions = ShortU16Len::read::<usize>(&mut reader)? as usize;
        for _ in 0..n_instructions {
            reader.consume(1)?; // program_id_index
            let ac_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
            reader.consume(ac_len)?;
            let data_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
            reader.consume(data_len)?;
        }

        // Read address_table_lookups
        let n_lookups = ShortU16Len::read::<usize>(&mut reader)? as usize;
        for _ in 0..n_lookups {
            let mut key = [MaybeUninit::zeroed(); 32];
            reader.read_exact(&mut key)?;
            // SAFETY: All bytes have been initialized by read_exact.
            let key: [u8; 32] = unsafe { std::mem::transmute_copy(&key) };
            out.insert(Pubkey::new_from_array(key));

            let w_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
            reader.consume(w_len)?;
            let r_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
            reader.consume(r_len)?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct VersionedTransaction {
    #[wincode(with = "containers::Vec<Pod<Signature>, ShortU16Len>")]
    pub signatures: Vec<Signature>,
    pub message: VersionedMessage,
}

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
            reader.consume(1)?; // advances by one
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
