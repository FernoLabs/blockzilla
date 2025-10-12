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

pub fn parse_bincode_tx_static_accounts(
    tx: &[u8],
    out: &mut AHashSet<Pubkey>,
) -> Result<Option<Pubkey>> {
    let vt: VersionedTransaction = wincode::deserialize(tx)?;

    let keys = vt.message.static_account_keys();
    if keys.is_empty() {
        return Ok(None);
    }

    // The first static account key is always the fee payer
    let fee_payer = Pubkey::new_from_array(keys[0]);

    // Add all static account keys
    out.extend(keys.iter().map(|p| Pubkey::new_from_array(*p)));

    // Address lookup table accounts (v0 only)
    if let Some(lookups) = vt.message.address_table_lookups() {
        out.extend(
            lookups
                .iter()
                .map(|l| Pubkey::new_from_array(l.account_key)),
        );
    }

    Ok(Some(fee_payer))
}

#[inline]
fn read_u16_le(buf: &[u8]) -> u16 {
    u16::from_le_bytes([buf[0], buf[1]])
}

#[inline]
fn read_pubkey_bytes(buf: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 32];
    key.copy_from_slice(&buf[..32]);
    key
}

pub fn parse_account_keys_only(
    tx: &[u8],
    out: &mut AHashSet<Pubkey>,
) -> Result<Option<Pubkey>> {
    let mut pos = 0;
    let len = tx.len();

    // 1️⃣ Read signatures (skip their bytes)
    if pos + 2 > len {
        return Ok(None);
    }
    let sig_len = read_u16_le(&tx[pos..]) as usize;
    pos += 2;
    let sig_bytes = sig_len.saturating_mul(64);
    pos += sig_bytes;
    if pos > len {
        return Ok(None);
    }

    // 2️⃣ Peek message prefix
    if pos >= len {
        return Ok(None);
    }
    let prefix = tx[pos];
    let is_v0 = prefix & 0x80 != 0;
    if is_v0 {
        pos += 1;
    }

    // 3️⃣ Read header (3 bytes)
    if pos + 3 > len {
        return Ok(None);
    }
    pos += 3; // Skip the 3 header bytes

    // 4️⃣ Read static account keys
    if pos + 2 > len {
        return Ok(None);
    }
    let n_keys = read_u16_le(&tx[pos..]) as usize;
    pos += 2;

    if n_keys == 0 {
        return Ok(None);
    }

    let mut first_key: Option<Pubkey> = None;

    // Bulk read all static keys
    let static_keys_end = pos + n_keys * 32;
    if static_keys_end > len {
        return Ok(None);
    }

    for i in 0..n_keys {
        let key_slice = &tx[pos + i * 32..pos + (i + 1) * 32];
        let key_bytes = read_pubkey_bytes(key_slice);
        let key = Pubkey::new_from_array(key_bytes);

        if i == 0 {
            first_key = Some(key);
        }
        out.insert(key);
    }
    pos = static_keys_end;

    // 5️⃣ Skip recent_blockhash (32 bytes)
    if pos + 32 > len {
        return Ok(first_key);
    }
    pos += 32;

    if is_v0 {
        // Skip instructions
        if pos + 2 > len {
            return Ok(first_key);
        }
        let n_instructions = read_u16_le(&tx[pos..]) as usize;
        pos += 2;

        for _ in 0..n_instructions {
            if pos + 1 > len {
                return Ok(first_key);
            }
            pos += 1; // program_id_index

            if pos + 2 > len {
                return Ok(first_key);
            }
            let ac_len = read_u16_le(&tx[pos..]) as usize;
            pos += 2;
            pos += ac_len;
            if pos > len {
                return Ok(first_key);
            }

            if pos + 2 > len {
                return Ok(first_key);
            }
            let data_len = read_u16_le(&tx[pos..]) as usize;
            pos += 2;
            pos += data_len;
            if pos > len {
                return Ok(first_key);
            }
        }

        // Read address_table_lookups
        if pos + 2 > len {
            return Ok(first_key);
        }
        let n_lookups = read_u16_le(&tx[pos..]) as usize;
        pos += 2;

        for _ in 0..n_lookups {
            if pos + 32 > len {
                return Ok(first_key);
            }
            let key_bytes = read_pubkey_bytes(&tx[pos..]);
            out.insert(Pubkey::new_from_array(key_bytes));
            pos += 32;

            if pos + 2 > len {
                return Ok(first_key);
            }
            let w_len = read_u16_le(&tx[pos..]) as usize;
            pos += 2;
            pos += w_len;
            if pos > len {
                return Ok(first_key);
            }

            if pos + 2 > len {
                return Ok(first_key);
            }
            let r_len = read_u16_le(&tx[pos..]) as usize;
            pos += 2;
            pos += r_len;
            if pos > len {
                return Ok(first_key);
            }
        }
    }

    Ok(first_key)
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

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub accounts: Vec<u8>,
    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct MessageAddressTableLookup {
    pub account_key: [u8; PUBKEY_BYTES],
    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub writable_indexes: Vec<u8>,
    #[wincode(with = "containers::Vec<Pod<u8>, ShortU16Len>")]
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct LegacyMessage {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<Pod<[u8; PUBKEY_BYTES]>, ShortU16Len>")]
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
    pub recent_blockhash: [u8; 32],
    #[wincode(with = "containers::Vec<Elem<CompiledInstruction>, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,
}

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
        let value = if first & 0x80 == 0 {
            let mut inner = MaybeUninit::uninit();
            LegacyMessage::read(reader, &mut inner)?;
            VersionedMessage::Legacy(unsafe { inner.assume_init() })
        } else {
            reader.consume(1)?; // skip prefix
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
