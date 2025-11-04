use ahash::AHashSet;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::mem::MaybeUninit;
use std::ptr::copy_nonoverlapping;
use wincode::ReadResult;
use wincode::SchemaRead;
use wincode::containers::{self, Elem, Pod};
use wincode::io::Reader;
use wincode::len::SeqLen;
use wincode::len::ShortU16Len;

/// Solana shortvec decoder
#[inline(always)]
fn read_short_u16_len(buf: &[u8], pos: &mut usize) -> anyhow::Result<usize> {
    let mut len = 0usize;
    let mut shift = 0;
    loop {
        if *pos >= buf.len() {
            anyhow::bail!("shortvec overflow");
        }
        let b = buf[*pos];
        *pos += 1;
        len |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(len)
}

#[inline(always)]
pub fn parse_account_keys_only_fast(
    tx: &[u8],
    out: &mut SmallVec<[Pubkey; 256]>,
) -> Result<Option<Pubkey>> {
    let mut pos = 0usize;

    let sig_len = read_short_u16_len(tx, &mut pos)? as usize;
    pos += sig_len * 64;
    if pos >= tx.len() {
        return Ok(None);
    }

    let prefix = tx[pos];
    let is_v0 = prefix & 0x80 != 0;
    if is_v0 {
        pos += 1;
    }

    pos += 3;
    if pos >= tx.len() {
        return Ok(None);
    }

    let n_keys = read_short_u16_len(tx, &mut pos)? as usize;
    if n_keys == 0 {
        return Ok(None);
    }

    out.clear();
    out.reserve(n_keys);
    let mut first_key: Option<Pubkey> = None;

    for i in 0..n_keys {
        if pos + 32 > tx.len() {
            anyhow::bail!("truncated key array");
        }
        let mut key = [0u8; 32];
        unsafe {
            copy_nonoverlapping(tx[pos..].as_ptr(), key.as_mut_ptr(), 32);
        }
        pos += 32;
        let pk = Pubkey::new_from_array(key);
        if i == 0 {
            first_key = Some(pk);
        }
        out.push(pk);
    }

    pos += 32;

    if is_v0 {
        let n_ix = read_short_u16_len(tx, &mut pos)? as usize;
        for _ in 0..n_ix {
            pos += 1;
            let ac_len = read_short_u16_len(tx, &mut pos)? as usize;
            pos += ac_len;
            let data_len = read_short_u16_len(tx, &mut pos)? as usize;
            pos += data_len;
        }

        let n_lookups = read_short_u16_len(tx, &mut pos)? as usize;
        for _ in 0..n_lookups {
            if pos + 32 > tx.len() {
                anyhow::bail!("truncated lookup key");
            }
            let mut key = [0u8; 32];
            unsafe {
                copy_nonoverlapping(tx[pos..].as_ptr(), key.as_mut_ptr(), 32);
            }
            pos += 32;
            out.push(Pubkey::new_from_array(key));

            let w_len = read_short_u16_len(tx, &mut pos)? as usize;
            pos += w_len;
            let r_len = read_short_u16_len(tx, &mut pos)? as usize;
            pos += r_len;
        }
    }

    Ok(first_key)
}

pub fn parse_account_keys_only(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<Option<Pubkey>> {
    let mut reader = Reader::new(tx);

    let sig_len = ShortU16Len::read::<usize>(&mut reader)?;
    let sig_bytes = sig_len.saturating_mul(64);
    reader.consume(sig_bytes)?;

    let buf = reader.as_slice();
    if buf.is_empty() {
        return Ok(None);
    }
    let prefix = buf[0];
    let is_v0 = prefix & 0x80 != 0;
    if is_v0 {
        reader.consume(1)?;
    }

    let mut header = [MaybeUninit::zeroed(); 3];
    reader.read_exact(&mut header)?;

    let n_keys = ShortU16Len::read::<usize>(&mut reader)?;
    if n_keys == 0 {
        return Ok(None);
    }

    let mut first_key: Option<Pubkey> = None;
    for i in 0..n_keys {
        let mut key_bytes = [MaybeUninit::zeroed(); 32];
        reader.read_exact(&mut key_bytes)?;
        let key_bytes: [u8; 32] = unsafe { std::mem::transmute_copy(&key_bytes) };
        let key = Pubkey::new_from_array(key_bytes);
        if i == 0 {
            first_key = Some(key);
        }
        out.insert(key);
    }

    reader.consume(32)?;

    if is_v0 {
        let n_instructions = ShortU16Len::read::<usize>(&mut reader)?;
        for _ in 0..n_instructions {
            reader.consume(1)?;
            let ac_len = ShortU16Len::read::<usize>(&mut reader)?;
            reader.consume(ac_len)?;
            let data_len = ShortU16Len::read::<usize>(&mut reader)?;
            reader.consume(data_len)?;
        }

        let n_lookups = ShortU16Len::read::<usize>(&mut reader)?;
        for _ in 0..n_lookups {
            let mut key = [MaybeUninit::zeroed(); 32];
            reader.read_exact(&mut key)?;
            let key: [u8; 32] = unsafe { std::mem::transmute_copy(&key) };
            out.insert(Pubkey::new_from_array(key));

            let w_len = ShortU16Len::read::<usize>(&mut reader)?;
            reader.consume(w_len)?;
            let r_len = ShortU16Len::read::<usize>(&mut reader)?;
            reader.consume(r_len)?;
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
            reader.consume(1)?;
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
    #[inline]
    pub fn instructions_len(&self) -> usize {
        match self {
            VersionedMessage::Legacy(m) => m.instructions.len(),
            VersionedMessage::V0(m) => m.instructions.len(),
        }
    }

    #[inline]
    pub fn instructions_iter(&self) -> impl Iterator<Item = &CompiledInstruction> {
        match self {
            VersionedMessage::Legacy(m) => m.instructions.iter(),
            VersionedMessage::V0(m) => m.instructions.iter(),
        }
    }
}
