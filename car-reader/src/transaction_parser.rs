use anyhow::Result;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::mem::MaybeUninit;
use std::ptr::copy_nonoverlapping;
use wincode::ReadResult;
use wincode::containers;
use wincode::io::Reader;
use wincode::len::ShortU16Len;
use wincode::{SchemaRead, SchemaWrite};

pub use compact_archive::format::{CompiledInstruction, MessageHeader, Signature};

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
pub fn parse_account_keys_only(
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

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct VersionedTransaction {
    #[wincode(with = "containers::Vec<Signature, ShortU16Len>")]
    pub signatures: Vec<Signature>,
    pub message: VersionedMessage,
}

pub const PUBKEY_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct MessageAddressTableLookup {
    pub account_key: [u8; PUBKEY_BYTES],
    #[wincode(with = "containers::Vec<u8, ShortU16Len>")]
    pub writable_indexes: Vec<u8>,
    #[wincode(with = "containers::Vec<u8, ShortU16Len>")]
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct LegacyMessage {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<[u8; PUBKEY_BYTES], ShortU16Len>")]
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
    pub recent_blockhash: [u8; 32],
    #[wincode(with = "containers::Vec<CompiledInstruction, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct V0Message {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<[u8; PUBKEY_BYTES], ShortU16Len>")]
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
    pub recent_blockhash: [u8; 32],
    #[wincode(with = "containers::Vec<CompiledInstruction, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,
    #[wincode(with = "containers::Vec<MessageAddressTableLookup, ShortU16Len>")]
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedMessage {
    Legacy(LegacyMessage),
    V0(V0Message),
}
impl<'de> SchemaRead<'de> for VersionedMessage {
    type Dst = Self;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let first = reader.peek()?;

        let value = if first & 0x80 == 0 {
            // LEGACY
            let mut inner = MaybeUninit::uninit();
            LegacyMessage::read(reader, &mut inner)?;
            VersionedMessage::Legacy(unsafe { inner.assume_init() })
        } else {
            // V0
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
