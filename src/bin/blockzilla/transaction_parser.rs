use ahash::AHashMap;
use ahash::AHashSet;
use anyhow::Result;
use blockzilla::car_block_reader::CarBlock;
use blockzilla::node::Node;
use solana_sdk::pubkey::Pubkey;
use std::io::Read;
use std::mem::MaybeUninit;
use std::ptr::copy_nonoverlapping;
use wincode::ReadResult;
use wincode::SchemaRead;
use wincode::containers::{self, Elem, Pod};
use wincode::io::Reader;
use wincode::len::SeqLen;
use wincode::len::ShortU16Len;

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct KeyStats {
    pub id: u32,
    pub count: u32,
    pub first_fee_payer: u32,
    pub first_epoch: u16,
    pub last_epoch: u16,
}

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
    out: &mut AHashSet<Pubkey>,
) -> anyhow::Result<Option<Pubkey>> {
    let mut pos = 0usize;

    // 1️⃣ Skip signatures
    let sig_len = read_short_u16_len(tx, &mut pos)? as usize;
    pos += sig_len * 64;
    if pos >= tx.len() {
        return Ok(None);
    }

    // 2️⃣ Version prefix
    let prefix = tx[pos];
    let is_v0 = prefix & 0x80 != 0;
    if is_v0 {
        pos += 1;
    }

    // 3️⃣ Header (3 bytes)
    pos += 3;
    if pos >= tx.len() {
        return Ok(None);
    }

    // 4️⃣ Account keys
    let n_keys = read_short_u16_len(tx, &mut pos)? as usize;
    if n_keys == 0 {
        return Ok(None);
    }

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
        out.insert(pk);
    }

    // 5️⃣ Skip recent_blockhash
    pos += 32;

    // 6️⃣ Versioned message (v0)
    if is_v0 {
        // Instructions
        let n_ix = read_short_u16_len(tx, &mut pos)? as usize;
        for _ in 0..n_ix {
            pos += 1; // program_id_index
            let ac_len = read_short_u16_len(tx, &mut pos)? as usize;
            pos += ac_len;
            let data_len = read_short_u16_len(tx, &mut pos)? as usize;
            pos += data_len;
        }

        // Address table lookups
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
            out.insert(Pubkey::new_from_array(key));

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

    // 1️⃣ Read signatures (skip their bytes)
    let sig_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
    let sig_bytes = sig_len.saturating_mul(64);
    reader.consume(sig_bytes)?;

    // 2️⃣ Peek message prefix
    let buf = reader.as_slice();
    if buf.is_empty() {
        return Ok(None);
    }
    let prefix = buf[0];
    let is_v0 = prefix & 0x80 != 0;
    if is_v0 {
        reader.consume(1)?; // skip prefix
    }

    // 3️⃣ Read header
    let mut header = [MaybeUninit::zeroed(); 3];
    reader.read_exact(&mut header)?;

    // 4️⃣ Read static account keys
    let n_keys = ShortU16Len::read::<usize>(&mut reader)? as usize;
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
            // First key is always fee payer
            first_key = Some(key);
        }
        out.insert(key);
    }

    // 5️⃣ Skip recent_blockhash (32 bytes)
    reader.consume(32)?;

    if is_v0 {
        // Skip instructions
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
            let key: [u8; 32] = unsafe { std::mem::transmute_copy(&key) };
            out.insert(Pubkey::new_from_array(key));

            let w_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
            reader.consume(w_len)?;
            let r_len = ShortU16Len::read::<usize>(&mut reader)? as usize;
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

/// Parse transactions within a CarBlock, extract all unique pubkeys,
/// and update `current_map` with KeyStats counts.
pub fn extract_transactions(
    cb: &CarBlock,
    map: &mut AHashMap<Pubkey, KeyStats>,
    next_id: &mut u32,
    epoch: u64,
) -> Result<()> {
    for entry_cid in cb.block()?.entries.iter() {
        let entry_cid = entry_cid?;
        let Node::Entry(entry) = cb.decode(entry_cid.hash_bytes())? else {
            tracing::error!("Entry not a Node::Entry {entry_cid:?}");
            continue;
            //return Err(anyhow!("Entry not a Node::Entry"));
        };
        for tx_cid in entry.transactions.iter() {
            let tx_cid = tx_cid?;
            let Node::Transaction(tx) = cb.decode(tx_cid.hash_bytes())? else {
                tracing::error!("Entry not a Node::Transaction {tx_cid:?}");
                continue;
                //return Err(anyhow!("Entry not a Node::Transaction"));
            };
            let mut out = Vec::new();
            let tx_bytes = match tx.data.next {
                None => tx.data.data,
                Some(df_cid) => {
                    let df_cid = df_cid.to_cid()?;
                    let mut reader = cb.dataframe_reader(&df_cid);
                    reader.read_to_end(&mut out)?;
                    &out
                }
            };

            let mut keys = AHashSet::with_capacity(32);
            // cargo run --release registry --file epoch-1.car  12.78s user 1.65s system 93% cpu 15.501 total
            // cargo run --release registry --file epoch-1.car  19.56s user 2.29s system 94% cpu 23.122 total
            let fee_payer_opt = parse_bincode_tx_static_accounts(tx_bytes, &mut keys)?;

            // Track fee payer
            let fee_payer_id = if let Some(fee_payer) = fee_payer_opt {
                ensure_key(fee_payer, map, next_id, epoch, 0)
            } else {
                0
            };

            for key in keys {
                let entry = map.entry(key).or_insert_with(|| {
                    let id = *next_id;
                    *next_id += 1;
                    KeyStats {
                        id,
                        count: 0,
                        first_fee_payer: fee_payer_id,
                        first_epoch: epoch as u16,
                        last_epoch: epoch as u16,
                    }
                });
                entry.count += 1;
                entry.last_epoch = epoch as u16;
            }
        }
    }
    Ok(())
}

/// Ensure a key exists in the map and return its ID.
fn ensure_key(
    key: Pubkey,
    map: &mut AHashMap<Pubkey, KeyStats>,
    next_id: &mut u32,
    epoch: u64,
    fee_payer_id: u32,
) -> u32 {
    map.entry(key)
        .or_insert_with(|| {
            let id = *next_id;
            *next_id += 1;
            KeyStats {
                id,
                count: 0,
                first_fee_payer: fee_payer_id,
                first_epoch: epoch as u16,
                last_epoch: epoch as u16,
            }
        })
        .id
}
