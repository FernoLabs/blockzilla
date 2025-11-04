use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::{CarBlock, CarBlockReader},
    confirmed_block,
    node::Node,
    open_epoch::{self, FetchMode},
};
use prost::Message;
use serde::{Deserialize, Serialize};
use solana_message::MessageHeader;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use std::{
    collections::{HashMap, hash_map::Entry},
    hash::{BuildHasherDefault, Hasher},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crate::transaction_parser::{VersionedMessage, VersionedTransaction};

const LOG_INTERVAL_SECS: u64 = 10;

#[derive(Default)]
struct NoOpHasher(u64);
impl Hasher for NoOpHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
    fn write(&mut self, _: &[u8]) {}
}
type U64Map<V> = HashMap<u64, V, BuildHasherDefault<NoOpHasher>>;

#[inline(always)]
fn pubkey_fp(k: &[u8]) -> u64 {
    u64::from_le_bytes([k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7]])
}

enum IndexEntry {
    Single(u32),
    Multiple(Vec<u32>),
}

impl IndexEntry {
    fn push(&mut self, id: u32) {
        match self {
            IndexEntry::Single(existing) => {
                let mut v = Vec::with_capacity(2);
                v.push(*existing);
                v.push(id);
                *self = IndexEntry::Multiple(v);
            }
            IndexEntry::Multiple(list) => list.push(id),
        }
    }
}

pub struct OrderedPubkeyRegistry {
    index: U64Map<IndexEntry>,
    pubkeys: Box<[u8]>,
    len: usize,
}

impl OrderedPubkeyRegistry {
    const KEY_SIZE: usize = 32;

    pub fn load<P: AsRef<Path>>(registry_dir: P, epoch: u64) -> Result<Self> {
        let path = registry_dir
            .as_ref()
            .join(format!("registry-pubkeys-{epoch:04}.bin"));
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("missing registry pubkeys file: {}", path.display()))?;
        let len_bytes = file.metadata()?.len() as usize;
        if len_bytes % Self::KEY_SIZE != 0 {
            return Err(anyhow!(
                "registry pubkeys file has invalid length: {} bytes",
                len_bytes
            ));
        }
        let mut data = Vec::with_capacity(len_bytes);
        // SAFETY: we immediately fill the buffer via read_exact
        unsafe {
            data.set_len(len_bytes);
        }
        file.read_exact(&mut data)?;
        let pubkeys = data.into_boxed_slice();

        let len = pubkeys.len() / Self::KEY_SIZE;
        let mut index: U64Map<IndexEntry> =
            HashMap::with_capacity_and_hasher(len, Default::default());
        for id in 0..len {
            let start = id * Self::KEY_SIZE;
            let slice = &pubkeys[start..start + Self::KEY_SIZE];
            let fp = pubkey_fp(slice);
            match index.entry(fp) {
                Entry::Vacant(v) => {
                    v.insert(IndexEntry::Single(id as u32));
                }
                Entry::Occupied(mut o) => {
                    o.get_mut().push(id as u32);
                }
            }
        }

        Ok(Self {
            index,
            pubkeys,
            len,
        })
    }

    #[inline]
    fn slice_for(&self, id: u32) -> &[u8] {
        let start = id as usize * Self::KEY_SIZE;
        &self.pubkeys[start..start + Self::KEY_SIZE]
    }

    #[inline]
    fn lookup_raw(&self, pk: &Pubkey) -> Option<u32> {
        let bytes = pk.as_ref();
        let fp = pubkey_fp(bytes);
        match self.index.get(&fp)? {
            IndexEntry::Single(id) => {
                if self.slice_for(*id).eq(bytes) {
                    Some(*id)
                } else {
                    None
                }
            }
            IndexEntry::Multiple(ids) => {
                ids.iter().copied().find(|id| self.slice_for(*id).eq(bytes))
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

pub trait RegistryAccess {
    fn get_or_insert(&mut self, pk: &Pubkey) -> Result<u32>;
}

impl RegistryAccess for OrderedPubkeyRegistry {
    fn get_or_insert(&mut self, pk: &Pubkey) -> Result<u32> {
        self.lookup_raw(pk)
            .ok_or_else(|| anyhow!("pubkey {} missing from registry", pk))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactInstruction {
    pub program_id: u32,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactInnerInstructions {
    pub index: u8,
    pub instructions: Vec<CompactInstruction>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTokenBalance {
    pub account_index: u8,
    pub mint: u32,
    pub ui_token_amount: String,
    pub owner: u32,
    pub program_id: u32,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub account_key: u32,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactLoadedAddresses {
    pub writable: Vec<u32>,
    pub readonly: Vec<u32>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTransactionMeta {
    pub err: Option<String>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<CompactInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<CompactTokenBalance>>,
    pub post_token_balances: Option<Vec<CompactTokenBalance>>,
    pub rewards: Option<Vec<CompactReward>>,
    pub loaded_addresses: Option<CompactLoadedAddresses>,
    pub return_data: Option<(u32, Vec<u8>)>,
    pub compute_units_consumed: Option<u64>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTransaction {
    pub signatures: Vec<Signature>,
    pub message_header: MessageHeader,
    pub account_keys: Vec<u32>,
    pub recent_blockhash: String,
    pub instructions: Vec<CompactInstruction>,
    pub address_table_lookups: Option<Vec<CompactAddressTableLookup>>,
    pub meta: Option<CompactTransactionMeta>,
    pub version: u8,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompactReward {
    pub pubkey: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct BlockWithIds {
    pub slot: u64,
    pub blockhash: String,
    pub previous_blockhash: String,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub rewards: Vec<CompactReward>,
    pub transactions: Vec<CompactTransaction>,
    pub num_transactions: u64,
}

thread_local! {
    static TL_META_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(256 * 1024));
    static TL_ZSTD_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(256 * 1024));
}
fn decode_protobuf_meta(bytes: &[u8]) -> Result<confirmed_block::TransactionStatusMeta> {
    if bytes.len() >= 4 && &bytes[0..4] == [0x28, 0xB5, 0x2F, 0xFD] {
        return TL_META_BUF.with(|cell| {
            let mut buf = cell.borrow_mut();
            buf.clear();
            zstd::stream::copy_decode(bytes, &mut *buf).context("zstd stream decode meta")?;
            confirmed_block::TransactionStatusMeta::decode(&buf[..])
                .context("prost decode after zstd")
        });
    }
    TL_ZSTD_BUF.with(|zbuf_cell| {
        let mut zbuf = zbuf_cell.borrow_mut();
        zbuf.clear();
        match zstd::bulk::decompress_to_buffer(bytes, &mut *zbuf) {
            Ok(_) => confirmed_block::TransactionStatusMeta::decode(&zbuf[..])
                .context("prost decode after bulk zstd"),
            Err(_) => confirmed_block::TransactionStatusMeta::decode(bytes)
                .context("prost decode raw failed"),
        }
    })
}

fn decode_wincode_tx(bytes: &[u8]) -> Result<VersionedTransaction> {
    let mut reader = wincode::io::Reader::new(bytes);
    let mut dst = std::mem::MaybeUninit::<VersionedTransaction>::uninit();
    <VersionedTransaction as wincode::SchemaRead>::read(&mut reader, &mut dst)
        .map_err(|_| anyhow!("wincode decode VersionedTransaction failed"))?;
    Ok(unsafe { dst.assume_init() })
}

fn cb_to_compact_block<R: RegistryAccess>(block: &CarBlock, reg: &mut R) -> Result<BlockWithIds> {
    let blk = block.block()?;
    let slot = blk.slot;
    let parent_slot = blk
        .meta
        .parent_slot
        .unwrap_or_else(|| slot.saturating_sub(1));
    let block_time = blk.meta.blocktime;
    let block_height = blk.meta.block_height;

    let blockhash = "missing".to_string();
    let previous_blockhash = "missing".to_string();

    let rewards = extract_rewards(block, reg).unwrap_or_default();

    let mut transactions = Vec::new();
    let mut tx_buf = Vec::<u8>::with_capacity(16 * 1024);
    let mut meta_buf = Vec::<u8>::with_capacity(8 * 1024);

    for entry_cid in blk.entries.iter() {
        let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else {
            continue;
        };
        for tx_cid in entry.transactions.iter() {
            let tx_cid = tx_cid?;
            let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                continue;
            };

            let tx_bytes: &[u8] = match tx.data.next {
                None => tx.data.data,
                Some(df_cbor) => {
                    tx_buf.clear();
                    let mut rdr = block.dataframe_reader(&df_cbor.to_cid()?);
                    rdr.read_to_end(&mut tx_buf)?;
                    &tx_buf
                }
            };

            let meta_bytes: &[u8] = match tx.metadata.next {
                None => tx.metadata.data,
                Some(df_cbor) => {
                    meta_buf.clear();
                    let mut rdr = block.dataframe_reader(&df_cbor.to_cid()?);
                    rdr.read_to_end(&mut meta_buf)?;
                    &meta_buf
                }
            };

            let vt = decode_wincode_tx(tx_bytes)?;
            let meta = decode_protobuf_meta(meta_bytes)?;

            let compact = build_compact_tx(vt, meta, reg)?;
            transactions.push(compact);
        }
    }

    Ok(BlockWithIds {
        slot,
        blockhash,
        previous_blockhash,
        parent_slot,
        block_time,
        block_height,
        rewards,
        num_transactions: transactions.len() as u64,
        transactions,
    })
}

fn build_compact_tx<R: RegistryAccess>(
    vt: VersionedTransaction,
    meta: confirmed_block::TransactionStatusMeta,
    reg: &mut R,
) -> Result<CompactTransaction> {
    let signatures: Vec<Signature> = vt
        .signatures
        .into_iter()
        .map(|s| Signature::from(s.0))
        .collect();

    let static_keys = vt.message.static_account_keys();
    let mut account_keys = Vec::with_capacity(static_keys.len());
    for raw in static_keys {
        account_keys.push(reg.get_or_insert(&Pubkey::new_from_array(*raw))?);
    }

    let header = vt.message.header();
    let message_header = MessageHeader {
        num_required_signatures: header.num_required_signatures,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
    };
    let recent_blockhash = "missing".to_string();

    let mut instructions = Vec::with_capacity(vt.message.instructions_len());
    for ix in vt.message.instructions_iter() {
        let pid = Pubkey::new_from_array(static_keys[ix.program_id_index as usize]);
        instructions.push(CompactInstruction {
            program_id: reg.get_or_insert(&pid)?,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
            stack_height: None,
        });
    }

    let address_table_lookups = vt.message.address_table_lookups().map(|lookups| {
        lookups
            .iter()
            .map(|l| {
                let id = reg
                    .get_or_insert(&Pubkey::new_from_array(l.account_key))
                    .unwrap_or(0);
                CompactAddressTableLookup {
                    account_key: id,
                    writable_indexes: l.writable_indexes.clone(),
                    readonly_indexes: l.readonly_indexes.clone(),
                }
            })
            .collect::<Vec<_>>()
    });

    let mut all_keys: Vec<Pubkey> = static_keys
        .iter()
        .map(|a| Pubkey::new_from_array(*a))
        .collect();
    for a in &meta.loaded_writable_addresses {
        if a.len() == 32 {
            if let Ok(arr) = <[u8; 32]>::try_from(a.as_slice()) {
                all_keys.push(Pubkey::new_from_array(arr));
            }
        }
    }
    for a in &meta.loaded_readonly_addresses {
        if a.len() == 32 {
            if let Ok(arr) = <[u8; 32]>::try_from(a.as_slice()) {
                all_keys.push(Pubkey::new_from_array(arr));
            }
        }
    }

    let compact_meta = make_compact_meta(meta, &all_keys, reg)?;

    let version = match vt.message {
        VersionedMessage::Legacy(_) => 0,
        VersionedMessage::V0(_) => 1,
    };

    Ok(CompactTransaction {
        signatures,
        message_header,
        account_keys,
        recent_blockhash,
        instructions,
        address_table_lookups,
        meta: Some(compact_meta),
        version,
    })
}
fn make_compact_meta<R: RegistryAccess>(
    meta: confirmed_block::TransactionStatusMeta,
    all_keys: &[Pubkey],
    reg: &mut R,
) -> Result<CompactTransactionMeta> {
    let err = meta.err.as_ref().map(|_| "TransactionError".to_string());

    let inner_instructions = if meta.inner_instructions_none || meta.inner_instructions.is_empty() {
        None
    } else {
        let mut out = Vec::with_capacity(meta.inner_instructions.len());
        for ui in meta.inner_instructions {
            if ui.instructions.is_empty() {
                continue;
            }
            let mut inst = Vec::with_capacity(ui.instructions.len());
            for i in ui.instructions {
                if let Some(pk) = all_keys.get(i.program_id_index as usize) {
                    inst.push(CompactInstruction {
                        program_id: reg.get_or_insert(pk).unwrap_or(0),
                        accounts: i.accounts,
                        data: i.data,
                        stack_height: None,
                    });
                }
            }
            if !inst.is_empty() {
                out.push(CompactInnerInstructions {
                    index: ui.index as u8,
                    instructions: inst,
                });
            }
        }
        if out.is_empty() { None } else { Some(out) }
    };

    let log_messages = if meta.log_messages_none || meta.log_messages.is_empty() {
        None
    } else {
        Some(meta.log_messages)
    };

    let pre_token_balances = if meta.pre_token_balances.is_empty() {
        None
    } else {
        let mut v = Vec::with_capacity(meta.pre_token_balances.len());
        for tb in meta.pre_token_balances {
            v.push(convert_token_balance(tb, reg)?);
        }
        Some(v)
    };

    let post_token_balances = if meta.post_token_balances.is_empty() {
        None
    } else {
        let mut v = Vec::with_capacity(meta.post_token_balances.len());
        for tb in meta.post_token_balances {
            v.push(convert_token_balance(tb, reg)?);
        }
        Some(v)
    };

    let loaded_addresses = {
        let w_len = meta.loaded_writable_addresses.len();
        let r_len = meta.loaded_readonly_addresses.len();
        if w_len == 0 && r_len == 0 {
            None
        } else {
            let mut writable = Vec::with_capacity(w_len);
            for b in meta.loaded_writable_addresses {
                if b.len() == 32 {
                    if let Ok(arr) = <[u8; 32]>::try_from(b.as_slice()) {
                        writable.push(reg.get_or_insert(&Pubkey::new_from_array(arr))?);
                    }
                }
            }
            let mut readonly = Vec::with_capacity(r_len);
            for b in meta.loaded_readonly_addresses {
                if b.len() == 32 {
                    if let Ok(arr) = <[u8; 32]>::try_from(b.as_slice()) {
                        readonly.push(reg.get_or_insert(&Pubkey::new_from_array(arr))?);
                    }
                }
            }
            Some(CompactLoadedAddresses { writable, readonly })
        }
    };

    let return_data = meta.return_data.and_then(|rd| {
        <[u8; 32]>::try_from(rd.program_id.as_slice())
            .ok()
            .map(|arr| Pubkey::new_from_array(arr))
            .and_then(|pk| reg.get_or_insert(&pk).ok().map(|id| (id, rd.data)))
    });

    Ok(CompactTransactionMeta {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions,
        log_messages,
        pre_token_balances,
        post_token_balances,
        rewards: None,
        loaded_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
    })
}

fn convert_token_balance<R: RegistryAccess>(
    tb: confirmed_block::TokenBalance,
    reg: &mut R,
) -> Result<CompactTokenBalance> {
    let mint = tb
        .mint
        .parse::<Pubkey>()
        .ok()
        .and_then(|pk| reg.get_or_insert(&pk).ok())
        .unwrap_or(0);
    let owner = if tb.owner.is_empty() {
        0
    } else {
        tb.owner
            .parse::<Pubkey>()
            .ok()
            .and_then(|pk| reg.get_or_insert(&pk).ok())
            .unwrap_or(0)
    };
    let program_id = if tb.program_id.is_empty() {
        0
    } else {
        tb.program_id
            .parse::<Pubkey>()
            .ok()
            .and_then(|pk| reg.get_or_insert(&pk).ok())
            .unwrap_or(0)
    };
    let ui_token_amount = tb
        .ui_token_amount
        .map(|a| {
            serde_json::json!({
                "amount": a.amount,
                "decimals": a.decimals,
                "uiAmount": a.ui_amount,
                "uiAmountString": a.ui_amount_string,
            })
            .to_string()
        })
        .unwrap_or_default();

    Ok(CompactTokenBalance {
        account_index: tb.account_index as u8,
        mint,
        ui_token_amount,
        owner,
        program_id,
    })
}

fn extract_rewards<R: RegistryAccess>(block: &CarBlock, reg: &mut R) -> Result<Vec<CompactReward>> {
    let blk = block.block()?;
    let Some(cid) = blk.rewards else {
        return Ok(Vec::new());
    };
    let node = block.decode(cid.hash_bytes())?;
    let Node::DataFrame(df_cbor) = node else {
        return Ok(Vec::new());
    };

    let mut buf = df_cbor.data.to_vec();
    if let Some(cid) = df_cbor.next {
        let mut rdr = block.dataframe_reader(&cid.to_cid()?);
        rdr.read_to_end(&mut buf)?;
    }

    #[derive(wincode::SchemaRead)]
    struct WReward {
        pubkey: String,
        lamports: i64,
        post_balance: u64,
        reward_type: Option<String>,
        commission: Option<u8>,
    }
    #[derive(wincode::SchemaRead)]
    struct WRewards(
        #[wincode(
            with = "wincode::containers::Vec<wincode::containers::Elem<WReward>, wincode::len::ShortU16Len>"
        )]
        Vec<WReward>,
    );

    let rewards: Vec<CompactReward> = (|| -> Result<Vec<CompactReward>> {
        let mut reader = wincode::io::Reader::new(&buf);
        let mut dst = std::mem::MaybeUninit::<WRewards>::uninit();
        <WRewards as wincode::SchemaRead>::read(&mut reader, &mut dst)
            .map_err(|_| anyhow!("wincode decode rewards failed"))?;
        let list = unsafe { dst.assume_init() }.0;
        let mut out = Vec::with_capacity(list.len());
        for r in list {
            if let Ok(pk) = r.pubkey.parse::<Pubkey>() {
                out.push(CompactReward {
                    pubkey: reg.get_or_insert(&pk)?,
                    lamports: r.lamports,
                    post_balance: r.post_balance,
                    reward_type: r.reward_type,
                    commission: r.commission,
                });
            }
        }
        Ok(out)
    })()
    .unwrap_or_default();

    Ok(rewards)
}

fn has_local_epoch(cache_dir: &str, epoch: u64) -> bool {
    Path::new(&format!("{cache_dir}/epoch-{epoch}.car")).exists()
}

async fn download_epoch_to_disk(cache_dir: &str, epoch: u64) -> Result<PathBuf> {
    tokio::fs::create_dir_all(cache_dir).await.ok();
    let out = PathBuf::from(format!("{cache_dir}/epoch-{epoch}.car"));
    if !out.exists() {
        crate::file_downloader::download_epoch(epoch, cache_dir, 3).await?;
    }
    Ok(out)
}

pub async fn run_car_optimizer(
    cache_dir: &str,
    epoch: u64,
    output_dir: &str,
    registry_dir: Option<&str>,
    zstd_level: i32,
) -> Result<()> {
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let out_dir = PathBuf::from(output_dir);
    std::fs::create_dir_all(&out_dir)?;
    let bin_path = out_dir.join(format!("epoch-{epoch:04}.bin"));
    let idx_path = out_dir.join(format!("epoch-{epoch:04}.idx"));

    let mut bin = BufWriter::with_capacity(
        32 * 1024 * 1024,
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&bin_path)?,
    );
    let mut idx = BufWriter::with_capacity(
        8 * 1024 * 1024,
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&idx_path)?,
    );

    let registry_base = registry_dir.unwrap_or(output_dir);
    let mut reg = OrderedPubkeyRegistry::load(registry_base, epoch)?;

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;
    let mut current_offset = 0u64;
    let mut total_bytes = 0u64;

    let mut ser_buf = Vec::with_capacity(2 * 1024 * 1024);

    while let Some(accessor) = car.next_block().await? {
        let t1 = Instant::now();
        let compact = cb_to_compact_block(&accessor, &mut reg)?;
        let serialized = postcard::to_allocvec(&compact)?;
        ser_buf.clear();
        zstd::stream::copy_encode(&serialized[..], &mut ser_buf, zstd_level)?;

        let t2 = Instant::now();

        let len = ser_buf.len() as u32;
        bin.write_all(&len.to_le_bytes())?;
        bin.write_all(&ser_buf)?;
        idx.write_all(&compact.slot.to_le_bytes())?;
        idx.write_all(&current_offset.to_le_bytes())?;

        current_offset += 4 + ser_buf.len() as u64;
        total_bytes += ser_buf.len() as u64;
        blocks += 1;

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            bin.flush()?;
            idx.flush()?;
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            let blkps = blocks as f64 / elapsed;
            println!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} keys | frame {:.1} ms",
                blocks,
                blkps,
                reg.len(),
                (t2 - t1).as_secs_f64() * 1000.0,
            );
            last_log = now;
        }
    }

    bin.flush()?;
    idx.flush()?;
    println!(
        "✅ Done: {blocks} blocks → {} | keys={} | {:.2} MB | {:.1}s",
        bin_path.display(),
        reg.len(),
        total_bytes as f64 / 1_000_000.0,
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
