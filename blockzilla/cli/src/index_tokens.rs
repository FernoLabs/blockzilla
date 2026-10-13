//! A small consumer: admitted compact blocks -> atomic SQLite rows/checkpoint.
use crate::stream::{self, EpochSourceArgs};
use anyhow::{Context, Result, bail, ensure};
use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSource, BlockSink, BlockView, CompactV2ParallelScanConfig, QueryError,
    QueryResult, ScanRequest,
};
use blockzilla_model::{ExecutionStatus, TokenBalanceCoverage};
use clap::Args;
use hivezilla_stream::frame;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    num::NonZeroU32,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    time::Duration,
};

const ORIGIN: &str = "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";
const APP_ID: i64 = 0x425a5449;

#[derive(Args, Debug)]
pub(crate) struct IndexArgs {
    /// SQLite file. A separate read-only connection can query it while indexing.
    #[arg(long)]
    pub db: PathBuf,
    /// Live relay base URL. HTTPS is required except on loopback.
    #[arg(
        long,
        conflicts_with = "epoch",
        required_unless_present = "epoch",
        requires = "api_key_file"
    )]
    pub url: Option<String>,
    /// File with the consumer API key. The key is never saved in SQLite.
    #[arg(long, requires = "url")]
    pub api_key_file: Option<PathBuf>,
    /// Read committed native compact chunks here when live replay has expired.
    /// This directory must be on the consumer's machine.
    #[arg(long, requires = "url")]
    pub native_history_dir: Option<PathBuf>,
    /// Read this historical epoch, then exit. Repeat the command to resume.
    #[arg(long)]
    pub epoch: Option<u64>,
    /// First slot to backfill before live, or first historical slot to read.
    #[arg(long)]
    pub from_slot: Option<u64>,
    /// Last historical slot to read, inclusive.
    #[arg(long, requires = "epoch")]
    pub to_slot: Option<u64>,
    /// Keep one mint. Unknown mint identities are retained as null.
    #[arg(long)]
    pub mint: Option<String>,
    /// Blockzilla archive origin for history and replay-window recovery.
    #[arg(long, env = "BLOCKZILLA_ORIGIN", default_value = ORIGIN)]
    pub origin: String,
    #[arg(long, default_value = ".blockzilla-cache")]
    pub cache_root: PathBuf,
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..=8))]
    pub threads: u16,
    /// Stop after at least N newly indexed blocks (live chunks stay atomic).
    #[arg(long)]
    pub max_blocks: Option<NonZeroU32>,
}

#[derive(Args, Debug)]
pub(crate) struct QueryArgs {
    #[arg(long)]
    pub db: PathBuf,
    /// Show progress, source records, and the last recovery job.
    #[arg(long)]
    pub status: bool,
    #[arg(long)]
    pub mint: Option<String>,
    #[arg(long)]
    pub owner: Option<String>,
    #[arg(long)]
    pub signature: Option<String>,
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub limit: u32,
}

#[derive(Default, Debug)]
struct Position {
    slot: Option<u64>,
    hash: Option<String>,
    cursor: Option<String>,
}

struct Db {
    connection: Connection,
    _lock: File,
    mint: Option<String>,
    from_slot: Option<u64>,
}

fn pubkey(text: &str) -> Result<()> {
    let bytes = bs58::decode(text)
        .into_vec()
        .context("Invalid public key")?;
    ensure!(
        bytes.len() == 32 && bs58::encode(bytes).into_string() == text,
        "Invalid public key"
    );
    Ok(())
}

impl Db {
    fn open(path: &Path, config: &Value, mint: Option<String>) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("indexer.lock"))?;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "Another indexer owns this database"
        );
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let id: i64 = connection.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        let tables: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            (id == 0 && version == 0 && tables == 0) || (id == APP_ID && version == 1),
            "This is not a supported Blockzilla token database"
        );
        connection.execute_batch(include_str!("index_tokens_schema.sql"))?;
        let previous: Option<String> = connection
            .query_row("SELECT value FROM settings WHERE key='config'", [], |r| {
                r.get(0)
            })
            .optional()?;
        let encoded = config.to_string();
        ensure!(
            previous.as_ref().is_none_or(|p| p == &encoded),
            "Index source or filter changed; use another database"
        );
        connection.execute(
            "INSERT OR IGNORE INTO settings VALUES ('config',?1)",
            [&encoded],
        )?;
        Ok(Self {
            connection,
            _lock: lock,
            mint,
            from_slot: config["fromSlot"].as_u64(),
        })
    }
    fn position(&self) -> Result<Position> {
        Ok(self.connection.query_row(
            "SELECT last_slot,last_blockhash,live_cursor FROM checkpoint WHERE id=1",
            [],
            |r| {
                Ok(Position {
                    slot: r.get(0)?,
                    hash: r.get(1)?,
                    cursor: r.get(2)?,
                })
            },
        )?)
    }
    fn count(&self) -> Result<u64> {
        Ok(self
            .connection
            .query_row("SELECT count(*) FROM blocks", [], |r| r.get(0))?)
    }
    fn anchor(&self, cursor: &str, initial: bool) -> Result<()> {
        ensure!(
            !frame::processed_cursor(cursor),
            "The token index requires finalized history; processed fork undo is not implemented"
        );
        let (_, slot, _) = frame::parse_cursor(cursor)?;
        if initial {
            self.connection.execute("UPDATE checkpoint SET live_cursor=?1,last_slot=?2 WHERE id=1 AND last_slot IS NULL", params![cursor,slot])?;
        } else {
            self.connection
                .execute("UPDATE checkpoint SET live_cursor=?1 WHERE id=1", [cursor])?;
        }
        Ok(())
    }
    fn job(&self, from: u64, to: u64, status: &str, error: Option<&str>) -> Result<()> {
        self.connection.execute("INSERT INTO recovery(id,from_slot,to_slot,status,error) VALUES(1,?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET from_slot=excluded.from_slot,to_slot=excluded.to_slot,status=excluded.status,error=excluded.error", params![from,to,status,error])?;
        Ok(())
    }
    fn apply(&mut self, blocks: &[Value], source: &Value, cursor: Option<&str>) -> Result<u64> {
        let source_text = source.to_string();
        let source_id = frame::digest(source_text.as_bytes());
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO sources VALUES(?1,?2)",
            params![source_id, source_text],
        )?;
        let (mut last, mut hash): (Option<u64>, Option<String>) = transaction.query_row(
            "SELECT last_slot,last_blockhash FROM checkpoint WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let first: Option<u64> =
            transaction.query_row("SELECT min(slot) FROM blocks", [], |r| r.get(0))?;
        let scope_start = first.or_else(|| last.and_then(|s| s.checked_add(1)));
        let mut added = 0;
        for block in blocks {
            let slot = number(block, "slot")?;
            if self.from_slot.is_some_and(|from| slot < from) {
                continue;
            }
            let blockhash = block["blockhash"].as_str();
            let parent = block["parentSlot"].as_u64();
            let digest = content_digest(block)?;
            let existing: Option<(Option<String>, String)> = transaction
                .query_row(
                    "SELECT blockhash,content_digest FROM blocks WHERE slot=?1",
                    [slot],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((old_hash, old_digest)) = existing {
                ensure!(
                    old_digest == digest && old_hash.as_deref() == blockhash,
                    "Conflicting repeated block at slot {slot}"
                );
                continue;
            }
            // On a replay-window reset, retained blocks can precede the
            // explicitly selected start of this database. Do not backdate it.
            if scope_start.is_some_and(|first| slot < first) {
                continue;
            }
            ensure!(
                last.is_none_or(|p| slot > p),
                "Unindexed block behind the checkpoint at slot {slot}"
            );
            if let (None, Some(from), Some(parent)) = (last, self.from_slot, parent) {
                ensure!(
                    parent < from,
                    "The requested history prefix is missing: required parent slot {parent}"
                );
            }
            if let (Some(p), Some(previous)) = (parent, last) {
                ensure!(
                    p == previous,
                    "Missing parent block: indexed {previous}, required {p}"
                );
                if let Some(h) = &hash {
                    ensure!(
                        block["parentBlockhash"] == *h,
                        "Parent blockhash changed at slot {slot}"
                    );
                }
            }
            let txs = block["transactions"]
                .as_array()
                .context("Missing transactions")?;
            ensure!(
                txs.len() as u64 == number(block, "transactionCount")?,
                "Transaction count changed at slot {slot}"
            );
            transaction.execute(
                "INSERT INTO blocks VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    slot,
                    blockhash,
                    parent,
                    number(block, "epoch")?,
                    number(block, "transactionCount")?,
                    source_id,
                    digest
                ],
            )?;
            for tx in txs {
                let index = number(tx, "index")?;
                let succeeded = tx["success"]
                    .as_bool()
                    .context("Execution result is missing")?;
                let signature = tx["signature"]
                    .as_str()
                    .context("Primary signature is missing")?;
                transaction.execute(
                    "INSERT INTO transactions VALUES(?1,?2,?3,?4)",
                    params![slot, index, signature, succeeded],
                )?;
                for balance in tx["tokenBalances"]
                    .as_array()
                    .context("Token balances are missing")?
                {
                    let mint = balance["mint"].as_str();
                    if self
                        .mint
                        .as_deref()
                        .is_some_and(|wanted| mint.is_some_and(|m| m != wanted))
                    {
                        continue;
                    }
                    let amount = balance["amount"]
                        .as_str()
                        .context("Amount must be an exact decimal string")?;
                    let raw: u64 = amount.parse()?;
                    ensure!(raw.to_string() == amount, "Invalid raw token amount");
                    let side = balance["side"].as_str().context("Missing balance side")?;
                    ensure!(matches!(side, "pre" | "post"), "Invalid balance side");
                    transaction.execute(
                        "INSERT INTO token_balances VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                        params![
                            slot,
                            index,
                            side,
                            number(balance, "balanceIndex")?,
                            number(balance, "accountIndex")?,
                            balance["account"].as_str(),
                            mint,
                            balance["owner"].as_str(),
                            balance["tokenProgram"].as_str(),
                            amount,
                            number(balance, "decimals")?
                        ],
                    )?;
                }
            }
            last = Some(slot);
            hash = blockhash.map(str::to_owned);
            added += 1;
        }
        transaction.execute("UPDATE checkpoint SET last_slot=?1,last_blockhash=?2,live_cursor=coalesce(?3,live_cursor) WHERE id=1",params![last,hash,cursor])?;
        transaction.commit()?;
        Ok(added)
    }
}

fn number(v: &Value, key: &str) -> Result<u64> {
    v[key]
        .as_u64()
        .with_context(|| format!("Missing integer {key}"))
}
fn content_digest(block: &Value) -> Result<String> {
    // Historical projection does not expose resolved account keys. Compare
    // the shared receipt/balance semantics so archive/live overlap can dedupe.
    let mut transactions = block["transactions"]
        .as_array()
        .context("Missing transactions")?
        .clone();
    for tx in &mut transactions {
        for balance in tx["tokenBalances"]
            .as_array_mut()
            .context("Missing token balances")?
        {
            balance
                .as_object_mut()
                .context("Invalid token balance")?
                .remove("account");
        }
    }
    let content = json!({"slot":block["slot"],"epoch":block["epoch"],"blockhash":block["blockhash"],
        "parentSlot":block["parentSlot"],"parentBlockhash":block["parentBlockhash"],
        "transactionCount":block["transactionCount"],"transactions":transactions});
    Ok(frame::digest(content.to_string().as_bytes()))
}
fn native_blocks(chunk: &frame::Chunk) -> Result<Vec<Value>> {
    let data = blockzilla_stream_decoder::decode_token_balances(&chunk.wire)?;
    let decoded: Value = serde_json::from_slice(&data)?;
    let mut blocks = decoded["blocks"]
        .as_array()
        .context("Missing decoded blocks")?
        .clone();
    for block in &mut blocks {
        block["epoch"] = decoded["epoch"].clone();
    }
    Ok(blocks)
}

struct HistorySink<'a> {
    db: &'a mut Db,
    source: Value,
    added: u64,
    first_row: u32,
    identities: Vec<(u64, [u8; 32], Option<[u8; 32]>)>,
}
impl BlockSink for HistorySink<'_> {
    fn visit_block(&mut self, block: BlockView<'_>) -> QueryResult<()> {
        let result = (|| -> Result<()> {
            let mut txs = Vec::new();
            for tx in block.transaction_views() {
                ensure!(
                    tx.token_balance_coverage == TokenBalanceCoverage::Complete,
                    "History token data is incomplete at slot {}",
                    block.header.slot
                );
                let success = match tx.header.status {
                    ExecutionStatus::Succeeded => true,
                    ExecutionStatus::Failed => false,
                    _ => bail!("History execution result is unavailable"),
                };
                let signature = tx
                    .primary_signature
                    .context("History primary signature is unavailable")?;
                let balances = tx.token_balances.iter().map(|b| json!({"side":b.side,"balanceIndex":b.balance_index,"accountIndex":b.account_index,
                    "account":null,"mint":b.mint.map(|k| bs58::encode(k).into_string()),"owner":b.owner.map(|k| bs58::encode(k).into_string()),
                    "tokenProgram":b.token_program.map(|k| bs58::encode(k).into_string()),"amount":b.amount.to_string(),"decimals":b.decimals})).collect::<Vec<_>>();
                txs.push(json!({"index":tx.header.tx_index,"signature":bs58::encode(signature).into_string(),"success":success,"tokenBalances":balances}));
            }
            let (parent, hash, previous) = self
                .identities
                .get((block.header.block_ordinal - self.first_row) as usize)
                .context("History row identity is absent")?;
            let record = json!({"slot":block.header.slot,"epoch":block.header.epoch,"blockhash":bs58::encode(hash).into_string(),"parentSlot":parent,"parentBlockhash":previous.map(|h|bs58::encode(h).into_string()),
                "transactionCount":txs.len(),"transactions":txs});
            self.added += self.db.apply(&[record], &self.source, None)?;
            Ok(())
        })();
        result.map_err(|e| QueryError::sink(std::io::Error::other(e.to_string())))
    }
}

fn history(
    db: &mut Db,
    args: &IndexArgs,
    from: u64,
    to: u64,
    limit: Option<NonZeroU32>,
    require_end: bool,
) -> Result<u64> {
    ensure!(from <= to, "Invalid history range");
    db.job(from, to, "BACKFILLING", None)?;
    let result = (|| -> Result<u64> {
        let mut added = 0;
        for epoch in from / 432000..=to / 432000 {
            let source = EpochSourceArgs {
                epoch,
                origin: Some(args.origin.clone()),
                cache_root: args.cache_root.clone(),
                archive_dir: None,
                source_id: None,
                threads: args.threads,
                max_blocks: None,
            };
            let mut archive = stream::open(&source).with_context(|| format!("Required Blockzilla history is unavailable for epoch {epoch}; database progress was retained"))?;
            archive.set_full_registry_limit(256 << 20);
            let slots = archive.canonical_slots();
            let start = slots.partition_point(|s| *s < from);
            let mut end = slots.partition_point(|s| *s <= to);
            if let Some(cap) = limit {
                end = end.min(start + cap.get().saturating_sub(added as u32) as usize);
            }
            if start >= end {
                continue;
            }
            let source = json!({"kind":"blockzilla-history","identity":archive.identity(),"hashContinuity":"checked-parent-links","accountKeys":"unavailable"});
            // Keep identity admission bounded, then let the common SDK decode
            // token rows in order. No unbounded epoch of decoded blocks is held.
            for first in (start..end).step_by(32) {
                let last = (first + 32).min(end);
                let identities = (first..last)
                    .map(|row| archive.block_identity(row))
                    .collect::<QueryResult<Vec<_>>>()?;
                let range = archive.bounded_range(
                    first as u32,
                    NonZeroU32::new((last - first) as u32).unwrap(),
                )?;
                let request = ScanRequest::bounded(range)
                    .without_instructions()
                    .without_required_signers()
                    .with_token_balances();
                let mut sink = HistorySink {
                    db,
                    source: source.clone(),
                    added: 0,
                    first_row: first as u32,
                    identities,
                };
                archive.scan_ordered_parallel(
                    &request,
                    &mut sink,
                    CompactV2ParallelScanConfig::new(args.threads as usize),
                )?;
                added += sink.added;
                archive.verify_local_unchanged()?;
            }
            if limit.is_some_and(|n| added >= n.get() as u64) {
                break;
            }
        }
        if require_end {
            ensure!(
                db.position()?.slot == Some(to),
                "History does not contain required boundary slot {to}; live cursor was not advanced"
            );
        }
        Ok(added)
    })();
    match &result {
        Ok(added) => {
            let state = if require_end {
                "COMPLETE"
            } else if limit.is_some_and(|n| *added >= n.get() as u64) {
                "LIMIT_REACHED"
            } else {
                "SCAN_COMPLETE"
            };
            db.job(from, to, state, None)?;
        }
        Err(e) => db.job(from, to, "BLOCKED", Some(&e.to_string()))?,
    }
    result
}

fn client(url: &str) -> Result<reqwest::blocking::Client> {
    let base = reqwest::Url::parse(url)?;
    ensure!(
        base.path() == "/"
            && base.query().is_none()
            && base.fragment().is_none()
            && base.username().is_empty()
            && base.password().is_none(),
        "Use a relay base URL without credentials or query parameters"
    );
    let loopback = base.host_str().is_some_and(|h| {
        h == "localhost"
            || h.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    ensure!(
        base.scheme() == "https" || (base.scheme() == "http" && loopback),
        "HTTPS is required outside loopback"
    );
    Ok(reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(45))
        .build()?)
}
fn head(client: &reqwest::blocking::Client, url: &str, token: &str) -> Result<Value> {
    let response = client
        .get(format!("{url}v1/head"))
        .bearer_auth(token)
        .send()?
        .error_for_status()?;
    let mut data = Vec::new();
    response.take(65537).read_to_end(&mut data)?;
    ensure!(data.len() <= 65536, "Stream head is too large");
    let value: Value = serde_json::from_slice(&data)?;
    ensure!(
        value["schema"] == "blockzilla-stream-head-v1" && value["state"] != "FAILED",
        "Live source is unavailable"
    );
    require_finalized_head(&value)?;
    Ok(value)
}
fn require_finalized_head(value: &Value) -> Result<()> {
    let cursor = value["latest_cursor"]
        .as_str()
        .context("Missing live cursor")?;
    ensure!(
        value["profile"] != "native-processed-compact-v1" && !frame::processed_cursor(cursor),
        "The token index requires finalized history; processed fork undo is not implemented"
    );
    Ok(())
}
fn read_frame(input: &mut impl Read) -> Result<frame::Chunk> {
    let mut header = [0; 16];
    input.read_exact(&mut header)?;
    let len = frame::frame_length(&header)?;
    let mut wire = vec![0; len];
    wire[..16].copy_from_slice(&header);
    input.read_exact(&mut wire[16..])?;
    frame::decode(wire.into())
}
fn recover(db: &mut Db, args: &IndexArgs, h: &Value) -> Result<()> {
    // An event position must never become a historical slot or a saved boundary.
    require_finalized_head(h)?;
    // Recover to the current head, not the eviction edge. That cursor stays
    // retained while backfill runs; the oldest edge can move on every block.
    let after = h["latest_cursor"]
        .as_str()
        .context("Missing replay boundary")?;
    let (_, boundary, _) = frame::parse_cursor(after)?;
    let position = db.position()?;
    if let Some(last) = position.slot {
        if last < boundary {
            backfill(
                db,
                args,
                last.checked_add(1).context("Slot overflow")?,
                boundary,
            )?;
        }
    } else if let Some(from) = args.from_slot {
        if from <= boundary {
            backfill(db, args, from, boundary)?;
        }
    }
    db.anchor(after, false)
}

fn backfill(db: &mut Db, args: &IndexArgs, from: u64, to: u64) -> Result<()> {
    let Some(root) = &args.native_history_dir else {
        return history(db, args, from, to, None, true).map(|_| ());
    };
    db.job(from, to, "BACKFILLING", None)?;
    let result = (|| -> Result<()> {
        let mut source = hivezilla_stream::source::Source::open(root, from)?;
        while db.position()?.slot.is_none_or(|last| last < to) {
            let chunk = source
                .next_chunk()?
                .context("Required committed native history is unavailable")?;
            ensure!(
                chunk.envelope.marker["genesis_hash"]
                    == "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
                "Native history is not mainnet-beta"
            );
            let identity = json!({"kind":"native-compact-history","genesisHash":chunk.envelope.marker["genesis_hash"],
                "nodeIdentity":chunk.envelope.marker["node_identity"],"session":chunk.session,
                "agaveRevision":chunk.envelope.marker["agave_revision"],"canonical":false,"publicationReady":false});
            let blocks = native_blocks(&chunk)?
                .into_iter()
                .filter(|b| b["slot"].as_u64().is_some_and(|s| s <= to))
                .collect::<Vec<_>>();
            ensure!(
                !blocks.is_empty(),
                "Native history does not contain required boundary slot {to}"
            );
            db.apply(&blocks, &identity, None)?;
        }
        ensure!(
            db.position()?.slot == Some(to),
            "Native history did not reach the required replay boundary"
        );
        Ok(())
    })();
    match &result {
        Ok(()) => db.job(from, to, "COMPLETE", None)?,
        Err(e) => db.job(from, to, "BLOCKED", Some(&e.to_string()))?,
    }
    result
}

pub(crate) fn run(args: IndexArgs) -> Result<()> {
    if let Some(m) = &args.mint {
        pubkey(m)?;
    }
    let config = json!({"schema":"blockzilla-token-index-config-v1","url":args.url,"epoch":args.epoch,"mint":args.mint,"origin":args.origin,"fromSlot":args.from_slot,"toSlot":args.to_slot,"nativeHistoryDir":args.native_history_dir});
    let mut db = Db::open(&args.db, &config, args.mint.clone())?;
    let before = db.count()?;
    if let Some(epoch) = args.epoch {
        let start = epoch.checked_mul(432000).context("Epoch overflow")?;
        let end = start.checked_add(431999).context("Epoch overflow")?;
        let from = args
            .from_slot
            .unwrap_or(start)
            .max(db.position()?.slot.map_or(start, |s| s + 1));
        let to = args.to_slot.unwrap_or(end);
        ensure!(
            from >= start && to <= end,
            "Historical range must stay inside the selected epoch"
        );
        if from <= to {
            history(&mut db, &args, from, to, args.max_blocks, false)?;
        }
        eprintln!(
            "Indexed {} new historical blocks into {}",
            db.count()? - before,
            args.db.display()
        );
        return Ok(());
    }
    let url = format!("{}/", args.url.as_ref().unwrap().trim_end_matches('/'));
    let token = fs::read_to_string(args.api_key_file.as_ref().unwrap())?;
    let token = token.trim();
    ensure!(
        !token.is_empty() && token.len() <= 4096,
        "Invalid API key file"
    );
    let client = client(&url)?;
    let initial_head = head(&client, &url, token)?;
    if db.position()?.cursor.is_none() {
        let h = initial_head;
        if args.from_slot.is_some() || db.position()?.slot.is_some() {
            recover(&mut db, &args, &h)?;
        } else {
            db.anchor(
                h["latest_cursor"].as_str().context("Missing live cursor")?,
                true,
            )?;
        }
    }
    let mut delay = 1;
    loop {
        let result = (|| -> Result<()> {
            let mut after = db.position()?.cursor.context("Missing saved live cursor")?;
            let mut endpoint = reqwest::Url::parse(&format!("{url}v1/blocks"))?;
            endpoint.query_pairs_mut().append_pair("after", &after);
            let response = client.get(endpoint).bearer_auth(token).send()?;
            if matches!(response.status().as_u16(), 400 | 410) {
                let h = head(&client, &url, token)?;
                recover(&mut db, &args, &h)?;
                return Ok(());
            }
            let mut response = response.error_for_status()?;
            ensure!(
                response
                    .headers()
                    .get("x-blockzilla-start-cursor")
                    .and_then(|v| v.to_str().ok())
                    == Some(&after),
                "Relay returned another start cursor"
            );
            loop {
                let chunk = read_frame(&mut response)?;
                let (session, slot, _) = frame::parse_cursor(&after)?;
                ensure!(
                    chunk.session == session && chunk.last_slot > slot,
                    "Stream session or order changed"
                );
                ensure!(
                    chunk.envelope.marker["genesis_hash"]
                        == "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
                    "This indexer currently supports mainnet-beta only"
                );
                let source = json!({"kind":"native-compact-stream","genesisHash":chunk.envelope.marker["genesis_hash"],"nodeIdentity":chunk.envelope.marker["node_identity"],"session":chunk.session,"agaveRevision":chunk.envelope.marker["agave_revision"],"canonical":false,"publicationReady":false});
                let blocks = native_blocks(&chunk)?;
                let added = db.apply(&blocks, &source, Some(&chunk.cursor))?;
                after = chunk.cursor.clone();
                delay = 1;
                println!(
                    "{}",
                    json!({"state":"LIVE","newBlocks":added,"indexedBlocks":db.count()?,"slot":chunk.last_slot,"blockhash":db.position()?.hash})
                );
                if args.max_blocks.is_some_and(|n| {
                    db.count()
                        .is_ok_and(|count| count - before >= n.get() as u64)
                }) {
                    return Ok(());
                }
            }
        })();
        // A test limit cannot turn a partially completed recovery into success.
        if result.is_ok()
            && args.max_blocks.is_some_and(|n| {
                db.count()
                    .is_ok_and(|count| count - before >= n.get() as u64)
            })
        {
            return Ok(());
        }
        if let Err(error) = result {
            let transient = error.chain().any(|e| {
                e.downcast_ref::<reqwest::Error>().is_some_and(|e| {
                    e.is_timeout()
                        || e.is_connect()
                        || e.is_body()
                        || e.status().is_some_and(|s| s.is_server_error())
                }) || e.downcast_ref::<std::io::Error>().is_some_and(|e| {
                    matches!(
                        e.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::WouldBlock
                    )
                })
            });
            if !transient {
                return Err(error);
            }
            eprintln!("Stream interrupted; retrying from the saved SQLite checkpoint in {delay}s");
            std::thread::sleep(Duration::from_secs(delay));
            delay = (delay * 2).min(30);
        }
    }
}

pub(crate) fn query(args: QueryArgs) -> Result<()> {
    let db = Connection::open_with_flags(&args.db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_secs(5))?;
    let id: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    ensure!(id == APP_ID, "This is not a Blockzilla token database");
    if args.status {
        let progress:Value=db.query_row("SELECT last_slot,last_blockhash,live_cursor FROM checkpoint WHERE id=1",[],|r|Ok(json!({"slot":r.get::<_,Option<u64>>(0)?,"blockhash":r.get::<_,Option<String>>(1)?,"streamCursor":r.get::<_,Option<String>>(2)?})))?;
        let count = |table: &str| -> Result<u64> {
            Ok(db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
        };
        let recovery:Option<Value>=db.query_row("SELECT from_slot,to_slot,status,error FROM recovery WHERE id=1",[],|r|Ok(json!({"fromSlot":r.get::<_,u64>(0)?,"toSlot":r.get::<_,u64>(1)?,"state":r.get::<_,String>(2)?,"error":r.get::<_,Option<String>>(3)?}))).optional()?;
        let sources = db
            .prepare("SELECT identity FROM sources ORDER BY source_id")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let sources = sources
            .iter()
            .map(|s| serde_json::from_str::<Value>(s))
            .collect::<serde_json::Result<Vec<_>>>()?;
        println!(
            "{}",
            json!({"checkpoint":progress,"blocks":count("blocks")?,"transactions":count("transactions")?,"balances":count("token_balances")?,"recovery":recovery,"sources":sources})
        );
    } else {
        let mut stmt=db.prepare("SELECT b.slot,t.signature,t.success,b.side,b.balance_index,b.account_index,b.account,b.mint,b.owner,b.token_program,b.amount,b.decimals FROM token_balances b JOIN transactions t USING(slot,tx_index) WHERE (?1 IS NULL OR b.mint=?1) AND (?2 IS NULL OR b.owner=?2) AND (?3 IS NULL OR t.signature=?3) ORDER BY b.slot DESC,b.tx_index DESC,b.side,b.balance_index LIMIT ?4")?;
        let rows=stmt.query_map(params![args.mint,args.owner,args.signature,args.limit],|r|Ok(json!({"slot":r.get::<_,u64>(0)?,"signature":r.get::<_,String>(1)?,"success":r.get::<_,bool>(2)?,"side":r.get::<_,String>(3)?,"balanceIndex":r.get::<_,u32>(4)?,"accountIndex":r.get::<_,u32>(5)?,"account":r.get::<_,Option<String>>(6)?,"mint":r.get::<_,Option<String>>(7)?,"owner":r.get::<_,Option<String>>(8)?,"tokenProgram":r.get::<_,Option<String>>(9)?,"amount":r.get::<_,String>(10)?,"decimals":r.get::<_,u8>(11)?})))?;
        for row in rows {
            println!("{}", row?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
