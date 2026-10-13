use crate::{
    event::{Event, REVISION, digest},
    journal::{atomic_json, open_reader},
};
use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::*;
use blockzilla_compact::{CompactBlockHeader, CompactPohEntry};
use blockzilla_primitives::WincodeLeb128FramedWriter;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufWriter, Read, Write},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BlockRow {
    pub block_id: u32,
    pub slot: u64,
    pub bank_id: u64,
    pub blockhash: [u8; 32],
    pub parent_slot: u64,
    pub parent_blockhash: [u8; 32],
    pub transactions: u64,
    pub signatures: u64,
    pub entries: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct FileRow {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub schema: String,
    pub canonical: bool,
    pub finalized: bool,
    pub source_profile: String,
    pub session_id: String,
    pub agave_revision: String,
    pub genesis_hash: String,
    pub node_identity: String,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot_exclusive: u64,
    pub blocks: Vec<BlockRow>,
    pub files: Vec<FileRow>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Evidence {
    pub sequence: u64,
    pub sha256: String,
    pub event: Event,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RawEvidence {
    pub sequence: u64,
    pub sha256: String,
    pub event: Box<serde_json::value::RawValue>,
}
#[derive(Deserialize)]
struct BuildEvent {
    kind: String,
    slot: u64,
    bank_id: Option<u64>,
    index: Option<u64>,
    projection: Option<WincodeArchiveV2NoRegistryTransaction>,
    num_hashes: Option<u64>,
    hash: Option<[u8; 32]>,
    transactions: Option<u64>,
    starting_transaction_index: Option<u64>,
    parent_slot: Option<u64>,
    parent_blockhash: Option<[u8; 32]>,
    blockhash: Option<[u8; 32]>,
    entries: Option<u64>,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Option<WincodeArchiveV2NoRegistryRewards>,
    parent: Option<u64>,
    status: Option<String>,
    dead_error: Option<String>,
}
// Parse only the compact projection and completeness fields. RawValue retains all
// wire bytes and native metadata without allocating or cloning a second JSON tree.
fn projection_event(raw: &str) -> Result<Event> {
    let e: BuildEvent = serde_json::from_str(raw)?;
    let slot = e.slot;
    Ok(match e.kind.as_str() {
        "Transaction" => Event::Transaction {
            slot,
            bank_id: e.bank_id.context("missing bank")?,
            index: e.index.context("missing index")?,
            is_vote: false,
            message_hash: vec![],
            wire_transaction: vec![],
            native_meta: serde_json::Value::Null,
            projection: e.projection.context("missing projection")?,
        },
        "Entry" => Event::Entry {
            slot,
            bank_id: e.bank_id.context("missing bank")?,
            index: e.index.context("missing index")?,
            num_hashes: e.num_hashes.context("missing hash count")?,
            hash: e.hash.context("missing hash")?,
            transactions: e.transactions.context("missing transaction count")?,
            starting_transaction_index: e
                .starting_transaction_index
                .context("missing starting index")?,
        },
        "Block" => Event::Block {
            slot,
            bank_id: e.bank_id.context("missing bank")?,
            parent_slot: e.parent_slot.context("missing parent")?,
            parent_blockhash: e.parent_blockhash.context("missing parent hash")?,
            blockhash: e.blockhash.context("missing block hash")?,
            transactions: e.transactions.context("missing transaction count")?,
            entries: e.entries.context("missing entry count")?,
            block_time: e.block_time,
            block_height: e.block_height,
            native_rewards: serde_json::Value::Null,
            rewards: e.rewards.context("missing rewards")?,
        },
        "Status" => Event::Status {
            slot,
            bank_id: e.bank_id,
            parent: e.parent,
            status: e.status.context("missing status")?,
            dead_error: e.dead_error,
        },
        "ClearBank" => Event::ClearBank {
            slot,
            bank_id: e.bank_id.context("missing bank")?,
            parent_slot: e.parent_slot.context("missing parent")?,
            parent_blockhash: e.parent_blockhash.context("missing parent hash")?,
        },
        "Footer" => Event::Footer {
            slot,
            bank_id: e.bank_id.context("missing bank")?,
            wire_footer: vec![],
        },
        "BankStateV2" => serde_json::from_str(raw)?,
        "BankStatusV2" => {
            let native: Event = serde_json::from_str(raw)?;
            let Event::BankStatusV2 {
                schema,
                slot,
                bank_id,
                parent,
                parent_blockhash,
                status,
                dead_error,
                ..
            } = native
            else {
                unreachable!()
            };
            ensure!(schema == crate::event::BANK_V2, "unsupported bank callback");
            if status == "clear" {
                Event::ClearBank {
                    slot,
                    bank_id: bank_id.context("missing cleared bank")?,
                    parent_slot: parent.context("missing parent")?,
                    parent_blockhash: parent_blockhash.context("missing parent hash")?,
                }
            } else {
                Event::Status {
                    slot,
                    bank_id,
                    parent,
                    status,
                    dead_error,
                }
            }
        }
        _ => bail!("unknown source event kind"),
    })
}
pub struct CompleteBank {
    pub source_bytes: usize,
    pub row: BlockRow,
    pub block: WincodeArchiveV2NoRegistryBlock,
    pub poh: Vec<CompactPohEntry>,
    pub signatures: Vec<u8>,
    pub evidence: Vec<RawEvidence>,
}

pub fn assemble(slot: u64, bank: u64, events: Vec<Evidence>) -> Result<Option<CompleteBank>> {
    let raw = events
        .into_iter()
        .map(|e| -> Result<_> {
            Ok(RawEvidence {
                sequence: e.sequence,
                sha256: e.sha256,
                event: serde_json::value::RawValue::from_string(serde_json::to_string(&e.event)?)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    assemble_raw(slot, bank, raw)
}
fn assemble_raw(slot: u64, bank: u64, events: Vec<RawEvidence>) -> Result<Option<CompleteBank>> {
    assemble_bank(slot, bank, events, true)
}

pub(crate) fn assemble_bank(
    slot: u64,
    bank: u64,
    events: Vec<RawEvidence>,
    require_root: bool,
) -> Result<Option<CompleteBank>> {
    let source_bytes = events.iter().map(|e| e.event.get().len() + 128).sum();
    let mut txs = BTreeMap::new();
    let mut entries = BTreeMap::new();
    let mut metadata = None;
    let mut root = false;
    let mut processed = false;
    let mut invalidated = false;
    let mut seen = BTreeMap::<String, String>::new();
    for e in &events {
        let event = projection_event(e.event.get())?;
        let (s, b, kind) = event.key();
        ensure!(
            s == slot && (b == Some(bank) || b.is_none()),
            "mixed bank source records"
        );
        invalidated |= matches!(kind, "dead" | "clear");
        let key = match &event {
            Event::Transaction { index, .. } => Some(format!("tx{index}")),
            Event::Entry { index, .. } => Some(format!("entry{index}")),
            Event::Block { .. } => Some("block".into()),
            _ => None,
        };
        if let Some(key) = key {
            if let Some(old) = seen.insert(key, e.sha256.clone()) {
                ensure!(old == e.sha256, "conflicting duplicate callback");
            }
        }
        match event {
            Event::Transaction {
                index, projection, ..
            } => {
                ensure!(
                    index == u64::from(projection.tx_index),
                    "transaction index differs from projection"
                );
                txs.insert(index, projection);
            }
            Event::Entry {
                index,
                num_hashes,
                hash,
                transactions,
                starting_transaction_index,
                ..
            } => {
                entries.insert(
                    index,
                    (num_hashes, hash, transactions, starting_transaction_index),
                );
            }
            Event::Block { .. } => metadata = Some(event),
            Event::Status {
                status, bank_id, ..
            } if status == "rooted" => {
                ensure!(bank_id == Some(bank), "root has no bank ID");
                root = true;
            }
            Event::Status {
                status, bank_id, ..
            } if status == "processed" => {
                ensure!(bank_id == Some(bank), "processed status has no bank ID");
                processed = true;
            }
            _ => {}
        }
    }
    let Some(Event::Block {
        parent_slot,
        parent_blockhash,
        blockhash,
        transactions,
        entries: entry_count,
        block_time,
        block_height,
        rewards,
        ..
    }) = metadata
    else {
        return Ok(None);
    };
    ensure!(
        !(invalidated && (root || require_root)),
        "rooted bank has invalidation evidence"
    );
    if invalidated
        || (require_root && !root)
        || (!require_root && !(processed || root))
        || txs.len() as u64 != transactions
        || entries.len() as u64 != entry_count
    {
        return Ok(None);
    }
    ensure!(
        entry_count > 0 && parent_slot < slot,
        "invalid block geometry"
    );
    let mut signatures = Vec::new();
    let mut sig_counts = Vec::new();
    for (expected, (index, tx)) in txs.iter().enumerate() {
        ensure!(*index == expected as u64, "non-dense transaction indexes");
        let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
            bail!("transaction projection is not decoded")
        };
        ensure!(
            !value.signatures.is_empty(),
            "transaction has no signatures"
        );
        for sig in &value.signatures {
            ensure!(sig.len() == 64, "bad signature length");
            signatures.extend(sig);
        }
        sig_counts.push(value.signatures.len() as u32);
    }
    let mut consumed = 0usize;
    let mut poh = Vec::new();
    for (expected, (index, (num_hashes, hash, count, start))) in entries.into_iter().enumerate() {
        ensure!(
            index == expected as u64 && start == consumed as u64,
            "non-dense entry indexes or transaction ranges"
        );
        let end = consumed
            .checked_add(usize::try_from(count)?)
            .context("entry count overflow")?;
        ensure!(end <= sig_counts.len(), "entry range exceeds transactions");
        let sigs = sig_counts[consumed..end]
            .iter()
            .try_fold(0u32, |a, b| a.checked_add(*b).context("signature overflow"))?;
        poh.push(CompactPohEntry {
            num_hashes,
            hash,
            tx_count: u32::try_from(count)?,
            signature_count: sigs,
        });
        consumed = end;
    }
    ensure!(
        consumed == txs.len() && poh.last().context("no entries")?.hash == blockhash,
        "entry totals or final hash mismatch"
    );
    let row = BlockRow {
        block_id: 0,
        slot,
        bank_id: bank,
        blockhash,
        parent_slot,
        parent_blockhash,
        transactions,
        signatures: signatures.len() as u64 / 64,
        entries: entry_count,
    };
    let block = WincodeArchiveV2NoRegistryBlock {
        header: WincodeArchiveV2NoRegistryBlockHeader {
            compact: CompactBlockHeader {
                slot,
                parent_slot,
                blockhash: 0,
                previous_blockhash: 0,
                block_time,
                block_height,
                shredding: vec![],
                poh_entries: vec![],
                rewards: None,
            },
            rewards: Some(rewards),
        },
        txs: txs.into_values().collect(),
    };
    Ok(Some(CompleteBank {
        source_bytes,
        row,
        block,
        poh,
        signatures,
        evidence: events,
    }))
}
pub(crate) fn bank_events(
    db: &Connection,
    slot: u64,
    bank: u64,
    codec: &str,
    max_event_bytes: usize,
) -> Result<Vec<RawEvidence>> {
    bank_events_through(db, slot, bank, codec, max_event_bytes, i64::MAX as u64)
}
pub(crate) fn bank_events_through(
    db: &Connection,
    slot: u64,
    bank: u64,
    codec: &str,
    max_event_bytes: usize,
    through: u64,
) -> Result<Vec<RawEvidence>> {
    let mut query=db.prepare("SELECT sequence,sha256,payload FROM events WHERE slot=?1 AND (bank=?2 OR bank IS NULL) AND sequence<=?3 ORDER BY sequence")?;
    let mut rows = query.query(params![slot, bank, through])?;
    let mut bytes = 0usize;
    let mut events = Vec::new();
    while let Some(row) = rows.next()? {
        let sequence: u64 = row.get(0)?;
        let sha256: String = row.get(1)?;
        let stored: Vec<u8> = row.get(2)?;
        let payload = crate::journal::decode_payload(&stored, codec, max_event_bytes)?;
        bytes = bytes
            .checked_add(payload.len())
            .context("bank size overflow")?;
        ensure!(
            bytes <= 128 * 1024 * 1024 && events.len() < 1_000_000,
            "bank source exceeds bounded worker budget"
        );
        ensure!(
            digest(&payload) == sha256,
            "journal payload digest mismatch"
        );
        events.push(RawEvidence {
            sequence,
            sha256,
            event: serde_json::value::RawValue::from_string(String::from_utf8(payload)?)?,
        });
    }
    Ok(events)
}
// Bank decoding dominates worker CPU. Keep at most four independent banks in
// flight, each under bank_events' 128 MiB source limit. Results keep root order;
// parent, duplicate, custody, and pending-batch checks stay on the caller.
const BANK_READERS: usize = 4;
fn load_bank_group(
    session: &Path,
    keys: &[(u64, u64)],
    codec: &str,
    max_event_bytes: usize,
) -> Vec<(u64, u64, Result<Option<CompleteBank>>)> {
    assert!(keys.len() <= BANK_READERS);
    thread::scope(|scope| {
        let tasks = keys
            .iter()
            .map(|&(slot, bank)| {
                (
                    slot,
                    bank,
                    scope.spawn(move || {
                        let source = open_reader(&session.join("events.sqlite"))?;
                        assemble_raw(
                            slot,
                            bank,
                            bank_events(&source, slot, bank, codec, max_event_bytes)?,
                        )
                    }),
                )
            })
            .collect::<Vec<_>>();
        tasks
            .into_iter()
            .map(|(slot, bank, task)| {
                (
                    slot,
                    bank,
                    task.join()
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("bank reader panicked"))),
                )
            })
            .collect()
    })
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Cursor {
    session_id: String,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    last_hash: Option<[u8; 32]>,
    batches: u64,
    #[serde(default)]
    compacted_blocks: u64,
    #[serde(default)]
    compacted_transactions: u64,
    #[serde(default)]
    compacted_entries: u64,
    #[serde(default)]
    updated_unix_ms: u64,
    startup_incomplete_roots: Vec<(u64, u64)>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingBatch {
    session_id: String,
    first_slot: u64,
    last_slot: u64,
}
fn pending_batch(output: &Path, session_id: &str, cursor: &Cursor) -> Result<Option<PendingBatch>> {
    let path = output.join("PENDING.json");
    if !path.exists() {
        return Ok(None);
    }
    let pending: PendingBatch = serde_json::from_slice(&bounded_bytes(&path, 4096)?)?;
    ensure!(
        pending.session_id == session_id
            && pending.first_slot <= pending.last_slot
            && pending.first_slot / 432000 == pending.last_slot / 432000,
        "invalid pending batch"
    );
    if cursor
        .last_slot
        .is_some_and(|last| pending.last_slot <= last)
    {
        fs::remove_file(path)?;
        File::open(output)?.sync_all()?;
        return Ok(None);
    }
    ensure!(
        cursor
            .last_slot
            .is_none_or(|last| pending.first_slot > last),
        "pending batch overlaps durable cursor"
    );
    Ok(Some(pending))
}

pub fn run(
    session: &Path,
    output: &Path,
    builder: &Path,
    max_blocks: usize,
    follow: bool,
) -> Result<()> {
    ensure!((1..=128).contains(&max_blocks), "batch size must be 1..128");
    fs::create_dir_all(output)?;
    let _lock = crate::journal::exclusive_lock(&output.join("WORKER.lock"))?;
    let result = run_inner(session, output, builder, max_blocks, follow);
    if let Err(error) = &result {
        let _ = atomic_json(
            &output.join("STATUS.json"),
            &json!({"state":"FAILED","error":format!("{error:#}"),"source_preserved":true}),
        );
    }
    result
}
fn run_inner(
    session: &Path,
    output: &Path,
    builder: &Path,
    max_blocks: usize,
    follow: bool,
) -> Result<()> {
    let session_bytes = fs::read(session.join("SESSION.json"))?;
    ensure!(
        session_bytes.len() < 1024 * 1024,
        "session metadata too large"
    );
    let meta: serde_json::Value = serde_json::from_slice(&session_bytes)?;
    ensure!(
        meta["schema"] == crate::event::SCHEMA && meta["agave_revision"] == REVISION,
        "unsupported source journal"
    );
    let session_id = meta["session_id"]
        .as_str()
        .context("missing session ID")?
        .to_string();
    let genesis = meta["config"]["genesis_hash"]
        .as_str()
        .context("missing genesis")?
        .to_string();
    // The fixed epoch geometry below is valid only for this explicitly bound mainnet source.
    ensure!(
        genesis == "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
        "worker currently supports mainnet epoch geometry only"
    );
    let codec = meta["payload_codec"].as_str().unwrap_or("json-v1");
    ensure!(
        matches!(codec, "json-v1" | "json-zstd-v1"),
        "unknown source codec"
    );
    let max_event_bytes = usize::try_from(
        meta["config"]["max_event_bytes"]
            .as_u64()
            .context("missing event limit")?,
    )?;
    ensure!(
        max_event_bytes <= 16 * 1024 * 1024,
        "source event limit exceeds decoder bound"
    );
    let cursor_path = output.join("CURSOR.json");
    let mut cursor: Cursor = if cursor_path.exists() {
        serde_json::from_slice(&fs::read(&cursor_path)?)?
    } else {
        Cursor {
            session_id: session_id.clone(),
            ..Cursor::default()
        }
    };
    ensure!(
        cursor.session_id == session_id,
        "output belongs to another capture session"
    );
    let db = open_reader(&session.join("events.sqlite"))?;
    let mut audit = open_audit(output)?;
    migrate_counters(&audit, &mut cursor)?;
    atomic_json(&cursor_path, &cursor)?;
    if meta["retirement"] == "rooted-banks-v1" {
        recover_retirement(&db, &audit, session, &session_id)?;
    }
    let has_root_watermark = db.prepare("SELECT latest_rooted_slot FROM status").is_ok();
    loop {
        ensure!(
            !session.join("RETIREMENT-FAILED.json").exists(),
            "source retirement failed; capture remains independent and source records are preserved"
        );
        let failure: serde_json::Value =
            serde_json::from_slice(&fs::read(session.join("FAILURE.json"))?)?;
        ensure!(
            failure.get("state").is_none(),
            "capture failure: {}",
            failure
        );
        let (state, durable): (String, u64) = db.query_row(
            "SELECT state,durable_sequence FROM status WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(state != "FAILED", "journal writer failed");
        ensure!(
            state != "CAPTURING"
                || crate::journal::exclusive_lock(&session.join("LIVE.lock")).is_err(),
            "capture ended without a clean close; possible source gap"
        );
        audit_late_events(&db, &mut audit)?;
        let pending_batch = pending_batch(output, &session_id, &cursor)?;
        let roots = {
            let mut q=db.prepare("SELECT slot,bank FROM events WHERE kind='rooted' AND slot>?1 GROUP BY slot,bank ORDER BY slot LIMIT 256")?;
            q.query_map(
                [cursor.last_slot.unwrap_or_else(|| {
                    cursor
                        .startup_incomplete_roots
                        .last()
                        .map(|r| r.0)
                        .unwrap_or(0)
                })],
                |r| Ok((r.get::<_, u64>(0)?, r.get::<_, u64>(1)?)),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        let newest: u64 = if has_root_watermark {
            db.query_row(
                "SELECT latest_rooted_slot FROM status WHERE id=1",
                [],
                |r| r.get(0),
            )?
        } else {
            db.query_row(
                "SELECT COALESCE(max(slot),0) FROM events WHERE kind='rooted'",
                [],
                |r| r.get(0),
            )?
        };
        let mut complete: Vec<CompleteBank> = Vec::new();
        let mut previous = cursor.last_slot.zip(cursor.last_hash);
        let prepared = roots
            .chunks(BANK_READERS)
            .flat_map(|keys| load_bank_group(session, keys, codec, max_event_bytes));
        for (slot, bank, block) in prepared {
            if let Some(pending) = &pending_batch {
                if slot > pending.last_slot {
                    break;
                }
                ensure!(slot >= pending.first_slot, "root before pending batch");
            }
            if cursor.startup_incomplete_roots.contains(&(slot, bank)) {
                continue;
            }
            let Some(block) = block? else {
                if cursor.last_slot.is_none()
                    && complete.is_empty()
                    && (newest > slot + 64 || state == "CLOSED")
                {
                    ensure!(
                        cursor.startup_incomplete_roots.len() < 4096,
                        "startup gap exceeds 4096 roots"
                    );
                    cursor.startup_incomplete_roots.push((slot, bank));
                    atomic_json(&cursor_path, &cursor)?;
                    continue;
                }
                ensure!(
                    newest <= slot + 256,
                    "rooted bank {slot}/{bank} remains incomplete after 256 slots; capture gap"
                );
                break;
            };
            if let Some((last, hash)) = previous {
                ensure!(
                    block.row.parent_slot == last && block.row.parent_blockhash == hash,
                    "parent chain gap at slot {slot}: expected parent {last}, got {}",
                    block.row.parent_slot
                );
            }
            if !complete.is_empty() && block.row.slot / 432000 != complete[0].row.slot / 432000 {
                break;
            }
            if pending_batch.is_none()
                && batch_is_full(
                    complete.len(),
                    complete.iter().map(|b| b.source_bytes).sum(),
                    block.source_bytes,
                    max_blocks,
                )
            {
                break;
            }
            previous = Some((block.row.slot, block.row.blockhash));
            complete.push(block);
            if pending_batch.is_none() && complete.len() == max_blocks {
                break;
            }
        }
        if let Some(pending) = &pending_batch {
            ensure!(
                complete
                    .first()
                    .is_some_and(|b| b.row.slot == pending.first_slot)
                    && complete
                        .last()
                        .is_some_and(|b| b.row.slot == pending.last_slot),
                "durable pending batch is no longer complete in the source"
            );
        }
        if complete.is_empty() {
            let pending: u64 = db.query_row(
                "SELECT count(*) FROM events WHERE kind='rooted' AND slot>?1",
                [cursor.last_slot.unwrap_or_else(|| {
                    cursor
                        .startup_incomplete_roots
                        .last()
                        .map(|r| r.0)
                        .unwrap_or(0)
                })],
                |r| r.get(0),
            )?;
            ensure!(
                state != "CLOSED" || pending == 0,
                "closed journal has incomplete rooted banks"
            );
            atomic_json(
                &output.join("STATUS.json"),
                &json!({"state":if state=="CLOSED"{"DRAINED"}else{"WAITING"},"last_compact_slot":cursor.last_slot,"first_compact_slot":cursor.first_slot,"compacted_blocks":cursor.compacted_blocks,"compacted_transactions":cursor.compacted_transactions,"compacted_entries":cursor.compacted_entries,"updated_unix_ms":cursor.updated_unix_ms,"durable_sequence":durable,"startup_incomplete_roots":cursor.startup_incomplete_roots,"full_epoch":false}),
            )?;
            if !follow || state == "CLOSED" {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(500));
            continue;
        }
        let first = complete[0].row.slot;
        let last = complete.last().unwrap().row.slot;
        if pending_batch.is_none() {
            atomic_json(
                &output.join("PENDING.json"),
                &PendingBatch {
                    session_id: session_id.clone(),
                    first_slot: first,
                    last_slot: last,
                },
            )?;
        }
        let epoch = first / 432000;
        let epoch_dir = output.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&epoch_dir)?;
        let normalized = epoch_dir.join(format!("{first}-{last}.normalized"));
        let compact = epoch_dir.join(format!("{first}-{last}.compact"));
        if !normalized.exists() {
            write_normalized(&normalized, &meta, &session_id, &genesis, &mut complete)?;
        }
        if !compact.exists() {
            let status = Command::new(builder)
                .arg("build-direct-agave-compact")
                .arg(&normalized)
                .arg(&compact)
                .args(["--level", "1"])
                .status()
                .context("run compact writer")?;
            ensure!(
                status.success(),
                "compact writer failed; durable source remains available"
            );
        }
        validate_compact_commit(&compact, &normalized)?;
        {
            let tx = audit.transaction()?;
            for bank in &complete {
                let through = bank
                    .evidence
                    .iter()
                    .map(|e| e.sequence)
                    .max()
                    .context("empty block source")?;
                tx.execute("INSERT OR IGNORE INTO commits(slot,bank,through_sequence,artifact) VALUES(?1,?2,?3,?4)",params![bank.row.slot,bank.row.bank_id,through,compact.to_string_lossy()])?;
                let existing: u64 = tx.query_row(
                    "SELECT bank FROM commits WHERE slot=?1",
                    [bank.row.slot],
                    |r| r.get(0),
                )?;
                ensure!(
                    existing == bank.row.bank_id,
                    "output slot already committed for another bank"
                );
            }
            tx.commit()?;
        }

        cursor.first_slot.get_or_insert(first);
        cursor.last_slot = Some(last);
        cursor.last_hash = Some(complete.last().unwrap().row.blockhash);
        cursor.batches += 1;
        cursor.compacted_blocks = cursor
            .compacted_blocks
            .checked_add(complete.len() as u64)
            .context("block counter overflow")?;
        cursor.compacted_transactions = cursor
            .compacted_transactions
            .checked_add(complete.iter().map(|b| b.row.transactions).sum())
            .context("transaction counter overflow")?;
        cursor.compacted_entries = cursor
            .compacted_entries
            .checked_add(complete.iter().map(|b| b.row.entries).sum())
            .context("entry counter overflow")?;
        cursor.updated_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        atomic_json(&cursor_path, &cursor)?; // Output files and marker were fsynced before cursor advance.
        if meta["retirement"] == "rooted-banks-v1" {
            request_retirement(session, &session_id, &compact, &complete)?;
        }

        fs::remove_file(output.join("PENDING.json"))?;
        File::open(output)?.sync_all()?;
        fs::remove_dir_all(&normalized)?;
        File::open(&epoch_dir)?.sync_all()?;
        atomic_json(
            &output.join("STATUS.json"),
            &json!({"state":"BUILDING","last_compact_slot":last,"epoch":epoch,"batches":cursor.batches,"first_compact_slot":cursor.first_slot,"compacted_blocks":cursor.compacted_blocks,"compacted_transactions":cursor.compacted_transactions,"compacted_entries":cursor.compacted_entries,"updated_unix_ms":cursor.updated_unix_ms,"durable_sequence":durable,"full_epoch":false,"canonical":false}),
        )?;
        if !follow && state != "CLOSED" {
            return Ok(());
        }
    }
}
fn bounded_bytes(path: &Path, max: u64) -> Result<Vec<u8>> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "expected regular file"
    );
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= max, "file exceeds read bound");
    Ok(bytes)
}
fn file_hash(path: &Path, max: u64) -> Result<(u64, String)> {
    use sha2::{Digest, Sha256};
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "expected regular file"
    );
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        ensure!(size <= max, "file exceeds size bound");
        hash.update(&buffer[..n]);
    }
    Ok((
        size,
        hash.finalize().iter().map(|b| format!("{b:02x}")).collect(),
    ))
}
pub(crate) fn validate_compact_commit(compact: &Path, normalized: &Path) -> Result<()> {
    let marker: serde_json::Value = serde_json::from_slice(&bounded_bytes(
        &compact.join("DIRECT-AGAVE-COMPACT.v1.json"),
        16 * 1024 * 1024,
    )?)?;
    ensure!(
        (marker["schema"] == "hivezilla-direct-agave-compact-v1"
            || marker["schema"] == "hivezilla-direct-agave-processed-compact-v1")
            && marker["state"] == "COMPLETE"
            && marker["canonical"] == false
            && marker["publication_ready"] == false
            && marker["source_profile"] == "agave-bank-callbacks-v1",
        "invalid compact commit marker"
    );
    let receipt_bytes = bounded_bytes(
        &normalized.join("DIRECT-AGAVE-NORMALIZED.v1.json"),
        16 * 1024 * 1024,
    )?;
    let receipt: Receipt = serde_json::from_slice(&receipt_bytes)?;
    ensure!(
        marker["finalized"] == receipt.finalized
            && ((receipt.finalized
                && receipt.schema == "hivezilla-direct-agave-normalized-v1"
                && marker["schema"] == "hivezilla-direct-agave-compact-v1")
                || (!receipt.finalized
                    && receipt.schema == "hivezilla-direct-agave-processed-normalized-v1"
                    && marker["schema"] == "hivezilla-direct-agave-processed-compact-v1"
                    && receipt.blocks.len() == 1)),
        "compact commitment differs from source"
    );
    ensure!(
        marker["source_receipt_sha256"] == digest(&receipt_bytes),
        "compact source receipt changed"
    );
    ensure!(
        marker["session_id"] == receipt.session_id
            && marker["agave_revision"] == receipt.agave_revision
            && marker["genesis_hash"] == receipt.genesis_hash
            && marker["node_identity"] == receipt.node_identity
            && marker["epoch"] == receipt.epoch
            && marker["start_slot"] == receipt.start_slot
            && marker["end_slot_exclusive"] == receipt.end_slot_exclusive
            && marker["produced_blocks"] == serde_json::to_value(&receipt.blocks)?,
        "compact commit refers to a different source range"
    );
    ensure!(
        marker["blocks"] == receipt.blocks.len()
            && marker["transactions"] == receipt.blocks.iter().map(|r| r.transactions).sum::<u64>()
            && marker["signatures"] == receipt.blocks.iter().map(|r| r.signatures).sum::<u64>()
            && marker["entries"] == receipt.blocks.iter().map(|r| r.entries).sum::<u64>(),
        "compact counts differ from source"
    );
    let rows = marker["files"]
        .as_array()
        .context("compact marker lacks files")?;
    ensure!(
        !rows.is_empty() && rows.len() <= 64,
        "invalid compact file count"
    );
    let mut names = BTreeSet::new();
    for row in rows {
        let name = row["path"].as_str().context("file path missing")?;
        ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                && name != "."
                && name != ".."
                && names.insert(name),
            "unsafe or duplicate compact filename"
        );
        let (size, hash) = file_hash(&compact.join(name), 512 * 1024 * 1024)?;
        ensure!(
            size == row["bytes"].as_u64().context("missing file size")?
                && hash == row["sha256"].as_str().context("missing file digest")?,
            "compact object changed before cursor commit"
        );
    }
    for required in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        "DIRECT-AGAVE-NORMALIZED.v1.json",
        "available-poh.wincode",
    ] {
        ensure!(
            names.contains(required),
            "compact manifest missing {required}"
        );
    }
    ensure!(
        names.contains("source-events.json") || names.contains("source-events.json.zst"),
        "compact manifest lacks source events"
    );
    if names.contains("source-events.json.zst") {
        let original = receipt
            .files
            .iter()
            .find(|f| f.path == "evidence/source-events.json")
            .context("receipt lacks source events")?;
        ensure!(
            marker["source_events_encoding"] == "zstd-json-v1"
                && marker["source_events_uncompressed_bytes"] == original.bytes
                && marker["source_events_uncompressed_sha256"] == original.sha256,
            "compressed evidence differs from original source receipt"
        );
    }
    Ok(())
}
fn write_normalized(
    output: &Path,
    meta: &serde_json::Value,
    session: &str,
    genesis: &str,
    blocks: &mut [CompleteBank],
) -> Result<()> {
    write_bank_normalized(output, meta, session, genesis, blocks, true)
}

pub(crate) fn write_bank_normalized(
    output: &Path,
    meta: &serde_json::Value,
    session: &str,
    genesis: &str,
    blocks: &mut [CompleteBank],
    finalized: bool,
) -> Result<()> {
    ensure!(
        finalized || blocks.len() == 1,
        "processed payload must contain one bank"
    );
    let stage = output.with_extension("normalizing");
    // A stage has no committed cursor or public marker. Recover only our own deterministic scratch path.
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir(&stage)?;
    for d in ["blocks", "index", "evidence"] {
        fs::create_dir(stage.join(d))?;
    }
    let mut block_file = WincodeLeb128FramedWriter::new(File::create(
        stage.join("blocks/live-no-registry-blocks.bin"),
    )?);
    let mut poh_file =
        WincodeLeb128FramedWriter::new(File::create(stage.join("evidence/available-poh.wincode"))?);
    let mut hashes = File::create(stage.join("index/blockhash_registry.bin"))?;
    let mut signatures = File::create(stage.join("index/signatures.bin"))?;
    let mut evidence = BufWriter::with_capacity(
        1024 * 1024,
        File::create(stage.join("evidence/source-events.json"))?,
    );
    evidence.write_all(b"[")?;
    let mut first_event = true;
    for (id, bank) in blocks.iter_mut().enumerate() {
        bank.row.block_id = id as u32;
        bank.block.header.compact.blockhash = id as u32;
        bank.block.header.compact.previous_blockhash = (id as u32).saturating_sub(1);
        block_file.write(&bank.block)?;
        poh_file.write(&WincodeArchiveV2PohRecord {
            block_id: id as u32,
            slot: bank.row.slot,
            entries: bank.poh.clone(),
        })?;
        hashes.write_all(&bank.row.blockhash)?;
        signatures.write_all(&bank.signatures)?;
        for e in &bank.evidence {
            if !first_event {
                evidence.write_all(b",")?;
            }
            first_event = false;
            serde_json::to_writer(&mut evidence, e)?;
        }
    }
    evidence.write_all(b"]\n")?;
    block_file.flush()?;
    poh_file.flush()?;
    drop(block_file);
    drop(poh_file);
    evidence.flush()?;
    evidence.get_ref().sync_all()?;
    for file in [&hashes, &signatures] {
        file.sync_all()?;
    }
    let mut files = Vec::new();
    for path in [
        "blocks/live-no-registry-blocks.bin",
        "index/blockhash_registry.bin",
        "index/signatures.bin",
        "evidence/source-events.json",
        "evidence/available-poh.wincode",
    ] {
        let full = stage.join(path);
        File::open(&full)?.sync_all()?;
        let bytes = fs::read(full)?;
        ensure!(
            bytes.len() <= 1024 * 1024 * 1024,
            "normalized file exceeds 1 GiB"
        );
        files.push(FileRow {
            path: path.into(),
            bytes: bytes.len() as u64,
            sha256: digest(&bytes),
        });
    }
    let receipt = Receipt {
        schema: if finalized {
            "hivezilla-direct-agave-normalized-v1"
        } else {
            "hivezilla-direct-agave-processed-normalized-v1"
        }
        .into(),
        canonical: false,
        finalized,
        source_profile: "agave-bank-callbacks-v1".into(),
        session_id: session.into(),
        agave_revision: REVISION.into(),
        genesis_hash: genesis.into(),
        node_identity: meta["config"]["node_identity"]
            .as_str()
            .context("missing identity")?
            .into(),
        epoch: blocks[0].row.slot / 432000,
        start_slot: blocks[0].row.slot,
        end_slot_exclusive: blocks.last().unwrap().row.slot + 1,
        blocks: blocks.iter().map(|b| b.row.clone()).collect(),
        files,
    };
    atomic_json(&stage.join("DIRECT-AGAVE-NORMALIZED.v1.json"), &receipt)?;
    for d in ["blocks", "index", "evidence"] {
        File::open(stage.join(d))?.sync_all()?;
    }
    File::open(&stage)?.sync_all()?;
    fs::rename(&stage, output)?;
    File::open(output.parent().unwrap())?.sync_all()?;
    Ok(())
}

fn batch_is_full(blocks: usize, bytes: usize, next_bytes: usize, max_blocks: usize) -> bool {
    blocks > 0 && (blocks >= max_blocks || bytes.saturating_add(next_bytes) > 128 * 1024 * 1024)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parallel_bank_reads_keep_input_order_and_source_checks() {
        let temp = tempfile::tempdir().unwrap();
        let db = Connection::open(temp.path().join("events.sqlite")).unwrap();
        db.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY,slot INTEGER,bank INTEGER,sha256 TEXT,payload BLOB);").unwrap();
        let mut sequence = 0u64;
        for slot in [10u64, 11, 12] {
            let bank = slot + 100;
            let events = [
                Event::Status {
                    slot,
                    bank_id: Some(bank),
                    parent: Some(slot - 1),
                    status: "rooted".into(),
                    dead_error: None,
                },
                Event::Entry {
                    slot,
                    bank_id: bank,
                    index: 0,
                    num_hashes: 1,
                    hash: [slot as u8; 32],
                    transactions: 0,
                    starting_transaction_index: 0,
                },
                Event::Block {
                    slot,
                    bank_id: bank,
                    parent_slot: slot - 1,
                    parent_blockhash: [slot as u8 - 1; 32],
                    blockhash: [slot as u8; 32],
                    transactions: 0,
                    entries: 1,
                    block_time: None,
                    block_height: None,
                    native_rewards: json!({"rewards":[]}),
                    rewards: WincodeArchiveV2NoRegistryRewards {
                        source_len: 0,
                        num_partitions: None,
                        decoded: Some(vec![]),
                        raw_fallback: None,
                        decode_error: None,
                    },
                },
            ];
            for event in events {
                sequence += 1;
                let raw = serde_json::to_vec(&event).unwrap();
                let sha = if slot == 11 {
                    "bad".to_string()
                } else {
                    digest(&raw)
                };
                let stored = zstd::stream::encode_all(raw.as_slice(), 1).unwrap();
                db.execute(
                    "INSERT INTO events VALUES(?1,?2,?3,?4,?5)",
                    params![sequence, slot, bank, sha, stored],
                )
                .unwrap();
            }
        }
        let rows = load_bank_group(
            temp.path(),
            &[(12, 112), (10, 110), (11, 111)],
            "json-zstd-v1",
            1024 * 1024,
        );
        assert_eq!(
            rows.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
            vec![(12, 112), (10, 110), (11, 111)]
        );
        assert_eq!(rows[0].2.as_ref().unwrap().as_ref().unwrap().row.slot, 12);
        assert_eq!(
            rows[1].2.as_ref().unwrap().as_ref().unwrap().row.bank_id,
            110
        );
        assert!(
            rows[2]
                .2
                .as_ref()
                .err()
                .unwrap()
                .to_string()
                .contains("journal payload digest mismatch")
        );
    }

    #[test]
    fn byte_budget_splits_before_count_limit() {
        assert!(!batch_is_full(0, 0, 100 * 1024 * 1024, 32));
        assert!(batch_is_full(1, 100 * 1024 * 1024, 40 * 1024 * 1024, 32));
        assert!(!batch_is_full(1, 100 * 1024 * 1024, 28 * 1024 * 1024, 32));
        assert!(batch_is_full(32, 1000, 10, 32));
    }
}

fn open_audit(output: &Path) -> Result<Connection> {
    let db = Connection::open(output.join("committed.sqlite"))?;
    // Recovery looks up every artifact. An unindexed lookup rescans all committed
    // slots for each artifact and becomes quadratic after a long live capture.
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS commits(slot INTEGER PRIMARY KEY,bank INTEGER NOT NULL,through_sequence INTEGER NOT NULL,artifact TEXT NOT NULL); CREATE INDEX IF NOT EXISTS commits_artifact ON commits(artifact); CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY CHECK(id=1),sequence INTEGER NOT NULL); INSERT OR IGNORE INTO audit VALUES(1,0);")?;
    Ok(db)
}
fn audit_late_events(source: &Connection, audit: &mut Connection) -> Result<()> {
    loop {
        let after: u64 =
            audit.query_row("SELECT sequence FROM audit WHERE id=1", [], |r| r.get(0))?;
        let rows = {
            let mut q=source.prepare("SELECT sequence,slot,bank,kind,sha256 FROM events WHERE sequence>?1 ORDER BY sequence LIMIT 4096")?;
            q.query_map([after], |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, Option<u64>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if rows.is_empty() {
            break;
        }
        // This worker alone writes commits, outside this audit pass. Events in
        // one slot can share the lookup. Keep the cache bounded to this chunk.
        let mut commits = BTreeMap::new();
        for (sequence, slot, bank, kind, sha) in &rows {
            if !commits.contains_key(slot) {
                let committed: Option<(u64, u64, String)> = audit
                    .query_row(
                        "SELECT bank,through_sequence,artifact FROM commits WHERE slot=?1",
                        [slot],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional()?;
                commits.insert(*slot, committed);
            }
            let Some((sealed_bank, through, artifact)) = commits[slot].as_ref() else {
                continue;
            };
            if kind == "rooted" {
                ensure!(
                    *bank == Some(*sealed_bank),
                    "late competing root for committed slot {slot}"
                );
            }
            if *sequence <= *through {
                continue;
            }
            if (kind == "dead" && bank.is_none())
                || (*bank == Some(*sealed_bank) && matches!(kind.as_str(), "dead" | "clear"))
            {
                bail!("late invalidation for committed slot {slot}");
            }
            if *bank == Some(*sealed_bank)
                && matches!(kind.as_str(), "transaction" | "entry" | "block" | "footer")
            {
                let same:bool=source.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE slot=?1 AND bank=?2 AND sha256=?3 AND sequence<=?4)",params![slot,sealed_bank,sha,through],|r|r.get(0))?;
                ensure!(
                    same || retained_source_contains(
                        Path::new(&artifact),
                        *slot,
                        *sealed_bank,
                        sha
                    )?,
                    "late conflicting {kind} callback for committed slot {slot}"
                );
            }
        }
        audit.execute(
            "UPDATE audit SET sequence=?1 WHERE id=1",
            [rows.last().unwrap().0],
        )?;
        if rows.len() < 4096 {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod late_tests {
    use super::*;
    #[test]
    fn existing_capture_recovery_uses_indexed_artifact_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let mut legacy = Connection::open(temp.path().join("committed.sqlite")).unwrap();
        legacy.execute_batch("CREATE TABLE commits(slot INTEGER PRIMARY KEY,bank INTEGER NOT NULL,through_sequence INTEGER NOT NULL,artifact TEXT NOT NULL);").unwrap();
        let tx = legacy.transaction().unwrap();
        for slot in 0..1000u64 {
            let artifact = if matches!(slot, 4 | 20 | 250) {
                "shared".to_string()
            } else {
                format!("artifact-{slot}")
            };
            tx.execute(
                "INSERT INTO commits VALUES(?1,?2,?3,?4)",
                params![slot, slot, slot + 1, artifact],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        drop(legacy);
        for _ in 0..2 {
            let audit = open_audit(temp.path()).unwrap();
            let slots = audit
                .prepare("SELECT slot FROM commits WHERE artifact=?1 ORDER BY slot")
                .unwrap()
                .query_map(["shared"], |row| row.get::<_, u64>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(slots, vec![4, 20, 250]);
            let plans = audit.prepare("EXPLAIN QUERY PLAN SELECT slot,bank,through_sequence FROM commits WHERE artifact=?1 ORDER BY slot").unwrap()
                .query_map(["shared"], |row| row.get::<_, String>(3)).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().unwrap();
            assert!(
                plans
                    .iter()
                    .any(|plan| plan.contains("SEARCH commits USING INDEX commits_artifact")),
                "{plans:?}"
            );
            let count: u64 = audit
                .query_row("SELECT count(*) FROM commits", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 1000);
        }
    }
    fn source() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY,slot INTEGER,bank INTEGER,kind TEXT,sha256 TEXT,payload BLOB); INSERT INTO events VALUES(1,10,7,'entry','original',X'00');").unwrap();
        db
    }
    #[test]
    fn late_identical_event_is_allowed_but_conflict_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let source = source();
        let mut audit = open_audit(temp.path()).unwrap();
        audit_late_events(&source, &mut audit).unwrap();
        audit
            .execute("INSERT INTO commits VALUES(10,7,1,'fixture')", [])
            .unwrap();
        source
            .execute_batch("INSERT INTO events VALUES(2,10,7,'entry','original',X'00');")
            .unwrap();
        audit_late_events(&source, &mut audit).unwrap();
        source
            .execute_batch("INSERT INTO events VALUES(3,10,7,'entry','changed',X'00');")
            .unwrap();
        assert!(audit_late_events(&source, &mut audit).is_err());
    }
    #[test]
    fn shared_slot_lookup_still_rejects_a_competing_root_in_the_same_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let source = source();
        let mut audit = open_audit(temp.path()).unwrap();
        audit_late_events(&source, &mut audit).unwrap();
        audit
            .execute("INSERT INTO commits VALUES(10,7,1,'fixture')", [])
            .unwrap();
        source.execute_batch("INSERT INTO events VALUES(2,10,7,'rooted','root',X'00'); INSERT INTO events VALUES(3,10,8,'rooted','competing',X'00');").unwrap();
        let error = audit_late_events(&source, &mut audit).unwrap_err();
        assert!(error.to_string().contains("late competing root"));
        let sequence: u64 = audit
            .query_row("SELECT sequence FROM audit WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sequence, 1);
    }
    #[test]
    fn retired_journal_uses_compressed_source_for_late_callback_audit() {
        let temp = tempfile::tempdir().unwrap();
        let artifact = temp.path().join("compact");
        fs::create_dir(&artifact).unwrap();
        let event = serde_json::to_string(&Event::Entry {
            slot: 10,
            bank_id: 7,
            index: 0,
            num_hashes: 1,
            hash: [9; 32],
            transactions: 0,
            starting_transaction_index: 0,
        })
        .unwrap();
        let sha = digest(event.as_bytes());
        let evidence = serde_json::to_vec(&vec![RawEvidence {
            sequence: 1,
            sha256: sha.clone(),
            event: serde_json::value::RawValue::from_string(event).unwrap(),
        }])
        .unwrap();
        fs::write(
            artifact.join("source-events.json.zst"),
            zstd::stream::encode_all(evidence.as_slice(), 1).unwrap(),
        )
        .unwrap();
        atomic_json(
            &artifact.join("DIRECT-AGAVE-COMPACT.v1.json"),
            &json!({
                "source_events_uncompressed_bytes":evidence.len(),
                "source_events_uncompressed_sha256":digest(&evidence),
            }),
        )
        .unwrap();
        let source = source();
        source.execute_batch("DELETE FROM events;").unwrap();
        let mut audit = open_audit(temp.path()).unwrap();
        audit
            .execute(
                "INSERT INTO commits VALUES(10,7,1,?1)",
                [artifact.to_str().unwrap()],
            )
            .unwrap();
        source
            .execute("INSERT INTO events VALUES(2,10,7,'entry',?1,X'00')", [&sha])
            .unwrap();
        audit_late_events(&source, &mut audit).unwrap();
        source
            .execute_batch("INSERT INTO events VALUES(3,10,7,'entry','changed',X'00');")
            .unwrap();
        let error = audit_late_events(&source, &mut audit).unwrap_err();
        assert!(
            error.to_string().contains("late conflicting entry"),
            "{error:#}"
        );
    }

    #[test]
    fn late_other_fork_death_is_allowed_but_competing_root_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let source = source();
        let mut audit = open_audit(temp.path()).unwrap();
        audit
            .execute("INSERT INTO commits VALUES(10,7,1,'fixture')", [])
            .unwrap();
        source
            .execute_batch("INSERT INTO events VALUES(2,10,8,'dead','dead',X'00');")
            .unwrap();
        audit_late_events(&source, &mut audit).unwrap();
        source
            .execute_batch("INSERT INTO events VALUES(3,10,8,'rooted','root',X'00');")
            .unwrap();
        assert!(audit_late_events(&source, &mut audit).is_err());
    }
}

fn migrate_counters(audit: &Connection, cursor: &mut Cursor) -> Result<()> {
    let Some(last) = cursor.last_slot else {
        return Ok(());
    };
    let count: u64 =
        audit.query_row("SELECT count(*) FROM commits WHERE slot<=?1", [last], |r| {
            r.get(0)
        })?;
    if count == cursor.compacted_blocks {
        return Ok(());
    }
    let artifacts = {
        let mut q = audit.prepare("SELECT DISTINCT artifact FROM commits WHERE slot<=?1")?;
        q.query_map([last], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut blocks = BTreeMap::new();
    for artifact in &artifacts {
        let marker: serde_json::Value = serde_json::from_slice(&bounded_bytes(
            &Path::new(artifact).join("DIRECT-AGAVE-COMPACT.v1.json"),
            16 * 1024 * 1024,
        )?)?;
        ensure!(
            marker["state"] == "COMPLETE" && marker["session_id"] == cursor.session_id,
            "counter migration found a different source"
        );
        for row in marker["produced_blocks"]
            .as_array()
            .context("missing block rows")?
        {
            let row: BlockRow = serde_json::from_value(row.clone())?;
            if row.slot > last {
                continue;
            }
            let bank: u64 =
                audit.query_row("SELECT bank FROM commits WHERE slot=?1", [row.slot], |r| {
                    r.get(0)
                })?;
            ensure!(bank == row.bank_id, "counter migration bank mismatch");
            ensure!(
                blocks.insert(row.slot, row).is_none(),
                "duplicate compact block in counter migration"
            );
        }
    }
    ensure!(
        blocks.len() as u64 == count,
        "counter migration coverage mismatch"
    );
    cursor.compacted_blocks = count;
    cursor.compacted_transactions = blocks.values().map(|b| b.transactions).sum();
    cursor.compacted_entries = blocks.values().map(|b| b.entries).sum();
    cursor.batches = artifacts.len() as u64;
    Ok(())
}
fn request_retirement(
    session: &Path,
    session_id: &str,
    compact: &Path,
    banks: &[CompleteBank],
) -> Result<()> {
    let marker_path = compact.join("DIRECT-AGAVE-COMPACT.v1.json");
    let request = crate::retention::Request {
        schema: "hivezilla-direct-agave-retire-v1".into(),
        session_id: session_id.into(),
        marker_path: marker_path.to_string_lossy().into_owned(),
        marker_sha256: digest(&bounded_bytes(&marker_path, 16 * 1024 * 1024)?),
        banks: banks
            .iter()
            .map(|b| crate::retention::Bank {
                slot: b.row.slot,
                bank_id: b.row.bank_id,
                through_sequence: b.evidence.iter().map(|e| e.sequence).max().unwrap(),
            })
            .collect(),
    };
    crate::retention::enqueue(session, &request)
}
fn recover_retirement(
    source: &Connection,
    audit: &Connection,
    session: &Path,
    session_id: &str,
) -> Result<()> {
    let artifacts = {
        let mut q = audit.prepare("SELECT DISTINCT artifact FROM commits")?;
        q.query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for artifact in artifacts {
        let banks = {
            let mut q = audit.prepare(
                "SELECT slot,bank,through_sequence FROM commits WHERE artifact=?1 ORDER BY slot",
            )?;
            q.query_map([&artifact], |r| {
                Ok(crate::retention::Bank {
                    slot: r.get(0)?,
                    bank_id: r.get(1)?,
                    through_sequence: r.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        let id = format!(
            "{:020}-{:020}.json",
            banks[0].slot,
            banks.last().unwrap().slot
        );
        let exists: bool = source.query_row(
            "SELECT EXISTS(SELECT 1 FROM retire_jobs WHERE id=?1)",
            [&id],
            |r| r.get(0),
        )?;
        if exists || session.join("retire-pending").join(&id).exists() {
            continue;
        }
        let marker_path = Path::new(&artifact).join("DIRECT-AGAVE-COMPACT.v1.json");
        crate::retention::enqueue(
            session,
            &crate::retention::Request {
                schema: "hivezilla-direct-agave-retire-v1".into(),
                session_id: session_id.into(),
                marker_path: marker_path.to_string_lossy().into_owned(),
                marker_sha256: digest(&bounded_bytes(&marker_path, 16 * 1024 * 1024)?),
                banks,
            },
        )?;
    }
    Ok(())
}
fn retained_source_contains(artifact: &Path, slot: u64, bank: u64, sha: &str) -> Result<bool> {
    let marker: serde_json::Value = serde_json::from_slice(&bounded_bytes(
        &artifact.join("DIRECT-AGAVE-COMPACT.v1.json"),
        16 * 1024 * 1024,
    )?)?;
    let bytes = if artifact.join("source-events.json.zst").exists() {
        let mut bytes = Vec::new();
        zstd::stream::read::Decoder::new(File::open(artifact.join("source-events.json.zst"))?)?
            .take(512 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= 512 * 1024 * 1024
                && marker["source_events_uncompressed_bytes"] == bytes.len()
                && marker["source_events_uncompressed_sha256"] == digest(&bytes),
            "retained source decompression digest differs"
        );
        bytes
    } else {
        let bytes = bounded_bytes(&artifact.join("source-events.json"), 512 * 1024 * 1024)?;
        let file = marker["files"]
            .as_array()
            .context("missing file list")?
            .iter()
            .find(|f| f["path"] == "source-events.json")
            .context("missing source evidence")?;
        ensure!(
            file["bytes"] == bytes.len() && file["sha256"] == digest(&bytes),
            "retained source digest differs"
        );
        bytes
    };
    let events: Vec<RawEvidence> = serde_json::from_slice(&bytes)?;
    #[derive(Deserialize)]
    struct Key {
        slot: u64,
        bank_id: Option<u64>,
    }
    for event in events {
        if event.sha256 == sha {
            ensure!(
                digest(event.event.get().as_bytes()) == sha,
                "retained event digest differs"
            );
            let key: Key = serde_json::from_str(event.event.get())?;
            if key.slot == slot && key.bank_id == Some(bank) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
