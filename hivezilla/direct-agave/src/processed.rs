//! Complete bank payloads are immutable. Roots select payloads; they never rebuild them.
use crate::{
    event::{REVISION, SCHEMA, digest},
    journal::{atomic_json, open_reader},
    worker::{self, BlockRow, RawEvidence},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const PROFILE: &str = "native-processed-compact-v1";
const MAX_ACTIVE: u64 = 4096;

struct ContextData {
    session: PathBuf,
    output: PathBuf,
    builder: PathBuf,
    meta: Value,
    id: String,
    genesis: String,
    codec: String,
    max_event_bytes: usize,
}

fn database(output: &Path, source_start: u64) -> Result<Connection> {
    let db = Connection::open(output.join("feed.sqlite"))?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS feed_state(id INTEGER PRIMARY KEY CHECK(id=1),source_sequence INTEGER NOT NULL,event_sequence INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS banks(slot INTEGER NOT NULL,bank INTEGER NOT NULL,first_sequence INTEGER NOT NULL,last_sequence INTEGER NOT NULL,root_sequence INTEGER NOT NULL DEFAULT 0,invalid_sequence INTEGER NOT NULL DEFAULT 0,dirty INTEGER NOT NULL DEFAULT 1,state TEXT NOT NULL DEFAULT 'waiting',artifact TEXT,marker_sha TEXT,projection_sha TEXT,through_sequence INTEGER,identity TEXT,PRIMARY KEY(slot,bank));
        CREATE INDEX IF NOT EXISTS bank_work ON banks(dirty,last_sequence);
        CREATE INDEX IF NOT EXISTS bank_state_count ON banks(state);
        CREATE TABLE IF NOT EXISTS dead_slots(slot INTEGER PRIMARY KEY,sequence INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS feed_events(sequence INTEGER PRIMARY KEY,kind TEXT NOT NULL,slot INTEGER NOT NULL,bank INTEGER NOT NULL,artifact TEXT NOT NULL,event_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS feed_bank_events ON feed_events(slot,bank,sequence);
        CREATE TABLE IF NOT EXISTS bank_observations(slot INTEGER NOT NULL,bank INTEGER NOT NULL,frozen_json TEXT,frozen_sequence INTEGER NOT NULL DEFAULT 0,confirmed_json TEXT,confirmed_sequence INTEGER NOT NULL DEFAULT 0,root_observed_ms INTEGER NOT NULL DEFAULT 0,invalid_observed_ms INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(slot,bank));
        CREATE TABLE IF NOT EXISTS bank_controls(source_sequence INTEGER PRIMARY KEY,slot INTEGER NOT NULL,bank INTEGER,kind TEXT NOT NULL,observed_at_unix_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS archive_catalog(slot INTEGER PRIMARY KEY,bank INTEGER NOT NULL,artifact TEXT NOT NULL,marker_sha TEXT NOT NULL,root_sequence INTEGER NOT NULL,custody_path TEXT NOT NULL,custody_sha TEXT NOT NULL,retirement_enqueued INTEGER NOT NULL DEFAULT 0);")?;
    db.execute_batch("CREATE INDEX IF NOT EXISTS archive_pending_retirement ON archive_catalog(retirement_enqueued,slot);")?;
    db.execute(
        "INSERT OR IGNORE INTO feed_state VALUES(1,?1,0)",
        [source_start],
    )?;
    Ok(db)
}

fn identity(row: &BlockRow) -> Value {
    json!({"slot":row.slot,"bank_id":row.bank_id,"blockhash":row.blockhash,
        "parent_slot":row.parent_slot,"parent_blockhash":row.parent_blockhash})
}
fn append_event(
    db: &Connection,
    kind: &str,
    source_sequence: u64,
    bank: &Value,
    marker_sha: &str,
    artifact: &str,
    reason: Option<&str>,
) -> Result<u64> {
    let previous: u64 = db.query_row(
        "SELECT event_sequence FROM feed_state WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    let sequence = previous.checked_add(1).context("event sequence overflow")?;
    ensure!(
        sequence <= 9_007_199_254_740_991,
        "event sequence exceeds portable cursor range"
    );
    let event = json!({"sequence":sequence,"source_sequence":source_sequence,"kind":kind,
        "bank":bank,"payload_sha256":marker_sha,"reason":reason});
    db.execute(
        "INSERT INTO feed_events VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            sequence,
            kind,
            bank["slot"].as_u64().context("missing slot")?,
            bank["bank_id"].as_u64().context("missing bank ID")?,
            artifact,
            event.to_string()
        ],
    )?;
    db.execute(
        "UPDATE feed_state SET event_sequence=?1 WHERE id=1",
        [sequence],
    )?;
    Ok(sequence)
}

/// Persist discovery before building. Restart resumes dirty banks, including same-slot forks.
fn discover(source: &Connection, db: &mut Connection, through: u64) -> Result<()> {
    let after: u64 = db.query_row(
        "SELECT source_sequence FROM feed_state WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    let rows = {
        let mut q=source.prepare("SELECT sequence,slot,bank,kind,CASE WHEN kind IN ('bank_state_v2','rooted','dead','clear') THEN payload END,sha256 FROM events WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT 16384")?;
        q.query_map(params![after, through], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, u64>(1)?,
                r.get::<_, Option<u64>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<Vec<u8>>>(4)?,
                r.get::<_, String>(5)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let tx = db.transaction()?;
    let mut bank_updates = BTreeMap::<(u64, u64), (u64, u64, u64, u64)>::new();
    for (sequence, slot, bank, kind, payload, sha) in &rows {
        if let Some(bank) = bank {
            let entry = bank_updates
                .entry((*slot, *bank))
                .or_insert((*sequence, *sequence, 0, 0));
            entry.1 = *sequence;
            if kind == "rooted" {
                entry.2 = *sequence;
            }
            if matches!(kind.as_str(), "dead" | "clear") {
                entry.3 = *sequence;
            }
        } else if kind == "dead" {
            tx.execute("INSERT INTO dead_slots VALUES(?1,?2) ON CONFLICT(slot) DO UPDATE SET sequence=max(sequence,excluded.sequence)", params![slot,sequence])?;
            tx.execute(
                "UPDATE banks SET dirty=1,invalid_sequence=max(invalid_sequence,?2) WHERE slot=?1",
                params![slot, sequence],
            )?;
        }
        if let Some(payload) = payload {
            let codec = if payload.starts_with(b"{") {
                "json-v1"
            } else {
                "json-zstd-v1"
            };
            let raw = crate::journal::decode_payload(payload, codec, 65536)?;
            ensure!(
                digest(&raw) == *sha,
                "bank observation source digest changed"
            );
            persist_observation(&tx, *sequence, serde_json::from_slice(&raw)?)?;
        }
    }
    // Preserve the same sequence extrema once per bank, instead of writing its
    // row twice for every account/transaction callback in this discovery batch.
    for ((slot, bank), (first, last, root, invalid)) in bank_updates {
        db_bank_update(&tx, slot, bank, first, last, root, invalid)?;
    }
    if let Some((last, ..)) = rows.last() {
        tx.execute(
            "UPDATE feed_state SET source_sequence=?1 WHERE id=1",
            [last],
        )?;
    }
    tx.commit()?;
    let waiting: u64 = db.query_row(
        "SELECT count(*) FROM banks WHERE state='waiting'",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        waiting <= MAX_ACTIVE,
        "more than 4096 unresolved banks; source retained, capture gap or backlog"
    );
    Ok(())
}
fn persist_observation(db: &Connection, sequence: u64, event: crate::event::Event) -> Result<()> {
    use crate::event::Event;
    match event {
        Event::BankStateV2(state) => {
            state.validate()?;
            let old:Option<String> = db.query_row("SELECT coalesce(frozen_json,confirmed_json) FROM bank_observations WHERE slot=?1 AND bank=?2",params![state.slot,state.bank_id],|r|r.get(0)).optional()?.flatten();
            if let Some(old) = old {
                let old: crate::event::NativeBankState = serde_json::from_str(&old)?;
                ensure!(
                    old.bank() == state.bank()
                        && old.block_height == state.block_height
                        && old.last_valid_block_height == state.last_valid_block_height,
                    "contradictory native bank queue tuple"
                );
            }
            let raw = serde_json::to_string(&state)?;
            db.execute(
                "INSERT OR IGNORE INTO bank_observations(slot,bank) VALUES(?1,?2)",
                params![state.slot, state.bank_id],
            )?;
            if state.confirmed {
                db.execute("UPDATE bank_observations SET confirmed_json=coalesce(confirmed_json,?3),confirmed_sequence=CASE WHEN confirmed_sequence=0 THEN ?4 ELSE confirmed_sequence END WHERE slot=?1 AND bank=?2",params![state.slot,state.bank_id,raw,sequence])?;
            } else {
                db.execute("UPDATE bank_observations SET frozen_json=coalesce(frozen_json,?3),frozen_sequence=CASE WHEN frozen_sequence=0 THEN ?4 ELSE frozen_sequence END WHERE slot=?1 AND bank=?2",params![state.slot,state.bank_id,raw,sequence])?;
            }
        }
        Event::BankStatusV2 {
            schema,
            slot,
            bank_id,
            status,
            observed_at_unix_ms,
            ..
        } => {
            ensure!(
                schema == crate::event::BANK_V2
                    && observed_at_unix_ms <= blockzilla_bank_stream_v2::SAFE,
                "invalid native status"
            );
            if !matches!(status.as_str(), "rooted" | "clear" | "dead") {
                return Ok(());
            }
            db.execute(
                "INSERT INTO bank_controls VALUES(?1,?2,?3,?4,?5)",
                params![sequence, slot, bank_id, status, observed_at_unix_ms],
            )?;
            if let Some(bank) = bank_id {
                db.execute(
                    "INSERT OR IGNORE INTO bank_observations(slot,bank) VALUES(?1,?2)",
                    params![slot, bank],
                )?;
                let column = if status == "rooted" {
                    "root_observed_ms"
                } else {
                    "invalid_observed_ms"
                };
                db.execute(&format!("UPDATE bank_observations SET {column}=CASE WHEN {column}=0 THEN ?3 ELSE {column} END WHERE slot=?1 AND bank=?2"),params![slot,bank,observed_at_unix_ms])?;
            } else if status == "dead" {
                db.execute("UPDATE bank_observations SET invalid_observed_ms=CASE WHEN invalid_observed_ms=0 THEN ?2 ELSE invalid_observed_ms END WHERE slot=?1",params![slot,observed_at_unix_ms])?;
            }
        }
        _ => {}
    }
    Ok(())
}
#[cfg(test)]
fn db_bank(db: &Connection, slot: u64, bank: u64, sequence: u64, kind: &str) -> Result<()> {
    let rooted = if kind == "rooted" { sequence } else { 0 };
    let invalid = if matches!(kind, "dead" | "clear") {
        sequence
    } else {
        0
    };
    db_bank_update(db, slot, bank, sequence, sequence, rooted, invalid)
}
fn db_bank_update(
    db: &Connection,
    slot: u64,
    bank: u64,
    first: u64,
    last: u64,
    rooted: u64,
    invalid: u64,
) -> Result<()> {
    db.execute("INSERT INTO banks(slot,bank,first_sequence,last_sequence,root_sequence,invalid_sequence) VALUES(?1,?2,?3,?4,?5,?6)
        ON CONFLICT(slot,bank) DO UPDATE SET last_sequence=excluded.last_sequence,root_sequence=max(root_sequence,excluded.root_sequence),invalid_sequence=max(invalid_sequence,excluded.invalid_sequence),dirty=1",
        params![slot,bank,first,last,rooted,invalid])?;
    db.execute("UPDATE banks SET invalid_sequence=max(invalid_sequence,coalesce((SELECT sequence FROM dead_slots WHERE slot=?1),0)) WHERE slot=?1 AND bank=?2", params![slot,bank])?;
    Ok(())
}

fn projection(bank: &worker::CompleteBank) -> Result<String> {
    // Root/footer records are deliberately absent from the immutable execution identity.
    Ok(digest(&serde_json::to_vec(&(
        &bank.row,
        &bank.block,
        &bank.poh,
        &bank.signatures,
    ))?))
}
fn source_records(
    source: &Connection,
    c: &ContextData,
    slot: u64,
    bank: u64,
    through: u64,
) -> Result<Vec<RawEvidence>> {
    worker::bank_events_through(source, slot, bank, &c.codec, c.max_event_bytes, through)
}
fn read_retained_file(path: &Path, root: &Path, helper: Option<&Path>) -> Result<Vec<u8>> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && helper.is_some() => {
            let helper = helper.unwrap();
            ensure!(
                helper.is_absolute() && path.starts_with(root),
                "unsafe archive restore path"
            );
            let status = Command::new(helper)
                .arg("restore")
                .arg("--output")
                .arg(root)
                .arg("--path")
                .arg(path)
                .status()
                .context("native archive restore helper failed")?;
            ensure!(
                status.success(),
                "native archive restore failed; source remains captured"
            );
            Ok(fs::read(path).context("restored native source is unavailable")?)
        }
        Err(error) => Err(error.into()),
    }
}

fn retained_records(
    db: &Connection,
    source: &Connection,
    c: &ContextData,
    slot: u64,
    bank: u64,
    through: u64,
) -> Result<Vec<RawEvidence>> {
    let proof: Option<(String, String)> = db
        .query_row(
            "SELECT custody_path,custody_sha FROM archive_catalog WHERE slot=?1 AND bank=?2",
            params![slot, bank],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let mut records = BTreeMap::new();
    if let Some((path, sha)) = proof {
        let raw = fs::read(&path)?;
        ensure!(digest(&raw) == sha, "retained custody proof changed");
        let proof: Value = serde_json::from_slice(&raw)?;
        let base = Path::new(&path)
            .parent()
            .context("missing custody directory")?;
        let row = &proof["files"][0];
        let helper = std::env::var_os("HIVEZILLA_NATIVE_ARCHIVE_HELPER").map(PathBuf::from);
        let compressed = read_retained_file(
            &base.join("source-events.json.zst"),
            &c.output,
            helper.as_deref(),
        )?;
        ensure!(
            row["bytes"] == compressed.len() && row["sha256"] == digest(&compressed),
            "retained custody source changed"
        );
        let mut bytes = Vec::new();
        zstd::stream::read::Decoder::new(compressed.as_slice())?
            .take(256 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= 256 * 1024 * 1024,
            "retained source exceeds limit"
        );
        for record in serde_json::from_slice::<Vec<RawEvidence>>(&bytes)? {
            records.insert(record.sequence, record);
        }
    }
    for record in source_records(source, c, slot, bank, through)? {
        if let Some(old) = records.get(&record.sequence) {
            ensure!(
                old.sha256 == record.sha256,
                "retained and live source conflict"
            );
        }
        records.insert(record.sequence, record);
    }
    Ok(records.into_values().collect())
}

struct PendingPayload {
    end: u64,
    id: Value,
    normalized: PathBuf,
    compact: PathBuf,
    marker_sha: String,
    projection: String,
}

#[derive(Default)]
struct PreparedBank {
    complete: Option<worker::CompleteBank>,
    payload: Option<PendingPayload>,
    projection: Option<String>,
    custody: Option<PathBuf>,
}

fn prepare_payload(
    c: &ContextData,
    slot: u64,
    bank: u64,
    mut complete: worker::CompleteBank,
    initial_projection: String,
) -> Result<(worker::CompleteBank, PendingPayload)> {
    let end = complete
        .evidence
        .iter()
        .map(|e| e.sequence)
        .max()
        .context("empty source bank")?;
    let id = identity(&complete.row);
    let base = c
        .output
        .join(format!("epoch-{}/{}-{}", slot / 432000, slot, bank));
    fs::create_dir_all(base.parent().unwrap())?;
    let normalized = base.with_extension("normalized");
    let compact = base.with_extension("compact");
    if !normalized.exists() {
        worker::write_bank_normalized(
            &normalized,
            &c.meta,
            &c.id,
            &c.genesis,
            std::slice::from_mut(&mut complete),
            false,
        )?;
    } else {
        // An orphaned build is bound to its retained source evidence, not a new callback snapshot.
        let receipt: worker::Receipt = serde_json::from_slice(&fs::read(
            normalized.join("DIRECT-AGAVE-NORMALIZED.v1.json"),
        )?)?;
        ensure!(
            receipt.blocks == vec![complete.row.clone()] && !receipt.finalized,
            "pending processed bank identity changed"
        );
        let evidence: Vec<RawEvidence> =
            serde_json::from_slice(&fs::read(normalized.join("evidence/source-events.json"))?)?;
        let pending = worker::assemble_bank(slot, bank, evidence, false)?
            .context("pending processed bank is incomplete")?;
        ensure!(
            projection(&pending)? == initial_projection,
            "pending processed bank execution changed"
        );
    }
    if !compact.exists() {
        let status = Command::new(&c.builder)
            .arg("build-direct-agave-compact")
            .arg(&normalized)
            .arg(&compact)
            .args(["--level", "1"])
            .status()?;
        ensure!(
            status.success(),
            "processed compact writer failed; source and pending stage preserved"
        );
    }
    worker::validate_compact_commit(&compact, &normalized)?;
    let marker_sha = digest(&fs::read(compact.join("DIRECT-AGAVE-COMPACT.v1.json"))?);
    Ok((
        complete,
        PendingPayload {
            end,
            id,
            normalized,
            compact,
            marker_sha,
            projection: initial_projection,
        },
    ))
}

fn commit_payload(
    db: &mut Connection,
    slot: u64,
    bank: u64,
    payload: &PendingPayload,
) -> Result<()> {
    let artifact = payload
        .compact
        .to_str()
        .context("non UTF-8 artifact path")?;
    let tx = db.transaction()?;
    append_event(
        &tx,
        "block",
        payload.end,
        &payload.id,
        &payload.marker_sha,
        artifact,
        None,
    )?;
    tx.execute("UPDATE banks SET state='published',artifact=?3,marker_sha=?4,projection_sha=?5,through_sequence=?6,identity=?7 WHERE slot=?1 AND bank=?2",
        params![slot,bank,artifact,payload.marker_sha,payload.projection,payload.end,payload.id.to_string()])?;
    tx.commit()?;
    // This is an owned conversion stage. The verified compact payload retains
    // the full source evidence; the committed bank no longer needs this duplicate.
    fs::remove_dir_all(&payload.normalized)?;
    Ok(())
}

fn custody(
    c: &ContextData,
    slot: u64,
    bank: u64,
    complete: &worker::CompleteBank,
    artifact: &str,
    marker_sha: &str,
) -> Result<PathBuf> {
    let dir = c.output.join(format!("custody/{slot}-{bank}"));
    fs::create_dir_all(dir.parent().unwrap())?;
    let marker_path = dir.join("CUSTODY.json");
    if marker_path.exists() {
        return Ok(marker_path);
    }
    let stage = dir.with_extension("pending");
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir(&stage)?;
    let bytes = serde_json::to_vec(&complete.evidence)?;
    let compressed = zstd::stream::encode_all(bytes.as_slice(), 1)?;
    let mut file = File::create(stage.join("source-events.json.zst"))?;
    file.write_all(&compressed)?;
    file.sync_all()?;
    let end = complete
        .evidence
        .iter()
        .map(|e| e.sequence)
        .max()
        .context("empty rooted evidence")?;
    atomic_json(
        &stage.join("CUSTODY.json"),
        &json!({"schema":"hivezilla-direct-agave-processed-custody-v1",
        "state":"COMPLETE","session_id":c.id,"finalized":true,"canonical":false,
        "produced_blocks":[complete.row],"through_sequence":end,
        "payload_marker_path":Path::new(artifact).join("DIRECT-AGAVE-COMPACT.v1.json"),"payload_marker_sha256":marker_sha,
        "files":[{"path":"source-events.json.zst","bytes":compressed.len(),"sha256":digest(&compressed)}]}),
    )?;
    File::open(&stage)?.sync_all()?;
    fs::rename(stage, &dir)?;
    File::open(dir.parent().unwrap())?.sync_all()?;
    Ok(marker_path)
}

fn process_banks(
    _source: &Connection,
    db: &mut Connection,
    c: &ContextData,
    through: u64,
) -> Result<()> {
    let keys = {
        let mut q = db.prepare(
            "SELECT slot,bank FROM banks WHERE dirty=1 ORDER BY last_sequence LIMIT 128",
        )?;
        q.query_map([], |r| Ok((r.get::<_, u64>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    // Keep at most four source banks in memory. Decode, normalize, convert and
    // prepare custody on separate workers. Only this writer commits feed state.
    for group in keys.chunks(4) {
        let prepared = std::thread::scope(|scope| -> Result<Vec<PreparedBank>> {
            let handles = group
                .iter()
                .copied()
                .map(|(slot, bank)| {
                    scope.spawn(move || -> Result<_> {
                        let audit = open_reader(&c.output.join("feed.sqlite"))?;
                        let (state,root,invalid,artifact,sha,old_projection):
                            (String,u64,u64,Option<String>,Option<String>,Option<String>) = audit.query_row(
                            "SELECT state,root_sequence,invalid_sequence,artifact,marker_sha,projection_sha FROM banks WHERE slot=?1 AND bank=?2",
                            params![slot, bank],
                            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
                        )?;
                        ensure!(root == 0 || invalid == 0, "bank has rooted and invalidation evidence");
                        if invalid > 0 {
                            return Ok(PreparedBank::default());
                        }
                        let input = open_reader(&c.session.join("events.sqlite"))?;
                        if state == "waiting" {
                            let footer_ready: bool = input.query_row(
                                "SELECT EXISTS(SELECT 1 FROM events WHERE slot=?1 AND bank=?2 AND kind='block' AND sequence<=?3)",
                                params![slot, bank, through], |r| r.get(0))?;
                            if !footer_ready {
                                return Ok(PreparedBank::default());
                            }
                        }
                        let events = retained_records(&audit, &input, c, slot, bank, through)?;
                        let Some(mut complete) = worker::assemble_bank(slot, bank, events, false)? else {
                            return Ok(PreparedBank::default());
                        };
                        let fingerprint = projection(&complete)?;
                        let payload = if state == "waiting" {
                            let (ready, payload) = prepare_payload(c, slot, bank, complete, fingerprint.clone())?;
                            complete = ready;
                            Some(payload)
                        } else {
                            ensure!(old_projection.as_deref() == Some(&fingerprint), "execution changed after processed publication");
                            None
                        };
                        let proof = if root > 0 && matches!(state.as_str(), "waiting" | "published") {
                            let (path, marker_sha) = if let Some(payload) = &payload {
                                (payload.compact.to_str().context("non UTF-8 artifact path")?, payload.marker_sha.as_str())
                            } else {
                                (artifact.as_deref().context("missing artifact")?, sha.as_deref().context("missing marker hash")?)
                            };
                            Some(custody(c, slot, bank, &complete, path, marker_sha)?)
                        } else { None };
                        Ok(PreparedBank { complete: Some(complete), payload, projection: Some(fingerprint), custody: proof })
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .map_err(|_| anyhow::anyhow!("bank reader panicked"))?
                })
                .collect()
        })?;
        for ((slot, bank), prepared) in group.iter().copied().zip(prepared) {
            let (mut state, root, invalid): (String, u64, u64) = db.query_row(
                "SELECT state,root_sequence,invalid_sequence FROM banks WHERE slot=?1 AND bank=?2",
                params![slot, bank],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            ensure!(
                root == 0 || invalid == 0,
                "bank has both rooted and invalidation evidence: {slot}/{bank}"
            );
            if invalid > 0 {
                ensure!(state != "rooted", "rooted bank was invalidated");
                if state == "published" {
                    let (artifact, sha, id): (String, String, String) = db.query_row(
                        "SELECT artifact,marker_sha,identity FROM banks WHERE slot=?1 AND bank=?2",
                        params![slot, bank],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )?;
                    let tx = db.transaction()?;
                    append_event(
                        &tx,
                        "invalidate",
                        invalid,
                        &serde_json::from_str(&id)?,
                        &sha,
                        &artifact,
                        Some("agave_bank_invalidated"),
                    )?;
                    tx.execute(
                        "UPDATE banks SET state='invalidated' WHERE slot=?1 AND bank=?2",
                        params![slot, bank],
                    )?;
                    tx.commit()?;
                } else {
                    db.execute(
                        "UPDATE banks SET state='invalidated' WHERE slot=?1 AND bank=?2",
                        params![slot, bank],
                    )?;
                }
            } else {
                let complete = if state == "waiting" {
                    if let Some(complete) = prepared.complete {
                        let payload = prepared
                            .payload
                            .as_ref()
                            .context("missing prepared payload")?;
                        commit_payload(db, slot, bank, payload)?;
                        state = "published".into();
                        Some(complete)
                    } else {
                        None
                    }
                } else {
                    prepared.complete
                };
                if matches!(state.as_str(), "published" | "rooted") {
                    complete.context("published bank became incomplete")?;
                    let (sha,old_projection,artifact,id):(String,String,String,String)=db.query_row("SELECT marker_sha,projection_sha,artifact,identity FROM banks WHERE slot=?1 AND bank=?2",params![slot,bank],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
                    ensure!(
                        prepared.projection.as_deref() == Some(&old_projection),
                        "execution changed after processed publication"
                    );
                    if root > 0 && state == "published" {
                        let existing: Option<u64> = db
                            .query_row(
                                "SELECT bank FROM archive_catalog WHERE slot=?1",
                                [slot],
                                |r| r.get(0),
                            )
                            .optional()?;
                        ensure!(
                            existing.is_none_or(|b| b == bank),
                            "two different banks rooted at one slot"
                        );
                        let proof = prepared
                            .custody
                            .context("missing prepared rooted custody")?;
                        let tx = db.transaction()?;
                        append_event(
                            &tx,
                            "root",
                            root,
                            &serde_json::from_str(&id)?,
                            &sha,
                            &artifact,
                            None,
                        )?;
                        let custody_sha = digest(&fs::read(&proof)?);
                        tx.execute("INSERT INTO archive_catalog(slot,bank,artifact,marker_sha,root_sequence,custody_path,custody_sha) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![slot,bank,artifact,sha,root,proof.to_str(),custody_sha])?;
                        tx.execute(
                            "UPDATE banks SET state='rooted' WHERE slot=?1 AND bank=?2",
                            params![slot, bank],
                        )?;
                        tx.commit()?;
                    }
                }
            }
            db.execute(
                "UPDATE banks SET dirty=0 WHERE slot=?1 AND bank=?2",
                params![slot, bank],
            )?;
        }
    }
    // Retire only after the new capture writer advertises support for custody proofs.
    if c.meta["processed_custody"] == true {
        let rows = {
            let mut q=db.prepare("SELECT slot,bank,custody_path FROM archive_catalog WHERE retirement_enqueued=0 LIMIT 128")?;
            q.query_map([], |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (slot, bank, path) in rows {
            let bytes = fs::read(&path)?;
            let proof: Value = serde_json::from_slice(&bytes)?;
            crate::retention::enqueue(
                &c.session,
                &crate::retention::Request {
                    schema: "hivezilla-direct-agave-retire-v1".into(),
                    session_id: c.id.clone(),
                    marker_path: path,
                    marker_sha256: digest(&bytes),
                    banks: vec![crate::retention::Bank {
                        slot,
                        bank_id: bank,
                        through_sequence: proof["through_sequence"]
                            .as_u64()
                            .context("missing custody sequence")?,
                    }],
                },
            )?;
            db.execute(
                "UPDATE archive_catalog SET retirement_enqueued=1 WHERE slot=?1",
                [slot],
            )?;
        }
    }
    Ok(())
}

pub fn run(
    session: &Path,
    output: &Path,
    builder: &Path,
    start: Option<u64>,
    follow: bool,
) -> Result<()> {
    fs::create_dir_all(output)?;
    let output = output.canonicalize()?;
    let session = session.canonicalize()?;
    let _lock = crate::journal::exclusive_lock(&output.join("WORKER.lock"))?;
    let result = run_inner(&session, &output, builder, start, follow);
    if let Err(error) = &result {
        let _ = atomic_json(
            &output.join("STATUS.json"),
            &json!({"state":"FAILED","profile":PROFILE,"error":format!("{error:#}"),"source_preserved":true}),
        );
    }
    result
}
fn run_inner(
    session: &Path,
    output: &Path,
    builder: &Path,
    start: Option<u64>,
    follow: bool,
) -> Result<()> {
    let meta: Value = serde_json::from_slice(&fs::read(session.join("SESSION.json"))?)?;
    ensure!(
        meta["schema"] == SCHEMA && meta["agave_revision"] == REVISION,
        "unsupported source journal"
    );
    let id = meta["session_id"]
        .as_str()
        .context("missing session")?
        .to_owned();
    let genesis = meta["config"]["genesis_hash"]
        .as_str()
        .context("missing genesis")?
        .to_owned();
    ensure!(
        genesis == "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
        "processed worker currently supports mainnet epoch geometry only"
    );
    let codec = meta["payload_codec"]
        .as_str()
        .unwrap_or("json-v1")
        .to_owned();
    let max_event_bytes = meta["config"]["max_event_bytes"]
        .as_u64()
        .context("missing event bound")? as usize;
    ensure!(max_event_bytes <= 16 * 1024 * 1024, "invalid event bound");
    let source = open_reader(&session.join("events.sqlite"))?;
    if !output.join("FEED.json").exists() && start.is_none() {
        let durable: u64 =
            source.query_row("SELECT durable_sequence FROM status WHERE id=1", [], |r| {
                r.get(0)
            })?;
        let first: Option<u64> =
            source.query_row("SELECT min(sequence) FROM events", [], |r| r.get(0))?;
        ensure!(
            first == Some(1) || (first.is_none() && durable == 0),
            "source prefix was retired; specify --start-sequence or use a fresh capture session"
        );
    }
    let header = json!({"schema":"hivezilla-processed-feed-v1","profile":PROFILE,"session_id":id,"genesis_hash":genesis,
        "node_identity":meta["config"]["node_identity"],"agave_revision":REVISION,"start_source_sequence":start.unwrap_or(0),
        "processed_custody":meta["processed_custody"]==true,"canonical":false,"publication_ready":false});
    let header_path = output.join("FEED.json");
    if header_path.exists() {
        let previous: Value = serde_json::from_slice(&fs::read(&header_path)?)?;
        for k in [
            "schema",
            "profile",
            "session_id",
            "genesis_hash",
            "node_identity",
            "agave_revision",
        ] {
            ensure!(previous[k] == header[k], "processed feed identity changed");
        }
        if let Some(start) = start {
            ensure!(
                previous["start_source_sequence"] == start,
                "source start changed"
            );
        }
    } else {
        atomic_json(&header_path, &header)?;
    }
    let durable: u64 =
        source.query_row("SELECT durable_sequence FROM status WHERE id=1", [], |r| {
            r.get(0)
        })?;
    ensure!(
        start.unwrap_or(0) <= durable,
        "source start exceeds durable position"
    );
    let mut db = database(output, start.unwrap_or(0))?;
    let c = ContextData {
        session: session.into(),
        output: output.into(),
        builder: builder.into(),
        meta,
        id,
        genesis,
        codec,
        max_event_bytes,
    };
    let snapshot: u64 =
        source.query_row("SELECT durable_sequence FROM status WHERE id=1", [], |r| {
            r.get(0)
        })?;
    loop {
        ensure!(
            !session.join("RETIREMENT-FAILED.json").exists(),
            "source retirement failed"
        );
        let failure: Value = serde_json::from_slice(&fs::read(session.join("FAILURE.json"))?)?;
        ensure!(failure.get("state").is_none(), "capture failed: {failure}");
        let (state, through): (String, u64) = source.query_row(
            "SELECT state,durable_sequence FROM status WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(state != "FAILED", "capture failed");
        ensure!(
            state != "CAPTURING"
                || crate::journal::exclusive_lock(&session.join("LIVE.lock")).is_err(),
            "capture ended without a clean close"
        );
        let ceiling = if follow { through } else { snapshot };
        discover(&source, &mut db, ceiling)?;
        let scanned: u64 = db.query_row(
            "SELECT source_sequence FROM feed_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        process_banks(&source, &mut db, &c, scanned)?;
        let (source_sequence, event_sequence): (u64, u64) = db.query_row(
            "SELECT source_sequence,event_sequence FROM feed_state WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let last_compact_slot: Option<u64> = db.query_row(
            "SELECT max(slot) FROM banks WHERE state IN ('published','rooted')",
            [],
            |r| r.get(0),
        )?;
        atomic_json(
            &output.join("STATUS.json"),
            &json!({"state":if state=="CLOSED"{"DRAINED"}else{"RUNNING"},"profile":PROFILE,
            "source_sequence":source_sequence,"event_sequence":event_sequence,"last_compact_slot":last_compact_slot,"retirement_supported":c.meta["processed_custody"]==true,
            "updated_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,"canonical":false,"publication_ready":false}),
        )?;
        let dirty: u64 =
            db.query_row("SELECT count(*) FROM banks WHERE dirty=1", [], |r| r.get(0))?;
        if ((!follow && source_sequence >= ceiling)
            || (state == "CLOSED" && source_sequence >= through))
            && dirty == 0
        {
            return Ok(());
        }
        if source_sequence >= through && dirty == 0 {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discovery_batches_callbacks_without_losing_fork_root_or_dead_boundaries() {
        let t = tempfile::tempdir().unwrap();
        let source = Connection::open_in_memory().unwrap();
        source.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY,slot INTEGER,bank INTEGER,kind TEXT,payload BLOB,sha256 TEXT);").unwrap();
        for (seq, slot, bank, kind) in [
            (1, 10, Some(1), "account"),
            (2, 10, Some(1), "transaction"),
            (3, 10, Some(2), "transaction"),
            (4, 10, Some(1), "rooted"),
            (5, 10, Some(2), "clear"),
            (6, 11, None, "dead"),
            (7, 11, Some(3), "transaction"),
            (8, 10, Some(1), "account"),
        ] {
            source
                .execute(
                    "INSERT INTO events VALUES(?1,?2,?3,?4,NULL,'unused')",
                    params![seq, slot, bank, kind],
                )
                .unwrap();
        }
        let mut db = database(t.path(), 0).unwrap();
        discover(&source, &mut db, 8).unwrap();
        let row = |slot: u64, bank: u64| -> (u64, u64, u64, u64, u64) {
            db.query_row("SELECT first_sequence,last_sequence,root_sequence,invalid_sequence,dirty FROM banks WHERE slot=?1 AND bank=?2",params![slot,bank],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap()
        };
        assert_eq!(row(10, 1), (1, 8, 4, 0, 1));
        assert_eq!(row(10, 2), (3, 5, 0, 5, 1));
        assert_eq!(row(11, 3), (7, 7, 0, 6, 1));
        assert_eq!(
            db.query_row("SELECT source_sequence FROM feed_state", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            8
        );
        discover(&source, &mut db, 8).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT last_sequence FROM banks WHERE slot=10 AND bank=1",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
            8
        );
    }
    #[test]
    fn archived_native_evidence_can_be_restored_before_late_callback_audit() {
        use std::os::unix::fs::PermissionsExt;
        let t = tempfile::tempdir().unwrap();
        let helper = t.path().join("restore");
        fs::write(&helper, "#!/bin/sh\nprintf recovered > \"$5\"\n").unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
        let path = t.path().join("source-events.json.zst");
        assert_eq!(
            read_retained_file(&path, t.path(), Some(&helper)).unwrap(),
            b"recovered"
        );
        assert_eq!(fs::read(path).unwrap(), b"recovered");
    }
    #[test]
    fn warm_native_evidence_does_not_call_archive_helper() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("source-events.json.zst");
        fs::write(&path, b"warm").unwrap();
        assert_eq!(
            read_retained_file(&path, t.path(), Some(Path::new("/missing-helper"))).unwrap(),
            b"warm"
        );
        fs::remove_file(&path).unwrap();
        assert!(read_retained_file(&path, t.path(), None).is_err());
    }
    fn insert_source(db: &Connection, sequence: u64, event: crate::event::Event) {
        let raw = serde_json::to_vec(&event).unwrap();
        let (slot, bank, kind) = event.key();
        db.execute(
            "INSERT INTO events VALUES(?1,?2,?3,?4,?5,?6)",
            params![sequence, slot, bank, kind, digest(&raw), raw],
        )
        .unwrap();
        db.execute("UPDATE status SET durable_sequence=?1", [sequence])
            .unwrap();
    }
    #[test]
    #[ignore = "requires BLOCKZILLA_BUILDER; run on the remote build host"]
    fn roots_reuse_payload_and_restart_retains_controls_and_late_source() {
        use crate::event::Event;
        let t = tempfile::tempdir().unwrap();
        let session = t.path().join("source");
        let output = t.path().join("output");
        fs::create_dir(&session).unwrap();
        fs::create_dir(session.join("retire-pending")).unwrap();
        fs::create_dir(&output).unwrap();
        let source = Connection::open(session.join("events.sqlite")).unwrap();
        source.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY,slot INTEGER,bank INTEGER,kind TEXT,sha256 TEXT,payload BLOB); CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER,retired_events INTEGER,latest_rooted_slot INTEGER); INSERT INTO status VALUES(1,'CLOSED',0,0,0);CREATE TABLE retire_jobs(id TEXT PRIMARY KEY,bank_index INTEGER,deleted_events INTEGER,marker_sha TEXT);").unwrap();
        let builder = PathBuf::from(std::env::var("BLOCKZILLA_BUILDER").unwrap());
        let c = ContextData {
            session: session.clone(),
            output: output.clone(),
            builder,
            meta: json!({"config":{"node_identity":"J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ"},"processed_custody":true}),
            id: "test".into(),
            genesis: "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),
            codec: "json-v1".into(),
            max_event_bytes: 16 * 1024 * 1024,
        };
        let mut db = database(&output, 0).unwrap();
        let mut sequence = 0;
        for bank in [7, 8] {
            for event in [
                Event::Status {
                    slot: 10,
                    bank_id: Some(bank),
                    parent: Some(9),
                    status: "processed".into(),
                    dead_error: None,
                },
                Event::Entry {
                    slot: 10,
                    bank_id: bank,
                    index: 0,
                    num_hashes: 1,
                    hash: [bank as u8; 32],
                    transactions: 0,
                    starting_transaction_index: 0,
                },
                Event::Block {
                    slot: 10,
                    bank_id: bank,
                    parent_slot: 9,
                    parent_blockhash: [6; 32],
                    blockhash: [bank as u8; 32],
                    transactions: 0,
                    entries: 1,
                    block_time: None,
                    block_height: None,
                    native_rewards: json!({"rewards":[]}),
                    rewards: blockzilla_archive_v2::WincodeArchiveV2NoRegistryRewards {
                        source_len: 0,
                        num_partitions: None,
                        decoded: Some(vec![]),
                        raw_fallback: None,
                        decode_error: None,
                    },
                },
            ] {
                sequence += 1;
                insert_source(&source, sequence, event);
            }
        }
        discover(&source, &mut db, sequence).unwrap();
        process_banks(&source, &mut db, &c, sequence).unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM feed_events", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        let path: String = db
            .query_row("SELECT artifact FROM banks WHERE bank=8", [], |r| r.get(0))
            .unwrap();
        let payload_before =
            fs::read(Path::new(&path).join("DIRECT-AGAVE-COMPACT.v1.json")).unwrap();
        // No converter can run from this point onward. Rooting must reuse the bytes.
        let c = ContextData {
            builder: t.path().join("missing-converter"),
            ..c
        };
        insert_source(
            &source,
            7,
            Event::ClearBank {
                slot: 10,
                bank_id: 7,
                parent_slot: 9,
                parent_blockhash: [6; 32],
            },
        );
        insert_source(
            &source,
            8,
            Event::Status {
                slot: 10,
                bank_id: Some(8),
                parent: Some(9),
                status: "rooted".into(),
                dead_error: None,
            },
        );
        discover(&source, &mut db, 8).unwrap();
        process_banks(&source, &mut db, &c, 8).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT group_concat(kind,',') FROM feed_events ORDER BY sequence",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "block,block,invalidate,root"
        );
        assert_eq!(
            fs::read(Path::new(&path).join("DIRECT-AGAVE-COMPACT.v1.json")).unwrap(),
            payload_before
        );
        assert_eq!(
            db.query_row("SELECT bank FROM archive_catalog", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            8
        );
        // Full source custody is verified before exact rooted source retirement.
        crate::retention::process_one(&session, &mut { source }, "test").unwrap();
        let source = Connection::open(session.join("events.sqlite")).unwrap();
        assert_eq!(
            source
                .query_row("SELECT count(*) FROM events WHERE bank=8", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        drop(db);
        let mut db = database(&output, 0).unwrap();
        insert_source(
            &source,
            9,
            Event::Footer {
                slot: 10,
                bank_id: 8,
                wire_footer: vec![1, 2, 3],
            },
        );
        discover(&source, &mut db, 9).unwrap();
        process_banks(&source, &mut db, &c, 9).unwrap();
        assert_eq!(
            db.query_row("SELECT event_sequence FROM feed_state", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            4
        );
        // A changed execution callback after publication fails without a second payload.
        insert_source(
            &source,
            10,
            Event::Entry {
                slot: 10,
                bank_id: 8,
                index: 0,
                num_hashes: 2,
                hash: [8; 32],
                transactions: 0,
                starting_transaction_index: 0,
            },
        );
        discover(&source, &mut db, 10).unwrap();
        assert!(process_banks(&source, &mut db, &c, 10).is_err());
    }
    #[test]
    #[ignore = "requires the real compact writer on Foundation"]
    fn native_v2_early_confirmation_waits_and_restart_reuses_original_payload() {
        use crate::event::{BANK_V2, Event, NativeBankState};
        let t = tempfile::tempdir().unwrap();
        let session = t.path().join("source");
        let output = t.path().join("processed");
        let v2out = t.path().join("v2");
        fs::create_dir_all(session.join("retire-pending")).unwrap();
        fs::create_dir(&output).unwrap();
        let source = Connection::open(session.join("events.sqlite")).unwrap();
        source.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY,slot INTEGER,bank INTEGER,kind TEXT,sha256 TEXT,payload BLOB);CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER,retired_events INTEGER,latest_rooted_slot INTEGER);INSERT INTO status VALUES(1,'CAPTURING',0,0,0);").unwrap();
        let c = ContextData {
            session: session.clone(),
            output: output.clone(),
            builder: PathBuf::from(std::env::var("BLOCKZILLA_BUILDER").unwrap()),
            meta: json!({"config":{"node_identity":"J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ"},"processed_custody":false}),
            id: "native-v2-test".into(),
            genesis: "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),
            codec: "json-v1".into(),
            max_event_bytes: 16 * 1024 * 1024,
        };
        atomic_json(&output.join("FEED.json"),&json!({"schema":"hivezilla-processed-feed-v1","profile":PROFILE,"session_id":c.id,"genesis_hash":c.genesis,"node_identity":c.meta["config"]["node_identity"],"agave_revision":REVISION})).unwrap();
        atomic_json(&output.join("STATUS.json"),&json!({"state":"RUNNING","updated_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64})).unwrap();
        let mut db = database(&output, 0).unwrap();
        let mut n = 0;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let tuple = |slot, bank, parent, hash, prev, height, confirmed| NativeBankState {
            schema: BANK_V2.into(),
            slot,
            bank_id: bank,
            parent_slot: parent,
            blockhash: [hash; 32],
            parent_blockhash: [prev; 32],
            block_height: height,
            last_valid_block_height: 166,
            observed_at_unix_ms: now,
            confirmed,
        };
        let status = |slot, bank, parent, status: &str| Event::BankStatusV2 {
            schema: BANK_V2.into(),
            slot,
            bank_id: Some(bank),
            parent: Some(parent),
            parent_blockhash: None,
            status: status.into(),
            dead_error: None,
            observed_at_unix_ms: now,
        };
        let entry = |slot, bank, hash| Event::Entry {
            slot,
            bank_id: bank,
            index: 0,
            num_hashes: 1,
            hash: [hash; 32],
            transactions: 0,
            starting_transaction_index: 0,
        };
        let block = |slot, bank, parent, hash, prev, height| Event::Block {
            slot,
            bank_id: bank,
            parent_slot: parent,
            parent_blockhash: [prev; 32],
            blockhash: [hash; 32],
            transactions: 0,
            entries: 1,
            block_time: None,
            block_height: Some(height),
            native_rewards: json!({"rewards":[]}),
            rewards: blockzilla_archive_v2::WincodeArchiveV2NoRegistryRewards {
                source_len: 0,
                num_partitions: None,
                decoded: Some(vec![]),
                raw_fallback: None,
                decode_error: None,
            },
        };
        for e in [
            Event::BankStateV2(tuple(10, 7, 9, 7, 6, 15, false)),
            status(10, 7, 9, "processed"),
            entry(10, 7, 7),
            block(10, 7, 9, 7, 6, 15),
            status(10, 7, 9, "rooted"),
        ] {
            n += 1;
            insert_source(&source, n, e);
        }
        discover(&source, &mut db, n).unwrap();
        process_banks(&source, &mut db, &c, n).unwrap();
        crate::confirmed::run(&output, &v2out, false).unwrap();
        let a = Connection::open(v2out.join("feed.sqlite")).unwrap();
        assert_eq!(
            a.query_row("SELECT event_sequence FROM feed_state", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        drop(a);
        // Native confirmation is durable while the complete-bank gate waits.
        for e in [
            Event::BankStateV2(tuple(17, 8, 10, 8, 7, 16, false)),
            Event::BankStateV2(tuple(17, 8, 10, 8, 7, 16, true)),
            status(17, 8, 10, "processed"),
        ] {
            n += 1;
            insert_source(&source, n, e);
        }
        discover(&source, &mut db, n).unwrap();
        process_banks(&source, &mut db, &c, n).unwrap();
        crate::confirmed::run(&output, &v2out, false).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM bank_observations WHERE confirmed_json IS NOT NULL",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM feed_events", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        for e in [entry(17, 8, 8), block(17, 8, 10, 8, 7, 16)] {
            n += 1;
            insert_source(&source, n, e);
        }
        discover(&source, &mut db, n).unwrap();
        process_banks(&source, &mut db, &c, n).unwrap();
        crate::confirmed::run(&output, &v2out, false).unwrap();
        let a = Connection::open(v2out.join("feed.sqlite")).unwrap();
        let events: Vec<String> = a
            .prepare("SELECT event_json FROM feed_events ORDER BY sequence")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(events.len(), 4);
        let full: blockzilla_bank_stream_v2::BankEvent = serde_json::from_str(&events[2]).unwrap();
        let confirm: blockzilla_bank_stream_v2::BankEvent =
            serde_json::from_str(&events[3]).unwrap();
        assert_eq!(confirm.kind, "confirmed");
        assert_eq!(confirm.bank, full.bank);
        assert_eq!(
            confirm.recent_blockhash.unwrap().last_valid_block_height,
            166
        );
        let frames: Vec<String> = a
            .prepare("SELECT frame_sha256 FROM feed_events ORDER BY sequence")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        drop(a);
        // Simulate a crash after block/control commit but before its input position.
        let a = Connection::open(v2out.join("feed.sqlite")).unwrap();
        a.execute("UPDATE feed_state SET processed_sequence=2", [])
            .unwrap();
        drop(a);
        crate::confirmed::run(&output, &v2out, false).unwrap();
        let a = Connection::open(v2out.join("feed.sqlite")).unwrap();
        let again: Vec<String> = a
            .prepare("SELECT frame_sha256 FROM feed_events ORDER BY sequence")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(again, frames);
    }
    #[test]
    fn same_slot_banks_and_controls_have_distinct_durable_positions() {
        let tmp = tempfile::tempdir().unwrap();
        let mut db = database(tmp.path(), 0).unwrap();
        db_bank(&db, 10, 7, 1, "status").unwrap();
        db_bank(&db, 10, 8, 2, "status").unwrap();
        let bank = |n| json!({"slot":10,"bank_id":n,"blockhash":vec![0;32],"parent_slot":9,"parent_blockhash":vec![0;32]});
        let tx = db.transaction().unwrap();
        assert_eq!(
            append_event(&tx, "block", 3, &bank(7), "abc", "a", None).unwrap(),
            1
        );
        assert_eq!(
            append_event(&tx, "block", 4, &bank(8), "def", "b", None).unwrap(),
            2
        );
        assert_eq!(
            append_event(&tx, "invalidate", 5, &bank(7), "abc", "a", Some("clear")).unwrap(),
            3
        );
        assert_eq!(
            append_event(&tx, "root", 6, &bank(8), "def", "b", None).unwrap(),
            4
        );
        tx.commit().unwrap();
        drop(db);
        let db = database(tmp.path(), 0).unwrap();
        assert_eq!(
            db.query_row("SELECT event_sequence FROM feed_state", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            4
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM banks", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            2
        );
    }
    #[test]
    fn control_and_position_commit_together() {
        let tmp = tempfile::tempdir().unwrap();
        let mut db = database(tmp.path(), 0).unwrap();
        {
            let tx = db.transaction().unwrap();
            append_event(
                &tx,
                "invalidate",
                2,
                &json!({"slot":10,"bank_id":7}),
                "abc",
                "a",
                None,
            )
            .unwrap();
        }
        assert_eq!(
            db.query_row("SELECT event_sequence FROM feed_state", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM feed_events", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
}
