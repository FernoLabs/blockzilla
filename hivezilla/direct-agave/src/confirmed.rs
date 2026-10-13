//! v2 selects the existing complete payloads. It never executes or compacts a bank again.
use crate::{
    event::NativeBankState,
    journal::{atomic_json, open_reader},
};
use anyhow::{Context, Result, ensure};
use blockzilla_bank_stream_v2::{
    self as v2, BankEvent, BankIdentity, Frame, SourceIdentity, artifact, chain::Chain,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
struct Producer {
    db: Connection,
    input: Connection,
    payload_root: PathBuf,
    source: SourceIdentity,
    chain: Chain,
}
impl Producer {
    fn open(input: &Path, output: &Path) -> Result<Self> {
        let payload_root = input.canonicalize()?;
        let header: Value =
            serde_json::from_slice(&artifact::bounded_file(&input.join("FEED.json"), 65536)?)?;
        ensure!(
            header["schema"] == "hivezilla-processed-feed-v1"
                && header["profile"] == "native-processed-compact-v1",
            "unsupported_processed_source"
        );
        let source = SourceIdentity::from_marker(&header)?;
        let db = Connection::open(output.join("feed.sqlite"))?;
        db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS feed_state(id INTEGER PRIMARY KEY CHECK(id=1),processed_sequence INTEGER NOT NULL,event_sequence INTEGER NOT NULL,chain_json TEXT NOT NULL);
          INSERT OR IGNORE INTO feed_state VALUES(1,0,0,'{\"banks\":{},\"latest\":null,\"boundary\":null,\"reconciliate\":false}');
          CREATE TABLE IF NOT EXISTS feed_events(sequence INTEGER PRIMARY KEY,artifact TEXT NOT NULL,event_json TEXT NOT NULL,frame_sha256 TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS event_artifact ON feed_events(artifact,sequence);
          CREATE TABLE IF NOT EXISTS publication_times(sequence INTEGER PRIMARY KEY,published_at_unix_ms INTEGER NOT NULL);
          CREATE UNIQUE INDEX IF NOT EXISTS frame_lookup ON feed_events(frame_sha256);
          CREATE TABLE IF NOT EXISTS banks(slot INTEGER NOT NULL,bank INTEGER NOT NULL,artifact TEXT NOT NULL,payload_event TEXT NOT NULL,frame_sha256 TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'published',confirmed_sequence INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(slot,bank));
          CREATE INDEX IF NOT EXISTS bank_pending ON banks(state,confirmed_sequence);")?;
        let chain = serde_json::from_str(&db.query_row(
            "SELECT chain_json FROM feed_state WHERE id=1",
            [],
            |r| r.get::<_, String>(0),
        )?)?;
        let h = json!({"schema":"hivezilla-bank-feed-v2","profile":v2::PROFILE,"session_id":source.session_id,"genesis_hash":source.genesis_hash,"node_identity":source.node_identity,"agave_revision":source.agave_revision,"payload_root":payload_root,"canonical":false,"publication_ready":false});
        if output.join("FEED.json").exists() {
            ensure!(
                serde_json::from_slice::<Value>(&fs::read(output.join("FEED.json"))?)? == h,
                "source_changed"
            );
        } else {
            atomic_json(&output.join("FEED.json"), &h)?;
        }
        let input = open_reader(&input.join("feed.sqlite"))?;
        Ok(Self {
            db,
            input,
            payload_root,
            source,
            chain,
        })
    }
    fn sequence(&self) -> Result<u64> {
        Ok(self.db.query_row(
            "SELECT event_sequence+1 FROM feed_state WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }
    fn stored_payload(&self, bank: &BankIdentity) -> Result<(String, BankEvent)> {
        let (path, event): (String, String) = self.db.query_row(
            "SELECT artifact,payload_event FROM banks WHERE slot=?1 AND bank=?2",
            params![bank.slot, bank.bank_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok((path, serde_json::from_str(&event)?))
    }
    fn commit(&mut self, frame: Frame, path: &str) -> Result<()> {
        let e = &frame.envelope.event;
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO feed_events VALUES(?1,?2,?3,?4)",
            params![e.sequence, path, serde_json::to_string(e)?, frame.sha256],
        )?;
        if e.kind == "block" {
            tx.execute("INSERT INTO banks(slot,bank,artifact,payload_event,frame_sha256) VALUES(?1,?2,?3,?4,?5)",params![e.bank.slot,e.bank.bank_id,path,serde_json::to_string(e)?,frame.sha256])?;
        } else {
            let state = if e.kind == "root" {
                "rooted"
            } else if e.kind == "invalidate" {
                "invalidated"
            } else {
                "published"
            };
            tx.execute("UPDATE banks SET state=?3,confirmed_sequence=CASE WHEN ?4='confirmed' THEN ?5 ELSE confirmed_sequence END WHERE slot=?1 AND bank=?2",params![e.bank.slot,e.bank.bank_id,state,e.kind,e.source_sequence])?;
        }
        tx.execute(
            "UPDATE feed_state SET event_sequence=?1,chain_json=?2 WHERE id=1",
            params![e.sequence, serde_json::to_string(&self.chain)?],
        )?;
        tx.commit()?;
        // Operator timing is separate from immutable callback time and frame bytes.
        self.db.execute(
            "INSERT OR IGNORE INTO publication_times VALUES(?1,?2)",
            params![e.sequence, now()],
        )?;
        Ok(())
    }
    fn observation(
        &self,
        bank: &BankIdentity,
        confirmed: bool,
    ) -> Result<Option<(NativeBankState, u64)>> {
        let column = if confirmed { "confirmed" } else { "frozen" };
        let row:Option<(Option<String>,u64)>=self.input.query_row(&format!("SELECT {column}_json,{column}_sequence FROM bank_observations WHERE slot=?1 AND bank=?2"),params![bank.slot,bank.bank_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let Some((Some(raw), sequence)) = row else {
            return Ok(None);
        };
        let state: NativeBankState = serde_json::from_str(&raw)?;
        state.validate()?;
        ensure!(
            sequence > 0 && state.bank() == *bank && state.confirmed == confirmed,
            "native_bank_identity_mismatch"
        );
        v2::clock_check(state.observed_at_unix_ms, now())?;
        Ok(Some((state, sequence)))
    }
    fn block(&mut self, path: &str, input_event: &Value) -> Result<()> {
        let bank: BankIdentity = serde_json::from_value(input_event["bank"].clone())?;
        let (state, source_sequence) = self
            .observation(&bank, false)?
            .context("complete bank lacks native queue tuple; dependency_unavailable")?;
        let e = BankEvent {
            sequence: self.sequence()?,
            source_sequence,
            kind: "block".into(),
            bank,
            payload_sha256: input_event["payload_sha256"]
                .as_str()
                .context("missing marker digest")?
                .into(),
            payload_frame_sha256: None,
            commitment: Some("processed".into()),
            confirmation_basis: Some("operator-trusted-agave-processed-bank-callback".into()),
            block_height: Some(state.block_height),
            recent_blockhash: None,
            observed_at_unix_ms: state.observed_at_unix_ms,
            reason: None,
        };
        let frame = artifact::build(&self.payload_root, Path::new(path), &self.source, e)?;
        self.chain.add_payload(&frame)?;
        self.commit(frame, path)
    }
    fn control(
        &mut self,
        kind: &str,
        bank: &BankIdentity,
        sequence: u64,
        observed: u64,
        state: Option<NativeBankState>,
    ) -> Result<()> {
        let (path, mut e) = self.stored_payload(bank)?;
        let payload = artifact::build(
            &self.payload_root,
            Path::new(&path),
            &self.source,
            e.clone(),
        )?;
        e.sequence = self.sequence()?;
        e.source_sequence = sequence;
        e.kind = kind.into();
        e.payload_frame_sha256 = Some(payload.sha256.clone());
        e.observed_at_unix_ms = observed;
        ensure!(observed > 0, "control lacks native observation time");
        v2::clock_check(observed, now())?;
        if kind == "invalidate" {
            e.commitment = None;
            e.confirmation_basis = None;
            e.block_height = None;
            e.recent_blockhash = None;
            e.reason = Some("agave_bank_invalidated".into());
        } else {
            let state = state.context("missing native expiry")?;
            ensure!(
                state.bank() == *bank && Some(state.block_height) == e.block_height,
                "control native identity differs from payload"
            );
            e.recent_blockhash = Some(state.recent());
            e.reason = None;
            e.commitment = Some(
                if kind == "confirmed" {
                    "confirmed"
                } else {
                    "finalized"
                }
                .into(),
            );
            e.confirmation_basis = Some(
                if kind == "confirmed" {
                    "operator-trusted-agave-confirmed-bank-callback"
                } else {
                    "operator-trusted-agave-rooted-bank-callback"
                }
                .into(),
            );
        }
        let frame = artifact::build(&self.payload_root, Path::new(&path), &self.source, e)?;
        self.chain.apply_control(&frame, &payload)?;
        self.commit(frame, &path)?;
        ensure!(
            !self.chain.reconciliate,
            "confirmed bank invalidated; reconciliation required"
        );
        Ok(())
    }
    fn confirmed(&mut self, bank: &BankIdentity) -> Result<()> {
        let (seen, state): (u64, String) = self.db.query_row(
            "SELECT confirmed_sequence,state FROM banks WHERE slot=?1 AND bank=?2",
            params![bank.slot, bank.bank_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if seen > 0 || state != "published" || self.chain.can_confirm(bank).is_err() {
            return Ok(());
        }
        if let Some((state, sequence)) = self.observation(bank, true)? {
            self.control(
                "confirmed",
                bank,
                sequence,
                state.observed_at_unix_ms,
                Some(state),
            )?;
        }
        Ok(())
    }
    fn step(&mut self) -> Result<bool> {
        let after: u64 = self.db.query_row(
            "SELECT processed_sequence FROM feed_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let row:Option<(u64,String,String)>=self.input.query_row("SELECT sequence,artifact,event_json FROM feed_events WHERE sequence>?1 ORDER BY sequence LIMIT 1",[after],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((sequence, path, raw)) = row else {
            return Ok(false);
        };
        ensure!(sequence == after + 1, "processed event gap");
        let event: Value = serde_json::from_str(&raw)?;
        let bank: BankIdentity = serde_json::from_value(event["bank"].clone())?;
        match event["kind"].as_str().context("missing event kind")? {
            "block" => {
                let old: Option<(String, String)> = self
                    .db
                    .query_row(
                        "SELECT artifact,payload_event FROM banks WHERE slot=?1 AND bank=?2",
                        params![bank.slot, bank.bank_id],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                if let Some((old_path, old)) = old {
                    let old: BankEvent = serde_json::from_str(&old)?;
                    ensure!(
                        old_path == path
                            && old.bank == bank
                            && event["payload_sha256"] == old.payload_sha256,
                        "replayed payload changed"
                    );
                } else {
                    self.block(&path, &event)?;
                }
            }
            kind @ ("root" | "invalidate") => {
                let current: String = self.db.query_row(
                    "SELECT state FROM banks WHERE slot=?1 AND bank=?2",
                    params![bank.slot, bank.bank_id],
                    |r| r.get(0),
                )?;
                if (kind == "root" && current == "rooted")
                    || (kind == "invalidate" && current == "invalidated")
                {
                    self.db.execute(
                        "UPDATE feed_state SET processed_sequence=?1 WHERE id=1",
                        [sequence],
                    )?;
                    return Ok(true);
                }
                self.confirmed(&bank)?;
                let native_sequence = event["source_sequence"]
                    .as_u64()
                    .context("missing source sequence")?;
                let (slot,id,status,observed):(u64,Option<u64>,String,u64)=self.input.query_row("SELECT slot,bank,kind,observed_at_unix_ms FROM bank_controls WHERE source_sequence=?1",[native_sequence],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
                ensure!(
                    slot == bank.slot
                        && id.is_none_or(|id| id == bank.bank_id)
                        && (if kind == "root" {
                            status == "rooted"
                        } else {
                            matches!(status.as_str(), "dead" | "clear")
                        }),
                    "native_control_index_mismatch"
                );
                let state = if kind == "root" {
                    Some(
                        self.observation(&bank, false)?
                            .context("root lacks native expiry")?
                            .0,
                    )
                } else {
                    None
                };
                self.control(kind, &bank, native_sequence, observed, state)?;
            }
            _ => anyhow::bail!("unsupported processed event"),
        }
        self.db.execute(
            "UPDATE feed_state SET processed_sequence=?1 WHERE id=1",
            [sequence],
        )?;
        Ok(true)
    }
    fn flush_confirmed(&mut self) -> Result<()> {
        let rows = {
            let mut q=self.db.prepare("SELECT payload_event FROM banks WHERE state='published' AND confirmed_sequence=0 ORDER BY slot,bank LIMIT 4096")?;
            q.query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        ensure!(rows.len() < 4096, "pending confirmation queue overflow");
        for raw in rows {
            let event: BankEvent = serde_json::from_str(&raw)?;
            self.confirmed(&event.bank)?;
        }
        Ok(())
    }
}
pub fn run(input: &Path, output: &Path, follow: bool) -> Result<()> {
    fs::create_dir_all(output)?;
    let _lock = crate::journal::exclusive_lock(&output.join("WORKER.lock"))?;
    let result = (|| {
        let mut p = Producer::open(input, output)?;
        ensure!(
            !p.chain.reconciliate,
            "confirmed bank invalidated; reconciliation required"
        );
        loop {
            let status: Value = serde_json::from_slice(&artifact::bounded_file(
                &input.join("STATUS.json"),
                65536,
            )?)?;
            ensure!(
                status["state"] != "FAILED",
                "source_failed: {}",
                status["error"]
            );
            let mut changed = false;
            for _ in 0..128 {
                if !p.step()? {
                    break;
                }
                changed = true;
            }
            p.flush_confirmed()?;
            let sequence: u64 = p.db.query_row(
                "SELECT event_sequence FROM feed_state WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            atomic_json(
                &output.join("STATUS.json"),
                &json!({"state":"RUNNING","profile":v2::PROFILE,"event_sequence":sequence,"updated_unix_ms":now(),"source_state":status["state"]}),
            )?;
            if !follow && !changed {
                return Ok(());
            }
            if !changed {
                thread::sleep(Duration::from_millis(20));
            }
        }
    })();
    if let Err(error) = &result {
        let _ = atomic_json(
            &output.join("STATUS.json"),
            &json!({"state":"FAILED","profile":v2::PROFILE,"error":format!("{error:#}"),"source_preserved":true}),
        );
    }
    result
}
