use crate::frame::{self, BankEvent, Chunk, Envelope, Object};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, params};
use serde_json::Value;
use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

pub struct Source {
    root: PathBuf,
    audit: Connection,
    pub session: String,
    pub position: u64,
    processed: bool,
}
pub fn bounded_file(path: &Path, max: usize) -> Result<Vec<u8>> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "not_regular_file"
    );
    let f = fs::File::open(path)?;
    ensure!(f.metadata()?.len() <= max as u64, "file_too_large");
    let mut bytes = Vec::new();
    f.take(max as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= max, "file_grew");
    Ok(bytes)
}
fn checked_path(root: &Path, path: &Path) -> Result<PathBuf> {
    let relative = path
        .strip_prefix(root)
        .context("artifact_outside_output_root")?;
    let mut p = root.to_path_buf();
    for c in relative.components() {
        let Component::Normal(c) = c else {
            anyhow::bail!("unsafe_artifact_path")
        };
        p.push(c);
        ensure!(
            !fs::symlink_metadata(&p)?.file_type().is_symlink(),
            "symlink_artifact"
        );
    }
    Ok(p)
}
impl Source {
    fn audit(root: &Path, processed: bool) -> Result<Connection> {
        let name = if processed {
            "feed.sqlite"
        } else {
            "committed.sqlite"
        };
        checked_path(root, &root.join(name))?;
        let audit = Connection::open_with_flags(
            root.join(name),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        audit.busy_timeout(std::time::Duration::from_millis(250))?;
        audit.execute_batch("PRAGMA query_only=ON;")?;
        Ok(audit)
    }
    pub fn open(root: &Path, start_slot: u64) -> Result<Self> {
        let root = root.canonicalize()?;
        let cursor: Value =
            serde_json::from_slice(&bounded_file(&root.join("CURSOR.json"), 65536)?)?;
        let session = cursor["session_id"]
            .as_str()
            .context("missing_session")?
            .to_owned();
        ensure!(
            frame::session_name(&session) && start_slot > 0,
            "invalid_source_start"
        );
        let audit = Self::audit(&root, false)?;
        Ok(Self {
            root,
            audit,
            session,
            position: start_slot - 1,
            processed: false,
        })
    }
    pub fn open_processed(
        root: &Path,
        start_sequence: Option<u64>,
        tail_events: u64,
    ) -> Result<Self> {
        let root = root.canonicalize()?;
        let header: Value = serde_json::from_slice(&bounded_file(&root.join("FEED.json"), 65536)?)?;
        ensure!(
            header["schema"] == "hivezilla-processed-feed-v1"
                && header["profile"] == frame::PROCESSED,
            "unsupported_processed_source"
        );
        let session = header["session_id"]
            .as_str()
            .context("missing_session")?
            .to_owned();
        ensure!(
            frame::session_name(&session) && (1..=4096).contains(&tail_events),
            "invalid_source_start"
        );
        let audit = Self::audit(&root, true)?;
        let last: u64 = audit.query_row(
            "SELECT event_sequence FROM feed_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let start = start_sequence.unwrap_or(last.saturating_sub(tail_events).saturating_add(1));
        ensure!(start > 0, "invalid_source_start");
        Ok(Self {
            root,
            audit,
            session,
            position: start - 1,
            processed: true,
        })
    }
    pub fn anchor(&self) -> String {
        if self.processed {
            frame::processed_anchor(&self.session, self.position)
        } else {
            frame::anchor(&self.session, self.position)
        }
    }
    pub fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        // A WAL reset or read-mark change can invalidate a long-lived reader.
        // Retry the same unacknowledged position; never treat this as an empty tail.
        for attempt in 0..=5 {
            match self.next_chunk_once() {
                Err(e) if attempt < 5 && retryable_sqlite(&e) => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    self.audit = Self::audit(&self.root, self.processed)?;
                }
                result => return result,
            }
        }
        unreachable!()
    }
    fn next_chunk_once(&mut self) -> Result<Option<Chunk>> {
        if self.processed {
            return self.next_processed();
        }
        let cursor: Value =
            serde_json::from_slice(&bounded_file(&self.root.join("CURSOR.json"), 65536)?)?;
        ensure!(
            cursor["session_id"] == self.session,
            "capture_session_changed"
        );
        let committed = cursor["last_slot"]
            .as_u64()
            .context("missing_commit_cursor")?;
        let row = {
            let mut q = self.audit.prepare(
                "SELECT artifact FROM commits WHERE slot>?1 AND slot<=?2 ORDER BY slot LIMIT 1",
            )?;
            q.query_map(params![self.position, committed], |r| r.get::<_, String>(0))?
                .next()
                .transpose()?
        };
        let Some(path) = row else { return Ok(None) };
        let path = checked_path(&self.root, Path::new(&path))?;
        let marker: Value = serde_json::from_slice(&bounded_file(
            &path.join("DIRECT-AGAVE-COMPACT.v1.json"),
            frame::MAX_META,
        )?)?;
        let last = marker["end_slot_exclusive"]
            .as_u64()
            .context("missing_end_slot")?
            .checked_sub(1)
            .context("invalid_end_slot")?;
        // Audit rows are written before the durable cursor. Never expose that pending tail.
        if last > committed {
            return Ok(None);
        }
        ensure!(
            marker["session_id"] == self.session,
            "artifact_session_mismatch"
        );
        let blocks = marker["produced_blocks"]
            .as_array()
            .context("missing_blocks")?;
        for b in blocks {
            let slot = b["slot"].as_u64().context("missing_slot")?;
            let (bank, artifact): (u64, String) = self.audit.query_row(
                "SELECT bank,artifact FROM commits WHERE slot=?1",
                [slot],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                b["bank_id"].as_u64() == Some(bank) && artifact == path.to_string_lossy(),
                "artifact_not_committed"
            );
        }
        let chunk = self.artifact_chunk(&path, marker, None)?;
        self.position = chunk.last_slot;
        Ok(Some(chunk))
    }
    fn next_processed(&mut self) -> Result<Option<Chunk>> {
        let row = {
            let mut q=self.audit.prepare("SELECT sequence,artifact,event_json FROM feed_events WHERE sequence>?1 ORDER BY sequence LIMIT 1")?;
            q.query_map([self.position], |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .next()
            .transpose()?
        };
        let Some((sequence, path, event)) = row else {
            let status = self.root.join("STATUS.json");
            if status.exists() {
                let status: Value = serde_json::from_slice(&bounded_file(&status, 65536)?)?;
                ensure!(
                    status["state"] != "FAILED",
                    "processed_source_failed: {}",
                    status["error"]
                );
            }
            return Ok(None);
        };
        ensure!(
            self.position.checked_add(1) == Some(sequence),
            "event_sequence_gap"
        );
        let event: BankEvent = serde_json::from_str(&event)?;
        ensure!(event.sequence == sequence, "event_index_mismatch");
        let path = checked_path(&self.root, Path::new(&path))?;
        let raw = bounded_file(&path.join("DIRECT-AGAVE-COMPACT.v1.json"), frame::MAX_META)?;
        ensure!(
            frame::digest(&raw) == event.payload_sha256,
            "processed_payload_changed"
        );
        let mut marker: Value = serde_json::from_slice(&raw)?;
        ensure!(
            marker["session_id"] == self.session,
            "artifact_session_mismatch"
        );
        let bank = &marker["produced_blocks"][0];
        for k in [
            "slot",
            "bank_id",
            "blockhash",
            "parent_slot",
            "parent_blockhash",
        ] {
            ensure!(
                bank[k] == serde_json::to_value(&event.bank)?[k],
                "control_bank_mismatch"
            );
        }
        let chunk = if event.kind == "block" {
            self.artifact_chunk(&path, marker, Some(event))?
        } else {
            marker["schema"] = "hivezilla-processed-control-v1".into();
            let mut control = serde_json::Map::new();
            for k in [
                "schema",
                "state",
                "source_profile",
                "session_id",
                "genesis_hash",
                "node_identity",
                "agave_revision",
                "finalized",
                "canonical",
                "publication_ready",
            ] {
                control.insert(k.into(), marker[k].clone());
            }
            let envelope = Envelope {
                schema: "blockzilla-stream-envelope-v1".into(),
                profile: frame::PROCESSED.into(),
                evidence_included: false,
                marker: Value::Object(control),
                objects: vec![],
                event: Some(event),
            };
            frame::decode(frame::encode(&envelope, &[])?)?
        };
        self.position = sequence;
        Ok(Some(chunk))
    }
    fn artifact_chunk(
        &self,
        path: &Path,
        marker: Value,
        event: Option<BankEvent>,
    ) -> Result<Chunk> {
        let mut payload = Vec::new();
        let mut objects = Vec::new();
        let files = marker["files"].as_array().context("missing_files")?;
        ensure!(files.len() <= 32, "too_many_files");
        for f in files {
            let name = f["path"].as_str().context("missing_path")?;
            if !frame::FILES.contains(&name) {
                ensure!(
                    matches!(name, "source-events.json.zst" | "source-events.json"),
                    "unsupported_compact_dependency"
                );
                continue;
            }
            let expected = f["bytes"].as_u64().context("missing_size")?;
            ensure!(
                expected <= (frame::MAX_PAYLOAD - payload.len()) as u64,
                "frame_too_large"
            );
            let bytes = bounded_file(&path.join(name), expected as usize)?;
            ensure!(
                bytes.len() as u64 == expected && f["sha256"] == frame::digest(&bytes),
                "source_object_changed"
            );
            objects.push(Object {
                path: name.into(),
                offset: payload.len(),
                bytes: bytes.len(),
                sha256: frame::digest(&bytes),
            });
            payload.extend_from_slice(&bytes);
        }
        let envelope = Envelope {
            schema: "blockzilla-stream-envelope-v1".into(),
            profile: if event.is_some() {
                frame::PROCESSED.into()
            } else {
                "native-compact-projection-v1".into()
            },
            evidence_included: false,
            marker,
            objects,
            event,
        };
        let chunk = frame::decode(frame::encode(&envelope, &payload)?)?;
        // Volatile worker timestamps are deliberately outside the immutable envelope.
        Ok(chunk)
    }
}

fn retryable_sqlite(e: &anyhow::Error) -> bool {
    matches!(e.downcast_ref::<rusqlite::Error>(),
        Some(rusqlite::Error::SqliteFailure(code, _))
        if matches!(code.code, rusqlite::ErrorCode::DatabaseBusy
            | rusqlite::ErrorCode::DatabaseLocked | rusqlite::ErrorCode::ReadOnly))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readonly_reader_follows_wal_growth_and_reset_without_writing_source_rows() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().canonicalize().unwrap();
        let db = Connection::open(root.join("committed.sqlite")).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE commits(slot INTEGER PRIMARY KEY,bank INTEGER,artifact TEXT);").unwrap();
        fs::write(
            root.join("CURSOR.json"),
            br#"{"session_id":"session1","last_slot":99}"#,
        )
        .unwrap();
        let mut reader = Source::open(&root, 100).unwrap();
        assert!(reader.audit.execute("DELETE FROM commits", []).is_err());
        for slot in 100..250 {
            let c = frame::fixture(slot, slot - 1);
            let dir = root.join(format!("{slot}.compact"));
            fs::create_dir(&dir).unwrap();
            fs::write(
                dir.join("DIRECT-AGAVE-COMPACT.v1.json"),
                serde_json::to_vec(&c.envelope.marker).unwrap(),
            )
            .unwrap();
            for o in &c.envelope.objects {
                fs::write(dir.join(&o.path), [42]).unwrap();
            }
            db.execute(
                "INSERT INTO commits VALUES(?1,?1,?2)",
                params![slot, dir.to_str().unwrap()],
            )
            .unwrap();
            fs::write(
                root.join("CURSOR.json"),
                format!("{{\"session_id\":\"session1\",\"last_slot\":{slot}}}"),
            )
            .unwrap();
            let got = reader.next_chunk().unwrap().unwrap();
            assert_eq!(got.cursor, c.cursor);
            db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        assert_eq!(
            db.query_row("SELECT count(*) FROM commits", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            150
        );
    }
    #[test]
    fn audit_alone_cannot_publish_a_pending_artifact() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().canonicalize().unwrap();
        let c = frame::fixture(100, 99);
        let dir = root.join("epoch-0/100-100.compact");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("DIRECT-AGAVE-COMPACT.v1.json"),
            [
                vec![b' '; 40_000],
                serde_json::to_vec(&c.envelope.marker).unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        for o in &c.envelope.objects {
            fs::write(dir.join(&o.path), [42]).unwrap();
        }
        let db = Connection::open(root.join("committed.sqlite")).unwrap();
        db.execute_batch(
            "CREATE TABLE commits(slot INTEGER PRIMARY KEY,bank INTEGER,artifact TEXT);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO commits VALUES(100,100,?1)",
            [dir.to_str().unwrap()],
        )
        .unwrap();
        fs::write(
            root.join("CURSOR.json"),
            br#"{"session_id":"session1","last_slot":99,"updated_unix_ms":1}"#,
        )
        .unwrap();
        let mut s = Source::open(&root, 100).unwrap();
        assert!(s.next_chunk().unwrap().is_none());
        fs::write(
            root.join("CURSOR.json"),
            br#"{"session_id":"session1","last_slot":100,"updated_unix_ms":1}"#,
        )
        .unwrap();
        let original = s.next_chunk().unwrap().unwrap();
        assert_eq!(original.last_slot, 100);
        // Current cursor timestamps do not change the immutable transport ID.
        fs::write(
            root.join("CURSOR.json"),
            br#"{"session_id":"session1","last_slot":100,"updated_unix_ms":99}"#,
        )
        .unwrap();
        let a = Source::open(&root, 100)
            .unwrap()
            .next_chunk()
            .unwrap()
            .unwrap();
        assert_eq!(a.cursor, original.cursor);
    }
    #[test]
    fn traversal_and_symlinks_are_rejected() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().canonicalize().unwrap();
        assert!(checked_path(&p, &p.join("../bad")).is_err());
        std::os::unix::fs::symlink("/tmp", p.join("link")).unwrap();
        assert!(checked_path(&p, &p.join("link/file")).is_err());
    }
}
