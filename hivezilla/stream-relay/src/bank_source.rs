use crate::source::bounded_file;
use anyhow::{Context, Result, ensure};
use blockzilla_bank_stream_v2::{self as v2, BankEvent, Frame, SourceIdentity, artifact};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::Value;
use std::path::{Path, PathBuf};
pub struct BankSource {
    root: PathBuf,
    payload_root: PathBuf,
    db: Connection,
    pub source: SourceIdentity,
    pub position: u64,
}
impl BankSource {
    pub fn open(root: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        let h: Value = serde_json::from_slice(&bounded_file(&root.join("FEED.json"), 65536)?)?;
        ensure!(
            h["schema"] == "hivezilla-bank-feed-v2" && h["profile"] == v2::PROFILE,
            "unsupported_bank_source"
        );
        let source = SourceIdentity::from_marker(&h)?;
        let payload_root =
            PathBuf::from(h["payload_root"].as_str().context("missing_payload_root")?)
                .canonicalize()?;
        let db = Connection::open_with_flags(
            root.join("feed.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        db.busy_timeout(std::time::Duration::from_millis(250))?;
        db.execute_batch("PRAGMA query_only=ON;")?;
        Ok(Self {
            root,
            payload_root,
            db,
            source,
            position: 0,
        })
    }
    pub fn seek(&mut self, cursor: &str) -> Result<()> {
        let (session, sequence, sha) = v2::parse_cursor(cursor)?;
        ensure!(session == self.source.session_id, "source_changed");
        if sequence > 0 {
            let actual: String = self.db.query_row(
                "SELECT frame_sha256 FROM feed_events WHERE sequence=?1",
                [sequence],
                |r| r.get(0),
            )?;
            ensure!(actual == sha, "invalid_cursor");
        } else {
            ensure!(sha == "-", "invalid_cursor");
        }
        self.position = sequence;
        Ok(())
    }
    fn build(&self, row: (String, String, String)) -> Result<Frame> {
        let (path, event, sha) = row;
        let event: BankEvent = serde_json::from_str(&event)?;
        let frame = artifact::build(&self.payload_root, Path::new(&path), &self.source, event)?;
        ensure!(frame.sha256 == sha, "frame_commit_changed");
        Ok(frame)
    }
    pub fn next(&mut self) -> Result<Option<Frame>> {
        for attempt in 0..=5 {
            match self.next_once() {
                Err(e) if attempt < 5 && retryable(&e) => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    self.db = Connection::open_with_flags(
                        self.root.join("feed.sqlite"),
                        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )?;
                    self.db
                        .busy_timeout(std::time::Duration::from_millis(250))?;
                    self.db.execute_batch("PRAGMA query_only=ON;")?;
                }
                result => return result,
            }
        }
        unreachable!()
    }
    fn next_once(&mut self) -> Result<Option<Frame>> {
        let row:Option<(u64,String,String,String)>=self.db.query_row("SELECT sequence,artifact,event_json,frame_sha256 FROM feed_events WHERE sequence>?1 ORDER BY sequence LIMIT 1",[self.position],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let Some((sequence, path, event, sha)) = row else {
            return Ok(None);
        };
        ensure!(sequence == self.position + 1, "replay_unavailable");
        let frame = self.build((path, event, sha))?;
        ensure!(
            frame.envelope.event.sequence == sequence,
            "event_index_mismatch"
        );
        self.position = sequence;
        Ok(Some(frame))
    }
    pub fn chunk(&self, sha: &str) -> Result<Option<Frame>> {
        ensure!(v2::is_digest(sha), "invalid_digest");
        let row = self
            .db
            .query_row(
                "SELECT artifact,event_json,frame_sha256 FROM feed_events WHERE frame_sha256=?1",
                [sha],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if row
            .as_ref()
            .is_some_and(|row: &(String, String, String)| archived_payload(Path::new(&row.0)))
        {
            return Ok(None); // The bounded live API reports 410 for cold dependencies.
        }
        row.map(|row| self.build(row)).transpose()
    }
    pub fn health(&self) -> Result<&'static str> {
        let status: Value =
            serde_json::from_slice(&bounded_file(&self.root.join("STATUS.json"), 65536)?)?;
        let input: Value = serde_json::from_slice(&bounded_file(
            &self.payload_root.join("STATUS.json"),
            65536,
        )?)?;
        if status["state"] == "FAILED" || input["state"] == "FAILED" {
            return Ok("failed");
        }
        let now = crate::store::now_ms();
        let fresh = |s: &Value| -> bool {
            s["updated_unix_ms"].as_u64().is_some_and(|t| {
                v2::clock_check(t, now).is_ok() && now.saturating_sub(t) <= v2::DEFAULT_MAX_LAG_MS
            })
        };
        Ok(
            if status["state"] == "RUNNING"
                && input["state"] == "RUNNING"
                && fresh(&status)
                && fresh(&input)
            {
                "live"
            } else {
                "stale"
            },
        )
    }
}

fn archived_payload(path: &Path) -> bool {
    path.join("REMOTE-CUSTODY.json").is_file()
        && v2::REQUIRED.iter().any(|name| !path.join(name).is_file())
}

#[cfg(test)]
mod cold_tests {
    use super::*;
    #[test]
    fn cold_payload_is_unavailable_until_exact_files_are_restored() {
        let t = tempfile::tempdir().unwrap();
        assert!(!archived_payload(t.path()));
        std::fs::write(t.path().join("REMOTE-CUSTODY.json"), b"{}").unwrap();
        assert!(archived_payload(t.path()));
        for name in v2::REQUIRED {
            std::fs::write(t.path().join(name), b"data").unwrap();
        }
        assert!(!archived_payload(t.path()));
    }
}

fn retryable(e: &anyhow::Error) -> bool {
    matches!(e.downcast_ref::<rusqlite::Error>(),Some(rusqlite::Error::SqliteFailure(code,_))
        if matches!(code.code,rusqlite::ErrorCode::DatabaseBusy|rusqlite::ErrorCode::DatabaseLocked|rusqlite::ErrorCode::ReadOnly))
}
