use crate::{
    frame::{self, Chunk},
    source::bounded_file,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
#[derive(Clone, Serialize, Deserialize)]
struct Checkpoint {
    schema: String,
    base: String,
    chunks: Vec<String>,
}
pub struct State {
    pub base: String,
    pub chunks: VecDeque<Arc<Chunk>>,
    pub bytes: usize,
    pub status: String,
    pub error: Option<String>,
    pub updated_ms: u64,
    pub received_chunks: u64,
    pub received_bytes: u64,
    pub upstream_http_version: Option<String>,
}
pub struct Store {
    pub state: Mutex<State>,
    pub notify: Notify,
    pub limit: usize,
    pub spool: Option<PathBuf>,
    _lock: Option<File>,
}
pub fn atomic(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
impl Store {
    pub fn open(base: String, limit: usize, spool: Option<PathBuf>) -> Result<Arc<Self>> {
        frame::parse_cursor(&base)?;
        ensure!(
            limit >= frame::MAX_FRAME && limit <= 1024 * 1024 * 1024,
            "invalid_replay_budget"
        );
        let mut state = State {
            base,
            chunks: VecDeque::new(),
            bytes: 0,
            status: "STARTING".into(),
            error: None,
            updated_ms: 0,
            received_chunks: 0,
            received_bytes: 0,
            upstream_http_version: None,
        };
        let mut lock = None;
        if let Some(p) = &spool {
            fs::create_dir_all(p)?;
            let f = File::options()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(p.join("LOCK"))?;
            use std::os::fd::AsRawFd;
            ensure!(
                unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
                "relay_spool_in_use"
            );
            lock = Some(f);
            if p.join("CHECKPOINT.json").exists() {
                let cp: Checkpoint = serde_json::from_slice(&bounded_file(
                    &p.join("CHECKPOINT.json"),
                    4 * 1024 * 1024,
                )?)?;
                ensure!(
                    cp.schema == "blockzilla-relay-checkpoint-v1",
                    "unsupported_checkpoint"
                );
                let session = frame::parse_cursor(&state.base)?.0.to_owned();
                ensure!(
                    frame::parse_cursor(&cp.base)?.0 == session
                        && frame::processed_cursor(&cp.base)
                            == frame::processed_cursor(&state.base),
                    "checkpoint_session_mismatch"
                );
                state.base = cp.base;
                ensure!(cp.chunks.len() <= 16384, "checkpoint_too_large");
                for cursor in cp.chunks {
                    let c = frame::decode(
                        bounded_file(&p.join(filename(&cursor)?), frame::MAX_FRAME)?.into(),
                    )?;
                    ensure!(c.cursor == cursor, "checkpoint_digest_mismatch");
                    if let Some(prev) = state.chunks.back() {
                        frame::follows(prev, &c)?;
                    } else if frame::processed_cursor(&state.base) {
                        ensure!(
                            c.session == frame::parse_cursor(&state.base)?.0
                                && c.envelope.profile == frame::PROCESSED
                                && c.position() == frame::parse_cursor(&state.base)?.1 + 1,
                            "checkpoint_event_gap"
                        );
                    }
                    state.bytes += c.wire.len();
                    ensure!(state.bytes <= limit, "replay_budget_shrunk");
                    state.chunks.push_back(Arc::new(c));
                }
            }
            // A crash can leave the next frame before its checkpoint, or an
            // evicted frame after checkpoint commit. Reclaim only our spool
            // filenames under the exclusive process lock.
            let retained: std::collections::BTreeSet<_> = state
                .chunks
                .iter()
                .map(|c| filename(&c.cursor))
                .collect::<Result<_>>()?;
            for entry in fs::read_dir(p)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                let owned = name
                    .strip_suffix(".frame")
                    .or_else(|| name.strip_suffix(".tmp"))
                    .and_then(|stem| stem.split_once('-'))
                    .is_some_and(|(slot, sha)| {
                        slot.parse::<u64>().is_ok()
                            && sha.len() == 64
                            && sha
                                .bytes()
                                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                    });
                if entry.file_type()?.is_file()
                    && (name == "CHECKPOINT.tmp" || (owned && !retained.contains(&name)))
                {
                    fs::remove_file(entry.path())?;
                }
            }
            File::open(p)?.sync_all()?;
        }
        Ok(Arc::new(Self {
            state: Mutex::new(state),
            notify: Notify::new(),
            limit,
            spool,
            _lock: lock,
        }))
    }
    pub fn latest(&self) -> String {
        let s = self.state.lock().unwrap();
        s.chunks
            .back()
            .map_or_else(|| s.base.clone(), |c| c.cursor.clone())
    }
    pub fn set_status(&self, status: &str, error: Option<String>) {
        let mut s = self.state.lock().unwrap();
        s.status = status.into();
        s.error = error;
        drop(s);
        self.notify.notify_waiters();
    }
    pub fn head(&self) -> Value {
        let s = self.state.lock().unwrap();
        let c = s.chunks.back();
        let age = if s.updated_ms > 0 {
            Some(now_ms().saturating_sub(s.updated_ms))
        } else {
            None
        };
        json!({"schema":"blockzilla-stream-head-v1","profile":if frame::processed_cursor(&s.base){frame::PROCESSED}else{"native-compact-projection-v1"},"state":s.status,"error":s.error,"stale":s.status!="LIVE" || age.is_none_or(|a|a>30000),"intake_age_ms":age,"earliest_after":s.base,"latest_cursor":c.map_or(&s.base,|c|&c.cursor),"last_slot":c.map(|c|c.last_slot),"genesis_hash":c.map(|c|&c.envelope.marker["genesis_hash"]),"node_identity":c.map(|c|&c.envelope.marker["node_identity"]),"finalized":!frame::processed_cursor(&s.base),"event_sequence":c.map(|c|c.position()),"event_kind":c.and_then(|c|c.envelope.event.as_ref().map(|e|&e.kind)),"canonical":false,"publication_ready":false,"evidence_included":false,"replay_bytes":s.bytes,"replay_limit_bytes":self.limit,"retained_chunks":s.chunks.len(),"received_chunks":s.received_chunks,"received_bytes":s.received_bytes,"last_admitted_unix_ms":s.updated_ms,"upstream_http_version":s.upstream_http_version})
    }
    pub fn after(&self, cursor: &str) -> Result<Option<Arc<Chunk>>> {
        frame::parse_cursor(cursor)?;
        let s = self.state.lock().unwrap();
        if s.base == cursor {
            return Ok(s.chunks.front().cloned());
        }
        let i = s
            .chunks
            .iter()
            .position(|c| c.cursor == cursor)
            .context("replay_unavailable")?;
        Ok(s.chunks.get(i + 1).cloned())
    }
    pub fn chunk(&self, id: &str) -> Option<Arc<Chunk>> {
        self.state
            .lock()
            .unwrap()
            .chunks
            .iter()
            .find(|c| c.digest == id)
            .cloned()
    }
    pub fn admit(&self, c: Chunk) -> Result<()> {
        ensure!(c.wire.len() <= self.limit, "chunk_exceeds_replay_budget");
        let (mut chunks, mut base, mut total) = {
            let s = self.state.lock().unwrap();
            if let Some(prev) = s.chunks.back() {
                if prev.cursor == c.cursor {
                    return Ok(());
                }
                frame::follows(prev, &c)?;
            } else {
                ensure!(
                    frame::parse_cursor(&s.base)?.0 == c.session
                        && frame::processed_cursor(&s.base)
                            == (c.envelope.profile == frame::PROCESSED)
                        && (if c.envelope.profile == frame::PROCESSED {
                            c.position() == frame::parse_cursor(&s.base)?.1 + 1
                        } else {
                            c.position() > frame::parse_cursor(&s.base)?.1
                        }),
                    "first_chunk_before_anchor"
                );
            }
            (s.chunks.clone(), s.base.clone(), s.bytes)
        };
        let len = c.wire.len();
        let new = Arc::new(c);
        chunks.push_back(new.clone());
        total += len;
        let mut removed = Vec::new();
        while total > self.limit {
            let old = chunks.pop_front().unwrap();
            total -= old.wire.len();
            base = old.cursor.clone();
            removed.push(old);
        }
        if let Some(p) = &self.spool {
            atomic(&p.join(filename(&new.cursor)?), &new.wire)?;
            atomic(
                &p.join("CHECKPOINT.json"),
                &serde_json::to_vec(&Checkpoint {
                    schema: "blockzilla-relay-checkpoint-v1".into(),
                    base: base.clone(),
                    chunks: chunks.iter().map(|c| c.cursor.clone()).collect(),
                })?,
            )?;
            for c in removed {
                fs::remove_file(p.join(filename(&c.cursor)?))?;
            }
            File::open(p)?.sync_all()?;
        }
        let mut s = self.state.lock().unwrap();
        s.base = base;
        s.chunks = chunks;
        s.bytes = total;
        s.status = "LIVE".into();
        s.error = None;
        s.updated_ms = now_ms();
        s.received_chunks += 1;
        s.received_bytes += len as u64;
        drop(s);
        self.notify.notify_waiters();
        Ok(())
    }
}
fn filename(cursor: &str) -> Result<String> {
    let (_, slot, sha) = frame::parse_cursor(cursor)?;
    ensure!(sha != "-", "anchor_has_no_chunk");
    Ok(format!("{slot}-{sha}.frame"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn processed_restart_keeps_same_slot_controls() {
        let t = tempfile::tempdir().unwrap();
        let s = Store::open(
            frame::processed_anchor("session1", 0),
            frame::MAX_FRAME,
            Some(t.path().into()),
        )
        .unwrap();
        for (sequence, bank, kind) in [
            (1, 7, "block"),
            (2, 8, "block"),
            (3, 7, "invalidate"),
            (4, 8, "root"),
        ] {
            s.admit(frame::processed_fixture(sequence, 100, bank, kind))
                .unwrap();
        }
        let cursor = s.latest();
        drop(s);
        let s = Store::open(
            frame::processed_anchor("session1", 0),
            frame::MAX_FRAME,
            Some(t.path().into()),
        )
        .unwrap();
        assert_eq!(s.latest(), cursor);
        assert_eq!(s.head()["event_sequence"], 4);
        assert_eq!(s.head()["finalized"], false);
        assert!(
            s.admit(frame::processed_fixture(6, 101, 9, "block"))
                .is_err()
        );
    }
    #[test]
    fn slow_cursor_expires_without_blocking_intake() {
        let s = Store::open(frame::anchor("session1", 99), frame::MAX_FRAME, None).unwrap();
        let mut last = 99;
        let mut old = String::new();
        for slot in 100..2400 {
            let c = frame::fixture(slot, last);
            if slot == 100 {
                old = c.cursor.clone()
            };
            s.admit(c).unwrap();
            last = slot;
        }
        // Small fixtures require a tighter limit after construction for this unit case.
        let mut s = Arc::try_unwrap(s).ok().unwrap();
        s.limit = 4096;
        s.admit(frame::fixture(2400, 2399)).unwrap();
        assert!(s.after(&old).is_err());
        assert!(s.state.lock().unwrap().bytes <= 4096);
    }
    #[test]
    fn durable_restart_and_conflicts() {
        let t = tempfile::tempdir().unwrap();
        let a = frame::fixture(100, 99);
        let b = frame::fixture(103, 100);
        let s = Store::open(
            frame::anchor("session1", 99),
            frame::MAX_FRAME,
            Some(t.path().into()),
        )
        .unwrap();
        s.admit(a.clone()).unwrap();
        s.admit(b.clone()).unwrap();
        drop(s);
        let orphan = frame::fixture(104, 103);
        let orphan_path = t.path().join(filename(&orphan.cursor).unwrap());
        fs::write(&orphan_path, &orphan.wire).unwrap();
        let s = Store::open(
            frame::anchor("session1", 99),
            frame::MAX_FRAME,
            Some(t.path().into()),
        )
        .unwrap();
        assert!(!orphan_path.exists());
        assert_eq!(s.latest(), b.cursor);
        assert_eq!(s.after(&a.cursor).unwrap().unwrap().cursor, b.cursor);
        s.admit(b).unwrap();
        assert!(s.admit(frame::fixture(104, 102)).is_err());
    }
}
