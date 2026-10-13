//! A control pins its exact payload. Frame/spool/head commits have one boundary.
use crate::{
    source::bounded_file,
    store::{atomic, now_ms},
};
use anyhow::{Context, Result, ensure};
use blockzilla_bank_stream_v2::{self as v2, Frame, Head, SourceIdentity, chain::Chain};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub source: SourceIdentity,
    pub cursor: String,
    pub chain: Chain,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    schema: String,
    source: SourceIdentity,
    base: String,
    events: Vec<String>,
    frames: Vec<String>,
    chain: Chain,
}
#[derive(Clone)]
struct State {
    base: String,
    events: VecDeque<Arc<Frame>>,
    frames: BTreeMap<String, Arc<Frame>>,
    chain: Chain,
    selected_at: Instant,
    selected_age: u64,
    status: String,
    health_at: Instant,
    error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use v2::fixtures::*;
    fn fresh(mut f: Frame) -> Frame {
        f.envelope.event.observed_at_unix_ms = now_ms();
        let n = u32::from_le_bytes(f.wire[8..12].try_into().unwrap()) as usize;
        v2::encode(f.envelope, &f.wire[16 + n..]).unwrap()
    }
    #[test]
    fn durable_control_dependencies_resume_and_duplicate_admission() {
        let t = tempfile::tempdir().unwrap();
        let base = v2::cursor(&source().session_id, 0, "-");
        let s = BankStore::open(source(), base.clone(), v2::MAX_FRAME, t.path().into()).unwrap();
        let b = fresh(block(1, 9, 1, 8, 9));
        s.admit(b.clone(), None).unwrap();
        assert!(s.head().latest_confirmed.is_none());
        let c = control(&b, 2, "root", now_ms());
        s.admit(c.clone(), None).unwrap();
        s.admit(c.clone(), None).unwrap();
        s.set_health("live", None);
        assert_eq!(s.head().status, "live");
        assert_eq!(s.after(&base).unwrap().unwrap().cursor, b.cursor);
        assert_eq!(s.after(&b.cursor).unwrap().unwrap().cursor, c.cursor);
        assert!(s.after(&c.cursor).unwrap().is_none());
        assert!(
            s.after(&v2::cursor("wrong-session", 0, "-"))
                .unwrap_err()
                .to_string()
                .contains("source_changed")
        );
        drop(s);
        let s = BankStore::open(source(), base, v2::MAX_FRAME, t.path().into()).unwrap();
        assert_eq!(s.latest(), c.cursor);
        assert_eq!(s.chunk(&b.sha256).unwrap().wire, b.wire);
        assert_eq!(s.head().status, "stale");
        let mut bad = c.envelope;
        bad.event.sequence = 4;
        assert!(s.admit(v2::encode(bad, &[]).unwrap(), None).is_err());
    }
    #[test]
    fn retention_pins_control_and_head_and_expiry_is_an_explicit_gap() {
        let t = tempfile::tempdir().unwrap();
        let base = v2::cursor(&source().session_id, 0, "-");
        let s = BankStore::open(source(), base.clone(), v2::MAX_FRAME, t.path().into()).unwrap();
        let b = fresh(block(1, 9, 1, 8, 9));
        s.admit(b.clone(), None).unwrap();
        s.admit(control(&b, 2, "root", now_ms()), None).unwrap();
        // Inflate a permitted fixture object to exercise byte eviction with only a few events.
        for n in 3..8 {
            let f = block(n, 10 + n, n, 8, 10);
            let mut e = f.envelope;
            let bytes = vec![42; 4 * 1024 * 1024];
            let mut body = vec![42; v2::REQUIRED.len() - 1];
            e.objects.last_mut().unwrap().bytes = bytes.len();
            e.objects.last_mut().unwrap().sha256 = v2::digest(&bytes);
            let row = e.marker["files"]
                .as_array_mut()
                .unwrap()
                .last_mut()
                .unwrap();
            row["bytes"] = bytes.len().into();
            row["sha256"] = v2::digest(&bytes).into();
            body.extend(bytes);
            s.admit(v2::encode(e, &body).unwrap(), None).unwrap();
        }
        assert!(
            s.after(&base)
                .unwrap_err()
                .to_string()
                .contains("replay_unavailable")
        );
        assert!(s.chunk(&b.sha256).is_some());
        assert!(s.metrics()["replay_bytes"].as_u64().unwrap() <= v2::MAX_FRAME as u64);
        drop(s);
        let s = BankStore::open(source(), base, v2::MAX_FRAME, t.path().into()).unwrap();
        assert!(s.chunk(&b.sha256).is_some());
    }
    #[test]
    fn repeated_health_or_older_root_cannot_renew_observation_age() {
        let t = tempfile::tempdir().unwrap();
        let s = BankStore::open(
            source(),
            v2::cursor(&source().session_id, 0, "-"),
            v2::MAX_FRAME,
            t.path().into(),
        )
        .unwrap();
        let root = fresh(block(1, 9, 1, 8, 9));
        s.admit(root.clone(), None).unwrap();
        s.admit(control(&root, 2, "root", now_ms()), None).unwrap();
        let b = fresh(block(3, 17, 2, 9, 10));
        s.admit(b.clone(), None).unwrap();
        s.admit(control(&b, 4, "confirmed", now_ms()), None)
            .unwrap();
        {
            s.state.lock().unwrap().selected_age = 20_000;
        }
        s.set_health("live", None);
        s.admit(control(&root, 5, "root", now_ms()), None).unwrap();
        s.set_health("live", None);
        assert_eq!(s.head().status, "stale");
        assert_eq!(
            s.head().latest_confirmed.unwrap().bank,
            b.envelope.event.bank
        );
        let mut future = control(&b, 6, "confirmed", now_ms() + 6000);
        assert!(s.admit(future.clone(), None).is_err());
        future.envelope.event.sequence = 6;
    }
    #[test]
    fn batch_commit_is_atomic_bounded_and_restores_all_dependencies() {
        let t = tempfile::tempdir().unwrap();
        let base = v2::cursor(&source().session_id, 0, "-");
        let s = BankStore::open(source(), base.clone(), v2::MAX_FRAME, t.path().into()).unwrap();
        let b = fresh(block(1, 9, 1, 8, 9));
        let root = control(&b, 2, "root", now_ms());
        assert!(
            s.admit_batch(vec![
                (b.clone(), None),
                (control(&b, 3, "root", now_ms()), None)
            ])
            .is_err()
        );
        assert_eq!(s.latest(), base);
        assert!(s.chunk(&b.sha256).is_none());
        assert!(!t.path().join("CHECKPOINT.json").exists());
        assert!(
            s.admit_batch(vec![(b.clone(), None); MAX_BATCH_EVENTS + 1])
                .is_err()
        );
        s.admit_batch(vec![(b.clone(), None), (root.clone(), None)])
            .unwrap();
        assert_eq!(s.latest(), root.cursor);
        drop(s);
        let s = BankStore::open(source(), base, v2::MAX_FRAME, t.path().into()).unwrap();
        assert_eq!(s.latest(), root.cursor);
        assert!(s.chunk(&b.sha256).is_some());
        assert_eq!(
            s.head().latest_confirmed.unwrap().bank,
            b.envelope.event.bank
        );
    }
}
pub struct BankStore {
    state: Mutex<State>,
    pub source: SourceIdentity,
    pub notify: Notify,
    pub limit: usize,
    spool: PathBuf,
    _lock: File,
}
pub const MAX_BATCH_EVENTS: usize = 8;
const MAX_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
impl BankStore {
    pub fn open(
        source: SourceIdentity,
        base: String,
        limit: usize,
        spool: PathBuf,
    ) -> Result<Arc<Self>> {
        source.validate()?;
        ensure!(
            v2::parse_cursor(&base)?.0 == source.session_id,
            "source_changed"
        );
        ensure!(
            (v2::MAX_FRAME..=16 * 1024 * 1024 * 1024).contains(&limit),
            "invalid_replay_budget"
        );
        fs::create_dir_all(&spool)?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(spool.join("LOCK"))?;
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "bank_spool_in_use"
        );
        let now = Instant::now();
        let mut state = State {
            base,
            events: VecDeque::new(),
            frames: BTreeMap::new(),
            chain: Chain::default(),
            selected_at: now,
            selected_age: 0,
            status: "starting".into(),
            health_at: now,
            error: None,
        };
        if spool.join("CHECKPOINT.json").exists() {
            let cp: Checkpoint = serde_json::from_slice(&bounded_file(
                &spool.join("CHECKPOINT.json"),
                MAX_CHECKPOINT_BYTES,
            )?)?;
            ensure!(
                cp.schema == "blockzilla-bank-spool-v2"
                    && cp.source == source
                    && cp.events.len() <= 65536
                    && cp.frames.len() <= 131072,
                "checkpoint_source_changed"
            );
            state.base = cp.base;
            state.chain = cp.chain;
            ensure!(
                v2::parse_cursor(&state.base)?.0 == source.session_id,
                "source_changed"
            );
            for sha in cp.frames {
                ensure!(v2::is_digest(&sha), "invalid_checkpoint_digest");
                let f = v2::decode(
                    bounded_file(&spool.join(format!("{sha}.frame")), v2::MAX_FRAME)?.into(),
                )?;
                ensure!(
                    f.sha256 == sha && f.source == source,
                    "checkpoint_frame_changed"
                );
                state.frames.insert(sha, Arc::new(f));
            }
            let mut sequence = v2::parse_cursor(&state.base)?.1;
            for cursor in cp.events {
                let (_, n, sha) = v2::parse_cursor(&cursor)?;
                let frame = state.frames.get(sha).context("dependency_unavailable")?;
                ensure!(
                    n == sequence + 1 && frame.cursor == cursor,
                    "checkpoint_event_gap"
                );
                sequence = n;
                state.events.push_back(frame.clone());
            }
            state.chain.validate(
                &source,
                state.events.back().map_or(&state.base, |f| &f.cursor),
            )?;
            Self::validate_dependencies(&state)?;
            ensure!(Self::bytes(&state) <= limit, "replay_budget_shrunk");
            if let Some(latest) = &state.chain.latest {
                v2::clock_check(latest.observed_at_unix_ms, now_ms())?;
                state.selected_age = now_ms().saturating_sub(latest.observed_at_unix_ms);
            }
        }
        for entry in fs::read_dir(&spool)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type()?.is_file()
                && ((name
                    .strip_suffix(".frame")
                    .is_some_and(|s| v2::is_digest(s) && !state.frames.contains_key(s)))
                    || name == "CHECKPOINT.tmp")
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(Arc::new(Self {
            state: Mutex::new(state),
            source,
            notify: Notify::new(),
            limit,
            spool,
            _lock: lock,
        }))
    }
    fn bytes(s: &State) -> usize {
        s.frames.values().map(|f| f.wire.len()).sum()
    }
    fn pins(s: &State) -> Result<BTreeSet<String>> {
        let mut pins = BTreeSet::new();
        for f in &s.events {
            pins.insert(f.sha256.clone());
            if let Some(sha) = &f.envelope.event.payload_frame_sha256 {
                pins.insert(sha.clone());
            }
        }
        if let Some(head) = &s.chain.latest {
            pins.insert(head.payload_frame_sha256.clone());
            pins.insert(v2::parse_cursor(&head.event_cursor)?.2.into());
        }
        Ok(pins)
    }
    fn validate_dependencies(s: &State) -> Result<()> {
        for f in &s.events {
            if let Some(sha) = &f.envelope.event.payload_frame_sha256 {
                f.envelope
                    .event
                    .matches_payload(s.frames.get(sha).context("dependency_unavailable")?)?;
            }
        }
        if let Some(h) = &s.chain.latest {
            let control = s
                .frames
                .get(v2::parse_cursor(&h.event_cursor)?.2)
                .context("head_control_unavailable")?;
            ensure!(
                v2::ConfirmedObservation::from_frame(control)? == *h,
                "head_control_changed"
            );
            control.envelope.event.matches_payload(
                s.frames
                    .get(&h.payload_frame_sha256)
                    .context("dependency_unavailable")?,
            )?;
        }
        Ok(())
    }
    fn save(&self, s: &State, previous: &State) -> Result<()> {
        Self::validate_dependencies(s)?;
        for (sha, f) in &s.frames {
            if !previous.frames.contains_key(sha) {
                atomic(&self.spool.join(format!("{sha}.frame")), &f.wire)?;
            }
        }
        let cp = Checkpoint {
            schema: "blockzilla-bank-spool-v2".into(),
            source: self.source.clone(),
            base: s.base.clone(),
            events: s.events.iter().map(|f| f.cursor.clone()).collect(),
            frames: s.frames.keys().cloned().collect(),
            chain: s.chain.clone(),
        };
        let bytes = serde_json::to_vec(&cp)?;
        ensure!(bytes.len() <= MAX_CHECKPOINT_BYTES, "checkpoint_too_large");
        atomic(&self.spool.join("CHECKPOINT.json"), &bytes)?;
        for sha in previous.frames.keys() {
            if !s.frames.contains_key(sha) {
                fs::remove_file(self.spool.join(format!("{sha}.frame")))?;
            }
        }
        File::open(&self.spool)?.sync_all()?;
        Ok(())
    }
    pub fn latest(&self) -> String {
        let s = self.state.lock().unwrap();
        s.events
            .back()
            .map_or_else(|| s.base.clone(), |f| f.cursor.clone())
    }
    pub fn needs_bootstrap(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.frames.is_empty() && s.events.is_empty() && s.chain.banks.is_empty()
    }
    pub fn set_health(&self, status: &str, error: Option<String>) {
        if status == "failed" {
            let report = serde_json::json!({"state":"failed","source":self.source,"error":error,"updated_unix_ms":now_ms()});
            let _ = atomic(
                &self.spool.join("SOURCE-ERROR.json"),
                &serde_json::to_vec(&report).unwrap(),
            );
        }
        let mut s = self.state.lock().unwrap();
        s.status = status.into();
        s.error = error;
        s.health_at = Instant::now();
        drop(s);
        self.notify.notify_waiters();
    }
    pub fn error(&self) -> Option<String> {
        self.state.lock().unwrap().error.clone()
    }
    pub fn head(&self) -> Head {
        let s = self.state.lock().unwrap();
        let now = now_ms();
        let latest = s.chain.latest.clone();
        let observed = latest.as_ref().map(|h| h.observed_at_unix_ms);
        let lag = observed.map(|t| now.saturating_sub(t));
        let mut status = if s.status == "failed" || s.chain.reconciliate {
            "failed"
        } else if latest.is_none() {
            "starting"
        } else {
            "stale"
        };
        if status != "failed"
            && s.status == "live"
            && s.health_at.elapsed() <= Duration::from_millis(v2::DEFAULT_MAX_LAG_MS)
            && s.selected_age
                .saturating_add(s.selected_at.elapsed().as_millis() as u64)
                <= v2::DEFAULT_MAX_LAG_MS
            && lag.is_some_and(|n| n <= v2::DEFAULT_MAX_LAG_MS)
            && observed.is_some_and(|t| v2::clock_check(t, now).is_ok())
        {
            status = "live";
        }
        Head {
            schema: v2::HEAD.into(),
            profile: v2::PROFILE.into(),
            source: self.source.clone(),
            cursor: Some(
                s.events
                    .back()
                    .map_or_else(|| s.base.clone(), |f| f.cursor.clone()),
            ),
            earliest_after: Some(s.base.clone()),
            status: status.into(),
            sampled_at_unix_ms: now,
            source_observed_at_unix_ms: observed,
            source_lag_ms: lag,
            latest_confirmed: latest,
        }
    }
    pub fn bootstrap(&self) -> Bootstrap {
        let s = self.state.lock().unwrap();
        Bootstrap {
            source: self.source.clone(),
            cursor: s
                .events
                .back()
                .map_or_else(|| s.base.clone(), |f| f.cursor.clone()),
            chain: s.chain.clone(),
        }
    }
    pub fn install_bootstrap(&self, b: Bootstrap, dependencies: Vec<Frame>) -> Result<()> {
        let mut old = self.state.lock().unwrap();
        ensure!(
            b.source == self.source
                && old.events.is_empty()
                && old.frames.is_empty()
                && old.chain.banks.is_empty()
                && (old.base == b.cursor
                    || old.base == v2::cursor(&self.source.session_id, 0, "-")),
            "invalid_bootstrap"
        );
        let mut s = old.clone();
        s.base = b.cursor.clone();
        s.chain = b.chain;
        s.chain.validate(&self.source, &b.cursor)?;
        ensure!(
            s.chain.banks.len() <= v2::chain::MAX_BANKS,
            "bank_queue_overflow"
        );
        for f in dependencies {
            ensure!(f.source == self.source, "source_changed");
            s.frames.insert(f.sha256.clone(), Arc::new(f));
        }
        if let Some(latest) = &s.chain.latest {
            v2::clock_check(latest.observed_at_unix_ms, now_ms())?;
            s.selected_at = Instant::now();
            s.selected_age = now_ms().saturating_sub(latest.observed_at_unix_ms);
        }
        ensure!(Self::bytes(&s) <= self.limit, "dependency_unavailable");
        self.save(&s, &old)?;
        *old = s;
        Ok(())
    }
    pub fn admit(&self, f: Frame, dependency: Option<Frame>) -> Result<()> {
        self.admit_batch(vec![(f, dependency)])
    }
    /// Bound the extra intake memory and share one durable checkpoint commit.
    /// No staged frame or head is visible before every frame and the checkpoint
    /// have been synced. A failed batch leaves the acknowledged boundary intact.
    pub fn admit_batch(&self, batch: Vec<(Frame, Option<Frame>)>) -> Result<()> {
        ensure!(
            !batch.is_empty() && batch.len() <= MAX_BATCH_EVENTS,
            "invalid_batch_size"
        );
        let mut old = self.state.lock().unwrap();
        let mut s = old.clone();
        for (f, dependency) in batch {
            ensure!(f.source == self.source, "source_changed");
            v2::clock_check(f.envelope.event.observed_at_unix_ms, now_ms())?;
            if s.events.back().is_some_and(|p| p.cursor == f.cursor) {
                continue;
            }
            let previous = s.events.back().map_or(&s.base, |p| &p.cursor);
            ensure!(
                f.envelope.event.sequence == v2::parse_cursor(previous)?.1 + 1,
                "event_sequence_gap"
            );
            let prior = s.chain.latest.clone();
            if let Some(dep) = dependency {
                ensure!(
                    dep.source == self.source && f.envelope.event.source_sequence > 0,
                    "source_changed"
                );
                s.frames.insert(dep.sha256.clone(), Arc::new(dep));
            }
            if f.envelope.event.kind == "block" {
                s.chain.add_payload(&f)?;
            } else {
                let sha = f
                    .envelope
                    .event
                    .payload_frame_sha256
                    .as_ref()
                    .context("missing_payload_frame")?;
                let payload = s.frames.get(sha).context("dependency_unavailable")?;
                s.chain.apply_control(&f, payload)?;
            }
            if s.chain.latest != prior {
                let t = s
                    .chain
                    .latest
                    .as_ref()
                    .map_or(now_ms(), |h| h.observed_at_unix_ms);
                s.selected_age = now_ms().saturating_sub(t);
                s.selected_at = Instant::now();
            }
            let f = Arc::new(f);
            s.frames.insert(f.sha256.clone(), f.clone());
            s.events.push_back(f);
        }
        loop {
            let pins = Self::pins(&s)?;
            s.frames.retain(|sha, _| pins.contains(sha));
            if Self::bytes(&s) <= self.limit && s.events.len() <= 65536 {
                break;
            }
            let old = s.events.pop_front().context("dependency_unavailable")?;
            s.base = old.cursor.clone();
            ensure!(!s.events.is_empty(), "dependency_unavailable");
        }
        self.save(&s, &old)?;
        *old = s;
        drop(old);
        self.notify.notify_waiters();
        Ok(())
    }
    pub fn after(&self, cursor: &str) -> Result<Option<Arc<Frame>>> {
        let (session, n, _) = v2::parse_cursor(cursor)?;
        ensure!(session == self.source.session_id, "source_changed");
        let s = self.state.lock().unwrap();
        if cursor == s.base {
            return Ok(s.events.front().cloned());
        }
        let base = v2::parse_cursor(&s.base)?.1;
        ensure!(n >= base, "replay_unavailable");
        let i = s
            .events
            .iter()
            .position(|f| f.cursor == cursor)
            .context("invalid_cursor")?;
        Ok(s.events.get(i + 1).cloned())
    }
    pub fn chunk(&self, sha: &str) -> Option<Arc<Frame>> {
        self.state.lock().unwrap().frames.get(sha).cloned()
    }
    pub fn metrics(&self) -> serde_json::Value {
        let s = self.state.lock().unwrap();
        let times: Vec<_> = s
            .events
            .iter()
            .filter(|f| f.envelope.event.kind == "block")
            .map(|f| f.envelope.event.observed_at_unix_ms)
            .collect();
        serde_json::json!({"replay_bytes":Self::bytes(&s),"replay_limit_bytes":self.limit,"retained_events":s.events.len(),"retained_frames":s.frames.len(),"replay_duration_ms":times.first().zip(times.last()).map(|(a,b)|b.saturating_sub(*a)),"earliest_after":s.base})
    }
}
