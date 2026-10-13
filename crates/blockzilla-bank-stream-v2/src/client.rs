//! Backend client admission. A processed payload never provides a usable hash.
//! Persist the application effects and `cursor()` in the same transaction.
use crate::*;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
#[derive(Clone)]
struct Proof {
    event: BankEvent,
    frame_sha256: String,
    rooted: bool,
    invalidated: bool,
}
pub struct ClientState {
    source: SourceIdentity,
    cursor: String,
    proofs: BTreeMap<String, Proof>,
    selected: Option<ConfirmedObservation>,
    selected_age: u64,
    selected_at: Instant,
    health: Option<Head>,
    health_at: Instant,
    failed: bool,
}
impl ClientState {
    pub fn new(source: SourceIdentity, after: String) -> Result<Self> {
        source.validate()?;
        ensure!(
            parse_cursor(&after)?.0 == source.session_id,
            "source_changed"
        );
        let now = Instant::now();
        Ok(Self {
            source,
            cursor: after,
            proofs: BTreeMap::new(),
            selected: None,
            selected_age: 0,
            selected_at: now,
            health: None,
            health_at: now,
            failed: false,
        })
    }
    pub fn cursor(&self) -> &str {
        &self.cursor
    }
    pub fn has_payload(&self, sha: &str) -> bool {
        self.proofs.contains_key(sha)
    }
    pub fn selected(&self) -> Option<&ConfirmedObservation> {
        self.selected.as_ref()
    }
    fn payload(&mut self, f: &Frame) -> Result<()> {
        ensure!(
            f.source == self.source && f.envelope.event.kind == "block",
            "source_changed"
        );
        if let Some(p) = self.proofs.get(&f.sha256) {
            ensure!(p.event == f.envelope.event, "payload_changed");
            return Ok(());
        }
        ensure!(
            self.proofs.len() < chain::MAX_BANKS,
            "client_bank_queue_overflow"
        );
        self.proofs.insert(
            f.sha256.clone(),
            Proof {
                event: f.envelope.event.clone(),
                frame_sha256: f.sha256.clone(),
                rooted: false,
                invalidated: false,
            },
        );
        Ok(())
    }
    fn select(&mut self, c: ConfirmedObservation, now: u64) -> Result<()> {
        clock_check(c.observed_at_unix_ms, now)?;
        if self
            .selected
            .as_ref()
            .is_none_or(|old| c.block_height > old.block_height)
        {
            self.selected_age = now.saturating_sub(c.observed_at_unix_ms);
            self.selected_at = Instant::now();
            self.selected = Some(c);
        }
        Ok(())
    }
    /// Resolve the head's exact immutable dependency. The operator-trusted
    /// head is the capture boundary; no earlier ancestor is inferred here.
    pub fn resolve_head(
        &mut self,
        head: &Head,
        control: &Frame,
        payload: &Frame,
        now: u64,
    ) -> Result<()> {
        head.validate(&self.source, now, DEFAULT_MAX_LAG_MS)?;
        ensure!(
            control.source == self.source
                && head.latest_confirmed.as_ref()
                    == Some(&ConfirmedObservation::from_frame(control)?),
            "head_control_mismatch"
        );
        control.envelope.event.matches_payload(payload)?;
        self.payload(payload)?;
        let c = ConfirmedObservation::from_frame(control)?;
        let p = self.proofs.get_mut(&payload.sha256).unwrap();
        ensure!(!p.invalidated, "invalidated_bank");
        p.rooted |= c.commitment == "finalized";
        self.select(c, now)
    }
    pub fn health(&mut self, h: Head, now: u64) -> Result<()> {
        h.validate(&self.source, now, DEFAULT_MAX_LAG_MS)?;
        // A stopped-source status immediately removes readiness. Identical
        // old health cannot renew the monotonic age of the selected bank.
        if h.status == "failed" {
            self.failed = true;
            self.selected = None;
        }
        self.health = Some(h);
        self.health_at = Instant::now();
        Ok(())
    }
    /// `false` means an exact duplicate of the last application boundary.
    pub fn apply(&mut self, f: &Frame, dependency: Option<&Frame>, now: u64) -> Result<bool> {
        ensure!(!self.failed && f.source == self.source, "source_failed");
        clock_check(f.envelope.event.observed_at_unix_ms, now)?;
        if f.cursor == self.cursor {
            return Ok(false);
        }
        let previous = parse_cursor(&self.cursor)?.1;
        ensure!(
            f.envelope.event.sequence == previous + 1,
            "event_sequence_gap"
        );
        let e = &f.envelope.event;
        if e.kind == "block" {
            self.payload(f)?;
        } else {
            let sha = e
                .payload_frame_sha256
                .as_ref()
                .context("missing_payload_frame")?;
            if let Some(p) = dependency {
                e.matches_payload(p)?;
                self.payload(p)?;
            }
            let p = self.proofs.get(sha).context("dependency_unavailable")?;
            ensure!(
                p.event.bank == e.bank
                    && p.event.payload_sha256 == e.payload_sha256
                    && p.frame_sha256 == *sha
                    && e.block_height
                        .is_none_or(|h| p.event.block_height == Some(h)),
                "payload_mismatch"
            );
            if e.kind == "invalidate" {
                let mut removed = BTreeSet::from([sha.clone()]);
                loop {
                    let next: Vec<_> = self
                        .proofs
                        .iter()
                        .filter(|(sha, p)| {
                            !removed.contains(*sha)
                                && self.proofs.iter().any(|(parent, q)| {
                                    removed.contains(parent)
                                        && p.event.bank.parent_matches(&q.event.bank)
                                })
                        })
                        .map(|(sha, _)| sha.clone())
                        .collect();
                    if next.is_empty() {
                        break;
                    }
                    removed.extend(next);
                }
                if removed.iter().any(|sha| self.proofs[sha].rooted) {
                    self.failed = true;
                    self.selected = None;
                    anyhow::bail!("rooted_bank_invalidated");
                }
                for sha in &removed {
                    self.proofs.get_mut(sha).unwrap().invalidated = true;
                }
                if self
                    .selected
                    .as_ref()
                    .is_some_and(|c| removed.contains(&c.payload_frame_sha256))
                {
                    self.selected = None;
                    self.failed = true;
                }
            } else {
                ensure!(!p.invalidated, "invalidated_bank");
                let c = ConfirmedObservation::from_frame(f)?;
                self.proofs.get_mut(sha).unwrap().rooted |= e.kind == "root";
                self.select(c, now)?;
            }
        }
        self.cursor = f.cursor.clone();
        if let Some(c) = &self.selected {
            if self.proofs.len() > 8192 {
                let floor = c.block_height.saturating_sub(4096);
                self.proofs
                    .retain(|_, p| p.event.block_height.is_some_and(|h| h >= floor));
            }
        }
        Ok(true)
    }
    pub fn ready_hash(&self, now: u64) -> Result<&RecentBlockhash> {
        ensure!(!self.failed, "source_failed");
        let h = self.health.as_ref().context("health_unavailable")?;
        h.validate(&self.source, now, DEFAULT_MAX_LAG_MS)?;
        ensure!(
            h.status == "live"
                && now.saturating_sub(h.sampled_at_unix_ms) <= DEFAULT_MAX_LAG_MS
                && self.health_at.elapsed() <= Duration::from_millis(DEFAULT_MAX_LAG_MS),
            "source_stale"
        );
        let c = self
            .selected
            .as_ref()
            .context("confirmed_hash_unavailable")?;
        let announced = h
            .latest_confirmed
            .as_ref()
            .context("confirmed_hash_unavailable")?;
        ensure!(
            (c == announced
                || (c.event_sequence > announced.event_sequence
                    && c.block_height > announced.block_height))
                && parse_cursor(&self.cursor)?.1
                    >= parse_cursor(h.cursor.as_deref().context("missing_head_cursor")?)?.1,
            "client_not_caught_up"
        );
        ensure!(
            self.selected_age
                .saturating_add(self.selected_at.elapsed().as_millis() as u64)
                <= 15_000
                && now.saturating_sub(c.observed_at_unix_ms) <= DEFAULT_MAX_LAG_MS,
            "confirmed_hash_expired_by_age"
        );
        c.recent_blockhash.validate(&c.bank, c.block_height)?;
        Ok(&c.recent_blockhash)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::*;
    #[test]
    fn processed_never_confirms_and_duplicate_is_idempotent() {
        let b = block(1, 17, 7, 3, 10);
        let mut s = ClientState::new(source(), cursor("fixture-session", 0, "-")).unwrap();
        assert!(s.apply(&b, None, 1000).unwrap());
        assert!(!s.apply(&b, None, 1000).unwrap());
        assert!(s.selected().is_none());
        assert!(s.ready_hash(1000).is_err());
    }
    #[test]
    fn exact_dependency_and_old_health_do_not_renew_monotonic_age() {
        let b = block(1, 17, 7, 3, 10);
        let c = control(&b, 2, "confirmed", 1000);
        let obs = ConfirmedObservation::from_frame(&c).unwrap();
        let h = Head {
            schema: HEAD.into(),
            profile: PROFILE.into(),
            source: source(),
            cursor: Some(c.cursor.clone()),
            earliest_after: Some(cursor("fixture-session", 0, "-")),
            status: "live".into(),
            sampled_at_unix_ms: 1000,
            source_observed_at_unix_ms: Some(1000),
            source_lag_ms: Some(0),
            latest_confirmed: Some(obs),
        };
        let mut s = ClientState::new(source(), c.cursor.clone()).unwrap();
        s.resolve_head(&h, &c, &b, 1000).unwrap();
        s.health(h.clone(), 1000).unwrap();
        s.ready_hash(1000).unwrap();
        s.selected_age = 16_000;
        s.health(h, 1000).unwrap();
        assert!(s.ready_hash(1000).is_err());
    }
}

#[cfg(test)]
mod delayed_health_tests {
    use super::*;
    use crate::fixtures::*;
    #[test]
    fn delayed_live_health_does_not_abort_replay_or_restore_readiness() {
        let b = block(1, 17, 7, 3, 10);
        let c = control(&b, 2, "confirmed", 1000);
        let h = Head {
            schema: HEAD.into(),
            profile: PROFILE.into(),
            source: source(),
            cursor: Some(c.cursor.clone()),
            earliest_after: Some(cursor("fixture-session", 0, "-")),
            status: "live".into(),
            sampled_at_unix_ms: 1000,
            source_observed_at_unix_ms: Some(1000),
            source_lag_ms: Some(0),
            latest_confirmed: Some(ConfirmedObservation::from_frame(&c).unwrap()),
        };
        let mut s = ClientState::new(source(), c.cursor.clone()).unwrap();
        s.resolve_head(&h, &c, &b, 1000).unwrap();
        s.health(h, 8000).unwrap();
        assert!(s.ready_hash(8000).is_err());
        s.apply(&control(&b, 3, "root", 1000), None, 8000).unwrap();
        assert!(s.ready_hash(8000).is_err());
    }
}
