//! Confirmation is bank-bound. An old control never renews a newer observation.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
pub const MAX_BANKS: usize = 16_384;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BankState {
    pub bank: BankIdentity,
    pub payload_sha256: String,
    pub payload_frame_sha256: String,
    pub block_height: u64,
    pub invalidated: bool,
    pub rooted: bool,
    pub confirmed: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chain {
    pub banks: BTreeMap<String, BankState>,
    pub latest: Option<ConfirmedObservation>,
    pub boundary: Option<(u64, [u8; 32])>,
    pub reconciliate: bool,
}
fn key(b: &BankIdentity) -> String {
    format!("{}:{}", b.slot, b.bank_id)
}
impl Chain {
    pub fn validate(&self, source: &SourceIdentity, cursor: &str) -> Result<()> {
        ensure!(self.banks.len() <= MAX_BANKS, "bank_queue_overflow");
        let (session, sequence, _) = parse_cursor(cursor)?;
        ensure!(session == source.session_id, "source_changed");
        for (k, b) in &self.banks {
            b.bank.validate()?;
            ensure!(
                *k == key(&b.bank)
                    && b.block_height <= SAFE
                    && is_digest(&b.payload_sha256)
                    && is_digest(&b.payload_frame_sha256)
                    && !(b.rooted && b.invalidated),
                "invalid_chain_state"
            );
        }
        if let Some((slot, _)) = self.boundary {
            ensure!(slot <= SAFE, "invalid_chain_boundary");
        }
        if let Some(c) = &self.latest {
            c.bank.validate()?;
            c.recent_blockhash.validate(&c.bank, c.block_height)?;
            let (session, n, sha) = parse_cursor(&c.event_cursor)?;
            let b = self
                .banks
                .get(&key(&c.bank))
                .context("head_bank_unavailable")?;
            ensure!(
                session == source.session_id
                    && n == c.event_sequence
                    && n > 0
                    && n <= sequence
                    && is_digest(sha)
                    && b.bank == c.bank
                    && b.payload_sha256 == c.payload_sha256
                    && b.payload_frame_sha256 == c.payload_frame_sha256
                    && b.block_height == c.block_height
                    && b.confirmed
                    && !b.invalidated
                    && matches!(c.commitment.as_str(), "confirmed" | "finalized"),
                "invalid_chain_head"
            );
            self.resolved(&c.bank)?;
        }
        Ok(())
    }
    pub fn add_payload(&mut self, f: &Frame) -> Result<()> {
        let e = &f.envelope.event;
        ensure!(e.kind == "block", "not_payload");
        let k = key(&e.bank);
        if let Some(old) = self.banks.get(&k) {
            ensure!(
                old.bank == e.bank
                    && old.payload_sha256 == e.payload_sha256
                    && old.payload_frame_sha256 == f.sha256,
                "contradictory_bank"
            );
            return Ok(());
        }
        ensure!(self.banks.len() < MAX_BANKS, "bank_queue_overflow");
        self.banks.insert(
            k,
            BankState {
                bank: e.bank.clone(),
                payload_sha256: e.payload_sha256.clone(),
                payload_frame_sha256: f.sha256.clone(),
                block_height: e.block_height.context("missing_height")?,
                invalidated: false,
                rooted: false,
                confirmed: false,
            },
        );
        Ok(())
    }
    pub fn seed(&mut self, observation: ConfirmedObservation, payload: &Frame) -> Result<()> {
        self.add_payload(payload)?;
        let b = self
            .banks
            .get_mut(&key(&observation.bank))
            .context("missing_payload")?;
        ensure!(
            b.bank == observation.bank
                && b.payload_sha256 == observation.payload_sha256
                && b.payload_frame_sha256 == observation.payload_frame_sha256
                && b.block_height == observation.block_height,
            "head_payload_mismatch"
        );
        b.confirmed = true;
        b.rooted = observation.commitment == "finalized";
        self.boundary = Some((b.bank.parent_slot, b.bank.parent_blockhash));
        self.latest = Some(observation);
        Ok(())
    }
    fn resolved(&self, bank: &BankIdentity) -> Result<Vec<String>> {
        let mut chain = Vec::new();
        let mut current = bank.clone();
        let mut visited = BTreeSet::new();
        loop {
            let k = key(&current);
            ensure!(visited.insert(k.clone()), "parent_cycle");
            let state = self.banks.get(&k).context("dependency_unavailable")?;
            ensure!(
                state.bank == current && !state.invalidated,
                "invalidated_ancestry"
            );
            chain.push(k);
            if state.rooted
                || self.boundary == Some((current.parent_slot, current.parent_blockhash))
            {
                return Ok(chain);
            }
            let mut parents = self
                .banks
                .values()
                .filter(|p| current.parent_matches(&p.bank) && !p.invalidated);
            let parent = parents.next().context("dependency_unavailable")?;
            ensure!(parents.next().is_none(), "ambiguous_parent_bank");
            ensure!(
                parent.block_height.checked_add(1) == Some(state.block_height),
                "parent_height_mismatch"
            );
            current = parent.bank.clone();
        }
    }
    pub fn can_confirm(&self, bank: &BankIdentity) -> Result<()> {
        self.resolved(bank).map(|_| ())
    }
    pub fn apply_control(&mut self, frame: &Frame, payload: &Frame) -> Result<()> {
        let e = &frame.envelope.event;
        e.matches_payload(payload)?;
        ensure!(frame.source == payload.source, "source_changed");
        self.add_payload(payload)?;
        let k = key(&e.bank);
        if e.kind == "invalidate" {
            let mut removed = BTreeSet::from([k.clone()]);
            loop {
                let next: Vec<_> = self
                    .banks
                    .iter()
                    .filter(|(k, b)| {
                        !removed.contains(*k)
                            && self.banks.iter().any(|(p, parent)| {
                                removed.contains(p) && b.bank.parent_matches(&parent.bank)
                            })
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                if next.is_empty() {
                    break;
                }
                removed.extend(next);
            }
            ensure!(
                !removed.iter().any(|k| self.banks[k].rooted),
                "rooted_bank_invalidated"
            );
            let confirmed = removed.iter().any(|k| self.banks[k].confirmed);
            for k in &removed {
                self.banks.get_mut(k).unwrap().invalidated = true;
            }
            if self
                .latest
                .as_ref()
                .is_some_and(|h| removed.contains(&key(&h.bank)))
            {
                self.latest = None;
            }
            self.reconciliate |= confirmed;
            return Ok(());
        }
        // The first exact native root is the trusted capture boundary. We do
        // not claim confirmation for uncaptured ancestors below that boundary.
        if e.kind == "root" && self.latest.is_none() && self.boundary.is_none() {
            self.boundary = Some((e.bank.parent_slot, e.bank.parent_blockhash));
        }
        self.resolved(&e.bank)?;
        let state = self.banks.get_mut(&k).unwrap();
        ensure!(
            state.block_height == e.block_height.context("missing_height")?,
            "confirmation_height_mismatch"
        );
        state.confirmed = true;
        state.rooted |= e.kind == "root";
        let next = ConfirmedObservation::from_frame(frame)?;
        if self
            .latest
            .as_ref()
            .is_none_or(|old| next.block_height > old.block_height)
        {
            self.latest = Some(next);
        }
        // Keep the latest root as the admitted ancestry anchor. The replay store
        // independently retains payload dependencies for older control events.
        if e.kind == "root" {
            let root_height = state.block_height;
            let root_bank = state.bank.clone();
            let selected_below = self
                .latest
                .as_ref()
                .is_some_and(|c| c.block_height < root_height);
            ensure!(!selected_below, "root_replaced_newer_head");
            let incompatible: Vec<_> = self
                .banks
                .iter()
                .filter(|(_, b)| {
                    b.block_height >= root_height
                        && b.bank != root_bank
                        && b.bank.slot == root_bank.slot
                })
                .map(|(k, _)| k.clone())
                .collect();
            ensure!(
                !incompatible
                    .iter()
                    .any(|k| self.banks[k].confirmed && !self.banks[k].invalidated),
                "competing_confirmed_root"
            );
            if self.banks.len() > MAX_BANKS / 2 {
                let floor = root_height.saturating_sub(4096);
                self.banks.retain(|_, b| b.block_height >= floor);
                if let Some(first) = self.banks.values().min_by_key(|b| b.block_height) {
                    self.boundary = Some((first.bank.parent_slot, first.bank.parent_blockhash));
                }
            }
        }
        Ok(())
    }
}
