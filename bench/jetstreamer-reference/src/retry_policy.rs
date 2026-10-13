use std::collections::BTreeMap;

pub const BODY_ERROR: &str = "Error reading until block: error decoding response body";
pub const HEADER_BODY_ERROR: &str = "Error reading header: error decoding response body";
pub const MAX_RETRIES_PER_WORKER_SLOT: u8 = 2;
pub const MAX_SAMPLED_EVENTS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Strict,
    BodyTransport,
}

impl Policy {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "strict" => Ok(Self::Strict),
            "body-transport" => Ok(Self::BodyTransport),
            _ => Err("reported-error-policy must be strict or body-transport".into()),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::BodyTransport => "body-transport",
        }
    }
}

pub struct Observation<'a> {
    pub worker: usize,
    pub context_worker: usize,
    pub workers: usize,
    pub slot: u64,
    pub start: u64,
    pub end: u64,
    pub pending: u64,
    pub callback_slot: u64,
    pub buffer_empty: bool,
    pub message: &'a str,
    pub elapsed_seconds: f64,
}

pub struct Event {
    pub worker: usize,
    pub slot: u64,
    pub attempt: u8,
    pub elapsed_seconds: f64,
    pub error_message: &'static str,
}

#[derive(Default)]
pub struct RetryTracker {
    attempts: BTreeMap<(usize, u64), u8>,
    pub allowed: u64,
    pub events: Vec<Event>,
}

impl RetryTracker {
    pub fn observe(&mut self, policy: Policy, event: Observation<'_>) -> Result<u8, String> {
        if policy == Policy::Strict {
            return Err("strict policy rejects reported upstream errors".into());
        }
        let error_message = match event.message {
            BODY_ERROR => BODY_ERROR,
            HEADER_BODY_ERROR => HEADER_BODY_ERROR,
            _ => return Err("reported error is not a permitted response-body error".into()),
        };
        if event.worker != event.context_worker || event.worker >= event.workers {
            return Err("reported error has an invalid worker identity".into());
        }
        if !(event.start..event.end).contains(&event.slot) {
            return Err("reported error slot is outside the requested range".into());
        }
        // Upstream reads its header, then a complete block, before transaction callbacks.
        // A transport restart is safe only when this worker has no partial block.
        if event.pending != 0 || event.callback_slot != u64::MAX || !event.buffer_empty {
            return Err("reported body error has unfinished callback or export state".into());
        }
        let attempt = self.attempts.entry((event.worker, event.slot)).or_default();
        if *attempt >= MAX_RETRIES_PER_WORKER_SLOT {
            return Err("reported body error exceeded the worker/slot retry limit".into());
        }
        *attempt += 1;
        self.allowed += 1;
        if self.events.len() < MAX_SAMPLED_EVENTS {
            self.events.push(Event {
                worker: event.worker,
                slot: event.slot,
                attempt: *attempt,
                elapsed_seconds: event.elapsed_seconds,
                error_message,
            });
        }
        Ok(*attempt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Observation<'static> {
        Observation {
            worker: 1,
            context_worker: 1,
            workers: 2,
            slot: 101,
            start: 100,
            end: 110,
            pending: 0,
            callback_slot: u64::MAX,
            buffer_empty: true,
            message: BODY_ERROR,
            elapsed_seconds: 1.0,
        }
    }

    #[test]
    fn strict_and_non_transport_errors_stay_fatal() {
        let mut tracker = RetryTracker::default();
        assert!(tracker.observe(Policy::Strict, event()).is_err());
        let mut header = event();
        header.message = HEADER_BODY_ERROR;
        assert!(tracker.observe(Policy::Strict, header).is_err());
        for message in [
            "Error reading until block: Unknown CID version",
            "operation timed out",
            "Error reading until block: error decoding response body: changed",
            "Error reading header: uvarint overflow",
            "Error reading header: Header size too large",
            "Error reading header: error decoding response body: changed",
        ] {
            let mut observed = event();
            observed.message = message;
            assert!(tracker.observe(Policy::BodyTransport, observed).is_err());
        }
        assert_eq!(tracker.allowed, 0);
        assert!(tracker.events.is_empty());
    }

    #[test]
    fn reject_partial_state_and_invalid_identity_or_range() {
        for (message, field) in [BODY_ERROR, HEADER_BODY_ERROR]
            .into_iter()
            .flat_map(|message| (0..7).map(move |field| (message, field)))
        {
            let mut observed = event();
            observed.message = message;
            match field {
                0 => observed.pending = 1,
                1 => observed.callback_slot = 101,
                2 => observed.buffer_empty = false,
                3 => observed.context_worker = 0,
                4 => observed.workers = 1,
                5 => observed.slot = 99,
                _ => observed.slot = 110,
            }
            assert!(
                RetryTracker::default()
                    .observe(Policy::BodyTransport, observed)
                    .is_err()
            );
        }
    }

    #[test]
    fn header_and_block_errors_share_one_budget_and_preserve_actual_messages() {
        let mut tracker = RetryTracker::default();
        let mut header = event();
        header.message = HEADER_BODY_ERROR;
        assert_eq!(tracker.observe(Policy::BodyTransport, header).unwrap(), 1);
        assert_eq!(tracker.observe(Policy::BodyTransport, event()).unwrap(), 2);
        let mut third = event();
        third.message = HEADER_BODY_ERROR;
        assert!(tracker.observe(Policy::BodyTransport, third).is_err());
        assert_eq!(tracker.allowed, 2);
        assert_eq!(tracker.events[0].error_message, HEADER_BODY_ERROR);
        assert_eq!(tracker.events[1].error_message, BODY_ERROR);
    }

    #[test]
    fn exactly_two_retries_per_worker_and_slot() {
        let mut tracker = RetryTracker::default();
        assert_eq!(tracker.observe(Policy::BodyTransport, event()).unwrap(), 1);
        assert_eq!(tracker.observe(Policy::BodyTransport, event()).unwrap(), 2);
        assert!(tracker.observe(Policy::BodyTransport, event()).is_err());
        let mut next_slot = event();
        next_slot.slot += 1;
        assert_eq!(
            tracker.observe(Policy::BodyTransport, next_slot).unwrap(),
            1
        );
        let mut other_worker = event();
        other_worker.worker = 0;
        other_worker.context_worker = 0;
        assert_eq!(
            tracker
                .observe(Policy::BodyTransport, other_worker)
                .unwrap(),
            1
        );
        assert_eq!(tracker.allowed, 4);
        assert_eq!(tracker.events.len(), 4);
    }

    #[test]
    fn samples_are_bounded_but_total_remains_exact() {
        let mut tracker = RetryTracker::default();
        for slot in 100..200 {
            let mut observed = event();
            observed.slot = slot;
            observed.end = 200;
            tracker.observe(Policy::BodyTransport, observed).unwrap();
        }
        assert_eq!(tracker.allowed, 100);
        assert_eq!(tracker.events.len(), MAX_SAMPLED_EVENTS);
        assert_eq!(Policy::parse("strict").unwrap().name(), "strict");
        assert_eq!(
            Policy::parse("body-transport").unwrap().name(),
            "body-transport"
        );
        assert!(Policy::parse("all").is_err());
    }
}
