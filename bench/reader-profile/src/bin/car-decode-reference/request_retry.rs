//! Benchmark-only retry policy for one exact sanitized request-send error.
//!
//! The shared source discards the typed reqwest cause. This message can also
//! describe a persistent failure; bounded retry does not prove transience.
use blockzilla_source::{RangeSource, SourceError, SourceResult};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

pub(super) const REQUEST_SEND_RETRY_LIMIT: usize = 2;
const BACKOFF_MS: [u64; REQUEST_SEND_RETRY_LIMIT] = [100, 300];

fn is_request_send_error(error: &SourceError) -> bool {
    matches!(error, SourceError::Protocol(message)
        if message == "HTTP request failed (error sending request)")
}

pub(super) fn read_range(
    source: &(impl RangeSource + ?Sized),
    object: &str,
    offset: u64,
    length: usize,
    destination: &mut Vec<u8>,
    retries: &AtomicU64,
) -> SourceResult<()> {
    read_range_with_wait(
        source,
        object,
        offset,
        length,
        destination,
        retries,
        thread::sleep,
    )
}

fn read_range_with_wait(
    source: &(impl RangeSource + ?Sized),
    object: &str,
    offset: u64,
    length: usize,
    destination: &mut Vec<u8>,
    retries: &AtomicU64,
    mut wait: impl FnMut(Duration),
) -> SourceResult<()> {
    for attempt in 0..=REQUEST_SEND_RETRY_LIMIT {
        // Discard any partial bytes before each attempt. No input offset moves
        // until the complete range passes the source's normal validation.
        destination.clear();
        match source.read_range_into(object, offset, length, destination) {
            Ok(()) => return Ok(()),
            Err(error) if attempt < REQUEST_SEND_RETRY_LIMIT && is_request_send_error(&error) => {
                let delay = Duration::from_millis(BACKOFF_MS[attempt]);
                eprintln!(
                    "[car-range-send-retry] object={object} bytes={offset}..{} retry={} retry_limit={REQUEST_SEND_RETRY_LIMIT} backoff_ms={}",
                    offset.saturating_add(length as u64),
                    attempt + 1,
                    delay.as_millis(),
                );
                wait(delay);
                retries.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded request-send retry loop always returns")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, io, sync::Mutex};

    struct Scripted {
        results: Mutex<VecDeque<SourceResult<Vec<u8>>>>,
        requests: Mutex<Vec<(String, u64, usize)>>,
    }
    impl Scripted {
        fn new(results: Vec<SourceResult<Vec<u8>>>) -> Self {
            Self {
                results: Mutex::new(results.into()),
                requests: Mutex::new(Vec::new()),
            }
        }
    }
    impl RangeSource for Scripted {
        fn size(&self, _: &str) -> SourceResult<Option<u64>> {
            Ok(Some(1000))
        }
        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            self.requests
                .lock()
                .unwrap()
                .push((object.to_owned(), offset, length));
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra request")
        }
        fn read_range_into(
            &self,
            object: &str,
            offset: u64,
            length: usize,
            destination: &mut Vec<u8>,
        ) -> SourceResult<()> {
            // Model a previous partial response followed by a send failure.
            destination.extend_from_slice(b"discard");
            let bytes = self.read_range(object, offset, length)?;
            destination.clear();
            destination.extend_from_slice(&bytes);
            Ok(())
        }
    }
    fn send_error() -> SourceError {
        SourceError::Protocol("HTTP request failed (error sending request)".into())
    }

    #[test]
    fn two_send_failures_retry_the_same_exact_range_then_publish_valid_bytes() {
        let source = Scripted::new(vec![
            Err(send_error()),
            Err(send_error()),
            Ok(vec![4, 5, 6]),
        ]);
        let retries = AtomicU64::new(0);
        let mut destination = vec![9; 20];
        let mut waits = Vec::new();
        read_range_with_wait(
            &source,
            "epoch.car",
            97,
            3,
            &mut destination,
            &retries,
            |delay| waits.push(delay),
        )
        .unwrap();
        assert_eq!(destination, [4, 5, 6]);
        assert_eq!(
            *source.requests.lock().unwrap(),
            vec![("epoch.car".to_owned(), 97, 3); 3]
        );
        assert_eq!(retries.load(Ordering::Relaxed), 2);
        assert_eq!(
            waits,
            [Duration::from_millis(100), Duration::from_millis(300)]
        );
    }

    #[test]
    fn persistent_send_failure_stops_after_three_attempts() {
        let source = Scripted::new(vec![
            Err(send_error()),
            Err(send_error()),
            Err(send_error()),
        ]);
        let retries = AtomicU64::new(0);
        let mut waits = 0;
        let error = read_range_with_wait(
            &source,
            "epoch.car",
            97,
            3,
            &mut Vec::new(),
            &retries,
            |_| waits += 1,
        )
        .unwrap_err();
        assert!(is_request_send_error(&error));
        assert_eq!(source.requests.lock().unwrap().len(), 3);
        assert_eq!(retries.load(Ordering::Relaxed), 2);
        assert_eq!(waits, 2);
    }

    #[test]
    fn a_retry_still_fails_when_the_next_response_changes_identity() {
        let source = Scripted::new(vec![
            Err(send_error()),
            Err(SourceError::Protocol(
                "object epoch.car changed strong ETag".into(),
            )),
        ]);
        let retries = AtomicU64::new(0);
        let error = read_range_with_wait(
            &source,
            "epoch.car",
            97,
            3,
            &mut Vec::new(),
            &retries,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "remote source protocol error: object epoch.car changed strong ETag"
        );
        assert_eq!(
            *source.requests.lock().unwrap(),
            vec![("epoch.car".to_owned(), 97, 3); 2]
        );
        assert_eq!(retries.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn protocol_identity_and_exhausted_body_errors_are_not_retried() {
        for error in [
            SourceError::Protocol(
                "range GET for epoch.car returned an unexpected Content-Range".into(),
            ),
            SourceError::Protocol("object epoch.car changed strong ETag".into()),
            SourceError::Protocol("range GET for epoch.car returned HTTP 200, expected 206".into()),
            SourceError::Protocol("HTTP request failed (builder error)".into()),
            SourceError::ShortRead {
                object: "epoch.car".into(),
                expected: 3,
                actual: 2,
            },
            SourceError::Io {
                object: "epoch.car".into(),
                source: io::Error::other("response body error"),
            },
            SourceError::NotFound("epoch.car".into()),
        ] {
            let expected = error.to_string();
            let source = Scripted::new(vec![Err(error)]);
            let retries = AtomicU64::new(0);
            let actual = read_range_with_wait(
                &source,
                "epoch.car",
                97,
                3,
                &mut Vec::new(),
                &retries,
                |_| panic!("unexpected retry delay"),
            )
            .unwrap_err();
            assert_eq!(actual.to_string(), expected);
            assert_eq!(source.requests.lock().unwrap().len(), 1);
            assert_eq!(retries.load(Ordering::Relaxed), 0);
        }
    }
}
