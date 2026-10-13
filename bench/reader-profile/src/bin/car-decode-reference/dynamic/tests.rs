use super::super::sharded::BlockRange;
use super::super::{Decoder, MetadataMode, common_export, scan};
use super::*;
use blockzilla_source::{RangeSource, SourceError, SourceResult};
use of_car_reader::{CarBlockReader, OrderedLosslessCarBlock, reconstruct::Cid36};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

const LEGACY: &[u8] = include_bytes!(
    "../../../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-157-biggest.car"
);
const MODERN: &[u8] = include_bytes!(
    "../../../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car"
);

struct Fixture {
    bytes: Arc<Vec<u8>>,
    ranges: Vec<BlockRange>,
    first_transaction_bytes: Vec<u8>,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let mut bytes = vec![0];
        let mut ranges = Vec::new();
        let mut first_transaction_bytes = Vec::new();
        for (number, (input, expected_transactions)) in
            [(LEGACY, 4208), (MODERN, 2969)].into_iter().enumerate()
        {
            let mut reader = CarBlockReader::with_capacity(input, 8192);
            reader.skip_header().unwrap();
            let header = reader.offset as usize;
            let mut raw = OrderedLosslessCarBlock::default();
            assert!(reader.read_until_block_ordered_lossless(&mut raw).unwrap());
            assert_eq!(raw.transactions.len(), expected_transactions);
            assert_eq!(reader.offset as usize, input.len());
            let slot = raw.block.as_ref().unwrap().slot;
            let start = bytes.len() as u64;
            bytes.extend_from_slice(&input[header..]);
            ranges.push(BlockRange {
                slot,
                transactions: expected_transactions as u64,
                start,
                end: bytes.len() as u64,
            });
            if number == 0 {
                first_transaction_bytes = raw.transactions[0].data.data.clone();
                // Keep two real blocks unchanged. Many small, valid empty
                // blocks between them exercise more jobs than the pool holds.
                for next in slot + 1..slot + 65 {
                    let start = bytes.len() as u64;
                    let mut payload = vec![0x85, 2, 0x1a];
                    payload.extend_from_slice(&(next as u32).to_be_bytes());
                    payload.extend_from_slice(&[0x80, 0x80, 0x83, 0xf6, 0xf6, 0xf6]);
                    assert!(36 + payload.len() < 128);
                    bytes.push((36 + payload.len()) as u8);
                    bytes.extend_from_slice(Cid36::compute(&payload).car_bytes());
                    bytes.extend_from_slice(&payload);
                    ranges.push(BlockRange {
                        slot: next,
                        transactions: 0,
                        start,
                        end: bytes.len() as u64,
                    });
                }
            }
        }
        assert!(ranges.windows(2).all(|pair| pair[0].slot < pair[1].slot));
        Fixture {
            bytes: Arc::new(bytes),
            ranges,
            first_transaction_bytes,
        }
    })
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Protocol,
    Short,
    Panic,
}

#[derive(Default)]
struct Observed {
    later_started: bool,
    completed: Vec<(usize, u64, usize)>,
}

#[derive(Clone)]
struct Memory {
    bytes: Arc<Vec<u8>>,
    client: usize,
    first: u64,
    later: u64,
    fault: Fault,
    reorder: bool,
    observed: Arc<(Mutex<Observed>, Condvar)>,
}

impl Memory {
    fn new(bytes: Arc<Vec<u8>>, ranges: &[BlockRange]) -> Self {
        Self {
            bytes,
            client: 0,
            first: ranges[0].start,
            later: ranges[1].start,
            fault: Fault::None,
            reorder: false,
            observed: Arc::new((Mutex::new(Observed::default()), Condvar::new())),
        }
    }
}

impl RangeSource for Memory {
    fn size(&self, _: &str) -> SourceResult<Option<u64>> {
        Ok(Some(self.bytes.len() as u64))
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        if offset == self.first {
            match self.fault {
                Fault::Protocol => {
                    return Err(SourceError::Protocol("injected identity mismatch".into()));
                }
                Fault::Short => return Ok(vec![0; length.saturating_sub(1)]),
                Fault::Panic => panic!("injected source panic"),
                Fault::None => (),
            }
        }
        let (state, condition) = &*self.observed;
        if self.reorder && offset == self.first {
            let observed = state.lock().unwrap();
            let (observed, timeout) = condition
                .wait_timeout_while(observed, Duration::from_secs(5), |value| {
                    !value.later_started
                })
                .unwrap();
            if timeout.timed_out() && !observed.later_started {
                return Err(SourceError::Protocol(
                    "later dynamic job never completed a read".into(),
                ));
            }
        }
        let end = offset as usize + length;
        let bytes = self
            .bytes
            .get(offset as usize..end)
            .ok_or_else(|| SourceError::OutOfBounds {
                object: object.into(),
                offset,
                length,
                size: self.bytes.len() as u64,
            })?
            .to_vec();
        let mut observed = state.lock().unwrap();
        observed.completed.push((self.client, offset, length));
        if offset >= self.later {
            observed.later_started = true;
            condition.notify_all();
        }
        Ok(bytes)
    }
}

fn config(parsers: usize, decoders: usize) -> Config {
    Config {
        parsers,
        decoders,
        job_bytes: 1,
        chunk_bytes: 1 << 20,
        reuse: true,
    }
}

fn within_deadline<T: Send + 'static>(run: impl FnOnce() -> T + Send + 'static) -> T {
    let (sender, receiver) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        sender.send(run()).ok();
    });
    let value = receiver
        .recv_timeout(Duration::from_secs(15))
        .expect("dynamic pipeline did not return; possible cancellation deadlock");
    handle.join().expect("pipeline test thread panicked");
    value
}

struct TemporaryExport(PathBuf);

impl TemporaryExport {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(std::env::temp_dir().join(format!(
            "car-dynamic-test-{}-{}.bin",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        )))
    }
}

impl Drop for TemporaryExport {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let mut spool = self.0.as_os_str().to_owned();
        spool.push(".spool");
        let _ = fs::remove_file(PathBuf::from(spool));
    }
}

#[test]
fn real_blocks_and_empty_blocks_match_sequential_rows_and_exact_export() {
    let data = fixture();
    let plan: Vec<_> = data
        .ranges
        .iter()
        .map(|row| [row.slot, row.transactions])
        .collect();
    let reference_path = TemporaryExport::new();
    let reference = common_export::Export::create(&reference_path.0).unwrap();
    let expected = scan(
        data.bytes.as_slice(),
        u64::MAX,
        1,
        MetadataMode::Owned,
        Some(&reference),
    )
    .unwrap();
    reference.finish(&plan).unwrap();
    let reference_bytes = fs::read(&reference_path.0).unwrap();
    assert_eq!(expected.len(), 66);
    assert_eq!(expected.iter().map(|row| row.tx).sum::<u64>(), 7177);
    for (parsers, decoders, clients, reorder, reuse, chunk_bytes) in [
        (1, 1, 1, false, true, 1 << 20),
        (2, 1, 2, true, true, 1 << 20),
        (4, 3, 2, false, false, 1 << 20),
        (1, 1, 1, false, true, 128 << 20),
        (1, 1, 1, false, false, 128 << 20),
    ] {
        let mut memory = Memory::new(Arc::clone(&data.bytes), &data.ranges);
        memory.reorder = reorder;
        let sources: Vec<_> = (0..clients)
            .map(|client| Memory {
                client,
                ..memory.clone()
            })
            .collect();
        let ranges = data.ranges.clone();
        let (actual, bytes, metrics) = within_deadline(move || {
            let path = TemporaryExport::new();
            let export = common_export::Export::create(&path.0).unwrap();
            let retries = AtomicU64::new(0);
            let metrics = Metrics::default();
            let rows = scan_dynamic(
                &sources,
                "test.car",
                &ranges,
                Config {
                    reuse,
                    chunk_bytes,
                    ..config(parsers, decoders)
                },
                MetadataMode::Owned,
                Some(&export),
                &retries,
                &metrics,
            )
            .unwrap();
            let plan: Vec<_> = ranges
                .iter()
                .map(|row| [row.slot, row.transactions])
                .collect();
            export.finish(&plan).unwrap();
            assert_eq!(retries.load(Ordering::Relaxed), 0);
            (rows, fs::read(&path.0).unwrap(), metrics.receipt())
        });
        assert_eq!(actual, expected);
        assert_eq!(bytes, reference_bytes);
        let pool_size = decoders + 2 * parsers;
        assert_eq!(metrics["raw_block_pool_size"], pool_size);
        assert_eq!(metrics["work_queue_capacity"], pool_size);
        assert_eq!(metrics["job_count"], data.ranges.len());
        assert_eq!(metrics["jobs_claimed"], data.ranges.len());
        assert_eq!(metrics["jobs_completed"], data.ranges.len());
        assert_eq!(metrics["blocks_parsed"], data.ranges.len());
        assert_eq!(metrics["blocks_decoded"], data.ranges.len());
        assert_eq!(metrics["parser_workers_finished"], parsers);
        assert_eq!(metrics["decode_workers_finished"], decoders);
        assert!(metrics["max_ready_queue_depth"].as_u64().unwrap() <= pool_size as u64);
        let observed = memory.observed.0.lock().unwrap();
        if reorder {
            assert!(observed.later_started);
            assert_ne!(observed.completed[0].1, memory.first);
            for client in 0..clients {
                assert!(observed.completed.iter().any(|row| row.0 == client));
            }
        }
        let mut reads: Vec<_> = observed
            .completed
            .iter()
            .map(|row| (row.1, row.2))
            .collect();
        reads.sort_unstable();
        let mut next = data.ranges[0].start;
        for (start, size) in reads {
            assert_eq!(start, next, "duplicate, skipped, or overlapping input read");
            next += size as u64;
        }
        assert_eq!(next, data.ranges.last().unwrap().end);
    }
}

#[test]
fn byte_jobs_cover_each_block_once_and_keep_large_blocks_whole() {
    let data = fixture();
    for target in [1, 100, 1 << 20, 4 << 20, u64::MAX] {
        let jobs = plan_jobs(&data.ranges, target).unwrap();
        let mut next = 0;
        for job in &jobs {
            assert_eq!(job.first, next);
            assert!(job.end > job.first && job.end <= data.ranges.len());
            next = job.end;
        }
        assert_eq!(next, data.ranges.len());
        if target == 1 {
            assert_eq!(jobs.len(), data.ranges.len());
        }
        if target == u64::MAX {
            assert_eq!(jobs.len(), 1);
        }
    }
    assert!(plan_jobs(&data.ranges, 0).is_err());
    assert!(plan_jobs(&[], 1).is_err());
}

#[test]
fn invalid_worker_client_chunk_and_job_bounds_fail_before_source_reads() {
    let data = fixture();
    let good = config(2, 2);
    for settings in [
        Config { parsers: 0, ..good },
        Config {
            parsers: 129,
            decoders: 129,
            ..good
        },
        Config {
            decoders: 0,
            ..good
        },
        Config {
            decoders: 257,
            ..good
        },
        Config {
            chunk_bytes: 0,
            ..good
        },
        Config {
            chunk_bytes: (128 << 20) + 1,
            ..good
        },
        Config {
            chunk_bytes: usize::MAX,
            ..good
        },
        Config {
            job_bytes: 0,
            ..good
        },
    ] {
        let memory = Memory::new(Arc::clone(&data.bytes), &data.ranges);
        assert!(
            scan_dynamic(
                &[memory.clone()],
                "test.car",
                &data.ranges,
                settings,
                MetadataMode::Owned,
                None,
                &AtomicU64::new(0),
                &Metrics::default()
            )
            .is_err()
        );
        assert!(memory.observed.0.lock().unwrap().completed.is_empty());
    }
    let sources: [Memory; 0] = [];
    assert!(
        scan_dynamic(
            &sources,
            "test.car",
            &data.ranges,
            good,
            MetadataMode::Owned,
            None,
            &AtomicU64::new(0),
            &Metrics::default()
        )
        .is_err()
    );
    let memory = Memory::new(Arc::clone(&data.bytes), &data.ranges);
    let sources = vec![memory.clone(); 3];
    assert!(
        scan_dynamic(
            &sources,
            "test.car",
            &data.ranges,
            good,
            MetadataMode::Owned,
            None,
            &AtomicU64::new(0),
            &Metrics::default()
        )
        .is_err()
    );
    assert!(memory.observed.0.lock().unwrap().completed.is_empty());
}

#[test]
fn source_errors_short_bodies_and_panics_cancel_all_workers() {
    for fault in [Fault::Protocol, Fault::Short, Fault::Panic] {
        let data = fixture();
        let ranges = data.ranges.clone();
        let memory = Memory {
            fault,
            ..Memory::new(Arc::clone(&data.bytes), &ranges)
        };
        let error = within_deadline(move || {
            let metrics = Metrics::default();
            scan_dynamic(
                &[memory],
                "test.car",
                &ranges,
                config(4, 1),
                MetadataMode::Owned,
                None,
                &AtomicU64::new(0),
                &metrics,
            )
            .unwrap_err()
        });
        let message = format!("{error:#}");
        match fault {
            Fault::Protocol => assert!(message.contains("injected identity mismatch"), "{message}"),
            Fault::Short => assert!(
                message.contains("length") || message.contains("short"),
                "{message}"
            ),
            Fault::Panic => assert!(message.contains("panic"), "{message}"),
            Fault::None => unreachable!(),
        }
    }
}

#[test]
fn decode_failure_with_more_jobs_than_pool_slots_returns_without_deadlock() {
    let data = fixture();
    let mut bytes = data.bytes.as_ref().clone();
    let position = bytes
        .windows(data.first_transaction_bytes.len())
        .position(|window| window == data.first_transaction_bytes)
        .unwrap();
    bytes[position..position + 3].copy_from_slice(&[0xff; 3]);
    // This fault is inside a transaction's opaque data. CAR framing, block
    // geometry, and node counts still parse; the transaction decoder fails.
    let mut reader = CarBlockReader::with_capacity(bytes.as_slice(), 8192);
    reader.skip_header().unwrap();
    let mut raw = OrderedLosslessCarBlock::default();
    assert!(reader.read_until_block_ordered_lossless(&mut raw).unwrap());
    assert_eq!(raw.transactions.len(), 4208);
    assert!(
        Decoder::default()
            .decode(&raw, MetadataMode::Owned, None)
            .is_err()
    );
    let ranges = data.ranges.clone();
    let memory = Memory::new(Arc::new(bytes), &ranges);
    within_deadline(move || {
        let result = scan_dynamic(
            &[memory],
            "test.car",
            &ranges,
            config(4, 1),
            MetadataMode::Owned,
            None,
            &AtomicU64::new(0),
            &Metrics::default(),
        );
        assert!(result.is_err(), "invalid transaction was accepted");
    });
}

#[test]
fn export_failure_cancels_producers_with_pending_jobs() {
    let data = fixture();
    let ranges = data.ranges.clone();
    let memory = Memory::new(Arc::clone(&data.bytes), &ranges);
    within_deadline(move || {
        let path = TemporaryExport::new();
        let export = common_export::Export::create(&path.0).unwrap();
        // Preseed the first slot to force a real Export::block failure from
        // the decoder, without a test-only decoder or writer implementation.
        export.block(ranges[0].slot, 1, &[1]).unwrap();
        let error = scan_dynamic(
            &[memory],
            "test.car",
            &ranges,
            config(4, 1),
            MetadataMode::Owned,
            Some(&export),
            &AtomicU64::new(0),
            &Metrics::default(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("duplicate export block"));
    });
}

#[test]
fn wrong_counts_slots_geometry_and_duplicate_jobs_cannot_pass_without_export() {
    let data = fixture();
    let mut variants = Vec::new();
    let mut wrong_count = data.ranges.clone();
    wrong_count[0].transactions += 1;
    variants.push(wrong_count);
    let mut wrong_slot = data.ranges.clone();
    wrong_slot[0].slot -= 1;
    variants.push(wrong_slot);
    let mut wrong_end = data.ranges.clone();
    wrong_end[0].end -= 1;
    variants.push(wrong_end);
    let mut duplicate = data.ranges.clone();
    duplicate.insert(1, duplicate[0].clone());
    variants.push(duplicate);
    for ranges in variants {
        let memory = Memory::new(Arc::clone(&data.bytes), &data.ranges);
        within_deadline(move || {
            assert!(
                scan_dynamic(
                    &[memory],
                    "test.car",
                    &ranges,
                    config(3, 1),
                    MetadataMode::Owned,
                    None,
                    &AtomicU64::new(0),
                    &Metrics::default()
                )
                .is_err()
            );
        });
    }
}

#[test]
fn cancellation_is_broadcast_to_every_waiter_and_keeps_the_first_error() {
    within_deadline(|| {
        let cancel = Arc::new(Cancel::new());
        let mut waiters = Vec::new();
        for _ in 0..8 {
            let receiver = cancel.receiver().clone();
            waiters.push(std::thread::spawn(move || {
                assert!(matches!(
                    receiver.recv_timeout(Duration::from_secs(5)),
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected)
                ));
            }));
        }
        cancel.fail(anyhow::anyhow!("first injected failure"));
        cancel.fail(anyhow::anyhow!("later injected failure"));
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert!(cancel.is_cancelled());
        assert!(
            cancel
                .take_error()
                .unwrap()
                .to_string()
                .contains("first injected failure")
        );
    });
}

#[test]
fn cancellation_wakes_a_production_send_blocked_on_a_full_queue() {
    within_deadline(|| {
        let cancel = Arc::new(Cancel::new());
        let (sender, receiver) = crossbeam_channel::bounded(1);
        sender.send(11).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker_cancel = Arc::clone(&cancel);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(send_or_cancel(&sender, 22, &worker_cancel))
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(receiver.len(), 1);
        cancel.fail(anyhow::anyhow!("stop while queue is full"));
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            false
        );
        worker.join().unwrap();
        assert_eq!(receiver.recv().unwrap(), 11);
        assert!(receiver.try_recv().is_err());
    });
}
