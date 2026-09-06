// Included inside reader.rs's test module so these tests can use its archive
// fixtures. Timeouts are deadlock guards; assertions depend on event order.
mod incremental_pipeline_regressions {
    use super::*;

    const DEADLOCK_GUARD: Duration = Duration::from_secs(10);

    fn receive_signal(receiver: &Mutex<Receiver<()>>, event: &str) -> Result<()> {
        receiver
            .lock()
            .unwrap()
            .recv_timeout(DEADLOCK_GUARD)
            .map_err(|error| {
                Error::InvalidIndex(format!("pipeline test did not receive {event}: {error}"))
            })
    }

    struct ReadOverlapSource {
        inner: LocalRangeSource,
        block_reads: AtomicUsize,
        projection_started: Mutex<Receiver<()>>,
        second_read_finished: SyncSender<()>,
    }

    impl RangeSource for ReadOverlapSource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            let second_block_read =
                object == BLOCKS_FILE && self.block_reads.fetch_add(1, Ordering::SeqCst) == 1;
            if second_block_read {
                self.projection_started
                    .lock()
                    .unwrap()
                    .recv_timeout(DEADLOCK_GUARD)
                    .map_err(|error| {
                        SourceError::Protocol(format!(
                            "second read did not overlap the first projection: {error}"
                        ))
                    })?;
            }
            let bytes = self.inner.read_range(object, offset, length)?;
            if second_block_read {
                self.second_read_finished.send(()).map_err(|error| {
                    SourceError::Protocol(format!(
                        "first projection stopped before the second read finished: {error}"
                    ))
                })?;
            }
            Ok(bytes)
        }
    }

    #[test]
    fn ordered_incremental_producer_reads_while_projection_is_running() {
        let fixture = Fixture::parallel_blocks(3);
        let (projection_started, projection_receiver) = sync_channel(1);
        let (second_read_finished, read_receiver) = sync_channel(1);
        let read_receiver = Mutex::new(read_receiver);
        let source = ReadOverlapSource {
            inner: fixture.source(),
            block_reads: AtomicUsize::new(0),
            projection_started: Mutex::new(projection_receiver),
            second_read_finished,
        };
        let archive = ArchiveReader::open_with_options(
            source,
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        let mut consumed = Vec::new();

        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..3,
                OrderedParallelBlockConfig {
                    max_blocks_per_batch: 1,
                    compressed_buffer_count: 2,
                    decode_workers: 1,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, row, _| -> Result<usize> {
                    if row == 0 {
                        projection_started.send(()).unwrap();
                        receive_signal(&read_receiver, "completion of the second source read")?;
                    }
                    Ok(row)
                },
                |row, projected| {
                    assert_eq!(projected, row);
                    consumed.push(row);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, vec![0, 1, 2]);
        assert_eq!(stats.read_call_count, 3);
        assert_eq!(stats.batch_count, 3);
    }

    #[test]
    fn ordered_incremental_publishes_before_a_later_block_finishes() {
        let fixture = Fixture::parallel_blocks(2);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let (first_consumed, first_receiver) = sync_channel(1);
        let first_receiver = Mutex::new(first_receiver);
        let mut consumed = Vec::new();

        archive
            .process_borrowed_blocks_parallel_ordered(
                0..2,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, row, _| -> Result<usize> {
                    if row == 1 {
                        receive_signal(&first_receiver, "ordered consumption of block zero")?;
                    }
                    Ok(row)
                },
                |row, projected| {
                    assert_eq!(projected, row);
                    consumed.push(row);
                    if row == 0 {
                        first_consumed.send(()).unwrap();
                    }
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, vec![0, 1]);
    }

    #[test]
    fn ordered_incremental_refills_past_the_former_wave_boundary() {
        const WORKERS: usize = 2;
        const FORMER_WAVE: usize = WORKERS * 4;
        const BLOCKS: usize = FORMER_WAVE + 4;
        let fixture = Fixture::parallel_blocks(BLOCKS);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let (next_started, next_receiver) = sync_channel(1);
        let next_receiver = Mutex::new(next_receiver);
        let (first_consumed, first_receiver) = sync_channel(1);
        let first_receiver = Mutex::new(first_receiver);
        let mut consumed = Vec::new();

        archive
            .process_borrowed_blocks_parallel_ordered(
                0..BLOCKS,
                OrderedParallelBlockConfig {
                    decode_workers: WORKERS,
                    max_blocks_per_batch: BLOCKS,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, row, _| -> Result<usize> {
                    if row == FORMER_WAVE - 1 {
                        receive_signal(&next_receiver, "projection beyond the old wave boundary")?;
                    } else if row == FORMER_WAVE {
                        receive_signal(&first_receiver, "consumption before the old wave ends")?;
                        next_started.send(()).unwrap();
                    }
                    Ok(row)
                },
                |row, projected| {
                    assert_eq!(projected, row);
                    consumed.push(row);
                    if row == 0 {
                        first_consumed.send(()).unwrap();
                    }
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, (0..BLOCKS).collect::<Vec<_>>());
    }

    #[test]
    fn ordered_incremental_selects_the_earlier_error_after_a_later_error_finishes() {
        let fixture = Fixture::parallel_blocks(4);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let (later_failed, later_receiver) = sync_channel(1);
        let later_receiver = Mutex::new(later_receiver);
        let mut consumed = Vec::new();

        let error = archive
            .process_borrowed_blocks_parallel_ordered(
                0..4,
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, row, _| -> Result<usize> {
                    if row == 1 {
                        receive_signal(&later_receiver, "the later projection error")?;
                        return Err(Error::InvalidBlock {
                            slot: 102,
                            message: "earlier ordered projection error".into(),
                        });
                    }
                    if row == 2 {
                        later_failed.send(()).unwrap();
                        return Err(Error::InvalidBlock {
                            slot: 103,
                            message: "later ordered projection error".into(),
                        });
                    }
                    Ok(row)
                },
                |row, projected| {
                    assert_eq!(projected, row);
                    consumed.push(row);
                    Ok(())
                },
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidBlock { slot: 102, .. }));
        assert_eq!(consumed, vec![0]);
    }

    #[derive(Default)]
    struct OutputLifetimes {
        live: AtomicUsize,
        peak: AtomicUsize,
        created: AtomicUsize,
        dropped: AtomicUsize,
    }

    struct TrackedOutput(Arc<OutputLifetimes>);

    impl TrackedOutput {
        fn new(counts: &Arc<OutputLifetimes>) -> Self {
            let live = counts.live.fetch_add(1, Ordering::SeqCst) + 1;
            counts.peak.fetch_max(live, Ordering::SeqCst);
            counts.created.fetch_add(1, Ordering::SeqCst);
            Self(Arc::clone(counts))
        }
    }

    impl Drop for TrackedOutput {
        fn drop(&mut self) {
            self.0.live.fetch_sub(1, Ordering::SeqCst);
            self.0.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn run_full_output_window(cancel_at_first_sink: bool) {
        const WORKERS: usize = 2;
        const WINDOW: usize = 2 * (4 * WORKERS);
        const BLOCKS: usize = WINDOW * 4;
        let fixture = Fixture::parallel_blocks(BLOCKS);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let counts = Arc::new(OutputLifetimes::default());
        let (window_full, full_receiver) = sync_channel(1);
        let full_receiver = Mutex::new(full_receiver);
        let config = OrderedParallelBlockConfig {
            decode_workers: WORKERS,
            max_blocks_per_batch: BLOCKS / 2,
            // The producer must also stop waiting for this token on error.
            compressed_buffer_count: 1,
            ..OrderedParallelBlockConfig::default()
        };
        let mut consumed = Vec::new();

        let result = archive.process_borrowed_blocks_parallel_ordered(
            0..BLOCKS,
            config,
            |_| Ok(()),
            |_, _, _| -> Result<TrackedOutput> {
                let output = TrackedOutput::new(&counts);
                let live = counts.live.load(Ordering::SeqCst);
                if live > WINDOW {
                    return Err(Error::InvalidIndex(format!(
                        "{live} outputs exceeded the {WINDOW}-block window"
                    )));
                }
                if live == WINDOW {
                    // Only the first sink waits for this signal. Do not block
                    // if a later full window produces a second signal.
                    let _ = window_full.try_send(());
                }
                Ok(output)
            },
            |row, _output| {
                consumed.push(row);
                if row == 0 {
                    // Keep the first output alive until all remaining window
                    // slots fill. This checks that sink ownership uses credit.
                    receive_signal(&full_receiver, "a full window of live outputs")?;
                    assert_eq!(counts.live.load(Ordering::SeqCst), WINDOW);
                    if cancel_at_first_sink {
                        return Err(Error::InvalidMetadata("cancel at the first sink".into()));
                    }
                }
                Ok(())
            },
        );

        assert_eq!(counts.live.load(Ordering::SeqCst), 0);
        assert_eq!(
            counts.created.load(Ordering::SeqCst),
            counts.dropped.load(Ordering::SeqCst)
        );
        assert_eq!(counts.peak.load(Ordering::SeqCst), WINDOW);
        if cancel_at_first_sink {
            let error = result.unwrap_err();
            assert!(matches!(error, Error::InvalidMetadata(_)));
            assert!(error.to_string().contains("cancel at the first sink"));
            assert_eq!(consumed, vec![0]);
        } else {
            let stats = result.unwrap();
            assert_eq!(consumed, (0..BLOCKS).collect::<Vec<_>>());
            assert_eq!(counts.created.load(Ordering::SeqCst), BLOCKS);
            assert_eq!(stats.max_in_flight_blocks, WINDOW);
            assert_eq!(stats.max_in_flight_transactions, 0);
            assert!(
                stats.max_in_flight_declared_uncompressed_bytes
                    <= 2 * config.uncompressed_batch_budget_bytes as u64
            );
        }
    }

    #[test]
    fn ordered_incremental_bounds_live_outputs_until_the_sink_returns() {
        run_full_output_window(false);
    }

    #[test]
    fn ordered_incremental_sink_error_releases_a_full_window_and_the_producer() {
        run_full_output_window(true);
    }

    #[test]
    fn ordered_incremental_oversized_blocks_run_alone_through_consumption() {
        let fixture = Fixture::build();
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let counts = Arc::new(OutputLifetimes::default());
        let mut consumed = Vec::new();
        let rows = &archive.index().rows;
        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..rows.len(),
                OrderedParallelBlockConfig {
                    decode_workers: 2,
                    uncompressed_batch_budget_bytes: 1,
                    compressed_buffer_count: 2,
                    ..OrderedParallelBlockConfig::default()
                },
                |_| Ok(()),
                |_, _, _| -> Result<TrackedOutput> { Ok(TrackedOutput::new(&counts)) },
                |row, _output| {
                    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
                    consumed.push(row);
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(consumed, (0..rows.len()).collect::<Vec<_>>());
        assert_eq!(counts.live.load(Ordering::SeqCst), 0);
        assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
        assert_eq!(stats.max_in_flight_blocks, 1);
        assert_eq!(
            stats.max_in_flight_transactions,
            rows.iter()
                .map(|row| u64::from(row.tx_count))
                .max()
                .unwrap()
        );
        assert_eq!(
            stats.max_in_flight_declared_uncompressed_bytes,
            rows.iter()
                .map(|row| u64::from(row.uncompressed_len))
                .max()
                .unwrap()
        );
    }

    fn transaction_window_fixture(block_count: usize, transactions_per_block: u32) -> Fixture {
        // Reuse the small archive's manifest/registry setup. Only block data,
        // its index, and the footer transaction count need replacement.
        let fixture = Fixture::parallel_blocks(block_count);
        let root = fixture.directory.path();
        let mut blocks = Vec::new();
        let mut rows = Vec::new();
        for block_id in 0..block_count {
            let slot = 101 + block_id as u64;
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: block_id as u32 + 1,
                    previous_blockhash_id: block_id as u32,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: transactions_per_block,
                tx_rows: (0..transactions_per_block)
                    .map(|tx_index| ArchiveV2HotTxRow {
                        tx_index,
                        flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                        message_offset: tx_index,
                        message_len: 1,
                        metadata_offset: 0,
                        metadata_len: 0,
                        signature_count: 0,
                        reserved: [0; 3],
                    })
                    .collect(),
                message_bytes: vec![0; transactions_per_block as usize],
                metadata_bytes: Vec::new(),
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id: block_id as u32,
                slot,
                compressed_offset: blocks.len() as u64,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: transactions_per_block,
                first_tx_ordinal: block_id as u64 * u64::from(transactions_per_block),
                first_signature_ordinal: 0,
                signature_count: 0,
            });
            blocks.extend_from_slice(&compressed);
        }
        fs::write(root.join(BLOCKS_FILE), &blocks).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(BLOCK_INDEX_FILE),
            blocks.len() as u64,
            1,
            0,
            &rows,
        )
        .unwrap();
        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: block_count as u64,
                transactions: block_count as u64 * u64::from(transactions_per_block),
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut metadata = Vec::new();
        for record in records {
            let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_u32_varint(&mut metadata, bytes.len() as u32);
            metadata.extend_from_slice(&bytes);
        }
        fs::write(root.join(META_FILE), metadata).unwrap();
        write_manifest(root, true, None);
        fixture
    }

    fn assert_resource_limited_window(
        archive: &ArchiveReader<LocalRangeSource>,
        config: OrderedParallelBlockConfig,
        expected_live: usize,
    ) -> OrderedParallelBlockStats {
        let counts = Arc::new(OutputLifetimes::default());
        let (window_full, full_receiver) = sync_channel(1);
        let full_receiver = Mutex::new(full_receiver);
        let mut consumed = Vec::new();
        let stats = archive
            .process_borrowed_blocks_parallel_ordered(
                0..archive.index().rows.len(),
                config,
                |_| Ok(()),
                |_, _, _| -> Result<TrackedOutput> {
                    let output = TrackedOutput::new(&counts);
                    let live = counts.live.load(Ordering::SeqCst);
                    assert!(
                        live <= expected_live,
                        "resource window admitted {live} outputs"
                    );
                    if live == expected_live {
                        let _ = window_full.try_send(());
                    }
                    Ok(output)
                },
                |row, _output| {
                    consumed.push(row);
                    if row == 0 {
                        receive_signal(&full_receiver, "the resource-limited output window")?;
                        assert_eq!(counts.live.load(Ordering::SeqCst), expected_live);
                    }
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(
            consumed,
            (0..archive.index().rows.len()).collect::<Vec<_>>()
        );
        assert_eq!(counts.live.load(Ordering::SeqCst), 0);
        assert_eq!(counts.peak.load(Ordering::SeqCst), expected_live);
        assert_eq!(stats.max_in_flight_blocks, expected_live);
        stats
    }

    #[test]
    fn ordered_incremental_declared_bytes_limit_the_window_below_the_block_cap() {
        let fixture = Fixture::parallel_blocks(12);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let block_bytes = archive.index().rows[0].uncompressed_len as usize;
        assert!(
            archive
                .index()
                .rows
                .iter()
                .all(|row| row.uncompressed_len as usize == block_bytes)
        );
        let stats = assert_resource_limited_window(
            &archive,
            OrderedParallelBlockConfig {
                decode_workers: 2,
                max_blocks_per_batch: 64,
                uncompressed_batch_budget_bytes: block_bytes * 2,
                compressed_buffer_count: 3,
                ..OrderedParallelBlockConfig::default()
            },
            4,
        );

        assert_eq!(stats.max_in_flight_transactions, 0);
        assert_eq!(
            stats.max_in_flight_declared_uncompressed_bytes,
            block_bytes as u64 * 4
        );
    }

    #[test]
    fn ordered_incremental_transaction_limit_stops_a_fourth_forty_thousand_tx_block() {
        let fixture = transaction_window_fixture(6, 40_000);
        let archive = ArchiveReader::open(fixture.source()).unwrap();
        let block_bytes = u64::from(archive.index().rows[0].uncompressed_len);
        assert!(
            archive
                .index()
                .rows
                .iter()
                .all(|row| u64::from(row.uncompressed_len) == block_bytes)
        );
        let stats = assert_resource_limited_window(
            &archive,
            OrderedParallelBlockConfig {
                decode_workers: 2,
                max_blocks_per_batch: 64,
                compressed_buffer_count: 3,
                ..OrderedParallelBlockConfig::default()
            },
            3,
        );

        assert_eq!(stats.max_in_flight_transactions, 120_000);
        assert!(stats.max_in_flight_transactions <= 131_072);
        assert_eq!(
            stats.max_in_flight_declared_uncompressed_bytes,
            block_bytes * 3
        );
    }

    fn with_completion_watchdog(run: impl FnOnce() + Send + 'static) {
        let (finished, completion) = sync_channel(1);
        let caller = std::thread::spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
            let _ = finished.send(outcome);
        });
        let outcome = completion
            .recv_timeout(DEADLOCK_GUARD)
            .expect("the pipeline did not finish panic cancellation before the watchdog expired");
        caller.join().unwrap();
        if let Err(payload) = outcome {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    fn ordered_incremental_worker_panic_releases_the_missing_first_result() {
        with_completion_watchdog(|| {
            let fixture = Fixture::parallel_blocks(64);
            let archive = ArchiveReader::open(fixture.source()).unwrap();
            let counts = Arc::new(OutputLifetimes::default());
            let mut consumed = Vec::new();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                archive.process_borrowed_blocks_parallel_ordered(
                    0..64,
                    OrderedParallelBlockConfig {
                        decode_workers: 2,
                        max_blocks_per_batch: 32,
                        compressed_buffer_count: 1,
                        ..OrderedParallelBlockConfig::default()
                    },
                    |_| Ok(()),
                    |_, row, _| -> Result<TrackedOutput> {
                        if row == 0 {
                            panic!("injected ordered worker panic");
                        }
                        Ok(TrackedOutput::new(&counts))
                    },
                    |row, _output| {
                        consumed.push(row);
                        Ok(())
                    },
                )
            }));

            let error = outcome
                .expect("worker panic must be returned as a reader error")
                .unwrap_err();
            assert!(matches!(error, Error::InvalidIndex(_)));
            assert!(error.to_string().contains("panic"));
            assert!(consumed.is_empty());
            assert_eq!(counts.live.load(Ordering::SeqCst), 0);
            assert_eq!(
                counts.created.load(Ordering::SeqCst),
                counts.dropped.load(Ordering::SeqCst)
            );
        });
    }

    #[test]
    fn ordered_incremental_sink_panic_releases_a_full_window() {
        with_completion_watchdog(|| {
            const WINDOW: usize = 16;
            let fixture = Fixture::parallel_blocks(64);
            let archive = ArchiveReader::open(fixture.source()).unwrap();
            let counts = Arc::new(OutputLifetimes::default());
            let (window_full, full_receiver) = sync_channel(1);
            let full_receiver = Mutex::new(full_receiver);
            let mut consumed = Vec::new();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                archive.process_borrowed_blocks_parallel_ordered(
                    0..64,
                    OrderedParallelBlockConfig {
                        decode_workers: 2,
                        max_blocks_per_batch: 32,
                        compressed_buffer_count: 1,
                        ..OrderedParallelBlockConfig::default()
                    },
                    |_| Ok(()),
                    |_, _, _| -> Result<TrackedOutput> {
                        let output = TrackedOutput::new(&counts);
                        if counts.live.load(Ordering::SeqCst) == WINDOW {
                            let _ = window_full.try_send(());
                        }
                        Ok(output)
                    },
                    |row, _output| {
                        consumed.push(row);
                        receive_signal(&full_receiver, "a full output window before sink panic")?;
                        panic!("injected ordered sink panic");
                    },
                )
            }));

            assert!(outcome.is_err(), "sink panic must propagate after cleanup");
            assert_eq!(consumed, vec![0]);
            assert_eq!(counts.live.load(Ordering::SeqCst), 0);
            assert_eq!(counts.peak.load(Ordering::SeqCst), WINDOW);
            assert_eq!(
                counts.created.load(Ordering::SeqCst),
                counts.dropped.load(Ordering::SeqCst)
            );
        });
    }

    struct PanickingBlocksSource(LocalRangeSource);

    impl RangeSource for PanickingBlocksSource {
        fn size(&self, object: &str) -> SourceResult<Option<u64>> {
            self.0.size(object)
        }

        fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
            if object == BLOCKS_FILE {
                panic!("injected ordered producer panic");
            }
            self.0.read_range(object, offset, length)
        }
    }

    #[test]
    fn ordered_incremental_source_panic_releases_the_waiting_dispatcher() {
        with_completion_watchdog(|| {
            let fixture = Fixture::parallel_blocks(4);
            let archive = ArchiveReader::open_with_options(
                PanickingBlocksSource(fixture.source()),
                OpenOptions {
                    hash_verification: HashVerification::SizesOnly,
                    ..OpenOptions::default()
                },
            )
            .unwrap();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                archive.process_borrowed_blocks_parallel_ordered(
                    0..4,
                    OrderedParallelBlockConfig {
                        decode_workers: 2,
                        compressed_buffer_count: 1,
                        ..OrderedParallelBlockConfig::default()
                    },
                    |_| Ok(()),
                    |_, _, _| -> Result<()> { panic!("failed source must not start projection") },
                    |_, ()| -> Result<()> { panic!("failed source must not publish output") },
                )
            }));

            let error = outcome
                .expect("producer panic must be returned as a reader error")
                .unwrap_err();
            assert!(matches!(error, Error::InvalidIndex(_)));
            assert!(error.to_string().contains("producer"));
            assert!(error.to_string().contains("panic"));
        });
    }
}
