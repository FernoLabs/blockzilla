//! Fixed input slots are reused only after their last borrowed consumer exits.
use super::*;

pub(super) fn validate(
    config: Option<IndexerV3NetworkInputConfig>,
) -> blockzilla_model::Result<()> {
    if let Some(c) = config
        && (!(1..=16).contains(&c.workers)
            || c.range_bytes == 0
            || c.range_bytes > 32 << 20
            || c.range_bytes > c.max_buffer_bytes
            || c.max_buffer_bytes > 1024 << 20)
    {
        return Err(QueryError::InvalidRequest("V3 input requires 1–16 workers, a range target up to 32 MiB, and a fitting byte budget up to 1 GiB".into()));
    }
    Ok(())
}

struct Group {
    jobs: VecDeque<ParallelScanJob>,
    range: Option<Range<usize>>,
}

impl ParallelInputJobs {
    pub(super) fn concurrent(
        mut plan: ParallelScanJobPlan,
        config: IndexerV3NetworkInputConfig,
        signature_source: Option<Arc<dyn RangeSource>>,
    ) -> blockzilla_model::Result<Self> {
        validate(Some(config))?;
        let (reader, selection) = match &plan {
            ParallelScanJobPlan::Ordered {
                reader, selection, ..
            }
            | ParallelScanJobPlan::Selected {
                reader, selection, ..
            } => (Arc::clone(reader), *selection),
        };
        let input_error = |e| source_error(IndexerV3InstructionSourceError::Reader(e));
        let bytes_for = |range: Range<usize>| -> blockzilla_model::Result<usize> {
            let semantic = reader
                .semantic_input_bytes(range.clone(), selection)
                .map_err(input_error)?;
            semantic
                .checked_add(
                    network_signature_bytes(&reader, range, signature_source.is_some())
                        .map_err(input_error)?,
                )
                .ok_or_else(|| QueryError::InvalidStream("V3 input size overflow".into()))
        };
        let mut groups = Vec::new();
        let mut next = plan.next_job()?;
        let mut capacities = [0usize; Object::ALL.len()];
        let mut signature_capacity = 0;
        while let Some(first) = next.take() {
            let start = first.blocks.get(0).expect("nonempty job");
            let mut end = start + first.blocks.len();
            let contiguous = first.blocks.iter().eq(start..end);
            let mut bytes = if contiguous {
                bytes_for(start..end)?
            } else {
                usize::MAX
            };
            let mut jobs = VecDeque::from([first]);
            next = plan.next_job()?;
            while contiguous && bytes <= config.range_bytes && end - start < 8192 {
                let Some(candidate) = next.as_ref() else {
                    break;
                };
                let candidate_end = end + candidate.blocks.len();
                if candidate_end - start > 8192 || !candidate.blocks.iter().eq(end..candidate_end) {
                    break;
                }
                let more = bytes_for(end..candidate_end)?;
                if more > config.range_bytes - bytes {
                    break;
                }
                bytes += more;
                end = candidate_end;
                jobs.push_back(next.take().unwrap());
                next = plan.next_job()?;
            }
            let range = (contiguous && bytes <= config.range_bytes).then_some(start..end);
            if let Some(range) = &range {
                let needed = reader
                    .semantic_input_capacities(range.clone(), selection)
                    .map_err(input_error)?;
                for (capacity, needed) in capacities.iter_mut().zip(needed) {
                    *capacity = (*capacity).max(needed);
                }
                signature_capacity = signature_capacity.max(
                    network_signature_bytes(&reader, range.clone(), signature_source.is_some())
                        .map_err(input_error)?
                        / SIGNATURE_BYTES,
                );
            }
            groups.push(Group { jobs, range });
        }
        // Per-plane maxima allow every worker to reuse its vectors even when
        // plane proportions change. Small allocator minima are charged too.
        let slot_bytes = capacities
            .iter()
            .map(|&n| if n == 0 { 0 } else { n.max(8) })
            .sum::<usize>()
            + signature_capacity * SIGNATURE_BYTES
            + SIGNATURE_BYTES;
        if slot_bytes > config.max_buffer_bytes {
            return Err(QueryError::InvalidRequest("V3 per-plane input capacities exceed the configured budget; reduce the range target".into()));
        }
        let workers = config
            .workers
            .min(config.max_buffer_bytes / slot_bytes)
            .min(groups.len().max(1));
        let capacity_bytes = workers * slot_bytes;
        let group_count = groups.len();
        let mut assignments: Vec<Vec<Group>> = (0..workers).map(|_| Vec::new()).collect();
        for (i, group) in groups.into_iter().enumerate() {
            assignments[i % workers].push(group);
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&cancelled);
        let (sender, receiver) = mpsc::sync_channel(1);
        let producer = thread::Builder::new()
            .name("blockzilla-v3-input".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    thread::scope(|scope| -> blockzilla_model::Result<()> {
                        let budget = blockzilla_source::input_budget::InputBudget::new(
                            capacity_bytes,
                            Arc::clone(&stop),
                        );
                        let mut receivers = Vec::new();
                        for assigned in assignments {
                            let (tx, rx) = mpsc::sync_channel(1);
                            receivers.push(rx);
                            let reader = Arc::clone(&reader);
                            let signatures = signature_source.clone();
                            let stop = Arc::clone(&stop);
                            let budget = Arc::clone(&budget);
                            scope.spawn(move || {
                                let result = catch_unwind(AssertUnwindSafe(
                                    || -> blockzilla_model::Result<()> {
                                        let Some(permit) = budget.acquire(slot_bytes) else {
                                            return Ok(());
                                        };
                                        let semantic = reader
                                            .allocate_semantic_input(capacities, permit)
                                            .map_err(input_error)?;
                                        let mut records = Vec::new();
                                        reserve_exact(
                                            &mut records,
                                            signature_capacity,
                                            "V3 recycled signature input",
                                        )
                                        .map_err(source_error)?;
                                        let mut retained = Arc::new(PrefetchedScanInput {
                                            semantic,
                                            signatures: signatures.as_ref().map(|_| {
                                                SignatureBatch {
                                                    block_range: 0..0,
                                                    first_signature_ordinal: 0,
                                                    signatures: records,
                                                }
                                            }),
                                        });
                                        for mut group in assigned {
                                            if stop.load(Ordering::Acquire) {
                                                return Ok(());
                                            }
                                            if let Some(range) = group.range {
                                                // The loader retains one reference. Both the input
                                                // jobs and independent semantic borrows must finish.
                                                while Arc::strong_count(&retained) != 1
                                                    || Arc::strong_count(&retained.semantic) != 1
                                                {
                                                    if stop.load(Ordering::Acquire) {
                                                        return Ok(());
                                                    }
                                                    thread::park_timeout(
                                                        std::time::Duration::from_millis(1),
                                                    );
                                                }
                                                let previous = Arc::try_unwrap(retained)
                                                    .expect("unshared input slot");
                                                let semantic = reader
                                                    .prefetch_semantic_input_reusing(
                                                        range.clone(),
                                                        selection,
                                                        previous.semantic,
                                                    )
                                                    .map_err(input_error)?;
                                                let signatures = load_network_signatures_reusing(
                                                    &reader,
                                                    signatures.as_ref(),
                                                    range,
                                                    previous.signatures,
                                                )
                                                .map_err(source_error)?;
                                                retained = Arc::new(PrefetchedScanInput {
                                                    semantic,
                                                    signatures,
                                                });
                                                for job in &mut group.jobs {
                                                    job.prefetched = Some(Arc::clone(&retained));
                                                }
                                            }
                                            // Sparse or oversized jobs use their existing decoder
                                            // path. They do not read gaps or enlarge input slots.
                                            if tx.send(Ok(group.jobs)).is_err() {
                                                return Ok(());
                                            }
                                        }
                                        Ok(())
                                    },
                                ))
                                .unwrap_or_else(|_| {
                                    Err(QueryError::InvalidStream(
                                        "V3 input worker panicked".into(),
                                    ))
                                });
                                if let Err(error) = result {
                                    let _ = tx.send(Err(error));
                                }
                            });
                        }
                        let result = (|| {
                            for i in 0..group_count {
                                let group = receivers[i % workers].recv().map_err(|_| {
                                    QueryError::InvalidStream(
                                        "V3 input worker stopped before its group".into(),
                                    )
                                })??;
                                if sender.send(Ok(group)).is_err() {
                                    return Ok(());
                                }
                            }
                            Ok(())
                        })();
                        // Disconnect every worker before the scope joins. This also
                        // wakes workers waiting to send after a sink or input failure.
                        stop.store(true, Ordering::Release);
                        drop(receivers);
                        result
                    })
                }))
                .unwrap_or_else(|_| {
                    Err(QueryError::InvalidStream(
                        "V3 concurrent input panicked".into(),
                    ))
                });
                if let Err(error) = result {
                    let _ = sender.send(Err(error));
                }
            })
            .map_err(|e| {
                QueryError::InvalidStream(format!("cannot create V3 input producer: {e}"))
            })?;
        Ok(Self {
            input_workers: workers,
            input_buffer_capacity_bytes: capacity_bytes,
            local: None,
            receiver: Some(receiver),
            pending: VecDeque::new(),
            producer: Some(producer),
            cancelled,
        })
    }
}
