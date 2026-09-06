//! Flat token rows retain compact references until the ordered consumer needs keys.

use std::num::NonZeroU32;

use blockzilla_model::{AccountReference, AccountResolver, IndexedTokenBalance, IndexedTokenSink};

use super::*;

pub(super) struct IndexedTokenOutput<'a> {
    pub rows: &'a mut Vec<IndexedTokenBalance>,
    pub accounts: &'a mut crate::CompactV2AccountReferences,
}

impl IndexedTokenOutput<'_> {
    /// The caller has validated the complete message, metadata, and row flags.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn append_validated<S: RangeSource>(
        self,
        reader: &ArchiveReader<S>,
        query_keys: &BoundQueryKeys,
        requirement: &TokenBalanceRequirement,
        tx_index: u32,
        message_bytes: &[u8],
        metadata_bytes: &[u8],
        limits: CompactV2MetadataProjectionLimits,
        balances: &crate::ProjectedCompactV2TokenBalances,
    ) -> CompactV2InstructionSourceResult<()> {
        let mut accounts_projected = false;
        for (side, values) in [
            (TokenBalanceSide::Pre, balances.pre.as_slice()),
            (TokenBalanceSide::Post, balances.post.as_slice()),
        ] {
            for (balance_index, balance) in values.iter().enumerate() {
                let selected = match (balance.mint, requirement) {
                    (_, TokenBalanceRequirement::None) => false,
                    (Some(reference), TokenBalanceRequirement::Mints(keys)) => {
                        keys.iter().any(|key| query_keys.matches(reference, key))
                    }
                    // Missing mint evidence must remain visible to the consumer.
                    (None, TokenBalanceRequirement::Mints(_))
                    | (_, TokenBalanceRequirement::All) => true,
                };
                if !selected {
                    continue;
                }
                if !accounts_projected {
                    self.accounts
                        .project_validated(
                            message_bytes,
                            metadata_bytes,
                            reader.message_schema(),
                            reader.metadata_schema(),
                            reader.registry_entries(),
                            limits,
                        )
                        .map_err(|error| {
                            CompactV2InstructionSourceError::Invalid(error.to_string())
                        })?;
                    accounts_projected = true;
                }
                let token_account = self
                    .accounts
                    .get(balance.account_index)
                    .copied()
                    .ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "token account index is outside validated message references".into(),
                        )
                    })?;
                // Each validated source list has at most 256 rows. Grow the
                // shared block buffer fallibly, without one vector per transaction.
                self.rows.try_reserve(1).map_err(|_| {
                    CompactV2InstructionSourceError::Invalid(
                        "cannot reserve indexed token rows".into(),
                    )
                })?;
                self.rows.push(IndexedTokenBalance {
                    tx_index,
                    side,
                    balance_index: u32::try_from(balance_index).map_err(|_| {
                        CompactV2InstructionSourceError::Invalid(
                            "token-balance index exceeds u32".into(),
                        )
                    })?,
                    account_index: balance.account_index,
                    token_account: account_reference(token_account)?,
                    mint: balance.mint.map(account_reference).transpose()?,
                    owner: balance.owner.map(account_reference).transpose()?,
                    token_program: balance.program_id.map(account_reference).transpose()?,
                    amount: balance.amount,
                    decimals: balance.decimals,
                });
            }
        }
        Ok(())
    }
}

fn account_reference(
    reference: CompactPubkey,
) -> CompactV2InstructionSourceResult<AccountReference> {
    match reference {
        CompactPubkey::Raw(key) => Ok(AccountReference::Inline(key)),
        CompactPubkey::Id(id) => NonZeroU32::new(id)
            .map(AccountReference::Registry)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "registry ID zero is not an account reference".into(),
                )
            }),
    }
}

struct ConsumerResolver<'a, S> {
    reader: &'a ArchiveReader<S>,
    context: ExactContext,
}

impl<S: RangeSource> AccountResolver for ConsumerResolver<'_, S> {
    fn resolve(&mut self, reference: AccountReference) -> blockzilla_model::Result<[u8; 32]> {
        let reference = match reference {
            AccountReference::Registry(id) => CompactPubkey::Id(id.get()),
            AccountReference::Inline(key) => CompactPubkey::Raw(key),
        };
        self.context
            .resolve_pubkey(self.reader, reference)
            .map_err(source_error)
    }
}

#[derive(Default)]
struct IndexedBlockBuffers {
    transactions: Vec<CanonicalTransaction>,
    rows: Vec<IndexedTokenBalance>,
}

struct IndexedProjectionWorker {
    recycle: Arc<Mutex<Vec<IndexedBlockBuffers>>>,
    buffers: Vec<IndexedBlockBuffers>,
    context: ExactContext,
    scratch: TransactionProjectionScratch,
    accounts: crate::CompactV2AccountReferences,
}

struct IndexedProjectedBlock {
    recycle: Arc<Mutex<Vec<IndexedBlockBuffers>>>,
    canonical: CanonicalBlock,
    rows: Vec<IndexedTokenBalance>,
    owned_payload_bytes: u64,
}

impl<S: RangeSource> CompactV2InstructionSource<S> {
    /// Scan token rows in source order without resolving their registry IDs.
    ///
    /// Select token balances, execution status, and no instructions, signatures,
    /// or required signers. The sink gets canonical headers and a flat borrowed
    /// token slice. It can resolve new dictionary entries through one bounded
    /// consumer cache. The configured full-registry policy applies to that one
    /// consumer cache; projection workers never resolve account keys.
    pub fn scan_token_balances_indexed_parallel(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn IndexedTokenSink,
        config: CompactV2ParallelScanConfig,
    ) -> blockzilla_model::Result<CompactV2ParallelScanReceipt> {
        if request.counts_only
            || request.include_instructions
            || request.include_required_signers
            || request.required_signer.is_some()
            || request.include_primary_signatures
            || !request.include_execution_status
            || !matches!(request.instruction_data, InstructionDataRequirement::None)
            || !request.token_balances.is_requested()
        {
            return Err(QueryError::InvalidRequest(
                "indexed token scans require token balances and execution status, with no instructions, signatures, or required signers".into(),
            ));
        }
        let parallel = compact_v2_parallel_reader_config(config).map_err(source_error)?;
        let identity = self.identity.clone();
        let mut validation_sink =
            blockzilla_model::FnBlockSink::new(|_: blockzilla_model::BlockView<'_>| Ok(()));
        let mut publisher = OrderedBlockPublisher::new(&identity, request, &mut validation_sink)?;
        let start = request
            .range
            .map_or(0usize, |range| range.first_block as usize);
        let end = request
            .range
            .map_or(identity.block_count as usize, |range| {
                range
                    .first_block
                    .checked_add(range.block_count.get())
                    .expect("OrderedBlockPublisher validated the requested u32 range")
                    as usize
            });
        let decoded_bytes =
            self.reader.index().rows[start..end]
                .iter()
                .try_fold(0u64, |total, row| {
                    total
                        .checked_add(u64::from(row.uncompressed_len))
                        .ok_or_else(|| {
                            source_error(CompactV2InstructionSourceError::Invalid(
                                "indexed decoded-byte count overflow".into(),
                            ))
                        })
                })?;
        let query_keys = Arc::new(
            BoundQueryKeys::bind(
                self.reader.source(),
                self.reader.registry_entries(),
                request,
            )
            .map_err(|error| {
                source_error(CompactV2InstructionSourceError::Invalid(error.to_string()))
            })?,
        );
        let requested_transactions =
            requested_transaction_count(&self.reader, start..end).map_err(source_error)?;
        let (shared_registry, mut registry_receipt) = prepare_parallel_registry(
            &self.reader,
            start,
            end,
            request,
            requested_transactions,
            config,
        )
        .map_err(source_error)?;
        if matches!(
            registry_receipt.mode,
            CompactV2ParallelRegistryMode::SparseWorkerCache
        ) {
            registry_receipt.mode = CompactV2ParallelRegistryMode::SparseConsumerCache;
            registry_receipt.resident_bound_bytes =
                COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES as u64;
        }
        let reader = &self.reader;
        let mut resolver = ConsumerResolver {
            reader,
            context: ExactContext::with_shared_registry(shared_registry),
        };
        let projected_bytes_current = AtomicU64::new(0);
        let max_projected_block_bytes = AtomicU64::new(0);
        let max_projected_batch_bytes = AtomicU64::new(0);
        let mut publish_wall_time = Duration::ZERO;

        let pipeline = reader
            .process_borrowed_blocks_parallel_ordered(
                start..end,
                parallel,
                |_| {
                    Ok::<_, CompactV2ParallelScanError>(IndexedProjectionWorker {
                        recycle: Arc::new(Mutex::new(Vec::new())),
                        buffers: Vec::new(),
                        context: ExactContext {
                            query_keys: Arc::clone(&query_keys),
                            ..ExactContext::default()
                        },
                        scratch: TransactionProjectionScratch::default(),
                        accounts: crate::CompactV2AccountReferences::default(),
                    })
                },
                |worker, _row_number, block| {
                    for mut buffer in worker
                        .recycle
                        .lock()
                        .map_err(|_| {
                            CompactV2InstructionSourceError::Invalid(
                                "indexed recycle queue poisoned".into(),
                            )
                        })?
                        .drain(..)
                    {
                        buffer.transactions.clear();
                        buffer.rows.clear();
                        let mut bytes =
                            capacity_bytes::<CanonicalTransaction>(buffer.transactions.capacity())?;
                        checked_add_payload(
                            &mut bytes,
                            capacity_bytes::<IndexedTokenBalance>(buffer.rows.capacity())?,
                        )?;
                        if worker.buffers.len() < 8 && bytes <= 8 << 20 {
                            worker.buffers.push(buffer);
                        }
                    }
                    let mut buffer = worker.buffers.pop().unwrap_or_default();
                    buffer
                        .transactions
                        .try_reserve(block.tx_rows_len())
                        .map_err(|_| {
                            CompactV2InstructionSourceError::Invalid(
                                "cannot reserve indexed transaction headers".into(),
                            )
                        })?;
                    let source_row = block.index_row;
                    for (index, row) in block.tx_rows().enumerate() {
                        if row.tx_index as usize != index {
                            return Err(CompactV2InstructionSourceError::Invalid(
                                "transaction order differs from block".into(),
                            )
                            .into());
                        }
                        let transaction = Self::project_transaction_inner(
                            reader,
                            &mut worker.context,
                            &mut worker.scratch,
                            request,
                            source_row.slot,
                            row,
                            block.message_bytes(),
                            block.metadata_bytes(),
                            None,
                            Some(IndexedTokenOutput {
                                rows: &mut buffer.rows,
                                accounts: &mut worker.accounts,
                            }),
                        );
                        worker.scratch.finish_transaction();
                        buffer.transactions.push(transaction?);
                    }
                    let mut owned_payload_bytes =
                        capacity_bytes::<CanonicalTransaction>(buffer.transactions.capacity())?;
                    checked_add_payload(
                        &mut owned_payload_bytes,
                        capacity_bytes::<IndexedTokenBalance>(buffer.rows.capacity())?,
                    )?;
                    max_projected_block_bytes.fetch_max(owned_payload_bytes, Ordering::Relaxed);
                    let current = atomic_checked_add(
                        &projected_bytes_current,
                        owned_payload_bytes,
                        "indexed projected output bytes",
                    )?;
                    max_projected_batch_bytes.fetch_max(current, Ordering::Relaxed);
                    Ok(IndexedProjectedBlock {
                        recycle: Arc::clone(&worker.recycle),
                        canonical: CanonicalBlock {
                            counts: None,
                            header: BlockHeader {
                                epoch: identity.epoch,
                                block_ordinal: source_row.block_id,
                                slot: source_row.slot,
                            },
                            transactions: buffer.transactions,
                        },
                        rows: buffer.rows,
                        owned_payload_bytes,
                    })
                },
                |_row_number, projected| {
                    let started = Instant::now();
                    publisher.publish(&projected.canonical)?;
                    sink.visit_indexed_block(
                        projected.canonical.as_view(),
                        &projected.rows,
                        &mut resolver,
                    )?;
                    publish_wall_time += started.elapsed();
                    let previous = projected_bytes_current
                        .fetch_sub(projected.owned_payload_bytes, Ordering::AcqRel);
                    if previous < projected.owned_payload_bytes {
                        return Err(CompactV2InstructionSourceError::Invalid(
                            "indexed projected byte accounting underflow".into(),
                        )
                        .into());
                    }
                    projected
                        .recycle
                        .lock()
                        .map_err(|_| {
                            CompactV2InstructionSourceError::Invalid(
                                "indexed recycle queue poisoned".into(),
                            )
                        })?
                        .push(IndexedBlockBuffers {
                            transactions: projected.canonical.transactions,
                            rows: projected.rows,
                        });
                    Ok(())
                },
            )
            .map_err(CompactV2ParallelScanError::into_query_error)?;

        let mut context_io = resolver.context.io;
        context_io
            .checked_add(ContextIo {
                calls: query_keys.read_calls,
                bytes: query_keys.read_bytes,
            })
            .map_err(source_error)?;
        context_io
            .checked_add(ContextIo {
                calls: registry_receipt.prefetch_read_calls,
                bytes: registry_receipt.prefetch_read_bytes,
            })
            .map_err(source_error)?;
        let source_read_calls = pipeline
            .read_call_count
            .checked_add(context_io.calls)
            .ok_or_else(|| {
                source_error(CompactV2InstructionSourceError::Invalid(
                    "indexed source-read count overflow".into(),
                ))
            })?;
        let source_read_bytes = pipeline
            .compressed_bytes
            .checked_add(context_io.bytes)
            .ok_or_else(|| {
                source_error(CompactV2InstructionSourceError::Invalid(
                    "indexed source-read byte count overflow".into(),
                ))
            })?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(source_read_calls),
            source_read_bytes: Some(source_read_bytes),
            decoded_bytes: Some(decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        Ok(CompactV2ParallelScanReceipt {
            scan: publisher.finish()?,
            pipeline,
            requested_workers: config.workers,
            effective_workers: pipeline.effective_workers,
            max_active_workers: pipeline.max_active_workers,
            compressed_buffer_count: parallel.compressed_buffer_count,
            max_projected_block_bytes: max_projected_block_bytes.load(Ordering::Relaxed),
            max_projected_batch_bytes: max_projected_batch_bytes.load(Ordering::Relaxed),
            registry: registry_receipt,
            signature_read_wall_time: Duration::ZERO,
            signature_assign_wall_time: Duration::ZERO,
            publish_wall_time,
        })
    }
}
