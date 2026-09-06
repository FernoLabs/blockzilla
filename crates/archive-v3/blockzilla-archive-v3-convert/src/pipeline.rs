//! Bounded, deterministic parallel processing with ordered commits.
//!
//! Workers can complete tasks in any order. The commit callback always receives
//! results in increasing, contiguous sequence order. `reserved_bytes` is a
//! caller-supplied upper bound for the memory retained by one task and its
//! result. The pipeline keeps the sum of these reservations within
//! [`PipelineConfig::max_in_flight_bytes`].

use std::{
    any::Any,
    collections::BTreeMap,
    error::Error,
    fmt,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread,
};

/// A sequence-keyed unit of work.
#[derive(Debug)]
pub struct OrderedTask<T> {
    /// A contiguous sequence number. The first value is set in the config.
    pub sequence: u64,
    /// The maximum bytes retained by this task and its result until commit.
    pub reserved_bytes: usize,
    /// The value passed to one worker.
    pub payload: T,
}

impl<T> OrderedTask<T> {
    pub const fn new(sequence: u64, reserved_bytes: usize, payload: T) -> Self {
        Self {
            sequence,
            reserved_bytes,
            payload,
        }
    }
}

/// Limits for an ordered parallel pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineConfig {
    /// Number of worker threads.
    pub worker_count: usize,
    /// Maximum tasks that can be running or waiting for ordered commit.
    pub max_in_flight_tasks: usize,
    /// Maximum sum of caller-declared task reservations.
    pub max_in_flight_bytes: usize,
    /// Required sequence number of the first task.
    pub first_sequence: u64,
}

impl PipelineConfig {
    pub const fn new(
        worker_count: usize,
        max_in_flight_tasks: usize,
        max_in_flight_bytes: usize,
    ) -> Self {
        Self {
            worker_count,
            max_in_flight_tasks,
            max_in_flight_bytes,
            first_sequence: 0,
        }
    }
}

/// High-water marks and completed work counts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelineStats {
    pub submitted_tasks: u64,
    pub committed_tasks: u64,
    pub peak_in_flight_tasks: usize,
    pub peak_reserved_bytes: usize,
}

/// A pipeline, worker, or commit failure.
#[derive(Debug)]
pub enum PipelineError<E> {
    InvalidConfig(&'static str),
    InvalidSequence {
        expected: u64,
        actual: u64,
    },
    SequenceOverflow,
    TaskExceedsByteBudget {
        sequence: u64,
        reserved_bytes: usize,
        max_in_flight_bytes: usize,
    },
    WorkerInitialization {
        worker_index: usize,
        source: E,
    },
    Worker {
        sequence: u64,
        source: E,
    },
    WorkerPanicked {
        sequence: u64,
        message: String,
    },
    Commit {
        sequence: u64,
        source: E,
    },
    /// Internal sentinel returned to the producer after the exact terminal
    /// worker or commit error has been retained by the ordered sink.
    TerminalResult,
    ChannelClosed,
    DuplicateResult {
        sequence: u64,
    },
}

/// A producer/read failure or a failure in the offset-independent encoding
/// stage driven from an ordered reader callback.
#[derive(Debug)]
pub enum OrderedEncodingStageError<E> {
    Producer(E),
    Pipeline(PipelineError<E>),
}

impl<E: fmt::Display> fmt::Display for OrderedEncodingStageError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Producer(source) => write!(formatter, "ordered producer failed: {source:#}"),
            Self::Pipeline(source) => write!(formatter, "ordered encoding stage: {source}"),
        }
    }
}

impl<E: Error + 'static> Error for OrderedEncodingStageError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Producer(source) => Some(source),
            Self::Pipeline(source) => Some(source),
        }
    }
}

impl<E: fmt::Display> fmt::Display for PipelineError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid pipeline config: {message}"),
            Self::InvalidSequence { expected, actual } => write!(
                formatter,
                "task sequence is not contiguous: expected {expected}, got {actual}"
            ),
            Self::SequenceOverflow => write!(formatter, "task sequence exceeds u64::MAX"),
            Self::TaskExceedsByteBudget {
                sequence,
                reserved_bytes,
                max_in_flight_bytes,
            } => write!(
                formatter,
                "task {sequence} reserves {reserved_bytes} bytes, above the {max_in_flight_bytes}-byte budget"
            ),
            Self::WorkerInitialization {
                worker_index,
                source,
            } => write!(
                formatter,
                "worker {worker_index} initialization failed: {source}"
            ),
            Self::Worker { sequence, source } => {
                write!(formatter, "worker failed for task {sequence}: {source:#}")
            }
            Self::WorkerPanicked { sequence, message } => {
                write!(formatter, "worker panicked for task {sequence}: {message}")
            }
            Self::Commit { sequence, source } => {
                write!(formatter, "commit failed for task {sequence}: {source:#}")
            }
            Self::TerminalResult => write!(formatter, "ordered encoding stage stopped"),
            Self::ChannelClosed => write!(formatter, "pipeline channel closed unexpectedly"),
            Self::DuplicateResult { sequence } => {
                write!(formatter, "worker returned task {sequence} more than once")
            }
        }
    }
}

impl<E: Error + 'static> Error for PipelineError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WorkerInitialization { source, .. }
            | Self::Worker { source, .. }
            | Self::Commit { source, .. } => Some(source),
            _ => None,
        }
    }
}

enum WorkerOutcome<R, E> {
    Completed(Result<R, E>),
    Panicked(String),
}

struct WorkerResult<R, E> {
    sequence: u64,
    reserved_bytes: usize,
    outcome: WorkerOutcome<R, E>,
}

/// Run independent task transforms in parallel and commit their results in order.
///
/// `make_worker` runs once per worker before any task is submitted. This lets a
/// converter give each worker its own file handle, decompressor, and scratch
/// buffers. A worker error or panic stops that worker. The driver still resolves
/// all earlier sequences before it reports the failure, so the committed prefix
/// is deterministic.
pub fn run_ordered_pipeline<I, T, R, E, MakeWorker, Worker, Commit>(
    tasks: I,
    config: PipelineConfig,
    mut make_worker: MakeWorker,
    mut commit: Commit,
) -> Result<PipelineStats, PipelineError<E>>
where
    I: IntoIterator<Item = OrderedTask<T>>,
    T: Send,
    R: Send,
    E: Send,
    MakeWorker: FnMut(usize) -> Result<Worker, E>,
    Worker: FnMut(T) -> Result<R, E> + Send,
    Commit: FnMut(u64, R) -> Result<(), E>,
{
    validate_config::<E>(config)?;

    let mut workers = Vec::with_capacity(config.worker_count);
    for worker_index in 0..config.worker_count {
        workers.push(make_worker(worker_index).map_err(|source| {
            PipelineError::WorkerInitialization {
                worker_index,
                source,
            }
        })?);
    }

    let task_capacity = config.max_in_flight_tasks.min(config.worker_count.max(1));
    let (task_sender, task_receiver) = mpsc::sync_channel(task_capacity);
    let task_receiver = Arc::new(Mutex::new(task_receiver));
    let (result_sender, result_receiver) = mpsc::channel();
    let cancelled = Arc::new(AtomicBool::new(false));

    thread::scope(|scope| {
        for mut worker in workers {
            let task_receiver = Arc::clone(&task_receiver);
            let result_sender = result_sender.clone();
            let cancelled = Arc::clone(&cancelled);
            scope.spawn(move || {
                worker_loop(&mut worker, task_receiver, result_sender, cancelled);
            });
        }
        drop(result_sender);

        let result = drive_pipeline(
            tasks.into_iter(),
            config,
            task_sender,
            &result_receiver,
            &cancelled,
            &mut commit,
        );
        cancelled.store(true, Ordering::Release);
        result
    })
}

/// Run only the offset-independent encode and ordered-write half of the
/// pipeline.
///
/// This is the adapter for a reader that already performs parallel borrowed
/// projection and ordered delivery. `produce` normally calls that reader and,
/// from its ordered consume callback, submits one mapped task to the provided
/// sink. The sink applies its own independent task and byte bounds. It drains
/// completed encodes and invokes `commit` in contiguous sequence order while
/// the producer continues.
pub fn run_ordered_encoding_stage<EncodeTask, Encoded, E, MakeEncoder, Encoder, Commit, Produce>(
    config: PipelineConfig,
    mut make_encoder: MakeEncoder,
    mut commit: Commit,
    produce: Produce,
) -> Result<PipelineStats, OrderedEncodingStageError<E>>
where
    EncodeTask: Send,
    Encoded: Send,
    E: Send,
    MakeEncoder: FnMut(usize) -> Result<Encoder, E>,
    Encoder: FnMut(EncodeTask) -> Result<Encoded, E> + Send,
    Commit: FnMut(u64, Encoded) -> Result<(), E>,
    Produce: FnOnce(&mut OrderedEncodingSink<'_, EncodeTask, Encoded, E, Commit>) -> Result<(), E>,
{
    validate_config(config).map_err(OrderedEncodingStageError::Pipeline)?;
    let mut encoders = Vec::with_capacity(config.worker_count);
    for worker_index in 0..config.worker_count {
        encoders.push(make_encoder(worker_index).map_err(|source| {
            OrderedEncodingStageError::Pipeline(PipelineError::WorkerInitialization {
                worker_index,
                source,
            })
        })?);
    }

    let capacity = config.max_in_flight_tasks.min(config.worker_count.max(1));
    let (task_sender, task_receiver) = mpsc::sync_channel(capacity);
    let task_receiver = Arc::new(Mutex::new(task_receiver));
    let (result_sender, result_receiver) = mpsc::channel();
    let cancelled = Arc::new(AtomicBool::new(false));

    thread::scope(|scope| {
        for mut worker in encoders {
            let task_receiver = Arc::clone(&task_receiver);
            let result_sender = result_sender.clone();
            let cancelled = Arc::clone(&cancelled);
            scope.spawn(move || {
                worker_loop(&mut worker, task_receiver, result_sender, cancelled);
            });
        }
        drop(result_sender);

        let mut sink = OrderedEncodingSink {
            config,
            sender: Some(task_sender),
            results: &result_receiver,
            cancelled: &cancelled,
            commit: &mut commit,
            accounting: StageAccounting::default(),
            reorder: BTreeMap::new(),
            next_submit: Some(config.first_sequence),
            next_commit: Some(config.first_sequence),
            terminal_error: None,
        };
        let result = match produce(&mut sink) {
            Ok(()) => sink.finish().map_err(OrderedEncodingStageError::Pipeline),
            Err(_) if sink.terminal_error.is_some() => {
                let error = sink
                    .terminal_error
                    .take()
                    .expect("terminal encoding error was retained");
                sink.abort();
                Err(OrderedEncodingStageError::Pipeline(error))
            }
            Err(source) if sink.cancelled.load(Ordering::Acquire) => {
                sink.abort();
                Err(OrderedEncodingStageError::Producer(source))
            }
            Err(source) => match sink.finish() {
                // An independent producer error belongs to the next,
                // not-yet-submitted sequence. Drain the submitted prefix first
                // so an earlier worker or commit error cannot be hidden.
                Ok(_) => Err(OrderedEncodingStageError::Producer(source)),
                Err(error) => Err(OrderedEncodingStageError::Pipeline(error)),
            },
        };
        cancelled.store(true, Ordering::Release);
        result
    })
}

/// Inline counterpart to [`run_ordered_encoding_stage`].
///
/// This keeps a global one-worker conversion truly single-threaded: the
/// reader's ordered consume callback encodes and commits each mapped task on
/// that same thread. `config.worker_count` must be zero to make the selected
/// mode explicit; the byte limit still rejects an oversized task.
pub fn run_inline_ordered_encoding_stage<
    EncodeTask,
    Encoded,
    E,
    MakeEncoder,
    Encoder,
    Commit,
    Produce,
>(
    config: PipelineConfig,
    mut make_encoder: MakeEncoder,
    mut commit: Commit,
    produce: Produce,
) -> Result<PipelineStats, OrderedEncodingStageError<E>>
where
    MakeEncoder: FnMut(usize) -> Result<Encoder, E>,
    Encoder: FnMut(EncodeTask) -> Result<Encoded, E>,
    Commit: FnMut(u64, Encoded) -> Result<(), E>,
    Produce: FnOnce(
        &mut InlineOrderedEncodingSink<'_, EncodeTask, Encoded, E, Encoder, Commit>,
    ) -> Result<(), E>,
{
    if config.worker_count != 0 {
        return Err(OrderedEncodingStageError::Pipeline(
            PipelineError::InvalidConfig("inline encoding requires worker_count zero"),
        ));
    }
    if config.max_in_flight_tasks == 0 {
        return Err(OrderedEncodingStageError::Pipeline(
            PipelineError::InvalidConfig("max_in_flight_tasks must be non-zero"),
        ));
    }
    if config.max_in_flight_bytes == 0 {
        return Err(OrderedEncodingStageError::Pipeline(
            PipelineError::InvalidConfig("max_in_flight_bytes must be non-zero"),
        ));
    }
    let mut encoder = make_encoder(0).map_err(|source| {
        OrderedEncodingStageError::Pipeline(PipelineError::WorkerInitialization {
            worker_index: 0,
            source,
        })
    })?;
    let mut sink = InlineOrderedEncodingSink {
        config,
        encoder: &mut encoder,
        commit: &mut commit,
        next_sequence: Some(config.first_sequence),
        stats: PipelineStats::default(),
        _task: std::marker::PhantomData,
        _encoded: std::marker::PhantomData,
        _error: std::marker::PhantomData,
    };
    produce(&mut sink).map_err(OrderedEncodingStageError::Producer)?;
    Ok(sink.stats)
}

/// The no-thread submission sink used by
/// [`run_inline_ordered_encoding_stage`].
pub struct InlineOrderedEncodingSink<'a, EncodeTask, Encoded, E, Encoder, Commit>
where
    Encoder: FnMut(EncodeTask) -> Result<Encoded, E>,
    Commit: FnMut(u64, Encoded) -> Result<(), E>,
{
    config: PipelineConfig,
    encoder: &'a mut Encoder,
    commit: &'a mut Commit,
    next_sequence: Option<u64>,
    stats: PipelineStats,
    _task: std::marker::PhantomData<fn(EncodeTask)>,
    _encoded: std::marker::PhantomData<fn() -> Encoded>,
    _error: std::marker::PhantomData<fn() -> E>,
}

impl<EncodeTask, Encoded, E, Encoder, Commit>
    InlineOrderedEncodingSink<'_, EncodeTask, Encoded, E, Encoder, Commit>
where
    Encoder: FnMut(EncodeTask) -> Result<Encoded, E>,
    Commit: FnMut(u64, Encoded) -> Result<(), E>,
{
    pub fn submit(&mut self, task: OrderedTask<EncodeTask>) -> Result<(), PipelineError<E>> {
        let expected = self.next_sequence.ok_or(PipelineError::SequenceOverflow)?;
        if task.sequence != expected {
            return Err(PipelineError::InvalidSequence {
                expected,
                actual: task.sequence,
            });
        }
        if task.reserved_bytes > self.config.max_in_flight_bytes {
            return Err(PipelineError::TaskExceedsByteBudget {
                sequence: task.sequence,
                reserved_bytes: task.reserved_bytes,
                max_in_flight_bytes: self.config.max_in_flight_bytes,
            });
        }
        self.stats.submitted_tasks += 1;
        self.stats.peak_in_flight_tasks = self.stats.peak_in_flight_tasks.max(1);
        self.stats.peak_reserved_bytes = self.stats.peak_reserved_bytes.max(task.reserved_bytes);
        let sequence = task.sequence;
        let output = (self.encoder)(task.payload)
            .map_err(|source| PipelineError::Worker { sequence, source })?;
        (self.commit)(sequence, output)
            .map_err(|source| PipelineError::Commit { sequence, source })?;
        self.stats.committed_tasks += 1;
        self.next_sequence = sequence.checked_add(1);
        Ok(())
    }
}

/// A bounded, ordered submission sink used by [`run_ordered_encoding_stage`].
pub struct OrderedEncodingSink<'a, EncodeTask, Encoded, E, Commit>
where
    Commit: FnMut(u64, Encoded) -> Result<(), E>,
{
    config: PipelineConfig,
    sender: Option<SyncSender<OrderedTask<EncodeTask>>>,
    results: &'a Receiver<WorkerResult<Encoded, E>>,
    cancelled: &'a AtomicBool,
    commit: &'a mut Commit,
    accounting: StageAccounting,
    reorder: BTreeMap<u64, WorkerResult<Encoded, E>>,
    next_submit: Option<u64>,
    next_commit: Option<u64>,
    terminal_error: Option<PipelineError<E>>,
}

impl<EncodeTask, Encoded, E, Commit> OrderedEncodingSink<'_, EncodeTask, Encoded, E, Commit>
where
    EncodeTask: Send,
    Encoded: Send,
    E: Send,
    Commit: FnMut(u64, Encoded) -> Result<(), E>,
{
    /// Submit one task from an already ordered mapping callback.
    pub fn submit(&mut self, task: OrderedTask<EncodeTask>) -> Result<(), PipelineError<E>> {
        let expected = self.next_submit.ok_or(PipelineError::SequenceOverflow)?;
        if task.sequence != expected {
            self.cancelled.store(true, Ordering::Release);
            return Err(PipelineError::InvalidSequence {
                expected,
                actual: task.sequence,
            });
        }
        if task.reserved_bytes > self.config.max_in_flight_bytes {
            self.cancelled.store(true, Ordering::Release);
            return Err(PipelineError::TaskExceedsByteBudget {
                sequence: task.sequence,
                reserved_bytes: task.reserved_bytes,
                max_in_flight_bytes: self.config.max_in_flight_bytes,
            });
        }

        let mut pending = Some(task);
        loop {
            self.drain_available()?;
            let task = pending.take().expect("submission task remains pending");
            if !self.accounting.can_accept(self.config, task.reserved_bytes) {
                pending = Some(task);
                self.receive_one()?;
                continue;
            }
            let sequence = task.sequence;
            let reserved_bytes = task.reserved_bytes;
            match self
                .sender
                .as_ref()
                .expect("encoding sink remains open during submission")
                .try_send(task)
            {
                Ok(()) => {
                    self.accounting.accepted(reserved_bytes);
                    self.next_submit = sequence.checked_add(1);
                    self.drain_available()?;
                    return Ok(());
                }
                Err(TrySendError::Full(task)) => {
                    pending = Some(task);
                    self.receive_one()?;
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.cancelled.store(true, Ordering::Release);
                    return Err(PipelineError::ChannelClosed);
                }
            }
        }
    }

    fn drain_available(&mut self) -> Result<(), PipelineError<E>> {
        loop {
            match self.results.try_recv() {
                Ok(result) => self.accept_result(result)?,
                Err(mpsc::TryRecvError::Empty) => return Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    if self.accounting.in_flight_tasks == 0 {
                        return Ok(());
                    }
                    self.cancelled.store(true, Ordering::Release);
                    return Err(PipelineError::ChannelClosed);
                }
            }
        }
    }

    fn receive_one(&mut self) -> Result<(), PipelineError<E>> {
        let result = self.results.recv().map_err(|_| {
            self.cancelled.store(true, Ordering::Release);
            PipelineError::ChannelClosed
        })?;
        self.accept_result(result)
    }

    fn accept_result(&mut self, result: WorkerResult<Encoded, E>) -> Result<(), PipelineError<E>> {
        let sequence = result.sequence;
        if self.reorder.insert(sequence, result).is_some() {
            self.cancelled.store(true, Ordering::Release);
            return Err(PipelineError::DuplicateResult { sequence });
        }
        while let Some(sequence) = self.next_commit {
            let Some(result) = self.reorder.remove(&sequence) else {
                break;
            };
            let reserved_bytes = result.reserved_bytes;
            match result.outcome {
                WorkerOutcome::Completed(Ok(output)) => {
                    if let Err(source) = (self.commit)(sequence, output) {
                        self.accounting.discarded(reserved_bytes);
                        self.cancelled.store(true, Ordering::Release);
                        self.terminal_error = Some(PipelineError::Commit { sequence, source });
                        return Err(PipelineError::TerminalResult);
                    }
                    self.accounting.completed(reserved_bytes);
                    self.next_commit = sequence.checked_add(1);
                }
                WorkerOutcome::Completed(Err(source)) => {
                    self.accounting.discarded(reserved_bytes);
                    self.cancelled.store(true, Ordering::Release);
                    self.terminal_error = Some(PipelineError::Worker { sequence, source });
                    return Err(PipelineError::TerminalResult);
                }
                WorkerOutcome::Panicked(message) => {
                    self.accounting.discarded(reserved_bytes);
                    self.cancelled.store(true, Ordering::Release);
                    self.terminal_error = Some(PipelineError::WorkerPanicked { sequence, message });
                    return Err(PipelineError::TerminalResult);
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<PipelineStats, PipelineError<E>> {
        self.sender.take();
        while self.accounting.in_flight_tasks != 0 {
            if let Err(error) = self.receive_one() {
                if matches!(error, PipelineError::TerminalResult) {
                    return Err(self
                        .terminal_error
                        .take()
                        .expect("terminal encoding error was retained"));
                }
                return Err(error);
            }
        }
        if let Some(error) = self.terminal_error.take() {
            return Err(error);
        }
        Ok(self.accounting.stats)
    }

    fn abort(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.sender.take();
    }
}

#[derive(Debug, Default)]
struct StageAccounting {
    stats: PipelineStats,
    in_flight_tasks: usize,
    reserved_bytes: usize,
}

impl StageAccounting {
    fn can_accept(&self, config: PipelineConfig, reserved_bytes: usize) -> bool {
        self.in_flight_tasks < config.max_in_flight_tasks
            && self
                .reserved_bytes
                .checked_add(reserved_bytes)
                .is_some_and(|bytes| bytes <= config.max_in_flight_bytes)
    }

    fn accepted(&mut self, reserved_bytes: usize) {
        self.in_flight_tasks += 1;
        self.reserved_bytes += reserved_bytes;
        self.stats.submitted_tasks += 1;
        self.stats.peak_in_flight_tasks = self.stats.peak_in_flight_tasks.max(self.in_flight_tasks);
        self.stats.peak_reserved_bytes = self.stats.peak_reserved_bytes.max(self.reserved_bytes);
    }

    fn completed(&mut self, reserved_bytes: usize) {
        self.discarded(reserved_bytes);
        self.stats.committed_tasks += 1;
    }

    fn discarded(&mut self, reserved_bytes: usize) {
        self.in_flight_tasks = self
            .in_flight_tasks
            .checked_sub(1)
            .expect("a completed encoding task is in flight");
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(reserved_bytes)
            .expect("a completed encoding task owns its reservation");
    }
}

fn validate_config<E>(config: PipelineConfig) -> Result<(), PipelineError<E>> {
    if config.worker_count == 0 {
        return Err(PipelineError::InvalidConfig(
            "worker_count must be non-zero",
        ));
    }
    if config.max_in_flight_tasks == 0 {
        return Err(PipelineError::InvalidConfig(
            "max_in_flight_tasks must be non-zero",
        ));
    }
    if config.max_in_flight_bytes == 0 {
        return Err(PipelineError::InvalidConfig(
            "max_in_flight_bytes must be non-zero",
        ));
    }
    Ok(())
}

fn worker_loop<T, R, E, Worker>(
    worker: &mut Worker,
    task_receiver: Arc<Mutex<Receiver<OrderedTask<T>>>>,
    result_sender: mpsc::Sender<WorkerResult<R, E>>,
    cancelled: Arc<AtomicBool>,
) where
    T: Send,
    R: Send,
    E: Send,
    Worker: FnMut(T) -> Result<R, E>,
{
    loop {
        let task = {
            let receiver = task_receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            receiver.recv()
        };
        let Ok(task) = task else {
            return;
        };
        if cancelled.load(Ordering::Acquire) {
            return;
        }

        let sequence = task.sequence;
        let reserved_bytes = task.reserved_bytes;
        let outcome = match panic::catch_unwind(AssertUnwindSafe(|| worker(task.payload))) {
            Ok(result) => WorkerOutcome::Completed(result),
            Err(payload) => WorkerOutcome::Panicked(panic_message(payload)),
        };
        let must_stop = !matches!(&outcome, WorkerOutcome::Completed(Ok(_)));
        if result_sender
            .send(WorkerResult {
                sequence,
                reserved_bytes,
                outcome,
            })
            .is_err()
        {
            return;
        }
        if must_stop {
            return;
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}

fn drive_pipeline<Iter, T, R, E, Commit>(
    mut tasks: Iter,
    config: PipelineConfig,
    task_sender: SyncSender<OrderedTask<T>>,
    result_receiver: &Receiver<WorkerResult<R, E>>,
    cancelled: &AtomicBool,
    commit: &mut Commit,
) -> Result<PipelineStats, PipelineError<E>>
where
    Iter: Iterator<Item = OrderedTask<T>>,
    T: Send,
    R: Send,
    E: Send,
    Commit: FnMut(u64, R) -> Result<(), E>,
{
    let mut stats = PipelineStats::default();
    let mut pending = None;
    let mut input_exhausted = false;
    let mut sender = Some(task_sender);
    let mut next_submit = Some(config.first_sequence);
    let mut next_commit = Some(config.first_sequence);
    let mut in_flight_tasks = 0usize;
    let mut reserved_bytes = 0usize;
    let mut reorder = BTreeMap::new();

    loop {
        while in_flight_tasks < config.max_in_flight_tasks && !input_exhausted {
            if pending.is_none() {
                pending = tasks.next();
                if pending.is_none() {
                    input_exhausted = true;
                    sender.take();
                    break;
                }
            }

            let task = pending.take().expect("pending task is present");
            let expected = next_submit.ok_or(PipelineError::SequenceOverflow)?;
            if task.sequence != expected {
                cancelled.store(true, Ordering::Release);
                return Err(PipelineError::InvalidSequence {
                    expected,
                    actual: task.sequence,
                });
            }
            if task.reserved_bytes > config.max_in_flight_bytes {
                cancelled.store(true, Ordering::Release);
                return Err(PipelineError::TaskExceedsByteBudget {
                    sequence: task.sequence,
                    reserved_bytes: task.reserved_bytes,
                    max_in_flight_bytes: config.max_in_flight_bytes,
                });
            }
            let Some(next_reserved_bytes) = reserved_bytes.checked_add(task.reserved_bytes) else {
                cancelled.store(true, Ordering::Release);
                return Err(PipelineError::TaskExceedsByteBudget {
                    sequence: task.sequence,
                    reserved_bytes: task.reserved_bytes,
                    max_in_flight_bytes: config.max_in_flight_bytes,
                });
            };
            if next_reserved_bytes > config.max_in_flight_bytes {
                pending = Some(task);
                break;
            }

            let sequence = task.sequence;
            let task_bytes = task.reserved_bytes;
            match sender
                .as_ref()
                .expect("sender exists while input is active")
                .try_send(task)
            {
                Ok(()) => {
                    in_flight_tasks += 1;
                    reserved_bytes = next_reserved_bytes;
                    stats.submitted_tasks += 1;
                    stats.peak_in_flight_tasks = stats.peak_in_flight_tasks.max(in_flight_tasks);
                    stats.peak_reserved_bytes = stats.peak_reserved_bytes.max(reserved_bytes);
                    next_submit = sequence.checked_add(1);
                }
                Err(TrySendError::Full(task)) => {
                    pending = Some(task);
                    break;
                }
                Err(TrySendError::Disconnected(_)) => {
                    cancelled.store(true, Ordering::Release);
                    return Err(PipelineError::ChannelClosed);
                }
            }

            debug_assert!(reserved_bytes >= task_bytes);
        }

        if input_exhausted && in_flight_tasks == 0 {
            return Ok(stats);
        }
        if in_flight_tasks == 0 {
            cancelled.store(true, Ordering::Release);
            return Err(PipelineError::ChannelClosed);
        }

        let result = result_receiver.recv().map_err(|_| {
            cancelled.store(true, Ordering::Release);
            PipelineError::ChannelClosed
        })?;
        let sequence = result.sequence;
        if reorder.insert(sequence, result).is_some() {
            cancelled.store(true, Ordering::Release);
            return Err(PipelineError::DuplicateResult { sequence });
        }

        while let Some(sequence) = next_commit {
            let Some(result) = reorder.remove(&sequence) else {
                break;
            };
            let result_reserved_bytes = result.reserved_bytes;

            match result.outcome {
                WorkerOutcome::Completed(Ok(output)) => {
                    commit(sequence, output)
                        .map_err(|source| PipelineError::Commit { sequence, source })?;
                    stats.committed_tasks += 1;
                    next_commit = sequence.checked_add(1);
                }
                WorkerOutcome::Completed(Err(source)) => {
                    cancelled.store(true, Ordering::Release);
                    return Err(PipelineError::Worker { sequence, source });
                }
                WorkerOutcome::Panicked(message) => {
                    cancelled.store(true, Ordering::Release);
                    return Err(PipelineError::WorkerPanicked { sequence, message });
                }
            }
            // Keep this result's reservation until ordered commit is complete.
            // The commit callback can encode and compress output pages, so
            // releasing it before the callback permits new work to refill the
            // byte budget during that peak.
            in_flight_tasks = in_flight_tasks
                .checked_sub(1)
                .expect("one result exists for each in-flight task");
            reserved_bytes = reserved_bytes
                .checked_sub(result_reserved_bytes)
                .expect("result reservation matches its task");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    use super::*;

    fn config(workers: usize) -> PipelineConfig {
        PipelineConfig {
            worker_count: workers,
            max_in_flight_tasks: 8,
            max_in_flight_bytes: 32,
            first_sequence: 0,
        }
    }

    #[test]
    fn commits_out_of_order_results_in_sequence() {
        let tasks = (0..12).map(|sequence| OrderedTask::new(sequence, 1, sequence));
        let mut committed = Vec::new();

        let stats = run_ordered_pipeline(
            tasks,
            config(4),
            |_| {
                Ok::<_, Infallible>(move |value| {
                    thread::sleep(Duration::from_millis((12 - value) % 4));
                    Ok(value * 2)
                })
            },
            |sequence, value| {
                committed.push((sequence, value));
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            committed,
            (0..12).map(|value| (value, value * 2)).collect::<Vec<_>>()
        );
        assert_eq!(stats.submitted_tasks, 12);
        assert_eq!(stats.committed_tasks, 12);
    }

    #[test]
    fn output_is_identical_for_one_and_many_workers() {
        fn run(workers: usize) -> Vec<u8> {
            let tasks = (0_u64..100).map(|sequence| OrderedTask::new(sequence, 16, sequence));
            let mut bytes = Vec::new();
            let mut pipeline_config = config(workers);
            pipeline_config.max_in_flight_tasks = 16;
            pipeline_config.max_in_flight_bytes = 256;
            run_ordered_pipeline(
                tasks,
                pipeline_config,
                |_| Ok::<_, Infallible>(|value: u64| Ok(value.wrapping_mul(0x9e37_79b9))),
                |sequence, value| {
                    bytes.extend_from_slice(&sequence.to_le_bytes());
                    bytes.extend_from_slice(&value.to_le_bytes());
                    Ok(())
                },
            )
            .unwrap();
            bytes
        }

        assert_eq!(run(1), run(8));
    }

    #[test]
    fn byte_and_task_limits_apply_to_uncommitted_work() {
        let tasks = (0..20).map(|sequence| OrderedTask::new(sequence, 4, sequence));
        let stats = run_ordered_pipeline(
            tasks,
            PipelineConfig {
                worker_count: 4,
                max_in_flight_tasks: 3,
                max_in_flight_bytes: 8,
                first_sequence: 0,
            },
            |_| Ok::<_, Infallible>(Ok),
            |_, _| Ok(()),
        )
        .unwrap();

        assert_eq!(stats.peak_in_flight_tasks, 2);
        assert_eq!(stats.peak_reserved_bytes, 8);
    }

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Worker(u64),
        Commit(u64),
        Init,
        Producer,
        Sink(String),
    }

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "{self:?}")
        }
    }

    impl Error for TestError {}

    #[test]
    fn reports_worker_error_after_deterministic_committed_prefix() {
        let tasks = (0..10).map(|sequence| OrderedTask::new(sequence, 1, sequence));
        let mut committed = Vec::new();
        let error = run_ordered_pipeline(
            tasks,
            config(4),
            |_| {
                Ok(|value| {
                    if value == 3 {
                        Err(TestError::Worker(value))
                    } else {
                        Ok(value)
                    }
                })
            },
            |sequence, _| {
                committed.push(sequence);
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(committed, vec![0, 1, 2]);
        assert!(matches!(
            error,
            PipelineError::Worker {
                sequence: 3,
                source: TestError::Worker(3)
            }
        ));
    }

    #[test]
    fn reports_commit_error_and_stops_the_prefix() {
        let tasks = (0..10).map(|sequence| OrderedTask::new(sequence, 1, sequence));
        let error = run_ordered_pipeline(
            tasks,
            config(4),
            |_| Ok(Ok::<_, TestError>),
            |sequence, _| {
                if sequence == 4 {
                    Err(TestError::Commit(sequence))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            PipelineError::Commit {
                sequence: 4,
                source: TestError::Commit(4)
            }
        ));
    }

    #[test]
    fn catches_worker_panics() {
        let tasks = (0..4).map(|sequence| OrderedTask::new(sequence, 1, sequence));
        let error = run_ordered_pipeline(
            tasks,
            config(2),
            |_| {
                Ok::<_, Infallible>(|value| {
                    if value == 2 {
                        panic!("bad block");
                    }
                    Ok(value)
                })
            },
            |_, _| Ok(()),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            PipelineError::WorkerPanicked {
                sequence: 2,
                ref message
            } if message == "bad block"
        ));
    }

    #[test]
    fn rejects_sequence_gaps() {
        let tasks = [OrderedTask::new(7, 1, ()), OrderedTask::new(9, 1, ())];
        let error = run_ordered_pipeline(
            tasks,
            PipelineConfig {
                first_sequence: 7,
                ..config(1)
            },
            |_| Ok::<_, Infallible>(|()| Ok(())),
            |_, ()| Ok(()),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            PipelineError::InvalidSequence {
                expected: 8,
                actual: 9
            }
        ));
    }

    #[test]
    fn rejects_a_task_larger_than_the_byte_budget() {
        let error = run_ordered_pipeline(
            [OrderedTask::new(0, 33, ())],
            config(1),
            |_| Ok::<_, Infallible>(|()| Ok(())),
            |_, ()| Ok(()),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            PipelineError::TaskExceedsByteBudget {
                sequence: 0,
                reserved_bytes: 33,
                max_in_flight_bytes: 32
            }
        ));
    }

    #[test]
    fn initializes_each_worker_once_before_submission() {
        let initialized = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&initialized);
        let error = run_ordered_pipeline(
            [OrderedTask::new(0, 1, ())],
            config(3),
            move |worker_index| {
                observed.fetch_add(1, Ordering::Relaxed);
                if worker_index == 2 {
                    Err(TestError::Init)
                } else {
                    Ok(|()| Ok(()))
                }
            },
            |_, ()| Ok(()),
        )
        .unwrap_err();

        assert_eq!(initialized.load(Ordering::Relaxed), 3);
        assert!(matches!(
            error,
            PipelineError::WorkerInitialization {
                worker_index: 2,
                source: TestError::Init
            }
        ));
    }

    fn encoding_config(workers: usize, max_tasks: usize, max_bytes: usize) -> PipelineConfig {
        PipelineConfig {
            worker_count: workers,
            max_in_flight_tasks: max_tasks,
            max_in_flight_bytes: max_bytes,
            first_sequence: 0,
        }
    }

    fn encoding_adapter_bytes(workers: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        let config = encoding_config(workers, 4, 16);
        if workers == 0 {
            run_inline_ordered_encoding_stage(
                config,
                |_| Ok::<_, TestError>(|value: u64| Ok(value.wrapping_mul(11))),
                |sequence, value| {
                    bytes.extend_from_slice(&sequence.to_le_bytes());
                    bytes.extend_from_slice(&value.to_le_bytes());
                    Ok(())
                },
                |sink| {
                    for sequence in 0_u64..64 {
                        sink.submit(OrderedTask::new(sequence, 4, sequence))
                            .map_err(|error| TestError::Sink(error.to_string()))?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        } else {
            run_ordered_encoding_stage(
                config,
                |_| {
                    Ok::<_, TestError>(|value: u64| {
                        thread::sleep(Duration::from_micros((value % 4) * 10));
                        Ok(value.wrapping_mul(11))
                    })
                },
                |sequence, value| {
                    bytes.extend_from_slice(&sequence.to_le_bytes());
                    bytes.extend_from_slice(&value.to_le_bytes());
                    Ok(())
                },
                |sink| {
                    for sequence in 0_u64..64 {
                        sink.submit(OrderedTask::new(sequence, 4, sequence))
                            .map_err(|error| TestError::Sink(error.to_string()))?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        }
        bytes
    }

    #[test]
    fn reader_adapter_has_inline_one_and_many_worker_parity() {
        let inline = encoding_adapter_bytes(0);
        assert_eq!(encoding_adapter_bytes(1), inline);
        assert_eq!(encoding_adapter_bytes(4), inline);
    }

    #[test]
    fn reader_adapter_releases_its_byte_budget_after_each_commit() {
        let stats = run_ordered_encoding_stage(
            encoding_config(4, 8, 8),
            |_| Ok::<_, TestError>(Ok::<u64, TestError>),
            |_, _| Ok(()),
            |sink| {
                for sequence in 0_u64..24 {
                    sink.submit(OrderedTask::new(sequence, 4, sequence))
                        .map_err(|error| TestError::Sink(error.to_string()))?;
                }
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(stats.submitted_tasks, 24);
        assert_eq!(stats.committed_tasks, 24);
        assert_eq!(stats.peak_in_flight_tasks, 2);
        assert_eq!(stats.peak_reserved_bytes, 8);
    }

    #[test]
    fn reader_adapter_reclaims_tasks_after_a_worker_error() {
        struct TrackedTask {
            sequence: u64,
            drops: Arc<AtomicUsize>,
        }

        impl Drop for TrackedTask {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(5));
        let worker_barrier = Arc::clone(&barrier);
        let mut committed = Vec::new();
        let error = run_ordered_encoding_stage(
            encoding_config(4, 4, 4),
            move |_| {
                let barrier = Arc::clone(&worker_barrier);
                Ok::<_, TestError>(move |task: TrackedTask| {
                    let sequence = task.sequence;
                    barrier.wait();
                    if sequence == 3 {
                        Err(TestError::Worker(sequence))
                    } else {
                        Ok(sequence)
                    }
                })
            },
            |sequence, _| {
                committed.push(sequence);
                Ok(())
            },
            |sink| {
                for sequence in 0_u64..4 {
                    sink.submit(OrderedTask::new(
                        sequence,
                        1,
                        TrackedTask {
                            sequence,
                            drops: Arc::clone(&drops),
                        },
                    ))
                    .map_err(|error| TestError::Sink(error.to_string()))?;
                }
                barrier.wait();
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(committed, vec![0, 1, 2]);
        assert_eq!(drops.load(Ordering::Relaxed), 4);
        assert!(matches!(
            error,
            OrderedEncodingStageError::Pipeline(PipelineError::Worker {
                sequence: 3,
                source: TestError::Worker(3)
            })
        ));
    }

    #[test]
    fn lower_worker_error_precedes_a_higher_producer_error() {
        let mut committed = Vec::new();
        let error = run_ordered_encoding_stage(
            encoding_config(2, 2, 2),
            |_| {
                Ok::<_, TestError>(|sequence| {
                    if sequence == 0 {
                        thread::sleep(Duration::from_millis(10));
                        Err(TestError::Worker(sequence))
                    } else {
                        Ok(sequence)
                    }
                })
            },
            |sequence, _| {
                committed.push(sequence);
                Ok(())
            },
            |sink| {
                sink.submit(OrderedTask::new(0, 1, 0))
                    .map_err(|error| TestError::Sink(error.to_string()))?;
                sink.submit(OrderedTask::new(1, 1, 1))
                    .map_err(|error| TestError::Sink(error.to_string()))?;
                Err(TestError::Producer)
            },
        )
        .unwrap_err();

        assert!(committed.is_empty());
        assert!(matches!(
            error,
            OrderedEncodingStageError::Pipeline(PipelineError::Worker {
                sequence: 0,
                source: TestError::Worker(0)
            })
        ));
    }

    #[test]
    fn worker_error_observed_during_later_submit_keeps_its_typed_error() {
        let error = run_ordered_encoding_stage(
            encoding_config(1, 1, 1),
            |_| {
                Ok::<_, TestError>(|sequence| {
                    if sequence == 0 {
                        Err(TestError::Worker(sequence))
                    } else {
                        Ok(sequence)
                    }
                })
            },
            |_, _| Ok(()),
            |sink| {
                sink.submit(OrderedTask::new(0, 1, 0))
                    .map_err(|error| TestError::Sink(error.to_string()))?;
                // The one-task bound forces this submission to receive task
                // zero's result before task one can enter the queue.
                sink.submit(OrderedTask::new(1, 1, 1))
                    .map_err(|error| TestError::Sink(error.to_string()))?;
                Ok(())
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            OrderedEncodingStageError::Pipeline(PipelineError::Worker {
                sequence: 0,
                source: TestError::Worker(0)
            })
        ));
    }
}
