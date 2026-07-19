//! Authenticated, downstream-only Yellowstone block relay.
//!
//! The relay deliberately has no upstream transport. A caller publishes a block only after the
//! corresponding WAL record is durable. Downstream clients can then consume the live tail or
//! replay a `from_slot` that is still present in the bounded in-memory ring.

use std::{
    collections::VecDeque,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
};

use futures::{Stream, StreamExt, stream};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use yellowstone_grpc_proto::{
    prelude::{
        CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeDeshredRequest, SubscribeReplayInfoRequest,
        SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
        SubscribeUpdatePong, geyser_server::Geyser, subscribe_update::UpdateOneof,
    },
    tonic::{Request, Response, Status, metadata::MetadataMap},
};

/// Metadata header used by Yellowstone's shared-token authentication.
pub const YELLOWSTONE_X_TOKEN_HEADER: &str = "x-token";

const MAX_FILTER_LABELS: usize = 16;
const MAX_FILTER_LABEL_BYTES: usize = 128;

type SubscribeStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static>>;
type SubscribeDeshredStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdateDeshred, Status>> + Send + 'static>>;

/// Hard memory/concurrency limits for a relay instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YellowstoneBlockRelayLimits {
    /// Maximum retained block records.
    pub max_records: usize,
    /// Maximum sum of retained protobuf-encoded update bytes.
    pub max_encoded_bytes: usize,
    /// Maximum simultaneously registered block subscriptions.
    pub max_clients: usize,
}

impl Default for YellowstoneBlockRelayLimits {
    fn default() -> Self {
        Self {
            max_records: 128,
            max_encoded_bytes: 128 * 1024 * 1024,
            max_clients: 4,
        }
    }
}

/// A shared-token verifier that retains only a SHA-256 digest of the configured token.
#[derive(Clone)]
pub struct YellowstoneRelayAuth {
    digest: [u8; 32],
}

impl YellowstoneRelayAuth {
    pub fn from_shared_x_token(token: impl AsRef<[u8]>) -> Result<Self, RelayConfigError> {
        let token = token.as_ref();
        if token.is_empty() {
            return Err(RelayConfigError::EmptyToken);
        }
        Ok(Self {
            digest: Sha256::digest(token).into(),
        })
    }

    fn accepts(&self, candidate: &[u8]) -> bool {
        let candidate: [u8; 32] = Sha256::digest(candidate).into();
        constant_time_digest_eq(&self.digest, &candidate)
    }
}

impl fmt::Debug for YellowstoneRelayAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("YellowstoneRelayAuth(<redacted>)")
    }
}

fn constant_time_digest_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut difference = 0u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayConfigError {
    EmptyToken,
    ZeroMaxRecords,
    ZeroMaxEncodedBytes,
    ZeroMaxClients,
}

impl fmt::Display for RelayConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyToken => "relay x-token must not be empty",
            Self::ZeroMaxRecords => "relay max_records must be non-zero",
            Self::ZeroMaxEncodedBytes => "relay max_encoded_bytes must be non-zero",
            Self::ZeroMaxClients => "relay max_clients must be non-zero",
        })
    }
}

impl std::error::Error for RelayConfigError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayPublishError {
    StatePoisoned,
    Closed,
    NotBlockUpdate,
    BlockContainsAccounts,
    NonMonotonicSequence {
        previous: u64,
        proposed: u64,
    },
    EncodedByteAccountingOverflow,
    EncodedRecordTooLarge {
        encoded_bytes: usize,
        maximum: usize,
    },
}

impl fmt::Display for RelayPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StatePoisoned => formatter.write_str("relay state lock is poisoned"),
            Self::Closed => formatter.write_str("relay is closed"),
            Self::NotBlockUpdate => formatter.write_str("relay accepts only block updates"),
            Self::BlockContainsAccounts => {
                formatter.write_str("relay block update unexpectedly contains account payloads")
            }
            Self::NonMonotonicSequence { previous, proposed } => write!(
                formatter,
                "durable sequence must increase: previous={previous} proposed={proposed}"
            ),
            Self::EncodedByteAccountingOverflow => {
                formatter.write_str("relay encoded-byte accounting overflow")
            }
            Self::EncodedRecordTooLarge {
                encoded_bytes,
                maximum,
            } => write!(
                formatter,
                "encoded relay record is too large: encoded_bytes={encoded_bytes} maximum={maximum}"
            ),
        }
    }
}

impl std::error::Error for RelayPublishError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayPublishReport {
    pub durable_sequence: u64,
    pub durable_slot: u64,
    pub encoded_bytes: usize,
    pub evicted_records: usize,
    pub evicted_encoded_bytes: usize,
    pub retained_records: usize,
    pub retained_encoded_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YellowstoneBlockRelayStats {
    pub retained_records: usize,
    pub retained_encoded_bytes: usize,
    pub first_available_slot: Option<u64>,
    pub last_durable_slot: Option<u64>,
    pub last_durable_sequence: Option<u64>,
    pub active_clients: usize,
}

#[derive(Debug)]
struct StoredBlockUpdate {
    durable_sequence: u64,
    slot: u64,
    encoded_bytes: usize,
    update: SubscribeUpdate,
}

#[derive(Debug, Default)]
struct RelayState {
    ring: VecDeque<Arc<StoredBlockUpdate>>,
    retained_encoded_bytes: usize,
    last_durable_sequence: Option<u64>,
    last_durable_slot: Option<u64>,
    evicted_through_sequence: Option<u64>,
    active_clients: usize,
    closed: bool,
}

#[derive(Debug)]
struct RelayInner {
    auth: YellowstoneRelayAuth,
    limits: YellowstoneBlockRelayLimits,
    state: Mutex<RelayState>,
    publication: watch::Sender<Option<u64>>,
}

/// Cloneable relay handle and Yellowstone `Geyser` service implementation.
#[derive(Clone, Debug)]
pub struct YellowstoneBlockRelay {
    inner: Arc<RelayInner>,
}

impl YellowstoneBlockRelay {
    pub fn new(
        auth: YellowstoneRelayAuth,
        limits: YellowstoneBlockRelayLimits,
    ) -> Result<Self, RelayConfigError> {
        if limits.max_records == 0 {
            return Err(RelayConfigError::ZeroMaxRecords);
        }
        if limits.max_encoded_bytes == 0 {
            return Err(RelayConfigError::ZeroMaxEncodedBytes);
        }
        if limits.max_clients == 0 {
            return Err(RelayConfigError::ZeroMaxClients);
        }
        let (publication, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(RelayInner {
                auth,
                limits,
                state: Mutex::new(RelayState::default()),
                publication,
            }),
        })
    }

    /// Publish one already-fsynced block update.
    ///
    /// The caller owns the durability boundary: this method must be invoked only after the exact
    /// update and its `durable_sequence` are recoverable from the caller's WAL. The relay never
    /// contacts an upstream provider and never acknowledges source durability itself.
    pub fn publish_fsynced(
        &self,
        durable_sequence: u64,
        mut update: SubscribeUpdate,
    ) -> Result<RelayPublishReport, RelayPublishError> {
        let slot = match update.update_oneof.as_ref() {
            Some(UpdateOneof::Block(block)) => {
                if !block.accounts.is_empty() {
                    return Err(RelayPublishError::BlockContainsAccounts);
                }
                block.slot
            }
            _ => return Err(RelayPublishError::NotBlockUpdate),
        };

        // Upstream labels have no meaning downstream and must not consume retained-byte budget.
        update.filters.clear();
        let encoded_bytes = update.encoded_len();
        if encoded_bytes > self.inner.limits.max_encoded_bytes {
            return Err(RelayPublishError::EncodedRecordTooLarge {
                encoded_bytes,
                maximum: self.inner.limits.max_encoded_bytes,
            });
        }

        let mut state = self.publish_state()?;
        if state.closed {
            return Err(RelayPublishError::Closed);
        }
        if let Some(previous) = state.last_durable_sequence
            && durable_sequence <= previous
        {
            return Err(RelayPublishError::NonMonotonicSequence {
                previous,
                proposed: durable_sequence,
            });
        }
        let retained_encoded_bytes = state
            .retained_encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or(RelayPublishError::EncodedByteAccountingOverflow)?;
        state.ring.push_back(Arc::new(StoredBlockUpdate {
            durable_sequence,
            slot,
            encoded_bytes,
            update,
        }));
        state.retained_encoded_bytes = retained_encoded_bytes;
        state.last_durable_sequence = Some(durable_sequence);
        state.last_durable_slot = Some(slot);

        let mut evicted_records = 0usize;
        let mut evicted_encoded_bytes = 0usize;
        while state.ring.len() > self.inner.limits.max_records
            || state.retained_encoded_bytes > self.inner.limits.max_encoded_bytes
        {
            let evicted = state
                .ring
                .pop_front()
                .expect("a newly inserted relay record is retained until limits are applied");
            state.retained_encoded_bytes -= evicted.encoded_bytes;
            state.evicted_through_sequence = Some(evicted.durable_sequence);
            evicted_records += 1;
            evicted_encoded_bytes += evicted.encoded_bytes;
        }

        let report = RelayPublishReport {
            durable_sequence,
            durable_slot: slot,
            encoded_bytes,
            evicted_records,
            evicted_encoded_bytes,
            retained_records: state.ring.len(),
            retained_encoded_bytes: state.retained_encoded_bytes,
        };
        self.inner.publication.send_replace(Some(durable_sequence));
        Ok(report)
    }

    pub fn stats(&self) -> Result<YellowstoneBlockRelayStats, RelayPublishError> {
        let state = self.publish_state()?;
        Ok(stats_from_state(&state))
    }

    /// Stop accepting publications/subscriptions and wake every active subscription with a
    /// retryable terminal status. This lets an owning tonic server drain cleanly.
    pub fn close(&self) -> Result<(), RelayPublishError> {
        let publication = {
            let mut state = self.publish_state()?;
            state.closed = true;
            state.last_durable_sequence
        };
        self.inner.publication.send_replace(publication);
        Ok(())
    }

    /// Wrap this relay in the generated Yellowstone service. The caller owns listener/TLS setup.
    pub fn into_geyser_service(
        self,
    ) -> yellowstone_grpc_proto::prelude::geyser_server::GeyserServer<Self> {
        yellowstone_grpc_proto::prelude::geyser_server::GeyserServer::new(self)
    }

    fn publish_state(&self) -> Result<MutexGuard<'_, RelayState>, RelayPublishError> {
        self.inner
            .state
            .lock()
            .map_err(|_| RelayPublishError::StatePoisoned)
    }

    fn status_state(&self) -> Result<MutexGuard<'_, RelayState>, Status> {
        self.inner
            .state
            .lock()
            .map_err(|_| Status::unavailable("relay state unavailable; retryable=true"))
    }

    fn authenticate(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let candidate = metadata
            .get(YELLOWSTONE_X_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("invalid relay credentials"))?;
        if !self.inner.auth.accepts(candidate.as_bytes()) {
            return Err(Status::unauthenticated("invalid relay credentials"));
        }
        Ok(())
    }

    fn open_subscription(
        &self,
        filter_labels: Vec<String>,
        from_slot: Option<u64>,
    ) -> Result<RelaySubscription, Status> {
        let mut state = self.status_state()?;
        if state.closed {
            return Err(Status::unavailable(
                "relay is shutting down; retryable=true",
            ));
        }
        if state.active_clients >= self.inner.limits.max_clients {
            return Err(Status::resource_exhausted(format!(
                "relay client limit reached; maximum_clients={}; retryable=true",
                self.inner.limits.max_clients
            )));
        }

        let (start_sequence, cursor_sequence) = match from_slot {
            Some(from_slot) => {
                let first = state.ring.iter().map(|record| record.slot).min();
                let highest = state.ring.iter().map(|record| record.slot).max();
                let last = state.last_durable_slot;
                let Some(start) = state.ring.iter().find(|record| record.slot >= from_slot) else {
                    return Err(from_slot_out_of_range(from_slot, first, highest, last));
                };
                if first.is_some_and(|first| from_slot < first)
                    || highest.is_some_and(|highest| from_slot > highest)
                {
                    return Err(from_slot_out_of_range(from_slot, first, highest, last));
                }
                (Some(start.durable_sequence), None)
            }
            None => (None, state.last_durable_sequence),
        };
        let publication = self.inner.publication.subscribe();
        state.active_clients += 1;
        drop(state);

        Ok(RelaySubscription {
            inner: Arc::clone(&self.inner),
            publication,
            filter_labels,
            start_sequence,
            cursor_sequence,
            last_delivered_slot: None,
        })
    }
}

fn stats_from_state(state: &RelayState) -> YellowstoneBlockRelayStats {
    YellowstoneBlockRelayStats {
        retained_records: state.ring.len(),
        retained_encoded_bytes: state.retained_encoded_bytes,
        first_available_slot: state.ring.iter().map(|record| record.slot).min(),
        last_durable_slot: state.last_durable_slot,
        last_durable_sequence: state.last_durable_sequence,
        active_clients: state.active_clients,
    }
}

fn from_slot_out_of_range(
    requested: u64,
    first_available: Option<u64>,
    highest_available: Option<u64>,
    last_durable: Option<u64>,
) -> Status {
    Status::out_of_range(format!(
        "requested_from_slot={requested} first_available_slot={} highest_available_slot={} last_durable_slot={} retryable=true",
        display_slot(first_available),
        display_slot(highest_available),
        display_slot(last_durable)
    ))
}

fn display_slot(slot: Option<u64>) -> String {
    slot.map_or_else(|| "none".to_string(), |slot| slot.to_string())
}

#[derive(Debug)]
struct RelaySubscription {
    inner: Arc<RelayInner>,
    publication: watch::Receiver<Option<u64>>,
    filter_labels: Vec<String>,
    start_sequence: Option<u64>,
    cursor_sequence: Option<u64>,
    last_delivered_slot: Option<u64>,
}

impl RelaySubscription {
    async fn recv(&mut self) -> Result<SubscribeUpdate, Status> {
        loop {
            let (next, closed) = {
                let state =
                    self.inner.state.lock().map_err(|_| {
                        Status::unavailable("relay state unavailable; retryable=true")
                    })?;
                self.ensure_not_lagged(&state)?;
                let next = if state.closed {
                    None
                } else {
                    state
                        .ring
                        .iter()
                        .find(|record| {
                            self.start_sequence.map_or_else(
                                || {
                                    self.cursor_sequence
                                        .is_none_or(|cursor| record.durable_sequence > cursor)
                                },
                                |start| record.durable_sequence >= start,
                            )
                        })
                        .cloned()
                };
                (next, state.closed)
            };

            if let Some(next) = next {
                self.start_sequence = None;
                self.cursor_sequence = Some(next.durable_sequence);
                self.last_delivered_slot = Some(next.slot);
                let mut update = next.update.clone();
                update.filters.clone_from(&self.filter_labels);
                return Ok(update);
            }

            if closed {
                return Err(Status::unavailable(format!(
                    "relay closed; retryable=true; last_durable_slot={}; last_delivered_slot={}",
                    display_slot(self.last_durable_slot()),
                    display_slot(self.last_delivered_slot)
                )));
            }

            self.publication.changed().await.map_err(|_| {
                Status::unavailable(format!(
                    "relay closed; retryable=true; last_durable_slot={}; last_delivered_slot={}",
                    display_slot(self.last_durable_slot()),
                    display_slot(self.last_delivered_slot)
                ))
            })?;
        }
    }

    fn ensure_not_lagged(&self, state: &RelayState) -> Result<(), Status> {
        let missed = match (self.start_sequence, self.cursor_sequence) {
            (Some(start), _) => state
                .evicted_through_sequence
                .is_some_and(|evicted| evicted >= start),
            (None, Some(cursor)) => state
                .evicted_through_sequence
                .is_some_and(|evicted| evicted > cursor),
            (None, None) => state.evicted_through_sequence.is_some(),
        };
        if missed {
            return Err(Status::unavailable(format!(
                "relay client lagged; retryable=true; last_durable_slot={}; last_delivered_slot={}",
                display_slot(state.last_durable_slot),
                display_slot(self.last_delivered_slot)
            )));
        }
        Ok(())
    }

    fn last_durable_slot(&self) -> Option<u64> {
        self.inner
            .state
            .lock()
            .ok()
            .and_then(|state| state.last_durable_slot)
    }
}

impl Drop for RelaySubscription {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.active_clients = state.active_clients.saturating_sub(1);
        }
    }
}

#[derive(Debug)]
struct ValidBlockSubscription {
    filter_labels: Vec<String>,
    from_slot: Option<u64>,
}

fn validate_block_subscription(
    request: &SubscribeRequest,
) -> Result<ValidBlockSubscription, Status> {
    if !request.accounts.is_empty()
        || !request.slots.is_empty()
        || !request.transactions.is_empty()
        || !request.transactions_status.is_empty()
        || !request.blocks_meta.is_empty()
        || !request.entry.is_empty()
        || !request.accounts_data_slice.is_empty()
    {
        return Err(Status::invalid_argument(
            "relay supports only unfiltered block subscriptions",
        ));
    }
    if request.ping.is_some() {
        return Err(Status::invalid_argument(
            "subscription filters and ping must be sent as separate requests",
        ));
    }
    if request.blocks.is_empty() {
        return Err(Status::invalid_argument(
            "relay subscription requires at least one block filter",
        ));
    }
    if request.blocks.len() > MAX_FILTER_LABELS {
        return Err(Status::invalid_argument(format!(
            "too many block filter labels; maximum={MAX_FILTER_LABELS}"
        )));
    }
    if request
        .commitment
        .is_some_and(|level| level != CommitmentLevel::Confirmed as i32)
    {
        return Err(Status::invalid_argument(
            "relay serves only confirmed block updates",
        ));
    }

    let mut labels = Vec::with_capacity(request.blocks.len());
    for (label, filter) in &request.blocks {
        if label.is_empty() || label.len() > MAX_FILTER_LABEL_BYTES {
            return Err(Status::invalid_argument(format!(
                "block filter label length must be 1..={MAX_FILTER_LABEL_BYTES} bytes"
            )));
        }
        if !filter.account_include.is_empty()
            || filter.cuckoo_account_include.is_some()
            || filter.include_transactions != Some(true)
            || filter.include_entries != Some(true)
            || filter.include_accounts == Some(true)
        {
            return Err(Status::invalid_argument(
                "block filters must request transactions and entries without accounts or account filtering",
            ));
        }
        labels.push(label.clone());
    }
    labels.sort_unstable();

    Ok(ValidBlockSubscription {
        filter_labels: labels,
        from_slot: request.from_slot,
    })
}

fn ping_only(request: &SubscribeRequest) -> Option<i32> {
    let ping = request.ping.as_ref()?;
    if request.accounts.is_empty()
        && request.slots.is_empty()
        && request.transactions.is_empty()
        && request.transactions_status.is_empty()
        && request.blocks.is_empty()
        && request.blocks_meta.is_empty()
        && request.entry.is_empty()
        && request.accounts_data_slice.is_empty()
        && request.commitment.is_none()
        && request.from_slot.is_none()
    {
        Some(ping.id)
    } else {
        None
    }
}

fn pong_update(id: i32) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: Vec::new(),
        update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id })),
        created_at: None,
    }
}

struct RelayConnection {
    inbound: yellowstone_grpc_proto::tonic::Streaming<SubscribeRequest>,
    inbound_open: bool,
    subscription: RelaySubscription,
    terminated: bool,
}

impl RelayConnection {
    async fn next(mut self) -> Option<(Result<SubscribeUpdate, Status>, Self)> {
        if self.terminated {
            return None;
        }

        loop {
            if !self.inbound_open {
                let update = self.subscription.recv().await;
                self.terminated = update.is_err();
                return Some((update, self));
            }
            tokio::select! {
                update = self.subscription.recv() => {
                    self.terminated = update.is_err();
                    return Some((update, self));
                },
                request = self.inbound.next() => {
                    match self.handle_inbound(request) {
                        Some(item) => return Some((item, self)),
                        None => continue,
                    }
                }
            }
        }
    }

    fn handle_inbound(
        &mut self,
        request: Option<Result<SubscribeRequest, Status>>,
    ) -> Option<Result<SubscribeUpdate, Status>> {
        match request {
            Some(Ok(request)) => {
                if let Some(id) = ping_only(&request) {
                    return Some(Ok(pong_update(id)));
                }
                self.terminated = true;
                Some(Err(Status::invalid_argument(
                    "dynamic relay subscription changes are unsupported",
                )))
            }
            Some(Err(status)) => {
                self.terminated = true;
                Some(Err(status))
            }
            None => {
                self.inbound_open = false;
                None
            }
        }
    }
}

#[yellowstone_grpc_proto::tonic::async_trait]
impl Geyser for YellowstoneBlockRelay {
    type SubscribeStream = SubscribeStream;

    async fn subscribe(
        &self,
        request: Request<yellowstone_grpc_proto::tonic::Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.authenticate(request.metadata())?;
        let mut inbound = request.into_inner();
        let initial = inbound.next().await.transpose()?.ok_or_else(|| {
            Status::invalid_argument(
                "relay request stream ended before a block subscription was registered",
            )
        })?;
        let validated = validate_block_subscription(&initial)?;
        // Register against the ring before returning response headers. A publication that occurs
        // after the client observes a successful Subscribe response is therefore always visible.
        let subscription = self.open_subscription(validated.filter_labels, validated.from_slot)?;
        let connection = RelayConnection {
            inbound,
            inbound_open: true,
            subscription,
            terminated: false,
        };
        Ok(Response::new(Box::pin(stream::unfold(
            connection,
            RelayConnection::next,
        ))))
    }

    type SubscribeDeshredStream = SubscribeDeshredStream;

    async fn subscribe_deshred(
        &self,
        request: Request<yellowstone_grpc_proto::tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        self.authenticate(request.metadata())?;
        Err(Status::unimplemented(
            "block relay does not implement SubscribeDeshred",
        ))
    }

    async fn subscribe_replay_info(
        &self,
        request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        self.authenticate(request.metadata())?;
        let first_available = self
            .status_state()?
            .ring
            .iter()
            .map(|record| record.slot)
            .min();
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available,
        }))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        self.authenticate(request.metadata())?;
        Ok(Response::new(PongResponse {
            count: request.into_inner().count,
        }))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        self.authenticate(request.metadata())?;
        Err(Status::unimplemented(
            "block relay does not implement GetLatestBlockhash",
        ))
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        self.authenticate(request.metadata())?;
        Err(Status::unimplemented(
            "block relay does not implement GetBlockHeight",
        ))
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        self.authenticate(request.metadata())?;
        Err(Status::unimplemented(
            "block relay does not implement GetSlot",
        ))
    }

    async fn is_blockhash_valid(
        &self,
        request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        self.authenticate(request.metadata())?;
        Err(Status::unimplemented(
            "block relay does not implement IsBlockhashValid",
        ))
    }

    async fn get_version(
        &self,
        request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        self.authenticate(request.metadata())?;
        Err(Status::unimplemented(
            "block relay does not implement GetVersion",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use prost::Message;
    use tokio::time::timeout;
    use yellowstone_grpc_proto::prelude::{
        SubscribeRequestFilterBlocks, SubscribeRequestFilterTransactions, SubscribeUpdateBlock,
    };

    use super::*;

    const TOKEN: &str = "test-relay-token";

    fn relay(max_records: usize, max_encoded_bytes: usize) -> YellowstoneBlockRelay {
        YellowstoneBlockRelay::new(
            YellowstoneRelayAuth::from_shared_x_token(TOKEN).unwrap(),
            YellowstoneBlockRelayLimits {
                max_records,
                max_encoded_bytes,
                max_clients: 8,
            },
        )
        .unwrap()
    }

    fn block_update(slot: u64, padding: usize) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["upstream-label-must-not-leak".to_string()],
            update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
                slot,
                blockhash: "x".repeat(padding.max(1)),
                parent_slot: slot.saturating_sub(1),
                ..Default::default()
            })),
            created_at: None,
        }
    }

    fn valid_request(label: &str, from_slot: Option<u64>) -> SubscribeRequest {
        SubscribeRequest {
            blocks: HashMap::from([(
                label.to_string(),
                SubscribeRequestFilterBlocks {
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(true),
                    ..Default::default()
                },
            )]),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            from_slot,
            ..Default::default()
        }
    }

    fn subscribe(
        relay: &YellowstoneBlockRelay,
        label: &str,
        from_slot: Option<u64>,
    ) -> RelaySubscription {
        let validated = validate_block_subscription(&valid_request(label, from_slot)).unwrap();
        relay
            .open_subscription(validated.filter_labels, validated.from_slot)
            .unwrap()
    }

    fn update_slot(update: &SubscribeUpdate) -> u64 {
        match update.update_oneof.as_ref() {
            Some(UpdateOneof::Block(block)) => block.slot,
            other => panic!("expected block update, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn one_fsynced_publication_fans_out_once_with_client_filter_labels() {
        let relay = relay(8, 1024 * 1024);
        let mut first = subscribe(&relay, "first-client", None);
        let mut second = subscribe(&relay, "second-client", None);

        relay.publish_fsynced(1, block_update(100, 8)).unwrap();

        let first_update = first.recv().await.unwrap();
        let second_update = second.recv().await.unwrap();
        assert_eq!(update_slot(&first_update), 100);
        assert_eq!(update_slot(&second_update), 100);
        assert_eq!(first_update.filters, ["first-client"]);
        assert_eq!(second_update.filters, ["second-client"]);
        assert!(
            timeout(Duration::from_millis(20), first.recv())
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(20), second.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn first_durable_sequence_zero_wakes_an_existing_live_subscription() {
        let relay = relay(8, 1024 * 1024);
        let mut subscription = subscribe(&relay, "sequence-zero", None);

        relay.publish_fsynced(0, block_update(99, 8)).unwrap();
        let update = timeout(Duration::from_secs(1), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update_slot(&update), 99);
        assert_eq!(update.filters, ["sequence-zero"]);
    }

    #[tokio::test]
    async fn close_wakes_clients_and_rejects_new_publications_and_subscriptions() {
        let relay = relay(8, 1024 * 1024);
        let mut subscription = subscribe(&relay, "closing", None);

        relay.close().unwrap();
        let status = timeout(Duration::from_secs(1), subscription.recv())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            status.code(),
            yellowstone_grpc_proto::tonic::Code::Unavailable
        );
        assert!(matches!(
            relay.publish_fsynced(0, block_update(1, 8)),
            Err(RelayPublishError::Closed)
        ));
        assert_eq!(
            relay
                .open_subscription(vec!["new".to_string()], None)
                .unwrap_err()
                .code(),
            yellowstone_grpc_proto::tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn from_slot_replays_snapshot_then_continues_live_without_duplication() {
        let relay = relay(8, 1024 * 1024);
        for (sequence, slot) in [(1, 100), (2, 101), (3, 102)] {
            relay
                .publish_fsynced(sequence, block_update(slot, 8))
                .unwrap();
        }
        let mut subscription = subscribe(&relay, "replay", Some(101));

        assert_eq!(update_slot(&subscription.recv().await.unwrap()), 101);
        relay.publish_fsynced(4, block_update(103, 8)).unwrap();
        assert_eq!(update_slot(&subscription.recv().await.unwrap()), 102);
        assert_eq!(update_slot(&subscription.recv().await.unwrap()), 103);
        assert!(
            timeout(Duration::from_millis(20), subscription.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn durable_sequence_preserves_duplicate_and_lower_slot_observations() {
        let relay = relay(8, 1024 * 1024);
        let mut fork = block_update(100, 8);
        match fork.update_oneof.as_mut() {
            Some(UpdateOneof::Block(block)) => block.blockhash = "fork-b".to_string(),
            _ => unreachable!(),
        }
        relay.publish_fsynced(10, block_update(100, 8)).unwrap();
        relay.publish_fsynced(11, fork).unwrap();
        relay.publish_fsynced(12, block_update(99, 8)).unwrap();

        let mut subscription = subscribe(&relay, "forks", Some(99));
        let observed = [
            update_slot(&subscription.recv().await.unwrap()),
            update_slot(&subscription.recv().await.unwrap()),
            update_slot(&subscription.recv().await.unwrap()),
        ];
        assert_eq!(observed, [100, 100, 99]);
        let stats = relay.stats().unwrap();
        assert_eq!(stats.first_available_slot, Some(99));
        assert_eq!(stats.last_durable_slot, Some(99));
        assert!(matches!(
            relay.publish_fsynced(12, block_update(101, 8)),
            Err(RelayPublishError::NonMonotonicSequence { .. })
        ));
    }

    #[tokio::test]
    async fn from_slot_outside_retained_ring_is_explicitly_rejected() {
        let relay = relay(2, 1024 * 1024);
        for (sequence, slot) in [(1, 100), (2, 101), (3, 102)] {
            relay
                .publish_fsynced(sequence, block_update(slot, 8))
                .unwrap();
        }

        for requested in [100, 103] {
            let error = relay
                .open_subscription(vec!["client".to_string()], Some(requested))
                .unwrap_err();
            assert_eq!(
                error.code(),
                yellowstone_grpc_proto::tonic::Code::OutOfRange
            );
            assert!(error.message().contains("first_available_slot=101"));
            assert!(error.message().contains("last_durable_slot=102"));
        }
    }

    #[tokio::test]
    async fn lagged_client_gets_retryable_status_with_durable_and_delivered_slots() {
        let relay = relay(2, 1024 * 1024);
        relay.publish_fsynced(1, block_update(100, 8)).unwrap();
        let mut subscription = subscribe(&relay, "slow", Some(100));
        assert_eq!(update_slot(&subscription.recv().await.unwrap()), 100);

        for (sequence, slot) in [(2, 101), (3, 102), (4, 103)] {
            relay
                .publish_fsynced(sequence, block_update(slot, 8))
                .unwrap();
        }
        let status = subscription.recv().await.unwrap_err();
        assert_eq!(
            status.code(),
            yellowstone_grpc_proto::tonic::Code::Unavailable
        );
        assert!(status.message().contains("retryable=true"));
        assert!(status.message().contains("last_durable_slot=103"));
        assert!(status.message().contains("last_delivered_slot=100"));
    }

    #[tokio::test]
    async fn authentication_filters_and_ping_are_strict() {
        let relay = relay(8, 1024 * 1024);
        let mut invalid_filter = valid_request("bad", None);
        invalid_filter.transactions.insert(
            "transaction".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        assert_eq!(
            validate_block_subscription(&invalid_filter)
                .unwrap_err()
                .code(),
            yellowstone_grpc_proto::tonic::Code::InvalidArgument
        );
        let mut accounts = valid_request("accounts", None);
        accounts
            .blocks
            .get_mut("accounts")
            .unwrap()
            .include_accounts = Some(true);
        assert!(validate_block_subscription(&accounts).is_err());

        let unauthenticated = Geyser::ping(&relay, Request::new(PingRequest { count: 7 }))
            .await
            .unwrap_err();
        assert_eq!(
            unauthenticated.code(),
            yellowstone_grpc_proto::tonic::Code::Unauthenticated
        );
        let mut wrong_token = Request::new(PingRequest { count: 7 });
        wrong_token.metadata_mut().insert(
            YELLOWSTONE_X_TOKEN_HEADER,
            "wrong-relay-token".parse().unwrap(),
        );
        assert_eq!(
            Geyser::ping(&relay, wrong_token).await.unwrap_err().code(),
            yellowstone_grpc_proto::tonic::Code::Unauthenticated
        );
        let mut authenticated = Request::new(PingRequest { count: 7 });
        authenticated
            .metadata_mut()
            .insert(YELLOWSTONE_X_TOKEN_HEADER, TOKEN.parse().unwrap());
        assert_eq!(
            Geyser::ping(&relay, authenticated)
                .await
                .unwrap()
                .into_inner()
                .count,
            7
        );
        assert_eq!(
            ping_only(&SubscribeRequest {
                ping: Some(yellowstone_grpc_proto::prelude::SubscribeRequestPing { id: 9 }),
                ..Default::default()
            }),
            Some(9)
        );
    }

    #[test]
    fn record_and_encoded_byte_limits_evict_oldest_and_reject_oversize() {
        let sample = block_update(1, 256);
        let per_record = (1..=3)
            .map(|slot| {
                let mut normalized = block_update(slot, 256);
                normalized.filters.clear();
                normalized.encoded_len()
            })
            .max()
            .unwrap();
        let byte_limited = relay(10, per_record * 2);
        let first = byte_limited.publish_fsynced(1, sample).unwrap();
        assert_eq!(first.retained_records, 1);
        byte_limited
            .publish_fsynced(2, block_update(2, 256))
            .unwrap();
        let third = byte_limited
            .publish_fsynced(3, block_update(3, 256))
            .unwrap();
        assert_eq!(third.evicted_records, 1);
        assert_eq!(third.retained_records, 2);
        assert!(third.retained_encoded_bytes <= per_record * 2);

        let record_limited = relay(2, 1024 * 1024);
        for slot in 1..=3 {
            let report = record_limited
                .publish_fsynced(slot, block_update(slot, 8))
                .unwrap();
            assert!(report.retained_records <= 2);
        }
        assert_eq!(
            record_limited.stats().unwrap().first_available_slot,
            Some(2)
        );

        let tiny = relay(2, 8);
        assert!(matches!(
            tiny.publish_fsynced(1, block_update(1, 256)),
            Err(RelayPublishError::EncodedRecordTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn unsupported_unary_is_authenticated_then_unimplemented() {
        let relay = relay(8, 1024 * 1024);
        let mut request = Request::new(GetSlotRequest { commitment: None });
        request
            .metadata_mut()
            .insert(YELLOWSTONE_X_TOKEN_HEADER, TOKEN.parse().unwrap());
        let status = Geyser::get_slot(&relay, request).await.unwrap_err();
        assert_eq!(
            status.code(),
            yellowstone_grpc_proto::tonic::Code::Unimplemented
        );
    }

    #[tokio::test]
    async fn generated_geyser_client_receives_publication_after_subscribe_response() {
        use tokio::sync::oneshot;
        use yellowstone_grpc_proto::prelude::geyser_client::GeyserClient;
        use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

        let relay = relay(8, 1024 * 1024);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = stream::unfold(listener, |listener| async move {
            let item = listener.accept().await.map(|(socket, _peer)| socket);
            Some((item, listener))
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let service = relay
            .clone()
            .into_geyser_service()
            .send_compressed(CompressionEncoding::Zstd);
        let server = tokio::spawn(async move {
            yellowstone_grpc_proto::tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let mut client = GeyserClient::connect(format!("http://{address}"))
            .await
            .unwrap()
            .accept_compressed(CompressionEncoding::Zstd);
        let (request_tx, request_rx) = futures::channel::mpsc::unbounded();
        request_tx
            .unbounded_send(valid_request("wire-client", None))
            .unwrap();
        let mut request = Request::new(request_rx);
        request
            .metadata_mut()
            .insert(YELLOWSTONE_X_TOKEN_HEADER, TOKEN.parse().unwrap());
        let mut updates = client.subscribe(request).await.unwrap().into_inner();

        let mut second_client = GeyserClient::connect(format!("http://{address}"))
            .await
            .unwrap()
            .accept_compressed(CompressionEncoding::Zstd);
        let (second_request_tx, second_request_rx) = futures::channel::mpsc::unbounded();
        second_request_tx
            .unbounded_send(valid_request("wire-client-2", None))
            .unwrap();
        let mut second_request = Request::new(second_request_rx);
        second_request
            .metadata_mut()
            .insert(YELLOWSTONE_X_TOKEN_HEADER, TOKEN.parse().unwrap());
        let mut second_updates = second_client
            .subscribe(second_request)
            .await
            .unwrap()
            .into_inner();

        request_tx
            .unbounded_send(SubscribeRequest {
                ping: Some(yellowstone_grpc_proto::prelude::SubscribeRequestPing { id: 41 }),
                ..Default::default()
            })
            .unwrap();
        let pong = timeout(Duration::from_secs(1), updates.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            pong.update_oneof,
            Some(UpdateOneof::Pong(SubscribeUpdatePong { id: 41 }))
        ));

        // A successful Subscribe response is the registration barrier: this publication cannot
        // fall into a response-header/request-body race, even though the request stream ends.
        drop(request_tx);
        drop(second_request_tx);
        relay.publish_fsynced(1, block_update(500, 8)).unwrap();
        let update = timeout(Duration::from_secs(1), updates.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(update_slot(&update), 500);
        assert_eq!(update.filters, ["wire-client"]);
        let second_update = timeout(Duration::from_secs(1), second_updates.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(update_slot(&second_update), 500);
        assert_eq!(second_update.filters, ["wire-client-2"]);
        assert!(
            timeout(Duration::from_millis(20), updates.message())
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(20), second_updates.message())
                .await
                .is_err()
        );

        let mut deshred = Request::new(stream::iter([SubscribeDeshredRequest::default()]));
        deshred
            .metadata_mut()
            .insert(YELLOWSTONE_X_TOKEN_HEADER, TOKEN.parse().unwrap());
        let status = client.subscribe_deshred(deshred).await.unwrap_err();
        assert_eq!(
            status.code(),
            yellowstone_grpc_proto::tonic::Code::Unimplemented
        );

        drop(updates);
        drop(second_updates);
        drop(client);
        drop(second_client);
        let _ = shutdown_tx.send(());
        timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}
