//! Bounded live transport for recent-slot shred repair.
//!
//! The runtime owns one long-lived UDP socket, correlates every response to an outstanding nonce
//! and request, rotates peers on bounded retries, answers signed repair ping challenges, and only
//! returns a shred after it passes all trust checks and is durably appended to the isolated repair
//! WAL. It never opens or mutates the raw-shred WAL or replication ACK state.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, ErrorKind},
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};

use solana_hash::Hash;
use solana_keypair::{Keypair, signable::Signable};
use solana_ledger::shred::Shred;
use solana_pubkey::Pubkey;
use tokio::net::UdpSocket;
use tracing::debug;

use crate::{
    repair_tracker::RepairRequest,
    repair_wal::{RepairProvenance, RepairWal},
    repair_wire::{
        RepairNonce, ShredRepairRequest, decode_matching_shred_response, encode_pong,
        encode_shred_repair_request, parse_verified_ping, split_shred_response,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairPeer {
    pub pubkey: Pubkey,
    pub repair_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainedMerkleRootExpectation {
    /// Original Turbine evidence, or an already promoted repair, established the exact field.
    Exact(Option<Hash>),
    /// A directly/derivatively trusted successor committed this FEC's Merkle root. The first
    /// persisted repair may therefore teach its own chained root, which is itself covered by that
    /// anchored Merkle root and the scheduled leader's signature.
    LearnFromAnchoredRoot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustedFecIdentity {
    pub merkle_root: Hash,
    pub chained_merkle_root: ChainedMerkleRootExpectation,
    /// The original or promoted successor FEC whose chain selected this exact root.
    pub trust_anchor_fec_set_index: u32,
}

/// Trust data required to admit a repair response.
///
/// There is deliberately no permissive implementation. Missing version, leader, or FEC identity
/// returns `None` and the runtime rejects the packet. The address/pubkey and request hooks allow an
/// integration to additionally bind gossip freshness, fork choice, and its current repair plan.
pub trait RepairTrust: Send + Sync + 'static {
    fn peer_is_authorized(&self, peer: &RepairPeer, source: SocketAddr) -> bool;

    fn request_response_is_authorized(
        &self,
        peer: &RepairPeer,
        request: ShredRepairRequest,
        shred: &Shred,
    ) -> bool;

    fn expected_shred_version(&self, slot: u64) -> Option<u16>;

    fn expected_slot_leader(&self, slot: u64) -> Option<Pubkey>;

    fn expected_fec_identity(&self, slot: u64, fec_set_index: u32) -> Option<TrustedFecIdentity>;

    /// Called after every wire, version, leader-signature, and Merkle-root check, immediately
    /// before the synchronous WAL append. Stateful trust stores atomically reserve an exact
    /// `(root, chained-root)` identity here so a neighbor conflict is rejected before persistence.
    /// No await or packet processing occurs between this reservation and the append; an append
    /// error terminates the isolated repair task before the reservation can authorize more work.
    fn reserve_repair_commitment(&self, _shred: &Shred) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairRuntimeConfig {
    pub max_peers: usize,
    pub max_outstanding: usize,
    pub max_requests_considered_per_tick: usize,
    pub max_new_requests_per_tick: usize,
    pub max_packets_per_tick: usize,
    pub max_packet_bytes: usize,
    pub max_suppressed_requests: usize,
    pub request_timeout: Duration,
    /// Cooldown after a request consumes its full retry budget. This prevents a still-missing
    /// tracker item from immediately creating a fresh nominally "bounded" retry cycle.
    pub exhaustion_cooldown: Duration,
    /// Number of retries after the initial request. Zero means one attempt total.
    pub max_retries: u8,
    pub initial_nonce: RepairNonce,
}

impl RepairRuntimeConfig {
    fn validate(self) -> io::Result<Self> {
        if self.max_peers == 0
            || self.max_outstanding == 0
            || self.max_requests_considered_per_tick == 0
            || self.max_new_requests_per_tick == 0
            || self.max_packets_per_tick == 0
            || self.max_packet_bytes == 0
            || self.max_suppressed_requests == 0
            || self.request_timeout.is_zero()
            || self.exhaustion_cooldown.is_zero()
        {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "repair runtime bounds and request timeout must be nonzero",
            ));
        }
        if self.max_packet_bytes < 1_280 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "repair max_packet_bytes must be at least 1280",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RepairRuntimeStats {
    pub requests_sent: u64,
    pub retries_sent: u64,
    pub requests_deduplicated: u64,
    pub requests_capacity_deferred: u64,
    pub requests_cooldown_deferred: u64,
    pub requests_exhausted: u64,
    pub packets_received: u64,
    pub packets_rejected: u64,
    pub pings_answered: u64,
    pub shreds_accepted: u64,
    /// Accepted shreds whose FEC root came from a trusted successor and whose chained root was
    /// learned only after the frame was durably written. This is the path that unlocks repair of
    /// a wholly absent 32+32 FEC set, walking strictly backwards from Turbine evidence.
    pub root_anchored_shreds_accepted: u64,
    pub repair_wal_bytes: u64,
    pub repair_wal_max_bytes: u64,
    pub repair_wal_syncs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairRejectReason {
    PacketAtReceiveLimit,
    UnknownPeer,
    UnauthorizedPeer,
    PingPubkeyMismatch,
    MalformedResponse(String),
    UnknownNonce {
        nonce: RepairNonce,
    },
    ResponseSourceMismatch {
        expected: SocketAddr,
        actual: SocketAddr,
    },
    RequestIdentity,
    MissingTrustedShredVersion,
    ShredVersionMismatch {
        expected: u16,
        actual: u16,
    },
    MissingTrustedSlotLeader,
    InvalidSlotLeaderSignature,
    MissingTrustedFecIdentity,
    FecMerkleRootMismatch,
    MissingChainedMerkleRoot,
    ChainedMerkleRootMismatch,
    RepairTrustChainConflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairRuntimeEvent {
    Retried {
        request: ShredRepairRequest,
        retry: u8,
        peer: RepairPeer,
    },
    Exhausted {
        request: ShredRepairRequest,
        attempts: u16,
    },
    PongSent {
        peer: RepairPeer,
    },
    Rejected {
        source: SocketAddr,
        reason: RepairRejectReason,
    },
}

#[derive(Debug)]
pub struct AcceptedRepair {
    pub shred: Shred,
    pub provenance: RepairProvenance,
    pub repair_wal_sequence: u64,
}

#[derive(Debug, Default)]
pub struct RepairPoll {
    pub accepted: Vec<AcceptedRepair>,
    pub events: Vec<RepairRuntimeEvent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutstandingRepair {
    pub nonce: RepairNonce,
    pub request: ShredRepairRequest,
    pub peer_index: usize,
    pub retries_used: u8,
    pub sent_at: Instant,
    pub deadline: Instant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RequestKey {
    Shred(u64, u64),
    HighestShred(u64, u64),
    Orphan(u64),
}

impl From<ShredRepairRequest> for RequestKey {
    fn from(request: ShredRepairRequest) -> Self {
        match request {
            ShredRepairRequest::Shred { slot, shred_index } => Self::Shred(slot, shred_index),
            ShredRepairRequest::HighestShred { slot, shred_index } => {
                Self::HighestShred(slot, shred_index)
            }
            ShredRepairRequest::Orphan { slot } => Self::Orphan(slot),
        }
    }
}

pub struct RepairRuntime<T: RepairTrust> {
    socket: UdpSocket,
    identity: Arc<Keypair>,
    trust: T,
    peers: Vec<RepairPeer>,
    config: RepairRuntimeConfig,
    repair_wal: RepairWal,
    outstanding: BTreeMap<RepairNonce, OutstandingRepair>,
    request_nonces: BTreeMap<RequestKey, RepairNonce>,
    suppressed_until: BTreeMap<RequestKey, Instant>,
    next_nonce: RepairNonce,
    next_peer: usize,
    recv_buffer: Vec<u8>,
    stats: RepairRuntimeStats,
}

impl<T: RepairTrust> RepairRuntime<T> {
    pub async fn bind<A: ToSocketAddrs>(
        bind_addr: A,
        identity: Arc<Keypair>,
        trust: T,
        peers: Vec<RepairPeer>,
        config: RepairRuntimeConfig,
        repair_wal: RepairWal,
    ) -> io::Result<Self> {
        let mut addresses = bind_addr.to_socket_addrs()?;
        let address = addresses.next().ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                "repair bind address did not resolve",
            )
        })?;
        let socket = UdpSocket::bind(address).await?;
        Self::from_socket(socket, identity, trust, peers, config, repair_wal)
    }

    pub fn from_socket(
        socket: UdpSocket,
        identity: Arc<Keypair>,
        trust: T,
        peers: Vec<RepairPeer>,
        config: RepairRuntimeConfig,
        repair_wal: RepairWal,
    ) -> io::Result<Self> {
        let config = config.validate()?;
        if peers.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "repair runtime requires at least one peer",
            ));
        }
        if peers.len() > config.max_peers {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "repair peer count {} exceeds configured maximum {}",
                    peers.len(),
                    config.max_peers
                ),
            ));
        }
        let unique_pubkeys: BTreeSet<_> = peers.iter().map(|peer| peer.pubkey).collect();
        let unique_addresses: BTreeSet<_> = peers.iter().map(|peer| peer.repair_addr).collect();
        if unique_pubkeys.len() != peers.len() || unique_addresses.len() != peers.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "repair peers must have unique pubkeys and unique repair addresses",
            ));
        }

        let stats = RepairRuntimeStats {
            repair_wal_bytes: repair_wal.file_len(),
            repair_wal_max_bytes: repair_wal.max_file_bytes(),
            ..RepairRuntimeStats::default()
        };
        Ok(Self {
            socket,
            identity,
            trust,
            peers,
            config,
            repair_wal,
            outstanding: BTreeMap::new(),
            request_nonces: BTreeMap::new(),
            suppressed_until: BTreeMap::new(),
            next_nonce: config.initial_nonce,
            next_peer: 0,
            recv_buffer: vec![0; config.max_packet_bytes],
            stats,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn stats(&self) -> RepairRuntimeStats {
        self.stats
    }

    pub fn outstanding_count(&self) -> usize {
        self.outstanding.len()
    }

    /// Startup snapshot used for deterministic peer rotation. Live gossip refresh can construct a
    /// replacement runtime in a future integration; this implementation intentionally does not
    /// mutate peer identity/address bindings while requests are outstanding.
    pub fn peers(&self) -> &[RepairPeer] {
        &self.peers
    }

    pub fn outstanding_repairs(&self) -> Vec<OutstandingRepair> {
        self.outstanding.values().copied().collect()
    }

    pub fn repair_wal(&self) -> &RepairWal {
        &self.repair_wal
    }

    /// Drives retries, admits newly due tracker work, and drains at most the configured number of
    /// currently queued UDP packets. The caller controls cadence; this method never sleeps.
    pub async fn service_tracker_requests<I>(
        &mut self,
        requests: I,
        now: Instant,
        unix_ms: u64,
    ) -> io::Result<RepairPoll>
    where
        I: IntoIterator<Item = RepairRequest>,
    {
        self.service_requests(
            requests.into_iter().map(tracker_request_to_wire),
            now,
            unix_ms,
        )
        .await
    }

    pub async fn service_requests<I>(
        &mut self,
        requests: I,
        now: Instant,
        unix_ms: u64,
    ) -> io::Result<RepairPoll>
    where
        I: IntoIterator<Item = ShredRepairRequest>,
    {
        let mut poll = RepairPoll::default();
        self.suppressed_until.retain(|_, until| *until > now);
        // Drain already-queued responses before evaluating deadlines so a packet that arrived at
        // the boundary cannot spuriously rotate its request to another peer.
        self.drain_packets(now, unix_ms, &mut poll).await?;
        let completed_this_tick: BTreeSet<_> = poll
            .accepted
            .iter()
            .map(|accepted| RequestKey::from(accepted.provenance.request))
            .collect();
        self.retry_expired(now, unix_ms, &mut poll).await?;
        self.enqueue_new(requests, &completed_this_tick, now, unix_ms)
            .await?;
        if self.repair_wal.sync_if_due(now)? {
            self.stats.repair_wal_syncs = self.stats.repair_wal_syncs.saturating_add(1);
        }
        Ok(poll)
    }

    pub fn flush_repair_wal(&mut self, now: Instant) -> io::Result<()> {
        self.repair_wal.flush_and_sync(now)
    }

    async fn retry_expired(
        &mut self,
        now: Instant,
        unix_ms: u64,
        poll: &mut RepairPoll,
    ) -> io::Result<()> {
        let expired: Vec<_> = self
            .outstanding
            .iter()
            .filter(|(_, attempt)| now >= attempt.deadline)
            .map(|(&nonce, _)| nonce)
            .collect();
        for nonce in expired {
            let Some(attempt) = self.remove_outstanding(nonce) else {
                continue;
            };
            if attempt.retries_used >= self.config.max_retries {
                self.stats.requests_exhausted = self.stats.requests_exhausted.saturating_add(1);
                poll.events.push(RepairRuntimeEvent::Exhausted {
                    request: attempt.request,
                    attempts: u16::from(attempt.retries_used) + 1,
                });
                self.suppress_exhausted(attempt.request, now)?;
                debug!(
                    request = ?attempt.request,
                    attempts = u16::from(attempt.retries_used) + 1,
                    "repair request exhausted its bounded retry budget"
                );
                continue;
            }

            let retry = attempt.retries_used + 1;
            let first_peer = (attempt.peer_index + 1) % self.peers.len();
            let sent = self
                .send_attempt(attempt.request, first_peer, retry, now, unix_ms)
                .await?;
            self.stats.retries_sent = self.stats.retries_sent.saturating_add(1);
            poll.events.push(RepairRuntimeEvent::Retried {
                request: attempt.request,
                retry,
                peer: self.peers[sent.peer_index].clone(),
            });
        }
        Ok(())
    }

    async fn enqueue_new<I>(
        &mut self,
        requests: I,
        completed_this_tick: &BTreeSet<RequestKey>,
        now: Instant,
        unix_ms: u64,
    ) -> io::Result<()>
    where
        I: IntoIterator<Item = ShredRepairRequest>,
    {
        let mut started = 0usize;
        for (considered, request) in requests.into_iter().enumerate() {
            if considered >= self.config.max_requests_considered_per_tick {
                self.stats.requests_capacity_deferred =
                    self.stats.requests_capacity_deferred.saturating_add(1);
                break;
            }
            let key = RequestKey::from(request);
            if completed_this_tick.contains(&key) || self.request_nonces.contains_key(&key) {
                self.stats.requests_deduplicated =
                    self.stats.requests_deduplicated.saturating_add(1);
                continue;
            }
            if self
                .suppressed_until
                .get(&key)
                .is_some_and(|until| *until > now)
            {
                self.stats.requests_cooldown_deferred =
                    self.stats.requests_cooldown_deferred.saturating_add(1);
                continue;
            }
            if started >= self.config.max_new_requests_per_tick {
                self.stats.requests_capacity_deferred =
                    self.stats.requests_capacity_deferred.saturating_add(1);
                continue;
            }
            if self.outstanding.len() >= self.config.max_outstanding {
                self.stats.requests_capacity_deferred =
                    self.stats.requests_capacity_deferred.saturating_add(1);
                continue;
            }
            let first_peer = self.next_peer;
            let sent = self
                .send_attempt(request, first_peer, 0, now, unix_ms)
                .await?;
            self.next_peer = (sent.peer_index + 1) % self.peers.len();
            started += 1;
        }
        Ok(())
    }

    fn suppress_exhausted(&mut self, request: ShredRepairRequest, now: Instant) -> io::Result<()> {
        let key = RequestKey::from(request);
        if !self.suppressed_until.contains_key(&key)
            && self.suppressed_until.len() >= self.config.max_suppressed_requests
        {
            return Err(io::Error::new(
                ErrorKind::OutOfMemory,
                "repair exhausted-request suppression capacity reached",
            ));
        }
        let until = now
            .checked_add(self.config.exhaustion_cooldown)
            .ok_or_else(|| {
                io::Error::new(ErrorKind::InvalidInput, "repair cooldown deadline overflow")
            })?;
        self.suppressed_until.insert(key, until);
        Ok(())
    }

    async fn send_attempt(
        &mut self,
        request: ShredRepairRequest,
        first_peer: usize,
        retries_used: u8,
        now: Instant,
        unix_ms: u64,
    ) -> io::Result<OutstandingRepair> {
        let peer_index = (0..self.peers.len())
            .map(|offset| (first_peer + offset) % self.peers.len())
            .find(|&index| {
                let peer = &self.peers[index];
                self.trust.peer_is_authorized(peer, peer.repair_addr)
            })
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::PermissionDenied,
                    "no configured repair peer passed the address/pubkey trust hook",
                )
            })?;
        self.send_attempt_to_peer(request, peer_index, retries_used, now, unix_ms)
            .await
    }

    async fn send_attempt_to_peer(
        &mut self,
        request: ShredRepairRequest,
        peer_index: usize,
        retries_used: u8,
        now: Instant,
        unix_ms: u64,
    ) -> io::Result<OutstandingRepair> {
        let peer = self.peers[peer_index].clone();
        if !self.trust.peer_is_authorized(&peer, peer.repair_addr) {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "selected repair peer no longer passes the address/pubkey trust hook",
            ));
        }
        let nonce = self.allocate_nonce()?;
        let packet = encode_shred_repair_request(
            request,
            self.identity.as_ref(),
            peer.pubkey,
            unix_ms,
            nonce,
        )
        .map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
        self.socket.send_to(&packet, peer.repair_addr).await?;
        let deadline = now
            .checked_add(self.config.request_timeout)
            .ok_or_else(|| {
                io::Error::new(ErrorKind::InvalidInput, "repair request deadline overflow")
            })?;
        let attempt = OutstandingRepair {
            nonce,
            request,
            peer_index,
            retries_used,
            sent_at: now,
            deadline,
        };
        self.outstanding.insert(nonce, attempt);
        let replaced = self.request_nonces.insert(RequestKey::from(request), nonce);
        debug_assert!(replaced.is_none());
        self.stats.requests_sent = self.stats.requests_sent.saturating_add(1);
        debug!(
            ?request,
            nonce,
            retry = retries_used,
            peer = %peer.repair_addr,
            "sent bounded shred repair request"
        );
        Ok(attempt)
    }

    async fn drain_packets(
        &mut self,
        now: Instant,
        unix_ms: u64,
        poll: &mut RepairPoll,
    ) -> io::Result<()> {
        for _ in 0..self.config.max_packets_per_tick {
            let (len, source) = match self.socket.try_recv_from(&mut self.recv_buffer) {
                Ok(received) => received,
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            };
            self.stats.packets_received = self.stats.packets_received.saturating_add(1);
            if len == self.recv_buffer.len() {
                self.reject(source, RepairRejectReason::PacketAtReceiveLimit, poll);
                continue;
            }
            let packet = self.recv_buffer[..len].to_vec();
            self.process_packet(&packet, source, now, unix_ms, poll)
                .await?;
        }
        Ok(())
    }

    async fn process_packet(
        &mut self,
        packet: &[u8],
        source: SocketAddr,
        now: Instant,
        unix_ms: u64,
        poll: &mut RepairPoll,
    ) -> io::Result<()> {
        if let Some(ping) = parse_verified_ping(packet) {
            let Some((peer_index, peer)) = self
                .peers
                .iter()
                .enumerate()
                .find(|(_, peer)| peer.repair_addr == source)
            else {
                self.reject(source, RepairRejectReason::UnknownPeer, poll);
                return Ok(());
            };
            if !self.trust.peer_is_authorized(peer, source) {
                self.reject(source, RepairRejectReason::UnauthorizedPeer, poll);
                return Ok(());
            }
            if ping.pubkey() != peer.pubkey {
                self.reject(source, RepairRejectReason::PingPubkeyMismatch, poll);
                return Ok(());
            }
            let peer = peer.clone();
            let pong = encode_pong(&ping, self.identity.as_ref())
                .map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
            self.socket.send_to(&pong, source).await?;
            self.stats.pings_answered = self.stats.pings_answered.saturating_add(1);
            poll.events
                .push(RepairRuntimeEvent::PongSent { peer: peer.clone() });

            // Agave challenges an unverified return address instead of serving the triggering
            // request. Send the pong first, then immediately reissue bounded outstanding work to
            // that same peer from the same socket. Waiting for the ordinary timeout and rotating
            // could otherwise challenge a second peer without ever making a post-pong request.
            let challenged: Vec<_> = self
                .outstanding
                .iter()
                .filter(|(_, attempt)| attempt.peer_index == peer_index)
                .map(|(&nonce, _)| nonce)
                .collect();
            for nonce in challenged {
                let Some(attempt) = self.remove_outstanding(nonce) else {
                    continue;
                };
                if attempt.retries_used >= self.config.max_retries {
                    self.stats.requests_exhausted = self.stats.requests_exhausted.saturating_add(1);
                    poll.events.push(RepairRuntimeEvent::Exhausted {
                        request: attempt.request,
                        attempts: u16::from(attempt.retries_used) + 1,
                    });
                    self.suppress_exhausted(attempt.request, now)?;
                    continue;
                }
                let retry = attempt.retries_used + 1;
                let sent = self
                    .send_attempt_to_peer(attempt.request, peer_index, retry, now, unix_ms)
                    .await?;
                self.stats.retries_sent = self.stats.retries_sent.saturating_add(1);
                poll.events.push(RepairRuntimeEvent::Retried {
                    request: attempt.request,
                    retry,
                    peer: self.peers[sent.peer_index].clone(),
                });
            }
            return Ok(());
        }

        let response = match split_shred_response(packet) {
            Ok(response) => response,
            Err(error) => {
                self.reject(
                    source,
                    RepairRejectReason::MalformedResponse(error.to_string()),
                    poll,
                );
                return Ok(());
            }
        };
        let Some(attempt) = self.outstanding.get(&response.nonce).copied() else {
            self.reject(
                source,
                RepairRejectReason::UnknownNonce {
                    nonce: response.nonce,
                },
                poll,
            );
            return Ok(());
        };
        let peer = self.peers[attempt.peer_index].clone();
        if source != peer.repair_addr {
            self.reject(
                source,
                RepairRejectReason::ResponseSourceMismatch {
                    expected: peer.repair_addr,
                    actual: source,
                },
                poll,
            );
            return Ok(());
        }
        if !self.trust.peer_is_authorized(&peer, source) {
            self.reject(source, RepairRejectReason::UnauthorizedPeer, poll);
            return Ok(());
        }

        let shred = match decode_matching_shred_response(packet, response.nonce, attempt.request) {
            Ok(shred) => shred,
            Err(error) => {
                self.reject(
                    source,
                    RepairRejectReason::MalformedResponse(error.to_string()),
                    poll,
                );
                return Ok(());
            }
        };
        if let Err(error) = shred.sanitize() {
            self.reject(
                source,
                RepairRejectReason::MalformedResponse(error.to_string()),
                poll,
            );
            return Ok(());
        }
        if !self
            .trust
            .request_response_is_authorized(&peer, attempt.request, &shred)
        {
            self.reject(source, RepairRejectReason::RequestIdentity, poll);
            return Ok(());
        }

        let Some(expected_version) = self.trust.expected_shred_version(shred.slot()) else {
            self.reject(source, RepairRejectReason::MissingTrustedShredVersion, poll);
            return Ok(());
        };
        if shred.version() != expected_version {
            self.reject(
                source,
                RepairRejectReason::ShredVersionMismatch {
                    expected: expected_version,
                    actual: shred.version(),
                },
                poll,
            );
            return Ok(());
        }

        let Some(expected_leader) = self.trust.expected_slot_leader(shred.slot()) else {
            self.reject(source, RepairRejectReason::MissingTrustedSlotLeader, poll);
            return Ok(());
        };
        if !shred.verify(&expected_leader) {
            self.reject(source, RepairRejectReason::InvalidSlotLeaderSignature, poll);
            return Ok(());
        }

        let Some(expected_fec) = self
            .trust
            .expected_fec_identity(shred.slot(), shred.fec_set_index())
        else {
            self.reject(source, RepairRejectReason::MissingTrustedFecIdentity, poll);
            return Ok(());
        };
        let actual_root = match shred.merkle_root() {
            Ok(root) => root,
            Err(error) => {
                self.reject(
                    source,
                    RepairRejectReason::MalformedResponse(error.to_string()),
                    poll,
                );
                return Ok(());
            }
        };
        if actual_root != expected_fec.merkle_root {
            self.reject(source, RepairRejectReason::FecMerkleRootMismatch, poll);
            return Ok(());
        }
        let actual_chained_root = match shred.chained_merkle_root() {
            Ok(root) => Some(root),
            Err(_) => {
                self.reject(source, RepairRejectReason::MissingChainedMerkleRoot, poll);
                return Ok(());
            }
        };
        let learned_chained_merkle_root = match expected_fec.chained_merkle_root {
            ChainedMerkleRootExpectation::Exact(expected) if actual_chained_root != expected => {
                self.reject(source, RepairRejectReason::ChainedMerkleRootMismatch, poll);
                return Ok(());
            }
            ChainedMerkleRootExpectation::Exact(_) => false,
            ChainedMerkleRootExpectation::LearnFromAnchoredRoot => true,
        };
        if !self.trust.reserve_repair_commitment(&shred) {
            self.reject(source, RepairRejectReason::RepairTrustChainConflict, poll);
            return Ok(());
        }

        let provenance = RepairProvenance {
            received_at_unix_ms: unix_ms,
            nonce: attempt.nonce,
            request: attempt.request,
            peer_addr: source.to_string(),
            peer_pubkey: peer.pubkey,
            shred_slot: shred.slot(),
            shred_index: shred.index(),
            fec_set_index: shred.fec_set_index(),
            shred_version: shred.version(),
            expected_slot_leader: expected_leader,
            fec_merkle_root: actual_root,
            trust_anchor_fec_set_index: expected_fec.trust_anchor_fec_set_index,
            learned_chained_merkle_root,
            chained_merkle_root: actual_chained_root,
            leader_signature: *shred.signature(),
        };
        // Persist before removing the nonce or exposing the shred. A disk error is fatal to this
        // service call and leaves the request outstanding for a bounded retry; no accepted shred
        // can bypass the provenance WAL.
        let append = self.repair_wal.append(&provenance, shred.payload(), now)?;
        self.stats.repair_wal_bytes = self
            .stats
            .repair_wal_bytes
            .saturating_add(append.frame_bytes as u64);
        if append.synced {
            self.stats.repair_wal_syncs = self.stats.repair_wal_syncs.saturating_add(1);
        }
        self.remove_outstanding(attempt.nonce);
        self.stats.shreds_accepted = self.stats.shreds_accepted.saturating_add(1);
        if learned_chained_merkle_root {
            self.stats.root_anchored_shreds_accepted =
                self.stats.root_anchored_shreds_accepted.saturating_add(1);
        }
        debug!(
            slot = shred.slot(),
            index = shred.index(),
            fec_set_index = shred.fec_set_index(),
            peer = %source,
            repair_wal_sequence = append.sequence,
            root_anchored = learned_chained_merkle_root,
            trust_anchor_fec_set_index = expected_fec.trust_anchor_fec_set_index,
            "accepted validated repair shred"
        );
        poll.accepted.push(AcceptedRepair {
            shred,
            provenance,
            repair_wal_sequence: append.sequence,
        });
        Ok(())
    }

    fn reject(&mut self, source: SocketAddr, reason: RepairRejectReason, poll: &mut RepairPoll) {
        self.stats.packets_rejected = self.stats.packets_rejected.saturating_add(1);
        debug!(peer = %source, ?reason, "rejected repair packet");
        poll.events
            .push(RepairRuntimeEvent::Rejected { source, reason });
    }

    fn remove_outstanding(&mut self, nonce: RepairNonce) -> Option<OutstandingRepair> {
        let attempt = self.outstanding.remove(&nonce)?;
        self.request_nonces
            .remove(&RequestKey::from(attempt.request));
        Some(attempt)
    }

    fn allocate_nonce(&mut self) -> io::Result<RepairNonce> {
        for _ in 0..=self.config.max_outstanding {
            let nonce = self.next_nonce;
            self.next_nonce = self.next_nonce.wrapping_add(1);
            if !self.outstanding.contains_key(&nonce) {
                return Ok(nonce);
            }
        }
        Err(io::Error::other(
            "repair nonce space is unexpectedly exhausted",
        ))
    }
}

fn tracker_request_to_wire(request: RepairRequest) -> ShredRepairRequest {
    match request {
        RepairRequest::WindowIndex { slot, index } => ShredRepairRequest::Shred {
            slot,
            shred_index: u64::from(index),
        },
        RepairRequest::HighestWindowIndex {
            slot,
            next_expected_data_index,
        } => ShredRepairRequest::HighestShred {
            slot,
            shred_index: u64::from(next_expected_data_index),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::serialize;
    use serde::Serialize;
    use solana_gossip::ping_pong::Ping;
    use solana_keypair::Signer;
    use tempfile::tempdir;

    use crate::repair_wal::{RepairWalConfig, RepairWalFsyncPolicy};

    struct TestTrust {
        peer: RepairPeer,
        version: u16,
        leader: Pubkey,
        fec: Option<TrustedFecIdentity>,
        allow_reservation: bool,
    }

    impl RepairTrust for TestTrust {
        fn peer_is_authorized(&self, peer: &RepairPeer, source: SocketAddr) -> bool {
            peer == &self.peer && source == self.peer.repair_addr
        }

        fn request_response_is_authorized(
            &self,
            _peer: &RepairPeer,
            request: ShredRepairRequest,
            shred: &Shred,
        ) -> bool {
            matches!(
                request,
                ShredRepairRequest::Shred { slot, shred_index }
                    if slot == shred.slot() && shred_index == u64::from(shred.index())
            )
        }

        fn expected_shred_version(&self, _slot: u64) -> Option<u16> {
            Some(self.version)
        }

        fn expected_slot_leader(&self, _slot: u64) -> Option<Pubkey> {
            Some(self.leader)
        }

        fn expected_fec_identity(
            &self,
            _slot: u64,
            _fec_set_index: u32,
        ) -> Option<TrustedFecIdentity> {
            self.fec
        }

        fn reserve_repair_commitment(&self, _shred: &Shred) -> bool {
            self.allow_reservation
        }
    }

    fn config() -> RepairRuntimeConfig {
        RepairRuntimeConfig {
            max_peers: 4,
            max_outstanding: 8,
            max_requests_considered_per_tick: 16,
            max_new_requests_per_tick: 8,
            max_packets_per_tick: 8,
            max_packet_bytes: 2_048,
            max_suppressed_requests: 64,
            request_timeout: Duration::from_millis(100),
            exhaustion_cooldown: Duration::from_secs(12),
            max_retries: 1,
            initial_nonce: 100,
        }
    }

    fn wal(directory: &tempfile::TempDir) -> RepairWal {
        RepairWal::open(
            RepairWalConfig {
                path: directory.path().join("runtime.repair.wal"),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
            },
            Instant::now(),
        )
        .unwrap()
    }

    fn signed_test_data_shred(slot: u64, index: u32, version: u16, leader: &Keypair) -> Shred {
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x90; // MerkleData { proof_size: 0, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[77..79].copy_from_slice(&version.to_le_bytes());
        payload[79..83].copy_from_slice(&(index / 32 * 32).to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0b1100_0000;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        let unsigned = Shred::new_from_serialized_shred(payload.clone()).unwrap();
        let signature = leader.sign_message(unsigned.merkle_root().unwrap().as_ref());
        payload[..64].copy_from_slice(signature.as_ref());
        let shred = Shred::new_from_serialized_shred(payload).unwrap();
        assert!(shred.verify(&leader.pubkey()));
        shred
    }

    #[tokio::test]
    async fn one_socket_retries_and_rotates_peer() {
        let directory = tempdir().unwrap();
        let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let first_key = Keypair::new();
        let second_key = Keypair::new();
        let peers = vec![
            RepairPeer {
                pubkey: first_key.pubkey(),
                repair_addr: first.local_addr().unwrap(),
            },
            RepairPeer {
                pubkey: second_key.pubkey(),
                repair_addr: second.local_addr().unwrap(),
            },
        ];
        struct AllPeers;
        impl RepairTrust for AllPeers {
            fn peer_is_authorized(&self, _peer: &RepairPeer, _source: SocketAddr) -> bool {
                true
            }
            fn request_response_is_authorized(
                &self,
                _peer: &RepairPeer,
                _request: ShredRepairRequest,
                _shred: &Shred,
            ) -> bool {
                false
            }
            fn expected_shred_version(&self, _slot: u64) -> Option<u16> {
                None
            }
            fn expected_slot_leader(&self, _slot: u64) -> Option<Pubkey> {
                None
            }
            fn expected_fec_identity(
                &self,
                _slot: u64,
                _fec_set_index: u32,
            ) -> Option<TrustedFecIdentity> {
                None
            }
        }
        let mut runtime = RepairRuntime::bind(
            "127.0.0.1:0",
            Arc::new(Keypair::new()),
            AllPeers,
            peers,
            config(),
            wal(&directory),
        )
        .await
        .unwrap();
        assert_eq!(
            runtime.stats().repair_wal_bytes,
            runtime.repair_wal().file_len(),
            "WAL metrics must include bytes recovered at open"
        );
        assert_eq!(
            runtime.stats().repair_wal_max_bytes,
            runtime.repair_wal().max_file_bytes()
        );
        let local_addr = runtime.local_addr().unwrap();
        let now = Instant::now();
        let request = ShredRepairRequest::Shred {
            slot: 123,
            shred_index: 7,
        };
        runtime
            .service_requests([request], now, 1_000)
            .await
            .unwrap();
        let mut buffer = [0u8; 512];
        let (_, source) = first.recv_from(&mut buffer).await.unwrap();
        assert_eq!(source, local_addr);

        let poll = runtime
            .service_requests(std::iter::empty(), now + Duration::from_millis(101), 1_101)
            .await
            .unwrap();
        let (_, retry_source) = second.recv_from(&mut buffer).await.unwrap();
        assert_eq!(
            retry_source, local_addr,
            "retry must use the same UDP socket"
        );
        assert!(matches!(
            poll.events.as_slice(),
            [RepairRuntimeEvent::Retried { request: seen, retry: 1, peer }]
                if *seen == request && peer.repair_addr == second.local_addr().unwrap()
        ));
    }

    #[tokio::test]
    async fn exhausted_request_is_not_immediately_started_again() {
        let directory = tempdir().unwrap();
        let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_key = Keypair::new();
        let peer = RepairPeer {
            pubkey: peer_key.pubkey(),
            repair_addr: peer_socket.local_addr().unwrap(),
        };
        let trust = TestTrust {
            peer: peer.clone(),
            version: 1,
            leader: Pubkey::default(),
            fec: None,
            allow_reservation: true,
        };
        let mut runtime_config = config();
        runtime_config.max_retries = 0;
        let mut runtime = RepairRuntime::bind(
            "127.0.0.1:0",
            Arc::new(Keypair::new()),
            trust,
            vec![peer],
            runtime_config,
            wal(&directory),
        )
        .await
        .unwrap();
        let request = ShredRepairRequest::Shred {
            slot: 123,
            shred_index: 7,
        };
        let now = Instant::now();

        runtime
            .service_requests([request], now, 1_000)
            .await
            .unwrap();
        let mut packet = [0u8; 512];
        peer_socket.recv_from(&mut packet).await.unwrap();
        let poll = runtime
            .service_requests([request], now + Duration::from_millis(101), 1_101)
            .await
            .unwrap();

        assert!(matches!(
            poll.events.as_slice(),
            [RepairRuntimeEvent::Exhausted { request: seen, attempts: 1 }] if *seen == request
        ));
        assert_eq!(runtime.outstanding_count(), 0);
        assert_eq!(runtime.stats().requests_sent, 1);
        assert_eq!(runtime.stats().requests_cooldown_deferred, 1);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                peer_socket.recv_from(&mut packet)
            )
            .await
            .is_err(),
            "the same request must remain suppressed after exhausting its budget"
        );

        runtime
            .service_requests(
                [request],
                now + runtime_config.exhaustion_cooldown + Duration::from_millis(102),
                13_102,
            )
            .await
            .unwrap();
        peer_socket.recv_from(&mut packet).await.unwrap();
        assert_eq!(runtime.stats().requests_sent, 2);
    }

    #[tokio::test]
    async fn suppressed_candidates_do_not_starve_later_new_requests() {
        let directory = tempdir().unwrap();
        let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_key = Keypair::new();
        let peer = RepairPeer {
            pubkey: peer_key.pubkey(),
            repair_addr: peer_socket.local_addr().unwrap(),
        };
        let trust = TestTrust {
            peer: peer.clone(),
            version: 1,
            leader: Pubkey::default(),
            fec: None,
            allow_reservation: true,
        };
        let runtime_config = config();
        let mut runtime = RepairRuntime::bind(
            "127.0.0.1:0",
            Arc::new(Keypair::new()),
            trust,
            vec![peer],
            runtime_config,
            wal(&directory),
        )
        .await
        .unwrap();
        let now = Instant::now();
        let requests = (0..=runtime_config.max_new_requests_per_tick as u64)
            .map(|shred_index| ShredRepairRequest::Shred {
                slot: 123,
                shred_index,
            })
            .collect::<Vec<_>>();
        let until = now + runtime_config.exhaustion_cooldown;
        for request in requests
            .iter()
            .take(runtime_config.max_new_requests_per_tick)
        {
            runtime
                .suppressed_until
                .insert(RequestKey::from(*request), until);
        }

        runtime
            .service_requests(requests.clone(), now, 1_000)
            .await
            .unwrap();

        let mut packet = [0u8; 512];
        peer_socket.recv_from(&mut packet).await.unwrap();
        assert_eq!(runtime.outstanding_count(), 1);
        assert_eq!(
            runtime.outstanding_repairs()[0].request,
            *requests.last().unwrap()
        );
        assert_eq!(
            runtime.stats().requests_cooldown_deferred,
            runtime_config.max_new_requests_per_tick as u64
        );
    }

    #[tokio::test]
    async fn answers_only_a_ping_signed_by_the_address_bound_peer() {
        #[derive(Serialize)]
        enum TestRepairResponse {
            Ping(Ping<32>),
        }

        let directory = tempdir().unwrap();
        let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_key = Keypair::new();
        let peer = RepairPeer {
            pubkey: peer_key.pubkey(),
            repair_addr: peer_socket.local_addr().unwrap(),
        };
        let trust = TestTrust {
            peer: peer.clone(),
            version: 1,
            leader: Pubkey::default(),
            fec: None,
            allow_reservation: true,
        };
        let mut runtime = RepairRuntime::bind(
            "127.0.0.1:0",
            Arc::new(Keypair::new()),
            trust,
            vec![peer.clone()],
            config(),
            wal(&directory),
        )
        .await
        .unwrap();
        let request = ShredRepairRequest::Shred {
            slot: 123,
            shred_index: 7,
        };
        let now = Instant::now();
        runtime.service_requests([request], now, 999).await.unwrap();
        let mut outbound = [0u8; 256];
        peer_socket.recv_from(&mut outbound).await.unwrap();

        let ping = serialize(&TestRepairResponse::Ping(Ping::new([7; 32], &peer_key))).unwrap();
        peer_socket
            .send_to(&ping, runtime.local_addr().unwrap())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let poll = runtime
            .service_requests(std::iter::empty(), now + Duration::from_millis(5), 1_000)
            .await
            .unwrap();
        assert!(matches!(
            poll.events.as_slice(),
            [
                RepairRuntimeEvent::PongSent { peer: seen },
                RepairRuntimeEvent::Retried {
                    request: retried,
                    retry: 1,
                    peer: retry_peer,
                }
            ] if seen == &peer && *retried == request && retry_peer == &peer
        ));
        let mut pong = [0u8; 256];
        let (len, _) = peer_socket.recv_from(&mut pong).await.unwrap();
        assert!(len > 4);
        let (retry_len, retry_source) = peer_socket.recv_from(&mut outbound).await.unwrap();
        assert!(retry_len > 4);
        assert_eq!(retry_source, runtime.local_addr().unwrap());
        assert_eq!(runtime.outstanding_repairs()[0].retries_used, 1);
    }

    #[tokio::test]
    async fn trust_chain_conflict_is_rejected_before_wal_append() {
        let directory = tempdir().unwrap();
        let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_key = Keypair::new();
        let peer = RepairPeer {
            pubkey: peer_key.pubkey(),
            repair_addr: peer_socket.local_addr().unwrap(),
        };
        let leader = Keypair::new();
        let version = 50_093;
        let shred = signed_test_data_shred(123, 0, version, &leader);
        let trust = TestTrust {
            peer: peer.clone(),
            version,
            leader: leader.pubkey(),
            fec: Some(TrustedFecIdentity {
                merkle_root: shred.merkle_root().unwrap(),
                chained_merkle_root: ChainedMerkleRootExpectation::Exact(
                    shred.chained_merkle_root().ok(),
                ),
                trust_anchor_fec_set_index: shred.fec_set_index(),
            }),
            allow_reservation: false,
        };
        let mut runtime = RepairRuntime::bind(
            "127.0.0.1:0",
            Arc::new(Keypair::new()),
            trust,
            vec![peer],
            config(),
            wal(&directory),
        )
        .await
        .unwrap();
        runtime
            .service_requests(
                [ShredRepairRequest::Shred {
                    slot: shred.slot(),
                    shred_index: u64::from(shred.index()),
                }],
                Instant::now(),
                1_000,
            )
            .await
            .unwrap();
        let attempt = runtime.outstanding_repairs()[0];
        let mut response = shred.payload().to_vec();
        response.extend_from_slice(&attempt.nonce.to_le_bytes());
        peer_socket
            .send_to(&response, runtime.local_addr().unwrap())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let poll = runtime
            .service_requests(std::iter::empty(), Instant::now(), 1_001)
            .await
            .unwrap();
        assert!(poll.accepted.is_empty());
        assert!(matches!(
            poll.events.as_slice(),
            [RepairRuntimeEvent::Rejected {
                reason: RepairRejectReason::RepairTrustChainConflict,
                ..
            }]
        ));
        assert_eq!(runtime.repair_wal().next_sequence(), 0);
        assert_eq!(runtime.outstanding_count(), 1);
    }

    #[tokio::test]
    async fn persists_only_fully_validated_matching_response() {
        let directory = tempdir().unwrap();
        let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_key = Keypair::new();
        let peer = RepairPeer {
            pubkey: peer_key.pubkey(),
            repair_addr: peer_socket.local_addr().unwrap(),
        };
        let leader = Keypair::new();
        let version = 50093;
        let slot = 123;
        let shred = signed_test_data_shred(slot, 0, version, &leader);
        let fec = TrustedFecIdentity {
            merkle_root: shred.merkle_root().unwrap(),
            chained_merkle_root: ChainedMerkleRootExpectation::Exact(
                shred.chained_merkle_root().ok(),
            ),
            trust_anchor_fec_set_index: shred.fec_set_index(),
        };
        let trust = TestTrust {
            peer: peer.clone(),
            version,
            leader: leader.pubkey(),
            fec: Some(fec),
            allow_reservation: true,
        };
        let wal_path = directory.path().join("runtime.repair.wal");
        let mut runtime = RepairRuntime::bind(
            "127.0.0.1:0",
            Arc::new(Keypair::new()),
            trust,
            vec![peer],
            config(),
            RepairWal::open(
                RepairWalConfig {
                    path: wal_path.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                },
                Instant::now(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let request = ShredRepairRequest::Shred {
            slot,
            shred_index: u64::from(shred.index()),
        };
        runtime
            .service_requests([request], Instant::now(), 1_000)
            .await
            .unwrap();
        let attempt = runtime.outstanding_repairs()[0];

        let mut forged = shred.payload().to_vec();
        forged[0] ^= 1;
        forged.extend_from_slice(&attempt.nonce.to_le_bytes());
        peer_socket
            .send_to(&forged, runtime.local_addr().unwrap())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let rejected = runtime
            .service_requests(std::iter::empty(), Instant::now(), 1_001)
            .await
            .unwrap();
        assert!(matches!(
            rejected.events.as_slice(),
            [RepairRuntimeEvent::Rejected {
                reason: RepairRejectReason::InvalidSlotLeaderSignature,
                ..
            }]
        ));
        assert_eq!(runtime.outstanding_count(), 1);
        assert_eq!(runtime.repair_wal().next_sequence(), 0);

        let mut response = shred.payload().to_vec();
        response.extend_from_slice(&attempt.nonce.to_le_bytes());
        peer_socket
            .send_to(&response, runtime.local_addr().unwrap())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let poll = runtime
            .service_requests(std::iter::empty(), Instant::now(), 1_002)
            .await
            .unwrap();

        assert_eq!(poll.accepted.len(), 1);
        assert_eq!(poll.accepted[0].shred.id(), shred.id());
        assert_eq!(runtime.outstanding_count(), 0);
        assert_eq!(runtime.stats().shreds_accepted, 1);
        runtime.flush_repair_wal(Instant::now()).unwrap();
        drop(runtime);
        let entries = RepairWal::read_all(&wal_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].provenance.request, request);
        assert_eq!(
            entries[0].shred_payload.as_slice(),
            shred.payload().as_ref()
        );
    }
}
