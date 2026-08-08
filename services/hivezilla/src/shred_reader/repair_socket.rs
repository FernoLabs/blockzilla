//! Bounded, continuously drained UDP ingress for Agave repair responses.
//!
//! The repair state machine can wait for an `EveryRecord` WAL fsync before accepting a shred.  It
//! must not leave the kernel receive queue unattended while that happens.  This module therefore
//! owns a small Tokio receive task that does no parsing or trust work: it copies datagrams into one
//! fixed-capacity channel, records socket-local loss, and immediately resumes `recvmsg(2)`.  The
//! single repair runtime remains the only consumer, preserving response order, nonce validation,
//! trust validation, and fsync-before-accept.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use socket2::SockRef;
#[cfg(target_os = "linux")]
use socket2::{SockAddr, SockAddrStorage};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{error, warn};

use super::loss_telemetry::socket_rxq_overflow_delta;

const SOCKET_DRAIN_BURST_MAX_RECORDS: usize = 64;
const SOCKET_OVERFLOW_LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairSocketConfig {
    pub requested_recv_buffer_bytes: usize,
    pub max_packet_bytes: usize,
    pub response_queue_capacity: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RepairSocketSnapshot {
    pub requested_recv_buffer_bytes: u64,
    pub effective_recv_buffer_bytes: u64,
    pub socket_rxq_overflow_supported: bool,
    pub socket_rxq_overflow: u64,
    pub socket_datagrams_received: u64,
    pub response_queue_capacity: u64,
    pub response_queue_depth: u64,
    pub response_queue_dropped: u64,
}

#[derive(Debug)]
pub struct RepairDatagram {
    pub payload: Box<[u8]>,
    pub source: SocketAddr,
    pub received_at_unix_ms: u64,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug)]
struct ReceivedDatagram {
    length: usize,
    source: SocketAddr,
    socket_rxq_overflow: Option<u32>,
    truncated: bool,
}

struct SocketOverflowTracker {
    previous: u32,
    unlogged: u64,
    last_log: Instant,
}

impl SocketOverflowTracker {
    fn new() -> Self {
        Self {
            previous: 0,
            unlogged: 0,
            last_log: Instant::now()
                .checked_sub(SOCKET_OVERFLOW_LOG_INTERVAL)
                .unwrap_or_else(Instant::now),
        }
    }
}

#[derive(Default)]
struct Counters {
    socket_rxq_overflow: AtomicU64,
    socket_datagrams_received: AtomicU64,
    response_queue_depth: AtomicU64,
    response_queue_dropped: AtomicU64,
    terminal_error: Mutex<Option<(io::ErrorKind, String)>>,
}

struct ReceiverTask {
    stop: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl Drop for ReceiverTask {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

pub struct RepairSocket {
    socket: Arc<UdpSocket>,
    incoming: mpsc::Receiver<RepairDatagram>,
    counters: Arc<Counters>,
    requested_recv_buffer_bytes: u64,
    effective_recv_buffer_bytes: u64,
    socket_rxq_overflow_supported: bool,
    response_queue_capacity: u64,
    ingress_stopped: bool,
    receiver_task: ReceiverTask,
}

impl RepairSocket {
    pub fn new(socket: UdpSocket, config: RepairSocketConfig) -> io::Result<Self> {
        let socket_ref = SockRef::from(&socket);
        if let Err(error) = socket_ref.set_recv_buffer_size(config.requested_recv_buffer_bytes) {
            warn!(
                requested_bytes = config.requested_recv_buffer_bytes,
                %error,
                "kernel rejected requested repair UDP receive buffer; continuing with the effective system limit"
            );
        }
        let effective_recv_buffer_bytes = socket_ref.recv_buffer_size()?;
        if effective_recv_buffer_bytes < config.requested_recv_buffer_bytes {
            warn!(
                requested_bytes = config.requested_recv_buffer_bytes,
                effective_bytes = effective_recv_buffer_bytes,
                "repair UDP receive buffer is below the requested size; raise the host net.core.rmem_max ceiling"
            );
        }

        #[cfg(target_os = "linux")]
        let socket_rxq_overflow_supported =
            match super::loss_telemetry::enable_socket_rxq_overflow(&socket) {
                Ok(()) => true,
                Err(error) => {
                    warn!(%error, "repair socket-overflow telemetry could not be enabled");
                    false
                }
            };
        #[cfg(not(target_os = "linux"))]
        let socket_rxq_overflow_supported = false;

        let socket = Arc::new(socket);
        let counters = Arc::new(Counters::default());
        let (sender, incoming) = mpsc::channel(config.response_queue_capacity);
        let receiver_socket = socket.clone();
        let receiver_counters = counters.clone();
        let (stop, stop_receiver) = oneshot::channel();
        let receiver = tokio::spawn(async move {
            let result = receive_loop(
                receiver_socket,
                sender,
                receiver_counters.clone(),
                config.max_packet_bytes,
                stop_receiver,
            )
            .await;
            if let Err(error) = result {
                *receiver_counters.terminal_error.lock().unwrap() =
                    Some((error.kind(), error.to_string()));
            }
        });

        Ok(Self {
            socket,
            incoming,
            counters,
            requested_recv_buffer_bytes: config.requested_recv_buffer_bytes as u64,
            effective_recv_buffer_bytes: effective_recv_buffer_bytes as u64,
            socket_rxq_overflow_supported,
            response_queue_capacity: config.response_queue_capacity as u64,
            ingress_stopped: false,
            receiver_task: ReceiverTask {
                stop: Some(stop),
                join: Some(receiver),
            },
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub async fn send_to(&self, payload: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(payload, target).await
    }

    /// Stops the receive owner and waits until it can no longer stage a datagram. Already-staged
    /// datagrams remain in the ordered channel for the runtime to validate and persist.
    pub async fn stop_ingress(&mut self) -> io::Result<()> {
        if !self.ingress_stopped {
            self.ingress_stopped = true;
            if let Some(stop) = self.receiver_task.stop.take() {
                let _ = stop.send(());
            }
            if let Some(join) = self.receiver_task.join.take() {
                join.await.map_err(|error| {
                    io::Error::other(format!("repair UDP receiver task failed: {error}"))
                })?;
            }
        }

        if let Some((kind, message)) = self.counters.terminal_error.lock().unwrap().clone() {
            return Err(io::Error::new(kind, message));
        }
        Ok(())
    }

    pub fn try_recv(&mut self) -> io::Result<Option<RepairDatagram>> {
        match self.incoming.try_recv() {
            Ok(datagram) => {
                let previous = self
                    .counters
                    .response_queue_depth
                    .fetch_sub(1, Ordering::Relaxed);
                debug_assert!(previous > 0, "repair response queue depth underflow");
                Ok(Some(datagram))
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                if let Some((kind, message)) = self.counters.terminal_error.lock().unwrap().clone()
                {
                    return Err(io::Error::new(kind, message));
                }
                if self.ingress_stopped {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "repair UDP receiver stopped unexpectedly",
                    ))
                }
            }
        }
    }

    pub fn snapshot(&self) -> RepairSocketSnapshot {
        RepairSocketSnapshot {
            requested_recv_buffer_bytes: self.requested_recv_buffer_bytes,
            effective_recv_buffer_bytes: self.effective_recv_buffer_bytes,
            socket_rxq_overflow_supported: self.socket_rxq_overflow_supported,
            socket_rxq_overflow: self.counters.socket_rxq_overflow.load(Ordering::Relaxed),
            socket_datagrams_received: self
                .counters
                .socket_datagrams_received
                .load(Ordering::Relaxed),
            response_queue_capacity: self.response_queue_capacity,
            response_queue_depth: self.counters.response_queue_depth.load(Ordering::Relaxed),
            response_queue_dropped: self.counters.response_queue_dropped.load(Ordering::Relaxed),
        }
    }
}

async fn receive_loop(
    socket: Arc<UdpSocket>,
    sender: mpsc::Sender<RepairDatagram>,
    counters: Arc<Counters>,
    max_packet_bytes: usize,
    mut stop: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut buffer = vec![0u8; max_packet_bytes];
    let mut overflow = SocketOverflowTracker::new();

    loop {
        let received = tokio::select! {
            biased;
            _ = &mut stop => return Ok(()),
            result = receive_datagram(&socket, &mut buffer) => result?,
        };
        enqueue_datagram(&sender, &counters, &buffer, received, &mut overflow);

        // One readiness wake drains a bounded burst before yielding. The outer loop immediately
        // waits for readability again, so the socket remains continuously serviced without an
        // unbounded CPU loop.
        for _ in 1..SOCKET_DRAIN_BURST_MAX_RECORDS {
            let received = match try_receive_datagram(&socket, &mut buffer) {
                Ok(received) => received,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            };
            enqueue_datagram(&sender, &counters, &buffer, received, &mut overflow);
        }
        tokio::task::yield_now().await;
    }
}

fn enqueue_datagram(
    sender: &mpsc::Sender<RepairDatagram>,
    counters: &Counters,
    buffer: &[u8],
    received: ReceivedDatagram,
    overflow: &mut SocketOverflowTracker,
) {
    counters
        .socket_datagrams_received
        .fetch_add(1, Ordering::Relaxed);
    if let Some(current) = received.socket_rxq_overflow {
        let dropped = socket_rxq_overflow_delta(current, overflow.previous);
        overflow.previous = current;
        if dropped != 0 {
            let total = counters
                .socket_rxq_overflow
                .fetch_add(dropped, Ordering::Relaxed)
                .saturating_add(dropped);
            overflow.unlogged = overflow.unlogged.saturating_add(dropped);
            if overflow.last_log.elapsed() >= SOCKET_OVERFLOW_LOG_INTERVAL {
                error!(
                    repair_socket_rxq_overflow_delta = overflow.unlogged,
                    repair_socket_rxq_overflow_total = total,
                    source = %received.source,
                    "Linux reported repair datagrams dropped from the repair socket receive queue"
                );
                overflow.unlogged = 0;
                overflow.last_log = Instant::now();
            }
        }
    }

    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(_) => {
            counters
                .response_queue_dropped
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let retained_length = received.length.min(buffer.len());
    let datagram = RepairDatagram {
        payload: buffer[..retained_length].to_vec().into_boxed_slice(),
        source: received.source,
        received_at_unix_ms: unix_millis(),
        truncated: received.truncated,
    };
    counters
        .response_queue_depth
        .fetch_add(1, Ordering::Relaxed);
    permit.send(datagram);
}

#[cfg(target_os = "linux")]
async fn receive_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<ReceivedDatagram> {
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            recvmsg_datagram(socket, buffer)
        })
        .await
}

#[cfg(not(target_os = "linux"))]
async fn receive_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<ReceivedDatagram> {
    let (length, source) = socket.recv_from(buffer).await?;
    Ok(ReceivedDatagram {
        length,
        source,
        socket_rxq_overflow: None,
        // `recv_from` does not expose truncation on every supported platform. Preserve the prior
        // fail-closed behavior at the configured receive limit.
        truncated: length == buffer.len(),
    })
}

#[cfg(target_os = "linux")]
fn try_receive_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<ReceivedDatagram> {
    socket.try_io(tokio::io::Interest::READABLE, || {
        recvmsg_datagram(socket, buffer)
    })
}

#[cfg(not(target_os = "linux"))]
fn try_receive_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<ReceivedDatagram> {
    let (length, source) = socket.try_recv_from(buffer)?;
    Ok(ReceivedDatagram {
        length,
        source,
        socket_rxq_overflow: None,
        truncated: length == buffer.len(),
    })
}

#[cfg(target_os = "linux")]
fn recvmsg_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<ReceivedDatagram> {
    use std::{mem, os::fd::AsRawFd, ptr};

    let mut io_vector = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let mut source_storage = SockAddrStorage::zeroed();
    let mut control = [0usize; 8];
    // SAFETY: every pointer refers to live writable storage for the duration of recvmsg. The
    // nonblocking flag preserves Tokio readiness semantics; MSG_TRUNC reports oversize packets.
    let mut message = unsafe { mem::zeroed::<libc::msghdr>() };
    let source_address = unsafe { source_storage.view_as::<libc::sockaddr_storage>() };
    message.msg_name = ptr::from_mut(source_address).cast::<libc::c_void>();
    message.msg_namelen = source_storage.size_of();
    message.msg_iov = &mut io_vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = mem::size_of_val(&control);
    let received = unsafe {
        libc::recvmsg(
            socket.as_raw_fd(),
            &mut message,
            libc::MSG_DONTWAIT | libc::MSG_TRUNC,
        )
    };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    let length = usize::try_from(received)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative repair UDP length"))?;
    let truncated = length > buffer.len() || message.msg_flags & libc::MSG_TRUNC != 0;

    let mut socket_rxq_overflow = None;
    if message.msg_flags & libc::MSG_CTRUNC == 0 {
        // SAFETY: recvmsg initialized the reported control region. CMSG helpers stay within it,
        // and the native u32 payload is deliberately read unaligned.
        unsafe {
            let mut header = libc::CMSG_FIRSTHDR(&message);
            while !header.is_null() {
                if (*header).cmsg_level == libc::SOL_SOCKET
                    && (*header).cmsg_type == libc::SO_RXQ_OVFL
                    && (*header).cmsg_len
                        >= libc::CMSG_LEN(mem::size_of::<u32>() as libc::c_uint) as usize
                {
                    socket_rxq_overflow =
                        Some(ptr::read_unaligned(libc::CMSG_DATA(header).cast::<u32>()));
                    break;
                }
                header = libc::CMSG_NXTHDR(&message, header);
            }
        }
    }

    // SAFETY: recvmsg initialized this address storage and its reported length.
    let source = unsafe { SockAddr::new(source_storage, message.msg_namelen) }
        .as_socket()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "repair UDP datagram had a non-IP source address",
            )
        })?;
    Ok(ReceivedDatagram {
        length,
        source,
        socket_rxq_overflow,
        truncated,
    })
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(queue_capacity: usize) -> RepairSocketConfig {
        RepairSocketConfig {
            requested_recv_buffer_bytes: 64 * 1024,
            max_packet_bytes: 2_048,
            response_queue_capacity: queue_capacity,
        }
    }

    async fn wait_for_accounted(socket: &RepairSocket, expected: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = socket.snapshot();
                if snapshot.socket_datagrams_received >= expected
                    && snapshot
                        .response_queue_depth
                        .saturating_add(snapshot.response_queue_dropped)
                        >= expected
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn receiver_drains_without_consumer_polling_and_preserves_order() {
        let bound = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = bound.local_addr().unwrap();
        let mut repair = RepairSocket::new(bound, config(8)).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        for value in 0..4u8 {
            sender.send_to(&[value], target).await.unwrap();
        }
        wait_for_accounted(&repair, 4).await;

        let before_poll = repair.snapshot();
        assert_eq!(before_poll.response_queue_depth, 4);
        assert_eq!(before_poll.response_queue_dropped, 0);
        for value in 0..4u8 {
            let datagram = repair.try_recv().unwrap().unwrap();
            assert_eq!(datagram.payload.as_ref(), &[value]);
        }
        assert_eq!(repair.snapshot().response_queue_depth, 0);
    }

    #[tokio::test]
    async fn full_user_queue_is_bounded_and_loss_is_counted() {
        let bound = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = bound.local_addr().unwrap();
        let repair = RepairSocket::new(bound, config(1)).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        for value in 0..4u8 {
            sender.send_to(&[value], target).await.unwrap();
        }
        wait_for_accounted(&repair, 4).await;

        let snapshot = repair.snapshot();
        assert_eq!(snapshot.response_queue_capacity, 1);
        assert_eq!(snapshot.response_queue_depth, 1);
        assert_eq!(snapshot.response_queue_dropped, 3);
        assert!(snapshot.effective_recv_buffer_bytes > 0);
    }

    #[tokio::test]
    async fn orderly_stop_preserves_the_complete_staged_prefix() {
        let bound = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = bound.local_addr().unwrap();
        let mut repair = RepairSocket::new(bound, config(8)).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        sender.send_to(&[1], target).await.unwrap();
        sender.send_to(&[2], target).await.unwrap();
        wait_for_accounted(&repair, 2).await;
        repair.stop_ingress().await.unwrap();

        assert_eq!(repair.snapshot().response_queue_depth, 2);
        assert_eq!(repair.try_recv().unwrap().unwrap().payload.as_ref(), &[1]);
        assert_eq!(repair.try_recv().unwrap().unwrap().payload.as_ref(), &[2]);
        assert!(repair.try_recv().unwrap().is_none());
        assert_eq!(repair.snapshot().response_queue_depth, 0);
    }
}
