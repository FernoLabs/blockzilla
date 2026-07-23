use std::{
    net::{IpAddr, SocketAddr, TcpListener, ToSocketAddrs, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use socket2::{Domain, Protocol as SocketProtocol, SockAddr, Socket, Type};
use solana_gossip::{
    cluster_info::ClusterInfo,
    contact_info::{ContactInfo, Protocol},
    gossip_service::GossipService,
};
use solana_keypair::Signer;
use solana_ledger::shred::Shred;
use solana_net_utils::{
    SocketAddrSpace, get_cluster_shred_version_with_binding, get_public_ip_addr_with_binding,
    ip_echo_server, multihomed_sockets::BindIpAddrs, verify_all_reachable_tcp,
    verify_all_reachable_udp,
};
use tokio::{
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use tracing::{info, warn};

use crate::{
    config::{Config, public_ipv4},
    identity,
    loss_telemetry::LossTelemetry,
    metrics::{self, Metrics, ServiceState},
    repair_service::{self, RepairServiceConfig},
};

const MAX_UDP_DATAGRAM_SIZE: usize = 2_048;
const FORWARD_DRAIN_TIMEOUT: Duration = Duration::from_secs(20);
const REPAIR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

struct ShredForwarder {
    socket: tokio::net::UdpSocket,
    targets: Vec<SocketAddr>,
}

struct ReceivePipeline {
    sample_interval: Duration,
    forwarder: Option<ShredForwarder>,
    forward_queue_capacity: usize,
    repair_observation_queue: Option<mpsc::Sender<Arc<[u8]>>>,
}

impl ShredForwarder {
    async fn bind(bind_ip: IpAddr, targets: Vec<SocketAddr>) -> Result<Option<Self>> {
        if targets.is_empty() {
            return Ok(None);
        }
        let socket = tokio::net::UdpSocket::bind(SocketAddr::new(bind_ip, 0))
            .await
            .context("failed to bind shred forwarding socket")?;
        Ok(Some(Self { socket, targets }))
    }

    async fn forward(&self, payload: &[u8], metrics: &Metrics) {
        for target in &self.targets {
            match self.socket.send_to(payload, *target).await {
                Ok(size) if size == payload.len() => metrics.record_forwarded(),
                Ok(_) | Err(_) => {
                    metrics.record_forward_send_error();
                }
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FinishedTask {
    None,
    Receiver,
    Metrics,
    MetricsLogger,
}

pub async fn run(config: Config) -> Result<()> {
    let entrypoint_names = config.entrypoints.clone();
    let entrypoints =
        tokio::task::spawn_blocking(move || resolve_entrypoints(&entrypoint_names)).await??;
    let advertised_override = config.advertised_ip;
    let shred_version_override = config.shred_version;
    let bind_ip = config.bind_ip;
    let bootstrap_entrypoints = entrypoints.clone();
    let (advertised_ip, shred_version) = tokio::task::spawn_blocking(move || {
        bootstrap_network(
            &bootstrap_entrypoints,
            bind_ip,
            advertised_override,
            shred_version_override,
        )
    })
    .await??;

    let keypair = Arc::new(identity::load_or_create(&config.identity_path)?);
    let identity_pubkey = keypair.pubkey().to_string();
    let gossip_bind = SocketAddr::new(config.bind_ip, config.gossip_port);
    let tvu_bind = SocketAddr::new(config.bind_ip, config.tvu_port);
    let gossip_advertised = SocketAddr::new(IpAddr::V4(advertised_ip), config.gossip_port);
    let tvu_advertised = SocketAddr::new(IpAddr::V4(advertised_ip), config.tvu_port);

    let gossip_udp = UdpSocket::bind(gossip_bind)
        .with_context(|| format!("failed to bind gossip UDP socket at {gossip_bind}"))?;
    let gossip_tcp = TcpListener::bind(gossip_bind)
        .with_context(|| format!("failed to bind gossip TCP socket at {gossip_bind}"))?;
    let (tvu_udp, effective_recv_buffer) = bind_tvu_socket(tvu_bind, config.udp_recv_buffer_bytes)?;

    let (gossip_udp, gossip_tcp, tvu_udp) = if config.require_reachability {
        let verification_entrypoints = entrypoints.clone();
        tokio::task::spawn_blocking(move || -> Result<_> {
            verify_public_reachability(
                &verification_entrypoints,
                &gossip_udp,
                &gossip_tcp,
                &tvu_udp,
            )?;
            Ok((gossip_udp, gossip_tcp, tvu_udp))
        })
        .await
        .context("public reachability task panicked")??
    } else {
        warn!("public port reachability check is disabled");
        (gossip_udp, gossip_tcp, tvu_udp)
    };
    tvu_udp.set_nonblocking(true)?;
    let tvu_udp = tokio::net::UdpSocket::from_std(tvu_udp)?;

    let mut contact_info = ContactInfo::new(keypair.pubkey(), unix_millis(), shred_version);
    contact_info
        .set_gossip(gossip_advertised)
        .context("failed to advertise gossip socket")?;
    contact_info
        .set_tvu(Protocol::UDP, tvu_advertised)
        .context("failed to advertise TVU socket")?;

    let mut cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Global);
    cluster_info.set_contact_debug_interval(0);
    cluster_info.set_bind_ip_addrs(Arc::new(
        BindIpAddrs::new(vec![config.bind_ip]).map_err(|error| anyhow!(error))?,
    ));
    cluster_info.set_entrypoints(
        entrypoints
            .iter()
            .map(ContactInfo::new_gossip_entry_point)
            .collect(),
    );
    let cluster_info = Arc::new(cluster_info);
    let exit = Arc::new(AtomicBool::new(false));

    // Finish every fallible local setup step before starting Agave's OS threads.  If, for
    // example, the metrics port is already occupied, returning here is then a clean exit rather
    // than a process kept alive by a detached gossip thread.
    let metrics = Arc::new(Metrics::new(config.dedup_capacity)?);
    let forwarder =
        ShredForwarder::bind(config.bind_ip, config.shred_forward_addrs.clone()).await?;
    let service_state = ServiceState {
        metrics: metrics.clone(),
        cluster_info: cluster_info.clone(),
        identity: identity_pubkey.clone(),
        advertised_ip: advertised_ip.to_string(),
        gossip_port: config.gossip_port,
        tvu_port: config.tvu_port,
        shred_version,
        forward_targets: config.shred_forward_addrs.len(),
        repair_enabled: config.repair_enabled,
    };
    let metrics_listener = tokio::net::TcpListener::bind(config.metrics_addr)
        .await
        .with_context(|| format!("failed to bind metrics endpoint at {}", config.metrics_addr))?;

    let ip_echo = ip_echo_server(gossip_tcp, std::num::NonZeroUsize::MIN, Some(shred_version));
    let gossip_service = GossipService::new(
        &cluster_info,
        None,
        Arc::new([gossip_udp]),
        None,
        true,
        None,
        exit.clone(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    info!(
        identity = %identity_pubkey,
        entrypoints = ?entrypoints,
        advertised_ip = %advertised_ip,
        gossip_port = config.gossip_port,
        tvu_port = config.tvu_port,
        shred_version,
        requested_udp_recv_buffer = config.udp_recv_buffer_bytes,
        effective_udp_recv_buffer = effective_recv_buffer,
        metrics_addr = %config.metrics_addr,
        forward_targets = ?config.shred_forward_addrs,
        "gossip and TVU receiver started"
    );

    let mut metrics_task = tokio::spawn(metrics::serve(
        metrics_listener,
        service_state.clone(),
        shutdown_rx.clone(),
    ));
    let mut log_task = tokio::spawn(log_metrics(
        service_state,
        config.metrics_interval,
        LossTelemetry::system(config.loss_telemetry_interfaces.clone()),
        shutdown_rx.clone(),
    ));
    let (repair_observation_tx, repair_task) = if config.repair_enabled {
        let (repair_tx, repair_rx) =
            mpsc::channel::<Arc<[u8]>>(config.repair_observation_queue_capacity);
        let repair_config = RepairServiceConfig {
            rpc_url: config.repair_rpc_url.clone(),
            wal_path: config.repair_wal_path.clone(),
            wal_max_bytes: config.repair_wal_max_bytes,
            max_peers: config.repair_max_peers,
            shred_version,
        };
        let repair_cluster_info = cluster_info.clone();
        let repair_metrics = metrics.clone();
        let repair_shutdown = shutdown_rx.clone();
        let supervisor = tokio::spawn(async move {
            // Repair is intentionally outside the receiver's terminal select. Any RPC, trust,
            // transport, or repair-WAL failure disables only this side path; raw forwarding must
            // remain live because it is the primary durability boundary.
            let worker = tokio::spawn(repair_service::run(
                repair_config,
                repair_cluster_info,
                repair_metrics.clone(),
                repair_rx,
                repair_shutdown.clone(),
            ));
            match worker.await {
                Ok(Ok(())) if *repair_shutdown.borrow() => {}
                Ok(Ok(())) => {
                    repair_metrics.record_repair_error();
                    warn!(
                        "bounded repair stopped unexpectedly; raw capture and forwarding continue"
                    );
                }
                Ok(Err(error)) => {
                    repair_metrics.record_repair_error();
                    warn!(%error, "bounded repair disabled; raw capture and forwarding continue");
                }
                Err(error) => {
                    repair_metrics.record_repair_error();
                    warn!(%error, "bounded repair task panicked; raw capture and forwarding continue");
                }
            }
            repair_metrics.mark_repair_inactive();
        });
        (Some(repair_tx), Some(supervisor))
    } else {
        (None, None)
    };
    let mut receiver_task = tokio::spawn(receive_and_forward_shreds(
        tvu_udp,
        shred_version,
        metrics.clone(),
        ReceivePipeline {
            sample_interval: config.sample_interval,
            forwarder,
            forward_queue_capacity: config.forward_queue_capacity,
            repair_observation_queue: repair_observation_tx,
        },
        shutdown_rx,
    ));

    let (finished_task, terminal_result) = tokio::select! {
        signal = shutdown_signal() => {
            if signal.is_ok() {
                info!("shutdown signal received");
            }
            (FinishedTask::None, signal)
        }
        result = &mut receiver_task => {
            (FinishedTask::Receiver, task_result("TVU receiver", result, true))
        }
        result = &mut metrics_task => {
            (FinishedTask::Metrics, task_result("metrics service", result, true))
        }
        result = &mut log_task => {
            (FinishedTask::MetricsLogger, task_result("metrics logger", result, true))
        }
    };

    let _ = shutdown_tx.send(true);
    exit.store(true, Ordering::Relaxed);

    let mut result = terminal_result;
    if finished_task != FinishedTask::Receiver {
        record_first_error(
            &mut result,
            task_result("TVU receiver", receiver_task.await, false),
        );
    }
    if finished_task != FinishedTask::Metrics {
        record_first_error(
            &mut result,
            task_result("metrics service", metrics_task.await, false),
        );
    }
    if finished_task != FinishedTask::MetricsLogger {
        record_first_error(
            &mut result,
            task_result("metrics logger", log_task.await, false),
        );
    }
    if let Some(mut repair_task) = repair_task
        && tokio::time::timeout(REPAIR_SHUTDOWN_TIMEOUT, &mut repair_task)
            .await
            .is_err()
    {
        repair_task.abort();
        let _ = repair_task.await;
        metrics.record_repair_error();
        metrics.mark_repair_inactive();
        warn!(
            timeout_seconds = REPAIR_SHUTDOWN_TIMEOUT.as_secs(),
            "timed out stopping bounded repair; raw capture was already stopped"
        );
    }

    let gossip_result = tokio::task::spawn_blocking(move || gossip_service.join())
        .await
        .context("gossip join task panicked")
        .and_then(|result| result.map_err(|_| anyhow!("a gossip thread panicked")));
    record_first_error(&mut result, gossip_result);
    let ip_echo_result = tokio::task::spawn_blocking(move || drop(ip_echo))
        .await
        .context("IP echo shutdown task panicked");
    record_first_error(&mut result, ip_echo_result);

    info!("shred-reader stopped");
    result
}

fn task_result(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
    unexpected_success: bool,
) -> Result<()> {
    match result {
        Ok(Ok(())) if unexpected_success => bail!("{name} stopped unexpectedly"),
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error).context(format!("{name} failed")),
        Err(error) => Err(error).context(format!("{name} task panicked")),
    }
}

fn record_first_error(result: &mut Result<()>, next: Result<()>) {
    if result.is_ok()
        && let Err(error) = next
    {
        *result = Err(error);
    }
}

fn resolve_entrypoints(entrypoints: &[String]) -> Result<Vec<SocketAddr>> {
    let mut resolved = Vec::with_capacity(entrypoints.len());
    let mut failures = Vec::new();
    for entrypoint in entrypoints {
        match entrypoint.to_socket_addrs() {
            Ok(mut addresses) => match addresses.find(SocketAddr::is_ipv4) {
                Some(address) if !resolved.contains(&address) => resolved.push(address),
                Some(_) => {}
                None => failures.push(format!("{entrypoint}: no IPv4 address")),
            },
            Err(error) => failures.push(format!("{entrypoint}: {error}")),
        }
    }

    if resolved.is_empty() {
        bail!(
            "failed to resolve any Solana entrypoint: {}",
            failures.join("; ")
        );
    }
    if !failures.is_empty() {
        warn!(failures = ?failures, "skipped unresolved Solana entrypoints");
    }
    Ok(resolved)
}

fn verify_public_reachability(
    entrypoints: &[SocketAddr],
    gossip_udp: &UdpSocket,
    gossip_tcp: &TcpListener,
    tvu_udp: &UdpSocket,
) -> Result<()> {
    let mut failures = Vec::new();
    for entrypoint in entrypoints {
        let tcp_reachable = verify_all_reachable_tcp(
            entrypoint,
            vec![
                gossip_tcp
                    .try_clone()
                    .context("failed to clone gossip TCP socket")?,
            ],
        );
        let udp_reachable = verify_all_reachable_udp(entrypoint, &[gossip_udp, tvu_udp]);
        if tcp_reachable && udp_reachable {
            info!(%entrypoint, "public gossip and TVU ports are reachable");
            return Ok(());
        }
        failures.push(format!(
            "{entrypoint}: tcp={tcp_reachable}, udp={udp_reachable}"
        ));
    }
    bail!(
        "public gossip/TVU reachability verification failed through all entrypoints: {}",
        failures.join("; ")
    )
}

fn bootstrap_network(
    entrypoints: &[SocketAddr],
    bind_ip: IpAddr,
    advertised_ip: Option<std::net::Ipv4Addr>,
    shred_version: Option<u16>,
) -> Result<(std::net::Ipv4Addr, u16)> {
    if shred_version == Some(0) {
        bail!("SHRED_VERSION must be nonzero");
    }
    if let (Some(advertised_ip), Some(shred_version)) = (advertised_ip, shred_version) {
        return Ok((advertised_ip, shred_version));
    }

    let mut failures = Vec::new();
    for entrypoint in entrypoints {
        let discovered_ip = match advertised_ip {
            Some(ip) => Ok(ip),
            None => get_public_ip_addr_with_binding(entrypoint, bind_ip)
                .map_err(anyhow::Error::from)
                .and_then(public_ipv4),
        };
        let discovered_version: Result<u16> = match shred_version {
            Some(version) => Ok(version),
            None => get_cluster_shred_version_with_binding(entrypoint, bind_ip)
                .map_err(anyhow::Error::from),
        }
        .and_then(|version| {
            if version == 0 {
                bail!("entrypoint returned invalid shred version 0");
            }
            Ok(version)
        });

        match (discovered_ip, discovered_version) {
            (Ok(ip), Ok(version)) => return Ok((ip, version)),
            (ip, version) => failures.push(format!(
                "{entrypoint}: public-ip={}, shred-version={}",
                ip.err()
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "ok".to_owned()),
                version
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "ok".to_owned())
            )),
        }
    }

    bail!(
        "failed to bootstrap Solana network through all entrypoints: {}",
        failures.join("; ")
    )
}

fn bind_tvu_socket(address: SocketAddr, requested_buffer: usize) -> Result<(UdpSocket, usize)> {
    let domain = Domain::for_address(address);
    let socket = Socket::new(domain, Type::DGRAM, Some(SocketProtocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(target_os = "linux")]
    if let Err(error) = crate::loss_telemetry::enable_socket_rxq_overflow(&socket) {
        warn!(%error, "kernel socket-overflow telemetry could not be enabled");
    }
    if let Err(error) = socket.set_recv_buffer_size(requested_buffer) {
        warn!(
            requested_buffer,
            %error,
            "kernel rejected requested UDP receive buffer; continuing with the system limit"
        );
    }
    socket.bind(&SockAddr::from(address))?;
    let effective_buffer = socket.recv_buffer_size()?;
    Ok((socket.into(), effective_buffer))
}

async fn receive_shreds(
    socket: tokio::net::UdpSocket,
    shred_version: u16,
    metrics: Arc<Metrics>,
    sample_interval: Duration,
    forward_queue: Option<mpsc::Sender<Arc<[u8]>>>,
    repair_observation_queue: Option<mpsc::Sender<Arc<[u8]>>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut buffer = [0u8; MAX_UDP_DATAGRAM_SIZE];
    let mut last_sample = Instant::now()
        .checked_sub(sample_interval)
        .unwrap_or_else(Instant::now);

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            received = socket.recv_from(&mut buffer) => {
                let (size, source) = received.context("TVU UDP receive failed")?;
                metrics.record_packet(size, source.ip());
                let Ok(shred) = Shred::new_from_serialized_shred(buffer[..size].to_vec()) else {
                    metrics.record_invalid();
                    continue;
                };
                if shred.version() != shred_version {
                    metrics.record_version_mismatch();
                    continue;
                }
                let _ = metrics.record_shred(shred.id(), shred.is_data());
                // Forwarding is deliberately independent from deduplication. UDP send success is
                // not a recorder acknowledgement, so every valid Turbine copy remains eligible to
                // recover an earlier kernel or recorder-queue loss. Every copy also reaches the
                // repair trust verifier: unauthenticated ShredId deduplication must never let a
                // forged preplay suppress a later, genuinely leader-signed Turbine packet.
                let forward_permit = forward_queue.as_ref().and_then(|queue| {
                    match queue.try_reserve() {
                        Ok(permit) => Some(permit),
                        Err(_) => {
                            metrics.record_forward_queue_drop();
                            None
                        }
                    }
                });
                let repair_permit = repair_observation_queue.as_ref().and_then(|queue| {
                    match queue.try_reserve() {
                        Ok(permit) => Some(permit),
                        Err(_) => {
                            metrics.record_repair_observation_queue_drop();
                            None
                        }
                    }
                });
                if forward_permit.is_some() || repair_permit.is_some() {
                    let payload: Arc<[u8]> = Arc::from(&buffer[..size]);
                    if let Some(permit) = forward_permit {
                        metrics.record_forward_queued();
                        permit.send(payload.clone());
                    }
                    if let Some(permit) = repair_permit {
                        permit.send(payload);
                    }
                }
                if last_sample.elapsed() >= sample_interval {
                    info!(
                        source = %source,
                        slot = shred.slot(),
                        index = shred.index(),
                        fec_set_index = shred.fec_set_index(),
                        shred_type = ?shred.shred_type(),
                        last_in_slot = shred.last_in_slot(),
                        "received shred sample"
                    );
                    last_sample = Instant::now();
                }
            }
        }
    }
}

async fn receive_and_forward_shreds(
    socket: tokio::net::UdpSocket,
    shred_version: u16,
    metrics: Arc<Metrics>,
    pipeline: ReceivePipeline,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let Some(forwarder) = pipeline.forwarder else {
        return receive_shreds(
            socket,
            shred_version,
            metrics,
            pipeline.sample_interval,
            None,
            pipeline.repair_observation_queue,
            shutdown,
        )
        .await;
    };

    let (forward_tx, mut forward_rx) = mpsc::channel::<Arc<[u8]>>(pipeline.forward_queue_capacity);
    let forward_metrics = metrics.clone();
    let mut forward_task = tokio::spawn(async move {
        while let Some(payload) = forward_rx.recv().await {
            forward_metrics.record_forward_dequeued();
            forwarder.forward(&payload, &forward_metrics).await;
        }
    });

    let receive_result = receive_shreds(
        socket,
        shred_version,
        metrics.clone(),
        pipeline.sample_interval,
        Some(forward_tx),
        pipeline.repair_observation_queue,
        shutdown,
    )
    .await;
    match tokio::time::timeout(FORWARD_DRAIN_TIMEOUT, &mut forward_task).await {
        Ok(result) => result.context("shred forwarding task panicked")?,
        Err(_) => {
            let queued_datagrams = metrics.forward_queue_depth();
            forward_task.abort();
            let _ = forward_task.await;
            warn!(
                queued_datagrams,
                timeout_seconds = FORWARD_DRAIN_TIMEOUT.as_secs(),
                "timed out draining shred forwarding queue during shutdown"
            );
        }
    }
    receive_result
}

async fn log_metrics(
    service: ServiceState,
    interval: Duration,
    loss_telemetry: LossTelemetry,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer.tick().await;
    let mut previous_loss = loss_telemetry.snapshot();

    info!(
        udp_in_errors_total = ?previous_loss.udp.in_errors,
        udp_no_ports_total = ?previous_loss.udp.no_ports,
        udp_receive_buffer_errors_total = ?previous_loss.udp.receive_buffer_errors,
        udp_checksum_errors_total = ?previous_loss.udp.checksum_errors,
        softnet_dropped_total = ?previous_loss.softnet.dropped,
        softnet_time_squeeze_total = ?previous_loss.softnet.time_squeeze,
        nic_receive_counters = ?previous_loss.interfaces,
        "network receive-loss baseline"
    );

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = timer.tick() => {
                let snapshot = service.metrics.snapshot(&service);
                let current_loss = loss_telemetry.snapshot();
                let loss_delta = current_loss.delta_since(&previous_loss);
                info!(
                    gossip_peers = snapshot.gossip_peers,
                    recent_gossip_peers = snapshot.recent_gossip_peers,
                    tvu_peers = snapshot.tvu_peers,
                    packets_total = snapshot.packets_total,
                    parsed_total = snapshot.parsed_total,
                    unique_total = snapshot.unique_total,
                    duplicates_total = snapshot.duplicates_total,
                    invalid_total = snapshot.invalid_total,
                    version_mismatch_total = snapshot.version_mismatch_total,
                    forward_targets = snapshot.forward_targets,
                    forwarded_datagrams_total = snapshot.forwarded_datagrams_total,
                    forward_errors_total = snapshot.forward_errors_total,
                    forward_send_errors_total = snapshot.forward_send_errors_total,
                    forward_queue_dropped_total = snapshot.forward_queue_dropped_total,
                    forward_queue_depth = snapshot.forward_queue_depth,
                    tracked_sources = snapshot.tracked_sources,
                    latest_slot = snapshot.latest_slot,
                    seconds_since_last_packet = ?snapshot.seconds_since_last_packet,
                    seconds_since_last_shred = ?snapshot.seconds_since_last_shred,
                    seconds_since_last_forward = ?snapshot.seconds_since_last_forward,
                    seconds_since_last_forward_error = ?snapshot.seconds_since_last_forward_error,
                    repair_enabled = snapshot.repair_enabled,
                    repair_active = snapshot.repair_active,
                    repair_peers = snapshot.repair_peers,
                    repair_tracked_slots = snapshot.repair_tracked_slots,
                    repair_outstanding = snapshot.repair_outstanding,
                    repair_observation_queue_dropped_total = snapshot.repair_observation_queue_dropped_total,
                    repair_requests_sent_total = snapshot.repair_requests_sent_total,
                    repair_retries_sent_total = snapshot.repair_retries_sent_total,
                    repair_requests_exhausted_total = snapshot.repair_requests_exhausted_total,
                    repair_requests_cooldown_deferred_total = snapshot.repair_requests_cooldown_deferred_total,
                    repair_packets_rejected_total = snapshot.repair_packets_rejected_total,
                    repair_pings_answered_total = snapshot.repair_pings_answered_total,
                    repair_shreds_accepted_total = snapshot.repair_shreds_accepted_total,
                    repair_root_anchored_shreds_accepted_total = snapshot.repair_root_anchored_shreds_accepted_total,
                    repair_wal_bytes_total = snapshot.repair_wal_bytes_total,
                    repair_wal_max_bytes = snapshot.repair_wal_max_bytes,
                    repair_wal_remaining_bytes = snapshot.repair_wal_remaining_bytes,
                    repair_errors_total = snapshot.repair_errors_total,
                    udp_in_errors_delta = ?loss_delta.udp.in_errors,
                    udp_no_ports_delta = ?loss_delta.udp.no_ports,
                    udp_receive_buffer_errors_delta = ?loss_delta.udp.receive_buffer_errors,
                    udp_checksum_errors_delta = ?loss_delta.udp.checksum_errors,
                    softnet_dropped_delta = ?loss_delta.softnet.dropped,
                    softnet_time_squeeze_delta = ?loss_delta.softnet.time_squeeze,
                    nic_receive_delta = ?loss_delta.interfaces,
                    "receiver metrics"
                );
                previous_loss = current_loss;
            }
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {},
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
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
    use solana_keypair::Keypair;

    fn signed_test_data_shred(slot: u64, version: u16) -> Vec<u8> {
        let leader = Keypair::new();
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x90; // MerkleData { proof_size: 0, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&0u32.to_le_bytes());
        payload[77..79].copy_from_slice(&version.to_le_bytes());
        payload[79..83].copy_from_slice(&0u32.to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0b1100_0000;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        let unsigned = Shred::new_from_serialized_shred(payload.clone()).unwrap();
        let signature = leader.sign_message(unsigned.merkle_root().unwrap().as_ref());
        payload[..64].copy_from_slice(signature.as_ref());
        payload
    }

    #[tokio::test]
    async fn forwards_exact_datagram_to_configured_destination() {
        let destination = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = destination.local_addr().unwrap();
        let forwarder = ShredForwarder::bind("127.0.0.1".parse().unwrap(), vec![target])
            .await
            .unwrap()
            .unwrap();
        forwarder.socket.writable().await.unwrap();
        let metrics = Metrics::new(1).unwrap();
        let expected = b"exact-shred-datagram";

        forwarder.forward(expected, &metrics).await;

        let mut received = [0u8; 64];
        let (size, _) =
            tokio::time::timeout(Duration::from_secs(1), destination.recv_from(&mut received))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&received[..size], expected);
    }

    #[tokio::test]
    async fn preserves_duplicate_forwarding_and_sends_every_copy_to_trust_verification() {
        let version = 50_093;
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = socket.local_addr().unwrap();
        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = Arc::new(Metrics::new(16).unwrap());
        let (forward_tx, mut forward_rx) = mpsc::channel(4);
        let (repair_tx, mut repair_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(receive_shreds(
            socket,
            version,
            metrics,
            Duration::from_secs(3_600),
            Some(forward_tx),
            Some(repair_tx),
            shutdown_rx,
        ));
        let payload = signed_test_data_shred(123, version);

        sender.send_to(&payload, receiver_addr).await.unwrap();
        sender.send_to(&payload, receiver_addr).await.unwrap();

        let forwarded_one = tokio::time::timeout(Duration::from_secs(1), forward_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let forwarded_two = tokio::time::timeout(Duration::from_secs(1), forward_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(forwarded_one.as_ref(), payload);
        assert_eq!(forwarded_two.as_ref(), payload);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), repair_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            payload
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), repair_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            payload,
            "unauthenticated deduplication must not suppress trust verification"
        );

        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn resolves_an_ipv4_entrypoint() {
        let resolved = resolve_entrypoints(&["127.0.0.1:8001".to_owned()]);
        assert!(resolved.unwrap().iter().all(SocketAddr::is_ipv4));
    }

    #[test]
    fn entrypoint_resolution_skips_failures() {
        let resolved = resolve_entrypoints(&[":".to_owned(), "127.0.0.1:8001".to_owned()]);
        assert_eq!(resolved.unwrap(), ["127.0.0.1:8001".parse().unwrap()]);
    }

    #[test]
    fn verifies_reachable_local_ports() {
        let echo_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let entrypoint = echo_listener.local_addr().unwrap();
        let echo = ip_echo_server(echo_listener, std::num::NonZeroUsize::MIN, Some(1));
        let gossip_udp = UdpSocket::bind("127.0.0.1:0").unwrap();
        let gossip_tcp = TcpListener::bind("127.0.0.1:0").unwrap();
        let tvu_udp = UdpSocket::bind("127.0.0.1:0").unwrap();

        let result = verify_public_reachability(&[entrypoint], &gossip_udp, &gossip_tcp, &tvu_udp);
        drop(echo);

        assert!(result.is_ok());
    }

    #[test]
    fn rejects_zero_shred_version_override() {
        let error = bootstrap_network(
            &["127.0.0.1:1".parse().unwrap()],
            "0.0.0.0".parse().unwrap(),
            Some("1.1.1.1".parse().unwrap()),
            Some(0),
        )
        .unwrap_err();
        assert!(error.to_string().contains("nonzero"));
    }

    #[test]
    fn rejects_zero_shred_version_from_entrypoint() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let entrypoint = listener.local_addr().unwrap();
        let echo = ip_echo_server(listener, std::num::NonZeroUsize::MIN, Some(0));

        let error = bootstrap_network(
            &[entrypoint],
            "0.0.0.0".parse().unwrap(),
            Some("1.1.1.1".parse().unwrap()),
            None,
        )
        .unwrap_err();
        drop(echo);

        assert!(error.to_string().contains("invalid shred version 0"));
    }
}
