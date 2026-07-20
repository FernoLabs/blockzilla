//! Minimal durable Solana shred UDP source.
//!
//! The adapter preserves each accepted UDP datagram in an independently compressed, lossless
//! ingress frame. Transport duplicates remain distinct observations while sharing a logical shred
//! key. The compressed representation is what is replicated to a remote durable spool.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tokio::net::UdpSocket;

use super::{
    IngestConfig, IngestRoleConfig, IngressRecordMeta, LogicalKey, ObservationId, ShredKind,
    SourceInputConfig, SpoolFullPolicy, SpoolJournalIdentity, SpoolOptions, SpoolWriter,
};

/// Uncompressed, byte-for-byte Solana shred datagram.
pub const RAW_SOLANA_SHRED_V1: u16 = 3;
/// Independently zstd-compressed, byte-for-byte Solana shred datagram.
pub const ZSTD_SOLANA_SHRED_V1: u16 = 4;
const COMMON_SHRED_HEADER_BYTES: usize = 83;
const SHRED_VARIANT_OFFSET: usize = 64;
const SLOT_OFFSET: usize = 65;
const INDEX_OFFSET: usize = 73;
const VERSION_OFFSET: usize = 77;
const FEC_SET_INDEX_OFFSET: usize = 79;
const MAX_UDP_DATAGRAM_BYTES: usize = 65_535;
const QUOTA_ENTRY_MIN_BYTES: u64 = 4_096;
const STATUS_SCHEMA_VERSION: u32 = 1;
const STATUS_INTERVAL: Duration = Duration::from_secs(5);
const RECEIVING_FRESHNESS: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct ShredUdpRecordConfig {
    pub ingest: IngestConfig,
    pub source_id: String,
    pub journal_id: [u8; 16],
    pub status_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ShredRecorderState {
    Waiting,
    Receiving,
    Stalled,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ShredRecorderStatus {
    schema_version: u32,
    updated_unix_secs: u64,
    started_unix_secs: u64,
    state: ShredRecorderState,
    accepted_total: u64,
    invalid_total: u64,
    bytes_total: u64,
    durable_through_sequence: Option<u64>,
    latest_slot: Option<u64>,
    shred_version: Option<u16>,
    last_durable_unix_secs: Option<u64>,
    spool_bytes: u64,
    spool_max_bytes: u64,
    filesystem_free_bytes: u64,
    filesystem_total_bytes: u64,
    reserve_free_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParsedShredHeader {
    pub(crate) slot: u64,
    pub(crate) index: u32,
    pub(crate) version: u16,
    pub(crate) fec_set_index: u32,
    pub(crate) kind: ShredKind,
}

pub async fn record_shred_udp(config: ShredUdpRecordConfig) -> Result<()> {
    ensure!(
        matches!(config.ingest.spool.full_policy, SpoolFullPolicy::FailClosed),
        "record-shred-udp currently requires spool.full_policy=fail_closed"
    );
    ensure!(
        config
            .ingest
            .sources
            .iter()
            .filter(|source| source.enabled)
            .count()
            == 1,
        "record-shred-udp currently requires exactly one enabled source per spool root"
    );
    if let Some(status_file) = config.status_file.as_deref() {
        ensure!(
            status_file.is_absolute() && status_file != Path::new("/"),
            "record-shred-udp status file must be an absolute non-root path"
        );
        ensure!(
            !status_file.starts_with(&config.ingest.spool.root),
            "record-shred-udp status file must live outside the quota-accounted spool root"
        );
    }
    let source = config
        .ingest
        .sources
        .iter()
        .find(|source| source.id == config.source_id)
        .with_context(|| format!("shred UDP source {:?} is not configured", config.source_id))?;
    ensure!(
        source.enabled,
        "shred UDP source {:?} is disabled",
        source.id
    );
    let SourceInputConfig::ShredUdp {
        bind,
        multicast_group,
        interface,
        auth,
    } = &source.input
    else {
        anyhow::bail!("source {:?} is not a shred_udp input", source.id);
    };
    ensure!(
        auth.is_none(),
        "raw shred UDP recording currently requires auth=null; authenticated envelopes are not yet implemented"
    );

    let origin_node_id = match &config.ingest.role {
        IngestRoleConfig::Primary { node_id, .. } | IngestRoleConfig::Replica { node_id, .. } => {
            node_id.clone()
        }
    };
    let identity = SpoolJournalIdentity {
        cluster_id: config.ingest.cluster_id.clone(),
        origin_node_id: origin_node_id.clone(),
        source_id: source.id.clone(),
        journal_id: config.journal_id,
    };
    let options = SpoolOptions {
        segment_target_bytes: config.ingest.spool.segment_bytes,
        max_record_bytes: source.queue.max_event_bytes,
    };
    let mut spool = SpoolWriter::open(&config.ingest.spool.root, identity, options)
        .context("open shred UDP ingress spool")?;
    let mut spool_bytes = spool_root_bytes(&config.ingest.spool.root)?;
    let mut next_sequence = spool.last_record().map_or(Ok(0), |record| {
        record
            .metadata()
            .observation
            .sequence
            .checked_add(1)
            .context("shred UDP observation sequence exhausted")
    })?;
    let recovered_header = spool
        .last_record()
        .map(|record| spool.read_record(record))
        .transpose()
        .context("read last recovered shred UDP observation")?
        .and_then(|record| decode_stored_shred(&record.payload).ok())
        .and_then(|payload| parse_shred_header(&payload));

    let bind_address: SocketAddr = bind.parse().context("parse shred UDP bind address")?;
    let socket = UdpSocket::bind(bind_address)
        .await
        .with_context(|| format!("bind shred UDP source at {bind_address}"))?;
    join_multicast(&socket, multicast_group.as_deref(), interface.as_deref())?;

    tracing::info!(
        source_id = %source.id,
        %bind_address,
        journal_id = %hex_journal_id(config.journal_id),
        next_sequence,
        "shred UDP durable recorder started"
    );

    let mut buffer = vec![0u8; MAX_UDP_DATAGRAM_BYTES];
    let mut accepted = 0u64;
    let mut invalid = 0u64;
    let mut bytes = 0u64;
    let started_unix_secs = unix_time_secs()?;
    let mut latest_slot = recovered_header.map(|header| header.slot);
    let mut shred_version = recovered_header.map(|header| header.version);
    let mut last_durable_unix_secs: Option<u64> = None;
    let mut last_durable_at: Option<Instant> = None;
    let mut last_report = Instant::now();
    publish_recorder_status(
        config.status_file.as_deref(),
        &config.ingest,
        ShredRecorderStatusInput {
            started_unix_secs,
            state: ShredRecorderState::Waiting,
            accepted_total: accepted,
            invalid_total: invalid,
            bytes_total: bytes,
            next_sequence,
            latest_slot,
            shred_version,
            last_durable_unix_secs,
            spool_bytes,
        },
    )?;
    let mut status_interval = tokio::time::interval(STATUS_INTERVAL);
    status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    status_interval.tick().await;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        let received = tokio::select! {
            received = socket.recv_from(&mut buffer) => Some(received.context("receive shred UDP")?),
            _ = status_interval.tick() => {
                let state = match last_durable_at {
                    None => ShredRecorderState::Waiting,
                    Some(last) if last.elapsed() <= RECEIVING_FRESHNESS => ShredRecorderState::Receiving,
                    Some(_) => ShredRecorderState::Stalled,
                };
                if let Err(error) = publish_recorder_status(
                    config.status_file.as_deref(),
                    &config.ingest,
                    ShredRecorderStatusInput {
                        started_unix_secs,
                        state,
                        accepted_total: accepted,
                        invalid_total: invalid,
                        bytes_total: bytes,
                        next_sequence,
                        latest_slot,
                        shred_version,
                        last_durable_unix_secs,
                        spool_bytes,
                    },
                ) {
                    tracing::warn!(error = %format!("{error:#}"), "publish shred UDP recorder status");
                }
                None
            }
            () = &mut shutdown => {
                if let Err(error) = publish_recorder_status(
                    config.status_file.as_deref(),
                    &config.ingest,
                    ShredRecorderStatusInput {
                        started_unix_secs,
                        state: ShredRecorderState::Stopped,
                        accepted_total: accepted,
                        invalid_total: invalid,
                        bytes_total: bytes,
                        next_sequence,
                        latest_slot,
                        shred_version,
                        last_durable_unix_secs,
                        spool_bytes,
                    },
                ) {
                    tracing::warn!(error = %format!("{error:#}"), "publish stopped shred UDP recorder status");
                }
                tracing::info!(
                    source_id = %source.id,
                    accepted_total = accepted,
                    invalid_total = invalid,
                    bytes_total = bytes,
                    "shred UDP durable recorder stopped cleanly"
                );
                return Ok(());
            }
        };
        let Some((length, peer)) = received else {
            continue;
        };
        if length as u64 > source.queue.max_event_bytes {
            invalid = invalid.saturating_add(1);
            continue;
        }
        let datagram = &buffer[..length];
        let Some(header) = parse_shred_header(datagram) else {
            invalid = invalid.saturating_add(1);
            continue;
        };
        let payload = zstd::bulk::compress(datagram, 1)
            .context("independently compress shred datagram for durable spool")?;
        ensure!(
            payload.len() as u64 <= source.queue.max_event_bytes,
            "compressed shred datagram exceeds configured source maximum"
        );
        let metadata = IngressRecordMeta::from_payload(
            config.ingest.cluster_id.clone(),
            ObservationId {
                origin_node_id: origin_node_id.clone(),
                journal_id: config.journal_id,
                sequence: next_sequence,
            },
            source.id.clone(),
            LogicalKey::Shred {
                slot: header.slot,
                kind: header.kind,
                shred_index: header.index,
                fec_set_index: Some(header.fec_set_index),
            },
            ZSTD_SOLANA_SHRED_V1,
            &payload,
        );
        let projected = spool.project_append(&metadata, &payload)?;
        ensure!(
            spool_bytes
                .checked_add(projected.additional_bytes)
                .is_some_and(|total| total <= config.ingest.spool.max_bytes),
            "shred UDP spool capacity would be exceeded"
        );
        let available_bytes = filesystem_available_bytes(&config.ingest.spool.root)?;
        ensure!(
            config
                .ingest
                .spool
                .reserve_free_bytes
                .checked_add(projected.additional_bytes)
                .is_some_and(|required| available_bytes >= required),
            "shred UDP filesystem reserve would be crossed"
        );
        spool
            .append_and_sync(metadata, &payload)
            .context("durably append shred UDP observation")?;
        spool_bytes = spool_bytes
            .checked_add(projected.additional_bytes)
            .context("shred UDP spool byte accounting overflow")?;
        next_sequence = next_sequence
            .checked_add(1)
            .context("shred UDP observation sequence exhausted")?;
        accepted = accepted.saturating_add(1);
        bytes = bytes.saturating_add(length as u64);
        latest_slot = Some(header.slot);
        shred_version = Some(header.version);
        last_durable_unix_secs = Some(unix_time_secs()?);
        last_durable_at = Some(Instant::now());

        if last_report.elapsed() >= Duration::from_secs(10) {
            tracing::info!(
                source_id = %source.id,
                accepted_total = accepted,
                invalid_total = invalid,
                bytes_total = bytes,
                latest_slot = header.slot,
                shred_version = header.version,
                last_peer = %peer,
                "shred UDP recorder metrics"
            );
            last_report = Instant::now();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ShredRecorderStatusInput {
    started_unix_secs: u64,
    state: ShredRecorderState,
    accepted_total: u64,
    invalid_total: u64,
    bytes_total: u64,
    next_sequence: u64,
    latest_slot: Option<u64>,
    shred_version: Option<u16>,
    last_durable_unix_secs: Option<u64>,
    spool_bytes: u64,
}

fn publish_recorder_status(
    path: Option<&Path>,
    ingest: &IngestConfig,
    input: ShredRecorderStatusInput,
) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let (filesystem_free_bytes, filesystem_total_bytes) =
        filesystem_capacity_bytes(&ingest.spool.root)?;
    let status = ShredRecorderStatus {
        schema_version: STATUS_SCHEMA_VERSION,
        updated_unix_secs: unix_time_secs()?,
        started_unix_secs: input.started_unix_secs,
        state: input.state,
        accepted_total: input.accepted_total,
        invalid_total: input.invalid_total,
        bytes_total: input.bytes_total,
        durable_through_sequence: input.next_sequence.checked_sub(1),
        latest_slot: input.latest_slot,
        shred_version: input.shred_version,
        last_durable_unix_secs: input.last_durable_unix_secs,
        spool_bytes: input.spool_bytes,
        spool_max_bytes: ingest.spool.max_bytes,
        filesystem_free_bytes,
        filesystem_total_bytes,
        reserve_free_bytes: ingest.spool.reserve_free_bytes,
    };
    write_json_atomic(path, &status)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("shred status file has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create shred status directory {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("shred status file has an invalid name")?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos();
    let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("create shred status temp {}", temporary.display()))?;
        serde_json::to_writer(&mut file, value).context("encode shred recorder status")?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "publish shred recorder status {} from {}",
                path.display(),
                temporary.display()
            )
        })?;
        sync_parent_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn unix_time_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open shred status directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync shred status directory {}", path.display()))
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let terminate = signal(SignalKind::terminate());
    let Ok(mut terminate) = terminate else {
        let _ = tokio::signal::ctrl_c().await;
        return;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate.recv() => {},
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn spool_root_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in
        fs::read_dir(path).with_context(|| format!("list spool root {}", path.display()))?
    {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path)
            .with_context(|| format!("inspect spool entry {}", entry_path.display()))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "shred UDP spool contains a symbolic link: {}",
            entry_path.display()
        );
        let entry_bytes = if metadata.is_dir() {
            QUOTA_ENTRY_MIN_BYTES
                .checked_add(spool_root_bytes(&entry_path)?)
                .context("shred UDP spool byte count overflow")?
        } else {
            ensure!(
                metadata.is_file(),
                "shred UDP spool contains a non-regular entry: {}",
                entry_path.display()
            );
            metadata.len().max(QUOTA_ENTRY_MIN_BYTES)
        };
        total = total
            .checked_add(entry_bytes)
            .context("shred UDP spool byte count overflow")?;
    }
    Ok(total)
}

#[cfg(unix)]
fn filesystem_capacity_bytes(path: &Path) -> Result<(u64, u64)> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("shred UDP spool path contains NUL: {}", path.display()))?;
    // SAFETY: stat is writable storage and path remains a valid NUL-terminated string.
    let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
    let result = unsafe { libc::statvfs(path.as_ptr(), &mut stat) };
    if result != 0 {
        return Err(io::Error::last_os_error()).context("read shred UDP filesystem free space");
    }
    let available = (stat.f_bavail as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("shred UDP filesystem available byte count overflow")?;
    let total = (stat.f_blocks as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("shred UDP filesystem total byte count overflow")?;
    Ok((available, total))
}

#[cfg(not(unix))]
fn filesystem_capacity_bytes(_path: &Path) -> Result<(u64, u64)> {
    Ok((u64::MAX, u64::MAX))
}

fn filesystem_available_bytes(path: &Path) -> Result<u64> {
    filesystem_capacity_bytes(path).map(|(available, _)| available)
}

pub(crate) fn decode_stored_shred(payload: &[u8]) -> Result<Vec<u8>> {
    zstd::bulk::decompress(payload, MAX_UDP_DATAGRAM_BYTES)
        .context("decompress stored shred datagram")
}

pub(crate) fn parse_shred_header(payload: &[u8]) -> Option<ParsedShredHeader> {
    if payload.len() < COMMON_SHRED_HEADER_BYTES {
        return None;
    }
    let kind = match payload[SHRED_VARIANT_OFFSET] & 0xf0 {
        0x60 | 0x70 => ShredKind::Coding,
        0x90 | 0xb0 => ShredKind::Data,
        _ => return None,
    };
    Some(ParsedShredHeader {
        slot: u64::from_le_bytes(payload[SLOT_OFFSET..SLOT_OFFSET + 8].try_into().ok()?),
        index: u32::from_le_bytes(payload[INDEX_OFFSET..INDEX_OFFSET + 4].try_into().ok()?),
        version: u16::from_le_bytes(
            payload[VERSION_OFFSET..VERSION_OFFSET + 2]
                .try_into()
                .ok()?,
        ),
        fec_set_index: u32::from_le_bytes(
            payload[FEC_SET_INDEX_OFFSET..FEC_SET_INDEX_OFFSET + 4]
                .try_into()
                .ok()?,
        ),
        kind,
    })
}

fn join_multicast(socket: &UdpSocket, group: Option<&str>, interface: Option<&str>) -> Result<()> {
    let Some(group) = group else {
        return Ok(());
    };
    let group: IpAddr = group.parse().context("parse shred UDP multicast group")?;
    let interface: IpAddr = interface
        .context("shred UDP multicast interface is required")?
        .parse()
        .context("parse shred UDP multicast interface")?;
    match (group, interface) {
        (IpAddr::V4(group), IpAddr::V4(interface)) => socket
            .join_multicast_v4(group, interface)
            .context("join IPv4 shred UDP multicast group"),
        (IpAddr::V6(_), IpAddr::V6(_)) => {
            anyhow::bail!("IPv6 shred UDP multicast requires an interface index and is unsupported")
        }
        _ => anyhow::bail!("shred UDP multicast group and interface address families differ"),
    }
}

fn hex_journal_id(journal_id: [u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in journal_id {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "blockzilla-shred-status-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock")
                .as_nanos()
        ))
    }

    #[test]
    fn parses_data_and_coding_shred_coordinates() {
        for (variant, kind) in [(0x90, ShredKind::Data), (0x6f, ShredKind::Coding)] {
            let mut payload = [0u8; COMMON_SHRED_HEADER_BYTES];
            payload[SHRED_VARIANT_OFFSET] = variant;
            payload[SLOT_OFFSET..SLOT_OFFSET + 8].copy_from_slice(&42u64.to_le_bytes());
            payload[INDEX_OFFSET..INDEX_OFFSET + 4].copy_from_slice(&7u32.to_le_bytes());
            payload[VERSION_OFFSET..VERSION_OFFSET + 2].copy_from_slice(&50093u16.to_le_bytes());
            payload[FEC_SET_INDEX_OFFSET..FEC_SET_INDEX_OFFSET + 4]
                .copy_from_slice(&3u32.to_le_bytes());

            assert_eq!(
                parse_shred_header(&payload),
                Some(ParsedShredHeader {
                    slot: 42,
                    index: 7,
                    version: 50093,
                    fec_set_index: 3,
                    kind,
                })
            );
        }
    }

    #[test]
    fn rejects_short_or_unknown_shreds() {
        assert_eq!(parse_shred_header(&[0; 82]), None);
        assert_eq!(parse_shred_header(&[0; COMMON_SHRED_HEADER_BYTES]), None);
    }

    #[test]
    fn atomically_publishes_only_the_recorder_status_contract() {
        let directory = unique_temp_dir();
        let path = directory.join("recorder.json");
        let status = ShredRecorderStatus {
            schema_version: STATUS_SCHEMA_VERSION,
            updated_unix_secs: 20,
            started_unix_secs: 10,
            state: ShredRecorderState::Receiving,
            accepted_total: 8,
            invalid_total: 1,
            bytes_total: 9_600,
            durable_through_sequence: Some(7),
            latest_slot: Some(42),
            shred_version: Some(50_093),
            last_durable_unix_secs: Some(20),
            spool_bytes: 12_345,
            spool_max_bytes: 20_000,
            filesystem_free_bytes: 30_000,
            filesystem_total_bytes: 40_000,
            reserve_free_bytes: 5_000,
        };

        write_json_atomic(&path, &status).expect("publish status");
        let raw = fs::read_to_string(&path).expect("read status");
        let actual: serde_json::Value = serde_json::from_str(&raw).expect("decode status");
        assert_eq!(actual["state"], "receiving");
        assert_eq!(actual["durable_through_sequence"], 7);
        assert_eq!(actual.as_object().expect("object").len(), 16);
        for forbidden in ["bind", "peer", "source_id", "journal_id", "token", "secret"] {
            assert!(!raw.contains(forbidden));
        }

        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
