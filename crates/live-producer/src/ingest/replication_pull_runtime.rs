//! File-backed, mTLS-only executable runtime for the durable raw pull source.
//!
//! Construction validates every immutable trust input and reserves the listener before opening
//! the mutable cumulative-ACK WAL. Serving always installs a client-certificate-verifying TLS
//! policy; the pull handler independently rechecks the verified leaf certificate and stream
//! allowlist on every RPC.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use anyhow::{Context, Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use tonic::transport::{Server, ServerTlsConfig};

use crate::grpc_raw::{GrpcRawCommittedReadLimits, GrpcRawLocalGcOutcome};

use super::{
    ClientCertificateAllowlist, CumulativePrimaryAck, Ed25519ReceiptKeyring, PullSourceGcResult,
    RawReplicationPullCommitObserver, RawReplicationPullLimits, RawReplicationPullService,
    RawReplicationPullSource, RawReplicationPullSourceConfig, load_mtls_server_material,
    mtls_only_server_tls_config,
};

pub const PULL_SOURCE_RUNTIME_SCHEMA_VERSION: u32 = 1;

/// Strict executable configuration for the standalone Dokploy pull-source service.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawReplicationPullRuntimeConfig {
    pub schema_version: u32,
    pub bind: String,
    pub tls: PullSourceServerTlsConfig,
    pub allowed_nodes_file: PathBuf,
    pub cache_root: PathBuf,
    pub cumulative_ack_wal_file: PathBuf,
    /// Non-authoritative monitoring snapshot. It is never read as cursor or deletion evidence.
    pub ack_status_file: PathBuf,
    pub expected_primary_id: String,
    pub trusted_receipt_keys: Vec<PullSourceTrustedReceiptKeyConfig>,
    pub gc: PullSourceGcConfig,
    pub limits: PullSourceRuntimeLimits,
}

impl fmt::Debug for RawReplicationPullRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullRuntimeConfig")
            .field("schema_version", &self.schema_version)
            .field("bind", &self.bind)
            .field("tls", &self.tls)
            .field("allowed_nodes_file", &"<redacted>")
            .field("cache_root", &"<redacted>")
            .field("cumulative_ack_wal_file", &"<redacted>")
            .field("ack_status_file", &"<redacted>")
            .field("expected_primary_id", &self.expected_primary_id)
            .field(
                "trusted_receipt_key_count",
                &self.trusted_receipt_keys.len(),
            )
            .field("gc", &self.gc)
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullSourceServerTlsConfig {
    pub server_certificate_file: PathBuf,
    pub server_private_key_file: PathBuf,
    pub client_ca_file: PathBuf,
    pub handshake_timeout_ms: u64,
}

impl fmt::Debug for PullSourceServerTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullSourceServerTlsConfig")
            .field("policy", &"mandatory mutual TLS")
            .field("credential_files", &"<redacted>")
            .field("handshake_timeout_ms", &self.handshake_timeout_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullSourceTrustedReceiptKeyConfig {
    pub key_id: String,
    pub public_key_file: PathBuf,
}

impl fmt::Debug for PullSourceTrustedReceiptKeyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullSourceTrustedReceiptKeyConfig")
            .field("key_id", &self.key_id)
            .field("public_key_file", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullSourceGcConfig {
    /// Disabled for the canary. When enabled, one oldest proof-covered generation may be retired
    /// only after CommitAck has crossed the local cumulative-ACK WAL fsync boundary.
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullSourceRuntimeLimits {
    pub max_records: usize,
    pub max_batch_compressed_bytes: u64,
    pub max_batch_uncompressed_bytes: u64,
    pub max_compressed_record_bytes: u64,
    pub max_uncompressed_record_bytes: u64,
}

/// Listener-owning mTLS runtime. The pre-bound socket makes port conflicts fail before the ACK WAL
/// is opened or initialized.
pub struct RawReplicationPullServerRuntime {
    bind: SocketAddr,
    listener: std::net::TcpListener,
    tls: ServerTlsConfig,
    service: RawReplicationPullService,
    gc_enabled: bool,
}

impl RawReplicationPullServerRuntime {
    pub fn from_config(config: &RawReplicationPullRuntimeConfig) -> Result<Self> {
        let bind = validate_runtime_config(config)?;

        let (server_identity, client_ca_root) = load_mtls_server_material(
            &config.tls.server_certificate_file,
            &config.tls.server_private_key_file,
            &config.tls.client_ca_file,
        )?;
        let tls = mtls_only_server_tls_config(
            server_identity,
            client_ca_root,
            Duration::from_millis(config.tls.handshake_timeout_ms),
        )?;
        // Parse all TLS inputs and prove that Tonic accepts the mandatory client-auth policy before
        // reserving a public socket or touching mutable source state.
        let _validated_tls = Server::builder()
            .tls_config(tls.clone())
            .context("validate pull-source mTLS policy")?;
        let allowlist = ClientCertificateAllowlist::load_json(&config.allowed_nodes_file)?;
        let keyring = load_trusted_receipt_keys(&config.trusted_receipt_keys)?;

        let listener = reserve_listener(bind)?;
        let bind = listener
            .local_addr()
            .context("inspect reserved pull-source listener")?;
        let status_publisher =
            Arc::new(PullAckStatusPublisher::new(config.ack_status_file.clone())?);

        // Listener reservation deliberately precedes ACK-WAL open/initialization.
        let source_config = RawReplicationPullSourceConfig::new(
            config.cache_root.clone(),
            config.cumulative_ack_wal_file.clone(),
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: config.limits.max_compressed_record_bytes,
                max_uncompressed_record_bytes: config.limits.max_uncompressed_record_bytes,
            },
            RawReplicationPullLimits {
                max_records: config.limits.max_records,
                max_compressed_bytes: config.limits.max_batch_compressed_bytes,
                max_uncompressed_bytes: config.limits.max_batch_uncompressed_bytes,
            },
            config.expected_primary_id.clone(),
            keyring,
        )
        .with_gc_enabled(config.gc.enabled);
        let mut source = RawReplicationPullSource::open(source_config)
            .map_err(|error| anyhow!("open durable pull source: {error}"))?;
        if config.gc.enabled {
            drain_startup_gc(&mut source);
        }
        let service = RawReplicationPullService::new(source, allowlist)
            .with_commit_observer(status_publisher);
        Ok(Self {
            bind,
            listener,
            tls,
            service,
            gc_enabled: config.gc.enabled,
        })
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind
    }

    pub fn gc_enabled(&self) -> bool {
        self.gc_enabled
    }

    /// Serve only mutually authenticated TLS connections and gracefully drain admitted RPCs after
    /// the shutdown future resolves.
    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let bind = self.bind;
        let listener = tokio::net::TcpListener::from_std(self.listener)
            .context("attach reserved pull-source listener to Tokio")?;
        let incoming = futures::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(Some((stream, listener)))
        });
        Server::builder()
            .tls_config(self.tls)
            .context("install mandatory pull-source mTLS policy")?
            .tcp_nodelay(true)
            .add_service(self.service.into_tonic_service())
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .with_context(|| format!("serve mTLS raw pull source on {bind}"))
    }
}

impl fmt::Debug for RawReplicationPullServerRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullServerRuntime")
            .field("bind", &self.bind)
            .field("tls", &"mandatory mutual TLS")
            .field("gc_enabled", &self.gc_enabled)
            .finish_non_exhaustive()
    }
}

fn validate_runtime_config(config: &RawReplicationPullRuntimeConfig) -> Result<SocketAddr> {
    ensure!(
        config.schema_version == PULL_SOURCE_RUNTIME_SCHEMA_VERSION,
        "unsupported pull-source runtime schema version"
    );
    let bind: SocketAddr = config
        .bind
        .parse()
        .context("parse pull-source bind address")?;
    ensure!(
        config.tls.handshake_timeout_ms > 0,
        "pull-source TLS handshake timeout must be nonzero"
    );
    ensure!(
        !config.trusted_receipt_keys.is_empty(),
        "pull-source must trust at least one Blockzilla receipt key"
    );
    validate_real_cache_root(&config.cache_root)?;
    for (label, path) in [
        ("server certificate", &config.tls.server_certificate_file),
        ("server private key", &config.tls.server_private_key_file),
        ("client CA", &config.tls.client_ca_file),
        ("client allowlist", &config.allowed_nodes_file),
        ("cumulative ACK WAL", &config.cumulative_ack_wal_file),
        ("ACK status snapshot", &config.ack_status_file),
    ] {
        validate_control_path(path, &config.cache_root, label)?;
    }
    for trusted in &config.trusted_receipt_keys {
        validate_control_path(
            &trusted.public_key_file,
            &config.cache_root,
            "trusted receipt public key",
        )?;
    }
    ensure!(
        config.ack_status_file != config.cumulative_ack_wal_file,
        "ACK status snapshot must be separate from the cumulative ACK WAL"
    );
    ensure!(
        config.limits.max_records > 0
            && config.limits.max_batch_compressed_bytes > 0
            && config.limits.max_batch_uncompressed_bytes > 0
            && config.limits.max_compressed_record_bytes > 0
            && config.limits.max_uncompressed_record_bytes > 0
            && config.limits.max_compressed_record_bytes
                <= config.limits.max_batch_compressed_bytes
            && config.limits.max_uncompressed_record_bytes
                <= config.limits.max_batch_uncompressed_bytes,
        "pull-source limits are invalid"
    );
    Ok(bind)
}

fn validate_real_cache_root(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute() && path != Path::new("/"),
        "pull-source cache root must be absolute and non-root"
    );
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect pull-source cache root {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "pull-source cache root must be a real directory"
    );
    Ok(())
}

fn validate_control_path(path: &Path, cache_root: &Path, label: &str) -> Result<()> {
    ensure!(
        path.is_absolute() && path != Path::new("/"),
        "{label} path must be absolute and non-root"
    );
    ensure!(
        !path.starts_with(cache_root),
        "{label} must be outside the pull-source cache root"
    );
    let canonical_cache = fs::canonicalize(cache_root).with_context(|| {
        format!(
            "canonicalize pull-source cache root {}",
            cache_root.display()
        )
    })?;
    if let Ok(canonical_path) = fs::canonicalize(path) {
        ensure!(
            !canonical_path.starts_with(&canonical_cache),
            "{label} resolves inside the pull-source cache root"
        );
    } else if let Some(parent) = path.parent()
        && let Ok(canonical_parent) = fs::canonicalize(parent)
    {
        ensure!(
            !canonical_parent.starts_with(&canonical_cache),
            "{label} parent resolves inside the pull-source cache root"
        );
    }
    Ok(())
}

fn load_trusted_receipt_keys(
    trusted: &[PullSourceTrustedReceiptKeyConfig],
) -> Result<Ed25519ReceiptKeyring> {
    let mut keyring = Ed25519ReceiptKeyring::new();
    for key in trusted {
        keyring
            .insert_spki_pem(&key.key_id, &key.public_key_file)
            .context("load trusted Blockzilla receipt verification key")?;
    }
    Ok(keyring)
}

fn reserve_listener(bind: SocketAddr) -> Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind(bind)
        .with_context(|| format!("bind mTLS pull-source listener on {bind}"))?;
    listener
        .set_nonblocking(true)
        .context("set pull-source listener nonblocking")?;
    Ok(listener)
}

fn drain_startup_gc(source: &mut RawReplicationPullSource) {
    drain_startup_gc_with(|| source.gc_one_acknowledged_generation());
}

fn drain_startup_gc_with<F>(mut next: F) -> usize
where
    F: FnMut() -> PullSourceGcResult,
{
    const PASSES_PER_FAIRNESS_BATCH: usize = 64;
    const MAX_FAIRNESS_BATCHES: usize = 64;

    let mut retired = 0usize;
    for batch in 0..MAX_FAIRNESS_BATCHES {
        for _ in 0..PASSES_PER_FAIRNESS_BATCH {
            match next() {
                PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Retired {
                    through_sequence,
                    ..
                }) => {
                    retired += 1;
                    tracing::info!(
                        through_sequence,
                        retired_generations = retired,
                        "startup retired an ACK-covered raw generation"
                    );
                }
                PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Busy) => {
                    tracing::debug!(
                        retired_generations = retired,
                        "startup retention is busy; caught-up pulls will retry"
                    );
                    return retired;
                }
                PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::NoDurableAck) => {
                    tracing::debug!(
                        retired_generations = retired,
                        "startup retention found no durable Blockzilla ACK"
                    );
                    return retired;
                }
                PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::AckDoesNotCoverOldest {
                    oldest_through_sequence,
                    durable_through_sequence,
                }) => {
                    tracing::debug!(
                        oldest_through_sequence,
                        durable_through_sequence,
                        retired_generations = retired,
                        "startup retention reached the first unacknowledged generation"
                    );
                    return retired;
                }
                PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::NothingToRetire) => {
                    tracing::debug!(
                        retired_generations = retired,
                        "startup retention is caught up"
                    );
                    return retired;
                }
                PullSourceGcResult::Disabled => return retired,
                PullSourceGcResult::Failed => {
                    tracing::warn!(
                        retired_generations = retired,
                        "startup retention maintenance failed; serving continues fail-safe"
                    );
                    return retired;
                }
            }
        }
        if batch + 1 < MAX_FAIRNESS_BATCHES {
            std::thread::yield_now();
        }
    }
    tracing::warn!(
        retired_generations = retired,
        "startup retention reached its fairness cap; caught-up pulls will continue maintenance"
    );
    retired
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PullAckStatus {
    schema_version: u32,
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
    journal_id: String,
    through_sequence: u64,
    primary_term: u64,
    updated_unix_secs: u64,
}

/// Atomic, fsynced monitoring publisher. The snapshot is explicitly downstream of the signed
/// ACK-WAL fsync and is never accepted as retention or replay authority.
struct PullAckStatusPublisher {
    path: PathBuf,
    temporary_sequence: AtomicU64,
}

impl PullAckStatusPublisher {
    fn new(path: PathBuf) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .context("ACK status file has no parent directory")?;
        let metadata =
            fs::symlink_metadata(parent).context("inspect ACK status parent directory")?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "ACK status parent must be a real directory"
        );
        match fs::symlink_metadata(&path) {
            Ok(metadata) => ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "ACK status path must be a regular non-symlink file"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect ACK status file"),
        }
        Ok(Self {
            path,
            temporary_sequence: AtomicU64::new(0),
        })
    }

    fn publish(&self, ack: &CumulativePrimaryAck) -> Result<()> {
        let updated_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_secs();
        let status = PullAckStatus {
            schema_version: 1,
            cluster_id: ack.stream.cluster_id.clone(),
            origin_node_id: ack.stream.origin_node_id.clone(),
            source_id: ack.stream.source_id.clone(),
            journal_id: lowercase_hex(&ack.stream.journal_id),
            through_sequence: ack.through_sequence,
            primary_term: ack.primary_term,
            updated_unix_secs,
        };
        self.publish_status(&status)
    }

    fn publish_status(&self, status: &PullAckStatus) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context("ACK status path lost its parent")?;
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .context("ACK status file name is not UTF-8")?;
        let sequence = self.temporary_sequence.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".{file_name}.tmp.{}.{}",
            std::process::id(),
            sequence
        ));
        let result = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
            let mut file = options
                .open(&temporary)
                .context("create ACK status temporary file")?;
            serde_json::to_writer(&mut file, status).context("encode ACK status snapshot")?;
            file.write_all(b"\n")?;
            file.sync_all().context("sync ACK status temporary file")?;
            drop(file);
            fs::rename(&temporary, &self.path).context("publish ACK status snapshot")?;
            File::open(parent)?
                .sync_all()
                .context("sync ACK status parent directory")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

impl fmt::Debug for PullAckStatusPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullAckStatusPublisher")
            .field("path", &"<redacted>")
            .finish()
    }
}

impl RawReplicationPullCommitObserver for PullAckStatusPublisher {
    fn after_durable_commit(&self, ack: &CumulativePrimaryAck) {
        if self.publish(ack).is_err() {
            // CommitAck is already durable. Monitoring failure must not cause the client to retry
            // an ACK whose source-side outcome is no longer ambiguous.
            tracing::warn!(
                through_sequence = ack.through_sequence,
                "durable pull ACK committed, but monitoring snapshot publication failed"
            );
        }
    }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn aligned_json(extra: &str) -> String {
        format!(
            r#"{{
                "schema_version": 1,
                "bind": "0.0.0.0:9443",
                "tls": {{
                    "server_certificate_file": "/run/secrets/hetzner_pull_source_certificate",
                    "server_private_key_file": "/tmp/blockzilla-pull-source/server-private-key.pem",
                    "client_ca_file": "/run/secrets/pull_client_ca",
                    "handshake_timeout_ms": 10000
                }},
                "allowed_nodes_file": "/run/secrets/pull_allowed_nodes",
                "cache_root": "/source-data/grpc-cache",
                "cumulative_ack_wal_file": "/control/pull-cumulative-ack.wal",
                "ack_status_file": "/control/pull-ack-status.json",
                "expected_primary_id": "blockzilla-primary",
                "trusted_receipt_keys": [{{
                    "key_id": "blockzilla-receiver-2026-07",
                    "public_key_file": "/run/secrets/blockzilla_receipt_public_key"
                }}],
                "gc": {{ "enabled": false }},
                "limits": {{
                    "max_records": 1,
                    "max_batch_compressed_bytes": 134217728,
                    "max_batch_uncompressed_bytes": 134217728,
                    "max_compressed_record_bytes": 134217728,
                    "max_uncompressed_record_bytes": 134217728
                }}
                {extra}
            }}"#
        )
    }

    #[test]
    fn strict_schema_matches_standalone_dokploy_paths() {
        let config: RawReplicationPullRuntimeConfig =
            serde_json::from_str(&aligned_json("")).expect("decode aligned config");
        assert_eq!(config.schema_version, PULL_SOURCE_RUNTIME_SCHEMA_VERSION);
        assert_eq!(config.bind, "0.0.0.0:9443");
        assert_eq!(config.cache_root, Path::new("/source-data/grpc-cache"));
        assert_eq!(
            config.cumulative_ack_wal_file,
            Path::new("/control/pull-cumulative-ack.wal")
        );
        assert!(!config.gc.enabled);
        assert_eq!(config.limits.max_records, 1);
    }

    #[test]
    fn schema_rejects_unknown_or_missing_tls_policy() {
        assert!(
            serde_json::from_str::<RawReplicationPullRuntimeConfig>(&aligned_json(
                ", \"client_cursor\": 42"
            ))
            .is_err()
        );
        let without_tls = aligned_json("").replace(
            r#""tls": {
                    "server_certificate_file": "/run/secrets/hetzner_pull_source_certificate",
                    "server_private_key_file": "/tmp/blockzilla-pull-source/server-private-key.pem",
                    "client_ca_file": "/run/secrets/pull_client_ca",
                    "handshake_timeout_ms": 10000
                },"#,
            "",
        );
        assert!(serde_json::from_str::<RawReplicationPullRuntimeConfig>(&without_tls).is_err());
    }

    #[test]
    fn config_debug_never_exposes_paths() {
        let config: RawReplicationPullRuntimeConfig =
            serde_json::from_str(&aligned_json("")).expect("decode aligned config");
        let debug = format!("{config:?}");
        for secret_path in [
            "/run/secrets/hetzner_pull_source_certificate",
            "/tmp/blockzilla-pull-source/server-private-key.pem",
            "/run/secrets/pull_client_ca",
            "/run/secrets/pull_allowed_nodes",
            "/run/secrets/blockzilla_receipt_public_key",
            "/control/pull-cumulative-ack.wal",
            "/control/pull-ack-status.json",
        ] {
            assert!(!debug.contains(secret_path));
        }
        assert!(debug.contains("mandatory mutual TLS"));
    }

    #[test]
    fn listener_is_reserved_exclusively_before_serving() {
        let first = reserve_listener("127.0.0.1:0".parse().unwrap()).expect("reserve listener");
        let address = first.local_addr().expect("inspect listener");
        assert!(reserve_listener(address).is_err());
        drop(first);
        assert!(reserve_listener(address).is_ok());
    }

    #[test]
    fn startup_gc_repeats_retired_generations_until_a_safe_terminal_outcome() {
        let mut outcomes = VecDeque::from([
            PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Retired {
                generation: PathBuf::from("redacted-one"),
                through_sequence: 10,
            }),
            PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Retired {
                generation: PathBuf::from("redacted-two"),
                through_sequence: 20,
            }),
            PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::AckDoesNotCoverOldest {
                oldest_through_sequence: 30,
                durable_through_sequence: 20,
            }),
        ]);
        let retired = drain_startup_gc_with(|| outcomes.pop_front().expect("bounded outcome"));
        assert_eq!(retired, 2);
        assert!(outcomes.is_empty());

        let mut calls = 0;
        let retired = drain_startup_gc_with(|| {
            calls += 1;
            PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Busy)
        });
        assert_eq!(retired, 0);
        assert_eq!(calls, 1);
    }

    #[test]
    fn post_commit_status_is_atomic_fsynced_and_monitor_compatible() {
        use crate::ingest::{
            ContentDigest, REPLICATION_PROTOCOL_VERSION, ReceiptDisposition, ReplicationStreamId,
        };

        let root = std::env::temp_dir().join(format!(
            "blockzilla-pull-status-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let status_path = root.join("pull-ack-status.json");
        let publisher = PullAckStatusPublisher::new(status_path.clone()).unwrap();
        let ack = CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: ReplicationStreamId {
                cluster_id: "mainnet-beta".into(),
                origin_node_id: "hetzner-relay".into(),
                source_id: "yellowstone-blocks".into(),
                journal_id: [0xab; 16],
            },
            primary_id: "blockzilla-primary".into(),
            primary_term: 7,
            through_sequence: 42,
            through_content_digest: ContentDigest([3; 32]),
            rolling_chain_digest: ContentDigest([4; 32]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 43,
            signing_key_id: "test-key".into(),
            signature: vec![5; 64],
        };

        // The production service invokes this observer only after source.commit_ack returns from
        // its ACK-WAL fsync. Before that callback there is no monitoring snapshot.
        assert!(!status_path.exists());
        publisher.after_durable_commit(&ack);
        let status: PullAckStatus =
            serde_json::from_slice(&fs::read(&status_path).unwrap()).unwrap();
        assert_eq!(status.schema_version, 1);
        assert_eq!(status.journal_id, "abababababababababababababababab");
        assert_eq!(status.through_sequence, 42);
        assert_eq!(status.primary_term, 7);
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp.")
        }));
        fs::remove_dir_all(root).unwrap();
    }
}
