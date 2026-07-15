//! mTLS-only Tonic runtime for durable raw-stream replication.
//!
//! The transport boundary deliberately has no bearer-token or plaintext authentication path.
//! Tonic must be built with its `tls-ring` feature, and callers must install the result of
//! [`mtls_only_server_tls_config`] on the server builder. Every RPC also fails closed unless
//! Tonic exposes a verified client certificate on the request.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{ErrorKind, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt},
};

use anyhow::{Context, Result, anyhow, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};
use zeroize::Zeroize;

use super::{
    BlockzillaRawReceiver, BoundedPushBatchBuilder, CumulativePrimaryAck, Ed25519ReceiptSigner,
    GetAckState, IngestConfig, IngestRoleConfig, PrimaryTermGuard, PrimaryTermStore,
    REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES, RawReceiverConfig, RawReceiverLimits,
    RawReplicationRecord, ReplicationStreamId, ReplicationWireError, SecretRef, SpoolOptions,
    ValidatedPushBatch, ValidatedPushRecord, ensure_receiver_spool_root_durable,
    validate_raw_replication_batch, wire,
};

const ALLOWLIST_SCHEMA_VERSION: u32 = 1;
const MAX_ALLOWLIST_BYTES: u64 = 256 * 1024;
const MAX_ALLOWLIST_CLIENTS: usize = 256;
const MAX_IDENTITIES_PER_CLIENT: usize = 64;
const CERTIFICATE_FINGERPRINT_BYTES: usize = 32;
const MAX_DECODING_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
const MAX_ENCODING_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_KNOWN_STREAMS: usize = 65_536;
const KNOWN_STREAMS_LOCK_FILE: &str = ".known-streams.lock";
const RECEIVER_CAPACITY_LOCK_FILE: &str = ".receiver-capacity.lock";
const MAX_TLS_CERTIFICATE_FILE_BYTES: u64 = 1024 * 1024;
const MAX_TLS_PRIVATE_KEY_FILE_BYTES: u64 = 64 * 1024;
const MAX_RECEIVER_CONFIG_FILE_BYTES: u64 = 1024 * 1024;
pub const REPLICATION_RECEIVER_SPOOL_DIRECTORY: &str = "replication-receiver";

/// Runtime limits which are enforced independently of HTTP/2 flow control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationServiceLimits {
    pub max_open_streams: usize,
    /// Persistent depth-4 journal-directory cap. Unlike max_open_streams, this bounds inodes and
    /// survives process restarts. This is an operator-managed global cap shared by every
    /// allowlisted identity; certificates which rotate journals excessively must be revoked.
    pub max_known_streams: usize,
    pub max_concurrent_requests: usize,
    pub max_decoding_message_bytes: usize,
    pub stream_idle_timeout: Duration,
    pub stream_total_timeout: Duration,
}

impl ReplicationServiceLimits {
    fn validate(self, receiver_limits: RawReceiverLimits) -> Result<Self> {
        ensure!(
            self.max_open_streams > 0,
            "maximum open streams must be nonzero"
        );
        ensure!(
            self.max_known_streams >= self.max_open_streams,
            "maximum known streams must cover maximum open streams"
        );
        ensure!(
            self.max_known_streams <= MAX_KNOWN_STREAMS,
            "maximum known streams exceeds the hard safety bound"
        );
        ensure!(
            self.max_concurrent_requests > 0,
            "maximum concurrent requests must be nonzero"
        );
        ensure!(
            !self.stream_idle_timeout.is_zero(),
            "stream idle timeout must be nonzero"
        );
        ensure!(
            !self.stream_total_timeout.is_zero(),
            "stream total timeout must be nonzero"
        );
        ensure!(
            self.stream_total_timeout >= self.stream_idle_timeout,
            "stream total timeout must not be shorter than its idle timeout"
        );
        let record_bytes = usize::try_from(receiver_limits.max_compressed_record_bytes)
            .context("compressed record limit does not fit this platform")?;
        let envelope_reserve = usize::try_from(REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES)
            .context("wire envelope reserve does not fit this platform")?;
        let minimum_decoding_bytes = record_bytes
            .checked_add(envelope_reserve)
            .context("maximum decoding message size overflow")?;
        ensure!(
            self.max_decoding_message_bytes >= minimum_decoding_bytes,
            "maximum decoding message size must cover one record plus its bounded envelope"
        );
        ensure!(
            self.max_decoding_message_bytes <= MAX_DECODING_MESSAGE_BYTES,
            "maximum decoding message size exceeds the hard safety bound"
        );
        // Reuse the wire builder's complete receiver-limit validation without retaining data.
        BoundedPushBatchBuilder::new(receiver_limits)
            .map_err(|error| anyhow!("invalid receiver limits: {error}"))?;
        Ok(self)
    }
}

/// Common receiver settings shared by every authenticated replication stream.
#[derive(Debug, Clone)]
pub struct ReplicationReceiverManagerConfig {
    pub spool_root: PathBuf,
    pub primary_id: String,
    pub spool_options: SpoolOptions,
    pub max_spool_bytes: u64,
    pub reserve_free_bytes: u64,
    pub receiver_limits: RawReceiverLimits,
}

impl ReplicationReceiverManagerConfig {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.spool_root.is_absolute(),
            "receiver spool root must be an absolute path"
        );
        ensure!(
            is_stable_id(&self.primary_id),
            "receiver primary id is not a stable identifier"
        );
        self.spool_options.validate()?;
        BoundedPushBatchBuilder::new(self.receiver_limits)
            .map_err(|error| anyhow!("invalid receiver limits: {error}"))?;
        ensure!(
            self.max_spool_bytes >= self.spool_options.segment_target_bytes,
            "maximum spool bytes must cover one target segment"
        );
        ensure!(
            self.receiver_limits.max_compressed_record_bytes <= self.spool_options.max_record_bytes,
            "receiver record limit exceeds spool record limit"
        );
        match fs::symlink_metadata(&self.spool_root) {
            Ok(_) => validate_real_directory(&self.spool_root, "receiver spool root")?,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect receiver spool root"),
        }
        Ok(self)
    }
}

/// Immutable client-certificate authorization policy.
///
/// A certificate fingerprint may publish only the exact
/// `(cluster_id, origin_node_id, source_id)` tuples in its entry. Journal rotation remains free
/// inside that stable authenticated namespace.
#[derive(Clone)]
pub struct ClientCertificateAllowlist {
    clients: Arc<HashMap<[u8; CERTIFICATE_FINGERPRINT_BYTES], Arc<HashSet<AllowedIdentity>>>>,
}

impl ClientCertificateAllowlist {
    /// Load a bounded, strict JSON policy from a non-symlink regular file.
    pub fn load_json(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = read_secure_allowlist(path)?;
        Self::from_json_bytes(&bytes)
            .with_context(|| format!("validate replication client allowlist {}", path.display()))
    }

    fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() as u64 <= MAX_ALLOWLIST_BYTES,
            "replication client allowlist exceeds the size limit"
        );
        let decoded: AllowlistFile =
            serde_json::from_slice(bytes).context("decode replication client allowlist JSON")?;
        ensure!(
            decoded.schema_version == ALLOWLIST_SCHEMA_VERSION,
            "unsupported replication client allowlist schema version"
        );
        ensure!(
            !decoded.clients.is_empty(),
            "replication client allowlist must not be empty"
        );
        ensure!(
            decoded.clients.len() <= MAX_ALLOWLIST_CLIENTS,
            "replication client allowlist has too many certificates"
        );

        let mut clients = HashMap::with_capacity(decoded.clients.len());
        for client in decoded.clients {
            let fingerprint = parse_fingerprint(&client.certificate_sha256)?;
            ensure!(
                !client.identities.is_empty(),
                "certificate allowlist entry must permit at least one identity"
            );
            ensure!(
                client.identities.len() <= MAX_IDENTITIES_PER_CLIENT,
                "certificate allowlist entry permits too many identities"
            );
            let mut identities = HashSet::with_capacity(client.identities.len());
            for identity in client.identities {
                ensure!(
                    is_stable_id(&identity.cluster_id),
                    "allowlisted cluster id is not a stable identifier"
                );
                ensure!(
                    is_stable_id(&identity.origin_node_id),
                    "allowlisted origin node id is not a stable identifier"
                );
                ensure!(
                    is_stable_id(&identity.source_id),
                    "allowlisted source id is not a stable identifier"
                );
                ensure!(
                    identities.insert(AllowedIdentity {
                        cluster_id: identity.cluster_id,
                        origin_node_id: identity.origin_node_id,
                        source_id: identity.source_id,
                    }),
                    "certificate allowlist entry repeats an identity"
                );
            }
            ensure!(
                clients.insert(fingerprint, Arc::new(identities)).is_none(),
                "replication client allowlist repeats a certificate fingerprint"
            );
        }
        Ok(Self {
            clients: Arc::new(clients),
        })
    }

    fn authenticate_leaf(&self, leaf_der: &[u8]) -> Result<AuthenticatedClient, AuthError> {
        let fingerprint: [u8; CERTIFICATE_FINGERPRINT_BYTES] = Sha256::digest(leaf_der).into();
        let identities = self
            .clients
            .get(&fingerprint)
            .cloned()
            .ok_or(AuthError::UnknownCertificate)?;
        Ok(AuthenticatedClient { identities })
    }

    pub fn certificate_count(&self) -> usize {
        self.clients.len()
    }
}

impl fmt::Debug for ClientCertificateAllowlist {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCertificateAllowlist")
            .field("certificate_count", &self.clients.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AllowedIdentity {
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
}

#[derive(Clone)]
struct AuthenticatedClient {
    identities: Arc<HashSet<AllowedIdentity>>,
}

impl AuthenticatedClient {
    fn permits(&self, stream: &ReplicationStreamId) -> bool {
        self.identities.contains(&AllowedIdentity {
            cluster_id: stream.cluster_id.clone(),
            origin_node_id: stream.origin_node_id.clone(),
            source_id: stream.source_id.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthError {
    MissingCertificate,
    UnknownCertificate,
    UnauthorizedIdentity,
}

impl AuthError {
    fn into_status(self) -> Status {
        match self {
            Self::MissingCertificate => {
                Status::unauthenticated("a verified client certificate is required")
            }
            Self::UnknownCertificate => {
                Status::permission_denied("client certificate is not authorized")
            }
            Self::UnauthorizedIdentity => {
                Status::permission_denied("client certificate is not authorized for this stream")
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllowlistFile {
    schema_version: u32,
    clients: Vec<AllowlistClient>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllowlistClient {
    certificate_sha256: String,
    identities: Vec<AllowlistIdentity>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllowlistIdentity {
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
}

/// Construct a server TLS policy which always requests and verifies a client certificate.
///
/// Installing this value is mandatory. The request handler independently rejects requests whose
/// TLS connection metadata contains no peer certificate, so accidentally making client auth
/// optional does not create a fallback authentication path.
pub fn mtls_only_server_tls_config(
    server_identity: Identity,
    client_ca_root: Certificate,
    handshake_timeout: Duration,
) -> Result<ServerTlsConfig> {
    ensure!(
        !handshake_timeout.is_zero(),
        "TLS handshake timeout must be nonzero"
    );
    Ok(ServerTlsConfig::new()
        .identity(server_identity)
        .client_ca_root(client_ca_root)
        .client_auth_optional(false)
        .timeout(handshake_timeout))
}

/// Fully constructed, fencing-term-owning mTLS receiver runtime.
///
/// Construction performs all bounded file loads and acquires the durable primary term. The term
/// guard then remains owned by the service until graceful server shutdown completes.
pub struct RawReplicationServerRuntime {
    bind: SocketAddr,
    listener: std::net::TcpListener,
    tls: ServerTlsConfig,
    service: RawReplicationService,
}

impl RawReplicationServerRuntime {
    /// Build the receiver runtime from an already parsed ingest configuration. The configuration
    /// is validated again so callers cannot bypass cross-field limits by deserializing directly.
    pub fn from_ingest_config(config: &IngestConfig) -> Result<Self> {
        config
            .validate()
            .map_err(|error| anyhow!("invalid ingest receiver configuration: {error}"))?;
        let (node_id, listener) = match &config.role {
            IngestRoleConfig::Primary {
                node_id,
                replica_listener: Some(listener),
            } => (node_id, listener),
            IngestRoleConfig::Primary {
                replica_listener: None,
                ..
            } => return Err(anyhow!("primary configuration has no replica listener")),
            IngestRoleConfig::Replica { .. } => {
                return Err(anyhow!("replica role cannot run a primary receiver"));
            }
        };
        for (label, path) in [
            ("server certificate", &listener.server_certificate_file),
            ("server private key", &listener.server_private_key_file),
            ("client CA", &listener.client_ca_file),
            ("client allowlist", &listener.allowed_nodes_file),
            ("primary term state", &listener.primary_term_file),
        ] {
            ensure_control_file_outside_spool(path, &config.spool.root, label)?;
        }

        let bind: SocketAddr = listener
            .bind
            .parse()
            .context("parse replica listener bind address")?;
        let (server_identity, client_ca_root) = load_mtls_server_material(
            &listener.server_certificate_file,
            &listener.server_private_key_file,
            &listener.client_ca_file,
        )?;
        let tls = mtls_only_server_tls_config(
            server_identity,
            client_ca_root,
            Duration::from_millis(listener.tls_handshake_timeout_ms),
        )?;
        let allowlist = ClientCertificateAllowlist::load_json(&listener.allowed_nodes_file)?;
        let signing_key_path = match &listener.receipt_signing_key {
            SecretRef::File { path } => path,
            SecretRef::Env { .. } => {
                return Err(anyhow!(
                    "receipt signing key must be a private file reference"
                ));
            }
        };
        ensure_control_file_outside_spool(
            signing_key_path,
            &config.spool.root,
            "receipt signing key",
        )?;
        let signer = Ed25519ReceiptSigner::load_pkcs8_pem(
            listener.receipt_signing_key_id.clone(),
            signing_key_path,
        )?;
        let receiver_limits = RawReceiverLimits {
            max_batch_records: listener.max_batch_events,
            max_batch_compressed_bytes: listener.max_batch_compressed_bytes,
            max_batch_uncompressed_bytes: listener.max_batch_uncompressed_bytes,
            max_compressed_record_bytes: listener.max_compressed_event_bytes,
            max_uncompressed_record_bytes: listener.max_uncompressed_event_bytes,
        };
        let receiver_root = replication_receiver_spool_root(&config.spool.root);
        validate_runtime_spool_roots(&config.spool.root, &receiver_root)?;
        let manager_config = ReplicationReceiverManagerConfig {
            // Inbound replication gets an exclusive namespace. Local source writers do not
            // participate in the persistent known-stream admission lock and therefore must
            // never share this exact root. max_spool_bytes applies to this subtree;
            // reserve_free_bytes is checked against the underlying filesystem as a whole.
            spool_root: receiver_root,
            primary_id: node_id.clone(),
            spool_options: SpoolOptions {
                segment_target_bytes: config.spool.segment_bytes,
                max_record_bytes: listener.max_compressed_event_bytes,
            },
            max_spool_bytes: config.spool.max_bytes,
            reserve_free_bytes: config.spool.reserve_free_bytes,
            receiver_limits,
        };
        let service_limits = ReplicationServiceLimits {
            max_open_streams: listener.max_open_streams,
            max_known_streams: listener.max_known_streams,
            max_concurrent_requests: listener.max_concurrent_requests,
            max_decoding_message_bytes: usize::try_from(listener.max_wire_request_bytes)
                .context("replica listener wire limit does not fit this platform")?,
            stream_idle_timeout: Duration::from_millis(listener.request_idle_timeout_ms),
            stream_total_timeout: Duration::from_millis(listener.request_total_timeout_ms),
        };
        // Re-run runtime-specific validation before consuming a term.
        manager_config.clone().validate()?;
        service_limits.validate(receiver_limits)?;
        // Tonic parses the certificate, key, and CA here. A malformed TLS input must not consume
        // a fencing term merely because the raw PEM files passed bounded loading.
        let _validated_tls = Server::builder()
            .tls_config(tls.clone())
            .context("validate replica listener mTLS policy")?;
        let listener_socket = std::net::TcpListener::bind(bind)
            .with_context(|| format!("bind mTLS replica listener on {bind}"))?;
        listener_socket
            .set_nonblocking(true)
            .context("set replica listener socket nonblocking")?;
        let bind = listener_socket
            .local_addr()
            .context("inspect bound replica listener address")?;

        // Acquire the fencing term only after immutable trust inputs validate and the listener is
        // successfully reserved. The socket cannot serve until the term-owning service exists.
        let primary_term_guard = PrimaryTermStore::new(&listener.primary_term_file).acquire()?;
        let service = RawReplicationService::new(
            manager_config,
            service_limits,
            allowlist,
            signer,
            primary_term_guard,
        )?;
        Ok(Self {
            bind,
            listener: listener_socket,
            tls,
            service,
        })
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind
    }

    pub fn primary_term(&self) -> u64 {
        self.service.primary_term()
    }

    /// Serve only TLS-authenticated replication and drain accepted RPCs after shutdown fires.
    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let bind = self.bind;
        let listener = tokio::net::TcpListener::from_std(self.listener)
            .context("attach replica listener socket to the Tokio runtime")?;
        let incoming = futures::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(Some((stream, listener)))
        });
        let service = self.service.into_tonic_service();
        Server::builder()
            .tls_config(self.tls)
            .context("install replica listener mTLS policy")?
            .tcp_nodelay(true)
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .with_context(|| format!("serve mTLS replica listener on {bind}"))
    }
}

/// Read a strict, bounded receiver configuration without following a final symlink.
pub fn load_ingest_receiver_config(path: impl AsRef<Path>) -> Result<IngestConfig> {
    let path = path.as_ref();
    let bytes = read_runtime_file(
        path,
        RuntimeFileKind::Public,
        MAX_RECEIVER_CONFIG_FILE_BYTES,
    )?;
    let json = std::str::from_utf8(&bytes)
        .with_context(|| format!("receiver config is not UTF-8: {}", path.display()))?;
    let config = IngestConfig::from_json(json)
        .map_err(|error| anyhow!("load receiver config {}: {error}", path.display()))?;
    ensure_control_file_outside_spool(path, &config.spool.root, "receiver config")?;
    Ok(config)
}

pub fn replication_receiver_spool_root(configured_spool_root: &Path) -> PathBuf {
    configured_spool_root.join(REPLICATION_RECEIVER_SPOOL_DIRECTORY)
}

fn validate_runtime_spool_roots(configured_root: &Path, receiver_root: &Path) -> Result<()> {
    match fs::symlink_metadata(configured_root) {
        Ok(_) => validate_real_directory(configured_root, "configured spool root")?,
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect configured spool root {}",
                    configured_root.display()
                )
            });
        }
    }
    match fs::symlink_metadata(receiver_root) {
        Ok(_) => validate_real_directory(receiver_root, "replication receiver spool root")?,
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect replication receiver spool root {}",
                    receiver_root.display()
                )
            });
        }
    }
    Ok(())
}

impl fmt::Debug for RawReplicationServerRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationServerRuntime")
            .field("bind", &self.bind)
            .field("primary_term", &self.primary_term())
            .field("tls", &"<redacted>")
            .finish_non_exhaustive()
    }
}

fn load_mtls_server_material(
    certificate_path: &Path,
    private_key_path: &Path,
    client_ca_path: &Path,
) -> Result<(Identity, Certificate)> {
    let certificate = read_runtime_file(
        certificate_path,
        RuntimeFileKind::Public,
        MAX_TLS_CERTIFICATE_FILE_BYTES,
    )?;
    let mut private_key = read_runtime_file(
        private_key_path,
        RuntimeFileKind::Private,
        MAX_TLS_PRIVATE_KEY_FILE_BYTES,
    )?;
    let client_ca = read_runtime_file(
        client_ca_path,
        RuntimeFileKind::Public,
        MAX_TLS_CERTIFICATE_FILE_BYTES,
    )?;
    ensure!(
        contains_pem_begin(&certificate),
        "server certificate file does not contain PEM data"
    );
    ensure!(
        contains_pem_begin(&private_key),
        "server private key file does not contain PEM data"
    );
    ensure!(
        contains_pem_begin(&client_ca),
        "client CA file does not contain PEM data"
    );
    let identity = Identity::from_pem(&certificate, &private_key);
    private_key.zeroize();
    Ok((identity, Certificate::from_pem(client_ca)))
}

fn contains_pem_begin(bytes: &[u8]) -> bool {
    const PEM_BEGIN: &[u8] = b"-----BEGIN ";
    bytes
        .windows(PEM_BEGIN.len())
        .any(|window| window == PEM_BEGIN)
}

fn ensure_control_file_outside_spool(path: &Path, spool_root: &Path, label: &str) -> Result<()> {
    ensure!(
        !path.starts_with(spool_root),
        "{label} must be outside the exclusive spool root"
    );
    if spool_root.exists()
        && let Ok(real_spool) = fs::canonicalize(spool_root)
    {
        if let Ok(real_path) = fs::canonicalize(path) {
            ensure!(
                !real_path.starts_with(&real_spool),
                "{label} resolves inside the exclusive spool root"
            );
        } else if let Some(parent) = path.parent()
            && let Ok(real_parent) = fs::canonicalize(parent)
        {
            ensure!(
                !real_parent.starts_with(&real_spool),
                "{label} resolves inside the exclusive spool root"
            );
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum RuntimeFileKind {
    Public,
    Private,
}

impl RuntimeFileKind {
    fn label(self) -> &'static str {
        match self {
            Self::Public => "public runtime",
            Self::Private => "private runtime",
        }
    }
}

fn read_runtime_file(path: &Path, kind: RuntimeFileKind, maximum: u64) -> Result<Vec<u8>> {
    let link_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {} file {}", kind.label(), path.display()))?;
    ensure!(
        !link_metadata.file_type().is_symlink(),
        "{} file must not be a symbolic link",
        kind.label()
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let mut file = options
        .open(path)
        .with_context(|| format!("open {} file {}", kind.label(), path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect opened {} file {}", kind.label(), path.display()))?;
    ensure!(metadata.is_file(), "runtime input must be a regular file");
    ensure!(
        metadata.len() > 0 && metadata.len() <= maximum,
        "runtime input file length is outside its safety bound"
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.dev() == link_metadata.dev() && metadata.ino() == link_metadata.ino(),
            "runtime input file changed while opening"
        );
        let forbidden_mode = match kind {
            RuntimeFileKind::Public => 0o022,
            RuntimeFileKind::Private => 0o077,
        };
        ensure!(
            metadata.mode() & forbidden_mode == 0,
            "runtime input file permissions are unsafe"
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            metadata.uid() == effective_uid || metadata.uid() == 0,
            "runtime input file must be owned by this user or root"
        );
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {} file {}", kind.label(), path.display()))?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "runtime input file changed while being read"
    );
    Ok(bytes)
}

/// Tonic replication implementation. Clone is cheap and retains the primary fencing lock.
#[derive(Clone)]
pub struct RawReplicationService {
    inner: Arc<RawReplicationServiceInner>,
}

struct RawReplicationServiceInner {
    allowlist: ClientCertificateAllowlist,
    admission: Arc<Semaphore>,
    manager: Arc<ReceiverManager>,
    signer: Ed25519ReceiptSigner,
    primary_term_guard: PrimaryTermGuard,
    receiver_limits: RawReceiverLimits,
    limits: ReplicationServiceLimits,
}

impl RawReplicationService {
    pub fn new(
        manager_config: ReplicationReceiverManagerConfig,
        limits: ReplicationServiceLimits,
        allowlist: ClientCertificateAllowlist,
        signer: Ed25519ReceiptSigner,
        primary_term_guard: PrimaryTermGuard,
    ) -> Result<Self> {
        let manager_config = manager_config.validate()?;
        let limits = limits.validate(manager_config.receiver_limits)?;
        ensure!(
            is_stable_id(signer.key_id()),
            "receipt signing key id must use the stable 1..=64 ASCII identifier grammar"
        );
        let primary_term = primary_term_guard.term();
        let manager = ReceiverManager::new(
            manager_config.clone(),
            primary_term.get(),
            limits.max_open_streams,
            limits.max_known_streams,
        );
        Ok(Self {
            inner: Arc::new(RawReplicationServiceInner {
                allowlist,
                admission: Arc::new(Semaphore::new(limits.max_concurrent_requests)),
                manager: Arc::new(manager),
                signer,
                primary_term_guard,
                receiver_limits: manager_config.receiver_limits,
                limits,
            }),
        })
    }

    /// Wrap this handler in the generated Tonic service with a hard per-message decode limit.
    pub fn into_tonic_service(self) -> wire::raw_replication_server::RawReplicationServer<Self> {
        let maximum = self.inner.limits.max_decoding_message_bytes;
        wire::raw_replication_server::RawReplicationServer::new(self)
            .max_decoding_message_size(maximum)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_BYTES)
    }

    pub fn max_decoding_message_bytes(&self) -> usize {
        self.inner.limits.max_decoding_message_bytes
    }

    pub fn primary_term(&self) -> u64 {
        self.inner.primary_term_guard.term().get()
    }

    fn authenticate<T>(&self, request: &Request<T>) -> Result<AuthenticatedClient, Status> {
        let certificates = request
            .peer_certs()
            .ok_or(AuthError::MissingCertificate.into_status())?;
        let leaf = certificates
            .first()
            .ok_or(AuthError::MissingCertificate.into_status())?;
        self.inner
            .allowlist
            .authenticate_leaf(leaf.as_ref())
            .map_err(AuthError::into_status)
    }

    fn acquire_admission(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("replication request limit reached"))
    }

    async fn read_bounded_batch(
        &self,
        authenticated: &AuthenticatedClient,
        mut stream: tonic::Streaming<wire::PushBatchRequest>,
    ) -> Result<ValidatedPushBatch, Status> {
        let mut builder =
            BoundedPushBatchBuilder::new(self.inner.receiver_limits).map_err(wire_error_status)?;
        loop {
            let item =
                tokio::time::timeout(self.inner.limits.stream_idle_timeout, stream.message())
                    .await
                    .map_err(|_| Status::deadline_exceeded("replication stream was idle"))?
                    .map_err(|_| {
                        Status::invalid_argument("replication stream framing is invalid")
                    })?;
            let Some(item) = item else {
                break;
            };
            let item: ValidatedPushRecord = item.try_into().map_err(wire_error_status)?;
            if !authenticated.permits(item.stream()) {
                return Err(AuthError::UnauthorizedIdentity.into_status());
            }
            builder.push(item).map_err(wire_error_status)?;
        }
        builder.finish().map_err(wire_error_status)
    }

    fn sign_ack(&self, ack: CumulativePrimaryAck) -> Result<wire::CumulativePrimaryAck, Status> {
        let ack = self.signed_ack_value(ack)?;
        wire::CumulativePrimaryAck::try_from(&ack)
            .map_err(|_| Status::internal("could not encode durable acknowledgement"))
    }
}

impl fmt::Debug for RawReplicationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationService")
            .field("primary_term", &self.primary_term())
            .field("limits", &self.inner.limits)
            .field("allowlist", &self.inner.allowlist)
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl wire::raw_replication_server::RawReplication for RawReplicationService {
    async fn push_batch(
        &self,
        request: Request<tonic::Streaming<wire::PushBatchRequest>>,
    ) -> Result<Response<wire::CumulativePrimaryAck>, Status> {
        // Authentication and global admission deliberately precede `into_inner` and therefore
        // precede the first poll of the streaming body.
        let authenticated = self.authenticate(&request)?;
        let permit = self.acquire_admission()?;
        let deadline = tokio::time::Instant::now()
            .checked_add(self.inner.limits.stream_total_timeout)
            .ok_or_else(|| Status::internal("replication request deadline overflow"))?;
        let stream = request.into_inner();
        let batch =
            tokio::time::timeout_at(deadline, self.read_bounded_batch(&authenticated, stream))
                .await
                .map_err(|_| {
                    Status::deadline_exceeded("replication request deadline exceeded")
                })??;

        let expected_stream = batch.stream().clone();
        let (_, records) = batch.into_parts();
        let manager = Arc::clone(&self.inner.manager);
        let durable_task = tokio::task::spawn_blocking(move || {
            // The task owns admission. RPC cancellation or a total-deadline timeout may detach
            // this blocking task, but cannot admit replacement memory/fsync work until it exits.
            let _permit = permit;
            manager.push(expected_stream, records)
        });
        let ack = tokio::time::timeout_at(deadline, durable_task)
            .await
            .map_err(|_| Status::deadline_exceeded("replication request deadline exceeded"))?
            .map_err(|_| Status::internal("durable receiver task failed"))?
            .map_err(manager_error_status)?;
        Ok(Response::new(self.sign_ack(ack)?))
    }

    async fn get_ack(
        &self,
        request: Request<wire::GetAckRequest>,
    ) -> Result<Response<wire::GetAckResponse>, Status> {
        let authenticated = self.authenticate(&request)?;
        let permit = self.acquire_admission()?;
        let deadline = tokio::time::Instant::now()
            .checked_add(self.inner.limits.stream_total_timeout)
            .ok_or_else(|| Status::internal("replication request deadline overflow"))?;
        let stream: ReplicationStreamId =
            request.into_inner().try_into().map_err(wire_error_status)?;
        if !authenticated.permits(&stream) {
            return Err(AuthError::UnauthorizedIdentity.into_status());
        }

        let manager = Arc::clone(&self.inner.manager);
        let durable_task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.get_ack(stream)
        });
        let ack = tokio::time::timeout_at(deadline, durable_task)
            .await
            .map_err(|_| Status::deadline_exceeded("replication request deadline exceeded"))?
            .map_err(|_| Status::internal("durable receiver task failed"))?
            .map_err(manager_error_status)?;
        let state = match ack {
            Some(ack) => GetAckState::DurablePrefix(self.signed_ack_value(ack)?),
            None => GetAckState::NoDurablePrefix,
        };
        let response = wire::GetAckResponse::try_from(&state)
            .map_err(|_| Status::internal("could not encode durable acknowledgement"))?;
        Ok(Response::new(response))
    }
}

impl RawReplicationService {
    fn signed_ack_value(
        &self,
        mut ack: CumulativePrimaryAck,
    ) -> Result<CumulativePrimaryAck, Status> {
        let expected_term = self.inner.primary_term_guard.term().get();
        if ack.primary_term == 0
            || ack.primary_term != expected_term
            || ack.durable_lsn == 0
            || !ack.disposition.authorizes_replica_gc()
        {
            return Err(Status::failed_precondition(
                "durable receiver returned an invalid acknowledgement",
            ));
        }
        self.inner
            .signer
            .sign_cumulative_ack(&mut ack)
            .map_err(|_| Status::internal("could not sign durable acknowledgement"))?;
        Ok(ack)
    }
}

struct ReceiverManager {
    config: ReplicationReceiverManagerConfig,
    primary_term: u64,
    max_open_streams: usize,
    max_known_streams: usize,
    registry: Mutex<ReceiverRegistry>,
}

struct ReceiverRegistry {
    receivers: HashMap<ReplicationStreamId, ReceiverRegistryEntry>,
    access_clock: u128,
}

enum ReceiverRegistryEntry {
    Open(OpenReceiver),
    Opening(Arc<OpeningGate>),
}

struct OpenReceiver {
    receiver: Arc<Mutex<BlockzillaRawReceiver>>,
    last_access: u128,
}

struct OpeningGate {
    completed: Mutex<bool>,
    ready: Condvar,
}

impl OpeningGate {
    fn new() -> Self {
        Self {
            completed: Mutex::new(false),
            ready: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut completed = self
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*completed {
            completed = self
                .ready
                .wait(completed)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn complete(&self) {
        let mut completed = self
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *completed = true;
        self.ready.notify_all();
    }
}

impl ReceiverRegistry {
    fn next_access(&mut self) -> u128 {
        self.access_clock = self.access_clock.saturating_add(1);
        self.access_clock
    }

    /// Evict only a receiver for which the registry owns the sole Arc. Any request which has
    /// acquired a receiver holds another strong reference, making active fsync/decompression
    /// impossible to evict. The least recently acquired idle receiver is selected.
    fn take_lru_idle(&mut self) -> Option<OpenReceiver> {
        let candidate = self
            .receivers
            .iter()
            .filter_map(|(stream, entry)| match entry {
                ReceiverRegistryEntry::Open(open) if Arc::strong_count(&open.receiver) == 1 => {
                    Some((stream, open.last_access))
                }
                ReceiverRegistryEntry::Open(_) | ReceiverRegistryEntry::Opening(_) => None,
            })
            .min_by_key(|(_, last_access)| *last_access)
            .map(|(stream, _)| stream.clone());
        match candidate.and_then(|stream| self.receivers.remove(&stream)) {
            Some(ReceiverRegistryEntry::Open(open)) => Some(open),
            Some(ReceiverRegistryEntry::Opening(_)) | None => None,
        }
    }
}

struct OpeningReservation<'manager> {
    manager: &'manager ReceiverManager,
    stream: ReplicationStreamId,
    gate: Arc<OpeningGate>,
    finished: bool,
}

impl OpeningReservation<'_> {
    fn install(
        mut self,
        receiver: BlockzillaRawReceiver,
        last_access: u128,
    ) -> std::result::Result<Arc<Mutex<BlockzillaRawReceiver>>, ReceiverManagerError> {
        let receiver = Arc::new(Mutex::new(receiver));
        let mut registry = self
            .manager
            .registry
            .lock()
            .map_err(|_| ReceiverManagerError::StatePoisoned)?;
        let still_reserved = matches!(
            registry.receivers.get(&self.stream),
            Some(ReceiverRegistryEntry::Opening(gate)) if Arc::ptr_eq(gate, &self.gate)
        );
        if !still_reserved {
            return Err(ReceiverManagerError::StatePoisoned);
        }
        registry.receivers.insert(
            self.stream.clone(),
            ReceiverRegistryEntry::Open(OpenReceiver {
                receiver: Arc::clone(&receiver),
                last_access,
            }),
        );
        self.finished = true;
        drop(registry);
        self.gate.complete();
        Ok(receiver)
    }
}

impl Drop for OpeningReservation<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Ok(mut registry) = self.manager.registry.lock() {
            let still_reserved = matches!(
                registry.receivers.get(&self.stream),
                Some(ReceiverRegistryEntry::Opening(gate)) if Arc::ptr_eq(gate, &self.gate)
            );
            if still_reserved {
                registry.receivers.remove(&self.stream);
            }
        }
        self.gate.complete();
    }
}

impl ReceiverManager {
    fn new(
        config: ReplicationReceiverManagerConfig,
        primary_term: u64,
        max_open_streams: usize,
        max_known_streams: usize,
    ) -> Self {
        debug_assert_ne!(primary_term, 0);
        debug_assert!(max_known_streams >= max_open_streams);
        Self {
            config,
            primary_term,
            max_open_streams,
            max_known_streams,
            registry: Mutex::new(ReceiverRegistry {
                receivers: HashMap::new(),
                access_clock: 0,
            }),
        }
    }

    fn push(
        &self,
        stream: ReplicationStreamId,
        records: Vec<RawReplicationRecord>,
    ) -> std::result::Result<CumulativePrimaryAck, ReceiverManagerError> {
        let require_initial_sequence_zero =
            !stream_directory_exists(&self.config.spool_root, &stream)
                .map_err(ReceiverManagerError::KnownRegistry)?;
        if require_initial_sequence_zero {
            validate_raw_replication_batch(&records, &stream, self.config.receiver_limits, true)
                .map_err(ReceiverManagerError::Push)?;
        }
        let receiver = self.receiver_for_push(&stream)?;
        let mut receiver = receiver
            .lock()
            .map_err(|_| ReceiverManagerError::StatePoisoned)?;
        receiver
            .push_batch(records)
            .map_err(ReceiverManagerError::Push)
    }

    fn get_ack(
        &self,
        stream: ReplicationStreamId,
    ) -> std::result::Result<Option<CumulativePrimaryAck>, ReceiverManagerError> {
        let Some(receiver) = self.receiver_for_existing(&stream)? else {
            return Ok(None);
        };
        let receiver = receiver
            .lock()
            .map_err(|_| ReceiverManagerError::StatePoisoned)?;
        Ok(receiver.get_ack())
    }

    fn receiver_for_push(
        &self,
        stream: &ReplicationStreamId,
    ) -> std::result::Result<Arc<Mutex<BlockzillaRawReceiver>>, ReceiverManagerError> {
        self.receiver(stream, true)
            .and_then(|receiver| receiver.ok_or(ReceiverManagerError::StatePoisoned))
    }

    fn receiver_for_existing(
        &self,
        stream: &ReplicationStreamId,
    ) -> std::result::Result<Option<Arc<Mutex<BlockzillaRawReceiver>>>, ReceiverManagerError> {
        self.receiver(stream, false)
    }

    fn receiver(
        &self,
        stream: &ReplicationStreamId,
        create: bool,
    ) -> std::result::Result<Option<Arc<Mutex<BlockzillaRawReceiver>>>, ReceiverManagerError> {
        loop {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| ReceiverManagerError::StatePoisoned)?;
            let access = registry.next_access();
            match registry.receivers.get_mut(stream) {
                Some(ReceiverRegistryEntry::Open(open)) => {
                    open.last_access = access;
                    return Ok(Some(Arc::clone(&open.receiver)));
                }
                Some(ReceiverRegistryEntry::Opening(gate)) => {
                    let gate = Arc::clone(gate);
                    drop(registry);
                    gate.wait();
                    continue;
                }
                None => {}
            }

            // Reserve one bounded open-stream slot before releasing the registry. Same-stream
            // callers wait on this gate, while unrelated streams remain free to progress during
            // flock, directory scans, recovery, decompression, and file opening.
            let evicted = if registry.receivers.len() >= self.max_open_streams {
                Some(
                    registry
                        .take_lru_idle()
                        .ok_or(ReceiverManagerError::OpenLimit)?,
                )
            } else {
                None
            };
            let gate = Arc::new(OpeningGate::new());
            registry.receivers.insert(
                stream.clone(),
                ReceiverRegistryEntry::Opening(Arc::clone(&gate)),
            );
            let reservation = OpeningReservation {
                manager: self,
                stream: stream.clone(),
                gate,
                finished: false,
            };
            drop(registry);
            // Drop may close file descriptors, so keep it outside the global registry lock.
            drop(evicted);

            if !create && !self.existing_stream_has_progress(stream)? {
                return Ok(None);
            }
            // Keep this cross-process admission guard alive until BlockzillaRawReceiver has
            // created and synced the new journal directory. Existing journals consume no slot.
            let _known_stream_admission = if create {
                self.admit_new_known_stream(stream)?
            } else {
                None
            };
            let receiver = BlockzillaRawReceiver::open(RawReceiverConfig {
                spool_root: self.config.spool_root.clone(),
                stream: stream.clone(),
                primary_id: self.config.primary_id.clone(),
                primary_term: self.primary_term,
                spool_options: self.config.spool_options,
                max_spool_bytes: self.config.max_spool_bytes,
                reserve_free_bytes: self.config.reserve_free_bytes,
                limits: self.config.receiver_limits,
            })
            .map_err(ReceiverManagerError::Open)?;
            return reservation.install(receiver, access).map(Some);
        }
    }

    fn admit_new_known_stream(
        &self,
        stream: &ReplicationStreamId,
    ) -> std::result::Result<Option<KnownStreamsGuard>, ReceiverManagerError> {
        if stream_directory_exists(&self.config.spool_root, stream)
            .map_err(ReceiverManagerError::KnownRegistry)?
        {
            return Ok(None);
        }
        let guard = KnownStreamsGuard::acquire(&self.config.spool_root)
            .map_err(ReceiverManagerError::KnownRegistry)?;
        // A second process may have admitted this exact stream while we waited for the lock.
        if stream_directory_exists(&self.config.spool_root, stream)
            .map_err(ReceiverManagerError::KnownRegistry)?
        {
            return Ok(Some(guard));
        }
        let known = count_known_streams(&self.config.spool_root, self.max_known_streams)
            .map_err(ReceiverManagerError::KnownRegistry)?;
        if known >= self.max_known_streams {
            return Err(ReceiverManagerError::KnownLimit);
        }
        Ok(Some(guard))
    }

    fn existing_stream_has_progress(
        &self,
        stream: &ReplicationStreamId,
    ) -> std::result::Result<bool, ReceiverManagerError> {
        let directory = stream_journal_directory(&self.config.spool_root, stream);
        let directory_metadata = match fs::symlink_metadata(&directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(ReceiverManagerError::Probe(error)),
        };
        if !directory_metadata.is_dir() || directory_metadata.file_type().is_symlink() {
            return Err(ReceiverManagerError::UnsafeExistingStream);
        }
        let progress = directory.join(super::RECEIVER_PROGRESS_WAL_FILE);
        match fs::symlink_metadata(progress) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
            Ok(_) => Err(ReceiverManagerError::UnsafeExistingStream),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(ReceiverManagerError::Probe(error)),
        }
    }
}

#[derive(Debug)]
enum ReceiverManagerError {
    OpenLimit,
    KnownLimit,
    StatePoisoned,
    UnsafeExistingStream,
    Probe(std::io::Error),
    KnownRegistry(anyhow::Error),
    Open(anyhow::Error),
    Push(anyhow::Error),
}

fn manager_error_status(error: ReceiverManagerError) -> Status {
    match error {
        ReceiverManagerError::OpenLimit => {
            Status::resource_exhausted("replication open-stream limit reached")
        }
        ReceiverManagerError::KnownLimit => {
            Status::resource_exhausted("replication known-stream limit reached")
        }
        ReceiverManagerError::Push(error) if is_capacity_error(&error) => {
            Status::resource_exhausted("durable receiver storage limit reached")
        }
        ReceiverManagerError::Push(error) if is_input_error(&error) => {
            Status::invalid_argument("replication batch was rejected")
        }
        ReceiverManagerError::StatePoisoned
        | ReceiverManagerError::UnsafeExistingStream
        | ReceiverManagerError::Push(_) => {
            Status::failed_precondition("durable receiver requires operator attention")
        }
        ReceiverManagerError::Open(error) => {
            // Consume the source for diagnostics classification without putting its paths or
            // payload-dependent context on the wire.
            let _root_cause = error.root_cause();
            Status::failed_precondition("durable receiver requires operator attention")
        }
        ReceiverManagerError::KnownRegistry(error) => {
            let _root_cause = error.root_cause();
            Status::failed_precondition("replication stream registry requires operator attention")
        }
        ReceiverManagerError::Probe(error) => {
            let _error_kind = error.kind();
            Status::unavailable("durable receiver storage is unavailable")
        }
    }
}

fn is_capacity_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("capacity would be exceeded")
            || message.contains("filesystem reserve would be crossed")
    })
}

fn is_input_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        [
            "batch must not be empty",
            "batch has",
            "batch compressed bytes exceed",
            "batch uncompressed bytes exceed",
            "sequence is not contiguous",
            "sequence gap",
            "stream must start",
            "was reused with different",
            "does not match receiver stream",
            "payload",
            "protobuf",
            "blockhash",
            "commitment",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    })
}

fn wire_error_status(_error: ReplicationWireError) -> Status {
    Status::invalid_argument("replication message is invalid")
}

/// Cross-process guard for the persistent journal-directory count. It is held from the last
/// pre-creation scan through receiver open, so two processes cannot both consume the final slot.
struct KnownStreamsGuard {
    _file: File,
}

impl KnownStreamsGuard {
    fn acquire(root: &Path) -> Result<Self> {
        ensure_receiver_spool_root_durable(root)?;
        validate_real_directory(root, "receiver spool root")?;
        let path = root.join(KNOWN_STREAMS_LOCK_FILE);
        let (file, created) = match open_known_streams_lock(&path, true) {
            Ok(file) => (file, true),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => (
                open_known_streams_lock(&path, false).with_context(|| {
                    format!("open known-stream admission lock {}", path.display())
                })?,
                false,
            ),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("create known-stream admission lock {}", path.display())
                });
            }
        };
        validate_known_streams_lock(&file, &path)?;
        lock_known_streams_file(&file, &path)?;
        // Revalidate the directory entry after acquiring the stable-inode lock. A replaced lock
        // path must never split admission into two independent lock domains.
        validate_known_streams_lock(&file, &path)?;
        if created {
            file.sync_all()
                .with_context(|| format!("sync known-stream admission lock {}", path.display()))?;
            sync_real_directory(root)?;
        }
        Ok(Self { _file: file })
    }
}

fn open_known_streams_lock(path: &Path, create_new: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    }
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    options.open(path)
}

fn validate_known_streams_lock(file: &File, path: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect known-stream admission lock {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "known-stream admission lock must be a regular file"
    );
    let path_metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "inspect known-stream admission lock path {}",
            path.display()
        )
    })?;
    ensure!(
        path_metadata.is_file() && !path_metadata.file_type().is_symlink(),
        "known-stream admission lock path must be a regular file"
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.dev() == path_metadata.dev() && metadata.ino() == path_metadata.ino(),
            "known-stream admission lock changed while opening"
        );
        ensure!(
            metadata.mode() & 0o077 == 0,
            "known-stream admission lock must be owner-only"
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            metadata.uid() == effective_uid || metadata.uid() == 0,
            "known-stream admission lock must be owned by this user or root"
        );
    }
    Ok(())
}

fn lock_known_streams_file(file: &File, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lock known-stream admission file {}", path.display()));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (file, path);
        Err(anyhow!(
            "known-stream admission requires Unix descriptor locks"
        ))
    }
}

fn validate_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "{label} must be a real directory"
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.mode() & 0o022 == 0,
            "{label} must not be group- or world-writable"
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            metadata.uid() == effective_uid || metadata.uid() == 0,
            "{label} must be owned by this user or root"
        );
    }
    Ok(())
}

fn sync_real_directory(path: &Path) -> Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY);
    options
        .open(path)
        .with_context(|| format!("open receiver spool root directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync receiver spool root directory {}", path.display()))
}

fn stream_directory_exists(root: &Path, stream: &ReplicationStreamId) -> Result<bool> {
    let paths = [
        root.to_path_buf(),
        root.join(&stream.cluster_id),
        root.join(&stream.cluster_id).join(&stream.origin_node_id),
        root.join(&stream.cluster_id)
            .join(&stream.origin_node_id)
            .join(&stream.source_id),
        stream_journal_directory(root, stream),
    ];
    for path in paths {
        match fs::symlink_metadata(&path) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "receiver stream path component is not a real directory"
            ),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect receiver stream path {}", path.display()));
            }
        }
    }
    Ok(true)
}

/// Count exact `<cluster>/<origin>/<source>/<journal-hex>` directories without descending into
/// journal contents. Every traversed name and file type is validated; unexpected root files or
/// symlinks fail closed. The visit budget also bounds pathological trees of empty parent dirs.
fn count_known_streams(root: &Path, maximum: usize) -> Result<usize> {
    validate_real_directory(root, "receiver spool root")?;
    let visit_budget = maximum
        .checked_mul(8)
        .and_then(|value| value.checked_add(16))
        .context("known-stream traversal budget overflow")?;
    let mut visited = 0usize;
    let mut known = 0usize;

    for cluster in fs::read_dir(root)
        .with_context(|| format!("list receiver spool root {}", root.display()))?
    {
        let cluster = cluster.context("read receiver spool root entry")?;
        visit_entry(&mut visited, visit_budget)?;
        let name = directory_entry_name(&cluster)?;
        let file_type = cluster
            .file_type()
            .context("inspect receiver root entry type")?;
        if matches!(
            name.as_str(),
            KNOWN_STREAMS_LOCK_FILE | RECEIVER_CAPACITY_LOCK_FILE
        ) {
            ensure!(
                file_type.is_file() && !file_type.is_symlink(),
                "receiver root lock entry is not a regular file"
            );
            continue;
        }
        ensure!(
            file_type.is_dir() && !file_type.is_symlink() && is_stable_id(&name),
            "receiver root contains an invalid cluster directory"
        );
        for origin in fs::read_dir(cluster.path()).context("list receiver cluster directory")? {
            let origin = origin.context("read receiver cluster entry")?;
            visit_entry(&mut visited, visit_budget)?;
            validate_named_directory(&origin, "origin")?;
            for source in fs::read_dir(origin.path()).context("list receiver origin directory")? {
                let source = source.context("read receiver origin entry")?;
                visit_entry(&mut visited, visit_budget)?;
                validate_named_directory(&source, "source")?;
                for journal in
                    fs::read_dir(source.path()).context("list receiver source directory")?
                {
                    let journal = journal.context("read receiver source entry")?;
                    visit_entry(&mut visited, visit_budget)?;
                    let journal_name = directory_entry_name(&journal)?;
                    let journal_type = journal
                        .file_type()
                        .context("inspect receiver journal entry type")?;
                    ensure!(
                        journal_type.is_dir()
                            && !journal_type.is_symlink()
                            && is_lower_hex_journal_id(&journal_name),
                        "receiver source contains an invalid journal directory"
                    );
                    known = known
                        .checked_add(1)
                        .context("known-stream count overflow")?;
                    if known >= maximum {
                        return Ok(known);
                    }
                }
            }
        }
    }
    Ok(known)
}

fn visit_entry(visited: &mut usize, maximum: usize) -> Result<()> {
    *visited = visited
        .checked_add(1)
        .context("known-stream traversal count overflow")?;
    ensure!(
        *visited <= maximum,
        "known-stream directory tree exceeds its traversal budget"
    );
    Ok(())
}

fn validate_named_directory(entry: &fs::DirEntry, label: &str) -> Result<()> {
    let name = directory_entry_name(entry)?;
    let file_type = entry
        .file_type()
        .with_context(|| format!("inspect receiver {label} entry type"))?;
    ensure!(
        file_type.is_dir() && !file_type.is_symlink() && is_stable_id(&name),
        "receiver tree contains an invalid {label} directory"
    );
    Ok(())
}

fn directory_entry_name(entry: &fs::DirEntry) -> Result<String> {
    entry
        .file_name()
        .into_string()
        .map_err(|_| anyhow!("receiver tree contains a non-UTF-8 entry name"))
}

fn is_lower_hex_journal_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn read_secure_allowlist(path: &Path) -> Result<Vec<u8>> {
    let link_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect replication client allowlist {}", path.display()))?;
    ensure!(
        !link_metadata.file_type().is_symlink(),
        "replication client allowlist must not be a symbolic link"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let mut file = options
        .open(path)
        .with_context(|| format!("open replication client allowlist {}", path.display()))?;
    let metadata = file.metadata().with_context(|| {
        format!(
            "inspect opened replication client allowlist {}",
            path.display()
        )
    })?;
    ensure!(
        metadata.is_file(),
        "replication client allowlist must be a regular file"
    );
    ensure!(
        metadata.len() <= MAX_ALLOWLIST_BYTES,
        "replication client allowlist exceeds the size limit"
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.dev() == link_metadata.dev() && metadata.ino() == link_metadata.ino(),
            "replication client allowlist changed while opening"
        );
        ensure!(
            metadata.mode() & 0o022 == 0,
            "replication client allowlist must not be group- or world-writable"
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            metadata.uid() == effective_uid || metadata.uid() == 0,
            "replication client allowlist must be owned by this user or root"
        );
    }

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(MAX_ALLOWLIST_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read replication client allowlist {}", path.display()))?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "replication client allowlist changed while being read"
    );
    Ok(bytes)
}

fn parse_fingerprint(value: &str) -> Result<[u8; CERTIFICATE_FINGERPRINT_BYTES]> {
    ensure!(
        value.len() == CERTIFICATE_FINGERPRINT_BYTES * 2,
        "certificate SHA-256 fingerprint must contain 64 lowercase hexadecimal characters"
    );
    let mut output = [0u8; CERTIFICATE_FINGERPRINT_BYTES];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (lower_hex_nibble(pair[0])? << 4) | lower_hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn lower_hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(anyhow!(
            "certificate SHA-256 fingerprint must use lowercase hexadecimal"
        )),
    }
}

fn is_stable_id(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        && value.len() <= 64
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

fn stream_journal_directory(root: &Path, stream: &ReplicationStreamId) -> PathBuf {
    root.join(&stream.cluster_id)
        .join(&stream.origin_node_id)
        .join(&stream.source_id)
        .join(hex_journal_id(stream.journal_id))
}

fn hex_journal_id(journal_id: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in journal_id {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        CommitmentEvidence, LogicalKey, ObservationId, RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
        REPLICATION_PROTOCOL_VERSION, ReplicationOffer, compute_content_digest,
    };

    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::atomic::{AtomicU64, Ordering};

    use ed25519_dalek::{
        SigningKey,
        pkcs8::{EncodePrivateKey, spki::der::pem::LineEnding},
    };

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "blockzilla-replication-service-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fingerprint(bytes: &[u8]) -> String {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn allowlist_json(certificate: &[u8]) -> Vec<u8> {
        format!(
            r#"{{
                "schema_version": 1,
                "clients": [{{
                    "certificate_sha256": "{}",
                    "identities": [{{
                        "cluster_id": "mainnet",
                        "origin_node_id": "hetzner-relay",
                        "source_id": "yellowstone-blocks"
                    }}]
                }}]
            }}"#,
            fingerprint(certificate)
        )
        .into_bytes()
    }

    fn stream(origin: &str, source: &str) -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "mainnet".to_owned(),
            origin_node_id: origin.to_owned(),
            source_id: source.to_owned(),
            journal_id: [7; 16],
        }
    }

    fn receiver_limits() -> RawReceiverLimits {
        RawReceiverLimits {
            max_batch_records: 8,
            max_batch_compressed_bytes: 4096,
            max_batch_uncompressed_bytes: 8192,
            max_compressed_record_bytes: 2048,
            max_uncompressed_record_bytes: 4096,
        }
    }

    #[test]
    fn certificate_is_bound_to_exact_cluster_origin_and_source() {
        let certificate = b"leaf certificate DER";
        let allowlist = ClientCertificateAllowlist::from_json_bytes(&allowlist_json(certificate))
            .expect("parse allowlist");
        let authenticated = allowlist
            .authenticate_leaf(certificate)
            .expect("authenticate certificate");
        assert!(authenticated.permits(&stream("hetzner-relay", "yellowstone-blocks")));
        let mut wrong_cluster = stream("hetzner-relay", "yellowstone-blocks");
        wrong_cluster.cluster_id = "testnet".to_owned();
        assert!(!authenticated.permits(&wrong_cluster));
        assert!(!authenticated.permits(&stream("other-relay", "yellowstone-blocks")));
        assert!(!authenticated.permits(&stream("hetzner-relay", "other-source")));
        assert!(matches!(
            allowlist.authenticate_leaf(b"different certificate"),
            Err(AuthError::UnknownCertificate)
        ));
    }

    #[test]
    fn allowlist_rejects_unknown_fields_duplicates_and_noncanonical_hashes() {
        let certificate = b"certificate";
        let mut unknown: serde_json::Value =
            serde_json::from_slice(&allowlist_json(certificate)).unwrap();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(
            ClientCertificateAllowlist::from_json_bytes(&serde_json::to_vec(&unknown).unwrap())
                .is_err()
        );

        let hash = fingerprint(certificate);
        let duplicate = format!(
            r#"{{"schema_version":1,"clients":[
                {{"certificate_sha256":"{hash}","identities":[{{"cluster_id":"mainnet","origin_node_id":"node","source_id":"source"}}]}},
                {{"certificate_sha256":"{hash}","identities":[{{"cluster_id":"mainnet","origin_node_id":"node2","source_id":"source"}}]}}
            ]}}"#
        );
        assert!(ClientCertificateAllowlist::from_json_bytes(duplicate.as_bytes()).is_err());

        let uppercase_hash = fingerprint(certificate).to_ascii_uppercase();
        let uppercase = format!(
            r#"{{"schema_version":1,"clients":[{{
                "certificate_sha256":"{uppercase_hash}",
                "identities":[{{"cluster_id":"mainnet","origin_node_id":"node","source_id":"source"}}]
            }}]}}"#
        );
        assert!(ClientCertificateAllowlist::from_json_bytes(uppercase.as_bytes()).is_err());
    }

    #[test]
    fn allowlist_file_is_bounded_and_loaded_from_a_regular_file() {
        let directory = TestDirectory::new("allowlist");
        let path = directory.0.join("clients.json");
        fs::write(&path, allowlist_json(b"certificate")).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let allowlist = ClientCertificateAllowlist::load_json(&path).expect("load allowlist");
        assert_eq!(allowlist.certificate_count(), 1);

        #[cfg(unix)]
        {
            let link = directory.0.join("clients-link.json");
            symlink(&path, &link).unwrap();
            assert!(ClientCertificateAllowlist::load_json(link).is_err());
            fs::set_permissions(&path, fs::Permissions::from_mode(0o622)).unwrap();
            assert!(ClientCertificateAllowlist::load_json(&path).is_err());
        }
    }

    #[test]
    fn runtime_limits_cover_one_complete_wire_record() {
        let limits = receiver_limits();
        let valid = ReplicationServiceLimits {
            max_open_streams: 4,
            max_known_streams: 8,
            max_concurrent_requests: 2,
            max_decoding_message_bytes: 2048
                + usize::try_from(REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES).unwrap(),
            stream_idle_timeout: Duration::from_secs(5),
            stream_total_timeout: Duration::from_secs(30),
        };
        assert!(valid.validate(limits).is_ok());
        assert!(
            ReplicationServiceLimits {
                max_decoding_message_bytes: 2048,
                ..valid
            }
            .validate(limits)
            .is_err()
        );
        assert!(
            ReplicationServiceLimits {
                max_concurrent_requests: 0,
                ..valid
            }
            .validate(limits)
            .is_err()
        );
    }

    #[test]
    fn service_rejects_a_signer_key_id_outside_the_wire_identifier_grammar() {
        let directory = TestDirectory::new("signer-key-id");
        let private_key_path = directory.0.join("receipt.private.pem");
        let private_key = SigningKey::from_bytes(&[42; 32])
            .to_pkcs8_pem(LineEnding::LF)
            .expect("encode private key");
        fs::write(&private_key_path, private_key.as_bytes()).expect("write private key");
        #[cfg(unix)]
        fs::set_permissions(&private_key_path, fs::Permissions::from_mode(0o600))
            .expect("set private key permissions");

        // The generic signer format deliberately accepts longer printable labels, but the
        // replication wire grammar is stricter. Fail during service construction, not on the
        // first ACK after data has already been durably accepted.
        let signer = Ed25519ReceiptSigner::load_pkcs8_pem("x".repeat(65), &private_key_path)
            .expect("load signer with generic key id");
        let allowlist =
            ClientCertificateAllowlist::from_json_bytes(&allowlist_json(b"certificate"))
                .expect("parse allowlist");
        let primary_term = PrimaryTermStore::new(directory.0.join("primary-term"))
            .acquire()
            .expect("acquire primary term");
        let receiver_limits = receiver_limits();
        let result = RawReplicationService::new(
            ReplicationReceiverManagerConfig {
                spool_root: directory.0.join("spool"),
                primary_id: "blockzilla-primary".to_owned(),
                spool_options: SpoolOptions {
                    segment_target_bytes: 1024 * 1024,
                    max_record_bytes: 4096,
                },
                max_spool_bytes: 8 * 1024 * 1024,
                reserve_free_bytes: 0,
                receiver_limits,
            },
            ReplicationServiceLimits {
                max_open_streams: 1,
                max_known_streams: 1,
                max_concurrent_requests: 1,
                max_decoding_message_bytes: 2048
                    + usize::try_from(REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES).unwrap(),
                stream_idle_timeout: Duration::from_secs(1),
                stream_total_timeout: Duration::from_secs(2),
            },
            allowlist,
            signer,
            primary_term,
        );

        let error = result.expect_err("reject signer key id before serving");
        assert!(error.to_string().contains("stable 1..=64"));
    }

    #[test]
    fn absent_get_ack_does_not_create_the_spool_root() {
        let directory = TestDirectory::new("absent-get-ack");
        let root = directory.0.join("not-created");
        let manager = ReceiverManager::new(
            ReplicationReceiverManagerConfig {
                spool_root: root.clone(),
                primary_id: "blockzilla-primary".to_owned(),
                spool_options: SpoolOptions {
                    segment_target_bytes: 1024 * 1024,
                    max_record_bytes: 4096,
                },
                max_spool_bytes: 8 * 1024 * 1024,
                reserve_free_bytes: 0,
                receiver_limits: receiver_limits(),
            },
            1,
            2,
            4,
        );
        assert_eq!(manager.get_ack(stream("node", "source")).unwrap(), None);
        assert!(!root.exists());
    }

    #[test]
    fn invalid_new_batch_does_not_consume_a_journal_or_create_the_root() {
        let directory = TestDirectory::new("invalid-new-batch");
        let root = directory.0.join("not-created");
        let limits = receiver_limits();
        let manager = ReceiverManager::new(
            ReplicationReceiverManagerConfig {
                spool_root: root.clone(),
                primary_id: "blockzilla-primary".to_owned(),
                spool_options: SpoolOptions {
                    segment_target_bytes: 1024 * 1024,
                    max_record_bytes: 4096,
                },
                max_spool_bytes: 8 * 1024 * 1024,
                reserve_free_bytes: 0,
                receiver_limits: limits,
            },
            1,
            1,
            1,
        );
        let stream = stream("rotating-attacker", "source");
        let compressed_payload = b"valid envelope metadata but not a zstd frame".to_vec();
        let logical_key = LogicalKey::Block {
            slot: 42,
            blockhash: [9; 32],
        };
        let record = RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: stream.cluster_id.clone(),
                record: ObservationId {
                    origin_node_id: stream.origin_node_id.clone(),
                    journal_id: stream.journal_id,
                    sequence: 0,
                },
                source_id: stream.source_id.clone(),
                logical_key: logical_key.clone(),
                content_digest: compute_content_digest(
                    &stream.cluster_id,
                    &logical_key,
                    RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                    &compressed_payload,
                ),
                payload_len: compressed_payload.len() as u64,
                payload_format_version: RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                commitment: CommitmentEvidence::Confirmed,
            },
            compressed_payload,
            raw_protobuf_sha256: [0; 32],
            uncompressed_len: 1,
        };

        assert!(matches!(
            manager.push(stream.clone(), vec![record]),
            Err(ReceiverManagerError::Push(_))
        ));
        assert!(!root.exists());
        assert!(!stream_journal_directory(&root, &stream).exists());
    }

    #[test]
    fn open_stream_cap_evicts_only_the_lru_idle_receiver() {
        let directory = TestDirectory::new("receiver-lru");
        let manager = ReceiverManager::new(
            ReplicationReceiverManagerConfig {
                spool_root: directory.0.clone(),
                primary_id: "blockzilla-primary".to_owned(),
                spool_options: SpoolOptions {
                    segment_target_bytes: 1024 * 1024,
                    max_record_bytes: 4096,
                },
                max_spool_bytes: 8 * 1024 * 1024,
                reserve_free_bytes: 0,
                receiver_limits: receiver_limits(),
            },
            1,
            1,
            2,
        );
        let first_stream = stream("node-a", "source");
        let second_stream = stream("node-b", "source");

        let first = manager
            .receiver_for_push(&first_stream)
            .expect("open first receiver");
        assert!(matches!(
            manager.receiver_for_push(&second_stream),
            Err(ReceiverManagerError::OpenLimit)
        ));
        drop(first);

        let second = manager
            .receiver_for_push(&second_stream)
            .expect("evict idle receiver and open second");
        let registry = manager.registry.lock().unwrap();
        assert_eq!(registry.receivers.len(), 1);
        assert!(!registry.receivers.contains_key(&first_stream));
        assert!(registry.receivers.contains_key(&second_stream));
        assert_eq!(Arc::strong_count(&second), 2);
    }

    #[test]
    fn known_stream_cap_survives_restart_and_does_not_block_existing_journal() {
        let directory = TestDirectory::new("known-stream-cap");
        let config = ReplicationReceiverManagerConfig {
            spool_root: directory.0.clone(),
            primary_id: "blockzilla-primary".to_owned(),
            spool_options: SpoolOptions {
                segment_target_bytes: 1024 * 1024,
                max_record_bytes: 4096,
            },
            max_spool_bytes: 8 * 1024 * 1024,
            reserve_free_bytes: 0,
            receiver_limits: receiver_limits(),
        };
        let first_stream = stream("node-a", "source");
        let second_stream = stream("node-b", "source");

        let first_process = ReceiverManager::new(config.clone(), 1, 1, 1);
        drop(
            first_process
                .receiver_for_push(&first_stream)
                .expect("admit first persistent stream"),
        );
        drop(first_process);

        let restarted = ReceiverManager::new(config, 2, 1, 1);
        drop(
            restarted
                .receiver_for_push(&first_stream)
                .expect("existing stream remains admissible at the persistent cap"),
        );
        assert!(matches!(
            restarted.receiver_for_push(&second_stream),
            Err(ReceiverManagerError::KnownLimit)
        ));
        assert!(!stream_journal_directory(&directory.0, &second_stream).exists());
    }

    #[test]
    fn mapped_errors_do_not_disclose_underlying_paths_or_payloads() {
        let status = manager_error_status(ReceiverManagerError::Open(anyhow!(
            "secret path /private/key and token hunter2"
        )));
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(!status.message().contains("/private"));
        assert!(!status.message().contains("hunter2"));

        let status = wire_error_status(ReplicationWireError::InvalidValue {
            field: "secret-field",
        });
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(!status.message().contains("secret-field"));
    }
}
