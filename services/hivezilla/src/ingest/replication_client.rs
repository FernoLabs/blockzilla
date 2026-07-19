//! Bounded mTLS client transport for raw replication.
//!
//! This layer deliberately owns only TLS, gRPC limits, deadlines, and strict wire conversion.
//! Cursor selection, retries, durable ACK persistence, receipt verification, and garbage
//! collection belong to the replica state machine above it.

use std::{
    error::Error,
    fmt,
    fs::{self, OpenOptions},
    io::Read,
    path::Path,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use futures::stream;
use prost::Message;
use tokio::time::timeout;
use tonic::Code;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint, Identity,
};
use zeroize::Zeroizing;

use super::{
    ClientTlsConfig, CumulativePrimaryAck, GetAckState, REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES,
    RawReplicationRecord, ReplicaAuthConfig, ReplicaUpstreamConfig, ReplicationStreamId,
    ValidatedPushRecord, wire,
};

const MAX_TLS_CERTIFICATE_FILE_BYTES: u64 = 1024 * 1024;
const MAX_TLS_PRIVATE_KEY_FILE_BYTES: u64 = 64 * 1024;
const MAX_WIRE_REQUEST_BYTES: usize = 256 * 1024 * 1024;
const MAX_WIRE_RESPONSE_BYTES: usize = 64 * 1024;
const REPLICATION_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const REPLICATION_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Stable, non-secret failure categories suitable for logs and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationClientErrorKind {
    InvalidConfiguration,
    CredentialFile,
    TlsConfiguration,
    ConnectTimeout,
    Connect,
    BatchLimit,
    WireEncoding,
    RequestTimeout,
    Rpc(Code),
    ResponseMismatch,
}

/// A deliberately redacted transport error.
///
/// Neither `Display` nor `Debug` includes an endpoint, credential path, remote status message, or
/// underlying TLS error. Use [`ReplicationClientError::kind`] for programmatic handling.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ReplicationClientError {
    kind: ReplicationClientErrorKind,
}

impl ReplicationClientError {
    fn new(kind: ReplicationClientErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(&self) -> ReplicationClientErrorKind {
        self.kind
    }
}

impl fmt::Display for ReplicationClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            ReplicationClientErrorKind::InvalidConfiguration => {
                "invalid replication client configuration"
            }
            ReplicationClientErrorKind::CredentialFile => {
                "replication TLS credential file is unavailable or unsafe"
            }
            ReplicationClientErrorKind::TlsConfiguration => "invalid replication TLS configuration",
            ReplicationClientErrorKind::ConnectTimeout => {
                "replication upstream connection timed out"
            }
            ReplicationClientErrorKind::Connect => "replication upstream connection failed",
            ReplicationClientErrorKind::BatchLimit => {
                "replication batch exceeds its configured limit"
            }
            ReplicationClientErrorKind::WireEncoding => {
                "replication message failed strict wire validation"
            }
            ReplicationClientErrorKind::RequestTimeout => "replication request timed out",
            ReplicationClientErrorKind::Rpc(_) => "replication RPC failed",
            ReplicationClientErrorKind::ResponseMismatch => {
                "replication response does not match the requested stream"
            }
        };
        formatter.write_str(message)
    }
}

impl fmt::Debug for ReplicationClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationClientError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl Error for ReplicationClientError {}

/// Checked transport limits derived from one validated upstream configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationClientLimits {
    max_batch_events: usize,
    max_batch_compressed_bytes: u64,
    max_batch_uncompressed_bytes: u64,
    max_compressed_event_bytes: u64,
    max_encoding_message_bytes: usize,
    max_decoding_message_bytes: usize,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ReplicationClientLimits {
    fn from_config(config: &ReplicaUpstreamConfig) -> Result<Self, ReplicationClientError> {
        if config.batch.max_events == 0
            || config.batch.max_bytes == 0
            || config.batch.max_uncompressed_bytes == 0
            || config.batch.max_compressed_event_bytes == 0
            || config.batch.max_compressed_event_bytes > config.batch.max_bytes
            || config.batch.max_uncompressed_event_bytes == 0
            || config.batch.max_uncompressed_event_bytes > config.batch.max_uncompressed_bytes
            || config.batch.ack_timeout_ms == 0
            || config.reconnect.connect_timeout_ms == 0
        {
            return Err(invalid_configuration());
        }

        let configured_wire_bytes = config
            .batch
            .max_compressed_event_bytes
            .checked_add(REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES)
            .ok_or_else(invalid_configuration)?;
        let configured_wire_bytes =
            usize::try_from(configured_wire_bytes).map_err(|_| invalid_configuration())?;

        Ok(Self {
            max_batch_events: config.batch.max_events,
            max_batch_compressed_bytes: config.batch.max_bytes,
            max_batch_uncompressed_bytes: config.batch.max_uncompressed_bytes,
            max_compressed_event_bytes: config.batch.max_compressed_event_bytes,
            // The receiver has the same hard per-message safety ceiling. A large cumulative
            // batch remains valid as long as each streamed record fits one bounded message.
            max_encoding_message_bytes: configured_wire_bytes.min(MAX_WIRE_REQUEST_BYTES),
            max_decoding_message_bytes: MAX_WIRE_RESPONSE_BYTES,
            connect_timeout: Duration::from_millis(config.reconnect.connect_timeout_ms),
            request_timeout: Duration::from_millis(config.batch.ack_timeout_ms),
        })
    }

    pub fn max_batch_events(self) -> usize {
        self.max_batch_events
    }

    pub fn max_batch_compressed_bytes(self) -> u64 {
        self.max_batch_compressed_bytes
    }

    pub fn max_batch_uncompressed_bytes(self) -> u64 {
        self.max_batch_uncompressed_bytes
    }

    pub fn max_compressed_event_bytes(self) -> u64 {
        self.max_compressed_event_bytes
    }

    pub fn max_encoding_message_bytes(self) -> usize {
        self.max_encoding_message_bytes
    }

    pub fn max_decoding_message_bytes(self) -> usize {
        self.max_decoding_message_bytes
    }

    pub fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    pub fn request_timeout(self) -> Duration {
        self.request_timeout
    }
}

/// Cloneable, bounded raw replication transport.
///
/// The channel contains client credentials, so its custom `Debug` implementation exposes limits
/// only. Receipt signatures returned by this client are intentionally not trusted until the
/// caller verifies them against `ReplicaUpstreamConfig::trusted_receipt_keys`.
#[derive(Clone)]
pub struct RawReplicationClient {
    channel: Channel,
    limits: ReplicationClientLimits,
}

impl RawReplicationClient {
    /// Connect to the configured upstream using an explicit CA and client identity.
    ///
    /// `config` is expected to have passed `IngestConfig::validate`, but security-critical
    /// transport invariants are checked again here so a direct programmatic caller fails closed.
    pub async fn connect(config: &ReplicaUpstreamConfig) -> Result<Self, ReplicationClientError> {
        validate_mtls_config(config)?;
        let limits = ReplicationClientLimits::from_config(config)?;
        let channel = connect_strict_mtls_channel(
            &config.endpoint,
            &config.tls,
            limits.connect_timeout,
            limits.request_timeout,
        )
        .await?;
        Ok(Self { channel, limits })
    }

    pub fn limits(&self) -> ReplicationClientLimits {
        self.limits
    }

    /// Poll the current durable prefix. The caller must verify a present ACK signature before it
    /// may authorize deletion.
    pub async fn get_ack(
        &self,
        stream: &ReplicationStreamId,
    ) -> Result<GetAckState, ReplicationClientError> {
        let request = wire::GetAckRequest::try_from(stream).map_err(|_| wire_error())?;
        let mut client = self.rpc_client();
        let response = timeout(self.limits.request_timeout, client.get_ack(request))
            .await
            .map_err(|_| request_timeout())?
            .map_err(rpc_error)?
            .into_inner();
        let state = GetAckState::try_from(response).map_err(|_| wire_error())?;
        if let GetAckState::DurablePrefix(ack) = &state
            && &ack.stream != stream
        {
            return Err(ReplicationClientError::new(
                ReplicationClientErrorKind::ResponseMismatch,
            ));
        }
        Ok(state)
    }

    /// Submit one non-empty, fully prevalidated batch as one gRPC stream message per record.
    /// The caller must verify the returned cumulative ACK before using it for durable deletion.
    pub async fn push_batch(
        &self,
        stream_id: &ReplicationStreamId,
        records: Vec<RawReplicationRecord>,
    ) -> Result<CumulativePrimaryAck, ReplicationClientError> {
        let requests = build_push_requests(self.limits, stream_id, records)?;
        let mut client = self.rpc_client();
        let response = timeout(
            self.limits.request_timeout,
            client.push_batch(stream::iter(requests)),
        )
        .await
        .map_err(|_| request_timeout())?
        .map_err(rpc_error)?
        .into_inner();
        let ack = CumulativePrimaryAck::try_from(response).map_err(|_| wire_error())?;
        if &ack.stream != stream_id {
            return Err(ReplicationClientError::new(
                ReplicationClientErrorKind::ResponseMismatch,
            ));
        }
        Ok(ack)
    }

    fn rpc_client(&self) -> wire::raw_replication_client::RawReplicationClient<Channel> {
        wire::raw_replication_client::RawReplicationClient::new(self.channel.clone())
            .max_encoding_message_size(self.limits.max_encoding_message_bytes)
            .max_decoding_message_size(self.limits.max_decoding_message_bytes)
    }
}

/// Establish the same fail-closed mTLS channel used by both replication directions.
///
/// This stays crate-private so callers cannot accidentally bypass either protocol's bounded RPC
/// wrapper and operate directly on a credential-bearing channel.
pub(crate) async fn connect_strict_mtls_channel(
    endpoint: &str,
    tls_config: &ClientTlsConfig,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Result<Channel, ReplicationClientError> {
    if connect_timeout.is_zero() || request_timeout.is_zero() {
        return Err(invalid_configuration());
    }
    validate_client_tls_config(tls_config)?;
    let endpoint_uri = normalize_secure_endpoint(endpoint)?;
    let server_name = tls_config
        .server_name
        .as_deref()
        .ok_or_else(invalid_configuration)?;
    if !valid_server_name(server_name) {
        return Err(invalid_configuration());
    }

    let (ca, identity) = load_client_tls_material(tls_config)?;
    let tls = TonicClientTlsConfig::new()
        .ca_certificate(ca)
        .identity(identity)
        .domain_name(server_name.to_owned());
    let endpoint = Endpoint::from_shared(endpoint_uri)
        .map_err(|_| invalid_configuration())?
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .http2_keep_alive_interval(REPLICATION_HTTP2_KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(REPLICATION_HTTP2_KEEP_ALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .tls_config(tls)
        .map_err(|_| ReplicationClientError::new(ReplicationClientErrorKind::TlsConfiguration))?;

    match timeout(connect_timeout, endpoint.connect()).await {
        Ok(Ok(channel)) => Ok(channel),
        Ok(Err(_)) => Err(ReplicationClientError::new(
            ReplicationClientErrorKind::Connect,
        )),
        Err(_) => Err(ReplicationClientError::new(
            ReplicationClientErrorKind::ConnectTimeout,
        )),
    }
}

impl fmt::Debug for RawReplicationClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationClient")
            .field("transport", &"mTLS")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

fn validate_mtls_config(config: &ReplicaUpstreamConfig) -> Result<(), ReplicationClientError> {
    if !matches!(config.auth, ReplicaAuthConfig::MutualTls) {
        return Err(invalid_configuration());
    }
    validate_client_tls_config(&config.tls)
}

fn validate_client_tls_config(config: &ClientTlsConfig) -> Result<(), ReplicationClientError> {
    if config.ca_file.is_none()
        || config.client_certificate_file.is_none()
        || config.client_private_key_file.is_none()
        || config.server_name.is_none()
    {
        return Err(invalid_configuration());
    }
    Ok(())
}

fn normalize_secure_endpoint(endpoint: &str) -> Result<String, ReplicationClientError> {
    let normalized = if endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else if let Some(authority) = endpoint.strip_prefix("grpcs://") {
        format!("https://{authority}")
    } else {
        return Err(invalid_configuration());
    };
    let remainder = normalized
        .strip_prefix("https://")
        .ok_or_else(invalid_configuration)?;
    let authority = remainder.split('/').next().unwrap_or_default();
    if authority.is_empty()
        || authority.contains('@')
        || normalized.chars().any(char::is_whitespace)
        || normalized.contains('?')
        || normalized.contains('#')
    {
        return Err(invalid_configuration());
    }
    Ok(normalized)
}

fn valid_server_name(server_name: &str) -> bool {
    rustls::pki_types::ServerName::try_from(server_name.to_owned()).is_ok()
}

fn build_push_requests(
    limits: ReplicationClientLimits,
    stream_id: &ReplicationStreamId,
    records: Vec<RawReplicationRecord>,
) -> Result<Vec<wire::PushBatchRequest>, ReplicationClientError> {
    if records.is_empty() {
        return Err(wire_error());
    }
    if records.len() > limits.max_batch_events {
        return Err(batch_limit());
    }

    let mut compressed_bytes = 0u64;
    let mut uncompressed_bytes = 0u64;
    let mut requests = Vec::with_capacity(records.len());
    for record in records {
        let record_bytes =
            u64::try_from(record.compressed_payload.len()).map_err(|_| batch_limit())?;
        if record_bytes > limits.max_compressed_event_bytes {
            return Err(batch_limit());
        }
        compressed_bytes = compressed_bytes
            .checked_add(record_bytes)
            .ok_or_else(batch_limit)?;
        if compressed_bytes > limits.max_batch_compressed_bytes {
            return Err(batch_limit());
        }
        uncompressed_bytes = uncompressed_bytes
            .checked_add(record.uncompressed_len)
            .ok_or_else(batch_limit)?;
        if uncompressed_bytes > limits.max_batch_uncompressed_bytes {
            return Err(batch_limit());
        }

        let validated =
            ValidatedPushRecord::new(stream_id.clone(), record).map_err(|_| wire_error())?;
        let request = wire::PushBatchRequest::try_from(validated).map_err(|_| wire_error())?;
        if request.encoded_len() > limits.max_encoding_message_bytes {
            return Err(batch_limit());
        }
        requests.push(request);
    }
    Ok(requests)
}

fn load_client_tls_material(
    config: &ClientTlsConfig,
) -> Result<(Certificate, Identity), ReplicationClientError> {
    let ca_path = config
        .ca_file
        .as_deref()
        .ok_or_else(invalid_configuration)?;
    let certificate_path = config
        .client_certificate_file
        .as_deref()
        .ok_or_else(invalid_configuration)?;
    let private_key_path = config
        .client_private_key_file
        .as_deref()
        .ok_or_else(invalid_configuration)?;

    let ca = read_credential_file(
        ca_path,
        CredentialFileKind::Public,
        MAX_TLS_CERTIFICATE_FILE_BYTES,
    )?;
    let certificate = read_credential_file(
        certificate_path,
        CredentialFileKind::Public,
        MAX_TLS_CERTIFICATE_FILE_BYTES,
    )?;
    let private_key = Zeroizing::new(read_credential_file(
        private_key_path,
        CredentialFileKind::Private,
        MAX_TLS_PRIVATE_KEY_FILE_BYTES,
    )?);

    if !contains_pem_begin(&ca)
        || !contains_pem_begin(&certificate)
        || !contains_pem_begin(&private_key[..])
    {
        return Err(ReplicationClientError::new(
            ReplicationClientErrorKind::TlsConfiguration,
        ));
    }

    let identity = Identity::from_pem(certificate, &private_key[..]);
    Ok((Certificate::from_pem(ca), identity))
}

#[derive(Clone, Copy)]
enum CredentialFileKind {
    Public,
    Private,
}

fn read_credential_file(
    path: &Path,
    kind: CredentialFileKind,
    maximum: u64,
) -> Result<Vec<u8>, ReplicationClientError> {
    if !path.is_absolute() {
        return Err(credential_error());
    }
    let link_metadata = fs::symlink_metadata(path).map_err(|_| credential_error())?;
    if !link_metadata.is_file() || link_metadata.file_type().is_symlink() {
        return Err(credential_error());
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut file = options.open(path).map_err(|_| credential_error())?;
    let metadata = file.metadata().map_err(|_| credential_error())?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(credential_error());
    }
    validate_credential_metadata(&metadata, &link_metadata, kind)?;

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| credential_error())?;
    if bytes.len() as u64 != metadata.len() {
        return Err(credential_error());
    }

    // A replacement cannot change the already-open inode, but rejecting it avoids accepting
    // credentials from a path whose operator-visible identity changed during validation. Repeat
    // the mode and owner checks as well so a concurrent chmod also fails closed.
    let final_opened_metadata = file.metadata().map_err(|_| credential_error())?;
    let final_link_metadata = fs::symlink_metadata(path).map_err(|_| credential_error())?;
    if final_opened_metadata.len() != metadata.len() {
        return Err(credential_error());
    }
    validate_credential_metadata(&final_opened_metadata, &final_link_metadata, kind)?;
    Ok(bytes)
}

fn validate_credential_metadata(
    opened: &fs::Metadata,
    linked: &fs::Metadata,
    kind: CredentialFileKind,
) -> Result<(), ReplicationClientError> {
    if !linked.is_file() {
        return Err(credential_error());
    }
    #[cfg(unix)]
    {
        if opened.dev() != linked.dev() || opened.ino() != linked.ino() {
            return Err(credential_error());
        }
        let forbidden_mode = match kind {
            CredentialFileKind::Public => 0o022,
            CredentialFileKind::Private => 0o077,
        };
        if opened.mode() & forbidden_mode != 0 {
            return Err(credential_error());
        }
        let effective_uid = unsafe { libc::geteuid() };
        if opened.uid() != effective_uid && opened.uid() != 0 {
            return Err(credential_error());
        }
    }
    #[cfg(not(unix))]
    let _ = (opened, linked, kind);
    Ok(())
}

fn contains_pem_begin(bytes: &[u8]) -> bool {
    const PEM_BEGIN: &[u8] = b"-----BEGIN ";
    bytes
        .windows(PEM_BEGIN.len())
        .any(|window| window == PEM_BEGIN)
}

fn invalid_configuration() -> ReplicationClientError {
    ReplicationClientError::new(ReplicationClientErrorKind::InvalidConfiguration)
}

fn credential_error() -> ReplicationClientError {
    ReplicationClientError::new(ReplicationClientErrorKind::CredentialFile)
}

fn batch_limit() -> ReplicationClientError {
    ReplicationClientError::new(ReplicationClientErrorKind::BatchLimit)
}

fn wire_error() -> ReplicationClientError {
    ReplicationClientError::new(ReplicationClientErrorKind::WireEncoding)
}

fn request_timeout() -> ReplicationClientError {
    ReplicationClientError::new(ReplicationClientErrorKind::RequestTimeout)
}

fn rpc_error(status: tonic::Status) -> ReplicationClientError {
    ReplicationClientError::new(ReplicationClientErrorKind::Rpc(status.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        ClientTlsConfig, CommitmentEvidence, LogicalKey, ObservationId,
        RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1, REPLICATION_PROTOCOL_VERSION, ReconnectConfig,
        ReplicaBatchLimits, ReplicationOffer, SecretRef, TrustedReceiptKeyConfig,
        compute_content_digest,
    };
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "blockzilla-replication-client-{label}-{}-{sequence}",
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

    fn config(directory: &TestDirectory) -> ReplicaUpstreamConfig {
        ReplicaUpstreamConfig {
            endpoint: "grpcs://primary.example:8443".to_owned(),
            expected_primary_id: "blockzilla-primary".to_owned(),
            cumulative_ack_wal_file: directory.0.join("cumulative-ack.wal"),
            tls: ClientTlsConfig {
                ca_file: Some(directory.0.join("ca.pem")),
                client_certificate_file: Some(directory.0.join("client.pem")),
                client_private_key_file: Some(directory.0.join("client-key.pem")),
                server_name: Some("primary.example".to_owned()),
            },
            auth: ReplicaAuthConfig::MutualTls,
            trusted_receipt_keys: vec![TrustedReceiptKeyConfig {
                key_id: "receipt-key".to_owned(),
                public_key_file: directory.0.join("receipt.pem"),
            }],
            reconnect: ReconnectConfig {
                initial_delay_ms: 10,
                max_delay_ms: 100,
                backoff_factor_milli: 2_000,
                jitter_percent: 10,
                connect_timeout_ms: 250,
                heartbeat_interval_ms: 1_000,
                idle_timeout_ms: 2_000,
                max_attempts: None,
            },
            batch: ReplicaBatchLimits {
                max_events: 2,
                max_bytes: 1024,
                max_uncompressed_bytes: 4096,
                max_compressed_event_bytes: 1024,
                max_uncompressed_event_bytes: 4096,
                ack_timeout_ms: 500,
            },
        }
    }

    fn stream_id() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "mainnet".to_owned(),
            origin_node_id: "source-node-replica".to_owned(),
            source_id: "yellowstone-blocks".to_owned(),
            journal_id: [7; 16],
        }
    }

    fn record(stream: &ReplicationStreamId, sequence: u64, bytes: usize) -> RawReplicationRecord {
        let compressed_payload = vec![sequence as u8; bytes];
        let logical_key = LogicalKey::Block {
            slot: sequence + 100,
            blockhash: [sequence as u8; 32],
        };
        RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: stream.cluster_id.clone(),
                record: ObservationId {
                    origin_node_id: stream.origin_node_id.clone(),
                    journal_id: stream.journal_id,
                    sequence,
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
            raw_protobuf_sha256: [9; 32],
            uncompressed_len: 2048,
        }
    }

    #[test]
    fn config_requires_mtls_complete_identity_and_server_name() {
        let directory = TestDirectory::new("config");
        let valid = config(&directory);
        assert!(validate_mtls_config(&valid).is_ok());

        let mut bearer = valid.clone();
        bearer.auth = ReplicaAuthConfig::Bearer {
            credential: SecretRef::Env {
                variable: "TOKEN".to_owned(),
            },
        };
        assert_eq!(
            validate_mtls_config(&bearer).unwrap_err().kind(),
            ReplicationClientErrorKind::InvalidConfiguration
        );

        let mut missing_ca = valid.clone();
        missing_ca.tls.ca_file = None;
        assert!(validate_mtls_config(&missing_ca).is_err());
        let mut missing_name = valid;
        missing_name.tls.server_name = None;
        assert!(validate_mtls_config(&missing_name).is_err());
    }

    #[test]
    fn endpoint_and_limits_are_normalized_and_bounded() {
        let directory = TestDirectory::new("limits");
        let mut config = config(&directory);
        assert_eq!(
            normalize_secure_endpoint(&config.endpoint).unwrap(),
            "https://primary.example:8443"
        );
        assert!(normalize_secure_endpoint("http://primary.example").is_err());
        assert!(normalize_secure_endpoint("https://user@primary.example").is_err());
        assert!(normalize_secure_endpoint("https://primary.example?token=secret").is_err());

        let limits = ReplicationClientLimits::from_config(&config).unwrap();
        assert_eq!(limits.max_batch_events(), 2);
        assert_eq!(limits.max_batch_compressed_bytes(), 1024);
        assert_eq!(limits.max_batch_uncompressed_bytes(), 4096);
        assert_eq!(
            limits.max_encoding_message_bytes(),
            1024 + REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES as usize
        );
        assert_eq!(limits.max_decoding_message_bytes(), 64 * 1024);
        assert_eq!(limits.connect_timeout(), Duration::from_millis(250));
        assert_eq!(limits.request_timeout(), Duration::from_millis(500));

        config.batch.max_bytes = u64::MAX;
        config.batch.max_compressed_event_bytes = u64::MAX;
        assert!(ReplicationClientLimits::from_config(&config).is_err());
    }

    #[test]
    fn push_wire_is_one_record_per_message_and_prevalidates_all_caps() {
        let directory = TestDirectory::new("wire");
        let config = config(&directory);
        let limits = ReplicationClientLimits::from_config(&config).unwrap();
        let stream = stream_id();
        let requests = build_push_requests(
            limits,
            &stream,
            vec![record(&stream, 0, 400), record(&stream, 1, 500)],
        )
        .expect("build bounded batch");
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| request.record.is_some()));
        assert!(requests.iter().all(|request| request.stream.is_some()));

        let count_error = build_push_requests(
            limits,
            &stream,
            vec![
                record(&stream, 0, 1),
                record(&stream, 1, 1),
                record(&stream, 2, 1),
            ],
        )
        .unwrap_err();
        assert_eq!(count_error.kind(), ReplicationClientErrorKind::BatchLimit);

        let bytes_error = build_push_requests(
            limits,
            &stream,
            vec![record(&stream, 0, 600), record(&stream, 1, 600)],
        )
        .unwrap_err();
        assert_eq!(bytes_error.kind(), ReplicationClientErrorKind::BatchLimit);

        let mut first = record(&stream, 0, 1);
        first.uncompressed_len = 3_000;
        let mut second = record(&stream, 1, 1);
        second.uncompressed_len = 3_000;
        let uncompressed_error = build_push_requests(limits, &stream, vec![first, second])
            .expect_err("cumulative uncompressed bytes must be bounded");
        assert_eq!(
            uncompressed_error.kind(),
            ReplicationClientErrorKind::BatchLimit
        );

        let mut wrong_stream = stream.clone();
        wrong_stream.origin_node_id = "another-node".to_owned();
        let stream_error =
            build_push_requests(limits, &stream, vec![record(&wrong_stream, 0, 1)]).unwrap_err();
        assert_eq!(
            stream_error.kind(),
            ReplicationClientErrorKind::WireEncoding
        );
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_or_linked_private_key_is_rejected_without_path_disclosure() {
        let directory = TestDirectory::new("unsafe-key");
        let key = directory.0.join("client-key.pem");
        fs::write(&key, b"-----BEGIN PRIVATE KEY-----\nredacted\n").unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();

        let error = read_credential_file(
            &key,
            CredentialFileKind::Private,
            MAX_TLS_PRIVATE_KEY_FILE_BYTES,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ReplicationClientErrorKind::CredentialFile);
        assert!(!format!("{error}").contains("client-key.pem"));
        assert!(!format!("{error:?}").contains("client-key.pem"));

        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            read_credential_file(&key, CredentialFileKind::Private, 4).is_err(),
            "credential reads remain bounded even for otherwise safe files"
        );
        let link = directory.0.join("linked-key.pem");
        symlink(&key, &link).unwrap();
        assert!(
            read_credential_file(
                &link,
                CredentialFileKind::Private,
                MAX_TLS_PRIVATE_KEY_FILE_BYTES
            )
            .is_err()
        );

        let non_file_path = directory.0.join("not-a-key");
        fs::create_dir(&non_file_path).unwrap();
        assert!(
            read_credential_file(
                &non_file_path,
                CredentialFileKind::Private,
                MAX_TLS_PRIVATE_KEY_FILE_BYTES
            )
            .is_err()
        );
    }

    #[test]
    fn remote_status_details_are_redacted_but_the_code_is_retained() {
        let error = rpc_error(tonic::Status::permission_denied(
            "secret endpoint, credential path, and peer detail",
        ));
        assert_eq!(
            error.kind(),
            ReplicationClientErrorKind::Rpc(Code::PermissionDenied)
        );
        assert!(!format!("{error}").contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
    }
}
