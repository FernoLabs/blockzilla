use std::{
    collections::HashSet,
    error::Error,
    fmt,
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

/// The only ingest configuration schema understood by this implementation.
pub const INGEST_CONFIG_SCHEMA_VERSION: u32 = 2;
/// Reserved protobuf envelope space above the compressed payload in each streamed record.
pub const REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES: u64 = 64 * 1024;
/// Conservative heap allowance for IDs, offers, Vec entries, and allocator metadata per record.
pub const REPLICATION_RECORD_MEMORY_RESERVE_BYTES: u64 = 64 * 1024;

/// Complete configuration for redundant live ingestion.
///
/// Credential material is deliberately absent from this schema. Every secret is
/// represented by [`SecretRef`], which points at an environment variable or an
/// absolute file path that the runtime may resolve later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestConfig {
    pub schema_version: u32,
    /// Stable chain/cluster identity (for example the genesis hash or an operator-controlled id).
    pub cluster_id: String,
    /// Process-wide memory ceiling across enabled source queues and decoded listener requests.
    pub max_total_queue_bytes: u64,
    /// Process-wide item ceiling; complements the byte budget for tiny events.
    pub max_total_queue_events: usize,
    pub role: IngestRoleConfig,
    pub spool: SpoolConfig,
    pub sources: Vec<IngestSourceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum IngestRoleConfig {
    Primary {
        node_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_listener: Option<ReplicaListenerConfig>,
    },
    #[serde(alias = "slave")]
    Replica {
        node_id: String,
        upstream: ReplicaUpstreamConfig,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaListenerConfig {
    pub bind: String,
    pub server_certificate_file: PathBuf,
    pub server_private_key_file: PathBuf,
    pub client_ca_file: PathBuf,
    pub allowed_nodes_file: PathBuf,
    /// Durable primary leadership term used to reject stale senders after failover.
    pub primary_term_file: PathBuf,
    pub receipt_signing_key_id: String,
    pub receipt_signing_key: SecretRef,
    /// Hard tonic/protobuf decoding ceiling for one streamed record, including wire metadata.
    pub max_wire_request_bytes: u64,
    pub max_batch_events: usize,
    pub max_batch_compressed_bytes: u64,
    pub max_batch_uncompressed_bytes: u64,
    pub max_compressed_event_bytes: u64,
    pub max_uncompressed_event_bytes: u64,
    pub max_concurrent_requests: usize,
    pub max_open_streams: usize,
    pub request_idle_timeout_ms: u64,
    pub request_total_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaUpstreamConfig {
    pub endpoint: String,
    pub tls: ClientTlsConfig,
    pub auth: ReplicaAuthConfig,
    /// Public keys accepted for durable deletion receipts. Keep old and new keys during rotation.
    pub trusted_receipt_keys: Vec<TrustedReceiptKeyConfig>,
    pub reconnect: ReconnectConfig,
    pub batch: ReplicaBatchLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedReceiptKeyConfig {
    pub key_id: String,
    pub public_key_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaBatchLimits {
    pub max_events: usize,
    pub max_bytes: u64,
    pub ack_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicaAuthConfig {
    MutualTls,
    Bearer {
        credential: SecretRef,
    },
    Ed25519 {
        key_id: String,
        signing_key: SecretRef,
        upstream_public_key_file: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSourceConfig {
    /// Stable audit identity. It must not change when an endpoint or credential rotates.
    pub id: String,
    pub enabled: bool,
    /// Lower values win only after commitment/completeness checks agree.
    pub priority: u16,
    pub required_for_epoch_close: bool,
    /// Per-source limits prevent one stalled feed from consuming all process memory.
    pub queue: QueueLimits,
    pub input: SourceInputConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceInputConfig {
    Grpc {
        endpoint: String,
        commitment: GrpcCommitment,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<SourceAuthConfig>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tls: Option<ClientTlsConfig>,
        reconnect: ReconnectConfig,
        /// Re-requesting an overlap is intentional; the dedup layer must be idempotent.
        resume_overlap_slots: u64,
    },
    ShredUdp {
        bind: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        multicast_group: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interface: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<SourceAuthConfig>,
    },
    ShredQuic {
        endpoint: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<SourceAuthConfig>,
        tls: ClientTlsConfig,
        reconnect: ReconnectConfig,
        resume_overlap_events: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrpcCommitment {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueLimits {
    pub max_events: usize,
    pub max_bytes: u64,
    /// Reject a single event before it can consume the whole byte budget.
    pub max_event_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectConfig {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Integer fixed point: 2000 means a 2.0x exponential backoff.
    pub backoff_factor_milli: u32,
    pub jitter_percent: u8,
    pub connect_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub idle_timeout_ms: u64,
    /// `null` means retry forever. A finite value must be non-zero.
    pub max_attempts: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTlsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_private_key_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceAuthConfig {
    XToken {
        credential: SecretRef,
    },
    Bearer {
        credential: SecretRef,
    },
    HmacSha256 {
        key_id: String,
        credential: SecretRef,
    },
}

/// A reference to credential material. There is intentionally no literal/value variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretRef {
    Env { variable: String },
    File { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpoolConfig {
    pub root: PathBuf,
    pub max_bytes: u64,
    pub segment_bytes: u64,
    pub reserve_free_bytes: u64,
    pub sync: SpoolSyncConfig,
    pub full_policy: SpoolFullPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpoolSyncConfig {
    /// Maximum time an accepted event may remain outside stable storage.
    pub max_delay_ms: u64,
    /// Force a sync after this many unsynced bytes even if the timer has not fired.
    pub max_unsynced_bytes: u64,
}

/// Lossy policies are intentionally not representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpoolFullPolicy {
    PauseSources,
    FailClosed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RedactedIngestConfigSummary {
    pub schema_version: u32,
    pub cluster_id: String,
    pub max_total_queue_bytes: u64,
    pub configured_enabled_queue_bytes: u64,
    pub max_total_queue_events: usize,
    pub configured_enabled_queue_events: usize,
    pub role: &'static str,
    pub node_id: String,
    pub accepts_replicas: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_listener: Option<RedactedReplicaListenerSummary>,
    pub source_count: usize,
    pub enabled_source_count: usize,
    pub spool_max_bytes: u64,
    pub spool_segment_bytes: u64,
    pub sources: Vec<RedactedSourceSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RedactedReplicaListenerSummary {
    pub max_wire_request_bytes: u64,
    pub max_batch_events: usize,
    pub max_batch_compressed_bytes: u64,
    pub max_batch_uncompressed_bytes: u64,
    pub max_compressed_event_bytes: u64,
    pub max_uncompressed_event_bytes: u64,
    pub max_concurrent_requests: usize,
    pub max_open_streams: usize,
    pub request_idle_timeout_ms: u64,
    pub request_total_timeout_ms: u64,
    pub max_in_flight_events: usize,
    /// Worst-case buffered batch, record metadata, decoded wire record, and decompression buffer.
    pub max_in_flight_memory_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RedactedSourceSummary {
    pub id: String,
    pub enabled: bool,
    pub kind: &'static str,
    pub priority: u16,
    pub required_for_epoch_close: bool,
    pub auth_method: Option<&'static str>,
    pub queue_max_events: usize,
    pub queue_max_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestConfigValidationError {
    issues: Vec<String>,
}

impl IngestConfigValidationError {
    pub fn issues(&self) -> &[String] {
        &self.issues
    }
}

impl fmt::Display for IngestConfigValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid ingest configuration")?;
        for issue in &self.issues {
            write!(formatter, "; {issue}")?;
        }
        Ok(())
    }
}

impl Error for IngestConfigValidationError {}

#[derive(Debug)]
pub enum IngestConfigLoadError {
    Json(serde_json::Error),
    Validation(IngestConfigValidationError),
}

impl fmt::Display for IngestConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "decode ingest configuration: {error}"),
            Self::Validation(error) => error.fmt(formatter),
        }
    }
}

impl Error for IngestConfigLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::Validation(error) => Some(error),
        }
    }
}

impl From<serde_json::Error> for IngestConfigLoadError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<IngestConfigValidationError> for IngestConfigLoadError {
    fn from(value: IngestConfigValidationError) -> Self {
        Self::Validation(value)
    }
}

impl IngestConfig {
    pub fn from_json(json: &str) -> Result<Self, IngestConfigLoadError> {
        let config = serde_json::from_str::<Self>(json)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), IngestConfigValidationError> {
        let mut issues = Vec::new();

        if self.schema_version != INGEST_CONFIG_SCHEMA_VERSION {
            issues.push(format!(
                "schema_version {} is unsupported; expected {}",
                self.schema_version, INGEST_CONFIG_SCHEMA_VERSION
            ));
        }

        validate_stable_id(&self.cluster_id, "cluster_id", &mut issues);
        validate_nonzero_u64(
            self.max_total_queue_bytes,
            "max_total_queue_bytes",
            &mut issues,
        );
        validate_nonzero_usize(
            self.max_total_queue_events,
            "max_total_queue_events",
            &mut issues,
        );

        self.validate_role(&mut issues);
        self.spool.validate("spool", &mut issues);

        let replica_listener = match &self.role {
            IngestRoleConfig::Primary {
                replica_listener, ..
            } => replica_listener.as_ref(),
            IngestRoleConfig::Replica { .. } => None,
        };
        if self.sources.is_empty() {
            if replica_listener.is_none() {
                issues.push(
                    "sources may be empty only for a primary with role.replica_listener"
                        .to_string(),
                );
            }
        } else if !self.sources.iter().any(|source| source.enabled) {
            issues.push("sources must contain at least one enabled input".to_string());
        }

        let mut source_ids = HashSet::with_capacity(self.sources.len());
        let mut enabled_queue_bytes = Some(0u64);
        let mut enabled_queue_events = Some(0usize);
        for (index, source) in self.sources.iter().enumerate() {
            let prefix = format!("sources[{index}]");
            validate_stable_id(&source.id, &format!("{prefix}.id"), &mut issues);
            if !source_ids.insert(source.id.as_str()) {
                issues.push(format!("{prefix}.id duplicates source id {:?}", source.id));
            }
            source
                .queue
                .validate(&format!("{prefix}.queue"), &mut issues);
            if source.queue.max_event_bytes > self.spool.segment_bytes {
                issues.push(format!(
                    "{prefix}.queue.max_event_bytes exceeds spool.segment_bytes"
                ));
            }
            source
                .input
                .validate(&format!("{prefix}.input"), &mut issues);
            if source.enabled {
                enabled_queue_bytes =
                    enabled_queue_bytes.and_then(|total| total.checked_add(source.queue.max_bytes));
                enabled_queue_events = enabled_queue_events
                    .and_then(|total| total.checked_add(source.queue.max_events));
            }
        }
        match enabled_queue_bytes {
            Some(total) if total > self.max_total_queue_bytes => issues.push(format!(
                "enabled source queue bytes {total} exceed max_total_queue_bytes {}",
                self.max_total_queue_bytes
            )),
            None => issues.push("enabled source queue byte sum overflows u64".to_string()),
            Some(_) => {}
        }
        match enabled_queue_events {
            Some(total) if total > self.max_total_queue_events => issues.push(format!(
                "enabled source queue events {total} exceed max_total_queue_events {}",
                self.max_total_queue_events
            )),
            None => issues.push("enabled source queue event sum overflows usize".to_string()),
            Some(_) => {}
        }
        if let Some(listener) = replica_listener {
            if let (Some(source_bytes), Some(listener_bytes)) =
                (enabled_queue_bytes, listener.max_in_flight_memory_bytes())
            {
                match source_bytes.checked_add(listener_bytes) {
                    Some(total) if total > self.max_total_queue_bytes => issues.push(format!(
                        "enabled source queues plus replica listener request/decompression memory {total} exceed max_total_queue_bytes {}",
                        self.max_total_queue_bytes
                    )),
                    None => issues.push(
                        "enabled source queue and replica listener byte sum overflows u64"
                            .to_string(),
                    ),
                    Some(_) => {}
                }
            }
            if let (Some(source_events), Some(listener_events)) =
                (enabled_queue_events, listener.max_in_flight_events())
            {
                match source_events.checked_add(listener_events) {
                    Some(total) if total > self.max_total_queue_events => issues.push(format!(
                        "enabled source queues plus replica listener request events {total} exceed max_total_queue_events {}",
                        self.max_total_queue_events
                    )),
                    None => issues.push(
                        "enabled source queue and replica listener event sum overflows usize"
                            .to_string(),
                    ),
                    Some(_) => {}
                }
            }
        }

        if issues.is_empty() {
            Ok(())
        } else {
            Err(IngestConfigValidationError { issues })
        }
    }

    pub fn redacted_summary(&self) -> RedactedIngestConfigSummary {
        let (role, node_id, replica_listener) = match &self.role {
            IngestRoleConfig::Primary {
                node_id,
                replica_listener,
            } => ("primary", node_id.clone(), replica_listener.as_ref()),
            IngestRoleConfig::Replica { node_id, .. } => ("replica", node_id.clone(), None),
        };
        let replica_listener = replica_listener.map(ReplicaListenerConfig::redacted_summary);
        let sources = self
            .sources
            .iter()
            .map(|source| RedactedSourceSummary {
                id: source.id.clone(),
                enabled: source.enabled,
                kind: source.input.kind_name(),
                priority: source.priority,
                required_for_epoch_close: source.required_for_epoch_close,
                auth_method: source.input.auth_method(),
                queue_max_events: source.queue.max_events,
                queue_max_bytes: source.queue.max_bytes,
            })
            .collect();
        RedactedIngestConfigSummary {
            schema_version: self.schema_version,
            cluster_id: self.cluster_id.clone(),
            max_total_queue_bytes: self.max_total_queue_bytes,
            configured_enabled_queue_bytes: self
                .sources
                .iter()
                .filter(|source| source.enabled)
                .fold(0u64, |total, source| {
                    total.saturating_add(source.queue.max_bytes)
                }),
            max_total_queue_events: self.max_total_queue_events,
            configured_enabled_queue_events: self
                .sources
                .iter()
                .filter(|source| source.enabled)
                .fold(0usize, |total, source| {
                    total.saturating_add(source.queue.max_events)
                }),
            role,
            node_id,
            accepts_replicas: replica_listener.is_some(),
            replica_listener,
            source_count: self.sources.len(),
            enabled_source_count: self.sources.iter().filter(|source| source.enabled).count(),
            spool_max_bytes: self.spool.max_bytes,
            spool_segment_bytes: self.spool.segment_bytes,
            sources,
        }
    }

    fn validate_role(&self, issues: &mut Vec<String>) {
        match &self.role {
            IngestRoleConfig::Primary {
                node_id,
                replica_listener,
            } => {
                validate_stable_id(node_id, "role.node_id", issues);
                if let Some(listener) = replica_listener {
                    listener.validate(
                        "role.replica_listener",
                        &self.spool,
                        self.max_total_queue_bytes,
                        self.max_total_queue_events,
                        issues,
                    );
                }
            }
            IngestRoleConfig::Replica { node_id, upstream } => {
                validate_stable_id(node_id, "role.node_id", issues);
                upstream.validate("role.upstream", &self.spool, issues);
            }
        }
    }
}

impl ReplicaListenerConfig {
    fn validate(
        &self,
        prefix: &str,
        spool: &SpoolConfig,
        max_total_queue_bytes: u64,
        max_total_queue_events: usize,
        issues: &mut Vec<String>,
    ) {
        validate_socket_addr(&self.bind, &format!("{prefix}.bind"), issues);
        for (name, path) in [
            ("server_certificate_file", &self.server_certificate_file),
            ("server_private_key_file", &self.server_private_key_file),
            ("client_ca_file", &self.client_ca_file),
            ("allowed_nodes_file", &self.allowed_nodes_file),
            ("primary_term_file", &self.primary_term_file),
        ] {
            validate_absolute_file(path, &format!("{prefix}.{name}"), issues);
        }
        validate_stable_id(
            &self.receipt_signing_key_id,
            &format!("{prefix}.receipt_signing_key_id"),
            issues,
        );
        self.receipt_signing_key
            .validate(&format!("{prefix}.receipt_signing_key"), issues);
        validate_nonzero_u64(
            self.max_wire_request_bytes,
            &format!("{prefix}.max_wire_request_bytes"),
            issues,
        );
        validate_nonzero_usize(
            self.max_batch_events,
            &format!("{prefix}.max_batch_events"),
            issues,
        );
        validate_nonzero_u64(
            self.max_batch_compressed_bytes,
            &format!("{prefix}.max_batch_compressed_bytes"),
            issues,
        );
        validate_nonzero_u64(
            self.max_batch_uncompressed_bytes,
            &format!("{prefix}.max_batch_uncompressed_bytes"),
            issues,
        );
        validate_nonzero_u64(
            self.max_compressed_event_bytes,
            &format!("{prefix}.max_compressed_event_bytes"),
            issues,
        );
        validate_nonzero_u64(
            self.max_uncompressed_event_bytes,
            &format!("{prefix}.max_uncompressed_event_bytes"),
            issues,
        );
        validate_nonzero_usize(
            self.max_concurrent_requests,
            &format!("{prefix}.max_concurrent_requests"),
            issues,
        );
        validate_nonzero_usize(
            self.max_open_streams,
            &format!("{prefix}.max_open_streams"),
            issues,
        );
        validate_nonzero_u64(
            self.request_idle_timeout_ms,
            &format!("{prefix}.request_idle_timeout_ms"),
            issues,
        );
        validate_nonzero_u64(
            self.request_total_timeout_ms,
            &format!("{prefix}.request_total_timeout_ms"),
            issues,
        );
        if self.request_total_timeout_ms < self.request_idle_timeout_ms {
            issues.push(format!(
                "{prefix}.request_total_timeout_ms must be >= {prefix}.request_idle_timeout_ms"
            ));
        }
        if self.max_open_streams > 4_096 {
            issues.push(format!("{prefix}.max_open_streams must be <= 4096"));
        }
        for (name, value) in [
            ("max_wire_request_bytes", self.max_wire_request_bytes),
            (
                "max_compressed_event_bytes",
                self.max_compressed_event_bytes,
            ),
            (
                "max_uncompressed_event_bytes",
                self.max_uncompressed_event_bytes,
            ),
        ] {
            if usize::try_from(value).is_err() {
                issues.push(format!(
                    "{prefix}.{name} exceeds this process address space"
                ));
            }
        }
        if self.max_compressed_event_bytes > self.max_batch_compressed_bytes {
            issues.push(format!(
                "{prefix}.max_compressed_event_bytes must be <= {prefix}.max_batch_compressed_bytes"
            ));
        }
        if self.max_uncompressed_event_bytes > self.max_batch_uncompressed_bytes {
            issues.push(format!(
                "{prefix}.max_uncompressed_event_bytes must be <= {prefix}.max_batch_uncompressed_bytes"
            ));
        }
        if self
            .max_compressed_event_bytes
            .checked_add(REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES)
            .is_none_or(|minimum| minimum > self.max_wire_request_bytes)
        {
            issues.push(format!(
                "{prefix}.max_wire_request_bytes must reserve at least {REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES} bytes above {prefix}.max_compressed_event_bytes for protobuf metadata"
            ));
        }
        if self.max_compressed_event_bytes > spool.segment_bytes {
            issues.push(format!(
                "{prefix}.max_compressed_event_bytes exceeds spool.segment_bytes"
            ));
        }
        if self.max_wire_request_bytes > max_total_queue_bytes {
            issues.push(format!(
                "{prefix}.max_wire_request_bytes exceeds max_total_queue_bytes"
            ));
        }
        if self.max_batch_events > max_total_queue_events {
            issues.push(format!(
                "{prefix}.max_batch_events exceeds max_total_queue_events"
            ));
        }
        match self.max_in_flight_memory_bytes() {
            Some(total) if total > max_total_queue_bytes => issues.push(format!(
                "{prefix} in-flight request/decompression memory {total} exceeds max_total_queue_bytes {max_total_queue_bytes}"
            )),
            None => issues.push(format!(
                "({prefix}.max_batch_compressed_bytes + per-record metadata reserve + {prefix}.max_wire_request_bytes + {prefix}.max_uncompressed_event_bytes) * {prefix}.max_concurrent_requests overflows u64"
            )),
            Some(_) => {}
        }
        match self.max_in_flight_events() {
            Some(total) if total > max_total_queue_events => issues.push(format!(
                "{prefix} in-flight batch events {total} exceed max_total_queue_events {max_total_queue_events}"
            )),
            None => issues.push(format!(
                "{prefix}.max_batch_events * {prefix}.max_concurrent_requests overflows usize"
            )),
            Some(_) => {}
        }
    }

    fn max_in_flight_memory_bytes(&self) -> Option<u64> {
        let concurrent_requests = u64::try_from(self.max_concurrent_requests).ok()?;
        let batch_records = u64::try_from(self.max_batch_events).ok()?;
        let record_metadata = batch_records.checked_mul(REPLICATION_RECORD_MEMORY_RESERVE_BYTES)?;
        self.max_batch_compressed_bytes
            .checked_add(record_metadata)?
            .checked_add(self.max_wire_request_bytes)?
            .checked_add(self.max_uncompressed_event_bytes)?
            .checked_mul(concurrent_requests)
    }

    fn max_in_flight_events(&self) -> Option<usize> {
        self.max_batch_events
            .checked_mul(self.max_concurrent_requests)
    }

    fn redacted_summary(&self) -> RedactedReplicaListenerSummary {
        RedactedReplicaListenerSummary {
            max_wire_request_bytes: self.max_wire_request_bytes,
            max_batch_events: self.max_batch_events,
            max_batch_compressed_bytes: self.max_batch_compressed_bytes,
            max_batch_uncompressed_bytes: self.max_batch_uncompressed_bytes,
            max_compressed_event_bytes: self.max_compressed_event_bytes,
            max_uncompressed_event_bytes: self.max_uncompressed_event_bytes,
            max_concurrent_requests: self.max_concurrent_requests,
            max_open_streams: self.max_open_streams,
            request_idle_timeout_ms: self.request_idle_timeout_ms,
            request_total_timeout_ms: self.request_total_timeout_ms,
            max_in_flight_events: self.max_in_flight_events().unwrap_or(usize::MAX),
            max_in_flight_memory_bytes: self.max_in_flight_memory_bytes().unwrap_or(u64::MAX),
        }
    }
}

impl ReplicaUpstreamConfig {
    fn validate(&self, prefix: &str, spool: &SpoolConfig, issues: &mut Vec<String>) {
        validate_secure_stream_endpoint(&self.endpoint, &format!("{prefix}.endpoint"), issues);
        self.tls.validate(&format!("{prefix}.tls"), issues);
        self.reconnect
            .validate(&format!("{prefix}.reconnect"), issues);
        self.batch.validate(&format!("{prefix}.batch"), issues);
        if self.batch.max_bytes > spool.segment_bytes {
            issues.push(format!(
                "{prefix}.batch.max_bytes exceeds spool.segment_bytes"
            ));
        }
        match &self.auth {
            ReplicaAuthConfig::MutualTls => {
                if self.tls.client_certificate_file.is_none()
                    || self.tls.client_private_key_file.is_none()
                {
                    issues.push(format!(
                        "{prefix}.auth mutual_tls requires TLS client certificate and private key files"
                    ));
                }
            }
            ReplicaAuthConfig::Bearer { credential } => {
                credential.validate(&format!("{prefix}.auth.credential"), issues);
            }
            ReplicaAuthConfig::Ed25519 {
                key_id,
                signing_key,
                upstream_public_key_file,
            } => {
                validate_stable_id(key_id, &format!("{prefix}.auth.key_id"), issues);
                signing_key.validate(&format!("{prefix}.auth.signing_key"), issues);
                validate_absolute_file(
                    upstream_public_key_file,
                    &format!("{prefix}.auth.upstream_public_key_file"),
                    issues,
                );
            }
        }

        if self.trusted_receipt_keys.is_empty() {
            issues.push(format!(
                "{prefix}.trusted_receipt_keys must contain at least one key"
            ));
        }
        let mut key_ids = HashSet::with_capacity(self.trusted_receipt_keys.len());
        for (index, key) in self.trusted_receipt_keys.iter().enumerate() {
            let key_prefix = format!("{prefix}.trusted_receipt_keys[{index}]");
            validate_stable_id(&key.key_id, &format!("{key_prefix}.key_id"), issues);
            validate_absolute_file(
                &key.public_key_file,
                &format!("{key_prefix}.public_key_file"),
                issues,
            );
            if !key_ids.insert(key.key_id.as_str()) {
                issues.push(format!(
                    "{key_prefix}.key_id duplicates trusted receipt key id {:?}",
                    key.key_id
                ));
            }
        }
    }
}

impl ReplicaBatchLimits {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        validate_nonzero_usize(self.max_events, &format!("{prefix}.max_events"), issues);
        validate_nonzero_u64(self.max_bytes, &format!("{prefix}.max_bytes"), issues);
        validate_nonzero_u64(
            self.ack_timeout_ms,
            &format!("{prefix}.ack_timeout_ms"),
            issues,
        );
    }
}

impl SourceInputConfig {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        match self {
            Self::Grpc {
                endpoint,
                auth,
                tls,
                reconnect,
                resume_overlap_slots,
                ..
            } => {
                let secure =
                    validate_http_endpoint(endpoint, &format!("{prefix}.endpoint"), issues);
                if !secure && tls.is_some() {
                    issues.push(format!(
                        "{prefix}.tls is only valid with an https gRPC endpoint"
                    ));
                }
                if !secure && auth.is_some() {
                    issues.push(format!(
                        "{prefix}.auth is forbidden on a plaintext http gRPC endpoint"
                    ));
                }
                if let Some(tls) = tls {
                    tls.validate(&format!("{prefix}.tls"), issues);
                }
                if let Some(auth) = auth {
                    auth.validate(&format!("{prefix}.auth"), issues);
                    if matches!(auth, SourceAuthConfig::HmacSha256 { .. }) {
                        issues.push(format!(
                            "{prefix}.auth hmac_sha256 is not valid for a gRPC source"
                        ));
                    }
                }
                reconnect.validate(&format!("{prefix}.reconnect"), issues);
                validate_nonzero_u64(
                    *resume_overlap_slots,
                    &format!("{prefix}.resume_overlap_slots"),
                    issues,
                );
            }
            Self::ShredUdp {
                bind,
                multicast_group,
                interface,
                auth,
            } => {
                validate_socket_addr(bind, &format!("{prefix}.bind"), issues);
                validate_udp_multicast(
                    multicast_group.as_deref(),
                    interface.as_deref(),
                    prefix,
                    issues,
                );
                if let Some(auth) = auth {
                    auth.validate(&format!("{prefix}.auth"), issues);
                    if !matches!(auth, SourceAuthConfig::HmacSha256 { .. }) {
                        issues.push(format!(
                            "{prefix}.auth only hmac_sha256 is valid for a UDP shred source"
                        ));
                    }
                }
            }
            Self::ShredQuic {
                endpoint,
                auth,
                tls,
                reconnect,
                resume_overlap_events,
            } => {
                validate_quic_endpoint(endpoint, &format!("{prefix}.endpoint"), issues);
                tls.validate(&format!("{prefix}.tls"), issues);
                if let Some(auth) = auth {
                    auth.validate(&format!("{prefix}.auth"), issues);
                    if matches!(auth, SourceAuthConfig::XToken { .. }) {
                        issues.push(format!(
                            "{prefix}.auth x_token is not valid for a QUIC shred source"
                        ));
                    }
                }
                reconnect.validate(&format!("{prefix}.reconnect"), issues);
                validate_nonzero_u64(
                    *resume_overlap_events,
                    &format!("{prefix}.resume_overlap_events"),
                    issues,
                );
            }
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Grpc { .. } => "grpc",
            Self::ShredUdp { .. } => "shred_udp",
            Self::ShredQuic { .. } => "shred_quic",
        }
    }

    fn auth_method(&self) -> Option<&'static str> {
        let auth = match self {
            Self::Grpc { auth, .. }
            | Self::ShredUdp { auth, .. }
            | Self::ShredQuic { auth, .. } => auth.as_ref(),
        }?;
        Some(auth.method_name())
    }
}

impl QueueLimits {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        validate_nonzero_usize(self.max_events, &format!("{prefix}.max_events"), issues);
        validate_nonzero_u64(self.max_bytes, &format!("{prefix}.max_bytes"), issues);
        validate_nonzero_u64(
            self.max_event_bytes,
            &format!("{prefix}.max_event_bytes"),
            issues,
        );
        if self.max_event_bytes > self.max_bytes {
            issues.push(format!(
                "{prefix}.max_event_bytes must be <= {prefix}.max_bytes"
            ));
        }
    }
}

impl ReconnectConfig {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        validate_nonzero_u64(
            self.initial_delay_ms,
            &format!("{prefix}.initial_delay_ms"),
            issues,
        );
        validate_nonzero_u64(self.max_delay_ms, &format!("{prefix}.max_delay_ms"), issues);
        if self.initial_delay_ms > self.max_delay_ms {
            issues.push(format!(
                "{prefix}.initial_delay_ms must be <= {prefix}.max_delay_ms"
            ));
        }
        if !(1_000..=10_000).contains(&self.backoff_factor_milli) {
            issues.push(format!(
                "{prefix}.backoff_factor_milli must be between 1000 and 10000"
            ));
        }
        if self.jitter_percent > 100 {
            issues.push(format!("{prefix}.jitter_percent must be <= 100"));
        }
        validate_nonzero_u64(
            self.connect_timeout_ms,
            &format!("{prefix}.connect_timeout_ms"),
            issues,
        );
        validate_nonzero_u64(
            self.heartbeat_interval_ms,
            &format!("{prefix}.heartbeat_interval_ms"),
            issues,
        );
        validate_nonzero_u64(
            self.idle_timeout_ms,
            &format!("{prefix}.idle_timeout_ms"),
            issues,
        );
        if self.idle_timeout_ms <= self.heartbeat_interval_ms {
            issues.push(format!(
                "{prefix}.idle_timeout_ms must be greater than {prefix}.heartbeat_interval_ms"
            ));
        }
        if self.max_attempts == Some(0) {
            issues.push(format!("{prefix}.max_attempts must be null or non-zero"));
        }
    }
}

impl ClientTlsConfig {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        for (name, path) in [
            ("ca_file", self.ca_file.as_ref()),
            (
                "client_certificate_file",
                self.client_certificate_file.as_ref(),
            ),
            (
                "client_private_key_file",
                self.client_private_key_file.as_ref(),
            ),
        ] {
            if let Some(path) = path {
                validate_absolute_file(path, &format!("{prefix}.{name}"), issues);
            }
        }
        if self.client_certificate_file.is_some() != self.client_private_key_file.is_some() {
            issues.push(format!(
                "{prefix}.client_certificate_file and client_private_key_file must be configured together"
            ));
        }
        if let Some(server_name) = &self.server_name
            && (server_name.is_empty()
                || server_name.chars().any(char::is_whitespace)
                || server_name.contains('/')
                || server_name.contains("\u{3a}\u{2f}\u{2f}"))
        {
            issues.push(format!("{prefix}.server_name is invalid"));
        }
    }
}

impl SourceAuthConfig {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        match self {
            Self::XToken { credential } | Self::Bearer { credential } => {
                credential.validate(&format!("{prefix}.credential"), issues);
            }
            Self::HmacSha256 { key_id, credential } => {
                validate_stable_id(key_id, &format!("{prefix}.key_id"), issues);
                credential.validate(&format!("{prefix}.credential"), issues);
            }
        }
    }

    fn method_name(&self) -> &'static str {
        match self {
            Self::XToken { .. } => "x_token",
            Self::Bearer { .. } => "bearer",
            Self::HmacSha256 { .. } => "hmac_sha256",
        }
    }
}

impl SecretRef {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        match self {
            Self::Env { variable } => {
                let mut chars = variable.chars();
                let valid_first = chars
                    .next()
                    .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
                if !valid_first
                    || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
                {
                    issues.push(format!("{prefix}.variable is not a valid environment name"));
                }
            }
            Self::File { path } => validate_absolute_file(path, &format!("{prefix}.path"), issues),
        }
    }
}

impl SpoolConfig {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        validate_absolute_directory(&self.root, &format!("{prefix}.root"), issues);
        validate_nonzero_u64(self.max_bytes, &format!("{prefix}.max_bytes"), issues);
        validate_nonzero_u64(
            self.segment_bytes,
            &format!("{prefix}.segment_bytes"),
            issues,
        );
        validate_nonzero_u64(
            self.reserve_free_bytes,
            &format!("{prefix}.reserve_free_bytes"),
            issues,
        );
        if self.segment_bytes > self.max_bytes {
            issues.push(format!(
                "{prefix}.segment_bytes must be <= {prefix}.max_bytes"
            ));
        }
        self.sync.validate(&format!("{prefix}.sync"), issues);
        if self.sync.max_unsynced_bytes > self.segment_bytes {
            issues.push(format!(
                "{prefix}.sync.max_unsynced_bytes must be <= {prefix}.segment_bytes"
            ));
        }
    }
}

impl SpoolSyncConfig {
    fn validate(&self, prefix: &str, issues: &mut Vec<String>) {
        validate_nonzero_u64(self.max_delay_ms, &format!("{prefix}.max_delay_ms"), issues);
        validate_nonzero_u64(
            self.max_unsynced_bytes,
            &format!("{prefix}.max_unsynced_bytes"),
            issues,
        );
    }
}

fn validate_stable_id(value: &str, field: &str, issues: &mut Vec<String>) {
    let mut chars = value.chars();
    let valid_first = chars
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric());
    let valid_rest = chars
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'));
    if value.len() > 64 || !valid_first || !valid_rest {
        issues.push(format!(
            "{field} must be 1-64 ASCII characters, start alphanumeric, and contain only alphanumeric, '-', '_' or '.'"
        ));
    }
}

fn validate_nonzero_u64(value: u64, field: &str, issues: &mut Vec<String>) {
    if value == 0 {
        issues.push(format!("{field} must be non-zero"));
    }
}

fn validate_nonzero_usize(value: usize, field: &str, issues: &mut Vec<String>) {
    if value == 0 {
        issues.push(format!("{field} must be non-zero"));
    }
}

fn validate_absolute_file(path: &Path, field: &str, issues: &mut Vec<String>) {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path
            .components()
            .any(|component| component == Component::ParentDir)
    {
        issues.push(format!(
            "{field} must be an absolute, unambiguous file path"
        ));
    }
}

fn validate_absolute_directory(path: &Path, field: &str, issues: &mut Vec<String>) {
    if !path.is_absolute()
        || path == Path::new("/")
        || path
            .components()
            .any(|component| component == Component::ParentDir)
    {
        issues.push(format!(
            "{field} must be an absolute, unambiguous non-root directory"
        ));
    }
}

fn validate_socket_addr(value: &str, field: &str, issues: &mut Vec<String>) {
    if SocketAddr::from_str(value).is_err() {
        issues.push(format!("{field} must be an IP socket address"));
    }
}

fn validate_udp_multicast(
    group: Option<&str>,
    interface: Option<&str>,
    prefix: &str,
    issues: &mut Vec<String>,
) {
    let parsed_group = group.and_then(|value| match IpAddr::from_str(value) {
        Ok(address) if address.is_multicast() => Some(address),
        _ => {
            issues.push(format!(
                "{prefix}.multicast_group must be a multicast IP address"
            ));
            None
        }
    });
    let parsed_interface = interface.and_then(|value| match IpAddr::from_str(value) {
        Ok(address) if !address.is_multicast() && !address.is_unspecified() => Some(address),
        _ => {
            issues.push(format!(
                "{prefix}.interface must be a concrete unicast IP address"
            ));
            None
        }
    });
    if group.is_some() && interface.is_none() {
        issues.push(format!(
            "{prefix}.interface is required when multicast_group is configured"
        ));
    }
    if let (Some(group), Some(interface)) = (parsed_group, parsed_interface)
        && group.is_ipv4() != interface.is_ipv4()
    {
        issues.push(format!(
            "{prefix}.multicast_group and interface must use the same IP family"
        ));
    }
}

/// Returns whether the endpoint is TLS protected.
fn validate_http_endpoint(value: &str, field: &str, issues: &mut Vec<String>) -> bool {
    let (secure, remainder) = if let Some(remainder) = value.strip_prefix("https://") {
        (true, remainder)
    } else if let Some(remainder) = value.strip_prefix("http://") {
        (false, remainder)
    } else {
        issues.push(format!("{field} must use http:// or https://"));
        return false;
    };
    validate_endpoint_remainder(remainder, field, false, issues);
    secure
}

fn validate_secure_stream_endpoint(value: &str, field: &str, issues: &mut Vec<String>) {
    let remainder = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("grpcs://"));
    match remainder {
        Some(remainder) => validate_endpoint_remainder(remainder, field, false, issues),
        None => issues.push(format!("{field} must use https:// or grpcs://")),
    }
}

fn validate_quic_endpoint(value: &str, field: &str, issues: &mut Vec<String>) {
    match value.strip_prefix("quic://") {
        Some(remainder) => validate_endpoint_remainder(remainder, field, true, issues),
        None => issues.push(format!("{field} must use quic://")),
    }
}

fn validate_endpoint_remainder(
    remainder: &str,
    field: &str,
    port_required: bool,
    issues: &mut Vec<String>,
) {
    let authority = remainder.split('/').next().unwrap_or_default();
    if remainder.is_empty()
        || authority.is_empty()
        || remainder.chars().any(char::is_whitespace)
        || remainder.contains('?')
        || remainder.contains('#')
        || authority.contains('@')
    {
        issues.push(format!(
            "{field} must contain a non-secret authority and no query, fragment, userinfo, or whitespace"
        ));
        return;
    }
    if port_required && !authority_has_valid_port(authority) {
        issues.push(format!("{field} must include a valid non-zero port"));
    }
}

fn authority_has_valid_port(authority: &str) -> bool {
    if let Ok(address) = SocketAddr::from_str(authority) {
        return address.port() != 0;
    }
    let Some((host, port)) = authority.rsplit_once(':') else {
        return false;
    };
    if host.is_empty() || host.contains(':') || host.contains('[') || host.contains(']') {
        return false;
    }
    port.parse::<u16>().is_ok_and(|port| port != 0)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn reconnect_json() -> Value {
        json!({
            "initial_delay_ms": 250,
            "max_delay_ms": 30_000,
            "backoff_factor_milli": 2_000,
            "jitter_percent": 20,
            "connect_timeout_ms": 10_000,
            "heartbeat_interval_ms": 5_000,
            "idle_timeout_ms": 20_000,
            "max_attempts": null
        })
    }

    fn queue_json() -> Value {
        json!({
            "max_events": 16,
            "max_bytes": 268_435_456u64,
            "max_event_bytes": 134_217_728u64
        })
    }

    fn valid_primary_json() -> Value {
        json!({
            "schema_version": 2,
            "cluster_id": "solana-mainnet",
            "max_total_queue_bytes": 1_073_741_824u64,
            "max_total_queue_events": 128,
            "role": {
                "mode": "primary",
                "node_id": "nas-primary-1",
                "replica_listener": {
                    "bind": "0.0.0.0:9443",
                    "server_certificate_file": "/etc/blockzilla/tls/server.crt",
                    "server_private_key_file": "/etc/blockzilla/tls/server.key",
                    "client_ca_file": "/etc/blockzilla/tls/replica-ca.crt",
                    "allowed_nodes_file": "/etc/blockzilla/auth/replicas.json",
                    "primary_term_file": "/var/lib/blockzilla/ingress-spool/primary-term",
                    "receipt_signing_key_id": "receipt-2026-q3",
                    "receipt_signing_key": {
                        "provider": "file",
                        "path": "/etc/blockzilla/secrets/receipt-2026-q3.key"
                    },
                    "max_wire_request_bytes": 41_943_040u64,
                    "max_batch_events": 32,
                    "max_batch_compressed_bytes": 67_108_864u64,
                    "max_batch_uncompressed_bytes": 536_870_912u64,
                    "max_compressed_event_bytes": 33_554_432u64,
                    "max_uncompressed_event_bytes": 134_217_728u64,
                    "max_concurrent_requests": 2,
                    "max_open_streams": 64,
                    "request_idle_timeout_ms": 15_000,
                    "request_total_timeout_ms": 60_000
                }
            },
            "spool": {
                "root": "/var/lib/blockzilla/ingress-spool",
                "max_bytes": 1_099_511_627_776u64,
                "segment_bytes": 536_870_912u64,
                "reserve_free_bytes": 10_737_418_240u64,
                "sync": {
                    "max_delay_ms": 50,
                    "max_unsynced_bytes": 8_388_608u64
                },
                "full_policy": "pause_sources"
            },
            "sources": [
                {
                    "id": "yellowstone-primary",
                    "enabled": true,
                    "priority": 0,
                    "required_for_epoch_close": true,
                    "queue": queue_json(),
                    "input": {
                        "kind": "grpc",
                        "endpoint": "https://grpc.example.net",
                        "commitment": "confirmed",
                        "auth": {
                            "method": "x_token",
                            "credential": {
                                "provider": "env",
                                "variable": "BLOCKZILLA_GRPC_PRIMARY_TOKEN"
                            }
                        },
                        "tls": {
                            "ca_file": "/etc/blockzilla/tls/provider-ca.crt"
                        },
                        "reconnect": reconnect_json(),
                        "resume_overlap_slots": 64
                    }
                },
                {
                    "id": "shred-lan-a",
                    "enabled": true,
                    "priority": 10,
                    "required_for_epoch_close": false,
                    "queue": queue_json(),
                    "input": {
                        "kind": "shred_udp",
                        "bind": "0.0.0.0:8001",
                        "multicast_group": "239.10.10.10",
                        "interface": "192.168.1.45",
                        "auth": {
                            "method": "hmac_sha256",
                            "key_id": "shred-key-2026-07",
                            "credential": {
                                "provider": "file",
                                "path": "/etc/blockzilla/secrets/shred-hmac.key"
                            }
                        }
                    }
                }
            ]
        })
    }

    fn valid_replica_json(mode: &str) -> Value {
        let mut value = valid_primary_json();
        value["role"] = json!({
            "mode": mode,
            "node_id": "backup-ingester-1",
            "upstream": {
                "endpoint": "https://192.168.1.45:9555",
                "tls": {
                    "ca_file": "/etc/blockzilla/tls/primary-ca.crt"
                },
                "auth": {
                    "method": "bearer",
                    "credential": {
                        "provider": "file",
                        "path": "/etc/blockzilla/secrets/replication.token"
                    }
                },
                "trusted_receipt_keys": [
                    {
                        "key_id": "receipt-2026-q2",
                        "public_key_file": "/etc/blockzilla/keys/receipt-2026-q2.pub"
                    },
                    {
                        "key_id": "receipt-2026-q3",
                        "public_key_file": "/etc/blockzilla/keys/receipt-2026-q3.pub"
                    }
                ],
                "reconnect": reconnect_json(),
                "batch": {
                    "max_events": 128,
                    "max_bytes": 67_108_864u64,
                    "ack_timeout_ms": 30_000
                }
            }
        });
        value
    }

    fn decode_and_validate(value: &Value) -> Result<IngestConfig, IngestConfigLoadError> {
        IngestConfig::from_json(&serde_json::to_string(value).unwrap())
    }

    fn validation_text(value: &Value) -> String {
        decode_and_validate(value).unwrap_err().to_string()
    }

    #[test]
    fn valid_primary_supports_multiple_grpc_and_shred_sources() {
        let config = decode_and_validate(&valid_primary_json()).unwrap();
        assert_eq!(config.sources.len(), 2);
        assert_eq!(config.redacted_summary().enabled_source_count, 2);
    }

    #[test]
    fn validated_primary_replica_listener_can_be_the_only_input() {
        let mut value = valid_primary_json();
        value["sources"] = json!([]);
        let config = decode_and_validate(&value).unwrap();
        let summary = config.redacted_summary();
        assert!(summary.accepts_replicas);
        assert_eq!(summary.source_count, 0);
        assert_eq!(summary.enabled_source_count, 0);
        assert_eq!(
            summary.replica_listener.unwrap().max_in_flight_memory_bytes,
            490_733_568
        );
    }

    #[test]
    fn empty_sources_require_a_valid_primary_replica_listener() {
        let mut primary_without_listener = valid_primary_json();
        primary_without_listener["role"]
            .as_object_mut()
            .unwrap()
            .remove("replica_listener");
        primary_without_listener["sources"] = json!([]);
        let error = validation_text(&primary_without_listener);
        assert!(error.contains("sources may be empty only for a primary"));

        let mut replica = valid_replica_json("replica");
        replica["sources"] = json!([]);
        let error = validation_text(&replica);
        assert!(error.contains("sources may be empty only for a primary"));

        let mut invalid_listener = valid_primary_json();
        invalid_listener["sources"] = json!([]);
        invalid_listener["role"]["replica_listener"]["primary_term_file"] =
            json!("relative/primary-term");
        let error = validation_text(&invalid_listener);
        assert!(error.contains("primary_term_file must be an absolute, unambiguous file path"));
    }

    #[test]
    fn replica_listener_limits_are_nonzero_and_fit_spool_and_process_budgets() {
        let mut zero = valid_primary_json();
        let zero_listener = &mut zero["role"]["replica_listener"];
        zero_listener["max_wire_request_bytes"] = json!(0);
        zero_listener["max_batch_events"] = json!(0);
        zero_listener["max_batch_compressed_bytes"] = json!(0);
        zero_listener["max_batch_uncompressed_bytes"] = json!(0);
        zero_listener["max_compressed_event_bytes"] = json!(0);
        zero_listener["max_uncompressed_event_bytes"] = json!(0);
        zero_listener["max_concurrent_requests"] = json!(0);
        zero_listener["max_open_streams"] = json!(0);
        zero_listener["request_idle_timeout_ms"] = json!(0);
        zero_listener["request_total_timeout_ms"] = json!(0);
        let error = validation_text(&zero);
        assert!(error.contains("max_wire_request_bytes must be non-zero"));
        assert!(error.contains("max_batch_events must be non-zero"));
        assert!(error.contains("max_batch_compressed_bytes must be non-zero"));
        assert!(error.contains("max_batch_uncompressed_bytes must be non-zero"));
        assert!(error.contains("max_compressed_event_bytes must be non-zero"));
        assert!(error.contains("max_uncompressed_event_bytes must be non-zero"));
        assert!(error.contains("max_concurrent_requests must be non-zero"));
        assert!(error.contains("max_open_streams must be non-zero"));
        assert!(error.contains("request_idle_timeout_ms must be non-zero"));
        assert!(error.contains("request_total_timeout_ms must be non-zero"));

        let mut incoherent = valid_primary_json();
        let listener = &mut incoherent["role"]["replica_listener"];
        listener["primary_term_file"] = json!("relative.term");
        listener["max_wire_request_bytes"] = json!(1_200_000_000u64);
        listener["max_batch_events"] = json!(100);
        listener["max_batch_compressed_bytes"] = json!(1_300_000_000u64);
        listener["max_batch_uncompressed_bytes"] = json!(1_300_000_000u64);
        listener["max_compressed_event_bytes"] = json!(1_400_000_000u64);
        listener["max_uncompressed_event_bytes"] = json!(1_400_000_000u64);
        listener["max_concurrent_requests"] = json!(2);
        listener["request_idle_timeout_ms"] = json!(60_000);
        listener["request_total_timeout_ms"] = json!(30_000);
        let error = validation_text(&incoherent);
        assert!(error.contains("primary_term_file must be an absolute"));
        assert!(error.contains(
            "max_compressed_event_bytes must be <= role.replica_listener.max_batch_compressed_bytes"
        ));
        assert!(error.contains("max_uncompressed_event_bytes must be <= role.replica_listener.max_batch_uncompressed_bytes"));
        assert!(error.contains("max_wire_request_bytes must reserve at least 65536 bytes above role.replica_listener.max_compressed_event_bytes"));
        assert!(error.contains("max_compressed_event_bytes exceeds spool.segment_bytes"));
        assert!(error.contains("max_wire_request_bytes exceeds max_total_queue_bytes"));
        assert!(error.contains("in-flight request/decompression memory"));
        assert!(error.contains("in-flight batch events"));
        assert!(error.contains(
            "request_total_timeout_ms must be >= role.replica_listener.request_idle_timeout_ms"
        ));
        assert!(error.contains("plus replica listener request/decompression memory"));
        assert!(error.contains("plus replica listener request events"));
    }

    #[test]
    fn redacted_summary_omits_endpoints_secret_refs_and_paths() {
        let config = decode_and_validate(&valid_primary_json()).unwrap();
        let summary = serde_json::to_string(&config.redacted_summary()).unwrap();
        assert!(!summary.contains("grpc.example.net"));
        assert!(!summary.contains("BLOCKZILLA_GRPC_PRIMARY_TOKEN"));
        assert!(!summary.contains("/etc/blockzilla"));
        assert!(!summary.contains("primary-term"));
        assert!(summary.contains("yellowstone-primary"));
        assert!(summary.contains("x_token"));
        assert!(summary.contains("\"max_in_flight_memory_bytes\":490733568"));
    }

    #[test]
    fn duplicate_and_unstable_source_ids_are_rejected() {
        let mut value = valid_primary_json();
        value["sources"][0]["id"] = json!("bad id");
        value["sources"][1]["id"] = json!("bad id");
        let error = validation_text(&value);
        assert!(error.contains("start alphanumeric"));
        assert!(error.contains("duplicates source id"));
    }

    #[test]
    fn every_source_queue_is_explicitly_bounded_by_events_and_bytes() {
        let mut value = valid_primary_json();
        value["sources"][0]["queue"]["max_events"] = json!(0);
        value["sources"][0]["queue"]["max_bytes"] = json!(0);
        value["sources"][0]["queue"]["max_event_bytes"] = json!(600_000_000u64);
        let error = validation_text(&value);
        assert!(error.contains("max_events must be non-zero"));
        assert!(error.contains("max_bytes must be non-zero"));
        assert!(error.contains("max_event_bytes must be <="));
        assert!(error.contains("exceeds spool.segment_bytes"));
    }

    #[test]
    fn enabled_source_queues_must_fit_global_byte_and_event_budgets() {
        let mut value = valid_primary_json();
        value["max_total_queue_bytes"] = json!(1);
        value["max_total_queue_events"] = json!(1);
        let error = validation_text(&value);
        assert!(error.contains("exceed max_total_queue_bytes"));
        assert!(error.contains("exceed max_total_queue_events"));
    }

    #[test]
    fn spool_limits_and_sync_budget_must_be_nonzero_and_coherent() {
        let mut value = valid_primary_json();
        value["spool"]["segment_bytes"] = json!(0);
        value["spool"]["sync"]["max_delay_ms"] = json!(0);
        value["spool"]["sync"]["max_unsynced_bytes"] = json!(600_000_000u64);
        let error = validation_text(&value);
        assert!(error.contains("spool.segment_bytes must be non-zero"));
        assert!(error.contains("spool.sync.max_delay_ms must be non-zero"));
        assert!(error.contains("max_unsynced_bytes must be <="));
    }

    #[test]
    fn unsupported_schema_and_empty_enabled_set_are_rejected() {
        let mut value = valid_primary_json();
        value["schema_version"] = json!(1);
        for source in value["sources"].as_array_mut().unwrap() {
            source["enabled"] = json!(false);
        }
        let error = validation_text(&value);
        assert!(error.contains("schema_version 1 is unsupported"));
        assert!(error.contains("at least one enabled input"));
    }

    #[test]
    fn literal_credentials_and_unknown_fields_are_not_deserializable() {
        let mut value = valid_primary_json();
        value["sources"][0]["input"]["auth"] = json!({
            "method": "x_token",
            "credential": {
                "provider": "literal",
                "value": "must-never-appear"
            }
        });
        let error = decode_and_validate(&value).unwrap_err().to_string();
        assert!(error.contains("unknown variant") || error.contains("unknown field"));
        assert!(!error.contains("must-never-appear"));
    }

    #[test]
    fn secret_refs_require_valid_env_names_or_absolute_files() {
        let mut value = valid_primary_json();
        value["sources"][0]["input"]["auth"]["credential"] = json!({
            "provider": "env",
            "variable": "9 BAD NAME"
        });
        value["sources"][1]["input"]["auth"]["credential"] = json!({
            "provider": "file",
            "path": "relative.key"
        });
        let error = validation_text(&value);
        assert!(error.contains("not a valid environment name"));
        assert!(error.contains("absolute, unambiguous file path"));
    }

    #[test]
    fn slave_is_an_input_alias_but_serializes_as_replica() {
        let config = decode_and_validate(&valid_replica_json("slave")).unwrap();
        assert!(matches!(config.role, IngestRoleConfig::Replica { .. }));
        let encoded = serde_json::to_string(&config).unwrap();
        assert!(encoded.contains("\"mode\":\"replica\""));
        assert!(!encoded.contains("\"mode\":\"slave\""));
    }

    #[test]
    fn replica_structurally_requires_node_upstream_and_auth() {
        let mut value = valid_primary_json();
        value["role"] = json!({
            "mode": "replica",
            "node_id": "backup-1",
            "upstream": {
                "endpoint": "https://primary.example.net",
                "tls": {},
                "reconnect": reconnect_json(),
                "batch": { "max_events": 1, "max_bytes": 1024, "ack_timeout_ms": 1000 }
            }
        });
        let error = decode_and_validate(&value).unwrap_err().to_string();
        assert!(error.contains("missing field `auth`"));

        value["role"] = json!({ "mode": "replica", "node_id": "backup-1" });
        let error = decode_and_validate(&value).unwrap_err().to_string();
        assert!(error.contains("missing field `upstream`"));

        value["role"] = valid_replica_json("replica")["role"].clone();
        value["role"].as_object_mut().unwrap().remove("node_id");
        let error = decode_and_validate(&value).unwrap_err().to_string();
        assert!(error.contains("missing field `node_id`"));
    }

    #[test]
    fn replica_rejects_insecure_transport_and_incomplete_mutual_tls() {
        let mut value = valid_replica_json("replica");
        value["role"]["upstream"]["endpoint"] = json!("http://primary.example.net");
        value["role"]["upstream"]["auth"] = json!({ "method": "mutual_tls" });
        let error = validation_text(&value);
        assert!(error.contains("must use https:// or grpcs://"));
        assert!(error.contains("requires TLS client certificate"));
    }

    #[test]
    fn replica_requires_valid_unique_trusted_receipt_keys() {
        let mut value = valid_replica_json("replica");
        value["role"]["upstream"]["trusted_receipt_keys"][1]["key_id"] =
            value["role"]["upstream"]["trusted_receipt_keys"][0]["key_id"].clone();
        value["role"]["upstream"]["trusted_receipt_keys"][0]["public_key_file"] =
            json!("relative.pub");
        let error = validation_text(&value);
        assert!(error.contains("duplicates trusted receipt key id"));
        assert!(error.contains("absolute, unambiguous file path"));

        value["role"]["upstream"]["trusted_receipt_keys"] = json!([]);
        let error = validation_text(&value);
        assert!(error.contains("must contain at least one key"));
    }

    #[test]
    fn source_transport_and_auth_kinds_are_validated_together() {
        let mut value = valid_primary_json();
        value["sources"][0]["input"]["endpoint"] = json!("udp://grpc.example.net:1000");
        value["sources"][0]["input"]["auth"] = json!({
            "method": "hmac_sha256",
            "key_id": "key-a",
            "credential": { "provider": "env", "variable": "HMAC_KEY" }
        });
        value["sources"][1]["input"]["auth"] = json!({
            "method": "bearer",
            "credential": { "provider": "env", "variable": "BEARER_TOKEN" }
        });
        let error = validation_text(&value);
        assert!(error.contains("must use http:// or https://"));
        assert!(error.contains("not valid for a gRPC source"));
        assert!(error.contains("only hmac_sha256 is valid"));
    }

    #[test]
    fn udp_multicast_requires_matching_concrete_interface() {
        let mut value = valid_primary_json();
        value["sources"][1]["input"]["interface"] = Value::Null;
        let error = validation_text(&value);
        assert!(error.contains("interface is required"));

        value["sources"][1]["input"]["interface"] = json!("::1");
        let error = validation_text(&value);
        assert!(error.contains("same IP family"));
    }

    #[test]
    fn reconnect_controls_must_be_live_and_coherent() {
        let mut value = valid_primary_json();
        let reconnect = &mut value["sources"][0]["input"]["reconnect"];
        reconnect["initial_delay_ms"] = json!(31_000);
        reconnect["max_delay_ms"] = json!(30_000);
        reconnect["backoff_factor_milli"] = json!(999);
        reconnect["jitter_percent"] = json!(101);
        reconnect["heartbeat_interval_ms"] = json!(20_000);
        reconnect["idle_timeout_ms"] = json!(20_000);
        reconnect["max_attempts"] = json!(0);
        let error = validation_text(&value);
        assert!(error.contains("initial_delay_ms must be <="));
        assert!(error.contains("between 1000 and 10000"));
        assert!(error.contains("jitter_percent must be <= 100"));
        assert!(error.contains("idle_timeout_ms must be greater"));
        assert!(error.contains("max_attempts must be null or non-zero"));
    }

    #[test]
    fn plaintext_grpc_cannot_carry_tls_configuration() {
        let mut value = valid_primary_json();
        value["sources"][0]["input"]["endpoint"] = json!("http://127.0.0.1:10000");
        let error = validation_text(&value);
        assert!(error.contains("tls is only valid with an https"));
        assert!(error.contains("auth is forbidden on a plaintext"));
    }

    #[test]
    fn quic_shred_requires_quic_endpoint_port_and_non_x_token_auth() {
        let mut value = valid_primary_json();
        value["sources"][1]["input"] = json!({
            "kind": "shred_quic",
            "endpoint": "quic://shreds.example.net",
            "auth": {
                "method": "x_token",
                "credential": { "provider": "env", "variable": "SHRED_TOKEN" }
            },
            "tls": {},
            "reconnect": reconnect_json(),
            "resume_overlap_events": 0
        });
        let error = validation_text(&value);
        assert!(error.contains("valid non-zero port"));
        assert!(error.contains("x_token is not valid"));
        assert!(error.contains("resume_overlap_events must be non-zero"));
    }
}
