use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{cell::Cell, rc::Rc};

pub(crate) const RPC_API_KEY_PREFIX: &str = "bz_live_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectCacheVariant {
    Json { lite: bool, include_rewards: bool },
    Binary { include_access: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectCacheIdentity {
    pub(crate) path: String,
    pub(crate) query_name: &'static str,
    pub(crate) query_value: &'static str,
}

pub(crate) fn direct_cache_identity(
    generation: &str,
    slot: u64,
    variant: DirectCacheVariant,
) -> DirectCacheIdentity {
    match variant {
        DirectCacheVariant::Json {
            lite,
            include_rewards,
        } => DirectCacheIdentity {
            path: format!(
                "/__blockzilla-cache/v1/{generation}/{}/{slot}.json",
                if lite { "block-lite" } else { "block" },
            ),
            query_name: "rewards",
            query_value: if include_rewards { "1" } else { "0" },
        },
        DirectCacheVariant::Binary { include_access } => DirectCacheIdentity {
            path: format!("/__blockzilla-cache/v1/{generation}/block/{slot}.bin"),
            query_name: "access",
            query_value: if include_access {
                "access"
            } else {
                "no-access"
            },
        },
    }
}

#[derive(Clone, Default)]
pub(crate) struct BackendReadTracker(Rc<Cell<bool>>);

impl BackendReadTracker {
    pub(crate) fn mark(&self) {
        self.0.set(true);
    }

    pub(crate) fn billable_reads(&self) -> u8 {
        u8::from(self.0.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BearerKeyError {
    Missing,
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RpcApiKeyStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RpcApiKeyRecord {
    pub(crate) key_id: String,
    pub(crate) customer_id: String,
    pub(crate) label: String,
    pub(crate) status: RpcApiKeyStatus,
}

impl RpcApiKeyRecord {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if !valid_stable_id(&self.key_id) {
            return Err("keyId must be 1-64 ASCII letters, digits, underscores, or hyphens");
        }
        if !valid_stable_id(&self.customer_id) {
            return Err("customerId must be 1-64 ASCII letters, digits, underscores, or hyphens");
        }
        if self.label.is_empty()
            || self.label.len() > 128
            || self.label.chars().any(char::is_control)
        {
            return Err("label must be 1-128 characters without control characters");
        }
        Ok(())
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.status == RpcApiKeyStatus::Enabled
    }
}

pub(crate) fn parse_bearer_key(header: Option<&str>) -> Result<&str, BearerKeyError> {
    let header = header.ok_or(BearerKeyError::Missing)?;
    let mut parts = header.split_ascii_whitespace();
    let scheme = parts.next().ok_or(BearerKeyError::Malformed)?;
    let key = parts.next().ok_or(BearerKeyError::Malformed)?;
    if !scheme.eq_ignore_ascii_case("bearer") || parts.next().is_some() || !valid_api_key(key) {
        return Err(BearerKeyError::Malformed);
    }
    Ok(key)
}

pub(crate) fn api_key_digest(key: &str) -> String {
    let digest = Sha256::digest(key.as_bytes());
    hex_lower(&digest)
}

pub(crate) fn rpc_method_for_metrics(value: &Value) -> &'static str {
    if value.is_array() {
        return "batch";
    }
    match value.get("method").and_then(Value::as_str) {
        Some("getBlock") => "getBlock",
        Some("getBlockTime") => "getBlockTime",
        Some("getVersion") => "getVersion",
        Some(_) => "other",
        None => "invalid",
    }
}

fn valid_api_key(key: &str) -> bool {
    let Some(random_part) = key.strip_prefix(RPC_API_KEY_PREFIX) else {
        return false;
    };
    (32..=120).contains(&random_part.len())
        && random_part
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_stable_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_KEY: &str = "bz_live_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";

    #[test]
    fn parses_well_formed_bearer_key() {
        assert_eq!(
            parse_bearer_key(Some(&format!("Bearer {VALID_KEY}"))),
            Ok(VALID_KEY)
        );
        assert_eq!(
            parse_bearer_key(Some(&format!("bearer\t{VALID_KEY}"))),
            Ok(VALID_KEY)
        );
    }

    #[test]
    fn rejects_missing_or_malformed_bearer_key() {
        assert_eq!(parse_bearer_key(None), Err(BearerKeyError::Missing));
        assert_eq!(
            parse_bearer_key(Some(VALID_KEY)),
            Err(BearerKeyError::Malformed)
        );
        assert_eq!(
            parse_bearer_key(Some("Bearer bz_live_too-short")),
            Err(BearerKeyError::Malformed)
        );
        assert_eq!(
            parse_bearer_key(Some(&format!("Bearer {VALID_KEY} extra"))),
            Err(BearerKeyError::Malformed)
        );
    }

    #[test]
    fn hashes_keys_as_lowercase_sha256() {
        assert_eq!(
            api_key_digest("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn parses_and_validates_key_record() {
        let record: RpcApiKeyRecord = serde_json::from_str(
            r#"{"keyId":"key_acme_main","customerId":"customer_acme","label":"Acme production","status":"enabled"}"#,
        )
        .expect("record JSON");
        assert!(record.validate().is_ok());
        assert!(record.is_enabled());
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_key_records() {
        let unknown_field = serde_json::from_str::<RpcApiKeyRecord>(
            r#"{"keyId":"key_1","customerId":"customer_1","label":"Primary","status":"enabled","enabled":true}"#,
        );
        assert!(unknown_field.is_err());

        let invalid = RpcApiKeyRecord {
            key_id: "key/one".to_string(),
            customer_id: "customer_1".to_string(),
            label: "Primary".to_string(),
            status: RpcApiKeyStatus::Disabled,
        };
        assert!(invalid.validate().is_err());
        assert!(!invalid.is_enabled());
    }

    #[test]
    fn keeps_metric_method_cardinality_bounded() {
        assert_eq!(
            rpc_method_for_metrics(&serde_json::json!({"method": "getBlock"})),
            "getBlock"
        );
        assert_eq!(
            rpc_method_for_metrics(&serde_json::json!({"method": "attacker-controlled"})),
            "other"
        );
        assert_eq!(rpc_method_for_metrics(&serde_json::json!([])), "batch");
    }

    #[test]
    fn collapses_all_backend_attempts_to_one_billable_read() {
        let tracker = BackendReadTracker::default();
        let shared = tracker.clone();
        assert_eq!(tracker.billable_reads(), 0);

        shared.mark();
        shared.mark();

        assert_eq!(tracker.billable_reads(), 1);
    }

    #[test]
    fn canonical_cache_identity_contains_only_content_dimensions() {
        let full = direct_cache_identity(
            "render-generation",
            42,
            DirectCacheVariant::Json {
                lite: false,
                include_rewards: true,
            },
        );
        assert_eq!(
            full.path,
            "/__blockzilla-cache/v1/render-generation/block/42.json"
        );
        assert_eq!((full.query_name, full.query_value), ("rewards", "1"));

        let lite = direct_cache_identity(
            "render-generation",
            42,
            DirectCacheVariant::Json {
                lite: true,
                include_rewards: false,
            },
        );
        assert_ne!(full, lite);

        let binary = direct_cache_identity(
            "render-generation",
            42,
            DirectCacheVariant::Binary {
                include_access: false,
            },
        );
        assert_eq!(
            binary.path,
            "/__blockzilla-cache/v1/render-generation/block/42.bin"
        );
        assert_eq!(
            (binary.query_name, binary.query_value),
            ("access", "no-access")
        );
    }
}
