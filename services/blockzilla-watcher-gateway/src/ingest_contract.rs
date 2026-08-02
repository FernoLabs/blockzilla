use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Deserializer, de};
use serde_json::{Map, Number, Value};
use std::{collections::HashSet, fmt};

pub const MAX_INGEST_JSON_BYTES: usize = 1024 * 1024;
const MAX_PUBLIC_GAPS: usize = 32;
const MAX_PUBLIC_INCIDENTS: usize = 32;
const MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;

const OVERALL_STATES: &[&str] = &["healthy", "degraded", "failed", "unknown"];
const UPSTREAM_STATES: &[&str] = &["connected", "reconnecting", "stalled", "unavailable"];
const RECORDER_STATES: &[&str] = &["recording", "paused", "stalled", "unavailable"];
const REPLICATION_STATES: &[&str] = &["caught_up", "syncing", "stalled", "unavailable"];
const INDEXER_STATES: &[&str] = &["indexing", "caught_up", "stalled", "unavailable"];
const OBJECT_STORE_STATES: &[&str] = &["standby", "uploading", "degraded", "unavailable"];
const FALLBACK_STATES: &[&str] = &["capturing", "standby", "stalled", "unavailable"];
const GAP_COVERAGE: &[&str] = &["raw", "normalized", "rpc_recoverable", "unproven"];
const INCIDENT_SEVERITIES: &[&str] = &["warning", "error", "critical"];
const PUBLIC_INCIDENT_IDS: &[&str] = &[
    "grpc_stale",
    "upstream_access_blocked",
    "replay_gap",
    "resume_coverage",
    "replay_recovery_failed",
    "disk_space",
    "disk_check_failed",
    "volume_invalid",
    "cache_rotation_failed",
    "generation_rotation_failed",
    "generation_backlog",
    "object_store",
    "receiver_ack_stale",
];

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> de::Visitor<'de> for UniqueValueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(|number| UniqueValue(Value::Number(number)))
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1024));
        while let Some(UniqueValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(UniqueValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(
                    "ingest status contains a duplicate object key",
                ));
            }
            let UniqueValue(value) = object.next_value()?;
            values.insert(key, value);
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{name} must be an object"))
}

fn field<'a>(object: &'a Map<String, Value>, key: &str, name: &str) -> Result<&'a Value> {
    object
        .get(key)
        .ok_or_else(|| anyhow!("{name}.{key} is required"))
}

fn integer(value: &Value, name: &str, positive: bool, nullable: bool) -> Result<Value> {
    if nullable && value.is_null() {
        return Ok(Value::Null);
    }
    let number = value.as_u64().filter(|number| *number <= MAX_SAFE_INTEGER);
    let number = if positive {
        number.filter(|number| *number > 0)
    } else {
        number
    };
    let Some(number) = number else {
        bail!(
            "{name} must be {} safe integer",
            if positive {
                "a positive"
            } else {
                "a non-negative"
            }
        );
    };
    Ok(Value::Number(Number::from(number)))
}

fn enum_value(value: &Value, allowed: &[&str], name: &str) -> Result<Value> {
    let Some(text) = value.as_str() else {
        bail!("{name} has an unsupported value");
    };
    ensure!(allowed.contains(&text), "{name} has an unsupported value");
    Ok(Value::String(text.to_owned()))
}

fn select_stage(
    root: &Map<String, Value>,
    key: &str,
    fields: &[(&str, FieldKind<'_>)],
) -> Result<Value> {
    let source = object(field(root, key, "ingest status")?, key)?;
    let mut selected = Map::new();
    for (field_name, kind) in fields {
        let value = field(source, field_name, key)?;
        let public = match kind {
            FieldKind::Integer { positive, nullable } => {
                integer(value, &format!("{key}.{field_name}"), *positive, *nullable)?
            }
            FieldKind::Enum(allowed) => enum_value(value, allowed, &format!("{key}.{field_name}"))?,
        };
        selected.insert((*field_name).to_owned(), public);
    }
    Ok(Value::Object(selected))
}

enum FieldKind<'a> {
    Integer { positive: bool, nullable: bool },
    Enum(&'a [&'a str]),
}

const fn number(nullable: bool) -> FieldKind<'static> {
    FieldKind::Integer {
        positive: false,
        nullable,
    }
}

const fn positive_number(nullable: bool) -> FieldKind<'static> {
    FieldKind::Integer {
        positive: true,
        nullable,
    }
}

fn public_gaps(value: &Value) -> Result<Value> {
    let gaps = value
        .as_array()
        .ok_or_else(|| anyhow!("gaps must be a bounded array"))?;
    ensure!(
        gaps.len() <= MAX_PUBLIC_GAPS,
        "gaps must be a bounded array"
    );
    let mut ranges = HashSet::with_capacity(gaps.len());
    let mut selected = Vec::with_capacity(gaps.len());
    for (index, child) in gaps.iter().enumerate() {
        let name = format!("gaps[{index}]");
        let child = object(child, &name)?;
        let from = integer(
            field(child, "from_slot", &name)?,
            &format!("{name}.from_slot"),
            false,
            false,
        )?;
        let to = integer(
            field(child, "to_slot", &name)?,
            &format!("{name}.to_slot"),
            false,
            false,
        )?;
        let produced = integer(
            field(child, "produced_blocks", &name)?,
            &format!("{name}.produced_blocks"),
            false,
            true,
        )?;
        let coverage = enum_value(
            field(child, "coverage", &name)?,
            GAP_COVERAGE,
            &format!("{name}.coverage"),
        )?;
        let from_number = from.as_u64().unwrap();
        let to_number = to.as_u64().unwrap();
        ensure!(to_number >= from_number, "{name} has a reversed slot range");
        ensure!(
            ranges.insert((from_number, to_number)),
            "gap ranges must be unique"
        );
        selected.push(Value::Object(Map::from_iter([
            ("from_slot".into(), from),
            ("to_slot".into(), to),
            ("produced_blocks".into(), produced),
            ("coverage".into(), coverage),
        ])));
    }
    Ok(Value::Array(selected))
}

fn public_incidents(value: &Value) -> Result<Value> {
    let incidents = value
        .as_array()
        .ok_or_else(|| anyhow!("incidents must be a bounded array"))?;
    ensure!(
        incidents.len() <= MAX_PUBLIC_INCIDENTS,
        "incidents must be a bounded array"
    );
    let mut seen = HashSet::with_capacity(incidents.len());
    let mut selected = Vec::with_capacity(incidents.len());
    for (index, child) in incidents.iter().enumerate() {
        let name = format!("incidents[{index}]");
        let child = object(child, &name)?;
        let id = enum_value(
            field(child, "id", &name)?,
            PUBLIC_INCIDENT_IDS,
            &format!("{name}.id"),
        )?;
        let severity = enum_value(
            field(child, "severity", &name)?,
            INCIDENT_SEVERITIES,
            &format!("{name}.severity"),
        )?;
        let started = integer(
            field(child, "started_unix_secs", &name)?,
            &format!("{name}.started_unix_secs"),
            true,
            false,
        )?;
        let resolved = integer(
            field(child, "resolved_unix_secs", &name)?,
            &format!("{name}.resolved_unix_secs"),
            true,
            true,
        )?;
        let id_text = id.as_str().unwrap();
        ensure!(
            seen.insert(id_text.to_owned()),
            "incident ids must be unique"
        );
        if let Some(resolved_number) = resolved.as_u64() {
            ensure!(
                resolved_number >= started.as_u64().unwrap(),
                "{name} resolves before it starts"
            );
        }
        selected.push(Value::Object(Map::from_iter([
            ("id".into(), id),
            ("severity".into(), severity),
            ("started_unix_secs".into(), started),
            ("resolved_unix_secs".into(), resolved),
        ])));
    }
    Ok(Value::Array(selected))
}

fn stage<'a>(status: &'a Map<String, Value>, name: &str) -> &'a Map<String, Value> {
    status[name]
        .as_object()
        .expect("selected stage is an object")
}

fn nullable_u64(stage: &Map<String, Value>, key: &str) -> Option<u64> {
    stage[key].as_u64()
}

fn state<'a>(stage: &'a Map<String, Value>, key: &str) -> &'a str {
    stage[key].as_str().expect("selected state is a string")
}

fn validate_consistency(status: &Map<String, Value>) -> Result<()> {
    let updated = status["updated_unix_secs"].as_u64().unwrap();
    let future_limit = updated.saturating_add(5);
    let gaps = status["gaps"].as_array().unwrap();
    if status["gaps_truncated"].as_bool() == Some(true) {
        ensure!(
            gaps.len() == MAX_PUBLIC_GAPS,
            "truncated gap feed must fill the public gap window"
        );
    }

    for (stage_name, timestamp) in [
        ("upstream", "updated_unix_secs"),
        ("recorder", "updated_unix_secs"),
        ("replication", "updated_unix_secs"),
        ("indexer", "updated_unix_secs"),
        ("object_store", "updated_unix_secs"),
        ("fallback", "updated_unix_secs"),
    ] {
        if let Some(value) = nullable_u64(stage(status, stage_name), timestamp) {
            ensure!(
                value <= future_limit,
                "stage timestamp is ahead of the status sample"
            );
        }
    }

    let upstream = stage(status, "upstream");
    ensure!(
        (state(upstream, "state") == "unavailable") == upstream["updated_unix_secs"].is_null(),
        "upstream state contradicts its timestamp"
    );

    let recorder = stage(status, "recorder");
    let recorder_evidence = [
        "durable_slot",
        "updated_unix_secs",
        "disk_free_bytes",
        "disk_total_bytes",
    ];
    if state(recorder, "state") == "unavailable" {
        ensure!(
            recorder_evidence.iter().all(|key| recorder[*key].is_null()),
            "unavailable recorder includes live evidence"
        );
    } else {
        ensure!(
            recorder_evidence
                .iter()
                .all(|key| !recorder[*key].is_null()),
            "available recorder is missing live evidence"
        );
    }

    let replication = stage(status, "replication");
    let replication_evidence = [
        "ack_through_sequence",
        "ack_slot",
        "updated_unix_secs",
        "lag_records",
    ];
    if state(replication, "state") == "unavailable" {
        ensure!(
            replication_evidence
                .iter()
                .all(|key| replication[*key].is_null()),
            "unavailable replication includes live evidence"
        );
    } else {
        ensure!(
            !replication["ack_through_sequence"].is_null()
                && !replication["updated_unix_secs"].is_null()
                && !replication["lag_records"].is_null(),
            "available replication is missing live evidence"
        );
        let lag = nullable_u64(replication, "lag_records").unwrap();
        ensure!(
            (lag == 0) != replication["ack_slot"].is_null(),
            "replication ACK slot contradicts record lag"
        );
        ensure!(
            state(replication, "state") != "caught_up" || lag == 0,
            "caught-up replication has record lag"
        );
        ensure!(
            state(replication, "state") != "syncing" || lag != 0,
            "syncing replication has no record lag"
        );
    }

    let indexer = stage(status, "indexer");
    let indexer_evidence = ["last_slot", "updated_unix_secs", "lag_slots"];
    if state(indexer, "state") == "unavailable" {
        ensure!(
            indexer_evidence.iter().all(|key| indexer[*key].is_null()),
            "unavailable indexer includes live evidence"
        );
    } else {
        ensure!(
            indexer_evidence.iter().all(|key| !indexer[*key].is_null()),
            "available indexer is missing live evidence"
        );
    }
    ensure!(
        state(indexer, "state") != "caught_up" || nullable_u64(indexer, "lag_slots") == Some(0),
        "caught-up indexer has slot lag"
    );

    let object_store = stage(status, "object_store");
    if state(object_store, "state") == "unavailable" {
        ensure!(
            object_store["committed_bytes"].is_null()
                && nullable_u64(object_store, "pending_bytes") == Some(0)
                && object_store["updated_unix_secs"].is_null(),
            "unavailable object store includes live evidence"
        );
    } else {
        ensure!(
            !object_store["updated_unix_secs"].is_null(),
            "available object store is missing a timestamp"
        );
        let pending = nullable_u64(object_store, "pending_bytes").unwrap();
        ensure!(
            state(object_store, "state") != "standby" || pending == 0,
            "standby object store has pending uploads"
        );
        ensure!(
            state(object_store, "state") != "uploading" || pending != 0,
            "uploading object store has no pending uploads"
        );
    }

    let fallback = stage(status, "fallback");
    let fallback_evidence = ["last_slot", "updated_unix_secs", "lag_slots"];
    let fallback_present = fallback_evidence
        .iter()
        .filter(|key| !fallback[**key].is_null())
        .count();
    if state(fallback, "state") == "unavailable" {
        ensure!(
            fallback_present == 0,
            "unavailable fallback includes live evidence"
        );
    } else if state(fallback, "state") == "capturing" {
        ensure!(
            fallback_present == fallback_evidence.len(),
            "capturing fallback is missing live evidence"
        );
    } else {
        ensure!(
            fallback_present == 0 || fallback_present == fallback_evidence.len(),
            "fallback evidence is incomplete"
        );
    }

    for incident in status["incidents"].as_array().unwrap() {
        let incident = incident.as_object().unwrap();
        ensure!(
            incident["started_unix_secs"].as_u64().unwrap() <= future_limit
                && incident["resolved_unix_secs"]
                    .as_u64()
                    .is_none_or(|resolved| resolved <= future_limit),
            "incident timestamp is ahead of the status sample"
        );
    }

    let has_failure = status["incidents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|incident| {
            let incident = incident.as_object().unwrap();
            incident["resolved_unix_secs"].is_null()
                && matches!(incident["severity"].as_str(), Some("error" | "critical"))
        })
        || gaps.iter().any(|gap| gap["coverage"] == "unproven");
    let has_warning = status["incidents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|incident| {
            let incident = incident.as_object().unwrap();
            incident["resolved_unix_secs"].is_null() && incident["severity"] == "warning"
        })
        || gaps.iter().any(|gap| gap["coverage"] != "raw");
    let overall = status["overall_state"].as_str().unwrap();
    ensure!(
        !has_failure || overall == "failed",
        "overall state hides a failed continuity signal"
    );
    ensure!(
        !has_warning || overall != "healthy",
        "overall state hides a degraded continuity signal"
    );
    Ok(())
}

fn public_ingest_status(value: Value) -> Result<Value> {
    let root = object(&value, "ingest status")?;
    let schema = field(root, "schema_version", "ingest status")?;
    ensure!(
        schema.as_u64() == Some(1),
        "unsupported ingest status schema"
    );

    let mut selected = Map::new();
    selected.insert("schema_version".into(), Value::Number(Number::from(1)));
    selected.insert(
        "updated_unix_secs".into(),
        integer(
            field(root, "updated_unix_secs", "ingest status")?,
            "updated_unix_secs",
            true,
            false,
        )?,
    );
    selected.insert(
        "overall_state".into(),
        enum_value(
            field(root, "overall_state", "ingest status")?,
            OVERALL_STATES,
            "overall_state",
        )?,
    );
    selected.insert(
        "upstream".into(),
        select_stage(
            root,
            "upstream",
            &[
                ("state", FieldKind::Enum(UPSTREAM_STATES)),
                ("updated_unix_secs", positive_number(true)),
                ("reconnects_1h", number(true)),
            ],
        )?,
    );
    selected.insert(
        "recorder".into(),
        select_stage(
            root,
            "recorder",
            &[
                ("state", FieldKind::Enum(RECORDER_STATES)),
                ("durable_slot", number(true)),
                ("updated_unix_secs", positive_number(true)),
                ("active_bytes", number(false)),
                ("sealed_generations", number(false)),
                ("unacknowledged_bytes", number(false)),
                ("disk_free_bytes", number(true)),
                ("disk_total_bytes", number(true)),
            ],
        )?,
    );
    selected.insert(
        "replication".into(),
        select_stage(
            root,
            "replication",
            &[
                ("state", FieldKind::Enum(REPLICATION_STATES)),
                ("ack_through_sequence", number(true)),
                ("ack_slot", number(true)),
                ("updated_unix_secs", positive_number(true)),
                ("lag_records", number(true)),
            ],
        )?,
    );
    selected.insert(
        "indexer".into(),
        select_stage(
            root,
            "indexer",
            &[
                ("state", FieldKind::Enum(INDEXER_STATES)),
                ("last_slot", number(true)),
                ("updated_unix_secs", positive_number(true)),
                ("lag_slots", number(true)),
            ],
        )?,
    );
    selected.insert(
        "object_store".into(),
        select_stage(
            root,
            "object_store",
            &[
                ("state", FieldKind::Enum(OBJECT_STORE_STATES)),
                ("provider", FieldKind::Enum(&["r2"])),
                ("committed_bytes", number(true)),
                ("pending_bytes", number(false)),
                ("updated_unix_secs", positive_number(true)),
            ],
        )?,
    );
    selected.insert(
        "fallback".into(),
        select_stage(
            root,
            "fallback",
            &[
                ("state", FieldKind::Enum(FALLBACK_STATES)),
                ("last_slot", number(true)),
                ("updated_unix_secs", positive_number(true)),
                ("lag_slots", number(true)),
            ],
        )?,
    );

    let recorder = selected["recorder"].as_object().unwrap();
    if let (Some(free), Some(total)) = (
        recorder["disk_free_bytes"].as_u64(),
        recorder["disk_total_bytes"].as_u64(),
    ) {
        ensure!(
            free <= total,
            "recorder.disk_free_bytes exceeds total capacity"
        );
    }

    selected.insert(
        "gaps".into(),
        public_gaps(field(root, "gaps", "ingest status")?)?,
    );
    let gaps_truncated = field(root, "gaps_truncated", "ingest status")?
        .as_bool()
        .ok_or_else(|| anyhow!("gaps_truncated must be boolean"))?;
    selected.insert("gaps_truncated".into(), Value::Bool(gaps_truncated));
    selected.insert(
        "incidents".into(),
        public_incidents(field(root, "incidents", "ingest status")?)?,
    );
    validate_consistency(&selected)?;
    Ok(Value::Object(selected))
}

pub fn public_ingest_json_bytes(raw: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        raw.len() <= MAX_INGEST_JSON_BYTES,
        "ingest status exceeds the public proxy limit"
    );
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let UniqueValue(value) = UniqueValue::deserialize(&mut deserializer)
        .context("decode duplicate-safe ingest status")?;
    deserializer.end().context("trailing ingest JSON data")?;
    serde_json::to_vec(&public_ingest_status(value)?).context("encode public ingest status")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_status() -> Value {
        json!({
            "schema_version": 1,
            "updated_unix_secs": 100,
            "overall_state": "healthy",
            "upstream": {"state": "connected", "updated_unix_secs": 100, "reconnects_1h": null},
            "recorder": {
                "state": "recording", "durable_slot": 1000, "updated_unix_secs": 100,
                "active_bytes": 1024, "sealed_generations": 0, "unacknowledged_bytes": 0,
                "disk_free_bytes": 2048, "disk_total_bytes": 4096
            },
            "replication": {
                "state": "caught_up", "ack_through_sequence": 1000, "ack_slot": 1000,
                "updated_unix_secs": 100, "lag_records": 0
            },
            "indexer": {"state": "unavailable", "last_slot": null, "updated_unix_secs": null, "lag_slots": null},
            "object_store": {"state": "unavailable", "provider": "r2", "committed_bytes": null, "pending_bytes": 0, "updated_unix_secs": null},
            "fallback": {"state": "unavailable", "last_slot": null, "updated_unix_secs": null, "lag_slots": null},
            "gaps": [], "gaps_truncated": false, "incidents": []
        })
    }

    #[test]
    fn projects_only_contract_fields() {
        let mut value = valid_status();
        value["secret"] = json!("must-not-leak");
        value["upstream"]["endpoint"] = json!("https://private.invalid");
        value["replication"]["journal_id"] = json!("private");
        let public: Value = serde_json::from_slice(
            &public_ingest_json_bytes(&serde_json::to_vec(&value).unwrap()).unwrap(),
        )
        .unwrap();
        let encoded = serde_json::to_string(&public).unwrap();
        assert!(!encoded.contains("must-not-leak"));
        assert!(!encoded.contains("endpoint"));
        assert!(!encoded.contains("journal_id"));
    }

    #[test]
    fn rejects_duplicate_keys() {
        assert!(public_ingest_json_bytes(br#"{"schema_version":1,"schema_version":1}"#).is_err());
    }

    #[test]
    fn rejects_contradictory_or_unsafe_fields() {
        let mut value = valid_status();
        value["recorder"]["active_bytes"] = Value::Bool(true);
        assert!(public_ingest_json_bytes(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value = valid_status();
        value["upstream"]["state"] = json!("secret-value");
        assert!(public_ingest_json_bytes(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value = valid_status();
        value["replication"]["lag_records"] = json!(2);
        assert!(public_ingest_json_bytes(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn ingest_limit_is_independent() {
        assert!(public_ingest_json_bytes(&vec![b' '; MAX_INGEST_JSON_BYTES + 1]).is_err());
    }
}
