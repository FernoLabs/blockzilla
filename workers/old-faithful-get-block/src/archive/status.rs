use crate::error::{FetchError, FetchResult, SourceError};
use of_car_reader::slot_ranges::SLOTS_PER_EPOCH;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use wasm_bindgen::JsValue;
use worker::{Bucket, Env, Fetch, Headers, Method, Request, RequestInit};

const DEFAULT_AVAILABILITY_SCAN_START_EPOCH: u64 = 0;
const DEFAULT_AVAILABILITY_SCAN_LIMIT: u64 = 1_100;
const DEFAULT_CLUSTER_RPC_URL: &str = "https://api.mainnet-beta.solana.com";
const DEFAULT_CLUSTER_EPOCH_SLOT_MS: u64 = 400;
const DEFAULT_CLUSTER_EPOCH_CACHE_MIN_MS: u64 = 5 * 60 * 1000;
const DEFAULT_CLUSTER_EPOCH_CACHE_MAX_MS: u64 = 60 * 60 * 1000;

thread_local! {
    static CLUSTER_EPOCH_CACHE: RefCell<Option<CachedClusterEpochHint>> = const { RefCell::new(None) };
}

#[derive(Serialize)]
pub(crate) struct InfoAvailability {
    pub(crate) index_presence: Option<IndexPresenceInfo>,
    pub(crate) cluster_epoch_hint_error: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct IndexPresenceInfo {
    latest_indexed_epoch: u64,
    epoch_end_slot: u64,
    first_epoch_slot: u64,
    last_epoch_slot: u64,
    slots_per_epoch: u64,
    expected_car_path: String,
    index_object_path: String,
    index_version: &'static str,
    range_index_object_present: bool,
    v2_previous_blockhash_index_present: bool,
    scan_start_epoch: u64,
    scan_target_epoch: u64,
    cluster_epoch_hint: Option<ClusterEpochHint>,
}

#[derive(Clone, Serialize)]
struct ClusterEpochHint {
    epoch: u64,
    absolute_slot: u64,
    slot_index: u64,
    slots_in_epoch: u64,
    epoch_from_absolute_slot: u64,
    candidate_index_scan_epoch: u64,
    slots_per_epoch: u64,
    slots_remaining_in_epoch: u64,
    estimated_ms_until_epoch_end: u64,
    cache_ttl_ms: u64,
    fetched_at_ms: u64,
    expires_at_ms: u64,
}

#[derive(Clone)]
struct CachedClusterEpochHint {
    hint: ClusterEpochHint,
    expires_at_ms: u64,
}

pub(crate) async fn get(env: &Env) -> InfoAvailability {
    let hint_result = cached_cluster_epoch_hint(env).await;
    let cluster_epoch_hint_error = hint_result
        .as_ref()
        .err()
        .map(|_| "cluster_epoch_hint_unavailable".to_string());
    if cluster_epoch_hint_error.is_some() {
        worker::console_warn!("getEpochInfo hint unavailable");
    }
    let cluster_epoch_hint = hint_result.ok().flatten();
    let index_presence = latest_index_presence(env, cluster_epoch_hint.as_ref())
        .await
        .ok();

    InfoAvailability {
        index_presence,
        cluster_epoch_hint_error,
    }
}

async fn latest_index_presence(
    env: &Env,
    cluster_epoch_hint: Option<&ClusterEpochHint>,
) -> FetchResult<IndexPresenceInfo> {
    let start = env_u64(env, "OF_AVAILABILITY_SCAN_START_EPOCH")
        .unwrap_or(DEFAULT_AVAILABILITY_SCAN_START_EPOCH);
    let limit =
        env_u64(env, "OF_AVAILABILITY_SCAN_LIMIT").unwrap_or(DEFAULT_AVAILABILITY_SCAN_LIMIT);
    let fallback_end =
        start
            .checked_add(limit)
            .ok_or_else(|| FetchError::MalformedSlotIndexEntry {
                key: format!("availability scan from epoch {start}"),
                reason: "scan end overflow".to_string(),
            })?;
    let end = cluster_epoch_hint.as_ref().map_or(fallback_end, |hint| {
        hint.candidate_index_scan_epoch.max(start)
    });
    let index_bucket = env.bucket("OF_INDEXES")?;

    for epoch in (start..=end).rev() {
        if let Some(info) =
            epoch_index_presence(&index_bucket, epoch, start, end, cluster_epoch_hint).await?
        {
            return Ok(info);
        }
    }

    Err(FetchError::MissingSlotIndex {
        key: format!("no Old Faithful slot-index object found from {start} through {end}"),
    })
}

async fn epoch_index_presence(
    index_bucket: &Bucket,
    epoch: u64,
    scan_start_epoch: u64,
    scan_target_epoch: u64,
    cluster_epoch_hint: Option<&ClusterEpochHint>,
) -> FetchResult<Option<IndexPresenceInfo>> {
    let v2_path = format!("slot-index/epoch-{epoch}-slot-ranges-v2.raw");
    let raw_path = format!("slot-index/epoch-{epoch}-slot-ranges.raw");
    let (index_path, index_version, previous_blockhash_available) =
        if index_bucket.head(&v2_path).await?.is_some() {
            (v2_path, "slot-ranges-v2", true)
        } else if index_bucket.head(&raw_path).await?.is_some() {
            (raw_path, "slot-ranges-raw", false)
        } else {
            return Ok(None);
        };

    let first_epoch_slot =
        epoch
            .checked_mul(SLOTS_PER_EPOCH)
            .ok_or_else(|| FetchError::MalformedSlotIndexEntry {
                key: format!("epoch {epoch}"),
                reason: "epoch start overflow".to_string(),
            })?;
    let last_epoch_slot = first_epoch_slot
        .checked_add(SLOTS_PER_EPOCH - 1)
        .ok_or_else(|| FetchError::MalformedSlotIndexEntry {
            key: format!("epoch {epoch}"),
            reason: "epoch end overflow".to_string(),
        })?;

    Ok(Some(IndexPresenceInfo {
        latest_indexed_epoch: epoch,
        epoch_end_slot: last_epoch_slot,
        first_epoch_slot,
        last_epoch_slot,
        slots_per_epoch: SLOTS_PER_EPOCH,
        expected_car_path: format!("{epoch}/epoch-{epoch}.car"),
        index_object_path: index_path,
        index_version,
        range_index_object_present: true,
        v2_previous_blockhash_index_present: previous_blockhash_available,
        scan_start_epoch,
        scan_target_epoch,
        cluster_epoch_hint: cluster_epoch_hint.cloned(),
    }))
}

#[derive(Deserialize)]
struct GetEpochInfoResponse {
    result: Option<GetEpochInfoResult>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetEpochInfoResult {
    epoch: u64,
    absolute_slot: u64,
    slot_index: u64,
    slots_in_epoch: u64,
}

async fn cached_cluster_epoch_hint(env: &Env) -> FetchResult<Option<ClusterEpochHint>> {
    let now = js_sys::Date::now() as u64;
    if let Some(hint) = CLUSTER_EPOCH_CACHE.with(|cache| {
        cache
            .borrow()
            .as_ref()
            .filter(|cached| cached.expires_at_ms > now)
            .map(|cached| cached.hint.clone())
    }) {
        return Ok(Some(hint));
    }

    let Some(hint) = fetch_cluster_epoch_hint(env, now).await? else {
        return Ok(None);
    };
    CLUSTER_EPOCH_CACHE.with(|cache| {
        *cache.borrow_mut() = Some(CachedClusterEpochHint {
            expires_at_ms: hint.expires_at_ms,
            hint: hint.clone(),
        });
    });
    Ok(Some(hint))
}

async fn fetch_cluster_epoch_hint(
    env: &Env,
    fetched_at_ms: u64,
) -> FetchResult<Option<ClusterEpochHint>> {
    let rpc_url = env_string(env, "OF_CLUSTER_RPC_URL")
        .unwrap_or_else(|| DEFAULT_CLUSTER_RPC_URL.to_string());
    if rpc_url.eq_ignore_ascii_case("off") || rpc_url.eq_ignore_ascii_case("disabled") {
        return Ok(None);
    }

    let headers = Headers::new();
    headers.set("Content-Type", "application/json")?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_headers(headers);
    init.with_body(Some(JsValue::from_str(
        r#"{"jsonrpc":"2.0","id":"of-info","method":"getEpochInfo","params":[]}"#,
    )));

    let request = Request::new_with_init(&rpc_url, &init)?;
    let mut response = Fetch::Request(request).send().await?;
    let status = response.status_code();
    if status != 200 {
        return Err(FetchError::Source(SourceError::SourceStatus {
            path: "cluster RPC".to_string(),
            range: None,
            status,
        }));
    }

    let bytes = response.bytes().await?;
    let decoded: GetEpochInfoResponse = serde_json::from_slice(&bytes).map_err(|err| {
        FetchError::Source(SourceError::SourceBody {
            path: "cluster RPC".to_string(),
            reason: format!("decode getEpochInfo response: {err}"),
        })
    })?;
    if let Some(error) = decoded.error {
        return Err(FetchError::Source(SourceError::SourceBody {
            path: "cluster RPC".to_string(),
            reason: format!("getEpochInfo returned error: {error}"),
        }));
    }
    let Some(result) = decoded.result else {
        return Err(FetchError::Source(SourceError::SourceBody {
            path: "cluster RPC".to_string(),
            reason: "getEpochInfo response did not include result".to_string(),
        }));
    };

    Ok(Some(cluster_epoch_hint_from_rpc(
        env,
        result,
        fetched_at_ms,
    )))
}

fn cluster_epoch_hint_from_rpc(
    env: &Env,
    result: GetEpochInfoResult,
    fetched_at_ms: u64,
) -> ClusterEpochHint {
    let slot_ms = env_u64(env, "OF_CLUSTER_EPOCH_SLOT_MS").unwrap_or(DEFAULT_CLUSTER_EPOCH_SLOT_MS);
    let min_ttl_ms =
        env_u64(env, "OF_CLUSTER_EPOCH_CACHE_MIN_MS").unwrap_or(DEFAULT_CLUSTER_EPOCH_CACHE_MIN_MS);
    let max_ttl_ms =
        env_u64(env, "OF_CLUSTER_EPOCH_CACHE_MAX_MS").unwrap_or(DEFAULT_CLUSTER_EPOCH_CACHE_MAX_MS);
    let slots_per_epoch = SLOTS_PER_EPOCH;
    let epoch_from_absolute_slot = result.absolute_slot / slots_per_epoch;
    let slot_offset = result.absolute_slot % slots_per_epoch;
    let slots_remaining_in_epoch = slots_per_epoch.saturating_sub(slot_offset.saturating_add(1));
    let estimated_ms_until_epoch_end = slots_remaining_in_epoch.saturating_mul(slot_ms);
    let cache_ttl_ms =
        estimated_ms_until_epoch_end.clamp(min_ttl_ms.min(max_ttl_ms), min_ttl_ms.max(max_ttl_ms));

    ClusterEpochHint {
        epoch: result.epoch,
        absolute_slot: result.absolute_slot,
        slot_index: result.slot_index,
        slots_in_epoch: result.slots_in_epoch,
        epoch_from_absolute_slot,
        candidate_index_scan_epoch: epoch_from_absolute_slot.saturating_sub(1),
        slots_per_epoch,
        slots_remaining_in_epoch,
        estimated_ms_until_epoch_end,
        cache_ttl_ms,
        fetched_at_ms,
        expires_at_ms: fetched_at_ms.saturating_add(cache_ttl_ms),
    }
}

fn env_u64(env: &Env, name: &str) -> Option<u64> {
    env.var(name)
        .ok()
        .and_then(|value| value.to_string().parse().ok())
}

fn env_string(env: &Env, name: &str) -> Option<String> {
    env.var(name)
        .ok()
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty())
}
