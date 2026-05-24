use super::{
    ERROR_CACHE_CONTROL, FetchError, FetchProfile, get_block, get_block_profiled, now_ms,
    source::Storage,
};
use crate::car_to_json_stream::RenderProfile;
use crate::get_block::{
    GetBlockConfig, TransactionDetails, contains_json_parsed, parse_get_block_config,
    render_get_block_json_bytes, render_get_block_json_bytes_profiled, render_get_block_time,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use worker::{Headers, Request, Response, Result, RouteContext};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("Method not found: {method}"),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32000,
            message: message.into(),
        }
    }

    fn skipped_slot(slot: u64) -> Self {
        Self {
            code: -32009,
            message: format!("Slot {slot} was skipped, or missing in long-term storage"),
        }
    }
}

pub async fn handle_rpc(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let storage = Storage::from_env(&ctx.env)?;
    let profile = profile_requested(&req);
    let body = req.bytes().await?;
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => return json_rpc_response(error_response(Value::Null, -32700, err.to_string())),
    };

    if value.is_array() {
        return json_rpc_response(error_response(
            Value::Null,
            -32600,
            "JSON-RPC batch requests are not supported; submit one request per HTTP call for predictable pricing",
        ));
    }

    handle_one_response(&storage, value, profile).await
}

async fn handle_one_response(storage: &Storage, value: Value, profile: bool) -> Result<Response> {
    if let Some(response) = handle_one_bytes(storage, value.clone(), profile).await {
        return response;
    }
    json_rpc_response(handle_one(storage, value).await)
}

async fn handle_one_bytes(
    storage: &Storage,
    value: Value,
    profile: bool,
) -> Option<Result<Response>> {
    let request: RpcRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(err) => {
            return Some(json_rpc_response(error_response(
                Value::Null,
                -32600,
                err.to_string(),
            )));
        }
    };
    let id = request.id.clone().unwrap_or(Value::Null);

    if request.jsonrpc.as_deref() != Some("2.0") {
        return Some(json_rpc_response(error_response(
            id,
            -32600,
            "Invalid Request: jsonrpc must be \"2.0\"",
        )));
    }
    let Some(method) = request.method.as_deref() else {
        return Some(json_rpc_response(error_response(
            id,
            -32600,
            "Invalid Request: missing method",
        )));
    };

    if request.params.as_ref().is_some_and(contains_json_parsed) {
        return Some(json_rpc_response(error_response(
            id,
            -32602,
            "The jsonParsed encoding is not supported by this archive RPC",
        )));
    }

    if profile && method == "getBlock" {
        return Some(profiled_bytes_result_response(
            id,
            handle_get_block_bytes_profiled(storage, &request).await,
        ));
    }

    let result = match method {
        "getBlock" => handle_get_block_bytes(storage, &request).await,
        "getBlockTime" => handle_get_block_time_bytes(storage, &request).await,
        _ => return None,
    };

    Some(bytes_result_response(id, result))
}

fn profile_requested(req: &Request) -> bool {
    req.url().ok().is_some_and(|url| {
        url.query_pairs().any(|(key, value)| {
            matches!(key.as_ref(), "profile" | "timing" | "debugTiming")
                && !matches!(value.as_ref(), "0" | "false" | "off" | "no")
        })
    })
}

async fn handle_one(storage: &Storage, value: Value) -> Value {
    let request: RpcRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(err) => return error_response(Value::Null, -32600, err.to_string()),
    };
    let id = request.id.clone().unwrap_or(Value::Null);

    if request.jsonrpc.as_deref() != Some("2.0") {
        return error_response(id, -32600, "Invalid Request: jsonrpc must be \"2.0\"");
    }
    let Some(method) = request.method.as_deref() else {
        return error_response(id, -32600, "Invalid Request: missing method");
    };

    if request.params.as_ref().is_some_and(contains_json_parsed) {
        return error_response(
            id,
            -32602,
            "The jsonParsed encoding is not supported by this archive RPC",
        );
    }

    let result = match method {
        "getBlock" => handle_get_block(storage, &request).await,
        "getBlockTime" => handle_get_block_time(storage, &request).await,
        "getVersion" => Ok(json!({
            "solana-core": concat!("blockzilla-old-faithful-worker/", env!("CARGO_PKG_VERSION"))
        })),
        _ => Err(RpcError::method_not_found(method)),
    };

    match result {
        Ok(result) => success_response(id, result),
        Err(err) => error_response(id, err.code, err.message),
    }
}

fn bytes_result_response(
    id: Value,
    result: std::result::Result<Option<Vec<u8>>, RpcError>,
) -> Result<Response> {
    match result {
        Ok(result) => match success_response_bytes(&id, result.as_deref()) {
            Ok(bytes) => json_rpc_bytes_response(bytes),
            Err(err) => json_rpc_response(error_response(id, err.code, err.message)),
        },
        Err(err) => json_rpc_response(error_response(id, err.code, err.message)),
    }
}

fn profiled_bytes_result_response(
    id: Value,
    result: std::result::Result<ProfiledBlockBytes, RpcError>,
) -> Result<Response> {
    match result {
        Ok(result) => match success_response_bytes(&id, result.bytes.as_deref()) {
            Ok(bytes) => json_rpc_bytes_response_profiled(bytes, &result),
            Err(err) => json_rpc_response(error_response(id, err.code, err.message)),
        },
        Err(err) => json_rpc_response(error_response(id, err.code, err.message)),
    }
}

async fn handle_get_block_bytes(
    storage: &Storage,
    request: &RpcRequest,
) -> std::result::Result<Option<Vec<u8>>, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlock requires slot as first parameter"))?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;

    block_json_bytes(storage, slot, config).await
}

async fn handle_get_block_bytes_profiled(
    storage: &Storage,
    request: &RpcRequest,
) -> std::result::Result<ProfiledBlockBytes, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlock requires slot as first parameter"))?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;

    block_json_bytes_profiled(storage, slot, config).await
}

async fn handle_get_block(
    storage: &Storage,
    request: &RpcRequest,
) -> std::result::Result<Value, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlock requires slot as first parameter"))?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;

    block_json(storage, slot, config.rewards, config.transaction_details).await
}

async fn handle_get_block_time_bytes(
    storage: &Storage,
    request: &RpcRequest,
) -> std::result::Result<Option<Vec<u8>>, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlockTime requires slot as first parameter"))?;

    block_time_json_bytes(storage, slot).await
}

async fn handle_get_block_time(
    storage: &Storage,
    request: &RpcRequest,
) -> std::result::Result<Value, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlockTime requires slot as first parameter"))?;

    let block = block_json(storage, slot, false, TransactionDetails::None).await?;
    Ok(block.get("blockTime").cloned().unwrap_or(Value::Null))
}

async fn block_json_bytes(
    storage: &Storage,
    slot: u64,
    config: GetBlockConfig,
) -> std::result::Result<Option<Vec<u8>>, RpcError> {
    let block = get_block(storage, slot, config.rewards)
        .await
        .map_err(fetch_to_rpc_error)?;
    if block.bytes.is_empty() {
        return Err(RpcError::skipped_slot(slot));
    }

    render_get_block_json_bytes(block.bytes, block.previous_blockhash, config)
        .map(Some)
        .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))
}

struct ProfiledBlockBytes {
    slot: u64,
    transaction_details: TransactionDetails,
    rewards: bool,
    bytes: Option<Vec<u8>>,
    fetch: FetchProfile,
    render: Option<RenderProfile>,
    response_bytes: usize,
    total_us: u128,
}

struct RpcTimer(f64);

impl RpcTimer {
    fn start() -> Self {
        Self(now_ms())
    }

    fn elapsed_us(&self) -> u128 {
        ((now_ms() - self.0).max(0.0) * 1000.0) as u128
    }
}

async fn block_json_bytes_profiled(
    storage: &Storage,
    slot: u64,
    config: GetBlockConfig,
) -> std::result::Result<ProfiledBlockBytes, RpcError> {
    let total = RpcTimer::start();
    let profiled = get_block_profiled(storage, slot, config.rewards)
        .await
        .map_err(fetch_to_rpc_error)?;
    let fetch = profiled.profile;
    let block = profiled.block;

    if block.bytes.is_empty() {
        return Err(RpcError::skipped_slot(slot));
    }

    let (bytes, render) =
        render_get_block_json_bytes_profiled(block.bytes, block.previous_blockhash, config)
            .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))?;
    let response_bytes = bytes.len();
    Ok(ProfiledBlockBytes {
        slot,
        transaction_details: config.transaction_details,
        rewards: config.rewards,
        bytes: Some(bytes),
        fetch,
        render: Some(render),
        response_bytes,
        total_us: total.elapsed_us(),
    })
}

async fn block_json(
    storage: &Storage,
    slot: u64,
    include_rewards: bool,
    transaction_details: TransactionDetails,
) -> std::result::Result<Value, RpcError> {
    let Some(bytes) = block_json_bytes(
        storage,
        slot,
        GetBlockConfig {
            rewards: include_rewards,
            transaction_details,
            ..Default::default()
        },
    )
    .await?
    else {
        return Ok(Value::Null);
    };

    serde_json::from_slice(&bytes)
        .map_err(|err| RpcError::internal(format!("failed to parse generated slot {slot}: {err}")))
}

async fn block_time_json_bytes(
    storage: &Storage,
    slot: u64,
) -> std::result::Result<Option<Vec<u8>>, RpcError> {
    let block = get_block(storage, slot, false)
        .await
        .map_err(fetch_to_rpc_error)?;
    if block.bytes.is_empty() {
        return Err(RpcError::skipped_slot(slot));
    }

    let block_time = render_get_block_time(block.bytes)
        .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))?;
    Ok(Some(match block_time {
        Some(block_time) => block_time.to_string().into_bytes(),
        None => b"null".to_vec(),
    }))
}

fn params_array(request: &RpcRequest) -> std::result::Result<Vec<Value>, RpcError> {
    match request.params.as_ref() {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(_) => Err(RpcError::invalid_params("params must be an array")),
    }
}

fn fetch_to_rpc_error(err: FetchError) -> RpcError {
    RpcError {
        code: -32000,
        message: err.client_message(),
    }
}

fn success_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id,
    })
}

fn success_response_bytes(
    id: &Value,
    result: Option<&[u8]>,
) -> std::result::Result<Vec<u8>, RpcError> {
    let mut out = Vec::with_capacity(result.map_or(64, |bytes| bytes.len().saturating_add(64)));
    out.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"result\":");
    match result {
        Some(result) => out.extend_from_slice(result),
        None => out.extend_from_slice(b"null"),
    }
    out.extend_from_slice(b",\"id\":");
    serde_json::to_writer(&mut out, id)
        .map_err(|err| RpcError::internal(format!("failed to serialize JSON-RPC id: {err}")))?;
    out.extend_from_slice(b"}");
    Ok(out)
}

fn error_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message.into(),
        },
        "id": id,
    })
}

fn json_rpc_response(value: Value) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", ERROR_CACHE_CONTROL)?;
    headers.set("Content-Type", "application/json")?;

    Ok(Response::from_json(&value)?.with_headers(headers))
}

fn json_rpc_bytes_response(bytes: Vec<u8>) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", ERROR_CACHE_CONTROL)?;
    headers.set("Content-Type", "application/json")?;

    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}

fn json_rpc_bytes_response_profiled(
    bytes: Vec<u8>,
    profile: &ProfiledBlockBytes,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", ERROR_CACHE_CONTROL)?;
    headers.set("Content-Type", "application/json")?;
    headers.set("Server-Timing", &server_timing_header(profile))?;
    headers.set("X-OF-Profile", &profile_json(profile).to_string())?;

    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}

fn server_timing_header(profile: &ProfiledBlockBytes) -> String {
    let render_total = profile.render.as_ref().map_or(0, |render| render.total_us);
    let car_read = profile
        .render
        .as_ref()
        .map_or(0, |render| render.read_car_us);
    let parse_zstd = profile
        .render
        .as_ref()
        .map_or(0, |render| render.decode_transactions_us);
    let tx_json = profile
        .render
        .as_ref()
        .map_or(0, |render| render.write_transaction_json_us);
    let meta_json = profile
        .render
        .as_ref()
        .map_or(0, |render| render.write_meta_json_us);
    let output_json = profile
        .render
        .as_ref()
        .map_or(0, |render| render.write_transactions_us);

    format!(
        concat!(
            "total;dur={:.3}, ",
            "slot-index;dur={:.3}, ",
            "old-faithful;dur={:.3}, ",
            "parent-hash;dur={:.3}, ",
            "render;dur={:.3}, ",
            "car-parse;dur={:.3}, ",
            "parse-zstd;dur={:.3}, ",
            "tx-json;dur={:.3}, ",
            "meta-json;dur={:.3}, ",
            "output-json;dur={:.3}"
        ),
        us_to_ms(profile.total_us),
        us_to_ms(profile.fetch.slot_index_us),
        us_to_ms(
            profile.fetch.block_download_us
                + profile.fetch.reward_download_us
                + profile.fetch.parent_block_download_us,
        ),
        us_to_ms(profile.fetch.previous_blockhash_us),
        us_to_ms(render_total),
        us_to_ms(car_read),
        us_to_ms(parse_zstd),
        us_to_ms(tx_json),
        us_to_ms(meta_json),
        us_to_ms(output_json),
    )
}

fn profile_json(profile: &ProfiledBlockBytes) -> Value {
    json!({
        "slot": profile.slot,
        "transactionDetails": transaction_details_label(profile.transaction_details),
        "rewards": profile.rewards,
        "responseBytes": profile.response_bytes,
        "totalMs": us_to_ms(profile.total_us),
        "fetch": {
            "totalMs": us_to_ms(profile.fetch.total_us),
            "slotIndexMs": us_to_ms(profile.fetch.slot_index_us),
            "oldFaithfulDownloadMs": us_to_ms(profile.fetch.block_download_us),
            "oldFaithfulBytes": profile.fetch.block_download_bytes,
            "blockRangeOffset": profile.fetch.block_range_offset,
            "blockRangeLen": profile.fetch.block_range_len,
            "previousBlockhashMs": us_to_ms(profile.fetch.previous_blockhash_us),
            "parentSlotDecodeMs": us_to_ms(profile.fetch.parent_slot_decode_us),
            "parentSlotIndexMs": us_to_ms(profile.fetch.parent_slot_index_us),
            "parentBlockDownloadMs": us_to_ms(profile.fetch.parent_block_download_us),
            "parentBlockDownloadBytes": profile.fetch.parent_block_download_bytes,
            "parentBlockhashDecodeMs": us_to_ms(profile.fetch.parent_blockhash_decode_us),
            "rewardsScanMs": us_to_ms(profile.fetch.rewards_scan_us),
            "rewardLookupMs": us_to_ms(profile.fetch.reward_lookup_us),
            "rewardDownloadMs": us_to_ms(profile.fetch.reward_download_us),
            "rewardDownloadBytes": profile.fetch.reward_download_bytes,
            "rewardSpliceMs": us_to_ms(profile.fetch.reward_splice_us),
        },
        "render": profile.render.as_ref().map(render_profile_json),
    })
}

fn render_profile_json(profile: &RenderProfile) -> Value {
    json!({
        "mode": profile.mode,
        "inputBytes": profile.input_bytes,
        "outputBytes": profile.output_bytes,
        "transactions": profile.transactions,
        "totalMs": us_to_ms(profile.total_us),
        "carParseMs": us_to_ms(profile.read_car_us),
        "decodeRewardsMs": us_to_ms(profile.decode_rewards_us),
        "writeHeaderMs": us_to_ms(profile.write_header_us),
        "writeRewardsMs": us_to_ms(profile.write_rewards_us),
        "parseZstdAndDecodeTransactionsMs": us_to_ms(profile.decode_transactions_us),
        "writeTransactionsMs": us_to_ms(profile.write_transactions_us),
        "writeTransactionJsonMs": us_to_ms(profile.write_transaction_json_us),
        "writeMetaJsonMs": us_to_ms(profile.write_meta_json_us),
    })
}

fn transaction_details_label(details: TransactionDetails) -> &'static str {
    match details {
        TransactionDetails::Full => "full",
        TransactionDetails::Signatures => "signatures",
        TransactionDetails::None => "none",
        TransactionDetails::Accounts => "accounts",
    }
}

fn us_to_ms(us: u128) -> f64 {
    us as f64 / 1000.0
}
