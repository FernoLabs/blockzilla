use crate::{
    archive::{Archive, GetBlockOptions},
    get_block::{contains_json_parsed, parse_get_block_config},
};
use anyhow::Result;
use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone)]
pub struct RpcState {
    archive: Arc<Archive>,
    proxy_url: Option<String>,
    client: Client,
}

impl RpcState {
    pub fn new(archive: Arc<Archive>, proxy_url: Option<String>) -> Result<Self> {
        let client = Client::builder().http1_only().tcp_nodelay(true).build()?;
        Ok(Self {
            archive,
            proxy_url,
            client,
        })
    }
}

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

pub async fn handle_rpc(State(state): State<RpcState>, body: Bytes) -> Response {
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::OK,
                Json(error_response(Value::Null, -32700, err.to_string())),
            )
                .into_response();
        }
    };

    if value.is_array() {
        return (
            StatusCode::OK,
            Json(error_response(
                Value::Null,
                -32600,
                "JSON-RPC batch requests are not supported; submit one request per HTTP call for predictable pricing",
            )),
        )
            .into_response();
    }

    handle_one_response(&state, value).await
}

async fn handle_one_response(state: &RpcState, value: Value) -> Response {
    if let Some(response) = handle_one_bytes(state, value.clone()).await {
        return response;
    }
    (StatusCode::OK, Json(handle_one(state, value).await)).into_response()
}

async fn handle_one_bytes(state: &RpcState, value: Value) -> Option<Response> {
    let request: RpcRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(err) => {
            return Some(json_value_response(error_response(
                Value::Null,
                -32600,
                err.to_string(),
            )));
        }
    };
    let id = request.id.clone().unwrap_or(Value::Null);

    if request.jsonrpc.as_deref() != Some("2.0") {
        return Some(json_value_response(error_response(
            id,
            -32600,
            "Invalid Request: jsonrpc must be \"2.0\"",
        )));
    }
    let Some(method) = request.method.as_deref() else {
        return Some(json_value_response(error_response(
            id,
            -32600,
            "Invalid Request: missing method",
        )));
    };

    if request.params.as_ref().is_some_and(contains_json_parsed) {
        return Some(json_value_response(error_response(
            id,
            -32602,
            "The jsonParsed encoding is not supported by this archive RPC",
        )));
    }

    let result = match method {
        "getBlock" => handle_get_block_bytes(state, &request).await,
        "getBlockTime" => handle_get_block_time_bytes(state, &request).await,
        _ => return None,
    };

    Some(bytes_result_response(state, &request, id, method, result).await)
}

async fn handle_one(state: &RpcState, value: Value) -> Value {
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
        "getBlock" => handle_get_block(state, &request).await,
        "getBlockTime" => handle_get_block_time(state, &request).await,
        "getVersion" => Ok(json!({
            "solana-core": concat!("blockzilla-old-faithful-get-block-worker/", env!("CARGO_PKG_VERSION"))
        })),
        _ => {
            if state.proxy_url.is_some() {
                return proxy_one(state, &request).await;
            }
            Err(RpcError::method_not_found(method))
        }
    };

    match result {
        Ok(result) => success_response(id, result),
        Err(err) => {
            if state.proxy_url.is_some() && should_try_proxy(method, &err) {
                proxy_one(state, &request).await
            } else {
                error_response(id, err.code, err.message)
            }
        }
    }
}

async fn bytes_result_response(
    state: &RpcState,
    request: &RpcRequest,
    id: Value,
    method: &str,
    result: std::result::Result<Option<Vec<u8>>, RpcError>,
) -> Response {
    match result {
        Ok(result) => match success_response_bytes(&id, result.as_deref()) {
            Ok(bytes) => json_bytes_response(bytes),
            Err(err) => json_value_response(error_response(id, err.code, err.message)),
        },
        Err(err) => {
            if state.proxy_url.is_some() && should_try_proxy(method, &err) {
                json_value_response(proxy_one(state, request).await)
            } else {
                json_value_response(error_response(id, err.code, err.message))
            }
        }
    }
}

async fn handle_get_block_bytes(
    state: &RpcState,
    request: &RpcRequest,
) -> std::result::Result<Option<Vec<u8>>, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlock requires slot as first parameter"))?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;

    let result = state
        .archive
        .get_block_json_bytes(slot, GetBlockOptions { config })
        .await
        .map_err(|err| RpcError::internal(err.to_string()))?;
    result.map(Some).ok_or_else(|| RpcError::skipped_slot(slot))
}

async fn handle_get_block(
    state: &RpcState,
    request: &RpcRequest,
) -> std::result::Result<Value, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlock requires slot as first parameter"))?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;

    let result = state
        .archive
        .get_block_json(slot, GetBlockOptions { config })
        .await
        .map_err(|err| RpcError::internal(err.to_string()))?;
    result.ok_or_else(|| RpcError::skipped_slot(slot))
}

async fn handle_get_block_time_bytes(
    state: &RpcState,
    request: &RpcRequest,
) -> std::result::Result<Option<Vec<u8>>, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlockTime requires slot as first parameter"))?;
    let result = state
        .archive
        .get_block_time_json_bytes(slot)
        .await
        .map_err(|err| RpcError::internal(err.to_string()))?;
    result.map(Some).ok_or_else(|| RpcError::skipped_slot(slot))
}

async fn handle_get_block_time(
    state: &RpcState,
    request: &RpcRequest,
) -> std::result::Result<Value, RpcError> {
    let params = params_array(request)?;
    let slot = params
        .first()
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::invalid_params("getBlockTime requires slot as first parameter"))?;
    let result = state
        .archive
        .get_block_time(slot)
        .await
        .map_err(|err| RpcError::internal(err.to_string()))?;
    result.ok_or_else(|| RpcError::skipped_slot(slot))
}

fn params_array(request: &RpcRequest) -> std::result::Result<Vec<Value>, RpcError> {
    match request.params.as_ref() {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(_) => Err(RpcError::invalid_params("params must be an array")),
    }
}

fn should_try_proxy(method: &str, err: &RpcError) -> bool {
    err.code == -32000 && matches!(method, "getBlock" | "getBlockTime")
}

async fn proxy_one(state: &RpcState, request: &RpcRequest) -> Value {
    let Some(proxy_url) = &state.proxy_url else {
        let id = request.id.clone().unwrap_or(Value::Null);
        let method = request.method.as_deref().unwrap_or("<missing>");
        return error_response(id, -32601, format!("Method not found: {method}"));
    };
    let id = request.id.clone().unwrap_or(Value::Null);
    let response = state.client.post(proxy_url).json(request).send().await;
    let response = match response {
        Ok(response) => response,
        Err(err) => return error_response(id, -32000, format!("proxy request failed: {err}")),
    };
    match response.json::<Value>().await {
        Ok(value) => value,
        Err(err) => error_response(id, -32000, format!("proxy response was not JSON: {err}")),
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

fn json_value_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

fn json_bytes_response(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response()
}
