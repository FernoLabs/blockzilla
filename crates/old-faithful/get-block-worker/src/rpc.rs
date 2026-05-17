use crate::{
    archive::{Archive, GetBlockOptions},
    get_block::{contains_json_parsed, parse_get_block_config},
};
use anyhow::Result;
use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::StatusCode,
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

    let response = if let Some(items) = value.as_array() {
        if items.is_empty() {
            error_response(Value::Null, -32600, "Invalid Request")
        } else {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(handle_one(&state, item.clone()).await);
            }
            Value::Array(out)
        }
    } else {
        handle_one(&state, value).await
    };

    (StatusCode::OK, Json(response)).into_response()
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
    Ok(result.unwrap_or(Value::Null))
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
    Ok(state
        .archive
        .get_block_time(slot)
        .await
        .map_err(|err| RpcError::internal(err.to_string()))?
        .unwrap_or(Value::Null))
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
