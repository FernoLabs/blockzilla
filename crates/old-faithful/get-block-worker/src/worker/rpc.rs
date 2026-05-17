use super::{ERROR_CACHE_CONTROL, FetchError, get_block, source::Storage};
use crate::get_block::{
    GetBlockConfig, TransactionDetails, contains_json_parsed, parse_get_block_config,
    render_get_block_json,
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
}

pub async fn handle_rpc(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let storage = Storage::from_env(&ctx.env)?;
    let body = req.bytes().await?;
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => return json_rpc_response(error_response(Value::Null, -32700, err.to_string())),
    };

    let response = if let Some(items) = value.as_array() {
        if items.is_empty() {
            error_response(Value::Null, -32600, "Invalid Request")
        } else {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(handle_one(&storage, item.clone()).await);
            }
            Value::Array(out)
        }
    } else {
        handle_one(&storage, value).await
    };

    json_rpc_response(response)
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

async fn block_json(
    storage: &Storage,
    slot: u64,
    include_rewards: bool,
    transaction_details: TransactionDetails,
) -> std::result::Result<Value, RpcError> {
    let block = get_block(storage, slot, include_rewards)
        .await
        .map_err(fetch_to_rpc_error)?;
    if block.bytes.is_empty() {
        return Ok(Value::Null);
    }

    render_get_block_json(
        block.bytes,
        block.previous_blockhash,
        GetBlockConfig {
            rewards: include_rewards,
            transaction_details,
            ..Default::default()
        },
    )
    .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))
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
