pub(crate) mod get_block;

use self::get_block::{
    contains_json_parsed, parse_get_block_config, render_get_block_json_bytes,
    render_get_block_time,
};
use crate::archive::{FetchedBlock, get_block as fetch_block_from_archive};
use crate::error::FetchError;
use crate::http_response::ERROR_CACHE_CONTROL;
use serde::Deserialize;
use serde_json::Value;
use worker::{Context, Env, Headers, Request, Response, Result};

#[derive(Debug, Deserialize)]
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

pub async fn handle_rpc(mut req: Request, env: &Env, ctx: &Context) -> Result<Response> {
    let value: Value = match serde_json::from_slice(&req.bytes().await?) {
        Ok(value) => value,
        Err(err) => return error_response(Value::Null, -32700, err.to_string()),
    };
    if value.is_array() {
        return error_response(
            Value::Null,
            -32600,
            "JSON-RPC batch requests are not supported; submit one request per HTTP call for predictable pricing",
        );
    }

    let RpcRequest {
        jsonrpc,
        id,
        method,
        params,
    } = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(err) => return error_response(Value::Null, -32600, err.to_string()),
    };
    let id = id.unwrap_or(Value::Null);

    if jsonrpc.as_deref() != Some("2.0") {
        return error_response(id, -32600, "Invalid Request: jsonrpc must be \"2.0\"");
    }
    let Some(method) = method.as_deref() else {
        return error_response(id, -32600, "Invalid Request: missing method");
    };
    if params.as_ref().is_some_and(contains_json_parsed) {
        return error_response(
            id,
            -32602,
            "The jsonParsed encoding is not supported by this archive RPC",
        );
    }

    match method {
        "getBlock" => bytes_result(id, get_block_json(env, ctx, params.as_ref()).await),
        "getBlockTime" => bytes_result(id, get_block_time(env, ctx, params.as_ref()).await),
        "getVersion" => version_response(id),
        _ => error_response(id, -32601, format!("Method not found: {method}")),
    }
}

async fn get_block_json(
    env: &Env,
    ctx: &Context,
    params: Option<&Value>,
) -> std::result::Result<Vec<u8>, RpcError> {
    let params = params_array(params)?;
    let slot = required_slot(params, "getBlock")?;
    let config = parse_get_block_config(params.get(1)).map_err(RpcError::invalid_params)?;
    let block = fetch_block(env, ctx, slot, config.rewards).await?;
    render_get_block_json_bytes(block.bytes, block.previous_blockhash, config)
        .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))
}

async fn get_block_time(
    env: &Env,
    ctx: &Context,
    params: Option<&Value>,
) -> std::result::Result<Vec<u8>, RpcError> {
    let params = params_array(params)?;
    let slot = required_slot(params, "getBlockTime")?;
    let block = fetch_block(env, ctx, slot, false).await?;
    Ok(
        match render_get_block_time(block.bytes)
            .map_err(|err| RpcError::internal(format!("failed to decode slot {slot}: {err}")))?
        {
            Some(block_time) => block_time.to_string().into_bytes(),
            None => b"null".to_vec(),
        },
    )
}

async fn fetch_block(
    env: &Env,
    ctx: &Context,
    slot: u64,
    include_rewards: bool,
) -> std::result::Result<FetchedBlock, RpcError> {
    let block = fetch_block_from_archive(env, ctx, slot, include_rewards)
        .await
        .map_err(fetch_to_rpc_error)?;
    if block.bytes.is_empty() {
        Err(RpcError::skipped_slot(slot))
    } else {
        Ok(block)
    }
}

fn params_array(params: Option<&Value>) -> std::result::Result<&[Value], RpcError> {
    match params {
        None => Ok(&[]),
        Some(Value::Array(items)) => Ok(items),
        Some(_) => Err(RpcError::invalid_params("params must be an array")),
    }
}

fn required_slot(params: &[Value], method: &str) -> std::result::Result<u64, RpcError> {
    params.first().and_then(Value::as_u64).ok_or_else(|| {
        RpcError::invalid_params(format!("{method} requires slot as first parameter"))
    })
}

fn fetch_to_rpc_error(err: FetchError) -> RpcError {
    RpcError {
        code: -32000,
        message: err.client_message(),
    }
}

fn bytes_result(id: Value, result: std::result::Result<Vec<u8>, RpcError>) -> Result<Response> {
    match result {
        Ok(result) => {
            let mut out = Vec::with_capacity(result.len().saturating_add(64));
            out.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"result\":");
            out.extend_from_slice(&result);
            out.extend_from_slice(b",\"id\":");
            if let Err(err) = serde_json::to_writer(&mut out, &id) {
                return error_response(
                    id,
                    -32000,
                    format!("failed to serialize JSON-RPC id: {err}"),
                );
            }
            out.extend_from_slice(b"}");
            bytes_response(out)
        }
        Err(err) => error_response(id, err.code, err.message),
    }
}

fn version_response(id: Value) -> Result<Response> {
    const VERSION_PREFIX: &[u8] =
        b"{\"jsonrpc\":\"2.0\",\"result\":{\"solana-core\":\"blockzilla-old-faithful-worker/";
    let mut out = Vec::with_capacity(VERSION_PREFIX.len() + env!("CARGO_PKG_VERSION").len() + 16);
    out.extend_from_slice(VERSION_PREFIX);
    out.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
    out.extend_from_slice(b"\"},\"id\":");
    serde_json::to_writer(&mut out, &id)?;
    out.extend_from_slice(b"}");
    bytes_response(out)
}

fn error_response(id: Value, code: i64, message: impl AsRef<str>) -> Result<Response> {
    let mut out = Vec::with_capacity(message.as_ref().len().saturating_add(80));
    out.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":");
    out.extend_from_slice(code.to_string().as_bytes());
    out.extend_from_slice(b",\"message\":");
    serde_json::to_writer(&mut out, message.as_ref())?;
    out.extend_from_slice(b"},\"id\":");
    serde_json::to_writer(&mut out, &id)?;
    out.extend_from_slice(b"}");
    bytes_response(out)
}

fn bytes_response(bytes: Vec<u8>) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", ERROR_CACHE_CONTROL)?;
    headers.set("Content-Type", "application/json")?;
    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}
