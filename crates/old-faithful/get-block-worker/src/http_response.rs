use serde::Serialize;
use worker::{Headers, Response, Result};

use crate::archive::FetchedBlock;
use crate::render::{
    car_bytes_to_json_bytes, car_bytes_to_json_light_bytes, car_bytes_to_protobuf,
};

pub(crate) const BLOCK_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
pub(crate) const INFO_CACHE_CONTROL: &str = "public, max-age=300, stale-while-revalidate=300";
pub(crate) const ERROR_CACHE_CONTROL: &str = "no-store";

#[derive(Clone, Copy)]
pub(crate) enum BlockResponseFormat {
    Json,
    Protobuf,
}

#[derive(Clone, Copy)]
pub(crate) enum BlockResponseMode {
    Full,
    Lite,
}

pub(crate) fn block_response(
    slot: u64,
    block: FetchedBlock,
    mode: BlockResponseMode,
    format: BlockResponseFormat,
    include_rewards: bool,
) -> Result<Response> {
    if block.bytes.is_empty() {
        let mut out = Vec::with_capacity(32);
        out.extend_from_slice(b"{\"ok\":true,\"empty\":true,\"slot\":");
        out.extend_from_slice(slot.to_string().as_bytes());
        out.extend_from_slice(b"}");
        return cached_binary_response(out, "application/json", BLOCK_CACHE_CONTROL);
    }

    match (mode, format) {
        (BlockResponseMode::Full, BlockResponseFormat::Json) => {
            match car_bytes_to_json_bytes(block.bytes, block.previous_blockhash, include_rewards) {
                Ok(bytes) => cached_binary_response(bytes, "application/json", BLOCK_CACHE_CONTROL),
                Err(err) => {
                    worker::console_error!("Error decoding slot {}: {:?}", slot, err);
                    json_error(
                        502,
                        "decode_error",
                        &format!("failed to decode slot {}: {err}", slot),
                    )
                }
            }
        }
        (BlockResponseMode::Lite, BlockResponseFormat::Json) => {
            match car_bytes_to_json_light_bytes(
                block.bytes,
                block.previous_blockhash,
                include_rewards,
            ) {
                Ok(bytes) => cached_binary_response(bytes, "application/json", BLOCK_CACHE_CONTROL),
                Err(err) => {
                    worker::console_error!("Error decoding slot {} (lite): {:?}", slot, err);
                    json_error(
                        502,
                        "decode_error",
                        &format!("failed to decode slot {} (lite): {err}", slot),
                    )
                }
            }
        }
        (mode, BlockResponseFormat::Protobuf) => match car_bytes_to_protobuf(
            slot,
            block.bytes,
            block.previous_blockhash,
            include_rewards,
            matches!(mode, BlockResponseMode::Full),
        ) {
            Ok(bytes) => {
                cached_binary_response(bytes, "application/x-protobuf", BLOCK_CACHE_CONTROL)
            }
            Err(err) => {
                worker::console_error!("Error protobuf-encoding slot {}: {:?}", slot, err);
                json_error(
                    502,
                    "decode_error",
                    &format!("failed to protobuf-encode slot {}: {err}", slot),
                )
            }
        },
    }
}

pub(crate) fn cached_json_response<T: Serialize>(
    value: &T,
    cache_control: &str,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Type", "application/json")?;

    Ok(Response::from_json(value)?.with_headers(headers))
}

pub(crate) fn cached_binary_response(
    bytes: Vec<u8>,
    content_type: &str,
    cache_control: &str,
) -> Result<Response> {
    let headers = Headers::new();
    headers.set("Cache-Control", cache_control)?;
    headers.set("Content-Length", &bytes.len().to_string())?;
    headers.set("Content-Type", content_type)?;

    Ok(Response::from_bytes(bytes)?.with_headers(headers))
}

pub(crate) fn json_error(status: u16, code: &str, message: &str) -> Result<Response> {
    let mut out = Vec::with_capacity(code.len().saturating_add(message.len()).saturating_add(48));
    out.extend_from_slice(b"{\"ok\":false,\"error\":{\"code\":");
    serde_json::to_writer(&mut out, code)?;
    out.extend_from_slice(b",\"message\":");
    serde_json::to_writer(&mut out, message)?;
    out.extend_from_slice(b"}}");
    Ok(cached_binary_response(out, "application/json", ERROR_CACHE_CONTROL)?.with_status(status))
}

pub(crate) fn with_server_timing(
    mut response: Response,
    started_at: f64,
    fetch_started_at: f64,
    render_started_at: f64,
) -> Result<Response> {
    let finished_at = js_sys::Date::now();
    response.headers_mut().set(
        "Server-Timing",
        &format!(
            "source;dur={:.1}, render;dur={:.1}, total;dur={:.1}",
            render_started_at - fetch_started_at,
            finished_at - render_started_at,
            finished_at - started_at
        ),
    )?;
    Ok(response)
}
