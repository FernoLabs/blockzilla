mod archive;
mod error;
mod http_response;
mod render;
mod rpc;

use crate::archive::get_block;
use crate::http_response::{
    BlockResponseFormat, BlockResponseMode, INFO_CACHE_CONTROL, block_response,
    cached_json_response, json_error, with_server_timing,
};
use worker::*;

#[event(fetch)]
async fn main(req: Request, env: Env, ctx: Context) -> Result<Response> {
    let method = req.method();
    let path = req.path();

    match (method, path.as_str()) {
        (Method::Get, "/") | (Method::Get, "/info") => handle_info(env).await,
        (Method::Post, "/") => rpc::handle_rpc(req, &env, &ctx).await,
        (Method::Get, path) if path.starts_with("/block/") => {
            handle_block(req, &env, &ctx, &path["/block/".len()..]).await
        }
        (Method::Get, path) if path.starts_with("/block-lite/") => {
            handle_block_lite(req, &env, &ctx, &path["/block-lite/".len()..]).await
        }
        _ => json_error(404, "not_found", "route not found"),
    }
}

async fn handle_info(env: Env) -> Result<Response> {
    let info = archive::get_status(&env).await;

    cached_json_response(
        &serde_json::json!({
            "ok": true,
            "json_rpc_methods": ["getBlock", "getBlockTime", "getVersion"],
            "block_routes": ["/block/:slot", "/block-lite/:slot"],
            "slot_index_source": "r2-slot-index-raw-with-compact-index-warmup-fallback",
            "blockhash_index_source": "r2-slot-index-v2-raw",
            "archive_source": "official-old-faithful-http",
            "availability": info.availability,
            "cluster_epoch_hint_error": info.cluster_epoch_hint_error,
        }),
        INFO_CACHE_CONTROL,
    )
}

async fn handle_block(req: Request, env: &Env, ctx: &Context, raw_slot: &str) -> Result<Response> {
    handle_block_response(req, env, ctx, raw_slot, BlockResponseMode::Full).await
}

async fn handle_block_lite(
    req: Request,
    env: &Env,
    ctx: &Context,
    raw_slot: &str,
) -> Result<Response> {
    handle_block_response(req, env, ctx, raw_slot, BlockResponseMode::Lite).await
}

async fn handle_block_response(
    req: Request,
    env: &Env,
    ctx: &Context,
    raw_slot: &str,
    mode: BlockResponseMode,
) -> Result<Response> {
    let started_at = js_sys::Date::now();
    let (slot, format, include_rewards) = match parse_block_request(&req, raw_slot) {
        Ok(request) => request,
        Err(response) => return Ok(response),
    };

    let fetch_started_at = js_sys::Date::now();
    let cache_key = block_cache_key(slot, mode, format, include_rewards);
    let cache = Cache::default();
    match cache.get(cache_key.clone(), false).await {
        Ok(Some(response)) => {
            return Ok(response);
        }
        Ok(None) => {}
        Err(err) => console_warn!("block cache lookup failed for slot {}: {}", slot, err),
    }

    let block = match get_block(env, ctx, slot, include_rewards).await {
        Ok(block) => block,
        Err(err) => {
            console_error!("Error fetching slot {}: {:?}", slot, err);
            let response = json_error(
                err.status_code(),
                err.code(),
                &format!("failed to fetch slot {}: {}", slot, err.client_message()),
            )?;
            return with_server_timing(response, started_at, fetch_started_at, js_sys::Date::now());
        }
    };
    let render_started_at = js_sys::Date::now();

    let mut response = block_response(slot, block, mode, format, include_rewards)?;
    let cache_policy = block_cache_policy(mode, format, &response);
    response
        .headers_mut()
        .set("x-of-cacheable", cache_policy.header_value())?;
    if cache_policy.cache {
        let mut cache_response = response.cloned()?;
        cache_response.headers_mut().set("x-of-cache", "HIT")?;
        cache_response
            .headers_mut()
            .set("x-of-cacheable", cache_policy.header_value())?;
        cache_response
            .headers_mut()
            .set("Server-Timing", "cache;desc=\"hit\"")?;
        ctx.wait_until(async move {
            if let Err(err) = Cache::default().put(cache_key, cache_response).await {
                console_warn!("block cache write failed: {}", err);
            }
        });
    }

    response.headers_mut().set("x-of-cache", "MISS")?;
    with_server_timing(response, started_at, fetch_started_at, render_started_at)
}

struct CachePolicy {
    cache: bool,
    reason: &'static str,
}

impl CachePolicy {
    fn header_value(&self) -> &'static str {
        if self.cache { "yes" } else { self.reason }
    }
}

fn block_cache_policy(
    mode: BlockResponseMode,
    format: BlockResponseFormat,
    response: &Response,
) -> CachePolicy {
    if response.status_code() != 200 {
        return CachePolicy {
            cache: false,
            reason: "status",
        };
    }
    if matches!(
        (mode, format),
        (BlockResponseMode::Full, BlockResponseFormat::Json)
    ) {
        return CachePolicy {
            cache: false,
            reason: "full-json",
        };
    }
    CachePolicy {
        cache: true,
        reason: "yes",
    }
}

fn block_cache_key(
    slot: u64,
    mode: BlockResponseMode,
    format: BlockResponseFormat,
    include_rewards: bool,
) -> String {
    format!(
        "https://of-getblock-worker.cache/v2/{}/{slot}.{}?rewards={include_rewards}",
        match mode {
            BlockResponseMode::Full => "block",
            BlockResponseMode::Lite => "block-lite",
        },
        match format {
            BlockResponseFormat::Json => "json",
            BlockResponseFormat::Protobuf => "bin",
        },
    )
}

fn parse_block_request(
    req: &Request,
    raw_slot: &str,
) -> std::result::Result<(u64, BlockResponseFormat, bool), Response> {
    let (slot_text, format) = parse_slot_param(raw_slot)?;
    let slot = slot_text.parse().map_err(|_| invalid_slot_response())?;
    let include_rewards = parse_include_rewards(req)?;

    Ok((slot, format, include_rewards))
}

fn parse_slot_param(raw_slot: &str) -> std::result::Result<(&str, BlockResponseFormat), Response> {
    match raw_slot.rsplit_once('.') {
        None => Ok((raw_slot, BlockResponseFormat::Json)),
        Some((slot, "json")) => Ok((slot, BlockResponseFormat::Json)),
        Some((slot, "bin")) => Ok((slot, BlockResponseFormat::Protobuf)),
        Some((_, "zstd")) => Err(unsupported_format_response(
            "zstd block responses are not supported; use .json or .bin",
        )),
        Some(_) => Err(unsupported_format_response(
            "unsupported block response extension; use .json or .bin",
        )),
    }
}

fn unsupported_format_response(message: &str) -> Response {
    json_error(400, "unsupported_format", message)
        .unwrap_or_else(|_| Response::error("Unsupported format", 400).unwrap())
}

fn parse_include_rewards(req: &Request) -> std::result::Result<bool, Response> {
    let url = req
        .url()
        .map_err(|_| Response::error("Invalid request URL", 400).unwrap())?;

    match url.query_pairs().find(|(key, _)| key == "rewards") {
        None => Ok(true),
        Some((_, value)) if value == "true" => Ok(true),
        Some((_, value)) if value == "false" => Ok(false),
        Some(_) => Err(
            json_error(400, "invalid_rewards", "rewards must be true or false")
                .unwrap_or_else(|_| Response::error("Invalid rewards flag", 400).unwrap()),
        ),
    }
}

fn invalid_slot_response() -> Response {
    json_error(400, "invalid_slot", "slot must be an unsigned integer")
        .unwrap_or_else(|_| Response::error("Invalid slot", 400).unwrap())
}
