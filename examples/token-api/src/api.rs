use crate::{
    format::{NO_ID, amount_to_ui},
    store::{HistoryPoint, OhlcvPoint, PricePoint, TokenOverview, TokenStore, parse_interval},
};
use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::info;

#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub index_dir: PathBuf,
    pub listen: SocketAddr,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "success": false,
                "message": self.message,
            })),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

pub async fn serve(config: ServeConfig) -> Result<()> {
    let store = Arc::new(TokenStore::load(&config.index_dir)?);
    let app = Router::new()
        .route("/", get(frontend_index))
        .route("/favicon.ico", get(favicon))
        .route("/app.js", get(frontend_js))
        .route("/styles.css", get(frontend_css))
        .route("/healthz", get(healthz))
        .route("/defi/networks", get(networks))
        .route("/defi/price", get(price))
        .route(
            "/defi/multi_price",
            get(multi_price_get).post(multi_price_post),
        )
        .route("/defi/history_price", get(history_price))
        .route("/defi/historical_price_unix", get(history_price))
        .route("/defi/ohlcv", get(ohlcv))
        .route("/defi/v3/ohlcv", get(ohlcv))
        .route("/defi/token_overview", get(token_overview))
        .route("/defi/v3/token/meta-data/single", get(token_overview))
        .route("/defi/v3/token/market-data", get(token_overview))
        .route("/defi/tokenlist", get(token_list))
        .route("/defi/txs/token", get(token_txs))
        .route("/defi/v3/token/txs", get(token_txs))
        .with_state(store);

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    info!("blockzilla token api listening on http://{}", config.listen);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn frontend_index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn frontend_js() -> Response {
    static_response(
        "application/javascript; charset=utf-8",
        include_str!("../static/app.js"),
    )
}

async fn frontend_css() -> Response {
    static_response(
        "text/css; charset=utf-8",
        include_str!("../static/styles.css"),
    )
}

async fn favicon() -> StatusCode {
    StatusCode::NO_CONTENT
}

fn static_response(content_type: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type)], body).into_response()
}

async fn healthz(State(store): State<Arc<TokenStore>>) -> Json<Value> {
    Json(json!({
        "ok": true,
        "tokens": store.token_count(),
        "latestUnixTime": store.latest_time(),
    }))
}

async fn networks() -> Json<Value> {
    Json(json!({
        "success": true,
        "data": ["solana"],
    }))
}

#[derive(Debug, Deserialize)]
struct AddressQuery {
    address: String,
}

#[derive(Debug, Deserialize)]
struct MultiPriceQuery {
    list_address: Option<String>,
    addresses: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MultiPriceBody {
    list_address: Option<String>,
    addresses: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    address: String,
    #[serde(rename = "type")]
    interval_type: Option<String>,
    time_from: Option<i64>,
    time_to: Option<i64>,
    fill: Option<String>,
    max_points: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct TradesQuery {
    address: String,
    offset: Option<usize>,
    limit: Option<usize>,
    sort_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillMode {
    None,
    Flat,
    Linear,
}

async fn price(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<AddressQuery>,
) -> ApiResult<Json<Value>> {
    let price = store
        .price_by_address(&query.address)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    Ok(Json(json!({
        "success": true,
        "data": price.map(price_json).unwrap_or(Value::Null),
    })))
}

async fn multi_price_get(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<MultiPriceQuery>,
) -> ApiResult<Json<Value>> {
    let addresses = split_addresses(query.list_address.or(query.addresses).unwrap_or_default());
    Ok(Json(multi_price_response(&store, addresses)?))
}

async fn multi_price_post(
    State(store): State<Arc<TokenStore>>,
    Json(body): Json<MultiPriceBody>,
) -> ApiResult<Json<Value>> {
    let addresses = if let Some(addresses) = body.addresses {
        addresses
    } else {
        split_addresses(body.list_address.unwrap_or_default())
    };
    Ok(Json(multi_price_response(&store, addresses)?))
}

async fn history_price(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<HistoryQuery>,
) -> ApiResult<Json<Value>> {
    let mint_id = store
        .token_id_for_address(&query.address)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let interval = parse_interval(query.interval_type.as_deref());
    let fill = parse_fill(query.fill.as_deref());
    let mut points = store.history(mint_id, query.time_from, query.time_to, interval);
    if fill != FillMode::None {
        points = fill_history_points(
            points,
            query.time_from,
            query.time_to,
            interval,
            fill,
            query.max_points.unwrap_or(1_200).clamp(1, 20_000),
        );
    }
    Ok(Json(json!({
        "success": true,
        "data": {
            "fill": fill.as_str(),
            "items": points.into_iter().map(|point| {
                json!({
                    "unixTime": point.unix_time,
                    "value": point.value,
                })
            }).collect::<Vec<_>>()
        }
    })))
}

async fn ohlcv(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<HistoryQuery>,
) -> ApiResult<Json<Value>> {
    let mint_id = store
        .token_id_for_address(&query.address)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let interval = parse_interval(query.interval_type.as_deref());
    let fill = parse_fill(query.fill.as_deref());
    let mut points = store.ohlcv(mint_id, query.time_from, query.time_to, interval);
    if fill != FillMode::None {
        points = fill_ohlcv_points(
            points,
            query.time_from,
            query.time_to,
            interval,
            fill,
            query.max_points.unwrap_or(1_200).clamp(1, 20_000),
        );
    }
    Ok(Json(json!({
        "success": true,
        "data": {
            "fill": fill.as_str(),
            "items": points.into_iter().map(ohlcv_json).collect::<Vec<_>>()
        }
    })))
}

async fn token_overview(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<AddressQuery>,
) -> ApiResult<Json<Value>> {
    let mint_id = store
        .token_id_for_address(&query.address)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let Some(overview) = store.token_overview(mint_id) else {
        return Err(ApiError::bad_request(
            "token was not observed in this index",
        ));
    };
    Ok(Json(json!({
        "success": true,
        "data": overview_json(&store, overview),
    })))
}

async fn token_list(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<ListQuery>,
) -> Json<Value> {
    let offset = query.offset.unwrap_or_default();
    let limit = query.limit.unwrap_or(50).min(1_000);
    let rows = store.token_list(offset, limit);
    let items = rows
        .into_iter()
        .map(|row| overview_json(&store, row))
        .collect::<Vec<_>>();
    let tokens = items.clone();
    Json(json!({
        "success": true,
        "data": {
            "total": store.token_count(),
            "items": items,
            "tokens": tokens,
        }
    }))
}

async fn token_txs(
    State(store): State<Arc<TokenStore>>,
    Query(query): Query<TradesQuery>,
) -> ApiResult<Json<Value>> {
    let mint_id = store
        .token_id_for_address(&query.address)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let offset = query.offset.unwrap_or_default();
    let limit = query.limit.unwrap_or(50).min(1_000);
    let descending = !matches!(query.sort_type.as_deref(), Some("asc"));
    let rows = store.trades(mint_id, offset, limit, descending);
    Ok(Json(json!({
        "success": true,
        "data": {
            "items": rows.into_iter().map(|swap| {
                let side = if swap.in_mint_id == mint_id { "sell" } else { "buy" };
                let price = store.swap_price_for_mint(swap, mint_id);
                json!({
                    "blockUnixTime": swap.block_time,
                    "blockHumanTime": human_time(swap.block_time),
                    "slot": swap.slot,
                    "txIndex": swap.tx_index,
                    "signatureId": null_if_zero(swap.signature_id),
                    "owner": store.id_or_zero_address(swap.owner_id),
                    "side": side,
                    "source": "blockzilla",
                    "base": {
                        "address": store.id_or_zero_address(swap.in_mint_id),
                        "uiAmount": amount_to_ui(swap.amount_in, swap.in_decimals),
                        "amount": swap.amount_in.to_string(),
                    },
                    "quote": {
                        "address": store.id_or_zero_address(swap.out_mint_id),
                        "uiAmount": amount_to_ui(swap.amount_out, swap.out_decimals),
                        "amount": swap.amount_out.to_string(),
                    },
                    "price": price.map(|point| point.value),
                    "volumeUSD": store.swap_volume_usd(swap, mint_id).unwrap_or_default(),
                    "dexProgram": store.id_or_zero_address(swap.dex_program_id),
                })
            }).collect::<Vec<_>>()
        }
    })))
}

fn multi_price_response(store: &TokenStore, addresses: Vec<String>) -> ApiResult<Value> {
    let mut data = serde_json::Map::new();
    for address in addresses {
        let price = store
            .price_by_address(&address)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        data.insert(address, price.map(price_json).unwrap_or(Value::Null));
    }
    Ok(json!({
        "success": true,
        "data": data,
    }))
}

fn overview_json(store: &TokenStore, overview: TokenOverview) -> Value {
    let price = overview.price;
    json!({
        "address": overview.address,
        "decimals": overview.token.decimals,
        "symbol": null_string(),
        "name": null_string(),
        "price": price.map(|point| point.value),
        "priceChange24hPercent": price.map(|point| point.price_change_24h).unwrap_or_default(),
        "v24hUSD": overview.volume_24h_usd,
        "volume24hUSD": overview.volume_24h_usd,
        "trade24h": overview.trades_24h,
        "holder": overview.token.token_account_count,
        "extensions": {
            "source": "blockzilla",
            "mintId": overview.token.mint_id,
            "program": store.id_or_zero_address(overview.token.program_id),
            "firstSlot": overview.token.first_slot,
            "lastSlot": overview.token.last_slot,
            "firstBlockUnixTime": overview.token.first_block_time,
            "lastBlockUnixTime": overview.token.last_block_time,
            "balanceChangeRows": overview.token.balance_change_count,
            "swapRows": overview.token.swap_count,
        }
    })
}

fn price_json(price: PricePoint) -> Value {
    json!({
        "value": price.value,
        "updateUnixTime": price.update_unix_time,
        "updateHumanTime": human_time(price.update_unix_time),
        "priceChange24h": price.price_change_24h,
    })
}

fn ohlcv_json(point: OhlcvPoint) -> Value {
    json!({
        "unixTime": point.unix_time,
        "o": point.open,
        "h": point.high,
        "l": point.low,
        "c": point.close,
        "v": point.volume_usd,
        "volume": point.volume_usd,
        "trades": point.trades,
    })
}

fn split_addresses(value: String) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

impl FillMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Flat => "flat",
            Self::Linear => "linear",
        }
    }
}

fn parse_fill(value: Option<&str>) -> FillMode {
    match value.unwrap_or("none") {
        "flat" | "previous" | "carry" => FillMode::Flat,
        "linear" | "slerp" | "splerp" => FillMode::Linear,
        _ => FillMode::None,
    }
}

fn fill_history_points(
    mut points: Vec<HistoryPoint>,
    time_from: Option<i64>,
    time_to: Option<i64>,
    interval: i64,
    fill: FillMode,
    max_points: usize,
) -> Vec<HistoryPoint> {
    if points.is_empty() || interval <= 0 {
        return points;
    }
    points.sort_by_key(|point| point.unix_time);
    points.dedup_by_key(|point| point.unix_time);

    let start = bucket_floor(time_from.unwrap_or(points[0].unix_time), interval);
    let end = bucket_floor(
        time_to.unwrap_or_else(|| points.last().map(|point| point.unix_time).unwrap_or(start)),
        interval,
    );
    if end < start {
        return Vec::new();
    }

    let stride = fill_stride(start, end, interval, max_points);
    let mut out = Vec::new();
    let mut cursor = 0usize;
    let mut previous = None;
    let mut t = start;
    while t <= end {
        while cursor < points.len() && points[cursor].unix_time <= t {
            previous = Some(points[cursor]);
            cursor += 1;
        }
        let next = points.get(cursor).copied();
        if let Some(value) = filled_value(t, previous, next, fill) {
            out.push(HistoryPoint {
                unix_time: t,
                value,
            });
        }
        match t.checked_add(stride) {
            Some(next_t) if next_t > t => t = next_t,
            _ => break,
        }
    }
    out
}

fn fill_ohlcv_points(
    points: Vec<OhlcvPoint>,
    time_from: Option<i64>,
    time_to: Option<i64>,
    interval: i64,
    fill: FillMode,
    max_points: usize,
) -> Vec<OhlcvPoint> {
    let real = points
        .iter()
        .copied()
        .map(|point| (point.unix_time, point))
        .collect::<std::collections::BTreeMap<_, _>>();
    let closes = points
        .into_iter()
        .map(|point| HistoryPoint {
            unix_time: point.unix_time,
            value: point.close,
        })
        .collect::<Vec<_>>();
    fill_history_points(closes, time_from, time_to, interval, fill, max_points)
        .into_iter()
        .map(|point| {
            real.get(&point.unix_time).copied().unwrap_or(OhlcvPoint {
                unix_time: point.unix_time,
                open: point.value,
                high: point.value,
                low: point.value,
                close: point.value,
                volume_usd: 0.0,
                trades: 0,
            })
        })
        .collect()
}

fn filled_value(
    unix_time: i64,
    previous: Option<HistoryPoint>,
    next: Option<HistoryPoint>,
    fill: FillMode,
) -> Option<f64> {
    match fill {
        FillMode::None => None,
        FillMode::Flat => previous.or(next).map(|point| point.value),
        FillMode::Linear => {
            if let Some(previous) = previous {
                if previous.unix_time == unix_time {
                    return Some(previous.value);
                }
                if let Some(next) = next
                    && next.unix_time > previous.unix_time
                {
                    let span = (next.unix_time - previous.unix_time) as f64;
                    let offset = (unix_time - previous.unix_time) as f64;
                    let ratio = (offset / span).clamp(0.0, 1.0);
                    return Some(previous.value + ((next.value - previous.value) * ratio));
                }
                return Some(previous.value);
            }
            next.map(|point| point.value)
        }
    }
}

fn fill_stride(start: i64, end: i64, interval: i64, max_points: usize) -> i64 {
    let buckets = ((end - start) / interval).saturating_add(1);
    let max_points = i64::try_from(max_points).unwrap_or(i64::MAX).max(1);
    let bucket_stride = ((buckets + max_points - 1) / max_points).max(1);
    interval.saturating_mul(bucket_stride).max(interval)
}

fn bucket_floor(unix_time: i64, interval_seconds: i64) -> i64 {
    if interval_seconds <= 1 {
        unix_time
    } else {
        unix_time - unix_time.rem_euclid(interval_seconds)
    }
}

fn human_time(unix_time: i64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_time)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .unwrap_or_else(|| unix_time.to_string())
}

fn null_if_zero(value: u32) -> Value {
    if value == NO_ID {
        Value::Null
    } else {
        json!(value)
    }
}

fn null_string() -> Value {
    Value::Null
}
