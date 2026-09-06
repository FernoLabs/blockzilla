use std::{
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use crate::{
    index_format::TransactionCoordinate,
    market_format::MarketScaledUiHistory,
    market_store::{
        Candle, MARKET_TRADER_ATTRIBUTION, MAX_MARKET_CANDLES, MAX_MARKET_PROGRAM_VOLUME_POINTS,
        MAX_MARKET_SLOT_CANDLES, MAX_MARKET_TRADE_PAGE_ROWS, MAX_MARKET_TRADER_ACTIVITY_POINTS,
        MarketHealth, MarketMint, MarketOhlcvQuery, MarketPair, MarketProgramSummary,
        MarketProgramVolumeQuery, MarketProgramVolumeSeries, MarketProvenance, MarketSlotCandle,
        MarketSlotOhlcvQuery, MarketStore, MarketSummary, MarketTradePage, MarketTradeQuery,
        MarketTradeView, MarketTraderActivityQuery, MarketTraderActivitySeries,
        MarketTraderActivitySummary, RegistryKeyView,
    },
    mint_metadata::{
        DisplayMetadataSource, MintAccountStatus, MintMetadataHealth, MintMetadataRecord,
        MintMetadataStore, TokenProgramKind,
    },
    postings_format::ProgramInstructionScope,
    postings_store::{
        MAX_OWNER_BALANCE_HISTORY_ROWS, MAX_POSTINGS_PAGE_ROWS, OwnerBalanceHistoryRangeQuery,
        OwnerBalanceHistorySeries, OwnerPostingsStore, PostingLookupKind, PostingsStore,
    },
    store::{PostingTransactionDetail, QueryStore, TransactionDetail},
};

const MAX_RETAINED_SCRATCH_BYTES: usize = 16 << 20;
const DEFAULT_POSTINGS_PAGE_ROWS: usize = 100;
const DEFAULT_OWNER_BALANCE_HISTORY_POINTS: usize = 1_000;
const POSTINGS_CURSOR_BYTES: usize = 80;
const POSTINGS_CURSOR_VERSION: u8 = 1;
const POSTINGS_CURSOR_DOMAIN: &[u8] = b"blockzilla-spyx-postings-cursor-v1\0";
const DEFAULT_MARKET_TRADE_PAGE_ROWS: usize = 100;
const DEFAULT_MARKET_MAX_POINTS: usize = 5_000;

struct OfficialMintDisplay {
    mint: &'static str,
    name: &'static str,
    symbol: &'static str,
    source_uri: &'static str,
}

const OFFICIAL_MINT_DISPLAYS: &[OfficialMintDisplay] = &[OfficialMintDisplay {
    mint: "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
    name: "BlazeStake Staked SOL",
    symbol: "bSOL",
    source_uri: "https://stake-docs.solblaze.org/developers/addresses",
}];

#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub bind: SocketAddr,
    pub cors_origin: String,
    pub max_blocking_reads: usize,
    pub static_dir: Option<PathBuf>,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 8787)),
            cors_origin: "*".to_owned(),
            max_blocking_reads: 4,
            static_dir: None,
        }
    }
}

#[derive(Clone)]
struct ApiState {
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    owner_postings: Option<Arc<OwnerPostingsStore>>,
    market: Option<Arc<MarketStore>>,
    mint_metadata: Option<Arc<MintMetadataStore>>,
    read_permits: Arc<Semaphore>,
    scratch_pool: Arc<ScratchPool>,
}

struct ScratchPool {
    buffers: Mutex<Vec<Vec<u8>>>,
    max_buffers: usize,
    max_retained_capacity: usize,
}

impl ScratchPool {
    fn new(max_buffers: usize, max_retained_capacity: usize) -> Self {
        let mut buffers = Vec::with_capacity(max_buffers);
        buffers.resize_with(max_buffers, Vec::new);
        Self {
            buffers: Mutex::new(buffers),
            max_buffers,
            max_retained_capacity,
        }
    }

    fn checkout(self: &Arc<Self>) -> ScratchLease {
        let buffer = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_default();
        ScratchLease {
            pool: Arc::clone(self),
            buffer: Some(buffer),
        }
    }

    fn return_buffer(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() > self.max_retained_capacity {
            buffer = Vec::new();
        } else {
            buffer.clear();
        }
        let mut buffers = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buffers.len() < self.max_buffers {
            buffers.push(buffer);
        }
    }
}

struct ScratchLease {
    pool: Arc<ScratchPool>,
    buffer: Option<Vec<u8>>,
}

impl ScratchLease {
    fn buffer(&mut self) -> &mut Vec<u8> {
        self.buffer.as_mut().expect("scratch lease owns one buffer")
    }
}

impl Drop for ScratchLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_buffer(buffer);
        }
    }
}

pub fn router(
    store: Arc<QueryStore>,
    cors_origin: &str,
    max_blocking_reads: usize,
) -> Result<Router> {
    router_with_postings(store, None, cors_origin, max_blocking_reads)
}

pub fn router_with_postings(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    cors_origin: &str,
    max_blocking_reads: usize,
) -> Result<Router> {
    router_with_indexes(store, postings, None, cors_origin, max_blocking_reads)
}

pub fn router_with_indexes(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    market: Option<Arc<MarketStore>>,
    cors_origin: &str,
    max_blocking_reads: usize,
) -> Result<Router> {
    router_with_metadata(
        store,
        postings,
        market,
        None,
        cors_origin,
        max_blocking_reads,
    )
}

pub fn router_with_metadata(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    market: Option<Arc<MarketStore>>,
    mint_metadata: Option<Arc<MintMetadataStore>>,
    cors_origin: &str,
    max_blocking_reads: usize,
) -> Result<Router> {
    router_with_all_indexes(
        store,
        postings,
        None,
        market,
        mint_metadata,
        cors_origin,
        max_blocking_reads,
    )
}

pub fn router_with_all_indexes(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    owner_postings: Option<Arc<OwnerPostingsStore>>,
    market: Option<Arc<MarketStore>>,
    mint_metadata: Option<Arc<MintMetadataStore>>,
    cors_origin: &str,
    max_blocking_reads: usize,
) -> Result<Router> {
    ensure!(
        max_blocking_reads != 0,
        "--max-blocking-reads must be positive"
    );
    if let Some(postings) = &postings {
        ensure!(
            postings.transaction_count() == store.transaction_count()
                && postings.source_transaction_sha256() == store.source_transaction_sha256(),
            "postings and transaction indexes cover different source data"
        );
    }
    if let Some(owner_postings) = &owner_postings {
        ensure!(
            owner_postings.transaction_count() == store.transaction_count()
                && owner_postings.source_transaction_sha256() == store.source_transaction_sha256(),
            "owner postings and transaction indexes cover different source data"
        );
    }
    if let Some(market) = &market {
        let health = market.health();
        ensure!(
            market.source_transaction_sha256() == store.source_transaction_sha256()
                && health.source_transactions_scanned <= store.transaction_count(),
            "market and transaction indexes cover different source data"
        );
    }
    ensure!(
        mint_metadata.is_none() || market.is_some(),
        "mint metadata requires the market index"
    );
    if let Some(metadata) = &mint_metadata {
        metadata.verify_identity()?;
    }
    let cors = if cors_origin == "*" {
        CorsLayer::new()
            .allow_methods([Method::GET, Method::HEAD])
            .allow_origin(tower_http::cors::Any)
    } else {
        let origin = HeaderValue::from_str(cors_origin)
            .with_context(|| format!("invalid CORS origin {cors_origin:?}"))?;
        CorsLayer::new()
            .allow_methods([Method::GET, Method::HEAD])
            .allow_origin(origin)
    };
    let state = ApiState {
        store,
        postings,
        owner_postings,
        market,
        mint_metadata,
        read_permits: Arc::new(Semaphore::new(max_blocking_reads)),
        scratch_pool: Arc::new(ScratchPool::new(
            max_blocking_reads,
            MAX_RETAINED_SCRATCH_BYTES,
        )),
    };
    Ok(Router::new()
        .route("/healthz", get(health))
        .route(
            "/api/v1/transactions/by-signature/{signature}",
            get(by_signature),
        )
        .route("/api/v1/transactions/by-coordinate", get(by_coordinate))
        .route("/api/v1/transactions/{id}", get(by_id))
        .route(
            "/api/v1/postings/token-account/{key}",
            get(postings_token_account),
        )
        .route(
            "/api/v1/postings/target-address/{key}",
            get(postings_target_address),
        )
        .route("/api/v1/postings/owner/{key}", get(postings_owner))
        .route("/api/v1/postings/program/{key}", get(postings_program))
        .route("/api/v1/market/provenance", get(market_provenance))
        .route(
            "/api/v1/market/scaled-ui-amount",
            get(market_scaled_ui_amount),
        )
        .route("/api/v1/market/summary", get(market_summary))
        .route("/api/v1/market/pairs", get(market_pairs))
        .route("/api/v1/market/mints", get(market_mints))
        .route("/api/v1/market/mints/{address}", get(market_mint))
        .route("/api/v1/market/programs", get(market_programs))
        .route("/api/v1/market/trades", get(market_trades))
        .route("/api/v1/market/trades/{id}", get(market_trade))
        .route("/api/v1/market/candles", get(market_candles))
        .route("/api/v1/market/slot-candles", get(market_slot_candles))
        .route("/api/v1/market/program-volume", get(market_program_volume))
        .route(
            "/api/v1/accounts/{address}/trading-summary",
            get(account_trading_summary),
        )
        .route(
            "/api/v1/accounts/{address}/trades",
            get(account_proven_trades),
        )
        .route(
            "/api/v1/accounts/{address}/trading-activity",
            get(account_trading_activity),
        )
        .route(
            "/api/v1/accounts/{address}/balance-history",
            get(account_balance_history),
        )
        .with_state(state)
        .layer(cors)
        .layer(TraceLayer::new_for_http()))
}

pub async fn serve(store: Arc<QueryStore>, config: ServeConfig) -> Result<()> {
    serve_with_postings(store, None, config).await
}

pub async fn serve_with_postings(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    config: ServeConfig,
) -> Result<()> {
    serve_with_indexes(store, postings, None, config).await
}

pub async fn serve_with_indexes(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    market: Option<Arc<MarketStore>>,
    config: ServeConfig,
) -> Result<()> {
    serve_with_metadata(store, postings, market, None, config).await
}

pub async fn serve_with_metadata(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    market: Option<Arc<MarketStore>>,
    mint_metadata: Option<Arc<MintMetadataStore>>,
    config: ServeConfig,
) -> Result<()> {
    serve_with_all_indexes(store, postings, None, market, mint_metadata, config).await
}

pub async fn serve_with_all_indexes(
    store: Arc<QueryStore>,
    postings: Option<Arc<PostingsStore>>,
    owner_postings: Option<Arc<OwnerPostingsStore>>,
    market: Option<Arc<MarketStore>>,
    mint_metadata: Option<Arc<MintMetadataStore>>,
    config: ServeConfig,
) -> Result<()> {
    let mut app = router_with_all_indexes(
        store,
        postings,
        owner_postings,
        market,
        mint_metadata,
        &config.cors_origin,
        config.max_blocking_reads,
    )?;
    if let Some(static_dir) = config.static_dir.as_deref() {
        app = with_static_site(app, static_dir)?;
    }
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind SPYx query service to {}", config.bind))?;
    tracing::info!(bind = %config.bind, "SPYx query service is ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve SPYx query API")
}

fn with_static_site(app: Router, static_dir: &FsPath) -> Result<Router> {
    ensure!(
        static_dir.is_dir(),
        "static site directory does not exist: {}",
        static_dir.display()
    );
    let static_dir = std::fs::canonicalize(static_dir).with_context(|| {
        format!(
            "canonicalize static site directory {}",
            static_dir.display()
        )
    })?;
    let index = static_dir.join("index.html");
    let fallback = static_dir.join("200.html");
    let app_assets = static_dir.join("_app");
    let data = static_dir.join("data");
    ensure!(
        index.is_file(),
        "static site index does not exist: {}",
        index.display()
    );
    ensure!(
        fallback.is_file(),
        "static site fallback does not exist: {}",
        fallback.display()
    );
    ensure!(
        app_assets.is_dir(),
        "static site asset directory does not exist: {}",
        app_assets.display()
    );
    ensure!(
        data.is_dir(),
        "static site data directory does not exist: {}",
        data.display()
    );
    Ok(app
        .route("/api", any(static_route_not_found))
        .route("/api/{*path}", any(static_route_not_found))
        .route("/healthz/{*path}", any(static_route_not_found))
        .nest_service("/_app", ServeDir::new(app_assets))
        .nest_service("/data", ServeDir::new(data))
        .fallback_service(ServeDir::new(static_dir).fallback(ServeFile::new(fallback))))
}

async fn static_route_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    index: HealthIndex,
    postings: HealthPostings,
    #[serde(skip_serializing_if = "Option::is_none")]
    market: Option<MarketHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mint_metadata: Option<MintMetadataHealth>,
}

#[derive(Serialize)]
struct HealthIndex {
    complete: bool,
    transactions: u64,
    source_transaction_sha256: String,
}

#[derive(Serialize)]
struct HealthPostings {
    available: bool,
    complete: bool,
    target_address: bool,
    token_account: bool,
    program: bool,
    owner: bool,
    owner_balance_history: bool,
    target_address_keys: u64,
    target_address_postings: u64,
    program_keys: u64,
    program_postings: u64,
    owner_keys: u64,
    owner_postings: u64,
    owner_balance_history_keys: u64,
    owner_balance_history_events: u64,
}

async fn health(State(state): State<ApiState>) -> Json<HealthResponse> {
    let complete = state.store.complete();
    let postings = state.postings.as_deref();
    let owner_postings = state.owner_postings.as_deref();
    let any_postings = postings.is_some() || owner_postings.is_some();
    let postings_complete = any_postings
        && postings.is_none_or(PostingsStore::complete)
        && owner_postings.is_none_or(OwnerPostingsStore::complete);
    Json(HealthResponse {
        status: "ok",
        index: HealthIndex {
            complete,
            transactions: state.store.transaction_count(),
            source_transaction_sha256: state.store.source_transaction_sha256().to_owned(),
        },
        postings: HealthPostings {
            available: any_postings,
            complete: postings_complete,
            target_address: postings.is_some(),
            token_account: postings.is_some(),
            program: postings.is_some(),
            owner: owner_postings.is_some(),
            owner_balance_history: owner_postings
                .is_some_and(OwnerPostingsStore::has_balance_history),
            target_address_keys: postings.map_or(0, PostingsStore::target_address_key_count),
            target_address_postings: postings
                .map_or(0, PostingsStore::target_address_posting_count),
            program_keys: postings.map_or(0, PostingsStore::program_key_count),
            program_postings: postings.map_or(0, PostingsStore::program_posting_count),
            owner_keys: owner_postings.map_or(0, OwnerPostingsStore::owner_key_count),
            owner_postings: owner_postings.map_or(0, OwnerPostingsStore::owner_posting_count),
            owner_balance_history_keys: owner_postings
                .map_or(0, OwnerPostingsStore::balance_history_owner_key_count),
            owner_balance_history_events: owner_postings
                .map_or(0, OwnerPostingsStore::balance_history_event_count),
        },
        market: state.market.as_deref().map(MarketStore::health),
        mint_metadata: state
            .mint_metadata
            .as_deref()
            .map(MintMetadataStore::health),
    })
}

#[derive(Debug, Default, Deserialize)]
struct MarketSummaryQuery {
    quote_mint: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MarketTradesHttpQuery {
    quote_mint: Option<String>,
    #[serde(alias = "venue")]
    program: Option<String>,
    offset: Option<u64>,
    limit: Option<usize>,
    #[serde(alias = "from")]
    time_from: Option<i64>,
    #[serde(alias = "to")]
    time_to: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MarketCandlesHttpQuery {
    quote_mint: String,
    #[serde(alias = "venue")]
    program: Option<String>,
    #[serde(default = "default_market_interval")]
    interval: String,
    #[serde(alias = "from")]
    time_from: Option<i64>,
    #[serde(alias = "to")]
    time_to: Option<i64>,
    #[serde(default = "default_market_max_points")]
    max_points: usize,
}

#[derive(Debug, Deserialize)]
struct MarketSlotCandlesHttpQuery {
    quote_mint: String,
    #[serde(alias = "venue")]
    program: Option<String>,
    slot_from: Option<u64>,
    slot_to: Option<u64>,
    #[serde(default = "default_market_max_points")]
    max_points: usize,
}

#[derive(Debug, Deserialize)]
struct MarketProgramVolumeHttpQuery {
    quote_mint: Option<String>,
    #[serde(default = "default_market_interval")]
    interval: String,
    #[serde(alias = "from")]
    time_from: Option<i64>,
    #[serde(alias = "to")]
    time_to: Option<i64>,
    #[serde(default = "default_market_max_points")]
    max_points: usize,
}

#[derive(Debug, Default, Deserialize)]
struct AccountProvenTradesHttpQuery {
    quote_mint: Option<String>,
    #[serde(alias = "venue")]
    program: Option<String>,
    offset: Option<u64>,
    limit: Option<usize>,
    #[serde(alias = "from")]
    time_from: Option<i64>,
    #[serde(alias = "to")]
    time_to: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AccountTradingActivityHttpQuery {
    quote_mint: Option<String>,
    #[serde(alias = "venue")]
    program: Option<String>,
    #[serde(default = "default_market_interval")]
    interval: String,
    #[serde(alias = "from")]
    time_from: Option<i64>,
    #[serde(alias = "to")]
    time_to: Option<i64>,
    #[serde(default = "default_market_max_points")]
    max_points: usize,
}

#[derive(Debug, Deserialize)]
struct AccountBalanceHistoryHttpQuery {
    transaction_id_from: Option<u64>,
    transaction_id_to: Option<u64>,
    #[serde(default = "default_owner_balance_history_points")]
    max_points: usize,
}

#[derive(Serialize)]
struct AccountBalanceHistoryResponse {
    supported: bool,
    artifact_complete: bool,
    address: String,
    attribution: &'static str,
    registry_id: u32,
    matching_events: u64,
    sampled: bool,
    items: Vec<crate::owner_balance_history_format::OwnerBalanceEventRecord>,
}

/// The account is included only through exact `trader_id` attribution from a
/// proven DEX trade. No transaction, trade, or protocol position is inferred.
#[derive(Serialize)]
struct AccountProvenTradesResponse {
    supported: bool,
    artifact_complete: bool,
    has_matching_proven_trades: bool,
    attribution: &'static str,
    includes_inferred_trades: bool,
    includes_protocol_positions: bool,
    trader: RegistryKeyView,
    #[serde(flatten)]
    page: MarketTradePage,
}

#[derive(Serialize)]
struct MarketPairsEnvelope {
    items: Vec<MarketPair>,
}

#[derive(Serialize)]
struct MarketMintsEnvelope {
    metadata_available: bool,
    items: Vec<MarketMintView>,
}

#[derive(Serialize)]
struct MarketProgramsEnvelope {
    items: Vec<MarketProgramSummary>,
}

#[derive(Serialize)]
struct MarketMintView {
    mint: crate::market_store::RegistryKeyView,
    decimals: u8,
    is_target: bool,
    direct_usd_quote: bool,
    trade_count: u64,
    metadata_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    mint_account_status: Option<MintAccountStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_program: Option<TokenProgramKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc_decimals: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_source_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    warnings: Vec<String>,
}

#[derive(Serialize)]
struct MarketCandlesEnvelope {
    items: Vec<Candle>,
}

#[derive(Serialize)]
struct MarketSlotCandlesEnvelope {
    items: Vec<MarketSlotCandle>,
}

enum MarketAddressLookup<T> {
    Found(T),
    AddressMissing(&'static str),
    PairMissing,
}

enum MarketCandleLookup {
    Found(Vec<Candle>),
    AddressMissing(&'static str),
    PairMissing,
    LimitExceeded { points: i128, max_points: usize },
}

enum MarketSlotCandleLookup {
    Found(Vec<MarketSlotCandle>),
    AddressMissing(&'static str),
    PairMissing,
    ProgramNotDex,
    LimitExceeded { max_points: usize },
}

enum MarketProgramVolumeLookup {
    Found(MarketProgramVolumeSeries),
    AddressMissing(&'static str),
}

enum MarketTradesLookup {
    Found(MarketTradePage),
    AddressMissing(&'static str),
    OffsetExceeded,
}

enum AccountSummaryLookup {
    Found(MarketTraderActivitySummary),
    AddressMissing(&'static str),
}

enum AccountTradesLookup {
    Found(AccountProvenTradesResponse),
    AddressMissing(&'static str),
    ProgramNotDex,
    OffsetExceeded,
}

enum AccountActivityLookup {
    Found(MarketTraderActivitySeries),
    AddressMissing(&'static str),
    ProgramNotDex,
    LimitExceeded { points: i128, max_points: usize },
}

enum AccountBalanceHistoryLookup {
    Found(OwnerBalanceHistorySeries),
    Missing,
}

async fn market_provenance(
    State(state): State<ApiState>,
) -> Result<Json<MarketProvenance>, ApiError> {
    market_blocking(state, |market| Ok(market.provenance()))
        .await
        .map(Json)
}

async fn market_scaled_ui_amount(
    State(state): State<ApiState>,
) -> Result<Json<MarketScaledUiHistory>, ApiError> {
    market_blocking(state, |market| Ok(market.scaled_ui_history()))
        .await
        .map(Json)
}

async fn market_summary(
    State(state): State<ApiState>,
    query: Result<Query<MarketSummaryQuery>, QueryRejection>,
) -> Result<Json<MarketSummary>, ApiError> {
    let Query(query) = market_query(query)?;
    let quote_mint = validate_optional_registry_address(query.quote_mint, "quote_mint")?;
    let result = market_blocking(state, move |market| {
        let Some(address) = quote_mint else {
            return market.market_overview(None).map(MarketAddressLookup::Found);
        };
        let Some(quote_mint_id) = market.registry_id_for_address(&address)? else {
            return Ok(MarketAddressLookup::AddressMissing("quote_mint"));
        };
        if market.pair_summary(quote_mint_id)?.is_none() {
            return Ok(MarketAddressLookup::PairMissing);
        }
        market
            .market_overview(Some(quote_mint_id))
            .map(MarketAddressLookup::Found)
    })
    .await?;
    match result {
        MarketAddressLookup::Found(summary) => Ok(Json(summary)),
        MarketAddressLookup::AddressMissing(field) => Err(market_address_not_found(field)),
        MarketAddressLookup::PairMissing => Err(ApiError::not_found(
            "market_pair_not_found",
            "the quote mint has no proven SPYx trades",
        )),
    }
}

async fn market_pairs(
    State(state): State<ApiState>,
) -> Result<Json<MarketPairsEnvelope>, ApiError> {
    let items = market_blocking(state, |market| market.pair_summaries()).await?;
    Ok(Json(MarketPairsEnvelope { items }))
}

async fn market_mints(
    State(state): State<ApiState>,
) -> Result<Json<MarketMintsEnvelope>, ApiError> {
    let metadata = state.mint_metadata.clone();
    let metadata_available = metadata.is_some();
    let items = market_blocking(state, move |market| {
        market
            .mint_summaries()?
            .into_iter()
            .map(|mint| {
                let record = metadata
                    .as_deref()
                    .and_then(|store| store.mint_by_address(&mint.mint.address));
                Ok(market_mint_view(mint, record))
            })
            .collect::<Result<Vec<_>>>()
    })
    .await?;
    Ok(Json(MarketMintsEnvelope {
        metadata_available,
        items,
    }))
}

async fn market_mint(
    State(state): State<ApiState>,
    Path(address): Path<String>,
) -> Result<Json<MarketMintView>, ApiError> {
    let address = validate_registry_address(address, "mint")?;
    let metadata = state.mint_metadata.clone();
    let result = market_blocking(state, move |market| {
        let mint = market
            .mint_summaries()?
            .into_iter()
            .find(|mint| mint.mint.address == address);
        Ok(mint.map(|mint| {
            let record = metadata
                .as_deref()
                .and_then(|store| store.mint_by_address(&mint.mint.address));
            market_mint_view(mint, record)
        }))
    })
    .await?;
    result.map(Json).ok_or_else(|| {
        ApiError::not_found(
            "market_mint_not_found",
            "the mint does not occur in a proven SPYx swap",
        )
    })
}

async fn market_programs(
    State(state): State<ApiState>,
) -> Result<Json<MarketProgramsEnvelope>, ApiError> {
    let items = market_blocking(state, |market| market.program_summaries()).await?;
    Ok(Json(MarketProgramsEnvelope { items }))
}

fn market_mint_view(mint: MarketMint, metadata: Option<&MintMetadataRecord>) -> MarketMintView {
    let display = metadata.and_then(|record| record.display.as_ref());
    let official = display
        .is_none()
        .then(|| official_mint_display(&mint.mint.address))
        .flatten();
    let mut warnings = metadata.map_or_else(Vec::new, |record| record.warnings.clone());
    if official.is_some() {
        warnings.push("official_project_display_fallback".to_owned());
    }
    MarketMintView {
        mint: mint.mint,
        decimals: mint.decimals,
        is_target: mint.is_target,
        direct_usd_quote: mint.direct_usd_quote,
        trade_count: mint.trade_count,
        metadata_available: metadata.is_some(),
        mint_account_status: metadata.map(|record| record.mint_account_status),
        token_program: metadata.and_then(|record| record.token_program),
        rpc_decimals: metadata.and_then(|record| record.rpc_decimals),
        metadata_source: display
            .map(|display| display_source_name(display.source).to_owned())
            .or_else(|| official.map(|_| "official_project_site".to_owned())),
        metadata_source_uri: official.map(|entry| entry.source_uri.to_owned()),
        name: display
            .and_then(|display| display.name.clone())
            .or_else(|| official.map(|entry| entry.name.to_owned())),
        symbol: display
            .and_then(|display| display.symbol.clone())
            .or_else(|| official.map(|entry| entry.symbol.to_owned())),
        uri: display.and_then(|display| display.uri.clone()),
        warnings,
    }
}

fn official_mint_display(address: &str) -> Option<&'static OfficialMintDisplay> {
    OFFICIAL_MINT_DISPLAYS
        .iter()
        .find(|entry| entry.mint == address)
}

const fn display_source_name(source: DisplayMetadataSource) -> &'static str {
    match source {
        DisplayMetadataSource::Token2022 => "token2022",
        DisplayMetadataSource::Metaplex => "metaplex",
    }
}

async fn market_trades(
    State(state): State<ApiState>,
    query: Result<Query<MarketTradesHttpQuery>, QueryRejection>,
) -> Result<Json<MarketTradePage>, ApiError> {
    let Query(query) = market_query(query)?;
    validate_market_time_range(query.time_from, query.time_to)?;
    let limit = query.limit.unwrap_or(DEFAULT_MARKET_TRADE_PAGE_ROWS);
    if !(1..=MAX_MARKET_TRADE_PAGE_ROWS).contains(&limit) {
        return Err(ApiError::bad_request(
            "invalid_market_trade_limit",
            format!("market trade limit must be from 1 through {MAX_MARKET_TRADE_PAGE_ROWS}"),
        ));
    }
    let quote_mint = validate_optional_registry_address(query.quote_mint, "quote_mint")?;
    let program = validate_optional_registry_address(query.program, "program")?;
    let result = market_blocking(state, move |market| {
        let quote_mint_id = match quote_mint {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) => Some(id),
                None => return Ok(MarketTradesLookup::AddressMissing("quote_mint")),
            },
            None => None,
        };
        let venue_program_id = match program {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) => Some(id),
                None => return Ok(MarketTradesLookup::AddressMissing("program")),
            },
            None => None,
        };
        match market.paged_trades(MarketTradeQuery {
            quote_mint_id,
            venue_program_id,
            time_from: query.time_from,
            time_to: query.time_to,
            offset: query.offset.unwrap_or(0),
            limit,
        }) {
            Ok(page) => Ok(MarketTradesLookup::Found(page)),
            Err(error) if error.to_string().contains("page offset exceeds") => {
                Ok(MarketTradesLookup::OffsetExceeded)
            }
            Err(error) => Err(error),
        }
    })
    .await?;
    match result {
        MarketTradesLookup::Found(page) => Ok(Json(page)),
        MarketTradesLookup::AddressMissing(field) => Err(market_address_not_found(field)),
        MarketTradesLookup::OffsetExceeded => Err(ApiError::bad_request(
            "invalid_market_trade_offset",
            "market trade offset exceeds the filtered result",
        )),
    }
}

async fn market_trade(
    State(state): State<ApiState>,
    Path(value): Path<String>,
) -> Result<Json<MarketTradeView>, ApiError> {
    let id = parse_unsigned_id(&value, "invalid_market_trade_id", "market trade ID")?;
    let trade = market_blocking(state, move |market| market.trade_by_ordinal(id)).await?;
    trade.map(Json).ok_or_else(|| {
        ApiError::not_found("market_trade_not_found", "market trade ID was not found")
    })
}

async fn market_candles(
    State(state): State<ApiState>,
    query: Result<Query<MarketCandlesHttpQuery>, QueryRejection>,
) -> Result<Json<MarketCandlesEnvelope>, ApiError> {
    let Query(query) = market_query(query)?;
    validate_market_time_range(query.time_from, query.time_to)?;
    let interval_seconds = parse_market_interval(&query.interval)?;
    if query.max_points == 0 || query.max_points > MAX_MARKET_CANDLES {
        return Err(ApiError::bad_request(
            "invalid_market_max_points",
            format!("max_points must be from 1 through {MAX_MARKET_CANDLES}"),
        ));
    }
    let quote_mint = validate_registry_address(query.quote_mint, "quote_mint")?;
    let program = validate_optional_registry_address(query.program, "program")?;
    let max_points = query.max_points;
    let result = market_blocking(state, move |market| {
        let Some(quote_mint_id) = market.registry_id_for_address(&quote_mint)? else {
            return Ok(MarketCandleLookup::AddressMissing("quote_mint"));
        };
        let venue_program_id = match program {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) => Some(id),
                None => return Ok(MarketCandleLookup::AddressMissing("program")),
            },
            None => None,
        };
        let Some(pair) = market.pair_summary(quote_mint_id)? else {
            return Ok(MarketCandleLookup::PairMissing);
        };
        let effective_to = query.time_to.unwrap_or(pair.last_block_time);
        let effective_from = query.time_from.unwrap_or_else(|| {
            pair.first_block_time.max(rolling_candle_start(
                effective_to,
                interval_seconds,
                max_points,
            ))
        });
        if effective_from > effective_to {
            return Ok(MarketCandleLookup::Found(Vec::new()));
        }
        let points = candle_window_points(effective_from, effective_to, interval_seconds);
        if points > max_points as i128 {
            return Ok(MarketCandleLookup::LimitExceeded { points, max_points });
        }
        market
            .ohlcv(MarketOhlcvQuery {
                quote_mint_id,
                interval_seconds,
                time_from: Some(effective_from),
                time_to: Some(effective_to),
                venue_program_id,
            })
            .map(MarketCandleLookup::Found)
    })
    .await?;
    match result {
        MarketCandleLookup::Found(items) => Ok(Json(MarketCandlesEnvelope { items })),
        MarketCandleLookup::AddressMissing(field) => Err(market_address_not_found(field)),
        MarketCandleLookup::PairMissing => Ok(Json(MarketCandlesEnvelope { items: Vec::new() })),
        MarketCandleLookup::LimitExceeded { points, max_points } => {
            Err(ApiError::bad_request_details(
                "market_candle_limit_exceeded",
                "the candle time window exceeds max_points",
                json!({ "points": points.to_string(), "max_points": max_points }),
            ))
        }
    }
}

async fn market_slot_candles(
    State(state): State<ApiState>,
    query: Result<Query<MarketSlotCandlesHttpQuery>, QueryRejection>,
) -> Result<Json<MarketSlotCandlesEnvelope>, ApiError> {
    let Query(query) = market_query(query)?;
    validate_market_slot_range(query.slot_from, query.slot_to)?;
    if query.max_points == 0 || query.max_points > MAX_MARKET_SLOT_CANDLES {
        return Err(ApiError::bad_request(
            "invalid_market_max_points",
            format!("max_points must be from 1 through {MAX_MARKET_SLOT_CANDLES}"),
        ));
    }
    let quote_mint = validate_registry_address(query.quote_mint, "quote_mint")?;
    let program = validate_optional_registry_address(query.program, "program")?;
    let max_points = query.max_points;
    let result = market_blocking(state, move |market| {
        let Some(quote_mint_id) = market.registry_id_for_address(&quote_mint)? else {
            return Ok(MarketSlotCandleLookup::AddressMissing("quote_mint"));
        };
        if market.pair_summary(quote_mint_id)?.is_none() {
            return Ok(MarketSlotCandleLookup::PairMissing);
        }
        let dex_program_id = match program {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) if market.is_executed_dex_program(id) => Some(id),
                Some(_) => return Ok(MarketSlotCandleLookup::ProgramNotDex),
                None => return Ok(MarketSlotCandleLookup::AddressMissing("program")),
            },
            None => None,
        };
        match market.slot_ohlcv(MarketSlotOhlcvQuery {
            quote_mint_id,
            dex_program_id,
            slot_from: query.slot_from,
            slot_to: query.slot_to,
            max_points,
        }) {
            Ok(items) => Ok(MarketSlotCandleLookup::Found(items)),
            Err(error) if error.to_string().contains("response exceeds max_points") => {
                Ok(MarketSlotCandleLookup::LimitExceeded { max_points })
            }
            Err(error) => Err(error),
        }
    })
    .await?;
    match result {
        MarketSlotCandleLookup::Found(items) => Ok(Json(MarketSlotCandlesEnvelope { items })),
        MarketSlotCandleLookup::AddressMissing(field) => Err(market_address_not_found(field)),
        MarketSlotCandleLookup::PairMissing => {
            Ok(Json(MarketSlotCandlesEnvelope { items: Vec::new() }))
        }
        MarketSlotCandleLookup::ProgramNotDex => Err(ApiError::bad_request(
            "market_program_is_not_dex",
            "program must identify an executed DEX program; a router is not a DEX volume filter",
        )),
        MarketSlotCandleLookup::LimitExceeded { max_points } => Err(ApiError::bad_request_details(
            "market_slot_candle_limit_exceeded",
            "the explicit slot range has more non-empty slots than max_points",
            json!({ "max_points": max_points }),
        )),
    }
}

async fn market_program_volume(
    State(state): State<ApiState>,
    query: Result<Query<MarketProgramVolumeHttpQuery>, QueryRejection>,
) -> Result<Json<MarketProgramVolumeSeries>, ApiError> {
    let Query(query) = market_query(query)?;
    let interval_seconds = parse_market_interval(&query.interval)?;
    if query.max_points == 0 || query.max_points > MAX_MARKET_PROGRAM_VOLUME_POINTS {
        return Err(ApiError::bad_request(
            "invalid_market_max_points",
            format!("max_points must be from 1 through {MAX_MARKET_PROGRAM_VOLUME_POINTS}"),
        ));
    }
    let (Some(time_from), Some(time_to)) = (query.time_from, query.time_to) else {
        return Err(ApiError::bad_request(
            "market_time_range_required",
            "time_from and time_to are required for program volume",
        ));
    };
    validate_market_time_range(Some(time_from), Some(time_to))?;
    let points = candle_window_points(time_from, time_to, interval_seconds);
    if points > query.max_points as i128 {
        return Err(ApiError::bad_request_details(
            "market_program_volume_limit_exceeded",
            "the program volume time window exceeds max_points",
            json!({ "points": points.to_string(), "max_points": query.max_points }),
        ));
    }
    let quote_mint = validate_optional_registry_address(query.quote_mint, "quote_mint")?;
    let max_points = query.max_points;
    let result = market_blocking(state, move |market| {
        let quote_mint_id = match quote_mint {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) => Some(id),
                None => return Ok(MarketProgramVolumeLookup::AddressMissing("quote_mint")),
            },
            None => None,
        };
        market
            .program_volume_series(MarketProgramVolumeQuery {
                quote_mint_id,
                interval_seconds,
                time_from,
                time_to,
                max_points,
            })
            .map(MarketProgramVolumeLookup::Found)
    })
    .await?;
    match result {
        MarketProgramVolumeLookup::Found(series) => Ok(Json(series)),
        MarketProgramVolumeLookup::AddressMissing(field) => Err(market_address_not_found(field)),
    }
}

async fn account_trading_summary(
    State(state): State<ApiState>,
    Path(address): Path<String>,
) -> Result<Json<MarketTraderActivitySummary>, ApiError> {
    let address = validate_registry_address(address, "account")?;
    let result = market_blocking(state, move |market| {
        match market.trader_activity_summary_by_address(&address)? {
            Some(summary) => Ok(AccountSummaryLookup::Found(summary)),
            None => Ok(AccountSummaryLookup::AddressMissing("account")),
        }
    })
    .await?;
    match result {
        AccountSummaryLookup::Found(summary) => Ok(Json(summary)),
        AccountSummaryLookup::AddressMissing(field) => Err(market_address_not_found(field)),
    }
}

async fn account_proven_trades(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    query: Result<Query<AccountProvenTradesHttpQuery>, QueryRejection>,
) -> Result<Json<AccountProvenTradesResponse>, ApiError> {
    let address = validate_registry_address(address, "account")?;
    let Query(query) = market_query(query)?;
    validate_market_time_range(query.time_from, query.time_to)?;
    let limit = query.limit.unwrap_or(DEFAULT_MARKET_TRADE_PAGE_ROWS);
    if !(1..=MAX_MARKET_TRADE_PAGE_ROWS).contains(&limit) {
        return Err(ApiError::bad_request(
            "invalid_market_trade_limit",
            format!("market trade limit must be from 1 through {MAX_MARKET_TRADE_PAGE_ROWS}"),
        ));
    }
    let quote_mint = validate_optional_registry_address(query.quote_mint, "quote_mint")?;
    let program = validate_optional_registry_address(query.program, "program")?;
    let result = market_blocking(state, move |market| {
        let Some(trader_id) = market.registry_id_for_address(&address)? else {
            return Ok(AccountTradesLookup::AddressMissing("account"));
        };
        let quote_mint_id = match quote_mint {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) => Some(id),
                None => return Ok(AccountTradesLookup::AddressMissing("quote_mint")),
            },
            None => None,
        };
        let venue_program_id = match program {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) if market.is_executed_dex_program(id) => Some(id),
                Some(_) => return Ok(AccountTradesLookup::ProgramNotDex),
                None => return Ok(AccountTradesLookup::AddressMissing("program")),
            },
            None => None,
        };
        let summary = market.trader_activity_summary(trader_id)?;
        let page = match market.paged_trader_trades(
            trader_id,
            MarketTradeQuery {
                quote_mint_id,
                venue_program_id,
                time_from: query.time_from,
                time_to: query.time_to,
                offset: query.offset.unwrap_or(0),
                limit,
            },
        ) {
            Ok(page) => page,
            Err(error) if error.to_string().contains("page offset exceeds") => {
                return Ok(AccountTradesLookup::OffsetExceeded);
            }
            Err(error) => return Err(error),
        };
        Ok(AccountTradesLookup::Found(AccountProvenTradesResponse {
            supported: true,
            artifact_complete: summary.artifact_complete,
            has_matching_proven_trades: page.total != 0,
            attribution: MARKET_TRADER_ATTRIBUTION,
            includes_inferred_trades: false,
            includes_protocol_positions: false,
            trader: summary.trader,
            page,
        }))
    })
    .await?;
    match result {
        AccountTradesLookup::Found(response) => Ok(Json(response)),
        AccountTradesLookup::AddressMissing(field) => Err(market_address_not_found(field)),
        AccountTradesLookup::ProgramNotDex => Err(market_program_is_not_dex()),
        AccountTradesLookup::OffsetExceeded => Err(ApiError::bad_request(
            "invalid_market_trade_offset",
            "market trade offset exceeds the filtered account result",
        )),
    }
}

async fn account_trading_activity(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    query: Result<Query<AccountTradingActivityHttpQuery>, QueryRejection>,
) -> Result<Json<MarketTraderActivitySeries>, ApiError> {
    let address = validate_registry_address(address, "account")?;
    let Query(query) = market_query(query)?;
    validate_market_time_range(query.time_from, query.time_to)?;
    let interval_seconds = parse_market_interval(&query.interval)?;
    if query.max_points == 0 || query.max_points > MAX_MARKET_TRADER_ACTIVITY_POINTS {
        return Err(ApiError::bad_request(
            "invalid_market_max_points",
            format!("max_points must be from 1 through {MAX_MARKET_TRADER_ACTIVITY_POINTS}"),
        ));
    }
    let quote_mint = validate_optional_registry_address(query.quote_mint, "quote_mint")?;
    let program = validate_optional_registry_address(query.program, "program")?;
    let max_points = query.max_points;
    let result = market_blocking(state, move |market| {
        let Some(trader_id) = market.registry_id_for_address(&address)? else {
            return Ok(AccountActivityLookup::AddressMissing("account"));
        };
        let quote_mint_id = match quote_mint {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) => Some(id),
                None => return Ok(AccountActivityLookup::AddressMissing("quote_mint")),
            },
            None => None,
        };
        let dex_program_id = match program {
            Some(address) => match market.registry_id_for_address(&address)? {
                Some(id) if market.is_executed_dex_program(id) => Some(id),
                Some(_) => return Ok(AccountActivityLookup::ProgramNotDex),
                None => return Ok(AccountActivityLookup::AddressMissing("program")),
            },
            None => None,
        };
        let summary = market.trader_activity_summary(trader_id)?;
        let (time_from, time_to) = account_activity_time_window(
            &summary,
            query.time_from,
            query.time_to,
            interval_seconds,
            max_points,
        );
        let points = candle_window_points(time_from, time_to, interval_seconds);
        if points > max_points as i128 {
            return Ok(AccountActivityLookup::LimitExceeded { points, max_points });
        }
        market
            .trader_activity_series(MarketTraderActivityQuery {
                trader_id,
                quote_mint_id,
                dex_program_id,
                interval_seconds,
                time_from,
                time_to,
                max_points,
            })
            .map(AccountActivityLookup::Found)
    })
    .await?;
    match result {
        AccountActivityLookup::Found(series) => Ok(Json(series)),
        AccountActivityLookup::AddressMissing(field) => Err(market_address_not_found(field)),
        AccountActivityLookup::ProgramNotDex => Err(market_program_is_not_dex()),
        AccountActivityLookup::LimitExceeded { points, max_points } => {
            Err(ApiError::bad_request_details(
                "market_trader_activity_limit_exceeded",
                "the account trading activity time window exceeds max_points",
                json!({ "points": points.to_string(), "max_points": max_points }),
            ))
        }
    }
}

async fn account_balance_history(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    query: Result<Query<AccountBalanceHistoryHttpQuery>, QueryRejection>,
) -> Result<Json<AccountBalanceHistoryResponse>, ApiError> {
    let canonical_address = validate_registry_address(address, "account")?;
    let raw_address = decode_posting_key(&canonical_address)?;
    let Query(query) = query.map_err(|error| {
        ApiError::bad_request(
            "invalid_balance_history_query",
            format!("invalid balance-history query: {error}"),
        )
    })?;
    if query.max_points == 0 || query.max_points > MAX_OWNER_BALANCE_HISTORY_ROWS {
        return Err(ApiError::bad_request(
            "invalid_balance_history_max_points",
            format!("max_points must be from 1 through {MAX_OWNER_BALANCE_HISTORY_ROWS}"),
        ));
    }
    if matches!(
        (query.transaction_id_from, query.transaction_id_to),
        (Some(from), Some(to)) if from > to
    ) {
        return Err(ApiError::bad_request(
            "invalid_balance_history_range",
            "transaction_id_from must be less than or equal to transaction_id_to",
        ));
    }
    let owner_postings = state
        .owner_postings
        .clone()
        .filter(|store| store.has_balance_history())
        .ok_or_else(owner_balance_history_unavailable)?;
    let artifact_complete = owner_postings.complete();
    let permit = state
        .read_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal(anyhow::anyhow!("read semaphore closed")))?;
    let result = tokio::task::spawn_blocking(move || -> Result<AccountBalanceHistoryLookup> {
        let _permit = permit;
        owner_postings
            .lookup_balance_history_range(
                raw_address,
                OwnerBalanceHistoryRangeQuery {
                    transaction_id_from: query.transaction_id_from,
                    transaction_id_to: query.transaction_id_to,
                    max_points: query.max_points,
                },
            )
            .map(|series| {
                series.map_or(AccountBalanceHistoryLookup::Missing, |series| {
                    AccountBalanceHistoryLookup::Found(series)
                })
            })
    })
    .await
    .map_err(|error| ApiError::internal(anyhow::Error::new(error)))?
    .map_err(ApiError::internal)?;
    match result {
        AccountBalanceHistoryLookup::Found(series) => Ok(Json(AccountBalanceHistoryResponse {
            supported: true,
            artifact_complete,
            address: canonical_address,
            attribution: "exact strict-replay transaction-final net balance across all indexed SPYx token accounts owned by this address; zero-net transactions are omitted",
            registry_id: series.registry_id,
            matching_events: series.matching_events,
            sampled: series.sampled,
            items: series.events,
        })),
        AccountBalanceHistoryLookup::Missing => Err(ApiError::not_found(
            "owner_balance_history_not_found",
            "the account has no SPYx owner balance-change events",
        )),
    }
}

fn account_activity_time_window(
    summary: &MarketTraderActivitySummary,
    requested_from: Option<i64>,
    requested_to: Option<i64>,
    interval_seconds: u64,
    max_points: usize,
) -> (i64, i64) {
    match (requested_from, requested_to) {
        (Some(from), Some(to)) => (from, to),
        (Some(from), None) => (from, summary.last_block_time.unwrap_or(from).max(from)),
        (None, Some(to)) => {
            let from = summary
                .first_block_time
                .unwrap_or(to)
                .max(rolling_candle_start(to, interval_seconds, max_points))
                .min(to);
            (from, to)
        }
        (None, None) => match (summary.first_block_time, summary.last_block_time) {
            (Some(first), Some(last)) => (
                first.max(rolling_candle_start(last, interval_seconds, max_points)),
                last,
            ),
            _ => (0, 0),
        },
    }
}

fn market_program_is_not_dex() -> ApiError {
    ApiError::bad_request(
        "market_program_is_not_dex",
        "program must identify an executed DEX program; a router or lending program is not a DEX trade filter",
    )
}

async fn market_blocking<T, F>(state: ApiState, operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(Arc<MarketStore>) -> Result<T> + Send + 'static,
{
    let market = state.market.clone().ok_or_else(market_unavailable)?;
    let permit = state
        .read_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal(anyhow::anyhow!("read semaphore closed")))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation(market)
    })
    .await
    .map_err(|error| ApiError::internal(anyhow::Error::new(error)))?
    .map_err(ApiError::internal)
}

fn market_query<T>(query: Result<Query<T>, QueryRejection>) -> Result<Query<T>, ApiError> {
    query.map_err(|error| {
        ApiError::bad_request(
            "invalid_market_query",
            format!("invalid market query: {error}"),
        )
    })
}

fn default_market_interval() -> String {
    "1h".to_owned()
}

const fn default_market_max_points() -> usize {
    DEFAULT_MARKET_MAX_POINTS
}

const fn default_owner_balance_history_points() -> usize {
    DEFAULT_OWNER_BALANCE_HISTORY_POINTS
}

fn parse_market_interval(value: &str) -> Result<u64, ApiError> {
    let normalized = value.trim().to_ascii_lowercase();
    let seconds = match normalized.as_str() {
        "1h" => 3_600,
        "4h" => 14_400,
        "1d" => 86_400,
        "1w" => 604_800,
        value if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => value
            .parse::<u64>()
            .map_err(|_| invalid_market_interval())?,
        _ => return Err(invalid_market_interval()),
    };
    if seconds == 0 || seconds > i64::MAX as u64 {
        return Err(invalid_market_interval());
    }
    Ok(seconds)
}

fn invalid_market_interval() -> ApiError {
    ApiError::bad_request(
        "invalid_market_interval",
        "interval must be 1h, 4h, 1d, 1w, or a positive number of seconds",
    )
}

fn validate_market_time_range(from: Option<i64>, to: Option<i64>) -> Result<(), ApiError> {
    if matches!((from, to), (Some(from), Some(to)) if from > to) {
        return Err(ApiError::bad_request(
            "invalid_market_time_range",
            "time_from must not be after time_to",
        ));
    }
    Ok(())
}

fn validate_market_slot_range(from: Option<u64>, to: Option<u64>) -> Result<(), ApiError> {
    if matches!((from, to), (Some(from), Some(to)) if from > to) {
        return Err(ApiError::bad_request(
            "invalid_market_slot_range",
            "slot_from must be less than or equal to slot_to",
        ));
    }
    Ok(())
}

fn candle_window_points(from: i64, to: i64, interval_seconds: u64) -> i128 {
    debug_assert!(from <= to);
    debug_assert!(interval_seconds != 0 && interval_seconds <= i64::MAX as u64);
    let interval = i128::from(interval_seconds);
    let first_bucket = i128::from(from).div_euclid(interval);
    let last_bucket = i128::from(to).div_euclid(interval);
    last_bucket - first_bucket + 1
}

fn rolling_candle_start(to: i64, interval_seconds: u64, max_points: usize) -> i64 {
    debug_assert!(interval_seconds != 0 && interval_seconds <= i64::MAX as u64);
    debug_assert!(max_points != 0);
    let interval = i128::from(interval_seconds);
    let last_bucket = i128::from(to).div_euclid(interval);
    let first_bucket = last_bucket - (max_points as i128 - 1);
    let start = first_bucket * interval;
    start.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn validate_optional_registry_address(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<String>, ApiError> {
    value
        .map(|value| validate_registry_address(value, field))
        .transpose()
}

fn validate_registry_address(value: String, field: &'static str) -> Result<String, ApiError> {
    let trimmed = value.trim();
    let decoded = bs58::decode(trimmed).into_vec().map_err(|_| {
        ApiError::bad_request(
            "invalid_market_address",
            format!("{field} is not valid base58"),
        )
    })?;
    if decoded.len() != 32 {
        return Err(ApiError::bad_request(
            "invalid_market_address",
            format!("{field} is not 32 bytes"),
        ));
    }
    Ok(bs58::encode(decoded).into_string())
}

fn market_address_not_found(field: &'static str) -> ApiError {
    ApiError::not_found(
        "market_address_not_found",
        format!("{field} is not present in the source registry"),
    )
}

fn market_unavailable() -> ApiError {
    ApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "market_not_available",
        "the market index is not available",
        None,
    )
}

fn parse_unsigned_id(value: &str, code: &'static str, label: &str) -> Result<u64, ApiError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ApiError::bad_request(
            code,
            format!("{label} must be an unsigned decimal integer"),
        ));
    }
    value
        .parse::<u64>()
        .map_err(|_| ApiError::bad_request(code, format!("{label} exceeds the u64 range")))
}

#[derive(Debug, Deserialize)]
struct CoordinateQuery {
    epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
}

async fn by_coordinate(
    State(state): State<ApiState>,
    query: Result<Query<CoordinateQuery>, QueryRejection>,
) -> Result<Json<TransactionEnvelope>, ApiError> {
    let Query(query) = query.map_err(|error| {
        ApiError::bad_request("invalid_coordinate", format!("invalid coordinate: {error}"))
    })?;
    let coordinate = TransactionCoordinate {
        epoch: query.epoch,
        slot: query.slot,
        source_block_id: query.source_block_id,
        tx_index: query.tx_index,
    };
    let id = state
        .store
        .lookup_coordinate(coordinate)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("transaction_not_found", "coordinate was not found"))?;
    Ok(Json(TransactionEnvelope {
        transaction: positioned_detail(state, id).await?,
    }))
}

async fn by_signature(
    State(state): State<ApiState>,
    Path(value): Path<String>,
) -> Result<Json<TransactionEnvelope>, ApiError> {
    let decoded = bs58::decode(&value)
        .into_vec()
        .map_err(|_| ApiError::bad_request("invalid_signature", "signature is not valid base58"))?;
    let signature: [u8; 64] = decoded
        .try_into()
        .map_err(|_| ApiError::bad_request("invalid_signature", "signature is not 64 bytes"))?;
    let matches = state
        .store
        .lookup_signature(signature)
        .map_err(ApiError::internal)?
        .transaction_ids;
    let id = match matches.as_slice() {
        [] => {
            return Err(ApiError::not_found(
                "transaction_not_found",
                "signature was not found",
            ));
        }
        [id] => *id,
        _ => {
            return Err(ApiError::conflict(
                "signature_has_multiple_transactions",
                "signature occurs in more than one transaction",
                json!({ "transaction_ids": matches }),
            ));
        }
    };
    Ok(Json(TransactionEnvelope {
        transaction: positioned_detail(state, id).await?,
    }))
}

async fn by_id(
    State(state): State<ApiState>,
    Path(value): Path<String>,
) -> Result<Json<TransactionEnvelope>, ApiError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ApiError::bad_request(
            "invalid_transaction_id",
            "transaction ID must be an unsigned decimal integer",
        ));
    }
    let id = value.parse::<u64>().map_err(|_| {
        ApiError::bad_request(
            "invalid_transaction_id",
            "transaction ID exceeds the u64 range",
        )
    })?;
    if id >= state.store.transaction_count() {
        return Err(ApiError::not_found(
            "transaction_not_found",
            "transaction ID was not found",
        ));
    }
    Ok(Json(TransactionEnvelope {
        transaction: positioned_detail(state, id).await?,
    }))
}

#[derive(Debug, Default, Deserialize)]
struct PostingsQuery {
    cursor: Option<String>,
    limit: Option<usize>,
    instruction_scope: Option<ProgramInstructionScope>,
}

#[derive(Serialize)]
struct PostingsEnvelope {
    kind: &'static str,
    key: String,
    registry_id: u32,
    flags: u32,
    total: u64,
    offset: u64,
    limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    instruction_scope: Option<ProgramInstructionScope>,
    items: Vec<PostingTransactionDetail>,
    next_cursor: Option<String>,
}

enum BlockingPostingsLookup {
    Found(PostingsEnvelope),
    Missing,
    CursorMismatch,
}

async fn postings_target_address(
    State(state): State<ApiState>,
    Path(key): Path<String>,
    query: Result<Query<PostingsQuery>, QueryRejection>,
) -> Result<Json<PostingsEnvelope>, ApiError> {
    postings_lookup(state, key, query, PostingLookupKind::TargetAddress).await
}

async fn postings_token_account(
    State(state): State<ApiState>,
    Path(key): Path<String>,
    query: Result<Query<PostingsQuery>, QueryRejection>,
) -> Result<Json<PostingsEnvelope>, ApiError> {
    postings_lookup(state, key, query, PostingLookupKind::TokenAccount).await
}

async fn postings_program(
    State(state): State<ApiState>,
    Path(key): Path<String>,
    query: Result<Query<PostingsQuery>, QueryRejection>,
) -> Result<Json<PostingsEnvelope>, ApiError> {
    postings_lookup(state, key, query, PostingLookupKind::Program).await
}

async fn postings_owner(
    State(state): State<ApiState>,
    Path(key): Path<String>,
    query: Result<Query<PostingsQuery>, QueryRejection>,
) -> Result<Json<PostingsEnvelope>, ApiError> {
    let Query(query) = query.map_err(|error| {
        ApiError::bad_request(
            "invalid_postings_query",
            format!("invalid postings query: {error}"),
        )
    })?;
    let limit = query.limit.unwrap_or(DEFAULT_POSTINGS_PAGE_ROWS);
    if query.instruction_scope.is_some() {
        return Err(ApiError::bad_request(
            "invalid_instruction_scope",
            "instruction_scope is only valid for program postings",
        ));
    }
    if !(1..=MAX_POSTINGS_PAGE_ROWS).contains(&limit) {
        return Err(ApiError::bad_request(
            "invalid_postings_limit",
            format!("postings limit must be from 1 through {MAX_POSTINGS_PAGE_ROWS}"),
        ));
    }
    let raw_key = decode_posting_key(&key)?;
    let canonical_key = bs58::encode(raw_key).into_string();
    let postings = state
        .owner_postings
        .clone()
        .ok_or_else(postings_unavailable)?;
    let cursor = match query.cursor.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(value) => Some(decode_postings_cursor(
            value,
            CursorPostingKind::Owner,
            postings.manifest_sha256_bytes(),
        )?),
    };
    let offset = cursor.map_or(0, |cursor| cursor.offset);
    let permit = state
        .read_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal(anyhow::anyhow!("read semaphore closed")))?;
    let query_store = state.store;
    let result = tokio::task::spawn_blocking(move || -> Result<BlockingPostingsLookup> {
        let _permit = permit;
        if let Some(cursor) = cursor {
            let Some(first_page) = postings.lookup(raw_key, 0, 1)? else {
                return Ok(BlockingPostingsLookup::Missing);
            };
            if first_page.registry_id != cursor.registry_id || cursor.offset > first_page.total {
                return Ok(BlockingPostingsLookup::CursorMismatch);
            }
        }
        let Some(page) = postings.lookup(raw_key, offset, limit)? else {
            return Ok(BlockingPostingsLookup::Missing);
        };
        let mut items = Vec::new();
        items
            .try_reserve_exact(page.transaction_ordinals.len())
            .context("reserve owner posting transaction response")?;
        for transaction_id in page.transaction_ordinals {
            items.push(query_store.posting_transaction_detail(transaction_id)?);
        }
        let next_cursor = page.next_offset.map(|next_offset| {
            encode_postings_cursor(
                CursorPostingKind::Owner,
                page.registry_id,
                next_offset,
                postings.manifest_sha256_bytes(),
            )
        });
        Ok(BlockingPostingsLookup::Found(PostingsEnvelope {
            kind: "owner",
            key: canonical_key,
            registry_id: page.registry_id,
            flags: 0,
            total: page.total,
            offset: page.offset,
            limit,
            instruction_scope: None,
            items,
            next_cursor,
        }))
    })
    .await
    .map_err(|error| ApiError::internal(anyhow::Error::new(error)))?
    .map_err(ApiError::internal)?;
    match result {
        BlockingPostingsLookup::Found(response) => Ok(Json(response)),
        BlockingPostingsLookup::Missing => Err(ApiError::not_found(
            "postings_not_found",
            "the key has no postings for the requested kind",
        )),
        BlockingPostingsLookup::CursorMismatch => Err(ApiError::bad_request(
            "invalid_postings_cursor",
            "postings cursor does not match the requested key range",
        )),
    }
}

async fn postings_lookup(
    state: ApiState,
    key: String,
    query: Result<Query<PostingsQuery>, QueryRejection>,
    kind: PostingLookupKind,
) -> Result<Json<PostingsEnvelope>, ApiError> {
    let Query(query) = query.map_err(|error| {
        ApiError::bad_request(
            "invalid_postings_query",
            format!("invalid postings query: {error}"),
        )
    })?;
    let limit = query.limit.unwrap_or(DEFAULT_POSTINGS_PAGE_ROWS);
    let instruction_scope = match (kind, query.instruction_scope) {
        (PostingLookupKind::Program, scope) => scope.unwrap_or_default(),
        (_, None) => ProgramInstructionScope::All,
        (_, Some(_)) => {
            return Err(ApiError::bad_request(
                "invalid_instruction_scope",
                "instruction_scope is only valid for program postings",
            ));
        }
    };
    if !(1..=MAX_POSTINGS_PAGE_ROWS).contains(&limit) {
        return Err(ApiError::bad_request(
            "invalid_postings_limit",
            format!("postings limit must be from 1 through {MAX_POSTINGS_PAGE_ROWS}"),
        ));
    }
    let raw_key = decode_posting_key(&key)?;
    let canonical_key = bs58::encode(raw_key).into_string();
    let postings = state.postings.clone().ok_or_else(postings_unavailable)?;
    let cursor_kind = CursorPostingKind::for_lookup(kind, instruction_scope);
    let cursor = match query.cursor.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(value) => Some(decode_postings_cursor(
            value,
            cursor_kind,
            postings.manifest_sha256_bytes(),
        )?),
    };
    let offset = cursor.map_or(0, |cursor| cursor.offset);
    let permit = state
        .read_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal(anyhow::anyhow!("read semaphore closed")))?;
    let query_store = state.store;
    let result = tokio::task::spawn_blocking(move || -> Result<BlockingPostingsLookup> {
        let _permit = permit;
        if let Some(cursor) = cursor {
            let first_page = if kind == PostingLookupKind::Program {
                postings.lookup_program(raw_key, instruction_scope, 0, 1)?
            } else {
                postings.lookup(kind, raw_key, 0, 1)?
            };
            let Some(first_page) = first_page else {
                return Ok(BlockingPostingsLookup::Missing);
            };
            if first_page.registry_id != cursor.registry_id || cursor.offset > first_page.total {
                return Ok(BlockingPostingsLookup::CursorMismatch);
            }
        }
        let page = if kind == PostingLookupKind::Program {
            postings.lookup_program(raw_key, instruction_scope, offset, limit)?
        } else {
            postings.lookup(kind, raw_key, offset, limit)?
        };
        let Some(page) = page else {
            return Ok(BlockingPostingsLookup::Missing);
        };
        let mut items = Vec::new();
        items
            .try_reserve_exact(page.transaction_ordinals.len())
            .context("reserve posting transaction response")?;
        for transaction_id in page.transaction_ordinals {
            items.push(query_store.posting_transaction_detail(transaction_id)?);
        }
        let next_cursor = page.next_offset.map(|next_offset| {
            encode_postings_cursor(
                cursor_kind,
                page.registry_id,
                next_offset,
                postings.manifest_sha256_bytes(),
            )
        });
        Ok(BlockingPostingsLookup::Found(PostingsEnvelope {
            kind: posting_kind_name(kind),
            key: canonical_key,
            registry_id: page.registry_id,
            flags: page.flags,
            total: page.total,
            offset: page.offset,
            limit,
            instruction_scope: (kind == PostingLookupKind::Program).then_some(instruction_scope),
            items,
            next_cursor,
        }))
    })
    .await
    .map_err(|error| ApiError::internal(anyhow::Error::new(error)))?
    .map_err(ApiError::internal)?;
    match result {
        BlockingPostingsLookup::Found(response) => Ok(Json(response)),
        BlockingPostingsLookup::Missing => Err(ApiError::not_found(
            "postings_not_found",
            "the key has no postings for the requested kind",
        )),
        BlockingPostingsLookup::CursorMismatch => Err(ApiError::bad_request(
            "invalid_postings_cursor",
            "postings cursor does not match the requested key range",
        )),
    }
}

#[derive(Debug, Clone, Copy)]
struct DecodedPostingsCursor {
    registry_id: u32,
    offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorPostingKind {
    TargetAddress,
    TokenAccount,
    ProgramAll,
    ProgramDirect,
    ProgramInner,
    Owner,
}

impl CursorPostingKind {
    fn for_lookup(value: PostingLookupKind, scope: ProgramInstructionScope) -> Self {
        match (value, scope) {
            (PostingLookupKind::TargetAddress, ProgramInstructionScope::All) => Self::TargetAddress,
            (PostingLookupKind::TokenAccount, ProgramInstructionScope::All) => Self::TokenAccount,
            (PostingLookupKind::Program, ProgramInstructionScope::All) => Self::ProgramAll,
            (PostingLookupKind::Program, ProgramInstructionScope::Direct) => Self::ProgramDirect,
            (PostingLookupKind::Program, ProgramInstructionScope::Inner) => Self::ProgramInner,
            _ => unreachable!("non-program posting scope was validated as all"),
        }
    }
}

fn encode_postings_cursor(
    kind: CursorPostingKind,
    registry_id: u32,
    offset: u64,
    manifest_sha256: [u8; 32],
) -> String {
    let mut bytes = [0u8; POSTINGS_CURSOR_BYTES];
    bytes[0] = POSTINGS_CURSOR_VERSION;
    bytes[1] = posting_kind_tag(kind);
    bytes[4..8].copy_from_slice(&registry_id.to_le_bytes());
    bytes[8..16].copy_from_slice(&offset.to_le_bytes());
    bytes[16..48].copy_from_slice(&manifest_sha256);
    let checksum = postings_cursor_checksum(&bytes[..48]);
    bytes[48..80].copy_from_slice(&checksum);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_postings_cursor(
    value: &str,
    expected_kind: CursorPostingKind,
    expected_manifest_sha256: [u8; 32],
) -> Result<DecodedPostingsCursor, ApiError> {
    let mut bytes = [0u8; POSTINGS_CURSOR_BYTES];
    let decoded = URL_SAFE_NO_PAD
        .decode_slice(value, &mut bytes)
        .map_err(|_| {
            ApiError::bad_request(
                "invalid_postings_cursor",
                "postings cursor is not valid base64url",
            )
        })?;
    let valid = decoded == bytes.len()
        && bytes[0] == POSTINGS_CURSOR_VERSION
        && bytes[1] == posting_kind_tag(expected_kind)
        && bytes[2..4] == [0, 0]
        && bytes[16..48] == expected_manifest_sha256
        && bytes[48..80] == postings_cursor_checksum(&bytes[..48]);
    if !valid {
        return Err(ApiError::bad_request(
            "invalid_postings_cursor",
            "postings cursor does not match this index or posting kind",
        ));
    }
    let registry_id = u32::from_le_bytes(bytes[4..8].try_into().expect("fixed cursor ID range"));
    if registry_id == 0 {
        return Err(ApiError::bad_request(
            "invalid_postings_cursor",
            "postings cursor has a reserved registry ID",
        ));
    }
    Ok(DecodedPostingsCursor {
        registry_id,
        offset: u64::from_le_bytes(bytes[8..16].try_into().expect("fixed cursor offset range")),
    })
}

fn postings_cursor_checksum(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(POSTINGS_CURSOR_DOMAIN);
    hasher.update(payload);
    hasher.finalize().into()
}

fn decode_posting_key(value: &str) -> Result<[u8; 32], ApiError> {
    let mut key = [0u8; 32];
    let decoded = bs58::decode(value).onto(&mut key).map_err(|_| {
        ApiError::bad_request("invalid_posting_key", "posting key is not valid base58")
    })?;
    if decoded != key.len() {
        return Err(ApiError::bad_request(
            "invalid_posting_key",
            "posting key is not 32 bytes",
        ));
    }
    Ok(key)
}

const fn posting_kind_tag(kind: CursorPostingKind) -> u8 {
    match kind {
        CursorPostingKind::TargetAddress => 1,
        CursorPostingKind::TokenAccount => 2,
        CursorPostingKind::ProgramAll => 3,
        CursorPostingKind::Owner => 4,
        CursorPostingKind::ProgramDirect => 5,
        CursorPostingKind::ProgramInner => 6,
    }
}

const fn posting_kind_name(kind: PostingLookupKind) -> &'static str {
    match kind {
        PostingLookupKind::TargetAddress => "target-address",
        PostingLookupKind::TokenAccount => "token-account",
        PostingLookupKind::Program => "program",
    }
}

fn postings_unavailable() -> ApiError {
    ApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "postings_not_available",
        "postings indexes are not available",
        None,
    )
}

fn owner_balance_history_unavailable() -> ApiError {
    ApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "owner_balance_history_not_available",
        "the owner balance-history index is not available",
        None,
    )
}

async fn positioned_detail(state: ApiState, id: u64) -> Result<TransactionDetail, ApiError> {
    let permit = state
        .read_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal(anyhow::anyhow!("read semaphore closed")))?;
    let store = state.store;
    let scratch_pool = state.scratch_pool;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut scratch = scratch_pool.checkout();
        store.transaction_detail(id, scratch.buffer())
    })
    .await
    .map_err(|error| ApiError::internal(anyhow::Error::new(error)))?
    .map_err(ApiError::internal)
}

#[derive(Serialize)]
struct TransactionEnvelope {
    transaction: TransactionDetail,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    details: Option<Value>,
}

impl ApiError {
    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message, None)
    }

    fn bad_request_details(code: &'static str, message: impl Into<String>, details: Value) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message, Some(details))
    }

    fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message, None)
    }

    fn conflict(code: &'static str, message: impl Into<String>, details: Value) -> Self {
        Self::new(StatusCode::CONFLICT, code, message, Some(details))
    }

    fn internal(error: anyhow::Error) -> Self {
        tracing::error!(error = %error, "SPYx query failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "query failed",
            None,
        )
    }

    fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        details: Option<Value>,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        let body = json!({
            "error": self.code,
            "message": self.message,
            "details": self.details,
        });
        (self.status, Json(body)).into_response()
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install shutdown signal");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        http::{Request, Uri},
        routing::get,
    };
    use tower::ServiceExt;

    #[test]
    fn scratch_pool_reuses_small_buffers_and_discards_large_buffers() {
        let pool = Arc::new(ScratchPool::new(1, 64));
        {
            let mut scratch = pool.checkout();
            scratch.buffer().reserve_exact(32);
        }
        {
            let mut scratch = pool.checkout();
            assert!(scratch.buffer().capacity() >= 32);
            scratch.buffer().resize(65, 0);
        }
        let mut scratch = pool.checkout();
        assert_eq!(scratch.buffer().capacity(), 0);
    }

    #[test]
    fn postings_cursor_is_bound_to_kind_manifest_key_and_offset() {
        let manifest = [7u8; 32];
        let encoded = encode_postings_cursor(CursorPostingKind::TargetAddress, 42, 101, manifest);
        let decoded =
            decode_postings_cursor(&encoded, CursorPostingKind::TargetAddress, manifest).unwrap();
        assert_eq!(decoded.registry_id, 42);
        assert_eq!(decoded.offset, 101);
        assert!(
            decode_postings_cursor(&encoded, CursorPostingKind::TokenAccount, manifest).is_err()
        );
        assert!(decode_postings_cursor(&encoded, CursorPostingKind::Owner, manifest).is_err());
        assert!(
            decode_postings_cursor(&encoded, CursorPostingKind::TargetAddress, [8u8; 32],).is_err()
        );

        let mut bytes = URL_SAFE_NO_PAD.decode(&encoded).unwrap();
        bytes[8] ^= 1;
        let tampered = URL_SAFE_NO_PAD.encode(bytes);
        assert!(
            decode_postings_cursor(&tampered, CursorPostingKind::TargetAddress, manifest,).is_err()
        );

        let owner = encode_postings_cursor(CursorPostingKind::Owner, 313_730, 21, [9u8; 32]);
        assert!(decode_postings_cursor(&owner, CursorPostingKind::Owner, [9u8; 32]).is_ok());
        assert!(decode_postings_cursor(&owner, CursorPostingKind::Owner, manifest).is_err());

        let direct = encode_postings_cursor(CursorPostingKind::ProgramDirect, 9, 1, manifest);
        assert!(
            decode_postings_cursor(&direct, CursorPostingKind::ProgramDirect, manifest).is_ok()
        );
        assert!(decode_postings_cursor(&direct, CursorPostingKind::ProgramAll, manifest).is_err());
        assert!(
            decode_postings_cursor(&direct, CursorPostingKind::ProgramInner, manifest).is_err()
        );
    }

    #[test]
    fn posting_key_decoder_is_exact_and_allocation_independent() {
        let key = [9u8; 32];
        let encoded = bs58::encode(key).into_string();
        assert_eq!(decode_posting_key(&encoded).unwrap(), key);
        assert!(decode_posting_key("not-0-base58").is_err());
        assert!(decode_posting_key(&bs58::encode([1u8; 31]).into_string()).is_err());
    }

    #[test]
    fn market_interval_parser_accepts_names_and_positive_seconds() {
        assert_eq!(parse_market_interval("1h").unwrap(), 3_600);
        assert_eq!(parse_market_interval("4H").unwrap(), 14_400);
        assert_eq!(parse_market_interval("1d").unwrap(), 86_400);
        assert_eq!(parse_market_interval("1w").unwrap(), 604_800);
        assert_eq!(parse_market_interval("900").unwrap(), 900);
        assert!(parse_market_interval("0").is_err());
        assert!(parse_market_interval("15m").is_err());
    }

    #[test]
    fn candle_window_counts_bucket_boundaries_and_rolls_exactly() {
        assert_eq!(candle_window_points(3_599, 3_600, 3_600), 2);
        assert_eq!(candle_window_points(3_600, 7_199, 3_600), 1);
        let start = rolling_candle_start(36_123, 3_600, 5);
        assert_eq!(start, 21_600);
        assert_eq!(candle_window_points(start, 36_123, 3_600), 5);
    }

    #[test]
    fn slot_and_program_volume_query_schemas_are_explicit_and_bounded() {
        let slot_uri: Uri = "/api/v1/market/slot-candles?quote_mint=11111111111111111111111111111111&venue=Vote111111111111111111111111111111111111111&slot_from=10&slot_to=20&max_points=7"
            .parse()
            .unwrap();
        let Query(slot) = Query::<MarketSlotCandlesHttpQuery>::try_from_uri(&slot_uri).unwrap();
        assert_eq!(
            slot.program.as_deref(),
            Some("Vote111111111111111111111111111111111111111")
        );
        assert_eq!(
            (slot.slot_from, slot.slot_to, slot.max_points),
            (Some(10), Some(20), 7)
        );
        assert!(validate_market_slot_range(slot.slot_from, slot.slot_to).is_ok());
        assert!(validate_market_slot_range(Some(21), Some(20)).is_err());

        let volume_uri: Uri =
            "/api/v1/market/program-volume?from=100&to=199&quote_mint=11111111111111111111111111111111"
                .parse()
                .unwrap();
        let Query(volume) =
            Query::<MarketProgramVolumeHttpQuery>::try_from_uri(&volume_uri).unwrap();
        assert_eq!((volume.time_from, volume.time_to), (Some(100), Some(199)));
        assert_eq!(volume.interval, "1h");
        assert_eq!(volume.max_points, DEFAULT_MARKET_MAX_POINTS);
        assert!(validate_market_time_range(volume.time_from, volume.time_to).is_ok());
    }

    #[test]
    fn account_trade_query_schemas_are_explicit_and_use_market_bounds() {
        let trades_uri: Uri = "/api/v1/accounts/11111111111111111111111111111111/trades?quote_mint=Vote111111111111111111111111111111111111111&venue=Stake11111111111111111111111111111111111111&from=100&to=200&offset=4&limit=7"
            .parse()
            .unwrap();
        let Query(trades) =
            Query::<AccountProvenTradesHttpQuery>::try_from_uri(&trades_uri).unwrap();
        assert_eq!(
            trades.quote_mint.as_deref(),
            Some("Vote111111111111111111111111111111111111111")
        );
        assert_eq!(
            trades.program.as_deref(),
            Some("Stake11111111111111111111111111111111111111")
        );
        assert_eq!(
            (
                trades.time_from,
                trades.time_to,
                trades.offset,
                trades.limit
            ),
            (Some(100), Some(200), Some(4), Some(7))
        );

        let activity_uri: Uri = "/api/v1/accounts/11111111111111111111111111111111/trading-activity?interval=900&max_points=12"
            .parse()
            .unwrap();
        let Query(activity) =
            Query::<AccountTradingActivityHttpQuery>::try_from_uri(&activity_uri).unwrap();
        assert_eq!(activity.interval, "900");
        assert_eq!(activity.max_points, 12);
        assert_eq!((activity.time_from, activity.time_to), (None, None));

        let balance_uri: Uri = "/api/v1/accounts/11111111111111111111111111111111/balance-history?transaction_id_from=10&transaction_id_to=20&max_points=7"
            .parse()
            .unwrap();
        let Query(balance) =
            Query::<AccountBalanceHistoryHttpQuery>::try_from_uri(&balance_uri).unwrap();
        assert_eq!(
            (
                balance.transaction_id_from,
                balance.transaction_id_to,
                balance.max_points,
            ),
            (Some(10), Some(20), 7)
        );

        let default_balance_uri: Uri =
            "/api/v1/accounts/11111111111111111111111111111111/balance-history"
                .parse()
                .unwrap();
        let Query(default_balance) =
            Query::<AccountBalanceHistoryHttpQuery>::try_from_uri(&default_balance_uri).unwrap();
        assert_eq!(
            default_balance.max_points,
            DEFAULT_OWNER_BALANCE_HISTORY_POINTS
        );
    }

    #[test]
    fn account_market_routes_register_with_api_state() {
        let _: Router<ApiState> = Router::new()
            .route(
                "/api/v1/accounts/{address}/trading-summary",
                get(account_trading_summary),
            )
            .route(
                "/api/v1/accounts/{address}/trades",
                get(account_proven_trades),
            )
            .route(
                "/api/v1/accounts/{address}/trading-activity",
                get(account_trading_activity),
            )
            .route(
                "/api/v1/accounts/{address}/balance-history",
                get(account_balance_history),
            );
    }

    #[test]
    fn zero_account_trades_serialize_as_supported_without_inference() {
        let address = bs58::encode([7u8; 32]).into_string();
        let response = AccountProvenTradesResponse {
            supported: true,
            artifact_complete: true,
            has_matching_proven_trades: false,
            attribution: MARKET_TRADER_ATTRIBUTION,
            includes_inferred_trades: false,
            includes_protocol_positions: false,
            trader: RegistryKeyView {
                registry_id: 7,
                address,
            },
            page: MarketTradePage {
                total: 0,
                offset: 0,
                limit: 100,
                trades: Vec::new(),
                next_offset: None,
            },
        };
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["supported"], true);
        assert_eq!(value["has_matching_proven_trades"], false);
        assert_eq!(value["attribution"], "parser_proven_exact_trader");
        assert_eq!(value["includes_inferred_trades"], false);
        assert_eq!(value["includes_protocol_positions"], false);
        assert_eq!(value["total"], 0);
        assert_eq!(value["trades"], json!([]));
        assert!(value.get("page").is_none(), "page fields must be flat");
    }

    #[test]
    fn account_activity_window_is_bounded_and_empty_accounts_stay_empty() {
        let empty = MarketTraderActivitySummary {
            supported: true,
            artifact_complete: true,
            has_proven_trades: false,
            attribution: MARKET_TRADER_ATTRIBUTION,
            includes_inferred_trades: false,
            includes_protocol_positions: false,
            trader: RegistryKeyView {
                registry_id: 1,
                address: bs58::encode([1u8; 32]).into_string(),
            },
            target_mint: RegistryKeyView {
                registry_id: 2,
                address: bs58::encode([2u8; 32]).into_string(),
            },
            target_decimals: 8,
            first_block_time: None,
            last_block_time: None,
            totals: crate::market_store::MarketTraderActivityTotals {
                trade_count: 0,
                buy_count: 0,
                sell_count: 0,
                target_bought_raw: "0".to_owned(),
                target_sold_raw: "0".to_owned(),
                quote_totals: Vec::new(),
            },
        };
        assert_eq!(
            account_activity_time_window(&empty, None, None, 60, 10),
            (0, 0)
        );
        assert_eq!(
            account_activity_time_window(&empty, Some(500), None, 60, 10),
            (500, 500)
        );

        let mut populated = empty;
        populated.has_proven_trades = true;
        populated.first_block_time = Some(100);
        populated.last_block_time = Some(10_000);
        let (from, to) = account_activity_time_window(&populated, None, None, 60, 10);
        assert_eq!(to, 10_000);
        assert_eq!(candle_window_points(from, to, 60), 10);
        assert_eq!(
            account_activity_time_window(&populated, Some(20_000), None, 60, 10),
            (20_000, 20_000)
        );
        assert_eq!(
            account_activity_time_window(&populated, None, Some(50), 60, 10),
            (50, 50)
        );
    }

    #[test]
    fn market_addresses_are_canonical_and_exact() {
        let address = bs58::encode([7u8; 32]).into_string();
        assert_eq!(
            validate_registry_address(format!(" {address} "), "quote_mint").unwrap(),
            address
        );
        assert!(validate_registry_address("not-base58!".to_owned(), "venue").is_err());
        assert!(validate_registry_address(bs58::encode([1u8; 31]).into_string(), "venue").is_err());
    }

    #[test]
    fn official_mint_display_is_exactly_bound_to_the_mint_address() {
        let display = official_mint_display("bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1").unwrap();
        assert_eq!(display.name, "BlazeStake Staked SOL");
        assert_eq!(display.symbol, "bSOL");
        assert!(official_mint_display("11111111111111111111111111111111").is_none());
    }

    #[test]
    fn health_omits_market_when_the_artifact_is_not_loaded() {
        let value = serde_json::to_value(HealthResponse {
            status: "ok",
            index: HealthIndex {
                complete: true,
                transactions: 1,
                source_transaction_sha256: "00".repeat(32),
            },
            postings: HealthPostings {
                available: false,
                complete: false,
                target_address: false,
                token_account: false,
                program: false,
                owner: false,
                owner_balance_history: false,
                target_address_keys: 0,
                target_address_postings: 0,
                program_keys: 0,
                program_postings: 0,
                owner_keys: 0,
                owner_postings: 0,
                owner_balance_history_keys: 0,
                owner_balance_history_events: 0,
            },
            market: None,
            mint_metadata: None,
        })
        .unwrap();
        assert!(value.get("market").is_none());
    }

    #[tokio::test]
    async fn missing_market_route_state_is_a_clear_not_implemented_response() {
        let response = market_unavailable().into_response();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"], "market_not_available");
    }

    #[tokio::test]
    async fn static_site_serves_root_and_spa_fallback_without_shadowing_api() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("index.html"), "site-index").unwrap();
        std::fs::write(directory.path().join("200.html"), "site-fallback").unwrap();
        std::fs::create_dir(directory.path().join("_app")).unwrap();
        std::fs::create_dir(directory.path().join("data")).unwrap();
        std::fs::write(directory.path().join("_app/app.js"), "site-script").unwrap();
        std::fs::write(directory.path().join("data/report.json"), "site-data").unwrap();
        let app = with_static_site(
            Router::new().route("/api/ping", get(|| async { "api-pong" })),
            directory.path(),
        )
        .unwrap();

        for (uri, expected) in [
            ("/", "site-index"),
            ("/search?transaction_id=42", "site-fallback"),
            ("/api/ping", "api-pong"),
            ("/_app/app.js", "site-script"),
            ("/data/report.json", "site-data"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(Uri::from_static(uri))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            assert_eq!(body.as_ref(), expected.as_bytes());
        }

        for uri in ["/api/missing", "/healthz/missing", "/_app/missing.js"] {
            let response = app
                .clone()
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }
}
