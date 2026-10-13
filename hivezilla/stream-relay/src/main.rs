use anyhow::{Context, Result, ensure};
use clap::{Args, Parser, Subcommand};
use hivezilla_stream::{
    server::{Api, Tls},
    source::{Source, bounded_file},
    store::Store,
    upstream::Upstream,
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(about = "Read-only compact bank origin and shared Rust relay")]
struct Cli {
    #[command(subcommand)]
    command: Mode,
}
#[derive(Subcommand)]
enum Mode {
    /// Publish the same immutable feed frames to a separate native R2 namespace.
    Publish(hivezilla_stream::publisher::Options),
    /// Private native v2 origin; requires a separate durable spool.
    BankOrigin {
        #[command(flatten)]
        http: Http,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        spool: PathBuf,
    },
    Origin {
        #[command(flatten)]
        http: Http,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        start_slot: Option<u64>,
        #[arg(long, conflicts_with = "start_slot")]
        tail_slots: Option<u64>,
        #[arg(long, conflicts_with_all=["start_slot","tail_slots"])]
        processed: bool,
        #[arg(long, requires = "processed")]
        start_sequence: Option<u64>,
        #[arg(long, requires = "processed", conflicts_with = "start_sequence")]
        tail_events: Option<u64>,
    },
    Relay {
        #[command(flatten)]
        http: Http,
        #[arg(long)]
        upstream: String,
        #[arg(long)]
        upstream_token_file: PathBuf,
        #[arg(long)]
        upstream_ca: Option<PathBuf>,
        #[arg(long)]
        upstream_identity: Option<PathBuf>,
        #[arg(long)]
        spool: PathBuf,
        /// Permit api_key in the request URL for browser demos.
        #[arg(long)]
        allow_query_api_key: bool,
        /// Serve small processed bank identities without an API key.
        #[arg(long)]
        public_blockhashes: bool,
        #[arg(long, default_value_t = 8)]
        public_max_requests: usize,
        /// Add the separate v2 bank profile. The expected source is operator pinned.
        #[arg(long,requires_all=["banks_source_file","banks_spool"])]
        banks_upstream: Option<String>,
        #[arg(long, requires = "banks_upstream")]
        banks_source_file: Option<PathBuf>,
        #[arg(long, requires = "banks_upstream")]
        banks_spool: Option<PathBuf>,
        #[arg(long, default_value_t = 4096, requires = "banks_upstream")]
        banks_replay_mib: usize,
    },
}
#[derive(Args)]
struct Http {
    #[arg(long, default_value = "127.0.0.1:8870")]
    bind: SocketAddr,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    #[arg(long)]
    tls_key: Option<PathBuf>,
    #[arg(long)]
    client_ca: Option<PathBuf>,
    #[arg(long)]
    cors_origin: Option<String>,
    #[arg(long, default_value_t = 128)]
    replay_mib: usize,
    #[arg(long, default_value_t = 32)]
    max_requests: usize,
}
fn token(p: &std::path::Path) -> Result<String> {
    let value = String::from_utf8(bounded_file(p, 4096)?)?.trim().to_owned();
    ensure!(
        (32..=256).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_graphic()),
        "invalid_token_file"
    );
    Ok(value)
}
fn api(http: &Http, store: Arc<Store>) -> Result<Api> {
    ensure!(
        (1..=1024).contains(&http.max_requests),
        "invalid_connection_budget"
    );
    if let Some(o) = &http.cors_origin {
        let u = reqwest::Url::parse(o)?;
        ensure!(
            matches!(u.scheme(), "http" | "https") && *o == u.origin().ascii_serialization(),
            "cors_requires_one_exact_origin"
        );
    }
    Ok(Api {
        store,
        token: Arc::new(token(&http.token_file)?),
        cors: http.cors_origin.clone(),
        requests: Arc::new(Semaphore::new(http.max_requests)),
        allow_query_api_key: false,
        public_blockhashes: false,
        public_requests: Arc::new(Semaphore::new(0)),
        banks: None,
        bank_only: false,
    })
}
async fn serve(http: Http, api: Api, origin: bool) -> Result<()> {
    let tls = Tls {
        cert: http.tls_cert,
        key: http.tls_key,
        client_ca: http.client_ca,
    };
    tokio::select! { r=hivezilla_stream::server::serve(http.bind,api,tls,origin)=>r,
    _=tokio::signal::ctrl_c()=>Ok(()) }
}
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match Cli::parse().command {
        Mode::Publish(options) => {
            tokio::task::spawn_blocking(move || hivezilla_stream::publisher::run(options)).await?
        }
        Mode::BankOrigin {
            http,
            output,
            spool,
        } => {
            use hivezilla_stream::{
                bank_server::BankApi, bank_source::BankSource, bank_store::BankStore,
            };
            let mut source = BankSource::open(&output)?;
            let base = blockzilla_bank_stream_v2::cursor(&source.source.session_id, 0, "-");
            let banks = BankStore::open(
                source.source.clone(),
                base,
                http.replay_mib
                    .checked_mul(1024 * 1024)
                    .context("budget_overflow")?,
                spool,
            )?;
            source.seek(&banks.latest())?;
            let store = banks.clone();
            std::thread::spawn(move || {
                loop {
                    let result = (|| -> Result<()> {
                        let status = source.health()?;
                        store.set_health(status, None);
                        let mut batch = Vec::new();
                        for _ in 0..hivezilla_stream::bank_store::MAX_BATCH_EVENTS {
                            let Some(f) = source.next()? else { break };
                            let dep = if let Some(sha) = &f.envelope.event.payload_frame_sha256 {
                                if store.chunk(sha).is_none() {
                                    source.chunk(sha)?
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            batch.push((f, dep));
                        }
                        if !batch.is_empty() {
                            store.admit_batch(batch)?;
                        }
                        Ok(())
                    })();
                    if let Err(e) = result {
                        store.set_health("failed", Some(e.to_string()));
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            });
            let dummy = Store::open(
                format!("{}:0:-", banks.source.session_id),
                16 * 1024 * 1024 + 65552,
                None,
            )?;
            let mut a = api(&http, dummy)?;
            a.bank_only = true;
            a.banks = Some(BankApi {
                store: banks,
                origin: true,
                output: Some(output),
            });
            serve(http, a, true).await
        }
        Mode::Origin {
            http,
            output,
            start_slot,
            tail_slots,
            processed,
            start_sequence,
            tail_events,
        } => {
            let source = if processed {
                Source::open_processed(&output, start_sequence, tail_events.unwrap_or(256))?
            } else {
                let start_slot = if let Some(s) = start_slot {
                    s
                } else {
                    let cursor: serde_json::Value =
                        serde_json::from_slice(&bounded_file(&output.join("CURSOR.json"), 65536)?)?;
                    let tail = tail_slots.unwrap_or(256);
                    ensure!((1..=4096).contains(&tail), "invalid_tail_window");
                    cursor["last_slot"]
                        .as_u64()
                        .context("missing_commit_cursor")?
                        .saturating_sub(tail)
                        .max(1)
                };
                Source::open(&output, start_slot)?
            };
            let store = Store::open(
                source.anchor(),
                http.replay_mib
                    .checked_mul(1024 * 1024)
                    .context("budget_overflow")?,
                None,
            )?;
            let reader_store = store.clone();
            std::thread::spawn(move || {
                let mut source = source;
                loop {
                    match source.next_chunk() {
                        Ok(Some(c)) => {
                            if let Err(e) = reader_store.admit(c) {
                                reader_store.set_status("FAILED", Some(e.to_string()));
                                return;
                            }
                        }
                        Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                        Err(e) => {
                            reader_store.set_status("FAILED", Some(e.to_string()));
                            return;
                        }
                    }
                }
            });
            let a = api(&http, store)?;
            serve(http, a, true).await
        }
        Mode::Relay {
            http,
            upstream,
            upstream_token_file,
            upstream_ca,
            upstream_identity,
            spool,
            allow_query_api_key,
            public_blockhashes,
            public_max_requests,
            banks_upstream,
            banks_source_file,
            banks_spool,
            banks_replay_mib,
        } => {
            let u = Upstream {
                url: format!("{}/", upstream.trim_end_matches('/')),
                token: token(&upstream_token_file)?,
                ca: upstream_ca,
                identity: upstream_identity,
            };
            let client = u.client()?;
            let head = u.head(&client).await?;
            let base = head["latest_cursor"]
                .as_str()
                .context("missing_upstream_cursor")?
                .to_owned();
            let store = Store::open(
                base,
                http.replay_mib
                    .checked_mul(1024 * 1024)
                    .context("budget_overflow")?,
                Some(spool),
            )?;
            ensure!(
                (1..=128).contains(&public_max_requests),
                "invalid_public_request_budget"
            );
            let mut a = api(&http, store.clone())?;
            a.allow_query_api_key = allow_query_api_key;
            a.public_blockhashes = public_blockhashes;
            a.public_requests = Arc::new(Semaphore::new(public_max_requests));
            if let Some(url) = banks_upstream {
                use hivezilla_stream::{
                    bank_server::BankApi, bank_store::BankStore, bank_upstream,
                };
                let expected: blockzilla_bank_stream_v2::SourceIdentity = serde_json::from_slice(
                    &bounded_file(&banks_source_file.context("missing_banks_source")?, 65536)?,
                )?;
                let bu = Upstream {
                    url: format!("{}/", url.trim_end_matches('/')),
                    token: u.token.clone(),
                    ca: u.ca.clone(),
                    identity: u.identity.clone(),
                };
                let bc = bu.client()?;
                let base = blockzilla_bank_stream_v2::cursor(&expected.session_id, 0, "-");
                let banks = BankStore::open(
                    expected,
                    base,
                    banks_replay_mib
                        .checked_mul(1024 * 1024)
                        .context("budget_overflow")?,
                    banks_spool.context("missing_banks_spool")?,
                )?;
                a.banks = Some(BankApi {
                    store: banks.clone(),
                    origin: false,
                    output: None,
                });
                tokio::spawn(bank_upstream::run(bu, bc, banks));
            }
            tokio::spawn(u.run(client, store));
            serve(http, a, false).await
        }
    }
}
