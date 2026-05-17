use anyhow::{Context, Result};
use axum::{Router, routing::post};
use clap::{Parser, Subcommand};
use of_car_reader::slot_ranges::write_slot_ranges_v2_raw;
use of_get_block_worker::archive::Archive;
use of_get_block_worker::index::build_slot_ranges_v2;
use of_get_block_worker::rpc::{self, RpcState};
use of_get_block_worker::source::SourceArgs;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

#[derive(Parser)]
#[command(name = "of-get-block-worker")]
#[command(about = "Serve Solana JSON-RPC responses from Old Faithful CAR files")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve a Solana JSON-RPC HTTP endpoint from CAR files and slot indexes.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8899")]
        bind: SocketAddr,
        /// Directory containing slot-index/epoch-N-slot-ranges-v2.raw files.
        #[arg(long)]
        index_dir: PathBuf,
        /// Optional upstream Solana JSON-RPC endpoint for methods not served locally.
        #[arg(long)]
        proxy_url: Option<String>,
        #[command(flatten)]
        source: SourceArgs,
    },

    /// Build a v2 slot range index with previousBlockhash side data.
    BuildSlotIndexV2 {
        #[arg(long)]
        epoch: u64,
        /// Output file, usually slot-index/epoch-N-slot-ranges-v2.raw.
        #[arg(long)]
        out: PathBuf,
        /// Base58 blockhash immediately before the first produced block in this epoch.
        #[arg(long)]
        seed_previous_blockhash: Option<String>,
        #[arg(long, default_value_t = of_slot_ranges::DEFAULT_MAX_BUCKET_PAYLOAD_BYTES)]
        max_bucket_payload_bytes: usize,
        #[arg(long)]
        allow_node_read_fallback: bool,
        #[command(flatten)]
        source: SourceArgs,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    match Cli::parse().command {
        Command::Serve {
            bind,
            index_dir,
            proxy_url,
            source,
        } => {
            let source = Arc::new(source.into_source()?);
            let archive = Arc::new(Archive::new(source, index_dir));
            let state = RpcState::new(archive, proxy_url)?;
            let app = Router::new()
                .route("/", post(rpc::handle_rpc))
                .with_state(state);
            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("bind {bind}"))?;
            tracing::info!("of-get-block-worker listening on http://{bind}");
            axum::serve(listener, app).await.context("serve JSON-RPC")?;
        }
        Command::BuildSlotIndexV2 {
            epoch,
            out,
            seed_previous_blockhash,
            max_bucket_payload_bytes,
            allow_node_read_fallback,
            source,
        } => {
            let source = Arc::new(source.into_source()?);
            let seed_previous_blockhash = seed_previous_blockhash
                .as_deref()
                .map(decode_blockhash)
                .transpose()?;
            let output = build_slot_ranges_v2(
                source,
                epoch,
                seed_previous_blockhash,
                max_bucket_payload_bytes,
                allow_node_read_fallback,
            )
            .await?;

            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            let mut file =
                std::fs::File::create(&out).with_context(|| format!("create {}", out.display()))?;
            write_slot_ranges_v2_raw(&mut file, &output.ranges)
                .with_context(|| format!("write {}", out.display()))?;

            println!(
                "epoch {}: wrote {} v2 slot range rows to {} (present slots {}, first {:?}, last {:?}, last blockhash {})",
                epoch,
                output.ranges.len(),
                out.display(),
                output.present_slots,
                output.first_present_slot,
                output.last_present_slot,
                bs58::encode(output.last_blockhash).into_string()
            );
        }
    }

    Ok(())
}

fn decode_blockhash(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode base58 blockhash {value}"))?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        anyhow::anyhow!("blockhash must be 32 bytes, got {}", bytes.len())
    })?;
    Ok(bytes)
}
