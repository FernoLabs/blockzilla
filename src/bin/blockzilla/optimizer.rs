use anyhow::Result;
use blockzilla::{
    block_stream::SolanaBlockStream,
};
use futures_util::io::AllowStdIo;
use std::{fs::File, time::Instant};
use tracing::info;

pub async fn run_car_optimizer(path: &str, output_dir: Option<String>) -> Result<()> {
    Ok(())
}
