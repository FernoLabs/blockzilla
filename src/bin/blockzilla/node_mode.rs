use anyhow::Result;
use blockzilla::CarStream;
use futures_util::io::AllowStdIo;
use std::{fs::File, time::Instant};
use tracing::info;

pub async fn run_node_mode(path: &str) -> Result<()> {
    info!("🔄 Reading CAR file: {path}");
    let start = Instant::now();

    let file = File::open(path)?;
    let reader = AllowStdIo::new(file);
    let mut stream = CarStream::new(reader).await?;

    info!("Header: {:?}", stream.header());

    let mut count: u64 = 0;
    let mut last_log = Instant::now();

    while let Some((_cid, _node)) = stream.next().await? {
        count += 1;

        if count.is_multiple_of(10_000) || last_log.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed();
            let rate = count as f64 / elapsed.as_secs_f64().max(0.001);
            info!(
                "[{:>10}] nodes parsed in {:>6.2?} ({:.2} nodes/s)",
                count, elapsed, rate
            );
            last_log = Instant::now();
        }
    }

    let total = start.elapsed();
    let rate = count as f64 / total.as_secs_f64().max(0.001);
    info!(
        "✅ Done. Parsed {count} nodes in {:.2?} ({:.2} nodes/s)",
        total, rate
    );

    Ok(())
}
