use sysinfo::{System};
use tracing::info;

pub const LOG_INTERVAL_SECS: u64 = 2;
pub const HTTP_BUFFER_SIZE: usize = 4 * 1024 * 1024;
pub const TEMP_CHUNK_DIR: &str = ".epoch_chunks";

pub fn auto_chunk_capacity(max_threads: usize) -> usize {
    let mut sys = System::new_all();
    sys.refresh_memory();

    let usable = (sys.available_memory() as f64 * 0.90) as usize;
    let per_task = usable / max_threads.max(1);
    let est_bytes_per_entry = 100;
    let capacity = ((per_task / est_bytes_per_entry) as f64 * 0.6) as usize;
    let rounded = (capacity / 1000) * 1000;

    info!(
        "🧮 Auto-tuned CHUNK_CAPACITY = {} ({} MB/task, {} GB total, {} threads)",
        rounded,
        per_task / 1_000_000,
        usable as f64 / 1_000_000_000.0,
        max_threads
    );

    rounded
    //1_000_000
}
