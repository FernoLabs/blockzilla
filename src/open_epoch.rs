use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use clap::ValueEnum;
use futures::StreamExt;
use once_cell::sync::Lazy;
use reqwest::{Client, header};
use std::{
    env,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context as TaskCx, Poll},
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncRead, ReadBuf},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FetchMode {
    Offline,
    SafeNetwork,
    Network,
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

static POOL_SMALL: Lazy<Mutex<Vec<BytesMut>>> = Lazy::new(|| Mutex::new(Vec::new()));
static POOL_LARGE: Lazy<Mutex<Vec<BytesMut>>> = Lazy::new(|| Mutex::new(Vec::new()));
const POOL_SMALL_LIMIT: usize = 256;
const POOL_LARGE_LIMIT: usize = 128;
const SMALL_THRESHOLD: usize = 4 * 1024 * 1024;

#[inline]
async fn get_buf(cap: usize) -> BytesMut {
    let pool = if cap <= SMALL_THRESHOLD {
        &*POOL_SMALL
    } else {
        &*POOL_LARGE
    };
    let mut p = pool.lock().await;
    p.pop().unwrap_or_else(|| BytesMut::with_capacity(cap))
}
#[inline]
async fn return_buf(mut buf: BytesMut) {
    buf.clear();
    let pool = if buf.capacity() <= SMALL_THRESHOLD {
        &*POOL_SMALL
    } else {
        &*POOL_LARGE
    };
    let mut p = pool.lock().await;
    let lim = if buf.capacity() <= SMALL_THRESHOLD {
        POOL_SMALL_LIMIT
    } else {
        POOL_LARGE_LIMIT
    };
    if p.len() < lim {
        p.push(buf);
    }
}
pub async fn warmup_pools() {
    let mut s = POOL_SMALL.lock().await;
    for _ in 0..POOL_SMALL_LIMIT / 2 {
        s.push(BytesMut::with_capacity(SMALL_THRESHOLD));
    }
    let mut l = POOL_LARGE.lock().await;
    for _ in 0..POOL_LARGE_LIMIT / 2 {
        l.push(BytesMut::with_capacity(8 * 1024 * 1024));
    }
}

pub struct ChannelReader {
    rx: mpsc::Receiver<Bytes>,
    current: Option<Bytes>,
    done: bool,
}
impl ChannelReader {
    pub fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            current: None,
            done: false,
        }
    }
}
impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskCx<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if let Some(mut chunk) = self.current.take() {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk.split_to(n));
                if !chunk.is_empty() {
                    self.current = Some(chunk);
                }
                return Poll::Ready(Ok(()));
            }
            if self.done {
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut self.rx).poll_recv(cx) {
                Poll::Ready(Some(chunk)) => {
                    if !chunk.is_empty() {
                        self.current = Some(chunk);
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn build_h2_client() -> Result<Client> {
    Ok(Client::builder()
        .http2_adaptive_window(true)
        .http2_initial_stream_window_size(Some(256 * 1024 * 1024))
        .http2_initial_connection_window_size(Some(1024 * 1024 * 1024))
        .http2_keep_alive_interval(Some(Duration::from_secs(15)))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(64)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(600))
        .build()?)
}
async fn head_content_length(client: &Client, url: &str) -> Result<u64> {
    let r = client.head(url).send().await?;
    if !r.status().is_success() {
        return Err(anyhow!("HEAD failed: {}", r.status()));
    }
    r.headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .context("Missing Content-Length")
}

async fn stream_epoch_safe(url: String) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let client = build_h2_client()?;
    let total = head_content_length(&client, &url).await?;
    let (tx, rx) = mpsc::channel::<Bytes>(256);
    tracing::info!("🛠️ SafeNetwork(H2): {:.2} GB", total as f64 / 1e9);

    tokio::spawn(async move {
        let mut written = 0u64;
        let start = Instant::now();
        let mut last_log = Instant::now();
        let resp = match client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("SafeNetwork error: {e}");
                return;
            }
        };
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            if let Ok(b) = chunk {
                written += b.len() as u64;
                if tx.send(b).await.is_err() {
                    break;
                }
                if last_log.elapsed() >= Duration::from_secs(1) {
                    let mbps =
                        (written as f64 / 1_048_576.0) / start.elapsed().as_secs_f64().max(0.001);
                    tracing::info!(
                        "📦 streamed {} / ~{:.2} GB ({:.1} MB/s)",
                        written,
                        total as f64 / 1e9,
                        mbps
                    );
                    last_log = Instant::now();
                }
            } else {
                break;
            }
        }
        drop(tx);
    });
    Ok(Box::new(ChannelReader::new(rx)))
}

// Network: adaptive multi-client H2 with byte-based window + stash guard
async fn stream_epoch_h2_multi_safe(url: String) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let slice_mb = env_u64("BZ_SLICE_MB", 16);
    let workers_min = env_usize("BZ_WORKERS_MIN", 28);
    let workers_max = env_usize("BZ_WORKERS_MAX", 64);
    let clients_n = env_usize("BZ_H2_CLIENTS", 8);
    let target_mbps = env_f64("BZ_TARGET_MBPS", 900.0);
    let alpha = env_f64("BZ_EWMA_ALPHA", 0.25);
    let adapt_interval_s = env_u64("BZ_ADAPT_INTERVAL_S", 4);
    let mem_cap_mb = env_usize("BZ_MEM_CAP_MB", 2048);
    let ring_slots_pow2 = env_usize("BZ_RING_SLOTS", 8192);
    let window_mb = env_u64("BZ_WINDOW_MB", 1024);
    let stash_guard_mb = env_u64("BZ_STASH_GUARD_MB", 768);

    let slice_bytes = slice_mb * 1024 * 1024;
    let unit_bytes = 1024 * 1024usize;
    let cap_units = (mem_cap_mb * 1024 * 1024) / unit_bytes;
    let ring_cap = ring_slots_pow2.next_power_of_two().max(1024);
    let ring_mask = ring_cap - 1;
    let adapt_interval = Duration::from_secs(adapt_interval_s);
    let window_bytes = window_mb * 1024 * 1024;
    let stash_guard = (stash_guard_mb * 1024 * 1024) as usize;

    let clients: Arc<Vec<Client>> =
        Arc::new((0..clients_n).map(|_| build_h2_client().unwrap()).collect());
    let total = head_content_length(&clients[0], &url).await?;
    let total_slices = total.div_ceil(slice_bytes) as u64;

    let active_workers = Arc::new(AtomicU64::new(workers_min as u64));
    let next_index = Arc::new(AtomicU64::new(0));
    let drained_index = Arc::new(AtomicU64::new(0));
    let delivered_bytes = Arc::new(AtomicU64::new(0));
    let stash_bytes = Arc::new(AtomicUsize::new(0));
    let mem_sem = Arc::new(Semaphore::new(cap_units));
    let stop = Arc::new(AtomicBool::new(false));

    type TxItem = (u64, Bytes, OwnedSemaphorePermit);
    let (tx_prod, mut rx_prod) = mpsc::channel::<TxItem>(512);
    let (tx_stream, rx_stream) = mpsc::channel::<Bytes>(512);

    tracing::info!(
        "🚀 Network(H2Adaptive): {:.2} GB | slice={} MB | workers={}..{} | mem={} MiB | clients={} | ring={} | window={} MB | stash_guard={} MB",
        total as f64 / 1e9,
        slice_mb,
        workers_min,
        workers_max,
        mem_cap_mb,
        clients.len(),
        ring_cap,
        window_mb,
        stash_guard_mb
    );

    for wid in 0..workers_max {
        let clients = clients.clone();
        let url = url.clone();
        let tx = tx_prod.clone();
        let active = active_workers.clone();
        let next_idx = next_index.clone();
        let drained = drained_index.clone();
        let stash_ref = stash_bytes.clone();
        let mem_sem = mem_sem.clone();
        let stop_flag = stop.clone();

        tokio::spawn(async move {
            let client = &clients[wid % clients.len()];
            while !stop_flag.load(Ordering::Relaxed) {
                if wid as u64 >= active.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                // stash guard: pause if backlog too high
                while stash_ref.load(Ordering::Relaxed) > stash_guard {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    if stop_flag.load(Ordering::Relaxed) {
                        return;
                    }
                }

                // schedule contiguous index
                let idx = loop {
                    let cur = next_idx.load(Ordering::Relaxed);
                    if cur >= total_slices {
                        break cur;
                    }
                    if next_idx
                        .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        break cur;
                    }
                };
                if idx >= total_slices {
                    break;
                }

                // enforce byte-based reordering window
                loop {
                    let drained_i = drained.load(Ordering::Relaxed);
                    let ahead_bytes =
                        (idx.saturating_sub(drained_i)) as u128 * (slice_bytes as u128);
                    if ahead_bytes <= window_bytes as u128 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    if stop_flag.load(Ordering::Relaxed) {
                        return;
                    }
                }

                // compute range
                let start = idx * slice_bytes;
                let need = if idx == total_slices - 1 {
                    (total - start) as usize
                } else {
                    slice_bytes as usize
                };

                // memory permit
                let units = need.div_ceil(unit_bytes).max(1) as u32;
                let permit = match mem_sem.clone().acquire_many_owned(units).await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // perform request
                let range = if idx == total_slices - 1 {
                    format!("bytes={}-{}", start, start + need as u64 - 1)
                } else {
                    format!("bytes={}-{}", start, start + slice_bytes - 1)
                };
                let mut sent = false;
                for _ in 0..3 {
                    let resp = client
                        .get(&url)
                        .header(header::RANGE, &range)
                        .header(header::ACCEPT_ENCODING, "identity")
                        .send()
                        .await;
                    if let Ok(r) = resp
                        && (r.status().is_success() || r.status() == 206)
                    {
                        let mut buf = get_buf(need).await;
                        buf.reserve(need.saturating_sub(buf.capacity()));
                        let mut s = r.bytes_stream();
                        let mut ok = true;
                        while let Some(c) = s.next().await {
                            match c {
                                Ok(b) => buf.extend_from_slice(&b),
                                Err(_) => {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if ok && buf.len() == need {
                            let b = buf.freeze();
                            stash_ref.fetch_add(need, Ordering::Relaxed);
                            if tx.send((idx, b, permit)).await.is_err() {
                                return;
                            }
                            sent = true;
                            break;
                        } else {
                            return_buf(buf).await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                if !sent {
                    //drop(permit);
                }
            }
        });
    }
    drop(tx_prod);

    {
        let workers_ref = active_workers.clone();
        let delivered = delivered_bytes.clone();
        let stash_ref = stash_bytes.clone();
        let stop_ref = stop.clone();
        tokio::spawn(async move {
            let mut ema_mbps = 0.0;
            let mut ema_stash = 0.0;
            let mut last = delivered.load(Ordering::Relaxed);
            let mut t = Instant::now();
            loop {
                tokio::time::sleep(adapt_interval).await;
                if stop_ref.load(Ordering::Relaxed) {
                    break;
                }
                let now = Instant::now();
                let dt = now.duration_since(t).as_secs_f64().max(0.001);
                t = now;
                let cur = delivered.load(Ordering::Relaxed);
                let delta = cur.saturating_sub(last);
                last = cur;
                let inst = (delta as f64 / 1_048_576.0) / dt;
                ema_mbps = alpha * inst + (1.0 - alpha) * ema_mbps;
                let stash_mb = (stash_ref.load(Ordering::Relaxed) as f64) / (1024.0 * 1024.0);
                ema_stash = alpha * stash_mb + (1.0 - alpha) * ema_stash;
                let mut w = workers_ref.load(Ordering::Relaxed);
                if ema_stash > 256.0 && w > workers_min as u64 {
                    w = w.saturating_sub(2).max(workers_min as u64);
                    workers_ref.store(w, Ordering::Relaxed);
                    tracing::info!("🧠 stash high → workers={} (stash={:.0}MB)", w, ema_stash);
                    continue;
                }
                if ema_mbps < target_mbps * 0.95 && ema_stash < 96.0 && w < workers_max as u64 {
                    w = (w + 2).min(workers_max as u64);
                    workers_ref.store(w, Ordering::Relaxed);
                    tracing::info!("⚙️ scale up → workers={} (stash={:.0}MB)", w, ema_stash);
                    continue;
                }
                if ema_mbps > target_mbps * 1.25 && ema_stash > 128.0 && w > workers_min as u64 {
                    w = w.saturating_sub(1).max(workers_min as u64);
                    workers_ref.store(w, Ordering::Relaxed);
                    tracing::info!("🧩 scale down → workers={} (stash={:.0}MB)", w, ema_stash);
                }
            }
        });
    }

    tokio::spawn({
        let delivered = delivered_bytes.clone();
        let stash_ref = stash_bytes.clone();
        let drained = drained_index.clone();
        let stop_ref = stop.clone();
        async move {
            struct Held {
                buf: Bytes,
                permit: OwnedSemaphorePermit,
                len: usize,
            }
            let mut ring: Vec<Option<Held>> = (0..ring_cap).map(|_| None).collect();
            let mut next_idx = 0u64;
            let mut written = 0u64;
            let start = Instant::now();
            let mut last_log = Instant::now();
            while let Some((idx, buf, permit)) = rx_prod.recv().await {
                let len = buf.len();
                let slot = (idx as usize) & ring_mask;
                ring[slot] = Some(Held { buf, permit, len });
                while let Some(Held { buf, permit, len }) =
                    ring[(next_idx as usize) & ring_mask].take()
                {
                    if tx_stream.send(buf).await.is_err() {
                        drop(permit);
                        drop(tx_stream);
                        stop_ref.store(true, Ordering::Relaxed);
                        return;
                    }
                    written += len as u64;
                    delivered.store(written, Ordering::Relaxed);
                    stash_ref.fetch_sub(len, Ordering::Relaxed);
                    drop(permit);
                    next_idx += 1;
                    drained.store(next_idx, Ordering::Relaxed);
                    if next_idx >= total_slices {
                        drop(tx_stream);
                        stop_ref.store(true, Ordering::Relaxed);
                        return;
                    }
                }
                if last_log.elapsed() >= Duration::from_secs(1) {
                    last_log = Instant::now();
                    let mbps =
                        (written as f64 / 1_048_576.0) / start.elapsed().as_secs_f64().max(0.001);
                    let stash_mb = stash_ref.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
                    tracing::info!(
                        "📦 {} / ~{:.2} GB ({:.1} MB/s) | stash: {:.0} MB",
                        written,
                        total as f64 / 1e9,
                        mbps,
                        stash_mb
                    );
                }
            }
        }
    });

    Ok(Box::new(ChannelReader::new(rx_stream)))
}

pub async fn open_epoch(
    epoch: u64,
    cache_dir: impl AsRef<Path>,
    mode: FetchMode,
) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    fs::create_dir_all(&cache_dir).await?;
    let file = format!("epoch-{epoch}.car");
    let cached_path = cache_dir.as_ref().join(&file);
    let url = format!("https://files.old-faithful.net/{epoch}/{file}");

    match mode {
        FetchMode::Offline => {
            if cached_path.exists() {
                Ok(Box::new(tokio::fs::File::open(&cached_path).await?))
            } else {
                Err(anyhow!("Cache missing: {}", cached_path.display()))
            }
        }
        FetchMode::SafeNetwork => stream_epoch_safe(url).await,
        FetchMode::Network => stream_epoch_h2_multi_safe(url).await,
    }
}
