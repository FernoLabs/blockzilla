use of_car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    confirmed_block::TransactionStatusMeta,
    metadata_decoder::{
        TokenBalanceVisit, TransactionStatusMetaVisitor, ZstdReusableDecoder,
        decode_protobuf_transaction_status_meta_borrowed, decode_transaction_status_meta_into,
        visit_protobuf_transaction_status_meta,
    },
    reconstruct::RawNode,
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    io::{BufReader, Cursor, Seek},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

static CAR_BYTES: &[u8] = include_bytes!("../../benches/fixtures/epoch-822-biggest.car");

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static REALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            REALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            REALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[derive(Clone, Copy)]
struct AllocSnapshot {
    alloc_count: u64,
    alloc_bytes: u64,
    realloc_count: u64,
    realloc_bytes: u64,
    dealloc_count: u64,
}

impl AllocSnapshot {
    fn now() -> Self {
        Self {
            alloc_count: ALLOC_COUNT.load(Ordering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            realloc_count: REALLOC_COUNT.load(Ordering::Relaxed),
            realloc_bytes: REALLOC_BYTES.load(Ordering::Relaxed),
            dealloc_count: DEALLOC_COUNT.load(Ordering::Relaxed),
        }
    }

    fn delta_from(self, start: Self) -> Self {
        Self {
            alloc_count: self.alloc_count - start.alloc_count,
            alloc_bytes: self.alloc_bytes - start.alloc_bytes,
            realloc_count: self.realloc_count - start.realloc_count,
            realloc_bytes: self.realloc_bytes - start.realloc_bytes,
            dealloc_count: self.dealloc_count - start.dealloc_count,
        }
    }
}

fn main() {
    let mode = arg_value("--mode").unwrap_or_else(|| "borrowed-read".to_string());
    let iters = arg_value("--iters")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100);
    let samples = arg_value("--samples")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8192);

    let start_allocs = AllocSnapshot::now();
    let started_at = Instant::now();
    let checksum = match mode.as_str() {
        "owned-read" => read_all_blocks_n_times(iters, true),
        "owned-read-no-rewards" => read_all_blocks_n_times(iters, false),
        "borrowed-read" => read_all_blocks_n_times_borrowed_metadata(iters, true),
        "quick-meta" => {
            let samples = collect_metadata_samples(samples);
            let setup_allocs = AllocSnapshot::now();
            let started_at = Instant::now();
            let checksum = decode_quick_samples(&samples, iters);
            print_result(&mode, iters, checksum, started_at, setup_allocs);
            return;
        }
        "prost-meta" => {
            let samples = collect_metadata_samples(samples);
            let setup_allocs = AllocSnapshot::now();
            let started_at = Instant::now();
            let checksum = decode_prost_samples(&samples, iters);
            print_result(&mode, iters, checksum, started_at, setup_allocs);
            return;
        }
        "visit-meta" => {
            let samples = collect_metadata_samples(samples);
            let setup_allocs = AllocSnapshot::now();
            let started_at = Instant::now();
            let checksum = decode_visit_samples(&samples, iters);
            print_result(&mode, iters, checksum, started_at, setup_allocs);
            return;
        }
        other => {
            eprintln!(
                "unknown --mode {other}; expected owned-read, owned-read-no-rewards, borrowed-read, quick-meta, prost-meta, or visit-meta"
            );
            std::process::exit(2);
        }
    };
    print_result(&mode, iters, checksum, started_at, start_allocs);
}

fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
    }
    None
}

fn print_result(
    mode: &str,
    iters: usize,
    checksum: usize,
    started_at: Instant,
    start_allocs: AllocSnapshot,
) {
    let elapsed = started_at.elapsed().as_secs_f64();
    let allocs = AllocSnapshot::now().delta_from(start_allocs);
    println!(
        "mode={mode} iters={iters} elapsed_s={elapsed:.6} iter_s={:.2} checksum={checksum} alloc_count={} alloc_mib={:.3} realloc_count={} realloc_mib={:.3} dealloc_count={}",
        iters as f64 / elapsed,
        allocs.alloc_count,
        allocs.alloc_bytes as f64 / (1024.0 * 1024.0),
        allocs.realloc_count,
        allocs.realloc_bytes as f64 / (1024.0 * 1024.0),
        allocs.dealloc_count
    );
}

fn read_all_blocks_n_times(n: usize, read_rewards: bool) -> usize {
    let cursor = Cursor::new(CAR_BYTES);
    let reader = BufReader::with_capacity(32 * 1024 * 1024, cursor);

    let mut car = CarBlockReader::with_capacity(reader, 8 * 1024 * 1024);
    car.skip_header().unwrap();
    let header_end_pos = car.reader.stream_position().unwrap();

    let mut group = if read_rewards {
        CarBlockGroup::new()
    } else {
        CarBlockGroup::without_rewards()
    };
    let mut checksum = 0usize;

    for _ in 0..n {
        car.reader
            .seek(std::io::SeekFrom::Start(header_end_pos))
            .unwrap();

        loop {
            let ok = car.read_until_block_into(&mut group).unwrap();
            if !ok {
                break;
            }

            let mut it = group.transactions();
            while let Some(tx) = it.next_tx().expect("iterator failed") {
                checksum ^= consume_owned_tx(tx.0, tx.1);
            }
        }
    }

    black_box(checksum)
}

fn read_all_blocks_n_times_borrowed_metadata(n: usize, read_rewards: bool) -> usize {
    let cursor = Cursor::new(CAR_BYTES);
    let reader = BufReader::with_capacity(32 * 1024 * 1024, cursor);

    let mut car = CarBlockReader::with_capacity(reader, 8 * 1024 * 1024);
    car.skip_header().unwrap();
    let header_end_pos = car.reader.stream_position().unwrap();

    let mut group = if read_rewards {
        CarBlockGroup::new()
    } else {
        CarBlockGroup::without_rewards()
    };
    let mut checksum = 0usize;

    for _ in 0..n {
        car.reader
            .seek(std::io::SeekFrom::Start(header_end_pos))
            .unwrap();

        loop {
            let ok = car.read_until_block_into(&mut group).unwrap();
            if !ok {
                break;
            }

            let mut it = group.transactions_borrowed_metadata();
            while let Some(tx) = it.next_tx().expect("iterator failed") {
                checksum ^= consume_borrowed_tx(&tx);
            }
        }
    }

    black_box(checksum)
}

fn collect_metadata_samples(max_samples: usize) -> Vec<(u64, Vec<u8>)> {
    let cursor = Cursor::new(CAR_BYTES);
    let mut car = CarBlockReader::with_capacity(cursor, 32 * 1024 * 1024);
    car.skip_header().unwrap();

    let mut scratch = Vec::with_capacity(1 << 20);
    let mut zstd = ZstdReusableDecoder::new();
    let mut samples = Vec::new();

    while samples.len() < max_samples {
        let Some(node) = car.read_lossless_node_with_scratch(&mut scratch).unwrap() else {
            break;
        };
        let RawNode::Transaction(tx) = node else {
            continue;
        };
        if tx.metadata.data.is_empty() || !tx.metadata.next.is_empty() {
            continue;
        }
        let bytes = if zstd.decompress_if_zstd(&tx.metadata.data).unwrap() {
            zstd.output().to_vec()
        } else {
            tx.metadata.data
        };

        samples.push((tx.slot, bytes));
    }

    assert!(
        !samples.is_empty(),
        "fixture did not contain metadata samples"
    );
    samples
}

fn decode_prost_samples(samples: &[(u64, Vec<u8>)], iters: usize) -> usize {
    let mut out = TransactionStatusMeta::default();
    let mut checksum = 0usize;
    for _ in 0..iters {
        for (slot, bytes) in samples {
            decode_transaction_status_meta_into(*slot, bytes, &mut out).unwrap();
            checksum ^= consume_owned_meta(&out);
        }
    }
    black_box(checksum)
}

fn decode_quick_samples(samples: &[(u64, Vec<u8>)], iters: usize) -> usize {
    let mut checksum = 0usize;
    for _ in 0..iters {
        for (_, bytes) in samples {
            let out = decode_protobuf_transaction_status_meta_borrowed(bytes).unwrap();
            checksum ^= consume_borrowed_meta(&out);
        }
    }
    black_box(checksum)
}

fn decode_visit_samples(samples: &[(u64, Vec<u8>)], iters: usize) -> usize {
    let mut checksum = 0usize;
    for _ in 0..iters {
        for (_, bytes) in samples {
            let mut visitor = CountingMetaVisitor::default();
            visit_protobuf_transaction_status_meta(bytes, &mut visitor).unwrap();
            checksum ^= visitor.checksum;
        }
    }
    black_box(checksum)
}

#[derive(Default)]
struct CountingMetaVisitor {
    checksum: usize,
}

impl<'a> TransactionStatusMetaVisitor<'a> for CountingMetaVisitor {
    #[inline]
    fn wants_status_error(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_log_messages(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        true
    }

    #[inline]
    fn status_ok(&mut self) {
        self.checksum ^= 1;
    }

    #[inline]
    fn status_error(&mut self, err: &'a [u8]) {
        self.checksum ^= err.len();
    }

    #[inline]
    fn fee(&mut self, fee: u64) {
        self.checksum ^= fee as usize;
    }

    #[inline]
    fn pre_balance(&mut self, index: usize, lamports: u64) {
        self.checksum ^= index ^ lamports as usize;
    }

    #[inline]
    fn post_balance(&mut self, index: usize, lamports: u64) {
        self.checksum ^= index ^ lamports as usize;
    }

    #[inline]
    fn log_message(&mut self, message: &'a str) {
        self.checksum ^= message.len();
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        self.checksum ^= token_balance_checksum(balance);
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        self.checksum ^= token_balance_checksum(balance);
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'a [u8]) {
        self.checksum ^= address.len();
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'a [u8]) {
        self.checksum ^= address.len();
    }

    #[inline]
    fn compute_units_consumed(&mut self, units: u64) {
        self.checksum ^= units as usize;
    }

    #[inline]
    fn cost_units(&mut self, units: u64) {
        self.checksum ^= units as usize;
    }
}

#[inline]
fn token_balance_checksum(balance: TokenBalanceVisit<'_>) -> usize {
    let mut checksum = balance.account_index as usize
        ^ balance.mint.len()
        ^ balance.owner.len()
        ^ balance.program_id.len();
    if let Some(amount) = balance.ui_token_amount {
        checksum ^= amount.decimals as usize;
        checksum ^= amount.amount.len();
        checksum ^= amount.ui_amount_string.len();
    }
    checksum
}

fn consume_owned_tx(tx: &VersionedTransaction<'_>, meta: Option<&TransactionStatusMeta>) -> usize {
    let mut out = tx.signatures.len() ^ static_key_count(tx);
    if let Some(meta) = meta {
        out ^= consume_owned_meta(meta);
    }
    out
}

fn consume_borrowed_tx(tx: &of_car_reader::car_block_group::BorrowedTx<'_, '_>) -> usize {
    let mut out = tx.transaction.signatures.len() ^ static_key_count(tx.transaction);
    if let Some(meta) = &tx.metadata {
        out ^= consume_borrowed_meta(meta);
    }
    out
}

fn consume_owned_meta(meta: &TransactionStatusMeta) -> usize {
    usize::from(meta.err.is_some())
        ^ meta.pre_balances.len()
        ^ meta.post_balances.len()
        ^ meta.inner_instructions.len()
        ^ meta.log_messages.len()
        ^ meta.pre_token_balances.len()
        ^ meta.post_token_balances.len()
        ^ meta.rewards.len()
        ^ meta.loaded_writable_addresses.len()
        ^ meta.loaded_readonly_addresses.len()
        ^ usize::from(meta.return_data.is_some())
        ^ usize::from(meta.compute_units_consumed.is_some())
        ^ usize::from(meta.cost_units.is_some())
}

fn consume_borrowed_meta(
    meta: &of_car_reader::metadata_decoder::BorrowedTransactionStatusMetaView<'_>,
) -> usize {
    usize::from(meta.err.is_some())
        ^ meta.pre_balances.len()
        ^ meta.post_balances.len()
        ^ meta.inner_instructions.len()
        ^ meta.log_messages.len()
        ^ meta.pre_token_balances.len()
        ^ meta.post_token_balances.len()
        ^ meta.rewards.len()
        ^ meta.loaded_writable_addresses.len()
        ^ meta.loaded_readonly_addresses.len()
        ^ usize::from(meta.return_data.is_some())
        ^ usize::from(meta.compute_units_consumed.is_some())
        ^ usize::from(meta.cost_units.is_some())
}

fn static_key_count(tx: &VersionedTransaction<'_>) -> usize {
    match &tx.message {
        VersionedMessage::Legacy(message) => message.account_keys.len(),
        VersionedMessage::V0(message) => message.account_keys.len(),
    }
}
