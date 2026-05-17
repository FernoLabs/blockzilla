use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
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
use std::hint::black_box;
use std::io::{BufReader, Cursor, Seek};

static CAR_BYTES: &[u8] = include_bytes!("fixtures/epoch-822-biggest.car");
static LEGACY_CAR_BYTES: &[u8] = include_bytes!("fixtures/epoch-157-biggest.car");

fn read_all_blocks_n_times(car_bytes: &[u8], n: usize, read_rewards: bool) {
    let cursor = Cursor::new(car_bytes);
    let reader = BufReader::with_capacity(32 * 1024 * 1024, cursor);

    let mut car = CarBlockReader::with_capacity(reader, 8 * 1024 * 1024);
    car.skip_header().unwrap();

    // Save position after header
    let header_end_pos = car.reader.stream_position().unwrap();

    let mut group = if read_rewards {
        CarBlockGroup::new()
    } else {
        CarBlockGroup::without_rewards()
    };

    for _ in 0..n {
        // Reset to after header
        car.reader
            .seek(std::io::SeekFrom::Start(header_end_pos))
            .unwrap();

        loop {
            let ok = car.read_until_block_into(&mut group).unwrap();
            if !ok {
                break;
            }

            let mut it = group.transactions();
            let mut checksum = 0usize;
            while let Some(tx) = it.next_tx().expect("iterator failed") {
                checksum ^= consume_owned_tx(tx.0, tx.1);
            }
            black_box(checksum);
        }
    }
}

fn read_all_blocks_n_times_borrowed_metadata(car_bytes: &[u8], n: usize, read_rewards: bool) {
    let cursor = Cursor::new(car_bytes);
    let reader = BufReader::with_capacity(32 * 1024 * 1024, cursor);

    let mut car = CarBlockReader::with_capacity(reader, 8 * 1024 * 1024);
    car.skip_header().unwrap();

    let header_end_pos = car.reader.stream_position().unwrap();

    let mut group = if read_rewards {
        CarBlockGroup::new()
    } else {
        CarBlockGroup::without_rewards()
    };

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
            let mut checksum = 0usize;
            while let Some(tx) = it.next_tx().expect("iterator failed") {
                checksum ^= consume_borrowed_tx(&tx);
            }
            black_box(checksum);
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("read CAR 100 times", |b| {
        b.iter(|| {
            read_all_blocks_n_times(black_box(CAR_BYTES), 1, true);
        })
    });
    c.bench_function("read CAR 100 times no rewards", |b| {
        b.iter(|| {
            read_all_blocks_n_times(black_box(CAR_BYTES), 1, false);
        })
    });
    c.bench_function("read CAR 100 times borrowed metadata", |b| {
        b.iter(|| {
            read_all_blocks_n_times_borrowed_metadata(black_box(CAR_BYTES), 1, true);
        })
    });

    let samples = collect_metadata_samples(CAR_BYTES, 8192);
    let mut group = c.benchmark_group("metadata protobuf decode");
    group.throughput(Throughput::Elements(samples.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("prost owned", samples.len()),
        &samples,
        |b, samples| {
            b.iter(|| decode_prost_samples(black_box(samples)));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("quick-protobuf borrowed", samples.len()),
        &samples,
        |b, samples| {
            b.iter(|| decode_quick_samples(black_box(samples)));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("visitor selected", samples.len()),
        &samples,
        |b, samples| {
            b.iter(|| decode_visit_samples(black_box(samples)));
        },
    );
    group.finish();

    c.bench_function("read legacy CAR 100 times", |b| {
        b.iter(|| {
            read_all_blocks_n_times(black_box(LEGACY_CAR_BYTES), 1, true);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn collect_metadata_samples(car_bytes: &[u8], max_samples: usize) -> Vec<(u64, Vec<u8>)> {
    let cursor = Cursor::new(car_bytes);
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

        let mut prost = TransactionStatusMeta::default();
        decode_transaction_status_meta_into(tx.slot, &bytes, &mut prost).unwrap();
        let _ = decode_protobuf_transaction_status_meta_borrowed(&bytes).unwrap();

        samples.push((tx.slot, bytes));
    }

    assert!(
        !samples.is_empty(),
        "metadata decode benchmark fixture did not contain metadata samples"
    );
    samples
}

fn decode_prost_samples(samples: &[(u64, Vec<u8>)]) {
    let mut out = TransactionStatusMeta::default();
    for (slot, bytes) in samples {
        decode_transaction_status_meta_into(*slot, bytes, &mut out).unwrap();
        black_box(&out);
    }
}

fn decode_quick_samples(samples: &[(u64, Vec<u8>)]) {
    for (_, bytes) in samples {
        let out = decode_protobuf_transaction_status_meta_borrowed(bytes).unwrap();
        black_box(out);
    }
}

fn decode_visit_samples(samples: &[(u64, Vec<u8>)]) {
    for (_, bytes) in samples {
        let mut visitor = CountingMetaVisitor::default();
        visit_protobuf_transaction_status_meta(bytes, &mut visitor).unwrap();
        black_box(visitor.checksum);
    }
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
        out ^= usize::from(meta.err.is_some());
        out ^= meta.pre_balances.len();
        out ^= meta.post_balances.len();
        out ^= meta.inner_instructions.len();
        out ^= meta.log_messages.len();
        out ^= meta.pre_token_balances.len();
        out ^= meta.post_token_balances.len();
        out ^= meta.rewards.len();
        out ^= meta.loaded_writable_addresses.len();
        out ^= meta.loaded_readonly_addresses.len();
        out ^= usize::from(meta.return_data.is_some());
        out ^= usize::from(meta.compute_units_consumed.is_some());
        out ^= usize::from(meta.cost_units.is_some());
    }
    out
}

fn consume_borrowed_tx(tx: &of_car_reader::car_block_group::BorrowedTx<'_, '_>) -> usize {
    let mut out = tx.transaction.signatures.len() ^ static_key_count(tx.transaction);
    if let Some(meta) = &tx.metadata {
        out ^= usize::from(meta.err.is_some());
        out ^= meta.pre_balances.len();
        out ^= meta.post_balances.len();
        out ^= meta.inner_instructions.len();
        out ^= meta.log_messages.len();
        out ^= meta.pre_token_balances.len();
        out ^= meta.post_token_balances.len();
        out ^= meta.rewards.len();
        out ^= meta.loaded_writable_addresses.len();
        out ^= meta.loaded_readonly_addresses.len();
        out ^= usize::from(meta.return_data.is_some());
        out ^= usize::from(meta.compute_units_consumed.is_some());
        out ^= usize::from(meta.cost_units.is_some());
    }
    out
}

fn static_key_count(tx: &VersionedTransaction<'_>) -> usize {
    match &tx.message {
        VersionedMessage::Legacy(message) => message.account_keys.len(),
        VersionedMessage::V0(message) => message.account_keys.len(),
    }
}
