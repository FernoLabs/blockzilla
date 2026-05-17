use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    hint::black_box,
    io::{BufReader, Cursor},
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use blockzilla_format::{
    CompactLogStream, CompactPubkey, KeyIndex, KeyStore, Leb128, LogEvent,
    WINCODE_LOG_ARCHIVE_KEYS_FREQUENCY_SORTED, WINCODE_LOG_ARCHIVE_V2_VERSION,
    WincodeLogArchiveHeaderV2, WincodeLogArchiveV2, WincodeTxLogRange, parse_logs,
    program_logs::{
        ProgramLog, account_compression, address_lookup_table, associated_token_account, loader_v3,
        loader_v4, memo, record, stake, system_program, token, token_2022, transfer_hook,
        zk_elgamal_proof,
    },
};
use blockzilla_log_parser::{ParsedLogLine, parse_line};
use clap::Parser;
use data_encoding::BASE64;
use of_car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    confirmed_block::TransactionStatusMeta,
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use solana_pubkey::Pubkey;

static CAR_BYTES: &[u8] = include_bytes!(
    "../../../crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car"
);
const COMPUTE_BUDGET_ID: &str = "ComputeBudget111111111111111111111111111111";
const UNKNOWN_TOP: usize = 30;
type WincodeVarintConfig = wincode::config::Configuration<
    true,
    { wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED },
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    wincode::int_encoding::VarInt,
>;
type WincodeLeb128Config = wincode::config::Configuration<
    true,
    { wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED },
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    Leb128,
>;

#[derive(Parser, Debug)]
#[command(
    name = "big-block-log-bench",
    about = "Measure raw vs compact log storage on the biggest-block benchmark fixture"
)]
struct Args {
    /// Number of parse-only iterations.
    #[arg(long, default_value_t = 1_000)]
    iters: usize,

    /// Number of serialize/decode iterations.
    #[arg(long, default_value_t = 200)]
    serialize_iters: usize,

    /// Optional markdown report path.
    #[arg(long)]
    markdown: Option<PathBuf>,
}

#[derive(Default)]
struct FixtureLogs {
    blocks: u64,
    txs: u64,
    txs_with_logs: u64,
    logs: Vec<String>,
    tx_log_ranges: Vec<WincodeTxLogRange>,
    keys: BTreeSet<[u8; 32]>,
    key_counts: BTreeMap<[u8; 32], u64>,
}

impl FixtureLogs {
    fn ensure_key(&mut self, key: [u8; 32]) {
        self.keys.insert(key);
        self.key_counts.entry(key).or_default();
    }

    fn record_key(&mut self, key: [u8; 32]) {
        self.keys.insert(key);
        *self.key_counts.entry(key).or_default() += 1;
    }

    fn ensure_pubkey_text(&mut self, text: &str) {
        if let Ok(pubkey) = Pubkey::from_str(text) {
            self.ensure_key(*pubkey.as_array());
        }
    }

    fn record_pubkey_text(&mut self, text: &str) {
        if let Ok(pubkey) = Pubkey::from_str(text) {
            self.record_key(*pubkey.as_array());
        }
    }

    fn keys_lexical(&self) -> Vec<[u8; 32]> {
        self.keys.iter().copied().collect()
    }

    fn keys_by_frequency(&self) -> Vec<[u8; 32]> {
        let mut keys = self.keys_lexical();
        keys.sort_unstable_by(|a, b| {
            self.key_counts
                .get(b)
                .copied()
                .unwrap_or_default()
                .cmp(&self.key_counts.get(a).copied().unwrap_or_default())
                .then_with(|| a.cmp(b))
        });
        keys
    }
}

struct BenchTiming {
    iters: usize,
    elapsed: Duration,
    checksum: usize,
}

struct Report {
    fixture_bytes: usize,
    blocks: u64,
    txs: u64,
    txs_with_logs: u64,
    log_lines: usize,
    raw_utf8_bytes: usize,
    raw_wincode_bytes: u64,
    raw_wincode_varint_bytes: u64,
    raw_postcard_bytes: u64,
    compact_wincode_bytes: u64,
    compact_wincode_varint_bytes: u64,
    compact_postcard_bytes: u64,
    frequency_compact_wincode_varint_bytes: u64,
    frequency_compact_wincode_leb128_bytes: u64,
    frequency_compact_postcard_bytes: u64,
    archive_lex_wincode_varint_bytes: u64,
    archive_frequency_wincode_varint_bytes: u64,
    archive_frequency_wincode_leb128_bytes: u64,
    archive_frequency_postcard_bytes: u64,
    archive_key_registry_bytes: usize,
    archive_tx_log_ranges: usize,
    compact_events_wincode_bytes: u64,
    compact_strings_wincode_bytes: u64,
    compact_data_wincode_bytes: u64,
    string_table_entries: usize,
    string_table_bytes: usize,
    data_table_arrays: usize,
    data_table_bytes: usize,
    key_count: usize,
    compact_events: usize,
    top_level_known_events: usize,
    plain_or_unparsed_events: usize,
    unknown_program_payload_events: usize,
    parser_kinds: BTreeMap<&'static str, u64>,
    unknown_payloads: UnknownPayloadReport,
    parse_timing: BenchTiming,
    parse_frequency_timing: BenchTiming,
    serialize_timing: BenchTiming,
    archive_wincode_varint_serialize_timing: BenchTiming,
    archive_wincode_leb128_serialize_timing: BenchTiming,
    archive_postcard_serialize_timing: BenchTiming,
    archive_wincode_varint_decode_timing: BenchTiming,
    archive_wincode_leb128_decode_timing: BenchTiming,
    archive_wincode_leb128_stream_decode_timing: BenchTiming,
    archive_postcard_decode_timing: BenchTiming,
    car_read_timing: BenchTiming,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let fixture = collect_fixture_logs()?;
    let keys = fixture.keys_lexical();
    let key_index = KeyIndex::build(keys.clone());
    let key_store = KeyStore { keys: keys.clone() };
    let lexical_archive =
        build_log_archive(&fixture, keys, parse_logs(&fixture.logs, &key_index), false)?;

    let frequency_keys = fixture.keys_by_frequency();
    let frequency_key_index = KeyIndex::build(frequency_keys.clone());
    let frequency_archive = build_log_archive(
        &fixture,
        frequency_keys,
        parse_logs(&fixture.logs, &frequency_key_index),
        true,
    )?;

    let report = build_report(
        &args,
        &fixture,
        &key_index,
        &key_store,
        &lexical_archive,
        &frequency_key_index,
        &frequency_archive,
    )?;
    print_report(&report);

    if let Some(path) = &args.markdown {
        fs::write(path, render_markdown(&report))
            .with_context(|| format!("write {}", path.display()))?;
    }

    Ok(())
}

fn build_log_archive(
    fixture: &FixtureLogs,
    keys: Vec<[u8; 32]>,
    logs: CompactLogStream,
    frequency_sorted_keys: bool,
) -> Result<WincodeLogArchiveV2> {
    let flags = if frequency_sorted_keys {
        WINCODE_LOG_ARCHIVE_KEYS_FREQUENCY_SORTED
    } else {
        0
    };

    Ok(WincodeLogArchiveV2 {
        header: WincodeLogArchiveHeaderV2 {
            version: WINCODE_LOG_ARCHIVE_V2_VERSION,
            flags,
            block_count: fixture.blocks,
            tx_count: fixture.txs,
            tx_with_logs: fixture.txs_with_logs,
            log_line_count: fixture.logs.len() as u64,
        },
        keys,
        tx_log_ranges: fixture.tx_log_ranges.clone(),
        logs,
    })
}

fn collect_fixture_logs() -> Result<FixtureLogs> {
    let mut car = CarBlockReader::with_capacity(CAR_BYTES, 32 << 20);
    car.skip_header().context("skip benchmark CAR header")?;

    let mut group = CarBlockGroup::new();
    let mut out = FixtureLogs::default();

    insert_known_program_ids(&mut out);

    loop {
        let has_block = car
            .read_until_block_into(&mut group)
            .context("read benchmark block")?;
        if !has_block {
            break;
        }

        out.blocks += 1;
        let mut it = group.transactions();
        while let Some((tx, meta)) = it.next_tx().context("decode benchmark transaction")? {
            let tx_index = out.txs as u32;
            out.txs += 1;
            collect_tx_keys(tx, &mut out);

            let Some(meta) = meta else {
                continue;
            };
            collect_meta_keys(meta, &mut out);

            if !meta.log_messages_none && !meta.log_messages.is_empty() {
                let start = out.logs.len();
                for line in &meta.log_messages {
                    collect_pubkeys_from_line(line, &mut out);
                    out.logs.push(line.clone());
                }
                let end = out.logs.len();
                if end > start {
                    out.txs_with_logs += 1;
                    out.tx_log_ranges.push(WincodeTxLogRange {
                        tx_index,
                        start: u32::try_from(start).expect("fixture log start exceeds u32"),
                        count: u32::try_from(end - start).expect("fixture log count exceeds u32"),
                    });
                }
            }
        }
    }

    Ok(out)
}

fn insert_known_program_ids(out: &mut FixtureLogs) {
    for id in [
        system_program::STR_ID,
        token::STR_ID,
        token_2022::STR_ID,
        associated_token_account::STR_ID,
        address_lookup_table::STR_ID,
        loader_v3::STR_ID,
        loader_v3::V1_STR_ID,
        loader_v3::V2_STR_ID,
        loader_v4::STR_ID,
        memo::STR_ID,
        record::STR_ID,
        stake::STR_ID,
        transfer_hook::STR_ID,
        account_compression::STR_ID,
        zk_elgamal_proof::STR_ID,
        COMPUTE_BUDGET_ID,
    ] {
        out.ensure_pubkey_text(id);
    }
}

fn collect_tx_keys(tx: &VersionedTransaction<'_>, out: &mut FixtureLogs) {
    match &tx.message {
        VersionedMessage::Legacy(message) => {
            for key in &message.account_keys {
                out.record_key(**key);
            }
        }
        VersionedMessage::V0(message) => {
            for key in &message.account_keys {
                out.record_key(**key);
            }
            for lookup in &message.address_table_lookups {
                out.record_key(*lookup.account_key);
            }
        }
    }
}

fn collect_meta_keys(meta: &TransactionStatusMeta, out: &mut FixtureLogs) {
    for key in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        if key.len() == 32 {
            let mut pubkey = [0u8; 32];
            pubkey.copy_from_slice(key);
            out.record_key(pubkey);
        }
    }
}

fn collect_pubkeys_from_line(line: &str, out: &mut FixtureLogs) {
    for token in line.split(|c: char| !c.is_ascii_alphanumeric()) {
        if (32..=44).contains(&token.len()) {
            out.record_pubkey_text(token);
        }
    }
}

fn build_report(
    args: &Args,
    fixture: &FixtureLogs,
    key_index: &KeyIndex,
    key_store: &KeyStore,
    lexical_archive: &WincodeLogArchiveV2,
    frequency_key_index: &KeyIndex,
    frequency_archive: &WincodeLogArchiveV2,
) -> Result<Report> {
    let compact = &lexical_archive.logs;
    let frequency_compact = &frequency_archive.logs;
    let raw_utf8_bytes = fixture
        .logs
        .iter()
        .map(|line| line.len() + 1)
        .sum::<usize>();
    let raw_wincode_bytes = wincode::serialized_size(&fixture.logs).context("raw log size")?;
    let raw_wincode_varint_bytes =
        wincode::config::serialized_size(&fixture.logs, wincode_varint_config())
            .context("raw log varint wincode size")?;
    let raw_postcard_bytes = postcard::to_allocvec(&fixture.logs)
        .context("raw log postcard encode")?
        .len() as u64;
    let compact_wincode_bytes = wincode::serialized_size(compact).context("compact log size")?;
    let compact_wincode_varint_bytes =
        wincode::config::serialized_size(compact, wincode_varint_config())
            .context("compact log varint wincode size")?;
    let compact_postcard_bytes = postcard::to_allocvec(compact)
        .context("compact log postcard encode")?
        .len() as u64;
    let frequency_compact_wincode_varint_bytes =
        wincode::config::serialized_size(frequency_compact, wincode_varint_config())
            .context("frequency compact log varint wincode size")?;
    let frequency_compact_wincode_leb128_bytes =
        wincode::config::serialized_size(frequency_compact, wincode_leb128_config())
            .context("frequency compact log LEB128 wincode size")?;
    let frequency_compact_postcard_bytes = postcard::to_allocvec(frequency_compact)
        .context("frequency compact log postcard encode")?
        .len() as u64;
    let archive_lex_wincode_varint_bytes =
        wincode::config::serialized_size(lexical_archive, wincode_varint_config())
            .context("lexical archive varint wincode size")?;
    let archive_frequency_wincode_varint_bytes =
        wincode::config::serialized_size(frequency_archive, wincode_varint_config())
            .context("frequency archive varint wincode size")?;
    let archive_frequency_wincode_leb128_bytes =
        wincode::config::serialized_size(frequency_archive, wincode_leb128_config())
            .context("frequency archive LEB128 wincode size")?;
    let frequency_archive_postcard_bytes = postcard::to_allocvec(frequency_archive)
        .context("frequency archive postcard encode")?
        .len() as u64;
    let compact_events_wincode_bytes =
        wincode::serialized_size(&compact.events).context("compact event size")?;
    let compact_strings_wincode_bytes =
        wincode::serialized_size(&compact.strings).context("compact string table size")?;
    let compact_data_wincode_bytes =
        wincode::serialized_size(&compact.data).context("compact data table size")?;

    let mut parser_kinds = BTreeMap::new();
    for line in &fixture.logs {
        *parser_kinds.entry(kind_name(parse_line(line))).or_default() += 1;
    }

    let mut top_level_known_events = 0usize;
    let mut plain_or_unparsed_events = 0usize;
    for event in &compact.events {
        if matches!(event, LogEvent::Plain { .. } | LogEvent::Unparsed { .. }) {
            plain_or_unparsed_events += 1;
        } else {
            top_level_known_events += 1;
        }
    }

    let unknown_payloads = analyze_unknown_payloads(fixture, key_store, compact);
    let parse_timing = bench_parse_only(args.iters, &fixture.logs, key_index);
    let parse_frequency_timing = bench_parse_only(args.iters, &fixture.logs, frequency_key_index);
    let serialize_timing =
        bench_parse_and_serialize(args.serialize_iters, &fixture.logs, key_index)?;
    let archive_wincode_varint_serialize_timing =
        bench_serialize_wincode_varint_archive(args.serialize_iters, frequency_archive)?;
    let archive_wincode_leb128_serialize_timing =
        bench_serialize_wincode_leb128_archive(args.serialize_iters, frequency_archive)?;
    let archive_postcard_serialize_timing =
        bench_serialize_postcard_archive(args.serialize_iters, frequency_archive)?;
    let archive_wincode_varint_bytes =
        wincode::config::serialize(frequency_archive, wincode_varint_config())
            .context("frequency archive varint wincode encode")?;
    let archive_wincode_leb128_bytes =
        wincode::config::serialize(frequency_archive, wincode_leb128_config())
            .context("frequency archive LEB128 wincode encode")?;
    let archive_postcard_bytes =
        postcard::to_allocvec(frequency_archive).context("frequency archive postcard encode")?;
    let archive_wincode_varint_decode_timing =
        bench_decode_wincode_varint_archive(args.serialize_iters, &archive_wincode_varint_bytes)?;
    let archive_wincode_leb128_decode_timing =
        bench_decode_wincode_leb128_archive(args.serialize_iters, &archive_wincode_leb128_bytes)?;
    let archive_wincode_leb128_stream_decode_timing = bench_stream_decode_wincode_leb128_archive(
        args.serialize_iters,
        &archive_wincode_leb128_bytes,
    )?;
    let archive_postcard_decode_timing =
        bench_decode_postcard_archive(args.serialize_iters, &archive_postcard_bytes)?;
    let car_read_timing = bench_read_car_extract_logs(args.serialize_iters)?;

    Ok(Report {
        fixture_bytes: CAR_BYTES.len(),
        blocks: fixture.blocks,
        txs: fixture.txs,
        txs_with_logs: fixture.txs_with_logs,
        log_lines: fixture.logs.len(),
        raw_utf8_bytes,
        raw_wincode_bytes,
        raw_wincode_varint_bytes,
        raw_postcard_bytes,
        compact_wincode_bytes,
        compact_wincode_varint_bytes,
        compact_postcard_bytes,
        frequency_compact_wincode_varint_bytes,
        frequency_compact_wincode_leb128_bytes,
        frequency_compact_postcard_bytes,
        archive_lex_wincode_varint_bytes,
        archive_frequency_wincode_varint_bytes,
        archive_frequency_wincode_leb128_bytes,
        archive_frequency_postcard_bytes: frequency_archive_postcard_bytes,
        archive_key_registry_bytes: frequency_archive.keys.len() * 32,
        archive_tx_log_ranges: frequency_archive.tx_log_ranges.len(),
        compact_events_wincode_bytes,
        compact_strings_wincode_bytes,
        compact_data_wincode_bytes,
        string_table_entries: compact.strings.len(),
        string_table_bytes: compact.strings.bytes.len(),
        data_table_arrays: compact.data.len(),
        data_table_bytes: compact.data.bytes.len(),
        key_count: fixture.keys.len(),
        compact_events: compact.events.len(),
        top_level_known_events,
        plain_or_unparsed_events,
        unknown_program_payload_events: unknown_payloads.total_events,
        parser_kinds,
        unknown_payloads,
        parse_timing,
        parse_frequency_timing,
        serialize_timing,
        archive_wincode_varint_serialize_timing,
        archive_wincode_leb128_serialize_timing,
        archive_postcard_serialize_timing,
        archive_wincode_varint_decode_timing,
        archive_wincode_leb128_decode_timing,
        archive_wincode_leb128_stream_decode_timing,
        archive_postcard_decode_timing,
        car_read_timing,
    })
}

#[derive(Default)]
struct UnknownPayloadReport {
    total_events: usize,
    inferred_program_events: usize,
    missing_program_events: usize,
    payload_utf8_bytes: usize,
    string_table_contribution_bytes: usize,
    unique_programs: usize,
    unique_shapes: usize,
    unique_exact_payloads: usize,
    top_programs: Vec<UnknownPayloadRow>,
    top_shapes: Vec<UnknownPayloadRow>,
    top_exact: Vec<UnknownPayloadRow>,
}

struct UnknownPayloadRow {
    key: String,
    count: u64,
    bytes: usize,
}

#[derive(Default)]
struct UnknownPayloadCounter {
    counts: BTreeMap<String, UnknownPayloadCount>,
}

#[derive(Default)]
struct UnknownPayloadCount {
    count: u64,
    bytes: usize,
}

impl UnknownPayloadCounter {
    fn add(&mut self, key: String, payload: &str) {
        let item = self.counts.entry(key).or_default();
        item.count += 1;
        item.bytes += payload.len();
    }

    fn len(&self) -> usize {
        self.counts.len()
    }

    fn top(&self, limit: usize) -> Vec<UnknownPayloadRow> {
        let mut rows = self
            .counts
            .iter()
            .map(|(key, value)| UnknownPayloadRow {
                key: key.clone(),
                count: value.count,
                bytes: value.bytes,
            })
            .collect::<Vec<_>>();
        rows.sort_unstable_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| b.bytes.cmp(&a.bytes))
                .then_with(|| a.key.cmp(&b.key))
        });
        rows.truncate(limit);
        rows
    }
}

fn analyze_unknown_payloads(
    fixture: &FixtureLogs,
    key_store: &KeyStore,
    compact: &CompactLogStream,
) -> UnknownPayloadReport {
    let mut programs = UnknownPayloadCounter::default();
    let mut shapes = UnknownPayloadCounter::default();
    let mut exact = UnknownPayloadCounter::default();
    let mut total_events = 0usize;
    let mut inferred_program_events = 0usize;
    let mut missing_program_events = 0usize;
    let mut payload_utf8_bytes = 0usize;

    for range in &fixture.tx_log_ranges {
        let start = range.start as usize;
        let end = start + range.count as usize;
        let mut stack = Vec::<String>::new();
        for index in start..end {
            let line = fixture.logs[index].trim_end();
            let event = &compact.events[index];
            let parsed = parse_line(line);

            if let Some((payload, explicit_program)) =
                unknown_program_payload(event, key_store, &compact.strings)
            {
                total_events += 1;
                payload_utf8_bytes += payload.len();

                let program = explicit_program
                    .or_else(|| stack.last().cloned())
                    .unwrap_or_else(|| "<no active program>".to_string());
                if program == "<no active program>" {
                    missing_program_events += 1;
                } else {
                    inferred_program_events += 1;
                }

                programs.add(program.clone(), payload);
                shapes.add(
                    format!("{program}\t{}", normalize_payload_shape(payload)),
                    payload,
                );
                exact.add(format!("{program}\t{payload}"), payload);
            }

            update_program_stack(parsed, &mut stack);
        }
    }

    UnknownPayloadReport {
        total_events,
        inferred_program_events,
        missing_program_events,
        payload_utf8_bytes,
        string_table_contribution_bytes: payload_utf8_bytes + total_events * 4,
        unique_programs: programs.len(),
        unique_shapes: shapes.len(),
        unique_exact_payloads: exact.len(),
        top_programs: programs.top(UNKNOWN_TOP),
        top_shapes: shapes.top(UNKNOWN_TOP),
        top_exact: exact.top(UNKNOWN_TOP),
    }
}

fn unknown_program_payload<'a>(
    event: &LogEvent,
    key_store: &KeyStore,
    st: &'a blockzilla_format::StringTable,
) -> Option<(&'a str, Option<String>)> {
    match event {
        LogEvent::ProgramLog(ProgramLog::Unknown(id)) => Some((st.resolve(*id), None)),
        LogEvent::ProgramIdLog {
            program,
            log: ProgramLog::Unknown(id),
        } => Some((
            st.resolve(*id),
            Some(program_id_to_string(key_store, *program)),
        )),
        _ => None,
    }
}

fn program_id_to_string(key_store: &KeyStore, id: CompactPubkey) -> String {
    match id {
        CompactPubkey::Id(id) => key_store
            .get(id)
            .map(|key| Pubkey::new_from_array(*key).to_string())
            .unwrap_or_else(|| format!("<invalid program id {id}>")),
        CompactPubkey::Raw(key) => Pubkey::new_from_array(key).to_string(),
    }
}

fn update_program_stack(parsed: ParsedLogLine<'_>, stack: &mut Vec<String>) {
    match parsed {
        ParsedLogLine::Invoke { program, depth } => {
            let depth = depth as usize;
            if depth == 0 {
                stack.clear();
                stack.push(program.to_string());
                return;
            }
            stack.truncate(depth.saturating_sub(1));
            stack.push(program.to_string());
        }
        ParsedLogLine::Success { program } | ParsedLogLine::Failure { program, .. } => {
            if stack.last().is_some_and(|active| active == program) {
                stack.pop();
            } else if let Some(position) = stack.iter().rposition(|active| active == program) {
                stack.truncate(position);
            } else {
                stack.clear();
            }
        }
        _ => {}
    }
}

fn normalize_payload_shape(value: &str) -> String {
    value
        .split_whitespace()
        .map(normalize_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_token(token: &str) -> String {
    let core = token.trim_matches(|c: char| {
        matches!(
            c,
            '[' | ']' | '(' | ')' | '{' | '}' | ',' | '.' | ':' | ';' | '\'' | '"' | '`'
        )
    });

    if core.is_empty() {
        return token.to_owned();
    }
    if Pubkey::from_str(core).is_ok() {
        return "<PUBKEY>".to_string();
    }
    if is_decimal(core) {
        return "<N>".to_string();
    }
    if is_hex(core) {
        return "<HEX>".to_string();
    }
    if looks_like_base64(core) {
        return "<B64>".to_string();
    }

    core.to_owned()
}

fn is_decimal(value: &str) -> bool {
    let mut saw_digit = false;
    for b in value.bytes() {
        match b {
            b'0'..=b'9' => saw_digit = true,
            b',' => {}
            _ => return false,
        }
    }
    saw_digit
}

fn is_hex(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("0x") else {
        return false;
    };
    !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

fn looks_like_base64(value: &str) -> bool {
    value.len() >= 20
        && value
            .bytes()
            .any(|b| b.is_ascii_digit() || matches!(b, b'+' | b'/' | b'='))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'='))
        && BASE64.decode(value.as_bytes()).is_ok()
}

fn bench_parse_only(iters: usize, logs: &[String], key_index: &KeyIndex) -> BenchTiming {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let compact = parse_logs(logs, key_index);
        checksum ^= compact.events.len();
        checksum ^= compact.strings.bytes.len();
        checksum ^= compact.data.bytes.len();
        black_box(&compact);
    }

    BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    }
}

fn bench_parse_and_serialize(
    iters: usize,
    logs: &[String],
    key_index: &KeyIndex,
) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let compact = parse_logs(logs, key_index);
        let bytes = wincode::config::serialize(&compact, wincode_leb128_config())
            .context("serialize compact logs")?;
        checksum ^= bytes.len();
        black_box(bytes);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_serialize_wincode_varint_archive(
    iters: usize,
    archive: &WincodeLogArchiveV2,
) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let bytes = wincode::config::serialize(archive, wincode_varint_config())
            .context("serialize wincode varint archive")?;
        checksum ^= bytes.len();
        black_box(bytes);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_serialize_wincode_leb128_archive(
    iters: usize,
    archive: &WincodeLogArchiveV2,
) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let bytes = wincode::config::serialize(archive, wincode_leb128_config())
            .context("serialize wincode LEB128 archive")?;
        checksum ^= bytes.len();
        black_box(bytes);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_serialize_postcard_archive(
    iters: usize,
    archive: &WincodeLogArchiveV2,
) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let bytes = postcard::to_allocvec(archive).context("serialize postcard archive")?;
        checksum ^= bytes.len();
        black_box(bytes);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_decode_wincode_varint_archive(iters: usize, bytes: &[u8]) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let archive: WincodeLogArchiveV2 =
            wincode::config::deserialize(black_box(bytes), wincode_varint_config())
                .context("decode wincode varint archive")?;
        checksum ^= archive.logs.events.len();
        checksum ^= archive.keys.len();
        checksum ^= archive.tx_log_ranges.len();
        black_box(archive);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_decode_wincode_leb128_archive(iters: usize, bytes: &[u8]) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let archive: WincodeLogArchiveV2 =
            wincode::config::deserialize(black_box(bytes), wincode_leb128_config())
                .context("decode wincode LEB128 archive")?;
        checksum ^= archive.logs.events.len();
        checksum ^= archive.keys.len();
        checksum ^= archive.tx_log_ranges.len();
        black_box(archive);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_stream_decode_wincode_leb128_archive(iters: usize, bytes: &[u8]) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let cursor = Cursor::new(black_box(bytes));
        let reader = BufReader::with_capacity(64 << 10, cursor);
        let archive: WincodeLogArchiveV2 =
            wincode::config::deserialize_from(reader, wincode_leb128_config())
                .context("stream decode wincode LEB128 archive")?;
        checksum ^= archive.logs.events.len();
        checksum ^= archive.keys.len();
        checksum ^= archive.tx_log_ranges.len();
        black_box(archive);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn bench_decode_postcard_archive(iters: usize, bytes: &[u8]) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        let archive: WincodeLogArchiveV2 =
            postcard::from_bytes(black_box(bytes)).context("decode postcard archive")?;
        checksum ^= archive.logs.events.len();
        checksum ^= archive.keys.len();
        checksum ^= archive.tx_log_ranges.len();
        black_box(archive);
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn wincode_varint_config() -> WincodeVarintConfig {
    wincode::config::Configuration::default()
        .disable_preallocation_size_limit()
        .with_varint_encoding()
}

fn wincode_leb128_config() -> WincodeLeb128Config {
    wincode::config::Configuration::default()
        .disable_preallocation_size_limit()
        .with_int_encoding::<Leb128>()
}

fn bench_read_car_extract_logs(iters: usize) -> Result<BenchTiming> {
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iters {
        checksum ^= read_car_extract_logs_once()?;
    }

    Ok(BenchTiming {
        iters,
        elapsed: started.elapsed(),
        checksum,
    })
}

fn read_car_extract_logs_once() -> Result<usize> {
    let cursor = Cursor::new(CAR_BYTES);
    let reader = BufReader::with_capacity(32 << 20, cursor);
    let mut car = CarBlockReader::with_capacity(reader, 8 << 20);
    car.skip_header().context("skip CAR header")?;

    let mut group = CarBlockGroup::new();
    let mut checksum = 0usize;
    loop {
        let has_block = car
            .read_until_block_into(&mut group)
            .context("read CAR block")?;
        if !has_block {
            break;
        }

        let mut it = group.transactions();
        while let Some((tx, meta)) = it.next_tx().context("decode CAR transaction")? {
            checksum ^= tx.signatures.len();
            if let Some(meta) = meta {
                checksum ^= meta.log_messages.len();
                checksum ^= meta.loaded_writable_addresses.len();
                checksum ^= meta.loaded_readonly_addresses.len();
            }
        }
    }

    Ok(checksum)
}

#[cfg(test)]
fn event_has_unknown_program_payload(event: &LogEvent) -> bool {
    matches!(
        event,
        LogEvent::ProgramLog(ProgramLog::Unknown(_))
            | LogEvent::ProgramIdLog {
                log: ProgramLog::Unknown(_),
                ..
            }
    )
}

fn kind_name(parsed: ParsedLogLine<'_>) -> &'static str {
    match parsed {
        ParsedLogLine::CustomProgramError { .. } => "CustomProgramError",
        ParsedLogLine::FailedToComplete { .. } => "FailedToComplete",
        ParsedLogLine::UnknownProgram { .. } => "UnknownProgram",
        ParsedLogLine::UnknownAccount { .. } => "UnknownAccount",
        ParsedLogLine::LogTruncated => "LogTruncated",
        ParsedLogLine::VerifyEd25519 => "VerifyEd25519",
        ParsedLogLine::VerifySecp256k1 => "VerifySecp256k1",
        ParsedLogLine::CloseContextState => "CloseContextState",
        ParsedLogLine::ProgramAccountNotWritable => "ProgramAccountNotWritable",
        ParsedLogLine::ProgramIdMismatch => "ProgramIdMismatch",
        ParsedLogLine::ProgramNotUpgradeable => "ProgramNotUpgradeable",
        ParsedLogLine::ProgramAndProgramDataAccountMismatch => {
            "ProgramAndProgramDataAccountMismatch"
        }
        ParsedLogLine::ProgramWasExtendedInThisBlockAlready => {
            "ProgramWasExtendedInThisBlockAlready"
        }
        ParsedLogLine::StakeMergingAccounts => "StakeMergingAccounts",
        ParsedLogLine::LoaderUpgradedProgram { .. } => "LoaderUpgradedProgram",
        ParsedLogLine::LoaderFinalizedAccount { .. } => "LoaderFinalizedAccount",
        ParsedLogLine::BpfInvoke { .. } => "BpfInvoke",
        ParsedLogLine::BpfSuccess { .. } => "BpfSuccess",
        ParsedLogLine::BpfFailure { .. } => "BpfFailure",
        ParsedLogLine::BpfConsumed { .. } => "BpfConsumed",
        ParsedLogLine::RuntimeWritablePrivilegeEscalated { .. } => {
            "RuntimeWritablePrivilegeEscalated"
        }
        ParsedLogLine::RuntimeSignerPrivilegeEscalated { .. } => "RuntimeSignerPrivilegeEscalated",
        ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { .. } => {
            "RuntimeAccountOwnerBalanceVerificationFailed"
        }
        ParsedLogLine::SystemTransferInsufficient { .. } => "SystemTransferInsufficient",
        ParsedLogLine::SystemTransferFromMustNotCarryData => "SystemTransferFromMustNotCarryData",
        ParsedLogLine::SystemAllocateAccountAlreadyInUse { .. } => {
            "SystemAllocateAccountAlreadyInUse"
        }
        ParsedLogLine::SystemCreateAccountAlreadyInUse { .. } => "SystemCreateAccountAlreadyInUse",
        ParsedLogLine::SystemCreateAccountDataSizeLimited { .. } => {
            "SystemCreateAccountDataSizeLimited"
        }
        ParsedLogLine::ProgramLog { .. } => "ProgramLog",
        ParsedLogLine::ProgramIdLog { .. } => "ProgramIdLog",
        ParsedLogLine::ProgramData { .. } => "ProgramData",
        ParsedLogLine::ProgramReturn { .. } => "ProgramReturn",
        ParsedLogLine::ProgramConsumption { .. } => "ProgramConsumption",
        ParsedLogLine::ProgramNotCached { .. } => "ProgramNotCached",
        ParsedLogLine::ProgramNotDeployed { .. } => "ProgramNotDeployed",
        ParsedLogLine::Invoke { .. } => "Invoke",
        ParsedLogLine::Success { .. } => "Success",
        ParsedLogLine::Failure { .. } => "Failure",
        ParsedLogLine::Consumed { .. } => "Consumed",
        ParsedLogLine::CbRequestUnits { .. } => "CbRequestUnits",
        ParsedLogLine::UnparsedProgram => "UnparsedProgram",
        ParsedLogLine::Plain { .. } => "Plain",
    }
}

fn print_report(report: &Report) {
    println!("{}", render_text(report));
}

fn render_text(report: &Report) -> String {
    let reduction = percent_reduction(report.raw_wincode_bytes, report.compact_wincode_bytes);
    let ratio = report.raw_wincode_bytes as f64 / report.compact_wincode_bytes.max(1) as f64;
    let parse_logs_per_s = logs_per_s(report.log_lines, &report.parse_timing);
    let parse_frequency_logs_per_s = logs_per_s(report.log_lines, &report.parse_frequency_timing);
    let serialize_logs_per_s = logs_per_s(report.log_lines, &report.serialize_timing);
    let archive_wincode_encode_mib_s = mib_per_s(
        report.archive_frequency_wincode_varint_bytes,
        &report.archive_wincode_varint_serialize_timing,
    );
    let archive_leb128_encode_mib_s = mib_per_s(
        report.archive_frequency_wincode_leb128_bytes,
        &report.archive_wincode_leb128_serialize_timing,
    );
    let archive_postcard_encode_mib_s = mib_per_s(
        report.archive_frequency_postcard_bytes,
        &report.archive_postcard_serialize_timing,
    );
    let archive_wincode_decode_mib_s = mib_per_s(
        report.archive_frequency_wincode_varint_bytes,
        &report.archive_wincode_varint_decode_timing,
    );
    let archive_leb128_decode_mib_s = mib_per_s(
        report.archive_frequency_wincode_leb128_bytes,
        &report.archive_wincode_leb128_decode_timing,
    );
    let archive_leb128_stream_decode_mib_s = mib_per_s(
        report.archive_frequency_wincode_leb128_bytes,
        &report.archive_wincode_leb128_stream_decode_timing,
    );
    let archive_postcard_decode_mib_s = mib_per_s(
        report.archive_frequency_postcard_bytes,
        &report.archive_postcard_decode_timing,
    );
    let car_read_mib_s = mib_per_s(CAR_BYTES.len() as u64, &report.car_read_timing);

    let mut out = String::new();
    out.push_str("big_block_log_bench\n");
    out.push_str(&format!(
        "fixture=epoch-822-biggest.car fixture_bytes={}\n",
        report.fixture_bytes
    ));
    out.push_str(&format!(
        "blocks={} txs={} txs_with_logs={} log_lines={} key_count={}\n",
        report.blocks, report.txs, report.txs_with_logs, report.log_lines, report.key_count
    ));
    out.push_str(&format!(
        "raw_utf8_bytes={} raw_wincode_bytes={} compact_wincode_bytes={} reduction_pct={reduction:.2} ratio={ratio:.2}x\n",
        report.raw_utf8_bytes, report.raw_wincode_bytes, report.compact_wincode_bytes
    ));
    out.push_str(&format!(
        "raw_codec_bytes wincode_fixint={} wincode_varint={} postcard={}\n",
        report.raw_wincode_bytes, report.raw_wincode_varint_bytes, report.raw_postcard_bytes
    ));
    out.push_str(&format!(
        "compact_log_bytes wincode_fixint={} wincode_varint_lex={} wincode_varint_freq={} wincode_leb128_freq={} postcard_lex={} postcard_freq={}\n",
        report.compact_wincode_bytes,
        report.compact_wincode_varint_bytes,
        report.frequency_compact_wincode_varint_bytes,
        report.frequency_compact_wincode_leb128_bytes,
        report.compact_postcard_bytes,
        report.frequency_compact_postcard_bytes
    ));
    out.push_str(&format!(
        "v2_log_archive_bytes wincode_varint_lex={} wincode_varint_freq={} wincode_leb128_freq={} postcard_freq={} key_registry_bytes={} tx_log_ranges={} source_car_bytes={} note=log_only_not_car_equivalent\n",
        report.archive_lex_wincode_varint_bytes,
        report.archive_frequency_wincode_varint_bytes,
        report.archive_frequency_wincode_leb128_bytes,
        report.archive_frequency_postcard_bytes,
        report.archive_key_registry_bytes,
        report.archive_tx_log_ranges,
        report.fixture_bytes
    ));
    out.push_str(&format!(
        "compact_events={} top_level_known_events={} plain_or_unparsed_events={} unknown_program_payload_events={}\n",
        report.compact_events,
        report.top_level_known_events,
        report.plain_or_unparsed_events,
        report.unknown_program_payload_events
    ));
    out.push_str(&format!(
        "string_table_entries={} string_table_bytes={} data_table_arrays={} data_table_bytes={}\n",
        report.string_table_entries,
        report.string_table_bytes,
        report.data_table_arrays,
        report.data_table_bytes
    ));
    out.push_str(&format!(
        "compact_events_wincode_bytes={} compact_strings_wincode_bytes={} compact_data_wincode_bytes={}\n",
        report.compact_events_wincode_bytes,
        report.compact_strings_wincode_bytes,
        report.compact_data_wincode_bytes
    ));
    out.push_str(&format!(
        "unknown_payloads total={} inferred_program={} missing_program={} payload_utf8_bytes={} string_table_contribution_bytes={} unique_programs={} unique_shapes={} unique_exact={}\n",
        report.unknown_payloads.total_events,
        report.unknown_payloads.inferred_program_events,
        report.unknown_payloads.missing_program_events,
        report.unknown_payloads.payload_utf8_bytes,
        report.unknown_payloads.string_table_contribution_bytes,
        report.unknown_payloads.unique_programs,
        report.unknown_payloads.unique_shapes,
        report.unknown_payloads.unique_exact_payloads
    ));
    out.push_str(&format!(
        "parse_only iters={} elapsed_s={:.6} logs_per_s={parse_logs_per_s:.0} checksum={}\n",
        report.parse_timing.iters,
        report.parse_timing.elapsed.as_secs_f64(),
        report.parse_timing.checksum
    ));
    out.push_str(&format!(
        "parse_only_frequency_keys iters={} elapsed_s={:.6} logs_per_s={parse_frequency_logs_per_s:.0} checksum={}\n",
        report.parse_frequency_timing.iters,
        report.parse_frequency_timing.elapsed.as_secs_f64(),
        report.parse_frequency_timing.checksum
    ));
    out.push_str(&format!(
        "parse_wincode_serialize iters={} elapsed_s={:.6} logs_per_s={serialize_logs_per_s:.0} checksum={}\n",
        report.serialize_timing.iters,
        report.serialize_timing.elapsed.as_secs_f64(),
        report.serialize_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_wincode_varint_serialize iters={} elapsed_s={:.6} MiB_s={archive_wincode_encode_mib_s:.2} checksum={}\n",
        report.archive_wincode_varint_serialize_timing.iters,
        report.archive_wincode_varint_serialize_timing.elapsed.as_secs_f64(),
        report.archive_wincode_varint_serialize_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_wincode_leb128_serialize iters={} elapsed_s={:.6} MiB_s={archive_leb128_encode_mib_s:.2} checksum={}\n",
        report.archive_wincode_leb128_serialize_timing.iters,
        report.archive_wincode_leb128_serialize_timing.elapsed.as_secs_f64(),
        report.archive_wincode_leb128_serialize_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_postcard_serialize iters={} elapsed_s={:.6} MiB_s={archive_postcard_encode_mib_s:.2} checksum={}\n",
        report.archive_postcard_serialize_timing.iters,
        report.archive_postcard_serialize_timing.elapsed.as_secs_f64(),
        report.archive_postcard_serialize_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_wincode_varint_decode iters={} elapsed_s={:.6} MiB_s={archive_wincode_decode_mib_s:.2} checksum={}\n",
        report.archive_wincode_varint_decode_timing.iters,
        report.archive_wincode_varint_decode_timing.elapsed.as_secs_f64(),
        report.archive_wincode_varint_decode_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_wincode_leb128_decode iters={} elapsed_s={:.6} MiB_s={archive_leb128_decode_mib_s:.2} checksum={}\n",
        report.archive_wincode_leb128_decode_timing.iters,
        report.archive_wincode_leb128_decode_timing.elapsed.as_secs_f64(),
        report.archive_wincode_leb128_decode_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_wincode_leb128_stream_decode iters={} elapsed_s={:.6} MiB_s={archive_leb128_stream_decode_mib_s:.2} checksum={}\n",
        report.archive_wincode_leb128_stream_decode_timing.iters,
        report.archive_wincode_leb128_stream_decode_timing.elapsed.as_secs_f64(),
        report.archive_wincode_leb128_stream_decode_timing.checksum
    ));
    out.push_str(&format!(
        "v2_log_archive_postcard_decode iters={} elapsed_s={:.6} MiB_s={archive_postcard_decode_mib_s:.2} checksum={}\n",
        report.archive_postcard_decode_timing.iters,
        report.archive_postcard_decode_timing.elapsed.as_secs_f64(),
        report.archive_postcard_decode_timing.checksum
    ));
    out.push_str(&format!(
        "old_car_read_extract_logs iters={} elapsed_s={:.6} MiB_s={car_read_mib_s:.2} checksum={}\n",
        report.car_read_timing.iters,
        report.car_read_timing.elapsed.as_secs_f64(),
        report.car_read_timing.checksum
    ));
    out.push_str("parser_kinds:\n");
    for (kind, count) in &report.parser_kinds {
        out.push_str(&format!("  {kind}={count}\n"));
    }
    out.push_str("top_unknown_payload_programs:\n");
    for row in &report.unknown_payloads.top_programs {
        out.push_str(&format!(
            "  count={} bytes={} program={}\n",
            row.count, row.bytes, row.key
        ));
    }
    out.push_str("top_unknown_payload_shapes:\n");
    for row in &report.unknown_payloads.top_shapes {
        out.push_str(&format!(
            "  count={} bytes={} {}\n",
            row.count,
            row.bytes,
            format_unknown_key(&row.key)
        ));
    }
    out
}

fn render_markdown(report: &Report) -> String {
    let reduction = percent_reduction(report.raw_wincode_bytes, report.compact_wincode_bytes);
    let ratio = report.raw_wincode_bytes as f64 / report.compact_wincode_bytes.max(1) as f64;
    let parse_logs_per_s = logs_per_s(report.log_lines, &report.parse_timing);
    let parse_frequency_logs_per_s = logs_per_s(report.log_lines, &report.parse_frequency_timing);
    let serialize_logs_per_s = logs_per_s(report.log_lines, &report.serialize_timing);
    let archive_wincode_encode_mib_s = mib_per_s(
        report.archive_frequency_wincode_varint_bytes,
        &report.archive_wincode_varint_serialize_timing,
    );
    let archive_postcard_encode_mib_s = mib_per_s(
        report.archive_frequency_postcard_bytes,
        &report.archive_postcard_serialize_timing,
    );
    let archive_wincode_decode_mib_s = mib_per_s(
        report.archive_frequency_wincode_varint_bytes,
        &report.archive_wincode_varint_decode_timing,
    );
    let archive_postcard_decode_mib_s = mib_per_s(
        report.archive_frequency_postcard_bytes,
        &report.archive_postcard_decode_timing,
    );

    let mut out = String::new();
    out.push_str("# Big Block Log Bench\n\n");
    out.push_str(
        "Fixture: `crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car`.\n\n",
    );
    out.push_str("Command:\n\n");
    out.push_str("```bash\n");
    out.push_str("cargo run --release -p blockzilla --bin big-block-log-bench -- --iters 1000 --serialize-iters 200 --markdown docs/big-block-log-bench.md\n");
    out.push_str("```\n\n");
    out.push_str("## Summary\n\n");
    out.push_str("| Metric | Value |\n|---|---:|\n");
    out.push_str(&format!("| Fixture bytes | {} |\n", report.fixture_bytes));
    out.push_str(&format!("| Blocks | {} |\n", report.blocks));
    out.push_str(&format!("| Transactions | {} |\n", report.txs));
    out.push_str(&format!(
        "| Transactions with logs | {} |\n",
        report.txs_with_logs
    ));
    out.push_str(&format!("| Log lines | {} |\n", report.log_lines));
    out.push_str(&format!(
        "| Registry keys for bench | {} |\n",
        report.key_count
    ));
    out.push_str(&format!(
        "| Raw UTF-8 bytes incl newline | {} |\n",
        report.raw_utf8_bytes
    ));
    out.push_str(&format!(
        "| Raw `Vec<String>` wincode bytes | {} |\n",
        report.raw_wincode_bytes
    ));
    out.push_str(&format!(
        "| Raw `Vec<String>` wincode varint bytes | {} |\n",
        report.raw_wincode_varint_bytes
    ));
    out.push_str(&format!(
        "| Raw `Vec<String>` postcard bytes | {} |\n",
        report.raw_postcard_bytes
    ));
    out.push_str(&format!(
        "| Compact log wincode bytes | {} |\n",
        report.compact_wincode_bytes
    ));
    out.push_str(&format!(
        "| Compact log wincode varint bytes, lexical keys | {} |\n",
        report.compact_wincode_varint_bytes
    ));
    out.push_str(&format!(
        "| Compact log wincode varint bytes, frequency keys | {} |\n",
        report.frequency_compact_wincode_varint_bytes
    ));
    out.push_str(&format!(
        "| Compact log postcard bytes, lexical keys | {} |\n",
        report.compact_postcard_bytes
    ));
    out.push_str(&format!(
        "| Compact log postcard bytes, frequency keys | {} |\n",
        report.frequency_compact_postcard_bytes
    ));
    out.push_str(&format!(
        "| V2 log archive wincode varint bytes, lexical keys | {} |\n",
        report.archive_lex_wincode_varint_bytes
    ));
    out.push_str(&format!(
        "| V2 log archive wincode varint bytes, frequency keys | {} |\n",
        report.archive_frequency_wincode_varint_bytes
    ));
    out.push_str(&format!(
        "| V2 log archive postcard bytes, frequency keys | {} |\n",
        report.archive_frequency_postcard_bytes
    ));
    out.push_str(&format!(
        "| V2 log archive key registry bytes | {} |\n",
        report.archive_key_registry_bytes
    ));
    out.push_str(&format!(
        "| V2 log archive tx log ranges | {} |\n",
        report.archive_tx_log_ranges
    ));
    out.push_str(&format!(
        "| Size reduction vs raw wincode | {:.2}% |\n",
        reduction
    ));
    out.push_str(&format!("| Raw/compact ratio | {:.2}x |\n", ratio));
    out.push_str(&format!("| Compact events | {} |\n", report.compact_events));
    out.push_str(&format!(
        "| Top-level known events | {} ({:.2}%) |\n",
        report.top_level_known_events,
        percent(
            report.top_level_known_events as u64,
            report.compact_events as u64
        )
    ));
    out.push_str(&format!(
        "| Plain/unparsed events | {} ({:.2}%) |\n",
        report.plain_or_unparsed_events,
        percent(
            report.plain_or_unparsed_events as u64,
            report.compact_events as u64
        )
    ));
    out.push_str(&format!(
        "| Unknown program payload events | {} ({:.2}%) |\n",
        report.unknown_program_payload_events,
        percent(
            report.unknown_program_payload_events as u64,
            report.compact_events as u64
        )
    ));
    out.push_str(&format!(
        "| String table entries | {} |\n",
        report.string_table_entries
    ));
    out.push_str(&format!(
        "| String table bytes | {} |\n",
        report.string_table_bytes
    ));
    out.push_str(&format!(
        "| Data table arrays | {} |\n",
        report.data_table_arrays
    ));
    out.push_str(&format!(
        "| Data table bytes | {} |\n",
        report.data_table_bytes
    ));
    out.push_str(&format!(
        "| Compact events wincode bytes | {} |\n",
        report.compact_events_wincode_bytes
    ));
    out.push_str(&format!(
        "| Compact strings wincode bytes | {} |\n",
        report.compact_strings_wincode_bytes
    ));
    out.push_str(&format!(
        "| Compact data wincode bytes | {} |\n",
        report.compact_data_wincode_bytes
    ));
    out.push_str(&format!(
        "| Parse-only throughput | {:.0} logs/s over {} iters |\n",
        parse_logs_per_s, report.parse_timing.iters
    ));
    out.push_str(&format!(
        "| Parse-only throughput, frequency keys | {:.0} logs/s over {} iters |\n",
        parse_frequency_logs_per_s, report.parse_frequency_timing.iters
    ));
    out.push_str(&format!(
        "| Parse + wincode serialize throughput | {:.0} logs/s over {} iters |\n",
        serialize_logs_per_s, report.serialize_timing.iters
    ));
    out.push_str(&format!(
        "| V2 log archive wincode varint serialize | {:.2} MiB/s over {} iters |\n",
        archive_wincode_encode_mib_s, report.archive_wincode_varint_serialize_timing.iters
    ));
    out.push_str(&format!(
        "| V2 log archive postcard serialize | {:.2} MiB/s over {} iters |\n",
        archive_postcard_encode_mib_s, report.archive_postcard_serialize_timing.iters
    ));
    out.push_str(&format!(
        "| V2 log archive wincode varint decode | {:.2} MiB/s over {} iters |\n",
        archive_wincode_decode_mib_s, report.archive_wincode_varint_decode_timing.iters
    ));
    out.push_str(&format!(
        "| V2 log archive postcard decode | {:.2} MiB/s over {} iters |\n",
        archive_postcard_decode_mib_s, report.archive_postcard_decode_timing.iters
    ));
    out.push_str(&format!(
        "| Unknown payload UTF-8 bytes | {} |\n",
        report.unknown_payloads.payload_utf8_bytes
    ));
    out.push_str(&format!(
        "| Unknown payload string-table contribution | {} |\n",
        report.unknown_payloads.string_table_contribution_bytes
    ));
    out.push_str(&format!(
        "| Unknown payload programs | {} |\n",
        report.unknown_payloads.unique_programs
    ));
    out.push_str(&format!(
        "| Unknown payload shapes | {} |\n",
        report.unknown_payloads.unique_shapes
    ));
    out.push_str(&format!(
        "| Unknown exact payloads | {} |\n",
        report.unknown_payloads.unique_exact_payloads
    ));

    out.push_str("\n## Parser Kinds\n\n");
    out.push_str("| Kind | Count |\n|---|---:|\n");
    for (kind, count) in &report.parser_kinds {
        out.push_str(&format!("| {kind} | {count} |\n"));
    }

    out.push_str("\n## Unknown Program Payloads By Program\n\n");
    out.push_str("| Program | Count | Payload bytes |\n|---|---:|---:|\n");
    for row in &report.unknown_payloads.top_programs {
        out.push_str(&format!(
            "| `{}` | {} | {} |\n",
            row.key, row.count, row.bytes
        ));
    }

    out.push_str("\n## Unknown Program Payload Shapes\n\n");
    out.push_str("| Program | Shape | Count | Payload bytes |\n|---|---|---:|---:|\n");
    for row in &report.unknown_payloads.top_shapes {
        let (program, shape) = split_unknown_key(&row.key);
        out.push_str(&format!(
            "| `{}` | `{}` | {} | {} |\n",
            program, shape, row.count, row.bytes
        ));
    }

    out.push_str("\n## Unknown Exact Payloads\n\n");
    out.push_str("| Program | Payload | Count | Payload bytes |\n|---|---|---:|---:|\n");
    for row in &report.unknown_payloads.top_exact {
        let (program, payload) = split_unknown_key(&row.key);
        out.push_str(&format!(
            "| `{}` | `{}` | {} | {} |\n",
            program, payload, row.count, row.bytes
        ));
    }

    out
}

fn split_unknown_key(key: &str) -> (&str, &str) {
    key.split_once('\t').unwrap_or((key, ""))
}

fn format_unknown_key(key: &str) -> String {
    let (program, payload) = split_unknown_key(key);
    format!("program={program} payload={payload}")
}

fn percent_reduction(raw: u64, compact: u64) -> f64 {
    if raw == 0 {
        return 0.0;
    }
    100.0 * (raw.saturating_sub(compact) as f64) / raw as f64
}

fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    100.0 * part as f64 / total as f64
}

fn logs_per_s(log_lines: usize, timing: &BenchTiming) -> f64 {
    let elapsed = timing.elapsed.as_secs_f64();
    if elapsed == 0.0 {
        return 0.0;
    }
    (log_lines * timing.iters) as f64 / elapsed
}

fn mib_per_s(bytes_per_iter: u64, timing: &BenchTiming) -> f64 {
    let elapsed = timing.elapsed.as_secs_f64();
    if elapsed == 0.0 {
        return 0.0;
    }
    (bytes_per_iter as f64 * timing.iters as f64) / elapsed / (1024.0 * 1024.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn biggest_block_log_storage_baseline() -> Result<()> {
        let fixture = collect_fixture_logs()?;
        let keys = fixture.keys_lexical();
        let key_index = KeyIndex::build(keys.clone());
        let key_store = KeyStore { keys };
        let compact = parse_logs(&fixture.logs, &key_index);
        let frequency_keys = fixture.keys_by_frequency();
        let frequency_key_index = KeyIndex::build(frequency_keys.clone());
        let frequency_compact = parse_logs(&fixture.logs, &frequency_key_index);

        let raw_utf8_bytes = fixture
            .logs
            .iter()
            .map(|line| line.len() + 1)
            .sum::<usize>();
        let raw_wincode_bytes = wincode::serialized_size(&fixture.logs)?;
        let compact_wincode_bytes = wincode::serialized_size(&compact)?;
        let frequency_compact_wincode_varint_bytes =
            wincode::config::serialized_size(&frequency_compact, wincode_varint_config())?;
        let frequency_archive =
            build_log_archive(&fixture, frequency_keys, frequency_compact, true)?;
        let frequency_archive_wincode_varint_bytes =
            wincode::config::serialized_size(&frequency_archive, wincode_varint_config())?;
        let frequency_archive_bytes =
            wincode::config::serialize(&frequency_archive, wincode_varint_config())?;
        let decoded_frequency_archive: WincodeLogArchiveV2 =
            wincode::config::deserialize(&frequency_archive_bytes, wincode_varint_config())?;

        let plain_or_unparsed = compact
            .events
            .iter()
            .filter(|event| matches!(event, LogEvent::Plain { .. } | LogEvent::Unparsed { .. }))
            .count();
        let unknown_program_payloads = compact
            .events
            .iter()
            .filter(|event| event_has_unknown_program_payload(event))
            .count();
        let unknown_payload_report = analyze_unknown_payloads(&fixture, &key_store, &compact);

        assert_eq!(fixture.blocks, 1);
        assert_eq!(fixture.txs, 2_969);
        assert_eq!(fixture.txs_with_logs, 2_968);
        assert_eq!(fixture.logs.len(), 26_767);
        assert_eq!(fixture.tx_log_ranges.len(), fixture.txs_with_logs as usize);
        assert_eq!(compact.events.len(), fixture.logs.len());
        assert_eq!(
            decoded_frequency_archive.logs.events.len(),
            fixture.logs.len()
        );
        assert_eq!(decoded_frequency_archive.keys.len(), fixture.keys.len());
        assert_eq!(raw_utf8_bytes, 1_749_896);
        assert_eq!(raw_wincode_bytes, 1_937_273);
        assert_eq!(plain_or_unparsed, 0);
        assert!(
            unknown_program_payloads <= 2_192,
            "unknown payload count regressed: {unknown_program_payloads}"
        );
        assert_eq!(
            unknown_payload_report.total_events,
            unknown_program_payloads
        );
        assert_eq!(unknown_payload_report.missing_program_events, 0);
        assert!(
            compact_wincode_bytes <= 438_995,
            "compact binary size regressed: {compact_wincode_bytes}"
        );
        assert!(
            percent_reduction(raw_wincode_bytes, compact_wincode_bytes) >= 77.0,
            "compact binary reduction regressed"
        );
        assert!(
            frequency_compact_wincode_varint_bytes < compact_wincode_bytes,
            "varint compact logs should beat default wincode: {frequency_compact_wincode_varint_bytes} >= {compact_wincode_bytes}"
        );
        assert!(
            frequency_archive_wincode_varint_bytes < raw_wincode_bytes,
            "standalone log archive should stay below raw wincode logs: {frequency_archive_wincode_varint_bytes} >= {raw_wincode_bytes}"
        );

        Ok(())
    }
}
