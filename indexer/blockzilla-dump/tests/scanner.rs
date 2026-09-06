use blockzilla_compact_v2_reader::{CompactV2MessageSchema, CompactV2MetadataSchema};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow,
    ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
    ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow,
    WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer,
    WincodeArchiveV2Header, write_archive_v2_hot_block_index,
};
use blockzilla_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
    CompactTokenBalance, OwnedCompactRecentBlockhash,
};
use blockzilla_compact_v2_reader::{
    COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES, COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
    manifest::{
        GENERATION_MANIFEST_FILE, GenerationFile, GenerationManifest, compute_generation_digest,
    },
};
use blockzilla_dump::{
    DumpDatabase, DumpKind, DumpState, OnIndeterminate,
    scan::{DumpRunConfig, SourceOptions, prepare_epoch, run_dump},
};
use blockzilla_primitives::{CompactPubkey, wincode_leb128_config};
use blockzilla_registry::{KeyIndex, write_registry};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const PAYER: [u8; 32] = [1; 32];
const PUMP: [u8; 32] = [2; 32];
const USDC: [u8; 32] = [3; 32];
const OWNER: [u8; 32] = [4; 32];
const TOKEN_PROGRAM: [u8; 32] = [5; 32];

struct ArchiveFixture {
    _directory: Option<TempDir>,
    root: PathBuf,
    epoch: u64,
}

impl ArchiveFixture {
    fn temporary(epoch: u64, indeterminate_block: bool) -> Self {
        let directory = TempDir::new().unwrap();
        let root = directory.path().to_path_buf();
        build_archive(&root, epoch, indeterminate_block);
        Self {
            _directory: Some(directory),
            root,
            epoch,
        }
    }

    fn at(root: PathBuf, epoch: u64, indeterminate_block: bool) -> Self {
        fs::create_dir_all(&root).unwrap();
        build_archive(&root, epoch, indeterminate_block);
        Self {
            _directory: None,
            root,
            epoch,
        }
    }
}

fn build_archive(root: &Path, epoch: u64, indeterminate_block: bool) {
    fs::write(
        root.join(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE),
        COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES,
    )
    .unwrap();
    let keys = [PAYER, PUMP, USDC, OWNER, TOKEN_PROGRAM];
    write_registry(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), &keys).unwrap();
    KeyIndex::build_from_slice(&keys)
        .write(&root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
        .unwrap();

    let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
        header: CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 4,
        },
        account_keys: vec![
            CompactPubkey::Id(1),
            CompactPubkey::Id(2),
            CompactPubkey::Id(3),
            CompactPubkey::Id(4),
            CompactPubkey::Id(5),
        ],
        recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
        instructions: vec![ArchiveV2HotInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: ArchiveV2HotInstructionData::Raw(vec![1]),
        }],
    });
    let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
    let metadata = CompactMetaV1 {
        err: None,
        fee: 5_000,
        pre_balances: vec![1_000_000],
        post_balances: vec![995_000],
        inner_instructions: Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![2],
                stack_height: Some(2),
            }],
        }]),
        logs: None,
        pre_token_balances: vec![CompactTokenBalance {
            account_index: 2,
            mint: Some(CompactPubkey::Id(3)),
            owner: Some(CompactPubkey::Id(4)),
            program_id: Some(CompactPubkey::Id(5)),
            amount: 1_000_000,
            decimals: 6,
        }],
        post_token_balances: vec![CompactTokenBalance {
            account_index: 2,
            mint: Some(CompactPubkey::Id(3)),
            owner: Some(CompactPubkey::Id(4)),
            program_id: Some(CompactPubkey::Id(5)),
            amount: 900_000,
            decimals: 6,
        }],
        rewards: Vec::new(),
        loaded_writable_addresses: Vec::new(),
        loaded_readonly_addresses: Vec::new(),
        return_data: None,
        compute_units_consumed: Some(25_000),
        cost_units: None,
    };
    let metadata = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
    let first_slot = epoch * 100 + 1;
    let first_block = block(
        first_slot,
        message.clone(),
        metadata,
        ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES,
    );
    let mut blocks = vec![first_block];
    if indeterminate_block {
        blocks.push(block(
            first_slot + 1,
            message,
            vec![0xff],
            ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
        ));
    }

    let mut compressed_file = Vec::new();
    let mut index_rows = Vec::new();
    for (block_id, block) in blocks.iter().enumerate() {
        let uncompressed = wincode::config::serialize(block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
        let offset = compressed_file.len() as u64;
        compressed_file.extend_from_slice(&compressed);
        index_rows.push(ArchiveV2HotBlockIndexRow {
            block_id: block_id as u32,
            slot: block.header.slot,
            compressed_offset: offset,
            compressed_len: compressed.len() as u32,
            uncompressed_len: uncompressed.len() as u32,
            tx_count: 1,
            first_tx_ordinal: block_id as u64,
            first_signature_ordinal: block_id as u64,
            signature_count: 1,
        });
    }
    fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed_file).unwrap();
    write_archive_v2_hot_block_index(
        &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        compressed_file.len() as u64,
        3,
        0,
        &index_rows,
    )
    .unwrap();
    let signatures = (0..blocks.len())
        .flat_map(|index| [epoch as u8 + index as u8; 64])
        .collect::<Vec<_>>();
    fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), signatures).unwrap();

    let records = [
        ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }),
        ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: blocks.len() as u64,
            transactions: blocks.len() as u64,
            metadata_raw_fallbacks: u64::from(indeterminate_block),
            ..WincodeArchiveV2Footer::default()
        }),
    ];
    let mut meta = Vec::new();
    for record in records {
        let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
        write_varint(&mut meta, bytes.len() as u32);
        meta.extend_from_slice(&bytes);
    }
    fs::write(root.join(ARCHIVE_V2_META_FILE), meta).unwrap();
    write_manifest(root, epoch);
}

fn block(slot: u64, message: Vec<u8>, metadata: Vec<u8>, flags: u32) -> ArchiveV2HotBlockBlob {
    ArchiveV2HotBlockBlob {
        header: ArchiveV2HotBlockHeader {
            slot,
            parent_slot: slot - 1,
            blockhash_id: slot as u32,
            previous_blockhash_id: slot as u32 - 1,
            block_time: Some(slot as i64),
            block_height: Some(slot),
            rewards: None,
        },
        tx_count: 1,
        tx_rows: vec![ArchiveV2HotTxRow {
            tx_index: 0,
            flags,
            message_offset: 0,
            message_len: message.len() as u32,
            metadata_offset: 0,
            metadata_len: metadata.len() as u32,
            signature_count: 1,
            reserved: [0; 3],
        }],
        message_bytes: message,
        metadata_bytes: metadata,
    }
}

fn write_manifest(root: &Path, epoch: u64) {
    let mut files = Vec::new();
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
    ] {
        let bytes = fs::read(root.join(name)).unwrap();
        files.push(GenerationFile {
            name: name.into(),
            size: bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(&bytes)),
        });
    }
    let mut manifest = GenerationManifest {
        schema_version: 1,
        cluster_id: "testnet".into(),
        epoch,
        generation_id: format!("test-generation-{epoch}"),
        generation_digest: "0".repeat(64),
        slots_per_epoch: 100,
        complete: true,
        files,
    };
    manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
    fs::write(
        root.join(GENERATION_MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

fn write_varint(output: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

struct MockGateway {
    base_url: String,
    stop: Arc<AtomicBool>,
    address: std::net::SocketAddr,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockGateway {
    fn start(fixture: &ArchiveFixture) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let root = fixture.root.clone();
        let epoch = fixture.epoch;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => serve_request(stream, &root, epoch),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            base_url: format!("http://{address}"),
            stop,
            address,
            thread: Some(thread),
        }
    }
}

impl Drop for MockGateway {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve_request(mut stream: TcpStream, root: &Path, epoch: u64) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut buffer).unwrap();
        if count == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..count]);
    }
    let request = String::from_utf8(request).unwrap();
    let mut lines = request.split("\r\n");
    let first = lines.next().unwrap();
    let mut first_parts = first.split_whitespace();
    let method = first_parts.next().unwrap();
    let path = first_parts.next().unwrap();
    let prefix = format!("/v1/epochs/{epoch}/");
    let object = if path == format!("{prefix}manifest") {
        Some(GENERATION_MANIFEST_FILE)
    } else {
        path.strip_prefix(&format!("{prefix}files/"))
    };
    let Some(object) = object else {
        write_response(&mut stream, 404, &[], None);
        return;
    };
    let bytes = match fs::read(root.join(object)) {
        Ok(bytes) => bytes,
        Err(_) => {
            write_response(&mut stream, 404, &[], None);
            return;
        }
    };
    if method == "HEAD" {
        write_response(
            &mut stream,
            200,
            &[],
            Some((0, 0, bytes.len(), bytes.len())),
        );
        return;
    }
    if object == GENERATION_MANIFEST_FILE {
        write_response(&mut stream, 200, &bytes, None);
        return;
    }
    let range = lines
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            let range = lower.strip_prefix("range: bytes=")?;
            let (start, end) = range.split_once('-')?;
            Some((
                start.parse::<usize>().unwrap(),
                end.parse::<usize>().unwrap(),
            ))
        })
        .unwrap();
    let body = &bytes[range.0..=range.1];
    write_response(
        &mut stream,
        206,
        body,
        Some((range.0, range.1, bytes.len(), body.len())),
    );
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    range: Option<(usize, usize, usize, usize)>,
) {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        _ => "Not Found",
    };
    let content_length = range.map(|value| value.3).unwrap_or(body.len());
    let mut header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {content_length}\r\nConnection: close\r\n"
    );
    if status == 206
        && let Some((start, end, total, _)) = range
    {
        header.push_str(&format!("Content-Range: bytes {start}-{end}/{total}\r\n"));
    }
    header.push_str("\r\n");
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
}

fn gateway_source(gateway: &MockGateway, cache: &Path) -> SourceOptions {
    SourceOptions {
        archive: None,
        gateway: Some(gateway.base_url.clone()),
        bearer_token: None,
        cache: Some(cache.to_path_buf()),
        allow_insecure_http: true,
        cluster_id: "testnet".into(),
        local_generation_prefix: None,
        epoch_zero_first_slot: 0,
        slots_per_epoch: 100,
        message_schema: CompactV2MessageSchema::Current,
        metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
    }
}

fn query_count(path: &Path, sql: &str) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

#[test]
fn gateway_program_and_token_scans_store_direct_cpi_and_pre_post_rows() {
    let fixture = ArchiveFixture::temporary(7, false);
    let gateway = MockGateway::start(&fixture);
    let work = TempDir::new().unwrap();
    let cache = work.path().join("cache");

    let program_output = work.path().join("pump.sqlite");
    let program = run_dump(&DumpRunConfig {
        source: gateway_source(&gateway, &cache),
        epochs: vec![7],
        output: program_output.clone(),
        threads: 2,
        on_indeterminate: OnIndeterminate::Fail,
        kind: DumpKind::Program,
        target_pubkey: PUMP,
    })
    .unwrap();
    assert!(!program.partial);
    let connection = Connection::open(&program_output).unwrap();
    let counts: (i64, i64) = connection
        .query_row(
            "SELECT direct_count, cpi_count FROM program_matches",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (1, 1));
    assert_eq!(
        query_count(&program_output, "SELECT count(*) FROM transaction_accounts"),
        5
    );
    assert_eq!(
        query_count(
            &program_output,
            "SELECT count(*) FROM transaction_signatures"
        ),
        1
    );

    let token_output = work.path().join("usdc.sqlite");
    let token = run_dump(&DumpRunConfig {
        source: gateway_source(&gateway, &cache),
        epochs: vec![7],
        output: token_output.clone(),
        threads: 2,
        on_indeterminate: OnIndeterminate::Fail,
        kind: DumpKind::Token,
        target_pubkey: USDC,
    })
    .unwrap();
    assert!(!token.partial);
    let connection = Connection::open(&token_output).unwrap();
    let counts: (i64, i64) = connection
        .query_row(
            "SELECT pre_count, post_count FROM token_matches",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (1, 1));
    let amounts = connection
        .prepare("SELECT side, amount_u64 FROM token_balances ORDER BY side")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        amounts,
        [
            ("post".into(), "900000".into()),
            ("pre".into(), "1000000".into())
        ]
    );

    let prepared = prepare_epoch(&gateway_source(&gateway, &cache), 7).unwrap();
    assert_eq!(
        fs::read(
            prepared
                .source_root
                .join(COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE)
        )
        .unwrap(),
        COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES
    );
    let mphf = prepared
        .source_root
        .join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let expected = fs::read(fixture.root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)).unwrap();
    let mut corrupt = expected.clone();
    corrupt[0] ^= 0xff;
    fs::write(&mphf, corrupt).unwrap();
    prepare_epoch(&gateway_source(&gateway, &cache), 7).unwrap();
    assert_eq!(fs::read(mphf).unwrap(), expected);
}

#[test]
fn indeterminate_record_and_skip_finish_partial_and_resume_without_duplicates() {
    let fixture = ArchiveFixture::temporary(8, true);
    let gateway = MockGateway::start(&fixture);
    let work = TempDir::new().unwrap();
    let cache = work.path().join("cache");
    let fail_output = work.path().join("fail.sqlite");
    let fail_config = DumpRunConfig {
        source: gateway_source(&gateway, &cache),
        epochs: vec![8],
        output: fail_output.clone(),
        threads: 2,
        on_indeterminate: OnIndeterminate::Fail,
        kind: DumpKind::Program,
        target_pubkey: PUMP,
    };
    assert!(run_dump(&fail_config).is_err());
    let failed = DumpDatabase::read_status(&fail_output).unwrap();
    assert_eq!(failed.state, DumpState::Failed);
    assert_eq!(failed.transaction_rows, 1);
    assert_eq!(failed.epochs[0].checkpoint.next_block_row, 1);
    assert!(run_dump(&fail_config).is_err());
    let resumed_failure = DumpDatabase::read_status(&fail_output).unwrap();
    assert_eq!(resumed_failure.transaction_rows, 1);
    assert_eq!(resumed_failure.epochs[0].checkpoint.next_block_row, 1);

    let output = work.path().join("record.sqlite");
    let config = DumpRunConfig {
        source: gateway_source(&gateway, &cache),
        epochs: vec![8],
        output: output.clone(),
        threads: 2,
        on_indeterminate: OnIndeterminate::Record,
        kind: DumpKind::Program,
        target_pubkey: PUMP,
    };
    assert!(run_dump(&config).unwrap().partial);
    let status = DumpDatabase::read_status(&output).unwrap();
    assert_eq!(status.state, DumpState::CompleteWithGaps);
    assert_eq!(status.transaction_rows, 1);
    assert_eq!(status.coverage_issue_rows, 1);
    assert_eq!(status.epochs[0].checkpoint.next_block_row, 2);

    assert!(run_dump(&config).unwrap().partial);
    let resumed = DumpDatabase::read_status(&output).unwrap();
    assert_eq!(resumed.transaction_rows, 1);
    assert_eq!(resumed.coverage_issue_rows, 1);

    let skip_output = work.path().join("skip.sqlite");
    let skip = run_dump(&DumpRunConfig {
        source: gateway_source(&gateway, &cache),
        epochs: vec![8],
        output: skip_output.clone(),
        threads: 1,
        on_indeterminate: OnIndeterminate::Skip,
        kind: DumpKind::Program,
        target_pubkey: PUMP,
    })
    .unwrap();
    assert!(skip.partial);
    let status = DumpDatabase::read_status(&skip_output).unwrap();
    assert_eq!(status.state, DumpState::CompleteWithGaps);
    assert_eq!(status.coverage_issue_rows, 0);
    assert_eq!(status.epochs[0].checkpoint.indeterminate_transactions, 1);
}

#[test]
fn local_archive_root_resolves_multiple_epoch_children_and_binds_each_path() {
    let work = TempDir::new().unwrap();
    let archive_root = work.path().join("epochs");
    let first = ArchiveFixture::at(archive_root.join("epoch-9"), 9, false);
    let second = ArchiveFixture::at(archive_root.join("epoch-10"), 10, false);
    let output = work.path().join("local.sqlite");
    let result = run_dump(&DumpRunConfig {
        source: SourceOptions {
            archive: Some(archive_root.clone()),
            gateway: None,
            bearer_token: None,
            cache: None,
            allow_insecure_http: false,
            cluster_id: "testnet".into(),
            local_generation_prefix: None,
            epoch_zero_first_slot: 0,
            slots_per_epoch: 100,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
        },
        epochs: vec![9, 10],
        output: output.clone(),
        threads: 2,
        on_indeterminate: OnIndeterminate::Fail,
        kind: DumpKind::Program,
        target_pubkey: PUMP,
    })
    .unwrap();
    assert!(!result.partial);
    let status = DumpDatabase::read_status(&output).unwrap();
    assert_eq!(status.transaction_rows, 2);
    assert_eq!(status.epochs.len(), 2);
    assert!(
        status.epochs[0]
            .source_identity
            .as_deref()
            .unwrap()
            .contains(first.root.to_str().unwrap())
    );
    assert!(
        status.epochs[1]
            .source_identity
            .as_deref()
            .unwrap()
            .contains(second.root.to_str().unwrap())
    );
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from_digit(u32::from(byte >> 4), 16).unwrap());
        output.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap());
    }
    output
}
