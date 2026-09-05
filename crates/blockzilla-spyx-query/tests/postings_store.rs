use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use blockzilla_spyx_query::{
    PostingLookupKind, PostingsOpenOptions, PostingsStore,
    index_format::IndexFileBinding,
    postings_format::{
        POSTINGS_HEADER_BYTES, POSTINGS_MANIFEST_FILE, POSTINGS_SCHEMA_VERSION,
        PROGRAM_INSTRUCTION_SCOPE_DIRECT, PROGRAM_INSTRUCTION_SCOPE_INNER, PostingRecord,
        PostingsDirectoryKind, PostingsDirectoryRecord, PostingsFileHeader, PostingsFileKind,
        PostingsManifest, PostingsSourceBinding, ProgramInstructionScope, ProgramPostingRecord,
        TARGET_ADDRESS_FLAG_MINT, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT, postings_semantic_sha256,
        program_postings_semantic_sha256,
    },
    verify_postings_artifact,
};
use blockzilla_token_transaction_dump::{
    ACCOUNTS_FILE, DUMP_MANIFEST_FILE, DUMP_SCHEMA_VERSION, DumpArtifactKind, DumpManifest,
    DumpSourceBinding, DumpWireProfile, PUBKEY_REGISTRY_FILE, SIGNATURES_FILE, TRANSACTIONS_FILE,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const SOURCE_TRANSACTIONS: u64 = 5;

struct Fixture {
    _temporary: TempDir,
    dump: PathBuf,
    postings: PathBuf,
    token_key: [u8; 32],
    mint_key: [u8; 32],
    program_key: [u8; 32],
}

fn fixture() -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let dump = temporary.path().join("dump");
    let postings = temporary.path().join("postings");
    fs::create_dir(&dump).unwrap();
    fs::create_dir(&postings).unwrap();

    let token_key = [1u8; 32];
    let mint_key = [2u8; 32];
    let program_key = [3u8; 32];
    let registry = [token_key, mint_key, program_key]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let transactions = vec![71, 72, 73];
    let signatures = vec![81; 64];
    let accounts = vec![91, 92];
    fs::write(dump.join(TRANSACTIONS_FILE), &transactions).unwrap();
    fs::write(dump.join(SIGNATURES_FILE), &signatures).unwrap();
    fs::write(dump.join(PUBKEY_REGISTRY_FILE), &registry).unwrap();
    fs::write(dump.join(ACCOUNTS_FILE), &accounts).unwrap();

    let source_manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::Consolidated,
        complete: true,
        mint: bs58::encode(mint_key).into_string(),
        mint_slot: 1_001,
        mint_signature: bs58::encode([8u8; 64]).into_string(),
        workers: 1,
        source_binding: DumpSourceBinding::TrustedLocalSizesOnly {
            cluster_id: "postings-store-test".to_owned(),
            slots_per_epoch: 1_000,
            wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
        },
        first_epoch: 1,
        last_epoch: 1,
        transactions: SOURCE_TRANSACTIONS,
        signatures: Some(1),
        pubkeys: Some(3),
        transaction_stream: TRANSACTIONS_FILE.to_owned(),
        transaction_stream_sha256: Some(digest_hex(&transactions)),
        account_id_log: None,
        account_id_log_sha256: None,
        discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
        discovered_accounts_sha256: Some(digest_hex(&accounts)),
        discovered_account_count: Some(1),
        signature_stream: Some(SIGNATURES_FILE.to_owned()),
        signature_stream_sha256: Some(digest_hex(&signatures)),
        pubkey_registry: Some(PUBKEY_REGISTRY_FILE.to_owned()),
        pubkey_registry_sha256: Some(digest_hex(&registry)),
        registry_maps: None,
    };
    fs::write(
        dump.join(DUMP_MANIFEST_FILE),
        serde_json::to_vec_pretty(&source_manifest).unwrap(),
    )
    .unwrap();

    write_normal_postings(&dump, &postings);
    Fixture {
        _temporary: temporary,
        dump,
        postings,
        token_key,
        mint_key,
        program_key,
    }
}

fn normal_target_data() -> (Vec<PostingsDirectoryRecord>, Vec<PostingRecord>) {
    (
        vec![
            PostingsDirectoryRecord {
                registry_id: 1,
                flags: TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                first_posting_row: 0,
                posting_count: 3,
            },
            PostingsDirectoryRecord {
                registry_id: 2,
                flags: TARGET_ADDRESS_FLAG_MINT,
                first_posting_row: 3,
                posting_count: 2,
            },
        ],
        [0, 2, 4, 1, 3]
            .into_iter()
            .map(|transaction_ordinal| PostingRecord {
                transaction_ordinal,
            })
            .collect(),
    )
}

fn normal_program_data() -> (Vec<PostingsDirectoryRecord>, Vec<ProgramPostingRecord>) {
    (
        vec![PostingsDirectoryRecord {
            registry_id: 3,
            flags: 0,
            first_posting_row: 0,
            posting_count: 2,
        }],
        vec![
            ProgramPostingRecord {
                transaction_ordinal: 0,
                instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
            },
            ProgramPostingRecord {
                transaction_ordinal: 4,
                instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_INNER,
            },
        ],
    )
}

fn write_normal_postings(dump: &Path, postings: &Path) {
    let (target_directory, target_postings) = normal_target_data();
    let (program_directory, program_postings) = normal_program_data();
    write_postings_set(
        dump,
        postings,
        &target_directory,
        &target_postings,
        &program_directory,
        &program_postings,
    );
}

fn write_postings_set(
    dump: &Path,
    postings: &Path,
    target_directory: &[PostingsDirectoryRecord],
    target_postings: &[PostingRecord],
    program_directory: &[PostingsDirectoryRecord],
    program_postings: &[ProgramPostingRecord],
) {
    let source_manifest_bytes = fs::read(dump.join(DUMP_MANIFEST_FILE)).unwrap();
    let source_manifest: DumpManifest = serde_json::from_slice(&source_manifest_bytes).unwrap();
    let transaction_bytes = fs::read(dump.join(TRANSACTIONS_FILE)).unwrap();
    let registry_bytes = fs::read(dump.join(PUBKEY_REGISTRY_FILE)).unwrap();
    let accounts_bytes = fs::read(dump.join(ACCOUNTS_FILE)).unwrap();
    let source_manifest_sha256 = digest_array(&source_manifest_bytes);
    let source_transaction_sha256 = digest_array(&transaction_bytes);

    let target_directory_binding = write_directory_file(
        postings,
        PostingsFileKind::TargetAddressDirectory,
        PostingsDirectoryKind::TargetAddress,
        target_directory,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let target_postings_binding = write_body_file(
        postings,
        PostingsFileKind::TargetAddressPostings,
        target_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let program_directory_binding = write_directory_file(
        postings,
        PostingsFileKind::ProgramDirectory,
        PostingsDirectoryKind::Program,
        program_directory,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let program_postings_binding = write_program_body_file(
        postings,
        PostingsFileKind::ProgramPostings,
        program_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let (program_direct_directory, program_direct_postings) = scoped_program_data(
        program_directory,
        program_postings,
        ProgramInstructionScope::Direct,
    );
    let program_direct_directory_binding = write_directory_file(
        postings,
        PostingsFileKind::ProgramDirectDirectory,
        PostingsDirectoryKind::Program,
        &program_direct_directory,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let program_direct_postings_binding = write_program_body_file(
        postings,
        PostingsFileKind::ProgramDirectPostings,
        &program_direct_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let (program_inner_directory, program_inner_postings) = scoped_program_data(
        program_directory,
        program_postings,
        ProgramInstructionScope::Inner,
    );
    let program_inner_directory_binding = write_directory_file(
        postings,
        PostingsFileKind::ProgramInnerDirectory,
        PostingsDirectoryKind::Program,
        &program_inner_directory,
        source_manifest_sha256,
        source_transaction_sha256,
    );
    let program_inner_postings_binding = write_program_body_file(
        postings,
        PostingsFileKind::ProgramInnerPostings,
        &program_inner_postings,
        source_manifest_sha256,
        source_transaction_sha256,
    );

    let manifest = PostingsManifest {
        schema_version: POSTINGS_SCHEMA_VERSION,
        artifact_kind: PostingsManifest::ARTIFACT_KIND.to_owned(),
        complete: false,
        canary_max_transactions: Some(SOURCE_TRANSACTIONS),
        transactions: SOURCE_TRANSACTIONS,
        created_unix_seconds: 1,
        source: PostingsSourceBinding {
            manifest_file: DUMP_MANIFEST_FILE.to_owned(),
            manifest_bytes: u64::try_from(source_manifest_bytes.len()).unwrap(),
            manifest_sha256: digest_hex(&source_manifest_bytes),
            transaction_file: TRANSACTIONS_FILE.to_owned(),
            transaction_bytes: u64::try_from(transaction_bytes.len()).unwrap(),
            transaction_sha256: digest_hex(&transaction_bytes),
            registry_file: PUBKEY_REGISTRY_FILE.to_owned(),
            registry_bytes: u64::try_from(registry_bytes.len()).unwrap(),
            registry_sha256: digest_hex(&registry_bytes),
            accounts_file: ACCOUNTS_FILE.to_owned(),
            accounts_bytes: u64::try_from(accounts_bytes.len()).unwrap(),
            accounts_sha256: digest_hex(&accounts_bytes),
            transactions: SOURCE_TRANSACTIONS,
            pubkeys: source_manifest.pubkeys.unwrap(),
            accounts: source_manifest.discovered_account_count.unwrap(),
        },
        target_address_semantic_sha256: digest_hex_array(
            postings_semantic_sha256(
                PostingsDirectoryKind::TargetAddress,
                target_directory,
                target_postings,
                SOURCE_TRANSACTIONS,
            )
            .unwrap(),
        ),
        program_semantic_sha256: digest_hex_array(
            program_postings_semantic_sha256(
                ProgramInstructionScope::All,
                program_directory,
                program_postings,
                SOURCE_TRANSACTIONS,
            )
            .unwrap(),
        ),
        program_direct_semantic_sha256: digest_hex_array(
            program_postings_semantic_sha256(
                ProgramInstructionScope::Direct,
                &program_direct_directory,
                &program_direct_postings,
                SOURCE_TRANSACTIONS,
            )
            .unwrap(),
        ),
        program_inner_semantic_sha256: digest_hex_array(
            program_postings_semantic_sha256(
                ProgramInstructionScope::Inner,
                &program_inner_directory,
                &program_inner_postings,
                SOURCE_TRANSACTIONS,
            )
            .unwrap(),
        ),
        target_address_directory: target_directory_binding,
        target_address_postings: target_postings_binding,
        program_directory: program_directory_binding,
        program_postings: program_postings_binding,
        program_direct_directory: program_direct_directory_binding,
        program_direct_postings: program_direct_postings_binding,
        program_inner_directory: program_inner_directory_binding,
        program_inner_postings: program_inner_postings_binding,
    };
    write_manifest(postings, &manifest);
}

fn write_directory_file(
    root: &Path,
    file_kind: PostingsFileKind,
    directory_kind: PostingsDirectoryKind,
    records: &[PostingsDirectoryRecord],
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
) -> IndexFileBinding {
    let mut bytes = PostingsFileHeader {
        kind: file_kind,
        complete: false,
        record_count: u64::try_from(records.len()).unwrap(),
        source_manifest_sha256,
        source_transaction_sha256,
    }
    .encode()
    .to_vec();
    for record in records {
        bytes.extend_from_slice(&record.encode(directory_kind).unwrap());
    }
    write_bound_file(root, file_kind, bytes, records.len())
}

fn write_body_file(
    root: &Path,
    file_kind: PostingsFileKind,
    records: &[PostingRecord],
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
) -> IndexFileBinding {
    let mut bytes = PostingsFileHeader {
        kind: file_kind,
        complete: false,
        record_count: u64::try_from(records.len()).unwrap(),
        source_manifest_sha256,
        source_transaction_sha256,
    }
    .encode()
    .to_vec();
    for record in records {
        bytes.extend_from_slice(&record.encode());
    }
    write_bound_file(root, file_kind, bytes, records.len())
}

fn write_program_body_file(
    root: &Path,
    file_kind: PostingsFileKind,
    records: &[ProgramPostingRecord],
    source_manifest_sha256: [u8; 32],
    source_transaction_sha256: [u8; 32],
) -> IndexFileBinding {
    let mut bytes = PostingsFileHeader {
        kind: file_kind,
        complete: false,
        record_count: u64::try_from(records.len()).unwrap(),
        source_manifest_sha256,
        source_transaction_sha256,
    }
    .encode()
    .to_vec();
    for record in records {
        bytes.extend_from_slice(&record.encode().unwrap());
    }
    write_bound_file(root, file_kind, bytes, records.len())
}

fn scoped_program_data(
    directory: &[PostingsDirectoryRecord],
    postings: &[ProgramPostingRecord],
    scope: ProgramInstructionScope,
) -> (Vec<PostingsDirectoryRecord>, Vec<ProgramPostingRecord>) {
    let mut scoped_directory = Vec::with_capacity(directory.len());
    let mut scoped_postings = Vec::new();
    for record in directory {
        let first = usize::try_from(record.first_posting_row).unwrap();
        let end = usize::try_from(record.end_posting_row().unwrap()).unwrap();
        let first_posting_row = u64::try_from(scoped_postings.len()).unwrap();
        scoped_postings.extend(
            postings[first..end]
                .iter()
                .copied()
                .filter(|posting| scope.includes(posting.instruction_scope_mask)),
        );
        scoped_directory.push(PostingsDirectoryRecord {
            registry_id: record.registry_id,
            flags: 0,
            first_posting_row,
            posting_count: u64::try_from(scoped_postings.len()).unwrap() - first_posting_row,
        });
    }
    (scoped_directory, scoped_postings)
}

fn write_bound_file(
    root: &Path,
    kind: PostingsFileKind,
    bytes: Vec<u8>,
    records: usize,
) -> IndexFileBinding {
    fs::write(root.join(kind.file_name()), &bytes).unwrap();
    IndexFileBinding {
        file: kind.file_name().to_owned(),
        bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: digest_hex(&bytes),
        records: u64::try_from(records).unwrap(),
        record_bytes: kind.record_bytes(),
    }
}

fn read_manifest(postings: &Path) -> PostingsManifest {
    serde_json::from_slice(&fs::read(postings.join(POSTINGS_MANIFEST_FILE)).unwrap()).unwrap()
}

fn write_manifest(postings: &Path, manifest: &PostingsManifest) {
    let mut bytes = serde_json::to_vec_pretty(manifest).unwrap();
    bytes.push(b'\n');
    fs::write(postings.join(POSTINGS_MANIFEST_FILE), bytes).unwrap();
}

fn binding_mut(manifest: &mut PostingsManifest, kind: PostingsFileKind) -> &mut IndexFileBinding {
    match kind {
        PostingsFileKind::TargetAddressDirectory => &mut manifest.target_address_directory,
        PostingsFileKind::TargetAddressPostings => &mut manifest.target_address_postings,
        PostingsFileKind::ProgramDirectory => &mut manifest.program_directory,
        PostingsFileKind::ProgramPostings => &mut manifest.program_postings,
        PostingsFileKind::ProgramDirectDirectory => &mut manifest.program_direct_directory,
        PostingsFileKind::ProgramDirectPostings => &mut manifest.program_direct_postings,
        PostingsFileKind::ProgramInnerDirectory => &mut manifest.program_inner_directory,
        PostingsFileKind::ProgramInnerPostings => &mut manifest.program_inner_postings,
    }
}

fn refresh_file_digest(postings: &Path, kind: PostingsFileKind) {
    let bytes = fs::read(postings.join(kind.file_name())).unwrap();
    let mut manifest = read_manifest(postings);
    let binding = binding_mut(&mut manifest, kind);
    binding.bytes = u64::try_from(bytes.len()).unwrap();
    binding.sha256 = digest_hex(&bytes);
    write_manifest(postings, &manifest);
}

fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn open_canary(fixture: &Fixture) -> anyhow::Result<PostingsStore> {
    PostingsStore::open_with_options(
        &fixture.dump,
        &fixture.postings,
        PostingsOpenOptions {
            allow_incomplete: true,
        },
    )
}

fn digest_array(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn digest_hex(bytes: &[u8]) -> String {
    digest_hex_array(digest_array(bytes))
}

fn digest_hex_array(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[test]
fn normal_pages_are_bounded_and_roles_and_absent_keys_are_exact() {
    let fixture = fixture();
    assert!(PostingsStore::open(&fixture.dump, &fixture.postings).is_err());
    let store = open_canary(&fixture).unwrap();
    assert!(!store.complete());
    assert_eq!(store.transaction_count(), SOURCE_TRANSACTIONS);

    let page = store
        .lookup(PostingLookupKind::TokenAccount, fixture.token_key, 1, 1)
        .unwrap()
        .unwrap();
    assert_eq!(page.registry_id, 1);
    assert_eq!(page.flags, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT);
    assert_eq!(page.total, 3);
    assert_eq!(page.offset, 1);
    assert_eq!(page.transaction_ordinals, [2]);
    assert_eq!(page.next_offset, Some(2));

    let mint = store
        .lookup(PostingLookupKind::TargetAddress, fixture.mint_key, 0, 200)
        .unwrap()
        .unwrap();
    assert_eq!(mint.flags, TARGET_ADDRESS_FLAG_MINT);
    assert_eq!(mint.transaction_ordinals, [1, 3]);
    assert!(
        store
            .lookup(PostingLookupKind::TokenAccount, fixture.mint_key, 0, 1)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .lookup(PostingLookupKind::Program, fixture.program_key, 0, 200)
            .unwrap()
            .unwrap()
            .transaction_ordinals,
        [0, 4]
    );
    let direct = store
        .lookup_program(fixture.program_key, ProgramInstructionScope::Direct, 0, 200)
        .unwrap()
        .unwrap();
    assert_eq!(direct.total, 1);
    assert_eq!(direct.transaction_ordinals, [0]);
    let inner = store
        .lookup_program(fixture.program_key, ProgramInstructionScope::Inner, 0, 200)
        .unwrap()
        .unwrap();
    assert_eq!(inner.total, 1);
    assert_eq!(inner.transaction_ordinals, [4]);
    assert!(
        store
            .lookup(PostingLookupKind::TargetAddress, fixture.program_key, 0, 1)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .lookup(PostingLookupKind::TargetAddress, [4; 32], 0, 1)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .lookup(PostingLookupKind::TargetAddress, fixture.token_key, 0, 0)
            .is_err()
    );
    assert!(
        store
            .lookup(PostingLookupKind::TargetAddress, fixture.token_key, 0, 201)
            .is_err()
    );
    assert!(
        store
            .lookup(PostingLookupKind::TargetAddress, fixture.token_key, 4, 1)
            .is_err()
    );
    let base58 = bs58::encode(fixture.token_key).into_string();
    assert_eq!(
        store
            .lookup_base58(PostingLookupKind::TokenAccount, &base58, 0, 2)
            .unwrap()
            .unwrap()
            .transaction_ordinals,
        [0, 2]
    );
}

#[test]
fn empty_generic_directory_and_body_files_are_valid() {
    let fixture = fixture();
    write_postings_set(&fixture.dump, &fixture.postings, &[], &[], &[], &[]);
    {
        let store = open_canary(&fixture).unwrap();
        assert_eq!(store.target_address_key_count(), 0);
        assert_eq!(store.target_address_posting_count(), 0);
        assert_eq!(store.program_key_count(), 0);
        assert_eq!(store.program_posting_count(), 0);
        assert!(
            store
                .lookup(PostingLookupKind::TargetAddress, fixture.token_key, 0, 200)
                .unwrap()
                .is_none()
        );
    }

    let zero_ranges = [
        PostingsDirectoryRecord {
            registry_id: 1,
            flags: TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
            first_posting_row: 0,
            posting_count: 0,
        },
        PostingsDirectoryRecord {
            registry_id: 2,
            flags: TARGET_ADDRESS_FLAG_MINT,
            first_posting_row: 0,
            posting_count: 0,
        },
    ];
    write_postings_set(
        &fixture.dump,
        &fixture.postings,
        &zero_ranges,
        &[],
        &[],
        &[],
    );
    let store = open_canary(&fixture).unwrap();
    let empty_page = store
        .lookup(PostingLookupKind::TokenAccount, fixture.token_key, 0, 200)
        .unwrap()
        .unwrap();
    assert_eq!(empty_page.total, 0);
    assert!(empty_page.transaction_ordinals.is_empty());
    assert_eq!(empty_page.next_offset, None);
}

#[test]
fn wrong_artifact_role_is_rejected_even_for_a_canary() {
    let fixture = fixture();
    let path = fixture
        .postings
        .join(PostingsFileKind::TargetAddressDirectory.file_name());
    overwrite(
        &path,
        POSTINGS_HEADER_BYTES as u64 + 4,
        &TARGET_ADDRESS_FLAG_MINT.to_le_bytes(),
    );
    refresh_file_digest(&fixture.postings, PostingsFileKind::TargetAddressDirectory);
    assert!(open_canary(&fixture).is_err());
}

#[test]
fn truncation_header_corruption_order_and_range_fail_closed() {
    let truncated = fixture();
    let path = truncated
        .postings
        .join(PostingsFileKind::TargetAddressPostings.file_name());
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    assert!(open_canary(&truncated).is_err());

    let header = fixture();
    let path = header
        .postings
        .join(PostingsFileKind::ProgramPostings.file_name());
    overwrite(&path, (POSTINGS_HEADER_BYTES - 1) as u64, &[1]);
    refresh_file_digest(&header.postings, PostingsFileKind::ProgramPostings);
    assert!(open_canary(&header).is_err());

    let order = fixture();
    let path = order
        .postings
        .join(PostingsFileKind::TargetAddressDirectory.file_name());
    overwrite(
        &path,
        POSTINGS_HEADER_BYTES as u64 + 24,
        &1u32.to_le_bytes(),
    );
    refresh_file_digest(&order.postings, PostingsFileKind::TargetAddressDirectory);
    assert!(open_canary(&order).is_err());

    let range = fixture();
    let path = range
        .postings
        .join(PostingsFileKind::TargetAddressDirectory.file_name());
    overwrite(
        &path,
        POSTINGS_HEADER_BYTES as u64 + 24 + 8,
        &2u64.to_le_bytes(),
    );
    refresh_file_digest(&range.postings, PostingsFileKind::TargetAddressDirectory);
    assert!(open_canary(&range).is_err());
}

#[test]
fn corrupt_body_file_digest_semantic_digest_and_source_bytes_fail_closed() {
    let corrupt = fixture();
    let path = corrupt
        .postings
        .join(PostingsFileKind::ProgramPostings.file_name());
    overwrite(&path, POSTINGS_HEADER_BYTES as u64, &99u64.to_le_bytes());
    refresh_file_digest(&corrupt.postings, PostingsFileKind::ProgramPostings);
    assert!(open_canary(&corrupt).is_err());

    let wrong_digest = fixture();
    let mut manifest = read_manifest(&wrong_digest.postings);
    manifest.program_postings.sha256 = "00".repeat(32);
    write_manifest(&wrong_digest.postings, &manifest);
    assert!(open_canary(&wrong_digest).is_err());

    let wrong_semantic = fixture();
    let path = wrong_semantic
        .postings
        .join(PostingsFileKind::TargetAddressPostings.file_name());
    overwrite(
        &path,
        POSTINGS_HEADER_BYTES as u64 + 2 * 8,
        &3u64.to_le_bytes(),
    );
    refresh_file_digest(
        &wrong_semantic.postings,
        PostingsFileKind::TargetAddressPostings,
    );
    assert!(open_canary(&wrong_semantic).is_err());

    let source_registry = fixture();
    let path = source_registry.dump.join(PUBKEY_REGISTRY_FILE);
    overwrite(&path, 0, &[4]);
    assert!(open_canary(&source_registry).is_err());

    let source_transaction = fixture();
    let path = source_transaction.dump.join(TRANSACTIONS_FILE);
    overwrite(&path, 0, &[99]);
    assert!(open_canary(&source_transaction).is_ok());
    assert!(
        verify_postings_artifact(&source_transaction.dump, &source_transaction.postings, true,)
            .is_err()
    );

    let source_signature = fixture();
    let path = source_signature.dump.join(SIGNATURES_FILE);
    overwrite(&path, 0, &[99]);
    assert!(open_canary(&source_signature).is_ok());
    assert!(
        verify_postings_artifact(&source_signature.dump, &source_signature.postings, true).is_err()
    );
}

#[test]
fn scoped_program_files_must_be_exact_filters_of_all_program_postings() {
    let fixture = fixture();
    let forged = ProgramPostingRecord {
        transaction_ordinal: 2,
        instruction_scope_mask: PROGRAM_INSTRUCTION_SCOPE_DIRECT,
    };
    let path = fixture
        .postings
        .join(PostingsFileKind::ProgramDirectPostings.file_name());
    overwrite(
        &path,
        POSTINGS_HEADER_BYTES as u64,
        &forged.encode().unwrap(),
    );
    refresh_file_digest(&fixture.postings, PostingsFileKind::ProgramDirectPostings);
    let mut manifest = read_manifest(&fixture.postings);
    let direct_directory = [PostingsDirectoryRecord {
        registry_id: 3,
        flags: 0,
        first_posting_row: 0,
        posting_count: 1,
    }];
    manifest.program_direct_semantic_sha256 = digest_hex_array(
        program_postings_semantic_sha256(
            ProgramInstructionScope::Direct,
            &direct_directory,
            &[forged],
            SOURCE_TRANSACTIONS,
        )
        .unwrap(),
    );
    write_manifest(&fixture.postings, &manifest);
    assert!(open_canary(&fixture).is_err());
}

#[cfg(unix)]
#[test]
fn open_store_uses_pinned_registry_and_body_handles_after_path_replacement() {
    let fixture = fixture();
    let store = open_canary(&fixture).unwrap();

    let registry_path = fixture.dump.join(PUBKEY_REGISTRY_FILE);
    let registry_bytes = fs::metadata(&registry_path).unwrap().len();
    fs::rename(&registry_path, fixture.dump.join("detached-registry.bin")).unwrap();
    fs::write(
        &registry_path,
        vec![0; usize::try_from(registry_bytes).unwrap()],
    )
    .unwrap();

    let body_path = fixture
        .postings
        .join(PostingsFileKind::TargetAddressPostings.file_name());
    let body_bytes = fs::metadata(&body_path).unwrap().len();
    fs::rename(
        &body_path,
        fixture.postings.join("detached-target-postings.bin"),
    )
    .unwrap();
    fs::write(&body_path, vec![0; usize::try_from(body_bytes).unwrap()]).unwrap();

    assert_eq!(
        store
            .lookup(PostingLookupKind::TokenAccount, fixture.token_key, 0, 200)
            .unwrap()
            .unwrap()
            .transaction_ordinals,
        [0, 2, 4]
    );
}
