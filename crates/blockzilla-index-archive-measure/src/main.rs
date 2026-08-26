use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    fs::{self, File},
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_index_archive_format::header::FILE_HEADER_LEN;
use of_car_reader::{
    CarBlockReader,
    confirmed_block::TransactionStatusMeta,
    metadata_decoder::{ZstdReusableDecoder, decode_transaction_status_meta_from_frame},
    reconstruct::{Cid36, LosslessCarBlock},
    versioned_transaction::{
        CompiledInstruction, MessageHeader, VersionedMessage, VersionedTransaction,
    },
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const PAGE_TARGET_BYTES: usize = 64 * 1024;
const PAGE_DIRECTORY_ENTRY_BYTES: usize = 32;
const PAGE_ENVELOPE_BYTES: usize = 16;
const ZSTD_LEVEL: i32 = 3;

const ROLE_SIGNER: u8 = 1 << 0;
const ROLE_WRITABLE: u8 = 1 << 1;
const ROLE_TOP_LEVEL_INSTRUCTION: u8 = 1 << 2;
const ROLE_RECORDED_CPI_INSTRUCTION: u8 = 1 << 3;

type PostingEntry = (u32, u8);
type AccountPostings = BTreeMap<u32, Vec<PostingEntry>>;

#[derive(Debug)]
struct TransactionAccounts {
    ids: Vec<u32>,
    roles: Vec<u8>,
}

#[derive(Debug, Default)]
struct FixtureData {
    dictionary: Vec<[u8; 32]>,
    dictionary_ids: HashMap<[u8; 32], u32>,
    transactions: Vec<TransactionAccounts>,
    message_ids: BTreeSet<u32>,
    lookup_table_keys: BTreeSet<[u8; 32]>,
    lookup_table_references: u64,
    blocks: u64,
    legacy_transactions: u64,
    v0_transactions: u64,
    v1_transactions: u64,
    loaded_writable: u64,
    loaded_readonly: u64,
    loaded_address_unavailable_transactions: u64,
    cpi_complete_transactions: u64,
    cpi_incomplete_transactions: u64,
    top_level_instructions: u64,
    recorded_cpi_instructions: u64,
    transaction_frame_bytes: u64,
    metadata_frame_bytes: u64,
}

impl FixtureData {
    fn intern(&mut self, key: [u8; 32]) -> Result<u32> {
        if let Some(id) = self.dictionary_ids.get(&key) {
            return Ok(*id);
        }
        let id = u32::try_from(self.dictionary.len()).context("pubkey dictionary exceeds u32")?;
        self.dictionary.push(key);
        self.dictionary_ids.insert(key, id);
        Ok(id)
    }
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactMeasurement {
    logical_records: u64,
    encoded_uncompressed_bytes: u64,
    zstd_payload_bytes: u64,
    selected_payload_bytes: u64,
    selected_raw_pages: u64,
    selected_zstd_pages: u64,
    file_header_bytes: u64,
    directory_bytes: u64,
    page_header_checksum_bytes: u64,
    padding_bytes: u64,
    page_count: u64,
    uncompressed_candidate_total_bytes: u64,
    selected_candidate_total_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SourceReport {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct PopulationReport {
    blocks: u64,
    transactions: u64,
    legacy_transactions: u64,
    v0_transactions: u64,
    v1_transactions: u64,
    loaded_writable_addresses: u64,
    loaded_readonly_addresses: u64,
    ordered_forward_account_references: u64,
    unique_message_pubkeys: u64,
    dictionary_pubkeys_message_plus_lookup: u64,
    lookup_table_descriptor_references: u64,
    unique_lookup_table_pubkeys: u64,
    reverse_postings_after_per_transaction_dedup: u64,
    duplicate_forward_positions_removed_from_postings: u64,
    top_level_instructions: u64,
    recorded_cpi_instructions: u64,
    /// Signed transaction frames as stored in the source CAR.
    transaction_frame_bytes: u64,
    /// Transaction status metadata frames as stored in the source CAR.
    metadata_frame_bytes: u64,
    /// Share of stored transaction-plus-metadata bytes that is metadata.
    metadata_share_of_stored_transaction_bytes: f64,
}

#[derive(Debug, Serialize)]
struct CoverageReport {
    loaded_addresses_exact: bool,
    loaded_address_unavailable_transactions: u64,
    cpi_roles_exact: bool,
    cpi_complete_transactions: u64,
    cpi_incomplete_transactions: u64,
}

#[derive(Debug, Serialize)]
struct PostingRoleCounts {
    signer: u64,
    writable: u64,
    top_level_instruction: u64,
    recorded_cpi_instruction: u64,
}

#[derive(Debug, Serialize)]
struct ComparisonReport {
    fixture_scope_fixed_reverse_over_forward_percent: f64,
    fixture_scope_compact_reverse_over_forward_percent: f64,
    fixture_scope_fixed_reverse_over_dictionary_plus_forward_percent: f64,
    fixture_scope_compact_reverse_over_dictionary_plus_forward_percent: f64,
    fixture_scope_fixed_reverse_plus_lookup_over_dictionary_plus_forward_percent: f64,
    fixture_scope_compact_reverse_plus_lookup_over_dictionary_plus_forward_percent: f64,
    fixed_forward_plus_reverse_bytes: u64,
    compact_forward_plus_reverse_bytes: u64,
    fixed_dictionary_forward_reverse_lookup_bytes: u64,
    compact_dictionary_forward_reverse_lookup_bytes: u64,
    compact_reverse_bytes_per_transaction: f64,
    compact_reverse_bytes_per_posting: f64,
}

#[derive(Debug, Serialize)]
struct ValidationReport {
    source_decode_complete: bool,
    forward_fixed_round_trip: bool,
    forward_compact_round_trip: bool,
    reverse_fixed_round_trip: bool,
    reverse_compact_combined_round_trip: bool,
    reverse_compact_split_nibbles_round_trip: bool,
    compressed_pages_round_trip: bool,
    reverse_rebuilt_from_decoded_forward_ids_and_normalized_roles: bool,
    pubkey_fingerprint_collision_buckets: u64,
}

#[derive(Debug, Serialize)]
struct MeasurementReport {
    report_schema: &'static str,
    source: SourceReport,
    population: PopulationReport,
    coverage: CoverageReport,
    posting_role_counts: PostingRoleCounts,
    candidate: CandidateReport,
    artifacts: BTreeMap<String, ArtifactMeasurement>,
    comparisons: ComparisonReport,
    validation: ValidationReport,
    limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct CandidateReport {
    page_target_bytes: u64,
    page_directory_entry_bytes: u64,
    page_envelope_and_checksum_bytes: u64,
    common_file_header_bytes: u64,
    zstd_level: i32,
    dictionary_order: &'static str,
    forward_fixed: &'static str,
    forward_compact: &'static str,
    reverse_fixed: &'static str,
    reverse_compact_combined: &'static str,
    reverse_compact_split_nibbles: &'static str,
    pubkey_lookup: &'static str,
    instruction_role_rule: &'static str,
    key_page_restart_rule: &'static str,
}

#[derive(Debug, Clone, Copy)]
enum PageMode {
    SingleBlockRowGroup,
    KeyAligned,
}

fn main() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let car_path = args
        .next()
        .map(PathBuf::from)
        .context("usage: blockzilla-index-archive-measure <fixture.car> [report.json]")?;
    let report_path = args.next().map(PathBuf::from);
    ensure!(
        args.next().is_none(),
        "usage: blockzilla-index-archive-measure <fixture.car> [report.json]"
    );

    let source_bytes = fs::metadata(&car_path)
        .with_context(|| format!("read metadata for {}", car_path.display()))?
        .len();
    let source_sha256 = hash_file(&car_path)?;
    let fixture = read_fixture(&car_path)?;
    let (postings, role_counts) = build_postings(&fixture.transactions)?;

    let forward_count: u64 = fixture
        .transactions
        .iter()
        .map(|transaction| transaction.ids.len() as u64)
        .sum();
    let posting_count: u64 = postings.values().map(|entries| entries.len() as u64).sum();
    ensure!(
        posting_count <= forward_count,
        "postings exceed forward positions"
    );

    let dictionary_records: Vec<Vec<u8>> = fixture
        .dictionary
        .iter()
        .map(|pubkey| pubkey.to_vec())
        .collect();
    let forward_fixed_records = encode_forward_fixed(&fixture.transactions)?;
    let forward_compact_records = encode_forward_compact(&fixture.transactions);
    validate_forward_fixed(&forward_fixed_records, &fixture.transactions)?;
    validate_forward_compact(&forward_compact_records, &fixture.transactions)?;
    let decoded_forward = decode_forward_compact(&forward_compact_records, &fixture.transactions)?;
    let rebuilt_transactions: Vec<TransactionAccounts> = decoded_forward
        .into_iter()
        .zip(&fixture.transactions)
        .map(|(ids, source)| TransactionAccounts {
            ids,
            roles: source.roles.clone(),
        })
        .collect();
    let (rebuilt_postings, _) = build_postings(&rebuilt_transactions)?;
    ensure!(
        rebuilt_postings == postings,
        "reverse postings cannot be rebuilt from decoded account IDs and canonical role rows"
    );

    let reverse_fixed_records = encode_reverse_fixed(&postings)?;
    let reverse_compact_combined_records = encode_reverse_compact_combined(&postings);
    let reverse_compact_split_records = encode_reverse_compact_split_nibbles(&postings);
    validate_reverse_fixed(&reverse_fixed_records, &postings)?;
    validate_reverse_compact_combined(&reverse_compact_combined_records, &postings)?;
    validate_reverse_compact_split_nibbles(&reverse_compact_split_records, &postings)?;

    let (lookup_records, collision_buckets) = encode_pubkey_lookup(&fixture.dictionary);

    let mut artifacts = BTreeMap::new();
    artifacts.insert(
        "canonical_dictionary".to_owned(),
        measure_records(&dictionary_records, PageMode::KeyAligned)?,
    );
    artifacts.insert(
        "canonical_forward_fixed".to_owned(),
        measure_records(&forward_fixed_records, PageMode::SingleBlockRowGroup)?,
    );
    artifacts.insert(
        "canonical_forward_compact".to_owned(),
        measure_records(&forward_compact_records, PageMode::SingleBlockRowGroup)?,
    );
    artifacts.insert(
        "reverse_postings_fixed".to_owned(),
        measure_records(&reverse_fixed_records, PageMode::KeyAligned)?,
    );
    artifacts.insert(
        "reverse_postings_compact_combined".to_owned(),
        measure_records(&reverse_compact_combined_records, PageMode::KeyAligned)?,
    );
    artifacts.insert(
        "reverse_postings_compact_split_nibbles".to_owned(),
        measure_records(&reverse_compact_split_records, PageMode::KeyAligned)?,
    );
    artifacts.insert(
        "pubkey_lookup".to_owned(),
        measure_records(&lookup_records, PageMode::KeyAligned)?,
    );

    let dictionary_bytes = physical(&artifacts, "canonical_dictionary");
    let fixed_forward_bytes = physical(&artifacts, "canonical_forward_fixed");
    let compact_forward_bytes = physical(&artifacts, "canonical_forward_compact");
    let fixed_reverse_bytes = physical(&artifacts, "reverse_postings_fixed");
    let compact_reverse_bytes = physical(&artifacts, "reverse_postings_compact_split_nibbles");
    let lookup_bytes = physical(&artifacts, "pubkey_lookup");

    let comparisons = ComparisonReport {
        fixture_scope_fixed_reverse_over_forward_percent: percent(
            fixed_reverse_bytes,
            fixed_forward_bytes,
        ),
        fixture_scope_compact_reverse_over_forward_percent: percent(
            compact_reverse_bytes,
            compact_forward_bytes,
        ),
        fixture_scope_fixed_reverse_over_dictionary_plus_forward_percent: percent(
            fixed_reverse_bytes,
            dictionary_bytes + fixed_forward_bytes,
        ),
        fixture_scope_compact_reverse_over_dictionary_plus_forward_percent: percent(
            compact_reverse_bytes,
            dictionary_bytes + compact_forward_bytes,
        ),
        fixture_scope_fixed_reverse_plus_lookup_over_dictionary_plus_forward_percent: percent(
            fixed_reverse_bytes + lookup_bytes,
            dictionary_bytes + fixed_forward_bytes,
        ),
        fixture_scope_compact_reverse_plus_lookup_over_dictionary_plus_forward_percent: percent(
            compact_reverse_bytes + lookup_bytes,
            dictionary_bytes + compact_forward_bytes,
        ),
        fixed_forward_plus_reverse_bytes: fixed_forward_bytes + fixed_reverse_bytes,
        compact_forward_plus_reverse_bytes: compact_forward_bytes + compact_reverse_bytes,
        fixed_dictionary_forward_reverse_lookup_bytes: dictionary_bytes
            + fixed_forward_bytes
            + fixed_reverse_bytes
            + lookup_bytes,
        compact_dictionary_forward_reverse_lookup_bytes: dictionary_bytes
            + compact_forward_bytes
            + compact_reverse_bytes
            + lookup_bytes,
        compact_reverse_bytes_per_transaction: ratio(
            compact_reverse_bytes,
            fixture.transactions.len() as u64,
        ),
        compact_reverse_bytes_per_posting: ratio(compact_reverse_bytes, posting_count),
    };

    let report = MeasurementReport {
        report_schema: "blockzilla-index-archive-account-measurement-v1",
        source: SourceReport {
            path: car_path.display().to_string(),
            bytes: source_bytes,
            sha256: source_sha256,
        },
        population: PopulationReport {
            blocks: fixture.blocks,
            transactions: fixture.transactions.len() as u64,
            legacy_transactions: fixture.legacy_transactions,
            v0_transactions: fixture.v0_transactions,
            v1_transactions: fixture.v1_transactions,
            loaded_writable_addresses: fixture.loaded_writable,
            loaded_readonly_addresses: fixture.loaded_readonly,
            ordered_forward_account_references: forward_count,
            unique_message_pubkeys: fixture.message_ids.len() as u64,
            dictionary_pubkeys_message_plus_lookup: fixture.dictionary.len() as u64,
            lookup_table_descriptor_references: fixture.lookup_table_references,
            unique_lookup_table_pubkeys: fixture.lookup_table_keys.len() as u64,
            reverse_postings_after_per_transaction_dedup: posting_count,
            duplicate_forward_positions_removed_from_postings: forward_count - posting_count,
            transaction_frame_bytes: fixture.transaction_frame_bytes,
            metadata_frame_bytes: fixture.metadata_frame_bytes,
            metadata_share_of_stored_transaction_bytes: metadata_share(
                fixture.transaction_frame_bytes,
                fixture.metadata_frame_bytes,
            ),
            top_level_instructions: fixture.top_level_instructions,
            recorded_cpi_instructions: fixture.recorded_cpi_instructions,
        },
        coverage: CoverageReport {
            loaded_addresses_exact: fixture.loaded_address_unavailable_transactions == 0,
            loaded_address_unavailable_transactions: fixture
                .loaded_address_unavailable_transactions,
            cpi_roles_exact: fixture.cpi_incomplete_transactions == 0,
            cpi_complete_transactions: fixture.cpi_complete_transactions,
            cpi_incomplete_transactions: fixture.cpi_incomplete_transactions,
        },
        posting_role_counts: role_counts,
        candidate: CandidateReport {
            page_target_bytes: PAGE_TARGET_BYTES as u64,
            page_directory_entry_bytes: PAGE_DIRECTORY_ENTRY_BYTES as u64,
            page_envelope_and_checksum_bytes: PAGE_ENVELOPE_BYTES as u64,
            common_file_header_bytes: FILE_HEADER_LEN as u64,
            zstd_level: ZSTD_LEVEL,
            dictionary_order: "first seen while normalizing message accounts, then lookup descriptors",
            forward_fixed: "ordered u32 registry IDs; transaction boundaries come only from canonical core rows",
            forward_compact: "ordered ULEB128 registry IDs; transaction boundaries come only from canonical core rows",
            reverse_fixed: "u32 registry ID, u32 count, then repeated u32 transaction ordinal plus u8 role mask",
            reverse_compact_combined: "ULEB128 registry-ID gap and count, then ULEB128((transaction gap << 4) | role)",
            reverse_compact_split_nibbles: "ULEB128 registry-ID gap and count, ULEB128 transaction gaps, then packed 4-bit roles",
            pubkey_lookup: "sorted SHA-256 64-bit fingerprint plus u32 registry ID; canonical pubkey verification required",
            instruction_role_rule: "top-level and recorded-CPI roles mark both the program account position and every instruction account position",
            key_page_restart_rule: "each 32-byte key-page directory entry supplies the page key bounds and the previous registry ID needed to restart key-gap decoding",
        },
        artifacts,
        comparisons,
        validation: ValidationReport {
            source_decode_complete: true,
            forward_fixed_round_trip: true,
            forward_compact_round_trip: true,
            reverse_fixed_round_trip: true,
            reverse_compact_combined_round_trip: true,
            reverse_compact_split_nibbles_round_trip: true,
            compressed_pages_round_trip: true,
            reverse_rebuilt_from_decoded_forward_ids_and_normalized_roles: true,
            pubkey_fingerprint_collision_buckets: collision_buckets,
        },
        limitations: vec![
            "This is an account-storage measurement, not a complete Index Archive encoder.",
            "The dictionary includes message and lookup-table descriptor pubkeys, but not effects-only pubkeys.",
            "Reverse postings measure only the message namespace; lookup-table and metadata namespace postings and namespace tags are not encoded.",
            "The fixture contains one unusually large block, so it cannot select epoch-wide page or codec policy.",
            "One transaction has incomplete CPI recording; message-account postings are exact, but CPI-role queries are not generation-wide exact.",
            "The 32-byte page directory entry and 16-byte page envelope are measurement candidates, not frozen schemas.",
            "Canonical core rows and the shared catalog row-group directory are not included in the account-only denominator.",
            "The newer bounded-subpage restart tables and locators are not included in these candidate totals.",
            "The sorted fingerprint lookup is a payload candidate; collision-bucket bounds and its top-level lookup directory are not yet a frozen exact lookup schema.",
            "A posting list larger than 64 KiB is not split by transaction range in this one-block probe; the full-epoch encoder must add and measure continuation pages.",
            "metadata_frame_bytes counts CAR metadata frames as stored, which are zstd-compressed, while transaction_frame_bytes are raw. The metadata share is therefore understated relative to a uniformly compressed archive.",
            "Frame byte counts cover transactions and metadata only; PoH, signatures, registry, rewards, and shredding are outside this denominator.",
        ],
    };

    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = report_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create report directory {}", parent.display()))?;
        }
        fs::write(&path, format!("{json}\n"))
            .with_context(|| format!("write {}", path.display()))?;
    }
    println!("{json}");
    Ok(())
}

fn read_fixture(path: &Path) -> Result<FixtureData> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = CarBlockReader::with_capacity(file, 128 << 20);
    reader.skip_header().context("read CAR header")?;

    let mut block = LosslessCarBlock::default();
    let mut fixture = FixtureData::default();
    let mut transaction_bytes = Vec::new();
    let mut metadata_bytes = Vec::new();
    let mut visited = HashSet::<Cid36>::new();
    let mut metadata = TransactionStatusMeta::default();
    let mut metadata_zstd = ZstdReusableDecoder::new();

    while reader
        .read_until_block_lossless(&mut block)
        .context("read lossless CAR block")?
    {
        fixture.blocks += 1;
        for raw in &block.transactions {
            raw.transaction_bytes_into(&block.dataframes, &mut transaction_bytes, &mut visited)
                .context("reassemble transaction frame")?;
            raw.metadata_bytes_into(&block.dataframes, &mut metadata_bytes, &mut visited)
                .context("reassemble metadata frame")?;

            fixture.transaction_frame_bytes += transaction_bytes.len() as u64;
            fixture.metadata_frame_bytes += metadata_bytes.len() as u64;

            let transaction = wincode::deserialize::<VersionedTransaction<'_>>(&transaction_bytes)
                .context("decode versioned transaction")?;
            decode_transaction_status_meta_from_frame(
                raw.slot,
                &metadata_bytes,
                &mut metadata,
                &mut metadata_zstd,
            )
            .context("decode transaction metadata")?;

            normalize_transaction(
                &mut fixture,
                &transaction,
                &metadata,
                metadata_bytes.is_empty(),
            )?;
        }
    }

    ensure!(fixture.blocks > 0, "fixture contains no block");
    Ok(fixture)
}

fn normalize_transaction(
    fixture: &mut FixtureData,
    transaction: &VersionedTransaction<'_>,
    metadata: &TransactionStatusMeta,
    metadata_frame_empty: bool,
) -> Result<()> {
    match &transaction.message {
        VersionedMessage::Legacy(message) => {
            fixture.legacy_transactions += 1;
            ensure!(
                metadata.loaded_writable_addresses.is_empty()
                    && metadata.loaded_readonly_addresses.is_empty(),
                "legacy transaction has loaded addresses"
            );
            normalize_message(
                fixture,
                &message.header,
                &message.account_keys,
                &message.instructions,
                &[],
                &[],
                &[],
                metadata,
                metadata_frame_empty,
            )?;
        }
        // v1 has no lookup tables, so there are no loaded addresses to reconcile.
        VersionedMessage::V1(_) => {
            fixture.v1_transactions += 1;
        }
        VersionedMessage::V0(message) => {
            fixture.v0_transactions += 1;
            let expected_writable: usize = message
                .address_table_lookups
                .iter()
                .map(|lookup| lookup.writable_indexes.len())
                .sum();
            let expected_readonly: usize = message
                .address_table_lookups
                .iter()
                .map(|lookup| lookup.readonly_indexes.len())
                .sum();
            if metadata_frame_empty && expected_writable + expected_readonly > 0 {
                fixture.loaded_address_unavailable_transactions += 1;
                bail!("V0 transaction requires loaded addresses but metadata is unavailable");
            }
            ensure!(
                metadata.loaded_writable_addresses.len() == expected_writable,
                "V0 writable lookup count does not match resolved metadata"
            );
            ensure!(
                metadata.loaded_readonly_addresses.len() == expected_readonly,
                "V0 readonly lookup count does not match resolved metadata"
            );
            let descriptor_keys: Vec<[u8; 32]> = message
                .address_table_lookups
                .iter()
                .map(|lookup| *lookup.account_key)
                .collect();
            normalize_message(
                fixture,
                &message.header,
                &message.account_keys,
                &message.instructions,
                &metadata.loaded_writable_addresses,
                &metadata.loaded_readonly_addresses,
                &descriptor_keys,
                metadata,
                metadata_frame_empty,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_message(
    fixture: &mut FixtureData,
    header: &MessageHeader,
    static_keys: &[&[u8; 32]],
    top_level_instructions: &[CompiledInstruction],
    loaded_writable: &[Vec<u8>],
    loaded_readonly: &[Vec<u8>],
    descriptor_keys: &[[u8; 32]],
    metadata: &TransactionStatusMeta,
    metadata_frame_empty: bool,
) -> Result<()> {
    let static_len = static_keys.len();
    let required = usize::from(header.num_required_signatures);
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    ensure!(
        required <= static_len,
        "signature count exceeds static accounts"
    );
    ensure!(
        readonly_signed <= required,
        "readonly signer count exceeds signers"
    );
    ensure!(
        readonly_unsigned <= static_len - required,
        "readonly unsigned count exceeds unsigned accounts"
    );

    let mut keys = Vec::with_capacity(static_len + loaded_writable.len() + loaded_readonly.len());
    let mut roles = Vec::with_capacity(keys.capacity());
    let writable_signed_end = required - readonly_signed;
    let writable_unsigned_end = static_len - readonly_unsigned;
    for (index, key) in static_keys.iter().enumerate() {
        keys.push(**key);
        let mut role = 0;
        if index < required {
            role |= ROLE_SIGNER;
        }
        if index < writable_signed_end || (index >= required && index < writable_unsigned_end) {
            role |= ROLE_WRITABLE;
        }
        roles.push(role);
    }
    for key in loaded_writable {
        keys.push(as_pubkey(key)?);
        roles.push(ROLE_WRITABLE);
    }
    for key in loaded_readonly {
        keys.push(as_pubkey(key)?);
        roles.push(0);
    }

    for instruction in top_level_instructions {
        mark_instruction_roles(
            &mut roles,
            usize::from(instruction.program_id_index),
            &instruction.accounts,
            ROLE_TOP_LEVEL_INSTRUCTION,
        )?;
        fixture.top_level_instructions += 1;
    }

    if metadata_frame_empty || metadata.inner_instructions_none {
        ensure!(
            metadata.inner_instructions.is_empty(),
            "metadata marks CPI unavailable but also contains CPI rows"
        );
        fixture.cpi_incomplete_transactions += 1;
    } else {
        fixture.cpi_complete_transactions += 1;
        for group in &metadata.inner_instructions {
            for instruction in &group.instructions {
                let program_index = usize::try_from(instruction.program_id_index)
                    .context("CPI program index exceeds usize")?;
                mark_instruction_roles(
                    &mut roles,
                    program_index,
                    &instruction.accounts,
                    ROLE_RECORDED_CPI_INSTRUCTION,
                )?;
                fixture.recorded_cpi_instructions += 1;
            }
        }
    }

    let mut ids = Vec::with_capacity(keys.len());
    for key in keys {
        let id = fixture.intern(key)?;
        fixture.message_ids.insert(id);
        ids.push(id);
    }
    fixture.loaded_writable += loaded_writable.len() as u64;
    fixture.loaded_readonly += loaded_readonly.len() as u64;
    fixture
        .transactions
        .push(TransactionAccounts { ids, roles });

    for key in descriptor_keys {
        fixture.lookup_table_references += 1;
        fixture.lookup_table_keys.insert(*key);
        fixture.intern(*key)?;
    }
    Ok(())
}

fn mark_instruction_roles(
    roles: &mut [u8],
    program_index: usize,
    account_indexes: &[u8],
    flag: u8,
) -> Result<()> {
    let program_role = roles
        .get_mut(program_index)
        .with_context(|| format!("program account index {program_index} is out of range"))?;
    *program_role |= flag;
    for account_index in account_indexes {
        let index = usize::from(*account_index);
        let role = roles
            .get_mut(index)
            .with_context(|| format!("instruction account index {index} is out of range"))?;
        *role |= flag;
    }
    Ok(())
}

fn as_pubkey(bytes: &[u8]) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("resolved account has {} bytes, expected 32", bytes.len()))
}

fn build_postings(
    transactions: &[TransactionAccounts],
) -> Result<(AccountPostings, PostingRoleCounts)> {
    let mut postings = AccountPostings::new();
    let mut role_counts = PostingRoleCounts {
        signer: 0,
        writable: 0,
        top_level_instruction: 0,
        recorded_cpi_instruction: 0,
    };
    for (transaction_ordinal, transaction) in transactions.iter().enumerate() {
        let transaction_ordinal =
            u32::try_from(transaction_ordinal).context("transaction ordinal exceeds u32")?;
        let mut deduplicated = BTreeMap::<u32, u8>::new();
        for (&id, &role) in transaction.ids.iter().zip(&transaction.roles) {
            *deduplicated.entry(id).or_default() |= role;
        }
        for (id, role) in deduplicated {
            role_counts.signer += u64::from(role & ROLE_SIGNER != 0);
            role_counts.writable += u64::from(role & ROLE_WRITABLE != 0);
            role_counts.top_level_instruction += u64::from(role & ROLE_TOP_LEVEL_INSTRUCTION != 0);
            role_counts.recorded_cpi_instruction +=
                u64::from(role & ROLE_RECORDED_CPI_INSTRUCTION != 0);
            postings
                .entry(id)
                .or_default()
                .push((transaction_ordinal, role));
        }
    }
    Ok((postings, role_counts))
}

fn encode_forward_fixed(transactions: &[TransactionAccounts]) -> Result<Vec<Vec<u8>>> {
    transactions
        .iter()
        .map(|transaction| {
            let mut record = Vec::with_capacity(transaction.ids.len() * 4);
            for id in &transaction.ids {
                record.extend_from_slice(&id.to_le_bytes());
            }
            Ok(record)
        })
        .collect()
}

fn encode_forward_compact(transactions: &[TransactionAccounts]) -> Vec<Vec<u8>> {
    transactions
        .iter()
        .map(|transaction| {
            let mut record = Vec::new();
            for id in &transaction.ids {
                put_uleb128(u64::from(*id), &mut record);
            }
            record
        })
        .collect()
}

fn encode_reverse_fixed(postings: &AccountPostings) -> Result<Vec<Vec<u8>>> {
    postings
        .iter()
        .map(|(id, entries)| {
            let mut record = Vec::with_capacity(8 + entries.len() * 5);
            record.extend_from_slice(&id.to_le_bytes());
            let count = u32::try_from(entries.len()).context("posting list exceeds u32")?;
            record.extend_from_slice(&count.to_le_bytes());
            for (transaction, role) in entries {
                record.extend_from_slice(&transaction.to_le_bytes());
                record.push(*role);
            }
            Ok(record)
        })
        .collect()
}

fn encode_reverse_compact_combined(postings: &AccountPostings) -> Vec<Vec<u8>> {
    let mut previous_key = 0u32;
    postings
        .iter()
        .map(|(id, entries)| {
            let mut record = Vec::new();
            put_uleb128(u64::from(*id - previous_key), &mut record);
            put_uleb128(entries.len() as u64, &mut record);
            let mut previous = 0u32;
            for (index, (transaction, role)) in entries.iter().enumerate() {
                let gap = if index == 0 {
                    *transaction
                } else {
                    transaction - previous
                };
                put_uleb128((u64::from(gap) << 4) | u64::from(*role), &mut record);
                previous = *transaction;
            }
            previous_key = *id;
            record
        })
        .collect()
}

fn encode_reverse_compact_split_nibbles(postings: &AccountPostings) -> Vec<Vec<u8>> {
    let mut previous_key = 0u32;
    postings
        .iter()
        .map(|(id, entries)| {
            let mut record = Vec::new();
            put_uleb128(u64::from(*id - previous_key), &mut record);
            put_uleb128(entries.len() as u64, &mut record);
            let mut previous = 0u32;
            for (index, (transaction, _)) in entries.iter().enumerate() {
                let gap = if index == 0 {
                    *transaction
                } else {
                    transaction - previous
                };
                put_uleb128(u64::from(gap), &mut record);
                previous = *transaction;
            }
            for pair in entries.chunks(2) {
                let low = pair[0].1 & 0x0f;
                let high = pair.get(1).map_or(0, |entry| entry.1 & 0x0f);
                record.push(low | (high << 4));
            }
            previous_key = *id;
            record
        })
        .collect()
}

fn encode_pubkey_lookup(dictionary: &[[u8; 32]]) -> (Vec<Vec<u8>>, u64) {
    let mut values: Vec<(u64, u32)> = dictionary
        .iter()
        .enumerate()
        .map(|(id, pubkey)| {
            let digest = Sha256::digest(pubkey);
            let fingerprint = u64::from_le_bytes(digest[..8].try_into().unwrap());
            (fingerprint, id as u32)
        })
        .collect();
    values.sort_unstable();
    let mut collision_buckets = 0u64;
    let mut start = 0usize;
    while start < values.len() {
        let mut end = start + 1;
        while end < values.len() && values[end].0 == values[start].0 {
            end += 1;
        }
        collision_buckets += u64::from(end - start > 1);
        start = end;
    }
    let records = values
        .into_iter()
        .map(|(fingerprint, id)| {
            let mut record = Vec::with_capacity(12);
            record.extend_from_slice(&fingerprint.to_le_bytes());
            record.extend_from_slice(&id.to_le_bytes());
            record
        })
        .collect();
    (records, collision_buckets)
}

fn measure_records(records: &[Vec<u8>], mode: PageMode) -> Result<ArtifactMeasurement> {
    let pages = match mode {
        PageMode::SingleBlockRowGroup => vec![records.concat()],
        PageMode::KeyAligned => pack_pages(records),
    };
    let mut uncompressed = 0u64;
    let mut zstd_bytes = 0u64;
    let mut selected_bytes = 0u64;
    let mut selected_raw_pages = 0u64;
    let mut selected_zstd_pages = 0u64;
    for page in &pages {
        uncompressed += page.len() as u64;
        let encoded = zstd::bulk::compress(page, ZSTD_LEVEL).context("compress candidate page")?;
        let decoded =
            zstd::bulk::decompress(&encoded, page.len()).context("decompress candidate page")?;
        ensure!(
            decoded == *page,
            "compressed candidate page does not round trip"
        );
        zstd_bytes += encoded.len() as u64;
        if encoded.len() < page.len() {
            selected_bytes += encoded.len() as u64;
            selected_zstd_pages += 1;
        } else {
            selected_bytes += page.len() as u64;
            selected_raw_pages += 1;
        }
    }
    let page_count = pages.len() as u64;
    let file_header_bytes = FILE_HEADER_LEN as u64;
    let directory_bytes = match mode {
        PageMode::SingleBlockRowGroup => 0,
        PageMode::KeyAligned => page_count * PAGE_DIRECTORY_ENTRY_BYTES as u64,
    };
    let page_header_checksum_bytes = page_count * PAGE_ENVELOPE_BYTES as u64;
    let fixed_overhead = file_header_bytes + directory_bytes + page_header_checksum_bytes;
    Ok(ArtifactMeasurement {
        logical_records: records.len() as u64,
        encoded_uncompressed_bytes: uncompressed,
        zstd_payload_bytes: zstd_bytes,
        selected_payload_bytes: selected_bytes,
        selected_raw_pages,
        selected_zstd_pages,
        file_header_bytes,
        directory_bytes,
        page_header_checksum_bytes,
        padding_bytes: 0,
        page_count,
        uncompressed_candidate_total_bytes: fixed_overhead + uncompressed,
        selected_candidate_total_bytes: fixed_overhead + selected_bytes,
    })
}

fn pack_pages(records: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut pages = Vec::new();
    let mut current = Vec::new();
    for record in records {
        if !current.is_empty() && current.len() + record.len() > PAGE_TARGET_BYTES {
            pages.push(std::mem::take(&mut current));
        }
        current.extend_from_slice(record);
        if current.len() >= PAGE_TARGET_BYTES {
            pages.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        pages.push(current);
    }
    pages
}

fn validate_forward_fixed(records: &[Vec<u8>], transactions: &[TransactionAccounts]) -> Result<()> {
    ensure!(
        records.len() == transactions.len(),
        "fixed forward row count"
    );
    for (record, expected) in records.iter().zip(transactions) {
        let mut cursor = 0usize;
        let mut ids = Vec::with_capacity(expected.ids.len());
        for _ in 0..expected.ids.len() {
            ids.push(read_u32(record, &mut cursor)?);
        }
        ensure!(cursor == record.len(), "fixed forward trailing bytes");
        ensure!(ids == expected.ids, "fixed forward IDs changed");
    }
    Ok(())
}

fn validate_forward_compact(
    records: &[Vec<u8>],
    transactions: &[TransactionAccounts],
) -> Result<()> {
    ensure!(
        records.len() == transactions.len(),
        "compact forward row count"
    );
    for (record, expected) in records.iter().zip(transactions) {
        let mut cursor = 0usize;
        let mut ids = Vec::with_capacity(expected.ids.len());
        for _ in 0..expected.ids.len() {
            ids.push(u32::try_from(read_uleb128(record, &mut cursor)?)?);
        }
        ensure!(cursor == record.len(), "compact forward trailing bytes");
        ensure!(ids == expected.ids, "compact forward IDs changed");
    }
    Ok(())
}

fn decode_forward_compact(
    records: &[Vec<u8>],
    transactions: &[TransactionAccounts],
) -> Result<Vec<Vec<u32>>> {
    ensure!(
        records.len() == transactions.len(),
        "compact forward row count"
    );
    records
        .iter()
        .zip(transactions)
        .map(|(record, transaction)| {
            let mut cursor = 0usize;
            let mut ids = Vec::with_capacity(transaction.ids.len());
            for _ in 0..transaction.ids.len() {
                ids.push(u32::try_from(read_uleb128(record, &mut cursor)?)?);
            }
            ensure!(cursor == record.len(), "compact forward trailing bytes");
            Ok(ids)
        })
        .collect()
}

fn validate_reverse_fixed(records: &[Vec<u8>], postings: &AccountPostings) -> Result<()> {
    let mut decoded = BTreeMap::new();
    for record in records {
        let mut cursor = 0usize;
        let id = read_u32(record, &mut cursor)?;
        let count = read_u32(record, &mut cursor)? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let transaction = read_u32(record, &mut cursor)?;
            let role = *record
                .get(cursor)
                .context("fixed reverse role is truncated")?;
            cursor += 1;
            entries.push((transaction, role));
        }
        ensure!(cursor == record.len(), "fixed reverse trailing bytes");
        ensure!(decoded.insert(id, entries).is_none(), "duplicate fixed key");
    }
    ensure!(&decoded == postings, "fixed reverse postings changed");
    Ok(())
}

fn validate_reverse_compact_combined(
    records: &[Vec<u8>],
    postings: &AccountPostings,
) -> Result<()> {
    let mut decoded = BTreeMap::new();
    let mut previous_key = 0u32;
    for record in records {
        let mut cursor = 0usize;
        let key_gap = u32::try_from(read_uleb128(record, &mut cursor)?)?;
        let id = previous_key
            .checked_add(key_gap)
            .context("registry key gap overflow")?;
        let count = read_uleb128(record, &mut cursor)? as usize;
        let mut entries = Vec::with_capacity(count);
        let mut previous = 0u32;
        for index in 0..count {
            let value = read_uleb128(record, &mut cursor)?;
            let role = (value & 0x0f) as u8;
            let gap = u32::try_from(value >> 4)?;
            let transaction = if index == 0 {
                gap
            } else {
                previous
                    .checked_add(gap)
                    .context("transaction gap overflow")?
            };
            entries.push((transaction, role));
            previous = transaction;
        }
        ensure!(cursor == record.len(), "combined reverse trailing bytes");
        ensure!(
            decoded.insert(id, entries).is_none(),
            "duplicate compact key"
        );
        previous_key = id;
    }
    ensure!(&decoded == postings, "combined reverse postings changed");
    Ok(())
}

fn validate_reverse_compact_split_nibbles(
    records: &[Vec<u8>],
    postings: &AccountPostings,
) -> Result<()> {
    let mut decoded = BTreeMap::new();
    let mut previous_key = 0u32;
    for record in records {
        let mut cursor = 0usize;
        let key_gap = u32::try_from(read_uleb128(record, &mut cursor)?)?;
        let id = previous_key
            .checked_add(key_gap)
            .context("registry key gap overflow")?;
        let count = read_uleb128(record, &mut cursor)? as usize;
        let mut transactions = Vec::with_capacity(count);
        let mut previous = 0u32;
        for index in 0..count {
            let gap = u32::try_from(read_uleb128(record, &mut cursor)?)?;
            let transaction = if index == 0 {
                gap
            } else {
                previous
                    .checked_add(gap)
                    .context("transaction gap overflow")?
            };
            transactions.push(transaction);
            previous = transaction;
        }
        let role_bytes = count.div_ceil(2);
        ensure!(
            record.len().saturating_sub(cursor) == role_bytes,
            "split reverse role length"
        );
        let mut entries = Vec::with_capacity(count);
        for (index, transaction) in transactions.into_iter().enumerate() {
            let packed = record[cursor + index / 2];
            let role = if index % 2 == 0 {
                packed & 0x0f
            } else {
                packed >> 4
            };
            entries.push((transaction, role));
        }
        cursor += role_bytes;
        ensure!(cursor == record.len(), "split reverse trailing bytes");
        ensure!(decoded.insert(id, entries).is_none(), "duplicate split key");
        previous_key = id;
    }
    ensure!(&decoded == postings, "split reverse postings changed");
    Ok(())
}

fn put_uleb128(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_uleb128(input: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = *input.get(*cursor).context("truncated ULEB128")?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            bail!("ULEB128 overflow");
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("ULEB128 overflow")
}

fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor.checked_add(4).context("u32 cursor overflow")?;
    let bytes = input.get(*cursor..end).context("truncated u32")?;
    *cursor = end;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn physical(artifacts: &BTreeMap<String, ArtifactMeasurement>, name: &str) -> u64 {
    artifacts[name].selected_candidate_total_bytes
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    ratio(numerator, denominator) * 100.0
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn hash_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = reader.read(&mut buffer).context("hash fixture")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("writing to a String is infallible");
    }
    Ok(hex)
}

/// Share of stored transaction-plus-metadata bytes that is metadata.
///
/// Both inputs are frame sizes as stored in the source CAR, so metadata is
/// counted compressed. This is the storage question -- what fraction of the
/// bytes an archive carries are runtime effects -- not a decoded-size ratio.
fn metadata_share(transaction_bytes: u64, metadata_bytes: u64) -> f64 {
    let total = transaction_bytes + metadata_bytes;
    if total == 0 {
        return 0.0;
    }
    metadata_bytes as f64 / total as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_transactions() -> Vec<TransactionAccounts> {
        vec![
            TransactionAccounts {
                ids: vec![0, 5, 9],
                roles: vec![ROLE_SIGNER | ROLE_WRITABLE, 0, ROLE_TOP_LEVEL_INSTRUCTION],
            },
            TransactionAccounts {
                ids: vec![5, 10, 5],
                roles: vec![ROLE_WRITABLE, ROLE_RECORDED_CPI_INSTRUCTION, ROLE_SIGNER],
            },
        ]
    }

    #[test]
    fn account_and_posting_candidates_round_trip() {
        let transactions = sample_transactions();
        let fixed_forward = encode_forward_fixed(&transactions).unwrap();
        let compact_forward = encode_forward_compact(&transactions);
        validate_forward_fixed(&fixed_forward, &transactions).unwrap();
        validate_forward_compact(&compact_forward, &transactions).unwrap();

        let decoded = decode_forward_compact(&compact_forward, &transactions).unwrap();
        assert_eq!(decoded[0], transactions[0].ids);
        assert_eq!(decoded[1], transactions[1].ids);

        let (postings, _) = build_postings(&transactions).unwrap();
        assert_eq!(postings[&5], vec![(0, 0), (1, ROLE_WRITABLE | ROLE_SIGNER)]);

        let fixed = encode_reverse_fixed(&postings).unwrap();
        let combined = encode_reverse_compact_combined(&postings);
        let split = encode_reverse_compact_split_nibbles(&postings);
        validate_reverse_fixed(&fixed, &postings).unwrap();
        validate_reverse_compact_combined(&combined, &postings).unwrap();
        validate_reverse_compact_split_nibbles(&split, &postings).unwrap();
    }

    #[test]
    fn varints_reject_truncation_and_round_trip_boundaries() {
        for value in [0, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            let mut bytes = Vec::new();
            put_uleb128(value, &mut bytes);
            let mut cursor = 0;
            assert_eq!(read_uleb128(&bytes, &mut cursor).unwrap(), value);
            assert_eq!(cursor, bytes.len());
        }
        assert!(read_uleb128(&[0x80], &mut 0).is_err());
    }

    #[test]
    fn block_pages_stay_whole_and_key_pages_respect_record_boundaries() {
        let records = vec![
            vec![1; PAGE_TARGET_BYTES / 2 + 1],
            vec![2; PAGE_TARGET_BYTES / 2 + 1],
        ];
        let block = measure_records(&records, PageMode::SingleBlockRowGroup).unwrap();
        assert_eq!(block.page_count, 1);
        assert_eq!(block.directory_bytes, 0);

        let keys = measure_records(&records, PageMode::KeyAligned).unwrap();
        assert_eq!(keys.page_count, 2);
        assert_eq!(keys.directory_bytes, 2 * PAGE_DIRECTORY_ENTRY_BYTES as u64);
    }

    #[test]
    fn raw_pages_are_selected_when_zstd_is_larger() {
        let records = vec![(0u8..=255).collect::<Vec<_>>()];
        let measured = measure_records(&records, PageMode::KeyAligned).unwrap();
        assert_eq!(measured.selected_raw_pages, 1);
        assert_eq!(measured.selected_zstd_pages, 0);
        assert_eq!(measured.selected_payload_bytes, 256);
    }
}
