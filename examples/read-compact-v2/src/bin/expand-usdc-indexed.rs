//! Expand only a completed compact USDC stream and its matching dictionary.
//!
//! INPUT.complete.json and INPUT.source.json are required. The command does not
//! follow paths stored in either file. It verifies stream hashes during the
//! expansion, so an error can leave partial output (including stdout). Discard
//! that output; only a successful exit establishes the completed expansion.
//! These checks bind the saved files and source metadata to each other. They do
//! not authenticate the chosen metadata against the original registry offline.
use std::{
    error::Error,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use blockzilla_example_workloads::{
    FinishedOutput, INDEXED_USDC_DICTIONARY_RECORD_BYTES, INDEXED_USDC_HEADER_BYTES,
    INDEXED_USDC_RECORD_BYTES, OutputReport, USDC_HEADER_BYTES, USDC_RECORD_BYTES,
    expand_indexed_usdc,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const SCOPE_DOMAIN: &[u8] = b"blockzilla.indexed-token-source-metadata.v1\0";
const MAX_METADATA_BYTES: u64 = 1 << 20;
const DATA_SCHEMA: &str = "blockzilla-example-usdc-indexed-recorded-balance-exclude-failed/v1";
const DICTIONARY_SCHEMA: &str = "blockzilla-example-usdc-source-account-dictionary/v1";

struct StreamExpectation {
    rows: u64,
    bytes: u64,
    sha256: [u8; 32],
}

struct Completion {
    source_scope: [u8; 32],
    data: StreamExpectation,
    dictionary: StreamExpectation,
    expanded_bytes: u64,
    coverage: ExpansionCoverage,
}

#[derive(Clone, Copy)]
struct ExpansionCoverage {
    complete: bool,
    indeterminate_transactions: u64,
    sha256: [u8; 32],
}

struct DigestReader<R> {
    inner: R,
    digest: Sha256,
    bytes: u64,
}

impl<R> DigestReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
        }
    }

    fn verify(&self, expected: &StreamExpectation, label: &str) -> Result<()> {
        if self.bytes != expected.bytes {
            return Err(format!("{label} stream length does not match completion").into());
        }
        let digest: [u8; 32] = self.digest.clone().finalize().into();
        if digest != expected.sha256 {
            return Err(format!("{label} SHA-256 does not match completion").into());
        }
        Ok(())
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(output)?;
        self.bytes = self
            .bytes
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("input byte count overflow"))?;
        self.digest.update(&output[..read]);
        Ok(read)
    }
}

struct VerifiedInputs {
    input: BufReader<DigestReader<File>>,
    dictionary: BufReader<DigestReader<File>>,
    completion: Completion,
}

fn main() -> Result<()> {
    run().map_err(|error| {
        format!("{error}. Expansion failed; discard any output created by this command").into()
    })
}

fn run() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: expand-usdc-indexed INPUT DICTIONARY OUTPUT (use - for stdout); INPUT.complete.json and INPUT.source.json are required".into());
    }
    // Metadata, lengths, and both source-scope headers are checked before any
    // output file is created. Data and dictionary hashes need the full stream.
    let inputs = prepare(Path::new(&args[0]), Path::new(&args[1]))?;
    let coverage = inputs.completion.coverage;
    let writer: Box<dyn Write> = if args[2] == "-" {
        Box::new(io::stdout().lock())
    } else {
        Box::new(
            File::options()
                .write(true)
                .create_new(true)
                .open(&args[2])?,
        )
    };
    let result = expand_verified(inputs, BufWriter::with_capacity(1 << 20, writer))?;
    eprintln!(
        "expanded_schema={} rows={} bytes={} output_complete={} indeterminate_transactions={} coverage_sha256={}",
        result.report.schema,
        result.report.row_count,
        result.report.output_bytes,
        coverage.complete,
        coverage.indeterminate_transactions,
        hex(coverage.sha256),
    );
    Ok(())
}

fn prepare(input_path: &Path, dictionary_path: &Path) -> Result<VerifiedInputs> {
    let completion_bytes = read_metadata(&sidecar(input_path, ".complete.json"))?;
    let completion = parse_completion(&serde_json::from_slice(&completion_bytes)?)?;
    // Never use source_metadata/data.path/dictionary.path from the manifest.
    let source_bytes = read_metadata(&sidecar(input_path, ".source.json"))?;
    validate_source_metadata(&source_bytes)?;
    let mut digest = Sha256::new();
    digest.update(SCOPE_DOMAIN);
    digest.update(&source_bytes);
    let scope: [u8; 32] = digest.finalize().into();
    if scope != completion.source_scope {
        return Err("source metadata scope SHA-256 does not match completion".into());
    }
    let mut input = File::open(input_path)?;
    let mut dictionary = File::open(dictionary_path)?;
    for (file, expected, label) in [
        (&input, &completion.data, "data"),
        (&dictionary, &completion.dictionary, "dictionary"),
    ] {
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != expected.bytes {
            return Err(format!("{label} file length does not match completion").into());
        }
    }
    let mint = verify_header(&mut input, *b"BZUSCI01", INDEXED_USDC_RECORD_BYTES, scope)?;
    let dictionary_mint = verify_header(
        &mut dictionary,
        *b"BZUSDI01",
        INDEXED_USDC_DICTIONARY_RECORD_BYTES,
        scope,
    )?;
    if mint != dictionary_mint {
        return Err("data and dictionary mint headers do not match".into());
    }
    // Hash below BufReader, in read-sized chunks rather than once per row.
    Ok(VerifiedInputs {
        input: BufReader::with_capacity(1 << 20, DigestReader::new(input)),
        dictionary: BufReader::with_capacity(1 << 20, DigestReader::new(dictionary)),
        completion,
    })
}

fn expand_verified<W: Write>(
    mut inputs: VerifiedInputs,
    writer: W,
) -> Result<FinishedOutput<W, OutputReport>> {
    let result = expand_indexed_usdc(&mut inputs.input, &mut inputs.dictionary, writer)?;
    inputs
        .input
        .get_ref()
        .verify(&inputs.completion.data, "data")?;
    inputs
        .dictionary
        .get_ref()
        .verify(&inputs.completion.dictionary, "dictionary")?;
    if result.report.row_count != inputs.completion.data.rows
        || result.report.output_bytes != inputs.completion.expanded_bytes
    {
        return Err("expanded row or byte count does not match completion".into());
    }
    Ok(result)
}

fn parse_completion(value: &Value) -> Result<Completion> {
    require_string(
        value,
        "schema",
        "blockzilla-example-indexed-usdc-completion/v1",
    )?;
    require_string(value, "state", "complete")?;
    let data = stream_expectation(&value["data"], DATA_SCHEMA, INDEXED_USDC_RECORD_BYTES)?;
    let dictionary = stream_expectation(
        &value["dictionary"],
        DICTIONARY_SCHEMA,
        INDEXED_USDC_DICTIONARY_RECORD_BYTES,
    )?;
    let expanded_bytes = encoded_length(data.rows, USDC_HEADER_BYTES, USDC_RECORD_BYTES)?;
    let coverage_value = &value["coverage"];
    let coverage = ExpansionCoverage {
        complete: coverage_value["complete"]
            .as_bool()
            .ok_or("completion has no coverage completeness flag")?,
        indeterminate_transactions: integer_field(coverage_value, "indeterminate_transactions")?,
        sha256: hash_field(coverage_value, "sha256")?,
    };
    if coverage.complete != (coverage.indeterminate_transactions == 0) {
        return Err("completion coverage flag and transaction count disagree".into());
    }
    // A completed extraction can have incomplete evidence. Preserve that state;
    // it does not prevent expansion of the rows that the source did record.
    Ok(Completion {
        source_scope: hash_field(value, "source_scope_metadata_sha256")?,
        data,
        dictionary,
        expanded_bytes,
        coverage,
    })
}

fn stream_expectation(
    value: &Value,
    schema: &str,
    record_bytes: usize,
) -> Result<StreamExpectation> {
    require_string(value, "schema", schema)?;
    let rows = integer_field(value, "rows")?;
    let bytes = integer_field(value, "bytes")?;
    if bytes != encoded_length(rows, INDEXED_USDC_HEADER_BYTES, record_bytes)? {
        return Err("completion stream row and byte counts disagree".into());
    }
    Ok(StreamExpectation {
        rows,
        bytes,
        sha256: hash_field(value, "sha256")?,
    })
}

fn encoded_length(rows: u64, header_bytes: usize, record_bytes: usize) -> Result<u64> {
    rows.checked_mul(record_bytes as u64)
        .and_then(|bytes| bytes.checked_add(header_bytes as u64))
        .ok_or_else(|| "completion byte count overflow".into())
}

fn validate_source_metadata(bytes: &[u8]) -> Result<()> {
    let source: Value = serde_json::from_slice(bytes)?;
    require_string(&source, "schema", "blockzilla-indexed-registry-scope/v1")?;
    let _: blockzilla_model::SourceIdentity =
        serde_json::from_value(source["source_identity"].clone())?;
    let entries = integer_field(&source, "registry_entries")?;
    if entries > u64::from(u32::MAX) {
        return Err("source registry entry count exceeds the ID namespace".into());
    }
    let expected_bytes = entries * 32;
    let admission = &source["registry_admission"];
    match admission["kind"].as_str() {
        Some("pinned-local-file-metadata") => {
            let identity = &admission["identity"];
            require_string(identity, "object", "registry.bin")?;
            if integer_field(identity, "size")? != expected_bytes {
                return Err("source registry size disagrees with its entry count".into());
            }
            for name in ["device", "inode"] {
                integer_field(identity, name)?;
            }
            for name in ["modified_seconds", "changed_seconds"] {
                if identity[name].as_i64().is_none() {
                    return Err(format!("source metadata is missing integer {name}").into());
                }
            }
            for name in ["modified_nanoseconds", "changed_nanoseconds"] {
                if !identity[name]
                    .as_i64()
                    .is_some_and(|value| (0..1_000_000_000).contains(&value))
                {
                    return Err(format!("source metadata has invalid {name}").into());
                }
            }
        }
        Some("strong-etag-object-metadata") => {
            require_string(admission, "object", "registry.bin")?;
            if integer_field(admission, "length")? != expected_bytes {
                return Err("source registry length disagrees with its entry count".into());
            }
            if !admission["url"].as_str().is_some_and(|url| !url.is_empty()) {
                return Err("source metadata has no registry URL".into());
            }
            if !admission["strong_etag"].as_str().is_some_and(|etag| {
                etag.len() >= 2
                    && etag.starts_with('"')
                    && etag.ends_with('"')
                    && !etag.chars().any(char::is_control)
            }) {
                return Err("source metadata has no strong registry ETag".into());
            }
        }
        _ => return Err("source metadata has an unsupported registry admission".into()),
    }
    Ok(())
}

fn verify_header(
    file: &mut File,
    magic: [u8; 8],
    record_bytes: usize,
    scope: [u8; 32],
) -> Result<[u8; 32]> {
    let mut header = [0; INDEXED_USDC_HEADER_BYTES];
    file.read_exact(&mut header)?;
    if header[..8] != magic || header[8..12] != (record_bytes as u32).to_be_bytes() {
        return Err("indexed stream has an unsupported header".into());
    }
    if header[44..76] != scope {
        return Err("indexed stream header does not match the verified source scope".into());
    }
    file.rewind()?;
    Ok(header[12..44].try_into().expect("fixed mint field"))
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn read_metadata(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        return Err(format!("metadata exceeds one MiB: {}", path.display()).into());
    }
    Ok(bytes)
}

fn require_string(value: &Value, field: &str, expected: &str) -> Result<()> {
    if value[field].as_str() != Some(expected) {
        return Err(format!("metadata {field} must be {expected}").into());
    }
    Ok(())
}

fn integer_field(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .ok_or_else(|| format!("metadata has no unsigned integer {field}").into())
}

fn hash_field(value: &Value, field: &str) -> Result<[u8; 32]> {
    let text = value[field]
        .as_str()
        .ok_or_else(|| format!("metadata has no {field}"))?;
    if text.len() != 64 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("metadata {field} must be a SHA-256 hex string").into());
    }
    let mut hash = [0; 32];
    for (output, pair) in hash.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *output = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(hash)
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use blockzilla_example_workloads::IndexedUsdcBalanceSink;
    use blockzilla_model::{
        AccountReference, AccountResolver, ArchiveFormat, BlockHeader, CanonicalBlock,
        CanonicalTransaction, CpiCoverage, ExecutionStatus, IndexedTokenBalance,
        InstructionCoverage, SourceIdentity, SourceVerification, TokenBalanceCoverage,
        TokenBalanceSide, TransactionHeader,
    };

    use super::*;

    struct NoRegistry;
    impl AccountResolver for NoRegistry {
        fn resolve(&mut self, _: AccountReference) -> blockzilla_model::Result<[u8; 32]> {
            panic!("fixture has inline keys only");
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        input: PathBuf,
        dictionary: PathBuf,
        completion: Value,
        source_bytes: Vec<u8>,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let input = directory.path().join("data.indexed");
            let dictionary = directory.path().join("data.indexed.pubkeys");
            let source = serde_json::json!({
                "schema":"blockzilla-indexed-registry-scope/v1",
                "source_identity":SourceIdentity {
                    format:ArchiveFormat::CompactV2,
                    label:"fixture".into(), cluster_id:Some("test".into()),
                    epoch:9, first_slot:99, slots_per_epoch:100, block_count:1,
                    verification:SourceVerification::OperatorTrusted,
                    binding:Some("fixture-candidate".into()),
                },
                "registry_entries":0,
                "registry_admission":{
                    "kind":"pinned-local-file-metadata",
                    "identity":{
                        "object":"registry.bin", "device":1, "inode":2, "size":0,
                        "modified_seconds":3, "modified_nanoseconds":4,
                        "changed_seconds":5, "changed_nanoseconds":6,
                    },
                },
            });
            let source_bytes = serde_json::to_vec_pretty(&source).unwrap();
            let mut scope = Sha256::new();
            scope.update(SCOPE_DOMAIN);
            scope.update(&source_bytes);
            let scope: [u8; 32] = scope.finalize().into();
            let block = CanonicalBlock {
                counts: None,
                header: BlockHeader {
                    epoch: 9,
                    slot: 99,
                    block_ordinal: 0,
                },
                transactions: vec![CanonicalTransaction {
                    header: TransactionHeader {
                        tx_index: 0,
                        status: ExecutionStatus::Succeeded,
                        failed_outer_instruction_index: None,
                        instruction_coverage: InstructionCoverage::Complete,
                        cpi_coverage: CpiCoverage::Complete,
                    },
                    primary_signature: None,
                    required_signers: vec![],
                    instructions: vec![],
                    token_balance_coverage: TokenBalanceCoverage::Complete,
                    token_balances: vec![],
                }],
            };
            let mut sink =
                IndexedUsdcBalanceSink::new(Vec::new(), Vec::new(), [1; 32], scope).unwrap();
            let rows =
                [TokenBalanceSide::Pre, TokenBalanceSide::Post].map(|side| IndexedTokenBalance {
                    tx_index: 0,
                    side,
                    balance_index: 0,
                    account_index: 3,
                    token_account: AccountReference::Inline([2; 32]),
                    mint: Some(AccountReference::Inline([1; 32])),
                    owner: Some(AccountReference::Inline([3; 32])),
                    token_program: None,
                    amount: 7,
                    decimals: 6,
                });
            sink.process_block(block.as_view(), &rows, &mut NoRegistry)
                .unwrap();
            let (data, mapping) = sink.finish().unwrap();
            let completion = serde_json::json!({
                "schema":"blockzilla-example-indexed-usdc-completion/v1", "state":"complete",
                "source_scope_metadata_sha256":hex(scope),
                // These deliberately unusable paths must never be followed.
                "source_metadata":"/not/the/derived/source.json",
                "data":{
                    "path":"/not/the/input", "schema":data.report.output.schema,
                    "rows":data.report.output.row_count, "bytes":data.report.output.output_bytes,
                    "sha256":hex(Sha256::digest(&data.writer)),
                },
                "dictionary":{
                    "path":"/not/the/dictionary", "schema":mapping.report.schema,
                    "rows":mapping.report.row_count, "bytes":mapping.report.output_bytes,
                    "sha256":hex(Sha256::digest(&mapping.writer)),
                },
                "coverage":{
                    "complete":true, "indeterminate_transactions":0,
                    "sha256":hex(Sha256::digest([])),
                },
            });
            std::fs::write(&input, data.writer).unwrap();
            std::fs::write(&dictionary, mapping.writer).unwrap();
            std::fs::write(sidecar(&input, ".source.json"), &source_bytes).unwrap();
            let fixture = Self {
                _directory: directory,
                input,
                dictionary,
                completion,
                source_bytes,
            };
            fixture.write_completion();
            fixture
        }

        fn write_completion(&self) {
            std::fs::write(
                sidecar(&self.input, ".complete.json"),
                serde_json::to_vec_pretty(&self.completion).unwrap(),
            )
            .unwrap();
        }
    }

    #[test]
    fn verifies_completed_files_and_ignores_manifest_paths() {
        let fixture = Fixture::new();
        let inputs = prepare(&fixture.input, &fixture.dictionary).unwrap();
        let result = expand_verified(inputs, Vec::new()).unwrap();
        assert_eq!(result.report.row_count, 2);
        assert_eq!(
            result.writer.len(),
            USDC_HEADER_BYTES + 2 * USDC_RECORD_BYTES
        );
        assert_eq!(&result.writer[..8], b"BZUSDC02");
    }

    #[test]
    fn rejects_record_boundary_truncation_before_expansion() {
        let fixture = Fixture::new();
        let mut data = std::fs::read(&fixture.input).unwrap();
        data.truncate(data.len() - INDEXED_USDC_RECORD_BYTES);
        let dictionary = std::fs::read(&fixture.dictionary).unwrap();
        // Structural parsing alone cannot identify this valid shorter stream.
        assert!(expand_indexed_usdc(data.as_slice(), dictionary.as_slice(), Vec::new()).is_ok());
        std::fs::write(&fixture.input, data).unwrap();
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());

        let fixture = Fixture::new();
        let mut dictionary = std::fs::read(&fixture.dictionary).unwrap();
        dictionary.truncate(dictionary.len() - INDEXED_USDC_DICTIONARY_RECORD_BYTES);
        std::fs::write(&fixture.dictionary, dictionary).unwrap();
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
    }

    #[test]
    fn rejects_same_length_changes_after_hashing_both_streams() {
        for change_dictionary in [false, true] {
            let fixture = Fixture::new();
            let path = if change_dictionary {
                &fixture.dictionary
            } else {
                &fixture.input
            };
            let mut bytes = std::fs::read(path).unwrap();
            let offset = if change_dictionary {
                // Change the token-account key, leaving IDs and row grammar valid.
                INDEXED_USDC_HEADER_BYTES + INDEXED_USDC_DICTIONARY_RECORD_BYTES + 8
            } else {
                INDEXED_USDC_HEADER_BYTES + 61 // amount byte
            };
            bytes[offset] ^= 1;
            std::fs::write(path, bytes).unwrap();
            let inputs = prepare(&fixture.input, &fixture.dictionary).unwrap();
            let mut partial_output = Vec::new();
            let error = expand_verified(inputs, &mut partial_output).unwrap_err();
            assert!(error.to_string().contains("SHA-256"));
            assert!(!partial_output.is_empty());
        }
    }

    #[test]
    fn requires_completion_and_complete_source_metadata() {
        let fixture = Fixture::new();
        std::fs::remove_file(sidecar(&fixture.input, ".complete.json")).unwrap();
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
        fixture.write_completion();
        let source = sidecar(&fixture.input, ".source.json");
        std::fs::remove_file(&source).unwrap();
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
        for bytes in [
            b"".as_slice(),
            b"{",
            b"{}",
            b"{\"schema\":\"blockzilla-indexed-registry-scope/v1\"}",
        ] {
            std::fs::write(&source, bytes).unwrap();
            assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
        }
        let mut changed = fixture.source_bytes.clone();
        changed.push(b' '); // valid JSON, different exact source-scope bytes
        std::fs::write(&source, changed).unwrap();
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
    }

    #[test]
    fn checks_scope_domain_headers_and_completion_counts() {
        let mut fixture = Fixture::new();
        fixture.completion["source_scope_metadata_sha256"] =
            hex(Sha256::digest(&fixture.source_bytes)).into();
        fixture.write_completion();
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
        let fixture = Fixture::new();
        for path in [&fixture.input, &fixture.dictionary] {
            let mut bytes = std::fs::read(path).unwrap();
            bytes[44] ^= 1; // equal file scopes still disagree with verified metadata
            std::fs::write(path, bytes).unwrap();
        }
        assert!(prepare(&fixture.input, &fixture.dictionary).is_err());
        let fixture = Fixture::new();
        for (field, value) in [
            ("state", Value::from("writing")),
            ("schema", Value::from("unknown")),
        ] {
            let mut completion = fixture.completion.clone();
            completion[field] = value;
            assert!(parse_completion(&completion).is_err());
        }
        for field in ["data", "dictionary"] {
            let mut completion = fixture.completion.clone();
            completion[field]["rows"] = Value::from(u64::MAX);
            assert!(parse_completion(&completion).is_err());
            completion = fixture.completion.clone();
            completion[field]["sha256"] = Value::from("00");
            assert!(parse_completion(&completion).is_err());
        }
        let mut completion = fixture.completion.clone();
        completion["coverage"]["complete"] = false.into();
        assert!(parse_completion(&completion).is_err());
        completion["coverage"]["indeterminate_transactions"] = 1.into();
        assert!(!parse_completion(&completion).unwrap().coverage.complete);
        completion["coverage"]["sha256"] = "bad".into();
        assert!(parse_completion(&completion).is_err());
    }
}
