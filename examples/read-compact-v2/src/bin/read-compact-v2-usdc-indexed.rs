//! Optional compact balances plus a first-observed account dictionary.
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSource, CompactV2Archive, CompactV2LocalDescriptor,
    CompactV2ParallelScanConfig, ScanRequest,
};
use blockzilla_example_workloads::{
    IndexedUsdcBalanceSink, MAINNET_USDC_MINT, ReadProgress, usdc_scan_request,
};
use blockzilla_model::{AccountResolver, BlockView, IndexedTokenBalance, IndexedTokenSink};
use blockzilla_read_compact_v2::{RunTiming, Source, arguments, finish_workload};
use sha2::{Digest, Sha256};

struct DigestFile {
    file: File,
    digest: Sha256,
}

impl Write for DigestFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn output(path: &Path) -> io::Result<BufWriter<DigestFile>> {
    Ok(BufWriter::with_capacity(
        1 << 20,
        DigestFile {
            file: OpenOptions::new().write(true).create_new(true).open(path)?,
            digest: Sha256::new(),
        },
    ))
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct Progress<'a, S> {
    sink: &'a mut S,
    progress: ReadProgress,
}

impl<S: IndexedTokenSink> IndexedTokenSink for Progress<'_, S> {
    fn visit_indexed_block(
        &mut self,
        block: BlockView<'_>,
        balances: &[IndexedTokenBalance],
        resolver: &mut dyn AccountResolver,
    ) -> blockzilla_model::Result<()> {
        self.sink.visit_indexed_block(block, balances, resolver)?;
        self.progress.observe(block);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = arguments("read-compact-v2-usdc-indexed")?;
    let dictionary_path = sidecar(&args.output, ".pubkeys");
    let source_path = sidecar(&args.output, ".source.json");
    let complete_path = sidecar(&args.output, ".complete.json");
    // A completion record applies to one fresh run, never to a resumed prefix.
    for path in [&args.output, &dictionary_path, &source_path, &complete_path] {
        if path.try_exists()? {
            return Err(format!("output already exists: {}", path.display()).into());
        }
    }
    let started = Instant::now();
    let mut archive = match &args.source {
        Source::Network { origin, cache_root } => {
            CompactV2Archive::open(origin, args.epoch, cache_root)?
        }
        Source::Local {
            epoch_root,
            candidate_id,
        } => CompactV2Archive::open_local(
            epoch_root,
            CompactV2LocalDescriptor::mainnet(args.epoch, candidate_id.clone())?,
        )?,
    };
    let scope = archive.indexed_registry_scope()?;
    let scope_bytes = serde_json::to_vec_pretty(&scope)?;
    let mut scope_digest = Sha256::new();
    scope_digest.update(b"blockzilla.indexed-token-source-metadata.v1\0");
    scope_digest.update(&scope_bytes);
    let scope_digest: [u8; 32] = scope_digest.finalize().into();
    let mut source_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&source_path)?;
    source_file.write_all(&scope_bytes)?;
    source_file.sync_all()?;
    let timing = RunTiming::after_open(started, &archive);
    let request = usdc_scan_request(ScanRequest::all(), MAINNET_USDC_MINT);
    let mut sink = IndexedUsdcBalanceSink::mainnet(
        output(&args.output)?,
        output(&dictionary_path)?,
        scope_digest,
    )?;
    let scan = Instant::now();
    let expected_blocks = u64::from(archive.identity().block_count);
    let parallel = archive.scan_token_balances_indexed_parallel(
        &request,
        &mut Progress {
            sink: &mut sink,
            progress: ReadProgress::new(Some(expected_blocks)),
        },
        CompactV2ParallelScanConfig::new(args.threads).with_full_registry_limit(3 << 30),
    )?;
    let elapsed = scan.elapsed().as_secs_f64();
    let (mut finished, mut dictionary) = sink.finish()?;
    finished.writer.flush()?;
    dictionary.writer.flush()?;
    finished.writer.get_ref().file.sync_all()?;
    dictionary.writer.get_ref().file.sync_all()?;
    let completion = serde_json::json!({
        "schema":"blockzilla-example-indexed-usdc-completion/v1",
        "state":"complete",
        "source_scope_metadata_sha256":hex(scope_digest),
        "source_metadata":source_path,
        "data":{
            "path":args.output, "schema":finished.report.output.schema,
            "rows":finished.report.output.row_count, "bytes":finished.report.output.output_bytes,
            "sha256":hex(finished.writer.get_ref().digest.clone().finalize()),
        },
        "dictionary":{
            "path":dictionary_path, "schema":dictionary.report.schema,
            "rows":dictionary.report.row_count, "bytes":dictionary.report.output_bytes,
            "sha256":hex(dictionary.writer.get_ref().digest.clone().finalize()),
        },
        "coverage":{
            "complete":finished.report.output_complete,
            "indeterminate_transactions":finished.report.coverage.indeterminate_transactions,
            "sha256":finished.report.coverage.sha256_hex(),
        },
        "discovery_semantics":"first observed in selected balances, not account creation",
    });
    // This verifies local input pins and scan/workload totals before completion.
    finish_workload(&args, archive, timing, parallel, elapsed, finished)?;
    println!(
        "indexed_dictionary_path={} indexed_dictionary_rows={} indexed_dictionary_bytes={} source_scope_metadata_sha256={}",
        dictionary_path.display(),
        dictionary.report.row_count,
        dictionary.report.output_bytes,
        hex(scope_digest)
    );
    let mut complete = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&complete_path)?;
    serde_json::to_writer_pretty(&mut complete, &completion)?;
    complete.write_all(b"\n")?;
    complete.sync_all()?;
    Ok(())
}
