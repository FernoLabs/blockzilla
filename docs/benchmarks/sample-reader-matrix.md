# Sample reader run: NAS disk and public network

## Scope

Run each complete epoch once: **0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000**.
The order is all Compact V2 jobs, all Indexer V3 jobs, then all CAR jobs.
Within each format, run disk first, then network. Run one process at a time.

There are 12 dedicated binaries and 264 jobs. Each format has four examples:

| Selection | Work performed |
| --- | --- |
| `slot-hours` | Count all blocks, transactions, and recorded inner instructions in ordered 9,000-slot buckets. |
| `usdc` | Write the recorded USDC pre/post token balances. This is not the instruction-ledger tool. |
| `pumpfun` | Write transactions with a direct or CPI Pump.fun invocation. |
| `firewatch` | Write the sorted, distinct programs reached by successful transactions for the selected signer wallet. |

The wallet is fixed in `run.json`. Its default is the wallet used by the existing
examples. A target can have no matches in an old epoch. Report that result; do
not select a new target after the run to improve the chart.

This runner replaces the earlier count-only shell runner. Use
`--workloads slot-hours` for that 66-job disk/network scope. Use `--mode local`
or `--mode network` for one source. No reader has a block limit.

## Prepare the NAS package

On the Mac, from the repository:

```sh
CARGO_TARGET_DIR=/private/tmp/blockzilla-sample-archive-build \
  bash scripts/build-archive-sample-bundle.sh /private/tmp/sample-reader-package-NEW
scp -O -P 22 -r /private/tmp/sample-reader-package-NEW \
  ach@192.168.1.46:/volume1/blockzilla/benchmark-results/
```

The build needs the configured Linux musl cross compiler. It produces 12 static
Linux binaries. The NAS needs Bash and Python 3.8 or later, with no Python packages.
The package records the source revision, source changes, and compiler version.
The runner records executable hashes. It never hashes archive files.

On the NAS, select the new package directory:

```sh
cd /volume1/blockzilla/benchmark-results/sample-reader-package-NEW
bash prepare-nas-sample-mirror.sh "$PWD/archive"
bash run-archive-sample-read-matrix.sh --mode both \
  --archive-root "$PWD/archive" --bin-dir "$PWD/bin" \
  --results-root "$PWD/results" --check-only
```

The mirror script uses the repaired sample paths and retained CAR folder. It
creates hard links, not copies. These are regular files, as required by the SDK.
The source and mirror must be on the same filesystem. It refuses to replace an existing mirror.
The preflight checks the fixed object names, disk lengths, public HTTP HEAD
lengths, and executable files. It does not read archive payloads. Length equality
is not proof of content equality: the full readers and output comparison follow.

For the current prepared archive at `/volume2/blockzilla-bench/archive`, use that
directory directly as `--archive-root`; do not run the older mirror recipe again.
Local CAR selection matches the SDK: prefer `epoch-N.car`, then
`epoch-N.car.zst`. Epoch 300 has the raw form; the other samples use compressed
CAR. The reader streams zstd during the timed scan. Preflight does not decompress
or warm those files. Public network CAR still uses `epoch-N.car` and the raw slot
index. The runner records `source_object` and `source_encoding` for each CAR job.
Local compressed and public raw CAR lengths are not compared. Their slot-index
lengths and application results are still compared. Keep these different input
encodings visible in any timing or stored-size chart.

The resolved results directory must be outside the archive and binary
directories, including paths reached through a symbolic link. Keep enough space
for all output files and attempt caches before starting the full matrix.

V3 epochs 200–800 use the September 1 rebuilt reader sets under
`archive-samples-v1-v3-current-rebuild-20260901`, not the earlier V3 gate folder.
The older folder has different transaction-directory and outcome-plane sizes.

After preflight succeeds and the NAS is ready, remove `--check-only` to start.
For a detached run, redirect both streams to `runner.log` with `nohup`. Do not
start it during another read-heavy job if the aim is an idle-host comparison.
No scheduler, compactor, or upload process is stopped by this package.

## Watch and resume

```sh
tail -f runner.log
cat results/status.json
```

The log shows the current format, source, epoch, workload, blocks/s, TPS, and
scan ETA every ten seconds while blocks arrive. During setup or a stalled read,
the resource log still reports activity. Reverse-index queries show no ETA:
visited candidate blocks are not a reliable measure of full-epoch progress.

Run the same command to resume. Successful jobs are skipped. Failed attempts,
partial output, and logs remain in their attempt folders. Retries get a new cache
and output file. A different source inventory, binary, or configuration requires
a new results directory. Resume also rechecks the saved reader metrics and a
streamed SHA-256 of each completed application output, including single-source
jobs with no parity peer. These output checks occur outside reader timings and
do not hash archive payloads. An interrupted process is stopped with its process
group, so it cannot keep writing after the runner exits.

Preflight records local resolved paths, file sizes, inode/device identifiers,
and change/modification times. HTTP admission requires the exact requested URL,
a successful HEAD response, its length, and a strong ETag. The inventory is
checked again after the matrix; changed source evidence fails the run. These
checks are metadata and transport identity checks, not a full archive content hash.

## Metrics and comparison rules

`summary.tsv` is updated after each job. `result.json` retains all reader fields.
`stdout.log`, `stderr.log`, and `resources.jsonl` preserve the raw evidence.
`inventory.json` lists publication object sizes; it is report data, not a reader
manifest or an archive seal. `parity.tsv` gives the final cross-format and
disk/network comparison status per epoch and workload.
`archive-sizes.tsv` is ready after preflight. If sizes differ,
`size-mismatches.json` lists every difference and the combined run does not start.
The local-compressed/public-raw CAR exception above applies only to payload
lengths. A missing slot index or unequal local/public index length still fails
preflight. A successful
group with only one scheduled job is marked `NO_PEER` in `parity.tsv`; it does
not claim cross-format parity. `PREFLIGHT_FAILED` and `INTERRUPTED` remain
distinct from a completed matrix result.

| Metric | Meaning |
| --- | --- |
| `stored_archive_bytes` | Sum of the fixed publication files present for this epoch and format, including optional listed files. |
| `bound_source_size_bytes` | Files bound by this reader. This can be smaller than the stored archive. |
| `source_object`, `source_encoding` | The selected CAR filename and its raw or zstd encoding. |
| `setup_s`, `scan_s`, `total_s`, `wall_s` | Reader setup, scan, total, and external process elapsed time. Output comparison is outside these times. |
| `total_tps` | Epoch transactions covered divided by total reader time. V3 targeted queries can cover transactions without decoding them. |
| `decoded_scan_tps` | V3 transactions actually decoded per scan second. Keep this separate from coverage TPS. |
| `scan_source_mb_s` | SDK logical source bytes per scan second; not physical disk bandwidth. |
| `setup_network_bytes`, `total_network_bytes`, `total_network_mb_s` | HTTP body bytes, including setup; total rate includes setup time. Not wire-level traffic. |
| `scan_local_read_mb_s`, cache counters | SDK file reads and local cache reads, kept separate. OS page cache can satisfy these reads. |
| `physical_disk_mb_s` | Sampled Linux process storage bytes/s. A cached read can report zero. |
| `host_rx_mb_s` | All non-loopback host interface receive bytes/s. Includes other jobs and can count tunnel traffic twice. Not reader download speed. |
| CPU and RSS | Sampled process CPU and resident memory. Samples are not an exact peak-memory measurement. |
| candidate/skipped/decoded counts | Work avoided by V3's reverse index. |

MB is decimal: 1,000,000 bytes. Missing fields stay missing; they are not set to
zero. Final SDK HTTP metrics are the reader-specific download metrics. Live
resource metrics are diagnostic and do not replace them.

For local compressed CAR, logical source bytes count the decoded CAR stream.
The wrapper does not expose compressed file-read counts, so
`scan_local_read_bytes` and `scan_local_read_mb_s` are unavailable for those
jobs. Stored compressed size is not substituted for measured reads.

Each network attempt starts with a separate empty application cache. Within a
job, the SDK can cache and reuse sidecars. The runner does not flush the OS page
cache or Cloudflare's cache. It does not perform a warm pass. Thus these are
single-run, empty-application-cache results, not controlled cold-disk results.
Retain the host load log and disclose concurrent NAS work in the article.

The count check compares every slot-hour bucket, not only totals. Workloads
compare output bytes directly, schema, row counts, completeness, and coverage
counts/digests. Old history can have incomplete recorded metadata: equal incomplete
outputs show format parity, not complete chain history. A mismatch or reader error
fails the run. The runner does not repair or upload anything.

Workload output is flushed by the examples; not all examples call `fsync`.
Do not label these times as durable-write throughput. Output files and network
caches remain on disk and can be large. Check free space before the full run.

## Article plan

1. State hardware, software revision, workers actually used, source, cache policy,
   concurrent jobs, sample epochs, target wallet, and completeness limits.
2. Show stored sizes for CAR, V2, and V3 for every epoch.
3. Show full-scan elapsed time, coverage TPS, and source/download MB/s separately.
4. Show USDC, Pump.fun, and FireWatch elapsed time and output parity. Include zero
   matches and failures. Do not remove unfavourable results.
5. Explain V2's compressed ordered blocks and shared key registry. Explain V3's
   separate data planes and reverse lookup, backed by bytes and decoded/skipped
   counts. A lower MB/s value can accompany a faster query if it reads fewer bytes.
6. Treat one pass as a reference trend, not a statistical claim. Do not extrapolate
   full-ledger duration from one recent epoch without a stated size/workload model.

CAR uses the current CAR example and its SDK. It does not use 12 decode workers
just because V2/V3 receive `--threads 12`. Report the actual reader settings. Do
not confuse this application scan with the separate optimized compactor benchmark.
