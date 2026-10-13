# Triton comparison on a machine with many CPU cores

`run_manycore.py` compares the CAR decode probe and the Jetstreamer adapter
pinned to Git revision `048d2cd25af949679c56347440b7cf5a9a900063`.
The upstream package version remains 0.7.0, but this revision is newer than the
published release used in the earlier tests. Both read epoch 900 directly from
`files.old-faithful.net`. Both use
mimalloc and produce the [same transaction export](README.md).
Historical artifacts retain their original revision, source hashes, and results.
The current runner accepts only the exact current Git pin.

Build on the test machine from the repository root. The Ubuntu host uses
Rust 1.98.0 and these build packages: `build-essential`, `pkg-config`,
`libssl-dev`, `clang`, `libclang-dev`, `llvm-dev`, `cmake`, and
`protobuf-compiler`. `time` and `sysstat` are installed for separate resource
checks. Use the same CPU target setting for both binaries. The native CPU
setting enables the AES instructions required by `gxhash`; the resulting
binaries are intended for this host.

```sh
RUSTFLAGS='-C target-cpu=native' cargo +1.98.0 build --release --locked -p blockzilla-reader-profile \
  --features reference-mimalloc --bin car-decode-reference
RUSTFLAGS='-C target-cpu=native' cargo +1.98.0 build --release --locked \
  --manifest-path bench/jetstreamer-reference/Cargo.toml
```

The CAR probe must include the worker and HTTP window options added for this
test. The runner uses 16 HTTP workers, sixteen 32 MiB buffers, and fresh HTTP
body allocation. Its decode worker sweep is 12, 32, 64, and 128. Jetstreamer uses
the same decode worker counts and its unchanged upstream HTTP implementation.
The number of requested workers is not the number of CPU cores used. Read the
CPU time in the results to check actual CPU use.

The optional `--parser-workers N` setting splits the CAR input into N ranges
for parallel parsing. It defaults to one. Smoke and baseline cases always use
one parser, and their command line omits the new flag. The sweep and
confirmation cases use the requested parser count. Each decode worker count
in `--workers` must be at least N. For example, use `--parser-workers 8` with
the default sweep, or `--parser-workers 16 --workers 32,64,128`.

With multiple parsers, CAR uses one blocking range downloader per parser. The
HTTP prefetch worker and buffer window settings remain recorded as requested
settings, but they do not control this transport. Receipts record
`http_transport_mode`, `effective_http_workers`, and `http_body_window_bytes` to show
the actual transport. The runner checks the parser count and transport in the
receipt. The raw slot index used to split the CAR ranges is fetched inside the
timed process. Jetstreamer is unchanged. Use a new result directory and preserve
the original one-parser results when testing this option.

Supply the trusted plan for the first 8,192 canonical blocks of epoch 900.
It covers slots `[388800000, 388808198)` and 8,925,832 transactions. The plan
format is described in [the common export instructions](README.md). The plan
can be recovered from the accepted Compact V2 index with `extract_index_plan.py`.
The script requires its independently accepted index hash and records the source
in the plan. The epoch-900 index hash from the earlier accepted result is
`4488c7cf2750c0a7fe11ae798b9d960ed72a0a9e904aa336a30c35198697d55d`.
Recover the plan before timing:

```sh
python3 bench/car-export/extract_index_plan.py \
  --index /data/epoch-900-archive-v2-blocks.index \
  --epoch 900 --start-slot 388800000 --blocks 8192 \
  --expected-index-sha256 4488c7cf2750c0a7fe11ae798b9d960ed72a0a9e904aa336a30c35198697d55d \
  --output /data/epoch-900-prefix-8192-plan.json
```

The runner also checks each complete output against the previously accepted
output hash, size, vote count, and failed transaction count.

```sh
python3 bench/car-export/run_manycore.py \
  --car-bin target/release/car-decode-reference \
  --jetstreamer-bin bench/jetstreamer-reference/target/release/jetstreamer-common-export-reference \
  --plan /data/epoch-900-prefix-8192-plan.json \
  --results /data/triton-manycore-new-run \
  --baseline
```

The results directory must be new. Use a Linux host with no other benchmark,
build, or test process running. Allow approximately 40 GiB for retained exports with the default sweep
and `--baseline`. The runner checks free space before each process. It keeps
all results and logs after a failure. It does not write a local CAR file.

The runner first compares 64 blocks. With `--baseline`, it then runs both
readers with 12 decode workers; CAR uses its earlier four HTTP workers and
eight buffers. The main sweep alternates reader order at each worker count.
Only one reader process runs at a time. For each reader, the runner selects
the fastest passing sweep setting by total time and runs that setting twice
more. Confirmation order is Jetstreamer/CAR, then CAR/Jetstreamer.
Use `--best-repeats N` to change the confirmation count. The final summary uses
only confirmation runs. The tuning results remain separate. Report the
individual confirmation times and their spread with the mean.

Every output must pass exact ordered block and transaction coverage, export
structure checks, byte comparison, SHA-256 comparison, and vote/failure totals.
The prefix runner explicitly requests and checks Jetstreamer's `strict` reported
error policy. Any reported error fails the run, and a receipt that permits body
retries is rejected. The optional recovery mode is available only in the
[full-epoch runner](README-full-epoch.md). Hidden lower-level transport retries
are not counted by the adapter. Connection recycle and timeout counters are
recorded separately. CAR transport counters are kept in each report.

`total_seconds` includes input setup, decoding, shared spool writes, the ordered
output copy, and final file sync. `scan_seconds` excludes the final copy. Output
verification is outside both timers. Each case records process time, user and
system CPU time, peak RSS, mean CPU use, HTTP counters when available, and host
load. The runner records the executable, plan, runner, and verifier hashes.

Triton does not supply the strong ETag required by the usual CAR admission
path. This test uses `--operator-trusted`. Before and after the series, it checks
the complete slot index, CAR length, and three 64 KiB CAR samples against the
accepted source hashes. These checks do not prove complete CAR byte identity
or bind every request to one source version.

Run the local control tests without network access:

```sh
python3 -m unittest discover -s bench/car-export -p 'test_run*.py'
```

Do not run the control with Python `-O`: the shared export verifier uses
assertions. Failed or partial runs do not establish a speed result.
