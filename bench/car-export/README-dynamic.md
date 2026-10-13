# Selected CAR scheduling experiments

`run_dynamic.py` compares selected reader settings on the same canonical prefix.
It starts one fresh reader process at a time. It keeps every output, receipt,
log, and failed sample in a new result directory. This controller does not
change the accepted full-epoch result or its source files.

Use the complete independently checked epoch plan and dataset manifest from
[the full-epoch procedure](README-full-epoch.md). The controller first checks
the full plan against the manifest, then selects the requested number of rows.
The default is 32,768 canonical blocks. For epoch 1000, this prefix contains
44,966,602 transactions in `[432000000, 432032784)`.

Build the CAR probe with the new scheduling options, the native export verifier,
and the Jetstreamer adapter with their checked-in sources and lockfiles. Use
Rust 1.98.0 and `RUSTFLAGS='-C target-cpu=native'` for both readers, as in the
[many-core build procedure](README-manycore.md). Jetstreamer must use Git
revision `048d2cd25af949679c56347440b7cf5a9a900063`. Stop other builds, tests,
and readers before the experiment.

Create a JSON file with only the cases to test. This first set separates the
effect of HTTP client pools from the effect of dynamic job allocation:

```json
[
  {"name":"static-p64-w64-c1","reader":"car","workers":64,"parser_workers":64,"scheduling":"static","http_clients":1},
  {"name":"static-p64-w64-c8","reader":"car","workers":64,"parser_workers":64,"scheduling":"static","http_clients":8},
  {"name":"dynamic-p64-w64-c8","reader":"car","workers":64,"parser_workers":64,"scheduling":"dynamic","http_clients":8},
  {"name":"jetstreamer-w64","reader":"jetstreamer","workers":64}
]
```

CAR cases also accept `job_mib` (default 256) and `http_chunk_mib` (1–128,
default 32). The larger chunk option applies to indexed static and dynamic
input in the benchmark probe; the SDK default is unchanged. It can reduce range
request count, but it does not reduce total input bytes or parser concurrency.
It is not a confirmed solution for HTTP 429. At 32 parsers, 128 MiB chunks give a
configured HTTP body window of 4 GiB, compared with 1 GiB at the default. Record a
new build and result series when testing this option. Old frozen binaries can
still reject values above 32 MiB.
`http_clients` is the number of separate HTTP client pools. It does not increase
the HTTP concurrency, which remains the parser count. Each client count must be
between one and the parser count. Dynamic mode supports 1–128 parsers and
1–256 decode workers independently. Static mode needs at least two parsers in
this controller and cannot have more parsers than decode workers. Dynamic jobs
end at complete physical block boundaries; their byte size can exceed the
requested target for a single large block.

Both readers accept `export_mode`: `disk` (the default) or `memory`. Memory mode
keeps encoded block payloads in a bounded RAM store until the ordered export.
It avoids the temporary disk spool. It still writes the complete export and
syncs the final file inside the reader's total time. It is not a discard-output
test. A memory case also accepts `export_memory_mib`, an integer from 1 to
262,144; the default is 196,608 MiB (192 GiB). The limit applies to stored
payload bytes, including reserved in-flight allocations. It excludes decoder
buffers, block-map metadata, allocator overhead, and the final writer buffer.
The reader fails if a payload would exceed the limit. Disk cases must omit
`export_memory_mib`.

Use the same export mode and memory limit for both readers in a reader-speed
comparison. A disk-versus-memory pair measures the storage change instead.
For example, this pair keeps the existing local CAR settings:

```json
[
  {"name":"dynamic-p64-w64-memory","reader":"car","workers":64,"parser_workers":64,"scheduling":"dynamic","http_clients":8,"http_chunk_mib":128,"export_mode":"memory","export_memory_mib":196608},
  {"name":"jetstreamer-w128-memory","reader":"jetstreamer","workers":128,"export_mode":"memory","export_memory_mib":196608}
]
```

Before each memory-mode reader, after any source warmup, the controller checks
Linux `MemAvailable`. It requires the requested payload limit plus 16 GiB for
other memory use and saves the sample. A failed check prevents that reader from
starting. This is a preflight check, not a guarantee against later memory
pressure. The extra export memory can also change which CAR pages stay cached.
The receipt must report the selected memory limit, a peak payload size no
larger than that limit, and zero disk-spool bytes. Results record these storage
metrics and `export_finish_seconds` when the reader supplies it.

Memory mode requires newly built adapters. Default disk cases send no new
export flags, and receipts from frozen disk-mode binaries remain compatible.
The effective mode and memory limit are saved with each selected case.

Run one initial sample per case. The paths below are examples:

```sh
JETSTREAMER_SPAWN_PENDING=1 \
JETSTREAMER_SPAWN_GRACE_SECS=30 \
JETSTREAMER_RECYCLE_PCT=50 \
python3 bench/car-export/run_dynamic.py \
  --car-bin /data/bin/car-decode-reference \
  --jetstreamer-bin /data/bin/jetstreamer-common-export-reference \
  --verifier-bin /data/bin/verify-export \
  --dataset-manifest /data/dataset-1000.json \
  --plan /data/epoch-1000-full-plan.json \
  --cases /data/dynamic-initial-cases.json \
  --results /data/dynamic-initial-new-run \
  --blocks 32768 --repetitions 1
```

The controller records the inherited Jetstreamer spawn, recycle, and legacy
index settings. An unset value means the pinned upstream default. Keep these
settings fixed across the experiments. It always requests the explicit bounded
`body-transport` policy. The two permitted response-body error messages, empty
callback-state guards, and shared limit of two retries per worker and slot are
unchanged. Header format errors and all other reported errors remain fatal.
All permitted retries remain in reader time and in the receipt.

Use a separate cases file for a selected pair, a new result directory, and
`--repetitions 3` for confirmation. The default `--order abba` reverses the case
order on alternate rounds. For two cases and three rounds, this gives
`A B B A A B`. `--order round-robin` keeps the same order in each round.
`--order random --seed 1` gives a repeatable shuffled order. No case is selected
automatically, and no full-epoch run starts automatically. Keep the initial
selection results separate from the repeated confirmation results.

Every sample must match the exact block plan and transaction counts. CAR must
also report the requested transport and all completed parser workers. Dynamic
CAR must report all jobs and blocks complete, all decode workers complete, and
the expected bounded raw-block pool and work queue. The native verifier checks
every exported transaction, ordering, flags, lengths, block counts, and vote and
failure totals. The controller hashes every complete output and compares every
byte with the first successful output. These checks are outside reader timing.

For later speed tests, add `--skip-output-verification`. The readers still
decode and write the full export with file sync inside their total time. The
controller keeps the receipt, complete block plan, transaction count, source
identity, binary identity, and export-file-size checks. It skips the native
full-export verifier, full output SHA-256, and full byte comparison.

In this mode, completed cases have state `MEASURED`, the series has state
`MEASURED_UNVERIFIED`, and acceptance is `UNVERIFIED`. There is no output hash or
byte-parity claim. The summary includes every requested measurement; any failed
case still makes the series `FAILED` and prevents a summary. These results are
performance measurements without full output verification. Omitting the flag
retains the full verification procedure and `PASS` acceptance rules.

The complete raw slot-index hash, CAR length, and every manifest CAR sample are
checked before and after the whole experiment. The epoch-1000 manifest contains
17 samples. A connection reset or incomplete response body permits up to three
attempts of the complete source check, with delays of 0.5 and 1 second. Failed
attempts remain in the status file and controller error log. Hash, length, HTTP
status, and other errors are not retried. No partial check can pass. These source
check retries are outside reader timing. The checks do not prove complete CAR immutability or pin each HTTP
response to one object version. The controller records that limit, the manifest
and plan hashes, source checks, binary hashes, helper hashes, commands, and
environment settings.

Reader timing includes source setup, decoding, disk spooling or RAM storage, ordered export,
and final file sync. Each result records total and scan time, process CPU time,
peak RSS, and average CPU use in cores. Host network and disk counters cover the
reader process interval. They can include other host activity. Disk devices and
partitions overlap, so do not add their counters together. Process thread and
I/O counters are sampled every five seconds; they can miss a peak or the last
interval. Dynamic parser and decoder work/wait times are sums of stage wall
times, not CPU time.

Each reader and each verification phase has a configurable 1,800-second limit.
A failed sample does not prevent the remaining selected samples from running.
A stop signal stops the experiment. The final state is `PASS` only if every
requested sample passes, all output hashes and bytes match, and the final
source and binary checks pass. Any failed sample keeps the final state `FAILED`
and prevents an accepted summary. The summary includes every requested repeat;
it does not remove slow samples or retain only the fastest result.

The result directory must be new. Allow space for all retained exports and the
active reader's temporary spool. The default preflight threshold is 64 GiB;
after the first output, it is at least twice that output size plus 4 GiB. These
thresholds are checks before each reader, not a bound on output size. Increase
`--minimum-free-gib` when the selected prefix needs more space.

The same controller can run the full epoch after settings are selected. For
epoch 1000, use `--blocks 431781 --repetitions 1` and suitable longer timeouts,
such as `--timeout-seconds 7200 --verification-timeout-seconds 3600`. Full-epoch
runs always require at least 512 GiB free before each reader, including memory-mode
runs. When output verification is enabled, the native verifier and exact output
comparison cover all 547,899,266 transactions. A single
full run per case remains a single measurement, not a repeated estimate.

## Warm local-cache comparison

The download and server setup instructions below were added after the reader
source archive was frozen. This later edit is documentation only; it does not
change the archived reader code or the recorded archive hash.

Download the CAR and raw slot index into a separate cache directory. The paths
below are examples. Allow space for the full input as well as the retained
exports and temporary spool:

```sh
python3 bench/car-export/download_car.py \
  --dataset-manifest /data/dataset-1000.json \
  --directory /data/cache \
  --operator-trusted --threads 8
```

The downloader uses disjoint 1 GiB CAR jobs and durable partial-job hashes.
Every range must have HTTP 206 and the exact range and length. Redirects,
encoded data, and identity mismatches are rejected. After HTTP 429, one shared
pause blocks new requests from all workers. It uses `Retry-After` when valid,
or 60 seconds when the header is absent or invalid. Requests already in flight
can finish. This is separate from the benchmark reader's retry policy.

To stop deliberately, send SIGTERM and wait for the downloader to exit. Resume
with the same directory, source, dataset manifest, and job size. It checks the
saved bytes against their hashes before continuing. Attempt counts persist.
Increase `--max-attempts` explicitly if a job has used its budget; do not remove
the saved state. For this task, download started with eight workers, then
resumed after a deliberate stop with `--threads 16 --max-attempts 8`, then
with `--threads 32 --max-attempts 10`. Each stop preserved the saved ranges.
This records the selected download settings, not a completed result.

The downloader makes both files read-only for final verification. A full-file
read checks every job hash and computes each file SHA-256.
The full raw-index hash and all 17 CAR samples must match the dataset manifest.
Only after these checks does it publish `manifest.json` with state `COMPLETE`.
`download-state.json` is progress evidence and cannot enable
the server. These checks identify the local bytes. Because the remote CAR has
no strong ETag, they do not prove that the original full CAR stayed unchanged
between download requests; `--operator-trusted` records this limit.

After the completed manifest is present, start the server in a separate
terminal or managed process:

```sh
python3 bench/car-export/serve_cache.py \
  --cache-dir /data/cache \
  --status /data/cache-server-status.json --port 8123
```

The server binds only to `127.0.0.1` and serves only the two epoch-1000 files.
Its first output line and status JSON give its PID, port, base URL, manifest
hash, file identities, counters, and resource use. Wait for `state: READY` and
use that recorded URL and PID below. The server trusts the downloader's
completed hashes; it does not hash the full CAR again at startup. It checks
the read-only file identities before and during transfers, supplies strong
hash-based ETags, and rejects changed files. Keep its source, files, and
configuration fixed for both readers. The local test measures decode and
export through loopback HTTP; it does not measure Triton network delivery.

Add these options to the controller command:

```sh
--base http://127.0.0.1:8123/ --allow-loopback-http \
--source-server-pid 12345 --warm-source-file /data/cache/epoch-1000.car \
--skip-output-verification
```

For the local speed tests, use the pinned Jetstreamer defaults:
`JETSTREAMER_SPAWN_PENDING=24`, `JETSTREAMER_SPAWN_GRACE_SECS=30`, and
`JETSTREAMER_RECYCLE_PCT=50`. Keep `JETSTREAMER_FORCE_LEGACY_INDEX` unset.
The earlier public-source example used a startup limit of one pending stream.
Do not carry that extra limit into the local comparison without recording it.
The skip flag above follows the user's choice for later speed tests; it does
not remove decode, export, or file-sync work from the measured reader time.

The explicit HTTP option admits only literal `127.0.0.1` or `::1`, without
credentials, query, or fragment. The supplied server binds IPv4 only. The
controller checks that the PID runs the checked-in server, that its ready
status names the requested endpoint, and that its complete manifest and both
read-only file identities match. The warmup path must name the served CAR.
The CAR probe receives the explicit loopback flag; Jetstreamer's existing HTTP
base option is unchanged. Receipts retain the actual local source URL.

Before every measured reader, the controller reads the complete CAR with a
32 MiB buffer. It checks file length and identity before and after and records
the bytes and elapsed time. This is a warm local-cache comparison, not a
guarantee that every page remains resident. It does not drop system caches.
Download and warmup time are excluded from reader timing. Common export,
ordered copy, and file sync remain inside reader total time. All canonical
row and count checks remain required. Full output verification remains required
unless the explicit skip option is selected.

Each local sample records server CPU separately from reader CPU, plus combined
reader/server CPU and average cores. Server CPU is sampled around the reader
process, with PID start time checks to reject process replacement. Server RSS
is sampled before and after; its reported peak covers the server lifetime.
These fields do not replace reader-only CPU or peak RSS. Keep the server's own
status, request/error counters, and process logs with the result.

Run the local control tests without network or reader processes:

```sh
python3 -m unittest discover -s bench/car-export -p 'test_run*.py'
```
