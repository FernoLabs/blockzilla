# Archive gateway operation logging — deployed 8 September 2026

The epoch 900 reader candidate encountered HTTP 500 and long waits. A live
Worker log records 19,400 ms elapsed time and 1 ms CPU time for one failed V2
range request. Its generic `internal_error` does not identify the failing R2
operation. An independent exact-range read also took 35.290 seconds before
completion, but that client timer includes connection setup. It does not prove
that this second delay occurred in R2.

A later three-read diagnostic measured DNS, individual TCP attempts, TLS,
response headers and body transfer separately. All three reads passed exact
range, length, ETag and body-hash checks. DNS took 0.0003–0.0133 seconds, TCP
0.0036–0.0040 seconds, TLS about 0.010–0.012 seconds, and response headers
0.142–0.270 seconds after connection. Total times were 0.947, 0.455 and 0.301
seconds. The long delay did not recur. There was no configured NAS HTTP proxy.
The filtered Worker tail returned no matching events; that is not evidence
that a request had no error. No probe, reader or tail remains active.

## Prepared change

`edgezilla/archive-samples/src/index.ts` wraps the three existing R2 operations
with an awaited timer. It logs only failures and completed calls taking at least
one second. The log identifies `head`, `get_full` or `get_range`, published
object key, elapsed milliseconds, and outcome. Range logs also include offset
and length. Raw errors, headers, credentials and bodies are not logged.

Each duration stops when the R2 call returns its metadata or stream handle.
The wrapper does not read, buffer or duplicate the body. Exact range and ETag
checks, routes, allowlists, response fields and error status remain unchanged.
The source uses structured logging and a timer around awaited storage I/O,
following the [Worker logging guidance](https://developers.cloudflare.com/workers/observability/logs/workers-logs/)
and [timer behavior](https://developers.cloudflare.com/workers/runtime-apis/performance/).

37 Worker tests pass, including separate slow metadata/range measurements,
all three R2 failure paths, unchanged streaming and error responses, omission
of raw error text/authorization values, and no added log for fast reads.
TypeScript checking and the local Wrangler bundle pass. Current Worker types
5.20260908.1 were consulted; project dependencies were not changed.

Prepared bundle and receipt:
`target/nas-validation/network-window-retry-20260908T070846Z/worker-operation-logging/`.
The receipt records source, tests, config and bundle hashes. This is a separate
Worker diagnostic package; it does not modify either frozen NAS reader build.

## Deployment and live checks

The user approved deployment. All prepared source/config and artifact hashes
passed verification. The exact reviewed bundle was deployed with no rebuild.
Version `37a8f269-32cf-4b35-a835-a02147b20afd` serves 100% of traffic as of
2026-09-08 09:24:57 UTC. The previous version,
`cd8ac1a5-b8ea-4c2f-9850-49c983effee4`, is the rollback reference. The first
command stopped before upload because it could not find the config beside
the prepared bundle; the command with the explicit config path succeeded.

After deployment, 12 sequential NAS reads of the same 7,634,257-byte epoch 900
V2 range passed HTTP 206, exact range, length, ETag and body-hash checks.
Response-header waits were 0.096–0.464 seconds after connection. Total request
times were 0.160–1.589 seconds. No long delay occurred in this sample. The
filtered live log connection was established before the reads; it returned no
operation records during this observation. The probe and log tail have ended.

These repeated reads can warm caches. They are response checks, not an SDK
throughput benchmark, and do not establish a reader speed increase or general
source stability. The log measures the R2 call, not subsequent body transfer.
Do not resume a full epoch timing solely because these checks passed. Use a
short controlled reader test to separate input-window effects from source
delays, and inspect operation logs if a delay returns.

`worker-deployment-receipt.json` in the candidate control directory binds the
deployment versions, prepared bundle, live probe and observation records.
The original preparation receipt is retained unchanged as historical evidence.

## Reader consequence

V2's batch planner stops at 65,536 transactions as well as its compressed and
uncompressed byte limits. Increasing only the compressed-byte target therefore
cannot reliably enlarge HTTP requests. A later V2 change must combine adjacent
input ranges independently of decode admission while retaining strict byte and
buffer limits. The current three-buffer window cannot hide every long delay
when ordered output is required.

V3 already separates input groups from four-block decode jobs, but one producer
still reads planes sequentially. Larger groups reduce request count; one slow
R2 call can still delay the next group. A concurrent plane scheduler is a later
candidate, not an implemented or measured result. Do not add retries or expand
memory limits without a targeted test and new measured package.
