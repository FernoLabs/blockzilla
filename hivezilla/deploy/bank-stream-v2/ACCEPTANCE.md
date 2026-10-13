# Native bank v2: provider handoff

Date: 10 October 2026. This is the provider record for the separate
`native-bank-compact-v2` profile. The owner request remains unchanged in
`docs/blockzilla-confirmed-stream-spec.md`. V1 admission remains separate.

The patched native host and plugin are installed on Foundation. The public v2
routes require a backend Bearer header. The public Rust soak and finite
JavaScript resume probe passed. Provider correctness checks passed; the proposed
250 ms p95 latency target was not met. This record does not certify Ferno
application acceptance or sustained JavaScript hash readiness.

## Native source and artifacts

The native host revision is `d688dd0e3a576b048a848e5faf2e8d277f8feb6e`.
The loaded plugin source snapshot is
`d1c69dc412fa2c3408fca2b95d97fb9f492337cd`. Its SHA-256 is
`ca1a3b451758bb42fb88c11b2437e2b67543d4d7d004dd7ca1b4339a41b4ad34`.
The native publication timing CLI was built from
`c1286dcc7f08c8aa1ab963dedb180daa800beafc`; the loaded plugin did not change
when that metric was added.

Source identity is pinned to genesis, node, Agave revision, and session. The
current session is `000000000000000018dd432f0e8f799c-282459`. Validator PID
2630745 runs the tested immutable host and has the tested library loaded.
The isolated host load test passed through the real plugin manager and real
Bank. Its skipped-slot test bank has slot 17, height 1, and native expiry 151.
Both frozen and confirmed tuples match the durable test journal. These are
test-genesis results; the live tuple comparison is a separate server-side check.

The source snapshots are in the isolated Foundation Git repository at
`/home/ubuntu/benchmarks/blockzilla-bank-v2-20261010/provider`. The primary Mac
working tree has other work and was not committed as part of this build.
The [final source receipt](../../stream-relay/evidence/bank-stream-v2-20261010/PROVIDER-SOURCE.json)
records the final provider commit and file manifest digest. The full file
manifest stays in that isolated checkout's parent directory. The
[runtime artifact receipt](../../stream-relay/evidence/bank-stream-v2-20261010/RUNTIME-ARTIFACTS.json)
records the immutable host, plugin, producer, transport, consumer, and v2 Wasm.

## Provider acceptance coverage

| Requested case | Test or evidence |
| --- | --- |
| 1. Processed alone cannot confirm or supply a hash | Shared Rust ClientState tests; JavaScript BankState tests |
| 2. Exact complete bank and both digests | Shared controls test; real encoded compact fixtures; strict dependency matching |
| 3. Same-slot banks and exact ancestors | Native bank isolation tests; persistent distinct positions; shared Chain parent walk and height check |
| 4. Early confirmation waits; incomplete data cannot publish | Real native writer test records confirmation before entry/block completion; dense Tower completeness tests |
| 5. Failed execution retains error | Real encoded Rust compact fixture, JavaScript expected decode; live decoded error counts |
| 6. Skipped slots retain native expiry | Actual host/plugin load test, slot 17 / height 1 / queue expiry 151 |
| 7. Source, digest, profile, and cursor rejection | Shared strict admission; JavaScript tests for all four source fields; public 400/401/410 checks |
| 8. Resume and idempotent application | Native crash/reuse test; shared duplicate admission; JavaScript durable acknowledgement test; live consumer restart |
| 9. Restart retains dependencies; eviction is a gap | Durable BankStore restore and byte eviction tests; real transport restart with the same spools |
| 10. Source stop removes readiness | Health age tests; no new source timestamp from a heartbeat; monotonic client expiry |
| 11. Descendant invalidation and rooted failure | Shared Chain tests; client descendant removal and source failure paths |
| 12. Finite queues, frames, and readers | Native queue/quota/oversize tests; frame and notice bounds; byte eviction and maximum batch tests |
| 13. Older root cannot renew head | Shared chain, client age, and BankStore tests |
| 14. Root uses frozen native queue data | Real writer root without a prior confirmed callback; missing tuple fails publication |
| 15. Clock, lag, and repeated old health | Shared future-clock/lag tests; delayed factual sample test; JavaScript wall-clock/monotonic test |
| 16. SSE boundaries, dependency GETs, and gaps | Real Axum route tests; notice preflight; public route checks and retained start cursor |

The compact fixture files and expected decoded JSON are under
`hivezilla/stream-relay/client/fixtures/bank-v2`. They contain synthetic test
transactions written with the current Rust compact encoder. They are binary
fixtures, rather than JSON-only examples. They prove decode behavior, not the
native plugin boundary. The separate native host test supplies that evidence.

All Cargo commands ran on Foundation. Recorded commands are in the build
receipts. The final test groups are: 9 shared contract tests; 30 relay tests;
13 normal decoder tests plus explicit fixture export; 27 native tests including
the real writer tests; 25 JavaScript tests; and 3 legacy Rust consumer tests.
The new Rust collector is also built and checked through the public integration
probe. The old v1 Wasm asset and decoder entry points remain separate.

An independent local finalized RPC comparison checked three public-stream
blocks, their parent identity, actual height, and 3,190 ordered first signatures.
All matched. This is a sample, not a full epoch or full metadata comparison.

## Backend use

Use `https://stream.blockzilla.dev/` and a private key file. The operator source
file is `ROOT/reports/bank-stream-v2-20261010/expected-source.json`, where ROOT is
`/home/ubuntu/live/hivezilla-validator-20260930`. Give this source file to the
consumer through a trusted operator path. Do not accept a changed source merely
because a public head returns it.

```sh
bank-consumer https://stream.blockzilla.dev/ KEY_FILE EXPECTED_SOURCE_JSON ledger.sqlite 1000
```

The routes are `/v2/banks/head`, `/v2/banks/events?after=CURSOR`, and
`/v2/banks/chunks/SHA256`. The public relay has no private `/v2/banks/state`
route. URL keys do not authorize v2. They remain an optional v1 demo feature.

The Rust collector resolves the exact head dependency, preserves all failed
transactions and loaded keys, and commits payload/event/cursor atomically. It
has at most four active finite GETs and twelve ordered jobs for fetch and native
decode. It never applies a later result before an earlier result. Decoded
payloads are stored once, rather than copied into each control row. Reopen the
same database after a normal restart. A changed source or expired boundary
requires reconciliation and a separate source decision.

The JavaScript client is an opt-in `./banks` module. See `client/BANKS-V2.md`.
It has four bounded GETs and applies acknowledgements in order. Its current
Wasm decode runs on one JavaScript thread. Finite resume is tested. Sustained
full-detail JavaScript decode capacity must be measured separately; slow decode
correctly removes hash readiness.

## Measured provider result

The [public soak summary](../../stream-relay/evidence/bank-stream-v2-20261010/SOAK-SUMMARY.json)
records an 11-minute live collection period. Final server-side validation took
another 195 seconds; its `seconds` value includes that validation. The consumer
applied 9,091 consecutive unique events: 3,030 blocks, 3,030 confirmations, and
3,031 roots. Its deliberate restart resumed the saved cursor with no missing or
duplicate events. No live invalidation occurred; invalidation is covered by the
provider tests above.

The consumer decoded 3,099,103 transactions, including 318,892 failed executions.
All failed executions had a non-null error. All transactions had resolved keys.
The last native sample matched 134 confirmation tuples with zero mismatches.
The original start cursor remained available. The byte-bounded spool retained
1,944.899 seconds, about 32.4 minutes, in 4,294,942,749 frame bytes.

| Measured stage | p50 | p95 |
| --- | ---: | ---: |
| Native confirmation callback to durable control | 349 ms | 639 ms |
| Durable control to Rust consumer | 669 ms | 4,854 ms |
| Native callback to Rust consumer, whole test | 1,048 ms | 5,387 ms |
| Native callback to Rust consumer, final 120 seconds | 1,041 ms | 1,563 ms |
| Ordered SQLite application | 0 ms | 13 ms |

These are separate distributions; their percentiles must not be added. They
include the actual public route, consumer restart, and planned eight-second
publication pause. The head had 123 live and three stale samples, zero failed
samples, and zero unavailable HTTP samples. During the pause, source time did
not refresh and readiness became stale. Publication restored a live head. The
validator and native worker retained their PIDs. The
[source-stop receipt](../../stream-relay/evidence/bank-stream-v2-20261010/SOURCE-STOP.json)
and [transport restart receipt](../../stream-relay/evidence/bank-stream-v2-20261010/TRANSPORT-RESTART.json)
record the separate checks.

The [resource summary](../../stream-relay/evidence/bank-stream-v2-20261010/RESOURCES.json)
records about 13.72 provider events and 4,710 decoded transactions per second.
The Rust consumer averaged 1.29 CPU cores and peaked at 1.31 GB RSS. The v2
origin averaged 0.70 cores; the shared v1/v2 relay averaged 0.57 cores. Service
memory figures include filesystem cache and are not process RSS. These are one
consumer measurements on Foundation, not a fan-out capacity test or a remote
second-machine consumer test.

The [JavaScript probe](../../stream-relay/evidence/bank-stream-v2-20261010/JAVASCRIPT-SUMMARY.json)
decoded 80 events and 27,896 transactions, preserved 2,154 failed execution
errors, and resumed exactly. It recorded zero hash-ready observations. Its
single-thread full-detail decode and durable file writes did not reach current
health. This proves finite decode/resume, not sustained ready transaction
transport. Use the measured Rust path; Ferno needs a separate capacity test and
possibly parallel decode before it uses the JavaScript path for signing.

## Measurement and limits

Native callback time remains immutable. The producer records publication time
in a separate SQLite table. The public probe separates callback-to-control,
control-to-consumer, and apply time. It records provider/consumer resource use,
retained bytes and time, exact cursor resume, and native tuple matches. Private
frames, signatures, SQLite files, and raw probe logs remain on Foundation. Only
small operator summaries are exported.

The 250 ms p95 value is a proposed target. Click-to-wallet and send-to-confirm
are Ferno measurements and are not performed by this provider test. Source
identity and replay correctness alone do not establish that target.

This release does not implement Ferno wallet watches, one-send reconciliation,
account hydration/undo, full history recovery, replay from genesis, Alpenglow
completeness, a second relay machine, or automatic source-session rebinding.
V2 replay is local and bounded. The separate processed R2 publisher can be behind
live delivery. Local compact/evidence retirement after upload is unfinished;
capture can stop at the disk reserve. No full uninterrupted epoch is claimed.

The first installation exposed quoted argument and systemd override-order
errors. The excluded startup/old-host captures remain in the deployment record.
Later tests found transient bootstrap and wrapped response-body reset errors,
slow checkpoint commits, and slow sequential client decode. Their files and
failed probes remain on Foundation. Transport upgrades retain the same source
and spools; they do not turn a failed interval into claimed coverage.
