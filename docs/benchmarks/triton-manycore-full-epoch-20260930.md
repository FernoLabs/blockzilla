# Zilla CAR and Jetstreamer on the EPYC server

Both readers passed the full epoch-1000 comparison: 547,899,266 transactions
and all 175,420,540,252 export bytes matched. Jetstreamer took 295.022897153 s;
Zilla CAR took 440.661107002 s. Jetstreamer throughput was 1.493650531 times
CAR throughput in this sample. The [final evidence](artifacts/triton-epoch1000-final-comparison-20260930.json)
contains the receipts, native verification, hashes, and completed byte comparison.

## Test system and input

The host is `ubuntu@213.239.141.61`: AMD EPYC 9555P, 64 physical cores,
128 hardware threads, about 1.1 TiB RAM, NVMe storage, and Ubuntu 26.04.
Both readers use Rust 1.98.0, release optimization, `-C target-cpu=native`,
and mimalloc 0.1.52.

The source is `https://files.old-faithful.net/1000/epoch-1000.car`, a
767,389,334,224-byte object. The canonical plan covers 431,781 blocks and
547,899,266 transactions in slots `[432000000, 432432000)`, including seven
zero-transaction blocks. Full coverage means every canonical block and
transaction; it does not require decoding the trailing CAR structural records.
The accepted CAR scan reports 767,371,629,390 delivered bytes.

The full Compact V2 index supplied the independent plan. Its payload SHA-256 is
`e65650edc7f486766ccaa9dbe575c876e3844a1e580165b309fe8619eb28ad3a`.
A fresh download matched the cached index. Earlier CAR, V2, and V3 results agree
with its counts, but do not record this exact index hash. The Triton raw slot
index is 5,184,000 bytes, with SHA-256
`6b0e72ff321996c23ef3fcd53a02d812141484ec4eea559f45d67113be1b89e3`.
The controller checks CAR length, the full raw-index hash, and 17 spread-out
64-KiB CAR samples. These checks do not pin every CAR response version.
CAR sharding uses the explicit operator-trusted mode.

## Code and acceptance rules

The benchmark adds independent CAR parser streams over separate indexed ranges.
Each stream checks byte boundaries, slots, and transaction counts. The accepted
run used 64 parsers and a shared budget of 64 decode workers, with one 32-MiB
HTTP body buffer per parser. Production SDK defaults are unchanged.
CAR allows two extra attempts for the exact request-send error, with 100/300-ms
backoff. The same range and identity checks apply again. Structural errors,
identity changes, decode errors, and exhausted body errors still fail.

Jetstreamer uses unmodified official Git revision
[048d2cd25af949679c56347440b7cf5a9a900063](https://github.com/anza-xyz/jetstreamer/commit/048d2cd25af949679c56347440b7cf5a9a900063).
Its package version is still 0.7.0. The latest
[published 0.7.0 release](https://github.com/anza-xyz/jetstreamer/releases/tag/jetstreamer%40v0.7.0)
is commit `cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24`; this Git head is 13 commits later.
It includes PR #102, which fixes block callbacks after work stealing and
connection restart. The adapter lock preserves upstream head dependency
versions, including Agave 4.2.2 and reqwest 0.13.5. See the
[version evidence](artifacts/jetstreamer-version-check-20260930.json).

Jetstreamer defaults to strict rejection of reported errors. The accepted run
used `body-transport`, with one shared limit of two errors per worker/slot for
`Error reading until block: error decoding response body` and
`Error reading header: error decoding response body`. The worker must have no
pending transaction, current callback slot, or export bytes. All other reported
errors fail. Retry events preserve the exact message. CAR retries an exact byte
range; Jetstreamer reopens a worker stream at a slot. These are different retry
units. All retries and backoff remain inside timing. Jetstreamer used 64 workers with
`JETSTREAMER_SPAWN_PENDING=1`, `JETSTREAMER_SPAWN_GRACE_SECS=30`, and
`JETSTREAMER_RECYCLE_PCT=50`.

Both readers fully decode transactions and write the same ordered export:
slot, transaction index, first signature, message hash, vote/failure flags,
fee, and pre/post lamport balances. Votes and failures are included; signature
verification is off. Byte equality covers these fields, not every metadata field.
Order, duplicate detection, callback counts, and the full plan remain mandatory.
Total time includes setup, decoding, spool writes, ordered copy, and final sync.
Native verification of every record, SHA-256, and full byte comparison follow
outside timing. This accepted pair uses one retained CAR run and one new
Jetstreamer run after retry-policy development. Readers ran separately; the
controller rechecked the retained CAR hash and source checks. This is a single
comparison across runs, with shared export code, not a repeated-run mean or a
randomized test.

## Full epoch-1000 result

| Reader | Total seconds | Scan seconds | Transactions/s | Mean CPU cores | Peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zilla CAR, 64 parsers / 64 workers | 440.661107002 | 388.960165441 | 1,243,357.440 | 6.4137 | 5,769.35 |
| Jetstreamer Git head, 64 workers | 295.022897153 | 243.134120562 | 1,857,141.501 | 20.1471 | 1,987.32 |

The throughput ratio uses reader total time, including decode and final export.
Scan time is shown separately. Whole-process time was 450.035698901 s for CAR
and 302.610203558 s for Jetstreamer; it includes process startup and shutdown.
CPU time was 2,886.405036 s and 6,096.704144 s respectively. Mean CPU cores use
CPU time divided by whole-process time, not reader total time.

CAR recovered 16 incomplete-body errors and two request-send errors.
Jetstreamer recovered 42 body errors while reading blocks, with zero header-body
retries. All 42 retries and their backoff are included in the times above.
It recorded 363 work steals, zero connection recycles, and zero operation timeouts.
Native verification confirmed
431,781 blocks, 547,899,266 transactions, 302,828,458 votes, and 66,856,874
failed transactions in each export. All 175,420,540,252 bytes matched, with SHA-256
`4c8c0ef7f0befb33dfbf2642925e2f5511bdce56faf295696e48c2bdf61a7285`.
The server keeps the CAR result in `results-full-epoch-1000-64/car` and the
accepted comparison in `results-latest-jetstreamer-full-r2`.

## Earlier evidence and validation

The earlier epoch-1000 prefix covered 8,192 blocks and 10,204,602 transactions.
CAR took 8.717835994 s; published Jetstreamer 0.7.0 took 6.823393532 s.
Both outputs passed native verification and exact byte comparison, with SHA-256
`500b5b9bd2eae34df83005fb08ad7298995480be2d2457499313f9b1280ec949`.
These short tests are separate from the final Git-head full-epoch comparison.

Epoch 900 has no accepted full comparison. Saved 32-MiB HTTP/1 and HTTP/2 probes
at byte 118,114,876,359 returned HTTP 206 with matching range headers but no body.
The [prefix and failure evidence](artifacts/triton-manycore-prefix-and-failures-20260930.json)
retains these failures and the passing individual tuning cases. The
[first full epoch-1000 attempt](artifacts/triton-epoch1000-first-full-failure-20260930.json)
also failed at 128 workers. Historical Jetstreamer strict-mode runs were stopped
and invalidated by the adapter; they do not prove that normal upstream retries
cannot recover. The first Git-head full run also failed because its earlier
policy rejected a header-body error. Its receipt, binary, and source are retained
in the final evidence. The exact network cause remains unproved.

All 13 actual HTTP fault-test cases passed with the accepted Jetstreamer binary.
Five Rust policy tests, 27 Python control tests, 18 CAR tests, and four native
verifier test groups also passed. Remote evidence and large exports remain under
`/home/ubuntu/benchmarks/zilla-triton-20260930`.
The base commit is `454ed4189dc082729b5babccbc9c5f15ea1861bf`, with benchmark
changes. All 20 recorded local and remote source files matched. The frozen
`final-accepted-benchmark-source.tar.gz` SHA-256 is
`bedf0fb3d39817189cf761eef1455b8e299b4cfb52d8cbb0d16d11a3a383a084`.
The final artifact records source and binary hashes. See the
[full-run instructions](../../bench/car-export/README-full-epoch.md),
[CAR options](../../bench/reader-profile/README-car-decode.md), and
[export contract](../../bench/car-export/README.md).
