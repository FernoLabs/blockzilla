# CAR and Jetstreamer: equal-output comparison

Status: PASS. All six cases passed. The four timed outputs are byte-identical.
The collector independently checked the output and receipt hashes after the run.

The first launch stopped before timing because the source precheck omitted its
HTTP user-agent and the gateway returned 403. No reader case ran. The retry uses
the same binaries and data contract with the corrected precheck header.

Both readers fully decode the same CAR prefix and write the same ordered binary
transaction export. It includes slot, index, first signature, message hash, vote
and failure flags, fee, and pre/post lamport balances. Votes and failed
transactions are included. Signature verification is disabled. Output equality
covers these fields; it is not a complete metadata-field equivalence test.

The runner first checks 64 blocks. It then runs CAR, Jetstreamer, Jetstreamer,
and CAR on 8,192 canonical blocks of epoch 900 (8,925,832 transactions). Both use
12 requested decode workers and mimalloc 0.1.52 on the same NAS and gateway.
Jetstreamer remains pinned to unmodified upstream 0.7.0. The output code is shared.

Acceptance requires exact block and transaction coverage, byte-for-byte output
parity, matching SHA-256, stable source objects, and matching vote/failure totals.
A failed case stops the series. Timing includes output ordering, writes, and final
file sync. Output verification runs after the timer. The comparison includes each
reader's internal representation and callback costs.

| Reader | Total time | TPS | Output size | CPU time | Peak memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| CAR | 79.58 s | 112,159 | 2.300 GB | 72.77 s | 436.0–458.2 MiB |
| Jetstreamer | 144.70 s | 61,685 | 2.300 GB | 598.29 s | 242.8–283.7 MiB |

The earlier 3.10× ratio used a different output contract. It is not the result of
this test. [Build instructions and export layout](../../bench/car-export/README.md).

Times are the means of two runs per reader. TPS is the identical transaction
count divided by mean total time. Output size uses decimal GB.

[Exact case results and verification](artifacts/car-jetstreamer-common-output-20260909.json).
