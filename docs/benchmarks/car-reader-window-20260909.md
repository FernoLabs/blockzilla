# CAR network reader — 9 September 2026

**Buffer reuse reduced large buffer allocations by about 97%, but did not
establish a speed gain. It remains optional and disabled by default.**

The main comparison used the first 8,192 blocks of epoch 900: 8,925,832
transactions per run. Each setting ran twice on the NAS, with 12 decode
workers and a 256 MiB HTTP body window.

| Mode | Scan TPS | Received MB/s | Peak memory MiB | Large buffer allocations |
|---|---:|---:|---:|---:|
| Current / 4 download workers | 178,149 | 196.56 | 349–350 | 292–295 |
| Reuse / 4 download workers | 145,545 | 161.13 | 351–352 | 8 |
| Reuse / 8 download workers | 196,723 | 218.90 | 352–354 | 8 |

Rates use total work divided by combined scan time; setup is excluded.
Received bytes include read-ahead. Memory is peak process RSS.

Four-worker reuse was **18.3% slower**. A separate four-run comparison over
2,048 blocks also showed lower speed: **181,464 → 158,070 TPS**, a 12.9% drop.
Eight-worker reuse varied from 35 to 56 seconds per scan. Network conditions
were not controlled, so these tests do not identify the cause of the difference.
CPU time changed little, and peak memory did not fall.

CAR already used concurrent 32 MiB range downloads. The new option returns
consumed buffers to the download queue. This avoids repeated large allocations,
but does not remove HTTP copies or CAR parsing costs. The existing four-worker
schedule is retained.

All **ten NAS cases** matched the accepted block counts and decode digests.
All **145 tests** passed. CAR performs full transaction and metadata decoding;
its TPS must not be ranked against V2/V3 identity-only scans.

[Full measurements and verification](artifacts/car-reader-window-20260909.json)
include CPU, setup time, byte counts, source identities, and build hashes.
[Design](../design/car-reusable-input.md) and
[run instructions](../../bench/reader-profile/README-car-decode.md).
