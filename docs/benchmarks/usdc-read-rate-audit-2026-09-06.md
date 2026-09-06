# USDC read-rate audit — 6 September 2026

## Finding

The high Compact V2 USDC rates are repeated application reads, largely served by the operating system's page cache. They are not physical SSD throughput and are not decompressed-byte rates. The evidence points to registry cache churn, not an arithmetic error in the byte counter.

Saved run: `/volume2/blockzilla-bench/results/all-v2-ssd-in-out-20260905/jobs/compact-v2/local/epoch-{epoch}/usdc/attempt-001/`.

| Epoch | Final SDK logical MB/s | Sampled process storage-read MB/s | Registry bytes | Registry mode |
| --- | ---: | ---: | ---: | --- |
| 600 | 306.50 | 307.96 | 720,572,192 | Shared full registry |
| 700 | 2,194.78 | 429.32 | 2,171,852,096 | Per-worker sparse cache |
| 800 | 2,082.17 | 426.92 | 1,192,566,592 | Per-worker sparse cache |
| 900 | 377.52 | 377.56 | 889,551,808 | Shared full registry |
| 1000 | 3,281.52 | 444.40 | 1,169,248,992 | Per-worker sparse cache |

Rates use decimal MB. The SDK column covers the complete scan. The storage column is the difference between the first and last saved `/proc/PID/io` `read_bytes` values divided by their sample interval. The last sample can precede completion by up to about ten seconds. These are process-attributed storage reads, not a direct device/NAND benchmark or a precise final whole-run disk rate.

## Why it happens

`crates/blockzilla-read-sdk/src/compact_query.rs` sets the default full-registry limit to 1 GiB. Below that limit, a dense scan can read one shared registry into memory. Above the limit, each worker falls back to eight 64 KiB registry chunks: only 512 KiB per worker, 6 MiB across twelve workers.

`resolve_token_balances()` first filters the mint through the already-bound query key/ID. It does not resolve every mint to a public key before filtering. For each selected balance row, it resolves the owner and token-program keys required by the output. Cache misses call `ensure_registry_chunk()`, which reads a registry chunk and records its byte length. Evicted chunks can be read again many times, and separate workers can fetch the same chunk.

The operating system can satisfy these reads from its page cache. They still incur application read calls and buffer copies. They count in SDK `source_read_bytes` and process `rchar`, but a cached read does not count as new storage I/O in process `read_bytes`.

## Independent counter check

| Epoch | Sample interval, seconds | Process read-call GB (`rchar`) | Process storage-read GB (`read_bytes`) | Ratio |
| --- | ---: | ---: | ---: | ---: |
| 600 | 210.156 | 64.670 | 64.719 | 1.00 |
| 700 | 170.133 | 370.562 | 73.041 | 5.07 |
| 800 | 190.173 | 396.885 | 81.188 | 4.89 |
| 900 | 160.178 | 60.435 | 60.476 | 1.00 |
| 1000 | 190.165 | 625.373 | 84.509 | 7.40 |

The full epoch 1000 SDK scan reports 656,248,129,561 logical bytes across 8,719,144 read calls in 199.982751 seconds. Its independently tracked local-read counter agrees exactly. The sampled process `rchar` rate is 3,288.58 MB/s, close to the final SDK logical rate. This rules out a simple SDK-only summation error as the explanation for the large rate.

The process counters cover all file reads, not a per-file trace. The exact registry-only repeated-byte total was not separately measured. However, the registry mode change, cache-miss code, independent process counters, and absence of the large discrepancy in shared-full epochs identify registry rereads as the cause supported by the available evidence.

## Reporting and performance actions

1. Label the existing counter as application/logical read MB/s, including rereads. Do not call it physical disk speed.
2. Report total elapsed time and transaction rate as the main workload measures, with separately labelled logical reads, process storage reads, output writes, and network bytes.
3. For dense USDC scans, avoid the abrupt fallback to a tiny per-worker cache. Use a single shared registry when the explicit memory budget permits, or design a shared cache/read-only mapped registry. Do not blindly remove the memory bound.
4. Avoid repeated resolution of the same token-program key. Continue to filter by bound mint ID before resolving output owners.
5. Check byte-for-byte output equality when changing the registry strategy. Then compare elapsed time, read-call count, and logical/storage bytes. A lower logical MB/s can be an improvement if duplicate reads disappear.

No SDK setting, binary, benchmark, or archive was changed during this audit. No performance rerun was started.

## Workload clarification

The user expects a stateful, instruction-derived token-account tracker. The measured `usdc-recorded-balances` example is a different workload: it emits recorded pre/post balance rows, with the mint, owner and token-program public keys repeated in each 136-byte output row. Its scan request explicitly disables instruction decoding. It does not discover account creation events or carry an account set into the next epoch. Do not present this benchmark as the performance of that stateful instruction tracker.

For an ID-based tracker, bind known public keys to this epoch's registry IDs, compare IDs while scanning, and resolve each newly discovered account's public key only once for persistent identity. Use a persistent open registry handle and a 32-byte read at `(id - 1) * 32`, or a shared memory image. At the next epoch, remap retained public keys through that epoch's key index; do not reuse raw epoch IDs. First discovery is not always account creation when starting midway through history.

The current NAS memory check showed 8,054,140,928 bytes total RAM and 3,827,077,120 bytes available, with 2,158,940,160 bytes of swap in use. A shared registry for one epoch can be feasible, but has to fit alongside pipeline buffers, output state and other jobs. This is one shared copy across workers, not one full copy per worker. Raising a limit does not implement the stateful workload described above.
