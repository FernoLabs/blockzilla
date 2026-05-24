# getBlock Worker: Triton Difference and Performance Review

Date: 2026-05-20

This note reviews the latest production comparison between the `of-get-block-worker`
Cloudflare Worker and the user-provided Triton RPC endpoint. The Triton URL is not
recorded here because it contains an access token.

## Inputs

- Worker: `https://cloudflare-solana-rpc.cheron-augustin.workers.dev/`
- Worker deployment observed in the last validation pass: `784e3fcc-2717-40f1-bb5f-5d621f6df4f3`
- Comparison artifact directory: `/tmp/blockzilla-correctness-prod-20260520T0810Z`
- Rows: `/tmp/blockzilla-correctness-prod-20260520T0810Z/calls.jsonl`
- Mismatches: `/tmp/blockzilla-correctness-prod-20260520T0810Z/mismatches.jsonl`
- Summary: `/tmp/blockzilla-correctness-prod-20260520T0810Z/summary.json`
- Triton cache: `/tmp/blockzilla-triton-cache-prod`

The run covered epochs `10, 100, 200, 300, 400, 500, 600, 700, 800, 900, 920`.
For each epoch it used the epoch boundary slot plus one seeded random slot, then
called:

- `getBlockTime`
- `getBlock` with `transactionDetails=none`
- `getBlock` with `transactionDetails=signatures`
- `getBlock` with `transactionDetails=accounts`
- `getBlock` with `transactionDetails=full`

All `getBlock` calls used `encoding=json`, `commitment=finalized`,
`maxSupportedTransactionVersion=0`, and `rewards=false`.

Triton rate limit impact in this run was zero: `0` retries and `0.0s` recorded
rate-limit waste. Triton response bodies were served from the local cache for this
final comparison, so correctness is current against the cached Triton baseline and
Worker timings are live for the deployed Worker.

## Correctness Summary

| Metric | Value |
|---|---:|
| Calls | 110 |
| Slots | 22 |
| Strict matches | 69 / 110 |
| Matches if only top-level `blockTime` is ignored | 77 / 110 |
| Strict mismatches | 41 |
| Worker null results | 17 |
| Triton null or error results | 15 |

Mismatch paths:

| Path | Count |
|---|---:|
| `$` | 17 |
| `$.blockTime` | 8 |
| `$.transactions[2].meta.err` | 4 |
| `$.transactions[182].meta.err` | 2 |
| `$.transactions[93].meta.err` | 2 |
| `$.transactions[1755].meta.postTokenBalances[7].uiTokenAmount.uiAmount` | 2 |
| `$.transactions[266].meta.postTokenBalances[5].uiTokenAmount.uiAmount` | 2 |
| `$.transactions[35].meta.postTokenBalances[17].uiTokenAmount.uiAmount` | 2 |
| `$.transactions[1337].meta.logMessages` | 1 |
| `$.transactions[1070].transaction.accountKeys[16].writable` | 1 |

## Per-Slot Diff Detail

This table shows only the mismatched field. The `$` field means the comparison
failed at the whole RPC result/error level, not inside a returned block object.

| Epoch | Slot | Sample | Call(s) | Field | Worker value | Triton value |
|---:|---:|---|---|---|---|---|
| 10 | 4320000 | `boundary` | `getBlockTime` | `$` | `null` | `1586288999` |
| 10 | 4320000 | `boundary` | `getBlock:none, getBlock:signatures, getBlock:accounts, getBlock:full` | `$.blockTime` | `null` | `1586288999` |
| 10 | 4537944 | `random-0` | `getBlockTime` | `$` | `null` | `0` |
| 10 | 4537944 | `random-0` | `getBlock:none, getBlock:signatures, getBlock:accounts, getBlock:full` | `$.blockTime` | `null` | `1586385865` |
| 100 | 43200000 | `boundary` | `getBlock:accounts, getBlock:full` | `$.transactions[182].meta.err` | `{"InstructionError":[0,{"Custom":0}]}` | `"CallChainTooDeep"` |
| 100 | 43269079 | `random-0` | `getBlock:accounts, getBlock:full` | `$.transactions[93].meta.err` | `{"InstructionError":[0,{"Custom":0}]}` | `"CallChainTooDeep"` |
| 200 | 86414917 | `random-0` | `getBlock:accounts, getBlock:full` | `$.transactions[2].meta.err` | `{"InstructionError":[0,{"Custom":41}]}` | `"CallChainTooDeep"` |
| 300 | 129600000 | `boundary` | `getBlockTime, getBlock:none, getBlock:signatures, getBlock:accounts, getBlock:full` | `$` | `null` | `{"error":{"code":-32009,"message":"Slot 129600000 was skipped, or missing in long-term storage"}}` |
| 300 | 129923269 | `random-0` | `getBlock:accounts, getBlock:full` | `$.transactions[2].meta.err` | `{"InstructionError":[2,{"Custom":6000}]}` | `"CallChainTooDeep"` |
| 400 | 172800000 | `boundary` | `getBlockTime, getBlock:none, getBlock:signatures, getBlock:accounts, getBlock:full` | `$` | `null` | `{"error":{"code":-32009,"message":"Slot 172800000 was skipped, or missing in long-term storage"}}` |
| 400 | 173004611 | `random-0` | `getBlock:accounts, getBlock:full` | `$.transactions[1755].meta.postTokenBalances[7].uiTokenAmount.uiAmount` | `99685529752.21405` | `99685529752.21404` |
| 600 | 259200000 | `boundary` | `getBlockTime, getBlock:none, getBlock:signatures, getBlock:accounts, getBlock:full` | `$` | `null` | `{"error":{"code":-32009,"message":"Slot 259200000 was skipped, or missing in long-term storage"}}` |
| 600 | 259368291 | `random-0` | `getBlock:accounts, getBlock:full` | `$.transactions[266].meta.postTokenBalances[5].uiTokenAmount.uiAmount` | `961497420999.5437` | `961497420999.5436` |
| 700 | 302544240 | `random-0` | `getBlock:full` | `$.transactions[1337].meta.logMessages` | `list length: 160` | `list length: 175` |
| 800 | 345696947 | `random-0` | `getBlock:accounts, getBlock:full` | `$.transactions[35].meta.postTokenBalances[17].uiTokenAmount.uiAmount` | `14600554.520117145` | `14600554.520117143` |
| 920 | 397698816 | `random-0` | `getBlock:accounts` | `$.transactions[1070].transaction.accountKeys[16].writable` | `true` | `false` |

## Difference Review

### 1. Old epoch block time

Impact:

- 2 `getBlockTime` mismatches in epoch 10.
- 8 `getBlock` mismatches at top-level `blockTime`.

Examples:

- Slot `4320000`: Worker `null`, Triton `1586288999`.
- Slot `4537944`: Worker `null`, Triton returned `0` for `getBlockTime` and a timestamp in `getBlock`.

Likely cause:

The old Old Faithful data available to the Worker does not consistently carry the
block-time metadata for early epochs. This is not a JSON rendering issue. It is a
source/index completeness issue.

Patch path:

- If exact parity matters for old epochs, extend the v2 index or sidecar metadata
  with block time.
- If we do not want to support old incomplete metadata, document old-epoch
  `blockTime` as best-effort.

### 2. Skipped or missing slot semantics

Impact:

- 15 mismatches across epoch boundary slots `129600000`, `172800000`, and
  `259200000`.
- Each affected slot mismatched for all 5 calls.

Observed behavior:

- Worker returns JSON-RPC `result: null`.
- Triton returns JSON-RPC error `-32009` with message
  `Slot ... was skipped, or missing in long-term storage`.

Likely cause:

Our slot-index lookup can distinguish "no block bytes found", but the RPC layer
currently maps that to `null` instead of Triton's skipped-slot error shape.

Patch path:

- Patched locally after this comparison: native and Worker RPC now return
  JSON-RPC error `-32009` for indexed skipped/missing slots.
- Rerun the production comparison after deploy; this should remove the 15
  skipped-slot mismatches from the table above.
- Keep true "unsupported/unavailable archive range" failures distinct from
  skipped slots.

### 3. Triton-only `CallChainTooDeep` disagreement

Impact:

- 8 mismatches in old epochs, present in both `accounts` and `full`.

Examples:

- Epoch 100, slot `43200000`, transaction `182`:
  Worker `{"InstructionError":[0,{"Custom":0}]}`, Triton `"CallChainTooDeep"`.
- Epoch 100, slot `43269079`, transaction `93`:
  Worker `{"InstructionError":[0,{"Custom":0}]}`, Triton `"CallChainTooDeep"`.
- Epoch 200, slot `86414917`, transaction `2`:
  Worker `{"InstructionError":[0,{"Custom":41}]}`, Triton `"CallChainTooDeep"`.

Meaning:

`CallChainTooDeep` is Solana's `TransactionError::CallChainTooDeep`. In the
Solana transaction-error crate it is documented as "Loader call chain is too
deep". It is a top-level transaction error, not an instruction-level custom
program error.

Cross-check:

After the initial comparison, the same slots/signatures were queried against
public mainnet-beta and Helius. Both agreed with the Worker, not Triton.

| Slot | Tx index | Public mainnet-beta | Helius | Cached Triton |
|---:|---:|---|---|---|
| 43200000 | 182 | `{"InstructionError":[0,{"Custom":0}]}` | `{"InstructionError":[0,{"Custom":0}]}` | `"CallChainTooDeep"` |
| 43269079 | 93 | `{"InstructionError":[0,{"Custom":0}]}` | `{"InstructionError":[0,{"Custom":0}]}` | `"CallChainTooDeep"` |
| 86414917 | 2 | `{"InstructionError":[0,{"Custom":41}]}` | `{"InstructionError":[0,{"Custom":41}]}` | `"CallChainTooDeep"` |
| 129923269 | 2 | `{"InstructionError":[2,{"Custom":6000}]}` | `{"InstructionError":[2,{"Custom":6000}]}` | `"CallChainTooDeep"` |

Likely cause:

This no longer looks like a Worker decoding bug. It looks like Triton has a
historical decoding or archive conversion issue for these old transaction-error
records.

Patch path:

- Do not patch the Worker to match Triton on these rows.
- Treat these as Triton-specific mismatches unless Triton can show another
  canonical source that returns `CallChainTooDeep`.

### 4. `uiAmount` float formatting

Impact:

- 6 mismatches, all tiny float deltas in token balances.

Examples:

- Worker `99685529752.21405`, Triton `99685529752.21404`.
- Worker `961497420999.5437`, Triton `961497420999.5436`.
- Worker `14600554.520117145`, Triton `14600554.520117143`.

Likely cause:

The exact floating JSON representation used by Solana/Triton is not reproduced by
our current conversion path for every magnitude/decimal combination. A previous
attempt to derive `uiAmount` directly from raw amount and decimals fixed some
samples but increased total mismatches, so it was reverted.

Patch path:

- Reproduce Solana's exact token amount conversion and float serialization path.
- For semantic correctness checks, prefer `uiAmountString`; it is exact while
  `uiAmount` is inherently lossy.

### 5. Truncated log messages

Impact:

- 1 mismatch in `full`, epoch 700, slot `302544240`, transaction `1337`.

Observed behavior:

- Worker `logMessages` length: `160`.
- Triton `logMessages` length: `175`.
- Worker body contains a truncation marker before the list ends.

Likely cause:

The archive data we read appears to contain truncated logs for that transaction,
or the log continuation is stored somewhere the current reader does not consume.

Patch path:

- Inspect the raw CAR/metadata frame for slot `302544240`.
- If the extra 15 log lines exist in another frame, teach the reader to join them.
- If they do not exist in Old Faithful, this is a source-data limitation.

### 6. Loaded address writable flag

Impact:

- 1 mismatch in `accounts`, epoch 920, slot `397698816`, transaction `1070`.

Observed behavior:

- Path: `$.transactions[1070].transaction.accountKeys[16].writable`
- Worker `true`, Triton `false`.
- Helius and public mainnet-beta also return `false`.
- The affected key is the System Program
  `11111111111111111111111111111111`, source `lookupTable`.

Likely cause:

Solana signs the v0 message with lookup-table indexes split into writable and
readonly buckets, but the JSON `accountKeys[].writable` field is rendered from
the loaded/sanitized account view. In that view, reserved accounts such as the
System Program are not writable even if the signed message requested them through
the writable lookup bucket.

For slot `397698816`, transaction `1070`, the signed message has one signature
and one required signer. Signature verification against the original message
passes. The lookup table's signed `writableIndexes` are `[27,28,2,22]`, and the
System Program is lookup index `2` at global account index `16`. Rebuilding a
mutated message that moves lookup index `2` from writable to readonly keeps the
same message length but makes signature verification fail. That proves the raw
transaction requested writable; it does not contradict the provider JSON
returning `writable:false`, because that flag is post reserved-key demotion.

Decision:

For Blockzilla archive/RPC output, the signed transaction remains the source of
truth. We keep account-key writability from the signed message and do not apply
reserved-account demotion in the renderer. This intentionally differs from
Helius, public mainnet-beta, and Triton on this field, but preserves exactly what
the user signed.

Validation:

- Sigverify artifact:
  `/tmp/blockzilla-sigverify-writable-demote-397698816-tx1070.json`.
- Transaction signature:
  `4m77cwnoB2PnbKdNaX6Z6Hiyvvwh7u1tWReYvPSDF9QmLU2GDewVFbord5fJoWWskER8xFsr14ewtjdxoJKaz9Zk`.
- Solscan's transaction detail API agrees with Helius, public mainnet-beta, and
  Triton for the rendered account list: global account index `16`
  (`11111111111111111111111111111111`) is shown as `writable:false`. The same
  Solscan payload also exposes raw `addressTableLookups.writableIndexes` as
  `[27,28,2,22]`, so this remains a sanitized-display versus signed-message
  distinction. We may want to revisit whether RPC compatibility should prefer
  Solscan/provider display semantics later.

## Performance Summary

Production benchmark timings from `summary.json`:

| Call | Worker p50 | Worker p90 | Worker p99 | Triton p50 | Triton p90 | Triton p99 |
|---|---:|---:|---:|---:|---:|---:|
| `getBlockTime` | 1.348s | 2.679s | 3.172s | 0.398s | 0.985s | 1.957s |
| `getBlock:none` | 0.708s | 1.815s | 3.064s | 0.374s | 2.131s | 3.045s |
| `getBlock:signatures` | 0.744s | 2.033s | 2.959s | 0.488s | 1.829s | 3.197s |
| `getBlock:accounts` | 1.580s | 6.406s | 8.337s | 1.031s | 4.789s | 30.573s |
| `getBlock:full` | 1.398s | 14.961s | 30.393s | 0.740s | 9.746s | 24.554s |

The largest Worker outlier was `getBlock:full` for slot `397698816`: `30.393s`
client-observed latency for about `7.47 MB` of JSON. The Worker profile for that
same response reported only `296 ms` inside the request path:

- slot index: `65 ms`
- Old Faithful block range download: `83 ms`
- parent block download: `88 ms`
- previous blockhash total: `153 ms`
- response bytes: about `7.47 MB`

## Where The Deployed Latency Comes From

The Worker emits `X-OF-Profile` and `Server-Timing` for profiled calls. Comparing
client-observed time with Worker profile time shows a large gap after the Worker
has fetched block bytes and built the response body.

| Call | Client p50 / p90 | Worker profile p50 / p90 | Fetch p50 / p90 | Client minus profile p50 / p90 | Max response bytes |
|---|---:|---:|---:|---:|---:|
| `getBlock:none` | 708 / 1815 ms | 117 / 337 ms | 117 / 337 ms | 491 / 1483 ms | 197 |
| `getBlock:signatures` | 744 / 2033 ms | 113 / 385 ms | 113 / 385 ms | 520 / 1731 ms | 231944 |
| `getBlock:accounts` | 1580 / 6406 ms | 111 / 314 ms | 111 / 314 ms | 1492 / 6118 ms | 5087728 |
| `getBlock:full` | 1398 / 14961 ms | 138 / 327 ms | 138 / 327 ms | 1310 / 14589 ms | 7467118 |

Interpretation:

- The indexed storage path is usually not the whole problem. R2 slot-index lookup
  plus Old Faithful HTTP range fetch is generally in the low hundreds of ms.
- Large `accounts` and `full` responses are dominated by response transfer,
  buffering, Cloudflare egress, and client read time.
- `getBlockTime` is still slower than it should be because it uses the block path
  for data that should ideally live directly in the index.
- The render sub-profile is currently not trustworthy in production: it reports
  `0 ms` even for multi-MB responses. The timing code uses Worker-side JS clocks,
  and the observed headers show those clocks are not measuring synchronous wasm
  rendering work accurately. Use local native render profiles as the CPU proxy
  until this is fixed.

Compression matters. Single curl probes against slot `173004611` with
`transactionDetails=full` showed:

| Client mode | Total time | Downloaded bytes | Header evidence |
|---|---:|---:|---|
| Plain curl | 4.746s | 5,097,437 | `content-length: 5097437` |
| `curl --compressed` | 1.884s | 559,297 | `content-encoding: zstd` |

For slot `397698816` with `transactionDetails=signatures`:

| Client mode | Total time | Downloaded bytes | Header evidence |
|---|---:|---:|---|
| Plain curl | 1.454s | 165,047 | `content-length: 165047` |
| `curl --compressed` | 1.127s | 121,860 | `content-encoding: zstd` |

This explains why production latency can look much worse than local native
rendering: uncompressed multi-MB JSON is expensive to move, even when the Worker
has already finished the useful archive work.

## Local Native Render Profile

Fixture:
`crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car`

Commands:

```bash
cargo run -p of-get-block-worker --release --bin render_fixture_profile -- 20
cargo run -p of-get-block-worker --release --bin render_alloc_profile -- 20
```

Render profile:

| Mode | Tx | Output bytes | p50 | p90 | Main cost |
|---|---:|---:|---:|---:|---|
| `full` | 2969 | 13,863,801 | 226.883 ms | 461.844 ms | metadata JSON writing |
| `accounts` | 2969 | 12,515,181 | 84.809 ms | 115.990 ms | metadata/account JSON writing |
| `signatures` | 2969 | 269,823 | 7.107 ms | 9.956 ms | CAR read and signature extraction |
| `none` | 2969 | 196 | 5.930 ms | 22.980 ms | CAR read |

Allocation profile:

| Mode | p50 elapsed | p90 elapsed | p50 alloc calls | p50 alloc bytes |
|---|---:|---:|---:|---:|
| `full` | 175.145 ms | 256.497 ms | 37,913 | 42,167,621 |
| `accounts` | 107.286 ms | 176.811 ms | 37,906 | 42,166,513 |
| `signatures` | 7.337 ms | 10.193 ms | 10 | 3,766,640 |
| `none` | 2.023 ms | 3.791 ms | 8 | 317,456 |

Local numbers isolate CAR parsing and JSON rendering. They do not include:

- R2 slot-index lookup.
- Old Faithful range fetch.
- extra parent-block lookup for `previousBlockhash`.
- Cloudflare isolate scheduling.
- Worker-to-client response transfer.
- client decompression or raw body read.

## Local Reader Correctness Pass

Plan:

- epochs `0,100,200,300,400,500,600,700,800,900,920`
- boundary slot plus 100 deterministic random slots per epoch
- 1111 slots total
- supported JSON matrix: `getBlockTime` plus `getBlock` with
  `encoding=json`, `transactionDetails=full/accounts/signatures/none`, and
  `rewards=false/true`

Artifacts:

- plan:
  `/tmp/blockzilla-reference-plan-epochs-0-100-200-300-400-500-600-700-800-900-920-boundary-plus-100.json`
- native local corpus:
  `/tmp/blockzilla-reference-local-json-direct-20260520`
- production Worker corpus:
  `/tmp/blockzilla-reference-worker-json-20260520`
- local-vs-Worker diff:
  `/tmp/blockzilla-compare-local-vs-worker-json-20260520`

The native reader corpus fetched each slot once and rendered all 9 supported
JSON calls from that decoded payload. It wrote 9999 JSON-RPC bodies. The decoded
response corpus is 11.8 GB; after gzip artifact compression it is 2.4 GB.

Local native phase split from `rows.jsonl`:

| Phase | p50 | p90 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|
| Slot fetch/decode | 2932 ms | 19284 ms | 30343 ms | 54221 ms | 215178 ms |
| Per-call render | 1.01 ms | 33.81 ms | 75.10 ms | 349.19 ms | 2182 ms |

Representative local render-only medians:

| Call | p50 render | p95 render |
|---|---:|---:|
| `full,rewards=false` | 13.72 ms | 231.17 ms |
| `accounts,rewards=false` | 7.00 ms | 89.27 ms |
| `signatures,rewards=false` | 0.99 ms | 4.55 ms |
| `none,rewards=false` | 0.28 ms | 1.34 ms |
| `getBlockTime` | 0.32 ms | 1.95 ms |

Production Worker JSON matrix summary:

| Metric | Value |
|---|---:|
| calls | 9999 |
| HTTP 200 | 9995 |
| final HTTP 503 | 4 |
| RPC ok | 9335 |
| RPC errors | 664 |
| p50 | 1.140 s |
| p90 | 3.087 s |
| p95 | 4.001 s |
| p99 | 6.941 s |
| decoded bytes | 11.8 GB |
| compressed wire bytes | 1.59 GB |
| retry sleep | 512 s |

The local-vs-Worker comparison found 9979 byte-identical responses out of 9999.
The 20 differences are:

- 4 production Worker failures for slot `345600208` with `rewards=true`
  (`full`, `accounts`, `signatures`, `none`). Cloudflare returned HTTP 503 with
  "Worker exceeded resource limits"; the native reader rendered all four.
- 16 `previousBlockhash` differences on boundary slots `345600000` and
  `397440000`. The local index directory used raw fallback indexes for epochs
  800 and 920, so the native corpus emitted `previousBlockhash: "todo"`.
  Production Worker has the v2 hash values for those boundaries.

This makes the current production gap very small for the supported JSON surface:
outside the known local-index artifact and one Worker resource-limit slot, the
native reader and production Worker bodies match byte-for-byte.

Comparer speed:

| Checker | Approach | Wall time |
|---|---|---:|
| Python original | parse and recursively diff every JSON body | stopped after ~7 min |
| Python optimized | gzip decode, byte compare first, JSON diff mismatches | ~82 s |
| Rust first pass | streaming gzip byte compare | 606 s |
| Rust optimized | concurrent gzip decode into memory, byte compare first, JSON diff mismatches | 43.1 s |
| Rust optimized + `zlib-rs` + preallocated decode buffers | same result, faster gzip and fewer reallocations | 40.6 s |

The Rust command used for the optimized pass was:

```bash
target/release/compare_reference_corpora \
  --left-label local-native \
  --left-rows /tmp/blockzilla-reference-local-json-direct-20260520/rows.jsonl \
  --right-label worker-prod \
  --right-rows /tmp/blockzilla-reference-worker-json-20260520/rows.jsonl \
  --out-dir /tmp/blockzilla-compare-local-vs-worker-json-rust-prealloc-20260520 \
  --threads 8
```

Targeted provider check for the mismatch slots:

- plan: `/tmp/blockzilla-mismatch-slots-plan.json`
- corpus: `/tmp/blockzilla-reference-mismatch-providers-20260520`
- slots: `345600000`, `345600208`, `397440000`
- endpoints: production Worker, Helius, public mainnet-beta, Triton

| Endpoint | Calls | HTTP/RPC ok | Errors | p50 | p90 | Notes |
|---|---:|---:|---:|---:|---:|---|
| Worker | 27 | 22 | 5 | 1.257 s | 77.336 s | resource-limit failures on `345600208` `rewards=true`; one transient final 500 on a boundary full call with the short retry budget |
| Helius | 27 | 27 | 0 | 0.590 s | 3.158 s | renders the `345600208` `rewards=true` cases successfully |
| Triton | 27 | 27 | 0 | 1.296 s | 5.711 s | renders the `345600208` `rewards=true` cases successfully |
| Public mainnet-beta | 27 | 20 | 7 | 2.404 s | 9.827 s | final 429s after short retries; retry sleep 54 s |

The targeted provider run confirms the `345600208` `rewards=true` failures are
specific to the Worker runtime path rather than bad Old Faithful data.

## Practical Conclusions

The remaining correctness differences are narrow and explainable. The highest
value compliance patches are:

1. Apply writable demotion to loaded program/reserved addresses.
2. Decide whether to enrich old indexes with block time.
3. Reproduce Solana's exact `uiAmount` float path only if byte-for-byte parity on
   lossy floats is required.
4. Investigate the single truncated-log row against raw CAR metadata.

For performance, the next improvements should not start with allocator changes.
The allocator profile says `full` and `accounts` still allocate heavily, so there
is room to improve JSON rendering, but deployed p90 is mostly not allocator time.
The next high-leverage work is:

1. Serve and benchmark compressed clients by default.
2. Move `getBlockTime` and possibly `blockhash` into the v2 index to avoid block
   range fetches for tiny responses.
3. Avoid the parent-block fetch when the v2 index can provide `previousBlockhash`.
4. Fix production render timing so wasm CPU time is visible in `Server-Timing`.
5. Only then continue allocation work for `full` and `accounts`, especially around
   metadata JSON writing.
