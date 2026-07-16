# Developer scripts

The files in this directory are reproducibility, benchmark, and correctness
utilities. They are not Blockzilla product commands or a stable automation API.
Read each script before use and write generated output under `target/` or another
ignored directory.

## Script inventory

### `bench-car-registry-macos.sh`

- **Purpose:** builds `blockzilla` in release mode and compares exact and
  SpaceSaving CAR pubkey-registry strategies, writing one log per strategy.
- **Prerequisites:** macOS, Bash, Rust/Cargo, `/usr/bin/time -l`, and a local CAR
  or CAR.ZST file.
- **Network and credentials:** no runtime network access or credentials. Cargo
  may access the crate registry when dependencies are not cached.
- **Output:** defaults to `target/bench-car-registry-macos`; `MAX_BLOCKS`,
  `INITIAL_CAPACITY`, and `HH_CAPS` tune the run.
- **Status:** active developer benchmark, macOS-specific and corpus-dependent.

```bash
MAX_BLOCKS=10000 scripts/bench-car-registry-macos.sh /data/epoch.car.zst
```

### `bench-rpc-getblock.sh`

- **Purpose:** sends repeated `getBlock` requests for selected slots and prints
  HTTP/RPC status, bytes, and curl timing fields as TSV.
- **Prerequisites:** Bash, curl, and a reachable JSON-RPC endpoint.
- **Network and credentials:** performs live requests. The endpoint comes from
  an argument or `BLOCKZILLA_WORKER_URL`; `CURL_EXTRA` can add curl flags.
  Provider URLs or flags can contain secrets and may be visible in process
  listings, so prefer short-lived read-only credentials.
- **Output:** TSV on standard output; response bodies are kept only in a
  temporary directory that the script removes on exit.
- **Status:** active lightweight benchmark; observe provider rate limits and
  billing.

```bash
ROUNDS=1 scripts/bench-rpc-getblock.sh https://rpc.example 378967388
```

### `bench_rpc_epoch_report.py`

- **Purpose:** selects boundary and random present slots from local slot-range
  indexes, benchmarks `getBlock`, and emits request, per-epoch, and global
  reports.
- **Prerequisites:** Python 3 and local `epoch-N-slot-ranges-v2.raw` files;
  curl is required by the default transport, while `urllib` is available as an
  alternative.
- **Network and credentials:** performs live requests. `--endpoint` or
  `BLOCKZILLA_WORKER_URL` supplies the URL and repeatable `--header` values can
  authenticate. Headers passed on the command line can be visible to local
  users.
- **Output:** a plan TSV, request TSV, per-epoch TSV, and global JSON. These
  reports include the endpoint URL, so inspect and redact them before sharing.
- **Status:** research benchmark. Use `--dry-run` to review the slot plan before
  sending requests.

```bash
python3 scripts/bench_rpc_epoch_report.py 700 \
  --endpoint https://rpc.example \
  --slot-index-dir /data/slot-index \
  --samples-per-epoch 10 \
  --dry-run
```

### `compare_rpc_epoch_reports.py`

- **Purpose:** compares two outputs from `bench_rpc_epoch_report.py` and
  calculates per-epoch and global latency ratios.
- **Prerequisites:** Python 3 and two complete benchmark report directories.
- **Network and credentials:** offline; no credentials required.
- **Output:** comparison TSV, JSON, and Markdown. The global JSON embeds source
  reports and can therefore retain endpoint URLs.
- **Status:** active offline analysis utility; comparison is meaningful only
  when both runs used the same slot plan and request configuration.

### `reference_rpc_matrix.py`

- **Purpose:** creates a resumable correctness corpus across endpoints,
  encodings, transaction-detail modes, and reward modes, then reports structural
  differences from a primary endpoint.
- **Prerequisites:** Python 3, curl, substantial disk space, and one or more
  reachable Solana JSON-RPC endpoints.
- **Network and credentials:** performs many live requests. Pass endpoints as
  `label=@ENV_VAR` to keep URLs out of arguments and artifacts. Endpoint labels,
  not URLs, are persisted.
- **Output:** plan and endpoint metadata, request/response bodies, response
  headers, row JSONL, differences, and summaries under `--out-dir`. Full
  response corpora can be large.
- **Status:** active correctness/research utility; default matrices can generate
  significant provider traffic and cost.

```bash
export REFERENCE_RPC_URL='https://rpc.example'
python3 scripts/reference_rpc_matrix.py \
  --out-dir target/reference-rpc \
  --endpoint reference=@REFERENCE_RPC_URL \
  --primary reference \
  --epochs 700 \
  --samples-per-epoch 1 \
  --dry-run
```

### `compare_reference_corpora.py`

- **Purpose:** compares the saved response rows and bodies from two reference
  corpora and records the first structural difference for each slot/call pair.
- **Prerequisites:** Python 3 and two row JSONL files whose body paths remain
  readable.
- **Network and credentials:** offline; no credentials required.
- **Output:** `mismatches.jsonl` and `summary.json` under `--out-dir`. Output
  retains source body paths and endpoint labels.
- **Status:** active offline correctness utility.

### `run-rpc-correctness-matrix.sh`

- **Purpose:** builds `rpc-correctness-check` and runs a standard matrix against
  Blockzilla, Helius, Triton, Solana mainnet, and optionally an Old Faithful
  endpoint.
- **Prerequisites:** Bash, Rust/Cargo, local slot indexes unless explicit slots
  are supplied, and access to the configured endpoints.
- **Network and credentials:** performs live requests and requires
  `BLOCKZILLA_WORKER_URL`, `HELIUS_RPC_URL`, and `TRITON_RPC_URL`; optional
  variables include `OF_WORKER_URL` and `MAINNET_BETA_RPC_URL`. URLs are exported
  to child processes and may contain provider keys.
- **Output:** defaults to a timestamped directory under
  `target/rpc-bench/correctness-matrix-*`.
- **Status:** maintainer-only integration benchmark. It can be slow, expensive,
  and rate limited; start with `SLOTS` and one `CASES` entry.

```bash
SLOTS=378967388 \
CASES=none:json:false \
scripts/run-rpc-correctness-matrix.sh
```

## Security and publication

Never commit endpoint URLs containing keys, authorization headers, raw response
corpora, generated benchmark reports, or production slot-index trees. Some
tools deliberately retain response bodies and paths for reproducibility. Review
every artifact before publishing it and follow the repository
[security policy](../SECURITY.md).
