# Developer scripts

These are benchmark, correctness, and reproducibility helpers—not stable
Blockzilla commands. Run them from the repository root, inspect them first, and
write output under `target/` or another ignored directory.

## Safety

Network scripts can consume provider quotas, incur cost, and create large or
sensitive artifacts. URLs and headers passed on the command line may also be
visible to local users.

- `bench-rpc-getblock.sh` prints its endpoint in TSV output.
- `bench_rpc_epoch_report.py` stores the endpoint in reports, and
  `compare_rpc_epoch_reports.py` embeds those reports.
- `reference_rpc_matrix.py` stores endpoint labels instead of URLs, but keeps
  response bodies and headers.
- `run-rpc-correctness-matrix.sh` contacts several providers and requires
  multiple credentials.

Use short-lived read-only credentials, start with dry runs or small inputs, and
review every artifact before publishing it. See the repository
[security policy](../SECURITY.md).

## Inventory

| Script | Mode | Purpose |
| --- | --- | --- |
| `bench-car-registry-macos.sh` | Local, macOS | Compare CAR pubkey-registry strategies. |
| `bench-rpc-getblock.sh` | Live network | Print repeated `getBlock` timings as TSV. |
| `bench_rpc_epoch_report.py` | Live network | Plan and benchmark representative epoch slots. |
| `compare_rpc_epoch_reports.py` | Offline | Compare two epoch benchmark reports. |
| `reference_rpc_matrix.py` | Live network | Build a resumable multi-endpoint correctness corpus. |
| `compare_reference_corpora.py` | Offline | Compare two saved correctness corpora. |
| `run-rpc-correctness-matrix.sh` | Maintainer-only | Run the standard multi-provider correctness matrix. |

Use `--help` where available for prerequisites, inputs, and output paths.

## Safe starting points

Run a bounded local benchmark on macOS:

```bash
MAX_BLOCKS=1000 scripts/bench-car-registry-macos.sh \
  crates/old-faithful/car-reader/benches/fixtures/epoch-157-biggest.car
```

Create a reference plan without sending requests:

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
