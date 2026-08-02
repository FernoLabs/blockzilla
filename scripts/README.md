# Developer scripts

These are benchmark, correctness, and reproducibility helpers—not stable
Blockzilla commands. Run them from the repository root, inspect them first, and
write output under `target/` or another ignored directory.

## Safety

Network scripts can consume provider quotas, incur cost, and create large or
sensitive artifacts. URLs and headers passed on the command line may also be
visible to local users.

- `bench-rpc-getblock.sh` prints its endpoint in TSV output.
- `rpc-epoch-bench` and `rpc-correctness-check` accept endpoints through
  environment-variable references and write only endpoint labels to artifacts.
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
| `run-rpc-correctness-matrix.sh` | Maintainer-only | Run the standard multi-provider correctness matrix. |
| `sync-replay-compact.sh` | SSH transfer + local validation | Resume the replay-minimal Compact epoch-0/1 files from the NAS and publish local Archive V2 manifests. |
| `test-sync-replay-compact.sh` | Offline | Exercise rsync, legacy-SCP fallback, interrupted transfers, and manifest guards. |
| `run-replay-marathon.sh` | Local/NAS, bounded | Resume a sealed Compact chain for several hours with boundary checkpoints, replay metrics, and `pidstat` telemetry. |

Use `--help` where available for prerequisites, inputs, and output paths.

The maintained RPC benchmark and correctness implementations are native Rust
binaries in `blockzilla-get-block`:

```bash
cargo run --locked -p blockzilla-get-block --bin rpc-epoch-bench -- --help
cargo run --locked -p blockzilla-get-block --bin rpc-correctness-check -- --help
```

Both commands plan deterministic representative slots and support several
provider labels in one run. The older Python report/corpus utilities were
retired instead of retaining a second network stack and artifact format.

## Production predecessor-tail seeding

Predecessor-tail discovery and publication is a supported Rust command, not a
Python runtime service:

```bash
blockzilla seed-previous-blockhash-tails \
  --archive-root /srv/blockzilla/archive-v2 \
  --discover \
  --start-epoch 713 \
  --end-epoch 799 \
  --receipt-dir /var/lib/blockzilla/tail-seed-receipts
```

The checked-in `systemd` timer invokes the `blockzilla-current` release. The
superseded Python implementation was retired after fixture and live dry-run
parity; Rust is the only maintained implementation.

## Checkpointed replay marathon

`run-replay-marathon.sh` is the safe wrapper for a long continuation from an
already completed epoch. It accepts only Blockzilla Compact generation
directories named `epoch-N` below an explicit archive root. It never opens a
CAR file and does not fetch or mutate archive objects.

Before launch, the wrapper:

- authenticates the supplied frozen checkpoint with its caller-owned SHA-256;
- runs `probe-compact --max-slots 0` against the completed anchor generation
  and every requested successor, checking the declared epoch, cluster,
  schedule, sealed manifest, control hashes, registries, and sidecars;
- refuses a non-consecutive range or an existing run directory;
- takes an advisory `flock`/`lockf` on the run directory's parent; and
- copies the exact replay binary and anchor checkpoint into the new run
  directory, verifies the copies, and makes them read-only.

The production host needs `pidstat` from `sysstat`, `stdbuf`, and either
`flock` or `lockf`. CPU affinity additionally needs Linux `taskset`. Run the
launch-free internal checks first:

```bash
scripts/run-replay-marathon.sh --self-test
```

Then validate the real inputs without creating the run directory or starting
replay:

```bash
scripts/run-replay-marathon.sh \
  --workspace /absolute/path/to/blockzilla-v1 \
  --binary /absolute/path/to/blockzilla-replay-poc \
  --archive-root /absolute/path/to/compact-epochs \
  --anchor-checkpoint /absolute/path/to/epoch-33.chk \
  --anchor-sha256 e02520615763e3e16dc3815a75fd903cdc90a2f6116c264903ad18983c8e9f25 \
  --completed-epoch 33 \
  --start-epoch 34 \
  --end-epoch 100 \
  --run-dir /absolute/path/to/runs/replay-34-100-20260730T120000Z \
  --duration 6h \
  --cpu-set 2 \
  --nice 5 \
  --sample-diffs 0 \
  --sample-accounts 0 \
  --dry-run
```

Remove only `--dry-run` to launch the same plan. `--duration` accepts seconds
or one `s`, `m`, or `h` suffix and defaults to six hours; an explicit `0`
disables the wall limit. The end epoch is inclusive. For the lowest-overhead
failure search, keep both sample counts at zero.

The run directory is self-contained operational evidence:

| Artifact | Meaning |
| --- | --- |
| `run-metadata.tsv` | Host, checkout, binary, anchor, range, affinity, nice, duration, and lock identity. |
| `input-hashes.tsv` | Binary/anchor hashes and every generation manifest/digest binding. |
| `generation-probes.log` | Exact preflight output for all admitted Compact generations. |
| `command.txt` | Shell-escaped command using the run-local binary and anchor copies. |
| `replay.log` | Raw line-buffered stdout/stderr from replay. |
| `generation-metrics.log` | One flushed `generation_metrics` record per exhausted epoch. |
| `resource-pidstat.log` | CPU, RSS/fault, I/O, and context-switch samples at the configured interval. |
| `pids.tsv` | Supervisor, replay, log-stream, and telemetry PIDs. |
| `checkpoint.latest.chk` | Latest complete boundary published atomically by replay, if one completed. |
| `exit-status.tsv` | Stop reason, all exit statuses, last completed epoch, input recheck, and final checkpoint hash. |
| `resume.env` | Shell-quoted checkpoint, SHA-256, and completed epoch for the next invocation. |

Generation metrics are flushed only after the checkpoint for that generation
has been durably published. On the wall limit, `SIGTERM`, or `SIGINT`, the
wrapper forwards the signal and leaves that last completed-generation file in
place. Exit status `124` means the duration expired; a replay failure retains
the replay binary's nonzero status. If no new generation reached its boundary,
`resume.env` deliberately points back to the immutable run-local anchor.

The hash in `exit-status.tsv`/`resume.env` is convenient output metadata, not
an authenticity boundary merely because it sits beside the checkpoint. Copy
the final checkpoint digest into trusted job metadata before using it as a
future `--anchor-sha256`.

## Replay-minimal Compact epochs 0 and 1

The Compact sync helper requests only the ordered-replay files. Epoch 0 also
requires the exact local mainnet `genesis.bin`; epoch 1 requires the preceding
blockhash tail and deliberately has no genesis file. The default inventory is
size- and present-row-pinned to the authoritative NAS generations. The genesis
digest is fixed to mainnet launch and cannot be changed with a command-line
option.

| Epoch | Present rows | Remote payload bytes | Genesis |
| ---: | ---: | ---: | --- |
| 0 | 431,548 | 110,408,350 | 132,347 bytes, locally supplied and SHA-256 pinned |
| 1 | 430,517 | 236,807,994 | absent |

```bash
scripts/sync-replay-compact.sh \
  --destination /path/with/at-least-600-MiB-free
```

The default `auto` transport first tries rsync over SSH on port 22. If the
macOS openrsync client cannot interoperate with the NAS rsync version, that
individual file is retried with OpenSSH's legacy SCP protocol (`scp -O`). To
select the known-compatible NAS path directly, use:

```bash
scripts/sync-replay-compact.sh \
  --destination /path/with/at-least-600-MiB-free \
  --transport scp \
  --ssh-port 22
```

`--ssh-port` accepts only ports 1 through 65535 and is applied to either
transport. The SSH source syntax and the remote path remain strictly
validated; transport selection never broadens the Compact file allowlist. New
sync markers bind the selected port. A marker from the original port-22-only
helper remains resumable on port 22 and is rejected for any other port.

The default local genesis path is
`/private/tmp/mainnet-genesis/genesis.bin`. Override it explicitly when the
same bytes live elsewhere:

```bash
scripts/sync-replay-compact.sh \
  --destination /path/to/replay-input \
  --genesis-bin /path/to/exact/genesis.bin
```

The exact allowlists are:

- Epoch 0: `archive-v2-blocks.zstd`, `archive-v2-blocks.index`,
  `archive-v2-meta.wincode`, `registry.bin`, `blockhash_registry.bin`, and the
  locally supplied `genesis.bin`.
- Epoch 1: the same five remote core files plus
  `prev_blockhash_tail.bin`, with no genesis file.

`blockhash_registry.bin` is explicitly added to both generated manifests;
`prev_blockhash_tail.bin` is additionally added to epoch 1. The cluster and
epoch width are fixed to `mainnet-beta` and 432,000 so an override cannot
mislabel these size-pinned generations.

Transfers resume at file boundaries into marker-bound epoch directories. Each
requested file is written to a dedicated hidden partial path; its exact pinned
size is checked before it is atomically renamed to the final allowlisted name.
A failed current-file transfer remains a partial and is safely retried, while
already completed size-pinned files are reused. The helper refuses a nonempty
unmarked directory and refuses any existing `archive-v2-generation.json`. It
invokes the Archive V2 gateway's structural manifest generator and publishes
the manifest only after every selected file is present. It never passes a
delete option to either transport. If one epoch has already been published and
a later epoch needs retrying, select it explicitly with `--epoch 1`.

The per-epoch source-directory overrides are intended for another immutable
copy of this same authoritative generation. File sizes and present-row counts
remain pinned; a different generation requires an explicit review and script
update instead of a permissive runtime switch.

Run the offline helper self-test with:

```bash
scripts/test-sync-replay-compact.sh
```

Build once and replay the two completed Compact generations in ledger order:

```bash
cargo build --release -p blockzilla-replay --bin blockzilla-replay-poc
target/release/blockzilla-replay-poc replay-compact-chain \
  /path/to/replay-input/epoch-0 \
  /path/to/replay-input/epoch-1 \
  --sample-diffs 0 \
  --sample-accounts 0
```

The general CAR/RPC utilities listed below are unrelated repository tools.
They are not replay acquisition paths, and their outputs are rejected by the
replay runtime.

## Safe starting points

Run a bounded local benchmark on macOS:

```bash
MAX_BLOCKS=1000 scripts/bench-car-registry-macos.sh \
  crates/old-faithful/car-reader/benches/fixtures/epoch-157-biggest.car
```

Create a reference plan without sending requests:

```bash
export REFERENCE_RPC_URL='https://rpc.example'
cargo run --locked -p blockzilla-get-block --bin rpc-correctness-check -- \
  --endpoint reference=@REFERENCE_RPC_URL \
  --primary reference \
  --output-dir target/reference-rpc \
  --samples-per-epoch 1 \
  --dry-run \
  700
```
