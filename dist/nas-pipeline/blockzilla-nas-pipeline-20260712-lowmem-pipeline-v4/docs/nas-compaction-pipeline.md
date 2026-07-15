# NAS compaction pipeline

This runbook packages and operates the NAS-local Hivezilla control plane for
historical CAR compaction and live epoch packaging. The production resource
model is deliberately simple:

- up to four low-memory first-seen CAR scans may run concurrently;
- exactly one memory-heavy finalizer may run, with no scan overlapping it;
- live capture remains continuously available;
- a closed live capture enters the same exclusive finalizer lane only after its
  completeness and repair gate passes.

The pipeline starts in monitor-only mode. It does not launch a scanner or
finalizer until `--execute` is explicitly enabled.

## Source of truth

The filesystem is authoritative. Process lists and old lane logs are useful
observations, but they are not durable completion records.

| Evidence | Pipeline meaning |
| --- | --- |
| `epoch-N.car.zst` or `epoch-N.car` | Historical input is available |
| Running scan child plus no durable scan marker | Scanning |
| `archive-v2-first-seen-scan-complete.v1` | Scan is durable and waiting for the exclusive MPHF finalizer |
| Finalizer child is running | Finalizing |
| Nonempty reader core and required access sidecars; no scan marker | Historical epoch complete |
| `FINALIZE-NEXT.md` in a closed live capture | Live capture is awaiting inspection/repair gating; it is not final by itself |
| Capture journal reports missing fields | Repair required, even if a compact archive can technically be produced |

Never use the mere existence of `archive-v2-meta.wincode` as completion. Some
builders create it at finalizer startup, so a killed process can leave an empty
or incomplete file. The legacy NAS lane scripts use `-f` for this check and
must not be used as the new pipeline's completion rule.

The reader core includes the pubkey registry and counts, MPHF, blockhash
registry, block blob and index, metadata, PoH, shredding, signatures, and vote
hash registry. Pipeline-owned first-seen output additionally requires its final
audit manifest. Older frequency-sorted archives predate that manifest and are
valid without it; `registry-hot-seed.bin` is an optimization, not archive
validity evidence. With the packaged default (`HIVEZILLA_NO_ACCESS=0`), both
block-access files are also required; a legacy reader-complete archive that
lacks them is reported as an index-sidecar backlog, not silently called done.

The reconciler should classify the union of raw CAR inputs, canonical archive
directories, and live capture directories. Many old canonical archives remain
after their source CAR has been removed, and many future epoch directories
contain prebuilt sidecars without being complete.

## Historical scheduling

Historical scan jobs use the low-memory first-seen builder in deferred mode:

```bash
blockzilla build-archive-v2-hot-blocks \
  /volume1/blockzilla/epoch-N.car.zst \
  /volume1/@home/ach/dev/blockzilla-v2/epoch-N \
  --previous-car /volume1/blockzilla/epoch-PREV.car.zst \
  --first-seen-registry \
  --first-seen-scan-only \
  --first-seen-finalizer-lock /path/to/state/first-seen-finalizer.lock \
  --first-seen-decode-workers 4 \
  --level 1
```

The scheduler owns the exact input selection and previous-epoch seed handling.
All four scanners must use the same finalizer lock. A completed scan writes and
syncs its candidate files before atomically publishing
`archive-v2-first-seen-scan-complete.v1`.

Block-access sidecars are enabled for production because they feed wallet and
reverse-index use cases. `--no-access` remains an explicit throughput/storage
option for experiments, repairs that do not need the sidecar, or constrained
backfills; it is not the packaged default.

The single finalizer uses the same lock exclusively:

```bash
blockzilla finalize-archive-v2-first-seen \
  /volume1/@home/ach/dev/blockzilla-v2/epoch-N \
  --finalizer-lock /path/to/state/first-seen-finalizer.lock
```

Finalization revalidates the full registry/count audit, builds the MPHF, and
publishes metadata last. It is safe to retry after an interruption. Do not run
an independent finalizer outside Hivezilla: the live finalizer does not
currently participate in the first-seen file lock, so only the pipeline-wide
exclusive resource gate prevents an unsafe overlap.

Recommended NAS settings:

```text
scan concurrency        4
projected RSS per scan  800 MiB
free-memory reserve     256 MiB after projected scan admission
free-disk reserve       256 GiB before starting new work
historical finalizers   1, exclusive
live finalizers         1, in the same exclusive lane
live capture            always reserved; never stopped to make room for scans
```

Reduce scan concurrency before increasing per-scan decoder workers. The four
lane proof peaked around 1.12 GiB for four 3,000-block scans, but full epochs
have larger registries and must retain memory headroom for live capture, the OS,
and filesystem cache.

## Live capture and repair gate

The current live supervisor writes append-friendly capture directories and
rotates at Old Faithful epoch boundaries. Its important artifacts are:

```text
producer-layout.json
blocks/live-no-registry-blocks.bin
index/block-index.bin
index/pubkey-runs/
poh/poh.wincode
journal/grpc-blocks.jsonl
FINALIZE-NEXT.md                 # only after capture close
```

The active JSON report on stdout is written only when capture exits. Realtime
monitoring primarily reads `journal/progress.json`, which live capture updates
atomically about every three seconds. If that snapshot is absent or stale, the
pipeline falls back to the append-only `journal/grpc-blocks.jsonl` tail. Its
final line may be partially written; readers must ignore an invalid trailing
line rather than classifying the capture as corrupt.

At the epoch boundary the pipeline must:

1. confirm the capture process closed successfully at the boundary;
2. run or consume `inspect-capture` and verify block/index/PoH consistency;
3. summarize missing fields and unavailable-slot gaps from the journal;
4. wait for required RPC/CAR repair jobs and bounded pubkey runs;
5. publish an empty `READY-TO-PACKAGE` approval marker in the capture directory;
6. enqueue live packaging into the exclusive finalizer lane;
7. run `build-archive-v2-hot-blocks-from-live` with `--registry-source runs`;
8. require a successful child exit and validate final sidecars before classifying it complete;
9. preserve repair provenance in the final output.

The finalizer command is structurally:

```bash
blockzilla build-archive-v2-hot-blocks-from-live \
  /volume1/@home/ach/dev/blockzilla-live/epoch-N-capture-TIMESTAMP \
  /volume1/@home/ach/dev/blockzilla-v2/epoch-N \
  --registry-source runs \
  --level 1
```

`MissingShredding` is a real repair state. The current live finalizer can write
empty shredding records, so the dashboard must distinguish "compact packaged"
from "canonical and repair-complete" rather than presenting both as green.
The API serializes the former terminal state as `packaged`: it is not queued a
second time, but the dashboard keeps it visually distinct from canonical
`complete` until repair and wallet-index sidecars exist.
`FINALIZE-NEXT.md` is a human handoff marker and contains no machine-readable
repair approval by itself. Hivezilla will not create `READY-TO-PACKAGE`; a
repair worker or operator publishes it only after the checklist above passes:

```bash
touch /volume1/@home/ach/dev/blockzilla-live/epoch-N-capture-TIMESTAMP/READY-TO-PACKAGE
```

## API and dashboard

Hivezilla serves the static dashboard and API from one process:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Liveness check |
| `GET /api/v1/status` | Complete current pipeline snapshot |
| `GET /api/v1/events` | Realtime Server-Sent Events state stream |
| `POST /api/v1/control/pause` | Stop scheduling new work |
| `POST /api/v1/control/resume` | Resume normal scheduling |
| `POST /api/v1/jobs/{kind}/{id}/pause` | Send `SIGSTOP` to one managed job |
| `POST /api/v1/jobs/{kind}/{id}/resume` | Send `SIGCONT` to one managed job |
| `POST /api/v1/jobs/{kind}/{id}/retry` | Safely retry a failed pipeline-owned item |

The status snapshot reports historical epoch states, active scan lanes,
exclusive-finalizer state and queue, live capture progress, throughput/ETA, and
host memory/storage observations. The epoch visualization should keep these
states distinct:

```text
unavailable -> queued -> scanning -> awaiting-finalizer -> finalizing -> complete
                              \-> failed/retryable

live-capturing -> repair-required -> live-ready -> live-finalizing
                                               -> packaged (compact reader core)
                                               -> canonical-complete
```

ETA is derived from observed progress, not a fixed duration per epoch.
Historical builders emit progress every three seconds with slot range,
blocks/second, percentage, and phase ETA. Live ETA uses the atomic live progress
snapshot and a rolling rate, with journal-tail growth as fallback. The aggregate
backlog should be weighted by input size or observed work; early and modern
epochs differ too much for a simple epoch count.

Bind to `127.0.0.1` unless the dashboard must be reachable directly on the LAN.
If binding to `0.0.0.0`, restrict port 8787 with the NAS firewall or a trusted
reverse proxy. Keep execution controls disabled unless authenticated; realtime
read access can expose filesystem paths and operational state.

### Authenticated controls

Set `HIVEZILLA_CONTROL_TOKEN` to a long random value in the private environment
file. Mutating API requests use:

```text
Authorization: Bearer <HIVEZILLA_CONTROL_TOKEN>
```

For example:

```bash
curl --fail --request POST \
  --header "Authorization: Bearer $HIVEZILLA_CONTROL_TOKEN" \
  http://127.0.0.1:8787/api/v1/control/pause
```

Controls are disabled entirely in observer mode. In execute mode they remain
disabled when no control token is configured, unless the operator deliberately
starts Hivezilla with its explicit unauthenticated-controls override.

The scheduler and job controls have deliberately different effects:

- scheduler pause stops launching new scans or a new finalizer; it does not
  interrupt children already running;
- scheduler resume allows the normal four-scan/one-finalizer policy to continue;
- job pause sends `SIGSTOP` to that managed scanner/finalizer, retaining its
  memory and file descriptors;
- job resume sends `SIGCONT` to the same managed child;
- retry is accepted only for a failed, pipeline-owned item after re-reconciliation;
  a merely blocked external/ambiguous item requires operator resolution first.

Control requests have no body. For job routes, `kind` is one of
`historical_scan`, `historical_finalizer`, or `live_finalizer`. Historical job
IDs are epoch numbers; live IDs are capture-directory basenames. The status
snapshot's `capabilities` and `scheduler` fields tell the UI which actions are
currently available and whether scheduling is paused.

`SIGSTOP` is not a memory-release mechanism. A paused full-epoch scan still
occupies RAM, so use scheduler pause when the goal is to drain work safely. Do
not pause live capture through the compaction controls.

A safe retry never deletes input or overwrites ambiguous output. If a failed
pre-marker historical scan left partial files, Hivezilla moves that directory
under:

```text
/volume1/@home/ach/dev/blockzilla-v2/.pipeline-quarantine/
```

The retry then starts with a fresh canonical `epoch-N` output directory. CAR
files and live capture directories are immutable sources from the pipeline's
perspective and are never removed by pause, resume, or retry. A durable
first-seen scan marker is finalized/retried in place instead of quarantined.

## Build the package

From a clean build environment with Rust, Node.js, and npm installed:

```bash
scripts/package-nas-pipeline.sh 2026.07.12-1
```

The script performs locked dependency builds and refuses to overwrite an
existing version. It creates:

```text
dist/nas-pipeline/blockzilla-nas-pipeline-2026.07.12-1/
  bin/blockzilla
  bin/blockzilla-live-producer
  bin/hivezilla
  ui/
  etc/nas-pipeline.env.example
  docs/nas-compaction-pipeline.md
  run-nas-pipeline.sh
  VERSION
  GIT_REVISION
  BUILD_PLATFORM
  SHA256SUMS

dist/nas-pipeline/blockzilla-nas-pipeline-2026.07.12-1.tar.gz
dist/nas-pipeline/blockzilla-nas-pipeline-2026.07.12-1.tar.gz.sha256
```

The package includes the exact `blockzilla` and `blockzilla-live-producer`
binaries built with the dashboard/control plane. This is important because the
progress snapshots, low-memory first-seen path, and live publication behavior
must match what Hivezilla expects. Packaging does not replace any production
binary in place: the wrapper defaults to its sibling `bin/blockzilla`, and an
absolute `BLOCKZILLA_BIN` remains an explicit rollback override.

The binaries are native to the packaging host. Build the deployable release on
Linux/x86_64 (the NAS itself or an equivalent build worker); a package produced
on macOS/arm64 is for local validation only. `BUILD_PLATFORM` records the host
tuple and the wrapper refuses to start when `uname` does not match, preventing
an incompatible release from reaching execute mode.

The included live-producer binary is staged for the next supervisor cutover;
Hivezilla does not replace or restart the active live capture. Update
`BLOCKZILLA_LIVE_BIN` in the live-supervisor environment only as a separate,
reviewed, reversible operation at a safe capture boundary.

## Deploy on the NAS

Copy the tarball to a release staging directory, then extract it as a new,
immutable version:

```bash
release_root=/home/ach/dev/blockzilla-pipeline/releases
config_root=/home/ach/dev/blockzilla-pipeline/config
version=2026.07.12-1
mkdir -p "$release_root" "$config_root"
sha256sum -c "blockzilla-nas-pipeline-$version.tar.gz.sha256"
tar -xzf "blockzilla-nas-pipeline-$version.tar.gz" -C "$release_root"
cd "$release_root/blockzilla-nas-pipeline-$version"
sha256sum -c SHA256SUMS
if [[ ! -e "$config_root/nas-pipeline.env" ]]; then
  install -m 0600 etc/nas-pipeline.env.example "$config_root/nas-pipeline.env"
fi
```

Edit `/home/ach/dev/blockzilla-pipeline/config/nas-pipeline.env`. Leave
`BLOCKZILLA_BIN` unset to use the packaged, matching compactor. To roll back only
the compactor intentionally, set an absolute override such as:

```text
/home/ach/dev/blockzilla-v1-registry-mphf-20260616/target/release/blockzilla.lowmem-final-20260712T2155
```

Start the first rollout in monitor-only mode, with a narrow epoch range if
desired:

```bash
HIVEZILLA_START_EPOCH=870 \
HIVEZILLA_END_EPOCH=873 \
HIVEZILLA_ENV_FILE=/home/ach/dev/blockzilla-pipeline/config/nas-pipeline.env \
./run-nas-pipeline.sh
```

Verify the API and dashboard before enabling execution:

```bash
curl --fail http://127.0.0.1:8787/healthz
curl --fail http://127.0.0.1:8787/api/v1/status
```

Review that completed, partial, live, and repair-required epochs are classified
correctly. Then set `HIVEZILLA_EXECUTE=1` in the private environment file and
restart the service. The package's example configuration and wrapper default to
monitor-only; `--execute` is never implied by binding the API.

For a background launch on the current NAS shell setup:

```bash
state_root=/home/ach/dev/blockzilla-v1-registry-mphf-20260616/logs/nas-compaction-pipeline
mkdir -p "$state_root"
HIVEZILLA_ENV_FILE=/home/ach/dev/blockzilla-pipeline/config/nas-pipeline.env \
  nohup ./run-nas-pipeline.sh >"$state_root/supervisor.out" 2>&1 &
printf '%s\n' "$!" >"$state_root/supervisor.pid"
```

A system or NAS service manager is preferable when available because it gives
clean restart policy and log rotation. Never store provider tokens in the
versioned release directory; use the private environment file with restrictive
permissions.

## Rollback

Releases are immutable, so rollback is a process switch rather than a file
rewrite:

1. set `HIVEZILLA_EXECUTE=0` and stop scheduling new jobs;
2. allow active scanners/finalizer to reach a durable boundary, or terminate
   only the Hivezilla supervisor if child ownership and signal behavior have
   been verified;
3. do not stop the independent live capture process;
4. start the previous release in monitor-only mode against the same roots;
5. verify `/healthz`, `/api/v1/status`, and epoch classifications;
6. re-enable execution only after confirming there is one scheduler.

Do not delete partial output during rollback. A surviving first-seen scan marker
is recoverable and auditable; the exclusive finalizer can safely retry it.
Quarantine ambiguous pre-marker output to a distinct path before rebuilding
rather than mutating or deleting it while diagnosing a failure.

Keep these rollback assets until the new version has completed real epochs:

- the prior Hivezilla release directory and tarball;
- the prior validated `blockzilla` binary;
- pipeline state/events and per-epoch logs;
- live capture journals and repair artifacts.

Before restarting, check that no second scheduler or finalizer remains:

```bash
ps aux | grep -E 'hivezilla pipeline|build-archive-v2-hot-blocks|capture-grpc' | grep -v grep
```

At most four historical scans, one independent live capture, and no more than
one exclusive finalizer should be visible.
