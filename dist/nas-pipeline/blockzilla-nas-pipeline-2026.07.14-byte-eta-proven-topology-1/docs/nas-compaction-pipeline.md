# NAS compaction pipeline

This runbook packages and operates the NAS-local Hivezilla control plane for
historical CAR compaction and live epoch packaging. The production resource
model is deliberately simple:

- up to four low-memory first-seen CAR scans may run concurrently;
- independent legacy registry-reuse ranges use an uncapped one-lane-at-a-time
  throughput tuner inside dynamic CPU, I/O, memory, and disk guards;
- exactly one memory-heavy finalizer may run, with no scan overlapping it;
- historical work is phase ordered: inventory, bounded CAR acquisition/preflight,
  the complete runnable scan sweep, then historical finalization;
- live finalization is a restartable registry-merge, MPHF-build, and hot-rewrite task pipeline;
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
| Matching `state/preflight/epoch-N.json` receipt | The full CAR stream passed bounded structural preflight and its PoH/shredding coverage was recorded |
| `.downloads/epoch-N.car[.zst].part` | Resumable acquisition is incomplete; this name is never accepted as canonical input |
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

Inventory is a scheduling barrier, not merely a dashboard refresh. Hivezilla
must successfully enumerate the CAR, archive, and live roots before starting
new work. A missing or failed epoch is retained as a visible terminal gap so it
does not freeze the corpus forever, while queued or active acquisition,
preflight, and scan tasks keep historical finalization deferred.

## CAR acquisition and structural preflight

The low-memory preflight command consumes a CAR to clean EOF before a new
historical scan is admitted:

```bash
blockzilla preflight-car /volume1/blockzilla/epoch-N.car.zst \
  --epoch N \
  --receipt /path/to/state/preflight/epoch-N.json \
  --io-buffer-mib 8 \
  --progress-json /path/to/state/progress/epoch-N-preflight.json
```

It uses one reusable block group, skips transaction payload allocation, and
keeps an epoch slot bitmap of about 54 KiB. The receipt binds the result to the
source byte length and nanosecond-resolution modification time, records
duplicate/out-of-epoch/non-monotonic slots, and summarizes PoH entries,
transaction references, hash counts, shredding spans, and missing coverage.
Publication is an atomic rename after the receipt and its parent directory are
synced.

A stable sibling receipt lock serializes duplicate or restarted preflights.
After acquiring it, a worker reuses an exact eligible v1 receipt whose epoch
and size/mtime fingerprint still match, so a controller restart does not
reread a multi-hundred-gigabyte CAR. Receipt reads are capped at 1 MiB.

This receipt is deliberately labelled `validation_level=structural`. It checks
CAR framing, block/entry grouping, a clean decompressed EOF, and source
stability during the pass; it does not recompute every CAR CID or prove a
trusted SHA-256 digest. A deep cryptographic/content verifier can be scheduled
separately when a trusted digest is available, without making its larger memory
footprint the NAS default.

Optional downloads are resumable and bounded to one task by default. They are
written only under `CAR_ROOT/.downloads/*.part`, use `aria2c` with `wget` as the
NAS fallback, run the same structural preflight, and are renamed atomically to
`epoch-N.car` or `epoch-N.car.zst` only after success. Automatic acquisition is
disabled unless a URL template and explicit inclusive epoch bounds are both
configured. This guard prevents an unbounded inventory from accidentally
requesting hundreds of historical CARs:

```text
HIVEZILLA_DOWNLOAD_CONCURRENCY=1
HIVEZILLA_PREFLIGHT_CAR=1
HIVEZILLA_START_EPOCH=864
HIVEZILLA_END_EPOCH=864
HIVEZILLA_CAR_SOURCE_URL_TEMPLATE=https://files.old-faithful.net/{epoch}/epoch-{epoch}.car
```

Leave `HIVEZILLA_CAR_SOURCE_URL_TEMPLATE` unset when Hivezilla should only use
CARs already present on the NAS. `HIVEZILLA_PREFLIGHT_CAR=1` is independently
useful for structurally checking existing canonical CARs before their first
compact scan.

Hivezilla measures the CAR and archive filesystems independently. New downloads
stop at the configured disk reserve, and each admitted preflight is budgeted at
2,304 MiB: 2 GiB for the maximum accepted zstd long window plus decoder/process
overhead. The normal working set is usually much smaller. Canonical publication
is no-clobber: if either
canonical CAR suffix appears concurrently, the partial is preserved for
operator review instead of overwriting it.

## Historical scheduling

Historical scheduling uses a corpus-wide barrier for every inventory
generation:

1. enumerate and classify every in-scope epoch;
2. finish or terminally classify every required download and CAR preflight;
3. continuously refill the admitted scan lanes until every runnable epoch is
   scan-ready, complete, failed, or definitively unavailable;
4. only then admit historical finalizers, one at a time.

An already scan-ready or complete archive is not retroactively preflighted: its
durable scan result is authoritative, and rereading a removed or multi-hundred-
gigabyte source cannot improve it. A newly discovered runnable CAR opens a new
sweep generation after any currently publishing finalizer exits.

Source CAR retirement is expected after a committed compact archive. Hivezilla
also recognizes the previous no-access model when the full reader core is
committed, the output has no pipeline ownership marker, and both block-access
sidecars are wholly absent. That narrow compatibility layout is complete; a
pipeline-owned output, a partial access pair, or a registry-only directory is
still incomplete. The dashboard renders its deleted source as `source retired`
and its absent access pair as `legacy no-access`, not as a missing-source error.

A closed live capture carrying explicit `READY-TO-PACKAGE` approval retains a
narrow priority exception between historical tasks. It never overlaps a CAR
acquisition or historical scan, but it may use the exclusive finalizer lane
before the historical sweep is complete so live capture storage cannot grow
unbounded behind a long backfill.

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

### Legacy registry-reuse workers

An epoch with a validated legacy registry, counts, MPHF, blockhash registry,
CAR source, and predecessor tail can bypass the first-seen scan/finalizer pair.
Hivezilla streams the compressed CAR once and reuses those sidecars directly:

```bash
blockzilla build-archive-v2-hot-blocks \
  /volume1/blockzilla/epoch-N.car.zst \
  /volume1/@home/ach/dev/blockzilla-v2/epoch-N \
  --registry-dir /volume1/@home/ach/dev/blockzilla-v2/epoch-N \
  --previous-car /volume1/blockzilla/epoch-PREV.car.zst \
  --resume --no-access --level 1
```

Independent contiguous range heads may run together. With
`--legacy-compact-concurrency 0`, there is no numeric lane ceiling. The tuner
measures aggregate useful MiB/s, establishes a median baseline, admits one
additional lane, and accepts it only when the aggregate gain is material. A
failed probe is paused and measured again before the tuner backs off, so it can
distinguish normal noise from real device contention. A positive concurrency
value remains available as an explicit compatibility cap.

CPU load and physical archive-device throughput are global guards; the legacy
per-worker CPU/I/O estimates are displayed for compatibility and are never
divided into a worker count. Candidate-specific memory and free-disk admission
can still withhold a requested probe. Compact/reuse always runs with
`--no-access`, so its memory reservation charges the heap-resident MPHF and
blockhash registry, a 512 MiB process/buffer allowance, and a 1 GiB minimum.
It does not incorrectly charge the disk-backed registry and counts files as
anonymous RSS. Conservative admission reserves each lane's possible future
growth. Once the adaptive tuner has a settled topology, a one-lane probe instead
uses the observed peak-to-current rebound of the running lanes plus the hard
memory guard; this avoids double-counting growth already represented by the
guard. A paused worker continues to count because `SIGSTOP` does not release
RSS. If hard pressure arrives during a probe, the probe is paused before the
accepted baseline lanes.

`--legacy-compact-auto-pause` adds a pressure controller for managed legacy
workers. Linux PSI `io full avg10` and `MemAvailable` are sampled every scheduler
poll. The controller:

- pauses one newest managed worker per reconciliation pass when I/O PSI reaches
  the pause threshold, CPU load reaches its ceiling, live capture stops
  advancing, or available memory falls below
  `memory reserve + memory guard`; hard pressure bypasses the normal cooldown;
- resumes at most one worker per cooldown only after I/O PSI falls to the lower
  resume threshold and available memory reaches
  `memory reserve + 2 × memory guard`, CPU load has recovered, and live capture
  is healthy;
- treats `--legacy-compact-min-running` as a bootstrap/healthy-state target,
  never as permission to violate a hard guard;
- probes only one additional lane at a time after recovery;
- treats missing PSI telemetry as unknown rather than as a blocker;
- keeps manual pause state separate and never automatically resumes a manually
  paused lane.

Managed legacy children are isolated process-group leaders, so stop/continue
signals cover the complete worker group. Workers inherited from the previous
controller are validated and counted, but are never auto-signalled because the
old release did not isolate their process groups. Their completion is accepted
only when the original PID disappears and the ownership PID, complete progress
record, and reader core all agree. Automatic pause intent is persisted before
`SIGSTOP`; startup safely sends an idempotent `SIGCONT` to an exactly matched
persisted worker so a controller restart cannot leave it silently frozen.

The current 7.5 GiB NAS profile is:

```text
legacy numeric concurrency ceiling  none (configured value 0)
CPU load ceiling                    12; never converted to lane slots
physical device throughput ceiling  none (0 MiB/s); measured for tuning
adaptive tuner / pressure pause     enabled
bootstrap healthy-state target      2 lanes
per-candidate memory floor          1,024 MiB
free-memory reserve                 1,536 MiB
adaptive memory guard               512 MiB
I/O PSI full avg10 pause / resume   40% / 25%
baseline settle / probe cooldown    180 / 60 seconds
rejected-probe backoff               300 seconds
```

`SIGSTOP` arrests CPU, I/O, and further allocation; it does not release RSS.
Memory therefore remains a hard admission constraint, while adaptive pause is
primarily an I/O safety valve and a way to stop an allocating worker from making
an already-low-memory condition worse. The status API and dashboard expose the
running/manual-paused/auto-paused counts, current PSI, thresholds, reason, and
last automatic action.

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
scan concurrency        2
projected RSS per scan  1,024 MiB
finalizer RSS floor     512 MiB, increased from registry/index size per stage
free-memory reserve     1,024 MiB after projected scan admission
free-disk reserve       256 GiB before starting new work
historical finalizers   1, exclusive
live finalizers         1, in the same exclusive lane
live capture            always reserved; never stopped to make room for scans
```

The two-lane setting is deliberate for the 7.5 GiB NAS, where swap is already
in use. Full epochs have larger registries than short proofs and must retain
headroom for live capture, the OS, and filesystem cache. Admission may run fewer
than two scans when `MemAvailable` falls.

The controller admits a finalizer stage only when `MemAvailable` is at least
the configured reserve plus that stage's estimate. Registry merge uses the
configured floor. MPHF build uses the larger of the floor and roughly four
times `registry.bin` plus construction overhead. Hot rewrite uses the larger of
the floor and the persisted MPHF size plus bounded rewrite overhead. Set the
floor with `--finalizer-memory-mib` or `HIVEZILLA_FINALIZER_MEMORY_MIB`; a
queued stage stays visible with an admission-blocked reason instead of risking
swap collapse.

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
7. run `prepare-archive-v2-live-registry`, publishing its marker last;
8. run `build-archive-v2-registry-index` in a fresh process so MPHF construction memory is released;
9. run `build-archive-v2-hot-blocks-from-live --registry-source runs`, reusing the prepared registry and MPHF;
10. require a successful child exit and validate final sidecars before classifying it complete;
11. preserve repair provenance in the final output.

The finalizer task sequence is structurally:

```bash
blockzilla prepare-archive-v2-live-registry \
  /volume1/@home/ach/dev/blockzilla-live/epoch-N-capture-TIMESTAMP \
  /volume1/@home/ach/dev/blockzilla-v2/epoch-N

blockzilla build-archive-v2-registry-index \
  /volume1/@home/ach/dev/blockzilla-v2/epoch-N/registry.bin \
  --output /volume1/@home/ach/dev/blockzilla-v2/epoch-N/registry.mphf

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

An atomically renamed repair union carrying a valid root
`REPAIR-REQUIRED.json` is a separate degraded-compaction input. Status schema
version 3 reports it as `repair_required` and lists its retained source capture
IDs. A closed same-epoch source named by that marker is marked `superseded_by`
the bundle, so it is neither queued nor counted as a second blocker. Active,
packaging, repair-gated, and cross-epoch sources are never hidden; this matters
when one retained capture also contains the following epoch's tail. Hidden
`prepare-epoch-repair` staging directories are ignored until their final atomic
rename.

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

The version 3 status snapshot reports inventory and scan-sweep gates,
historical epoch states, active task lanes, exclusive-finalizer state and
queue, live capture progress, throughput/ETA, host memory/storage observations,
and semantic artifact state for every epoch. Artifact state distinguishes
missing, pending, building, candidate, present, structurally verified, invalid,
and not-applicable evidence for the CAR, preflight receipt, source PoH/shred
coverage, scan marker, registry family, blocks/indexes, PoH, shredding,
signatures, vote hashes, and optional access sidecars. The epoch visualization
should keep these states distinct:

```text
unavailable -> queued -> scanning -> awaiting-finalizer -> finalizing -> complete
                              \-> failed/retryable

live-capturing -> repair-required -> live-ready -> live-finalizing
                                               -> packaged (compact reader core)
                                               -> canonical-complete
```

The runnable historical queue ETA is strictly remaining readable source bytes
divided by aggregate measured source-read bytes per second. On Linux, Hivezilla
matches each worker's CAR file descriptor by process start identity and file
device/inode, samples its `fdinfo` offset, and smooths the aggregate rate over a
rolling window. Queued work contributes full CAR size; active or paused work
contributes `source size - current offset`. Worker count, decoded block rate,
task duration, and `/proc` process-I/O counters never scale this ETA. Sampling is
relearned after worker identity, topology, pause, or resume changes, and the UI
reports an unavailable ETA until all active readers have compatible source-byte
coverage. Action-required epochs and queued epochs without a known source size
are explicitly excluded.

Historical builders still emit slot range, block rate, percentage, and phase
progress for individual task visibility. Live ETA is separate: it uses the
atomic live progress snapshot and a rolling slot rate, with journal-tail growth
as fallback.

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

Control requests have no body. Task kinds exposed by active lanes include
`car_download`, `car_preflight`, `historical_scan`, `historical_finalizer`, and
`live_finalizer`. Historical/acquisition task IDs are epoch numbers; live IDs
are capture-directory basenames. The status snapshot's `capabilities` and
`scheduler` fields tell the UI which actions are currently available and
whether scheduling is paused.

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

Live-stage retry is also in place: a valid prepared-registry marker and a
completed MPHF are retained, and the controller resumes at the first missing
stage. Unowned or generation-inconsistent output remains blocked for operator
review.

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

When replacing an already-running scheduler, start the first rollout in
monitor-only mode on a different port and a different state root. Observer mode
still publishes `status.json`, so sharing the production state root would make
the two controllers overwrite each other's view. Invoke the binary directly so
there is no chance of inheriting `HIVEZILLA_EXECUTE=1` from the production
environment:

```bash
release=/home/ach/dev/blockzilla-pipeline/releases/blockzilla-nas-pipeline-$version
shadow_state=/home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2-shadow
mkdir -p "$shadow_state"
"$release/bin/hivezilla" pipeline \
  --bind 0.0.0.0:8788 \
  --blockzilla-bin "$release/bin/blockzilla" \
  --car-root /volume1/blockzilla \
  --archive-root /volume1/@home/ach/dev/blockzilla-v2 \
  --live-root /volume1/@home/ach/dev/blockzilla-live \
  --state-root "$shadow_state" \
  --scan-concurrency 2 \
  --legacy-compact-concurrency 0 \
  --legacy-compact-finalizer-overlap 0 \
  --legacy-compact-cpu-cores-per-worker 1 \
  --legacy-compact-cpu-budget-cores 12 \
  --legacy-compact-io-mib-per-sec-per-worker 120 \
  --legacy-compact-io-budget-mib-per-sec 0 \
  --legacy-compact-auto-pause \
  --legacy-compact-min-running 2 \
  --legacy-compact-memory-guard-mib 512 \
  --legacy-compact-io-pause-full-avg10 40 \
  --legacy-compact-io-resume-full-avg10 25 \
  --legacy-compact-pause-cooldown-secs 60 \
  --scan-memory-mib 1024 \
  --finalizer-memory-mib 512 \
  --memory-reserve-mib 1536 \
  --disk-reserve-gib 256 \
  --download-concurrency 1 \
  --start-epoch 0 \
  --end-epoch 999 \
  --priority-epoch-start 863 \
  --priority-epoch-end 899 \
  --ui-dir "$release/ui"
```

`--legacy-compact-concurrency 0` is adaptive mode, not zero workers. It has no
numeric lane ceiling: the controller establishes a baseline, probes one extra
lane, verifies a rejected probe with a paired pause, and repeats while aggregate
throughput improves. CPU load, memory headroom, free space, I/O PSI, live-slot
advancement, and marginal throughput are runtime guards. The legacy per-worker
CPU/I/O estimates remain visible for compatibility but are never divided into a
fixed worker count. A zero finalizer-overlap value is likewise adaptive in this
mode. Accepted lane counts and their throughput baselines are persisted per
scheduler context, so a restart or a switch between bulk and finalizer-overlap
work does not collapse a proven topology back to one lane.

Verify the API and dashboard before enabling execution:

```bash
wget -qO- http://127.0.0.1:8788/healthz
wget -qO- http://127.0.0.1:8788/api/v1/status
```

Review that completed, partial, live, and repair-required epochs are classified
correctly. Pause the old scheduler, let its active lanes drain, stop that exact
Hivezilla PID, stop the shadow observer, and only then start the new release on
`0.0.0.0:8787` with `HIVEZILLA_EXECUTE=1`. Keep the independent live producer
running throughout. The package's example configuration and wrapper default to
monitor-only; `--execute` is never implied by binding the API.

For a background launch on the current NAS shell setup:

```bash
state_root=/home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2
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
