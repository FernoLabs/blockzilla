# Current Hivezilla shred hardening rollout

Status: **operator-reviewed rollout for the integrated Hivezilla shred runtime**.

The `blockzilla-hivezilla-shred` image contains the current `hivezilla` binary. That one binary
runs `serve-shred-reader`, `record-shred-udp`, and `serve-shred-spool-pull-source`; there is no
standalone shred-reader image or deployment in this rollout. The separate
`blockzilla-hivezilla-shred-status` image runs the Rust `serve-shred-status` command.

This procedure preserves capture while introducing the hardened receiver and pull source. It does
not authorize a live read-write retention rollout.

## Non-negotiable boundaries

- Publish an exact commit from `main`, then deploy only the digest recorded by the publishing
  workflow. A SHA tag is an index into GHCR, not deployment authority.
- Keep recorder, reader identity, spool, status, and ACK-control state on persistent volumes. Never
  initialize an empty ACK WAL during a source upgrade.
- Only one pull source may own a cumulative-ACK WAL. Source handoff is sequential even when reader
  and recorder handoff use an overlap.
- The first pull-source candidate uses `gc.enabled=false` and a read-only `/data` mount. It may
  advance the protected ACK WAL, but it cannot retire source data.
- The only checked read-write GC artifact is a default-off, one-retirement canary against frozen
  clone volumes. It is never pointed at the live spool.
- Do not restart the recorder's UDP input without an overlap path. UDP send success alone is not a
  durable receipt.

## Candidate gates

Require all of the following before touching a live process:

1. `Current Hivezilla shred Linux gate` is green for the exact candidate SHA. The gate builds both
   runtime images on Linux, checks dynamic libraries, verifies UID/GID `10001`, validates the
   source-trial and clone-only Compose contracts, and rejects mutable publishing tags/actions. It
   also tests the read-only `shred-epoch-audit`/`repair-wal-inspect` image; that diagnostic image is
   not part of the live publisher.
2. The reviewed SHA is reachable from `main` and the manual publisher produced
   `published-image-digests.json` for it.
3. The target host reports the `aes` CPU feature used by the explicit amd64 build floor:

   ```sh
   grep -qw aes /proc/cpuinfo
   ```

4. A frozen raw-only reconstruction control reproduces its manifest and hashes. Any repair-assisted
   audit uses a separately frozen repair-WAL prefix and leaves that prefix unchanged.
5. Free space covers the configured reserve plus measured spool growth for the complete GC-off
   trial and rollback window, plus two segment targets. Abort if that cannot be proved.
6. The exact live data, status, and control volume names and the current stream tuple are recorded.
   Record the ACK WAL and lock device/inode, owner, mode, size, and SHA-256 before handoff.

## Publish immutable images

The manual
[`publish-current-shred-images.yml`](../../.github/workflows/publish-current-shred-images.yml)
workflow is the release boundary. GitHub exposes `workflow_dispatch` only from a workflow already
present on the default branch. Select the workflow definition from `main` and enter the exact
lowercase 40-character reviewed commit in `revision`.

Both package-write jobs target the `hivezilla-shred-release` GitHub environment. Configure that
environment in repository settings before the first dispatch: require a reviewer who is not the
dispatcher, restrict deployment branches to the protected default branch, and prevent
self-approval. The workflow also fails unless `GITHUB_REF` is exactly the repository's reported
default branch; selecting a feature-branch workflow definition is not a release path.

Each build checks out that SHA, verifies that it is an ancestor of the current default branch, uses
only a job-scoped `GITHUB_TOKEN`, and publishes one SHA tag for each integrated runtime:

- `ghcr.io/fernolabs/blockzilla-hivezilla-shred:<revision>`;
- `ghcr.io/fernolabs/blockzilla-hivezilla-shred-status:<revision>`.

The final job validates both `sha256:` values and uploads
`current-hivezilla-shred-image-digests-<revision>`. Preserve that artifact with the rollout record.
Copy its digest values, not its tags, into deployment configuration. If GHCR access is private,
configure a host-level credential with only `read:packages`; never put it in Compose environment
variables or source-controlled files.

## Prepare the host

Install
[`90-hivezilla-shred-sockets.conf`](../../services/hivezilla/host/90-hivezilla-shred-sockets.conf)
as `/etc/sysctl.d/90-hivezilla-shred-sockets.conf`, apply the sysctl configuration, and verify:

```sh
sysctl net.core.rmem_max net.core.wmem_max
```

Both values must be `134217728`. The asset intentionally leaves `rmem_default` and
`net.core.netdev_max_backlog` unchanged. A running socket does not resize when the ceiling changes;
verify the effective buffer after starting the candidate.

The integrated receiver deployment must also carry the bounded runtime settings from
[`shred-reader.env.example`](../../services/hivezilla/config/shred-reader.env.example):

- `REPAIR_UDP_RECV_BUFFER_BYTES=67108864` for the continuously drained repair socket;
- `REPAIR_RESPONSE_QUEUE_CAPACITY=4096` for staged responses; and
- a cutover-specific, nonzero `SHRED_FORWARD_BIND_ADDR` such as `127.0.0.1:18104` whenever
  `SHRED_FORWARD_ADDRS` is configured. The bind and every target must use the same address family.

Do not reuse one fixed forwarding source address for blue, green, and final receiver processes;
source-specific post-fsync evidence depends on those addresses remaining distinct.
The checked ingest example reserves `blue=127.0.0.1:18004`, `green=127.0.0.1:18104`, and
`final=127.0.0.1:18204`. Change the private recorder allowlist and the corresponding receiver
binding as one reviewed unit.

Prepare private regular files for the pull-source config, server certificate/key, client CA,
allowed-node list, and trusted NAS receipt public key. Do not use symlinks. Replace
`__SHRED_JOURNAL_ID__` in a private copy of
[`raw-shred-pull-source.example.json`](../../services/hivezilla/config/raw-shred-pull-source.example.json)
with the incumbent journal ID and keep `gc.enabled=false`.

Resolve the three live external volumes rather than guessing their names:

- recorder spool mounted at `/data`;
- recorder status mounted at `/status`;
- pull-source ACK state mounted at `/control`.

If the incumbent uses a host directory, create an external bind-backed local volume whose device
is that exact canonical directory. Inspect the volume's `Options.device` and compare the WAL and
lock from the host and a one-shot container. Mapping the same directory is acceptable; copying its
contents into a new named volume is not.

### Protected ACK control namespace

On Linux, the source requires the dedicated control directory to be `root:root` mode `0700`. The
cumulative ACK WAL and its stable `${WAL}.lock` must both be one-link, non-symlink `root:root` mode
`0600` regular files. Every ancestor through `/` must be a root-owned, non-symlink directory with
owner `rwx` and no group/world write bit. Run these read-only checks only after fencing the
incumbent source; the two `flock` probes prove both descriptors are released:

```bash
set -euo pipefail
: "${CONTROL_DIR:?set the absolute dedicated ACK control directory}"
: "${ACK_WAL:?set the absolute cumulative ACK WAL path}"
test "$(dirname "$ACK_WAL")" = "$CONTROL_DIR"
ACK_LOCK="${ACK_WAL}.lock"

test ! -L "$CONTROL_DIR"
test "$(stat -Lc '%u:%g %a %F' "$CONTROL_DIR")" = '0:0 700 directory'
for file in "$ACK_WAL" "$ACK_LOCK"; do
  test ! -L "$file"
  test "$(stat -Lc '%u:%g %a %h %F' "$file")" = '0:0 600 1 regular file'
done

path=$CONTROL_DIR
while :; do
  test -d "$path" && test ! -L "$path"
  test "$(stat -Lc %u "$path")" = 0
  mode="$(stat -Lc %a "$path")"
  permissions=$((8#$mode))
  (( (permissions & 0700) == 0700 ))
  (( (permissions & 0022) == 0 ))
  test "$path" = / && break
  path="$(dirname "$path")"
done

ack_name="$(basename "$ACK_WAL")"
test ! -e "$CONTROL_DIR/.${ack_name}.compact.tmp"
flock --exclusive --nonblock "$ACK_WAL" true
flock --exclusive --nonblock "$ACK_LOCK" true
stat -Lc '%d:%i %u:%g %a %s %h %F %n' "$ACK_WAL" "$ACK_LOCK"
sha256sum "$ACK_WAL" "$ACK_LOCK"
```

Do not run a creation, ownership, or mode command against the live namespace as part of this
check. Record any mismatch and restore the incumbent unchanged; never copy or recreate its WAL.

### Destructive GC namespace

The clone-only canary additionally requires the spool root and its cluster, origin, and source
directories to be `root:10001` mode `0750`; the exact journal leaf must be `root:10001` mode
`03770` (setgid + sticky). The precreated `.retention.lock` and optional
`.retired-prefix.v1.json` must be one-link, non-symlink `root:10001` mode `0640` regular files.
Ancestors above the spool root must be root-owned and traversable by recorder GID `10001` (or by
other-execute), with replacement blocked by no group/world write bit or a sticky directory.

Verify the frozen canary namespace before the first read-write start:

```bash
set -euo pipefail
: "${SPOOL_ROOT:?set the absolute canary spool root}"
: "${GC_CANARY_JOURNAL_ID:?set the distinct 32-hex canary journal ID}"
CLUSTER=solana-mainnet
ORIGIN=hivezilla-shred-gc-canary
SOURCE=shred-reader-gc-canary
JOURNAL_DIR="$SPOOL_ROOT/$CLUSTER/$ORIGIN/$SOURCE/$GC_CANARY_JOURNAL_ID"

path="$(dirname "$SPOOL_ROOT")"
while :; do
  test -d "$path" && test ! -L "$path"
  test "$(stat -Lc %u "$path")" = 0
  gid="$(stat -Lc %g "$path")"
  mode="$(stat -Lc %a "$path")"
  permissions=$((8#$mode))
  (( (permissions & 0001) != 0 || (gid == 10001 && (permissions & 0010) != 0) ))
  (( (permissions & 0022) == 0 || (permissions & 01000) != 0 ))
  test "$path" = / && break
  path="$(dirname "$path")"
done

for directory in \
  "$SPOOL_ROOT" \
  "$SPOOL_ROOT/$CLUSTER" \
  "$SPOOL_ROOT/$CLUSTER/$ORIGIN" \
  "$SPOOL_ROOT/$CLUSTER/$ORIGIN/$SOURCE"; do
  test ! -L "$directory"
  test "$(stat -Lc '%u:%g %a %F' "$directory")" = '0:10001 750 directory'
done
test ! -L "$JOURNAL_DIR"
test "$(stat -Lc '%u:%g %a %F' "$JOURNAL_DIR")" = '0:10001 3770 directory'

RETENTION_LOCK="$JOURNAL_DIR/.retention.lock"
test ! -L "$RETENTION_LOCK"
test "$(stat -Lc '%u:%g %a %h %F' "$RETENTION_LOCK")" = \
  '0:10001 640 1 regular file'
MARKER="$JOURNAL_DIR/.retired-prefix.v1.json"
if test -e "$MARKER"; then
  test ! -L "$MARKER"
  test "$(stat -Lc '%u:%g %a %h %F' "$MARKER")" = \
    '0:10001 640 1 regular file'
fi
flock --exclusive --nonblock "$RETENTION_LOCK" true
stat -Lc '%d:%i %u:%g %a %s %h %F %n' "$RETENTION_LOCK"
test ! -e "$MARKER" || stat -Lc '%d:%i %u:%g %a %s %h %F %n' "$MARKER"
```

These are exact recommended modes, stricter than some ancestor traversal combinations accepted by
the runtime. Apply ownership/mode preparation only to stopped, disposable canary volumes; never to
the live recorder tree.

## Sequential non-deleting source trial

The checked
[`docker-compose.hivezilla-shred-pull-source.yml`](../../docker-compose.hivezilla-shred-pull-source.yml)
runs as control UID `0`, adds only recorder GID `10001`, drops every capability except
`DAC_READ_SEARCH`, mounts the root filesystem read-only, and mounts the live spool and recorder
status read-only. `/control` remains writable so a verified ACK can cross its fsync boundary.

Before starting it:

1. Stop and fence only the incumbent pull source. Keep the recorder and NAS receiver live.
2. Prove the incumbent released both the WAL and sidecar locks.
3. Recheck the recorded WAL identity and stream tuple. Do not move, copy, truncate, or recreate the
   WAL or lock.
4. Set the required `HIVEZILLA_SHRED_*_VOLUME`, config, credential-path, and image-digest variables.
   Every bind-mounted path should be absolute, or explicitly start with `./` when rendering locally.
5. Render the model with a unique project name and inspect it before `up`:

   ```sh
   docker compose \
     --project-name hivezilla-shred-source-trial \
     --file docker-compose.hivezilla-shred-pull-source.yml \
     config
   ```

Verify the digest-qualified image, exact external volume names, `/data:ro`, `/status:ro`, and the
expected `/control` mount. Start the source, allow port `18443` only from the NAS egress address,
and confirm that its first offered record is exactly durable ACK + 1. Then confirm the NAS durable
cursor and the source's post-fsync ACK status advance together.

The trial deliberately disables deletion. Watch measured spool growth and stop inside the approved
disk-runway window. If startup preflight, stream identity, ACK signature, exact retained anchor, or
successor validation fails, stop the candidate and restart the unchanged incumbent. Never repair a
preflight failure by deleting state.

## Clone-only GC canary

Live GC remains off. Give the canary three distinct, disposable data/status/control clone volumes
and verify that none resolves to a live volume. Do not mount or copy the production ACK WAL into
the canary. Populate the data/status clones by replaying a bounded frozen raw-shred sample through
an offline canary recorder so every stored record has the separate canary origin, source, and
journal identity. Require enough sealed segments to retire one while retaining an ACK anchor and
active successor.

The canary config
[`raw-shred-pull-source-gc-canary.example.json`](../../services/hivezilla/config/raw-shred-pull-source-gc-canary.example.json)
sets `control_uid=0`, `recorder_gid=10001`, and a process-wide retirement budget of one. The
[`GC canary overlay`](../../docker-compose.hivezilla-shred-pull-source-gc-canary.yml) is inert unless
the `gc-canary` profile is explicitly enabled, changes cloned `/data` to read-write, and disables
automatic restart. The budget resets on process restart, so restarting a canary is a new destructive
test and requires a fresh clone. Its server binds only `127.0.0.1:19443`, uses
`hivezilla-shred-gc-canary/shred-reader-gc-canary/__GC_CANARY_JOURNAL_ID__`, writes only
`gc-canary-ack.wal`, and expects primary `blockzilla-gc-canary`; none of those values may equal the
production stream/control identity.

Create canary-only server credentials, client CA, one-node allowlist, and receipt-signing key. Set
the overlay's five required `HIVEZILLA_SHRED_GC_CANARY_*_PATH` variables to those files. The server
certificate must cover a canary-only name, the allowlist must contain exactly one local test client,
and the matching local durable receiver/client must use the exact canary stream tuple and expected
primary ID. Never use the production NAS certificate, allowlist, receipt key, endpoint, or client
configuration. Because the listener is loopback-only, run the controlled mTLS client on the same
host (or in a container using that host network namespace), persist its ACK, observe at most one
retirement, then stop both sides.

After the offline canary recorder is stopped, prepare only the disposable namespace. Replace the
variables with host paths inside the clone volumes; these commands must never resolve to live
paths:

```bash
set -euo pipefail
: "${CANARY_CONTROL_DIR:?set the disposable canary control directory}"
: "${CANARY_SPOOL_ROOT:?set the disposable canary spool root}"
: "${GC_CANARY_JOURNAL_ID:?set a new non-production 32-hex journal ID}"
test "${#GC_CANARY_JOURNAL_ID}" = 32
case "$GC_CANARY_JOURNAL_ID" in
  *[!0-9a-f]*) exit 2 ;;
esac

install -d -o root -g root -m 0700 "$CANARY_CONTROL_DIR"
CANARY_ACK_WAL="$CANARY_CONTROL_DIR/gc-canary-ack.wal"
CANARY_ACK_LOCK="${CANARY_ACK_WAL}.lock"
test ! -e "$CANARY_ACK_WAL" && test ! -e "$CANARY_ACK_LOCK"
install -o root -g root -m 0600 /dev/null "$CANARY_ACK_WAL"
install -o root -g root -m 0600 /dev/null "$CANARY_ACK_LOCK"

CANARY_ORIGIN="$CANARY_SPOOL_ROOT/solana-mainnet/hivezilla-shred-gc-canary"
CANARY_SOURCE="$CANARY_ORIGIN/shred-reader-gc-canary"
CANARY_JOURNAL="$CANARY_SOURCE/$GC_CANARY_JOURNAL_ID"
for directory in \
  "$CANARY_SPOOL_ROOT" \
  "$CANARY_SPOOL_ROOT/solana-mainnet" \
  "$CANARY_ORIGIN" \
  "$CANARY_SOURCE"; do
  chown root:10001 "$directory"
  chmod 0750 "$directory"
done
chown root:10001 "$CANARY_JOURNAL"
chmod 03770 "$CANARY_JOURNAL"
RETENTION_LOCK="$CANARY_JOURNAL/.retention.lock"
test -f "$RETENTION_LOCK" && test ! -L "$RETENTION_LOCK"
chown root:10001 "$RETENTION_LOCK"
chmod 0640 "$RETENTION_LOCK"
RETIRED_PREFIX_MARKER="$CANARY_JOURNAL/.retired-prefix.v1.json"
if test -e "$RETIRED_PREFIX_MARKER"; then
  test -f "$RETIRED_PREFIX_MARKER" && test ! -L "$RETIRED_PREFIX_MARKER"
  chown root:10001 "$RETIRED_PREFIX_MARKER"
  chmod 0640 "$RETIRED_PREFIX_MARKER"
fi
```

Run the read-only control and GC verification blocks above immediately afterward and preserve their
output with the canary evidence.

Before rendering, prove the overlay cannot resolve to a live volume or credential. This comparison
checks both the resolved file identity and content so a copied production credential also fails:

```bash
set -euo pipefail
for suffix in DATA_VOLUME STATUS_VOLUME CONTROL_VOLUME; do
  live_name="HIVEZILLA_SHRED_${suffix}"
  clone_name="HIVEZILLA_SHRED_GC_CANARY_${suffix%_VOLUME}_CLONE_VOLUME"
  : "${!live_name:?set the inspected live volume name}"
  : "${!clone_name:?set the disposable canary clone volume name}"
  test "${!live_name}" != "${!clone_name}"
done

for suffix in \
  SERVER_CERTIFICATE \
  SERVER_PRIVATE_KEY \
  CLIENT_CA \
  ALLOWED_NODES \
  RECEIPT_PUBLIC_KEY; do
  live_name="HIVEZILLA_SHRED_PULL_${suffix}_PATH"
  canary_name="HIVEZILLA_SHRED_GC_CANARY_${suffix}_PATH"
  : "${!live_name:?set the production credential path}"
  : "${!canary_name:?set the canary-only credential path}"
  live_path="$(readlink -e -- "${!live_name}")"
  canary_path="$(readlink -e -- "${!canary_name}")"
  test "$live_path" != "$canary_path"
  test "$(stat -Lc '%d:%i' "$live_path")" != "$(stat -Lc '%d:%i' "$canary_path")"
  test "$(sha256sum "$live_path" | cut -d ' ' -f 1)" != \
    "$(sha256sum "$canary_path" | cut -d ' ' -f 1)"
done
```

Render both files before starting:

```sh
docker compose \
  --project-name hivezilla-shred-gc-canary \
  --profile gc-canary \
  --file docker-compose.hivezilla-shred-pull-source.yml \
  --file docker-compose.hivezilla-shred-pull-source-gc-canary.yml \
  config
```

Require all rendered volume names to be clone names, `restart: "no"`, cloned `/data` to be the only
read-write spool mount, the loopback bind/distinct identities above, and canary-only secret file
paths. Start once, let only the controlled local client cause verified ACKs to advance the
precreated fresh canary ACK WAL,
then stop it. Store before/after manifests and prove:

- no more than one oldest, fully ACK-covered sealed segment retired;
- the active segment, ACK-anchor segment, exact successor, and every unacknowledged record remain;
- the retired-prefix marker and ACK WAL remain fsynced and reopen cleanly;
- the live volumes and their manifests are unchanged.

This result does not authorize live GC. A steady-state read-write source needs a separate review,
unbounded/durable budget semantics, monitoring, disk-runway alerts, and an explicit rollback
boundary.

## Reader and recorder overlap

Run a receiver candidate with the same integrated digest and the `serve-shred-reader` command. It
must use a separate persistent identity/repair-WAL volume, distinct gossip/TVU/metrics ports, and a
fixed `SHRED_FORWARD_BIND_ADDR`. Keep the incumbent receiver live while the candidate joins gossip
and receives current-version TVU traffic. Carry `REPAIR_UDP_RECV_BUFFER_BYTES` and
`REPAIR_RESPONSE_QUEUE_CAPACITY` explicitly in the deployment rather than relying on an old
environment file that predates the bounded repair ingress.

Duplicate shreds during a bounded overlap are safe observations, but they increase UDP, recorder,
disk, and NAS load. Continue only while receiver overflow, forward-queue drops/errors, recorder
socket overflow/backpressure, filesystem reserve, and NAS ACK lag remain healthy.

Do not stop the incumbent based on UDP send success. The recorder must attribute candidate-origin
datagrams after its group fsync boundary and show advancing durable sequence/slot samples for that
source. Read this proof from the recorder's private `durable_sources` status field unless the
deployed Rust status collector explicitly exposes the same bounded fields. If the incumbent
recorder cannot expose source-specific durable evidence, use a reviewed isolated-recorder overlap
or defer the cutover; do not restart the only UDP recorder to gain a metric.

Once candidate-specific durable evidence is stable through a representative busy interval, stop
the incumbent receiver gracefully. Preserve its identity and repair WAL until the post-cutover
audit has been stored. The recorder itself is replaced only through an overlap that always leaves
one proven receiver-to-recorder path live; two writers must never share a spool.

## Acceptance and rollback evidence

For the exact cutover window record:

- source revision and both deployed image digests;
- first/last durable raw sequence and slot for each source address;
- receiver and recorder restart counts;
- TVU/repair socket overflow, forward queue high-water/drop/error, recorder backpressure, and disk
  reserve deltas;
- NAS cumulative ACK and lag;
- frozen raw/repair prefix identities plus reconstruction/audit hashes and unexplained produced-slot
  loss.

If the gate's reconstruction image is rebuilt for the audit, record its image digest and revision.
Run it only on an amd64 host with `aes`, with a read-only container root, raw/repair inputs mounted
read-only, a small `/tmp` tmpfs, and only the report destination writable. The image runs as
UID/GID `10001`; never grant it a writable live ingest mount.

Rollback stops only the candidate and restores the unchanged incumbent process against the same
volumes and WAL. Never delete or rewrite ingest data to make rollback succeed. After a new retention
marker or protected WAL format is published, rollback compatibility must be proved explicitly;
otherwise preserve the new source and escalate rather than starting marker-unaware code.
