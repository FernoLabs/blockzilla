# Live validator canary operations

Use the native `hivezilla` command on the prepared Ubuntu host. Run it as the
owner of the prepared directory, with noninteractive `sudo` available. Do not
run it as root. It reads the paths and release identities in
`config/validator-argv.json` and `reports/install.json`.

The existing live node must keep running. Do not prepare the same live
directory again and do not run a restart command. `start` refuses to run if
the service or another validator process is active. A `status` call does not
change the service, extend its time limit, or change kernel settings.

## Inspect the running node

```sh
hivezilla validator-canary status \
  --root /path/to/prepared-canary
```

The JSON report includes the selected systemd state, memory and CPU counters,
public RPC identity, genesis hash, and processed, confirmed, and finalized
slots. RPC calls use `127.0.0.1:8899`; proxy settings and redirects are disabled.
RPC can be unavailable while the validator downloads or loads a snapshot.
An active service alone does not prove that the node is ready for capture.

The report contains selected log categories and numeric values from the
current service run. It does not contain raw log messages. Plugin metrics
come from `127.0.0.1:8999`; labels are removed and values with the same metric
name are added. Identity keypair bytes and configuration text are not printed.
Status reads the identity public key from RPC, not from the private key file.

## Start a stopped canary

First prepare a new directory with `hivezilla prepare-validator-canary`.
Complete the pinned validator build if the release does not supply the
executable. Create the identity keypair separately and keep it private. The
preparation command does not create or replace keys, start a service, or edit
kernel settings.

Check the prepared node without starting it:

```sh
hivezilla validator-canary start --check-only \
  --root /path/to/prepared-canary
```

The check requires:

- The prepared validator executable, matching Agave version, plugin asset
  hashes, and a passing plugin configuration check. A legacy
  `validator_ready=false` field does not block a completed source build.
- A non-voting validator with `--no-xdp`, private RPC bound to
  `127.0.0.1:8899`, gRPC bound to `127.0.0.1:10000`, and plugin metrics bound
  to `127.0.0.1:8999`.
- The configured genesis hash, known validators, entrypoints, and
  `--limit-blockstore-size 100000000`. The existing prepared argument list is
  used directly; it is not passed through a shell.
- At least 900 GiB of total memory, 850 GiB of available memory, and 1 TiB of
  free space on each distinct ledger, accounts, or snapshot filesystem.
- No active validator process and free loopback TCP ports 8899, 8900, 10000,
  and 8999. These checks do not prove that external gossip traffic can reach
  the host. The validator still needs its configured network access.

Start only after those checks pass:

```sh
hivezilla validator-canary start \
  --root /path/to/prepared-canary
```

The script creates the transient systemd unit
`hivezilla-validator-canary.service`, with these limits:

| Setting | Value |
| --- | --- |
| Maximum run time | 21,600 seconds, or 6 hours |
| Memory high threshold | `800G` |
| Memory hard limit | `850G` |
| Open file limit | 1,000,000 |
| Locked memory limit | 2,000,000,000 bytes |
| Automatic restart | Disabled |
| File creation mask | `0077` |

The service runs as the directory owner. Output goes to the system journal.
The time limit is a canary limit, not a continuous validator service policy.

The live host was later prepared for continuous operation. Its maximum run
time was removed without restarting the validator, and a persistent unit was
installed for the next boot. See the
[validator Tunnel setup](hivezilla-validator-tunnel.md) for that deployment.
Use systemd to operate that continuous service; this finite start command must
not replace its persistent unit.

Before changing the UDP limits, the script preserves
`reports/kernel-before.json`. If the file already exists, it keeps every field
and byte unchanged. If it is absent, the script durably creates it once before
changing `net.core.rmem_max` and `net.core.wmem_max` to `134217728`. A failed
or repeated start never replaces this baseline. The preparation script does
not change these kernel values.

## Stop explicitly

```sh
hivezilla validator-canary stop \
  --root /path/to/prepared-canary
```

The script checks that the active unit runs as the current user and uses the
prepared validator executable. It then requests a systemd stop. It does not
delete the ledger, accounts, snapshots, captures, reports, or identity. It does
not restore the host UDP limits automatically. Keep the original kernel
snapshot for a separate planned host change.

Each start, check-only start, or stop writes a new durable
`reports/validator-ops-<id>.json` record. It records source hashes, operator UID,
times, outcome, and selected service facts. Existing records are not replaced.
An operation failure also gets a record once the prepared directory and
operation lock are valid. Read that record and call `status`; do not issue a
restart to hide a failed start or an incomplete bootstrap.

Validator operation is separate from capture, archive verification, and R2
publication. An RPC finalized slot is the trusted node's observation. It is
not an independent consensus proof, shred evidence, or permission to publish
an official canonical archive. Use the sealed execution-canary input checks
before the separate R2 publisher can upload data.
