# Building and deploying a new binary to the NAS

How to ship a new build of `blockzilla`, `blockzilla-monitor`, or any other
Rust binary in this workspace to `Blockzilla-00` (192.168.1.45). Covers the
mechanics only -- see `nas-deployment-layout.md` for what's where on the NAS,
and `DEPLOYED.md` (on the NAS, `/volume1/blockzilla/DEPLOYED.md`) for binary
provenance history.

## Why cross-compile, and glibc vs musl

The Mac doing the building is Apple Silicon; the NAS is Debian 12 (bookworm),
x86_64. A plain `cargo build --release` produces a macOS ARM64 binary the NAS
can't run. Cross-linking with `cross`/a generic `x86_64-unknown-linux-gnu`
Docker image is fine for the *compiler* but historically wasn't safe for a
**glibc**-linked binary: that toolchain used to default to a newer glibc
baseline than an older NAS image had, which fails at runtime with
`GLIBC_x.xx not found` even though it links and runs fine locally.

Two ways to avoid that, and as of 2026-08-07 this NAS runs the **musl** build:

- **musl (preferred, currently deployed for `blockzilla`):** `cross build
  --target x86_64-unknown-linux-musl` produces a static-pie binary with no
  runtime libc dependency at all -- nothing to version-match against the NAS,
  ever again. Verified as not slower than the glibc build in production (see
  `DEPLOYED.md`, 2026-08-07 entry) before switching.
- **glibc (fallback):** building inside a `rust:1.97-bookworm` container
  matches the NAS's OS and glibc exactly -- same effect as native
  compilation, just running on emulated x86_64 via Docker. Use this if a
  dependency ever refuses to build against musl (rare, but some C-linked
  crates assume glibc).

Both paths need one extra flag on this Mac: `gxhash` (used by the Archive V2
dedup code) hard-requires AES-NI/SSE2 intrinsics and refuses to build without
`RUSTFLAGS="-C target-feature=+aes,+sse2"` under Docker/QEMU x86_64 emulation
on Apple Silicon -- the emulated CPU doesn't advertise those features by
default the way `target-cpu=native` would need. AES-NI is standard on any
real x86_64 server CPU from the last decade (including the NAS's), so this
is safe to always pass, not a narrowing workaround.

## Build (musl, preferred)

From the repo root, with Docker Desktop running (`cross` shells out to it):

```bash
RUSTFLAGS="-C target-feature=+aes,+sse2" \
  cross build --release --target x86_64-unknown-linux-musl -p <package-name>
```

Binary lands at `target/x86_64-unknown-linux-musl/release/<package-name>`.
Verify before shipping it anywhere:

```bash
file target/x86_64-unknown-linux-musl/release/<package-name>
```

Expect `ELF 64-bit LSB pie executable, x86-64 ... static-pie linked`. No
`interpreter` line and no `GNU/Linux X.Y.Z` version tag -- that's the point.

## Build (glibc, fallback)

```bash
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/work -w /work \
  -v monitor_cargo_registry:/usr/local/cargo/registry \
  -e RUSTFLAGS="-C target-feature=+aes,+sse2" \
  rust:1.97-bookworm \
  cargo build --release -p <package-name>
```

Swap `<package-name>` for `blockzilla`, `blockzilla-monitor`, etc. The named
Docker volume caches the crate registry/build artifacts across runs --
reuse the same volume name every time so incremental builds actually help
(a cold build of `blockzilla-monitor` takes ~5 min; warm, ~2-3 min).

Verify the output before shipping it anywhere:

```bash
file target/release/<package-name>
```

Expect `ELF 64-bit LSB pie executable, x86-64 ... dynamically linked ... for
GNU/Linux 3.2.0`.

## Deploy

NAS binaries live flat in `/volume1/blockzilla/bin/`, no dated release
directories -- each binary has a `.prev` sibling for one-step rollback. SSH
is `ssh -p 22 ach@192.168.1.45` (not port 35022, despite older `~/.ssh/config`
entries).

1. **Stage as `.new`, don't overwrite the live binary directly** (path below
   is the musl output; swap in `target/release/<package-name>` for a glibc
   build):

   ```bash
   cat target/x86_64-unknown-linux-musl/release/<package-name> | \
     ssh -p 22 ach@192.168.1.45 "cat > /volume1/blockzilla/bin/<package-name>.new"
   ssh -p 22 ach@192.168.1.45 "chmod +x /volume1/blockzilla/bin/<package-name>.new"
   ```

2. **Smoke-test the staged binary standalone**, before it's driving anything:

   ```bash
   ssh -p 22 ach@192.168.1.45 "/volume1/blockzilla/bin/<package-name>.new --help"
   ```

   A `GLIBC_x.xx not found` error here means a glibc build used the wrong
   image (see "Why cross-compile" above) -- fix and rebuild, don't proceed.
   musl binaries can't hit this failure mode at all (no libc dependency to
   mismatch); a musl smoke-test failure means something else is wrong.

3. **Rotate and swap:**

   ```bash
   ssh -p 22 ach@192.168.1.45 "cd /volume1/blockzilla/bin && \
     mv <package-name> <package-name>.prev && \
     mv <package-name>.new <package-name>"
   ```

4. **Restart via systemd, never a raw `kill`.** Every long-running service
   here has `Restart=always`; killing the process just races the supervisor
   into relaunching the *old* binary path a few seconds later, and if
   `KillMode=control-group` is set (it is, on the archive scheduler), a
   plain restart also takes down every in-flight child job. Use the unit
   name, not the PID:

   ```bash
   ssh -p 22 ach@192.168.1.45 "systemctl --user restart <unit-name>.service"
   ```

   Find the right unit with `systemctl --user status <pid>` if unsure which
   service owns a running process -- don't assume from the process tree
   alone; `ps`'s parent PID often just shows `systemd --user` for anything
   under lingering, whether or not it's unit-managed.

5. **Verify it actually came back up** -- `active (running)`, listening on
   the expected port, serving real content:

   ```bash
   ssh -p 22 ach@192.168.1.45 "systemctl --user status <unit-name>.service --no-pager | head -8"
   ssh -p 22 ach@192.168.1.45 "ss -ltnp | grep <port>"
   ```

   Give it 2-3 seconds after "Started" before checking the port -- there's
   a real startup window (loading state, binding the listener) where
   `systemctl` already reports active but nothing's listening yet.

## Record it

Update `/volume1/blockzilla/DEPLOYED.md` on the NAS with what changed, the
new SHA256 (`sha256sum /volume1/blockzilla/bin/<package-name>`), which
target/toolchain built it (musl via `cross` vs glibc via Docker), and which
service it's driving. If the working tree had uncommitted changes at build
time (common during active development), say so explicitly -- don't imply a
clean tagged-commit provenance that isn't true.

## Restarting `blockzilla-archive` specifically: mind in-flight PoH migrations

`KillMode=control-group` means every restart kills any running
`migrate-poh-signature-counts` child along with the scheduler process. That
job kind has no restart-adoption subsystem (unlike legacy compact), so a
killed child's ownership marker is left stuck in `running` state with a now-
dead pid forever -- the duplicate-writer guard then refuses to ever re-admit
that epoch. After *any* restart while a migration was active, check for this
before walking away:

```bash
ssh -p 22 ach@192.168.1.45 "wget -qO- http://127.0.0.1:8786/api/v1/status" | \
  grep -o 'poh_migration:[0-9]* spawn failed[^"]*'
```

For each stuck epoch, confirm the recorded pid is actually dead
(`kill -0 <pid>`, expect it to fail) before deleting its marker directly --
safe because this migration is whole-epoch atomic with no partial
checkpoint:

```bash
ssh -p 22 ach@192.168.1.45 "rm /volume1/blockzilla/scheduler-state/poh_migrations/epoch-<N>.json"
```

37 of these accumulated silently across this session's several restarts
before being caught and cleaned up -- worth checking after every restart,
not just ones that felt risky.

## Rollback

```bash
ssh -p 22 ach@192.168.1.45 "cd /volume1/blockzilla/bin && \
  mv <package-name> <package-name>.rolled-back && \
  mv <package-name>.prev <package-name> && \
  systemctl --user restart <unit-name>.service"
```

Only one `.prev` generation is kept by convention -- if you need to go back
further, check `DEPLOYED.md`'s history for the SHA256 of an older build and
rebuild from that commit instead.
