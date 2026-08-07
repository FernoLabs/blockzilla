# Building and deploying a new binary to the NAS

How to ship a new build of `blockzilla`, `blockzilla-monitor`, or any other
Rust binary in this workspace to `Blockzilla-00` (192.168.1.45). Covers the
mechanics only -- see `nas-deployment-layout.md` for what's where on the NAS,
and `DEPLOYED.md` (on the NAS, `/volume1/blockzilla/DEPLOYED.md`) for binary
provenance history.

## Why cross-compile

The Mac doing the building is Apple Silicon; the NAS is Debian 12 (bookworm),
x86_64, glibc 2.36. A plain `cargo build --release` produces a macOS ARM64
binary the NAS can't run. Cross-linking with `cross`/a generic
`x86_64-unknown-linux-gnu` Docker image also isn't safe here: that toolchain
defaults to a newer glibc baseline (2.39, Ubuntu 24.04) than the NAS has,
which fails at runtime with `GLIBC_2.39 not found` even though it links and
runs fine locally. Building inside a `rust:1.97-bookworm` container sidesteps
both problems -- same OS, same glibc, genuinely native compilation (just
running on emulated x86_64 via Docker, not real cross-linking), so the
`.so` dependency versions match exactly.

## Build

From the repo root, with Docker Desktop running:

```bash
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/work -w /work \
  -v monitor_cargo_registry:/usr/local/cargo/registry \
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

Expect `ELF 64-bit LSB pie executable, x86-64 ... for GNU/Linux 3.2.0`.

## Deploy

NAS binaries live flat in `/volume1/blockzilla/bin/`, no dated release
directories -- each binary has a `.prev` sibling for one-step rollback. SSH
is `ssh -p 22 ach@192.168.1.45` (not port 35022, despite older `~/.ssh/config`
entries).

1. **Stage as `.new`, don't overwrite the live binary directly:**

   ```bash
   cat target/release/<package-name> | \
     ssh -p 22 ach@192.168.1.45 "cat > /volume1/blockzilla/bin/<package-name>.new"
   ssh -p 22 ach@192.168.1.45 "chmod +x /volume1/blockzilla/bin/<package-name>.new"
   ```

2. **Smoke-test the staged binary standalone**, before it's driving anything:

   ```bash
   ssh -p 22 ach@192.168.1.45 "/volume1/blockzilla/bin/<package-name>.new --help"
   ```

   A `GLIBC_x.xx not found` error here means the wrong build image was used
   (see "Why cross-compile" above) -- fix and rebuild, don't proceed.

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
new SHA256 (`sha256sum /volume1/blockzilla/bin/<package-name>`), and which
service it's driving. If the working tree had uncommitted changes at build
time (common during active development), say so explicitly -- don't imply a
clean tagged-commit provenance that isn't true.

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
