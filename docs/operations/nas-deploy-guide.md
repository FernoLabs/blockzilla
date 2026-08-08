# Building and deploying a new binary to the NAS

Ship a build of `blockzilla`, `blockzilla-monitor`, or any workspace binary
to `Blockzilla-00`. See `nas-deployment-layout.md` for NAS layout and LAN
IP, `DEPLOYED.md` (on NAS, `/volume1/blockzilla/DEPLOYED.md`) for history.

## Build (musl — default)

One-time setup:

```bash
brew install FiloSottile/musl-cross/musl-cross
rustup target add x86_64-unknown-linux-musl
```

Build:

```bash
RUSTFLAGS="-C target-feature=+aes,+sse2" \
  cargo build --release --target x86_64-unknown-linux-musl -p <package-name>
```

Verify (expect `static-pie linked`, no `interpreter` line):

```bash
file target/x86_64-unknown-linux-musl/release/<package-name>
```

Don't use `cargo-zigbuild` — fails to link this workspace (missing symbols
in zig 0.16's `libunwind.a`).

## Build (glibc — only if a dep won't build against musl)

```bash
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/work -w /work \
  -v monitor_cargo_registry:/usr/local/cargo/registry \
  -e RUSTFLAGS="-C target-feature=+aes,+sse2" \
  rust:1.97-bookworm \
  cargo build --release -p <package-name>
```

Verify (expect `dynamically linked ... for GNU/Linux 3.2.0`):

```bash
file target/release/<package-name>
```

## Deploy

SSH: `ssh -p 22 <nas-user>@<nas-lan-ip>`. Binaries live flat in
`/volume1/blockzilla/bin/`, each with a `.prev` sibling for rollback.

1. Stage as `.new` (swap `target/release/<package-name>` for glibc):

   ```bash
   cat target/x86_64-unknown-linux-musl/release/<package-name> | \
     ssh -p 22 <nas-user>@<nas-lan-ip> "cat > /volume1/blockzilla/bin/<package-name>.new"
   ssh -p 22 <nas-user>@<nas-lan-ip> "chmod +x /volume1/blockzilla/bin/<package-name>.new"
   ```

2. Smoke-test:

   ```bash
   ssh -p 22 <nas-user>@<nas-lan-ip> "/volume1/blockzilla/bin/<package-name>.new --help"
   ```

   `GLIBC_x.xx not found` → wrong glibc build image, fix and rebuild.

3. Rotate and swap:

   ```bash
   ssh -p 22 <nas-user>@<nas-lan-ip> "cd /volume1/blockzilla/bin && \
     mv <package-name> <package-name>.prev && \
     mv <package-name>.new <package-name>"
   ```

4. Restart (never raw `kill` — `Restart=always` just relaunches the old
   binary):

   ```bash
   ssh -p 22 <nas-user>@<nas-lan-ip> "systemctl --user restart <unit-name>.service"
   ```

   Unsure of the unit? `systemctl --user status <pid>` (don't trust `ps`'s
   parent PID).

5. Verify it's back up (wait 2-3s after "Started" before checking the port):

   ```bash
   ssh -p 22 <nas-user>@<nas-lan-ip> "systemctl --user status <unit-name>.service --no-pager | head -8"
   ssh -p 22 <nas-user>@<nas-lan-ip> "ss -ltnp | grep <port>"
   ```

## Record it

Update `/volume1/blockzilla/DEPLOYED.md` on NAS: what changed, SHA256
(`sha256sum /volume1/blockzilla/bin/<package-name>`), toolchain (musl vs
glibc), service driven, and any uncommitted local changes at build time.

## `blockzilla-archive` restarts: check for stuck PoH migrations

`KillMode=control-group` kills in-flight `migrate-poh-signature-counts`
children on restart, leaving their ownership marker stuck `running` — the
duplicate-writer guard then refuses to re-admit that epoch. After any
restart during an active migration:

```bash
ssh -p 22 <nas-user>@<nas-lan-ip> "wget -qO- http://127.0.0.1:8786/api/v1/status" | \
  grep -o 'poh_migration:[0-9]* spawn failed[^"]*'
```

For each stuck epoch, confirm pid is dead (`kill -0 <pid>` fails), then
delete its marker (safe — migration is whole-epoch atomic):

```bash
ssh -p 22 <nas-user>@<nas-lan-ip> "rm /volume1/blockzilla/scheduler-state/poh_migrations/epoch-<N>.json"
```

## Rollback

```bash
ssh -p 22 <nas-user>@<nas-lan-ip> "cd /volume1/blockzilla/bin && \
  mv <package-name> <package-name>.rolled-back && \
  mv <package-name>.prev <package-name> && \
  systemctl --user restart <unit-name>.service"
```

Only one `.prev` gen kept — for older builds, check `DEPLOYED.md` for SHA256
and rebuild from that commit.
