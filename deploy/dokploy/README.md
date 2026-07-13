# Raw Yellowstone recorder on Dokploy

This deployment runs only `record-grpc-raw`: it retains confirmed Yellowstone
protobuf update envelopes in the crash-recoverable segmented WAL and does not
build archives, indexes, PoH sidecars, or signatures. It has no inbound port.

## Deploy

1. Create a Dokploy **Compose** application from this repository and select
   `docker-compose.dokploy.yml` as the Compose file.
2. Add the variables from `.env.example` in Dokploy. Store the real
   `BLOCKZILLA_GRPC_X_TOKEN` only in Dokploy's environment/secret UI. Never put
   it in Git or build arguments.
3. Before the first start, set `BLOCKZILLA_RAW_FROM_SLOT` to the earliest slot
   the provider can replay, if known. Leaving it empty starts at the provider's
   live position. The durable journal controls all later resumes.
4. Deploy. Keep the existing NAS/Mac recorder alive until the Hetzner journal is
   advancing at least as fast as the finalized head and its start overlaps a
   durable slot from another recorder.

The image is built with Rust 1.96 for `linux/amd64`. Runtime UID/GID `10001`
owns the named `raw_data` volume. If an existing externally created volume is
mounted instead, make `/data` writable by `10001:10001` before starting. The
endpoint, cluster ID, origin node ID, and source ID are part of the persisted
spool identity; use a new volume rather than changing them in place.

The default 768 MiB hard memory limit is deliberately above the recorder's
normal footprint while leaving headroom for a worst-case 128 MiB protobuf,
its encoded buffer, and its compressed buffer. Measure the live high-water
mark before lowering this limit; an OOM restart can exceed provider replay.

## Operate and verify

The supervisor reconnects after stream errors and the Rust recorder recovers
the WAL tail before requesting `last durable slot + 1`. A 20 GiB free-space
floor is enforced both before launch and before each append. Falling through
that floor pauses capture and makes the container unhealthy; it never deletes
old segments automatically.

The healthcheck reads only the `raw-blocks.jsonl` size and modification time. It
does not parse the actively appended JSON tail. A journal older than 180 seconds
is unhealthy after the startup grace period. Docker's `unless-stopped` policy
does not restart a merely unhealthy container; reconnects are handled by the
in-container supervisor, while `unhealthy` is an operator alert for a stale feed
or exhausted disk.

Useful commands from the Dokploy terminal are:

```sh
df -h /data
stat /data/grpc-raw/raw-blocks.jsonl
/usr/local/bin/blockzilla-live-producer inspect-grpc-raw \
  --output-dir /data/grpc-raw
```

Run `inspect-grpc-raw --verify-payloads` only against a stopped recorder or a
filesystem snapshot/copy; a full scan of files being appended is not a
consistent verification snapshot.

Capacity must include the 20 GiB reserve **plus** the required outage window.
Measure the compressed byte growth on this provider before relying on it. Since
there is no automatic retention, arrange verified export/consumption and delete
segments only after the downstream durability protocol says they are safe.

Do not stop the prior recorder based only on a healthy badge. Compare the last
durable slot and its rate over several minutes, confirm overlap, then perform
the handoff. A provider that cannot replay the requested slot can still leave a
gap after any disconnect; redundant streams and downstream slot dedup/repair
remain necessary for a no-loss design.
