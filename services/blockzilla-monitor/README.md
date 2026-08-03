# Blockzilla Monitor (Topcoat)

`blockzilla-monitor` is the new standalone monitor service. It starts a single
binary that serves:

- Topcoat + Datastar monitor shell at `/`
- Public API proxy to the local watcher scheduler on `/api/*`
- Ingest-status sidecar forwarding and same safety checks that the existing
  watcher gateway applies.

This is the first cut for replacing the previous svelte-based monitor surface.

Start it with:

```console
cargo run -p blockzilla-monitor -- \
  --listen 127.0.0.1:8890 \
  --upstream 127.0.0.1:8786 \
  --ingest-upstream 127.0.0.1:8790
```

Topcoat UI is enabled by default.

Run `--help` to inspect all arguments.

