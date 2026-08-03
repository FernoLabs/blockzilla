# Services

Deployable processes and operational clients that support Blockzilla live here.

| Service | Status | Role |
| --- | --- | --- |
| [`hivezilla`](hivezilla/README.md) | Prototype | Captures live gRPC input, retains recoverable raw data, and provides durable replication primitives. |
| [`blockzilla-archive-gateway`](blockzilla-archive-gateway/README.md) | Reference | Serves completed Archive V2 generations through authenticated, bounded HTTP ranges. |
| [`blockzilla-watcher-gateway`](blockzilla-watcher-gateway/README.md) | Implemented | Publishes runtime telemetry and provides the bounded Rust public boundary for the watcher. |
| [`blockzilla-monitor`](blockzilla-monitor/README.md) | New | New standalone Rust monitor shell (Topcoat) for blockzilla, replacing the Node/Svelte watcher UI path.

Edgezilla is the architectural name for the replicated read plane, not a
separate package or directory. In the target architecture, Blockzilla remains
the only canonical catalog authority; a fenced Hivezilla worker performs the
physical archive build and upload.

Operational apps (including the watcher web UI) are placed in [`../apps/`](../apps/)
for deployment outside this services-only boundary.
