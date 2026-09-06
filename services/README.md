# Services

Deployable processes and operational clients that support Blockzilla live here.

| Service | Status | Role |
| --- | --- | --- |
| [`hivezilla`](hivezilla/README.md) | Prototype | Captures live gRPC input, retains recoverable raw data, and provides durable replication primitives. |
| [`blockzilla-archive-gateway`](blockzilla-archive-gateway/README.md) | Reference | Serves completed Archive V2 generations through authenticated, bounded HTTP ranges. |
| [`blockzilla-monitor`](blockzilla-monitor/README.md) | Implemented | Read-only Topcoat dashboard and curated public monitoring boundary for the scheduler. |

Edgezilla is the architectural name for the replicated read plane, not a
separate package or directory. In the target architecture, Blockzilla remains
the only canonical catalog authority; a fenced Hivezilla worker performs the
physical archive build and upload.

Cloudflare Workers are placed in [`../workers/`](../workers/) for deployment
outside this services-only boundary.
