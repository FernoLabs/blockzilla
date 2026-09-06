# Services

Deployable processes are grouped under `blockzilla/` and `hivezilla/` at the workspace root.

| Service | Status | Role |
| --- | --- | --- |
| [`hivezilla`](../../hivezilla/service/README.md) | Prototype | Captures live gRPC input, retains recoverable raw data, and provides durable replication primitives. |
| [`blockzilla-archive-gateway`](../../blockzilla/archive-gateway/README.md) | Reference | Serves completed Archive V2 generations through authenticated, bounded HTTP ranges. |
| [`blockzilla-monitor`](../../blockzilla/monitor/README.md) | Implemented | Read-only Topcoat dashboard and curated public monitoring boundary for the scheduler. |

[Edgezilla](../../edgezilla/README.md) groups the independently deployed Workers.
Blockzilla retains catalog authority; Hivezilla provides capture and replication.
See the [workspace structure](../design/workspace-restructure.md) for crate locations.
