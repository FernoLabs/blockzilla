# Services

Deployable processes and operational clients that support Blockzilla live here.

| Service | Status | Role |
| --- | --- | --- |
| [`hivezilla`](hivezilla/README.md) | Prototype | Captures live gRPC input, retains recoverable raw data, and provides durable replication primitives. |
| [`blockzilla-archive-gateway`](blockzilla-archive-gateway/README.md) | Reference | Serves completed Archive V2 generations through authenticated, bounded HTTP ranges. |
| [`blockzilla-watcher`](blockzilla-watcher/README.md) | Prototype | Monitors live indexing, archive work, and NAS health through the watcher API. |
| [`blockzilla-watcher-gateway`](blockzilla-watcher-gateway/README.md) | Implemented | Publishes runtime telemetry and provides the bounded Rust public boundary for the watcher. |
| [`blockzilla-get-block`](blockzilla-get-block/README.md) | Experimental | Serves Archive V2 through a read-only Worker. |
| [`old-faithful-get-block`](old-faithful-get-block/README.md) | Experimental | Provides a read-only Old Faithful compatibility path. |

Edgezilla is the architectural name for the replicated read plane, not a
separate package or directory. In the target architecture, Blockzilla remains
the only canonical catalog authority; a fenced Hivezilla worker performs the
physical archive build and upload.
