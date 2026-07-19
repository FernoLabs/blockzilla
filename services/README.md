# Services

Deployable processes and operational clients that support Blockzilla live here.

| Service | Status | Role |
| --- | --- | --- |
| [`hivezilla`](hivezilla/README.md) | Prototype | Captures live gRPC input and retains recoverable raw data. |
| [`blockzilla-watcher`](blockzilla-watcher/README.md) | Prototype | Monitors live indexing, archive work, and NAS health through the watcher API. |
| [`blockzilla-get-block`](blockzilla-get-block/README.md) | Experimental | Serves Archive V2 through a read-only Worker. |
| [`old-faithful-get-block`](old-faithful-get-block/README.md) | Experimental | Provides a read-only Old Faithful compatibility path. |

Edgezilla is the architectural name for the replicated read plane, not a
separate package or directory. Blockzilla remains the only archive authority
and publisher.
