# Blockzilla system schema

Status: **proposed target architecture**. The current implementation status is
listed below the diagram.

```mermaid
flowchart LR
    subgraph SOURCES["Independent source inputs"]
        direction TB
        G1["Yellowstone gRPC A"]
        G2["Yellowstone gRPC B"]
        S1["shred stream A"]
        S2["shred stream B"]
        CAR["CAR / CAR.ZST"]
    end

    subgraph HIVES["Hivezilla source instances"]
        direction TB
        HG1["gRPC instance A"]
        HG2["gRPC instance B"]
        HS1["shred instance A"]
        HS2["shred instance B"]
    end

    BLOCK["Blockzilla<br/>archive authority"]
    LOCAL["Local Archive V2<br/>canonical storage"]
    EDGE["Edgezilla Archive V2<br/>R2 online + B2 recovery"]
    STREAM["Blockzilla Streamer<br/>sync + index"]
    DB["Local indexer / database"]
    WORKER["Edgezilla read-only<br/>getBlock Worker"]
    RPC["getBlock clients"]

    G1 --> HG1 --> BLOCK
    G2 --> HG2 --> BLOCK
    S1 --> HS1 --> BLOCK
    S2 --> HS2 --> BLOCK
    CAR --> BLOCK
    BLOCK --> LOCAL
    BLOCK --> EDGE
    LOCAL --> STREAM --> DB
    EDGE --> STREAM
    EDGE --> WORKER --> RPC
```

The repeated Hivezilla boxes are deliberate: each source instance has its own
identity, cursor, WAL, and failure boundary. They meet at Blockzilla, which owns
deduplication, repair, ordering, validation, and canonical publication.

There is no Hivezilla-to-Streamer path. Streamer consumes only committed
Blockzilla archives. There is also no Edgezilla write path: Blockzilla writes
and verifies both edge copies; Edgezilla stores and serves them read-only.

## Responsibilities

- **Hivezilla** captures one network source per independently supervised
  instance and preserves replay evidence when downstream processing fails.
- **Blockzilla** is the main product and sole archive authority. It accepts CAR
  or Hivezilla evidence and produces canonical Archive V2.
- **Edgezilla** is the replicated read boundary: one Archive V2 generation is
  copied to R2 for online reads and B2 for recovery, then exposed by a read-only
  Worker.
- **Blockzilla Streamer** is the planned indexer-facing path. It reads compact
  blocks from verified local or edge storage for backfill and follow.

## Current implementation

| Path | Status on current `main` |
| --- | --- |
| CAR/CAR.ZST → Blockzilla → local Archive V2 | Implemented |
| Current Hivezilla capture directory → Blockzilla → local Archive V2 | Implemented prototype path |
| Yellowstone capture and durable-ingest foundations | Implemented under `hivezilla/` |
| Multiple production gRPC instances | Planned |
| Shred Hivezilla implementation | Planned |
| Blockzilla server, scheduler, and R2/B2 publisher | Planned |
| R2 → read-only Edgezilla Worker → `getBlock` | Implemented |
| Experimental CAR-backed Edgezilla compatibility Worker | Restored and buildable; intentionally outside the canonical schema above |
| Independently verified B2 publication/recovery | Planned |
| `blockzilla sync` and `blockzilla stream` | Planned |

See the [system overview](system-overview.md), the
[Streamer contract](local-streaming.md), and the
[roadmap](../../ROADMAP.md) for the boundaries behind this schema.

The repository also contains a read-only experimental Old Faithful CAR-backed
Worker. It is useful for compatibility and reference testing, but it is not a
Blockzilla Archive V2 authority and is intentionally omitted from the main
product flow above.
