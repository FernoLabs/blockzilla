# V3 concurrent input

V3 uses the same input-window settings type as V2, exported as
`IndexerV3NetworkInputConfig`. The default is eight input workers, a 32 MiB
group target, and a 256 MiB reserved input-capacity limit. Call
`set_network_input_config(None)` on the archive or instruction source to use
the previous input schedule. Local sources retain their existing schedule.
The diagnostic tool exposes `--v3-legacy-input` for a same-binary comparison.

The planner joins adjacent selected jobs up to the sum of their selected
semantic-plane bytes and signature bytes. Each group contains at most 8,192
blocks. It preserves the existing four-block decode jobs. It does not fill
sparse gaps or read unrequested planes. Sparse jobs and groups larger than the
target use the existing per-job path and its separate resource limits.

For each selected plane, the planner finds the maximum required capacity
across the planned groups. It also finds the maximum signature capacity.
Each input slot reserves these capacities once. The number of slots is the
smaller of the configured worker count, group count, and number that fits the
byte budget. This accounts for changes in plane proportions across groups;
using only the largest aggregate group size would not bound retained vectors.
If one slot cannot fit, the scan returns an error before starting input workers.

Each input worker owns one slot and reads its assigned groups in order. Plane
and signature reads within one group are sequential; groups load concurrently.
The output coordinator receives groups in their original order. Input workers
keep one reference to their last group. Before filling a slot again, a worker
waits until only that reference and its semantic owner remain. The wait checks
cancellation at intervals of one millisecond. No transaction payload is copied
to recycle input. Compressed planes and signature vectors retain their capacity.
There are still group-level allocations and HTTP body copies; this is not a
claim of an allocation-free or fully zero-copy reader.

The byte permit stays with the shared semantic storage, including when a group
is queued or a decode worker retains it. Signatures are dropped before that
semantic owner. The fixed slot budget includes the reserved semantic and
signature vectors. It excludes the block index, group-plan metadata, HTTP
buffers, decoded frames, registry data, and canonical output. The existing
oversized/sparse fallback retains its own limits.

The coordinator disconnects all input result channels before it joins workers
on completion or failure. Cancellation stops workers waiting for a shared slot.
A later input result cannot pass an earlier group in the input stream. Existing
HTTP range, object identity, length, and retry checks remain in the source.
The decoded-job and projected-output admission limits do not increase.

The focused fixture compares local, previous remote, and concurrent remote
output. It delays the first directory range until a later directory read starts,
checks reused buffer addresses, and verifies source-byte parity for dense and
sparse selections. It also checks input failure and sink cancellation with
multiple queued groups. The NAS comparison measures count, transaction
identities, and USDC on epoch 900 prefixes with fresh caches and 12 decode workers.
