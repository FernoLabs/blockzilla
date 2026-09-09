# V2 concurrent input window

The network query path groups adjacent compressed block frames into larger HTTP
ranges without increasing the decoded-block or ordered-output windows. The
slot index already provides exact offsets and lengths; no format or index
migration is needed. The local path retains its existing read schedule.

`CompactV2ParallelScanConfig::new` enables eight remote input workers, a 32 MiB
range target, and a 256 MiB compressed capacity limit. Set `network_input=None`
to compare with the legacy schedule. These settings apply only when the source
advertises concurrent reads. The lower-level `OrderedParallelBlockConfig`
keeps this option disabled unless its caller selects it.

The reader first validates the small decode plans and then merges only adjacent
plans within the compressed range target. It never merges an unselected gap or
splits a compressed frame. The maximum selected range length determines each
buffer's reserved capacity. The number of buffers is the smaller of the worker
limit and the byte budget divided by that capacity. Thus an unusually large
frame reduces concurrency; a frame larger than the entire budget fails before
input allocation. Up to 16 workers and a 1 GiB explicit budget are admitted.
The range target must fit the configured byte budget; invalid settings fail
before any input workers start.

Free, downloading, queued and worker-held buffers are the same fixed set of
recycled vectors. Decode jobs use shared references to slices in a downloaded
buffer. The last consumer returns that buffer to the pool. The budget covers
these caller-owned compressed buffers, not the HTTP library's internal memory,
registry storage, decompressed frames, or output values. Exact-capacity buffers
avoid geometric growth when the built-in HTTP source fills them in place.

The rolling decoder still admits at most 96 blocks with 12 workers, at most
131,072 transactions, and 64 MiB of declared decoded input under the normal query
configuration. An oversized decoded block runs alone under the existing rule.
Output remains ordered, and a later failed download cannot publish ahead of
an earlier range. Cancellation joins all input/decode workers and releases
retained buffers. HTTP range, object identity, exact-length and retry checks
remain in the source layer.

The change deliberately retains the current HTTP client and body-read API. A
separate async-body experiment can measure the known blocking-adapter copy
and per-read wake allocation. It is not necessary to change the format or
HTTP protocol to separate download geometry from decode scheduling.

Validation uses a delayed first read, later input failure, sink cancellation,
a selected subrange, and oversized decoded blocks. It checks exact contiguous
read coverage, ordered callbacks, shared-window capacity, reduced request
count, preserved decode admission, and joined workers. The 234 V2 library tests
pass. NAS comparisons use count, transaction identities and indexed USDC,
alternating new input with `--v2-legacy-input` in the same diagnostic binary.
No allocation counter or flame graph is enabled in timing runs.

The download-only 547–558 MB/s result is a transport ceiling on a short region,
not a promised SDK speed. Its sixteen 64 MiB requests discard input immediately;
the ordered reader must retain input for its consumers. Compare SDK speed and
RSS together before changing the default budget or increasing concurrency.
