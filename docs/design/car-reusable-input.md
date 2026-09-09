# Reusable CAR network input

CAR already uses concurrent 32 MiB HTTP ranges. This change applies the input
buffer reuse used by the newer V2 and V3 paths to CAR's ordered byte stream.
The default remains four download workers and eight input slots (256 MiB).
Buffer reuse is optional and disabled by default.

A scheduled range now owns both its byte geometry and a body vector. The input
worker fills that vector, validates the response, and sends the result to the
ordered reader. When the caller consumes the complete chunk, the reader moves
that vector into the next scheduled range. No body copy is needed to transfer
ownership. Only the initial window needs new large body vectors during a
successful fixed-size stream. A short final range reuses a larger vector.

The read helper keeps initialized storage. It initializes only new bytes when
a vector grows, then reads the response over the complete requested slice.
A short body or read failure never enters the output stream. Retry attempts
repeat identity, status, range, length, and exact-body checks before publication.
There is no change to CAR framing, metadata decoding, or transaction ordering.

The existing chunk window bounds queued, active, completed, and consumer-held
body vectors. The new schedule moves a consumed vector directly to its next
owner; it does not briefly hold an old body while allocating its replacement.
The window excludes HTTP/TLS allocations, parser buffers, decoder workspaces,
and canonical output. The Read interface still copies bytes into caller buffers.
This is not a claim of a fully zero-copy CAR reader.

`CarHttpOptions::reuse_body_buffers` defaults to false. Set it to true to
reuse bodies. The archive API exposes `CarArchiveOptions::reuse_http_body_buffers`. The probe exposes `--legacy-http-buffers` and can
compare both modes in the same executable. Two counters measure body-vector
capacity growth: `body_buffer_allocations` and `body_buffer_allocated_bytes`.
They exclude other allocations and measure cumulative capacity, not peak RSS.

The HTTP fixture compares both modes over 22 ranges, including a short final
range and out-of-order completion. It checks exact output and counters: the
reusable mode grows four vectors, while the legacy mode grows 22. The existing
suite covers early drop, worker joins, malformed headers and bodies, retries,
object identity, and real CAR decoding.

The NAS benchmark uses the first 8,192 canonical blocks of epoch 900, 12 decode
workers, and the borrowed metadata visitor. It compares legacy/four workers,
reusable/four workers, and reusable/eight workers in repeated fresh processes.
It verifies each block against the previous count and decode-digest reference.
A second test compares both four-worker modes over 2,048 blocks. All ten
cases pass. Four-worker reuse is 18.3% slower in the longer test and 12.9%
slower in the short confirmation, using combined transaction counts and scan
time. Network conditions were not controlled, but neither test supports a
default change. Peak RSS also did not fall. Buffer reuse remains an experiment.
This full CAR decode workload differs from V2/V3 transaction-identity scans.
