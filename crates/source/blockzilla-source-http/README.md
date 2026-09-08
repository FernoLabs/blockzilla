# blockzilla-source-http

HTTP byte ranges with pinned object identities.

This crate does not depend on an archive format or reader.

Exact range reads require HTTP 206, the requested range, the expected length,
and the pinned object identity. Incomplete response bodies can be retried twice.
HTTP 500, 502, 503 and 504 can also be retried twice, with 250/500 ms backoff.
Numeric Retry-After values up to five seconds are respected. Longer or
unsupported values fail without an early retry. With both retry budgets, one
read has at most nine GET attempts. Invalid successful responses and changed
identities are not retried.

`server_error_retries` and `incomplete_body_retries` are separate counters.
GET counts include retries. Body counters include bytes consumed by the reader;
error bodies are dropped without being read. No error body enters a cache.

HTTP sources recommend up to eight concurrent reads. Each format reader applies
its own input-buffer limit; this hint does not create threads in this crate.
