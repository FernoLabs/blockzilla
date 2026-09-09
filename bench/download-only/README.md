# Download-only HTTP probe

Measure the transport ceiling before changing V2 decode scheduling. This probe
uses reqwest 0.13.4, the same client version as the SDK, with HTTP/2 explicitly
enabled in this benchmark package. The tool requires the explicit `transport` feature so ordinary workspace builds
do not enable HTTP/2 for production readers through feature unification.
The V2 example's selected dependency graph
currently enables blocking + rustls only. Reqwest already uses Hyper and Tokio;
this experiment does not replace them or change the production HTTP source.

Build with `cargo build --release -p blockzilla-download-only --features transport`. Use a fresh
output path for each attempt:

```sh
blockzilla-download-only --url https://HOST/OBJECT \
  --protocol http1 --workers 4 --clients 1 --range-mib 32 \
  --bytes 536870912 --output result.json
```

- Protocols: `http1`, `http2`, `http2-adaptive`. Protocol mismatch fails; HTTP/2
  never silently becomes HTTP/1.1. Adaptive mode enables reqwest's adaptive
  HTTP/2 flow-control window; ordinary HTTP/2 keeps the client's defaults.
- `--clients 1` shares a connection pool. `--clients 4` uses four independent
  pools. The pool count is not proof of physical connection count; a pool can
  open/recycle connections. Record HTTP/2 multiplexing separately from HTTP/1.1
  concurrent connections rather than assuming equivalent connection behavior.
- Up to 16 download workers; each reuses a 1 MiB body buffer. HTTP/TLS internal
  buffers are additional memory. Bodies are discarded without decoding, disk
  writes, or an ordered-consumer buffer. This is not a measurement of SDK RSS
  or a complete replacement for its bounded ordered input scheduler.
- Requests use exact closed ranges, identity encoding, pinned strong ETags,
  length and Content-Range checks. Redirects and retries are disabled. HEAD
  pins each pool before scan timing and checks identity again after the scan.
  Setup includes client creation and initial HEADs. Total is setup + scan;
  post-scan HEAD, receipt serialization and process startup are excluded.
- Results record exact received body bytes (including partial failed reads),
  per-request header wait and body-read durations, protocol, Cloudflare request
  ID/cache status, and peak active requests. Header wait includes pool, connect,
  TLS and server wait as applicable; it is not a pure origin-latency metric.
- `--hash` computes BLAKE3 for each fixed 1 MiB piece. Flatten the piece hashes
  from ordered requests to compare the same offset/length with different
  request sizes or protocols. Run these correctness checks separately from
  uninstrumented timing. Strong ETag/range checks alone do not prove byte parity.
- A failed run saves a receipt with `valid=false`; it has no accepted MB/s.
  The supervisor should capture process CPU/RSS and avoid concurrent archive
  work. Sample repeated fixed ranges in alternating order; CDN state is not
  controlled. All offsets and lengths must be whole MiB, at most 16 GiB/run.

Start with one fixed V2 object region. Validate HTTP/1.1 and HTTP/2 hashes, then
compare 1/4/8 workers with 32 MiB ranges. For a promising protocol, test 16 and
64 MiB and independent pools. Repeat the best configuration and baseline in
alternating order before integrating it into the SDK. Test native async input
only after this control indicates the blocking bridge is material. Larger
windows or another library are hypotheses, not measured improvements.

The repository's CAR acquisition uses aria2 with `--split=4`,
`--max-connection-per-server=4`, and `--min-split-size=64M`. That minimum is not
a fixed 64 MiB request size. A previous fast CAR transfer is useful motivation,
but its source, interval, concurrency and destination differ from V2 count.

Official guidance checked 9 September 2026:

- [Cloudflare HTTP/2](https://developers.cloudflare.com/speed/optimization/protocol/http2/): enabled by default at the edge. Client-to-edge protocol is separate from origin transport.
- [R2 public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/): r2.dev is development-only and rate limited. Our current URL is a Workers gateway, so this is not evidence of an r2.dev limit in our test.
- [R2 architecture](https://developers.cloudflare.com/r2/how-r2-works/): location, request origin and access patterns affect performance.
- [Cloudflare cache behavior](https://developers.cloudflare.com/cache/concepts/default-cache-behavior/): byte-range and cacheable-object limits apply. A custom domain does not imply that a large archive object is cached.
- [aria2 options](https://aria2.github.io/manual/en/html/aria2c.html): splitting and per-server connection settings control parallel transfers.
