# Blockzilla watcher gateway

`blockzilla-watcher-gateway` is the bounded Rust public boundary for the
Blockzilla watcher. It lives in the main Cargo workspace but runs as a separate
process, so public HTTP traffic cannot interrupt archive scheduling or replay.

It supersedes the former Python proxy while preserving the explicit public
endpoint and header allowlists, strict ingest-status projection, JSON and SSE
redaction, body limits, private-address checks, and secret-free `502` responses.
Static watcher assets are streamed without buffering.

Run it against local-only watcher upstreams (serve mode is now the default):

```console
cargo run --release -p blockzilla-watcher-gateway -- \
  --listen 127.0.0.1:8787 \
  --upstream 127.0.0.1:8786 \
  --ingest-upstream 127.0.0.1:8790
```

For the new Rust Topcoat monitor shell, opt in with `--topcoat-ui` (off by default):

```console
cargo run --release -p blockzilla-watcher-gateway -- \
  --listen 192.168.1.45:8788 \
  --upstream 127.0.0.1:8786 \
  --ingest-upstream 127.0.0.1:8790 \
  --topcoat-ui
```

Legacy form still works for scripts that still use `serve` explicitly:

```console
cargo run --release -p blockzilla-watcher-gateway -- serve --topcoat-ui
```

Test the contract with:

```console
cargo test -p blockzilla-watcher-gateway
```

Publish bounded runtime operations directly from Rust:

```console
cargo run --release -p blockzilla-watcher-gateway -- \
  publish-runtime-operations \
  --output /path/to/ui/api/v1/sidecars/runtime-operations/status.json \
  --interval-secs 5
```

Use `--once` for a single atomic publication. The collector samples only
same-user processes, keys counter history by both PID and process start time,
redacts command lines and paths from its output, and bounds published process
and job lists. Install the checked-in
`systemd/blockzilla-watcher-runtime-operations.service` as a user unit so the
publisher has the same Unix identity as the observed jobs. The former Python
publisher was retired after fixture and live-output parity.
