# Blockzilla watcher gateway

`blockzilla-watcher-gateway` is the bounded Rust public boundary for the
Blockzilla watcher. It lives in the main Cargo workspace but runs as a separate
process, so public HTTP traffic cannot interrupt archive scheduling or replay.

It supersedes the former Python proxy while preserving the explicit public
endpoint and header allowlists, strict ingest-status projection, JSON and SSE
redaction, body limits, private-address checks, and secret-free `502` responses.
Static watcher assets are streamed without buffering.

Run it against local-only watcher upstreams:

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

Test the contract with:

```console
cargo test -p blockzilla-watcher-gateway
```

This binary is intentionally focused on the public watcher runtime; the legacy
`publish-runtime-operations` flow is handled by the dedicated service unit and
is hidden from normal CLI usage.
