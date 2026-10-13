# Jetstreamer common-output reference

See [the comparison contract and commands](../car-export/README.md).
This independent benchmark workspace pins the unmodified upstream Git revision
`048d2cd25af949679c56347440b7cf5a9a900063` and uses mimalloc. The upstream package
still reports version `0.7.0`; this revision is newer than the published 0.7.0
release. The lockfile starts from that revision's dependency lock and adds this
adapter plus its pinned allocator. It produces the same transaction export as
the CAR decode probe.
Its dependencies are intentionally separate from the production workspace.

Build on the test host with the same compiler and target settings as the CAR
probe:

```sh
RUSTFLAGS='-C target-cpu=native' cargo +1.98.0 build --release --locked \
  --manifest-path bench/jetstreamer-reference/Cargo.toml
```

The default `--reported-error-policy strict` rejects every upstream error
callback. The optional `--reported-error-policy body-transport` permits only
these exact messages:

- `Error reading until block: error decoding response body`
- `Error reading header: error decoding response body`

It permits at most two such errors for each worker and failed slot. Both error
types share this limit; a change from block to header does not reset it. The callback
worker identity and slot range must match the request. The worker must have no
pending transaction, current callback slot, or buffered export bytes. All other
errors stop the run.

Upstream reads the raw CAR header on each new stream, before the slot seek or
any block callback. It then reads a complete block before it calls transaction
handlers. Thus a permitted response-body error occurs before callbacks for
that block. Header length, format, and timeout errors remain fatal. The
adapter lets upstream reopen its stream and resume; it does not change upstream
code. A transaction decode error can occur after some callbacks and is never
permitted by this policy. The pinned Git revision also includes upstream's fix
for the block callback position after work stealing and connection restart.

The receipt separates permitted retries from fatal `errors`. It records the
policy, exact retry total, limit, first 64 retry events, and whether the event
list is truncated. Each event records worker, slot, attempt, elapsed time, and
the exact error. Upstream recycle, timeout, and work-steal counters are separate.
Retries and their backoff remain inside the reader time. The full-run controller
also imposes a finite process timeout.

Neither mode changes transaction order, duplicate block detection, callback
counts, the complete canonical plan, or the export contract. A recovered run
must pass the same native structure checks and full byte comparison. Historical
strict-mode failures remain failed evidence; they do not prove that normal
upstream retry behavior cannot complete the epoch.

The policy unit tests use only the standard library:

```sh
rustc --edition=2021 --test bench/jetstreamer-reference/src/retry_policy.rs \
  -o /tmp/jetstreamer-retry-policy-tests
/tmp/jetstreamer-retry-policy-tests
```

The separate HTTP fault test checks the actual adapter with a truncated CAR
response. It compares a clean export with a recovered export and checks that
strict mode rejects the same fault. Run it with the options listed by
`python3 bench/jetstreamer-reference/test_body_recovery.py --help`.
