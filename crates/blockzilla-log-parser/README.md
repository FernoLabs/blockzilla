# blockzilla-log-parser

`blockzilla-log-parser` classifies the top-level shape of Solana runtime log
lines while borrowing text from the input. Blockzilla uses those classifications
when building compact, byte-preserving log records.

The parser deliberately does not validate pubkeys, decode base64 payloads, or
interpret program-specific payload data. Unknown input remains available as
borrowed text rather than being discarded.

```rust
use blockzilla_log_parser::{ParsedLogLine, parse_line};

assert_eq!(
    parse_line("Program Example111111111111111111111111111111 success"),
    ParsedLogLine::Success {
        program: "Example111111111111111111111111111111",
    },
);
```

Run its tests and parser benchmark with:

```bash
cargo test --locked -p blockzilla-log-parser
cargo bench --locked -p blockzilla-log-parser --bench parse_line
```
