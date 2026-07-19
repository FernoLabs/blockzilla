# blockzilla-log-parser

`blockzilla-log-parser` classifies the top-level shape of Solana runtime log
lines while borrowing text from the input.

It does not validate pubkeys, decode base64, or interpret program-specific
payloads. Unknown input is preserved as borrowed text.

## Check

```bash
cargo test --locked -p blockzilla-log-parser
cargo bench --locked -p blockzilla-log-parser --bench parse_line
```
