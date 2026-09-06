# Hivezilla

[The service](service/README.md) captures live input and retains recoverable data.
`protocol/` defines its messages. `object-store/` provides storage support.
`ledger-compat/` and `gossip-compat/` contain the Agave compatibility code used
by Hivezilla. Shared Solana codecs live in `../crates/solana-codec/`.
