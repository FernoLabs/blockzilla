# Blockzilla example workloads

This crate contains three small application rules and one transaction identity
dump sink. It does not open an archive.
A CAR, Compact V2, Indexer V3, or Jetstreamer example supplies records to one
sink. Equal output files, output counts, and coverage digests prove application
parity before a speed comparison. Compare the output files outside the timed
reader run.

The full comparison target has 12 distinct workload binaries: USDC, Pump.fun,
and FireWatch for each of the four readers. The Jetstreamer workload binaries
are code references and are excluded from final real-workload timings.
Transaction identity exporters and durable instruction-ledger binaries are
separate from this count.

The sinks do not stop when old history has missing metadata or CPI records.
They keep confirmed evidence. Each report marks the output as incomplete and
includes a SHA-256 digest of every affected transaction coordinate and reason
bit. A count alone cannot hide gaps at different positions.

## Workloads

- `UsdcBalanceSink` writes exact recorded pre- and post-token-balance rows for
  the selected mint. `mainnet()` selects Solana USDC.
- `PumpSink` writes one transaction record with its primary signature and its
  confirmed direct and CPI invocation counts. It does not filter by execution
  status. `mainnet()` selects Pump.fun.
- `FirewatchSink` writes the sorted, distinct program set reached by successful
  transactions for one required signer wallet.
- `TransactionIdentityDumpSink` writes every transaction coordinate and its
  required primary signature. It is a strict cross-format transaction identity
  parity output.

Each `*_scan_request` helper removes data planes that the workload does not
use. The format reader can then avoid unnecessary decoding and allocation.
Full instruction account projection is the default. Pump.fun and FireWatch
permit readers to omit unused instruction account lists. CAR, Compact V2, and
Indexer V3 avoid their resolution and allocation. The current Jetstreamer path
can still materialize the lists.

A speed result compares the complete application jobs. It includes source
access, decoding, projection, and sink work. It does not require equal internal
decoder work. A report must state each request projection. Require equal output
row and byte counts, byte-for-byte equal output files, and equal coverage counts
and digests.

## Canonical binary records

Each output starts with a 44-byte header:

| Offset | Bytes | Value |
| --- | ---: | --- |
| 0 | 8 | Workload magic and schema version |
| 8 | 4 | Big-endian fixed record size |
| 12 | 32 | Target mint, program, or wallet |

All integer fields in records use big-endian byte order. Optional public keys
use one presence byte and 32 value bytes. An absent value has a zero presence
byte and 32 zero bytes.

### USDC recorded balance: 136 bytes

`epoch:u64, slot:u64, tx_index:u32, side:u8, balance_index:u32,
account_index:u32, mint:[u8;32], owner:optional_pubkey,
token_program:optional_pubkey, amount:u64, decimals:u8`

The sink writes pre rows before post rows. It keeps source `balance_index`
values, even when the request selects only one mint.

### Pump.fun transaction: 92 bytes

`epoch:u64, slot:u64, tx_index:u32, primary_signature:[u8;64],
direct_count:u32, cpi_count:u32`

### FireWatch wallet-program relation: 64 bytes

`wallet:[u8;32], program:[u8;32]`

The program rows are unique and sorted by their 32-byte public key.

### Transaction identity dump: header 48 bytes, record 80 bytes

This stream uses little-endian integer fields. It has no target key. The caller
sets the epoch and the covered `[start_slot, end_slot_exclusive)` range when it
creates `TransactionIdentityDumpSink`.

Header:

`magic:[u8;8]="BZTXID01", schema_version:u32=1, header_length:u32=48,
record_length:u32=80, reserved:u32=0, epoch:u64, start_slot:u64,
end_slot_exclusive:u64`

Record:

`slot:u64, tx_index:u32, reserved:u32=0, primary_signature:[u8;64]`

The sink rejects a missing primary signature, a different epoch, a slot outside
the header range, a decreasing slot, and a transaction index that does not
start at zero for each slot and increase by one. Its finish report contains the
record count, total output bytes, SHA-256 of header plus records, and optional
first and last written slots.

The exact output size is `48 + 80 * records` bytes. For an epoch-900 dump, the
runner must calculate this value from the reported record count; the slot range
does not define a fixed transaction count.

Compare transaction identity dumps only when their record count, byte count,
output SHA-256, first slot, and last slot match.
