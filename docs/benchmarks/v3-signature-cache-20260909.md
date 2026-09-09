# V3 sealed-epoch signature cache

Date: 2026-09-09

Host: Blockzilla NAS, 12 reader workers

Source: epoch 900 over the public sample gateway

The test compares the normal V3 network reader with the same reader after one
complete `signatures.bin` download. The persistent cache binds the file to its
exact length and strong ETag. It publishes the local file only after all ranges
finish.

The one-time 32,380,385,536-byte signature download took 73.93 seconds. Sixteen
concurrent 64 MiB requests gave 438.0 MB/s.

| Workload | Blocks | Signature mode | Scan time | TPS | Network bytes |
|---|---:|---|---:|---:|---:|
| Transaction identities | 32,768 | Network ranges | 31.43 s | 1,134,969 | 3.54 GB |
| Transaction identities | 32,768 | Local epoch cache | 11.40 s | 3,130,242 | 1.12 GB |
| Pump.fun | 4,096 requested; 3,997 candidates | Network ranges | 50.39 s | 86,040 | 554 MB |
| Pump.fun | 4,096 requested; 3,997 candidates | Local epoch cache | 44.82 s | 96,743 | 262 MB |

The transaction workload is 2.76 times faster and uses 68.4% fewer network
bytes. It needs one primary signature for every output transaction. The
Pump.fun workload is 12.4% faster and uses 52.7% fewer network bytes. It needs
signatures only for matched transactions, so a future sparse signature reader
can remove more work without a complete epoch cache.

The output identities and coverage results are equal between each pair. USDC
is not a signature workload. Its request already disables instructions and
primary signatures, so the signature cache correctly has no effect on it.
