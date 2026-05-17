# TODO

- reader benchmark
- compact benchmark

- handle block reward

- validation
  - compute blockhash
  - verify tx signature

- merge blockhash and registry builder

- String dedup in string table acrose epoch ?
  - try inside block first
  - accross epoch is harder as table is huge and may not fit in memory
  - maybe add a postprocessing on epoch or a new type foreignId or gloabalId

- Simplify error handling  
  - Single error type per crate  
  - Add context only at I/O and top-level boundaries  

- Split archive data  
  - Separate data required for replay from runtime-only data  
  - Runtime-only includes logs, inner instructions, return data  

- Try new encodings  
  - Review compact encoding to remove unnecessary allocations and clones
  - Evaluate `wincode` for low-allocation streaming encoding  
  - Evaluate `rkyv` for zero-copy / archive-friendly layouts  

- proper parser ?
  - current parser is hand rolled and suboptimal

Optimize transaction error storage (u32 + u32 + potentail tuple)
Make reqwest in reader optional

# Backlog

- Archive V2 durable nonce follow-up
  - Verify `OwnedCompactRecentBlockhash::Nonce` values against nonce-account state so raw recent-blockhash fallback cannot hide a parser/modeling mistake.
  - Add/extend nonce-account indexing; durable nonce accounts will likely need their own lookup path.

- explore pzstd
  - multiple frame decodeing to allow multithreading ?
  - one frame per block ? 
  - seekable zstd https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md

- cloudflare worker for rpc endpoints
  - get block
    - read index
    - read offest of epoch
    - return json encoded

- live archive producer
  - keep the live producer in its own deployable `blockzilla-live-producer` crate
  - build Blockzilla archive from our own feed first, not from CAR first
  - use Triton CAR as repair source for missed PoH entries and shredding data
  - keep regular RPC fallback visibly incomplete when PoH/shredding is missing
  - add normalized live block intake structs and completeness states
  - add shred stream sidecar for historical shred research
  - append blockhash and signature sidecars live
  - count pubkeys live from epoch start, then write ordered registry and repack at epoch close
