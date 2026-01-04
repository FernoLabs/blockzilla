# TODO

- Simplify error handling  
  - Single error type per crate  
  - Add context only at I/O and top-level boundaries  

- Handle base64 logs  
  - Build a `BytesTable` for base64-decoded log data  
  - Store multiple `StrId` entries for program data logs and return data (multiple base64 blobs per tx)  
  - Future: detect pubkeys inside instruction/return data and replace them with ids  

- Split archive data  
  - Separate data required for replay from runtime-only data  
  - Runtime-only includes logs, inner instructions, return data  

- Try new encodings  
  - Review compact encoding to remove unnecessary allocations and clones
  - Evaluate `wincode` for low-allocation streaming encoding  
  - Evaluate `rkyv` for zero-copy / archive-friendly layouts  

- wincode optimisation
  - can we use slice with shortu16 len decode ?