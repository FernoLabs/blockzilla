# TODO

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

- wincode optimisation
  - can we use slice with shortu16 len decode ?

Optimize transaction error storage (u32 + u32 + potentail tuple)

make reqwest in reader optionel

## Backlog

- explore https://crates.io/crates/gxhash
- explore perfec hash function for registry (may reduce memory usage drastcly while keeping perf)
  - https://crates.io/crates/ph
- try reucing size of hashtable for registry by only storing half pubk (maybe faster compact / read)
- detect pubkeys inside instruction and log and replace them with ids  