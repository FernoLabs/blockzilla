# Blockzilla v1 – Epoch 800 Storage & Compression Report

## 1. Source Data (Raw Ledger)

**epoch-800.car.zst**

Epoch 800 is a good benchmark, one of the biggest and packed with a lot of different tx.

- Uncompressed CAR size (reported): **768 GB**
- Compressed size: **309 GB**

## 2. Blockzilla Compact Format Output

**Directory:** `blockzilla-v1/epoch-800/`  
**Total size:** **469 GB**

| File | Size | Purpose |
|------|------|---------|
| `compact.bin` | 358 GB | Fully decoded compact transaction and metadata stream |
| `compact.bin.zst` | 111 GB | Zstd-compressed compact format |
| `registry.bin` | 1.2 GB | Global registry (programs, accounts, strings, etc.) |
| `blockhash_registry.bin` | 14 MB | Blockhash lookup registry |

### Key Ratios

- Raw CAR (uncompressed): **768 GB**
- Blockzilla compact (uncompressed): **358 GB**
- Blockzilla compact (compressed): **111 GB**

### Compression and Reduction

- Raw → Compact (uncompressed): **~2.14× smaller**
- Raw → Compact (compressed): **~6.9× smaller**
- Compact → Compact.zst: **~3.2× compression**

---

## 3. Transaction Dump Statistics

Text dumps generated during processing:

| File | Lines | Size |
|------|-------|------|
| `dumps-800.log` | 791,819,039 | 46 GB |
| `dumps-800-sorted.log` | 141,343,910 | 32 GB |

### Observations

- Sorting and deduplication reduced line count by **~82%**
- Disk size reduction is more modest at **~30%**, suggesting longer average lines after sorting or denser representations
- Confirms extremely high transaction and instruction cardinality for this epoch

---

## 4. High-Level Takeaways

- Epoch 800 raw ledger data is extremely large at **~768 GB uncompressed**
- Blockzilla's compact format reduces this to:
  - **358 GB** fully decoded and query-ready
  - **111 GB** when stored compressed
- This validates Blockzilla's core design goals:
  - Faster loading
  - Cheaper storage
  - Practical full-epoch history for indexing, analytics, and replay workloads
- Registry overhead remains negligible relative to transaction volume at **<0.5%**

## 5. Annex

### epoch 800 (commit a3c6f85ff99f8c105b40a463cc4dfe352ad7e703)

this is pre blockhash registry

```
blocks=430282
txs=681751357
metas_some=681751357
payload_bytes_total=399271823624
file_bytes_total_including_u32_prefix=399273544752

      10326768     0.00%  header
  187751700906    47.02%  tx
  211509795950    52.97%  meta
       1721128             frame_prefix_u32

tx_serialized_bytes=187751700906
instr_data_raw_bytes=91592322513
compactness(instr_data_raw / tx_serialized)=0.4878

   45972779389    24.49%  signatures
    2045254071     1.09%  msg.header
   21816043424    11.62%  msg.recent_blockhash
    8357598656     4.45%  msg.account_keys
  105060634692    55.96%  ix.container(total)
    9649807468     5.14%  ix.accounts
   93417933271    49.76%  ix.data(serialized)
    1992893953     1.06%  ix.overhead(approx)
    3817639317     2.03%  atl.container
    3671022193     1.96%  atl.payload

meta_bytes_total=211509795950
meta_logs_some=681749141
meta_log_lines=687731973
meta_log_events=6023357145
meta_logs_bytes_total=68022444220 (strings=21801878919 events_container=46220565301 events_sum=45538366939)

top LogEvent kinds by bytes:
    83782070    21555516309  Data
  1912118280     7834749058  Invoke
   942001668     7021430076  Consumed
  1856195993     3861214262  Success
  1069922494     3277269940  ProgramLog
    68129905     1614234863  Return
    53312189      228688796  FailureCustomProgramError
    31437925      125744068  Consumption
     2363518        8366780  Failure
     2926514        5853028  ProgramLogError
      381058        3692732  System
      424851         849702  Unparsed
      241205         510382  FailureInvalidAccountData
      113263         226526  Plain
        5375          18790  FailureInvalidProgramArgument
         445            890  UnknownAccount
         314            628  ProgramNotDeployed
          31             62  UnknownProgram
          47             47  CloseContextState
```

### Log duplicate may not be that important

```
$ wc -l dumps-800.log
791819039 dumps-800.log
$ wc -l dumps-800-sorted.log
141343910 dumps-800-sorted.log
$ ls -lh dumps-800*
-rw-r--r-- 1 ach admin 46G Jan  2 18:50 dumps-800.log
-rw-r--r-- 1 ach admin 32G Jan  2 19:34 dumps-800-sorted.log
```

### focus on base64 data and string dedup

33% encoding overhead
10Gb max
10Gb dedup
dedup across epoch may give insane result