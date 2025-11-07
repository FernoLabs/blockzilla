# epoch 800 analyze

## On disique size almost div 2

-rw-r--r-- 1 ach admin 384G Nov  7 12:32 optimized/epoch-0800/optimized/blocks.bin
-rw-r--r-- 1 ach admin 1.7G Nov  7 02:37 registry/epoch-0800/fp2key.bin
-rw-r--r-- 1 ach admin 1.2G Nov  7 02:37 registry/epoch-0800/keys.bin
-rw-r--r-- 1 ach admin 768G Oct 22 23:29 /volume1/blockzilla/epoch-800.car

### CBOR encoded
-rw-r--r-- 1 ach admin 674G Nov  7 17:22 optimized/epoch-0800/optimized/blocks.bin

### wincode
-rw-r--r-- 1 ach admin 675G Nov  7 19:31 optimized/epoch-0800/optimized/blocks.bin

## what composed a compressed epoch

📊 Analysis Results:
  Epoch:                 800
  Total Blocks:          430282
  Total Transactions:    681751357
  Total Instructions:    1311142596
  Total Rewards:         0
  Total Block Bytes:     411936720945 (payload)
  Total Block Bytes:     411938442073 (payload + length prefix)
  Avg Block Payload:     957364.52 bytes
  Avg Block On-Disk:     957368.52 bytes
  Avg Tx per Block:      1584.43
  Avg Ixs per Block:     3047.17
  Avg Rewards per Block: 0.00
  Throughput (avg):      211904.72 tx/s | 133.74 blk/s

  Block field sizes:
    Slot                              2151410 bytes (  0.0%)
    Transactions                 411934139253 bytes (100.0%)
    Rewards                            430282 bytes (  0.0%)

  Transaction field sizes:
    Signatures                    46680451702 bytes ( 11.3%)
    Header                         2045254071 bytes (  0.5%)
    Account IDs                   10603143954 bytes (  2.6%)
    Recent blockhash              21816043424 bytes (  5.3%)
    Instructions                 105060634692 bytes ( 25.5%)
    Address table lookups          4731378478 bytes (  1.1%)
    Version flag                    681751357 bytes (  0.2%)
    Metadata field               220314621028 bytes ( 53.5%)

    Metadata usage:
      Compact metadata txs: 681751357
      Raw metadata txs:     0
      No metadata txs:      0
          • None (Option tag)               0 bytes (  0.0%)
          • Some tag overhead       681751357 bytes (  0.3%)
          • Compact payload      218951118314 bytes ( 99.4%)
          • Raw payload                     0 bytes (  0.0%)

    Compact metadata field sizes:
    Fee                            1394419198 bytes (  0.6%)
    Error                          1482316254 bytes (  0.7%)
    Pre balances                  25824204219 bytes ( 11.8%)
    Post balances                 25829807264 bytes ( 11.8%)
    Pre token balances            18171101173 bytes (  8.3%)
    Post token balances           18191563076 bytes (  8.3%)
    Loaded writable                5394959012 bytes (  2.5%)
    Loaded readonly                2463601308 bytes (  1.1%)
    Return data (Option)            978390858 bytes (  0.4%)
    Inner ixs (Option)            45116712828 bytes ( 20.6%)
    Log messages (Option)         74104043124 bytes ( 33.8%)
          • Return data payload     296639501 bytes (  0.1%)
          • Inner ixs payload     44434961471 bytes ( 20.3%)
          • Log stream            73422291767 bytes ( 33.5%)
        Log event bytes:   44373575324
        Log string bytes:  26808368180 (789104215 strings)
        Txs with logs:     681748456