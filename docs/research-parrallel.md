# Parrallel block producing

Car filer reading is slow.
Multi thread reader should not be necessary.

Bench read
 - read then build cid_map
   - hashmap Vs sorted array on block max 8k entry ?

Uvar int reader slow ?
* can we log and check if we always have 2 byte ?

Remove cid_map hashmap
replace with array (sorted)
remove entry once read (swap replace)
