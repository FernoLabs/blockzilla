# Solana log compression

Most of data from archive node is log data. Using structured log we succesfuly reduce the archive size by a lot.
But we relay on know program log. Can we generalise this to all logs.

Let say a program dump
* random message <pubk> : <u64> finalized

Can we auto detect the patern and understand the type.
Bassicaly run througth tokenizer.

