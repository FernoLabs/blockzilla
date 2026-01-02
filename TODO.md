simplify error handling
review encoding to compact it is doing a bunch of allocation and clone

//first pass uncompress if needed
//do stuff (avoid zstd decompress x3)
//delete uncompressed (if was decompressed)
this weridly did not work as planed

build meta log BytesTable where we store bs64 decoded data
or at least store multiple Strid for Program data log and returned data as they can return multiple bs64
that will give us a better dedup and compression capability
- future optimisation may be intruction data pubk detection and replace with id.

Optimise registry builder by decoding up to log including zstd, so zstd stream decode up to log.