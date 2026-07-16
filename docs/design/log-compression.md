# Generalized Solana log compression

Status: **research idea**.

Structured parsing of logs from known programs reduces Archive V2 size, but it
depends on program-specific parsers. This note asks whether repeated log shapes
can be discovered and encoded without knowing the program in advance.

For example, a program may repeatedly emit:

```text
random message <pubkey> : <u64> finalized
```

A generalized compressor could tokenize many samples, infer a stable template,
and encode each line as:

```text
template_id + typed(pubkey) + typed(u64)
```

## Questions to answer

- How many observations are required before a template is safe to use?
- How are ambiguous tokens distinguished from typed values?
- How is the original byte string reconstructed exactly?
- How are template versions scoped so one program upgrade cannot reinterpret
  older logs?
- When does the template dictionary cost more than ordinary zstd compression?
- How does a reader reject corrupt or adversarial templates with bounded work?

Any implementation must retain a raw fallback and prove byte-for-byte round
trips over a representative corpus. Until then, generalized templates remain a
research layer rather than part of the Archive V2 format contract.
