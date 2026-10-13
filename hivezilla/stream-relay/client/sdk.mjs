import { StreamError, streamChunks } from './index.mjs';

const encoder = new TextEncoder();
const text = new TextDecoder('utf-8', { fatal: true });
const check = (ok, code) => { if (!ok) throw new StreamError(code); };
const MAX_OUTPUT = 32 * 1024 * 1024;

/** Load the shared Rust codec. Node can read a local file URL; browsers use HTTP. */
export async function loadDecoder(wasm = new URL('./decoder.wasm', import.meta.url)) {
  if (wasm instanceof URL || typeof wasm === 'string') {
    const url = new URL(wasm, import.meta.url);
    if (url.protocol === 'file:' && typeof process !== 'undefined' && process.versions?.node) {
      wasm = await (await import('node:fs/promises')).readFile(url);
    } else {
      const response = await fetch(url, { redirect: 'error' });
      check(response.ok, 'decoder_download_failed');
      wasm = await response.arrayBuffer();
    }
  }
  const result = await WebAssembly.instantiate(wasm, {});
  const instance = result instanceof WebAssembly.Instance ? result : result.instance;
  const api = instance.exports;
  check(api.memory instanceof WebAssembly.Memory && ['bz_alloc', 'bz_free', 'bz_decode']
    .every(name => typeof api[name] === 'function'), 'invalid_decoder_abi');
  return {
    decode(chunk, { programs = [], accounts = [], successfulOnly = false, includeDetails = false } = {}) {
      check(chunk.wire instanceof Uint8Array && chunk.wire.length <= 16 * 1024 * 1024 + 65552, 'invalid_decoder_input');
      check(Array.isArray(programs) && programs.length <= 256 && programs.every(p => typeof p === 'string')
        && Array.isArray(accounts) && accounts.length <= 256 && accounts.every(p => typeof p === 'string')
        && typeof successfulOnly === 'boolean' && typeof includeDetails === 'boolean', 'invalid_filter_options');
      const options = encoder.encode(JSON.stringify({ programs, accounts, successful_only: successfulOnly, include_details: includeDetails }));
      check(options.length <= 65536, 'filter_options_too_large');
      let input = 0, config = 0, output = 0, outputSize = 0;
      try {
        input = api.bz_alloc(chunk.wire.length);
        new Uint8Array(api.memory.buffer, input, chunk.wire.length).set(chunk.wire);
        config = api.bz_alloc(options.length);
        new Uint8Array(api.memory.buffer, config, options.length).set(options);
        const packed = api.bz_decode(input, chunk.wire.length, config, options.length);
        output = Number(packed & 0xffffffffn); outputSize = Number(packed >> 32n);
        check(outputSize > 0 && outputSize <= MAX_OUTPUT, 'decoded_output_limit_exceeded');
        const value = JSON.parse(text.decode(new Uint8Array(api.memory.buffer, output, outputSize)));
        if (value.error) throw new StreamError('compact_decode_failed', { reason: value.error, cursor: chunk.cursor });
        check(value.profile === chunk.envelope.profile && value.canonical === false && value.publicationReady === false
          && value.session === chunk.envelope.marker.session_id && value.epoch === chunk.envelope.marker.epoch
          && Array.isArray(value.blocks) && value.blocks.length === chunk.blocks.length, 'decoded_identity_mismatch');
        let transactions = 0;
        for (const [i, b] of value.blocks.entries()) {
          check(b.slot === chunk.blocks[i].slot && b.parentSlot === chunk.blocks[i].parent_slot && b.finality === (chunk.envelope.event ? 'processed' : 'finalized') && (!chunk.envelope.event || b.bankId === chunk.envelope.event.bank.bank_id)
            && Number.isSafeInteger(b.transactionCount) && b.transactionCount >= 0 && Array.isArray(b.transactions), 'decoded_identity_mismatch');
          transactions += b.transactionCount;
        }
        check(transactions === value.transactionCount && transactions === chunk.envelope.marker.transactions, 'decoded_transaction_count_mismatch');
        return value;
      } finally {
        if (output) api.bz_free(output, outputSize);
        if (config) api.bz_free(config, options.length);
        if (input) api.bz_free(input, chunk.wire.length);
      }
    },
    get memoryBytes() { return api.memory.buffer.byteLength; },
  };
}

/** One source, one chunk checkpoint, local filters. No server-side subscriptions. */
export function createClient({ url, apiKey, decoder, wasm, ...transport }) {
  check(typeof apiKey === 'string' && apiKey.length > 0, 'missing_api_key');
  const ready = decoder ? Promise.resolve(decoder) : loadDecoder(wasm);
  // Preserve a lazy initialization rejection without creating an unhandled promise.
  ready.catch(() => {});
  return {
    async *stream({ programs = [], accounts = [], successfulOnly = false, includeDetails = false,
      after, signal, mode = 'binary', onCheckpoint, onStatus } = {}) {
      const codec = await ready;
      for await (const chunk of streamChunks({ ...transport, url, apiKey, after, signal, mode, onCheckpoint, onStatus })) {
        const started = performance.now();
        const control = chunk.envelope.event && chunk.envelope.event.kind !== 'block';
        const value = control ? { profile: chunk.envelope.profile, session: chunk.envelope.marker.session_id, epoch: Math.floor(chunk.envelope.event.bank.slot / 432000), canonical: false, publicationReady: false, transactionCount: 0, blocks: [] } : codec.decode(chunk, { programs, accounts, successfulOnly, includeDetails });
        yield {
          ...value,
          kind: chunk.envelope.event?.kind ?? 'block',
          event: chunk.envelope.event,
          cursor: chunk.cursor, digest: chunk.digest, wireBytes: chunk.wire.length,
          decodeMilliseconds: performance.now() - started,
          matchedTransactions: value.blocks.reduce((n, b) => n + b.transactions.length, 0),
          // These remain chunk-local dictionaries, identified by this digest.
          dictionaries: {
            scope: chunk.digest,
            pubkeys: chunk.objects.get('registry.bin'),
            blockhashes: chunk.objects.get('blockhash_registry.bin'),
            previousBlockhashes: chunk.objects.get('prev_blockhash_tail.bin'),
            voteHashes: chunk.objects.get('vote_hash_registry.bin'),
          },
          resolvePubkey: reference => resolvePubkey(reference, chunk.objects.get('registry.bin')),
          ack: chunk.ack,
        };
      }
    },
  };
}

/** Resolve a compact key in this chunk's dictionary; no epoch-global ID claim. */
export function resolvePubkey(reference, dictionary) {
  let bytes;
  const id = typeof reference === 'number' ? reference : reference?.Id;
  if (id !== undefined) {
    check(Number.isSafeInteger(id) && id > 0 && dictionary instanceof Uint8Array
      && dictionary.length % 32 === 0 && id <= dictionary.length / 32, 'invalid_pubkey_reference');
    bytes = dictionary.subarray((id - 1) * 32, id * 32);
  } else {
    const raw = reference?.Raw;
    check(Array.isArray(raw) && raw.length === 32 && raw.every(n => Number.isInteger(n) && n >= 0 && n <= 255), 'invalid_pubkey_reference');
    bytes = new Uint8Array(raw);
  }
  const alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  let value = 0n;
  for (const byte of bytes) value = value * 256n + BigInt(byte);
  let encoded = '';
  while (value) { encoded = alphabet[Number(value % 58n)] + encoded; value /= 58n; }
  for (const byte of bytes) { if (byte) break; encoded = '1' + encoded; }
  return encoded;
}
