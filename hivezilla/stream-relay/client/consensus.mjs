// Independent immutable footer facts. Existing v1/v2 execution readers stay separate.
import { StreamError } from './index.mjs';

export const CONSENSUS_PROFILE = 'native-consensus-fact-v1';
export const MAX_CONSENSUS_FACT_BYTES = 20 + 4096 + 65535;
const MAX_BINDING_BYTES = 4096, MAX_OUTPUT_BYTES = 1024 * 1024;
const encoder = new TextEncoder(), text = new TextDecoder('utf-8', { fatal: true });
const check = (ok, code, detail) => { if (!ok) throw new StreamError(code, detail); };
const sha = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
  .map(n => n.toString(16).padStart(2, '0')).join('');

/** Use the same strict Rust footer grammar in Node.js and the browser. */
export async function loadConsensusDecoder(wasm = new URL('./decoder-consensus-v1.wasm', import.meta.url)) {
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
  const api = (result instanceof WebAssembly.Instance ? result : result.instance).exports;
  check(api.memory instanceof WebAssembly.Memory
    && ['bz_alloc', 'bz_free', 'bz_decode_consensus'].every(k => typeof api[k] === 'function'),
  'invalid_consensus_decoder_abi');
  return {
    decode(wire, binding) {
      check(wire instanceof Uint8Array && wire.length >= 20 && wire.length <= MAX_CONSENSUS_FACT_BYTES,
        'invalid_consensus_fact_size');
      const options = encoder.encode(JSON.stringify(binding));
      check(options.length <= MAX_BINDING_BYTES, 'consensus_binding_too_large');
      let input = 0, opts = 0, output = 0, n = 0;
      try {
        input = api.bz_alloc(wire.length);
        new Uint8Array(api.memory.buffer, input, wire.length).set(wire);
        opts = api.bz_alloc(options.length);
        new Uint8Array(api.memory.buffer, opts, options.length).set(options);
        const packed = api.bz_decode_consensus(input, wire.length, opts, options.length);
        output = Number(packed & 0xffffffffn); n = Number(packed >> 32n);
        check(n > 0 && n <= MAX_OUTPUT_BYTES, 'consensus_output_limit_exceeded');
        const value = JSON.parse(text.decode(new Uint8Array(api.memory.buffer, output, n)));
        check(!value.error, 'consensus_decode_failed', { reason: value.error });
        check(value.profile === CONSENSUS_PROFILE && value.verification === 'captured_not_verified'
          && value.carrier_consensus_block_id === null, 'invalid_consensus_projection');
        return value;
      } finally {
        if (output) api.bz_free(output, n);
        if (opts) api.bz_free(opts, options.length);
        if (input) api.bz_free(input, wire.length);
      }
    },
    get memoryBytes() { return api.memory.buffer.byteLength; },
  };
}

/** Bind the whole fact to a trusted receipt and its execution payload. The
 * decoded certificate target can be older than the carrying block. This call
 * does not set a bank's confirmed/rooted state or verify BLS signatures. */
export async function decodeConsensusFact(wire, expectedDigest, binding, decoder) {
  check(wire instanceof Uint8Array && wire.length >= 20 && wire.length <= MAX_CONSENSUS_FACT_BYTES,
    'invalid_consensus_fact_size');
  check(typeof expectedDigest === 'string' && /^[a-f0-9]{64}$/.test(expectedDigest), 'invalid_consensus_digest');
  const copy = wire.slice(), expected = structuredClone(binding);
  const digest = await sha(copy);
  check(digest === expectedDigest, 'consensus_digest_mismatch');
  const decoded = decoder.decode(copy, expected);
  return { wire: copy, digest, decoded };
}
