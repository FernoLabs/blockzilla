export interface FilterOptions {
  programs?: string[];
  accounts?: string[];
  successfulOnly?: boolean;
  includeDetails?: boolean;
}
export interface Transaction {
  index: number;
  signature: string;
  signatures: string[];
  success: boolean | null;
  topLevelPrograms: string[];
  innerPrograms: string[];
  innerCoverage: 'recorded' | 'unavailable';
  accountKeys?: string[];
  /** Compact Rust schema values. Integers outside JS's exact range are decimal strings. */
  message?: unknown;
  metadata?: unknown;
}
export interface Block {
  slot: number;
  parentSlot: number;
  blockhash: string;
  parentBlockhash: string;
  bankId: number;
  finality: 'finalized' | 'processed';
  transactionCount: number;
  transactions: Transaction[];
}
export type PubkeyReference = number | { Id: number } | { Raw: number[] };
export interface DecodedBatch {
  profile: 'native-compact-projection-v1' | 'native-processed-compact-v1';
  kind?: 'block' | 'root' | 'invalidate';
  /** Bank IDs are local to the producer and capture session. */
  event?: { sequence: number; source_sequence: number; kind: 'block' | 'root' | 'invalidate';
    bank: { slot: number; bank_id: number; blockhash: number[]; parent_slot: number; parent_blockhash: number[] };
    payload_sha256: string; reason: string | null };
  epoch: number;
  session: string;
  canonical: false;
  publicationReady: false;
  transactionCount: number;
  matchedTransactions: number;
  blocks: Block[];
  cursor: string;
  digest: string;
  wireBytes: number;
  decodeMilliseconds: number;
  dictionaries: {
    scope: string;
    pubkeys?: Uint8Array;
    blockhashes?: Uint8Array;
    previousBlockhashes?: Uint8Array;
    voteHashes?: Uint8Array;
  };
  resolvePubkey(reference: PubkeyReference): string;
  /** Call only after durable processing. Required before advancing to the next batch. */
  ack(): Promise<void>;
}
export interface StreamOptions extends FilterOptions {
  after?: string;
  signal?: AbortSignal;
  mode?: 'binary' | 'sse';
  onCheckpoint?: (cursor: string) => void | Promise<void>;
  onStatus?: (status: unknown) => void | Promise<void>;
}
export interface CompactDecoder {
  decode(chunk: unknown, options?: FilterOptions): {
    profile: 'native-compact-projection-v1' | 'native-processed-compact-v1';
    kind?: 'block' | 'root' | 'invalidate';
  /** Bank IDs are local to the producer and capture session. */
  event?: { sequence: number; source_sequence: number; kind: 'block' | 'root' | 'invalidate';
    bank: { slot: number; bank_id: number; blockhash: number[]; parent_slot: number; parent_blockhash: number[] };
    payload_sha256: string; reason: string | null };
  epoch: number;
    session: string;
    canonical: false;
    publicationReady: false;
    transactionCount: number;
    blocks: Block[];
  };
  readonly memoryBytes: number;
}
export type WasmInput = string | URL | Uint8Array | ArrayBuffer | WebAssembly.Module;
export function loadDecoder(wasm?: WasmInput): Promise<CompactDecoder>;
export function resolvePubkey(reference: PubkeyReference, dictionary?: Uint8Array): string;
export function createClient(options: {
  url: string | URL;
  apiKey: string;
  decoder?: CompactDecoder;
  wasm?: WasmInput;
  fetchImpl?: typeof fetch;
  reconnect?: boolean;
}): { stream(options?: StreamOptions): AsyncGenerator<DecodedBatch> };
