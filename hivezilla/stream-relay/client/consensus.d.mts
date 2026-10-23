export const CONSENSUS_PROFILE: 'native-consensus-fact-v1';
export const MAX_CONSENSUS_FACT_BYTES: number;
export interface ConsensusBinding {
  source: { genesis_hash: string; node_identity: string; agave_revision: string; session_id: string };
  bank: { slot: number; bank_id: number; blockhash: number[]; parent_slot: number; parent_blockhash: number[] };
  execution_payload_sha256: string;
  source_event_sequence: number;
  source_event_sha256: string;
}
export interface CapturedAggregate { signature: number[]; bitmap: number[] }
export interface ConsensusProjection {
  profile: 'native-consensus-fact-v1';
  binding: ConsensusBinding;
  verification: 'captured_not_verified';
  carrier_consensus_block_id: null;
  footer_bytes: number;
  footer_sha256: string;
  footer: {
    native_schema: 'agave-4.3.2-versioned-block-footer-v1';
    verification: 'captured_not_verified';
    bank_hash: number[];
    /** Exact Unix nanoseconds. Use BigInt(value) for arithmetic. */
    block_producer_time_nanos: string;
    block_user_agent: number[];
    block_final_cert: null | {
      slot: string; block_id: number[];
      final_aggregate: CapturedAggregate; notar_aggregate: CapturedAggregate | null;
    };
    skip_reward_cert: null | { slot: string; aggregate: CapturedAggregate };
    notar_reward_cert: null | { slot: string; block_id: number[]; aggregate: CapturedAggregate };
  };
}
export interface ConsensusDecoder {
  decode(wire: Uint8Array, binding: ConsensusBinding): ConsensusProjection;
  readonly memoryBytes: number;
}
export function loadConsensusDecoder(wasm?: URL | string | BufferSource | WebAssembly.Module): Promise<ConsensusDecoder>;
export function decodeConsensusFact(wire: Uint8Array, expectedDigest: string, binding: ConsensusBinding,
  decoder: ConsensusDecoder): Promise<{ wire: Uint8Array; digest: string; decoded: ConsensusProjection }>;
