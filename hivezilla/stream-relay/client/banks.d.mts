import type { Transaction, WasmInput } from './sdk.mjs';
export interface SourceIdentity { genesis_hash:string; node_identity:string; agave_revision:string; session_id:string }
export interface BankIdentity { slot:number; bank_id:number; blockhash:number[]; parent_slot:number; parent_blockhash:number[] }
export interface RecentBlockhash { blockhash:string; context_slot:number; last_valid_block_height:number }
export interface BankEvent { sequence:number; source_sequence:number; kind:'block'|'confirmed'|'root'|'invalidate'; bank:BankIdentity; payload_sha256:string; payload_frame_sha256:string|null; commitment:'processed'|'confirmed'|'finalized'|null; confirmation_basis:string|null; block_height:number|null; recent_blockhash:RecentBlockhash|null; observed_at_unix_ms:number; reason:string|null }
export interface ConfirmedObservation { bank:BankIdentity; block_height:number; payload_sha256:string; payload_frame_sha256:string; recent_blockhash:RecentBlockhash; observed_at_unix_ms:number; commitment:'confirmed'|'finalized'; event_sequence:number; event_cursor:string }
export interface BankHead { schema:'blockzilla-confirmed-head-v1'; profile:'native-bank-compact-v2'; source:SourceIdentity; cursor:string|null; earliest_after:string|null; status:'starting'|'live'|'stale'|'failed'; sampled_at_unix_ms:number; source_observed_at_unix_ms:number|null; source_lag_ms:number|null; latest_confirmed:ConfirmedObservation|null }
export interface BankTransaction extends Transaction { /** Non-null for failed execution. */ err:unknown|null }
export interface BankDecoded { profile:'native-bank-compact-v2'; event:BankEvent; blocks?:Array<{ slot:number; blockHeight:number; parentSlot:number; blockhash:string; parentBlockhash:string; bankId:number; finality:'processed'; transactionCount:number; transactions:BankTransaction[] }> }
export interface BankFrame { wire:Uint8Array; digest:string; cursor:string; event:BankEvent; decoded:BankDecoded }
export interface BankDecoder { decode(wire:Uint8Array,options?:{includeDetails?:boolean}):BankDecoded; validateHead(head:unknown,source:SourceIdentity,now?:number):BankHead; readonly memoryBytes:number }
export function loadBankDecoder(wasm?:WasmInput):Promise<BankDecoder>;
export function bankCursor(cursor:string):{session:string;sequence:number;digest:string};
export function decodeBankFrame(wire:Uint8Array,digest:string,source:SourceIdentity,decoder:BankDecoder):Promise<BankFrame>;
export function createBankClient(options:{url:string|URL;apiKey:string;source:SourceIdentity;decoder?:BankDecoder;wasm?:WasmInput;fetchImpl?:typeof fetch;reconnect?:boolean;wallClock?:()=>number;monotonicClock?:()=>number}):{
  readyHash():RecentBlockhash & {block_height:number;commitment:'confirmed'|'finalized'};
  stream(options?:{after?:string;signal?:AbortSignal;onBootstrap?:(value:{after:string;head:BankHead;control:BankFrame;payload:BankFrame})=>void|Promise<void>;onCheckpoint?:(cursor:string)=>void|Promise<void>;onStatus?:(head:BankHead)=>void|Promise<void>}):AsyncGenerator<BankFrame & {dependency:BankFrame|null;ack():Promise<void>}>;
};
