// Native canary: identical processed frames over binary HTTP and SSE with GET.
import {readFile,writeFile} from 'node:fs/promises';
import assert from 'node:assert/strict';
import {loadDecoder,createClient} from '../client/sdk.mjs';
const [,,url,keyPath,wasmPath,reportPath]=process.argv;
const apiKey=(await readFile(keyPath,'utf8')).trim();
const headers={Authorization:`Bearer ${apiKey}`};
const head=await(await fetch(new URL('/v1/head',url),{headers})).json();
assert.equal(head.profile,'native-processed-compact-v1');assert.equal(head.state,'LIVE');
const after=head.latest_cursor;
const decoder=await loadDecoder(await readFile(wasmPath));
const client=createClient({url,apiKey,decoder,reconnect:false});
const runs={};let firstBlock;
// Cloudflare Tunnel can buffer an open binary response. Public checks use
// SSE and finite binary GETs; the direct canary also checks open binary HTTP.
const modes=process.env.BLOCKZILLA_FINITE_HTTP_ONLY ? ['sse','sse'] : ['binary','sse'];
for(const [run,mode] of modes.entries()) {
 const rows=[];
 for await(const batch of client.stream({after,mode,signal:AbortSignal.timeout(45000)})) {
  assert.equal(batch.profile,'native-processed-compact-v1');
  if(batch.kind==='block') {assert.equal(batch.blocks.length,1);assert.equal(batch.blocks[0].finality,'processed');firstBlock??=batch.blocks[0];}
  else {assert.equal(batch.blocks.length,0);assert.equal(batch.transactionCount,0);assert.equal(batch.dictionaries.pubkeys,undefined);}
  rows.push({cursor:batch.cursor,kind:batch.kind,bank:batch.event.bank,transactions:batch.transactionCount,bytes:batch.wireBytes});
  await batch.ack();if(rows.length===16)break;
 }
 assert.equal(rows.length,16);runs[run]=rows;
}
assert.deepEqual(runs[0],runs[1]);assert.ok(firstBlock);
assert.ok(runs[0].some(r=>r.kind==='root'));
// A fresh client continues at the exact acknowledged event, including controls.
for await(const b of client.stream({after:runs[0][0].cursor,mode:'sse',signal:AbortSignal.timeout(15000)})) {
 assert.equal(b.cursor,runs[0][1].cursor);await b.ack();break;
}
let rpc;
if(process.env.BLOCKZILLA_LOCAL_RPC) {
 // A processed bank can precede the RPC's finalized view. Wait only for this
 // independent comparison; delivery itself must not wait for finality.
 let result;const deadline=Date.now()+30000;
 do {
  result=await(await fetch(process.env.BLOCKZILLA_LOCAL_RPC,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'getBlock',params:[firstBlock.slot,{commitment:'finalized',transactionDetails:'signatures',rewards:false,maxSupportedTransactionVersion:0}]})})).json();
  if(result.result)break;
  await new Promise(resolve=>setTimeout(resolve,500));
 }while(Date.now()<deadline);
 assert.ok(result.result,JSON.stringify(result.error));const block=result.result;
 assert.equal(block.blockhash,firstBlock.blockhash);assert.equal(block.previousBlockhash,firstBlock.parentBlockhash);
 assert.equal(block.parentSlot,firstBlock.parentSlot);assert.equal(block.signatures.length,firstBlock.transactionCount);
 assert.deepEqual(block.signatures,firstBlock.transactions.map(t=>t.signature));
 rpc={slot:firstBlock.slot,transactions:firstBlock.transactionCount,blockhash:firstBlock.blockhash,orderedSignatures:'PASSED'};
}
await writeFile(reportPath,JSON.stringify({state:'PASSED',profile:head.profile,runs,rpc})+'\n');
console.log(JSON.stringify({state:'PASSED',events:runs[0].length,rpc}));
