#!/usr/bin/env python3
"""Build portable v2 admission and export only synthetic SDK fixtures."""
import argparse,json,os,subprocess,time,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);a=p.parse_args();b=a.base;src=b/'provider';env=os.environ.copy()
env.update(CARGO_BUILD_JOBS='4',CARGO_TARGET_DIR='/home/ubuntu/benchmarks/hivezilla-stream-20261004/target',RUSTFLAGS='-C target-cpu=x86-64 -C target-feature=+aes,+sse2',BLOCKZILLA_BANK_FIXTURES=str(src/'hivezilla/stream-relay/client/fixtures/bank-v2'))
commands=[('hivezilla/stream-relay/decoder',['/home/ubuntu/.cargo/bin/cargo','+1.98.0','test','--offline','export_bank_sdk_fixtures','--','--ignored'])]
receipt={'state':'RUNNING','commands':[]}
for cwd,cmd in commands:
 print(cwd,'fixture export',flush=True);receipt['commands'].append({'cwd':cwd,'argv':cmd});subprocess.run(cmd,cwd=src/cwd,env=env,check=True)
wasm_env=env.copy();wasm_env.pop('RUSTFLAGS');wasm_env['CARGO_TARGET_DIR']='/home/ubuntu/benchmarks/hivezilla-stream-20261004/target-wasm';cmd=['/home/ubuntu/.cargo/bin/cargo','+1.98.0','build','--locked','--offline','--release','--target','wasm32-unknown-unknown']
receipt['commands'].append({'cwd':'hivezilla/stream-relay/decoder','argv':cmd});subprocess.run(cmd,cwd=src/'hivezilla/stream-relay/decoder',env=wasm_env,check=True)
shutil.copy2(Path(wasm_env['CARGO_TARGET_DIR'])/'wasm32-unknown-unknown/release/blockzilla_stream_decoder.wasm',src/'hivezilla/stream-relay/client/decoder-banks-v2.wasm')
node=shutil.which('node')
if node:subprocess.run([node,'--test','test.mjs','sdk-test.mjs','banks-test.mjs'],cwd=src/'hivezilla/stream-relay/client',check=True)
receipt.update(state='PASSED' if node else 'BUILT_PENDING_JS_TESTS',completed_unix_ms=int(time.time()*1000));(b/'CLIENT-BUILD.json').write_text(json.dumps(receipt,indent=2)+'\n')
