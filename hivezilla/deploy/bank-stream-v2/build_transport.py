#!/usr/bin/env python3
import argparse,json,os,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);a=p.parse_args();b=a.base
src=b/'provider';env=os.environ.copy();env.update(CARGO_BUILD_JOBS='8',CARGO_TARGET_DIR='/home/ubuntu/benchmarks/hivezilla-stream-20261004/target',RUSTFLAGS='-C target-cpu=x86-64 -C target-feature=+aes,+sse2')
receipt={'state':'RUNNING','commands':[]}
for d in ['hivezilla/stream-relay','hivezilla/stream-relay/decoder','hivezilla/stream-relay/examples/rust-consumer']:
 for mode in ['test','build']:
  cmd=['/home/ubuntu/.cargo/bin/cargo','+1.98.0',mode,'--offline']+(['--release'] if mode=='build' else [])
  receipt['commands'].append({'cwd':d,'argv':cmd});log_path=b/(Path(d).name+'-'+mode+'.log')
  print(d,mode,flush=True)
  with log_path.open('w') as log:r=subprocess.run(cmd,cwd=src/d,env=env,stdout=log,stderr=subprocess.STDOUT)
  if r.returncode:
   receipt.update(state='FAILED',completed_unix_ms=int(time.time()*1000));(b/'TRANSPORT-BUILD.json').write_text(json.dumps(receipt,indent=2)+'\n');print(log_path.read_text()[-6500:]);raise SystemExit(r.returncode)
receipt.update(state='PASSED',completed_unix_ms=int(time.time()*1000));(b/'TRANSPORT-BUILD.json').write_text(json.dumps(receipt,indent=2)+'\n')
print('Transport, decoder and consumer checks passed',flush=True)
