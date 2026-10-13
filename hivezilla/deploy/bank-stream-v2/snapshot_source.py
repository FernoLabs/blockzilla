#!/usr/bin/env python3
"""Create a provider code snapshot in an isolated remote checkout, never the dirty Mac checkout."""
import argparse,pathlib,subprocess,json,hashlib
p=argparse.ArgumentParser();p.add_argument('--base',type=pathlib.Path,required=True);a=p.parse_args();b=a.base;root=b/'provider';files=[]
# The provider graph is an isolated source copy. Explicit source suffixes exclude
# credentials, databases, capture payloads, logs, build trees, and benchmark inputs.
suffixes={'.rs','.toml','.lock','.md','.py','.mjs','.mts','.json','.sql','.html','.css','.js','.ts','.svg','.proto','.sh','.wasm','.patch','.service','.conf','.marker'}
for path in root.rglob('*'):
 if not path.is_file():continue
 rel=path.relative_to(root)
 if any(part in {'target','target-native','target-wasm','agave-source','.git','evidence','__pycache__','node_modules','artifacts','captures','compact','reports'} or part.startswith('._') for part in rel.parts):continue
 if path.suffix not in suffixes:continue
 # Synthetic SDK fixtures are the only encoded ledger fixtures exported in this task.
 if path.name.endswith('.key') or path.name.startswith('.env'):continue
 files.append(rel)
for path in (root/'hivezilla/stream-relay/client/fixtures/bank-v2').glob('*.bin'):files.append(path.relative_to(root))
files=sorted(set(files))
if not (root/'.git').exists():subprocess.run(['git','init','--initial-branch=codex/confirmed-bank-stream-v2'],cwd=root,check=True)
subprocess.run(['git','config','user.name','Codex'],cwd=root,check=True);subprocess.run(['git','config','user.email','codex@local'],cwd=root,check=True)
for offset in range(0,len(files),100):subprocess.run(['git','add','--']+[str(p) for p in files[offset:offset+100]],cwd=root,check=True)
subprocess.run(['git','commit','-m','Build opt-in native bank confirmation stream v2'],cwd=root,check=True)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip();manifest={str(p):hashlib.sha256((root/p).read_bytes()).hexdigest() for p in files};receipt={'provider_commit':revision,'snapshot_kind':'isolated complete provider code graph','upstream_base':'454ed418','file_count':len(files),'files':manifest}
previous=b/'PROVIDER-SOURCE.json'
if previous.exists():
 for key in ('native_plugin_source_commit','native_host_commit','producer_source_commit'):
  value=json.loads(previous.read_text()).get(key)
  if value is not None:receipt[key]=value
previous.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({k:v for k,v in receipt.items() if k!='files'}))
