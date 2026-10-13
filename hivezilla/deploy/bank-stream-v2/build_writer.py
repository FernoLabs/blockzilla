#!/usr/bin/env python3
import argparse,os,shutil,subprocess,json,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);a=p.parse_args();b=a.base
rev=subprocess.check_output(['git','-C',str(b/'agave'),'rev-parse','HEAD'],text=True).strip();env=os.environ.copy();env.update(HIVEZILLA_AGAVE_REVISION=rev,CARGO_BUILD_JOBS='12',CARGO_TARGET_DIR='/home/ubuntu/benchmarks/blockzilla-processed-stream-20261010/target-native',RUSTFLAGS='-C target-cpu=x86-64 -C target-feature=+aes,+sse2')
cmd=['/home/ubuntu/.cargo/bin/cargo','+1.98.0','build','--offline','--release','-p','blockzilla']
with (b/'cli-build.log').open('w') as log:r=subprocess.run(cmd,cwd=b/'provider',env=env,stdout=log,stderr=subprocess.STDOUT)
if r.returncode:print((b/'cli-build.log').read_text()[-6500:]);raise SystemExit(r.returncode)
shutil.copy2(Path(env['CARGO_TARGET_DIR'])/'release/blockzilla',b/'blockzilla');print('Writer built for',rev,flush=True)
subprocess.run(['python3','-u',str(b/'provider/hivezilla/deploy/bank-stream-v2/build_native.py'),'--base',str(b)],check=True)
