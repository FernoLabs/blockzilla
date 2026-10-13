#!/usr/bin/env python3
import argparse,json,os,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);a=p.parse_args();b=a.base;host=b/'agave';src=b/'provider'
env=os.environ.copy();revision=subprocess.check_output(['git','-C',str(host),'rev-parse','HEAD'],text=True).strip();env.update(HIVEZILLA_AGAVE_REVISION=revision,CARGO_BUILD_JOBS='12',CARGO_TARGET_DIR='/home/ubuntu/benchmarks/blockzilla-processed-stream-20261010/target-native-abi',RUSTFLAGS='-C target-cpu=x86-64 -C target-feature=+aes,+sse2')
patch=[]
for name,directory in [('agave-geyser-plugin-interface','geyser-plugin-interface'),('solana-transaction-status','transaction-status'),('solana-transaction-status-client-types','transaction-status-client-types')]:patch+=['--config',f'patch."https://github.com/anza-xyz/agave".{name}.path="{host/directory}"']
with (b/'plugin-release.log').open('w') as log:r=subprocess.run(['/home/ubuntu/.cargo/bin/cargo','+1.97.1','build','--offline','--ignore-rust-version','--release','--features','bank-v2',*patch],cwd=src/'hivezilla/direct-agave',env=env,stdout=log,stderr=subprocess.STDOUT)
if r.returncode:print((b/'plugin-release.log').read_text()[-6500:]);raise SystemExit(r.returncode)
plugin=Path(env['CARGO_TARGET_DIR'])/'release/libhivezilla_direct_agave.so'
t=b/'native-load-canary';t.mkdir(exist_ok=True)
config={'bank_v2':True,'libpath':str(plugin),'output_root':str(t/'capture'),'genesis_hash':'5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d','node_identity':'J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ','queue_events':1024,'queue_bytes':8*1024*1024,'max_event_bytes':1024*1024,'max_journal_bytes':64*1024*1024,'reserve_disk_bytes':1024*1024,'commit_interval_ms':2}
(t/'config.json').write_text(json.dumps(config));env.update(HIVEZILLA_NATIVE_TEST_CONFIG=str(t/'config.json'),HIVEZILLA_NATIVE_TEST_RECEIPT=str(t/'NATIVE-LOAD.json'),CARGO_TARGET_DIR='/home/ubuntu/live/hivezilla-validator-20260930/releases/agave-shred-hook-v4.3.0/target')
cmd=['/home/ubuntu/.cargo/bin/cargo','+1.97.1','test','--offline','--release','-p','solana-geyser-plugin-manager','--features','agave-unstable-api','--lib','native_bank_v2_actual_plugin_load','--','--ignored','--nocapture']
print('Loading actual plugin through the patched host test',flush=True)
with (b/'native-load-test.log').open('w') as log:r=subprocess.run(cmd,cwd=host,env=env,stdout=log,stderr=subprocess.STDOUT)
if r.returncode:print((b/'native-load-test.log').read_text()[-7500:]);raise SystemExit(r.returncode)
print((t/'NATIVE-LOAD.json').read_text(),flush=True)
with (b/'native-host-build.log').open('w') as log:r=subprocess.run(['/home/ubuntu/.cargo/bin/cargo','+1.97.1','build','--offline','--release','--bin','agave-validator'],cwd=host,env=env,stdout=log,stderr=subprocess.STDOUT)
receipt={'state':'PASSED' if r.returncode==0 else 'FAILED','agave_revision':revision,'updated_unix_ms':int(time.time()*1000),'load_receipt':str(t/'NATIVE-LOAD.json'),'command':cmd};(b/'HOST-BUILD.json').write_text(json.dumps(receipt,indent=2)+'\n')
if r.returncode:print((b/'native-host-build.log').read_text()[-6500:]);raise SystemExit(r.returncode)
print('Exact host and plugin pair passed',flush=True)
