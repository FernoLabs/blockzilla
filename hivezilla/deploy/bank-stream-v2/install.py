#!/usr/bin/env python3
"""Install only tested immutable artifacts. Save all replaced units and config."""
import argparse,hashlib,importlib.util,json,shutil,subprocess,sys,time,shlex
from pathlib import Path

def run(*args):return subprocess.check_output(args,text=True).strip()
def write(path,data):
 path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix(path.suffix+'.new');tmp.write_text(data);tmp.chmod(0o600);tmp.replace(path)
def json_write(path,data):write(path,json.dumps(data,indent=2)+'\n')
def active(name):return run('systemctl','show',name,'-p','ActiveState','--value')=='active'
def exec_line(name):
 lines=run('systemctl','cat',name+'.service').splitlines();return [s.removeprefix('ExecStart=') for s in lines if s.startswith('ExecStart=') and s!='ExecStart='][-1]
def prepare_validator_command(previous_command,binary,config_path,helper_path=None):
 """Check the immutable binary and prepare bounded retention before any stop.

 Package deploy/direct-agave/snapshot_budget.py beside deploy/bank-stream-v2.
 Missing helper files or unsupported flags leave the running services in place.
 """
 helper_path=helper_path or Path(__file__).resolve().parent.parent/'direct-agave/snapshot_budget.py'
 spec=importlib.util.spec_from_file_location('_blockzilla_snapshot_budget',helper_path)
 if spec is None or spec.loader is None:raise RuntimeError('snapshot retention helper unavailable')
 helper=importlib.util.module_from_spec(spec);sys.modules[spec.name]=helper;spec.loader.exec_module(helper)
 args=shlex.split(previous_command)
 if not args:raise ValueError('empty previous validator command')
 args[0]=str(binary)
 args=helper.bounded_snapshot_args(args,run(str(binary),'--help'))
 while '--geyser-plugin-config' in args:
  i=args.index('--geyser-plugin-config')
  if i+1>=len(args) or args[i+1]!=str(config_path):raise ValueError('unknown plugin configuration')
  del args[i:i+2]
 if '--geyser-plugin-always-enabled' not in args:args.append('--geyser-plugin-always-enabled')
 return ' '.join(json.dumps(value) for value in args)
def validator_startup_command(helper_path,snapshot_directory):
 return ['/usr/bin/python3',str(helper_path),'--check-start','--snapshots',str(snapshot_directory)]
def install(path,text,report):
 backup=report/'saved'/str(path).lstrip('/');backup.parent.mkdir(parents=True,exist_ok=True)
 change=report/'changes'/str(path).lstrip('/')
 if not change.exists():
  if path.exists():shutil.copy2(path,backup)
  json_write(change,{'existed':path.exists()})
 src=report/'new'/str(path).lstrip('/');write(src,text);subprocess.run(['sudo','mkdir','-p',str(path.parent)],check=True);subprocess.run(['sudo','install','-m','644',str(src),str(path)],check=True)
def override(name,cmd,report,extra=''):
 install(Path('/etc/systemd/system')/(name+'.service.d/zz-bank-v2.conf'),'[Service]\nExecStart=\nExecStart='+cmd+'\n'+extra,report)
def unit(cmd,writable,after,memory='8G',cpu='400%'):
 return '[Unit]\nAfter='+after+'\n[Service]\nUser=ubuntu\nGroup=ubuntu\nUMask=0077\nExecStart='+cmd+'\nRestart=on-failure\nRestartSec=5\nMemoryMax='+memory+'\nCPUQuota='+cpu+'\nTasksMax=256\nNoNewPrivileges=true\nProtectSystem=strict\nProtectHome=read-only\nReadWritePaths='+writable+'\nPrivateTmp=true\nTimeoutStopSec=30\n[Install]\nWantedBy=multi-user.target\n'
def wait(predicate,seconds,label):
 until=time.monotonic()+seconds
 while time.monotonic()<until:
  if predicate():return
  time.sleep(0.5)
 raise RuntimeError(label)
def main(a):
 b=a.base;r=a.root;rev=json.loads((b/'HOST-SOURCE.json').read_text())['agave_revision']
 release=r/'releases/bank-stream-v2-20261010';report=r/'reports/bank-stream-v2-20261010'
 if a.resume:
  receipt=json.loads((report/'DEPLOYMENT.json').read_text());assert receipt['state'] in ['PREPARED','START_POST_FIXED'];assert receipt['agave_revision']==rev
  for name,expected in receipt['artifacts'].items():assert hashlib.sha256((release/name).read_bytes()).hexdigest()==expected
  old={}
  for name in ['hivezilla-validator','hivezilla-direct-worker','hivezilla-stream-origin','hivezilla-stream-relay','hivezilla-processed-stream-publisher']:
   lines=(report/(name+'.previous-unit')).read_text().splitlines();old[name]=[line.removeprefix('ExecStart=') for line in lines if line.startswith('ExecStart=') and line!='ExecStart='][-1]
  assert (r/'reports/bank-v2-native-load.json').exists(),'native load receipt required'
 else:
  for name in ['HOST-BUILD.json','NATIVE-BUILD.json','TRANSPORT-BUILD.json']:
   receipt=json.loads((b/name).read_text());assert receipt['state']=='PASSED',(name,receipt)
   if name!='TRANSPORT-BUILD.json':assert receipt['agave_revision']==rev,'artifact_revision_mismatch'
  assert json.loads((b/'native-load-canary/JOURNAL-CHECK.json').read_text())['state']=='PASSED'
  assert shutil.disk_usage(r).free>416*1024**3,'disk_reserve'
  release=r/'releases/bank-stream-v2-20261010';release.mkdir(exist_ok=False)
  paths={
   'agave-validator':r/'releases/agave-shred-hook-v4.3.0/target/release/agave-validator',
   'hivezilla-direct-agave':Path('/home/ubuntu/benchmarks/blockzilla-processed-stream-20261010/target-native-abi/release/hivezilla-direct-agave'),
   'libhivezilla_direct_agave.so':Path('/home/ubuntu/benchmarks/blockzilla-processed-stream-20261010/target-native-abi/release/libhivezilla_direct_agave.so'),
   'hivezilla-stream':Path('/home/ubuntu/benchmarks/hivezilla-stream-20261004/target/release/hivezilla-stream'),
   'bank-consumer':Path('/home/ubuntu/benchmarks/hivezilla-stream-20261004/target/release/bank-consumer'),
   'blockzilla':b/'blockzilla',
   'snapshot_budget.py':Path(__file__).resolve().parent.parent/'direct-agave/snapshot_budget.py'}
  for name,path in paths.items():shutil.copy2(path,release/name)
  hashes={name:hashlib.sha256((release/name).read_bytes()).hexdigest() for name in paths}
  report=r/'reports/bank-stream-v2-20261010';report.mkdir(exist_ok=False)
  names=['hivezilla-validator','hivezilla-direct-worker','hivezilla-stream-origin','hivezilla-stream-relay','hivezilla-processed-stream-publisher'];old={n:exec_line(n) for n in names}
  config_path=r/'config/direct-agave.json'
  validator=prepare_validator_command(old['hivezilla-validator'],release/'agave-validator',config_path,release/'snapshot_budget.py')
  startup_command=validator_startup_command(release/'snapshot_budget.py',r/'snapshots')
  run(*startup_command)
  for n in names:write(report/(n+'.previous-unit'),run('systemctl','cat',n+'.service')+'\n')
  config=json.loads(config_path.read_text());write(report/'previous-plugin-config.json',json.dumps(config,indent=2)+'\n')
  before={p.name for p in (r/'captures/direct-agave-live').iterdir() if p.is_dir()};old_pid=run('systemctl','show','hivezilla-validator','-p','MainPID','--value')
  receipt={'state':'PREPARED','agave_revision':rev,'artifacts':hashes,'release':str(release),'old_validator_pid':old_pid,'v1_contracts_preserved':True,'full_epoch_coverage':False,'snapshot_retention_args':['--maximum-full-snapshots-to-retain','1','--maximum-incremental-snapshots-to-retain','1']}
  json_write(report/'DEPLOYMENT.json',receipt)
  if a.prepare_only:print(json.dumps(receipt));return
  # Stop the ordered writer first; a new source session is explicit after restart.
  loader=release/'load_when_ready.py';shutil.copy2(Path(__file__).with_name('load_when_ready.py'),loader)
  subprocess.run(['sudo','systemctl','stop','hivezilla-direct-worker.service'],check=True)
  startup_line=' '.join(json.dumps(value) for value in startup_command)
  override('hivezilla-validator',validator,report,f'ExecStartPre=\nExecStartPre={startup_line}\nExecStartPost=\nExecStartPost=/usr/bin/python3 {loader} --root {r} --binary {release}/agave-validator --config {config_path}\nTimeoutStartSec=1800\nReadWritePaths={r}/reports\n')
  override('hivezilla-direct-worker',old['hivezilla-direct-worker'].replace('/releases/processed-worker-v4-20261010/hivezilla-direct-agave','/releases/bank-stream-v2-20261010/hivezilla-direct-agave').replace('/releases/processed-stream-v1-20261010/blockzilla','/releases/bank-stream-v2-20261010/blockzilla'),report)
  config['bank_v2']=True;config['libpath']=str(release/'libhivezilla_direct_agave.so');write(config_path,json.dumps(config,indent=2)+'\n')
  subprocess.run(['sudo','systemctl','daemon-reload'],check=True);subprocess.run(['sudo','systemctl','restart','hivezilla-validator.service'],check=True)
 session=None
 def capture_ready():
  nonlocal session
  loaded=json.loads((r/'reports/bank-v2-native-load.json').read_text())
  found=[p for p in (r/'captures/direct-agave-live').iterdir() if p.is_dir() and p.name==loaded['session_id'] and (p/'events.sqlite').exists() and not (p/'CLOSED.json').exists() and json.loads((p/'SESSION.json').read_text()).get('agave_revision')==rev]
  if len(found)==1:session=found[0];return True
  return False
 wait(capture_ready,1800,'native capture did not start; restore saved validator/config/worker before restart')
 output=r/'compact/direct-agave-processed-live'/session.name;subprocess.run(['sudo','systemctl','start','hivezilla-direct-worker.service'],check=True)
 def payload_ready():
  if not (output/'STATUS.json').exists():return False
  s=json.loads((output/'STATUS.json').read_text());assert s['state']!='FAILED',s
  return s.get('event_sequence',0)>=32
 wait(payload_ready,900,'native payload did not become ready')
 bank_output=r/'compact/bank-v2-live'/session.name;bank_output.mkdir(parents=True,exist_ok=False)
 source={k:json.loads((output/'FEED.json').read_text())[k] for k in ['genesis_hash','node_identity','agave_revision','session_id']};assert source['agave_revision']==rev
 source_file=report/'expected-source.json';json_write(source_file,source)
 keys=r/'config/stream-20261004';stream=release/'hivezilla-stream';engine=release/'hivezilla-direct-agave'
 if (report/'TRANSPORT-RELEASE.json').exists():
  transport=json.loads((report/'TRANSPORT-RELEASE.json').read_text());transport_release=Path(transport['release'])
  for name,digest in transport['artifacts'].items():assert hashlib.sha256((transport_release/name).read_bytes()).hexdigest()==digest
  stream=transport_release/'hivezilla-stream';receipt.update(transport_release=str(transport_release),transport_artifacts=transport['artifacts'])
 producer=unit(f'{engine} build-confirmed {output} {bank_output} --follow',f'{bank_output} {output}/feed.sqlite-shm','hivezilla-direct-worker.service','2G','200%')
 install(Path('/etc/systemd/system/hivezilla-bank-v2-producer.service'),producer,report)
 origin_spool=r/'bank-v2-origin-spool'/session.name;origin_spool.mkdir(parents=True)
 origin=unit(f'{stream} bank-origin --output {bank_output} --spool {origin_spool} --replay-mib 4096 --bind 127.0.0.1:8880 --token-file {keys}/origin-api.key --tls-cert {keys}/origin.crt --tls-key {keys}/origin.key --client-ca {keys}/ca.crt --max-requests 8',f'{origin_spool} {bank_output}/feed.sqlite-shm','hivezilla-bank-v2-producer.service')
 install(Path('/etc/systemd/system/hivezilla-bank-v2-origin.service'),origin,report)
 subprocess.run(['sudo','systemctl','daemon-reload'],check=True);subprocess.run(['sudo','systemctl','start','hivezilla-bank-v2-producer.service'],check=True)
 wait(lambda:(bank_output/'FEED.json').exists() and (bank_output/'feed.sqlite-shm').exists(),30,'v2 producer did not initialize');subprocess.run(['sudo','systemctl','start','hivezilla-bank-v2-origin.service'],check=True)
 # Keep old publication draining to R2. New session publication gets its own unit/checkpoint.
 old_output=old['hivezilla-stream-origin'].split('--output ',1)[1].split()[0]
 v1_origin=old['hivezilla-stream-origin'].replace(old_output,str(output))
 override('hivezilla-stream-origin',v1_origin,report,'ReadWritePaths=\nReadWritePaths='+str(output/'feed.sqlite-shm')+'\n')
 v1_spool=r/'stream-relay-spool-bank-v2'/session.name;v1_spool.mkdir(parents=True)
 bank_spool=r/'bank-v2-relay-spool'/session.name;bank_spool.mkdir(parents=True)
 relay=old['hivezilla-stream-relay'];old_spool=relay.split('--spool ',1)[1].split()[0];relay=relay.replace(relay.split()[0],str(stream),1).replace(old_spool,str(v1_spool))
 relay+=f' --banks-upstream https://127.0.0.1:8880 --banks-source-file {source_file} --banks-spool {bank_spool} --banks-replay-mib 4096'
 override('hivezilla-stream-relay',relay,report,f'ReadWritePaths=\nReadWritePaths={v1_spool} {bank_spool}\nMemoryMax=16G\nCPUQuota=400%\n')
 publisher_state=r/'reports/native-processed-r2-bank-v2'/session.name;publisher_state.mkdir(parents=True)
 publisher=old['hivezilla-processed-stream-publisher'].replace(old_output,str(output));old_state=publisher.split('--state ',1)[1].split()[0];publisher=publisher.replace(old_state,str(publisher_state))
 install(Path('/etc/systemd/system/hivezilla-bank-v2-r2-publisher.service'),unit(publisher,f'{publisher_state} {output}/feed.sqlite-shm','hivezilla-direct-worker.service','2G','400%'),report)
 subprocess.run(['sudo','systemctl','daemon-reload'],check=True);subprocess.run(['sudo','systemctl','restart','hivezilla-stream-origin.service'],check=True);time.sleep(2)
 subprocess.run(['sudo','systemctl','restart','hivezilla-stream-relay.service'],check=True);subprocess.run(['sudo','systemctl','start','hivezilla-bank-v2-r2-publisher.service'],check=True)
 subprocess.run(['sudo','systemctl','enable','hivezilla-bank-v2-producer.service','hivezilla-bank-v2-origin.service','hivezilla-bank-v2-r2-publisher.service'],check=True)
 receipt.update(state='INSTALLED_PENDING_PUBLIC_CHECK',source=source,session=str(session),processed_output=str(output),bank_output=str(bank_output),relay_spool=str(bank_spool),validator_pid=run('systemctl','show','hivezilla-validator','-p','MainPID','--value'),updated_unix_ms=int(time.time()*1000));json_write(report/'DEPLOYMENT.json',receipt);print(json.dumps(receipt))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);p.add_argument('--root',type=Path,required=True);p.add_argument('--prepare-only',action='store_true');p.add_argument('--resume',action='store_true');main(p.parse_args())
