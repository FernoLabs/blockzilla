import hashlib,json,os,shlex,sqlite3,subprocess,time
from contextlib import closing
from pathlib import Path
r=Path('/home/ubuntu/live/hivezilla-validator-20260930');report=r/'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6'
def read(p):return json.loads(p.read_text())
pin=read(r/'config/native-active-source.json');registry=read(r/'config/storage-jobs.json');jobs=read(r/'reports/storage-jobs/STATUS.json');capacity=read(r/'reports/native-capacity/STATUS.json');installation=read(report/'INSTALLATION.json')
assert hashlib.sha256((r/'config/storage-jobs.json').read_bytes()).hexdigest()==installation['registry_sha256']
assert jobs['state']=='HEALTHY' and not jobs['issues'] and jobs['registry_sha256']==installation['registry_sha256']
now=int(time.time()*1000);assert 0<=now-jobs['updated_unix_ms']<90000 and 0<=now-capacity['updated_unix_ms']<90000
out={'schema':'blockzilla-approved-extra-final-status-v1','sampled_unix_ms':now,'source':pin,'registry':registry,'registry_sha256':installation['registry_sha256'],'jobs':jobs,'capacity':capacity,'storage':{},'pids':{},'sender':read(r/'reports/foundation-watch/STATUS.json')}
assert len(registry['jobs'])==3
assert pin==installation['active_source']==jobs['active_source']==capacity['source']
assert hashlib.sha256((r/'config/native-active-source.json').read_bytes()).hexdigest()=='ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
assert capacity['schema']=='blockzilla-native-capacity-v1' and capacity['state']=='RUNNING'
assert {j['unit']:j['max_archived_banks'] for j in registry['jobs']}=={'hivezilla-native-cold-storage-20261012-live.service':None,'hivezilla-native-cold-storage-20261016-source-18de229d.service':785546,'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service':308768}
for job in registry['jobs']:
 output=Path(job['output']);state=output/'cold-storage';dbpath=state/'archive.sqlite'
 assert hashlib.sha256(Path(job['helper']).read_bytes()).hexdigest()==job['helper_sha256']
 assert hashlib.sha256(json.dumps(job['expected_command'],separators=(',',':'),ensure_ascii=False).encode()).hexdigest()==job['expected_command_sha256']
 unittext=subprocess.check_output(['systemctl','cat',job['unit']],text=True)
 starts=[line[10:] for line in unittext.splitlines() if line.startswith('ExecStart=') and line!='ExecStart='];assert shlex.split(starts[-1])==job['expected_command']
 with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
  db.execute('PRAGMA query_only=ON');db.execute('BEGIN');deadline=time.monotonic()+20;db.set_progress_handler(lambda:time.monotonic()>deadline,10000)
  meta=json.loads(db.execute('SELECT value FROM meta WHERE id=1').fetchone()[0]);assert meta=={'source':job['source'],'namespace':job['namespace']}
  count=db.execute('SELECT count(*) FROM archives').fetchone()[0];retired=db.execute('SELECT count(*) FROM archives WHERE retired=1').fetchone()[0]
 status=read(state/'STATUS.json');maximum=job['max_archived_banks']
 if maximum is not None:
  assert count==retired==maximum
  assert status['state']=='COMPLETE' and status['max_archived_banks']==maximum and status['pending_bundles']==0 and status['stop_reason']=='max_archived_banks'
 else:
  assert count>=retired>28871 and status['state']=='RUNNING' and 0<=int(time.time()*1000)-status['updated_unix_ms']<90000
 definition=subprocess.check_output(['systemctl','show',job['unit'],'-p','MainPID,ActiveState,Result,Restart,InvocationID'],text=True)
 out['storage'][job['unit']]={'source':job['source'],'namespace':job['namespace'],'archived_banks':count,'retired_banks':retired,'absolute_cap':maximum,'verified_helper_sha256':job['helper_sha256'],'unit_definition_sha256':hashlib.sha256(unittext.encode()).hexdigest(),'status':status,'service':dict(l.split('=',1) for l in definition.splitlines())}
 service=out['storage'][job['unit']]['service']
 if maximum is not None:assert service['ActiveState']=='inactive' and service['Result']=='success' and service['Restart']=='no'
 else:
  assert service['ActiveState']=='active' and int(service['MainPID'])>0 and service['Restart']=='no'
  assert (Path('/proc')/service['MainPID']/'cmdline').read_bytes().decode().rstrip('\0').split('\0')==job['expected_command']
native=['hivezilla-validator','hivezilla-direct-worker','hivezilla-bank-v2-producer','hivezilla-bank-v2-r2-publisher','hivezilla-shred-hook-recorder']
transports=['hivezilla-bank-v2-origin','hivezilla-stream-origin','hivezilla-stream-relay'];protected=native+transports
baseline=read(r/'reports/native-recovery-20261018/recurrence/protocol-retry/INSTALLATION.json')['protected_pids']
transport_path=r/'reports/native-recovery-20261018/recurrence/connection-idle/INSTALLATION.json'
transport_bytes=transport_path.read_bytes();transport=json.loads(transport_bytes);transport_sha256=hashlib.sha256(transport_bytes).hexdigest();binding=read(report/'TRANSPORT-PIDS.json')
assert transport['state']=='ACTIVE' and transport['source']==pin and transport['protected_native_pids_unchanged'] is True
assert transport['source_changed'] is False and transport['validator_restarted'] is False and transport['bank_limits_changed'] is False
assert binding['state']=='VERIFIED' and binding['source']==pin and binding['installation_sha256']==transport_sha256
expected=binding['protected_pids'];assert set(expected)==set(protected) and all(type(p) is int and p>0 for p in expected.values())
assert set(transport['protected_native_pids'])==set(native)
for u in native:assert int(transport['protected_native_pids'][u])==int(baseline[u])==expected[u]
transport_units={u+'.service' for u in transports}
assert set(transport['old_transport_pids'])==set(transport['transport_pids'])==transport_units
changes={}
for u in transports:
 unit=u+'.service';old=int(transport['old_transport_pids'][unit]);new=int(transport['transport_pids'][unit])
 assert old>0 and new>0 and old!=new and new==expected[u]
 changes[unit]={'old':old,'new':new}
for u in protected:
 pid=int(subprocess.check_output(['systemctl','show',u,'-p','MainPID','--value'],text=True).strip());assert pid==expected[u];out['pids'][u]=pid
out['protected_native_pids_unchanged']=True
out['transport_pid_changes']=changes
out['transport_installation']={'state':transport['state'],'sha256':transport_sha256,'release':transport['release'],'binary_sha256':transport['binary_sha256'],'source':transport['source']}
fs=os.statvfs(r);out['free_disk_bytes']=fs.f_bavail*fs.f_frsize;out['initial_free_disk_bytes']=read(report/'BEFORE.json')['free_disk_bytes'];out['net_free_space_change_bytes']=out['free_disk_bytes']-out['initial_free_disk_bytes'];out['net_free_space_change_is_cleanup_only']=False
out['snapshot_files']=[{'name':p.name,'bytes':p.stat().st_size,'allocated_bytes':p.stat().st_blocks*512} for p in sorted((r/'snapshots').iterdir()) if p.is_file() and not p.is_symlink()]
out['state']='PASSED';print(json.dumps(out,separators=(',',':')))
