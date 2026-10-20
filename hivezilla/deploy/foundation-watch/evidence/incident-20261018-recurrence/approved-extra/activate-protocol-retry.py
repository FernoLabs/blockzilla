import hashlib, importlib.util, json, time
from pathlib import Path
root=Path('/home/ubuntu/live/hivezilla-validator-20260930')
report=root/'reports/native-recovery-20261018/recurrence/protocol-retry'
stage=json.loads((report/'STAGE.json').read_text());release=Path(stage['release'])
assert stage['state']=='STAGED_TESTED_NOT_ACTIVE' and release.parent==root/'releases'
assert hashlib.sha256((release/'MANIFEST.json').read_bytes()).hexdigest()==stage['manifest_sha256']
for n,h in stage['manifest'].items():
 assert Path(n).name==n and not (release/n).is_symlink()
 assert hashlib.sha256((release/n).read_bytes()).hexdigest()==h

def module(name,path):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
r=module('recovery',root/'releases/incident-resume-20261018-d25366e623ecead4/recover_session.py')
j=module('jobs',release/'storage_jobs.py')
live='hivezilla-native-cold-storage-20261012-live.service';guardian='hivezilla-native-storage-jobs.service'
registry_path=root/'config/storage-jobs.json';before=registry_path.read_bytes();registry=json.loads(before)
assert hashlib.sha256(before).hexdigest()=='0836d5da479c399eb99a5b5aad2c61b3e096378054449060de33b6d32b696442'
assert len(registry['jobs'])==2
oldjob=next(x for x in registry['jobs'] if x['max_archived_banks']==785546)
job=next(x for x in registry['jobs'] if x['unit']==live)
assert job['helper_sha256']=='5a320696a79da06a8f8e5ddd550e9d53da808b37119407c8708a44bd4c64c923'
assert hashlib.sha256(Path(job['helper']).read_bytes()).hexdigest()==job['helper_sha256']
pin=root/'config/native-active-source.json';pinbytes=pin.read_bytes();source=json.loads(pinbytes)
assert hashlib.sha256(pinbytes).hexdigest()=='ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
assert job['source']==source and job['max_archived_banks'] is None
oldcmd=job['expected_command'].copy();assert r.exec_args(r.run('sudo','systemctl','cat',live))==oldcmd
live_override=Path('/etc/systemd/system')/(live+'.d')/r.SOURCE_DROPIN
oldtext=r.run('sudo','cat',str(live_override))+'\n';assert r.exec_args(oldtext)==oldcmd
guardian_old=j.unit_command(r.run('sudo','systemctl','cat',guardian))
assert guardian_old[:3]==['/usr/bin/python3','-B',str(root/'releases/storage-protection-20261018-94c2aaca04758c49/storage_jobs.py')]
assert hashlib.sha256(Path(guardian_old[2]).read_bytes()).hexdigest()=='4d79f19af5f0c858e5e68214796480c27ba5cfffe35f6160b22a76f8a80db28e'
guardian_new=guardian_old.copy();guardian_new[2]=str(release/'storage_jobs.py')
guardian_override=Path('/etc/systemd/system')/(guardian+'.d')/'zz-protocol-retry-20261018.conf'
assert not guardian_override.exists()
protected=['hivezilla-validator','hivezilla-direct-worker','hivezilla-bank-v2-producer','hivezilla-bank-v2-origin','hivezilla-stream-origin','hivezilla-stream-relay','hivezilla-bank-v2-r2-publisher','hivezilla-shred-hook-recorder']
def pids():return {u:r.run('systemctl','show',u,'-p','MainPID','--value') for u in protected}
pidbefore=pids();assert pidbefore['hivezilla-validator']=='2142808' and all(int(p)>0 for p in pidbefore.values())
state=Path(job['output'])/'cold-storage'
r.write(report/'FAILURE-BEFORE.json',(state/'LAST-FAILURE.json').read_text())
failure=json.loads((state/'LAST-FAILURE.json').read_text())
assert failure['source']==source and failure['error_type']=='ResponseStreamingError'
assert failure['invocation_id']==r.run('systemctl','show',live,'-p','InvocationID','--value')
assert r.run('systemctl','show',live,'-p','ActiveState','--value')=='failed'
newcmd=oldcmd.copy();newcmd[1]=str(release/'cold_storage.py')
job.update(helper=newcmd[1],helper_sha256=stage['manifest']['cold_storage.py'],expected_command=newcmd,expected_command_sha256=j.command_sha256(newcmd));j.validate_job(root,job)
assert oldjob==next(x for x in json.loads(before)['jobs'] if x['max_archived_banks']==785546)
lines=oldtext.splitlines();at=[i for i,l in enumerate(lines) if l.startswith('ExecStart=') and l!='ExecStart='];assert len(at)==1
lines[at[0]]='ExecStart='+r.systemd_command(newcmd)
newtext='\n'.join(lines)+'\n'
installation=report/'INSTALLATION.json';assert not installation.exists()
r.write(report/'previous-registry.json',before.decode());r.write(report/'previous-live-override.conf',oldtext)
record=dict(state='PREPARED',started_unix_ms=int(time.time()*1000),release=str(release),source=source,protected_pids=pidbefore,old_command=oldcmd,command=newcmd,guardian_command=guardian_new,manifest=stage['manifest'],tests=stage['test_output'],bank_caps_changed=False)
r.json_write(installation,record)
def unchanged():
 assert pin.read_bytes()==pinbytes and pids()==pidbefore

def running(unit,command):
 for _ in range(120):
  pid=r.run('systemctl','show',unit,'-p','MainPID','--value')
  if int(pid)>0:
   try:
    if (Path('/proc')/pid/'cmdline').read_bytes().decode().rstrip('\0').split('\0')==command:return pid
   except FileNotFoundError:pass
  time.sleep(.25)
 raise RuntimeError('process_command_unavailable')

def healthy(since,digest):
 for _ in range(350):
  data=json.loads((root/'reports/storage-jobs/STATUS.json').read_text())
  if data['state']=='HEALTHY' and data['registry_sha256']==digest and data['updated_unix_ms']>=since:return data
  time.sleep(.2)
 raise RuntimeError('guardian_not_healthy')
try:
 unchanged();r.run('sudo','systemctl','stop',guardian,live)
 r.install(report,live_override,newtext)
 r.install(report,guardian_override,'[Service]\nExecStart=\nExecStart='+r.systemd_command(guardian_new)+'\n')
 r.json_write(registry_path,registry);r.run('sudo','systemctl','daemon-reload');unchanged()
 r.run('sudo','systemctl','start',live);pid=running(live,newcmd)
 r.run('sudo','systemctl','start',guardian);gpid=running(guardian,guardian_new)
 status=healthy(record['started_unix_ms'],hashlib.sha256(registry_path.read_bytes()).hexdigest());unchanged()
 record.update(state='ACTIVE',completed_unix_ms=int(time.time()*1000),live_pid=pid,guardian_pid=gpid,storage_jobs=status,protected_pids_unchanged=True,registry_sha256=hashlib.sha256(registry_path.read_bytes()).hexdigest())
 r.json_write(installation,record);print(json.dumps(record))
except Exception as error:
 record.update(state='ACTIVATION_FAILED',error_type=type(error).__name__);r.json_write(installation,record)
 try:
  r.run('sudo','systemctl','stop',guardian,live);unchanged()
  r.install(report,live_override,oldtext)
  if guardian_override.exists():r.run('sudo','rm','--',str(guardian_override))
  r.write(registry_path,before.decode());r.run('sudo','systemctl','daemon-reload');since=int(time.time()*1000)
  r.run('sudo','systemctl','start',live);rollback_pid=running(live,oldcmd)
  r.run('sudo','systemctl','start',guardian);running(guardian,guardian_old)
  restored=healthy(since,hashlib.sha256(before).hexdigest());unchanged()
  record.update(rollback='PREVIOUS_HELPER_VERIFIED',rollback_pid=rollback_pid);r.json_write(installation,record)
 except Exception as rollback_error:
  record.update(state='ROLLBACK_FAILED',rollback_error_type=type(rollback_error).__name__);r.json_write(installation,record)
  raise RuntimeError('activation_and_rollback_verification_failed') from None
 raise RuntimeError('activation_failed_previous_helper_restored') from None
