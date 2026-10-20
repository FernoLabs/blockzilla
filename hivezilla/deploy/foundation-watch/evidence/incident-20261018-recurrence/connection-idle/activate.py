import hashlib,importlib.util,json,subprocess,time
from pathlib import Path
root=Path('/home/ubuntu/live/hivezilla-validator-20260930');report=root/'reports/native-recovery-20261018/recurrence/connection-idle';build=json.loads((report/'BUILD.json').read_text())
assert build['state']=='READY_NOT_ACTIVE' and build['tests_passed'] is True and build['tests_failed']==0 and build['tests_count']>=36
assert build['red_comparison_passed'] is True
assert build['patch_sha256']=='2f3cbe5eb0c7e52180d8a072a5e82a9e1db71b271d46fcfaacadc2110fae6b95'
assert build['donor_manifest_sha256']=='0099305fcfc27cb0ef00ec684260909636867c43ee436e0863f56772f2044dd4'
release=Path(build['release']);binary=Path(build['binary']);assert release.parent==root/'releases' and binary==release/'hivezilla-stream'
assert release.resolve(strict=True)==release and not binary.is_symlink() and hashlib.sha256(binary.read_bytes()).hexdigest()==build['binary_sha256']
assert hashlib.sha256((release/'MANIFEST.json').read_bytes()).hexdigest()==build['release_manifest_sha256']
assert hashlib.sha256((release/'SOURCE.json').read_bytes()).hexdigest()==build['source_manifest_sha256']
manifest=json.loads((release/'MANIFEST.json').read_text())
assert manifest['schema']=='blockzilla-bank-connection-idle-release-v1' and manifest['binary_sha256']==build['binary_sha256']
for name,entry in manifest['files'].items():
 assert Path(name).name==name and not (release/name).is_symlink()
 body=(release/name).read_bytes();assert len(body)==entry['bytes'] and hashlib.sha256(body).hexdigest()==entry['sha256']
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
m=module('recovery',root/'releases/incident-resume-20261018-d25366e623ecead4/recover_session.py')
v=module('verify',root/'releases/incident-recovery-20261018-a6b89ac28c538eaa/verify_live.py')
units=['hivezilla-bank-v2-origin.service','hivezilla-stream-origin.service','hivezilla-stream-relay.service']
old_hashes={units[0]:'e769871d519750a187814a82e1a1f3e48cfc9bde0948410674a70dca7c97d1b7',units[1]:'abd32e4c935caa8e307b0a2e2ec408acd49bf7ff2641f47a8a784140fdc480cd',units[2]:'1bfc4577e5090d2a073a7114a0dd086f6397f4205e0909fcd20cc701d79846ae'}
pinpath=root/'config/native-active-source.json';pin=pinpath.read_bytes();source=json.loads(pin);assert hashlib.sha256(pin).hexdigest()=='ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
registry=root/'config/storage-jobs.json';registrybytes=registry.read_bytes();assert hashlib.sha256(registrybytes).hexdigest()=='c813c9e2accfb857eba1bf331f60af616ddfc8084b7aa2bce276ebf1fdb0c4c3'
protected=['hivezilla-validator','hivezilla-direct-worker','hivezilla-bank-v2-producer','hivezilla-bank-v2-r2-publisher','hivezilla-shred-hook-recorder']
def pids():return {u:m.run('systemctl','show',u,'-p','MainPID','--value') for u in protected}
pidbefore=pids();assert pidbefore['hivezilla-validator']=='2142808' and all(int(p)>0 for p in pidbefore.values())
assert v.rpc('getHealth')=='ok'
oldcmd={};newcmd={};oldtext={};override={};oldpids={}
for u in units:
 oldcmd[u]=m.exec_args(m.run('sudo','systemctl','cat',u));override[u]=Path('/etc/systemd/system')/(u+'.d')/m.SOURCE_DROPIN
 oldtext[u]=m.run('sudo','cat',str(override[u]))+'\n';assert m.exec_args(oldtext[u])==oldcmd[u]
 assert hashlib.sha256(Path(oldcmd[u][0]).read_bytes()).hexdigest()==old_hashes[u]
 pid=m.run('systemctl','show',u,'-p','MainPID','--value');assert int(pid)>0 and (Path('/proc')/pid/'cmdline').read_bytes().decode().rstrip('\0').split('\0')==oldcmd[u];oldpids[u]=pid
 newcmd[u]=[str(binary),*oldcmd[u][1:]]
 assert source['session_id'] in ' '.join(oldcmd[u])
 for option in ('--spool','--banks-spool','--output'):
  if option in oldcmd[u]:assert Path(oldcmd[u][oldcmd[u].index(option)+1]).is_relative_to(root)
cp=root/'bank-v2-origin-spool'/source['session_id']/'CHECKPOINT.json';beforecp=json.loads(cp.read_text());assert beforecp['source']==source;beforesequence=beforecp['chain']['latest']['event_sequence']
key=(root/'config/stream-20261004/client-api.key').read_text().strip();headers={'Authorization':'Bearer '+key}
def v1head():return json.loads(v.http_bytes('http://127.0.0.1:8871/v1/head',headers=headers))
beforev1=v1head();assert beforev1['profile']=='native-processed-compact-v1' and beforev1['state']=='LIVE' and beforev1['latest_cursor'].startswith(source['session_id']+':')
installation=report/'INSTALLATION.json';assert not installation.exists()
record=dict(state='PREPARED',started_unix_ms=int(time.time()*1000),release=str(release),binary_sha256=build['binary_sha256'],source=source,protected_native_pids=pidbefore,old_transport_pids=oldpids,commands=newcmd,old_commands=oldcmd,bank_limits_changed=False,source_changed=False,validator_restarted=False,connection_idle_seconds=300,total_connection_lifetime_seconds=None)
for u in units:m.write(report/(u+'.previous-override.conf'),oldtext[u])
m.json_write(installation,record)
def unchanged():
 assert pinpath.read_bytes()==pin and registry.read_bytes()==registrybytes and pids()==pidbefore

def verify_process(unit,command,deadline):
 while time.monotonic()<deadline:
  pid=m.run('systemctl','show',unit,'-p','MainPID','--value')
  try:
   if int(pid)>0 and (Path('/proc')/pid/'cmdline').read_bytes().decode().rstrip('\0').split('\0')==command:
    assert hashlib.sha256((Path('/proc')/pid/'exe').read_bytes()).hexdigest()==hashlib.sha256(Path(command[0]).read_bytes()).hexdigest();return pid
  except FileNotFoundError:pass
  time.sleep(.5)
 raise RuntimeError('transport_process_unavailable')

def verify_flow(since_sequence,deadline):
 while time.monotonic()<deadline:
  unchanged()
  try:
   details={};v.private_sample(root,source,details);h=v1head()
   checkpoint=json.loads(cp.read_text())
   if checkpoint['source']==source and checkpoint['chain']['latest']['event_sequence']>since_sequence and h['profile']=='native-processed-compact-v1' and h['state']=='LIVE' and not h['stale'] and h['latest_cursor'].startswith(source['session_id']+':') and h['event_sequence']>beforev1['event_sequence']:
    public={};v.public_sample(public);assert v.rpc('getHealth')=='ok';return {'private':details,'v1_head':h,'public':public,'origin_confirmed_sequence':checkpoint['chain']['latest']['event_sequence']}
  except (v.CheckFailure,OSError,ValueError,KeyError):pass
  time.sleep(1)
 raise RuntimeError('transport_flow_unavailable')
try:
 unchanged()
 for u in units:
  lines=oldtext[u].splitlines();at=[i for i,l in enumerate(lines) if l.startswith('ExecStart=') and l!='ExecStart='];assert len(at)==1
  lines[at[0]]='ExecStart='+m.systemd_command(newcmd[u]);m.install(report,override[u],'\n'.join(lines)+'\n')
 m.run('sudo','systemctl','daemon-reload');unchanged();newpids={}
 # Both origins keep their exact source and replay paths. Restart the relay last.
 for u in units:
  since=time.monotonic();m.run('sudo','systemctl','restart',u)
  newpids[u]=verify_process(u,newcmd[u],time.monotonic()+180)
  if u==units[0]:
   while time.monotonic()-since<6:time.sleep(.25)
   verify_flow(beforesequence,time.monotonic()+240)
  elif u==units[1]:
   while time.monotonic()-since<6:time.sleep(.25)
   verify_flow(beforesequence,time.monotonic()+120)
 # A fresh admitted confirmed control must pass after the relay reload.
 time.sleep(6);details=verify_flow(beforesequence,time.monotonic()+240);unchanged()
 record.update(state='ACTIVE',completed_unix_ms=int(time.time()*1000),transport_pids=newpids,protected_native_pids_unchanged=True,flow=details)
 m.json_write(installation,record)
 expected={**{u:int(p) for u,p in pidbefore.items()},**{u.removesuffix('.service'):int(p) for u,p in newpids.items()}}
 m.json_write(root/'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6/TRANSPORT-PIDS.json',dict(state='VERIFIED',source=source,protected_pids=expected,installation_sha256=hashlib.sha256(installation.read_bytes()).hexdigest()))
 print(json.dumps(record))
except Exception as error:
 record.update(state='ACTIVATION_FAILED',error_type=type(error).__name__);m.json_write(installation,record)
 try:
  unchanged()
  for u in units:m.install(report,override[u],oldtext[u])
  m.run('sudo','systemctl','daemon-reload');restored={}
  for u in units:
   m.run('sudo','systemctl','restart',u);restored[u]=verify_process(u,oldcmd[u],time.monotonic()+180)
  time.sleep(6);proof=verify_flow(beforesequence,time.monotonic()+240);unchanged()
  record.update(rollback='PREVIOUS_TRANSPORTS_VERIFIED',rollback_pids=restored,rollback_flow=proof);m.json_write(installation,record)
 except Exception as rollback_error:
  record.update(state='ROLLBACK_FAILED',rollback_error_type=type(rollback_error).__name__);m.json_write(installation,record)
  raise RuntimeError('transport_activation_and_rollback_verification_failed') from None
 raise RuntimeError('transport_activation_failed_previous_binaries_restored') from None
