#!/usr/bin/env python3
"""Prepare by default. --apply changes only live cold storage and its supervisor."""
import argparse,copy,fcntl,hashlib,importlib.util,json,os,re,shlex,sqlite3,stat,subprocess,sys,time
from contextlib import closing
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORT=ROOT/'reports/native-recovery-20261018/recurrence/transfer-deadline'
RELEASE=ROOT/'releases/transfer-deadline-20261018-498831996a2dc34c'
REGISTRY=ROOT/'config/storage-jobs.json'
PIN=ROOT/'config/native-active-source.json'
OLD_REGISTRY_SHA='c813c9e2accfb857eba1bf331f60af616ddfc8084b7aa2bce276ebf1fdb0c4c3'
PIN_SHA='ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
MANIFEST_SHA='90661d5e0d5c02f465056a8109faf8975f856d28b84a46c764232232c10aa03a'
COLD_SHA='8a95785f2df4e2f53b414322f0fc42a8d2edd93a02f04cfca6358bb312c19800'
GUARDIAN_SHA='567c15e41b3f21665a765fbfaa5b2c70d9a565d1280463da959d946472a3c87c'
FINITE_SHA='7f0d6719792abf9b65d954e824fe59a8fa3126210136ebc6c0a8386f26a57b71'
LIVE='hivezilla-native-cold-storage-20261012-live.service'
GUARDIAN='hivezilla-native-storage-jobs.service'
FINITE={'hivezilla-native-cold-storage-20261016-source-18de229d.service':785546,'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service':308768}
PIDS={'hivezilla-validator':2142808,'hivezilla-direct-worker':916425,'hivezilla-bank-v2-producer':917148,'hivezilla-bank-v2-r2-publisher':917318,'hivezilla-shred-hook-recorder':2142799,'hivezilla-bank-v2-origin':2134416,'hivezilla-stream-origin':2135639,'hivezilla-stream-relay':2136117}
BASE=ROOT/'reports/native-recovery-20261018/recurrence'
ANCHORS={
 BASE/'approved-extra-18ddd9a6/INSTALLATION.json':'af2b662cb508ad7d4364f4eb446c60a74619824f2a1a627afc74918bf1298306',
 BASE/'approved-extra-18ddd9a6/TRANSPORT-PIDS.json':'0bc47d250141d0bdfbc6c3e521727f7f37b318191c77275a6c1ec2a1d6169f29',
 BASE/'connection-idle/INSTALLATION.json':'a6c047dbc3e64eae965abf2215ae738e5099160502a18754c2a25257edf490f8',
 BASE/'connection-idle/SAME-CONNECTION-630S.json':'6d443c4c7edba0c6df77c005b9b74175de6fd857e045c07a29f55d16c5df816f',
 BASE/'protocol-retry/INSTALLATION.json':'eec692609be680d0f58090383dedd7897dd5e2e35eb01a015953baf13132b475'}
LIVE_OVERRIDE=Path('/etc/systemd/system')/(LIVE+'.d')/('z'*96+'-native-source.conf')
GUARDIAN_OVERRIDE=Path('/etc/systemd/system')/(GUARDIAN+'.d')/'zz-transfer-deadline-20261018.conf'

def require(ok,code):
 if not ok:raise RuntimeError(code)
def sha(data):return hashlib.sha256(data).hexdigest()
def read(path,limit=2**20):
 require(stat.S_ISREG(path.lstat().st_mode),'metadata_not_regular')
 with path.open('rb') as f:b=f.read(limit+1)
 require(len(b)<=limit,'metadata_too_large');return b
def obj(path):return json.loads(read(path))
def digest(path):
 with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def now():return time.time_ns()//1000000
def dump(v):return (json.dumps(v,indent=2,sort_keys=True)+'\n').encode()
def save(path,data,mode=0o600):
 path.parent.mkdir(parents=True,exist_ok=True)
 temp=path.with_name(path.name+'.new')
 with temp.open('wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
 temp.chmod(mode);os.replace(temp,path)
 fd=os.open(path.parent,os.O_RDONLY)
 try:os.fsync(fd)
 finally:os.close(fd)
def run(*args):return subprocess.check_output(args,text=True,timeout=75).strip()
def unit_text(unit):return subprocess.check_output(['sudo','systemctl','cat',unit],text=True,timeout=15)
def command(text):
 values=[]
 for line in text.splitlines():
  if line=='ExecStart=':values=[]
  elif line.startswith('ExecStart='):values.append(shlex.split(line[10:]))
 require(len(values)==1,'effective_execstart_not_unique');return values[0]
def quote(args):
 require(all(isinstance(x,str) and '\n' not in x and '\r' not in x and '\0' not in x for x in args),'unsafe_command_argument')
 return ' '.join('"'+x.replace('\\','\\\\').replace('"','\\"').replace('%','%%')+'"' for x in args)
def show(unit):return dict(x.split('=',1) for x in run('systemctl','show',unit,'-p','MainPID,ActiveState,SubState,Result,Restart,InvocationID').splitlines())
def pids():return {u:int(run('systemctl','show',u,'-p','MainPID','--value')) for u in PIDS}
def argv(pid):return (Path('/proc')/str(pid)/'cmdline').read_bytes().decode().rstrip('\0').split('\0')
def module(path):
 s=importlib.util.spec_from_file_location('deadline_jobs',path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def authority(final_sha,cloud_sha,expected_registry):
 require(sha(read(PIN))==PIN_SHA and pids()==PIDS,'source_or_protected_pid_changed')
 source=obj(PIN)
 for path,h in ANCHORS.items():require(sha(read(path))==h,'canonical_receipt_changed:'+path.name)
 binding=obj(BASE/'approved-extra-18ddd9a6/TRANSPORT-PIDS.json');transport=obj(BASE/'connection-idle/INSTALLATION.json')
 require(binding['state']=='VERIFIED' and binding['source']==source and binding['protected_pids']==PIDS and binding['installation_sha256']==ANCHORS[BASE/'connection-idle/INSTALLATION.json'],'transport_binding_changed')
 require(transport['state']=='ACTIVE' and transport['source']==source and transport['protected_native_pids_unchanged'] is True and transport['source_changed'] is False and transport['bank_limits_changed'] is False and transport['validator_restarted'] is False,'transport_installation_invalid')
 proof=obj(BASE/'connection-idle/SAME-CONNECTION-630S.json');require(proof['state']=='PASSED' and proof['source']==source and proof['registry_sha256']==OLD_REGISTRY_SHA,'transport_proof_invalid')
 for unit,pid in transport['transport_pids'].items():
  require(int(pid)==PIDS[unit.removesuffix('.service')] and argv(pid)==transport['commands'][unit],'transport_runtime_changed')
 final_path=BASE/'approved-extra-18ddd9a6/FINAL-STATUS.json';cloud_path=BASE/'approved-extra-18ddd9a6/CLOUD-PROOF-FINAL.json'
 require(sha(read(final_path))==final_sha and sha(read(cloud_path))==cloud_sha,'completion_proof_hash_changed')
 final,cloud=obj(final_path),obj(cloud_path)
 require(final['state']=='PASSED' and final['registry_sha256']==OLD_REGISTRY_SHA and final['registry']==expected_registry and final['source']==source and final['pids']==PIDS,'completion_status_scope_invalid')
 require(cloud['schema']=='blockzilla-approved-extra-cloudproof-v1' and cloud['state']=='PASSED' and cloud['registry_sha256']==OLD_REGISTRY_SHA and cloud['archived_banks']==cloud['retired_banks']==308768 and cloud['additional_banks']==51606 and cloud['pending_bundles']==0 and cloud['unit_state']=='inactive' and cloud['proof_scope']=='independent_final_new_bundle' and cloud['selection']=='maximum_durable_archive_rowid' and cloud['full_wrapped_get'] is True and cloud['index_reopened_read_only'] is True and type(cloud['bundle_banks']) is int and cloud['bundle_banks']>0 and cloud['local_receipts_checked']==2*cloud['bundle_banks'],'finite_cloud_proof_invalid')
 finite_rows={j['unit']:j for j in expected_registry['jobs'] if j['unit']!=LIVE}
 require(set(finite_rows)==set(FINITE),'finite_scope_changed')
 for unit,row in finite_rows.items():
  require(row['max_archived_banks']==FINITE[unit] and row['helper_sha256']==FINITE_SHA and digest(Path(row['helper']))==FINITE_SHA,'finite_helper_or_cap_changed')
  require(command(unit_text(unit))==row['expected_command'],'finite_command_changed')
  s=show(unit);require(s['ActiveState']=='inactive' and s['SubState']=='dead' and s['MainPID']=='0' and s['Result']=='success' and s['Restart']=='no','finite_job_not_complete')
  value=archive(row)
  require(value['archived_banks']==value['retired_banks']==FINITE[unit] and value['metadata']=={'source':row['source'],'namespace':row['namespace']},'finite_database_not_complete')
  status=obj(Path(row['output'])/'cold-storage/STATUS.json')
  require(status['state']=='COMPLETE' and status['archived_banks']==status['retired_banks']==FINITE[unit] and status['pending_bundles']==0 and status['max_archived_banks']==FINITE[unit] and status['stop_reason']=='max_archived_banks','finite_status_not_complete')
  require(not list((Path(row['output'])/'cold-storage').glob('bundle*.tar')),'finite_pending_bundle_file')
  old=final['storage'][unit];require(old['archived_banks']==old['retired_banks']==old['absolute_cap']==FINITE[unit] and old['source']==row['source'] and old['namespace']==row['namespace'],'finite_final_status_mismatch')
  if FINITE[unit]==308768:
   require(cloud['source']==row['source'] and cloud['namespace']==row['namespace'] and cloud['final_archive_rowid']==value['max_archive_rowid'] and cloud['last_archive_progress_unix_ms']==value['last_archive_progress_unix_ms'],'finite_cloud_index_changed')
 require(sha(read(final_path))==final_sha and sha(read(cloud_path))==cloud_sha,'completion_proof_changed_during_read')
 return source,final,cloud

def archive(row):
 path=Path(row['output'])/'cold-storage/archive.sqlite'
 require(stat.S_ISREG(path.lstat().st_mode),'archive_database_not_regular')
 with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
  db.execute('PRAGMA query_only=ON');db.execute('BEGIN');end=time.monotonic()+20;db.set_progress_handler(lambda:time.monotonic()>end,10000)
  count=db.execute('SELECT count(*),coalesce(sum(retired),0),coalesce(max(rowid),0) FROM archives').fetchone()
  meta=json.loads(db.execute('SELECT value FROM meta WHERE id=1').fetchone()[0]);progress=db.execute('SELECT value FROM meta WHERE id=2').fetchone()
 return dict(archived_banks=count[0],retired_banks=count[1],max_archive_rowid=count[2],last_archive_progress_unix_ms=int(progress[0]) if progress else None,metadata=meta)

def release_check():
 stage=obj(REPORT/'STAGE.json');require(stage['state']=='STAGED_TESTED_NOT_ACTIVE' and stage['release']==str(RELEASE) and stage['tests_passed'] is True and stage['tests_count']==51,'stage_not_tested')
 require(digest(RELEASE/'MANIFEST.json')==MANIFEST_SHA==stage['manifest_sha256'],'release_manifest_changed')
 manifest=obj(RELEASE/'MANIFEST.json')
 for n,h in manifest.items():require(Path(n).name==n and stat.S_ISREG((RELEASE/n).lstat().st_mode) and digest(RELEASE/n)==h and not (RELEASE/n).stat().st_mode&0o222,'release_file_changed')
 require(not RELEASE.stat().st_mode&0o222 and manifest['cold_storage.py']==COLD_SHA and manifest['storage_jobs.py']==GUARDIAN_SHA,'release_helpers_changed')
 return stage,module(RELEASE/'storage_jobs.py')

def prepare(args):
 require(not (REPORT/'INSTALLATION.json').exists(),'installation_already_prepared')
 stage,j=release_check();raw=read(REGISTRY);require(sha(raw)==OLD_REGISTRY_SHA,'old_registry_changed');registry=json.loads(raw)
 source,final,cloud=authority(args.final_status_sha256,args.cloud_proof_sha256,registry)
 require(registry['schema']==j.SCHEMA and registry['root']==str(ROOT) and len(registry['jobs'])==3,'registry_scope_changed')
 live=next(x for x in registry['jobs'] if x['unit']==LIVE);require(live['source']==source and live['max_archived_banks'] is None and live['helper_sha256']=='837b89f1c2dcfac6fb34415f783d88274ae0fd8d9200c53d5a70333cfbc57b63' and digest(Path(live['helper']))==live['helper_sha256'],'old_live_row_changed')
 live_text=unit_text(LIVE);oldcmd=live['expected_command'];require(command(live_text)==oldcmd,'old_live_command_changed')
 require(sha(live_text.encode())=='638cd86f4b1d8c0a462f6d4137e2991d389a9a5994fa1f27a094598b8b3add6f','live_unit_changed')
 oldlive=show(LIVE);require(oldlive['ActiveState']=='active' and oldlive['Restart']=='no' and int(oldlive['MainPID'])>0 and argv(oldlive['MainPID'])==oldcmd,'old_live_runtime_changed')
 old_override=subprocess.check_output(['sudo','cat','--',str(LIVE_OVERRIDE)],timeout=15);require(command(old_override.decode())==oldcmd,'live_override_changed')
 guardian_text=unit_text(GUARDIAN);guardian_old=command(guardian_text)
 require(sha(guardian_text.encode())=='5daddedd371e734140e8fd0ad1b20d085e9825f9b1c4dd0d46d8fccf30ff6c19' and guardian_old[:3]==['/usr/bin/python3','-B',str(ROOT/'releases/protocol-retry-20261018-ccc4e4475ec568d1/storage_jobs.py')],'guardian_unit_changed')
 require(digest(Path(guardian_old[2]))=='9c586efd91790488dedaa282e88f16625c3a187791c6316c62e269bf2a12b376' and not GUARDIAN_OVERRIDE.exists(),'old_guardian_changed')
 guardian_state=show(GUARDIAN);require(guardian_state['ActiveState']=='active' and argv(guardian_state['MainPID'])==guardian_old,'old_guardian_not_running')
 new=copy.deepcopy(registry);row=next(x for x in new['jobs'] if x['unit']==LIVE);newcmd=oldcmd.copy();newcmd[1]=str(RELEASE/'cold_storage.py')
 row.update(helper=newcmd[1],helper_sha256=COLD_SHA,expected_command=newcmd,expected_command_sha256=j.command_sha256(newcmd));j.validate_job(ROOT,row)
 require([x for x in new['jobs'] if x['unit']!=LIVE]==[x for x in registry['jobs'] if x['unit']!=LIVE],'finite_rows_changed')
 for old,newrow in zip(registry['jobs'],new['jobs']):
  if old['unit']==LIVE:require({k:v for k,v in old.items() if k not in ('helper','helper_sha256','expected_command','expected_command_sha256')}=={k:v for k,v in newrow.items() if k not in ('helper','helper_sha256','expected_command','expected_command_sha256')},'live_scope_changed')
 guardian_new=guardian_old.copy();guardian_new[2]=str(RELEASE/'storage_jobs.py')
 lines=old_override.decode().splitlines();at=[i for i,line in enumerate(lines) if line.startswith('ExecStart=') and line!='ExecStart='];require(len(at)==1,'live_override_not_unique');lines[at[0]]='ExecStart='+quote(newcmd)
 proposed_live=('\n'.join(lines)+'\n').encode();proposed_guardian=('[Service]\nExecStart=\nExecStart='+quote(guardian_new)+'\n').encode();newraw=dump(new)
 live_before=archive(live);require(live_before['metadata']=={'source':source,'namespace':live['namespace']},'live_database_binding_changed')
 files={'registry-before.json':raw,'registry-after.json':newraw,'live-before.conf':old_override,'live-after.conf':proposed_live,'guardian-before.unit':guardian_text.encode(),'guardian-after.conf':proposed_guardian,'live-before.unit':live_text.encode(),'FINAL-STATUS.json':read(BASE/'approved-extra-18ddd9a6/FINAL-STATUS.json'),'CLOUD-PROOF-FINAL.json':read(BASE/'approved-extra-18ddd9a6/CLOUD-PROOF-FINAL.json')}
 for n,data in files.items():save(REPORT/n,data,0o400)
 plan=dict(schema='blockzilla-transfer-deadline-transition-plan-v1',state='PREPARED',prepared_unix_ms=now(),release=str(RELEASE),registry_before_sha256=OLD_REGISTRY_SHA,registry_after_sha256=sha(newraw),source=source,source_pin_sha256=PIN_SHA,protected_pids=PIDS,old_live_row=live,new_live_row=row,old_live_command=oldcmd,live_command=newcmd,old_guardian_command=guardian_old,guardian_command=guardian_new,old_live_state=oldlive,old_guardian_state=guardian_state,prepare_observed_live=live_before,final_status_sha256=args.final_status_sha256,cloud_proof_sha256=args.cloud_proof_sha256,anchors={str(p):h for p,h in ANCHORS.items()},files={n:sha(data) for n,data in files.items()},live_override=str(LIVE_OVERRIDE),guardian_override=str(GUARDIAN_OVERRIDE),finite_rows_unchanged=True,bank_caps_changed=False,service_actions=[])
 save(REPORT/'PLAN.json',dump(plan),0o400)
 record=dict(schema='blockzilla-transfer-deadline-install-v1',state='PREPARED',prepared_unix_ms=plan['prepared_unix_ms'],release=str(RELEASE),plan_sha256=digest(REPORT/'PLAN.json'),registry_before_sha256=OLD_REGISTRY_SHA,registry_after_sha256=sha(newraw),source=source,protected_pids=PIDS,live_command=newcmd,guardian_command=guardian_new,final_status_sha256=args.final_status_sha256,cloud_proof_sha256=args.cloud_proof_sha256,finite_rows_unchanged=True,bank_caps_changed=False,service_actions=[])
 save(REPORT/'INSTALLATION.json',dump(record));print(json.dumps(record))

def install_unit(source,destination):
 run('sudo','install','-d','-m','755',str(destination.parent));run('sudo','install','-m','644',str(source),str(destination))
def registry_install(raw):
 s=REGISTRY.stat();require(s.st_uid==os.geteuid() and not s.st_mode&0o022,'registry_owner_or_mode_changed')
 save(REGISTRY,raw,s.st_mode&0o777)
def running(unit,expected):
 end=time.monotonic()+35
 while time.monotonic()<end:
  value=show(unit);pid=int(value['MainPID'])
  if value['ActiveState']=='active' and pid>0:
   try:
    if argv(pid)==expected:return dict(pid=pid,invocation_id=value['InvocationID'],argv=expected)
   except (FileNotFoundError,ProcessLookupError):pass
  time.sleep(.25)
 raise RuntimeError('new_runtime_command_unavailable')
def healthy(since,registry_sha):
 end=time.monotonic()+90
 while time.monotonic()<end:
  try:
   value=obj(ROOT/'reports/storage-jobs/STATUS.json')
   if value['state']=='HEALTHY' and not value['issues'] and value['registry_sha256']==registry_sha and value['updated_unix_ms']>=since and now()-value['updated_unix_ms']<90000:return value
  except (FileNotFoundError,KeyError,ValueError):pass
  time.sleep(.25)
 raise RuntimeError('guardian_not_healthy')

def apply(args):
 release_check();record=obj(REPORT/'INSTALLATION.json');require(record['state']=='PREPARED','installation_not_prepared')
 require(digest(REPORT/'PLAN.json')==record['plan_sha256'],'prepared_plan_changed');plan=obj(REPORT/'PLAN.json')
 require(args.final_status_sha256==plan['final_status_sha256'] and args.cloud_proof_sha256==plan['cloud_proof_sha256'],'proof_authority_changed')
 for n,h in plan['files'].items():require(digest(REPORT/n)==h,'proposed_or_backup_file_changed:'+n)
 oldraw=read(REPORT/'registry-before.json');newraw=read(REPORT/'registry-after.json');old=json.loads(oldraw)
 authority(args.final_status_sha256,args.cloud_proof_sha256,old)
 require(read(REGISTRY)==oldraw and pids()==PIDS,'pre_apply_registry_or_pid_changed')
 require(unit_text(LIVE).encode()==read(REPORT/'live-before.unit') and unit_text(GUARDIAN).encode()==read(REPORT/'guardian-before.unit'),'pre_apply_unit_changed')
 require(show(LIVE)['MainPID']==plan['old_live_state']['MainPID'] and show(LIVE)['InvocationID']==plan['old_live_state']['InvocationID'] and argv(show(LIVE)['MainPID'])==plan['old_live_command'],'pre_apply_live_invocation_changed')
 require(not GUARDIAN_OVERRIDE.exists(),'guardian_override_already_exists')
 require(show(GUARDIAN)['MainPID']==plan['old_guardian_state']['MainPID'] and show(GUARDIAN)['InvocationID']==plan['old_guardian_state']['InvocationID'] and argv(show(GUARDIAN)['MainPID'])==plan['old_guardian_command'],'pre_apply_guardian_invocation_changed')
 record.update(state='ACTIVATING',started_unix_ms=now());save(REPORT/'INSTALLATION.json',dump(record))
 touched=False
 try:
  touched=True
  run('sudo','systemctl','stop',GUARDIAN);record['service_actions'].append(['stop',GUARDIAN])
  run('sudo','systemctl','stop',LIVE);record['service_actions'].append(['stop',LIVE])
  require(show(LIVE)['MainPID']=='0' and read(REGISTRY)==oldraw and pids()==PIDS,'stop_or_scope_changed')
  authority(args.final_status_sha256,args.cloud_proof_sha256,old)
  live_before=archive(plan['old_live_row']);before=dict(schema='blockzilla-transfer-deadline-before-v1',sampled_unix_ms=now(),registry_before_sha256=OLD_REGISTRY_SHA,source=plan['source'],source_pin_sha256=PIN_SHA,protected_pids=PIDS,live_before=live_before,old_live_row=plan['old_live_row'],old_live_invocation=plan['old_live_state']['InvocationID'],final_status_sha256=args.final_status_sha256,cloud_proof_sha256=args.cloud_proof_sha256)
  require(not (REPORT/'BEFORE.json').exists(),'before_receipt_exists');save(REPORT/'BEFORE.json',dump(before),0o400)
  record.update(before_sha256=digest(REPORT/'BEFORE.json'),live_before=live_before)
  install_unit(REPORT/'live-after.conf',LIVE_OVERRIDE);install_unit(REPORT/'guardian-after.conf',GUARDIAN_OVERRIDE);registry_install(newraw)
  record.update(state='CONFIGURED',configured_unix_ms=now());save(REPORT/'INSTALLATION.json',dump(record));run('sudo','systemctl','daemon-reload')
  require(pids()==PIDS and sha(read(PIN))==PIN_SHA and read(REGISTRY)==newraw,'configured_scope_changed')
  require(command(unit_text(LIVE))==plan['live_command'] and command(unit_text(GUARDIAN))==plan['guardian_command'],'new_effective_command_mismatch')
  activation=now();record['activation_started_unix_ms']=activation
  run('sudo','systemctl','start',LIVE);record['service_actions'].append(['start',LIVE]);live=running(LIVE,plan['live_command'])
  run('sudo','systemctl','start',GUARDIAN);record['service_actions'].append(['start',GUARDIAN]);guardian=running(GUARDIAN,plan['guardian_command'])
  status=healthy(activation,plan['registry_after_sha256']);require(pids()==PIDS and sha(read(PIN))==PIN_SHA and read(REGISTRY)==newraw,'post_apply_scope_changed')
  record.update(state='ACTIVE',completed_unix_ms=now(),live_pid=live['pid'],guardian_pid=guardian['pid'],live_invocation_id=live['invocation_id'],guardian_invocation_id=guardian['invocation_id'],observed_live_command=live['argv'],observed_guardian_command=guardian['argv'],protected_pids_unchanged=True,storage_jobs=status,post_transition_cloud_proof='PENDING_INDEPENDENT_CHECK')
  save(REPORT/'INSTALLATION.json',dump(record));print(json.dumps(record))
 except BaseException as error:
  record.update(state='ACTIVATION_FAILED',error_type=type(error).__name__,failed_unix_ms=now());save(REPORT/'INSTALLATION.json',dump(record))
  if touched:
   rollback=dict(schema='blockzilla-transfer-deadline-rollback-v1',state='STARTED',started_unix_ms=now(),registry_before_sha256=OLD_REGISTRY_SHA,source=plan['source'],protected_pids=PIDS)
   try:
    require(pids()==PIDS and sha(read(PIN))==PIN_SHA,'rollback_source_or_protected_pid_changed_before_stop')
    run('sudo','systemctl','stop',GUARDIAN,LIVE)
    require(pids()==PIDS and sha(read(PIN))==PIN_SHA,'rollback_source_or_protected_pid_changed_before_restore')
    require(sha(read(REGISTRY)) in (plan['registry_before_sha256'],plan['registry_after_sha256']),'rollback_registry_changed_externally')
    require(subprocess.check_output(['sudo','cat','--',str(LIVE_OVERRIDE)],timeout=15) in (read(REPORT/'live-before.conf'),read(REPORT/'live-after.conf')),'rollback_live_override_changed_externally')
    install_unit(REPORT/'live-before.conf',LIVE_OVERRIDE)
    if GUARDIAN_OVERRIDE.exists():
     require(subprocess.check_output(['sudo','cat','--',str(GUARDIAN_OVERRIDE)],timeout=15)==read(REPORT/'guardian-after.conf'),'rollback_guardian_override_changed_externally');run('sudo','rm','--',str(GUARDIAN_OVERRIDE))
    registry_install(oldraw);run('sudo','systemctl','daemon-reload')
    require(pids()==PIDS and sha(read(PIN))==PIN_SHA,'rollback_source_or_protected_pid_changed')
    since=now();run('sudo','systemctl','start',LIVE);live=running(LIVE,plan['old_live_command']);run('sudo','systemctl','start',GUARDIAN);guardian=running(GUARDIAN,plan['old_guardian_command']);status=healthy(since,OLD_REGISTRY_SHA)
    rollback.update(state='PREVIOUS_HELPER_RESTORED',completed_unix_ms=now(),live_pid=live['pid'],guardian_pid=guardian['pid'],registry_sha256=sha(read(REGISTRY)),storage_jobs=status,protected_pids_unchanged=pids()==PIDS)
   except BaseException as rollback_error:
    rollback.update(state='ROLLBACK_FAILED',error_type=type(rollback_error).__name__,completed_unix_ms=now())
   save(REPORT/'ROLLBACK.json',dump(rollback));record['rollback_sha256']=digest(REPORT/'ROLLBACK.json');record['rollback_state']=rollback['state'];save(REPORT/'INSTALLATION.json',dump(record))
  raise RuntimeError('transfer_deadline_activation_failed_inspect_receipts') from None

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--final-status-sha256',required=True);parser.add_argument('--cloud-proof-sha256',required=True);parser.add_argument('--apply',action='store_true');args=parser.parse_args()
 require(all(re.fullmatch('[0-9a-f]{64}',v) for v in (args.final_status_sha256,args.cloud_proof_sha256)),'invalid_proof_hash')
 require(REPORT.is_dir(),'stage_report_missing')
 with (REPORT/'INSTALL.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  if args.apply:apply(args)
  else:prepare(args)
if __name__=='__main__':main()
