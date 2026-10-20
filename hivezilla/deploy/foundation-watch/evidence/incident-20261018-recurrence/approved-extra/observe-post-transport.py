import argparse,hashlib,importlib.util,json,os,signal,subprocess,time
from pathlib import Path
r=Path('/home/ubuntu/live/hivezilla-validator-20260930');report=r/'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6'
s=importlib.util.spec_from_file_location('v',r/'releases/incident-recovery-20261018-a6b89ac28c538eaa/verify_live.py');v=importlib.util.module_from_spec(s);s.loader.exec_module(v)
parser=argparse.ArgumentParser();parser.add_argument('--expected-pids',required=True);parser.add_argument('--report-name',default='FLOW-POST-TRANSPORT.json');args=parser.parse_args()
assert args.report_name=='FLOW-POST-TRANSPORT.json'
expected_record=json.loads(Path(args.expected_pids).read_text());assert expected_record['state']=='VERIFIED'
stop_requested=False
def stop(*_):
 global stop_requested
 stop_requested=True
signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
pin=(r/'config/native-active-source.json').read_bytes();source=json.loads(pin)
assert expected_record['source']==source
installation=json.loads((report/'INSTALLATION.json').read_text());expected=expected_record['protected_pids'];registrysha=installation['registry_sha256']
headers={'Authorization':'Bearer '+(r/'config/stream-20261004/client-api.key').read_text().strip()}
old='hivezilla-native-cold-storage-20261016-source-18ddd9a6.service';live='hivezilla-native-cold-storage-20261012-live.service'
start=time.monotonic();deadline=start+7200;samples=[];faults=[];last_print=0;complete_since=None
while time.monotonic()<deadline and not stop_requested:
 now=int(time.time()*1000);sample={'ms':now};issues=[]
 try:
  assert (r/'config/native-active-source.json').read_bytes()==pin,'source_pin_changed'
  assert hashlib.sha256((r/'config/storage-jobs.json').read_bytes()).hexdigest()==registrysha,'registry_changed'
  jobs=json.loads((r/'reports/storage-jobs/STATUS.json').read_text());assert now-jobs['updated_unix_ms']<90000,'guardian_stale'
  sample['guardian']=jobs['state'];sample['job_issues']=jobs['issues']
  if jobs['state']!='HEALTHY':issues.append('guardian_'+jobs['state'])
  oldjob=next(j for j in jobs['jobs'] if j['unit']==old);livejob=next(j for j in jobs['jobs'] if j['unit']==live)
  sample.update(old_count=oldjob.get('archived_banks'),old_retired=oldjob.get('retired_banks'),old_state=oldjob['state'],live_count=livejob.get('archived_banks'),live_state=livejob['state'])
  assert oldjob['max_archived_banks']==308768,'cap_changed'
  assert sample['old_count'] is not None and sample['old_count']<=308768,'cap_exceeded'
  h=json.loads(v.http_bytes(v.PRIVATE_URL+'/v2/banks/head',headers=headers));sample.update(head=h.get('status'),lag=h.get('source_lag_ms'),confirmed_slot=h.get('latest_confirmed',{}).get('bank',{}).get('slot'))
  if h.get('source')!=source:issues.append('head_source_changed')
  if h.get('status')!='live':issues.append('head_'+str(h.get('status')));sample['fault_head']=h
  if not isinstance(h.get('source_lag_ms'),int) or h['source_lag_ms']>10000:issues.append('head_source_lag')
  sample['rpc']=v.rpc('getHealth')
  if sample['rpc']!='ok':issues.append('rpc_unhealthy')
  if len(samples)%6==0:
   pids={u:int(subprocess.check_output(['systemctl','show',u,'-p','MainPID','--value'],text=True)) for u in expected}
   if pids!=expected:issues.append('protected_pid_changed')
  sample['free_bytes']=os.statvfs(r).f_bavail*os.statvfs(r).f_frsize
  if sample['old_count']==sample['old_retired']==308768 and oldjob['state']=='COMPLETE' and oldjob.get('active_state')=='inactive':
   if complete_since is None:complete_since=time.monotonic()
  else:complete_since=None
 except Exception as e:issues.append(type(e).__name__);sample['error']=str(e)[:128] if isinstance(e,AssertionError) else type(e).__name__
 sample['issues']=issues;samples.append(sample)
 if issues:faults.append(sample)
 out={'schema':'blockzilla-approved-extra-observation-v1','state':'OBSERVING','source':source,'registry_sha256':registrysha,'duration_seconds':round(time.monotonic()-start,3),'samples':samples,'faults':faults}
 vpath=report/args.report_name;temp=vpath.with_suffix('.new');temp.write_text(json.dumps(out,separators=(',',':'))+'\n');temp.chmod(0o600);os.replace(temp,vpath)
 if time.monotonic()-last_print>=30:
  print(json.dumps({'duration_seconds':out['duration_seconds'],'samples':len(samples),'faults':len(faults),'latest':sample}),flush=True);last_print=time.monotonic()
 if complete_since is not None and time.monotonic()-complete_since>=65:break
 time.sleep(10)
out.update(state='STOPPED' if stop_requested else 'COMPLETE' if complete_since is not None else 'TIMEOUT',duration_seconds=round(time.monotonic()-start,3))
vpath.write_text(json.dumps(out,separators=(',',':'))+'\n');vpath.chmod(0o600)
print(json.dumps({'state':out['state'],'duration_seconds':out['duration_seconds'],'samples':len(samples),'faults':len(faults),'latest':sample}),flush=True)
