#!/usr/bin/env python3
"""Read-only 120-second flow check after the live storage helper change."""
import argparse,hashlib,importlib.util,json,os,subprocess,time
from pathlib import Path
r=Path('/home/ubuntu/live/hivezilla-validator-20260930')
p=argparse.ArgumentParser();p.add_argument('--registry-sha256',required=True);p.add_argument('--installation-sha256',required=True);a=p.parse_args()
sha=lambda b:hashlib.sha256(b).hexdigest()
pin=(r/'config/native-active-source.json').read_bytes();assert sha(pin)=='ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315';source=json.loads(pin)
installpath=r/'reports/native-recovery-20261018/recurrence/incident-20261020-storage/circuit/INSTALLATION.json';installraw=installpath.read_bytes();assert sha(installraw)==a.installation_sha256;install=json.loads(installraw);assert install['state']=='ACTIVE_PENDING_CLOUD_PROOF' and install['source']==source and install['registry_after_sha256']==a.registry_sha256
expected={'hivezilla-validator':2142808,'hivezilla-direct-worker':916425,'hivezilla-bank-v2-producer':917148,'hivezilla-bank-v2-r2-publisher':4167494,'hivezilla-shred-hook-recorder':2142799,'hivezilla-bank-v2-origin':2134416,'hivezilla-stream-origin':2135639,'hivezilla-stream-relay':2136117};assert install['protected_pids']==expected
spec=importlib.util.spec_from_file_location('v',r/'releases/incident-recovery-20261018-a6b89ac28c538eaa/verify_live.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
headers={'Authorization':'Bearer '+(r/'config/stream-20261004/client-api.key').read_text().strip()}
start=time.monotonic();samples=[];faults=[]
while True:
 now=time.time_ns()//1000000;sample={'sampled_unix_ms':now};issues=[]
 try:
  assert (r/'config/native-active-source.json').read_bytes()==pin
  assert installpath.read_bytes()==installraw
  assert sha((r/'config/storage-jobs.json').read_bytes())==a.registry_sha256
  jobs=json.loads((r/'reports/storage-jobs/STATUS.json').read_text());assert jobs['active_source']==source and jobs['registry_sha256']==a.registry_sha256 and -5000<=now-jobs['updated_unix_ms']<90000
  sample['guardian']=jobs['state'];assert jobs['state']=='HEALTHY' and jobs['issues']==[]
  live=next(j for j in jobs['jobs'] if j['unit']=='hivezilla-native-cold-storage-20261012-live.service');assert live['source']==source and live['state']=='RUNNING' and live['issues']==[] and live['max_archived_banks'] is None
  sample.update(live_archived_banks=live['archived_banks'],live_retired_banks=live['retired_banks'])
  for unit,maximum in [('hivezilla-native-cold-storage-20261016-source-18de229d.service',785546),('hivezilla-native-cold-storage-20261016-source-18ddd9a6.service',308768)]:
   j=next(j for j in jobs['jobs'] if j['unit']==unit);assert j['state']=='COMPLETE' and j['archived_banks']==j['retired_banks']==j['max_archived_banks']==maximum and j['issues']==[]
  h=json.loads(v.http_bytes(v.PRIVATE_URL+'/v2/banks/head',headers=headers));assert h['source']==source and h['status']=='live' and type(h['source_lag_ms']) is int and 0<=h['source_lag_ms']<=10000
  sample.update(head=h['status'],source_lag_ms=h['source_lag_ms'],confirmed_slot=h['latest_confirmed']['bank']['slot'])
  sample['rpc']=v.rpc('getHealth');assert sample['rpc']=='ok'
  actual={u:int(subprocess.check_output(['systemctl','show',u,'-p','MainPID','--value'],text=True,timeout=10)) for u in expected};assert actual==expected
  for unit,key in [('hivezilla-native-cold-storage-20261012-live.service','live_pid'),('hivezilla-native-storage-jobs.service','guardian_pid')]:
   assert int(subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True,timeout=10))==int(install[key])
  status=json.loads((Path(install['new_live_row']['output'])/'cold-storage/STATUS.json').read_bytes())
  assert status['state']=='RUNNING' and -5000<=now-status['updated_unix_ms']<90000 and status['full_remote_readback_required'] is True
  sample['recovery']=status['recovery'];assert sample['recovery']['state'] in {'NORMAL','RECOVERING','BACKOFF','OPEN'}
  sample['storage_progress']=status['storage_progress'];assert status['storage_progress'] in {'WAITING','TRANSFERRING','ADVANCING'}
  assert sha(Path(install['new_live_row']['helper']).read_bytes())=='f757f2be29f01378448cbe5bb5b529db2625e6654c065f0f17ebd8ba3cbb6d41'
  for unit,key in [('hivezilla-native-cold-storage-20261012-live.service','live_invocation_id'),('hivezilla-native-storage-jobs.service','guardian_invocation_id')]:
   assert subprocess.check_output(['systemctl','show',unit,'-p','InvocationID','--value'],text=True,timeout=5).strip()==install[key]
  sample['free_disk_bytes']=os.statvfs(r).f_bavail*os.statvfs(r).f_frsize
 except Exception as e:
  issues.append(type(e).__name__)
 sample['issues']=issues;samples.append(sample)
 if issues:faults.append(sample)
 if time.monotonic()-start>=120:break
 time.sleep(10)
passed=not faults and len(samples)>=12 and samples[-1]['confirmed_slot']>samples[0]['confirmed_slot'] and samples[-1]['live_archived_banks']>samples[0]['live_archived_banks'] and samples[-1]['live_retired_banks']>samples[0]['live_retired_banks']
out={'schema':'blockzilla-storage-recovery-flow-v1','state':'PASSED' if passed else 'FAILED','source':source,'registry_sha256':a.registry_sha256,'installation_sha256':a.installation_sha256,'duration_seconds':round(time.monotonic()-start,3),'protected_pids':expected,'samples':samples,'faults':faults,'service_actions':[],'data_mutations':[]}
print(json.dumps(out,separators=(',',':')),flush=True)
raise SystemExit(0 if passed else 1)
