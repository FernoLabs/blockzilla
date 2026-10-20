import collections,datetime,hashlib,json,math,os,re,stat,subprocess,time
from pathlib import Path
ROOT=Path('/home/ubuntu/live/hivezilla-validator-20260930')
START='2026-10-18 11:09:00 UTC'
PIDS={'hivezilla-validator':2142808,'hivezilla-direct-worker':916425,'hivezilla-bank-v2-producer':917148,'hivezilla-bank-v2-r2-publisher':917318,'hivezilla-shred-hook-recorder':2142799,'hivezilla-bank-v2-origin':2134416,'hivezilla-stream-origin':2135639,'hivezilla-stream-relay':2136117,'hivezilla-native-cold-storage-20261012-live':2353823,'hivezilla-native-storage-jobs':2353836}
SOURCE_KEYS=('genesis_hash','node_identity','agave_revision','session_id')
IDENT=re.compile(r'^[A-Za-z_][A-Za-z0-9_.:-]{0,95}$')
def identifier(v):return v if isinstance(v,str) and IDENT.fullmatch(v) else None
def source(v):return {k:v[k] for k in SOURCE_KEYS} if isinstance(v,dict) and all(isinstance(v.get(k),str) and re.fullmatch('[A-Za-z0-9-]{1,96}',v[k]) for k in SOURCE_KEYS) else None
def sha(b):return hashlib.sha256(b).hexdigest()
def read(path,maxbytes=2**20):
 s=path.lstat();assert stat.S_ISREG(s.st_mode)
 with path.open('rb') as f:b=f.read(maxbytes+1)
 assert len(b)<=maxbytes
 return json.loads(b),dict(path=str(path),sha256=sha(b),bytes=len(b),mtime_unix_ms=s.st_mtime_ns//1000000)
def selected(v,keys):
 out={}
 for k in keys:
  x=v.get(k)
  if x is None or type(x) in (int,bool) or type(x) is float and math.isfinite(x):out[k]=x
  elif isinstance(x,str) and identifier(x):out[k]=x
 return out
def codes(xs):return [x for x in xs if identifier(x)][:40] if isinstance(xs,list) else []
def safe_status(v):
 keys=('schema','state','storage_progress','capacity_state','updated_unix_ms','sampled_unix_ms','sampled_at_unix_ms','archived_banks','retired_banks','freed_bytes_this_run','keep_seconds','batch_bank_limit','parallel_bundle_limit','pending_bundles','last_batch_banks','last_batch_seconds','full_remote_readback_required','progress_stall_seconds','last_archive_progress_unix_ms','eligible_wait_since_unix_ms','oldest_eligible_pending_slot','error_type','network_error_type','http_status','max_archived_banks','archive_allowance_banks','stop_reason','free_disk_bytes','total_disk_bytes','capture_reserve_bytes','recovery_minimum_free_bytes','next_archive_budget_bytes','snapshot_headroom_bytes','recovery_headroom_bytes','snapshot_capture_reserve_at_risk','snapshot_headroom_low','snapshot_archive_allocated_bytes','active_temporary_archive_count','registry_sha256','invocation_id','send_error','last_candidate_seen_unix_ms','last_candidate_eligible_unix_ms','growth_observation_seconds','growth_sample_count','net_disk_growth_bytes_per_second','non_archive_growth_bytes_per_second','estimated_seconds_to_capture_reserve','estimated_seconds_to_snapshot_headroom_exhaustion')
 out=selected(v,keys)
 for k in ('source','active_source'):
  if k in v:out[k]=source(v[k])
 for k in ('errors','issues','faults'):
  if k in v:out[k]=codes(v[k])
 if isinstance(v.get('jobs'),list):
  out['jobs']=[dict(selected(x,('unit','state','max_archived_banks','retryable','active_state','invocation_id','frozen','archived_banks','retired_banks','remaining_banks','status_age_ms','storage_progress','last_archive_progress_unix_ms','error_type','network_error_type','http_status','failure_unix_ms','retry_attempts','next_retry_unix_ms')),source=source(x.get('source')),issues=codes(x.get('issues'))) for x in v['jobs'][:16]]
 return out

def trim(v):
 if isinstance(v,dict):return {k:trim(x) for k,x in v.items() if x is not None}
 if isinstance(v,list):return [trim(x) for x in v]
 return v
out=dict(schema='blockzilla-storage-circuit-live-status-v1',sampled_unix_ms=time.time_ns()//1000000,host_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),service_actions=[],configuration_changes=[],deleted_files=[])
r,rm=read(ROOT/'config/storage-jobs.json',256*1024);pin,pm=read(ROOT/'config/native-active-source.json',65536);job=next(x for x in r['jobs'] if x['max_archived_banks'] is None)
out.update(registry_sha256=rm['sha256'],source_pin_sha256=pm['sha256'],source=source(pin),reports={})
for name,path in [('live_cold',Path(job['output'])/'cold-storage/STATUS.json'),('capacity',ROOT/'reports/native-capacity/STATUS.json'),('jobs',ROOT/'reports/storage-jobs/STATUS.json'),('sender',ROOT/'reports/foundation-watch/STATUS.json')]:
 v,m=read(path);out['reports'][name]=dict(metadata=m,status=trim(safe_status(v)))
out['pids']={u:int(subprocess.check_output(['systemctl','show',u,'-p','MainPID','--value'],text=True,timeout=5)) for u in PIDS}
ledger,lm=read(ROOT/'reports/storage-jobs/RETRIES.json');key=sha(json.dumps(job,sort_keys=True,separators=(',',':')).encode());out['live_retry_ledger']=selected(ledger['jobs'].get(key,{}),('attempts','last_observed_archived_banks','healthy_since_unix_ms','healthy_start_archived_banks','last_reset_unix_ms','next_retry_unix_ms','last_retry_unix_ms'))
v,m=read(Path(job['output'])/'cold-storage/STATUS.json')
out['recovery']=v.get('recovery')
out['recovery_state_persisted']=v.get('recovery_state_persisted')
out['helper_sha256']=sha(Path(job['helper']).read_bytes())
out['protected_pids_unchanged']=all(out['pids'][unit]==pid for unit,pid in {'hivezilla-validator':2142808,'hivezilla-direct-worker':916425,'hivezilla-bank-v2-producer':917148,'hivezilla-bank-v2-r2-publisher':4167494,'hivezilla-shred-hook-recorder':2142799,'hivezilla-bank-v2-origin':2134416,'hivezilla-stream-origin':2135639,'hivezilla-stream-relay':2136117}.items())
out['generations']={unit:dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show',unit,'-p','MainPID,InvocationID,ActiveState,SubState'],text=True,timeout=5).splitlines()) for unit in ['hivezilla-native-cold-storage-20261012-live','hivezilla-native-storage-jobs']}
print(json.dumps(out,separators=(',',':')))

