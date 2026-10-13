#!/usr/bin/env python3
"""Finite public-route soak. Private ledger payloads stay on Foundation."""
import argparse,json,time,urllib.request,urllib.error,urllib.parse,subprocess,pathlib,sqlite3,hashlib,os,statistics
p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);p.add_argument('--seconds',type=int,default=660);p.add_argument('--report',type=pathlib.Path);a=p.parse_args();r=a.root;report=a.report or r/'reports/bank-stream-v2-20261010';out=report/'live-probe';out.mkdir(exist_ok=True)
key=(r/'config/stream-20261004/client-api.key').read_text().strip();base='https://stream.blockzilla.dev'
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):raise RuntimeError('redirect_rejected')
opener=urllib.request.build_opener(NoRedirect)
def get(path,auth=True):
 request=urllib.request.Request(base+path,headers=({'Authorization':'Bearer '+key,'User-Agent':'blockzilla-bank-probe/2'} if auth else {'User-Agent':'blockzilla-bank-probe/2'}))
 return opener.open(request,timeout=12)
def head():
 for attempt in range(3):
  try:
   with get('/v2/banks/head') as response:return json.load(response)
  except urllib.error.HTTPError as error:
   if error.code<500:raise
  except (OSError,TimeoutError):pass
  time.sleep(0.5)
 raise RuntimeError('public head network unavailable')
def code(path,expected,auth=True):
 try:
  with get(path,auth) as response:status=response.status;data=json.load(response)
 except urllib.error.HTTPError as e:
  status=e.code
  try:data=json.load(e)
  except ValueError:data={}
 assert status==expected,(status,expected)
 return {'status':status,'error':data.get('error')}
def write(name,value):(out/name).write_text(json.dumps(value,indent=2)+'\n')
def frame_bytes(path):
 total=0
 for p in path.glob('*.frame'):
  try:total+=p.stat().st_size
  except FileNotFoundError:pass # A bounded writer can retire an old frame here.
 return total
d= json.loads((report/'DEPLOYMENT.json').read_text());assert d['state'] in ['INSTALLED_PENDING_PUBLIC_CHECK','RECOVERED_PENDING_PUBLIC_CHECK'];source=d['source'];source_file=report/'expected-source.json';assert json.loads(source_file.read_text())==source
h=head();deadline=time.monotonic()+300
while h['status']!='live' and time.monotonic()<deadline:
 assert h['status']!='failed','source_failed';time.sleep(0.5);h=head()
assert h['source']==source and h['status']=='live',h['status'];begin=h['cursor'];write('initial-head.json',h)
checks={'no_header':code('/v2/banks/head',401,False),'url_key_rejected':code('/v2/banks/head?api_key=probe',401,False),'wrong_source':code('/v2/banks/events?after=wrong-session:p0:-',410),'bad_cursor':code('/v2/banks/events?after=bad',400),'missing_dependency':code('/v2/banks/chunks/'+'0'*64,410)};write('http-checks.json',checks)
transport=d.get('consumer_release',d.get('transport_release',d['release']));log=(out/'rust-consumer.jsonl').open('w');errors=(out/'rust-consumer.stderr').open('w');args=[transport+'/bank-consumer',base+'/',str(r/'config/stream-20261004/client-api.key'),str(source_file),str(out/'consumer.sqlite'),'1000000'];consumer=subprocess.Popen(args,stdout=log,stderr=errors)
start=time.monotonic();samples=[];consumer_restart=None;native=pathlib.Path(d['processed_output']);bank=pathlib.Path(d['bank_output']);spool=pathlib.Path(d['relay_spool']);previous=None
try:
 while time.monotonic()-start<a.seconds:
  if consumer.poll() is not None:raise RuntimeError('consumer_failed; inspect server-local stderr')
  if consumer_restart is None and time.monotonic()-start>=min(120,a.seconds/3):
   consumer.terminate();consumer.wait(timeout=20)
   checkpoint=sqlite3.connect(out/'consumer.sqlite').execute('SELECT cursor FROM source WHERE id=1').fetchone()[0]
   consumer_restart={'after_sequence':int(checkpoint.split(':p')[1].split(':')[0]),'elapsed_s':round(time.monotonic()-start,3)}
   consumer=subprocess.Popen(args,stdout=log,stderr=errors)
  head_unavailable=False
  try:h=head();assert h['source']==source
  except RuntimeError as e:
   if str(e)!='public head network unavailable':raise
   head_unavailable=True
  sample={'elapsed_s':round(time.monotonic()-start,3),'head_status':'unavailable' if head_unavailable else h['status'],'head_request_unavailable':head_unavailable,'source_lag_ms':None if head_unavailable else h['source_lag_ms'],'event_sequence':int(h['cursor'].split(':p')[1].split(':')[0]),'disk_free_bytes':os.statvfs(r).f_bavail*os.statvfs(r).f_frsize}
  for unit in ['hivezilla-validator','hivezilla-direct-worker','hivezilla-bank-v2-producer','hivezilla-bank-v2-origin','hivezilla-stream-relay']:
   raw=subprocess.check_output(['systemctl','show',unit,'-p','MainPID,ActiveState,MemoryCurrent,CPUUsageNSec'],text=True);data=dict(line.split('=',1) for line in raw.splitlines());sample[unit]=data
  for name,path in [('processed',native),('banks',bank)]:sample[name+'_state']=json.loads((path/'STATUS.json').read_text())['state']
  proc=pathlib.Path('/proc')/str(consumer.pid)
  try:
   rss=[line.split()[1] for line in (proc/'status').read_text().splitlines() if line.startswith('VmRSS:')]
   sample['consumer_rss_bytes']=int(rss[0])*1024 if rss else 0
   stat=(proc/'stat').read_text().split();sample['consumer_cpu_ticks']=int(stat[13])+int(stat[14]);sample['consumer_pid']=consumer.pid
  except FileNotFoundError:sample['consumer_rss_bytes']=None
  sample['spool_bytes']=frame_bytes(spool);sample['checkpoint_bytes']=(spool/'CHECKPOINT.json').stat().st_size
  samples.append(sample);write('samples.json',samples);time.sleep(5)
finally:
 if consumer.poll() is None:
  consumer.terminate()
  try:consumer.wait(timeout=20)
  except subprocess.TimeoutExpired:consumer.kill();consumer.wait()
 log.close();errors.close()
rows=[json.loads(line) for line in (out/'rust-consumer.jsonl').read_text().splitlines() if line.strip()];assert rows
sequences=[e['sequence'] for e in rows];assert len(set(sequences))==len(sequences) and all(b==a+1 for a,b in zip(sequences,sequences[1:]))
assert consumer_restart is not None and any(n>consumer_restart['after_sequence'] for n in sequences)
kinds={k:sum(e['kind']==k for e in rows) for k in ['block','confirmed','root','invalidate']}
def pct(values,q):return sorted(values)[min(len(values)-1,int((len(values)-1)*q))] if values else None
latency=[e['source_to_consumer_ms'] for e in rows if e['kind']=='confirmed'];apply=[e['apply_ms'] for e in rows]
# All verification occurs on the server. Export counts and matching results only.
bdb=sqlite3.connect('file:'+str(bank/'feed.sqlite')+'?mode=ro',uri=True);pdb=sqlite3.connect('file:'+str(native/'feed.sqlite')+'?mode=ro',uri=True);matched=0;fail=0
for raw, in bdb.execute('SELECT event_json FROM feed_events ORDER BY sequence DESC LIMIT 400'):
 e=json.loads(raw)
 if e['kind']!='confirmed':continue
 b=e['bank'];row=pdb.execute('SELECT confirmed_json,confirmed_sequence FROM bank_observations WHERE slot=? AND bank=?',[b['slot'],b['bank_id']]).fetchone();s=json.loads(row[0]);ok=e['source_sequence']==row[1] and e['block_height']==s['block_height'] and e['observed_at_unix_ms']==s['observed_at_unix_ms'] and e['recent_blockhash']['last_valid_block_height']==s['last_valid_block_height'] and all(b[k]==s[k] for k in ['slot','bank_id','parent_slot','blockhash','parent_blockhash']);matched+=int(ok);fail+=int(not ok)
assert matched>0 and fail==0
publication={seq:at for seq,at in bdb.execute('SELECT sequence,published_at_unix_ms FROM publication_times')}
callback_control=[];control_consumer=[]
for entry in rows:
 if entry['kind']!='confirmed' or entry['sequence'] not in publication:continue
 event=json.loads(bdb.execute('SELECT event_json FROM feed_events WHERE sequence=?',[entry['sequence']]).fetchone()[0]);delay=publication[entry['sequence']]-event['observed_at_unix_ms'];callback_control.append(delay);control_consumer.append(entry['source_to_consumer_ms']-delay)
cdb=sqlite3.connect(out/'consumer.sqlite');transactions=0;failed=0;missing_errors=0;resolved_keys=0
for raw, in cdb.execute('SELECT decoded_json FROM payloads'):
 decoded=json.loads(raw)
 for block in decoded.get('blocks',[]):
  for tx in block['transactions']:
   transactions+=1;resolved_keys+=int('accountKeys' in tx)
   if tx['success'] is False:failed+=1;missing_errors+=int(tx.get('err') is None)
assert failed>0 and missing_errors==0
cp=json.loads((spool/'CHECKPOINT.json').read_text());blocks=[]
for cursor in cp['events']:
 sha=cursor.rsplit(':',1)[1];path=spool/(sha+'.frame')
 try:wire=path.read_bytes()
 except FileNotFoundError:continue # This snapshot can outlive normal byte eviction.
 assert hashlib.sha256(wire).hexdigest()==sha;n=int.from_bytes(wire[8:12],'little');event=json.loads(wire[16:16+n])['event']
 if event['kind']=='block':blocks.append(event['observed_at_unix_ms'])
# Measure a current boundary, rather than an older checkpoint snapshot.
current=json.loads((spool/'CHECKPOINT.json').read_text());early=[]
for cursor in current['events'][:12]:
 try:
  wire=(spool/(cursor.rsplit(':',1)[1]+'.frame')).read_bytes();n=int.from_bytes(wire[8:12],'little');event=json.loads(wire[16:16+n])['event']
  if event['kind']=='block':early.append(event['observed_at_unix_ms'])
 except FileNotFoundError:pass
assert early and blocks
replay=max(0,(max(blocks)-min(early))/1000)
# Confirm the start position is still accepted after the full soak.
with get('/v2/banks/events?after='+urllib.parse.quote(begin,safe='')) as response:
 assert response.headers.get('X-Blockzilla-Start-Cursor')==begin
 line=response.readline(65537).decode();assert line.startswith('event: status')
summary={'state':'PASSED','seconds':round(time.monotonic()-start,3),'source':source,'start_cursor':begin,'events':len(rows),'kinds':kinds,'confirmed_ready_observations':sum(e['confirmed_hash_ready'] for e in rows),'callback_to_consumer_ms':{'p50':pct(latency,.5),'p95':pct(latency,.95)},'apply_ms':{'p50':pct(apply,.5),'p95':pct(apply,.95)},'callback_to_control_ms':{'p50':pct(callback_control,.5),'p95':pct(callback_control,.95)},'control_to_consumer_ms':{'p50':pct(control_consumer,.5),'p95':pct(control_consumer,.95)},'decoded_transactions':transactions,'failed_transactions':failed,'failed_transactions_missing_error':missing_errors,'transactions_with_resolved_keys':resolved_keys,'native_tuple_matches':matched,'native_tuple_mismatches':fail,'retained_replay_seconds':replay,'start_cursor_retained':True,'retained_frame_bytes':frame_bytes(spool),'head_live_samples':sum(s['head_status']=='live' for s in samples),'head_samples':len(samples),'final_head_status':head()['status'],'consumer_restart':consumer_restart,'exact_resume_no_duplicates':True,'ferno_app_measured':False,'sub_250ms_target_met':pct(latency,.95)<250};write('SUMMARY.json',summary);print(json.dumps(summary))
