#!/usr/bin/env python3
"""Transport and checkpoint fixture; helper output is mocked, not a codec oracle."""
import importlib.util, json, sqlite3, struct, sys, tempfile
from pathlib import Path
spec=importlib.util.spec_from_file_location('scan',str(Path(__file__).resolve().parents[1] / 'debridge-native-approval-scan.py')); m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
b=Path(tempfile.mkdtemp(prefix='debridge-native-controller-test-')).resolve();p=b/'proof';p.mkdir();s=b/'source';s.mkdir();o=b/'out';creds=b/'credentials';creds.write_text('R2_BUCKET=test\n')
source={'session_id':'session1','genesis_hash':'genesis','node_identity':'node','agave_revision':'revision'}
head={'last_commit':'prefix/commits/00000000000000000004.json'};raw=json.dumps(head).encode();(p/'CLOUD-HEAD.json').write_bytes(raw)
proof={'state':'PASSED','recorded_rooted_chain_complete':True,'cloud_head_sha256':m.digest(raw),'source':source,'source_root':str(s),'fixed_root':{'slot':456192001,'bank_id':2,'parent_slot':456192000,'blockhash':[2]*32,'parent_blockhash':[1]*32},'first_bank':{'slot':456192000,'bank_id':1,'parent_slot':456191999,'blockhash':[1]*32,'parent_blockhash':[0]*32},'previous_epoch_parent':{'slot':456191999,'blockhash':[0]*32},'frozen_cloud_event_sequence':4,'publisher_sequence_offset':0,'selected_rooted_banks':2,'epoch':1056};(p/'STATUS.json').write_text(json.dumps(proof))
db=sqlite3.connect(p/'inventory.sqlite');db.execute('CREATE TABLE banks(slot,bank,parent_slot,blockhash,parent_hash,artifact,marker_sha,block_event_seq,block_event_sha,transactions,selected,metadata_verified,root_event_seq)')
objects={}
for i in [1,2]:
 slot=456191999+i; identity={'slot':slot,'bank_id':i,'parent_slot':slot-1,'blockhash':[i]*32,'parent_blockhash':[i-1]*32};art=Path(f'{slot}-{i}.compact');(s/art).mkdir();marker={**source,'produced_blocks':[identity]};mr=json.dumps(marker).encode();(s/art/'DIRECT-AGAVE-COMPACT.v1.json').write_bytes(mr);ms=m.digest(mr)
 event={'kind':'block','bank':identity,'sequence':2*i-1,'source_sequence':100+i,'payload_sha256':ms,'reason':None};env={'marker':marker,'event':event};er=json.dumps(env).encode();wire=b'BZSTRM01'+struct.pack('<II',len(er),0)+er;sha=m.digest(wire);key=f'prefix/chunks/{slot}-{sha}.frame';commit={'schema':'blockzilla-native-stream-commit-v1','profile':'native-processed-compact-v1','session':'session1','canonical':False,'publication_ready':False,**{k:source[k] for k in ['genesis_hash','node_identity','agave_revision']},'sequence':2*i-1,'event':event,'object':key,'sha256':sha,'bytes':len(wire)};objects[f'prefix/commits/{2*i-1:020}.json']=json.dumps(commit).encode();objects[key]=wire if i==1 else wire+b'corrupt'
 db.execute('INSERT INTO banks VALUES(?,?,?,?,?,?,?,?,?,?,1,1,?)',[slot,i,slot-1,bytes([i]*32),bytes([i-1]*32),str(art),ms,2*i-1,m.digest(m.canonical(event)),1,2*i])
db.commit();db.close()
helper=b/'helper';helper.write_text('#!'+sys.executable+'\n'+'''import sys,json,struct
while True:
 h=sys.stdin.buffer.read(4)
 if not h:break
 n=struct.unpack('<I',h)[0];wire=sys.stdin.buffer.read(n);size=struct.unpack('<I',wire[8:12])[0];e=json.loads(wire[16:16+size]);bank=e['event']['bank']
 keys=['authority','token-account','TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA','73dUbDAWUDZRfXM3DcuRgR4q74nr97MyuBt95738JnMz']
 tx={'success':True,'index':0,'signature':'signature','signatures':['signature'],'accountKeys':keys,'innerCoverage':'recorded','message':{'Legacy':{'instructions':[{'program_id_index':2,'accounts':[1,3,0],'data':{'Raw':[4]+[255]*8}}]}},'metadata':{'inner_instructions':[]}}
 def b58(data):
  alphabet='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';n=int.from_bytes(bytes(data),'big');result=''
  while n:n,r=divmod(n,58);result=alphabet[r]+result
  return '1'*(len(data)-len(bytes(data).lstrip(b'\\0')))+result
 block={'slot':bank['slot'],'bankId':bank['bank_id'],'parentSlot':bank['parent_slot'],'blockhash':b58(bank['blockhash']),'parentBlockhash':b58(bank['parent_blockhash']),'transactionCount':1,'finality':'processed','transactions':[tx]}
 decoded={'profile':'native-processed-compact-v1','session':'session1','canonical':False,'publicationReady':False,'epoch':1056,'transactionCount':1,'blocks':[block]};raw=json.dumps(decoded).encode();sys.stdout.buffer.write(struct.pack('<I',len(raw))+raw);sys.stdout.buffer.flush()
''');helper.chmod(0o700)
def get(_,key):r=objects[key];return r,json.loads(r)
m.inventory.r2_get=get;m.inventory.r2_read=lambda _,key,limit:objects[key]
sys.argv=['scan','--inventory-dir',str(p),'--out-dir',str(o),'--decoder',str(helper),'--credentials-file',str(creds),'--max-blocks','1'];m.main()
status=json.loads((o/'STATUS.json').read_text());assert status['state']=='PILOT_PARTIAL' and status['banks_processed']==1 and status['approval_events']==1 and status['complete_frozen_rooted_range'] is False
sys.argv[-1]='100'
try:m.main()
except RuntimeError as e:assert str(e)=='whole_frame_digest_conflict'
else:raise AssertionError('bad frame accepted')
status=json.loads((o/'STATUS.json').read_text());assert status['state']=='COVERAGE_UNKNOWN' and status['banks_processed']==1 and status['approval_events']==1 and status['candidate_token_accounts']==1
assert (o/'candidates.txt').read_text()=='token-account\n'
db=sqlite3.connect(o/'approvals.sqlite');record=json.loads(db.execute('SELECT record_json FROM approvals').fetchone()[0]);assert record['amount']==str(2**64-1) and record['signature']=='signature' and record['frame_sha256']
print('PASS: durable resume, exact source binding, one-bank limit, atomic approval evidence, candidate export, corrupt next frame gives partial coverage without zero totals')
