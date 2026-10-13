#!/usr/bin/env python3
"""Do not capture historical startup replay as a current live source."""
import argparse,json,subprocess,time,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--binary',type=Path,required=True);p.add_argument('--config',type=Path,required=True);a=p.parse_args()
def rpc(url,method,params):
 request=urllib.request.Request(url,json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),{'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=8) as response:data=json.load(response)
 if 'error' in data:raise RuntimeError(str(data['error'].get('code')))
 return data['result']
pid=int(subprocess.check_output(['systemctl','show','hivezilla-validator.service','-p','MainPID','--value'],text=True))
assert (Path('/proc')/str(pid)/'exe').resolve()==a.binary.resolve(),'running host differs from tested native host'
deadline=time.monotonic()+1700;network_at=0;network_slot=None
while time.monotonic()<deadline:
 try:
  if rpc('http://127.0.0.1:8899','getHealth',[])!='ok':raise RuntimeError('node_unhealthy')
  local=rpc('http://127.0.0.1:8899','getSlot',[{'commitment':'processed'}])
  if time.monotonic()-network_at>5:
   network_slot=rpc('https://api.mainnet-beta.solana.com','getSlot',[{'commitment':'finalized'}]);network_at=time.monotonic()
  if local<network_slot-16:raise RuntimeError('node_catching_up')
  capture_root=Path(json.loads(a.config.read_text())['output_root']);before={p.name for p in capture_root.iterdir() if p.is_dir()}
  result=subprocess.run([str(a.binary),'--ledger',str(a.root/'ledger'),'plugin','load',str(a.config)],capture_output=True,text=True)
  if result.returncode:raise RuntimeError('native_plugin_load_failed: '+(result.stdout+result.stderr)[-1200:])
  new=[p for p in capture_root.iterdir() if p.is_dir() and p.name not in before and (p/'SESSION.json').exists()]
  assert len(new)==1,'native load must create exactly one capture session'
  session=json.loads((new[0]/'SESSION.json').read_text());assert session.get('bank_observations_v2') is True,'v2 callback journal required'
  report=a.root/'reports/bank-v2-native-load.json';report.parent.mkdir(exist_ok=True);report.write_text(json.dumps({'state':'LOADED','validator_binary':str(a.binary),'plugin_config':str(a.config),'validator_pid':pid,'host_executable_verified':True,'session_id':session['session_id'],'agave_revision':session['agave_revision'],'local_processed_slot':local,'network_finalized_slot':network_slot,'loaded_unix_ms':int(time.time()*1000)},indent=2)+'\n')
  print('Native plugin loaded after validator catch-up',flush=True);raise SystemExit(0)
 except (OSError,ValueError,RuntimeError) as error:
  print('Waiting for live capture: '+str(error),flush=True);time.sleep(2)
raise SystemExit('Validator did not reach live health; compact capture was not started')
