from pathlib import Path
import json,os,signal,select,time
r=Path('/home/ubuntu/live/hivezilla-validator-20260930');report=r/'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6';target='/tmp/megaloadon-observe-approved-extra-20261018.py';matches=[]
for path in Path('/proc').iterdir():
 if not path.name.isdigit():continue
 try:
  args=(path/'cmdline').read_bytes().decode().rstrip('\0').split('\0')
  if len(args)==4 and args[0].endswith('python3') and args[1:]==['-B','-u',target]:matches.append(int(path.name))
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
assert len(matches)==1
pid=matches[0];fd=os.pidfd_open(pid)
try:
 assert (Path('/proc')/str(pid)/'cmdline').read_bytes().decode().rstrip('\0').split('\0')[1:]==['-B','-u',target]
 signal.pidfd_send_signal(fd,signal.SIGTERM)
 ready,_,_=select.select([fd],[],[],10);assert ready
finally:os.close(fd)
flow=json.loads((report/'FLOW.json').read_text());flow.update(state='STOPPED_BEFORE_PLANNED_TRANSPORT_RESTART',stopped_unix_ms=int(time.time()*1000),observer_pid=pid,service_actions=[])
path=report/'FLOW-PRE-TRANSPORT.json';assert not path.exists();path.write_text(json.dumps(flow,separators=(',',':'))+'\n');path.chmod(0o600)
print(json.dumps({'state':flow['state'],'samples':len(flow['samples']),'faults':len(flow['faults']),'observer_pid':pid,'service_actions':[]}))
