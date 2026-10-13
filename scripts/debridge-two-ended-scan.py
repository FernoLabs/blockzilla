#!/usr/bin/env python3
"""Finite, two-host scan with durable, exclusive epoch ownership.

The controller uses the owner's existing SSH agent. Remote scanners run in
bounded systemd units and scan only one epoch. If this controller stops, each
remote unit can finish its current epoch, but no further epoch can start.
Claims never expire and are never transferred automatically.
"""
import argparse
import concurrent.futures
import contextlib
import copy
import fcntl
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import threading
import time


FOUNDATION = '/home/ubuntu/live/hivezilla-validator-20260930/jobs/debridge-delegation-audit-20261013'
NAS = '/volume1/blockzilla/jobs/debridge-delegation-audit-20261013'
HOSTS = {
    'megaloadon': {
        'ssh': ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', 'ubuntu@213.239.141.61'],
        'root': FOUNDATION, 'system': True, 'threads': 8, 'memory': '6G',
        'binary': FOUNDATION + '/blockzilla-delegation-audit-http-r5',
        'source': ['--gateway', 'http://127.0.0.1:18880', '--cache', FOUNDATION + '/cache', '--allow-insecure-http'],
        'order': 'ascending',
    },
    'nas': {
        'ssh': ['ssh', '-p', '22', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', 'ach@192.168.1.46'],
        'root': NAS, 'system': False, 'threads': 2, 'memory': '2G',
        'binary': NAS + '/blockzilla-delegation-audit',
        'source': ['--archive-root', '/volume1/blockzilla/archive', '--source-generation-prefix', 'nas-compact-v2-operator-20261013'],
        'order': 'descending',
    },
}
for number in (2, 3):
    host = copy.deepcopy(HOSTS['megaloadon'])
    host['root'] = FOUNDATION + f'/parallel-{number}'
    HOSTS[f'megaloadon{number}'] = host

# Only structured JSON enters this remote helper. Scanner arguments are never
# interpolated into a shell command. The SSH command itself is shell-quoted.
REMOTE = r'''
import json,sqlite3,subprocess,sys,os
p=json.load(sys.stdin)
root=p['root']; epoch=p['epoch']; system=p['system']; unit=p['unit']
os.makedirs(root,exist_ok=True)
ctl=['systemctl']+([] if system else ['--user'])
def dbrow():
    path=root+'/approvals.sqlite'
    if not os.path.exists(path): return None
    c=sqlite3.connect('file:'+path+'?mode=ro',uri=True,timeout=10)
    c.row_factory=sqlite3.Row
    try:
        row=c.execute('select epoch,first_row,next_row,total_rows,state,error,committed_transactions,approval_events from epochs where epoch=?',(epoch,)).fetchone()
        return dict(row) if row else None
    finally: c.close()
def service():
    r=subprocess.run(ctl+['show',unit,'-p','LoadState','-p','ActiveState','-p','SubState','-p','Result','-p','ExecMainStatus'],text=True,capture_output=True)
    return dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
state=service(); row=dbrow()
if state.get('ActiveState') in ('active','activating','deactivating'):
    print(json.dumps({'state':'running','service':state,'epoch':row})); sys.exit(0)
if row and row['state']=='complete' and row['next_row']==row['total_rows']:
    if state.get('LoadState')!='not-found' and (state.get('Result')!='success' or state.get('ExecMainStatus')!='0'):
        print(json.dumps({'state':'failed','service':state,'epoch':row})); sys.exit(0)
    print(json.dumps({'state':'complete','service':state,'epoch':row})); sys.exit(0)
if state.get('LoadState')!='not-found':
    print(json.dumps({'state':'failed','service':state,'epoch':row})); sys.exit(0)
if p['action']=='status':
    print(json.dumps({'state':'not_started','service':state,'epoch':row})); sys.exit(0)
if not os.path.isfile(p['binary']):
    print(json.dumps({'state':'waiting_for_binary','epoch':row})); sys.exit(0)
cmd=(['sudo','-n'] if system else [])+['systemd-run']+([] if system else ['--user'])
cmd+=['--unit='+unit,'--property=Nice=19','--property=CPUQuota='+str(p['threads']*100)+'%','--property=MemoryMax='+p['memory'],'--property=IOWeight=10','--property=IOSchedulingClass=idle']
if system: cmd+=['--property=User=ubuntu']
cmd+=['--working-directory='+root]+p['argv']
r=subprocess.run(cmd,text=True,capture_output=True,timeout=30)
if r.returncode and service().get('ActiveState') in ('active','activating'):
    print(json.dumps({'state':'running','service':service(),'epoch':dbrow()})); sys.exit(0)
print(json.dumps({'state':'started' if r.returncode==0 else 'launch_error','exit_code':r.returncode,'detail':r.stderr[-2000:],'epoch':row}))
'''


class Claims:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def connection(self):
        c = sqlite3.connect(self.path, timeout=30)
        try:
            c.row_factory = sqlite3.Row
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('PRAGMA synchronous=FULL')
            with c:
                yield c
        finally:
            c.close()

    def initialize(self, start, end, seed):
        with self.connection() as c:
            c.executescript('CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS claims(epoch INTEGER PRIMARY KEY,owner TEXT,state TEXT NOT NULL,claimed_at REAL,completed_at REAL,receipt TEXT);')
            spec = json.dumps({'start': start, 'end': end, 'kind': 'debridge-two-ended-v1'}, sort_keys=True)
            old = c.execute("SELECT value FROM settings WHERE key='spec'").fetchone()
            if old and old[0] != spec:
                raise RuntimeError('claim range changed; use a new claims database')
            c.execute("INSERT OR IGNORE INTO settings VALUES('spec',?)", (spec,))
            c.executemany("INSERT OR IGNORE INTO claims(epoch,state) VALUES(?,'unclaimed')", ((e,) for e in range(start, end + 1)))
            for row in seed:
                if not start <= row['epoch'] <= end:
                    raise RuntimeError('seed epoch outside claim range')
                complete = row['state'] == 'complete' and row['next_row'] == row['total_rows']
                state = 'complete' if complete else 'claimed'
                prior = c.execute('SELECT owner,state FROM claims WHERE epoch=?', (row['epoch'],)).fetchone()
                if prior['owner'] not in (None, 'nas'):
                    raise RuntimeError('seed conflicts with an existing owner')
                if prior['state'] != 'unclaimed':
                    continue
                c.execute('UPDATE claims SET owner=?,state=?,claimed_at=COALESCE(claimed_at,?),completed_at=?,receipt=? WHERE epoch=?', ('nas', state, time.time(), time.time() if complete else None, json.dumps({'seed_checkpoint': row}), row['epoch']))

    def claim(self, owner):
        with self.connection() as c:
            c.execute('BEGIN IMMEDIATE')
            active = c.execute("SELECT epoch FROM claims WHERE owner=? AND state='claimed' ORDER BY epoch", (owner,)).fetchall()
            if len(active) > 1:
                raise RuntimeError('owner has more than one active claim')
            if active:
                return active[0]['epoch']
            order = 'ASC' if HOSTS[owner]['order'] == 'ascending' else 'DESC'
            row = c.execute("SELECT epoch FROM claims WHERE state='unclaimed' ORDER BY epoch " + order + ' LIMIT 1').fetchone()
            if row is None:
                return None
            c.execute("UPDATE claims SET owner=?,state='claimed',claimed_at=? WHERE epoch=? AND state='unclaimed'", (owner, time.time(), row['epoch']))
            return row['epoch']

    def complete(self, owner, epoch, receipt):
        row = receipt.get('epoch') or {}
        if not (receipt['state'] == 'complete' and row.get('epoch') == epoch and row.get('state') == 'complete' and row.get('next_row') == row.get('total_rows')):
            raise RuntimeError('remote completion proof is incomplete')
        with self.connection() as c:
            n = c.execute("UPDATE claims SET state='complete',completed_at=?,receipt=? WHERE epoch=? AND owner=? AND state='claimed'", (time.time(), json.dumps(receipt), epoch, owner)).rowcount
            if n != 1:
                raise RuntimeError('completion has no exclusive claim')

    def rows(self):
        with self.connection() as c:
            return [dict(r) for r in c.execute('SELECT * FROM claims ORDER BY epoch')]


def remote(owner, epoch, action):
    host = HOSTS[owner]
    unit = f'blockzilla-debridge-two-ended-{owner}-e{epoch}-20261013.service'
    argv = [host['binary']] + host['source'] + [
        '--epoch-start', str(epoch), '--epoch-end', str(epoch), '--start-slot', '386160031',
        '--output', host['root'] + '/approvals.sqlite', '--threads', str(host['threads']),
        '--message-schema', 'post-unknown-instruction-fallbacks-v1',
        '--metadata-schema', 'unmarked-historical-compatibility', '--order', host['order'],
        '--candidates', host['root'] + '/candidates.txt', '--status', host['root'] + '/STATUS.json',
    ]
    payload = {k: host[k] for k in ('root', 'system', 'threads', 'memory', 'binary')}
    payload.update(epoch=epoch, unit=unit, action=action, argv=argv)
    cmd = host['ssh'] + [shlex.join(['python3', '-c', REMOTE])]
    run = subprocess.run(cmd, input=json.dumps(payload), text=True, capture_output=True, timeout=65)
    if run.returncode:
        raise RuntimeError('SSH failed: ' + run.stderr[-1200:])
    return json.loads(run.stdout)


def atomic_json(path, value):
    temp = path.with_suffix('.tmp')
    with temp.open('w') as f:
        json.dump(value, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def run(args):
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    with (work / 'controller.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        claims = Claims(work / 'claims.sqlite')
        seed = json.loads(Path(args.seed).read_text()) if args.seed else []
        claims.initialize(args.epoch_start, args.epoch_end, seed)
        workers, write_lock = {}, threading.Lock()

        def report(owner, value):
            with write_lock:
                observed_at = time.time()
                workers[owner] = dict(value, updated_at=observed_at)
                rows = claims.rows()
                counts = {state: sum(r['state'] == state for r in rows) for state in ('complete', 'claimed', 'unclaimed')}
                atomic_json(work / 'STATUS.json', {'kind': 'debridge-two-ended-controller-v1', 'updated_at': time.time(), 'epochs': [args.epoch_start, args.epoch_end], 'counts': counts, 'workers': workers, 'claims_never_expire': True, 'history_complete': counts['complete'] == len(rows), 'controller_required_for_next_epoch': True})
                print(json.dumps({'owner': owner, 'observed_at': observed_at, **value}), flush=True)

        def worker(owner):
            while True:
                epoch = claims.claim(owner)
                if epoch is None:
                    report(owner, {'state': 'met_other_worker', 'epoch': None})
                    return
                last = None
                while True:
                    try:
                        result = remote(owner, epoch, 'start')
                        report(owner, dict(result, claimed_epoch=epoch))
                        if result['state'] == 'complete':
                            claims.complete(owner, epoch, result)
                            break
                        if result['state'] in ('failed', 'launch_error'):
                            report(owner, {'state': 'needs_attention', 'claimed_epoch': epoch, 'failure': result})
                            return
                        last = result
                    except Exception as error:
                        report(owner, {'state': 'transport_wait', 'claimed_epoch': epoch, 'error': str(error), 'last_remote_receipt': last})
                    time.sleep(args.poll_seconds)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(args.workers)) as pool:
            futures = [pool.submit(worker, owner) for owner in args.workers]
            for future in futures:
                future.result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work', required=True)
    parser.add_argument('--seed')
    parser.add_argument('--epoch-start', type=int, default=893)
    parser.add_argument('--epoch-end', type=int, default=1049)
    parser.add_argument('--poll-seconds', type=float, default=20)
    parser.add_argument('--workers', nargs='+', choices=tuple(HOSTS), default=list(HOSTS))
    parser.add_argument('--foundation-work', default=FOUNDATION)
    parser.add_argument('--gateway-url', choices=('http://127.0.0.1:18880', 'http://127.0.0.1:18881'), default='http://127.0.0.1:18880')
    parser.add_argument('--detach', action='store_true')
    args = parser.parse_args()
    if args.epoch_start < 893 or args.epoch_end < args.epoch_start or args.poll_seconds < 5:
        parser.error('invalid scan range or polling interval')
    if len(set(args.workers)) != len(args.workers):
        parser.error('duplicate worker')
    for name, host in HOSTS.items():
        if name.startswith('megaloadon'):
            host['source'][1] = args.gateway_url
    if args.foundation_work != FOUNDATION:
        if not args.foundation_work.startswith(FOUNDATION + '/') or '..' in Path(args.foundation_work).parts:
            parser.error('Foundation work must be inside the task directory')
        HOSTS['megaloadon']['root'] = args.foundation_work
        source = HOSTS['megaloadon']['source']
        source[source.index('--cache') + 1] = args.foundation_work + '/cache'
    if args.detach:
        work = Path(args.work)
        work.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, str(Path(__file__).resolve())] + [arg for arg in sys.argv[1:] if arg != '--detach']
        with (work / 'controller.log').open('ab') as output:
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output, stderr=output, start_new_session=True)
        (work / 'controller.pid').write_text(str(process.pid) + '\n')
        print(json.dumps({'controller_pid': process.pid, 'work': str(work)}))
    else:
        run(args)


if __name__ == '__main__':
    main()
