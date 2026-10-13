#!/usr/bin/env python3
"""Replace the tested transport only. Preserve the native process and spools."""
import argparse, hashlib, json, pathlib, shutil, subprocess, time
from install import install

p = argparse.ArgumentParser()
p.add_argument('--root', type=pathlib.Path, required=True)
p.add_argument('--binary', type=pathlib.Path, required=True)
p.add_argument('--name', required=True)
p.add_argument('--test-log', type=pathlib.Path, required=True)
p.add_argument('--build-log', type=pathlib.Path, required=True)
a = p.parse_args()
assert a.name.startswith('bank-stream-v2-') and '/' not in a.name
assert 'test result: ok.' in a.test_log.read_text()
assert 'test result: FAILED' not in a.test_log.read_text()
assert 'Finished' in a.build_log.read_text() and 'error:' not in a.build_log.read_text()
r = a.root
report = r / 'reports/bank-stream-v2-20261010'
path = report / 'DEPLOYMENT.json'
d = json.loads(path.read_text())
native_pid = subprocess.check_output(['systemctl', 'show', 'hivezilla-validator', '-p', 'MainPID', '--value'], text=True).strip()
release = r / 'releases' / a.name
release.mkdir(exist_ok=False)
shutil.copy2(a.binary, release / 'hivezilla-stream')
sha = hashlib.sha256(a.binary.read_bytes()).hexdigest()
for name in ['hivezilla-bank-v2-origin', 'hivezilla-stream-relay']:
    lines = subprocess.check_output(['systemctl', 'cat', name + '.service'], text=True).splitlines()
    line = [s for s in lines if s.startswith('ExecStart=') and s != 'ExecStart='][-1]
    args = line.split(' ', 1)[1]
    override = pathlib.Path('/etc/systemd/system') / (name + '.service.d/zzzzzzzz-bank-transport.conf')
    install(override, '[Service]\nExecStart=\nExecStart=' + str(release / 'hivezilla-stream') + ' ' + args + '\n', report)
subprocess.run(['sudo', 'systemctl', 'daemon-reload'], check=True)
subprocess.run(['sudo', 'systemctl', 'restart', 'hivezilla-bank-v2-origin'], check=True)
subprocess.run(['sudo', 'systemctl', 'restart', 'hivezilla-stream-relay'], check=True)
assert subprocess.check_output(['systemctl', 'show', 'hivezilla-validator', '-p', 'MainPID', '--value'], text=True).strip() == native_pid
d.update(transport_release=str(release), transport_artifacts={'hivezilla-stream': sha}, updated_unix_ms=int(time.time() * 1000))
path.write_text(json.dumps(d, indent=2) + '\n')
receipt = {'release': str(release), 'artifact_sha256': sha, 'validator_pid_unchanged': True, 'spools_preserved': True, 'source_preserved': True}
(release / 'TRANSPORT-UPGRADE.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
