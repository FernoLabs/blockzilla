import base64, hashlib, json, os, re, subprocess, sys
from pathlib import Path
root=Path('/home/ubuntu/live/hivezilla-validator-20260930')
raw=sys.stdin.buffer.read(1048577); assert len(raw)<=1048576
package=json.loads(raw)
assert re.fullmatch(r'protocol-retry-20261018-[0-9a-f]{16}',package['release'])
files={name:base64.b64decode(value,validate=True) for name,value in package['files'].items()}
expected={'cold_storage.py','test_cold_storage.py','storage_jobs.py','test_storage_jobs.py'}
assert set(files)==expected|{'MANIFEST.json'}
assert hashlib.sha256(files['MANIFEST.json']).hexdigest()==package['manifest_sha256']
manifest=json.loads(files['MANIFEST.json']);assert set(manifest)==expected
assert all(hashlib.sha256(files[n]).hexdigest()==h for n,h in manifest.items())
release=root/'releases'/package['release'];release.mkdir(mode=0o700,exist_ok=True)
assert not release.is_symlink() and release.resolve()==release
for name,data in files.items():
 path=release/name;assert not path.is_symlink()
 if path.exists():assert path.read_bytes()==data
 else:path.write_bytes(data);path.chmod(0o600)
result=subprocess.run(['/usr/bin/python3','-B','-m','unittest','discover','-s',str(release),'-p','test_*.py'],capture_output=True,text=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),timeout=90)
assert result.returncode==0,result.stderr[-6000:]
for p in release.iterdir():p.chmod(0o444)
release.chmod(0o555)
report=root/'reports/native-recovery-20261018/recurrence/protocol-retry';report.mkdir(mode=0o700,parents=True,exist_ok=True)
receipt=dict(state='STAGED_TESTED_NOT_ACTIVE',release=str(release),manifest=manifest,manifest_sha256=package['manifest_sha256'],test_output=result.stderr.strip(),service_actions=[])
assert not (report/'STAGE.json').exists()
(report/'STAGE.json').write_text(json.dumps(receipt,indent=2)+'\n');(report/'STAGE.json').chmod(0o600)
print(json.dumps(receipt))
