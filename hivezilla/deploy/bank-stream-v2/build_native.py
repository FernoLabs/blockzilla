#!/usr/bin/env python3
"""Build v2 only against the pinned, patched Agave source on Foundation."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--base', type=Path, required=True)
a = p.parse_args()
src = a.base / 'provider'
host = a.base / 'agave'
revision = subprocess.check_output(['git', '-C', str(host), 'rev-parse', 'HEAD'], text=True).strip()
env = os.environ.copy()
env.update(HIVEZILLA_AGAVE_REVISION=revision,
           CARGO_TARGET_DIR='/home/ubuntu/benchmarks/blockzilla-processed-stream-20261010/target-native-abi',
           CARGO_BUILD_JOBS='8', RUSTFLAGS='-C target-cpu=x86-64 -C target-feature=+aes,+sse2',
           BLOCKZILLA_BUILDER=str(a.base/'blockzilla'),
           BLOCKZILLA_TEST_BINARY=str(a.base/'blockzilla'))
config = []
for name, directory in [('agave-geyser-plugin-interface', 'geyser-plugin-interface'),
                        ('solana-transaction-status', 'transaction-status'),
                        ('solana-transaction-status-client-types', 'transaction-status-client-types')]:
    config += ['--config', f'patch."https://github.com/anza-xyz/agave".{name}.path="{host / directory}"']
commands = [
    (src / 'crates/blockzilla-bank-stream-v2', ['test', '--offline']),
    (src / 'hivezilla/direct-agave', ['test', '--offline', '--ignore-rust-version', '--features', 'bank-v2', *config]),
    (src / 'hivezilla/direct-agave', ['test', '--offline', '--ignore-rust-version', '--lib', '--features', 'bank-v2', *config, '--', '--ignored']),
    (src / 'hivezilla/direct-agave', ['build', '--offline', '--ignore-rust-version', '--release', '--features', 'bank-v2', *config]),
]
receipt = {'state': 'RUNNING', 'agave_revision': revision, 'commands': []}
log_path = a.base / 'native-provider-build.log'
with log_path.open('w') as log:
    for cwd, args in commands:
        cmd = ['/home/ubuntu/.cargo/bin/cargo', '+1.97.1', *args]
        receipt['commands'].append({'cwd': str(cwd), 'argv': cmd})
        print('Checking ' + str(cwd.relative_to(src)) + ' ' + args[0], flush=True)
        result = subprocess.run(cmd, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
        log.flush()
        if result.returncode:
            receipt.update(state='FAILED', completed_unix_ms=int(time.time() * 1000))
            (a.base / 'NATIVE-BUILD.json').write_text(json.dumps(receipt, indent=2) + '\n')
            print(log_path.read_text()[-6000:])
            raise SystemExit(result.returncode)
receipt.update(state='PASSED', completed_unix_ms=int(time.time() * 1000))
(a.base / 'NATIVE-BUILD.json').write_text(json.dumps(receipt, indent=2) + '\n')
print('Native v2 capture and contract checks passed', flush=True)
