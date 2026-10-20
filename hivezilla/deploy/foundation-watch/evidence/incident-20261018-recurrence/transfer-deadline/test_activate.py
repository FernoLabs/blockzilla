"""Run exact frozen apply() with local fakes; no service or host access."""
import argparse
import ast
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import types

path = Path('/tmp/megaloadon-activate-transfer-deadline-20261018.py')
assert hashlib.sha256(path.read_bytes()).hexdigest() == 'a8ec7e7dd2d9699d1760ac6949750b6bfafed9dc7561f6e8283c547da58d9566'
tree = ast.parse(path.read_text())
code = compile(ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)
                               and node.name == 'apply'], type_ignores=[]), str(path), 'exec')


def case(change):
    with tempfile.TemporaryDirectory() as folder:
        report = Path(folder)
        live, guardian = 'live.service', 'guardian.service'
        registry, pin = report / 'registry', report / 'pin'
        live_override, guardian_override = report / 'live.conf', report / 'guardian.conf'
        expected_pids = {str(n): n for n in range(1, 9)}
        old_command, old_guardian = ['python3', 'old-cold.py'], ['python3', 'old-jobs.py']
        plan = dict(final_status_sha256='final', cloud_proof_sha256='cloud', files={},
                    old_live_state=dict(MainPID='101', InvocationID='a' * 32),
                    old_guardian_state=dict(MainPID='102', InvocationID='b' * 32),
                    old_live_command=old_command, old_guardian_command=old_guardian,
                    registry_before_sha256='oldsha', registry_after_sha256='newsha', source={})
        record = dict(state='PREPARED', plan_sha256='plan')
        state = dict(first_stop_failed=False, rollback_stopped=False)
        actions, writes, saved = [], [], {}

        def require(ok, name):
            if not ok:
                raise RuntimeError(name)

        def read(p):
            if p == pin:
                changed = ((change == 'pin_before' and state['first_stop_failed'])
                           or (change == 'pin_after' and state['rollback_stopped']))
                return b'changed' if changed else b'pin'
            if p == registry or p.name == 'registry-before.json':
                return b'{}'
            if p.name == 'registry-after.json':
                return b'{"new":true}'
            return {'live-before.unit': b'live-unit', 'guardian-before.unit': b'guardian-unit',
                    'live-before.conf': b'old-live', 'live-after.conf': b'new-live'}[p.name]

        def pids():
            if change == 'pids_before' and state['first_stop_failed']:
                return {'changed': 999}
            return expected_pids.copy()

        def run(*args):
            actions.append(list(args))
            if args == ('sudo', 'systemctl', 'stop', guardian):
                state['first_stop_failed'] = True
                raise TimeoutError('first stop partly succeeded')
            if args == ('sudo', 'systemctl', 'stop', guardian, live):
                state['rollback_stopped'] = True
            return ''

        def save(p, raw):
            saved[p.name] = json.loads(raw)

        env = dict(release_check=lambda: None, obj=lambda p: copy.deepcopy(record if p.name == 'INSTALLATION.json' else plan),
                   REPORT=report, REGISTRY=registry, PIN=pin, LIVE=live, GUARDIAN=guardian,
                   LIVE_OVERRIDE=live_override, GUARDIAN_OVERRIDE=guardian_override,
                   PIDS=expected_pids, PIN_SHA='pinsha', OLD_REGISTRY_SHA='oldsha', require=require, digest=lambda p: 'plan',
                   read=read, pids=pids, authority=lambda *args: None, unit_text=lambda u: 'live-unit' if u == live else 'guardian-unit',
                   show=lambda u: dict(MainPID='101' if u == live else '102', InvocationID=('a' if u == live else 'b') * 32),
                   argv=lambda pid: old_command if pid == '101' else old_guardian,
                   now=lambda: 1000, save=save, dump=lambda v: json.dumps(v).encode(), run=run,
                   json=json, sha=lambda raw: {b'pin': 'pinsha', b'{}': 'oldsha'}.get(raw, 'changed'),
                   subprocess=types.SimpleNamespace(check_output=lambda *a, **k: b'old-live'),
                   install_unit=lambda *a: writes.append(('unit', a)),
                   registry_install=lambda raw: writes.append(('registry', raw)),
                   running=lambda u, expected: dict(pid=900 if u == live else 901),
                   healthy=lambda *args: dict(state='HEALTHY'), print=lambda *a, **k: None)
        exec(code, env)
        try:
            env['apply'](argparse.Namespace(final_status_sha256='final', cloud_proof_sha256='cloud'))
        except RuntimeError as error:
            assert str(error) == 'transfer_deadline_activation_failed_inspect_receipts'
        else:
            raise AssertionError('failed activation accepted')
        assert actions[0] == ['sudo', 'systemctl', 'stop', guardian]
        assert 'ROLLBACK.json' in saved, 'partial first stop must enter rollback'
        if change == 'none':
            assert saved['ROLLBACK.json']['state'] == 'PREVIOUS_HELPER_RESTORED'
            assert writes and state['rollback_stopped']
        else:
            assert saved['ROLLBACK.json']['state'] == 'ROLLBACK_FAILED'
            assert writes == [], 'changed identity must block configuration writes'
            if change in ('pin_before', 'pids_before'):
                assert len(actions) == 1, 'changed identity must also block rollback stop'
            else:
                assert state['rollback_stopped']


for change in ('none', 'pin_before', 'pids_before', 'pin_after'):
    case(change)
print('PASS: partial first-stop rollback; source/PID guards before stop; source guard before restore')
