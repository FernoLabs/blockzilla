import importlib.util
from contextlib import closing
import json
from pathlib import Path
import re
import shlex
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("storage_jobs", Path(__file__).with_name("storage_jobs.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Fixture:
    def __init__(self, directory, continuous=False):
        self.root = Path(directory).resolve()
        (self.root / "config").mkdir()
        self.source = dict(genesis_hash="genesis", node_identity="node", agave_revision="a" * 40,
                           session_id="0" * 16 + "18de229d3fc8a9d9-275b05")
        self.active = dict(self.source, session_id="0" * 16 + "18df884b2474cf8e-20b258")
        if continuous:
            self.source = self.active.copy()
        (self.root / "config/native-active-source.json").write_text(json.dumps(self.active))
        helper = self.root / "releases/test/cold_storage.py"
        helper.parent.mkdir(parents=True); helper.write_text("# exact tested helper\n")
        session = self.source["session_id"]
        self.paths = {"--output": self.root / "compact/direct-agave-processed-live" / session,
                      "--bank-output": self.root / "compact/bank-v2-live" / session,
                      "--capture": self.root / "captures/direct-agave-live" / session,
                      "--publisher-state": self.root / "reports/native-processed-r2-bank-v2" / session,
                      "--credentials-file": self.root / "config/native-cold-storage.env",
                      "--v1-spool": self.root / "stream-relay-spool-bank-v2" / session,
                      "--bank-origin-spool": self.root / "bank-v2-origin-spool" / session}
        args = ["/usr/bin/python3", str(helper), "archive"]
        for option, path in self.paths.items():
            args.extend([option, str(path)])
            if option != "--credentials-file": path.mkdir(parents=True)
        args += ["--follow", "--retire", "--keep-seconds", "7200", "--batch-banks", "64",
                 "--parallel-bundles", "4", "--max-batches", "460"]
        if not continuous: args += ["--max-archived-banks", "10"]
        self.job = dict(unit=m.LIVE_UNIT if continuous else "hivezilla-native-cold-storage-20261016-source-18de229d.service",
            source=self.source, output=str(self.paths["--output"]), max_archived_banks=None if continuous else 10,
            namespace="b" * 64, helper=str(helper), helper_sha256=m.file_sha256(helper), expected_command=args,
            expected_command_sha256=m.command_sha256(args), allow_restart=True, max_retries=3, retry_delay_seconds=60)
        (self.paths["--output"] / "FEED.json").write_text(json.dumps(self.source))
        (self.paths["--bank-output"] / "FEED.json").write_text(json.dumps(dict(self.source, payload_root=self.job["output"])))
        (self.paths["--capture"] / "SESSION.json").write_text(json.dumps(dict(session_id=session,
            agave_revision=self.source["agave_revision"], config={key: self.source[key] for key in ("genesis_hash", "node_identity")})))
        self.state = self.paths["--output"] / "cold-storage"; self.state.mkdir()
        with closing(sqlite3.connect(self.state / "archive.sqlite")) as db, db:
            db.executescript("CREATE TABLE meta(id INTEGER PRIMARY KEY,value TEXT);"
                             "CREATE TABLE archives(id INTEGER PRIMARY KEY,retired INTEGER);"
                             "CREATE INDEX archives_retired ON archives(retired);")
            db.execute("INSERT INTO meta VALUES(1,?)", (json.dumps(dict(source=self.source, namespace=self.job["namespace"])),))
            db.executemany("INSERT INTO archives VALUES(?,1)", [(1,), (2,), (3,)])
        self.show = dict(ActiveState="active", SubState="running", MainPID="99", Result="success",
                         InvocationID="c" * 32, Restart="no", FreezerState="running")
        self.proc = self.root / "proc"; (self.proc / "99").mkdir(parents=True)
        (self.proc / "99/cmdline").write_bytes(("\0".join(args) + "\0").encode())
        self.calls = []
        self.unit_args = args.copy()
        self.status(1000)
        self.registry = self.root / "config/storage-jobs.json"
        self.write_registry()

    def write_registry(self):
        self.registry.write_text(json.dumps(dict(schema=m.SCHEMA, root=str(self.root), jobs=[self.job])))
        self.registry.chmod(0o600)

    def status(self, now):
        (self.state / "STATUS.json").write_text(json.dumps(dict(state="RUNNING", updated_unix_ms=now,
            storage_progress="ADVANCING", last_archive_progress_unix_ms=now)))

    def fail(self, kind="ReadTimeoutError", http_status=None, invocation=None, now=1000, network_error_type=None):
        self.show.update(ActiveState="failed", SubState="failed", MainPID="0", Result="exit-code")
        if invocation is not None: self.show["InvocationID"] = invocation
        failure = dict(state="FAILED", error="archive_operation_failed", error_type=kind,
                       http_status=http_status, invocation_id=self.show["InvocationID"], source=self.source,
                       updated_unix_ms=now, network_error_type=network_error_type)
        (self.state / "LAST-FAILURE.json").write_text(json.dumps(failure))
        (self.state / "STATUS.json").write_text(json.dumps(failure))

    def run(self, *args):
        self.calls.append(args)
        if args[:3] == ("sudo", "systemctl", "cat"):
            return "[Service]\nExecStart=" + shlex.join(self.unit_args) + "\n"
        if args[:2] == ("systemctl", "show"):
            return "\n".join(f"{key}={value}" for key, value in self.show.items())
        if args[:3] in [("sudo", "systemctl", "reset-failed"), ("sudo", "systemctl", "start")]:
            return ""
        raise AssertionError(args)

    def inspect(self, *_args, **kwargs):
        kwargs.setdefault("now_ms", 1000)
        kwargs.setdefault("runner", self.run)
        return m.inspect_job(self.root, self.job, proc_root=self.proc, **kwargs)

    def mutations(self):
        return [args for args in self.calls if args[:3] in
                [("sudo", "systemctl", "reset-failed"), ("sudo", "systemctl", "start")]]


class StorageJobsTests(unittest.TestCase):
    def test_registry_binds_exact_old_source_cap_paths_and_raw_bytes_digest(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d)
            root, jobs, digest = m.load_registry(f.registry)
            self.assertEqual((root, jobs), (f.root, [f.job]))
            self.assertEqual(digest, m.file_sha256(f.registry))
            self.assertNotEqual(jobs[0]["source"], f.active)
            self.assertEqual(f.inspect()["state"], "RUNNING")
            self.assertEqual(f.mutations(), [])

    def test_uncapped_old_job_rejects_and_only_exact_active_live_source_is_allowed(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d); f.job["max_archived_banks"] = None; f.write_registry()
            with self.assertRaisesRegex(RuntimeError, "uncapped_storage_scope"):
                m.load_registry(f.registry)
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d, continuous=True)
            m.load_registry(f.registry)
            self.assertEqual(f.inspect()["remaining_banks"], None)
            (f.root / "config/native-active-source.json").write_text(json.dumps(dict(f.active,
                session_id="1" * 32 + "-123")))
            with self.assertRaisesRegex(RuntimeError, "uncapped_storage_scope"):
                m.load_registry(f.registry)
            self.assertFalse(f.inspect()["retryable"])

    def test_command_hash_and_scope_changes_cannot_add_authority(self):
        for change in ("cap", "credentials", "helper", "writeable", "duplicate"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                f = Fixture(d)
                if change == "cap": f.job["max_archived_banks"] = 11
                if change == "credentials":
                    f.job["expected_command"][f.job["expected_command"].index("--credentials-file") + 1] = "/other/credentials"
                    f.job["expected_command_sha256"] = m.command_sha256(f.job["expected_command"])
                if change == "helper": f.job["helper"] = "/tmp/cold_storage.py"
                if change == "duplicate": f.job["expected_command"] += ["--output", f.job["output"]]
                f.write_registry()
                if change == "writeable": f.registry.chmod(0o666)
                with self.assertRaises(RuntimeError): m.load_registry(f.registry)

    def test_inspection_blocks_modified_helper_command_namespace_and_broad_restart(self):
        for change in ("helper", "command", "namespace", "restart", "source"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                f = Fixture(d); f.fail()
                if change == "helper": Path(f.job["helper"]).write_text("changed")
                if change == "command": f.unit_args += ["--unexpected"]
                if change == "restart": f.show["Restart"] = "on-failure"
                if change == "source":
                    (f.paths["--output"] / "FEED.json").write_text(json.dumps(f.active))
                if change == "namespace":
                    with closing(sqlite3.connect(f.state / "archive.sqlite")) as db, db:
                        db.execute("UPDATE meta SET value=? WHERE id=1", (json.dumps(dict(source=f.source, namespace="d" * 64)),))
                snapshot = f.inspect()
                self.assertFalse(snapshot["retryable"])
                self.assertIn("storage_job_unavailable", snapshot["issues"])
                self.assertEqual(f.mutations(), [])

    def test_only_network_types_and_fixed_transient_http_statuses_can_retry(self):
        cases = [("ReadTimeoutError", None, True), ("EndpointConnectionError", None, True),
                 ("BundleTransferTimeout", None, True), ("BundleTransferError", None, False),
                 ("ConnectTimeoutError", None, True), ("ConnectionClosedError", None, True),
                 ("ClientError", 503, True), ("ClientError", 429, True), ("ClientError", 403, False),
                 ("ClientError", None, False), ("NoCredentialsError", 503, False),
                 ("RuntimeError", 503, False), ("OSError", None, False), ("SSLError", None, False)]
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d)
            for kind, status, allowed in cases:
                with self.subTest(kind=kind, status=status):
                    f.fail(kind, status)
                    self.assertEqual(f.inspect()["retryable"], allowed)
            failure = json.loads((f.state / "LAST-FAILURE.json").read_text())
            failure.update(error_type="ReadTimeoutError", invocation_id="d" * 32)
            (f.state / "LAST-FAILURE.json").write_text(json.dumps(failure))
            self.assertFalse(f.inspect()["retryable"])

    def test_bundle_timeout_uses_existing_fresh_invocation_source_and_bounded_retry_policy(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d); f.fail('BundleTransferTimeout'); state = {'jobs': {}}; saved = []
            save = lambda value: saved.append(json.dumps(value))
            m.retry_job(f.root, f.job, f.inspect(), state, save, 1000, runner=f.run, inspector=f.inspect)
            self.assertEqual(f.mutations(), [])
            m.retry_job(f.root, f.job, f.inspect(now_ms=61000), state, save, 61000, runner=f.run, inspector=f.inspect)
            self.assertEqual(len(f.mutations()), 2)
            self.assertEqual(state['jobs'][m.job_key(f.job)]['attempts'], 1)
            m.retry_job(f.root, f.job, f.inspect(now_ms=100000), state, save, 100000, runner=f.run, inspector=f.inspect)
            self.assertEqual(len(f.mutations()), 2)
            f.fail('BundleTransferTimeout'); failure = json.loads((f.state / 'LAST-FAILURE.json').read_text())
            failure['source'] = f.active
            (f.state / 'LAST-FAILURE.json').write_text(json.dumps(failure))
            self.assertFalse(f.inspect()['retryable'])

    def test_stream_protocol_retry_requires_fixed_subtype_and_matching_failure_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d)
            for subtype in (None, 'ProtocolError', 'IncompleteReadError', 'DO_NOT_PRINT private URL', {}):
                with self.subTest(subtype=subtype):
                    f.fail('ResponseStreamingError', network_error_type=subtype)
                    snapshot = f.inspect()
                    allowed = subtype == 'ProtocolError'
                    self.assertEqual(snapshot['retryable'], allowed)
                    self.assertEqual(snapshot['network_error_type'], 'ProtocolError' if allowed else None)
                    self.assertNotIn('DO_NOT_PRINT', json.dumps(snapshot))
            f.fail('ResponseStreamingError', network_error_type='ProtocolError')
            state = {'jobs': {}}; save = lambda value: None
            m.retry_job(f.root, f.job, f.inspect(), state, save, 1000, runner=f.run, inspector=f.inspect)
            self.assertEqual(f.mutations(), [])
            m.retry_job(f.root, f.job, f.inspect(now_ms=61000), state, save, 61000,
                        runner=f.run, inspector=f.inspect)
            self.assertEqual(len(f.mutations()), 2)
            self.assertEqual(state['jobs'][m.job_key(f.job)]['attempts'], 1)
            f.fail('ResponseStreamingError', network_error_type='ProtocolError')
            failure = json.loads((f.state / 'LAST-FAILURE.json').read_text())
            failure['invocation_id'] = 'd' * 32
            (f.state / 'LAST-FAILURE.json').write_text(json.dumps(failure))
            self.assertFalse(f.inspect()['retryable'])

    def test_retry_reserves_attempt_before_action_and_preserves_backoff_across_restart(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d); f.fail(); saved = []; state = {"jobs": {}}
            def save(value): saved.append(json.dumps(value))
            snapshot = f.inspect()
            m.retry_job(f.root, f.job, snapshot, state, save, 1000, runner=f.run, inspector=f.inspect)
            self.assertEqual(f.mutations(), [])
            state = json.loads(saved[-1])
            m.retry_job(f.root, f.job, f.inspect(now_ms=60999), state, save, 60999, runner=f.run, inspector=f.inspect)
            self.assertEqual(f.mutations(), [])
            def action(*args):
                if args[:3] == ("sudo", "systemctl", "reset-failed"):
                    self.assertEqual(json.loads(saved[-1])["jobs"][m.job_key(f.job)]["attempts"], 1)
                return f.run(*args)
            m.retry_job(f.root, f.job, f.inspect(now_ms=61000), state, save, 61000, runner=action, inspector=f.inspect)
            self.assertEqual(len(f.mutations()), 2)
            state = json.loads(saved[-1])
            m.retry_job(f.root, f.job, f.inspect(now_ms=1000000), state, save, 1000000, runner=f.run, inspector=f.inspect)
            self.assertEqual(len(f.mutations()), 2)  # The same failed invocation is never retried twice.

    def test_fresh_command_recheck_and_counter_regression_stop_retry(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d); f.fail(); state = {"jobs": {}}; save = lambda value: None
            snapshot = f.inspect()
            m.retry_job(f.root, f.job, snapshot, state, save, 1000, runner=f.run, inspector=f.inspect)
            f.unit_args += ["--unexpected"]
            m.retry_job(f.root, f.job, snapshot, state, save, 61000, runner=f.run, inspector=f.inspect)
            self.assertIn("storage_job_changed_before_retry", snapshot["issues"])
            self.assertEqual(f.mutations(), [])
            f.unit_args = f.job["expected_command"].copy()
            with closing(sqlite3.connect(f.state / "archive.sqlite")) as db, db:
                db.execute("DELETE FROM archives WHERE id=3")
            snapshot = f.inspect(now_ms=62000)
            m.retry_job(f.root, f.job, snapshot, state, save, 62000, runner=f.run, inspector=f.inspect)
            self.assertIn("storage_job_archive_counter_regressed", snapshot["issues"])
            self.assertFalse(snapshot["retryable"])
            self.assertEqual(f.mutations(), [])

    def test_three_consecutive_retries_require_durable_advance_and_sixty_healthy_seconds_to_reset(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d); state = {"jobs": {}}; saved = []
            save = lambda value: saved.append(json.dumps(value))
            now = 1000
            for number in range(3):
                f.fail(invocation=f"{number + 1:032x}", now=now)
                m.retry_job(f.root, f.job, f.inspect(now_ms=now), state, save, now, runner=f.run, inspector=f.inspect)
                now += 300000
                m.retry_job(f.root, f.job, f.inspect(now_ms=now), state, save, now, runner=f.run, inspector=f.inspect)
            self.assertEqual(state["jobs"][m.job_key(f.job)]["attempts"], 3)
            f.fail(invocation="e" * 32, now=now)
            snapshot = f.inspect(now_ms=now)
            m.retry_job(f.root, f.job, snapshot, state, save, now, runner=f.run, inspector=f.inspect)
            self.assertIn("storage_job_retry_exhausted", snapshot["issues"])
            self.assertEqual(len(f.mutations()), 6)
            f.show.update(ActiveState="active", MainPID="99")
            f.status(now)
            m.retry_job(f.root, f.job, f.inspect(now_ms=now), state, save, now, runner=f.run, inspector=f.inspect)
            now += 60000; f.status(now)
            m.retry_job(f.root, f.job, f.inspect(now_ms=now), state, save, now, runner=f.run, inspector=f.inspect)
            self.assertEqual(state["jobs"][m.job_key(f.job)]["attempts"], 3)
            with closing(sqlite3.connect(f.state / "archive.sqlite")) as db, db: db.execute("INSERT INTO archives VALUES(4,1)")
            m.retry_job(f.root, f.job, f.inspect(now_ms=now), state, save, now, runner=f.run, inspector=f.inspect)
            self.assertEqual(state["jobs"][m.job_key(f.job)]["attempts"], 0)

    def test_cap_completion_never_retries_and_report_binds_registry_and_current_source(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(d); f.fail()
            with closing(sqlite3.connect(f.state / "archive.sqlite")) as db, db:
                db.executemany("INSERT INTO archives VALUES(?,1)", [(i,) for i in range(4, 11)])
            self.assertEqual(f.inspect()["state"], "COMPLETE")
            status = m.collect(f.root, [f.job], now_ms=1000, runner=f.run, registry_sha256="f" * 64)
            self.assertEqual(status["state"], "HEALTHY")
            self.assertEqual(status["active_source"], f.active)
            self.assertEqual(status["registry_sha256"], "f" * 64)
            self.assertEqual(status["jobs"][0]["source"], f.source)
            self.assertEqual(f.mutations(), [])
            self.assertTrue(all(re.fullmatch(r"[a-z0-9_]{1,80}", issue) for issue in status["issues"]))


if __name__ == "__main__":
    unittest.main()
