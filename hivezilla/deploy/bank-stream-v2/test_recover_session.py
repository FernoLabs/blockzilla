import importlib.util
from contextlib import closing
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("recovery", Path(__file__).with_name("recover_session.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class RecoveryTests(unittest.TestCase):
    def capacity_fixture(self, root, *, now=1000000, sampled_free=700 * 1024**3,
                         archive=128 * 1024**3, minimum=500 * 1024**3):
        source = dict(genesis_hash="1" * 32, node_identity="2" * 32,
                      agave_revision="a" * 40, session_id="0" * 32 + "-1")
        (root / "config").mkdir(exist_ok=True)
        (root / m.ACTIVE_SOURCE).write_text(json.dumps(source))
        directory = root / "reports/native-capacity"; directory.mkdir(parents=True, exist_ok=True)
        snapshot = sampled_free - 400 * 1024**3 - archive
        recovery = sampled_free - minimum - archive
        data = dict(schema="blockzilla-native-capacity-v1", state="RUNNING", source=source,
                    updated_unix_ms=now, free_disk_bytes=sampled_free, capture_reserve_bytes=400 * 1024**3,
                    recovery_minimum_free_bytes=minimum, next_archive_budget_bytes=archive,
                    snapshot_headroom_bytes=snapshot, recovery_headroom_bytes=recovery,
                    snapshot_capture_reserve_at_risk=snapshot < 0, snapshot_headroom_low=recovery < 0,
                    capacity_state="CRITICAL" if snapshot < 0 else "WARNING" if recovery < 0 else "OK")
        path = directory / "STATUS.json"; path.write_text(json.dumps(data))
        return path, data

    def test_recovery_capacity_legacy_gate_applies_only_when_collector_directory_is_absent(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=500 * 1024**3)):
                result = m.recovery_capacity(root)
                self.assertEqual((result["mode"], result["required_free_bytes"]), ("legacy", 500 * 1024**3))
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=500 * 1024**3 - 1)):
                with self.assertRaisesRegex(RuntimeError, "less_than_100_GiB"):
                    m.recovery_capacity(root)
            (root / "reports/native-capacity").mkdir(parents=True)
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=1000 * 1024**3)):
                with self.assertRaisesRegex(RuntimeError, "report_invalid_or_stale"):
                    m.recovery_capacity(root)

    def test_recovery_capacity_uses_current_disk_and_the_reported_next_archive_budget(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); self.capacity_fixture(root, archive=160 * 1024**3)
            required = 660 * 1024**3
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=required - 1)):
                with self.assertRaisesRegex(RuntimeError, "insufficient_recovery_snapshot_headroom"):
                    m.recovery_capacity(root, now_ms=1000000)
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=required)):
                result = m.recovery_capacity(root, now_ms=1000000)
            self.assertEqual((result["mode"], result["required_free_bytes"]), ("capacity_report", required))
            # A recent low-space sample can be followed by actual verified retirement.
            self.capacity_fixture(root, sampled_free=600 * 1024**3)
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=700 * 1024**3)):
                self.assertEqual(m.recovery_capacity(root, now_ms=1000000)["free_disk_bytes"], 700 * 1024**3)
            # Space lost after the first statvfs sample must block admission.
            with mock.patch.object(m.shutil, "disk_usage", side_effect=[SimpleNamespace(free=700 * 1024**3),
                                   SimpleNamespace(free=600 * 1024**3)]):
                with self.assertRaisesRegex(RuntimeError, "insufficient_recovery_snapshot_headroom"):
                    m.recovery_capacity(root, now_ms=1000000)

    def test_recovery_capacity_rejects_stale_failed_wrong_source_and_inconsistent_reports(self):
        changes = {"stale": lambda data: data.update(updated_unix_ms=909999),
                   "future": lambda data: data.update(updated_unix_ms=1000001),
                   "failed": lambda data: data.update(state="FAILED"),
                   "schema": lambda data: data.update(schema="other"),
                   "source": lambda data: data.update(source=dict(data["source"], session_id="1" * 32 + "-1")),
                   "math": lambda data: data.update(recovery_headroom_bytes=0),
                   "bool_integer": lambda data: data.update(next_archive_budget_bytes=True),
                   "reserve": lambda data: data.update(capture_reserve_bytes=399 * 1024**3),
                   "minimum": lambda data: data.update(recovery_minimum_free_bytes=499 * 1024**3),
                   "archive_floor": lambda data: data.update(next_archive_budget_bytes=127 * 1024**3),
                   "risk": lambda data: data.update(snapshot_headroom_low=True),
                   "capacity_state": lambda data: data.update(capacity_state="WARNING")}
        for name, change in changes.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as d:
                root = Path(d); path, data = self.capacity_fixture(root); change(data)
                path.write_text(json.dumps(data))
                with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=1000 * 1024**3)):
                    with self.assertRaisesRegex(RuntimeError, "report_invalid_or_stale"):
                        m.recovery_capacity(root, now_ms=1000000)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); self.capacity_fixture(root, now=910000)
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=700 * 1024**3)):
                m.recovery_capacity(root, now_ms=1000000)  # Exactly 90 seconds remains admissible.

    def test_recovery_capacity_rejects_symlinks_oversize_and_nonobject_metadata(self):
        for change in ("directory_link", "status_link", "pin_link", "oversize", "nonobject"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                root = Path(d); path, data = self.capacity_fixture(root)
                if change == "directory_link":
                    original = path.parent; moved = root / "moved-capacity"; original.rename(moved); original.symlink_to(moved)
                elif change in ("status_link", "pin_link"):
                    target = path if change == "status_link" else root / m.ACTIVE_SOURCE
                    moved = root / "saved-metadata.json"; target.rename(moved); target.symlink_to(moved)
                elif change == "oversize": path.write_text(" " * 65537)
                else: path.write_text("[]")
                with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=1000 * 1024**3)):
                    with self.assertRaisesRegex(RuntimeError, "recovery_capacity_report"):
                        m.recovery_capacity(root, now_ms=1000000)

    def test_invalid_installed_capacity_blocks_new_recovery_and_saved_resume_before_service_actions(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); path, data = self.capacity_fixture(root); data["state"] = "FAILED"
            path.write_text(json.dumps(data))
            report = root / "saved-report"; report.mkdir()
            (report / "RECOVERY.json").write_text(json.dumps(dict(state="SWITCH_PREPARED", root=str(root.resolve()),
                                                                validator_pid=123)))
            with mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=1000 * 1024**3)), \
                 mock.patch.object(m, "run") as action, mock.patch.object(m, "existing_capture") as adoption:
                with self.assertRaisesRegex(RuntimeError, "report_invalid_or_stale"):
                    m.recover(root, root / "new-report")
                with self.assertRaisesRegex(RuntimeError, "report_invalid_or_stale"):
                    m.resume_switch(root, report)
            action.assert_not_called(); adoption.assert_not_called()
            self.assertFalse((root / "new-report").exists())

    def test_snapshot_margin_blocks_recovery_and_resume_even_above_the_legacy_gate(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); self.capacity_fixture(root)
            report = root / "saved-report"; report.mkdir()
            (report / "RECOVERY.json").write_text(json.dumps(dict(state="SWITCH_PREPARED", root=str(root.resolve()),
                                                                validator_pid=123)))
            with mock.patch.object(m.time, "time", return_value=1000), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "run") as action, mock.patch.object(m, "existing_capture") as adoption:
                with self.assertRaisesRegex(RuntimeError, "insufficient_recovery_snapshot_headroom"):
                    m.recover(root, root / "new-report")
                with self.assertRaisesRegex(RuntimeError, "insufficient_recovery_snapshot_headroom"):
                    m.resume_switch(root, report)
            action.assert_not_called(); adoption.assert_not_called()

    def capture_fixture(self, root, source, state="CLOSED"):
        capture = root / "captures/direct-agave-live" / source["session_id"]
        capture.mkdir(parents=True)
        (capture / "SESSION.json").write_text(json.dumps(dict(session_id=source["session_id"],
            agave_revision=source["agave_revision"], bank_observations_v2=True,
            config={key: source[key] for key in ("genesis_hash", "node_identity")})))
        (capture / "LIVE.lock").touch()
        (capture / "FAILURE.json").write_text(json.dumps({"state": "FAILED"} if state == "FAILED" else {}))
        with closing(sqlite3.connect(capture / "events.sqlite")) as db:
            db.execute("CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER)")
            db.execute("INSERT INTO status VALUES(1,?,10)", (state,)); db.commit()
        if state == "CLOSED":
            (capture / "CLOSED.json").write_text(json.dumps(dict(state="CLOSED", durable_sequence=10)))
        return capture

    def upgrade_worker_fixture(self, root):
        binary = root / "releases/upgrade/hivezilla-direct-agave"
        binary.parent.mkdir(parents=True); binary.write_bytes(b"tested-new-worker"); binary.chmod(0o700)
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
        builder = binary.with_name("blockzilla"); builder.write_bytes(b"tested-new-builder"); builder.chmod(0o700)
        builder_digest = hashlib.sha256(builder.read_bytes()).hexdigest()
        upgrade = m.revision_upgrade_options("1" * 32 + "-7b", "a" * 40, "b" * 40, binary, digest,
                                             builder, builder_digest)
        return binary, digest, upgrade

    def upgrade_environment(self, directory):
        root = Path(directory).resolve(); worker, digest, upgrade = self.upgrade_worker_fixture(root)
        old = dict(genesis_hash="g", node_identity="n", agave_revision="a" * 40, session_id="0" * 32 + "-1")
        source = dict(old, agave_revision="b" * 40, session_id="1" * 32 + "-7b")
        self.capture_fixture(root, old)
        capture = self.capture_fixture(root, source, "CAPTURING")
        writer = (capture / "LIVE.lock").open("r"); self.addCleanup(writer.close)
        fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = root / "config"; config.mkdir()
        plugin = worker.with_name("plugin.so"); plugin.write_bytes(b"tested-plugin")
        host = worker.with_name("agave-validator"); host.write_bytes(b"tested-host")
        (config / "direct-agave.json").write_text(json.dumps(dict(bank_v2=True,
            reserve_disk_bytes=400 * 1024**3, libpath=str(plugin))))
        (config / "native-active-source.json").write_text(json.dumps(old))
        pin = root / "old-source.json"; pin.write_text(json.dumps(old))
        commands = self.commands_fixture(root, old["session_id"], pin)
        commands["hivezilla-direct-worker"] += ["--engine", "/current/parent-proof-worker",
                                                  "--blockzilla", "/current/compact-writer"]
        for identity, status in [(old, dict(state="DRAINED", source_sequence=10, event_sequence=11)),
                                 (source, dict(state="RUNNING", event_sequence=32))]:
            output = root / "compact/direct-agave-processed-live" / identity["session_id"]
            output.mkdir(parents=True)
            (output / "FEED.json").write_text(json.dumps(identity))
            (output / "STATUS.json").write_text(json.dumps(status))
        old_output = root / "compact/direct-agave-processed-live" / old["session_id"]
        with closing(sqlite3.connect(old_output / "feed.sqlite")) as db:
            db.execute("CREATE TABLE feed_events(sequence INTEGER PRIMARY KEY)")
            db.execute("CREATE TABLE feed_state(id INTEGER PRIMARY KEY,source_sequence INTEGER,event_sequence INTEGER)")
            db.execute("INSERT INTO feed_state VALUES(1,10,11)")
            db.execute("CREATE TABLE banks(dirty INTEGER)")
            db.executemany("INSERT INTO feed_events VALUES(?)", [(10,), (11,)]); db.commit()
        (old_output / "WORKER.lock").touch()
        state = root / "reports/native-processed-r2-bank-v2" / old["session_id"]
        state.mkdir(parents=True)
        (state / "CHECKPOINT.json").write_text(json.dumps(dict(source=str(old_output), session=old["session_id"],
            sequence=3, cursor=old["session_id"] + ":p10:digest")))
        (root / "reports/bank-v2-native-load.json").write_text(json.dumps(dict(state="LOADED", validator_pid=123,
            session_id=source["session_id"], agave_revision=source["agave_revision"], host_executable_verified=True,
            writer_lock_verified=True, validator_binary=str(host))))
        calls = []
        def run(*args):
            calls.append(args)
            if args[:3] == ("sudo", "systemctl", "cat"):
                return "[Service]\nExecStart=" + m.systemd_command(commands[args[3].removesuffix(".service")])
            if args[:2] == ("systemctl", "show"):
                return "123"
            return ""
        original_resolve = Path.resolve
        def resolve(path, *args, **kwargs):
            return host if str(path) == "/proc/123/exe" else original_resolve(path, *args, **kwargs)
        return SimpleNamespace(root=root, worker=worker, digest=digest, upgrade=upgrade, old=old, source=source,
            host=host, capture=capture, commands=commands, state=state, output=old_output, run=run,
            calls=calls, resolve=resolve)

    def same_revision_environment(self, directory):
        env = self.upgrade_environment(directory)
        env.source["agave_revision"] = env.old["agave_revision"]
        meta = json.loads((env.capture / "SESSION.json").read_text())
        meta["agave_revision"] = env.old["agave_revision"]
        (env.capture / "SESSION.json").write_text(json.dumps(meta))
        old_capture = env.root / "captures/direct-agave-live" / env.old["session_id"]
        (old_capture / "FAILURE.json").write_text(json.dumps(dict(state="FAILED", error="disk reserve reached")))
        (old_capture / "CLOSED.json").unlink()
        (env.output / "STATUS.json").write_text(json.dumps(dict(state="FAILED")))
        output = env.root / "compact/direct-agave-processed-live" / env.source["session_id"]
        (output / "FEED.json").write_text(json.dumps(env.source))
        return env

    def interrupted_same_revision(self, env, *, state="CAPTURE_STARTED"):
        report = env.root / "recovery-report"
        def install(_report, path, _text):
            if "r2-drain" in path.name:
                raise RuntimeError("interrupted_drain_install")
        with mock.patch.object(m, "run", side_effect=env.run), \
             mock.patch.object(Path, "resolve", new=env.resolve), \
             mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
             mock.patch.object(m, "install", side_effect=install), \
             mock.patch.object(m, "prepare_source_switch", wraps=m.prepare_source_switch,
                               side_effect=RuntimeError("interrupted_before_switch") if state == "PREPARED" else None):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                m.recover(env.root, report, env.source["session_id"])
        self.assertEqual(json.loads((report / "RECOVERY.json").read_text())["state"], state)
        env.calls.clear()
        return report

    def test_same_revision_capture_started_resume_preserves_saved_plan_spools_and_old_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.same_revision_environment(d)
            report = self.interrupted_same_revision(env)
            before = json.loads((report / "RECOVERY.json").read_text())
            sentinel = Path(before["switch_paths"][3]) / "retained.frame"
            sentinel.write_bytes(b"retained-payload")
            checkpoint = (env.state / "CHECKPOINT.json").read_bytes()
            old_capture = env.root / "captures/direct-agave-live" / env.old["session_id"]
            failure = (old_capture / "FAILURE.json").read_bytes()
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install"), mock.patch.object(m, "finish_switch") as finish:
                m.resume_switch(env.root, report)
            finish.assert_called_once()
            after = json.loads((report / "RECOVERY.json").read_text())
            self.assertEqual(after["state"], "SWITCH_PREPARED")
            for key in ("root", "session_id", "old_source", "artifacts", "switch_paths", "old_failure_sha256", "old_session_sha256"):
                self.assertEqual(after[key], before[key])
            self.assertEqual(sentinel.read_bytes(), b"retained-payload")
            self.assertEqual((env.state / "CHECKPOINT.json").read_bytes(), checkpoint)
            self.assertEqual((old_capture / "FAILURE.json").read_bytes(), failure)
            self.assertFalse(any("plugin" in call for call in env.calls))
            self.assertEqual(json.loads((env.root / m.ACTIVE_SOURCE).read_text()), env.old)

    def test_same_revision_prepared_resume_requires_exact_saved_session_without_discovery(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.same_revision_environment(d)
            report = self.interrupted_same_revision(env, state="PREPARED")
            record = json.loads((report / "RECOVERY.json").read_text())
            del record["session_id"]
            (report / "RECOVERY.json").write_text(json.dumps(record))
            with mock.patch.object(m, "run") as action, mock.patch.object(m, "existing_capture") as capture:
                with self.assertRaisesRegex(RuntimeError, "not_prepared"):
                    m.resume_switch(env.root, report)
            action.assert_not_called(); capture.assert_not_called()
            record["session_id"] = env.source["session_id"]
            (report / "RECOVERY.json").write_text(json.dumps(record))
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install"), mock.patch.object(m, "finish_switch") as finish:
                m.resume_switch(env.root, report)
            finish.assert_called_once()
            self.assertFalse(any("plugin" in call for call in env.calls))

    def test_same_revision_resume_rejects_changed_runtime_root_pid_source_and_old_evidence(self):
        mutations = ("root", "pid", "source", "host", "plugin", "config", "failure", "session")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as d:
                env = self.same_revision_environment(d)
                report = self.interrupted_same_revision(env)
                record = json.loads((report / "RECOVERY.json").read_text())
                if mutation == "root": record["root"] = str(env.root / "other")
                elif mutation == "pid": record["validator_pid"] = 124
                elif mutation == "source": record["source"] = dict(env.source, genesis_hash="other")
                elif mutation == "host": env.host.write_bytes(b"changed host")
                elif mutation == "plugin": env.host.with_name("plugin.so").write_bytes(b"changed plugin")
                elif mutation == "config": (env.root / "config/direct-agave.json").write_text("{}")
                else:
                    capture = env.root / "captures/direct-agave-live" / env.old["session_id"]
                    (capture / ("FAILURE.json" if mutation == "failure" else "SESSION.json")).write_text("{}")
                (report / "RECOVERY.json").write_text(json.dumps(record))
                with mock.patch.object(m, "run", side_effect=env.run), \
                     mock.patch.object(Path, "resolve", new=env.resolve), \
                     mock.patch.object(m, "prepare_source_switch") as prepare, mock.patch.object(m, "finish_switch") as finish:
                    with self.assertRaises(RuntimeError):
                        m.resume_switch(env.root, report)
                prepare.assert_not_called(); finish.assert_not_called()
                self.assertTrue(all(call[:2] == ("systemctl", "show") for call in env.calls))

    def test_same_revision_resume_rejects_unrecorded_changed_or_unsafe_switch_paths(self):
        for mutation in ("unrecorded", "changed", "symlink", "file"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as d:
                env = self.same_revision_environment(d)
                report = self.interrupted_same_revision(env)
                record = json.loads((report / "RECOVERY.json").read_text())
                target = Path(record["switch_paths"][0])
                if mutation == "unrecorded": del record["switch_paths"]
                elif mutation == "changed": record["switch_paths"][0] += "-other"
                else:
                    target.rmdir()
                    if mutation == "symlink": target.symlink_to(env.root, target_is_directory=True)
                    else: target.write_bytes(b"keep this file")
                (report / "RECOVERY.json").write_text(json.dumps(record))
                with mock.patch.object(m, "run", side_effect=env.run), \
                     mock.patch.object(Path, "resolve", new=env.resolve), \
                     mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                     mock.patch.object(m, "install") as install, mock.patch.object(m, "finish_switch") as finish:
                    with self.assertRaisesRegex(RuntimeError, "switch_path"):
                        m.resume_switch(env.root, report)
                install.assert_not_called(); finish.assert_not_called()
                self.assertFalse(any("plugin" in call for call in env.calls))
                if mutation == "file": self.assertEqual(target.read_bytes(), b"keep this file")

    def test_upgrade_switch_adopts_loaded_source_and_saves_current_worker_for_old_source(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d); report = env.root / "upgrade-report"
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install") as install, mock.patch.object(m, "finish_switch") as finish:
                m.recover(env.root, report, env.source["session_id"], "a" * 40, "b" * 40, env.worker, env.digest, Path(env.upgrade["builder_binary"]),
                              env.upgrade["builder_sha256"])
            record = json.loads((report / "RECOVERY.json").read_text())
            self.assertEqual(record["state"], "SWITCH_PREPARED")
            self.assertEqual(record["source"], env.source)
            self.assertEqual(record["old_capture_state"], "CLOSED")
            self.assertEqual(record["old_publisher_tail"]["events"], 1)
            self.assertFalse(any("plugin" in call for call in env.calls))
            worker_override = install.call_args_list[0].args[2]
            self.assertIn(str(env.worker), worker_override)
            self.assertIn(env.upgrade["builder_binary"], worker_override)
            self.assertNotIn("CPUQuota", worker_override)
            saved = m.exec_args((report / "hivezilla-direct-worker.previous-unit").read_text())
            self.assertIn("/current/parent-proof-worker", saved)
            self.assertEqual(json.loads((env.root / m.ACTIVE_SOURCE).read_text()), env.old)
            finish.assert_called_once()
            env.calls.clear()
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "finish_switch") as resumed:
                m.resume_switch(env.root, report)
            resumed.assert_called_once()
            self.assertTrue(all(call[:2] == ("systemctl", "show") for call in env.calls))

    def test_upgrade_prepared_interruption_resumes_same_capture_without_plugin_reload(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d); report = env.root / "upgrade-report"
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "prepare_source_switch", side_effect=RuntimeError("interrupted")):
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    m.recover(env.root, report, env.source["session_id"], "a" * 40, "b" * 40, env.worker, env.digest, Path(env.upgrade["builder_binary"]),
                              env.upgrade["builder_sha256"])
            self.assertEqual(json.loads((report / "RECOVERY.json").read_text())["state"], "PREPARED")
            env.calls.clear()
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install"), mock.patch.object(m, "finish_switch") as finish:
                m.resume_switch(env.root, report)
            finish.assert_called_once()
            self.assertEqual(json.loads((report / "RECOVERY.json").read_text())["state"], "SWITCH_PREPARED")
            self.assertFalse(any("plugin" in call for call in env.calls))

    def test_upgrade_resumes_after_source_directories_exist_without_resetting_spools(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d); report = env.root / "upgrade-report"
            def install(_report, path, _text):
                if "r2-drain" in path.name:
                    raise RuntimeError("interrupted_drain_unit_install")
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install", side_effect=install):
                with self.assertRaisesRegex(RuntimeError, "interrupted_drain_unit_install"):
                    m.recover(env.root, report, env.source["session_id"], "a" * 40, "b" * 40, env.worker,
                              env.digest, Path(env.upgrade["builder_binary"]), env.upgrade["builder_sha256"])
            record = json.loads((report / "RECOVERY.json").read_text())
            self.assertEqual(record["state"], "CAPTURE_STARTED")
            spool = Path(record["switch_paths"][3]); sentinel = spool / "retained.frame"
            sentinel.write_bytes(b"retained-immutable-payload")
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install"), mock.patch.object(m, "finish_switch") as finish:
                m.resume_switch(env.root, report)
            finish.assert_called_once()
            self.assertEqual(sentinel.read_bytes(), b"retained-immutable-payload")
            self.assertFalse(any("plugin" in call for call in env.calls))

    def test_upgrade_switch_pins_source_only_after_all_running_commands_match(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d); report = env.root / "upgrade-report"
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "install"), mock.patch.object(m, "finish_switch"):
                m.recover(env.root, report, env.source["session_id"], "a" * 40, "b" * 40, env.worker, env.digest, Path(env.upgrade["builder_binary"]),
                              env.upgrade["builder_sha256"])
            record = json.loads((report / "RECOVERY.json").read_text())
            banks = Path(record["bank_output"])
            (banks / "FEED.json").write_text(json.dumps(env.source)); (banks / "feed.sqlite-shm").touch()
            new_state = env.root / "reports/native-processed-r2-bank-v2" / env.source["session_id"]
            (new_state / "CHECKPOINT.json").write_text("{}")
            storage = env.root / "compact/direct-agave-processed-live" / env.source["session_id"] / "cold-storage"
            storage.mkdir(); (storage / "STATUS.json").write_text("{}")
            replacements = m.replacement_commands(env.commands, env.root, env.source["session_id"],
                                                   report / "expected-source.json", env.worker, Path(env.upgrade["builder_binary"]))
            processes = {name: str(700 + index) for index, (name, _, _) in enumerate(replacements)}
            command_lines = {processes[name]: args for name, args, _ in replacements}
            original_read = Path.read_bytes
            mismatch = True
            def read_bytes(path):
                if str(path).startswith("/proc/") and path.name == "cmdline":
                    args = command_lines[path.parent.name]
                    if mismatch and args[1] == "relay":
                        args = [*args, "--unexpected"]
                    return ("\0".join(args) + "\0").encode()
                return original_read(path)
            def run(*args):
                if args[:2] == ("systemctl", "show") and args[2] != "hivezilla-validator":
                    return processes[args[2]]
                return env.run(*args)
            with mock.patch.object(m, "run", side_effect=run), mock.patch.object(m, "install"), \
                 mock.patch.object(Path, "read_bytes", new=read_bytes), mock.patch.object(m.time, "sleep"):
                with self.assertRaisesRegex(RuntimeError, "running_service_command_mismatch"):
                    m.finish_switch(env.root, report, record, env.commands, "123")
                self.assertEqual(json.loads((env.root / m.ACTIVE_SOURCE).read_text()), env.old)
                mismatch = False
                m.finish_switch(env.root, report, record, env.commands, "123")
            self.assertEqual(json.loads((env.root / m.ACTIVE_SOURCE).read_text()), env.source)
            self.assertIn(("sudo", "systemctl", "start", "hivezilla-direct-worker"), env.calls)
            self.assertEqual(json.loads((report / "RECOVERY.json").read_text())["state"],
                             "RECOVERED_PENDING_PUBLIC_CHECK")

    def test_upgrade_rejects_undrained_closed_feed_before_stopping_any_service(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d)
            (env.output / "STATUS.json").write_text(json.dumps(dict(state="RUNNING")))
            with mock.patch.object(m, "run", side_effect=env.run), \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)):
                with self.assertRaisesRegex(RuntimeError, "not_terminal"):
                    m.recover(env.root, env.root / "report", env.source["session_id"], "a" * 40, "b" * 40,
                              env.worker, env.digest, Path(env.upgrade["builder_binary"]),
                              env.upgrade["builder_sha256"])
            self.assertTrue(all(call[:3] == ("sudo", "systemctl", "cat") for call in env.calls))
            self.assertEqual(json.loads((env.root / m.ACTIVE_SOURCE).read_text()), env.old)

    def test_clean_old_source_publisher_drain_requires_upgrade_receipt_and_exact_closed_capture(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d); report = env.root / "upgrade-report"; report.mkdir()
            (report / "RECOVERY.json").write_text(json.dumps(dict(root=str(env.root), old_source=env.old,
                                                                  revision_upgrade=env.upgrade)))
            args = ["/current/stream", "publish", "--processed", "--output", str(env.output), "--state", str(env.state)]
            command = report / "old-publisher-command.json"; command.write_text(json.dumps(args))
            def publish(args, check):
                self.assertEqual(args[-2:], ["--max-chunks", "1"])
                (env.state / "CHECKPOINT.json").write_text(json.dumps(dict(source=str(env.output),
                    session=env.old["session_id"], sequence=4, cursor=env.old["session_id"] + ":p11:digest")))
            with mock.patch.object(m.subprocess, "run", side_effect=publish) as child:
                m.drain_closed_source(command)
            child.assert_called_once()
            receipt = json.loads((env.state / "DRAIN-RECEIPT.json").read_text())
            self.assertEqual(receipt["checkpoint_sequence"], 4)
            self.assertTrue(receipt["remote_head_checked_by_publisher"])

    def test_closed_processed_status_requires_full_discovery_no_dirty_banks_and_released_worker(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.upgrade_environment(d)
            m.terminal_upgrade_feed(env.root, env.old, "CLOSED")
            with (env.output / "WORKER.lock").open("r") as writer:
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError, "worker_is_live"):
                    m.terminal_upgrade_feed(env.root, env.old, "CLOSED")
            with closing(sqlite3.connect(env.output / "feed.sqlite")) as db:
                db.execute("UPDATE feed_state SET source_sequence=9"); db.commit()
                with self.assertRaisesRegex(RuntimeError, "not_drained"):
                    m.terminal_upgrade_feed(env.root, env.old, "CLOSED")
                db.execute("UPDATE feed_state SET source_sequence=10")
                db.execute("INSERT INTO banks VALUES(1)"); db.commit()
                with self.assertRaisesRegex(RuntimeError, "not_drained"):
                    m.terminal_upgrade_feed(env.root, env.old, "CLOSED")

    def test_revision_upgrade_requires_explicit_pins_and_new_tested_worker(self):
        good = ("1" * 32 + "-7b", "a" * 40, "b" * 40, Path("/release/worker"), "c" * 64,
                Path("/release/blockzilla"), "d" * 64)
        self.assertIsNone(m.revision_upgrade_options(None, None, None, None, None))
        for index in range(len(good)):
            args = list(good); args[index] = None
            with self.assertRaisesRegex(RuntimeError, "revision_upgrade_requires"):
                m.revision_upgrade_options(*args)
        with self.assertRaisesRegex(RuntimeError, "revision_upgrade_requires"):
            m.revision_upgrade_options(good[0], good[1], good[1], good[3], good[4])
        with self.assertRaisesRegex(RuntimeError, "revision_upgrade_requires"):
            m.revision_upgrade_options(good[0], good[1], "short", good[3], good[4])

    def test_upgrade_worker_requires_exact_immutable_executable_and_hash(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); binary, digest, upgrade = self.upgrade_worker_fixture(root)
            self.assertEqual(m.verify_upgrade_worker(root, upgrade), binary)
            binary.write_bytes(b"different")
            with self.assertRaisesRegex(RuntimeError, "artifact_mismatch"):
                m.verify_upgrade_worker(root, upgrade)
            binary.write_bytes(b"tested-new-worker"); binary.chmod(0o600)
            with self.assertRaisesRegex(RuntimeError, "artifact_mismatch"):
                m.verify_upgrade_worker(root, upgrade)
            binary.chmod(0o700)
            link = binary.with_name("symlink"); link.symlink_to(binary)
            with self.assertRaisesRegex(RuntimeError, "artifact_mismatch"):
                m.verify_upgrade_worker(root, dict(upgrade, worker_binary=str(link)))
            builder = Path(upgrade["builder_binary"]); builder.write_bytes(b"wrong-revision-builder")
            with self.assertRaisesRegex(RuntimeError, "builder_artifact_mismatch"):
                m.verify_upgrade_worker(root, upgrade)

    def test_upgrade_rebinds_only_worker_engine_and_producer_binary(self):
        root = Path("/root"); binary = root / "releases/upgrade/worker"
        commands = self.commands_fixture(root, "old-session", root / "old-source.json")
        commands["hivezilla-direct-worker"] += ["--engine", "/current/parent-proof-worker",
            "--blockzilla", "/current/compact-writer", "--processed", "--output-root", "/current/output"]
        changed = {name: args for name, args, _ in m.replacement_commands(
            commands, root, "new-session", root / "new-source.json", binary)}
        self.assertEqual(changed["hivezilla-direct-worker"], [str(binary) if arg == "/current/parent-proof-worker"
            else arg for arg in commands["hivezilla-direct-worker"]])
        self.assertEqual(changed["hivezilla-bank-v2-producer"][0], str(binary))
        self.assertEqual(commands["hivezilla-bank-v2-producer"][0], "/release/engine")
        self.assertEqual(changed["hivezilla-native-cold-storage-20261012-live"][-6:],
                         commands["hivezilla-native-cold-storage-20261012-live"][-6:])

    def test_upgrade_existing_capture_keeps_exact_node_and_genesis_checks(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); old = dict(genesis_hash="g", node_identity="n", agave_revision="a" * 40,
                                      session_id="0" * 32 + "-1")
            new = dict(old, agave_revision="b" * 40, session_id="1" * 32 + "-7b")
            capture = self.capture_fixture(root, new, "CAPTURING")
            with (capture / "LIVE.lock").open("r") as writer:
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError, "source_mismatch"):
                    m.existing_capture(root, new["session_id"], old, 123)
                self.assertEqual(m.existing_capture(root, new["session_id"], old, 123, "b" * 40), capture)
                for key in ("genesis_hash", "node_identity"):
                    with self.assertRaisesRegex(RuntimeError, "source_mismatch"):
                        m.existing_capture(root, new["session_id"], dict(old, **{key: "wrong"}), 123, "b" * 40)

    def test_clean_upgrade_requires_checkpointed_closed_source_with_no_writer(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); old = dict(genesis_hash="g", node_identity="n", agave_revision="a" * 40,
                                      session_id="0" * 32 + "-1")
            capture = self.capture_fixture(root, old)
            self.assertEqual(m.terminal_upgrade_capture(root, old), "CLOSED")
            with (capture / "LIVE.lock").open("r") as writer:
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError, "writer_is_live"):
                    m.terminal_upgrade_capture(root, old)
            (capture / "CLOSED.json").write_text(json.dumps(dict(state="CLOSED", durable_sequence=9)))
            with self.assertRaisesRegex(RuntimeError, "not_terminal"):
                m.terminal_upgrade_capture(root, old)
            (capture / "events.sqlite-wal").write_bytes(b"uncheckpointed")
            with self.assertRaisesRegex(RuntimeError, "uncheckpointed_wal"):
                m.terminal_upgrade_capture(root, old)

    def test_upgrade_native_load_receipt_must_match_current_host_and_source(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); reports = root / "reports"; reports.mkdir()
            receipt = dict(state="LOADED", validator_pid=123, session_id="session", agave_revision="b" * 40,
                           host_executable_verified=True, writer_lock_verified=True, validator_binary="/new/host")
            path = reports / "bank-v2-native-load.json"; path.write_text(json.dumps(receipt))
            m.verify_upgrade_load(root, "123", "session", "b" * 40, Path("/new/host"))
            for key, wrong in [("state", "CAPTURE_FAILED"), ("validator_pid", 124), ("session_id", "other"),
                               ("agave_revision", "a" * 40), ("writer_lock_verified", False),
                               ("validator_binary", "/old/host")]:
                path.write_text(json.dumps(dict(receipt, **{key: wrong})))
                with self.assertRaisesRegex(RuntimeError, "receipt_mismatch"):
                    m.verify_upgrade_load(root, "123", "session", "b" * 40, Path("/new/host"))

    def test_source_override_has_precedence_over_later_batch_configuration(self):
        text = "# /etc/systemd/system/hivezilla-bank-v2-r2-publisher.service.d/zzzzzzzzzzz-batch-20261012.conf\n"
        m.check_source_override_order("hivezilla-bank-v2-r2-publisher", text)
        text += "# /etc/systemd/system/hivezilla-bank-v2-r2-publisher.service.d/" + "z" * 97 + "-unknown.conf\n"
        with self.assertRaisesRegex(RuntimeError, "does_not_sort_last"):
            m.check_source_override_order("hivezilla-bank-v2-r2-publisher", text)

    def test_resume_uses_saved_source_without_loading_plugin_or_resetting_data(self):
        with tempfile.TemporaryDirectory() as d:
            env = self.same_revision_environment(d)
            report = self.interrupted_same_revision(env)
            record = json.loads((report / "RECOVERY.json").read_text())
            record.update(state="SWITCH_PREPARED", source=env.source)
            (report / "RECOVERY.json").write_text(json.dumps(record))
            with mock.patch.object(m, "run", side_effect=env.run) as action, \
                 mock.patch.object(Path, "resolve", new=env.resolve), \
                 mock.patch.object(m, "existing_capture") as capture, \
                 mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)), \
                 mock.patch.object(m, "finish_switch") as finish:
                m.resume_switch(env.root, report)
            action.assert_called_once_with("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value")
            capture.assert_called_once_with(env.root, env.source["session_id"], env.old, "123")
            finish.assert_called_once_with(env.root, report, record, env.commands, "123")
            self.assertEqual(json.loads((report / "RECOVERY.json").read_text()), record)

    def test_install_creates_readable_systemd_directory_despite_private_umask(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); report = root / "report"
            target = root / "systemd/unit.service.d/source.conf"
            with mock.patch.object(m, "run") as command:
                m.install(report, target, "[Service]\nExecStart=/app\n")
            self.assertEqual(command.call_args_list[0].args,
                             ("sudo", "install", "-d", "-m", "755", str(target.parent)))
            self.assertEqual(command.call_args_list[1].args[:5],
                             ("sudo", "install", "-m", "644", str(report / "new" / str(target).lstrip("/"))))

    def test_install_reads_protected_backup_with_sudo_and_keeps_original_across_resume(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); report = root / "report"
            target = root / "systemd/cold-storage.service.d/source.conf"
            target.parent.mkdir(parents=True)
            previous = "# exact original\n\n[Service]\nExecStart=\nExecStart=/current/cold-storage --keep-seconds 7200"
            target.write_text(previous); target.chmod(0o600)
            saved = report / "saved" / str(target).lstrip("/")
            chmod_paths = []
            original_chmod = Path.chmod
            def chmod(path, *args, **kwargs):
                chmod_paths.append(path)
                return original_chmod(path, *args, **kwargs)
            def mutation(*args):
                self.assertEqual(saved.read_bytes(), previous.encode())
                self.assertNotIn("chmod", args)
            with mock.patch.object(m.subprocess, "check_output", return_value=previous) as protected_read, \
                 mock.patch.object(m.shutil, "copy2", side_effect=PermissionError("protected source")), \
                 mock.patch.object(Path, "chmod", new=chmod), \
                 mock.patch.object(m, "run", side_effect=mutation):
                m.install(report, target, "[Service]\nExecStart=/new/cold-storage\n")
            protected_read.assert_called_once_with(["sudo", "cat", "--", str(target)], text=True)
            self.assertNotIn(target, chmod_paths)
            self.assertEqual(target.read_bytes(), previous.encode())
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            target.write_text("partially installed new source")
            with mock.patch.object(m.subprocess, "check_output") as reread, mock.patch.object(m, "run"):
                m.install(report, target, "[Service]\nExecStart=/new/cold-storage\n")
            reread.assert_not_called()
            self.assertEqual(saved.read_bytes(), previous.encode())

    def test_existing_restart_capture_needs_exact_host_source_and_live_writer(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); pid = 123; session_id = "1" * 32 + "-7b"
            session = root / "captures/direct-agave-live" / session_id
            session.mkdir(parents=True)
            old = dict(genesis_hash="g", node_identity="n", agave_revision="r", session_id="0" * 32 + "-1")
            (session / "SESSION.json").write_text(json.dumps(dict(session_id=session_id,
                agave_revision="r", bank_observations_v2=True, config=dict(genesis_hash="g", node_identity="n"))))
            (session / "FAILURE.json").write_text("{}")
            with closing(sqlite3.connect(session / "events.sqlite")) as db:
                db.execute("CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER)")
                db.execute("INSERT INTO status VALUES(1,'CAPTURING',10)"); db.commit()
            with (session / "LIVE.lock").open("w") as writer:
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertEqual(m.existing_capture(root, session_id, old, pid), session)
                with self.assertRaisesRegex(RuntimeError, "host_mismatch"):
                    m.existing_capture(root, session_id, old, pid + 1)
                with self.assertRaisesRegex(RuntimeError, "source_mismatch"):
                    m.existing_capture(root, session_id, dict(old, agave_revision="other"), pid)
                (session / "FAILURE.json").write_text('{"state":"FAILED"}')
                with self.assertRaisesRegex(RuntimeError, "failed_or_closed"):
                    m.existing_capture(root, session_id, old, pid)
            (session / "FAILURE.json").write_text("{}")
            with self.assertRaisesRegex(RuntimeError, "writer_missing"):
                m.existing_capture(root, session_id, old, pid)

    def commands_fixture(self, root, session, expected):
        processed = root / "compact/direct-agave-processed-live" / session
        banks = root / "compact/bank-v2-live" / session
        storage = ["/usr/bin/python3", "/release/cold_storage.py", "archive"]
        for option, value in [("--output", processed), ("--bank-output", banks),
                              ("--capture", root / "captures/direct-agave-live" / session),
                              ("--publisher-state", root / "reports/native-processed-r2-bank-v2" / session),
                              ("--v1-spool", root / "stream-relay-spool-bank-v2" / session),
                              ("--bank-origin-spool", root / "bank-v2-origin-spool" / session)]:
            storage += [option, str(value)]
        storage += ["--credentials-file", "/private/env", "--keep-seconds", "7200", "--retire", "--follow"]
        return {
            "hivezilla-direct-worker": ["/usr/bin/python3", "/release/worker_launcher.py"],
            "hivezilla-bank-v2-producer": ["/release/engine", "build-confirmed", str(processed), str(banks), "--follow"],
            "hivezilla-bank-v2-origin": ["/release/stream", "bank-origin", "--output", str(banks), "--spool", "/old/spool"],
            "hivezilla-stream-origin": ["/release/stream", "origin", "--output", str(processed)],
            "hivezilla-stream-relay": ["/release/stream", "relay", "--spool", "/old/v1", "--banks-spool", "/old/v2", "--banks-source-file", str(expected)],
            "hivezilla-bank-v2-r2-publisher": ["/release/stream", "publish", "--processed", "--output", str(processed), "--state", "/old/state"],
            "hivezilla-native-cold-storage-20261012-live": storage,
        }

    def test_storage_switch_rebinds_writable_paths_for_a_strict_sandbox(self):
        root = Path("/root")
        commands = self.commands_fixture(root, "old-session", root / "old-source.json")
        replacements = m.replacement_commands(commands, root, "new-session", root / "new-source.json")
        name, args, writable = replacements[-1]
        self.assertEqual(name, "hivezilla-native-cold-storage-20261012-live")
        text = m.override_text(args, writable)
        permission_line = text.split("ReadWritePaths=")[2].strip()
        self.assertNotIn("old-session", permission_line)
        self.assertIn('"/root/compact/direct-agave-processed-live/new-session"', permission_line)
        self.assertIn("bank-v2-live/new-session/feed.sqlite-shm", permission_line)
        self.assertIn("captures/direct-agave-live/new-session/events.sqlite-shm", permission_line)

    def test_invalid_saved_origin_never_stops_services_or_reloads_capture(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); session = "0" * 32 + "-1"
            capture = root / "captures/direct-agave-live" / session
            capture.mkdir(parents=True)
            (capture / "FAILURE.json").write_text(json.dumps({"state": "FAILED"}))
            expected = root / "old-source.json"
            expected.write_text(json.dumps(dict(genesis_hash="g", node_identity="n", agave_revision="r", session_id=session)))
            commands = self.commands_fixture(root, session, expected)
            commands["hivezilla-bank-v2-origin"] = ["/release/stream", "bank-origin", "--output", "/old", "--spool"]
            calls = []
            def read_unit(*args):
                calls.append(args)
                self.assertEqual(args[:3], ("sudo", "systemctl", "cat"))
                return "[Service]\nExecStart=" + m.systemd_command(commands[args[3].removesuffix(".service")])
            with mock.patch.object(m, "run", side_effect=read_unit), mock.patch.object(
                    m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * 1024**3)):
                with self.assertRaisesRegex(RuntimeError, "missing_option_value"):
                    m.recover(root, root / "new-report")
            self.assertTrue(calls)
            self.assertTrue(all(call[:3] == ("sudo", "systemctl", "cat") for call in calls))
            self.assertFalse((root / m.ACTIVE_SOURCE).exists())
            self.assertEqual([p.name for p in capture.parent.iterdir()], [session])

    def drain_fixture(self, root, positions=(11, 12, 13), after=10, sequence=3):
        output, state = root / "output", root / "state"
        output.mkdir(); state.mkdir()
        source = dict(genesis_hash="g", node_identity="n", agave_revision="r", session_id="session")
        (output / "FEED.json").write_text(json.dumps(source))
        (output / "STATUS.json").write_text(json.dumps({"state": "FAILED"}))
        with closing(sqlite3.connect(output / "feed.sqlite")) as db:
            db.execute("CREATE TABLE feed_events(sequence INTEGER PRIMARY KEY)")
            db.executemany("INSERT INTO feed_events VALUES(?)", [(after,), *[(p,) for p in positions]])
            db.commit()
        self.checkpoint(output, state, after, sequence)
        args = ["/release/hivezilla-stream", "publish", "--processed", "--output", str(output),
                "--state", str(state), "--credentials-file", "/private/credentials.env", "--batch-events", "256"]
        path = root / "command.json"; path.write_text(json.dumps(args))
        return output, state, source, args, path

    def checkpoint(self, output, state, position, sequence):
        (state / "CHECKPOINT.json").write_text(json.dumps({"source": str(output), "session": "session",
            "sequence": sequence, "cursor": f"session:p{position}:digest"}))

    def test_closed_tail_uses_event_positions_not_publisher_commit_count(self):
        with tempfile.TemporaryDirectory() as d:
            output, state, source, args, _ = self.drain_fixture(Path(d), after=10, sequence=3)
            tail = m.tail_plan(output, state, source)
            self.assertEqual(tail, dict(events=3, checkpoint_sequence=3, expected_sequence=6, last_position=13))
            command = m.finite_drain_command(args, output, state, tail)
            self.assertEqual(command[-2:], ["--max-chunks", "3"])
            self.assertEqual(args[-2:], ["--batch-events", "256"])

    def test_tail_gap_and_wrong_source_reject_before_publication(self):
        with tempfile.TemporaryDirectory() as d:
            output, state, source, _, _ = self.drain_fixture(Path(d), positions=(11, 13))
            with self.assertRaisesRegex(RuntimeError, "tail_gap"):
                m.tail_plan(output, state, source)
            with self.assertRaisesRegex(RuntimeError, "source_mismatch"):
                m.tail_plan(output, state, dict(source, session_id="another"))
            self.checkpoint(output, state, 50, 3)
            with self.assertRaisesRegex(RuntimeError, "checkpoint_beyond_source"):
                m.tail_plan(output, state, source)

    def test_drain_recounts_after_partial_commit_and_restart(self):
        with tempfile.TemporaryDirectory() as d:
            output, state, _, _, path = self.drain_fixture(Path(d))
            def interrupted(args, check):
                self.assertEqual(args[-2:], ["--max-chunks", "3"])
                self.checkpoint(output, state, 11, 4)
                raise RuntimeError("interrupted_after_one_commit")
            with mock.patch.object(m.subprocess, "run", side_effect=interrupted):
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    m.drain_closed_source(path)
            def complete(args, check):
                self.assertEqual(args[-2:], ["--max-chunks", "2"])
                self.checkpoint(output, state, 13, 6)
            with mock.patch.object(m.subprocess, "run", side_effect=complete):
                m.drain_closed_source(path)
            self.assertEqual(json.loads((state / "DRAIN-RECEIPT.json").read_text())["checkpoint_sequence"], 6)
            with mock.patch.object(m.subprocess, "run") as child:
                m.drain_closed_source(path)
                child.assert_not_called()

    def test_unfinished_intent_and_live_source_cannot_be_declared_drained(self):
        with tempfile.TemporaryDirectory() as d:
            output, state, _, _, path = self.drain_fixture(Path(d), positions=())
            (state / "INTENT.json").write_text(json.dumps({"commits": [{"sequence": 4, "cursor": "session:p11:digest"}]}))
            with self.assertRaisesRegex(RuntimeError, "pending_intent"):
                m.drain_closed_source(path)
            (output / "STATUS.json").write_text(json.dumps({"state": "RUNNING"}))
            with self.assertRaisesRegex(RuntimeError, "not_closed_failed"):
                m.drain_closed_source(path)

    def test_storage_switch_keeps_credentials_and_retirement_policy(self):
        root = Path("/root")
        options = ["--output", "--bank-output", "--capture", "--publisher-state", "--v1-spool", "--bank-origin-spool"]
        args = ["/usr/bin/python3", "/release/cold_storage.py", "archive"]
        for option in options:
            args += [option, "/old/session"]
        args += ["--credentials-file", "/private/env", "--keep-seconds", "7200", "--retire", "--follow"]
        changed = m.storage_command(args, root, "new-session")
        for option in options:
            self.assertTrue(changed[changed.index(option) + 1].endswith("/new-session"))
        self.assertEqual(changed[-6:], args[-6:])
        self.assertEqual(args[args.index("--output") + 1], "/old/session")

    def test_last_nonempty_override_wins_and_quoted_paths_survive(self):
        text = 'ExecStart=/old --output /old-session\nExecStart=\nExecStart="/tested release/app" --output "/new session"\n'
        self.assertEqual(m.exec_args(text), ["/tested release/app", "--output", "/new session"])

    def test_option_replacement_does_not_rebind_other_source_paths(self):
        args = ["stream", "relay", "--spool", "/old", "--banks-source-file", "/old/source.json"]
        got = m.replace_option(args, "--spool", "/new")
        self.assertEqual(got[-1], "/old/source.json")
        self.assertEqual(args[3], "/old")
        self.assertEqual(got[3], "/new")

    def test_invalid_or_repeated_options_reject(self):
        for args in [["app"], ["app", "--spool"], ["app", "--spool", "--x"],
                     ["app", "--spool", "a", "--spool", "b"]]:
            with self.assertRaises(RuntimeError):
                m.replace_option(args, "--spool", "new")

    def test_systemd_directive_injection_rejects(self):
        for value in ["x\nExecStart=/bad", "%n", "$TOKEN", "x\0y"]:
            with self.assertRaises(RuntimeError):
                m.override_text(["app", value])

    def test_source_pin_keeps_all_four_fields(self):
        value = dict(genesis_hash="g", node_identity="n", agave_revision="r", session_id="s", extra=1)
        self.assertEqual(m.source_tuple(value), {k: value[k] for k in value if k != "extra"})
        del value["agave_revision"]
        with self.assertRaises(KeyError):
            m.source_tuple(value)


if __name__ == "__main__":
    unittest.main()
