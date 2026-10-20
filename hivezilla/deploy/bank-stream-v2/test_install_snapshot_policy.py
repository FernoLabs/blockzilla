import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


INSTALL_PATH = Path(__file__).with_name("install.py")
SPEC = importlib.util.spec_from_file_location("bank_v2_install_under_test", INSTALL_PATH)
INSTALL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INSTALL)
FULL_FLAG = "--maximum-full-snapshots-to-retain"
INCREMENTAL_FLAG = "--maximum-incremental-snapshots-to-retain"
HELP = f"""agave-validator 4.3.0
        {FULL_FLAG} <NUMBER>
        {INCREMENTAL_FLAG} <NUMBER>
"""


def build_fixture(directory):
    base, root = directory / "build", directory / "live"
    base.mkdir()
    (root / "releases").mkdir(parents=True)
    (root / "reports").mkdir()
    (root / "config").mkdir()
    (root / "config/direct-agave.json").write_text('{}')
    (root / "captures/direct-agave-live").mkdir(parents=True)
    (base / "native-load-canary").mkdir()
    (base / "HOST-SOURCE.json").write_text(json.dumps({"agave_revision": "revision"}))
    for name in ("HOST-BUILD.json", "NATIVE-BUILD.json", "TRANSPORT-BUILD.json"):
        (base / name).write_text(json.dumps({"state": "PASSED", "agave_revision": "revision"}))
    (base / "native-load-canary/JOURNAL-CHECK.json").write_text('{"state":"PASSED"}')
    return base, root


def copy_fixture_artifact(source, destination):
    data = Path(source).read_bytes() if Path(source).name == "snapshot_budget.py" else b"tested artifact"
    Path(destination).write_bytes(data)


class InstallSnapshotPolicyTest(unittest.TestCase):
    def test_install_prepares_verified_retention_and_preserves_other_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "tested validator"
            binary.write_text(f"#!{sys.executable}\nimport sys\n"
                              "assert sys.argv[1:] == ['--help']\n"
                              f"print({HELP!r})\n")
            binary.chmod(0o700)
            config = root / "config/direct-agave.json"
            previous = shlex.join(["/old/agave-validator", "--ledger", str(root / "ledger data"),
                                   "--no-voting", "--geyser-plugin-config", str(config),
                                   FULL_FLAG, "2", INCREMENTAL_FLAG + "=4"])
            prepared = shlex.split(INSTALL.prepare_validator_command(previous, binary, config))
            self.assertEqual(prepared[0], str(binary))
            self.assertEqual(prepared[prepared.index("--ledger") + 1], str(root / "ledger data"))
            self.assertIn("--no-voting", prepared)
            self.assertNotIn("--geyser-plugin-config", prepared)
            self.assertEqual(prepared.count("--geyser-plugin-always-enabled"), 1)
            for flag in (FULL_FLAG, INCREMENTAL_FLAG):
                self.assertEqual(prepared.count(flag), 1)
                self.assertEqual(prepared[prepared.index(flag) + 1], "1")

    def test_both_unit_templates_keep_bounded_retention_and_capture_options(self):
        directory = INSTALL_PATH.parent.parent / "direct-agave"
        for name in ("hivezilla-validator.service", "hivezilla-validator-shred-hook.conf"):
            with self.subTest(name=name):
                commands = [line.removeprefix("ExecStart=") for line in (directory / name).read_text().splitlines()
                            if line.startswith("ExecStart=") and line != "ExecStart="]
                self.assertEqual(len(commands), 1)
                args = shlex.split(commands[0])
                for flag in (FULL_FLAG, INCREMENTAL_FLAG):
                    self.assertEqual(args.count(flag), 1)
                    self.assertEqual(args[args.index(flag) + 1], "1")
                for flag in ("--identity", "--ledger", "--accounts", "--snapshots",
                             "--geyser-plugin-config", "--enable-rpc-transaction-history", "--no-voting"):
                    self.assertIn(flag, args)
                preflight = [shlex.split(line.removeprefix("ExecStartPre="))
                             for line in (directory / name).read_text().splitlines()
                             if line.startswith("ExecStartPre=") and line != "ExecStartPre="]
                self.assertEqual(len(preflight), 1)
                self.assertEqual(preflight[0][0], "/usr/bin/python3")
                self.assertIn("--check-start", preflight[0])
                self.assertIn("/releases/snapshot-budget-20261013/", preflight[0][1])
                self.assertEqual(preflight[0][preflight[0].index("--snapshots") + 1],
                                 args[args.index("--snapshots") + 1])

    def test_unsupported_pinned_binary_fails_before_service_stop(self):
        with tempfile.TemporaryDirectory() as temporary:
            base, root = build_fixture(Path(temporary))

            def fake_run(*args):
                if args[-1] == "--help":
                    return HELP.replace(INCREMENTAL_FLAG, "--unsupported-option")
                if args[:2] == ("systemctl", "cat"):
                    return f"ExecStart=/old/agave-validator --geyser-plugin-config {root}/config/direct-agave.json"
                raise AssertionError(args)

            with mock.patch.object(INSTALL.shutil, "disk_usage", return_value=SimpleNamespace(free=500 * 1024**3)), \
                    mock.patch.object(INSTALL.shutil, "copy2", side_effect=copy_fixture_artifact), \
                    mock.patch.object(INSTALL, "run", side_effect=fake_run), \
                    mock.patch.object(INSTALL.subprocess, "run") as service_command:
                with self.assertRaisesRegex(ValueError, "pinned binary help"):
                    INSTALL.main(SimpleNamespace(base=base, root=root, resume=False, prepare_only=False))
                service_command.assert_not_called()

    def test_low_startup_budget_fails_before_any_service_stop(self):
        with tempfile.TemporaryDirectory() as temporary:
            base, root = build_fixture(Path(temporary))

            def fake_run(*args):
                if args[-1] == "--help":
                    return HELP
                if "--check-start" in args:
                    self.assertEqual(args[1], str(root / "releases/bank-stream-v2-20261010/snapshot_budget.py"))
                    raise subprocess.CalledProcessError(75, args)
                if args[:2] == ("systemctl", "cat"):
                    return f"ExecStart=/old/agave-validator --geyser-plugin-config {root}/config/direct-agave.json"
                raise AssertionError(args)

            with mock.patch.object(INSTALL.shutil, "disk_usage", return_value=SimpleNamespace(free=500 * 1024**3)), \
                    mock.patch.object(INSTALL.shutil, "copy2", side_effect=copy_fixture_artifact), \
                    mock.patch.object(INSTALL, "run", side_effect=fake_run), \
                    mock.patch.object(INSTALL.subprocess, "run") as service_command:
                with self.assertRaises(subprocess.CalledProcessError) as error:
                    INSTALL.main(SimpleNamespace(base=base, root=root, resume=False, prepare_only=False))
                self.assertEqual(error.exception.returncode, 75)
                service_command.assert_not_called()

    def test_future_override_uses_hashed_release_copy_for_every_start(self):
        with tempfile.TemporaryDirectory() as temporary:
            base, root = build_fixture(Path(temporary))

            def fake_run(*args):
                if args[-1] == "--help":
                    return HELP
                if "--check-start" in args:
                    return '{"startup_disk_gate_passed":true}'
                if args[:2] == ("systemctl", "cat"):
                    return f"ExecStart=/old/agave-validator --geyser-plugin-config {root}/config/direct-agave.json"
                if args[:2] == ("systemctl", "show"):
                    return '123'
                raise AssertionError(args)

            with mock.patch.object(INSTALL.shutil, "disk_usage", return_value=SimpleNamespace(free=700 * 1024**3)), \
                    mock.patch.object(INSTALL.shutil, "copy2", side_effect=copy_fixture_artifact), \
                    mock.patch.object(INSTALL, "run", side_effect=fake_run), \
                    mock.patch.object(INSTALL.subprocess, "run"), \
                    mock.patch.object(INSTALL, "override", side_effect=RuntimeError("override captured")) as override:
                with self.assertRaisesRegex(RuntimeError, "override captured"):
                    INSTALL.main(SimpleNamespace(base=base, root=root, resume=False, prepare_only=False))
            name, command, report, extra = override.call_args.args
            self.assertEqual(name, "hivezilla-validator")
            preflight = [shlex.split(line.removeprefix("ExecStartPre=")) for line in extra.splitlines()
                         if line.startswith("ExecStartPre=") and line != "ExecStartPre="]
            expected_helper = root / "releases/bank-stream-v2-20261010/snapshot_budget.py"
            self.assertEqual(preflight, [INSTALL.validator_startup_command(expected_helper, root / "snapshots")])
            receipt = json.loads((report / "DEPLOYMENT.json").read_text())
            self.assertIn("snapshot_budget.py", receipt["artifacts"])
            self.assertEqual(expected_helper.read_bytes(),
                             (INSTALL_PATH.parent.parent / "direct-agave/snapshot_budget.py").read_bytes())

    def test_missing_packaged_helper_fails_before_binary_or_service_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(INSTALL, "__file__", str(Path(temporary) / "bank-stream-v2/install.py")), \
                    mock.patch.object(INSTALL, "run") as command:
                with self.assertRaises(FileNotFoundError):
                    INSTALL.prepare_validator_command("/old/validator", Path("/release/validator"), Path("/config"))
                command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
