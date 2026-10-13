import fcntl
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


class WorkerRecoveryTest(unittest.TestCase):
    def test_signal_stop_is_retryable_but_build_error_is_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = root / "capture" / "00000000000000000000000000000001-1"
            session.mkdir(parents=True)
            (session / "SESSION.json").write_text(json.dumps({
                "session_id": session.name,
                "config": {"node_identity": "node", "genesis_hash": "genesis",
                           "max_journal_bytes": 1024**3, "reserve_disk_bytes": 0}}))
            (session / "FAILURE.json").write_text("{}")
            connection = sqlite3.connect(session / "events.sqlite")
            connection.executescript("CREATE TABLE status(id,state,durable_sequence,error);"
                                     "INSERT INTO status VALUES(1,'CAPTURING',10,NULL);"
                                     "CREATE TABLE events(slot,kind);")
            connection.close()
            with (session / "LIVE.lock").open("w+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                engine = root / "engine"
                for code, expected in [("import os,signal; os.kill(os.getpid(),signal.SIGKILL)", 75),
                                       ("raise SystemExit(1)", 1)]:
                    engine.write_text(f"#!{sys.executable}\n{code}\n")
                    engine.chmod(0o700)
                    result = subprocess.run([sys.executable, str(Path(__file__).with_name("worker_launcher.py")),
                                             "--capture-root", str(session.parent), "--output-root", str(root / "output"),
                                             "--engine", str(engine), "--blockzilla", str(engine)],
                                            capture_output=True, text=True, timeout=15)
                    self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                    self.assertTrue((session / "events.sqlite").exists())


if __name__ == "__main__":
    unittest.main()
