import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("recovery", Path(__file__).with_name("recover_session.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class RecoveryTests(unittest.TestCase):
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
