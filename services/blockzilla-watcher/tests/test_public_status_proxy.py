import importlib.util
import json
from pathlib import Path
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts" / "public-status-proxy.py"
SPEC = importlib.util.spec_from_file_location("public_status_proxy", SCRIPT)
proxy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proxy)


class PublicStatusProxyTests(unittest.TestCase):
    def test_snapshot_paths_are_reduced_to_basenames(self):
        private = {
            "epochs": [{
                "input_path": "/volume1/archive/cars/epoch-341.car.zst",
                "output_path": "/volume1/archive/v2/epoch-341",
            }],
            "live": [{
                "capture_dir": "/volume1/private/live/capture-1",
                "output_path": "/volume1/archive/v2/epoch-1004",
            }],
            "errors": [
                {"message": "open /home/operator/private/state failed"},
                {"message": "read /mnt/archive/private failed"},
            ],
        }
        public = json.loads(proxy.public_json_bytes(json.dumps(private).encode()))
        self.assertEqual(public["epochs"][0]["input_path"], "epoch-341.car.zst")
        self.assertEqual(public["epochs"][0]["output_path"], "epoch-341")
        self.assertEqual(public["live"][0]["capture_dir"], "capture-1")
        self.assertEqual(public["live"][0]["output_path"], "epoch-1004")
        serialized = json.dumps(public)
        self.assertNotIn("/volume1", serialized)
        self.assertNotIn("/home/operator", serialized)
        self.assertNotIn("/mnt/archive", serialized)
        self.assertIn("<redacted-path>", serialized)

    def test_snapshot_patch_sse_is_sanitized(self):
        line = b'data: {"data":{"live":[{"capture_dir":"/tmp/private/live"}]}}\n'
        public = proxy.public_sse_line(line)
        self.assertTrue(public.startswith(b"data:"))
        self.assertNotIn(b"/tmp/private", public)
        self.assertIn(b'"capture_dir":"live"', public)

    def test_invalid_sse_data_fails_closed(self):
        self.assertIsNone(proxy.public_sse_line(b"data: not-json\n"))

    def test_urls_and_non_path_fields_are_unchanged(self):
        value = {
            "status_url": "/api/v1/status",
            "message": "see https://example.invalid/path",
            "capture_id": "epoch-1004-capture",
        }
        self.assertEqual(proxy.sanitize_public_value(value), value)

    def test_bind_addresses_must_be_loopback(self):
        self.assertEqual(
            proxy.network_address("127.0.0.1:8787", "--listen", True),
            ("127.0.0.1", 8787),
        )
        self.assertEqual(
            proxy.network_address("192.168.1.10:8787", "--listen", True),
            ("192.168.1.10", 8787),
        )
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address("0.0.0.0:8787", "--listen", True)
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address("192.168.1.10:8786", "--upstream", False)
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address("203.0.113.10:8787", "--listen", True)


if __name__ == "__main__":
    unittest.main()
