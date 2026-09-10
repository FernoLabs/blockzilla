"""Independent checks for the export acceptance gate."""
import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('comparison', Path(__file__).with_name('run.py'))
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


class AcceptanceTests(unittest.TestCase):
    def test_exact_coverage_flags_balances_and_byte_parity(self):
        with tempfile.TemporaryDirectory() as folder:
            a, b = Path(folder) / 'a.bin', Path(folder) / 'b.bin'
            tx = (struct.pack('<QQ', 3, 0) + bytes([1]) * 64 + bytes([2]) * 32
                  + bytes([1, 0]) + struct.pack('<QQQQQ', 5000, 1, 10, 1, 5))
            data = (comparison.MAGIC + struct.pack('<QQQ', 2, 0, 0)
                    + struct.pack('<QQQ', 3, 1, len(tx)) + tx)
            a.write_bytes(data)
            b.write_bytes(data)
            result = comparison.verify_export(a, [[2, 0], [3, 1]])
            self.assertEqual((result['votes'], result['failed']), (1, 0))
            comparison.identical(a, b)
            # Equal counts do not hide a changed balance.
            b.write_bytes(data[:-1] + bytes([1]))
            comparison.verify_export(b, [[2, 0], [3, 1]])
            with self.assertRaises(AssertionError):
                comparison.identical(a, b)
            # Reject truncation, altered order, invalid flags, and trailing data.
            for corrupted in [data[:-1], data[:64] + struct.pack('<Q', 4) + data[72:],
                              data[:176] + bytes([2]) + data[177:], data + b'x']:
                b.write_bytes(corrupted)
                with self.assertRaises(AssertionError):
                    comparison.verify_export(b, [[2, 0], [3, 1]])


if __name__ == '__main__':
    unittest.main()
