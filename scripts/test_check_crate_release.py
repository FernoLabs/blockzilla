"""Regression checks for release graphs with independent path workspaces."""

import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "check_crate_release", Path(__file__).with_name("check-crate-release.py")
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


def package(name, dependencies=()):
    return {
        "name": name,
        "manifest_path": f"/fixtures/{name}/Cargo.toml",
        "dependencies": [
            {"name": dependency, "path": f"/fixtures/{dependency}", "kind": kind}
            for dependency, kind in dependencies
        ],
    }


class IndependentWorkspaces(unittest.TestCase):
    def test_follows_standalone_graph_once_and_ignores_dev_dependencies(self):
        root = package("blockzilla", [("relay", None), ("decoder", None), ("fixture", "dev")])
        standalone = {
            "relay": package("relay", [("bank", None)]),
            "decoder": package("decoder", [("bank", None)]),
            "bank": package("bank"),
        }
        loaded = []

        def load(manifest):
            name = manifest.parent.name
            loaded.append(name)
            return {"packages": [standalone[name]]}

        result = release.release_packages({"packages": [root]}, load)
        self.assertEqual([entry["name"] for entry in result], ["bank", "relay", "decoder", "blockzilla"])
        self.assertEqual(loaded, ["relay", "bank", "decoder"])

    def test_detects_a_cycle_across_workspace_boundaries(self):
        root = package("blockzilla", [("relay", None)])
        relay = package("relay", [("blockzilla", None)])
        with self.assertRaisesRegex(ValueError, "Local dependency cycle at blockzilla"):
            release.release_packages({"packages": [root]}, lambda manifest: {"packages": [relay]})

    def test_missing_workspace_has_a_clear_error_without_a_loader(self):
        root = package("blockzilla", [("relay", None)])
        with self.assertRaisesRegex(ValueError, "relay is outside the inspected workspace"):
            release.release_packages({"packages": [root]})


if __name__ == "__main__":
    unittest.main()
