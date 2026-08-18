from pathlib import Path
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ProjectMetadataTests(unittest.TestCase):
    def test_pyproject_locks_python_timezone_and_console_entrypoint(self):
        project = tomllib.loads((ROOT / "pyproject.toml").read_text("utf-8"))

        self.assertEqual(project["project"]["name"], "sens-mms")
        self.assertEqual(project["project"]["requires-python"], ">=3.14,<3.15")
        self.assertEqual(
            project["project"]["dependencies"],
            ["tzdata>=2026.3,<2027"],
        )
        self.assertEqual(
            project["project"]["scripts"]["sens-mms"],
            "sens_mms.cli:main",
        )
        self.assertEqual(project["build-system"]["build-backend"], "uv_build")
        self.assertEqual(
            project["build-system"]["requires"],
            ["uv_build>=0.12.0,<0.13"],
        )
        self.assertEqual(
            project["tool"]["uv"]["build-backend"],
            {"module-name": "sens_mms", "module-root": ""},
        )

    def test_python_version_and_lockfile_are_committed_and_consistent(self):
        self.assertEqual((ROOT / ".python-version").read_text("utf-8"), "3.14\n")
        lock = tomllib.loads((ROOT / "uv.lock").read_text("utf-8"))
        packages = {package["name"]: package for package in lock["package"]}

        self.assertEqual(lock["requires-python"], ">=3.14, <3.15")
        self.assertIn("sens-mms", packages)
        self.assertEqual(packages["tzdata"]["version"], "2026.3")

    def test_readme_uses_locked_uv_cli_and_preserves_legacy_launcher(self):
        readme = (ROOT / "README.md").read_text("utf-8")

        self.assertIn("uv sync --locked", readme)
        self.assertIn("uv run sens-mms preflight", readme)
        self.assertIn("uv run sens-mms live", readme)
        self.assertIn("uv run sens-mms preflight --resend-failed", readme)
        self.assertIn("python sens_mms_cli.py", readme)


if __name__ == "__main__":
    unittest.main()
