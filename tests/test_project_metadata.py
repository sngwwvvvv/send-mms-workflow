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
        self.assertIn("## 실행 환경 준비", readme)
        self.assertIn(
            "이 저장소는 Python 3.14와 잠긴 의존성 집합을 `uv.lock`으로 "
            "관리한다. 프로젝트 루트에서 다음 명령으로 동일한 실행 환경을 "
            "준비한다.",
            readme,
        )
        self.assertIn(
            "`.env`는 uv 의존성 파일이 아니므로 커밋하지 않는다. `uv sync`와 "
            "`--help` 확인은 실발송 승인이 아니며 SENS API를 호출하지 않는다.",
            readme,
        )
        self.assertIn("uv run sens-mms preflight", readme)
        self.assertIn("uv run sens-mms live", readme)
        self.assertIn("uv run sens-mms preflight --resend-failed", readme)
        self.assertIn(
            "uv run sens-mms live --resend-failed --approval-token "
            "$resendApprovalToken --confirm-sender-registered",
            readme,
        )
        self.assertIn("## 기존 실행 스크립트 호환", readme)
        self.assertIn(
            "기존 자동화도 같은 uv 환경에서 `uv run python sens_mms_cli.py "
            "preflight`처럼 계속 실행할 수 있다. 다만 운영 문서와 예시는 "
            "설치된 `sens-mms` 진입점을 기본 경로로 사용한다.",
            readme,
        )
        self.assertIn("uv run python sens_mms_cli.py preflight", readme)


if __name__ == "__main__":
    unittest.main()
