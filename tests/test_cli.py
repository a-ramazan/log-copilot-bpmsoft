import io
import tempfile
from contextlib import redirect_stdout
from pathlib import Path
import sys
import unittest

from logcopilot.cli import main as cli_main


class CliTests(unittest.TestCase):
    def test_cli_run_command_creates_run_directory(self) -> None:
        content = "2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=200 latency=25ms size=32 ip=10.0.0.1\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "cli.log"
            out_dir = root / "out"
            log_file.write_text(content, encoding="utf-8")

            original_argv = sys.argv
            stdout = io.StringIO()
            try:
                sys.argv = [
                    "logcopilot.cli",
                    "run",
                    "--input",
                    str(log_file),
                    "--profile",
                    "traffic",
                    "--out",
                    str(out_dir),
                ]
                with redirect_stdout(stdout):
                    cli_main()
            finally:
                sys.argv = original_argv

            output = stdout.getvalue()
            self.assertIn("profile: traffic", output)
            run_dirs = [path for path in (out_dir / "runs").iterdir() if path.is_dir()]
            self.assertEqual(1, len(run_dirs))


if __name__ == "__main__":
    unittest.main()
