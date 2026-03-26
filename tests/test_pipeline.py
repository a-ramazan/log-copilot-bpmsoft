import json
import tempfile
from pathlib import Path
import unittest

from logcopilot.pipeline import main as pipeline_main


class PipelineTests(unittest.TestCase):
    def test_single_file_input_generates_incident_run_outputs(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] FATAL  Host Start - Startup exception|System.Security.SecurityException: System login error.
   at Foo.Bar()
2026-03-11 08:21:15,037 [1] ERROR  Host Start - Hosting failed to start
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "single.log"
            out_dir = root / "out"
            log_file.write_text(content, encoding="utf-8")

            import sys

            original_argv = sys.argv
            try:
                sys.argv = [
                    "logcopilot.pipeline",
                    "--input",
                    str(log_file),
                    "--out",
                    str(out_dir),
                    "--semantic",
                    "off",
                ]
                pipeline_main()
            finally:
                sys.argv = original_argv

            sqlite_path = out_dir / "logcopilot.sqlite"
            runs_dir = out_dir / "runs"
            run_dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
            self.assertEqual(1, len(run_dirs))
            run_dir = run_dirs[0]

            summary = json.loads((run_dir / "analysis_summary.json").read_text(encoding="utf-8"))
            llm_ready = json.loads((run_dir / "llm_ready_clusters.json").read_text(encoding="utf-8"))
            self.assertEqual(2, summary["event_count"])
            self.assertTrue(sqlite_path.exists())
            self.assertTrue((run_dir / "events.csv").exists())
            self.assertTrue((run_dir / "clusters.csv").exists())
            self.assertTrue((run_dir / "run_summary.json").exists())
            self.assertTrue((run_dir / "manifest.json").exists())
            self.assertTrue(llm_ready)


if __name__ == "__main__":
    unittest.main()
