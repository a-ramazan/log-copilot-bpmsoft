import tempfile
from pathlib import Path
import unittest

from logcopilot.pipeline import run_pipeline
from logcopilot.storage import StorageRepository


class StorageTests(unittest.TestCase):
    def test_run_and_event_rows_are_persisted(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup failed
2026-03-11 08:21:15,037 [1] INFO Gateway - GET /api/orders status=200 latency=100ms size=256 ip=10.0.0.1
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "input.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_pipeline(str(log_file), profile="incidents", out_dir=str(root / "out"), semantic="off")

            repo = StorageRepository(Path(result.db_path))
            summary = repo.get_run_summary(result.run_id)

            self.assertIsNotNone(summary)
            self.assertEqual("incidents", summary["profile"])
            self.assertEqual(2, summary["event_count"])
            self.assertTrue(summary["artifacts"])


if __name__ == "__main__":
    unittest.main()
