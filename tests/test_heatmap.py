import json
import tempfile
from pathlib import Path
import unittest

from logcopilot.pipeline import run_pipeline


class HeatmapIntegrationTests(unittest.TestCase):
    def test_heatmap_profile_on_fixture_log(self) -> None:
        content = """2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=200 latency=120ms size=320 ip=10.0.0.1
2026-03-11 08:20:20 INFO Gateway - GET /api/orders status=200 latency=210ms size=300 ip=10.0.0.2
2026-03-11 08:21:00 INFO Billing - POST /api/payments status=200 latency=900ms size=512 ip=10.0.0.3
2026-03-11 08:21:30 INFO Billing - POST /api/payments status=500 latency=1200ms size=530 ip=10.0.0.4
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "heatmap.log"
            out_dir = root / "out"
            log_file.write_text(content, encoding="utf-8")

            result = run_pipeline(str(log_file), profile="heatmap", out_dir=str(out_dir))
            run_dir = Path(result.output_dir)

            self.assertEqual("heatmap", result.profile)
            self.assertTrue((run_dir / "heatmap_timeseries.csv").exists())
            self.assertTrue((run_dir / "top_hotspots.md").exists())
            self.assertTrue((run_dir / "heatmap_findings.json").exists())

            summary = json.loads((run_dir / "run_summary.json").read_text(encoding="utf-8"))
            self.assertEqual("completed", summary["status"])
            self.assertGreater(summary["event_count"], 0)


if __name__ == "__main__":
    unittest.main()
