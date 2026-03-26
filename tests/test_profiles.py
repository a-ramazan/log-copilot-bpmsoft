import json
import tempfile
from pathlib import Path
import unittest

from logcopilot.service import run_profile


class ProfileIntegrationTests(unittest.TestCase):
    def test_heatmap_profile_writes_expected_artifacts(self) -> None:
        content = """2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=200 latency=120ms size=320 ip=10.0.0.1
2026-03-11 08:20:10 INFO Gateway - GET /api/orders status=200 latency=250ms size=330 ip=10.0.0.2
2026-03-11 08:21:00 INFO Billing - POST /api/payments status=200 latency=900ms size=512 ip=10.0.0.3
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "heatmap.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(str(log_file), profile="heatmap", out_dir=str(root / "out"))

            run_dir = Path(result.output_dir)
            summary = json.loads((run_dir / "run_summary.json").read_text(encoding="utf-8"))

            self.assertEqual("heatmap", result.profile)
            self.assertTrue((run_dir / "heatmap_timeseries.csv").exists())
            self.assertTrue((run_dir / "top_hotspots.md").exists())
            self.assertEqual(3, summary["event_count"])
            self.assertEqual("completed", summary["status"])

    def test_traffic_profile_writes_expected_artifacts(self) -> None:
        content = """2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=500 latency=1200ms size=320 ip=10.0.0.1
2026-03-11 08:20:10 INFO Gateway - GET /api/orders status=500 latency=1500ms size=330 ip=10.0.0.2
2026-03-11 08:20:20 INFO Gateway - GET /health status=200 latency=10ms size=10 ip=10.0.0.3
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "traffic.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(str(log_file), profile="traffic", out_dir=str(root / "out"))
            run_dir = Path(result.output_dir)

            self.assertEqual("traffic", result.profile)
            self.assertTrue((run_dir / "traffic_summary.csv").exists())
            self.assertTrue((run_dir / "latency_report.md").exists())
            self.assertTrue((run_dir / "suspicious_traffic.md").exists())


if __name__ == "__main__":
    unittest.main()
