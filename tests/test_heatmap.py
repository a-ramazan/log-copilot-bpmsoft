import json
from pathlib import Path
import unittest

from logcopilot.service import run_profile


class HeatmapRealLogsTests(unittest.TestCase):
    def test_heatmap_on_nginx_log(self) -> None:
        project_root = Path(__file__).resolve().parent.parent
        log_file = project_root / "Logs/heatmap" / "nginx_logs.log"
        out_dir = project_root / "out"

        result = run_profile(str(log_file), profile="heatmap", out_dir=str(out_dir))
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