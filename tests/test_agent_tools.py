import tempfile
from pathlib import Path
import unittest

from logcopilot.agent.tools import (
    get_heatmap,
    get_run_summary,
    get_top_incidents,
    get_traffic_anomalies,
    get_traffic_summary,
    open_artifact,
)
from logcopilot.service import run_profile


class AgentToolsTests(unittest.TestCase):
    def test_agent_tools_read_storage_outputs(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:21:15 INFO Gateway - GET /api/orders status=500 latency=1300ms size=256 ip=10.0.0.1
2026-03-11 08:21:16 INFO Gateway - GET /api/orders status=500 latency=1400ms size=260 ip=10.0.0.2
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")

            incident_result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )
            traffic_result = run_profile(str(log_file), profile="traffic", out_dir=str(root / "out"))
            heatmap_result = run_profile(str(log_file), profile="heatmap", out_dir=str(root / "out"))

            db_path = incident_result.db_path
            incident_summary = get_run_summary(incident_result.run_id, db_path=db_path)
            incident_rows = get_top_incidents(incident_result.run_id, db_path=db_path)
            traffic_rows = get_traffic_summary(traffic_result.run_id, db_path=db_path)
            anomaly_rows = get_traffic_anomalies(traffic_result.run_id, db_path=db_path)
            heatmap_rows = get_heatmap(heatmap_result.run_id, db_path=db_path)
            artifact = open_artifact(heatmap_result.run_id, "top_hotspots_md", db_path=db_path)

            self.assertIsNotNone(incident_summary)
            self.assertTrue(incident_rows)
            self.assertTrue(traffic_rows)
            self.assertTrue(anomaly_rows)
            self.assertTrue(heatmap_rows)
            self.assertIsNotNone(artifact)
            self.assertTrue(artifact["path"].endswith("top_hotspots.md"))


if __name__ == "__main__":
    unittest.main()
