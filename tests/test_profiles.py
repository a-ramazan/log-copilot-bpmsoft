import json
import tempfile
from pathlib import Path
import unittest

from logcopilot.service import run_profile


class ProfileIntegrationTests(unittest.TestCase):
    def test_incidents_profile_on_java_style_logs_extracts_structure(self) -> None:
        content = """17/06/09 20:10:42 ERROR executor.CoarseGrainedExecutorBackend: Executor lost due to IOException|java.io.IOException: Connection refused
   at org.apache.spark.Executor.run(Executor.scala:123)
17/06/09 20:10:43 WARN executor.CoarseGrainedExecutorBackend: Executor failed to reconnect after timeout
17/06/09 20:10:44 INFO executor.CoarseGrainedExecutorBackend: Successfully registered with driver
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "spark_incidents.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            profile_summary = result.run_summary["profile_summary"]
            analysis_summary = profile_summary["analysis_summary"]

            self.assertGreaterEqual(analysis_summary["timestamp_coverage"], 1.0)
            self.assertGreaterEqual(analysis_summary["level_coverage"], 1.0)
            self.assertGreaterEqual(analysis_summary["component_coverage"], 1.0)
            self.assertGreaterEqual(profile_summary["incident_event_count"], 2)

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

    def test_windows_servicing_run_summary_contains_parser_diagnostics(self) -> None:
        content = """2016-09-28 04:30:31, Info                  CBS    Warning: Unrecognized packageExtended attribute.
2016-09-28 04:30:31, Info                  CBS    Expecting attribute name [HRESULT = 0x800f080d - CBS_E_MANIFEST_INVALID_ITEM]
2016-09-28 04:30:31, Info                  CBS    Failed to get next element [HRESULT = 0x800f080d - CBS_E_MANIFEST_INVALID_ITEM]
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "Windows_2k.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            diagnostics = result.run_summary["parser_diagnostics"]
            profile_fit = result.run_summary["profile_fit"]

            self.assertEqual("windows_servicing", diagnostics["dominant_parser"])
            self.assertEqual("incidents", profile_fit["recommended_profile"])
            self.assertGreaterEqual(diagnostics["parse_quality"]["score"], 0.75)

    def test_access_log_run_summary_marks_incidents_as_bad_fit(self) -> None:
        content = """199.60.47.128 - - [01/Jan/2016:00:25:53 +0100] "GET http://example.com/login HTTP/1.1" 302 0
199.60.47.128 - - [01/Jan/2016:00:26:01 +0100] "POST http://example.com/api/orders HTTP/1.1" 500 128
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "access.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            self.assertEqual("traffic", result.run_summary["profile_fit"]["recommended_profile"])
            self.assertEqual("low", result.run_summary["profile_fit"]["fit_label"])


if __name__ == "__main__":
    unittest.main()
