import json
import sqlite3
import tempfile
from dataclasses import FrozenInstanceError
from pathlib import Path
import unittest
from unittest.mock import patch

from logcopilot.domain import PipelineConfig, ProfileStageResult
from logcopilot.pipeline import main as pipeline_main, run_pipeline
from logcopilot.storage import StorageRepository


class PipelineContractTests(unittest.TestCase):
    def test_pipeline_config_is_immutable_and_expands_input_path(self) -> None:
        config = PipelineConfig(input_path=Path("~/sample.log"), profile="traffic")

        self.assertEqual(Path("~/sample.log").expanduser(), config.input_path)
        self.assertEqual("traffic", config.profile)
        with self.assertRaises(FrozenInstanceError):
            config.profile = "heatmap"

    def test_pipeline_config_rejects_local_agent_provider(self) -> None:
        with self.assertRaises(ValueError):
            PipelineConfig(input_path=Path("~/sample.log"), profile="traffic", agent_provider="local")

    def test_profile_stage_result_exposes_legacy_payload_sections(self) -> None:
        result = ProfileStageResult(
            profile="heatmap",
            payload={
                "artifact_paths": {"heatmap_timeseries_csv": "out/heatmap_timeseries.csv"},
                "summary": {"bucket_count": 1},
            },
            duration_seconds=0.1,
        )

        self.assertEqual({"bucket_count": 1}, result.summary)
        self.assertEqual(
            {"heatmap_timeseries_csv": "out/heatmap_timeseries.csv"},
            result.artifact_paths,
        )


class PipelineTests(unittest.TestCase):
    def test_direct_public_run_pipeline_creates_run_outputs(self) -> None:
        content = "2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=200 latency=25ms size=32 ip=10.0.0.1\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "direct.log"
            out_dir = root / "out"
            log_file.write_text(content, encoding="utf-8")

            result = run_pipeline(str(log_file), profile="traffic", out_dir=str(out_dir))

            self.assertEqual("completed", result.status)
            self.assertEqual("traffic", result.profile)
            self.assertEqual(1, result.event_count)
            self.assertTrue((Path(result.output_dir) / "events.csv").exists())
            self.assertTrue((Path(result.output_dir) / "traffic_summary.csv").exists())
            timings = result.run_summary["trace_summary"]["timings_seconds"]
            self.assertIn("parse", timings)
            self.assertIn("event_building", timings)
            self.assertIn("write_events_csv", timings)
            self.assertIn("store_events", timings)
            self.assertIn("store_aggregates", timings)
            self.assertIn("write_profile_artifacts", timings)
            self.assertIsNone(result.agent_result)
            self.assertNotIn("agent_result", result.run_summary)

    def test_agent_enabled_pipeline_persists_agent_result(self) -> None:
        content = "2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=500 latency=1200ms size=32 ip=10.0.0.1\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent-enabled.log"
            out_dir = root / "out"
            log_file.write_text(content, encoding="utf-8")

            result = run_pipeline(
                str(log_file),
                profile="traffic",
                out_dir=str(out_dir),
                agent="on",
                agent_question="Summarize this traffic run",
            )

            self.assertIsNotNone(result.agent_result)
            self.assertEqual("completed", result.agent_result["status"])
            self.assertEqual("none", result.agent_result["provider"])
            self.assertIn("agent_stage", result.run_summary["trace_summary"]["timings_seconds"])
            self.assertIn("agent_result_json", result.artifact_paths)
            self.assertTrue((Path(result.output_dir) / "agent_result.json").exists())

            stored = StorageRepository(Path(result.db_path)).get_agent_result(result.run_id)
            self.assertIsNotNone(stored)
            self.assertEqual("completed", stored["status"])
            self.assertEqual("none", stored["provider"])

            summary = json.loads((Path(result.output_dir) / "run_summary.json").read_text(encoding="utf-8"))
            manifest = json.loads((Path(result.output_dir) / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("none", summary["agent_summary"]["provider"])
            self.assertIn("short_summary", summary["agent_summary"])
            self.assertNotIn("cards", summary["agent_summary"])
            self.assertNotIn("agent_result", summary)
            self.assertIn("agent_result_json", manifest["artifacts"])

    def test_critical_failure_after_run_start_marks_run_failed(self) -> None:
        content = "2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=200 latency=25ms size=32 ip=10.0.0.1\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "failure.log"
            out_dir = root / "out"
            log_file.write_text(content, encoding="utf-8")

            with patch("logcopilot.pipeline.run_event_building", side_effect=RuntimeError("boom")):
                with self.assertRaises(RuntimeError):
                    run_pipeline(str(log_file), profile="traffic", out_dir=str(out_dir))

            with sqlite3.connect(out_dir / "logcopilot.sqlite") as connection:
                row = connection.execute(
                    "SELECT status, event_count, summary_json FROM runs"
                ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual("failed", row[0])
            self.assertEqual(0, row[1])
            failure_summary = json.loads(row[2])
            self.assertEqual("failed", failure_summary["status"])
            self.assertEqual("RuntimeError", failure_summary["error"]["type"])

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
