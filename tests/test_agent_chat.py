import json
import os
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from logcopilot.agent.config import AgentModelConfig, provider_is_configured, resolve_agent_model_config
from logcopilot.agent.stage import validate_agent_result_payload
from logcopilot.domain import AgentInputContext, IncidentCard
from logcopilot.pipeline import run_pipeline


class AgentStructuredLayerTests(unittest.TestCase):
    def test_pipeline_agent_result_is_structured_not_chat_like(self) -> None:
        content = """2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=500 latency=1200ms size=32 ip=10.0.0.1
2026-03-11 08:20:10 INFO Gateway - GET /api/orders status=500 latency=1500ms size=33 ip=10.0.0.2
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "traffic.log"
            log_file.write_text(content, encoding="utf-8")

            result = run_pipeline(str(log_file), profile="traffic", out_dir=str(root / "out"), agent="on")

            self.assertIsNotNone(result.agent_result)
            agent_result = result.agent_result
            self.assertEqual("traffic", agent_result["profile"])
            self.assertEqual("completed", agent_result["status"])
            self.assertIn(agent_result["overall_status"], {"ok", "warning", "critical", "limited"})
            self.assertIn("short_summary", agent_result)
            self.assertIn("technical_summary", agent_result)
            self.assertIn("business_summary", agent_result)
            self.assertTrue(agent_result["cards"])
            self.assertEqual("traffic", agent_result["cards"][0]["card_type"])
            self.assertNotIn("answer", agent_result)
            self.assertNotIn("plan", agent_result)
            self.assertNotIn("trace", agent_result)
            self.assertNotIn("visuals", agent_result)
            self.assertTrue(result.findings)
            self.assertEqual(result.summary.short_summary, agent_result["short_summary"])
            self.assertEqual(result.summary.quality_status, result.quality.status)
            self.assertTrue((Path(result.output_dir) / "findings.json").exists())

    def test_public_output_is_compact_and_agent_context_is_not_persisted(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:20:50,000 ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "incidents.log"
            log_file.write_text(content, encoding="utf-8")

            result = run_pipeline(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
                agent="on",
            )
            summary = json.loads((Path(result.output_dir) / "run_summary.json").read_text(encoding="utf-8"))
            serialized = json.dumps(summary, ensure_ascii=False)

            self.assertEqual("incidents", summary["profile"])
            self.assertNotIn("findings", summary)
            self.assertIn("quality", summary)
            self.assertNotIn("raw_text", serialized)
            self.assertLess(len(serialized), 12000)
            self.assertFalse((Path(result.output_dir) / "agent_input_context.json").exists())

    def test_structured_llm_payload_validates_to_profile_cards(self) -> None:
        input_context = AgentInputContext(
            profile="incidents",
            run_id="r1",
            run_summary={"event_count": 2},
            parser_diagnostics={"parse_quality": {"score": 0.9, "label": "high"}},
            profile_fit={"fit_label": "high"},
            facts={
                "compact_llm_ready_cluster_facts": [
                    {
                        "cluster_id": "cluster-a",
                        "hits": 2,
                        "incident_hits": 2,
                        "confidence_score": 0.8,
                        "confidence_label": "high",
                    }
                ]
            },
        )
        payload = {
            "profile": "incidents",
            "overall_status": "warning",
            "confidence": 0.82,
            "short_summary": "Startup failures dominate.",
            "technical_summary": "Cluster cluster-a contains repeated startup exceptions.",
            "business_summary": "Startup instability may delay service availability.",
            "key_findings": ["cluster-a is repeated"],
            "recommended_actions": ["Inspect startup configuration"],
            "limitations": [],
            "cards": [
                {
                    "card_type": "incident",
                    "cluster_id": "cluster-a",
                    "title": "Startup failure",
                    "severity": "high",
                    "summary": "Repeated startup exception.",
                }
            ],
        }

        result = validate_agent_result_payload(payload, input_context, AgentModelConfig(provider="yandex", model="m"))

        self.assertEqual("incidents", result.profile)
        self.assertEqual("warning", result.overall_status)
        self.assertIsInstance(result.cards[0], IncidentCard)
        self.assertEqual("cluster-a", result.cards[0].cluster_id)

    def test_yandex_config_reads_yc_dotenv_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "YC_AI_API_KEY=test-key\nYC_FOLDER_ID=test-folder\n",
                encoding="utf-8",
            )
            old_cwd = Path.cwd()
            try:
                os.chdir(root)
                with patch.dict(os.environ, {}, clear=True):
                    config = resolve_agent_model_config("yandex")
            finally:
                os.chdir(old_cwd)

        self.assertEqual("yandex", config.provider)
        self.assertEqual("test-key", config.api_key)
        self.assertEqual("test-folder", config.folder_id)
        self.assertEqual("gpt://test-folder/yandexgpt/latest", config.model)
        self.assertTrue(provider_is_configured(config))

    def test_yandex_without_required_config_uses_deterministic_fallback(self) -> None:
        content = "2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=500 latency=1200ms size=32 ip=10.0.0.1\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "traffic.log"
            log_file.write_text(content, encoding="utf-8")
            with patch.dict(os.environ, {"YC_AI_API_KEY": "", "YC_FOLDER_ID": ""}, clear=False), patch(
                "logcopilot.agent.stage._invoke_structured_llm",
                side_effect=AssertionError("LLM should not be called when Yandex config is incomplete"),
            ):
                result = run_pipeline(
                    str(log_file),
                    profile="traffic",
                    out_dir=str(root / "out"),
                    agent="on",
                    agent_provider="yandex",
                )

        self.assertIsNotNone(result.agent_result)
        self.assertEqual("yandex", result.agent_result["provider"])
        self.assertTrue(result.agent_result["cards"])


if __name__ == "__main__":
    unittest.main()
