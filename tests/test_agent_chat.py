import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from logcopilot.agent.agent import MAX_PROMPT_ROWS, ask_agent, build_prompt_payload, prepare_agent_context, stream_agent
from logcopilot.agent.config import AgentModelConfig
from logcopilot.service import run_profile


class AgentChatTests(unittest.TestCase):
    def test_capabilities_question_does_not_trigger_data_action(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            execution = prepare_agent_context(
                question="Привет, что ты умеешь?",
                run_id=result.run_id,
                db_path=result.db_path,
            )
            self.assertEqual("capabilities", execution.plan["action"])
            self.assertIsNone(execution.plan["visual_hint"])

    def test_incident_run_uses_incident_action_only_and_sets_visual_hint(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:21:15 INFO Gateway - GET /api/orders status=500 latency=1300ms size=256 ip=10.0.0.1
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            execution = prepare_agent_context(
                question="Покажи топ инциденты",
                run_id=result.run_id,
                db_path=result.db_path,
            )
            self.assertEqual("incidents", execution.profile)
            self.assertEqual("top_incidents", execution.plan["action"])
            self.assertEqual("incidents_top_clusters", execution.plan["visual_hint"]["kind"])
            self.assertNotIn(execution.plan["action"], {"heatmap", "traffic_summary", "traffic_500"})

    def test_agent_returns_fallback_answer_and_generates_chart_without_live_llm(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:20:50,000 ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            answer = ask_agent(
                question="Покажи топ инциденты",
                run_id=result.run_id,
                db_path=result.db_path,
                provider="local",
                base_url="http://127.0.0.1:1/v1",
                api_key="dummy",
            )

            self.assertIn("Топ инциденты", answer.answer)
            self.assertTrue(answer.visuals)
            self.assertTrue(Path(answer.visuals[0]["path"]).exists())

    def test_follow_up_question_uses_session_memory(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:20:50,000 ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            first_turn = prepare_agent_context(
                question="Покажи топ-3 инцидента",
                run_id=result.run_id,
                db_path=result.db_path,
            )
            self.assertEqual("top_incidents", first_turn.plan["action"])
            self.assertTrue(first_turn.memory.get("focused_cluster_id"))

            second_turn = prepare_agent_context(
                question="Как это исправить?",
                run_id=result.run_id,
                db_path=result.db_path,
                session_state=first_turn.memory,
            )
            self.assertEqual("incident_cluster_causes", second_turn.plan["action"])

    def test_greeting_with_incident_question_prefers_incident_action(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            execution = prepare_agent_context(
                question="Привет что скажешь по логам какие тут инциденты есть?",
                run_id=result.run_id,
                db_path=result.db_path,
            )
            self.assertEqual("top_incidents", execution.plan["action"])

    def test_explain_follow_up_prefers_focused_incident_cluster(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:20:50,000 ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            first_turn = prepare_agent_context(
                question="Покажи топ-3 инцидента",
                run_id=result.run_id,
                db_path=result.db_path,
            )
            second_turn = prepare_agent_context(
                question="Объясни, с чем связан этот инцидент?",
                run_id=result.run_id,
                db_path=result.db_path,
                session_state=first_turn.memory,
            )
            self.assertEqual("incident_cluster_causes", second_turn.plan["action"])

    def test_incident_prompt_payload_does_not_include_unrelated_traffic_readiness(self) -> None:
        facts = {
            "profile": "incidents",
            "run_summary": {"run_id": "r1", "profile": "incidents"},
            "data_quality": {
                "event_count": 12,
                "field_coverage": {"timestamp": 1.0, "component": 1.0, "method": 0.0, "path": 0.0},
                "parser_quality": {"score": 0.5, "label": "medium"},
                "visual_readiness": {
                    "incidents": {"available": True, "reason": None},
                    "traffic": {"available": False, "reason": "method/path coverage is too low"},
                },
            },
            "visual_info": {"status": "skipped"},
            "top_incidents": [],
        }

        payload = build_prompt_payload(
            plan={"action": "overview"},
            facts=facts,
            visuals=[],
        )

        self.assertIn("incidents", payload["data_quality"]["visual_readiness"])
        self.assertNotIn("traffic", payload["data_quality"]["visual_readiness"])

    def test_heatmap_visual_is_skipped_when_timestamp_coverage_is_low(self) -> None:
        content = """Gateway error happened
Another line without timestamp
Third line without timestamp
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "heatmap.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(str(log_file), profile="heatmap", out_dir=str(root / "out"))

            execution = prepare_agent_context(
                question="Покажи heatmap",
                run_id=result.run_id,
                db_path=result.db_path,
            )

            self.assertEqual("heatmap", execution.plan["action"])
            self.assertFalse(execution.visuals)
            self.assertEqual("unavailable", execution.facts["visual_info"]["status"])

    def test_prepare_agent_context_does_not_open_gui_windows(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
2026-03-11 08:20:50,000 ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            with patch("matplotlib.pyplot.show", side_effect=AssertionError("show should not be called")):
                execution = prepare_agent_context(
                    question="Покажи топ инциденты",
                    run_id=result.run_id,
                    db_path=result.db_path,
                )
            self.assertTrue(execution.visuals)

    def test_prompt_payload_limits_rows_and_keeps_compact_shape(self) -> None:
        facts = {
            "run_summary": {"run_id": "r1", "profile": "incidents"},
            "data_quality": {"event_count": 12},
            "visual_info": {"status": "created"},
            "top_incidents": [
                {
                    "cluster_id": f"cluster-{index}",
                    "incident_hits": index,
                    "hits": index,
                    "confidence_label": "high",
                    "payload_json": {"exception_type": "ValueError", "verbose": "x" * 200},
                }
                for index in range(10)
            ],
        }

        payload = build_prompt_payload(
            plan={"action": "top_incidents"},
            facts=facts,
            visuals=[{"kind": "incidents_top_clusters", "title": "Top", "caption": "caption", "path": "/tmp/a.png"}],
        )

        self.assertEqual(MAX_PROMPT_ROWS, len(payload["top_incidents"]))
        self.assertIn("visuals", payload)
        self.assertNotIn("payload_json", payload["top_incidents"][0])

    def test_overview_payload_includes_incident_content_not_only_ids(self) -> None:
        facts = {
            "profile": "incidents",
            "run_summary": {"run_id": "r1", "profile": "incidents"},
            "data_quality": {"event_count": 12},
            "visual_info": {"status": "skipped"},
            "top_incidents": [
                {
                    "cluster_id": "c1",
                    "incident_hits": 10,
                    "hits": 10,
                    "confidence_label": "high",
                    "representative_text": "failed password for root from <ip>",
                    "payload_json": {
                        "sample_messages": [
                            "Failed password for root from 192.0.2.10 port 2222 ssh2",
                            "Failed password for root from 192.0.2.11 port 2222 ssh2",
                        ],
                        "levels": "UNKNOWN:10",
                        "source_files": "OpenSSH_2k.log (10)",
                        "exception_type": None,
                    },
                }
            ],
        }

        payload = build_prompt_payload(
            plan={"action": "overview"},
            facts=facts,
            visuals=[],
        )

        self.assertEqual("failed password for root from <ip>", payload["top_incidents"][0]["representative_text"])
        self.assertTrue(payload["top_incidents"][0]["sample_messages"])

    def test_yandex_provider_streams_and_extracts_block_content(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""

        class FakeLLM:
            def stream(self, messages):
                class Chunk:
                    def __init__(self, content):
                        self.content = content

                yield Chunk([{"text": "Сводка по " }])
                yield Chunk([{"text": "логам готова."}])

            def invoke(self, messages):
                raise AssertionError("invoke should not be called when streaming works")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            with patch(
                "logcopilot.agent.agent.resolve_model_config",
                return_value=AgentModelConfig(
                    provider="yandex",
                    model="dummy",
                    base_url="https://example.invalid",
                    api_key="dummy",
                    temperature=0.1,
                    folder_id="folder",
                ),
            ), patch("logcopilot.agent.agent.build_chat_model", return_value=FakeLLM()):
                execution, iterator = stream_agent(
                    question="Что там по логам?",
                    run_id=result.run_id,
                    provider="yandex",
                    db_path=result.db_path,
                )
                answer = "".join(iterator)

            self.assertEqual("Сводка по логам готова.", answer)
            self.assertIn("answer: llm_stream", execution.trace)

    def test_yandex_provider_falls_back_to_invoke_if_stream_breaks(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] ERROR Host Start - Startup exception|System.Security.SecurityException: Login failed
   at Foo.Bar()
"""

        class FakeResponse:
            content = [{"text": "Резервный ответ готов."}]

        class FakeLLM:
            def stream(self, messages):
                raise RuntimeError("stream failed")

            def invoke(self, messages):
                return FakeResponse()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "agent.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="incidents",
                out_dir=str(root / "out"),
                semantic="off",
            )

            with patch(
                "logcopilot.agent.agent.resolve_model_config",
                return_value=AgentModelConfig(
                    provider="yandex",
                    model="dummy",
                    base_url="https://example.invalid",
                    api_key="dummy",
                    temperature=0.1,
                    folder_id="folder",
                ),
            ), patch("logcopilot.agent.agent.build_chat_model", return_value=FakeLLM()):
                execution, iterator = stream_agent(
                    question="Что там по логам?",
                    run_id=result.run_id,
                    provider="yandex",
                    db_path=result.db_path,
                )
                answer = "".join(iterator)

            self.assertEqual("Резервный ответ готов.", answer)
            self.assertTrue(any("answer: llm_invoke_fallback" in item for item in execution.trace))

    def test_profile_fit_question_routes_to_profile_fit_summary(self) -> None:
        content = """127.0.0.1 - - [11/Mar/2026:08:21:15 +0000] "GET /api/orders HTTP/1.1" 200 321
127.0.0.1 - - [11/Mar/2026:08:21:16 +0000] "GET /api/orders HTTP/1.1" 500 321
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "access.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="heatmap",
                out_dir=str(root / "out"),
                semantic="off",
            )

            execution = prepare_agent_context(
                question="Правильный ли я сценарий выбрал?",
                run_id=result.run_id,
                db_path=result.db_path,
            )
            self.assertEqual("profile_fit_summary", execution.plan["action"])

    def test_malformed_planner_visual_hint_falls_back_to_rule_plan(self) -> None:
        content = """127.0.0.1 - - [11/Mar/2026:08:21:15 +0000] "GET /api/orders HTTP/1.1" 200 321
127.0.0.1 - - [11/Mar/2026:08:21:16 +0000] "GET /api/orders HTTP/1.1" 500 321
"""

        class FakePlannerResponse:
            content = '{"action":"profile_fit_summary","args":"oops","visual_hint":"chart"}'

        class FakePlanner:
            def invoke(self, messages):
                return FakePlannerResponse()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "access.log"
            log_file.write_text(content, encoding="utf-8")
            result = run_profile(
                str(log_file),
                profile="heatmap",
                out_dir=str(root / "out"),
                semantic="off",
            )

            execution = prepare_agent_context(
                question="Правильный ли я сценарий выбрал?",
                run_id=result.run_id,
                db_path=result.db_path,
                planner_model=FakePlanner(),
            )
            self.assertEqual("profile_fit_summary", execution.plan["action"])


if __name__ == "__main__":
    unittest.main()
