import tempfile
from pathlib import Path
import unittest

from logcopilot.agent.agent import ask_agent, prepare_agent_context
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

    def test_incident_run_uses_incident_action_only(self) -> None:
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
            self.assertNotIn(execution.plan["action"], {"heatmap", "traffic_summary", "traffic_500"})

    def test_agent_returns_fallback_answer_without_live_llm(self) -> None:
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

            answer = ask_agent(
                question="Покажи топ инциденты",
                run_id=result.run_id,
                db_path=result.db_path,
                provider="local",
                base_url="http://127.0.0.1:1/v1",
                api_key="dummy",
            )

            self.assertIn("Топ инциденты", answer.answer)

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
            self.assertEqual("incident_cluster", second_turn.plan["action"])


if __name__ == "__main__":
    unittest.main()
