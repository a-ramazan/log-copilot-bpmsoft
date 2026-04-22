import json
import tempfile
from pathlib import Path
import unittest

from logcopilot.pipeline import run_pipeline
from logcopilot.storage import StorageRepository


class AgentPersistenceTests(unittest.TestCase):
    def test_agent_result_and_cards_are_persisted(self) -> None:
        content = """2026-03-11 08:20:00 INFO Gateway - GET /api/orders status=500 latency=1200ms size=32 ip=10.0.0.1
2026-03-11 08:20:10 INFO Gateway - GET /api/orders status=500 latency=1500ms size=33 ip=10.0.0.2
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_file = root / "traffic.log"
            log_file.write_text(content, encoding="utf-8")

            result = run_pipeline(str(log_file), profile="traffic", out_dir=str(root / "out"), agent="on")
            repo = StorageRepository(Path(result.db_path))
            stored_result = repo.get_agent_result(result.run_id)
            stored_cards = repo.get_agent_cards(result.run_id)
            cards_artifact = json.loads((Path(result.output_dir) / "agent_cards.json").read_text(encoding="utf-8"))

            self.assertIsNotNone(stored_result)
            self.assertEqual("completed", stored_result["status"])
            self.assertEqual("traffic", stored_result["profile"])
            self.assertTrue(stored_result["cards_json"])
            self.assertTrue(stored_cards)
            self.assertEqual(stored_cards[0]["payload_json"]["card_type"], cards_artifact[0]["card_type"])
            self.assertIn("agent_cards_json", result.artifact_paths)


if __name__ == "__main__":
    unittest.main()
