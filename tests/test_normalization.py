import unittest

from logcopilot.text import normalize_text


class NormalizationTests(unittest.TestCase):
    def test_masks_dynamic_values(self) -> None:
        text = (
            "User test@example.com from 173.100.0.3 failed at 2026-03-11 08:22:34 "
            "for entity 123456 and uid a09c4eea-f280-49c3-bb12-e8f9d8d94d70"
        )
        normalized = normalize_text(text)
        self.assertIn("<email>", normalized)
        self.assertIn("<ip>", normalized)
        self.assertIn("<datetime>", normalized)
        self.assertIn("<num>", normalized)
        self.assertIn("<uuid>", normalized)


if __name__ == "__main__":
    unittest.main()
