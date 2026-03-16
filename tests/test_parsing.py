import tempfile
from pathlib import Path
import unittest

from logcopilot.parsing import iter_events_for_file


class ParsingTests(unittest.TestCase):
    def test_multiline_stacktrace_is_grouped_into_single_event(self) -> None:
        content = """2026-03-11 08:20:49,617 [1] FATAL  Host Start - Startup exception|System.Security.SecurityException: System login error.
   at Foo.Bar()
   at Baz.Qux()
2026-03-11 08:21:15,037 [1] ERROR  Host Start - Hosting failed to start
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            file_path = root / "Error.log"
            file_path.write_text(content, encoding="utf-8")
            events = list(iter_events_for_file(file_path, root))

        self.assertEqual(2, len(events))
        self.assertEqual("FATAL", events[0].level)
        self.assertIn("System.Security.SecurityException", events[0].stacktrace)
        self.assertIn("Foo.Bar", events[0].stacktrace)
        self.assertEqual("Hosting failed to start", events[1].message)


if __name__ == "__main__":
    unittest.main()

