import tempfile
from pathlib import Path
import unittest

from logcopilot.parsing import build_default_registry, iter_events_for_file, parse_file
from logcopilot.parsing.parsers import (
    GenericFallbackParser,
    JsonParser,
    LogfmtParser,
    SyslogParser,
    TextMultilineParser,
    WebAccessParser,
    WindowsServicingParser,
)
from logcopilot.core.events import build_event
from logcopilot.quality import assess_profile_fit
from logcopilot.parsing.pipeline import canonical_to_raw_event


class ParserSubsystemTests(unittest.TestCase):
    def test_json_logs_parser_preserves_unknown_fields(self) -> None:
        parser = JsonParser()
        content = (
            '{"timestamp":"2026-03-11T08:21:15Z","level":"error","message":"boom",'
            '"request_id":"r-1","custom":"x"}\n'
        )

        result = parser.parse(content, source="app.log")

        self.assertEqual("json", result.parser_name)
        self.assertEqual(1, len(result.events))
        event = result.events[0]
        self.assertEqual("ERROR", event.level)
        self.assertEqual("boom", event.message)
        self.assertEqual("r-1", event.request_id)
        self.assertEqual({"custom": "x"}, event.attributes)
        self.assertGreater(result.confidence, 0.7)

    def test_json_parser_degrades_on_dirty_input(self) -> None:
        parser = JsonParser()
        content = '{"message":"ok","level":"info"}\nnot-json\n'

        result = parser.parse(content, source="mixed.log")

        self.assertEqual(2, len(result.events))
        self.assertEqual("generic_fallback", result.events[1].parser_name)
        self.assertGreater(result.stats["fallback_ratio"], 0.0)
        self.assertTrue(result.warnings)

    def test_logfmt_parser_maps_known_fields_and_keeps_rest(self) -> None:
        parser = LogfmtParser()
        content = 'time=2026-03-11T08:21:15Z level=warn msg="cache miss" request_id=req-7 foo=bar latency=12ms\n'

        result = parser.parse(content, source="app.log")

        event = result.events[0]
        self.assertEqual("WARN", event.level)
        self.assertEqual("cache miss", event.message)
        self.assertEqual("req-7", event.request_id)
        self.assertEqual(12.0, event.latency_ms)
        self.assertEqual({"foo": "bar"}, event.attributes)

    def test_web_access_parser_extracts_request_fields(self) -> None:
        parser = WebAccessParser()
        content = '127.0.0.1 - - [11/Mar/2026:08:21:15 +0000] "GET /api/orders HTTP/1.1" 200 321 "-" "curl/8.0" request_time=0.245\n'

        result = parser.parse(content, source="access.log")

        event = result.events[0]
        self.assertEqual("GET", event.http_method)
        self.assertEqual("/api/orders", event.http_path)
        self.assertEqual(200, event.http_status)
        self.assertEqual(245.0, event.latency_ms)
        self.assertEqual("127.0.0.1", event.client_ip)
        self.assertEqual("curl/8.0", event.user_agent)

    def test_syslog_parser_extracts_host_component_and_message(self) -> None:
        parser = SyslogParser()
        content = "Mar 11 08:21:15 host1 sshd[123]: ERROR Failed password for invalid user root from 192.0.2.10 port 2222 ssh2\n"

        result = parser.parse(content, source="syslog.log")

        event = result.events[0]
        self.assertEqual("ERROR", event.level)
        self.assertEqual("sshd", event.component)
        self.assertIn("Failed password", event.message)
        self.assertEqual("host1", event.attributes["host"])
        self.assertEqual("192.0.2.10", event.client_ip)

    def test_windows_servicing_parser_extracts_level_component_and_csi_prefix(self) -> None:
        parser = WindowsServicingParser()
        content = (
            "2016-09-28 04:30:31, Info                  CSI    "
            "00000001@2016/9/27:20:30:31.455 WcpInitialize called\n"
            "2016-09-28 04:30:31, Info                  CBS    Ending TrustedInstaller initialization.\n"
        )

        result = parser.parse(content, source="Windows_2k.log")

        self.assertEqual("windows_servicing", result.parser_name)
        self.assertEqual(2, len(result.events))
        self.assertEqual("INFO", result.events[0].level)
        self.assertEqual("CSI", result.events[0].component)
        self.assertEqual("00000001@2016/9/27:20:30:31.455", result.events[0].attributes["csi_prefix"])
        self.assertIn("WcpInitialize called", result.events[0].message)
        self.assertEqual("CBS", result.events[1].component)
        self.assertGreater(result.confidence, 0.8)

    def test_text_multiline_parser_groups_java_and_stacktrace(self) -> None:
        parser = TextMultilineParser()
        content = """17/06/09 20:10:42 ERROR executor.CoarseGrainedExecutorBackend: Executor lost due to IOException
   at org.apache.spark.Executor.run(Executor.scala:123)
17/06/09 20:10:43 INFO executor.CoarseGrainedExecutorBackend: Successfully reconnected
"""

        result = parser.parse(content, source="spark.log")

        self.assertEqual(2, len(result.events))
        self.assertEqual("ERROR", result.events[0].level)
        self.assertIn("org.apache.spark.Executor.run", result.events[0].stacktrace)
        self.assertEqual(1, result.stats["multiline_events_count"])

    def test_generic_fallback_parser_is_conservative(self) -> None:
        parser = GenericFallbackParser()
        content = "weird custom line\n  still same blob\n\nnext blob\n"

        result = parser.parse(content, source="unknown.log")

        self.assertEqual("generic_fallback", result.parser_name)
        self.assertEqual(2, len(result.events))
        self.assertLessEqual(result.confidence, 0.4)
        self.assertEqual(1.0, result.stats["fallback_ratio"])

    def test_generic_fallback_extracts_minimal_structure_without_claiming_full_parse(self) -> None:
        parser = GenericFallbackParser()
        content = "2026-03-11 08:21:15 ERROR Worker - sync failed request_id=req-7 status=500\n"

        result = parser.parse(content, source="unknown.log")

        event = result.events[0]
        self.assertEqual("ERROR", event.level)
        self.assertEqual("Worker", event.component)
        self.assertEqual("req-7", event.request_id)
        self.assertEqual(500, event.http_status)
        self.assertLessEqual(event.parser_confidence, 0.5)

    def test_registry_selection_prefers_structured_parser_and_uses_fallback(self) -> None:
        registry = build_default_registry()
        parser, selection = registry.select('{"message":"ok","level":"info"}\n')
        self.assertEqual("json", parser.name)
        self.assertFalse(selection.used_fallback)

        parser, selection = registry.select("totally unknown format without strong signals")
        self.assertEqual("generic_fallback", parser.name)
        self.assertTrue(selection.used_fallback)

    def test_registry_selection_prefers_web_access_for_common_access_lines(self) -> None:
        registry = build_default_registry()
        parser, selection = registry.select(
            '199.60.47.128 - - [01/Jan/2016:00:25:53 +0100] '
            '"GET http://example.com/login HTTP/1.1" 302 0\n'
        )

        self.assertEqual("web_access", parser.name)
        self.assertFalse(selection.used_fallback)

    def test_confidence_scoring_favors_structured_parse_over_generic(self) -> None:
        structured = JsonParser().parse('{"timestamp":"2026-03-11T08:21:15Z","level":"info","message":"ok"}\n')
        generic = GenericFallbackParser().parse("just some raw text\n")
        self.assertGreater(structured.confidence, generic.confidence)

    def test_compatibility_iter_events_returns_raw_event_adapter(self) -> None:
        content = "2026-03-11 08:21:15 INFO Gateway - GET /api/orders status=500 latency=1450ms size=3210 ip=10.0.0.8 user-agent=Mozilla/5.0\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            file_path = root / "app.log"
            file_path.write_text(content, encoding="utf-8")

            events = list(iter_events_for_file(file_path, root))

        self.assertEqual(1, len(events))
        self.assertEqual("text_multiline", events[0].parser_profile)
        self.assertEqual("GET", events[0].method)
        self.assertEqual("/api/orders", events[0].path)
        self.assertGreater(events[0].parser_confidence, 0.5)

    def test_parse_file_exposes_parse_result_stats(self) -> None:
        content = "foo=bar message=test level=info\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            file_path = root / "app.log"
            file_path.write_text(content, encoding="utf-8")

            result = parse_file(file_path, root)

        self.assertEqual("logfmt", result.parser_name)
        self.assertEqual(1, result.stats["total_events"])
        self.assertGreaterEqual(result.stats["parsed_level_ratio"], 1.0)

    def test_profile_fit_prefers_traffic_for_access_logs(self) -> None:
        parser = WebAccessParser()
        content = (
            '199.60.47.128 - - [01/Jan/2016:00:25:53 +0100] "GET http://example.com/login HTTP/1.1" 302 0\n'
            '199.60.47.128 - - [01/Jan/2016:00:26:01 +0100] "POST http://example.com/api/orders HTTP/1.1" 500 128\n'
        )

        result = parser.parse(content, source="access.log")
        events = [build_event(canonical_to_raw_event(event, source_file="access.log"), "run-1") for event in result.events]
        fit = assess_profile_fit(events, selected_profile="incidents")

        self.assertEqual("traffic", fit["recommended_profile"])
        self.assertEqual("low", fit["fit_label"])

    def test_profile_fit_prefers_incidents_for_windows_servicing_errors(self) -> None:
        parser = WindowsServicingParser()
        content = (
            "2016-09-28 04:30:31, Info                  CBS    Expecting attribute name "
            "[HRESULT = 0x800f080d - CBS_E_MANIFEST_INVALID_ITEM]\n"
            "2016-09-28 04:30:31, Info                  CBS    Failed to get next element "
            "[HRESULT = 0x800f080d - CBS_E_MANIFEST_INVALID_ITEM]\n"
        )

        result = parser.parse(content, source="Windows_2k.log")
        events = [build_event(canonical_to_raw_event(event, source_file="Windows_2k.log"), "run-1") for event in result.events]
        fit = assess_profile_fit(events, selected_profile="incidents")

        self.assertEqual("incidents", fit["recommended_profile"])


if __name__ == "__main__":
    unittest.main()
