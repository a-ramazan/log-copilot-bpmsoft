from __future__ import annotations

"""Command-line interface for LogCopilot pipeline execution."""

import argparse

from .domain import RunResult
from .pipeline import run_pipeline


def _add_run_arguments(
    parser: argparse.ArgumentParser,
    *,
    require_input: bool,
    require_profile: bool,
) -> None:
    parser.add_argument("--input", required=require_input, help="Path to a single .log file")
    parser.add_argument(
        "--profile",
        required=require_profile,
        choices=("heatmap", "incidents", "traffic"),
        help="Processing scenario",
    )
    parser.add_argument("--out", default="out", help="Base output directory")
    parser.add_argument("--clean-out", action="store_true", help="Clean the run directory before writing")
    parser.add_argument("--semantic", choices=("off", "auto", "on"), default="on")
    parser.add_argument("--semantic-model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--semantic-min-cluster-size", type=int, default=3)
    parser.add_argument("--semantic-min-samples", type=int, default=None)
    parser.add_argument("--sample-events", type=int, default=0)
    parser.add_argument("--agent", choices=("off", "on"), default="off", help="Legacy flag; interpretation stage always runs")
    parser.add_argument("--agent-provider", choices=("none", "yandex"), default="none", help="External LLM provider for the mandatory interpretation stage")
    parser.add_argument("--agent-question", default=None)


def _print_run_result(result: RunResult) -> None:
    print(f"run_id: {result.run_id}")
    print(f"profile: {result.profile}")
    print(f"status: {result.status}")
    print(f"events: {result.event_count}")
    print(f"db_path: {result.db_path}")
    print(f"output_dir: {result.output_dir}")
    for artifact_name, artifact_path in sorted(result.artifact_paths.items()):
        print(f"{artifact_name}: {artifact_path}")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for pipeline execution."""
    parser = argparse.ArgumentParser(description="LogCopilot CLI")
    _add_run_arguments(parser, require_input=False, require_profile=False)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run one processing profile for one .log file")
    _add_run_arguments(run_parser, require_input=True, require_profile=True)
    return parser


def main() -> None:
    """Parse command-line arguments and execute the requested pipeline mode."""
    parser = build_parser()
    args = parser.parse_args()
    if not args.input:
        parser.error("--input is required")

    result = run_pipeline(
        input_path=args.input,
        profile=args.profile or "incidents",
        out_dir=args.out,
        clean_out=args.clean_out,
        sample_events=args.sample_events,
        semantic=args.semantic,
        semantic_model=args.semantic_model,
        semantic_min_cluster_size=args.semantic_min_cluster_size,
        semantic_min_samples=args.semantic_min_samples,
        agent=args.agent,
        agent_provider=args.agent_provider,
        agent_question=args.agent_question,
    )
    _print_run_result(result)


__all__ = [
    "build_parser",
    "main",
]


if __name__ == "__main__":
    main()
