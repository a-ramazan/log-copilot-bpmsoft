from __future__ import annotations

"""Command-line entry point and public pipeline API for LogCopilot."""

import argparse

from .domain import PipelineRunResult, RunResult
from .runtime._runner import build_legacy_pipeline_result, run_pipeline


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
    parser.add_argument("--log-level", choices=("INFO", "DEBUG"), default="INFO")


def _print_run_result(result: RunResult) -> None:
    print(f"run_id: {result.run_id}")
    print(f"profile: {result.profile}")
    print(f"status: {result.status}")
    print(f"events: {result.event_count}")
    print(f"db_path: {result.db_path}")
    print(f"output_dir: {result.output_dir}")
    for artifact_name, artifact_path in sorted(result.artifact_paths.items()):
        print(f"{artifact_name}: {artifact_path}")


def _print_legacy_result(result: PipelineRunResult) -> None:
    print(f"run_id: {result.run_id}")
    print(f"profile: {result.profile}")
    print(f"status: {result.status}")
    print(f"events: {result.event_count}")
    print(f"signature_clusters: {result.cluster_count}")
    print(f"semantic_clusters: {result.semantic_cluster_count}")
    print(f"output_dir: {result.output_dir}")
    for artifact_name, artifact_path in sorted(result.artifact_paths.items()):
        print(f"{artifact_name}: {artifact_path}")


def build_parser() -> argparse.ArgumentParser:
    """Build the unified CLI parser for modern and legacy invocations.

    Returns:
        Configured argument parser for both `run` subcommand and legacy flags.
    """
    parser = argparse.ArgumentParser(description="LogCopilot CLI")
    _add_run_arguments(parser, require_input=False, require_profile=False)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run one processing profile for one .log file")
    _add_run_arguments(run_parser, require_input=True, require_profile=True)
    return parser


def _run_legacy_pipeline(
    input_path: str,
    out_dir: str,
    clean_out: bool = False,
    log_level: str = "INFO",
    sample_events: int = 0,
    semantic: str = "on",
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    semantic_min_cluster_size: int = 3,
    semantic_min_samples: int | None = None,
) -> PipelineRunResult:
    """Run the legacy incidents-only wrapper retained for old CLI behavior."""
    del log_level
    run_result = run_pipeline(
        input_path=input_path,
        profile="incidents",
        out_dir=out_dir,
        clean_out=clean_out,
        sample_events=sample_events,
        semantic=semantic,
        semantic_model=semantic_model,
        semantic_min_cluster_size=semantic_min_cluster_size,
        semantic_min_samples=semantic_min_samples,
    )
    return build_legacy_pipeline_result(run_result)


def main() -> None:
    """Parse command-line arguments and execute the requested pipeline mode.

    Returns:
        None.
    """
    parser = build_parser()
    args = parser.parse_args()
    if not args.input:
        parser.error("--input is required")

    if args.command == "run" or args.profile:
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
        )
        _print_run_result(result)
        return

    result = _run_legacy_pipeline(
        input_path=args.input,
        out_dir=args.out,
        clean_out=args.clean_out,
        log_level=args.log_level,
        sample_events=args.sample_events,
        semantic=args.semantic,
        semantic_model=args.semantic_model,
        semantic_min_cluster_size=args.semantic_min_cluster_size,
        semantic_min_samples=args.semantic_min_samples,
    )
    _print_legacy_result(result)


__all__ = [
    "build_legacy_pipeline_result",
    "build_parser",
    "main",
    "run_pipeline",
]


if __name__ == "__main__":
    main()
