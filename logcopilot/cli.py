from __future__ import annotations

import argparse

from .service import run_profile


def build_parser() -> argparse.ArgumentParser:
    """

    :return:
    """
    parser = argparse.ArgumentParser(description="LogCopilot CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run one processing profile for one .log file")
    run_parser.add_argument("--input", required=True, help="Path to a single .log file")
    run_parser.add_argument(
        "--profile",
        required=True,
        choices=("heatmap", "incidents", "traffic"),
        help="Processing scenario",
    )
    run_parser.add_argument("--out", default="out", help="Base output directory")
    run_parser.add_argument("--clean-out", action="store_true", help="Clean the run directory before writing")
    run_parser.add_argument("--semantic", choices=("off", "auto", "on"), default="on")
    run_parser.add_argument("--semantic-model", default="sentence-transformers/all-MiniLM-L6-v2")
    run_parser.add_argument("--semantic-min-cluster-size", type=int, default=3)
    run_parser.add_argument("--semantic-min-samples", type=int, default=None)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command != "run":
        parser.error(f"Unsupported command: {args.command}")

    result = run_profile(
        input_path=args.input,
        profile=args.profile,
        out_dir=args.out,
        clean_out=args.clean_out,
        semantic=args.semantic,
        semantic_model=args.semantic_model,
        semantic_min_cluster_size=args.semantic_min_cluster_size,
        semantic_min_samples=args.semantic_min_samples,
    )
    print(f"run_id: {result.run_id}")
    print(f"profile: {result.profile}")
    print(f"status: {result.status}")
    print(f"events: {result.event_count}")
    print(f"db_path: {result.db_path}")
    print(f"output_dir: {result.output_dir}")
    for artifact_name, artifact_path in sorted(result.artifact_paths.items()):
        print(f"{artifact_name}: {artifact_path}")


if __name__ == "__main__":
    main()
