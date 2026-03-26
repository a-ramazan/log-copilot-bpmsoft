import argparse

from .service import build_legacy_pipeline_result, run_profile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Legacy incident pipeline wrapper")
    parser.add_argument("--input", required=True, help="Path to a single .log file")
    parser.add_argument("--out", required=True, help="Base output directory")
    parser.add_argument("--clean-out", action="store_true")
    parser.add_argument("--log-level", choices=("INFO", "DEBUG"), default="INFO")
    parser.add_argument("--sample-events", type=int, default=0)
    parser.add_argument("--semantic", choices=("off", "auto", "on"), default="on")
    parser.add_argument(
        "--semantic-model",
        default="sentence-transformers/all-MiniLM-L6-v2",
    )
    parser.add_argument("--semantic-min-cluster-size", type=int, default=3)
    parser.add_argument("--semantic-min-samples", type=int, default=None)
    return parser.parse_args()


def run_pipeline(
    input_path: str,
    out_dir: str,
    clean_out: bool = False,
    log_level: str = "INFO",
    sample_events: int = 0,
    semantic: str = "on",
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    semantic_min_cluster_size: int = 3,
    semantic_min_samples: int | None = None,
):
    del log_level
    run_result = run_profile(
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
    args = parse_args()
    result = run_pipeline(
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
    print(f"run_id: {result.run_id}")
    print(f"profile: {result.profile}")
    print(f"status: {result.status}")
    print(f"events: {result.event_count}")
    print(f"signature_clusters: {result.cluster_count}")
    print(f"semantic_clusters: {result.semantic_cluster_count}")
    print(f"output_dir: {result.output_dir}")
    for artifact_name, artifact_path in sorted(result.artifact_paths.items()):
        print(f"{artifact_name}: {artifact_path}")


if __name__ == "__main__":
    main()
