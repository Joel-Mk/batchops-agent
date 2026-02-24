import argparse
from src.orchestrator import run_pipeline, resume_pipeline

def main():
    parser = argparse.ArgumentParser(description="BatchOps Agent CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("--provider", required=True)
    run_parser.add_argument("--pipeline", required=True)
    run_parser.add_argument("--snapshot-date", required=True)

    # Resume command
    resume_parser = subparsers.add_parser("resume", help="Resume a failed run")
    resume_parser.add_argument("--run-id", required=True)

    args = parser.parse_args()

    if args.command == "run":
        run_pipeline(
            provider=args.provider,
            pipeline=args.pipeline,
            snapshot_date=args.snapshot_date
        )

    elif args.command == "resume":
        resume_pipeline(args.run_id)

    else:
        parser.print_help()

if __name__ == "__main__":
    main()