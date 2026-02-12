import argparse
import subprocess
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="Run Bronze load then Gold refresh")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--agent-id", type=int, default=None, help="Optional agent filter")
    return parser.parse_args()


def run_step(command: list[str], step_name: str):
    print(f"\n=== {step_name} ===")
    print(" ".join(command))
    result = subprocess.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{step_name} failed with exit code {result.returncode}")


def main():
    args = parse_args()

    bronze_cmd = [sys.executable, "load_bronze_incremental.py"]
    gold_cmd = [
        sys.executable,
        "refresh_gold.py",
        "--from-date",
        args.from_date,
        "--to-date",
        args.to_date,
    ]

    if args.agent_id is not None:
        gold_cmd.extend(["--agent-id", str(args.agent_id)])

    run_step(bronze_cmd, "Bronze Incremental Load")
    run_step(gold_cmd, "Gold Refresh")

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
