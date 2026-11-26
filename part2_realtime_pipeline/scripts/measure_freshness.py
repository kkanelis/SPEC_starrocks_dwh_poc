import argparse
import json
import math
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def query_freshness(database: str, table: str, container: str) -> float:
    sql = (
        "SELECT DATE_FORMAT(MAX(trade_time), '%Y-%m-%d %H:%i:%s.%f'), "
        f"DATE_FORMAT(CURRENT_TIMESTAMP(3), '%Y-%m-%d %H:%i:%s.%f') FROM {database}.{table}"
    )
    result = subprocess.run(
        [
            "docker", "exec", container,
            "mysql", "--batch", "--skip-column-names",
            "-h", "127.0.0.1", "-P", "9030", "-u", "root", "-e", sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    values = result.stdout.strip().split("\t")
    if len(values) != 2 or not values[0] or values[0].upper() == "NULL":
        raise RuntimeError(f"No trade_time rows found in {database}.{table}")
    timestamp_format = "%Y-%m-%d %H:%M:%S.%f"
    latest_trade = datetime.strptime(values[0], timestamp_format).replace(tzinfo=timezone.utc)
    sampled_at = datetime.strptime(values[1], timestamp_format).replace(tzinfo=timezone.utc)
    return (sampled_at - latest_trade).total_seconds()


def percentile_nearest_rank(values: list, percentile: float) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sample newest-row freshness from StarRocks.")
    parser.add_argument("--duration-seconds", type=float, default=240)
    parser.add_argument("--interval-seconds", type=float, default=2)
    parser.add_argument("--database", default="poc_stream")
    parser.add_argument("--table", default="crypto_trades")
    parser.add_argument("--starrocks-container", default="quickstart")
    parser.add_argument("--output", type=Path, default=Path("benchmark_results/freshness_samples.jsonl"))
    args = parser.parse_args()
    if args.duration_seconds <= 0 or args.interval_seconds <= 0:
        parser.error("duration and interval must be positive")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    samples = []
    started = time.monotonic()
    next_sample = started
    with args.output.open("w", encoding="utf-8") as file:
        while time.monotonic() - started < args.duration_seconds:
            freshness_seconds = query_freshness(
                args.database, args.table, args.starrocks_container
            )
            sample = {
                "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
                "freshness_seconds": freshness_seconds,
            }
            file.write(json.dumps(sample) + "\n")
            file.flush()
            samples.append(freshness_seconds)
            print(json.dumps(sample), flush=True)
            next_sample += args.interval_seconds
            time.sleep(max(0, next_sample - time.monotonic()))

    summary = {
        "samples": len(samples),
        "interval_seconds": args.interval_seconds,
        "median_seconds": statistics.median(samples),
        "p95_nearest_rank_seconds": percentile_nearest_rank(samples, 0.95),
        "max_seconds": max(samples),
        "raw_samples": display_path(args.output),
    }
    summary_path = args.output.with_name("freshness_summary.json")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
