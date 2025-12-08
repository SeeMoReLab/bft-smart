#!/usr/bin/env python3
"""
Plot monitoring throughput over time from output/out.txt.

Reads lines containing the "Monitor" summary emitted by SmallBankClient and
builds a single time series where x = cumulative time (seconds) and
y = throughput (TPS) across all monitor intervals.
"""
import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt


MONITOR_RE = re.compile(r"Monitor[^\\n]*duration=([0-9.]+)s[^\\n]*tps=([0-9.eE+-]+)")


def parse_monitor(file_path: Path):
    times = []
    tps_values = []
    cumulative_time = 0.0

    with file_path.open() as f:
        for line in f:
            match = MONITOR_RE.search(line)
            if not match:
                continue
            duration = float(match.group(1))
            tps = float(match.group(2))
            cumulative_time += duration
            times.append(cumulative_time)
            tps_values.append(tps)
    return times, tps_values


def main():
    parser = argparse.ArgumentParser(description="Plot monitoring throughput from SmallBankClient output.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/out.txt"),
        help="Path to the output log file (default: output/out.txt)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to save the plot as an image; if omitted, shows an interactive window.",
    )
    args = parser.parse_args()

    if not args.input.exists():
        parser.error(f"Input file not found: {args.input}")

    times, tps_values = parse_monitor(args.input)
    if not times:
        parser.error("No monitor lines found in input.")

    plt.figure(figsize=(10, 5))
    plt.plot(times, tps_values, marker="o", linestyle="-", color="tab:blue", label="Throughput (TPS)")
    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (TPS)")
    plt.title("SmallBank Monitoring Throughput Over Time")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(args.output)
        print(f"Saved plot to {args.output}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
