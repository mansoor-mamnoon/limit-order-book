#!/usr/bin/env python3
import argparse, csv, math, os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

def read_latencies(latencies_csv):
    xs = []
    with open(latencies_csv, "r", newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)
        # Expect columns: i, latency, (units header string)
        for row in rdr:
            if not row: continue
            try:
                xs.append(float(row[1]))
            except Exception:
                pass
    return xs

def read_summary(summary_csv):
    out = {}
    with open(summary_csv, "r", newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)
        for row in rdr:
            if len(row) >= 3:
                k, v, u = row[0], row[1], row[2]
                out[k] = (v, u)
    return out

def plot_histogram(latencies, out_png):
    # Log2 buckets like the C++ histogram (for visual parity)
    # Build buckets explicitly so axis labels are clear.
    if not latencies:
        raise SystemExit("No latencies found to plot.")
    # Guard against zeros/negatives; latencies may be cycles or ns
    vals = [max(1.0, float(v)) for v in latencies]
    max_v = max(vals)
    max_bucket = int(min(40, math.log2(max_v))) if max_v > 1 else 0

    buckets = [0]*(max_bucket+1)
    for v in vals:
        b = 0 if v <= 1 else min(max_bucket, int(math.log2(v)))
        buckets[b] += 1

    fig = plt.figure(figsize=(9, 5.5))
    plt.bar(range(len(buckets)), buckets)
    plt.xlabel("Latency bucket (log2 units)")
    plt.ylabel("Count")
    plt.title("Latency Histogram (log2 buckets)")
    plt.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)

def plot_series(latencies, out_png, sample_stride=1_000):
    if not latencies:
        raise SystemExit("No latencies found to plot.")
    # Downsample for readability if very large:
    if sample_stride > 1 and len(latencies) > sample_stride:
        xs = list(range(0, len(latencies), sample_stride))
        ys = [latencies[i] for i in xs]
    else:
        xs = list(range(len(latencies)))
        ys = latencies

    fig = plt.figure(figsize=(10, 5.5))
    plt.plot(xs, ys, marker="", linewidth=0.7)
    plt.xlabel("Event index (downsampled)" if len(ys) < len(latencies) else "Event index")
    plt.ylabel("Latency (cycles or ns)")
    plt.title("Per-Event Latency Over Time")
    plt.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="bench_out", help="Directory containing CSVs from bench_tool")
    ap.add_argument("--stride", type=int, default=1000, help="Downsample stride for time-series plot")
    args = ap.parse_args()

    d = Path(args.dir)
    lat_csv = d / "latencies.csv"
    sum_csv = d / "summary.csv"

    if not lat_csv.exists():
        raise SystemExit(f"Missing {lat_csv}. Run bench_tool first.")
    if not sum_csv.exists():
        raise SystemExit(f"Missing {sum_csv}. Run bench_tool first.")

    lats = read_latencies(lat_csv)
    # Make plots
    hist_png = d / "latency_histogram.png"
    series_png = d / "latencies.png"
    plot_histogram(lats, hist_png)
    plot_series(lats, series_png, sample_stride=args.stride)

    print(f"[plot] Wrote {hist_png}")
    print(f"[plot] Wrote {series_png}")

if __name__ == "__main__":
    main()
