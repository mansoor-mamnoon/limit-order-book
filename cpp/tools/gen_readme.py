#!/usr/bin/env python3
import argparse, csv
from pathlib import Path

def read_kvu(csv_path):
    """Read 3-col CSVs like summary.csv -> {key: (value, units)}.
       For 2-col CSVs like environment.csv -> {key: (value, '')}."""
    out = {}
    with open(csv_path, "r", newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)  # skip header
        for row in rdr:
            if not row: 
                continue
            if len(row) >= 3:
                k, v, u = row[0], row[1], row[2]
                out[k] = (v, u)
            elif len(row) == 2:
                k, v = row[0], row[1]
                out[k] = (v, "")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="bench_out", help="Directory with bench CSVs & graphs")
    ap.add_argument("--out", default="bench_out/README.md", help="Output README path")
    args = ap.parse_args()

    d = Path(args.dir)
    sum_csv   = d / "summary.csv"
    env_csv   = d / "environment.csv"
    hist_png  = d / "latency_histogram.png"
    series_png= d / "latencies.png"

    if not sum_csv.exists():
        raise SystemExit(f"Missing {sum_csv}. Run bench_tool first.")
    if not hist_png.exists() or not series_png.exists():
        raise SystemExit(f"Missing graphs in {d}. Run plot_bench.py first.")

    S = read_kvu(sum_csv)
    E = read_kvu(env_csv) if env_csv.exists() else {}

    def gv(k, default="0", default_u=""):
        v,u = S.get(k, (default, default_u))
        return v,u

    events_total, _   = gv("events_total")
    warmup_events, _  = gv("warmup_events")
    events_measured,_ = gv("events_measured")
    p50,u50   = gv("p50")
    p90,u90   = gv("p90")
    p99,u99   = gv("p99")
    p999,u999 = gv("p999")
    thr,uth   = gv("throughput", default_u="events_per_second")

    # Environment (best-effort)
    cpu_model   = E.get("cpu_model", ("unknown",""))[0]
    os_name     = E.get("os", ("unknown",""))[0]
    compiler    = E.get("compiler", ("unknown",""))[0]
    rdtsc_mode  = E.get("rdtsc_mode", ("ns",""))[0]
    pin_core    = E.get("pin_core", ("-",""))[0]
    zipf_s      = E.get("zipf_s", ("-",""))[0]
    zipf_levels = E.get("zipf_levels", ("-",""))[0]
    pareto_a    = E.get("pareto_alpha", ("-",""))[0]
    walk_sigma  = E.get("walk_sigma", ("-",""))[0]
    mkt_ratio   = E.get("market_ratio", ("-",""))[0]
    cnc_ratio   = E.get("cancel_ratio", ("-",""))[0]
    mod_ratio   = E.get("modify_ratio", ("-",""))[0]
    stp_en      = E.get("stp_enabled", ("-",""))[0]

    md = f"""```markdown
# LOB Benchmark Report

## Summary (steady-state)
- Events total: **{events_total}**
- Warm-up ignored: **{warmup_events}**
- Events measured: **{events_measured}**
- p50: **{p50} {u50}**
- p90: **{p90} {u90}**
- p99: **{p99} {u99}**
- p999: **{p999} {u999}**
- Throughput: **{thr} {uth}**

## Environment & Methodology
- CPU: **{cpu_model}**
- OS: **{os_name}** | Compiler: **{compiler}** | Timing units: **{rdtsc_mode}**
- CPU pinning: **{pin_core}**
- Workload knobs: Zipf(s)=**{zipf_s}**, Zipf levels=**{zipf_levels}**, Pareto α=**{pareto_a}**, Walk σ=**{walk_sigma}**
- Ratios: market=**{mkt_ratio}**, cancel=**{cnc_ratio}**, modify=**{mod_ratio}**, STP enabled=**{stp_en}**

## Latency Histogram
![Latency Histogram](./latency_histogram.png)

## Per-Event Latency Over Time
![Per-Event Latency](./latencies.png)
```"""

    Path(args.out).write_text(md)
    print(f"[readme] Wrote {args.out}")

if __name__ == "__main__":
    main()
