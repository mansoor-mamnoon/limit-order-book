# python/olob/viz/make_gif.py
from __future__ import annotations
import argparse
from pathlib import Path
import io

import numpy as np
import pandas as pd
import plotly.graph_objects as go

try:
    import imageio.v2 as imageio
except Exception as e:
    raise SystemExit(
        "imageio is required. Install with: pip install imageio\n"
        f"Original import error: {e}"
    )

def load_quotes(quotes_csv: str) -> pd.DataFrame:
    df = pd.read_csv(quotes_csv)

    # Timestamp column detection
    ts_col = None
    for c in ["ts_ns", "time_ns", "timestamp_ns", "ts", "timestamp", "time"]:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise SystemExit("No timestamp column found (expected one of ts_ns/time_ns/ts/timestamp/time).")

    if ts_col.endswith("ns"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ns", utc=True)
    elif ts_col.endswith("us"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="us", utc=True)
    elif ts_col.endswith("ms"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ms", utc=True)
    else:
        df["ts_dt"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    # Try to map bid/ask columns; if missing, we can still plot mid if present
    def first(names):
        for n in names:
            if n in df.columns:
                return n
        return None

    bpx = first(["bid_px", "best_bid", "bpx", "bid"])
    apx = first(["ask_px", "best_ask", "apx", "ask"])

    # Make numeric if present
    for c in [bpx, apx]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Compute mid/spread if possible; else pass through existing 'mid' if present
    if bpx and apx:
        df["mid"] = (df[bpx] + df[apx]) / 2.0
        df["spread"] = (df[apx] - df[bpx])
    else:
        if "mid" not in df.columns:
            df["mid"] = np.nan
        df["mid"] = pd.to_numeric(df["mid"], errors="coerce")
        # Spread might not be available
        if "spread" in df.columns:
            df["spread"] = pd.to_numeric(df["spread"], errors="coerce")
        else:
            df["spread"] = np.nan

    return df.sort_values("ts_dt").reset_index(drop=True)

def figure_for_window(d: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if "mid" in d.columns and d["mid"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["mid"], mode="lines", name="mid"))
    if "spread" in d.columns and d["spread"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["spread"], mode="lines", name="spread", yaxis="y2"))
    fig.update_layout(
        title="Mid (left) & Spread (right)",
        xaxis=dict(title="time (UTC)"),
        yaxis=dict(title="price"),
        yaxis2=dict(title="spread", overlaying="y", side="right"),
        width=900,
        height=400,
        margin=dict(l=30, r=30, t=40, b=30),
    )
    return fig

def main():
    ap = argparse.ArgumentParser(description="Make a GIF from quotes (mid/spread).")
    ap.add_argument("--quotes", required=True, help="Quotes CSV (e.g., taq_quotes.csv)")
    ap.add_argument("--out", default="out/viz.gif", help="Output GIF path (default: out/viz.gif)")
    ap.add_argument("--seconds", type=int, default=60, help="Duration window from start (default: 60s)")
    ap.add_argument("--fps", type=int, default=10, help="Frames per second (default: 10)")
    args = ap.parse_args()

    # Ensure output directory exists and is writable
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = load_quotes(args.quotes)
    if df.empty:
        raise SystemExit("Empty quotes CSV.")

    # Slice to the requested time window
    t0 = df["ts_dt"].min()
    t1 = t0 + pd.to_timedelta(args.seconds, unit="s")
    df = df[(df["ts_dt"] >= t0) & (df["ts_dt"] <= t1)].copy()
    if df.empty:
        raise SystemExit("No data in the requested time window.")

    # Frame sampling
    N = len(df)
    total_frames = max(1, args.seconds * args.fps)
    step = max(1, int(N / total_frames))

    frames = []
    # Check kaleido availability first (plotly static image engine)
    try:
        import kaleido  # noqa: F401
    except Exception as e:
        raise SystemExit(
            "Plotly static image export needs 'kaleido'. "
            "Install with: pip install kaleido\n"
            f"Original error: {e}"
        )

    for i in range(0, N, step):
        d = df.iloc[: i + 1]
        fig = figure_for_window(d)
        # Export to PNG bytes via kaleido (no temp files)
        png_bytes = fig.to_image(format="png", engine="kaleido", scale=2)
        frames.append(imageio.imread(io.BytesIO(png_bytes)))

    # Write GIF
    imageio.mimsave(out_path, frames, fps=args.fps)
    print(f"[ok] wrote {out_path}")

if __name__ == "__main__":
    main()
