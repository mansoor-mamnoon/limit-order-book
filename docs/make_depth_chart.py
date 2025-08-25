from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def main() -> None:
    ap = argparse.ArgumentParser(description="Make a simple best bid/ask chart from normalized Parquet.")
    ap.add_argument("--parquet", required=True, help="Path to events.parquet")
    ap.add_argument("--out", default="docs/depth_chart.png", help="Output PNG path")
    ap.add_argument("--freq", default="1S", help="Resample frequency (e.g., 1S, 500ms)")
    args = ap.parse_args()

    p = Path(args.parquet)
    if not p.exists():
        raise SystemExit(f"Parquet not found: {p}")

    # Load the normalized table: columns expected: ts, side, price, qty, type
    df = pd.read_parquet(p)

    # Ensure timestamp dtype and set as index for resampling
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # We'll approximate best bid/ask from incremental book updates:
    #  - take 'book' rows only
    #  - for bids (side=='B'), best = max(price); for asks (side=='A'), best = min(price)
    #  - resample to a regular time grid and forward-fill the last known best
    book = df[df["type"] == "book"].copy()

    if book.empty:
        raise SystemExit("No 'book' rows found in Parquet; cannot plot best bid/ask.")

    book = book.set_index("ts")

    # Compute best bid & best ask at each event time
    bids = book[book["side"] == "B"].groupby("ts")["price"].max().rename("best_bid")
    asks = book[book["side"] == "A"].groupby("ts")["price"].min().rename("best_ask")

    # Union of event times, then resample to regular frequency
    span_start = book.index.min()
    span_end = book.index.max()

    # Build regular time index
    rng = pd.date_range(span_start, span_end, freq=args.freq, tz="UTC")

    # Reindex and forward-fill
    best_bid = bids.reindex(rng).ffill()
    best_ask = asks.reindex(rng).ffill()

    # Optional: overlay trades as dots (downsampled)
    trades = df[df["type"] == "trade"].copy()
    if not trades.empty:
        if not pd.api.types.is_datetime64_any_dtype(trades["ts"]):
            trades["ts"] = pd.to_datetime(trades["ts"], utc=True)
        trades = trades.set_index("ts").sort_index()
        # Downsample trades to avoid overplotting: last trade per resample bin
        trades_px = trades["price"].resample(args.freq).last()

    # Plot
    plt.figure(figsize=(10, 4))
    plt.plot(best_bid.index, best_bid.values, label="Best Bid")
    plt.plot(best_ask.index, best_ask.values, label="Best Ask")
    if not trades.empty:
        plt.scatter(trades_px.index, trades_px.values, s=6, label="Trades")

    plt.title("BTCUSDT — Approx. Best Bid/Ask (from diffs)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Price")
    plt.legend(loc="best")
    plt.tight_layout()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    print(f"Wrote chart: {out_path}")


if __name__ == "__main__":
    main()