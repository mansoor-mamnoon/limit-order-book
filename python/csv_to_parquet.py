#!/usr/bin/env python3
"""
Convert the TAQ CSVs (quotes or trades) to Parquet with a stable schema.

Usage:
  python python/csv_to_parquet.py --in taq_quotes.csv --out taq_quotes.parquet
  python python/csv_to_parquet.py --in taq_trades.csv --out taq_trades.parquet
"""
import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.inp)
    # enforce dtypes
    if "ts_ns" in df.columns:
        df["ts_ns"] = df["ts_ns"].astype("int64")

    # Write Parquet (requires pyarrow)
    df.to_parquet(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows")

if __name__ == "__main__":
    main()
