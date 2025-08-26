# app.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, Dict

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ---------------------------
# Auto-detection helpers
# ---------------------------

def _find_default_pnl() -> Optional[Path]:
    best = Path("out/sweeps/acceptance/best.json")
    if best.exists():
        try:
            b = json.loads(best.read_text())
            run_dir = Path(b["run_dir"])
            cand = run_dir / "pnl_timeseries.csv"
            if cand.exists():
                return cand
        except Exception:
            pass
    for p in Path("out").rglob("pnl_timeseries.csv"):
        return p
    return None

def _infer_quotes_path() -> Optional[Path]:
    for cand in [Path("taq_quotes.csv"), Path("out/tmp_report/taq_quotes.csv")]:
        if cand.exists():
            return cand
    return None

def _infer_trades_path() -> Optional[Path]:
    for cand in [Path("taq_trades.csv"), Path("out/tmp_report/taq_trades.csv")]:
        if cand.exists():
            return cand
    return None

def _infer_depth_path() -> Optional[Path]:
    # common output from reconstruction
    for p in Path(".").rglob("top10_depth.parquet"):
        return p
    return None

def _to_utc(ts: pd.Timestamp | str) -> pd.Timestamp:
    """Return a timezone-aware UTC pandas.Timestamp for comparisons/plotting."""
    t = pd.to_datetime(ts, errors="coerce")
    if t.tzinfo is None or getattr(t, "tz", None) is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")

# ---------------------------
# Data loaders (cached)
# ---------------------------

@st.cache_data(show_spinner=False)
def load_quotes(quotes_csv: str, map_cols: Dict[str, str], ffill_l1: bool, resample_ms: int) -> pd.DataFrame:
    df = pd.read_csv(quotes_csv)

    # --- timestamp detection ---
    ts_col = None
    for c in ["ts_ns", "time_ns", "timestamp_ns", "ts", "timestamp", "time"]:
        if c in df.columns:
            ts_col = c; break
    if ts_col is None:
        raise ValueError("Quotes CSV must contain a timestamp column like ts_ns/ts/time.")
    if ts_col.endswith("ns"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ns", utc=True)
    elif ts_col.endswith("us"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="us", utc=True)
    elif ts_col.endswith("ms"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ms", utc=True)
    else:
        df["ts_dt"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    # --- map or guess columns ---
    def map_or_guess(key, guesses):
        col = (map_cols.get(key) or "").strip()
        if col and col in df.columns: return col
        for n in guesses:
            if n in df.columns: return n
        return None

    bpx = map_or_guess("bid_px", ["bid_px","best_bid","bpx","bid"])
    apx = map_or_guess("ask_px", ["ask_px","best_ask","apx","ask"])
    bsz = map_or_guess("bid_sz", ["bid_sz","bsz","bid_size","bid_qty","bid_vol"])
    asz = map_or_guess("ask_sz", ["ask_sz","asz","ask_size","ask_qty","ask_vol"])

    # make numeric where present
    for c in [bpx, apx, bsz, asz]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- optional resample to regular time grid for smoother lines ---
    df = df.sort_values("ts_dt")
    if resample_ms and resample_ms > 0:
        # keep only the columns we know + anything already provided like 'mid'
        keep = ["ts_dt"] + [c for c in [bpx, apx, bsz, asz, "mid", "spread", "microprice"] if c]
        d = df[keep].set_index("ts_dt")
        # last observation carried forward for L1 fields (prices/sizes), mean for already-provided metrics
        how = "last"
        d = d.resample(f"{resample_ms}ms").apply(how)
        d = d.reset_index()
        df = d

    # --- forward fill L1 if asked (fixes early None/NaN) ---
    if ffill_l1:
        fill_cols = [c for c in [bpx, apx, bsz, asz] if c]
        if fill_cols:
            df[fill_cols] = df[fill_cols].ffill()

    # --- compute mid/spread/microprice AFTER filling ---
    if bpx and apx:
        df["mid"] = (df[bpx] + df[apx]) / 2.0
        df["spread"] = df[apx] - df[bpx]
    elif "mid" not in df.columns:
        df["mid"] = np.nan; df["spread"] = np.nan

    if bpx and apx and bsz and asz:
        denom = (df[bsz] + df[asz]).replace(0.0, np.nan)
        df["microprice"] = (df[bpx] * df[asz] + df[apx] * df[bsz]) / denom
    elif "microprice" not in df.columns:
        df["microprice"] = np.nan

    return df.reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_pnl(pnl_csv: str, map_cols: Dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(pnl_csv)

    ts_col = None
    for c in ["ts_ns", "time_ns", "ts", "timestamp", "time"]:
        if c in df.columns:
            ts_col = c; break
    if ts_col is None:
        raise ValueError("PnL CSV must contain a timestamp column like ts_ns/ts/time.")

    if ts_col.endswith("ns"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ns", utc=True)
    elif ts_col.endswith("us"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="us", utc=True)
    elif ts_col.endswith("ms"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ms", utc=True)
    else:
        df["ts_dt"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    def pick(name, guesses):
        col = (map_cols.get(name) or "").strip()
        if col and col in df.columns:
            return col
        for g in guesses:
            if g in df.columns:
                return g
        return None

    eq = pick("equity", ["equity","equity_value","eq"])
    ca = pick("cash", ["cash","cash_value"])
    inv = pick("inventory", ["inventory","position","pos","qty","size"])

    for (alias, col) in [("equity", eq), ("cash", ca), ("inventory", inv)]:
        if col and col in df.columns:
            df[alias] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[alias] = np.nan

    return df.sort_values("ts_dt").reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_depth_top10(parquet_path: str) -> pd.DataFrame:
    """
    Accepts tidy:
      ts_ns/ts, side ('B'/'A'), level, price, qty
    Or wide common format with:
      bid_px_1..10, bid_sz_1..10, ask_px_1..10, ask_sz_1..10 (+ ts/_ns).
    Returns tidy sorted by time.
    """
    df = pd.read_parquet(parquet_path)

    # time column
    ts_col = None
    for c in ["ts_ns", "ts", "time_ns", "timestamp"]:
        if c in df.columns:
            ts_col = c; break
    if ts_col is None:
        raise ValueError("Depth parquet must contain ts/ts_ns/time.")
    if str(ts_col).endswith("ns"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ns", utc=True)
    else:
        df["ts_dt"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    # already tidy?
    if set(["side","level","price","qty"]).issubset(df.columns):
        tidy = df[["ts_dt","side","level","price","qty"]].copy()
        return tidy.sort_values(["ts_dt","side","level"]).reset_index(drop=True)

    # try melt from wide
    bid_px_cols = [c for c in df.columns if c.startswith("bid_px_")]
    ask_px_cols = [c for c in df.columns if c.startswith("ask_px_")]
    bid_sz_cols = [c for c in df.columns if c.startswith("bid_sz_")]
    ask_sz_cols = [c for c in df.columns if c.startswith("ask_sz_")]

    if bid_px_cols and ask_px_cols and bid_sz_cols and ask_sz_cols:
        def _to_long(prefix_px, prefix_sz, side_label):
            px = df.melt(id_vars=["ts_dt"], value_vars=[c for c in df.columns if c.startswith(prefix_px)],
                         var_name="col", value_name="price")
            sz = df.melt(id_vars=["ts_dt"], value_vars=[c for c in df.columns if c.startswith(prefix_sz)],
                         var_name="col", value_name="qty")
            px["level"] = px["col"].str.replace(prefix_px, "", regex=False).astype(int)
            sz["level"] = sz["col"].str.replace(prefix_sz, "", regex=False).astype(int)
            m = px.merge(sz[["ts_dt","level","qty"]], on=["ts_dt","level"], how="left")
            m["side"] = side_label
            return m[["ts_dt","side","level","price","qty"]]

        bids = _to_long("bid_px_", "bid_sz_", "B")
        asks = _to_long("ask_px_", "ask_sz_", "A")
        tidy = pd.concat([bids, asks], ignore_index=True)
        # coerce numeric
        tidy["price"] = pd.to_numeric(tidy["price"], errors="coerce")
        tidy["qty"] = pd.to_numeric(tidy["qty"], errors="coerce")
        tidy = tidy.dropna(subset=["price","qty"])
        return tidy.sort_values(["ts_dt","side","level"]).reset_index(drop=True)

    # If we reach here, we don't know the format—return empty
    return pd.DataFrame(columns=["ts_dt","side","level","price","qty"])

# ---------------------------
# Plot builders
# ---------------------------

def figure_mid_micro(df: pd.DataFrame, upto=None) -> go.Figure:
    if upto is not None:
        upto = _to_utc(upto)
    d = df if upto is None else df[df["ts_dt"] <= upto]
    fig = go.Figure()
    if "mid" in d.columns and d["mid"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["mid"], mode="lines", name="mid"))
    if "microprice" in d.columns and d["microprice"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["microprice"], mode="lines", name="microprice"))
    fig.update_layout(title="Mid & Microprice", xaxis_title="time (UTC)", yaxis_title="price",
                      height=320, margin=dict(l=10,r=10,t=35,b=10))
    return fig

def figure_spread(df: pd.DataFrame, upto=None) -> go.Figure:
    if upto is not None:
        upto = _to_utc(upto)
    d = df if upto is None else df[df["ts_dt"] <= upto]
    if "spread" in d.columns and d["spread"].notna().any():
        fig = px.area(d, x="ts_dt", y="spread", title="Spread", height=220)
    else:
        fig = go.Figure()
        fig.update_layout(title="Spread (no data)", height=220)
    fig.update_layout(margin=dict(l=10,r=10,t=35,b=10))
    return fig

def figure_pnl(df: pd.DataFrame, upto=None) -> go.Figure:
    if upto is not None:
        upto = _to_utc(upto)
    d = df if upto is None else df[df["ts_dt"] <= upto]
    fig = go.Figure()
    if "equity" in d.columns and d["equity"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["equity"], mode="lines", name="equity"))
    if "cash" in d.columns and d["cash"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["cash"], mode="lines", name="cash"))
    if "inventory" in d.columns and d["inventory"].notna().any():
        fig.add_trace(go.Scatter(x=d["ts_dt"], y=d["inventory"], mode="lines", name="inventory", yaxis="y2"))
    fig.update_layout(
        title="PnL / Inventory",
        xaxis=dict(title="time (UTC)"),
        yaxis=dict(title="notional"),
        yaxis2=dict(title="inventory", overlaying="y", side="right", showgrid=False),
        height=340, margin=dict(l=10,r=10,t=35,b=10)
    )
    return fig

def figure_depth_heatmap(depth_df: pd.DataFrame, start=None, end=None, side="both",
                         price_bins=120) -> Optional[go.Figure]:
    if depth_df is None or depth_df.empty: return None
    d = depth_df.copy()
    if start is not None and end is not None:
        d = d[(d["ts_dt"] >= start) & (d["ts_dt"] <= end)]
    if d.empty or "qty" not in d.columns or "price" not in d.columns: return None
    if side != "both" and "side" in d.columns:
        d = d[d["side"].astype(str).str.upper().isin([side[0].upper()])]
    if d.empty: return None

    # Bin time coarsely for speed
    d["time_bin"] = pd.to_datetime(d["ts_dt"].dt.floor("200ms"))
    # Clip price range to robust percentiles
    try:
        pmin, pmax = np.nanpercentile(d["price"], [1, 99])
        d = d[(d["price"] >= pmin) & (d["price"] <= pmax)]
    except Exception:
        pass
    d["price_bin"] = pd.cut(d["price"], bins=price_bins)

    heat = d.groupby(["time_bin", "price_bin"])["qty"].sum().reset_index()
    if heat.empty: return None
    heat["price_mid"] = heat["price_bin"].apply(lambda x: (x.left + x.right) / 2 if pd.notnull(x) else np.nan)
    pivot = heat.pivot(index="price_mid", columns="time_bin", values="qty").sort_index(ascending=False)
    fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns, y=pivot.index, coloraxis="coloraxis"))
    fig.update_layout(
        title="L2 Depth Heatmap (qty)",
        xaxis_title="time (UTC)",
        yaxis_title="price",
        height=520,
        coloraxis=dict(colorscale="Turbo"),
        margin=dict(l=10,r=10,t=35,b=10),
    )
    return fig

# ---------------------------
# Streamlit UI
# ---------------------------

st.set_page_config(page_title="LOB Live Viz", layout="wide", initial_sidebar_state="expanded")
st.title("Minimal Live Viz")
st.caption("Streamlit: mid/microprice, spread, PnL/inventory, and optional L2 heatmap. Controls: play/pause, 1×/10×/100×.")

# Defaults
default_quotes = _infer_quotes_path()
default_pnl = _find_default_pnl()
default_depth = _infer_depth_path()

# Sidebar: file paths
with st.sidebar:
    st.header("Inputs")
    quotes_path = st.text_input("Quotes CSV", value=str(default_quotes) if default_quotes else "")
    pnl_path = st.text_input("PnL timeseries CSV", value=str(default_pnl) if default_pnl else "")
    depth_path = st.text_input("Depth parquet (optional)", value=str(default_depth) if default_depth else "")

# Sidebar: column mappings (persisted in session)
def _init_mapping_state():
    for k in ["map_bid_px","map_ask_px","map_bid_sz","map_ask_sz","map_equity","map_cash","map_inventory"]:
        st.session_state.setdefault(k, "")

_init_mapping_state()

with st.sidebar:
    st.divider()
    st.header("Column mapping (quotes)")
    map_bid_px = st.text_input("bid_px (optional)", value=st.session_state["map_bid_px"])
    map_ask_px = st.text_input("ask_px (optional)", value=st.session_state["map_ask_px"])
    map_bid_sz = st.text_input("bid_sz (optional)", value=st.session_state["map_bid_sz"])
    map_ask_sz = st.text_input("ask_sz (optional)", value=st.session_state["map_ask_sz"])

    st.header("Column mapping (PnL)")
    map_equity = st.text_input("equity (optional)", value=st.session_state["map_equity"])
    map_cash = st.text_input("cash (optional)", value=st.session_state["map_cash"])
    map_inventory = st.text_input("inventory (optional)", value=st.session_state["map_inventory"])

    load_clicked = st.button("Load / Reload", type="primary")

    st.divider()
    st.header("Playback")
    speed = st.radio("Speed", options=[1, 10, 100], index=1, horizontal=True)
    window_sec = st.slider("Visible window (sec)", min_value=10, max_value=300, value=60, step=10)
    step_points = st.slider("Step per refresh (points)", min_value=1, max_value=100, value=10, step=1)

    if "playing" not in st.session_state:
        st.session_state.playing = False
    if st.button("▶ Play / ⏸ Pause"):
        st.session_state.playing = not st.session_state.playing

    st.divider()
    st.header("Smoothing")
    if "ffill_l1" not in st.session_state:
        st.session_state.ffill_l1 = True
    ffill_l1 = st.checkbox("Forward-fill best bid/ask", value=st.session_state.ffill_l1)
    resample_ms = st.selectbox("Resample (ms)", [0, 50, 100, 200], index=2, help="0 = no resample")

# Prepare mapping dicts
qmap = {"bid_px": map_bid_px, "ask_px": map_ask_px, "bid_sz": map_bid_sz, "ask_sz": map_ask_sz}
pmap = {"equity": map_equity, "cash": map_cash, "inventory": map_inventory}

# Load data on first run or when clicked
if load_clicked or ("_loaded_once" not in st.session_state):
    try:
        qdf = load_quotes(quotes_path, qmap, ffill_l1=ffill_l1, resample_ms=int(resample_ms)) if quotes_path else pd.DataFrame()
        pdf = load_pnl(pnl_path, pmap) if pnl_path else pd.DataFrame()
        ddf = load_depth_top10(depth_path) if depth_path else pd.DataFrame()
        st.session_state.qdf = qdf
        st.session_state.pdf = pdf
        st.session_state.ddf = ddf
        st.session_state._loaded_once = True
        st.session_state.idx = 0
        # Persist mapping back to session
        st.session_state["map_bid_px"] = map_bid_px
        st.session_state["map_ask_px"] = map_ask_px
        st.session_state["map_bid_sz"] = map_bid_sz
        st.session_state["map_ask_sz"] = map_ask_sz
        st.session_state["map_equity"] = map_equity
        st.session_state["map_cash"] = map_cash
        st.session_state["map_inventory"] = map_inventory
        st.success("Loaded data.")
    except Exception as e:
        st.error(f"Failed to load: {e}")
        st.stop()

qdf = st.session_state.get("qdf", pd.DataFrame())
pdf = st.session_state.get("pdf", pd.DataFrame())
ddf = st.session_state.get("ddf", pd.DataFrame())

# Quick previews to help mapping
with st.expander("Preview (first 5 rows)"):
    st.write("Quotes")
    st.dataframe(qdf.head() if not qdf.empty else pd.DataFrame({"info":["No quotes loaded"]}))
    st.write("PnL")
    st.dataframe(pdf.head() if not pdf.empty else pd.DataFrame({"info":["No PnL loaded"]}))

if qdf.empty:
    st.warning("Provide a valid Quotes CSV (and click Load / Reload).")
    st.stop()

# Timeline controls
t0 = qdf["ts_dt"].min()
t1 = qdf["ts_dt"].max()
tick_times = qdf["ts_dt"].values

# Time cursor
topbar = st.container()
with topbar:
    col1, col2 = st.columns([5,1])
    with col1:
        idx = st.slider("Time cursor", min_value=0, max_value=len(tick_times)-1,
                        value=int(st.session_state.get("idx", 0)))
    with col2:
        st.write("")
        cur_ts = pd.to_datetime(tick_times[idx]).strftime('%H:%M:%S.%f')[:-3]
        st.write(f"**{cur_ts} UTC**")

# Keep the latest slider value
st.session_state.idx = int(idx)

# ----- Autoplay (play/pause) -----
# How many points to advance per refresh?
advance_per_refresh = max(1, int(step_points)) * (speed if speed in (1, 10, 100) else 1)

if st.session_state.playing:
    # advance, clamp to the end
    st.session_state.idx = min(len(tick_times) - 1, st.session_state.idx + advance_per_refresh)

    # If we hit the end, stop playing so it doesn't spam reruns
    if st.session_state.idx >= len(tick_times) - 1:
        st.session_state.playing = False
    else:
        # Streamlit ≥1.30 uses st.rerun(); fall back for older versions
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

# Current time (UTC-aware)
cur_time = _to_utc(tick_times[st.session_state.idx])
win_start = cur_time - pd.to_timedelta(window_sec, unit="s")

# Charts
top1, top2 = st.columns([3,2])
with top1:
    st.plotly_chart(figure_mid_micro(qdf, upto=cur_time), use_container_width=True)
with top2:
    st.plotly_chart(figure_spread(qdf, upto=cur_time), use_container_width=True)

if not pdf.empty:
    st.plotly_chart(figure_pnl(pdf, upto=cur_time), use_container_width=True)
else:
    st.info("PnL CSV not provided — set path and Load to see PnL & inventory.")

# Depth heatmap (optional)
if not ddf.empty:
    dclip = ddf[(ddf["ts_dt"] >= _to_utc(win_start)) & (ddf["ts_dt"] <= _to_utc(cur_time))]
    fig_hm = figure_depth_heatmap(dclip, start=win_start, end=cur_time, side="both")
    if fig_hm:
        st.plotly_chart(fig_hm, use_container_width=True, theme="streamlit")
else:
    st.caption("Tip: Provide `top10_depth.parquet` for the L2 heatmap (optional).")

st.markdown(
    "<small>Controls: Play/Pause toggles auto-advance. 'Speed' multiplies step size per refresh. "
    "Use 'Visible window' to adjust the heatmap span.</small>",
    unsafe_allow_html=True
)
