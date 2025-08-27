# python/olob/crashsnapshot.py
from __future__ import annotations
import os, shutil, subprocess, json
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

def _newest_snapshot_in(dirpath: Path) -> Path | None:
    # Look for a plausible snapshot file written by SnapshotWriter
    cands = []
    for p in dirpath.rglob("*.bin"):
        try:
            sz = p.stat().st_size
        except FileNotFoundError:
            continue
        if sz >= 64:  # bigger than a tiny header
            cands.append((p.stat().st_mtime, p))
    if not cands:
        return None
    cands.sort(reverse=True)
    return cands[0][1]

def run_replay_with_snapshot(
    events_csv: str,
    cut_ns: int,
    out_dir: str,
    cadence_ms: int = 50,
    speed: str = "50x",
) -> Dict[str, str]:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    snap = out / "snapshot.bin"
    replay = _find_replay_tool()

    # Pass A: full file, dump snapshot at cut
    quotes_A = out / "quotes_A.csv"
    trades_A = out / "trades_A.bin"
    cmd1 = [
        replay,
        "--file", str(events_csv),
        "--speed", speed,
        "--cadence-ms", str(cadence_ms),
        "--quotes-out", str(quotes_A),
        "--trades-out", str(trades_A),
        "--snapshot-at-ns", str(int(cut_ns)),
        "--snapshot-out", str(snap),
    ]
    subprocess.check_call(cmd1)

    # Tail & Pass B unchanged ...
    df = pd.read_csv(events_csv)
    tcol = next((c for c in ["ts_ns","time_ns","timestamp_ns"] if c in df.columns), None)
    if not tcol:
        tcol = next((c for c in ["ts","timestamp","time"] if c in df.columns), None)
        if not tcol:
            raise SystemExit("events CSV needs a timestamp column")
        t = pd.to_datetime(df[tcol], utc=True, errors="coerce").view("int64")
        df["ts_ns"] = t; tcol = "ts_ns"

    df_tail = df[df[tcol] >= int(cut_ns)].copy()
    tail_csv = out / "events_tail.csv"
    df_tail.to_csv(tail_csv, index=False)

    quotes_B = out / "quotes_B.csv"
    trades_B = out / "trades_B.bin"
    cmd2 = [
        replay,
        "--file", str(tail_csv),
        "--speed", speed,
        "--cadence-ms", str(cadence_ms),
        "--quotes-out", str(quotes_B),
        "--trades-out", str(trades_B),
        "--snapshot-in", str(snap),
    ]
    subprocess.check_call(cmd2)

    return {
        "snap": str(snap),
        "quotes_A": str(quotes_A),
        "quotes_B": str(quotes_B),
        "trades_A": str(trades_A),
        "trades_B": str(trades_B),
        "tail_events": str(tail_csv),
    }

def _find_replay_tool() -> str:
    # 1) PATH
    exe = shutil.which("replay_tool")
    if exe:
        return exe
    here = Path(__file__).resolve()
    repo = here.parents[3] if (len(here.parents) >= 4 and (here.parents[3] / "cpp").exists()) else here.parents[2]
    candidates = [
        repo / "build" / "cpp" / "tools" / "replay_tool",   # <- correct cmake out
        repo / "build" / "cpp" / "replay_tool",             # legacy
        repo / "cpp" / "build" / "tools" / "replay_tool",
        repo / "cpp" / "tools" / "replay_tool",             # local debug run
    ]
    # also look inside scikit-build folders if present
    for p in (repo / "_skbuild").rglob("replay_tool"):
        candidates.append(p)
    for c in candidates:
        if c.exists() and os.access(c, os.X_OK):
            return str(c)
    raise SystemExit("replay_tool not found. Build it via: cmake -S cpp -B build/cpp && cmake --build build/cpp -j")

# ---- Optional backtest equivalence (A vs B) ---------------------------------
def prove_equivalence(
    strategy_yaml: str,
    quotes_full: str,
    quotes_resume: str,
    out_dir: str,
    trades_full: str | None = None,
    trades_resume: str | None = None,
    seed: int = 123,
) -> dict:
    """
    Runs backtests on (A full) and (B resume) and asserts identical fills/econ.
    If quotes files are missing (e.g., replay_tool didn't emit L1 yet), this
    raises a clear error so you can run snapshot-proof without --strategy.
    """
    from pathlib import Path
    import pandas as pd

    qA = Path(quotes_full)
    qB = Path(quotes_resume)
    if not qA.exists() or not qB.exists() or qA.stat().st_size == 0 or qB.stat().st_size == 0:
        raise SystemExit(
            "[snapshot-proof] quotes CSVs not found or empty. Your current replay_tool "
            "build ignores --quotes-out. Re-run without --strategy, or wire L1 output "
            "in replay_tool before using equivalence."
        )

    from olob.backtest import run_backtest as _run_backtest

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # A (full)
    outA = out / "A"
    outA.mkdir(exist_ok=True)
    a_sum = _run_backtest(
        strategy_yaml=strategy_yaml,
        quotes_csv=str(qA),
        trades_csv=str(trades_full) if trades_full else None,
        out_dir=str(outA),
        seed=int(seed),
    )
    fills_A = Path(a_sum["fills_csv"])

    # B (resume)
    outB = out / "B"
    outB.mkdir(exist_ok=True)
    b_sum = _run_backtest(
        strategy_yaml=strategy_yaml,
        quotes_csv=str(qB),
        trades_csv=str(trades_resume) if trades_resume else None,
        out_dir=str(outB),
        seed=int(seed),
    )
    fills_B = Path(b_sum["fills_csv"])

    # Prefer a repo helper if you have one; else strict equality
    try:
        from olob.crashcheck import compare_fills  # optional helper
        ok, msg = compare_fills(str(fills_A), str(fills_B))
    except Exception:
        ca = pd.read_csv(fills_A)
        cb = pd.read_csv(fills_B)
        cols = [c for c in ca.columns if c in cb.columns]
        ok = ca[cols].equals(cb[cols])
        msg = "strict equality on shared columns" if ok else "fills differ"

    res = {
        "fills_A": str(fills_A),
        "fills_B": str(fills_B),
        "ok": bool(ok),
        "message": msg,
    }
    (out / "equivalence.json").write_text(json.dumps(res, indent=2))
    if not ok:
        raise SystemExit(f"[FAIL] Snapshot equivalence: {msg}")
    return res