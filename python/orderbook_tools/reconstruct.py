from __future__ import annotations
import os, gzip, json, argparse, glob
from decimal import Decimal
from typing import List, Dict, Any, Tuple, Optional

try:
    import pandas as pd
except Exception:
    pd = None

from .booklib import L2Book

# ---------- IO helpers ----------
def _open_text(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "rt")

def _load_json_file(path: str) -> Any:
    # snapshot files are whole JSON objects
    with _open_text(path) as f:
        return json.load(f)

def _iter_jsonl(path: str):
    with _open_text(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def _iter_diff_objects(path: str):
    """
    Yield dicts from either:
      - JSONL (one object per line), OR
      - whole-file JSON (object or array), possibly nested under keys like
        'data', 'result', 'events', 'updates', 'payload', 'depth', 'list', 'records'.
    """
    yielded = False
    # First try JSONL
    for obj in _iter_jsonl(path):
        yielded = True
        yield obj
    if yielded:
        return

    # Fallback: try whole-file JSON (pretty-printed or array)
    try:
        root = _load_json_file(path)
    except Exception:
        return

    def walk(o):
        if isinstance(o, list):
            for x in o:
                yield from walk(x)
        elif isinstance(o, dict):
            # If it already looks like a diff update, yield it
            if "U" in o and "u" in o:
                yield o
            else:
                # search common containers
                for k in ("data", "result", "events", "updates", "payload", "depth", "list", "records"):
                    if k in o:
                        yield from walk(o[k])

    for x in walk(root):
        yield x


def _iter_jsonl_peek(path: str) -> Optional[Dict[str, Any]]:
    """Return the first successfully-parsed JSON object from a jsonl file, or None."""
    for obj in _iter_jsonl(path):
        return obj
    return None

def _recursive_globs(root: str, patterns: List[str]) -> List[str]:
    out: List[str] = []
    for pat in patterns:
        out += glob.glob(os.path.join(root, "**", pat), recursive=True)
    return sorted(set(out))

def _scan_raw_dir(raw_dir: str, date: str, exchange: str, symbol: str,
                  snap_glob: Optional[str], diff_glob: Optional[str],
                  debug: bool) -> Dict[str, List[str]]:
    root = os.path.join(raw_dir, date, exchange, symbol)
    if not os.path.isdir(root):
        raise FileNotFoundError(f"Raw path not found: {root}")

    default_snap_pats = ["*snapshot*.json*", "*book_snapshot*.json*", "*rest*book*.json*", "*snap*.json*"]
    default_diff_pats = ["*depth*.json*", "*diff*.json*", "*delta*.json*"]

    snap_pats = [snap_glob] if snap_glob else default_snap_pats
    diff_pats = [diff_glob] if diff_glob else default_diff_pats

    snaps = _recursive_globs(root, snap_pats)
    diffs = _recursive_globs(root, diff_pats)

    if debug:
        print(f"[reconstruct] search root: {root}")
        print(f"[reconstruct] snapshot patterns: {snap_pats}")
        print(f"[reconstruct] diff patterns: {diff_pats}")
        print(f"[reconstruct] found snapshots: {len(snaps)}")
        for p in snaps[:10]:
            print("  snap:", os.path.relpath(p, root))
        if len(snaps) > 10: print("  ...")
        print(f"[reconstruct] found diffs: {len(diffs)}")
        for p in diffs[:10]:
            print("  diff:", os.path.relpath(p, root))
        if len(diffs) > 10: print("  ...")

    return {"root": root, "snapshots": snaps, "diffs": diffs}

# ---------- parsers ----------
def _unwrap(obj: Any) -> Any:
    # handle wrappers like {"data": {...}} or {"result": {...}} or {"stream": ..., "data": {...}}
    if isinstance(obj, dict):
        if "data" in obj and isinstance(obj["data"], dict):
            return _unwrap(obj["data"])
        if "result" in obj and isinstance(obj["result"], dict):
            return _unwrap(obj["result"])
        # already unwrapped
        return obj
    return obj

def _parse_snapshot_any(obj: Any) -> Tuple[int, List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
    o = _unwrap(obj)
    # accept {"lastUpdateId":..., "bids":[["p","q"],...], "asks":[...]}
    last_id = int(o["lastUpdateId"])
    bids_raw = o.get("bids", [])
    asks_raw = o.get("asks", [])
    bids = [(Decimal(px), Decimal(q)) for px, q in bids_raw]
    asks = [(Decimal(px), Decimal(q)) for px, q in asks_raw]
    return last_id, bids, asks

def _coerce_int(x) -> int:
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        return int(x)
    # floats are unexpected here but be robust
    return int(x)

def _parse_diff_msg(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Accept common Binance spot/futures depth update forms:
      {"e":"depthUpdate","E":169..., "U":123, "u":140, "b":[["p","q"],...], "a":[["p","q"],...]}
    Possibly wrapped as {"stream": "...", "data": {...}} or {"data": {...}}.
    Also handle 'bids'/'asks' instead of 'b'/'a'. Event time 'E' may be missing.
    """
    d = _unwrap(obj)
    if not isinstance(d, dict):
        return None

    # U/u are mandatory to sequence diffs
    if "U" not in d or "u" not in d:
        return None

    try:
        U = _coerce_int(d["U"])
        u = _coerce_int(d["u"])
    except Exception:
        return None

    E = d.get("E", 0)
    try:
        E = _coerce_int(E)
    except Exception:
        E = 0

    # depth arrays
    if "b" in d or "a" in d:
        b_raw = d.get("b", [])
        a_raw = d.get("a", [])
    else:
        # some loggers rename to bids/asks
        b_raw = d.get("bids", [])
        a_raw = d.get("asks", [])

    # Normalize to Decimal tuples
    try:
        b = [(Decimal(px), Decimal(q)) for (px, q) in b_raw]
        a = [(Decimal(px), Decimal(q)) for (px, q) in a_raw]
    except Exception:
        # unexpected structure
        return None

    return {"E": E, "U": U, "u": u, "b": b, "a": a}

# ---------- reconstruction ----------
def _emit_record(book: L2Book, E: int, u: int, expected: int, applied: bool,
                 reason: str, drops: int, gaps: int, resyncs: int, levels: int) -> Dict[str, Any]:
    bb = book.best_bid(); aa = book.best_ask()
    rec: Dict[str, Any] = {
        "event_time_ms": E,
        "seq_u": u,
        "expected_next": expected,
        "applied": applied,
        "reason": reason,
        "drops": drops,
        "gaps": gaps,
        "resyncs": resyncs,
        "crc32": book.checksum(levels=levels),
        "best_bid_ticks": (bb.price_ticks if bb else None),
        "best_bid_qty": (str(bb.qty) if bb else None),
        "best_ask_ticks": (aa.price_ticks if aa else None),
        "best_ask_qty": (str(aa.qty) if aa else None),
    }

    # --- NEW: include full L1–L10 ladders as absolute price/qty arrays ---
    # Uses your L2Book.snapshot_tops(n), which returns prices as Decimal already.
    tops = book.snapshot_tops(levels)
    # Cast to Python floats for compact Parquet and easy downstream use
    rec["bids"] = [[float(p), float(q)] for p, q in tops.get("bids", [])]
    rec["asks"] = [[float(p), float(q)] for p, q in tops.get("asks", [])]

    return rec

def _encode_decimals(d: Dict[str, Any]) -> Dict[str, Any]:
    from decimal import Decimal as D
    return {k: (str(v) if isinstance(v, D) else v) for k, v in d.items()}

def reconstruct(raw_dir: str, date: str, exchange: str, symbol: str,
                out_dir: str, tick_size: float, levels: int = 10,
                snap_glob: Optional[str] = None, diff_glob: Optional[str] = None,
                debug: bool = False, peek: bool = False) -> str:
    scan = _scan_raw_dir(raw_dir, date, exchange, symbol, snap_glob, diff_glob, debug)
    snaps = scan["snapshots"]; diffs_files = scan["diffs"]

    if not snaps:
        raise RuntimeError(
            "No REST snapshot files found.\n"
            "Hint: pass --snap-glob '*YOURPATTERN*.json*' or run with --debug to see a directory listing."
        )
    if not diffs_files:
        raise RuntimeError(
            "No diff/depth files found.\n"
            "Hint: pass --diff-glob '*YOURPATTERN*.json*' or run with --debug to see a directory listing."
        )

    # Optional: peek at the first actual diff object (JSONL or whole JSON)
    if debug and peek:
        first = diffs_files[0]
        sample = None
        for obj in _iter_diff_objects(first):
            sample = obj
            break
        if sample is None:
            print(f"[reconstruct] peek: {os.path.basename(first)} has no diff objects")
        else:
            top = _unwrap(sample)
            print(f"[reconstruct] peek first diff keys: {list(top.keys())}")


    book = L2Book(Decimal(str(tick_size)))

    # Load snapshots
    snapshots: List[Tuple[int, List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]] = []
    for p in snaps:
        try:
            obj = _load_json_file(p)
            snapshots.append(_parse_snapshot_any(obj))
        except Exception as e:
            if debug: print(f"[reconstruct] skip snapshot {p}: {e}")
            continue
    if not snapshots:
        raise RuntimeError("Snapshots were found but none could be parsed (try --debug).")

    first_last_id, b0, a0 = snapshots[0]
    book.apply_snapshot(b0, a0)
    expected = first_last_id + 1

    # Load diffs (from all files), sort by (u,E)
    diffs: List[Dict[str, Any]] = []
    for p in diffs_files:
        for obj in _iter_diff_objects(p):
            m = _parse_diff_msg(obj)
            if m:
                diffs.append(m)
    diffs.sort(key=lambda x: (x["u"], x["E"]))

    if debug:
        print(f"[reconstruct] parsed diffs: {len(diffs)}")

    if not diffs:
        raise RuntimeError(
            "Found diff files, but parsed 0 diff messages.\n"
            "Run again with --peek to print the first line’s keys:\n"
            "  python -m orderbook_tools.reconstruct ... --peek --debug"
        )

    records: List[Dict[str, Any]] = []
    drops = gaps = resyncs = 0
    snap_idx = 0

    for msg in diffs:
        U, u, E = msg["U"], msg["u"], msg["E"]
        if u < expected:
            drops += 1
            continue
        if U > expected:
            gaps += 1
            if snap_idx + 1 < len(snapshots):
                snap_idx += 1
                s_last, sb, sa = snapshots[snap_idx]
                book.apply_snapshot(sb, sa)
                expected = s_last + 1
                resyncs += 1
                if U > expected:
                    records.append(_emit_record(book, E, u, expected, False, "gap_after_resync",
                                                drops, gaps, resyncs, levels))
                    continue
            else:
                records.append(_emit_record(book, E, u, expected, False, "gap_no_snapshot",
                                            drops, gaps, resyncs, levels))
                continue

        # apply
        book.apply_updates(msg["b"], msg["a"])
        expected = u + 1
        records.append(_emit_record(book, E, u, expected, True, "ok",
                                    drops, gaps, resyncs, levels))

    out_root = os.path.join(out_dir, date, exchange, symbol)
    os.makedirs(out_root, exist_ok=True)
    pq_path = os.path.join(out_root, "reconstructed_states.parquet")
    jl_path = os.path.join(out_root, "reconstructed_states.jsonl")

    if pd is not None:
        df = pd.DataFrame.from_records(records)
        try:
            df.to_parquet(pq_path, index=False)
            return pq_path
        except Exception:
            with open(jl_path, "w") as f:
                for r in records:
                    f.write(json.dumps(_encode_decimals(r)) + "\n")
            return jl_path
    else:
        with open(jl_path, "w") as f:
            for r in records:
                f.write(json.dumps(_encode_decimals(r)) + "\n")
        return jl_path

def main():
    ap = argparse.ArgumentParser(description="Deterministic L2 reconstruction (snapshot + diffs).")
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--date", required=True)         # YYYY-MM-DD
    ap.add_argument("--exchange", required=True)     # e.g., binance
    ap.add_argument("--symbol", required=True)       # e.g., BTCUSDT
    ap.add_argument("--out-dir", default="recon")
    ap.add_argument("--tick-size", type=float, default=0.01)
    ap.add_argument("--levels", type=int, default=10)
    ap.add_argument("--snap-glob", default=None, help="Override snapshot glob, e.g. '*depth/snapshot-*.json.gz'")
    ap.add_argument("--diff-glob", default=None, help="Override diff glob, e.g. '*depth/diffs-*.jsonl.gz'")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--peek", action="store_true", help="Print the first diff line’s keys (for debugging format)")
    args = ap.parse_args()

    path = reconstruct(args.raw_dir, args.date, args.exchange, args.symbol,
                       args.out_dir, args.tick_size, args.levels,
                       snap_glob=args.snap_glob, diff_glob=args.diff_glob,
                       debug=args.debug, peek=args.peek)
    print(f"Wrote reconstruction states to: {path}")

if __name__ == "__main__":
    main()
