# python/olob/sweep.py
from __future__ import annotations
import argparse, itertools, json, math, os, re, shlex, subprocess, sys, time, hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import yaml  # PyYAML
except ImportError:
    print("[err] PyYAML missing. pip install pyyaml", file=sys.stderr); raise

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    plt = None  # plotting optional

SAFE_FLOAT_FMT = "{:.6g}"

def _slugify(s: str) -> str:
    s = s.replace("/", "-")
    s = re.sub(r"[^A-Za-z0-9._=-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_.")
    return s.lower()[:120]

def _short_hash(obj: Any) -> str:
    j = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.md5(j).hexdigest()[:8]

def _set_by_path(root: dict, dotted: str, value: Any) -> None:
    cur = root
    parts = dotted.split(".")
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

@dataclass(frozen=True)
class RunSpec:
    base_strategy: Path
    overrides: Dict[str, Any]   # dot-path -> value
    seed: int
    quotes: Path
    trades: Path
    out_root: Path
    extra_args: List[str]
    metric: str                 # 'sharpe_like' | 'pnl_over_dd' | 'pnl_total'
    resume: bool

def _format_val(v: Any) -> str:
    if isinstance(v, float):
        return SAFE_FLOAT_FMT.format(v)
    return str(v)

def _build_slug(strategy_path: Path, overrides: Dict[str, Any], seed: int) -> str:
    parts = [strategy_path.stem]
    for k in sorted(overrides.keys()):
        parts.append(f"{_slugify(k)}{_format_val(overrides[k])}")
    parts.append(f"seed{seed}")
    parts.append(_short_hash(overrides))
    return _slugify("-".join(parts))

def _write_strategy_yaml(base_strategy: Path, overrides: Dict[str, Any], out_file: Path) -> None:
    with base_strategy.open("r") as f:
        base_cfg = yaml.safe_load(f)
    for k, v in overrides.items():
        _set_by_path(base_cfg, k, v)
    with out_file.open("w") as f:
        yaml.safe_dump(base_cfg, f, sort_keys=False)

def _completed_sentinel(run_dir: Path) -> Path:
    return run_dir / "_SUCCESS"

def _already_done(run_dir: Path) -> bool:
    return _completed_sentinel(run_dir).exists()

def _touch_success(run_dir: Path) -> None:
    _completed_sentinel(run_dir).write_text("ok\n", encoding="utf-8")

def _run_backtest(run_dir: Path, strategy_yaml: Path, quotes: Path, trades: Path, seed: int, extra_args: List[str]) -> None:
    cmd = [
        sys.executable, "-m", "olob.cli", "backtest",
        "--strategy", str(strategy_yaml),
        "--quotes", str(quotes),
        "--trades", str(trades),
        "--out", str(run_dir),
        "--seed", str(seed),
    ] + list(extra_args)

    stdout = (run_dir / "stdout.log").open("wb")
    stderr = (run_dir / "stderr.log").open("wb")
    try:
        subprocess.run(cmd, stdout=stdout, stderr=stderr, check=True)
    finally:
        stdout.close(); stderr.close()

def _find_json(run_dir: Path, pattern: str) -> Path | None:
    matches = list(run_dir.glob(pattern))
    return matches[0] if matches else None

def _collect_metrics(run_dir: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {"run_dir": str(run_dir)}
    risk = run_dir / "risk_summary.json"
    if risk.exists():
        try:
            out.update({f"risk.{k}": v for k, v in json.loads(risk.read_text()).items()})
        except Exception:
            out["risk.load_error"] = True

    summ = _find_json(run_dir, "*_summary.json")
    if summ:
        try:
            out.update({f"summ.{k}": v for k, v in json.loads(summ.read_text()).items()})
        except Exception:
            out["summ.load_error"] = True

    # Canonical fields for ranking
    out["pnl_total"]   = out.get("risk.pnl_total")
    out["max_drawdown"]= out.get("risk.max_drawdown")
    out["sharpe_like"] = out.get("risk.sharpe_like")
    out["filled_qty"]  = out.get("summ.filled_qty")
    out["avg_px"]      = out.get("summ.avg_px")
    out["fees"]        = out.get("summ.fees")
    return out

def _score(row: Dict[str, Any], metric: str) -> float:
    if metric == "sharpe_like":
        return float(row.get("sharpe_like") or float("-inf"))
    elif metric == "pnl_over_dd":
        pnl = float(row.get("pnl_total") or 0.0)
        dd  = abs(float(row.get("max_drawdown") or 0.0))
        denom = 1.0 + dd
        return pnl / denom
    elif metric == "pnl_total":
        return float(row.get("pnl_total") or float("-inf"))
    else:
        return float(row.get("sharpe_like") or float("-inf"))

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default

def _row_params_parent_qty(row: Dict[str, Any]) -> float | None:
    p = row.get("params")
    if isinstance(p, dict) and "parent_qty" in p:
        try:
            return float(p["parent_qty"])
        except Exception:
            return None
    return None

def _run_one(spec: RunSpec, overrides: Dict[str, Any]) -> Dict[str, Any]:
    slug = _build_slug(spec.base_strategy, overrides, spec.seed)
    run_dir = spec.out_root / slug
    _ensure_dir(run_dir)

    if spec.resume and _already_done(run_dir):
        row = _collect_metrics(run_dir)
        row.update({"slug": slug, "seed": spec.seed, "params": overrides})
        row["score"] = _score(row, spec.metric)
        row["status"] = "skipped_resume"
        return row

    lock = run_dir / ".lock"
    try:
        fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        # Another process is running this slug; wait for success
        for _ in range(180):
            if _already_done(run_dir):
                row = _collect_metrics(run_dir)
                row.update({"slug": slug, "seed": spec.seed, "params": overrides})
                row["score"] = _score(row, spec.metric)
                row["status"] = "joined"
                return row
            time.sleep(1.0)

    strategy_yaml = run_dir / "strategy.yaml"
    _write_strategy_yaml(spec.base_strategy, overrides, strategy_yaml)

    try:
        _run_backtest(run_dir, strategy_yaml, spec.quotes, spec.trades, spec.seed, spec.extra_args)
        _touch_success(run_dir)
        status = "ok"
    except subprocess.CalledProcessError:
        status = "failed"
        # Save last 80 lines of stderr for quick triage
        try:
            errf = run_dir / "stderr.log"
            tail = errf.read_text(encoding="utf-8", errors="replace").splitlines()[-80:]
            (run_dir / "stderr_tail.txt").write_text("\n".join(tail), encoding="utf-8")
        except Exception:
            pass

    row = _collect_metrics(run_dir)
    row.update({"slug": slug, "seed": spec.seed, "params": overrides})
    row["score"] = _score(row, spec.metric)
    row["status"] = status
    return row

def run_sweep(
    base_strategy: Path,
    quotes: Path,
    trades: Path,
    out_root: Path,
    grid: Dict[str, List[Any]],
    seeds: List[int],
    max_procs: int,
    metric: str,
    topk: int,
    resume: bool,
    extra_args: List[str],
    require_full_fill: bool = False,
    min_fill_ratio: float = 1.0,
) -> Tuple[Path, Path]:
    _ensure_dir(out_root)
    keys = sorted(grid.keys())
    values = [grid[k] for k in keys]
    combos = list(itertools.product(*values))

    template_spec = RunSpec(
        base_strategy=base_strategy.resolve(),
        overrides={},
        seed=0,
        quotes=quotes.resolve(),
        trades=trades.resolve(),
        out_root=out_root.resolve(),
        extra_args=extra_args,
        metric=metric,
        resume=resume,
    )

    rows: List[Dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max_procs) as ex:
        futs = []
        for comb in combos:
            ov = {k: v for k, v in zip(keys, comb)}
            for s in seeds:
                spec = template_spec.__class__(**{**template_spec.__dict__, "overrides": ov, "seed": s})
                futs.append(ex.submit(_run_one, spec, ov))

        for i, fut in enumerate(as_completed(futs), 1):
            row = fut.result()
            rows.append(row)
            print(f"[{i}/{len(futs)}] {row.get('status','?'):>8} {row['slug']} score={row['score']!r}")

    # Aggregate outputs
    import csv
    agg_csv = out_root / "aggregate.csv"
    agg_json = out_root / "aggregate.json"
    fieldnames = [
        "slug","seed","score","pnl_total","max_drawdown","sharpe_like",
        "filled_qty","avg_px","fees","run_dir","status","params"
    ]
    with agg_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: (json.dumps(r[k]) if k=="params" else r.get(k)) for k in fieldnames})
    agg_json.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")

    # Filter + tie-break sort for ranking view
    def _is_valid(r: Dict[str, Any]) -> bool:
        sc = r.get("score")
        if sc in (None, float("inf"), float("-inf")):
            return False
        return math.isfinite(float(sc))

    def _is_full(r: Dict[str, Any]) -> bool:
        if not require_full_fill:
            return True
        fq = _safe_float(r.get("filled_qty"))
        pq = _row_params_parent_qty(r)
        return (pq is None) or (fq >= min_fill_ratio * float(pq))

    clean = [r for r in rows if _is_valid(r) and _is_full(r)]

    def _abs_dd(r: Dict[str, Any]) -> float:
        return abs(_safe_float(r.get("max_drawdown")))

    rows_sorted = sorted(
        clean,
        key=lambda r: (
            _safe_float(r.get("score"), -1e300),
            _safe_float(r.get("filled_qty"), 0.0),
            -_safe_float(r.get("fees"), 0.0),
            -_abs_dd(r),
        ),
        reverse=True,
    )

    top = rows_sorted[:topk]

    best_json = out_root / "best.json"
    if top:
        best_json.write_text(json.dumps(top[0], indent=2, sort_keys=True), encoding="utf-8")

    plot_path = out_root / "ranking.png"
    if plt and top:
        labels = [r["slug"] for r in top]
        scores = [float(r["score"]) for r in top]
        fig = plt.figure(figsize=(max(8, 0.55*len(top)), 5))
        plt.bar(range(len(top)), scores)
        plt.xticks(range(len(top)), labels, rotation=35, ha="right")
        plt.ylabel(metric)
        plt.title(f"Sweep ranking (top {len(top)})")
        plt.margins(x=0.01)
        plt.tight_layout()
        fig.savefig(plot_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
    else:
        plot_path = Path()

    (out_root / "manifest.json").write_text(json.dumps({
        "base_strategy": str(base_strategy),
        "quotes": str(quotes),
        "trades": str(trades),
        "grid": grid,
        "seeds": seeds,
        "metric": metric,
        "topk": topk,
        "rows": len(rows),
        "ranked_rows": len(rows_sorted),
        "require_full_fill": require_full_fill,
        "min_fill_ratio": min_fill_ratio,
    }, indent=2, sort_keys=True), encoding="utf-8")

    print(f"[ok] Wrote {agg_csv}")
    if plot_path:
        print(f"[ok] Wrote {plot_path}")
    if best_json.exists():
        print(f"[ok] Wrote {best_json}")
    return agg_csv, plot_path

def main(argv: List[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="lob sweep", description="Grid parameter sweep runner")
    ap.add_argument("--grid", required=True, help="YAML file describing grid")
    ap.add_argument("--out", required=False, help="Override out_root in grid YAML")
    ap.add_argument("--max-procs", type=int, default=None, help="Override concurrency")
    ap.add_argument("--metric", choices=["sharpe_like","pnl_over_dd","pnl_total"], default=None)
    ap.add_argument("--topk", type=int, default=None)
    ap.add_argument("--resume", action="store_true", help="Skip runs with _SUCCESS")
    args = ap.parse_args(argv)

    cfg = yaml.safe_load(Path(args.grid).read_text())
    base_strategy = Path(cfg["base_strategy"])
    quotes = Path(cfg["quotes"])
    trades = Path(cfg["trades"])
    out_root = Path(args.out or cfg.get("out_root", "out/sweeps/default"))
    grid = cfg["grid"]
    seeds = cfg.get("seeds", [1])
    max_procs = args.max_procs or int(cfg.get("max_procs", os.cpu_count() or 4))
    metric = args.metric or cfg.get("metric", "sharpe_like")
    topk = args.topk or int(cfg.get("topk", 20))
    resume = bool(args.resume or cfg.get("resume", True))
    extra_args = cfg.get("extra_args", [])
    if isinstance(extra_args, str):
        extra_args = shlex.split(extra_args)

    require_full_fill = bool(cfg.get("require_full_fill", False))
    min_fill_ratio = float(cfg.get("min_fill_ratio", 1.0))

    run_sweep(
        base_strategy=base_strategy,
        quotes=quotes,
        trades=trades,
        out_root=out_root,
        grid=grid,
        seeds=seeds,
        max_procs=max_procs,
        metric=metric,
        topk=topk,
        resume=resume,
        extra_args=extra_args,
        require_full_fill=require_full_fill,
        min_fill_ratio=min_fill_ratio,
    )

if __name__ == "__main__":
    main()
