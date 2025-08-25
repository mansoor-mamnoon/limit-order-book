from __future__ import annotations

import asyncio
import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import ClientResponseError, ClientConnectorError, WSMsgType

# ---------- config helpers ----------

@dataclass
class Endpoints:
    rest_base: str
    ws_base: str  # combined stream endpoint

def _endpoints_for(exchange: str) -> Endpoints:
    ex = exchange.lower()
    if ex == "binanceus":
        # US endpoints
        return Endpoints(
            rest_base="https://api.binance.us",
            ws_base="wss://stream.binance.us:9443/stream",
        )
    elif ex == "binance":
        # Global endpoints
        return Endpoints(
            rest_base="https://api.binance.com",
            ws_base="wss://stream.binance.com/stream",
        )
    else:
        raise ValueError(f"Unknown exchange: {exchange}")

# ---------- filesystem helpers ----------

def _utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _utc_hms() -> str:
    return datetime.now(timezone.utc).strftime("%H%M%S")

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

class JsonlGzWriter:
    def __init__(self, path: Path):
        _ensure_dir(path.parent)
        # Use binary file object with gzip and write bytes
        self._f = gzip.open(str(path), mode="wt", encoding="utf-8")
        self._path = path
        self._count = 0

    def write(self, obj) -> None:
        self._f.write(json.dumps(obj, separators=(",", ":")) + "\n")
        self._count += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        try:
            self._f.close()
        except Exception:
            pass

class JsonGzWriter:
    def __init__(self, path: Path):
        _ensure_dir(path.parent)
        self._f = gzip.open(str(path), mode="wt", encoding="utf-8")
        self._path = path

    def write_obj(self, obj) -> None:
        self._f.write(json.dumps(obj, separators=(",", ":")))

    def close(self) -> None:
        try:
            self._f.close()
        except Exception:
            pass

# ---------- REST snapshot ----------

async def _rest_depth_snapshot(session: aiohttp.ClientSession, rest_base: str, symbol: str) -> dict:
    """
    Fetch full depth (bids/asks + lastUpdateId) for seeding.
    """
    url = f"{rest_base}/api/v3/depth"
    params = {"symbol": symbol.upper(), "limit": 1000}
    try:
        async with session.get(url, params=params, timeout=20) as r:
            r.raise_for_status()
            return await r.json()
    except ClientResponseError as e:
        if e.status == 451 and "binance.com" in url:
            raise RuntimeError(
                "HTTP 451 from api.binance.com (geo-block). "
                "Use --exchange binanceus to switch to US endpoints."
            )
        raise
    except ClientConnectorError as e:
        raise RuntimeError(f"REST connect error: {e}") from e
    
def run_capture(*, symbol: str, minutes: int, raw_root: str, snapshot_every_sec: int, exchange: str) -> None:
    """
    Wrapper so olob.cli can call into this module.
    """
    asyncio.run(_consumer_depth_and_trades(
        symbol=symbol,
        raw_root=raw_root,
        minutes=minutes,
        snapshot_every_sec=snapshot_every_sec,
        exchange=exchange,
    ))

# ---------- WS consumer ----------

async def _consumer_depth_and_trades(
    *,
    symbol: str,
    raw_root: str,
    minutes: int,
    snapshot_every_sec: int,
    exchange: str,
) -> None:
    ep = _endpoints_for(exchange)
    sym = symbol.lower()

    date = _utc_date_str()
    base = Path(raw_root) / date / exchange / symbol.upper()
    depth_dir = base / "depth"
    trades_dir = base / "trades"
    _ensure_dir(depth_dir)
    _ensure_dir(trades_dir)

    depth_path = depth_dir / f"diffs-{_utc_hms()}.jsonl.gz"
    trades_path = trades_dir / f"trades-{_utc_hms()}.jsonl.gz"
    depth_out = JsonlGzWriter(depth_path)
    trades_out = JsonlGzWriter(trades_path)

    streams = f"{sym}@depth@100ms/{sym}@trade"
    ws_url = f"{ep.ws_base}?streams={streams}"

    print(f"[capture] exchange={exchange} symbol={symbol} minutes={minutes}")
    print(f"[capture] REST={ep.rest_base}  WS={ws_url}")
    print(f"[capture] writing diffs -> {depth_out.path}")
    print(f"[capture] writing trades -> {trades_out.path}")

    stop_at = asyncio.get_event_loop().time() + minutes * 60
    last_snapshot = 0.0

    async with aiohttp.ClientSession(raise_for_status=True) as session:
        # initial snapshot immediately
        try:
            snap = await _rest_depth_snapshot(session, ep.rest_base, symbol)
            snap_path = depth_dir / f"snapshot-{_utc_hms()}.json.gz"
            JsonGzWriter(snap_path).write_obj(snap)
            print(f"[capture] wrote snapshot -> {snap_path.name}")
        except Exception as e:
            print(f"[capture][WARN] initial snapshot failed: {e}")

        async with session.ws_connect(ws_url, heartbeat=20) as ws:
            while True:
                # periodic snapshot
                now = asyncio.get_event_loop().time()
                if now - last_snapshot >= float(snapshot_every_sec):
                    try:
                        snap = await _rest_depth_snapshot(session, ep.rest_base, symbol)
                        snap_path = depth_dir / f"snapshot-{_utc_hms()}.json.gz"
                        JsonGzWriter(snap_path).write_obj(snap)
                        print(f"[capture] wrote snapshot -> {snap_path.name}")
                    except Exception as e:
                        print(f"[capture][WARN] snapshot failed: {e}")
                    last_snapshot = now

                # stop condition
                if now >= stop_at:
                    break

                msg = await ws.receive(timeout=30)
                if msg.type == WSMsgType.CLOSED:
                    print("[capture] WS closed by server")
                    break
                if msg.type == WSMsgType.ERROR:
                    print(f"[capture] WS error: {ws.exception()}")
                    break
                if msg.type != WSMsgType.TEXT:
                    continue

                try:
                    obj = json.loads(msg.data)
                except Exception:
                    continue

                # Combined stream wrapper: {"stream": "...", "data": {...}}
                data = obj.get("data", obj)

                # depth diffs
                if data.get("e") == "depthUpdate":
                    depth_out.write(obj)  # keep the wrapper; recon can unwrap
                    if depth_out.count % 500 == 0:
                        print(f"[capture] diffs written: {depth_out.count}")

                # trades
                elif data.get("e") in ("trade", "aggTrade", "aggTradeUpdate"):
                    trades_out.write(obj)
                    if trades_out.count % 500 == 0:
                        print(f"[capture] trades written: {trades_out.count}")

    depth_out.close()
    trades_out.close()
    print(f"[capture] done. diffs={depth_out.count} trades={trades_out.count}")
    if depth_out.count == 0:
        print("[capture][WARN] no diffs captured; check WS connectivity and symbol.")
