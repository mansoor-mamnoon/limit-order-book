# python/olob/crypto/binance.py
from __future__ import annotations
import asyncio, json, gzip, os, ssl
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
import aiohttp
import certifi

from .common import dpath, ensure_dir, new_jsonl_path, JsonlGz

def _endpoints(exchange: str) -> Tuple[str, str]:
    e = exchange.lower()
    if e == "binance":
        return "https://api.binance.com", "wss://stream.binance.com:9443/stream"
    if e == "binanceus":
        return "https://api.binance.us", "wss://stream.binance.us:9443/stream"
    raise ValueError(f"Unsupported exchange: {exchange}")

def _make_ssl_context() -> ssl.SSLContext:
    cafile = os.getenv("LOB_CA_BUNDLE", certifi.where())
    ctx = ssl.create_default_context(cafile=cafile)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

class LocalBook:
    def __init__(self) -> None:
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: int = 0

    def apply_diff(self, bids: List[List[str]], asks: List[List[str]]) -> None:
        for px_s, qty_s, *_ in bids:
            px = float(px_s); qty = float(qty_s)
            if qty == 0.0: self.bids.pop(px, None)
            else:          self.bids[px] = qty
        for px_s, qty_s, *_ in asks:
            px = float(px_s); qty = float(qty_s)
            if qty == 0.0: self.asks.pop(px, None)
            else:          self.asks[px] = qty

async def _rest_depth_snapshot(session: aiohttp.ClientSession, rest_base: str, symbol: str) -> Dict[str, Any]:
    url = f"{rest_base}/api/v3/depth"
    params = {"symbol": symbol.upper(), "limit": "1000"}
    async with session.get(url, params=params, timeout=20) as r:
        r.raise_for_status()
        return await r.json()

async def _persist_snapshot(root: str, symbol: str, snap: Dict[str, Any]) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = dpath(root, date_str, "binance", symbol.upper(), "depth")  # keep folder name 'binance' or use exchange if you prefer
    ensure_dir(out_dir)
    ts = datetime.now(timezone.utc).strftime("%H%M%S")
    path = out_dir / f"snapshot-{ts}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as f:
        json.dump(snap, f, separators=(",", ":"))
    return path

async def _consumer_depth_and_trades(
    symbol: str,
    minutes: int,
    raw_root: str,
    snapshot_every_sec: int,
    exchange: str,
) -> None:
    rest_base, ws_base = _endpoints(exchange)
    symbol_l = symbol.lower()

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base_dir = Path(raw_root) / date_str / exchange.lower() / symbol.upper()
    depth_dir = base_dir / "depth"; trades_dir = base_dir / "trades"
    ensure_dir(depth_dir); ensure_dir(trades_dir)

    depth_path = new_jsonl_path(depth_dir, "diffs")
    trade_path = new_jsonl_path(trades_dir, "trades")
    depth_writer = JsonlGz(depth_path)
    trade_writer = JsonlGz(trade_path)

    stream = f"{symbol_l}@depth@100ms/{symbol_l}@trade"
    ws_url = f"{ws_base}?streams={stream}"

    local = LocalBook()
    pending: List[Dict[str, Any]] = []
    synced = False

    ssl_ctx = _make_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(raise_for_status=True, connector=connector) as session:
        # initial REST snapshot
        snap = await _rest_depth_snapshot(session, rest_base, symbol)
        local.last_update_id = int(snap["lastUpdateId"])
        await _persist_snapshot(raw_root, symbol, snap)

        # periodic snapshot task
        async def snapshot_task():
            while True:
                await asyncio.sleep(snapshot_every_sec)
                s = await _rest_depth_snapshot(session, rest_base, symbol)
                await _persist_snapshot(raw_root, symbol, s)

        snapper = asyncio.create_task(snapshot_task())
        end_time = datetime.now(timezone.utc) + timedelta(minutes=minutes)

        try:
            async with session.ws_connect(ws_url, heartbeat=30, timeout=30, ssl=ssl_ctx) as ws:
                while datetime.now(timezone.utc) < end_time:
                    msg = await ws.receive()
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        obj = json.loads(msg.data)
                        data = obj.get("data", {})

                        etype = data.get("e")
                        if etype == "depthUpdate":
                            depth_writer.write(obj)

                            U = int(data["U"]); u = int(data["u"])
                            if u <= local.last_update_id:
                                continue
                            if not synced:
                                pending.append(obj)
                                if U <= local.last_update_id + 1 <= u:
                                    local.apply_diff(snap["bids"], snap["asks"])
                                    for ev in pending:
                                        d = ev["data"]
                                        local.apply_diff(d.get("b", []), d.get("a", []))
                                        local.last_update_id = int(d["u"])
                                    pending.clear()
                                    synced = True
                                continue

                            if U <= local.last_update_id + 1 <= u:
                                local.apply_diff(data.get("b", []), data.get("a", []))
                                local.last_update_id = u
                            else:
                                # continuity lost -> resync
                                synced = False
                                pending.clear()
                                snap = await _rest_depth_snapshot(session, rest_base, symbol)
                                local.last_update_id = int(snap["lastUpdateId"])
                                await _persist_snapshot(raw_root, symbol, snap)

                        elif etype == "trade":
                            trade_writer.write(obj)
                        else:
                            pass

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    else:
                        pass
        finally:
            snapper.cancel()
            depth_writer.close()
            trade_writer.close()

def run_capture(symbol: str, minutes: int, raw_root: str, snapshot_every_sec: int = 600, exchange: str = "binance") -> None:
    asyncio.run(_consumer_depth_and_trades(
        symbol=symbol, minutes=minutes, raw_root=raw_root,
        snapshot_every_sec=snapshot_every_sec, exchange=exchange
    ))
