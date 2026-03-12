"""
simulator.py — Stock exchange WebSocket event simulator.

Serves a WebSocket feed at ws://HOST:PORT.  Every connected client receives
a continuous stream of JSON-encoded events that match the dagster_stock
Pydantic schemas (TradeEvent, OrderEvent, QuoteEvent, MarketStatsEvent,
TradingHaltEvent).

Usage:
    python dagster_stock/utils/simulator.py
    python dagster_stock/utils/simulator.py --host 0.0.0.0 --port 8765 --rate 20

Environment variables (override CLI defaults):
    SIM_HOST      listen address     (default: 0.0.0.0)
    SIM_PORT      listen port        (default: 8765)
    SIM_RATE      events per second  (default: 20)
    SIM_SYMBOLS   number of symbols  (default: 10)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import signal
import uuid
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Default parameters ────────────────────────────────────────────────────────

DEFAULT_HOST    = os.getenv("SIM_HOST",    "0.0.0.0")
DEFAULT_PORT    = int(os.getenv("SIM_PORT",    "8765"))
DEFAULT_RATE    = float(os.getenv("SIM_RATE",  "20"))    # events/second total
DEFAULT_SYMBOLS = int(os.getenv("SIM_SYMBOLS", "10"))

SYMBOL_UNIVERSE = [
    "AAPL", "MSFT", "GOOG", "TSLA", "AMZN",
    "NVDA", "META", "NFLX", "AMD",  "INTC",
    "JPM",  "BAC",  "GS",   "MS",   "WFC",
    "XOM",  "CVX",  "BP",   "SLB",  "COP",
]

AGENT_CODES = ["MM", "ARB", "RET", "INST"]
ORDER_TYPES = ["market", "limit", "stop"]
FILL_STATUSES = ["F", "PF", "O", "C"]
HALT_REASONS  = [
    "circuit_breaker_l1",
    "circuit_breaker_l2",
    "regulatory_halt",
    "news_pending",
    "order_imbalance",
]


# ── Price state per symbol ────────────────────────────────────────────────────

class SymbolState:
    """Maintains a realistic price series for one symbol."""

    HALF_SPREAD_PCT  = 0.0005   # 5 bps half-spread
    TICK_VOLATILITY  = 0.0008   # 8 bps per tick random walk
    MEAN_REVERSION   = 0.002    # pull-back toward initial price
    HALT_PROBABILITY = 0.00005  # per tick

    def __init__(self, symbol: str, seed_price: float) -> None:
        self.symbol      = symbol
        self.price       = seed_price
        self.ref_price   = seed_price   # mean-reversion anchor
        self.bid_size    = random.uniform(100, 2_000)
        self.ask_size    = random.uniform(100, 2_000)
        self.daily_volume = 0.0
        self.day_open    = seed_price
        self.day_high    = seed_price
        self.day_low     = seed_price
        self.last_stats_at: float = 0.0
        self.halted      = False
        self.halt_end_at: float | None = None

    # ── Price update ─────────────────────────────────────────────────────────

    def tick(self) -> None:
        """Advance price by one tick (geometric random walk + mean reversion)."""
        if self.halted:
            return
        shock = random.gauss(0, self.TICK_VOLATILITY)
        pull  = -self.MEAN_REVERSION * math.log(self.price / self.ref_price)
        self.price = self.price * math.exp(shock + pull)
        self.price = max(0.01, self.price)
        self.day_high = max(self.day_high, self.price)
        self.day_low  = min(self.day_low,  self.price)
        # Refresh book depth every tick
        self.bid_size = max(1.0, self.bid_size * random.uniform(0.8, 1.2))
        self.ask_size = max(1.0, self.ask_size * random.uniform(0.8, 1.2))

    @property
    def bid(self) -> float:
        return self.price * (1 - self.HALF_SPREAD_PCT)

    @property
    def ask(self) -> float:
        return self.price * (1 + self.HALF_SPREAD_PCT)

    @property
    def mid(self) -> float:
        return self.price

    def should_halt(self) -> bool:
        return not self.halted and random.random() < self.HALT_PROBABILITY

    def trigger_halt(self, now_ts: float) -> dict[str, Any]:
        self.halted = True
        duration_s = random.uniform(60, 600)   # 1–10 minutes
        self.halt_end_at = now_ts + duration_s
        return {
            "type":       "halt",
            "halt_id":    str(uuid.uuid4()),
            "symbol":     self.symbol,
            "reason":     random.choice(HALT_REASONS),
            "halt_start": _iso_now(),
            "halt_end":   None,
        }

    def maybe_resume(self, now_ts: float) -> dict[str, Any] | None:
        if self.halted and self.halt_end_at is not None and now_ts >= self.halt_end_at:
            self.halted = False
            end_iso = datetime.fromtimestamp(self.halt_end_at, tz=timezone.utc).isoformat()
            return {
                "type":       "halt",
                "halt_id":    str(uuid.uuid4()),
                "symbol":     self.symbol,
                "reason":     "resumed",
                "halt_start": datetime.fromtimestamp(
                    self.halt_end_at - random.uniform(60, 600), tz=timezone.utc
                ).isoformat(),
                "halt_end":   end_iso,
            }
        return None


# ── Event generators ──────────────────────────────────────────────────────────

def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def make_trade(sym: SymbolState) -> dict[str, Any]:
    side = random.choice(["buy", "sell"])
    qty  = round(random.uniform(1, 1_000), 2)
    sym.daily_volume += qty
    return {
        "type":       "trade",
        "trade_id":   str(uuid.uuid4()),
        "symbol":     sym.symbol,
        "price":      round(sym.price, 4),
        "quantity":   qty,
        "side":       side,
        "agent_type": random.choice(AGENT_CODES),
        "mid_price":  round(sym.mid, 4),
        "timestamp":  _iso_now(),
    }


def make_order(sym: SymbolState) -> dict[str, Any]:
    side       = random.choice(["buy", "sell"])
    order_type = random.choice(ORDER_TYPES)
    price_adj  = random.uniform(-0.005, 0.005)
    price      = round(sym.price * (1 + price_adj), 4)
    return {
        "type":        "order",
        "order_id":    str(uuid.uuid4()),
        "symbol":      sym.symbol,
        "price":       max(0.01, price),
        "quantity":    round(random.uniform(1, 2_000), 2),
        "side":        side,
        "order_type":  order_type,
        "fill_status": random.choice(FILL_STATUSES),
        "agent_code":  random.choice(AGENT_CODES),
        "timestamp":   _iso_now(),
    }


def make_quote(sym: SymbolState) -> dict[str, Any]:
    return {
        "type":      "quote",
        "quote_id":  str(uuid.uuid4()),
        "symbol":    sym.symbol,
        "bid":       round(sym.bid, 4),
        "ask":       round(sym.ask, 4),
        "bid_size":  round(sym.bid_size, 2),
        "ask_size":  round(sym.ask_size, 2),
        "timestamp": _iso_now(),
    }


def make_stats(sym: SymbolState) -> dict[str, Any]:
    return {
        "type":         "stats",
        "symbol":       sym.symbol,
        "daily_volume": round(sym.daily_volume, 2),
        "daily_high":   round(sym.day_high, 4),
        "daily_low":    round(sym.day_low, 4),
        "open_price":   round(sym.day_open, 4),
        "timestamp":    _iso_now(),
    }


# ── Event weights: how often each type is emitted ────────────────────────────

_EVENT_WEIGHTS = [
    ("trade", 0.35),
    ("order", 0.40),
    ("quote", 0.20),
    ("stats", 0.05),
]
_EVENT_TYPES, _WEIGHTS = zip(*_EVENT_WEIGHTS)


def next_event(states: list[SymbolState], now_ts: float) -> dict[str, Any]:
    """Pick a random symbol and event type and return the serialisable dict."""
    # Check for halt resumptions first
    for sym in states:
        resumed = sym.maybe_resume(now_ts)
        if resumed:
            return resumed

    sym = random.choice(states)
    sym.tick()

    if sym.halted:
        # While halted we only emit stats (no trades/orders/quotes)
        return make_stats(sym)

    # Possibly trigger a new halt
    if sym.should_halt():
        return sym.trigger_halt(now_ts)

    event_type = random.choices(_EVENT_TYPES, weights=_WEIGHTS, k=1)[0]

    # Emit stats every ~60 seconds per symbol
    if now_ts - sym.last_stats_at > 60:
        sym.last_stats_at = now_ts
        return make_stats(sym)

    if event_type == "trade":
        return make_trade(sym)
    elif event_type == "order":
        return make_order(sym)
    elif event_type == "quote":
        return make_quote(sym)
    else:
        return make_stats(sym)


# ── WebSocket server ──────────────────────────────────────────────────────────

_connected: set[WebSocketServerProtocol] = set()


async def handler(ws: WebSocketServerProtocol) -> None:
    _connected.add(ws)
    log.info("Client connected (%d total): %s", len(_connected), ws.remote_address)
    try:
        await ws.wait_closed()
    finally:
        _connected.discard(ws)
        log.info("Client disconnected (%d remaining)", len(_connected))


async def broadcast_loop(states: list[SymbolState], rate: float) -> None:
    """Continuously generate events and broadcast to all connected clients."""
    interval = 1.0 / rate
    log.info("Broadcast loop started: %.1f events/sec, %d symbols", rate, len(states))

    while True:
        await asyncio.sleep(interval)
        if not _connected:
            continue

        now_ts = asyncio.get_event_loop().time()
        event  = next_event(states, now_ts)
        payload = json.dumps(event)

        dead: list[WebSocketServerProtocol] = []
        for ws in list(_connected):
            try:
                await ws.send(payload)
            except Exception:
                dead.append(ws)

        for ws in dead:
            _connected.discard(ws)


async def serve(host: str, port: int, rate: float, n_symbols: int) -> None:
    symbols  = SYMBOL_UNIVERSE[:n_symbols]
    seed_prices = {s: random.uniform(10, 500) for s in symbols}
    states   = [SymbolState(s, seed_prices[s]) for s in symbols]

    log.info("Simulating %d symbols: %s", n_symbols, symbols)

    stop = asyncio.Event()

    def _on_signal(*_: Any) -> None:
        log.info("Shutdown signal — stopping simulator.")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_signal)

    async with websockets.serve(handler, host, port):
        log.info("WebSocket simulator listening on ws://%s:%d", host, port)
        broadcast_task = asyncio.create_task(broadcast_loop(states, rate))
        await stop.wait()
        broadcast_task.cancel()

    log.info("Simulator stopped.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stock exchange WebSocket simulator")
    p.add_argument("--host",    default=DEFAULT_HOST,    help="Listen address")
    p.add_argument("--port",    default=DEFAULT_PORT,    type=int, help="Listen port")
    p.add_argument("--rate",    default=DEFAULT_RATE,    type=float, help="Events per second")
    p.add_argument("--symbols", default=DEFAULT_SYMBOLS, type=int,
                   help=f"Number of symbols to simulate (max {len(SYMBOL_UNIVERSE)})")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    n    = min(args.symbols, len(SYMBOL_UNIVERSE))
    asyncio.run(serve(args.host, args.port, args.rate, n))


if __name__ == "__main__":
    main()
