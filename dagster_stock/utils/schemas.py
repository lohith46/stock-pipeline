"""
Pydantic v2 schemas matching the DE-Stock exchange simulator's event format.

Every message from the simulator has the envelope:
    {"event_type": "<EventType>", "data": { ... }}

plus a one-off welcome message on first connect:
    {"type": "welcome", "message": "...", "timestamp": "..."}

References:
    mock-stock/src/events/schema.rs
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


# ── Shared primitives ─────────────────────────────────────────────────────────

class _Event(BaseModel):
    event_id:  str
    timestamp: datetime
    symbol:    str


# ── Trade ─────────────────────────────────────────────────────────────────────

class TradeExecutedEvent(_Event):
    """stock.trades — a buy/sell match that cleared the order book."""
    trade_id:         str
    price:            float = Field(gt=0)
    quantity:         float = Field(gt=0)
    buy_order_id:     str
    sell_order_id:    str
    buyer_agent_id:   str
    seller_agent_id:  str
    is_aggressive_buy: bool


# ── Orders ────────────────────────────────────────────────────────────────────

class OrderPlacedEvent(_Event):
    """stock.orders — a new order submitted to the exchange."""
    order_id:   str
    side:       Literal["Buy", "Sell"]
    order_type: Literal["Market", "Limit", "StopLoss", "StopLimit"]
    price:      Optional[float] = None   # None for Market orders
    quantity:   float = Field(gt=0)
    agent_id:   str
    agent_type: Literal[
        "RetailTrader",
        "InstitutionalInvestor",
        "MarketMaker",
        "HighFrequencyTrader",
    ]


class OrderCancelledEvent(_Event):
    """stock.orders — an order has been cancelled."""
    order_id: str
    reason:   str


# ── Quotes ────────────────────────────────────────────────────────────────────

class QuoteUpdateEvent(_Event):
    """stock.quotes — best bid/ask snapshot after each order book change."""
    best_bid:      Optional[float] = None
    best_bid_size: float = Field(ge=0)
    best_ask:      Optional[float] = None
    best_ask_size: float = Field(ge=0)
    spread:        Optional[float] = None


# ── Order book ────────────────────────────────────────────────────────────────

class PriceLevel(BaseModel):
    price:       float
    quantity:    float
    order_count: int


class OrderBookSnapshotEvent(_Event):
    """stock.orderbook — periodic full depth-of-book snapshot."""
    bids: list[PriceLevel]
    asks: list[PriceLevel]


# ── Market stats ──────────────────────────────────────────────────────────────

class MarketStatsEvent(_Event):
    """stock.stats — rolling session OHLCV + VWAP, emitted periodically."""
    open:        float = Field(gt=0)
    high:        float = Field(gt=0)
    low:         float = Field(gt=0)
    close:       float = Field(gt=0)
    volume:      float = Field(ge=0)
    trade_count: int   = Field(ge=0)
    vwap:        float = Field(gt=0)


# ── Halts ─────────────────────────────────────────────────────────────────────

class TradingHaltEvent(_Event):
    """stock.halts — circuit breaker or regulatory halt triggered."""
    reason:                str
    circuit_breaker_level: Optional[str] = None   # "Level1" | "Level2" | "Level3" | null
    reference_price:       float
    current_price:         float
    price_change_percent:  float


class TradingResumeEvent(_Event):
    """stock.halts — trading resumed after a halt."""
    halt_duration_ms: int


# ── Agent actions ─────────────────────────────────────────────────────────────

class AgentActionEvent(_Event):
    """stock.agents — an agent's strategic decision (informational)."""
    agent_id:         str
    agent_type:       str
    action:           str
    decision_factors: list[str]


# ── Envelope ──────────────────────────────────────────────────────────────────

EVENT_SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "TradeExecuted":     TradeExecutedEvent,
    "OrderPlaced":       OrderPlacedEvent,
    "OrderCancelled":    OrderCancelledEvent,
    "QuoteUpdate":       QuoteUpdateEvent,
    "OrderBookSnapshot": OrderBookSnapshotEvent,
    "MarketStats":       MarketStatsEvent,
    "TradingHalt":       TradingHaltEvent,
    "TradingResume":     TradingResumeEvent,
    "AgentAction":       AgentActionEvent,
}
