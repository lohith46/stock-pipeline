"""
Pydantic v2 event schemas — used for type-safe serialisation at the
WebSocket → Kafka boundary and for validation in silver assets.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class TradeEvent(BaseModel):
    trade_id:   str
    symbol:     str
    price:      float = Field(gt=0)
    quantity:   float = Field(gt=0)
    side:       Literal["buy", "sell"]
    agent_type: str
    mid_price:  Optional[float] = None
    timestamp:  datetime

    @field_validator("symbol")
    @classmethod
    def symbol_upper(cls, v: str) -> str:
        return v.upper()


class OrderEvent(BaseModel):
    order_id:    str
    symbol:      str
    price:       float = Field(gt=0)
    quantity:    float = Field(gt=0)
    side:        Literal["buy", "sell"]
    order_type:  Literal["market", "limit", "stop"]
    fill_status: str
    agent_code:  str
    timestamp:   datetime


class QuoteEvent(BaseModel):
    quote_id:  str
    symbol:    str
    bid:       float = Field(gt=0)
    ask:       float = Field(gt=0)
    bid_size:  float = Field(gt=0)
    ask_size:  float = Field(gt=0)
    timestamp: datetime

    @field_validator("ask")
    @classmethod
    def ask_gt_bid(cls, v: float, info) -> float:
        bid = info.data.get("bid", 0)
        if v <= bid:
            raise ValueError(f"ask ({v}) must be greater than bid ({bid})")
        return v


class MarketStatsEvent(BaseModel):
    symbol:       str
    daily_volume: float = Field(ge=0)
    daily_high:   float = Field(gt=0)
    daily_low:    float = Field(gt=0)
    open_price:   float = Field(gt=0)
    timestamp:    datetime


class TradingHaltEvent(BaseModel):
    halt_id:    str
    symbol:     str
    reason:     str
    halt_start: datetime
    halt_end:   Optional[datetime] = None
