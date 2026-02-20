"""
ws_to_kafka.py — WebSocket → Kafka bridge.

Connects to a stock exchange WebSocket simulator, deserialises events
using Pydantic schemas, and publishes them to the appropriate Kafka topic.

Usage:
    python dagster_stock/utils/ws_to_kafka.py

Environment variables:
    WS_URL                    WebSocket feed URL (default: ws://localhost:8765)
    KAFKA_BOOTSTRAP_SERVERS   (default: localhost:9092)
"""

import asyncio
import json
import logging
import os
import signal
import sys

import websockets
from confluent_kafka import Producer

from dagster_stock.utils.schemas import (
    TradeEvent, OrderEvent, QuoteEvent, MarketStatsEvent, TradingHaltEvent,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

WS_URL = os.getenv("WS_URL", "ws://localhost:8765")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_MAP: dict[str, tuple[type, str]] = {
    "trade": (TradeEvent, "stock.trades"),
    "order": (OrderEvent, "stock.orders"),
    "quote": (QuoteEvent, "stock.quotes"),
    "stats": (MarketStatsEvent, "stock.stats"),
    "halt":  (TradingHaltEvent, "stock.halts"),
}

_stop = asyncio.Event()


def _on_signal(*_):
    log.info("Shutdown signal received.")
    _stop.set()


async def bridge():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    log.info("Connecting to %s …", WS_URL)

    async with websockets.connect(WS_URL) as ws:
        log.info("Connected. Forwarding events to Kafka …")
        while not _stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                payload = json.loads(raw)
                event_type = payload.get("type", "").lower()

                if event_type not in TOPIC_MAP:
                    log.warning("Unknown event type: %s — skipping", event_type)
                    continue

                schema_cls, topic = TOPIC_MAP[event_type]
                event = schema_cls.model_validate(payload)
                serialised = event.model_dump_json().encode("utf-8")

                producer.produce(topic, value=serialised)
                producer.poll(0)

            except Exception as exc:
                log.error("Failed to process message: %s | raw=%s", exc, raw[:200])

    producer.flush()
    log.info("Producer flushed. Exiting.")


def main():
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _on_signal)
    asyncio.run(bridge())


if __name__ == "__main__":
    main()
