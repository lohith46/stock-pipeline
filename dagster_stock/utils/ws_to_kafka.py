"""
ws_to_kafka.py — DE-Stock WebSocket → Kafka bridge.

Connects to the mock-stock exchange simulator (ws://HOST:PORT), receives
JSON events with the envelope {"event_type": "...", "data": {...}}, and
publishes the data payload (enriched with event_type) to the appropriate
Kafka topic.

Usage:
    python dagster_stock/utils/ws_to_kafka.py

Configuration (environment variables):
    WS_URL                    Simulator address    (default: ws://localhost:8080)
    KAFKA_BOOTSTRAP_SERVERS                        (default: localhost:9092)
    RECONNECT_DELAY_S         Seconds before retry (default: 5)
    MAX_RECONNECT_ATTEMPTS    0 = unlimited        (default: 0)
    LOG_INTERVAL_N            Log every N events   (default: 500)
    SKIP_UNKNOWN_EVENTS       Drop events not in map (default: false)

Topic overrides (each defaults to the value shown):
    TOPIC_TRADES      stock.trades
    TOPIC_ORDERS      stock.orders
    TOPIC_QUOTES      stock.quotes
    TOPIC_ORDERBOOK   stock.orderbook
    TOPIC_STATS       stock.stats
    TOPIC_HALTS       stock.halts
    TOPIC_AGENTS      stock.agents
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

import websockets
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────────

WS_URL                  = os.getenv("WS_URL",                  "ws://localhost:8080")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RECONNECT_DELAY_S       = float(os.getenv("RECONNECT_DELAY_S", "5"))
MAX_RECONNECT_ATTEMPTS  = int(os.getenv("MAX_RECONNECT_ATTEMPTS", "0"))   # 0 = unlimited
LOG_INTERVAL_N          = int(os.getenv("LOG_INTERVAL_N", "500"))
SKIP_UNKNOWN_EVENTS     = os.getenv("SKIP_UNKNOWN_EVENTS", "false").lower() == "true"

# Topic routing table — override individual topics via env vars
TOPIC_ROUTER: dict[str, str] = {
    "TradeExecuted":     os.getenv("TOPIC_TRADES",    "stock.trades"),
    "OrderPlaced":       os.getenv("TOPIC_ORDERS",    "stock.orders"),
    "OrderCancelled":    os.getenv("TOPIC_ORDERS",    "stock.orders"),
    "QuoteUpdate":       os.getenv("TOPIC_QUOTES",    "stock.quotes"),
    "OrderBookSnapshot": os.getenv("TOPIC_ORDERBOOK", "stock.orderbook"),
    "MarketStats":       os.getenv("TOPIC_STATS",     "stock.stats"),
    "TradingHalt":       os.getenv("TOPIC_HALTS",     "stock.halts"),
    "TradingResume":     os.getenv("TOPIC_HALTS",     "stock.halts"),
    "AgentAction":       os.getenv("TOPIC_AGENTS",    "stock.agents"),
}


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        log.error("Kafka delivery failed [%s]: %s", msg.topic(), err)


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":      KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms":              5,          # micro-batch for throughput
        "batch.num.messages":     1000,
        "compression.type":       "lz4",
        "acks":                   "1",        # leader-only for speed
        "retries":                3,
        "socket.keepalive.enable": True,
    })


# ── Bridge logic ──────────────────────────────────────────────────────────────

_stop = asyncio.Event()


def _on_signal(*_: Any) -> None:
    log.info("Shutdown signal received — draining and exiting.")
    _stop.set()


async def bridge(producer: Producer) -> None:
    """Connect to mock-stock and forward events to Kafka until stopped."""
    attempt   = 0
    published = 0
    skipped   = 0

    while not _stop.is_set():
        attempt += 1
        if MAX_RECONNECT_ATTEMPTS and attempt > MAX_RECONNECT_ATTEMPTS:
            log.error("Max reconnect attempts (%d) reached — giving up.", MAX_RECONNECT_ATTEMPTS)
            break

        try:
            log.info("Connecting to %s (attempt %d) …", WS_URL, attempt)
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                log.info("Connected. Routing events to Kafka …")
                attempt = 0  # reset on successful connect

                async for raw in ws:
                    if _stop.is_set():
                        break

                    try:
                        envelope: dict[str, Any] = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        log.warning("JSON decode error — skipping: %s", exc)
                        skipped += 1
                        continue

                    # Skip the initial welcome message
                    if envelope.get("type") == "welcome":
                        log.info("Simulator: %s", envelope.get("message", "connected"))
                        continue

                    event_type: str = envelope.get("event_type", "")
                    data: dict[str, Any] = envelope.get("data", {})

                    if not event_type or not data:
                        log.debug("Malformed envelope — skipping: %s", raw[:120])
                        skipped += 1
                        continue

                    topic = TOPIC_ROUTER.get(event_type)
                    if topic is None:
                        if SKIP_UNKNOWN_EVENTS:
                            skipped += 1
                            continue
                        # Publish to a catch-all topic so nothing is silently lost
                        topic = f"stock.unknown"
                        log.debug("No topic mapping for event_type=%s — publishing to %s", event_type, topic)

                    # Inject event_type into payload so the bronze layer can distinguish
                    # OrderPlaced vs OrderCancelled in the same stock.orders topic, etc.
                    payload = {"event_type": event_type, **data}
                    serialised = json.dumps(payload).encode("utf-8")

                    # Use symbol as partition key for locality
                    key = data.get("symbol", event_type).encode("utf-8")

                    producer.produce(
                        topic,
                        key=key,
                        value=serialised,
                        callback=_delivery_report,
                    )
                    producer.poll(0)   # trigger delivery callbacks
                    published += 1

                    if published % LOG_INTERVAL_N == 0:
                        log.info(
                            "Published %d events (%d skipped) — last: %s → %s",
                            published, skipped, event_type, topic,
                        )

        except websockets.exceptions.ConnectionClosedError as exc:
            log.warning("Connection closed: %s", exc)
        except websockets.exceptions.InvalidURI as exc:
            log.error("Invalid WS_URL=%s: %s", WS_URL, exc)
            break
        except OSError as exc:
            log.warning("Connection error: %s", exc)
        except Exception as exc:
            log.error("Unexpected error: %s", exc, exc_info=True)

        if not _stop.is_set():
            log.info("Reconnecting in %.1fs …", RECONNECT_DELAY_S)
            try:
                await asyncio.wait_for(_stop.wait(), timeout=RECONNECT_DELAY_S)
            except asyncio.TimeoutError:
                pass

    producer.flush(timeout=30)
    log.info("Flushed producer. Total published=%d skipped=%d.", published, skipped)


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_signal)

    producer = _make_producer()
    log.info("Kafka target: %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("Topic routing: %s", TOPIC_ROUTER)

    try:
        loop.run_until_complete(bridge(producer))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
