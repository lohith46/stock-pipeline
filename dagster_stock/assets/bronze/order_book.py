"""
bronze_order_book — raw OrderBookSnapshot events from stock.orderbook.

Each row contains a periodic depth-of-book snapshot (bids/asks stored as
JSON-serialised lists) for one symbol.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.orderbook"


@asset(
    name="bronze_order_book",
    description="Raw order-book depth snapshots from the stock.orderbook Kafka topic.",
    required_resource_keys={"kafka", "storage"},
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_order_book(
    context: AssetExecutionContext,
    kafka: KafkaConsumerResource,
    storage: StorageResource,
) -> pd.DataFrame:
    messages = kafka.poll(topic=TOPIC, max_records=2_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    # bids/asks arrive as lists of dicts; serialise to JSON strings for Parquet
    import json
    rows = []
    for m in messages:
        row = dict(m)
        for key in ("bids", "asks"):
            if isinstance(row.get(key), list):
                row[key] = json.dumps(row[key])
        rows.append(row)

    df = pd.DataFrame(rows)
    context.log.info("Polled %d order-book snapshots from %s", len(df), TOPIC)

    storage.write_parquet(df, layer="bronze", table="order_book", context=context)
    return df
