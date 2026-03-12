"""
bronze_orders — raw order events from the stock.orders Kafka topic.

Both OrderPlaced and OrderCancelled messages land on the same topic.
The `event_type` field (injected by the ws_to_kafka bridge) distinguishes them.

Schema-on-read: all fields stored as-is; no type coercion happens here.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.orders"


@asset(
    name="bronze_orders",
    description=(
        "Raw order events (OrderPlaced + OrderCancelled) from the stock.orders Kafka topic. "
        "Distinguish event types via the injected `event_type` column."
    ),
    required_resource_keys={"kafka", "storage"},
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_orders(
    context: AssetExecutionContext,
    kafka: KafkaConsumerResource,
    storage: StorageResource,
) -> pd.DataFrame:
    messages = kafka.poll(topic=TOPIC, max_records=10_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    df = pd.DataFrame(messages)

    placed    = (df["event_type"] == "OrderPlaced").sum()    if "event_type" in df.columns else 0
    cancelled = (df["event_type"] == "OrderCancelled").sum() if "event_type" in df.columns else 0
    context.log.info(
        "Polled %d order records from %s  (placed=%d, cancelled=%d)",
        len(df), TOPIC, placed, cancelled,
    )

    storage.write_parquet(df, layer="bronze", table="orders", context=context)
    return df
