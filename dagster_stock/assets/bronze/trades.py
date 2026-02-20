"""
bronze_trades — raw ingestion from the stock.trades Kafka topic.

Schema-on-read: all fields stored as-is; no type coercion happens here.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.trades"


@asset(
    name="bronze_trades",
    description="Raw trade events polled from the stock.trades Kafka topic.",
    required_resource_keys={"kafka", "storage"},
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_trades(context: AssetExecutionContext, kafka: KafkaConsumerResource, storage: StorageResource) -> pd.DataFrame:
    """Poll messages from stock.trades and persist as raw Parquet."""
    messages = kafka.poll(topic=TOPIC, max_records=10_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    df = pd.DataFrame(messages)
    context.log.info("Polled %d trade records from %s", len(df), TOPIC)

    storage.write_parquet(df, layer="bronze", table="trades", context=context)
    return df
