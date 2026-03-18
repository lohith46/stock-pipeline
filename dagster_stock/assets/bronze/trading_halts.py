"""
bronze_trading_halts — raw ingestion from the stock.halts Kafka topic.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.halts"


@asset(
    name="bronze_trading_halts",
    description="Raw trading halt events polled from the stock.halts Kafka topic.",
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_trading_halts(context: AssetExecutionContext, kafka: KafkaConsumerResource, storage: StorageResource) -> pd.DataFrame:
    messages = kafka.poll(topic=TOPIC, max_records=1_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    df = pd.DataFrame(messages)
    context.log.info("Polled %d trading-halt records from %s", len(df), TOPIC)

    storage.write_parquet(df, layer="bronze", table="trading_halts", context=context)
    return df
