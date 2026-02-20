"""
bronze_quotes — raw ingestion from the stock.quotes Kafka topic.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.quotes"


@asset(
    name="bronze_quotes",
    description="Raw quote events polled from the stock.quotes Kafka topic.",
    required_resource_keys={"kafka", "storage"},
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_quotes(context: AssetExecutionContext, kafka: KafkaConsumerResource, storage: StorageResource) -> pd.DataFrame:
    messages = kafka.poll(topic=TOPIC, max_records=10_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    df = pd.DataFrame(messages)
    context.log.info("Polled %d quote records from %s", len(df), TOPIC)

    storage.write_parquet(df, layer="bronze", table="quotes", context=context)
    return df
