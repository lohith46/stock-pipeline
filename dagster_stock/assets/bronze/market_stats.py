"""
bronze_market_stats — raw ingestion from the stock.stats Kafka topic.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.stats"


@asset(
    name="bronze_market_stats",
    description="Raw market statistics polled from the stock.stats Kafka topic.",
    required_resource_keys={"kafka", "storage"},
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_market_stats(context: AssetExecutionContext, kafka: KafkaConsumerResource, storage: StorageResource) -> pd.DataFrame:
    messages = kafka.poll(topic=TOPIC, max_records=5_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    df = pd.DataFrame(messages)
    context.log.info("Polled %d market-stats records from %s", len(df), TOPIC)

    storage.write_parquet(df, layer="bronze", table="market_stats", context=context)
    return df
