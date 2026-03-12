"""
bronze_agent_actions — raw AgentAction events from stock.agents.

Records each agent's strategic decision (action + decision factors) for
later analysis of agent behaviour patterns.
"""

import json
import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource

TOPIC = "stock.agents"


@asset(
    name="bronze_agent_actions",
    description="Raw agent decision events from the stock.agents Kafka topic.",
    required_resource_keys={"kafka", "storage"},
    metadata={"topic": TOPIC, "layer": "bronze"},
)
def bronze_agent_actions(
    context: AssetExecutionContext,
    kafka: KafkaConsumerResource,
    storage: StorageResource,
) -> pd.DataFrame:
    messages = kafka.poll(topic=TOPIC, max_records=5_000, timeout_ms=5_000)

    if not messages:
        context.log.warning("No messages received from %s", TOPIC)
        return pd.DataFrame()

    rows = []
    for m in messages:
        row = dict(m)
        # decision_factors is a list of strings — serialise for Parquet
        if isinstance(row.get("decision_factors"), list):
            row["decision_factors"] = json.dumps(row["decision_factors"])
        rows.append(row)

    df = pd.DataFrame(rows)
    context.log.info("Polled %d agent-action records from %s", len(df), TOPIC)

    storage.write_parquet(df, layer="bronze", table="agent_actions", context=context)
    return df
