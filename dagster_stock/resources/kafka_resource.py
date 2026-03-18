"""
KafkaConsumerResource — polls Kafka topics and commits offsets.

Configured via environment variables:
    KAFKA_BOOTSTRAP_SERVERS   (default: localhost:9092)
    KAFKA_GROUP_ID            (default: dagster-stock-pipeline)
    KAFKA_AUTO_OFFSET_RESET   (default: earliest)
"""

import json
import os
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from dagster import ConfigurableResource, get_dagster_logger


class KafkaConsumerResource(ConfigurableResource):
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "dagster-stock-pipeline")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    def _make_consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
            "allow.auto.create.topics": True,
        })

    def poll(self, topic: str, max_records: int = 10_000, timeout_ms: int = 5_000) -> list[dict[str, Any]]:
        log = get_dagster_logger()
        consumer = self._make_consumer()
        consumer.subscribe([topic])

        records: list[dict[str, Any]] = []
        timeout_s = timeout_ms / 1_000

        try:
            while len(records) < max_records:
                msg = consumer.poll(timeout=timeout_s)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        log.warning("Topic %s does not exist yet, returning empty result", topic)
                        break
                    raise KafkaException(msg.error())

                try:
                    records.append(json.loads(msg.value().decode("utf-8")))
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    log.warning("Failed to decode message: %s", exc)

            consumer.commit(asynchronous=False)
            log.info("Committed offsets after polling %d records from %s", len(records), topic)
        finally:
            consumer.close()

        return records

    def get_lag(self, topic: str) -> int:
        """Return total consumer group lag across all partitions for *topic*."""
        consumer = self._make_consumer()
        consumer.subscribe([topic])
        consumer.poll(timeout=1)

        total_lag = 0
        metadata = consumer.list_topics(topic=topic, timeout=10)
        partitions = list(metadata.topics[topic].partitions.keys())

        from confluent_kafka import TopicPartition
        tps = [TopicPartition(topic, p) for p in partitions]

        committed = consumer.committed(tps, timeout=10)
        high_watermarks = [consumer.get_watermark_offsets(tp, timeout=5) for tp in tps]

        for committed_tp, (_, high) in zip(committed, high_watermarks):
            committed_offset = committed_tp.offset if committed_tp.offset >= 0 else 0
            total_lag += max(0, high - committed_offset)

        consumer.close()
        return total_lag
