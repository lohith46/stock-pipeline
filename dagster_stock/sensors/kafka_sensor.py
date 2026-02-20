"""
kafka_lag_sensor — triggers the bronze_job when consumer lag exceeds a threshold.

Polls lag every 60 s; fires a run request when any monitored topic
has accumulated more than LAG_THRESHOLD unprocessed messages.
"""

from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.jobs.jobs import bronze_job

MONITORED_TOPICS = ["stock.trades", "stock.orders", "stock.quotes", "stock.stats", "stock.halts"]
LAG_THRESHOLD = 500


@sensor(
    job=bronze_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Fires bronze_job when Kafka consumer lag exceeds threshold.",
)
def kafka_lag_sensor(context: SensorEvaluationContext):
    kafka = KafkaConsumerResource()
    high_lag_topics: dict[str, int] = {}

    for topic in MONITORED_TOPICS:
        try:
            lag = kafka.get_lag(topic)
            if lag > LAG_THRESHOLD:
                high_lag_topics[topic] = lag
        except Exception as exc:
            context.log.warning("Could not fetch lag for %s: %s", topic, exc)

    if high_lag_topics:
        context.log.info("High lag detected: %s — triggering bronze_job", high_lag_topics)
        yield RunRequest(
            run_key=f"lag-{context.cursor or 0}",
            run_config={},
            tags={"trigger": "kafka_lag_sensor", "lag_topics": str(list(high_lag_topics.keys()))},
        )
        context.update_cursor(str(int(context.cursor or 0) + 1))
