"""
Dagster entrypoint — registers all assets, resources, jobs, schedules, and sensors.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_stock.assets import bronze, silver, gold
from dagster_stock.checks.silver_checks import silver_checks
from dagster_stock.checks.gold_checks import gold_checks
from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.duckdb_resource import DuckDBResource
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.io_managers.parquet_io_manager import ParquetIOManager
from dagster_stock.sensors.kafka_sensor import kafka_lag_sensor
from dagster_stock.sensors.freshness_sensor import gold_freshness_sensor
from dagster_stock.schedules.schedules import daily_bronze_schedule, hourly_silver_schedule
from dagster_stock.jobs.jobs import bronze_job, silver_job, gold_job, full_pipeline_job

bronze_assets = load_assets_from_modules([bronze], group_name="bronze")
silver_assets = load_assets_from_modules([silver], group_name="silver")
gold_assets   = load_assets_from_modules([gold],   group_name="gold")

defs = Definitions(
    assets=[*bronze_assets, *silver_assets, *gold_assets],
    asset_checks=[*silver_checks, *gold_checks],
    resources={
        "kafka":    KafkaConsumerResource(),
        "duckdb":   DuckDBResource(),
        "storage":  StorageResource(),
        "io_manager": ParquetIOManager(),
    },
    jobs=[bronze_job, silver_job, gold_job, full_pipeline_job],
    schedules=[daily_bronze_schedule, hourly_silver_schedule],
    sensors=[kafka_lag_sensor, gold_freshness_sensor],
)
