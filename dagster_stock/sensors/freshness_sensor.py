"""
freshness_sensor — fires an alert when gold assets exceed the freshness SLA.

Checks once every 30 minutes whether the most recent gold_ohlcv or
gold_market_quality partition is older than FRESHNESS_SLA_HOURS.
"""

import pandas as pd
from datetime import datetime, timezone

from dagster import sensor, SensorEvaluationContext, SensorResult, DefaultSensorStatus

from dagster_stock.resources.storage_resource import StorageResource

FRESHNESS_SLA_HOURS = 25


@sensor(
    minimum_interval_seconds=1800,
    default_status=DefaultSensorStatus.RUNNING,
    description="Alerts when gold assets are older than the freshness SLA.",
)
def gold_freshness_sensor(context: SensorEvaluationContext):
    storage = StorageResource()
    stale_assets: list[str] = []

    for asset_name, layer, table in [
        ("gold_ohlcv", "gold", "ohlcv"),
        ("gold_market_quality", "gold", "market_quality"),
    ]:
        try:
            df = storage.read_parquet(layer=layer, table=table)
            if df.empty:
                stale_assets.append(f"{asset_name} (no data)")
                continue
            max_date = pd.to_datetime(df["trade_date"]).max()
            age_hours = (datetime.now(tz=timezone.utc) - pd.Timestamp(max_date).tz_localize("UTC")).total_seconds() / 3600
            if age_hours > FRESHNESS_SLA_HOURS:
                stale_assets.append(f"{asset_name} ({age_hours:.1f}h old)")
        except Exception as exc:
            context.log.warning("Could not check freshness for %s: %s", asset_name, exc)

    if stale_assets:
        context.log.error("FRESHNESS SLA BREACH: %s", stale_assets)
        # In production, emit a PagerDuty / Slack alert here

    return SensorResult(dynamic_partitions_requests=[])
