"""
silver_quotes — validated QuoteUpdate records promoted from the staging layer.

Source: stg_quotes (staging/quotes Parquet)

Only rows where _validation_status == "valid" are included (quotes have no
surrogate key for deduplication; anomalies should be investigated upstream).
Original bronze columns (best_bid, best_ask, best_bid_size, best_ask_size, spread)
are preserved alongside the renamed bid/ask columns and all derived microstructure
metrics added in staging.

Key columns present in the output
----------------------------------
Original bronze  : symbol, best_bid, best_ask, best_bid_size, best_ask_size,
                   spread, timestamp, event_type
From staging     : bid, ask, bid_size, ask_size, timestamp_utc,
                   mid_price, spread_bps, depth_imbalance,
                   _validation_status, _invalid_reasons
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.quotes import stg_quotes
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="silver_quotes",
    description=(
        "Validated QuoteUpdate records with original bronze columns preserved "
        "alongside renamed bid/ask columns and microstructure metrics from staging."
    ),
    deps=[stg_quotes],
    metadata={"layer": "silver"},
)
def silver_quotes(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="staging", table="quotes")

    if df.empty:
        context.log.warning("No staged quotes to promote.")
        return pd.DataFrame()

    is_valid = df["_validation_status"] == "valid"
    silver = df[is_valid].reset_index(drop=True)

    context.log.info(
        "silver_quotes: %d promoted from %d staged  (%d invalid skipped)",
        len(silver),
        len(df),
        (~is_valid).sum(),
    )

    storage.write_parquet(silver, layer="silver", table="quotes", context=context)
    return silver
