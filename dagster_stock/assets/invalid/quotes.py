"""
invalid_quotes — QuoteUpdate records rejected from the silver layer.

Reads stg_quotes and materialises only the rows where:
    • _validation_status == "invalid"  (null bid/ask, crossed market, etc.)

Quotes do not carry a surrogate key suitable for deduplication; if you observe
repeated (symbol, timestamp) pairs investigate the upstream source rather than
silently discarding them here.

All original bronze columns are preserved alongside every staging diagnostic
column so analysts can trace the exact failure reason without re-running.

Stored under ./data/invalid/quotes/ with the standard daily partitioning.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.quotes import stg_quotes
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="invalid_quotes",
    description=(
        "Invalid QuoteUpdate records rejected from the silver layer, "
        "with full reason metadata preserved for investigation."
    ),
    deps=[stg_quotes],
    metadata={"layer": "invalid"},
)
def invalid_quotes(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="staging", table="quotes")

    if df.empty:
        context.log.info("No staged quotes — nothing to scan for invalids.")
        return pd.DataFrame()

    flagged = df[df["_validation_status"] == "invalid"].copy()

    context.log.info("invalid_quotes: %d flagged", len(flagged))

    if not flagged.empty:
        storage.write_parquet(flagged, layer="invalid", table="quotes", context=context)

    return flagged
