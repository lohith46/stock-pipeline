"""
ref_symbols — derived catalog of active symbols observed across the pipeline.

Scans all three staging tables (trades, orders, quotes) and produces one row
per distinct symbol with provenance metadata.  This tells you which symbols are
active, when they were first and last seen, and which event streams they appear
in — useful for validating that the mock-stock configuration matches what the
pipeline actually receives.

Columns
-------
symbol          : ticker string (e.g. "AAPL")
first_seen_utc  : earliest timestamp_utc across all source tables
last_seen_utc   : latest  timestamp_utc across all source tables
seen_in_trades  : True if symbol appears in stg_trades
seen_in_orders  : True if symbol appears in stg_orders
seen_in_quotes  : True if symbol appears in stg_quotes
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.trades import stg_trades
from dagster_stock.assets.staging.orders import stg_orders
from dagster_stock.assets.staging.quotes import stg_quotes
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="ref_symbols",
    description=(
        "Derived catalog of active symbols observed across staging layers, "
        "with first/last seen timestamps and provenance flags."
    ),
    deps=[stg_trades, stg_orders, stg_quotes],
    metadata={"layer": "reference"},
)
def ref_symbols(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    sources: dict[str, tuple[str, str]] = {
        "trades": ("staging", "trades"),
        "orders": ("staging", "orders"),
        "quotes": ("staging", "quotes"),
    }

    frames: dict[str, pd.DataFrame] = {}
    for key, (layer, table) in sources.items():
        df = storage.read_parquet(layer=layer, table=table)
        if not df.empty and "symbol" in df.columns and "timestamp_utc" in df.columns:
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
            frames[key] = df[["symbol", "timestamp_utc"]].dropna()
        else:
            frames[key] = pd.DataFrame(columns=["symbol", "timestamp_utc"])

    all_symbols: set[str] = set()
    for df in frames.values():
        all_symbols.update(df["symbol"].dropna().unique())

    if not all_symbols:
        context.log.warning("ref_symbols: no symbols found across staging tables.")
        return pd.DataFrame(columns=["symbol", "first_seen_utc", "last_seen_utc",
                                     "seen_in_trades", "seen_in_orders", "seen_in_quotes"])

    rows = []
    for sym in sorted(all_symbols):
        timestamps = []
        seen_flags: dict[str, bool] = {}
        for key, df in frames.items():
            sym_rows = df[df["symbol"] == sym]
            seen_flags[f"seen_in_{key}"] = not sym_rows.empty
            if not sym_rows.empty:
                timestamps.extend(sym_rows["timestamp_utc"].dropna().tolist())

        rows.append({
            "symbol":          sym,
            "first_seen_utc":  min(timestamps) if timestamps else pd.NaT,
            "last_seen_utc":   max(timestamps) if timestamps else pd.NaT,
            **seen_flags,
        })

    result = pd.DataFrame(rows)
    context.log.info("ref_symbols: %d distinct symbols catalogued", len(result))
    storage.write_parquet(result, layer="reference", table="symbols", context=context)
    return result
