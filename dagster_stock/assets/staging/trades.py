"""
stg_trades — labeled and normalized TradeExecuted records.

Reads bronze_trades and keeps ALL original columns intact.
No rows are dropped here; every record is labeled with diagnostic columns:

    price_cast        : float64 version of price (coerced, NaN on failure)
    quantity_cast     : float64 version of quantity
    timestamp_utc     : timezone-aware UTC datetime (NaT on failure)
    side_normalized   : "buy" | "sell" — derived from is_aggressive_buy; "unknown"
                        when the source column is absent
    _is_duplicate     : True when (trade_id, symbol, timestamp) repeats; first
                        occurrence stays False
    _validation_status: "valid" | "invalid"
    _invalid_reasons  : pipe-separated list of triggered checks, e.g.
                        "null_price|non_positive_quantity"
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.trades import bronze_trades
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.utils.validation import build_invalid_reasons, label_duplicates


@asset(
    name="stg_trades",
    description=(
        "Labeled and normalized TradeExecuted records. "
        "All rows retained; invalids and duplicates are flagged, not dropped."
    ),
    deps=[bronze_trades],
    metadata={"layer": "staging"},
)
def stg_trades(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="trades")

    if df.empty:
        context.log.warning("No bronze trades to process.")
        return pd.DataFrame()

    # Restrict to TradeExecuted rows only (bridge injects event_type)
    if "event_type" in df.columns:
        df = df[df["event_type"] == "TradeExecuted"].copy()
    else:
        df = df.copy()

    # ── 1. Derived / cast columns (originals unchanged) ────────────────────
    df["price_cast"]    = pd.to_numeric(df["price"],    errors="coerce")
    df["quantity_cast"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    if "is_aggressive_buy" in df.columns:
        df["side_normalized"] = df["is_aggressive_buy"].map({True: "buy", False: "sell"})
    else:
        df["side_normalized"] = "unknown"

    # ── 2. Duplicate flag (mark, don't remove) ─────────────────────────────
    df["_is_duplicate"] = label_duplicates(df, subset=["trade_id", "symbol", "timestamp"])

    # ── 3. Validation reasons ───────────────────────────────────────────────
    null_price    = df["price_cast"].isna()
    null_quantity = df["quantity_cast"].isna()
    null_symbol   = df["symbol"].isna() if "symbol" in df.columns else pd.Series(True, index=df.index)
    null_ts       = df["timestamp_utc"].isna()
    neg_price     = (~null_price) & (df["price_cast"] <= 0)
    neg_quantity  = (~null_quantity) & (df["quantity_cast"] <= 0)

    df["_invalid_reasons"] = build_invalid_reasons(df, {
        "null_price":            null_price,
        "null_quantity":         null_quantity,
        "null_symbol":           null_symbol,
        "null_timestamp":        null_ts,
        "non_positive_price":    neg_price,
        "non_positive_quantity": neg_quantity,
    })
    df["_validation_status"] = df["_invalid_reasons"].apply(
        lambda r: "invalid" if r else "valid"
    )

    n_valid   = (df["_validation_status"] == "valid").sum()
    n_invalid = (df["_validation_status"] == "invalid").sum()
    n_dup     = df["_is_duplicate"].sum()
    context.log.info(
        "stg_trades: %d total | %d valid | %d invalid | %d duplicate",
        len(df), n_valid, n_invalid, n_dup,
    )

    storage.write_parquet(df, layer="staging", table="trades", context=context)
    return df
