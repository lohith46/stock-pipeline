"""
stg_quotes — labeled and normalized QuoteUpdate records.

Reads bronze_quotes and keeps ALL original columns intact.
No rows are dropped; every record is annotated:

    bid, ask, bid_size, ask_size : float64 casts of best_bid / best_ask /
                                    best_bid_size / best_ask_size
                                    (originals best_* columns remain unchanged)
    timestamp_utc                : timezone-aware UTC datetime
    mid_price                    : (bid + ask) / 2  — NaN when market is invalid
    spread_bps                   : (ask − bid) / mid_price × 10 000 — NaN when invalid
    depth_imbalance              : (bid_size − ask_size) / (bid_size + ask_size) — NaN
                                    when invalid or sizes sum to zero
    _validation_status           : "valid" | "invalid"
    _invalid_reasons             : pipe-separated list of triggered checks, e.g.
                                    "null_bid|crossed_market"

Note: quotes do not have a natural surrogate key, so no _is_duplicate flag is
added here.  If the same (symbol, timestamp) appears twice it is a data
anomaly upstream; surface it in checks rather than silently dropping.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.quotes import bronze_quotes
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.utils.validation import build_invalid_reasons


@asset(
    name="stg_quotes",
    description=(
        "Labeled and normalized QuoteUpdate records. "
        "All rows retained; invalids are flagged, not dropped."
    ),
    deps=[bronze_quotes],
    metadata={"layer": "staging"},
)
def stg_quotes(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="quotes")

    if df.empty:
        context.log.warning("No bronze quotes to process.")
        return pd.DataFrame()

    df = df.copy()

    # ── 1. Cast to new columns (originals best_bid / best_ask etc. unchanged) ──
    df["bid"]      = pd.to_numeric(df.get("best_bid"),      errors="coerce")
    df["ask"]      = pd.to_numeric(df.get("best_ask"),      errors="coerce")
    df["bid_size"] = pd.to_numeric(df.get("best_bid_size"), errors="coerce")
    df["ask_size"] = pd.to_numeric(df.get("best_ask_size"), errors="coerce")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # ── 2. Derived microstructure — only populated for rows with a valid market ──
    valid_market = (
        df["bid"].notna()
        & df["ask"].notna()
        & (df["bid"] > 0)
        & (df["ask"] > df["bid"])
    )

    df["mid_price"]       = pd.NA
    df["spread_bps"]      = pd.NA
    df["depth_imbalance"] = pd.NA

    if valid_market.any():
        mid = (df.loc[valid_market, "bid"] + df.loc[valid_market, "ask"]) / 2
        df.loc[valid_market, "mid_price"]  = mid
        df.loc[valid_market, "spread_bps"] = (
            (df.loc[valid_market, "ask"] - df.loc[valid_market, "bid"]) / mid * 10_000
        )
        size_sum = df.loc[valid_market, "bid_size"] + df.loc[valid_market, "ask_size"]
        nonzero_size = valid_market & (size_sum != 0)
        if nonzero_size.any():
            df.loc[nonzero_size, "depth_imbalance"] = (
                (df.loc[nonzero_size, "bid_size"] - df.loc[nonzero_size, "ask_size"])
                / size_sum[nonzero_size[nonzero_size].index]
            )

    # ── 3. Validation reasons ────────────────────────────────────────────────
    null_bid    = df["bid"].isna()
    null_ask    = df["ask"].isna()
    null_ts     = df["timestamp_utc"].isna()
    null_symbol = df["symbol"].isna() if "symbol" in df.columns else pd.Series(True, index=df.index)
    zero_bid    = (~null_bid) & (df["bid"] <= 0)
    crossed     = (~null_bid) & (~null_ask) & (df["ask"] <= df["bid"])

    df["_invalid_reasons"] = build_invalid_reasons(df, {
        "null_bid":        null_bid,
        "null_ask":        null_ask,
        "null_timestamp":  null_ts,
        "null_symbol":     null_symbol,
        "non_positive_bid": zero_bid,
        "crossed_market":  crossed,
    })
    df["_validation_status"] = df["_invalid_reasons"].apply(
        lambda r: "invalid" if r else "valid"
    )

    context.log.info(
        "stg_quotes: %d total | %d valid | %d invalid",
        len(df),
        (df["_validation_status"] == "valid").sum(),
        (df["_validation_status"] == "invalid").sum(),
    )

    storage.write_parquet(df, layer="staging", table="quotes", context=context)
    return df
