"""
ref_agent_type_map — canonical mapping of raw agent_type values to their
normalized pipeline representations.

This is a static reference table derived from the mock-stock specification.
It exists so that any asset that needs to normalise or validate agent_type can
JOIN against a single authoritative source rather than embedding the mapping
inline.

Columns
-------
raw_value        : exact string as it arrives from mock-stock / Kafka
normalized_value : pipeline-standard short code used in silver / gold layers
display_name     : human-readable label for dashboards / reports
"""

import pandas as pd
from dagster import asset


_ROWS = [
    ("RetailTrader",          "retail",        "Retail Trader"),
    ("InstitutionalInvestor", "institutional", "Institutional Investor"),
    ("MarketMaker",           "market_maker",  "Market Maker"),
    ("HighFrequencyTrader",   "hft",           "High-Frequency Trader"),
]


@asset(
    name="ref_agent_type_map",
    description=(
        "Static reference table mapping raw mock-stock agent_type strings to "
        "their normalized pipeline codes and display names."
    ),
    metadata={"layer": "reference"},
)
def ref_agent_type_map() -> pd.DataFrame:
    return pd.DataFrame(_ROWS, columns=["raw_value", "normalized_value", "display_name"])
