"""
Shared validation helpers used by the staging layer.

build_invalid_reasons(df, checks) — vectorized construction of a pipe-separated
    reason string for every row, given a dict of {reason_name: boolean_mask}.

label_duplicates(df, subset) — add a boolean _is_duplicate column; the first
    occurrence of each key is kept as False; subsequent occurrences become True.
"""

from __future__ import annotations

import pandas as pd


def build_invalid_reasons(df: pd.DataFrame, checks: dict[str, pd.Series]) -> pd.Series:
    """
    Return a pipe-separated string describing why each row is invalid.

    Parameters
    ----------
    df     : DataFrame whose index is used for alignment.
    checks : {reason_name: boolean mask}  —  True means the row has that problem.

    Returns
    -------
    pd.Series[str]  — empty string means the row is valid.
    """
    if not checks:
        return pd.Series("", index=df.index, dtype=str)

    flag_df = pd.DataFrame(checks, index=df.index).fillna(False).astype(bool)
    return flag_df.apply(
        lambda row: "|".join(flag_df.columns[row.values]), axis=1
    )


def label_duplicates(df: pd.DataFrame, subset: list[str]) -> pd.Series:
    """
    Return a boolean Series that is True for every row *after* the first
    occurrence of the key formed by `subset`.  First occurrence → False.
    Rows with any NaN in the key columns are never flagged as duplicates.
    """
    return df.duplicated(subset=subset, keep="first")
