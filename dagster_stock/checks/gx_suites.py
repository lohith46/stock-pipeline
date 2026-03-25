"""
Shared Great Expectations (GX 1.x) helper for Dagster asset checks.

GX 1.x API differences from 0.18:
  - context.sources  → context.data_sources
  - Expectations are class instances, not validator method calls
  - Validation runs via ValidationDefinition.run()

_SuiteBuilder wraps an ExpectationSuite and exposes validator-style methods
so the build(v) callbacks in silver_checks / gold_checks stay unchanged.
"""
from __future__ import annotations

from typing import Callable

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity


class _SuiteBuilder:
    """Proxy that translates validator-style calls into GX 1.x suite.add_expectation() calls."""

    def __init__(self, suite: gx.ExpectationSuite) -> None:
        self._suite = suite

    def expect_column_values_to_not_be_null(self, column: str) -> None:
        self._suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=column))

    def expect_column_values_to_be_between(
        self, column: str, min_value=None, max_value=None
    ) -> None:
        self._suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column=column, min_value=min_value, max_value=max_value
            )
        )

    def expect_column_values_to_be_in_set(self, column: str, value_set) -> None:
        self._suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(column=column, value_set=list(value_set))
        )

    def expect_column_values_to_be_unique(self, column: str) -> None:
        self._suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=column))


def _failure_label(r) -> str:
    cfg = r.expectation_config
    exp_type = getattr(cfg, "type", type(cfg).__name__)
    col = getattr(cfg, "column", None) or getattr(cfg, "column_list", "—")
    return f"{exp_type}({col})"


def run_suite(
    df: pd.DataFrame,
    suite_name: str,
    build_expectations: Callable,
    severity: AssetCheckSeverity = AssetCheckSeverity.ERROR,
) -> AssetCheckResult:
    """
    Spin up an ephemeral GX 1.x context, load df as a batch, run expectations,
    and return a Dagster AssetCheckResult with failure details in metadata.
    """
    ctx = gx.get_context(mode="ephemeral")

    ds = ctx.data_sources.add_pandas(suite_name)
    asset = ds.add_dataframe_asset(suite_name)
    batch_def = asset.add_batch_definition_whole_dataframe(suite_name)

    suite = ctx.suites.add(gx.ExpectationSuite(name=suite_name))
    build_expectations(_SuiteBuilder(suite))

    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name=suite_name, data=batch_def, suite=suite)
    )
    result = vd.run(batch_parameters={"dataframe": df})

    failed = [r for r in result.results if not r.success]
    failure_labels = [_failure_label(r) for r in failed]

    return AssetCheckResult(
        passed=bool(result.success),
        severity=severity,
        metadata={
            "expectations_evaluated": len(result.results),
            "expectations_failed": len(failed),
            "failures": str(failure_labels) if failure_labels else "none",
        },
    )
