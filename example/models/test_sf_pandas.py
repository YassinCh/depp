"""Stress test Snowflake Pandas executor - reads 1M rows from polars model."""

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import PandasDbt, SessionObject


def model(dbt: "PandasDbt", session: "SessionObject") -> pd.DataFrame:
    df = dbt.ref("test_sf_polars")
    df.columns = df.columns.str.lower()
    df["doubled_score"] = df["score"] * 2
    return df
