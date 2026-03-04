"""Test partitioned parallel reads."""

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import PolarsDbt, SessionObject


def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    df = dbt.ref("test_pg_polars", partition_on="id", partition_num=4)
    return df.select("id", "name", "score")
