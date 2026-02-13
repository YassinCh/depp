"""Test Postgres Polars executor - 1M rows."""

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import PolarsDbt, SessionObject


def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    n = 1_000_000
    return pl.DataFrame(
        {
            "id": range(n),
            "name": [f"user_{i}" for i in range(n)],
            "score": [i * 0.1 for i in range(n)],
            "tags": [["a", "b"]] * n,
        }
    )
