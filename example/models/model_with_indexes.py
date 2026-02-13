"""Example demonstrating index configuration for Python models."""

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import PolarsDbt, SessionObject


def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    """Example model showing how to configure indexes.

    This model demonstrates three types of index configurations:
    1. Unique index on a single column
    2. Hash index on a single column
    3. Multi-column index (default btree)
    """
    # Configure constraints and indexes for this model
    dbt.config(
        constraints=[
            {"type": "primary_key", "columns": ["id"]},
        ],
        indexes=[
            {"columns": ["category"], "type": "hash"},
            {"columns": ["name", "created_at"], "unique": True},
        ],
    )

    # Create sample data
    df = pl.DataFrame(
        {
            "id": range(1, 101),
            "name": [f"item_{i}" for i in range(1, 101)],
            "category": [f"cat_{i % 5}" for i in range(1, 101)],
            "value": pl.Series(range(1, 101)).cast(pl.Float64) * 10.5,
            "created_at": pl.datetime_range(
                start=pl.datetime(2024, 1, 1),
                end=pl.datetime(2024, 4, 9),
                interval="1d",
                eager=True,
            )[:100],
        }
    )

    return df
