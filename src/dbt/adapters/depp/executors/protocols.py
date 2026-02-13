from dataclasses import dataclass
from typing import Any, Literal, Protocol

import pandas as pd
import polars as pl
from dbt.adapters.contracts.connection import Credentials

from dbt.adapters.depp.db import DatabaseOps

from .result import ExecutionResult


def geometry_columns(df: Any) -> list[str]:
    """Get geometry column names from a DataFrame."""
    return [str(c) for c, dt in df.dtypes.items() if str(dt) == "geometry"]


@dataclass(frozen=True)
class DbContext:
    """Bundles database operations and credentials."""

    ops: DatabaseOps
    creds: Credentials


@dataclass(frozen=True)
class SourceInfo:
    """Parsed database table reference."""

    full_name: str
    schema: str
    table: str

    @classmethod
    def parse(cls, table_name: str) -> "SourceInfo":
        """Parse a qualified table name into components."""
        parts = [p.strip('"') for p in table_name.split(".")]
        return cls(
            full_name=f"{parts[-2]}.{parts[-1]}",
            schema=parts[-2],
            table=parts[-1],
        )


class Reader(Protocol):
    """Protocol for reading data from a database into Arrow format."""

    def read_arrow(self, ctx: DbContext, source: SourceInfo) -> Any: ...


class Converter[T](Protocol):
    """Protocol for converting between Arrow and DataFrame libraries."""

    def from_arrow(self, arrow: Any) -> T: ...

    def prepare_array_columns(
        self, df: T, db_ops: DatabaseOps
    ) -> tuple[T, dict[str, bool]]: ...

    def to_polars(self, df: T) -> pl.DataFrame: ...
    def to_pandas(self, df: T) -> pd.DataFrame: ...


class Writer(Protocol):
    """Protocol for writing DataFrames to a database."""

    def write(
        self, ctx: DbContext, df: Any, source: SourceInfo
    ) -> ExecutionResult: ...


@dataclass(frozen=True)
class GeoArrowResult:
    """Arrow table with geometry column metadata."""

    table: Any
    geometry_columns: list[str]
    srid: int
    format: Literal["wkb", "geojson"]
