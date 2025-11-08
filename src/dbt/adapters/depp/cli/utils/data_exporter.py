"""Export model data to parquet files."""

from pathlib import Path
from typing import Literal

from dbt.adapters.postgres.connections import PostgresCredentials

from ...executors import AbstractPythonExecutor


def export_to_parquet(
    table_name: str,
    output_path: Path,
    db_creds: PostgresCredentials,
    library: Literal["polars", "pandas", "geopandas"] = "polars",
) -> None:
    """Read table from database and save to parquet."""
    executor = AbstractPythonExecutor.get_executor_class(library)({}, db_creds, library)
    df = executor.read_df(table_name)
    (df.write_parquet if library == "polars" else df.to_parquet)(output_path)  # type: ignore
