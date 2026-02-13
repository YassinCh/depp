"""Export model data to parquet files."""

from pathlib import Path
from typing import Literal

from dbt.adapters.contracts.connection import Credentials

from dbt.adapters.depp.executors import Executor


def export_to_parquet(
    table_name: str,
    output_path: Path,
    db_creds: Credentials,
    library: Literal["polars", "pandas", "geopandas"] = "polars",
) -> None:
    """Read table from database and save to parquet."""
    executor = Executor.create(library, db_creds.type, db_creds)
    df = executor.read_df(table_name)
    if library == "polars":
        df.write_parquet(output_path)
    else:
        df.to_parquet(output_path)
