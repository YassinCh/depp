import time

import polars as pl
from sqlalchemy import create_engine

from .abstract_executor import AbstractPythonExecutor
from .result import ExecutionResult


class PolarsLocalExecutor(AbstractPythonExecutor[pl.DataFrame]):
    library_name = "polars"
    handled_types = ["PolarsDbt"]

    def read_df(self, table_name: str) -> pl.DataFrame:
        """Read via connectorx (Postgres) or ADBC (Snowflake)."""
        if self.db_type == "postgres":
            return super().read_df(table_name)

        start = time.perf_counter()
        source = self.get_source_info(table_name)
        query = f'SELECT * FROM "{source.schema}"."{source.table}"'
        result = pl.read_database(query, connection=self.conn_string, engine="adbc")
        self._read_time += time.perf_counter() - start
        return result

    def write_dataframe(
        self, df: pl.DataFrame, table: str, schema: str
    ) -> ExecutionResult:
        """Write via ADBC (works for both Postgres and Snowflake)."""
        if self.db_type == "postgres":
            df, array_cols = self._prepare_pg_array_columns(df)
        else:
            array_cols = {}

        df.write_database(
            table_name=f"{schema}.{table}",
            connection=self.conn_string,
            if_table_exists="replace",
            engine="adbc",
        )

        if array_cols:
            self._cast_pg_array_columns(schema, table, array_cols)

        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def _cast_pg_array_columns(
        self, schema: str, table: str, array_cols: dict[str, bool]
    ) -> None:
        """Cast string columns back to PostgreSQL ARRAY types."""
        engine = create_engine(self.conn_string)
        with engine.begin() as conn:
            for col, is_int in array_cols.items():
                arr_type = "INTEGER[]" if is_int else "TEXT[]"
                query = f"ALTER TABLE {schema}.{table} ALTER COLUMN {col} TYPE {arr_type} USING {col}::{arr_type}"
                conn.exec_driver_sql(query)

    def _prepare_pg_array_columns(
        self, df: pl.DataFrame
    ) -> tuple[pl.DataFrame, dict[str, bool]]:
        """Convert list columns to PostgreSQL array format. Returns col -> is_integer."""
        array_cols = {}

        for col in df.columns:
            if not isinstance(df[col].dtype, pl.List):
                continue
            values = df[col].to_list()
            sample = next((v for v in values if v is not None), None)

            df = df.with_columns(
                pl.Series(
                    col,
                    [
                        "{" + ",".join(str(x) for x in v) + "}"
                        if v is not None
                        else None
                        for v in values
                    ],
                )
            )

            array_cols[col] = sample is not None and all(
                isinstance(x, int) for x in sample
            )

        return df, array_cols
