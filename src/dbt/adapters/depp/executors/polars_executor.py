import polars as pl
from sqlalchemy import create_engine

from .abstract_executor import AbstractPythonExecutor
from .result import ExecutionResult


class PolarsLocalExecutor(AbstractPythonExecutor[pl.DataFrame]):
    library_name = "polars"
    handled_types = ["PolarsDbt"]

    def write_dataframe(
        self, df: pl.DataFrame, table: str, schema: str
    ) -> ExecutionResult:
        df, array_cols = self.prepare_array_columns(df)

        df.write_database(
            table_name=f"{schema}.{table}",
            connection=self.conn_string,
            if_table_exists="replace",
            engine="adbc",
        )

        if array_cols:
            engine = create_engine(self.conn_string)
            with engine.begin() as conn:
                for col, is_int in array_cols.items():
                    arr_type = "INTEGER[]" if is_int else "TEXT[]"
                    query = f"ALTER TABLE {schema}.{table} ALTER COLUMN {col} TYPE {arr_type} USING {col}::{arr_type}"
                    conn.exec_driver_sql(query)

        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def prepare_array_columns(
        self, df: pl.DataFrame
    ) -> tuple[pl.DataFrame, dict[str, bool]]:
        """Convert list columns to PostgreSQL format and return mapping of col -> is_integer."""
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
