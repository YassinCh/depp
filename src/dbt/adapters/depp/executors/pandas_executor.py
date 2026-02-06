import time
from typing import Any

import pandas as pd
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.dialects.postgresql import ARRAY

from .abstract_executor import AbstractPythonExecutor
from .result import ExecutionResult


class PandasPythonExecutor(AbstractPythonExecutor[pd.DataFrame]):
    library_name = "pandas"
    handled_types = ["PandasDbt"]

    def read_df(self, table_name: str) -> pd.DataFrame:
        """Read via connectorx (Postgres) or snowflake-connector (Snowflake)."""
        if self.db_type == "postgres":
            return super().read_df(table_name)

        start = time.perf_counter()
        source = self.get_source_info(table_name)
        query = f'SELECT * FROM "{source.schema}"."{source.table}"'
        conn = self._get_snowflake_connection()
        try:
            result = conn.cursor().execute(query).fetch_pandas_all()
        finally:
            conn.close()
        self._read_time += time.perf_counter() - start
        return result

    def write_dataframe(
        self, df: pd.DataFrame, table: str, schema: str
    ) -> ExecutionResult:
        if self.db_type == "snowflake":
            return self._write_snowflake(df, table, schema)

        engine = create_engine(self.conn_string)
        df, dtype = self._prepare_pg_array_columns(df)
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            dtype=dtype if dtype else None,  # type: ignore
        )
        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def _write_snowflake(
        self, df: pd.DataFrame, table: str, schema: str
    ) -> ExecutionResult:
        """Fast bulk write via PUT + COPY INTO (snowflake write_pandas)."""
        from snowflake.connector.pandas_tools import write_pandas  # type: ignore[import-untyped]

        conn = self._get_snowflake_connection()
        try:
            write_pandas(
                conn,
                df,
                table_name=table.upper(),
                schema=schema.upper(),
                auto_create_table=True,
                overwrite=True,
            )
        finally:
            conn.close()
        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def _get_snowflake_connection(self) -> Any:
        """Create a Snowflake connection from stored credentials."""
        import snowflake.connector  # type: ignore[import-untyped]

        return snowflake.connector.connect(
            user=getattr(self.creds, "user", ""),
            password=getattr(self.creds, "password", ""),
            account=getattr(self.creds, "account", ""),
            database=self.creds.database,
            schema=self.creds.schema,
            warehouse=getattr(self.creds, "warehouse", ""),
        )

    def _prepare_pg_array_columns(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, ARRAY[Any]]]:
        """Convert list columns to PostgreSQL ARRAY format and return dtype mapping."""
        array_dtype: dict[str, ARRAY[Any]] = {}

        for col in df.select_dtypes("object").columns:
            if not df[col].apply(isinstance, args=(list,)).any():
                continue

            sample = df[col].dropna().iloc[0] if df[col].notna().any() else None
            df[col] = df[col].apply(
                lambda x: f"{{{','.join(map(str, x))}}}" if x else None
            )
            array_dtype[col] = (
                ARRAY(Integer)
                if sample and all(isinstance(v, int) for v in sample)
                else ARRAY(String)  # type: ignore
            )

        return df, array_dtype
