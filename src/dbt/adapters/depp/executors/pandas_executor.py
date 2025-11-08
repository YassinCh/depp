from typing import Any

import pandas as pd
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.dialects.postgresql import ARRAY

from .abstract_executor import AbstractPythonExecutor
from .result import ExecutionResult


class PandasPythonExecutor(AbstractPythonExecutor[pd.DataFrame]):
    library_name = "pandas"
    handled_types = ["PandasDbt"]

    def write_dataframe(
        self, df: pd.DataFrame, table: str, schema: str
    ) -> ExecutionResult:
        engine = create_engine(self.conn_string)

        df, dtype = self.prepare_array_columns(df)

        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            dtype=dtype if dtype else None,  # type: ignore
        )
        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def prepare_array_columns(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, ARRAY[Any]]]:
        """Convert list columns to PostgreSQL format and return dtype mapping."""
        # TODO: Duplicate code and should be
        array_dtype = {}

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
