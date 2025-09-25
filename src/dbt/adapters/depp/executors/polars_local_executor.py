import polars as pl

from .abstract_executor import AbstractPythonExecutor


class PolarsLocalExecutor(AbstractPythonExecutor[pl.DataFrame]):
    library_name = "polars"

    def get_csv(self, df: pl.DataFrame, table: str, schema: str) -> str:
        df.head(1).write_database(
            table_name=f"{schema}.{table}",
            connection=self.conn_string,
            if_table_exists="replace",
            engine="adbc",
        )
        return df.write_csv(separator="\t", null_value="\\N", include_header=False)
