import polars as pl

from .abstract_executor import AbstractPythonExecutor


class PolarsLocalExecutor(AbstractPythonExecutor[pl.DataFrame]):
    def read_df(self, table_name: str):
        parts = table_name.replace('"', "").split(".")
        if len(parts) == 3:
            database, schema, table = parts
            query = f'SELECT * FROM "{schema}"."{table}"'
        elif len(parts) == 2:
            schema, table = parts
            query = f'SELECT * FROM "{schema}"."{table}"'
        else:
            table = parts[0]
            query = f'SELECT * FROM "{table}"'

        return pl.read_database(query, self.engine)

    def write_df(self, table_name: str, dataframe: pl.DataFrame):
        parts = table_name.replace('"', "").split(".")
        if len(parts) == 3:
            database, schema, table = parts
        elif len(parts) == 2:
            schema, table = parts
        else:
            schema, table = "test", parts[0]

        dataframe.write_database(
            table_name=table,
            connection=self.engine,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

        return f"Successfully wrote {len(dataframe)} rows to {table_name}"
