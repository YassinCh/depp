import pandas as pd

from .abstract_executor import AbstractPythonExecutor


class PandasPythonExecutor(AbstractPythonExecutor[pd.DataFrame]):
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
        return pd.read_sql_query(query, self.engine)

    def write_df(self, table_name: str, dataframe: pd.DataFrame):
        parts = table_name.replace('"', "").split(".")
        if len(parts) == 3:
            database, schema, table = parts
        elif len(parts) == 2:
            schema, table = parts
        else:
            schema, table = "test", parts[0]

        dataframe.to_sql(
            table, self.engine, schema=schema, if_exists="replace", index=False
        )
        return f"Successfully wrote {len(dataframe)} rows to {table_name}"
