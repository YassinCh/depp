import polars as pl

from .abstract_executor import AbstractPythonExecutor


class PolarsLocalExecutor(AbstractPythonExecutor[pl.DataFrame]):
    library_name = "polars"

    def get_csv(self, df: pl.DataFrame) -> str:
        return df.write_csv(separator="\t", null_value="\\N", include_header=False)
