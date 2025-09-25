import pandas as pd

from .abstract_executor import AbstractPythonExecutor


class PandasPythonExecutor(AbstractPythonExecutor[pd.DataFrame]):
    library_name = "pandas"

    def get_csv(self, df: pd.DataFrame):
        return df.to_csv(sep="\t", na_rep="\\N", index=False, header=False)
