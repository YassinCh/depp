from dbt.adapters.base.plugin import AdapterPlugin

from ...include import depp  # type: ignore
from .adapter import PythonAdapter
from .config import DeppCredentials


def __getattr__(name: str):
    return AdapterPlugin(
        adapter=PythonAdapter,  # type: ignore
        credentials=DeppCredentials,
        include_path=depp.PACKAGE_PATH,
    )
