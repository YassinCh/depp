from dbt.adapters.base.plugin import AdapterPlugin

# TODO: fix this
from ...include import depp
from .adapter import PythonAdapter
from .config import DeppCredentials


def __getattr__(name: str):
    return AdapterPlugin(
        adapter=PythonAdapter,  # type: ignore
        credentials=DeppCredentials,
        include_path=depp.PACKAGE_PATH,
    )
