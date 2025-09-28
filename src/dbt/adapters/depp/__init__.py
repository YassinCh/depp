from dbt.adapters.base.plugin import AdapterPlugin

from ...include import depp  # type: ignore
from .adapter import DeppAdapter
from .config import DeppCredentials


def __getattr__(name: str) -> AdapterPlugin:
    return AdapterPlugin(
        adapter=DeppAdapter,  # type: ignore
        credentials=DeppCredentials,
        include_path=depp.PACKAGE_PATH,
    )
