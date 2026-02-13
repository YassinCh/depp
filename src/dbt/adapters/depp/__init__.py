from dbt.adapters.base.plugin import AdapterPlugin

from dbt.include import depp

from .adapter import DeppAdapter
from .config import DeppCredentials


def __getattr__(_name: str) -> AdapterPlugin:
    return AdapterPlugin(
        adapter=DeppAdapter,
        credentials=DeppCredentials,
        include_path=depp.PACKAGE_PATH,
    )
