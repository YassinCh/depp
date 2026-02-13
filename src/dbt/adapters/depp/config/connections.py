from dataclasses import dataclass

from dbt.adapters.contracts.connection import Credentials

from .adapter_type import ADAPTER_NAME


@dataclass
class DeppCredentials(Credentials):
    """Credentials for the DEPP adapter."""

    db_profile: str = ""

    def _connection_keys(self) -> tuple[str, ...]:
        return ("db_profile",)

    @property
    def type(self) -> str:
        return ADAPTER_NAME

    @property
    def unique_field(self) -> str:
        return self.db_profile
