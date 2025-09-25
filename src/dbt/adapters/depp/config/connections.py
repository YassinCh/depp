from dataclasses import dataclass
from typing import Any, Dict, Tuple

from dbt.adapters.contracts.connection import Credentials

from .adapter_type import ADAPTER_NAME


@dataclass
class DeppCredentials(Credentials):
    db_profile: str = ""

    def _connection_keys(self) -> Tuple[str, ...]:
        return ("db_profile",)

    @property
    def type(self) -> str:
        return ADAPTER_NAME

    @property
    def unique_field(self) -> str:
        return self.db_profile

    @classmethod
    def translate_aliases(
        cls, kwargs: Dict[str, Any], recurse: bool = False
    ) -> Dict[str, Any]:
        return kwargs
