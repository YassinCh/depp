from typing import Any, Optional

from dbt.adapters.contracts.connection import Credentials

from ..utils import find_funcs_in_stack
from .adapter_type import ADAPTER_NAME


class DeppCredentialsWrapper:
    _db_creds: Optional[Credentials] = None

    def __init__(self, db_creds: Credentials):
        self._db_creds = db_creds

    @property
    def type(self) -> str:
        # TODO: duplicate logic also present in adapter_type.py
        if find_funcs_in_stack({"to_target_dict", "db_materialization"}):
            return self.db_creds.type
        return ADAPTER_NAME

    @property
    def db_creds(self) -> Credentials:
        if self._db_creds is None:
            raise ValueError("No valid DB Credentials")
        return self._db_creds

    def __getattr__(self, name: str) -> Any:
        """Directly proxy to the DB adapter"""
        return getattr(self._db_creds, name)
