from functools import partial
from typing import TYPE_CHECKING

from ..utils import find_funcs_in_stack

if TYPE_CHECKING:
    from ..adapter import PythonAdapter

ADAPTER_NAME = "depp"


class AdapterTypeDescriptor:
    # TODO: can we use this in a more general way for all type things like connections?
    type_str: str = ADAPTER_NAME

    def __get__(
        self, obj: "PythonAdapter | None", objtype: type["PythonAdapter"] | None = None
    ):
        def _type(instance: "PythonAdapter | None" = None):
            if instance is None:
                return ADAPTER_NAME
            if find_funcs_in_stack({"render", "db_materialization"}):
                return instance.db_adapter.type()
            return ADAPTER_NAME

        return partial(_type, obj) if obj else _type
