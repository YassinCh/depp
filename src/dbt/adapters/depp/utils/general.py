import inspect
import time
from contextlib import contextmanager
from functools import wraps
from types import FrameType
from typing import TYPE_CHECKING, Callable, Concatenate, Iterator, ParamSpec, TypeVar

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.types import CodeExecution, CodeExecutionStatus
from dbt.adapters.factory import FACTORY
from dbt_common.events.functions import fire_event

if TYPE_CHECKING:
    from ..adapter import DeppAdapter

T = TypeVar("T", bound="DeppAdapter")
P = ParamSpec("P")

funcT = Callable[Concatenate[T, P], AdapterResponse]


def logs(func: funcT[T, P]) -> funcT[T, P]:
    """Decorator for python executor methods to log"""

    @wraps(func)
    def logs(self: T, *args: P.args, **kwargs: P.kwargs) -> AdapterResponse:
        connection_name = self.connections.get_thread_connection().name
        compiled_code = args[1]
        fire_event(CodeExecution(conn_name=connection_name, code_content=compiled_code))

        start_time = time.time()
        response = func(self, *args, **kwargs)
        elapsed = round((time.time() - start_time), 2)

        fire_event(CodeExecutionStatus(status=response.__str__(), elapsed=elapsed))  # type: ignore
        return response

    return logs  # type: ignore[return-value]


def find_funcs_in_stack(funcs: set[str]) -> bool:
    frame: FrameType | None = inspect.currentframe()
    while frame:
        if frame.f_code.co_name in funcs:
            return True
        frame = frame.f_back
    return False


@contextmanager
def release_plugin_lock() -> Iterator[None]:
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()
