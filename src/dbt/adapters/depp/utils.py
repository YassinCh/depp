import inspect
import time
from contextlib import contextmanager
from functools import wraps
from types import FrameType
from typing import TYPE_CHECKING, Any, Callable, Concatenate, ParamSpec, TypeVar

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.types import CodeExecution, CodeExecutionStatus
from dbt.adapters.factory import FACTORY
from dbt.config.project import load_raw_project
from dbt.config.renderer import ProfileRenderer
from dbt_common.events.functions import fire_event

if TYPE_CHECKING:
    from .adapter import PythonAdapter

T = TypeVar("T", bound="PythonAdapter")
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

        fire_event(CodeExecutionStatus(status=response._message, elapsed=elapsed))  # type: ignore
        return response

    return logs


def find_funcs_in_stack(funcs: set[str]) -> bool:
    frame: FrameType | None = inspect.currentframe()
    while frame:
        if frame.f_code.co_name in funcs:
            return True
        frame = frame.f_back
    return False


@contextmanager
def release_plugin_lock():
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()


def find_profile(override: str | None, root: str, rendered: ProfileRenderer):
    if override is not None:
        return override

    raw_profile = load_raw_project(root).get("profile")
    return rendered.render_value(raw_profile)


def find_target(override: str | None, profile: dict[str, Any], render: ProfileRenderer):
    if override is not None:
        return override
    if "target" in profile:
        return render.render_value(profile["target"])
    return "default"
