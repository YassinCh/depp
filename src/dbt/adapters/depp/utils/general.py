import inspect
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType

from dbt.adapters.factory import FACTORY


def find_funcs_in_stack(funcs: set[str]) -> bool:
    """Check if any of the given function names appear in the call stack."""
    frame: FrameType | None = inspect.currentframe()
    while frame:
        if frame.f_code.co_name in funcs:
            return True
        frame = frame.f_back
    return False


@contextmanager
def release_plugin_lock() -> Iterator[None]:
    """Temporarily release the dbt plugin factory lock."""
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()
