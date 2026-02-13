"""Profiling utilities for DEPP adapter."""

import time
from dataclasses import dataclass


@dataclass
class ProfilingMetrics:
    """Profiling metrics for model execution."""

    read_time: float = 0.0
    transform_time: float = 0.0
    write_time: float = 0.0
    total_time: float = 0.0
    rows_read: int = 0
    rows_written: int = 0
    memory_mb: float = 0.0


class Timer:
    """Simple context manager for timing operations."""

    def __init__(self) -> None:
        self.start: float = 0.0
        self.elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args: object) -> None:
        self.elapsed = time.perf_counter() - self.start
