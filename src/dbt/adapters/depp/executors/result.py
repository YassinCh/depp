from dataclasses import dataclass


@dataclass
class ExecutionResult:
    """Result from executing a dbt Python model."""

    rows_affected: int
    schema: str
    table: str
    read_time: float = 0.0
    transform_time: float = 0.0
    write_time: float = 0.0

    def __str__(self) -> str:
        """Format the result as a summary string."""
        timing = (
            f" E:{self.read_time:.1f}s"
            f" T:{self.transform_time:.1f}s"
            f" L:{self.write_time:.1f}s"
            if self.read_time > 0 or self.write_time > 0
            else ""
        )
        return f"SELECT {self.rows_affected:,}{timing}"
