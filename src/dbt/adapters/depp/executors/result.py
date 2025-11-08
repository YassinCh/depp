from dataclasses import dataclass


@dataclass
class ExecutionResult:
    """Result from executing a dbt Python model."""

    rows_affected: int
    schema: str
    table: str

    def __str__(self) -> str:
        return f"SELECT {self.rows_affected:,}"
