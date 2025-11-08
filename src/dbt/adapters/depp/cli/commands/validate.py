"""Validate Python models and configuration."""

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from dbt.adapters.postgres.connections import PostgresCredentials
from dbt.cli.main import dbtRunner
from rich.console import Console
from rich.table import Table

from ...config import DbInfo
from ...utils.validation import (
    ValidationResult,
    validate_db_connection,
    validate_mypy,
    validate_python_syntax,
    validate_type_hints,
)
from ..main import app

console = Console()


@app.command
def validate(
    model: Annotated[str | None, Parameter(help="Model file to validate")] = None,
    skip_mypy: Annotated[bool, Parameter(help="Skip mypy type checking")] = False,
    skip_db: Annotated[bool, Parameter(help="Skip database connection check")] = False,
) -> None:
    """Validate Python models and configuration."""
    results: list[ValidationResult] = []
    conn_type = "DB Connection"
    if not skip_db:
        try:
            success = dbtRunner().invoke(["parse"]).success
        except Exception as e:
            results.append(ValidationResult(conn_type, False, str(e)))

        if not success:
            results.append(
                ValidationResult(conn_type, False, "Failed to parse dbt project")
            )
        elif not isinstance(
            db_creds := DbInfo.load_profile_info().profile.credentials,
            PostgresCredentials,
        ):
            results.append(
                ValidationResult(conn_type, False, "Only PostgreSQL supported")
            )
        else:
            results.append(validate_db_connection(db_creds))

    if model:
        model_path = (
            Path("models") / model if not model.startswith("models") else Path(model)
        )
        if model_path.suffix != ".py":
            model_path = model_path.with_suffix(".py")
        if not model_path.exists():
            return console.print(f"[red]Model not found: {model_path}")
        py_files = [model_path]
    else:
        if not (models_dir := Path("models")).exists():
            return console.print("[yellow]No models/ directory found")
        if not (py_files := list(models_dir.rglob("*.py"))):
            return console.print("[yellow]No Python models found")

    for py_file in py_files:
        results.extend(
            [
                validate_python_syntax(py_file),
                validate_type_hints(py_file),
                *([] if skip_mypy else [validate_mypy(py_file)]),
            ]
        )

    table = Table(title="Validation Results")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Message")

    for result in results:
        table.add_row(
            result.name,
            "[green]✓ PASS" if result.passed else "[red]✗ FAIL",
            result.message,
        )

    console.print(table)
    if not all(r.passed for r in results):
        raise SystemExit(1)
