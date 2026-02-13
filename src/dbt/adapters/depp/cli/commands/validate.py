"""Validate Python models and configuration."""

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from dbt.cli.main import dbtRunner
from rich.console import Console
from rich.table import Table

from dbt.adapters.depp.cli.main import app
from dbt.adapters.depp.config import DbInfo
from dbt.adapters.depp.utils.validation import (
    ValidationResult,
    validate_db_connection,
    validate_model_file,
    validate_ty,
)

console = Console()


def check_db(skip_db: bool) -> list[ValidationResult]:
    """Run database connection validation."""
    if skip_db:
        return []
    conn_type = "DB Connection"
    try:
        success = dbtRunner().invoke(["parse"]).success
    except Exception as e:
        return [ValidationResult(conn_type, False, str(e))]
    if not success:
        return [ValidationResult(conn_type, False, "Failed to parse dbt project")]
    db_creds = DbInfo.load_profile_info().profile.credentials
    return [validate_db_connection(db_creds)]


def find_model_files(model: str | None) -> list[Path] | None:
    """Resolve model files to validate, or None on error."""
    if model:
        model_path = (
            Path("models") / model if not model.startswith("models") else Path(model)
        )
        if model_path.suffix != ".py":
            model_path = model_path.with_suffix(".py")
        if not model_path.exists():
            console.print(f"[red]Model not found: {model_path}")
            return None
        return [model_path]
    if not (models_dir := Path("models")).exists():
        console.print("[yellow]No models/ directory found")
        return None
    py_files = list(models_dir.rglob("*.py"))
    if not py_files:
        console.print("[yellow]No Python models found")
        return None
    return py_files


def print_results(results: list[ValidationResult]) -> None:
    """Print validation results as a rich table."""
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


@app.command
def validate(
    model: Annotated[str | None, Parameter(help="Model file to validate")] = None,
    skip_ty: Annotated[bool, Parameter(help="Skip ty type checking")] = False,
    skip_db: Annotated[bool, Parameter(help="Skip database connection check")] = False,
) -> None:
    """Validate Python models and configuration."""
    results = check_db(skip_db)
    py_files = find_model_files(model)
    if py_files is None:
        return
    for py_file in py_files:
        results.extend(validate_model_file(py_file))
        if not skip_ty:
            results.append(validate_ty(py_file))
    print_results(results)
