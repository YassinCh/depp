"""Inspect Python model configuration and dependencies."""

from typing import Annotated

from cyclopts import Parameter
from rich.console import Console
from rich.table import Table

from dbt.adapters.depp.cli.main import app
from dbt.adapters.depp.cli.utils import get_dependencies, parse_model_config

console = Console()


@app.command
def inspect(
    model: Annotated[str, Parameter(help="Model name to inspect")],
    profile: Annotated[str | None, Parameter(help="Profile to use")] = None,
) -> None:
    """Inspect model configuration, dependencies, and compiled code."""
    result = parse_model_config(model, console, profile)
    if not result:
        return
    manifest, node, config = result

    config_table = Table(title=f"Model: {model}", show_header=False)
    config_table.add_column("Property", style="cyan")
    config_table.add_column("Value", style="yellow")
    for prop, value in [
        ("Library", config.library),
        ("Schema", node.schema),
        ("Database", node.database),
        ("Materialized", node.config.materialized),
        ("Alias", node.alias or "None"),
    ]:
        config_table.add_row(prop, value)
    console.print(config_table)

    if not (deps := get_dependencies(node.to_dict(), manifest.to_dict())):  # type: ignore[arg-type]
        console.print("\n[dim]No dependencies found")
        return

    dep_table = Table(title="Dependencies")
    dep_table.add_column("Name", style="green")
    dep_table.add_column("Table", style="dim")
    for name, table in deps:
        dep_table.add_row(name, table)
    console.print(dep_table)
