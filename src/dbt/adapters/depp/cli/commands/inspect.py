"""Inspect Python model configuration and dependencies."""

from typing import Annotated, cast

from cyclopts import Parameter
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from rich.console import Console
from rich.table import Table

from ...config import ModelConfig
from ..main import app
from ..utils import get_dependencies

console = Console()


@app.command
def inspect(
    model: Annotated[str, Parameter(help="Model name to inspect")],
    profile: Annotated[str | None, Parameter(help="Profile to use")] = None,
) -> None:
    """Inspect model configuration, dependencies, and compiled code."""
    res = dbtRunner().invoke(["parse"] + (["--profile", profile] if profile else []))

    if not res.success or not res.result:
        return console.print(f"[red]Failed to parse dbt: {res.exception}")

    manifest = cast(Manifest, res.result)
    if not (
        node := next((n for n in manifest.nodes.values() if n.name == model), None)
    ):
        return console.print(f"[red]Model '{model}' not found")
    if node.resource_type.value != "model":
        return console.print(f"[red]'{model}' is not a model")

    config = ModelConfig.from_model(
        node.to_dict(),
        getattr(node, "compiled_code", None) or getattr(node, "raw_code", ""),  # type: ignore[arg-type]
    )

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
        return console.print("\n[dim]No dependencies found")

    dep_table = Table(title="Dependencies")
    dep_table.add_column("Name", style="green")
    dep_table.add_column("Table", style="dim")
    for name, table in deps:
        dep_table.add_row(name, table)
    console.print(dep_table)
