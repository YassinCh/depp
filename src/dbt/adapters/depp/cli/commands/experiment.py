"""Export upstream dependencies to parquet and generate marimo notebook."""

from pathlib import Path
from typing import Annotated, Literal, cast

from cyclopts import Parameter
from dbt.adapters.postgres.connections import PostgresCredentials
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from rich.console import Console

from ...config import DbInfo, ModelConfig
from ..main import app
from ..utils import export_to_parquet, generate_notebook, get_dependencies

console = Console()


@app.command
def experiment(
    model: Annotated[str, Parameter(help="Python model name")],
    execute: Annotated[bool, Parameter(help="Execute upstream models fresh")] = False,
    profile: Annotated[str | None, Parameter(help="Profile to use")] = None,
    output_dir: Annotated[Path, Parameter(help="Output path")] = Path("experimenting"),
) -> None:
    """Export upstream dependencies to parquet and generate marimo notebook."""
    data_dir = output_dir / "data"
    notebook_path = output_dir / f"{model}.py"

    parse_args = ["parse"] + (["--profile", profile] if profile else [])
    res = dbtRunner().invoke(parse_args)

    if not res.success or not res.result:
        return console.print(f"[red]Error: Failed to parse dbt: {res.exception}")

    manifest = cast(Manifest, res.result)
    if not (
        node := next((n for n in manifest.nodes.values() if n.name == model), None)
    ):
        return console.print(f"[red]Error: Model '{model}' not found")
    if node.resource_type.value != "model":
        return console.print(f"[red]Error: '{model}' is not a model")
    if not (deps := get_dependencies(node.to_dict(), manifest.to_dict())):  # type: ignore[arg-type]
        return console.print(f"[red]Model '{model}' has no dependencies to export")

    code = getattr(node, "compiled_code", None) or getattr(node, "raw_code", "")
    config = ModelConfig.from_model(node.to_dict(), code)  # type: ignore[arg-type]

    if execute:
        console.print(f"[blue]Executing {len(deps)} upstream dependencies...")
        run_args = ["run", "-s", f"+{model}", "--exclude", model] + (
            ["--profile", profile] if profile else []
        )
        run_res = dbtRunner().invoke(run_args)
        if not run_res.success:
            return console.print("[red]Failed to execute upstream models")
        console.print("[green]Upstream models executed successfully\n")

    data_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"Exporting {len(deps)} dependencies for {model} ({config.library})")

    db_creds = cast(PostgresCredentials, DbInfo.load_profile_info().profile.credentials)
    library = cast(Literal["polars", "pandas", "geopandas"], config.library)

    for name, table in deps:
        console.print(f"  - {name} → {(parquet_path := data_dir / f'{name}.parquet')}")
        export_to_parquet(table, parquet_path, db_creds, library)

    generate_notebook(notebook_path, deps, config.library)
    console.print(f"[green]Generated notebook run with: marimo edit {notebook_path}")
