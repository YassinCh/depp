"""Export upstream dependencies to parquet and generate marimo notebook."""

from pathlib import Path
from typing import Annotated, Literal, cast

from cyclopts import Parameter
from dbt.cli.main import dbtRunner
from rich.console import Console

from dbt.adapters.depp.cli.main import app
from dbt.adapters.depp.cli.utils import (
    export_to_parquet,
    generate_notebook,
    get_dependencies,
    parse_model_config,
)
from dbt.adapters.depp.config import DbInfo

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

    result = parse_model_config(model, console, profile)
    if not result:
        return
    manifest, node, config = result

    if not (deps := get_dependencies(node.to_dict(), manifest.to_dict())):  # type: ignore[arg-type]
        console.print(f"[red]Model '{model}' has no dependencies to export")
        return

    if execute:
        console.print(f"[blue]Executing {len(deps)} upstream dependencies...")
        run_args = ["run", "-s", f"+{model}", "--exclude", model] + (
            ["--profile", profile] if profile else []
        )
        run_res = dbtRunner().invoke(run_args)
        if not run_res.success:
            console.print("[red]Failed to execute upstream models")
            return
        console.print("[green]Upstream models executed successfully\n")

    data_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"Exporting {len(deps)} dependencies for {model} ({config.library})")

    db_info = DbInfo.load_profile_info()
    db_creds = db_info.profile.credentials
    library = cast(Literal["polars", "pandas", "geopandas"], config.library)

    for name, table in deps:
        console.print(f"  - {name} → {(parquet_path := data_dir / f'{name}.parquet')}")
        export_to_parquet(table, parquet_path, db_creds, library)

    generate_notebook(notebook_path, deps, config.library)
    console.print(f"[green]Generated notebook run with: marimo edit {notebook_path}")
