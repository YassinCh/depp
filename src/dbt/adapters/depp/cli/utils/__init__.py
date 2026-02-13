"""Utility functions for the dbt-depp CLI."""

from pathlib import Path

from rich.console import Console
from rich.prompt import Prompt

from .data_exporter import export_to_parquet
from .dbt_parser import parse_and_find_model, parse_model_config
from .dependency_extractor import get_dependencies
from .marimo_generator import generate_notebook
from .templates import render_template


def confirm_overwrite(path: Path, console: Console) -> bool:
    """Prompt user to confirm file overwrite. Returns True to proceed."""
    if not path.exists():
        return True
    answer = Prompt.ask(
        f"[yellow]{path} exists. Overwrite?[/yellow]", choices=["y", "n"], default="n"
    )
    if answer != "y":
        console.print("[red]Cancelled")
        return False
    return True


__all__ = [
    "confirm_overwrite",
    "export_to_parquet",
    "generate_notebook",
    "get_dependencies",
    "parse_and_find_model",
    "parse_model_config",
    "render_template",
]
