"""Shared helpers for parsing dbt projects and finding model nodes."""

from typing import Any, cast

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from rich.console import Console

from dbt.adapters.depp.config import ModelConfig


def parse_and_find_model(
    model: str,
    console: Console,
    profile: str | None = None,
) -> tuple[Manifest, Any] | None:
    """Parse dbt project and find a model node by name.

    Returns (manifest, node) or prints error and returns None.
    """
    parse_args = ["parse"] + (["--profile", profile] if profile else [])
    res = dbtRunner().invoke(parse_args)

    if not res.success or not res.result:
        console.print(f"[red]Error: Failed to parse dbt: {res.exception}")
        return None

    manifest = cast(Manifest, res.result)
    node = next((n for n in manifest.nodes.values() if n.name == model), None)
    if not node:
        console.print(f"[red]Error: Model '{model}' not found")
        return None
    if node.resource_type.value != "model":
        console.print(f"[red]Error: '{model}' is not a model")
        return None
    return manifest, node


def parse_model_config(
    model: str, console: Console, profile: str | None = None
) -> tuple[Manifest, Any, ModelConfig] | None:
    """Parse project, find model, and resolve its config."""
    result = parse_and_find_model(model, console, profile)
    if not result:
        return None
    manifest, node = result
    code = str(getattr(node, "compiled_code", None) or getattr(node, "raw_code", ""))
    config = ModelConfig.from_model(node.to_dict(), code)
    return manifest, node, config
