"""Extract dependencies from dbt model nodes."""

from typing import TypedDict


class DependsOn(TypedDict, total=False):
    nodes: list[str]


class NodeDict(TypedDict, total=False):
    depends_on: DependsOn
    name: str
    database: str
    schema: str
    identifier: str
    alias: str


class ManifestDict(TypedDict, total=False):
    nodes: dict[str, NodeDict]
    sources: dict[str, NodeDict]


def get_dependencies(node: NodeDict, manifest: ManifestDict) -> list[tuple[str, str]]:
    """Get upstream dependencies as (name, table_name) tuples."""
    all_nodes: dict[str, NodeDict] = {
        **manifest.get("nodes", {}),
        **manifest.get("sources", {}),
    }
    deps = []

    for dep_id in node.get("depends_on", {}).get("nodes", []):
        if not (dep_node := all_nodes.get(dep_id)):
            continue
        name = dep_node.get("name", dep_id.split(".")[-1])
        identifier = dep_node.get("identifier", dep_node.get("alias", name))
        table = (
            f'"{dep_node.get("database")}"."{dep_node.get("schema")}"."{identifier}"'
        )
        deps.append((name, table))

    return deps
