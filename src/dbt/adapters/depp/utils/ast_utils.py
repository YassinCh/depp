"""Utilities for using ast to extract information out of from Python."""

import ast
import contextlib
import inspect
from typing import Optional, Union


def get_library_from_typehint(
    code: str, mapping: dict[str, str], func: str = "model"
) -> str | None:
    """Extract the library name from the model function's type annotation."""
    # TODO: find out a way to both simplify this and think about this mapping
    # TODO: Auto crete this mapping?
    for node in ast.walk(ast.parse(code)):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name == func
            and node.args.args
            and (ann := node.args.args[0].annotation)
        ):
            return mapping.get(
                (
                    str(ann.value)
                    if isinstance(ann, ast.Constant)
                    else ast.unparse(ann)
                ).split(".")[-1]
            )
    return None


def get_docstring(node: Union[ast.Module, ast.FunctionDef]) -> Optional[str]:
    """Extract docstring from node if it exists."""
    if (
        node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    ):
        return inspect.cleandoc(node.body[0].value.value)
    return None


def extract_python_docstring(file_path: str, func_name: str = "model") -> Optional[str]:
    """Extract docstring from Python model file."""
    with contextlib.suppress(Exception):
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                if doc := get_docstring(node):
                    return doc
        return get_docstring(tree)
    return None
