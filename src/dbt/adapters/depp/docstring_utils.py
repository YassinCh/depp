"""Utilities for extracting docstrings from Python model files."""

import ast
import inspect
from typing import Optional


def extract_python_docstring(file_path: str) -> Optional[str]:
    """Extract docstring from Python model file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.FunctionDef)
                and node.name == "model"
                and node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                docstring = node.body[0].value.value
                return inspect.cleandoc(docstring)

        if (
            tree.body
            and isinstance(tree.body[0], ast.Expr)
            and isinstance(tree.body[0].value, ast.Constant)
            and isinstance(tree.body[0].value.value, str)
        ):
            docstring = tree.body[0].value.value
            return inspect.cleandoc(docstring)

    except (FileNotFoundError, SyntaxError, UnicodeDecodeError, AttributeError):
        pass

    return None
