import ast
from typing import Optional

LIBRARY_MAP = {"PandasDbtObject": "pandas", "PolarsDbtObject": "polars"}


def get_library_from_model(compiled_code: str) -> Optional[str]:
    """Extract the library name from the model function's type annotation."""
    for node in ast.walk(ast.parse(compiled_code)):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name == "model"
            and node.args.args
            and (ann := node.args.args[0].annotation)
        ):
            return LIBRARY_MAP.get(
                (
                    str(ann.value)
                    if isinstance(ann, ast.Constant)
                    else ast.unparse(ann)
                ).split(".")[-1]
            )
    return None
