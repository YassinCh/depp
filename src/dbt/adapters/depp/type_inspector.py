"""Module for inspecting Python model type annotations to auto-configure library."""

import ast
import re
from typing import Optional


def extract_model_function_signature(compiled_code: str) -> Optional[str]:
    """Extract the model function signature from compiled Python code."""
    try:
        tree = ast.parse(compiled_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "model":
                if node.args.args and len(node.args.args) >= 1:
                    dbt_arg = node.args.args[0]
                    if dbt_arg.annotation:
                        return _annotation_to_string(dbt_arg.annotation)

    except (SyntaxError, ValueError):
        return _extract_signature_with_regex(compiled_code)

    return None


def _annotation_to_string(annotation: ast.AST) -> str:
    """Convert AST annotation to string."""
    if isinstance(annotation, ast.Name):
        return annotation.id
    elif isinstance(annotation, ast.Attribute):
        parts = []
        current = annotation
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        return ".".join(reversed(parts))
    elif isinstance(annotation, ast.Constant):
        return str(annotation.value)
    else:
        # For complex annotations, try to unparse (Python 3.9+)
        try:
            return ast.unparse(annotation)
        except AttributeError:
            return str(annotation)


def _extract_signature_with_regex(compiled_code: str) -> Optional[str]:
    """Fallback regex-based extraction for type annotations."""
    # Look for pattern: def model(dbt: SomeType, ...
    pattern = r"def\s+model\s*\(\s*dbt\s*:\s*([a-zA-Z_][a-zA-Z0-9_.]*)"
    match = re.search(pattern, compiled_code)
    if match:
        return match.group(1)
    return None


def infer_library_from_type(type_annotation: str) -> Optional[str]:
    """Infer the library configuration from dbt object type annotation.

    Args:
        type_annotation: The type annotation string (e.g., "PandasDbtObject")

    Returns:
        The library name ("pandas" or "polars") or None if not detected
    """
    # Clean up the type name - remove module prefixes
    type_name = (
        type_annotation.split(".")[-1] if "." in type_annotation else type_annotation
    )

    if type_name == "PandasDbtObject":
        return "pandas"
    elif type_name == "PolarsDbtObject":
        return "polars"

    return None


def inject_auto_config(compiled_code: str) -> tuple[str, Optional[str]]:
    """Inject automatic library configuration based on type annotations.

    Args:
        compiled_code: Original compiled dbt Python model code

    Returns:
        Tuple of (modified_code, detected_library) where modified_code has
        the config injection and detected_library is the inferred library
    """
    type_annotation = extract_model_function_signature(compiled_code)
    if not type_annotation:
        return compiled_code, None

    library = infer_library_from_type(type_annotation)
    if not library:
        return compiled_code, None

    # Inject the config call at the beginning of the model function
    config_injection = f'    dbt.config(library="{library}")'

    # Find the model function and inject the config call
    lines = compiled_code.split("\n")
    modified_lines = []
    in_model_function = False
    config_injected = False

    for i, line in enumerate(lines):
        modified_lines.append(line)

        # Detect the start of model function
        if line.strip().startswith("def model(") and not config_injected:
            in_model_function = True
            continue

        # Inject config after the function definition line and any docstring
        if in_model_function and not config_injected:
            stripped = line.strip()
            # Skip empty lines and docstring
            if (
                stripped
                and not stripped.startswith('"""')
                and not stripped.startswith("'''")
                and not stripped.startswith('"')
                and not stripped.endswith('"""')
                and not stripped.endswith("'''")
            ):
                # Insert config before the first real line of code
                modified_lines.insert(-1, config_injection)
                config_injected = True
                in_model_function = False

    return "\n".join(modified_lines), library
