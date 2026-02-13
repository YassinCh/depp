"""Validation utilities for DEPP adapter."""

import ast
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.contracts.connection import Credentials
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from dbt.adapters.depp.db import get_db_ops


@dataclass
class ValidationResult:
    """Result of a validation check."""

    name: str
    passed: bool
    message: str = ""


def validate_python_syntax(source: str) -> ValidationResult:
    """Validate Python source syntax."""
    try:
        ast.parse(source)
        return ValidationResult("Syntax", True)
    except SyntaxError as e:
        return ValidationResult("Syntax", False, str(e))


def validate_type_hints(source: str) -> ValidationResult:
    """Check model function has type hints."""
    try:
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "model":
                if not node.returns or not all(a.annotation for a in node.args.args):
                    return ValidationResult("Type Hints", False, "Missing annotations")
                return ValidationResult("Type Hints", True)
        return ValidationResult("Type Hints", False, "No model() function")
    except (SyntaxError, OSError) as e:
        return ValidationResult("Type Hints", False, str(e))


def validate_model_file(file_path: Path) -> list[ValidationResult]:
    """Read file once and run syntax + type hint checks."""
    source = file_path.read_text()
    return [validate_python_syntax(source), validate_type_hints(source)]


def validate_ty(file_path: Path) -> ValidationResult:
    """Run ty type checker on file."""
    try:
        result = subprocess.run(
            ["ty", "check", str(file_path)],
            check=False, capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return ValidationResult("ty", True)
        return ValidationResult("ty", False, result.stdout.strip())
    except FileNotFoundError:
        return ValidationResult("ty", False, "ty not found")
    except OSError as e:
        return ValidationResult("ty", False, str(e))


def validate_db_connection(creds: Credentials) -> ValidationResult:
    """Test database connection."""
    try:
        db_ops = get_db_ops(creds)
        engine = create_engine(db_ops.sqlalchemy_url(creds))
        with engine.connect():
            pass
        return ValidationResult("DB Connection", True)
    except (SQLAlchemyError, OSError) as e:
        return ValidationResult("DB Connection", False, str(e))
