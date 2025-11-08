"""Validation utilities for DEPP adapter."""

import ast
import subprocess
from dataclasses import dataclass
from pathlib import Path

import psycopg2  # type: ignore
from dbt.adapters.postgres.connections import PostgresCredentials


@dataclass
class ValidationResult:
    """Result of a validation check."""

    name: str
    passed: bool
    message: str = ""


def validate_python_syntax(file_path: Path) -> ValidationResult:
    """Validate Python file syntax."""
    try:
        ast.parse(file_path.read_text())
        return ValidationResult("Syntax", True)
    except SyntaxError as e:
        return ValidationResult("Syntax", False, str(e))


def validate_type_hints(file_path: Path) -> ValidationResult:
    """Check model function has type hints."""
    try:
        tree = ast.parse(file_path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "model":
                if not node.returns or not all(a.annotation for a in node.args.args):
                    return ValidationResult("Type Hints", False, "Missing annotations")
                return ValidationResult("Type Hints", True)
        return ValidationResult("Type Hints", False, "No model() function")
    except Exception as e:
        return ValidationResult("Type Hints", False, str(e))


def validate_mypy(file_path: Path) -> ValidationResult:
    """Run mypy strict on file."""
    try:
        result = subprocess.run(
            ["mypy", "--strict", str(file_path)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return ValidationResult("Mypy", True)
        return ValidationResult("Mypy", False, result.stdout.strip())
    except FileNotFoundError:
        return ValidationResult("Mypy", False, "mypy not found")
    except Exception as e:
        return ValidationResult("Mypy", False, str(e))


def validate_db_connection(creds: PostgresCredentials) -> ValidationResult:
    """Test database connection."""
    try:
        conn = psycopg2.connect(
            host=creds.host,
            port=creds.port,
            user=creds.user,
            password=creds.password,
            database=creds.database,
            connect_timeout=5,
        )
        conn.close()
        return ValidationResult("DB Connection", True)
    except Exception as e:
        return ValidationResult("DB Connection", False, str(e))
