"""Validation utilities for DEPP adapter."""

import ast
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.contracts.connection import Credentials


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


def validate_db_connection(creds: Credentials) -> ValidationResult:
    """Test database connection for any supported backend."""
    if creds.type == "postgres":
        return _validate_postgres_connection(creds)
    if creds.type == "snowflake":
        return _validate_snowflake_connection(creds)
    return ValidationResult("DB Connection", False, f"Unsupported backend: {creds.type}")


def _validate_postgres_connection(creds: Credentials) -> ValidationResult:
    try:
        import psycopg2  # type: ignore[import-untyped]

        conn = psycopg2.connect(
            host=getattr(creds, "host", "localhost"),
            port=getattr(creds, "port", 5432),
            user=getattr(creds, "user", ""),
            password=getattr(creds, "password", ""),
            database=creds.database,
            connect_timeout=5,
        )
        conn.close()
        return ValidationResult("DB Connection", True)
    except Exception as e:
        return ValidationResult("DB Connection", False, str(e))


def _validate_snowflake_connection(creds: Credentials) -> ValidationResult:
    try:
        import snowflake.connector  # type: ignore[import-untyped]

        conn = snowflake.connector.connect(
            user=getattr(creds, "user", ""),
            password=getattr(creds, "password", ""),
            account=getattr(creds, "account", ""),
            database=creds.database,
            schema=creds.schema,
            warehouse=getattr(creds, "warehouse", ""),
            login_timeout=5,
        )
        conn.cursor().execute("SELECT 1")
        conn.close()
        return ValidationResult("DB Connection", True)
    except ImportError:
        return ValidationResult(
            "DB Connection", False, "snowflake-connector-python not installed"
        )
    except Exception as e:
        return ValidationResult("DB Connection", False, str(e))
