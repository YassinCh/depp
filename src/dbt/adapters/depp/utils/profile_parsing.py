from typing import Any

from dbt.config.project import load_raw_project
from dbt.config.renderer import ProfileRenderer


def find_profile(
    override: str | None, root: str, render: ProfileRenderer
) -> str | None:
    """Find the profile name from override or project config."""
    if override is not None:
        return override

    raw_profile = load_raw_project(root).get("profile")
    result = render.render_value(raw_profile)
    return result if isinstance(result, str) else None


def find_target(
    override: str | None, profile: dict[str, Any], render: ProfileRenderer
) -> str:
    """Find the target name from override or profile config."""
    if override is not None:
        return override
    if "target" in profile:
        result = render.render_value(profile["target"])
        return result if isinstance(result, str) else "default"
    return "default"
