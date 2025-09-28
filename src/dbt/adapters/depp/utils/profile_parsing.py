from typing import Any

from dbt.config.project import load_raw_project
from dbt.config.renderer import ProfileRenderer


def find_profile(override: str | None, root: str, render: ProfileRenderer):
    if override is not None:
        return override

    raw_profile = load_raw_project(root).get("profile")
    return render.render_value(raw_profile)


def find_target(override: str | None, profile: dict[str, Any], render: ProfileRenderer):
    if override is not None:
        return override
    if "target" in profile:
        return render.render_value(profile["target"])
    return "default"
