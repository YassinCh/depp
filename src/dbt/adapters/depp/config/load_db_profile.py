from argparse import Namespace
from typing import Any

from dbt.config.profile import Profile, read_profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import get_flags

from ..utils import find_profile, find_target


def load_profile_info() -> tuple[Profile, dict[str, Any]]:
    """Load database profile from depp adapter configuration"""
    # TODO: some of this code feels like it could use an upgrade also returning tuple isn't ideal
    flags: Namespace = get_flags()  # type: ignore
    renderer = ProfileRenderer(getattr(flags, "VARS", {}))

    name = find_profile(flags.PROFILE, flags.PROJECT_DIR, renderer)
    profile = read_profile(flags.PROFILES_DIR)[name]
    target_name = find_target(flags.TARGET, profile, renderer)
    _, depp_dict = Profile.render_profile(profile, name, target_name, renderer)

    if not (db_target := depp_dict.get("db_profile")):
        raise ValueError("depp credentials must have a `db_profile` property set")

    try:
        db_profile = Profile.from_raw_profile_info(profile, name, renderer, db_target)
    except RecursionError as e:
        raise AttributeError("Cannot nest depp profiles within each other") from e

    threads = getattr(flags, "THREADS", depp_dict.get("threads") or db_profile.threads)
    override_properties = dict(threads=threads)
    return db_profile, override_properties
