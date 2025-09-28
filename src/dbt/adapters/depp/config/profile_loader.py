from argparse import Namespace
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from dbt.adapters.factory import FACTORY
from dbt.adapters.protocol import RelationProtocol
from dbt.config.profile import Profile, read_profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import get_flags

from ..utils import find_profile, find_target

if TYPE_CHECKING:
    from ..adapter import DeppAdapter


@dataclass
class DbInfo:
    profile: Profile
    override_properties: dict[str, Any]
    relation: RelationProtocol | None = None


def load_profile_info() -> DbInfo:
    """Load database profile from depp adapter configuration"""
    # TODO: some of this code feels like it could use an upgrade
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
    return DbInfo(db_profile, override_properties)


@lru_cache(maxsize=1)
def get_db_profile_info():
    db_info = load_profile_info()
    relation = FACTORY.get_relation_class_by_name(db_info.profile.credentials.type)
    return DbInfo(db_info.profile, db_info.override_properties, relation)


class RelationDescriptor:
    """Descriptor that lazily loads and caches the Relation class"""

    def __init__(self):
        self._relation = None

    def __get__(self, instance: "DeppAdapter", owner: "type[DeppAdapter]"):
        if self._relation is None:
            self._relation = get_db_profile_info().relation
        return self._relation
