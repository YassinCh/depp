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
    from dbt.config.runtime import RuntimeConfig

    from ..adapter import DeppAdapter


@dataclass
class DbInfo:
    profile: Profile
    override_properties: dict[str, Any]
    relation: RelationProtocol | None = None

    @classmethod
    def load_profile_info(cls) -> "DbInfo":
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
            db_profile = Profile.from_raw_profile_info(
                profile, name, renderer, db_target
            )
        except RecursionError as e:
            raise AttributeError("Cannot nest depp profiles within each other") from e

        threads = getattr(
            flags, "THREADS", depp_dict.get("threads") or db_profile.threads
        )
        override_properties = dict(threads=threads)
        return cls(db_profile, override_properties)

    @classmethod
    @lru_cache(maxsize=1)
    def get_cached_with_relation(cls) -> "DbInfo":
        """Get cached DbInfo instance with relation populated"""
        db_info = cls.load_profile_info()
        relation = FACTORY.get_relation_class_by_name(db_info.profile.credentials.type)
        return cls(db_info.profile, db_info.override_properties, relation)

    def apply_overrides(self, config: "RuntimeConfig") -> None:
        """Apply override properties to the given config object"""
        if self.override_properties:
            for key, value in self.override_properties.items():
                if value is not None:
                    setattr(config, key, value)


class RelationDescriptor:
    """Descriptor that lazily loads and caches the Relation class"""

    def __init__(self):
        self._relation = None

    def __get__(self, instance: "DeppAdapter", owner: "type[DeppAdapter]"):
        if self._relation is None:
            self._relation = DbInfo.get_cached_with_relation().relation
        return self._relation
