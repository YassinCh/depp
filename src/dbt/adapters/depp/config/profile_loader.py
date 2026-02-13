from argparse import Namespace
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from dbt.adapters.factory import FACTORY
from dbt.adapters.protocol import RelationProtocol
from dbt.config.profile import Profile, read_profile
from dbt.config.renderer import ProfileRenderer
from dbt.config.runtime import RuntimeConfig
from dbt.flags import get_flags

from dbt.adapters.depp.utils import find_profile, find_target


@dataclass
class DbInfo:
    """Database connection info loaded from profile configuration."""

    profile: Profile
    override_properties: dict[str, Any]
    relation: RelationProtocol | None = None

    @classmethod
    def load_profile_info(cls) -> "DbInfo":
        """Load database profile from depp adapter configuration."""
        flags: Namespace = get_flags()
        renderer = ProfileRenderer(getattr(flags, "VARS", {}))

        name = find_profile(flags.PROFILE, flags.PROJECT_DIR, renderer)
        if name is None:
            raise ValueError("Profile name not found")
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
        override_properties = {"threads": threads}
        return cls(db_profile, override_properties)

    @classmethod
    @lru_cache(maxsize=1)
    def get_cached_with_relation(cls) -> "DbInfo":
        """Get cached DbInfo instance with relation populated."""
        db_info = cls.load_profile_info()
        relation = FACTORY.get_relation_class_by_name(db_info.profile.credentials.type)
        return cls(db_info.profile, db_info.override_properties, relation)

    def apply_overrides(self, config: RuntimeConfig) -> None:
        """Apply override properties to the given config object."""
        if self.override_properties:
            for key, value in self.override_properties.items():
                if value is not None:
                    setattr(config, key, value)
