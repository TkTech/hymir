import dataclasses
import functools
from functools import cached_property
from threading import local
from typing import Callable

from redis import StrictRedis

_config_local = local()


@dataclasses.dataclass
class Config:
    redis_url: str = "redis://localhost:6379/0"

    @cached_property
    def redis(self) -> StrictRedis:
        """
        Get the backend for the current configuration.
        """
        return StrictRedis.from_url(self.redis_url)


def get_configuration():
    """
    Get the current global configuration.
    """
    config = getattr(_config_local, "config", None)
    if not config:
        config = Config()
        setter = getattr(_config_local, "setter", None)
        if setter:
            setter(config)
        setattr(_config_local, "config", config)

    return config


def set_configuration(f: Callable[[Config], None]):
    """
    Decorator to set a provider for configuration values.
    """
    setattr(_config_local, "setter", f)

    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return _wrapper
