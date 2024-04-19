import dataclasses
import functools
from functools import cached_property, lru_cache
from typing import Callable, Optional

from redis import StrictRedis


@dataclasses.dataclass
class Config:
    redis_url: str = "redis://localhost:6379/0"

    @cached_property
    def redis(self) -> StrictRedis:
        """
        Get the backend for the current configuration.
        """
        return StrictRedis.from_url(self.redis_url)


_config: Optional[Config] = None
_setter: Optional[Callable[[Config], None]] = None


@lru_cache
def get_configuration() -> Config:
    """
    Get the current global configuration.
    """
    global _config
    global _setter

    if not _config:
        _config = Config()
        if _setter:
            _setter(_config)

    return _config


def set_configuration(f: Callable[[Config], None]):
    """
    Decorator to set a provider for configuration values.
    """
    global _setter

    _setter = f
    get_configuration.cache_clear()

    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return _wrapper
