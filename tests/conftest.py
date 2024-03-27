import pytest

from hymir.config import Config, set_configuration

pytest_plugins = ("celery.contrib.pytest",)


@set_configuration
def set_config(config: Config):
    config.redis_url = "redis://localhost:6379/0"


@pytest.fixture(scope="session")
def celery_enable_logging():
    return True


@pytest.fixture(scope="session")
def celery_config():
    return {
        "broker_url": "redis://localhost:6379/0",
        "result_backend": "redis://localhost:6379/0",
    }
