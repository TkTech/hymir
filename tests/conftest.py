import pytest

from hymir.config import Config, set_configuration
from hymir.executors.celery import CeleryExecutor
from hymir.executors.debug import DebugExecutor

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


@pytest.fixture(params=[CeleryExecutor, DebugExecutor], scope="session")
def executor(request, celery_session_worker):
    return request.param()
