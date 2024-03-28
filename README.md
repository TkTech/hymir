# Hymir

Hymir is a python library for creating and running simple workflows by composing
together Chains (which run sequentially) and Groups (which run in parallel) of
jobs.

Hymir is built on top of [redis][] for intermediate storage and tracking
workflow state, and comes with an Executor for running the workflows using
[Celery][].

## Installation

```bash
pip install hymir
```

## Usage

Using the `@job` decorator is the simplest way to define a job. The decorated
function should return a `Success`, `Failure`, `Retry`, or `CheckLater` object.
The only limitations are:

- The decorated function must be importable from other files
  (`from mymodule import myjob` should work)
- The outputs and inputs of a job must be JSON-safe.

When jobs depend on the output of other jobs, you can specify the inputs of the
job using the `inputs` parameter of the `@job` decorator and the output of the
job using the `output` parameter.

```python
from hymir.job import Success
from hymir.config import Config, set_configuration
from hymir.executors.celery import CeleryExecutor
from hymir.workflow import (
    Workflow,
    job,
    Group,
    Chain,
)


@set_configuration
def set_config(config: Config):
    config.redis_url = "redis://localhost:6379/0"
    
    
@job(output="words")
def uppercase_word(word: str):
    return Success(word.upper())


@job(inputs=["words"], output="count")
def count_uppercase_words(words: list[str]):
    count = sum(1 for word in words if word.isupper())
    return Success(count)


workflow = Workflow(
    Chain(
        Group(
            uppercase_word("hello"),
            uppercase_word("world"),
        ),
        count_uppercase_words()
    )
)

# This assumes you've already setup & configured a celery app before you
# get here.
executor = CeleryExecutor()
workflow_id = executor.run(workflow)
```

[Celery]: https://docs.celeryproject.org/en/stable/
[Redis]: https://redis.io/
