.. Hymir documentation master file, created by
   sphinx-quickstart on Wed Mar 27 16:06:28 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Hymir
=====

Hymir is a python library for creating and running simple workflows by composing
together Chains (which run sequentially) and Groups (which run in parallel) of
jobs.

Hymir is built on top of Redis for intermediate storage and tracking workflows,
and by default comes with an executor for running those workflows using Celery.
If you want to use a different task queue, you can implement your own executor
by subclassing :class:`hymir.executor.Executor`.

Disclaimer
==========

Hymir is still in the early stages of development and is not yet ready for
production use. The API is subject to change, although the high-level API is
unlikely to change significantly.

Installation
============

Hymir can be installed via pip:

.. code-block:: bash

    pip install hymir

Example
=======

Using the `@job` decorator is the simplest way to define a job. The decorated
function should return a `Success`, `Failure`, `Retry`, or `CheckLater` object.
The only limitations are:

- The decorated function must be importable from other files
  (`from mymodule import myjob` should work)
- The outputs and inputs of a job must be JSON-safe.

When jobs depend on the output of other jobs, you can specify the inputs of the
job using the `inputs` parameter of the `@job` decorator and the output of the
job using the `output` parameter.

.. code-block:: python

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


   @job(inputs=["count"])
   def save_results(count: int):
      # This could be anything - save to a django model, redis,
      # send an email, etc...
      print(count)


   workflow = Workflow(
       Chain(
           Group(
               uppercase_word("hello"),
               uppercase_word("world"),
           ),
           count_uppercase_words(),
           save_results()
       )
   )

   # This assumes you've already setup & configured a celery app before you
   # get here.
   executor = CeleryExecutor()
   workflow_id = executor.run(workflow)

   # Block until all jobs in the workflow are complete. Typically, you
   # don't want to do any blocking calls but this is very handy for
   # testing.
   executor.wait(workflow_id)


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   how_it_works
   hymir



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
