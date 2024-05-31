How It Works
============

Overall, Hymir is a very simple tool. Given a list of Chains and Groups, it
produces a graph using `networkx`_, which is then resolved in topological order
until every node in the graph has been visited and completed. As it works, it
stores the state of each job and the overall workflow into `redis`_.

There are just a few concepts to understand:

- **Job**: A job is a function that is wrapped by the :func:`hymir.job.job()`
  decorator. When called, it returns a :class:`hymir.job.Job` object that is
  added to the workflow graph.
- **Chain**: A chain is a sequence of jobs that are executed in order.
- **Group**: A group is a set of jobs that are executed in parallel.
- **Workflow**: A workflow is a collection of chains and groups that are
  executed in the specified order.
- **Executor**: An executor is the engine responsible for starting and
  resolving the workflow graph. For example, the CeleryExecutor uses the Celery
  distributed task queue to execute the jobs in the graph asynchronously,
  possibly on other machines.


Jobs
====

When you wrap a function in the :func:`hymir.job.job()` decorator, the function
is being replaced by one that returns a :class:`hymir.job.Job` object when it gets called.
This object is then added to the workflow graph. For example, instead of doing this:


.. code-block:: python

    from hymir.job import job
    from hymir.workflow import Chain, Workflow

    @job()
    def my_job(word: str):
        pass


    workflow = Workflow(
        Chain(
            my_job("hello world")
        )
    )


You can do this, which is functionally equivalent:


.. code-block:: python

    from hymir.job import Job
    from hymir.workflow import Chain, Workflow

    def my_job():
        pass

    workflow = Workflow(
        Chain(
            Job(
                name="mymodule.my_job",
                args=("hello world",),
            )
        )
    )


Since Job objects should be considered immutable once created, some helpers
exist on the Job to create a new one with your changes:


.. code-block:: python

    from hymir.job import Job
    from hymir.workflow import Chain, Workflow

    def my_job():
        pass

    workflow = Workflow(
        Chain(
            Job.from_function(my_job).set("hello world")
        )
    )


There are a few of rules for jobs that should be followed:

- The job must be idempotent. This means that the job can be run multiple times
  without causing any side effects. This allows recovery from failures and
  retries.
- The job must be importable. This means that the job must be importable from
  the module that is running the workflow. This means that lambdas and
  dynamically created functions are not supported.
- All arguments to the job must be serializable. This means that the arguments
  to the job must be able to be serialized and deserialized to JSON. For
  example, instead of passing a Django ORM object, pass the primary key and
  look it up in the job.


Chains and Groups
=================

Chains and Groups are used to define the order in which jobs are executed. A
Chain is a sequence of jobs that are executed in order, while a Group is a set
of jobs that are executed in parallel. For example, the following workflow
executes the jobs `job1`, `job2`, and `job3` in order, and then executes `job4`
and `job5` in parallel:


.. code-block:: python

    from hymir.job import Job
    from hymir.workflow import Chain, Group, Workflow

    workflow = Workflow(
        Chain(
          job1,
          job2,
          job3
        ),
        Group(
            Job.from_function(job4),
            Job.from_function(job5),
        )
    )


Workflow
========

A Workflow is just a thin veneer over a directed acyclic graph (DAG), providing
the tools to turn Chains and Groups into a graph. The Workflow object is
itself stateless, and can be reused across multiple executions.

You can visually explore the generated graph by using
:func:`hymir.visualize.render_workflow()`:

.. code-block:: python

    from hymir.visualize import render_graph

    render_workflow(workflow, "workflow.svg")


Executor
========

The Executor is the engine that drives the workflow. It is responsible for
starting the workflow and resolving the graph in topological order. As the
workflow progresses, the Executor stores the state of each job as a
:class:`hymir.executor.JobState` object, and the overall workflow as a
:class:`hymir.executor.WorkflowState` object.

How an Executor resolves the graph is up to the implementation. For example, the
CeleryExecutor uses the Celery distributed task queue to execute the jobs in the
graph asynchronously, possibly on 1 or more machines.


.. _networkx: https://networkx.github.io/
.. _redis: http://redis.io/
.. _dataclass: https://docs.python.org/3/library/dataclasses.html