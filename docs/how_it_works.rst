How It Works
============

Overall, Hymir is a very simple tool. Given a list of Chains and Groups, it
produces a graph using `networkx`_, which is then resolved in topological order
until every node in the graph has been visited and completed. As it works, it
stores the state of each job and the overall workflow into `redis`_.

When you wrap a function in the :func:`hymir.workflow.job()` decorator, the function
is being replaced by one that returns a :class:`hymir.job.Job` object when it gets called.
This object is then added to the workflow graph. For example, instead of doing this:


.. code-block:: python

    from hymir.workflow import job, Chain, Workflow

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
                kwargs=[]
            )
        )
    )


You can visually explore the generated graph by using :func:`hymir.visualize.render_workflow()`:

.. code-block:: python

    from hymir.visualize import render_graph

    render_workflow(workflow, "workflow.svg")


.. _networkx: https://networkx.github.io/
.. _redis: http://redis.io/
.. _dataclass: https://docs.python.org/3/library/dataclasses.html