Callbacks
=========

You'll often want to run a Job in response to other events in the workflow.
For example, you may want to run a Job if the workflow finishes, even if it
failed. You can accomplish this using workflow callbacks.

.. code-block:: python

  from hymir.workflow import Workflow, job, Chain
  from hymir.job import Failure

  @job()
  def dummy_job():
    # Doesn't do anything but fail
    return Failure()

  @job()
  def on_finished():
    # Always called, no matter what.
    print("Workflow finished")

  workflow = Workflow(
    Chain(
      dummy_job(),
    )
  )
  workflow.on(Workflow.Callbacks.ON_FINISHED, on_finished())